#!/usr/bin/env python3
"""$SYS/broker monitoring tree over an MQTT-over-WebSocket connection.

The WebSocket probe suite covers framing guards and a plain CONNECT/SUBSCRIBE/
PUBLISH round-trip, but never the $SYS monitoring tree, which FLOWMQ_SYS_TOPICS
makes opt-in and which reaches subscribers through the ordinary retained-message
plus live-fan-out path. This probe pins that path over a real WebSocket transport:

  1. A WebSocket client subscribing to $SYS/# receives the retained snapshot
     (RETAIN=1, decimal payloads), exactly like a TCP client.
  2. The tree keeps updating after subscribe: the live fan-out reaches the
     WebSocket client too, with RETAIN cleared (MQTT-3.3.1-8). This is the
     transport-independent counterpart of the TCP probe, and it exercises the
     $SYS delivery path through the WebSocket write buffer.
  3. MQTT-4.1.2-2: a client PUBLISH into the $SYS tree over WebSocket is refused
     with NOT_AUTHORIZED (0x87) in its PUBACK.

This is an end-to-end wire test: it launches the real broker binary with its
WebSocket listener enabled and drives it over TCP.

Usage: websocket_sys_topics_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
import base64
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

HOST = "127.0.0.1"
BIN = sys.argv[1]
PORT = [int(sys.argv[2])]

# WebSocket opcodes (RFC 6455 §5.2).
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def enc_len(n):
    out = b""
    while True:
        d = n % 128
        n //= 128
        if n:
            d |= 0x80
        out += bytes([d])
        if not n:
            return out


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect(cid, clean_start=True):
    flags = 0x02 if clean_start else 0x00
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(topic, payload, qos=0, pid=1):
    body = utf8(topic)
    if qos:
        body += struct.pack("!H", pid)
    body += enc_len(0) + payload
    header = 0x30 | (qos << 1)
    return bytes([header]) + enc_len(len(body)) + body


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def ws_handshake(port):
    c = socket.create_connection((HOST, port), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        "GET / HTTP/1.1\r\n"
        "Host: %s:%d\r\n" % (HOST, port) +
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n" % key +
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    )
    c.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = c.recv(1)
        if not chunk:
            raise EOFError("closed during handshake")
        resp += chunk
    if b" 101 " not in resp.split(b"\r\n", 1)[0]:
        raise AssertionError("websocket upgrade failed: %r" % resp.split(b"\r\n", 1)[0])
    return c


def ws_send_masked(c, opcode, payload=b"", fin=True):
    b0 = (0x80 if fin else 0x00) | (opcode & 0x0F)
    length = len(payload)
    if length < 126:
        hdr = bytes([b0, 0x80 | length])
    elif length < 65536:
        hdr = bytes([b0, 0x80 | 126]) + struct.pack("!H", length)
    else:
        hdr = bytes([b0, 0x80 | 127]) + struct.pack("!Q", length)
    mask = os.urandom(4)
    masked = bytes(payload[i] ^ mask[i % 4] for i in range(length))
    c.sendall(hdr + mask + masked)


def ws_recv_frame(c, timeout=2.0):
    c.settimeout(timeout)
    b0, b1 = recv_exact(c, 2)
    fin = (b0 & 0x80) != 0
    opcode = b0 & 0x0F
    masked = (b1 & 0x80) != 0
    length = b1 & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(c, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(c, 8))[0]
    if masked:
        recv_exact(c, 4)
    payload = recv_exact(c, length) if length else b""
    return fin, opcode, payload


def ws_mqtt_read(c, timeout=2.0):
    """Read one binary frame carrying an MQTT packet; return (mqtt_header, body).

    The packet is laid out as the fixed-header byte, then the Remaining Length
    VBI, then the body (the rest of the frame)."""
    fin, opcode, payload = ws_recv_frame(c, timeout)
    if opcode != OP_BINARY:
        raise AssertionError("expected a binary frame, got opcode 0x%02x" % opcode)
    header = payload[0]
    mult = 1
    val = 0
    i = 1
    while True:
        b = payload[i]
        i += 1
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    return header, payload[i:]


def parse_publish(body):
    """Return (topic, payload) from an MQTT 5 PUBLISH body.

    The body is Topic Name (length-prefixed), then the Property Length VBI, then
    the application payload. We skip the property length so the $SYS topics (which
    carry no properties) still parse, and a property-carrying PUBLISH does not
    shift the payload."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    # Skip the Property Length VBI (MQTT §2.2.2.2).
    while True:
        b = body[off]
        off += 1
        if not b & 128:
            break
    return topic, body[off:]


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    ws_port = PORT[0] + 1
    workdir = tempfile.mkdtemp(prefix="flowmq-wssys-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    env["FLOWMQ_WEBSOCKET_PORT"] = str(ws_port)
    env["FLOWMQ_SYS_TOPICS"] = "1"
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            c = socket.create_connection((HOST, ws_port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, ws_port, workdir
        except Exception:  # noqa: BLE001
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start")


def stop(proc, workdir):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    time.sleep(0.6)


results = []


def case(name, fn):
    try:
        info = fn()
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()


def case_sys_tree_reaches_websocket_subscriber():
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            ws_send_masked(c, OP_BINARY, connect("ws-sys-sub"))
            header, body = ws_mqtt_read(c)
            assert header & 0xF0 == 0x20, "no CONNACK over WebSocket: %r" % header

            ws_send_masked(c, OP_BINARY, subscribe(1, "$SYS/#"))
            header, body = ws_mqtt_read(c)
            assert header & 0xF0 == 0x90, "no SUBACK over WebSocket: %r" % header

            # Collect the retained snapshot(s): each is a binary frame carrying a
            # PUBLISH with the RETAIN flag set (MQTT-3.3.1-2).
            snapshots = {}
            deadline = time.time() + 2.0
            while time.time() < deadline:
                try:
                    header, body = ws_mqtt_read(c, idle := 0.4)
                except (EOFError, socket.timeout):
                    break
                except AssertionError:
                    break
                if header & 0xF0 != 0x30:
                    continue
                retain = header & 0x01
                topic, payload = parse_publish(body)
                text = payload.decode()
                assert text.replace(".", "", 1).isdigit(), (
                    "%s payload is not a decimal number: %r" % (topic, text))
                if retain == 1:
                    snapshots[topic] = text
            assert snapshots, "the $SYS tree did not reach a WebSocket subscriber"
            for topic in ("$SYS/broker/clients/connected",
                          "$SYS/broker/uptime",
                          "$SYS/broker/messages/sent"):
                assert topic in snapshots, (
                    "%s missing over WebSocket (got %r)" % (topic, sorted(snapshots)))

            # A live update follows on the next sweep, with RETAIN cleared.
            live = None
            deadline = time.time() + 6.0
            while time.time() < deadline:
                try:
                    header, body = ws_mqtt_read(c, 1.0)
                except (EOFError, socket.timeout):
                    continue
                except AssertionError:
                    continue
                if header & 0xF0 == 0x30 and (header & 0x01) == 0:
                    live = parse_publish(body)[0]
                    break
            assert live is not None, "no live $SYS update reached the WebSocket subscriber"
            return "%d topics retained over WebSocket; live update on %s" % (
                len(snapshots), live)
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_sys_publish_refused_over_websocket():
    """MQTT-4.1.2-2 over WebSocket: a client PUBLISH into the $SYS tree is refused
    with NOT_AUTHORIZED (0x87) in its PUBACK, like over TCP."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            ws_send_masked(c, OP_BINARY, connect("ws-spoofer"))
            header, body = ws_mqtt_read(c)
            assert header & 0xF0 == 0x20, "no CONNACK: %r" % header

            ws_send_masked(c, OP_BINARY, publish("$SYS/broker/uptime", b"999999",
                                                 qos=1, pid=7))
            header, body = ws_mqtt_read(c)
            assert header & 0xF0 == 0x40, (
                "no PUBACK for the $SYS publish over WebSocket: header 0x%02x" % header)
            pid = struct.unpack("!H", body[0:2])[0]
            reason = body[2] if len(body) > 2 else 0x00
            assert pid == 7, "PUBACK carried the wrong Packet Identifier"
            assert reason == 0x87, (
                "a client $SYS publish was not refused with NOT_AUTHORIZED (0x87); "
                "reason was 0x%02x" % reason)
            return "$SYS publish refused with 0x87 over WebSocket"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("WebSocket: the $SYS monitoring tree reaches a subscriber (retained + live update)",
     case_sys_tree_reaches_websocket_subscriber)
case("WebSocket: MQTT-4.1.2-2, a client PUBLISH into $SYS is refused (0x87)",
     case_sys_publish_refused_over_websocket)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
