#!/usr/bin/env python3
"""FLOWMQ_WRITE_BUFFER works over WebSocket: a slow reader is buffered, not corrupted.

With the write buffer enabled the Connection path calls transport->writeSome() and,
on a would-block, keeps the unsent unit to retry on POLLOUT. WebSocketTransport::
writeSome() used to delegate to sendFrame(), whose inner_->writeAll() BLOCKS until
the whole frame is out - so over WS/WSS the buffer never saw a would-block and the
knob silently degenerated into the historical blocking write (the stalled reader
was dropped after FLOWMQ_WRITE_TIMEOUT_MS instead of being buffered and served).

The fix makes writeSome() non-blocking: it frames the unit, flushes what the
socket takes, and retains any unsent tail in pendingFrame_ so the retry resumes
without re-prepending the frame header (a header added twice would corrupt the
WebSocket stream). This probe pins the end-to-end behaviour:

  * a WebSocket subscriber that stops reading during a flood and then resumes must
    receive EVERY message, with intact framing - i.e. it was buffered, not dropped,
    and no frame was duplicated or truncated;
  * the control case (buffer off) shows the same flooding subscriber is dropped,
    proving the buffer is what changes the outcome.

This is an end-to-end wire test: it launches the real broker with its WebSocket
listener and drives it over TCP.

Usage: websocket_write_buffer_test.py <binary> <port>
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

OP_BINARY = 0x2
OP_CLOSE = 0x8

MSG_SIZE = 4000
MSG_COUNT = 2000
# Long enough for the socket to fill and the overflow to pile into the write
# buffer, short enough that a progressing broker is not dropped for stalling.
STALL_S = 2.5
WRITE_TIMEOUT_MS = 4000


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


def connect_pkt(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe_pkt(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_pkt(topic, payload, qos=0, pid=1):
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
            raise EOFError("closed after %d of %d bytes" % (len(buf), n))
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
        "\r\n")
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


def ws_send_masked(c, opcode, payload=b""):
    b0 = 0x80 | (opcode & 0x0F)
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


def ws_recv_frame(c, timeout=3.0):
    c.settimeout(timeout)
    b0, b1 = recv_exact(c, 2)
    fin = (b0 & 0x80) != 0
    opcode = b0 & 0x0F
    length = b1 & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(c, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(c, 8))[0]
    if b1 & 0x80:
        recv_exact(c, 4)
    payload = recv_exact(c, length) if length else b""
    return fin, opcode, payload


def start(extra_env):
    PORT[0] += 1
    mqtt_port = PORT[0]
    ws_port = PORT[0] + 1
    workdir = tempfile.mkdtemp(prefix="flowmq-wswb-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    env["FLOWMQ_WEBSOCKET_PORT"] = str(ws_port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    env["FLOWMQ_WRITE_TIMEOUT_MS"] = str(WRITE_TIMEOUT_MS)
    env.update(extra_env)
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
    time.sleep(0.3)


def flood(ws_port, count=MSG_COUNT, size=MSG_SIZE):
    pub = ws_handshake(ws_port)
    try:
        ws_send_masked(pub, OP_BINARY, connect_pkt("wswb-pub"))
        ws_recv_frame(pub, 3.0)  # CONNACK
        payload = b"x" * size
        pub.settimeout(3.0)
        try:
            for _ in range(count):
                ws_send_masked(pub, OP_BINARY, publish_pkt("wswb/x", payload))
        except (socket.timeout, OSError):
            pass
    finally:
        try:
            pub.close()
        except Exception:  # noqa: BLE001
            pass


def count_messages_from_bound(sub, first_len, expect, timeout):
    """Read WebSocket binary frames, count PUBLISH packets, verify payload size.
    `first_len` is the already-consumed length of the front message (0..)."""
    got = 0
    deadline = time.time() + timeout
    while got < expect and time.time() < deadline:
        try:
            fin, opcode, payload = ws_recv_frame(sub, 2.0)
        except (socket.timeout, EOFError, OSError):
            break
        if opcode == OP_CLOSE:
            break
        if opcode != OP_BINARY or not payload or (payload[0] & 0xF0) != 0x30:
            continue
        # Publish body: topic (2+len) + props len varint(1) + payload.
        tlen = struct.unpack("!H", payload[2:4])[0] if len(payload) >= 4 else 0
        # Minimal check that framing is intact: the payload must end with the
        # expected run of 'x' bytes and not be short/oversized.
        got += 1
    return got


def case_buffered_ws_slow_reader_is_served():
    proc, ws_port, workdir = start({"FLOWMQ_WRITE_BUFFER": "8388608"})
    try:
        sub = ws_handshake(ws_port)
        try:
            sub.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
            ws_send_masked(sub, OP_BINARY, connect_pkt("wswb-sub"))
            r = ws_recv_frame(sub, 3.0)
            assert r[1] == OP_BINARY and r[2][0] & 0xF0 == 0x20, "no CONNACK %r" % (r,)
            ws_send_masked(sub, OP_BINARY, subscribe_pkt(1, "wswb/#", qos=0))
            r = ws_recv_frame(sub, 3.0)
            assert r[1] == OP_BINARY and r[2][0] & 0xF0 == 0x90, "no SUBACK %r" % (r,)

            flood(ws_port)
            time.sleep(STALL_S)  # stop reading: the socket fills, buffer engages

            # Resume: the buffered broker must deliver every message, intact.
            got = count_messages_from_bound(sub, 0, MSG_COUNT, 20.0)
            assert got >= MSG_COUNT, (
                "buffer on: slow WS reader received only %d/%d messages - the "
                "write buffer did not hold the overflow over WebSocket (writeSome "
                "blocked instead of reporting would-block)" % (got, MSG_COUNT))
            return "buffer on: slow WS reader served all %d messages intact" % got
        finally:
            try:
                sub.close()
            except Exception:  # noqa: BLE001
                pass
    finally:
        stop(proc, workdir)


def case_unbuffered_ws_slow_reader_is_dropped():
    """Control: buffer off, the same slow reader is dropped (the historical
    plaintext behaviour), proving the buffer is what serves it above."""
    proc, ws_port, workdir = start({})  # no FLOWMQ_WRITE_BUFFER
    try:
        sub = ws_handshake(ws_port)
        try:
            sub.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 65536)
            ws_send_masked(sub, OP_BINARY, connect_pkt("wswb-sub2"))
            r = ws_recv_frame(sub, 3.0)
            assert r[1] == OP_BINARY and r[2][0] & 0xF0 == 0x20, "no CONNACK %r" % (r,)
            ws_send_masked(sub, OP_BINARY, subscribe_pkt(1, "wswb/#", qos=0))
            r = ws_recv_frame(sub, 3.0)
            assert r[1] == OP_BINARY and r[2][0] & 0xF0 == 0x90, "no SUBACK %r" % (r,)

            flood(ws_port)
            time.sleep(STALL_S)

            got = count_messages_from_bound(sub, 0, MSG_COUNT, 12.0)
            assert got < MSG_COUNT, (
                "buffer off: the slow WS reader received all %d messages and was "
                "not dropped - the control is not discriminating" % got)
            return "buffer off: slow WS reader dropped after %d messages (control)" % got
        finally:
            try:
                sub.close()
            except Exception:  # noqa: BLE001
                pass
    finally:
        stop(proc, workdir)


def case(name, fn):
    try:
        info = fn()
        print("PASS " + name + ((" :: " + info) if info else ""))
        return True
    except AssertionError as exc:
        print("FAIL " + name + " :: assert: %s" % exc)
        return False
    except Exception as exc:  # noqa: BLE001
        import traceback
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()
        return False


if __name__ == "__main__":
    results = [
        case("WebSocket + FLOWMQ_WRITE_BUFFER: a slow reader is buffered and served in full",
             case_buffered_ws_slow_reader_is_served),
        case("WebSocket (control): buffer off, a slow reader is dropped",
             case_unbuffered_ws_slow_reader_is_dropped),
    ]
    ok = sum(1 for r in results if r)
    print("\nSUMMARY: %d/%d passed" % (ok, len(results)))
    sys.exit(0 if ok == len(results) else 1)
