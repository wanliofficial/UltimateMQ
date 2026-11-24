#!/usr/bin/env python3
"""MQTT-3.1.2-8 / §3.1.2-8 / §3.14.2.2.2: what a client DISCONNECT does to its Will.

A client may register a Will Message in the CONNECT. The Will is published when
the connection ends abnormally, but a graceful client DISCONNECT is the signal
that the client left on purpose -- and which Reason Code it carries decides
whether the Will still fires:

  * MQTT-3.1.2-8: the Will is deleted only by a DISCONNECT with Reason Code
    0x00 (Normal disconnection). The connection teardown publishes the Will for
    every other case, so willPending stays armed unless the code is 0x00.
  * 0x04 (Disconnect with Will Message) exists precisely so a client can end the
    connection cleanly yet still have its Will published. So a 0x04 DISCONNECT
    must publish the Will; a 0x00 must not.

Also pinned, §3.14.2.2.2: if the CONNECT declared Session Expiry Interval 0
(a live-only connection), the client MAY NOT raise it to non-zero in its
DISCONNECT -- doing so would turn a connection that ends at once into a durable
session at the last moment, so the broker answers DISCONNECT 0x82 (PROTOCOL_ERROR)
and refuses.

Usage: disconnect_will_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
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

REASON_SUCCESS = 0x00
REASON_PROTOCOL_ERROR = 0x82
REASON_DISCONNECT_WITH_WILL_MESSAGE = 0x04

DISCONNECT = 0xE0
PROP_SESSION_EXPIRY_INTERVAL = 0x11


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


def connect(cid, will_topic=None, will_msg=None, session_expiry=None):
    # Clean Start always; Will Flag only when a Will is supplied.
    has_will = will_topic is not None
    flags = 0x02 | (0x04 if has_will else 0x00)
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60)
    # CONNECT properties
    connect_props = b""
    if session_expiry is not None:
        connect_props = bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
    body += enc_len(len(connect_props)) + connect_props
    # Payload: Client Id, then (Will properties, Will Topic, Will Message)
    body += utf8(cid)
    if will_topic is not None:
        body += enc_len(0)  # Will properties (empty)
        body += utf8(will_topic)
        body += struct.pack("!H", len(will_msg)) + will_msg
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def disconnect_normal():
    # RL 0 -> Reason Code 0x00.
    return bytes([DISCONNECT])


def disconnect_with_will():
    # RL 1 -> Reason Code 0x04.
    return bytes([DISCONNECT]) + enc_len(1) + bytes([REASON_DISCONNECT_WITH_WILL_MESSAGE])


def disconnect_session_expiry(value):
    # Reason Code 0x00 + Session Expiry Interval property.
    props = bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", value)
    body = bytes([REASON_SUCCESS]) + enc_len(len(props)) + props
    return bytes([DISCONNECT]) + enc_len(len(body)) + body


def read_packet(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        header = _recv_exact(sock, 1)[0]
    except (socket.timeout, EOFError):
        return None
    mult = 1
    val = 0
    while True:
        try:
            b = _recv_exact(sock, 1)[0]
        except (socket.timeout, EOFError):
            return None
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = _recv_exact(sock, val) if val else b""
    return header, body


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-dwill-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            c = socket.create_connection((HOST, mqtt_port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, mqtt_port, workdir
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
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
        return
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()
        return
    results.append((name, True))
    print("PASS " + name + ((" :: " + info) if info else ""))


def _drain_publish(sock, timeout=1.5):
    """Return the first PUBLISH (QoS 0) body received, or None if none arrives."""
    pkt = read_packet(sock, timeout)
    if pkt is None:
        return None
    h, b = pkt
    if h & 0xF0 != 0x30:
        return ("non-publish", h, b)
    tlen = struct.unpack("!H", b[0:2])[0]
    off = 2 + tlen
    # MQTT 5 QoS 0 PUBLISH body: Topic Name + Properties + payload. Skip the
    # property length VBI to reach the payload.
    plen = 0
    mult = 1
    while True:
        x = b[off]
        off += 1
        plen += (x & 127) * mult
        mult *= 128
        if not x & 128:
            break
    topic = b[2:2 + tlen].decode()
    payload = b[off + plen:]
    return (topic, payload)


def case_disconnect_with_will_publishes():
    """A client that ends with DISCONNECT 0x04 (Disconnect with Will Message)
    still has its Will published: the subscriber receives the Will Message."""
    proc, port, workdir = start()
    sub = socket.create_connection((HOST, port), timeout=5)
    cli = socket.create_connection((HOST, port), timeout=5)
    try:
        sub.sendall(connect("dw-sub"))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
        cli.sendall(connect("dw-withwill", will_topic="dw/status",
                            will_msg=b"gone-via-04"))
        assert read_packet(cli)[0] & 0xF0 == 0x20, "no CONNACK (client)"
        sub.sendall(subscribe(1, "dw/#", qos=0))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        cli.sendall(disconnect_with_will())
        got = _drain_publish(sub, 1.5)
        assert got is not None and got[0] != "non-publish", (
            "DISCONNECT 0x04 must publish the Will, subscriber got %r" % (got,))
        assert got[0] == "dw/status", (
            "the Will arrived on the wrong topic %r" % (got[0],))
        assert got[1] == b"gone-via-04", (
            "the Will payload was altered, got %r" % (got[1],))
        return "DISCONNECT 0x04 published the Will (dw/status = gone-via-04)"
    finally:
        sub.close()
        cli.close()
        stop(proc, workdir)


def case_disconnect_normal_suppresses_will():
    """Control: the same Will, but the client ends with DISCONNECT 0x00 (Normal
    disconnection). MQTT-3.1.2-8 deletes the Will, so the subscriber gets
    nothing -- proving the 0x04 case above is about the Reason Code, not about
    Wills in general."""
    proc, port, workdir = start()
    sub = socket.create_connection((HOST, port), timeout=5)
    cli = socket.create_connection((HOST, port), timeout=5)
    try:
        sub.sendall(connect("dw2-sub"))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
        cli.sendall(connect("dw2-normal", will_topic="dw2/status",
                            will_msg=b"should-never-appear"))
        assert read_packet(cli)[0] & 0xF0 == 0x20, "no CONNACK (client)"
        sub.sendall(subscribe(1, "dw2/#", qos=0))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        cli.sendall(disconnect_normal())
        got = _drain_publish(sub, 1.5)
        assert got is None, (
            "a Normal DISCONNECT (0x00) must suppress the Will, subscriber got %r"
            % (got,))
        return "DISCONNECT 0x00 suppressed the Will (nothing delivered)"
    finally:
        sub.close()
        cli.close()
        stop(proc, workdir)


def case_disconnect_cannot_raise_session_expiry():
    """§3.14.2.2.2: a CONNECT with Session Expiry Interval 0 is live-only; the
    client may not raise it to non-zero in its DISCONNECT. The broker answers
    DISCONNECT 0x82 (PROTOCOL_ERROR)."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("dw-expiry", session_expiry=0))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(disconnect_session_expiry(60))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker closed with no DISCONNECT (raised expiry)"
            h, b = pkt
            assert h & 0xF0 == 0xE0, (
                "expected a DISCONNECT for a raised Session Expiry, got 0x%02X" % h)
            assert b and b[0] == REASON_PROTOCOL_ERROR, (
                "raising a zero Session Expiry must be DISCONNECT 0x82, got %r" % b)
            return "DISCONNECT raising a zero Session Expiry -> DISCONNECT 0x82"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.1.2-8: a client DISCONNECT with Reason Code 0x04 (Disconnect with Will Message) still has its Will published",
     case_disconnect_with_will_publishes)
case("MQTT-3.1.2-8: a client DISCONNECT with Reason Code 0x00 (Normal) deletes its Will, so it is never published",
     case_disconnect_normal_suppresses_will)
case("MQTT 3.14.2.2.2: a DISCONNECT that raises a zero CONNECT Session Expiry Interval to non-zero is refused with DISCONNECT 0x82 (PROTOCOL_ERROR)",
     case_disconnect_cannot_raise_session_expiry)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
