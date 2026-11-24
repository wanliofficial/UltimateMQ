#!/usr/bin/env python3
"""MQTT-3.3.4-6 / §3.3.4-6: a PUBLISH from a client must not carry a Subscription
Identifier.

The Subscription Identifier is an identifier used to identify the subscription
that caused a PUBLISH to be sent. It is a property of the SUBSCRIBE (and of the
PUBLISH the *server* forwards to a subscriber, MQTT-3.8.4.1.2 / §3.3.2.10); it
has no meaning on a PUBLISH a client sends to the server. The spec forbids it:
"If the Server receives a PUBLISH packet containing the Subscription Identifier
Property it is a Malformed Packet" / equivalently it is a Protocol Error to send
one inbound.

handlePublish rejects an inbound PUBLISH that carries any Subscription Identifier
with a DISCONNECT 0x82 (PROTOCOL_ERROR) -- not silently forwarding it as if the
identifier meant something. The rejection happens before routing.

Pinned here on the live TCP path:

  * a v5 PUBLISH carrying a Subscription Identifier -> DISCONNECT 0x82;
  * control: the same PUBLISH without it is accepted (a normal PUBACK), so the
    rejection is specific to the forbidden property.

Usage: publish_subscription_id_test.py <binary> <port>
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

PUBLISH_QOS1 = 0x32
PUBACK = 0x40
DISCONNECT = 0xE0

PROP_SUBSCRIPTION_IDENTIFIER = 0x0B
REASON_PROTOCOL_ERROR = 0x82


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


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def publish(pid, topic, payload, properties=b""):
    # Variable Header order the broker parses: Topic Name, Packet Identifier
    # (QoS>0), Properties, then payload.
    body = utf8(topic) + struct.pack("!H", pid) + enc_len(len(properties)) + properties + payload
    return bytes([PUBLISH_QOS1]) + enc_len(len(body)) + body


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
    workdir = tempfile.mkdtemp(prefix="flowmq-subid-")
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


def case_publish_with_subscription_id_disconnects():
    """A v5 PUBLISH carrying a Subscription Identifier (0x0B) is refused with a
    DISCONNECT 0x82 (PROTOCOL_ERROR)."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("sid-reject"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            # Subscription Identifier property: id 0x0B + VBI value (1).
            props = bytes([PROP_SUBSCRIPTION_IDENTIFIER, 0x01])
            c.sendall(publish(7, "sid/topic", b"data", props))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker closed with no DISCONNECT (subscription id in PUBLISH)"
            h, b = pkt
            assert h & 0xF0 == DISCONNECT, (
                "expected DISCONNECT for a PUBLISH with a Subscription Identifier, "
                "got header 0x%02X" % h)
            assert b and b[0] == REASON_PROTOCOL_ERROR, (
                "an inbound Subscription Identifier must be DISCONNECT 0x82, got %r" % b)
            return "a PUBLISH carrying a Subscription Identifier -> DISCONNECT 0x82"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_publish_with_zero_subscription_id_disconnects():
    """A zero Subscription Identifier is a Protocol Error before applicability."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("sid-zero"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(publish(9, "sid/zero", b"data", bytes([PROP_SUBSCRIPTION_IDENTIFIER, 0x00])))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no DISCONNECT for zero Subscription Identifier"
            h, b = pkt
            assert h & 0xF0 == DISCONNECT, "expected DISCONNECT, got 0x%02X" % h
            assert b and b[0] == REASON_PROTOCOL_ERROR, (
                "a zero Subscription Identifier must be PROTOCOL_ERROR (0x82), got %r" % b)
            return "zero Subscription Identifier -> DISCONNECT 0x82"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_publish_without_subscription_id_accepted():
    """Control: the same QoS 1 PUBLISH with an empty property block (no
    Subscription Identifier) is accepted with a normal PUBACK, so the rejection
    above is specific to the forbidden property."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("sid-ok"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(publish(8, "sid/ok", b"data", properties=b""))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker sent no reply for a clean PUBLISH"
            h, b = pkt
            assert h & 0xF0 == PUBACK, (
                "a clean PUBLISH must get a PUBACK, got header 0x%02X" % h)
            # PUBACK/SUCCESS short form; reason byte absent or 0x00.
            reason = b[2] if len(b) > 2 else 0x00
            assert reason == 0x00, "clean PUBLISH must be PUBACK/SUCCESS, got 0x%02X" % reason
            return "a clean PUBLISH is still accepted with PUBACK/SUCCESS"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.3.4-6: a v5 PUBLISH carrying a Subscription Identifier is refused with DISCONNECT 0x82 (PROTOCOL_ERROR)",
     case_publish_with_subscription_id_disconnects)
case("MQTT-3.8.2-16: a zero Subscription Identifier is a Protocol Error -> DISCONNECT 0x82",
     case_publish_with_zero_subscription_id_disconnects)
case("MQTT 5 (control): a QoS 1 PUBLISH with no Subscription Identifier is still accepted with PUBACK/SUCCESS",
     case_publish_without_subscription_id_accepted)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
