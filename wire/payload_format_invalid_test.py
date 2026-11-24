#!/usr/bin/env python3
"""MQTT 5 Payload Format Indicator must be honoured (MQTT-3.3.2-5 / §3.3.2.11).

The Payload Format Indicator (property 0x01, a single byte) is the publisher's
contract about its payload: 0 means "unspecified bytes", 1 means "the payload is
UTF-8 encoded Unicode character data". A publisher that sets it to 1 but sends
bytes that are not valid UTF-8 is lying about its own packet, and MQTT 5 makes
the Server a party to that contract [MQTT-3.3.2-5]:

  "If the Payload Format Indicator is 1, and the Payload contains characters
   which are not in conformance with the Unicode specification [RFC3629], the
   Server MUST respond to the PUBLISH packet with the reason code
   Payload Format Invalid (0x99)."

The three consequences pinned here, matching the broker's three branches
(Connection::handlePublish):

  * QoS 1  -> PUBACK with PAYLOAD_FORMAT_INVALID (0x99);
  * QoS 2  -> PUBREC with PAYLOAD_FORMAT_INVALID (0x99);
  * QoS 0  -> there is no ack, so a v5 broker answers with a DISCONNECT carrying
    0x99 (a 3.1.1 broker has no DISCONNECT and simply drops the message).

publish_property_forward_test.py already checks that a VALID UTF-8 payload with
the indicator set is forwarded unchanged; this probe pins the other half -- a
payload that VIOLATES the declared format is refused, and a payload that is valid
is still accepted (control), so the refusal is about the format, not about the
indicator always being rejected.

Usage: payload_format_invalid_test.py <binary> <port>
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

PROP_PAYLOAD_FORMAT_INDICATOR = 0x01
REASON_PAYLOAD_FORMAT_INVALID = 0x99


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


def subscribe(pid, filt, qos=1):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos & 0x03])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(pid, topic, payload, qos, pfi=None):
    """MQTT 5 PUBLISH at the given QoS, optionally with a Payload Format
    Indicator. Variable header order: Topic Name, Packet Id (QoS>0), Properties,
    Payload."""
    props = b""
    if pfi is not None:
        props = bytes([PROP_PAYLOAD_FORMAT_INDICATOR]) + bytes([pfi])
    body = utf8(topic)
    if qos > 0:
        body += struct.pack("!H", pid)
    body += enc_len(len(props)) + props + payload
    header = 0x30 | ((qos & 0x03) << 1)
    return bytes([header]) + enc_len(len(body)) + body


def read_packet(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        header = recv_exact(sock, 1)[0]
    except (socket.timeout, EOFError):
        return None
    mult = 1
    val = 0
    while True:
        try:
            b = recv_exact(sock, 1)[0]
        except (socket.timeout, EOFError):
            return None
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = recv_exact(sock, val) if val else b""
    return header, body


def recv_exact(sock, n):
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
    workdir = tempfile.mkdtemp(prefix="flowmq-pfi-")
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


# A lone 0xFF byte is never valid UTF-8 (it is neither a valid start byte nor a
# continuation byte on its own), so Payload Format Indicator = 1 is violated.
INVALID_UTF8 = b"\xff\xfe"
VALID_UTF8 = "héllo ☃".encode("utf-8")


def case_qos1_invalid_payload():
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("pfi-qos1"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK"
            pkt = read_packet(pub, 2.0)
            pub.sendall(publish(0x42, "pfi/a", INVALID_UTF8, qos=1, pfi=1))
            header, body = read_packet(pub, 2.0)
            assert header is not None and header & 0xF0 == 0x40, (
                "expected a PUBACK for the QoS 1 publish, got %r" % (pkt,))
            # v5 PUBACK body: Packet Id (2) + Reason Code (1) + Properties.
            reason = body[2] if len(body) >= 3 else 0x00
            assert reason == REASON_PAYLOAD_FORMAT_INVALID, (
                "QoS 1 with an invalid UTF-8 payload must be PUBACK 0x99, got 0x%02x" % reason)
            return "QoS 1 invalid UTF-8 + PFI=1 -> PUBACK 0x99 (Payload Format Invalid)"
        finally:
            pub.close()
    finally:
        stop(proc, workdir)


def case_qos2_invalid_payload():
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("pfi-qos2"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK"
            pub.sendall(publish(0x43, "pfi/b", INVALID_UTF8, qos=2, pfi=1))
            header, body = read_packet(pub, 2.0)
            assert header is not None and header & 0xF0 == 0x50, (
                "expected a PUBREC for the QoS 2 publish, got %r" % (header,))
            # v5 PUBREC body: Packet Id (2) + Reason Code (1) + Properties.
            reason = body[2] if len(body) >= 3 else 0x00
            assert reason == REASON_PAYLOAD_FORMAT_INVALID, (
                "QoS 2 with an invalid UTF-8 payload must be PUBREC 0x99, got 0x%02x" % reason)
            return "QoS 2 invalid UTF-8 + PFI=1 -> PUBREC 0x99 (Payload Format Invalid)"
        finally:
            pub.close()
    finally:
        stop(proc, workdir)


def case_qos0_invalid_payload_disconnects():
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("pfi-qos0"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK"
            pub.sendall(publish(0x00, "pfi/c", INVALID_UTF8, qos=0, pfi=1))
            header, body = read_packet(pub, 2.0)
            assert header is not None and header & 0xF0 == 0xE0, (
                "a QoS 0 publish with an invalid UTF-8 payload must be answered "
                "with a DISCONNECT, got header 0x%02x" % header)
            reason = body[0] if body else 0x00
            assert reason == REASON_PAYLOAD_FORMAT_INVALID, (
                "QoS 0 invalid UTF-8 + PFI=1 must be DISCONNECT 0x99, got 0x%02x" % reason)
            return "QoS 0 invalid UTF-8 + PFI=1 -> DISCONNECT 0x99 (Payload Format Invalid)"
        finally:
            pub.close()
    finally:
        stop(proc, workdir)


def case_valid_payload_still_accepted():
    """Control: the same publish with VALID UTF-8 under PFI=1 is accepted, so the
    0x99 refusals above are about the format being violated, not the indicator
    itself. QoS 1 with a valid payload yields PUBACK 0x00 (Success)."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        sub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("pfi-ok-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            sub.sendall(connect("pfi-ok-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            sub.sendall(subscribe(1, "pfi/ok", qos=1))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            pub.sendall(publish(0x44, "pfi/ok", VALID_UTF8, qos=1, pfi=1))
            header, body = read_packet(pub, 2.0)
            assert header is not None and header & 0xF0 == 0x40, (
                "a valid UTF-8 QoS 1 publish must draw a PUBACK, got %r" % (header,))
            # v5 PUBACK body: Packet Id (2) + Reason Code (1) + Properties.
            reason = body[2] if len(body) >= 3 else 0x00
            assert reason == 0x00, (
                "a valid UTF-8 payload must be accepted (PUBBACK 0x00), got 0x%02x" % reason)
            # And it is actually delivered to the subscriber.
            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "the valid payload must be delivered to the subscriber, got %r" % (pkt,))
            return "valid UTF-8 under PFI=1 accepted (PUBBACK 0x00) and delivered"
        finally:
            pub.close()
            sub.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.3.2-5: a QoS 1 PUBLISH with an invalid UTF-8 payload and Payload Format Indicator 1 is PUBACK 0x99",
     case_qos1_invalid_payload)
case("MQTT-3.3.2-5: a QoS 2 PUBLISH with an invalid UTF-8 payload and Payload Format Indicator 1 is PUBREC 0x99",
     case_qos2_invalid_payload)
case("MQTT-3.3.2-5: a QoS 0 PUBLISH with an invalid UTF-8 payload and Payload Format Indicator 1 is DISCONNECT 0x99",
     case_qos0_invalid_payload_disconnects)
case("MQTT 5 (control): a PUBLISH with VALID UTF-8 under Payload Format Indicator 1 is accepted and delivered",
     case_valid_payload_still_accepted)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
