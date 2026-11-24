#!/usr/bin/env python3
"""MQTT 5 Response Topic must be a legal Topic Name (§4.7.3 / MQTT-4.7.3-1).

The Response Topic (property 0x08) names the Topic Name a requester wants its
response sent to. The broker treats it as a routing-relevant value, so a
publisher that puts a wildcard in it, or leaves it empty, is making the same
mistake as a wildcard Topic Name on the PUBLISH itself (MQTT-3.3.2-1). The spec
is explicit that the Response Topic must be a valid Topic Name [MQTT-4.7.3-1]:

  "The Topic Name in the Response Topic ... MUST NOT contain wildcard
   characters ... and it MUST NOT be an empty string."

A violation is a Protocol Error [MQTT-4.13.1-1]. Because PUBACK and PUBREC
define no reason code for "your Response Topic is illegal" (their reason code
sets cover topic-name-of-the-PUBLISH, not the response topic), the broker
reports it the same way for every QoS: a DISCONNECT carrying 0x82
(PROTOCOL_ERROR), and the connection ends. The message is never routed.

publish_property_forward_test.py checks that a well-formed Response Topic is
forwarded to subscribers; this probe pins the other half -- a Response Topic
that VIOLATES the Topic Name rules is refused, regardless of QoS, and a legal
one is accepted.

Usage: response_topic_validation_test.py <binary> <port>
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

PROP_RESPONSE_TOPIC = 0x08
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


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos & 0x03])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(pid, topic, payload, qos, response_topic=None):
    """MQTT 5 PUBLISH with an optional Response Topic (0x08) property."""
    props = b""
    if response_topic is not None:
        props = bytes([PROP_RESPONSE_TOPIC]) + utf8(response_topic)
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
    workdir = tempfile.mkdtemp(prefix="flowmq-rtpc-")
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


def _expect_protocol_disconnect(pub, what):
    pkt = read_packet(pub, 2.0)
    assert pkt is not None and pkt[0] & 0xF0 == 0xE0, (
        "%s: expected a DISCONNECT, got %r" % (what, pkt))
    reason = pkt[1][0] if pkt[1] else 0x00
    assert reason == REASON_PROTOCOL_ERROR, (
        "%s: Response Topic violation must be DISCONNECT 0x82 (PROTOCOL_ERROR), "
        "got 0x%02x" % (what, reason))
    return "DISCONNECT 0x82"


def case_qos0_wildcard_response_topic():
    """A QoS 0 PUBLISH whose Response Topic contains '+' is a Protocol Error:
    DISCONNECT 0x82, and the message is not routed."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        sub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("rt-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK"
            sub.sendall(connect("rt-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK"
            sub.sendall(subscribe(1, "rt/#", qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            pub.sendall(publish(0, "rt/a", b"hi", qos=0, response_topic="req/+"))
            info = _expect_protocol_disconnect(pub, "QoS 0 wildcard Response Topic")
            # The PUBLISH must not have been routed despite a live subscriber.
            assert read_packet(sub, 1.0) is None, (
                "a PUBLISH with an illegal Response Topic must not be routed")
            return info + "; not routed"
        finally:
            pub.close()
            sub.close()
            stop(proc, workdir)
    finally:
        pass


def case_qos1_hash_response_topic():
    """The same violation at QoS 1: the Response Topic '#' wildcard is also a
    Protocol Error, reported with DISCONNECT 0x82 (NOT a PUBACK -- PUBACK's reason
    codes cannot express an illegal Response Topic)."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("rt-qos1-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK"
            pub.sendall(publish(0x51, "rt/b", b"hi", qos=1, response_topic="req/#"))
            info = _expect_protocol_disconnect(pub, "QoS 1 '#' Response Topic")
            return info + " (not a PUBACK)"
        finally:
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_empty_response_topic():
    """An empty Response Topic is also illegal (§4.7.3: MUST NOT be empty):
    DISCONNECT 0x82."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("rt-empty-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK"
            pub.sendall(publish(0, "rt/c", b"hi", qos=0, response_topic=""))
            info = _expect_protocol_disconnect(pub, "empty Response Topic")
            return info
        finally:
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_legal_response_topic_accepted():
    """Control: a Response Topic that is a legal Topic Name (no wildcards,
    non-empty) is accepted -- the QoS 1 publish draws a normal PUBACK 0x00, so
    the refusals above are specifically about the illegal Response Topic."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("rt-ok-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK"
            pub.sendall(publish(0x52, "rt/d", b"hi", qos=1, response_topic="service/response"))
            header, body = read_packet(pub, 2.0)
            assert header is not None and header & 0xF0 == 0x40, (
                "a legal Response Topic must be accepted (a PUBACK), got %r" % (header,))
            reason = body[2] if len(body) >= 3 else 0x00
            assert reason == 0x00, (
                "a legal Response Topic must get PUBACK 0x00 (Success), got 0x%02x" % reason)
            return "legal Response Topic accepted (PUBBACK 0x00)"
        finally:
            pub.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT-4.7.3-1: a QoS 0 PUBLISH whose Response Topic contains a '+' wildcard is DISCONNECT 0x82 (PROTOCOL_ERROR)",
     case_qos0_wildcard_response_topic)
case("MQTT-4.7.3-1: a QoS 1 PUBLISH whose Response Topic contains a '#' wildcard is DISCONNECT 0x82, not a PUBACK",
     case_qos1_hash_response_topic)
case("MQTT-4.7.3-1: an empty Response Topic is a Protocol Error -> DISCONNECT 0x82",
     case_empty_response_topic)
case("MQTT 5 (control): a PUBLISH with a legal (non-wildcard, non-empty) Response Topic is accepted",
     case_legal_response_topic_accepted)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
