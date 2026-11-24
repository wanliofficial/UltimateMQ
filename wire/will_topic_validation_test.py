#!/usr/bin/env python3
"""MQTT 5 Will Topic must be a legal Topic Name (MQTT-3.1.2.12 / §3.1.3.1).

A Will carries a Will Topic and a Will Payload. The Will Topic is a Topic Name
-- the exact destination a PUBLISH would name -- so it is subject to the same
rule: a Topic Name MUST NOT contain wildcard characters (`+` / `#`)
[MQTT-3.3.2-1] and must not be empty. A publisher that arms a Will with such a
topic is making the same mistake as a wildcard Topic Name on a PUBLISH.

Because a Will is published by the broker itself on the client's abnormal
disconnection, the broker cannot route a Will whose topic contains a wildcard,
so the whole CONNECT is refused rather than accepted and then failed later
when the Will would fire [MQTT-3.1.2-12]:

  * MQTT 5: CONNACK with TOPIC_NAME_INVALID (0x90), then the connection is
    closed;
  * MQTT 3.1.1: its CONNACK return-code table only spans 0x00-0x05, so a v5
    reason code would be an undefined value on the wire; the connection is
    simply closed instead [MQTT-3.1.4-1].

will_delay_cancel_test.py and disconnect_will_test.py arm Wills with valid
topics; the rejection of an illegal Will topic was unguarded.

Usage: will_topic_validation_test.py <binary> <port>
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

PROP_SESSION_EXPIRY_INTERVAL = 0x11
PROP_RESPONSE_TOPIC = 0x08
REASON_SUCCESS = 0x00
REASON_PROTOCOL_ERROR = 0x82
REASON_TOPIC_NAME_INVALID = 0x90


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


def connect(cid, version=5, will_topic="will/status", will_msg=b"bye",
            session_expiry=None, will_response_topic=None):
    """MQTT CONNECT with an armed Will (QoS 1). Will QoS bits go in the CONNECT
    flags byte (bits 4-3); the Will Payload is Binary Data (2-byte length)."""
    flags = 0x04 | ((1 & 0x03) << 3)  # Will present, QoS 1
    if session_expiry is not None and version == 5:
        flags |= 0x02  # Clean Start
    props = b""
    if version == 5:
        if session_expiry is not None:
            props += bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
        body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(len(props)) + props
    else:
        flags |= 0x02  # CleanSession (3.1.1)
        body = utf8("MQTT") + bytes([4, flags]) + struct.pack("!H", 60)
    body += utf8(cid)
    will_props = b""
    if version == 5 and will_response_topic is not None:
        will_props = bytes([PROP_RESPONSE_TOPIC]) + utf8(will_response_topic)
    if version == 5:
        body += enc_len(len(will_props)) + will_props
    body += utf8(will_topic)
    body += struct.pack("!H", len(will_msg)) + will_msg
    return bytes([0x10]) + enc_len(len(body)) + body


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
    workdir = tempfile.mkdtemp(prefix="flowmq-wtc-")
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


def case_will_plus_wildcard():
    """A Will Topic with a `+` wildcard is a Protocol Error -> CONNACK 0x90, close."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("wtc-plus", will_topic="will/+/status"))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK for a `+` Will Topic"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected a CONNACK, got header 0x%02x" % h
            reason = b[1]
            assert reason == REASON_TOPIC_NAME_INVALID, (
                "a `+` in the Will Topic must be CONNACK 0x90 (TOPIC_NAME_INVALID), "
                "got 0x%02x" % reason)
            assert read_packet(c, 1.0) is None, (
                "the broker must close the connection after the CONNACK")
            return "a `+` Will Topic -> CONNACK 0x90 and close"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_will_hash_wildcard():
    """A Will Topic with a `#` wildcard is a Protocol Error -> CONNACK 0x90, close."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("wtc-hash", will_topic="will/#"))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK for a `#` Will Topic"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected a CONNACK, got header 0x%02x" % h
            reason = b[1]
            assert reason == REASON_TOPIC_NAME_INVALID, (
                "a `#` in the Will Topic must be CONNACK 0x90 (TOPIC_NAME_INVALID), "
                "got 0x%02x" % reason)
            return "a `#` Will Topic -> CONNACK 0x90 and close"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_will_empty_topic():
    """An empty Will Topic is illegal too -> CONNACK 0x90, close."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("wtc-empty", will_topic=""))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK for an empty Will Topic"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected a CONNACK, got header 0x%02x" % h
            reason = b[1]
            assert reason == REASON_TOPIC_NAME_INVALID, (
                "an empty Will Topic must be CONNACK 0x90 (TOPIC_NAME_INVALID), "
                "got 0x%02x" % reason)
            return "an empty Will Topic -> CONNACK 0x90 and close"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_will_accepted_and_armed():
    """Control: a Will with a legal topic is accepted (CONNACK 0x00), so the
    refusals above are specifically about the illegal Will topic."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("wtc-ok", will_topic="will/ok", session_expiry=60))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK for a legal Will Topic"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected a CONNACK, got header 0x%02x" % h
            reason = b[1]
            assert reason == REASON_SUCCESS, (
                "a legal Will Topic must be accepted (CONNACK 0x00), got 0x%02x" % reason)
            return "a legal Will Topic -> CONNACK 0x00 (accepted)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_will_response_topic_wildcard():
    """An illegal Will Response Topic makes the CONNECT a Protocol Error."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("wtc-response", will_response_topic="reply/#"))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK for an illegal Will Response Topic"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected a CONNACK, got header 0x%02x" % h
            assert b[1] == REASON_PROTOCOL_ERROR, (
                "an illegal Will Response Topic must be CONNACK 0x82, got 0x%02x" % b[1])
            return "wildcard Will Response Topic -> CONNACK 0x82"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_311_will_wildcard_closes():
    """MQTT 3.1.1: the same `#` Will Topic has no v5 reason code, so the broker
    just closes the connection (no usable CONNACK code) [MQTT-3.1.4-1]."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("wtc-311", version=4, will_topic="will/#"))
            # The broker must not hand back a success CONNACK. It either sends
            # a CONNACK with a 3.1.1 rejection code or just closes; both mean
            # the Will was refused. The one thing it must NOT do is accept it.
            pkt = read_packet(c, 2.0)
            if pkt is not None:
                h, b = pkt
                assert h & 0xF0 == 0x20, (
                    "3.1.1 `#` Will Topic must not be accepted, got header 0x%02x" % h)
                assert b[1] != REASON_SUCCESS, (
                    "3.1.1 `#` Will Topic must not draw CONNACK 0x00")
            assert read_packet(c, 1.0) is None, (
                "the broker must close the connection after refusing the Will")
            return "3.1.1 `#` Will Topic -> refused and connection closed"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.1.2-12: a v5 CONNECT whose Will Topic contains a `+` wildcard is CONNACK 0x90 (TOPIC_NAME_INVALID) and closed",
     case_will_plus_wildcard)
case("MQTT-3.1.2-12: a v5 CONNECT whose Will Topic contains a `#` wildcard is CONNACK 0x90 (TOPIC_NAME_INVALID)",
     case_will_hash_wildcard)
case("MQTT-3.1.2-12: a v5 CONNECT whose Will Topic is empty is CONNACK 0x90 (TOPIC_NAME_INVALID)",
     case_will_empty_topic)
case("MQTT 5 (control): a CONNECT with a legal Will Topic is accepted (CONNACK 0x00)",
     case_will_accepted_and_armed)
case("MQTT-3.3.2-14: a wildcard Will Response Topic makes the CONNECT a Protocol Error -> CONNACK 0x82",
     case_will_response_topic_wildcard)
case("MQTT-3.1.4-1: an MQTT 3.1.1 CONNECT whose Will Topic contains `#` is refused and the connection is closed",
     case_311_will_wildcard_closes)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
