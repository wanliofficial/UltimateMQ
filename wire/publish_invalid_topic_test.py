#!/usr/bin/env python3
"""MQTT-3.3.2-1 / §2.2.2.6: a PUBLISH whose Topic Name is illegal (a wildcard)
is refused with the matching reason code, and the message is never routed.

In MQTT a Topic Name -- the destination a PUBLISH names -- MUST NOT contain
wildcard characters; wildcards are only legal in Topic Filters (SUBSCRIBE /
UNSUBSCRIBE) [MQTT-3.3.2-1]. The broker rejects such a PUBLISH, but how depends
on the QoS and the protocol version, because each acknowledgement packet
carries its own legal reason codes (§3.4.2.1, §3.5.2.1):

  * QoS 1  -> PUBACK with TOPIC_NAME_INVALID (0x90);
  * QoS 2  -> PUBREC with TOPIC_NAME_INVALID (0x90);
  * QoS 0  -> no ack exists, so on MQTT 5 the broker sends a DISCONNECT carrying
    TOPIC_NAME_INVALID (a server MAY end a QoS 0 publish with DISCONNECT,
    MQTT-5.4.1-1); on MQTT 3.1.1 it is silently dropped.

The refusal happens before routing, so a subscriber on a filter the illegal
topic would otherwise match receives nothing.

This pins the QoS 1 and QoS 2 reason codes on the live TCP path (the 3.1.1
QoS 0 drop is covered by mqtt311_test.py; the ACL NOT_AUTHORIZED variants by
sys_topics_test.py).

Usage: publish_invalid_topic_test.py <binary> <port>
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

REASON_TOPIC_NAME_INVALID = 0x90

PUBLISH_QOS0 = 0x30
PUBLISH_QOS1 = 0x32
PUBLISH_QOS2 = 0x34
PUBACK = 0x40
PUBREC = 0x50
DISCONNECT = 0xE0


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


def subscribe(pid, filt, qos=2):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(pid, topic, payload, qos, reason=None):
    # Variable Header order the broker parses: Topic Name, Packet Identifier
    # (QoS > 0), Properties (MQTT 5), then payload.
    hdr = PUBLISH_QOS0 | (qos << 1)
    body = utf8(topic)
    if qos > 0:
        body += struct.pack("!H", pid)
    body += enc_len(0)  # property length (MQTT 5)
    body += payload
    return bytes([hdr]) + enc_len(len(body)) + body


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
    workdir = tempfile.mkdtemp(prefix="flowmq-pit-")
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


def _expect(reply, label, want_header, want_reason):
    assert reply is not None, "broker sent no reply (%s)" % label
    h, b = reply
    assert h & 0xF0 == want_header, (
        "expected header 0x%02X for %s, got 0x%02X" % (want_header, label, h))
    pid = struct.unpack("!H", b[0:2])[0]
    reason = b[2] if len(b) > 2 else 0x00
    assert reason == want_reason, (
        "%s: expected reason 0x%02X, got 0x%02X (body %r)"
        % (label, want_reason, reason, b))
    return pid, reason


def case_qos1_invalid_topic_puback_not_found():
    """A QoS 1 PUBLISH to a topic name containing a '+' wildcard is answered
    PUBACK 0x90 (TOPIC_NAME_INVALID), and the payload is not delivered to a
    subscriber on a filter the topic would have matched."""
    proc, port, workdir = start()
    pub = socket.create_connection((HOST, port), timeout=5)
    sub = socket.create_connection((HOST, port), timeout=5)
    try:
        pub.sendall(connect("pit-qos1-pub"))
        assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (publisher)"
        sub.sendall(connect("pit-qos1-sub"))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
        sub.sendall(subscribe(1, "pit/#", qos=2))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        # Topic name "pit/a/+/b" carries '+', which is illegal in a Topic Name.
        pub.sendall(publish(7, "pit/a/+/b", b"x", qos=1))
        h, b = read_packet(pub)
        _expect((h, b), "QoS 1 invalid-topic publish", PUBACK, REASON_TOPIC_NAME_INVALID)

        # The refused message must not reach the subscriber.
        try:
            extra = read_packet(sub, 1.0)
        except Exception:  # noqa: BLE001
            extra = None
        assert extra is None or extra[0] & 0xF0 != 0x30, (
            "the invalid-topic PUBLISH must not be routed, got %r" % (extra,))
        return "QoS 1 PUBLISH to a wildcard topic name -> PUBACK 0x90, not routed"
    finally:
        pub.close()
        sub.close()
        stop(proc, workdir)


def case_qos2_invalid_topic_pubrec_not_found():
    """A QoS 2 PUBLISH to a topic name containing a '#' wildcard is answered
    PUBREC 0x90 (TOPIC_NAME_INVALID); the exactly-once flow never completes."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("pit-qos2"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(publish(9, "pit/#", b"y", qos=2))
            h, b = read_packet(c)
            _expect((h, b), "QoS 2 invalid-topic publish", PUBREC, REASON_TOPIC_NAME_INVALID)
            return "QoS 2 PUBLISH to a wildcard topic name -> PUBREC 0x90"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_qos0_invalid_topic_disconnect_v5():
    """On MQTT 5 a QoS 0 PUBLISH to an illegal topic name is ended with a
    DISCONNECT carrying TOPIC_NAME_INVALID (no ack packet exists at QoS 0)."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("pit-qos0"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(publish(0, "pit/a/#", b"z", qos=0))
            h, b = read_packet(c)
            assert h is not None, "broker closed with no DISCONNECT (QoS 0 invalid topic)"
            assert h & 0xF0 == DISCONNECT, (
                "expected DISCONNECT for a QoS 0 invalid-topic publish, got 0x%02X" % h)
            assert b and b[0] == REASON_TOPIC_NAME_INVALID, (
                "QoS 0 invalid-topic must be DISCONNECT 0x90, got %r" % b)
            return "QoS 0 PUBLISH to a wildcard topic name -> DISCONNECT 0x90"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_qos1_valid_topic_still_accepted():
    """Control: the same publisher with a legal topic name is accepted with
    PUBACK/SUCCESS, so the 0x90 above is specific to the wildcard and not to
    QoS 1 publishing in general."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("pit-ok"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(publish(11, "pit/ok", b"w", qos=1))
            h, b = read_packet(c)
            assert h is not None and h & 0xF0 == PUBACK, (
                "a valid QoS 1 PUBLISH must get a PUBACK, got %r" % (h,))
            reason = b[2] if len(b) > 2 else 0x00
            assert reason == 0x00, "valid QoS 1 PUBLISH must be PUBACK/SUCCESS, got 0x%02X" % reason
            return "a valid QoS 1 PUBLISH is still accepted with PUBACK/SUCCESS"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.3.2-1: a QoS 1 PUBLISH to a topic name with a '+' wildcard is answered PUBACK 0x90 (TOPIC_NAME_INVALID) and not routed",
     case_qos1_invalid_topic_puback_not_found)
case("MQTT-3.3.2-1: a QoS 2 PUBLISH to a topic name with a '#' wildcard is answered PUBREC 0x90 (TOPIC_NAME_INVALID)",
     case_qos2_invalid_topic_pubrec_not_found)
case("MQTT-3.3.2-1: a QoS 0 PUBLISH to an illegal topic name is ended with DISCONNECT 0x90 (TOPIC_NAME_INVALID)",
     case_qos0_invalid_topic_disconnect_v5)
case("MQTT 5 (control): a QoS 1 PUBLISH with a legal topic name is still accepted with PUBACK/SUCCESS",
     case_qos1_valid_topic_still_accepted)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
