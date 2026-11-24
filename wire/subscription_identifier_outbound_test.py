#!/usr/bin/env python3
"""MQTT-3.3.4-3 / MQTT-3.3.4-4: the Subscription Identifier travels outbound.

A Subscription Identifier is set by the subscriber (in the SUBSCRIBE properties,
§3.8.2.1.2) and identifies the subscription that caused a PUBLISH to be sent.
The server MUST include it in the PUBLISH it forwards to that subscriber
[MQTT-3.3.4-3], and when one delivered copy satisfies several subscriptions it
MUST carry the identifier of *all* of them [MQTT-3.3.4-4] (repeats kept, because
two subscriptions may share a value).

publish_subscription_id_test.py covers the *inbound* rejection of a client that
wrongly sends one; this pins the outbound side, which no wire probe drove:

  * a subscription carrying Subscription Identifier 300 (a multi-byte Variable
    Byte Integer, so the encoder's width handling is exercised) gets that exact
    identifier back on the forwarded PUBLISH;
  * two matching subscriptions with distinct identifiers (7 and 300) produce a
    single PUBLISH carrying both -- the combined-delivery rule;
  * control: a subscription with no identifier yields a forwarded PUBLISH that
    has no Subscription Identifier at all.

Usage: subscription_identifier_outbound_test.py <binary> <port>
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

PROP_SUBSCRIPTION_IDENTIFIER = 0x0B


def vbi(value):
    """Encode an int as a MQTT Variable Byte Integer."""
    out = b""
    while True:
        d = value % 128
        value //= 128
        if value:
            d |= 0x80
        out += bytes([d])
        if not value:
            return out


def dec_vbi(buf, off):
    """Decode a Variable Byte Integer; return (value, next_offset)."""
    mult = 1
    val = 0
    while True:
        b = buf[off]
        off += 1
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            return val, off


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + b"\x00" + utf8(cid)
    return bytes([0x10]) + vbi(len(body)) + body


def subscribe(pid, filt, qos=0, identifier=None):
    # SUBSCRIBE body: Packet Id, Properties, then (topic filter + options).
    props = b""
    if identifier is not None:
        props = bytes([PROP_SUBSCRIPTION_IDENTIFIER]) + vbi(identifier)
    body = struct.pack("!H", pid) + vbi(len(props)) + props + utf8(filt) + bytes([qos & 0x03])
    return bytes([0x82]) + vbi(len(body)) + body


def publish_qos0(topic, payload):
    # QoS 0: no packet id. Body: topic, properties (empty), payload.
    body = utf8(topic) + b"\x00" + payload
    return bytes([0x30]) + vbi(len(body)) + body


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


def parse_publish(body):
    """Return (topic, subscription_identifiers, payload) from a v5 QoS 0 PUBLISH
    body. Only 0x0B is decoded; other properties are skipped by their length."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    plen, off = dec_vbi(body, off)
    pend = off + plen
    identifiers = []
    while off < pend:
        pid = body[off]
        off += 1
        if pid == PROP_SUBSCRIPTION_IDENTIFIER:
            value, off = dec_vbi(body, off)
            identifiers.append(value)
        else:
            raise AssertionError("unexpected property id 0x%02X in forwarded PUBLISH" % pid)
    payload = body[pend:]
    return topic, identifiers, payload


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-subid-out-")
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


def _run_delivery(sub_filters, topic, payload):
    """sub_filters: list of (filter, identifier) to subscribe on one client.
    Returns the identifiers on the single forwarded PUBLISH."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("sio-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
            pub.sendall(connect("sio-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (publisher)"
            for i, (filt, ident) in enumerate(sub_filters):
                sub.sendall(subscribe(i + 1, filt, qos=0, identifier=ident))
                assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK for %r" % filt

            pub.sendall(publish_qos0(topic, payload))
            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "expected a forwarded PUBLISH, got %r" % (pkt,))
            _, identifiers, got_payload = parse_publish(pkt[1])
            assert got_payload == payload, "payload altered: %r" % got_payload
            return identifiers
        finally:
            sub.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_single_identifier_forwarded():
    """A subscription with Subscription Identifier 300 (multi-byte VBI) gets that
    exact identifier on the forwarded PUBLISH."""
    ids = _run_delivery([("sio/one", 300)], "sio/one", b"hello")
    assert ids == [300], "expected [300] on the forwarded PUBLISH, got %r" % (ids,)
    return "forwarded PUBLISH carried Subscription Identifier 300 (two-byte VBI)"


def case_all_matching_identifiers_in_one_copy():
    """Two matching subscriptions (7 and 300) fold into one PUBLISH carrying
    both identifiers [MQTT-3.3.4-4]."""
    ids = _run_delivery([("sio/+", 7), ("sio/#", 300)], "sio/x", b"multi")
    assert sorted(ids) == [7, 300], (
        "one combined copy must carry both identifiers {7, 300}, got %r" % (ids,))
    return "one combined PUBLISH carried both identifiers (7 and 300)"


def case_no_identifier_when_unset():
    """Control: a subscription with no Subscription Identifier yields a forwarded
    PUBLISH with no Subscription Identifier property."""
    ids = _run_delivery([("sio/none", None)], "sio/none", b"plain")
    assert ids == [], (
        "a subscription without an identifier must not add one, got %r" % (ids,))
    return "forwarded PUBLISH carried no Subscription Identifier"


case("MQTT-3.3.4-3: a subscription's Subscription Identifier (300, multi-byte VBI) is included on the forwarded PUBLISH",
     case_single_identifier_forwarded)
case("MQTT-3.3.4-4: one delivered copy carries the identifiers of every matching subscription (7 and 300)",
     case_all_matching_identifiers_in_one_copy)
case("MQTT 5 (control): a subscription with no Subscription Identifier adds none to the forwarded PUBLISH",
     case_no_identifier_when_unset)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
