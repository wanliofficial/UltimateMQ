#!/usr/bin/env python3
"""MQTT-3.3.2-4 / §3.2.2.5: application properties survive a PUBLISH round-trip.

When a broker forwards a PUBLISH to a subscriber, a defined set of application
properties MUST be carried through unchanged [MQTT-3.3.2-4]: Payload Format
Indicator, Content Type, Response Topic, Correlation Data and User Property.
Each is a property of the application message, not of the connection, so a
subscriber must see exactly what the publisher sent.

Three further properties are deliberately NOT forwarded, because they are not
about the message as delivered:
  * Topic Alias (0x23) is per-connection (MQTT-3.3.2-8): the publisher's alias
    table has no meaning to the subscriber, so it must not leak through;
  * Subscription Identifier is a property of the subscriber's own subscription;
  * Message Expiry Interval is rewritten into an absolute deadline rather than
    passed on as an interval.

This pins the forwarding set on the live TCP path: the publisher attaches
Response Topic, Correlation Data, Content Type, Payload Format Indicator and a
User Property, and the subscriber must receive all five byte-for-byte while the
Topic Alias it did not set must be absent.

Usage: publish_property_forward_test.py <binary> <port>
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

PROP_PAYLOAD_FORMAT_INDICATOR = 0x01
PROP_CONTENT_TYPE = 0x03
PROP_RESPONSE_TOPIC = 0x08
PROP_CORRELATION_DATA = 0x09
PROP_TOPIC_ALIAS = 0x23
PROP_USER_PROPERTY = 0x26


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


def prop_str(pid, value):
    return bytes([pid]) + utf8(value)


def prop_bytes(pid, value):
    return bytes([pid]) + struct.pack("!H", len(value)) + value


def prop_u8(pid, value):
    return bytes([pid]) + bytes([value])


def prop_u16(pid, value):
    return bytes([pid]) + struct.pack("!H", value)


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=1):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_with_props(pid, topic, payload, properties):
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


def parse_publish_props(body):
    """Return (topic, packet_id, {prop_id: [values]}, payload) from a v5
    PUBLISH body (Topic Name, Packet Id, Properties, payload)."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    pid = struct.unpack("!H", body[off:off + 2])[0]
    off += 2
    plen = 0
    mult = 1
    while True:
        x = body[off]
        off += 1
        plen += (x & 127) * mult
        mult *= 128
        if not x & 128:
            break
    pend = off + plen
    props = {}
    while off < pend:
        pid_ = body[off]
        off += 1
        if pid_ == PROP_PAYLOAD_FORMAT_INDICATOR:
            props.setdefault(pid_, []).append(body[off])
            off += 1
        elif pid_ in (PROP_CONTENT_TYPE, PROP_RESPONSE_TOPIC):
            slen = struct.unpack("!H", body[off:off + 2])[0]
            props.setdefault(pid_, []).append(body[off + 2:off + 2 + slen].decode())
            off += 2 + slen
        elif pid_ == PROP_CORRELATION_DATA:
            dlen = struct.unpack("!H", body[off:off + 2])[0]
            props.setdefault(pid_, []).append(body[off + 2:off + 2 + dlen])
            off += 2 + dlen
        elif pid_ == PROP_USER_PROPERTY:
            klen = struct.unpack("!H", body[off:off + 2])[0]
            key = body[off + 2:off + 2 + klen].decode()
            off += 2 + klen
            vlen = struct.unpack("!H", body[off:off + 2])[0]
            val = body[off + 2:off + 2 + vlen].decode()
            off += 2 + vlen
            props.setdefault(pid_, []).append((key, val))
        elif pid_ == PROP_TOPIC_ALIAS:
            props.setdefault(pid_, []).append(struct.unpack("!H", body[off:off + 2])[0])
            off += 2
        else:
            raise AssertionError("unexpected property id 0x%02X in forwarded PUBLISH" % pid_)
    payload = body[pend:]
    return topic, pid, props, payload


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-pprops-")
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


def case_application_properties_forwarded():
    """A PUBLISH carrying Response Topic, Correlation Data, Content Type, Payload
    Format Indicator and a User Property is delivered to the subscriber with all
    five intact."""
    proc, port, workdir = start()
    pub = socket.create_connection((HOST, port), timeout=5)
    sub = socket.create_connection((HOST, port), timeout=5)
    try:
        pub.sendall(connect("pp-pub"))
        assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (publisher)"
        sub.sendall(connect("pp-sub"))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
        sub.sendall(subscribe(1, "pp/topic", qos=1))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        # User Property (0x26) is a UTF-8 string pair: id + key + value.
        props = (prop_u8(PROP_PAYLOAD_FORMAT_INDICATOR, 1)
                 + prop_str(PROP_CONTENT_TYPE, "application/json")
                 + prop_str(PROP_RESPONSE_TOPIC, "req/answer")
                 + prop_bytes(PROP_CORRELATION_DATA, b"corr-1234")
                 + bytes([PROP_USER_PROPERTY]) + utf8("trace") + utf8("abc"))

        pub.sendall(publish_with_props(7, "pp/topic", b'{"k":1}', props))
        # Publisher's PUBACK.
        h, b = read_packet(pub)
        assert h is not None and h & 0xF0 == PUBACK, "no PUBACK for publisher"

        h, b = read_packet(sub)
        assert h is not None and h & 0xF0 == 0x30, (
            "expected PUBLISH to subscriber, got header 0x%02X" % (h or -1))
        topic, pid, rprops, payload = parse_publish_props(b)
        assert topic == "pp/topic", "forwarded on wrong topic %r" % topic
        # The packet id is per-direction (publisher->broker 7, broker->
        # subscriber a fresh id), so it is not compared across the two legs.
        assert pid != 0, "forwarded PUBLISH carried no packet id"
        assert payload == b'{"k":1}', "payload altered: %r" % payload

        assert rprops.get(PROP_PAYLOAD_FORMAT_INDICATOR) == [1], (
            "Payload Format Indicator not forwarded: %r" % rprops)
        assert rprops.get(PROP_CONTENT_TYPE) == ["application/json"], (
            "Content Type not forwarded: %r" % rprops)
        assert rprops.get(PROP_RESPONSE_TOPIC) == ["req/answer"], (
            "Response Topic not forwarded: %r" % rprops)
        assert rprops.get(PROP_CORRELATION_DATA) == [b"corr-1234"], (
            "Correlation Data not forwarded: %r" % rprops)
        assert rprops.get(PROP_USER_PROPERTY) == [("trace", "abc")], (
            "User Property not forwarded: %r" % rprops)
        return "Response Topic, Correlation Data, Content Type, Payload Format and User Property all forwarded"
    finally:
        pub.close()
        sub.close()
        stop(proc, workdir)


def case_topic_alias_not_forwarded():
    """Control: a Topic Alias on the published message is per-connection
    (MQTT-3.3.2-8) and must NOT appear on the subscriber's copy."""
    props = (prop_str(PROP_RESPONSE_TOPIC, "req/answer")
             + prop_u16(PROP_TOPIC_ALIAS, 1))
    proc, port, workdir = start()
    pub = socket.create_connection((HOST, port), timeout=5)
    sub = socket.create_connection((HOST, port), timeout=5)
    try:
        pub.sendall(connect("pp-alias-pub"))
        assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (publisher)"
        sub.sendall(connect("pp-alias-sub"))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
        sub.sendall(subscribe(1, "pp/alias", qos=1))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        pub.sendall(publish_with_props(8, "pp/alias", b"payload", props))
        read_packet(pub)  # PUBACK

        h, b = read_packet(sub)
        assert h is not None and h & 0xF0 == 0x30, (
            "expected PUBLISH to subscriber, got header 0x%02X" % (h or -1))
        topic, pid, rprops, payload = parse_publish_props(b)
        assert PROP_TOPIC_ALIAS not in rprops, (
            "the per-connection Topic Alias must not be forwarded, got %r" % rprops)
        assert rprops.get(PROP_RESPONSE_TOPIC) == ["req/answer"], (
            "the other application property should still be forwarded: %r" % rprops)
        return "Topic Alias was not forwarded; Response Topic still was"
    finally:
        pub.close()
        sub.close()
        stop(proc, workdir)


case("MQTT-3.3.2-4: a PUBLISH forwards Response Topic, Correlation Data, Content Type, Payload Format Indicator and User Property unchanged",
     case_application_properties_forwarded)
case("MQTT-3.3.2-8: a per-connection Topic Alias on a PUBLISH is not forwarded to the subscriber",
     case_topic_alias_not_forwarded)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
