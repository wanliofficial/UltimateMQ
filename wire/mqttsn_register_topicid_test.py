#!/usr/bin/env python3
"""MQTT-SN explicit REGISTER / REGACK and the publish-by-assigned-id round trip.

An MQTT-SN device maps a topic name to a short id explicitly with REGISTER
(§5.3.8); the gateway answers REGACK (§5.3.9) carrying the id it assigned. The
device then publishes with that id and no name on the wire. This is the
device-initiated counterpart to the implicit id a gateway assigns on a
subscribe-by-name (covered by mqttsn_subscribe_topicid_test.py).

Nothing exercises the gateway handler here either: MQTTSNTests.cpp is codec
round-trip only, so it checks that a REGISTER parses, never that the gateway
emits a REGACK, validates the name, or routes a later publish by the assigned
id. Two branches of Gateway::handleRegister are pinned:

  1. a valid topic name is registered and the REGACK comes back ACCEPTED with a
     non-zero assigned id and the echoed MsgId; a publish carrying only that id
     then reaches a regular MQTT subscriber on the resolved name;
  2. a topic NAME containing a wildcard ("sn/bad/#") is illegal - a topic name,
     unlike a topic filter, may not have wildcards - and the gateway must
     refuse it with REGACK INVALID_TOPIC_ID (0x02) and id 0, NOT silently
     register it.

Usage: mqttsn_register_topicid_test.py <binary> <port>
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
TCP_PORT = [int(sys.argv[2])]

CONNECT = 0x04
CONNACK = 0x05
REGISTER = 0x0A
REGACK = 0x0B
PUBLISH = 0x0C
DISCONNECT = 0x18

TIDT_NORMAL = 0b00
RETURN_ACCEPTED = 0x00
RETURN_INVALID_TOPIC_ID = 0x02


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port


SN_PORT = [free_udp_port()]


def sn_encode(msg_type, body):
    one = len(body) + 2
    if one < 256 and one != 0x01:
        return bytes([one, msg_type]) + body
    three = len(body) + 4
    assert three <= 0xFFFF
    return bytes([0x01, (three >> 8) & 0xFF, three & 0xFF, msg_type]) + body


def sn_decode_type(data):
    return data[3] if data[0] == 0x01 else data[1]


def sn_connect(client_id):
    return sn_encode(CONNECT, bytes([0x04, 0x01, 0x00, 0x00]) + client_id.encode())


def sn_register(msg_id, topic):
    # REGISTER body: TopicId(2, the client's requested id, 0 = "you assign one")
    # + MsgId(2) + Topic Name (raw bytes to end of datagram, §5.3.8).
    return sn_encode(REGISTER, struct.pack("!H", 0) + struct.pack("!H", msg_id) + topic.encode())


def sn_publish_normal(topic_id, payload):
    return sn_encode(PUBLISH, bytes([TIDT_NORMAL]) + struct.pack("!H", topic_id) + payload)


def sn_disconnect():
    return sn_encode(DISCONNECT, b"")


# --- MQTT (subscriber side) -------------------------------------------------


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


def mqtt_connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def mqtt_subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def mqtt_read_pkt(c, timeout=3.0):
    c.settimeout(timeout)
    try:
        h = c.recv(1)
    except socket.timeout:
        return None
    if not h:
        return None
    n, mult = 0, 1
    while True:
        b = c.recv(1)
        if not b:
            return None
        b = b[0]
        n += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    data = b""
    while len(data) < n:
        chunk = c.recv(n - len(data))
        if not chunk:
            return None
        data += chunk
    return (h[0], data)


def mqtt_publish_topic(body):
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    plen, mult = 0, 1
    while True:
        b = body[off]
        off += 1
        plen += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    return topic, body[off + plen:]


# --- lifecycle --------------------------------------------------------------


def start():
    workdir = tempfile.mkdtemp(prefix="flowmq-snreg-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(TCP_PORT[0])
    env["FLOWMQ_MQTT_SN_PORT"] = str(SN_PORT[0])
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            c = socket.create_connection((HOST, TCP_PORT[0]), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, workdir
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


def sn_recv(sock, timeout=2.0):
    sock.settimeout(timeout)
    try:
        data, _ = sock.recvfrom(70000)
    except socket.timeout:
        return None
    return data


def parse_regack(data):
    """Return (assigned_id, msg_id, return_code) from a REGACK datagram.

    Body is TopicId(2) + MsgId(2) + ReturnCode(1) (§5.3.9)."""
    assert sn_decode_type(data) == REGACK, "expected REGACK, got 0x%02X" % sn_decode_type(data)
    body = data[4:] if data[0] == 0x01 else data[2:]
    assigned = struct.unpack("!H", body[0:2])[0]
    msg_id = struct.unpack("!H", body[2:4])[0]
    return assigned, msg_id, body[4]


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
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))


def case_register_assigns_id_and_routes_publish():
    proc, workdir = start()
    sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sub = socket.create_connection((HOST, TCP_PORT[0]), timeout=5)
    try:
        sn.sendto(sn_connect("sn-reg"), (HOST, SN_PORT[0]))
        assert sn_recv(sn) is not None, "no CONNACK"

        sub.sendall(mqtt_connect("sn-reg-watch"))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(mqtt_subscribe(1, "sn/reg/topic", qos=0))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"

        sn.sendto(sn_register(5, "sn/reg/topic"), (HOST, SN_PORT[0]))
        ack = sn_recv(sn)
        assert ack is not None, "no REGACK"
        assigned, msg_id, code = parse_regack(ack)
        assert code == RETURN_ACCEPTED, "REGACK not ACCEPTED: 0x%02X" % code
        assert msg_id == 5, "REGACK echoed the wrong MsgId %d" % msg_id
        assert assigned != 0, "gateway assigned topic id 0 in the REGACK"

        payload = b"hello-register"
        sn.sendto(sn_publish_normal(assigned, payload), (HOST, SN_PORT[0]))
        pkt = mqtt_read_pkt(sub, timeout=3.0)
        assert pkt is not None, (
            "no PUBLISH reached the MQTT subscriber: the gateway did not map the "
            "REGISTER-assigned id back to the topic name")
        header, body = pkt
        assert header & 0xF0 == 0x30, "expected a PUBLISH, got 0x%02X" % header
        topic, forwarded = mqtt_publish_topic(body)
        assert topic == "sn/reg/topic", "forwarded to %r, expected %r" % (topic, "sn/reg/topic")
        assert forwarded == payload, "payload corrupted in transit"
        return "REGISTER assigned id %d; publish-by-id forwarded to %r" % (assigned, topic)
    finally:
        sn.close()
        sub.close()
        stop(proc, workdir)


def case_register_rejects_wildcard_topic_name():
    proc, workdir = start()
    sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sn.sendto(sn_connect("sn-bad"), (HOST, SN_PORT[0]))
        assert sn_recv(sn) is not None, "no CONNACK"

        # A topic NAME may not contain wildcards (§5.3.1); "sn/bad/#" is a filter,
        # not a valid name, so the gateway must refuse it.
        sn.sendto(sn_register(7, "sn/bad/#"), (HOST, SN_PORT[0]))
        ack = sn_recv(sn)
        assert ack is not None, "no REGACK for the invalid name"
        assigned, msg_id, code = parse_regack(ack)
        assert code == RETURN_INVALID_TOPIC_ID, (
            "an invalid topic name was not refused with INVALID_TOPIC_ID: 0x%02X" % code)
        assert assigned == 0, (
            "gateway returned id %d for an invalid name; it must be 0" % assigned)
        assert msg_id == 7, "REGACK echoed the wrong MsgId %d" % msg_id
        return "wildcard topic name refused with INVALID_TOPIC_ID (id 0)"
    finally:
        sn.close()
        stop(proc, workdir)


case("MQTT-SN REGISTER: a valid topic name is assigned an id and a publish by it is routed",
     case_register_assigns_id_and_routes_publish)
case("MQTT-SN REGISTER: a wildcard topic name is refused with INVALID_TOPIC_ID (id 0)",
     case_register_rejects_wildcard_topic_name)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
