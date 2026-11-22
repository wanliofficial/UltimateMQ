#!/usr/bin/env python3
"""MQTT-SN gateway topic-id assignment and the publish-by-assigned-id round trip.

When an MQTT-SN client subscribes by Topic Name (Topic Id type NAME, §5.3.1),
the gateway allocates a NORMAL Topic Id for that name and returns it in the
SUBACK (§5.3.10). The client then publishes with that short id instead of
resending the name, and the gateway maps the id back to the name
(Gateway::handlePublish -> Peer::topicNameFor) before routing to the broker.

This is the core of the MQTT-SN gateway's value over plain MQTT: it lets a
constrained device stop sending full topic strings. None of it is covered:

  * MQTTSNTests.cpp is entirely codec round-trip tests (encode/decode of a
    datagram in isolation) - it never runs a Gateway, so it cannot see the
    name->id->name mapping;
  * the one MQTT-SN wire probe (mqttsn_large_publish_test.py) uses a
    PREDEFINED topic id, which is deployment-wide and needs no gateway
    assignment, so it too never exercises the allocation table.

This probe drives a real gateway end to end over UDP:

  1. a client CONNECTs (clean session) and SUBSCRIBEs by name "sn/room/temp";
  2. the SUBACK must come back ACCEPTED and carry a non-zero assigned id;
  3. the client PUBLISHes by that assigned NORMAL id (no name on the wire),
     and a regular MQTT subscriber must receive it on "sn/room/temp" unchanged -
     proof the gateway resolved the id back to the name and routed it;
  4. control: re-subscribing by the same name returns the SAME assigned id, so
     the table reuses entries rather than leaking a new id each time.

Usage: mqttsn_subscribe_topicid_test.py <binary> <port>
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

# MQTT-SN message types (§5.2.2).
CONNECT = 0x04
CONNACK = 0x05
PUBLISH = 0x0C
SUBSCRIBE = 0x12
SUBACK = 0x13
DISCONNECT = 0x18

# MQTT-SN Topic Id types (low two flag bits, §5.3.1).
TIDT_NORMAL = 0b00
TIDT_PREDEFINED = 0b01
TIDT_NAME = 0b11

RETURN_ACCEPTED = 0x00

TOPIC_NAME = "sn/room/temp"


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port


SN_PORT = [free_udp_port()]


# ------------------------------------------------------------------ MQTT-SN


def sn_encode(msg_type, body):
    """Encode a datagram per §5.2.1 (length counts itself, 1- or 3-octet form)."""
    one = len(body) + 2
    if one < 256 and one != 0x01:
        return bytes([one, msg_type]) + body
    three = len(body) + 4
    assert three <= 0xFFFF
    return bytes([0x01, (three >> 8) & 0xFF, three & 0xFF, msg_type]) + body


def sn_decode_type(data):
    """Return the MsgType byte, handling both length forms."""
    if data[0] == 0x01:
        return data[3]
    return data[1]


def sn_connect(client_id):
    # Flags: CleanSession (0x04); ProtocolId 1; Duration 0.
    return sn_encode(CONNECT, bytes([0x04, 0x01, 0x00, 0x00]) + client_id.encode())


def sn_subscribe_name(msg_id, topic):
    # Flags: Topic Id type NAME (0b11), QoS 0 -> 0x03. The Topic Name follows the
    # MsgId as raw bytes to the end of the datagram (MQTT-SN Topic Name is NOT
    # length-prefixed; see Gateway::handleSubscribe / the SUBSCRIBE parser).
    flags = TIDT_NAME
    return sn_encode(SUBSCRIBE, bytes([flags]) + struct.pack("!H", msg_id) + topic.encode())


def sn_publish_normal(topic_id, payload):
    # Flags: Topic Id type NORMAL (0b00), QoS 0 -> 0x00. Topic id, then data.
    return sn_encode(PUBLISH, bytes([TIDT_NORMAL]) + struct.pack("!H", topic_id) + payload)


def sn_disconnect():
    return sn_encode(DISCONNECT, b"")


# ---------------------------------------------------------------------- MQTT


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
    """Split an MQTT 5 QoS 0 PUBLISH body into (topic, payload)."""
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


# ---------------------------------------------------------------- lifecycle


def start():
    workdir = tempfile.mkdtemp(prefix="flowmq-sntid-")
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


def parse_suback(data):
    """Return (assigned_id, msg_id, return_code) from a SUBACK datagram.

    Body is flags(1) + TopicId(2) + MsgId(2) + ReturnCode(1) (§5.3.10)."""
    assert sn_decode_type(data) == SUBACK, "expected SUBACK, got 0x%02X" % sn_decode_type(data)
    if data[0] == 0x01:
        body = data[4:]
    else:
        body = data[2:]
    assigned = struct.unpack("!H", body[1:3])[0]
    msg_id = struct.unpack("!H", body[3:5])[0]
    return_code = body[5]
    return assigned, msg_id, return_code


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


def case_subscribe_assigns_id_then_publish_uses_it():
    proc, workdir = start()
    sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sub = socket.create_connection((HOST, TCP_PORT[0]), timeout=5)
    try:
        sn.sendto(sn_connect("sn-reg"), (HOST, SN_PORT[0]))
        assert sn_recv(sn) is not None, "no CONNACK"

        # Regular MQTT subscriber for the topic the gateway will assign an id to.
        sub.sendall(mqtt_connect("sn-tid-watch"))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(mqtt_subscribe(1, TOPIC_NAME, qos=0))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"

        # Subscribe by Topic Name; the gateway must assign a NORMAL id.
        sn.sendto(sn_subscribe_name(1, TOPIC_NAME), (HOST, SN_PORT[0]))
        ack = sn_recv(sn)
        assert ack is not None, "no SUBACK for the MQTT-SN subscribe"
        assigned, msg_id, code = parse_suback(ack)
        assert code == RETURN_ACCEPTED, (
            "SUBACK was not ACCEPTED: return code 0x%02X" % code)
        assert msg_id == 1, "SUBACK echoed the wrong MsgId %d" % msg_id
        assert assigned != 0, "gateway assigned topic id 0 (§5.3.10 needs a real id)"

        # Publish using ONLY the assigned id - no topic name on the wire.
        payload = b"21.5"
        sn.sendto(sn_publish_normal(assigned, payload), (HOST, SN_PORT[0]))

        # The MQTT subscriber must receive it on the resolved topic name.
        pkt = mqtt_read_pkt(sub, timeout=3.0)
        assert pkt is not None, (
            "no PUBLISH reached the MQTT subscriber: the gateway did not map the "
            "assigned topic id back to %r" % TOPIC_NAME)
        header, body = pkt
        assert header & 0xF0 == 0x30, "expected a PUBLISH, got 0x%02X" % header
        topic, forwarded = mqtt_publish_topic(body)
        assert topic == TOPIC_NAME, "forwarded to %r, expected %r" % (topic, TOPIC_NAME)
        assert forwarded == payload, "payload corrupted in transit"
        return "subscribed by name, gateway assigned id %d, publish-by-id forwarded" % assigned
    finally:
        sn.close()
        sub.close()
        stop(proc, workdir)


def case_resubscribe_reuses_same_id():
    proc, workdir = start()
    sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sn.sendto(sn_connect("sn-reuse"), (HOST, SN_PORT[0]))
        assert sn_recv(sn) is not None, "no CONNACK"

        sn.sendto(sn_subscribe_name(1, TOPIC_NAME), (HOST, SN_PORT[0]))
        first = parse_suback(sn_recv(sn))[0]

        sn.sendto(sn_subscribe_name(2, TOPIC_NAME), (HOST, SN_PORT[0]))
        second = parse_suback(sn_recv(sn))[0]

        assert first != 0 and second != 0, "gateway handed out id 0"
        assert first == second, (
            "re-subscribing by the same name returned a different id (%d then %d); "
            "the gateway should reuse the existing mapping" % (first, second))
        return "re-subscribe reused id %d" % first
    finally:
        sn.close()
        stop(proc, workdir)


case("MQTT-SN gateway: a subscribe-by-name assigns a topic id, and a publish by that id is routed",
     case_subscribe_assigns_id_then_publish_uses_it)
case("MQTT-SN gateway: re-subscribing by the same name reuses the same assigned id",
     case_resubscribe_reuses_same_id)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
