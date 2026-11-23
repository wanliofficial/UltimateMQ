#!/usr/bin/env python3
"""MQTT-SN gateway: a SHORT topic Name must not carry wildcard characters.

MQTT-SN §5.3.1: a SHORT topic is a two-byte Topic Name. An MQTT Topic Name in a
PUBLISH MUST NOT contain wildcard characters (MQTT-3.3.2-1 / §1.5.3); the MQTT
path already enforces this for a full topic-name publish via
Topic::isValidTopicName(), and the gateway enforces it for a SUBSCRIBE with a
SHORT topic via Topic::isValid(). But the PUBLISH path resolved a SHORT topic's
two bytes straight into the routed topic name with no validation, so a device
could publish to the literal name "++" (or "#x", or a name containing a NUL)
which the rest of the broker treats as an illegal topic Name.

The test: an SN device sends a QoS 1 PUBLISH whose two SHORT-topic bytes are
'++' (0x2B 0x2B). The gateway must reject it with a PUBACK carrying
INVALID_TOPIC_ID, exactly as it already rejects the same two bytes as a
SUBSCRIBE filter (the control case). Before the fix the PUBLISH was accepted
with PUBACK ACCEPTED and the malformed name was routed.

Usage: mqttsn_short_topic_test.py <binary> <tcp-port>
Exit code is 0 only when every case passes.
"""
import os
import signal
import socket
import struct
import subprocess
import sys
import time

HOST = "127.0.0.1"
BIN = sys.argv[1]
TCP_PORT = [int(sys.argv[2])]

SN_PORT = [0]

T_PUBLISH = 0x0C
T_PUBACK = 0x0D
T_SUBSCRIBE = 0x12
T_SUBACK = 0x13

TIDT_SHORT = 0b10
RETURN_ACCEPTED = 0x00
RETURN_INVALID_TOPIC_ID = 0x02
RETURN_NOT_SUPPORTED = 0x03

results = []


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port


def sn_encode(msg_type, body):
    one = len(body) + 2
    if one < 256 and one != 0x01:
        return bytes([one, msg_type]) + body
    three = len(body) + 4
    assert three <= 0xFFFF
    return bytes([0x01, (three >> 8) & 0xFF, three & 0xFF, msg_type]) + body


def sn_type(data):
    return data[3] if data[0] == 0x01 else data[1]


def sn_body(data):
    return data[4:] if data[0] == 0x01 else data[2:]


def sn_recv(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        data, _ = sock.recvfrom(2048)
    except socket.timeout:
        return None
    return data


def sn_connect(client_id, duration=30):
    return sn_encode(0x04, bytes([0x00, 0x01]) + struct.pack("!H", duration) + client_id.encode())


def sn_publish_short(short_topic, msg_id, payload=b"x"):
    # Flags: QoS 1 (0x20) | TopicIdType SHORT (0b10) -> 0x22.
    flags = 0x20 | TIDT_SHORT
    body = bytes([flags]) + struct.pack("!H", short_topic) + struct.pack("!H", msg_id) + payload
    return sn_encode(T_PUBLISH, body)


def sn_subscribe_short(short_topic, msg_id):
    # Flags: QoS 0 | TopicIdType SHORT (0b10) -> 0x02.
    flags = 0x02
    body = bytes([flags]) + struct.pack("!H", msg_id) + struct.pack("!H", short_topic)
    return sn_encode(T_SUBSCRIBE, body)


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


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
    mult = 1
    val = 0
    while True:
        b = c.recv(1)
        if not b:
            return None
        val += (b[0] & 127) * mult
        mult *= 128
        if not b[0] & 128:
            break
    body = c.recv(val) if val else b""
    return h[0], body


class Broker:
    def __init__(self, tcp_port, sn_port):
        self.tcp_port = tcp_port
        self.sn_port = sn_port
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env["MQTT_PORT"] = str(self.tcp_port)
        env["FLOWMQ_MQTT_SN_PORT"] = str(self.sn_port)
        self.proc = subprocess.Popen([BIN], env=env, cwd=os.path.dirname(BIN),
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                c = socket.create_connection((HOST, self.tcp_port), timeout=0.2)
                c.close()
                time.sleep(0.3)
                return
            except Exception:  # noqa: BLE001
                time.sleep(0.05)
        self.proc.kill()
        raise RuntimeError("server did not start")

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        time.sleep(0.3)


def case(name, fn):
    try:
        info = fn()
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
        return
    except Exception as exc:  # noqa: BLE001
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        return
    results.append((name, True))
    print("PASS " + name + ((" :: " + info) if info else ""))


def _connect(sn, dest):
    sn.sendto(sn_connect("sn-short"), dest)
    assert sn_type(sn_recv(sn)) == 0x05, "no CONNACK to the SN device"


def case_short_publish_wildcard_rejected():
    TCP_PORT[0] += 1
    SN_PORT[0] = free_udp_port()
    b = Broker(TCP_PORT[0], SN_PORT[0])
    b.start()
    try:
        sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sn.settimeout(3)
        dest = (HOST, SN_PORT[0])
        _connect(sn, dest)

        # QoS 1 PUBLISH on a SHORT topic whose two bytes are '++' (a wildcard
        # the MQTT Topic Name must never contain).
        WILDCARD_SHORT = 0x2B2B
        sn.sendto(sn_publish_short(WILDCARD_SHORT, 5, b"x"), dest)
        reply = sn_recv(sn)
        assert reply is not None, "gateway did not answer the SHORT PUBLISH"
        assert sn_type(reply) == T_PUBACK, (
            "expected a PUBACK for a QoS 1 PUBLISH, got msg type 0x%02x" % sn_type(reply))
        body = sn_body(reply)
        rc = body[4]
        assert rc == RETURN_INVALID_TOPIC_ID, (
            "SHORT topic '++' PUBLISH return code 0x%02x, expected INVALID_TOPIC_ID "
            "(0x%02x): an invalid topic Name was accepted and routed"
            % (rc, RETURN_INVALID_TOPIC_ID))
        sn.close()
        return "a SHORT-topic PUBLISH carrying '++' is rejected PUBACK+INVALID_TOPIC_ID"
    finally:
        b.stop()


def case_short_subscribe_wildcard_rejected_control():
    """Control: the same two bytes as a SUBSCRIBE filter are already rejected
    (NOT_SUPPORTED), proving the PUBLISH accept is the outlier, not intent."""
    TCP_PORT[0] += 1
    SN_PORT[0] = free_udp_port()
    b = Broker(TCP_PORT[0], SN_PORT[0])
    b.start()
    try:
        sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sn.settimeout(3)
        dest = (HOST, SN_PORT[0])
        _connect(sn, dest)

        WILDCARD_SHORT = 0x2B2B
        sn.sendto(sn_subscribe_short(WILDCARD_SHORT, 6), dest)
        reply = sn_recv(sn)
        assert reply is not None, "gateway did not answer the SHORT SUBSCRIBE"
        assert sn_type(reply) == T_SUBACK, (
            "expected a SUBACK, got msg type 0x%02x" % sn_type(reply))
        # SUBACK body: flags (1) + topic id (2) + msg id (2) + return code (1).
        body = sn_body(reply)
        rc = body[5]
        assert rc == RETURN_NOT_SUPPORTED, (
            "SHORT filter '++' SUBSCRIBE return code 0x%02x, expected NOT_SUPPORTED"
            % rc)
        sn.close()
        return "the same SHORT '++' bytes as a SUBSCRIBE are already rejected (control)"
    finally:
        b.stop()


case("MQTT-SN: a SHORT-topic PUBLISH carrying wildcard bytes is rejected PUBACK+INVALID_TOPIC_ID",
     case_short_publish_wildcard_rejected)
case("MQTT-SN (control): the same SHORT bytes as a SUBSCRIBE are already rejected",
     case_short_subscribe_wildcard_rejected_control)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
