#!/usr/bin/env python3
"""MQTT-SN gateway: a QoS 2 PUBLISH that must be rejected is answered with a
PUBACK carrying the ReturnCode, not a bare PUBREC.

MQTT-SN v1.2 §6.6: "Regardless of the requested QoS level the client may
receive in response to its PUBLISH a PUBACK message which contains either
ReturnCode = 'Rejection: invalid topic Id' ... or 'Rejection: congestion'."
PUBACK/PUBREC/PUBREL/PUBCOMP are the QoS 1/2 acknowledgement flow (§5.3.6,
§5.4.13-14), and only PUBACK has a ReturnCode field - PUBREC/PUBREL/PUBCOMP
carry none. So a rejected PUBLISH at ANY QoS must be signalled with a PUBACK
whose ReturnCode says why.

The gateway already does this correctly for a QoS 1 PUBLISH to an unknown topic
id (it answers PUBACK + INVALID_TOPIC_ID), and the native-MQTT path answers a
rejected QoS 2 PUBLISH with a PUBREC carrying a >= 0x80 reason (after which the
client sends no PUBREL). But the MQTT-SN QoS 2 reject path used the bare PUBREC
branch - byte-for-byte identical to the PUBACK it sends on a *successful* QoS 2
publish - so a client could not tell rejection from acceptance. Worse, the
client would then complete PUBREL -> PUBCOMP and believe its message was
delivered exactly-once, while the gateway had routed it to nobody.

The test: an SN device sends a QoS 2 PUBLISH on a NORMAL topic id it never
REGISTERed (so the gateway cannot resolve the name). The gateway must reply
with a PUBACK whose ReturnCode is INVALID_TOPIC_ID (0x02). It must NOT be a
PUBREC. A MQTT subscriber on the broker, and even a PUBREL/PUBCOMP round trip
afterwards, must never see the payload.

Usage: mqttsn_qos2_reject_test.py <binary> <tcp-port>
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

SN_PORT = [0]

CONNECT = 0x04
CONNACK = 0x05
PUBLISH = 0x0C

# Authoritative MQTT-SN 1.2 message type codes (§5.3.2 table). PUBLISH=0x0C,
# PUBACK=0x0D, PUBCOMP=0x0E, PUBREC=0x0F, PUBREL=0x10 - only PUBACK has a
# ReturnCode field; PUBREC/PUBREL/PUBCOMP carry none.
T_CONNACK = 0x05
T_PUBLISH = 0x0C
T_PUBACK = 0x0D
T_PUBCOMP = 0x0E
T_PUBREC = 0x0F
T_PUBREL = 0x10

TIDT_NORMAL = 0b00

RETURN_ACCEPTED = 0x00
RETURN_INVALID_TOPIC_ID = 0x02

results = []


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port


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


def sn_encode(msg_type, body):
    one = len(body) + 2
    if one < 256 and one != 0x01:
        return bytes([one, msg_type]) + body
    three = len(body) + 4
    assert three <= 0xFFFF
    return bytes([0x01, (three >> 8) & 0xFF, three & 0xFF, msg_type]) + body


def sn_type(data):
    return data[3] if data[0] == 0x01 else data[1]


def sn_recv(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        data, _ = sock.recvfrom(2048)
    except socket.timeout:
        return None
    return data


def sn_connect(client_id, duration=30, will=False):
    flags = 0x00
    if will:
        flags |= 0x08
    return sn_encode(0x04, bytes([flags, 0x01]) + struct.pack("!H", duration) + client_id.encode())


def sn_publish_qos2_normal(topic_id, msg_id, payload=b"data"):
    # Flags: QoS 2 (bits 6-5 = 0b10 -> 0x40), TopicIdType NORMAL (bits 1-0 = 0b00).
    flags = 0x40 | TIDT_NORMAL
    body = bytes([flags]) + struct.pack("!H", topic_id) + struct.pack("!H", msg_id) + payload
    return sn_encode(T_PUBLISH, body)


def sn_pubrel(msg_id):
    return sn_encode(T_PUBREL, struct.pack("!H", msg_id))


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


def mqtt_subscribe(pid, filt, qos=2):
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
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))


def case_qos2_reject_returns_puback_with_code():
    TCP_PORT[0] += 1
    SN_PORT[0] = free_udp_port()
    b = Broker(TCP_PORT[0], SN_PORT[0])
    b.start()
    try:
        # A subscriber on the bridged broker, watching for any leak.
        sub = socket.create_connection((HOST, b.tcp_port), timeout=5)
        sub.sendall(mqtt_connect("sn-q2-sub"))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(mqtt_subscribe(1, "sn/q2/#", qos=2))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sn.settimeout(3)
        dest = (HOST, SN_PORT[0])
        sn.sendto(sn_connect("sn-q2-dev"), dest)
        assert sn_type(sn_recv(sn)) == T_CONNACK, "no CONNACK to the SN device"

        # QoS 2 PUBLISH on a NORMAL topic id that was never REGISTERed.
        UNMAPPED_ID = 0x2A
        sn.sendto(sn_publish_qos2_normal(UNMAPPED_ID, 7, b"secret"), dest)

        # The gateway must reject with a PUBACK carrying INVALID_TOPIC_ID,
        # NOT a bare PUBREC (which would be identical to an acceptance ack).
        reply = sn_recv(sn)
        assert reply is not None, "gateway did not answer the QoS 2 PUBLISH"
        assert sn_type(reply) == T_PUBACK, (
            "expected a PUBACK rejection for a QoS 2 PUBLISH to an unknown "
            "topic id, got msg type 0x%02x (a bare PUBREC is indistinguishable "
            "from a QoS 2 acceptance)" % sn_type(reply))
        # PUBACK body: topic id (2) + msg id (2) + return code (1).
        assert len(reply[2 if reply[0] != 0x01 else 4:]) >= 5, "short PUBACK body"
        body = reply[4:] if reply[0] == 0x01 else reply[2:]
        topic_id, msg_id, rc = struct.unpack("!HHB", body[0:5])
        assert msg_id == 7, "PUBACK msg id 0x%04x, expected 7" % msg_id
        assert rc == RETURN_INVALID_TOPIC_ID, (
            "PUBACK return code 0x%02x, expected INVALID_TOPIC_ID (0x%02x)"
            % (rc, RETURN_INVALID_TOPIC_ID))

        # Even if the client completes the QoS 2 handshake (PUBREL -> PUBCOMP),
        # the rejected payload must never reach a subscriber.
        sn.sendto(sn_pubrel(7), dest)
        comp = sn_recv(sn)
        # The gateway may answer PUBCOMP (harmless) or nothing; either way the
        # payload below must not be delivered. We only assert no leak on sub.
        leak = mqtt_read_pkt(sub, timeout=1.5)
        assert leak is None, (
            "the rejected QoS 2 payload leaked to a subscriber: %r" % (leak,))
        sub.close()
        return "a rejected QoS 2 PUBLISH is answered with PUBACK + INVALID_TOPIC_ID and never routed"
    finally:
        try:
            sn.close()
        except Exception:  # noqa: BLE001
            pass
        b.stop()


case("MQTT-SN: a QoS 2 PUBLISH to an unknown topic id is rejected with PUBACK+ReturnCode, not a bare PUBREC",
     case_qos2_reject_returns_puback_with_code)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
