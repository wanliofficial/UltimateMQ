#!/usr/bin/env python3
"""MQTT-SN gateway: QoS 1 PUBACK, PINGREQ/PINGRESP, and Will on abnormal disconnect.

Three MQTT-SN gateway behaviors have no wire coverage. The existing SN probes
cover the three-octet length form, REGISTER/subscribe topic-id assignment, and
publish-by-id routing, but not:

  1. a QoS 1 PUBLISH is answered with a PUBACK carrying the topic id the gateway
     knows the topic by, the message id, and the return code (§5.3.6); and the
     message is delivered to an MQTT subscriber through the bridged session;
  2. a PINGREQ from a connected peer is answered with an empty PINGRESP, which is
     how a battery-powered sensor keeps its session alive between publishes;
  3. a peer that stops pinging past 1.5x its keep-alive is reaped as an
     abnormal end, which PUBLISHES ITS WILL (§6.1). This is the gateway's
     keepalive-sweeper -> releasePeer(abnormal) -> will-publish path, exercised
     end to end: a silent device's Will must reach an MQTT subscriber.

The gateway runs on UDP (FLOWMQ_MQTT_SN_PORT) and bridges into the same broker
the MQTT subscriber is connected to over TCP.

Usage: mqttsn_qos1_ping_will_test.py <binary> <tcp-port>
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
WILLTOPICREQ = 0x06
WILLTOPIC = 0x07
WILLMSGREQ = 0x08
WILLMSG = 0x09
PUBLISH = 0x0C
PUBACK = 0x0D
SUBSCRIBE = 0x12
SUBACK = 0x13
PINGREQ = 0x16
PINGRESP = 0x17
DISCONNECT = 0x18

# Topic Id types (flag bits 1-0, §5.3.1).
TIDT_NORMAL = 0b00
TIDT_PREDEFINED = 0b01
TIDT_NAME = 0b11

# MQTT-SN return codes (§5.3.20).
RC_ACCEPTED = 0x00

RETURN_ACCEPTED = 0x00


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port


SN_PORT = [free_udp_port()]
PREDEF_ID = 0x01
PREDEF_TOPIC = "sn/predef"


# ------------------------------------------------------------------ MQTT-SN


def sn_encode(msg_type, body):
    one = len(body) + 2
    if one < 256 and one != 0x01:
        return bytes([one, msg_type]) + body
    three = len(body) + 4
    assert three <= 0xFFFF
    return bytes([0x01, (three >> 8) & 0xFF, three & 0xFF, msg_type]) + body


def sn_type(data):
    if data[0] == 0x01:
        return data[3]
    return data[1]


def sn_connect(client_id, duration, will=False, clean=True):
    flags = 0x04 if clean else 0x00
    if will:
        flags |= 0x08  # Will present
    return sn_encode(CONNECT, bytes([flags, 0x01]) + struct.pack("!H", duration) + client_id.encode())


def sn_subscribe(msg_id, topic):
    # QoS 0, Topic Name type. SUBSCRIBE carries MsgId followed by the filter text.
    return sn_encode(SUBSCRIBE, bytes([TIDT_NAME]) + struct.pack("!H", msg_id) + topic.encode())


def sn_will_topic(topic):
    # Will topic flag byte: QoS 0, no retain, topic name follows as raw bytes.
    return sn_encode(WILLTOPIC, bytes([0x00]) + topic.encode())


def sn_will_msg(payload):
    return sn_encode(WILLMSG, payload)


def sn_publish_predef(msg_id, payload):
    # Flags: QoS 1 (bits 6-5 = 0b01 -> 0x20) + predefined topic id (0b01).
    flags = 0x20 | TIDT_PREDEFINED
    return sn_encode(PUBLISH, bytes([flags]) + struct.pack("!H", PREDEF_ID) +
                     struct.pack("!H", msg_id) + payload)


def sn_pingreq(client_id):
    return sn_encode(PINGREQ, client_id.encode())


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


def mqtt_subscribe(pid, filt, qos=1):
    # One options byte: requested QoS in the low two bits.
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


def mqtt_publish_qos1(pid, topic, payload):
    body = utf8(topic) + struct.pack("!H", pid) + enc_len(0) + payload
    return bytes([0x32]) + enc_len(len(body)) + body


def mqtt_publish_qos0(topic, payload):
    body = utf8(topic) + enc_len(0) + payload
    return bytes([0x30]) + enc_len(len(body)) + body


def mqtt_puback(c, timeout=3.0):
    pkt = mqtt_read_pkt(c, timeout)
    return pkt is not None and pkt[0] & 0xF0 == 0x40


def mqtt_split_publish(header, body):
    """Split a PUBLISH body into (topic, payload). At QoS>0 a Packet Identifier
    follows the Topic Name, then the Property Length (VBI), then the payload."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    qos = (header >> 1) & 0x03
    if qos > 0:
        off += 2  # Packet Identifier
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
    workdir = tempfile.mkdtemp(prefix="flowmq-snq1-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(TCP_PORT[0])
    env["FLOWMQ_MQTT_SN_PORT"] = str(SN_PORT[0])
    env["FLOWMQ_MQTT_SN_PREDEFINED_TOPICS"] = "%d=%s" % (PREDEF_ID, PREDEF_TOPIC)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
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


def udp_socket():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.settimeout(3.0)
    return s


def case_qos1_puback_roundtrip():
    proc, workdir = start()
    try:
        # An MQTT subscriber on the bridged broker receives the SN publish.
        sub = socket.create_connection((HOST, TCP_PORT[0]), timeout=5)
        sub.sendall(mqtt_connect("sn-q1-sub"))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(mqtt_subscribe(1, PREDEF_TOPIC, qos=1))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        sn = udp_socket()
        sn.sendto(sn_connect("sn-q1-dev", 30), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == CONNACK, "no CONNACK to the SN device"

        # QoS 1 PUBLISH by the predefined topic id.
        sn.sendto(sn_publish_predef(7, b"sn-qos1"), (HOST, SN_PORT[0]))

        # The subscriber receives the bridged PUBLISH. The bridge delivered it at
        # QoS 1, so the broker expects a PUBACK for the broker-assigned packet id.
        pkt = mqtt_read_pkt(sub)
        assert pkt is not None and pkt[0] & 0xF0 == 0x30, "SN QoS 1 publish not bridged"
        topic, payload = mqtt_split_publish(pkt[0], pkt[1])
        assert (topic, payload) == (PREDEF_TOPIC, b"sn-qos1"), "%r/%r" % (topic, payload)
        # PUBACK echoes the delivered packet id (first two bytes of the body).
        delivered_pid = struct.unpack("!H", pkt[1][0:2])[0]
        sub.sendall(bytes([0x40]) + enc_len(2) + struct.pack("!H", delivered_pid))

        # The gateway answers the device's QoS 1 PUBLISH with a PUBACK.
        data = sn.recv(256)
        assert sn_type(data) == PUBACK, "no PUBACK to the SN device: 0x%02x" % sn_type(data)
        # PUBACK body: TopicId(2) + MsgId(2) + ReturnCode(1).
        assert len(data) >= 6, "PUBACK too short: %r" % data
        ack_topic = struct.unpack("!H", data[2:4])[0]
        ack_msg = struct.unpack("!H", data[4:6])[0]
        rc = data[6]
        assert ack_msg == 7, "PUBACK echoed the wrong msg id %d" % ack_msg
        assert rc == RC_ACCEPTED, "PUBACK return code 0x%02x, expected ACCEPTED" % rc

        sn.close()
        sub.close()
        return "SN QoS 1 publish bridged to MQTT and answered with PUBACK/ACCEPTED"
    finally:
        stop(proc, workdir)


def case_pingreq_gets_pingresp():
    proc, workdir = start()
    try:
        sn = udp_socket()
        sn.sendto(sn_connect("sn-ping-dev", 30), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == CONNACK, "no CONNACK"

        sn.sendto(sn_pingreq("sn-ping-dev"), (HOST, SN_PORT[0]))
        data = sn.recv(256)
        assert sn_type(data) == PINGRESP, "no PINGRESP to a PINGREQ: 0x%02x" % sn_type(data)
        # PINGRESP has an empty body (length 2 = length byte + type byte).
        assert len(data) == 2, "PINGRESP was not empty: %r" % data
        sn.close()
        return "a PINGREQ from a connected peer is answered with an empty PINGRESP"
    finally:
        stop(proc, workdir)


def case_will_published_on_abnormal_disconnect():
    proc, workdir = start()
    will_topic = "sn/will/last"
    will_payload = b"device-gone"
    try:
        # An MQTT subscriber waits on the will topic before the device connects.
        sub = socket.create_connection((HOST, TCP_PORT[0]), timeout=5)
        sub.sendall(mqtt_connect("sn-will-sub"))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x20, "no CONNACK for will subscriber"
        sub.sendall(mqtt_subscribe(1, will_topic, qos=0))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x90, "no SUBACK for will topic"

        # SN device connects WITH a Will and completes the handshake.
        sn = udp_socket()
        # keep-alive of 1s: a silent peer is reaped after ~1.5s.
        sn.sendto(sn_connect("sn-will-dev", 1, will=True), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == WILLTOPICREQ, "no WILLTOPICREQ"
        sn.sendto(sn_will_topic(will_topic), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == WILLMSGREQ, "no WILLMSGREQ"
        sn.sendto(sn_will_msg(will_payload), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == CONNACK, "no CONNACK after will handshake"

        # The device now goes silent (no PINGREQ, no DISCONNECT). The gateway's
        # keep-alive sweep must reap it as an abnormal end and publish its Will.
        deadline = time.time() + 6.0
        got = None
        while time.time() < deadline:
            pkt = mqtt_read_pkt(sub, 0.5)
            if pkt is not None and pkt[0] & 0xF0 == 0x30:
                topic, payload = mqtt_split_publish(pkt[0], pkt[1])
                if topic == will_topic:
                    got = payload
                    break
        assert got == will_payload, (
            "the device's Will was not published on abnormal disconnect: %r" % got)
        sn.close()
        sub.close()
        return "a silent peer's Will was published after the keep-alive timeout"
    finally:
        stop(proc, workdir)


def case_clean_retransmitted_connect_discards_old_session():
    proc, workdir = start()
    topic = "sn/clean/reconnect"
    try:
        publisher = socket.create_connection((HOST, TCP_PORT[0]), timeout=5)
        publisher.sendall(mqtt_connect("sn-clean-publisher"))
        assert mqtt_read_pkt(publisher)[0] & 0xF0 == 0x20, "no CONNACK for publisher"

        sn = udp_socket()
        sn.sendto(sn_connect("sn-clean-dev", 30, clean=False), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == CONNACK, "no CONNACK for persistent SN session"
        sn.sendto(sn_subscribe(1, topic), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == SUBACK, "no SUBACK for persistent SN session"

        # Same endpoint and Client Id, but CleanSession=1: this may be a lost
        # CONNACK retransmission, and §5.3.1 still requires the old session and
        # all of its subscriptions to be discarded before the new one starts.
        sn.sendto(sn_connect("sn-clean-dev", 30, clean=True), (HOST, SN_PORT[0]))
        assert sn_type(sn.recv(256)) == CONNACK, "no CONNACK for clean reconnect"
        publisher.sendall(mqtt_publish_qos1(7, topic, b"must-not-arrive"))
        assert mqtt_puback(publisher), "broker did not acknowledge the first publish"
        sn.settimeout(0.5)
        try:
            stale = sn.recv(256)
        except socket.timeout:
            stale = None
        assert stale is None, "clean reconnect retained the old subscription: %r" % stale

        # The new clean session is usable: subscribing again restores delivery.
        sn.sendto(sn_subscribe(2, topic), (HOST, SN_PORT[0]))
        sn.settimeout(3.0)
        assert sn_type(sn.recv(256)) == SUBACK, "new clean session could not subscribe"
        publisher.sendall(mqtt_publish_qos1(8, topic, b"new-session"))
        assert mqtt_puback(publisher), "broker did not acknowledge the second publish"
        delivered = sn.recv(256)
        assert sn_type(delivered) == PUBLISH, "new clean session did not receive its publish"
        assert delivered.endswith(b"new-session"), "wrong payload after clean reconnect"

        sn.close()
        publisher.close()
        return "CleanSession=1 on a retransmitted CONNECT removed the old subscription"
    finally:
        stop(proc, workdir)


case("MQTT-SN QoS 1: a publish by a predefined topic id is bridged and answered with PUBACK/ACCEPTED",
     case_qos1_puback_roundtrip)
case("MQTT-SN keep-alive: a PINGREQ from a connected peer is answered with an empty PINGRESP",
     case_pingreq_gets_pingresp)
case("MQTT-SN Will: a peer silent past its keep-alive is reaped and its Will is published",
     case_will_published_on_abnormal_disconnect)
case("MQTT-SN session: a clean retransmitted CONNECT discards the old session",
     case_clean_retransmitted_connect_discards_old_session)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
