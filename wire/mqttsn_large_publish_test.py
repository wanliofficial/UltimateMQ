#!/usr/bin/env python3
"""MQTT-SN 1.2 §5.2.1: the Length field's three-octet form.

§5.2.1 says the Length field "specifies the total number of octets contained in
the message (including the Length field itself). If the first octet of the
Length field is coded 0x01 then the Length field is 3-octet long; in this case,
the two following octets specify the total number of octets of the message".

Two mistakes are easy to make here and both were present in this gateway before
this probe existed:

  * using 0xFF as the marker instead of 0x01. 0xFF is the legal one-octet length
    255, so a receiver following the spec reads a 0xFF-prefixed datagram as a
    255-octet message and rejects a longer one (and never sees 0xFF as a marker
    at all);
  * counting the message as the one-octet form would, omitting the two extra
    length octets. The declared total is then 2 less than the datagram, so a
    conformant peer rejects the message as malformed.

Both show up only for messages of 256 octets or more, so a probe that stays
under 255 octets cannot see them. This probe therefore publishes a payload that
forces the three-octet form and checks that the gateway accepts it (routing it to
a regular MQTT subscriber), which requires the produced datagram to be readable
by anyone who implements §5.2.1.

The gateway is started with FLOWMQ_MQTT_SN_PORT plus a predefined topic id, so
the PUBLISH can be sent by topic id with no REGISTER round trip.

Usage: mqttsn_large_publish_test.py <binary> <port>
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
# TCP port comes from the runner; the UDP MQTT-SN port is derived from it so the
# two never collide across probes.
TCP_PORT = [int(sys.argv[2])]
SN_PORT = [TCP_PORT[0] + 1000]

# MQTT-SN message types (§5.2.2).
CONNECT = 0x04
CONNACK = 0x05
PUBLISH = 0x0C
PUBACK = 0x0D
SUBSCRIBE = 0x12
SUBACK = 0x13
PINGREQ = 0x16
PINGRESP = 0x17
DISCONNECT = 0x18

PREDEFINED_TOPIC_ID = 1
PREDEFINED_TOPIC_NAME = "sn/large"


# ------------------------------------------------------------------ MQTT-SN


def sn_encode(msg_type, body):
    """Encode a datagram exactly as §5.2.1 specifies (the reference for this
    probe, deliberately written from the spec text rather than from the C++)."""
    total1 = len(body) + 2          # length octet + MsgType
    total3 = len(body) + 4          # three length octets + MsgType
    if total1 <= 0xFF and total1 != 0x01:
        return bytes([total1, msg_type]) + body
    assert total3 <= 0xFFFF
    return bytes([0x01, (total3 >> 8) & 0xFF, total3 & 0xFF, msg_type]) + body


def sn_connect(client_id):
    # Flags: CleanSession; ProtocolId 1; Duration 0 (no keep-alive expiry).
    return sn_encode(CONNECT, bytes([0x04, 0x01, 0x00, 0x00]) + client_id.encode())


def sn_publish_qos0(topic_id, payload):
    # Flags 0x01: QoS 0, TopicIdType PREDEFINED (0b01, §5.3.1). TopicId, then Data.
    return sn_encode(PUBLISH, bytes([0x01]) + struct.pack("!H", topic_id) + payload)


def sn_disconnect():
    return sn_encode(DISCONNECT, b"")


def sn_parse_length(data):
    """Return (declared_total, header_octets) per §5.2.1."""
    if data[0] == 0x01:
        assert len(data) >= 3
        return ((data[1] << 8) | data[2]), 3
    return data[0], 1


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


def mqtt_read_pkt(c, timeout=2.0):
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


def mqtt_publish_parts(body):
    """Split an MQTT 5 PUBLISH body into (topic, payload).

    A QoS 0 PUBLISH body is: topic (UTF-8), then the Property Length as a
    Variable Byte Integer (§3.3.2.3.2), then the properties, then the payload.
    Skipping the property length is what makes a parse look one octet long."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    # Property Length (Variable Byte Integer); this broker sends no properties on
    # a forwarded publish, so it is a single 0x00, but decode it properly.
    plen, mult = 0, 1
    while True:
        b = body[off]
        off += 1
        plen += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    off += plen
    return topic, body[off:]


# ---------------------------------------------------------------- lifecycle


def start():
    workdir = tempfile.mkdtemp(prefix="flowmq-mqttsn-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(TCP_PORT[0])
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    env["FLOWMQ_MQTT_SN_PORT"] = str(SN_PORT[0])
    env["FLOWMQ_MQTT_SN_PREDEFINED_TOPICS"] = "%d=%s" % (
        PREDEFINED_TOPIC_ID, PREDEFINED_TOPIC_NAME)
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
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()


def case_large_publish_is_delivered():
    """A PUBLISH over 255 octets must use the three-octet Length and reach a
    regular MQTT subscriber unchanged. This is the end-to-end consequence of the
    encoder and decoder agreeing on §5.2.1."""
    proc, workdir = start()
    sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sn.sendto(sn_connect("sn-large"), (HOST, SN_PORT[0]))
        reply = sn_recv(sn)
        assert reply is not None, "no CONNACK"
        declared, hdr = sn_parse_length(reply)
        assert declared == len(reply), (
            "CONNACK length field %d does not match the %d-octet datagram "
            "(§5.2.1: the Length includes itself)" % (declared, len(reply)))
        assert reply[hdr] == CONNACK, "expected CONNACK, got type 0x%02X" % reply[hdr]

        # Regular MQTT subscriber for the predefined topic.
        sub = socket.create_connection((HOST, TCP_PORT[0]), timeout=5)
        sub.sendall(mqtt_connect("sn-watch"))
        connack = mqtt_read_pkt(sub)
        assert connack and connack[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(mqtt_subscribe(1, PREDEFINED_TOPIC_NAME, qos=0))
        suback = mqtt_read_pkt(sub)
        assert suback and suback[0] & 0xF0 == 0x90, "no SUBACK"

        # 300 payload octets: the PUBLISH body is 303 octets, so the datagram is
        # 307 octets and the three-octet Length form is required.
        payload = bytes((i % 251) for i in range(300))
        datagram = sn_publish_qos0(PREDEFINED_TOPIC_ID, payload)
        assert len(datagram) > 255, "probe payload must force the long form"
        declared, hdr = sn_parse_length(datagram)
        assert declared == len(datagram), (
            "probe bug: declared %d != datagram %d" % (declared, len(datagram)))
        sn.sendto(datagram, (HOST, SN_PORT[0]))

        # The gateway answers a QoS 0 PUBLISH with nothing, so the evidence that
        # it accepted the datagram is the forwarded MQTT PUBLISH.
        pkt = mqtt_read_pkt(sub, timeout=3.0)
        assert pkt is not None, (
            "no PUBLISH reached the subscriber: the gateway likely rejected the "
            "307-octet PUBLISH (a broker that ignores the §5.2.1 three-octet "
            "Length drops every message of 256 octets or more)")
        header, body = pkt
        assert header & 0xF0 == 0x30, "expected a PUBLISH, got 0x%02X" % header
        topic, forwarded = mqtt_publish_parts(body)
        assert topic == PREDEFINED_TOPIC_NAME, (
            "forwarded to %r, expected %r" % (topic, PREDEFINED_TOPIC_NAME))
        assert forwarded == payload, (
            "payload corrupted in transit: %d octets forwarded, %d sent"
            % (len(forwarded), len(payload)))

        # The gateway stays healthy after the large datagram: a PINGREQ is
        # answered with PINGRESP.
        sn.sendto(sn_encode(PINGREQ, b""), (HOST, SN_PORT[0]))
        pong = sn_recv(sn)
        assert pong is not None, "no PINGRESP after a large PUBLISH"
        declared, hdr = sn_parse_length(pong)
        assert declared == len(pong), "PINGRESP length field does not match the datagram"
        assert pong[hdr] == PINGRESP, "expected PINGRESP, got type 0x%02X" % pong[hdr]

        sn.sendto(sn_disconnect(), (HOST, SN_PORT[0]))
        sub.close()
        return "307-octet PUBLISH (three-octet Length) forwarded intact to %r" % topic
    finally:
        sn.close()
        stop(proc, workdir)


def case_small_publish_still_uses_one_octet_length():
    """Control: a PUBLISH under 256 octets keeps the one-octet form, so the
    long-form fix did not break the ordinary path."""
    proc, workdir = start()
    sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        sn.sendto(sn_connect("sn-small"), (HOST, SN_PORT[0]))
        assert sn_recv(sn) is not None, "no CONNACK"

        sub = socket.create_connection((HOST, TCP_PORT[0]), timeout=5)
        sub.sendall(mqtt_connect("sn-watch-small"))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(mqtt_subscribe(1, PREDEFINED_TOPIC_NAME, qos=0))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x90, "no SUBACK"

        payload = b"small"
        datagram = sn_publish_qos0(PREDEFINED_TOPIC_ID, payload)
        assert datagram[0] != 0x01, "probe bug: short PUBLISH used the long form"
        declared, hdr = sn_parse_length(datagram)
        assert declared == len(datagram) and hdr == 1, (
            "one-octet Length must equal the datagram size (%d vs %d)"
            % (declared, len(datagram)))
        sn.sendto(datagram, (HOST, SN_PORT[0]))

        pkt = mqtt_read_pkt(sub, timeout=3.0)
        assert pkt is not None, "short PUBLISH was not forwarded"
        header, body = pkt
        assert header & 0xF0 == 0x30, "expected a PUBLISH, got 0x%02X" % header
        _topic, forwarded = mqtt_publish_parts(body)
        assert forwarded == payload, "payload corrupted in transit"

        sn.sendto(sn_disconnect(), (HOST, SN_PORT[0]))
        sub.close()
        return "short PUBLISH still uses the one-octet Length"
    finally:
        sn.close()
        stop(proc, workdir)


case("MQTT-SN §5.2.1: a PUBLISH of 256+ octets uses the three-octet Length (marker 0x01) and is delivered",
     case_large_publish_is_delivered)
case("MQTT-SN §5.2.1 (control): a short PUBLISH keeps the one-octet Length",
     case_small_publish_still_uses_one_octet_length)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
