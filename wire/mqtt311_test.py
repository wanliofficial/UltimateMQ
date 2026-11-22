#!/usr/bin/env python3
"""MQTT 3.1.1 (protocol level 4) over the wire.

Every other wire probe speaks MQTT 5, but the broker also implements MQTT 3.1.1,
which is a genuinely different wire path with its own rules and no wire coverage
at all:

  * no Properties section anywhere (CONNECT, SUBSCRIBE, PUBLISH, acks carry only
    their fixed/variable header);
  * a failed subscription is reported with the byte 0x80 in the SUBACK
    (MQTT-3.8.4-1), not a reason code, and the connection stays open;
  * a QoS 0 PUBLISH that violates the spec (e.g. an invalid topic name) is
    silently dropped, with no DISCONNECT (3.1.1 has no reason codes to report it
    with), and the connection stays usable;
  * the Clean Session flag (not a Session Expiry Interval) governs persistence.

This probe launches the real broker and pins the 3.1.1 path end to end, so a
regression that accidentally applies MQTT 5 framing or reason codes to a 3.1.1
client is caught on the wire.

Usage: mqtt311_test.py <binary> <port>
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

LEVEL = 4  # MQTT 3.1.1

# Reason/failure codes as used by 3.1.1.
FAILURE_80 = 0x80


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


def connect(cid, clean_session=True):
    # 3.1.1 CONNECT: protocol name, level 4, flags (clean session bit 0x02),
    # keep alive, client id. NO property section.
    flags = 0x02 if clean_session else 0x00
    body = utf8("MQTT") + bytes([LEVEL, flags]) + struct.pack("!H", 60) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0):
    # 3.1.1 SUBSCRIBE: packet id, (filter, requested qos) pairs. NO properties.
    body = struct.pack("!H", pid) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_qos0(topic, payload):
    body = utf8(topic) + payload
    return bytes([0x30]) + enc_len(len(body)) + body


def publish_qos1(pid, topic, payload):
    body = utf8(topic) + struct.pack("!H", pid) + payload
    return bytes([0x32]) + enc_len(len(body)) + body


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def read_packet(sock, timeout=3.0):
    sock.settimeout(timeout)
    header = recv_exact(sock, 1)[0]
    mult = 1
    val = 0
    while True:
        b = recv_exact(sock, 1)[0]
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = recv_exact(sock, val) if val else b""
    return header, body


def parse_publish(body):
    # 3.1.1 QoS 0 PUBLISH body: topic (len-prefixed) then payload. No properties.
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    return topic, body[2 + tlen:]


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-311-")
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
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))


def case_311_basic_roundtrip():
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("s311"))
            header, body = read_packet(sub)
            assert header & 0xF0 == 0x20, "no CONNACK: %r" % header
            # 3.1.1 CONNACK: byte 0 = connect flags, byte 1 = return code (0x00).
            assert body[1] == 0x00, "3.1.1 CONNACK return code 0x%02x" % body[1]

            pub.sendall(connect("p311"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for publisher"

            sub.sendall(subscribe(1, "t311", qos=0))
            header, body = read_packet(sub)
            assert header & 0xF0 == 0x90, "no SUBACK: %r" % header
            # 3.1.1 SUBACK granted QoS is the low byte 0x00 (not a reason code).
            assert body[0:2] == struct.pack("!H", 1), "SUBACK echoed the wrong pid"
            assert body[2] == 0x00, "granted QoS was 0x%02x, expected 0x00" % body[2]

            # QoS 0 publish: delivered with no ack and no property section.
            pub.sendall(publish_qos0("t311", b"hello311"))
            header, body = read_packet(sub)
            assert header & 0xF0 == 0x30, "no PUBLISH delivered: %r" % header
            topic, payload = parse_publish(body)
            assert (topic, payload) == ("t311", b"hello311"), "%r/%r" % (topic, payload)

            # QoS 1 publish: PUBACK carries only the packet id (no reason code).
            pub.sendall(publish_qos1(7, "t311", b"q1"))
            header, body = read_packet(pub)
            assert header & 0xF0 == 0x40, "no PUBACK: %r" % header
            assert body[0:2] == struct.pack("!H", 7), "PUBACK pid %r" % body[0:2]
            assert len(body) == 2, (
                "3.1.1 PUBACK must be exactly a packet id, got %d bytes" % len(body))
            return "3.1.1 CONNECT/SUBACK/PUBLISH/PUBACK round-trip with no properties"
        finally:
            sub.close()
            pub.close()
    finally:
        stop(proc, workdir)


def case_311_invalid_filter_reports_0x80_and_stays_open():
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("s311bad"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"

            # "a/#b" is an invalid topic filter (# must occupy its own level).
            c.sendall(subscribe(2, "a/#b", qos=0))
            header, body = read_packet(c)
            assert header & 0xF0 == 0x90, "no SUBACK for the invalid filter: %r" % header
            assert body[0:2] == struct.pack("!H", 2), "SUBACK echoed the wrong pid"
            assert body[2] == FAILURE_80, (
                "3.1.1 must report an invalid filter as 0x80, got 0x%02x" % body[2])

            # The connection must still be alive: a following valid SUBSCRIBE is
            # answered, not dropped. (An MQTT 5 broker would not even reach here for
            # some of these; 3.1.1 keeps the session and reports per-filter.)
            c.sendall(subscribe(3, "ok311", qos=1))
            header, body = read_packet(c)
            assert header & 0xF0 == 0x90, "connection died after the invalid filter"
            assert body[2] == 0x01, "valid re-subscribe granted 0x%02x, expected 0x01" % body[2]
            return "invalid filter -> SUBACK 0x80 and the connection stays usable"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_311_qos0_invalid_topic_is_silently_dropped():
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("s311drop"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
            sub.sendall(subscribe(1, "keep311", qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"

            pub.sendall(connect("p311drop"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for publisher"

            # A QoS 0 PUBLISH to an invalid topic name (wildcard not allowed in a
            # name). 3.1.1 has no DISCONNECT for this, so it is dropped silently.
            pub.sendall(publish_qos0("bad/#name", b"x"))

            # The subscriber is never served the bad topic, and the publisher's
            # connection stays open (a later valid publish still lands).
            pub.sendall(publish_qos0("keep311", b"after"))
            header, body = read_packet(sub, timeout=3.0)
            assert header & 0xF0 == 0x30, (
                "no valid PUBLISH after the invalid one (connection may have been "
                "dropped): %r" % header)
            topic, payload = parse_publish(body)
            assert (topic, payload) == ("keep311", b"after"), "%r/%r" % (topic, payload)
            return "QoS 0 invalid topic silently dropped; connection stayed usable"
        finally:
            sub.close()
            pub.close()
    finally:
        stop(proc, workdir)


case("MQTT 3.1.1: a CONNECT/SUBSCRIBE/PUBLISH(QoS 0,1)/PUBACK round-trip with no properties",
     case_311_basic_roundtrip)
case("MQTT 3.1.1: an invalid topic filter is reported as 0x80 and the connection stays open",
     case_311_invalid_filter_reports_0x80_and_stays_open)
case("MQTT 3.1.1: a QoS 0 publish to an invalid topic is silently dropped, connection stays open",
     case_311_qos0_invalid_topic_is_silently_dropped)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
