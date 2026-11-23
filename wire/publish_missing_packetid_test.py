#!/usr/bin/env python3
"""MQTT 5: a QoS 1/2 PUBLISH with no Packet Identifier is a Protocol Error, not Malformed.

MQTT-2.2.1-3 (§2.2.1.3): in the Variable Header of a PUBLISH whose QoS Level is
greater than 0, a Packet Identifier field is present and the packet is malformed
if it is not. A QoS 1 PUBLISH that carries only a Topic Name and no Packet
Identifier is otherwise well-formed -- it breaks a rule rather than the encoding
-- so it must end the connection with DISCONNECT 0x82 (PROTOCOL_ERROR), not
0x81 (MALFORMED_PACKET).

The codec read the id when present but never required it: parsePublish called
parsePacketId, whose "not present" failure is a generic std::runtime_error that
falls through to the catch-all and is reported as MALFORMED_PACKET. This probe
sends a QoS 1 PUBLISH with a valid Topic Name and no Packet Identifier and pins
the reason code.

Usage: publish_missing_packetid_test.py <binary> <port>
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

REASON_MALFORMED_PACKET = 0x81
REASON_PROTOCOL_ERROR = 0x82


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


def connect(cid, clean_start=True):
    flags = 0x02 if clean_start else 0x00
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def publish_no_packet_id(topic, qos=1):
    # QoS 1 PUBLISH: fixed header 0x32, Remaining Length = Topic Name only, then
    # the Topic Name -- and no Packet Identifier, which QoS > 0 requires.
    body = utf8(topic)
    return bytes([0x30 | (qos << 1)]) + enc_len(len(body)) + body


def publish_with_packet_id(topic, packet_id, qos=1):
    # MQTT 5: after the Packet Identifier a PUBLISH carries a Properties length;
    # a well-formed packet here has the zero-length Properties byte (0x00).
    body = utf8(topic) + struct.pack("!H", packet_id) + b"\x00"
    return bytes([0x30 | (qos << 1)]) + enc_len(len(body)) + body


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
    try:
        header = recv_exact(sock, 1)[0]
    except (socket.timeout, EOFError):
        return None
    mult = 1
    val = 0
    while True:
        try:
            b = recv_exact(sock, 1)[0]
        except (socket.timeout, EOFError):
            return None
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = recv_exact(sock, val) if val else b""
    return header, body


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-pid-")
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


def case_missing_packet_id_is_protocol_error():
    proc, mqtt_port, workdir = start()
    try:
        c = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            c.sendall(connect("pid-pub"))
            connack = read_packet(c, 2.0)
            assert connack is not None and connack[0] & 0xF0 == 0x20, (
                "no CONNACK: %r" % (connack,))
            # QoS 1 PUBLISH with a valid Topic Name but no Packet Identifier.
            c.sendall(publish_no_packet_id("pid/topic", qos=1))
            disc = read_packet(c, 2.0)
            assert disc is not None, "broker did not answer the malformed PUBLISH"
            header, body = disc
            assert header & 0xF0 == 0xE0, (
                "expected a DISCONNECT, got header 0x%02x" % header)
            assert body, "DISCONNECT with an empty body carries no reason code"
            reason = body[0]
            assert reason == REASON_PROTOCOL_ERROR, (
                "QoS>0 PUBLISH without a Packet Identifier must be PROTOCOL_ERROR "
                "(0x%02x), got 0x%02x" % (REASON_PROTOCOL_ERROR, reason))
            return "a QoS 1 PUBLISH with no Packet Identifier is answered DISCONNECT 0x82 (PROTOCOL_ERROR)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_qos1_with_packet_id_still_accepted():
    """Control: a well-formed QoS 1 PUBLISH (Packet Identifier present) is
    accepted with a PUBACK, so the new requirement did not break the normal path."""
    proc, mqtt_port, workdir = start()
    try:
        c = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            c.sendall(connect("pid-pub-ok"))
            assert read_packet(c, 2.0) is not None, "no CONNACK"
            c.sendall(publish_with_packet_id("pid/ok", 1, qos=1))
            r = read_packet(c, 2.0)
            assert r is not None and r[0] & 0xF0 == 0x40, (
                "no PUBACK for a well-formed QoS 1 PUBLISH: %r" % (r,))
            return "a well-formed QoS 1 PUBLISH is still accepted with a PUBACK"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT 5: a QoS 1 PUBLISH with no Packet Identifier is answered DISCONNECT 0x82 (PROTOCOL_ERROR)",
     case_missing_packet_id_is_protocol_error)
case("MQTT 5 (control): a well-formed QoS 1 PUBLISH with a Packet Identifier still gets a PUBACK",
     case_qos1_with_packet_id_still_accepted)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
