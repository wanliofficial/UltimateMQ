#!/usr/bin/env python3
"""MQTT 5 QoS 2: a re-sent PUBREL after a reconnect must keep the reserved bits 0.

MQTT-3.6.1-1 (§3.6.1.1): "Bits 3,2,1 and 0 of the Fixed Header in the PUBREL
packet are reserved and MUST be set to 0,0,1 and 0 respectively. The Server MUST
treat any other value as malformed and close the Network Connection." The PUBREL
packet has NO DUP flag -- DUP is defined only for PUBLISH (§3.3.1-8); §4.4.0-1
says a resumed session resends unacknowledged PUBRELs "using their original
Packet Identifiers" but says nothing about a DUP bit.

When a subscriber is mid-QoS-2 (it has sent PUBREC, the broker has sent PUBREL,
and the subscriber has not yet sent PUBCOMP) and the connection drops, the broker
must resend that PUBREL on reconnect (Clean Start = 0). The flowmq gateway set
the PUBLISH DUP bit on the resend and OR'd it into the wire byte, emitting 0x6A
instead of 0x62 -- a malformed PUBREL a conformant client MUST reject and close,
breaking exactly-once recovery (and firing any Will as if the client died).

This probe drives the subscriber leg to the point where the broker has an
outstanding PUBREL, hard-closes the socket (no PUBCOMP), reconnects with
Clean Start = 0, and pins the first byte of the resumed PUBREL.

Usage: pubrel_dup_bit_test.py <binary> <port>
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

PUBLISH_QOS2 = 0x34
PUBREC = 0x50
PUBREL = 0x62         # correct: PUBREL, QoS-1 flags, reserved bits 0010
PUBCOMP = 0x70

REASON_SUCCESS = 0x00


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


def connect(cid, clean_start, session_expiry=60):
    flags = (0x02 if clean_start else 0x00)
    # CONNECT properties: Session Expiry Interval (0x11) = a 4-byte value.
    props = bytes([0x11]) + struct.pack("!I", session_expiry)
    body = (utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60)
            + enc_len(len(props)) + props + utf8(cid))
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=2):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_qos2(pid, topic, payload):
    body = utf8(topic) + struct.pack("!H", pid) + enc_len(0) + payload
    return bytes([PUBLISH_QOS2]) + enc_len(len(body)) + body


def pubrec(pid):
    return bytes([PUBREC]) + enc_len(2) + struct.pack("!H", pid)


def parse_publish(body):
    tlen = struct.unpack("!H", body[0:2])[0]
    off = 2 + tlen
    pid = struct.unpack("!H", body[off:off + 2])[0]
    return pid


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


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-pubrel-")
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


def case_resumed_pubrel_has_no_dup_bit():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-pubrel2-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.time() + 5
        up = False
        while time.time() < deadline and not up:
            try:
                socket.create_connection((HOST, mqtt_port), timeout=0.2).close()
                up = True
            except Exception:  # noqa: BLE001
                time.sleep(0.05)
        assert up, "server did not start"
        time.sleep(0.3)

        CID = "pubrel-dup-sub"
        topic = "pubrel/t"

        # First connection: persistent session, QoS 2 subscription.
        sub = socket.create_connection((HOST, mqtt_port), timeout=5)
        sub.sendall(connect(CID, clean_start=True))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(subscribe(1, topic, qos=2))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"

        # A publisher (separate client) drives a QoS 2 message to the topic.
        pub = socket.create_connection((HOST, mqtt_port), timeout=5)
        pub.sendall(connect("pubrel-dup-pub", clean_start=True))
        assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for publisher"
        pub.sendall(publish_qos2(20, topic, b"leg"))
        assert read_packet(pub)[0] == PUBREC, "publisher did not get PUBREC"

        # Subscriber receives the QoS 2 PUBLISH and answers PUBREC; the broker then
        # owes it a PUBREL.
        h, b = read_packet(sub)
        assert h == PUBLISH_QOS2, "subscriber did not get the QoS 2 PUBLISH: 0x%02X" % h
        rpid = parse_publish(b)
        sub.sendall(pubrec(rpid))
        h, b = read_packet(sub)
        assert h == PUBREL, "subscriber did not get the first PUBREL: 0x%02X" % h
        assert h == PUBREL and (h & 0xF8) == 0x60 and (h & 0x07) == 0x02, (
            "first PUBREL reserved bits wrong: 0x%02X" % h)

        # Hard-close WITHOUT sending PUBCOMP: the PUBREL stays outstanding.
        pub.close()
        sub.close()
        time.sleep(0.4)

        # Reconnect with Clean Start = 0 to resume the session.
        sub2 = socket.create_connection((HOST, mqtt_port), timeout=5)
        sub2.sendall(connect(CID, clean_start=False))
        assert read_packet(sub2)[0] & 0xF0 == 0x20, "no CONNACK on resume"

        # The broker must resend the outstanding PUBREL with the reserved bits
        # 0010 (0x62). A DUP bit would make it 0x6A -- malformed per MQTT-3.6.1-1.
        h, b = read_packet(sub2, 3.0)
        assert h is not None, "broker did not resend the PUBREL on resume"
        assert h & 0xF0 == 0x60, "resumed packet is not a PUBREL: 0x%02X" % h
        assert (h & 0x07) == 0x02, (
            "resumed PUBREL low bits are %d, expected reserved 0010 (bit 3 DUP set?)"
            % (h & 0x07))
        assert not (h & 0x08), (
            "resumed PUBREL 0x%02X set the reserved DUP bit (bit 3); PUBREL has no "
            "DUP flag and must be 0x62 (MQTT-3.6.1-1)" % h)
        assert struct.unpack("!H", b[0:2])[0] == rpid, "resumed PUBREL carried a new id"
        sub2.close()
        return "the resumed PUBREL is 0x62 (reserved bits 0010, no DUP)"
    finally:
        stop(proc, workdir)


case("MQTT 5 QoS 2: a PUBREL re-sent after reconnect keeps the reserved bits 0 (no DUP)",
     case_resumed_pubrel_has_no_dup_bit)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
