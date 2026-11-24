#!/usr/bin/env python3
"""MQTT 4.3.3-5: a QoS 2 PUBREL/PUBREC for an identifier that never started a
flow is answered with PACKET_IDENTIFIER_NOT_FOUND (0x92), not silently accepted.

The QoS 2 four-way handshake pairs each PUBREL with the PUBREC the broker sent,
and each PUBCOMP with the PUBREL it sent. A client that sends a PUBREL (or a
PUBCOMP) for an identifier the broker never associated with an outstanding QoS 2
delivery -- a stale retransmit after a restart, a duplicated datagram, or a
plain bug -- must not be told the flow completed. MQTT-4.3.3-5 expects the broker
to report PACKET_IDENTIFIER_NOT_FOUND (0x92) for such an unrecognised identifier
rather than answering SUCCESS as if a real flow had been there.

Two directions are pinned:

  * an inbound PUBREL with no matching awaiting-PUBREL slot -> PUBCOMP carrying
    0x92 (Connection::handlePubrel -> Session::pubrel);
  * an inbound PUBREC for an id that is not an outstanding inflight delivery ->
    PUBREL carrying 0x92 (Connection::handlePubrec -> Session::pubrec).

On MQTT 5 the broker serialises a non-SUCCESS reason as
Packet Id (2) + Reason Code (1) + Property Length (1) = Remaining Length 4, so
the body is [pid_hi, pid_lo, 0x92, 0x00].

Usage: qos2_unknown_id_test.py <binary> <port>
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

PUBLISH_QOS2 = 0x34   # PUBLISH, QoS 2 (0x30 | (2 << 1))
PUBREC = 0x50
PUBREL = 0x62         # PUBREL, reserved bits 0010 (0x40 | 0x02)
PUBCOMP = 0x70

REASON_SUCCESS = 0x00
REASON_PACKET_ID_NOT_FOUND = 0x92


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


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=2):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def pubrec(pid, reason=REASON_SUCCESS):
    # A PUBREC carrying an explicit (non-SUCCESS) reason code is three bytes of
    # Variable Header: Packet Id (2) + Reason Code (1). The short RL=2 form would
    # read as reason 0x00 (SUCCESS), which is what the unknown-id test wants to
    # stay away from, so always spell the reason out.
    return bytes([PUBREC]) + enc_len(3) + struct.pack("!H", pid) + bytes([reason])


def pubrel(pid):
    return bytes([PUBREL]) + enc_len(2) + struct.pack("!H", pid)


def pubcomp(pid):
    return bytes([PUBCOMP]) + enc_len(2) + struct.pack("!H", pid)


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


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-qos2uid-")
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


def case_unknown_pubrel_answers_pubcomp_not_found():
    """A PUBREL for an id with no outstanding QoS 2 flow gets PUBCOMP 0x92."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("qos2-pubrel-stray"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            # No PUBLISH was ever sent on id 0x1234, so there is no matching
            # awaiting-PUBREL slot; the broker must answer PACKET_ID_NOT_FOUND.
            c.sendall(pubrel(0x1234))
            h, b = read_packet(c)
            assert h == PUBCOMP, "expected PUBCOMP to a stray PUBREL, got 0x%02X" % h
            pid = struct.unpack("!H", b[0:2])[0]
            reason = b[2] if len(b) > 2 else REASON_SUCCESS
            assert pid == 0x1234, "PUBCOMP carried pid 0x%04X" % pid
            assert reason == REASON_PACKET_ID_NOT_FOUND, (
                "a PUBREL for an identifier that started no QoS 2 flow must be "
                "answered PUBCOMP 0x92 (MQTT-4.3.3-5), got 0x%02X" % reason)
            return "a stray PUBREL is answered PUBCOMP 0x92 (PACKET_IDENTIFIER_NOT_FOUND)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_unknown_pubrec_answers_pubrel_not_found():
    """A PUBREC for an id that is not an outstanding inflight delivery gets PUBREL 0x92."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("qos2-pubrec-stray"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            # id 0x0778 is not in the broker's inflight set for this session.
            c.sendall(pubrec(0x0778, reason=REASON_SUCCESS))
            h, b = read_packet(c)
            assert h == PUBREL, "expected PUBREL to a stray PUBREC, got 0x%02X" % h
            pid = struct.unpack("!H", b[0:2])[0]
            reason = b[2] if len(b) > 2 else REASON_SUCCESS
            assert pid == 0x0778, "PUBREL carried pid 0x%04X" % pid
            assert reason == REASON_PACKET_ID_NOT_FOUND, (
                "a PUBREC for an identifier that is not an outstanding delivery "
                "must be answered PUBREL 0x92 (MQTT-4.3.3-5), got 0x%02X" % reason)
            return "a stray PUBREC is answered PUBREL 0x92 (PACKET_IDENTIFIER_NOT_FOUND)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_known_flow_still_completes():
    """Control: once a QoS 2 PUBLISH really starts a flow, its PUBREL gets
    PUBCOMP/SUCCESS, so the 0x92 path is not simply taken for every PUBREL."""
    proc, port, workdir = start()
    pub = socket.create_connection((HOST, port), timeout=5)
    sub = socket.create_connection((HOST, port), timeout=5)
    try:
        pub.sendall(connect("qos2-ok-pub"))
        assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for publisher"
        sub.sendall(connect("qos2-ok-sub"))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(subscribe(1, "qos2ok/t", qos=2))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"

        pid = 0x2A
        payload = b"real-flow"
        pub_body = utf8("qos2ok/t") + struct.pack("!H", pid) + enc_len(0) + payload
        pub.sendall(bytes([PUBLISH_QOS2]) + enc_len(len(pub_body)) + pub_body)
        h, b = read_packet(pub)
        assert h == PUBREC, "expected PUBREC to publisher, got 0x%02X" % h

        # Subscriber leg: PUBLISH -> PUBREC -> PUBREL -> PUBCOMP.
        h, b = read_packet(sub)
        assert h == PUBLISH_QOS2, "expected PUBLISH to subscriber, got 0x%02X" % h
        rtopic, rpid, rpayload = _parse_publish(b)
        sub.sendall(pubrec(rpid))
        h, b = read_packet(sub)
        assert h == PUBREL, "expected PUBREL to subscriber, got 0x%02X" % h
        sub.sendall(pubcomp(rpid))

        # Publisher's PUBREL completes its real flow: SUCCESS, not 0x92.
        pub.sendall(pubrel(pid))
        h, b = read_packet(pub)
        assert h == PUBCOMP, "expected PUBCOMP to publisher, got 0x%02X" % h
        reason = b[2] if len(b) > 2 else REASON_SUCCESS
        assert reason == REASON_SUCCESS, (
            "a PUBREL completing a real QoS 2 flow must be PUBCOMP/SUCCESS, got "
            "0x%02X" % reason)
        return "a real QoS 2 flow still completes PUBCOMP/SUCCESS"
    finally:
        pub.close()
        sub.close()
        stop(proc, workdir)


def _parse_publish(body):
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
    return topic, pid, body[off + plen:]


case("MQTT 4.3.3-5: a PUBREL for an identifier that started no QoS 2 flow is answered PUBCOMP 0x92 (PACKET_IDENTIFIER_NOT_FOUND)",
     case_unknown_pubrel_answers_pubcomp_not_found)
case("MQTT 4.3.3-5: a PUBREC for an id that is not an outstanding delivery is answered PUBREL 0x92 (PACKET_IDENTIFIER_NOT_FOUND)",
     case_unknown_pubrec_answers_pubrel_not_found)
case("MQTT 4.3.3-5 (control): a PUBREL completing a real QoS 2 flow is still answered PUBCOMP/SUCCESS",
     case_known_flow_still_completes)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
