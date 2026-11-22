#!/usr/bin/env python3
"""End-to-end MQTT QoS 2 exactly-once over the wire.

QoS 2 is the protocol's once-and-once-only contract, and it is actually TWO
independent four-way handshakes that happen to share a packet id:

  * publisher -> broker : PUBLISH, then (on PUBREC) PUBREL, then PUBCOMP;
  * broker -> subscriber: PUBLISH, then (on the subscriber's PUBREC) PUBREL,
    then PUBCOMP.

The subscriber therefore also speaks QoS 2 as a receiver (PUBREC/PUBREL/PUBCOMP),
and the publisher speaks it as a sender (PUBREL/PUBCOMP). Driving both sides
confirms the message crosses each leg exactly once.

The existing recvmax_window_test.py exercises the flow-control window blocking
(a third PUBLISH held while two QoS 2 flows are outstanding), but nothing covers
the happy-path handshake itself or its two subtle correctness points:

  1. exactly-once: the subscriber receives the payload ONE time, with no DUP, even
     though the broker forwards after PUBREC and both legs complete;
  2. a DUP retransmit of an in-flight QoS 2 PUBLISH (the publisher resends before
     its PUBREL, because it never saw the first PUBREC) must be answered with
     PUBREC/SUCCESS again, NOT PACKET_IDENTIFIER_IN_USE, and must NOT be forwarded
     to the subscriber a second time (MQTT-4.3.3-1, MQTT-4.4.0-2: the flow is
     already in progress, so it is not a new identifier clash and the message is
     not re-routed).

Usage: qos2_exactly_once_test.py <binary> <port>
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

# Fixed-header bytes for the QoS 2 control packets.
PUBLISH_QOS2 = 0x34   # PUBLISH, QoS 2 (0x30 | (2 << 1))
PUBREC = 0x50
PUBREL = 0x62         # PUBREL, QoS 1 flags (0x40 | 0x02)
PUBCOMP = 0x70

REASON_SUCCESS = 0x00
REASON_PACKET_ID_IN_USE = 0x91


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


def publish_qos2(pid, topic, payload, dup=False):
    body = utf8(topic) + struct.pack("!H", pid) + enc_len(0) + payload
    header = PUBLISH_QOS2 | (0x08 if dup else 0x00)
    return bytes([header]) + enc_len(len(body)) + body


def pubrec(pid):
    # A receiver's PUBREC: Packet Identifier only (Remaining Length 2, reason/SUCCESS
    # and properties omitted; §3.5.2 allows the short form).
    return bytes([PUBREC]) + enc_len(2) + struct.pack("!H", pid)


def pubrel(pid):
    return bytes([PUBREL]) + enc_len(2) + struct.pack("!H", pid)


def pubcomp(pid):
    # A receiver's PUBCOMP: Packet Identifier only (Remaining Length 2).
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
    """Return (header, body) for one packet, body having the Remaining Length
    stripped."""
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


def parse_publish_qos2(body):
    """QoS 2 PUBLISH body: Topic Name (len-prefixed), Packet Id (2), Property
    Length (VBI), payload."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    pid = struct.unpack("!H", body[off:off + 2])[0]
    off += 2
    plen = 0
    mult = 1
    while True:
        b = body[off]
        off += 1
        plen += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    return topic, pid, body[off + plen:]


def ack_pid_and_reason(body):
    """A broker PUBREC/PUBCOMP body is Packet Id (2) then an optional trailing
    zero byte (the RL=3 short form: reason 0x00). Return (pid, reason)."""
    pid = struct.unpack("!H", body[0:2])[0]
    reason = body[2] if len(body) > 2 else REASON_SUCCESS
    return pid, reason


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-qos2-")
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


def _connect_sub_and_pub(topic):
    proc, port, workdir = start()
    pub = socket.create_connection((HOST, port), timeout=5)
    sub = socket.create_connection((HOST, port), timeout=5)
    try:
        pub.sendall(connect("qos2-pub"))
        assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for publisher"
        sub.sendall(connect("qos2-sub"))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub.sendall(subscribe(1, topic, qos=2))
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"
        return proc, port, workdir, pub, sub, topic
    except Exception:
        pub.close()
        sub.close()
        stop(proc, workdir)
        raise


def case_full_handshake_delivers_exactly_once():
    topic = "qos2/t"
    ctx = _connect_sub_and_pub(topic)
    proc, port, workdir, pub, sub, topic = ctx
    try:
        pid = 10
        payload = b"exactly-once"
        # Publisher -> broker: PUBLISH (QoS 2).
        pub.sendall(publish_qos2(pid, topic, payload))
        # Broker -> publisher: PUBREC.
        h, b = read_packet(pub)
        assert h == PUBREC, "expected PUBREC to publisher, got 0x%02X" % h
        assert ack_pid_and_reason(b) == (pid, REASON_SUCCESS), (
            "PUBREC to publisher was %r" % (ack_pid_and_reason(b),))

        # Broker -> subscriber: PUBLISH (QoS 2). The broker assigns its OWN packet
        # id for the broker->subscriber leg, independent of the publisher's id.
        h, b = read_packet(sub)
        assert h == PUBLISH_QOS2, "expected PUBLISH to subscriber, got 0x%02X" % h
        rtopic, rpid, rpayload = parse_publish_qos2(b)
        assert rtopic == topic and rpid != 0 and rpayload == payload, (
            "subscriber got %r/%r/%r" % (rtopic, rpid, rpayload))
        sub.sendall(pubrec(rpid))
        # Broker -> subscriber: PUBREL (carries the same id the subscriber sent).
        h, b = read_packet(sub)
        assert h == PUBREL, "expected PUBREL to subscriber, got 0x%02X" % h
        assert struct.unpack("!H", b[0:2])[0] == rpid
        sub.sendall(pubcomp(rpid))

        # Publisher -> broker: PUBREL (completes the publisher's leg).
        pub.sendall(pubrel(pid))
        # Broker -> publisher: PUBCOMP.
        h, b = read_packet(pub)
        assert h == PUBCOMP, "expected PUBCOMP to publisher, got 0x%02X" % h
        assert ack_pid_and_reason(b) == (pid, REASON_SUCCESS), (
            "PUBCOMP to publisher was %r" % (ack_pid_and_reason(b),))

        # Exactly-once: no further PUBLISH reaches the subscriber.
        extra = []
        deadline = time.time() + 1.0
        while time.time() < deadline:
            try:
                h, b = read_packet(sub, 0.4)
            except (EOFError, socket.timeout):
                break
            if h == PUBLISH_QOS2:
                extra.append(parse_publish_qos2(b))
        assert not extra, "subscriber received %d extra PUBLISH(es): %r" % (len(extra), extra)
        return "QoS 2 PUBLISH/PUBREC/PUBREL/PUBCOMP completed on both legs; delivered once"
    finally:
        pub.close()
        sub.close()
        stop(proc, workdir)


def case_dup_retransmit_is_not_re_routed():
    topic = "qos2/dup"
    ctx = _connect_sub_and_pub(topic)
    proc, port, workdir, pub, sub, topic = ctx
    try:
        pid = 11
        payload = b"dup-once"
        # Original PUBLISH (QoS 2).
        pub.sendall(publish_qos2(pid, topic, payload, dup=False))
        h, b = read_packet(pub)
        assert h == PUBREC, "expected first PUBREC, got 0x%02X" % h

        # Retransmit the same PUBLISH (DUP=1) before sending PUBREL, as a client
        # that never saw the first PUBREC would.
        pub.sendall(publish_qos2(pid, topic, payload, dup=True))
        h, b = read_packet(pub)
        assert h == PUBREC, "expected a second PUBREC for the DUP, got 0x%02X" % h
        rpid, reason = ack_pid_and_reason(b)
        assert rpid == pid, "second PUBREC carried pid %d" % rpid
        assert reason == REASON_SUCCESS, (
            "a DUP retransmit of an in-flight QoS 2 PUBLISH was refused with "
            "reason 0x%02X; it must be PUBREC/SUCCESS (MQTT-4.3.3-1), not "
            "PACKET_IDENTIFIER_IN_USE" % reason)

        # The subscriber must have received the original exactly once; the DUP must
        # not have been forwarded again.
        h, b = read_packet(sub)
        assert h == PUBLISH_QOS2, "expected the subscriber's PUBLISH, got 0x%02X" % h
        rtopic, rpid, rpayload = parse_publish_qos2(b)
        assert rtopic == topic and rpid != 0 and rpayload == payload, (
            "subscriber got %r/%r/%r" % (rtopic, rpid, rpayload))
        # Complete both legs so the broker releases the flow cleanly.
        sub.sendall(pubrec(rpid))
        read_packet(sub)            # PUBREL
        sub.sendall(pubcomp(rpid))
        pub.sendall(pubrel(pid))
        read_packet(pub)            # PUBCOMP

        extra = []
        deadline = time.time() + 1.0
        while time.time() < deadline:
            try:
                h, b = read_packet(sub, 0.4)
            except (EOFError, socket.timeout):
                break
            if h == PUBLISH_QOS2:
                extra.append(parse_publish_qos2(b))
        assert not extra, "the DUP retransmit was forwarded %d extra time(s)" % len(extra)
        return "DUP retransmit answered PUBREC/SUCCESS and not re-forwarded"
    finally:
        pub.close()
        sub.close()
        stop(proc, workdir)


case("QoS 2: the full PUBLISH/PUBREC/PUBREL/PUBCOMP handshake delivers exactly once",
     case_full_handshake_delivers_exactly_once)
case("QoS 2: a DUP retransmit of an in-flight PUBLISH gets PUBREC/SUCCESS and is not re-routed",
     case_dup_retransmit_is_not_re_routed)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
