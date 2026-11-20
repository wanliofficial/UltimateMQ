#!/usr/bin/env python3
"""MQTT-4.9 / §3.1.2.11.9: Receive Maximum caps the number of QoS 1/2 messages
the broker keeps outstanding (sent, unacknowledged) to a client.

The subtle case this probe pins down is the QoS 2 leg between PUBREC and PUBCOMP.
A QoS 2 PUBLISH must stay outstanding until the client's PUBCOMP, NOT its PUBREC:
once the broker has sent PUBREC the flow is only half done (PUBREL/PUBCOMP is
still owed), so the slot may not be freed early. A broker that releases the slot
on PUBREC admits one extra message per completed PUBREC and silently exceeds the
client's advertised Receive Maximum.

The session counts both inflightMessages and pendingPubrel toward the window
(Session::pushDelivery / releaseBlocked), so a QoS 2 message occupies its slot
from PUBLISH until PUBCOMP.

Test: a subscriber advertises Receive Maximum = 2 and subscribes at QoS 2. Two
messages fill the window; the subscriber answers both with PUBREC (moving them
to pendingPubrel). A third message must then stay blocked, because both flows
are still outstanding. It is only released once the subscriber completes a flow
with PUBCOMP.

This is an end-to-end wire test: it launches the real broker binary and drives
it over TCP, so it complements the in-process gtest suite.

Usage: recvmax_window_test.py <binary> <port>
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

SESSION_EXPIRY = 0x11
RECEIVE_MAXIMUM = 0x21  # §3.1.2.11.9, a Two Byte Integer (no length prefix)


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


def connect(cid, clean_start=True, props=b""):
    flags = 0x02 if clean_start else 0x00
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(len(props)) + props + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def recv_max_prop(value):
    # Property ID + Two Byte Integer value (§1.5.2: no length byte).
    return bytes([RECEIVE_MAXIMUM]) + struct.pack("!H", value)


def subscribe(pid, filt, qos=2):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(topic, payload, qos=0, pid=1):
    body = utf8(topic)
    if qos:
        body += struct.pack("!H", pid)
    body += enc_len(0) + payload
    header = 0x30 | (qos << 1)
    return bytes([header]) + enc_len(len(body)) + body


def pubrec(pid, reason=0x00):
    return bytes([0x50, 0x04]) + struct.pack("!H", pid) + bytes([reason]) + b"\x00"


def pubrel(pid):
    return bytes([0x62, 0x02]) + struct.pack("!H", pid)


def pubcomp(pid):
    return bytes([0x70, 0x02]) + struct.pack("!H", pid)


def pingreq():
    return bytes([0xC0, 0x00])


def read_pkt(c, timeout=1.5):
    c.settimeout(timeout)
    try:
        h = c.recv(1)
    except socket.timeout:
        return ("timeout",)
    except Exception as exc:  # noqa: BLE001
        return ("err", repr(exc))
    if not h:
        return ("eof",)
    n, mult = 0, 1
    while True:
        b = c.recv(1)
        if not b:
            return ("eof",)
        b = b[0]
        n += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    data = b""
    while len(data) < n:
        chunk = c.recv(n - len(data))
        if not chunk:
            return ("eof",)
        data += chunk
    return ("pkt", h[0], data)


def parse_publish_id(body):
    tlen = struct.unpack("!H", body[0:2])[0]
    off = 2 + tlen
    return struct.unpack("!H", body[off:off + 2])[0]


def start():
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-recvmax-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            c = socket.create_connection((HOST, port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, port, workdir
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


def new_client(port, cid, clean_start=True, props=b""):
    c = socket.create_connection((HOST, port), timeout=5)
    c.sendall(connect(cid, clean_start, props))
    r = read_pkt(c)
    assert r[0] == "pkt" and r[1] & 0xF0 == 0x20, "no CONNACK for %s" % cid
    return c


def drain_publish(c, budget):
    """Read up to `budget` PUBLISH packets, returning their Packet Identifiers."""
    got = []
    for _ in range(budget):
        r = read_pkt(c, 1.0)
        if r[0] != "pkt" or (r[1] & 0xF0) != 0x30:
            break
        got.append(parse_publish_id(r[2]))
    return got


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


def case_window_holds_through_pubcomp():
    """Receive Maximum 2, QoS 2: a slot freed by a PUBREC is NOT available for a
    new message until the matching PUBCOMP. The window fills with two
    outstanding PUBLISHes; the subscriber PUBRECs both (moving them to
    pendingPubrel); a third message must then stay blocked, because both flows
    are still outstanding. It is released only after a PUBCOMP completes a flow."""
    proc, port, workdir = start()
    try:
        props = recv_max_prop(2)
        sub = new_client(port, "wm-sub", clean_start=True, props=props)
        sub.sendall(subscribe(1, "wm/#", qos=2))
        r = read_pkt(sub)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        pub = new_client(port, "wm-pub", clean_start=True)

        # Two messages fill the window; the publisher drives both to PUBCOMP.
        pub.sendall(publish("wm/x", b"m1", qos=2, pid=1))
        assert read_pkt(pub)[1] & 0xF0 == 0x50, "no PUBREC for m1"
        pub.sendall(pubrel(1)); read_pkt(pub)
        pub.sendall(publish("wm/x", b"m2", qos=2, pid=2))
        assert read_pkt(pub)[1] & 0xF0 == 0x50, "no PUBREC for m2"
        pub.sendall(pubrel(2)); read_pkt(pub)

        got = drain_publish(sub, 2)
        assert len(got) == 2, "expected 2 PUBLISH in window, got %d (%r)" % (len(got), got)
        first_pid = got[0]

        # Subscriber PUBRECs both: a buggy broker would now treat the window as
        # empty; a correct one still has 2 outstanding (pendingPubrel).
        for pid in got:
            sub.sendall(pubrec(pid, 0x00))

        # A THIRD message arrives now. Correct broker: still 2 outstanding, so
        # it is BLOCKED and must not reach the subscriber. The broker does owe a
        # PUBREL (0x62) for each PUBREC - that is the normal QoS 2 leg - so drain
        # those and require that no PUBLISH (0x30) shows up.
        pub.sendall(publish("wm/x", b"m3", qos=2, pid=3))
        assert read_pkt(pub)[1] & 0xF0 == 0x50, "no PUBREC for m3"
        pub.sendall(pubrel(3)); read_pkt(pub)

        deadline = time.time() + 1.5
        leaked = False
        while time.time() < deadline:
            r = read_pkt(sub, max(0.1, deadline - time.time()))
            if r[0] != "pkt":
                break
            if (r[1] & 0xF0) == 0x30:
                leaked = True
                break
            # 0x62 PUBREL (or anything else): expected, keep draining
        assert not leaked, (
            "a 3rd PUBLISH leaked after two PUBRECs but no PUBCOMP: the window "
            "freed the slot on PUBREC instead of PUBCOMP (MQTT-4.9)")

        # Completing the first flow (PUBCOMP) releases the parked third.
        sub.sendall(pubrel(first_pid))
        # Drain until the PUBCOMP for first_pid arrives (PUBs owed for other
        # PUBRECs may interleave); a PUBLISH for the released 3rd must follow.
        got_comp = False
        third = None
        deadline = time.time() + 2.0
        while time.time() < deadline and (third is None or not got_comp):
            r = read_pkt(sub, max(0.1, deadline - time.time()))
            if r[0] != "pkt":
                break
            t = r[1] & 0xF0
            if t == 0x70 and struct.unpack("!H", r[2][0:2])[0] == first_pid:
                got_comp = True
                sub.sendall(pubcomp(first_pid))
            elif t == 0x30:
                third = r
        assert got_comp, "no PUBCOMP for the completed flow %d" % first_pid
        assert third is not None and third[1] & 0xF0 == 0x30, (
            "the parked 3rd message was not released after a PUBCOMP")
        assert parse_publish_id(third[2]) == 3, "released message had the wrong id"

        sub.sendall(pingreq())
        p = read_pkt(sub)
        assert p[0] == "pkt" and p[1] & 0xF0 == 0xD0, "connection did not survive"
        sub.close()
        pub.close()
        return "window held at 2 through PUBREC; 3rd blocked until a PUBCOMP"
    finally:
        stop(proc, workdir)


def case_window_holds_qos1():
    """Control at QoS 1: Receive Maximum 2 means at most 2 un-acked PUBLISHes;
    the third waits for the first PUBACK."""
    proc, port, workdir = start()
    try:
        props = recv_max_prop(2)
        sub = new_client(port, "wq-sub", clean_start=True, props=props)
        sub.sendall(subscribe(1, "wq/#", qos=1))
        r = read_pkt(sub)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        pub = new_client(port, "wq-pub", clean_start=True)
        for i in range(1, 4):
            pub.sendall(publish("wq/x", b"q%d" % i, qos=1, pid=i))

        got = drain_publish(sub, 2)
        assert len(got) == 2, "QoS1: expected 2 PUBLISH in window, got %d" % len(got)
        early = read_pkt(sub, 1.0)
        assert early[0] == "timeout", "QoS1: a 3rd PUBLISH leaked early: %r" % (early,)

        # ACK the first; the third is released.
        sub.sendall(bytes([0x40, 0x02]) + struct.pack("!H", got[0]))
        third = read_pkt(sub, 1.5)
        assert third[0] == "pkt" and third[1] & 0xF0 == 0x30, (
            "QoS1: parked 3rd not released after a PUBACK: %r" % (third,))

        sub.close()
        pub.close()
        return "QoS1 window held at 2; 3rd released after the first PUBACK"
    finally:
        stop(proc, workdir)


case("MQTT-4.9: Receive Maximum window holds a QoS 2 message through PUBCOMP, not PUBREC",
      case_window_holds_through_pubcomp)
case("MQTT-4.9 (control): Receive Maximum window holds at QoS 1 until PUBACK",
      case_window_holds_qos1)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
