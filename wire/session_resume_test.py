#!/usr/bin/env python3
"""Session resume: an unacked QoS 1 message is retransmitted with DUP on reconnect.

When a client with a persistent session (Session Expiry Interval > 0) drops its
connection with a QoS 1 message still unacknowledged, the broker must keep the
session and, on reconnect with Clean Start = 0, retransmit that message with the
DUP flag set (MQTT-4.4.0-1 / §4.6). The subscriber gets the message again, marked
as a retransmission, and completing the new flow with PUBACK clears it.

SessionLifecycleTests exercises Session::resume directly, but the full lifecycle
is an integration none of them drive over a real connection:

  * the client drops the socket WITHOUT a graceful DISCONNECT, so the session is
    kept by its Session Expiry Interval rather than ended;
  * a SECOND client publishes to the topic while the first is (still) connected;
  * the first client reconnects, and the broker must locate the persisted session
    in its session map (not create a fresh one, which would report
    sessionPresent = 0 and lose the subscription and the unacked message);
  * the unacked message returns with DUP = 1.

Usage: session_resume_test.py <binary> <port>
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

# MQTT 5 property IDs.
PROP_SESSION_EXPIRY_INTERVAL = 0x11


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


def connect(cid, clean_start, session_expiry):
    """MQTT 5 CONNECT carrying a Session Expiry Interval property."""
    flags = 0x02 if clean_start else 0x00
    prop = bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
    body = (utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) +
            enc_len(len(prop)) + prop + utf8(cid))
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=1):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_qos1(pid, topic, payload):
    body = utf8(topic) + struct.pack("!H", pid) + enc_len(0) + payload
    return bytes([0x32]) + enc_len(len(body)) + body  # PUBLISH, QoS 1, DUP 0


def puback(pid):
    return bytes([0x40]) + enc_len(2) + struct.pack("!H", pid)


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


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-resume-")
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


def case_unacked_qos1_is_retransmitted_on_resume():
    proc, port, workdir = start()
    topic = "r/t"
    try:
        # --- First connection: persistent session, subscribed. ---
        a = socket.create_connection((HOST, port), timeout=5)
        a.sendall(connect("resume-a", clean_start=True, session_expiry=60))
        header, body = read_packet(a)
        assert header & 0xF0 == 0x20, "no CONNACK on first connect"
        # First connect: no pre-existing session, so sessionPresent = 0.
        assert body[0] & 0x01 == 0, (
            "first connect reported sessionPresent=1 with no prior session")
        a.sendall(subscribe(1, topic, qos=1))
        assert read_packet(a)[0] & 0xF0 == 0x90, "no SUBACK"

        # --- Second client publishes QoS 1 while A is connected. ---
        b = socket.create_connection((HOST, port), timeout=5)
        b.sendall(connect("resume-b", clean_start=True, session_expiry=60))
        assert read_packet(b)[0] & 0xF0 == 0x20, "no CONNACK for publisher"
        b.sendall(publish_qos1(5, topic, b"resume-me"))

        # A receives the PUBLISH but does NOT ack it: the message stays in flight.
        # The broker assigns its own packet id for the broker->A leg, independent of
        # the publisher's, so capture whatever id it chose.
        header, body = read_packet(a)
        assert header & 0xF0 == 0x30, "A did not receive the PUBLISH"
        assert header & 0x08 == 0, "the first delivery must not have DUP set"
        t, first_pid, payload = parse_publish(body)
        assert (t, payload) == (topic, b"resume-me"), (
            "first delivery was %r/%r" % (t, payload))
        assert first_pid != 0, "the broker delivered the PUBLISH with packet id 0"

        # A drops the socket WITHOUT a graceful DISCONNECT and WITHOUT the PUBACK,
        # so the QoS 1 message is still unacknowledged. The session must survive
        # (Session Expiry Interval 60s), not be discarded.
        a.close()
        b.close()
        time.sleep(0.8)  # let the broker register the drop and keep the session

        # --- Reconnect: Clean Start = 0 resumes the persisted session. ---
        a = socket.create_connection((HOST, port), timeout=5)
        a.sendall(connect("resume-a", clean_start=False, session_expiry=60))
        header, body = read_packet(a)
        assert header & 0xF0 == 0x20, "no CONNACK on reconnect"
        assert body[0] & 0x01 == 1, (
            "reconnect did not report sessionPresent=1: the broker did not find "
            "the persisted session (it lost the subscription or the unacked "
            "message, or created a fresh session instead of resuming)")

        # The unacked message is retransmitted with DUP = 1, keeping the same
        # broker-assigned packet id (MQTT-4.4.0-1 / §4.6).
        header, body = read_packet(a, timeout=4.0)
        assert header & 0xF0 == 0x30, "no retransmitted PUBLISH after resume"
        assert header & 0x08 != 0, (
            "the retransmitted PUBLISH did not carry the DUP flag (MQTT-4.4.0-1)")
        t, re_pid, payload = parse_publish(body)
        assert (t, re_pid, payload) == (topic, first_pid, b"resume-me"), (
            "retransmission was %r/%r/%r (expected the original topic/pid/payload)"
            % (t, re_pid, payload))

        # Completing the new flow with PUBACK clears it; nothing further arrives.
        a.sendall(puback(first_pid))
        deadline = time.time() + 1.0
        extra = []
        while time.time() < deadline:
            try:
                header, body = read_packet(a, 0.4)
            except (EOFError, socket.timeout):
                break
            if header & 0xF0 == 0x30:
                extra.append(parse_publish(body))
        assert not extra, "after the PUBACK the subscriber got %d extra PUBLISH(es)" % len(extra)
        a.close()
        return "unacked QoS 1 retransmitted with DUP on resume; PUBACK completed the new flow"
    finally:
        try:
            a.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            b.close()
        except Exception:  # noqa: BLE001
            pass
        stop(proc, workdir)


case("session resume: an unacked QoS 1 message is retransmitted with DUP on reconnect",
     case_unacked_qos1_is_retransmitted_on_resume)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
