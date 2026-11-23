#!/usr/bin/env python3
"""MQTT 5: a reconnecting session must not be sent more than Receive Maximum
unacknowledged QoS>0 PUBLISHes, even when messages were buffered while offline.

MQTT-4.9.0-2: "The Server MUST NOT send more than Receive Maximum QoS 1 and QoS 2
PUBLISH packets for which it has not received PUBACK, PUBCOMP, or PUBREC >= 0x80
during the time that a Session is resumed." Receive Maximum is per-connection and
is not part of session state, so a durable session does not know the client's
real window until it reconnects.

The flowmq gateway buffers an offline persistent session's messages under the
default Receive Maximum (65535) because setReceiveMaximum only learns the
negotiated value on reconnect. So while a subscriber is offline, publishers fill
its inflight/queued set with up to MAX_QUEUED_MESSAGES (1000) unacked QoS>0
messages. On reconnect the client negotiates a small Receive Maximum (e.g. 5),
but flush() sent the whole offline buffer at once -- far more than 5 outstanding
PUBLISH packets on a connection whose quota is 5.

The test: client A subscribes (persistent) to `t` and goes offline; client B
publishes 100 QoS 1 messages to `t`; A reconnects with Receive Maximum = 5. A
must receive at most 5 unacknowledged PUBLISHes before it acks, and the remaining
messages must still arrive as it acknowledges (none lost).

Usage: receive_maximum_resume_test.py <binary> <port>
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

PUBLISH_QOS1 = 0x32
CONNACK = 0x20
PUBACK = 0x40

RECEIVE_MAXIMUM = 5
TOTAL_MESSAGES = 100


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


def connect(cid, clean_start, receive_maximum=None, session_expiry=60):
    flags = 0x02 if clean_start else 0x00
    props = b""
    if receive_maximum is not None:
        # Receive Maximum (0x21) is a 2-byte value.
        props += bytes([0x21]) + struct.pack("!H", receive_maximum)
    props += bytes([0x11]) + struct.pack("!I", session_expiry)  # Session Expiry
    body = (utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60)
            + enc_len(len(props)) + props + utf8(cid))
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=1):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_qos1(pid, topic, payload):
    body = utf8(topic) + struct.pack("!H", pid) + enc_len(0) + payload
    return bytes([PUBLISH_QOS1]) + enc_len(len(body)) + body


def puback(pid):
    return bytes([PUBACK]) + enc_len(2) + struct.pack("!H", pid)


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


def parse_publish_qos1(body):
    tlen = struct.unpack("!H", body[0:2])[0]
    off = 2 + tlen
    pid = struct.unpack("!H", body[off:off + 2])[0]
    return pid


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-recvmax-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            socket.create_connection((HOST, mqtt_port), timeout=0.2).close()
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


def case_resume_respects_receive_maximum():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-recvmax2-")
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

        CID = "recvmax-sub"
        topic = "recvmax/t"

        # Client A: persistent session, QoS 1 subscription, then go offline. The
        # first connect omits the Receive Maximum property, so the session buffers
        # the offline queue under the protocol default (65535) -- the whole point:
        # the client's real window is only declared on reconnect.
        a = socket.create_connection((HOST, mqtt_port), timeout=5)
        a.sendall(connect(CID, clean_start=True, receive_maximum=None, session_expiry=60))
        assert read_packet(a)[0] & 0xF0 == CONNACK, "no CONNACK for subscriber"
        a.sendall(subscribe(1, topic, qos=1))
        assert read_packet(a)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"
        a.close()  # session persists (Session Expiry > 0)
        time.sleep(0.3)

        # Client B publishes 100 QoS 1 messages while A is offline.
        b = socket.create_connection((HOST, mqtt_port), timeout=5)
        b.sendall(connect("recvmax-pub", clean_start=True))
        assert read_packet(b)[0] & 0xF0 == CONNACK, "no CONNACK for publisher"
        for i in range(TOTAL_MESSAGES):
            b.sendall(publish_qos1(1000 + i, topic, b"m"))
        deadline2 = time.time() + 3
        got = 0
        while got < TOTAL_MESSAGES and time.time() < deadline2:
            r = read_packet(b, 1.0)
            if r and r[0] & 0xF0 == PUBACK:
                got += 1
        b.close()
        assert got == TOTAL_MESSAGES, "publisher only got %d/%d PUBACKs" % (got, TOTAL_MESSAGES)
        time.sleep(0.3)

        # A reconnects with a small Receive Maximum and must not be flooded with
        # more than 5 unacknowledged PUBLISHes before it acks.
        a2 = socket.create_connection((HOST, mqtt_port), timeout=5)
        a2.sendall(connect(CID, clean_start=False, receive_maximum=RECEIVE_MAXIMUM))
        connack = read_packet(a2, 2.0)
        assert connack is not None and connack[0] & 0xF0 == CONNACK, "no CONNACK on resume"

        # Count PUBLISHes received before sending any PUBACK. Before the fix the
        # broker flushes the whole offline buffer (~100); after it, at most 5.
        first_burst = []
        deadline3 = time.time() + 1.0
        while time.time() < deadline3:
            r = read_packet(a2, 0.5)
            if r is None:
                break
            if (r[0] & 0xF0) == 0x30:
                first_burst.append(parse_publish_qos1(r[1]))
        assert len(first_burst) <= RECEIVE_MAXIMUM, (
            "resumed session received %d unacknowledged PUBLISHes before acking; "
            "Receive Maximum is %d (MQTT-4.9.0-2)"
            % (len(first_burst), RECEIVE_MAXIMUM))

        # Every message must still be delivered as A acknowledges (none lost).
        delivered = set(first_burst)
        for pid in first_burst:
            a2.sendall(puback(pid))
        guard = time.time() + 10
        while len(delivered) < TOTAL_MESSAGES and time.time() < guard:
            r = read_packet(a2, 0.5)
            if r is None:
                continue
            if (r[0] & 0xF0) == 0x30:
                pid = parse_publish_qos1(r[1])
                delivered.add(pid)
                a2.sendall(puback(pid))
        a2.close()
        assert len(delivered) == TOTAL_MESSAGES, (
            "only %d/%d messages were delivered after resume" % (len(delivered), TOTAL_MESSAGES))
        return ("resume sent %d (<= Receive Maximum %d) before acking; all %d "
                "messages eventually delivered" % (len(first_burst), RECEIVE_MAXIMUM,
                                                   len(delivered)))
    finally:
        stop(proc, workdir)


case("MQTT 5: a resumed session is not sent more than Receive Maximum unacked QoS>0 PUBLISHes (MQTT-4.9.0-2)",
     case_resume_respects_receive_maximum)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
