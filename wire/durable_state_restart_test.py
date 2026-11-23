#!/usr/bin/env python3
"""Durable state survives a broker restart (FLOWMQ_STATE_FILE).

The Store persists the broker's durable state (retained messages and persistent
sessions, with their subscriptions) to a file and restores it on the next start.
Every existing probe sets FLOWMQ_STATE_FILE only to keep runs isolated; none
restarts the broker against the SAME file to prove the state actually survives.
That stop -> flush -> restart -> load -> restore path is the broker's most complex
durability code and had zero coverage, so a regression that wrote an incomplete
snapshot, or restored sessions without their subscriptions, would pass every
current test.

This probe starts a broker on a fixed state file, writes durable state, stops it
gracefully (SIGTERM, which flushes the store), starts a NEW broker on the same
file, and checks the state is back:

  1. a retained message published before the restart is delivered to a brand-new
     subscriber after it, with the RETAIN flag set (§3.3.1-9);
  2. a persistent session that subscribed before the restart (Session Expiry
     Interval > 0) is still present after it: a reconnect with Clean Start = 0
     reports sessionPresent = 1, and a publish to its subscription is routed to
     it WITHOUT re-subscribing;
  3. a clean-session client's subscription does NOT survive (it was never
     durable), so a reconnect is a fresh session (sessionPresent = 0).

Usage: durable_state_restart_test.py <binary> <port>
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


def connect(cid, clean_start=True, session_expiry=None):
    flags = 0x02 if clean_start else 0x00
    props = b""
    if session_expiry is not None:
        props = bytes([0x11]) + struct.pack("!I", session_expiry)  # 0x11 = Session Expiry Interval
    body = (utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) +
            enc_len(len(props)) + props + utf8(cid))
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_qos0(topic, payload, retain=False):
    body = utf8(topic) + enc_len(0) + payload
    return bytes([0x30 | (1 if retain else 0)]) + enc_len(len(body)) + body


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


def parse_publish(header, body):
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    qos = (header >> 1) & 0x03
    if qos > 0:
        off += 2
    plen, mult = 0, 1
    while True:
        b = body[off]
        off += 1
        plen += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    return topic, body[off + plen:]


class Broker:
    """A broker process bound to a fixed state file and TCP port."""

    def __init__(self, port, state_file):
        self.port = port
        self.state_file = state_file
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env["MQTT_PORT"] = str(self.port)
        env["FLOWMQ_STATE_FILE"] = self.state_file
        self.proc = subprocess.Popen([BIN], env=env, cwd=os.path.dirname(self.state_file),
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                c = socket.create_connection((HOST, self.port), timeout=0.2)
                c.close()
                time.sleep(0.3)
                return
            except Exception:  # noqa: BLE001
                time.sleep(0.05)
        self.proc.kill()
        raise RuntimeError("server did not start")

    def stop(self):
        # SIGTERM: the server flushes the store to the state file, then exits.
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=8)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
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


def case_retained_and_sessions_survive_restart():
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-durable-")
    state_file = os.path.join(workdir, "state.db")
    retained_topic = "durable/retained"
    persistent_topic = "durable/session"
    try:
        # ---- First run: write durable state. ----
        b1 = Broker(port, state_file)
        b1.start()
        try:
            pub = socket.create_connection((HOST, port), timeout=5)
            pub.sendall(connect("dur-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for publisher"

            # (1) A retained message.
            pub.sendall(publish_qos0(retained_topic, b"survives", retain=True))

            # (2) A PERSISTENT session (expiry > 0) that subscribes, then goes away
            #     WITHOUT a clean teardown: the session must be persisted.
            p = socket.create_connection((HOST, port), timeout=5)
            p.sendall(connect("dur-persistent", clean_start=True, session_expiry=3600))
            assert read_packet(p)[0] & 0xF0 == 0x20, "no CONNACK for persistent client"
            p.sendall(subscribe(1, persistent_topic, qos=1))
            assert read_packet(p)[0] & 0xF0 == 0x90, "no SUBACK for persistent client"
            p.close()

            # (3) A CLEAN session (expiry 0) that subscribes: must NOT be persisted.
            c = socket.create_connection((HOST, port), timeout=5)
            c.sendall(connect("dur-clean", clean_start=True, session_expiry=0))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK for clean client"
            c.sendall(subscribe(1, "durable/clean", qos=0))
            assert read_packet(c)[0] & 0xF0 == 0x90, "no SUBACK for clean client"
            c.close()

            time.sleep(0.5)
            pub.close()
        finally:
            b1.stop()

        # ---- Second run: same state file, fresh process. ----
        b2 = Broker(port, state_file)
        b2.start()
        try:
            # (1) The retained message is back, delivered to a NEW subscriber with
            #     the RETAIN flag set.
            sub = socket.create_connection((HOST, port), timeout=5)
            sub.sendall(connect("dur-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK after restart"
            sub.sendall(subscribe(1, retained_topic, qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK after restart"
            header, body = read_packet(sub, timeout=3.0)
            assert header & 0xF0 == 0x30, "the retained message did not survive the restart"
            assert header & 0x01 == 1, "the restored retained message lost its RETAIN flag"
            topic, payload = parse_publish(header, body)
            assert (topic, payload) == (retained_topic, b"survives"), "%r/%r" % (topic, payload)
            sub.close()

            # (2) The persistent session is back: reconnect Clean Start = 0 reports
            #     sessionPresent = 1, and a publish to its subscription (with no
            #     re-subscribe) is routed to it.
            p = socket.create_connection((HOST, port), timeout=5)
            p.sendall(connect("dur-persistent", clean_start=False, session_expiry=3600))
            header, body = read_packet(p)
            assert header & 0xF0 == 0x20, "no CONNACK for the resumed persistent client"
            assert body[0] & 0x01 == 1, (
                "the persistent session did not survive the restart: sessionPresent=0 "
                "(its subscription was lost)")
            # Publish on a THIRD connection; the restored subscription must route it.
            pub = socket.create_connection((HOST, port), timeout=5)
            pub.sendall(connect("dur-pub2"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for second publisher"
            pub.sendall(publish_qos0(persistent_topic, b"routed"))
            header, body = read_packet(p, timeout=3.0)
            assert header & 0xF0 == 0x30, (
                "the restored subscription did not route a publish after the restart")
            topic, payload = parse_publish(header, body)
            assert (topic, payload) == (persistent_topic, b"routed"), "%r/%r" % (topic, payload)
            pub.close()
            p.close()

            # (3) The clean session's subscription did not survive: reconnecting with
            #     Clean Start = 0 finds no session, so sessionPresent = 0.
            c = socket.create_connection((HOST, port), timeout=5)
            c.sendall(connect("dur-clean", clean_start=False, session_expiry=0))
            header, body = read_packet(c)
            assert header & 0xF0 == 0x20, "no CONNACK for the clean-client reconnect"
            assert body[0] & 0x01 == 0, (
                "a clean-session subscription survived the restart (it must not be "
                "persisted): sessionPresent=1")
            c.close()
            return "retained + persistent session restored after restart; clean session not persisted"
        finally:
            b2.stop()
    finally:
        time.sleep(0.2)


case("durable state: retained messages and persistent sessions survive a broker restart",
     case_retained_and_sessions_survive_restart)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
