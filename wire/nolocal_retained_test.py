#!/usr/bin/env python3
"""MQTT 5 No Local must also apply to retained messages a new subscription unlocks.

MQTT-3.8.3-3: "If the No Local option is set to 1, Application Messages
MUST NOT be forwarded to a connection with a ClientID equal to the ClientID of
the publishing connection." Retained messages are Application Messages, and the
exclusion has no "except retained" carve-out: a client that publishes a topic
with RETAIN=1 and then subscribes to that same topic with No Local=1 must NOT be
handed its own retained message back.

The live fan-out path (Session::deliver) already honours No Local, but the
retained path (Session::deliverRetained) never checked it, so the subscriber
received its own retained publication on the subscribe. This probe pins it:

  1. The same client publishes `t` with RETAIN=1 and subscribes to `t` with
     No Local=1: it must receive NO PUBLISH (its own retained copy suppressed).
  2. Control: a different client subscribing to `t` with No Local=0 must still
     receive the retained `t`, proving the message exists and only the No-Local
     self-delivery is suppressed.

Usage: nolocal_retained_test.py <binary> <port>
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

NO_LOCAL = 0x04  # Subscription Options bit 2 (MQTT-3.8.3-1)


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


def subscribe(pid, filt, options):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([options])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(topic, payload, retain=True, qos=0):
    body = utf8(topic) + enc_len(0) + payload
    return bytes([0x30 | (0x01 if retain else 0) | (qos << 1)]) + enc_len(len(body)) + body


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
    except socket.timeout:
        return None
    except EOFError:
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
    workdir = tempfile.mkdtemp(prefix="flowmq-nolocal-")
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


def case_no_local_suppresses_own_retained():
    proc, mqtt_port, workdir = start()
    try:
        # One client publishes t with RETAIN=1, then subscribes to t with No
        # Local=1 and must not be handed its own retained copy back.
        a = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            a.sendall(connect("nl-pub"))
            assert read_packet(a) is not None, "no CONNACK for the publisher"
            a.sendall(publish("nl/own", b"mine", retain=True))
            # Let the retained store settle before subscribing.
            time.sleep(0.2)
            a.sendall(subscribe(1, "nl/own", NO_LOCAL))
            sub = read_packet(a, 2.0)
            assert sub is not None and sub[0] & 0xF0 == 0x90, (
                "no SUBACK for the No Local subscription: %r" % (sub,))
            leaked = read_packet(a, 1.0)  # any PUBLISH here is the bug
            assert leaked is None, (
                "No Local subscription received its own retained message: %r"
                % (leaked,))
            return "a No Local=1 subscriber does not receive its own retained message"
        finally:
            a.close()
    finally:
        stop(proc, workdir)


def case_other_client_still_gets_retained():
    """Control: the retained message is still stored and a different client (or
    the same client without No Local) receives it, so only the No-Local
    self-delivery is suppressed, not the retained store itself."""
    proc, mqtt_port, workdir = start()
    try:
        pub = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            pub.sendall(connect("nl-pub2"))
            assert read_packet(pub) is not None, "no CONNACK for the publisher"
            pub.sendall(publish("nl/other", b"theirs", retain=True))
            time.sleep(0.2)
        finally:
            pub.close()

        sub = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            sub.sendall(connect("nl-sub2"))
            assert read_packet(sub) is not None, "no CONNACK for the subscriber"
            sub.sendall(subscribe(1, "nl/other", 0x00))  # No Local off
            suback = read_packet(sub, 2.0)
            assert suback is not None and suback[0] & 0xF0 == 0x90, (
                "no SUBACK for the plain subscription: %r" % (suback,))
            got = read_packet(sub, 2.0)
            assert got is not None and got[0] & 0xF0 == 0x30, (
                "a plain subscriber did not receive the retained message: %r" % (got,))
            tlen = struct.unpack("!H", got[1][0:2])[0]
            assert got[1][2:2 + tlen] == b"nl/other", "wrong retained topic delivered"
            return "a plain subscriber still receives the retained message"
        finally:
            sub.close()
    finally:
        stop(proc, workdir)


case("MQTT 5 No Local: a No Local=1 subscriber does not receive its own retained message (MQTT-3.8.3-3)",
     case_no_local_suppresses_own_retained)
case("MQTT 5 No Local (control): a plain subscriber still receives the retained message",
     case_other_client_still_gets_retained)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
