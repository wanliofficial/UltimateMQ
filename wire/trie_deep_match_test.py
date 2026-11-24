#!/usr/bin/env python3
"""A deeply nested subscription must not crash the broker when it is matched.

Trie::match walks the routing trie with a recursive DFS, recursing once per topic
level. Both the depth of a Topic Filter and the depth of a Topic Name are bounded
only by the 65535-byte UTF-8 string limit, so a client may SUBSCRIBE to a filter
with tens of thousands of levels and then PUBLISH to a topic with the same depth.
The DFS then recurses once per level on the connection thread, which the broker
runs on a std::thread with the default 512 KB stack, and the stack overflows.

Because Broker::publish() calls trie->match() for every inbound PUBLISH, the
crash takes the whole broker process down: a remote denial of service from one
ordinary client doing two legal operations, no oversized packet required
(a ~40 KB topic is well under the 1 MiB default packet cap).

The fix replaces the recursive DFS with an explicit-stack walk, so the work is
bounded by the trie size with no stack growth. The test subscribes to a
DEPTH-level filter, publishes to the matching DEPTH-level topic, and then requires
the broker to still answer a fresh client (CONNACK + PINGRESP). Before the fix the
process dies during the PUBLISH and nothing answers.

Usage: trie_deep_match_test.py <binary> <port>
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

# Levels of nesting. 20000 levels is a ~40 KB name: comfortably inside the
# 65535-byte MQTT string limit and the 1 MiB packet cap, and deep enough to
# overflow the 512 KB connection-thread stack with the recursive walk.
DEPTH = 20000


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


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(topic, payload=b"deep"):
    body = utf8(topic) + enc_len(0) + payload
    return bytes([0x30]) + enc_len(len(body)) + body


def pingreq():
    return bytes([0xC0, 0x00])


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("closed after %d of %d bytes" % (len(buf), n))
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
    try:
        body = recv_exact(sock, val) if val else b""
    except (socket.timeout, EOFError):
        return None
    return header, body


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-triematch-")
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
    if proc.poll() is None:
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


def case_deep_topic_does_not_crash():
    proc, mqtt_port, workdir = start()
    try:
        deep = "/".join(["a"] * DEPTH)
        watcher = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            watcher.sendall(connect("deep-watcher"))
            connack = read_packet(watcher)
            assert connack is not None, "no CONNACK for the first client"
            assert connack[0] & 0xF0 == 0x20, "expected a CONNACK"

            # A second client subscribes to the deep filter and publishes to the
            # matching deep topic; Broker::publish() matches the trie on the
            # publisher's own connection thread.
            c = socket.create_connection((HOST, mqtt_port), timeout=5)
            try:
                c.sendall(connect("deep-publisher"))
                assert read_packet(c) is not None, "no CONNACK for the publisher"
                c.sendall(subscribe(1, deep, qos=0))
                suback = read_packet(c, 5.0)
                assert suback is not None, (
                    "no SUBACK for a %d-level filter: the broker stopped "
                    "answering" % DEPTH)
                assert suback[0] & 0xF0 == 0x90, "expected a SUBACK"
                c.sendall(publish(deep))
            finally:
                c.close()

            # The broker must still be serving: a fresh client completes CONNECT
            # and a PINGREQ round trip. With the recursive DFS the whole process
            # dies during the PUBLISH above.
            probe = socket.create_connection((HOST, mqtt_port), timeout=5)
            try:
                probe.sendall(connect("deep-liveness"))
                assert read_packet(probe, 5.0) is not None, (
                    "no CONNACK after publishing to a %d-level topic: the "
                    "recursive trie match crashed the broker" % DEPTH)
                probe.sendall(pingreq())
                assert read_packet(probe, 5.0) is not None, (
                    "no PINGRESP after the deep topic: broker is gone")
            finally:
                probe.close()
            return ("a publish to a %d-level topic is matched and the broker "
                    "keeps serving" % DEPTH)
        finally:
            watcher.close()
    finally:
        stop(proc, workdir)


def case_normal_subscription_still_matches():
    """Control: ordinary wildcard matching is unaffected by the rewrite."""
    proc, mqtt_port, workdir = start()
    try:
        c = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            c.sendall(connect("plain-sub"))
            assert read_packet(c) is not None, "no CONNACK"
            c.sendall(subscribe(1, "sensor/+/temp", qos=0))
            suback = read_packet(c, 2.0)
            assert suback is not None and suback[0] & 0xF0 == 0x90, "no SUBACK"
            assert suback[1][-1] <= 2, "wildcard filter not granted"

            pub = socket.create_connection((HOST, mqtt_port), timeout=5)
            try:
                pub.sendall(connect("plain-pub"))
                assert read_packet(pub) is not None, "no CONNACK for publisher"
                pub.sendall(publish("sensor/kitchen/temp", b"21"))
            finally:
                pub.close()
            got = read_packet(c, 2.0)
            assert got is not None, "the wildcard subscription did not receive the message"
            assert got[0] & 0xF0 == 0x30, "expected a PUBLISH, got 0x%02X" % got[0]
            assert got[1].endswith(b"21"), "wrong payload delivered"
            return "an ordinary 'sensor/+/temp' subscription still matches"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("PUBLISH: a deeply nested topic match does not overflow the connection stack",
     case_deep_topic_does_not_crash)
case("PUBLISH (control): ordinary wildcard matching still works",
     case_normal_subscription_still_matches)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
