#!/usr/bin/env python3
"""MQTT-4.8.2 / §4.8.2: shared subscriptions deliver each message to one member.

A shared subscription ("$share/{ShareName}/{filter}") lets several sessions act as
one logical subscriber. Each application message matching the filter is delivered
to *one* member of the group, not all of them -- the group decides who, and the
broker spreads the load. The semantics [MQTT-4.8.2]:

  * every message published while members are present is delivered to exactly one
    member of the group (never zero, never several);
  * the choice spreads across the members: the broker round-robins, so a stream of
    N messages reaches the group's members in turn rather than always the same one;
  * groups are independent: the same underlying filter under two different share
    names is two groups, and each group receives every message once (one copy per
    group, distributed within it).

subscribe_nested_share_test.py covers the parser hardening (a deeply nested
"$share/a/..." filter must not overflow the stack); this pins the delivery
semantics, which no wire probe drove:

  * two members of "$share/g1/sh/load" receive four published messages split 2/2
    and interleaved -- each message once, load spread across both;
  * control: two DIFFERENT groups ("$share/g1/..." and "$share/g2/...", one member
    each) each receive all four messages -- group independence.

Usage: shared_subscription_roundrobin_test.py <binary> <port>
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


def vbi(value):
    out = b""
    while True:
        d = value % 128
        value //= 128
        if value:
            d |= 0x80
        out += bytes([d])
        if not value:
            return out


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + b"\x00" + utf8(cid)
    return bytes([0x10]) + vbi(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + vbi(0) + utf8(filt) + bytes([qos & 0x03])
    return bytes([0x82]) + vbi(len(body)) + body


def publish_qos0(topic, payload):
    body = utf8(topic) + b"\x00" + payload
    return bytes([0x30]) + vbi(len(body)) + body


def read_packet(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        header = _recv_exact(sock, 1)[0]
    except (socket.timeout, EOFError):
        return None
    mult = 1
    val = 0
    while True:
        try:
            b = _recv_exact(sock, 1)[0]
        except (socket.timeout, EOFError):
            return None
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = _recv_exact(sock, val) if val else b""
    return header, body


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def drain_publishes(sock, expected, timeout=1.5):
    """Collect up to `expected` QoS 0 PUBLISH payloads, or until a short quiet
    period proves no more are coming. Returns the list of payloads."""
    out = []
    deadline = None
    while len(out) < expected:
        pkt = read_packet(sock, timeout)
        if pkt is None:
            break
        h, b = pkt
        if h & 0xF0 != 0x30:
            continue
        tlen = struct.unpack("!H", b[0:2])[0]
        off = 2 + tlen
        plen = 0
        mult = 1
        while True:
            x = b[off]
            off += 1
            plen += (x & 127) * mult
            mult *= 128
            if not x & 128:
                break
        out.append(b[off + plen:])
    return out


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-shared-")
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


def case_group_splits_load_one_each():
    """Two members of one shared group: four messages are split across them, each
    delivered exactly once, distributed (not all to one member)."""
    proc, port, workdir = start()
    try:
        a = socket.create_connection((HOST, port), timeout=5)
        b = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            a.sendall(connect("sh-a"))
            assert read_packet(a)[0] & 0xF0 == 0x20, "no CONNACK (a)"
            b.sendall(connect("sh-b"))
            assert read_packet(b)[0] & 0xF0 == 0x20, "no CONNACK (b)"
            pub.sendall(connect("sh-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"

            a.sendall(subscribe(1, "$share/g1/sh/load", qos=0))
            assert read_packet(a)[0] & 0xF0 == 0x90, "no SUBACK (a)"
            b.sendall(subscribe(1, "$share/g1/sh/load", qos=0))
            assert read_packet(b)[0] & 0xF0 == 0x90, "no SUBACK (b)"

            for i in range(4):
                pub.sendall(publish_qos0("sh/load", ("m%d" % i).encode()))
                time.sleep(0.1)

            got_a = drain_publishes(a, 4, 1.5)
            got_b = drain_publishes(b, 4, 1.5)
            total = got_a + got_b
            assert sorted(total) == [b"m0", b"m1", b"m2", b"m3"], (
                "each of the four messages must be delivered exactly once across "
                "the group, got a=%r b=%r" % (got_a, got_b))
            assert len(got_a) >= 1 and len(got_b) >= 1, (
                "the load must spread across members, but a=%r b=%r" % (got_a, got_b))
            return "four messages split across two members (a=%d, b=%d), each once" % (
                len(got_a), len(got_b))
        finally:
            a.close()
            b.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_distinct_groups_each_get_all():
    """Control: two different share names over the same filter are independent
    groups; a message is delivered once per group, so each single-member group
    receives all four."""
    proc, port, workdir = start()
    try:
        a = socket.create_connection((HOST, port), timeout=5)
        b = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            a.sendall(connect("grp-a"))
            assert read_packet(a)[0] & 0xF0 == 0x20, "no CONNACK (a)"
            b.sendall(connect("grp-b"))
            assert read_packet(b)[0] & 0xF0 == 0x20, "no CONNACK (b)"
            pub.sendall(connect("grp-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"

            a.sendall(subscribe(1, "$share/g1/grp/topic", qos=0))
            assert read_packet(a)[0] & 0xF0 == 0x90, "no SUBACK (a)"
            b.sendall(subscribe(1, "$share/g2/grp/topic", qos=0))
            assert read_packet(b)[0] & 0xF0 == 0x90, "no SUBACK (b)"

            for i in range(4):
                pub.sendall(publish_qos0("grp/topic", ("m%d" % i).encode()))
                time.sleep(0.1)

            got_a = drain_publishes(a, 4, 1.5)
            got_b = drain_publishes(b, 4, 1.5)
            expect = [b"m0", b"m1", b"m2", b"m3"]
            assert sorted(got_a) == expect, (
                "group g1 must receive all four, got %r" % (got_a,))
            assert sorted(got_b) == expect, (
                "group g2 must receive all four independently, got %r" % (got_b,))
            return "two share names delivered all four messages to each group independently"
        finally:
            a.close()
            b.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT-4.8.2: a shared group delivers each message to exactly one member, spreading the load",
     case_group_splits_load_one_each)
case("MQTT-4.8.2 (control): two different share names are independent groups, each receiving every message once",
     case_distinct_groups_each_get_all)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
