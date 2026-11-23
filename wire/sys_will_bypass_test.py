#!/usr/bin/env python3
"""A Will must not bypass the $SYS protection (MQTT-4.1.2-2).

With FLOWMQ_SYS_TOPICS enabled the broker publishes a $SYS/broker/... monitoring
tree and refuses a client PUBLISH to a $SYS topic, so only the broker can write
it. That refusal lives in the PUBLISH handler, but a Will is published by the
broker on the client's behalf and does not go through that handler: it reaches
Broker::publish() directly (both the immediate path on disconnect and the
delayed path in publishDueWills(), and the MQTT-SN Will as well).

Before the fix, a client could therefore register a Will on "$SYS/broker/evil",
drop its connection, and have the broker publish the forged message into the
monitoring tree - an unauthenticated write to the tree the broker reserves for
itself. This probe:

  1. enables FLOWMQ_SYS_TOPICS and subscribes a watcher to "$SYS/#";
  2. reads the genuine retained tree so we know the feature is live;
  3. connects a device whose Will is on "$SYS/broker/evil", drops it abnormally,
     and asserts the forged message NEVER reaches the watcher;
  4. asserts the genuine tree still works: a normal publish on an ordinary topic
     by another client is still delivered, and the $SYS tree keeps updating.

Usage: sys_will_bypass_test.py <binary> <port>
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

FORGED_TOPIC = "$SYS/broker/evil"


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


def connect(cid, clean_start=True, will=None):
    flags = 0x02 if clean_start else 0x00
    if will is not None:
        flags |= 0x04  # Will present
        flags |= (will.get("qos", 0) & 0x03) << 3
        if will.get("retain"):
            flags |= 0x20
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(0)
    body += utf8(cid)
    if will is not None:
        body += enc_len(0)  # empty Will Properties
        body += utf8(will["topic"])
        # Will Payload is Binary Data (§1.5.6): 2-byte length, not a VBI.
        body += struct.pack("!H", len(will["payload"])) + will["payload"]
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
    try:
        header = recv_exact(sock, 1)[0]
    except (socket.timeout, EOFError):
        return None
    val = 0
    mult = 1
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


def parse_publish(header, body):
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    if (header >> 1) & 0x03:
        off += 2  # packet id
    plen = 0
    mult = 1
    while True:
        b = body[off]
        off += 1
        plen += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    return topic, body[off + plen:]


def start():
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-syswill-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_SYS_TOPICS"] = "1"
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            c = socket.create_connection((HOST, port), timeout=0.2)
            c.close()
            time.sleep(0.4)
            return proc, port, workdir
        except Exception:  # noqa: BLE001
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start")


def stop(proc):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    time.sleep(0.5)


def drain(sock, seconds):
    """Collect every PUBLISH delivered within `seconds`; returns a dict."""
    seen = {}
    deadline = time.time() + seconds
    while time.time() < deadline:
        packet = read_packet(sock, timeout=max(0.05, deadline - time.time()))
        if packet is None:
            continue
        header, body = packet
        if header & 0xF0 != 0x30:
            continue
        topic, payload = parse_publish(header, body)
        seen[topic] = payload
    return seen


def read_ack(sock, expected_type, timeout=3.0):
    """Read until a packet of `expected_type` arrives, skipping any PUBLISH the
    broker fans out in between (a watcher on $SYS/# receives live PUBLISHes that
    can sit ahead of a SUBACK on the same socket). Returns the packet or None."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        packet = read_packet(sock, timeout=max(0.05, deadline - time.time()))
        if packet is None:
            return None
        if packet[0] & 0xF0 == expected_type:
            return packet
    return None


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


def case_will_cannot_forge_sys_topic():
    proc, port, workdir = start()
    watcher = device = publisher = None
    try:
        # 1-2: watch the tree and confirm it is genuinely live.
        watcher = socket.create_connection((HOST, port), timeout=5)
        watcher.sendall(connect("sys-watcher"))
        assert read_ack(watcher, 0x20) is not None, "no CONNACK for watcher"
        watcher.sendall(subscribe(1, "$SYS/#"))
        assert read_ack(watcher, 0x90) is not None, "no SUBACK for watcher"
        snapshot = drain(watcher, 1.2)
        assert snapshot, "the $SYS tree is empty: FLOWMQ_SYS_TOPICS is not live"
        assert any(t.startswith("$SYS/broker/") for t in snapshot), (
            "no $SYS/broker/... topics in the snapshot: %r" % sorted(snapshot)[:5])

        # 3: subscribe the watcher to an ordinary topic FIRST, so the Will below
        #    and the control publish can be observed without reading anything in
        #    between (an ack read would swallow a fanned-out PUBLISH).
        watcher.sendall(subscribe(2, "normal/topic"))
        assert read_ack(watcher, 0x90) is not None, "no SUBACK for normal topic"

        # 4: a device whose Will targets the monitoring tree, dropped abnormally
        #    so the Will fires immediately (no Will Delay).
        device = socket.create_connection((HOST, port), timeout=5)
        device.sendall(connect("sys-device",
                               will={"topic": FORGED_TOPIC, "payload": b"HACKED",
                                     "qos": 0, "retain": False}))
        assert read_ack(device, 0x20) is not None, "no CONNACK for device"
        device.close()

        # 5: a normal publisher on an ordinary topic proves the fan-out still
        #    works while the forged Will is being refused.
        publisher = socket.create_connection((HOST, port), timeout=5)
        publisher.sendall(connect("sys-publisher"))
        assert read_ack(publisher, 0x20) is not None, "no CONNACK for publisher"
        publisher.sendall(publish_qos0("normal/topic", b"still-works"))

        seen = drain(watcher, 2.5)
        assert FORGED_TOPIC not in seen, (
            "the Will bypassed the $SYS protection: the broker published %r=%r into "
            "its own monitoring tree" % (FORGED_TOPIC, seen[FORGED_TOPIC]))
        assert seen.get("normal/topic") == b"still-works", (
            "ordinary fan-out broke: normal/topic did not arrive (%r)"
            % (seen.get("normal/topic"),))
        # 5: the genuine tree keeps updating after the refused Will.
        assert any(t.startswith("$SYS/broker/") for t in seen), (
            "the broker stopped refreshing its own $SYS tree after refusing the "
            "Will: %r" % sorted(seen)[:5])
        return "a $SYS Will is refused while ordinary traffic and the real tree still work"
    finally:
        for s in (watcher, device, publisher):
            try:
                if s is not None:
                    s.close()
            except Exception:  # noqa: BLE001
                pass
        stop(proc)


case("$SYS protection: a client Will on a $SYS topic cannot forge the monitoring tree (MQTT-4.1.2-2)",
     case_will_cannot_forge_sys_topic)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
