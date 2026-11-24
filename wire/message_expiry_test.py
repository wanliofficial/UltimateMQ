#!/usr/bin/env python3
"""MQTT-3.3.2-9 / MQTT-3.3.1-11: the Message Expiry Interval counts down and
retires messages.

The Message Expiry Interval (property 0x02, a four-byte integer of seconds) is
the publisher's statement of how long the message stays current. Two normative
consequences:

  * MQTT-3.3.2-9: when the server forwards the message it MUST set the outgoing
    interval to the time that is *still left*, not echo the value the publisher
    wrote. A subscriber therefore sees a slightly smaller number than was sent,
    counting the seconds already spent in the broker.
  * MQTT-3.3.1-11 / §3.3.2.9: once the interval has elapsed the message is spent.
    A retained copy that has expired MUST NOT be delivered to a later subscriber
    -- it is no longer the "last known good" value of the topic.

Both were implemented (Connection::handleDeliver rewrites the property to the
remaining time and drops an already-expired delivery; Session::deliverRetained
skips an expired retained message) but no wire probe drove them. Pinned here:

  * a PUBLISH with a 100 s expiry is forwarded with an interval strictly below
    100 (it has started counting down) but still positive;
  * a retained PUBLISH with a 1 s expiry, read again after it has lapsed, is not
    delivered to a new subscriber;
  * control: a retained PUBLISH with no expiry is still delivered to a new
    subscriber, so the suppression above is about expiry, not about retained
    delivery being broken.

Usage: message_expiry_test.py <binary> <port>
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

PROP_MESSAGE_EXPIRY_INTERVAL = 0x02


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


def dec_vbi(buf, off):
    mult = 1
    val = 0
    while True:
        b = buf[off]
        off += 1
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            return val, off


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


def publish(pid, topic, payload, qos=1, retain=False, expiry=None):
    header = 0x30 | ((qos & 0x03) << 1) | (0x01 if retain else 0x00)
    props = b""
    if expiry is not None:
        props = bytes([PROP_MESSAGE_EXPIRY_INTERVAL]) + struct.pack("!I", expiry)
    # Variable header: Topic Name, Packet Identifier (QoS>0), Properties.
    body = utf8(topic)
    if qos > 0:
        body += struct.pack("!H", pid)
    body += vbi(len(props)) + props
    body += payload
    return bytes([header]) + vbi(len(body)) + body


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


def parse_publish_expiry(body, qos):
    """Return (topic, expiry_or_None, payload) from a v5 PUBLISH body."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    if qos > 0:
        off += 2  # packet id
    plen, off = dec_vbi(body, off)
    pend = off + plen
    expiry = None
    while off < pend:
        pid = body[off]
        off += 1
        if pid == PROP_MESSAGE_EXPIRY_INTERVAL:
            expiry = struct.unpack("!I", body[off:off + 4])[0]
            off += 4
        else:
            raise AssertionError("unexpected property id 0x%02X" % pid)
    return topic, expiry, body[pend:]


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-expiry-")
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


def case_forwarded_expiry_counts_down():
    """A live PUBLISH with a 100 s expiry is forwarded with a strictly smaller,
    still-positive interval (the broker subtracts the time already spent)."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("exp-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            pub.sendall(connect("exp-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            sub.sendall(subscribe(1, "exp/live", qos=1))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            pub.sendall(publish(7, "exp/live", b"hi", qos=1, expiry=100))
            # Publisher PUBACK.
            assert read_packet(pub)[0] & 0xF0 == 0x40, "no PUBACK for publisher"

            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "expected PUBLISH to subscriber, got %r" % (pkt,))
            _, expiry, payload = parse_publish_expiry(pkt[1], qos=1)
            assert payload == b"hi", "payload altered: %r" % payload
            assert expiry is not None, "the forwarded PUBLISH dropped the Message Expiry Interval"
            assert 0 < expiry < 100, (
                "the forwarded expiry must be the remaining time (< 100 s), got %d" % expiry)
            return "forwarded Message Expiry counted down to %d s (from 100)" % expiry
        finally:
            sub.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_expired_retained_not_delivered():
    """A retained message with a 1 s expiry is not delivered to a subscriber that
    connects after it has lapsed."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("exp-ret-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            pub.sendall(publish(1, "exp/retired", b"stale", qos=1, retain=True, expiry=1))
            assert read_packet(pub)[0] & 0xF0 == 0x40, "no PUBACK for the retained publish"
        finally:
            pub.close()

        # Let the 1 s interval lapse (the sweeper also drops it, at 1 s granularity).
        time.sleep(2.5)

        sub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("exp-ret-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            sub.sendall(subscribe(1, "exp/retired", qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"
            pkt = read_packet(sub, 1.5)
            assert pkt is None, (
                "an expired retained message must not be delivered to a new "
                "subscriber, got %r" % (pkt,))
            return "expired retained message was not delivered"
        finally:
            sub.close()
            stop(proc, workdir)
    finally:
        pass


def case_unexpired_retained_delivered():
    """Control: the same retained publish with NO expiry is still delivered to a
    new subscriber, so the suppression above is about expiry, not retained
    delivery being broken."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("exp-fresh-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            pub.sendall(publish(1, "exp/fresh", b"good", qos=1, retain=True, expiry=None))
            assert read_packet(pub)[0] & 0xF0 == 0x40, "no PUBACK for the retained publish"
        finally:
            pub.close()

        sub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("exp-fresh-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            sub.sendall(subscribe(1, "exp/fresh", qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"
            pkt = read_packet(sub, 1.5)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "a retained message with no expiry must still be delivered, got %r" % (pkt,))
            assert pkt[0] & 0x01 == 1, "a retained snapshot must keep RETAIN=1"
            return "retained message with no expiry still delivered with RETAIN=1"
        finally:
            sub.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT-3.3.2-9: a forwarded PUBLISH carries the remaining Message Expiry Interval (below the published 100 s)",
     case_forwarded_expiry_counts_down)
case("MQTT-3.3.1-11: a retained message whose Message Expiry has elapsed is not delivered to a new subscriber",
     case_expired_retained_not_delivered)
case("MQTT 5 (control): a retained message with no expiry is still delivered to a new subscriber",
     case_unexpired_retained_delivered)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
