#!/usr/bin/env python3
"""Core retained-message delivery semantics over the wire.

Retained messages are one of MQTT's three original features, and the contract
around the RETAIN flag is a frequent source of interop bugs. The $SYS tree is
delivered through the retained path (covered by sys_topics_test.py), but the
general contract for an arbitrary client topic is not exercised over the wire:

  1. a message published with RETAIN=1 is stored, and a NEW subscriber receives
     it immediately on SUBSCRIBE, with the RETAIN flag still set
     (MQTT-3.3.1-9: the last retained message on a matching topic is sent to a
     new subscriber with RETAIN=1);
  2. a subscriber that was ALREADY subscribed when the message was published
     receives it as a live delivery with the RETAIN flag CLEARED (MQTT-3.3.1-8:
     a Publish sent to a matching established subscription has RETAIN=0 unless
     the subscription asked for Retain As Published);
  3. a zero-length payload published with RETAIN=1 CLEARS the retained message
     (MQTT-3.3.1-9 / §3.3.1-9), so a later subscriber receives nothing;
  4. a wildcard subscription receives the retained messages on every matching
     topic.

Usage: retained_messages_test.py <binary> <port>
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


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0, options=0x00):
    # One Subscription Options byte: the requested QoS lives in its low two
    # bits, so fold the QoS into the options byte rather than emitting both a
    # separate QoS byte and an options byte (the parser reads exactly one byte).
    byte = (options & 0xFC) | (qos & 0x03)
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([byte])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(pid, topic, payload, retain):
    # QoS 0 PUBLISH: Topic Name, Property Length, Payload. There is NO Packet
    # Identifier at QoS 0, so omit it (a stray 2 bytes would be read as the
    # Property Length and corrupt every parse).
    body = utf8(topic) + enc_len(0) + payload
    header = 0x30 | (1 if retain else 0)  # QoS 0, retain bit
    return bytes([header]) + enc_len(len(body)) + body


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
    """QoS 0 PUBLISH body: topic (len-prefixed), property length (VBI), payload.
    Returns (topic, retain_unused, payload)."""
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
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
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-retained-")
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


def _connect_subscribe(port, cid, filt, qos=0):
    c = socket.create_connection((HOST, port), timeout=5)
    c.sendall(connect(cid))
    assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK for %s" % cid
    c.sendall(subscribe(1, filt, qos=qos))
    assert read_packet(c)[0] & 0xF0 == 0x90, "no SUBACK for %s" % cid
    return c


def case_snapshot_to_new_subscriber_keeps_retain():
    proc, port, workdir = start()
    try:
        pub = _connect_subscribe(port, "ret-pub", "other")  # subscribed, irrelevant
        try:
            # Publish a retained message on a topic nobody subscribes to yet.
            pub.sendall(publish(1, "snap/t", b"snapshot", retain=True))

            # A NEW subscriber must receive it immediately with RETAIN=1.
            sub = _connect_subscribe(port, "ret-new", "snap/t")
            try:
                header, body = read_packet(sub, timeout=3.0)
                assert header & 0xF0 == 0x30, "no retained PUBLISH delivered: %r" % header
                assert header & 0x01 == 1, (
                    "the retained snapshot was delivered with RETAIN cleared; a new "
                    "subscriber must receive it with RETAIN=1 (MQTT-3.3.1-9)")
                topic, payload = parse_publish(body)
                assert (topic, payload) == ("snap/t", b"snapshot"), "%r/%r" % (topic, payload)
                return "retained snapshot delivered to a new subscriber with RETAIN=1"
            finally:
                sub.close()
        finally:
            pub.close()
    finally:
        stop(proc, workdir)


def case_live_delivery_to_existing_clears_retain():
    proc, port, workdir = start()
    try:
        sub = _connect_subscribe(port, "ret-live", "live/t")
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("ret-live-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK for publisher"
            # An already-subscribed client gets the message as a LIVE delivery.
            pub.sendall(publish(2, "live/t", b"live", retain=True))
            header, body = read_packet(sub, timeout=3.0)
            assert header & 0xF0 == 0x30, "no live PUBLISH delivered: %r" % header
            assert header & 0x01 == 0, (
                "a live delivery to an established subscription kept the RETAIN flag; "
                "it must be cleared (MQTT-3.3.1-8) unless Retain As Published was asked")
            topic, payload = parse_publish(body)
            assert (topic, payload) == ("live/t", b"live"), "%r/%r" % (topic, payload)
            return "live delivery to an existing subscriber carried RETAIN=0"
        finally:
            pub.close()
            sub.close()
    finally:
        stop(proc, workdir)


def case_empty_payload_clears_retained():
    proc, port, workdir = start()
    try:
        pub = _connect_subscribe(port, "clr-pub", "other")
        try:
            # Publish a retained message, then clear it with a zero-length payload.
            pub.sendall(publish(1, "clr/t", b"data", retain=True))
            pub.sendall(publish(2, "clr/t", b"", retain=True))

            # A new subscriber must receive NOTHING on the cleared topic.
            sub = _connect_subscribe(port, "clr-new", "clr/t")
            try:
                header, body = read_packet(sub, timeout=1.5)
                raise AssertionError(
                    "a subscriber received a PUBLISH after the retained message was "
                    "cleared: %r" % header)
            except socket.timeout:
                pass  # expected: no retained message remains
            return "a zero-length retained payload cleared the retained message"
        finally:
            pub.close()
    finally:
        stop(proc, workdir)


def case_wildcard_receives_matching_retained():
    proc, port, workdir = start()
    try:
        pub = _connect_subscribe(port, "wc-pub", "other")
        try:
            pub.sendall(publish(1, "wc/a/1", b"one", retain=True))
            pub.sendall(publish(2, "wc/b/2", b"two", retain=True))
            pub.sendall(publish(3, "wc/a/3", b"three", retain=True))

            # A wildcard subscriber gets the retained message for every matching topic.
            sub = _connect_subscribe(port, "wc-new", "wc/#")
            try:
                got = {}
                deadline = time.time() + 2.0
                while time.time() < deadline and len(got) < 3:
                    try:
                        header, body = read_packet(sub, 0.5)
                    except (EOFError, socket.timeout):
                        break
                    if header & 0xF0 == 0x30 and header & 0x01 == 1:
                        topic, payload = parse_publish(body)
                        got[topic] = payload
                assert got == {"wc/a/1": b"one", "wc/b/2": b"two", "wc/a/3": b"three"}, (
                    "wildcard subscription received the wrong retained set: %r" % got)
                return "wildcard subscription received all matching retained messages"
            finally:
                sub.close()
        finally:
            pub.close()
    finally:
        stop(proc, workdir)


case("retained: a message published with RETAIN=1 is delivered to a new subscriber with RETAIN=1",
     case_snapshot_to_new_subscriber_keeps_retain)
case("retained: a live delivery to an already-subscribed client has the RETAIN flag cleared",
     case_live_delivery_to_existing_clears_retain)
case("retained: a zero-length payload published with RETAIN=1 clears the retained message",
     case_empty_payload_clears_retained)
case("retained: a wildcard subscription receives every matching retained message",
     case_wildcard_receives_matching_retained)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
