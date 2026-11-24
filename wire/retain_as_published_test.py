#!/usr/bin/env python3
"""MQTT 5 Retain As Published subscription option (§3.8.3.1).

The RETAIN flag on a PUBLISH tells a subscriber whether the message is the
current retained value of its topic. But a live message the broker forwards is
not a retained snapshot -- it was just published -- so by default the broker
clears the RETAIN flag on the way to the subscriber
[MQTT-3.3.1-8 / §3.8.3-1], reserving RETAIN=1 for the retained snapshot a
subscriber gets when it first subscribes.

A subscription may ask for the as-published flag instead: the "Retain As
Published" option (Subscription Options bit 3, value 0x08) makes the broker
forward the message with the RETAIN flag exactly as the publisher set it,
rather than clearing it [§3.8.3.1]:

  "If Retain As Published is set to 1, the Server MUST set the RETAIN flag to 1
   when the message was originally published with that flag set to 1, and to 0
   when the original message had that flag set to 0."

So a LIVE, non-retained publish forwarded to a Retain As Published subscriber
keeps RETAIN = 0 (the as-published value), and a LIVE publish that was sent
RETAIN = 1 keeps RETAIN = 1 -- in both cases preserving the original flag
instead of forcing it to 0.

retained_messages_test.py pins the DEFAULT (flag cleared on live delivery); this
probe pins the option that overrides it. Both directions are exercised, plus the
retained-snapshot delivery which always carries RETAIN=1 regardless of the
option (so the option does not break the snapshot path).

Usage: retain_as_published_test.py <binary> <port>
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

RETAIN_AS_PUBLISHED = 0x08  # Subscription Options bit 3 (§3.8.3.1)


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


def subscribe(pid, filt, options=0x00, qos=0):
    # Subscription Options byte: bits 0-1 QoS, bit 3 Retain As Published (§3.8.3.1).
    opts = (options & 0x0F) | (qos & 0x03)
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([opts])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(pid, topic, payload, retain=False, qos=0):
    header = 0x30 | ((qos & 0x03) << 1) | (0x01 if retain else 0x00)
    body = utf8(topic)
    if qos > 0:
        body += struct.pack("!H", pid)
    body += enc_len(0) + payload
    return bytes([header]) + enc_len(len(body)) + body


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


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-rap-")
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


def case_live_nonretain_with_option_keeps_flag_clear():
    """A Retain As Published subscriber receives a LIVE, non-retained publish with
    RETAIN = 0 -- the as-published flag is 0, and the option preserves it."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("rap-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            pub.sendall(connect("rap-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            sub.sendall(subscribe(1, "rap/plain", options=RETAIN_AS_PUBLISHED, qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            # QoS 0, RETAIN = 0.
            pub.sendall(publish(0, "rap/plain", b"live", retain=False, qos=0))
            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "expected the live PUBLISH, got %r" % (pkt,))
            assert pkt[0] & 0x01 == 0, (
                "a live non-retained publish must keep RETAIN=0 even with "
                "Retain As Published (the as-published flag is 0)")
            return "Retain As Published: live non-retained publish forwarded with RETAIN=0"
        finally:
            sub.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_live_retain_with_option_sets_flag():
    """The point of the option: a LIVE publish that WAS sent with RETAIN=1 is
    forwarded with RETAIN=1 preserved, where the default would have cleared it."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("rap2-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            pub.sendall(connect("rap2-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            sub.sendall(subscribe(1, "rap/flag", options=RETAIN_AS_PUBLISHED, qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            # QoS 0, RETAIN = 1. This is a LIVE forward (the subscriber already
            # exists), but the as-published flag is 1, so Retain As Published must
            # preserve RETAIN=1.
            pub.sendall(publish(0, "rap/flag", b"flagged", retain=True, qos=0))
            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "expected the live PUBLISH, got %r" % (pkt,))
            assert pkt[0] & 0x01 == 1, (
                "Retain As Published must preserve the as-published RETAIN=1 on a "
                "live publish; the default would have cleared it")
            return "Retain As Published: live publish sent with RETAIN=1 forwarded with RETAIN=1 preserved"
        finally:
            sub.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_default_clears_live_retain_flag():
    """Control: WITHOUT the option, the same live publish sent with RETAIN=1 is
    forwarded with the flag CLEARED (the default, MQTT-3.3.1-8). This is the
    behaviour the option overrides, proving the option above is what set it."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("rap3-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            pub.sendall(connect("rap3-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            # No Retain As Published (options = 0x00).
            sub.sendall(subscribe(1, "rap/default", options=0x00, qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            pub.sendall(publish(0, "rap/default", b"flagged", retain=True, qos=0))
            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "expected the live PUBLISH, got %r" % (pkt,))
            assert pkt[0] & 0x01 == 0, (
                "without Retain As Published the live RETAIN=1 publish must be "
                "forwarded with the flag cleared (default, MQTT-3.3.1-8)")
            return "default: live RETAIN=1 publish forwarded with RETAIN cleared (0)"
        finally:
            sub.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_retained_snapshot_keeps_flag_with_option():
    """A retained-snapshot delivery to a new Retain As Published subscriber always
    carries RETAIN=1 (it is the retained value), regardless of the option -- the
    option governs live forwards, not the snapshot."""
    proc, port, workdir = start()
    try:
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            pub.sendall(connect("rap4-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            pub.sendall(publish(1, "rap/snap", b"retained-value", retain=True, qos=1))
            assert read_packet(pub)[0] & 0xF0 == 0x40, "no PUBACK for the retained publish"
        finally:
            pub.close()

        sub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("rap4-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            sub.sendall(subscribe(1, "rap/snap", options=RETAIN_AS_PUBLISHED, qos=1))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"
            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "a retained message must reach the new subscriber, got %r" % (pkt,))
            assert pkt[0] & 0x01 == 1, (
                "a retained-snapshot delivery must carry RETAIN=1")
            return "retained snapshot to a Retain As Published subscriber keeps RETAIN=1"
        finally:
            sub.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT-3.3.1-8 / §3.8.3.1: Retain As Published preserves the as-published flag -- a live non-retained publish keeps RETAIN=0",
     case_live_nonretain_with_option_keeps_flag_clear)
case("MQTT-3.8.3.1: Retain As Published preserves the as-published flag -- a live publish sent with RETAIN=1 keeps RETAIN=1",
     case_live_retain_with_option_sets_flag)
case("MQTT-3.3.1-8 (control): without the option a live RETAIN=1 publish is forwarded with the flag cleared",
     case_default_clears_live_retain_flag)
case("MQTT-3.3.1-9: a retained-snapshot delivery to a Retain As Published subscriber still carries RETAIN=1",
     case_retained_snapshot_keeps_flag_with_option)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
