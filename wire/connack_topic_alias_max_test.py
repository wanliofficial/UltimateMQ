#!/usr/bin/env python3
"""MQTT 5 server-advertised Topic Alias Maximum (§3.1.2.9.6, §2.2.2.2).

A client may compress its outgoing PUBLISH Topic Names by registering a Topic
Alias (0x23, a two-byte integer) on one packet and sending the bare alias on
later ones. How many aliases may exist at once is a per-server policy the client
cannot guess, so MQTT 5 requires the Server to state it: the CONNACK MUST carry
the Topic Alias Maximum property (0x22), and an alias above that maximum is
invalid [§3.1.2.9.6, §3.3.2.3.4]. A value of 0 means the Server does not support
Topic Alias at all.

The broker advertises 16 and enforces it (Connection::sendConnack sets
TOPIC_ALIAS_MAXIMUM; Connection::resolveTopicAlias rejects alias 0 and anything
above it). topic_alias_test.py drives the resolver branches but never checks that
the CONNACK actually announces the maximum, nor that the announced number is the
same one the server enforces. Pinned here:

  * the success CONNACK carries a Topic Alias Maximum property whose value is a
    positive number (the broker uses 16);
  * that exact value is the enforcement boundary: an alias AT the maximum is
    accepted and routes, while one above it is refused with DISCONNECT 0x94
    (Topic Alias invalid) -- so the advertised cap and the enforced cap cannot
    silently drift apart.

Usage: connack_topic_alias_max_test.py <binary> <port>
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

PROP_TOPIC_ALIAS = 0x23
PROP_TOPIC_ALIAS_MAXIMUM = 0x22
REASON_SUCCESS = 0x00
REASON_TOPIC_ALIAS_INVALID = 0x94


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


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(topic, payload, alias=None, qos=0):
    body = utf8(topic)
    if alias is None:
        body += enc_len(0)
    else:
        prop = bytes([PROP_TOPIC_ALIAS]) + struct.pack("!H", alias)
        body += enc_len(len(prop)) + prop
    body += payload
    return bytes([0x30 | (qos << 1)]) + enc_len(len(body)) + body


def publish_alias_only(alias, payload):
    prop = bytes([PROP_TOPIC_ALIAS]) + struct.pack("!H", alias)
    body = utf8("") + enc_len(len(prop)) + prop + payload
    return bytes([0x30]) + enc_len(len(body)) + body


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
        b = recv_exact(sock, 1)[0]
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = recv_exact(sock, val) if val else b""
    return header, body


def parse_connack_topic_alias_max(body):
    """Extract the Topic Alias Maximum (0x22) from a v5 CONNACK body.

    The CONNACK also carries other properties (Session Expiry Interval, Receive
    Maximum, ...), so this walks the property block by known fixed widths and
    returns the alias maximum, or None if the property is absent.
    """
    # body[0] = Acknowledge Flags, body[1] = Reason Code, body[2:] = properties.
    plen, off = _dec_vbi(body, 2)
    pend = off + plen
    fixed = {
        0x11: 4,  # Session Expiry Interval
        0x21: 2,  # Receive Maximum
        0x22: 2,  # Topic Alias Maximum
        0x24: 1,  # Maximum QoS
        0x25: 1,  # Retain Available
        0x27: 4,  # Maximum Packet Size
        0x28: 1,  # Wildcard Subscription Available
        0x29: 1,  # Subscription Identifier Available
        0x2A: 1,  # Shared Subscription Available
    }
    while off < pend:
        pid = body[off]
        off += 1
        if pid == PROP_TOPIC_ALIAS_MAXIMUM:
            return struct.unpack("!H", body[off:off + 2])[0]
        if pid in fixed:
            off += fixed[pid]
        else:
            # Fallback: treat as a UTF-8 string (2-byte length prefix).
            slen = struct.unpack("!H", body[off:off + 2])[0]
            off += 2 + slen
    return None


def _dec_vbi(buf, off):
    mult = 1
    val = 0
    while True:
        b = buf[off]
        off += 1
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            return val, off


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-tamax-")
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


def case_connack_advertises_alias_max():
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("tamax-adv"))
            pkt = read_packet(c)
            assert pkt is not None, "no CONNACK"
            header, body = pkt
            assert header & 0xF0 == 0x20, "expected CONNACK, got 0x%02x" % header
            assert body[1] == REASON_SUCCESS, (
                "CONNACK not success: 0x%02x" % body[1])
            tamax = parse_connack_topic_alias_max(body)
            assert tamax is not None, (
                "the CONNACK must carry the Topic Alias Maximum property (§3.1.2.9.6)")
            assert tamax > 0, (
                "the broker advertises Topic Alias support, so the maximum must be "
                "positive, got %d" % tamax)
            return "CONNACK advertises Topic Alias Maximum = %d" % tamax
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_advertised_max_is_the_enforcement_boundary():
    """The number the CONNACK advertises is exactly where enforcement kicks in:
    an alias AT the maximum routes, one above it is DISCONNECT 0x94."""
    proc, port, workdir = start()
    try:
        # First learn the advertised maximum.
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("tamax-bound"))
            header, body = read_packet(c)
            assert header & 0xF0 == 0x20, "no CONNACK"
            tamax = parse_connack_topic_alias_max(body)
            assert tamax is not None and tamax > 0, (
                "need a positive advertised maximum to test the boundary, got %r" % tamax)

            c.sendall(subscribe(1, "tamax/#", qos=0))
            assert read_packet(c)[0] & 0xF0 == 0x90, "no SUBACK"

            # An alias exactly at the maximum, with a Topic Name to register it,
            # must be accepted and routed (not refused).
            c.sendall(publish("tamax/at", b"ok", alias=tamax))
            pkt = read_packet(c, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "an alias AT the advertised maximum (%d) must be accepted, got %r"
                % (tamax, pkt))
            assert pkt[1][2:2 + struct.unpack('!H', pkt[1][0:2])[0]].decode() == "tamax/at", (
                "the at-maximum alias did not route to its topic")
        finally:
            c.close()

        # A separate connection: an alias one above the maximum must be refused.
        c2 = socket.create_connection((HOST, port), timeout=5)
        try:
            c2.sendall(connect("tamax-over"))
            assert read_packet(c2)[0] & 0xF0 == 0x20, "no CONNACK (over-max)"
            c2.sendall(publish("tamax/over", b"x", alias=tamax + 1))
            pkt = read_packet(c2, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0xE0, (
                "an alias above the advertised maximum must draw a DISCONNECT, got %r"
                % (pkt,))
            assert pkt[1][0] == REASON_TOPIC_ALIAS_INVALID, (
                "over-maximum alias must be DISCONNECT 0x94, got 0x%02x" % pkt[1][0])
            return ("alias %d accepted, alias %d refused (advertised max = %d)"
                    % (tamax, tamax + 1, tamax))
        finally:
            c2.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.1.2.9.6: a successful CONNACK carries a positive Topic Alias Maximum property",
     case_connack_advertises_alias_max)
case("MQTT-3.3.2.3.4: the advertised Topic Alias Maximum is the exact enforcement boundary (at-max accepted, above-max refused 0x94)",
     case_advertised_max_is_the_enforcement_boundary)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
