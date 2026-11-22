#!/usr/bin/env python3
"""MQTT 5 Topic Alias resolution over a real connection (MQTT-3.3.2-1, §3.3.2.7/8,
§3.3.2.3.4, OASIS MQTT-474).

The Frame codec has unit tests for parsing the Topic Alias property, and the
Connection resolver (Connection::resolveTopicAlias) carries spec citations for every
branch, but no wire or integration test ever drives a real client through those
branches. This probe launches the real broker and pins the whole contract:

  1. Register then reuse: a PUBLISH with a Topic Name and a Topic Alias registers
     the mapping; a later alias-only PUBLISH (zero-length Topic Name) is expanded
     to the same topic and routed to a subscriber.
  2. An alias-only PUBLISH with an UNMAPPED alias is a Protocol Error -> the broker
     answers DISCONNECT 0x82 (Protocol Error) and closes (OASIS MQTT-474).
  3. Topic Alias 0 is invalid -> DISCONNECT 0x94 (Topic Alias invalid).
  4. An alias above the advertised maximum (16) -> DISCONNECT 0x94.
  5. A zero-length Topic Name with no Topic Alias at all -> DISCONNECT 0x82
     (MQTT-3.3.2-7).

Usage: topic_alias_test.py <binary> <port>
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

# Property IDs (MQTT §2.2.2).
PROP_TOPIC_ALIAS = 0x23
# Reason codes (MQTT §2.4).
REASON_SUCCESS = 0x00
REASON_PROTOCOL_ERROR = 0x82
REASON_TOPIC_ALIAS_INVALID = 0x94
# The broker advertises Topic Alias Maximum = 16 in CONNACK.
ALIAS_MAX = 16


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


def publish(topic, payload, alias=None, qos=0, pid=1):
    """MQTT 5 QoS 0 PUBLISH. alias=None sends an empty property block."""
    body = utf8(topic)
    if alias is None:
        body += enc_len(0)
    else:
        prop = bytes([PROP_TOPIC_ALIAS]) + struct.pack("!H", alias)
        body += enc_len(len(prop)) + prop
    body += payload
    return bytes([0x30 | (qos << 1)]) + enc_len(len(body)) + body


def publish_alias_only(alias, payload):
    """A zero-length Topic Name carrying only a Topic Alias property."""
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
    """Read one MQTT packet -> (header, body) where body has the Remaining Length
    already stripped."""
    sock.settimeout(timeout)
    header = recv_exact(sock, 1)[0]
    mult = 1
    val = 0
    i = 0
    while True:
        b = recv_exact(sock, 1)[0]
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = recv_exact(sock, val) if val else b""
    return header, body


def parse_publish_topic(body):
    tlen = struct.unpack("!H", body[0:2])[0]
    return body[2:2 + tlen].decode()


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-alias-")
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


def case_alias_registers_then_reuses():
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("alias-reg"))
            header, _ = read_packet(c)
            assert header & 0xF0 == 0x20, "no CONNACK: %r" % header

            c.sendall(subscribe(1, "sensors/#"))
            header, _ = read_packet(c)
            assert header & 0xF0 == 0x90, "no SUBACK: %r" % header

            # Register alias 1 -> "sensors/a" with a real Topic Name.
            c.sendall(publish("sensors/a", b"first", alias=1))
            header, body = read_packet(c)
            assert header & 0xF0 == 0x30, "first PUBLISH not delivered: %r" % header
            assert parse_publish_topic(body) == "sensors/a", (
                "first delivery had the wrong topic: %r" % parse_publish_topic(body))

            # Reuse alias 1 with a zero-length Topic Name; it must expand to the
            # same topic and route identically.
            c.sendall(publish_alias_only(1, b"second"))
            header, body = read_packet(c)
            assert header & 0xF0 == 0x30, "alias-only PUBLISH not delivered: %r" % header
            assert parse_publish_topic(body) == "sensors/a", (
                "alias-only delivery did not expand to sensors/a: %r" % parse_publish_topic(body))
            return "alias 1 registered and reused for sensors/a"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def _expect_disconnect(name, packet, expected_reason):
    """Send `packet` on a fresh connection and assert the broker answers DISCONNECT
    with `expected_reason` and then closes."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect(name))
            header, _ = read_packet(c)
            assert header & 0xF0 == 0x20, "no CONNACK: %r" % header
            c.sendall(packet)
            header, body = read_packet(c)
            assert header & 0xF0 == 0xE0, (
                "%s: expected a DISCONNECT, got header 0x%02x" % (name, header))
            reason = body[0] if body else 0x00
            assert reason == expected_reason, (
                "%s: DISCONNECT reason was 0x%02x, expected 0x%02x"
                % (name, reason, expected_reason))
            return "DISCONNECT 0x%02x" % reason
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_alias_only_unknown_is_protocol_error():
    return _expect_disconnect(
        "unknown-alias", publish_alias_only(9, b"x"), REASON_PROTOCOL_ERROR)


def case_alias_zero_is_invalid():
    return _expect_disconnect(
        "alias-zero", publish("a/b", b"x", alias=0), REASON_TOPIC_ALIAS_INVALID)


def case_alias_above_max_is_invalid():
    return _expect_disconnect(
        "alias-above-max", publish("a/b", b"x", alias=ALIAS_MAX + 1),
        REASON_TOPIC_ALIAS_INVALID)


def case_empty_topic_no_alias_is_protocol_error():
    # MQTT-3.3.2-7: a zero-length Topic Name with no Topic Alias property is a
    # Protocol Error -> DISCONNECT 0x82. The packet must still be structurally
    # valid MQTT 5 (it carries a Properties Length of 0), or the Frame decoder
    # rejects it as Malformed Packet (0x81) before the resolver runs.
    body = utf8("") + enc_len(0)  # empty Topic Name, empty property block
    packet = bytes([0x30]) + enc_len(len(body)) + body
    return _expect_disconnect(
        "empty-no-alias", packet, REASON_PROTOCOL_ERROR)


case("Topic Alias: a PUBLISH registers the alias, an alias-only PUBLISH reuses it",
     case_alias_registers_then_reuses)
case("Topic Alias: an alias-only PUBLISH with an unmapped alias -> DISCONNECT 0x82",
     case_alias_only_unknown_is_protocol_error)
case("Topic Alias: alias 0 -> DISCONNECT 0x94 (Topic Alias invalid)",
     case_alias_zero_is_invalid)
case("Topic Alias: alias above the advertised maximum -> DISCONNECT 0x94",
     case_alias_above_max_is_invalid)
case("Topic Alias: zero-length Topic Name with no alias -> DISCONNECT 0x82 (MQTT-3.3.2-7)",
     case_empty_topic_no_alias_is_protocol_error)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
