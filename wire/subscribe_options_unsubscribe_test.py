#!/usr/bin/env python3
"""MQTT-3.8.4-1 / MQTT-3.11.3: reserved SUBSCRIBE option bits and UNSUBSCRIBE
of a never-subscribed filter.

Two subscription-path rules the broker enforces:

  * MQTT-3.8.4-1: "The Server checks the SUBSCRIBE properties... If the
    remaining bits are not 0x00 it is a Malformed Packet." More precisely,
    in MQTT 5 bits 6 and 7 of each Subscription Options byte are reserved and
    MUST be set to 0; in MQTT 3.1.1 bits 2-5 are reserved too. A SUBSCRIBE that
    sets any of them must be rejected as Malformed (0x81), not silently
    accepted with the reserved bits reinterpreted as QoS / noLocal / retain
    handling.

  * MQTT-3.11.3: an MQTT 5 UNSUBACK carries "a Reason Code... per Topic
    Filter... in the order in which the Topic Filters were stored". Removing a
    subscription that does not exist is not an error: the broker answers
    No Subscription Existed (0x11) for that filter. A filter that was really
    subscribed and is removed answers Success (0x00).

On the malformed SUBSCRIBE the broker sends DISCONNECT 0x81 (a packet the
connection is already inside a session for, so closeWithReason uses the v5
DISCONNECT form). On the UNSUBACK cases the body is
Packet Id (2) + Property Length (1) + one Reason Code per filter.

Usage: subscribe_options_unsubscribe_test.py <binary> <port>
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

REASON_MALFORMED_PACKET = 0x81
REASON_SUCCESS = 0x00
REASON_NO_SUBSCRIPTION_EXISTED = 0x11

SUBSCRIBE = 0x82
SUBACK = 0x90
UNSUBSCRIBE = 0xA2
UNSUBACK = 0xB0


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


def connect(cid, version=5):
    flags = 0x02
    body = (utf8("MQTT") + bytes([version, flags]) + struct.pack("!H", 60)
            + enc_len(0) + utf8(cid))
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, options, version=5):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([options])
    return bytes([SUBSCRIBE]) + enc_len(len(body)) + body


def unsubscribe(pid, filt, version=5):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt)
    return bytes([UNSUBSCRIBE]) + enc_len(len(body)) + body


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


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-sub-")
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


def _expect_malformed_disconnect(sent_pkt, label, connect_version=5):
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("sub-reserved", version=connect_version))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK (%s)" % label
            c.sendall(sent_pkt)
            if connect_version == 5:
                # MQTT 5: the broker explains the close with DISCONNECT 0x81.
                pkt = read_packet(c, 2.0)
                assert pkt is not None, "broker closed with no DISCONNECT (%s)" % label
                header, body = pkt
                assert header & 0xF0 == 0xE0, (
                    "expected a DISCONNECT for %s, got header 0x%02X" % (label, header))
                assert body and body[0] == REASON_MALFORMED_PACKET, (
                    "%s must be MALFORMED_PACKET (0x%02X), got %r"
                    % (label, REASON_MALFORMED_PACKET, body))
                return "%s -> DISCONNECT 0x81" % label
            # MQTT 3.1.1: the server has no DISCONNECT packet, so a malformed
            # SUBSCRIBE simply ends the connection (MQTT-3.9 / closeWithReason).
            # Confirm the peer closes with no further bytes.
            try:
                extra = c.recv(16)
            except socket.timeout:
                extra = b"<timeout>"
            assert extra == b"", (
                "a 3.1.1 malformed SUBSCRIBE must close the connection with no "
                "further bytes, got %r" % extra)
            return "%s -> connection closed (3.1.1 has no DISCONNECT)" % label
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_v5_reserved_bit6():
    # options 0x40 sets bit 6 (reserved in v5); low bits (QoS 0) are valid.
    return _expect_malformed_disconnect(
        subscribe(1, "a/b", options=0x40), "a v5 SUBSCRIBE with reserved bit 6 set (0x40)")


def case_v5_reserved_bit7():
    # options 0x80 sets bit 7 (reserved in v5).
    return _expect_malformed_disconnect(
        subscribe(1, "a/b", options=0x80), "a v5 SUBSCRIBE with reserved bit 7 set (0x80)")


def case_v311_reserved_bit2():
    # MQTT 3.1.1 reserves bits 2-5; options 0x04 sets bit 2. Connect at
    # protocol level 4 so the broker validates the options under 3.1.1 rules
    # (a v5 connection would read 0x04 as the legal noLocal bit).
    return _expect_malformed_disconnect(
        subscribe(1, "a/b", options=0x04, version=4),
        "a 3.1.1 SUBSCRIBE with reserved bit 2 set (0x04)",
        connect_version=4)


def case_valid_subscribe_accepted():
    """Control: a v5 SUBSCRIBE with only the low three bits used (QoS 0, no
    other options) is accepted, so the rejection above is about the reserved
    bits, not about subscribing itself."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("sub-ok"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(subscribe(1, "a/b", options=0x00))
            h, b = read_packet(c)
            assert h & 0xF0 == 0x90, "expected SUBACK, got header 0x%02X" % h
            # body: pid(2) + prop_len(1) + reason(1); granted QoS 0 -> 0x00.
            reason = b[3] if len(b) > 3 else REASON_MALFORMED_PACKET
            assert reason == REASON_SUCCESS, (
                "a valid SUBSCRIBE must be granted QoS 0 (0x00), got 0x%02X" % reason)
            return "a valid v5 SUBSCRIBE is granted SUBACK 0x00"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_unsubscribe_unknown_filter():
    """An MQTT 5 UNSUBSCRIBE of a filter that was never subscribed gets
    UNSUBACK 0x11 (NO_SUBSCRIPTION_EXISTED), not an error and not Success."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("unsub-unknown"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(unsubscribe(1, "never/subbed"))
            h, b = read_packet(c)
            assert h == UNSUBACK, "expected UNSUBACK, got header 0x%02X" % h
            reason = b[3] if len(b) > 3 else REASON_MALFORMED_PACKET
            assert reason == REASON_NO_SUBSCRIPTION_EXISTED, (
                "UNSUBSCRIBE of a never-subscribed filter must be 0x11 "
                "(NO_SUBSCRIPTION_EXISTED), got 0x%02X" % reason)
            return "UNSUBSCRIBE of a never-subscribed filter -> UNSUBACK 0x11"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_unsubscribe_known_filter():
    """Control: after a real SUBSCRIBE, UNSUBSCRIBING the same filter gets
    UNSUBACK 0x00 (Success), so the 0x11 path is specific to the filter not
    existing."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("unsub-known"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(subscribe(1, "a/b", options=0x00))
            assert read_packet(c)[0] & 0xF0 == 0x90, "no SUBACK"
            c.sendall(unsubscribe(2, "a/b"))
            h, b = read_packet(c)
            assert h == UNSUBACK, "expected UNSUBACK, got header 0x%02X" % h
            reason = b[3] if len(b) > 3 else REASON_MALFORMED_PACKET
            assert reason == REASON_SUCCESS, (
                "UNSUBSCRIBE of a subscribed filter must be 0x00 (Success), "
                "got 0x%02X" % reason)
            return "UNSUBSCRIBE of a subscribed filter -> UNSUBACK 0x00"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.8.4-1: a v5 SUBSCRIBE with reserved bit 6 set (options 0x40) is refused DISCONNECT 0x81 (MALFORMED_PACKET)",
     case_v5_reserved_bit6)
case("MQTT-3.8.4-1: a v5 SUBSCRIBE with reserved bit 7 set (options 0x80) is refused DISCONNECT 0x81 (MALFORMED_PACKET)",
     case_v5_reserved_bit7)
case("MQTT-3.8.4-1: a 3.1.1 SUBSCRIBE with reserved bit 2 set (options 0x04) ends the connection (3.1.1 has no server DISCONNECT)",
     case_v311_reserved_bit2)
case("MQTT 5 (control): a SUBSCRIBE using only the low three option bits (QoS 0) is granted SUBACK 0x00",
     case_valid_subscribe_accepted)
case("MQTT-3.11.3: an UNSUBSCRIBE of a filter that was never subscribed is answered UNSUBACK 0x11 (NO_SUBSCRIPTION_EXISTED)",
     case_unsubscribe_unknown_filter)
case("MQTT 5 (control): an UNSUBSCRIBE of a filter that was subscribed is answered UNSUBACK 0x00 (Success)",
     case_unsubscribe_known_filter)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
