#!/usr/bin/env python3
"""MQTT-3.1.3-6 / -7 / -8: a zero-length Client Identifier.

A Client may connect with an empty Client Identifier, but only if it also asks for
a clean session -- there would be no name under which a session could be resumed
otherwise. The rules:

  * MQTT-3.1.3-6: with Clean Start = 1 the Server may accept the empty id, assign
    a unique one, and treat the client as if it had supplied that id;
  * MQTT-3.1.3-7 (MQTT 5): when it does, the Server MUST return the assigned id in
    the CONNACK, as the Assigned Client Identifier property (0x12), so the client
    knows the name its session now has;
  * MQTT-3.1.3-8 / §3.1.3.1: with Clean Start = 0 an empty id is not acceptable --
    the Session cannot be resumed under no name. MQTT 5 answers CONNACK 0x85
    (Client Identifier not valid); MQTT 3.1.1 has no such code, so its CONNACK
    return code is 0x02 (Identifier rejected).

Pinned here over the wire:

  * v5, empty id + Clean Start 1  -> CONNACK success (0x00) carrying a non-empty
    Assigned Client Identifier;
  * v5, empty id + Clean Start 0  -> CONNACK 0x85;
  * 3.1.1, empty id + CleanSession 0 -> CONNACK 0x02.

Usage: zero_client_id_test.py <binary> <port>
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

PROP_ASSIGNED_CLIENT_IDENTIFIER = 0x12
REASON_SUCCESS = 0x00
REASON_CLIENT_ID_NOT_VALID = 0x85


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


def connect(cid, version, clean):
    """CONNECT with an optionally empty Client Id. version 5 or 4 (3.1.1)."""
    flags = 0x02 if clean else 0x00
    body = utf8("MQTT") + bytes([version, flags]) + struct.pack("!H", 60)
    if version == 5:
        body += b"\x00"  # empty CONNECT properties
    body += utf8(cid)
    return bytes([0x10]) + vbi(len(body)) + body


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


def parse_connack(body, version):
    """Return (reason_code, assigned_client_id_or_None) from a CONNACK body."""
    reason = body[1]
    if version != 5:
        return reason, None
    prop_len, off = dec_vbi(body, 2)
    pend = off + prop_len
    assigned = None
    # Fixed-width MQTT 5 CONNACK properties the broker may send; anything not in
    # this table is treated as a UTF-8 string (2-byte length prefix).
    fixed_width = {
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
        if pid == PROP_ASSIGNED_CLIENT_IDENTIFIER:
            slen = struct.unpack("!H", body[off:off + 2])[0]
            assigned = body[off + 2:off + 2 + slen].decode()
            off += 2 + slen
        elif pid in fixed_width:
            off += fixed_width[pid]
        else:
            # Assume a UTF-8 string (2-byte length prefix) for the remainder
            # (Reason String 0x1F, Response Information 0x1A, ...).
            slen = struct.unpack("!H", body[off:off + 2])[0]
            off += 2 + slen
    return reason, assigned


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-zeroid-")
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


def case_v5_empty_clean_start_assigns():
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect(b"", 5, clean=True))
            pkt = read_packet(c)
            assert pkt is not None, "no CONNACK for an empty id with Clean Start"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected CONNACK, got header 0x%02X" % h
            reason, assigned = parse_connack(b, 5)
            assert reason == REASON_SUCCESS, (
                "an empty id with Clean Start must be accepted (0x00), got 0x%02X" % reason)
            assert assigned is not None and assigned != "", (
                "the CONNACK must carry a non-empty Assigned Client Identifier")
            return "empty id + Clean Start accepted, Assigned Client Identifier = %r" % assigned
        finally:
            c.close()
            stop(proc, workdir)
    finally:
        pass


def case_v5_empty_no_clean_start_rejected():
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect(b"", 5, clean=False))
            pkt = read_packet(c)
            assert pkt is not None, "no CONNACK for an empty id without Clean Start"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected CONNACK, got header 0x%02X" % h
            reason, _ = parse_connack(b, 5)
            assert reason == REASON_CLIENT_ID_NOT_VALID, (
                "an empty id without Clean Start must be CONNACK 0x85, got 0x%02X" % reason)
            return "empty id without Clean Start -> CONNACK 0x85"
        finally:
            c.close()
            stop(proc, workdir)
    finally:
        pass


def case_311_empty_no_clean_rejected():
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect(b"", 4, clean=False))
            pkt = read_packet(c)
            assert pkt is not None, "no CONNACK for an MQTT 3.1.1 empty id without CleanSession"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected CONNACK, got header 0x%02X" % h
            reason, _ = parse_connack(b, 4)
            assert reason == 0x02, (
                "3.1.1 empty id without CleanSession must be CONNACK 0x02, got 0x%02X" % reason)
            return "MQTT 3.1.1 empty id without CleanSession -> CONNACK 0x02"
        finally:
            c.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT-3.1.3-6/-7: a v5 empty Client Id with Clean Start is assigned one, returned as the Assigned Client Identifier",
     case_v5_empty_clean_start_assigns)
case("MQTT-3.1.3-8: a v5 empty Client Id without Clean Start is refused with CONNACK 0x85",
     case_v5_empty_no_clean_start_rejected)
case("MQTT 3.1.1: an empty Client Id without CleanSession is refused with CONNACK 0x02 (Identifier rejected)",
     case_311_empty_no_clean_rejected)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
