#!/usr/bin/env python3
"""MQTT 3.1.2-12 / §3.1.2.9 / §3.1.2.10: a CONNECT whose MQTT 5 properties set
Maximum Packet Size or Receive Maximum to 0 is refused with CONNACK 0x82.

Two CONNECT properties are numeric values with a non-zero floor:

  * Maximum Packet Size (0x24) is a Four Byte Integer; "If the Remaining
    Length is less than 1... a value of 0 means the Client cannot accept any
    packet" and the Server treats a 0 as a Protocol Error rather than an
    unbounded limit (Connection::handleConnect rejects it);
  * Receive Maximum (0x21) is a Two Byte Integer that "MUST NOT be 0"
    [MQTT-3.1.2-10], because 0 would mean the Client grants the Server no
    outbound window at all.

Connection::handleConnect validates both before any session state is touched or
the Will is armed, answering the refused CONNECT with a CONNACK carrying
0x82 (PROTOCOL_ERROR) -- the encoding a v5 client reads for a rejected
CONNECT -- and closing the connection. The values 0 are the boundary: the same
properties with any non-zero value connect normally.

Usage: connect_zero_property_test.py <binary> <port>
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

REASON_PROTOCOL_ERROR = 0x82

# Property identifiers (MQTT 5).
PROP_RECEIVE_MAXIMUM = 0x21
PROP_MAXIMUM_PACKET_SIZE = 0x27


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


def prop_max_packet_size(value):
    # 0x24 + Four Byte Integer.
    return bytes([PROP_MAXIMUM_PACKET_SIZE]) + struct.pack("!I", value)


def prop_receive_maximum(value):
    # 0x21 + Two Byte Integer.
    return bytes([PROP_RECEIVE_MAXIMUM]) + struct.pack("!H", value)


def connect_with_properties(cid, properties=b""):
    flags = 0x02  # Clean Start
    body = (utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60)
            + enc_len(len(properties)) + properties + utf8(cid))
    return bytes([0x10]) + enc_len(len(body)) + body


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
    workdir = tempfile.mkdtemp(prefix="flowmq-czero-")
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


def _expect_protocol_error(properties, label):
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect_with_properties("czero-" + label, properties))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker closed with no CONNACK (%s)" % label
            header, body = pkt
            assert header & 0xF0 == 0x20, (
                "expected a CONNACK for %s, got header 0x%02X" % (label, header))
            assert len(body) >= 2 and body[0] == 0x00 and body[1] == REASON_PROTOCOL_ERROR, (
                "%s must be refused with CONNACK 0x82 (PROTOCOL_ERROR), got %r"
                % (label, body))
            return "%s -> CONNACK 0x82" % label
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_max_packet_size_zero():
    # Maximum Packet Size property: id 0x24 + Four Byte Integer 0. The
    # property-length prefix (VBI) is added by connect_with_properties.
    return _expect_protocol_error(prop_max_packet_size(0), "Maximum Packet Size = 0")


def case_receive_maximum_zero():
    # Receive Maximum property: id 0x21 + Two Byte Integer 0.
    return _expect_protocol_error(prop_receive_maximum(0), "Receive Maximum = 0")


def case_nonzero_values_accepted():
    """Control: the same two properties with non-zero values connect normally,
    so the refusal is specific to the 0 boundary rather than to the properties
    being present at all."""
    props = prop_receive_maximum(10) + prop_max_packet_size(65536)
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect_with_properties("czero-ok", props))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker closed the connection on valid properties"
            header, body = pkt
            assert header & 0xF0 == 0x20, (
                "non-zero Maximum Packet Size / Receive Maximum must get a CONNACK, "
                "got header 0x%02X" % header)
            # A granted CONNACK echoes reason 0x00; a refusal would be >= 0x80.
            assert len(body) >= 2 and body[1] < 0x80, (
                "non-zero properties must be accepted (reason < 0x80), got %r" % body)
            return "non-zero Maximum Packet Size / Receive Maximum are accepted"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT 3.1.2-12: a CONNECT with Maximum Packet Size = 0 is refused with CONNACK 0x82 (PROTOCOL_ERROR)",
     case_max_packet_size_zero)
case("MQTT-3.1.2-10: a CONNECT with Receive Maximum = 0 is refused with CONNACK 0x82 (PROTOCOL_ERROR)",
     case_receive_maximum_zero)
case("MQTT 5 (control): a CONNECT with non-zero Maximum Packet Size and Receive Maximum is accepted",
     case_nonzero_values_accepted)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
