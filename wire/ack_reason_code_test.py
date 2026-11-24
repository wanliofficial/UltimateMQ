#!/usr/bin/env python3
"""MQTT §3.4.2.1 / §3.5.2.1 / §3.6.2.1 / §3.7.2.1: an acknowledgement's Reason
Code is a fixed per-packet set, and a 3.1.1 ack carries none at all.

Each MQTT 5 acknowledgement (PUBACK, PUBREC, PUBREL, PUBCOMP) is "Packet
Identifier followed by Reason Code and Property Length", but the Reason Code
may only be one of a small set defined for that packet type [MQTT-3.4.2.1,
3.5.2.1, 3.6.2.1, 3.7.2.1]. Any other byte is a Malformed Packet: the broker
must reject it rather than read an out-of-range code into the flow as if it
meant something.

MQTT 3.1.1 is stricter still: the four acks are "exactly a Packet Identifier"
with no Reason Code and no Property section [MQTT-3.4.2-1 etc.]. A 3.1.1 ack
that carries even one extra byte is malformed.

Frame::parseAckReasonAndProperties enforces both: a 3.1.1 ack with a trailing
byte throws, and a v5 ack whose Reason Code is not in isLegalAckReason throws.
Either way the parse exception falls through to Connection::run's catch-all and
closeWithReason -- a v5 client gets DISCONNECT 0x81 (MALFORMED_PACKET); a 3.1.1
client's connection is simply closed (the server has no DISCONNECT packet in
3.1.1).

Pinned here for PUBACK (the other acks share the identical code path):

  * v5 PUBACK with an out-of-range Reason Code (0x01) -> DISCONNECT 0x81;
  * 3.1.1 PUBACK with a trailing byte (as if a reason code) -> connection close;
  * control: v5 PUBACK with a legal Reason Code (0x00) is parsed, not malformed.

Usage: ack_reason_code_test.py <binary> <port>
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

PUBACK = 0x40


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
    body = (utf8("MQTT") + bytes([version, 0x02]) + struct.pack("!H", 60)
            + enc_len(0) + utf8(cid))
    return bytes([0x10]) + enc_len(len(body)) + body


def puback(pid, reason=None, version=5):
    # v5: Packet Id (2) + optional Reason Code. 3.1.1: Packet Id (2) only -- any
    # trailing byte is the malformation.
    body = struct.pack("!H", pid)
    if reason is not None:
        body += bytes([reason])
    return bytes([PUBACK]) + enc_len(len(body)) + body


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
    workdir = tempfile.mkdtemp(prefix="flowmq-ackrc-")
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


def case_v5_illegal_reason_code():
    """A v5 PUBACK whose Reason Code is not in the legal PUBACK set (0x01 is
    not among 0x00/0x10/0x80/0x83/0x87/0x90/0x91/0x97/0x99) is Malformed: the
    broker answers DISCONNECT 0x81."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("ackrc-illegal"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(puback(0x1234, reason=0x01))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker closed with no DISCONNECT (v5 illegal ack reason)"
            header, body = pkt
            assert header & 0xF0 == 0xE0, (
                "expected DISCONNECT for an illegal PUBACK reason code, got 0x%02X" % header)
            assert body and body[0] == REASON_MALFORMED_PACKET, (
                "an illegal ack Reason Code must be MALFORMED_PACKET (0x%02X), got %r"
                % (REASON_MALFORMED_PACKET, body))
            return "v5 PUBACK with an out-of-range reason code -> DISCONNECT 0x81"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_v311_trailing_byte():
    """A 3.1.1 PUBACK carrying any extra byte beyond the Packet Identifier is
    Malformed. 3.1.1 has no server DISCONNECT, so the connection simply ends."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("ackrc-311", version=4))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK (3.1.1)"
            c.sendall(puback(0x1234, reason=0x00, version=4))
            try:
                extra = c.recv(16)
            except socket.timeout:
                extra = b"<timeout>"
            except ConnectionResetError:
                # The broker closes a 3.1.1 connection whose ack is malformed. Under
                # load the close can surface as a TCP RST rather than a clean FIN,
                # which Python raises as ConnectionResetError. Either way the
                # connection is gone and no further MQTT bytes arrive -- exactly
                # what this case asserts -- so treat a reset as an empty read.
                extra = b""
            assert extra == b"", (
                "a 3.1.1 PUBACK with a trailing byte must close the connection "
                "with no further bytes, got %r" % extra)
            return "3.1.1 PUBACK with a trailing byte -> connection closed"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_v5_legal_reason_code():
    """Control: a v5 PUBACK with a legal Reason Code (0x00) is parsed, not
    treated as malformed. With no matching inflight flow the broker answers
    nothing harmful -- the point is it does not DISCONNECT for the reason code
    itself (the connection stays open and no malformed close arrives)."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("ackrc-ok"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK"
            c.sendall(puback(0x1234, reason=0x00))
            try:
                pkt = read_packet(c, 1.5)
            except Exception:  # noqa: BLE001
                pkt = None
            # An unmatched PUBREC/PUBACK with reason 0x00 draws no reply and no
            # malformed close: the broker just ignores the unknown identifier.
            assert pkt is None or (pkt[0] & 0xF0 != 0xE0), (
                "a legal Reason Code must not be malformed, got %r" % (pkt,))
            return "v5 PUBACK with a legal reason code (0x00) is not malformed"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT 3.4.2.1: a v5 PUBACK with an out-of-range Reason Code (0x01) is refused DISCONNECT 0x81 (MALFORMED_PACKET)",
     case_v5_illegal_reason_code)
case("MQTT-3.4.2-1: a 3.1.1 PUBACK carrying a trailing byte (a reason code) closes the connection (3.1.1 has no DISCONNECT)",
     case_v311_trailing_byte)
case("MQTT 5 (control): a PUBACK with a legal Reason Code (0x00) is not treated as malformed",
     case_v5_legal_reason_code)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
