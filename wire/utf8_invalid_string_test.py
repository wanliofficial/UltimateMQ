#!/usr/bin/env python3
"""MQTT-1.5.4 / §1.5.4: an ill-formed UTF-8 Encoded String makes the packet Malformed.

Every text field in MQTT (Client Id, Topic Name, Topic Filter, Will Topic,
User Name, ...) is a UTF-8 Encoded String and "MUST be well-formed UTF-8
as defined in RFC 3629" [MQTT-1.5.4-1]; in addition it "MUST NOT include an
encoding of the null character (U+0000)" [MQTT-1.5.4-2]. A receiver that gets
either treats the packet as malformed.

The codec enforces both in one place (Frame::parseString -> Utf8::isValid),
rejecting stray continuation bytes, truncated sequences, overlong encodings,
encoded surrogates (U+D800..U+DFFF) and anything above U+10FFFF. The failure is
a Frame::MalformedString (a std::runtime_error), so it falls through to the
catch-all in Connection::run(). Because the client is still IDLE (no session
exists yet), Connection::closeWithReason answers it with a CONNACK carrying
0x81 (MALFORMED_PACKET) per MQTT-3.2.2-2 -- not a DISCONNECT, which a client
with no session could not interpret -- then closes.

Cases pinned (all in the Client Id, the first text field after the fixed
CONNECT header, so the very first string the broker reads is the one tested):

  * a lone continuation byte (0x80)                              -> CONNACK 0x81;
  * an encoded surrogate U+D800 (ED A0 80)                      -> CONNACK 0x81;
  * an overlong encoding of U+0000 (C0 80)                      -> CONNACK 0x81;
  * a multi-byte sequence that claims more continuation bytes
    than the packet carries (truncated, E6 96)                  -> CONNACK 0x81;
  * control: a well-formed 2-byte sequence (C3 A9, "é") in the
    same position is accepted (CONNACK success), so the check
    is not simply "reject any byte >= 0x80".

Usage: utf8_invalid_string_test.py <binary> <port>
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


def raw_field(payload_bytes):
    # A length-prefixed string field whose value is exactly the given bytes,
    # without any UTF-8 validation (we are building an ill-formed one on
    # purpose).
    return struct.pack("!H", len(payload_bytes)) + payload_bytes


def connect_with_raw_clientid(clientid_bytes, clean_start=True):
    flags = 0x02 if clean_start else 0x00
    body = (utf8(b"MQTT")              # protocol name, length-prefixed (valid)
            + bytes([5, flags])          # version 5 + flags
            + struct.pack("!H", 60)      # keep alive
            + enc_len(0)                 # properties (v5)
            + raw_field(clientid_bytes))  # Client Id (the field under test)
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
    workdir = tempfile.mkdtemp(prefix="flowmq-utf8-")
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


def _expect_malformed_close(value_bytes, label):
    # Prefix with two ASCII bytes so the Client Id field is comfortably longer
    # than its 2-byte length prefix: the test then exercises Utf8::isValid on
    # the ill-formed bytes rather than tripping the string-length bound first.
    clientid_bytes = b"ab" + value_bytes
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect_with_raw_clientid(clientid_bytes))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker closed the connection with no reply (%s)" % label
            header, body = pkt
            # A malformed Client Id is rejected while the client is still IDLE
            # (no session yet), so MQTT-3.2.2-2 requires a CONNACK carrying the
            # reason code, not a DISCONNECT the client could not yet interpret:
            # 0x20 CONNACK, session-present 0x00, reason 0x81, property length
            # 0x00.
            assert header & 0xF0 == 0x20, (
                "expected a CONNACK for an ill-formed Client Id (%s), got header "
                "0x%02X" % (label, header))
            assert len(body) >= 2 and body[0] == 0x00 and body[1] == REASON_MALFORMED_PACKET, (
                "an ill-formed UTF-8 Client Id (%s) must be CONNACK 0x81 "
                "(MALFORMED_PACKET), got %r" % (label, body))
            return "%s -> CONNACK 0x81 (MALFORMED_PACKET)" % label
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_lone_continuation_byte():
    return _expect_malformed_close(b"\x80", "a lone 0x80 continuation byte")


def case_encoded_surrogate():
    return _expect_malformed_close(b"\xED\xA0\x80", "an encoded surrogate U+D800 (ED A0 80)")


def case_overlong_nul():
    return _expect_malformed_close(b"\xC0\x80", "an overlong encoding of U+0000 (C0 80)")


def case_truncated_sequence():
    # 0xE6 starts a 3-byte sequence but only one continuation byte follows.
    return _expect_malformed_close(b"\xE6\x96", "a truncated 3-byte sequence (E6 96)")


def case_valid_multibyte_accepted():
    """Control: a well-formed 2-byte UTF-8 sequence (C3 A9 = "é") in the same
    position is accepted with a CONNACK, so the rejection above is about
    well-formedness, not about rejecting non-ASCII bytes outright."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect_with_raw_clientid("café".encode("utf-8")))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "broker closed the connection on a valid Client Id"
            header, body = pkt
            assert header & 0xF0 == 0x20, (
                "a well-formed UTF-8 Client Id must get a CONNACK, got header 0x%02X" % header)
            return "a well-formed multi-byte Client Id (café) is accepted with CONNACK"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-1.5.4-1: a CONNECT whose Client Id is a lone continuation byte (0x80) is refused with CONNACK 0x81 (MALFORMED_PACKET)",
     case_lone_continuation_byte)
case("MQTT-1.5.4-1: a CONNECT whose Client Id encodes a surrogate U+D800 (ED A0 80) is refused with CONNACK 0x81 (MALFORMED_PACKET)",
     case_encoded_surrogate)
case("MQTT-1.5.4-2: a CONNECT whose Client Id is an overlong U+0000 (C0 80) is refused with CONNACK 0x81 (MALFORMED_PACKET)",
     case_overlong_nul)
case("MQTT-1.5.4-1: a CONNECT whose Client Id truncates a multi-byte sequence (E6 96) is refused with CONNACK 0x81 (MALFORMED_PACKET)",
     case_truncated_sequence)
case("MQTT-1.5.4 (control): a CONNECT whose Client Id is a well-formed multi-byte string (café) still gets a CONNACK",
     case_valid_multibyte_accepted)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
