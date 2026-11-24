#!/usr/bin/env python3
"""MQTT 5 enhanced-authentication refusal (§4.12 / MQTT-3.1.2.17 / MQTT-4.12.0-1).

MQTT 5 defines an optional *enhanced authentication* exchange carried by the
CONNECT and AUTH packets, in which the client and server challenge each other
with an Authentication Method and opaque Authentication Data. This broker
implements only the classic username/password authentication (§4.12) and does
not offer any enhanced method, so it must refuse the mechanism cleanly rather
than ignore it or pretend to start the exchange.

Three related refusals are pinned here, matching Connection::handleConnect and
Connection::handleAuth:

  * a CONNECT that carries an Authentication Method (property 0x15) asks for a
    mechanism the broker does not implement; the server MUST NOT proceed and
    answers CONNACK with BAD_AUTHENTICATION_METHOD (0x8C) [MQTT-4.12.0-1],
    then closes;
  * a CONNECT that carries Authentication Data (0x16) but no Authentication
    Method is a Protocol Error [MQTT-3.1.2-17] -> the broker answers CONNACK with
    reason 0x82 and closes (for a CONNECT error a CONNACK carrying a negative
    reason code, rather than a DISCONNECT, is a conformant choice);
  * an AUTH packet arriving outside a running enhanced-authentication exchange
    is a Protocol Error too [§4.13.1 / MQTT-4.12.0-7]; since the broker never
    runs one, any AUTH is refused with DISCONNECT 0x82.

Each is deterministic and independent. The CONNECT-without-a-mechanism case
also serves as a control: a CONNECT that sends only username/password is
accepted, proving the broker has authentication working and that the refusals
are specifically about the enhanced mechanism.

Usage: enhanced_auth_refused_test.py <binary> <port>
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

PROP_AUTHENTICATION_METHOD = 0x15
PROP_AUTHENTICATION_DATA = 0x16
REASON_SUCCESS = 0x00
REASON_BAD_AUTHENTICATION_METHOD = 0x8C
REASON_PROTOCOL_ERROR = 0x82

# AUTH packet: type 15, flags 0, no properties (just the reason code 0x00).
AUTH_PACKET = bytes([0xF0, 0x02, REASON_SUCCESS, 0x00])


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


def connect(cid, props=b"", username=None, password=None):
    """Build a v5 CONNECT with an optional property blob and/or credentials."""
    flags = 0x02  # Clean Start
    payload = utf8(cid)
    if username is not None:
        flags |= 0x80
        payload += utf8(username)
    if password is not None:
        flags |= 0x40
        payload += utf8(password)
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(len(props)) + props + payload
    return bytes([0x10]) + enc_len(len(body)) + body


def connect_auth_method(cid, method=b"SomeMethod"):
    props = bytes([PROP_AUTHENTICATION_METHOD]) + utf8(method)
    return connect(cid, props=props)


def connect_auth_data_no_method(cid, data=b"\x01\x02"):
    props = bytes([PROP_AUTHENTICATION_DATA]) + struct.pack("!H", len(data)) + data
    return connect(cid, props=props)


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
    workdir = tempfile.mkdtemp(prefix="flowmq-auth-")
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


def case_method_refused():
    """A CONNECT with an Authentication Method is refused with CONNACK 0x8C."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect_auth_method("auth-method"))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK for an enhanced-auth CONNECT"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected CONNACK, got header 0x%02x" % h
            reason = b[1]
            assert reason == REASON_BAD_AUTHENTICATION_METHOD, (
                "an enhanced Authentication Method must be CONNACK 0x8C "
                "(Bad authentication method), got 0x%02x" % reason)
            # The connection ends after the refusal.
            assert read_packet(c, 1.0) is None, (
                "the broker must close the connection after refusing the method")
            return "enhanced-auth CONNECT -> CONNACK 0x8C and close"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_data_without_method_disconnects():
    """Authentication Data without a Method is a Protocol Error: the broker
    answers CONNACK with reason 0x82 (Protocol Error) and closes. (For a CONNECT
    error the server may either send such a CONNACK or just close; 0x82 is a
    valid negative CONNACK reason code in MQTT 5.)"""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect_auth_data_no_method("auth-data-only"))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK/DISCONNECT for Authentication Data without a Method"
            h, b = pkt
            assert h & 0xF0 == 0x20, (
                "expected a CONNACK carrying 0x82, got header 0x%02x" % h)
            reason = b[1]
            assert reason == REASON_PROTOCOL_ERROR, (
                "Authentication Data without a Method must be CONNACK 0x82 "
                "(Protocol Error), got 0x%02x" % reason)
            assert read_packet(c, 1.0) is None, (
                "the broker must close the connection after the CONNACK")
            return "Authentication Data without a Method -> CONNACK 0x82 and close"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_stray_auth_disconnects():
    """An AUTH packet outside an exchange is a Protocol Error -> DISCONNECT 0x82."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("auth-plain"))
            assert read_packet(c)[0] & 0xF0 == 0x20, "no CONNACK for a plain CONNECT"
            c.sendall(AUTH_PACKET)
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no DISCONNECT for a stray AUTH"
            h, b = pkt
            assert h & 0xF0 == 0xE0, (
                "expected a DISCONNECT, got header 0x%02x" % h)
            reason = b[0]
            assert reason == REASON_PROTOCOL_ERROR, (
                "a stray AUTH must be DISCONNECT 0x82 (Protocol Error), got 0x%02x" % reason)
            return "AUTH outside an exchange -> DISCONNECT 0x82"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_plain_credentials_accepted():
    """Control: a plain CONNECT with username/password (no enhanced mechanism)
    is accepted, proving authentication works and the refusals above are about
    the enhanced-auth mechanism specifically."""
    proc, port, workdir = start()
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("auth-ok", username="user", password="pass"))
            pkt = read_packet(c, 2.0)
            assert pkt is not None, "no CONNACK for a plain credential CONNECT"
            h, b = pkt
            assert h & 0xF0 == 0x20, "expected CONNACK, got header 0x%02x" % h
            reason = b[1]
            assert reason == REASON_SUCCESS, (
                "a plain username/password CONNECT must be accepted (0x00), "
                "got 0x%02x" % reason)
            return "plain credential CONNECT -> CONNACK 0x00 (accepted)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-4.12.0-1: a CONNECT carrying an Authentication Method is refused with CONNACK 0x8C (Bad authentication method)",
     case_method_refused)
case("MQTT-3.1.2-17: Authentication Data without an Authentication Method is a Protocol Error -> CONNACK 0x82 and close",
     case_data_without_method_disconnects)
case("MQTT-4.12.0-7 / 4.13.1: an AUTH packet outside a running exchange is a Protocol Error -> DISCONNECT 0x82",
     case_stray_auth_disconnects)
case("MQTT 5 (control): a CONNECT with plain username/password and no enhanced mechanism is accepted (CONNACK 0x00)",
     case_plain_credentials_accepted)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
