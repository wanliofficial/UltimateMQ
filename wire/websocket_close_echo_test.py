#!/usr/bin/env python3
"""WebSocket CLOSE: the reply echoes the status code the peer sent.

RFC 6455 §5.5.1: "If an endpoint receives a Close frame and did not previously
send a Close frame, the endpoint MUST send a Close frame in response. (When
sending a Close frame in response, the endpoint typically echos the status code
it received.)" When the peer's Close carries no status code, the reply must also
carry none: the implied code 1005 is reserved and MUST NOT be sent on the wire
(§7.4.1).

The broker used to answer every received Close with a fixed CLOSE 1000, so a
client's 1001 ("going away") or 1011 ("internal error") was rewritten to 1000,
and a bodiless Close was answered with a status code the peer never sent. This
probe pins the echo:

  * a Close carrying 1001 is answered 1001;
  * a Close carrying no body is answered with an empty body;
  * a Close carrying 1000 is answered 1000 (control).

Usage: websocket_close_echo_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
import base64
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

OP_CLOSE = 0x8


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def ws_handshake(port):
    c = socket.create_connection((HOST, port), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        "GET / HTTP/1.1\r\n"
        "Host: %s:%d\r\n" % (HOST, port) +
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n" % key +
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    )
    c.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = c.recv(1)
        if not chunk:
            raise EOFError("closed during handshake")
        resp += chunk
    if b" 101 " not in resp.split(b"\r\n", 1)[0]:
        raise AssertionError("websocket upgrade failed: %r"
                             % resp.split(b"\r\n", 1)[0])
    return c


def ws_send_masked(c, opcode, payload=b""):
    """Send one masked client frame (clients MUST mask, RFC 6455 §5.1)."""
    b0 = 0x80 | (opcode & 0x0F)
    length = len(payload)
    if length < 126:
        hdr = bytes([b0, 0x80 | length])
    elif length < 65536:
        hdr = bytes([b0, 0x80 | 126]) + struct.pack("!H", length)
    else:
        hdr = bytes([b0, 0x80 | 127]) + struct.pack("!Q", length)
    mask = os.urandom(4)
    masked = bytes(payload[i] ^ mask[i % 4] for i in range(length))
    c.sendall(hdr + mask + masked)


def ws_recv_frame(c, timeout=2.0):
    c.settimeout(timeout)
    b0, b1 = recv_exact(c, 2)
    opcode = b0 & 0x0F
    length = b1 & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(c, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(c, 8))[0]
    if b1 & 0x80:
        recv_exact(c, 4)
    payload = recv_exact(c, length) if length else b""
    return opcode, payload


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    ws_port = PORT[0] + 1
    workdir = tempfile.mkdtemp(prefix="flowmq-ws-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    env["FLOWMQ_WEBSOCKET_PORT"] = str(ws_port)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            c = socket.create_connection((HOST, ws_port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, ws_port, workdir
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


def recv_close_reply(c):
    opcode, payload = ws_recv_frame(c, 2.0)
    assert opcode == OP_CLOSE, (
        "expected a CLOSE reply, got opcode 0x%02x" % opcode)
    return payload


def case_close_status_is_echoed():
    """A client Close carrying 1001 must be answered 1001, not rewritten to the
    broker's fixed 1000."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            ws_send_masked(c, OP_CLOSE, struct.pack("!H", 1001))
            payload = recv_close_reply(c)
            assert len(payload) >= 2, (
                "expected a CLOSE reply with a status code, got %r" % payload)
            status = struct.unpack("!H", payload[0:2])[0]
            assert status == 1001, (
                "expected the received status 1001 to be echoed, got %d "
                "(1000 means the code was not echoed)" % status)
            return "a CLOSE carrying 1001 is echoed back as 1001"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_bodiless_close_echoes_no_code():
    """A client Close carrying no status code must be answered with no status
    code: 1005 is reserved and MUST NOT be sent on the wire (§7.4.1)."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            ws_send_masked(c, OP_CLOSE, b"")
            payload = recv_close_reply(c)
            assert payload == b"", (
                "expected an empty CLOSE reply to a bodiless Close, got %r "
                "(a 2-byte body would be a status code the peer never sent)"
                % payload)
            return "a bodiless CLOSE is answered with an empty CLOSE body"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_close_1000_still_1000():
    """Control: a normal Close 1000 is answered 1000, as before."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            ws_send_masked(c, OP_CLOSE, struct.pack("!H", 1000))
            payload = recv_close_reply(c)
            assert len(payload) >= 2 and struct.unpack("!H", payload[0:2])[0] == 1000, (
                "expected CLOSE 1000 to be answered 1000, got %r" % payload)
            return "a normal CLOSE 1000 is answered 1000"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


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


case("WebSocket CLOSE: a received status 1001 is echoed back (RFC 6455 §5.5.1)",
     case_close_status_is_echoed)
case("WebSocket CLOSE: a bodiless Close is answered with an empty body (1005 not sent)",
     case_bodiless_close_echoes_no_code)
case("WebSocket CLOSE: a normal Close 1000 is still answered 1000",
     case_close_1000_still_1000)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
