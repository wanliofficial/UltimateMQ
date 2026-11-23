#!/usr/bin/env python3
"""WebSocket 64-bit frame length: the most significant bit MUST be 0.

RFC 6455 §5.2: "If 127, the following 8 bytes interpreted as a 64-bit unsigned
integer (the most significant bit MUST be 0) are the payload length." A set MSB
is therefore a protocol error and the connection must be failed with CLOSE 1002.

Without the check the broker decodes the 8 bytes into an ordinary huge length
and only the MAX_MESSAGE_BYTES cap stops it -- with CLOSE 1009 (message too
big), i.e. the wrong reason, after the framing error has been misread as a
legal-but-large frame. This probe pins the reason: an MSB-set 64-bit length is
answered CLOSE 1002 immediately, without the broker reading a mask or payload;
a genuine over-cap length (MSB clear) still gets CLOSE 1009, so the cap path is
intact.

Usage: websocket_length_msb_test.py <binary> <port>
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

# Must match MAX_MESSAGE_BYTES in src/WebSocket.cpp.
MAX_MESSAGE_BYTES = 16 * 1024 * 1024

OP_BINARY = 0x2
OP_CLOSE = 0x8
CLOSE_PROTOCOL_ERROR = 1002
CLOSE_MESSAGE_TOO_BIG = 1009


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


def close_status(payload):
    assert len(payload) >= 2, "CLOSE frame carries no status code: %r" % payload
    return struct.unpack("!H", payload[0:2])[0]


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


def case_msb_set_64bit_length_rejected():
    """A BINARY frame whose 127-prefixed 8-byte length has its MSB set violates
    RFC 6455 §5.2 and must be answered CLOSE 1002. No mask or payload follows:
    the framing error is detected at the length itself."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            # FIN + BINARY, MASK + 127, then a length with the top bit set.
            msb_set = (1 << 63) | 1
            c.sendall(bytes([0x82, 0xFF]) + struct.pack("!Q", msb_set))
            opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for an MSB-set 64-bit length, got opcode 0x%02x"
                % opcode)
            status = close_status(payload)
            assert status == CLOSE_PROTOCOL_ERROR, (
                "expected CLOSE 1002 for an MSB-set 64-bit length, got %d "
                "(1009 would mean the framing error was misread as a large "
                "message)" % status)
            return "a 64-bit length with the MSB set is rejected with CLOSE 1002 (RFC 6455 §5.2)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_msb_clear_over_cap_still_1009():
    """Control: a genuine 64-bit length with the MSB clear but over the message
    cap is still answered CLOSE 1009, proving the new MSB check did not disturb
    the byte-cap path."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            oversized = MAX_MESSAGE_BYTES + 1  # MSB clear
            c.sendall(bytes([0x82, 0xFF]) + struct.pack("!Q", oversized))
            opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for an over-cap 64-bit length, got opcode 0x%02x"
                % opcode)
            status = close_status(payload)
            assert status == CLOSE_MESSAGE_TOO_BIG, (
                "expected CLOSE 1009 for an over-cap length, got %d" % status)
            return "an over-cap 64-bit length with the MSB clear still gets CLOSE 1009"
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


case("WebSocket: a 64-bit frame length with the MSB set is rejected with CLOSE 1002 (RFC 6455 §5.2)",
     case_msb_set_64bit_length_rejected)
case("WebSocket: an over-cap 64-bit length with the MSB clear is still rejected with CLOSE 1009",
     case_msb_clear_over_cap_still_1009)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
