#!/usr/bin/env python3
"""WebSocket upgrade-rejection response is clean HTTP, not HTTP + WebSocket framing.

Two coupled broker defects are pinned down here, both on the path that rejects a
non-WebSocket HTTP request (src/WebSocket.cpp, WebSocketTransport::handshake):

 1. Content-Length off-by-one. The 400 Bad Request body "websocket required.\n"
    is exactly 20 bytes, but the header advertised Content-Length: 21. A client
    that trusts the header waits for a byte that never arrives (or reads into the
    next record). The fix derives the length from the body so it cannot drift.

 2. CLOSE frame appended to the HTTP error. When the upgrade is rejected the
    handshake returns false, Connection::run() ends, and the Connection is
    destroyed - whose closeSocket() calls WebSocketTransport::teardown(). That
    teardown() unconditionally sent a WebSocket CLOSE (1000), appending 0x88 0x80
    0x03 0xE8 AFTER the plain HTTP 400 body, even though the peer never completed
    the upgrade and is still speaking HTTP. The fix sends CLOSE only once the
    session was actually established.

This is an end-to-end wire test: it launches the real broker with its WebSocket
listener and drives it over TCP, complementing the in-process gtest suite.

Usage: websocket_reject_response_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
import os
import signal
import socket
import subprocess
import sys
import tempfile
import time

HOST = "127.0.0.1"
BIN = sys.argv[1]
PORT = [int(sys.argv[2])]

# WebSocket CLOSE opcode/first byte (RFC 6455 §5.2): FIN + opcode 0x08.
WS_CLOSE_FIRST_BYTE = 0x88


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    ws_port = PORT[0] + 1
    workdir = tempfile.mkdtemp(prefix="flowmq-wsrej-")
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


def read_until_close(c, timeout=3.0):
    """Read every byte the peer sends until it closes, or timeout."""
    c.settimeout(timeout)
    data = b""
    try:
        while True:
            chunk = c.recv(4096)
            if not chunk:
                break
            data += chunk
    except socket.timeout:
        pass
    return data


def parse_http_response(data):
    """Split a raw HTTP response into (headers_text, body_bytes)."""
    sep = data.find(b"\r\n\r\n")
    assert sep != -1, "no header/body separator in %r" % data[:64]
    return data[:sep].decode("latin-1"), data[sep + 4:]


def reject_upgrade(ws_port):
    """Send a plain GET with no WebSocket upgrade headers; return the raw reply."""
    c = socket.create_connection((HOST, ws_port), timeout=3)
    try:
        c.sendall(b"GET / HTTP/1.1\r\nHost: example\r\n\r\n")
        return read_until_close(c)
    finally:
        c.close()


def case_content_length_matches_body():
    """The 400's Content-Length must equal the real body length and the body must
    be exactly the text, with no extra trailing bytes."""
    proc, ws_port, workdir = start()
    try:
        raw = reject_upgrade(ws_port)
        headers, body = parse_http_response(raw)
        status_line = headers.split("\r\n", 1)[0]
        assert " 400 " in status_line, "expected 400, got %r" % status_line

        declared = None
        for line in headers.split("\r\n")[1:]:
            name, _, value = line.partition(":")
            if name.strip().lower() == "content-length":
                declared = int(value.strip())
                break
        assert declared is not None, "no Content-Length header in %r" % headers
        assert declared == len(body), (
            "Content-Length %d does not match the %d-byte body %r"
            % (declared, len(body), body))
        assert body == b"websocket required.\n", (
            "unexpected 400 body: %r" % body)
        # Nothing may follow the body: a stray WebSocket CLOSE would show up here.
        assert len(raw) == headers.__len__() + 4 + len(body), (
            "trailing bytes after the 400 body: %r" % raw[headers.__len__() + 4 + len(body):])
        return ("400 Bad Request advertises Content-Length %d for the %d-byte body, "
                "nothing trails it" % (declared, len(body)))
    finally:
        stop(proc, workdir)


def case_no_close_frame_on_rejected_upgrade():
    """A rejected upgrade must yield pure HTTP: no WebSocket CLOSE frame (0x88
    first byte) may be appended after the response."""
    proc, ws_port, workdir = start()
    try:
        raw = reject_upgrade(ws_port)
        _, body = parse_http_response(raw)
        trailing = raw[raw.find(b"\r\n\r\n") + 4:]
        # The whole on-wire reply after the headers must be exactly the 20-byte
        # body; anything else (a CLOSE frame's 0x88 ...) is framing leakage.
        assert trailing == b"websocket required.\n", (
            "rejected upgrade carried non-HTTP bytes after the body: %r" % trailing)
        assert WS_CLOSE_FIRST_BYTE not in trailing, (
            "a WebSocket CLOSE frame (0x88) leaked into the HTTP reject reply: %r"
            % trailing)
        return "rejected upgrade is pure HTTP - no WebSocket CLOSE frame appended"
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


case("WebSocket reject: the 400 Content-Length equals the real body length (no off-by-one)",
     case_content_length_matches_body)
case("WebSocket reject: a rejected upgrade is pure HTTP - no WebSocket CLOSE frame is appended",
     case_no_close_frame_on_rejected_upgrade)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
