#!/usr/bin/env python3
"""WebSocket upgrade: the |Upgrade| header must carry the token "websocket".

RFC 6455 §4.2.1 reads the client opening handshake, and item 3 requires "An
|Upgrade| header field containing the value 'websocket', treated
case-insensitively." Like |Connection|, |Upgrade| is a comma-separated list of
protocol tokens (RFC 9110 §7.8), so the broker must match a whole token, not a
substring.

A substring test (the bug this pins) accepts "notwebsocket" and "xwebsocketx"
because they merely *contain* the letters "websocket". This probe sends those
two values in an otherwise valid upgrade request and expects the broker to
refuse them with 400, then confirms a genuine comma-separated list
("h2c, websocket") and a mixed-case token ("WebSocket") are still accepted with
101.

Usage: websocket_upgrade_token_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
import base64
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


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def upgrade_request(port, upgrade_value):
    """Build a minimal upgrade request with a caller-chosen |Upgrade| value."""
    key = base64.b64encode(os.urandom(16)).decode()
    return (
        "GET / HTTP/1.1\r\n"
        "Host: %s:%d\r\n" % (HOST, port) +
        "Upgrade: %s\r\n" % upgrade_value +
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n" % key +
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    ).encode()


def read_status_line(c):
    """Read the HTTP response status line, then drain the headers."""
    c.settimeout(3.0)
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = c.recv(1)
        if not chunk:
            break
        resp += chunk
    return resp.split(b"\r\n", 1)[0]


def try_upgrade(port, upgrade_value):
    """Return the HTTP status code the broker answers, or None if it closed."""
    c = socket.create_connection((HOST, port), timeout=5)
    try:
        c.sendall(upgrade_request(port, upgrade_value))
        line = read_status_line(c)
        if not line:
            return None
        parts = line.split(b" ", 2)
        if len(parts) < 2:
            return None
        return int(parts[1])
    finally:
        c.close()


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


def case_substring_upgrade_values_rejected():
    """An |Upgrade| value that merely *contains* "websocket" ("notwebsocket",
    "xwebsocketx") is not the websocket upgrade token and must be refused with
    400, not upgraded."""
    proc, ws_port, workdir = start()
    try:
        for bad in ("notwebsocket", "xwebsocketx", "websockets"):
            status = try_upgrade(ws_port, bad)
            assert status == 400, (
                "Upgrade: %r must be rejected with 400, got %r "
                "(a substring match wrongly upgrades it)" % (bad, status))
        return "Upgrade values that only *contain* 'websocket' are rejected with 400"
    finally:
        stop(proc, workdir)


def case_valid_upgrade_tokens_accepted():
    """A real upgrade token is still accepted: a mixed-case single token and a
    comma-separated list that includes it must both return 101."""
    proc, ws_port, workdir = start()
    try:
        for good in ("websocket", "WebSocket", "h2c, websocket"):
            status = try_upgrade(ws_port, good)
            assert status == 101, (
                "a valid Upgrade token %r must be accepted with 101, got %r"
                % (good, status))
        return "websocket / WebSocket / 'h2c, websocket' all upgrade with 101"
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


case("WebSocket upgrade: 'notwebsocket'/'xwebsocketx' are rejected with 400 (token, not substring)",
     case_substring_upgrade_values_rejected)
case("WebSocket upgrade: 'websocket'/'WebSocket'/'h2c, websocket' are accepted with 101",
     case_valid_upgrade_tokens_accepted)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
