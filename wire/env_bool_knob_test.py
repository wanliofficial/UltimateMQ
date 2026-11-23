#!/usr/bin/env python3
"""Boolean env knobs treat =0 as OFF, not as "set, therefore on".

Several opt-in knobs were read with getenv(...) != nullptr, so merely defining the
variable enabled the behaviour: FLOWMQ_WS_REQUIRE_SUBPROTOCOL=0 read as "set the
variable to zero", which a human means as "off", yet turned the requirement ON.
The same trap affected FLOWMQ_TLS_VERIFY_CLIENTS and FLOWMQ_TLS_CERT_AS_USERNAME.

Env::isEnabled() now treats an empty value and the explicit off-tokens 0/false/no
(all case-insensitive) as OFF, and any other non-empty value as ON. This probe
pins the most observable knob, FLOWMQ_WS_REQUIRE_SUBPROTOCOL, end to end:

  * =0     -> requirement OFF: a client that offers no "mqtt" subprotocol still
              gets 101 Switching Protocols (the pre-fix code refused it);
  * =false -> requirement OFF: same, ruling out a special-case for the digit 0;
  * =1     -> requirement ON: such a client is refused with 400;
  * a client that DOES offer "mqtt" is accepted under =1.

This is an end-to-end wire test: it launches the real broker with its WebSocket
listener and drives it over TCP.

Usage: env_bool_knob_test.py <binary> <port>
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

results = []


def start(env_extra):
    PORT[0] += 1
    mqtt_port = PORT[0]
    ws_port = PORT[0] + 1
    workdir = tempfile.mkdtemp(prefix="flowmq-envbool-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    env["FLOWMQ_WEBSOCKET_PORT"] = str(ws_port)
    env.update(env_extra)
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
    time.sleep(0.3)


def try_upgrade(ws_port, offer_mqtt):
    """Return the HTTP status line of the upgrade attempt."""
    c = socket.create_connection((HOST, ws_port), timeout=3)
    try:
        key = "dGhlIHNhbXBsZSBub25jZQ=="
        req = (
            "GET /mqtt HTTP/1.1\r\n"
            "Host: example\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            "Sec-WebSocket-Key: %s\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            % key)
        if offer_mqtt:
            req += "Sec-WebSocket-Protocol: mqtt\r\n"
        req += "\r\n"
        c.sendall(req.encode())
        c.settimeout(3)
        resp = b""
        while b"\r\n" not in resp:
            chunk = c.recv(1)
            if not chunk:
                break
            resp += chunk
        return resp.split(b"\r\n", 1)[0].decode("latin-1", "replace")
    finally:
        c.close()


def case_knot_equal_zero_is_off():
    """=0 means OFF: a client with no subprotocol must still be upgraded."""
    proc, ws_port, workdir = start({"FLOWMQ_WS_REQUIRE_SUBPROTOCOL": "0"})
    try:
        status = try_upgrade(ws_port, offer_mqtt=False)
        assert " 101 " in status, (
            "FLOWMQ_WS_REQUIRE_SUBPROTOCOL=0 was treated as ON: a client without "
            "the mqtt subprotocol was refused (%r), expected 101" % status)
        return "=0 is OFF: a client with no subprotocol still gets 101"
    finally:
        stop(proc, workdir)


def case_knot_false_is_off():
    """=false also means OFF (not just the digit 0)."""
    proc, ws_port, workdir = start({"FLOWMQ_WS_REQUIRE_SUBPROTOCOL": "false"})
    try:
        status = try_upgrade(ws_port, offer_mqtt=False)
        assert " 101 " in status, (
            "FLOWMQ_WS_REQUIRE_SUBPROTOCOL=false was treated as ON: refused (%r)"
            % status)
        return "=false is OFF: a client with no subprotocol still gets 101"
    finally:
        stop(proc, workdir)


def case_one_is_on():
    """=1 means ON: a client with no subprotocol is refused with 400."""
    proc, ws_port, workdir = start({"FLOWMQ_WS_REQUIRE_SUBPROTOCOL": "1"})
    try:
        status = try_upgrade(ws_port, offer_mqtt=False)
        assert " 400 " in status, (
            "FLOWMQ_WS_REQUIRE_SUBPROTOCOL=1 did not enforce the subprotocol: "
            "expected 400, got %r" % status)
        # A client that offers "mqtt" is still accepted.
        status_ok = try_upgrade(ws_port, offer_mqtt=True)
        assert " 101 " in status_ok, (
            "a client offering the mqtt subprotocol was refused under =1: %r"
            % status_ok)
        return "=1 is ON: no-subprotocol rejected 400, mqtt subprotocol accepted 101"
    finally:
        stop(proc, workdir)


def case(name, fn):
    try:
        info = fn()
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()
    else:
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))


if __name__ == "__main__":
    case("env bool: FLOWMQ_WS_REQUIRE_SUBPROTOCOL=0 is OFF (no subprotocol still upgrades)",
         case_knot_equal_zero_is_off)
    case("env bool: FLOWMQ_WS_REQUIRE_SUBPROTOCOL=false is OFF",
         case_knot_false_is_off)
    case("env bool: FLOWMQ_WS_REQUIRE_SUBPROTOCOL=1 is ON (no subprotocol refused)",
         case_one_is_on)
    print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
    sys.exit(0 if all(ok for _, ok in results) else 1)
