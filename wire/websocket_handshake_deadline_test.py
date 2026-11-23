#!/usr/bin/env python3
"""The WebSocket HTTP upgrade has one absolute deadline, not a per-byte budget.

WebSocketTransport::handshake reads the upgrade request byte by byte and used to
call inner_->readable(HANDSHAKE_TIMEOUT_MS) for EACH byte. Because that timeout
restarts on every byte, a slowloris dribbling one byte just under the timeout
kept the connection - and its handler thread - alive for up to
MAX_HANDSHAKE_BYTES * HANDSHAKE_TIMEOUT_MS (~22 hours with the defaults): a cheap
thread-exhaustion attack.

The fix computes ONE steady_clock deadline at the start of the handshake and
passes only the time actually left to each readable() call (and gives up when it
is spent), so the whole upgrade costs at most HANDSHAKE_TIMEOUT_MS regardless of
how the bytes are spread.

This probe launches the real broker with its WebSocket listener and dribbles one
byte every 2s for ~16s (never completing the header terminator). With the fix the
broker drops the connection at ~10s, so the later sends fail and recv reports
close; with the per-byte bug the connection is still open at 16s.

Usage: websocket_handshake_deadline_test.py <binary> <port>
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

# Matches HANDSHAKE_TIMEOUT_MS in src/WebSocket.cpp.
HANDSHAKE_TIMEOUT_S = 10
DRIBBLE_GAP_S = 2.0
# Enough gaps that a per-byte timer (reset every gap) would clearly keep the
# connection alive for the whole window, while the absolute deadline expires at
# ~10s from the first byte.
DRIBBLE_SECONDS = 16.0

results = []


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    ws_port = PORT[0] + 1
    workdir = tempfile.mkdtemp(prefix="flowmq-wshs-")
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
    time.sleep(0.3)


def is_closed_by_peer(s):
    """True if recv() reports an orderly close / reset; False if still open."""
    try:
        s.settimeout(1.5)
        return s.recv(1) == b""
    except socket.timeout:
        return False
    except OSError:
        return True


def case_absolute_deadline_drops_slowloris():
    proc, ws_port, workdir = start()
    try:
        s = socket.create_connection((HOST, ws_port), timeout=3)
        try:
            # Open a request but never terminate it; dribble one byte per gap.
            start_t = time.time()
            s.sendall(b"GET / HTTP/1.1\r\nHost: x\r\n")
            while time.time() - start_t < DRIBBLE_SECONDS:
                time.sleep(DRIBBLE_GAP_S)
                try:
                    s.sendall(b"a")
                except OSError:
                    break  # broker already dropped us - expected with the fix

            # Give the broker a moment if the last send raced the close.
            time.sleep(1.0)
            closed = is_closed_by_peer(s)
            assert closed, (
                "a slowloris dribbling one byte every %.0fs was still connected "
                "%.0fs after the upgrade began -- the handshake timeout is being "
                "reset per byte, not enforced as one absolute deadline"
                % (DRIBBLE_GAP_S, DRIBBLE_SECONDS))
            return ("the upgrade was dropped ~%ds after it began despite per-byte "
                    "dribbling (one absolute deadline)" % HANDSHAKE_TIMEOUT_S)
        finally:
            s.close()
    finally:
        stop(proc, workdir)


def case_normal_upgrade_still_succeeds():
    """A client that sends its full upgrade promptly must still get a 101: the
    deadline must not fire on a fast, legitimate handshake."""
    proc, ws_port, workdir = start()
    try:
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
                "Sec-WebSocket-Protocol: mqtt\r\n\r\n" % key)
            c.sendall(req.encode())
            c.settimeout(3)
            resp = b""
            while b"\r\n\r\n" not in resp:
                chunk = c.recv(1)
                if not chunk:
                    raise AssertionError("closed during a fast upgrade: %r" % resp)
                resp += chunk
            status = resp.split(b"\r\n", 1)[0]
            assert b" 101 " in status, "fast upgrade not accepted: %r" % status
            return "a prompt, complete upgrade still gets 101 Switching Protocols"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


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


case("WebSocket: one absolute upgrade deadline drops a slowloris that dribbles bytes",
     case_absolute_deadline_drops_slowloris)
case("WebSocket (control): a prompt complete upgrade still succeeds",
     case_normal_upgrade_still_succeeds)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
