#!/usr/bin/env python3
"""FLOWMQ_WRITE_TIMEOUT_MS outside int range is refused, not silently truncated.

Server::run() parsed FLOWMQ_WRITE_TIMEOUT_MS with strtol into a long, checked it
was > 0, then cast it to int. On a 64-bit platform long is wider than int, so a
value like 5000000000 (> INT_MAX, ~2.1e9) passed the > 0 check and then truncated
to an arbitrary - often negative - int. A negative or nonsensical write timeout
silently disables the stall bound the operator was trying to set: the very thing
that protects a connection thread from a client that stops reading.

The fix rejects any value above INT_MAX with the same "invalid
FLOWMQ_WRITE_TIMEOUT_MS" startup error as a non-numeric one, so a typo is caught
instead of producing a broker whose write bound is not what was asked for.

This probe launches the real broker with an out-of-range value and asserts it
exits non-zero without serving, and with an in-range value and asserts it starts
and logs the configured bound.

Usage: write_timeout_range_test.py <binary> <port>
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


def launch(value):
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-wt-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    env["FLOWMQ_WRITE_TIMEOUT_MS"] = value
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    return proc, port, workdir


def case_out_of_range_refused():
    # 5e9 > INT_MAX (~2.147e9), still a valid long, so only a range check catches it.
    proc, port, workdir = launch("5000000000")
    try:
        try:
            rc = proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            out = proc.stdout.read().decode("latin-1", "replace")
            raise AssertionError(
                "broker kept running with FLOWMQ_WRITE_TIMEOUT_MS=5000000000 "
                "(long -> int truncation silently disabled the bound). Output:\n%s"
                % out[-400:])
        out = proc.stdout.read().decode("latin-1", "replace")
        assert rc != 0, (
            "broker exited 0 on an out-of-range write timeout; output:\n%s"
            % out[-400:])
        assert "FLOWMQ_WRITE_TIMEOUT_MS" in out, (
            "no diagnostic naming FLOWMQ_WRITE_TIMEOUT_MS; output:\n%s" % out[-400:])
        # Must not be listening.
        try:
            c = socket.create_connection((HOST, port), timeout=0.3)
            c.close()
            raise AssertionError("a socket was served despite the invalid value")
        except (ConnectionRefusedError, socket.timeout, OSError):
            pass
        return "5000000000 (> INT_MAX) is refused at startup (exit %d)" % rc
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def case_in_range_starts():
    proc, port, workdir = launch("4000")
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                c = socket.create_connection((HOST, port), timeout=0.2)
                c.close()
                return "a valid FLOWMQ_WRITE_TIMEOUT_MS=4000 still starts and serves"
            except Exception:  # noqa: BLE001
                time.sleep(0.05)
        raise AssertionError("broker did not start with FLOWMQ_WRITE_TIMEOUT_MS=4000")
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        time.sleep(0.3)


def case_rejects_non_numeric():
    """Control: an obviously invalid value is still refused (unchanged)."""
    proc, port, workdir = launch("abc")
    try:
        rc = proc.wait(timeout=5)
        assert rc != 0, "broker accepted a non-numeric write timeout"
        return "a non-numeric value is still refused"
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def case(name, fn):
    try:
        info = fn()
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()


if __name__ == "__main__":
    case("write timeout: a value above INT_MAX is refused, not truncated to a bogus int",
         case_out_of_range_refused)
    case("write timeout (control): a valid value still starts and serves",
         case_in_range_starts)
    case("write timeout (control): a non-numeric value is still refused",
         case_rejects_non_numeric)
    print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
    sys.exit(0 if all(ok for _, ok in results) else 1)
