#!/usr/bin/env python3
"""A client that opens the TLS port and sends nothing must not pin a thread.

The TLS handshake ran SSL_accept() on a blocking socket with no read timeout.
A peer that completes the TCP handshake but never sends its ClientHello (or
stalls mid-handshake) therefore left the serving thread blocked in SSL_accept
forever, holding both the thread and its descriptor. Since every connection is
one thread, an attacker could exhaust the broker's entire connection budget with
nothing more than a handful of bare TCP connects -- a cheap slowloris against the
TLS port. The plaintext path already bounds an idle peer with a CONNECT timeout;
the handshake itself had no equivalent bound.

The fix drives SSL_accept with a bounded poll() loop (the socket is switched to
non-blocking for the handshake and restored afterwards), so a silent peer is
dropped after a fixed handshake timeout instead of holding the thread forever.

This probe launches the real broker with TLS enabled, opens several sockets that
send nothing, waits past the handshake timeout, and asserts every one has been
closed by the broker. Pre-fix the sockets stay open indefinitely.

Usage: tls_handshake_timeout_test.py <binary> <port>
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

# The broker's TLS handshake timeout is 10s; wait comfortably past it.
HANDSHAKE_TIMEOUT_S = 10
WAIT_S = HANDSHAKE_TIMEOUT_S + 3.0

results = []


class SkipTest(Exception):
    pass


def case(name, fn):
    try:
        info = fn()
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except SkipTest as exc:
        results.append((name, True))
        print("SKIP " + name + " :: " + str(exc))
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()


def gen_cert(workdir):
    cert = os.path.join(workdir, "cert.pem")
    key = os.path.join(workdir, "key.pem")
    rc = subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", key, "-out", cert, "-days", "1", "-subj", "/CN=localhost"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
    if rc != 0 or not (os.path.exists(cert) and os.path.exists(key)):
        raise SkipTest("openssl not available to generate a test certificate")
    return cert, key


def thread_count(pid):
    # Darwin: `ps -M` lists one line per thread; Linux: count /proc/<pid>/task.
    if os.path.isdir("/proc/%d/task" % pid):
        try:
            return len(os.listdir("/proc/%d/task" % pid))
        except OSError:
            return -1
    out = subprocess.run(["ps", "-M", "-p", str(pid)],
                         capture_output=True, text=True).stdout
    return out.count("\n")


def start(tls_cert, tls_key):
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-tls-hs-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_TLS_CERT"] = tls_cert
    env["FLOWMQ_TLS_KEY"] = tls_key
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            c = socket.create_connection((HOST, port), timeout=0.2)
            c.close()
            time.sleep(0.4)
            return proc, port, workdir
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


def socket_closed_by_peer(s):
    """True when the broker has closed s (recv returns b'' or the socket errors).
    False when it is still held open (recv times out)."""
    try:
        s.settimeout(1.5)
        s.sendall(b"")  # no-op probe
        data = s.recv(1)
        return data == b""  # orderly close
    except socket.timeout:
        return False  # still held open -> the bug
    except OSError:
        return True  # reset/closed


def case_silent_tls_peer_is_dropped():
    cert, key = gen_cert(tempfile.mkdtemp(prefix="flowmq-tls-gen-"))
    proc, port, workdir = start(cert, key)
    silent = []
    try:
        # Complete the TCP handshake only: never send a TLS ClientHello.
        for _ in range(4):
            silent.append(socket.create_connection((HOST, port), timeout=2))
        time.sleep(0.5)

        # Wait past the handshake timeout plus slack.
        time.sleep(WAIT_S)

        still_open = sum(0 if socket_closed_by_peer(s) else 1 for s in silent)
        assert still_open == 0, (
            "%d/%d silent TLS peers still held open %.0fs after connect -- the "
            "handshake has no timeout" % (still_open, len(silent), WAIT_S))
        return ("all %d silent TLS peers were dropped after the handshake timeout"
                % len(silent))
    finally:
        for s in silent:
            try:
                s.close()
            except Exception:  # noqa: BLE001
                pass
        stop(proc, workdir)


if __name__ == "__main__":
    case("TLS: a peer that never sends its ClientHello is dropped after the handshake timeout",
         case_silent_tls_peer_is_dropped)
    print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
    sys.exit(0 if all(ok for _, ok in results) else 1)
