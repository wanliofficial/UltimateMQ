#!/usr/bin/env python3
"""A mTLS-looking config without a certificate must fail loudly, not run plaintext.

TLSContext::load() returned a null context (plaintext broker) as soon as
FLOWMQ_TLS_CERT and FLOWMQ_TLS_KEY were both unset - before it ever looked at
FLOWMQ_TLS_VERIFY_CLIENTS or FLOWMQ_TLS_CA. So an operator who set a CA and asked
for client verification, but forgot the server certificate, got an UNENCRYPTED
broker with no client authentication, while the configuration read as if mutual
TLS were on. That is the most dangerous possible failure mode for a TLS setup: it
succeeds, quietly, with none of the protection requested.

The fix refuses to start when client verification or a CA is configured without
the certificate/key pair, so the misconfiguration is caught at startup instead of
silently degrading to plaintext.

This probe launches the real broker with FLOWMQ_TLS_CA + FLOWMQ_TLS_VERIFY_CLIENTS
but no cert/key and asserts it exits non-zero without serving; the control case
confirms a plaintext broker (no TLS variables at all) still starts and serves.

Requires openssl to mint a throwaway CA.

Usage: tls_mtls_without_cert_test.py <binary> <port>
Exit code is 0 only when every case passes (or openssl is unavailable).
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


class SkipTest(Exception):
    pass


def gen_ca(workdir):
    ca = os.path.join(workdir, "ca.pem")
    key = os.path.join(workdir, "ca.key")
    rc = subprocess.run(
        ["openssl", "req", "-x509", "-newkey", "rsa:2048", "-nodes",
         "-keyout", key, "-out", ca, "-days", "1", "-subj", "/CN=test-ca"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL).returncode
    if rc != 0 or not os.path.exists(ca):
        raise SkipTest("openssl not available to generate a test CA")
    return ca


def case_mtls_without_cert_refuses_to_start():
    ca = gen_ca(tempfile.mkdtemp(prefix="flowmq-cagen-"))
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-mtls-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_TLS_CA"] = ca
    env["FLOWMQ_TLS_VERIFY_CLIENTS"] = "1"
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    # Deliberately NO FLOWMQ_TLS_CERT / FLOWMQ_TLS_KEY.
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    try:
        # It must exit on its own (fatal misconfiguration), not keep running.
        try:
            rc = proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            out = proc.stdout.read().decode("latin-1", "replace")
            raise AssertionError(
                "broker kept running with FLOWMQ_TLS_CA + VERIFY_CLIENTS but no "
                "certificate - it is serving plaintext without client auth. "
                "Startup output:\n%s" % out[-400:])
        out = proc.stdout.read().decode("latin-1", "replace")
        assert rc != 0, (
            "broker exited 0 on a mTLS-without-certificate config; output:\n%s"
            % out[-400:])
        # And it must not have been listening on the port.
        try:
            c = socket.create_connection((HOST, port), timeout=0.3)
            c.close()
            raise AssertionError("a socket was served despite the fatal config")
        except (ConnectionRefusedError, socket.timeout, OSError):
            pass
        return ("mTLS config without a certificate is refused at startup (exit %d)"
                % rc)
    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


def case_plaintext_still_starts():
    """Control: with no TLS variables at all the broker must still start and
    serve plaintext - the new check must not break the default path."""
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-plain-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                c = socket.create_connection((HOST, port), timeout=0.2)
                c.close()
                return "a plaintext broker (no TLS variables) still starts and serves"
            except Exception:  # noqa: BLE001
                time.sleep(0.05)
        raise AssertionError("plaintext broker did not start")
    finally:
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
        time.sleep(0.3)


def case(name, fn):
    try:
        info = fn()
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except SkipTest as exc:
        results.append((name, True))
        print("SKIP " + name + " :: " + str(exc))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()


if __name__ == "__main__":
    case("TLS: client verification without a certificate is refused at startup (not plaintext)",
         case_mtls_without_cert_refuses_to_start)
    case("TLS (control): a plaintext broker with no TLS variables still starts",
         case_plaintext_still_starts)
    print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
    sys.exit(0 if all(ok for _, ok in results) else 1)
