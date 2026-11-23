#!/usr/bin/env python3
"""A PEM bundle with an intermediate is sent as a chain, so clients can verify it.

TLSContext::load() called SSL_CTX_use_certificate_file(), which loads only the
FIRST certificate in a PEM file. An operator who points FLOWMQ_TLS_CERT at a
bundle (leaf certificate first, then the intermediate CA certificates) therefore
had the intermediates silently dropped: the server presented its leaf only, and a
client that did not already hold the intermediate could not build a path to its
trust anchor, so the handshake failed even though the configuration was correct.
Switching to SSL_CTX_use_certificate_chain_file() loads the leaf plus every
intermediate in the bundle (and accepts a single-cert file unchanged).

This probe builds a three-level PKI (root CA -> intermediate CA -> leaf), starts
the broker with a leaf+intermediate bundle as FLOWMQ_TLS_CERT, and connects with a
client that trusts ONLY the root CA. The handshake succeeds if and only if the
server sent the intermediate. A control case connects to a server configured with
the leaf alone while the client trusts leaf+intermediate, proving the failure mode
the fix removes is real and specific to a missing chain.

Requires openssl.

Usage: tls_cert_chain_test.py <binary> <port>
Exit code is 0 only when every case passes (or openssl is unavailable).
"""
import os
import signal
import socket
import ssl
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


def run(*args):
    return subprocess.run(args, stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL).returncode


def openssl(*args):
    rc = run("openssl", *args)
    if rc != 0:
        raise SkipTest("openssl command failed: %s" % " ".join(args))


def build_pki(workdir):
    root_key = os.path.join(workdir, "root.key")
    root_crt = os.path.join(workdir, "root.pem")
    int_key = os.path.join(workdir, "int.key")
    int_csr = os.path.join(workdir, "int.csr")
    int_crt = os.path.join(workdir, "int.pem")
    leaf_key = os.path.join(workdir, "leaf.key")
    leaf_csr = os.path.join(workdir, "leaf.csr")
    leaf_crt = os.path.join(workdir, "leaf.pem")
    ext = os.path.join(workdir, "leaf.ext")

    openssl("req", "-x509", "-newkey", "rsa:2048", "-nodes", "-keyout", root_key,
            "-out", root_crt, "-days", "2", "-subj", "/CN=Test Root CA",
            "-addext", "basicConstraints=critical,CA:TRUE")

    openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", int_key,
            "-out", int_csr, "-subj", "/CN=Test Intermediate CA")
    with open(ext, "w") as f:
        f.write("basicConstraints=critical,CA:TRUE\n"
                "keyUsage=critical,keyCertSign,cRLSign\n")
    openssl("x509", "-req", "-in", int_csr, "-CA", root_crt, "-CAkey", root_key,
            "-CAcreateserial", "-out", int_crt, "-days", "2",
            "-extfile", ext)

    openssl("req", "-newkey", "rsa:2048", "-nodes", "-keyout", leaf_key,
            "-out", leaf_csr, "-subj", "/CN=localhost")
    with open(ext, "w") as f:
        f.write("subjectAltName=DNS:localhost\n"
                "basicConstraints=CA:FALSE\n"
                "keyUsage=critical,digitalSignature,keyEncipherment\n"
                "extendedKeyUsage=serverAuth\n")
    openssl("x509", "-req", "-in", leaf_csr, "-CA", int_crt, "-CAkey", int_key,
            "-CAcreateserial", "-out", leaf_crt, "-days", "2",
            "-extfile", ext)

    # Bundle: leaf first, then the intermediate (the usual nginx/caddy layout).
    bundle = os.path.join(workdir, "bundle.pem")
    with open(bundle, "wb") as out:
        for p in (leaf_crt, int_crt):
            with open(p, "rb") as crt:
                out.write(crt.read())

    return {"root": root_crt, "int": int_crt, "leaf": leaf_crt,
            "leaf_key": leaf_key, "bundle": bundle}


def start(cert, key):
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-chain-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_TLS_CERT"] = cert
    env["FLOWMQ_TLS_KEY"] = key
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


def client_handshake_ok(port, cafile):
    """True if the TLS handshake completes with the given trust anchor."""
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_REQUIRED
    ctx.load_verify_locations(cafile=cafile)
    raw = socket.create_connection((HOST, port), timeout=3)
    try:
        with ctx.wrap_socket(raw, server_hostname="localhost"):
            return True
    except (ssl.SSLError, OSError):
        return False
    finally:
        try:
            raw.close()
        except Exception:  # noqa: BLE001
            pass


def case_bundle_sends_intermediate():
    """Trusting only the ROOT, a client verifies a server presenting leaf+intermediate
    only if the intermediate is actually sent."""
    pki = build_pki(tempfile.mkdtemp(prefix="flowmq-pki-"))
    proc, port, workdir = start(pki["bundle"], pki["leaf_key"])
    try:
        ok = client_handshake_ok(port, pki["root"])
        assert ok, (
            "a client trusting only the root CA could not verify the server "
            "presenting a leaf+intermediate bundle -- the intermediate was not "
            "sent (certificate loaded as a single cert, not a chain)")
        return "leaf+intermediate bundle is sent as a chain; root-only client verifies"
    finally:
        stop(proc, workdir)


def case_leaf_only_needs_intermediate_in_client():
    """Control: with the server presenting only the LEAF, a root-only client must
    FAIL (proving the chain, not something else, is what makes the case above
    pass), while a client that already trusts leaf+intermediate succeeds."""
    pki = build_pki(tempfile.mkdtemp(prefix="flowmq-pki-"))
    proc, port, workdir = start(pki["leaf"], pki["leaf_key"])
    try:
        # Client trusting only the root: cannot build the path (no intermediate).
        root_only_ok = client_handshake_ok(port, pki["root"])
        assert not root_only_ok, (
            "a root-only client verified a leaf-only server; the control is not "
            "discriminating (the intermediate must be required)")

        # Client that already trusts the intermediate: succeeds, proving the
        # server itself and the leaf are fine.
        with_int = os.path.join(workdir, "int_plus_root.pem")
        with open(with_int, "wb") as out:
            for p in (pki["root"], pki["int"]):
                with open(p, "rb") as crt:
                    out.write(crt.read())
        int_ok = client_handshake_ok(port, with_int)
        assert int_ok, "a client trusting root+intermediate failed against a leaf server"
        return ("leaf-only server: root-only client fails, root+intermediate client "
                "succeeds (the intermediate is what the chain supplies)")
    finally:
        stop(proc, workdir)


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
    case("TLS: a leaf+intermediate PEM bundle is sent as a chain (root-only client verifies)",
         case_bundle_sends_intermediate)
    case("TLS (control): a leaf-only server requires the client to hold the intermediate",
         case_leaf_only_needs_intermediate_in_client)
    print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
    sys.exit(0 if all(ok for _, ok in results) else 1)
