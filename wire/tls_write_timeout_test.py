#!/usr/bin/env python3
"""A TLS client that stops reading is dropped; its stalled write is bounded.

SecureTransport set SO_SNDTIMEO only on the plaintext path (the `else` branch of
the constructor). For TLS the socket is blocking again after the handshake, so a
stalled SSL_write blocks inside OpenSSL's write(2) forever: the poll()-on-WANT_
WRITE fallback in writeAll() is unreachable because a blocking write never
returns WANT_WRITE. A client that completes the handshake, subscribes, and then
stops reading therefore pinned the broker's thread and its queued messages
indefinitely, with no write-stall deadline to drop it.

The fix sets SO_SNDTIMEO for both paths, so a stalled TLS write returns EAGAIN,
writeAll() reports failure, and the connection is dropped after
FLOWMQ_WRITE_TIMEOUT_MS just like plaintext.

Discriminator: after the flood fills the subscriber's receive window and we wait
past the write timeout, a fixed broker has already given up and closed the
connection, so resuming reads yields EOF/reset with the message stream cut short.
A buggy broker was merely blocked mid-write; resuming reads unblocks it and the
messages keep flowing, so the connection looks healthy and nothing was dropped.

This is an end-to-end wire test: it launches the real broker with TLS enabled and
drives it over TLS. Requires openssl to mint a throwaway certificate.

Usage: tls_write_timeout_test.py <binary> <port>
Exit code is 0 only when every case passes (or the cert cannot be generated).
"""
import os
import signal
import socket
import ssl
import struct
import subprocess
import sys
import tempfile
import time

HOST = "127.0.0.1"
BIN = sys.argv[1]
PORT = [int(sys.argv[2])]

# Broker write-stall bound for this run; the stalled reader is dropped after it.
WRITE_TIMEOUT_MS = 2000
# How long the subscriber refuses to read: comfortably past the write timeout so
# a correct broker has already dropped it.
STALL_S = 4.0
MSG_SIZE = 2000
MSG_COUNT = 1500

results = []


class SkipTest(Exception):
    pass


def enc_len(n):
    out = b""
    while True:
        d = n % 128
        n //= 128
        if n:
            d |= 0x80
        out += bytes([d])
        if not n:
            return out


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect_pkt(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe_pkt(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_pkt(topic, payload, qos=0, pid=1):
    body = utf8(topic)
    if qos:
        body += struct.pack("!H", pid)
    body += enc_len(0) + payload
    header = 0x30 | (qos << 1)
    return bytes([header]) + enc_len(len(body)) + body


def recv_pkt(c, timeout=2.0):
    c.settimeout(timeout)
    try:
        h = c.recv(1)
    except socket.timeout:
        return ("timeout",)
    except (ssl.SSLError, OSError):
        return ("err",)
    if not h:
        return ("eof",)
    n, mult = 0, 1
    while True:
        try:
            b = c.recv(1)
        except (ssl.SSLError, OSError):
            return ("eof",)
        if not b:
            return ("eof",)
        b = b[0]
        n += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    data = b""
    while len(data) < n:
        try:
            chunk = c.recv(n - len(data))
        except (ssl.SSLError, OSError):
            return ("eof",)
        if not chunk:
            return ("eof",)
        data += chunk
    return ("pkt", h[0], data)


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


def tls_context():
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    return ctx


def tls_connect(port, rcvbuf=None):
    raw = socket.create_connection((HOST, port), timeout=5)
    if rcvbuf is not None:
        raw.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, rcvbuf)
    return tls_context().wrap_socket(raw, server_hostname="localhost")


def start(cert, key):
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-tlswt-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_TLS_CERT"] = cert
    env["FLOWMQ_TLS_KEY"] = key
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    env["FLOWMQ_WRITE_TIMEOUT_MS"] = str(WRITE_TIMEOUT_MS)
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


def flood(port):
    """Publish as fast as possible until the broker backpressures us; the point
    is only to overflow the stalled subscriber's path, not to deliver a count."""
    pub = tls_connect(port)
    pub.settimeout(3.0)
    pub.sendall(connect_pkt("tlswt-pub"))
    try:
        recv_pkt(pub, 2.0)
    except Exception:  # noqa: BLE001
        pass
    payload = b"x" * MSG_SIZE
    try:
        for _ in range(MSG_COUNT):
            pub.sendall(publish_pkt("tlswt/x", payload, qos=0, pid=1))
    except (socket.timeout, ssl.SSLError, OSError):
        pass  # saturated: the stalled subscriber backpressured the broker
    finally:
        try:
            pub.close()
        except Exception:  # noqa: BLE001
            pass


def case_stalled_tls_reader_is_dropped():
    cert, key = gen_cert(tempfile.mkdtemp(prefix="flowmq-tlsgen-"))
    proc, port, workdir = start(cert, key)
    try:
        # Subscribe with a tiny receive buffer so the socket backpressures fast.
        sub = tls_connect(port, rcvbuf=4096)
        try:
            sub.sendall(connect_pkt("tlswt-sub"))
            r = recv_pkt(sub, 2.0)
            assert r[0] == "pkt" and r[1] & 0xF0 == 0x20, "no CONNACK: %r" % (r,)
            sub.sendall(subscribe_pkt(1, "tlswt/#", qos=0))
            r = recv_pkt(sub, 2.0)
            assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK: %r" % (r,)

            # Stop reading and let the flood fill the pipe, then wait past the
            # write timeout.
            flood(port)
            time.sleep(STALL_S)

            # Resume reading. A fixed broker already closed us (EOF/reset) with
            # the stream cut short; a buggy broker was merely blocked mid-write
            # and keeps delivering now that we read.
            received = 0
            disconnected = False
            sub.settimeout(2.0)
            deadline = time.time() + 5.0
            while time.time() < deadline:
                pkt = recv_pkt(sub, 1.0)
                if pkt[0] in ("eof", "err"):
                    disconnected = True
                    break
                if pkt[0] == "timeout":
                    # No EOF and no more data: if the broker never dropped us it
                    # is either done (finite flood) or still healthy.
                    break
                received += 1

            assert disconnected, (
                "a TLS client that stopped reading was NOT dropped after %dms: "
                "resuming reads yielded %d further messages and then stalled, so "
                "the stalled SSL_write is unbounded (SO_SNDTIMEO not set for TLS)"
                % (WRITE_TIMEOUT_MS, received))
            return ("the stalled TLS reader was dropped after the %dms write "
                    "timeout (resumed reads got EOF after %d msgs)"
                    % (WRITE_TIMEOUT_MS, received))
        finally:
            try:
                sub.close()
            except Exception:  # noqa: BLE001
                pass
    finally:
        stop(proc, workdir)


def case_healthy_tls_reader_is_served():
    """Control: a TLS reader that keeps reading must receive the flood without
    being dropped -- the timeout must only fire on a stalled write."""
    cert, key = gen_cert(tempfile.mkdtemp(prefix="flowmq-tlsgen-"))
    proc, port, workdir = start(cert, key)
    try:
        sub = tls_connect(port)
        try:
            sub.sendall(connect_pkt("tlswt-ok"))
            r = recv_pkt(sub, 2.0)
            assert r[0] == "pkt" and r[1] & 0xF0 == 0x20, "no CONNACK: %r" % (r,)
            sub.sendall(subscribe_pkt(1, "tlswt/#", qos=0))
            r = recv_pkt(sub, 2.0)
            assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK: %r" % (r,)

            flood(port)

            # Keep reading; the connection must stay healthy.
            sub.settimeout(3.0)
            received = 0
            disconnected = False
            idle_deadline = None
            while received < 50:
                pkt = recv_pkt(sub, 2.0)
                if pkt[0] in ("eof", "err"):
                    disconnected = True
                    break
                if pkt[0] == "timeout":
                    if idle_deadline is None:
                        idle_deadline = time.time() + 3.0
                    elif time.time() > idle_deadline:
                        break
                    continue
                idle_deadline = None
                received += 1
            assert not disconnected, (
                "a healthy TLS reader that keeps reading was dropped after %d "
                "messages; the write timeout fired wrongly" % received)
            assert received > 0, "healthy TLS reader received nothing"
            return "healthy TLS reader stayed connected and received %d messages" % received
        finally:
            try:
                sub.close()
            except Exception:  # noqa: BLE001
                pass
    finally:
        stop(proc, workdir)


def case(name, fn):
    try:
        info = fn()
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
    else:
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))


if __name__ == "__main__":
    case("TLS: a client that stops reading is dropped for its stalled write",
         case_stalled_tls_reader_is_dropped)
    case("TLS (control): a client that keeps reading is served, not dropped",
         case_healthy_tls_reader_is_served)
    print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
    sys.exit(0 if all(ok for _, ok in results) else 1)
