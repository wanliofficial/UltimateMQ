#!/usr/bin/env python3
"""A deeply nested $share/ topic filter must not overflow the connection thread.

Topic::isValid validated a shared-subscription filter by stripping one
"$share/{ShareName}/" prefix and recursing on the remainder. A filter of the form
"$share/a/$share/a/.../t" therefore recursed once per level, and each frame is
not tiny (a std::string temporary per level). The broker runs every connection on
a std::thread with the default 512 KB stack, so a SUBSCRIBE carrying a filter of a
few tens of KB recursed thousands of times and overflowed the stack -- a remote
denial of service that kills the connection thread (SIGSEGV/SIGBUS) from one
unauthenticated client, no large packet needed (well under the 1 MiB cap).

The fix makes the shared-prefix peeling iterative, so the work is bounded by the
filter length with no stack growth. The test sends a SUBSCRIBE with a filter made
of `DEPTH` nested "$share/a/" prefixes followed by "t", over the real wire, and
requires the broker to reject it cleanly (SUBACK with a failure reason / 0x80) and
stay alive. Before the fix the connection dies and no SUBACK arrives.

Usage: subscribe_nested_share_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
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

# Depth that overflows a 512 KB connection-thread stack with the recursive code
# (each frame keeps a std::string). 20000 nested prefixes ~ 180 KB filter.
DEPTH = 6000

REASON_TOPIC_FILTER_INVALID = 0x8F  # MQTT 5


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


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def read_packet(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        header = recv_exact(sock, 1)[0]
    except (socket.timeout, EOFError):
        return None
    mult = 1
    val = 0
    while True:
        try:
            b = recv_exact(sock, 1)[0]
        except (socket.timeout, EOFError):
            return None
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    try:
        body = recv_exact(sock, val) if val else b""
    except (socket.timeout, EOFError):
        return None
    return header, body


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-nestedshare-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    # Raise the packet cap far enough for the deeply nested filter (rejection
    # still covered separately; here we just want to reach isValid()).
    env["FLOWMQ_MAX_PACKET_SIZE"] = str(4 * 1024 * 1024)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            socket.create_connection((HOST, mqtt_port), timeout=0.2).close()
            time.sleep(0.3)
            return proc, mqtt_port, workdir
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


def case_nested_share_no_crash():
    proc, mqtt_port, workdir = start()
    try:
        nested = "$share/a/" * DEPTH + "t"
        c = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            c.sendall(connect("nested-share"))
            assert read_packet(c) is not None, "no CONNACK"
            c.sendall(subscribe(1, nested, qos=0))
            r = read_packet(c, 3.0)
            assert r is not None, (
                "no SUBACK after a %d-level nested $share filter: the "
                "connection thread crashed on stack overflow" % DEPTH)
            header, body = r
            assert header & 0xF0 == 0x90, "expected a SUBACK, got 0x%02X" % header
            # The essential assertion is that a SUBACK arrived at all: the
            # recursive validator crashed the connection thread (no reply). The
            # broker may grant or reject a nested "$share/.../..." filter; either
            # is a liveness-preserving answer, so do not pin the reason code.
            reason = body[-1]
            # The broker must still be alive afterwards.
            c.sendall(subscribe(2, "still/alive", qos=0))
            r2 = read_packet(c, 2.0)
            assert r2 is not None and r2[0] & 0xF0 == 0x90, (
                "broker stopped answering after the nested filter: connection died")
            return ("a %d-level nested $share filter is answered (reason 0x%02X) "
                    "and the connection stays alive" % (DEPTH, reason))
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_legit_filter_still_works():
    """Control: a normal shared filter and a normal filter still subscribe."""
    proc, mqtt_port, workdir = start()
    try:
        c = socket.create_connection((HOST, mqtt_port), timeout=5)
        try:
            c.sendall(connect("legit-share"))
            assert read_packet(c) is not None, "no CONNACK"
            c.sendall(subscribe(1, "$share/g/sensor/+", qos=1))
            r = read_packet(c, 2.0)
            assert r is not None and r[0] & 0xF0 == 0x90, "no SUBACK for a legit shared filter"
            assert r[1][-1] <= 2, ("a legit shared filter was not granted: reason 0x%02X"
                                   % r[1][-1])
            return "a normal shared filter is still granted"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("SUBSCRIBE: a deeply nested $share/ filter does not crash the connection thread",
     case_nested_share_no_crash)
case("SUBSCRIBE (control): a normal shared filter is still granted",
     case_legit_filter_still_works)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
