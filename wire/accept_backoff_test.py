#!/usr/bin/env python3
"""accept() EMFILE/ENFILE back off instead of spinning the accept loop.

When the broker runs out of file descriptors, accept() fails with EMFILE/ENFILE
but the kernel keeps the just-arrived connection queued and the listening
socket readable. poll() therefore reports POLLIN again immediately, the loop
calls accept() once more, it fails the same way, and the single broker thread
spins at 100% CPU (and floods the log) until an fd frees up. This is reachable
in normal operation: every connection holds a descriptor, so the broker's fd
limit is hit well before its configured max_connections ceiling. The default
soft RLIMIT_NOFILE is 256 on macOS, far below the 1024 connection ceiling.

The fix pauses each listener for a short backoff when accept() fails with
EMFILE/ENFILE (its POLLIN mask is dropped until the deadline, and the poll
timeout is clamped so the loop wakes to retry), and clears the pause on the
next successful accept. So an overloaded broker yields the CPU instead of
spinning, and resumes accepting the moment descriptors free up.

This probe launches the real broker, raises its OWN fd ceiling, and opens a
flood of idle connections to drive the broker into EMFILE:
  * it asserts the broker logs the backoff line (proving the EMFILE branch ran);
  * it samples the broker's CPU during the exhausted window and asserts it is
    NOT spinning (a tight accept loop would burn ~a full core);
  * after closing the flood it asserts the broker recovers -- a fresh client
    completes CONNECT -- proving the backoff cleared and accepting resumed.
If the host's fd limit is high enough that the flood cannot exhaust it, the
probe skips (the behaviour cannot be triggered in-process here).

This is an end-to-end wire test: it launches the real broker binary and drives
it over TCP, so it complements the in-process gtest suite.

Usage: accept_backoff_test.py <binary> <port>
Exit code is 0 only when every case passes (a skip is also 0).
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

# Substring of the broker's EMFILE/ENFILE log line (Server.cpp).
BACKOFF_MARK = b"pausing accepts briefly"

# How many idle connections to attempt while hunting for EMFILE. Far above the
# macOS default soft limit (256) so exhaustion is reliably reached there; on a
# host with a much higher limit the flood simply stops at the cap and we skip.
FLOOD_TARGET = 600

# How long to keep the backlog full while sampling the broker's CPU. Long
# enough to average out scheduler noise, short enough to keep the probe quick.
CPU_WINDOW_S = 2.0


def raise_own_fd_limit():
    """Let this probe hold far more sockets than the broker can hold, so the
    BROKER (not the probe) is the one that runs out of descriptors."""
    try:
        import resource
    except ImportError:
        return False
    try:
        _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        target = 65536
        if hard != resource.RLIM_INFINITY:
            target = min(target, hard)
        resource.setrlimit(resource.RLIMIT_NOFILE, (target, hard))
        return True
    except (ValueError, OSError):
        return False


def start():
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-backoff-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    # Capture stderr so we can observe the broker's backoff log line.
    err = open(os.path.join(workdir, "broker.err"), "wb")
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=err,
                            preexec_fn=cap_broker_fd_limit)
    for _ in range(100):
        try:
            c = socket.create_connection((HOST, port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, port, workdir, err
        except Exception:  # noqa: BLE001
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start")


def cap_broker_fd_limit():
    """Run in the broker child before exec: give it a SMALL fd ceiling so the
    flood can exhaust it in-process, regardless of the (raised) limit this
    probe runs with. The broker needs only a handful of fds to run, so 64 is
    generous for startup yet trivially exhausted by ~50 idle connections."""
    try:
        import resource
        _soft, hard = resource.getrlimit(resource.RLIMIT_NOFILE)
        soft = min(64, hard) if hard != resource.RLIM_INFINITY else 64
        resource.setrlimit(resource.RLIMIT_NOFILE, (soft, hard))
    except (ValueError, OSError):
        pass


def stop(proc, workdir, err):
    try:
        err.close()
    except Exception:  # noqa: BLE001
        pass
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    time.sleep(0.3)


def broker_cpu_seconds(pid):
    """Cumulative CPU time (user+sys) of a process from `ps -o time`."""
    out = subprocess.run(["ps", "-o", "time=", "-p", str(pid)],
                         capture_output=True, text=True).stdout.strip()
    # Formats like "1:23", "0:01.20", or "1-02:03:04" (days).
    days = 0
    if "-" in out:
        d, out = out.split("-", 1)
        days = int(d)
    parts = out.split(":")
    parts = [float(p) for p in parts]
    while len(parts) < 3:
        parts.insert(0, 0.0)
    h, m, s = parts
    return days * 86400 + h * 3600 + m * 60 + s


def read_marker(err):
    err.flush()
    try:
        with open(err.name, "rb") as f:
            data = f.read()
    except FileNotFoundError:
        return False
    return BACKOFF_MARK in data


def broker_accepts(port):
    """True while the broker still completes an accept+CONNECT cycle, False
    once it has stopped accepting (fd exhaustion). This is fix-independent: it
    measures observable service, not the broker's own log output."""
    try:
        c = socket.create_connection((HOST, port), timeout=1.0)
    except OSError:
        return False
    try:
        c.sendall(connect("probe"))
        c.settimeout(0.6)
        return c.recv(1) == b"\x20"
    except OSError:
        return False
    finally:
        try:
            c.close()
        except Exception:  # noqa: BLE001
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


import struct  # noqa: E402  (kept after the helpers that don't need it)


def connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


results = []


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


class SkipTest(Exception):
    pass


def case_emfile_backs_off_and_recovers():
    raised = raise_own_fd_limit()
    proc, port, workdir, err = start()
    held = []
    exhausted = False
    import threading
    try:
        # Phase 1 -- drive the broker into fd exhaustion: keep opening idle
        # connections until it stops completing new CONNACKs. The broker caps
        # its own fd ceiling well below this flood, so this is reached quickly;
        # if the host limit is too high to exhaust in-process, skip.
        for _ in range(FLOOD_TARGET):
            try:
                c = socket.create_connection((HOST, port), timeout=1.0)
                held.append(c)  # keep open so the broker keeps the fd
            except OSError:
                break
            if len(held) % 5 == 0 and not broker_accepts(port):
                exhausted = True
                break
        if not exhausted:
            exhausted = not broker_accepts(port)
        if not exhausted:
            raise SkipTest(
                "could not exhaust the broker's fd limit in-process "
                "(host limit too high for this flood); behaviour not triggered")

        # Phase 2 -- keep the backlog NON-empty while sampling. This is the
        # condition under which the old code spins: poll() returns immediately
        # (listener readable), accept() fails with EMFILE, repeat. The fix drops
        # the POLLIN mask for a backoff window instead, so the loop sleeps.
        filler_stop = threading.Event()

        def keep_filling():
            while not filler_stop.is_set():
                try:
                    held.append(socket.create_connection((HOST, port), timeout=1.0))
                except OSError:
                    time.sleep(0.01)

        filler = threading.Thread(target=keep_filling)
        filler.start()
        try:
            t0 = broker_cpu_seconds(proc.pid)
            wall_start = time.time()
            time.sleep(CPU_WINDOW_S)
            t1 = broker_cpu_seconds(proc.pid)
        finally:
            filler_stop.set()
            filler.join()
        cpu_used = t1 - t0
        wall = time.time() - wall_start

        # The fix must keep the broker off-CPU. Measured: the old code burns
        # ~0.2*wall (paced by its per-failure log write), the fix ~0.01*wall; the
        # 0.10*wall bound sits safely between and would flag a regression to a
        # tight spin.
        assert cpu_used < 0.10 * wall, (
            "broker burned %.2fs CPU over %.1fs wall while in EMFILE backoff "
            "-- looks like an accept spin" % (cpu_used, wall))

        # The backoff path ran (fix-specific: the old code never logs this).
        assert read_marker(err), (
            "EMFILE reached but the broker never logged its accept backoff")

        # Phase 3 -- free descriptors: close the whole flood. The broker's
        # accepted threads see EOF and exit; within one backoff window it
        # resumes accepting. A fresh client must complete CONNECT.
        for c in held:
            try:
                c.close()
            except Exception:  # noqa: BLE001
                pass
        held = []
        time.sleep(1.5)

        client = socket.create_connection((HOST, port), timeout=5)
        try:
            client.sendall(connect("recover"))
            client.settimeout(5)
            head = client.recv(1)
            assert head == b"\x20", (
                "no CONNACK after recovering from EMFILE backoff: %r" % (head,))
            return ("broker hit EMFILE, logged backoff, stayed off-CPU "
                    "(%.2fs/%.1fs), and recovered a fresh CONNECT"
                    % (cpu_used, wall))
        finally:
            client.close()
    finally:
        stop(proc, workdir, err)


if __name__ == "__main__":
    case("accept(): EMFILE/ENFILE backs off (no spin) and recovers",
         case_emfile_backs_off_and_recovers)
    print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
    sys.exit(0 if all(ok for _, ok in results) else 1)
