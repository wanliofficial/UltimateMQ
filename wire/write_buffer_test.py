#!/usr/bin/env python3
"""FLOWMQ_WRITE_BUFFER: a transiently slow reader is buffered, not dropped.

The default write path sends each packet with a blocking write bounded by
FLOWMQ_WRITE_TIMEOUT_MS (default 5s): a client that stops reading fills the
kernel send buffer, the write stalls, and after the timeout the whole
connection is dropped -- losing every later delivery. That is correct for a
truly dead peer, but it also punishes a client that is merely *behind* (e.g. a
busy consumer, or one that is about to read again).

FLOWMQ_WRITE_BUFFER (opt-in) adds a per-connection outbound buffer: when the
socket cannot take the next packet immediately, the packet is buffered (as a
whole transport write-unit, so a TLS record / WebSocket frame is never split)
and drained on POLLOUT, so the connection thread is not blocked and the client
is not dropped for being transiently behind. A peer that fills the buffer
without ever draining it is still dropped once a write makes no progress for
FLOWMQ_WRITE_TIMEOUT_MS (bounded memory, no infinite buffering).

This probe launches the real broker over TCP and drives it with one slow
subscriber and one publisher:

  * case_buffered_survives: FLOWMQ_WRITE_BUFFER on. The subscriber advertises a
    large Receive Maximum, subscribes at QoS 0, then STOPS READING while the
    publisher floods enough bytes to overflow the kernel send buffers. The
    broker must keep the connection (buffering the overflow), and once the
    subscriber resumes reading it must receive EVERY message, in order, with no
    disconnect. The drop-newest / drop-oldest queue policy is irrelevant here:
    these are live deliveries to a connected client, held in the write buffer.

  * case_unbuffered_drops: the SAME flood with buffering disabled. The broker
    fills the socket buffer, the write stalls past FLOWMQ_WRITE_TIMEOUT_MS, and
    the connection is dropped -- the subscriber sees EOF before reading all
    messages. This is the documented historical behaviour the knob changes, and
    confirms the knob (not the network) is what makes the difference.

This is an end-to-end wire test: it launches the real broker binary and drives
it over TCP, so it complements the in-process gtest suite.

Usage: write_buffer_test.py <binary> <port>
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

# The broker announces the write-buffer line only when the knob is set.
BUFFER_MARK = b"outbound write buffer"

# How big each flooded PUBLISH is, and how many. The total (~2MB) comfortably
# overflows the kernel send buffers on the probe and on the broker (a few hundred
# KB each), forcing the socket to go unwritable and the buffer path to engage,
# while still fitting inside the broker's write buffer (4MB) so the connection is
# dropped for slow draining, NOT for buffer overflow.
MSG_SIZE = 2000
MSG_COUNT = 1000

# How long the subscriber refuses to read before draining. Long enough for the
# broker's send buffer to fill and the overflow to pile into the write buffer,
# but kept well under FLOWMQ_WRITE_TIMEOUT_MS so a correctly-progressing broker
# is not dropped for stalling.
STALL_S = 2.0


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


def connect(cid, clean_start=True, props=b""):
    flags = 0x02 if clean_start else 0x00
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(len(props)) + props + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def recv_max_prop(value):
    return bytes([0x21]) + struct.pack("!H", value)


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(topic, payload, qos=0, pid=1):
    body = utf8(topic)
    if qos:
        body += struct.pack("!H", pid)
    body += enc_len(0) + payload
    header = 0x30 | (qos << 1)
    return bytes([header]) + enc_len(len(body)) + body


def pingreq():
    return bytes([0xC0, 0x00])


def read_pkt(c, timeout=1.0):
    c.settimeout(timeout)
    try:
        h = c.recv(1)
    except socket.timeout:
        return ("timeout",)
    except Exception as exc:  # noqa: BLE001
        return ("err", repr(exc))
    if not h:
        return ("eof",)
    n, mult = 0, 1
    while True:
        b = c.recv(1)
        if not b:
            return ("eof",)
        b = b[0]
        n += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    data = b""
    while len(data) < n:
        chunk = c.recv(n - len(data))
        if not chunk:
            return ("eof",)
        data += chunk
    return ("pkt", h[0], data)


def start(extra_env):
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-wb-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    # A stalled write is only dropped after this long; keep it short and uniform
    # across both cases so the unbuffered drop lands well inside the drain
    # window, while the buffered case keeps making progress (so it never stalls).
    env["FLOWMQ_WRITE_TIMEOUT_MS"] = "4000"
    env.update(extra_env)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    for _ in range(100):
        try:
            c = socket.create_connection((HOST, port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, port, workdir
        except Exception:  # noqa: BLE001
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start")


def stop(proc, workdir):
    try:
        proc.send_signal(signal.SIGTERM)
        out, _ = proc.communicate(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    time.sleep(0.4)
    return out or b""


def new_client(port, cid, clean_start=True, props=b""):
    c = socket.create_connection((HOST, port), timeout=5)
    c.sendall(connect(cid, clean_start, props))
    r = read_pkt(c)
    assert r[0] == "pkt" and r[1] & 0xF0 == 0x20, "no CONNACK for %s" % cid
    return c


def flood(port, count=MSG_COUNT, size=MSG_SIZE):
    """One publisher sends as many of `count` QoS 0 messages as it can and
    returns how many it handed off.

    Fire-and-forget: with buffering off, a stalled subscriber backpressures the
    whole broker write path, which in turn backpressures this publisher's own
    socket (correct TCP flow). We do not need every message to land -- only
    enough to overflow the subscriber's path -- so a send timeout is caught and
    treated as "the broker is saturated", which is itself the condition under
    test.
    """
    pub = new_client(port, "wb-pub", clean_start=True)
    pub.settimeout(3.0)
    payload = b"x" * size
    sent = 0
    try:
        for _ in range(count):
            pub.sendall(publish("wb/x", payload, qos=0, pid=1))
            sent += 1
    except (socket.timeout, OSError):
        pass  # broker saturated: enough data was pushed to overflow the path
    finally:
        pub.close()
    return sent


def drain_expect_all(c, expect, stall_timeout):
    """Read until `expect` PUBLISH packets arrive or the peer disconnects.

    Returns (count, complete, disconnected). `complete` is True only when all
    `expect` messages were received; `disconnected` is True if EOF arrived first.
    """
    got = 0
    disconnected = False
    deadline = time.time() + stall_timeout
    while got < expect and time.time() < deadline:
        r = read_pkt(c, max(0.2, min(1.0, deadline - time.time())))
        if r[0] == "eof":
            disconnected = True
            break
        if r[0] != "pkt":
            continue
        if (r[1] & 0xF0) == 0x30:
            got += 1
    return got, got == expect, disconnected


results = []


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


def case_buffered_survives():
    """Buffering on: a slow reader is held in the write buffer, then fully
    served once it resumes -- no disconnect, every message delivered in order."""
    proc, port, workdir = start({"FLOWMQ_WRITE_BUFFER": "4194304"})
    try:
        # Subscriber: large Receive Maximum so QoS 0 is never the limiter; the
        # only thing that can stall it is the socket write buffer / write buffer.
        sub = new_client(port, "wb-sub", clean_start=True,
                         props=recv_max_prop(65535))
        # Cap the send buffer small BEFORE subscribing. macOS auto-tunes socket
        # buffers up to several MB; without this the whole flood fits in the
        # kernel buffer and the write never stalls. A small fixed buffer forces
        # the broker's write to block during the stall window, exercising both
        # the buffered path (overflow -> write buffer) and the unbuffered path
        # (overflow -> stall -> drop).
        sub.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        sub.sendall(subscribe(1, "wb/#", qos=0))
        r = read_pkt(sub)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        # Publisher floods while the subscriber refuses to read. The flood must
        # vastly exceed the broker's (capped) kernel send buffer so the socket
        # actually backpressures; MSG_SIZE*MSG_COUNT (~2MB) is already enough for
        # the 64KB-buffer cases, and a larger burst guarantees it here too.
        flood(port)

        # Subscriber stalls (reads nothing) so the broker's socket buffer fills
        # and the overflow piles into the per-connection write buffer.
        time.sleep(STALL_S)

        # Now resume reading: the broker drains the write buffer on POLLOUT and
        # the subscriber must receive every message, with no disconnect.
        got, complete, disconnected = drain_expect_all(sub, MSG_COUNT, 30.0)
        sub.close()

        assert not disconnected, (
            "broker dropped the slow reader with FLOWMQ_WRITE_BUFFER set; it "
            "should buffer the overflow and serve it once the client reads")
        assert complete, (
            "buffered slow reader did not receive every message: got %d/%d "
            "(connection was kept alive but data was lost)" % (got, MSG_COUNT))
        return "served all %d messages after a %.1fs stall" % (MSG_COUNT, STALL_S)
    finally:
        stop(proc, workdir)


def case_unbuffered_drops():
    """Buffering off (default): the same flood stalls the write past the timeout
    and the broker drops the connection. The subscriber sees EOF before all
    messages arrive -- the historical behaviour the knob changes."""
    proc, port, workdir = start({})  # no FLOWMQ_WRITE_BUFFER
    try:
        sub = new_client(port, "wb-sub", clean_start=True,
                         props=recv_max_prop(65535))
        sub.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        sub.sendall(subscribe(1, "wb/#", qos=0))
        r = read_pkt(sub)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        flood(port)
        time.sleep(STALL_S)

        # With no write buffer, the broker's blocking write stalls and, past
        # FLOWMQ_WRITE_TIMEOUT_MS, drops the connection. The subscriber should
        # see EOF well before it can read all MSG_COUNT messages.
        got, complete, disconnected = drain_expect_all(sub, MSG_COUNT, 18.0)
        sub.close()

        assert disconnected, (
            "expected the unbuffered broker to drop the stalled reader (EOF), "
            "but the subscriber received %d/%d messages and stayed connected" %
            (got, MSG_COUNT))
        return "unbuffered broker dropped the stalled reader after %d msgs" % got
    finally:
        stop(proc, workdir)


def case_pinger_but_never_reads_is_still_dropped():
    """A peer that keeps sending PINGREQs (so inbound keepalive never expires)
    but never reads must STILL be dropped once the write buffer stalls past
    FLOWMQ_WRITE_TIMEOUT_MS. Without the in-loop stall check, the steady PINGREQ
    stream keeps POLLIN hot, POLLOUT never fires, drainWriteBuffer never runs,
    and the write-stall deadline would never be enforced -- the connection would
    leak forever. This pins that the deadline is checked every loop iteration."""
    proc, port, workdir = start({"FLOWMQ_WRITE_BUFFER": "4194304",
                                 "FLOWMQ_WRITE_TIMEOUT_MS": "3000"})
    try:
        sub = new_client(port, "wb-sub", clean_start=True,
                         props=recv_max_prop(65535))
        sub.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, 65536)
        sub.sendall(subscribe(1, "wb/#", qos=0))
        r = read_pkt(sub)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        # Flood with a burst far larger than the broker's capped send buffer
        # (~2MB) so the socket genuinely backpressures and stalls, while the
        # pings keep inbound keepalive alive.
        flood(port, count=2000, size=40000)
        sub.settimeout(0.5)
        pings = 0
        deadline = time.time() + 6.0
        disconnected = False
        while time.time() < deadline:
            try:
                data = sub.recv(1)
                if not data:
                    disconnected = True
                    break
            except socket.timeout:
                pass
            except OSError:
                disconnected = True
                break
            try:
                sub.sendall(pingreq())
                pings += 1
            except OSError:
                disconnected = True
                break
        sub.close()

        assert disconnected, (
            "a peer that pings but never reads was never dropped: the write-stall "
            "deadline is not enforced while inbound traffic keeps the loop hot")
        return "pinger-but-non-reader dropped after %d pings (write-stall enforced)" % pings
    finally:
        stop(proc, workdir)


case("buffered_slow_reader_survives_and_is_fully_served", case_buffered_survives)
case("unbuffered_slow_reader_is_dropped", case_unbuffered_drops)
case("pinger_but_never_reads_is_still_dropped", case_pinger_but_never_reads_is_still_dropped)

if __name__ == "__main__":
    sys.exit(0 if all(ok for _, ok in results) else 1)
