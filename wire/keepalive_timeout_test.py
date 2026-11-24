#!/usr/bin/env python3
"""MQTT-3.1.2-24 / §3.1.2-10 / MQTT-4.13: the Keep Alive timer and its timeout.

Keep Alive is a contract about rhythm, not just a number the client advertises:
if the Client does not send another Control Packet within 1.5 times the Keep
Alive interval, the Server MUST close the Network Connection [MQTT-3.1.2-24].
The timer resets on every complete packet received (§3.1.2-20), so a client
that keeps talking stays connected; a client that goes silent is dropped exactly
at the 1.5x mark, not at the nominal Keep Alive.

The close is abnormal, so the Client's Will Message (if any) is published:
MQTT-3.1.2-8 deletes the Will only on a graceful DISCONNECT 0x00, which never
arrives here. And an MQTT 5 Server tells the client why it is leaving --
DISCONNECT 0x8D (KEEP_ALIVE_TIMEOUT) -- rather than letting it confuse a timeout
with a network fault (MQTT-4.13).

This drives the real timer over the wire (a short Keep Alive, then silence):

  * a client that connects with Keep Alive 1 and then sends nothing is closed:
    it receives DISCONNECT 0x8D, and a subscriber on its Will topic receives the
    Will Message (proving the close was treated as abnormal);
  * control: a client that sends a PINGREQ inside every Keep Alive window keeps
    the connection -- no DISCONNECT, no Will -- proving the timeout tracks the
    last received packet, not elapsed wall-clock alone.

Note these cases are deliberately slower (a couple of seconds each) because the
behaviour under test is a real-time deadline.

Usage: keepalive_timeout_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time

HOST = "127.0.0.1"
BIN = sys.argv[1]
PORT = [int(sys.argv[2])]

DISCONNECT = 0xE0
PINGREQ = 0xC0
PINGRESP = 0xD0

REASON_KEEP_ALIVE_TIMEOUT = 0x8D


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


def connect(cid, keepalive, will_topic=None, will_msg=None):
    has_will = will_topic is not None
    flags = 0x02 | (0x04 if has_will else 0x00)
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", keepalive) + enc_len(0) + utf8(cid)
    if has_will:
        body += enc_len(0) + utf8(will_topic) + struct.pack("!H", len(will_msg)) + will_msg
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def pingreq():
    return bytes([PINGREQ, 0x00])


def read_packet(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        header = _recv_exact(sock, 1)[0]
    except (socket.timeout, EOFError):
        return None
    mult = 1
    val = 0
    while True:
        try:
            b = _recv_exact(sock, 1)[0]
        except (socket.timeout, EOFError):
            return None
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = _recv_exact(sock, val) if val else b""
    return header, body


def _recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-ka-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            c = socket.create_connection((HOST, mqtt_port), timeout=0.2)
            c.close()
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


def _drain_publish(sock, timeout):
    """Return (topic, payload) of the first QoS 0 PUBLISH, or None if none."""
    pkt = read_packet(sock, timeout)
    if pkt is None:
        return None
    h, b = pkt
    if h & 0xF0 != 0x30:
        return ("non-publish", h, b)
    tlen = struct.unpack("!H", b[0:2])[0]
    off = 2 + tlen
    plen = 0
    mult = 1
    while True:
        x = b[off]
        off += 1
        plen += (x & 127) * mult
        mult *= 128
        if not x & 128:
            break
    topic = b[2:2 + tlen].decode()
    payload = b[off + plen:]
    return (topic, payload)


def case_timeout_closes_and_publishes_will():
    """Keep Alive 1, then silence: the server closes at ~1.5x, sending DISCONNECT
    0x8D to the client, and the Will is published to the subscriber."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        cli = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("ka-sub", 60))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
            cli.sendall(connect("ka-cli", 1, will_topic="ka/status",
                                will_msg=b"timed-out"))
            assert read_packet(cli)[0] & 0xF0 == 0x20, "no CONNACK (client)"
            sub.sendall(subscribe(1, "ka/#", qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            # Stay silent past 1.5x the 1s Keep Alive. The server closes the client
            # connection at that point and publishes its Will.
            pkt = read_packet(cli, 4.0)
            assert pkt is not None, "server sent nothing; it did not enforce Keep Alive"
            h, b = pkt
            assert h & 0xF0 == DISCONNECT, (
                "expected DISCONNECT for a Keep Alive timeout, got header 0x%02X" % h)
            assert b and b[0] == REASON_KEEP_ALIVE_TIMEOUT, (
                "a Keep Alive timeout must be DISCONNECT 0x8D, got %r" % b)

            got = _drain_publish(sub, 2.0)
            assert got is not None and got[0] != "non-publish", (
                "an abnormal close must publish the Will, subscriber got %r" % (got,))
            assert got[0] == "ka/status", "Will arrived on the wrong topic %r" % got[0]
            assert got[1] == b"timed-out", "Will payload altered: %r" % got[1]
            return "client got DISCONNECT 0x8D; subscriber received the Will (ka/status = timed-out)"
        finally:
            sub.close()
            cli.close()
            stop(proc, workdir)
    finally:
        pass


def case_activity_resets_timer():
    """Control: a PINGREQ inside every Keep Alive window resets the timer, so the
    connection is neither closed nor its Will published, even though real time
    keeps passing well past 1.5x the nominal Keep Alive."""
    proc, port, workdir = start()
    stop_event = threading.Event()

    def keep_pinging(sock):
        # Send a PINGREQ every 0.5s against a Keep Alive of 1s: each lands well
        # inside the window and resets the 1.5x deadline.
        while not stop_event.is_set():
            try:
                sock.sendall(pingreq())
            except OSError:
                return
            if stop_event.wait(0.5):
                return

    try:
        cli = socket.create_connection((HOST, port), timeout=5)
        ping_thread = threading.Thread(target=keep_pinging, args=(cli,), daemon=True)
        try:
            cli.sendall(connect("ka-alive", 1, will_topic="ka-alive/status",
                                will_msg=b"must-not-appear"))
            assert read_packet(cli)[0] & 0xF0 == 0x20, "no CONNACK"
            ping_thread.start()

            # Keep exchanging PINGREQ/PINGRESP for ~3.5s -- more than three Keep
            # Alive windows -- to confirm the timer keeps resetting.
            deadline = time.time() + 3.5
            pongs = 0
            while time.time() < deadline:
                pkt = read_packet(cli, 1.0)
                if pkt is None:
                    break
                h, b = pkt
                assert h & 0xF0 == PINGRESP, (
                    "a Keep Alive client must only see PINGRESP, got 0x%02X" % h)
                pongs += 1
            assert pongs >= 3, "expected a stream of PINGRESP, got %d" % pongs

            # No DISCONNECT has arrived mid-stream: the connection survived.
            cli.setblocking(False)
            try:
                extra = cli.recv(100)
            except (BlockingIOError, socket.timeout):
                extra = b""
            except OSError:
                extra = b"<closed>"
            assert extra == b"", (
                "an active client must not be closed, but received %r" % extra)
            return "client stayed alive past 3 Keep Alive windows (%d PINGRESP, no DISCONNECT)" % pongs
        finally:
            stop_event.set()
            ping_thread.join(timeout=2)
            cli.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT-3.1.2-24: a client silent past 1.5x Keep Alive is closed with DISCONNECT 0x8D and its Will is published",
     case_timeout_closes_and_publishes_will)
case("MQTT-3.1.2-20: a PINGREQ inside every Keep Alive window resets the timer, keeping the connection open",
     case_activity_resets_timer)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
