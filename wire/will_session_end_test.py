#!/usr/bin/env python3
"""An armed Will fires when the Session ends, not after its full delay.

MQTT 5 §3.1.2.11.3: "The Server delays publishing the Client's Will Message
until the Will Delay Interval has passed or the Session ends, whichever happens
first." A client can therefore arm a long Will Delay at CONNECT and end its
session at DISCONNECT time by sending Session Expiry Interval = 0; the Will must
then be published at once, because the Session has ended.

The broker decided whether to delay the Will from the CONNECT Session Expiry
Interval alone (session->isPersistent(), which is still the CONNECT value at that
point) and armed it in pendingWills. releaseSession() then honoured the
DISCONNECT override and discarded the session - but discarding a session never
touched the pending Will, and the sweeper that expires sessions explicitly
forces any armed Will due ('the Session ends when its state is destroyed, so the
Will goes out now'). So the Will of a session that had ALREADY ended sat in
pendingWills until its full delay elapsed.

This probe:

  1. watches the Will topic;
  2. connects with Will Delay = 6s and Session Expiry = 3600, then sends
     DISCONNECT (0x04, Disconnect with Will Message) carrying Session Expiry
     Interval = 0 - i.e. the session ends now and the Will must go out now;
  3. asserts the Will arrives promptly (well under the 6s delay);
  4. as a control, repeats with Session Expiry = 3600 (session survives) and
     asserts the SAME Will Delay really is honoured - so case 3 proves the
     early publication came from the session ending, not from a broker that
     ignores Will Delay altogether.

Usage: will_session_end_test.py <binary> <port>
Exit code is 0 only when both cases pass.
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

PROP_SESSION_EXPIRY_INTERVAL = 0x11
PROP_WILL_DELAY_INTERVAL = 0x18

DELAY = 6
PROMPT_LIMIT = 3.0  # must arrive well inside DELAY


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


def connect(cid, session_expiry, will, will_delay):
    flags = 0x02 | 0x04  # Clean Start + Will Flag
    flags |= (will.get("qos", 1) & 0x03) << 3
    if will.get("retain"):
        flags |= 0x20
    props = b""
    if session_expiry is not None:
        props += bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(len(props)) + props
    body += utf8(cid)
    will_props = bytes([PROP_WILL_DELAY_INTERVAL]) + struct.pack("!I", will_delay)
    body += enc_len(len(will_props)) + will_props
    body += utf8(will["topic"])
    # Will Payload is Binary Data (§1.5.6): a 2-byte length prefix.
    body += struct.pack("!H", len(will["payload"])) + will["payload"]
    return bytes([0x10]) + enc_len(len(body)) + body


def disconnect_with_will(session_expiry):
    """DISCONNECT, Reason Code 0x04 (Disconnect with Will Message), with a
    Session Expiry Interval property (§3.14.2.2)."""
    props = bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
    body = bytes([0x04]) + enc_len(len(props)) + props
    return bytes([0xE0]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=1):
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
    val = 0
    mult = 1
    while True:
        try:
            b = recv_exact(sock, 1)[0]
        except (socket.timeout, EOFError):
            return None
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    body = recv_exact(sock, val) if val else b""
    return header, body


def read_ack(sock, expected_type, timeout=3.0):
    deadline = time.time() + timeout
    while time.time() < deadline:
        packet = read_packet(sock, timeout=max(0.05, deadline - time.time()))
        if packet is None:
            return None
        if packet[0] & 0xF0 == expected_type:
            return packet
    return None


def parse_publish(header, body):
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    if (header >> 1) & 0x03:
        off += 2
    plen = 0
    mult = 1
    while True:
        b = body[off]
        off += 1
        plen += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    return topic, body[off + plen:]


def start():
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-willsession-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            c = socket.create_connection((HOST, port), timeout=0.2)
            c.close()
            time.sleep(0.4)
            return proc, port
        except Exception:  # noqa: BLE001
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start")


def stop(proc):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    time.sleep(0.5)


def run(disc_expiry, will_topic):
    """Watch will_topic, arm a DELAY-second Will, end the connection with
    DISCONNECT 0x04 carrying `disc_expiry`, and report what arrived.

    Returns (prompt, later, elapsed_at_prompt): `prompt` is the packet seen
    within PROMPT_LIMIT (well inside DELAY) or None; `later` is the packet seen
    while waiting the remainder of the delay plus a margin."""
    proc, port = start()
    watcher = device = None
    try:
        watcher = socket.create_connection((HOST, port), timeout=5)
        watcher.sendall(connect("watcher-" + will_topic.replace("/", "-"), 0,
                                {"topic": "unused/will", "payload": b"", "qos": 1},
                                0))
        assert read_ack(watcher, 0x20) is not None, "no CONNACK for watcher"
        watcher.sendall(subscribe(1, will_topic))
        assert read_ack(watcher, 0x90) is not None, "no SUBACK for watcher"

        device = socket.create_connection((HOST, port), timeout=5)
        device.sendall(connect("device-" + will_topic.replace("/", "-"), 3600,
                               {"topic": will_topic, "payload": b"bye", "qos": 1},
                               DELAY))
        assert read_ack(device, 0x20) is not None, "no CONNACK for device"
        armed_at = time.time()
        device.sendall(disconnect_with_will(disc_expiry))
        device.close()

        prompt = read_packet(watcher, timeout=PROMPT_LIMIT)
        elapsed = time.time() - armed_at
        later = None
        if prompt is None:
            later = read_packet(watcher, timeout=(DELAY - PROMPT_LIMIT) + 1.5)
        return prompt, later, elapsed
    finally:
        for s in (watcher, device):
            try:
                if s is not None:
                    s.close()
            except Exception:  # noqa: BLE001
                pass
        stop(proc)


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
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))


def case_session_end_publishes_will_promptly():
    topic = "will/session-end"
    prompt, later, elapsed = run(disc_expiry=0, will_topic=topic)
    assert prompt is not None, (
        "the Will of an ENDED session was not published within %.1fs: it is "
        "waiting out the full %ds Will Delay even though the Session already "
        "ended (§3.1.2.11.3)" % (PROMPT_LIMIT, DELAY))
    header, body = prompt
    assert header & 0xF0 == 0x30, "expected a PUBLISH, got header 0x%02x" % header
    got_topic, payload = parse_publish(header, body)
    assert (got_topic, payload) == (topic, b"bye"), "%r/%r" % (got_topic, payload)
    return "Will published %.2fs after the session ended, not after the %ds delay" % (
        elapsed, DELAY)


def case_live_session_honours_the_delay():
    topic = "will/session-live"
    prompt, later, elapsed = run(disc_expiry=3600, will_topic=topic)
    assert prompt is None, (
        "the Will was published within %.1fs of DISCONNECT even though the Session "
        "survives (Session Expiry 3600): the %ds Will Delay must still be honoured"
        % (PROMPT_LIMIT, DELAY))
    assert later is not None and later[0] & 0xF0 == 0x30, (
        "with a surviving session the delayed Will never fired at all (%r): the "
        "control only means something if the Will does eventually arrive" % (later,))
    return "with a surviving session nothing is published early and the %ds delay is honoured" % DELAY


case("Will Delay: ending the Session at DISCONNECT publishes the armed Will at once (§3.1.2.11.3)",
     case_session_end_publishes_will_promptly)
case("Will Delay: with the Session still alive the same delay is honoured (control)",
     case_live_session_honours_the_delay)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
