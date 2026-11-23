#!/usr/bin/env python3
"""MQTT 5 Will Delay cancelled by a reconnect (MQTT-3.1.4-6 / §3.1.3.2).

A client may set a Will Delay Interval in its CONNECT: if its connection ends
abnormally, the broker waits that long before publishing the Will, giving the
client a grace period to reconnect and "take back" what it was going to say. The
spec makes the reconnect explicit (MQTT-3.1.4-6): if the same client id
reconnects while the Will is still pending, the pending Will is cancelled and is
NEVER published.

The will-delay *publishing* path is covered in-process, but the cancellation
contract - the more security-relevant half - is not exercised over the wire, so
a regression that published the Will anyway (or on the next sweep) would silently
announce a device as gone when it is in fact back. This probe has two cases:

  1. cancellation: a subscriber watches the Will topic; the device connects with
     a Will Delay and a Will, drops with no DISCONNECT (arming the delayed Will),
     and reconnects with the same client id WELL INSIDE the delay window. The
     reconnected socket is held open past the original deadline, and the Will
     must never be published (MQTT-3.1.4-6).
  2. the control: with the same setup but NO reconnect, the Will DOES fire shortly
     after the delay. This proves case 1 is not a vacuous pass - the Will really
     was armed and would have gone out had the device not come back.

Usage: will_delay_cancel_test.py <binary> <port>
Exit code is 0 only when the case passes.
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

# MQTT 5 property IDs.
PROP_SESSION_EXPIRY_INTERVAL = 0x11
PROP_WILL_DELAY_INTERVAL = 0x18


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


def connect(cid, clean_start, session_expiry, will=None, will_delay=None):
    """CONNECT with optional Will and Will Delay Interval.

    Wire order (MQTT 5, §3.1.2): CONNECT Properties, Client ID, then - when the
    Will Flag is set - Will Properties, Will Topic, Will Message. The Will Delay
    Interval lives in the Will Properties block (§3.1.3.3), not the CONNECT
    Properties block; putting it in the wrong block is a Malformed Packet.
    """
    flags = 0x02 if clean_start else 0x00
    if will is not None:
        flags |= 0x04  # Will present
        if will.get("retain"):
            flags |= 0x20  # Will Retain
        # Will QoS bits 4-3 (0b11 = QoS 1 here).
        flags |= (will.get("qos", 1) & 0x03) << 3
    props = b""
    if session_expiry is not None:
        props += bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
    body = (utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) +
            enc_len(len(props)) + props)
    body += utf8(cid)
    if will is not None:
        will_props = b""
        if will_delay is not None:
            will_props += bytes([PROP_WILL_DELAY_INTERVAL]) + struct.pack("!I", will_delay)
        body += enc_len(len(will_props)) + will_props
        body += utf8(will["topic"])
        # The Will Payload is Binary Data (§1.5.6): a 2-byte length prefix, not a
        # VBI Remaining Length.
        body += struct.pack("!H", len(will["payload"])) + will["payload"]
    return bytes([0x10]) + enc_len(len(body)) + body


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
    body = recv_exact(sock, val) if val else b""
    return header, body


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


def start():
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-will-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
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
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))


def case_will_delay_cancelled_by_reconnect():
    proc, port, workdir = start()
    will_topic = "device/will"
    will_payload = b"offline"
    delay = 5
    sub = dev = dev2 = None
    try:
        # A subscriber on the Will topic must stay silent the whole time. It MUST
        # actually subscribe, or the case passes vacuously (nothing to receive).
        sub = socket.create_connection((HOST, port), timeout=5)
        sub.sendall(connect("will-sub", True, None))
        assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK for subscriber"
        sub_body = struct.pack("!H", 1) + enc_len(0) + utf8(will_topic) + bytes([1])
        sub.sendall(bytes([0x82]) + enc_len(len(sub_body)) + sub_body)
        assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK for subscriber"

        # 1-2: device connects WITH a Will and a delay, then drops abnormally.
        dev = socket.create_connection((HOST, port), timeout=5)
        dev.sendall(connect("device-1", True, 3600,
                            will={"topic": will_topic, "payload": will_payload,
                                  "qos": 1, "retain": False},
                            will_delay=delay))
        assert read_packet(dev)[0] & 0xF0 == 0x20, "no CONNACK for device"
        dev.close()  # abnormal: no DISCONNECT, so the delayed Will is armed
        armed_at = time.time()

        # 3: the same client reconnects WELL INSIDE the delay window. This is the
        #    operation under test: it must cancel the pending Will (MQTT-3.1.4-6).
        dev2 = socket.create_connection((HOST, port), timeout=5)
        dev2.sendall(connect("device-1", False, 3600,
                             will={"topic": will_topic, "payload": will_payload,
                                   "qos": 1, "retain": False},
                             will_delay=delay))
        header = read_packet(dev2)[0]
        assert header & 0xF0 == 0x20, "no CONNACK on reconnect"
        # The cancellation is only meaningful if the reconnect beat the deadline;
        # assert it so a future slowdown cannot silently make this case vacuous.
        elapsed = time.time() - armed_at
        assert elapsed < delay, (
            "the reconnect landed %.1fs after the drop, past the %ds Will Delay - "
            "the Will would already have fired, so this case proves nothing"
            % (elapsed, delay))

        # 4: hold the reconnected socket open and wait PAST the original deadline.
        #    The cancelled Will must never appear. (Keeping dev2 open matters: an
        #    abnormal close here would arm a *fresh* Will and mask the check.)
        late = read_packet(sub, timeout=delay + 2.5)
        assert late is None, (
            "the Will was published after the in-window reconnect cancelled it; "
            "got header 0x%02x" % (late[0],))
        return "a reconnect well inside the Will Delay window cancelled the pending Will"
    finally:
        for s in (sub, dev, dev2):
            try:
                if s is not None:
                    s.close()
            except Exception:  # noqa: BLE001
                pass
        stop(proc, workdir)


def case_will_delay_without_reconnect_actually_fires():
    """Companion to the cancellation case: with NO reconnect, the same delayed
    Will DOES fire after the delay. This proves the cancellation case above is
    real and not a vacuous pass - the Will would have been published had the
    device not come back before the deadline."""
    proc, port, workdir = start()
    will_topic = "device2/will"
    will_payload = b"offline2"
    dev = None
    watcher = None
    try:
        # Subscribe a watcher BEFORE the device drops, so the Will (a live QoS 1
        # publish) is delivered to it. A Will is not retained, so a subscriber
        # that arrives after the fact would miss it.
        watcher = socket.create_connection((HOST, port), timeout=5)
        watcher.sendall(connect("watcher-2", True, None))
        assert read_packet(watcher)[0] & 0xF0 == 0x20, "no CONNACK for watcher"
        sub_body = struct.pack("!H", 1) + enc_len(0) + utf8(will_topic) + bytes([1])
        watcher.sendall(bytes([0x82]) + enc_len(len(sub_body)) + sub_body)
        assert read_packet(watcher)[0] & 0xF0 == 0x90, "no SUBACK for watcher"

        dev = socket.create_connection((HOST, port), timeout=5)
        dev.sendall(connect("device-2", True, 3600,
                            will={"topic": will_topic, "payload": will_payload,
                                  "qos": 1, "retain": False},
                            will_delay=2))
        assert read_packet(dev)[0] & 0xF0 == 0x20, "no CONNACK for device"
        dev.close()  # abnormal drop, and NO reconnect this time

        # The Will must be published shortly after the 2s delay elapses.
        header, body = read_packet(watcher, timeout=5.0)
        assert header is not None and header & 0xF0 == 0x30, (
            "the delayed Will did not fire after the delay elapsed with no "
            "reconnect; got header %r" % header)
        tlen = struct.unpack("!H", body[0:2])[0]
        topic = body[2:2 + tlen].decode()
        off = 2 + tlen + 2  # topic + packet id (QoS 1)
        plen = 0
        mult = 1
        while True:
            b = body[off]
            off += 1
            plen += (b & 127) * mult
            mult *= 128
            if not b & 128:
                break
        payload = body[off + plen:]
        assert (topic, payload) == (will_topic, will_payload), (
            "the fired Will was %r/%r" % (topic, payload))
        return "without a reconnect the delayed Will fires after the delay"
    finally:
        try:
            if dev is not None:
                dev.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            if watcher is not None:
                watcher.close()
        except Exception:  # noqa: BLE001
            pass
        stop(proc, workdir)


case("will delay: a reconnect within the delay window cancels the pending Will (MQTT-3.1.4-6)",
     case_will_delay_cancelled_by_reconnect)
case("will delay: without a reconnect the same delayed Will DOES fire after the delay",
     case_will_delay_without_reconnect_actually_fires)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
