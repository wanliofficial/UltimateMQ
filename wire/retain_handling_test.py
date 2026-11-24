#!/usr/bin/env python3
"""MQTT §3.8.3.1 / §2.2.2.6: SUBSCRIBE Retain Handling controls retained delivery.

Each SUBSCRIBE carries Subscription Options whose bits 4-5 are the Retain
Handling setting [MQTT-3.8.3-1], telling the broker when (if ever) to send the
retained messages that match the new subscription:

  0 = SEND AT SUBSCRIBE TIME -- send retained messages when the subscription is
      established, whether or not it already existed;
  1 = SEND AT SUBSCRIBE TIME ONLY IF THE SUBSCRIPTION IS NEW -- send retained
      messages only the first time the filter is subscribed, not on a re-subscribe
      of an established subscription;
  2 = DO NOT SEND RETAINED MESSAGES AT SUBSCRIBE TIME -- never send retained
      messages as a result of this subscription.

retained_messages_test.py covers setting 0 (the default), but settings 1 and 2
and the new-vs-established distinction are a separate, equally normative piece
of behaviour that no wire probe drove. This pins all three, including the
new-vs-re-subscribe split for setting 1.

Usage: retain_handling_test.py <binary> <port>
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

RETAIN_HANDLING_AT_SUBSCRIBE = 0
RETAIN_HANDLING_IF_NEW = 1
RETAIN_HANDLING_DO_NOT_SEND = 2


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


def subscribe(pid, filt, qos=0, retain_handling=0):
    options = ((retain_handling & 0x03) << 4) | (qos & 0x03)
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([options])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish_retain(topic, payload):
    # QoS 0, RETAIN=1.
    body = utf8(topic) + enc_len(0) + payload
    return bytes([0x31]) + enc_len(len(body)) + body


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
    workdir = tempfile.mkdtemp(prefix="flowmq-rh-")
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


def _publish_retained_then_subscribe(workdir, port, filt, retain_handling):
    """Helper: publish a retained message on `filt`, then connect a subscriber
    that subscribes with the given retain handling. Returns (proc, sub, saw)."""
    pub = socket.create_connection((HOST, port), timeout=5)
    sub = socket.create_connection((HOST, port), timeout=5)
    pub.sendall(connect("rh-pub"))
    assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (publisher)"
    sub.sendall(connect("rh-sub"))
    assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (subscriber)"
    # Publish the retained message (no subscriber yet).
    pub.sendall(publish_retain(filt, b"snap"))
    # The publisher gets no ack at QoS 0; drain anything and discard.
    try:
        read_packet(pub, 0.5)
    except Exception:  # noqa: BLE001
        pass
    pub.close()
    # Now the subscriber establishes its subscription with the given handling.
    sub.sendall(subscribe(1, filt, qos=0, retain_handling=retain_handling))
    assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"
    return pub, sub


def case_handling_0_sends_retained():
    proc, port, workdir = start()
    try:
        pub, sub = _publish_retained_then_subscribe(
            workdir, port, "rh/always", RETAIN_HANDLING_AT_SUBSCRIBE)
        try:
            pkt = read_packet(sub, 1.5)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "retain handling 0 must send the retained message, got %r" % (pkt,))
            h, b = pkt
            tlen = struct.unpack("!H", b[0:2])[0]
            topic = b[2:2 + tlen].decode()
            assert topic == "rh/always", "retained delivered on wrong topic %r" % topic
            assert h & 0x01 == 1, "a retained snapshot must keep RETAIN=1"
            return "retain handling 0 sends the retained message with RETAIN=1"
        finally:
            sub.close()
            stop(proc, workdir)
    finally:
        pass


def case_handling_2_suppresses_retained():
    """Retain handling 2 (DO NOT SEND) suppresses the retained delivery entirely,
    even though a matching retained message exists."""
    proc, port, workdir = start()
    try:
        pub, sub = _publish_retained_then_subscribe(
            workdir, port, "rh/never", RETAIN_HANDLING_DO_NOT_SEND)
        try:
            pkt = read_packet(sub, 1.5)
            assert pkt is None, (
                "retain handling 2 must suppress the retained message, got %r" % (pkt,))
            return "retain handling 2 suppresses the retained message"
        finally:
            sub.close()
            stop(proc, workdir)
    finally:
        pass


def case_handling_1_new_then_restored():
    """Retain handling 1 sends the retained message on the FIRST (new) subscribe,
    but NOT on a re-subscribe of the now-established filter."""
    proc, port, workdir = start()
    try:
        pub, sub = _publish_retained_then_subscribe(
            workdir, port, "rh/ifnew", RETAIN_HANDLING_IF_NEW)
        try:
            # First subscribe: new subscription -> retained is sent.
            pkt = read_packet(sub, 1.5)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "retain handling 1 must send retained on a NEW subscription, got %r" % (pkt,))
            h, b = pkt
            tlen = struct.unpack("!H", b[0:2])[0]
            assert b[2:2 + tlen].decode() == "rh/ifnew", "wrong retained topic"
            assert h & 0x01 == 1, "new-subscription retained keeps RETAIN=1"

            # Re-subscribe the SAME filter on the SAME connection: the subscription
            # is no longer new, so retain handling 1 must send nothing.
            sub.sendall(subscribe(2, "rh/ifnew", qos=0, retain_handling=RETAIN_HANDLING_IF_NEW))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK for re-subscribe"
            again = read_packet(sub, 1.0)
            assert again is None, (
                "retain handling 1 must NOT resend retained on an established "
                "re-subscribe, got %r" % (again,))
            return "retain handling 1 sends retained on the new subscribe but not on re-subscribe"
        finally:
            sub.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT 3.8.3-1: Retain Handling 0 (SEND AT SUBSCRIBE) delivers the retained message with RETAIN=1",
     case_handling_0_sends_retained)
case("MQTT 3.8.3-1: Retain Handling 2 (DO NOT SEND) suppresses the retained message entirely",
     case_handling_2_suppresses_retained)
case("MQTT 3.8.3-1: Retain Handling 1 sends retained on a new subscription but not on a re-subscribe",
     case_handling_1_new_then_restored)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
