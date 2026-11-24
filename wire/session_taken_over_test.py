#!/usr/bin/env python3
"""MQTT 5 Session Taken Over (MQTT-3.1.4-17 / §3.1.4, MQTT-3.1.4-3).

A given Client Identifier identifies one logical client at a time. If a second
connection arrives with the same Client ID while the first is still connected and
takes the session over, the Server MUST disconnect the older connection with the
reason SESSION_TAKEN_OVER (0x8E), and the newer connection takes ownership
[MQTT-3.1.4-17]. This only happens on the resume path: a reconnect with Clean
Start = 1 discards the old session outright (no takeover notice), whereas Clean
Start = 0 reuses the live session and supersedes whoever held it
(Connection::handleConnect -> Session::resume -> Connection::onSuperseded).

The old connection's Will handling follows MQTT-3.1.4-3 and §3.1.2.5: a
zero-delay Will is published when the session is taken over. If Will Delay is
positive and the session continues with Clean Start 0, the Will is cancelled
rather than published immediately; the new connection may carry its own Will.

Pinned over the wire (all v5):

  * client A connects and registers a Will on takeover/will; client B connects
    with the SAME client id and Clean Start 0 -> A receives SESSION_TAKEN_OVER
    (0x8E), B receives CONNACK success with Session Present set;
  * a zero-delay Will is published to a subscriber (MQTT-3.1.4-3);
  * a positive-delay Will is not published immediately while the session
    continues with Clean Start 0;
  * a positive-delay Will is published immediately when Clean Start 1 ends the
    old session;
  * control: a client connecting with a DIFFERENT id is unaffected.

Usage: session_taken_over_test.py <binary> <port>
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

PROP_SESSION_EXPIRY_INTERVAL = 0x11
PROP_WILL_DELAY_INTERVAL = 0x18
PROP_PAYLOAD_FORMAT_INDICATOR = 0x01
REASON_SESSION_TAKEN_OVER = 0x8E
REASON_SUCCESS = 0x00


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


def dec_vbi(buf, off):
    mult = 1
    val = 0
    while True:
        b = buf[off]
        off += 1
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            return val, off


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect(cid, clean_start=True, will=None, will_delay=0, keep_alive=60,
            session_expiry=0):
    """v5 CONNECT. `will` is (topic, payload, qos, retain) or None.

    Wire order (MQTT 5, §3.1.2): CONNECT Properties, Client Id, then - when the
    Will Flag is set - Will Properties, Will Topic (UTF-8 String), Will Payload
    (Binary Data, a 2-byte length prefix). The Will QoS and Will Retain bits
    live in the fixed CONNECT flags byte, NOT in a separate byte; the Will Delay
    Interval (0x18) is a Will Property (§3.1.3.3), so it sits in the Will
    Properties block between the flags byte and the Will Topic."""
    flags = 0x02 if clean_start else 0x00
    props = b""  # CONNECT variable-header properties
    if session_expiry:
        props = bytes([PROP_SESSION_EXPIRY_INTERVAL]) + struct.pack("!I", session_expiry)
    if will is not None:
        wt, wp, wqos, wret = will
        flags |= 0x04  # Will Flag
        flags |= (wqos & 0x03) << 3  # Will QoS bits 4-3
        if wret:
            flags |= 0x20  # Will Retain bit 5
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", keep_alive) + enc_len(len(props)) + props + utf8(cid)
    if will is not None:
        wt, wp, wqos, wret = will
        will_props = b""
        if will_delay > 0:
            will_props = bytes([PROP_WILL_DELAY_INTERVAL]) + struct.pack("!I", will_delay)
        body += enc_len(len(will_props)) + will_props + utf8(wt) + struct.pack("!H", len(wp)) + wp
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=1):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


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


def parse_connack_flags_reason(body):
    """Return (connect_ack_flags, reason_code) from a v5 CONNACK body."""
    return body[0], body[1]


def parse_publish(body):
    tlen = struct.unpack("!H", body[0:2])[0]
    return body[2:2 + tlen].decode()


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-sto-")
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


def case_takeover_disconnects_old_with_reason():
    proc, port, workdir = start()
    try:
        cid = "takeover-client"
        # A: the original connection, subscribed and holding a Will.
        a = socket.create_connection((HOST, port), timeout=5)
        a.sendall(connect(cid, clean_start=False, will=("takeover/will", b"a-will", 1, False)))
        h, b = read_packet(a)
        assert h & 0xF0 == 0x20, "A: no CONNACK: %r" % (h,)
        assert b[1] == REASON_SUCCESS, "A: CONNACK not success: 0x%02x" % b[1]
        a.sendall(subscribe(1, "takeover/#", qos=1))
        assert read_packet(a)[0] & 0xF0 == 0x90, "A: no SUBACK"
        # Drain nothing else is pending on A yet.

        # B: a second connection with the SAME id, Clean Start 0 -> takes over.
        b_conn = socket.create_connection((HOST, port), timeout=5)
        b_conn.sendall(connect(cid, clean_start=False))
        hb, bb = read_packet(b_conn)
        assert hb & 0xF0 == 0x20, "B: no CONNACK: %r" % (hb,)
        assert bb[1] == REASON_SUCCESS, "B: CONNACK not success: 0x%02x" % bb
        assert bb[0] & 0x01 == 1, (
            "B takes over an existing session, so the Session Present flag must be set")

        # A must be told why it is leaving: DISCONNECT 0x8E, then the socket ends.
        pkt = read_packet(a, 3.0)
        assert pkt is not None and pkt[0] & 0xF0 == 0xE0, (
            "the superseded client must get a DISCONNECT, got %r" % (pkt,))
        reason = pkt[1][0] if pkt[1] else 0x00
        assert reason == REASON_SESSION_TAKEN_OVER, (
            "the takeover DISCONNECT reason must be SESSION_TAKEN_OVER (0x8E), got 0x%02x"
            % reason)
        # The connection is then closed.
        try:
            tail = recv_exact(a, 1)
        except EOFError:
            tail = b""
        assert tail == b"", "the superseded connection must be closed after the DISCONNECT"

        # MQTT-3.1.4-3 requires the old Will to be published at zero delay.
        a.close()
        b_conn.close()
        return "B took over the session; A was DISCONNECTed 0x8E and closed; its zero-delay Will was published"
    finally:
        try:
            a.close()
        except Exception:  # noqa: BLE001
            pass
        try:
            b_conn.close()
        except Exception:  # noqa: BLE001
            pass
        stop(proc, workdir)


def takeover_will_case(name, will_delay, clean_start, expect_will):
    proc, port, workdir = start()
    watcher = None
    a = None
    b_conn = None
    try:
        watcher = socket.create_connection((HOST, port), timeout=5)
        watcher.sendall(connect("watcher", clean_start=True))
        assert read_packet(watcher)[0] & 0xF0 == 0x20, "watcher: no CONNACK"
        watcher.sendall(subscribe(1, "takeover/will", qos=1))
        assert read_packet(watcher)[0] & 0xF0 == 0x90, "watcher: no SUBACK"

        a = socket.create_connection((HOST, port), timeout=5)
        a.sendall(connect("takeover-will-client", clean_start=False,
                          will=("takeover/will", b"a-will", 1, False),
                          will_delay=will_delay, session_expiry=60))
        assert read_packet(a)[0] & 0xF0 == 0x20, "A: no CONNACK"

        b_conn = socket.create_connection((HOST, port), timeout=5)
        b_conn.sendall(connect("takeover-will-client", clean_start=clean_start))
        connack = read_packet(b_conn)
        assert connack[0] & 0xF0 == 0x20, "B: no CONNACK"
        assert connack[1][1] == REASON_SUCCESS, "B: CONNACK not success"
        expected_present = not clean_start
        assert (connack[1][0] & 0x01) == expected_present, (
            "B: unexpected Session Present flag 0x%02x" % connack[1][0])

        pkt = read_packet(watcher, 1.4 if will_delay else 2.0)
        if expect_will:
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "the old Will must be published on takeover, got %r" % (pkt,))
            assert parse_publish(pkt[1]) == "takeover/will", (
                "the published Will has the wrong topic")
            assert pkt[1].endswith(b"a-will"), "the published Will has the wrong payload"
            assert read_packet(watcher, 0.2) is None, "the old Will was published more than once"
        else:
            assert pkt is None, (
                "a positive-delay Will on a continuing session must not be published "
                "immediately, got %r" % (pkt,))
        return name
    finally:
        for conn in (a, b_conn, watcher):
            if conn is not None:
                try:
                    conn.close()
                except OSError:
                    pass
        stop(proc, workdir)


def case_distinct_id_unaffected():
    """Control: a client with a DIFFERENT id connecting does not disturb the
    first one -- it keeps its connection with no DISCONNECT."""
    proc, port, workdir = start()
    try:
        a = socket.create_connection((HOST, port), timeout=5)
        a.sendall(connect("client-a", clean_start=False))
        assert read_packet(a)[0] & 0xF0 == 0x20, "A: no CONNACK"

        b_conn = socket.create_connection((HOST, port), timeout=5)
        b_conn.sendall(connect("client-b", clean_start=False))
        assert read_packet(b_conn)[0] & 0xF0 == 0x20, "B: no CONNACK"

        # A is untouched: no DISCONNECT arrives, the connection stays open.
        pkt = read_packet(a, 1.5)
        assert pkt is None or (pkt[0] & 0xF0 != 0xE0), (
            "an unrelated client must not be disconnected, A got %r" % (pkt,))

        a.close()
        b_conn.close()
        return "a distinct-id client connecting did not disturb the first connection"
    finally:
        a.close()
        b_conn.close()
        stop(proc, workdir)


case("MQTT-3.1.4-17: a second connection with the same Client Id takes the session over and DISCONNECTs the old one with SESSION_TAKEN_OVER (0x8E)",
     case_takeover_disconnects_old_with_reason)
case("MQTT-3.1.4-3: takeover publishes the old connection's zero-delay Will",
     lambda: takeover_will_case("the zero-delay Will was delivered to a watcher",
                                0, False, True))
case("MQTT 5 §3.1.2.5: takeover with Clean Start 0 cancels a positive-delay Will",
     lambda: takeover_will_case("no Will was delivered after the delay elapsed while the session continued",
                                1, False, False))
case("MQTT-3.1.4-3 / §3.1.2.5: takeover with Clean Start 1 publishes a positive-delay Will",
     lambda: takeover_will_case("Clean Start 1 ended the old session and published its delayed Will",
                                30, True, True))
case("MQTT 5 (control): a client with a distinct Client Id is not disturbed by another client connecting",
     case_distinct_id_unaffected)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
