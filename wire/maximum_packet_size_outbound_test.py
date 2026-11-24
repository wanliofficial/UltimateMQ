#!/usr/bin/env python3
"""MQTT-3.1.2-24 / §3.1.2.11.4: the server must not exceed a client's Maximum
Packet Size on the way out.

A client may declare a Maximum Packet Size (property 0x27, a four-byte integer) in
its CONNECT, capping the total size of any packet it is willing to receive
[§3.1.2.11.4]. The server MUST NOT send a packet larger than that [MQTT-3.1.2-24]:
doing so would make a conforming client close the connection, losing every later
delivery too. So a message the broker cannot fit into the client's budget must be
dropped for that subscriber rather than truncated or forced through.

connect_zero_property_test.py covers refusing a Maximum Packet Size of 0; the
outbound enforcement was unguarded. Connection::sendPacket drops a packet whose
serialized size exceeds clientMaxPacketSize (and reports failure so a QoS>0
delivery releases its inflight slot). Pinned here on the live path:

  * a subscriber that declares Maximum Packet Size 50 is NOT sent a large PUBLISH
    (a 200-byte payload), while it IS still sent a small one on the same
    connection -- so the drop is size-specific and the connection stays healthy,
    not torn down;
  * control: a subscriber that declares no Maximum Packet Size receives the same
    large PUBLISH intact.

Usage: maximum_packet_size_outbound_test.py <binary> <port>
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

PROP_MAXIMUM_PACKET_SIZE = 0x27


def vbi(value):
    out = b""
    while True:
        d = value % 128
        value //= 128
        if value:
            d |= 0x80
        out += bytes([d])
        if not value:
            return out


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect(cid, max_packet_size=None):
    props = b""
    if max_packet_size is not None:
        props = bytes([PROP_MAXIMUM_PACKET_SIZE]) + struct.pack("!I", max_packet_size)
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + vbi(len(props)) + props + utf8(cid)
    return bytes([0x10]) + vbi(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + vbi(0) + utf8(filt) + bytes([qos & 0x03])
    return bytes([0x82]) + vbi(len(body)) + body


def publish_qos0(topic, payload):
    body = utf8(topic) + b"\x00" + payload
    return bytes([0x30]) + vbi(len(body)) + body


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


def parse_publish_payload(body):
    tlen = struct.unpack("!H", body[0:2])[0]
    off = 2 + tlen
    plen = 0
    mult = 1
    while True:
        x = body[off]
        off += 1
        plen += (x & 127) * mult
        mult *= 128
        if not x & 128:
            break
    return body[0:0] + body[off + plen:]


def start():
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-mps-")
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


def case_oversized_dropped_small_delivered():
    """A subscriber capped at 50 bytes: a 200-byte PUBLISH is dropped, but a small
    PUBLISH on the same connection still arrives -- the drop is size-specific."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("mps-sub", max_packet_size=50))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            pub.sendall(connect("mps-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            sub.sendall(subscribe(1, "mps/#", qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            big = b"B" * 200
            pub.sendall(publish_qos0("mps/big", big))
            time.sleep(0.2)
            pub.sendall(publish_qos0("mps/small", b"hi"))
            time.sleep(0.2)

            # The only PUBLISH that may arrive is the small one; the oversized copy
            # must have been dropped, not truncated and not forced through.
            pkt = read_packet(sub, 1.5)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "expected the small PUBLISH to arrive, got %r" % (pkt,))
            payload = parse_publish_payload(pkt[1])
            assert payload == b"hi", "expected the small payload, got %r" % payload
            # Nothing else must follow (the big one was suppressed).
            extra = read_packet(sub, 1.0)
            assert extra is None, (
                "an oversized PUBLISH must be dropped for a 50-byte client, got %r" % (extra,))
            return "the 200-byte PUBLISH was dropped; the small one arrived, connection intact"
        finally:
            sub.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


def case_no_cap_receives_large():
    """Control: a subscriber with no Maximum Packet Size receives the same large
    PUBLISH intact, so the drop above is about the declared cap."""
    proc, port, workdir = start()
    try:
        sub = socket.create_connection((HOST, port), timeout=5)
        pub = socket.create_connection((HOST, port), timeout=5)
        try:
            sub.sendall(connect("mps-nocap-sub"))
            assert read_packet(sub)[0] & 0xF0 == 0x20, "no CONNACK (sub)"
            pub.sendall(connect("mps-nocap-pub"))
            assert read_packet(pub)[0] & 0xF0 == 0x20, "no CONNACK (pub)"
            sub.sendall(subscribe(1, "mps/large", qos=0))
            assert read_packet(sub)[0] & 0xF0 == 0x90, "no SUBACK"

            big = bytes(range(256)) * 2  # 512 bytes of varied content
            pub.sendall(publish_qos0("mps/large", big))
            pkt = read_packet(sub, 2.0)
            assert pkt is not None and pkt[0] & 0xF0 == 0x30, (
                "a subscriber with no cap must receive the large PUBLISH, got %r" % (pkt,))
            payload = parse_publish_payload(pkt[1])
            assert payload == big, "the large payload was altered (len %d vs %d)" % (
                len(payload), len(big))
            return "a subscriber with no Maximum Packet Size received the 512-byte PUBLISH intact"
        finally:
            sub.close()
            pub.close()
            stop(proc, workdir)
    finally:
        pass


case("MQTT-3.1.2-24: a PUBLISH larger than the client's Maximum Packet Size is dropped, while a small one still arrives",
     case_oversized_dropped_small_delivered)
case("MQTT 5 (control): a subscriber with no Maximum Packet Size receives the same large PUBLISH intact",
     case_no_cap_receives_large)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
