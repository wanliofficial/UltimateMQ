#!/usr/bin/env python3
"""MQTT-SN: exhausting a peer's topic-id space must not hang the gateway.

MQTT-SN §5.3.9: a gateway assigns a two-byte Topic Id per registered Topic Name
(REGISTER -> REGACK). Peer::topicIdFor lazily allocates those ids by scanning for
a free one. The id space is 65535 values, but nothing bounded how many distinct
names one peer could register, so a device could REGISTER 65535 distinct topics
and then mint one more. At that point every id is taken, the "find a free id"
loop never finds one and spins forever. The gateway is driven from the same
single-threaded event loop as the MQTT broker (Gateway::handleReadable runs on
the broker poll loop), so that spin is an unbounded hang -- a whole-broker
denial of service from one unauthenticated UDP peer.

The test: connect an SN device, REGISTER distinct topic names up to and beyond
the 16-bit id space, then confirm the gateway is still responsive -- a final
REGISTER must get a REGACK (it may be a REJECTED_CONGESTION now that the id space
is full) rather than the gateway hanging. Before the fix the gateway stops
answering entirely.

To keep the probe quick it registers `TARGET` topics (>> enough to exhaust the
space if the id space were not bounded; a bounded gateway rejects far earlier).
Sending is done in one burst and the gateway's liveness is checked with a final
REGISTER that must be answered within a few seconds.

Usage: mqttsn_topicid_exhaustion_test.py <binary> <tcp-port>
Exit code is 0 only when the case passes.
"""
import os
import signal
import socket
import struct
import subprocess
import sys
import time

HOST = "127.0.0.1"
BIN = sys.argv[1]
TCP_PORT = [int(sys.argv[2])]
SN_PORT = [0]

T_REGISTER = 0x0A
T_REGACK = 0x0B

results = []


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port


def sn_encode(msg_type, body):
    one = len(body) + 2
    if one < 256 and one != 0x01:
        return bytes([one, msg_type]) + body
    three = len(body) + 4
    assert three <= 0xFFFF
    return bytes([0x01, (three >> 8) & 0xFF, three & 0xFF, msg_type]) + body


def sn_type(data):
    return data[3] if data[0] == 0x01 else data[1]


def sn_body(data):
    return data[4:] if data[0] == 0x01 else data[2:]


def sn_recv(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        data, _ = sock.recvfrom(2048)
    except socket.timeout:
        return None
    return data


def sn_connect(client_id, duration=60):
    return sn_encode(0x04, bytes([0x00, 0x01]) + struct.pack("!H", duration)
                     + client_id.encode())


def sn_register(topic, msg_id):
    return sn_encode(T_REGISTER, struct.pack("!H", 0) + struct.pack("!H", msg_id)
                     + topic.encode())


class Broker:
    def __init__(self, tcp_port, sn_port):
        self.tcp_port = tcp_port
        self.sn_port = sn_port
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env["MQTT_PORT"] = str(self.tcp_port)
        env["FLOWMQ_MQTT_SN_PORT"] = str(self.sn_port)
        self.proc = subprocess.Popen([BIN], env=env, cwd=os.path.dirname(BIN),
                                     stdout=subprocess.DEVNULL,
                                     stderr=subprocess.DEVNULL)
        deadline = time.time() + 5
        while time.time() < deadline:
            try:
                c = socket.create_connection((HOST, self.tcp_port), timeout=0.2)
                c.close()
                time.sleep(0.3)
                return
            except Exception:  # noqa: BLE001
                time.sleep(0.05)
        self.proc.kill()
        raise RuntimeError("server did not start")

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        time.sleep(0.3)


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


def case_topicid_exhaustion_does_not_hang():
    TCP_PORT[0] += 1
    SN_PORT[0] = free_udp_port()
    b = Broker(TCP_PORT[0], SN_PORT[0])
    b.start()
    try:
        sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sn.settimeout(3)
        dest = (HOST, SN_PORT[0])
        sn.sendto(sn_connect("sn-exhaust"), dest)
        assert sn_type(sn_recv(sn)) == 0x05, "no CONNACK to the SN device"

        # Register more distinct topic names than the 16-bit id space holds, in a
        # burst. A bounded gateway starts rejecting before this; an unbounded one
        # fills the id space and then spins in its find-a-free-id loop.
        nsent = 0
        for i in range(66000):
            sn.sendto(sn_register("x/%d" % i, (i % 60000) + 1), dest)
            nsent += 1
        # Drain whatever REGACKs are queued (best effort, non-blocking).
        sn.settimeout(0.2)
        try:
            while True:
                sn.recvfrom(2048)
        except socket.timeout:
            pass

        # Liveness: the gateway must still answer a fresh REGISTER within a few
        # seconds. If it is spinning in topicIdFor() it answers nothing.
        sn.settimeout(5)
        sn.sendto(sn_register("alive/check", 12345), dest)
        reply = sn_recv(sn, 5.0)
        assert reply is not None, (
            "gateway did not answer after %d REGISTERs: topic-id space "
            "exhaustion hung the broker event loop" % nsent)
        assert sn_type(reply) == T_REGACK, (
            "expected a REGACK, got msg type 0x%02x" % sn_type(reply))
        # With the id space full the gateway must signal congestion, not accept.
        body = sn_body(reply)
        rc = body[4]
        assert rc == 0x01, (
            "REGISTER past a full topic-id space returned 0x%02x, expected "
            "REJECTED_CONGESTION (0x01)" % rc)
        sn.close()
        return ("gateway answers a REGISTER after %d distinct topics with "
                "REJECTED_CONGESTION instead of hanging" % nsent)
    finally:
        b.stop()


case("MQTT-SN: exhausting a peer's topic-id space does not hang the gateway",
     case_topicid_exhaustion_does_not_hang)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
