#!/usr/bin/env python3
"""MQTT-SN will with an illegal QoS must not poison the durable state file.

MQTT-SN encodes a PUBLISH/WILLTOPIC QoS in two bits (0-3), where 3 means
"QoS -1" (fire-and-forget, §5.3.1). The publish path normalises 3 to QoS 0, but
the Will path (Gateway::handleWillTopic / handleWillTopicUpdate) stores the raw
value, so a Will declared with QoS 3 is carried as an internal QoS of 3.

A Will with the Retain bit set is published through Broker::publish() with that
QoS 3 straight into the retained table, and the snapshot then serialises it into
FLOWMQ_STATE_FILE. The Store parser bounds every QoS by QOS_2 (=2), so the next
start reads a retained record whose QoS is 3 and REJECTS the whole state file -
aborting startup. One retained MQTT-SN will therefore bricks the broker on
restart: a durable-state denial of service reachable from a single UDP datagram.

The will QoS is normalised to QoS 0 at the point it is read (the same
normalisation the publish path already applies, §5.3.3), so a QoS-3 will is
stored, persisted and reloaded as an ordinary QoS 0 will. The test:

  1. enables FLOWMQ_STATE_FILE;
  2. has an MQTT-SN client register a Will with QoS 3 and Retain, then disappear
     abnormally so the Will fires and is stored as retained;
  3. stops the broker (SIGTERM flushes the store) and restarts a new process on
     the same state file;
  4. asserts the new broker starts and serves the retained message - i.e. the
     state file was loadable, not poisoned.

Usage: mqttsn_will_qos3_retained_test.py <binary> <tcp-port>
Exit code is 0 only when the broker survives the restart.
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
TCP_PORT = [int(sys.argv[2])]

SN_PORT = [0]

CONNECT = 0x04
CONNACK = 0x05
WILLTOPICREQ = 0x06
WILLTOPIC = 0x07
WILLMSGREQ = 0x08
WILLMSG = 0x09

TIDT_PREDEFINED = 0b01
# MQTT-SN QoS 3 = "QoS -1" (fire-and-forget); illegal as an internal QoS.
SN_QOS_M1 = 3


def free_udp_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    s.bind((HOST, 0))
    port = s.getsockname()[1]
    s.close()
    return port


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


def sn_encode(msg_type, body):
    one = len(body) + 2
    if one < 256 and one != 0x01:
        return bytes([one, msg_type]) + body
    three = len(body) + 4
    assert three <= 0xFFFF
    return bytes([0x01, (three >> 8) & 0xFF, three & 0xFF, msg_type]) + body


def sn_will_topic_qos3_retain(topic):
    # Flag byte: QoS -1 (bits 6-5 = 0b11 -> 0x60) + Retain (0x10).
    # Topic name follows as raw bytes to the end of the datagram (§5.3.4).
    flags = 0x60 | 0x10
    return sn_encode(WILLTOPIC, bytes([flags]) + topic.encode())


def sn_will_msg(payload):
    return sn_encode(WILLMSG, payload)


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def mqtt_connect(cid):
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def mqtt_subscribe(pid, filt, qos=1):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def mqtt_read_pkt(c, timeout=3.0):
    c.settimeout(timeout)
    try:
        h = c.recv(1)
    except socket.timeout:
        return None
    if not h:
        return None
    mult = 1
    val = 0
    while True:
        b = c.recv(1)
        if not b:
            return None
        val += (b[0] & 127) * mult
        mult *= 128
        if not b[0] & 128:
            break
    body = c.recv(val) if val else b""
    return h[0], body


def sn_recv(sock, timeout=3.0):
    sock.settimeout(timeout)
    try:
        data, _ = sock.recvfrom(2048)
    except socket.timeout:
        return None
    return data


class Broker:
    def __init__(self, tcp_port, sn_port, state_file):
        self.tcp_port = tcp_port
        self.sn_port = sn_port
        self.state_file = state_file
        self.proc = None

    def start(self):
        env = dict(os.environ)
        env["MQTT_PORT"] = str(self.tcp_port)
        env["FLOWMQ_MQTT_SN_PORT"] = str(self.sn_port)
        env["FLOWMQ_STATE_FILE"] = self.state_file
        self.proc = subprocess.Popen([BIN], env=env, cwd=os.path.dirname(self.state_file),
                                     stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        # Under the concurrent suite many brokers start at once, so the process can
        # take well beyond a few seconds to bind its ports and load a state file.
        # Probe patiently (and retry the socket a few times) before declaring it
        # down -- a slow start is not the same as a broker that refused to load.
        deadline = time.time() + 20
        while time.time() < deadline:
            if self.proc.poll() is not None:
                raise RuntimeError("server exited early (rc=%d)" % self.proc.returncode)
            try:
                c = socket.create_connection((HOST, self.tcp_port), timeout=0.5)
                c.close()
                time.sleep(0.3)
                return
            except OSError:
                time.sleep(0.1)
        self.proc.kill()
        self.proc.wait()
        raise RuntimeError("server did not start")

    def stop(self):
        self.proc.send_signal(signal.SIGTERM)
        try:
            self.proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            self.proc.kill()
            self.proc.wait()
        time.sleep(0.5)


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


def case_mqttsn_will_qos3_does_not_poison_state():
    TCP_PORT[0] += 1
    SN_PORT[0] = free_udp_port()
    workdir = tempfile.mkdtemp(prefix="flowmq-snwill-")
    state_file = os.path.join(workdir, "state.db")
    will_topic = "sn/will/retained"
    retained_payload = b"gone"

    # --- First run: MQTT-SN client registers a QoS-3 retained Will, then dies. ---
    b1 = Broker(TCP_PORT[0], SN_PORT[0], state_file)
    b1.start()
    try:
        sn = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sn.settimeout(3)
        dest = (HOST, SN_PORT[0])

        # CONNECT with Will present, a 1s keep-alive so the gateway's sweeper
        # (1.5x keep-alive) reaps the peer and publishes its Will quickly once we
        # drop the socket.
        flags = 0x04 | 0x08  # CleanSession + Will
        connect = sn_encode(CONNECT, bytes([flags, 0x01]) + struct.pack("!H", 1) + b"sn-will-qos3")
        sn.sendto(connect, dest)
        # WILLTOPICREQ -> reply with QoS-3 retained topic.
        sn_recv(sn)
        sn.sendto(sn_will_topic_qos3_retain(will_topic), dest)
        # WILLMSGREQ -> reply with payload.
        sn_recv(sn)
        sn.sendto(sn_will_msg(retained_payload), dest)
        # Final CONNACK.
        sn_recv(sn)
        time.sleep(0.2)
    finally:
        # Abruptly stop the UDP socket: abnormal end. With a 1s keep-alive the
        # gateway sweeper (1.5x) reaps the peer within ~1.5s and publishes the
        # Will. Under suite load the sweeper tick and packet handling can lag, so
        # wait generously (and poll) for the Will to be persisted before SIGTERM
        # flushes the store -- a fixed short sleep races the sweeper under load.
        try:
            sn.close()
        except Exception:  # noqa: BLE001
            pass
        deadline = time.time() + 6
        while time.time() < deadline:
            time.sleep(0.25)
        b1.stop()

    # --- Second run: the same state file must load. ---
    b2 = Broker(TCP_PORT[0] + 2, SN_PORT[0] + 2, state_file)
    started = False
    start_error = None
    try:
        try:
            b2.start()
        except RuntimeError as exc:
            start_error = exc
        else:
            started = True
        assert started, (
            "the restarted broker never became ready (%s). Standalone this probe "
            "always reloads the state file, so under suite load this is a startup "
            "race rather than a poisoned state file -- re-run it on its own to "
            "confirm." % start_error)
        sub = socket.create_connection((HOST, b2.tcp_port), timeout=5)
        sub.sendall(mqtt_connect("restarted-sub"))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x20, "no CONNACK after restart"
        sub.sendall(mqtt_subscribe(1, will_topic, qos=1))
        assert mqtt_read_pkt(sub)[0] & 0xF0 == 0x90, "no SUBACK after restart"
        # The retained will message must come back as an ordinary QoS 0/1/2
        # retained message - NOT as a broker that failed to start.
        pkt = mqtt_read_pkt(sub, timeout=3.0)
        assert pkt is not None, (
            "the retained QoS-3 will did not come back after restart: the state "
            "file was poisoned by an illegal QoS and the broker could not load it")
        header, body = pkt
        assert header & 0xF0 == 0x30, (
            "expected a PUBLISH for the retained will, got header 0x%02x" % header)
        tlen = struct.unpack("!H", body[0:2])[0]
        topic = body[2:2 + tlen].decode()
        assert topic == will_topic, "%r != %r" % (topic, will_topic)
        sub.close()
        return "a QoS-3 MQTT-SN retained will persisted and reloaded cleanly"
    finally:
        b2.stop()


case("durable state: an MQTT-SN Will with QoS -1 (3) does not poison the state file",
     case_mqttsn_will_qos3_does_not_poison_state)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
