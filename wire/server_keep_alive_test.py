#!/usr/bin/env python3
"""MQTT 5 Server Keep Alive override (§3.1.2.10).

A Client sends its desired Keep Alive in the CONNECT. The Server may impose its
own lower limit and, when it does, MUST tell the client the value it actually
enforced -- otherwise the client keeps pinging on its own (longer) schedule and
the two sides disagree about when the connection is stale [§3.1.2.10]:

  "If the Server does not accept the Keep Alive value sent by the Client, it
   MUST send a CONNACK ... containing a Server Keep Alive property. The Server
   MUST use this value when determining when to disconnect the Client."

In MQTT 5 the CONNACK carries no Keep Alive field in its fixed variable header,
so the override is expressed solely as the Server Keep Alive property (0x13, a
two-byte integer). The broker honours FLOWMQ_SERVER_KEEP_ALIVE: when it is set
and is lower than the client's request, it arms its keep-alive timer at that
value and advertises it via 0x13.

Pinned here over the wire:

  * with FLOWMQ_SERVER_KEEP_ALIVE = S and a client requesting a larger Keep
    Alive, the CONNACK carries a Server Keep Alive property whose value is S;
  * control: with FLOWMQ_SERVER_KEEP_ALIVE unset, the same client request yields
    a CONNACK with NO Server Keep Alive property (the server accepted the
    client's value, so there is nothing to override).

The timer-enforcement half (the broker disconnects a silent client at the
advertised value) is covered by keepalive_timeout_test.py; this probe pins the
advertisement half -- that the override is actually communicated.

Usage: server_keep_alive_test.py <binary> <tcp-port>
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

PROP_SERVER_KEEP_ALIVE = 0x13


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


def connect(cid, keep_alive):
    # MQTT 5, Clean Start, no properties. Keep Alive is the 2-byte field at
    # offset 6-7 of the CONNECT fixed variable header.
    body = utf8("MQTT") + bytes([5, 0x02]) + struct.pack("!H", keep_alive) + b"\x00" + utf8(cid)
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


def parse_server_keep_alive(body):
    """Return the Server Keep Alive (0x13) value from a v5 CONNACK body, or None
    if the property is absent. Walks the property block by known fixed widths."""
    # body[0] = Connect Acknowledge Flags, body[1] = Reason Code, body[2:] = props.
    plen, off = _dec_vbi(body, 2)
    pend = off + plen
    fixed = {
        0x11: 4,  # Session Expiry Interval
        0x21: 2,  # Receive Maximum
        0x22: 2,  # Topic Alias Maximum
        0x24: 1,  # Maximum QoS
        0x25: 1,  # Retain Available
        0x27: 4,  # Maximum Packet Size
        0x28: 1,  # Wildcard Subscription Available
        0x29: 1,  # Subscription Identifier Available
        0x2A: 1,  # Shared Subscription Available
    }
    while off < pend:
        pid = body[off]
        off += 1
        if pid == PROP_SERVER_KEEP_ALIVE:
            return struct.unpack("!H", body[off:off + 2])[0]
        if pid in fixed:
            off += fixed[pid]
        else:
            slen = struct.unpack("!H", body[off:off + 2])[0]
            off += 2 + slen
    return None


def _dec_vbi(buf, off):
    mult = 1
    val = 0
    while True:
        b = buf[off]
        off += 1
        val += (b & 127) * mult
        mult *= 128
        if not b & 128:
            return val, off


def start(server_keep_alive=None):
    PORT[0] += 1
    mqtt_port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-ska-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    if server_keep_alive is not None:
        env["FLOWMQ_SERVER_KEEP_ALIVE"] = str(server_keep_alive)
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


def case_override_advertised():
    """FLOWMQ_SERVER_KEEP_ALIVE = 30, client requests 60: the CONNACK must carry
    Server Keep Alive = 30."""
    server_value = 30
    proc, port, workdir = start(server_keep_alive=server_value)
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("ska-override", keep_alive=60))
            pkt = read_packet(c)
            assert pkt is not None, "no CONNACK"
            header, body = pkt
            assert header & 0xF0 == 0x20, "expected CONNACK, got 0x%02x" % header
            assert body[1] == 0x00, "override CONNACK not success: 0x%02x" % body[1]
            ska = parse_server_keep_alive(body)
            assert ska is not None, (
                "the CONNACK must carry the Server Keep Alive property when the "
                "server overrides the client's value (§3.1.2.10)")
            assert ska == server_value, (
                "Server Keep Alive must be the enforced %d, got %d" % (server_value, ska))
            return "override advertised: Server Keep Alive = %d s" % ska
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_override_lower_than_request():
    """The override only fires when the server value is LOWER than the client's
    request. Here the client asks for 10 (below the server's 30): no override,
    so no Server Keep Alive property is sent -- the client's tighter value wins."""
    proc, port, workdir = start(server_keep_alive=30)
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("ska-client-tighter", keep_alive=10))
            pkt = read_packet(c)
            assert pkt is not None, "no CONNACK"
            header, body = pkt
            assert header & 0xF0 == 0x20, "expected CONNACK, got 0x%02x" % header
            ska = parse_server_keep_alive(body)
            assert ska is None, (
                "with the client's Keep Alive below the server limit there is no "
                "override, so the Server Keep Alive property must be absent, got %r"
                % (ska,))
            return "client asked 10 (< server 30): no override, no Server Keep Alive property"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_unset_no_override():
    """Control: with FLOWMQ_SERVER_KEEP_ALIVE unset the server accepts the
    client's Keep Alive, so no Server Keep Alive property is sent."""
    proc, port, workdir = start(server_keep_alive=None)
    try:
        c = socket.create_connection((HOST, port), timeout=5)
        try:
            c.sendall(connect("ska-unset", keep_alive=60))
            pkt = read_packet(c)
            assert pkt is not None, "no CONNACK"
            header, body = pkt
            assert header & 0xF0 == 0x20, "expected CONNACK, got 0x%02x" % header
            ska = parse_server_keep_alive(body)
            assert ska is None, (
                "with no server limit configured there is no override, so the "
                "Server Keep Alive property must be absent, got %r" % (ska,))
            return "no FLOWMQ_SERVER_KEEP_ALIVE: client's 60 s accepted, no property"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


case("MQTT-3.1.2.10: a Server Keep Alive override is advertised as the Server Keep Alive property in the CONNACK",
     case_override_advertised)
case("MQTT-3.1.2.10: no override property when the client's Keep Alive is already below the server limit",
     case_override_lower_than_request)
case("MQTT 5 (control): with no server limit configured, the client's Keep Alive is accepted with no Server Keep Alive property",
     case_unset_no_override)

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
