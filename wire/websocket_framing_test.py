#!/usr/bin/env python3
"""WebSocket framing guards: the per-message byte cap is enforced BEFORE the
payload is buffered, and a fragmented control frame is a protocol error.

Two broker fixes are pinned down here, both in WebSocketTransport::nextFrame:

1. Cap-before-buffer (the MAX_MESSAGE_BYTES guard). The broker assembles one
   WebSocket message at a time, so a frame may declare a payload far larger than
   the cap. A correct broker rejects the declared length as soon as it is read
   (CLOSE 1009, message too big); a broker that buffers the whole advertised
   payload first keeps appending until the cap is checked, so a peer that
   declares a huge length and then dribbles the bytes pins broker memory. The
   probe sends an oversized frame header and NONE of its payload: the fixed
   broker answers CLOSE 1009 immediately, the old one blocks waiting for bytes
   that never arrive.

2. No fragmented control frames (RFC 6455 §5.5.1). CLOSE (0x08), PING (0x09) and
   PONG (0x0A) must not be split across frames. A CLOSE control frame with the
   FIN bit clear is a protocol error and must be rejected with CLOSE 1002, not
   honoured as an orderly close (CLOSE 1000).

This is an end-to-end wire test: it launches the real broker binary with its
WebSocket listener enabled and drives it over TCP, so it complements the in-process
gtest suite.

Usage: websocket_framing_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
import base64
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

# Must match MAX_MESSAGE_BYTES in src/WebSocket.cpp.
MAX_MESSAGE_BYTES = 16 * 1024 * 1024

# WebSocket opcodes (RFC 6455 §5.2).
OP_TEXT = 0x1
OP_BINARY = 0x2
OP_CLOSE = 0x8
OP_PING = 0x9
OP_PONG = 0xA

# Close status codes (RFC 6455 §7.4).
CLOSE_PROTOCOL_ERROR = 1002
CLOSE_MESSAGE_TOO_BIG = 1009


def free_port():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
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


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect(cid, clean_start=True, props=b""):
    flags = 0x02 if clean_start else 0x00
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(len(props)) + props + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def recv_exact(sock, n):
    buf = b""
    while len(buf) < n:
        chunk = sock.recv(n - len(buf))
        if not chunk:
            raise EOFError("connection closed after %d of %d bytes" % (len(buf), n))
        buf += chunk
    return buf


# ---------------------------------------------------------------- WebSocket client

def ws_handshake(port):
    """Open a TCP connection and perform the MQTT-over-WebSocket upgrade,
    returning the socket once the 101 Switching Protocols response is read."""
    c = socket.create_connection((HOST, port), timeout=5)
    key = base64.b64encode(os.urandom(16)).decode()
    req = (
        "GET / HTTP/1.1\r\n"
        "Host: %s:%d\r\n" % (HOST, port) +
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Key: %s\r\n" % key +
        "Sec-WebSocket-Version: 13\r\n"
        "Sec-WebSocket-Protocol: mqtt\r\n"
        "\r\n"
    )
    c.sendall(req.encode())
    resp = b""
    while b"\r\n\r\n" not in resp:
        chunk = c.recv(1)
        if not chunk:
            raise EOFError("closed during handshake")
        resp += chunk
    status_line = resp.split(b"\r\n", 1)[0]
    if b" 101 " not in status_line:
        raise AssertionError("websocket upgrade failed: %r" % status_line)
    return c


def ws_send_masked(c, opcode, payload=b"", fin=True):
    """Send one client frame. Clients MUST mask (RFC 6455 §5.1)."""
    b0 = (0x80 if fin else 0x00) | (opcode & 0x0F)
    length = len(payload)
    if length < 126:
        hdr = bytes([b0, 0x80 | length])
    elif length < 65536:
        hdr = bytes([b0, 0x80 | 126]) + struct.pack("!H", length)
    else:
        hdr = bytes([b0, 0x80 | 127]) + struct.pack("!Q", length)
    mask = os.urandom(4)
    masked = bytes(payload[i] ^ mask[i % 4] for i in range(length))
    c.sendall(hdr + mask + masked)


def ws_recv_frame(c, timeout=2.0):
    """Read one server frame (servers do not mask). Returns (fin, opcode, payload)."""
    c.settimeout(timeout)
    b0, b1 = recv_exact(c, 2)
    fin = (b0 & 0x80) != 0
    opcode = b0 & 0x0F
    masked = (b1 & 0x80) != 0
    length = b1 & 0x7F
    if length == 126:
        length = struct.unpack("!H", recv_exact(c, 2))[0]
    elif length == 127:
        length = struct.unpack("!Q", recv_exact(c, 8))[0]
    if masked:
        recv_exact(c, 4)  # server never masks; discard if it ever does
    payload = recv_exact(c, length) if length else b""
    return fin, opcode, payload


def close_status(payload):
    assert len(payload) >= 2, "CLOSE frame carries no status code: %r" % payload
    return struct.unpack("!H", payload[0:2])[0]


# ---------------------------------------------------------------- broker lifecycle

def start():
    # Derive both ports from the single free port the runner handed us, so
    # there is no second free_port() race (bind TOCTOU) between the check and
    # the broker's bind.
    PORT[0] += 1
    mqtt_port = PORT[0]
    ws_port = PORT[0] + 1
    workdir = tempfile.mkdtemp(prefix="flowmq-ws-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(mqtt_port)
    env["FLOWMQ_WEBSOCKET_PORT"] = str(ws_port)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            c = socket.create_connection((HOST, ws_port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, ws_port, workdir
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


# ---------------------------------------------------------------- cases

def case_oversized_frame_closed_before_buffering():
    """A frame declaring a payload larger than MAX_MESSAGE_BYTES, with NONE of
    the payload sent, must be answered CLOSE 1009 at once. A broker that checks
    the cap only after buffering the whole payload would instead block waiting
    for the (never-arriving) bytes and send nothing."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            # FIN + BINARY, MASK + 64-bit length, declared length one byte over
            # the cap, and no payload bytes following.
            oversized = MAX_MESSAGE_BYTES + 1
            hdr = bytes([0x82, 0xFF]) + struct.pack("!Q", oversized)
            c.sendall(hdr)
            fin, opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for an oversized frame, got opcode 0x%02x" % opcode)
            status = close_status(payload)
            assert status == CLOSE_MESSAGE_TOO_BIG, (
                "expected CLOSE 1009 (message too big), got %d" % status)
            return ("oversized frame (%d bytes declared, 0 sent) rejected with "
                    "CLOSE 1009 before any payload was buffered" % oversized)
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_fragmented_control_frame_rejected():
    """A CLOSE control frame with the FIN bit clear violates RFC 6455 §5.5.1 and
    must be rejected with CLOSE 1002, not honoured as an orderly close (1000)."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            ws_send_masked(c, OP_CLOSE, b"", fin=False)
            fin, opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected a CLOSE reply, got opcode 0x%02x" % opcode)
            status = close_status(payload)
            assert status == CLOSE_PROTOCOL_ERROR, (
                "expected CLOSE 1002 for a fragmented control frame, got %d "
                "(1000 would mean it was honoured as a normal close)" % status)
            return "fragmented CLOSE (FIN=0) rejected with CLOSE 1002, not honoured as 1000"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_wrapped_mqtt_round_trips():
    """A well-formed MQTT CONNECT carried in a masked binary frame gets a
    CONNACK, proving the framing guards did not break the normal path. A QoS 0
    PUBLISH to a matching subscriber is then delivered back in a binary frame."""
    proc, ws_port, workdir = start()
    try:
        sub = ws_handshake(ws_port)
        try:
            # MQTT requires a CONNECT before any other packet.
            ws_send_masked(sub, OP_BINARY, connect("ws-sub"))
            r = ws_recv_frame(sub, 2.0)
            assert r[1] == OP_BINARY and r[2][0] & 0xF0 == 0x20, (
                "no CONNACK over WebSocket for the first client: %r" % (r,))

            # SUBSCRIBE to ws/# at QoS 0 (packet id 1, no properties).
            sub_body = struct.pack("!H", 1) + enc_len(0) + utf8("ws/#") + bytes([0x00])
            ws_send_masked(sub, OP_BINARY, bytes([0x82]) + enc_len(len(sub_body)) + sub_body)
            r = ws_recv_frame(sub, 2.0)
            assert r[1] == OP_BINARY, "SUBSCRIBE reply was not a binary frame"
            assert r[2][0] & 0xF0 == 0x90, "no SUBACK for the WebSocket SUBSCRIBE"

            pub = ws_handshake(ws_port)
            try:
                # CONNECT, then a QoS 0 PUBLISH to ws/hello.
                ws_send_masked(pub, OP_BINARY, connect("ws-pub"))
                r = ws_recv_frame(pub, 2.0)
                assert r[1] == OP_BINARY and r[2][0] & 0xF0 == 0x20, (
                    "no CONNACK over WebSocket for the publisher: %r" % (r,))
                pub_body = utf8("ws/hello") + enc_len(0) + b"hi"
                ws_send_masked(pub, OP_BINARY, bytes([0x30]) + enc_len(len(pub_body)) + pub_body)
                # The subscriber receives the PUBLISH in a binary frame.
                fin, opcode, payload = ws_recv_frame(sub, 2.0)
                assert opcode == OP_BINARY and payload[0] & 0xF0 == 0x30, (
                    "subscriber did not receive the PUBLISH over WebSocket")
                tlen = struct.unpack("!H", payload[2:4])[0]
                assert payload[4:4 + tlen] == b"ws/hello", "wrong topic delivered"
                return "CONNECT/SUBSCRIBE/PUBLISH round-trip over WebSocket framing"
            finally:
                pub.close()
        finally:
            sub.close()
    finally:
        stop(proc, workdir)


def case_rsv_bit_set_is_rejected():
    """A frame with a reserved RSV bit set violates RFC 6455 §5.2 (the MQTT
    binding negotiates no extensions) and must fail the connection with CLOSE
    1002, not be processed as an ordinary data frame."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            # FIN + BINARY with RSV1 set (0x40), MASK + zero length. The payload
            # is empty so nothing else needs to be sent.
            c.sendall(bytes([0x82 | 0x40, 0x80 | 0]))
            fin, opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for a reserved-RSV frame, got opcode 0x%02x" % opcode)
            status = close_status(payload)
            assert status == CLOSE_PROTOCOL_ERROR, (
                "expected CLOSE 1002 for a reserved RSV bit, got %d" % status)
            return "a frame with RSV1 set is rejected with CLOSE 1002 (RFC 6455 §5.2)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_reserved_opcode_is_rejected():
    """A reserved opcode (0x03, a non-control frame reserved for future use)
    violates RFC 6455 §5.2 and must fail the connection with CLOSE 1002 rather
    than being silently consumed."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            # FIN + reserved opcode 0x03, MASK + zero length.
            c.sendall(bytes([0x83, 0x80 | 0]))
            fin, opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for a reserved opcode, got opcode 0x%02x" % opcode)
            status = close_status(payload)
            assert status == CLOSE_PROTOCOL_ERROR, (
                "expected CLOSE 1002 for a reserved opcode, got %d" % status)
            return "a reserved opcode (0x03) is rejected with CLOSE 1002 (RFC 6455 §5.2)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_oversized_control_frame_rejected():
    """A control frame (PING) with a payload larger than 125 bytes violates
    RFC 6455 §5.5 and must fail the connection with CLOSE 1002, not be answered
    with a PONG."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            # FIN + PING, MASK, payload length 126 (one over the limit).
            c.sendall(bytes([0x8A, 0x80 | 126]) + struct.pack("!H", 126) + os.urandom(4) + b"x" * 126)
            fin, opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for an oversized control frame, got opcode 0x%02x" % opcode)
            status = close_status(payload)
            assert status == CLOSE_PROTOCOL_ERROR, (
                "expected CLOSE 1002 for an oversized control frame, got %d" % status)
            return "a PING with a 126-byte payload is rejected with CLOSE 1002 (RFC 6455 §5.5)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_stray_continuation_frame_rejected():
    """A CONTINUATION frame (0x0) that arrives with no fragmented message in
    progress violates RFC 6455 §5.4 (it has nothing to continue) and must fail
    the connection with CLOSE 1002. Without the check the broker would treat it
    as the start of a fresh binary message and assemble whatever follows."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            # CONTINUATION opcode, FIN set, MASK + zero length, as the very
            # first frame after the handshake (no BINARY fragment opened one).
            ws_send_masked(c, 0x00, b"")
            fin, opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for a stray CONTINUATION frame, got opcode 0x%02x" % opcode)
            status = close_status(payload)
            assert status == CLOSE_PROTOCOL_ERROR, (
                "expected CLOSE 1002 for a stray CONTINUATION frame, got %d" % status)
            return "a CONTINUATION frame with no open message is rejected with CLOSE 1002 (RFC 6455 §5.4)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_interleaved_data_frame_during_fragment_rejected():
    """A new BINARY data frame arriving while a fragmented message is still open
    (a BINARY fragment with FIN=0 has been sent, no CONTINUATION has closed it)
    violates RFC 6455 §5.4: only a CONTINUATION may continue the fragment. The
    broker must reject it with CLOSE 1002 rather than silently dropping the
    partial message and starting a competing one."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            # Open a fragmented message: BINARY, FIN clear, one payload byte.
            ws_send_masked(c, OP_BINARY, b"\x01", fin=False)
            # A second, new BINARY data frame interrupts the open fragment.
            ws_send_masked(c, OP_BINARY, b"\x02", fin=True)
            fin, opcode, payload = ws_recv_frame(c, 2.0)
            assert opcode == OP_CLOSE, (
                "expected CLOSE for an interleaved data frame, got opcode 0x%02x" % opcode)
            status = close_status(payload)
            assert status == CLOSE_PROTOCOL_ERROR, (
                "expected CLOSE 1002 for an interleaved data frame, got %d" % status)
            return "a new data frame during a fragmented message is rejected with CLOSE 1002 (RFC 6455 §5.4)"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


def case_legitimate_fragmentation_reassembles():
    """The §5.4 guard must not break legal fragmentation: a BINARY frame with
    FIN=0 followed by a CONTINUATION with FIN=1 reassembles into one MQTT
    CONNECT, which gets its CONNACK. This proves the fragmenting_ state machine
    accepts a well-formed fragment sequence end to end."""
    proc, ws_port, workdir = start()
    try:
        c = ws_handshake(ws_port)
        try:
            mqtt = connect("ws-frag")
            # Split the MQTT CONNECT bytes across a BINARY fragment and a
            # CONTINUATION frame; the broker must deliver the reassembled
            # stream to the protocol layer, which answers CONNACK.
            ws_send_masked(c, OP_BINARY, mqtt[:3], fin=False)
            ws_send_masked(c, 0x00, mqtt[3:], fin=True)
            r = ws_recv_frame(c, 2.0)
            assert r[1] == OP_BINARY and r[2][0] & 0xF0 == 0x20, (
                "no CONNACK after a legitimate fragmented CONNECT: %r" % (r,))
            return "a legal BINARY+CONTINUATION fragment pair reassembles and completes the CONNECT"
        finally:
            c.close()
    finally:
        stop(proc, workdir)


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


case("WebSocket: an oversized frame is rejected with CLOSE 1009 before its payload is buffered",
     case_oversized_frame_closed_before_buffering)
case("WebSocket: a fragmented control frame (CLOSE, FIN=0) is rejected with CLOSE 1002 (RFC 6455 §5.5.1)",
     case_fragmented_control_frame_rejected)
case("WebSocket: a frame with a reserved RSV bit set is rejected with CLOSE 1002 (RFC 6455 §5.2)",
     case_rsv_bit_set_is_rejected)
case("WebSocket: a frame with a reserved opcode (0x03) is rejected with CLOSE 1002 (RFC 6455 §5.2)",
     case_reserved_opcode_is_rejected)
case("WebSocket: a control frame (PING) with a payload over 125 bytes is rejected with CLOSE 1002 (RFC 6455 §5.5)",
     case_oversized_control_frame_rejected)
case("WebSocket (control): a normal MQTT CONNECT/SUBSCRIBE/PUBLISH round-trips inside binary frames",
     case_wrapped_mqtt_round_trips)
case("WebSocket: a CONTINUATION frame with no open message is rejected with CLOSE 1002 (RFC 6455 §5.4)",
     case_stray_continuation_frame_rejected)
case("WebSocket: a new data frame during a fragmented message is rejected with CLOSE 1002 (RFC 6455 §5.4)",
     case_interleaved_data_frame_during_fragment_rejected)
case("WebSocket: a legal BINARY+CONTINUATION fragment pair reassembles and completes the CONNECT",
     case_legitimate_fragmentation_reassembles)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
