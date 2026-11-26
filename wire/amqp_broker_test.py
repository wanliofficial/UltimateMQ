#!/usr/bin/env python3
"""AMQP 0-9-1 broker wire probe.

Exercises the real TCP listener with a dependency-free client: protocol header,
SASL PLAIN handshake, channel negotiation, topology, direct/fanout/topic routing,
empty and multi-frame content, mandatory/immediate Basic.Return, prefetch,
acknowledgement, redelivery after disconnect, and reserved-bit rejection.
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
BINARY = sys.argv[1]
AMQP_PORT = int(sys.argv[2])

FRAME_METHOD = 1
FRAME_HEADER = 2
FRAME_BODY = 3
FRAME_END = 0xCE


def short_string(value):
    if isinstance(value, str):
        value = value.encode()
    return bytes([len(value)]) + value


def long_string(value):
    if isinstance(value, str):
        value = value.encode()
    return struct.pack("!I", len(value)) + value


def field_value(value):
    if value is None:
        return b"V"
    if isinstance(value, bool):
        return b"t" + bytes([1 if value else 0])
    if isinstance(value, int):
        if 0 <= value <= 0xff:
            return b"B" + bytes([value])
        if -0x80 <= value < 0:
            return b"b" + struct.pack("!b", value)
        if 0 <= value <= 0x7fff:
            return b"U" + struct.pack("!H", value)
        if -0x8000 <= value < 0:
            return b"s" + struct.pack("!h", value)
        if 0 <= value <= 0x7fffffff:
            return b"I" + struct.pack("!I", value)
        if -0x80000000 <= value < 0:
            return b"i" + struct.pack("!i", value)
        if value < 0:
            return b"L" + struct.pack("!q", value)
        return b"l" + struct.pack("!Q", value)
    if isinstance(value, str):
        return b"S" + long_string(value)
    if isinstance(value, (bytes, bytearray)):
        return b"x" + long_string(bytes(value))
    if isinstance(value, list):
        return b"A" + long_string(b"".join(field_value(item) for item in value))
    if isinstance(value, dict):
        return b"F" + table(value)
    raise TypeError("unsupported AMQP field value: %r" % (value,))


def table(value):
    if value is None:
        value = {}
    payload = b"".join(short_string(key) + field_value(item) for key, item in value.items())
    return long_string(payload)


def frame(frame_type, channel, payload):
    return struct.pack("!BHI", frame_type, channel, len(payload)) + payload + bytes([FRAME_END])


def method(class_id, method_id, arguments=b"", channel=1):
    return frame(FRAME_METHOD, channel, struct.pack("!HH", class_id, method_id) + arguments)


def read_exact(sock, size):
    data = b""
    while len(data) < size:
        chunk = sock.recv(size - len(data))
        if not chunk:
            raise EOFError("connection closed after %d/%d bytes" % (len(data), size))
        data += chunk
    return data


def read_frame(sock):
    header = read_exact(sock, 7)
    frame_type, channel, size = struct.unpack("!BHI", header)
    payload = read_exact(sock, size)
    assert read_exact(sock, 1) == bytes([FRAME_END]), "bad frame terminator"
    return frame_type, channel, payload


def read_method(sock):
    frame_type, channel, payload = read_frame(sock)
    assert frame_type == FRAME_METHOD, "expected method frame, got %d" % frame_type
    assert len(payload) >= 4
    class_id, method_id = struct.unpack("!HH", payload[:4])
    return class_id, method_id, channel, payload[4:]


def expect_method(sock, class_id, method_id, channel=1):
    result = read_method(sock)
    assert result[:3] == (class_id, method_id, channel), \
        "expected %d/%d on channel %d, got %r" % (class_id, method_id, channel, result)
    return result[3]


def read_content(sock, class_id, method_id, channel=1):
    arguments = expect_method(sock, class_id, method_id, channel)
    frame_type, header_channel, payload = read_frame(sock)
    assert frame_type == FRAME_HEADER and header_channel == channel
    content_class, weight, body_size = struct.unpack("!HHQ", payload[:12])
    assert (content_class, weight) == (class_id, 0)
    properties = payload[12:]
    assert len(properties) >= 2
    body = b""
    while len(body) < body_size:
        frame_type, body_channel, chunk = read_frame(sock)
        assert frame_type == FRAME_BODY and body_channel == channel
        body += chunk
    assert len(body) == body_size
    return arguments, properties, body


def sasl_start(port, username, password):
    sock = socket.create_connection((HOST, port), timeout=5)
    sock.settimeout(5)
    sock.sendall(b"AMQP\x00\x00\x09\x01")
    expect_method(sock, 10, 10, 0)
    response = ("\x00" + username + "\x00" + password).encode()
    start_ok = struct.pack("!I", 0) + short_string("PLAIN") + long_string(response) + short_string("en_US")
    sock.sendall(method(10, 11, start_ok, 0))
    return sock


def tune_client(port, channel_max, frame_max):
    sock = sasl_start(port, "guest", "guest")
    tune = expect_method(sock, 10, 30, 0)
    assert len(tune) == 8
    _channels, _frame_max, _heartbeat = struct.unpack("!HIH", tune)
    sock.sendall(method(10, 31, struct.pack("!HIH", channel_max, frame_max, 0), 0))
    return sock


class Client:
    def __init__(self, port=AMQP_PORT, username="guest", password="guest"):
        self.sock = sasl_start(port, username, password)
        self.sock.settimeout(5)
        tune = expect_method(self.sock, 10, 30, 0)
        assert len(tune) == 8
        self.channel_max, self.frame_max, _heartbeat = struct.unpack("!HIH", tune)
        self.frame_max = min(self.frame_max, 4096)
        self.sock.sendall(method(10, 31, struct.pack("!HIH", self.channel_max, self.frame_max, 0), 0))
        self.sock.sendall(method(10, 40, short_string("/") + short_string("") + b"\x00", 0))
        assert expect_method(self.sock, 10, 41, 0) == short_string("")
        self.sock.sendall(method(20, 10, short_string("")))
        assert expect_method(self.sock, 20, 11) == long_string("")

    def close(self):
        self.sock.close()

    def open_channel(self, channel=1):
        self.sock.sendall(method(20, 10, short_string(""), channel))
        expect_method(self.sock, 20, 11, channel)

    def close_channel(self, channel=1, code=0):
        args = struct.pack("!H", code) + short_string("") + struct.pack("!HH", 0, 0)
        self.sock.sendall(method(20, 40, args, channel))
        expect_method(self.sock, 20, 41, channel)

    def declare_exchange(self, name, exchange_type, arguments=None, durable=False):
        args = struct.pack("!H", 0) + short_string(name) + short_string(exchange_type)
        args += bytes([2 if durable else 0]) + struct.pack("!I", 0)
        if arguments is not None:
            args = args[:-4] + arguments
        self.sock.sendall(method(40, 10, args))
        expect_method(self.sock, 40, 11)

    def declare_queue(self, name="", exclusive=False, durable=False,
                      auto_delete=False, arguments=None, channel=1):
        bits = ((1 << 1) if durable else 0) | ((1 << 2) if exclusive else 0) | \
               ((1 << 3) if auto_delete else 0)
        assert bits < 32, bits
        args = struct.pack("!H", 0) + short_string(name) + bytes([bits])
        args += struct.pack("!I", 0) if arguments is None else long_string(arguments)
        self.sock.sendall(method(50, 10, args, channel))
        response = expect_method(self.sock, 50, 11, channel)
        declared = response[1:1 + response[0]].decode()
        return declared

    def bind(self, queue, exchange, routing_key, arguments=None, channel=1):
        args = struct.pack("!H", 0) + short_string(queue) + short_string(exchange)
        args += short_string(routing_key) + b"\x00" + struct.pack("!I", 0)
        if arguments is not None:
            args = args[:-4] + arguments
        self.sock.sendall(method(50, 20, args, channel))
        expect_method(self.sock, 50, 21, channel)

    def publish(self, exchange, routing_key, body, mandatory=False, immediate=False,
                channel=1, properties=None):
        bits = (1 if mandatory else 0) | (2 if immediate else 0)
        args = struct.pack("!H", 0) + short_string(exchange) + short_string(routing_key) + bytes([bits])
        if properties is None:
            properties = b"\x00\x00"
        header = struct.pack("!HHQ", 60, 0, len(body)) + properties
        self.sock.sendall(method(60, 40, args, channel) +
                          frame(FRAME_HEADER, channel, header))
        for offset in range(0, len(body), self.frame_max - 8):
            self.sock.sendall(frame(FRAME_BODY, channel,
                                    body[offset:offset + self.frame_max - 8]))

    def confirm_select(self, no_wait=False, channel=1):
        self.sock.sendall(method(85, 10, bytes([1 if no_wait else 0]), channel))
        if not no_wait:
            expect_method(self.sock, 85, 11, channel)

    def tx_select(self, channel=1):
        self.sock.sendall(method(90, 10, b"", channel))
        expect_method(self.sock, 90, 11, channel)

    def tx_commit(self, channel=1):
        self.sock.sendall(method(90, 20, b"", channel))
        expect_method(self.sock, 90, 21, channel)

    def tx_rollback(self, channel=1):
        self.sock.sendall(method(90, 30, b"", channel))
        expect_method(self.sock, 90, 31, channel)

    def get(self, queue, no_ack=True):
        args = struct.pack("!H", 0) + short_string(queue) + bytes([1 if no_ack else 0])
        self.sock.sendall(method(60, 70, args))
        class_id, method_id, _channel, payload = read_method(self.sock)
        if (class_id, method_id) == (10, 50):
            raise AssertionError("connection closed during Basic.Get: %r" % payload)
        if (class_id, method_id) == (60, 72):
            assert payload == b"\x00", "Basic.GetEmpty requires empty cluster-id"
            return None
        assert (class_id, method_id) == (60, 71), (
            "expected Basic.GetOk (60/71), got %d/%d" % (class_id, method_id))
        header_type, _header_channel, header_payload = read_frame(self.sock)
        assert header_type == FRAME_HEADER
        body_size = struct.unpack("!Q", header_payload[4:12])[0]
        body = b""
        while len(body) < body_size:
            body_type, _body_channel, chunk = read_frame(self.sock)
            assert body_type == FRAME_BODY
            body += chunk
        delivery_tag = struct.unpack("!Q", payload[:8])[0]
        redelivered = payload[8] & 1
        return delivery_tag, bool(redelivered), body

    def ack(self, delivery_tag, channel=1, multiple=False):
        self.sock.sendall(method(60, 80, struct.pack("!Q", delivery_tag) +
                                 bytes([1 if multiple else 0]), channel))

    def qos(self, prefetch_count, prefetch_size=0, global_qos=False, channel=1):
        self.sock.sendall(method(60, 10, struct.pack("!IHB", prefetch_size,
                                                      prefetch_count,
                                                      1 if global_qos else 0), channel))
        expect_method(self.sock, 60, 11, channel)

    def consume(self, queue, tag, no_ack=False, no_local=False, channel=1):
        args = struct.pack("!H", 0) + short_string(queue) + short_string(tag)
        bits = (1 if no_local else 0) | (2 if no_ack else 0)
        args += bytes([bits]) + struct.pack("!I", 0)
        self.sock.sendall(method(60, 20, args, channel))
        response = expect_method(self.sock, 60, 21, channel)
        assert response == short_string(tag)

    def cancel(self, tag, channel=1):
        self.sock.sendall(method(60, 30, short_string(tag) + b"\x00", channel))
        expect_method(self.sock, 60, 31, channel)

    def delivery(self, channel=1):
        return read_content(self.sock, 60, 60, channel)


def redelivered(arguments):
    return bool(arguments[9 + arguments[0]] & 1)


def expect_channel_close(client, code, channel=1):
    arguments = expect_method(client.sock, 20, 40, channel)
    assert struct.unpack("!H", arguments[:2])[0] == code, arguments
    text_size = arguments[2]
    offset = 3 + text_size
    failed_class, failed_method = struct.unpack("!HH", arguments[offset:offset + 4])
    return failed_class, failed_method


def expect_no_frame(client):
    client.sock.settimeout(0.3)
    try:
        data = client.sock.recv(1)
    except socket.timeout:
        return
    finally:
        client.sock.settimeout(5)
    raise AssertionError("unexpected data while delivery should be paused: %r" % data)


def start_broker(port=AMQP_PORT, authenticate=False, workdir=None):
    if workdir is None:
        workdir = tempfile.mkdtemp(prefix="flowmq-amqp-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port + 1)
    env["FLOWMQ_AMQP_PORT"] = str(port)
    env["FLOWMQ_DEBUG"] = "1"
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    if authenticate:
        users = os.path.join(workdir, "users")
        acl = os.path.join(workdir, "acl")
        with open(users, "w") as output:
            output.write("alice:secret\n")
        with open(acl, "w") as output:
            output.write("alice,read,q/#\nalice,write,p/#\n")
        env["FLOWMQ_PASSWORD_FILE"] = users
        env["FLOWMQ_ACL_FILE"] = acl
    err = open(os.path.join(workdir, "broker.err"), "wb")
    proc = subprocess.Popen([BINARY], cwd=workdir, env=env,
                            stdout=subprocess.DEVNULL, stderr=err)
    deadline = time.time() + 5
    while time.time() < deadline:
        try:
            probe = socket.create_connection((HOST, port), timeout=0.2)
            probe.close()
            return proc, workdir, err
        except OSError:
            if proc.poll() is not None:
                break
            time.sleep(0.05)
    proc.kill()
    proc.wait()
    err.close()
    with open(os.path.join(workdir, "broker.err"), "rb") as output:
        details = output.read().decode(errors="replace").strip()
    raise RuntimeError("AMQP listener did not start" + (": " + details if details else ""))


def stop_broker(proc, err):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    err.close()


def parse_return(arguments, body):
    reply_code = struct.unpack("!H", arguments[:2])[0]
    size = arguments[2]
    reply_text = arguments[3:3 + size].decode()
    offset = 3 + size
    exchange_size = arguments[offset]
    exchange = arguments[offset + 1:offset + 1 + exchange_size].decode()
    offset += 1 + exchange_size
    key_size = arguments[offset]
    routing_key = arguments[offset + 1:offset + 1 + key_size].decode()
    return reply_code, reply_text, exchange, routing_key, body


def delivery_tag(arguments, expected_tag=None):
    tag_size = arguments[0]
    tag = arguments[1:1 + tag_size]
    if expected_tag is not None:
        assert tag == expected_tag
    return struct.unpack("!Q", arguments[1 + tag_size:9 + tag_size])[0]


def expect_confirm(sock, sequence, channel=1):
    arguments = expect_method(sock, 60, 80, channel)
    confirmed, multiple = struct.unpack("!QB", arguments)
    assert (confirmed, multiple) == (sequence, 0), (confirmed, multiple)


def run():
    proc, _workdir, err = start_broker()
    try:
        client = Client()
        client.declare_exchange("wire.topic", "topic")
        client.declare_exchange("wire.fanout", "fanout")

        queue = client.declare_queue(exclusive=True)
        client.bind(queue, "wire.topic", "orders.*")
        large = bytes(range(256)) * 40
        client.publish("wire.topic", "orders.created", large)
        delivery = client.get(queue)
        assert delivery[2] == large, "multi-frame topic delivery was corrupted"
        client.publish("wire.topic", "orders.created.detail", b"no-match")
        assert client.get(queue) is None
        client.publish("wire.topic", "orders.created", b"")
        assert client.get(queue)[2] == b"", "zero-length content was lost"
        assert client.get(queue) is None, "Basic.GetEmpty payload is malformed"

        direct_queue = client.declare_queue(exclusive=True)
        client.publish("", direct_queue, b"direct")
        assert client.get(direct_queue)[2] == b"direct"

        properties_queue = client.declare_queue(exclusive=True)
        client.consume(properties_queue, "properties.consumer", no_ack=True)
        properties = (struct.pack("!H", 0x9004) + short_string("text/plain") +
                      b"\x02" + short_string("node-a"))
        client.publish("", properties_queue, b"properties", properties=properties)
        _arguments, delivered_properties, delivered_body = client.delivery()
        assert delivered_properties == properties and delivered_body == b"properties"
        client.cancel("properties.consumer")

        fanout_a = client.declare_queue(exclusive=True)
        fanout_b = client.declare_queue(exclusive=True)
        client.bind(fanout_a, "wire.fanout", "")
        client.bind(fanout_b, "wire.fanout", "")
        client.publish("wire.fanout", "", b"fanout")
        assert client.get(fanout_a)[2] == b"fanout"
        assert client.get(fanout_b)[2] == b"fanout"

        client.publish("", "wire.missing", b"mandatory", mandatory=True)
        args, _properties, body = read_content(client.sock, 60, 50)
        assert parse_return(args, body) == (312, "NO_ROUTE", "", "wire.missing", b"mandatory")

        immediate_queue = client.declare_queue(exclusive=True)
        client.publish("", immediate_queue, b"immediate", immediate=True)
        args, _properties, body = read_content(client.sock, 60, 50)
        assert parse_return(args, body) == (313, "NO_CONSUMERS", "", immediate_queue, b"immediate")
        assert client.get(immediate_queue) is None, "immediate message must not be queued"

        flow_queue = client.declare_queue()
        client.qos(1)
        client.consume(flow_queue, "wire.consumer")
        client.publish("", flow_queue, b"first")
        first_args, _properties, first_body = client.delivery()
        assert first_body == b"first"
        first_tag = delivery_tag(first_args)
        client.publish("", flow_queue, b"second")
        expect_no_frame(client)
        client.ack(first_tag)
        second_args, _properties, second_body = client.delivery()
        assert second_body == b"second"
        second_tag = delivery_tag(second_args)
        assert second_tag == first_tag + 1
        client.close()

        resumed = Client()
        message = None
        for _ in range(50):
            message = resumed.get(flow_queue, no_ack=False)
            if message is not None:
                break
            time.sleep(0.05)
        assert message is not None and message[1] and message[2] == b"second", \
            "unacknowledged delivery was not requeued as redelivered: %r" % (message,)
        resumed.ack(message[0])
        assert resumed.get(flow_queue) is None

        resumed.open_channel(2)
        edge_queue = resumed.declare_queue("wire.edges", channel=2)
        resumed.qos(1, channel=2)
        resumed.consume(edge_queue, "edge.one", channel=2)
        resumed.consume(edge_queue, "edge.two", channel=2)
        resumed.publish("", edge_queue, b"one")
        resumed.publish("", edge_queue, b"two")
        one_args, _properties, one_body = resumed.delivery(2)
        two_args, _properties, two_body = resumed.delivery(2)
        assert (one_body, two_body) == (b"one", b"two")
        one_tag = delivery_tag(one_args, b"edge.one")
        two_tag = delivery_tag(two_args, b"edge.two")
        resumed.publish("", edge_queue, b"three")
        expect_no_frame(resumed)
        resumed.ack(one_tag, 2)
        three_args, _properties, three_body = resumed.delivery(2)
        assert three_body == b"three"
        three_tag = delivery_tag(three_args, b"edge.one")
        resumed.ack(two_tag, 2)
        resumed.ack(three_tag, 2)
        resumed.cancel("edge.one", 2)
        resumed.cancel("edge.two", 2)

        resumed.qos(0, prefetch_size=3, channel=2)
        resumed.consume(edge_queue, "edge.size", channel=2)
        resumed.publish("", edge_queue, b"large")
        large_args, _properties, _body = resumed.delivery(2)
        large_tag = delivery_tag(large_args)
        resumed.publish("", edge_queue, b"x")
        expect_no_frame(resumed)
        resumed.ack(large_tag, 2)
        small_args, _properties, small_body = resumed.delivery(2)
        assert small_body == b"x"
        resumed.ack(delivery_tag(small_args), 2)
        resumed.cancel("edge.size", 2)

        resumed.qos(1, channel=2)
        resumed.consume(edge_queue, "edge.recover", channel=2)
        resumed.publish("", edge_queue, b"recover")
        original_args, _properties, _body = resumed.delivery(2)
        resumed.sock.sendall(method(60, 110, b"\x00", 2))
        expect_method(resumed.sock, 60, 111, 2)
        recovered_args, _properties, recovered_body = resumed.delivery(2)
        assert recovered_body == b"recover" and redelivered(recovered_args)
        resumed.ack(delivery_tag(recovered_args), 2)
        resumed.cancel("edge.recover", 2)

        resumed.qos(0, channel=2)
        resumed.consume(edge_queue, "edge.flow", no_ack=True, channel=2)
        resumed.publish("", edge_queue, b"flow.one")
        assert resumed.delivery(2)[2] == b"flow.one"
        resumed.sock.sendall(method(20, 20, b"\x00", 2))
        assert expect_method(resumed.sock, 20, 21, 2) == b"\x00"
        resumed.publish("", edge_queue, b"flow.two")
        expect_no_frame(resumed)
        resumed.sock.sendall(method(20, 20, b"\x01", 2))
        assert expect_method(resumed.sock, 20, 21, 2) == b"\x01"
        assert resumed.delivery(2)[2] == b"flow.two"
        resumed.cancel("edge.flow", 2)
        resumed.close_channel(2)
        resumed.open_channel(2)

        resumed.declare_queue("queue.redeclare", auto_delete=True)
        resumed.sock.sendall(method(50, 10, struct.pack("!H", 0) +
                                    short_string("queue.redeclare") + b"\x00" +
                                    struct.pack("!I", 0)))
        assert expect_channel_close(resumed, 406) == (50, 10)
        resumed.close_channel(1)
        resumed.open_channel(1)

        resumed.declare_queue("queue.exclusive", exclusive=True)
        other = Client()
        other.sock.sendall(method(50, 10, struct.pack("!H", 0) +
                                  short_string("queue.exclusive") + b"\x01" +
                                  struct.pack("!I", 0)))
        assert expect_channel_close(other, 405) == (50, 10)
        other.close_channel()
        other.close()

        resumed.sock.sendall(method(50, 10, struct.pack("!H", 0) + short_string("bad.bits")
                                    + b"\x80" + struct.pack("!I", 0)))
        try:
            resumed.sock.recv(1)
        except (ConnectionResetError, BrokenPipeError):
            pass
        resumed.close()

        graceful = Client()
        graceful.sock.sendall(method(10, 50, struct.pack("!H", 0) +
                                     short_string("") + struct.pack("!HH", 0, 0), 0))
        expect_method(graceful.sock, 10, 51, 0)
        assert graceful.sock.recv(1) == b"", "Close-Ok must be followed by socket close"
        graceful.close()

        for channel_max, frame_max in ((65535, 131072), (2047, 0xffffffff),
                                       (2047, 4095)):
            rejected = tune_client(AMQP_PORT, channel_max, frame_max)
            assert rejected.recv(1) == b"", \
                "invalid Tune-Ok must close without negotiated close"
            rejected.close()

        oversized = tune_client(AMQP_PORT, 2047, 4096)
        oversized.sendall(method(10, 40, short_string("/") + short_string("") + b"\x00", 0))
        expect_method(oversized, 10, 41, 0)
        oversized.sendall(struct.pack("!BHI", FRAME_METHOD, 1, 4089))
        close = expect_method(oversized, 10, 50, 0)
        assert struct.unpack("!H", close[:2])[0] == 501
        oversized.sendall(method(10, 51, b"", 0))
        assert oversized.recv(1) == b""
        oversized.close()

        client.close()
        print("PASS AMQP handshake, routing, returns, prefetch scopes, close handshakes, limits")
    finally:
        stop_broker(proc, err)


def run_features():
    port = AMQP_PORT + 3
    proc, _workdir, err = start_broker(port)
    try:
        client = Client(port)
        queue = client.declare_queue()
        client.confirm_select()
        client.publish("", queue, b"confirmed.one")
        expect_confirm(client.sock, 1)
        client.publish("", queue, b"confirmed.two")
        expect_confirm(client.sock, 2)
        client.publish("", "wire.missing", b"returned", mandatory=True)
        _arguments, _properties, body = read_content(client.sock, 60, 50)
        assert body == b"returned"
        expect_confirm(client.sock, 3)

        client.open_channel(2)
        client.confirm_select(no_wait=True, channel=2)
        client.publish("", queue, b"no-wait", channel=2)
        expect_confirm(client.sock, 1, channel=2)

        client.open_channel(3)
        client.confirm_select(channel=3)
        client.sock.sendall(method(90, 10, b"", 3))
        assert expect_channel_close(client, 406, 3) == (90, 10)
        client.close_channel(3)
        client.close()

        transactional = Client(port)
        transactional.tx_select()
        tx_queue = transactional.declare_queue()
        transactional.publish("", tx_queue, b"rolled.back")
        assert transactional.get(tx_queue) is None, "transactional publish became visible early"
        transactional.tx_rollback()
        assert transactional.get(tx_queue) is None, "rollback did not discard staged publish"
        transactional.publish("", tx_queue, b"committed")
        transactional.tx_commit()
        assert transactional.get(tx_queue)[2] == b"committed"

        transactional.publish("", tx_queue, b"ack.transaction")
        transactional.tx_commit()
        delivery = transactional.get(tx_queue, no_ack=False)
        assert delivery is not None
        transactional.ack(delivery[0])
        transactional.tx_rollback()
        transactional.sock.sendall(method(60, 110, b"\x01"))
        expect_method(transactional.sock, 60, 111)
        redelivered = transactional.get(tx_queue, no_ack=False)
        assert redelivered is not None and redelivered[1], "rollback lost a staged ack"
        transactional.ack(redelivered[0])
        transactional.tx_commit()
        assert transactional.get(tx_queue) is None, "commit did not apply staged ack"
        transactional.close()

        headers = Client(port)
        headers.declare_exchange("wire.headers", "headers")
        all_queue = headers.declare_queue()
        any_queue = headers.declare_queue()
        typed_queue = headers.declare_queue()
        headers.bind(all_queue, "wire.headers", "", table({
            "x-match": "all", "format": "text", "color": "blue"}))
        headers.bind(any_queue, "wire.headers", "", table({
            "x-match": "any", "color": "red"}))
        headers.bind(typed_queue, "wire.headers", "", table({
            "x-match": "all", "count": 1}))

        def publish_headers(values):
            properties = struct.pack("!H", 0x2000) + table(values)
            headers.publish("wire.headers", "", b"headers", properties=properties)

        publish_headers({"format": "text", "color": "blue"})
        assert headers.get(all_queue)[2] == b"headers"
        assert headers.get(any_queue) is None
        publish_headers({"format": "text", "color": "red"})
        assert headers.get(all_queue) is None
        assert headers.get(any_queue)[2] == b"headers"
        publish_headers({"count": "1"})
        assert headers.get(typed_queue) is None, "headers exchange ignored field types"
        publish_headers({"count": 1})
        assert headers.get(typed_queue)[2] == b"headers"
        headers.close()
        print("PASS AMQP publisher confirms, transactions, and headers exchanges")
    finally:
        stop_broker(proc, err)


def run_persistence():
    port = AMQP_PORT + 4
    workdir = tempfile.mkdtemp(prefix="flowmq-amqp-persist-")
    proc, _workdir, err = start_broker(port, workdir=workdir)
    try:
        client = Client(port)
        client.declare_exchange("durable.exchange", "direct", durable=True)
        queue = client.declare_queue("durable.queue", durable=True)
        client.bind(queue, "durable.exchange", "jobs")
        client.confirm_select()
        client.publish("durable.exchange", "jobs", b"confirmed.persistent")
        expect_confirm(client.sock, 1)
        state_path = os.path.join(workdir, "state.db.amqp")
        assert os.path.exists(state_path), "confirmed durable publish was not flushed"
        client.close()
        proc.kill()
        proc.wait()
        err.close()

        proc, _workdir, err = start_broker(port, workdir=workdir)
        client = Client(port)
        client.declare_exchange("durable.exchange", "direct", durable=True)
        assert client.declare_queue("durable.queue", durable=True) == queue
        assert client.get(queue)[2] == b"confirmed.persistent"
        client.publish("durable.exchange", "jobs", b"binding.restored")
        assert client.get(queue)[2] == b"binding.restored"

        client.confirm_select()
        client.publish("durable.exchange", "jobs", b"unacked")
        expect_confirm(client.sock, 1)
        client.publish("durable.exchange", "jobs", b"still.queued")
        expect_confirm(client.sock, 2)
        unacked = client.get(queue, no_ack=False)
        assert unacked is not None and unacked[2] == b"unacked" and not unacked[1]
        stop_broker(proc, err)
        proc = None

        proc, _workdir, err = start_broker(port, workdir=workdir)
        client = Client(port)
        recovered = client.get(queue, no_ack=False)
        assert recovered is not None and recovered[1] and recovered[2] == b"unacked", \
            "unacknowledged durable message was not recovered: %r" % (recovered,)
        client.ack(recovered[0])
        queued = client.get(queue, no_ack=False)
        assert queued is not None and not queued[1] and queued[2] == b"still.queued"
        client.ack(queued[0])
        assert client.get(queue) is None
        client.close()
        print("PASS AMQP durable topology, confirmed-publish crash recovery, and unacked redelivery")
    finally:
        if proc is not None and proc.poll() is None:
            stop_broker(proc, err)
        elif err is not None and not err.closed:
            err.close()


def run_auth_acl():
    port = AMQP_PORT + 2
    proc, _workdir, err = start_broker(port, authenticate=True)
    try:
        rejected = sasl_start(port, "alice", "wrong")
        assert rejected.recv(1) == b"", "pre-open authentication failure must close silently"
        rejected.close()

        client = Client(port, "alice", "secret")
        queue = client.declare_queue("q/jobs")
        assert queue == "q/jobs"
        client.bind(queue, "amq.direct", "p/jobs")
        client.publish("amq.direct", "p/jobs", b"authorized")
        assert client.get(queue)[2] == b"authorized"

        client.publish("amq.direct", "private/jobs", b"denied")
        close = expect_method(client.sock, 20, 40, 1)
        code = struct.unpack("!H", close[:2])[0]
        text_size = close[2]
        offset = 3 + text_size
        failed_class, failed_method = struct.unpack("!HH", close[offset:offset + 4])
        assert (code, failed_class, failed_method) == (403, 60, 40), close
        client.sock.sendall(method(20, 10, short_string(""), channel=2))
        expect_method(client.sock, 20, 11, 2)
        client.close()
    finally:
        stop_broker(proc, err)


if __name__ == "__main__":
    run()
    run_auth_acl()
    run_features()
    run_persistence()
    print("PASS AMQP password authentication, ACLs, and channel-scoped refusal")
