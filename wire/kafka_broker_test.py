#!/usr/bin/env python3
"""Kafka wire probe: Produce/Fetch, ApiVersions, partition management and Connect.

Wire layouts follow the Apache Kafka protocol message schemas: Produce v2/v3,
Fetch v0, ApiVersions v0/v4, ElectLeaders v1, and flexible AlterPartitions v0
(request header v2 / response header v1). Connect endpoints follow the Kafka
Connect REST API; this broker intentionally reports no running connector tasks.
"""
import http.client
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import time
import zlib


class Reader:
    def __init__(self, data):
        self.data = data
        self.pos = 0

    def take(self, size):
        result = self.data[self.pos:self.pos + size]
        assert len(result) == size, "truncated wire value"
        self.pos += size
        return result

    def i8(self):
        return struct.unpack(">b", self.take(1))[0]

    def i16(self):
        return struct.unpack(">h", self.take(2))[0]

    def i32(self):
        return struct.unpack(">i", self.take(4))[0]

    def i64(self):
        return struct.unpack(">q", self.take(8))[0]

    def string(self, nullable=False):
        size = self.i16()
        if nullable and size == -1:
            return None
        assert size >= 0
        return self.take(size).decode()

    def uvarint(self):
        value = 0
        for shift in range(0, 35, 7):
            byte = self.i8() & 0xff
            value |= (byte & 0x7f) << shift
            if not byte & 0x80:
                return value
        raise AssertionError("invalid unsigned varint")

    def compact_string(self, nullable=False):
        size = self.uvarint()
        if nullable and size == 0:
            return None
        assert size > 0
        return self.take(size - 1).decode()

    def array(self):
        size = self.i32()
        assert size >= 0
        return size

    def compact_array(self):
        size = self.uvarint()
        assert size > 0
        return size - 1

    def tags(self):
        for _ in range(self.uvarint()):
            self.uvarint()
            self.take(self.uvarint())

    def done(self):
        assert self.pos == len(self.data), "trailing response bytes"


def i8(value):
    return struct.pack(">b", value)


def i16(value):
    return struct.pack(">h", value)


def i32(value):
    return struct.pack(">i", value)


def i64(value):
    return struct.pack(">q", value)


def string(value, nullable=False):
    if value is None and nullable:
        return i16(-1)
    raw = value.encode()
    return i16(len(raw)) + raw


def uvarint(value):
    result = bytearray()
    while value >= 0x80:
        result.append((value & 0x7f) | 0x80)
        value >>= 7
    result.append(value)
    return bytes(result)


def varint(value):
    return uvarint(value * 2 if value >= 0 else -value * 2 - 1)


def crc32c(data):
    crc = 0xffffffff
    for byte in data:
        crc ^= byte
        for _ in range(8):
            crc = (crc >> 1) ^ (0x82f63b78 if crc & 1 else 0)
    return crc ^ 0xffffffff


def compact_string(value):
    return uvarint(len(value.encode()) + 1) + value.encode()


def record_batch(value):
    timestamp = 1700000000000
    record = (b"\x00" + varint(0) + varint(0) + varint(-1) +
              varint(len(value)) + value + varint(0))
    body = (i16(0) + i32(0) + i64(timestamp) + i64(timestamp) + i64(-1) +
            i16(-1) + i32(-1) + i32(1) + varint(len(record)) + record)
    return i64(0) + i32(len(body) + 9) + i32(-1) + i8(2) + struct.pack(">I", crc32c(body)) + body


def legacy_record(value):
    payload = b"\x01\x00" + i64(1700000000000) + i32(-1) + i32(len(value)) + value
    checksum = zlib.crc32(payload) & 0xffffffff
    if checksum >= 1 << 31:
        checksum -= 1 << 32
    return i64(0) + i32(len(payload) + 4) + i32(checksum) + payload


class Kafka:
    def __init__(self, port):
        self.socket = socket.create_connection(("127.0.0.1", port), timeout=5)
        self.correlation = 0

    def close(self):
        self.socket.close()

    def recv(self, size):
        return recv_exact(self.socket, size)

    def request(self, key, version, body=b"", flexible=False):
        self.correlation += 1
        client = compact_string("wire-probe") if flexible else string("wire-probe")
        if flexible:
            client += uvarint(0)
        payload = i16(key) + i16(version) + i32(self.correlation) + client + body
        self.socket.sendall(i32(len(payload)) + payload)
        size = Reader(self.recv(4)).i32()
        response = Reader(self.recv(size))
        assert response.i32() == self.correlation
        if flexible:
            response.tags()
        return response


def recv_exact(sock, size):
    chunks = []
    while size:
        chunk = sock.recv(size)
        assert chunk, "broker closed connection"
        chunks.append(chunk)
        size -= len(chunk)
    return b"".join(chunks)


def wait_ready(process, port):
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError("broker exited during startup")
        try:
            sock = socket.create_connection(("127.0.0.1", port), timeout=0.2)
            sock.close()
            return
        except OSError:
            time.sleep(0.05)
    raise AssertionError("broker did not start")


def free_port():
    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()
    return port


def launch(binary, port, connect_port, state):
    env = os.environ.copy()
    env.update(FLOWMQ_KAFKA_PORT=str(port),
               FLOWMQ_KAFKA_ADVERTISED_HOST="127.0.0.1",
               FLOWMQ_KAFKA_STATE_FILE=state,
               FLOWMQ_KAFKA_CONNECT_PORT=str(connect_port))
    process = subprocess.Popen([binary], env=env, stdout=subprocess.DEVNULL)
    wait_ready(process, port)
    return process


def check_connect(port):
    connection = http.client.HTTPConnection("127.0.0.1", port, timeout=5)
    connection.request("GET", "/connectors")
    response = connection.getresponse()
    assert response.status == 200 and json.loads(response.read()) == []
    body = json.dumps({"name": "file-source", "config": {
        "connector.class": "org.example.FileStreamSource", "topic": "events"}})
    connection.request("POST", "/connectors", body, {"Content-Type": "application/json"})
    response = connection.getresponse()
    created = json.loads(response.read())
    assert response.status == 201 and created["type"] == "source", (response.status, created)
    connection.request("GET", "/connectors/file-source/status")
    response = connection.getresponse()
    status = json.loads(response.read())
    assert status["connector"]["state"] == "UNASSIGNED" and status["tasks"] == []
    connection.request("POST", "/connectors", body, {"Content-Type": "application/json"})
    duplicate = connection.getresponse()
    assert duplicate.status == 409, (duplicate.status, duplicate.read())
    connection.request("DELETE", "/connectors/file-source")
    response = connection.getresponse()
    assert response.status == 204
    response.read()
    connection.close()


def run(binary, port):
    connect_port = free_port()
    with tempfile.TemporaryDirectory(prefix="flowmq-kafka-wire-") as directory:
        state = os.path.join(directory, "state")
        process = launch(binary, port, connect_port, state)
        try:
            client = Kafka(port)
            versions = client.request(18, 0)
            assert versions.i16() == 0
            ranges = {}
            for _ in range(versions.array()):
                key, minimum, maximum = versions.i16(), versions.i16(), versions.i16()
                ranges[key] = (minimum, maximum)
            assert ranges[56] == (0, 0) and ranges[43] == (0, 1)
            versions.done()

            versions_v4 = client.request(18, 4, compact_string("wire-probe") +
                                         compact_string("1") + uvarint(0), flexible=True)
            assert versions_v4.i16() == 35
            ranges_v4 = {}
            for _ in range(versions_v4.array()):
                key, minimum, maximum = versions_v4.i16(), versions_v4.i16(), versions_v4.i16()
                ranges_v4[key] = (minimum, maximum)
            assert ranges_v4[18] == (0, 2)
            versions_v4.done()

            metadata = client.request(3, 0, i32(-1))
            metadata.array()
            assert metadata.i32() == 1
            assert metadata.string() == "127.0.0.1"
            assert metadata.i32() == port
            assert metadata.array() == 0
            metadata.done()

            metadata_v1 = client.request(3, 1, i32(-1))
            assert metadata_v1.array() == 1 and metadata_v1.i32() == 1
            metadata_host = metadata_v1.string()
            assert metadata_host == "127.0.0.1", metadata_host
            assert metadata_v1.i32() == port
            assert metadata_v1.string(nullable=True) is None
            assert metadata_v1.i32() == 1 and metadata_v1.array() == 0
            metadata_v1.done()

            created = client.request(19, 0, i32(1) + string("managed") +
                                     i32(2) + i16(1) + i32(0) + i32(0) + i32(3000))
            assert created.i32() == 1 and created.string() == "managed"
            assert created.i16() == 0
            created.done()
            deleted = client.request(20, 0, i32(1) + string("managed") + i32(3000))
            assert deleted.i32() == 1 and deleted.string() == "managed" and deleted.i16() == 0
            deleted.done()

            record = legacy_record(b"wire-value")
            produce = client.request(0, 1, i16(1) + i32(5000) + i32(1) +
                                     string("events") + i32(1) + i32(0) + i32(len(record)) + record)
            assert produce.i32() == 1
            assert produce.string() == "events"
            assert produce.array() == 1
            assert produce.i32() == 0
            assert produce.i16() == 0, produce.data.hex()
            assert produce.i64() == 0
            assert produce.i32() == 0
            produce.done()

            fetch = client.request(1, 0, i32(-1) + i32(100) + i32(1) +
                                   i32(1) + string("events") + i32(1) + i32(0) + i64(0) + i32(65536))
            assert fetch.array() == 1
            assert fetch.string() == "events"
            assert fetch.array() == 1
            assert fetch.i32() == 0 and fetch.i16() == 0
            assert fetch.i64() == 1
            messages = Reader(fetch.take(fetch.i32()))
            offset, size = messages.i64(), messages.i32()
            message = Reader(messages.take(size))
            checksum = struct.unpack(">I", message.data[:4])[0]
            assert checksum == zlib.crc32(message.data[4:]) & 0xffffffff
            message.i32()
            assert message.i8() == 0 and message.i8() == 0
            assert message.i32() == -1
            assert message.take(message.i32()) == b"wire-value"
            assert messages.done() is None and fetch.done() is None

            limited = client.request(1, 3, i32(-1) + i32(1) + i32(1) + i32(1) +
                                     i32(1) + string("events") + i32(1) +
                                     i32(0) + i64(0) + i32(65536))
            assert limited.i32() == 0 and limited.array() == 1 and limited.string() == "events"
            assert limited.array() == 1 and limited.i32() == 0 and limited.i16() == 0
            assert limited.i64() == 1 and limited.take(limited.i32()) == b""
            limited.done()

            record_v2 = record_batch(b"wire-v2")
            produce_v2 = client.request(0, 2, i16(1) + i32(5000) + i32(1) +
                                        string("events.v2") + i32(1) + i32(0) +
                                        i32(len(record_v2)) + record_v2)
            assert produce_v2.i32() == 1 and produce_v2.string() == "events.v2"
            assert produce_v2.array() == 1 and produce_v2.i32() == 0
            assert produce_v2.i16() == 0 and produce_v2.i64() == 0
            assert produce_v2.i64() == -1 and produce_v2.i32() == 0
            produce_v2.done()

            record_v3 = record_batch(b"wire-v3")
            produce_v3 = client.request(0, 3, i16(-1) + i16(1) + i32(5000) + i32(1) +
                                        string("events.v3") + i32(1) + i32(0) +
                                        i32(len(record_v3)) + record_v3)
            assert produce_v3.i32() == 1 and produce_v3.string() == "events.v3"
            assert produce_v3.array() == 1 and produce_v3.i32() == 0
            assert produce_v3.i16() == 0 and produce_v3.i64() == 0
            assert produce_v3.i64() == -1 and produce_v3.i32() == 0
            produce_v3.done()

            alter_body = (uvarint(2) + compact_string("events") + uvarint(2) +
                          i32(0) + i32(1) + uvarint(2) + i32(1) +
                          uvarint(0) + uvarint(0) + i32(5000) + uvarint(0))
            alter = client.request(56, 0, alter_body, flexible=True)
            assert alter.i32() == 0 and alter.i16() == 0
            assert alter.compact_array() == 1 and alter.compact_string() == "events"
            assert alter.compact_array() == 1
            assert alter.i32() == 0 and alter.i32() == 1 and alter.i32() == 0
            assert alter.compact_array() == 1 and alter.i32() == 1
            assert alter.compact_array() == 1 and alter.i32() == 1
            assert alter.i16() == 0 and alter.compact_string(nullable=True) is None
            alter.tags()
            alter.tags()
            alter.tags()
            alter.done()

            elect = client.request(43, 1, i8(0) + i32(1) + string("events") +
                                   i32(1) + i32(0) + i32(5000))
            assert elect.i32() == 0 and elect.i16() == 0
            assert elect.array() == 1 and elect.string() == "events"
            assert elect.array() == 1 and elect.i32() == 0 and elect.i16() == 0
            assert elect.string(nullable=True) is None
            elect.done()
            client.close()
            check_connect(connect_port)
        finally:
            process.terminate()
            process.wait(timeout=5)
            assert process.returncode == 0

        process = launch(binary, port, connect_port, state)
        try:
            client = Kafka(port)
            offsets = client.request(2, 0, i32(-1) + i32(1) + string("events") +
                                     i32(1) + i32(0) + i64(-1) + i32(1))
            assert offsets.array() == 1 and offsets.string() == "events"
            assert offsets.array() == 1 and offsets.i32() == 0 and offsets.i16() == 0
            assert offsets.array() == 1
            recovered_offset = offsets.i64()
            assert recovered_offset == 1, recovered_offset
            offsets.done()
            client.close()
        finally:
            process.terminate()
            process.wait(timeout=5)
            assert process.returncode == 0
    print("Kafka wire probe passed")


def main():
    if len(sys.argv) != 3:
        print("usage: kafka_broker_test.py <flowmq_kafka> <port>", file=sys.stderr)
        return 2
    run(os.path.abspath(sys.argv[1]), int(sys.argv[2]))
    return 0


if __name__ == "__main__":
    sys.exit(main())
