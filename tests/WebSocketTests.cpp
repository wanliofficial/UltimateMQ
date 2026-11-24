#include <gtest/gtest.h>
#include "../src/WebSocket.h"
#include "../src/TLS.h"
#include "../src/Broker.h"
#include "../src/Connection.h"
#include "../src/SysTopics.h"
#include <algorithm>
#include <cstdint>
#include <limits>
#include <string>
#include <vector>

namespace MQTT {

namespace {

constexpr uint8_t OPCODE_CONT = 0x00;
constexpr uint8_t OPCODE_BINARY = 0x02;
constexpr uint8_t OPCODE_TEXT = 0x01;
constexpr uint8_t OPCODE_CLOSE = 0x08;
constexpr uint8_t OPCODE_PING = 0x09;
constexpr uint8_t OPCODE_PONG = 0x0A;

//! A ClientTransport stand-in that feeds scripted bytes to the WebSocket frame
//! parser and records whatever the parser writes back (a CLOSE on a protocol
//! error, a PONG on a PING). It stands in for the accepted socket so the RFC
//! 6455 framing checks in WebSocketTransport::nextFrame can be exercised with
//! no real network. Constructed with fd = -1 and no TLS, the SecureTransport
//! base is inert (its only fd work, setsockopt, fails harmlessly on -1).
class ScriptedTransport : public SecureTransport {
public:
    ScriptedTransport() : SecureTransport(-1, nullptr) {}

    //! Bytes the parser will read next, in order.
    std::vector<uint8_t> input;
    //! Everything the parser has written back (CLOSE/PONG frames).
    std::vector<uint8_t> output;
    size_t writeBudget = std::numeric_limits<size_t>::max();

    int fd() const override { return -1; }
    bool handshake(std::string *) override { return true; }
    int readable(int) override { return 1; }

    ssize_t readSome(uint8_t *buf, size_t max) override {
        if (input.empty()) {
            return 0; // orderly end: the scripted bytes are exhausted
        }
        size_t take = std::min(max, input.size());
        for (size_t i = 0; i < take; ++i) {
            buf[i] = input[i];
        }
        input.erase(input.begin(), input.begin() + static_cast<long>(take));
        return static_cast<ssize_t>(take);
    }

    bool writeAll(const uint8_t *buf, size_t len) override {
        output.insert(output.end(), buf, buf + len);
        return true;
    }
    ssize_t writeSome(const uint8_t *buf, size_t len) override {
        const size_t take = std::min(len, writeBudget);
        output.insert(output.end(), buf, buf + take);
        writeBudget -= take;
        return static_cast<ssize_t>(take);
    }
    void teardown() override {}
};

//! Build a client frame: FIN/opcode byte + (MASK|length) + 4 mask bytes +
//! payload (XOR-masked when masked). \p rsv is OR-ed into the first byte to
//! inject reserved-bit errors.
std::vector<uint8_t> clientFrame(uint8_t finOpcode, const std::vector<uint8_t> &payload,
                                 bool masked = true, const uint8_t mask[4] = nullptr,
                                 uint8_t rsv = 0) {
    static const uint8_t defaultMask[4] = {0x11, 0x22, 0x33, 0x44};
    const uint8_t *m = masked ? (mask ? mask : defaultMask) : nullptr;
    std::vector<uint8_t> frame;
    frame.push_back(static_cast<uint8_t>(finOpcode | rsv));
    frame.push_back(static_cast<uint8_t>((masked ? 0x80 : 0x00) | payload.size()));
    if (masked) {
        frame.insert(frame.end(), m, m + 4);
        for (size_t i = 0; i < payload.size(); ++i) {
            frame.push_back(static_cast<uint8_t>(payload[i] ^ m[i % 4]));
        }
    } else {
        frame.insert(frame.end(), payload.begin(), payload.end());
    }
    return frame;
}

//! The payload of the first frame the parser wrote back (out[0]=0x80|opcode,
//! out[1]=length, then the body). Empty when nothing was written.
std::vector<uint8_t> firstWrittenFrame(const std::vector<uint8_t> &out) {
    if (out.size() < 2) {
        return {};
    }
    size_t len = out[1] & 0x7F;
    if (out.size() < 2 + len) {
        return {};
    }
    return std::vector<uint8_t>(out.begin() + 2, out.begin() + 2 + static_cast<long>(len));
}

uint16_t closeStatus(const std::vector<uint8_t> &out) {
    auto frame = firstWrittenFrame(out);
    if (frame.size() < 2) {
        return 0;
    }
    return static_cast<uint16_t>((frame[0] << 8) | frame[1]);
}

class WebSocketTest : public ::testing::Test {
protected:
    std::unique_ptr<ScriptedTransport> makeTransport(std::vector<uint8_t> bytes) {
        auto transport = std::make_unique<ScriptedTransport>();
        transport->input = std::move(bytes);
        return transport;
    }
};

TEST_F(WebSocketTest, WouldBlockFrameIsRetriedWithoutDuplicatingItsHeader) {
    auto transport = makeTransport({});
    ScriptedTransport *observer = transport.get();
    observer->writeBudget = 5;
    WebSocketTransport ws(std::move(transport));
    std::vector<uint8_t> payload(1000, 0x5A);

    EXPECT_EQ(ws.writeSome(payload.data(), payload.size()), 0);
    ASSERT_EQ(observer->output.size(), 5u);
    EXPECT_EQ(ws.pendingWriteBytes(), payload.size() + 4u - 5u);
    EXPECT_EQ(ws.writeSome(payload.data(), payload.size()), 0);
    EXPECT_EQ(observer->output.size(), 5u);

    observer->writeBudget = 10000;
    EXPECT_EQ(ws.writeSome(payload.data(), payload.size()),
              static_cast<ssize_t>(payload.size()));
    EXPECT_EQ(ws.pendingWriteBytes(), 0u);
    ASSERT_EQ(observer->output.size(), payload.size() + 4u);
    EXPECT_EQ(observer->output[0], 0x80 | OPCODE_BINARY);
    EXPECT_EQ(observer->output[1], 126);
    EXPECT_EQ(observer->output[2], 0x03);
    EXPECT_EQ(observer->output[3], 0xE8);
    EXPECT_TRUE(std::equal(payload.begin(), payload.end(), observer->output.begin() + 4));
}

TEST_F(WebSocketTest, ConnectionAccountsForPendingFrameBytesAndDrainsThem) {
    Broker broker;
    broker.setWriteBufferBytes(1024);
    const size_t baseline = sysGetWriteBufferBytes();
    {
        auto inner = makeTransport({});
        ScriptedTransport *observer = inner.get();
        observer->writeBudget = 1;
        auto websocket = std::make_unique<WebSocketTransport>(std::move(inner));
        Connection connection(-1, &broker, std::move(websocket));
        PingreqPacket packet;

        ASSERT_TRUE(connection.sendPacketForTest(packet));
        EXPECT_EQ(connection.writeBufferBytesForTest(), 5u)
            << "both the retained MQTT unit and raw WebSocket tail must be counted";
        EXPECT_EQ(sysGetWriteBufferBytes(), baseline + 5u);

        observer->writeBudget = 0;
        connection.drainWriteBufferForTest();
        EXPECT_EQ(connection.writeBufferBytesForTest(), 5u);

        observer->writeBudget = 100;
        connection.drainWriteBufferForTest();
        EXPECT_EQ(connection.writeBufferBytesForTest(), 0u);
        EXPECT_EQ(sysGetWriteBufferBytes(), baseline);
        EXPECT_EQ(observer->output.size(), 4u);
    }
    EXPECT_EQ(sysGetWriteBufferBytes(), baseline);
}

TEST_F(WebSocketTest, DeliversAMaskedBinaryFramePayload) {
    auto transport = makeTransport(clientFrame(0x80 | OPCODE_BINARY, {0x10, 0x20, 0x30}));
    // readSome drives the parser, which may write a CLOSE/PONG back through the
    // same transport; capture a raw pointer before ownership moves into ws so
    // the assertions below can still inspect what the parser wrote.
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    ssize_t n = ws.readSome(buf, sizeof(buf));
    ASSERT_EQ(n, 3);
    EXPECT_EQ(buf[0], 0x10);
    EXPECT_EQ(buf[1], 0x20);
    EXPECT_EQ(buf[2], 0x30);
    // A legal masked frame is accepted: the parser writes nothing back (no CLOSE).
    EXPECT_TRUE(observer->output.empty());
}

//! RFC 6455 §5.1: the server MUST fail the connection when a client frame is
//! not masked. Masking is what makes the framing safe against cache poisoning
//! over shared proxies, so this enforcement is security-critical.
TEST_F(WebSocketTest, RejectsAnUnmaskedClientFrame) {
    auto transport = makeTransport(clientFrame(0x80 | OPCODE_BINARY, {0x01}, /*masked=*/false));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    EXPECT_EQ(ws.readSome(buf, sizeof(buf)), 0); // rejected
    EXPECT_EQ(closeStatus(observer->output), 1002); // protocol error CLOSE
}

//! RFC 6455 §5.2: RSV1/2/3 must be 0 unless an extension defining them was
//! negotiated. The MQTT-over-WebSocket binding negotiates none, so any set RSV
//! bit is a protocol error and must fail the connection.
TEST_F(WebSocketTest, RejectsAReservedRsvBit) {
    uint8_t mask[4] = {0, 0, 0, 0};
    auto transport = makeTransport(clientFrame(0x80 | OPCODE_BINARY, {0x01}, true, mask, /*rsv=*/0x40));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    EXPECT_EQ(ws.readSome(buf, sizeof(buf)), 0);
    EXPECT_EQ(closeStatus(observer->output), 1002);
}

//! RFC 6455 §5.2: a reserved opcode (0x03-0x07, 0x0B-0x0F) must fail the
//! connection rather be silently ignored.
TEST_F(WebSocketTest, RejectsAReservedOpcode) {
    uint8_t mask[4] = {0, 0, 0, 0};
    auto transport = makeTransport(clientFrame(0x80 | 0x03, {0x01}, true, mask));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    EXPECT_EQ(ws.readSome(buf, sizeof(buf)), 0);
    EXPECT_EQ(closeStatus(observer->output), 1002);
}

//! RFC 6455 §5.5: a control frame (here CLOSE) with a payload over 125 bytes is
//! a protocol error and must fail the connection, not be buffered.
TEST_F(WebSocketTest, RejectsAnOversizedControlFrame) {
    // 126-byte payload needs the 16-bit extended length (126 marker); a control
    // frame must nonetheless be <= 125 bytes (RFC 6455 §5.5).
    std::vector<uint8_t> frame = {0x88, 126, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00};
    frame.insert(frame.end(), 126, 0x00);
    auto transport = makeTransport(std::move(frame));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    EXPECT_EQ(ws.readSome(buf, sizeof(buf)), 0);
    EXPECT_EQ(closeStatus(observer->output), 1002);
}

//! RFC 6455 §5.5.1: a control frame must not be fragmented (FIN clear).
TEST_F(WebSocketTest, RejectsAFragmentedControlFrame) {
    uint8_t mask[4] = {0, 0, 0, 0};
    // PING with FIN clear.
    auto transport = makeTransport(clientFrame(0x00 | OPCODE_PING, {0x01}, true, mask));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    EXPECT_EQ(ws.readSome(buf, sizeof(buf)), 0);
    EXPECT_EQ(closeStatus(observer->output), 1002);
}

//! RFC 6455 §5.4: a CONTINUATION frame with no open fragment has nothing to
//! continue and must fail the connection.
TEST_F(WebSocketTest, RejectsAnOrphanContinuationFrame) {
    uint8_t mask[4] = {0, 0, 0, 0};
    auto transport = makeTransport(clientFrame(0x80 | OPCODE_CONT, {0x01}, true, mask));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    EXPECT_EQ(ws.readSome(buf, sizeof(buf)), 0);
    EXPECT_EQ(closeStatus(observer->output), 1002);
}

//! RFC 6455 §5.6 / MQTT binding: text frames are not part of MQTT-over-WebSocket
//! and must be rejected (1003 unsupported data).
TEST_F(WebSocketTest, RejectsATextFrame) {
    uint8_t mask[4] = {0, 0, 0, 0};
    auto transport = makeTransport(clientFrame(0x80 | OPCODE_TEXT, {0x68, 0x69}, true, mask));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    EXPECT_EQ(ws.readSome(buf, sizeof(buf)), 0);
    EXPECT_EQ(closeStatus(observer->output), 1003);
}

//! A masked PING is answered with a PONG carrying the same (unmasked) payload
//! (RFC 6455 §5.5.2).
TEST_F(WebSocketTest, AnswersAPingWithAPong) {
    uint8_t mask[4] = {0x00, 0x00, 0x00, 0x00};
    std::vector<uint8_t> payload = {'h', 'i'};
    auto transport = makeTransport(clientFrame(0x80 | OPCODE_PING, payload, true, mask));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    // readSome returns 0 (a PING yields no MQTT message) but a PONG is written.
    ws.readSome(buf, sizeof(buf));
    ASSERT_FALSE(observer->output.empty());
    EXPECT_EQ(observer->output[0] & 0x0F, OPCODE_PONG); // PONG opcode
    auto pong = firstWrittenFrame(observer->output);
    ASSERT_EQ(pong.size(), 2u);
    EXPECT_EQ(pong[0], 'h');
    EXPECT_EQ(pong[1], 'i');
}

//! A fragmented binary message (BINARY, FIN clear, then CONTINUATION, FIN set)
//! is reassembled into one MQTT message (RFC 6455 §5.4).
TEST_F(WebSocketTest, ReassemblesAFragmentedBinaryMessage) {
    uint8_t mask[4] = {0, 0, 0, 0};
    std::vector<uint8_t> bytes;
    auto first = clientFrame(0x00 | OPCODE_BINARY, {0x01, 0x02}, true, mask); // FIN clear
    auto cont = clientFrame(0x80 | OPCODE_CONT, {0x03, 0x04}, true, mask);    // FIN set
    bytes.insert(bytes.end(), first.begin(), first.end());
    bytes.insert(bytes.end(), cont.begin(), cont.end());

    auto transport = makeTransport(std::move(bytes));
    ScriptedTransport *observer = transport.get();
    WebSocketTransport ws(std::move(transport));

    uint8_t buf[16] = {0};
    ssize_t n = ws.readSome(buf, sizeof(buf));
    ASSERT_EQ(n, 4);
    EXPECT_EQ(buf[0], 0x01);
    EXPECT_EQ(buf[1], 0x02);
    EXPECT_EQ(buf[2], 0x03);
    EXPECT_EQ(buf[3], 0x04);
    EXPECT_TRUE(observer->output.empty());
}

} // namespace

} // namespace MQTT
