#include <gtest/gtest.h>
#include "../src/Connection.h"
#include "../src/Broker.h"
#include "../src/SysTopics.h"
#include <sys/socket.h>
#include <sys/time.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <cerrno>
#include <memory>
#include <vector>

namespace MQTT {

namespace {
//! A Connection bound to one end of a socketpair, so handleIncoming() can be
//! driven directly instead of through the network. The peer end is closed by
//! the destructor.
struct SocketPairConnection {
    std::shared_ptr<Connection> connection;
    int peer = -1;

    SocketPairConnection(Broker *broker) {
        int fds[2] = {-1, -1};
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, fds) != 0) {
            throw std::runtime_error("socketpair failed");
        }
        connection = std::make_shared<Connection>(fds[0], broker);
        peer = fds[1];
    }

    ~SocketPairConnection() {
        connection->detach();
        connection.reset();
        if (peer >= 0) {
            ::close(peer);
            peer = -1;
        }
    }

    //! Whether the broker sent anything back. With its own end closed and the
    //! loop stopped, a recv on the peer returns EOF (0) when the broker stayed
    //! silent, and the response bytes when it did not.
    size_t readBack(char *buffer, size_t size) {
        connection->detach();
        ssize_t got = ::recv(peer, buffer, size, 0);
        return got < 0 ? 0 : static_cast<size_t>(got);
    }

    //! Read what the broker has already written without closing its end, so a
    //! second packet can be inspected afterwards.
    size_t readPeer(char *buffer, size_t size) {
        ssize_t got = ::recv(peer, buffer, size, 0);
        return got < 0 ? 0 : static_cast<size_t>(got);
    }
};

//! A Connection bound to one end of a real TCP loopback connection, with a
//! deliberately tiny send buffer so a write the peer never drains stalls. A TCP
//! socket is required (not socketpair(AF_UNIX)) because only TCP distinguishes a
//! graceful FIN from a RST: AF_UNIX delivers a clean EOF however the socket is
//! closed, so it cannot observe the write-stall teardown this pins.
struct TCPLoopbackConnection {
    std::shared_ptr<Connection> connection;
    int peer = -1;

    explicit TCPLoopbackConnection(Broker *broker) {
        const int listener = ::socket(AF_INET, SOCK_STREAM, 0);
        if (listener < 0) {
            throw std::runtime_error("socket failed");
        }
        sockaddr_in addr{};
        addr.sin_family = AF_INET;
        addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
        addr.sin_port = 0;
        if (::bind(listener, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0 ||
            ::listen(listener, 1) != 0) {
            ::close(listener);
            throw std::runtime_error("bind/listen failed");
        }
        socklen_t addrLen = sizeof(addr);
        if (::getsockname(listener, reinterpret_cast<sockaddr *>(&addr), &addrLen) != 0) {
            ::close(listener);
            throw std::runtime_error("getsockname failed");
        }
        peer = ::socket(AF_INET, SOCK_STREAM, 0);
        // A small receive buffer on the peer keeps the kernel from absorbing the
        // whole payload, so the broker's write is forced to stall.
        const int rcvbuf = 4096;
        ::setsockopt(peer, SOL_SOCKET, SO_RCVBUF, &rcvbuf, sizeof(rcvbuf));
        if (peer < 0 ||
            ::connect(peer, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) != 0) {
            if (peer >= 0) {
                ::close(peer);
            }
            ::close(listener);
            throw std::runtime_error("connect failed");
        }
        const int server = ::accept(listener, nullptr, nullptr);
        ::close(listener);
        if (server < 0) {
            ::close(peer);
            throw std::runtime_error("accept failed");
        }
        const int sndbuf = 4096;
        ::setsockopt(server, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        connection = std::make_shared<Connection>(server, broker);
    }

    ~TCPLoopbackConnection() {
        connection->detach();
        connection.reset();
        if (peer >= 0) {
            ::close(peer);
            peer = -1;
        }
    }
};

} // namespace

class ConnectionStateTests : public ::testing::Test {
protected:
    Broker broker;
};

TEST_F(ConnectionStateTests, PingreqBeforeConnectIsRefused)
{
    // MQTT-3.1.0-1: the first packet from a client MUST be a CONNECT. A PINGREQ
    // before it must not be answered, and the connection must be closed.
    SocketPairConnection pair(&broker);
    auto pingreq = std::make_shared<PingreqPacket>();
    pair.connection->handleIncoming(pingreq);
    EXPECT_FALSE(pair.connection->isConnected());

    char buffer[64];
    EXPECT_EQ(pair.readBack(buffer, sizeof(buffer)), 0u)
        << "the broker answered a packet that preceded CONNECT";
}

TEST_F(ConnectionStateTests, SubscribeBeforeConnectIsRefused)
{
    // Without a session the SUBSCRIBE handler throws; the guard in
    // handleIncoming() must turn that into a plain close instead of letting the
    // exception escape, and must not grant anything.
    SocketPairConnection pair(&broker);
    auto subscribe = std::make_shared<SubscribePacket>();
    subscribe->packetId = 1;
    subscribe->subscriptions.emplace_back(
        "pre/#", SubscriptionOptions{QoS::QOS_0, false, false,
                                     RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES});
    EXPECT_NO_THROW(pair.connection->handleIncoming(subscribe));
    EXPECT_FALSE(pair.connection->isConnected());

    char buffer[64];
    EXPECT_EQ(pair.readBack(buffer, sizeof(buffer)), 0u)
        << "a pre-CONNECT SUBSCRIBE was answered";
}

TEST_F(ConnectionStateTests, AuthWithoutMethodIsAProtocolError)
{
    // The broker refuses every Authentication Method, so no enhanced
    // authentication can be running and [MQTT-4.12.0-7] forbids the client from
    // sending AUTH. The answer must be DISCONNECT 0x82 (§4.13.1): a Server only
    // sends AUTH inside an agreed exchange, and AUTH's Reason Code is limited to
    // 0x00/0x18/0x19 (MQTT-3.15.2-1), so the old AUTH 0x8C reply was invalid.
    SocketPairConnection pair(&broker);
    auto connect = std::make_shared<ConnectPacket>();
    connect->protocolName = "MQTT";
    connect->protocolVersion = Version::MQTT5;
    connect->clientId = "auth-no-method";
    connect->cleanStart = true;
    connect->keepAlive = 60;
    connect->willFlag = false;
    connect->willQos = QoS::QOS_0;
    connect->willRetain = false;
    pair.connection->handleIncoming(connect);
    ASSERT_TRUE(pair.connection->isConnected()) << "the CONNECT was refused";

    char buffer[64];
    ASSERT_GT(pair.readPeer(buffer, sizeof(buffer)), 0u) << "no CONNACK";

    // A syntactically legal AUTH (Reason Code 0x19, Re-authenticate) is still
    // invalid on a connection without an authentication method.
    auto auth = std::make_shared<AuthPacket>(ReasonCode::REAUTHENTICATE);
    pair.connection->handleIncoming(auth);
    EXPECT_FALSE(pair.connection->isConnected());

    const size_t size = pair.readPeer(buffer, sizeof(buffer));
    ASSERT_GE(size, 3u) << "the broker sent no DISCONNECT";
    EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0xE0) << "expected a DISCONNECT packet";
    EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x82)
        << "expected Protocol Error (0x82), got 0x"
        << std::hex << static_cast<int>(static_cast<uint8_t>(buffer[2]));
}

TEST_F(ConnectionStateTests, SecondConnectOnTheSameConnectionIsAProtocolError)
{
    // MQTT-3.1.0-2: "The Server MUST process a second CONNECT packet sent from a
    // Client as a Protocol Error and close the Network Connection." The second
    // CONNECT parses perfectly, so §1.5.6/§4.13 make it 0x82 Protocol Error, not
    // 0x81 Malformed Packet. It used to reach run()'s catch-all (a plain
    // std::runtime_error) and was reported as MALFORMED_PACKET.
    SocketPairConnection pair(&broker);
    auto connect = std::make_shared<ConnectPacket>();
    connect->protocolName = "MQTT";
    connect->protocolVersion = Version::MQTT5;
    connect->clientId = "second-connect";
    connect->cleanStart = true;
    connect->keepAlive = 60;
    connect->willFlag = false;
    connect->willQos = QoS::QOS_0;
    connect->willRetain = false;
    pair.connection->handleIncoming(connect);
    ASSERT_TRUE(pair.connection->isConnected()) << "the first CONNECT was refused";

    char buffer[64];
    ASSERT_GT(pair.readPeer(buffer, sizeof(buffer)), 0u) << "no CONNACK";

    // The forbidden second CONNECT. handleIncoming() does not gate it (the
    // connection is already CONNECTED), so handleConnect() must refuse it.
    auto second = std::make_shared<ConnectPacket>(*connect);
    EXPECT_THROW(pair.connection->handleIncoming(second), Frame::ProtocolViolation)
        << "a second CONNECT was not reported as a Protocol Error";
}

TEST_F(ConnectionStateTests, SecondConnectIsReportedAsProtocolErrorOnTheWire)
{
    // The same violation as seen by the client: run() maps ProtocolViolation to
    // DISCONNECT 0x82. Reached through closeWithReason() here so the mapping is
    // asserted without a network thread.
    SocketPairConnection pair(&broker);
    auto connect = std::make_shared<ConnectPacket>();
    connect->protocolName = "MQTT";
    connect->protocolVersion = Version::MQTT5;
    connect->clientId = "second-connect-wire";
    connect->cleanStart = true;
    connect->keepAlive = 60;
    connect->willFlag = false;
    connect->willQos = QoS::QOS_0;
    connect->willRetain = false;
    pair.connection->handleIncoming(connect);
    ASSERT_TRUE(pair.connection->isConnected()) << "the CONNECT was refused";

    char buffer[64];
    ASSERT_GT(pair.readPeer(buffer, sizeof(buffer)), 0u) << "no CONNACK";

    auto second = std::make_shared<ConnectPacket>(*connect);
    try {
        pair.connection->handleIncoming(second);
        FAIL() << "a second CONNECT was accepted";
    } catch (const Frame::ProtocolViolation &) {
        // Exactly what run() catches and turns into DISCONNECT PROTOCOL_ERROR.
    }
    pair.connection->closeWithReason(ReasonCode::PROTOCOL_ERROR);
    EXPECT_FALSE(pair.connection->isConnected());

    const size_t size = pair.readPeer(buffer, sizeof(buffer));
    ASSERT_GE(size, 3u) << "the broker sent no DISCONNECT";
    EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0xE0) << "expected a DISCONNECT packet";
    EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x82)
        << "expected Protocol Error (0x82), got 0x"
        << std::hex << static_cast<int>(static_cast<uint8_t>(buffer[2]));
}

TEST_F(ConnectionStateTests, PublishWithWildcardResponseTopicIsAProtocolError)
{
    // MQTT-3.3.2-14: the Response Topic is used as the Topic Name of the reply,
    // so it MUST NOT contain wildcard characters. It is a Protocol Error
    // (MQTT-4.13.1-1), reported with DISCONNECT 0x82 for every QoS because no
    // PUBACK/PUBREC reason code exists for it.
    SocketPairConnection pair(&broker);
    auto connect = std::make_shared<ConnectPacket>();
    connect->protocolName = "MQTT";
    connect->protocolVersion = Version::MQTT5;
    connect->clientId = "wildcard-response-topic";
    connect->cleanStart = true;
    connect->keepAlive = 60;
    connect->willFlag = false;
    connect->willQos = QoS::QOS_0;
    connect->willRetain = false;
    pair.connection->handleIncoming(connect);
    ASSERT_TRUE(pair.connection->isConnected()) << "the CONNECT was refused";

    char buffer[64];
    ASSERT_GT(pair.readPeer(buffer, sizeof(buffer)), 0u) << "no CONNACK";

    // QoS 1 so a per-message rejection would have to be a PUBACK; a wildcard
    // Response Topic is a Protocol Error instead, so the connection must close.
    auto publish = std::make_shared<PublishPacket>(
        "a/b", std::vector<uint8_t>{'x'}, QoS::QOS_1, false, /*packetId=*/1);
    publish->properties[PropertyID::RESPONSE_TOPIC] = std::string("reply/#");
    pair.connection->handleIncoming(publish);
    EXPECT_FALSE(pair.connection->isConnected())
        << "a PUBLISH with a wildcard Response Topic was accepted";

    const size_t size = pair.readPeer(buffer, sizeof(buffer));
    ASSERT_GE(size, 3u) << "the broker sent no DISCONNECT";
    EXPECT_EQ(static_cast<uint8_t>(buffer[0]), 0xE0) << "expected a DISCONNECT packet";
    EXPECT_EQ(static_cast<uint8_t>(buffer[2]), 0x82)
        << "expected Protocol Error (0x82), got 0x"
        << std::hex << static_cast<int>(static_cast<uint8_t>(buffer[2]));
}

TEST_F(ConnectionStateTests, BufferedWriteBuffersOverflowAndTracksTheGauge)
{
    // With FLOWMQ_WRITE_BUFFER set, a write the socket cannot take immediately is
    // buffered and the live occupancy gauge ($SYS/broker/writebuffer/bytes) rises
    // with it. A connection without the buffer configured never engages it.
    broker.setWriteBufferBytes(256 * 1024); // 256KB buffer
    SocketPairConnection pair(&broker);
    ASSERT_TRUE(pair.connection->writeBufferActiveForTest())
        << "the write buffer did not activate for a configured broker";

    // A 128KB PUBLISH: the kernel socketpair buffer (typically 8-64KB auto-tuned)
    // absorbs part of it, and the remainder buffers in the write buffer (128KB
    // < 256KB limit, so the connection survives). This exercises the buffered path
    // and verifies the live gauge tracks the buffered bytes.
    std::vector<uint8_t> payload(128 * 1024, 'x');
    auto publish = std::make_shared<PublishPacket>("buf/t", payload, QoS::QOS_0);
    ASSERT_TRUE(pair.connection->sendPacketForTest(*publish))
        << "a packet within the buffer limit must be accepted";

    const size_t buffered = pair.connection->writeBufferBytesForTest();
    EXPECT_GT(buffered, 0u)
        << "a write larger than the socket buffer must leave a buffered remainder";
    EXPECT_EQ(sysGetWriteBufferBytes(), buffered)
        << "the live gauge must equal this connection's buffered bytes";

    // Break the peer and drain: the write now fails on the first unit, so nothing
    // is freed -- but the per-iteration gauge accounting must not over-subtract or
    // leave the gauge above the true buffered count.
    ::close(pair.peer);
    pair.peer = -1;
    pair.connection->drainWriteBufferForTest();
    EXPECT_LE(sysGetWriteBufferBytes(), buffered)
        << "a failed drain must never inflate the live gauge";
}

TEST_F(ConnectionStateTests, WriteBufferGaugeReturnsToZeroOnDestruction)
{
    // A connection torn down with bytes still buffered (a peer that vanished
    // mid-write) must remove every one of those bytes from the live gauge in its
    // destructor, or $SYS/broker/writebuffer/bytes would stay permanently
    // inflated. Driven in a nested scope so the gauge can be read after the
    // connection is destroyed.
    broker.setWriteBufferBytes(256 * 1024);
    const size_t baseline = sysGetWriteBufferBytes();
    {
        int fds[2] = {-1, -1};
        ASSERT_EQ(socketpair(AF_UNIX, SOCK_STREAM, 0, fds), 0);
        auto connection = std::make_shared<Connection>(fds[0], &broker);
        std::vector<uint8_t> payload(128 * 1024, 'x');
        auto publish = std::make_shared<PublishPacket>("buf/t", payload, QoS::QOS_0);
        ASSERT_TRUE(connection->sendPacketForTest(*publish));
        ASSERT_GT(connection->writeBufferBytesForTest(), 0u);
        EXPECT_GT(sysGetWriteBufferBytes(), baseline);
        ::close(fds[1]);
        connection->detach();
    } // connection destroyed here
    EXPECT_EQ(sysGetWriteBufferBytes(), baseline)
        << "the gauge must return to its baseline once the connection is gone";
}

TEST_F(ConnectionStateTests, NoWriteBufferKeepsThePathSynchronous)
{
    // Default (no FLOWMQ_WRITE_BUFFER): the buffer never engages, so a write goes
    // straight through and nothing is buffered -- the historical behaviour.
    SocketPairConnection pair(&broker);
    EXPECT_FALSE(pair.connection->writeBufferActiveForTest());

    std::vector<uint8_t> payload(2048, 'x');
    auto publish = std::make_shared<PublishPacket>("sync/t", payload, QoS::QOS_0);
    EXPECT_TRUE(pair.connection->sendPacketForTest(*publish));
    EXPECT_EQ(pair.connection->writeBufferBytesForTest(), 0u);
    EXPECT_EQ(sysGetWriteBufferBytes(), 0u);
}

TEST_F(ConnectionStateTests, WriteStallDropResetsThePeerSoItSeesTheEndImmediately)
{
    // A peer that stops reading fills the kernel send buffer until the blocking
    // write stalls past FLOWMQ_WRITE_TIMEOUT_MS, and the connection is dropped.
    // The teardown must reach the peer as a RST (SO_LINGER={1,0}), not a
    // graceful FIN: in production the autotuned send buffer holds megabytes the
    // peer refuses to read, and a plain close() would keep that data queued
    // ahead of the FIN, so the stalled reader would never see EOF and would leak.
    // (This test caps the send buffer to 4KB so the stall happens at once; the
    // RST-vs-FIN distinction is what it pins.)
    broker.setWriteTimeoutMs(200);
    TCPLoopbackConnection pair(&broker);

    std::vector<uint8_t> payload(1024 * 1024, 'x');
    auto publish = std::make_shared<PublishPacket>("stall/t", payload, QoS::QOS_0);
    EXPECT_FALSE(pair.connection->sendPacketForTest(*publish))
        << "a write the peer never drains must be reported as failed";
    EXPECT_FALSE(pair.connection->isConnected());

    // Tear down exactly as run() does after a failed write.
    pair.connection->detach();

    // Bound the drain so a regression (data queued ahead of a FIN) fails here
    // instead of hanging the suite.
    timeval tv{2, 0};
    ::setsockopt(pair.peer, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::vector<char> buf(64 * 1024);
    size_t total = 0;
    ssize_t n = 0;
    while ((n = ::recv(pair.peer, buf.data(), buf.size(), 0)) > 0) {
        total += static_cast<size_t>(n);
        if (total >= payload.size()) {
            break;
        }
    }
    const int lastErrno = n < 0 ? errno : 0;
    EXPECT_EQ(lastErrno, ECONNRESET)
        << "a write-stall drop must reach the peer as a RST, not a clean FIN";
    EXPECT_LT(total, payload.size())
        << "the undeliverable send buffer must be discarded, not drained";
}

TEST_F(ConnectionStateTests, GracefulCloseStillDeliversQueuedDataThenEOF)
{
    // The complement of the write-stall drop: a close that is not a drop keeps
    // the historical FIN semantics, so the peer still receives every buffered
    // byte and then a clean EOF (recv() == 0).
    TCPLoopbackConnection pair(&broker);

    std::vector<uint8_t> payload(1024, 'x');
    auto publish = std::make_shared<PublishPacket>("graceful/t", payload, QoS::QOS_0);
    ASSERT_TRUE(pair.connection->sendPacketForTest(*publish));

    pair.connection->detach();

    timeval tv{2, 0};
    ::setsockopt(pair.peer, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    std::vector<char> buf(64 * 1024);
    size_t total = 0;
    ssize_t n = 0;
    while ((n = ::recv(pair.peer, buf.data(), buf.size(), 0)) > 0) {
        total += static_cast<size_t>(n);
    }
    EXPECT_EQ(n, 0) << "a graceful close must end with a clean EOF, not an error";
    EXPECT_GT(total, 0u) << "the graceful path must still deliver the queued bytes";
}

} // namespace MQTT
