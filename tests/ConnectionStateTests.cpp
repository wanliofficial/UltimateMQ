#include <gtest/gtest.h>
#include "../src/Connection.h"
#include "../src/Broker.h"
#include <sys/socket.h>
#include <unistd.h>
#include <memory>

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

} // namespace MQTT
