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

} // namespace MQTT
