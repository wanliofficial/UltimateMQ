#include <gtest/gtest.h>
#include "../src/Broker.h"
#include "../src/Session.h"
#include "TestClient.h"
#include <atomic>
#include <memory>
#include <thread>
#include <vector>

namespace MQTT {

namespace {
SubscriptionOptions defaultOptions() {
    return SubscriptionOptions{QoS::QOS_0, false, false,
                               RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
}
}

class SessionLifecycleTest : public ::testing::Test {
protected:
    Broker broker;
};

TEST_F(SessionLifecycleTest, SessionRegistersAndUnregistersOnDestruction)
{
    {
        auto session = std::make_shared<Session>(&broker, "lifecycle");
        session->connect(session);
        EXPECT_EQ(broker.getConnectedClients(), 1);
        EXPECT_EQ(broker.findSession("lifecycle"), session);
    }
    // Dropping the last reference must not leave a dangling entry behind
    EXPECT_EQ(broker.getConnectedClients(), 0);
    EXPECT_EQ(broker.findSession("lifecycle"), nullptr);
}

TEST_F(SessionLifecycleTest, TakeoverNotifiesOldConnectionOnlyOnce)
{
    auto oldSession = std::make_shared<Session>(&broker, "dup");
    oldSession->connect(oldSession);

    std::atomic<int> takeovers{0};
    oldSession->setTakeoverCallback([&takeovers]() { takeovers++; });

    // The second delivery takes the session over and must not reach the subscriber
    broker.subscribe("dup", "t/x");
    broker.publish(Message("t/x", "data"));

    EXPECT_EQ(takeovers.load(), 1);
}

TEST_F(SessionLifecycleTest, RetainedMessageIsQueuedThenDeliveredByFlush)
{
    broker.publish(MQTT::Message("ret/t", "old", MQTT::QoS::QOS_0, true));
    auto session = std::make_shared<Session>(&broker, "retained-sub");
    session->connect(session);
    auto client = test::attach(*session);

    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE};
    session->subscribe("ret/t", options);
    session->deliverRetained("ret/t", options);
    session->flush();

    ASSERT_EQ(client->size(), 1u);
    EXPECT_EQ(client->delivered[0].topic, "ret/t");
    EXPECT_TRUE(client->delivered[0].retain);
}

TEST_F(SessionLifecycleTest, DeliveryAfterTakeoverIsSilent)
{
    auto session = std::make_shared<Session>(&broker, "gone");
    session->connect(session);

    auto client = test::attach(*session);
    session->setTakeoverCallback([]() {});  // connection already replaced
    session->subscribe("t/y", defaultOptions());

    broker.publish(Message("t/y", "data"));
    session->flush();
    EXPECT_EQ(client->size(), 0u);
}

TEST_F(SessionLifecycleTest, DisconnectCallbackIsInvokedExactlyOnce)
{
    auto session = std::make_shared<Session>(&broker, "disc");
    session->connect(session);
    std::atomic<int> disconnects{0};
    session->setDisconnectCallback([&disconnects]() { disconnects++; });
    session->disconnect();
    // A repeated disconnect must not notify the (possibly destroyed) connection again
    session->disconnect();
    EXPECT_EQ(disconnects.load(), 1);
    EXPECT_EQ(broker.getConnectedClients(), 0);
}

TEST_F(SessionLifecycleTest, BrokerForgetsOnlyTheMatchingSession)
{
    auto first = std::make_shared<Session>(&broker, "same-id");
    auto client = test::attach(*first);
    first->subscribe("f/x", defaultOptions());

    // A reconnect installs a new session under the same client id
    auto second = std::make_shared<Session>(&broker, "same-id");
    second->connect(second);
    EXPECT_EQ(broker.findSession("same-id"), second);

    // Disconnecting the stale session must not remove the live one
    first->disconnect();
    EXPECT_EQ(broker.findSession("same-id"), second);
}

TEST_F(SessionLifecycleTest, ConcurrentPublishAndReconnectDoesNotCrash)
{
    std::atomic<bool> stop{false};
    std::atomic<int> published{0};
    std::vector<std::thread> threads;
    std::vector<std::shared_ptr<Session>> subscribers;

    for (int i = 0; i < 4; ++i) {
        auto session = std::make_shared<Session>(&broker, "load-" + std::to_string(i));
        subscribers.push_back(session);
        auto client = test::attach(*session);
        session->setTakeoverCallback([]() {});
        session->subscribe("load/#", defaultOptions());
        session->connect(session);
    }

    for (int i = 0; i < 2; ++i) {
        threads.emplace_back([this, &stop, &published]() {
            int n = 0;
            while (!stop) {
                broker.publish(Message("load/" + std::to_string(n++), "payload"));
                published++;
            }
        });
    }
    // Churn: connect and disconnect sessions with colliding client ids while
    // messages are being published
    threads.emplace_back([this, &stop]() {
        int round = 0;
        std::vector<std::shared_ptr<Session>> alive;
        while (!stop) {
            auto session = std::make_shared<Session>(&broker, "churn-" + std::to_string(round++ % 3));
            session->subscribe("load/#", defaultOptions());
            session->connect(session);
            alive.push_back(session);
            if (alive.size() > 8) {
                alive.erase(alive.begin());
            }
        }
    });

    std::this_thread::sleep_for(std::chrono::milliseconds(300));
    stop = true;
    for (auto &thread : threads) {
        thread.join();
    }
    EXPECT_GT(published.load(), 0);
    EXPECT_EQ(broker.getConnectedClients(), 4);
}

} // namespace MQTT
