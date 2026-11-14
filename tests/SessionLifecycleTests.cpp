#include <gtest/gtest.h>
#include "../src/Broker.h"
#include "../src/Session.h"
#include "TestClient.h"
#include <atomic>
#include <chrono>
#include <memory>
#include <thread>
#include <vector>

namespace MQTT {

namespace {
SubscriptionOptions optionsFor(QoS qos = QoS::QOS_0, RetainHandling handling =
                                    RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES) {
    return SubscriptionOptions{qos, false, false, handling};
}
}

class SessionLifecycleTest : public ::testing::Test {
protected:
    Broker broker;
};

TEST_F(SessionLifecycleTest, NonPersistentSessionIsForgottenAfterClose)
{
    auto session = std::make_shared<Session>(&broker, "np");
    session->connect(session, /*persistent=*/false);
    EXPECT_EQ(broker.findSession("np"), session);
    EXPECT_EQ(broker.getConnectedClients(), 1);

    auto client = test::attach(*session);
    session->close(client);

    EXPECT_EQ(broker.findSession("np"), nullptr);
    EXPECT_EQ(broker.getConnectedClients(), 0);
}

TEST_F(SessionLifecycleTest, PersistentSessionSurvivesClose)
{
    auto session = std::make_shared<Session>(&broker, "persist");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("p/t", optionsFor());

    session->close(client);

    // The session is still registered (so it can be resumed) but no longer counted
    EXPECT_EQ(broker.findSession("persist"), session);
    EXPECT_EQ(broker.getConnectedClients(), 0);
}

TEST_F(SessionLifecycleTest, DiscardDropsPersistentSession)
{
    auto session = std::make_shared<Session>(&broker, "drop");
    session->connect(session, /*persistent=*/true);
    session->subscribe("d/t", optionsFor());

    session->discard();

    EXPECT_EQ(broker.findSession("drop"), nullptr);
}

TEST_F(SessionLifecycleTest, ArmExpiryMarksSessionExpiredLater)
{
    auto session = std::make_shared<Session>(&broker, "exp");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->close(client);  // persistent: kept, but now disconnected

    auto now = std::chrono::steady_clock::now();
    EXPECT_FALSE(session->isExpired(now));

    session->armExpiry(std::chrono::seconds(60));
    EXPECT_FALSE(session->isExpired(now));
    EXPECT_TRUE(session->isExpired(now + std::chrono::seconds(61)));
}

TEST_F(SessionLifecycleTest, ClearExpiryCancelsPendingExpiry)
{
    auto session = std::make_shared<Session>(&broker, "exp2");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->close(client);

    session->armExpiry(std::chrono::seconds(60));
    auto future = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    EXPECT_TRUE(session->isExpired(future));

    session->clearExpiry();
    EXPECT_FALSE(session->isExpired(future));
}

TEST_F(SessionLifecycleTest, ResumeCancelsPendingExpiry)
{
    // A reconnecting client revives an expired session: it must no longer be
    // swept out from under the new connection.
    auto session = std::make_shared<Session>(&broker, "exp3");
    session->connect(session, /*persistent=*/true);
    session->subscribe("r/#", optionsFor());
    auto client = test::attach(*session);
    session->close(client);
    session->armExpiry(std::chrono::seconds(60));

    // Simulate a reconnect
    auto newClient = test::attach(*session);
    session->resume(newClient);

    auto future = std::chrono::steady_clock::now() + std::chrono::seconds(120);
    EXPECT_FALSE(session->isExpired(future));
}

TEST_F(SessionLifecycleTest, SweepDropsAnExpiredSession)
{
    auto session = std::make_shared<Session>(&broker, "sweep");
    session->connect(session, /*persistent=*/true);
    session->subscribe("s/#", optionsFor());
    auto client = test::attach(*session);
    session->close(client);
    session->armExpiry(std::chrono::seconds(1));
    EXPECT_EQ(broker.findSession("sweep"), session);

    // A still-connected session is never swept even if it somehow has an expiry
    auto live = std::make_shared<Session>(&broker, "live");
    live->connect(live, /*persistent=*/true);
    live->armExpiry(std::chrono::seconds(0));  // 0 cancels; stays armed=false
    broker.sweepExpiredSessions();
    EXPECT_EQ(broker.findSession("live"), live);

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    broker.sweepExpiredSessions();
    EXPECT_EQ(broker.findSession("sweep"), nullptr);
}

TEST_F(SessionLifecycleTest, ExpiredMessageIsNotDelivered)
{
    auto session = std::make_shared<Session>(&broker, "mx");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);
    session->subscribe("m/#", optionsFor());

    // A message whose expiry deadline is already past must be dropped...
    Message stale{"m/t", std::string("old"), QoS::QOS_0};
    stale.hasExpiry = true;
    stale.expiryDeadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    session->deliver("m/t", stale);

    // ...while a still-live message to the same topic is delivered normally.
    session->deliver("m/t", Message{"m/t", std::string("new"), QoS::QOS_0});

    session->flush();

    ASSERT_EQ(client->size(), 1u);
    EXPECT_EQ(std::string(client->delivered[0].payload.begin(), client->delivered[0].payload.end()), "new");
}

TEST_F(SessionLifecycleTest, ExpiredQueuedMessageIsNotFlushed)
{
    // An offline persistent session buffers messages; an expired one must be
    // dropped when the queue is finally drained on reconnect.
    auto session = std::make_shared<Session>(&broker, "mq");
    session->connect(session, /*persistent=*/true);
    // Bind the offline client directly: bindClient (via test::attach) would
    // clear the buffer we want to keep, and reconnect uses resume() anyway.
    auto offline = std::make_shared<RecordingClient>();
    session->bindClient(offline);
    session->subscribe("q/#", optionsFor());

    Message stale{"q/t", std::string("old"), QoS::QOS_0};
    stale.hasExpiry = true;
    stale.expiryDeadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    session->deliver("q/t", stale);
    session->deliver("q/t", Message{"q/t", std::string("new"), QoS::QOS_0});
    session->close(offline);  // persistent: the buffered queue is kept

    // Reconnect with a new client and drain the queue
    auto live = std::make_shared<RecordingClient>();
    session->resume(live);
    session->flush();

    ASSERT_EQ(live->size(), 1u);
    EXPECT_EQ(std::string(live->delivered[0].payload.begin(), live->delivered[0].payload.end()), "new");
}

TEST_F(SessionLifecycleTest, SweepPurgesExpiredRetainedMessage)
{
    Message retained{"rt", std::string("x"), QoS::QOS_0, /*retain=*/true};
    retained.hasExpiry = true;
    retained.expiryDeadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    broker.publish(retained);
    EXPECT_EQ(broker.getRetainedCount(), 1u);

    broker.sweepExpiredSessions();
    EXPECT_EQ(broker.getRetainedCount(), 0u);
}

TEST_F(SessionLifecycleTest, ResumeRetransmitsUnackedMessagesWithDup)
{
    auto session = std::make_shared<Session>(&broker, "q1");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);
    session->subscribe("q/t", optionsFor(QoS::QOS_1));

    broker.publish(Message("q/t", "hi", QoS::QOS_1));
    ASSERT_EQ(first->size(), 1u);
    EXPECT_FALSE(first->delivered[0].dup);
    uint16_t packetId = first->packetIds[0];

    // The connection drops before acknowledging the message
    session->close(first);
    EXPECT_EQ(broker.findSession("q1"), session);

    // Reconnect: the message is retransmitted with the same packet id, DUP=1
    auto second = std::make_shared<RecordingClient>();
    session->resume(second);
    session->flush();

    ASSERT_EQ(second->size(), 1u);
    EXPECT_TRUE(second->delivered[0].dup);
    EXPECT_EQ(second->packetIds[0], packetId);

    // Once acknowledged it is not retransmitted again
    session->puback(packetId);
    auto third = std::make_shared<RecordingClient>();
    session->resume(third);
    session->flush();
    EXPECT_EQ(third->size(), 0u);
}

TEST_F(SessionLifecycleTest, ResumeResendsPubrecForInboundQos2AwaitingPubrel)
{
    auto session = std::make_shared<Session>(&broker, "q2");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);

    // An inbound QoS 2 PUBLISH: the broker publishes to subscribers and records
    // that it is now awaiting this client's PUBREL (packet id 7).
    ASSERT_EQ(session->publish(7, Message("q/t", "payload", QoS::QOS_2)),
              ReasonCode::SUCCESS);

    // The connection drops before the client's PUBREL arrives
    session->close(first);
    EXPECT_EQ(broker.findSession("q2"), session);

    // On reconnect the broker must resend the PUBREC so the client can complete
    // the flow with its PUBREL, using the original packet id.
    auto second = std::make_shared<RecordingClient>();
    session->resume(second);
    EXPECT_EQ(second->resentPubrecs, (std::vector<uint16_t>{7}));

    // Once the client's PUBREL is received the flow is done and nothing is resent
    session->pubrel(7);
    auto third = std::make_shared<RecordingClient>();
    session->resume(third);
    EXPECT_TRUE(third->resentPubrecs.empty());
}

TEST_F(SessionLifecycleTest, ResumeResendsEveryPendingInboundQos2Pubrec)
{
    auto session = std::make_shared<Session>(&broker, "q2multi");
    session->connect(session, /*persistent=*/true);
    test::attach(*session);

    session->publish(3, Message("q/t", "a", QoS::QOS_2));
    session->publish(9, Message("q/t", "b", QoS::QOS_2));

    auto second = std::make_shared<RecordingClient>();
    session->resume(second);
    std::vector<uint16_t> expected{3, 9};
    EXPECT_EQ(second->resentPubrecs, expected);
}

TEST_F(SessionLifecycleTest, ResumeStopsThePreviousClient)
{
    auto session = std::make_shared<Session>(&broker, "handover");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);

    auto second = std::make_shared<RecordingClient>();
    session->resume(second);

    EXPECT_EQ(first->superseded.load(), 1);
}

TEST_F(SessionLifecycleTest, StaleCloseDoesNotTearDownResumedSession)
{
    auto session = std::make_shared<Session>(&broker, "race");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);

    auto second = std::make_shared<RecordingClient>();
    session->resume(second);

    // The old connection finishing late must not unregister the live session
    session->close(first);
    EXPECT_EQ(broker.findSession("race"), session);
}

TEST_F(SessionLifecycleTest, RetainedMessageIsQueuedThenDeliveredByFlush)
{
    broker.publish(MQTT::Message("ret/t", "old", MQTT::QoS::QOS_0, true));
    auto session = std::make_shared<Session>(&broker, "retained-sub");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);

    auto options = optionsFor(QoS::QOS_0, RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE);
    session->subscribe("ret/t", options);
    session->deliverRetained("ret/t", options);
    session->flush();

    ASSERT_EQ(client->size(), 1u);
    EXPECT_EQ(client->delivered[0].topic, "ret/t");
    EXPECT_TRUE(client->delivered[0].retain);
}

TEST_F(SessionLifecycleTest, CloseNotifiesExactlyOnce)
{
    auto session = std::make_shared<Session>(&broker, "disc");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);

    std::atomic<int> disconnects{0};
    session->setDisconnectCallback([&disconnects]() { disconnects++; });

    session->close(client);
    // A repeated close from a stale connection must be a no-op
    session->close(client);

    EXPECT_EQ(disconnects.load(), 1);
    EXPECT_EQ(broker.getConnectedClients(), 0);
}

TEST_F(SessionLifecycleTest, PersistentSessionBuffersMessagesWhileOffline)
{
    auto session = std::make_shared<Session>(&broker, "offline");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);
    session->subscribe("o/t", optionsFor());
    session->close(first);  // goes offline, but the session survives

    // A message published while the client is away is buffered
    broker.publish(Message("o/t", "buffered"));

    auto second = std::make_shared<RecordingClient>();
    session->resume(second);
    session->flush();

    ASSERT_EQ(second->size(), 1u);
    EXPECT_EQ(std::string(second->delivered[0].payload.begin(), second->delivered[0].payload.end()),
              "buffered");
}

TEST_F(SessionLifecycleTest, NonPersistentCloseRemovesBrokerSubscriptions)
{    auto session = std::make_shared<Session>(&broker, "npsub");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);
    session->subscribe("np/t", optionsFor());
    EXPECT_TRUE(broker.isSubscribed("npsub", "np/t"));

    session->close(client);

    // A gone session must not leave a routing entry behind
    EXPECT_FALSE(broker.isSubscribed("npsub", "np/t"));
}

TEST_F(SessionLifecycleTest, SubscriptionCountAndLookupTrackFilters)
{
    auto session = std::make_shared<Session>(&broker, "count");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);

    EXPECT_EQ(session->getSubscriptionCount(), 0u);
    EXPECT_FALSE(session->hasSubscription("c/a"));

    session->subscribe("c/a", optionsFor());
    session->subscribe("c/b", optionsFor());
    EXPECT_EQ(session->getSubscriptionCount(), 2u);
    EXPECT_TRUE(session->hasSubscription("c/a"));
    EXPECT_TRUE(session->hasSubscription("c/b"));

    // Re-subscribing the same filter does not increase the count
    session->subscribe("c/a", optionsFor());
    EXPECT_EQ(session->getSubscriptionCount(), 2u);

    session->unsubscribe("c/a");
    EXPECT_EQ(session->getSubscriptionCount(), 1u);
    EXPECT_FALSE(session->hasSubscription("c/a"));
}

TEST_F(SessionLifecycleTest, SubscriptionLimitDefaultsAndAppliesFromBroker)
{
    // A fresh session starts with the documented default limit
    auto session = std::make_shared<Session>(&broker, "limit");
    EXPECT_EQ(session->getMaxSubscriptions(), DEFAULT_MAX_SUBSCRIPTIONS);

    // insertSession (called on connect) applies the broker's configured limit
    broker.setMaxSubscriptions(3);
    session->connect(session, /*persistent=*/false);
    EXPECT_EQ(session->getMaxSubscriptions(), 3u);
}

TEST_F(SessionLifecycleTest, DiscardingAStaleSessionKeepsTheLiveOne)
{
    auto first = std::make_shared<Session>(&broker, "same-id");
    auto client = test::attach(*first);
    first->subscribe("f/x", optionsFor());

    // A clean start installs a fresh session under the same client id
    auto second = std::make_shared<Session>(&broker, "same-id");
    second->connect(second, /*persistent=*/false);
    EXPECT_EQ(broker.findSession("same-id"), second);

    // Discarding the stale session must not remove the live one
    first->discard();
    EXPECT_EQ(broker.findSession("same-id"), second);
}

TEST_F(SessionLifecycleTest, ConcurrentPublishAndReconnectDoesNotCrash)
{
    std::atomic<bool> stop{false};
    std::atomic<int> published{0};
    std::vector<std::thread> threads;
    std::vector<std::shared_ptr<Session>> subscribers;
    std::vector<std::shared_ptr<RecordingClient>> clients;

    for (int i = 0; i < 4; ++i) {
        auto session = std::make_shared<Session>(&broker, "load-" + std::to_string(i));
        subscribers.push_back(session);
        clients.push_back(test::attach(*session));
        session->subscribe("load/#", optionsFor());
        session->connect(session, /*persistent=*/false);
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
    // Churn: connect and drop sessions with colliding client ids while messages
    // are being published
    threads.emplace_back([this, &stop]() {
        int round = 0;
        std::vector<std::shared_ptr<RecordingClient>> alive;
        while (!stop) {
            auto session = std::make_shared<Session>(&broker, "churn-" + std::to_string(round++ % 3));
            session->subscribe("load/#", optionsFor());
            session->connect(session, /*persistent=*/false);
            auto client = test::attach(*session);
            alive.push_back(client);
            session->close(client);
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

TEST_F(SessionLifecycleTest, ReceiveMaximumCapsOutstandingQos1Messages)
{
    // A client that advertises Receive Maximum = 2 may have at most 2 QoS 1
    // messages outstanding; the 3rd and 4th are parked until the client acks.
    auto session = std::make_shared<Session>(&broker, "rcv1");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("r/t", optionsFor(QoS::QOS_1));
    session->setReceiveMaximum(2);

    broker.publish(Message("r/t", "m1", QoS::QOS_1));
    broker.publish(Message("r/t", "m2", QoS::QOS_1));
    broker.publish(Message("r/t", "m3", QoS::QOS_1));
    broker.publish(Message("r/t", "m4", QoS::QOS_1));
    session->flush();

    // Only the first two are delivered (window full); two remain blocked
    ASSERT_EQ(client->size(), 2u);
    EXPECT_EQ(std::string(client->delivered[0].payload.begin(), client->delivered[0].payload.end()), "m1");
    EXPECT_EQ(std::string(client->delivered[1].payload.begin(), client->delivered[1].payload.end()), "m2");

    // Acknowledging the first frees a slot; the next blocked message flows in
    session->puback(client->packetIds[0]);
    session->flush();
    ASSERT_EQ(client->size(), 3u);
    EXPECT_EQ(std::string(client->delivered[2].payload.begin(), client->delivered[2].payload.end()), "m3");
}

TEST_F(SessionLifecycleTest, ReceiveMaximumDoesNotThrottleQos0)
{
    // QoS 0 has no acknowledgement and is therefore never flow-controlled, even
    // with a Receive Maximum of 1.
    auto session = std::make_shared<Session>(&broker, "rcv0");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("z/t", optionsFor(QoS::QOS_0));
    session->setReceiveMaximum(1);

    broker.publish(Message("z/t", "a", QoS::QOS_0));
    broker.publish(Message("z/t", "b", QoS::QOS_0));
    broker.publish(Message("z/t", "c", QoS::QOS_0));
    session->flush();

    EXPECT_EQ(client->size(), 3u);
}

TEST_F(SessionLifecycleTest, ReceiveMaximumWindowReleasesOnPubcomp)
{
    // The same gating applies to QoS 2 and releases on PUBREC->PUBCOMP.
    auto session = std::make_shared<Session>(&broker, "rcv2");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("q2/t", optionsFor(QoS::QOS_2));
    session->setReceiveMaximum(1);

    broker.publish(Message("q2/t", "a", QoS::QOS_2));
    broker.publish(Message("q2/t", "b", QoS::QOS_2));
    session->flush();

    ASSERT_EQ(client->size(), 1u);
    EXPECT_EQ(std::string(client->delivered[0].payload.begin(), client->delivered[0].payload.end()), "a");

    // The PUBREC the broker sent is acked by the client's PUBREL; the flow
    // completes with PUBCOMP, which frees the window slot.
    session->pubrel(client->packetIds[0]);
    session->pubcomp(client->packetIds[0]);
    session->flush();

    ASSERT_EQ(client->size(), 2u);
    EXPECT_EQ(std::string(client->delivered[1].payload.begin(), client->delivered[1].payload.end()), "b");
}

TEST_F(SessionLifecycleTest, DefaultReceiveMaximumIsUnlimited)
{
    // Without setReceiveMaximum the default leaves behaviour unchanged: many
    // QoS 1 messages are all delivered.
    auto session = std::make_shared<Session>(&broker, "rcvdef");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("d/t", optionsFor(QoS::QOS_1));

    for (int i = 0; i < 50; ++i) {
        broker.publish(Message("d/t", "m" + std::to_string(i), QoS::QOS_1));
    }
    session->flush();

    EXPECT_EQ(client->size(), 50u);
}

} // namespace MQTT
