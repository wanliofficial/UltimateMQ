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
    session->resume(newClient, Version::MQTT5);

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
    session->deliver({"m/#"}, stale);

    // ...while a still-live message to the same topic is delivered normally.
    session->deliver({"m/#"}, Message{"m/t", std::string("new"), QoS::QOS_0});

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
    session->deliver({"q/#"}, stale);
    session->deliver({"q/#"}, Message{"q/t", std::string("new"), QoS::QOS_0});
    session->close(offline);  // persistent: the buffered queue is kept

    // Reconnect with a new client and drain the queue
    auto live = std::make_shared<RecordingClient>();
    session->resume(live, Version::MQTT5);
    session->flush();

    ASSERT_EQ(live->size(), 1u);
    EXPECT_EQ(std::string(live->delivered[0].payload.begin(), live->delivered[0].payload.end()), "new");
}

TEST_F(SessionLifecycleTest, SweepPurgesExpiredRetainedMessage)
{
    // A retained message whose expiry has already elapsed must not be stored
    // at all (MQTT-3.3.2-9): it is not retained and never delivered.
    Message retained{"rt", std::string("x"), QoS::QOS_0, /*retain=*/true};
    retained.hasExpiry = true;
    retained.expiryDeadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    broker.publish(retained);
    EXPECT_EQ(broker.getRetainedCount(), 0u);

    // A message that is still live is stored...
    Message live{"rt2", std::string("y"), QoS::QOS_0, /*retain=*/true};
    live.hasExpiry = true;
    live.expiryDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(60);
    broker.publish(live);
    EXPECT_EQ(broker.getRetainedCount(), 1u);

    // ...and a retained publish that has already expired REPLACES it with
    // nothing rather than storing an undeliverable message (MQTT-3.3.1-2 with
    // MQTT-3.3.2-9).
    live.expiryDeadline = std::chrono::steady_clock::now() - std::chrono::seconds(1);
    broker.publish(live);
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
    session->resume(second, Version::MQTT5);
    session->flush();

    ASSERT_EQ(second->size(), 1u);
    EXPECT_TRUE(second->delivered[0].dup);
    EXPECT_EQ(second->packetIds[0], packetId);

    // Once acknowledged it is not retransmitted again
    session->puback(packetId);
    auto third = std::make_shared<RecordingClient>();
    session->resume(third, Version::MQTT5);
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
    session->resume(second, Version::MQTT5);
    EXPECT_EQ(second->resentPubrecs, (std::vector<uint16_t>{7}));

    // Once the client's PUBREL is received the flow is done and nothing is resent
    session->pubrel(7);
    auto third = std::make_shared<RecordingClient>();
    session->resume(third, Version::MQTT5);
    EXPECT_TRUE(third->resentPubrecs.empty());
}

TEST_F(SessionLifecycleTest, ResumeResendsPubrelForOutgoingQos2AwaitingPubcomp)
{
    auto session = std::make_shared<Session>(&broker, "outq2");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);
    session->subscribe("out/q", optionsFor(QoS::QOS_2));

    // Deliver a QoS 2 message; it enters the outgoing inflight set.
    broker.publish(Message("out/q", "hello", QoS::QOS_2));
    ASSERT_EQ(first->size(), 1u);
    uint16_t packetId = first->packetIds[0];

    // The client acknowledges the PUBLISH with PUBREC; the broker now sends a
    // PUBREL and moves into the "waiting for PUBCOMP" leg (MQTT-4.3.3).
    ASSERT_EQ(session->pubrec(packetId), ReasonCode::SUCCESS);

    // The connection drops before the client's PUBCOMP arrives.
    session->close(first);
    EXPECT_EQ(broker.findSession("outq2"), session);

    // On reconnect the broker must re-send the PUBREL so the client can finish
    // the flow with PUBCOMP; the PUBLISH itself is not re-sent (MQTT-4.4.0-1).
    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);
    EXPECT_EQ(second->resentPubrels, (std::vector<uint16_t>{packetId}));
    EXPECT_TRUE(second->resentPubrecs.empty());
    EXPECT_EQ(second->size(), 0u);

    // Once the PUBCOMP arrives the flow is complete and nothing is re-sent.
    session->pubcomp(packetId);
    auto third = std::make_shared<RecordingClient>();
    session->resume(third, Version::MQTT5);
    EXPECT_TRUE(third->resentPubrels.empty());
}

TEST_F(SessionLifecycleTest, PubcompBeforePubrecClearsPendingPubrel)
{
    auto session = std::make_shared<Session>(&broker, "outq2b");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);
    session->subscribe("out/qb", optionsFor(QoS::QOS_2));

    broker.publish(Message("out/qb", "x", QoS::QOS_2));
    ASSERT_EQ(first->size(), 1u);
    uint16_t packetId = first->packetIds[0];
    ASSERT_EQ(session->pubrec(packetId), ReasonCode::SUCCESS);

    // A PUBCOMP without an intervening disconnect completes the flow.
    session->pubcomp(packetId);
    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);
    EXPECT_TRUE(second->resentPubrels.empty());
}

TEST_F(SessionLifecycleTest, InboundQos2PacketIdReuseIsRejectedWhileAwaitingPubrel)
{
    auto session = std::make_shared<Session>(&broker, "q2reuse");
    session->connect(session, /*persistent=*/false);
    test::attach(*session);

    ASSERT_EQ(session->publish(5, Message("q2/reuse", "first", QoS::QOS_2)),
              ReasonCode::SUCCESS);
    // A second QoS 2 PUBLISH reusing id 5 before the PUBREL would start a flow
    // whose PUBREL could never be paired (MQTT-3.3.4-1).
    EXPECT_EQ(session->publish(5, Message("q2/reuse", "second", QoS::QOS_2)),
              ReasonCode::PACKET_IDENTIFIER_IN_USE);
    // A different id is still accepted.
    EXPECT_EQ(session->publish(6, Message("q2/reuse", "third", QoS::QOS_2)),
              ReasonCode::SUCCESS);
    // After the PUBREL the id is free again.
    EXPECT_EQ(session->pubrel(5), ReasonCode::SUCCESS);
    EXPECT_EQ(session->publish(5, Message("q2/reuse", "fourth", QoS::QOS_2)),
              ReasonCode::SUCCESS);
}

TEST_F(SessionLifecycleTest, SpuriousPubrelReportsPacketIdentifierNotFound)
{
    auto session = std::make_shared<Session>(&broker, "q2stray");
    session->connect(session, /*persistent=*/false);
    test::attach(*session);

    // No QoS 2 flow ever started with id 42 (MQTT-4.3.3-5).
    EXPECT_EQ(session->pubrel(42), ReasonCode::PACKET_IDENTIFIER_NOT_FOUND);
}

TEST_F(SessionLifecycleTest, PubrecWithoutOutstandingDeliveryReportsNotFound)
{
    auto session = std::make_shared<Session>(&broker, "q2pubrec");
    session->connect(session, /*persistent=*/false);
    test::attach(*session);

    // Nothing was delivered under id 77, so the PUBREC is stale (MQTT-4.3.3-5).
    EXPECT_EQ(session->pubrec(77), ReasonCode::PACKET_IDENTIFIER_NOT_FOUND);
}

TEST_F(SessionLifecycleTest, ResumeResendsEveryPendingInboundQos2Pubrec)
{
    auto session = std::make_shared<Session>(&broker, "q2multi");
    session->connect(session, /*persistent=*/true);
    test::attach(*session);

    session->publish(3, Message("q/t", "a", QoS::QOS_2));
    session->publish(9, Message("q/t", "b", QoS::QOS_2));

    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);
    std::vector<uint16_t> expected{3, 9};
    EXPECT_EQ(second->resentPubrecs, expected);
}

TEST_F(SessionLifecycleTest, ResumeStopsThePreviousClient)
{
    auto session = std::make_shared<Session>(&broker, "handover");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);

    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);

    EXPECT_EQ(first->superseded.load(), 1);
}

TEST_F(SessionLifecycleTest, StaleCloseDoesNotTearDownResumedSession)
{
    auto session = std::make_shared<Session>(&broker, "race");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);

    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);

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
    session->deliverRetained("ret/t", options, /*isNewSubscription=*/true);
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
    session->resume(second, Version::MQTT5);
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

TEST_F(SessionLifecycleTest, UnsubscribeReportsWhetherASubscriptionExisted)
{
    auto session = std::make_shared<Session>(&broker, "uq");
    session->connect(session, /*persistent=*/false);
    session->subscribe("u/a", optionsFor());

    // An existing filter is removed and reports true (UNSUBACK Success)
    EXPECT_TRUE(session->unsubscribe("u/a"));
    // Removing it a second time removes nothing and reports false
    // (UNSUBACK No Subscription Existed, MQTT-3.11.3)
    EXPECT_FALSE(session->unsubscribe("u/a"));
    // A filter that was never subscribed also reports false
    EXPECT_FALSE(session->unsubscribe("u/never"));
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

TEST_F(SessionLifecycleTest, RejectedDeliveryReleasesInflightSlot)
{
    // MQTT-3.1.2-24: the broker must not send a packet larger than the client's
    // Maximum Packet Size. When a QoS 1 delivery is refused for that reason the
    // reserved packet id must be released, otherwise the un-acknowledgeable
    // message occupies the Receive Maximum window forever and blocks every later
    // delivery for the client.
    auto session = std::make_shared<Session>(&broker, "rcv-reject");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("rj/t", optionsFor(QoS::QOS_1));
    session->setReceiveMaximum(1);
    // The client refuses payloads larger than 4 bytes.
    client->refuseLargerThan = 4;

    // A big message is refused by the client -> the slot must be freed again.
    broker.publish(Message("rj/t", "too-big-payload", QoS::QOS_1));
    session->flush();
    EXPECT_EQ(client->size(), 0u);

    // Because the slot was released, a following small message is delivered.
    broker.publish(Message("rj/t", "ok", QoS::QOS_1));
    session->flush();
    ASSERT_EQ(client->size(), 1u);
    EXPECT_EQ(std::string(client->delivered[0].payload.begin(), client->delivered[0].payload.end()),
              "ok");
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

TEST_F(SessionLifecycleTest, NoLocalSubscriptionSuppressesOwnMessages)
{
    // MQTT-3.8.2-3: a subscription with the No Local option set must not
    // receive messages this same client published. Publishing goes through
    // Session::publish so the message is tagged with its originator.
    auto subscriber = std::make_shared<Session>(&broker, "nl-self");
    subscriber->connect(subscriber, /*persistent=*/true);
    auto client = test::attach(*subscriber);
    // Subscribe with No Local set
    SubscriptionOptions noLocalOpts{QoS::QOS_0, /*noLocal=*/true, false,
                                    RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    subscriber->subscribe("nl/t", noLocalOpts);

    // The SAME client publishes to its own session
    subscriber->publish(1, Message("nl/t", "own", QoS::QOS_0));
    subscriber->flush();

    // The no-local subscriber must NOT receive its own message
    EXPECT_EQ(client->size(), 0u);
}

TEST_F(SessionLifecycleTest, NormalSubscriptionStillReceivesOthers)
{
    // Control: without No Local, a subscriber receives other clients' messages.
    auto publisher = std::make_shared<Session>(&broker, "nrm-pub");
    publisher->connect(publisher, /*persistent=*/true);
    test::attach(*publisher);

    auto subscriber = std::make_shared<Session>(&broker, "nrm-sub");
    subscriber->connect(subscriber, /*persistent=*/true);
    auto subscriberClient = test::attach(*subscriber);
    subscriber->subscribe("nrm/t", optionsFor(QoS::QOS_0));

    publisher->publish(1, Message("nrm/t", "hello", QoS::QOS_0));
    publisher->flush();
    subscriber->flush();

    ASSERT_EQ(subscriberClient->size(), 1u);
    EXPECT_EQ(std::string(subscriberClient->delivered[0].payload.begin(),
                          subscriberClient->delivered[0].payload.end()), "hello");
}


TEST_F(SessionLifecycleTest, ResumeRetransmitsEverySubscriptionIdentifier)
{
    // MQTT-3.3.4-3 applies to retransmissions too: a QoS 1 message caused by
    // overlapping subscriptions is re-sent with the whole identifier list, not
    // with just the first one.
    auto session = std::make_shared<Session>(&broker, "sid-resume");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);
    SubscriptionOptions broad = optionsFor(QoS::QOS_1);
    broad.subscriptionIdentifier = 7;
    SubscriptionOptions narrow = optionsFor(QoS::QOS_1);
    narrow.subscriptionIdentifier = 9;
    session->subscribe("sid/#", broad);
    session->subscribe("sid/+", narrow);

    broker.publish(Message("sid/x", "hi", QoS::QOS_1));
    ASSERT_EQ(first->size(), 1u);
    // MQTT-3.3.4-4: the order of the identifiers is not significant.
    auto delivered = first->identifiersPerDelivery[0];
    std::sort(delivered.begin(), delivered.end());
    ASSERT_EQ(delivered, (SubscriptionIdentifiers{7, 9}));
    uint16_t packetId = first->packetIds[0];

    session->close(first);
    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);
    session->flush();

    ASSERT_EQ(second->size(), 1u);
    EXPECT_TRUE(second->delivered[0].dup);
    EXPECT_EQ(second->packetIds[0], packetId);
    auto resent = second->identifiersPerDelivery[0];
    std::sort(resent.begin(), resent.end());
    EXPECT_EQ(resent, delivered)
        << "the retransmission must repeat the same identifier list";
}

} // namespace MQTT
