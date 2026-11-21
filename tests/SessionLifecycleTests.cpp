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

TEST_F(SessionLifecycleTest, BackpressureDropsIncrementTheSysCounter)
{
    // An offline persistent session whose delivery queue fills (MAX_QUEUED_MESSAGES)
    // drops further messages. The drop must be observable through the
    // $SYS/broker/messages/dropped counter, not just a debug log line. The counter
    // is process-wide, so assert the delta around the burst.
    broker.setSysTopics(true);
    auto session = std::make_shared<Session>(&broker, "bp");
    session->connect(session, /*persistent=*/true);
    session->subscribe("bp/#", optionsFor());
    // No client attached: the session is offline, so flush() keeps the queue.

    auto droppedBefore = [&] {
        broker.refreshSysTopics();
        auto values = broker.sysTopicValues();
        auto it = values.find("$SYS/broker/messages/dropped");
        return it == values.end() ? 0u : std::stoul(it->second);
    }();

    // One more than the per-session queue cap (Session::MAX_QUEUED_MESSAGES,
    // private): that many queue, the final one is dropped under backpressure.
    const size_t queueCap = 1000;
    for (size_t i = 0; i <= queueCap; ++i) {
        broker.publish(Message{"bp/t", std::string("x"), QoS::QOS_0, /*retain=*/false});
    }

    auto droppedAfter = [&] {
        broker.refreshSysTopics();
        auto values = broker.sysTopicValues();
        auto it = values.find("$SYS/broker/messages/dropped");
        return it == values.end() ? 0u : std::stoul(it->second);
    }();

    EXPECT_GT(droppedAfter, droppedBefore)
        << "a full delivery queue must increment the dropped counter";
}

TEST_F(SessionLifecycleTest, DiscardCountsHeldDeliveriesAsDropped)
{
    // A discard throws away everything a session is holding for its client --
    // the offline/ready buffer (pendingDeliveries), Receive-Max-blocked messages
    // (blockedDeliveries) and unacknowledged inflight QoS 1/2. All of those are
    // genuine losses, so tearing a session down must move them into the $SYS
    // dropped counter rather than losing them silently. This is the path a
    // backpressure connection close takes (a slow reader whose write buffer
    // fills), so it must be observable like the queue drop. Persistent sessions
    // that merely go dormant are NOT discarded, so their buffer is not counted.
    broker.setSysTopics(true);
    auto droppedCounter = [&] {
        broker.refreshSysTopics();
        auto values = broker.sysTopicValues();
        auto it = values.find("$SYS/broker/messages/dropped");
        return it == values.end() ? 0u : std::stoul(it->second);
    };

    // An OFFLINE persistent session (no client bound): published messages stay
    // in its buffer rather than being flushed out, so they are "held" at discard.
    // It is persistent, so close() would keep it resumable (not discarded, not
    // counted); we call discard() directly to model the teardown that a
    // backpressure connection close performs on a non-resumable session.
    auto session = std::make_shared<Session>(&broker, "bp-discard");
    session->connect(session, /*persistent=*/true);
    session->subscribe("bp-discard/#", optionsFor());
    for (size_t i = 0; i < 10; ++i) {
        broker.publish(Message{"bp-discard/t", std::string("x"), QoS::QOS_0});
    }
    // No client attached: the ten messages sit in the offline buffer, held.
    ASSERT_EQ(session->droppedOnDiscardForTest(), 10u);

    const auto beforeDiscard = droppedCounter();
    session->discard();
    const auto afterDiscard = droppedCounter();
    EXPECT_EQ(afterDiscard - beforeDiscard, 10u)
        << "discarding a session must count every held delivery as dropped";
}

TEST_F(SessionLifecycleTest, DropOldestKeepsTheNewestWhenTheQueueIsFull)
{
    // FLOWMQ_QUEUE_DROP=oldest: when the delivery queue is full the broker evicts
    // the oldest buffered message to admit the newest, so a reconnecting client
    // sees the most recent traffic rather than a stale backlog. With the cap at
    // Session::MAX_QUEUED_MESSAGES (1000), publishing 1001 distinct messages must
    // drop index 0 and keep 1..1000.
    broker.setQueueDropPolicy(QueueDropPolicy::DropOldest);
    auto session = std::make_shared<Session>(&broker, "oldest");
    session->connect(session, /*persistent=*/true);
    session->subscribe("oldest/#", optionsFor());

    for (size_t i = 0; i <= 1000; ++i) {
        broker.publish(Message{"oldest/t", std::to_string(i), QoS::QOS_0,
                               /*retain=*/false});
    }

    // Reconnect a fresh client (resume, not bindClient, so the offline buffer is
    // kept) and observe which payloads survived the eviction.
    auto client = std::make_shared<RecordingClient>();
    session->resume(client, Version::MQTT5);
    session->flush();

    ASSERT_EQ(client->size(), 1000u);
    auto payload = [](const Message &m) {
        return std::string(m.payload.begin(), m.payload.end());
    };
    // The newest 1000 survive; the oldest (index 0) was evicted.
    EXPECT_EQ(payload(client->delivered.front()), "1");
    EXPECT_EQ(payload(client->delivered.back()), "1000");
}

TEST_F(SessionLifecycleTest, DropNewestKeepsTheOldestWhenTheQueueIsFull)
{
    // The default policy (and FLOWMQ_QUEUE_DROP=newest) drops the incoming
    // message, so the buffered history is kept: index 0 survives, the last
    // (index 1000) is dropped. This is the mirror of the drop-oldest test and
    // guards the default against regressing.
    broker.setQueueDropPolicy(QueueDropPolicy::DropNewest);
    auto session = std::make_shared<Session>(&broker, "newest");
    session->connect(session, /*persistent=*/true);
    session->subscribe("newest/#", optionsFor());

    for (size_t i = 0; i <= 1000; ++i) {
        broker.publish(Message{"newest/t", std::to_string(i), QoS::QOS_0,
                               /*retain=*/false});
    }

    auto client = std::make_shared<RecordingClient>();
    session->resume(client, Version::MQTT5);
    session->flush();

    ASSERT_EQ(client->size(), 1000u);
    auto payload = [](const Message &m) {
        return std::string(m.payload.begin(), m.payload.end());
    };
    EXPECT_EQ(payload(client->delivered.front()), "0");
    EXPECT_EQ(payload(client->delivered.back()), "999");
}

TEST_F(SessionLifecycleTest, DropOldestWithQoS1DoesNotLeakTheFlowControlWindow)
{
    // The subtle case: a QoS 1 message sitting in the delivery queue has already
    // claimed an inflight packet-id slot. Evicting it (drop-oldest) must release
    // that slot, or the Receive Maximum window leaks. The invariant to guard:
    // after a burst that evicts, every queued QoS1 message holds exactly one
    // inflight slot and every evicted one released its own, so the live inflight
    // count equals the number of messages the queue actually kept.
    broker.setQueueDropPolicy(QueueDropPolicy::DropOldest);
    auto session = std::make_shared<Session>(&broker, "oldestqos1");
    session->connect(session, /*persistent=*/true);
    session->subscribe("oldestqos1/#", optionsFor(QoS::QOS_1));
    // Make the flow-control window exactly the queue cap so a leaked slot (an
    // evicted message whose id was never released) would over-subscribe it and
    // force the incoming message into the blocked queue.
    session->setReceiveMaximum(1000);

    // One more than the cap: the oldest is evicted on the final publish.
    for (size_t i = 0; i <= 1000; ++i) {
        broker.publish(Message{"oldestqos1/t", std::to_string(i), QoS::QOS_1,
                               /*retain=*/false});
    }

    auto client = std::make_shared<RecordingClient>();
    session->resume(client, Version::MQTT5);
    session->flush();

    // The queue kept a full cap of messages...
    ASSERT_EQ(client->size(), 1000u);
    // ...and the flow-control window was not over-subscribed. With Receive
    // Maximum equal to the queue cap, a leaked slot (an evicted QoS1 message
    // whose id release was skipped) would leave the window full, forcing the
    // incoming message into the blocked queue instead of admitting it. The
    // correct build releases the evicted slot, so nothing is left blocked.
    EXPECT_EQ(session->blockedCountForTest(), 0u)
        << "evicting a queued QoS1 message must release its flow-control slot";
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

TEST_F(SessionLifecycleTest, PacketIdsAreNotReusedWhileStillInFlight)
{
    // MQTT-2.2.1-4: a Server MUST assign a non-zero Packet Identifier that is
    // *currently unused* to every new QoS>0 PUBLISH it sends. The identifier
    // space is 1..65535 and wraps, so the allocator must skip identifiers that
    // are still unacknowledged: handing one out again would make the client's
    // PUBACK ambiguous and could silently drop a message.
    auto session = std::make_shared<Session>(&broker, "wrap");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("wrap/t", optionsFor(QoS::QOS_1));

    // Walk the counter up to the very end of the identifier space, then leave a
    // message outstanding at 65535 while the counter wraps back to 1.
    session->setNextPacketIdForTest(65534);
    broker.publish(Message("wrap/t", "a", QoS::QOS_1));   // id 65534
    broker.publish(Message("wrap/t", "b", QoS::QOS_1));   // id 65535
    session->flush();
    ASSERT_EQ(client->size(), 2u);
    const uint16_t firstId = client->packetIds[0];
    const uint16_t secondId = client->packetIds[1];
    EXPECT_EQ(firstId, 65534u);
    EXPECT_EQ(secondId, 65535u);

    // Acknowledge 65534 but leave 65535 outstanding, then force the counter to
    // wrap past it. The next identifier must skip the still-in-flight one.
    session->puback(firstId);
    session->setNextPacketIdForTest(65535);
    broker.publish(Message("wrap/t", "c", QoS::QOS_1));
    session->flush();
    ASSERT_EQ(client->size(), 3u);
    EXPECT_NE(client->packetIds[2], secondId)
        << "the allocator reused a Packet Identifier that is still in flight";
    EXPECT_NE(client->packetIds[2], 0);
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

TEST_F(SessionLifecycleTest, PubrecRejectionEndsTheFlowWithoutPubrel)
{
    // A PUBREC with a Reason Code of 0x80 or greater means the client refused the
    // message. MQTT-4.3.3 only lets a sender answer a PUBREC below 0x80 with a
    // PUBREL, and MQTT-4.4.0-2 says such a rejection acknowledges the PUBLISH,
    // which must never be retransmitted. The identifier is reusable at once.
    auto session = std::make_shared<Session>(&broker, "q2reject");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("rej/q", optionsFor(QoS::QOS_2));

    broker.publish(Message("rej/q", "hello", QoS::QOS_2));
    ASSERT_EQ(client->size(), 1u);
    const uint16_t packetId = client->packetIds[0];

    const auto reason = session->pubrec(packetId, ReasonCode::QUOTA_EXCEEDED);
    EXPECT_FALSE(reason.has_value())
        << "a rejecting PUBREC must not be answered with a PUBREL";

    // Nothing is left to retransmit on reconnect: no PUBREL, no PUBLISH.
    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);
    session->flush();
    EXPECT_TRUE(second->resentPubrels.empty())
        << "a rejected PUBLISH was remembered for a PUBREL retransmission";
    EXPECT_EQ(second->size(), 0u)
        << "a rejected PUBLISH must not be retransmitted";
}

TEST_F(SessionLifecycleTest, PubrecAcceptanceStillSendsAPubrel)
{
    auto session = std::make_shared<Session>(&broker, "q2accept");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);
    session->subscribe("acc/q", optionsFor(QoS::QOS_2));

    broker.publish(Message("acc/q", "hello", QoS::QOS_2));
    ASSERT_EQ(client->size(), 1u);
    const auto reason = session->pubrec(client->packetIds[0]);
    ASSERT_TRUE(reason.has_value());
    EXPECT_EQ(*reason, ReasonCode::SUCCESS);
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

TEST_F(SessionLifecycleTest, ResumeDoesNotDuplicateOfflineBufferedQos1Messages)
{
    // A QoS>0 message buffered while the session is offline lives in BOTH
    // inflightMessages and pendingDeliveries. resume() must re-queue it only
    // once, otherwise the reconnecting client receives a duplicate copy.
    auto session = std::make_shared<Session>(&broker, "offline-dup");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);
    session->subscribe("od/t", optionsFor(QoS::QOS_1));
    session->close(first);  // offline: client unbound, queue retained

    broker.publish(Message("od/t", "m1", QoS::QOS_1));
    broker.publish(Message("od/t", "m2", QoS::QOS_1));

    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);
    session->flush();

    ASSERT_EQ(second->size(), 2u) << "each buffered message must arrive exactly once";
    EXPECT_EQ(std::string(second->delivered[0].payload.begin(), second->delivered[0].payload.end()),
              "m1");
    EXPECT_EQ(std::string(second->delivered[1].payload.begin(), second->delivered[1].payload.end()),
              "m2");
    EXPECT_NE(second->packetIds[0], second->packetIds[1]);
}

TEST_F(SessionLifecycleTest, ResumeKeepsUnackedRetransmissionAlongsideOfflineBuffer)
{
    // The duplicate fix must not drop the other half of resume(): a message
    // that WAS sent but never acknowledged still needs a DUP retransmission,
    // while one buffered offline is a first delivery (DUP=0). §4.6 also wants
    // the retransmission to go out before the offline-queued message, because
    // the original send order was "live" first, then "offline".
    auto session = std::make_shared<Session>(&broker, "mixed");
    session->connect(session, /*persistent=*/true);
    auto first = test::attach(*session);
    session->subscribe("mix/t", optionsFor(QoS::QOS_1));

    broker.publish(Message("mix/t", "live", QoS::QOS_1));
    ASSERT_EQ(first->size(), 1u);
    uint16_t liveId = first->packetIds[0];
    session->close(first);  // dropped before the PUBACK

    broker.publish(Message("mix/t", "offline", QoS::QOS_1));

    auto second = std::make_shared<RecordingClient>();
    session->resume(second, Version::MQTT5);
    session->flush();

    ASSERT_EQ(second->size(), 2u);
    // The unacknowledged retransmission leads; the offline buffer follows.
    const auto &firstDelivered = second->delivered[0];
    const std::string firstPayload(firstDelivered.payload.begin(),
                                   firstDelivered.payload.end());
    EXPECT_EQ(firstPayload, "live");
    EXPECT_TRUE(firstDelivered.dup) << "an unacknowledged sent message is a retransmission";
    EXPECT_EQ(second->packetIds[0], liveId);
    const auto &secondDelivered = second->delivered[1];
    const std::string secondPayload(secondDelivered.payload.begin(),
                                    secondDelivered.payload.end());
    EXPECT_EQ(secondPayload, "offline");
    EXPECT_FALSE(secondDelivered.dup) << "a message buffered offline is a first transmission";
}

TEST_F(SessionLifecycleTest, ReceiveMaximumCountsQos2AwaitingPubcomp)
{
    // MQTT-4.9: a QoS 2 PUBLISH is outstanding until PUBCOMP, not PUBREC. With
    // Receive Maximum 1, the second PUBLISH must stay blocked while the first
    // is in the PUBREL/PUBCOMP leg.
    auto session = std::make_shared<Session>(&broker, "q2-window");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("q2w/t", optionsFor(QoS::QOS_2));
    session->setReceiveMaximum(1);

    broker.publish(Message("q2w/t", "a", QoS::QOS_2));
    broker.publish(Message("q2w/t", "b", QoS::QOS_2));
    session->flush();
    ASSERT_EQ(client->size(), 1u);
    uint16_t idA = client->packetIds[0];

    session->pubrec(idA, ReasonCode::SUCCESS);  // PUBCOMP still outstanding
    session->flush();
    EXPECT_EQ(client->size(), 1u) << "PUBREC must not free the Receive Maximum slot";

    session->pubcomp(idA);
    session->flush();
    ASSERT_EQ(client->size(), 2u);
    EXPECT_EQ(std::string(client->delivered[1].payload.begin(), client->delivered[1].payload.end()),
              "b");
}

TEST_F(SessionLifecycleTest, ReceiveMaximumSlotFreedByRejectedPubrec)
{
    // A rejected PUBREC (>=0x80) ends the QoS 2 flow immediately, so it must
    // free the slot and let the next blocked message through.
    auto session = std::make_shared<Session>(&broker, "q2-reject");
    session->connect(session, /*persistent=*/true);
    auto client = test::attach(*session);
    session->subscribe("q2r/t", optionsFor(QoS::QOS_2));
    session->setReceiveMaximum(1);

    broker.publish(Message("q2r/t", "a", QoS::QOS_2));
    broker.publish(Message("q2r/t", "b", QoS::QOS_2));
    session->flush();
    ASSERT_EQ(client->size(), 1u);
    uint16_t idA = client->packetIds[0];

    session->pubrec(idA, static_cast<ReasonCode>(0x80));
    session->flush();
    ASSERT_EQ(client->size(), 2u);
    EXPECT_EQ(std::string(client->delivered[1].payload.begin(), client->delivered[1].payload.end()),
              "b");
}

} // namespace MQTT
