#include <gtest/gtest.h>

#include "../src/Broker.h"
#include "../src/Session.h"
#include "../src/SysTopics.h"
#include "../src/Topic.h"

namespace MQTT {

TEST(SysTopicsTest, RecognisesTheMonitoringTree)
{
    EXPECT_TRUE(SysTopics::isSysTopic("$SYS/broker/load"));
    EXPECT_TRUE(SysTopics::isSysTopic("$SYS/"));
    // An ordinary user topic is not part of the tree.
    EXPECT_FALSE(SysTopics::isSysTopic("sensors/temperature"));
    EXPECT_FALSE(SysTopics::isSysTopic("SYS/broker/load"));
    EXPECT_FALSE(SysTopics::isSysTopic(""));
}

TEST(SysTopicsTest, LeadingLevelWildcardDoesNotMatchTheTree)
{
    // MQTT-4.7.2-1: a filter whose first level is a wildcard never matches a
    // "$..." topic, so "#" or "+/..." must not reach the monitoring tree.
    EXPECT_EQ(SysTopics::matchedCount("#"), 0u);
    EXPECT_EQ(SysTopics::matchedCount("+"), 0u);
    EXPECT_EQ(SysTopics::matchedCount("+/broker/load"), 0u);
    // An explicit "$SYS/..." filter reaches the tree.
    EXPECT_EQ(SysTopics::matchedCount("$SYS/#"), SysTopics::allTopics().size());
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/load"), 1u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/clients/connected"), 1u);
    // A filter that matches none of the published topics.
    EXPECT_EQ(SysTopics::matchedCount("$SYS/nope/nada"), 0u);
}

TEST(SysTopicsTest, PublishesEveryMonitoringTopic)
{
    // The broker refreshes exactly the topics it advertises, so each name in
    // allTopics() must produce a payload.
    for (const std::string &topic : SysTopics::allTopics()) {
        EXPECT_FALSE(SysTopics::payload(topic, 1, 1, 1).empty())
            << "no payload for " << topic;
    }
}

TEST(SysTopicsTest, CountsAreReportedAsDecimalNumbers)
{
    EXPECT_EQ(SysTopics::payload("$SYS/broker/clients/connected", 3, 3, 0), "3");
    EXPECT_EQ(SysTopics::payload("$SYS/broker/clients/total", 0, 7, 0), "7");
    EXPECT_EQ(SysTopics::payload("$SYS/broker/retaineds/count", 0, 0, 0), "0");
}

TEST(SysTopicsTest, DroppedCountReflectsTheProcessCounter)
{
    // $SYS/broker/messages/dropped backs the backpressure metric. Unlike the
    // connected/sessions/retained topics its value comes from a process-wide
    // counter (the same one sent/received use), not from the arguments, so a
    // subscriber sees the live cumulative drop count as a plain decimal.
    EXPECT_EQ(SysTopics::payload("$SYS/broker/messages/dropped", 0, 0, 0),
              std::to_string(sysGetTotalDropped()));
    EXPECT_EQ(SysTopics::payload("$SYS/broker/messages/dropped", 99, 0, 0),
              std::to_string(sysGetTotalDropped()));
}

TEST(SysTopicsTest, DroppedTopicIsInTheMonitoringTree)
{
    // The dropped topic must be part of the advertised tree so a "$SYS/#" subscriber
    // receives it and it is refreshed on the sweep like every other counter.
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/messages/dropped"), 1u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/#"), SysTopics::allTopics().size());
    bool found = false;
    for (const std::string &topic : SysTopics::allTopics()) {
        if (topic == "$SYS/broker/messages/dropped") {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "dropped topic missing from allTopics()";
}

TEST(SysTopicsTest, WriteBufferBytesIsTheLiveGauge)
{
    // $SYS/broker/writebuffer/bytes backs the live outbound backpressure gauge.
    // Unlike sent/received/dropped it is not one of the process-wide cumulative
    // counters passed into payload(); it reads the live write-buffer occupancy
    // directly, so a subscriber sees current buffering as a plain decimal that
    // rises while a slow reader is behind and falls again as it drains.
    EXPECT_EQ(SysTopics::payload("$SYS/broker/writebuffer/bytes", 7, 7, 7),
              std::to_string(sysGetWriteBufferBytes()));
    // The connected-clients gauge is the other live value exposed this way.
    EXPECT_EQ(SysTopics::payload("$SYS/broker/clients/connected", 0, 0, 0),
              std::to_string(sysGetConnectedClients()));
}

TEST(SysTopicsTest, WriteBufferBytesIsInTheMonitoringTree)
{
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/writebuffer/bytes"), 1u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/#"), SysTopics::allTopics().size());
    bool found = false;
    for (const std::string &topic : SysTopics::allTopics()) {
        if (topic == "$SYS/broker/writebuffer/bytes") {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "writebuffer/bytes topic missing from allTopics()";
}

TEST(SysTopicsTest, QueuedBytesIsInTheMonitoringTree)
{
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/queued/bytes"), 1u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/#"), SysTopics::allTopics().size());
    bool found = false;
    for (const auto &topic : SysTopics::allTopics()) {
        if (topic == "$SYS/broker/queued/bytes") {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "queued/bytes topic missing from allTopics()";
}

TEST(SysTopicsTest, QueuedBytesIsTheLiveGauge)
{
    // $SYS/broker/queued/bytes backs the live in-memory queued-bytes gauge: the
    // bytes of undelivered messages held across all session queues. Like the
    // write-buffer gauge it is not one of the cumulative counters passed into
    // payload(); it reads the live gauge directly, so a subscriber sees the
    // current in-process backpressure as a plain decimal that rises as fan-out
    // queues messages for slow consumers and falls as they are flushed, dropped
    // or acknowledged -- the stage of backpressure that writebuffer/bytes (the
    // socket) and messages/dropped (post-loss) cannot show.
    EXPECT_EQ(SysTopics::payload("$SYS/broker/queued/bytes", 7, 7, 7),
              std::to_string(sysGetQueuedBytes()));
}

TEST(SysTopicsTest, QueuedBytesGaugeRisesAndFallsWithDelivery)
{
    // Drive a real session's queues through publish -> discard and check the live
    // gauge tracks the held bytes and returns exactly to zero, proving the
    // session reconciles the gauge on enqueue and on teardown with no drift.
    Broker broker;
    broker.setSysTopics(true);
    broker.sweepExpiredSessions();
    // The gauge is process-wide; other tests in the suite may leave held bytes,
    // so measure the delta around this session's activity rather than assuming a
    // zero baseline.
    const size_t baseline = sysGetQueuedBytes();

    auto session = std::make_shared<Session>(&broker, "queued-gauge", /*cleanStart=*/false);
    session->setPersistent(true);
    session->connect(session, /*persistent=*/true);
    session->subscribe("t/#", SubscriptionOptions{});
    // No client attached: the session is offline, so flush() keeps the queue and
    // the published messages stay held in memory.

    // Two QoS 1 messages (payload 1000 + topic "t/a") and one QoS 0 (payload 50
    // + topic "t/b"). With no client bound to flush() they sit in the queues.
    std::string big(1000, 'x');
    broker.publish(Message("t/a", big, QoS::QOS_1));
    broker.publish(Message("t/b", std::string(50, 'y'), QoS::QOS_0));
    broker.publish(Message("t/c", big, QoS::QOS_1));
    broker.sweepExpiredSessions();

    // 2 * (1000 payload + 3 topic) + 1 * (50 payload + 3 topic) added on top of
    // whatever the gauge already held.
    const size_t expected = baseline + 2 * (1000 + 3) + (50 + 3);
    EXPECT_EQ(sysGetQueuedBytes(), expected);

    // Discarding the session throws away everything it held; the gauge must drop
    // back to the baseline (the session reconciles on teardown).
    session->discard();
    broker.sweepExpiredSessions();
    EXPECT_EQ(sysGetQueuedBytes(), baseline);
}

TEST(SysTopicsTest, UnknownTopicYieldsNoPayload)
{
    EXPECT_TRUE(SysTopics::payload("$SYS/does/not/exist", 0, 0, 0).empty());
    EXPECT_TRUE(SysTopics::payload("not/sys", 0, 0, 0).empty());
}

// The next three tests exercise the broker-level behaviour of the $SYS tree:
// what it counts, and what it does to the durable state. They use a real Broker
// because the pure SysTopics::payload() helper cannot see the retained set.

TEST(SysTopicsTest, RetainedCountExcludesTheMonitoringTree)
{
    // $SYS/broker/retaineds/count reports the messages CLIENTS have retained, not
    // the broker's own monitoring tree. Counting the tree would fold it into its
    // own metric and inflate it by the number of $SYS topics, so a broker with
    // the feature on but no client traffic must report zero.
    Broker broker;
    broker.setSysTopics(true);
    broker.sweepExpiredSessions(); // refresh populates the $SYS tree

    EXPECT_EQ(broker.getRetainedCount(), SysTopics::allTopics().size()); // just the tree
    auto retained = broker.snapshotRetained();
    auto it = retained.find("$SYS/broker/retaineds/count");
    ASSERT_NE(it, retained.end());
    EXPECT_EQ(std::string(it->second.payload.begin(), it->second.payload.end()), "0");

    // One client-retained message brings the count to one, not one-plus-the-tree.
    Message clientMessage{"sensors/temp", std::string("1"), QoS::QOS_0, /*retain=*/true};
    broker.publish(clientMessage);
    broker.sweepExpiredSessions();
    retained = broker.snapshotRetained();
    it = retained.find("$SYS/broker/retaineds/count");
    ASSERT_NE(it, retained.end());
    EXPECT_EQ(std::string(it->second.payload.begin(), it->second.payload.end()), "1");
}

TEST(SysTopicsTest, MonitoringTreeIsNotPersisted)
{
    // The $SYS tree is a live, process-local view regenerated on every sweep, so
    // it must never enter the durable state: not on snapshot, and not on restore
    // (a state file written by an older build must not re-serve a stale tree to
    // a broker that has the feature disabled).
    Broker enabled;
    enabled.setSysTopics(true);
    enabled.sweepExpiredSessions();
    auto state = enabled.snapshotState();
    for (const auto &[topic, message] : state.retained) {
        EXPECT_FALSE(SysTopics::isSysTopic(topic))
            << "monitoring topic leaked into the durable state: " << topic;
    }

    // A hand-written state file carrying a stale $SYS entry, restored into a
    // broker with the feature off, must not serve it.
    PersistedState stale;
    stale.retained["$SYS/broker/uptime"] = Message("$SYS/broker/uptime", std::string("999"),
                                                    QoS::QOS_0, /*retain=*/true);
    Broker disabled;
    disabled.restoreState(stale);
    EXPECT_EQ(disabled.getRetainedCount(), 0u);
    EXPECT_EQ(disabled.snapshotRetained().count("$SYS/broker/uptime"), 0u);
}

TEST(SysTopicsTest, RefreshDoesNotMarkTheStoreDirty)
{
    // The uptime topic changes every second. A $SYS refresh must not mark the
    // store dirty, or an idle broker would rewrite its state file every sweep
    // with data that is discarded on the next start. With no other mutation the
    // durable state stays empty across repeated refreshes.
    Broker broker;
    broker.setSysTopics(true);
    for (int i = 0; i < 3; ++i) {
        broker.sweepExpiredSessions();
    }
    auto state = broker.snapshotState();
    EXPECT_TRUE(state.retained.empty()) << "a $SYS-only change leaked into the state";
    EXPECT_TRUE(state.sessions.empty());
}

} // namespace MQTT
