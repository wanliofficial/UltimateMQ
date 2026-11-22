#include <gtest/gtest.h>

#include <array>

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
        EXPECT_FALSE(SysTopics::payload(topic, 1, 1, 1, 1).empty())
            << "no payload for " << topic;
    }
}

TEST(SysTopicsTest, CountsAreReportedAsDecimalNumbers)
{
    EXPECT_EQ(SysTopics::payload("$SYS/broker/clients/connected", 3, 3, 0, 0), "3");
    EXPECT_EQ(SysTopics::payload("$SYS/broker/clients/total", 0, 7, 0, 0), "7");
    EXPECT_EQ(SysTopics::payload("$SYS/broker/subscriptions/count", 0, 0, 5, 0), "5");
    EXPECT_EQ(SysTopics::payload("$SYS/broker/retaineds/count", 0, 0, 0, 4), "4");
}

TEST(SysTopicsTest, DroppedCountReflectsTheProcessCounter)
{
    // $SYS/broker/messages/dropped backs the backpressure metric. Unlike the
    // connected/sessions/retained topics its value comes from a process-wide
    // counter (the same one sent/received use), not from the arguments, so a
    // subscriber sees the live cumulative drop count as a plain decimal.
    EXPECT_EQ(SysTopics::payload("$SYS/broker/messages/dropped", 0, 0, 0, 0),
              std::to_string(sysGetTotalDropped()));
    EXPECT_EQ(SysTopics::payload("$SYS/broker/messages/dropped", 99, 0, 0, 0),
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
    EXPECT_EQ(SysTopics::payload("$SYS/broker/writebuffer/bytes", 7, 7, 7, 7),
              std::to_string(sysGetWriteBufferBytes()));
    // The connected-clients gauge is the other live value exposed this way.
    EXPECT_EQ(SysTopics::payload("$SYS/broker/clients/connected", 0, 0, 0, 0),
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
    EXPECT_EQ(SysTopics::payload("$SYS/broker/queued/bytes", 7, 7, 7, 7),
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

TEST(SysTopicsTest, DisconnectTopicsAreInTheMonitoringTree)
{
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/disconnect/keepalive"), 1u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/disconnect/auth"), 1u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/disconnect/other"), 1u);
    // The whole disconnect subtree is reached by a "$SYS/#" subscription.
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/disconnect/#"), 6u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/#"), SysTopics::allTopics().size());
}

TEST(SysTopicsTest, DisconnectCategoriesClassifyEveryReason)
{
    // The taxonomy must map every ReasonCode that can end a connection into
    // exactly one category, so the disconnect counters account for every client
    // that leaves with none lost. Drive each representative reason through the
    // broker and read the category it landed in.
    Broker broker;
    broker.setSysTopics(true);
    broker.sweepExpiredSessions();
    const std::string ka = "$SYS/broker/disconnect/keepalive";
    const std::string proto = "$SYS/broker/disconnect/protocol";
    const std::string quota = "$SYS/broker/disconnect/quota";
    const std::string auth = "$SYS/broker/disconnect/auth";
    const std::string normal = "$SYS/broker/disconnect/normal";
    const std::string other = "$SYS/broker/disconnect/other";
    // The disconnect counters are process-wide; other tests in the suite tear
    // down connections and land here too, so measure the delta this test causes
    // rather than assuming a zero baseline.
    auto before = [&] {
        auto v = broker.sysTopicValues();
        auto get = [&](const std::string &t) {
            auto it = v.find(t);
            return it == v.end() ? 0u : std::stoul(it->second);
        };
        return std::array<size_t, 6>{get(ka), get(proto), get(quota),
                                     get(auth), get(normal), get(other)};
    };
    const auto base = before();

    broker.recordDisconnect(ReasonCode::KEEP_ALIVE_TIMEOUT);
    broker.recordDisconnect(ReasonCode::PROTOCOL_ERROR);
    broker.recordDisconnect(ReasonCode::MALFORMED_PACKET);
    broker.recordDisconnect(ReasonCode::PACKET_TOO_LARGE);
    broker.recordDisconnect(ReasonCode::QUOTA_EXCEEDED);
    broker.recordDisconnect(ReasonCode::MESSAGE_RATE_TOO_HIGH);
    broker.recordDisconnect(ReasonCode::BAD_USERNAME_OR_PASSWORD);
    broker.recordDisconnect(ReasonCode::NOT_AUTHORIZED);
    broker.recordDisconnect(ReasonCode::NORMAL_DISCONNECTION);
    broker.recordDisconnect(ReasonCode::SERVER_UNAVAILABLE); // falls to other
    broker.sweepExpiredSessions();

    const auto after = before();
    EXPECT_EQ(after[0] - base[0], 1u); // KEEP_ALIVE_TIMEOUT
    EXPECT_EQ(after[1] - base[1], 3u); // PROTOCOL_ERROR, MALFORMED_PACKET, PACKET_TOO_LARGE
    EXPECT_EQ(after[2] - base[2], 2u); // QUOTA_EXCEEDED, MESSAGE_RATE_TOO_HIGH
    EXPECT_EQ(after[3] - base[3], 2u); // BAD_USERNAME_OR_PASSWORD, NOT_AUTHORIZED
    EXPECT_EQ(after[4] - base[4], 1u); // NORMAL_DISCONNECTION
    EXPECT_EQ(after[5] - base[5], 1u); // SERVER_UNAVAILABLE
}

TEST(SysTopicsTest, UnknownTopicYieldsNoPayload)
{
    EXPECT_TRUE(SysTopics::payload("$SYS/does/not/exist", 0, 0, 0, 0).empty());
    EXPECT_TRUE(SysTopics::payload("not/sys", 0, 0, 0, 0).empty());
}

// The next tests exercise the broker-level behaviour of the $SYS tree:
// what it counts, and what it does to the durable state. They use a real Broker
// because the pure SysTopics::payload() helper cannot see the retained set.

TEST(SysTopicsTest, SubscriptionCountTracksNormalAndShared)
{
    // $SYS/broker/subscriptions/count reports the total subscription INSTANCES the
    // broker routes: every (client, filter) normal subscription plus every shared
    // subscription member (which lives only in the shared-subscription map). It is
    // the fan-out workload / routing-table pressure in one number -- the capacity
    // complement to clients/total (how many sessions) and the backpressure gauges.
    Broker broker;
    broker.setSysTopics(true);
    broker.sweepExpiredSessions();
    auto count = [&] {
        broker.refreshSysTopics();
        auto values = broker.sysTopicValues();
        auto it = values.find("$SYS/broker/subscriptions/count");
        return it == values.end() ? 0u : std::stoul(it->second);
    };

    ASSERT_EQ(count(), 0u);

    // Two normal subscriptions on one client.
    auto a = std::make_shared<Session>(&broker, "sub-a");
    a->connect(a, /*persistent=*/true);
    a->subscribe("a/#", SubscriptionOptions{});
    a->subscribe("b/c", SubscriptionOptions{});
    broker.sweepExpiredSessions();
    EXPECT_EQ(count(), 2u);

    // A normal + a shared subscription on a second client. The shared one lands
    // only in the shared-subscription map, so it must be counted from there.
    auto b = std::make_shared<Session>(&broker, "sub-b");
    b->connect(b, /*persistent=*/true);
    b->subscribe("a/#", SubscriptionOptions{});
    b->subscribe("$share/grp/t/#", SubscriptionOptions{});
    broker.sweepExpiredSessions();
    EXPECT_EQ(count(), 4u); // 3 normal (a:2 + b:1) + 1 shared member

    // A second member in the SAME share group on the SAME filter adds one
    // instance (per-member counting, not per-filter).
    auto c = std::make_shared<Session>(&broker, "sub-c");
    c->connect(c, /*persistent=*/true);
    c->subscribe("$share/grp/t/#", SubscriptionOptions{});
    broker.sweepExpiredSessions();
    EXPECT_EQ(count(), 5u);

    // Unsubscribing removes exactly the instance that left.
    a->unsubscribe("a/#");
    broker.sweepExpiredSessions();
    EXPECT_EQ(count(), 4u);
}

TEST(SysTopicsTest, SubscriptionCountIsInTheMonitoringTree)
{
    EXPECT_EQ(SysTopics::matchedCount("$SYS/broker/subscriptions/count"), 1u);
    EXPECT_EQ(SysTopics::matchedCount("$SYS/#"), SysTopics::allTopics().size());
    bool found = false;
    for (const auto &topic : SysTopics::allTopics()) {
        if (topic == "$SYS/broker/subscriptions/count") {
            found = true;
            break;
        }
    }
    EXPECT_TRUE(found) << "subscriptions/count topic missing from allTopics()";
}

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
