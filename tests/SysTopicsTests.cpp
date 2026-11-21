#include <gtest/gtest.h>

#include "Broker.h"
#include "SysTopics.h"
#include "Topic.h"

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
