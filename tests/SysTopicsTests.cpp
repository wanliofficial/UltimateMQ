#include <gtest/gtest.h>

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

} // namespace MQTT
