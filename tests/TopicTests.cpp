#include <gtest/gtest.h>
#include "../src/Topic.h"

namespace MQTT
{
    
TEST(TopicTest, Split)
{
    std::string topic = "a/b/c";
    auto result = Topic::split(topic);
    std::vector<std::string> expected = {"a", "b", "c"};
    EXPECT_EQ(result, expected);
}

TEST(TopicTest, Join)
{
    std::vector<std::string> topicLevels = {"x", "y", "z"};
    auto result = Topic::join(topicLevels);
    EXPECT_EQ(result, "x/y/z");
}

TEST(TopicTest, IsValid)
{
    EXPECT_TRUE(Topic::isValid("a/b/c"));
    EXPECT_TRUE(Topic::isValid("a//c"));
    EXPECT_TRUE(Topic::isValid("#"));
    EXPECT_TRUE(Topic::isValid("+"));
    EXPECT_TRUE(Topic::isValid("a/#"));
    EXPECT_TRUE(Topic::isValid("a/+"));

    EXPECT_FALSE(Topic::isValid(""));            // empty
    EXPECT_FALSE(Topic::isValid("a/b/c#"));      // '#' inside a level
    EXPECT_FALSE(Topic::isValid("a/b#c"));       // '+'/'#' inside a level
    EXPECT_FALSE(Topic::isValid("#/a"));         // '#' not last
    EXPECT_FALSE(Topic::isValid("a/#/b"));       // '#' not last
}

TEST(TopicTest, IsValidSharedSubscription)
{
    // MQTT-4.8.2-1: a shared subscription must start with "$share/" and its
    // ShareName must be at least one character long. MQTT-4.8.2-2: the ShareName
    // must not contain '/', '+' or '#', and must be followed by '/' and a real
    // Topic Filter. A bogus "$share/..." is not an ordinary filter: accepting it
    // grants a subscription that can never match, and a wildcard in the
    // ShareName would silently be matched as a topic level.
    EXPECT_TRUE(Topic::isValid("$share/group/topic"));
    EXPECT_TRUE(Topic::isValid("$share/g/a/+"));
    EXPECT_TRUE(Topic::isValid("$share/g/#"));
    EXPECT_TRUE(Topic::isValid("$share/g/a//b"));

    EXPECT_FALSE(Topic::isValid("$share//t"));      // empty ShareName
    EXPECT_FALSE(Topic::isValid("$share/"));        // no ShareName, no filter
    EXPECT_FALSE(Topic::isValid("$share/g"));       // ShareName not followed by '/'
    EXPECT_FALSE(Topic::isValid("$share/g/"));      // empty filter
    EXPECT_FALSE(Topic::isValid("$share/+/t"));     // '+' in ShareName
    EXPECT_FALSE(Topic::isValid("$share/#"));       // '#' in ShareName
    EXPECT_FALSE(Topic::isValid("$share/g+/t"));    // '+' inside ShareName
    EXPECT_FALSE(Topic::isValid("$share/g/a/#/b")); // '#' not last in the filter
    EXPECT_FALSE(Topic::isValid("$share/g/a/b#"));  // '#' inside a filter level

    // "$share" without the trailing '/' is an ordinary first level.
    EXPECT_TRUE(Topic::isValid("$shared/x"));
    EXPECT_TRUE(Topic::isValid("$share"));
}

TEST(TopicTest, IsValidTopicNameForbidsWildcards)
{
    // A PUBLISH topic (or Will topic) must carry no wildcards at all
    // (MQTT-3.3.2-1).
    EXPECT_TRUE(Topic::isValidTopicName("a/b/c"));
    EXPECT_TRUE(Topic::isValidTopicName("a//c"));
    EXPECT_FALSE(Topic::isValidTopicName("a/+"));
    EXPECT_FALSE(Topic::isValidTopicName("a/#"));
    EXPECT_FALSE(Topic::isValidTopicName("a/b#"));
    EXPECT_FALSE(Topic::isValidTopicName(""));
}

TEST(TopicTest, Match)
{
    EXPECT_TRUE(Topic::match("a/b/c", "a/+/c"));
    EXPECT_TRUE(Topic::match("a/b/c/d", "a/#"));
    EXPECT_FALSE(Topic::match("a/b/c", "x/+/c"));
}

TEST(TopicTest, WildcardsDoNotMatchDollarTopics)
{
    // MQTT-4.7.2-1: a wildcard first level must not match a '$' topic.
    EXPECT_FALSE(Topic::match("$SYS/broker/uptime", "#"));
    EXPECT_FALSE(Topic::match("$SYS/broker/uptime", "+/broker/uptime"));
    EXPECT_FALSE(Topic::match("$SYS/broker/uptime", "+/#"));
    // An explicit '$' first level still matches.
    EXPECT_TRUE(Topic::match("$SYS/broker/uptime", "$SYS/broker/uptime"));
    EXPECT_TRUE(Topic::match("$SYS/broker/uptime", "$SYS/#"));
    EXPECT_TRUE(Topic::match("$SYS/broker/uptime", "$SYS/+/uptime"));
    // A '$' later in the topic is an ordinary character.
    EXPECT_TRUE(Topic::match("a/$x", "#"));
    EXPECT_TRUE(Topic::match("a/$x", "a/+"));
}

TEST(TopicTest, IsShared)
{
    EXPECT_TRUE(Topic::isShared("$share/group/topic"));
    EXPECT_FALSE(Topic::isShared("normal/topic"));
    EXPECT_FALSE(Topic::isShared("$share"));
    EXPECT_FALSE(Topic::isShared("$share/"));
}

TEST(TopicTest, SplitShared)
{
    auto result1 = Topic::splitShared("$share/group1/a/b/c");
    EXPECT_EQ(result1.first, "group1");
    EXPECT_EQ(result1.second, "a/b/c");

    auto result2 = Topic::splitShared("normal/topic");
    EXPECT_EQ(result2.first, "");
    EXPECT_EQ(result2.second, "normal/topic");

    auto result3 = Topic::splitShared("$share/group2/");
    EXPECT_EQ(result3.first, "group2");
    EXPECT_EQ(result3.second, "");

    auto result4 = Topic::splitShared("$share/");
    EXPECT_EQ(result4.first, "");
    EXPECT_EQ(result4.second, "");
}

TEST(TopicOverlapTest, IdenticalFiltersOverlap)
{
    EXPECT_TRUE(Topic::filtersOverlap("a/b", "a/b"));
    EXPECT_TRUE(Topic::filtersOverlap("#", "#"));
    EXPECT_TRUE(Topic::filtersOverlap("a/+/c", "a/+/c"));
}

TEST(TopicOverlapTest, DifferentFirstLevelsNeverOverlap)
{
    EXPECT_FALSE(Topic::filtersOverlap("sensors/#", "private/#"));
    EXPECT_FALSE(Topic::filtersOverlap("a/b", "c/b"));
    EXPECT_FALSE(Topic::filtersOverlap("sensors/+", "private/+"));
}

TEST(TopicOverlapTest, HashOverlapsEverything)
{
    EXPECT_TRUE(Topic::filtersOverlap("#", "sensors/temp"));
    EXPECT_TRUE(Topic::filtersOverlap("sensors/#", "#"));
    // "a/#" also matches "a" (zero further levels)
    EXPECT_TRUE(Topic::filtersOverlap("a/#", "a"));
    EXPECT_TRUE(Topic::filtersOverlap("a", "a/#"));
}

TEST(TopicOverlapTest, PlusMatchesExactlyOneLevel)
{
    EXPECT_TRUE(Topic::filtersOverlap("sensors/+", "sensors/temp"));
    EXPECT_TRUE(Topic::filtersOverlap("+/temp", "sensors/+"));
    // "sensors/+" cannot match the two-level "sensors/a/b"
    EXPECT_FALSE(Topic::filtersOverlap("sensors/+", "sensors/a/b"));
    // ... nor the single-level "sensors"
    EXPECT_FALSE(Topic::filtersOverlap("sensors/+", "sensors"));
}

TEST(TopicOverlapTest, PrefixesOfDifferentLengthDoNotOverlap)
{
    EXPECT_FALSE(Topic::filtersOverlap("a/b", "a/b/c"));
    EXPECT_FALSE(Topic::filtersOverlap("a/b/c", "a/b"));
}

} // namespace MQTT

// Add more tests as needed
