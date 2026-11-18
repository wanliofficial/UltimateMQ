#include <gtest/gtest.h>
#include "../src/Trie.h"
#include <algorithm>

class TrieTest : public ::testing::Test {
protected:
    MQTT::Trie trie;

    void SetUp() override {
        // Set up code here (if needed)
    }

    void TearDown() override {
        // Clean up code here (if needed)
    }
};

TEST_F(TrieTest, InsertAndMatch) {
    trie.insert("sensor/temperature");
    trie.insert("sensor/humidity");
    trie.insert("sensor/+/value");
    trie.insert("actuator/#");

    auto matches = trie.match("sensor/temperature");
    EXPECT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0], "sensor/temperature");

    matches = trie.match("sensor/pressure");
    EXPECT_EQ(matches.size(), 0);

    matches = trie.match("sensor/temperature/value");
    EXPECT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0], "sensor/+/value");

    matches = trie.match("actuator/light/on");
    EXPECT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0], "actuator/#");
}

TEST_F(TrieTest, Remove) {
    trie.insert("sensor/temperature");
    trie.insert("sensor/humidity");

    trie.remove("sensor/temperature");

    auto matches = trie.match("sensor/temperature");
    EXPECT_EQ(matches.size(), 0);

    matches = trie.match("sensor/humidity");
    EXPECT_EQ(matches.size(), 1);
    EXPECT_EQ(matches[0], "sensor/humidity");
}

TEST_F(TrieTest, WildcardsDoNotMatchDollarTopics) {
    // MQTT-4.7.2-1: the fan-out path must not hand a '$' topic to '#' or '+'.
    trie.insert("#");
    trie.insert("+/state");
    trie.insert("$SYS/#");

    auto matches = trie.match("$SYS/broker/uptime");
    ASSERT_EQ(matches.size(), 1u);
    EXPECT_EQ(matches[0], "$SYS/#");

    // An ordinary topic still reaches the wildcard filters.
    matches = trie.match("room/state");
    ASSERT_EQ(matches.size(), 2u);
    EXPECT_NE(std::find(matches.begin(), matches.end(), "#"), matches.end());
    EXPECT_NE(std::find(matches.begin(), matches.end(), "+/state"), matches.end());
}

TEST_F(TrieTest, MultiLevelWildcardMatchesTheParentLevel) {
    // MQTT-4.7.1-2: the routing table must deliver "sport" to a subscriber of
    // "sport/#" as well, since '#' includes the parent level.
    trie.insert("sport/#");

    auto matches = trie.match("sport");
    ASSERT_EQ(matches.size(), 1u);
    EXPECT_EQ(matches[0], "sport/#");

    matches = trie.match("sport/tennis");
    ASSERT_EQ(matches.size(), 1u);
    EXPECT_EQ(matches[0], "sport/#");
}

TEST_F(TrieTest, SingleLevelWildcardMatchesATrailingEmptyLevel) {
    // "sport/" is the two levels "sport" and "", so "sport/+" matches it while
    // "sport" itself is one level and must not match.
    trie.insert("sport/+");

    auto matches = trie.match("sport/");
    ASSERT_EQ(matches.size(), 1u);
    EXPECT_EQ(matches[0], "sport/+");

    EXPECT_TRUE(trie.match("sport").empty());
}

TEST_F(TrieTest, AHashNodeWithoutItsOwnFilterIsSkippedNotDereferenced)
{
    // Every filter that goes through SUBSCRIBE validation has its '#' as the
    // last level, so a '#' node is always a leaf carrying its own filter. A
    // restored state file can hold a filter that never went through that
    // validation ("a/#/b"), which leaves a '#' node with children but no filter
    // of its own. Matching a topic through it must skip the node rather than
    // call value() on an empty optional and throw out of a plain publish.
    trie.insert("a/#/b");

    std::vector<std::string> matches;
    ASSERT_NO_THROW(matches = trie.match("a/x")) << "matching threw instead of skipping";
    EXPECT_TRUE(matches.empty()) << "an unreportable '#' node produced a bogus match";
}

// Add more tests as needed
