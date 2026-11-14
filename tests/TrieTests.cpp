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

// Add more tests as needed
