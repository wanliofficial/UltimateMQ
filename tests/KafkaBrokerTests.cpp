#include <gtest/gtest.h>
#include "../src/KafkaBroker.h"
#include "../src/KafkaErrors.h"
#include "../src/KafkaWire.h"
#include <cstdio>
#include <chrono>
#include <filesystem>
#include <fstream>
#include <string>
#include <thread>
#include <unistd.h>

namespace Kafka {
namespace {

std::string temporaryStatePath() {
    return (std::filesystem::temp_directory_path() /
            ("flowmq-kafka-test-" + std::to_string(::getpid()) + ".state"))
        .string();
}

std::vector<std::pair<std::string, std::vector<uint8_t>>> groupAssignment() {
    std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments;
    for (const std::string& member : {std::string("member-a"), std::string("member-b")}) {
        Writer writer;
        writer.writeInt16(0);
        writer.writeArrayLength(1);
        writer.writeString("events");
        writer.writeArrayLength(1);
        writer.writeInt32(0);
        writer.writeNullableBytes(nullptr);
        assignments.emplace_back(member, writer.finish());
    }
    return assignments;
}

} // namespace

TEST(KafkaBrokerTest, ProducesAndFetchesOrderedRecords) {
    Broker broker;
    broker.createTopic("events", 2, 1, {}, {});
    std::vector<ProducedRecord> records;
    records.push_back({std::vector<uint8_t>{'k'}, std::vector<uint8_t>{'a'}, 1000, {}});
    records.push_back({std::nullopt, std::vector<uint8_t>{'b'}, 1001, {}});
    const ProduceResult produced = broker.produce("events", 0, records, 1, {});
    EXPECT_EQ(produced.error, Error::NONE);
    EXPECT_EQ(produced.baseOffset, 0);

    const FetchResult fetched = broker.fetch("events", 0, 0, 1024, {});
    EXPECT_EQ(fetched.error, Error::NONE);
    ASSERT_EQ(fetched.records.size(), 2U);
    EXPECT_EQ(fetched.records[0].offset, 0);
    EXPECT_EQ(fetched.records[0].value, std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{'a'}));
    EXPECT_FALSE(fetched.records[1].key.has_value());
    EXPECT_EQ(fetched.highWatermark, 2);
    EXPECT_EQ(broker.listOffsets("events", 0, -1, {}).offset, 2);
    EXPECT_EQ(broker.listOffsets("events", 0, -2, {}).offset, 0);
    EXPECT_EQ(broker.fetch("events", 0, 3, 1024, {}).error, Error::OFFSET_OUT_OF_RANGE);
}

TEST(KafkaBrokerTest, IdempotentDuplicateReturnsOriginalOffset) {
    Broker broker;
    broker.createTopic("events", 1, 1, {}, {});
    const auto producer = broker.initProducerID({}, 0, "client");
    ASSERT_EQ(producer.error, Error::NONE);
    std::vector<ProducedRecord> records;
    records.push_back({std::nullopt, std::vector<uint8_t>{'v'}, 1000, {},
                       producer.producerID, producer.producerEpoch, 0});
    EXPECT_EQ(broker.produce("events", 0, records, 1, {}).baseOffset, 0);
    EXPECT_EQ(broker.produce("events", 0, records, 1, {}).baseOffset, 0);
    EXPECT_EQ(broker.fetch("events", 0, 0, 1024, {}).highWatermark, 1);
}

TEST(KafkaBrokerTest, RejectsInvalidTopologyAndOversizedRecords) {
    Broker broker(nullptr, 1, {}, 16);
    EXPECT_THROW(broker.createTopic("bad/topic", 1, 1, {}, {}), BrokerError);
    EXPECT_THROW(broker.createTopic("events", 0, 1, {}, {}), BrokerError);
    EXPECT_THROW(broker.createTopic("events", 1, 2, {}, {}), BrokerError);
    broker.createTopic("events", 1, 1, {}, {});
    EXPECT_THROW(broker.createTopic("events", 1, 1, {}, {}), BrokerError);
    std::vector<ProducedRecord> records;
    records.push_back({std::nullopt, std::vector<uint8_t>(17, 'x'), -1, {}});
    EXPECT_EQ(broker.produce("events", 0, records, 1, {}).error, Error::MESSAGE_TOO_LARGE);
}

TEST(KafkaBrokerTest, PersistsTopicsRecordsAndOffsets) {
    const std::string path = temporaryStatePath();
    std::remove(path.c_str());
    {
        Broker writer(nullptr, 7, path);
        writer.createTopic("events", 1, 1, {{"retention.ms", "-1"}}, {});
        std::vector<ProducedRecord> records;
        records.push_back({std::vector<uint8_t>{}, std::vector<uint8_t>{'v'}, 42,
                           {{"header", {1, 2}}}});
        ASSERT_EQ(writer.produce("events", 0, records, -1, {}).error, Error::NONE);
        writer.commitOffset("group", -1, "", "events", 0, 1, {});
        ASSERT_TRUE(writer.flush());
    }
    {
        Broker reader(nullptr, 7, path);
        ASSERT_TRUE(reader.load());
        const FetchResult fetched = reader.fetch("events", 0, 0, 1024, {});
        ASSERT_EQ(fetched.records.size(), 1U);
        EXPECT_TRUE(fetched.records[0].key.has_value());
        EXPECT_TRUE(fetched.records[0].key->empty());
        ASSERT_EQ(fetched.records[0].headers.size(), 1U);
        EXPECT_EQ(fetched.records[0].headers[0].key, "header");
        EXPECT_EQ(reader.fetchOffsets("group", {{"events", 0}}, {}).at({"events", 0}).offset, 1);
    }
    std::remove(path.c_str());
}

TEST(KafkaBrokerTest, PersistsLargeRecordHeaderKeys) {
    const std::string path = temporaryStatePath();
    std::remove(path.c_str());
    {
        Broker writer(nullptr, 1, path);
        writer.createTopic("events", 1, 1, {}, {});
        std::vector<ProducedRecord> records;
        records.push_back({std::nullopt, std::vector<uint8_t>{'v'}, 1000,
                           {{std::string(40000, 'h'), {1}}}});
        ASSERT_EQ(writer.produce("events", 0, records, 1, {}).error, Error::NONE);
    }
    {
        Broker reader(nullptr, 1, path);
        ASSERT_TRUE(reader.load());
        const FetchResult fetched = reader.fetch("events", 0, 0, 1024 * 1024, {});
        ASSERT_EQ(fetched.records.size(), 1U);
        ASSERT_EQ(fetched.records[0].headers.size(), 1U);
        EXPECT_EQ(fetched.records[0].headers[0].key.size(), 40000U);
    }
    std::remove(path.c_str());
}

TEST(KafkaBrokerTest, TopicDeletionPurgesPersistedReferences) {
    const std::string path = temporaryStatePath();
    std::remove(path.c_str());
    {
        Broker writer(nullptr, 1, path);
        writer.createTopic("events", 1, 1, {}, {});
        writer.commitOffset("group", -1, "", "events", 0, 1, {});
        const auto producer = writer.initProducerID("txn", 30000, "client");
        ASSERT_EQ(writer.addPartitionsToTxn("txn", producer.producerID, producer.producerEpoch,
                                            {{"events", {0}}}, {}).at({"events", 0}), Error::NONE);
        std::vector<ProducedRecord> records;
        records.push_back({std::nullopt, std::vector<uint8_t>{'v'}, 1000, {},
                           producer.producerID, producer.producerEpoch, 0});
        ASSERT_EQ(writer.produce("events", 0, records, 1, {}, "txn").error, Error::NONE);
        writer.deleteTopic("events", {});
    }
    {
        Broker reader(nullptr, 1, path);
        ASSERT_TRUE(reader.load());
        EXPECT_TRUE(reader.topics({}, false, {}, true).empty());
        EXPECT_TRUE(reader.fetchOffsets("group", {{"events", 0}}, {}).at({"events", 0}).error ==
                    Error::UNKNOWN_TOPIC_OR_PARTITION);
    }
    std::remove(path.c_str());
}

TEST(KafkaBrokerTest, CoordinatesBasicConsumerGroup) {
    Broker broker;
    const std::vector<uint8_t> metadata = {1, 2, 3};
    auto joined = broker.joinGroup("group", "member-a", "client-a", "127.0.0.1",
                                   10000, 3000, "consumer", "range", 10000, metadata);
    ASSERT_EQ(joined.members.size(), 1U);
    joined = broker.joinGroup("group", "member-b", "client-b", "127.0.0.1",
                              10000, 3000, "consumer", "range", 10000, metadata);
    ASSERT_EQ(joined.members.size(), 2U);
    ASSERT_EQ(joined.generation, 2);
    ASSERT_EQ(joined.leaderID, "member-a");
    broker.syncGroup("group", "member-a", 2, groupAssignment());
    broker.syncGroup("group", "member-b", 2, {});
    EXPECT_FALSE(broker.memberAssignment("group", "member-a", 2).empty());
    EXPECT_FALSE(broker.memberAssignment("group", "member-b", 2).empty());
    broker.heartbeatGroup("group", "member-a", 2);
    broker.leaveGroup("group", "member-a");
    EXPECT_THROW(broker.heartbeatGroup("group", "member-a", 3), BrokerError);
}

TEST(KafkaBrokerTest, TransactionVisibilityAndControlMarkers) {
    Broker broker;
    broker.createTopic("events", 1, 1, {}, {});
    const auto producer = broker.initProducerID("txn", 30000, "client");
    ASSERT_EQ(producer.error, Error::NONE);
    ASSERT_EQ(broker.addPartitionsToTxn("txn", producer.producerID, producer.producerEpoch,
                                        {{"events", {0}}}, {}).at({"events", 0}), Error::NONE);
    std::vector<ProducedRecord> records;
    records.push_back({std::nullopt, std::vector<uint8_t>{'a'}, 1000, {},
                       producer.producerID, producer.producerEpoch, 0});
    ASSERT_EQ(broker.produce("events", 0, records, -1, {}, "txn").error, Error::NONE);

    FetchResult unstable = broker.fetch("events", 0, 0, 1024, {}, true);
    EXPECT_EQ(unstable.highWatermark, 1);
    EXPECT_EQ(unstable.lastStableOffset, 0);
    EXPECT_TRUE(unstable.records.empty());
    EXPECT_EQ(broker.listOffsets("events", 0, -1, {}, true).offset, 0);
    EXPECT_EQ(broker.listOffsets("events", 0, -1, {}, false).offset, 1);

    broker.endTxn("txn", producer.producerID, producer.producerEpoch, true, {});
    FetchResult committed = broker.fetch("events", 0, 0, 1024, {}, true);
    EXPECT_EQ(committed.highWatermark, 2);
    EXPECT_EQ(committed.lastStableOffset, 2);
    ASSERT_EQ(committed.records.size(), 2U);
    EXPECT_EQ(committed.records[0].value, std::optional<std::vector<uint8_t>>(std::vector<uint8_t>{'a'}));
    EXPECT_TRUE(committed.records[1].control);
    EXPECT_TRUE(committed.records[1].controlCommit);

    ASSERT_EQ(broker.addPartitionsToTxn("txn", producer.producerID, producer.producerEpoch,
                                        {{"events", {0}}}, {}).at({"events", 0}), Error::NONE);
    records.clear();
    records.push_back({std::nullopt, std::vector<uint8_t>{'b'}, 1001, {},
                       producer.producerID, producer.producerEpoch, 1});
    ASSERT_EQ(broker.produce("events", 0, records, -1, {}, "txn").error, Error::NONE);
    broker.endTxn("txn", producer.producerID, producer.producerEpoch, false, {});

    FetchResult aborted = broker.fetch("events", 0, 0, 1024, {}, true);
    ASSERT_EQ(aborted.abortedTransactions.size(), 1U);
    EXPECT_EQ(aborted.abortedTransactions[0].producerID, producer.producerID);
    EXPECT_EQ(aborted.abortedTransactions[0].firstOffset, 2);
    ASSERT_EQ(aborted.records.size(), 4U);
    EXPECT_FALSE(aborted.records[2].controlCommit);
    EXPECT_EQ(aborted.records[3].offset, 3);
    EXPECT_EQ(aborted.lastStableOffset, 4);
    EXPECT_EQ(broker.fetch("events", 0, 0, 1024, {}, false).records.size(), 4U);
}

TEST(KafkaBrokerTest, TransactionMarkersCompleteOnlyRequestedPartitions) {
    Broker broker;
    broker.createTopic("events", 2, 1, {}, {});
    const auto producer = broker.initProducerID("txn", 30000, "client");
    ASSERT_EQ(broker.addPartitionsToTxn("txn", producer.producerID, producer.producerEpoch,
                                        {{"events", {0, 1}}}, {}).at({"events", 0}), Error::NONE);
    for (int32_t partition : {0, 1}) {
        std::vector<ProducedRecord> records;
        records.push_back({std::nullopt, std::vector<uint8_t>{'v'}, 1000, {},
                           producer.producerID, producer.producerEpoch, 0});
        ASSERT_EQ(broker.produce("events", partition, records, -1, {}, "txn").error, Error::NONE);
    }
    EXPECT_THROW(broker.writeTxnMarkers(producer.producerID, producer.producerEpoch, -1, true,
                                        {{"events", {0}}}, {}), BrokerError);
    EXPECT_EQ(broker.writeTxnMarkers(producer.producerID, producer.producerEpoch, 0, true,
                                    {{"events", {0}}}, {}).at({"events", 0}), Error::NONE);
    EXPECT_EQ(broker.fetch("events", 0, 0, 1024, {}, true).lastStableOffset, 2);
    EXPECT_EQ(broker.fetch("events", 1, 0, 1024, {}, true).lastStableOffset, 0);
    EXPECT_EQ(broker.writeTxnMarkers(producer.producerID, producer.producerEpoch, 0, true,
                                    {{"events", {1}}}, {}).at({"events", 1}), Error::NONE);
    EXPECT_EQ(broker.fetch("events", 1, 0, 1024, {}, true).lastStableOffset, 2);
}

TEST(KafkaBrokerTest, PersistsTransactionMarkersAndProducerSequences) {
    const std::string path = temporaryStatePath();
    std::remove(path.c_str());
    int64_t producerID = -1;
    int16_t producerEpoch = -1;
    {
        Broker writer(nullptr, 7, path);
        writer.createTopic("events", 1, 1, {}, {});
        const auto producer = writer.initProducerID("txn", 30000, "client");
        producerID = producer.producerID;
        producerEpoch = producer.producerEpoch;
        ASSERT_EQ(producer.error, Error::NONE);
        ASSERT_EQ(writer.addPartitionsToTxn("txn", producerID, producerEpoch,
                                            {{"events", {0}}}, {}).at({"events", 0}), Error::NONE);
        std::vector<ProducedRecord> records;
        records.push_back({std::nullopt, std::vector<uint8_t>{'v'}, 1000, {}, producerID, producerEpoch, 0});
        ASSERT_EQ(writer.produce("events", 0, records, -1, {}, "txn").error, Error::NONE);
        writer.endTxn("txn", producerID, producerEpoch, true, {});
        ASSERT_TRUE(writer.flush());
    }
    {
        Broker reader(nullptr, 7, path);
        ASSERT_TRUE(reader.load());
        const FetchResult fetched = reader.fetch("events", 0, 0, 1024, {}, true);
        ASSERT_EQ(fetched.records.size(), 2U);
        EXPECT_TRUE(fetched.records[1].controlCommit);
        EXPECT_EQ(fetched.lastStableOffset, 2);
        std::vector<ProducedRecord> duplicate;
        duplicate.push_back({std::nullopt, std::vector<uint8_t>{'v'}, 1000, {}, producerID, producerEpoch, 0});
        const ProduceResult result = reader.produce("events", 0, duplicate, -1, {});
        EXPECT_EQ(result.error, Error::NONE);
        EXPECT_EQ(result.baseOffset, 0);
        EXPECT_EQ(reader.fetch("events", 0, 0, 1024, {}).highWatermark, 2);
    }
    std::remove(path.c_str());
}

TEST(KafkaBrokerTest, TransactionCompletionRequiresTransactionalIDAuthorization) {
    const std::string aclPath = temporaryStatePath() + ".txn-acl";
    {
        std::ofstream acl(aclPath);
        acl << "owner,txn-id-write,txn\n"
               "owner,cluster-admin,cluster\n";
    }
    MQTT::Auth auth;
    ASSERT_TRUE(auth.loadAcl(aclPath));
    Broker broker(&auth);
    broker.createTopic("events", 1, 1, {}, "owner");
    const auto producer = broker.initProducerID("txn", 30000, "client", "owner");
    ASSERT_EQ(producer.error, Error::NONE);
    try {
        broker.addPartitionsToTxn("txn", producer.producerID, producer.producerEpoch,
                                  {{"events", {0}}}, "intruder");
        FAIL() << "unauthorized transaction partition registration succeeded";
    } catch (const BrokerError& error) {
        EXPECT_EQ(error.code, Error::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }
    try {
        broker.endTxn("txn", producer.producerID, producer.producerEpoch, true, "intruder");
        FAIL() << "unauthorized transaction completion succeeded";
    } catch (const BrokerError& error) {
        EXPECT_EQ(error.code, Error::TRANSACTIONAL_ID_AUTHORIZATION_FAILED);
    }
    std::remove(aclPath.c_str());
}

TEST(KafkaBrokerTest, TransactionTimeoutAbortsAndReleasesLSO) {
    Broker broker;
    broker.createTopic("events", 1, 1, {}, {});
    const auto producer = broker.initProducerID("txn", 1, "client");
    ASSERT_EQ(broker.addPartitionsToTxn("txn", producer.producerID, producer.producerEpoch,
                                        {{"events", {0}}}, {}).at({"events", 0}), Error::NONE);
    std::vector<ProducedRecord> records;
    records.push_back({std::nullopt, std::vector<uint8_t>{'v'}, 1000, {},
                       producer.producerID, producer.producerEpoch, 0});
    ASSERT_EQ(broker.produce("events", 0, records, -1, {}, "txn").error, Error::NONE);
    std::this_thread::sleep_for(std::chrono::milliseconds(5));
    try {
        broker.endTxn("txn", producer.producerID, producer.producerEpoch, true, {});
        FAIL() << "expired transaction committed";
    } catch (const BrokerError& error) {
        EXPECT_EQ(error.code, Error::INVALID_TXN_STATE);
    }
    const FetchResult fetched = broker.fetch("events", 0, 0, 1024, {}, true);
    EXPECT_EQ(fetched.lastStableOffset, 2);
    ASSERT_EQ(fetched.abortedTransactions.size(), 1U);
    ASSERT_EQ(fetched.records.size(), 2U);
    EXPECT_TRUE(fetched.records[1].control);
    EXPECT_FALSE(fetched.records[1].controlCommit);
}

TEST(KafkaBrokerTest, SingleNodeElectionRequiresClusterAdministration) {
    Broker broker;
    broker.createTopic("events", 1, 1, {}, {});
    EXPECT_NO_THROW(broker.validatePartitionElection("events", 0, {}));
    EXPECT_THROW(broker.validatePartitionElection("events", 1, {}), BrokerError);

    const std::string aclPath = temporaryStatePath() + ".election-acl";
    {
        std::ofstream acl(aclPath);
        acl << "admin,cluster-admin,cluster\n";
    }
    MQTT::Auth auth;
    ASSERT_TRUE(auth.loadAcl(aclPath));
    Broker secured(&auth);
    secured.createTopic("events", 1, 1, {}, "admin");
    EXPECT_NO_THROW(secured.validatePartitionElection("events", 0, "admin"));
    try {
        secured.validatePartitionElection("events", 0, "intruder");
        FAIL() << "unauthorized election succeeded";
    } catch (const BrokerError& error) {
        EXPECT_EQ(error.code, Error::CLUSTER_AUTHORIZATION_FAILED);
    }
    std::remove(aclPath.c_str());
}

TEST(KafkaBrokerTest, OffsetDeletionAndDedicatedGroupACL) {
    const std::string aclPath = temporaryStatePath() + ".acl";
    {
        std::ofstream acl(aclPath);
        acl << "consumer,group-write,workers\n"
               "consumer,group-read,workers\n"
               "consumer,group-describe,workers\n"
               "consumer,cluster-admin,cluster\n";
    }
    MQTT::Auth auth;
    ASSERT_TRUE(auth.loadAcl(aclPath));
    Broker broker(&auth);
    broker.createTopic("events", 1, 1, {}, "consumer");
    EXPECT_THROW(broker.joinGroup("workers", "m", "c", "127.0.0.1", 10000, 3000,
                                  "consumer", "range", 10000, {}, "intruder"), BrokerError);
    const auto joined = broker.joinGroup("workers", "m", "c", "127.0.0.1", 10000, 3000,
                                         "consumer", "range", 10000, {}, "consumer");
    Writer assignment;
    assignment.writeInt16(0);
    assignment.writeArrayLength(1);
    assignment.writeString("events");
    assignment.writeArrayLength(1);
    assignment.writeInt32(0);
    assignment.writeNullableBytes(nullptr);
    broker.syncGroup("workers", "m", joined.generation, {{"m", assignment.finish()}}, "consumer");
    broker.commitOffset("workers", -1, "", "events", 0, 1, "consumer");
    EXPECT_EQ(broker.fetchOffsets("workers", {{"events", 0}}, "consumer").at({"events", 0}).offset, 1);
    EXPECT_EQ(broker.fetchOffsets("workers", {{"events", 0}}, "intruder").at({"events", 0}).error,
              Error::GROUP_AUTHORIZATION_FAILED);
    EXPECT_EQ(broker.describeGroups({"workers"}, "consumer").at(0).error, Error::NONE);
    EXPECT_EQ(broker.describeGroups({"workers"}, "intruder").at(0).error,
              Error::GROUP_AUTHORIZATION_FAILED);
    broker.commitOffset("workers", -1, "", "events", 0, -1, "consumer");
    EXPECT_EQ(broker.fetchOffsets("workers", {{"events", 0}}, "consumer").at({"events", 0}).offset, 0);
    std::remove(aclPath.c_str());
}

TEST(KafkaBrokerTest, EmptyOffsetFetchChecksTopicACL) {
    const std::string aclPath = temporaryStatePath() + ".offset-fetch-acl";
    {
        std::ofstream acl(aclPath);
        acl << "admin,read,events\n"
               "admin,write,events\n"
               "admin,group-read,workers\n"
               "admin,cluster-admin,cluster\n"
               "consumer,group-read,workers\n";
    }
    MQTT::Auth auth;
    ASSERT_TRUE(auth.loadAcl(aclPath));
    Broker broker(&auth);
    broker.createTopic("events", 1, 1, {}, "admin");
    broker.commitOffset("workers", -1, "", "events", 0, 1, "admin");
    const auto result = broker.fetchOffsets("workers", {}, "consumer");
    EXPECT_EQ(result.at({"events", 0}).error, Error::TOPIC_AUTHORIZATION_FAILED);
    std::remove(aclPath.c_str());
}

} // namespace Kafka
