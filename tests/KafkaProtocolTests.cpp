#include <gtest/gtest.h>
#include "../src/KafkaBroker.h"
#include "../src/KafkaErrors.h"
#include "../src/KafkaProtocol.h"
#include "../src/KafkaWire.h"

namespace Kafka {
namespace {

std::vector<Record> sampleRecords() {
    Record first;
    first.offset = 4;
    first.timestamp = 1000;
    first.key = std::vector<uint8_t>{'k'};
    first.value = std::vector<uint8_t>{'v'};
    first.headers = {{"h", {1, 2, 3}}};
    Record second;
    second.offset = 5;
    second.timestamp = 1010;
    second.value = std::vector<uint8_t>{};
    return {first, second};
}

} // namespace

TEST(KafkaProtocolTest, RecordBatchRoundTripPreservesNullAndHeaders) {
    const std::vector<Record> input = sampleRecords();
    const std::vector<uint8_t> batch = encodeMessageSet(input, 2);
    const std::vector<ProducedRecord> output = decodeProduceRecords(batch, 3, 1024);
    ASSERT_EQ(output.size(), input.size());
    EXPECT_EQ(output[0].timestamp, 1000);
    EXPECT_EQ(output[0].key, input[0].key);
    EXPECT_EQ(output[0].value, input[0].value);
    ASSERT_EQ(output[0].headers.size(), 1U);
    EXPECT_EQ(output[0].headers[0].key, "h");
    EXPECT_EQ(output[0].headers[0].value, (std::vector<uint8_t>{1, 2, 3}));
    EXPECT_FALSE(output[1].key.has_value());
    EXPECT_TRUE(output[1].value.has_value());
    EXPECT_TRUE(output[1].value->empty());
}

TEST(KafkaProtocolTest, LegacyMessageRoundTrip) {
    const std::vector<Record> input = sampleRecords();
    for (int16_t version : {int16_t(0), int16_t(1)}) {
        const std::vector<uint8_t> messages = encodeMessageSet(input, version);
        const std::vector<ProducedRecord> output = decodeProduceRecords(messages, version, 1024);
        ASSERT_EQ(output.size(), input.size());
        EXPECT_EQ(output[0].key, input[0].key);
        EXPECT_EQ(output[0].value, input[0].value);
        EXPECT_FALSE(output[1].key.has_value());
        if (version == 1) EXPECT_EQ(output[0].timestamp, input[0].timestamp);
    }
}

TEST(KafkaProtocolTest, DecodesMinimumSizeRecordBatch) {
    const std::vector<uint8_t> batch = {
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x3a, 0x00, 0x00, 0x00, 0x00,
        0x02, 0x8a, 0x65, 0xa9, 0x0a, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xa2, 0x16,
        0x4d, 0x13, 0x13, 0x00, 0x00, 0x01, 0xa2, 0x16,
        0x4d, 0x13, 0x13, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x01, 0x10, 0x00, 0x00,
        0x00, 0x02, 0x6b, 0x02, 0x76, 0x00,
    };
    const std::vector<ProducedRecord> output = decodeProduceRecords(batch, 3, 1024, true);
    ASSERT_EQ(output.size(), 1U);
    EXPECT_EQ(output[0].key, (std::vector<uint8_t>{'k'}));
    EXPECT_EQ(output[0].value, (std::vector<uint8_t>{'v'}));
    EXPECT_EQ(output[0].producerID, 2);
    EXPECT_EQ(output[0].producerEpoch, 0);
    EXPECT_EQ(output[0].sequence, 0);
    EXPECT_THROW(decodeProduceRecords(batch, 3, 69), BrokerError);
}

TEST(KafkaProtocolTest, RejectsCorruptBatchAndTruncatedMessageSet) {
    std::vector<uint8_t> batch = encodeMessageSet(sampleRecords(), 2);
    batch.back() ^= 1;
    EXPECT_THROW(decodeProduceRecords(batch, 3, 1024), BrokerError);
    EXPECT_THROW(decodeProduceRecords(std::vector<uint8_t>{1, 2, 3}, 3, 1024), BrokerError);
}

TEST(KafkaProtocolTest, EncodesSingleRecordControlBatchesWithV0Key) {
    Record abort;
    abort.offset = 7;
    abort.timestamp = 1000;
    abort.producerID = 42;
    abort.producerEpoch = 3;
    abort.sequence = 0;
    abort.transactional = true;
    abort.control = true;
    Record commit = abort;
    commit.offset = 8;
    commit.controlCommit = true;
    const std::vector<uint8_t> bytes = encodeMessageSet({abort, commit}, 4);
    Reader batches(bytes);
    std::vector<std::vector<uint8_t>> encodedBatches;
    for (int16_t expectedType : {int16_t(0), int16_t(1)}) {
        const size_t batchStart = bytes.size() - batches.remaining();
        const int64_t baseOffset = batches.readInt64();
        const int32_t length = batches.readInt32();
        EXPECT_EQ(baseOffset, expectedType == 0 ? 7 : 8);
        EXPECT_EQ(length, 60);
        ASSERT_GE(length, 49);
        ASSERT_LE(batchStart + 12 + static_cast<size_t>(length), bytes.size());
        encodedBatches.push_back(std::vector<uint8_t>(
            bytes.begin() + static_cast<std::ptrdiff_t>(batchStart),
            bytes.begin() + static_cast<std::ptrdiff_t>(batchStart + 12 + length)));
        (void)batches.readRaw(static_cast<size_t>(length));
        Reader body(encodedBatches.back());
        (void)body.readRaw(21);
        EXPECT_EQ(body.readInt16(), 0x30);
        EXPECT_EQ(body.readInt32(), 0);
        EXPECT_EQ(body.readInt64(), 1000);
        EXPECT_EQ(body.readInt64(), 1000);
        EXPECT_EQ(body.readInt64(), 42);
        EXPECT_EQ(body.readInt16(), 3);
        EXPECT_EQ(body.readInt32(), 0);
        EXPECT_EQ(body.readInt32(), 1);
        const int32_t recordLength = body.readVarint();
        Reader record(body.readRaw(static_cast<size_t>(recordLength)));
        (void)record.readInt8();
        EXPECT_EQ(record.readVarint(), 0);
        EXPECT_EQ(record.readVarint(), 0);
        ASSERT_EQ(record.readVarint(), 4);
        const std::vector<uint8_t> keyBytes = record.readRaw(4);
        EXPECT_EQ(keyBytes, (std::vector<uint8_t>{0, 0, 0, static_cast<uint8_t>(expectedType)}));
        Reader key(keyBytes);
        EXPECT_EQ(key.readInt16(), 0);
        EXPECT_EQ(key.readInt16(), expectedType);
        EXPECT_EQ(record.readVarint(), -1);
        EXPECT_EQ(record.readVarint(), 0);
        EXPECT_EQ(record.remaining(), 0U);
        EXPECT_EQ(baseOffset, expectedType == 0 ? 7 : 8);
    }
    EXPECT_EQ(batches.remaining(), 0U);
    for (const auto& batch : encodedBatches) {
        EXPECT_THROW(decodeProduceRecords(batch, 3, 1024, true), BrokerError);
    }
    EXPECT_EQ(encodeMessageSet({abort, commit}, 1).size(), 0U);
}

} // namespace Kafka
