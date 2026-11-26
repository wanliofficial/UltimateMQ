#include "AMQPBroker.h"
#include "AMQPWire.h"
#include <gtest/gtest.h>
#include <memory>

using namespace AMQP;

TEST(AMQPWireTests, MethodFrameRoundTrip) {
    const std::vector<uint8_t> arguments = {0, 1, 2, 3};
    const auto encoded = encodeMethod(CLASS_CHANNEL, 10, arguments, 7, 4096);

    ASSERT_EQ(encoded.size(), 16u);
    EXPECT_EQ(encoded[0], FRAME_METHOD);
    EXPECT_EQ(encoded[1], 0);
    EXPECT_EQ(encoded[2], 7);
    EXPECT_EQ(encoded[3], 0);
    EXPECT_EQ(encoded[4], 0);
    EXPECT_EQ(encoded[5], 0);
    EXPECT_EQ(encoded[6], 8);
    EXPECT_EQ(encoded[7], 0);
    EXPECT_EQ(encoded[8], 20);
    EXPECT_EQ(encoded[9], 0);
    EXPECT_EQ(encoded[10], 10);
    EXPECT_EQ(encoded[11], 0);
    EXPECT_EQ(encoded[15], FRAME_END);

    Frame frame;
    frame.type = FRAME_METHOD;
    frame.channel = 7;
    frame.payload.assign(encoded.begin() + 7, encoded.end() - 1);
    const Method decoded = decodeMethod(frame);
    EXPECT_EQ(decoded.classID, CLASS_CHANNEL);
    EXPECT_EQ(decoded.methodID, 10u);
    EXPECT_EQ(decoded.arguments, arguments);
}

TEST(AMQPWireTests, ContentHeaderUsesZeroMethodWeight) {
    const std::vector<uint8_t> properties = {0, 0};
    const auto encoded = encodeContentHeader(3, CLASS_BASIC, 71, 0x0102030405060708ULL,
                                             properties, 4096);

    ASSERT_EQ(encoded.size(), 22u);
    EXPECT_EQ(encoded[0], FRAME_HEADER);
    EXPECT_EQ(encoded[1], 0);
    EXPECT_EQ(encoded[2], 3);
    EXPECT_EQ(encoded[7], 0);
    EXPECT_EQ(encoded[8], CLASS_BASIC);
    EXPECT_EQ(encoded[9], 0);
    EXPECT_EQ(encoded[10], 0);
    EXPECT_EQ(encoded[19], properties[0]);
    EXPECT_EQ(encoded[20], properties[1]);
    EXPECT_EQ(encoded[21], FRAME_END);
}

TEST(AMQPWireTests, FieldTableRoundTrip) {
    auto nested = std::make_shared<FieldTable>();
    nested->emplace("enabled", true);
    nested->emplace("name", std::string("value"));
    auto table = std::make_shared<FieldTable>();
    table->emplace("nested", nested);
    table->emplace("count", uint32_t{42});
    table->emplace("ratio", 1.5);
    table->emplace("payload", std::vector<uint8_t>{0, 1, 255});

    Writer writer;
    writer.table(table);
    Reader reader(writer.data());
    const auto decoded = reader.table();
    EXPECT_TRUE(reader.done());
    ASSERT_EQ(decoded->size(), table->size());
    ASSERT_EQ(decoded->at("count").index(), 7u);
    ASSERT_EQ(decoded->at("ratio").index(), 11u);
    ASSERT_EQ(decoded->at("payload").index(), 14u);
    EXPECT_EQ(std::get<uint32_t>(decoded->at("count")), 42u);
    EXPECT_DOUBLE_EQ(std::get<double>(decoded->at("ratio")), 1.5);
    EXPECT_EQ(std::get<std::vector<uint8_t>>(decoded->at("payload")),
              (std::vector<uint8_t>{0, 1, 255}));
    const auto decodedNestedValue = decoded->at("nested");
    ASSERT_EQ(decodedNestedValue.index(), 16u);
    ASSERT_TRUE(std::holds_alternative<std::shared_ptr<FieldTable>>(decodedNestedValue));
    const auto decodedNested = std::get<std::shared_ptr<FieldTable>>(decodedNestedValue);
    ASSERT_TRUE(std::holds_alternative<bool>(decodedNested->at("enabled")));
    ASSERT_TRUE(std::holds_alternative<std::string>(decodedNested->at("name")));
    EXPECT_TRUE(std::get<bool>(decodedNested->at("enabled")));
    EXPECT_EQ(std::get<std::string>(decodedNested->at("name")), "value");
}

TEST(AMQPWireTests, FrameMaxIncludesHeaderAndTerminator) {
    EXPECT_EQ(encodeFrame(FRAME_BODY, 1, std::vector<uint8_t>(4088), 4096).size(), 4096u);
    EXPECT_THROW(encodeFrame(FRAME_BODY, 1, std::vector<uint8_t>(4089), 4096), WireError);
    EXPECT_THROW(encodeFrame(FRAME_BODY, 1, {}, 7), WireError);
}

TEST(AMQPWireTests, RejectsTruncatedAndTrailingInput) {
    const std::vector<uint8_t> oneByte = {0};
    EXPECT_THROW((Reader(oneByte).u16()), WireError);
    const std::vector<uint8_t> value = {1};
    Reader reader(value);
    EXPECT_EQ(reader.u8(), 1u);
    EXPECT_TRUE(reader.done());
    EXPECT_NO_THROW(reader.requireEnd());
    const std::vector<uint8_t> trailing = {1, 2};
    Reader trailingReader(trailing);
    (void)trailingReader.u8();
    EXPECT_THROW(trailingReader.requireEnd(), WireError);
    EXPECT_THROW((Writer{}.shortString(std::string(256, 'x'))), WireError);
}

TEST(AMQPBrokerTests, TopicWildcardsMatchWholeWords) {
    EXPECT_TRUE(Broker::topicMatches("orders._new", "orders._new"));
    EXPECT_TRUE(Broker::topicMatches("orders.eu-#", "orders.eu-#"));
    EXPECT_TRUE(Broker::topicMatches("a.#.b", "a.x.y.b"));
    EXPECT_FALSE(Broker::topicMatches("a.#.b", "a.x.y.c"));
    EXPECT_TRUE(Broker::topicMatches("orders.*", "orders.created"));
    EXPECT_FALSE(Broker::topicMatches("orders.*", "orders.created.detail"));
    EXPECT_FALSE(Broker::topicMatches("orders.*", "payments.created"));
    EXPECT_TRUE(Broker::topicMatches("orders.#", "orders"));
    EXPECT_TRUE(Broker::topicMatches("orders.#", "orders.created.detail"));
    EXPECT_TRUE(Broker::topicMatches("#", "anything.at.all"));
    EXPECT_FALSE(Broker::topicMatches("orders.#", "payments.created"));
}

TEST(AMQPBrokerTests, DirectAndFanoutRouting) {
    Broker broker;
    broker.declareExchange("direct.test", "direct", false, false, false, false, 1);
    broker.declareExchange("fanout.test", "fanout", false, false, false, false, 1);
    ASSERT_EQ(broker.declareQueue("queue.a", false, false, false, false, 1), "queue.a");
    ASSERT_EQ(broker.declareQueue("queue.b", false, false, false, false, 1), "queue.b");
    broker.bindQueue("queue.a", "direct.test", "key", 1);
    broker.bindQueue("queue.a", "fanout.test", "", 1);
    broker.bindQueue("queue.b", "fanout.test", "", 1);

    Message message;
    message.exchange = "direct.test";
    message.routingKey = "key";
    message.body = {'x'};
    EXPECT_TRUE(broker.publish(message, "", false, false, 1).routed);
    EXPECT_EQ(broker.queueMessageCount("queue.a"), 1u);
    EXPECT_EQ(broker.queueMessageCount("queue.b"), 0u);

    message.exchange = "fanout.test";
    broker.publish(message, "", false, false, 1);
    EXPECT_EQ(broker.queueMessageCount("queue.a"), 2u);
    EXPECT_EQ(broker.queueMessageCount("queue.b"), 1u);
}

TEST(AMQPBrokerTests, ImmediateWithoutConsumersDoesNotQueue) {
    Broker broker;
    broker.declareQueue("queue.immediate", false, false, false, false, 1);
    Message message;
    message.routingKey = "queue.immediate";
    message.body = {'x'};
    const PublishResult result = broker.publish(message, "", false, true, 1);
    EXPECT_TRUE(result.routed);
    EXPECT_EQ(result.immediateDeliveries, 0u);
    EXPECT_EQ(broker.queueMessageCount("queue.immediate"), 0u);
}

TEST(AMQPBrokerTests, QueueRedeclarationAndLifecycle) {
    Broker broker;
    broker.declareQueue("queue.lifecycle", false, true, false, false, 1);
    EXPECT_THROW(broker.declareQueue("queue.lifecycle", false, false, false, false, 1),
                 BrokerError);
    EXPECT_THROW(broker.declareQueue("queue.missing", true, false, false, false, 1),
                 BrokerError);

    Message message;
    message.routingKey = "queue.lifecycle";
    message.body = {'a'};
    message.body.push_back('b');
    broker.publish(message, "", false, false, 1);
    broker.publish(message, "", false, false, 1);
    EXPECT_EQ(broker.purgeQueue("queue.lifecycle", 1), 2u);
    EXPECT_EQ(broker.deleteQueue("queue.lifecycle", false, false, 1), 0u);
    EXPECT_THROW(broker.queueMessageCount("queue.lifecycle"), BrokerError);
}

TEST(AMQPBrokerTests, QueueRedeclarationChecksAutoDeleteAndArguments) {
    Broker broker;
    Writer firstArguments;
    auto table = std::make_shared<FieldTable>();
    table->emplace("x-max-length", uint32_t{10});
    firstArguments.table(table);
    const auto arguments = firstArguments.data();
    broker.declareQueue("queue.equivalent", false, true, false, true, 1, arguments);
    EXPECT_NO_THROW(broker.declareQueue("queue.equivalent", false, true, false, true, 1,
                                        arguments));
    EXPECT_THROW(broker.declareQueue("queue.equivalent", false, true, false, false, 1,
                                     arguments), BrokerError);
    EXPECT_THROW(broker.declareQueue("queue.equivalent", false, true, false, true, 1),
                 BrokerError);
}

TEST(AMQPBrokerTests, PassiveDeclarationHonorsExclusiveOwnership) {
    Broker broker;
    broker.declareQueue("queue.exclusive", false, false, true, false, 1);
    EXPECT_EQ(broker.declareQueue("queue.exclusive", true, false, false, false, 1),
              "queue.exclusive");
    EXPECT_THROW(broker.declareQueue("queue.exclusive", true, false, false, false, 2),
                 BrokerError);
}

TEST(AMQPBrokerTests, AutoDeleteExchangeIsRemovedAfterLastBinding) {
    Broker broker;
    broker.declareExchange("auto.exchange", "direct", false, false, true, false, 1);
    broker.declareQueue("queue.auto", false, false, false, false, 1);
    broker.bindQueue("queue.auto", "auto.exchange", "key", 1);
    EXPECT_NO_THROW(broker.declareExchange("auto.exchange", "direct", true, false, true,
                                           false, 1));
    broker.unbindQueue("queue.auto", "auto.exchange", "key");
    EXPECT_THROW(broker.declareExchange("auto.exchange", "direct", true, false, true,
                                        false, 1), BrokerError);
}
