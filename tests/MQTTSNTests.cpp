#include <gtest/gtest.h>
#include "MQTTSN.h"
#include <cstring>

using namespace MQTT;
using namespace MQTT::MQTTSN;

namespace {

std::vector<uint8_t> datagramOf(std::initializer_list<uint8_t> bytes) {
    return std::vector<uint8_t>(bytes.begin(), bytes.end());
}

} // namespace

TEST(MQTTSNFlagsTest, EncodeDecodeRoundTrip)
{
    Flags flags;
    flags.dup = true;
    flags.qos = 2;
    flags.retain = true;
    flags.will = true;
    flags.cleanSession = true;
    flags.topicIdType = TopicIdType::PREDEFINED;
    const uint8_t byte = flags.encode();
    EXPECT_EQ(byte, 0xDD);
    const Flags back = Flags::decode(byte);
    EXPECT_TRUE(back.dup);
    EXPECT_EQ(back.qos, 2);
    EXPECT_TRUE(back.retain);
    EXPECT_TRUE(back.will);
    EXPECT_TRUE(back.cleanSession);
    EXPECT_EQ(back.topicIdType, TopicIdType::PREDEFINED);
}

TEST(MQTTSNCodecTest, ShortDatagramUsesOneByteLength)
{
    // A 3-byte CONNACK: length 3, MsgType 0x05, return code 0x00
    const std::vector<uint8_t> datagram = encode(MsgType::CONNACK, {0x00});
    EXPECT_EQ(datagram, (std::vector<uint8_t>{0x03, 0x05, 0x00}));
}

TEST(MQTTSNCodecTest, LongDatagramUsesThreeByteLength)
{
    // §5.2.1: a total of 0xFF or more switches to 0xFF + a 16-bit length
    std::vector<uint8_t> body(300, 0xAB);
    const std::vector<uint8_t> datagram = encode(MsgType::PUBLISH, body);
    ASSERT_EQ(datagram.size(), body.size() + 4u);
    EXPECT_EQ(datagram[0], 0xFF);
    EXPECT_EQ(datagram[1], static_cast<uint8_t>((body.size() + 2) >> 8));
    EXPECT_EQ(datagram[2], static_cast<uint8_t>((body.size() + 2) & 0xFF));
    EXPECT_EQ(datagram[3], static_cast<uint8_t>(MsgType::PUBLISH));
}

TEST(MQTTSNCodecTest, ConnectRoundTrip)
{
    // Flags, ProtocolId 1, Duration 30, ClientId "sensor1"
    const std::vector<uint8_t> wire = datagramOf(
        {0x0D, static_cast<uint8_t>(MsgType::CONNECT), 0x2F, 0x01, 0x00, 0x1E,
         's', 'e', 'n', 's', 'o', 'r', '1'});
    Parsed parsed;
    ASSERT_TRUE(parse(wire.data(), wire.size(), &parsed));
    EXPECT_EQ(parsed.type, MsgType::CONNECT);
    EXPECT_TRUE(parsed.flags.cleanSession);
    EXPECT_TRUE(parsed.flags.will);
    EXPECT_EQ(parsed.flags.qos, 1);
    EXPECT_EQ(parsed.flags.topicIdType, TopicIdType::NAME);
    EXPECT_EQ(parsed.protocolId, 0x01);
    EXPECT_EQ(parsed.duration, 30);
    EXPECT_EQ(parsed.clientId, "sensor1");
}

TEST(MQTTSNCodecTest, PublishCarriesMsgIdOnlyAboveQosZero)
{
    // QoS 1: flags 0x20, topic id 0x0007, msg id 0x0042, payload "ab"
    const std::vector<uint8_t> wire = datagramOf(
        {0x09, static_cast<uint8_t>(MsgType::PUBLISH), 0x21, 0x00, 0x07, 0x00,
         0x42, 'a', 'b'});
    Parsed parsed;
    ASSERT_TRUE(parse(wire.data(), wire.size(), &parsed));
    EXPECT_EQ(parsed.topicId, 7);
    EXPECT_EQ(parsed.msgId, 0x42);
    EXPECT_EQ(parsed.data, (std::vector<uint8_t>{'a', 'b'}));

    // QoS 0: no MsgId field, everything after the topic id is payload
    const std::vector<uint8_t> qos0 = datagramOf(
        {0x08, static_cast<uint8_t>(MsgType::PUBLISH), 0x00, 0x00, 0x07, 'a',
         'b', 'c'});
    Parsed parsedQos0;
    ASSERT_TRUE(parse(qos0.data(), qos0.size(), &parsedQos0));
    EXPECT_EQ(parsedQos0.msgId, 0);
    EXPECT_EQ(parsedQos0.data, (std::vector<uint8_t>{'a', 'b', 'c'}));
}

TEST(MQTTSNCodecTest, SubscribeByNameCarriesFilterText)
{
    const std::string filter = "sensors/+/temp";
    std::vector<uint8_t> wire;
    wire.push_back(static_cast<uint8_t>(3 + 2 + filter.size()));
    wire.push_back(static_cast<uint8_t>(MsgType::SUBSCRIBE));
    wire.push_back(0x03); // QoS 0, topic name type
    wire.push_back(0x00);
    wire.push_back(0x05);
    wire.insert(wire.end(), filter.begin(), filter.end());
    Parsed parsed;
    ASSERT_TRUE(parse(wire.data(), wire.size(), &parsed));
    EXPECT_EQ(parsed.msgId, 5);
    EXPECT_EQ(parsed.flags.topicIdType, TopicIdType::NAME);
    EXPECT_EQ(parsed.topic, filter);
}

TEST(MQTTSNCodecTest, SubscribeByPredefinedIdCarriesTopicId)
{
    const std::vector<uint8_t> wire = datagramOf(
        {0x07, static_cast<uint8_t>(MsgType::SUBSCRIBE), 0x01, 0x00, 0x09,
         0x00, 0x03});
    Parsed parsed;
    ASSERT_TRUE(parse(wire.data(), wire.size(), &parsed));
    EXPECT_EQ(parsed.flags.topicIdType, TopicIdType::PREDEFINED);
    EXPECT_EQ(parsed.topicId, 3);
    EXPECT_TRUE(parsed.topic.empty());
}

TEST(MQTTSNCodecTest, LengthMismatchIsRejected)
{
    // Declared length 4, actual datagram 5 bytes: the length field must match
    // the datagram exactly (§5.2.1).
    const std::vector<uint8_t> wire = {0x04, static_cast<uint8_t>(MsgType::PINGREQ),
                                       0x01, 0x02, 0x03};
    Parsed parsed;
    EXPECT_FALSE(parse(wire.data(), wire.size(), &parsed));
}

TEST(MQTTSNCodecTest, UnknownMessageTypeIsRejected)
{
    const std::vector<uint8_t> wire = {0x03, 0x55, 0x00};
    Parsed parsed;
    EXPECT_FALSE(parse(wire.data(), wire.size(), &parsed));
}

TEST(MQTTSNCodecTest, DisconnectWithAndWithoutDuration)
{
    Parsed parsed;
    const std::vector<uint8_t> plain = {0x02, static_cast<uint8_t>(MsgType::DISCONNECT)};
    ASSERT_TRUE(parse(plain.data(), plain.size(), &parsed));
    EXPECT_EQ(parsed.duration, 0);
    const std::vector<uint8_t> sleeping = datagramOf(
        {0x04, static_cast<uint8_t>(MsgType::DISCONNECT), 0x01, 0xF4});
    ASSERT_TRUE(parse(sleeping.data(), sleeping.size(), &parsed));
    EXPECT_EQ(parsed.duration, 500); // 0x01F4: asleep for 500 seconds
}

TEST(MQTTSNCodecTest, WillTopicUpdateCarriesFlagsAndTopic)
{
    // Flags 0x10 (Will Retain, QoS 0), topic "sensor1/will"
    const std::vector<uint8_t> wire = datagramOf(
        {0x0F, static_cast<uint8_t>(MsgType::WILLTOPICUPD), 0x10,
         's', 'e', 'n', 's', 'o', 'r', '1', '/', 'w', 'i', 'l', 'l'});
    Parsed parsed;
    ASSERT_TRUE(parse(wire.data(), wire.size(), &parsed));
    EXPECT_EQ(parsed.type, MsgType::WILLTOPICUPD);
    EXPECT_TRUE(parsed.flags.retain);
    EXPECT_EQ(parsed.flags.qos, 0);
    EXPECT_EQ(parsed.topic, "sensor1/will");
}

TEST(MQTTSNCodecTest, WillMessageUpdateCarriesPayload)
{
    const std::vector<uint8_t> wire = datagramOf(
        {0x09, static_cast<uint8_t>(MsgType::WILLMSGUPD),
         'o', 'f', 'f', 'l', 'i', 'n', 'e'});
    Parsed parsed;
    ASSERT_TRUE(parse(wire.data(), wire.size(), &parsed));
    EXPECT_EQ(parsed.type, MsgType::WILLMSGUPD);
    EXPECT_EQ(parsed.data, std::vector<uint8_t>({'o', 'f', 'f', 'l', 'i', 'n', 'e'}));
}

TEST(MQTTSNCodecTest, WillUpdateResponsesCarryReturnCode)
{
    Parsed parsed;
    const std::vector<uint8_t> accepted = encode(MsgType::WILLTOPICRESP,
                                                 {static_cast<uint8_t>(ReturnCode::ACCEPTED)});
    ASSERT_TRUE(parse(accepted.data(), accepted.size(), &parsed));
    EXPECT_EQ(parsed.type, MsgType::WILLTOPICRESP);
    EXPECT_EQ(parsed.returnCode, ReturnCode::ACCEPTED);

    const std::vector<uint8_t> rejected = encode(MsgType::WILLMSGRESP,
                                                 {static_cast<uint8_t>(ReturnCode::INVALID_TOPIC_ID)});
    ASSERT_TRUE(parse(rejected.data(), rejected.size(), &parsed));
    EXPECT_EQ(parsed.type, MsgType::WILLMSGRESP);
    EXPECT_EQ(parsed.returnCode, ReturnCode::INVALID_TOPIC_ID);
}

TEST(MQTTSNCodecTest, PubackReturnCodeIsParsed)
{
    // topic id 0x0007, msg id 0x0042, return code 1 (rejected congestion)
    const std::vector<uint8_t> wire = datagramOf(
        {0x07, static_cast<uint8_t>(MsgType::PUBACK), 0x00, 0x07,
         0x00, 0x42, 0x01});
    Parsed parsed;
    ASSERT_TRUE(parse(wire.data(), wire.size(), &parsed));
    EXPECT_EQ(parsed.topicId, 7);
    EXPECT_EQ(parsed.msgId, 0x42);
    EXPECT_EQ(parsed.returnCode, ReturnCode::REJECTED_CONGESTION);
}
