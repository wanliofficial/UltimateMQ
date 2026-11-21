#include <gtest/gtest.h>
#include "MQTTSN.h"
#include <cstring>

using namespace MQTT;
using namespace MQTT::MQTTSN;

namespace {

std::vector<uint8_t> datagramOf(std::initializer_list<uint8_t> bytes) {
    return std::vector<uint8_t>(bytes.begin(), bytes.end());
}

//! A QoS 0 PUBLISH body of exactly \p payloadOctets payload octets: flags (1),
//! topic id (2), then the payload.
std::vector<uint8_t> publishBodyOf(size_t payloadOctets) {
    std::vector<uint8_t> body;
    body.push_back(0x00);       // flags: QoS 0, normal topic id
    body.push_back(0x00);       // topic id 0x0001
    body.push_back(0x01);
    body.insert(body.end(), payloadOctets, 0x5A);
    return body;
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
    // §5.2.1: "the Length field is either 1- or 3-octet long and specifies the
    // total number of octets contained in the message (including the Length
    // field itself). If the first octet of the Length field is coded 0x01 then
    // the Length field is 3-octet long."
    const std::vector<uint8_t> body = publishBodyOf(297);
    const std::vector<uint8_t> datagram = encode(MsgType::PUBLISH, body);
    ASSERT_EQ(datagram.size(), body.size() + 4u);
    EXPECT_EQ(datagram[0], 0x01) << "the 3-octet marker is 0x01, not 0xFF";
    // The declared total counts the 3 length octets, the MsgType and the body.
    EXPECT_EQ(datagram[1], static_cast<uint8_t>((body.size() + 4) >> 8));
    EXPECT_EQ(datagram[2], static_cast<uint8_t>((body.size() + 4) & 0xFF));
    EXPECT_EQ(datagram[3], static_cast<uint8_t>(MsgType::PUBLISH));
    // A decoded message must re-parse: an encoder the decoder rejects would make
    // every datagram over 255 octets undeliverable.
    Parsed parsed;
    ASSERT_TRUE(parse(datagram.data(), datagram.size(), &parsed));
    EXPECT_EQ(parsed.type, MsgType::PUBLISH);
    EXPECT_EQ(parsed.data.size(), 297u);
}

TEST(MQTTSNCodecTest, LongDatagramRoundTripsAtEveryLengthBoundary)
{
    // 253 body octets is the largest message that fits the one-octet form
    // (253 + 2 = 255); 254 is the first that needs the three-octet form.
    for (const size_t bodyOctets : {size_t(3), size_t(4), size_t(253), size_t(254),
                                    size_t(255), size_t(300), size_t(4096)}) {
        const std::vector<uint8_t> body = publishBodyOf(bodyOctets - 3);
        ASSERT_EQ(body.size(), bodyOctets);
        const std::vector<uint8_t> datagram = encode(MsgType::PUBLISH, body);
        const size_t expectedTotal = bodyOctets + 2 <= 255 ? bodyOctets + 2 : bodyOctets + 4;
        ASSERT_EQ(datagram.size(), expectedTotal) << "body=" << bodyOctets;

        Parsed parsed;
        ASSERT_TRUE(parse(datagram.data(), datagram.size(), &parsed)) << "body=" << bodyOctets;
        EXPECT_EQ(parsed.type, MsgType::PUBLISH) << "body=" << bodyOctets;
        // The payload is everything after the flags and the topic id.
        EXPECT_EQ(parsed.data.size(), bodyOctets - 3) << "body=" << bodyOctets;
    }
}

TEST(MQTTSNCodecTest, Length255StaysInTheOneOctetForm)
{
    // 0xFF in the first octet is the one-octet length 255, not a marker: a
    // 255-octet message (253 body octets) is a legal short datagram.
    const std::vector<uint8_t> body = publishBodyOf(250);
    ASSERT_EQ(body.size(), 253u);
    const std::vector<uint8_t> datagram = encode(MsgType::PUBLISH, body);
    ASSERT_EQ(datagram.size(), 255u);
    EXPECT_EQ(datagram[0], 0xFF);
    EXPECT_EQ(datagram[1], static_cast<uint8_t>(MsgType::PUBLISH));

    Parsed parsed;
    ASSERT_TRUE(parse(datagram.data(), datagram.size(), &parsed));
    EXPECT_EQ(parsed.data.size(), 250u);
}

TEST(MQTTSNCodecTest, OverlongMessageIsRefusedRatherThanMangled)
{
    // §5.2.1: "The 3-octet format allows the encoding of message lengths up to
    // 65535 octets", and MQTT-SN has no fragmentation, so a bigger message
    // cannot be sent at all. Emitting it would wrap the 16-bit length and give
    // the peer a datagram whose length field lies; refuse instead.
    EXPECT_TRUE(encode(MsgType::PUBLISH, publishBodyOf(65529)).empty()); // total 65536

    // The largest representable message (65535 octets) still encodes, and the
    // length field it emits is the real total.
    const std::vector<uint8_t> body = publishBodyOf(65528); // 65531 + 4 = 65535
    ASSERT_EQ(body.size(), 65531u);
    const std::vector<uint8_t> datagram = encode(MsgType::PUBLISH, body);
    ASSERT_EQ(datagram.size(), 65535u);
    EXPECT_EQ(datagram[0], 0x01);
    EXPECT_EQ(datagram[1], 0xFF);
    EXPECT_EQ(datagram[2], 0xFF);
    Parsed parsed;
    ASSERT_TRUE(parse(datagram.data(), datagram.size(), &parsed));
    EXPECT_EQ(parsed.data.size(), 65528u);
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
