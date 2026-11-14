#include <gtest/gtest.h>
#include "../src/Frame.h"
#include <vector>
#include <stdexcept>

namespace MQTT {

class FrameTest : public ::testing::Test 
{
protected:
    Frame instance;
    Frame *frame = &instance;
};

TEST_F(FrameTest, ParseConnectPacket) 
{
    std::vector<uint8_t> payload = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x05,                            // Protocol version
        0b00000010,                      // Connect flags (10000010)
                                         // Username flag: 0
                                         // Password flag: 0
                                         // Will retain: 0
                                         // Will QoS: 00
                                         // Will flag: 0
                                         // Clean start: 1
                                         // Reserved: 0
        0x00, 0x3C,                      // Keep alive
        0x00,                            // Properties length
        0x00, 0x03, 'a', 'b', 'c'        // Client ID
    };

    auto packet = frame->parseConnect(payload.data(), payload.size());

    EXPECT_EQ(packet.protocolName, "MQTT");
    EXPECT_EQ(packet.protocolVersion, Version::MQTT5);
    EXPECT_TRUE(packet.cleanStart);
    EXPECT_FALSE(packet.willFlag);
    EXPECT_EQ(packet.willQos, QoS::QOS_0);
    EXPECT_FALSE(packet.willRetain);
    EXPECT_EQ(packet.keepAlive, 60);
    EXPECT_EQ(packet.clientId, "abc");
}

TEST_F(FrameTest, ParsePublishPacket) 
{
    FixedHeader header;
    header.type = PacketType::PUBLISH;
    header.dup = false;
    header.qos = MQTT::QoS::QOS_1;
    header.retain = true;

    std::vector<uint8_t> payload = {
        0x00, 0x05, 't', 'o', 'p', 'i', 'c',  // Topic name
        0x00, 0x0A,                           // Packet identifier
        0x00,                                 // Properties length
        'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'  // Payload
    };

    auto packet = frame->parsePublish(header, payload.data(), payload.size());

    EXPECT_EQ(packet.topicName, "topic");
    EXPECT_EQ(packet.packetId, 10);
    EXPECT_EQ(packet.payload, std::vector<uint8_t>({'H', 'e', 'l', 'l', 'o', ' ', 'W', 'o', 'r', 'l', 'd'}));
}

TEST_F(FrameTest, ParseSubscribePacket) 
{
    std::vector<uint8_t> payload = {
        0x00, 0x0A,                           // Packet identifier
        0x00,                                 // Properties length
        0x00, 0x05, 't', 'o', 'p', 'i', 'c',  // Topic filter
        0x01                                  // QoS
    };

    auto packet = frame->parseSubscribe(payload.data(), payload.size());

    EXPECT_EQ(packet.packetId, 10);
    ASSERT_EQ(packet.subscriptions.size(), 1);
    EXPECT_EQ(packet.subscriptions[0].first, "topic");
    EXPECT_EQ(packet.subscriptions[0].second.maximumQos, QoS::QOS_1);
}

TEST_F(FrameTest, ParseUnsubscribePacket) 
{
    std::vector<uint8_t> payload = {
        0x00, 0x0B,                           // Packet identifier
        0x00,                                 // Properties length
        0x00, 0x05, 't', 'o', 'p', 'i', 'c'   // Topic filter
    };

    auto packet = frame->parseUnsubscribe(payload.data(), payload.size());

    EXPECT_EQ(packet.packetId, 11);
    ASSERT_EQ(packet.topicFilters.size(), 1);
    EXPECT_EQ(packet.topicFilters[0], "topic");
}

TEST_F(FrameTest, ParsePingReqPacket) 
{
    std::vector<uint8_t> bytes = {0xC0, 0x00};
    auto packet = frame->parsePingreq(bytes.data(), bytes.size());

    EXPECT_EQ(packet.type, PacketType::PINGREQ);
}

TEST_F(FrameTest, ParseDisconnectPacket)
{
    std::vector<uint8_t> payload = {
        0x00,  // Reason Code (Normal disconnection)
        0x00   // Properties length
    };

    auto packet = frame->parseDisconnect(payload.data(), payload.size());

    EXPECT_EQ(packet.type, PacketType::DISCONNECT);
    EXPECT_EQ(packet.reasonCode, ReasonCode::SUCCESS);
}

TEST_F(FrameTest, ParseDisconnectWithSessionExpiry)
{
    // DISCONNECT carrying a Session Expiry Interval property: reason code, then
    // a property length (0x06), then the property (id 0x11, length 0x04, 4 bytes).
    std::vector<uint8_t> payload = {
        0x00,                   // Reason Code (Normal disconnection)
        0x06,                   // Properties length
        0x11, 0x04, 0x00, 0x00, 0x0e, 0x10   // Session Expiry Interval = 3600
    };

    auto packet = frame->parseDisconnect(payload.data(), payload.size());

    EXPECT_EQ(packet.type, PacketType::DISCONNECT);
    ASSERT_EQ(packet.properties.count(PropertyID::SESSION_EXPIRY_INTERVAL), 1u);
    EXPECT_TRUE(std::holds_alternative<uint32_t>(
        packet.properties[PropertyID::SESSION_EXPIRY_INTERVAL]));
}

TEST_F(FrameTest, ParseConnectSessionExpiryProperty)
{
    // Regression guard: a four-byte integer property in CONNECT properties must
    // consume its length byte. Previously the parser skipped the length byte and
    // mis-read the value, throwing "Invalid property ID" on well-formed packets.
    std::vector<uint8_t> payload = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x05,                            // Protocol version
        0b00000010,                      // Connect flags (clean start)
        0x00, 0x3c,                      // Keep alive = 60
        0x06,                            // Properties length
        0x11, 0x04, 0x00, 0x00, 0x0e, 0x10,  // Session Expiry Interval = 3600
        0x00, 0x03, 'c', 'i', 'd'        // Client ID
    };

    auto packet = frame->parseConnect(payload.data(), payload.size());

    ASSERT_EQ(packet.properties.count(PropertyID::SESSION_EXPIRY_INTERVAL), 1u);
    EXPECT_TRUE(std::holds_alternative<uint32_t>(
        packet.properties[PropertyID::SESSION_EXPIRY_INTERVAL]));
    EXPECT_EQ(packet.clientId, "cid");
}

TEST_F(FrameTest, ParseAuthPacket) 
{
    std::vector<uint8_t> payload = {
        0x18,  // Reason Code (Continue authentication)
        0x00   // Properties length
    };

    auto packet = frame->parseAuth(payload.data(), payload.size());

    EXPECT_EQ(packet.type, PacketType::AUTH);
    EXPECT_EQ(packet.reasonCode, ReasonCode::CONTINUE_AUTHENTICATION);
}

TEST_F(FrameTest, SerializeConnectPacket) 
{
    ConnectPacket packet; 
    packet.type = PacketType::CONNECT;
    packet.protocolName = "MQTT";
    packet.protocolVersion = Version::MQTT5;
    packet.cleanStart = true;
    packet.keepAlive = 60;
    packet.clientId = "testclient";

    auto serialized = frame->serializeConnect(packet);

    ASSERT_EQ(serialized.size(), 24);
    EXPECT_EQ(serialized[0], 0x10);  // CONNECT packet type
    EXPECT_EQ(serialized[1], 22);    // Remaining length
    // ... Add more specific checks for the serialized packet
}

TEST_F(FrameTest, SerializePublishPacket) 
{
    PublishPacket packet;
    packet.type = PacketType::PUBLISH;
    packet.dup = false;
    packet.qos = MQTT::QoS::QOS_1;
    packet.retain = true;
    packet.topicName = "test/topic";
    packet.packetId = 1234;
    packet.payload = {'t', 'e', 's', 't'};

    auto serialized = frame->serializePublish(packet);

    ASSERT_EQ(serialized.size(), 21);
    EXPECT_EQ(serialized[0], 0x33);  // PUBLISH packet type with QoS 1 and retain
    EXPECT_EQ(serialized[1], 19);    // Remaining length
    // ... Add more specific checks for the serialized packet
}

TEST_F(FrameTest, DecodeRemainingLength) 
{
    std::vector<uint8_t> input = {0x7F};
    auto [length, bytes]= frame->decodeRemainingLength(input.data(), input.size());
    EXPECT_EQ(length, 127);
    EXPECT_EQ(bytes, 1);

    input = {0x80, 0x01};
    std::tie(length, bytes) = frame->decodeRemainingLength(input.data(), input.size());
    EXPECT_EQ(length, 128);
    EXPECT_EQ(bytes, 2);

    input = {0xFF, 0xFF, 0xFF, 0x7F};
    std::tie(length, bytes) = frame->decodeRemainingLength(input.data(), input.size());
    EXPECT_EQ(length, 268435455);
    EXPECT_EQ(bytes, 4);
}

TEST_F(FrameTest, EncodeRemainingLength) 
{
    auto encoded = frame->encodeRemainingLength(127);
    EXPECT_EQ(encoded, std::vector<uint8_t>({0x7F}));

    encoded = frame->encodeRemainingLength(128);
    EXPECT_EQ(encoded, std::vector<uint8_t>({0x80, 0x01}));

    encoded = frame->encodeRemainingLength(268435455); 
    EXPECT_EQ(encoded, std::vector<uint8_t>({0xFF, 0xFF, 0xFF, 0x7F}));
}

} // namespace MQTT

namespace MQTT {

TEST_F(FrameTest, ParseConnectPacketMqtt311HasNoProperties)
{
    std::vector<uint8_t> payload = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x04,                            // Protocol version 3.1.1
        0b00000010,                      // Connect flags: clean start, no will/user/pass
        0x00, 0x3C,                      // Keep alive
        0x00, 0x03, 'a', 'b', 'c'        // Client ID (no property length in v3.1.1)
    };

    auto packet = frame->parseConnect(payload.data(), payload.size());

    EXPECT_EQ(packet.protocolName, "MQTT");
    EXPECT_EQ(packet.protocolVersion, Version::MQTT311);
    EXPECT_EQ(packet.clientId, "abc");
    EXPECT_TRUE(packet.properties.empty());
}

TEST_F(FrameTest, SerializeConnackForMqtt311HasNoProperties)
{
    Frame v4(Version::MQTT311);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    auto data = v4.serialize(connack);
    std::vector<uint8_t> expected = {0x20, 0x02, 0x00, 0x00};
    EXPECT_EQ(data, expected);
}

TEST_F(FrameTest, SerializeConnackForMqtt311MapsFailureCodes)
{
    Frame v4(Version::MQTT311);
    // v5 reason codes must be translated into the v3.1.1 return code table
    ConnackPacket badCredentials(false, ReasonCode::BAD_USERNAME_OR_PASSWORD);
    EXPECT_EQ(v4.serialize(badCredentials), (std::vector<uint8_t>{0x20, 0x02, 0x00, 0x04}));

    ConnackPacket notAuthorized(false, ReasonCode::NOT_AUTHORIZED);
    EXPECT_EQ(v4.serialize(notAuthorized), (std::vector<uint8_t>{0x20, 0x02, 0x00, 0x05}));

    ConnackPacket badProtocol(false, ReasonCode::UNSUPPORTED_PROTOCOL_VERSION);
    EXPECT_EQ(v4.serialize(badProtocol), (std::vector<uint8_t>{0x20, 0x02, 0x00, 0x01}));
}

TEST_F(FrameTest, SerializeConnackForMqtt5KeepsV5ReasonCode)
{
    Frame v5(Version::MQTT5);
    ConnackPacket badCredentials(false, ReasonCode::BAD_USERNAME_OR_PASSWORD);
    EXPECT_EQ(v5.serialize(badCredentials), (std::vector<uint8_t>{0x20, 0x02, 0x00, 0x86}));
}

TEST_F(FrameTest, SerializeConnackForMqtt5AdvertisesMaximumPacketSize)
{
    Frame v5(Version::MQTT5);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    connack.properties[PropertyID::MAXIMUM_PACKET_SIZE] = static_cast<uint32_t>(1024 * 1024);
    // 0x20, remaining length 9, flags 0, reason 0, property length 6,
    // property id 0x27, length byte 4, then the 4-byte big-endian value
    // 0x00100000.
    EXPECT_EQ(v5.serialize(connack),
              (std::vector<uint8_t>{0x20, 0x09, 0x00, 0x00, 0x06, 0x27, 0x04,
                                    0x00, 0x10, 0x00, 0x00}));
}

TEST_F(FrameTest, SerializeConnackAdvertisesTopicAliasMaximumWithLengthByte)
{
    Frame v5(Version::MQTT5);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    connack.properties[PropertyID::TOPIC_ALIAS_MAXIMUM] = static_cast<uint16_t>(16);
    // 0x20, remaining length 7, flags 0, reason 0, property length 4,
    // property id 0x22, length byte 2, then the 2-byte value 0x0010.
    EXPECT_EQ(v5.serialize(connack),
              (std::vector<uint8_t>{0x20, 0x07, 0x00, 0x00, 0x04, 0x22, 0x02, 0x00, 0x10}));
}

TEST_F(FrameTest, SerializeConnackForMqtt311IgnoresProperties){
    Frame v4(Version::MQTT311);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    connack.properties[PropertyID::MAXIMUM_PACKET_SIZE] = static_cast<uint32_t>(1024 * 1024);
    // MQTT 3.1.1 has no property section, so it is dropped entirely.
    EXPECT_EQ(v4.serialize(connack), (std::vector<uint8_t>{0x20, 0x02, 0x00, 0x00}));
}

TEST_F(FrameTest, SerializePublishForMqtt311HasNoProperties)
{
    Frame v4(Version::MQTT311);
    PublishPacket publish("t", std::vector<uint8_t>{'h', 'i'}, QoS::QOS_0, false);
    auto data = v4.serialize(publish);
    // 0x30, remaining length 5, topic length 2 + "t", payload "hi"
    std::vector<uint8_t> expected = {0x30, 0x05, 0x00, 0x01, 't', 'h', 'i'};
    EXPECT_EQ(data, expected);
}

TEST_F(FrameTest, SerializePublishForMqtt5CarriesPropertyLength)
{
    Frame v5(Version::MQTT5);
    PublishPacket publish("t", std::vector<uint8_t>{'h', 'i'}, QoS::QOS_0, false);
    auto data = v5.serialize(publish);
    std::vector<uint8_t> expected = {0x30, 0x06, 0x00, 0x01, 't', 0x00, 'h', 'i'};
    EXPECT_EQ(data, expected);
}

TEST_F(FrameTest, ParsePubackWithOnlyPacketIdDoesNotThrow)
{
    std::vector<uint8_t> payload = {0x00, 0x05};
    auto packet = frame->parsePuback(payload.data(), payload.size());
    EXPECT_EQ(packet.packetId, 5);
    EXPECT_EQ(packet.reasonCode, ReasonCode::SUCCESS);
}

TEST_F(FrameTest, ParseStringThrowsOnTruncatedLength)
{
    std::vector<uint8_t> payload = {0x00};
    EXPECT_THROW(frame->parseString(payload.data(), payload.size()), std::runtime_error);
}

TEST_F(FrameTest, ParsePublishTopicAliasPropertyIsTwoByteInteger)
{
    // A PUBLISH carrying Topic Alias 7 must expose it as a uint16_t property;
    // the connection layer then expands it into a topic name.
    Frame v5;
    v5.setVersion(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x01, 'a',              // Topic name "a"
        0x04,                         // Properties length
        0x23, 0x02, 0x00, 0x07,       // TOPIC_ALIAS, len 2, value 7
        'h', 'i'                      // Payload
    };
    FixedHeader header(0x30);
    auto packet = v5.parsePublish(header, payload.data(), payload.size());
    ASSERT_EQ(packet.topicName, "a");
    auto it = packet.properties.find(PropertyID::TOPIC_ALIAS);
    ASSERT_NE(it, packet.properties.end());
    ASSERT_TRUE(std::holds_alternative<uint16_t>(it->second));
    EXPECT_EQ(std::get<uint16_t>(it->second), 7);
}

TEST_F(FrameTest, ParsePublishWithEmptyTopicNameIsAccepted)
{
    // An alias-only PUBLISH (empty topic name) is legal once the alias has been
    // mapped; the parser must not reject the empty name.
    Frame v5;
    v5.setVersion(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x00,                   // Empty topic name
        0x04,                         // Properties length
        0x23, 0x02, 0x00, 0x07,       // TOPIC_ALIAS = 7
        'h', 'i'                      // Payload
    };
    FixedHeader header(0x30);
    auto packet = v5.parsePublish(header, payload.data(), payload.size());
    EXPECT_TRUE(packet.topicName.empty());
    ASSERT_NE(packet.properties.find(PropertyID::TOPIC_ALIAS), packet.properties.end());
}

} // namespace MQTT
