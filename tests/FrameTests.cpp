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

TEST_F(FrameTest, ParseConnectWithUserProperty)
{
    // A User Property (0x26) is a UTF-8 key/value pair and must be parsed into
    // the properties map, not rejected as an unknown property id.
    std::vector<uint8_t> payload = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x05,                            // Protocol version
        0b00000010,                      // Connect flags (clean start)
        0x00, 0x3c,                      // Keep alive = 60
        0x10,                            // Properties length (16)
        0x26, 0x00, 0x06, 'c', 'l', 'i', 'e', 'n', 't',  // 0x26, key "client"
        0x00, 0x05, 'm', 'y', 'a', 'p', 'p',             // value "myapp"
        0x00, 0x03, 'c', 'i', 'd'        // Client ID
    };

    auto packet = frame->parseConnect(payload.data(), payload.size());

    ASSERT_EQ(packet.properties.count(PropertyID::USER_PROPERTY), 1u);
    const auto &props = std::get<UserProperties>(packet.properties[PropertyID::USER_PROPERTY]);
    EXPECT_EQ(props.at("client"), "myapp");
    EXPECT_EQ(packet.clientId, "cid");
}

TEST_F(FrameTest, ParseConnectWithMultipleUserPropertiesAccumulate)
{
    // Distinct-key User Properties accumulate into one map; a later parse must
    // not crash on the variant already holding a UserProperties value.
    std::vector<uint8_t> payload = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x05,                            // Protocol version
        0b00000010,                      // Connect flags (clean start)
        0x00, 0x3c,                      // Keep alive = 60
        0x18,                            // Properties length (24)
        0x26, 0x00, 0x04, 'r', 'e', 'g', 'i', 0x00, 0x02, 'e', 'u',   // regi -> eu
        0x26, 0x00, 0x05, 't', 'r', 'a', 'c', 'e', 0x00, 0x03, 'a', 'b', 'c', // trace -> abc
        0x00, 0x02, 'c', 'd'             // Client ID
    };

    auto packet = frame->parseConnect(payload.data(), payload.size());

    ASSERT_EQ(packet.properties.count(PropertyID::USER_PROPERTY), 1u);
    const auto &props = std::get<UserProperties>(packet.properties[PropertyID::USER_PROPERTY]);
    EXPECT_EQ(props.at("regi"), "eu");
    EXPECT_EQ(props.at("trace"), "abc");
    EXPECT_EQ(packet.clientId, "cd");
}

TEST_F(FrameTest, SerializeUserPropertiesEmitIdPerPair)
{
    // Serialization must repeat the property id byte for each User Property
    // pair, so a round-trip parses back the same set of pairs.
    Properties props;
    props[PropertyID::USER_PROPERTY] = UserProperties{{"a", "1"}, {"b", "2"}};

    auto encoded = frame->serializeProperties(props);

    // Expect: id a <len> "1" id b <len> "2"
    std::vector<uint8_t> expected = {
        0x26, 0x00, 0x01, 'a', 0x00, 0x01, '1',
        0x26, 0x00, 0x01, 'b', 0x00, 0x01, '2'
    };
    EXPECT_EQ(encoded, expected);
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

TEST_F(FrameTest, SerializePubackWithFailureCodeIsTwoBytesOnMqtt311)
{
    // MQTT 3.1.1 PUBACK is strictly packet id + 2 bytes: a non-SUCCESS reason
    // code must not leak onto the wire (MQTT-3.4.2-1).
    Frame v4(Version::MQTT311);
    PubackPacket puback{7, ReasonCode::NOT_AUTHORIZED};
    EXPECT_EQ(v4.serialize(puback),
              (std::vector<uint8_t>{0x40, 0x02, 0x00, 0x07}));
}

TEST_F(FrameTest, SerializePubcompWithFailureCodeIsTwoBytesOnMqtt311)
{
    Frame v4(Version::MQTT311);
    PubcompPacket pubcomp{9, ReasonCode::PACKET_IDENTIFIER_NOT_FOUND};
    EXPECT_EQ(v4.serialize(pubcomp),
              (std::vector<uint8_t>{0x70, 0x02, 0x00, 0x09}));
}

TEST_F(FrameTest, SerializePubackFailureCarriesReasonCodeOnMqtt5)
{
    // MQTT 5 adds the reason code plus a (here empty) property section.
    Frame v5(Version::MQTT5);
    PubackPacket puback{7, ReasonCode::PACKET_IDENTIFIER_IN_USE};
    EXPECT_EQ(v5.serialize(puback),
              (std::vector<uint8_t>{0x40, 0x04, 0x00, 0x07, 0x91, 0x00}));
}

TEST_F(FrameTest, SerializePublishEmitsItsPropertiesOnMqtt5)
{
    // Every MQTT 5 PUBLISH carries a property length; properties that are set
    // must actually reach the wire. Emitting an empty section silently dropped
    // Subscription Identifier, Message Expiry, User Properties and friends.
    Frame mqtt5(Version::MQTT5);
    PublishPacket packet;
    packet.type = PacketType::PUBLISH;
    packet.qos = QoS::QOS_0;
    packet.topicName = "a/b";
    packet.payload = {'h', 'i'};
    packet.properties[PropertyID::SUBSCRIPTION_IDENTIFIER] = static_cast<uint32_t>(7);

    auto serialized = mqtt5.serializePublish(packet);

    // Fixed header (1) + remaining length (1) + topic (2 + 3) + property
    // length (1) + property (0x0B, 0x07) + payload (2)
    ASSERT_EQ(serialized.size(), 2 + 5 + 1 + 2 + 2);
    EXPECT_EQ(serialized[0], 0x30);
    EXPECT_EQ(serialized[1], 10);   // remaining length
    EXPECT_EQ(serialized[2], 0x00); // topic length hi
    EXPECT_EQ(serialized[3], 0x03); // topic length lo
    EXPECT_EQ(serialized[7], 2);    // property length = 2 bytes
    EXPECT_EQ(serialized[8], 0x0B); // SUBSCRIPTION_IDENTIFIER
    EXPECT_EQ(serialized[9], 0x07); // value 7
    EXPECT_EQ(serialized[10], 'h');
    EXPECT_EQ(serialized[11], 'i');
}

TEST_F(FrameTest, SerializePublishWithNoPropertiesEmitsAZeroLength)
{
    // A properties-free MQTT 5 PUBLISH still needs its (zero) property length.
    Frame mqtt5(Version::MQTT5);
    PublishPacket packet;
    packet.type = PacketType::PUBLISH;
    packet.qos = QoS::QOS_0;
    packet.topicName = "a";
    packet.payload = {'x'};

    auto serialized = mqtt5.serializePublish(packet);

    ASSERT_EQ(serialized.size(), 2 + 3 + 1 + 1);
    EXPECT_EQ(serialized[1], 5);  // remaining length
    EXPECT_EQ(serialized[5], 0);  // property length
    EXPECT_EQ(serialized[6], 'x');
}

TEST_F(FrameTest, SerializePublishOmitsPropertiesOnMqtt311)
{
    // MQTT 3.1.1 has no property section at all.
    Frame mqtt311(Version::MQTT311);
    PublishPacket packet;
    packet.type = PacketType::PUBLISH;
    packet.qos = QoS::QOS_0;
    packet.topicName = "a";
    packet.payload = {'x'};
    packet.properties[PropertyID::SUBSCRIPTION_IDENTIFIER] = static_cast<uint32_t>(7);

    auto serialized = mqtt311.serializePublish(packet);

    ASSERT_EQ(serialized.size(), 2 + 3 + 1);
    EXPECT_EQ(serialized[1], 4);  // remaining length: no property section
    EXPECT_EQ(serialized[5], 'x');
}

TEST_F(FrameTest, SubscriptionIdentifierUsesAVariableByteInteger)
{
    // Larger identifiers take more than one byte, with the continuation bit set.
    Frame mqtt5(Version::MQTT5);
    PublishPacket packet;
    packet.type = PacketType::PUBLISH;
    packet.qos = QoS::QOS_0;
    packet.topicName = "a";
    packet.properties[PropertyID::SUBSCRIPTION_IDENTIFIER] = static_cast<uint32_t>(300);

    auto serialized = mqtt5.serializePublish(packet);

    // 300 = 0b1_0010_1100 -> two 7-bit groups: 0xAC (continuation), 0x02
    ASSERT_EQ(serialized.size(), 2 + 3 + 1 + 3);
    EXPECT_EQ(serialized[1], 7);     // remaining length
    EXPECT_EQ(serialized[5], 3);     // property length = 3 bytes
    EXPECT_EQ(serialized[6], 0x0B);  // SUBSCRIPTION_IDENTIFIER
    EXPECT_EQ(serialized[7], 0xAC);
    EXPECT_EQ(serialized[8], 0x02);
}

TEST_F(FrameTest, ParsesSubscriptionIdentifierFromASubscribe)
{
    // The SUBSCRIBE property is a Variable Byte Integer with no length byte.
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x01,                          // packet id
        0x02,                                // property length
        0x0B, 0x07,                          // SUBSCRIPTION_IDENTIFIER = 7
        0x00, 0x03, 'a', '/', 'b',           // filter
        0x01,                                // options: QoS 1
    };
    auto subscribe = mqtt5.parseSubscribe(payload.data(), payload.size());

    ASSERT_EQ(subscribe.subscriptions.size(), 1u);
    EXPECT_EQ(subscribe.subscriptions[0].first, "a/b");
    EXPECT_EQ(subscribe.subscriptions[0].second.maximumQos, QoS::QOS_1);
    EXPECT_EQ(subscribe.subscriptions[0].second.subscriptionIdentifier, 7u);
}

TEST_F(FrameTest, RejectsASubscriptionIdentifierOfZero)
{
    // MQTT-3.8.2-16: the value 0 is a protocol error.
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x01,
        0x02,
        0x0B, 0x00,        // SUBSCRIPTION_IDENTIFIER = 0
        0x00, 0x01, 'a',
        0x00,
    };
    EXPECT_THROW(mqtt5.parseSubscribe(payload.data(), payload.size()), std::runtime_error);
}

TEST_F(FrameTest, RejectsASubscribeWithReservedOptionBits)
{
    // MQTT-3.8.4-1: bits 6 and 7 of the subscription options are reserved and a
    // SUBSCRIBE that sets either is malformed.
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x01,                    // packet id
        0x00,                          // property length
        0x00, 0x03, 'a', '/', 'b',     // filter
        0xC0,                          // reserved bits 7 and 6 set
    };
    EXPECT_THROW(mqtt5.parseSubscribe(payload.data(), payload.size()), std::runtime_error);
}

TEST_F(FrameTest, AcceptsASubscribeWithOnlyTheDefinedOptionBits)
{
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x01,
        0x00,
        0x00, 0x03, 'a', '/', 'b',
        0x2E,                          // QoS 2, no local, retain as published, retain handling 2
    };
    auto subscribe = mqtt5.parseSubscribe(payload.data(), payload.size());
    ASSERT_EQ(subscribe.subscriptions.size(), 1u);
    const auto &options = subscribe.subscriptions[0].second;
    EXPECT_EQ(options.maximumQos, QoS::QOS_2);
    EXPECT_TRUE(options.noLocal);
    EXPECT_TRUE(options.retainAsPublished);
    EXPECT_EQ(options.retainHandling, RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES);
}

TEST_F(FrameTest, RejectsAPropertyAppearingTwice)
{
    // "It is a Protocol Error to include the Payload Format Indicator more than
    // once": the second copy must not silently replace the first.
    Frame mqtt5(Version::MQTT5);
    // parseProperties expects the property length first
    std::vector<uint8_t> section = {0x04, 0x01, 0x00, 0x01, 0x01};
    EXPECT_THROW(mqtt5.parseProperties(section.data(), section.size()),
                 std::runtime_error);
}

TEST_F(FrameTest, AllowsRepeatedUserProperties)
{
    // User Property is the one property that may appear many times.
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> section = {
        0x0E,                                  // property length
        0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v',
        0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'w',
    };
    auto parsed = mqtt5.parseProperties(section.data(), section.size()).first;
    const auto &pairs = std::get<UserProperties>(parsed.at(PropertyID::USER_PROPERTY));
    EXPECT_EQ(pairs.size(), 1u);
    EXPECT_EQ(pairs.at("k"), "w");  // a repeated key keeps the last value
}

TEST_F(FrameTest, RejectsABooleanPropertyThatIsNeitherZeroNorOne)
{
    // Payload Format Indicator, Maximum QoS, Retain Available and friends are
    // booleans: any value above 1 is a protocol error.
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> section = {0x02, 0x01, 0x02};
    EXPECT_THROW(mqtt5.parseProperties(section.data(), section.size()),
                 std::runtime_error);
}

TEST_F(FrameTest, AcceptsABooleanPropertyOfOne)
{
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> section = {0x02, 0x01, 0x01};
    auto parsed = mqtt5.parseProperties(section.data(), section.size()).first;
    EXPECT_EQ(std::get<uint8_t>(parsed.at(PropertyID::PAYLOAD_FORMAT_INDICATOR)), 1);
}

} // namespace MQTT
