#include <gtest/gtest.h>
#include "../src/Frame.h"
#include <vector>
#include <set>
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
    // a property length (0x05), then the property (id 0x11 followed by four
    // big-endian bytes; a Four Byte Integer has no length of its own, §1.5.3).
    std::vector<uint8_t> payload = {
        0x00,                   // Reason Code (Normal disconnection)
        0x05,                   // Properties length
        0x11, 0x00, 0x00, 0x0e, 0x10   // Session Expiry Interval = 3600
    };

    auto packet = frame->parseDisconnect(payload.data(), payload.size());

    EXPECT_EQ(packet.type, PacketType::DISCONNECT);
    ASSERT_EQ(packet.properties.count(PropertyID::SESSION_EXPIRY_INTERVAL), 1u);
    EXPECT_TRUE(std::holds_alternative<uint32_t>(
        packet.properties[PropertyID::SESSION_EXPIRY_INTERVAL]));
}

TEST_F(FrameTest, ParseConnectSessionExpiryProperty)
{
    // Regression guard: a Four Byte Integer property is the identifier followed
    // by exactly four big-endian bytes (§1.5.3, §2.2.2.2). There is no length
    // byte of its own, so a spec-correct CONNECT must parse.
    std::vector<uint8_t> payload = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x05,                            // Protocol version
        0b00000010,                      // Connect flags (clean start)
        0x00, 0x3c,                      // Keep alive = 60
        0x05,                            // Properties length
        0x11, 0x00, 0x00, 0x0e, 0x10,    // Session Expiry Interval = 3600
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

//! Append a Variable Byte Integer (the property-block length prefix).
static void appendVBI(std::vector<uint8_t>& out, size_t value) {
    do {
        uint8_t d = static_cast<uint8_t>(value % 128);
        value /= 128;
        if (value) d |= 0x80;
        out.push_back(d);
    } while (value);
}

//! Build a complete CONNECT payload: fixed header, one property block, client id.
static std::vector<uint8_t> connectPayload(const std::vector<uint8_t>& props,
                                           const std::string& clientId,
                                           bool will = false) {
    auto lengthBytes = std::vector<uint8_t>();
    appendVBI(lengthBytes, props.size());
    std::vector<uint8_t> p = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x05,                            // Protocol version
        static_cast<uint8_t>(0b00000010 | (will ? 0b00000100 : 0)),
        0x00, 0x3c,                      // Keep alive = 60
    };
    p.insert(p.end(), lengthBytes.begin(), lengthBytes.end());
    p.insert(p.end(), props.begin(), props.end());
    p.push_back(static_cast<uint8_t>(clientId.size() >> 8));
    p.push_back(static_cast<uint8_t>(clientId.size() & 0xFF));
    p.insert(p.end(), clientId.begin(), clientId.end());
    return p;
}

TEST_F(FrameTest, ParseConnectRejectsForeignProperty)
{
    // §2.2.2.2 (MQTT-2.2.2-1): a property that is not defined for CONNECT makes
    // the packet Malformed. These ids are all defined for some other packet.
    const std::vector<uint8_t> foreign[] = {
        {0x23, 0x02, 0x00, 0x01},                    // Topic Alias (PUBLISH)
        {0x0B, 0x07},                                // Subscription Identifier
        {0x18, 0x04, 0x00, 0x00, 0x00, 0x05},        // Will Delay Interval
        {0x01, 0x01},                                // Payload Format Indicator
        {0x03, 0x00, 0x04, 't', 'e', 'x', 't'},      // Content Type
        {0x08, 0x00, 0x03, 'a', '/', 'b'},           // Response Topic
        {0x12, 0x00, 0x02, 'x', 'x'},                // Assigned Client Identifier
        {0x13, 0x02, 0x00, 0x1E},                    // Server Keep Alive
        {0x1F, 0x00, 0x02, 'n', 'o'},                // Reason String
        {0x24, 0x02},                                // Maximum QoS
    };
    for (const auto& prop : foreign) {
        auto payload = connectPayload(prop, "cid");
        EXPECT_THROW(frame->parseConnect(payload.data(), payload.size()),
                     std::runtime_error);
    }
}

TEST_F(FrameTest, ParseConnectRejectsForeignPropertyInWill)
{
    // The Will property block has its own set (§3.1.2.12). A Subscription
    // Identifier, which only PUBLISH and SUBSCRIBE may carry, makes the packet
    // that carries the Will Malformed.
    auto payload = connectPayload({0x0B, 0x07}, "cid", true);
    // Will topic, Will payload (each with a two-byte length prefix).
    const std::vector<uint8_t> will = {0x00, 0x02, 't', 'p', 0x00, 0x03, 'd', 'a', 't'};
    payload.insert(payload.end(), will.begin(), will.end());

    EXPECT_THROW(frame->parseConnect(payload.data(), payload.size()), std::runtime_error);
}

TEST_F(FrameTest, ParseConnectAcceptsRequestResponseInformation)
{
    // Regression guard: Request Response Information (0x19) is a one-byte
    // boolean (§3.1.2.11.7) that used to be unknown to the parser, so any
    // CONNECT carrying it was dropped as "Invalid property ID".
    const std::vector<uint8_t> props = {
        0x11, 0x00, 0x00, 0x0e, 0x10,                    // Session Expiry = 3600
        0x19, 0x01,                                      // Request Response Information
        0x17, 0x01,                                      // Request Problem Information
        0x26, 0x00, 0x01, 'a', 0x00, 0x01, 'b',          // User Property a=b
    };
    auto payload = connectPayload(props, "cid");

    auto packet = frame->parseConnect(payload.data(), payload.size());

    ASSERT_EQ(packet.properties.count(PropertyID::REQUEST_RESPONSE_INFORMATION), 1u);
    EXPECT_TRUE(std::get<bool>(packet.properties[PropertyID::REQUEST_RESPONSE_INFORMATION]));
    ASSERT_EQ(packet.properties.count(PropertyID::REQUEST_PROBLEM_INFORMATION), 1u);
    EXPECT_TRUE(std::get<bool>(packet.properties[PropertyID::REQUEST_PROBLEM_INFORMATION]));
    ASSERT_EQ(packet.properties.count(PropertyID::SESSION_EXPIRY_INTERVAL), 1u);
    EXPECT_EQ(std::get<uint32_t>(packet.properties[PropertyID::SESSION_EXPIRY_INTERVAL]), 3600u);
    EXPECT_EQ(packet.properties.count(PropertyID::USER_PROPERTY), 1u);
    EXPECT_EQ(packet.clientId, "cid");
}

TEST_F(FrameTest, ParsePublishRejectsForeignProperty)
{
    // §3.3.2.3: Session Expiry Interval, Receive Maximum and Topic Alias Maximum
    // are CONNECT/CONNACK properties; a PUBLISH carrying one is Malformed.
    const std::vector<uint8_t> foreign[] = {
        {0x11, 0x04, 0x00, 0x00, 0x00, 0x3c},  // Session Expiry Interval
        {0x21, 0x02, 0x00, 0x0a},              // Receive Maximum
        {0x22, 0x02, 0x00, 0x05},              // Topic Alias Maximum
    };
    for (const auto& prop : foreign) {
        std::vector<uint8_t> payload = {'t', 'p'};
        appendVBI(payload, prop.size());
        payload.insert(payload.end(), prop.begin(), prop.end());
        payload.insert(payload.end(), {'h', 'i'});
        EXPECT_THROW(frame->parsePublish(0x30, payload.data(), payload.size()),
                     std::runtime_error);
    }
}

TEST_F(FrameTest, ParsePublishStillAcceptsItsOwnProperties)
{
    // Control: the properties PUBLISH does define keep parsing.
    std::vector<uint8_t> payload = {0x00, 0x04, 't', 'p', '/', 'a'};
    const std::vector<uint8_t> props = {
        0x23, 0x00, 0x01,                          // Topic Alias
        0x03, 0x00, 0x04, 't', 'e', 'x', 't',      // Content Type
        0x01, 0x01,                                // Payload Format Indicator
    };
    appendVBI(payload, props.size());
    payload.insert(payload.end(), props.begin(), props.end());
    payload.insert(payload.end(), {'h', 'i'});

    auto publish = frame->parsePublish(0x30, payload.data(), payload.size());
    EXPECT_EQ(publish.properties.count(PropertyID::TOPIC_ALIAS), 1u);
    EXPECT_EQ(publish.properties.count(PropertyID::CONTENT_TYPE), 1u);
}

TEST_F(FrameTest, FixedWidthIntegerPropertiesHaveNoLengthPrefix)
{
    // §1.5.2 / §1.5.3: a Two Byte Integer is exactly two bytes and a Four Byte
    // Integer exactly four, big-endian. §2.2.2.2 makes a Property an identifier
    // followed by its value, so there is no length byte in between. Any client
    // that sends the spec encoding must be understood.
    const std::vector<uint8_t> props = {
        0x11, 0x00, 0x00, 0x00, 0x3c,        // Session Expiry Interval = 60
        0x21, 0x00, 0x0a,                    // Receive Maximum = 10
        0x27, 0x00, 0x00, 0x04, 0x00,        // Maximum Packet Size = 1024
        0x22, 0x00, 0x05,                    // Topic Alias Maximum = 5
    };
    auto payload = connectPayload(props, "cid");

    auto packet = frame->parseConnect(payload.data(), payload.size());

    EXPECT_EQ(packet.clientId, "cid");
    EXPECT_EQ(std::get<uint32_t>(packet.properties[PropertyID::SESSION_EXPIRY_INTERVAL]), 60u);
    EXPECT_EQ(std::get<uint16_t>(packet.properties[PropertyID::RECEIVE_MAXIMUM]), 10);
    EXPECT_EQ(std::get<uint32_t>(packet.properties[PropertyID::MAXIMUM_PACKET_SIZE]), 1024u);
    EXPECT_EQ(std::get<uint16_t>(packet.properties[PropertyID::TOPIC_ALIAS_MAXIMUM]), 5);
}

TEST_F(FrameTest, ParsePublishMessageExpiryAndTopicAliasWithoutLengthPrefix)
{
    // §3.3.2.3.3 (Message Expiry Interval, Four Byte Integer) and §3.3.2.3.4
    // (Topic Alias, Two Byte Integer) in one spec-correct property block.
    std::vector<uint8_t> payload = {0x00, 0x01, 'a'};
    const std::vector<uint8_t> props = {
        0x02, 0x00, 0x00, 0x00, 0x1e,        // Message Expiry Interval = 30
        0x23, 0x00, 0x07,                    // Topic Alias = 7
    };
    appendVBI(payload, props.size());
    payload.insert(payload.end(), props.begin(), props.end());
    payload.insert(payload.end(), {'h', 'i'});

    auto publish = frame->parsePublish(0x30, payload.data(), payload.size());

    EXPECT_EQ(std::get<uint32_t>(publish.properties[PropertyID::MESSAGE_EXPIRY_INTERVAL]), 30u);
    EXPECT_EQ(std::get<uint16_t>(publish.properties[PropertyID::TOPIC_ALIAS]), 7);
}

TEST_F(FrameTest, SerializeThenParseFixedWidthIntegersRoundTrips)
{
    // The encoder and the decoder must agree with the spec and with each other.
    Frame v5(Version::MQTT5);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    connack.properties[PropertyID::SERVER_KEEP_ALIVE] = static_cast<uint16_t>(30);
    connack.properties[PropertyID::MAXIMUM_PACKET_SIZE] = static_cast<uint32_t>(4096);
    const auto wire = v5.serialize(connack);

    // fixed header (2) + flags/reason (2) + property length (1) +
    // 0x13 + 2 + 0x27 + 4 = 5 + 8 bytes of body.
    ASSERT_EQ(wire.size(), 2u + 2u + 1u + 8u);
    EXPECT_EQ(wire[4], 0x08);  // property length
    EXPECT_EQ(wire[5], 0x13);  // Server Keep Alive, no length byte
    EXPECT_EQ(wire[6], 0x00);
    EXPECT_EQ(wire[7], 0x1e);
    EXPECT_EQ(wire[8], 0x27);  // Maximum Packet Size, no length byte
    EXPECT_EQ(wire[9], 0x00);
    EXPECT_EQ(wire[10], 0x00);
    EXPECT_EQ(wire[11], 0x10);
    EXPECT_EQ(wire[12], 0x00);
}

TEST_F(FrameTest, ParseSubscribeRejectsForeignProperty)
{
    // §3.8.2.1.2: only Subscription Identifier and User Property are allowed
    // in SUBSCRIBE properties.
    const std::vector<uint8_t> foreign[] = {
        {0x23, 0x00, 0x01},                    // Topic Alias (PUBLISH only)
        {0x11, 0x00, 0x00, 0x00, 0x3c},        // Session Expiry Interval
    };
    for (const auto& prop : foreign) {
        std::vector<uint8_t> payload = {0x00, 0x02};  // Packet ID
        appendVBI(payload, prop.size());
        payload.insert(payload.end(), prop.begin(), prop.end());
        payload.insert(payload.end(), {'f', '/'});
        payload.push_back(0x00);                       // Options
        EXPECT_THROW(frame->parseSubscribe(payload.data(), payload.size()),
                     std::runtime_error);
    }
}

TEST_F(FrameTest, ParseBinaryDataFieldKeepsArbitraryBytes)
{
    // §1.5.6: a Binary Data field is a two-byte length followed by that many
    // opaque bytes. It is not a UTF-8 Encoded String, so MQTT-1.5.4-1's
    // well-formedness check must not apply and invalid UTF-8 must survive.
    const std::vector<uint8_t> data = {0xff, 0xfe, 0x00, 0x80, 0x41};
    std::vector<uint8_t> payload = {0x00, static_cast<uint8_t>(data.size())};
    payload.insert(payload.end(), data.begin(), data.end());

    auto parsed = frame->parseBinaryData(payload.data(), payload.size());

    EXPECT_EQ(parsed.size(), data.size());
    EXPECT_EQ(parsed, std::string(data.begin(), data.end()));
}

TEST_F(FrameTest, ParsePublishCorrelationDataWithInvalidUtf8)
{
    // §3.3.2.3.6: Correlation Data is Binary Data; a request id that is not
    // valid UTF-8 must still be routed, not rejected as a malformed packet.
    const std::vector<uint8_t> data = {0xff, 0xfe, 0x00};
    std::vector<uint8_t> props;
    props.push_back(0x09);  // Correlation Data
    props.push_back(0x00);
    props.push_back(static_cast<uint8_t>(data.size()));
    props.insert(props.end(), data.begin(), data.end());
    std::vector<uint8_t> payload = {0x00, 0x02, 't', 'p'};
    appendVBI(payload, props.size());
    payload.insert(payload.end(), props.begin(), props.end());
    payload.insert(payload.end(), {'h', 'i'});

    auto publish = frame->parsePublish(0x30, payload.data(), payload.size());

    ASSERT_EQ(publish.properties.count(PropertyID::CORRELATION_DATA), 1u);
    EXPECT_EQ(std::get<std::string>(publish.properties[PropertyID::CORRELATION_DATA]),
              std::string(data.begin(), data.end()));
}

TEST_F(FrameTest, ParseConnectWithBinaryWillPayloadAndPassword)
{
    // §3.1.3.3 and §3.1.3.5: the Will Payload and the Password are Binary Data.
    // Both must keep bytes that are not valid UTF-8.
    const std::vector<uint8_t> binary = {0xff, 0xfe, 0x00, 0x80};
    // §3.1.3.2-3: CONNECT payload order is properties, Client ID, then the Will.
    std::vector<uint8_t> payload = {
        0x00, 0x04, 'M', 'Q', 'T', 'T',  // Protocol name
        0x05,                            // Protocol version
        0b01000110,                      // Will flag + clean start + password flag
        0x00, 0x3c,                      // Keep alive = 60
        0x00,                            // (CONNECT) properties length
        0x00, 0x03, 'c', 'i', 'd',       // Client ID
        0x00,                            // Will properties length
        0x00, 0x03, 'w', 't', 'p',       // Will topic
        0x00, static_cast<uint8_t>(binary.size()),  // Will payload length
    };
    payload.insert(payload.end(), binary.begin(), binary.end());
    payload.push_back(0x00);
    payload.push_back(static_cast<uint8_t>(binary.size()));  // Password length
    payload.insert(payload.end(), binary.begin(), binary.end());

    auto packet = frame->parseConnect(payload.data(), payload.size());

    ASSERT_TRUE(packet.willMsg.has_value());
    EXPECT_EQ(*packet.willMsg, std::string(binary.begin(), binary.end()));
    ASSERT_TRUE(packet.password.has_value());
    EXPECT_EQ(*packet.password, std::string(binary.begin(), binary.end()));
}

TEST_F(FrameTest, ParseDisconnectRejectsForeignProperty)
{
    // §3.14.2.2.2: DISCONNECT properties are limited to Session Expiry Interval,
    // Reason String and Server Reference.
    std::vector<uint8_t> payload = {0x00, 0x02, 0x23, 0x02, 0x00, 0x01};
    EXPECT_THROW(frame->parseDisconnect(payload.data(), payload.size()),
                 std::runtime_error);
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

TEST_F(FrameTest, ParseConnectPacketExposesInvalidFlagCombinations)
{
    // Will Flag / Will QoS / Will Retain / reserved are surfaced so the CONNECT
    // handler can reject the protocol errors they encode (MQTT-3.1.2-3, -13,
    // -14, -15) instead of silently storing a malformed Will.
    auto parseWithFlags = [this](uint8_t flags) {
        // MQTT 3.1.1 keeps the payload simple: no property section.
        std::vector<uint8_t> payload = {
            0x00, 0x04, 'M', 'Q', 'T', 'T',
            0x04,
            flags,
            0x00, 0x3C,
            0x00, 0x01, 'c'
        };
        // A Will Flag needs the Will Topic/Message fields present.
        if (flags & 0x04) {
            const char *topic = "w";
            const char *msg = "m";
            payload.push_back(0x00); payload.push_back(0x01); payload.push_back('w');
            payload.push_back(0x00); payload.push_back(0x01); payload.push_back('m');
        }
        return frame->parseConnect(payload.data(), payload.size());
    };

    // Will Flag = 1 with Will QoS = 3: keep the raw value for the handler to see
    auto willQos3 = parseWithFlags(0x02 | 0x04 | (3 << 3));
    EXPECT_TRUE(willQos3.willFlag);
    EXPECT_EQ(static_cast<uint8_t>(willQos3.willQos), 3);

    // Will Flag = 0 with Will QoS != 0
    auto qosNoFlag = parseWithFlags(0x02 | (2 << 3));
    EXPECT_FALSE(qosNoFlag.willFlag);
    EXPECT_EQ(static_cast<uint8_t>(qosNoFlag.willQos), 2);

    // Will Flag = 0 with Will Retain = 1
    auto retainNoFlag = parseWithFlags(0x02 | 0x20);
    EXPECT_FALSE(retainNoFlag.willFlag);
    EXPECT_TRUE(retainNoFlag.willRetain);

    // Reserved bit set
    auto reserved = parseWithFlags(0x02 | 0x01);
    EXPECT_TRUE(reserved.reservedFlagSet);

    // A clean CONNECT sets none of the error flags
    auto clean = parseWithFlags(0x02);
    EXPECT_FALSE(clean.reservedFlagSet);
    EXPECT_FALSE(clean.willFlag);
    EXPECT_EQ(static_cast<uint8_t>(clean.willQos), 0);
    EXPECT_FALSE(clean.willRetain);
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

TEST_F(FrameTest, SerializeConnackForMqtt311SetsTheSessionPresentFlag)
{
    // MQTT 3.1.1 CONNACK bit 0 is the Session Present flag exactly as in MQTT 5
    // (MQTT-3.2.2-3): a resumed session must report 1, otherwise the client
    // re-subscribes over a session that was in fact restored.
    Frame v4(Version::MQTT311);
    ConnackPacket resumed(true, ReasonCode::SUCCESS);
    EXPECT_EQ(v4.serialize(resumed), (std::vector<uint8_t>{0x20, 0x02, 0x01, 0x00}));
}

TEST_F(FrameTest, SerializeConnackForMqtt31HasNoSessionPresentFlag)
{
    // MQTT 3.1 (MQIsdp) has no Session Present bit: byte 1 is reserved and must
    // stay 0, so a resumed session still emits flags 0x00.
    Frame v3(Version::MQTT31);
    ConnackPacket resumed(true, ReasonCode::SUCCESS);
    EXPECT_EQ(v3.serialize(resumed), (std::vector<uint8_t>{0x20, 0x02, 0x00, 0x00}));
}

TEST_F(FrameTest, SerializeConnackForMqtt311NeverEmitsAV5ReasonCode)
{
    // The MQTT 3.1.1 return code table only spans 0x00-0x05. A v5-only reason
    // code (here TOPIC_NAME_INVALID 0x90) must not leak onto a 3.1.1 CONNACK:
    // the client would read an undefined return code.
    Frame v4(Version::MQTT311);
    ConnackPacket badWillTopic(false, ReasonCode::TOPIC_NAME_INVALID);
    auto data = v4.serialize(badWillTopic);
    ASSERT_EQ(data.size(), 4u);
    EXPECT_LE(data[3], 0x05) << "a 3.1.1 CONNACK carried the out-of-range code 0x"
                             << std::hex << static_cast<int>(data[3]);
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
    // 0x20, remaining length 8, flags 0, reason 0, property length 5,
    // property id 0x27, then the 4-byte big-endian value 0x00100000.
    // §1.5.3: a Four Byte Integer has no length prefix of its own.
    EXPECT_EQ(v5.serialize(connack),
              (std::vector<uint8_t>{0x20, 0x08, 0x00, 0x00, 0x05, 0x27,
                                    0x00, 0x10, 0x00, 0x00}));
}

TEST_F(FrameTest, SerializeConnackAdvertisesTopicAliasMaximumAsTwoByteInteger)
{
    Frame v5(Version::MQTT5);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    connack.properties[PropertyID::TOPIC_ALIAS_MAXIMUM] = static_cast<uint16_t>(16);
    // 0x20, remaining length 6, flags 0, reason 0, property length 3,
    // property id 0x22, then the 2-byte value 0x0010. §1.5.2: a Two Byte
    // Integer is two bytes big-endian with no length prefix.
    EXPECT_EQ(v5.serialize(connack),
              (std::vector<uint8_t>{0x20, 0x06, 0x00, 0x00, 0x03, 0x22, 0x00, 0x10}));
}

TEST_F(FrameTest, SerializeConnackAdvertisesCapabilityFlags)
{
    // The broker tells the client which optional features it implements. The
    // boolean capabilities (Wildcard / Subscription Identifier / Shared
    // Subscription / Retain Available) are single bytes, and Maximum QoS is a
    // byte too (MQTT-3.1.2-4).
    Frame v5(Version::MQTT5);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    connack.properties[PropertyID::WILDCARD_SUBSCRIPTION_AVAILABLE] = true;
    connack.properties[PropertyID::SUBSCRIPTION_IDENTIFIER_AVAILABLE] = true;
    connack.properties[PropertyID::SHARED_SUBSCRIPTION_AVAILABLE] = true;
    connack.properties[PropertyID::RETAIN_AVAILABLE] = true;
    connack.properties[PropertyID::MAXIMUM_QOS] = static_cast<uint8_t>(QoS::QOS_2);
    // Each property is an id byte plus one value byte, so the property section
    // is 10 bytes long.
    auto data = v5.serialize(connack);
    // Header: 0x20, remaining length (4 fixed + 10 props = 14), flags, reason.
    ASSERT_GE(data.size(), 4u);
    EXPECT_EQ(data[0], 0x20);
    // Walk the property section and check every advertised id is present.
    uint8_t propertyLength = data[4];
    std::set<uint8_t> ids;
    size_t i = 5;
    for (uint8_t n = 0; n < propertyLength; ) {
        uint8_t id = data[i];
        ids.insert(id);
        i += 2;  // id + single value byte
        n += 2;
    }
    EXPECT_EQ(ids.count(static_cast<uint8_t>(PropertyID::WILDCARD_SUBSCRIPTION_AVAILABLE)), 1u);
    EXPECT_EQ(ids.count(static_cast<uint8_t>(PropertyID::SUBSCRIPTION_IDENTIFIER_AVAILABLE)), 1u);
    EXPECT_EQ(ids.count(static_cast<uint8_t>(PropertyID::SHARED_SUBSCRIPTION_AVAILABLE)), 1u);
    EXPECT_EQ(ids.count(static_cast<uint8_t>(PropertyID::RETAIN_AVAILABLE)), 1u);
    EXPECT_EQ(ids.count(static_cast<uint8_t>(PropertyID::MAXIMUM_QOS)), 1u);
}

TEST_F(FrameTest, SerializedCapabilityFlagsUseCompactEncoding)
{
    // CONNACK is a server-to-client packet the broker never parses back, so the
    // guarantee is the wire encoding: every capability is exactly an id byte plus
    // one value byte, with no length prefix.
    Frame v5(Version::MQTT5);
    ConnackPacket connack(false, ReasonCode::SUCCESS);
    connack.properties[PropertyID::RETAIN_AVAILABLE] = true;
    auto data = v5.serialize(connack);
    // 0x20, remaining length, flags, reason, property length 2, id 0x25, value 1.
    ASSERT_GE(data.size(), 7u);
    EXPECT_EQ(data[0], 0x20);
    EXPECT_EQ(data[4], 0x02);                 // property length
    EXPECT_EQ(data[5], 0x25);                 // RETAIN_AVAILABLE id
    EXPECT_EQ(data[6], 0x01);                 // available
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

TEST_F(FrameTest, ParsesAckVariableHeaderInSpecOrder)
{
    // MQTT-3.4.2 / 3.5.2 / 3.6.2 / 3.7.2: the variable header is the Packet
    // Identifier, then the Reason Code, and only then the Property Length.
    // Parsing the property section first turns a non-zero Reason Code into a
    // property length (0x91 would claim 145 property bytes) and rejects a legal
    // packet as malformed.
    Frame v5(Version::MQTT5);

    std::vector<uint8_t> puback = {0x00, 0x07, 0x91, 0x00};
    auto a = v5.parsePuback(puback.data(), puback.size());
    EXPECT_EQ(a.packetId, 7);
    EXPECT_EQ(a.reasonCode, ReasonCode::PACKET_IDENTIFIER_IN_USE);

    std::vector<uint8_t> pubrec = {0x00, 0x08, 0x92, 0x00};
    auto r = v5.parsePubrec(pubrec.data(), pubrec.size());
    EXPECT_EQ(r.packetId, 8);
    EXPECT_EQ(r.reasonCode, ReasonCode::PACKET_IDENTIFIER_NOT_FOUND);

    std::vector<uint8_t> pubrel = {0x00, 0x09, 0x92, 0x00};
    auto rel = v5.parsePubrel(pubrel.data(), pubrel.size());
    EXPECT_EQ(rel.packetId, 9);
    EXPECT_EQ(rel.reasonCode, ReasonCode::PACKET_IDENTIFIER_NOT_FOUND);

    std::vector<uint8_t> pubcomp = {0x00, 0x0A, 0x00, 0x00};
    auto comp = v5.parsePubcomp(pubcomp.data(), pubcomp.size());
    EXPECT_EQ(comp.packetId, 10);
    EXPECT_EQ(comp.reasonCode, ReasonCode::SUCCESS);
}

TEST_F(FrameTest, ParsesAckReasonCodeWithoutPropertyLength)
{
    // A Remaining Length of 3 carries the Reason Code but no Property Length.
    Frame v5(Version::MQTT5);
    std::vector<uint8_t> payload = {0x00, 0x07, 0x10};
    auto packet = v5.parsePuback(payload.data(), payload.size());
    EXPECT_EQ(packet.packetId, 7);
    EXPECT_EQ(packet.reasonCode, ReasonCode::NO_MATCHING_SUBSCRIBERS);
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
        0x03,                         // Properties length
        0x23, 0x00, 0x07,             // TOPIC_ALIAS, two-byte value 7
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
        0x03,                         // Properties length
        0x23, 0x00, 0x07,             // TOPIC_ALIAS = 7
        'h', 'i'                      // Payload
    };
    FixedHeader header(0x30);
    auto packet = v5.parsePublish(header, payload.data(), payload.size());
    EXPECT_TRUE(packet.topicName.empty());
    ASSERT_NE(packet.properties.find(PropertyID::TOPIC_ALIAS), packet.properties.end());
}

TEST_F(FrameTest, ParsePublishWithZeroPacketIdentifierIsAProtocolViolation)
{
    // MQTT-2.2.1-3: a PUBLISH with QoS > 0 must carry a non-zero Packet
    // Identifier. The bytes parse cleanly, so this is a Protocol Error (0x82),
    // reported through Frame::ProtocolViolation rather than as malformed input.
    Frame v5;
    v5.setVersion(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x01, 'a',              // Topic name "a"
        0x00, 0x00,                   // Packet Identifier 0
        0x00,                         // No properties
        'h', 'i'                      // Payload
    };
    FixedHeader header(0x32);         // PUBLISH, QoS 1
    EXPECT_THROW(v5.parsePublish(header, payload.data(), payload.size()),
                 Frame::ProtocolViolation);
}

TEST_F(FrameTest, ParseSubscribeWithZeroPacketIdentifierIsAProtocolViolation)
{
    // Same rule for SUBSCRIBE (MQTT-2.2.1-3): identifier 0 is never valid.
    Frame v5;
    v5.setVersion(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x00,                   // Packet Identifier 0
        0x00,                         // No properties
        0x00, 0x01, 'a',              // Filter "a"
        0x01                          // Requested QoS 1
    };
    FixedHeader header(0x82);
    EXPECT_THROW(v5.parseSubscribe(payload.data(), payload.size()),
                 Frame::ProtocolViolation);
}

TEST_F(FrameTest, ParsePublishWithEmptyTopicNameAndNoAliasLeavesTopicEmpty)
{
    // MQTT-3.3.2-7 makes this a Protocol Error, but the decision belongs to the
    // connection layer (it must answer DISCONNECT 0x82). The parser therefore
    // keeps the empty topic and must not invent an alias, so the layer above can
    // tell "empty topic, no alias" apart from "empty topic, alias present".
    Frame v5;
    v5.setVersion(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x00,                   // Empty topic name
        0x00,                         // No properties at all
        'h', 'i'                      // Payload
    };
    FixedHeader header(0x30);
    auto packet = v5.parsePublish(header, payload.data(), payload.size());
    EXPECT_TRUE(packet.topicName.empty());
    EXPECT_EQ(packet.properties.find(PropertyID::TOPIC_ALIAS), packet.properties.end());
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
    // User Property is the one property that may appear many times, and the
    // Server must forward every pair, in order, when it relays a PUBLISH
    // (MQTT-3.3.2-17 / MQTT-3.3.2-18). A repeated name is therefore two pairs,
    // not one overwritten value.
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> section = {
        0x0E,                                  // property length
        0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'v',
        0x26, 0x00, 0x01, 'k', 0x00, 0x01, 'w',
    };
    auto parsed = mqtt5.parseProperties(section.data(), section.size()).first;
    const auto &pairs = std::get<UserProperties>(parsed.at(PropertyID::USER_PROPERTY));
    ASSERT_EQ(pairs.size(), 2u) << "a repeated name must not collapse the second pair";
    auto it = pairs.begin();
    EXPECT_EQ(it->first, "k");
    EXPECT_EQ(it->second, "v");
    ++it;
    EXPECT_EQ(it->first, "k");
    EXPECT_EQ(it->second, "w");
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


TEST_F(FrameTest, ParsesEverySubscriptionIdentifierFromAPublish)
{
    // MQTT-3.3.4-4: a single copy published for several overlapping
    // subscriptions carries one Subscription Identifier per subscription, so
    // the parser has to collect all of them instead of rejecting the repeat or
    // keeping only the last one.
    Frame v5;
    v5.setVersion(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x03, 'a', '/', 'b',  // Topic name "a/b"
        0x05,                       // Properties length
        0x0B, 0x07,                 // SUBSCRIPTION_IDENTIFIER = 7
        0x0B, 0xAC, 0x02,           // SUBSCRIPTION_IDENTIFIER = 300
        'h', 'i',                   // Payload
    };
    FixedHeader header(0x30);
    auto packet = v5.parsePublish(header, payload.data(), payload.size());

    EXPECT_EQ(packet.subscriptionIdentifiers, (SubscriptionIdentifiers{7, 300}));
    EXPECT_EQ(packet.properties.count(PropertyID::SUBSCRIPTION_IDENTIFIER), 0u)
        << "the repeatable identifiers live in their own list, not in the map";
    ASSERT_EQ(packet.payload.size(), 2u);
    EXPECT_EQ(packet.payload[0], 'h');
}

TEST_F(FrameTest, SerializesEverySubscriptionIdentifierOnAPublish)
{
    // The outbound side must emit one property per identifier, including a
    // repeated identifier used by two subscriptions.
    Frame mqtt5(Version::MQTT5);
    PublishPacket packet;
    packet.type = PacketType::PUBLISH;
    packet.qos = QoS::QOS_0;
    packet.topicName = "a/b";
    packet.payload = {'h', 'i'};
    packet.subscriptionIdentifiers = {7, 300, 7};

    auto serialized = mqtt5.serializePublish(packet);

    // Fixed header (1) + remaining length (1) + topic (2 + 3) + property
    // length (1) + identifiers (2 + 3 + 2) + payload (2)
    ASSERT_EQ(serialized.size(), 2 + 5 + 1 + 7 + 2);
    EXPECT_EQ(serialized[0], 0x30);
    EXPECT_EQ(serialized[1], 15);   // remaining length
    EXPECT_EQ(serialized[7], 7);    // property length = 7 bytes
    EXPECT_EQ(serialized[8], 0x0B);
    EXPECT_EQ(serialized[9], 0x07);
    EXPECT_EQ(serialized[10], 0x0B);
    EXPECT_EQ(serialized[11], 0xAC); // 300 = 0b1_0010_1100
    EXPECT_EQ(serialized[12], 0x02);
    EXPECT_EQ(serialized[13], 0x0B);
    EXPECT_EQ(serialized[14], 0x07);
    EXPECT_EQ(serialized[15], 'h');
    EXPECT_EQ(serialized[16], 'i');
}

TEST_F(FrameTest, ASubscribeStillRejectsARepeatedSubscriptionIdentifier)
{
    // MQTT-3.8.2-16: the property may appear only once in a SUBSCRIBE, so the
    // list-based handling added for PUBLISH must not loosen that check.
    Frame mqtt5(Version::MQTT5);
    std::vector<uint8_t> payload = {
        0x00, 0x01,                 // packet id
        0x04,                       // property length
        0x0B, 0x07,                 // SUBSCRIPTION_IDENTIFIER = 7
        0x0B, 0x08,                 // ... again
        0x00, 0x01, 'a',            // filter
        0x00,                       // options: QoS 0
    };
    EXPECT_THROW(mqtt5.parseSubscribe(payload.data(), payload.size()), std::runtime_error);
}

} // namespace MQTT
