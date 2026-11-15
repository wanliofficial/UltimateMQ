
#include "Frame.h"
#include <set>
#include <memory>
#include <iostream>
#include "Log.h"
#include <algorithm>
#include <cstdint>

namespace MQTT {

namespace {
//! Read a 2- or 4-byte integer property in big-endian (wire) order. The earlier
//! implementation reinterpreted the bytes as a native integer, which silently
//! byte-swapped every value on little-endian hosts.
uint16_t readU16BE(const uint8_t *p) {
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}
uint32_t readU32BE(const uint8_t *p) {
    return (static_cast<uint32_t>(p[0]) << 24) | (static_cast<uint32_t>(p[1]) << 16) |
           (static_cast<uint32_t>(p[2]) << 8) | static_cast<uint32_t>(p[3]);
}
} // namespace

size_t Frame::feed(const uint8_t *buffer, size_t length) {
    size_t used = 0;
    while (used < length) {
        if (isComplete()) {
            // The caller has to parse() the buffered packet before feeding more
            break;
        }
        if (partial.empty()) {
            // Fixed header byte
            partial.push_back(buffer[used++]);
            headerComplete = false;
            remainingLength = 0;
            remainingLengthBytes = 0;
            continue;
        }
        if (headerComplete) {
            size_t bufferedBody = partial.size() - 1 - remainingLengthBytes;
            size_t needed = remainingLength - bufferedBody;
            size_t take = std::min(needed, length - used);
            partial.insert(partial.end(), buffer + used, buffer + used + take);
            used += take;
            continue;
        }
        // Remaining length bytes
        uint8_t encoded = buffer[used++];
        remainingLengthBytes++;
        partial.push_back(encoded);
        remainingLength += static_cast<size_t>(encoded & 0x7f) << (7 * (remainingLengthBytes - 1));
        if (remainingLengthBytes > MAX_REMAINING_LENGTH_BYTES) {
            throw std::runtime_error("Malformed Remaining Length");
        }
        if ((encoded & 0x80) == 0) {
            headerComplete = true;
            // The whole Remaining Length is now known. Refuse oversized packets
            // here, before buffering a single body byte, so the per-connection
            // partial buffer can never grow past the limit plus the header.
            if (remainingLength > maxPacketSize) {
                throw PacketTooLarge("packet exceeds maximum packet size");
            }
        }
    }
    return used;
}

bool Frame::isComplete() const {
    if (!headerComplete) {
        return false;
    }
    return partial.size() - 1 - remainingLengthBytes >= remainingLength;
}

void Frame::reset() {
    partial.clear();
    headerComplete = false;
    remainingLength = 0;
    remainingLengthBytes = 0;
}

std::shared_ptr<Packet> Frame::parse() {
    if (!isComplete()) {
        throw std::runtime_error("No complete packet buffered");
    }
    auto packet = parse(partial.data(), partial.size());
    reset();
    return packet;
}

std::shared_ptr<Packet> Frame::parse(const uint8_t *buffer, size_t length) {
    if (length < 2) {
        throw std::runtime_error("Insufficient data for a valid MQTT packet");
    }
    FixedHeader header(buffer[0]);
    if (!header.isValid()) {
        throw std::runtime_error("Invalid MQTT packet header");
    }
    if (header.type == PacketType::PINGREQ && length == 2 && buffer[1] == 0) {
        return std::make_unique<PingreqPacket>();
    }
    if (header.type == PacketType::PINGRESP && length == 2 && buffer[1] == 0) {
        return std::make_unique<PingrespPacket>();
    }
    size_t offset = 1;  
    auto [remainingLength, lengthBytes] = decodeRemainingLength(buffer + offset, length - offset);
    offset += lengthBytes;
    if (length < offset + remainingLength) {
        throw std::runtime_error("Incomplete packet: insufficient data");
    }
    
    switch (header.type) {
    case PacketType::CONNECT:
        return std::make_shared<ConnectPacket>(parseConnect(buffer + offset, remainingLength));
    case PacketType::PUBLISH:
        return std::make_shared<PublishPacket>(parsePublish(header, buffer + offset, remainingLength));
    case PacketType::PUBACK:
        return std::make_shared<PubackPacket>(parsePuback(buffer + offset, remainingLength));
    case PacketType::PUBREC:
        return std::make_shared<PubrecPacket>(parsePubrec(buffer + offset, remainingLength));
    case PacketType::PUBREL:
        return std::make_shared<PubrelPacket>(parsePubrel(buffer + offset, remainingLength));
    case PacketType::PUBCOMP:
        return std::make_shared<PubcompPacket>(parsePubcomp(buffer + offset, remainingLength));
    case PacketType::SUBSCRIBE:
        return std::make_shared<SubscribePacket>(parseSubscribe(buffer + offset, remainingLength));
    case PacketType::UNSUBSCRIBE:
        return std::make_shared<UnsubscribePacket>(parseUnsubscribe(buffer + offset, remainingLength));
    case PacketType::DISCONNECT:
        return std::make_shared<DisconnectPacket>(parseDisconnect(buffer + offset, remainingLength));
    case PacketType::AUTH:
        return std::make_shared<AuthPacket>(parseAuth(buffer + offset, remainingLength));
    default:
        throw std::runtime_error("Unsupported packet type");
    }
}

std::pair<size_t, size_t> Frame::decodeRemainingLength(const uint8_t *buffer, size_t length) {
    return decodeVariableByteInteger(buffer, length);
}

std::pair<size_t, size_t> Frame::decodeVariableByteInteger(const uint8_t *buffer, size_t length) {
    size_t multiplier = 1;
    size_t value = 0;
    int8_t encodedByte = 0;
    size_t offset = 0;
    do {
        if (offset >= length) {
            throw std::runtime_error("Malformed Variable Byte Integer: truncated");
        }
        encodedByte = buffer[offset++];
        value += (encodedByte & 127) * multiplier;
        if (multiplier > Frame::MAX_MULTIPLIER) {
            throw std::runtime_error("Malformed Variable Byte Integer");
        }
        multiplier *= 128;
    } while ((encodedByte & 128) != 0);
    return {value, offset};
}

std::string Frame::parseString(const uint8_t* buffer, size_t length) {
    if (length < 2) {
        throw std::runtime_error("String length field is truncated");
    }
    uint16_t strLen = (buffer[0] << 8) | buffer[1];
    if (strLen + 2 > length) {
        throw std::runtime_error("String length is greater than the remaining data");
    }
    return std::string(reinterpret_cast<const char*>(buffer + 2), strLen);
}

uint16_t Frame::parsePacketId(const uint8_t* buffer, size_t length) {
    if (length < 2) {
        throw std::runtime_error("Packet ID is not present in the data");
    }
    return buffer[0] << 8 | buffer[1];
}

Properties Frame::doParseProperties(const uint8_t *buffer, size_t length) {
    Properties properties;
    std::string contentType;
    std::string responseTopic;
    std::string correlationData;
    std::string assignedClientId;
    std::string authenticationMethod;
    std::string authenticationData;
    uint32_t subscriptionIdentifier;
    size_t subscriptionIdentifierBytes;

    // Every property except User Property may appear at most once per packet
    // ("It is a Protocol Error to include ... more than once"), so a repeat is
    // rejected instead of silently overwriting the earlier value.
    std::set<PropertyID> seen;

    size_t offset = 0;
    while (offset < length) {
        auto [id, idBytes] = decodeVariableByteInteger(buffer+offset, length-offset);
            offset += idBytes;
        PropertyID propertyId = static_cast<PropertyID>(id);
        if (propertyId != PropertyID::USER_PROPERTY && !seen.insert(propertyId).second) {
            throw std::runtime_error("Invalid properties: a property appears more than once");
        }
        // Payload Format Indicator, Request Response Information, Request
        // Problem Information, Maximum QoS, Retain Available, Wildcard
        // Subscription Available, Subscription Identifier Available and Shared
        // Subscription Available are all booleans: only 0 and 1 are legal.
        if ((propertyId == PropertyID::PAYLOAD_FORMAT_INDICATOR ||
             propertyId == PropertyID::REQUEST_RESPONSE_INFORMATION ||
             propertyId == PropertyID::REQUEST_PROBLEM_INFORMATION ||
             propertyId == PropertyID::MAXIMUM_QOS ||
             propertyId == PropertyID::RETAIN_AVAILABLE ||
             propertyId == PropertyID::WILDCARD_SUBSCRIPTION_AVAILABLE ||
             propertyId == PropertyID::SUBSCRIPTION_IDENTIFIER_AVAILABLE ||
             propertyId == PropertyID::SHARED_SUBSCRIPTION_AVAILABLE) &&
            (offset >= length || buffer[offset] > 1)) {
            throw std::runtime_error("Invalid properties: a boolean property is neither 0 nor 1");
        }
        switch (propertyId) {
            case PropertyID::PAYLOAD_FORMAT_INDICATOR:
                if (offset + 1 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");
                }   
                properties[propertyId] = static_cast<uint8_t>(buffer[offset]);
                offset += 1;
                break;
            case PropertyID::USER_PROPERTY:
                // MQTT 5 User Property: a UTF-8 key/value pair, and the property may
                // appear multiple times in one packet. Accumulate every pair into a
                // single map keyed by the property id so the accessors see them all.
                {
                    if (offset + 2 > length) {
                        throw std::runtime_error("Invalid properties: USER_PROPERTY");
                    }
                    std::string key = parseString(buffer + offset, length - offset);
                    offset += key.length() + 2;
                    if (offset + 2 > length) {
                        throw std::runtime_error("Invalid properties: USER_PROPERTY");
                    }
                    std::string value = parseString(buffer + offset, length - offset);
                    offset += value.length() + 2;
                    if (properties.find(propertyId) == properties.end()) {
                        properties[propertyId] = UserProperties();
                    }
                    std::get<UserProperties>(properties[propertyId])[key] = value;
                }
                break;
            case PropertyID::MESSAGE_EXPIRY_INTERVAL:
                // MQTT 5 four-byte integer: id, length byte (0x04), then the value
                if (offset + 1 + 4 > length || buffer[offset] != 4) {
                    throw std::runtime_error("Invalid properties: MESSAGE_EXPIRY_INTERVAL");
                }
                properties[propertyId] = readU32BE(buffer + offset + 1);
                offset += 1 + 4;
                break;
            case PropertyID::CONTENT_TYPE:
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");  
                }  
                contentType = parseString(buffer + offset, length - offset);
                properties[propertyId] = contentType;
                offset += contentType.length() + 2;
                break;
            case PropertyID::RESPONSE_TOPIC:
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");  
                }  
                responseTopic = parseString(buffer + offset, length - offset);     
                properties[propertyId] = responseTopic;
                offset += responseTopic.length() + 2;
                break;  
            case PropertyID::CORRELATION_DATA:
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");  
                }  
                correlationData = parseString(buffer + offset, length - offset);
                properties[propertyId] = correlationData;
                offset += correlationData.length() + 2;
                break;
            case PropertyID::REASON_STRING:
                // A human-readable reason string a client may attach to a DISCONNECT
                // or an acknowledgement; accept and store it. Must not be forwarded
                // to other clients.
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");
                }
                correlationData = parseString(buffer + offset, length - offset);
                properties[propertyId] = correlationData;
                offset += correlationData.length() + 2;
                break;
            case PropertyID::SERVER_REFERENCE:
                // A server reference string (server->client in AUTH/CONNACK); accept
                // it so a peer sending it is not rejected as an unknown property.
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");
                }
                correlationData = parseString(buffer + offset, length - offset);
                properties[propertyId] = correlationData;
                offset += correlationData.length() + 2;
                break;
            case PropertyID::SUBSCRIPTION_IDENTIFIER:
                // MQTT 5 Variable Byte Integer (MQTT-3.8.2.1.2): unlike the
                // fixed-width integer properties it has no length byte. A value
                // of 0 is a protocol error (MQTT-3.8.2-16).
                {
                    auto [identifier, identifierBytes] =
                        decodeVariableByteInteger(buffer + offset, length - offset);
                    if (identifier == 0) {
                        throw std::runtime_error("Invalid properties: SUBSCRIPTION_IDENTIFIER of 0");
                    }
                    properties[propertyId] = static_cast<uint32_t>(identifier);
                    offset += identifierBytes;
                }
                break;
            case PropertyID::SESSION_EXPIRY_INTERVAL:
                // MQTT 5 four-byte integer: id, length byte (0x04), then the value
                if (offset + 1 + 4 > length || buffer[offset] != 4) {
                    throw std::runtime_error("Invalid properties: SESSION_EXPIRY_INTERVAL");
                }
                properties[propertyId] = readU32BE(buffer + offset + 1);
                offset += 1 + 4;
                break;
            case PropertyID::ASSIGNED_CLIENT_IDENTIFIER:
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");  
                }
                assignedClientId = parseString(buffer + offset, length - offset);
                properties[propertyId] = assignedClientId;
                offset += assignedClientId.length() + 2;
                break;
            case PropertyID::SERVER_KEEP_ALIVE:
                // MQTT 5 two-byte integer: id, length byte (0x02), then the value
                if (offset + 1 + 2 > length || buffer[offset] != 2) {
                    throw std::runtime_error("Invalid properties: SERVER_KEEP_ALIVE");
                }
                properties[propertyId] = readU16BE(buffer + offset + 1);
                offset += 1 + 2;
                break;
            case PropertyID::AUTHENTICATION_METHOD:
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");  
                }   
                authenticationMethod = parseString(buffer + offset, length - offset);
                properties[propertyId] = authenticationMethod;
                offset += authenticationMethod.length() + 2;        
                break;
            case PropertyID::AUTHENTICATION_DATA:
                if (offset + 2 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");  
                }   
                //TODO: parse authentication data
                authenticationData = parseString(buffer + offset, length - offset);
                properties[propertyId] = authenticationData;
                offset += authenticationData.length() + 2;
                break;          
            case PropertyID::REQUEST_PROBLEM_INFORMATION:
                if (offset + 1 > length) {
                    throw std::runtime_error("Invalid REQUEST_PROBLEM_INFORMATION property: insufficient data");  
                }   
                properties[propertyId] = static_cast<bool>(buffer[offset]);
                offset += 1;
                break;
            case PropertyID::RECEIVE_MAXIMUM:
                // MQTT 5 two-byte integer: id, length byte (0x02), then the value
                if (offset + 1 + 2 > length || buffer[offset] != 2) {
                    throw std::runtime_error("Invalid properties: RECEIVE_MAXIMUM");
                }
                properties[propertyId] = readU16BE(buffer + offset + 1);
                offset += 1 + 2;
                break;
            case PropertyID::TOPIC_ALIAS_MAXIMUM:
                // MQTT 5 two-byte integer: id, length byte (0x02), then the value
                if (offset + 1 + 2 > length || buffer[offset] != 2) {
                    throw std::runtime_error("Invalid properties: TOPIC_ALIAS_MAXIMUM");
                }
                properties[propertyId] = readU16BE(buffer + offset + 1);
                offset += 1 + 2;
                break;
            case PropertyID::TOPIC_ALIAS:
                // MQTT 5 two-byte integer: id, length byte (0x02), then the value
                if (offset + 1 + 2 > length || buffer[offset] != 2) {
                    throw std::runtime_error("Invalid properties: TOPIC_ALIAS");
                }
                properties[propertyId] = readU16BE(buffer + offset + 1);
                offset += 1 + 2;
                break;
            case PropertyID::MAXIMUM_QOS:
                if (offset + 1 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");    
                }   
                properties[propertyId] = static_cast<uint8_t>(buffer[offset]);
                offset += 1;
                break;
            case PropertyID::MAXIMUM_PACKET_SIZE:
                // MQTT 5 four-byte integer: id, length byte (0x04), then the value
                if (offset + 1 + 4 > length || buffer[offset] != 4) {
                    throw std::runtime_error("Invalid properties: MAXIMUM_PACKET_SIZE");
                }
                properties[propertyId] = readU32BE(buffer + offset + 1);
                offset += 1 + 4;
                break;
            case PropertyID::WILL_DELAY_INTERVAL:
                // MQTT 5 four-byte integer: id, length byte (0x04), then the value
                if (offset + 1 + 4 > length || buffer[offset] != 4) {
                    throw std::runtime_error("Invalid properties: WILL_DELAY_INTERVAL");
                }
                properties[propertyId] = readU32BE(buffer + offset + 1);
                offset += 1 + 4;
                break;
            case PropertyID::WILDCARD_SUBSCRIPTION_AVAILABLE:
                if (offset + 1 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");    
                }      
                properties[propertyId] = static_cast<bool>(buffer[offset]);
                offset += 1;
                break;
            case PropertyID::SUBSCRIPTION_IDENTIFIER_AVAILABLE:
                if (offset + 1 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");    
                }      
                properties[propertyId] = static_cast<bool>(buffer[offset]);
                offset += 1;
                break;
            case PropertyID::SHARED_SUBSCRIPTION_AVAILABLE:
                if (offset + 1 > length) {
                    throw std::runtime_error("Invalid properties: insufficient data");    
                }      
                properties[propertyId] = static_cast<bool>(buffer[offset]);
                offset += 1;
                break;
            default:
                throw std::runtime_error("Invalid property ID");
        }
    }
    return properties;
}

std::pair<Properties, size_t> Frame::parseProperties(const uint8_t *buffer, size_t length) {
    if (length == 0) {
        throw std::runtime_error("Properties: no property length present");
    }
    if(buffer[0] == 0) {
        return std::make_pair(Properties(), 1);
    }
    auto [propertyLength, propertyBytes] = decodeVariableByteInteger(buffer, length);
    if(propertyBytes + propertyLength > length) {
        throw std::runtime_error("Not enough data for properperties");
    }     
    const uint8_t *propertyBuffer = buffer + propertyBytes;
    Properties properties = doParseProperties(propertyBuffer, propertyLength);
    return std::make_pair(properties, propertyBytes + propertyLength);
}   

// Helper functions for parsing specific packet types
ConnectPacket Frame::parseConnect(const uint8_t *buffer, size_t length) {  
    auto connect = ConnectPacket();
    size_t offset = 0;
    // Parse protocol name
    std::string protoName = parseString(buffer, length);
    offset += protoName.length() + 2;

    Version protoVersion = static_cast<Version>(buffer[offset]);
    connect.protocolName = protoName;
    connect.protocolVersion = protoVersion;
    logDebug("CONNECT protocol name=%s version=%d", protoName.c_str(), (int)protoVersion);
    offset += 1;
    if (!connect.isValidProtocol()) {
        throw std::runtime_error("Invalid protocol name or version");
    }
    
    // Parse connect flags  
    ConnectFlags flags(buffer[offset]);
    connect.cleanStart = flags.cleanStart;
    connect.willFlag = flags.willFlag;
    connect.willQos = flags.willQoS;
    connect.willRetain = flags.willRetain;
    offset += 1;

    // Parse keep alive
    connect.keepAlive = (buffer[offset] << 8) | buffer[offset + 1];
    offset += 2;    
    logDebug("CONNECT keepAlive=%u cleanStart=%d", connect.keepAlive, (int)connect.cleanStart);

    // Parse properties (MQTT 5 only - for MQTT 3.1.1 the payload starts right after the keep alive)
    if (protoVersion == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length-offset);
        connect.properties = properties;
        offset += propLength;
    }

    // Parse client ID
    std::string clientId = parseString(buffer+offset, length-offset);
    connect.clientId = clientId;
    offset += clientId.length() + 2;
    logDebug("CONNECT clientId=%s", clientId.c_str());

    // Parse will properties and message if present
    if (connect.willFlag) {
        // Parse will properties
        if(protoVersion == Version::MQTT5) {
            auto [willProperties, willPropertyLength] = parseProperties(buffer+offset, length-offset);
            connect.willProperties = willProperties;
            offset += willPropertyLength;
        }   
        // Parse will topic
        std::string willTopic = parseString(buffer+offset, length-offset);
        connect.willTopic = willTopic;
        offset += willTopic.length() + 2;
        
        // Parse will message
        std::string willMessage = parseString(buffer+offset, length-offset);
        connect.willMsg = willMessage;
        offset += willMessage.length() + 2;
    }

    // Parse username if present
    if (flags.username) {
        std::string username = parseString(buffer+offset, length-offset);
        connect.username = username;
        offset += username.length() + 2;
    }

    if(flags.password) {
        std::string password = parseString(buffer+offset, length-offset);
        connect.password = password;
        offset += password.length() + 2;
    }

    return connect;
}

PublishPacket Frame::parsePublish(const FixedHeader &header, const uint8_t *buffer, size_t length) {
    auto publish = PublishPacket();
    publish.type = PacketType::PUBLISH;
    publish.dup = header.dup;
    publish.qos = header.qos;
    publish.retain = header.retain;

    size_t offset = 0;  

    // Parse topic name
    std::string topicName = parseString(buffer, length);
    publish.topicName = topicName;
    offset += topicName.length() + 2;

    // Parse packet identifier if QoS > 0
    if (publish.qos > QoS::QOS_0) { 
        publish.packetId = parsePacketId(buffer+offset, length-offset);
        offset += 2;
    }

    if(version == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length - offset);
        publish.properties = properties;
        offset += propLength;
    }
    
    // Parse payload
    publish.payload = std::vector<uint8_t>(buffer+offset, buffer+length);
    return publish;
}

PubackPacket Frame::parsePuback(const uint8_t *buffer, size_t length) {
    auto puback = PubackPacket();
    size_t offset = 0;
    // Parse packet identifier
    puback.packetId = parsePacketId(buffer, length);
    offset += 2;

    // A packet with only a packet identifier carries neither properties nor a reason code
    if (offset >= length) {
        return puback;
    }

    // Parse properties
    if(version == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length - offset);
        puback.properties = properties;
        offset += propLength;
    }
        
    // Parse reason code (only present when there is data left after the properties)
    if (offset < length) {
        puback.reasonCode = static_cast<ReasonCode>(buffer[offset]);
    }  

    return puback;
}

PubrecPacket Frame::parsePubrec(const uint8_t *buffer, size_t length) {
    auto pubrec = PubrecPacket();
    size_t offset = 0;
    // Parse packet identifier      
    pubrec.packetId = parsePacketId(buffer, length);
    offset += 2;

    // A packet with only a packet identifier carries neither properties nor a reason code
    if (offset >= length) {
        return pubrec;
    }
    // Parse properties
    if(version == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length - offset);
        pubrec.properties = properties;
        offset += propLength;
    }
    // Parse reason code (only present when there is data left after the properties)
    if (offset < length) {
        pubrec.reasonCode = static_cast<ReasonCode>(buffer[offset]);
    }
    return pubrec;
}

PubrelPacket Frame::parsePubrel(const uint8_t *buffer, size_t length) {
    auto pubrel = PubrelPacket();
    size_t offset = 0;
    // Parse packet identifier
    pubrel.packetId = parsePacketId(buffer, length);
    offset += 2;

    // A packet with only a packet identifier carries neither properties nor a reason code
    if (offset >= length) {
        return pubrel;
    }
    // Parse properties
    if(version == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length - offset);
        pubrel.properties = properties;
        offset += propLength;
    }
    // Parse reason code (only present when there is data left after the properties)
    if (offset < length) {
        pubrel.reasonCode = static_cast<ReasonCode>(buffer[offset]);
    }   
    return pubrel;
}

PubcompPacket Frame::parsePubcomp(const uint8_t *buffer, size_t length) {
    auto pubcomp = PubcompPacket();
    size_t offset = 0;
    // Parse packet identifier  
    pubcomp.packetId = parsePacketId(buffer, length);
    offset += 2;

    // A packet with only a packet identifier carries neither properties nor a reason code
    if (offset >= length) {
        return pubcomp;
    }  
    
    // Parse properties
    if(version == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length - offset);
        pubcomp.properties = properties;
        offset += propLength;
    }

    // Parse reason code (only present when there is data left after the properties)
    if (offset < length) {
        pubcomp.reasonCode = static_cast<ReasonCode>(buffer[offset]);
    }

    return pubcomp;
}

SubscribePacket Frame::parseSubscribe(const uint8_t *buffer, size_t length) {
    auto subscribe = SubscribePacket();
    size_t offset = 0;

    // Parse packet identifier
    subscribe.packetId = parsePacketId(buffer, length);  
    offset += 2;

    // Parse properties
    if(version == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length - offset);
        subscribe.properties = properties;
        offset += propLength;
    }
    
    // Parse subscriptions
    while ((length-offset) > 0) {
        // Parse topic filter   
        std::string topicFilter = parseString(buffer+offset, length-offset);
        offset += topicFilter.length() + 2;

        uint8_t options = buffer[offset];
        SubscriptionOptions subOptions; 
        subOptions.maximumQos = static_cast<MQTT::QoS>(options & 0x03);
        subOptions.noLocal = (options & 0x04) != 0;
        subOptions.retainAsPublished = (options & 0x08) != 0;
        subOptions.retainHandling = static_cast<RetainHandling>((options >> 4) & 0x03);
        // MQTT-3.8.4-1: bits 6 and 7 are reserved, and a SUBSCRIBE that sets
        // either must be treated as malformed rather than quietly accepted.
        if ((options & 0xC0) != 0) {
            throw std::runtime_error("Invalid SUBSCRIBE: reserved option bits are set");
        }
        // The Subscription Identifier is a property of the whole SUBSCRIBE, so
        // it applies to every entry in it (MQTT-3.8.2.1.2).
        auto identifier = subscribe.properties.find(PropertyID::SUBSCRIPTION_IDENTIFIER);
        if (identifier != subscribe.properties.end() &&
            std::holds_alternative<uint32_t>(identifier->second)) {
            subOptions.subscriptionIdentifier = std::get<uint32_t>(identifier->second);
        }

        subscribe.subscriptions.emplace_back(std::move(topicFilter), subOptions);
        offset += 1;
    }

    return subscribe;
}

UnsubscribePacket Frame::parseUnsubscribe(const uint8_t *buffer, size_t length) {
    auto unsubscribe = UnsubscribePacket();
    size_t offset = 0;  

    // Parse packet identifier
    unsubscribe.packetId = parsePacketId(buffer, length);
    offset += 2;
    
    // Parse properties
    if(version == Version::MQTT5) {
        auto [properties, propLength] = parseProperties(buffer+offset, length - offset);
        unsubscribe.properties = properties;
        offset += propLength;
    }

    // Parse topic filters
    while ((length-offset) > 0) {
        std::string topicFilter = parseString(buffer+offset, length-offset);
        unsubscribe.topicFilters.push_back(topicFilter);
        offset += topicFilter.length() + 2;
    }

    return unsubscribe;
}

PingreqPacket Frame::parsePingreq(const uint8_t *buffer, size_t length) {
    return PingreqPacket();
}

PingrespPacket Frame::parsePingresp(const uint8_t *buffer, size_t length) {
    return PingrespPacket();
}

DisconnectPacket Frame::parseDisconnect(const uint8_t *buffer, size_t length) {
    auto disconnect = DisconnectPacket();
    if (length == 0) {
        // DISCONNECT with an empty body means reason code 0 (normal) and no properties
        return disconnect;
    }
    // Reason code is the first byte (present whenever there is a body)
    disconnect.reasonCode = static_cast<ReasonCode>(buffer[0]);
    size_t offset = 1;
    // Remaining bytes are the property length followed by the properties
    if (length - offset > 0) {
        auto [properties, propLength] = parseProperties(buffer + offset, length - offset);
        disconnect.properties = properties;
    }
    return disconnect;
}

AuthPacket Frame::parseAuth(const uint8_t *payload, size_t length) {
    auto auth = AuthPacket();

    if (length >= 1) {
        auth.reasonCode = static_cast<ReasonCode>(payload[0]); 
    }
    else {
        auth.reasonCode = ReasonCode::SUCCESS;
    }

    // Parse properties if present

    return auth;
}

std::vector<uint8_t> Frame::serialize(const Packet &packet) {
    if (packet.type == PacketType::CONNECT) {
        // The CONNECT packet determines the protocol version for the whole connection
        version = static_cast<const ConnectPacket&>(packet).protocolVersion;
    }
    switch (packet.type) {
        case PacketType::CONNECT:
            return serializeConnect(static_cast<const ConnectPacket&>(packet));
        case PacketType::CONNACK:
            return serializeConnack(static_cast<const ConnackPacket&>(packet));
        case PacketType::PUBLISH:
            return serializePublish(static_cast<const PublishPacket&>(packet));
        case PacketType::PUBACK:
            return serializePuback(static_cast<const PubackPacket&>(packet));
        case PacketType::PUBREC:
            return serializePubrec(static_cast<const PubrecPacket&>(packet));
        case PacketType::PUBREL:
            return serializePubrel(static_cast<const PubrelPacket&>(packet));
        case PacketType::PUBCOMP:
            return serializePubcomp(static_cast<const PubcompPacket&>(packet));
        case PacketType::SUBSCRIBE:
            return serializeSubscribe(static_cast<const SubscribePacket&>(packet));
        case PacketType::SUBACK:
            return serializeSuback(static_cast<const SubackPacket&>(packet));
        case PacketType::UNSUBSCRIBE:
            return serializeUnsubscribe(static_cast<const UnsubscribePacket&>(packet));
        case PacketType::UNSUBACK:
            return serializeUnsuback(static_cast<const UnsubackPacket&>(packet));
        case PacketType::PINGREQ:
            return serializePingreq(static_cast<const PingreqPacket&>(packet));
        case PacketType::PINGRESP:
            return serializePingresp(static_cast<const PingrespPacket&>(packet));
        case PacketType::DISCONNECT:
            return serializeDisconnect(static_cast<const DisconnectPacket&>(packet));
        case PacketType::AUTH:
            return serializeAuth(static_cast<const AuthPacket&>(packet));
        default:
            throw std::runtime_error("Unknown packet type");
    }
}

std::vector<uint8_t> Frame::serializeConnect(const ConnectPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::CONNECT) << 4);
    // Serialize variable header
    // Protocol Name
    buffer.push_back(0x00); // Length MSB
    buffer.push_back(0x04); // Length LSB
    buffer.push_back('M');
    buffer.push_back('Q');
    buffer.push_back('T');
    buffer.push_back('T');

    // Protocol Version
    buffer.push_back(static_cast<uint8_t>(packet.protocolVersion));

    // Connect Flags
    uint8_t connectFlags = 0;
    connectFlags |= packet.cleanStart ? 0x02 : 0;
    connectFlags |= packet.willFlag ? 0x04 : 0;
    connectFlags |= static_cast<uint8_t>(packet.willQos) << 3;
    connectFlags |= packet.willRetain ? 0x20 : 0;
    connectFlags |= packet.password.has_value() ? 0x40 : 0;
    connectFlags |= packet.username.has_value() ? 0x80 : 0;
    buffer.push_back(connectFlags);

    // Keep Alive
    buffer.push_back(packet.keepAlive >> 8);
    buffer.push_back(packet.keepAlive & 0xFF);

    // Properties (MQTT 5 only)
    if (packet.protocolVersion == Version::MQTT5) {
        auto propertiesBuffer = serializeProperties(packet.properties);
        buffer.insert(buffer.end(), propertiesBuffer.begin(), propertiesBuffer.end());
    }

    // Payload
    // Client Identifier
    buffer.push_back(packet.clientId.length() >> 8);
    buffer.push_back(packet.clientId.length() & 0xFF);
    buffer.insert(buffer.end(), packet.clientId.begin(), packet.clientId.end());

    // Will Properties (if Will Flag is set)
    if (packet.willFlag && packet.willProperties) {
        auto willPropertiesBuffer = serializeProperties(*packet.willProperties);
        buffer.insert(buffer.end(), willPropertiesBuffer.begin(), willPropertiesBuffer.end());
    }

    // Will Topic (if Will Flag is set)
    if (packet.willFlag && packet.willTopic) {
        buffer.push_back(packet.willTopic->length() >> 8);
        buffer.push_back(packet.willTopic->length() & 0xFF);
        buffer.insert(buffer.end(), packet.willTopic->begin(), packet.willTopic->end());
    }

    // Will Message (if Will Flag is set)
    if (packet.willFlag && packet.willMsg) {
        buffer.push_back(packet.willMsg->length() >> 8);
        buffer.push_back(packet.willMsg->length() & 0xFF);
        buffer.insert(buffer.end(), packet.willMsg->begin(), packet.willMsg->end());
    }

    // Username (if present)
    if (packet.username) {
        buffer.push_back(packet.username->length() >> 8);
        buffer.push_back(packet.username->length() & 0xFF);
        buffer.insert(buffer.end(), packet.username->begin(), packet.username->end());
    }

    // Password (if present)
    if (packet.password) {
        buffer.push_back(packet.password->length() >> 8);
        buffer.push_back(packet.password->length() & 0xFF);
        buffer.insert(buffer.end(), packet.password->begin(), packet.password->end());
    }

    // Calculate and insert remaining length
    auto remainingLengthBuffer = encodeRemainingLength(buffer.size() - 1);
    buffer.insert(buffer.begin() + 1, remainingLengthBuffer.begin(), remainingLengthBuffer.end());
    return buffer;
}

std::vector<uint8_t> Frame::serializeConnack(const ConnackPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::CONNACK) << 4);

    // Variable header, byte 1: Connect Acknowledge Flags
    uint8_t connAckFlags = 0;
    if (version != Version::MQTT311 && packet.sessionPresent) {
        connAckFlags |= 0x01;
    }

    // Variable header, byte 2: Connect Return Code / Reason Code
    uint8_t code = static_cast<uint8_t>(packet.reasonCode);
    if (version == Version::MQTT311 || version == Version::MQTT31) {
        // MQTT 3.x uses the older return code table; the values that matter
        // here differ from the v5 reason codes.
        switch (packet.reasonCode) {
        case ReasonCode::UNSUPPORTED_PROTOCOL_VERSION:
            code = 0x01;
            break;
        case ReasonCode::CLIENT_IDENTIFIER_NOT_VALID:
            code = 0x02;
            break;
        case ReasonCode::BAD_USERNAME_OR_PASSWORD:
            code = 0x04;
            break;
        case ReasonCode::NOT_AUTHORIZED:
            code = 0x05;
            break;
        case ReasonCode::SERVER_UNAVAILABLE:
        case ReasonCode::SERVER_BUSY:
            code = 0x03;
            break;
        default:
            code = packet.reasonCode == ReasonCode::SUCCESS ? 0x00 : static_cast<uint8_t>(packet.reasonCode);
            break;
        }
    }

    // Assemble the variable header (flags + code), plus the property section for
    // MQTT 5 when there are properties to advertise.
    std::vector<uint8_t> body;
    body.push_back(connAckFlags);
    body.push_back(code);
    if (version == Version::MQTT5) {
        auto propertiesBuffer = serializeProperties(packet.properties);
        if (!propertiesBuffer.empty()) {
            auto propertyLengthBytes = encodeRemainingLength(propertiesBuffer.size());
            body.insert(body.end(), propertyLengthBytes.begin(), propertyLengthBytes.end());
            body.insert(body.end(), propertiesBuffer.begin(), propertiesBuffer.end());
        }
    }

    // Fixed header: type byte, remaining length, then the variable header body.
    auto remainingLengthBytes = encodeRemainingLength(body.size());
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());
    buffer.insert(buffer.end(), body.begin(), body.end());
    return buffer;
}

std::vector<uint8_t> Frame::serializePublish(const PublishPacket &packet) {
    std::vector<uint8_t> buffer;
    uint8_t flags = static_cast<uint8_t>(PacketType::PUBLISH) << 4;
    flags |= (packet.dup ? 0x08 : 0);
    flags |= (static_cast<uint8_t>(packet.qos) << 1);
    flags |= (packet.retain ? 0x01 : 0);
    buffer.push_back(flags);
    // Serialize properties
    auto propertiesBuffer = serializeProperties(packet.properties);
    
    // Serialize remaining length
    uint32_t remainingLength = 2;                 // Topic Name length (2 bytes)
    remainingLength += packet.topicName.length(); // Topic Name
    if (packet.qos > QoS::QOS_0) {
        remainingLength += 2; // Packet Identifier (2 bytes)
    }
    if (version == Version::MQTT5) {
        // Property Length is a Variable Byte Integer, so account for its width
        remainingLength += static_cast<uint32_t>(
            encodeRemainingLength(static_cast<uint32_t>(propertiesBuffer.size())).size());
        remainingLength += static_cast<uint32_t>(propertiesBuffer.size());
    }
    remainingLength += packet.payload.size(); // Payload

    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());
    // Serialize Topic Name
    buffer.push_back((packet.topicName.length() >> 8) & 0xFF);
    buffer.push_back(packet.topicName.length() & 0xFF);
    buffer.insert(buffer.end(), packet.topicName.begin(), packet.topicName.end());

    // Serialize Packet Identifier (if QoS > 0)
    if (packet.qos > QoS::QOS_0) {
        buffer.push_back((packet.packetId >> 8) & 0xFF);
        buffer.push_back(packet.packetId & 0xFF);
    }
    // Serialize properties (MQTT 5 only)
    if (version == Version::MQTT5) {
        auto propertyLength =
            encodeRemainingLength(static_cast<uint32_t>(propertiesBuffer.size()));
        buffer.insert(buffer.end(), propertyLength.begin(), propertyLength.end());
        buffer.insert(buffer.end(), propertiesBuffer.begin(), propertiesBuffer.end());
    }
    // Serialize Payload
    buffer.insert(buffer.end(), packet.payload.begin(), packet.payload.end());
    return buffer;
}

std::vector<uint8_t> Frame::serializeSubscribe(const SubscribePacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back((static_cast<uint8_t>(PacketType::SUBSCRIBE) << 4) | 0x02);
    // Serialize remaining length
    uint32_t remainingLength = 2; // Packet Identifier (2 bytes)
    if (version == Version::MQTT5) {
        remainingLength += 1; // Property Length
    }
    for (const auto &subscription : packet.subscriptions) {
        remainingLength += 2 + subscription.first.length(); // Topic length (2 bytes) + topic string
        remainingLength += 1;                               // Subscription Options (1 byte)
    }

    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize properties (MQTT 5 only)
    if (version == Version::MQTT5) {
        buffer.push_back(0); // Property Length
    }

    // Serialize subscriptions
    for (const auto &subscription : packet.subscriptions) {
        // Topic length
        buffer.push_back((subscription.first.length() >> 8) & 0xFF);
        buffer.push_back(subscription.first.length() & 0xFF);

        // Topic string
        buffer.insert(buffer.end(), subscription.first.begin(), subscription.first.end());

        // Subscription Options
        uint8_t options = 0;
        options |= static_cast<uint8_t>(subscription.second.maximumQos) & 0x03;
        options |= subscription.second.noLocal ? 0x04 : 0;
        options |= subscription.second.retainAsPublished ? 0x08 : 0;
        options |= static_cast<uint8_t>(subscription.second.retainHandling) << 4;
        buffer.push_back(options);
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializeSuback(const SubackPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::SUBACK) << 4);
    // Calculate remaining length
    uint32_t remainingLength = 2;                 // Packet Identifier (2 bytes)
    if (version == Version::MQTT5) {
        remainingLength += 1;                     // Property Length
    }
    remainingLength += packet.reasonCodes.size(); // One byte for each reason code

    // Serialize remaining length
    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize properties (MQTT 5 only)
    if (version == Version::MQTT5) {
        buffer.push_back(0); // Property Length
    }

    // Serialize reason codes
    for (const auto &reasonCode : packet.reasonCodes) {
        buffer.push_back(static_cast<uint8_t>(reasonCode));
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializeUnsubscribe(const UnsubscribePacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back((static_cast<uint8_t>(PacketType::UNSUBSCRIBE) << 4) | 0x02);
    // Serialize remaining length
    uint32_t remainingLength = 2; // Packet Identifier (2 bytes)
    for (const auto &topic : packet.topicFilters) {
        remainingLength += 2 + topic.length(); // Topic length (2 bytes) + topic string
    }

    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize topic filters
    for (const auto &topic : packet.topicFilters) {
        // Topic length
        buffer.push_back((topic.length() >> 8) & 0xFF);
        buffer.push_back(topic.length() & 0xFF);

        // Topic string
        buffer.insert(buffer.end(), topic.begin(), topic.end());
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializeUnsuback(const UnsubackPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::UNSUBACK) << 4);
    // Calculate remaining length
    uint32_t remainingLength = 2;                 // Packet Identifier (2 bytes)
    if (version == Version::MQTT5) {
        remainingLength += 1;                     // Property Length
    }
    remainingLength += packet.reasonCodes.size(); // One byte for each reason code

    // Serialize remaining length
    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize properties (MQTT 5 only)
    if (version == Version::MQTT5) {
        buffer.push_back(0); // Property Length
    }

    // Serialize reason codes
    for (const auto &reasonCode : packet.reasonCodes) {
        buffer.push_back(static_cast<uint8_t>(reasonCode));
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializePuback(const PubackPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::PUBACK) << 4);
    // Calculate remaining length
    // MQTT 3.1.1 acks are exactly a packet identifier: no reason code and no
    // property section (MQTT-3.4.2-1, 3.5.2, 3.6.2, 3.7.2).
    const bool isV5 = version == Version::MQTT5;
    uint32_t remainingLength = 2; // Packet Identifier (2 bytes)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            remainingLength += 1; // Reason Code (1 byte)
        }
        remainingLength += 1; // Property Length
    }

    // Serialize remaining length
    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize Reason Code if not SUCCESS (MQTT 5 only)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            buffer.push_back(static_cast<uint8_t>(packet.reasonCode));
        }
        // No properties are emitted
        buffer.push_back(0); // Property Length
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializePubrec(const PubrecPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::PUBREC) << 4);
    // Calculate remaining length
    // MQTT 3.1.1 acks are exactly a packet identifier: no reason code and no
    // property section (MQTT-3.4.2-1, 3.5.2, 3.6.2, 3.7.2).
    const bool isV5 = version == Version::MQTT5;
    uint32_t remainingLength = 2; // Packet Identifier (2 bytes)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            remainingLength += 1; // Reason Code (1 byte)
        }
        remainingLength += 1; // Property Length
    }

    // Serialize remaining length
    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize Reason Code if not SUCCESS (MQTT 5 only)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            buffer.push_back(static_cast<uint8_t>(packet.reasonCode));
        }
        // No properties are emitted
        buffer.push_back(0); // Property Length
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializePubrel(const PubrelPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back((static_cast<uint8_t>(PacketType::PUBREL) << 4) | 0x02);
    // Calculate remaining length
    // MQTT 3.1.1 acks are exactly a packet identifier: no reason code and no
    // property section (MQTT-3.4.2-1, 3.5.2, 3.6.2, 3.7.2).
    const bool isV5 = version == Version::MQTT5;
    uint32_t remainingLength = 2; // Packet Identifier (2 bytes)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            remainingLength += 1; // Reason Code (1 byte)
        }
        remainingLength += 1; // Property Length
    }

    // Serialize remaining length
    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize Reason Code if not SUCCESS (MQTT 5 only)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            buffer.push_back(static_cast<uint8_t>(packet.reasonCode));
        }
        // No properties are emitted
        buffer.push_back(0); // Property Length
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializePubcomp(const PubcompPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::PUBCOMP) << 4);
    // Calculate remaining length
    // MQTT 3.1.1 acks are exactly a packet identifier: no reason code and no
    // property section (MQTT-3.4.2-1, 3.5.2, 3.6.2, 3.7.2).
    const bool isV5 = version == Version::MQTT5;
    uint32_t remainingLength = 2; // Packet Identifier (2 bytes)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            remainingLength += 1; // Reason Code (1 byte)
        }
        remainingLength += 1; // Property Length
    }

    // Serialize remaining length
    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Packet Identifier
    buffer.push_back((packet.packetId >> 8) & 0xFF);
    buffer.push_back(packet.packetId & 0xFF);

    // Serialize Reason Code if not SUCCESS (MQTT 5 only)
    if (isV5) {
        if (packet.reasonCode != ReasonCode::SUCCESS) {
            buffer.push_back(static_cast<uint8_t>(packet.reasonCode));
        }
        // No properties are emitted
        buffer.push_back(0); // Property Length
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializePingreq(const PingreqPacket &packet) {
    return {static_cast<uint8_t>(PacketType::PINGREQ) << 4, 0x00};
}

std::vector<uint8_t> Frame::serializePingresp(const PingrespPacket &packet) {
    return {static_cast<uint8_t>(PacketType::PINGRESP) << 4, 0x00};
}

std::vector<uint8_t> Frame::serializeDisconnect(const DisconnectPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::DISCONNECT) << 4);
    // Calculate remaining length
    uint32_t remainingLength = 0;
    if (packet.reasonCode != ReasonCode::NORMAL_DISCONNECTION) {
        remainingLength += 1; // Reason Code (1 byte)
    }
    if (!packet.properties.empty()) {
        std::vector<uint8_t> propertiesBytes = serializeProperties(packet.properties);
        remainingLength += propertiesBytes.size() + 1; // Properties + Property Length
    }

    // Serialize remaining length
    std::vector<uint8_t> remainingLengthBytes = encodeRemainingLength(remainingLength);
    buffer.insert(buffer.end(), remainingLengthBytes.begin(), remainingLengthBytes.end());

    // Serialize Reason Code if not NORMAL_DISCONNECTION
    if (packet.reasonCode != ReasonCode::NORMAL_DISCONNECTION) {
        buffer.push_back(static_cast<uint8_t>(packet.reasonCode));
    }

    // Serialize Properties if not empty
    if (!packet.properties.empty()) {
        std::vector<uint8_t> propertiesBytes = serializeProperties(packet.properties);
        buffer.insert(buffer.end(), propertiesBytes.begin(), propertiesBytes.end());
    }
    return buffer;
}

std::vector<uint8_t> Frame::serializeAuth(const AuthPacket &packet) {
    std::vector<uint8_t> buffer;
    buffer.push_back(static_cast<uint8_t>(PacketType::AUTH) << 4);
    // Serialize remaining length
    std::vector<uint8_t> remainingLength = encodeRemainingLength(2 + packet.properties.size());
    buffer.insert(buffer.end(), remainingLength.begin(), remainingLength.end());

    // Serialize reason code
    buffer.push_back(static_cast<uint8_t>(packet.reasonCode));

    // Serialize properties
    std::vector<uint8_t> properties = serializeProperties(packet.properties);
    buffer.insert(buffer.end(), properties.begin(), properties.end());
    return buffer;
}

// Helper function to serialize the remaining length field
std::vector<uint8_t> Frame::encodeRemainingLength(size_t length) {
    std::vector<uint8_t> encoded;
    do {
        uint8_t encodedByte = length % 128;
        length /= 128;
        if (length > 0) {
            encodedByte |= 128;
        }
        encoded.push_back(encodedByte);
    } while (length > 0);
    return encoded;
}

std::vector<uint8_t> Frame::serializeProperties(const Properties &properties) {
    std::vector<uint8_t> buffer;
    for (const auto &[id, value] : properties) {
        std::visit([&buffer, id](auto &&arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, bool>) {
                // Boolean / byte properties carry no length prefix.
                buffer.push_back(static_cast<uint8_t>(id));
                buffer.push_back(arg ? 1 : 0);
            } else if constexpr (std::is_same_v<T, uint8_t>) {
                buffer.push_back(static_cast<uint8_t>(id));
                buffer.push_back(arg);
            } else if constexpr (std::is_same_v<T, uint16_t>) {
                // MQTT 5 two-byte integer: a length byte (0x02) then the value.
                buffer.push_back(static_cast<uint8_t>(id));
                buffer.push_back(0x02);
                buffer.push_back((arg >> 8) & 0xFF);
                buffer.push_back(arg & 0xFF);
            } else if constexpr (std::is_same_v<T, uint32_t>) {
                if (id == PropertyID::SUBSCRIPTION_IDENTIFIER) {
                    // A Variable Byte Integer: no length byte, least significant
                    // 7-bit group first with the continuation bit set (MQTT-1.5.5).
                    buffer.push_back(static_cast<uint8_t>(id));
                    size_t remaining = arg;
                    do {
                        uint8_t encoded = remaining & 0x7F;
                        remaining >>= 7;
                        if (remaining != 0) {
                            encoded |= 0x80;
                        }
                        buffer.push_back(encoded);
                    } while (remaining != 0);
                    return;
                }
                // MQTT 5 four-byte integer: a length byte (0x04) then the value.
                buffer.push_back(static_cast<uint8_t>(id));
                buffer.push_back(0x04);
                buffer.push_back((arg >> 24) & 0xFF);
                buffer.push_back((arg >> 16) & 0xFF);
                buffer.push_back((arg >> 8) & 0xFF);
                buffer.push_back(arg & 0xFF);
            } else if constexpr (std::is_same_v<T, std::string>) {
                buffer.push_back(static_cast<uint8_t>(id));
                uint16_t length = static_cast<uint16_t>(arg.length());
                buffer.push_back((length >> 8) & 0xFF);
                buffer.push_back(length & 0xFF);
                buffer.insert(buffer.end(), arg.begin(), arg.end());
            } else if constexpr (std::is_same_v<T, UserProperties>) {
                // Each User Property pair carries its own property id byte.
                for (const auto& [key, val] : arg) {
                    buffer.push_back(static_cast<uint8_t>(id));
                    uint16_t keyLength = static_cast<uint16_t>(key.length());
                    buffer.push_back((keyLength >> 8) & 0xFF);
                    buffer.push_back(keyLength & 0xFF);
                    buffer.insert(buffer.end(), key.begin(), key.end());
                    
                    uint16_t valLength = static_cast<uint16_t>(val.length());
                    buffer.push_back((valLength >> 8) & 0xFF);
                    buffer.push_back(valLength & 0xFF);
                    buffer.insert(buffer.end(), val.begin(), val.end());
                }
            }
        }, value);
    }
    return buffer;
}
}