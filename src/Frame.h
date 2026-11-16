#ifndef FRAME_H
#define FRAME_H
#pragma once

#include "MQTT.h"
#include <stdexcept>

namespace MQTT {

class Frame {
    Version version = Version::MQTT5;

public:
    //! Which packet a property block belongs to. §2.2.2.2 defines every property
    //! for a fixed set of packet types; a property outside that set makes the
    //! packet Malformed (MQTT-2.2.2-1). The Will block of a CONNECT has its own
    //! set, hence WILL rather than CONNECT.
    enum class PropertyContext {
        NONE,  //!< no applicability check (used by tests and unknown callers)
        CONNECT,
        WILL,
        CONNACK,
        PUBLISH,
        PUBACK,
        PUBREC,
        PUBREL,
        PUBCOMP,
        SUBSCRIBE,
        SUBACK,
        UNSUBSCRIBE,
        UNSUBACK,
        DISCONNECT,
        AUTH,
    };

private:
    Properties doParseProperties(const uint8_t *buffer, size_t length,
                                 SubscriptionIdentifiers *identifiers = nullptr,
                                 PropertyContext context = PropertyContext::NONE);

    // Streaming parser state: a TCP segment may contain a partial packet, more
    // than one packet, or a packet split across several segments
    std::vector<uint8_t> partial;
    bool headerComplete = false;
    size_t remainingLengthBytes = 0;
    size_t remainingLength = 0;
    //! Upper bound for one packet's Remaining Length; enforced in feed()
    size_t maxPacketSize = DEFAULT_MAX_PACKET_SIZE;

public:
    //! A packet whose declared Remaining Length exceeds the configured limit.
    //! Distinct from a generic parse failure because the server must report it
    //! as PACKET_TOO_LARGE (0x95) rather than MALFORMED_PACKET (MQTT-4.13).
    class PacketTooLarge : public std::runtime_error {
    public:
        explicit PacketTooLarge(const std::string &what) : std::runtime_error(what) {}
    };
    //! A packet whose bytes parse fine but whose values violate the protocol
    //! (for example a Packet Identifier of 0, MQTT-2.2.1-3). Distinct from a
    //! generic parse failure because the server must report it as
    //! PROTOCOL_ERROR (0x82) rather than MALFORMED_PACKET (MQTT-4.13).
    class ProtocolViolation : public std::runtime_error {
    public:
        explicit ProtocolViolation(const std::string &what) : std::runtime_error(what) {}
    };
    //! A text field that is not a valid UTF-8 Encoded String (MQTT-1.5.4-1/-2).
    //! Reported as MALFORMED_PACKET (0x81), which is what the specification calls
    //! a packet containing ill-formed UTF-8 or U+0000.
    class MalformedString : public std::runtime_error {
    public:
        explicit MalformedString(const std::string &what) : std::runtime_error(what) {}
    };

    static constexpr size_t MAX_MULTIPLIER = 128 * 128 * 128;
    static constexpr size_t MAX_LENGTH = 268435455; 
    //! Default cap on a single packet's Remaining Length (1 MiB). Bounds the
    //! per-connection buffering so a client cannot declare a 256 MiB packet and
    //! stall the broker by dribbling it out one byte at a time.
    static constexpr size_t DEFAULT_MAX_PACKET_SIZE = 1024 * 1024;
    Frame() = default;
    Frame(Version version) : version(version) {}

    void setVersion(Version version) { this->version = version; }
    Version getVersion() const { return version; }
    //! Configure the largest Remaining Length this frame will buffer
    void setMaxPacketSize(size_t size) { maxPacketSize = size; }
    size_t getMaxPacketSize() const { return maxPacketSize; }

    // Parse MQTT packets
    std::shared_ptr<Packet> parse(const uint8_t *buffer, size_t length);

    //! Feed bytes from the socket; returns the number of bytes consumed
    size_t feed(const uint8_t *buffer, size_t length);
    //! True when a complete packet has been buffered and parse() can be called
    bool isComplete() const;
    //! Parse the buffered packet and clear the streaming state
    std::shared_ptr<Packet> parse();
    //! Drop any partially buffered packet
    void reset();
    //! Maximum size of the remaining length field (4 bytes, MQTT-1.5.5)
    static constexpr size_t MAX_REMAINING_LENGTH_BYTES = 4;

    // Parse the remaining length field of an MQTT packet
    std::pair<size_t, size_t> decodeRemainingLength(const uint8_t *buffer, size_t length);
    std::pair<size_t, size_t> decodeVariableByteInteger(const uint8_t *buffer, size_t length);
    std::string parseString(const uint8_t* buffer, size_t length);
    uint16_t parsePacketId(const uint8_t* buffer, size_t length);

    // Parse specific packet types
    ConnectPacket parseConnect(const uint8_t *buffer, size_t length);
    ConnackPacket parseConnack(const uint8_t *buffer, size_t length); 
    PublishPacket parsePublish(const FixedHeader &header, const uint8_t *buffer, size_t length);
    PubackPacket parsePuback(const uint8_t *buffer, size_t length);
    PubrecPacket parsePubrec(const uint8_t *buffer, size_t length);
    PubrelPacket parsePubrel(const uint8_t *buffer, size_t length);
    PubcompPacket parsePubcomp(const uint8_t *buffer, size_t length);
    SubscribePacket parseSubscribe(const uint8_t *buffer, size_t length);
    SubackPacket parseSuback(const uint8_t *buffer, size_t length);
    UnsubscribePacket parseUnsubscribe(const uint8_t *buffer, size_t length);
    UnsubackPacket parseUnsuback(const uint8_t *buffer, size_t length);
    PingreqPacket parsePingreq(const uint8_t *buffer, size_t length);
    PingrespPacket parsePingresp(const uint8_t *buffer, size_t length);
    DisconnectPacket parseDisconnect(const uint8_t *buffer, size_t length);
    AuthPacket parseAuth(const uint8_t *buffer, size_t length);

    // Parse properties
    //! Parse a property block. \p identifiers, when given, collects every
    //! Subscription Identifier instead of rejecting a repeat: a PUBLISH may
    //! carry one per subscription that caused it (MQTT-3.3.4-4). \p context
    //! selects the §2.2.2.2 applicability table to enforce.
    std::pair<Properties, size_t> parseProperties(const uint8_t *buffer, size_t length,
                                                  SubscriptionIdentifiers *identifiers = nullptr,
                                                  PropertyContext context = PropertyContext::NONE);

    // Serialize MQTT packets
    std::vector<uint8_t> serialize(const Packet &packet);
    // Serialize the remaining length field
    std::vector<uint8_t> encodeRemainingLength(size_t length);
    // Serialize specific packet types
    std::vector<uint8_t> serializeConnect(const ConnectPacket &packet);
    std::vector<uint8_t> serializeConnack(const ConnackPacket &packet);
    std::vector<uint8_t> serializePublish(const PublishPacket &packet);
    std::vector<uint8_t> serializePuback(const PubackPacket &packet);
    std::vector<uint8_t> serializePubrec(const PubrecPacket &packet);
    std::vector<uint8_t> serializePubrel(const PubrelPacket &packet);
    std::vector<uint8_t> serializePubcomp(const PubcompPacket &packet);
    std::vector<uint8_t> serializeSubscribe(const SubscribePacket &packet);
    std::vector<uint8_t> serializeSuback(const SubackPacket &packet);
    std::vector<uint8_t> serializeUnsubscribe(const UnsubscribePacket &packet);
    std::vector<uint8_t> serializeUnsuback(const UnsubackPacket &packet);
    std::vector<uint8_t> serializePingreq(const PingreqPacket &packet);
    std::vector<uint8_t> serializePingresp(const PingrespPacket &packet);
    std::vector<uint8_t> serializeDisconnect(const DisconnectPacket &packet);
    std::vector<uint8_t> serializeAuth(const AuthPacket &packet);

    // Serialize properties
    std::vector<uint8_t> serializeProperties(const Properties &properties);
};
}

#endif // FRAME_H