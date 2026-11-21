#ifndef MQTTSN_H
#define MQTTSN_H
#pragma once

#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <string>
#include <vector>
#include <netinet/in.h>
#include <sys/time.h>

namespace MQTT {

class Broker;
class Session;

//! MQTT-SN (Sensor Networks) 1.2 gateway: the UDP-based MQTT variant for
//! constrained devices (MQTT-SN Protocol Specification 1.2). The gateway
//! translates between MQTT-SN datagrams and the broker's ordinary session
//! model, so MQTT-SN clients share subscriptions, retained messages, QoS
//! flows and durable sessions with MQTT 3.1.1/5 clients.
namespace MQTTSN {

//! Message type identifiers (§5.2.2, the MsgType byte after the length field).
enum class MsgType : uint8_t {
    ADVERTISE = 0x00,
    SEARCHGW = 0x01,
    GWINFO = 0x02,
    CONNECT = 0x04,
    CONNACK = 0x05,
    WILLTOPICREQ = 0x06,
    WILLTOPIC = 0x07,
    WILLMSGREQ = 0x08,
    WILLMSG = 0x09,
    REGISTER = 0x0A,
    REGACK = 0x0B,
    PUBLISH = 0x0C,
    PUBACK = 0x0D,
    PUBCOMP = 0x0E,
    PUBREC = 0x0F,
    PUBREL = 0x10,
    SUBSCRIBE = 0x12,
    SUBACK = 0x13,
    UNSUBSCRIBE = 0x14,
    UNSUBACK = 0x15,
    PINGREQ = 0x16,
    PINGRESP = 0x17,
    DISCONNECT = 0x18,
    WILLTOPICUPD = 0x1A,
    WILLTOPICRESP = 0x1B,
    WILLMSGUPD = 0x1C,
    WILLMSGRESP = 0x1D,
};

//! Gateway return codes (§5.3.9): accepted, rejected, unknown topic, unknown
//! feature. There is no "not authorized" code, so access denials surface as
//! Invalid Topic Id.
enum class ReturnCode : uint8_t {
    ACCEPTED = 0x00,
    REJECTED_CONGESTION = 0x01,
    INVALID_TOPIC_ID = 0x02,
    NOT_SUPPORTED = 0x03,
};

//! Topic Id type carried in the low two flag bits (§5.3.1): a gateway-issued
//! id, a deployment-wide predefined id, a two-character short topic, or the
//! full topic/filter text.
enum class TopicIdType : uint8_t {
    NORMAL = 0b00,
    PREDEFINED = 0b01,
    SHORT = 0b10,
    NAME = 0b11,
};

//! The PUBLISH/SUBSCRIBE/WILLTOPIC flag byte (§5.3.1).
struct Flags {
    bool dup = false;
    uint8_t qos = 0; // 0, 1, 2, or 3 meaning QoS -1
    bool retain = false;
    bool will = false;
    bool cleanSession = false;
    TopicIdType topicIdType = TopicIdType::NORMAL;

    static Flags decode(uint8_t byte);
    uint8_t encode() const;
};

//! Largest total length a message can declare. §5.2.1: "If the first octet of
//! the Length field is coded 0x01 then the Length field is 3-octet long ... The
//! 3-octet format allows the encoding of message lengths up to 65535 octets."
//! (The first octet is the marker 0x01, not 0xFF: a 0xFF there would make the
//! one-octet length 255, which cannot describe a longer datagram.)
inline constexpr size_t MAX_MESSAGE_LENGTH = 0xFFFF;

//! Encode one datagram: the length field (§5.2.1), the MsgType, then the body.
//! The Length field specifies "the total number of octets contained in the
//! message (including the Length field itself)": one octet for a total below
//! 256, otherwise the marker 0x01 followed by the total as a 16-bit big-endian
//! value counting all three length octets plus the MsgType and body.
//! Returns an empty vector when the message would not fit in 65535 octets,
//! which is not representable at all (MQTT-SN has no fragmentation, §5.2.1) -
//! better to refuse than to emit a datagram whose length field lies.
std::vector<uint8_t> encode(MsgType type, const std::vector<uint8_t> &body);

struct Parsed {
    MsgType type;
    Flags flags;                 //!< valid for CONNECT/PUBLISH/SUBSCRIBE/...
    uint8_t protocolId = 0;      //!< CONNECT
    uint16_t duration = 0;       //!< CONNECT / DISCONNECT
    std::string clientId;        //!< CONNECT / PINGREQ
    uint8_t gwId = 0;            //!< SEARCHGW-adjacent GWINFO/ADVERTISE
    uint8_t radius = 0;          //!< SEARCHGW
    uint16_t topicId = 0;        //!< REGISTER/PUBLISH/PUBACK/SUBACK/UNSUBSCRIBE
    uint16_t msgId = 0;          //!< REGISTER/ack flows/SUBSCRIBE/UNSUBSCRIBE
    ReturnCode returnCode = ReturnCode::ACCEPTED; //!< PUBACK / will-update responses
    std::string topic;           //!< REGISTER / SUBSCRIBE / UNSUBSCRIBE / WILLTOPIC
    std::vector<uint8_t> data;   //!< PUBLISH payload / WILLMSG
};

//! Decode a datagram. Returns false when the length framing or the message
//! type is invalid; a UDP datagram is either one whole message or garbage, so
//! there is no partial state to carry.
bool parse(const uint8_t *data, size_t length, Parsed *out);

class Gateway {
public:
    //! \p predefinedTopics maps predefined topic ids to topic names
    //! ("1=sensors/temp,2=home/#"). The gateway binds a UDP socket immediately
    //! so a port clash aborts startup.
    Gateway(Broker *broker, uint16_t port,
            const std::map<uint16_t, std::string> &predefinedTopics);
    ~Gateway();
    Gateway(const Gateway &) = delete;
    Gateway &operator=(const Gateway &) = delete;

    void start();
    void stop();
    int fd() const { return socketFd_; }
    uint16_t port() const { return port_; }

    //! Drain every datagram currently queued on the UDP socket. Called by the
    //! server loop when the descriptor is readable; never blocks.
    void handleReadable();

    //! Periodic maintenance: keep-alive timeouts, will publication for dead
    //! peers, ADVERTISE broadcasts. Called by the server sweep.
    void tick();

private:
    struct Peer;
    //! One MQTT-SN client, identified by its UDP endpoint.
    std::shared_ptr<Peer> peerFor(const sockaddr_in &addr);
    //! An existing peer without creating one (datagrams after DISCONNECT)
    std::shared_ptr<Peer> peerAt(const sockaddr_in &addr);
    static std::string keyFor(const sockaddr_in &addr);

    friend struct Peer;

    //! Send one datagram to a peer endpoint
    bool sendTo(const sockaddr_in &addr, const std::vector<uint8_t> &data);
    void sendConnack(const sockaddr_in &addr, ReturnCode code);
    void sendSuback(const sockaddr_in &addr, const Flags &flags, uint16_t topicId,
                    uint16_t msgId, ReturnCode code);
    void sendWillResp(const sockaddr_in &addr, MsgType type, ReturnCode code);

    void dispatch(const sockaddr_in &addr, const Parsed &parsed);
    void handleConnect(const sockaddr_in &addr, const Parsed &parsed);
    void handleWillTopic(const sockaddr_in &addr, const Parsed &parsed);
    void handleWillMessage(const sockaddr_in &addr, const Parsed &parsed);
    void handleWillTopicUpdate(const sockaddr_in &addr, const Parsed &parsed);
    void handleWillMessageUpdate(const sockaddr_in &addr, const Parsed &parsed);
    void handleRegister(const sockaddr_in &addr, const Parsed &parsed);
    void handlePublish(const sockaddr_in &addr, const Parsed &parsed);
    void handleSubscribe(const sockaddr_in &addr, const Parsed &parsed);
    void handleUnsubscribe(const sockaddr_in &addr, const Parsed &parsed);
    void handlePingreq(const sockaddr_in &addr, const Parsed &parsed);
    void handleDisconnect(const sockaddr_in &addr, const Parsed &parsed);
    //! Create (or resume) the session once the CONNECT/WILL handshake is done
    void finishConnect(const std::shared_ptr<Peer> &peer, bool cleanSession);
    //! Tear a peer down: publish its will when the end was abnormal, release
    //! or keep the session, drop the endpoint state.
    void releasePeer(const std::shared_ptr<Peer> &peer, bool abnormal);
    void publishWillForPeer(Peer *peer);

    Broker *broker_;
    uint16_t port_;
    int socketFd_ = -1;
    std::map<std::string, std::shared_ptr<Peer>> peers;
    std::map<uint16_t, std::string> predefinedTopics;
    bool running_ = false;
    std::chrono::steady_clock::time_point lastAdvertise{
        std::chrono::steady_clock::now()};
};

} // namespace MQTTSN

} // namespace MQTT

#endif
