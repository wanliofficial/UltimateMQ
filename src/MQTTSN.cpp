#include "MQTTSN.h"
#include "Broker.h"
#include "Session.h"
#include "Topic.h"
#include "Auth.h"
#include "Log.h"
#include <cerrno>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <arpa/inet.h>
#include <sys/socket.h>

namespace MQTT {
namespace MQTTSN {

// ---------------------------------------------------------------- flags

Flags Flags::decode(uint8_t byte) {
    Flags flags;
    flags.dup = (byte & 0x80) != 0;
    flags.qos = (byte >> 5) & 0x03;
    flags.retain = (byte & 0x10) != 0;
    flags.will = (byte & 0x08) != 0;
    flags.cleanSession = (byte & 0x04) != 0;
    flags.topicIdType = static_cast<TopicIdType>(byte & 0x03);
    return flags;
}

uint8_t Flags::encode() const {
    uint8_t byte = 0;
    if (dup) {
        byte |= 0x80;
    }
    byte |= static_cast<uint8_t>((qos & 0x03) << 5);
    if (retain) {
        byte |= 0x10;
    }
    if (will) {
        byte |= 0x08;
    }
    if (cleanSession) {
        byte |= 0x04;
    }
    byte |= static_cast<uint8_t>(topicIdType);
    return byte;
}

// ---------------------------------------------------------------- codec

std::vector<uint8_t> encode(MsgType type, const std::vector<uint8_t> &body) {
    std::vector<uint8_t> datagram;
    // §5.2.1: the Length field counts the whole message, itself included. The
    // one-octet form has to carry the value in a single octet, so anything that
    // would need 256 or more (or whose value is the 0x01 marker) uses the
    // three-octet form: 0x01, then the total as a 16-bit big-endian value.
    const size_t oneOctetTotal = body.size() + 2; // length octet + MsgType
    const size_t threeOctetTotal = body.size() + 4; // 3 length octets + MsgType
    if (oneOctetTotal < 256) {
        datagram.push_back(static_cast<uint8_t>(oneOctetTotal));
    } else {
        if (threeOctetTotal > MAX_MESSAGE_LENGTH) {
            return datagram; // not representable; sendTo() drops an empty vector
        }
        datagram.push_back(0x01);
        datagram.push_back(static_cast<uint8_t>(threeOctetTotal >> 8));
        datagram.push_back(static_cast<uint8_t>(threeOctetTotal & 0xFF));
    }
    datagram.push_back(static_cast<uint8_t>(type));
    datagram.insert(datagram.end(), body.begin(), body.end());
    return datagram;
}

namespace {

uint16_t readU16(const uint8_t *p) {
    return static_cast<uint16_t>((static_cast<uint16_t>(p[0]) << 8) | p[1]);
}

void appendU16(std::vector<uint8_t> &out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value >> 8));
    out.push_back(static_cast<uint8_t>(value & 0xFF));
}

} // namespace

bool parse(const uint8_t *data, size_t length, Parsed *out) {
    if (length < 2) {
        return false;
    }
    size_t offset = 0;
    size_t total = 0;
    // §5.2.1: a one-octet Length describes every message shorter than 256
    // octets. The three-octet form is marked by a first octet of 0x01, and the
    // two following octets are the total number of octets in the message,
    // length field included. (0xFF is not the marker: it is the one-octet value
    // 255, and a 255-octet message is a legal short datagram.)
    if (data[0] != 0x01) {
        total = data[0];
        offset = 1;
    } else {
        if (length < 3) {
            return false;
        }
        total = (static_cast<size_t>(data[1]) << 8) | data[2];
        offset = 3;
    }
    // A datagram that carries extra trailing bytes is malformed; the length
    // field must match the datagram exactly (§5.2.1).
    if (total < offset + 1 || total != length) {
        return false;
    }
    const uint8_t *body = data + offset;
    const size_t bodyLength = total - offset;
    out->type = static_cast<MsgType>(body[0]);
    out->flags = Flags{};
    out->protocolId = 0;
    out->duration = 0;
    out->clientId.clear();
    out->gwId = 0;
    out->radius = 0;
    out->topicId = 0;
    out->msgId = 0;
    out->topic.clear();
    out->data.clear();
    const uint8_t *rest = body + 1;
    size_t restLength = bodyLength - 1;

    // The flag byte appears first in every message that carries one
    switch (out->type) {
    case MsgType::CONNECT:
    case MsgType::WILLTOPIC:
    case MsgType::WILLTOPICUPD:
    case MsgType::PUBLISH:
    case MsgType::SUBSCRIBE:
    case MsgType::UNSUBSCRIBE:
        if (restLength < 1) {
            return false;
        }
        out->flags = Flags::decode(rest[0]);
        ++rest;
        --restLength;
        break;
    default:
        break;
    }

    switch (out->type) {
    case MsgType::ADVERTISE:
        if (restLength < 3) {
            return false;
        }
        out->gwId = rest[0];
        out->duration = readU16(rest + 1);
        return true;
    case MsgType::SEARCHGW:
        if (restLength < 1) {
            return false;
        }
        out->radius = rest[0];
        return true;
    case MsgType::GWINFO:
        if (restLength < 1) {
            return false;
        }
        out->gwId = rest[0];
        return true;
    case MsgType::CONNECT:
        // Flags (1) + ProtocolId (1) + Duration (2) + ClientId (§5.3.3)
        if (restLength < 4) {
            return false;
        }
        out->protocolId = rest[0];
        out->duration = readU16(rest + 1);
        out->clientId.assign(reinterpret_cast<const char *>(rest + 3), restLength - 3);
        return true;
    case MsgType::WILLTOPIC:
        out->topic.assign(reinterpret_cast<const char *>(rest), restLength);
        return true;
    case MsgType::WILLTOPICUPD:
        // §5.3.17: flags (already consumed) + the new Will Topic
        out->topic.assign(reinterpret_cast<const char *>(rest), restLength);
        return true;
    case MsgType::WILLMSGUPD:
        // §5.3.18: the new Will payload
        out->data.assign(rest, rest + restLength);
        return true;
    case MsgType::WILLMSG:
        out->data.assign(rest, rest + restLength);
        return true;
    case MsgType::REGISTER:
        if (restLength < 4) {
            return false;
        }
        out->topicId = readU16(rest);
        out->msgId = readU16(rest + 2);
        out->topic.assign(reinterpret_cast<const char *>(rest + 4), restLength - 4);
        return true;
    case MsgType::PUBLISH:
        // Flags (parsed above) + TopicId (2) + MsgId (2 unless QoS 0/-1) + Data
        if (restLength < 2) {
            return false;
        }
        out->topicId = readU16(rest);
        if (out->flags.qos > 0 && out->flags.qos < 3) {
            if (restLength < 4) {
                return false;
            }
            out->msgId = readU16(rest + 2);
            out->data.assign(rest + 4, rest + restLength);
        } else {
            out->data.assign(rest + 2, rest + restLength);
        }
        return true;
    case MsgType::PUBACK:
        if (restLength < 5) {
            return false;
        }
        out->topicId = readU16(rest);
        out->msgId = readU16(rest + 2);
        out->returnCode = static_cast<ReturnCode>(rest[4]); // §5.3.6
        return true;
    case MsgType::PUBCOMP:
    case MsgType::PUBREC:
    case MsgType::PUBREL:
        if (restLength < 2) {
            return false;
        }
        out->msgId = readU16(rest);
        return true;
    case MsgType::SUBSCRIBE:
    case MsgType::UNSUBSCRIBE:
        if (restLength < 2) {
            return false;
        }
        out->msgId = readU16(rest);
        rest += 2;
        restLength -= 2;
        // Topic Name type carries the filter text; the other types carry a
        // 16-bit topic id in place of it (§5.3.10 / §5.3.12).
        if (out->flags.topicIdType == TopicIdType::NAME) {
            out->topic.assign(reinterpret_cast<const char *>(rest), restLength);
        } else {
            if (restLength < 2) {
                return false;
            }
            out->topicId = readU16(rest);
        }
        return true;
    case MsgType::PINGREQ:
        out->clientId.assign(reinterpret_cast<const char *>(rest), restLength);
        return true;
    case MsgType::DISCONNECT:
        if (restLength == 0) {
            return true;
        }
        if (restLength < 2) {
            return false;
        }
        out->duration = readU16(rest);
        return true;
    case MsgType::CONNACK:
    case MsgType::WILLTOPICREQ:
    case MsgType::WILLMSGREQ:
    case MsgType::WILLTOPICRESP:
    case MsgType::WILLMSGRESP:
    case MsgType::REGACK:
    case MsgType::SUBACK:
    case MsgType::UNSUBACK:
    case MsgType::PINGRESP:
        // Gateway->client messages: nothing to parse inbound. CONNACK and the
        // will-update answers carry a single return code byte.
        if (restLength == 1 &&
            (out->type == MsgType::CONNACK || out->type == MsgType::WILLTOPICRESP ||
             out->type == MsgType::WILLMSGRESP)) {
            out->returnCode = static_cast<ReturnCode>(rest[0]);
            return true;
        }
        return restLength == 0;
    default:
        return false;
    }
}

// ---------------------------------------------------------------- peer

struct Gateway::Peer : public Client {
    enum class WillStage {
        NONE,          //!< no will handshake in progress
        AWAIT_TOPIC,   //!< WILLTOPICREQ sent, waiting for WILLTOPIC
        AWAIT_MESSAGE, //!< WILLMSGREQ sent, waiting for WILLMSG
    };

    Peer(Gateway *gateway, const sockaddr_in &address)
        : gateway(gateway), address(address),
          lastActivity(std::chrono::steady_clock::now()) {}

    // --- Client interface: deliveries from the Session --------------
    bool deliver(const Message &message, uint16_t packetId, QoS qos,
                 const SubscriptionIdentifiers &subscriptionIdentifiers) override {
        (void)subscriptionIdentifiers; // MQTT 5 concept, not representable in SN
        // A concrete topic may never have been mapped (e.g. matched through a
        // wildcard filter). Allocate the id lazily so the client can refer to
        // it later with REGISTER-less publishes if it learned it. If the peer's
        // id space is exhausted there is no id to address this PUBLISH with, so
        // drop it rather than emit a PUBLISH carrying id 0 (never a valid id).
        uint16_t id = 0;
        if (!topicIdFor(message.topic, id)) {
            logDebug("mqtt-sn: peer %s topic-id space full, dropping PUBLISH on %s",
                     clientId.c_str(), message.topic.c_str());
            return false;
        }
        Flags flags;
        flags.dup = message.dup;
        flags.qos = static_cast<uint8_t>(qos);
        flags.retain = message.retain;
        flags.topicIdType = TopicIdType::NORMAL;
        std::vector<uint8_t> body;
        body.push_back(flags.encode());
        appendU16(body, id);
        if (qos != QoS::QOS_0) {
            appendU16(body, packetId);
        }
        body.insert(body.end(), message.payload.begin(), message.payload.end());
        return gateway->sendTo(address, encode(MsgType::PUBLISH, body));
    }

    void sendPubrec(uint16_t packetId) override {
        std::vector<uint8_t> body;
        appendU16(body, packetId);
        gateway->sendTo(address, encode(MsgType::PUBREC, body));
    }

    void sendPubrel(uint16_t packetId) override {
        std::vector<uint8_t> body;
        appendU16(body, packetId);
        gateway->sendTo(address, encode(MsgType::PUBREL, body));
    }

    void onSuperseded(Version version) override {
        // MQTT-SN has no takeover notification: the old UDP endpoint simply
        // stops being served. The session itself is left for the new peer.
        (void)version;
        session.reset();
    }

    // --- session & topic bookkeeping --------------------------------

    //! Map (or lazily allocate) the topic id for a topic name. Returns false when
    //! the peer's 16-bit id space is exhausted (no free id), leaving \p id
    //! untouched. The scan below is bounded by the 65535 ids and never wraps into
    //! an infinite loop: once every id is taken it gives up instead of spinning,
    //! because the gateway runs on the broker's single event-loop thread and a
    //! spin there would hang the whole broker (a denial of service reachable by
    //! registering 65535 distinct topic names -- §5.3.9).
    bool topicIdFor(const std::string &topic, uint16_t &id) {
        auto it = topicNameToId.find(topic);
        if (it != topicNameToId.end()) {
            id = it->second;
            return true;
        }
        // At most 65534 ids (1..65535; 0 is never a valid id) can be allocated.
        if (topicIdToName.size() >= 65535) {
            return false;
        }
        // Find the next free id in the 1..65535 space without looping forever: a
        // bounded sweep from the cursor, wrapping once at most.
        uint16_t candidate = nextTopicId;
        for (uint32_t tries = 0; tries < 65535; ++tries) {
            if (candidate == 0) {
                candidate = 1;
            }
            if (topicIdToName.count(candidate) == 0) {
                id = candidate;
                topicIdToName[id] = topic;
                topicNameToId[topic] = id;
                nextTopicId = static_cast<uint16_t>(candidate + 1);
                return true;
            }
            ++candidate;
        }
        return false;
    }

    //! The topic name an inbound topic id refers to, or empty when unknown
    std::string topicNameFor(uint16_t id) const {
        auto it = topicIdToName.find(id);
        if (it != topicIdToName.end()) {
            return it->second;
        }
        return std::string();
    }

    Gateway *gateway;
    sockaddr_in address;
    std::string clientId;
    std::shared_ptr<Session> session;
    std::string username;
    //! The CleanSession flag of the CONNECT that created the current binding
    bool cleanSession = true;
    //! Will captured through the WILLTOPIC/WILLMSG handshake
    bool hasWill = false;
    std::string willTopic;
    std::vector<uint8_t> willPayload;
    QoS willQos = QoS::QOS_0;
    bool willRetain = false;
    WillStage willStage = WillStage::NONE;
    //! MQTT-SN CONNECT duration is the keep alive; 0 disables the timeout
    uint16_t keepAlive = 0;
    uint16_t nextTopicId = 1;
    std::map<uint16_t, std::string> topicIdToName;
    std::map<std::string, uint16_t> topicNameToId;
    std::chrono::steady_clock::time_point lastActivity;
};

// ---------------------------------------------------------------- gateway

Gateway::Gateway(Broker *broker, uint16_t port,
                 const std::map<uint16_t, std::string> &predefinedTopics)
    : broker_(broker), port_(port), predefinedTopics(predefinedTopics) {
    socketFd_ = socket(AF_INET, SOCK_DGRAM, 0);
    if (socketFd_ < 0) {
        throw std::runtime_error(std::string("failed to create MQTT-SN UDP socket: ") +
                                 std::strerror(errno));
    }
    int reuse = 1;
    setsockopt(socketFd_, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse));
    // ADVERTISE broadcasts (Gateway::tick) need the socket-level allowance
    int broadcast = 1;
    setsockopt(socketFd_, SOL_SOCKET, SO_BROADCAST, &broadcast, sizeof(broadcast));
    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = INADDR_ANY;
    address.sin_port = htons(port_);
    if (bind(socketFd_, reinterpret_cast<sockaddr *>(&address), sizeof(address)) < 0) {
        close(socketFd_);
        socketFd_ = -1;
        throw std::runtime_error(std::string("failed to bind MQTT-SN port ") +
                                 std::to_string(port_) + ": " + std::strerror(errno));
    }
}

Gateway::~Gateway() {
    stop();
}

void Gateway::start() {
    running_ = true;
    logInfo("MQTT-SN gateway listening on UDP port %u", port_);
}

void Gateway::stop() {
    running_ = false;
    if (socketFd_ >= 0) {
        close(socketFd_);
        socketFd_ = -1;
    }
}

std::shared_ptr<Gateway::Peer> Gateway::peerFor(const sockaddr_in &addr) {
    char ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    const std::string key = std::string(ip) + ":" + std::to_string(ntohs(addr.sin_port));
    auto it = peers.find(key);
    if (it != peers.end()) {
        it->second->lastActivity = std::chrono::steady_clock::now();
        return it->second;
    }
    auto peer = std::make_shared<Peer>(this, addr);
    peers[key] = peer;
    return peer;
}

bool sendToHelper(int fd, const sockaddr_in &addr, const std::vector<uint8_t> &data) {
    ssize_t sent = sendto(fd, data.data(), data.size(), 0,
                          reinterpret_cast<const sockaddr *>(&addr), sizeof(addr));
    return sent == static_cast<ssize_t>(data.size());
}

// Gateway::sendTo as a member needs access to socketFd_; implemented as a
// lambda-free member below (sendToHelper exists for the Peer side).
bool Gateway::sendTo(const sockaddr_in &addr, const std::vector<uint8_t> &data) {
    if (socketFd_ < 0 || data.empty()) {
        return false;
    }
    return sendToHelper(socketFd_, addr, data);
}

void Gateway::handleReadable() {
    uint8_t buffer[65535];
    for (;;) {
        sockaddr_in peerAddr{};
        socklen_t addrLen = sizeof(peerAddr);
        ssize_t got = recvfrom(socketFd_, buffer, sizeof(buffer), MSG_DONTWAIT,
                               reinterpret_cast<sockaddr *>(&peerAddr), &addrLen);
        if (got < 0) {
            return; // EAGAIN/EWOULDBLOCK: queue drained
        }
        if (got == 0) {
            continue;
        }
        Parsed parsed;
        if (!parse(buffer, static_cast<size_t>(got), &parsed)) {
            logDebug("mqtt-sn: malformed datagram of %u bytes dropped",
                     static_cast<unsigned>(got));
            continue;
        }
        dispatch(peerAddr, parsed);
    }
}

std::string Gateway::keyFor(const sockaddr_in &addr) {
    char ip[INET_ADDRSTRLEN] = {0};
    inet_ntop(AF_INET, &addr.sin_addr, ip, sizeof(ip));
    return std::string(ip) + ":" + std::to_string(ntohs(addr.sin_port));
}

std::shared_ptr<Gateway::Peer> Gateway::peerAt(const sockaddr_in &addr) {
    auto it = peers.find(keyFor(addr));
    if (it == peers.end()) {
        return nullptr;
    }
    it->second->lastActivity = std::chrono::steady_clock::now();
    return it->second;
}

void Gateway::sendConnack(const sockaddr_in &addr, ReturnCode code) {
    std::vector<uint8_t> body{static_cast<uint8_t>(code)};
    sendTo(addr, encode(MsgType::CONNACK, body));
}

void Gateway::sendSuback(const sockaddr_in &addr, const Flags &flags, uint16_t topicId,
                         uint16_t msgId, ReturnCode code) {
    std::vector<uint8_t> body;
    body.push_back(flags.encode());
    appendU16(body, topicId);
    appendU16(body, msgId);
    body.push_back(static_cast<uint8_t>(code));
    sendTo(addr, encode(MsgType::SUBACK, body));
}

void Gateway::sendWillResp(const sockaddr_in &addr, MsgType type, ReturnCode code) {
    std::vector<uint8_t> body{static_cast<uint8_t>(code)};
    sendTo(addr, encode(type, body));
}

void Gateway::dispatch(const sockaddr_in &addr, const Parsed &p) {
    switch (p.type) {
    case MsgType::CONNECT:
        handleConnect(addr, p);
        return;
    case MsgType::WILLTOPIC:
        handleWillTopic(addr, p);
        return;
    case MsgType::WILLTOPICUPD:
        handleWillTopicUpdate(addr, p);
        return;
    case MsgType::WILLMSGUPD:
        handleWillMessageUpdate(addr, p);
        return;
    case MsgType::WILLMSG:
        handleWillMessage(addr, p);
        return;
    case MsgType::REGISTER:
        handleRegister(addr, p);
        return;
    case MsgType::PUBLISH:
        handlePublish(addr, p);
        return;
    case MsgType::SUBSCRIBE:
        handleSubscribe(addr, p);
        return;
    case MsgType::UNSUBSCRIBE:
        handleUnsubscribe(addr, p);
        return;
    case MsgType::PINGREQ:
        handlePingreq(addr, p);
        return;
    case MsgType::DISCONNECT:
        handleDisconnect(addr, p);
        return;
    case MsgType::SEARCHGW: {
        // The gateway answers with its id; the client takes the address from
        // the datagram's source (§5.2.3 GWINFO carries no address here).
        std::vector<uint8_t> body{static_cast<uint8_t>(1)};
        sendTo(addr, encode(MsgType::GWINFO, body));
        return;
    }
    case MsgType::PUBACK: {
        auto peer = peerAt(addr);
        if (peer && peer->session) {
            // §5.3.6: only an Accepted return code completes the flow. A
            // rejected publish (e.g. congestion) is retransmitted with DUP
            // instead, keeping the identifier outstanding until the client
            // accepts it.
            if (p.returnCode == ReturnCode::ACCEPTED) {
                peer->session->puback(p.msgId);
                peer->session->flush();
            } else {
                peer->session->retransmitInflight(p.msgId);
                peer->session->flush();
            }
        }
        return;
    }
    case MsgType::PUBREC: {
        auto peer = peerAt(addr);
        if (peer && peer->session) {
            peer->session->pubrec(p.msgId);
            peer->session->flush();
            std::vector<uint8_t> body;
            appendU16(body, p.msgId);
            peer->gateway->sendTo(peer->address, encode(MsgType::PUBREL, body));
        }
        return;
    }
    case MsgType::PUBREL: {
        auto peer = peerAt(addr);
        if (peer && peer->session) {
            peer->session->pubrel(p.msgId);
            std::vector<uint8_t> body;
            appendU16(body, p.msgId);
            peer->gateway->sendTo(peer->address, encode(MsgType::PUBCOMP, body));
        }
        return;
    }
    case MsgType::PUBCOMP: {
        auto peer = peerAt(addr);
        if (peer && peer->session) {
            peer->session->pubcomp(p.msgId);
            peer->session->flush();
        }
        return;
    }
    default:
        // ADVERTISE / GWINFO / gateway-side messages and unknown types are
        // ignored (a looped-back broadcast included).
        return;
    }
}

void Gateway::handleConnect(const sockaddr_in &addr, const Parsed &p) {
    // §5.3.3: the Protocol Id is the SN protocol version, fixed at 1.0.
    if (p.protocolId != 0x01) {
        sendConnack(addr, ReturnCode::NOT_SUPPORTED);
        return;
    }
    if (p.clientId.empty()) {
        sendConnack(addr, ReturnCode::INVALID_TOPIC_ID);
        return;
    }
    // MQTT-SN 1.2 has no username/password fields; the gateway therefore
    // serves anonymous deployments only. A broker configured with mandatory
    // credentials refuses SN connects instead of silently skipping auth.
    if (broker_->getAuth() && broker_->getAuth()->authRequired()) {
        sendConnack(addr, ReturnCode::NOT_SUPPORTED);
        return;
    }
    // Bound the peer table before growing it. Every incomplete CONNECT leaves a
    // Peer behind, and the keep-alive sweep in tick() cannot reap one whose
    // keepAlive is still 0 - so without this cap a stream of CONNECTs from
    // spoofed UDP source ports would grow the map without limit. Refusing an
    // established peer is not possible here (every one of them is live and
    // renewed by traffic), so a full table is answered like any busy gateway.
    constexpr size_t maxPeers = 4096;
    if (peers.size() >= maxPeers && peers.find(keyFor(addr)) == peers.end()) {
        logInfo("mqtt-sn: peer table full (%zu), refusing CONNECT from %s", peers.size(),
                keyFor(addr).c_str());
        sendConnack(addr, ReturnCode::REJECTED_CONGESTION);
        return;
    }

    auto peer = peerFor(addr);
    const bool clean = p.flags.cleanSession;
    if (clean) {
        peer->topicIdToName.clear();
        peer->topicNameToId.clear();
        peer->nextTopicId = 1;
    }
    peer->clientId = p.clientId;
    peer->keepAlive = p.duration;
    peer->cleanSession = clean;
    peer->willStage = Peer::WillStage::NONE;
    peer->hasWill = false;
    peer->willTopic.clear();
    peer->willPayload.clear();
    peer->willRetain = false;
    peer->willQos = QoS::QOS_0;

    // §5.3.2: the Will is enrolled through a WILLTOPICREQ/WILLTOPIC and a
    // WILLMSGREQ/WILLMSG round trip before the CONNACK goes out.
    if (p.flags.will) {
        peer->willStage = Peer::WillStage::AWAIT_TOPIC;
        sendTo(addr, encode(MsgType::WILLTOPICREQ, {}));
        return;
    }
    finishConnect(peer, clean);
}

void Gateway::handleWillTopic(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || peer->willStage != Peer::WillStage::AWAIT_TOPIC) {
        return; // state machine violation: ignore rather than guess
    }
    if (!p.topic.empty()) {
        // An empty WILLTOPIC deletes a previously registered will; a non-empty
        // one carries the QoS and Retain bits in its own flag byte (§5.3.4).
        if (!Topic::isValidTopicName(p.topic)) {
            // Not a topic name: abandon the handshake, the client will retry.
            releasePeer(peer, /*abnormal=*/false);
            return;
        }
        peer->hasWill = true;
        peer->willTopic = p.topic;
        // §5.3.1: a raw QoS of 3 means "QoS -1" (fire-and-forget). The publish
        // path normalises it to QoS 0; the Will must too, or an internal QoS of
        // 3 would be carried into the retained table and persisted, where the
        // Store parser (bounded by QOS_2) rejects it and aborts startup.
        peer->willQos = static_cast<QoS>(p.flags.qos == 3 ? 0 : (p.flags.qos & 0x03));
        peer->willRetain = p.flags.retain;
        peer->willPayload.clear();
    } else {
        peer->hasWill = false;
    }
    peer->willStage = Peer::WillStage::AWAIT_MESSAGE;
    sendTo(addr, encode(MsgType::WILLMSGREQ, {}));
}

void Gateway::handleWillMessage(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || peer->willStage != Peer::WillStage::AWAIT_MESSAGE) {
        return;
    }
    peer->willPayload = p.data;
    peer->willStage = Peer::WillStage::NONE;
    finishConnect(peer, peer->cleanSession);
}

void Gateway::handleWillTopicUpdate(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || !peer->session) {
        return; // only a connected client has a will to update
    }
    // §5.3.17: an empty new Will Topic deletes the will, exactly as in the
    // WILLTOPIC handshake. A non-empty one must be a legal topic name; its own
    // flag byte carries the new Will QoS and Will Retain bits.
    if (!p.topic.empty() && !Topic::isValidTopicName(p.topic)) {
        sendWillResp(addr, MsgType::WILLTOPICRESP, ReturnCode::INVALID_TOPIC_ID);
        return;
    }
    peer->hasWill = !p.topic.empty();
    peer->willTopic = p.topic;
    // §5.3.1 / §5.3.17: a raw QoS of 3 means "QoS -1" (fire-and-forget); the
    // publish path normalises it to QoS 0, so the updated Will must too.
    peer->willQos = static_cast<QoS>(p.flags.qos == 3 ? 0 : (p.flags.qos & 0x03));
    peer->willRetain = p.flags.retain;
    peer->willPayload.clear();
    sendWillResp(addr, MsgType::WILLTOPICRESP, ReturnCode::ACCEPTED);
}

void Gateway::handleWillMessageUpdate(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || !peer->session) {
        return;
    }
    // §5.3.18: the new will payload; the topic was fixed (or deleted) by the
    // preceding WILLTOPICUPD.
    peer->willPayload = p.data;
    sendWillResp(addr, MsgType::WILLMSGRESP, ReturnCode::ACCEPTED);
}

void Gateway::finishConnect(const std::shared_ptr<Peer> &peer, bool clean) {
    auto existing = broker_->findSession(peer->clientId);
    // A retransmitted CONNECT (lost CONNACK) or a returning client on the
    // same UDP endpoint must not create a second binding for the session the
    // peer already holds - but it still has to receive the deliveries it may
    // have missed while away, so the outstanding messages are re-pushed.
    if (existing && peer->session == existing) {
        sendConnack(peer->address, ReturnCode::ACCEPTED);
        existing->resume(peer, Version::MQTT311);
        peer->session = existing;
        existing->flush();
        return;
    }
    if (existing) {
        if (clean) {
            existing->discard();
        }
        existing.reset();
    }
    const bool resumed = broker_->findSession(peer->clientId) != nullptr && !clean;
    auto sessionToUse =
        resumed ? broker_->findSession(peer->clientId)
                : std::make_shared<Session>(broker_, peer->clientId, clean);
    sessionToUse->setUsername(peer->username);
    sessionToUse->setPersistent(!clean);
    peer->session = sessionToUse;
    sendConnack(peer->address, ReturnCode::ACCEPTED);
    if (resumed) {
        // Session::resume() rebinds the client and retransmits the
        // unacknowledged QoS>0 messages (asleep PINGREQ path included). The
        // resume first detaches the previous binding - which for a
        // re-PINGREQ awake call is this same peer - so restore it after.
        sessionToUse->resume(peer, Version::MQTT311);
        peer->session = sessionToUse;
        sessionToUse->flush();
    } else {
        sessionToUse->bindClient(peer);
        sessionToUse->connect(sessionToUse, /*persistent=*/!clean);
    }
}

void Gateway::handleRegister(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer) {
        return;
    }
    // §5.3.9 REGACK echoes the msg id with the id the gateway assigned.
    const auto ack = [&](uint16_t topicId, ReturnCode code) {
        std::vector<uint8_t> body;
        appendU16(body, topicId);
        appendU16(body, p.msgId);
        body.push_back(static_cast<uint8_t>(code));
        sendTo(addr, encode(MsgType::REGACK, body));
    };
    if (!peer->session || !Topic::isValidTopicName(p.topic)) {
        ack(0, ReturnCode::INVALID_TOPIC_ID);
        return;
    }
    uint16_t topicId = 0;
    if (!peer->topicIdFor(p.topic, topicId)) {
        // The peer's id space is full: reject with congestion rather than hang.
        ack(0, ReturnCode::REJECTED_CONGESTION);
        return;
    }
    ack(topicId, ReturnCode::ACCEPTED);
}

void Gateway::handlePublish(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || !peer->session) {
        return; // PUBLISH before CONNECT: nothing to route through
    }
    // Resolve the topic name per the Topic Id type (§5.3.1).
    std::string topic;
    bool known = false;
    switch (p.flags.topicIdType) {
    case TopicIdType::NAME:
        topic = p.topic;
        known = Topic::isValidTopicName(topic);
        break;
    case TopicIdType::NORMAL:
        topic = peer->topicNameFor(p.topicId);
        known = !topic.empty();
        break;
    case TopicIdType::PREDEFINED: {
        auto it = predefinedTopics.find(p.topicId);
        if (it != predefinedTopics.end()) {
            topic = it->second;
            known = true;
        }
        break;
    }
    case TopicIdType::SHORT:
        // A short topic name is exactly the two bytes of the topic id
        // (§5.3.1): no mapping needed, the bytes are the name.
        topic.assign(2, '\0');
        topic[0] = static_cast<char>(p.topicId >> 8);
        topic[1] = static_cast<char>(p.topicId & 0xFF);
        // The bytes are an MQTT Topic Name and must obey the same "no wildcard
        // characters" rule as a full topic-name publish (MQTT-3.3.2-1); the
        // SUBSCRIBE path already validates its SHORT filter, so PUBLISH must too,
        // or a device could route an illegal name like "++" or "#x".
        known = Topic::isValidTopicName(topic);
        break;
    }
    // MQTT-SN §6.6: "Regardless of the requested QoS level the client may
    // receive in response to its PUBLISH a PUBACK message which contains either
    // ReturnCode = 'Rejection: invalid topic Id' ... or 'Rejection: congestion'."
    // Only PUBACK has a ReturnCode field; PUBREC/PUBREL/PUBCOMP carry none, so a
    // rejection at ANY QoS must be a PUBACK. An earlier shape answered a QoS 2
    // rejection with a bare PUBREC - byte-for-byte identical to the PUBREC sent
    // on a *successful* QoS 2 publish - so the client could not tell rejection
    // from acceptance and would complete PUBREL/PUBCOMP believing an
    // exactly-once delivery that never happened. Answer every rejection with a
    // PUBACK carrying the code (topic id, msg id, return code), as the QoS 1
    // branch already does.
    const auto reject = [&](ReturnCode code) {
        std::vector<uint8_t> body;
        appendU16(body, p.topicId);
        appendU16(body, p.msgId);
        body.push_back(static_cast<uint8_t>(code));
        sendTo(addr, encode(MsgType::PUBACK, body));
    };
    if (!known) {
        reject(ReturnCode::INVALID_TOPIC_ID);
        return;
    }
    // QoS -1 (0b11) is a fire-and-forget publish for simple devices (§5.3.3);
    // route it like QoS 0.
    const uint8_t qos = p.flags.qos == 3 ? 0 : p.flags.qos;
    Message message{topic, p.data, static_cast<QoS>(qos), p.flags.retain};
    // Access control: an unauthorized topic must neither be routed nor
    // revealed as existing, so it answers with an invalid topic id.
    if (broker_->getAuth() && broker_->getAuth()->isForbidden(peer->username, topic,
                                                              /*write=*/true)) {
        reject(ReturnCode::INVALID_TOPIC_ID);
        return;
    }
    const ReasonCode reason = peer->session->publish(p.msgId, message, p.flags.dup);
    if (qos == 1) {
        std::vector<uint8_t> body;
        // §5.3.6: the PUBACK carries the topic id the gateway knows the topic
        // by, which for a topic-name publish is the one it assigns now. If the
        // peer's id space is exhausted the message was still routed; ack with id
        // 0 so the client is not left waiting (the flow completed).
        uint16_t ackId = p.topicId;
        if (p.flags.topicIdType == TopicIdType::NAME &&
            !peer->topicIdFor(topic, ackId)) {
            ackId = 0;
        }
        appendU16(body, ackId);
        appendU16(body, p.msgId);
        body.push_back(static_cast<uint8_t>(reason == ReasonCode::SUCCESS
                                                ? ReturnCode::ACCEPTED
                                                : ReturnCode::INVALID_TOPIC_ID));
        sendTo(addr, encode(MsgType::PUBACK, body));
    } else if (qos == 2) {
        // PUBREC has no return code field; the flow continues with PUBREL.
        std::vector<uint8_t> body;
        appendU16(body, p.msgId);
        sendTo(addr, encode(MsgType::PUBREC, body));
    }
}

void Gateway::handleSubscribe(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || !peer->session) {
        return;
    }
    const auto reject = [&](uint16_t topicId, ReturnCode code) {
        sendSuback(addr, p.flags, topicId, p.msgId, code);
    };
    // Resolve the topic filter per the Topic Id type
    std::string filter;
    uint16_t filterId = p.topicId;
    switch (p.flags.topicIdType) {
    case TopicIdType::NAME:
        filter = p.topic;
        break;
    case TopicIdType::SHORT:
        filter.assign(2, '\0');
        filter[0] = static_cast<char>(p.topicId >> 8);
        filter[1] = static_cast<char>(p.topicId & 0xFF);
        break;
    case TopicIdType::PREDEFINED: {
        auto it = predefinedTopics.find(p.topicId);
        if (it == predefinedTopics.end()) {
            reject(p.topicId, ReturnCode::INVALID_TOPIC_ID);
            return;
        }
        filter = it->second;
        break;
    }
    case TopicIdType::NORMAL:
        filter = peer->topicNameFor(p.topicId);
        if (filter.empty()) {
            reject(p.topicId, ReturnCode::INVALID_TOPIC_ID);
            return;
        }
        break;
    }
    if (!Topic::isValid(filter)) {
        reject(filterId, ReturnCode::NOT_SUPPORTED);
        return;
    }
    if (broker_->getAuth() &&
        broker_->getAuth()->isForbiddenSubscribe(peer->username, filter)) {
        reject(filterId, ReturnCode::INVALID_TOPIC_ID);
        return;
    }
    SubscriptionOptions options;
    // Every option is set explicitly: SubscriptionOptions has no default
    // member initializers for noLocal, so leaving it unset would suppress
    // deliveries with an uninitialized flag.
    options.noLocal = false;
    // The requested QoS is 0..2; a QoS -1 flag on a SUBSCRIBE is invalid and
    // would leak into the SUBACK, so clamp it here.
    const uint8_t requestedQos = p.flags.qos > 2 ? 2 : p.flags.qos;
    options.maximumQos = static_cast<QoS>(requestedQos);
    // SN has no Retain-As-Published option, so the publisher's retain flag is
    // what the subscriber sees; retained messages always keep retain=1.
    options.retainAsPublished = true;
    options.retainHandling = RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE;
    const bool isNew = !peer->session->hasSubscription(filter);
    // A topic-name SUBSCRIBE is granted the id the gateway assigns to the filter.
    // If the peer's id space is exhausted, still honour the subscription but grant
    // id 0 rather than hang; the client can then only address it by name.
    uint16_t grantedId = filterId;
    if (p.flags.topicIdType == TopicIdType::NAME &&
        !peer->topicIdFor(filter, grantedId)) {
        grantedId = 0;
    }
    peer->session->subscribe(filter, options);
    // SUBACK first (§5.3.10), then the retained messages it unlocks. The
    // granted QoS rides in the flag byte's QoS bits.
    Flags grantedFlags = p.flags;
    grantedFlags.qos = requestedQos;
    sendSuback(addr, grantedFlags, grantedId, p.msgId, ReturnCode::ACCEPTED);
    if (!Topic::isShared(filter)) {
        peer->session->deliverRetained(filter, options, isNew);
    }
    peer->session->flush();
}

void Gateway::handleUnsubscribe(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || !peer->session) {
        return;
    }
    std::string filter;
    switch (p.flags.topicIdType) {
    case TopicIdType::NAME:
        filter = p.topic;
        break;
    case TopicIdType::SHORT:
        filter.assign(2, '\0');
        filter[0] = static_cast<char>(p.topicId >> 8);
        filter[1] = static_cast<char>(p.topicId & 0xFF);
        break;
    case TopicIdType::PREDEFINED: {
        auto it = predefinedTopics.find(p.topicId);
        if (it != predefinedTopics.end()) {
            filter = it->second;
        }
        break;
    }
    case TopicIdType::NORMAL:
        filter = peer->topicNameFor(p.topicId);
        break;
    }
    if (!filter.empty()) {
        peer->session->unsubscribe(filter);
    }
    std::vector<uint8_t> body;
    appendU16(body, p.msgId);
    sendTo(addr, encode(MsgType::UNSUBACK, body));
}

void Gateway::handlePingreq(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer || !peer->session) {
        // Only known, connected clients are served; a datagram from an
        // endpoint that disconnected (or timed out) gets no answer.
        return;
    }
    sendTo(addr, encode(MsgType::PINGRESP, {}));
    if (!p.clientId.empty()) {
        // PINGREQ carrying the client id announces "awake": an asleep device
        // is listening again, so pending deliveries are pushed now (§6.14).
        // The re-PINGREQ awake path rebinds this same peer, so keep the
        // session reference restore.
        const auto session = peer->session;
        session->resume(peer, Version::MQTT311);
        peer->session = session;
        session->flush();
    }
}

void Gateway::handleDisconnect(const sockaddr_in &addr, const Parsed &p) {
    auto peer = peerAt(addr);
    if (!peer) {
        return;
    }
    // §5.3.15: the gateway answers DISCONNECT before releasing the client.
    sendTo(addr, encode(MsgType::DISCONNECT, {}));
    if (peer->session) {
        if (peer->cleanSession) {
            peer->session->discard();
        } else if (p.duration > 0) {
            // A duration announces sleep mode: the session (and its offline
            // buffer) is kept for that long, then expires like a session
            // expiry interval.
            peer->session->armExpiry(std::chrono::seconds(p.duration));
        }
        peer->session.reset();
    }
    peers.erase(keyFor(addr));
}

void Gateway::releasePeer(const std::shared_ptr<Peer> &peer, bool abnormal) {
    if (abnormal && peer->hasWill) {
        publishWillForPeer(peer.get());
    }
    if (peer->session) {
        // A persistent session outlives the endpoint exactly as for an
        // abnormal TCP close; a non-persistent one is dropped with its state.
        peer->session->close(peer);
        peer->session.reset();
    }
    peers.erase(keyFor(peer->address));
}

void Gateway::publishWillForPeer(Peer *peer) {
    Message will{peer->willTopic, peer->willPayload, peer->willQos, peer->willRetain};
    if (broker_->getAuth() &&
        broker_->getAuth()->isForbidden(peer->username, will.topic, /*write=*/true)) {
        return;
    }
    broker_->publish(will);
}

void Gateway::tick() {
    if (socketFd_ < 0) {
        return;
    }
    const auto now = std::chrono::steady_clock::now();
    std::vector<std::shared_ptr<Peer>> expired;
    for (auto &entry : peers) {
        const auto &peer = entry.second;
        if (peer->keepAlive > 0 &&
            now - peer->lastActivity >
                std::chrono::seconds(static_cast<long>(peer->keepAlive) * 3 / 2)) {
            expired.push_back(peer);
        }
    }
    // The keep alive died: an abnormal end, so the Will goes out (§6.1) and
    // the session settles exactly as for a lost TCP client.
    for (auto &peer : expired) {
        releasePeer(peer, /*abnormal=*/true);
    }
    // §6.1 ADVERTISE: broadcast our presence every 15 minutes so new sensors
    // can find the gateway without SEARCHGW.
    const auto advertiseInterval = std::chrono::seconds(900);
    if (now - lastAdvertise >= advertiseInterval) {
        lastAdvertise = now;
        std::vector<uint8_t> body{static_cast<uint8_t>(1)};
        appendU16(body, 900);
        const std::vector<uint8_t> datagram = encode(MsgType::ADVERTISE, body);
        sockaddr_in broadcast{};
        broadcast.sin_family = AF_INET;
        broadcast.sin_addr.s_addr = INADDR_BROADCAST;
        broadcast.sin_port = htons(port_);
        sendToHelper(socketFd_, broadcast, datagram);
    }
}

} // namespace MQTTSN
} // namespace MQTT
