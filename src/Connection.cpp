#include "Frame.h"
#include "Connection.h"
#include "Topic.h"
#include "Session.h"
#include "Broker.h"
#include "Auth.h"
#include "Tls.h"
#include <unistd.h>
#include <sys/socket.h>
#include <poll.h>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <memory>
#include "Log.h"

namespace MQTT {

namespace {
//! How long a connecting client may take to send its CONNECT packet
constexpr int CONNECT_TIMEOUT_SECONDS = 10;
//! Granularity used to check the keep alive timer while waiting for data
constexpr int POLL_SLICE_MS = 250;
}

Connection::Connection(int sockfd, Broker* broker, std::shared_ptr<TlsContext> tls)
    : sockfd(sockfd), state(State::IDLE), broker(broker),
      writeTimeoutMs(broker ? broker->getWriteTimeoutMs()
                            : SecureTransport::WRITE_STALL_TIMEOUT_MS),
      auth(broker ? broker->getAuth() : nullptr),
      transport(std::make_unique<SecureTransport>(sockfd, std::move(tls), writeTimeoutMs)),
      lastActivity(std::chrono::steady_clock::now()) {
    // Cap the packet size before any byte (including the first CONNECT) is read.
    // The version is fixed later, but the limit does not depend on it.
    frame.setMaxPacketSize(broker ? broker->getMaxPacketSize() : Frame::DEFAULT_MAX_PACKET_SIZE);
}

Connection::~Connection() {
    closeSocket();
}

void Connection::run() {
    try {
        // Complete the TLS handshake before any MQTT bytes are read. A failed
        // handshake (wrong protocol, untrusted client certificate, ...) ends
        // the connection here, before a CONNECT can be forged.
        if (transport->tlsEnabled()) {
            std::string handshakeError;
            if (!transport->handshake(&handshakeError)) {
                logInfo("dropping connection: %s", handshakeError.c_str());
                return;
            }
        }
        scheduleReadTimeout();
        runReadLoop();
    } catch (const std::exception &e) {
        logError("dropping connection: %s", e.what());
    }
    state = State::DISCONNECTED;

    // An abnormal termination publishes the Will message
    if (willPending) {
        publishWill();
    }

    // Detach from the session. A graceful DISCONNECT with a zero session-expiry
    // interval discards the session immediately (MQTT-3.14.4-2); otherwise a
    // persistent session is kept so it can be resumed on a later connect. A
    // session already taken over by a newer connection is left untouched.
    if (session) {
        std::shared_ptr<Session> owned;
        {
            std::lock_guard<std::recursive_mutex> lock(writeMutex);
            owned = session;
            session.reset();
        }
        if (gracefulDisconnect && sessionExpirySeconds == 0) {
            // The client asked to end the session (or is MQTT 3.1.1): drop it
            // rather than leaving it resumable.
            owned->discard();
        } else {
            owned->close(shared_from_this());
            if (gracefulDisconnect && sessionExpirySeconds > 0) {
                // The session is kept, but must be dropped if it is not resumed
                // before the DISCONNECT Session Expiry Interval elapses
                // (MQTT-3.14.4-2). The background sweeper enforces the deadline.
                owned->armExpiry(std::chrono::seconds(sessionExpirySeconds));
            }
        }
    }
    closeSocket();
}

void Connection::scheduleReadTimeout() {
    if (state == State::CONNECTED) {
        armKeepAliveTimer();
    } else {
        readTimeout = std::chrono::milliseconds(CONNECT_TIMEOUT_SECONDS * 1000);
    }
}

void Connection::armKeepAliveTimer() {
    if (keepAliveSeconds == 0) {
        readTimeout = std::chrono::milliseconds(0); // disabled
        return;
    }
    // MQTT-3.1.2-24: the broker disconnects after 1.5 times the keep alive
    readTimeout = std::chrono::milliseconds(static_cast<int64_t>(keepAliveSeconds) * 1500);
}

void Connection::runReadLoop() {
    while (state != State::DISCONNECTED) {
        if (readTimeout.count() > 0) {
            auto deadline = lastActivity + readTimeout;
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                logDebug("keep alive expired, closing connection");
                return;
            }
            int timeout = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
            timeout = std::min(timeout, POLL_SLICE_MS);
            int ready = transport->readable(timeout);
            if (ready < 0) {
                continue;
            }
            if (ready == 0) {
                continue;
            }
        }

        ssize_t bytesRead = transport->readSome(buffer, BUFFER_SIZE);
        if (bytesRead <= 0) {
            return;
        }
        lastActivity = std::chrono::steady_clock::now();

        size_t consumed = 0;
        while (consumed < static_cast<size_t>(bytesRead) && state != State::DISCONNECTED) {
            size_t used = frame.feed(buffer + consumed, bytesRead - consumed);
            if (used == 0) {
                break;
            }
            consumed += used;
            if (frame.isComplete()) {
                auto packet = frame.parse();
                frame.reset();
                handleIncoming(packet);
            }
        }
    }
}

void Connection::resetParser() {
    frame.reset();
}

bool Connection::isConnected() const {
    return state == State::CONNECTED;
}

void Connection::closeSocket() {
    // Flush the TLS close_notify before the descriptor goes away
    if (transport) {
        transport->teardown();
    }
    if (sockfd >= 0) {
        shutdown(sockfd, SHUT_RDWR);
        close(sockfd);
        sockfd = -1;
    }
}

void Connection::setDisconnectCallback(std::function<void()> callback) {
    disconnectCallback = std::move(callback);
}

void Connection::onSuperseded() {
    // Another connection now owns the session: stop touching it and shut down.
    // The session itself is left intact so it can be resumed by the new client.
    {
        std::lock_guard<std::recursive_mutex> lock(writeMutex);
        session.reset();
        connected = false;
    }
    state = State::DISCONNECTED;
}

void Connection::publishWill() {
    willPending = false;
    Message will = willMessage;
    will.qos = willMessage.qos;
    will.retain = willMessage.retain;
    // The Will is published as the client, so the same write ACL applies as
    // for a regular PUBLISH
    const std::string username = session ? session->getUsername() : std::string();
    if (auth && auth->isForbidden(username, will.topic, /*write=*/true)) {
        logDebug("denied will on %s (acl)", will.topic.c_str());
        return;
    }
    broker->publish(will);
}

void Connection::handleIncoming(std::shared_ptr<Packet> packet) {
    switch (packet->type) {
    case PacketType::CONNECT:
        handleConnect(std::static_pointer_cast<ConnectPacket>(packet));
        break;
    case PacketType::PUBLISH:
        handlePublish(std::static_pointer_cast<PublishPacket>(packet));
        break;
    case PacketType::PUBACK:
        handlePuback(std::static_pointer_cast<PubackPacket>(packet));
        break;
    case PacketType::PUBREC:
        handlePubrec(std::static_pointer_cast<PubrecPacket>(packet));
        break;
    case PacketType::PUBREL:
        handlePubrel(std::static_pointer_cast<PubrelPacket>(packet));
        break;
    case PacketType::PUBCOMP:
        handlePubcomp(std::static_pointer_cast<PubcompPacket>(packet));
        break;
    case PacketType::SUBSCRIBE:
        handleSubscribe(std::static_pointer_cast<SubscribePacket>(packet));
        break;
    case PacketType::UNSUBSCRIBE:
        handleUnsubscribe(std::static_pointer_cast<UnsubscribePacket>(packet));
        break;
    case PacketType::PINGREQ:
        handlePingreq(std::static_pointer_cast<PingreqPacket>(packet));
        break;
    case PacketType::DISCONNECT:
        handleDisconnect(std::static_pointer_cast<DisconnectPacket>(packet));
        break;
    case PacketType::AUTH:
        handleAuth(std::static_pointer_cast<AuthPacket>(packet));
        break;
    default:
        throw std::runtime_error("Unhandled packet type");
    }
}

void Connection::handleConnect(std::shared_ptr<ConnectPacket> connect) {
    if (state != State::IDLE) {
        throw std::runtime_error("Bad connect packet");
    }
    frame.setVersion(connect->protocolVersion);
    // Topic Alias mappings are scoped to a single network connection, so a fresh
    // CONNECT starts with an empty table (MQTT-3.3.2-7).
    topicAliases.clear();

    // Authenticate before any session state is touched (and before the Will is
    // armed): a rejected client must not be able to take over an existing
    // session, learn about its messages, or publish a Will message.
    std::string username;
    if (auth && auth->authRequired()) {
        if (!connect->username) {
            logInfo("rejecting client %s: username required", connect->clientId.c_str());
            ConnackPacket connack{false, ReasonCode::BAD_USERNAME_OR_PASSWORD};
            sendPacket(connack);
            state = State::DISCONNECTED;
            return;
        }
        username = *connect->username;
        const std::string password = connect->password ? *connect->password : std::string();
        if (!auth->authenticate(username, password)) {
            logInfo("rejecting client %s: bad credentials for user %s",
                    connect->clientId.c_str(), username.c_str());
            ConnackPacket connack{false, ReasonCode::BAD_USERNAME_OR_PASSWORD};
            sendPacket(connack);
            state = State::DISCONNECTED;
            return;
        }
    } else if (connect->username) {
        username = *connect->username;
    }

    keepAliveSeconds = connect->keepAlive;
    armKeepAliveTimer();

    if (connect->willFlag && connect->willTopic && connect->willMsg) {
        willPending = true;
        willMessage = Message(*connect->willTopic, *connect->willMsg, connect->willQos, connect->willRetain);
    }

    // Resolve the session for this client id. A clean start discards any session
    // left behind by an earlier connection; otherwise the existing session is
    // reused so that its subscriptions and unacknowledged messages survive.
    auto existing = broker->findSession(connect->clientId);
    // A session belongs to the user that authenticated for it: never hand the
    // subscriptions and buffered messages of one user to another. If the
    // username changes, drop the old session and start fresh.
    if (existing && existing->getUsername() != username) {
        logInfo("client %s changed user (%s -> %s), discarding its session",
                connect->clientId.c_str(), existing->getUsername().c_str(), username.c_str());
        existing->discard();
        existing.reset();
    }
    if (existing && connect->cleanStart) {
        existing->discard();
        existing.reset();
    }
    bool resumed = (existing != nullptr) && !connect->cleanStart;

    auto sessionToUse = resumed ? existing : std::make_shared<Session>(broker, connect->clientId,
                                                                        connect->cleanStart);
    // The session belongs to the user it authenticated with, and a persistent
    // connection keeps it across disconnects (unless cleanStart discarded it)
    sessionToUse->setUsername(username);
    sessionToUse->setPersistent(!connect->cleanStart);
    session = sessionToUse;

    setDisconnectCallback([weak = weak_from_this()]() {
        if (auto connection = weak.lock()) {
            if (connection->connected) {
                connection->state = State::DISCONNECTED;
            }
        }
    });

    // Publish the CONNACK before driving the session so that no delivery can
    // race ahead of it. sessionPresent is set only when an existing session is
    // actually being resumed.
    ConnackPacket connack{false, ReasonCode::SUCCESS};
    connack.sessionPresent = resumed;
    // Advertise our Maximum Packet Size to MQTT 5 clients (MQTT-3.1.2-6): they
    // then keep their own packets within the limit we will enforce. Older
    // versions have no property section, so nothing is added for them.
    if (frame.getVersion() == Version::MQTT5) {
        const size_t limit = broker ? broker->getMaxPacketSize() : Frame::DEFAULT_MAX_PACKET_SIZE;
        // MAXIMUM_PACKET_SIZE is a 4-byte field; the limit fits comfortably.
        connack.properties[PropertyID::MAXIMUM_PACKET_SIZE] = static_cast<uint32_t>(limit);
        // Tell the client how many Topic Aliases it may use on this connection
        // (MQTT-3.1.2-26). Advertising it is what makes alias-only PUBLISHes
        // legal; the mapping itself is tracked per connection below.
        connack.properties[PropertyID::TOPIC_ALIAS_MAXIMUM] = TOPIC_ALIAS_MAXIMUM;
        // Apply the client's Receive Maximum (MQTT-3.1.2-10 / 4.9): it caps how
        // many QoS 1/2 messages the broker keeps outstanding to this client. A
        // value of 0 is a protocol violation and the broker must close the
        // connection. MQTT 3.1.1 has no such property, so nothing is set and the
        // session keeps its default of no effective limit.
        auto it = connect->properties.find(PropertyID::RECEIVE_MAXIMUM);
        if (it != connect->properties.end() && std::holds_alternative<uint16_t>(it->second)) {
            uint16_t receiveMaximum = std::get<uint16_t>(it->second);
            if (receiveMaximum == 0) {
                logInfo("client %s sent RECEIVE_MAXIMUM 0, closing connection",
                        connect->clientId.c_str());
                connack.reasonCode = ReasonCode::PROTOCOL_ERROR;
                sendPacket(connack);
                throw std::runtime_error("RECEIVE_MAXIMUM must not be 0");
            }
            sessionToUse->setReceiveMaximum(receiveMaximum);
        }
    }
    sendPacket(connack);

    {
        std::lock_guard<std::recursive_mutex> lock(writeMutex);
        connected = true;
        state = State::CONNECTED;
    }
    // The session only knows the Client interface; it keeps this connection
    // alive through the shared_ptr it receives here. resume() also retransmits
    // the unacknowledged QoS>0 messages of a resumed session.
    if (resumed) {
        sessionToUse->resume(shared_from_this());
        sessionToUse->flush();
    } else {
        sessionToUse->bindClient(shared_from_this());
        sessionToUse->connect(sessionToUse, /*persistent=*/!connect->cleanStart);
    }
    logInfo("client connected: %s (resumed=%d)", connect->clientId.c_str(), (int)resumed);
}

bool Connection::resolveTopicAlias(const std::shared_ptr<PublishPacket> &publish) {
    // MQTT 3.1.1 has no properties, so no alias can be present.
    if (frame.getVersion() != Version::MQTT5) {
        return true;
    }
    auto it = publish->properties.find(PropertyID::TOPIC_ALIAS);
    if (it == publish->properties.end()) {
        // No alias: the topic name must be present and is routed as-is.
        return true;
    }
    if (!std::holds_alternative<uint16_t>(it->second)) {
        return true;
    }
    const uint16_t alias = std::get<uint16_t>(it->second);

    // MQTT-3.3.2-8: an alias of 0, or one above what we advertised in CONNACK,
    // is a protocol error.
    if (alias == 0 || alias > TOPIC_ALIAS_MAXIMUM) {
        logDebug("client %s: invalid topic alias %u (max %u)", session->getClientId().c_str(),
                 alias, TOPIC_ALIAS_MAXIMUM);
        DisconnectPacket disconnect{ReasonCode::TOPIC_ALIAS_INVALID};
        sendPacket(disconnect);
        state = State::DISCONNECTED;
        return false;
    }

    if (!publish->topicName.empty()) {
        // The topic name registers (or updates) the mapping for this alias
        // (MQTT-3.3.2-9) and the message is routed with it.
        topicAliases[alias] = publish->topicName;
        return true;
    }

    // Empty topic name: the alias must already be mapped (MQTT-3.3.2-11).
    auto mapping = topicAliases.find(alias);
    if (mapping == topicAliases.end()) {
        logDebug("client %s: unknown topic alias %u", session->getClientId().c_str(), alias);
        DisconnectPacket disconnect{ReasonCode::TOPIC_ALIAS_INVALID};
        sendPacket(disconnect);
        state = State::DISCONNECTED;
        return false;
    }
    publish->topicName = mapping->second;
    return true;
}

void Connection::handlePublish(std::shared_ptr<PublishPacket> publish) {
    if (!session) {
        throw std::runtime_error("PUBLISH before CONNECT");
    }
    // MQTT 5 Topic Alias: expand an alias into its topic name (or register it)
    // before the message is routed, so the rest of the path only sees real
    // topics. An invalid alias closes the connection.
    if (!resolveTopicAlias(publish)) {
        return;
    }
    // Access control: a client may only publish to topics it is allowed to write
    if (auth && auth->isForbidden(session->getUsername(), publish->topicName, /*write=*/true)) {
        logDebug("client %s: denied write on %s (acl)", session->getClientId().c_str(),
                 publish->topicName.c_str());
        if (publish->qos == QoS::QOS_1) {
            PubackPacket puback{publish->packetId, ReasonCode::NOT_AUTHORIZED};
            sendPacket(puback);
        } else if (publish->qos == QoS::QOS_2) {
            PubrecPacket pubrec{publish->packetId, ReasonCode::NOT_AUTHORIZED};
            sendPacket(pubrec);
        } else if (frame.getVersion() == Version::MQTT5) {
            // MQTT 5: a QoS0 violation is reported with a DISCONNECT
            DisconnectPacket disconnect{ReasonCode::NOT_AUTHORIZED};
            sendPacket(disconnect);
            state = State::DISCONNECTED;
        }
        // MQTT 3.1.1 has no way to report a QoS0 rejection; silently drop it
        return;
    }
    Message message{publish->topicName, publish->payload, publish->qos, publish->retain};
    // MQTT 5 Message Expiry Interval (MQTT-3.3.2-9): record the absolute time at
    // which the message goes stale, so a slow or offline subscriber does not
    // receive it after it has expired.
    if (frame.getVersion() == Version::MQTT5) {
        auto it = publish->properties.find(PropertyID::MESSAGE_EXPIRY_INTERVAL);
        if (it != publish->properties.end() &&
            std::holds_alternative<uint32_t>(it->second)) {
            uint32_t interval = std::get<uint32_t>(it->second);
            message.hasExpiry = true;
            message.expiryDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(interval);
        }
    }
    ReasonCode reason = session->publish(publish->packetId, message);
    if (publish->qos == QoS::QOS_1) {
        PubackPacket puback{publish->packetId, reason};
        sendPacket(puback);
    } else if (publish->qos == QoS::QOS_2) {
        PubrecPacket pubrec{publish->packetId, reason};
        sendPacket(pubrec);
    }
}

void Connection::handlePuback(std::shared_ptr<PubackPacket> puback) {
    if (session) {
        session->puback(puback->packetId);
        // A freed window slot may have released a flow-controlled message
        session->flush();
    }
}

void Connection::handlePubrec(std::shared_ptr<PubrecPacket> pubrec) {
    if (!session) {
        return;
    }
    ReasonCode reason = session->pubrec(pubrec->packetId);
    PubrelPacket pubrel{pubrec->packetId, reason};
    sendPacket(pubrel);
}

void Connection::handlePubrel(std::shared_ptr<PubrelPacket> pubrel) {
    if (!session) {
        return;
    }
    ReasonCode reason = session->pubrel(pubrel->packetId);
    PubcompPacket pubcomp{pubrel->packetId, reason};
    sendPacket(pubcomp);
}

void Connection::handlePubcomp(std::shared_ptr<PubcompPacket> pubcomp) {
    if (session) {
        session->pubcomp(pubcomp->packetId);
        // A freed window slot may have released a flow-controlled message
        session->flush();
    }
}

void Connection::handleSubscribe(std::shared_ptr<SubscribePacket> subscribe) {
    if (!session) {
        throw std::runtime_error("SUBSCRIBE before CONNECT");
    }
    // Sentinel meaning "this filter was accepted" in the per-filter outcome list.
    constexpr int acceptMarker = -1;
    // Access control: subscribe only to filters the client may read. A denied
    // filter is reported in the SUBACK and is not registered. A session also
    // has a bounded number of subscriptions so one client cannot exhaust memory
    // by subscribing to an unbounded set of filters; filters beyond the limit
    // are reported with QUOTA_EXCEEDED.
    const bool isV5 = frame.getVersion() == Version::MQTT5;
    const size_t maxSubscriptions = session->getMaxSubscriptions();
    // Per-filter outcome: <0 means accepted (the granted QoS is recorded later),
    // otherwise the reason code reported for a rejected filter.
    std::vector<int> rejected(subscribe->subscriptions.size(), acceptMarker);
    for (size_t i = 0; i < subscribe->subscriptions.size(); ++i) {
        const auto& subscription = subscribe->subscriptions[i];
        if (auth && auth->isForbidden(session->getUsername(), subscription.first,
                                      /*write=*/false)) {
            logDebug("client %s: denied subscribe to %s (acl)", session->getClientId().c_str(),
                     subscription.first.c_str());
            rejected[i] = isV5 ? static_cast<int>(ReasonCode::NOT_AUTHORIZED) : 0x80;
            continue;
        }
        if (!session->hasSubscription(subscription.first) &&
            session->getSubscriptionCount() >= maxSubscriptions) {
            // New filter would push the session past its quota; a re-subscribe
            // of an existing filter is still allowed (it only updates options).
            logDebug("client %s: subscription quota (%zu) reached, rejecting %s",
                     session->getClientId().c_str(), maxSubscriptions,
                     subscription.first.c_str());
            rejected[i] = isV5 ? static_cast<int>(ReasonCode::QUOTA_EXCEEDED) : 0x80;
            continue;
        }
        session->subscribe(subscription.first, subscription.second);
    }

    SubackPacket suback{subscribe->packetId};
    // Grant the requested QoS, or report the per-filter failure otherwise
    for (size_t i = 0; i < subscribe->subscriptions.size(); ++i) {
        if (rejected[i] != acceptMarker) {
            suback.reasonCodes.push_back(static_cast<ReasonCode>(static_cast<uint8_t>(rejected[i])));
        } else {
            suback.reasonCodes.push_back(static_cast<ReasonCode>(
                static_cast<uint8_t>(subscribe->subscriptions[i].second.maximumQos)));
        }
    }
    sendPacket(suback);

    // Retained messages are delivered after the SUBACK (MQTT-3.3.1-9)
    for (size_t i = 0; i < subscribe->subscriptions.size(); ++i) {
        if (rejected[i] != acceptMarker) {
            continue;
        }
        const auto& subscription = subscribe->subscriptions[i];
        if (!Topic::isShared(subscription.first)) {
            session->deliverRetained(subscription.first, subscription.second);
        }
    }
    // The queued retained messages are written here, with no lock held
    session->flush();
}

void Connection::handleUnsubscribe(std::shared_ptr<UnsubscribePacket> unsubscribe) {
    if (!session) {
        throw std::runtime_error("UNSUBSCRIBE before CONNECT");
    }
    for (const auto& topic : unsubscribe->topicFilters) {
        session->unsubscribe(topic);
    }
    UnsubackPacket unsuback{unsubscribe->packetId};
    if (frame.getVersion() != Version::MQTT5) {
        // MQTT 3.1.1 has no reason codes in UNSUBACK
        unsuback.reasonCodes.clear();
    }
    sendPacket(unsuback);
}

void Connection::handlePingreq(std::shared_ptr<PingreqPacket> pingreq) {
    PingrespPacket pingResp;
    sendPacket(pingResp);
}

void Connection::handleDisconnect(std::shared_ptr<DisconnectPacket> disconnect) {
    // A normal DISCONNECT discards the Will message (MQTT-3.14.4-3); the read
    // loop then exits and run() tears the session down.
    willPending = false;
    // MQTT 5 Session Expiry Interval (MQTT-3.14.4-2): 0 (or absent when the
    // CONNECT did not request persistence) discards the session immediately;
    // a positive interval keeps it resumable for that long. An interval of 0
    // overrides persistence, so a graceful DISCONNECT ends the session.
    gracefulDisconnect = true;
    if (frame.getVersion() == Version::MQTT5) {
        auto it = disconnect->properties.find(PropertyID::SESSION_EXPIRY_INTERVAL);
        if (it != disconnect->properties.end() &&
            std::holds_alternative<uint32_t>(it->second)) {
            uint32_t interval = std::get<uint32_t>(it->second);
            sessionExpirySeconds = interval;
        }
    }
    state = State::DISCONNECTED;
}

void Connection::handleAuth(std::shared_ptr<AuthPacket> auth) {
    // TODO: implement enhanced authentication
    AuthPacket authResp;
    sendPacket(authResp);
}

void Connection::deliver(const Message &message, uint16_t packetId, QoS qos) {
    handleDeliver(message, packetId, qos);
}

void Connection::handleDeliver(const Message &message, uint16_t packetId, QoS qos) {
    if (state != State::CONNECTED) {
        return;
    }
    PublishPacket publish{message.topic, message.payload, qos, message.retain};
    publish.packetId = packetId;
    publish.dup = message.dup;
    logDebug("deliver %s qos=%d packetId=%u dup=%d", message.topic.c_str(), (int)qos, packetId,
             (int)message.dup);
    sendPacket(publish);
}

void Connection::sendPubrec(uint16_t packetId) {
    if (state != State::CONNECTED) {
        return;
    }
    // The original PUBREC always carried SUCCESS (only successful inbound QoS 2
    // flows are remembered), so the resend does too.
    PubrecPacket pubrec{packetId, ReasonCode::SUCCESS};
    logDebug("resend PUBREC packetId=%u (awaiting PUBREL)", packetId);
    sendPacket(pubrec);
}

void Connection::sendPacket(Packet &packet) {
    std::lock_guard<std::recursive_mutex> lock(writeMutex);
    if (sockfd < 0) {
        return;
    }
    auto data = frame.serialize(packet);
    if (!transport->writeAll(data.data(), data.size())) {
        // The peer is gone; stop the connection on the next loop iteration
        state = State::DISCONNECTED;
    }
}

} // namespace MQTT
