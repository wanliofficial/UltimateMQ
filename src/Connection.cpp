#include "Frame.h"
#include "Connection.h"
#include "Topic.h"
#include "Session.h"
#include "Broker.h"
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

Connection::Connection(int sockfd, Broker* broker)
    : sockfd(sockfd), state(State::IDLE), broker(broker),
      lastActivity(std::chrono::steady_clock::now()) { }

Connection::~Connection() {
    closeSocket();
}

void Connection::run() {
    try {
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

    // Detach from the session: from now on the broker may deliver to the
    // session and the stale callbacks become no-ops
    if (session) {
        std::lock_guard<std::recursive_mutex> lock(writeMutex);
        session.reset();
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
            struct pollfd pfd{sockfd, POLLIN, 0};
            int ready = poll(&pfd, 1, timeout);
            if (ready < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return;
            }
            if (ready == 0) {
                continue;
            }
        }

        ssize_t bytesRead = read(sockfd, buffer, BUFFER_SIZE);
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
    if (sockfd >= 0) {
        shutdown(sockfd, SHUT_RDWR);
        close(sockfd);
        sockfd = -1;
    }
}

void Connection::publishWill() {
    willPending = false;
    Message will = willMessage;
    will.qos = willMessage.qos;
    will.retain = willMessage.retain;
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

    keepAliveSeconds = connect->keepAlive;
    armKeepAliveTimer();

    if (connect->willFlag && connect->willTopic && connect->willMsg) {
        willPending = true;
        willMessage = Message(*connect->willTopic, *connect->willMsg, connect->willQos, connect->willRetain);
    }

    // Take over any session bound to this client id: the previous connection is
    // told to stop using it. This is race free: the takeover flag is set under
    // the session lock, and the next delivery on the old session either sees it
    // here (and is dropped) or has already been queued before the flag was set.
    auto oldSession = broker->findSession(connect->clientId);
    if (oldSession) {
        oldSession->setTakeoverCallback([weak = weak_from_this()]() {
            if (auto connection = weak.lock()) {
                connection->state = State::DISCONNECTED;
                connection->connected = false;
                // The session is about to be reassigned, so stop using this one
                connection->session.reset();
            }
        });
    }

    auto newSession = std::make_shared<Session>(broker, connect->clientId,
                                                connect->cleanStart);
    bool resumed = (oldSession != nullptr) && !connect->cleanStart;

    newSession->setDisconnectCallback([weak = weak_from_this()]() {
        if (auto connection = weak.lock()) {
            if (connection->connected) {
                connection->state = State::DISCONNECTED;
            }
        }
    });

    // Publish the CONNACK before connecting so that no delivery can race ahead of it
    ConnackPacket connack{false, ReasonCode::SUCCESS};
    connack.sessionPresent = resumed;
    sendPacket(connack);

    {
        std::lock_guard<std::recursive_mutex> lock(writeMutex);
        session = newSession;
        connected = true;
        state = State::CONNECTED;
    }
    // The session only knows the Client interface; it keeps this connection
    // alive through the shared_ptr it receives here
    newSession->bindClient(shared_from_this());
    newSession->connect(newSession);
    logInfo("client connected: %s", connect->clientId.c_str());
}

void Connection::handlePublish(std::shared_ptr<PublishPacket> publish) {
    if (!session) {
        throw std::runtime_error("PUBLISH before CONNECT");
    }
    Message message{publish->topicName, publish->payload, publish->qos, publish->retain};
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
    }
}

void Connection::handleSubscribe(std::shared_ptr<SubscribePacket> subscribe) {
    if (!session) {
        throw std::runtime_error("SUBSCRIBE before CONNECT");
    }
    for (const auto& subscription : subscribe->subscriptions) {
        session->subscribe(subscription.first, subscription.second);
    }
    SubackPacket suback{subscribe->packetId};

    // Grant the requested QoS
    for (const auto& subscription : subscribe->subscriptions) {
        suback.reasonCodes.push_back(static_cast<ReasonCode>(static_cast<uint8_t>(subscription.second.maximumQos)));
    }
    sendPacket(suback);

    // Retained messages are delivered after the SUBACK (MQTT-3.3.1-9)
    for (const auto& subscription : subscribe->subscriptions) {
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
    // A normal DISCONNECT discards the Will message (MQTT-3.14.4-3)
    willPending = false;
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
    logDebug("deliver %s qos=%d packetId=%u", message.topic.c_str(), (int)qos, packetId);
    sendPacket(publish);
}

void Connection::sendPacket(Packet &packet) {
    std::lock_guard<std::recursive_mutex> lock(writeMutex);
    if (sockfd < 0) {
        return;
    }
    auto data = frame.serialize(packet);
    size_t sent = 0;
    while (sent < data.size()) {
        ssize_t written = send(sockfd, data.data() + sent, data.size() - sent, MSG_NOSIGNAL);
        if (written < 0) {
            if (errno == EINTR) {
                continue;
            }
            // The peer is gone; stop the connection on the next loop iteration
            state = State::DISCONNECTED;
            return;
        }
        sent += static_cast<size_t>(written);
    }
}

} // namespace MQTT
