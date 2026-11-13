#include <string>
#include <cstdint>
#include <string>
#include <functional>
#include <iostream>
#include "MQTT.h"
#include "Topic.h"
#include "Session.h"
#include "Broker.h"
#include "Message.h"

namespace MQTT {
Session::Session(Broker *broker, const std::string &clientId, bool cleanStart)
    : broker(broker), clientId(clientId), connected(false), cleanStart(cleanStart) {}

Session::~Session() {
    std::cout << "Session::~Session: " << clientId << std::endl;
    broker->removeSession(this);
}

void Session::connect() {
    connected = true;
    broker->insertSession(clientId, this);
    printf("Session::connect: %s\n", clientId.c_str());
}

void Session::discard() {
    printf("Session::discard: %s\n", clientId.c_str());
    disconnect();
}

void Session::disconnect() {
    broker->removeSession(this);
    connected = false;
    if (onDisconnect) {
        onDisconnect();
    }
}

ReasonCode Session::publish(uint16_t packetId, const Message &message) {
    broker->publish(message);
    if (message.qos == QoS::QOS_2) {
        awaitingPubrel.insert(packetId);
    }
    return ReasonCode::SUCCESS;
}

void Session::puback(uint16_t packetId) {
    inflightMessages.erase(packetId);
}

ReasonCode Session::pubrec(uint16_t packetId) {
    inflightMessages.erase(packetId);
    return ReasonCode::SUCCESS;
}

ReasonCode Session::pubrel(uint16_t packetId) {
    awaitingPubrel.erase(packetId);
    return ReasonCode::SUCCESS;
}

void Session::pubcomp(uint16_t packetId) {
    // broker->pubcomp(packet);
}

void Session::subscribe(const std::string &topicFilter, const SubscriptionOptions &options) {
    if (Topic::isShared(topicFilter)) {
        auto [group, realTopicFilter] = Topic::splitShared(topicFilter);
        broker->sharedSubscribe(clientId, realTopicFilter, group);
        newSubscription = subscriptions.find(realTopicFilter) == subscriptions.end();
        subscriptions[realTopicFilter] = options;
        // Shared subscriptions never receive retained messages
    }
    else {
        newSubscription = subscriptions.find(topicFilter) == subscriptions.end();
        broker->subscribe(clientId, topicFilter);
        subscriptions[topicFilter] = options;
    }
}

void Session::unsubscribe(const std::string &topicFilter) {
    if (Topic::isShared(topicFilter)) {
        auto [group, realTopicFilter] = Topic::splitShared(topicFilter);
        broker->sharedUnsubscribe(clientId, realTopicFilter, group);
        subscriptions.erase(realTopicFilter);
    } else {
        broker->unsubscribe(clientId, topicFilter);
        subscriptions.erase(topicFilter);
    }
}

void Session::setDeliverCallback(std::function<void(const Message &, uint16_t, QoS)> callback) {
    onDeliver = callback;
}

void Session::deliver(const std::string &topic, const Message &message) {
    QoS qos = message.qos;
    auto it = subscriptions.find(topic);
    if (it != subscriptions.end() && it->second.maximumQos < qos) {
        // The delivered QoS is the minimum of the published QoS and the granted QoS
        qos = it->second.maximumQos;
    }
    Message outgoing = message;
    if (it != subscriptions.end() && !it->second.retainAsPublished) {
        // MQTT-3.3.1-8: when forwarding a live message the retain flag is cleared
        // unless the subscription asked for retain-as-published
        outgoing.retain = false;
    }
    deliverWithQos(outgoing, qos);
}

void Session::deliverRetained(const std::string &topicFilter, const SubscriptionOptions &options) {
    if (options.retainHandling == RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES) {
        return;
    }
    if (options.retainHandling == RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE_IF_NEW &&
        !newSubscription) {
        return;
    }
    for (const auto &[topic, message] : broker->getRetainedMessages()) {
        if (!Topic::match(topic, topicFilter)) {
            continue;
        }
        Message outgoing = message;
        // MQTT-3.3.1-8: messages sent because of a subscription keep the retain flag
        outgoing.retain = true;
        QoS qos = message.qos < options.maximumQos ? message.qos : options.maximumQos;
        deliverWithQos(outgoing, qos);
    }
}

void Session::deliverWithQos(const Message &message, QoS qos) {
    Message outgoing = message;
    outgoing.qos = qos;
    if (qos == QoS::QOS_0) {
        onDeliver(outgoing, 0, qos);
        return;
    }
    packetId = nextPacketId();
    inflightMessages[packetId] = std::make_shared<Message>(outgoing);
    onDeliver(outgoing, packetId, qos);
}

void Session::setDisconnectCallback(std::function<void()> callback) {
    onDisconnect = callback;
}

inline uint16_t Session::nextPacketId() {
    if (packetId++ > UINT16_MAX) {
        packetId = 1;
    }
    return packetId;
}

}