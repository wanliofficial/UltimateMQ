#include <string>
#include <cstdint>
#include <functional>
#include <iostream>
#include "MQTT.h"
#include "Topic.h"
#include "Session.h"
#include "Broker.h"
#include "Message.h"
#include "Log.h"

namespace MQTT {

Session::Session(Broker *broker, const std::string &clientId, bool cleanStart)
    : clientId(clientId), cleanStart(cleanStart), broker(broker) {}

Session::~Session() {
    // The broker only holds weak references, but drop the map entry defensively
    if (registered.load()) {
        broker->forgetSession(this);
    }
}

void Session::connect(const std::shared_ptr<Session> &self) {
    selfRef = self;
    connected = true;
    // The broker holds a weak reference: if another connection registered the
    // same client id first, this session stays unregistered and must be dropped
    if (broker->insertSessionIfAbsent(clientId, self)) {
        registered = true;
    }
    logDebug("session connected: %s (registered=%d)", clientId.c_str(), (int)registered.load());
}

void Session::discard() {
    logDebug("session discarded: %s", clientId.c_str());
    disconnect();
}

void Session::disconnect() {
    if (registered.exchange(false)) {
        broker->forgetSession(this);
    }
    connected = false;
    std::function<void()> notify;
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        pendingDeliveries.clear();
        notify = std::move(onDisconnect);
        onDisconnect = nullptr;
    }
    if (notify) {
        notify();
    }
}

ReasonCode Session::publish(uint16_t packetId, const Message &message) {
    broker->publish(message);
    if (message.qos == QoS::QOS_2) {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        awaitingPubrel.insert(packetId);
    }
    return ReasonCode::SUCCESS;
}

void Session::puback(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    inflightMessages.erase(packetId);
}

ReasonCode Session::pubrec(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    inflightMessages.erase(packetId);
    return ReasonCode::SUCCESS;
}

ReasonCode Session::pubrel(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    awaitingPubrel.erase(packetId);
    return ReasonCode::SUCCESS;
}

void Session::pubcomp(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    inflightMessages.erase(packetId);
}

void Session::subscribe(const std::string &topicFilter, const SubscriptionOptions &options) {
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        newSubscription = subscriptions.find(topicFilter) == subscriptions.end();
        subscriptions[topicFilter] = options;
    }
    if (Topic::isShared(topicFilter)) {
        auto [group, realTopicFilter] = Topic::splitShared(topicFilter);
        broker->sharedSubscribe(clientId, realTopicFilter, group);
    } else {
        broker->subscribe(clientId, topicFilter);
    }
}

void Session::unsubscribe(const std::string &topicFilter) {
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        subscriptions.erase(topicFilter);
    }
    if (Topic::isShared(topicFilter)) {
        auto [group, realTopicFilter] = Topic::splitShared(topicFilter);
        broker->sharedUnsubscribe(clientId, realTopicFilter, group);
    } else {
        broker->unsubscribe(clientId, topicFilter);
    }
}

void Session::bindClient(const std::shared_ptr<Client>& client) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    this->client = client;
    pendingDeliveries.clear();
}

void Session::setTakeoverCallback(std::function<void()> callback) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    onTakeover = std::move(callback);
}

void Session::dropOnDelivery() {
    if (!onTakeover) {
        return;
    }
    auto takeover = std::move(onTakeover);
    onTakeover = nullptr;
    pendingDeliveries.clear();
    takeover();
    // The session has been handed over to a newer connection: no outdated
    // message may reach the client that took it over
    pendingDeliveries.clear();
}

void Session::setDisconnectCallback(std::function<void()> callback) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    onDisconnect = std::move(callback);
}

void Session::deliver(const std::string &topic, const Message &message) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (onTakeover) {
        // The session has been taken over: tell the old connection to release
        // it before anything else is delivered
        dropOnDelivery();
        return;
    }

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
    pushDelivery(outgoing, qos);
}

void Session::deliverRetained(const std::string &topicFilter, const SubscriptionOptions &options) {
    if (options.retainHandling == RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES) {
        return;
    }

    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
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
        pushDelivery(outgoing, qos);
    }
}

void Session::pushDelivery(const Message &message, QoS qos) {
    Message outgoing = message;
    outgoing.qos = qos;
    uint16_t id = 0;
    if (qos != QoS::QOS_0) {
        id = nextPacketId();
        inflightMessages[id] = std::make_shared<Message>(outgoing);
    }
    // Only queue while the broker lock is held (Broker::publish) or while this
    // session lock is held: flush() is then called outside of both.
    pendingDeliveries.emplace_back(std::move(outgoing), id, qos);
}

std::string Session::matchingFilter(const std::string &topic) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    // The trie only returns filters that actually match, but a session may
    // hold the same filter under a different client; return the first hit.
    for (const auto &[filter, options] : subscriptions) {
        if (Topic::match(topic, filter)) {
            return filter;
        }
    }
    return topic;
}

void Session::flush() {
    std::vector<Delivery> deliveries;
    std::shared_ptr<Client> target;
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        if (pendingDeliveries.empty()) {
            return;
        }
        deliveries.swap(pendingDeliveries);
        target = client;
    }
    if (!target) {
        return;
    }
    for (const auto &delivery : deliveries) {
        target->deliver(delivery.message, delivery.packetId, delivery.qos);
    }
}

uint16_t Session::nextPacketId() {
    if (packetId == 0) {
        packetId = 1;
    }
    return packetId++;
}

} // namespace MQTT
