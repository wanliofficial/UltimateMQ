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
    : clientId(clientId), cleanStart(cleanStart), broker(broker) {
    if (broker) {
        auth = broker->getAuth();
    }
}

Session::~Session() = default;

void Session::connect(const std::shared_ptr<Session> &self, bool persistent) {
    this->persistent = persistent;
    connected = true;
    if (broker) {
        auth = broker->getAuth();
    }
    // The broker keeps a strong reference so the session (and its in-flight
    // state) can outlive the TCP connection for a later reconnect
    broker->insertSession(clientId, self);
    // A persistent session is durable state from now on
    if (persistent) {
        broker->notifyStateChanged();
    }
    logDebug("session connected: %s (persistent=%d)", clientId.c_str(), (int)persistent);
}

Session::Snapshot Session::snapshot() const {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    Snapshot snapshot;
    snapshot.clientId = clientId;
    snapshot.username = username;
    snapshot.nextPacketId = packetId;
    snapshot.subscriptions = subscriptions;
    snapshot.inflight.reserve(inflightMessages.size());
    for (const auto &entry : inflightMessages) {
        if (entry.second) {
            snapshot.inflight.emplace_back(entry.first, *entry.second);
        }
    }
    snapshot.awaitingPubrel = awaitingPubrel;
    return snapshot;
}

void Session::restore(const Snapshot &snapshot) {
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        username = snapshot.username;
        // A restored session was persistent by definition
        persistent = true;
        packetId = snapshot.nextPacketId == 0 ? 1 : snapshot.nextPacketId;
        subscriptions = snapshot.subscriptions;
        inflightMessages.clear();
        for (const auto &entry : snapshot.inflight) {
            inflightMessages[entry.first] = std::make_shared<Message>(entry.second);
        }
        awaitingPubrel = snapshot.awaitingPubrel;
    }
    connected = false;
}

void Session::setMutateCallback(std::function<void()> callback) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    mutateCallback = std::move(callback);
}

void Session::notifyMutation() {
    // Called with deliveryMutex held. The callback only flips an atomic flag, so
    // it can never re-enter the session or block on another lock.
    if (mutateCallback) {
        mutateCallback();
    }
}

void Session::close(const std::shared_ptr<Client> &client) {
    bool wasBound;
    std::function<void()> notify;
    std::map<std::string, SubscriptionOptions> filters;
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        wasBound = (this->client == client);
        if (!wasBound) {
            // A newer connection has already taken this session over: nothing to do
            return;
        }
        this->client.reset();
        if (!persistent) {
            // A non-persistent session holds nothing worth keeping
            pendingDeliveries.clear();
            filters.swap(subscriptions);
        }
        // A persistent session keeps its subscriptions, its offline message
        // buffer (pendingDeliveries) and its unacknowledged inflight messages
        // so that they can be retransmitted on the next connect
        notify = std::move(onDisconnect);
        onDisconnect = nullptr;
    }
    connected = false;
    if (!persistent) {
        removeSubscriptions(filters);
        broker->forgetSession(this);
    }
    // The durable state changed (session went offline, or was dropped entirely)
    broker->notifyStateChanged();
    logDebug("session closed: %s (persistent=%d)", clientId.c_str(), (int)persistent);
    if (notify) {
        notify();
    }
}

void Session::removeSubscriptions(const std::map<std::string, SubscriptionOptions> &filters) {
    // Drop this client from the broker routing tables. Called without any
    // session lock so that the Broker -> Session lock order is preserved.
    for (const auto &entry : filters) {
        const std::string &filter = entry.first;
        if (Topic::isShared(filter)) {
            auto [group, realTopicFilter] = Topic::splitShared(filter);
            broker->sharedUnsubscribe(clientId, realTopicFilter, group);
        } else {
            broker->unsubscribe(clientId, filter);
        }
    }
}

void Session::discard() {
    logDebug("session discarded: %s", clientId.c_str());
    std::function<void()> notify;
    std::map<std::string, SubscriptionOptions> filters;
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        // Throw away everything: subscriptions, offline buffer and inflight
        filters.swap(subscriptions);
        pendingDeliveries.clear();
        inflightMessages.clear();
        awaitingPubrel.clear();
        client.reset();
        notify = std::move(onDisconnect);
        onDisconnect = nullptr;
    }
    connected = false;
    removeSubscriptions(filters);
    broker->forgetSession(this);
    // The session is gone: drop it from the durable state as well
    broker->notifyStateChanged();
    if (notify) {
        notify();
    }
}

ReasonCode Session::publish(uint16_t packetId, const Message &message) {
    broker->publish(message);
    if (message.qos == QoS::QOS_2) {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        awaitingPubrel.insert(packetId);
        notifyMutation();
    }
    return ReasonCode::SUCCESS;
}

void Session::puback(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (inflightMessages.erase(packetId) > 0) {
        notifyMutation();
    }
}

ReasonCode Session::pubrec(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (inflightMessages.erase(packetId) > 0) {
        notifyMutation();
    }
    return ReasonCode::SUCCESS;
}

ReasonCode Session::pubrel(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (awaitingPubrel.erase(packetId) > 0) {
        notifyMutation();
    }
    return ReasonCode::SUCCESS;
}

void Session::pubcomp(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (inflightMessages.erase(packetId) > 0) {
        notifyMutation();
    }
}

void Session::subscribe(const std::string &topicFilter, const SubscriptionOptions &options) {
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        newSubscription = subscriptions.find(topicFilter) == subscriptions.end();
        subscriptions[topicFilter] = options;
        notifyMutation();
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
        if (subscriptions.erase(topicFilter) > 0) {
            notifyMutation();
        }
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

void Session::resume(const std::shared_ptr<Client>& client) {
    std::shared_ptr<Client> previous;
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        previous = std::move(this->client);
        this->client = client;
        // Retransmit every QoS>0 message that was delivered but never
        // acknowledged, keeping the original packet id and setting DUP=1
        // (MQTT-3.3.1-8 / MQTT-4.4.0-1)
        for (const auto& [id, stored] : inflightMessages) {
            Message outgoing = *stored;
            outgoing.dup = true;
            pendingDeliveries.emplace_back(std::move(outgoing), id, stored->qos);
        }
    }
    if (previous) {
        // Tell the connection that lost the session to release it
        previous->onSuperseded();
    }
}

void Session::setDisconnectCallback(std::function<void()> callback) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    onDisconnect = std::move(callback);
}

void Session::deliver(const std::string &topic, const Message &message) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);

    // Enforce read access before anything else: an unauthorized client must not
    // learn that a message exists on a topic it can read via subscription
    if (auth && auth->isForbidden(username, topic, /*write=*/false)) {
        logDebug("session %s: denied read on %s (acl)", clientId.c_str(), topic.c_str());
        return;
    }

    QoS qos = message.qos;
    auto it = subscriptions.find(topic);
    if (it != subscriptions.end() && it->second.maximumQos < qos) {
        // The delivered QoS is the minimum of the published QoS and the granted QoS
        qos = it->second.maximumQos;
    }

    Message outgoing = message;
    outgoing.dup = false;
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

    // Enforce read access per retained topic below: an unauthorized client must
    // not learn that a retained message exists on a topic it may not read. A
    // filter can be broader than the rule that actually grants it, so the check
    // uses each concrete topic, not the subscription filter.

    // Snapshot the retained messages under the broker lock (never while holding
    // the session lock, to keep Broker -> Session lock ordering)
    auto retained = broker->snapshotRetained();

    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (options.retainHandling == RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE_IF_NEW &&
        !newSubscription) {
        return;
    }
    for (const auto &[topic, message] : retained) {
        if (!Topic::match(topic, topicFilter)) {
            continue;
        }
        if (auth && auth->isForbidden(username, topic, /*write=*/false)) {
            logDebug("session %s: denied read on %s (acl, retained)", clientId.c_str(), topic.c_str());
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
    // Bound the per-session queue: a persistent session that stays offline
    // would otherwise grow without limit. Once the cap is reached new messages
    // are dropped rather than buffered forever.
    if (pendingDeliveries.size() >= MAX_QUEUED_MESSAGES) {
        logDebug("session %s: queue full (%zu), dropping message on %s", clientId.c_str(),
                 pendingDeliveries.size(), message.topic.c_str());
        return;
    }
    Message outgoing = message;
    outgoing.qos = qos;
    uint16_t id = 0;
    if (qos != QoS::QOS_0) {
        id = nextPacketId();
        inflightMessages[id] = std::make_shared<Message>(outgoing);
        notifyMutation();
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
        target = client;
        if (!target || pendingDeliveries.empty()) {
            // No client bound (offline persistent session): keep the queue so
            // it can be delivered when the client reconnects
            return;
        }
        deliveries.swap(pendingDeliveries);
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
