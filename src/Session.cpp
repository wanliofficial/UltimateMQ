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
    // A live session never expires: cancel any disconnect-time expiry
    clearExpiry();
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
    snapshot.pendingPubrel = pendingPubrel;
    for (const auto &blocked : blockedDeliveries) {
        snapshot.blocked.push_back(blocked.message);
    }
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
        // The subscription identifier is not part of the durable snapshot, so a
        // restored session starts with none recorded; a retransmission after the
        // next resume simply omits it.
        inflightIdentifiers.clear();
        for (const auto &entry : snapshot.inflight) {
            inflightMessages[entry.first] = std::make_shared<Message>(entry.second);
        }
        awaitingPubrel = snapshot.awaitingPubrel;
        pendingPubrel = snapshot.pendingPubrel;
        blockedDeliveries.clear();
        for (const auto &m : snapshot.blocked) {
            Message copy = m;
            copy.hasExpiry = false;  // restored backlog never expires in memory
            blockedDeliveries.emplace_back(std::move(copy), /*packetId=*/0, copy.qos);
        }
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
            blockedDeliveries.clear();
            filters.swap(subscriptions);
        }
        // A persistent session keeps its subscriptions, its offline message
        // buffer (pendingDeliveries), its flow-control backlog
        // (blockedDeliveries) and its unacknowledged inflight messages so that
        // they can be retransmitted on the next connect
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
        blockedDeliveries.clear();
        inflightMessages.clear();
        awaitingPubrel.clear();
        pendingPubrel.clear();
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

void Session::armExpiry(std::chrono::seconds seconds) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (seconds.count() <= 0) {
        hasExpiry = false;
        return;
    }
    expiryDeadline = std::chrono::steady_clock::now() + seconds;
    hasExpiry = true;
}

void Session::clearExpiry() {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    hasExpiry = false;
}

bool Session::isExpired(std::chrono::steady_clock::time_point now) const {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    return hasExpiry && now >= expiryDeadline;
}

ReasonCode Session::publish(uint16_t packetId, const Message &message, bool duplicate) {
    // Tag the message with this session so the fan-out can honour the MQTT 5
    // No Local option: a subscriber that opted in must not receive its own
    // publications. Set for every inbound publish; a restored (durable) message
    // never goes through here, so its originator stays null.
    Message outgoing = message;
    outgoing.originSession = this;
    if (outgoing.qos == QoS::QOS_2) {
        {
            std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
            // A QoS 2 flow is complete only once the client's PUBREL has been
            // answered. A second PUBLISH reusing that identifier would start a
            // flow whose PUBREL could never be paired, so MQTT-3.3.4-1 forbids
            // it; report PACKET_IDENTIFIER_IN_USE (0x91) without re-routing.
            if (awaitingPubrel.count(packetId) > 0) {
                // A DUP retransmit (the client did not get our PUBREC) is NOT a
                // new identifier clash: the flow is already in progress. Do not
                // re-route the message, but tell the caller to resend PUBREC
                // with SUCCESS (MQTT-4.3.3-1). Returning anything else would
                // look like a hard error to a client that is simply retrying.
                if (duplicate) {
                    return ReasonCode::SUCCESS;
                }
                return ReasonCode::PACKET_IDENTIFIER_IN_USE;
            }
        }
        broker->publish(outgoing);
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        awaitingPubrel.insert(packetId);
        notifyMutation();
        return ReasonCode::SUCCESS;
    }
    broker->publish(outgoing);
    return ReasonCode::SUCCESS;
}

void Session::puback(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (inflightMessages.erase(packetId) > 0) {
        inflightIdentifiers.erase(packetId);
        notifyMutation();
        releaseBlocked();
    }
}

std::optional<ReasonCode> Session::pubrec(uint16_t packetId, ReasonCode clientReason) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    // A PUBREC with a Reason Code of 0x80 or greater means the client refused the
    // message. The flow ends there: the PUBLISH counts as acknowledged and must
    // never be retransmitted, so the identifier is released at once, no PUBREL is
    // sent, and nothing is remembered for a resume (MQTT-4.3.3 / MQTT-4.4.0-2).
    const bool rejected = static_cast<uint8_t>(clientReason) >= 0x80;
    if (inflightMessages.erase(packetId) > 0) {
        inflightIdentifiers.erase(packetId);
        // The PUBLISH is acknowledged; from now on only the PUBREL/PUBCOMP leg
        // of the QoS 2 flow is outstanding. Remember it so a resume can re-send
        // the PUBREL if the connection drops before the PUBCOMP (MQTT-4.4.0-1).
        // A rejected message has no such leg, so its id is not remembered.
        if (!rejected) {
            pendingPubrel.insert(packetId);
        }
        notifyMutation();
        releaseBlocked();
        if (rejected) {
            return std::nullopt;
        }
        return ReasonCode::SUCCESS;
    }
    // No such outstanding QoS 2 delivery: MQTT-4.3.3-5 expects
    // PACKET_IDENTIFIER_NOT_FOUND. The PUBREL is still sent so the client can
    // complete (or abandon) its flow; a client that sent a stale PUBREC simply
    // gets the matching PUBCOMP below. A rejected stale PUBREC needs no answer.
    if (rejected) {
        return std::nullopt;
    }
    return ReasonCode::PACKET_IDENTIFIER_NOT_FOUND;
}

ReasonCode Session::pubrel(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    if (awaitingPubrel.erase(packetId) > 0) {
        notifyMutation();
        return ReasonCode::SUCCESS;
    }
    // A PUBREL for an identifier that started no QoS 2 flow (MQTT-4.3.3-5).
    return ReasonCode::PACKET_IDENTIFIER_NOT_FOUND;
}

void Session::pubcomp(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    // The PUBCOMP completes the outgoing QoS 2 flow. It normally follows the
    // PUBREC that moved the id into pendingPubrel, but a client that skips
    // straight to PUBCOMP (or sends a duplicate) must still clear both.
    bool freed = false;
    if (pendingPubrel.erase(packetId) > 0) {
        freed = true;
    }
    if (inflightMessages.erase(packetId) > 0) {
        inflightIdentifiers.erase(packetId);
        freed = true;
    }
    if (freed) {
        notifyMutation();
        releaseBlocked();
    }
}

void Session::subscribe(const std::string &topicFilter, const SubscriptionOptions &options) {
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
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

bool Session::hasSubscription(const std::string &topicFilter) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    return subscriptions.find(topicFilter) != subscriptions.end();
}

size_t Session::getSubscriptionCount() {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    return subscriptions.size();
}

size_t Session::getSharedSubscriptionCount() {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    size_t count = 0;
    for (const auto &[filter, options] : subscriptions) {
        if (Topic::isShared(filter)) {
            ++count;
        }
    }
    return count;
}

bool Session::unsubscribe(const std::string &topicFilter) {
    bool removed = false;
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        if (subscriptions.erase(topicFilter) > 0) {
            notifyMutation();
            removed = true;
        }
    }
    if (Topic::isShared(topicFilter)) {
        auto [group, realTopicFilter] = Topic::splitShared(topicFilter);
        broker->sharedUnsubscribe(clientId, realTopicFilter, group);
    } else {
        broker->unsubscribe(clientId, topicFilter);
    }
    // The caller reports Success or No Subscription Existed in the MQTT 5
    // UNSUBACK (MQTT-3.11.3), so it needs to know whether one was removed.
    return removed;
}

void Session::bindClient(const std::shared_ptr<Client>& client) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    this->client = client;
    pendingDeliveries.clear();
}

void Session::resume(const std::shared_ptr<Client>& client, Version protocolVersion) {
    std::shared_ptr<Client> previous;
    std::vector<uint16_t> awaitingRel;
    std::vector<uint16_t> outgoingRel;
    {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        previous = std::move(this->client);
        this->client = client;
        // The session is live again: it no longer has a pending expiry and it is
        // connected again (a resumed session must count as an active client).
        hasExpiry = false;
        connected = true;
        // Retransmit every QoS>0 message that was delivered but never
        // acknowledged, keeping the original packet id and setting DUP=1
        // (MQTT-3.3.1-8 / MQTT-4.4.0-1)
        for (const auto& [id, stored] : inflightMessages) {
            Message outgoing = *stored;
            outgoing.dup = true;
            // Echo the original subscription identifiers on the retransmitted
            // PUBLISH (MQTT-3.3.4-3 applies to retransmissions too).
            SubscriptionIdentifiers identifiers;
            auto idIt = inflightIdentifiers.find(id);
            if (idIt != inflightIdentifiers.end()) {
                identifiers = idIt->second;
            }
            pendingDeliveries.emplace_back(std::move(outgoing), id, stored->qos, identifiers);
        }
        // Now that the unacked inflight messages have been re-queued (still
        // occupying their window slots), promote any blocked messages that fit.
        releaseBlocked();
        // Inbound QoS 2: the broker sent PUBREC but never got the matching PUBREL
        // before the connection dropped. Remembering only the packet id is
        // enough (the payload already reached the subscribers), so re-send the
        // PUBREC now and let the client complete the flow with its PUBREL
        // (MQTT-4.3.2-2 / MQTT-4.4.0-1).
        awaitingRel.assign(awaitingPubrel.begin(), awaitingPubrel.end());
        // Outbound QoS 2: the broker sent PUBREL but never got the PUBCOMP
        // before the connection dropped. Re-send the PUBREL now so the client
        // can finish the flow (MQTT-4.4.0-1); the PUBLISH itself was already
        // delivered, so only the PUBREL/PUBCOMP leg is outstanding.
        outgoingRel.assign(pendingPubrel.begin(), pendingPubrel.end());
    }
    if (previous) {
        // Tell the connection that lost the session to release it
        previous->onSuperseded(protocolVersion);
    }
    for (uint16_t id : awaitingRel) {
        client->sendPubrec(id);
    }
    for (uint16_t id : outgoingRel) {
        client->sendPubrel(id);
    }
}

void Session::setDisconnectCallback(std::function<void()> callback) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    onDisconnect = std::move(callback);
}

void Session::deliver(const std::vector<std::string> &filters, const Message &message) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);

    // Enforce read access on the concrete topic, never on the filter: a filter
    // can be broader than the ACL rule that grants it ("#" under
    // "sensors/#"), and checking the filter would deny every such delivery.
    if (auth && auth->isForbidden(username, message.topic, /*write=*/false)) {
        logDebug("session %s: denied read on %s (acl)", clientId.c_str(), message.topic.c_str());
        return;
    }

    // MQTT-3.3.2-9: a message past its expiry must not be delivered to a new
    // subscriber (the fan-out queue is full of live subscribers, so this drops
    // only this one stale copy).
    if (message.isExpired(std::chrono::steady_clock::now())) {
        logDebug("session %s: dropping expired message on %s", clientId.c_str(),
                 message.topic.c_str());
        return;
    }

    // Several of this session's subscriptions can match the same message. The
    // broker delivers one copy for all of them, so the options of every
    // matching subscription have to be combined (MQTT-3.3.4-2/-3/-4/-5):
    //  * the copy goes out at the highest Maximum QoS of the matching
    //    subscriptions, not at the QoS of whichever one was found first;
    //  * it carries the identifier of every subscription that has one, with
    //    repeats kept because two subscriptions may share an identifier;
    //  * No Local suppresses the copy only when every matching subscription
    //    asked for it, since the others still want their own message;
    //  * Retain As Published is honoured if any matching subscription asked
    //    for it.
    QoS maximumQos = QoS::QOS_0;
    bool retainAsPublished = false;
    bool anyRequestedNoLocal = false;
    bool allRequestedNoLocal = true;
    bool matched = false;
    SubscriptionIdentifiers identifiers;
    for (const auto &filter : filters) {
        auto it = subscriptions.find(filter);
        if (it == subscriptions.end()) {
            // Unsubscribed between the match and the delivery: not a
            // subscription of this session any more.
            continue;
        }
        const SubscriptionOptions &options = it->second;
        matched = true;
        if (options.maximumQos > maximumQos) {
            maximumQos = options.maximumQos;
        }
        retainAsPublished = retainAsPublished || options.retainAsPublished;
        if (options.noLocal) {
            anyRequestedNoLocal = true;
        } else {
            allRequestedNoLocal = false;
        }
        if (options.subscriptionIdentifier != 0) {
            identifiers.push_back(options.subscriptionIdentifier);
        }
    }
    if (!matched) {
        return;
    }
    // MQTT-3.8.3-3: a subscription with the No Local option set must not
    // receive messages this same client published. Compare the originator set
    // on the live fan-out path.
    if (allRequestedNoLocal && anyRequestedNoLocal && message.originSession == this) {
        logDebug("session %s: skipping own message on %s (no local)",
                 clientId.c_str(), message.topic.c_str());
        return;
    }

    QoS qos = message.qos < maximumQos ? message.qos : maximumQos;

    Message outgoing = message;
    outgoing.dup = false;
    if (!retainAsPublished) {
        // MQTT-3.3.1-8: when forwarding a live message the retain flag is cleared
        // unless the subscription asked for retain-as-published
        outgoing.retain = false;
    }
    pushDelivery(outgoing, qos, identifiers);
}

void Session::deliverRetained(const std::string &topicFilter, const SubscriptionOptions &options,
                              bool isNewSubscription) {
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
        !isNewSubscription) {
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
        // MQTT-3.3.2-9: a retained message past its expiry is stale; do not
        // deliver it (and the broker purges it from the retained set on sweep).
        if (message.isExpired(std::chrono::steady_clock::now())) {
            logDebug("session %s: skipping expired retained message on %s", clientId.c_str(), topic.c_str());
            continue;
        }
        Message outgoing = message;
        // MQTT-3.3.1-8: messages sent because of a subscription keep the retain flag
        outgoing.retain = true;
        QoS qos = message.qos < options.maximumQos ? message.qos : options.maximumQos;
        // A retained delivery caused by this subscription also carries its
        // identifier (MQTT-3.3.4-3 covers every publish the subscription causes).
        SubscriptionIdentifiers identifiers;
        if (options.subscriptionIdentifier != 0) {
            identifiers.push_back(options.subscriptionIdentifier);
        }
        pushDelivery(outgoing, qos, identifiers);
    }
}

void Session::pushDelivery(const Message &message, QoS qos,
                           const SubscriptionIdentifiers &subscriptionIdentifiers) {
    // Bound the per-session queue: a persistent session that stays offline
    // would otherwise grow without limit. Once the cap is reached new messages
    // are dropped rather than buffered forever. Blocked messages count toward
    // the cap too, so flow control cannot bypass it.
    if (pendingDeliveries.size() + blockedDeliveries.size() >= MAX_QUEUED_MESSAGES) {
        logDebug("session %s: queue full (%zu), dropping message on %s", clientId.c_str(),
                 pendingDeliveries.size() + blockedDeliveries.size(), message.topic.c_str());
        return;
    }
    Message outgoing = message;
    outgoing.qos = qos;
    // MQTT 5 flow control: a QoS 1/2 message may only enter the inflight set
    // while the outstanding count is below the client's Receive Maximum. Beyond
    // that it is parked in blockedDeliveries (no packet id yet) and released as
    // the client acknowledges earlier messages. QoS 0 has no acknowledgement and
    // is therefore never flow-controlled.
    if (qos != QoS::QOS_0 && inflightMessages.size() >= receiveMaximum) {
        logDebug("session %s: receive maximum reached (%u), blocking message on %s",
                 clientId.c_str(), receiveMaximum, message.topic.c_str());
        blockedDeliveries.emplace_back(std::move(outgoing), /*packetId=*/0, qos,
                                        subscriptionIdentifiers);
        return;
    }
    uint16_t id = 0;
    if (qos != QoS::QOS_0) {
        id = nextPacketId();
        inflightMessages[id] = std::make_shared<Message>(outgoing);
        // Remember the subscription identifiers so a later retransmission of this
        // inflight message still echoes them (MQTT-3.3.4-3).
        if (!subscriptionIdentifiers.empty()) {
            inflightIdentifiers[id] = subscriptionIdentifiers;
        }
        notifyMutation();
    }
    // Only queue while the broker lock is held (Broker::publish) or while this
    // session lock is held: flush() is then called outside of both.
    pendingDeliveries.emplace_back(std::move(outgoing), id, qos, subscriptionIdentifiers);
}

void Session::releaseBlocked() {
    // Called with deliveryMutex held after an inflight slot freed up. Promote
    // as many blocked QoS>0 messages as the window now allows: assign a packet
    // id, record it as inflight and enqueue it for sending. Any remaining stay
    // blocked until a later acknowledgement frees another slot.
    while (!blockedDeliveries.empty() &&
           inflightMessages.size() < receiveMaximum &&
           pendingDeliveries.size() < MAX_QUEUED_MESSAGES) {
        Delivery blocked = std::move(blockedDeliveries.front());
        blockedDeliveries.erase(blockedDeliveries.begin());
        blocked.message.qos = blocked.qos;
        uint16_t id = nextPacketId();
        blocked.packetId = id;
        inflightMessages[id] = std::make_shared<Message>(blocked.message);
        if (!blocked.subscriptionIdentifiers.empty()) {
            inflightIdentifiers[id] = blocked.subscriptionIdentifiers;
        }
        notifyMutation();
        pendingDeliveries.emplace_back(std::move(blocked.message), id, blocked.qos,
                                        blocked.subscriptionIdentifiers);
    }
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
        // Skip messages that expired while queued offline: delivering a stale
        // message to a reconnecting client would violate MQTT-3.3.2-9.
        if (delivery.message.isExpired(std::chrono::steady_clock::now())) {
            logDebug("session %s: dropping expired queued message", clientId.c_str());
            // A stale QoS>0 message must not keep its inflight slot; release it
            // so the flow-control window (Receive Maximum) does not leak.
            if (delivery.qos != QoS::QOS_0) {
                releaseInflight(delivery.packetId);
            }
            continue;
        }
        const bool sent = target->deliver(delivery.message, delivery.packetId, delivery.qos,
                                          delivery.subscriptionIdentifiers);
        if (!sent && delivery.qos != QoS::QOS_0) {
            // The client refused the packet (e.g. it exceeds its Maximum Packet
            // Size, MQTT-3.1.2-24). Free the packet id and inflight slot now, or
            // the un-acknowledgeable message would occupy the flow-control window
            // forever and block every later QoS>0 delivery for this client.
            logDebug("session %s: delivery rejected, releasing packetId=%u", clientId.c_str(),
                     delivery.packetId);
            releaseInflight(delivery.packetId);
        }
    }
}

void Session::releaseInflight(uint16_t packetId) {
    std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
    bool freed = false;
    if (inflightMessages.erase(packetId) > 0) {
        freed = true;
    }
    if (inflightIdentifiers.erase(packetId) > 0) {
        freed = true;
    }
    if (freed) {
        notifyMutation();
        releaseBlocked();
    }
}

uint16_t Session::nextPacketId() {
    // MQTT-2.2.1-4: every new QoS>0 PUBLISH must carry a Packet Identifier that
    // is currently unused. The 1..65535 space wraps, so walking the counter
    // blindly would hand out an identifier that is still awaiting its PUBACK /
    // PUBREC / PUBCOMP. Skip those; the inflight set is at most Receive Maximum
    // (65535) entries, so a free identifier always exists for a new message and
    // the scan terminates well inside one turn of the loop.
    for (uint32_t tries = 0; tries < 65535; ++tries) {
        if (packetId == 0) {
            packetId = 1;
        }
        const uint16_t candidate = packetId++;
        if (inflightMessages.count(candidate) == 0 &&
            pendingPubrel.count(candidate) == 0) {
            return candidate;
        }
    }
    // Unreachable in practice: it would need all 65535 identifiers to be
    // outstanding, which the flow-control window already forbids.
    return packetId == 0 ? (packetId = 1) : packetId++;
}

} // namespace MQTT
