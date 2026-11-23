#include "Broker.h"
#include "Topic.h"
#include "Session.h"
#include "SysTopics.h"
#include "Log.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

namespace MQTT {

// Process-wide counters behind the $SYS/broker/messages/* topics. They live at
// namespace scope (not as Broker members) so the free helper functions in this
// file can read them without a back-reference to a Broker instance. Relaxed
// atomics: they are informational snapshots only, never used for correctness.
std::atomic<uint64_t> totalMessagesSent{0};
std::atomic<uint64_t> totalMessagesReceived{0};
std::atomic<uint64_t> totalMessagesDropped{0};
// Live outbound write-buffer occupancy across all connections (bytes), and the
// number of currently connected clients. These are gauges, not cumulative
// counters: they rise and fall as connections buffer and drain, or connect and
// disconnect. They back the real-time $SYS/broker/writebuffer/bytes and
// $SYS/broker/clients/connected topics, so an operator can see backpressure
// building BEFORE messages are dropped (messages/dropped stays 0 until a loss
// has already happened).
std::atomic<uint64_t> liveWriteBufferBytes{0};
//! Cumulative disconnect counters split by why the connection ended, so an
//! operator can see not just that clients leave but HOW: keep-alive timeout,
//! a protocol/malformed violation, a quota/packet-size refusal, a graceful
//! disconnect, or anything else. These back the $SYS/broker/disconnect/* topics.
//! Monotonic counters (they never decrease), incremented once per connection at
//! teardown from the reason the connection recorded for itself.
std::atomic<uint64_t> disconnectsKeepAlive{0};
std::atomic<uint64_t> disconnectsProtocol{0};
std::atomic<uint64_t> disconnectsQuota{0};
std::atomic<uint64_t> disconnectsAuth{0};
std::atomic<uint64_t> disconnectsNormal{0};
std::atomic<uint64_t> disconnectsOther{0};
//! Live in-memory occupancy of every session's undelivered messages (payload +
//! topic bytes in pendingDeliveries + blockedDeliveries + inflightMessages). A
//! gauge, not a counter: it rises as fan-out queues messages for slow consumers
//! and falls as they are flushed, dropped or acknowledged. It backs
//! $SYS/broker/queued/bytes, the in-process stage of the backpressure signal
//! that writebuffer/bytes (the socket buffer) and messages/dropped (post-loss)
//! cannot see: under the default DropNewest policy a message can be discarded
//! before it ever reaches the socket, so this gauge is the only one that shows
//! the pressure building up beforehand.
std::atomic<uint64_t> liveQueuedBytes{0};
std::atomic<uint64_t> liveConnectedClients{0};
std::chrono::steady_clock::time_point sysStartedAt{std::chrono::steady_clock::now()};

// Free helpers backing the $SYS/broker/messages/* and uptime/load topics. They
// read only process state, so no broker lock is involved; the broker forwards
// its own locked counters into the parts that need them.
size_t sysGetTotalSent();
size_t sysGetTotalReceived();
double sysUptimeSeconds();
size_t sysGetRetainedCount();
size_t sysGetWriteBufferBytes();
size_t sysGetConnectedClients();
//! Cumulative disconnect counters by category (keep-alive / protocol / quota /
//! normal / other), backing the $SYS/broker/disconnect/* topics.
size_t sysGetDisconnectsKeepAlive();
size_t sysGetDisconnectsProtocol();
size_t sysGetDisconnectsQuota();
size_t sysGetDisconnectsAuth();
size_t sysGetDisconnectsNormal();
size_t sysGetDisconnectsOther();

Broker::Broker(Auth* auth, Store* store) : trie(std::make_unique<Trie>()), auth(auth), store(store) {}

PersistedState Broker::snapshotState() const {
    PersistedState state;
    // Collect the persistent sessions under the broker lock, then snapshot each
    // one. The lock order is Broker -> Session, matching every other path.
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        // The $SYS/broker/... monitoring tree is regenerated live by
        // refreshSysTopics() on every sweep and is never meaningful to persist:
        // its values (uptime, live counts) describe the current process only.
        // Persisting it would (a) write a stale snapshot of the tree into every
        // state file, and (b) re-serve it on the next start even when the
        // monitoring feature is disabled. Exclude it from the durable state.
        for (const auto &[topic, message] : retainedMessages) {
            if (!SysTopics::isSysTopic(topic)) {
                state.retained[topic] = message;
            }
        }
        for (const auto &[clientId, session] : sessions) {
            if (session && session->isPersistent()) {
                state.sessions.push_back(session->snapshot());
            }
        }
    }
    return state;
}

void Broker::restoreState(const PersistedState &state) {
    size_t keptRetained = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        // The $SYS/broker/... monitoring tree is a live, process-local view and
        // is never restored from disk: it is repopulated by refreshSysTopics()
        // only while the feature is enabled. Drop any $SYS entries a state file
        // written by an older build may still carry, so a $SYS-disabled broker
        // never serves a stale monitoring tree it no longer generates.
        for (const auto &[topic, message] : state.retained) {
            if (!SysTopics::isSysTopic(topic)) {
                retainedMessages[topic] = message;
                ++keptRetained;
            }
        }
        for (const auto &persisted : state.sessions) {
            auto session = std::make_shared<Session>(this, persisted.clientId, /*cleanStart=*/false);
            session->restore(persisted);
            session->setMutateCallback([this]() { notifyStateChanged(); });
            session->setAuth(auth);
            session->setMaxSubscriptions(maxSubscriptions);
            session->setMaxSharedSubscriptions(
                sharedQuotaExplicit ? maxSharedSubscriptions : maxSubscriptions);
            session->setQueueDropPolicy(queueDropPolicy);
            sessions[persisted.clientId] = session;
            // Re-create the routing entries so live publishes reach the session
            // while it is offline (they are buffered until it reconnects)
            for (const auto &entry : persisted.subscriptions) {
                const std::string &filter = entry.first;
                // MQTT-3.8.3-1 / §4.7: a restored filter never went through the
                // SUBSCRIBE validation that rejects a misplaced wildcard, and a
                // hand-written or damaged state file can therefore hold one that
                // is not a legal Topic Filter at all. Inserting it puts an
                // unreachable node in the routing trie that makes every publish
                // through it fail, so drop it here and say so instead.
                if (!Topic::isValid(filter)) {
                    logError("state file: dropping session %s subscription with an invalid "
                             "topic filter: %s",
                             persisted.clientId.c_str(), filter.c_str());
                    continue;
                }
                if (Topic::isShared(filter)) {
                    auto [group, realFilter] = Topic::splitShared(filter);
                    sharedSubscriptions[realFilter][group].push_back(
                        SharedSubscription(persisted.clientId, realFilter, group));
                    // The routing table is keyed by the real filter: a publish
                    // carries a topic name, never a "$share/..." string, so the
                    // group would never match if the prefix were kept.
                    trie->insert(realFilter);
                } else {
                    subscriptions[filter].insert(persisted.clientId);
                    trie->insert(filter);
                }
            }
        }
    }
    logInfo("restored %zu retained message(s) and %zu persistent session(s) from disk",
            keptRetained, state.sessions.size());
}

void Broker::insertSession(const std::string &clientId, const std::shared_ptr<Session>& session) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    sessions[clientId] = session;
    if (session) {
        // Every durable mutation of this session marks the store dirty
        session->setMutateCallback([this]() { notifyStateChanged(); });
        // Apply the broker's subscription limits to the newly created session
        session->setMaxSubscriptions(maxSubscriptions);
        session->setMaxSharedSubscriptions(
            sharedQuotaExplicit ? maxSharedSubscriptions : maxSubscriptions);
        // Apply the broker's subscription limits to the newly created session
        session->setQueueDropPolicy(queueDropPolicy);
    }
}

std::shared_ptr<Session> Broker::findSession(const std::string &clientId) const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it = sessions.find(clientId);
    if (it != sessions.end()) {
        return it->second;
    }
    return nullptr;
}

void Broker::forgetSession(Session* session) {
    if (session == nullptr) {
        return;
    }
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it = sessions.find(session->getClientId());
    if (it != sessions.end() && it->second.get() == session) {
        sessions.erase(it);
    }
}

void Broker::removeSession(const std::string &clientId) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    sessions.erase(clientId);
}

void Broker::sweepExpiredSessions() {
    auto now = std::chrono::steady_clock::now();
    std::vector<std::shared_ptr<Session>> victims;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        // Drop retained messages past their MQTT 5 expiry so they are not
        // re-delivered to future subscribers and do not linger in the store.
        for (auto it = retainedMessages.begin(); it != retainedMessages.end();) {
            if (it->second.isExpired(now)) {
                it = retainedMessages.erase(it);
                notifyStateChanged();
            } else {
                ++it;
            }
        }
        for (auto it = sessions.begin(); it != sessions.end();) {
            auto &session = it->second;
            if (session && !session->isConnected() && session->isExpired(now)) {
                victims.push_back(session);
                // MQTT-3.1.2-8 / MQTT-3.1.3-9: the Session ends when its state
                // is destroyed, so the Will goes out now even if the Will Delay
                // Interval has not elapsed. Cap the pending Will's deadline to
                // the present so publishDueWills() (called below) releases it
                // instead of leaving it parked for the rest of the delay.
                auto willIt = pendingWills.find(it->first);
                if (willIt != pendingWills.end()) {
                    willIt->second.deadline = now;
                    notifyStateChanged();
                }
                it = sessions.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Publish any delayed Wills whose Will Delay Interval has elapsed (or whose
    // session was just discarded above). Done outside the lock (publish() takes
    // it) and independently of the session sweep, so a delayed Will fires even
    // for a non-persistent session that was torn down at disconnect.
    publishDueWills();
    // Refresh the $SYS/broker monitoring messages with the current counters.
    // A no-op unless FLOWMQ_SYS_TOPICS is set.
    refreshSysTopics();
    // Discard outside the broker lock: discard() takes the broker lock itself
    // (to remove subscriptions) and would deadlock otherwise.
    for (auto &session : victims) {
        session->discard();
    }
}

void Broker::armWill(const std::string &clientId, const Message &will, QoS qos,
                     bool retain, uint32_t delaySeconds, bool hasExpiry,
                     uint32_t expiryInterval) {
    if (delaySeconds == 0) {
        return;
    }
    std::lock_guard<std::recursive_mutex> lock(mutex);
    PendingWill pending;
    pending.message = will;
    pending.qos = qos;
    pending.retain = retain;
    pending.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(delaySeconds);
    pending.hasExpiry = hasExpiry;
    pending.expiryInterval = expiryInterval;
    // Capture the username so the write ACL still applies when the Will is
    // published, even if the session has since been torn down.
    if (auto session = findSession(clientId)) {
        pending.username = session->getUsername();
    }
    pendingWills[clientId] = std::move(pending);
    notifyStateChanged();
}

void Broker::cancelWill(const std::string &clientId) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    if (pendingWills.erase(clientId) > 0) {
        notifyStateChanged();
    }
}

void Broker::publishDueWills() {
    auto now = std::chrono::steady_clock::now();
    std::vector<PendingWill> due;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        for (auto it = pendingWills.begin(); it != pendingWills.end();) {
            if (it->second.deadline <= now) {
                due.push_back(std::move(it->second));
                it = pendingWills.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Publish outside the broker lock (publish() takes it itself).
    for (auto &pending : due) {
        // MQTT-3.3.2-9: the lifetime of a Will counts from its publication, so
        // it is stamped here and not when the Will was armed. The Will Delay has
        // therefore already elapsed and does not shorten the Message Expiry
        // Interval; an interval of 0 publishes a message that is already stale.
        if (pending.hasExpiry) {
            pending.message.hasExpiry = true;
            pending.message.expiryDeadline = now + std::chrono::seconds(pending.expiryInterval);
        }
        // The Will is published as the originating client, so the same write ACL
        // applies as for a regular PUBLISH.
        if (auth && auth->isForbidden(pending.username, pending.message.topic,
                                      /*write=*/true)) {
            logDebug("broker: denied delayed will on %s (acl)", pending.message.topic.c_str());
            continue;
        }
        publish(pending.message);
    }
}

void Broker::removeSession(Session* session) {
    forgetSession(session);
}

std::map<std::string, Message> Broker::snapshotRetained() const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    return retainedMessages;
}

size_t Broker::getRetainedCount() const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    return retainedMessages.size();
}

int Broker::getConnectedClients() const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    int connected = 0;
    for (const auto &[clientId, session] : sessions) {
        if (session && session->isConnected()) {
            connected++;
        }
    }
    return connected;
}

bool Broker::isSubscribed(const std::string &clientId, const std::string &topicFilter) const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it = subscriptions.find(topicFilter);
    if (it != subscriptions.end()) {
        return it->second.find(clientId) != it->second.end();
    }
    return false;
}

std::set<std::string> Broker::getSubscriptions(const std::string &topicFilter) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    std::set<std::string> clientIds;
    auto it = subscriptions.find(topicFilter);
    if (it != subscriptions.end()) {
        clientIds = it->second;
    }
    return clientIds;
}

void Broker::subscribe(const std::string &clientId, const std::string &topicFilter) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    if (subscriptions.find(topicFilter) == subscriptions.end()) {
        trie->insert(topicFilter);
    }
    subscriptions[topicFilter].insert(clientId);
}

void Broker::unsubscribe(const std::string &clientId, const std::string &topicFilter) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it = subscriptions.find(topicFilter);
    if (it == subscriptions.end()) {
        return;
    }

    it->second.erase(clientId);

    if (it->second.empty()) {
        // §4.8.2: a Shared Subscription lasts as long as it is associated with
        // at least one Session, and "survives when the Session that originally
        // created it unsubscribes, unless there are no other Sessions left".
        // Plain and shared subscriptions on the same Topic Filter share one
        // trie entry, so dropping the last plain subscriber must not take the
        // shared group's routing entry with it - exactly the guard
        // sharedUnsubscribe() already applies in the other direction. Without
        // it the group stayed registered in sharedSubscriptions but was no
        // longer reachable from the trie, so it silently stopped receiving
        // every later message until the broker restarted.
        if (sharedSubscriptions.find(topicFilter) == sharedSubscriptions.end()) {
            trie->remove(topicFilter);
        }
        subscriptions.erase(it);
    }
}

void Broker::sharedSubscribe(const std::string &clientId, const std::string &topicFilter, const std::string &group) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    SharedSubscription sharedSubscription(clientId, topicFilter, group);
    auto &members = sharedSubscriptions[topicFilter][group];
    if (std::find(members.begin(), members.end(), sharedSubscription) == members.end()) {
        members.push_back(sharedSubscription);
    }
    trie->insert(topicFilter);
}

void Broker::sharedUnsubscribe(const std::string &clientId, const std::string &topicFilter, const std::string &group) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    SharedSubscription sharedSubscription(clientId, topicFilter, group);
    auto filterIt = sharedSubscriptions.find(topicFilter);
    if (filterIt == sharedSubscriptions.end()) {
        return;
    }
    auto groupIt = filterIt->second.find(group);
    if (groupIt == filterIt->second.end()) {
        return;
    }
    auto &members = groupIt->second;
    auto position = std::find(members.begin(), members.end(), sharedSubscription);
    if (position != members.end()) {
        members.erase(position);
    }
    if (members.empty()) {
        filterIt->second.erase(groupIt);
        sharedRoundRobin.erase({topicFilter, group});
    }
    if (filterIt->second.empty()) {
        sharedSubscriptions.erase(filterIt);
        // Plain subscriptions live in the trie under the same filter string, so
        // only drop the entry when no ordinary subscription still needs it.
        if (subscriptions.find(topicFilter) == subscriptions.end()) {
            trie->remove(topicFilter);
        }
    }
}

std::shared_ptr<Session> Broker::nextSharedMember(const std::string &topicFilter,
                                                  const std::string &group,
                                                  std::vector<SharedSubscription> &members) {
    if (members.empty()) {
        return nullptr;
    }
    auto &cursor = sharedRoundRobin[{topicFilter, group}];
    // Walk at most one full lap: members may name sessions that have since been
    // torn down, and those must not consume a turn.
    for (size_t attempt = 0; attempt < members.size(); ++attempt) {
        const size_t index = cursor % members.size();
        cursor = (index + 1) % members.size();
        auto sessionIt = sessions.find(members[index].clientId);
        if (sessionIt != sessions.end() && sessionIt->second) {
            return sessionIt->second;
        }
    }
    return nullptr;
}

void Broker::publish(const Message &message) {
    // MQTT-4.1.2-2: while the $SYS monitoring tree is enabled only the broker
    // writes it. A client PUBLISH to a $SYS topic is refused in
    // Connection::handlePublish, but a Will that the broker publishes on the
    // client's behalf (Connection::publishWill and the delayed path in
    // publishDueWills, plus an MQTT-SN Will) reaches this method directly and
    // used to bypass that rule, letting any client inject or forge monitoring
    // topics. Enforce it at the one choke point every brokered publish passes
    // through. refreshSysTopics() does not call this method - it writes the
    // tree through dispatchTargets() directly - so the real monitoring tree is
    // unaffected.
    if (sysTopicsEnabled && SysTopics::isSysTopic(message.topic)) {
        logDebug("broker: refused publish to $SYS topic %s", message.topic.c_str());
        return;
    }
    std::vector<std::pair<std::shared_ptr<Session>, std::string>> targets;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        collectTargets(message, targets);
    }
    const size_t delivered = dispatchTargets(targets, message);
    // Count what actually reached a subscriber: one increment per delivery the
    // broker handed to a session (live fan-out or retained). This backs the
    // $SYS/broker/messages/sent topic.
    incrementMessagesSent(static_cast<uint64_t>(delivered));
}

size_t Broker::dispatchTargets(
    std::vector<std::pair<std::shared_ptr<Session>, std::string>> &targets,
    const Message &message) {
    // A session reachable through several ordinary subscriptions (e.g. "a/#"
    // and "a/b") receives a single copy, while every share group is its own
    // subscription and keeps its own copy. Key the dedup on the session for
    // ordinary deliveries and on the group's filter for shared ones.
    // The matched filters of an ordinary session are kept together so
    // Session::deliver() can combine their options as MQTT-3.3.4-2 requires:
    // keeping only the first one would deliver at the wrong QoS, drop the other
    // Subscription Identifiers, and misjudge No Local.
    struct Dispatch {
        std::shared_ptr<Session> session;
        std::vector<std::string> filters;
    };
    std::vector<Dispatch> unique;
    std::map<Session *, size_t> ordinaryIndex;
    // A shared subscription is identified by its full "$share/group/filter"
    // name; an ordinary filter (even a "$SYS/..." one) shares one identity per
    // session.
    for (auto &target : targets) {
        if (Topic::isShared(target.second)) {
            unique.push_back(Dispatch{std::move(target.first), {std::move(target.second)}});
            continue;
        }
        auto inserted = ordinaryIndex.emplace(target.first.get(), unique.size());
        if (inserted.second) {
            unique.push_back(Dispatch{std::move(target.first), {}});
        }
        unique[inserted.first->second].filters.push_back(std::move(target.second));
    }
    // Deliveries are queued on the sessions here; the sockets are only written
    // by Session::flush() so that no client write is done while a lock is held
    for (const auto &dispatch : unique) {
        dispatch.session->deliver(dispatch.filters, message);
    }
    std::set<Session *> flushed;
    for (const auto &dispatch : unique) {
        if (flushed.insert(dispatch.session.get()).second) {
            dispatch.session->flush();
        }
    }
    return unique.size();
}

void Broker::collectTargets(
    const Message &message,
    std::vector<std::pair<std::shared_ptr<Session>, std::string>> &targets) {
    if (message.retain) {
        if (message.payload.empty()) {
            // An empty retained payload removes the retained message for that topic
            retainedMessages.erase(message.topic);
        } else if (message.isExpired(std::chrono::steady_clock::now())) {
            // MQTT-3.3.2-9: a Message Expiry Interval of 0 (or one already elapsed)
            // means the message expires immediately, so it must never be
            // delivered. A retained publish with that property REPLACES any
            // existing retained message on the topic (MQTT-3.3.1-2) with
            // nothing, so erase rather than store a message no subscriber may
            // receive.
            logDebug("broker: not retaining %s, message already expired", message.topic.c_str());
            retainedMessages.erase(message.topic);
        } else if (retainedMessages.find(message.topic) == retainedMessages.end() &&
                   maxRetained != 0 && retainedMessages.size() >= maxRetained) {
            // Bounded so a client cannot pin broker memory by retaining an
            // unbounded set of distinct topics. A new topic over the cap is not
            // stored (the live fan-out above is unaffected); replacing an
            // existing topic always falls through to the assignment below.
            logDebug("broker: retained set full (%zu), not retaining %s", maxRetained,
                     message.topic.c_str());
        } else {
            retainedMessages[message.topic] = message;
        }
        // The retained set is durable state
        notifyStateChanged();
    }

    std::vector<std::string> topicFilters = trie->match(message.topic);
    for (const auto &topicFilter : topicFilters) {
        auto it = subscriptions.find(topicFilter);
        if (it != subscriptions.end()) {
            for (const auto &clientId : it->second) {
                auto sessionIt = sessions.find(clientId);
                if (sessionIt != sessions.end() && sessionIt->second) {
                    targets.emplace_back(sessionIt->second, topicFilter);
                }
            }
        }
        auto sharedIt = sharedSubscriptions.find(topicFilter);
        if (sharedIt != sharedSubscriptions.end()) {
            // Every share group on this filter receives one copy; the members
            // of a group take turns (MQTT-4.8.2: a shared subscription behaves
            // like a single ordinary subscription per group).
            for (auto &[group, members] : sharedIt->second) {
                auto session = nextSharedMember(topicFilter, group, members);
                if (session) {
                    // The full "$share/group/filter" name is what the session
                    // stores, so it is what carries that subscription's options.
                    targets.emplace_back(std::move(session),
                                         "$share/" + group + "/" + topicFilter);
                }
            }
        }
    }
}
void Broker::setSysTopics(bool enabled) {
    sysTopicsEnabled = enabled;
    sysStartedAt = std::chrono::steady_clock::now();
}

void Broker::incrementMessagesReceived() {
    ++totalMessagesReceived;
}

void Broker::incrementMessagesSent(uint64_t count) {
    totalMessagesSent += count;
}

void Broker::incrementMessagesDropped(uint64_t count) {
    totalMessagesDropped += count;
}

size_t sysGetTotalSent() {
    return totalMessagesSent.load(std::memory_order_relaxed);
}

size_t sysGetTotalReceived() {
    return totalMessagesReceived.load(std::memory_order_relaxed);
}

size_t sysGetTotalDropped() {
    return totalMessagesDropped.load(std::memory_order_relaxed);
}

size_t sysGetWriteBufferBytes() {
    return liveWriteBufferBytes.load(std::memory_order_relaxed);
}

size_t sysGetQueuedBytes() {
    return liveQueuedBytes.load(std::memory_order_relaxed);
}

size_t sysGetConnectedClients() {
    return liveConnectedClients.load(std::memory_order_relaxed);
}

size_t sysGetDisconnectsKeepAlive() {
    return disconnectsKeepAlive.load(std::memory_order_relaxed);
}

size_t sysGetDisconnectsProtocol() {
    return disconnectsProtocol.load(std::memory_order_relaxed);
}

size_t sysGetDisconnectsQuota() {
    return disconnectsQuota.load(std::memory_order_relaxed);
}

size_t sysGetDisconnectsAuth() {
    return disconnectsAuth.load(std::memory_order_relaxed);
}

size_t sysGetDisconnectsNormal() {
    return disconnectsNormal.load(std::memory_order_relaxed);
}

size_t sysGetDisconnectsOther() {
    return disconnectsOther.load(std::memory_order_relaxed);
}

void Broker::adjustWriteBufferBytes(int64_t delta) {
    liveWriteBufferBytes.fetch_add(delta, std::memory_order_relaxed);
}

void Broker::adjustQueuedBytes(int64_t delta) {
    liveQueuedBytes.fetch_add(delta, std::memory_order_relaxed);
}

void Broker::adjustConnectedClients(int64_t delta) {
    liveConnectedClients.fetch_add(delta, std::memory_order_relaxed);
}

void Broker::recordDisconnect(ReasonCode reason) {
    // Classify the reason a connection ended into one of the $SYS/broker/disconnect/*
    // categories. The mapping is deliberately coarse: an operator needs to tell a
    // client that keeps the link open too long (keep-alive) from one the broker is
    // actively refusing (a protocol/malformed violation, a rate or connection
    // quota) apart from a clean leave (normal). The individual reason codes within
    // the protocol and quota groups all call for the same operator response, so
    // they are folded together. Purely informational; no correctness path reads
    // these.
    switch (reason) {
        case ReasonCode::KEEP_ALIVE_TIMEOUT:
            disconnectsKeepAlive.fetch_add(1, std::memory_order_relaxed);
            break;
        case ReasonCode::MALFORMED_PACKET:
        case ReasonCode::PROTOCOL_ERROR:
        case ReasonCode::UNSUPPORTED_PROTOCOL_VERSION:
        case ReasonCode::CLIENT_IDENTIFIER_NOT_VALID:
        case ReasonCode::TOPIC_FILTER_INVALID:
        case ReasonCode::TOPIC_NAME_INVALID:
        case ReasonCode::PACKET_IDENTIFIER_IN_USE:
        case ReasonCode::PACKET_IDENTIFIER_NOT_FOUND:
        case ReasonCode::RECEIVE_MAXIMUM_EXCEEDED:
        case ReasonCode::TOPIC_ALIAS_INVALID:
        case ReasonCode::PACKET_TOO_LARGE:
            disconnectsProtocol.fetch_add(1, std::memory_order_relaxed);
            break;
        case ReasonCode::QUOTA_EXCEEDED:
        case ReasonCode::MESSAGE_RATE_TOO_HIGH:
        case ReasonCode::CONNECTION_RATE_EXCEEDED:
            disconnectsQuota.fetch_add(1, std::memory_order_relaxed);
            break;
        case ReasonCode::BAD_USERNAME_OR_PASSWORD:
        case ReasonCode::NOT_AUTHORIZED:
        case ReasonCode::BAD_AUTHENTICATION_METHOD:
            disconnectsAuth.fetch_add(1, std::memory_order_relaxed);
            break;
        case ReasonCode::NORMAL_DISCONNECTION:
            disconnectsNormal.fetch_add(1, std::memory_order_relaxed);
            break;
        default:
            disconnectsOther.fetch_add(1, std::memory_order_relaxed);
            break;
    }
}

double sysUptimeSeconds() {
    const auto elapsed = std::chrono::steady_clock::now() - sysStartedAt;
    return std::chrono::duration_cast<std::chrono::duration<double>>(elapsed).count();
}

std::map<std::string, std::string> Broker::sysTopicValues() const {
    std::map<std::string, std::string> values;
    if (!sysTopicsEnabled) {
        return values;
    }
    // Gather every broker-level count under the broker lock, then build each
    // topic's payload from the snapshot outside the (much longer) publish path.
    SysTopics::BrokerCounts counts;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        for (const auto &[clientId, session] : sessions) {
            if (!session) {
                continue;
            }
            if (session->isConnected()) {
                ++counts.connected;
            }
            // QoS 1/2 messages the broker has sent but the client has not yet
            // acknowledged, aggregated across every session. Each occupies a
            // Receive Maximum slot (MQTT-4.9), so this is the broker's live
            // flow-control occupancy: it rises when clients are slow to ack and
            // is the count counterpart to queued/bytes (which also covers the
            // not-yet-sent queue and QoS 0). Safely read under the broker lock:
            // Broker -> Session is the documented lock order, and no Session
            // method calls back into the Broker while holding its own lock.
            counts.inflight += session->getInflightCount();
        }
        counts.sessions = sessions.size();
        // Total subscription instances the broker routes: every (client, filter)
        // normal subscription in `subscriptions`, plus every shared-subscription
        // member (which lives only in `sharedSubscriptions`, not `subscriptions`).
        // This is the fan-out workload and routing-table pressure in one number,
        // the capacity-planning complement to clients/total (how many sessions)
        // and the backpressure gauges (how hard each one is pushed).
        for (const auto &[filter, clients] : subscriptions) {
            counts.subscriptions += clients.size();
        }
        for (const auto &[filter, groups] : sharedSubscriptions) {
            for (const auto &[group, members] : groups) {
                counts.subscriptions += members.size();
            }
        }
        // $SYS/broker/retaineds/count reports the retained messages clients have
        // published, not the broker's own monitoring tree: counting the $SYS
        // topics here would fold the tree into its own metric (and grow it every
        // time a topic were added), so the monitoring topics are excluded.
        for (const auto &[topic, message] : retainedMessages) {
            if (!SysTopics::isSysTopic(topic)) {
                ++counts.retained;
            }
        }
    }
    for (const std::string &topic : SysTopics::allTopics()) {
        const std::string value = SysTopics::payload(topic, counts);
        if (!value.empty()) {
            values[topic] = value;
        }
    }
    return values;
}

void Broker::refreshSysTopics() {
    // The counts move on their own, so each sweep writes the current value as a
    // retained message (so a client that subscribes later sees live values, via
    // the "send on subscribe" retained path) AND fans the new value out to the
    // clients already subscribed. Without the live fan-out a monitoring client
    // would receive one snapshot at subscribe time and never see the gauge move
    // again, which makes the whole tree useless for watching backpressure or
    // in-flight pressure build.
    //
    // $SYS values are informational and refreshed every sweep, so a topic whose
    // value did not change is skipped entirely: no retained rewrite, no fan-out,
    // and no notification to the persistence layer. That keeps a steady-state
    // broker from rewriting its state file (uptime alone changes every second)
    // and from chattering the tree to subscribers when nothing moved.
    if (!sysTopicsEnabled) {
        return;
    }
    // Each changed topic carries its own payload, so resolve its subscribers as a
    // separate batch: dispatchTargets() fans one message out to one target list.
    struct Update {
        Message message;
        std::vector<std::pair<std::shared_ptr<Session>, std::string>> targets;
    };
    std::vector<Update> updates;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        for (const auto &[topic, value] : sysTopicValues()) {
            auto it = retainedMessages.find(topic);
            if (it != retainedMessages.end() &&
                it->second.payload == std::vector<uint8_t>(value.begin(), value.end())) {
                continue;
            }
            Update update;
            update.message = Message(topic, value, QoS::QOS_0, /*retain=*/true);
            retainedMessages[topic] = update.message;
            // A $SYS refresh never marks the store dirty: the monitoring tree is
            // excluded from the durable state (see snapshotState), so persisting
            // it would rewrite the state file every sweep (uptime alone changes
            // every second) with data that is discarded on the next start.
            //
            // Resolve the topic's subscribers directly rather than routing the
            // message through collectTargets(): that would re-store it as
            // retained (already done) and, worse, let the tree's own delivery be
            // counted in $SYS/broker/messages/sent, a self-referential metric.
            // The (session, filter) pairs are gathered now and dispatched outside
            // the lock, exactly like ordinary live fan-out.
            for (const auto &filter : trie->match(topic)) {
                auto subs = subscriptions.find(filter);
                if (subs != subscriptions.end()) {
                    for (const auto &clientId : subs->second) {
                        auto sessionIt = sessions.find(clientId);
                        if (sessionIt != sessions.end() && sessionIt->second) {
                            update.targets.emplace_back(sessionIt->second, filter);
                        }
                    }
                }
                // A $SYS topic is never shared, but resolve shared groups too so
                // a "$share/g/$SYS/#" subscription keeps working like any other.
                auto shared = sharedSubscriptions.find(filter);
                if (shared != sharedSubscriptions.end()) {
                    for (auto &[group, members] : shared->second) {
                        auto session = nextSharedMember(filter, group, members);
                        if (session) {
                            update.targets.emplace_back(std::move(session),
                                                        "$share/" + group + "/" + filter);
                        }
                    }
                }
            }
            updates.push_back(std::move(update));
        }
    }
    // Fan out outside the lock: ordinary delivery queues on each session and
    // flushes it, so a $SYS subscriber sees every value change rather than one
    // stale snapshot at subscribe time. A topic nobody subscribes to costs
    // nothing beyond the retained write above.
    for (auto &update : updates) {
        if (!update.targets.empty()) {
            dispatchTargets(update.targets, update.message);
        }
    }
}

} // namespace MQTT
