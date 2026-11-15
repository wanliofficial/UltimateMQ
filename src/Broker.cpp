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
std::chrono::steady_clock::time_point sysStartedAt{std::chrono::steady_clock::now()};

// Free helpers backing the $SYS/broker/messages/* and uptime/load topics. They
// read only process state, so no broker lock is involved; the broker forwards
// its own locked counters into the parts that need them.
size_t sysGetTotalSent();
size_t sysGetTotalReceived();
double sysUptimeSeconds();
size_t sysGetRetainedCount();

Broker::Broker(Auth* auth, Store* store) : trie(std::make_unique<Trie>()), auth(auth), store(store) {}

PersistedState Broker::snapshotState() const {
    PersistedState state;
    // Collect the persistent sessions under the broker lock, then snapshot each
    // one. The lock order is Broker -> Session, matching every other path.
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        state.retained = retainedMessages;
        for (const auto &[clientId, session] : sessions) {
            if (session && session->isPersistent()) {
                state.sessions.push_back(session->snapshot());
            }
        }
    }
    return state;
}

void Broker::restoreState(const PersistedState &state) {
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        retainedMessages = state.retained;
        for (const auto &persisted : state.sessions) {
            auto session = std::make_shared<Session>(this, persisted.clientId, /*cleanStart=*/false);
            session->restore(persisted);
            session->setMutateCallback([this]() { notifyStateChanged(); });
            session->setAuth(auth);
            session->setMaxSubscriptions(maxSubscriptions);
            session->setMaxSharedSubscriptions(
                sharedQuotaExplicit ? maxSharedSubscriptions : maxSubscriptions);
            sessions[persisted.clientId] = session;
            // Re-create the routing entries so live publishes reach the session
            // while it is offline (they are buffered until it reconnects)
            for (const auto &entry : persisted.subscriptions) {
                const std::string &filter = entry.first;
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
            state.retained.size(), state.sessions.size());
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
                it = sessions.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Publish any delayed Wills whose Will Delay Interval has elapsed. Done
    // outside the lock (publish() takes it) and independently of the session
    // sweep, so a delayed Will fires even for a non-persistent session that was
    // torn down at disconnect.
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
                     bool retain, uint32_t delaySeconds) {
    if (delaySeconds == 0) {
        return;
    }
    std::lock_guard<std::recursive_mutex> lock(mutex);
    PendingWill pending;
    pending.message = will;
    pending.qos = qos;
    pending.retain = retain;
    pending.deadline = std::chrono::steady_clock::now() + std::chrono::seconds(delaySeconds);
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
        trie->remove(topicFilter);
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
    std::vector<std::pair<std::shared_ptr<Session>, std::string>> targets;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        collectTargets(message, targets);
    }
    // A session reachable through several ordinary subscriptions (e.g. "a/#"
    // and "a/b") receives a single copy, while every share group is its own
    // subscription and keeps its own copy. Key the dedup on the session for
    // ordinary deliveries and on the group's filter for shared ones.
    std::vector<std::pair<std::shared_ptr<Session>, std::string>> unique;
    std::set<std::pair<Session *, std::string>> seen;
    unique.reserve(targets.size());
    for (auto &target : targets) {
        // A shared subscription is identified by its full "$share/group/filter"
        // name; an ordinary filter (even a "$SYS/..." one) shares one identity
        // per session.
        const bool shared = Topic::isShared(target.second);
        std::pair<Session *, std::string> key{target.first.get(),
                                              shared ? target.second : std::string()};
        if (seen.insert(key).second) {
            unique.push_back(std::move(target));
        }
    }
    // Deliveries are queued on the sessions here; the sockets are only written
    // by Session::flush() so that no client write is done while a lock is held
    for (const auto &[session, filter] : unique) {
        session->deliver(filter, message);
    }
    std::set<Session *> flushed;
    for (const auto &[session, filter] : unique) {
        if (flushed.insert(session.get()).second) {
            session->flush();
        }
    }
    // Count what actually reached a subscriber: one increment per delivery the
    // broker handed to a session (live fan-out or retained). This backs the
    // $SYS/broker/messages/sent topic.
    incrementMessagesSent(static_cast<uint64_t>(unique.size()));
}

void Broker::collectTargets(
    const Message &message,
    std::vector<std::pair<std::shared_ptr<Session>, std::string>> &targets) {
    if (message.retain) {
        if (message.payload.empty()) {
            // An empty retained payload removes the retained message for that topic
            retainedMessages.erase(message.topic);
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

size_t sysGetTotalSent() {
    return totalMessagesSent.load(std::memory_order_relaxed);
}

size_t sysGetTotalReceived() {
    return totalMessagesReceived.load(std::memory_order_relaxed);
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
    size_t connected = 0;
    size_t sessionCount = 0;
    size_t retainedCount = 0;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        for (const auto &[clientId, session] : sessions) {
            if (session && session->isConnected()) {
                ++connected;
            }
        }
        sessionCount = sessions.size();
        retainedCount = retainedMessages.size();
    }
    for (const std::string &topic : SysTopics::allTopics()) {
        const std::string value = SysTopics::payload(topic, connected, sessionCount, retainedCount);
        if (!value.empty()) {
            values[topic] = value;
        }
    }
    return values;
}

void Broker::refreshSysTopics() {
    // Only the counts move on their own; a snapshot refresh keeps the retained
    // $SYS messages current so a client that subscribes with "send on subscribe"
    // sees live values. Called by the sweeper on its normal interval.
    //
    // $SYS values are informational and refreshed every sweep, so a topic whose
    // value did not change is left untouched: only a real change is written and
    // reported to the persistence layer, keeping a steady-state broker from
    // rewriting its state file every second. Updates are never fanned out live
    // (a $SYS subscription receives them on the next subscribe, like any
    // retained message).
    if (!sysTopicsEnabled) {
        return;
    }
    for (const auto &[topic, value] : sysTopicValues()) {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        auto it = retainedMessages.find(topic);
        if (it != retainedMessages.end() && it->second.payload ==
                                              std::vector<uint8_t>(value.begin(), value.end())) {
            continue;
        }
        retainedMessages[topic] = Message(topic, value, QoS::QOS_0, /*retain=*/true);
        notifyStateChanged();
    }
}

} // namespace MQTT
