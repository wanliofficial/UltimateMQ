#include "Broker.h"
#include "Topic.h"
#include "Session.h"
#include "Log.h"
#include <algorithm>
#include <chrono>
#include <iostream>
#include <thread>
#include <vector>

namespace MQTT {

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
    std::vector<std::shared_ptr<Session>> targets;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        collectTargets(message, targets);
    }
    // A session may match through several subscription filters (e.g. "a/#" and
    // "a/b"): deliver a single copy per session
    std::vector<std::shared_ptr<Session>> unique;
    std::set<Session *> seen;
    unique.reserve(targets.size());
    for (const auto &session : targets) {
        if (seen.insert(session.get()).second) {
            unique.push_back(session);
        }
    }
    // Deliveries are queued on the sessions here; the sockets are only written
    // by Session::flush() so that no client write is done while a lock is held
    for (const auto &session : unique) {
        session->deliver(session->matchingFilter(message.topic), message);
    }
    for (const auto &session : unique) {
        session->flush();
    }
}

void Broker::collectTargets(const Message &message, std::vector<std::shared_ptr<Session>> &targets) {
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
                    targets.push_back(sessionIt->second);
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
                    targets.push_back(std::move(session));
                }
            }
        }
    }
}

} // namespace MQTT
