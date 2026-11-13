#include "Broker.h"
#include "Topic.h"
#include "Session.h"
#include "Log.h"
#include <algorithm>
#include <iostream>
#include <thread>
#include <random>

namespace MQTT {

Broker::Broker() : trie(std::make_unique<Trie>()) {}

void Broker::insertSession(const std::string &clientId, const std::shared_ptr<Session>& session) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    sessions[clientId] = session;
}

bool Broker::insertSessionIfAbsent(const std::string &clientId, const std::shared_ptr<Session>& session) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it = sessions.find(clientId);
    if (it != sessions.end()) {
        if (auto existing = it->second.lock()) {
            if (existing != session) {
                return false;
            }
        }
    }
    sessions[clientId] = session;
    return true;
}

std::shared_ptr<Session> Broker::findSession(const std::string &clientId) const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it = sessions.find(clientId);
    if (it != sessions.end()) {
        return it->second.lock();
    }
    return nullptr;
}

void Broker::forgetSession(Session* session) {
    if (session == nullptr) {
        return;
    }
    std::lock_guard<std::recursive_mutex> lock(mutex);
    auto it = sessions.find(session->getClientId());
    if (it != sessions.end()) {
        auto existing = it->second.lock();
        if (!existing || existing.get() == session) {
            sessions.erase(it);
        }
    }
}

void Broker::removeSession(const std::string &clientId) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    sessions.erase(clientId);
}

void Broker::removeSession(Session* session) {
    forgetSession(session);
}

int Broker::getConnectedClients() const {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    int connected = 0;
    for (const auto &[clientId, weakSession] : sessions) {
        if (!weakSession.expired()) {
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
    auto &sharedSubs = sharedSubscriptions[topicFilter];
    if (std::find(sharedSubs.begin(), sharedSubs.end(), sharedSubscription) == sharedSubs.end()) {
        sharedSubs.push_back(sharedSubscription);
    }
    trie->insert(topicFilter);
}

void Broker::sharedUnsubscribe(const std::string &clientId, const std::string &topicFilter, const std::string &group) {
    std::lock_guard<std::recursive_mutex> lock(mutex);
    SharedSubscription sharedSubscription(clientId, topicFilter, group);
    auto it = sharedSubscriptions.find(topicFilter);
    if (it == sharedSubscriptions.end()) {
        return;
    }
    auto &sharedSubs = it->second;
    auto position = std::find(sharedSubs.begin(), sharedSubs.end(), sharedSubscription);
    if (position != sharedSubs.end()) {
        sharedSubs.erase(position);
    }
    if (sharedSubs.empty()) {
        sharedSubscriptions.erase(it);
    }
}

static int randIdx(int size) {
    static thread_local std::mt19937 gen(std::random_device{}());
    std::uniform_int_distribution<> dis(0, size - 1);
    return dis(gen);
}

void Broker::publish(const Message &message) {
    std::vector<std::shared_ptr<Session>> targets;
    {
        std::lock_guard<std::recursive_mutex> lock(mutex);
        collectTargets(message, targets);
    }
    // Deliveries are queued on the sessions here; the sockets are only written
    // by Session::flush() so that no client write is done while a lock is held
    for (const auto &session : targets) {
        session->deliver(session->matchingFilter(message.topic), message);
    }
    for (const auto &session : targets) {
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
    }

    std::vector<std::string> topicFilters = trie->match(message.topic);
    for (const auto &topicFilter : topicFilters) {
        auto it = subscriptions.find(topicFilter);
        if (it != subscriptions.end()) {
            for (const auto &clientId : it->second) {
                auto sessionIt = sessions.find(clientId);
                if (sessionIt != sessions.end()) {
                    if (auto session = sessionIt->second.lock()) {
                        targets.push_back(std::move(session));
                    }
                }
            }
        }
        auto sharedIt = sharedSubscriptions.find(topicFilter);
        if (sharedIt != sharedSubscriptions.end() && !sharedIt->second.empty()) {
            auto idx = randIdx(static_cast<int>(sharedIt->second.size()));
            const SharedSubscription &sharedSubscription = sharedIt->second.at(idx);
            auto sessionIt = sessions.find(sharedSubscription.clientId);
            if (sessionIt != sessions.end()) {
                if (auto session = sessionIt->second.lock()) {
                    targets.push_back(std::move(session));
                }
            }
        }
    }
}

} // namespace MQTT
