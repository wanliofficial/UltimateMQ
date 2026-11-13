#ifndef BROKER_H
#define BROKER_H
#pragma once

#include <mutex>
#include <set>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>
#include "Trie.h"
#include "Message.h"
#include "Subscription.h"

namespace MQTT {
class Session;
/*!
 * The broker owns all shared state (sessions, subscriptions, retained
 * messages). Every access goes through the internal lock, so callers must
 * never hold a Session pointer across two broker calls (e.g. findSession()
 * followed by Session::connect()): it may be gone by then.
 */
class Broker {
    std::unique_ptr<Trie> trie;
    // The broker does not own sessions: they die with the connection that
    // drives them. findSession()/collectTargets() hand out shared references so
    // a session cannot be destroyed while a publisher is still using it.
    std::unordered_map<std::string, std::weak_ptr<Session>> sessions;
    std::unordered_map<std::string, std::set<std::string>> subscriptions;
    std::map<std::string, std::vector<SharedSubscription>> sharedSubscriptions;
    // Retained messages, keyed by their exact topic name
    std::map<std::string, Message> retainedMessages;
    mutable std::recursive_mutex mutex;

    //! Resolve the target sessions; called with the broker lock held
    void collectTargets(const Message &message, std::vector<std::shared_ptr<Session>> &targets);

public:
    explicit Broker();
    ~Broker() = default;
    void insertSession(const std::string &clientId, const std::shared_ptr<Session>& session);
    //! Register \p session unless another live session already uses \p clientId
    bool insertSessionIfAbsent(const std::string &clientId, const std::shared_ptr<Session>& session);
    std::shared_ptr<Session> findSession(const std::string &clientId) const;
    //! Remove the session from the map if (and only if) it is mapped to `session`
    void forgetSession(Session* session);
    void removeSession(const std::string &clientId);
    void removeSession(Session* session);

    void subscribe(const std::string &clientId, const std::string &topicFilter);
    void unsubscribe(const std::string &clientId, const std::string &topicFilter);
    void sharedSubscribe(const std::string &clientId, const std::string &topicFilter, const std::string &group);
    void sharedUnsubscribe(const std::string &clientId, const std::string &topicFilter, const std::string &group);
    //! Fan out a message. Safe to call from any thread: the actual writes to
    //! the clients happen after the broker lock has been released.
    void publish(const Message &message);

    //! All retained messages, keyed by topic name
    const std::map<std::string, Message> &getRetainedMessages() const { return retainedMessages; }
    size_t getRetainedCount() const { return retainedMessages.size(); }

    int getConnectedClients() const;
    bool isSubscribed(const std::string& clientId, const std::string& topicFilter) const;
    std::set<std::string> getSubscriptions(const std::string &topicFilter);
};
}

#endif
