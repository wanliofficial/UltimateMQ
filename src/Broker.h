#ifndef BROKER_H
#define BROKER_H
#pragma once

#include <mutex>
#include <set>
#include <vector>
#include <map>
#include <unordered_map>
#include <memory>
#include <atomic>
#include "Trie.h"
#include "Message.h"
#include "Subscription.h"
#include "Auth.h"
#include "Store.h"
#include "Frame.h"
#include "Tls.h"

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
    // The broker keeps a strong reference to every session so that a session
    // (and its subscriptions and unacknowledged in-flight messages) can outlive
    // the TCP connection that drives it and be resumed by a later connect.
    std::unordered_map<std::string, std::shared_ptr<Session>> sessions;
    std::unordered_map<std::string, std::set<std::string>> subscriptions;
    std::map<std::string, std::vector<SharedSubscription>> sharedSubscriptions;
    // Retained messages, keyed by their exact topic name
    std::map<std::string, Message> retainedMessages;
    mutable std::recursive_mutex mutex;

    //! Authentication / access control. The Broker does not own the Auth object:
    //! it is owned by the Server and outlives the broker. It is immutable after
    //! load, so queries need no locking here.
    Auth* auth = nullptr;

    //! Durable state. Not owned by the broker (the Server owns it); may be null.
    Store* store = nullptr;

    //! Upper bound for a single packet's Remaining Length, advertised to MQTT 5
    //! clients and enforced per connection. Defaults to Frame::DEFAULT_MAX_PACKET_SIZE.
    size_t maxPacketSize = Frame::DEFAULT_MAX_PACKET_SIZE;

    //! How long a stalled write may block before the connection is dropped,
    //! freeing its thread. Bounds a slow reader's hold on a connection thread.
    int writeTimeoutMs = SecureTransport::WRITE_STALL_TIMEOUT_MS;

    //! Upper bound on the number of subscriptions a single session may hold,
    //! so one client cannot exhaust memory by registering unbounded filters.
    size_t maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS;

    //! Resolve the target sessions; called with the broker lock held
    void collectTargets(const Message &message, std::vector<std::shared_ptr<Session>> &targets);

public:
    explicit Broker(Auth* auth = nullptr, Store* store = nullptr);
    ~Broker() = default;

    //! Attach the authentication/access-control engine (must be done before
    //! any client connects; the Auth object must outlive the broker)
    void setAuth(Auth* authHandle) { auth = authHandle; }
    Auth* getAuth() const { return auth; }

    //! Attach the persistence store (must be done before restore()). Not owned.
    void setStore(Store* storeHandle) { store = storeHandle; }
    Store* getStore() const { return store; }

    //! Configure the maximum packet size accepted from clients (and advertised
    //! to MQTT 5 clients via CONNACK). Must be set before any client connects.
    void setMaxPacketSize(size_t size) { maxPacketSize = size; }
    size_t getMaxPacketSize() const { return maxPacketSize; }

    //! Configure how long a stalled write may block before the connection is
    //! dropped. Must be set before any client connects.
    void setWriteTimeoutMs(int ms) { writeTimeoutMs = ms; }
    int getWriteTimeoutMs() const { return writeTimeoutMs; }

    //! Configure the maximum number of subscriptions per session. Must be set
    //! before any client connects.
    void setMaxSubscriptions(size_t limit) { maxSubscriptions = limit; }
    size_t getMaxSubscriptions() const { return maxSubscriptions; }
    //! Flag the durable state as changed (cheap: a single atomic store)
    void notifyStateChanged() {
        if (store) {
            store->markDirty();
        }
    }
    //! Write a snapshot of the entire durable state (retained messages and
    //! persistent sessions). Safe to call from the persistence thread.
    PersistedState snapshotState() const;
    //! Re-create the retained messages and persistent sessions from a snapshot.
    //! Called once at startup, before any client is accepted.
    void restoreState(const PersistedState& state);

    void insertSession(const std::string &clientId, const std::shared_ptr<Session>& session);
    std::shared_ptr<Session> findSession(const std::string &clientId) const;
    //! Remove and drop every disconnected session whose disconnect-time expiry
    //! deadline has passed. Called periodically by the server's event loop.
    void sweepExpiredSessions();
    //! Remove the session from the map if it is the one mapped to `session`
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

    //! All retained messages, keyed by topic name (returns a snapshot; thread safe)
    std::map<std::string, Message> snapshotRetained() const;
    size_t getRetainedCount() const;

    int getConnectedClients() const;
    bool isSubscribed(const std::string& clientId, const std::string& topicFilter) const;
    std::set<std::string> getSubscriptions(const std::string &topicFilter);
};
}

#endif
