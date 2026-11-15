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
#include "TLS.h"

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
    //! Shared subscriptions, keyed first by the real topic filter and then by
    //! the share group name. Different groups on the same filter are
    //! independent: each one delivers one copy of every matching message
    //! (MQTT-4.8.2), so they must never share a member pool.
    std::map<std::string, std::map<std::string, std::vector<SharedSubscription>>> sharedSubscriptions;
    //! Round-robin cursor per (topic filter, group): the index of the member
    //! that receives the next message. Distribution inside a group is even, so
    //! no member starves. Keyed by the same pair as sharedSubscriptions.
    std::map<std::pair<std::string, std::string>, size_t> sharedRoundRobin;
    // Retained messages, keyed by their exact topic name
    std::map<std::string, Message> retainedMessages;

    //! A Will held pending its Will Delay Interval. The broker owns these (not
    //! the session or connection) so a delayed Will survives the disconnect even
    //! of a non-persistent session, and so the sweeper can publish it later.
    struct PendingWill {
        Message message;
        QoS qos = QoS::QOS_0;
        bool retain = false;
        std::chrono::steady_clock::time_point deadline;
        std::string username;
    };
    //! Delayed Wills keyed by client id. Populated on an abnormal disconnect
    //! whose CONNECT carried a Will Delay Interval; published by the sweeper once
    //! the deadline passes, or cancelled if the client reconnects first.
    std::map<std::string, PendingWill> pendingWills;
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

    //! Upper bound on the distinct shared (\$share/group/filter) subscriptions
    //! a single session may hold. They are bounded separately because each one
    //! also registers a group member in the broker. Defaults to the session's
    //! ordinary subscription limit unless explicitly configured.
    size_t maxSharedSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS;
    bool sharedQuotaExplicit = false;

    //! Per-client inbound PUBLISH rate, in messages per second. 0 disables the
    //! limit. A publisher that exceeds it is disconnected, so a single client
    //! cannot saturate the broker's fan-out path with an unbounded burst.
    size_t maxPublishRate = 0;

    //! Keep alive the broker prefers, in seconds. 0 disables the override and
    //! the broker honours whatever the client asks for. When set, a client that
    //! asks for more (or for none) is told to use this value via the MQTT 5
    //! Server Keep Alive property, so dead connections are always reaped.
    uint16_t serverKeepAlive = 0;

    //! Resolve the target sessions; called with the broker lock held
    void collectTargets(const Message &message, std::vector<std::shared_ptr<Session>> &targets);
    //! Pick the member of one share group that receives the next message,
    //! advancing that group's round-robin cursor. Called with the lock held.
    std::shared_ptr<Session> nextSharedMember(const std::string &topicFilter,
                                              const std::string &group,
                                              std::vector<SharedSubscription> &members);

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

    //! Configure the maximum number of shared subscriptions per session. Must
    //! be set before any client connects.
    void setMaxSharedSubscriptions(size_t limit) {
        maxSharedSubscriptions = limit;
        sharedQuotaExplicit = true;
    }
    size_t getMaxSharedSubscriptions() const { return maxSharedSubscriptions; }
    //! Whether an explicit shared-subscription limit was configured. When it was
    //! not, sessions fall back to their ordinary subscription limit, so raising
    //! FLOWMQ_MAX_SUBSCRIPTIONS also raises the shared quota.
    bool hasExplicitSharedQuota() const { return sharedQuotaExplicit; }

    //! Configure the per-client inbound PUBLISH rate (messages/second; 0 means
    //! unlimited, the default). Must be set before any client connects.
    void setMaxPublishRate(size_t rate) { maxPublishRate = rate; }
    size_t getMaxPublishRate() const { return maxPublishRate; }

    //! Configure the keep alive the broker prefers (seconds; 0 keeps the
    //! client's own value). Must be set before any client connects.
    void setServerKeepAlive(uint16_t seconds) { serverKeepAlive = seconds; }
    uint16_t getServerKeepAlive() const { return serverKeepAlive; }
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
    //! Arm a delayed Will (MQTT Will Delay Interval) to be published by the
    //! sweeper once the delay elapses, keyed by client id. A reconnect calls
    //! cancelWill to drop it before it fires.
    void armWill(const std::string &clientId, const Message &will, QoS qos,
                 bool retain, uint32_t delaySeconds);
    //! Cancel a pending delayed Will for this client id (a reconnect within the
    //! window cancels it). No-op if none is pending.
    void cancelWill(const std::string &clientId);
    //! Publish every pending delayed Will whose deadline has passed. Called by
    //! the sweeper alongside the session-expiry sweep.
    void publishDueWills();
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
