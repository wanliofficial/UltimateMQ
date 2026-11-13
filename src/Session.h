#ifndef SESSION_H
#define SESSION_H
#pragma once

#include <atomic>
#include <memory>
#include <mutex>
#include <string>
#include <queue>
#include <functional>
#include <vector>
#include "Message.h"
#include "MQTT.h"
#include "Broker.h"
#include "Store.h"

namespace MQTT {

class Broker;

/*!
 * A session holds the subscription state of one client id.
 *
 * The session outlives the TCP connection: it can be taken over by a newer
 * connection while the old one is still running, and it is kept alive by the
 * client it is bound to. It therefore never stores a raw pointer to the
 * connection: deliveries are collected into a queue and handed to the client
 * interface (`Client::deliver`) by flush(), which is always called after all
 * broker and session locks have been released.
 *
 * Lock order: Broker::mutex -> Session::deliveryMutex -> Connection::writeMutex.
 * Session never takes the broker lock while holding deliveryMutex.
 */
class Client {
public:
    virtual ~Client() = default;
    virtual void deliver(const Message& message, uint16_t packetId, QoS qos) = 0;
    //! Called when a newer connection takes over this session; the receiver must
    //! stop using the session and release it
    virtual void onSuperseded() = 0;
};

class Session : public std::enable_shared_from_this<Session> {
public:
    //! The durable part of a session, as written to the state file
    using Snapshot = PersistedSession;

    Session(Broker* broker, const std::string& clientId, bool cleanStart = true);
    ~Session();

    const std::string& getClientId() const { return clientId; }
    bool isConnected() const { return connected; }
    bool cleanStartRequested() const { return cleanStart; }

    //! Register with the broker (keeps a shared reference to this session)
    void connect(const std::shared_ptr<Session>& self, bool persistent = false);
    //! Called when the driving connection is gone; tears the session down unless
    //! it is persistent (and therefore kept alive for a later reconnect)
    void close(const std::shared_ptr<Client>& client);
    void discard();
    ReasonCode publish(uint16_t packetId, const Message& message);
    void subscribe(const std::string& topic, const SubscriptionOptions& options);
    void unsubscribe(const std::string& topic);
    void puback(uint16_t packetId);
    ReasonCode pubrec(uint16_t packetId);
    ReasonCode pubrel(uint16_t packetId);
    void pubcomp(uint16_t packetId);

    //! Bind the session to the connection that drives it (may be nullptr to detach)
    void bindClient(const std::shared_ptr<Client>& client);
    //! Rebind a persistent session to a new connection and retransmit the
    //! unacknowledged QoS>0 messages it still holds
    void resume(const std::shared_ptr<Client>& client);
    void setDisconnectCallback(std::function<void()> callback);

    //! The username this session authenticated with (empty for anonymous / auth-disabled)
    void setUsername(std::string username) { this->username = std::move(username); }
    const std::string& getUsername() const { return username; }

    //! Mark the session as persistent (it will be written to the state file)
    void setPersistent(bool value) { persistent = value; }
    //! Attach the access-control engine (used for read checks on delivery)
    void setAuth(Auth* authHandle) { auth = authHandle; }
    //! Callback invoked on every durable change; must not take other locks
    void setMutateCallback(std::function<void()> callback);
    //! Copy the durable state out, for the persistence store (thread safe)
    Snapshot snapshot() const;
    //! Restore durable state on startup; the caller inserts the session into the
    //! broker afterwards. Does not touch the broker.
    void restore(const Snapshot& snapshot);

    //! Queue a message for this session (thread safe)
    void deliver(const std::string& topic, const Message& message);
    //! Queue the retained messages matching a topic filter (honours the retain handling option)
    void deliverRetained(const std::string& topicFilter, const SubscriptionOptions& options);
    //! Hand the queued deliveries to the bound client; must be called without holding any lock
    void flush();
    //! The subscription filter of this session that matches \p topic (or the topic itself)
    std::string matchingFilter(const std::string &topic);
    bool isPersistent() const { return persistent; }

private:
    struct Delivery {
        Message message;
        uint16_t packetId;
        QoS qos;

        Delivery(Message message, uint16_t packetId, QoS qos)
            : message(std::move(message)), packetId(packetId), qos(qos) {}
    };

    std::string clientId;
    std::atomic<bool> connected{false};
    bool cleanStart = true;
    bool persistent = false;
    std::string username;
    //! Upper bound of the per-session delivery queue
    static constexpr size_t MAX_QUEUED_MESSAGES = 1000;
    uint16_t packetId = 1;
    std::set<uint16_t> awaitingPubrel;
    std::map<std::string, SubscriptionOptions> subscriptions;
    std::map<uint16_t, std::shared_ptr<Message>> inflightMessages;
    std::vector<Delivery> pendingDeliveries;
    std::shared_ptr<Client> client;

    mutable std::recursive_mutex deliveryMutex;
    std::function<void()> onDisconnect;
    //! Invoked whenever the durable state changes (dirty marker / persistence)
    std::function<void()> mutateCallback;
    //! Called with deliveryMutex held; never takes any other lock
    void notifyMutation();

    void pushDelivery(const Message& message, QoS qos);
    void removeSubscriptions(const std::map<std::string, SubscriptionOptions>& filters);
    uint16_t nextPacketId();

    Broker* broker;
    Auth* auth = nullptr;
    bool newSubscription = true;
};

}

#endif
