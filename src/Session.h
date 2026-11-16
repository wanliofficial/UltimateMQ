#ifndef SESSION_H
#define SESSION_H
#pragma once

#include <atomic>
#include <chrono>
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
    virtual void deliver(const Message& message, uint16_t packetId, QoS qos,
                         uint32_t subscriptionIdentifier = 0) = 0;
    //! Resend a PUBREC for an inbound QoS 2 flow that is still awaiting its PUBREL.
    //! Called on resume so a client whose PUBREL was lost can complete the
    //! handshake instead of starting the QoS 2 flow over.
    virtual void sendPubrec(uint16_t packetId) = 0;
    //! Re-send a PUBREL for an outgoing QoS 2 flow that had already sent its
    //! PUBREC but whose PUBCOMP was lost when the connection dropped. Called on
    //! resume so the client can complete the handshake (MQTT-4.4.0-1).
    virtual void sendPubrel(uint16_t packetId) = 0;
    //! Called when a newer connection takes over this session; the receiver must
    //! stop using the session and release it. \p version is the MQTT version of
    //! the session being handed over, so a protocol-aware client can tell its
    //! peer why it was disconnected.
    virtual void onSuperseded(Version version) = 0;
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
    //! \p duplicate is the DUP flag of an inbound QoS 2 PUBLISH. A DUP retransmit
    //! for a flow already awaiting PUBREL must not re-route the message, but the
    //! caller must resend PUBREC with SUCCESS (MQTT-4.3.3-1) rather than report an
    //! error.
    ReasonCode publish(uint16_t packetId, const Message& message, bool duplicate = false);
    void subscribe(const std::string& topic, const SubscriptionOptions& options);
    //! Remove a subscription. Returns true when one actually existed and was
    //! removed, false when there was none (MQTT 5 UNSUBACK reports the two cases
    //! with Success and No Subscription Existed respectively, MQTT-3.11.3).
    bool unsubscribe(const std::string& topic);
    void puback(uint16_t packetId);
    ReasonCode pubrec(uint16_t packetId);
    ReasonCode pubrel(uint16_t packetId);
    void pubcomp(uint16_t packetId);

    //! Arm a disconnect-time expiry: after a graceful disconnect the session is
    //! dropped this many seconds later unless a client reconnects first. Called
    //! when a persistent session is kept alive on a DISCONNECT with a positive
    //! Session Expiry Interval. A value of 0 cancels any pending expiry.
    void armExpiry(std::chrono::seconds seconds);
    //! Cancel any pending expiry (a live session never expires)
    void clearExpiry();
    //! Whether this session's expiry deadline has passed (thread safe)
    bool isExpired(std::chrono::steady_clock::time_point now) const;

    //! Bind the session to the connection that drives it (may be nullptr to detach)
    void bindClient(const std::shared_ptr<Client>& client);
    //! Rebind a persistent session to a new connection and retransmit the
    //! unacknowledged QoS>0 messages it still holds
    //! Bind \p client as the live client, retransmitting anything outstanding.
    //! \p protocolVersion is the version the new connection negotiated, used to
    //! tell a superseded connection how to report the takeover.
    void resume(const std::shared_ptr<Client>& client, Version protocolVersion);
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

    //! Queue a message for this session (thread safe). \p filter is the
    //! subscription the delivery comes through; its options (Maximum QoS,
    //! Retain As Published, No Local) are applied, while read access is checked
    //! against the concrete message topic.
    void deliver(const std::string& filter, const Message& message);
    //! Queue the retained messages matching a topic filter (honours the retain handling option).
    //! \p isNewSubscription is whether this filter was newly created by the current
    //! SUBSCRIBE (as opposed to a re-subscribe updating options); it drives the
    //! "send at subscribe only if the subscription did not already exist" handling.
    void deliverRetained(const std::string& topicFilter, const SubscriptionOptions& options,
                         bool isNewSubscription);
    //! Hand the queued deliveries to the bound client; must be called without holding any lock
    void flush();
    bool isPersistent() const { return persistent; }

    //! Whether this session already holds the exact topic filter
    bool hasSubscription(const std::string& topicFilter);
    //! Number of distinct topic filters this session holds (thread safe)
    size_t getSubscriptionCount();
    //! Upper bound on the number of subscriptions a single session may hold.
    //! Bounds per-client memory so one client cannot register an unbounded set
    //! of filters. Default is DEFAULT_MAX_SUBSCRIPTIONS; tunable via
    //! FLOWMQ_MAX_SUBSCRIPTIONS.
    void setMaxSubscriptions(size_t limit) { maxSubscriptions = limit; }
    size_t getMaxSubscriptions() const { return maxSubscriptions; }
    //! Upper bound on the number of distinct shared (\$share/group/filter)
    //! subscriptions a single session may hold. They are excluded from
    //! maxSubscriptions because one entry can be re-expanded into many group
    //! registrations, so they need their own bound. Defaults to
    //! DEFAULT_MAX_SUBSCRIPTIONS; tunable via FLOWMQ_MAX_SHARED_SUBSCRIPTIONS.
    void setMaxSharedSubscriptions(size_t limit) { maxSharedSubscriptions = limit; }
    size_t getMaxSharedSubscriptions() const { return maxSharedSubscriptions; }
    //! Number of distinct shared subscriptions this session holds (thread safe)
    size_t getSharedSubscriptionCount();
    //! MQTT 5 Receive Maximum: cap on the number of QoS 1/2 messages the broker
    //! may keep outstanding (sent, unacknowledged) to this client. Defaults to
    //! the protocol default of 65535, i.e. no effective limit.
    void setReceiveMaximum(uint16_t limit) { receiveMaximum = limit; }

private:
    struct Delivery {
        Message message;
        uint16_t packetId;
        QoS qos;
        //! MQTT 5 Subscription Identifier of the subscription that caused this
        //! delivery (0 for none); echoed on the PUBLISH (MQTT-3.3.2-5).
        uint32_t subscriptionIdentifier;

        Delivery(Message message, uint16_t packetId, QoS qos, uint32_t subscriptionIdentifier = 0)
            : message(std::move(message)), packetId(packetId), qos(qos),
              subscriptionIdentifier(subscriptionIdentifier) {}
    };

    std::string clientId;
    std::atomic<bool> connected{false};
    bool cleanStart = true;
    bool persistent = false;
    std::string username;
    //! Upper bound of the per-session delivery queue
    static constexpr size_t MAX_QUEUED_MESSAGES = 1000;
    //! Upper bound on subscriptions held by this session
    size_t maxSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS;
    //! Upper bound on distinct shared subscriptions held by this session
    size_t maxSharedSubscriptions = DEFAULT_MAX_SUBSCRIPTIONS;
    //! Cap on outstanding (sent, unacknowledged) QoS 1/2 messages to the client
    uint16_t receiveMaximum = 65535;
    uint16_t packetId = 1;
    std::set<uint16_t> awaitingPubrel;
    //! Outgoing QoS 2 packet ids for which this session has sent a PUBREL and is
    //! waiting for the matching PUBCOMP. If the connection drops in that state
    //! the PUBREL is re-sent on resume (MQTT-4.4.0-1); the message itself has
    //! already been delivered, so only the packet id needs remembering.
    std::set<uint16_t> pendingPubrel;
    std::map<std::string, SubscriptionOptions> subscriptions;
    std::map<uint16_t, std::shared_ptr<Message>> inflightMessages;
    //! The MQTT 5 Subscription Identifier carried by each inflight delivery, so
    //! a retransmitted QoS 1/2 PUBLISH still echoes it (MQTT-3.3.2-5 applies to
    //! retransmissions too). Keyed by the same packet id as inflightMessages.
    std::map<uint16_t, uint32_t> inflightIdentifiers;
    std::vector<Delivery> pendingDeliveries;
    //! QoS>0 messages waiting for an inflight slot to free up under the client's
    //! MQTT 5 Receive Maximum. They have no packet id yet and are not inflight,
    //! so they are not retransmitted until released into the inflight set.
    std::vector<Delivery> blockedDeliveries;
    std::shared_ptr<Client> client;

    //! Disconnect-time session expiry. hasExpiry marks the deadline as armed;
    //! expiryDeadline is an absolute steady-clock time point. Guarded by
    //! deliveryMutex.
    bool hasExpiry = false;
    std::chrono::steady_clock::time_point expiryDeadline;

    mutable std::recursive_mutex deliveryMutex;
    std::function<void()> onDisconnect;
    //! Invoked whenever the durable state changes (dirty marker / persistence)
    std::function<void()> mutateCallback;
    //! Called with deliveryMutex held; never takes any other lock
    void notifyMutation();

    void pushDelivery(const Message& message, QoS qos, uint32_t subscriptionIdentifier = 0);
    void removeSubscriptions(const std::map<std::string, SubscriptionOptions>& filters);
    uint16_t nextPacketId();
    //! Move blocked QoS>0 messages into the inflight set / send queue while the
    //! outstanding count is below the client's Receive Maximum.
    void releaseBlocked();

    Broker* broker;
    Auth* auth = nullptr;
};

}

#endif
