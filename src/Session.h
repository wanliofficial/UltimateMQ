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
    //! Queue one delivery for the client. Returns false when the packet could
    //! not be sent (for example it exceeds the client's Maximum Packet Size,
    //! MQTT-3.1.2-24): the session then releases the packet id it reserved so
    //! the flow-control window is not leaked.
    //! \p subscriptionIdentifiers holds the MQTT 5 Subscription Identifier of
    //! every subscription that caused this copy (MQTT-3.3.4-4), so it can be
    //! more than one when overlapping subscriptions produced a single copy.
    virtual bool deliver(const Message& message, uint16_t packetId, QoS qos,
                         const SubscriptionIdentifiers& subscriptionIdentifiers) = 0;
    //! Resend a PUBREC for an inbound QoS 2 flow that is still awaiting its PUBREL.
    //! Called on resume so a client whose PUBREL was lost can complete the
    //! handshake instead of starting the QoS 2 flow over.
    virtual void sendPubrec(uint16_t packetId) = 0;
    //! Re-send a PUBREL for an outgoing QoS 2 flow that had already sent its
    //! PUBREC but whose PUBCOMP was lost when the connection dropped. Called on
    //! resume so the client can complete the handshake (MQTT-4.4.0-1).
    virtual void sendPubrel(uint16_t packetId) = 0;
    //! Called when a newer connection takes over this session; the receiver must
    //! stop using the session and release it. \p publishWill tells it whether the
    //! old connection's Will is due under MQTT-3.1.4-3.
    virtual uint32_t willDelayInterval() const { return 0; }
    virtual void onSuperseded(bool publishWill) = 0;
};

class Session : public std::enable_shared_from_this<Session> {
    friend class Broker;
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
    bool close(const std::shared_ptr<Client>& client, bool endSession = false);
    void discard(bool publishWill = false);
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
    //! Handle the client's PUBREC for an outbound QoS 2 PUBLISH. Returns the
    //! Reason Code to carry in the PUBREL, or nullopt when no PUBREL may be sent
    //! because the client rejected the message with a Reason Code of 0x80 or
    //! greater. A sender only sends PUBREL for a PUBREC below 0x80 (MQTT-4.3.3),
    //! and such a rejection acknowledges the PUBLISH, which must then never be
    //! retransmitted and whose identifier is immediately reusable (MQTT-4.4.0-2).
    std::optional<ReasonCode> pubrec(uint16_t packetId,
                                     ReasonCode clientReason = ReasonCode::SUCCESS);
    ReasonCode pubrel(uint16_t packetId);
    void pubcomp(uint16_t packetId);
    //! Re-queue an inflight QoS 1/2 message for immediate retransmission with
    //! DUP set, without acknowledging it. Used when the receiver rejected the
    //! publish but the flow is still open (MQTT-SN §5.3.6 congestion rejection);
    //! the identifier stays outstanding until an acknowledgement arrives.
    void retransmitInflight(uint16_t packetId);

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
    //! Bind \p client as the live client with the new connection's persistence,
    //! retransmitting anything outstanding.
    void resume(const std::shared_ptr<Client>& client, bool persistent);
    //! Whether takeover ends the old session or its Will has zero delay
    //! (MQTT-3.1.4-3 / §3.1.2.5).
    bool shouldPublishWillOnTakeover(bool newCleanStart) const;
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

    //! Queue a message for this session (thread safe). \p filters are all the
    //! subscriptions of this session the message matched; their options are
    //! combined as MQTT-3.3.4-2 requires (a single copy is delivered at the
    //! highest Maximum QoS of the matching subscriptions, carries every
    //! Subscription Identifier, and is suppressed by No Local only when every
    //! matching subscription asked for it). Read access is checked against the
    //! concrete message topic.
    void deliver(const std::vector<std::string>& filters, const Message& message);
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
    //! Number of QoS 1/2 messages this session currently has in flight -- sent but
    //! not yet acknowledged -- across both legs of the flow: messages awaiting
    //! PUBACK/PUBCOMP (inflightMessages) plus QoS 2 packet ids awaiting PUBCOMP
    //! after the PUBREL went out (pendingPubrel). Both occupy a Receive Maximum
    //! slot, so their sum is the session's live flow-control occupancy (thread safe).
    size_t getInflightCount();
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
    //! the protocol default of 65535, i.e. no effective limit. Written under
    //! deliveryMutex: it is set on the connect path before the session delivers,
    //! but the delivery path reads it under the lock, so the write takes the lock
    //! too (a recursive mutex, so a nested call is safe).
    void setReceiveMaximum(uint16_t limit) {
        std::lock_guard<std::recursive_mutex> lock(deliveryMutex);
        receiveMaximum = limit;
    }
    //! Choose what happens when the delivery queue is full. DropNewest (the
    //! default) drops the incoming message; DropOldest evicts the oldest buffered
    //! message to keep the newest. Opt-in via FLOWMQ_QUEUE_DROP=oldest; the
    //! default path is unchanged.
    void setQueueDropPolicy(QueueDropPolicy policy) { queueDropPolicy = policy; }
    //! Test hook: position the outgoing Packet Identifier counter, so a test can
    //! reach the wrap-around of the 16-bit space without sending 65k messages.
    void setNextPacketIdForTest(uint16_t id) { packetId = id; }
    //! Test hook: number of messages flow-blocked under the client's Receive
    //! Maximum (QoS>0 admitted to the window but parked pending an inflight
    //! slot). Lets a test assert that dropping a queued QoS>0 delivery released
    //! its slot rather than over-subscribing the window.
    size_t blockedCountForTest() { return blockedDeliveries.size(); }
    //! Test hook: number of deliveries the session is currently holding that a
    //! discard would throw away -- pending + flow-blocked + inflight (QoS>0 sent
    //! but unacknowledged). Lets a test assert that tearing a session down counts
    /// those messages as dropped rather than silently losing them.
    size_t droppedOnDiscardForTest() {
        return pendingDeliveries.size() + blockedDeliveries.size() + inflightMessages.size();
    }

private:
    void connectLocked(const std::shared_ptr<Session>& self, bool persistent);
    std::function<void()> resumeLocked(const std::shared_ptr<Client>& client, bool persistent);
    bool closeLocked(const std::shared_ptr<Client>& client, bool endSession);
    std::function<void()> discardLocked(bool publishWill);

    struct Delivery {
        Message message;
        uint16_t packetId;
        QoS qos;
        //! MQTT 5 Subscription Identifiers of the subscriptions that caused this
        //! delivery (empty for none); echoed on the PUBLISH (MQTT-3.3.4-3/-4).
        SubscriptionIdentifiers subscriptionIdentifiers;
        //! Monotonic insertion order across pendingDeliveries and
        //! blockedDeliveries, so "oldest" is well defined even when messages of
        //! one queue are split across the two containers (the drop-oldest policy
        //! compares the two fronts). Assigned once when the delivery is created.
        unsigned long long sequence = 0;
        Delivery(Message message, uint16_t packetId, QoS qos,
                 SubscriptionIdentifiers subscriptionIdentifiers = {},
                 unsigned long long sequence = 0)
            : message(std::move(message)), packetId(packetId), qos(qos),
              subscriptionIdentifiers(std::move(subscriptionIdentifiers)),
              sequence(sequence) {}
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
    //! Backpressure policy when the delivery queue is full (see QueueDropPolicy).
    QueueDropPolicy queueDropPolicy = QueueDropPolicy::DropNewest;
    //! Monotonic counter assigning each Delivery its insertion order; never reset
    //! for the life of the session so the drop-oldest comparison is total.
    unsigned long long deliverySequence = 0;
    std::set<uint16_t> awaitingPubrel;
    //! Outgoing QoS 2 packet ids for which this session has sent a PUBREL and is
    //! waiting for the matching PUBCOMP. If the connection drops in that state
    //! the PUBREL is re-sent on resume (MQTT-4.4.0-1); the message itself has
    //! already been delivered, so only the packet id needs remembering.
    std::set<uint16_t> pendingPubrel;
    std::map<std::string, SubscriptionOptions> subscriptions;
    std::map<uint16_t, std::shared_ptr<Message>> inflightMessages;
    //! The MQTT 5 Subscription Identifiers carried by each inflight delivery, so
    //! a retransmitted QoS 1/2 PUBLISH still echoes them (MQTT-3.3.4-3 applies
    //! to retransmissions too). Keyed by the same packet id as inflightMessages.
    std::map<uint16_t, SubscriptionIdentifiers> inflightIdentifiers;
    std::vector<Delivery> pendingDeliveries;
    //! QoS>0 messages waiting for an inflight slot to free up under the client's
    //! MQTT 5 Receive Maximum. They have no packet id yet and are not inflight,
    //! so they are not retransmitted until released into the inflight set.
    std::vector<Delivery> blockedDeliveries;
    //! The queued-bytes contribution this session has most recently reported to
    //! the broker's live gauge (via syncQueuedBytesLocked). The delta between it
    //! and a fresh recount is what gets added to the gauge, so the gauge stays
    /// accurate as messages enter and leave the queues. Guarded by deliveryMutex.
    uint64_t reportedQueuedBytes = 0;
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

    //! Count every delivery the session still holds (pending + flow-blocked +
    //! unacknowledged inflight QoS 1/2) into the broker's backpressure drop
    //! metric, then return that count. Called, with deliveryMutex held, from each
    //! path that throws those deliveries away (discard, and the non-persistent
    //! branch of close) so a torn-down session's lost messages are observable
    //! rather than vanishing silently. Idempotent per teardown path: the caller
    //! clears the queues right after, so a second call counts zero.
    uint64_t countHeldDeliveriesLocked();

    void pushDelivery(const Message& message, QoS qos,
                      const SubscriptionIdentifiers& subscriptionIdentifiers = {});
    //! Drop the single oldest buffered delivery (front of pendingDeliveries or
    //! blockedDeliveries, by insertion sequence) to make room for a newer one.
    //! Used by the drop-oldest backpressure policy. A dropped QoS>0 delivery
    //! that had claimed an inflight slot releases it, so the Receive Maximum
    //! window never leaks. Called with deliveryMutex held.
    void dropOldestQueued();
    //! Approximate in-memory bytes the session's undelivered messages occupy
    //! (payload + topic of every pending, flow-blocked and unacknowledged
    //! inflight message). Used only for the $SYS/broker/queued/bytes gauge, so
    //! it deliberately counts neither container/vector overhead nor property
    //! storage: it is a directional backpressure signal, not an allocator-accurate
    //! figure. Reads only; may be called with or without deliveryMutex held (it
    //! copies nothing and touches no shared mutable state).
    size_t queuedBytesApprox() const;
    //! Reconcile the broker's live queued-bytes gauge with what this session
    //! currently holds, by recounting all three delivery queues and applying the
    //! difference. Called with deliveryMutex held at every point the queues
    //! change (enqueue, flush, drop, discard, restore, reconnect), so the gauge
    //! tracks the real in-process buffer instead of drifting. A no-op without a
    //! broker. Purely informational: nothing depends on it for correctness.
    void syncQueuedBytesLocked();
    //! Forget an inflight QoS>0 message and release its window slot without
    //! retransmitting it (used when a queued copy is dropped before it was ever
    //! sent). Called with deliveryMutex held.
    void releaseInflightIfPresent(uint16_t packetId);
    void removeSubscriptions(const std::map<std::string, SubscriptionOptions>& filters);
    uint16_t nextPacketId();
    //! Move blocked QoS>0 messages into the inflight set / send queue while the
    //! outstanding count is below the client's Receive Maximum.
    void releaseBlocked();
    //! On resume, demote excess QoS>0 messages, including retransmissions, back
    //! to the blocked queue so a smaller Receive Maximum is honoured
    //! (MQTT-4.9.0-2). Called with deliveryMutex held.
    void reflowToReceiveMaximum();
    //! Forget an inflight QoS>0 message that will never be acknowledged (it was
    //! dropped by flow control/expiry or refused by the client) and release its
    //! slot so the Receive Maximum window does not leak.
    void releaseInflight(uint16_t packetId);

    Broker* broker;
    Auth* auth = nullptr;
};

}

#endif
