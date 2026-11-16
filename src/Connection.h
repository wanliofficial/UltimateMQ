#ifndef CONNECTION_H
#define CONNECTION_H
#pragma once

#include <sys/socket.h>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <atomic>
#include <functional>
#include "MQTT.h"
#include "Message.h"
#include "Frame.h"
#include "Session.h"
#include "Auth.h"
#include "TLS.h"

namespace MQTT {
class Broker;
class Session;

/*!
 * One client connection. The object is owned by the shared_ptr returned from
 * Server::handleClient(); the event loop keeps a weak_ptr so that a session
 * can never call into an already destroyed connection.
 */
class Connection : public Client, public std::enable_shared_from_this<Connection> {
    enum class State {
        IDLE,
        CONNECTED,
        DISCONNECTED
    };

public:
    explicit Connection(int sockfd, Broker* broker,
                        std::shared_ptr<TLSContext> TLS = nullptr);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void run();
    bool isConnected() const;

    //! Discard any incomplete frame and start over (a TCP segment may split packets)
    void resetParser();

    void handleIncoming(std::shared_ptr<Packet> packet);
    void handleConnect(std::shared_ptr<ConnectPacket> packet);
    void handlePublish(std::shared_ptr<PublishPacket> packet);
    void handlePuback(std::shared_ptr<PubackPacket> packet);
    void handlePubrec(std::shared_ptr<PubrecPacket> packet);
    void handlePubrel(std::shared_ptr<PubrelPacket> packet);
    void handlePubcomp(std::shared_ptr<PubcompPacket> packet);
    void handleSubscribe(std::shared_ptr<SubscribePacket> packet);
    void handleUnsubscribe(std::shared_ptr<UnsubscribePacket> packet);
    void handlePingreq(std::shared_ptr<PingreqPacket> packet);
    void handlePingresp(std::shared_ptr<PingrespPacket> packet);
    void handleDisconnect(std::shared_ptr<DisconnectPacket> packet);
    void handleAuth(std::shared_ptr<AuthPacket> packet);

    //! Client interface: write a delivery to the socket (called without any broker/session lock).
    //! Returns false if the packet could not be sent (e.g. over the client's
    //! Maximum Packet Size), so the session can release the inflight slot.
    bool deliver(const Message& message, uint16_t packetId, QoS qos,
                 const SubscriptionIdentifiers& subscriptionIdentifiers) override;
    //! Client interface: resend a PUBREC for an inbound QoS 2 flow awaiting PUBREL
    void sendPubrec(uint16_t packetId) override;
    //! Client interface: resend a PUBREL for an outgoing QoS 2 flow awaiting PUBCOMP
    void sendPubrel(uint16_t packetId) override;
    //! Client interface: a newer connection has taken over this session; stop using it
    void onSuperseded(Version version) override;
    bool handleDeliver(const Message& message, uint16_t packetId, QoS qos,
                       const SubscriptionIdentifiers& subscriptionIdentifiers);
    //! Serialize and write a packet. Returns false when nothing was written
    //! (socket gone, or the packet exceeds the client's Maximum Packet Size).
    bool sendPacket(Packet &packet);
    //! Report a Topic Alias protocol violation with a DISCONNECT carrying \p
    //! reason, mark the connection closed and return false (MQTT-3.3.2-7/-8).
    bool failTopicAlias(ReasonCode reason, const std::string &detail);
    //! End the connection with a reason code (MQTT 5 only) instead of vanishing
    void closeWithReason(ReasonCode reason);

private:
    void runReadLoop();
    void closeSocket();
    void publishWill();
    //! Stamps the Will's Message Expiry Interval onto \p will as a deadline
    //! starting now (MQTT-3.3.2-9). No-op when the CONNECT carried no interval.
    void armWillExpiry(Message &will) const;
    void armKeepAliveTimer();
    void scheduleReadTimeout();
    void setDisconnectCallback(std::function<void()> callback);

    int sockfd;
    std::atomic<State> state;
    Frame frame;
    int writeTimeoutMs;
    std::shared_ptr<Session> session;
    Broker* broker;
    Auth* auth = nullptr;
    std::recursive_mutex writeMutex;
    std::atomic<bool> connected{false};
    //! Whether a CONNACK has already been sent on this connection. A failure
    //! while parsing the CONNECT must answer with CONNACK (MQTT-3.2.2-2), but
    //! only once: every later failure gets a DISCONNECT instead.
    bool connackSent = false;
    std::chrono::steady_clock::time_point lastActivity;
    uint16_t keepAliveSeconds = 0;
    std::chrono::milliseconds readTimeout{0};
    //! Session Expiry Interval (MQTT 5) in effect for this connection: taken from
    //! the CONNECT and possibly overridden by a graceful DISCONNECT
    //! (MQTT-3.14.4-2). 0 (the default when absent, and always on MQTT 3.1.x with
    //! clean session) means the session is discarded when the connection closes.
    uint32_t sessionExpirySeconds = 0;
    //! The Session Expiry Interval exactly as the CONNECT packet carried it (0
    //! when absent). §3.14.2.2.2 compares against this value, not against the
    //! one a DISCONNECT may have overridden afterwards.
    uint32_t connectSessionExpiry = 0;
    //! MQTT 5 Topic Alias support (MQTT-3.3.2-7..11). The broker advertises
    //! TopicAliasMaximum in CONNACK and a client may then send a PUBLISH whose
    //! topic name is replaced by an alias after mapping it once. The mapping is
    //! scoped to this network connection (not the session), so it lives here and
    //! is reset for every CONNECT. An alias is valid in 1..TOPIC_ALIAS_MAXIMUM;
    //! one with a topic name registers/updates the mapping, one without a topic
    //! resolves through it.
    static constexpr uint16_t TOPIC_ALIAS_MAXIMUM = 16;
    std::map<uint16_t, std::string> topicAliases;
    //! Largest packet the client accepts (MQTT 5 Maximum Packet Size,
    //! MQTT-3.1.2-24). 0 means "no limit" (the property was absent). A packet
    //! larger than this is dropped instead of written, since sending it would
    //! break the connection.
    size_t clientMaxPacketSize = 0;
    //! Expand an inbound PUBLISH's Topic Alias into its topic name. Returns false
    //! (after signalling the error to the client) when the alias is invalid, as
    //! MQTT-3.3.2-9 requires the connection to be closed in that case.
    bool resolveTopicAlias(const std::shared_ptr<PublishPacket>& publish);
    //! Per-client inbound PUBLISH rate limit (token bucket). Returns false when
    //! a message must be refused because the client exceeded its allowance.
    bool allowPublish();
    //! Messages per second this client may publish; 0 means unlimited
    size_t publishRateLimit = 0;
    //! Tokens available to this connection's bucket, capped at one second's worth
    double publishTokens = 0;
    std::chrono::steady_clock::time_point publishBucketStamp;
    bool willPending = false;
    Message willMessage;
    //! Parsed Will Delay Interval (seconds) for this connection's Will. 0 means
    //! publish immediately on abnormal disconnect; >0 arms the Will on the
    //! session when the connection closes (so the delay counts from the close,
    //! MQTT-3.1.3.2), and the sweeper publishes it after the delay (cancellable
    //! by a reconnect). See handleConnect and run().
    uint32_t willDelay = 0;
    //! Parsed Will Message Expiry Interval. The Will is published like any other
    //! Application Message (MQTT-3.3.2-6 / MQTT-3.3.2-9), so its lifetime has to
    //! be tracked as an absolute deadline to stop delivering it once stale. The
    //! interval is kept here and stamped onto the Message only at publication
    //! time (publishWill), because the Will Delay Interval must not be subtracted
    //! from it.
    bool willHasExpiry = false;
    uint32_t willExpiryInterval = 0;
    std::function<void()> disconnectCallback;
    std::unique_ptr<SecureTransport> transport;
    static const int BUFFER_SIZE = 4096;
    uint8_t buffer[BUFFER_SIZE];
};
} // namespace MQTT

#endif
