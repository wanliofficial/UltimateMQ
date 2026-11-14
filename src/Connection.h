#ifndef CONNECTION_H
#define CONNECTION_H
#pragma once

#include <sys/socket.h>
#include <chrono>
#include <cstdint>
#include <memory>
#include <mutex>
#include <atomic>
#include <functional>
#include "MQTT.h"
#include "Message.h"
#include "Frame.h"
#include "Session.h"
#include "Auth.h"
#include "Tls.h"

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
                        std::shared_ptr<TlsContext> tls = nullptr);
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

    //! Client interface: write a delivery to the socket (called without any broker/session lock)
    void deliver(const Message& message, uint16_t packetId, QoS qos) override;
    //! Client interface: resend a PUBREC for an inbound QoS 2 flow awaiting PUBREL
    void sendPubrec(uint16_t packetId) override;
    //! Client interface: a newer connection has taken over this session; stop using it
    void onSuperseded() override;
    void handleDeliver(const Message& message, uint16_t packetId, QoS qos);
    void sendPacket(Packet &packet);

private:
    void runReadLoop();
    void closeSocket();
    void publishWill();
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
    std::chrono::steady_clock::time_point lastActivity;
    uint16_t keepAliveSeconds = 0;
    std::chrono::milliseconds readTimeout{0};
    //! Set when the client sent a graceful DISCONNECT (as opposed to the read
    //! loop ending on an error or keep-alive timeout). Governs whether a
    //! zero session-expiry interval discards the session on teardown.
    bool gracefulDisconnect = false;
    //! MQTT 5 session expiry interval from a graceful DISCONNECT; 0 (the default
    //! when absent or on MQTT 3.1.1) means the session is discarded on disconnect.
    uint32_t sessionExpirySeconds = 0;
    bool willPending = false;
    Message willMessage;
    std::function<void()> disconnectCallback;
    std::unique_ptr<SecureTransport> transport;
    static const int BUFFER_SIZE = 4096;
    uint8_t buffer[BUFFER_SIZE];
};
} // namespace MQTT

#endif
