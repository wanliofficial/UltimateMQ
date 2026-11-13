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
};

class Session : public std::enable_shared_from_this<Session> {
public:
    Session(Broker* broker, const std::string& clientId, bool cleanStart = true);
    ~Session();

    const std::string& getClientId() const { return clientId; }
    bool isConnected() const { return connected; }
    bool cleanStartRequested() const { return cleanStart; }

    //! Register with the broker (keeps a shared reference to this session)
    void connect(const std::shared_ptr<Session>& self);
    void disconnect();
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
    void setDisconnectCallback(std::function<void()> callback);
    //! Called when the client id reconnects elsewhere; the old connection must stop using the session
    void setTakeoverCallback(std::function<void()> callback);

    //! Queue a message for this session (thread safe)
    void deliver(const std::string& topic, const Message& message);
    //! Queue the retained messages matching a topic filter (honours the retain handling option)
    void deliverRetained(const std::string& topicFilter, const SubscriptionOptions& options);
    //! Hand the queued deliveries to the bound client; must be called without holding any lock
    void flush();
    //! The subscription filter of this session that matches \p topic (or the topic itself)
    std::string matchingFilter(const std::string &topic);

private:
    struct Delivery {
        Message message;
        uint16_t packetId;
        QoS qos;

        Delivery(Message message, uint16_t packetId, QoS qos)
            : message(std::move(message)), packetId(packetId), qos(qos) {}
    };

    std::string clientId;
    bool connected = false;
    bool cleanStart = true;
    uint16_t packetId = 1;
    std::set<uint16_t> awaitingPubrel;
    std::map<std::string, SubscriptionOptions> subscriptions;
    std::map<uint16_t, std::shared_ptr<Message>> inflightMessages;
    std::vector<Delivery> pendingDeliveries;
    std::shared_ptr<Client> client;

    std::recursive_mutex deliveryMutex;
    std::function<void()> onDisconnect;
    std::function<void()> onTakeover;

    void pushDelivery(const Message& message, QoS qos);
    void dropOnDelivery();
    uint16_t nextPacketId();

    Broker* broker;
    // Guarded by the broker mutex: prevents a second connect() from replacing
    // the session that is currently registered under the same client id
    std::atomic<bool> registered{false};
    std::weak_ptr<Session> selfRef;
    bool newSubscription = true;
};

}

#endif
