#ifndef AMQP_CONNECTION_H
#define AMQP_CONNECTION_H

#include "AMQPBroker.h"
#include "TLS.h"
#include <atomic>
#include <chrono>
#include <cstdint>
#include <map>
#include <memory>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <vector>

namespace AMQP {

struct ChannelError : std::runtime_error {
    uint16_t code;
    uint16_t classID;
    uint16_t methodID;
    ChannelError(uint16_t replyCode, const std::string& message,
                 uint16_t failedClass, uint16_t failedMethod)
        : std::runtime_error(message), code(replyCode), classID(failedClass),
          methodID(failedMethod) {}
};

struct ConnectionError : std::runtime_error {
    uint16_t code;
    ConnectionError(uint16_t replyCode, const std::string& message)
        : std::runtime_error(message), code(replyCode) {}
};

class Connection : public std::enable_shared_from_this<Connection> {
public:
    Connection(int socket, Broker* broker, std::shared_ptr<MQTT::TLSContext> TLS,
               int writeTimeoutMs, size_t maxMessageBytes);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void run();
    void close();
    uint64_t connectionID() const { return id; }

    bool canDeliver(uint16_t channel, const std::string& consumerTag,
                    size_t bytes, bool noAck = false) const;
    uint64_t reserveDelivery(uint16_t channel, const std::string& queue,
                             const std::shared_ptr<Message>& message,
                             const std::string& consumerTag,
                             bool track = true, bool enforceQoS = true);
    bool sendDelivery(uint16_t channel, const std::string& consumerTag,
                      const std::shared_ptr<Message>& message,
                      uint64_t deliveryTag, uint16_t classID,
                      uint16_t methodID);
    void discardQueue(const std::string& queue);
    std::optional<Delivery> discardDelivery(uint16_t channel, uint64_t deliveryTag);
    std::vector<Delivery> snapshotUnacked() const;

private:
    enum class Phase { PROTOCOL, START, TUNE, OPEN, CLOSED };
    struct StagedPublish {
        Message message;
        bool mandatory = false;
        bool immediate = false;
    };
    struct TransactionAck {
        Delivery delivery;
        bool requeue = false;
    };
    struct Channel {
        bool open = false;
        bool closing = false;
        bool flowActive = true;
        uint32_t prefetchSize = 0;
        uint16_t prefetchCount = 0;
        bool globalQoS = false;
        uint64_t nextDeliveryTag = 1;
        uint64_t unackedBytes = 0;
        std::map<std::string, uint16_t> consumerUnacked;
        std::map<std::string, size_t> consumerUnackedBytes;
        std::map<std::string, std::string> consumerQueues;
        std::map<uint64_t, Delivery> unacked;
        bool confirm = false;
        bool transactional = false;
        uint64_t nextPublishSequence = 1;
        std::vector<StagedPublish> transactionPublishes;
        std::map<uint64_t, TransactionAck> transactionAcks;
    };
    struct PendingPublish {
        uint16_t channel = 0;
        std::string exchange;
        std::string routingKey;
        bool mandatory = false;
        bool immediate = false;
        uint64_t bodySize = 0;
        uint64_t received = 0;
        Message message;
    };

    int socket;
    Broker* broker;
    std::shared_ptr<MQTT::TLSContext> TLS;
    std::unique_ptr<MQTT::SecureTransport> transport;
    int writeTimeoutMs;
    size_t maxMessageBytes;
    uint64_t id = 0;
    Phase phase = Phase::PROTOCOL;
    bool opened = false;
    bool awaitingClose = false;
    std::chrono::steady_clock::time_point closeDeadline;
    std::set<uint16_t> closingChannels;
    std::atomic<bool> closed{false};
    std::atomic<bool> cleaned{false};
    std::string user;
    std::string virtualHost = "/";
    uint16_t proposedChannelMax = 2047;
    uint32_t proposedFrameMax = DEFAULT_FRAME_MAX;
    uint16_t proposedHeartbeat = 60;
    uint16_t channelMax = 2047;
    uint32_t frameMax = DEFAULT_FRAME_MAX;
    uint16_t heartbeat = 60;
    std::chrono::steady_clock::time_point lastActivity;
    std::chrono::steady_clock::time_point lastWrite;
    mutable std::mutex stateMutex;
    std::map<uint16_t, Channel> channels;
    std::map<uint16_t, PendingPublish> pendingPublishes;
    std::mutex writeMutex;

    bool handshake();
    bool readExact(uint8_t* data, size_t size, std::chrono::milliseconds timeoutMs);
    std::optional<Frame> readFrame();
    bool writeFrame(const std::vector<uint8_t>& frame);
    bool sendMethod(uint16_t classID, uint16_t methodID,
                    const std::vector<uint8_t>& arguments, uint16_t channel);
    bool sendContent(uint16_t channel, uint16_t classID, uint16_t methodID,
                     const std::vector<uint8_t>& arguments,
                     const Message& message);
    bool sendClose(uint16_t code, const std::string& text,
                   uint16_t classID = 0, uint16_t methodID = 0,
                   uint16_t channel = 0);
    void awaitClose();
    void finishChannelClose(uint16_t channel);
    void handleFrame(const Frame& frame);
    void handleMethod(const Method& method, uint16_t channel);
    void failChannel(uint16_t channel, const ChannelError& error);
    void handleHeader(const Frame& frame);
    void handleBody(const Frame& frame);
    void completePublish(uint16_t channel, PendingPublish& pending);
    bool publishNow(uint16_t channel, Message& message, bool mandatory, bool immediate,
                    uint64_t confirmSequence);
    bool sendReturn(uint16_t channel, uint16_t code, const std::string& text,
                    const Message& message);
    void handleConfirm(const Method& method, uint16_t channel);
    void handleTX(const Method& method, uint16_t channel);
    void handleAcknowledgement(uint16_t channel, uint64_t tag, bool multiple,
                               bool requeue);
    Channel& requireChannel(uint16_t channel, uint16_t classID, uint16_t methodID);
    Channel* findOpenChannel(uint16_t channel);
    bool deliveryAllowedLocked(const Channel& state,
                               const std::string& consumerTag,
                               size_t bytes) const;
    void releaseDeliveryLocked(Channel& state, const Delivery& delivery);
    void handleConnection(const Method& method);
    void handleChannelClass(const Method& method, uint16_t channel);
    void handleExchange(const Method& method, uint16_t channel);
    void handleQueue(const Method& method, uint16_t channel);
    void handleBasic(const Method& method, uint16_t channel);
    void checkWritePermission(const std::string& topic, uint16_t channel,
                              uint16_t classID, uint16_t methodID);
    void checkReadPermission(const std::string& topic, uint16_t channel,
                             uint16_t classID, uint16_t methodID);
    std::vector<Delivery> takeUnacked(uint16_t channel, uint64_t tag,
                                      bool multiple);
    std::vector<Delivery> peekUnacked(uint16_t channel, uint64_t tag, bool multiple) const;
    std::vector<Delivery> allUnacked(uint16_t channel);
    std::vector<Delivery> allUnacked();
    void cleanup();
};

} // namespace AMQP

#endif
