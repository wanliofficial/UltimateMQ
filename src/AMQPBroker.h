#ifndef AMQP_BROKER_H
#define AMQP_BROKER_H

#include "AMQPWire.h"
#include "Auth.h"
#include <atomic>
#include <chrono>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <mutex>
#include <set>
#include <string>
#include <vector>

namespace MQTT {
class TLSContext;
}

namespace AMQP {

class Connection;
class Store;

struct BrokerError : std::runtime_error {
    uint16_t code;
    BrokerError(uint16_t replyCode, const std::string& message)
        : std::runtime_error(message), code(replyCode) {}
};

struct Message {
    std::string exchange;
    std::string routingKey;
    std::vector<uint8_t> properties;
    std::vector<uint8_t> body;
    std::shared_ptr<FieldTable> headers;
    uint64_t publisherConnectionID = 0;
    bool redelivered = false;
    std::chrono::steady_clock::time_point deadline;
    bool expires = false;
};

struct Delivery {
    std::string queue;
    std::shared_ptr<Message> message;
    std::string consumerTag;
    uint64_t deliveryTag = 0;
};

struct PublishResult {
    bool routed = false;
    size_t immediateDeliveries = 0;
};

struct TransactionPublish {
    Message message;
    bool mandatory = false;
    bool immediate = false;
};

struct TransactionAcknowledgement {
    Delivery delivery;
    bool requeue = false;
};

struct Exchange {
    std::string name;
    std::string type;
    bool durable = false;
    bool autoDelete = false;
    bool internal = false;
    bool wasBound = false;
    std::vector<uint8_t> arguments;
};

struct Queue {
    std::string name;
    bool durable = false;
    bool autoDelete = false;
    bool exclusive = false;
    uint64_t ownerConnectionID = 0;
    bool hadConsumer = false;
    size_t nextConsumer = 0;
    std::deque<std::shared_ptr<Message>> messages;
    std::vector<uint8_t> arguments;
};

struct Binding {
    std::string exchange;
    std::string queue;
    std::string routingKey;
    std::vector<uint8_t> arguments;
    std::shared_ptr<FieldTable> headerArguments;
    bool operator==(const Binding& other) const {
        return exchange == other.exchange && queue == other.queue &&
               routingKey == other.routingKey && arguments == other.arguments;
    }
};

struct PersistedMessage {
    Message message;
    int64_t expiryRemainingSeconds = -1;
    bool unacked = false;
};

struct PersistedExchange {
    std::string name;
    std::string type;
    bool durable = false;
    bool autoDelete = false;
    bool internal = false;
    std::vector<uint8_t> arguments;
};

struct PersistedQueue {
    std::string name;
    bool durable = false;
    bool autoDelete = false;
    std::vector<uint8_t> arguments;
    std::vector<PersistedMessage> messages;
};

struct PersistedBinding {
    std::string exchange;
    std::string queue;
    std::string routingKey;
    std::vector<uint8_t> arguments;
};

struct PersistedState {
    std::vector<PersistedExchange> exchanges;
    std::vector<PersistedQueue> queues;
    std::vector<PersistedBinding> bindings;
    uint64_t nextQueueID = 1;
};

struct Consumer {
    std::weak_ptr<Connection> connection;
    uint16_t channel = 0;
    std::string queue;
    std::string tag;
    bool noAck = false;
    bool noLocal = false;
    bool exclusive = false;
};

class Broker {
public:
    explicit Broker(MQTT::Auth* auth = nullptr,
                    size_t maxQueues = 4096,
                    size_t maxMessageBytes = 64 * 1024 * 1024,
                    Store* persistence = nullptr);
    ~Broker() = default;

    bool authenticate(const std::string& username, const std::string& password) const;
    uint64_t registerConnection(const std::shared_ptr<Connection>& connection);
    void setUsername(uint64_t connectionID, const std::string& username);
    void unregisterConnection(uint64_t connectionID);
    std::string username(uint64_t connectionID) const;

    void declareExchange(const std::string& name, const std::string& type,
                         bool passive, bool durable, bool autoDelete, bool internal,
                         uint64_t connectionID,
                         const std::vector<uint8_t>& arguments = {});
    void deleteExchange(const std::string& name, bool ifUnused,
                        uint64_t connectionID);

    std::string declareQueue(const std::string& name, bool passive, bool durable,
                             bool exclusive, bool autoDelete, uint64_t connectionID,
                             const std::vector<uint8_t>& arguments = {});
    void bindQueue(const std::string& queue, const std::string& exchange,
                   const std::string& routingKey, uint64_t connectionID,
                   const std::vector<uint8_t>& arguments = {});
    void unbindQueue(const std::string& queue, const std::string& exchange,
                     const std::string& routingKey, uint64_t connectionID,
                     const std::vector<uint8_t>& arguments = {});
    size_t purgeQueue(const std::string& queue, uint64_t connectionID);
    size_t deleteQueue(const std::string& queue, bool ifUnused, bool ifEmpty,
                       uint64_t connectionID);
    uint32_t queueMessageCount(const std::string& queue) const;
    uint32_t queueConsumerCount(const std::string& queue) const;

    std::string consume(const std::shared_ptr<Connection>& connection,
                        uint16_t channel, const std::string& queue,
                        const std::string& requestedTag, bool noLocal, bool noAck,
                        bool exclusive, uint64_t connectionID);
    void cancel(const std::shared_ptr<Connection>& connection, uint16_t channel,
                const std::string& tag, uint64_t connectionID);
    void removeChannel(const std::shared_ptr<Connection>& connection,
                       uint16_t channel, uint64_t connectionID);

    PublishResult publish(const Message& message, const std::string& username,
                          bool mandatory, bool immediate, uint64_t connectionID,
                          bool synchronize = false);
    void dispatch();
    bool flush();
    bool commitTransaction(const std::vector<TransactionPublish>& publishes,
                           const std::vector<TransactionAcknowledgement>& acknowledgements,
                           const std::string& username,
                           std::vector<PublishResult>& results);
    void failNextFlushForTest() { failNextFlush = true; }
    PersistedState snapshotState() const;
    void restoreState(const PersistedState& state);
    std::shared_ptr<Message> get(const std::string& queue, uint64_t connectionID,
                                 bool noAck = false,
                                 const std::function<uint64_t(const std::shared_ptr<Message>&)>&
                                     reserve = {});
    void acknowledge(const std::shared_ptr<Connection>& connection, uint16_t channel,
                     const std::function<std::vector<Delivery>()>& extractDeliveries,
                     bool requeue, uint64_t connectionID);
    void recover(const std::shared_ptr<Connection>& connection, uint16_t channel,
                 const std::function<std::vector<Delivery>()>& extractDeliveries,
                 bool requeue, uint64_t connectionID);
    void requeue(const Delivery& delivery);
    void requeueFailedDelivery(const std::shared_ptr<Connection>& connection,
                               uint16_t channel, uint64_t deliveryTag);

    static bool topicMatches(const std::string& pattern, const std::string& key);

private:
    struct Task {
        std::shared_ptr<Connection> connection;
        uint16_t channel = 0;
        std::string consumerTag;
        std::shared_ptr<Message> message;
        uint64_t deliveryTag = 0;
        bool noAck = false;
    };

    MQTT::Auth* auth;
    Store* store;
    size_t maxQueues;
    size_t maxMessageBytes;
    mutable std::mutex mutex;
    std::mutex dispatchMutex;
    std::map<std::string, Exchange> exchanges;
    std::map<std::string, Queue> queues;
    std::vector<Binding> bindings;
    std::map<std::pair<uint16_t, std::string>, Consumer> consumers;
    std::map<uint64_t, std::weak_ptr<Connection>> connections;
    std::map<uint64_t, std::string> usernames;
    std::atomic<uint64_t> nextConnectionID{1};
    std::atomic<uint64_t> nextQueueID{1};
    bool failNextFlush = false;

    std::vector<Task> dispatchLocked();
    void sendTasks(const std::vector<Task>& tasks);
    bool canDeliverLocked(const Consumer& consumer, size_t bytes) const;
    static bool matches(const Exchange& exchange, const Binding& binding,
                        const Message& message);
    static bool fieldValueEquals(const FieldValue& left, const FieldValue& right);
    static bool expired(const std::shared_ptr<Message>& message);
    void markDirtyLocked();
    void routeLocked(const Message& input, std::shared_ptr<Message>& message,
                     std::set<std::string>& routed, bool& durableRouted);
    bool hasImmediateConsumerLocked(const std::string& queue, const Message& message) const;
    bool removeImmediateLocked(const std::shared_ptr<Message>& message,
                               const std::set<std::string>& routed);
    bool flushLocked(const std::set<const Message*>* excludedUnacked = nullptr);
    PersistedState snapshotStateLocked(const std::set<const Message*>* excludedUnacked = nullptr) const;
    void requeueLocked(const Delivery& delivery);
    void removeBindingsLocked(const std::string& queue);
    void eraseConsumersLocked(const std::string& queue);
    void maybeAutoDeleteLocked(const std::string& queue);
    void maybeAutoDeleteExchangesLocked();
    void authorizeQueueReadLocked(const std::string& queue, uint64_t connectionID);
    uint32_t queueConsumerCountLocked(const std::string& name) const;
};

} // namespace AMQP

#endif
