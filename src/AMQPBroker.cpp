#include "AMQPBroker.h"
#include "AMQPConnection.h"
#include <algorithm>
#include <charconv>
#include <functional>
#include <limits>

namespace AMQP {
namespace {
constexpr uint16_t ACCESS_REFUSED = 403;
constexpr uint16_t NOT_FOUND = 404;
constexpr uint16_t RESOURCE_LOCKED = 405;
constexpr uint16_t PRECONDITION_FAILED = 406;
constexpr uint16_t NOT_IMPLEMENTED = 540;
} // namespace

Broker::Broker(MQTT::Auth* authHandle, size_t maxQueuesValue,
               size_t maxMessageBytesValue)
    : auth(authHandle), maxQueues(maxQueuesValue), maxMessageBytes(maxMessageBytesValue) {
    exchanges.emplace("", Exchange{"", "direct", true, false, false, false});
    exchanges.emplace("amq.direct", Exchange{"amq.direct", "direct", true, false, false, false});
    exchanges.emplace("amq.fanout", Exchange{"amq.fanout", "fanout", true, false, false, false});
    exchanges.emplace("amq.topic", Exchange{"amq.topic", "topic", true, false, false, false});
}

bool Broker::authenticate(const std::string& username, const std::string& password) const {
    return !auth || auth->authenticate(username, password);
}

uint64_t Broker::registerConnection(const std::shared_ptr<Connection>& connection) {
    uint64_t id = nextConnectionID.fetch_add(1);
    std::lock_guard<std::mutex> lock(mutex);
    connections[id] = connection;
    usernames[id] = "";
    return id;
}

void Broker::setUsername(uint64_t connectionID, const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex);
    usernames[connectionID] = username;
}

std::string Broker::username(uint64_t connectionID) const {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = usernames.find(connectionID);
    return it == usernames.end() ? std::string() : it->second;
}

void Broker::unregisterConnection(uint64_t connectionID) {
    std::lock_guard<std::mutex> lock(mutex);
    connections.erase(connectionID);
    usernames.erase(connectionID);
    for (auto it = consumers.begin(); it != consumers.end();) {
        if (it->second.connection.expired()) {
            it = consumers.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = queues.begin(); it != queues.end();) {
        if (it->second.exclusive && it->second.ownerConnectionID == connectionID) {
            removeBindingsLocked(it->first);
            eraseConsumersLocked(it->first);
            it = queues.erase(it);
        } else {
            ++it;
        }
    }
    for (auto it = queues.begin(); it != queues.end();) {
        if (it->second.autoDelete && it->second.hadConsumer &&
            queueConsumerCountLocked(it->first) == 0) {
            removeBindingsLocked(it->first);
            eraseConsumersLocked(it->first);
            it = queues.erase(it);
        } else {
            ++it;
        }
    }
    maybeAutoDeleteExchangesLocked();
}

void Broker::declareExchange(const std::string& name, const std::string& type,
                             bool passive, bool durable, bool autoDelete,
                             bool internal, uint64_t,
                             const std::vector<uint8_t>& arguments) {
    if (!name.empty() && name.rfind("amq.", 0) == 0) {
        auto builtin = exchanges.find(name);
        if (builtin == exchanges.end()) {
            throw BrokerError(ACCESS_REFUSED, "reserved exchange name");
        }
        if (!passive && (builtin->second.type != type || builtin->second.durable != durable ||
                         builtin->second.internal != internal ||
                         builtin->second.arguments != arguments)) {
            throw BrokerError(PRECONDITION_FAILED, "built-in exchange declaration mismatch");
        }
        return;
    }
    std::lock_guard<std::mutex> lock(mutex);
    auto it = exchanges.find(name);
    if (it != exchanges.end()) {
        if (passive) {
            return;
        }
        if (it->second.type != type) {
            throw BrokerError(PRECONDITION_FAILED, "exchange type mismatch");
        }
        if (it->second.durable != durable || it->second.autoDelete != autoDelete ||
            it->second.internal != internal || it->second.arguments != arguments) {
            throw BrokerError(PRECONDITION_FAILED, "exchange declaration mismatch");
        }
        return;
    }
    if (passive) {
        throw BrokerError(NOT_FOUND, "exchange not found");
    }
    if (type.empty()) {
        throw BrokerError(PRECONDITION_FAILED, "exchange type is empty");
    }
    if (type != "direct" && type != "fanout" && type != "topic") {
        throw BrokerError(NOT_IMPLEMENTED, "unsupported exchange type");
    }
    if (exchanges.size() >= maxQueues) {
        throw BrokerError(RESOURCE_LOCKED, "exchange limit reached");
    }
    exchanges.emplace(name, Exchange{name, type, durable, autoDelete, internal, false,
                                     arguments});
}

void Broker::deleteExchange(const std::string& name, bool ifUnused, uint64_t) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = exchanges.find(name);
    if (it == exchanges.end()) {
        throw BrokerError(NOT_FOUND, "exchange not found");
    }
    if (name.empty() || name.rfind("amq.", 0) == 0) {
        throw BrokerError(ACCESS_REFUSED, "cannot delete a built-in exchange");
    }
    bool used = std::any_of(bindings.begin(), bindings.end(),
                            [&](const Binding& binding) { return binding.exchange == name; });
    if (ifUnused && used) {
        throw BrokerError(PRECONDITION_FAILED, "exchange is in use");
    }
    bindings.erase(std::remove_if(bindings.begin(), bindings.end(),
                                  [&](const Binding& binding) { return binding.exchange == name; }),
                   bindings.end());
    exchanges.erase(it);
}

std::string Broker::declareQueue(const std::string& requestedName, bool passive,
                                 bool durable, bool exclusive, bool autoDelete,
                                 uint64_t connectionID,
                                 const std::vector<uint8_t>& arguments) {
    std::lock_guard<std::mutex> lock(mutex);
    std::string name = requestedName;
    if (name.empty() && !passive) {
        name = "amq.gen-" + std::to_string(nextQueueID.fetch_add(1));
    }
    auto it = queues.find(name);
    if (it != queues.end()) {
        if (it->second.exclusive && it->second.ownerConnectionID != connectionID) {
            throw BrokerError(RESOURCE_LOCKED, "exclusive queue belongs to another connection");
        }
        if (passive) {
            return name;
        }
        // queue.declare.equivalence: durable, exclusive, auto-delete and arguments
        // must match an existing queue (Errata 6).
        if (it->second.durable != durable || it->second.exclusive != exclusive ||
            it->second.autoDelete != autoDelete || it->second.arguments != arguments) {
            throw BrokerError(PRECONDITION_FAILED, "queue declaration mismatch");
        }
        return name;
    }
    if (passive) {
        throw BrokerError(NOT_FOUND, "queue not found");
    }
    if (!requestedName.empty() && requestedName.rfind("amq.", 0) == 0) {
        throw BrokerError(ACCESS_REFUSED, "reserved queue name");
    }
    if (queues.size() >= maxQueues) {
        throw BrokerError(RESOURCE_LOCKED, "queue limit reached");
    }
    queues.emplace(name, Queue{name, durable, autoDelete, exclusive, connectionID,
                               false, 0, {}, arguments});
    return name;
}

void Broker::bindQueue(const std::string& queueName, const std::string& exchangeName,
                       const std::string& routingKey, uint64_t connectionID) {
    std::lock_guard<std::mutex> lock(mutex);
    authorizeQueueReadLocked(queueName, connectionID);
    auto queue = queues.find(queueName);
    if (queue == queues.end()) {
        throw BrokerError(NOT_FOUND, "queue not found");
    }
    auto exchange = exchanges.find(exchangeName);
    if (exchange == exchanges.end()) {
        throw BrokerError(NOT_FOUND, "exchange not found");
    }
    if (queue->second.exclusive && queue->second.ownerConnectionID != connectionID) {
        throw BrokerError(RESOURCE_LOCKED, "exclusive queue belongs to another connection");
    }
    Binding binding{exchangeName, queueName, routingKey};
    if (std::find(bindings.begin(), bindings.end(), binding) == bindings.end()) {
        bindings.push_back(std::move(binding));
        exchange->second.wasBound = true;
    }
}

void Broker::unbindQueue(const std::string& queue, const std::string& exchange,
                         const std::string& routingKey) {
    std::lock_guard<std::mutex> lock(mutex);
    if (queues.find(queue) == queues.end() || exchanges.find(exchange) == exchanges.end()) {
        throw BrokerError(NOT_FOUND, "queue or exchange not found");
    }
    Binding binding{exchange, queue, routingKey};
    bindings.erase(std::remove_if(bindings.begin(), bindings.end(),
                                  [&](const Binding& item) { return item.exchange == binding.exchange &&
                                                                   item.queue == binding.queue &&
                                                                   item.routingKey == binding.routingKey; }),
                   bindings.end());
    auto exchangeIt = exchanges.find(exchange);
    maybeAutoDeleteExchangesLocked();
}

size_t Broker::purgeQueue(const std::string& name, uint64_t connectionID) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = queues.find(name);
    if (it == queues.end()) {
        throw BrokerError(NOT_FOUND, "queue not found");
    }
    if (it->second.exclusive && it->second.ownerConnectionID != connectionID) {
        throw BrokerError(RESOURCE_LOCKED, "exclusive queue belongs to another connection");
    }
    size_t count = it->second.messages.size();
    it->second.messages.clear();
    return count;
}

uint32_t Broker::queueMessageCount(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = queues.find(name);
    if (it == queues.end()) {
        throw BrokerError(NOT_FOUND, "queue not found");
    }
    return static_cast<uint32_t>(it->second.messages.size());
}

uint32_t Broker::queueConsumerCount(const std::string& name) const {
    std::lock_guard<std::mutex> lock(mutex);
    return queueConsumerCountLocked(name);
}

uint32_t Broker::queueConsumerCountLocked(const std::string& name) const {
    size_t count = 0;
    for (const auto& entry : consumers) {
        if (entry.second.queue == name && !entry.second.connection.expired()) {
            ++count;
        }
    }
    return static_cast<uint32_t>(count);
}

size_t Broker::deleteQueue(const std::string& name, bool ifUnused, bool ifEmpty,
                           uint64_t connectionID) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = queues.find(name);
    if (it == queues.end()) {
        throw BrokerError(NOT_FOUND, "queue not found");
    }
    if (it->second.exclusive && it->second.ownerConnectionID != connectionID) {
        throw BrokerError(RESOURCE_LOCKED, "exclusive queue belongs to another connection");
    }
    if (ifUnused && queueConsumerCountLocked(name) != 0) {
        throw BrokerError(PRECONDITION_FAILED, "queue is in use");
    }
    if (ifEmpty && !it->second.messages.empty()) {
        throw BrokerError(PRECONDITION_FAILED, "queue is not empty");
    }
    size_t count = it->second.messages.size();
    std::vector<std::shared_ptr<Connection>> affected;
    for (const auto& entry : connections) {
        auto connection = entry.second.lock();
        if (connection) {
            affected.push_back(std::move(connection));
        }
    }
    for (const auto& connection : affected) {
        connection->discardQueue(name);
    }
    removeBindingsLocked(name);
    eraseConsumersLocked(name);
    maybeAutoDeleteExchangesLocked();
    queues.erase(it);
    return count;
}

std::string Broker::consume(const std::shared_ptr<Connection>& connection,
                            uint16_t channel, const std::string& queue,
                            const std::string& requestedTag, bool noLocal,
                            bool noAck, bool exclusive, uint64_t connectionID) {
    std::lock_guard<std::mutex> lock(mutex);
    auto queueIt = queues.find(queue);
    if (queueIt == queues.end()) {
        throw BrokerError(NOT_FOUND, "queue not found");
    }
    authorizeQueueReadLocked(queue, connectionID);
    if (queueIt->second.exclusive && queueIt->second.ownerConnectionID != connectionID) {
        throw BrokerError(RESOURCE_LOCKED, "exclusive queue belongs to another connection");
    }
    std::string tag = requestedTag;
    if (tag.empty()) {
        tag = "ctag-" + std::to_string(nextQueueID.fetch_add(1));
    }
    if (consumers.count({channel, tag})) {
        throw BrokerError(PRECONDITION_FAILED, "consumer tag already in use");
    }
    if (exclusive && queueConsumerCountLocked(queue) != 0) {
        throw BrokerError(ACCESS_REFUSED, "queue has exclusive consumers");
    }
    if (queueIt->second.hadConsumer) {
        for (const auto& entry : consumers) {
            if (entry.second.queue == queue && entry.second.exclusive &&
                entry.second.connection.lock() != connection) {
                throw BrokerError(ACCESS_REFUSED, "queue has an exclusive consumer");
            }
        }
    }
    consumers.emplace(std::make_pair(channel, tag),
                      Consumer{connection, channel, queue, tag, noAck, noLocal,
                               exclusive});
    queueIt->second.hadConsumer = true;
    return tag;
}

void Broker::cancel(const std::shared_ptr<Connection>& connection, uint16_t channel,
                    const std::string& tag, uint64_t) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = consumers.find({channel, tag});
    if (it == consumers.end() || it->second.connection.lock() != connection) {
        throw BrokerError(NOT_FOUND, "consumer not found");
    }
    std::string queue = it->second.queue;
    consumers.erase(it);
    maybeAutoDeleteLocked(queue);
}

void Broker::removeChannel(const std::shared_ptr<Connection>& connection,
                           uint16_t channel, uint64_t) {
    std::lock_guard<std::mutex> lock(mutex);
    for (auto it = consumers.begin(); it != consumers.end();) {
        if (it->first.first == channel && it->second.connection.lock() == connection) {
            std::string queue = it->second.queue;
            it = consumers.erase(it);
            maybeAutoDeleteLocked(queue);
        } else {
            ++it;
        }
    }
}

bool Broker::matches(const Exchange& exchange, const Binding& binding,
                     const Message& message) {
    if (exchange.type == "fanout") {
        return true;
    }
    if (exchange.type == "direct") {
        return binding.routingKey == message.routingKey;
    }
    return exchange.type == "topic" && topicMatches(binding.routingKey, message.routingKey);
}

bool Broker::topicMatches(const std::string& pattern, const std::string& key) {
    // §3.1.3.3: '*' matches one dot-delimited word and '#' matches zero or
    // more; wildcards embedded in a literal word have no special meaning.
    auto words = [](const std::string& value) {
        std::vector<std::string> result;
        if (value.empty()) return result;
        size_t start = 0;
        while (true) {
            size_t end = value.find('.', start);
            if (end == std::string::npos) {
                result.push_back(value.substr(start));
                break;
            }
            result.push_back(value.substr(start, end - start));
            start = end + 1;
        }
        return result;
    };
    const auto patternWords = words(pattern);
    const auto keyWords = words(key);
    std::vector<int8_t> memo((patternWords.size() + 1) * (keyWords.size() + 1), -1);
    std::function<bool(size_t, size_t)> match = [&](size_t p, size_t k) -> bool {
        int8_t& cached = memo[p * (keyWords.size() + 1) + k];
        if (cached != -1) return cached != 0;
        bool result;
        if (p == patternWords.size()) {
            result = k == keyWords.size();
        } else if (patternWords[p] == "#") {
            result = match(p + 1, k) || (k < keyWords.size() && match(p, k + 1));
        } else if (patternWords[p] == "*") {
            result = k < keyWords.size() && match(p + 1, k + 1);
        } else {
            result = k < keyWords.size() && patternWords[p] == keyWords[k] &&
                     match(p + 1, k + 1);
        }
        cached = result ? 1 : 0;
        return result;
    };
    return match(0, 0);
}

bool Broker::expired(const std::shared_ptr<Message>& message) {
    return message->expires &&
           std::chrono::steady_clock::now() >= message->deadline;
}

bool Broker::canDeliverLocked(const Consumer& consumer, size_t bytes) const {
    auto connection = consumer.connection.lock();
    return connection && connection->canDeliver(consumer.channel, consumer.tag, bytes,
                                               consumer.noAck);
}

void Broker::requeueLocked(const Delivery& delivery) {
    auto queue = queues.find(delivery.queue);
    if (queue == queues.end()) {
        return;
    }
    delivery.message->redelivered = true;
    queue->second.messages.push_front(delivery.message);
}

PublishResult Broker::publish(const Message& input, const std::string& username,
                              bool mandatory, bool immediate,
                              uint64_t connectionID) {
    (void)mandatory;
    if (input.body.size() > maxMessageBytes) {
        throw BrokerError(RESOURCE_LOCKED, "message exceeds broker limit");
    }
    if (auth && auth->isForbidden(username, input.routingKey, true)) {
        throw BrokerError(ACCESS_REFUSED, "publish denied by ACL");
    }
    std::unique_lock<std::mutex> dispatch(dispatchMutex);
    std::unique_lock<std::mutex> lock(mutex);
    auto exchangeIt = exchanges.find(input.exchange);
    if (exchangeIt == exchanges.end()) {
        throw BrokerError(NOT_FOUND, "exchange not found");
    }
    if (exchangeIt->second.internal) {
        throw BrokerError(ACCESS_REFUSED, "cannot publish to an internal exchange");
    }
    auto message = std::make_shared<Message>(input);
    PublishResult result;
    std::set<std::string> routed;
    if (input.exchange.empty()) {
        auto queue = queues.find(input.routingKey);
        if (queue != queues.end()) {
            queue->second.messages.push_back(message);
            routed.insert(queue->first);
        }
    } else {
        for (const auto& binding : bindings) {
            if (binding.exchange == input.exchange &&
                matches(exchangeIt->second, binding, *message) &&
                queues.count(binding.queue)) {
                queues.at(binding.queue).messages.push_back(message);
                routed.insert(binding.queue);
            }
        }
    }
    result.routed = !routed.empty();
    std::vector<Task> tasks;
    bool progress = true;
    while (progress) {
        progress = false;
        for (auto& [name, queue] : queues) {
            (void)name;
            while (!queue.messages.empty()) {
                auto item = queue.messages.front();
                if (expired(item)) {
                    queue.messages.pop_front();
                    progress = true;
                    continue;
                }
                std::vector<Consumer> candidates;
                for (const auto& entry : consumers) {
                    const Consumer& candidate = entry.second;
                    if (candidate.queue == queue.name) {
                        auto owner = candidate.connection.lock();
                        if (owner && (!candidate.noLocal ||
                                      owner->connectionID() != item->publisherConnectionID)) {
                            candidates.push_back(candidate);
                        }
                    }
                }
                std::shared_ptr<Consumer> selected;
                Consumer selectedCopy;
                if (!candidates.empty()) {
                    size_t start = queue.nextConsumer % candidates.size();
                    for (size_t n = 0; n < candidates.size(); ++n) {
                        const Consumer& candidate = candidates[(start + n) % candidates.size()];
                        if (canDeliverLocked(candidate, item->body.size())) {
                            selectedCopy = candidate;
                            selected = std::make_shared<Consumer>(candidate);
                            queue.nextConsumer = (start + n + 1) % candidates.size();
                            break;
                        }
                    }
                }
                if (!selected) {
                    break;
                }
                auto connection = selected->connection.lock();
                if (!connection) {
                    break;
                }
                uint64_t deliveryTag = connection->reserveDelivery(
                    selected->channel, queue.name, item, selected->tag,
                    !selected->noAck, !selected->noAck);
                if (deliveryTag == 0) {
                    break;
                }
                queue.messages.pop_front();
                tasks.push_back(Task{connection, selected->channel, selected->tag,
                                     item, deliveryTag, selected->noAck});
                result.immediateDeliveries += immediate && item == message ? 1 : 0;
                progress = true;
            }
        }
    }
    if (immediate) {
        for (const std::string& queueName : routed) {
            auto& queued = queues.at(queueName).messages;
            queued.erase(std::remove_if(queued.begin(), queued.end(),
                                        [&](const std::shared_ptr<Message>& item) {
                                            return item == message;
                                        }),
                         queued.end());
        }
    }
    lock.unlock();
    dispatch.unlock();
    sendTasks(tasks);
    return result;
}

void Broker::dispatch() {
    std::unique_lock<std::mutex> dispatch(dispatchMutex);
    std::unique_lock<std::mutex> lock(mutex);
    auto tasks = dispatchLocked();
    lock.unlock();
    dispatch.unlock();
    sendTasks(tasks);
}

std::shared_ptr<Message> Broker::get(const std::string& queueName,
                                     uint64_t connectionID, bool) {
    std::lock_guard<std::mutex> lock(mutex);
    auto it = queues.find(queueName);
    if (it == queues.end()) {
        throw BrokerError(NOT_FOUND, "queue not found");
    }
    authorizeQueueReadLocked(queueName, connectionID);
    if (it->second.exclusive && it->second.ownerConnectionID != connectionID) {
        throw BrokerError(RESOURCE_LOCKED, "exclusive queue belongs to another connection");
    }
    while (!it->second.messages.empty()) {
        auto message = it->second.messages.front();
        it->second.messages.pop_front();
        if (!expired(message)) {
            return message;
        }
    }
    return nullptr;
}

void Broker::acknowledge(const std::shared_ptr<Connection>& connection,
                         uint16_t channel, const std::vector<Delivery>& deliveries,
                         bool requeue, uint64_t connectionID) {
    (void)connection;
    (void)channel;
    (void)connectionID;
    std::unique_lock<std::mutex> dispatch(dispatchMutex);
    std::unique_lock<std::mutex> lock(mutex);
    for (const auto& delivery : deliveries) {
        if (requeue) {
            requeueLocked(delivery);
        }
    }
    auto tasks = dispatchLocked();
    lock.unlock();
    dispatch.unlock();
    sendTasks(tasks);
}

void Broker::recover(const std::shared_ptr<Connection>& connection,
                     uint16_t channel, const std::vector<Delivery>& deliveries,
                     bool requeue, uint64_t connectionID) {
    (void)channel;
    if (requeue) {
        acknowledge(connection, 0, deliveries, true, connectionID);
        return;
    }
    std::unique_lock<std::mutex> dispatch(dispatchMutex);
    std::unique_lock<std::mutex> lock(mutex);
    std::vector<Task> tasks;
    for (const auto& delivery : deliveries) {
        bool redelivered = false;
        for (const auto& entry : consumers) {
            const Consumer& candidate = entry.second;
            if (candidate.queue != delivery.queue ||
                candidate.connection.lock() != connection) {
                continue;
            }
            uint64_t tag = connection->reserveDelivery(candidate.channel, delivery.queue,
                                                       delivery.message, candidate.tag,
                                                       !candidate.noAck, !candidate.noAck);
            if (tag != 0) {
                delivery.message->redelivered = true;
                tasks.push_back(Task{connection, candidate.channel, candidate.tag,
                                     delivery.message, tag, candidate.noAck});
                redelivered = true;
                break;
            }
        }
        if (!redelivered) {
            requeueLocked(delivery);
        }
    }
    auto queued = dispatchLocked();
    tasks.insert(tasks.end(), queued.begin(), queued.end());
    lock.unlock();
    dispatch.unlock();
    sendTasks(tasks);
}

void Broker::requeue(const Delivery& delivery) {
    std::lock_guard<std::mutex> lock(mutex);
    requeueLocked(delivery);
}

std::vector<Broker::Task> Broker::dispatchLocked() {
    std::vector<Task> tasks;
    bool progress = true;
    while (progress) {
        progress = false;
        for (auto& [name, queue] : queues) {
            (void)name;
            while (!queue.messages.empty()) {
                auto item = queue.messages.front();
                if (expired(item)) {
                    queue.messages.pop_front();
                    progress = true;
                    continue;
                }
                Consumer selected;
                bool found = false;
                for (const auto& entry : consumers) {
                    const Consumer& candidate = entry.second;
                    auto owner = candidate.connection.lock();
                    if (candidate.queue == queue.name && owner &&
                        (!candidate.noLocal ||
                         owner->connectionID() != item->publisherConnectionID) &&
                        canDeliverLocked(candidate, item->body.size())) {
                        selected = candidate;
                        found = true;
                        break;
                    }
                }
                if (!found) {
                    break;
                }
                auto connection = selected.connection.lock();
                if (!connection) {
                    break;
                }
                uint64_t tag = connection->reserveDelivery(selected.channel,
                                                           queue.name, item,
                                                           selected.tag,
                                                           !selected.noAck, !selected.noAck);
                if (tag == 0) {
                    break;
                }
                queue.messages.pop_front();
                tasks.push_back(Task{connection, selected.channel, selected.tag,
                                     item, tag, selected.noAck});
                progress = true;
            }
        }
    }
    return tasks;
}

void Broker::sendTasks(const std::vector<Task>& tasks) {
    for (const auto& task : tasks) {
        if (!task.connection->sendDelivery(task.channel, task.consumerTag,
                                           task.message, task.deliveryTag,
                                           CLASS_BASIC, 60)) {
            auto delivery = task.connection->discardDelivery(task.channel,
                                                              task.deliveryTag);
            if (delivery) requeue(*delivery);
        }
    }
}

void Broker::removeBindingsLocked(const std::string& queue) {
    bindings.erase(std::remove_if(bindings.begin(), bindings.end(),
                                  [&](const Binding& binding) { return binding.queue == queue; }),
                   bindings.end());
}

void Broker::eraseConsumersLocked(const std::string& queue) {
    for (auto it = consumers.begin(); it != consumers.end();) {
        if (it->second.queue == queue) {
            it = consumers.erase(it);
        } else {
            ++it;
        }
    }
}

void Broker::authorizeQueueReadLocked(const std::string& queue,
                                      uint64_t connectionID) {
    if (!auth) return;
    auto it = usernames.find(connectionID);
    const std::string username = it == usernames.end() ? std::string() : it->second;
    if (auth->isForbiddenSubscribe(username, queue)) {
        throw BrokerError(ACCESS_REFUSED, "queue read denied by ACL");
    }
}

void Broker::maybeAutoDeleteLocked(const std::string& queueName) {
    auto it = queues.find(queueName);
    if (it != queues.end() && it->second.autoDelete && it->second.hadConsumer &&
        queueConsumerCountLocked(queueName) == 0) {
        removeBindingsLocked(queueName);
        eraseConsumersLocked(queueName);
        queues.erase(it);
    }
    maybeAutoDeleteExchangesLocked();
}

void Broker::maybeAutoDeleteExchangesLocked() {
    std::vector<std::string> remove;
    for (const auto& entry : exchanges) {
        if (!entry.second.autoDelete || !entry.second.wasBound) continue;
        bool bound = std::any_of(bindings.begin(), bindings.end(),
                                 [&](const Binding& binding) {
                                     return binding.exchange == entry.first;
                                 });
        if (!bound) remove.push_back(entry.first);
    }
    for (const std::string& name : remove) exchanges.erase(name);
}

} // namespace AMQP
