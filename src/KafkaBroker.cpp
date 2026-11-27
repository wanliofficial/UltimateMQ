#include "KafkaBroker.h"
#include "KafkaErrors.h"
#include "KafkaWire.h"
#include "Log.h"
#include <algorithm>
#include <cerrno>
#include <chrono>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <limits>
#include <random>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace Kafka {
namespace {

constexpr int32_t STATE_MAGIC = 0x4B464D34;
constexpr size_t MAX_TOPICS = 10000;
constexpr size_t MAX_PARTITIONS = 10000;
constexpr int64_t MAX_STATE_BYTES = 2LL * 1024 * 1024 * 1024;

int64_t nowMillis() {
    return std::chrono::duration_cast<std::chrono::milliseconds>(
               std::chrono::system_clock::now().time_since_epoch())
        .count();
}

bool validTopicName(const std::string& name) {
    if (name.empty() || name.size() > 249 || name == "." || name == "..") return false;
    for (unsigned char ch : name) {
        if (!((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') ||
              (ch >= '0' && ch <= '9') || ch == '.' || ch == '_' || ch == '-')) {
            return false;
        }
    }
    return true;
}

int64_t configInteger(const std::map<std::string, std::string>& configs,
                      const std::string& key, int64_t fallback) {
    auto found = configs.find(key);
    if (found == configs.end()) return fallback;
    try {
        size_t consumed = 0;
        long long value = std::stoll(found->second, &consumed);
        if (consumed != found->second.size()) throw std::invalid_argument("trailing data");
        return value;
    } catch (const std::exception&) {
        throw BrokerError(Error::INVALID_CONFIG, "invalid numeric topic configuration");
    }
}

size_t recordBytes(const Record& record) {
    size_t bytes = 64;
    if (record.key) bytes += record.key->size();
    if (record.value) bytes += record.value->size();
    for (const RecordHeader& header : record.headers) bytes += header.key.size() + header.value.size();
    return bytes;
}

std::string randomSuffix() {
    std::random_device random;
    std::ostringstream output;
    output << std::hex << random() << random();
    return output.str();
}

void writeRecord(Writer& writer, const Record& record) {
    writer.writeInt64(record.offset);
    writer.writeInt64(record.timestamp);
    writer.writeBoolean(record.key.has_value());
    writer.writeNullableBytes(record.key ? &*record.key : nullptr);
    writer.writeBoolean(record.value.has_value());
    writer.writeNullableBytes(record.value ? &*record.value : nullptr);
    writer.writeArrayLength(record.headers.size());
    for (const RecordHeader& header : record.headers) {
        const std::vector<uint8_t> key(header.key.begin(), header.key.end());
        writer.writeBytes(key);
        writer.writeBytes(header.value);
    }
    writer.writeBoolean(record.transactional);
    writer.writeBoolean(record.control);
    writer.writeBoolean(record.controlCommit);
    writer.writeString(record.transactionalID);
    writer.writeInt64(record.producerID);
    writer.writeInt16(record.producerEpoch);
    writer.writeInt32(record.sequence);
}

Record readRecord(Reader& reader) {
    Record record;
    record.offset = reader.readInt64();
    record.timestamp = reader.readInt64();
    const bool hasKey = reader.readBoolean();
    std::vector<uint8_t> key = reader.readNullableBytes();
    if (hasKey) record.key = std::move(key);
    const bool hasValue = reader.readBoolean();
    std::vector<uint8_t> value = reader.readNullableBytes();
    if (hasValue) record.value = std::move(value);
    const int32_t headers = reader.readArrayLength();
    for (int32_t i = 0; i < headers; ++i) {
        RecordHeader header;
        const std::vector<uint8_t> key = reader.readBytes();
        header.key.assign(key.begin(), key.end());
        header.value = reader.readBytes();
        record.headers.push_back(std::move(header));
    }
    record.transactional = reader.readBoolean();
    record.control = reader.readBoolean();
    record.controlCommit = reader.readBoolean();
    record.transactionalID = reader.readString();
    record.producerID = reader.readInt64();
    record.producerEpoch = reader.readInt16();
    record.sequence = reader.readInt32();
    return record;
}

void validateMemberAssignment(const std::vector<uint8_t>& bytes) {
    Reader reader(bytes);
    if (reader.readInt16() != 0) throw BrokerError(Error::INCONSISTENT_GROUP_PROTOCOL,
                                                 "unsupported member assignment version");
    const int32_t topics = reader.readArrayLength();
    for (int32_t topic = 0; topic < topics; ++topic) {
        (void)reader.readString();
        const int32_t partitions = reader.readArrayLength();
        for (int32_t partition = 0; partition < partitions; ++partition) (void)reader.readInt32();
    }
    (void)reader.readNullableBytes();
    if (reader.remaining() != 0) throw BrokerError(Error::INCONSISTENT_GROUP_PROTOCOL,
                                                   "trailing member assignment data");
}

std::vector<uint8_t> emptyAssignment() {
    Writer writer;
    writer.writeInt16(0);
    writer.writeArrayLength(0);
    writer.writeNullableBytes(nullptr);
    return writer.finish();
}

} // namespace

Broker::Broker(MQTT::Auth* auth, int32_t brokerID, std::string stateFile,
               size_t maxMessageBytes, size_t maxLogBytes,
               const std::string& advertisedHost, uint16_t advertisedPort)
    : auth(auth), id(brokerID), stateFile(std::move(stateFile)),
      maxMessageSize(std::max<size_t>(maxMessageBytes, 1)),
      maxLogSize(std::max<size_t>(maxLogBytes, 1)), host(advertisedHost), port(advertisedPort) {}

void Broker::checkWritePermission(const std::string& topic, const std::string& username) const {
    if (auth && auth->isForbidden(username, topic, true)) {
        throw BrokerError(Error::TOPIC_AUTHORIZATION_FAILED, "topic write denied");
    }
}

void Broker::checkReadPermission(const std::string& topic, const std::string& username) const {
    if (auth && auth->isForbidden(username, topic, false)) {
        throw BrokerError(Error::TOPIC_AUTHORIZATION_FAILED, "topic read denied");
    }
}

void Broker::checkGroupPermission(const std::string& group, const std::string& username,
                                  MQTT::Auth::KafkaOperation operation) const {
    if (group.empty()) throw BrokerError(Error::INVALID_GROUP_ID, "empty group id");
    if (auth && auth->isKafkaForbidden(username, group, operation)) {
        throw BrokerError(Error::GROUP_AUTHORIZATION_FAILED, "group access denied");
    }
}

void Broker::checkClusterPermission(const std::string& username) const {
    if (auth && auth->isKafkaForbidden(username, "cluster",
                                      MQTT::Auth::KafkaOperation::CLUSTER_ADMIN)) {
        throw BrokerError(Error::CLUSTER_AUTHORIZATION_FAILED, "cluster administration denied");
    }
}

void Broker::validatePartitionElection(const std::string& topic, int32_t partition,
                                       const std::string& username) {
    checkClusterPermission(username);
    std::lock_guard<std::mutex> lock(mutex);
    auto found = topicMap.find(topic);
    if (found == topicMap.end() || found->second.partitions.count(partition) == 0) {
        throw BrokerError(Error::UNKNOWN_TOPIC_OR_PARTITION, "unknown topic partition");
    }
    // A single-node partition is already led by its only ISR member; election is a no-op.
}

void Broker::createTopic(const std::string& name, int32_t partitions,
                         int16_t replicationFactor,
                         const std::map<std::string, std::string>& configs,
                         const std::string& username) {
    if (!validTopicName(name)) throw BrokerError(Error::INVALID_TOPIC, "invalid topic name");
    if (partitions < 1 || static_cast<size_t>(partitions) > MAX_PARTITIONS) {
        throw BrokerError(Error::INVALID_PARTITIONS, "invalid partition count");
    }
    if (replicationFactor != 1) {
        throw BrokerError(Error::INVALID_REPLICATION_FACTOR,
                          "single-node broker supports replication factor 1");
    }
    for (const auto& config : configs) {
        if (config.first == "cleanup.policy") {
            if (config.second != "delete" && config.second != "compact") {
                throw BrokerError(Error::INVALID_CONFIG, "unsupported cleanup policy");
            }
            if (config.second == "compact") {
                throw BrokerError(Error::INVALID_CONFIG, "compaction is not implemented");
            }
        } else if (config.first == "retention.ms" || config.first == "retention.bytes" ||
                   config.first == "max.message.bytes") {
            const int64_t value = configInteger(configs, config.first, -1);
            if (value < -1 || (config.first == "max.message.bytes" && value < 1)) {
                throw BrokerError(Error::INVALID_CONFIG, "invalid topic configuration");
            }
        } else if (config.first == "min.insync.replicas") {
            if (configInteger(configs, config.first, 1) != 1) {
                throw BrokerError(Error::INVALID_CONFIG, "min.insync.replicas must be 1");
            }
        } else {
            throw BrokerError(Error::INVALID_CONFIG, "unsupported topic configuration");
        }
    }
    checkClusterPermission(username);
    checkWritePermission(name, username);
    std::lock_guard<std::mutex> lock(mutex);
    if (topicMap.find(name) != topicMap.end()) {
        throw BrokerError(Error::TOPIC_ALREADY_EXISTS, "topic already exists");
    }
    if (topicMap.size() >= MAX_TOPICS) {
        throw BrokerError(Error::UNKNOWN_SERVER_ERROR, "topic limit reached");
    }
    size_t partitionCount = 0;
    for (const auto& entry : topicMap) partitionCount += entry.second.partitions.size();
    if (partitionCount + static_cast<size_t>(partitions) > MAX_PARTITIONS) {
        throw BrokerError(Error::UNKNOWN_SERVER_ERROR, "partition limit reached");
    }
    Topic topic;
    topic.name = name;
    topic.configs = configs;
    for (int32_t partition = 0; partition < partitions; ++partition) topic.partitions[partition];
    topicMap.emplace(name, std::move(topic));
    dirty = true;
    if (!flushLocked()) {
        topicMap.erase(name);
        throw BrokerError(Error::UNKNOWN_SERVER_ERROR, "cannot persist topic creation");
    }
}

void Broker::deleteTopic(const std::string& name, const std::string& username) {
    checkClusterPermission(username);
    checkWritePermission(name, username);
    std::lock_guard<std::mutex> lock(mutex);
    auto found = topicMap.find(name);
    if (found == topicMap.end()) {
        throw BrokerError(Error::UNKNOWN_TOPIC_OR_PARTITION, "unknown topic");
    }
    const Topic originalTopic = found->second;
    std::map<std::string, std::map<std::pair<std::string, int32_t>, int64_t>> originalOffsets;
    std::map<std::string, ProducerState> originalProducers;
    std::vector<std::pair<std::tuple<std::string, int32_t, int64_t>, SequenceState>> originalSequences;
    for (auto& group : groups) {
        auto& offsets = group.second.offsets;
        if (offsets.empty()) continue;
        originalOffsets[group.first] = offsets;
        for (auto offset = offsets.begin(); offset != offsets.end();) {
            if (offset->first.first == name) offset = offsets.erase(offset);
            else ++offset;
        }
    }
    for (auto& entry : producerStates) {
        ProducerState& producer = entry.second;
        bool changed = false;
        for (auto offset = producer.partitionOffsets.begin(); offset != producer.partitionOffsets.end();) {
            if (offset->first.first == name) { offset = producer.partitionOffsets.erase(offset); changed = true; }
            else ++offset;
        }
        for (auto partition = producer.addedPartitions.begin(); partition != producer.addedPartitions.end();) {
            if (partition->first == name) { partition = producer.addedPartitions.erase(partition); changed = true; }
            else ++partition;
        }
        for (auto offset = producer.pendingOffsets.begin(); offset != producer.pendingOffsets.end();) {
            if (std::get<1>(offset->first) == name) { offset = producer.pendingOffsets.erase(offset); changed = true; }
            else ++offset;
        }
        if (changed) originalProducers[entry.first] = producer;
    }
    for (auto sequence = sequences.begin(); sequence != sequences.end();) {
        if (std::get<0>(sequence->first) == name) {
            originalSequences.emplace_back(sequence->first, sequence->second);
            sequence = sequences.erase(sequence);
        } else ++sequence;
    }
    topicMap.erase(found);
    dirty = true;
    if (flushLocked()) return;
    topicMap.emplace(name, std::move(originalTopic));
    for (auto& offsets : originalOffsets) groups[offsets.first].offsets = std::move(offsets.second);
    for (auto& producer : originalProducers) producerStates[producer.first] = std::move(producer.second);
    for (auto& sequence : originalSequences) sequences[sequence.first] = std::move(sequence.second);
    throw BrokerError(Error::UNKNOWN_SERVER_ERROR, "cannot persist topic deletion");
}

std::vector<TopicMetadata> Broker::topics(const std::vector<std::string>& requested,
                                          bool allowAutoCreate,
                                          const std::string& username, bool allTopics) {
    std::lock_guard<std::mutex> lock(mutex);
    std::vector<TopicMetadata> result;
    if (requested.empty()) {
        if (!allTopics) return result;
        for (const auto& entry : topicMap) {
            if (!auth || !auth->isForbidden(username, entry.first, false)) {
                result.push_back({entry.first, entry.second.configs, Error::NONE});
            }
        }
        return result;
    }
    for (const std::string& name : requested) {
        auto found = topicMap.find(name);
        if (found == topicMap.end() && allowAutoCreate && validTopicName(name) &&
            topicMap.size() < MAX_TOPICS && (!auth || !auth->isForbidden(username, name, true))) {
            Topic topic;
            topic.name = name;
            topic.partitions[0];
            found = topicMap.emplace(name, std::move(topic)).first;
            dirty = true;
            if (!flushLocked()) {
                topicMap.erase(found);
                found = topicMap.end();
            }
        }
        if (found == topicMap.end() || (auth && auth->isForbidden(username, name, false))) {
            result.push_back({name, {}, Error::UNKNOWN_TOPIC_OR_PARTITION});
        } else {
            result.push_back({name, found->second.configs, Error::NONE});
        }
    }
    return result;
}

std::vector<PartitionMetadata> Broker::partitions(const std::string& topic) const {
    std::lock_guard<std::mutex> lock(mutex);
    std::vector<PartitionMetadata> result;
    auto found = topicMap.find(topic);
    if (found == topicMap.end()) return result;
    for (const auto& entry : found->second.partitions) {
        result.push_back({topic, entry.first, id, {id}, {id}, Error::NONE});
    }
    return result;
}

Broker::Topic& Broker::requireTopic(const std::string& name, const std::string& username, bool write) {
    if (write) checkWritePermission(name, username);
    else checkReadPermission(name, username);
    auto found = topicMap.find(name);
    if (found == topicMap.end()) {
        if (write && validTopicName(name) && topicMap.size() < MAX_TOPICS) {
            Topic topic;
            topic.name = name;
            topic.partitions[0];
            found = topicMap.emplace(name, std::move(topic)).first;
            dirty = true;
        } else {
            throw BrokerError(Error::UNKNOWN_TOPIC_OR_PARTITION, "unknown topic");
        }
    }
    return found->second;
}

void Broker::enforceRetentionLocked(Topic& topic, Partition& partition) {
    const int64_t retentionMs = configInteger(topic.configs, "retention.ms", -1);
    const int64_t retentionBytes = configInteger(topic.configs, "retention.bytes", -1);
    const int64_t now = nowMillis();
    auto removeOldest = [&] {
        if (partition.records.empty()) return;
        partition.bytes -= recordBytes(partition.records.front());
        partition.logStartOffset = partition.records.front().offset + 1;
        partition.records.pop_front();
        dirty = true;
    };
    const int64_t stableOffset = lastStableOffsetLocked(topic, partition);
    while (!partition.records.empty() && partition.records.front().offset < stableOffset &&
           ((retentionMs >= 0 && now - partition.records.front().timestamp >= retentionMs) ||
            (retentionBytes >= 0 && partition.bytes > static_cast<size_t>(retentionBytes)))) {
        removeOldest();
    }
    if (partition.records.empty()) partition.logStartOffset = partition.nextOffset;
    while (!partition.abortedTransactions.empty() &&
           partition.abortedTransactions.front().firstOffset < partition.logStartOffset) {
        partition.abortedTransactions.erase(partition.abortedTransactions.begin());
        dirty = true;
    }
}

ProduceResult Broker::produce(const std::string& topicName, int32_t partitionID,
                              std::vector<ProducedRecord>& records, int16_t requiredAcks,
                              const std::string& username, const std::string& transactionalID) {
    if (requiredAcks != -1 && requiredAcks != 0 && requiredAcks != 1) {
        return {Error::INVALID_REQUIRED_ACKS, -1, 0};
    }
    if (records.empty()) return {Error::INVALID_RECORD, -1, 0};
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    try {
        Topic& topic = requireTopic(topicName, username, true);
        auto partitionFound = topic.partitions.find(partitionID);
        if (partitionFound == topic.partitions.end()) return {Error::UNKNOWN_TOPIC_OR_PARTITION, -1, 0};
        Partition& partition = partitionFound->second;
        int64_t duplicateBaseOffset = -1;
        const int16_t sequenceError = checkSequenceLocked(topicName, partitionID, records,
                                                           duplicateBaseOffset);
        if (sequenceError != Error::NONE) return {sequenceError, duplicateBaseOffset, partition.logStartOffset};
        if (duplicateBaseOffset >= 0) {
            return {Error::NONE, duplicateBaseOffset, partition.logStartOffset};
        }
        const Partition originalPartition = partition;
        const auto sequenceKey = std::make_tuple(topicName, partitionID, records.front().producerID);
        const auto oldSequence = sequences.find(sequenceKey);
        const std::optional<SequenceState> originalSequence = oldSequence == sequences.end()
            ? std::nullopt : std::optional<SequenceState>(oldSequence->second);
        std::optional<ProducerState> originalProducer;
        ProducerState* producer = nullptr;
        if (!transactionalID.empty()) {
            producer = &requireProducerLocked(transactionalID, records.front().producerID,
                                              records.front().producerEpoch, username);
            if (producer->stage != ProducerState::Stage::ONGOING ||
                producer->addedPartitions.count({topicName, partitionID}) == 0) {
                return {Error::INVALID_TXN_STATE, -1, 0};
            }
            if (producer->partitionOffsets.count({topicName, partitionID}) == 0 &&
                producer->partitionOffsets.size() >= 10000) {
                return {Error::CONCURRENT_TRANSACTIONS, -1, 0};
            }
            originalProducer = *producer;
        }
        ProduceResult result = appendLocked(topic, partition, records, transactionalID);
        if (result.error != Error::NONE) return result;
        if (producer) producer->partitionOffsets.emplace(std::make_pair(topicName, partitionID), result.baseOffset);
        recordSequenceLocked(topicName, partitionID, records, result.baseOffset);
        enforceRetentionLocked(topic, partition);
        result.logStartOffset = partition.logStartOffset;
        if (requiredAcks != 0 && !flushLocked()) {
            partition = std::move(originalPartition);
            if (originalSequence) sequences[sequenceKey] = *originalSequence;
            else sequences.erase(sequenceKey);
            if (producer) *producer = std::move(*originalProducer);
            dirty = true;
            return {Error::UNKNOWN_SERVER_ERROR, -1, 0};
        }
        return result;
    } catch (const BrokerError& error) {
        return {error.code, -1, 0};
    }
}

ProduceResult Broker::appendLocked(Topic& topic, Partition& partition,
                                   std::vector<ProducedRecord>& records,
                                   const std::string& transactionalID) {
    const size_t topicLimit = static_cast<size_t>(configInteger(topic.configs, "max.message.bytes",
                                                                static_cast<int64_t>(maxMessageSize)));
    size_t incomingBytes = 0;
    for (const ProducedRecord& input : records) {
        Record record;
        record.timestamp = input.timestamp < 0 ? nowMillis() : input.timestamp;
        record.key = input.key;
        record.value = input.value;
        record.headers = input.headers;
        record.producerID = input.producerID;
        record.producerEpoch = input.producerEpoch;
        record.sequence = input.sequence;
        record.transactional = !transactionalID.empty();
        record.transactionalID = transactionalID;
        const size_t recordSize = recordBytes(record);
        if (recordSize > maxMessageSize || recordSize > topicLimit) {
            return {Error::MESSAGE_TOO_LARGE, -1, 0};
        }
        incomingBytes += recordSize;
    }
    if (incomingBytes > maxLogSize || partition.bytes + incomingBytes > maxLogSize) {
        return {Error::RECORD_LIST_TOO_LARGE, -1, 0};
    }
    const int64_t baseOffset = partition.nextOffset;
    for (ProducedRecord& input : records) {
        Record record;
        record.offset = partition.nextOffset++;
        record.timestamp = input.timestamp < 0 ? nowMillis() : input.timestamp;
        record.key = input.key;
        record.value = input.value;
        record.headers = input.headers;
        record.producerID = input.producerID;
        record.producerEpoch = input.producerEpoch;
        record.sequence = input.sequence;
        record.transactional = !transactionalID.empty();
        record.transactionalID = transactionalID;
        partition.bytes += recordBytes(record);
        partition.timestamp = std::max(partition.timestamp, record.timestamp);
        partition.records.push_back(std::move(record));
    }
    dirty = true;
    return {Error::NONE, baseOffset, partition.logStartOffset};
}

int16_t Broker::checkSequenceLocked(const std::string& topic, int32_t partition,
                                    const std::vector<ProducedRecord>& records,
                                    int64_t& duplicateBaseOffset) {
    if (records.front().producerID < 0) {
        for (const ProducedRecord& record : records) {
            if (record.producerID >= 0 || record.producerEpoch != -1 || record.sequence != -1) {
                return Error::INVALID_RECORD;
            }
        }
        return Error::NONE;
    }
    if (records.front().producerEpoch < 0 || records.front().sequence < 0) return Error::INVALID_RECORD;
    auto key = std::make_tuple(topic, partition, records.front().producerID);
    auto found = sequences.find(key);
    const auto producer = producerEpochs.find(records.front().producerID);
    if (producer == producerEpochs.end()) return Error::UNKNOWN_PRODUCER_ID;
    if (found == sequences.end()) {
        found = sequences.emplace(key, SequenceState{producer->second, 0, {}}).first;
    }
    SequenceState& state = found->second;
    if (records.front().producerEpoch != state.producerEpoch) return Error::INVALID_PRODUCER_EPOCH;
    const int32_t firstSequence = records.front().sequence;
    if (firstSequence < state.nextSequence) {
        auto previous = state.offsets.find(firstSequence);
        if (previous == state.offsets.end()) return Error::OUT_OF_ORDER_SEQUENCE_NUMBER;
        for (size_t i = 0; i < records.size(); ++i) {
            auto offset = state.offsets.find(firstSequence + static_cast<int32_t>(i));
            if (offset == state.offsets.end()) return Error::OUT_OF_ORDER_SEQUENCE_NUMBER;
            if (i == 0) duplicateBaseOffset = offset->second;
        }
        return Error::NONE;
    }
    if (firstSequence != state.nextSequence) return Error::OUT_OF_ORDER_SEQUENCE_NUMBER;
    for (size_t i = 0; i < records.size(); ++i) {
        const ProducedRecord& record = records[i];
        if (record.producerID != records.front().producerID ||
            record.producerEpoch != records.front().producerEpoch ||
            record.sequence != firstSequence + static_cast<int32_t>(i)) {
            return Error::INVALID_RECORD;
        }
    }
    return Error::NONE;
}

void Broker::recordSequenceLocked(const std::string& topic, int32_t partition,
                                  const std::vector<ProducedRecord>& records,
                                  int64_t baseOffset) {
    if (records.empty() || records.front().producerID < 0) return;
    auto key = std::make_tuple(topic, partition, records.front().producerID);
    SequenceState& state = sequences[key];
    if (state.producerEpoch != records.front().producerEpoch) {
        state.producerEpoch = records.front().producerEpoch;
        state.nextSequence = 0;
        state.offsets.clear();
    }
    for (size_t i = 0; i < records.size(); ++i) {
        state.offsets[records[i].sequence] = baseOffset + static_cast<int64_t>(i);
        state.nextSequence = records[i].sequence + 1;
    }
    dirty = true;
}

Broker::ProducerState& Broker::requireProducerLocked(const std::string& transactionalID,
                                             int64_t producerID, int16_t producerEpoch,
                                             const std::string& username) {
    if (auth && auth->isKafkaForbidden(username, transactionalID,
                                       MQTT::Auth::KafkaOperation::TRANSACTIONAL_ID_WRITE)) {
        throw BrokerError(Error::TRANSACTIONAL_ID_AUTHORIZATION_FAILED,
                          "transactional id authorization failed");
    }
    auto found = producerStates.find(transactionalID);
    if (found == producerStates.end() || found->second.producerID != producerID) {
        throw BrokerError(Error::UNKNOWN_PRODUCER_ID, "unknown producer");
    }
    if (found->second.producerEpoch != producerEpoch) {
        throw BrokerError(Error::INVALID_PRODUCER_EPOCH, "producer epoch mismatch");
    }
    return found->second;
}

InitProducerResult Broker::initProducerID(const std::string& transactionalID,
                                          int32_t transactionTimeoutMs,
                                          const std::string& clientID,
                                          const std::string& username) {
    if (!transactionalID.empty() && auth &&
        auth->isKafkaForbidden(username, transactionalID, MQTT::Auth::KafkaOperation::TRANSACTIONAL_ID_WRITE)) {
        return {Error::TRANSACTIONAL_ID_AUTHORIZATION_FAILED, -1, -1};
    }
    if (transactionTimeoutMs < 0 || transactionTimeoutMs > 2147483647 ||
        (!transactionalID.empty() && transactionTimeoutMs == 0)) {
        return {Error::INVALID_TXN_TIMEOUT, -1, -1};
    }
    std::lock_guard<std::mutex> lock(mutex);
    const std::string key = transactionalID.empty() ? "client:" + clientID : transactionalID;
    ProducerState& state = producerStates[key];
    if (state.stage == ProducerState::Stage::ONGOING) finishTxnLocked(key, state, false);
    state.producerID = nextProducerID++;
    state.producerEpoch = 0;
    producerEpochs[state.producerID] = 0;
    state.transactionTimeoutMs = transactionTimeoutMs;
    state.stage = ProducerState::Stage::EMPTY;
    state.partitionOffsets.clear();
    state.pendingGroups.clear();
    state.pendingOffsets.clear();
    for (auto& entry : sequences) {
        if (std::get<2>(entry.first) == state.producerID) {
            entry.second.producerEpoch = -1;
            entry.second.offsets.clear();
        }
    }
    for (const auto& topic : topicMap) {
        for (const auto& partition : topic.second.partitions) {
            SequenceState sequence;
            sequence.producerEpoch = 0;
            sequences[std::make_tuple(topic.first, partition.first, state.producerID)] = sequence;
        }
    }
    dirty = true;
    return {Error::NONE, state.producerID, state.producerEpoch};
}

std::map<std::pair<std::string, int32_t>, int16_t>
Broker::addPartitionsToTxn(const std::string& transactionalID, int64_t producerID,
                           int16_t producerEpoch,
                           const std::vector<std::pair<std::string, std::vector<int32_t>>>& partitions,
                           const std::string& username) {
    if (transactionalID.empty()) throw BrokerError(Error::INVALID_REQUEST, "transactional id required");
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    ProducerState& producer = requireProducerLocked(transactionalID, producerID, producerEpoch, username);
    std::map<std::pair<std::string, int32_t>, int16_t> results;
    for (const auto& topic : partitions) {
        auto found = topicMap.find(topic.first);
        for (int32_t partition : topic.second) {
            int16_t error = Error::NONE;
            if (auth && auth->isForbidden(username, topic.first, true)) {
                error = Error::TOPIC_AUTHORIZATION_FAILED;
            } else if (found == topicMap.end() || found->second.partitions.count(partition) == 0) {
                error = Error::UNKNOWN_TOPIC_OR_PARTITION;
            } else {
                producer.addedPartitions.emplace(topic.first, partition);
            }
            results[{topic.first, partition}] = error;
        }
    }
    if (producer.stage == ProducerState::Stage::EMPTY &&
        std::any_of(results.begin(), results.end(), [](const auto& entry) { return entry.second == Error::NONE; })) {
        producer.stage = ProducerState::Stage::ONGOING;
        producer.transactionStartedAt = nowMillis();
    }
    dirty = true;
    return results;
}

void Broker::addOffsetsToTxn(const std::string& transactionalID, int64_t producerID,
                             int16_t producerEpoch, const std::string& group,
                             const std::string& username) {
    if (transactionalID.empty()) throw BrokerError(Error::INVALID_REQUEST, "transactional id required");
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    ProducerState& producer = requireProducerLocked(transactionalID, producerID, producerEpoch, username);
    checkGroupPermission(group, username, MQTT::Auth::KafkaOperation::GROUP_WRITE);
    if (producer.stage == ProducerState::Stage::EMPTY) producer.transactionStartedAt = nowMillis();
    producer.stage = ProducerState::Stage::ONGOING;
    producer.pendingGroups.insert(group);
    dirty = true;
}

void Broker::txnOffsetCommit(const std::string& transactionalID, int64_t producerID,
                             int16_t producerEpoch, int32_t generation,
                             const std::string& memberID, const std::string& group,
                             const std::string& topic, int32_t partition, int64_t offset,
                             const std::string& username) {
    if (transactionalID.empty()) throw BrokerError(Error::INVALID_REQUEST, "transactional id required");
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    ProducerState& producer = requireProducerLocked(transactionalID, producerID, producerEpoch, username);
    if (producer.stage != ProducerState::Stage::ONGOING) throw BrokerError(Error::INVALID_TXN_STATE, "inactive transaction");
    checkGroupPermission(group, username, MQTT::Auth::KafkaOperation::GROUP_READ);
    if (producer.pendingGroups.count(group) == 0) {
        throw BrokerError(Error::INVALID_TXN_STATE, "group was not added to transaction");
    }
    auto found = topicMap.find(topic);
    if (found == topicMap.end() || found->second.partitions.count(partition) == 0) {
        throw BrokerError(Error::UNKNOWN_TOPIC_OR_PARTITION, "unknown offset partition");
    }
    (void)requireTopic(topic, username, false);
    if (generation >= 0) {
        auto groupFound = groups.find(group);
        if (groupFound == groups.end() || groupFound->second.members.count(memberID) == 0 ||
            groupFound->second.generation != generation || groupFound->second.state != Group::State::STABLE) {
            throw BrokerError(Error::ILLEGAL_GENERATION, "invalid group generation");
        }
    }
    if (offset < -1) throw BrokerError(Error::INVALID_COMMIT_OFFSET_SIZE, "invalid offset");
    producer.pendingOffsets[{group, topic, partition}] = offset;
}

int64_t Broker::lastStableOffsetLocked(const Topic& topic, const Partition& partition) const {
    int64_t stable = partition.nextOffset;
    for (const auto& entry : producerStates) {
        const ProducerState& producer = entry.second;
        if (producer.stage != ProducerState::Stage::ONGOING) continue;
        for (const auto& offset : producer.partitionOffsets) {
            if (offset.first.first == topic.name && offset.first.second >= 0 &&
                topic.partitions.count(offset.first.second) &&
                &topic.partitions.at(offset.first.second) == &partition) {
                stable = std::min(stable, offset.second);
            }
        }
    }
    return stable;
}

void Broker::appendTxnMarkerLocked(Topic& topic, Partition& partition,
                                   const ProducerState& producer, const std::string& transactionalID,
                                   int64_t firstOffset, bool commit) {
    Record marker;
    marker.offset = partition.nextOffset++;
    marker.timestamp = nowMillis();
    marker.producerID = producer.producerID;
    marker.producerEpoch = producer.producerEpoch;
    marker.transactional = true;
    marker.control = true;
    marker.controlCommit = commit;
    marker.transactionalID = transactionalID;
    partition.records.push_back(std::move(marker));
    partition.bytes += recordBytes(partition.records.back());
    partition.timestamp = std::max(partition.timestamp, partition.records.back().timestamp);
    if (!commit) partition.abortedTransactions.push_back({producer.producerID, firstOffset});
}

void Broker::finishTxnLocked(const std::string& transactionalID, ProducerState& producer, bool commit,
                             const std::set<std::pair<std::string, int32_t>>* selected) {
    std::vector<std::pair<std::string, int32_t>> completed;
    for (const auto& entry : producer.partitionOffsets) {
        if (!selected || selected->count(entry.first)) completed.push_back(entry.first);
    }
    struct PartitionBackup {
        Topic* topic;
        Partition* partition;
        size_t recordCount;
        size_t bytes;
        size_t abortedCount;
        int64_t nextOffset;
        int64_t timestamp;
    };
    std::vector<PartitionBackup> backups;
    std::map<std::string, std::map<std::pair<std::string, int32_t>, int64_t>> oldOffsets;
    for (const auto& key : completed) {
        auto found = topicMap.find(key.first);
        if (found == topicMap.end() || found->second.partitions.count(key.second) == 0) {
            throw BrokerError(Error::UNKNOWN_TOPIC_OR_PARTITION, "transaction topic was deleted");
        }
        Topic& topic = found->second;
        Partition& partition = topic.partitions.at(key.second);
        if (partition.nextOffset >= partition.logStartOffset &&
            partition.bytes + recordBytes(Record{}) > maxLogSize) {
            throw BrokerError(Error::RECORD_LIST_TOO_LARGE, "transaction marker exceeds log limit");
        }
        backups.push_back({&topic, &partition, partition.records.size(), partition.bytes,
                           partition.abortedTransactions.size(), partition.nextOffset, partition.timestamp});
    }
    if (completed.empty() || completed.size() == producer.partitionOffsets.size()) {
        for (const auto& entry : producer.pendingOffsets) {
            const std::string& group = std::get<0>(entry.first);
            oldOffsets[group] = groups[group].offsets;
        }
    }
    const ProducerState original = producer;
    for (const auto& key : completed) {
        appendTxnMarkerLocked(topicMap.at(key.first), topicMap.at(key.first).partitions.at(key.second),
                              producer, transactionalID, producer.partitionOffsets.at(key), commit);
        producer.partitionOffsets.erase(key);
        producer.addedPartitions.erase(key);
    }
    if (producer.partitionOffsets.empty()) {
        if (commit) {
            for (const auto& entry : producer.pendingOffsets) {
                const auto& key = entry.first;
                if (entry.second == -1) groups[std::get<0>(key)].offsets.erase({std::get<1>(key), std::get<2>(key)});
                else groups[std::get<0>(key)].offsets[{std::get<1>(key), std::get<2>(key)}] = entry.second;
            }
        }
        producer.stage = ProducerState::Stage::EMPTY;
        producer.addedPartitions.clear();
        producer.pendingGroups.clear();
        producer.pendingOffsets.clear();
        producer.transactionStartedAt = 0;
    }
    dirty = true;
    if (flushLocked()) return;
    for (const PartitionBackup& backup : backups) {
        backup.partition->records.resize(backup.recordCount);
        backup.partition->abortedTransactions.resize(backup.abortedCount);
        backup.partition->bytes = backup.bytes;
        backup.partition->nextOffset = backup.nextOffset;
        backup.partition->timestamp = backup.timestamp;
    }
    for (const auto& entry : oldOffsets) groups[entry.first].offsets = entry.second;
    producer = std::move(original);
    throw BrokerError(Error::UNKNOWN_SERVER_ERROR, "cannot persist transaction completion");
}

void Broker::endTxn(const std::string& transactionalID, int64_t producerID,
                    int16_t producerEpoch, bool commit, const std::string& username) {
    if (transactionalID.empty()) throw BrokerError(Error::INVALID_REQUEST, "transactional id required");
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    ProducerState& producer = requireProducerLocked(transactionalID, producerID, producerEpoch, username);
    if (producer.stage != ProducerState::Stage::ONGOING) throw BrokerError(Error::INVALID_TXN_STATE, "inactive transaction");
    finishTxnLocked(transactionalID, producer, commit);
}

std::map<std::pair<std::string, int32_t>, int16_t> Broker::writeTxnMarkers(
    int64_t producerID, int16_t producerEpoch, int32_t coordinatorEpoch, bool commit,
    const std::vector<std::pair<std::string, std::vector<int32_t>>>& partitions,
    const std::string& username) {
    checkClusterPermission(username);
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    if (coordinatorEpoch != 0) throw BrokerError(Error::INVALID_TXN_STATE, "invalid coordinator epoch");
    auto producer = producerEpochs.find(producerID);
    if (producer == producerEpochs.end()) throw BrokerError(Error::UNKNOWN_PRODUCER_ID, "unknown producer");
    if (producer->second != producerEpoch) throw BrokerError(Error::INVALID_PRODUCER_EPOCH, "producer epoch mismatch");
    auto state = std::find_if(producerStates.begin(), producerStates.end(), [&](const auto& entry) {
        return entry.second.producerID == producerID;
    });
    if (state == producerStates.end()) throw BrokerError(Error::UNKNOWN_PRODUCER_ID, "unknown producer");
    if (state->second.stage != ProducerState::Stage::ONGOING) {
        throw BrokerError(Error::INVALID_TXN_STATE, "inactive transaction");
    }
    std::set<std::pair<std::string, int32_t>> selected;
    std::map<std::pair<std::string, int32_t>, int16_t> results;
    for (const auto& topic : partitions) {
        for (int32_t partition : topic.second) {
            const auto key = std::make_pair(topic.first, partition);
            if (state->second.partitionOffsets.count(key) == 0 || !selected.insert(key).second) {
                results[key] = Error::INVALID_TXN_STATE;
            } else {
                results[key] = Error::NONE;
            }
        }
    }
    if (std::any_of(results.begin(), results.end(), [](const auto& entry) { return entry.second != Error::NONE; })) {
        return results;
    }
    finishTxnLocked(state->first, state->second, commit, &selected);
    return results;
}

FetchResult Broker::fetch(const std::string& topicName, int32_t partitionID, int64_t offset,
                          int32_t maxBytes, const std::string& username, bool readCommitted) {
    if (maxBytes < 0) return {Error::INVALID_FETCH_SIZE, 0, 0, 0, {}, {}};
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    try {
        Topic& topic = requireTopic(topicName, username, false);
        auto found = topic.partitions.find(partitionID);
        if (found == topic.partitions.end()) {
            return {Error::UNKNOWN_TOPIC_OR_PARTITION, 0, 0, 0, {}, {}};
        }
        Partition& partition = found->second;
        if (offset < partition.logStartOffset || offset > partition.nextOffset) {
            return {Error::OFFSET_OUT_OF_RANGE, partition.nextOffset, partition.nextOffset,
                    partition.logStartOffset, {}, {}};
        }
        FetchResult result;
        result.highWatermark = partition.nextOffset;
        result.lastStableOffset = readCommitted ? lastStableOffsetLocked(topic, partition)
                                                : partition.nextOffset;
        result.logStartOffset = partition.logStartOffset;
        if (readCommitted) result.abortedTransactions = partition.abortedTransactions;
        size_t bytes = 0;
        for (const Record& record : partition.records) {
            if (record.offset < offset || record.offset >= result.lastStableOffset) continue;
            const size_t size = recordBytes(record);
            if (!result.records.empty() && bytes + size > static_cast<size_t>(maxBytes)) break;
            result.records.push_back(record);
            bytes += size;
        }
        return result;
    } catch (const BrokerError& error) {
        return {error.code, 0, 0, 0, {}, {}};
    }
}

OffsetResult Broker::listOffsets(const std::string& topicName, int32_t partitionID,
                                 int64_t timestamp, const std::string& username, bool readCommitted) {
    std::lock_guard<std::mutex> lock(mutex);
    expireTransactionsLocked();
    try {
        Topic& topic = requireTopic(topicName, username, false);
        auto found = topic.partitions.find(partitionID);
        if (found == topic.partitions.end()) return {-1, -1, Error::UNKNOWN_TOPIC_OR_PARTITION};
        Partition& partition = found->second;
        const int64_t endOffset = readCommitted ? lastStableOffsetLocked(topic, partition)
                                                 : partition.nextOffset;
        if (timestamp == -2) return {partition.logStartOffset, -1, Error::NONE};
        if (timestamp == -1 || timestamp == -3) return {endOffset, partition.timestamp, Error::NONE};
        for (const Record& record : partition.records) {
            if (record.offset >= endOffset) break;
            if (record.timestamp >= timestamp) return {record.offset, record.timestamp, Error::NONE};
        }
        return {endOffset, -1, Error::NONE};
    } catch (const BrokerError& error) {
        return {-1, -1, error.code};
    }
}

void Broker::commitOffset(const std::string& group, int32_t generation,
                          const std::string& memberID, const std::string& topic,
                          int32_t partition, int64_t offset, const std::string& username) {
    if (group.empty()) throw BrokerError(Error::INVALID_GROUP_ID, "empty group id");
    if (offset < -1) throw BrokerError(Error::INVALID_COMMIT_OFFSET_SIZE, "invalid offset");
    checkGroupPermission(group, username, MQTT::Auth::KafkaOperation::GROUP_READ);
    std::lock_guard<std::mutex> lock(mutex);
    try {
        Topic& committedTopic = requireTopic(topic, username, false);
        if (committedTopic.partitions.count(partition) == 0) {
            throw BrokerError(Error::UNKNOWN_TOPIC_OR_PARTITION, "unknown offset partition");
        }
    } catch (const BrokerError& error) {
        throw BrokerError(error.code == Error::UNKNOWN_TOPIC_OR_PARTITION
                              ? Error::UNKNOWN_TOPIC_OR_PARTITION
                              : Error::TOPIC_AUTHORIZATION_FAILED,
                          "offset commit topic is unavailable");
    }
    expireGroupsLocked();
    auto groupFound = groups.find(group);
    if (generation >= 0) {
        if (groupFound == groups.end()) throw BrokerError(Error::ILLEGAL_GENERATION, "unknown group");
        Group& state = groupFound->second;
        auto member = state.members.find(memberID);
        if (member == state.members.end()) throw BrokerError(Error::UNKNOWN_MEMBER_ID, "unknown member");
        if (state.generation != generation) throw BrokerError(Error::ILLEGAL_GENERATION, "illegal generation");
        if (state.state != Group::State::STABLE) {
            throw BrokerError(Error::REBALANCE_IN_PROGRESS, "group is rebalancing");
        }
    }
    const auto originalGroup = groups.find(group);
    const std::optional<Group> originalState = originalGroup == groups.end()
        ? std::nullopt : std::optional<Group>(originalGroup->second);
    if (offset == -1) groups[group].offsets.erase({topic, partition});
    else groups[group].offsets[{topic, partition}] = offset;
    dirty = true;
    if (!flushLocked()) {
        if (originalState) groups[group] = *originalState;
        else groups.erase(group);
        throw BrokerError(Error::UNKNOWN_SERVER_ERROR, "cannot persist offset commit");
    }
}

std::map<std::pair<std::string, int32_t>, OffsetResult>
Broker::fetchOffsets(const std::string& group,
                     const std::vector<std::pair<std::string, int32_t>>& partitions,
                     const std::string& username) const {
    std::map<std::pair<std::string, int32_t>, OffsetResult> result;
    if (group.empty()) {
        for (const auto& partition : partitions) {
            result[partition] = {-1, -1, Error::INVALID_GROUP_ID};
        }
        return result;
    }
    if (auth && auth->isKafkaForbidden(username, group, MQTT::Auth::KafkaOperation::GROUP_READ)) {
        for (const auto& partition : partitions) {
            result[partition] = {-1, -1, Error::GROUP_AUTHORIZATION_FAILED};
        }
        return result;
    }
    std::lock_guard<std::mutex> lock(mutex);
    auto groupFound = groups.find(group);
    if (partitions.empty() && groupFound != groups.end()) {
        for (const auto& committed : groupFound->second.offsets) {
            try {
                checkReadPermission(committed.first.first, username);
                result[committed.first] = {committed.second, -1, Error::NONE};
            } catch (const BrokerError& error) {
                result[committed.first] = {-1, -1, error.code};
            }
        }
        return result;
    }
    for (const auto& partition : partitions) {
        try {
            checkReadPermission(partition.first, username);
            auto topic = topicMap.find(partition.first);
            if (topic == topicMap.end() || topic->second.partitions.find(partition.second) ==
                                              topic->second.partitions.end()) {
                result[partition] = {-1, -1, Error::UNKNOWN_TOPIC_OR_PARTITION};
                continue;
            }
            int64_t offset = -1;
            if (groupFound != groups.end()) {
                auto committed = groupFound->second.offsets.find(partition);
                if (committed != groupFound->second.offsets.end()) offset = committed->second;
            }
            if (offset < 0) offset = topic->second.partitions.at(partition.second).logStartOffset;
            result[partition] = {offset, -1, Error::NONE};
        } catch (const BrokerError& error) {
            result[partition] = {-1, -1, error.code};
        }
    }
    return result;
}

std::string Broker::findCoordinator() const { return host + ":" + std::to_string(port); }

JoinGroupResult Broker::joinGroup(const std::string& group,
                                  const std::string& requestedMemberID,
                                  const std::string& clientID,
                                  const std::string& clientHost,
                                  int32_t sessionTimeout,
                                  int32_t heartbeatInterval,
                                  const std::string& protocolType,
                                  const std::string& protocol,
                                  int32_t rebalanceTimeout,
                                  const std::vector<uint8_t>& metadata,
                                  const std::string& username) {
    checkGroupPermission(group, username, MQTT::Auth::KafkaOperation::GROUP_WRITE);
    if (sessionTimeout < 6000 || sessionTimeout > 1800000 || heartbeatInterval < 1000 ||
        rebalanceTimeout < 0 || rebalanceTimeout > 2147483647) {
        throw BrokerError(Error::INVALID_SESSION_TIMEOUT, "invalid group timeout");
    }
    std::lock_guard<std::mutex> lock(mutex);
    expireGroupsLocked();
    Group& state = groups[group];
    if (!state.protocol.empty() &&
        (state.protocol != protocol || state.protocolType != protocolType)) {
        throw BrokerError(Error::INCONSISTENT_GROUP_PROTOCOL, "group protocol mismatch");
    }
    std::string memberID = requestedMemberID;
    if (memberID.empty()) memberID = clientID + "-" + randomSuffix();
    auto existing = state.members.find(memberID);
    const bool membershipChanged = existing == state.members.end() || state.state == Group::State::STABLE;
    if (membershipChanged) {
        ++state.generation;
        state.state = Group::State::PREPARING_REBALANCE;
        state.assignments.reset();
    }
    GroupMember member;
    member.memberID = memberID;
    member.clientID = clientID;
    member.clientHost = clientHost;
    member.metadata = metadata;
    member.sessionTimeout = sessionTimeout;
    member.lastHeartbeat = std::chrono::steady_clock::now();
    state.members[memberID] = std::move(member);
    state.protocolType = protocolType;
    state.protocol = protocol;
    state.rebalanceTimeout = rebalanceTimeout;
    if (state.leader.empty() || state.members.find(state.leader) == state.members.end() ||
        memberID < state.leader) {
        state.leader = memberID;
    }
    JoinGroupResult result;
    result.generation = state.generation;
    result.leaderID = state.leader;
    result.memberID = memberID;
    for (const auto& entry : state.members) result.members.push_back(entry.second);
    state.state = Group::State::COMPLETING_REBALANCE;
    groupChanged.notify_all();
    return result;
}

void Broker::expireGroupsLocked() {
    const auto now = std::chrono::steady_clock::now();
    for (auto& entry : groups) {
        Group& group = entry.second;
        for (auto member = group.members.begin(); member != group.members.end();) {
            if (now - member->second.lastHeartbeat > std::chrono::milliseconds(member->second.sessionTimeout)) {
                member = group.members.erase(member);
                group.state = Group::State::PREPARING_REBALANCE;
                group.assignments.reset();
                ++group.generation;
            } else {
                ++member;
            }
        }
        if (group.members.empty() && group.state != Group::State::EMPTY) {
            group.state = Group::State::EMPTY;
            ++group.generation;
            group.leader.clear();
            group.assignments.reset();
        }
    }
}

void Broker::heartbeatGroup(const std::string& group, const std::string& memberID,
                            int32_t generation, const std::string& username) {
    checkGroupPermission(group, username, MQTT::Auth::KafkaOperation::GROUP_WRITE);
    std::lock_guard<std::mutex> lock(mutex);
    expireGroupsLocked();
    auto found = groups.find(group);
    if (found == groups.end()) throw BrokerError(Error::UNKNOWN_MEMBER_ID, "unknown group");
    Group& state = found->second;
    auto member = state.members.find(memberID);
    if (member == state.members.end()) throw BrokerError(Error::UNKNOWN_MEMBER_ID, "unknown member");
    if (generation != state.generation) throw BrokerError(Error::ILLEGAL_GENERATION, "illegal generation");
    if (state.state == Group::State::PREPARING_REBALANCE) {
        throw BrokerError(Error::REBALANCE_IN_PROGRESS, "group is rebalancing");
    }
    member->second.lastHeartbeat = std::chrono::steady_clock::now();
}

void Broker::leaveGroup(const std::string& group, const std::string& memberID,
                        const std::string& username) {
    checkGroupPermission(group, username, MQTT::Auth::KafkaOperation::GROUP_WRITE);
    std::lock_guard<std::mutex> lock(mutex);
    auto found = groups.find(group);
    if (found == groups.end()) return;
    Group& state = found->second;
    state.members.erase(memberID);
    if (state.members.empty()) {
        state.state = Group::State::EMPTY;
        state.leader.clear();
        state.assignments.reset();
    } else {
        state.state = Group::State::PREPARING_REBALANCE;
        state.assignments.reset();
    }
    ++state.generation;
    groupChanged.notify_all();
}

void Broker::expireTransactionsLocked() {
    const int64_t now = nowMillis();
    for (auto& entry : producerStates) {
        ProducerState& producer = entry.second;
        if (producer.stage == ProducerState::Stage::ONGOING && producer.transactionTimeoutMs > 0 &&
            now - producer.transactionStartedAt > producer.transactionTimeoutMs) {
            try {
                finishTxnLocked(entry.first, producer, false);
            } catch (const BrokerError& error) {
                MQTT::logError("cannot expire Kafka transaction %s: %s", entry.first.c_str(), error.what());
            }
        }
    }
}

void Broker::syncGroup(
    const std::string& group, const std::string& memberID, int32_t generation,
    std::vector<std::pair<std::string, std::vector<uint8_t>>> groupAssignments,
    const std::string& username) {
    checkGroupPermission(group, username, MQTT::Auth::KafkaOperation::GROUP_WRITE);
    std::unique_lock<std::mutex> lock(mutex);
    auto found = groups.find(group);
    if (found == groups.end()) throw BrokerError(Error::UNKNOWN_MEMBER_ID, "unknown group");
    Group& state = found->second;
    auto member = state.members.find(memberID);
    if (member == state.members.end()) throw BrokerError(Error::UNKNOWN_MEMBER_ID, "unknown member");
    if (generation != state.generation) throw BrokerError(Error::ILLEGAL_GENERATION, "illegal generation");
    if (state.state == Group::State::PREPARING_REBALANCE) {
        throw BrokerError(Error::REBALANCE_IN_PROGRESS, "group is rebalancing");
    }
    if (memberID == state.leader) {
        std::map<std::string, std::vector<uint8_t>> byMember;
        for (auto& entry : groupAssignments) {
            validateMemberAssignment(entry.second);
            if (state.members.count(entry.first) == 0 ||
                !byMember.emplace(entry.first, entry.second).second) {
                throw BrokerError(Error::INCONSISTENT_GROUP_PROTOCOL,
                                  "invalid member assignment list");
            }
        }
        if (byMember.size() != state.members.size()) {
            throw BrokerError(Error::INCONSISTENT_GROUP_PROTOCOL,
                              "missing member assignment");
        }
        state.assignments = std::move(groupAssignments);
        state.state = Group::State::STABLE;
        groupChanged.notify_all();
        return;
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(member->second.sessionTimeout);
    while (!state.assignments && std::chrono::steady_clock::now() < deadline) {
        groupChanged.wait_for(lock, std::chrono::milliseconds(100));
        expireGroupsLocked();
        if (state.members.find(memberID) == state.members.end()) {
            throw BrokerError(Error::UNKNOWN_MEMBER_ID, "member expired");
        }
        if (state.generation != generation) throw BrokerError(Error::ILLEGAL_GENERATION, "generation changed");
    }
    if (!state.assignments) throw BrokerError(Error::REBALANCE_IN_PROGRESS, "leader did not sync");
    state.state = Group::State::STABLE;
}

std::vector<uint8_t> Broker::memberAssignment(const std::string& group,
                                              const std::string& memberID,
                                              int32_t generation) {
    std::lock_guard<std::mutex> lock(mutex);
    auto found = groups.find(group);
    if (found == groups.end()) throw BrokerError(Error::UNKNOWN_MEMBER_ID, "unknown group");
    Group& state = found->second;
    if (state.generation != generation) throw BrokerError(Error::ILLEGAL_GENERATION, "illegal generation");
    if (!state.assignments) return emptyAssignment();
    for (const auto& assignment : *state.assignments) {
        if (assignment.first == memberID) return assignment.second;
    }
    return emptyAssignment();
}

std::vector<GroupDescription> Broker::describeGroups(const std::vector<std::string>& requested,
                                                      const std::string& username) {
    std::lock_guard<std::mutex> lock(mutex);
    expireGroupsLocked();
    std::vector<GroupDescription> result;
    auto denied = [&](const std::string& name) {
        return auth && auth->isKafkaForbidden(username, name, MQTT::Auth::KafkaOperation::GROUP_DESCRIBE);
    };
    auto append = [&](const std::string& name, const Group& group) {
        if (denied(name)) return;
        GroupDescription description;
        description.groupID = name;
        switch (group.state) {
        case Group::State::EMPTY: description.state = "Empty"; break;
        case Group::State::PREPARING_REBALANCE: description.state = "PreparingRebalance"; break;
        case Group::State::COMPLETING_REBALANCE: description.state = "CompletingRebalance"; break;
        case Group::State::STABLE: description.state = "Stable"; break;
        case Group::State::DEAD: description.state = "Dead"; break;
        }
        description.protocolType = group.protocolType;
        description.protocol = group.protocol;
        description.generation = group.generation;
        for (const auto& entry : group.members) description.members.push_back(entry.second);
        result.push_back(std::move(description));
    };
    if (requested.empty()) {
        for (const auto& entry : groups) append(entry.first, entry.second);
    } else {
        for (const std::string& name : requested) {
            auto found = groups.find(name);
            if (denied(name)) {
                result.push_back({name, "Dead", {}, {}, 0, {}, Error::GROUP_AUTHORIZATION_FAILED});
            } else if (found == groups.end()) {
                result.push_back({name, "Dead", {}, {}, 0, {}});
            } else {
                append(name, found->second);
            }
        }
    }
    return result;
}

bool Broker::flush() {
    std::lock_guard<std::mutex> lock(mutex);
    return flushLocked();
}

bool Broker::flushLocked() {
    if (stateFile.empty()) {
        dirty = false;
        return true;
    }
    try {
        const bool directorySynced = writeSnapshot(stateFile);
        dirty = false;
        if (!directorySynced) {
            MQTT::logError("Kafka state replaced but parent-directory fsync failed");
        }
        return true;
    } catch (const std::exception& error) {
        MQTT::logError("cannot persist Kafka state: %s", error.what());
        return false;
    }
}

bool Broker::writeSnapshot(const std::string& path) const {
    Writer body;
    body.writeInt32(id);
    body.writeArrayLength(topicMap.size());
    for (const auto& topicEntry : topicMap) {
        const Topic& topic = topicEntry.second;
        body.writeString(topic.name);
        body.writeArrayLength(topic.configs.size());
        for (const auto& config : topic.configs) {
            body.writeString(config.first);
            body.writeString(config.second);
        }
        body.writeArrayLength(topic.partitions.size());
        for (const auto& partitionEntry : topic.partitions) {
            const Partition& partition = partitionEntry.second;
            body.writeInt32(partitionEntry.first);
            body.writeInt64(partition.logStartOffset);
            body.writeInt64(partition.nextOffset);
            body.writeInt64(partition.timestamp);
            body.writeArrayLength(partition.records.size());
            for (const Record& record : partition.records) writeRecord(body, record);
            body.writeArrayLength(partition.abortedTransactions.size());
            for (const AbortedTransaction& aborted : partition.abortedTransactions) {
                body.writeInt64(aborted.producerID);
                body.writeInt64(aborted.firstOffset);
            }
        }
    }
    size_t offsetCount = 0;
    for (const auto& group : groups) offsetCount += group.second.offsets.size();
    body.writeArrayLength(offsetCount);
    for (const auto& group : groups) {
        for (const auto& offset : group.second.offsets) {
            body.writeString(group.first);
            body.writeString(offset.first.first);
            body.writeInt32(offset.first.second);
            body.writeInt64(offset.second);
        }
    }
    body.writeArrayLength(producerStates.size());
    for (const auto& entry : producerStates) {
        const ProducerState& producer = entry.second;
        body.writeString(entry.first);
        body.writeInt64(producer.producerID);
        body.writeInt16(producer.producerEpoch);
        body.writeBoolean(producer.stage == ProducerState::Stage::ONGOING);
        body.writeInt32(producer.transactionTimeoutMs);
        body.writeInt64(producer.transactionStartedAt);
        body.writeArrayLength(producer.partitionOffsets.size());
        for (const auto& offset : producer.partitionOffsets) {
            body.writeString(offset.first.first);
            body.writeInt32(offset.first.second);
            body.writeInt64(offset.second);
        }
        body.writeArrayLength(producer.addedPartitions.size());
        for (const auto& partition : producer.addedPartitions) {
            body.writeString(partition.first);
            body.writeInt32(partition.second);
        }
        body.writeArrayLength(producer.pendingGroups.size());
        for (const std::string& group : producer.pendingGroups) body.writeString(group);
        body.writeArrayLength(producer.pendingOffsets.size());
        for (const auto& offset : producer.pendingOffsets) {
            body.writeString(std::get<0>(offset.first));
            body.writeString(std::get<1>(offset.first));
            body.writeInt32(std::get<2>(offset.first));
            body.writeInt64(offset.second);
        }
    }
    body.writeArrayLength(producerEpochs.size());
    for (const auto& producer : producerEpochs) {
        body.writeInt64(producer.first);
        body.writeInt16(producer.second);
    }
    body.writeArrayLength(sequences.size());
    for (const auto& entry : sequences) {
        const SequenceState& sequence = entry.second;
        body.writeString(std::get<0>(entry.first));
        body.writeInt32(std::get<1>(entry.first));
        body.writeInt64(std::get<2>(entry.first));
        body.writeInt16(sequence.producerEpoch);
        body.writeInt32(sequence.nextSequence);
        body.writeArrayLength(sequence.offsets.size());
        for (const auto& offset : sequence.offsets) {
            body.writeInt32(offset.first);
            body.writeInt64(offset.second);
        }
    }
    body.writeInt64(nextProducerID);
    const std::vector<uint8_t> payload = body.finish();
    Writer output;
    output.writeInt32(STATE_MAGIC);
    output.writeInt32(crc32c(payload));
    output.writeRaw(payload);
    const std::vector<uint8_t> bytes = output.finish();
    const std::string temporary = path + ".tmp";
    const int descriptor = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (descriptor < 0) throw std::runtime_error(std::strerror(errno));
    size_t written = 0;
    while (written < bytes.size()) {
        const ssize_t count = ::write(descriptor, bytes.data() + written, bytes.size() - written);
        if (count < 0 && errno == EINTR) continue;
        if (count <= 0) {
            ::close(descriptor);
            ::unlink(temporary.c_str());
            throw std::runtime_error("short state write");
        }
        written += static_cast<size_t>(count);
    }
    if (::fsync(descriptor) != 0 || failNextFileSync) {
        if (failNextFileSync) {
            failNextFileSync = false;
            ::close(descriptor);
            ::unlink(temporary.c_str());
            throw std::runtime_error("injected state-file fsync failure");
        }
        ::close(descriptor);
        ::unlink(temporary.c_str());
        throw std::runtime_error(std::strerror(errno));
    }
    if (::close(descriptor) != 0) {
        ::unlink(temporary.c_str());
        throw std::runtime_error(std::strerror(errno));
    }
    if (::rename(temporary.c_str(), path.c_str()) != 0) {
        ::unlink(temporary.c_str());
        throw std::runtime_error(std::strerror(errno));
    }
    if (failNextDirectorySync) {
        failNextDirectorySync = false;
        MQTT::logError("injected Kafka parent-directory fsync failure");
        return false;
    }
    const size_t slash = path.find_last_of('/');
    const std::string directory = slash == std::string::npos ? "." : path.substr(0, slash);
    const int directoryDescriptor = ::open(directory.c_str(), O_RDONLY);
    if (directoryDescriptor < 0) {
        MQTT::logError("cannot open Kafka state directory %s: %s", directory.c_str(), std::strerror(errno));
        return false;
    }
    const int syncResult = ::fsync(directoryDescriptor);
    const int syncError = errno;
    ::close(directoryDescriptor);
    if (syncResult != 0) {
        MQTT::logError("cannot fsync Kafka state directory %s: %s", directory.c_str(), std::strerror(syncError));
        return false;
    }
    return true;
}

bool Broker::load() {
    if (stateFile.empty()) return true;
    std::ifstream input(stateFile, std::ios::binary | std::ios::ate);
    if (!input) return errno == ENOENT;
    const std::streamoff length = input.tellg();
    if (length < 8 || length > MAX_STATE_BYTES) return false;
    input.seekg(0);
    std::vector<uint8_t> bytes(static_cast<size_t>(length));
    input.read(reinterpret_cast<char*>(bytes.data()), length);
    if (!input) return false;
    try {
        Reader reader(std::move(bytes));
        if (reader.readInt32() != STATE_MAGIC) return false;
        const uint32_t checksum = static_cast<uint32_t>(reader.readInt32());
        std::vector<uint8_t> payload = reader.readRaw(reader.remaining());
        if (crc32c(payload) != checksum) return false;
        Reader body(std::move(payload));
        (void)body.readInt32();
        const int32_t topicCount = body.readArrayLength();
        if (static_cast<size_t>(topicCount) > MAX_TOPICS) return false;
        std::map<std::string, Topic> loadedTopics;
        size_t partitionCount = 0;
        for (int32_t topicIndex = 0; topicIndex < topicCount; ++topicIndex) {
            Topic topic;
            topic.name = body.readString();
            if (!validTopicName(topic.name) || loadedTopics.count(topic.name)) return false;
            const int32_t configs = body.readArrayLength();
            for (int32_t i = 0; i < configs; ++i) {
                const std::string key = body.readString();
                const std::string value = body.readString();
                if (!topic.configs.emplace(key, value).second) return false;
            }
            const int32_t partitions = body.readArrayLength();
            if (partitions < 1 || static_cast<size_t>(partitions) > MAX_PARTITIONS) return false;
            partitionCount += static_cast<size_t>(partitions);
            if (partitionCount > MAX_PARTITIONS) return false;
            for (int32_t partitionIndex = 0; partitionIndex < partitions; ++partitionIndex) {
                const int32_t partitionID = body.readInt32();
                Partition partition;
                partition.logStartOffset = body.readInt64();
                partition.nextOffset = body.readInt64();
                partition.timestamp = body.readInt64();
                if (partitionID < 0 || partition.logStartOffset < 0 ||
                    partition.nextOffset < partition.logStartOffset ||
                    topic.partitions.count(partitionID)) {
                    return false;
                }
                const int32_t recordCount = body.readArrayLength();
                int64_t expectedOffset = partition.logStartOffset;
                for (int32_t recordIndex = 0; recordIndex < recordCount; ++recordIndex) {
                    Record record = readRecord(body);
                    if (record.offset != expectedOffset++ || (record.control && !record.transactional) ||
                        (record.transactional && record.transactionalID.empty())) return false;
                    partition.bytes += recordBytes(record);
                    partition.records.push_back(std::move(record));
                }
                if (partition.nextOffset != expectedOffset) return false;
                const int32_t abortedCount = body.readArrayLength();
                for (int32_t i = 0; i < abortedCount; ++i) {
                    AbortedTransaction aborted;
                    aborted.producerID = body.readInt64();
                    aborted.firstOffset = body.readInt64();
                    if (aborted.producerID < 0 || aborted.firstOffset < partition.logStartOffset ||
                        aborted.firstOffset >= partition.nextOffset) return false;
                    partition.abortedTransactions.push_back(aborted);
                }
                topic.partitions.emplace(partitionID, std::move(partition));
            }
            loadedTopics.emplace(topic.name, std::move(topic));
        }
        const int32_t offsetCount = body.readArrayLength();
        std::map<std::string, Group> loadedGroups;
        for (int32_t i = 0; i < offsetCount; ++i) {
            const std::string group = body.readString();
            const std::string topic = body.readString();
            const int32_t partition = body.readInt32();
            const int64_t offset = body.readInt64();
            if (group.empty() || offset < 0 || partition < 0 || loadedTopics.count(topic) == 0 ||
                loadedTopics.at(topic).partitions.count(partition) == 0 ||
                !loadedGroups[group].offsets.emplace(std::make_pair(topic, partition), offset).second) {
                return false;
            }
        }
        std::map<std::string, ProducerState> loadedProducers;
        const int32_t producerCount = body.readArrayLength();
        for (int32_t i = 0; i < producerCount; ++i) {
            const std::string key = body.readString();
            ProducerState producer;
            producer.producerID = body.readInt64();
            producer.producerEpoch = body.readInt16();
            producer.stage = body.readBoolean() ? ProducerState::Stage::ONGOING : ProducerState::Stage::EMPTY;
            producer.transactionTimeoutMs = body.readInt32();
            producer.transactionStartedAt = body.readInt64();
            if (key.empty() || producer.producerID < 1 || producer.producerEpoch < 0 ||
                producer.transactionTimeoutMs < 0 || !loadedProducers.emplace(key, producer).second) return false;
            ProducerState& loaded = loadedProducers.at(key);
            const int32_t offsetCount = body.readArrayLength();
            for (int32_t j = 0; j < offsetCount; ++j) {
                const std::string topic = body.readString();
                const int32_t partition = body.readInt32();
                const int64_t offset = body.readInt64();
                if (loadedTopics.count(topic) == 0 || loadedTopics.at(topic).partitions.count(partition) == 0 ||
                    offset < 0 || !loaded.partitionOffsets.emplace(std::make_pair(topic, partition), offset).second) return false;
            }
            const int32_t partitionCount = body.readArrayLength();
            for (int32_t j = 0; j < partitionCount; ++j) {
                const std::string topic = body.readString();
                const int32_t partition = body.readInt32();
                if (loadedTopics.count(topic) == 0 || loadedTopics.at(topic).partitions.count(partition) == 0 ||
                    !loaded.addedPartitions.emplace(topic, partition).second) return false;
            }
            const int32_t groupCount = body.readArrayLength();
            for (int32_t j = 0; j < groupCount; ++j) {
                if (!loaded.pendingGroups.insert(body.readString()).second) return false;
            }
            const int32_t pendingCount = body.readArrayLength();
            for (int32_t j = 0; j < pendingCount; ++j) {
                const std::string group = body.readString();
                const std::string topic = body.readString();
                const int32_t partition = body.readInt32();
                const int64_t offset = body.readInt64();
                if (loaded.pendingGroups.count(group) == 0 || loadedTopics.count(topic) == 0 ||
                    loadedTopics.at(topic).partitions.count(partition) == 0 || offset < -1 ||
                    !loaded.pendingOffsets.emplace(std::make_tuple(group, topic, partition), offset).second) return false;
            }
        }
        std::map<int64_t, int16_t> loadedEpochs;
        const int32_t epochCount = body.readArrayLength();
        for (int32_t i = 0; i < epochCount; ++i) {
            const int64_t producerID = body.readInt64();
            const int16_t epoch = body.readInt16();
            if (producerID < 1 || epoch < 0 || !loadedEpochs.emplace(producerID, epoch).second) return false;
        }
        std::map<std::tuple<std::string, int32_t, int64_t>, SequenceState> loadedSequences;
        const int32_t sequenceCount = body.readArrayLength();
        for (int32_t i = 0; i < sequenceCount; ++i) {
            const std::string topic = body.readString();
            const int32_t partition = body.readInt32();
            const int64_t producerID = body.readInt64();
            SequenceState sequence;
            sequence.producerEpoch = body.readInt16();
            sequence.nextSequence = body.readInt32();
            if (loadedTopics.count(topic) == 0 || loadedTopics.at(topic).partitions.count(partition) == 0 ||
                loadedEpochs.count(producerID) == 0 || sequence.producerEpoch < 0 || sequence.nextSequence < 0 ||
                !loadedSequences.emplace(std::make_tuple(topic, partition, producerID), sequence).second) return false;
            SequenceState& loaded = loadedSequences.at(std::make_tuple(topic, partition, producerID));
            const int32_t offsetCount = body.readArrayLength();
            for (int32_t j = 0; j < offsetCount; ++j) {
                const int32_t number = body.readInt32();
                const int64_t offset = body.readInt64();
                if (number < 0 || offset < 0 || !loaded.offsets.emplace(number, offset).second) return false;
            }
        }
        int64_t loadedNextProducerID = body.readInt64();
        if (loadedNextProducerID < 1) return false;
        for (const auto& producer : loadedEpochs) loadedNextProducerID = std::max(loadedNextProducerID, producer.first + 1);
        if (body.remaining() != 0) return false;
        std::lock_guard<std::mutex> lock(mutex);
        topicMap = std::move(loadedTopics);
        groups = std::move(loadedGroups);
        producerStates = std::move(loadedProducers);
        producerEpochs = std::move(loadedEpochs);
        sequences = std::move(loadedSequences);
        nextProducerID = loadedNextProducerID;
        dirty = false;
        return true;
    } catch (const std::exception&) {
        return false;
    }
}

} // namespace Kafka
