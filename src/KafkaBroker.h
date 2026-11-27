#ifndef KAFKA_BROKER_H
#define KAFKA_BROKER_H

#include "Auth.h"
#include <chrono>
#include <cstdint>
#include <deque>
#include <map>
#include <mutex>
#include <optional>
#include <set>
#include <string>
#include <tuple>
#include <vector>

namespace Kafka {

struct BrokerError : std::runtime_error {
    int16_t code;
    explicit BrokerError(int16_t errorCode, const std::string& message)
        : std::runtime_error(message), code(errorCode) {}
};

struct RecordHeader {
    std::string key;
    std::vector<uint8_t> value;
};

struct Record {
    int64_t offset = 0;
    int64_t timestamp = 0;
    std::optional<std::vector<uint8_t>> key;
    std::optional<std::vector<uint8_t>> value;
    std::vector<RecordHeader> headers;
    int64_t producerID = -1;
    int16_t producerEpoch = -1;
    int32_t sequence = -1;
    bool transactional = false;
    bool control = false;
    bool controlCommit = false;
    std::string transactionalID;
};

struct TopicMetadata {
    std::string name;
    std::map<std::string, std::string> configs;
    int16_t error = 0;
};

struct PartitionMetadata {
    std::string topic;
    int32_t partition = 0;
    int32_t leader = 1;
    std::vector<int32_t> replicas;
    std::vector<int32_t> isr;
    int16_t error = 0;
};

struct ProducedRecord {
    std::optional<std::vector<uint8_t>> key;
    std::optional<std::vector<uint8_t>> value;
    int64_t timestamp = -1;
    std::vector<RecordHeader> headers;
    int64_t producerID = -1;
    int16_t producerEpoch = -1;
    int32_t sequence = -1;
};

struct ProduceResult {
    int16_t error = 0;
    int64_t baseOffset = -1;
    int64_t logStartOffset = 0;
};

struct AbortedTransaction {
    int64_t producerID = -1;
    int64_t firstOffset = -1;
};

struct FetchResult {
    int16_t error = 0;
    int64_t highWatermark = 0;
    int64_t lastStableOffset = 0;
    int64_t logStartOffset = 0;
    std::vector<AbortedTransaction> abortedTransactions;
    std::vector<Record> records;
};

struct OffsetResult {
    int64_t offset = -1;
    int64_t timestamp = -1;
    int16_t error = 0;
};

struct GroupMember {
    std::string memberID;
    std::string clientID;
    std::string clientHost;
    std::vector<uint8_t> metadata;
    std::vector<std::string> subscriptions;
    int32_t sessionTimeout = 30000;
    std::chrono::steady_clock::time_point lastHeartbeat;
};

struct GroupDescription {
    std::string groupID;
    std::string state;
    std::string protocolType;
    std::string protocol;
    int32_t generation = 0;
    std::vector<GroupMember> members;
    int16_t error = 0;
};

struct JoinGroupResult {
    int32_t generation = 0;
    std::string leaderID;
    std::string memberID;
    std::vector<GroupMember> members;
};

struct InitProducerResult {
    int16_t error = 0;
    int64_t producerID = -1;
    int16_t producerEpoch = -1;
};

class Broker {
public:
    explicit Broker(MQTT::Auth* auth = nullptr, int32_t brokerID = 1,
                    std::string stateFile = {}, size_t maxMessageBytes = 1024 * 1024,
                    size_t maxLogBytes = 1024LL * 1024 * 1024,
                    const std::string& advertisedHost = "localhost",
                    uint16_t advertisedPort = 9092);

    int32_t brokerID() const { return id; }
    const std::string& advertisedHost() const { return host; }
    uint16_t advertisedPort() const { return port; }
    size_t maxMessageBytes() const { return maxMessageSize; }

    void createTopic(const std::string& name, int32_t partitions, int16_t replicationFactor,
                     const std::map<std::string, std::string>& configs,
                     const std::string& username);
    void deleteTopic(const std::string& name, const std::string& username);
    std::vector<TopicMetadata> topics(const std::vector<std::string>& requested,
                                      bool allowAutoCreate, const std::string& username,
                                      bool allTopics = false);
    std::vector<PartitionMetadata> partitions(const std::string& topic) const;
    void validatePartitionElection(const std::string& topic, int32_t partition,
                                   const std::string& username);

    ProduceResult produce(const std::string& topic, int32_t partition,
                          std::vector<ProducedRecord>& records, int16_t requiredAcks,
                          const std::string& username, const std::string& transactionalID = {});
    FetchResult fetch(const std::string& topic, int32_t partition, int64_t offset,
                      int32_t maxBytes, const std::string& username,
                      bool readCommitted = false);
    OffsetResult listOffsets(const std::string& topic, int32_t partition,
                             int64_t timestamp, const std::string& username,
                             bool readCommitted = false);

    InitProducerResult initProducerID(const std::string& transactionalID,
                                      int32_t transactionTimeoutMs,
                                      const std::string& clientID,
                                      const std::string& username = {});
    std::map<std::pair<std::string, int32_t>, int16_t> addPartitionsToTxn(
        const std::string& transactionalID, int64_t producerID, int16_t producerEpoch,
        const std::vector<std::pair<std::string, std::vector<int32_t>>>& partitions,
        const std::string& username);
    void addOffsetsToTxn(const std::string& transactionalID, int64_t producerID,
                         int16_t producerEpoch, const std::string& group,
                         const std::string& username);
    void txnOffsetCommit(const std::string& transactionalID, int64_t producerID,
                         int16_t producerEpoch, int32_t generation,
                         const std::string& memberID, const std::string& group,
                         const std::string& topic, int32_t partition, int64_t offset,
                         const std::string& username);
    void endTxn(const std::string& transactionalID, int64_t producerID,
                int16_t producerEpoch, bool commit, const std::string& username);
    std::map<std::pair<std::string, int32_t>, int16_t> writeTxnMarkers(
        int64_t producerID, int16_t producerEpoch, int32_t coordinatorEpoch, bool commit,
        const std::vector<std::pair<std::string, std::vector<int32_t>>>& partitions,
        const std::string& username);

    void commitOffset(const std::string& group, int32_t generation,
                      const std::string& memberID, const std::string& topic,
                      int32_t partition, int64_t offset, const std::string& username);
    std::map<std::pair<std::string, int32_t>, OffsetResult>
    fetchOffsets(const std::string& group,
                 const std::vector<std::pair<std::string, int32_t>>& partitions,
                 const std::string& username) const;

    std::string findCoordinator() const;
    JoinGroupResult joinGroup(const std::string& group, const std::string& memberID,
                              const std::string& clientID, const std::string& clientHost,
                              int32_t sessionTimeout, int32_t heartbeatInterval,
                              const std::string& protocolType, const std::string& protocol,
                              int32_t rebalanceTimeout,
                              const std::vector<uint8_t>& metadata,
                              const std::string& username = {});
    void heartbeatGroup(const std::string& group, const std::string& memberID,
                        int32_t generation, const std::string& username = {});
    void leaveGroup(const std::string& group, const std::string& memberID,
                    const std::string& username = {});
    void syncGroup(const std::string& group, const std::string& memberID,
                   int32_t generation,
                   std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments,
                   const std::string& username = {});
    std::vector<uint8_t> memberAssignment(const std::string& group,
                                          const std::string& memberID,
                                          int32_t generation);
    std::vector<GroupDescription> describeGroups(const std::vector<std::string>& groups,
                                                 const std::string& username = {});

    bool load();
    bool flush();

    void failNextFileSyncForTest() { failNextFileSync = true; }
    void failNextDirectorySyncForTest() { failNextDirectorySync = true; }

private:
    struct Partition {
        int64_t logStartOffset = 0;
        int64_t nextOffset = 0;
        int64_t timestamp = -1;
        size_t bytes = 0;
        std::deque<Record> records;
        std::vector<AbortedTransaction> abortedTransactions;
    };
    struct Topic {
        std::string name;
        std::map<std::string, std::string> configs;
        std::map<int32_t, Partition> partitions;
    };
    struct ProducerState {
        enum class Stage { EMPTY, ONGOING };
        int64_t producerID = -1;
        int16_t producerEpoch = -1;
        Stage stage = Stage::EMPTY;
        int32_t transactionTimeoutMs = 0;
        int64_t transactionStartedAt = 0;
        std::map<std::pair<std::string, int32_t>, int64_t> partitionOffsets;
        std::set<std::pair<std::string, int32_t>> addedPartitions;
        std::set<std::string> pendingGroups;
        std::map<std::tuple<std::string, std::string, int32_t>, int64_t> pendingOffsets;
    };
    struct SequenceState {
        int16_t producerEpoch = -1;
        int32_t nextSequence = 0;
        std::map<int32_t, int64_t> offsets;
    };
    struct Group {
        enum class State { EMPTY, PREPARING_REBALANCE, COMPLETING_REBALANCE, STABLE, DEAD };
        std::string protocolType;
        std::string protocol;
        int32_t generation = 0;
        int32_t rebalanceTimeout = 30000;
        std::string leader;
        State state = State::EMPTY;
        std::map<std::string, GroupMember> members;
        std::map<std::pair<std::string, int32_t>, int64_t> offsets;
        std::optional<std::vector<std::pair<std::string, std::vector<uint8_t>>>> assignments;
    };

    MQTT::Auth* auth;
    int32_t id;
    std::string stateFile;
    size_t maxMessageSize;
    size_t maxLogSize;
    std::string host;
    uint16_t port;
    mutable std::mutex mutex;
    std::condition_variable groupChanged;
    std::map<std::string, Topic> topicMap;
    std::map<std::string, Group> groups;
    std::map<std::string, ProducerState> producerStates;
    std::map<std::tuple<std::string, int32_t, int64_t>, SequenceState> sequences;
    std::map<int64_t, int16_t> producerEpochs;
    int64_t nextProducerID = 1;
    bool dirty = false;
    mutable bool failNextFileSync = false;
    mutable bool failNextDirectorySync = false;

    Topic& requireTopic(const std::string& name, const std::string& username, bool write);
    void checkWritePermission(const std::string& topic, const std::string& username) const;
    void checkReadPermission(const std::string& topic, const std::string& username) const;
    void checkGroupPermission(const std::string& group, const std::string& username,
                              MQTT::Auth::KafkaOperation operation) const;
    void checkClusterPermission(const std::string& username) const;
    void enforceRetentionLocked(Topic& topic, Partition& partition);
    ProduceResult appendLocked(Topic& topic, Partition& partition,
                               std::vector<ProducedRecord>& records,
                               const std::string& transactionalID = {});
    ProducerState& requireProducerLocked(const std::string& transactionalID,
                                         int64_t producerID, int16_t producerEpoch,
                                         const std::string& username);
    int16_t checkSequenceLocked(const std::string& topic, int32_t partition,
                                const std::vector<ProducedRecord>& records,
                                int64_t& duplicateBaseOffset);
    void recordSequenceLocked(const std::string& topic, int32_t partition,
                              const std::vector<ProducedRecord>& records,
                              int64_t baseOffset);
    int64_t lastStableOffsetLocked(const Topic& topic, const Partition& partition) const;
    void appendTxnMarkerLocked(Topic& topic, Partition& partition,
                               const ProducerState& producer, const std::string& transactionalID,
                               int64_t firstOffset, bool commit);
    void finishTxnLocked(const std::string& transactionalID, ProducerState& producer, bool commit,
                         const std::set<std::pair<std::string, int32_t>>* selected = nullptr);
    bool flushLocked();
    void expireGroupsLocked();
    void expireTransactionsLocked();
    bool writeSnapshot(const std::string& path) const;
};

} // namespace Kafka

#endif
