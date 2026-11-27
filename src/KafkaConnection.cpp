#include "KafkaConnection.h"
#include "KafkaErrors.h"
#include "KafkaProtocol.h"
#include "Log.h"
#include <algorithm>
#include <chrono>
#include <cerrno>
#include <cstring>
#include <limits>
#include <thread>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <unistd.h>

namespace Kafka {
namespace {

struct APIRange {
    int16_t key;
    int16_t minimum;
    int16_t maximum;
};

constexpr int16_t PRODUCE = 0;
constexpr int16_t FETCH = 1;
constexpr int16_t LIST_OFFSETS = 2;
constexpr int16_t METADATA = 3;
constexpr int16_t OFFSET_COMMIT = 8;
constexpr int16_t OFFSET_FETCH = 9;
constexpr int16_t FIND_COORDINATOR = 10;
constexpr int16_t JOIN_GROUP = 11;
constexpr int16_t HEARTBEAT = 12;
constexpr int16_t LEAVE_GROUP = 13;
constexpr int16_t SYNC_GROUP = 14;
constexpr int16_t DESCRIBE_GROUPS = 15;
constexpr int16_t SASL_HANDSHAKE = 17;
constexpr int16_t API_VERSIONS = 18;
constexpr int16_t CREATE_TOPICS = 19;
constexpr int16_t DELETE_TOPICS = 20;
constexpr int16_t INIT_PRODUCER_ID = 22;
constexpr int16_t ADD_PARTITIONS_TO_TXN = 24;
constexpr int16_t ADD_OFFSETS_TO_TXN = 25;
constexpr int16_t END_TXN = 26;
constexpr int16_t WRITE_TXN_MARKERS = 27;
constexpr int16_t TXN_OFFSET_COMMIT = 28;
constexpr int16_t SASL_AUTHENTICATE = 36;
constexpr int16_t ALTER_PARTITIONS = 56;
constexpr int16_t ELECT_LEADERS = 43;

void skipTags(Reader& reader, const char* location) {
    const size_t tagOffset = reader.remaining();
    const uint32_t count = reader.readUnsignedVarint();
    if (count > reader.remaining()) throw ProtocolError("invalid Kafka tagged fields at " + std::string(location) +
                                                        " remaining-byte offset " + std::to_string(tagOffset) +
                                                        " with count " + std::to_string(count));
    for (uint32_t i = 0; i < count; ++i) {
        (void)reader.readUnsignedVarint();
        const uint32_t size = reader.readUnsignedVarint();
        (void)reader.readRaw(size);
    }
}

const std::vector<APIRange>& apiRanges() {
    static const std::vector<APIRange> ranges = {
        {PRODUCE, 0, 3},          {FETCH, 0, 4},
        {METADATA, 0, 1},         {LIST_OFFSETS, 0, 2},
        {OFFSET_COMMIT, 0, 2},    {OFFSET_FETCH, 0, 1},
        {FIND_COORDINATOR, 0, 0}, {JOIN_GROUP, 0, 1},
        {HEARTBEAT, 0, 0},        {LEAVE_GROUP, 0, 0},
        {SYNC_GROUP, 0, 0},       {DESCRIBE_GROUPS, 0, 0},
        {API_VERSIONS, 0, 2},     {CREATE_TOPICS, 0, 0},
        {DELETE_TOPICS, 0, 0},    {SASL_HANDSHAKE, 0, 1},
        {SASL_AUTHENTICATE, 0, 0},
        {INIT_PRODUCER_ID, 0, 0},  {ADD_PARTITIONS_TO_TXN, 0, 0},
        {ADD_OFFSETS_TO_TXN, 0, 0}, {END_TXN, 0, 2},
        {WRITE_TXN_MARKERS, 0, 0}, {TXN_OFFSET_COMMIT, 0, 1},
        {ALTER_PARTITIONS, 0, 0}, {ELECT_LEADERS, 0, 1},
    };
    return ranges;
}

bool supported(const RequestHeader& header) {
    for (const APIRange& range : apiRanges()) {
        if (range.key == header.apiKey) {
            return header.apiVersion >= range.minimum && header.apiVersion <= range.maximum;
        }
    }
    return false;
}

std::string peerHost(int descriptor) {
    sockaddr_storage address{};
    socklen_t addressLength = sizeof(address);
    if (::getpeername(descriptor, reinterpret_cast<sockaddr*>(&address), &addressLength) != 0) return {};
    char host[INET6_ADDRSTRLEN]{};
    const void* source = nullptr;
    if (address.ss_family == AF_INET) {
        source = &reinterpret_cast<sockaddr_in*>(&address)->sin_addr;
    } else if (address.ss_family == AF_INET6) {
        source = &reinterpret_cast<sockaddr_in6*>(&address)->sin6_addr;
    }
    return source && ::inet_ntop(address.ss_family, source, host, sizeof(host)) ? host : "";
}

void writeApiVersions(Writer& writer, int16_t version, int16_t error = Error::NONE) {
    writer.writeInt16(error);
    writer.writeArrayLength(apiRanges().size());
    for (const APIRange& range : apiRanges()) {
        writer.writeInt16(range.key);
        writer.writeInt16(range.minimum);
        writer.writeInt16(range.maximum);
    }
    if (version >= 1) writer.writeInt32(0);
}

void writeMetadata(Writer& writer, Broker& broker, Reader& reader,
                   int16_t version, const std::string& username) {
    const int32_t requestedCount = reader.readNullableArrayLength();
    std::vector<std::string> requested;
    for (int32_t i = 0; i < requestedCount; ++i) requested.push_back(reader.readString());
    // Kafka Metadata v0-v3 requests contain only Topics; v1 adds fields to the response.

    writer.writeArrayLength(1);
    writer.writeInt32(broker.brokerID());
    writer.writeString(broker.advertisedHost());
    writer.writeInt32(broker.advertisedPort());
    if (version >= 1) {
        writer.writeNullableString(nullptr);
    }

    const std::vector<TopicMetadata> topics = broker.topics(requested, true, username,
                                                            requestedCount < 0);
    if (version >= 1) writer.writeInt32(broker.brokerID());
    writer.writeArrayLength(topics.size());
    for (const TopicMetadata& topic : topics) {
        writer.writeInt16(topic.error);
        writer.writeString(topic.name);
        if (version >= 1) writer.writeBoolean(false);
        const std::vector<PartitionMetadata> partitions = broker.partitions(topic.name);
        writer.writeArrayLength(partitions.size());
        for (const PartitionMetadata& partition : partitions) {
            writer.writeInt16(partition.error);
            writer.writeInt32(partition.partition);
            writer.writeInt32(partition.leader);
            writer.writeArrayLength(partition.replicas.size());
            for (int32_t replica : partition.replicas) writer.writeInt32(replica);
            writer.writeArrayLength(partition.isr.size());
            for (int32_t replica : partition.isr) writer.writeInt32(replica);
        }
    }
}

void handleProduce(Writer& writer, Broker& broker, Reader& reader,
                   int16_t version, const std::string& username, bool& suppressResponse) {
    std::string transactionalID;
    // Kafka Produce v3 adds transactionalId; v2 still starts with requiredAcks.
    if (version >= 3) transactionalID = reader.readNullableString();
    const int16_t requiredAcks = reader.readInt16();
    suppressResponse = requiredAcks == 0;
    (void)reader.readInt32();
    const int32_t topicCount = reader.readArrayLength();
    struct PartitionResult {
        int32_t partition;
        ProduceResult result;
    };
    struct TopicResult {
        std::string topic;
        std::vector<PartitionResult> partitions;
    };
    std::vector<TopicResult> results;
    for (int32_t topicIndex = 0; topicIndex < topicCount; ++topicIndex) {
        TopicResult topic;
        topic.topic = reader.readString();
        const int32_t partitionCount = reader.readArrayLength();
        for (int32_t partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            const int32_t partitionID = reader.readInt32();
            const std::vector<uint8_t> messageSet = version >= 3
                ? reader.readNullableBytes() : reader.readBytes();
            try {
                std::vector<ProducedRecord> records =
                    decodeProduceRecords(messageSet, version, broker.maxMessageBytes(),
                                         !transactionalID.empty());
                ProduceResult result = broker.produce(topic.topic, partitionID, records,
                                                      requiredAcks, username, transactionalID);
                topic.partitions.push_back({partitionID, result});
            } catch (const BrokerError& error) {
                topic.partitions.push_back({partitionID, {error.code, -1, 0}});
            }
        }
        results.push_back(std::move(topic));
    }
    writer.writeArrayLength(results.size());
    for (const TopicResult& topic : results) {
        writer.writeString(topic.topic);
        writer.writeArrayLength(topic.partitions.size());
        for (const PartitionResult& partition : topic.partitions) {
            writer.writeInt32(partition.partition);
            writer.writeInt16(partition.result.error);
            writer.writeInt64(partition.result.baseOffset);
            if (version >= 2) writer.writeInt64(-1);
        }
    }
    if (version >= 1) writer.writeInt32(0);
}

void handleFetch(Writer& writer, Broker& broker, Reader& reader,
                 int16_t version, const std::string& username) {
    (void)reader.readInt32();
    const int32_t maxWait = reader.readInt32();
    const int32_t minBytes = reader.readInt32();
    const int32_t maxBytes = version >= 3 ? reader.readInt32() : std::numeric_limits<int32_t>::max();
    const bool readCommitted = version >= 4 && reader.readInt8() == 1;
    struct FetchPartition {
        int32_t partition;
        int64_t offset;
        int32_t maxBytes;
        FetchResult result;
    };
    struct FetchTopic {
        std::string topic;
        std::vector<FetchPartition> partitions;
    };
    std::vector<FetchTopic> topics;
    const int32_t topicCount = reader.readArrayLength();
    auto responseBytes = [version](const FetchResult& result) {
        return result.error == Error::NONE ? encodeMessageSet(result.records, version).size() : 0;
    };
    size_t receivedBytes = 0;
    auto refreshPartition = [&](FetchTopic& topic, FetchPartition& partition) {
        const size_t remaining = maxBytes < 0 || receivedBytes >= static_cast<size_t>(maxBytes)
            ? 0 : static_cast<size_t>(maxBytes) - receivedBytes;
        const int32_t allowance = partition.maxBytes < 0 ? partition.maxBytes : static_cast<int32_t>(
            std::min<size_t>(static_cast<size_t>(partition.maxBytes), remaining));
        partition.result = broker.fetch(topic.topic, partition.partition, partition.offset,
                                        allowance, username, readCommitted);
        size_t encoded = responseBytes(partition.result);
        if (encoded > remaining || encoded + partition.result.abortedTransactions.size() * 16 > remaining) {
            partition.result.records.clear();
            partition.result.abortedTransactions.clear();
            encoded = 0;
        }
        receivedBytes += encoded;
    };
    for (int32_t topicIndex = 0; topicIndex < topicCount; ++topicIndex) {
        FetchTopic topic;
        topic.topic = reader.readString();
        const int32_t partitionCount = reader.readArrayLength();
        for (int32_t partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            FetchPartition partition;
            partition.partition = reader.readInt32();
            partition.offset = reader.readInt64();
            partition.maxBytes = reader.readInt32();
            refreshPartition(topic, partition);
            topic.partitions.push_back(std::move(partition));
        }
        topics.push_back(std::move(topic));
    }
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(std::clamp(maxWait, 0, 30000));
    while (receivedBytes < static_cast<size_t>(std::max(minBytes, 0)) &&
           std::chrono::steady_clock::now() < deadline) {
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        receivedBytes = 0;
        for (FetchTopic& topic : topics) {
            for (FetchPartition& partition : topic.partitions) {
                refreshPartition(topic, partition);
            }
        }
    }
    if (version >= 1) writer.writeInt32(0);
    writer.writeArrayLength(topics.size());
    for (const FetchTopic& topic : topics) {
        writer.writeString(topic.topic);
        writer.writeArrayLength(topic.partitions.size());
        for (const FetchPartition& partition : topic.partitions) {
            writer.writeInt32(partition.partition);
            writer.writeInt16(partition.result.error);
            writer.writeInt64(partition.result.highWatermark);
            if (version >= 4) {
                writer.writeInt64(partition.result.lastStableOffset);
                if (readCommitted) {
                    writer.writeArrayLength(partition.result.abortedTransactions.size());
                    for (const AbortedTransaction& aborted : partition.result.abortedTransactions) {
                        writer.writeInt64(aborted.producerID);
                        writer.writeInt64(aborted.firstOffset);
                    }
                } else {
                    writer.writeInt32(-1);
                }
            }
            const std::vector<uint8_t> records = partition.result.error == Error::NONE
                                                    ? encodeMessageSet(partition.result.records, version)
                                                    : std::vector<uint8_t>{};
            writer.writeBytes(records);
        }
    }
}

void handleListOffsets(Writer& writer, Broker& broker, Reader& reader,
                       int16_t version, const std::string& username) {
    (void)reader.readInt32();
    const bool readCommitted = version >= 2 && reader.readInt8() == 1;
    const int32_t topicCount = reader.readArrayLength();
    struct TopicOffsets {
        std::string topic;
        std::vector<std::pair<int32_t, OffsetResult>> partitions;
    };
    std::vector<TopicOffsets> topics;
    for (int32_t topicIndex = 0; topicIndex < topicCount; ++topicIndex) {
        TopicOffsets topic;
        topic.topic = reader.readString();
        const int32_t partitionCount = reader.readArrayLength();
        for (int32_t partitionIndex = 0; partitionIndex < partitionCount; ++partitionIndex) {
            const int32_t partition = reader.readInt32();
            const int64_t timestamp = reader.readInt64();
            if (version == 0) (void)reader.readInt32();
            topic.partitions.push_back({partition, broker.listOffsets(topic.topic, partition,
                                                                      timestamp, username,
                                                                      readCommitted)});
        }
        topics.push_back(std::move(topic));
    }
    if (version >= 2) writer.writeInt32(0);
    writer.writeArrayLength(topics.size());
    for (const TopicOffsets& topic : topics) {
        writer.writeString(topic.topic);
        writer.writeArrayLength(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            writer.writeInt32(partition.first);
            writer.writeInt16(partition.second.error);
            if (version == 0) {
                writer.writeArrayLength(partition.second.error == Error::NONE ? 1 : 0);
                if (partition.second.error == Error::NONE) writer.writeInt64(partition.second.offset);
            } else if (partition.second.error == Error::NONE) {
                writer.writeInt64(partition.second.timestamp);
                writer.writeInt64(partition.second.offset);
            } else {
                writer.writeInt64(-1);
                writer.writeInt64(-1);
            }
        }
    }
}

void handleOffsetCommit(Writer& writer, Broker& broker, Reader& reader,
                        int16_t version, const std::string& username) {
    const std::string group = reader.readString();
    int32_t generation = -1;
    std::string memberID;
    if (version >= 1) {
        generation = reader.readInt32();
        memberID = reader.readString();
    }
    if (version >= 2) (void)reader.readInt64();
    struct TopicCommits {
        std::string topic;
        std::vector<std::pair<int32_t, int16_t>> partitions;
    };
    std::vector<TopicCommits> results;
    const int32_t topicCount = reader.readArrayLength();
    for (int32_t i = 0; i < topicCount; ++i) {
        TopicCommits topic;
        topic.topic = reader.readString();
        const int32_t partitionCount = reader.readArrayLength();
        for (int32_t j = 0; j < partitionCount; ++j) {
            const int32_t partition = reader.readInt32();
            const int64_t offset = reader.readInt64();
            if (version == 1) (void)reader.readInt64();
            (void)reader.readNullableString();
            try {
                broker.commitOffset(group, generation, memberID, topic.topic,
                                    partition, offset, username);
                topic.partitions.push_back({partition, Error::NONE});
            } catch (const BrokerError& error) {
                topic.partitions.push_back({partition, error.code});
            }
        }
        results.push_back(std::move(topic));
    }
    writer.writeArrayLength(results.size());
    for (const TopicCommits& topic : results) {
        writer.writeString(topic.topic);
        writer.writeArrayLength(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            writer.writeInt32(partition.first);
            writer.writeInt16(partition.second);
        }
    }
}

void handleOffsetFetch(Writer& writer, Broker& broker, Reader& reader,
                       int16_t version, const std::string& username) {
    const std::string group = reader.readString();
    std::vector<std::pair<std::string, int32_t>> requested;
    if (version >= 1) {
        const int32_t topicCount = reader.readNullableArrayLength();
        if (topicCount >= 0) {
            for (int32_t i = 0; i < topicCount; ++i) {
                const std::string topic = reader.readString();
                const int32_t partitions = reader.readArrayLength();
                for (int32_t j = 0; j < partitions; ++j) {
                    requested.push_back({topic, reader.readInt32()});
                }
            }
        }
    } else {
        const int32_t topicCount = reader.readArrayLength();
        for (int32_t i = 0; i < topicCount; ++i) {
            const std::string topic = reader.readString();
            const int32_t partitions = reader.readArrayLength();
            for (int32_t j = 0; j < partitions; ++j) {
                requested.push_back({topic, reader.readInt32()});
            }
        }
    }
    const auto offsets = broker.fetchOffsets(group, requested, username);
    std::map<std::string, std::vector<std::pair<int32_t, OffsetResult>>> topics;
    for (const auto& entry : offsets) topics[entry.first.first].push_back({entry.first.second, entry.second});
    if (version >= 1 && topics.empty()) writer.writeInt32(-1);
    else writer.writeArrayLength(topics.size());
    for (const auto& topic : topics) {
        writer.writeString(topic.first);
        writer.writeArrayLength(topic.second.size());
        for (const auto& partition : topic.second) {
            writer.writeInt32(partition.first);
            writer.writeInt64(partition.second.offset);
            writer.writeNullableString(nullptr);
            writer.writeInt16(partition.second.error);
        }
    }
}

void handleFindCoordinator(Writer& writer, Broker& broker, Reader& reader, int16_t version) {
    (void)reader.readString();
    if (version >= 1) (void)reader.readInt8();
    if (version >= 1) writer.writeInt32(0);
    writer.writeInt16(Error::NONE);
    if (version >= 1) writer.writeNullableString(nullptr);
    writer.writeInt32(broker.brokerID());
    writer.writeString(broker.advertisedHost());
    writer.writeInt32(broker.advertisedPort());
}

void handleJoinGroup(Writer& writer, Broker& broker, Reader& reader,
                     int16_t version, int descriptor, const std::string& clientID,
                     const std::string& username) {
    const std::string group = reader.readString();
    const int32_t sessionTimeout = reader.readInt32();
    const int32_t rebalanceTimeout = version >= 1 ? reader.readInt32() : sessionTimeout;
    const std::string memberID = reader.readString();
    const std::string protocolType = reader.readString();
    std::string protocol;
    std::vector<uint8_t> metadata;
    const int32_t protocolCount = reader.readArrayLength();
    for (int32_t i = 0; i < protocolCount; ++i) {
        const std::string name = reader.readString();
        const std::vector<uint8_t> bytes = reader.readNullableBytes();
        if (protocol.empty()) {
            protocol = name;
            metadata = bytes;
        }
    }
    const std::string clientHost = peerHost(descriptor);
    const int32_t heartbeatInterval = std::max(1000, sessionTimeout / 3);
    try {
        const JoinGroupResult groupResult = broker.joinGroup(
            group, memberID, clientID, clientHost, sessionTimeout, heartbeatInterval,
            protocolType, protocol, rebalanceTimeout, metadata, username);
        writer.writeInt16(Error::NONE);
        writer.writeInt32(groupResult.generation);
        writer.writeString(protocol);
        writer.writeString(groupResult.leaderID);
        writer.writeString(groupResult.memberID);
        writer.writeArrayLength(groupResult.members.size());
        for (const GroupMember& member : groupResult.members) {
            writer.writeString(member.memberID);
            writer.writeNullableBytes(&member.metadata);
        }
    } catch (const BrokerError& error) {
        writer.writeInt16(error.code);
        writer.writeInt32(-1);
        writer.writeString("");
        writer.writeString("");
        writer.writeString("");
        writer.writeArrayLength(0);
    }
}

void handleHeartbeat(Writer& writer, Broker& broker, Reader& reader, int16_t version,
                     const std::string& username) {
    const std::string group = reader.readString();
    const int32_t generation = reader.readInt32();
    const std::string memberID = reader.readString();
    if (version >= 1) writer.writeInt32(0);
    try {
        broker.heartbeatGroup(group, memberID, generation, username);
        writer.writeInt16(Error::NONE);
    } catch (const BrokerError& error) {
        writer.writeInt16(error.code);
    }
}

void handleLeaveGroup(Writer& writer, Broker& broker, Reader& reader, int16_t version,
                      const std::string& username) {
    const std::string group = reader.readString();
    const std::string memberID = reader.readString();
    if (version >= 1) writer.writeInt32(0);
    try {
        broker.leaveGroup(group, memberID, username);
        writer.writeInt16(Error::NONE);
    } catch (const BrokerError& error) {
        writer.writeInt16(error.code);
    }
}

void handleSyncGroup(Writer& writer, Broker& broker, Reader& reader, int16_t version,
                     const std::string& username) {
    const std::string group = reader.readString();
    const int32_t generation = reader.readInt32();
    const std::string memberID = reader.readString();
    const int32_t assignmentCount = reader.readArrayLength();
    std::vector<std::pair<std::string, std::vector<uint8_t>>> assignments;
    for (int32_t i = 0; i < assignmentCount; ++i) {
        assignments.emplace_back(reader.readString(), reader.readBytes());
    }
    if (version >= 1) writer.writeInt32(0);
    try {
        broker.syncGroup(group, memberID, generation, std::move(assignments), username);
        const std::vector<uint8_t> result = broker.memberAssignment(group, memberID, generation);
        writer.writeInt16(Error::NONE);
        writer.writeBytes(result);
    } catch (const BrokerError& error) {
        writer.writeInt16(error.code);
        writer.writeNullableBytes(nullptr);
    }
}

void handleDescribeGroups(Writer& writer, Broker& broker, Reader& reader, int16_t version,
                          const std::string& username) {
    const std::vector<std::string> groups = reader.readStringArray();
    const std::vector<GroupDescription> descriptions = broker.describeGroups(groups, username);
    if (version >= 1) writer.writeInt32(0);
    writer.writeArrayLength(descriptions.size());
    for (const GroupDescription& group : descriptions) {
        writer.writeInt16(group.error);
        writer.writeString(group.groupID);
        writer.writeString(group.state);
        writer.writeString(group.protocolType);
        writer.writeString(group.protocol);
        writer.writeArrayLength(group.members.size());
        for (const GroupMember& member : group.members) {
            writer.writeString(member.memberID);
            writer.writeString(member.clientID);
            writer.writeString(member.clientHost);
            writer.writeNullableBytes(&member.metadata);
            writer.writeNullableBytes(nullptr);
        }
    }
}

void handleCreateTopics(Writer& writer, Broker& broker, Reader& reader,
                        const std::string& username) {
    const int32_t count = reader.readArrayLength();
    struct TopicResult {
        std::string topic;
        int16_t error;
    };
    std::vector<TopicResult> results;
    for (int32_t i = 0; i < count; ++i) {
        const std::string topic = reader.readString();
        const int32_t partitions = reader.readInt32();
        const int16_t replicationFactor = reader.readInt16();
        const int32_t assignments = reader.readNullableArrayLength();
        for (int32_t j = 0; j < assignments; ++j) {
            (void)reader.readInt32();
            const int32_t replicas = reader.readArrayLength();
            for (int32_t k = 0; k < replicas; ++k) (void)reader.readInt32();
        }
        std::map<std::string, std::string> configs;
        const int32_t configCount = reader.readNullableArrayLength();
        for (int32_t j = 0; j < configCount; ++j) {
            configs[reader.readString()] = reader.readNullableString();
        }
        try {
            if (assignments > 0) throw BrokerError(Error::INVALID_REPLICA_ASSIGNMENT,
                                                  "manual replica assignment is unsupported");
            broker.createTopic(topic, partitions, replicationFactor, configs, username);
            results.push_back({topic, Error::NONE});
        } catch (const BrokerError& error) {
            results.push_back({topic, error.code});
        }
    }
    (void)reader.readInt32();
    writer.writeArrayLength(results.size());
    for (const TopicResult& result : results) {
        writer.writeString(result.topic);
        writer.writeInt16(result.error);
    }
}

void handleDeleteTopics(Writer& writer, Broker& broker, Reader& reader,
                        const std::string& username) {
    const std::vector<std::string> topics = reader.readStringArray();
    (void)reader.readInt32();
    writer.writeArrayLength(topics.size());
    for (const std::string& topic : topics) {
        writer.writeString(topic);
        try {
            broker.deleteTopic(topic, username);
            writer.writeInt16(Error::NONE);
        } catch (const BrokerError& error) {
            writer.writeInt16(error.code);
        }
    }
}

void handleElectLeaders(Writer& writer, Broker& broker, Reader& reader,
                       int16_t version, const std::string& username) {
    int8_t electionType = 0;
    if (version >= 1) electionType = reader.readInt8();
    struct TopicResult {
        std::string topic;
        std::vector<std::pair<int32_t, int16_t>> partitions;
    };
    std::vector<TopicResult> results;
    const int32_t topicCount = reader.readArrayLength();
    for (int32_t i = 0; i < topicCount; ++i) {
        TopicResult topic;
        topic.topic = reader.readString();
        const int32_t partitionCount = reader.readArrayLength();
        for (int32_t j = 0; j < partitionCount; ++j) {
            const int32_t partition = reader.readInt32();
            int16_t error = Error::NONE;
            try {
                if (electionType != 0) throw BrokerError(Error::INVALID_REQUEST,
                                                         "unclean election is unsupported");
                broker.validatePartitionElection(topic.topic, partition, username);
            } catch (const BrokerError& failure) {
                error = failure.code;
            }
            topic.partitions.emplace_back(partition, error);
        }
        results.push_back(std::move(topic));
    }
    (void)reader.readInt32();
    writer.writeInt32(0);
    if (version >= 1) writer.writeInt16(Error::NONE);
    writer.writeArrayLength(results.size());
    for (const TopicResult& topic : results) {
        writer.writeString(topic.topic);
        writer.writeArrayLength(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            writer.writeInt32(partition.first);
            writer.writeInt16(partition.second);
            writer.writeNullableString(nullptr);
        }
    }
}

void handleAlterPartitions(Writer& writer, Broker& broker, Reader& reader,
                           const std::string& username) {
    struct PartitionResult {
        int32_t partition;
        int32_t leader;
        int16_t error;
    };
    struct TopicResult {
        std::string topic;
        std::vector<PartitionResult> partitions;
    };
    std::vector<TopicResult> results;
    int16_t topLevelError = Error::NONE;
    const int32_t topicCount = reader.readCompactArrayLength();
    for (int32_t i = 0; i < topicCount; ++i) {
        TopicResult topic;
        topic.topic = reader.readCompactString();
        const int32_t partitionCount = reader.readCompactArrayLength();
        for (int32_t j = 0; j < partitionCount; ++j) {
            const int32_t partition = reader.readInt32();
            const int32_t leader = reader.readInt32();
            const int32_t replicaCount = reader.readCompactArrayLength();
            std::vector<int32_t> isr;
            for (int32_t k = 0; k < replicaCount; ++k) isr.push_back(reader.readInt32());
            skipTags(reader, "partition");
            int16_t error = Error::NONE;
            try {
                if (leader != broker.brokerID()) throw BrokerError(Error::INVALID_REPLICA_ASSIGNMENT,
                                                                   "leader must be the single broker");
                if (isr.empty() || isr.size() != 1 || isr.front() != broker.brokerID()) {
                    throw BrokerError(Error::INVALID_REPLICA_ASSIGNMENT,
                                      "single-node ISR must contain only this broker");
                }
                broker.validatePartitionElection(topic.topic, partition, username);
            } catch (const BrokerError& failure) {
                error = failure.code;
            }
            topic.partitions.push_back({partition, leader, error});
        }
        skipTags(reader, "topic");
        results.push_back(std::move(topic));
    }
    // AlterPartitions v0 request schema: Topics, TimeoutMs, TaggedFields.
    (void)reader.readInt32();
    skipTags(reader, "request body");
    writer.writeInt32(0);
    writer.writeInt16(topLevelError);
    writer.writeCompactArrayLength(results.size());
    for (const TopicResult& topic : results) {
        writer.writeCompactString(topic.topic);
        writer.writeCompactArrayLength(topic.partitions.size());
        for (const PartitionResult& partition : topic.partitions) {
            writer.writeInt32(partition.partition);
            writer.writeInt32(partition.leader);
            writer.writeInt32(0);
            writer.writeCompactArrayLength(1);
            writer.writeInt32(broker.brokerID());
            writer.writeCompactArrayLength(1);
            writer.writeInt32(broker.brokerID());
            writer.writeInt16(partition.error);
            writer.writeCompactNullableString(nullptr);
            writer.writeUnsignedVarint(0);
        }
        writer.writeUnsignedVarint(0);
    }
    writer.writeUnsignedVarint(0);
}

void handleInitProducerID(Writer& writer, Broker& broker, Reader& reader,
                          const std::string& clientID, const std::string& username) {
    const std::string transactionalID = reader.readNullableString();
    const int32_t timeout = reader.readInt32();
    const InitProducerResult result = broker.initProducerID(transactionalID, timeout, clientID, username);
    writer.writeInt32(0);
    writer.writeInt16(result.error);
    writer.writeInt64(result.producerID);
    writer.writeInt16(result.producerEpoch);
}

void handleAddPartitionsToTxn(Writer& writer, Broker& broker, Reader& reader,
                              const std::string& username) {
    const std::string transactionalID = reader.readString();
    const int64_t producerID = reader.readInt64();
    const int16_t producerEpoch = reader.readInt16();
    std::vector<std::pair<std::string, std::vector<int32_t>>> requested;
    const int32_t topicCount = reader.readArrayLength();
    for (int32_t i = 0; i < topicCount; ++i) {
        std::string topic = reader.readString();
        const int32_t partitionCount = reader.readArrayLength();
        std::vector<int32_t> partitions;
        for (int32_t j = 0; j < partitionCount; ++j) partitions.push_back(reader.readInt32());
        requested.push_back({std::move(topic), std::move(partitions)});
    }
    writer.writeInt32(0);
    std::map<std::pair<std::string, int32_t>, int16_t> results;
    try {
        results = broker.addPartitionsToTxn(transactionalID, producerID, producerEpoch,
                                            requested, username);
    } catch (const BrokerError& error) {
        for (const auto& topic : requested) {
            for (int32_t partition : topic.second) results[{topic.first, partition}] = error.code;
        }
    }
    writer.writeArrayLength(requested.size());
    for (const auto& topic : requested) {
        writer.writeString(topic.first);
        writer.writeArrayLength(topic.second.size());
        for (int32_t partition : topic.second) {
            writer.writeInt32(partition);
            writer.writeInt16(results[{topic.first, partition}]);
        }
    }
}

void handleAddOffsetsToTxn(Writer& writer, Broker& broker, Reader& reader,
                           const std::string& username) {
    const std::string transactionalID = reader.readString();
    const int64_t producerID = reader.readInt64();
    const int16_t producerEpoch = reader.readInt16();
    const std::string group = reader.readString();
    writer.writeInt32(0);
    try {
        broker.addOffsetsToTxn(transactionalID, producerID, producerEpoch, group, username);
        writer.writeInt16(Error::NONE);
    } catch (const BrokerError& error) {
        writer.writeInt16(error.code);
    }
}

void handleTxnOffsetCommit(Writer& writer, Broker& broker, Reader& reader,
                           int16_t version, const std::string& username) {
    const std::string transactionalID = reader.readString();
    const std::string group = reader.readString();
    const int64_t producerID = reader.readInt64();
    const int16_t producerEpoch = reader.readInt16();
    int32_t generation = -1;
    std::string memberID;
    if (version >= 3) {
        generation = reader.readInt32();
        memberID = reader.readString();
    }
    struct TopicResult {
        std::string topic;
        std::vector<std::pair<int32_t, int16_t>> partitions;
    };
    std::vector<TopicResult> results;
    const int32_t topicCount = reader.readArrayLength();
    for (int32_t i = 0; i < topicCount; ++i) {
        TopicResult topic;
        topic.topic = reader.readString();
        const int32_t partitionCount = reader.readArrayLength();
        for (int32_t j = 0; j < partitionCount; ++j) {
            const int32_t partition = reader.readInt32();
            const int64_t offset = reader.readInt64();
            (void)reader.readNullableString();
            try {
                broker.txnOffsetCommit(transactionalID, producerID, producerEpoch, generation,
                                       memberID, group, topic.topic, partition, offset, username);
                topic.partitions.push_back({partition, Error::NONE});
            } catch (const BrokerError& error) {
                topic.partitions.push_back({partition, error.code});
            }
        }
        results.push_back(std::move(topic));
    }
    writer.writeInt32(0);
    writer.writeArrayLength(results.size());
    for (const TopicResult& topic : results) {
        writer.writeString(topic.topic);
        writer.writeArrayLength(topic.partitions.size());
        for (const auto& partition : topic.partitions) {
            writer.writeInt32(partition.first);
            writer.writeInt16(partition.second);
        }
    }
}

void handleEndTxn(Writer& writer, Broker& broker, Reader& reader,
                  const std::string& username) {
    const std::string transactionalID = reader.readString();
    const int64_t producerID = reader.readInt64();
    const int16_t producerEpoch = reader.readInt16();
    const bool commit = reader.readBoolean();
    writer.writeInt32(0);
    try {
        broker.endTxn(transactionalID, producerID, producerEpoch, commit, username);
        writer.writeInt16(Error::NONE);
    } catch (const BrokerError& error) {
        writer.writeInt16(error.code);
    }
}

void handleWriteTxnMarkers(Writer& writer, Broker& broker, Reader& reader,
                           const std::string& username) {
    const int32_t markerCount = reader.readArrayLength();
    struct Marker {
        int64_t producerID;
        int16_t producerEpoch;
        int32_t coordinatorEpoch;
        bool commit;
        std::vector<std::pair<std::string, std::vector<int32_t>>> partitions;
        std::map<std::pair<std::string, int32_t>, int16_t> errors;
    };
    std::vector<Marker> markers;
    for (int32_t i = 0; i < markerCount; ++i) {
        Marker marker;
        marker.producerID = reader.readInt64();
        marker.producerEpoch = reader.readInt16();
        marker.commit = reader.readBoolean();
        const int32_t topicCount = reader.readArrayLength();
        for (int32_t j = 0; j < topicCount; ++j) {
            std::string topic = reader.readString();
            const int32_t partitionCount = reader.readArrayLength();
            std::vector<int32_t> partitions;
            for (int32_t k = 0; k < partitionCount; ++k) partitions.push_back(reader.readInt32());
            marker.partitions.emplace_back(std::move(topic), std::move(partitions));
        }
        try {
            marker.coordinatorEpoch = reader.readInt32();
            marker.errors = broker.writeTxnMarkers(marker.producerID, marker.producerEpoch,
                                                   marker.coordinatorEpoch, marker.commit, marker.partitions,
                                                   username);
        } catch (const BrokerError& error) {
            for (const auto& topic : marker.partitions) {
                for (int32_t partition : topic.second) marker.errors[{topic.first, partition}] = error.code;
            }
        }
        markers.push_back(std::move(marker));
    }
    writer.writeArrayLength(markers.size());
    for (const Marker& marker : markers) {
        writer.writeInt64(marker.producerID);
        writer.writeArrayLength(marker.partitions.size());
        for (const auto& topic : marker.partitions) {
            writer.writeString(topic.first);
            writer.writeArrayLength(topic.second.size());
            for (int32_t partition : topic.second) {
                writer.writeInt32(partition);
                writer.writeInt16(marker.errors.at({topic.first, partition}));
            }
        }
    }
}

void handleSASLHandshake(Writer& writer, Reader& reader) {
    const std::string mechanism = reader.readString();
    writer.writeInt16(mechanism == "PLAIN" ? Error::NONE : Error::UNSUPPORTED_SASL_MECHANISM);
    writer.writeStringArray({"PLAIN"});
}

void handleSASLAuthenticate(Writer& writer, Reader& reader, MQTT::Auth& auth,
                            bool& authenticated, std::string& username,
                            bool& authenticationFailed) {
    const std::vector<uint8_t> token = reader.readNullableBytes();
    const std::vector<size_t> separators = [&token]() {
        std::vector<size_t> positions;
        for (size_t i = 0; i < token.size(); ++i) {
            if (token[i] == 0) positions.push_back(i);
        }
        return positions;
    }();
    if (separators.size() != 2) {
        const std::string message = "invalid PLAIN token";
        authenticationFailed = true;
        writer.writeInt16(Error::ILLEGAL_SASL_STATE);
        writer.writeNullableString(&message);
        writer.writeNullableBytes(nullptr);
        return;
    }
    username.assign(reinterpret_cast<const char*>(token.data()), separators[0]);
    (void)username;
    const std::string identity(reinterpret_cast<const char*>(token.data() + separators[0] + 1),
                               separators[1] - separators[0] - 1);
    username = identity;
    const std::string password(reinterpret_cast<const char*>(token.data() + separators[1] + 1),
                               token.size() - separators[1] - 1);
    if (!auth.authenticate(identity, password)) {
        const std::string message = "authentication failed";
        authenticationFailed = true;
        writer.writeInt16(Error::SASL_AUTHENTICATION_FAILED);
        writer.writeNullableString(&message);
        writer.writeNullableBytes(nullptr);
        return;
    }
    authenticated = true;
    writer.writeInt16(Error::NONE);
    writer.writeNullableString(nullptr);
    writer.writeNullableBytes(nullptr);
}

void writeUnsupported(Writer& writer, const RequestHeader& header) {
    switch (header.apiKey) {
    case METADATA:
        writer.writeArrayLength(0);
        writer.writeArrayLength(0);
        writer.writeInt32(-1);
        if (header.apiVersion >= 1) writer.writeNullableString(nullptr);
        break;
    case FETCH:
        if (header.apiVersion >= 1) writer.writeInt32(0);
        writer.writeArrayLength(0);
        break;
    case PRODUCE:
        if (header.apiVersion >= 1) writer.writeInt32(0);
        writer.writeArrayLength(0);
        break;
    case LIST_OFFSETS:
    case OFFSET_COMMIT:
    case OFFSET_FETCH:
    case CREATE_TOPICS:
    case DELETE_TOPICS:
        writer.writeArrayLength(0);
        break;
    default:
        writer.writeInt16(Error::UNSUPPORTED_VERSION);
        break;
    }
}

} // namespace

Connection::Connection(int socket, Broker* broker, std::shared_ptr<MQTT::TLSContext> TLS,
                       int writeTimeoutMs, size_t maxRequestBytes, MQTT::Auth* auth,
                       bool authenticationRequired)
    : descriptor(socket), broker(broker), TLS(std::move(TLS)),
      transport(std::make_unique<MQTT::SecureTransport>(socket, TLS, writeTimeoutMs)),
      writeTimeoutMs(writeTimeoutMs), maxRequestBytes(maxRequestBytes), auth(auth),
      authenticationRequired(authenticationRequired) {}

Connection::~Connection() {
    close();
}

void Connection::shutdown() {
    std::lock_guard<std::mutex> lock(closeMutex);
    if (descriptor >= 0) ::shutdown(descriptor, SHUT_RDWR);
}

void Connection::close() {
    std::lock_guard<std::mutex> lock(closeMutex);
    if (closed.exchange(true)) return;
    if (descriptor >= 0) ::shutdown(descriptor, SHUT_RDWR);
    if (transport) transport->teardown();
    if (descriptor >= 0) {
        ::close(descriptor);
        descriptor = -1;
    }
}

bool Connection::readExact(uint8_t* data, size_t size) {
    size_t offset = 0;
    while (offset < size && !closed.load()) {
        const ssize_t count = transport->readSome(data + offset, size - offset);
        if (count > 0) {
            offset += static_cast<size_t>(count);
        } else if (count == 0 || (count < 0 && errno != EINTR)) {
            return false;
        }
    }
    return offset == size;
}

bool Connection::writeAll(const uint8_t* data, size_t size) {
    return !closed.load() && transport->writeAll(data, size);
}

void Connection::run() {
    std::string error;
    if (!transport->handshake(&error)) return;
    while (!closed.load()) {
        uint8_t lengthBytes[4];
        if (!readExact(lengthBytes, sizeof(lengthBytes))) return;
        Reader lengthReader(std::vector<uint8_t>(lengthBytes, lengthBytes + 4));
        const int32_t length = lengthReader.readInt32();
        if (length < 8 || static_cast<size_t>(length) > maxRequestBytes) {
            throw ProtocolError("invalid Kafka request size");
        }
        std::vector<uint8_t> body(static_cast<size_t>(length));
        if (!readExact(body.data(), body.size())) return;
        Reader reader(std::move(body));
        RequestHeader header;
        header.apiKey = reader.readInt16();
        header.apiVersion = reader.readInt16();
        header.correlationID = reader.readInt32();
        const bool flexible = header.apiKey == ALTER_PARTITIONS ||
                              (header.apiKey == API_VERSIONS && header.apiVersion >= 3);
        header.clientID = flexible ? reader.readCompactNullableString() : reader.readNullableString();
        if (flexible) skipTags(reader, "request header");
        if (header.clientID.size() > 256) throw ProtocolError("Kafka client id is too long");
        if (!supported(header) && header.apiKey != API_VERSIONS) {
            throw ProtocolError("unsupported Kafka API " + std::to_string(header.apiKey) +
                                " version " + std::to_string(header.apiVersion));
        }
        std::vector<uint8_t> response;
        try {
            response = dispatch(header, reader);
            if (reader.remaining() != 0) throw ProtocolError("trailing request data");
        } catch (const std::exception& error) {
            throw ProtocolError("Kafka API " + std::to_string(header.apiKey) +
                                " version " + std::to_string(header.apiVersion) + ": " + error.what());
        }
        if (suppressResponse) {
            suppressResponse = false;
            continue;
        }
        if (response.size() > std::numeric_limits<int32_t>::max()) {
            throw ProtocolError("Kafka response is too large");
        }
        Writer prefix;
        prefix.writeInt32(static_cast<int32_t>(response.size()));
        const std::vector<uint8_t> prefixBytes = prefix.finish();
        if (!writeAll(prefixBytes.data(), prefixBytes.size()) ||
            !writeAll(response.data(), response.size())) {
            return;
        }
        if (authenticationFailed) return;
    }
}

std::vector<uint8_t> Connection::dispatch(const RequestHeader& header, Reader& reader) {
    suppressResponse = false;
    authenticationFailed = false;
    if (authenticationRequired && !authenticated && header.apiKey != API_VERSIONS &&
        header.apiKey != SASL_HANDSHAKE && header.apiKey != SASL_AUTHENTICATE) {
        throw ProtocolError("Kafka authentication required");
    }
    Writer writer;
    writer.writeInt32(header.correlationID);
    const bool flexible = header.apiKey == ALTER_PARTITIONS ||
                          (header.apiKey == API_VERSIONS && header.apiVersion >= 3);
    if (flexible) writer.writeUnsignedVarint(0);
    switch (header.apiKey) {
    case API_VERSIONS:
        if (supported(header)) {
            writeApiVersions(writer, header.apiVersion);
        } else {
            if (header.apiVersion >= 3) {
                (void)reader.readCompactNullableString();
                (void)reader.readCompactNullableString();
                skipTags(reader, "ApiVersions body");
            }
            // Kafka ApiVersions v3+ uses response header v1 but an unsupported
            // version is answered with the v0 body, including no throttle time.
            writeApiVersions(writer, 0, Error::UNSUPPORTED_VERSION);
        }
        break;
    case METADATA:
        writeMetadata(writer, *broker, reader, header.apiVersion, user);
        break;
    case PRODUCE:
        handleProduce(writer, *broker, reader, header.apiVersion, user, suppressResponse);
        break;
    case FETCH:
        handleFetch(writer, *broker, reader, header.apiVersion, user);
        break;
    case LIST_OFFSETS:
        handleListOffsets(writer, *broker, reader, header.apiVersion, user);
        break;
    case OFFSET_COMMIT:
        handleOffsetCommit(writer, *broker, reader, header.apiVersion, user);
        break;
    case OFFSET_FETCH:
        handleOffsetFetch(writer, *broker, reader, header.apiVersion, user);
        break;
    case FIND_COORDINATOR:
        handleFindCoordinator(writer, *broker, reader, header.apiVersion);
        break;
    case JOIN_GROUP:
        handleJoinGroup(writer, *broker, reader, header.apiVersion, descriptor, header.clientID, user);
        break;
    case HEARTBEAT:
        handleHeartbeat(writer, *broker, reader, header.apiVersion, user);
        break;
    case LEAVE_GROUP:
        handleLeaveGroup(writer, *broker, reader, header.apiVersion, user);
        break;
    case SYNC_GROUP:
        handleSyncGroup(writer, *broker, reader, header.apiVersion, user);
        break;
    case DESCRIBE_GROUPS:
        handleDescribeGroups(writer, *broker, reader, header.apiVersion, user);
        break;
    case CREATE_TOPICS:
        handleCreateTopics(writer, *broker, reader, user);
        break;
    case DELETE_TOPICS:
        handleDeleteTopics(writer, *broker, reader, user);
        break;
    case SASL_HANDSHAKE:
        handleSASLHandshake(writer, reader);
        break;
    case SASL_AUTHENTICATE:
        if (!auth) throw ProtocolError("SASL is not configured");
        handleSASLAuthenticate(writer, reader, *auth, authenticated, user, authenticationFailed);
        break;
    case INIT_PRODUCER_ID:
        handleInitProducerID(writer, *broker, reader, header.clientID, user);
        break;
    case ADD_PARTITIONS_TO_TXN:
        handleAddPartitionsToTxn(writer, *broker, reader, user);
        break;
    case ADD_OFFSETS_TO_TXN:
        handleAddOffsetsToTxn(writer, *broker, reader, user);
        break;
    case TXN_OFFSET_COMMIT:
        handleTxnOffsetCommit(writer, *broker, reader, header.apiVersion, user);
        break;
    case END_TXN:
        handleEndTxn(writer, *broker, reader, user);
        break;
    case WRITE_TXN_MARKERS:
        handleWriteTxnMarkers(writer, *broker, reader, user);
        break;
    case ELECT_LEADERS:
        handleElectLeaders(writer, *broker, reader, header.apiVersion, user);
        break;
    case ALTER_PARTITIONS:
        handleAlterPartitions(writer, *broker, reader, user);
        break;
    default:
        writeUnsupported(writer, header);
        break;
    }
    return writer.finish();
}

} // namespace Kafka
