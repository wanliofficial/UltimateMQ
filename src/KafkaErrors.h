#ifndef KAFKA_ERRORS_H
#define KAFKA_ERRORS_H

#include <cstdint>

namespace Kafka {

namespace Error {
constexpr int16_t NONE = 0;
constexpr int16_t OFFSET_OUT_OF_RANGE = 1;
constexpr int16_t CORRUPT_MESSAGE = 2;
constexpr int16_t UNKNOWN_TOPIC_OR_PARTITION = 3;
constexpr int16_t INVALID_FETCH_SIZE = 4;
constexpr int16_t LEADER_NOT_AVAILABLE = 5;
constexpr int16_t NOT_LEADER_FOR_PARTITION = 6;
constexpr int16_t REQUEST_TIMED_OUT = 7;
constexpr int16_t BROKER_NOT_AVAILABLE = 8;
constexpr int16_t REPLICA_NOT_AVAILABLE = 9;
constexpr int16_t MESSAGE_TOO_LARGE = 10;
constexpr int16_t OFFSET_METADATA_TOO_LARGE = 12;
constexpr int16_t COORDINATOR_LOAD_IN_PROGRESS = 14;
constexpr int16_t COORDINATOR_NOT_AVAILABLE = 15;
constexpr int16_t NOT_COORDINATOR = 16;
constexpr int16_t INVALID_TOPIC = 17;
constexpr int16_t RECORD_LIST_TOO_LARGE = 18;
constexpr int16_t NOT_ENOUGH_REPLICAS = 19;
constexpr int16_t NOT_ENOUGH_REPLICAS_AFTER_APPEND = 20;
constexpr int16_t INVALID_REQUIRED_ACKS = 21;
constexpr int16_t ILLEGAL_GENERATION = 22;
constexpr int16_t INCONSISTENT_GROUP_PROTOCOL = 23;
constexpr int16_t INVALID_GROUP_ID = 24;
constexpr int16_t UNKNOWN_MEMBER_ID = 25;
constexpr int16_t INVALID_SESSION_TIMEOUT = 26;
constexpr int16_t REBALANCE_IN_PROGRESS = 27;
constexpr int16_t INVALID_COMMIT_OFFSET_SIZE = 28;
constexpr int16_t TOPIC_AUTHORIZATION_FAILED = 29;
constexpr int16_t GROUP_AUTHORIZATION_FAILED = 30;
constexpr int16_t CLUSTER_AUTHORIZATION_FAILED = 31;
constexpr int16_t INVALID_TIMESTAMP = 32;
constexpr int16_t UNSUPPORTED_SASL_MECHANISM = 33;
constexpr int16_t ILLEGAL_SASL_STATE = 34;
constexpr int16_t UNSUPPORTED_VERSION = 35;
constexpr int16_t TOPIC_ALREADY_EXISTS = 36;
constexpr int16_t INVALID_PARTITIONS = 37;
constexpr int16_t INVALID_REPLICATION_FACTOR = 38;
constexpr int16_t INVALID_REPLICA_ASSIGNMENT = 39;
constexpr int16_t INVALID_CONFIG = 40;
constexpr int16_t NOT_CONTROLLER = 41;
constexpr int16_t INVALID_REQUEST = 42;
constexpr int16_t UNSUPPORTED_FOR_MESSAGE_FORMAT = 43;
constexpr int16_t INVALID_PRECONDITION = 44;
constexpr int16_t UNKNOWN_SERVER_ERROR = -1;
constexpr int16_t OUT_OF_ORDER_SEQUENCE_NUMBER = 45;
constexpr int16_t INVALID_PRODUCER_EPOCH = 47;
constexpr int16_t INVALID_TXN_STATE = 51;
constexpr int16_t INVALID_TXN_TIMEOUT = 52;
constexpr int16_t CONCURRENT_TRANSACTIONS = 53;
constexpr int16_t TRANSACTIONAL_ID_NOT_FOUND = 55;
constexpr int16_t SASL_AUTHENTICATION_FAILED = 58;
constexpr int16_t UNKNOWN_PRODUCER_ID = 59;
constexpr int16_t UNKNOWN_COMPRESSION_TYPE = 49;
constexpr int16_t INVALID_RECORD = 87;
constexpr int16_t INVALID_TIMESTAMP_RECORD = 89;
constexpr int16_t TRANSACTIONAL_ID_AUTHORIZATION_FAILED = 90;
} // namespace Error

} // namespace Kafka

#endif
