#ifndef KAFKA_PROTOCOL_H
#define KAFKA_PROTOCOL_H

#include "KafkaBroker.h"
#include <cstdint>
#include <vector>

namespace Kafka {

std::vector<ProducedRecord> decodeProduceRecords(const std::vector<uint8_t>& messageSet,
                                                  int16_t produceVersion,
                                                  size_t maxMessageBytes,
                                                  bool allowTransactions = false);
std::vector<uint8_t> encodeMessageSet(const std::vector<Record>& records,
                                      int16_t fetchVersion);

} // namespace Kafka

#endif
