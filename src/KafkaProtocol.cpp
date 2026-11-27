#include "KafkaProtocol.h"
#include "KafkaErrors.h"
#include "KafkaWire.h"
#include <algorithm>
#include <limits>

namespace Kafka {
namespace {

ProducedRecord decodeOldMessage(Reader& reader, size_t maxMessageBytes) {
    const int64_t messageOffset = reader.readInt64();
    const int32_t messageSize = reader.readInt32();
    if (messageOffset < 0 || messageSize < 14 ||
        static_cast<size_t>(messageSize) > maxMessageBytes) {
        throw BrokerError(Error::CORRUPT_MESSAGE, "invalid legacy message size");
    }
    std::vector<uint8_t> message = reader.readRaw(static_cast<size_t>(messageSize));
    const uint32_t expectedCRC = (static_cast<uint32_t>(message[0]) << 24) |
                                (static_cast<uint32_t>(message[1]) << 16) |
                                (static_cast<uint32_t>(message[2]) << 8) | message[3];
    std::vector<uint8_t> crcInput(message.begin() + 4, message.end());
    if (crc32(crcInput) != expectedCRC) throw BrokerError(Error::CORRUPT_MESSAGE, "message CRC mismatch");
    Reader body(message);
    (void)body.readRaw(4);
    const uint8_t magic = body.readInt8();
    const uint8_t attributes = body.readInt8();
    if (magic > 1 || (attributes & 0x07) != 0) {
        throw BrokerError(Error::UNKNOWN_COMPRESSION_TYPE, "compressed legacy messages are unsupported");
    }
    ProducedRecord record;
    if (magic == 1) record.timestamp = body.readInt64();
    const int32_t keyLength = body.readInt32();
    if (keyLength < -1) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid key length");
    if (keyLength >= 0) record.key = body.readRaw(static_cast<size_t>(keyLength));
    const int32_t valueLength = body.readInt32();
    if (valueLength < -1) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid value length");
    if (valueLength >= 0) record.value = body.readRaw(static_cast<size_t>(valueLength));
    if (body.remaining() != 0) throw BrokerError(Error::CORRUPT_MESSAGE, "trailing legacy message data");
    return record;
}

ProducedRecord decodeBatchRecord(Reader& reader, int64_t baseTimestamp,
                                 int32_t& offsetDelta, int64_t producerID,
                                 int16_t producerEpoch, int32_t baseSequence) {
    const int32_t length = reader.readVarint();
    if (length < 0) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid record length");
    Reader body(reader.readRaw(static_cast<size_t>(length)));
    (void)body.readInt8();
    const int64_t timestampDelta = body.readVarint();
    offsetDelta = body.readVarint();
    if (offsetDelta < 0) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid record offset delta");
    ProducedRecord record;
    record.timestamp = baseTimestamp + timestampDelta;
    record.producerID = producerID;
    record.producerEpoch = producerEpoch;
    record.sequence = baseSequence + offsetDelta;
    const int32_t keyLength = body.readVarint();
    if (keyLength < -1) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid record key length");
    if (keyLength >= 0) record.key = body.readRaw(static_cast<size_t>(keyLength));
    const int32_t valueLength = body.readVarint();
    if (valueLength < -1) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid record value length");
    if (valueLength >= 0) record.value = body.readRaw(static_cast<size_t>(valueLength));
    const int32_t headerCount = body.readVarint();
    if (headerCount < 0) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid record header count");
    for (int32_t i = 0; i < headerCount; ++i) {
        RecordHeader header;
        const int32_t headerKeyLength = body.readVarint();
        if (headerKeyLength < 0) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid header key length");
        const std::vector<uint8_t> key = body.readRaw(static_cast<size_t>(headerKeyLength));
        header.key.assign(key.begin(), key.end());
        const int32_t headerValueLength = body.readVarint();
        if (headerValueLength < 0) throw BrokerError(Error::CORRUPT_MESSAGE, "invalid header value length");
        header.value = body.readRaw(static_cast<size_t>(headerValueLength));
        record.headers.push_back(std::move(header));
    }
    if (body.remaining() != 0) throw BrokerError(Error::CORRUPT_MESSAGE, "trailing record data");
    return record;
}

std::vector<ProducedRecord> decodeBatch(Reader& reader, size_t maxMessageBytes,
                                        bool allowTransactions) {
    const int64_t baseOffset = reader.readInt64();
    const int32_t batchLength = reader.readInt32();
    // Kafka RecordBatch: batchLength includes the 5-byte leader-epoch/magic prefix.
    constexpr int32_t MINIMUM_BATCH_LENGTH = 49;
    if (baseOffset < 0 || batchLength < MINIMUM_BATCH_LENGTH ||
        static_cast<size_t>(batchLength) > maxMessageBytes ||
        static_cast<size_t>(batchLength) + 12 > maxMessageBytes) {
        throw BrokerError(Error::CORRUPT_MESSAGE, "invalid record batch length");
    }
    std::vector<uint8_t> batch = reader.readRaw(static_cast<size_t>(batchLength));
    Reader body(batch);
    (void)body.readInt32();
    if (body.readInt8() != 2) throw BrokerError(Error::CORRUPT_MESSAGE, "unsupported record batch magic");
    const uint32_t expectedCRC = static_cast<uint32_t>(body.readInt32());
    std::vector<uint8_t> crcInput(batch.begin() + 9, batch.end());
    if (crc32c(crcInput) != expectedCRC) throw BrokerError(Error::CORRUPT_MESSAGE, "record batch CRC mismatch");
    const int16_t attributes = body.readInt16();
    if ((attributes & 0x07) != 0) {
        throw BrokerError(Error::UNKNOWN_COMPRESSION_TYPE, "compressed batches are unsupported");
    }
    if ((attributes & 0x10) != 0 && !allowTransactions) {
        throw BrokerError(Error::UNSUPPORTED_FOR_MESSAGE_FORMAT,
                          "transactional batches require a transactional Produce request");
    }
    if ((attributes & 0x20) != 0) {
        throw BrokerError(Error::UNSUPPORTED_FOR_MESSAGE_FORMAT,
                          "control batches are not accepted on this API");
    }
    const int32_t lastOffsetDelta = body.readInt32();
    const int64_t firstTimestamp = body.readInt64();
    (void)body.readInt64();
    const int64_t producerID = body.readInt64();
    const int16_t producerEpoch = body.readInt16();
    const int32_t baseSequence = body.readInt32();
    const int32_t recordCount = body.readInt32();
    if (recordCount < 0 || static_cast<size_t>(recordCount) > body.remaining() + 1) {
        throw BrokerError(Error::CORRUPT_MESSAGE, "invalid record count");
    }
    std::vector<ProducedRecord> records;
    int64_t expectedDelta = 0;
    for (int32_t i = 0; i < recordCount; ++i) {
        int32_t offsetDelta = 0;
        ProducedRecord record = decodeBatchRecord(body, firstTimestamp, offsetDelta,
                                                  producerID, producerEpoch, baseSequence);
        if (offsetDelta != expectedDelta) {
            throw BrokerError(Error::CORRUPT_MESSAGE, "record batch offset metadata mismatch");
        }
        records.push_back(std::move(record));
        ++expectedDelta;
    }
    if (body.remaining() != 0 || lastOffsetDelta != recordCount - 1) {
        throw BrokerError(Error::CORRUPT_MESSAGE, "record batch offset metadata mismatch");
    }
    return records;
}

std::vector<uint8_t> encodeLegacyMessage(const Record& record, int16_t fetchVersion) {
    Writer payload;
    payload.writeInt8(fetchVersion == 0 ? 0 : 1);
    payload.writeInt8(0);
    if (fetchVersion > 0) payload.writeInt64(record.timestamp);
    payload.writeNullableBytes(record.key ? &*record.key : nullptr);
    payload.writeNullableBytes(record.value ? &*record.value : nullptr);
    std::vector<uint8_t> bytes = payload.finish();
    const uint32_t checksum = crc32(bytes);
    Writer message;
    message.writeInt64(record.offset);
    message.writeInt32(static_cast<int32_t>(bytes.size() + 4));
    message.writeInt32(static_cast<int32_t>(checksum));
    message.writeRaw(bytes);
    return message.finish();
}

void writeBatchRecord(Writer& writer, const Record& record, int64_t timestampDelta,
                      int32_t offsetDelta) {
    Writer payload;
    payload.writeInt8(0);
    payload.writeVarint(static_cast<int32_t>(std::max<int64_t>(timestampDelta,
                                                               std::numeric_limits<int32_t>::min())));
    payload.writeVarint(offsetDelta);
    if (record.control) {
        // Kafka control-record key v0: version:int16, type:int16. Coordinator epoch is v1+.
        Writer key;
        key.writeInt16(0);
        key.writeInt16(record.controlCommit ? 1 : 0);
        const std::vector<uint8_t> keyBytes = key.finish();
        payload.writeVarint(static_cast<int32_t>(keyBytes.size()));
        payload.writeRaw(keyBytes);
        payload.writeVarint(-1);
        payload.writeVarint(0);
    } else {
        if (record.key) {
            payload.writeVarint(static_cast<int32_t>(record.key->size()));
            payload.writeRaw(*record.key);
        } else {
            payload.writeVarint(-1);
        }
        if (record.value) {
            payload.writeVarint(static_cast<int32_t>(record.value->size()));
            payload.writeRaw(*record.value);
        } else {
            payload.writeVarint(-1);
        }
        payload.writeVarint(static_cast<int32_t>(record.headers.size()));
        for (const RecordHeader& header : record.headers) {
            payload.writeVarint(static_cast<int32_t>(header.key.size()));
            payload.writeRaw(reinterpret_cast<const uint8_t*>(header.key.data()), header.key.size());
            payload.writeVarint(static_cast<int32_t>(header.value.size()));
            payload.writeRaw(header.value);
        }
    }
    std::vector<uint8_t> bytes = payload.finish();
    writer.writeVarint(static_cast<int32_t>(bytes.size()));
    writer.writeRaw(bytes);
}

std::vector<uint8_t> encodeBatch(const std::vector<Record>& records) {
    if (records.empty()) return {};
    const int64_t firstTimestamp = records.front().timestamp;
    int64_t maxTimestamp = firstTimestamp;
    Writer recordsWriter;
    for (const Record& record : records) {
        maxTimestamp = std::max(maxTimestamp, record.timestamp);
        writeBatchRecord(recordsWriter, record, record.timestamp - firstTimestamp,
                         static_cast<int32_t>(record.offset - records.front().offset));
    }
    Writer body;
    body.writeInt16((records.front().transactional ? 0x10 : 0) | (records.front().control ? 0x20 : 0));
    body.writeInt32(static_cast<int32_t>(records.back().offset - records.front().offset));
    body.writeInt64(firstTimestamp);
    body.writeInt64(maxTimestamp);
    body.writeInt64(records.front().producerID);
    body.writeInt16(records.front().producerEpoch);
    body.writeInt32(records.front().sequence);
    body.writeInt32(static_cast<int32_t>(records.size()));
    body.writeRaw(recordsWriter.finish());
    std::vector<uint8_t> bodyBytes = body.finish();
    Writer batch;
    batch.writeInt64(records.front().offset);
    batch.writeInt32(static_cast<int32_t>(bodyBytes.size() + 9));
    batch.writeInt32(0);
    batch.writeInt8(2);
    batch.writeInt32(static_cast<int32_t>(crc32c(bodyBytes)));
    batch.writeRaw(bodyBytes);
    return batch.finish();
}

} // namespace

std::vector<ProducedRecord> decodeProduceRecords(const std::vector<uint8_t>& messageSet,
                                                  int16_t produceVersion,
                                                  size_t maxMessageBytes,
                                                  bool allowTransactions) {
    std::vector<ProducedRecord> records;
    Reader reader(messageSet);
    while (reader.remaining() != 0) {
        if (reader.remaining() < 12) {
            throw BrokerError(Error::CORRUPT_MESSAGE, "truncated message set");
        }
        std::vector<ProducedRecord> decoded;
        if (produceVersion >= 2) {
            decoded = decodeBatch(reader, maxMessageBytes, allowTransactions);
        } else {
            decoded.push_back(decodeOldMessage(reader, maxMessageBytes));
        }
        records.insert(records.end(), std::make_move_iterator(decoded.begin()),
                       std::make_move_iterator(decoded.end()));
    }
    return records;
}

std::vector<uint8_t> encodeMessageSet(const std::vector<Record>& records, int16_t fetchVersion) {
    if (fetchVersion >= 2) {
        std::vector<uint8_t> output;
        size_t start = 0;
        while (start < records.size()) {
            size_t end = start + 1;
            while (end < records.size() && !records[start].control &&
                   records[end].producerID == records[start].producerID &&
                   records[end].producerEpoch == records[start].producerEpoch &&
                   records[end].transactional == records[start].transactional &&
                   records[end].control == records[start].control) ++end;
            std::vector<Record> batch(records.begin() + static_cast<std::ptrdiff_t>(start),
                                      records.begin() + static_cast<std::ptrdiff_t>(end));
            const std::vector<uint8_t> encoded = encodeBatch(batch);
            output.insert(output.end(), encoded.begin(), encoded.end());
            start = end;
        }
        return output;
    }
    Writer writer;
    for (const Record& record : records) {
        if (!record.control) writer.writeRaw(encodeLegacyMessage(record, fetchVersion));
    }
    return writer.finish();
}

} // namespace Kafka
