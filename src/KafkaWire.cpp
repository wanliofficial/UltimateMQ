#include "KafkaWire.h"
#include <limits>
#include <utility>

namespace Kafka {
namespace {

void appendInt(std::vector<uint8_t>& bytes, uint64_t value, size_t width) {
    for (size_t i = 0; i < width; ++i) {
        bytes.push_back(static_cast<uint8_t>(value >> ((width - i - 1) * 8)));
    }
}

const std::vector<uint32_t>& crcTable() {
    static const std::vector<uint32_t> table = [] {
        std::vector<uint32_t> values(256);
        for (uint32_t i = 0; i < values.size(); ++i) {
            uint32_t value = i;
            for (int bit = 0; bit < 8; ++bit) {
                value = (value & 1) ? (value >> 1) ^ 0x82F63B78U : value >> 1;
            }
            values[i] = value;
        }
        return values;
    }();
    return table;
}

} // namespace

Reader::Reader(const std::vector<uint8_t>& bytes)
    : storage(bytes), data(storage.data()), size(storage.size()) {}

Reader::Reader(std::vector<uint8_t>&& bytes)
    : storage(std::move(bytes)), data(storage.data()), size(storage.size()) {}

void Reader::require(size_t length) const {
    if (length > size - offset) {
        throw ProtocolError("truncated Kafka value at offset " + std::to_string(offset) +
                            " of " + std::to_string(size) + " (need " + std::to_string(length) + ")");
    }
}

uint8_t Reader::readInt8() {
    require(1);
    return data[offset++];
}

int16_t Reader::readInt16() {
    require(2);
    const uint16_t value = static_cast<uint16_t>((data[offset] << 8) | data[offset + 1]);
    offset += 2;
    return static_cast<int16_t>(value);
}

int32_t Reader::readInt32() {
    require(4);
    const uint32_t value = (static_cast<uint32_t>(data[offset]) << 24) |
                           (static_cast<uint32_t>(data[offset + 1]) << 16) |
                           (static_cast<uint32_t>(data[offset + 2]) << 8) |
                           data[offset + 3];
    offset += 4;
    return static_cast<int32_t>(value);
}

int64_t Reader::readInt64() {
    const uint64_t high = static_cast<uint32_t>(readInt32());
    const uint64_t low = static_cast<uint32_t>(readInt32());
    return static_cast<int64_t>((high << 32) | low);
}

uint32_t Reader::readUnsignedVarint() {
    uint32_t value = 0;
    for (unsigned shift = 0; shift < 35; shift += 7) {
        const uint8_t byte = readInt8();
        value |= static_cast<uint32_t>(byte & 0x7F) << shift;
        if ((byte & 0x80) == 0) return value;
    }
    throw ProtocolError("invalid unsigned varint");
}

int32_t Reader::readVarint() {
    const uint32_t value = readUnsignedVarint();
    return static_cast<int32_t>((value >> 1) ^ (0U - (value & 1U)));
}

bool Reader::readBoolean() {
    return readInt8() != 0;
}

std::string Reader::readString() {
    const int32_t length = readInt16();
    if (length < 0) throw ProtocolError("null non-nullable Kafka string");
    const std::vector<uint8_t> bytes = readRaw(static_cast<size_t>(length));
    return std::string(bytes.begin(), bytes.end());
}

std::string Reader::readNullableString() {
    const int32_t length = readInt16();
    if (length < 0) return {};
    const std::vector<uint8_t> bytes = readRaw(static_cast<size_t>(length));
    return std::string(bytes.begin(), bytes.end());
}

std::vector<uint8_t> Reader::readBytes() {
    const int32_t length = readInt32();
    if (length < 0) throw ProtocolError("null non-nullable Kafka bytes");
    return readRaw(static_cast<size_t>(length));
}

std::vector<uint8_t> Reader::readNullableBytes() {
    const int32_t length = readInt32();
    if (length < 0) return {};
    return readRaw(static_cast<size_t>(length));
}

std::vector<uint8_t> Reader::readRaw(size_t length) {
    require(length);
    std::vector<uint8_t> result(data + offset, data + offset + length);
    offset += length;
    return result;
}

std::vector<std::string> Reader::readStringArray() {
    const int32_t count = readArrayLength();
    std::vector<std::string> values;
    values.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) values.push_back(readString());
    return values;
}

std::vector<std::string> Reader::readNullableStringArray() {
    const int32_t count = readNullableArrayLength();
    std::vector<std::string> values;
    values.reserve(static_cast<size_t>(count));
    for (int32_t i = 0; i < count; ++i) values.push_back(readNullableString());
    return values;
}

int32_t Reader::readArrayLength() {
    const int32_t count = readInt32();
    if (count < 0 || static_cast<size_t>(count) > remaining() + 1) {
        throw ProtocolError("invalid Kafka array length " + std::to_string(count) +
                            " at offset " + std::to_string(offset));
    }
    return count;
}

int32_t Reader::readNullableArrayLength() {
    const int32_t count = readInt32();
    if (count < -1 || (count > 0 && static_cast<size_t>(count) > remaining() + 1)) {
        throw ProtocolError("invalid nullable Kafka array length");
    }
    return count;
}

int32_t Reader::readCompactArrayLength() {
    const uint32_t count = readUnsignedVarint();
    if (count == 0 || count > remaining() + 1) throw ProtocolError("invalid compact Kafka array length");
    return static_cast<int32_t>(count - 1);
}

int32_t Reader::readCompactNullableArrayLength() {
    const uint32_t count = readUnsignedVarint();
    if (count > remaining() + 1) throw ProtocolError("invalid compact nullable Kafka array length");
    return count == 0 ? -1 : static_cast<int32_t>(count - 1);
}

std::string Reader::readCompactString() {
    const uint32_t length = readUnsignedVarint();
    if (length == 0) throw ProtocolError("null non-nullable compact Kafka string");
    const std::vector<uint8_t> bytes = readRaw(length - 1);
    return std::string(bytes.begin(), bytes.end());
}

std::string Reader::readCompactNullableString() {
    const uint32_t length = readUnsignedVarint();
    if (length == 0) return {};
    const std::vector<uint8_t> bytes = readRaw(length - 1);
    return std::string(bytes.begin(), bytes.end());
}

void Writer::writeInt8(uint8_t value) {
    bytes.push_back(value);
}

void Writer::writeInt16(int16_t value) {
    appendInt(bytes, static_cast<uint16_t>(value), 2);
}

void Writer::writeInt32(int32_t value) {
    appendInt(bytes, static_cast<uint32_t>(value), 4);
}

void Writer::writeInt64(int64_t value) {
    appendInt(bytes, static_cast<uint64_t>(value), 8);
}

void Writer::writeUnsignedVarint(uint32_t value) {
    while (value >= 0x80) {
        bytes.push_back(static_cast<uint8_t>(value | 0x80));
        value >>= 7;
    }
    bytes.push_back(static_cast<uint8_t>(value));
}

void Writer::writeVarint(int32_t value) {
    const uint32_t encoded = (static_cast<uint32_t>(value) << 1) ^
                             static_cast<uint32_t>(value >> 31);
    writeUnsignedVarint(encoded);
}

void Writer::writeBoolean(bool value) {
    writeInt8(value ? 1 : 0);
}

void Writer::writeString(const std::string& value) {
    if (value.size() > std::numeric_limits<int16_t>::max()) {
        throw ProtocolError("Kafka string is too long");
    }
    writeInt16(static_cast<int16_t>(value.size()));
    writeRaw(reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

void Writer::writeNullableString(const std::string* value) {
    if (!value) {
        writeInt16(-1);
        return;
    }
    writeString(*value);
}

void Writer::writeBytes(const std::vector<uint8_t>& value) {
    if (value.size() > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ProtocolError("Kafka bytes value is too long");
    }
    writeInt32(static_cast<int32_t>(value.size()));
    writeRaw(value);
}

void Writer::writeNullableBytes(const std::vector<uint8_t>* value) {
    if (!value) {
        writeInt32(-1);
        return;
    }
    writeBytes(*value);
}

void Writer::writeRaw(const uint8_t* data, size_t length) {
    bytes.insert(bytes.end(), data, data + length);
}

void Writer::writeRaw(const std::vector<uint8_t>& value) {
    bytes.insert(bytes.end(), value.begin(), value.end());
}

void Writer::writeArrayLength(size_t length) {
    if (length > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ProtocolError("Kafka array is too long");
    }
    writeInt32(static_cast<int32_t>(length));
}

void Writer::writeNullableArrayLength(size_t length) {
    if (length > static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ProtocolError("Kafka array is too long");
    }
    writeInt32(static_cast<int32_t>(length));
}

void Writer::writeCompactArrayLength(size_t length) {
    if (length >= static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ProtocolError("Kafka compact array is too long");
    }
    writeUnsignedVarint(static_cast<uint32_t>(length + 1));
}

void Writer::writeCompactNullableArrayLength(size_t length) {
    if (length >= static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ProtocolError("Kafka compact nullable array is too long");
    }
    writeUnsignedVarint(static_cast<uint32_t>(length + 1));
}

void Writer::writeCompactString(const std::string& value) {
    if (value.size() >= static_cast<size_t>(std::numeric_limits<int32_t>::max())) {
        throw ProtocolError("Kafka compact string is too long");
    }
    writeUnsignedVarint(static_cast<uint32_t>(value.size() + 1));
    writeRaw(reinterpret_cast<const uint8_t*>(value.data()), value.size());
}

void Writer::writeCompactNullableString(const std::string* value) {
    if (!value) {
        writeUnsignedVarint(0);
        return;
    }
    writeCompactString(*value);
}

void Writer::writeStringArray(const std::vector<std::string>& values) {
    writeArrayLength(values.size());
    for (const std::string& value : values) writeString(value);
}

void Writer::writeNullableStringArray(const std::vector<std::string>& values) {
    writeNullableArrayLength(values.size());
    for (const std::string& value : values) writeString(value);
}

uint32_t crc32c(const std::vector<uint8_t>& bytes) {
    uint32_t crc = 0xFFFFFFFFU;
    for (uint8_t byte : bytes) crc = crcTable()[(crc ^ byte) & 0xFF] ^ (crc >> 8);
    return crc ^ 0xFFFFFFFFU;
}

uint32_t crc32(const std::vector<uint8_t>& bytes) {
    uint32_t crc = 0xFFFFFFFFU;
    for (uint8_t byte : bytes) {
        crc ^= byte;
        for (int bit = 0; bit < 8; ++bit) crc = (crc >> 1) ^ (0xEDB88320U & (0U - (crc & 1U)));
    }
    return crc ^ 0xFFFFFFFFU;
}

} // namespace Kafka
