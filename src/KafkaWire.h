#ifndef KAFKA_WIRE_H
#define KAFKA_WIRE_H

#include <cstddef>
#include <cstdint>
#include <stdexcept>
#include <string>
#include <vector>

namespace Kafka {

struct ProtocolError : std::runtime_error {
    explicit ProtocolError(const std::string& message) : std::runtime_error(message) {}
};

class Reader {
public:
    explicit Reader(const std::vector<uint8_t>& bytes);
    explicit Reader(std::vector<uint8_t>&& bytes);
    size_t remaining() const { return size - offset; }
    uint8_t readInt8();
    int16_t readInt16();
    int32_t readInt32();
    int64_t readInt64();
    uint32_t readUnsignedVarint();
    int32_t readVarint();
    bool readBoolean();
    std::string readString();
    std::string readNullableString();
    std::vector<uint8_t> readBytes();
    std::vector<uint8_t> readNullableBytes();
    std::vector<uint8_t> readRaw(size_t length);
    std::vector<std::string> readStringArray();
    std::vector<std::string> readNullableStringArray();
    int32_t readArrayLength();
    int32_t readNullableArrayLength();
    int32_t readCompactArrayLength();
    int32_t readCompactNullableArrayLength();
    std::string readCompactString();
    std::string readCompactNullableString();

private:
    std::vector<uint8_t> storage;
    const uint8_t* data;
    size_t size;
    size_t offset = 0;

    void require(size_t length) const;
};

class Writer {
public:
    std::vector<uint8_t> finish() { return std::move(bytes); }
    void writeInt8(uint8_t value);
    void writeInt16(int16_t value);
    void writeInt32(int32_t value);
    void writeInt64(int64_t value);
    void writeUnsignedVarint(uint32_t value);
    void writeVarint(int32_t value);
    void writeBoolean(bool value);
    void writeString(const std::string& value);
    void writeNullableString(const std::string* value);
    void writeBytes(const std::vector<uint8_t>& value);
    void writeNullableBytes(const std::vector<uint8_t>* value);
    void writeRaw(const uint8_t* data, size_t length);
    void writeRaw(const std::vector<uint8_t>& value);
    void writeArrayLength(size_t length);
    void writeNullableArrayLength(size_t length);
    void writeCompactArrayLength(size_t length);
    void writeCompactNullableArrayLength(size_t length);
    void writeCompactString(const std::string& value);
    void writeCompactNullableString(const std::string* value);
    void writeStringArray(const std::vector<std::string>& values);
    void writeNullableStringArray(const std::vector<std::string>& values);

private:
    std::vector<uint8_t> bytes;
};

uint32_t crc32c(const std::vector<uint8_t>& bytes);
uint32_t crc32(const std::vector<uint8_t>& bytes);

} // namespace Kafka

#endif
