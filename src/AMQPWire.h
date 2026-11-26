#ifndef AMQP_WIRE_H
#define AMQP_WIRE_H

#include <cstddef>
#include <cstdint>
#include <map>
#include <memory>
#include <stdexcept>
#include <string>
#include <variant>
#include <vector>

namespace AMQP {

constexpr uint8_t FRAME_METHOD = 1;
constexpr uint8_t FRAME_HEADER = 2;
constexpr uint8_t FRAME_BODY = 3;
constexpr uint8_t FRAME_HEARTBEAT = 8;
constexpr uint8_t FRAME_END = 0xCE;
constexpr uint16_t CLASS_CONNECTION = 10;
constexpr uint16_t CLASS_CHANNEL = 20;
constexpr uint16_t CLASS_EXCHANGE = 40;
constexpr uint16_t CLASS_QUEUE = 50;
constexpr uint16_t CLASS_BASIC = 60;
constexpr uint16_t CLASS_CONFIRM = 85;
constexpr uint16_t CLASS_TX = 90;
constexpr size_t MIN_FRAME_MAX = 4096;
constexpr uint32_t DEFAULT_FRAME_MAX = 131072;

struct WireError : std::runtime_error {
    explicit WireError(const std::string& message) : std::runtime_error(message) {}
};

struct Decimal {
    uint8_t scale = 0;
    uint32_t value = 0;
    bool operator==(const Decimal& other) const {
        return scale == other.scale && value == other.value;
    }
};

struct FieldArray;
struct FieldTable;

using FieldValue = std::variant<std::monostate, bool, int8_t, uint8_t, int16_t,
                                uint16_t, int32_t, uint32_t, int64_t, uint64_t,
                                float, double, Decimal, std::string,
                                std::vector<uint8_t>, std::shared_ptr<FieldArray>,
                                std::shared_ptr<FieldTable>>;
struct FieldArray : std::vector<FieldValue> {};
struct FieldTable : std::map<std::string, FieldValue> {};

class Reader {
public:
    Reader(const uint8_t* data, size_t size) : data_(data), size_(size) {}
    explicit Reader(const std::vector<uint8_t>& data)
        : Reader(data.data(), data.size()) {}

    size_t remaining() const { return size_ - offset_; }
    bool done() const { return offset_ == size_; }
    uint8_t u8();
    uint16_t u16();
    uint32_t u32();
    uint64_t u64();
    uint8_t bits();
    std::string shortString();
    std::string longString();
    FieldValue fieldValue(unsigned depth = 0);
    std::shared_ptr<FieldTable> table();
    void requireEnd();

private:
    void require(size_t count) const;
    Reader subReader(uint32_t length);

    const uint8_t* data_;
    size_t size_;
    size_t offset_ = 0;
};

class Writer {
public:
    Writer& u8(uint8_t value);
    Writer& u16(uint16_t value);
    Writer& u32(uint32_t value);
    Writer& u64(uint64_t value);
    Writer& bits(uint8_t value);
    Writer& shortString(const std::string& value);
    Writer& longString(const std::string& value);
    Writer& bytes(const std::vector<uint8_t>& value);
    Writer& fieldValue(const FieldValue& value);
    Writer& table(const std::shared_ptr<FieldTable>& value);
    const std::vector<uint8_t>& data() const { return data_; }

private:
    std::vector<uint8_t> data_;
};

struct Frame {
    uint8_t type = 0;
    uint16_t channel = 0;
    std::vector<uint8_t> payload;
};

struct Method {
    uint16_t classID = 0;
    uint16_t methodID = 0;
    std::vector<uint8_t> arguments;
};

std::vector<uint8_t> encodeFrame(uint8_t type, uint16_t channel,
                                 const std::vector<uint8_t>& payload,
                                 uint32_t frameMax);
std::vector<uint8_t> encodeMethod(uint16_t classID, uint16_t methodID,
                                  const std::vector<uint8_t>& arguments,
                                  uint16_t channel, uint32_t frameMax);
std::vector<uint8_t> encodeContentHeader(uint16_t channel, uint16_t classID,
                                         uint16_t methodID, uint64_t bodySize,
                                         const std::vector<uint8_t>& properties,
                                         uint32_t frameMax);
std::vector<uint8_t> encodeBody(uint16_t channel, const uint8_t* data,
                                size_t size, uint32_t frameMax);
Method decodeMethod(const Frame& frame);

} // namespace AMQP

#endif
