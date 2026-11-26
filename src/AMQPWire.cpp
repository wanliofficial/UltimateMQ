#include "AMQPWire.h"
#include <cstring>
#include <limits>

namespace AMQP {
namespace {

void append16(std::vector<uint8_t>& out, uint16_t value) {
    out.push_back(static_cast<uint8_t>(value >> 8));
    out.push_back(static_cast<uint8_t>(value));
}

void append32(std::vector<uint8_t>& out, uint32_t value) {
    out.push_back(static_cast<uint8_t>(value >> 24));
    out.push_back(static_cast<uint8_t>(value >> 16));
    out.push_back(static_cast<uint8_t>(value >> 8));
    out.push_back(static_cast<uint8_t>(value));
}

void append64(std::vector<uint8_t>& out, uint64_t value) {
    for (int shift = 56; shift >= 0; shift -= 8) {
        out.push_back(static_cast<uint8_t>(value >> shift));
    }
}

} // namespace

void Reader::require(size_t count) const {
    if (count > remaining()) {
        throw WireError("truncated AMQP field");
    }
}

Reader Reader::subReader(uint32_t length) {
    require(length);
    Reader result(data_ + offset_, length);
    offset_ += length;
    return result;
}

uint8_t Reader::u8() {
    require(1);
    return data_[offset_++];
}

uint16_t Reader::u16() {
    require(2);
    uint16_t value = static_cast<uint16_t>((data_[offset_] << 8) | data_[offset_ + 1]);
    offset_ += 2;
    return value;
}

uint32_t Reader::u32() {
    require(4);
    uint32_t value = (static_cast<uint32_t>(data_[offset_]) << 24) |
                     (static_cast<uint32_t>(data_[offset_ + 1]) << 16) |
                     (static_cast<uint32_t>(data_[offset_ + 2]) << 8) |
                     static_cast<uint32_t>(data_[offset_ + 3]);
    offset_ += 4;
    return value;
}

uint64_t Reader::u64() {
    require(8);
    uint64_t value = 0;
    for (unsigned i = 0; i < 8; ++i) {
        value = (value << 8) | data_[offset_++];
    }
    return value;
}

uint8_t Reader::bits() {
    return u8();
}

std::string Reader::shortString() {
    uint8_t length = u8();
    require(length);
    std::string value(reinterpret_cast<const char*>(data_ + offset_), length);
    offset_ += length;
    return value;
}

std::string Reader::longString() {
    uint32_t length = u32();
    require(length);
    std::string value(reinterpret_cast<const char*>(data_ + offset_), length);
    offset_ += length;
    return value;
}

FieldValue Reader::fieldValue(unsigned depth) {
    // AMQP-4.2.5: a recursively nested value is valid only while it fits the frame.
    // The depth cap prevents a hostile table from exhausting the native stack.
    if (depth > 32) {
        throw WireError("AMQP field table nesting is too deep");
    }
    char type = static_cast<char>(u8());
    switch (type) {
    case 't': return u8() != 0;
    case 'b': return static_cast<int8_t>(u8());
    case 'B': return u8();
    case 'U': return static_cast<int16_t>(u16());
    case 'u': return u16();
    case 'I': return static_cast<int32_t>(u32());
    case 'i': return u32();
    case 'L': return static_cast<int64_t>(u64());
    case 'l': return u64();
    case 'f': {
        uint32_t raw = u32();
        float value;
        static_assert(sizeof(value) == sizeof(raw), "unexpected float size");
        std::memcpy(&value, &raw, sizeof(value));
        return value;
    }
    case 'd': {
        uint64_t raw = u64();
        double value;
        static_assert(sizeof(value) == sizeof(raw), "unexpected double size");
        std::memcpy(&value, &raw, sizeof(value));
        return value;
    }
    case 'D': {
        Decimal decimal;
        decimal.scale = u8();
        decimal.value = u32();
        return decimal;
    }
    case 's': return shortString();
    case 'S': {
        uint32_t length = u32();
        require(length);
        std::vector<uint8_t> value(data_ + offset_, data_ + offset_ + length);
        offset_ += length;
        return value;
    }
    case 'A': {
        uint32_t length = u32();
        Reader values = subReader(length);
        auto array = std::make_shared<FieldArray>();
        while (!values.done()) {
            array->push_back(values.fieldValue(depth + 1));
        }
        return array;
    }
    case 'T': return u64();
    case 'F': {
        uint32_t length = u32();
        Reader values = subReader(length);
        auto table = std::make_shared<FieldTable>();
        while (!values.done()) {
            std::string name = values.shortString();
            table->insert_or_assign(std::move(name), values.fieldValue(depth + 1));
        }
        return table;
    }
    case 'V': return std::monostate{};
    default: throw WireError("unknown AMQP field type");
    }
}

std::shared_ptr<FieldTable> Reader::table() {
    uint32_t length = u32();
    Reader values = subReader(length);
    auto result = std::make_shared<FieldTable>();
    while (!values.done()) {
        std::string name = values.shortString();
        result->insert_or_assign(std::move(name), values.fieldValue());
    }
    return result;
}

void Reader::requireEnd() {
    if (!done()) {
        throw WireError("trailing bytes in AMQP method");
    }
}

Writer& Writer::u8(uint8_t value) {
    data_.push_back(value);
    return *this;
}

Writer& Writer::u16(uint16_t value) {
    append16(data_, value);
    return *this;
}

Writer& Writer::u32(uint32_t value) {
    append32(data_, value);
    return *this;
}

Writer& Writer::u64(uint64_t value) {
    append64(data_, value);
    return *this;
}

Writer& Writer::bits(uint8_t value) {
    data_.push_back(value);
    return *this;
}

Writer& Writer::shortString(const std::string& value) {
    if (value.size() > 255) {
        throw WireError("AMQP short string exceeds 255 octets");
    }
    data_.push_back(static_cast<uint8_t>(value.size()));
    data_.insert(data_.end(), value.begin(), value.end());
    return *this;
}

Writer& Writer::longString(const std::string& value) {
    if (value.size() > std::numeric_limits<uint32_t>::max()) {
        throw WireError("AMQP long string is too large");
    }
    u32(static_cast<uint32_t>(value.size()));
    data_.insert(data_.end(), value.begin(), value.end());
    return *this;
}

Writer& Writer::bytes(const std::vector<uint8_t>& value) {
    data_.insert(data_.end(), value.begin(), value.end());
    return *this;
}

Writer& Writer::fieldValue(const FieldValue& value) {
    if (std::holds_alternative<std::monostate>(value)) {
        u8('V');
    } else if (auto v = std::get_if<bool>(&value)) {
        u8('t'); u8(*v ? 1 : 0);
    } else if (auto v = std::get_if<int8_t>(&value)) {
        u8('b'); u8(static_cast<uint8_t>(*v));
    } else if (auto v = std::get_if<uint8_t>(&value)) {
        u8('B'); u8(*v);
    } else if (auto v = std::get_if<int16_t>(&value)) {
        u8('U'); u16(static_cast<uint16_t>(*v));
    } else if (auto v = std::get_if<uint16_t>(&value)) {
        u8('u'); u16(*v);
    } else if (auto v = std::get_if<int32_t>(&value)) {
        u8('I'); u32(static_cast<uint32_t>(*v));
    } else if (auto v = std::get_if<uint32_t>(&value)) {
        u8('i'); u32(*v);
    } else if (auto v = std::get_if<int64_t>(&value)) {
        u8('L'); u64(static_cast<uint64_t>(*v));
    } else if (auto v = std::get_if<uint64_t>(&value)) {
        u8('l'); u64(*v);
    } else if (auto v = std::get_if<float>(&value)) {
        u8('f');
        uint32_t raw;
        std::memcpy(&raw, v, sizeof(raw));
        u32(raw);
    } else if (auto v = std::get_if<double>(&value)) {
        u8('d');
        uint64_t raw;
        std::memcpy(&raw, v, sizeof(raw));
        u64(raw);
    } else if (auto v = std::get_if<Decimal>(&value)) {
        u8('D'); u8(v->scale); u32(v->value);
    } else if (auto v = std::get_if<std::string>(&value)) {
        u8('s'); shortString(*v);
    } else if (auto v = std::get_if<std::vector<uint8_t>>(&value)) {
        u8('S'); u32(static_cast<uint32_t>(v->size()));
        data_.insert(data_.end(), v->begin(), v->end());
    } else if (auto v = std::get_if<std::shared_ptr<FieldArray>>(&value)) {
        u8('A');
        Writer nested;
        if (*v) {
            for (const auto& item : **v) {
                nested.fieldValue(item);
            }
        }
        u32(static_cast<uint32_t>(nested.data().size()));
        bytes(nested.data());
    } else if (auto v = std::get_if<std::shared_ptr<FieldTable>>(&value)) {
        u8('F');
        Writer nested;
        if (*v) {
            for (const auto& item : **v) {
                nested.shortString(item.first);
                nested.fieldValue(item.second);
            }
        }
        u32(static_cast<uint32_t>(nested.data().size()));
        bytes(nested.data());
    }
    return *this;
}

Writer& Writer::table(const std::shared_ptr<FieldTable>& value) {
    Writer nested;
    if (value) {
        for (const auto& item : *value) {
            nested.shortString(item.first);
            nested.fieldValue(item.second);
        }
    }
    u32(static_cast<uint32_t>(nested.data().size()));
    bytes(nested.data());
    return *this;
}

std::vector<uint8_t> encodeFrame(uint8_t type, uint16_t channel,
                                 const std::vector<uint8_t>& payload,
                                 uint32_t frameMax) {
    // connection.tune-ok.frame-max: the limit includes the seven-byte header
    // and frame-end octet (Errata 11).
    if (payload.size() > frameMax || frameMax < 8 ||
        payload.size() > frameMax - 8) {
        throw WireError("AMQP frame exceeds negotiated frame-max");
    }
    std::vector<uint8_t> result;
    result.reserve(payload.size() + 8);
    result.push_back(type);
    append16(result, channel);
    append32(result, static_cast<uint32_t>(payload.size()));
    result.insert(result.end(), payload.begin(), payload.end());
    result.push_back(FRAME_END);
    return result;
}

std::vector<uint8_t> encodeMethod(uint16_t classID, uint16_t methodID,
                                  const std::vector<uint8_t>& arguments,
                                  uint16_t channel, uint32_t frameMax) {
    std::vector<uint8_t> payload;
    payload.reserve(arguments.size() + 4);
    append16(payload, classID);
    append16(payload, methodID);
    payload.insert(payload.end(), arguments.begin(), arguments.end());
    return encodeFrame(FRAME_METHOD, channel, payload, frameMax);
}

std::vector<uint8_t> encodeContentHeader(uint16_t channel, uint16_t classID,
                                         uint16_t methodID, uint64_t bodySize,
                                         const std::vector<uint8_t>& properties,
                                         uint32_t frameMax) {
    Writer payload;
    payload.u16(classID);
    payload.u16(0);
    (void)methodID;
    payload.u64(bodySize);
    payload.bytes(properties);
    return encodeFrame(FRAME_HEADER, channel, payload.data(), frameMax);
}

std::vector<uint8_t> encodeBody(uint16_t channel, const uint8_t* data,
                                size_t size, uint32_t frameMax) {
    return encodeFrame(FRAME_BODY, channel,
                       std::vector<uint8_t>(data, data + size), frameMax);
}

Method decodeMethod(const Frame& frame) {
    if (frame.type != FRAME_METHOD) {
        throw WireError("expected an AMQP method frame");
    }
    Reader reader(frame.payload);
    Method method;
    method.classID = reader.u16();
    method.methodID = reader.u16();
    method.arguments.assign(frame.payload.begin() + 4, frame.payload.end());
    return method;
}

} // namespace AMQP
