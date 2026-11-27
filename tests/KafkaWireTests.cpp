#include <gtest/gtest.h>
#include "../src/KafkaWire.h"
#include <limits>

namespace Kafka {

TEST(KafkaWireTest, RoundTripsPrimitiveValues) {
    Writer writer;
    writer.writeInt8(0xFE);
    writer.writeInt16(-12345);
    writer.writeInt32(std::numeric_limits<int32_t>::min());
    writer.writeInt64(-1234567890123LL);
    writer.writeBoolean(true);
    writer.writeString("kafka");
    const std::vector<uint8_t> bytes = {0, 1, 0xFF};
    writer.writeBytes(bytes);
    writer.writeNullableString(nullptr);
    writer.writeNullableBytes(nullptr);

    Reader reader(writer.finish());
    EXPECT_EQ(reader.readInt8(), 0xFE);
    EXPECT_EQ(reader.readInt16(), -12345);
    EXPECT_EQ(reader.readInt32(), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(reader.readInt64(), -1234567890123LL);
    EXPECT_TRUE(reader.readBoolean());
    EXPECT_EQ(reader.readString(), "kafka");
    EXPECT_EQ(reader.readBytes(), bytes);
    EXPECT_TRUE(reader.readNullableString().empty());
    EXPECT_TRUE(reader.readNullableBytes().empty());
    EXPECT_EQ(reader.remaining(), 0);
}

TEST(KafkaWireTest, VarintsUseZigZagEncoding) {
    Writer writer;
    writer.writeVarint(-1);
    writer.writeVarint(0);
    writer.writeVarint(1);
    writer.writeVarint(std::numeric_limits<int32_t>::min());
    writer.writeVarint(std::numeric_limits<int32_t>::max());

    const std::vector<uint8_t> bytes = writer.finish();
    EXPECT_EQ(bytes[0], 1);
    EXPECT_EQ(bytes[1], 0);
    EXPECT_EQ(bytes[2], 2);
    Reader reader(bytes);
    EXPECT_EQ(reader.readVarint(), -1);
    EXPECT_EQ(reader.readVarint(), 0);
    EXPECT_EQ(reader.readVarint(), 1);
    EXPECT_EQ(reader.readVarint(), std::numeric_limits<int32_t>::min());
    EXPECT_EQ(reader.readVarint(), std::numeric_limits<int32_t>::max());
}

TEST(KafkaWireTest, CRC32CMatchesKnownVector) {
    const std::vector<uint8_t> bytes = {'1', '2', '3', '4', '5', '6', '7', '8', '9'};
    EXPECT_EQ(crc32c(bytes), 0xE3069283U);
}

TEST(KafkaWireTest, CompactFieldsRoundTrip) {
    Writer writer;
    writer.writeCompactArrayLength(2);
    writer.writeCompactString("topic");
    writer.writeInt32(0);
    writer.writeCompactNullableString(nullptr);
    writer.writeUnsignedVarint(0);
    writer.writeCompactNullableArrayLength(0);

    Reader reader(writer.finish());
    EXPECT_EQ(reader.readCompactArrayLength(), 2);
    EXPECT_EQ(reader.readCompactString(), "topic");
    EXPECT_EQ(reader.readInt32(), 0);
    EXPECT_TRUE(reader.readCompactNullableString().empty());
    EXPECT_EQ(reader.readUnsignedVarint(), 0U);
    EXPECT_EQ(reader.readCompactNullableArrayLength(), 0);
    EXPECT_EQ(reader.remaining(), 0U);

    const std::vector<uint8_t> nullArray = {0};
    Reader nullReader(nullArray);
    EXPECT_EQ(nullReader.readCompactNullableArrayLength(), -1);
    EXPECT_THROW(nullReader.readCompactArrayLength(), ProtocolError);
}

TEST(KafkaWireTest, RejectsTruncatedAndInvalidValues) {
    Reader truncated(std::vector<uint8_t>{0, 1});
    EXPECT_THROW(truncated.readInt32(), ProtocolError);

    const std::vector<uint8_t> nullString = {0xFF, 0xFF};
    Reader invalidString(nullString);
    EXPECT_THROW(invalidString.readString(), ProtocolError);

    Writer writer;
    writer.writeInt32(-1);
    Reader nullArray(writer.finish());
    EXPECT_THROW(nullArray.readArrayLength(), ProtocolError);
    EXPECT_EQ(nullArray.remaining(), 0);
    Writer nullableWriter;
    nullableWriter.writeInt32(-1);
    Reader nullableNull(nullableWriter.finish());
    EXPECT_EQ(nullableNull.readNullableArrayLength(), -1);
}

} // namespace Kafka
