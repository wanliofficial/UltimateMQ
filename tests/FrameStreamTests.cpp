#include <gtest/gtest.h>
#include "../src/Frame.h"
#include <vector>
#include <stdexcept>

namespace MQTT {

namespace {
std::vector<uint8_t> connectPacket(const std::string &clientId, uint8_t version = 5, uint16_t keepAlive = 60) {
    std::vector<uint8_t> pkt;
    pkt.push_back(0x10);
    std::vector<uint8_t> body = {0x00, 0x04, 'M', 'Q', 'T', 'T', version, 0x02,
                                 static_cast<uint8_t>(keepAlive >> 8),
                                 static_cast<uint8_t>(keepAlive & 0xFF)};
    if (version == 5) {
        body.push_back(0x00);
    }
    body.push_back(static_cast<uint8_t>(clientId.size() >> 8));
    body.push_back(static_cast<uint8_t>(clientId.size() & 0xFF));
    body.insert(body.end(), clientId.begin(), clientId.end());
    pkt.push_back(static_cast<uint8_t>(body.size()));
    pkt.insert(pkt.end(), body.begin(), body.end());
    return pkt;
}
}

class FrameStreamTest : public ::testing::Test {
protected:
    Frame frame;
};

TEST_F(FrameStreamTest, ParsesPacketSplitAcrossWrites)
{
    auto pkt = connectPacket("split");
    // Feed one byte at a time
    size_t consumed = 0;
    for (size_t i = 0; i < pkt.size(); ++i) {
        consumed += frame.feed(pkt.data() + i, 1);
        if (i + 1 < pkt.size()) {
            EXPECT_FALSE(frame.isComplete());
        }
    }
    EXPECT_EQ(consumed, pkt.size());
    ASSERT_TRUE(frame.isComplete());
    auto packet = frame.parse();
    EXPECT_EQ(std::static_pointer_cast<ConnectPacket>(packet)->clientId, "split");
    EXPECT_FALSE(frame.isComplete());
}

TEST_F(FrameStreamTest, ParsesTwoPacketsInOneBuffer)
{
    auto first = connectPacket("one");
    std::vector<uint8_t> buffer = first;
    // PINGREQ
    buffer.push_back(0xC0);
    buffer.push_back(0x00);

    size_t used = frame.feed(buffer.data(), buffer.size());
    // The first packet is complete, so feeding stops there
    EXPECT_EQ(used, first.size());
    ASSERT_TRUE(frame.isComplete());
    EXPECT_EQ(frame.parse()->type, PacketType::CONNECT);

    used = frame.feed(buffer.data() + first.size(), buffer.size() - first.size());
    EXPECT_EQ(used, 2u);
    ASSERT_TRUE(frame.isComplete());
    EXPECT_EQ(frame.parse()->type, PacketType::PINGREQ);
}

TEST_F(FrameStreamTest, RejectsRemainingLengthLongerThanFourBytes)
{
    std::vector<uint8_t> buffer = {0xC0, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF};
    EXPECT_THROW(frame.feed(buffer.data(), buffer.size()), std::runtime_error);
}

TEST_F(FrameStreamTest, RejectsNonMinimalRemainingLengthEncoding)
{
    // §1.5.5: the Remaining Length must use the minimum number of bytes. A value
    // of 0 encoded as 0x80 0x00 (two bytes, the second carrying nothing) decodes
    // to 0 but is non-minimal, so it is a Malformed Packet rather than a PINGREQ
    // of length 0.
    std::vector<uint8_t> nonMinimal = {0xC0, 0x80, 0x00};
    EXPECT_THROW(frame.feed(nonMinimal.data(), nonMinimal.size()), std::runtime_error);
}

TEST_F(FrameStreamTest, AcceptsMinimalMultiByteRemainingLength)
{
    // The guard must not over-reject: a genuinely multi-byte Remaining Length in
    // minimal form (0x80 0x01 = 128) is still accepted. A small limit forces the
    // length to be validated once the field is complete; only a body byte would
    // finish the packet, so here we assert the header itself is not rejected.
    frame.setMaxPacketSize(128);
    std::vector<uint8_t> minimal = {0xC0, 0x80, 0x01};
    EXPECT_NO_THROW(frame.feed(minimal.data(), minimal.size()));
    EXPECT_FALSE(frame.isComplete());
}

TEST_F(FrameStreamTest, ResetDropsPartialPacket)
{
    auto pkt = connectPacket("partial");
    frame.feed(pkt.data(), 5);
    EXPECT_FALSE(frame.isComplete());
    frame.reset();
    EXPECT_FALSE(frame.isComplete());
    // The full packet must now be parsed from scratch
    frame.feed(pkt.data(), pkt.size());
    EXPECT_TRUE(frame.isComplete());
}

TEST_F(FrameStreamTest, DefaultMaxPacketSizeIsOneMebibyte)
{
    EXPECT_EQ(frame.getMaxPacketSize(), Frame::DEFAULT_MAX_PACKET_SIZE);
    EXPECT_EQ(Frame::DEFAULT_MAX_PACKET_SIZE, 1024u * 1024u);
}

TEST_F(FrameStreamTest, AcceptsPacketAtTheMaximumSize)
{
    // A tiny limit so the boundary is cheap to exercise
    frame.setMaxPacketSize(8);
    // PUBLISH (qos0), topic "a": variable header is 3 bytes, so a Remaining
    // Length of 8 is exactly at the limit and must be accepted.
    std::vector<uint8_t> header = {0x30, 0x08, 0x00, 0x01, 'a', '1', '2', '3', '4', '5'};
    EXPECT_NO_THROW(frame.feed(header.data(), header.size()));
    EXPECT_TRUE(frame.isComplete());
}

TEST_F(FrameStreamTest, RejectsPacketAboveTheMaximumSize)
{
    frame.setMaxPacketSize(8);
    // PUBLISH header advertising a Remaining Length of 9, one over the limit.
    // feed() must refuse it as soon as the length is known, before any body byte.
    std::vector<uint8_t> header = {0x30, 0x09};
    EXPECT_THROW(frame.feed(header.data(), header.size()), std::runtime_error);
}

TEST_F(FrameStreamTest, RejectsBeforeBufferingTheBody)
{
    frame.setMaxPacketSize(4);
    // Advertise a large Remaining Length, then dribble a single body byte. The
    // throw must happen on the length field, so the body byte is never consumed
    // and only the two header bytes were buffered.
    std::vector<uint8_t> data = {0x30, 0x7F, 0x00};
    EXPECT_THROW(frame.feed(data.data(), data.size()), std::runtime_error);
    EXPECT_EQ(frame.feed(data.data(), 0), 0u);
}

TEST_F(FrameStreamTest, LimitAppliesBeforeTheLengthIsFullyDecoded)
{
    // A multi-byte Remaining Length (0x80 0x01 = 128) must be checked once the
    // field is complete, not byte by byte along the way.
    frame.setMaxPacketSize(64);
    std::vector<uint8_t> data = {0x30, 0x80, 0x01};
    EXPECT_THROW(frame.feed(data.data(), data.size()), std::runtime_error);
}

TEST_F(FrameStreamTest, RaisingTheLimitAcceptsLargerPackets)
{
    // The boundary test's 9-byte packet is refused at limit 8...
    frame.setMaxPacketSize(8);
    EXPECT_THROW(frame.feed(std::vector<uint8_t>{0x30, 0x09}.data(), 2), std::runtime_error);
    // ...and accepted once the limit is raised. The header is re-fed after reset.
    frame.reset();
    frame.setMaxPacketSize(9);
    std::vector<uint8_t> header = {0x30, 0x09, 0x00, 0x01, 'a', '1', '2', '3', '4', '5', '6'};
    EXPECT_NO_THROW(frame.feed(header.data(), header.size()));
    EXPECT_TRUE(frame.isComplete());
}

} // namespace MQTT
