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

} // namespace MQTT
