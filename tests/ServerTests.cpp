#include <gtest/gtest.h>
#include "../src/Server.h"
#include "../src/Env.h"

namespace MQTT {

// The primary MQTT port is parsed from MQTT_PORT and passed to this constructor.
// A mistyped value must fail the startup loudly (a thrown exception) rather than
// bind the broker to an unintended port: 0 would let the OS pick an ephemeral
// port, and a value above 65535 would wrap through htons() to a wrong port. The
// Server constructor validates the port before opening any socket, so an invalid
// value throws without side effects; a valid one constructs cleanly.

TEST(ServerTest, RejectsNonPositivePort) {
    EXPECT_THROW(Server server(0), std::runtime_error);
    EXPECT_THROW(Server server(-1), std::runtime_error);
}

TEST(ServerTest, RejectsPortAboveMaximum) {
    EXPECT_THROW(Server server(65536), std::runtime_error);
    EXPECT_THROW(Server server(99999), std::runtime_error);
}

TEST(ServerTest, AcceptsBoundaryPorts) {
    // The two ends of the legal range must construct without throwing. The
    // Server is never started, so no socket is opened and stop() (called by the
    // destructor) is a no-op on a server that never ran.
    EXPECT_NO_THROW(Server server(1));
    EXPECT_NO_THROW(Server server(1883));
    EXPECT_NO_THROW(Server server(65535));
}

TEST(EnvTest, ParseLimitRejectsWhitespaceSignsAndJunk)
{
    unsigned long long out = 0;
    for (const char *value : {" 1", "\t1", "+1", "-1", "1 ", "1x", "", "0"}) {
        EXPECT_FALSE(Env::parseLimit(value, 100, out)) << "value=[" << value << "]";
    }
    EXPECT_TRUE(Env::parseLimit("1", 100, out));
    EXPECT_EQ(out, 1U);
    EXPECT_TRUE(Env::parseLimit("100", 100, out));
    EXPECT_EQ(out, 100U);
    EXPECT_FALSE(Env::parseLimit("101", 100, out));
}

TEST(EnvTest, ZeroIsOnlyAcceptedWhenTheSettingAllowsIt)
{
    unsigned long long out = 1;
    EXPECT_FALSE(Env::parseLimit("0", 100, out));
    EXPECT_TRUE(Env::parseLimit("0", 100, out, true));
    EXPECT_EQ(out, 0U);
}

} // namespace MQTT
