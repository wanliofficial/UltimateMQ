#include <gtest/gtest.h>
#include "../src/Server.h"

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

} // namespace MQTT
