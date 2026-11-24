#include <gtest/gtest.h>
#include "../src/TLS.h"
#include <sys/socket.h>
#include <unistd.h>
#include <fcntl.h>
#include <chrono>
#include <cstdint>
#include <stdexcept>
#include <thread>
#include <vector>

namespace MQTT {

namespace {

//! Two ends of one socketpair: a real descriptor drives the non-blocking read
//! path that previously mistook EAGAIN for a closed peer.
struct SocketPair {
    int ends[2] = {-1, -1};
    SocketPair() {
        if (socketpair(AF_UNIX, SOCK_STREAM, 0, ends) != 0) {
            throw std::runtime_error("socketpair failed");
        }
    }
    ~SocketPair() {
        for (int &fd : ends) {
            if (fd >= 0) {
                ::close(fd);
                fd = -1;
            }
        }
    }
};

} // namespace

//! A non-blocking socket that is momentarily empty must WAIT for the rest of the
//! packet, not fail the read. Under FLOWMQ_WRITE_BUFFER the socket is
//! non-blocking, and a WebSocket frame (or MQTT packet) routinely arrives across
//! several reads. SecureTransport::readSome used to return -1 the moment read(2)
//! hit EAGAIN; WebSocketTransport::fillRawBuffer read that as "peer gone" and
//! closed the connection mid-frame, so a slow writer silently truncated the
//! stream. This pins that the read waits (bounded by the write timeout) and then
//! returns the bytes that arrive.
TEST(NonBlockingReadTests, WaitsForArrivingPacketInsteadOfFailingOnEagain)
{
    SocketPair pair;
    const int fd = pair.ends[0];
    const int flags = ::fcntl(fd, F_GETFL, 0);
    ASSERT_GE(flags, 0);
    ASSERT_EQ(::fcntl(fd, F_SETFL, flags | O_NONBLOCK), 0);

    // Generous timeout: the writer deliberately delivers late, so a read that
    // gives up at the first EAGAIN returns -1 while a correct one waits.
    SecureTransport transport(fd, nullptr, /*writeTimeoutMs=*/5000);

    const std::vector<uint8_t> payload = {0x30, 0x05, 'a', 'b', 'c', 'd', 'e'};
    std::thread writer([&pair, &payload]() {
        std::this_thread::sleep_for(std::chrono::milliseconds(250));
        ::write(pair.ends[1], payload.data(), payload.size());
    });

    uint8_t buf[16];
    const ssize_t got = transport.readSome(buf, sizeof(buf));
    writer.join();

    ASSERT_EQ(got, static_cast<ssize_t>(payload.size()))
        << "a read on an idle non-blocking socket must wait for the rest of the "
           "packet rather than report the peer gone";
    EXPECT_EQ(std::vector<uint8_t>(buf, buf + got), payload);
}

//! A genuine orderly close must still be reported as 0 (EOF), not conflated
//! with the transient would-block case above.
TEST(NonBlockingReadTests, OrderlyCloseStillReportsEndOfStream)
{
    SocketPair pair;
    SecureTransport transport(pair.ends[0], nullptr, /*writeTimeoutMs=*/5000);
    ::close(pair.ends[1]);
    pair.ends[1] = -1;

    uint8_t buf[16];
    EXPECT_EQ(transport.readSome(buf, sizeof(buf)), 0)
        << "a FIN must be reported as an orderly close";
}

} // namespace MQTT
