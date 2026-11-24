#include "Frame.h"
#include "Connection.h"
#include "Env.h"
#include "Topic.h"
#include "Utf8.h"
#include "Session.h"
#include "Broker.h"
#include "SysTopics.h"
#include "Auth.h"
#include "TLS.h"
#include <unistd.h>
#include <sys/socket.h>
#include <poll.h>
#include <fcntl.h>
#include <cerrno>
#include <algorithm>
#include <cstring>
#include <iostream>
#include <sstream>
#include <iomanip>
#include <memory>
#include <atomic>
#include <vector>
#include <chrono>
#include <random>
#include "Log.h"

namespace MQTT {

namespace {
//! How long a connecting client may take to send its CONNECT packet
constexpr int CONNECT_TIMEOUT_SECONDS = 10;
//! Granularity used to check the keep alive timer while waiting for data
constexpr int POLL_SLICE_MS = 250;
//! Ceiling on a default (no FLOWMQ_WRITE_BUFFER) TLS connection's kernel send
//! buffer. Unbounded, the auto-tuned buffer swallows a slow reader's whole output
//! so SO_SNDTIMEO never fires; this bounds undeliverable data per connection and
//! makes a stalled TLS write backpressure and time out predictably.
constexpr int TLS_DEFAULT_SNDBUF = 16 * 1024;

//! Build a ClientID for a client that sent a zero-length one. The broker must
//! treat such a client as if it had supplied this value (MQTT-3.1.3-6), so the
//! name is random to keep sessions apart, and the caller retries until it is
//! not already in use (MQTT-3.1.3-7).
std::string generateClientId() {
    static std::atomic<uint64_t> counter{0};
    static const uint64_t seed = [] {
        std::random_device device;
        auto now = static_cast<uint64_t>(
            std::chrono::steady_clock::now().time_since_epoch().count());
        return (static_cast<uint64_t>(device()) << 32) ^ device() ^ now;
    }();
    std::ostringstream out;
    out << "auto-" << std::hex << std::setw(16) << std::setfill('0')
        << (seed ^ (counter.fetch_add(1) * 0x9E3779B97F4A7C15ull));
    return out.str();
}
}

Connection::Connection(int sockfd, Broker* broker, std::shared_ptr<TLSContext> TLS)
    : Connection(sockfd, broker,
                 std::make_unique<SecureTransport>(
                     sockfd, std::move(TLS),
                     broker ? broker->getWriteTimeoutMs()
                            : SecureTransport::WRITE_STALL_TIMEOUT_MS)) {}

Connection::Connection(int sockfd, Broker* broker,
                       std::unique_ptr<ClientTransport> ownedTransport)
    : sockfd(sockfd), state(State::IDLE), broker(broker),
      writeTimeoutMs(broker ? broker->getWriteTimeoutMs()
                            : SecureTransport::WRITE_STALL_TIMEOUT_MS),
      auth(broker ? broker->getAuth() : nullptr),
      transport(std::move(ownedTransport)),
      lastActivity(std::chrono::steady_clock::now()) {
    // The optional outbound write buffer: when enabled the socket is made
    // non-blocking so writes that the kernel cannot take immediately return
    // EAGAIN (routed into the buffer) instead of stalling the connection thread.
    // With it disabled (the default) the socket stays blocking and the historical
    // SO_SNDTIMEO + writeAll path is used unchanged: a stalled write is bounded
    // by SO_SNDTIMEO (set on both the plaintext and TLS sockets below), so a peer
    // that stops reading is dropped after FLOWMQ_WRITE_TIMEOUT_MS rather than
    // pinning the connection thread forever.
    if (broker && broker->getWriteBufferBytes() > 0) {
        writeBufferLimit = broker->getWriteBufferBytes();
        writeBufferActive = true;
        const int flags = ::fcntl(sockfd, F_GETFL, 0);
        if (flags >= 0) {
            ::fcntl(sockfd, F_SETFL, flags | O_NONBLOCK);
        }
        // Cap the kernel send buffer. Left alone it auto-tunes to several MB and
        // silently swallows a slow reader's output, so the socket never reports
        // "would block" and the write buffer below never engages -- the broker
        // would happily queue megabytes of undeliverable data per connection.
        // A bounded send buffer makes backpressure engage at a predictable point
        // (the kernel buffer fills, the next write is would-block, overflow goes
        // into the write buffer), bounding per-connection kernel memory. The
        // value is doubled by the kernel for bookkeeping, so request half the
        // intended ceiling.
        const int sndbuf = static_cast<int>(writeBufferLimit / 2);
        if (sndbuf > 0) {
            ::setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
        }
    } else if (transport && transport->TLSEnabled()) {
        // Default (no FLOWMQ_WRITE_BUFFER) TLS path: the socket stays blocking and
        // writeAll() relies on SO_SNDTIMEO to bound a stalled SSL_write. But the
        // kernel send buffer auto-tunes to several MB, and an unbounded buffer
        // silently swallows a slow reader's output: SSL_write keeps succeeding
        // into the kernel buffer, SO_SNDTIMEO never fires, and a client that
        // stops reading is never dropped (its stalled write is effectively
        // unbounded). Capping SO_SNDBUF makes backpressure engage at a predictable
        // point: the buffer fills, the next SSL_write blocks in write(2), and
        // SO_SNDTIMEO turns that into EAGAIN -> a dropped connection after
        // FLOWMQ_WRITE_TIMEOUT_MS. The kernel doubles the value for bookkeeping,
        // so request half the intended ceiling.
        const int sndbuf = static_cast<int>(TLS_DEFAULT_SNDBUF / 2);
        ::setsockopt(sockfd, SOL_SOCKET, SO_SNDBUF, &sndbuf, sizeof(sndbuf));
    }
    // Cap the packet size before any byte (including the first CONNECT) is read.
    // The version is fixed later, but the limit does not depend on it.
    frame.setMaxPacketSize(broker ? broker->getMaxPacketSize() : Frame::DEFAULT_MAX_PACKET_SIZE);
    // Snapshot the inbound rate limit (0 means unlimited). The bucket starts
    // full with one second of allowance so a short burst is tolerated, then
    // refills at the configured rate.
    publishRateLimit = broker ? broker->getMaxPublishRate() : 0;
    publishTokens = static_cast<double>(publishRateLimit);
    publishBucketStamp = std::chrono::steady_clock::now();
    // This connection now counts toward the live connected-clients gauge.
    if (broker) {
        broker->adjustConnectedClients(+1);
    }
}

Connection::~Connection() {
    // Record why this connection ended, classified, exactly once here: the
    // destructor runs for every connection regardless of which path closed it
    // (graceful DISCONNECT, keep-alive timeout, protocol error, rate limit, or a
    // peer that simply vanished). disconnectReason was set at the decision point
    // and defaults to NORMAL_DISCONNECTION, so even an unannounced close is
    // counted. Recorded before the gauges are released so it captures this
    // connection's full lifecycle.
    if (broker) {
        broker->recordDisconnect(disconnectReason);
    }
    // Release this connection's contribution to the live gauges before the
    // socket goes away: any bytes still buffered (a slow reader we never caught
    // up with) leave the global write-buffer occupancy, and the client leaves
    // the connected count. Symmetric with the constructor's +1.
    if (broker) {
        if (writeBufferBytes > 0) {
            broker->adjustWriteBufferBytes(-static_cast<int64_t>(writeBufferBytes));
        }
        broker->adjustConnectedClients(-1);
    }
    closeSocket();
}

void Connection::run() {
    try {
        // Complete the TLS handshake before any MQTT bytes are read. A failed
        // handshake (wrong protocol, untrusted client certificate, ...) ends
        // the connection here, before a CONNECT can be forged.
        if (transport->TLSEnabled()) {
            std::string handshakeError;
            if (!transport->handshake(&handshakeError)) {
                logInfo("dropping connection: %s", handshakeError.c_str());
                return;
            }
        }
        scheduleReadTimeout();
        runReadLoop();
    } catch (const Frame::PacketTooLarge &e) {
        logError("dropping connection: %s", e.what());
        // MQTT-4.13: a packet above the limit is reported as PACKET_TOO_LARGE,
        // not as a malformed one, so the client knows to send smaller packets.
        closeWithReason(ReasonCode::PACKET_TOO_LARGE);
        } catch (const Frame::ProtocolViolation &e) {
            logError("dropping connection: %s", e.what());
            // MQTT-4.13: the packet parsed cleanly but its content breaks a rule, so
            // the client is told PROTOCOL_ERROR rather than MALFORMED_PACKET.
            closeWithReason(ReasonCode::PROTOCOL_ERROR);
        } catch (const Frame::UnsupportedProtocol &e) {
            logError("dropping connection: %s", e.what());
            // §3.1.2.1: answer UNSUPPORTED_PROTOCOL_VERSION in the encoding of the
            // version the client asked for - 0x84 on an MQTT 5 framing, 0x01 on the
            // shared MQTT 3.1.x framing (whose return code table stops at 0x05).
            // For an unknown version byte there is no encoding both sides agree
            // on, so the Network Connection simply closes, as MQTT-3.1.2-1 allows.
            if (e.detected >= 3 && e.detected <= 5) {
                frame.setVersion(e.detected == 5 ? Version::MQTT5
                                                 : (e.detected == 3 ? Version::MQTT31
                                                                    : Version::MQTT311));
                ConnackPacket connack{false, ReasonCode::UNSUPPORTED_PROTOCOL_VERSION};
                disconnectReason = ReasonCode::UNSUPPORTED_PROTOCOL_VERSION;
                connackSent = true;
                sendPacket(connack);
            }
        } catch (const std::exception &e) {
        logError("dropping connection: %s", e.what());
        // Anything else the broker cannot parse is malformed (MQTT-4.13): tell
        // an MQTT 5 client which error ended the connection instead of vanishing.
        closeWithReason(ReasonCode::MALFORMED_PACKET);
    }
    state = State::DISCONNECTED;

    // An abnormal termination publishes the Will message.
    const auto ownedSession = currentSession();
    if (willPending.exchange(false)) {
        // MQTT-3.1.2-8 / §3.1.2.11.3: releasing a Will is triggered by the Will
        // Delay Interval elapsing *or* the Session ending, whichever comes
        // first. A Session ends here only when it is not carried past this
        // connection - a non-persistent session, or a persistent one whose
        // DISCONNECT lowered the Session Expiry Interval to 0 (MQTT-3.14.4-2).
        // A session that survives merely goes dormant: its Will Delay runs in
        // full, and when it *does* end later (the sweeper discarding it at the
        // Session Expiry) the Will goes out then. The decision must use
        // sessionEndsAtClose(), not the CONNECT-time persistence alone, or a
        // session ended by the DISCONNECT would still serve out a Will Delay
        // for a Session that no longer exists. `session` is checked because a
        // CONNECT refused before the session is assigned must not turn this
        // teardown into a null dereference.
        const bool sessionEnds = !ownedSession || sessionEndsAtClose(*ownedSession);
        const auto dueWill = broker->settleConnectionWill(
            ownedSession, shared_from_this(), sessionEnds, willMessage, willDelay,
            willHasExpiry, willExpiryInterval, authenticatedUsername);
        if (dueWill) {
            // MQTT-3.3.2-9: the Will's Message Expiry starts at publication,
            // after any Will Delay, not at CONNECT or Network Connection close.
            publishWillMessage(*dueWill, authenticatedUsername);
        }
    }

    // Detach from the session. Whether it outlives this connection is governed
    // by the Session Expiry Interval (MQTT-3.14.4-2 / 3.1.2-23): a graceful
    // DISCONNECT may override it with a value carried in the DISCONNECT, and a
    // zero interval discards the session at once. A non-zero interval keeps the
    // session resumable for that long, whether the close was graceful or not.
    releaseSession();
    closeSocket();
}

bool Connection::sessionEndsAtClose(const Session &liveSession) const {
    // Whether the session outlives this Network Connection. MQTT 3.1.x has no
    // Session Expiry Interval, so its Clean Session flag is the whole story:
    // CleanSession=0 keeps the session until a later CleanSession=1 replaces it,
    // and there is no deadline to arm. MQTT 5 uses the interval instead, which a
    // DISCONNECT may lower - down to 0, which ends the session at once
    // (MQTT-3.14.4-2). Reading it as "sessionExpirySeconds == 0 means discard"
    // dropped every MQTT 3.1.1 persistent session, because that field is only
    // ever set for MQTT 5.
    //
    // The session is passed in rather than read from this->session so
    // releaseSession() can ask before it drops the member.
    if (!liveSession.isPersistent()) {
        return true;
    }
    return frame.getVersion() == Version::MQTT5 && sessionExpirySeconds == 0;
}

void Connection::releaseSession() {
    auto owned = currentSession();
    if (!owned) {
        return;
    }
    const bool endSession = sessionEndsAtClose(*owned);
    if (owned->close(shared_from_this(), endSession) && !endSession &&
        frame.getVersion() == Version::MQTT5 && sessionExpirySeconds > 0) {
        owned->armExpiry(std::chrono::seconds(sessionExpirySeconds));
    }
}

void Connection::scheduleReadTimeout() {
    if (state == State::CONNECTED) {
        armKeepAliveTimer();
    } else {
        readTimeout = std::chrono::milliseconds(CONNECT_TIMEOUT_SECONDS * 1000);
    }
}

void Connection::armKeepAliveTimer() {
    if (keepAliveSeconds == 0) {
        readTimeout = std::chrono::milliseconds(0); // disabled
        return;
    }
    // MQTT-3.1.2-24: the broker disconnects after 1.5 times the keep alive
    readTimeout = std::chrono::milliseconds(static_cast<int64_t>(keepAliveSeconds) * 1500);
}

void Connection::runReadLoop() {
    //! The keep alive just expired: the Network Connection is closed but the
    //! Will still has to go out (MQTT-3.1.2-8 treats it as an abnormal end), so
    //! the reason travels back to run() instead of closing here.
    bool keepAliveExpired = false;
    while (state != State::DISCONNECTED) {
        // Enforce the outbound write-stall deadline even when the peer keeps
        // sending (so POLLIN, never POLLOUT, refreshes lastActivity/keepalive)
        // but never reads: the write buffer would otherwise never be drained and
        // its stall check (in drainWriteBuffer) would never run. A peer that
        // sends keepalives but will not read is too slow, not alive.
        const bool writePending = writeBufferActive && writeBufferPending();
        if (writePending) {
            int64_t stalledFor;
            {
                std::lock_guard<std::recursive_mutex> lock(writeMutex);
                stalledFor = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::steady_clock::now() - lastWriteProgress).count();
            }
            if (stalledFor > writeTimeoutMs) {
                logInfo("connection closed: outbound write stalled %lld ms (slow or dead reader)",
                        (long long)stalledFor);
                state = State::DISCONNECTED;
                return;
            }
        }
        int timeout = POLL_SLICE_MS;
        if (readTimeout.count() > 0) {
            auto deadline = lastActivity + readTimeout;
            auto now = std::chrono::steady_clock::now();
            if (now >= deadline) {
                logDebug("keep alive expired, closing connection");
                // Only a connected client has a Keep Alive to violate: before
                // CONNECT this same timer is the CONNECT timeout, which just
                // drops a silent peer without inventing a reason code.
                keepAliveExpired = state == State::CONNECTED;
                break;
            }
            timeout = static_cast<int>(
                std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
            timeout = std::min(timeout, POLL_SLICE_MS);
        }
        // Keep Alive 0 removes only the inbound deadline, not outbound progress:
        // a buffered writer must still wake on POLLOUT and enforce its stall bound.
        // Its socket is non-blocking even while idle, so EAGAIN must wait for
        // POLLIN rather than being mistaken for a peer close.
        if (readTimeout.count() > 0 || writeBufferActive) {
            int ready;
            if (writePending && transport->fd() >= 0) {
                struct pollfd pfd;
                pfd.fd = transport->fd();
                pfd.events = POLLIN | POLLOUT;
                pfd.revents = 0;
                ready = ::poll(&pfd, 1, timeout);
                if (ready < 0) {
                    if (errno == EINTR) {
                        continue;
                    }
                    ready = 1; // let the drain/read surface a real error
                } else if (ready > 0) {
                    if (pfd.revents & POLLOUT) {
                        drainWriteBuffer();
                    }
                    if (state == State::DISCONNECTED) {
                        return;
                    }
                    // Only read when the peer actually has data; a lone POLLOUT
                    // wake-up must not turn into a (would-block) read.
                    if (!(pfd.revents & POLLIN)) {
                        continue;
                    }
                    ready = 1;
                } else {
                    continue; // timeout: keep watching the deadline
                }
            } else {
                ready = transport->readable(timeout);
                if (ready < 0) {
                    continue;
                }
                if (ready == 0) {
                    continue;
                }
            }
        }

        ssize_t bytesRead = transport->readSome(buffer, BUFFER_SIZE);
        if (bytesRead <= 0) {
            return;
        }

        size_t consumed = 0;
        while (consumed < static_cast<size_t>(bytesRead) && state != State::DISCONNECTED) {
            size_t used = frame.feed(buffer + consumed, bytesRead - consumed);
            if (used == 0) {
                break;
            }
            consumed += used;
            if (frame.isComplete()) {
                auto packet = frame.parse();
                frame.reset();
                handleIncoming(packet);
                // MQTT-3.1.2-24: the Keep Alive timer resets only on a complete
                // Control Packet, not on partial bytes dribbled by the peer.
                lastActivity = std::chrono::steady_clock::now();
            }
        }
    }
    if (keepAliveExpired) {
        // MQTT-3.1.2-24: the server must close the connection after 1.5 times
        // the Keep Alive. §3.11.3 defines 0x8D (KEEP_ALIVE_TIMEOUT) for exactly
        // this case, and an MQTT 5 client that gets no DISCONNECT cannot tell a
        // timeout from a network fault.
        closeWithReason(ReasonCode::KEEP_ALIVE_TIMEOUT);
        // The Will is left pending exactly as for any abnormal close: MQTT-3.1.2-8
        // only deletes it on a normal DISCONNECT, which never arrived here. When a
        // DISCONNECT was already handled, willPending is false already and the
        // loop above exits on the DISCONNECTED state rather than this timeout.
    }
}

void Connection::resetParser() {
    frame.reset();
}

bool Connection::isConnected() const {
    return state == State::CONNECTED;
}

void Connection::closeSocket() {
    std::lock_guard<std::recursive_mutex> lock(writeMutex);
    // A write-stall drop must reach the peer as a RST, not a graceful FIN. The
    // kernel send buffer is still full of data the peer refuses to read, so a
    // plain close() would leave that data queued behind the peer's closed
    // receive window and the FIN would never be sent: the stalled reader would
    // never see EOF and would leak. SO_LINGER={1,0} makes close() discard the
    // queued data and send a RST immediately, so the peer sees the end of the
    // connection at once regardless of how much is stuck in the buffer.
    if (writeStallDrop && sockfd >= 0) {
        struct linger lingerOpt{};
        lingerOpt.l_onoff = 1;
        lingerOpt.l_linger = 0;
        ::setsockopt(sockfd, SOL_SOCKET, SO_LINGER, &lingerOpt, sizeof(lingerOpt));
        close(sockfd);
        sockfd = -1;
        return;
    }
    // Flush the TLS close_notify before the descriptor goes away
    if (transport) {
        transport->teardown();
    }
    if (sockfd >= 0) {
        shutdown(sockfd, SHUT_RDWR);
        close(sockfd);
        sockfd = -1;
    }
}

void Connection::setDisconnectCallback(std::function<void()> callback) {
    disconnectCallback = std::move(callback);
}

void Connection::onSuperseded(bool publishOldWill) {
    // MQTT-3.1.4-3 requires the old Network Connection to receive DISCONNECT
    // 0x8E when it speaks MQTT 5, then to be closed. MQTT 3.1.1 has no server
    // DISCONNECT packet, so that peer observes only the close.
    disconnectReason = ReasonCode::SESSION_TAKEN_OVER;
    if (frame.getVersion() == Version::MQTT5 && state == State::CONNECTED) {
        try {
            DisconnectPacket disconnect{ReasonCode::SESSION_TAKEN_OVER};
            sendPacket(disconnect);
        } catch (const std::exception &e) {
            logDebug("superseded connection could not be notified: %s", e.what());
        }
    }
    storeSession(nullptr);
    connected = false;
    state = State::DISCONNECTED;

    // MQTT-3.1.4-3 publishes the old Will when the new connection has Clean Start
    // set, the old Session is not continuing, or its Will Delay is zero. Otherwise
    // the continuing Session cancels the delayed Will by reconnecting within the
    // delay interval (MQTT-3.1.3-9 / §3.1.3.2.2); resumeSession() also clears any
    // Will left pending by the old connection.
    if (willPending.exchange(false) && publishOldWill) {
        publishWill();
    }
    // The takeover thread never destroys a transport that its owning thread may
    // still be reading. Wake that thread; run() performs teardown after readSome
    // has returned (TLS and WebSocket state are not safe to free concurrently).
    if (transport) {
        transport->interrupt();
    }
}

void Connection::publishWill() {
    willPending = false;
    Message will = willMessage;
    armWillExpiry(will);
    publishWillMessage(will, authenticatedUsername);
}

void Connection::publishWillMessage(const Message &will,
                                    const std::string &username) {
    // The Will is published as the client, so the same write ACL applies as
    // for a regular PUBLISH
    if (auth && auth->isForbidden(username, will.topic, /*write=*/true)) {
        logDebug("denied will on %s (acl)", will.topic.c_str());
        return;
    }
    broker->publish(will);
}

void Connection::armWillExpiry(Message &will) const {
    // MQTT-3.3.2-9: the Message Expiry Interval of a Will is relative to its
    // publication, which is exactly now: the Will Delay Interval has already
    // elapsed and must not be subtracted from the lifetime.
    if (!willHasExpiry) {
        return;
    }
    will.hasExpiry = true;
    will.expiryDeadline =
        std::chrono::steady_clock::now() + std::chrono::seconds(willExpiryInterval);
}

void Connection::handleIncoming(std::shared_ptr<Packet> packet) {
    // MQTT-3.1.0-1: the first packet from a client MUST be a CONNECT, and
    // MQTT-3.1.0-2 forbids a second one on the same Network Connection. Only
    // CONNECT itself is dispatched while no session exists; everything else is
    // refused here, before any handler can answer it. Previously a PINGREQ
    // sent before the CONNECT was answered with a PINGRESP, which amounts to
    // completing a protocol exchange none of the two packets is allowed to
    // take part in.
    if (packet->type != PacketType::CONNECT && !currentSession() && state != State::CONNECTED) {
        logInfo("refusing packet type %d before CONNECT (MQTT-3.1.0-1)",
                static_cast<int>(packet->type));
        // No CONNACK here: it is the answer to a CONNECT, and no CONNECT was
        // accepted. §4.13: a DISCONNECT may only follow in a session, so the
        // Network Connection is simply closed as MQTT-3.1.0-1 requires.
        if (state == State::CONNECTED) {
            closeWithReason(ReasonCode::PROTOCOL_ERROR);
        } else {
            state = State::DISCONNECTED;
        }
        return;
    }
    switch (packet->type) {
    case PacketType::CONNECT:
        handleConnect(std::static_pointer_cast<ConnectPacket>(packet));
        break;
    case PacketType::PUBLISH:
        handlePublish(std::static_pointer_cast<PublishPacket>(packet));
        break;
    case PacketType::PUBACK:
        handlePuback(std::static_pointer_cast<PubackPacket>(packet));
        break;
    case PacketType::PUBREC:
        handlePubrec(std::static_pointer_cast<PubrecPacket>(packet));
        break;
    case PacketType::PUBREL:
        handlePubrel(std::static_pointer_cast<PubrelPacket>(packet));
        break;
    case PacketType::PUBCOMP:
        handlePubcomp(std::static_pointer_cast<PubcompPacket>(packet));
        break;
    case PacketType::SUBSCRIBE:
        handleSubscribe(std::static_pointer_cast<SubscribePacket>(packet));
        break;
    case PacketType::UNSUBSCRIBE:
        handleUnsubscribe(std::static_pointer_cast<UnsubscribePacket>(packet));
        break;
    case PacketType::PINGREQ:
        handlePingreq(std::static_pointer_cast<PingreqPacket>(packet));
        break;
    case PacketType::DISCONNECT:
        handleDisconnect(std::static_pointer_cast<DisconnectPacket>(packet));
        break;
    case PacketType::AUTH:
        handleAuth(std::static_pointer_cast<AuthPacket>(packet));
        break;
    default:
        throw std::runtime_error("Unhandled packet type");
    }
}

void Connection::handleConnect(std::shared_ptr<ConnectPacket> connect) {
    if (state != State::IDLE) {
        // MQTT-3.1.0-2: "The Server MUST process a second CONNECT packet sent
        // from a Client as a Protocol Error and close the Network Connection."
        // The packet parses perfectly - it just contradicts the state the
        // Server is already in - so §1.5.6/§4.13 make this a Protocol Error
        // (0x82), not a Malformed Packet (0x81). Throwing the generic
        // std::runtime_error here fell through to run()'s catch-all and was
        // reported to the client as MALFORMED_PACKET.
        throw Frame::ProtocolViolation("a second CONNECT on the same Network Connection "
                                       "(MQTT-3.1.0-2)");
    }
    frame.setVersion(connect->protocolVersion);
    // Topic Alias mappings are scoped to a single network connection, so a fresh
    // CONNECT starts with an empty table (MQTT-3.3.2-7).
    topicAliases.clear();

    // Validate the CONNECT flags before anything else: several combinations are
    // protocol errors that must be refused outright rather than half-honoured.
    // Rejecting here also keeps a malformed Will from ever being armed.
    {
        bool badFlags = false;
        const char *why = "";
        // MQTT-3.1.2-3: the reserved flag bit MUST be zero.
        if (connect->reservedFlagSet) {
            badFlags = true;
            why = "reserved CONNECT flag bit is set";
        }
        // MQTT-3.1.2-2 (MQTT 3.1.x only): the Password Flag may only be set when
        // the User Name Flag is set. MQTT 5 dropped that restriction, so a v5
        // Password without a User Name stays legal here.
        else if (frame.getVersion() != Version::MQTT5 && connect->password &&
                 !connect->username) {
            badFlags = true;
            why = "Password Flag set without the User Name Flag";
        }
        // MQTT-3.1.2-13 / MQTT-3.1.2-14: when the Will Flag is 0 the Will QoS must
        // be 0; when it is 1 the Will QoS may be 0, 1 or 2 but never 3.
        else if (connect->willFlag
                     ? static_cast<uint8_t>(connect->willQos) > 2
                     : connect->willQos != QoS::QOS_0) {
            badFlags = true;
            why = connect->willFlag ? "Will QoS is 3" : "Will QoS set without the Will Flag";
        }
        // MQTT-3.1.2-15: when the Will Flag is 0 the Will Retain flag must be 0.
        else if (!connect->willFlag && connect->willRetain) {
            badFlags = true;
            why = "Will Retain set without the Will Flag";
        }
        if (badFlags) {
            logInfo("rejecting client %s: invalid CONNECT flags (%s)",
                    connect->clientId.c_str(), why);
            if (frame.getVersion() == Version::MQTT5) {
                // MQTT 5 has a Protocol Error reason code for this.
                ConnackPacket connack{false, ReasonCode::PROTOCOL_ERROR};
                disconnectReason = ReasonCode::PROTOCOL_ERROR;
                connackSent = true;
                sendPacket(connack);
            }
            // MQTT 3.1.1 (MQTT-3.1.4-1): a CONNECT that does not conform is
            // answered by closing the Network Connection, with no CONNACK.
            state = State::DISCONNECTED;
            return;
        }
    }

    // Authenticate before any session state is touched (and before the Will is
    // armed): a rejected client must not be able to take over an existing
    // session, learn about its messages, or publish a Will message.
    //
    // Enhanced authentication is not implemented, so a CONNECT that asks for an
    // authentication method must be refused up front (MQTT-4.12.0-1) rather
    // than silently falling back to username/password. A method name plus data
    // without a method is also a protocol error (MQTT-3.1.2-17).
    if (frame.getVersion() == Version::MQTT5) {
        if (connect->properties.find(PropertyID::AUTHENTICATION_DATA) !=
                connect->properties.end() &&
            connect->properties.find(PropertyID::AUTHENTICATION_METHOD) ==
                connect->properties.end()) {
            logInfo("rejecting client %s: authentication data without a method",
                    connect->clientId.c_str());
            ConnackPacket connack{false, ReasonCode::PROTOCOL_ERROR};
            disconnectReason = ReasonCode::PROTOCOL_ERROR;
            connackSent = true;
            sendPacket(connack);
            state = State::DISCONNECTED;
            return;
        }
        auto method = connect->properties.find(PropertyID::AUTHENTICATION_METHOD);
        if (method != connect->properties.end()) {
            logInfo("rejecting client %s: unsupported authentication method",
                    connect->clientId.c_str());
            ConnackPacket connack{false, ReasonCode::BAD_AUTHENTICATION_METHOD};
            disconnectReason = ReasonCode::BAD_AUTHENTICATION_METHOD;
            connackSent = true;
            sendPacket(connack);
            state = State::DISCONNECTED;
            return;
        }
    }

    // A zero-length ClientID is a special case: the broker must assign a unique
    // one and treat the client as if it had supplied it (MQTT-3.1.3-6), and
    // report it in the CONNACK (MQTT-3.1.3-7). It is only legal with CleanStart
    // set, because there is no name the session could be resumed under
    // (MQTT-3.1.3-8 region / MQTT 3.1.1 return code 0x02 below).
    bool assignedClientId = false;
    if (connect->clientId.empty()) {
        if (!connect->cleanStart) {
            logInfo("rejecting client: a zero-length ClientID requires a clean start");
            if (frame.getVersion() == Version::MQTT5) {
                disconnectReason = ReasonCode::CLIENT_IDENTIFIER_NOT_VALID;
                ConnackPacket connack{false, ReasonCode::CLIENT_IDENTIFIER_NOT_VALID};
                connackSent = true;
                sendPacket(connack);
            } else {
                // MQTT 3.1.1 has no 0x85 reason code; its CONNACK return code for
                // "identifier rejected" is 0x02.
                disconnectReason = ReasonCode::CLIENT_IDENTIFIER_NOT_VALID;
                ConnackPacket connack{false, static_cast<ReasonCode>(0x02)};
                connackSent = true;
                sendPacket(connack);
            }
            state = State::DISCONNECTED;
            return;
        }
        do {
            connect->clientId = generateClientId();
        } while (broker && broker->findSession(connect->clientId) != nullptr);
        assignedClientId = true;
        logDebug("assigned ClientID %s", connect->clientId.c_str());
    }

    std::string username;
    // When a verified client certificate is available and the client sends no
    // MQTT username, the certificate's identity can stand in for the username
    // (mTLS authorisation). Gated behind FLOWMQ_TLS_CERT_AS_USERNAME so the
    // default behaviour is unchanged.
    const bool certAsUsername = Env::isEnabled("FLOWMQ_TLS_CERT_AS_USERNAME");
    bool certificateAuthenticated = false;
    if (certAsUsername && !connect->username) {
        const std::string identity = transport->peerIdentity();
        if (!identity.empty()) {
            username = identity;
            // The certificate itself is the credential, so this identity is
            // already authenticated; no password check applies.
            certificateAuthenticated = true;
            logInfo("client %s: using verified client-certificate identity %s as the username",
                    connect->clientId.c_str(), username.c_str());
        }
    }
    if (connect->username) {
        // A username was supplied: authenticate it with the password as usual.
        // (This takes precedence over any certificate identity.)
        username = *connect->username;
        if (auth && auth->authRequired()) {
            const std::string password = connect->password ? *connect->password : std::string();
            if (!auth->authenticate(username, password)) {
                logInfo("rejecting client %s: bad credentials for user %s",
                        connect->clientId.c_str(), username.c_str());
                ConnackPacket connack{false, ReasonCode::BAD_USERNAME_OR_PASSWORD};
                disconnectReason = ReasonCode::BAD_USERNAME_OR_PASSWORD;
                connackSent = true;
                sendPacket(connack);
                state = State::DISCONNECTED;
                return;
            }
        }
    } else if (!certificateAuthenticated) {
        // No username and no usable certificate identity: a broker that requires
        // authentication must reject the connection (MQTT-3.1.2-9).
        if (auth && auth->authRequired()) {
            logInfo("rejecting client %s: username required", connect->clientId.c_str());
            ConnackPacket connack{false, ReasonCode::BAD_USERNAME_OR_PASSWORD};
            disconnectReason = ReasonCode::BAD_USERNAME_OR_PASSWORD;
            connackSent = true;
            sendPacket(connack);
            state = State::DISCONNECTED;
            return;
        }
        // Authentication not required: leave the client anonymous, as before.
    }

    // Keep alive: normally the client's value, but the broker may prefer a
    // shorter (or finite) one. MQTT 5 lets the server override it through the
    // Server Keep Alive CONNACK property (MQTT-3.1.2-21); the client must then
    // use that value, and 3.1.1 clients are unaffected. Without this, a client
    // asking for 0 would leave its connection un-reaped forever.
    keepAliveSeconds = connect->keepAlive;
    armKeepAliveTimer();

    // The Will topic is a topic name, so a wildcard in it is a protocol error
    // (MQTT-3.1.2-12): refusing the whole CONNECT is safer than accepting it and
    // later failing to publish a Will it cannot route.
    if (connect->willFlag && connect->willTopic && !Topic::isValidTopicName(*connect->willTopic)) {
        logInfo("rejecting client %s: will topic %s contains a wildcard",
                connect->clientId.c_str(), connect->willTopic->c_str());
        if (frame.getVersion() == Version::MQTT5) {
            ConnackPacket connack{false, ReasonCode::TOPIC_NAME_INVALID};
            disconnectReason = ReasonCode::TOPIC_NAME_INVALID;
            connackSent = true;
            sendPacket(connack);
        }
        // MQTT 3.1.1's return code table only spans 0x00-0x05, so a v5 reason
        // code would be an undefined value on the wire. [MQTT-3.1.4-1] requires a
        // CONNECT that does not conform to be answered by closing the Network
        // Connection, which is what the other v3 CONNECT rejections do too.
        state = State::DISCONNECTED;
        return;
    }
    // MQTT 5 property validation: reject before any session state is touched or
    // the Will is armed. A rejected CONNECT must never publish a Will or cancel
    // another client's pending Will.
    uint32_t validatedClientMaxPacket = 0;
    uint16_t validatedReceiveMaximum = 65535;
    if (frame.getVersion() == Version::MQTT5) {
        auto maxPacket = connect->properties.find(PropertyID::MAXIMUM_PACKET_SIZE);
        if (maxPacket != connect->properties.end() &&
            std::holds_alternative<uint32_t>(maxPacket->second)) {
            const uint32_t clientLimit = std::get<uint32_t>(maxPacket->second);
            if (clientLimit == 0) {
                logInfo("rejecting client %s: Maximum Packet Size must not be 0",
                        connect->clientId.c_str());
                ConnackPacket rejectConnack{false, ReasonCode::PROTOCOL_ERROR};
                disconnectReason = ReasonCode::PROTOCOL_ERROR;
                connackSent = true;
                sendPacket(rejectConnack);
                state = State::DISCONNECTED;
                return;
            }
            validatedClientMaxPacket = clientLimit;
        }
        auto it = connect->properties.find(PropertyID::RECEIVE_MAXIMUM);
        if (it != connect->properties.end() && std::holds_alternative<uint16_t>(it->second)) {
            uint16_t receiveMaximum = std::get<uint16_t>(it->second);
            if (receiveMaximum == 0) {
                logInfo("rejecting client %s: Receive Maximum must not be 0",
                        connect->clientId.c_str());
                ConnackPacket rejectConnack{false, ReasonCode::PROTOCOL_ERROR};
                disconnectReason = ReasonCode::PROTOCOL_ERROR;
                connackSent = true;
                sendPacket(rejectConnack);
                state = State::DISCONNECTED;
                return;
            }
            validatedReceiveMaximum = receiveMaximum;
        }
    }

    // Whether the session survives this connection is decided by the Session
    // Expiry Interval, not by Clean Start (MQTT-3.1.2-23): Clean Start only says
    // whether a *previous* session is discarded, while a non-zero Session Expiry
    // Interval keeps the session resumable after the connection closes. MQTT 5
    // carries that interval as a CONNECT property (absent = 0 = discard on
    // close); MQTT 3.1.x has no such property, so its Clean Session flag is the
    // only signal (cleanSession=0 keeps the session).
    uint32_t connectSessionExpiry = 0;
    bool persistentSession = false;
    if (frame.getVersion() == Version::MQTT5) {
        auto expiry = connect->properties.find(PropertyID::SESSION_EXPIRY_INTERVAL);
        if (expiry != connect->properties.end() &&
            std::holds_alternative<uint32_t>(expiry->second)) {
            connectSessionExpiry = std::get<uint32_t>(expiry->second);
        }
        persistentSession = connectSessionExpiry > 0;
    } else {
        persistentSession = !connect->cleanStart;
    }
    sessionExpirySeconds = connectSessionExpiry;
    this->connectSessionExpiry = connectSessionExpiry;
    sessionClientId = connect->clientId;
    authenticatedUsername = username;

    // All validation has passed: now it is safe to arm the Will and cancel any
    // pending delayed Will for this client id. A rejected CONNECT never reaches
    // here, so it cannot publish a Will or cancel another client's pending Will.
    if (connect->willFlag && connect->willTopic && connect->willMsg) {
        willPending = true;
        willMessage = Message(*connect->willTopic, *connect->willMsg, connect->willQos, connect->willRetain);
        willDelay = 0;
        if (connect->willProperties) {
            for (PropertyID forwardable : {PropertyID::PAYLOAD_FORMAT_INDICATOR,
                                           PropertyID::MESSAGE_EXPIRY_INTERVAL,
                                           PropertyID::CONTENT_TYPE,
                                           PropertyID::RESPONSE_TOPIC,
                                           PropertyID::CORRELATION_DATA,
                                           PropertyID::USER_PROPERTY}) {
                auto it = connect->willProperties->find(forwardable);
                if (it != connect->willProperties->end()) {
                    willMessage.properties[forwardable] = it->second;
                }
            }
            auto it = connect->willProperties->find(PropertyID::WILL_DELAY_INTERVAL);
            if (it != connect->willProperties->end() && std::holds_alternative<uint32_t>(it->second)) {
                willDelay = std::get<uint32_t>(it->second);
            }
            auto expiry = connect->willProperties->find(PropertyID::MESSAGE_EXPIRY_INTERVAL);
            if (expiry != connect->willProperties->end() &&
                std::holds_alternative<uint32_t>(expiry->second)) {
                willExpiryInterval = std::get<uint32_t>(expiry->second);
                willHasExpiry = true;
            }
        }
    }
    // Authorization, Clean Start replacement and registry installation are one
    // atomic broker transition. Two simultaneous CONNECTs for one ClientID can
    // therefore never install distinct live sessions.
    auto activation = broker->prepareSession(connect->clientId, username,
                                              connect->cleanStart, persistentSession,
                                              shared_from_this(), validatedReceiveMaximum);
    if (activation.rejection != ReasonCode::SUCCESS) {
        logInfo("rejecting client %s: session belongs to a different user (%s)",
                connect->clientId.c_str(), username.c_str());
        ConnackPacket connack{false, activation.rejection};
        disconnectReason = activation.rejection;
        connackSent = true;
        sendPacket(connack);
        state = State::DISCONNECTED;
        return;
    }
    auto sessionToUse = activation.session;
    const bool resumed = activation.resumed;
    clientMaxPacketSize = validatedClientMaxPacket;
    storeSession(sessionToUse);

    setDisconnectCallback([weak = weak_from_this()]() {
        if (auto connection = weak.lock()) {
            if (connection->connected) {
                connection->state = State::DISCONNECTED;
            }
        }
    });

    // Publish the CONNACK before driving the session so that no delivery can
    // race ahead of it. sessionPresent is set only when an existing session is
    // actually being resumed.
    ConnackPacket connack{false, ReasonCode::SUCCESS};
    connack.sessionPresent = resumed;
    if (frame.getVersion() == Version::MQTT5) {
        const size_t limit = broker ? broker->getMaxPacketSize() : Frame::DEFAULT_MAX_PACKET_SIZE;
        connack.properties[PropertyID::MAXIMUM_PACKET_SIZE] = static_cast<uint32_t>(limit);
        connack.properties[PropertyID::TOPIC_ALIAS_MAXIMUM] = TOPIC_ALIAS_MAXIMUM;
        connack.properties[PropertyID::WILDCARD_SUBSCRIPTION_AVAILABLE] = true;
        connack.properties[PropertyID::SUBSCRIPTION_IDENTIFIER_AVAILABLE] = true;
        connack.properties[PropertyID::SHARED_SUBSCRIPTION_AVAILABLE] = true;
        connack.properties[PropertyID::RETAIN_AVAILABLE] = true;
        connack.properties[PropertyID::MAXIMUM_QOS] = static_cast<uint8_t>(QoS::QOS_2);
        if (assignedClientId) {
            connack.properties[PropertyID::ASSIGNED_CLIENT_IDENTIFIER] = connect->clientId;
        }
        const uint16_t preferred = broker ? broker->getServerKeepAlive() : 0;
        if (preferred != 0 && (keepAliveSeconds == 0 || keepAliveSeconds > preferred)) {
            keepAliveSeconds = preferred;
            armKeepAliveTimer();
            connack.properties[PropertyID::SERVER_KEEP_ALIVE] = preferred;
        }
    }
    connackSent = true;
    bool connackWritten = false;
    try {
        connackWritten = sendPacket(connack);
    } catch (...) {
        if (activation.finish) {
            activation.finish();
        }
        throw;
    }
    if (!connackWritten || state == State::DISCONNECTED) {
        if (activation.finish) {
            activation.finish();
        }
        state = State::DISCONNECTED;
        if (transport) {
            transport->interrupt();
        }
        return;
    }
    {
        std::lock_guard<std::recursive_mutex> lock(writeMutex);
        if (state != State::DISCONNECTED) {
            connected = true;
            state = State::CONNECTED;
        }
    }
    if (activation.finish && currentSession() == sessionToUse &&
        state == State::CONNECTED) {
        activation.finish();
    }
    if (resumed && currentSession() == sessionToUse && state == State::CONNECTED) {
        sessionToUse->flush();
    }
    logInfo("client connected: %s (resumed=%d)", connect->clientId.c_str(), (int)resumed);
}

bool Connection::allowPublish() {
    if (publishRateLimit == 0) {
        return true;
    }
    const auto now = std::chrono::steady_clock::now();
    const double elapsed =
        std::chrono::duration<double>(now - publishBucketStamp).count();
    publishBucketStamp = now;
    // Refill, but never beyond one second of allowance so an idle client
    // cannot bank an unbounded burst.
    publishTokens = std::min(publishTokens + elapsed * static_cast<double>(publishRateLimit),
                             static_cast<double>(publishRateLimit));
    if (publishTokens < 1.0) {
        return false;
    }
    publishTokens -= 1.0;
    return true;
}

bool Connection::resolveTopicAlias(const std::shared_ptr<PublishPacket> &publish) {
    // MQTT 3.1.1 has no properties, so no alias can be present. A PUBLISH
    // without a topic name is a protocol violation there too (MQTT-3.3.2-1),
    // but 3.1.1 has no reason codes: the connection is simply closed.
    if (frame.getVersion() != Version::MQTT5) {
        return true;
    }
    const bool emptyTopic = publish->topicName.empty();

    // MQTT-3.3.2-7: "It is a Protocol Error if the Topic Name is zero length and
    // there is no Topic Alias" -> DISCONNECT 0x82.
    if (publish->properties.find(PropertyID::TOPIC_ALIAS) == publish->properties.end()) {
        if (emptyTopic) {
            return failTopicAlias(ReasonCode::PROTOCOL_ERROR,
                                  "PUBLISH with an empty topic and no topic alias");
        }
        // With a topic name the packet is routed as-is.
        return true;
    }
    // parseProperties() guarantees a uint16_t for TOPIC_ALIAS.
    const uint16_t alias = std::get<uint16_t>(publish->properties[PropertyID::TOPIC_ALIAS]);

    // MQTT-3.3.2-8: an alias of 0, or one above what we advertised in CONNACK,
    // is a protocol error reported with Topic Alias invalid (0x94).
    if (alias == 0 || alias > TOPIC_ALIAS_MAXIMUM) {
        return failTopicAlias(ReasonCode::TOPIC_ALIAS_INVALID,
                              "invalid topic alias " + std::to_string(alias) + " (max " +
                                  std::to_string(TOPIC_ALIAS_MAXIMUM) + ")");
    }

    if (!emptyTopic) {
        // The topic name registers (or updates) the mapping for this alias
        // (§3.3.2.3.4 step 2b/3b) and the message is routed with it.
        topicAliases[alias] = publish->topicName;
        return true;
    }

    // Empty topic name: the alias must already be mapped. §3.3.2.3.4 step 3a:
    // an unmapped alias with a zero length Topic Name is a Protocol Error and
    // the receiver answers with DISCONNECT 0x82 (OASIS MQTT-474); 0x94 is
    // reserved for an alias number that is 0 or above our advertised maximum.
    auto mapping = topicAliases.find(alias);
    if (mapping == topicAliases.end()) {
        return failTopicAlias(ReasonCode::PROTOCOL_ERROR,
                              "unknown topic alias " + std::to_string(alias));
    }
    publish->topicName = mapping->second;
    return true;
}

bool Connection::failTopicAlias(ReasonCode reason, const std::string &detail) {
    logDebug("client %s: %s", sessionClientId.c_str(), detail.c_str());
    DisconnectPacket disconnect{reason};
    sendPacket(disconnect);
    state = State::DISCONNECTED;
    return false;
}

void Connection::handlePublish(std::shared_ptr<PublishPacket> publish) {
    auto liveSession = currentSession();
    if (!liveSession) {
        throw std::runtime_error("PUBLISH before CONNECT");
    }
    // Inbound rate limit: a publisher exceeding its allowance is disconnected
    // (MQTT 5 gets a DISCONNECT QUOTA_EXCEEDED; MQTT 3.1.1 is simply closed).
    if (!allowPublish()) {
        logDebug("client %s: publish rate limit (%zu/s) exceeded",
                 liveSession->getClientId().c_str(), publishRateLimit);
        // Recorded for $SYS/broker/disconnect/quota: the client was refused for
        // exceeding its publish rate, not for a protocol violation.
        disconnectReason = ReasonCode::QUOTA_EXCEEDED;
        if (frame.getVersion() == Version::MQTT5) {
            DisconnectPacket disconnect{ReasonCode::QUOTA_EXCEEDED};
            sendPacket(disconnect);
        }
        state = State::DISCONNECTED;
        return;
    }
    // MQTT 5 Topic Alias: expand an alias into its topic name (or register it)
    // before the message is routed, so the rest of the path only sees real
    // topics. An invalid alias closes the connection.
    if (!resolveTopicAlias(publish)) {
        return;
    }
    // MQTT-3.3.4-6: a PUBLISH sent from a Client to a Server MUST NOT contain a
    // Subscription Identifier. Echoing one would be meaningless (the server has
    // no subscription of its own) and accepting it lets a client claim its
    // message belongs to subscriptions it never made.
    if (frame.getVersion() == Version::MQTT5 &&
        !publish->subscriptionIdentifiers.empty()) {
        logDebug("client %s: inbound PUBLISH carries a Subscription Identifier",
                 liveSession->getClientId().c_str());
        DisconnectPacket disconnect{ReasonCode::PROTOCOL_ERROR};
        sendPacket(disconnect);
        state = State::DISCONNECTED;
        return;
    }
    // MQTT-3.3.2-14: the Response Topic is used as the Topic Name of a response
    // message, so it must be a legal Topic Name - no wildcard characters, and
    // not empty (§4.7.3 / MQTT-4.7.3-1). A violation is a Protocol Error
    // (MQTT-4.13.1-1): PUBACK/PUBREC define no reason code for it, so every QoS
    // is reported with DISCONNECT 0x82 rather than an ack that would be rejected
    // as an out-of-range reason code.
    if (frame.getVersion() == Version::MQTT5) {
        auto responseTopic = publish->properties.find(PropertyID::RESPONSE_TOPIC);
        if (responseTopic != publish->properties.end() &&
            std::holds_alternative<std::string>(responseTopic->second) &&
            !Topic::isValidTopicName(std::get<std::string>(responseTopic->second))) {
            logDebug("client %s: Response Topic %s is not a valid topic name",
                     liveSession->getClientId().c_str(),
                     std::get<std::string>(responseTopic->second).c_str());
            DisconnectPacket disconnect{ReasonCode::PROTOCOL_ERROR};
            sendPacket(disconnect);
            state = State::DISCONNECTED;
            return;
        }
    }
    // A PUBLISH topic must be a legal topic name: wildcards are never allowed
    // (MQTT-3.3.2-1). QoS 1/2 get the reason code in the ack; MQTT 5 QoS 0 is
    // reported with a DISCONNECT (MQTT-5.4.1-1), 3.1.1 is simply dropped.
    if (!Topic::isValidTopicName(publish->topicName)) {
        logDebug("client %s: invalid topic name %s", liveSession->getClientId().c_str(),
                 publish->topicName.c_str());
        if (publish->qos == QoS::QOS_1) {
            PubackPacket puback{publish->packetId, ReasonCode::TOPIC_NAME_INVALID};
            sendPacket(puback);
        } else if (publish->qos == QoS::QOS_2) {
            PubrecPacket pubrec{publish->packetId, ReasonCode::TOPIC_NAME_INVALID};
            sendPacket(pubrec);
        } else if (frame.getVersion() == Version::MQTT5) {
            DisconnectPacket disconnect{ReasonCode::TOPIC_NAME_INVALID};
            sendPacket(disconnect);
            state = State::DISCONNECTED;
        }
        return;
    }
    // Access control: a client may only publish to topics it is allowed to write
    if (auth && auth->isForbidden(liveSession->getUsername(), publish->topicName, /*write=*/true)) {
        logDebug("client %s: denied write on %s (acl)", liveSession->getClientId().c_str(),
                 publish->topicName.c_str());
        if (publish->qos == QoS::QOS_1) {
            PubackPacket puback{publish->packetId, ReasonCode::NOT_AUTHORIZED};
            sendPacket(puback);
        } else if (publish->qos == QoS::QOS_2) {
            PubrecPacket pubrec{publish->packetId, ReasonCode::NOT_AUTHORIZED};
            sendPacket(pubrec);
        } else if (frame.getVersion() == Version::MQTT5) {
            // MQTT 5: a QoS0 violation is reported with a DISCONNECT
            DisconnectPacket disconnect{ReasonCode::NOT_AUTHORIZED};
            sendPacket(disconnect);
            state = State::DISCONNECTED;
        }
        // MQTT 3.1.1 has no way to report a QoS0 rejection; silently drop it
        return;
    }
    // MQTT-3.3.2-5: a Payload Format Indicator of 1 promises the payload is
    // UTF-8 encoded character data. When it is not, QoS 1/2 are answered with
    // PAYLOAD_FORMAT_INVALID (0x99) and a QoS 0 MQTT 5 publish is reported with
    // a DISCONNECT carrying the same code.
    if (frame.getVersion() == Version::MQTT5) {
        auto pfi = publish->properties.find(PropertyID::PAYLOAD_FORMAT_INDICATOR);
        if (pfi != publish->properties.end() &&
            std::holds_alternative<uint8_t>(pfi->second) &&
            std::get<uint8_t>(pfi->second) == 1 &&
            !Utf8::isValid(publish->payload.data(), publish->payload.size())) {
            logDebug("client %s: PUBLISH claims UTF-8 payload but it is malformed",
                     liveSession->getClientId().c_str());
            if (publish->qos == QoS::QOS_1) {
                PubackPacket puback{publish->packetId, ReasonCode::PAYLOAD_FORMAT_INVALID};
                sendPacket(puback);
            } else if (publish->qos == QoS::QOS_2) {
                PubrecPacket pubrec{publish->packetId, ReasonCode::PAYLOAD_FORMAT_INVALID};
                sendPacket(pubrec);
            } else {
                DisconnectPacket disconnect{ReasonCode::PAYLOAD_FORMAT_INVALID};
                sendPacket(disconnect);
                state = State::DISCONNECTED;
            }
            return;
        }
    }
    Message message{publish->topicName, publish->payload, publish->qos, publish->retain};
    // MQTT 5 Message Expiry Interval (MQTT-3.3.2-9): record the absolute time at
    // which the message goes stale, so a slow or offline subscriber does not
    // receive it after it has expired.
    if (frame.getVersion() == Version::MQTT5) {
        auto it = publish->properties.find(PropertyID::MESSAGE_EXPIRY_INTERVAL);
        if (it != publish->properties.end() &&
            std::holds_alternative<uint32_t>(it->second)) {
            uint32_t interval = std::get<uint32_t>(it->second);
            message.hasExpiry = true;
            message.expiryDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(interval);
        }
        // MQTT-3.3.2-4: these properties belong to the application message and
        // are forwarded unchanged to every subscriber. Message Expiry is
        // excluded above (it becomes a deadline), and Topic Alias /
        // Subscription Identifier are per-connection, never forwarded.
        for (PropertyID forwardable : {PropertyID::PAYLOAD_FORMAT_INDICATOR,
                                       PropertyID::CONTENT_TYPE,
                                       PropertyID::RESPONSE_TOPIC,
                                       PropertyID::CORRELATION_DATA,
                                       PropertyID::USER_PROPERTY}) {
            auto it = publish->properties.find(forwardable);
            if (it != publish->properties.end()) {
                message.properties[forwardable] = it->second;
            }
        }
    }
    // $SYS/broker tree (MQTT-4.1.2-2): while the monitoring tree is enabled a
    // client publish to a $SYS topic is refused, so only the broker can write
    // it. With the feature off this is ordinary traffic and is stored/forwarded
    // as before.
    if (broker && broker->sysTopicsOn() && SysTopics::isSysTopic(publish->topicName)) {
        logDebug("client %s: refused publish to $SYS topic %s", liveSession->getClientId().c_str(),
                 publish->topicName.c_str());
        if (publish->qos == QoS::QOS_1) {
            PubackPacket puback{publish->packetId, ReasonCode::NOT_AUTHORIZED};
            sendPacket(puback);
        } else if (publish->qos == QoS::QOS_2) {
            PubrecPacket pubrec{publish->packetId, ReasonCode::NOT_AUTHORIZED};
            sendPacket(pubrec);
        } else if (frame.getVersion() == Version::MQTT5) {
            DisconnectPacket disconnect{ReasonCode::NOT_AUTHORIZED};
            sendPacket(disconnect);
            state = State::DISCONNECTED;
        }
        return;
    }
    if (broker) {
        broker->incrementMessagesReceived();
    }
    ReasonCode reason = liveSession->publish(publish->packetId, message, publish->dup);
    if (publish->qos == QoS::QOS_1) {
        PubackPacket puback{publish->packetId, reason};
        sendPacket(puback);
    } else if (publish->qos == QoS::QOS_2) {
        PubrecPacket pubrec{publish->packetId, reason};
        sendPacket(pubrec);
    }
}

void Connection::handlePuback(std::shared_ptr<PubackPacket> puback) {
    auto liveSession = currentSession();
    if (liveSession) {
        liveSession->puback(puback->packetId);
        // A freed window slot may have released a flow-controlled message
        liveSession->flush();
    }
}

void Connection::handlePubrec(std::shared_ptr<PubrecPacket> pubrec) {
    auto liveSession = currentSession();
    if (!liveSession) {
        return;
    }
    auto reason = liveSession->pubrec(pubrec->packetId, pubrec->reasonCode);
    if (!reason) {
        // The client rejected the PUBLISH (Reason Code >= 0x80). No PUBREL may
        // follow (MQTT-4.3.3) and the message is not retransmitted (MQTT-4.4.0-2);
        // the freed window slot may release a flow-controlled message.
        liveSession->flush();
        return;
    }
    PubrelPacket pubrel{pubrec->packetId, *reason};
    sendPacket(pubrel);
}

void Connection::handlePubrel(std::shared_ptr<PubrelPacket> pubrel) {
    auto liveSession = currentSession();
    if (!liveSession) {
        return;
    }
    ReasonCode reason = liveSession->pubrel(pubrel->packetId);
    PubcompPacket pubcomp{pubrel->packetId, reason};
    sendPacket(pubcomp);
}

void Connection::handlePubcomp(std::shared_ptr<PubcompPacket> pubcomp) {
    auto liveSession = currentSession();
    if (liveSession) {
        liveSession->pubcomp(pubcomp->packetId);
        // A freed window slot may have released a flow-controlled message
        liveSession->flush();
    }
}

void Connection::handleSubscribe(std::shared_ptr<SubscribePacket> subscribe) {
    auto liveSession = currentSession();
    if (!liveSession) {
        throw std::runtime_error("SUBSCRIBE before CONNECT");
    }
    // Sentinel meaning "this filter was accepted" in the per-filter outcome list.
    constexpr int acceptMarker = -1;
    // MQTT-3.8.3-3: the payload must hold at least one filter/options pair. A
    // SUBSCRIBE with an empty payload is a protocol error, not a no-op.
    if (subscribe->subscriptions.empty()) {
        logInfo("client %s: SUBSCRIBE with no topic filters", liveSession->getClientId().c_str());
        closeWithReason(ReasonCode::PROTOCOL_ERROR);
        return;
    }
    // Validate the subscription options before any of them is applied. A
    // Maximum QoS of 3, a Retain Handling of 3 and No Local on a shared
    // subscription are protocol errors (MQTT-3.8.3.1); reporting them as a
    // per-filter failure would leave the client believing it is subscribed.
    for (const auto &[filter, options] : subscribe->subscriptions) {
        const bool badQos = static_cast<uint8_t>(options.maximumQos) > 2;
        const bool badRetainHandling =
            static_cast<uint8_t>(options.retainHandling) > 2;
        const bool noLocalOnShared = options.noLocal && Topic::isShared(filter);
        if (badQos || badRetainHandling || noLocalOnShared) {
            logInfo("client %s: invalid subscription options for %s (qos=%d, retainHandling=%d, noLocal=%d)",
                    liveSession->getClientId().c_str(), filter.c_str(),
                    static_cast<int>(options.maximumQos),
                    static_cast<int>(options.retainHandling), (int)options.noLocal);
            closeWithReason(ReasonCode::PROTOCOL_ERROR);
            return;
        }
    }
    // Access control: subscribe only to filters the client may read. A denied
    // filter is reported in the SUBACK and is not registered. A session also
    // has a bounded number of subscriptions so one client cannot exhaust memory
    // by subscribing to an unbounded set of filters; filters beyond the limit
    // are reported with QUOTA_EXCEEDED.
    const bool isV5 = frame.getVersion() == Version::MQTT5;
    const size_t maxSubscriptions = liveSession->getMaxSubscriptions();
    const size_t maxSharedSubscriptions = liveSession->getMaxSharedSubscriptions();
    // Shared subscriptions are counted separately: a single "$share/g/f" entry
    // also registers a group member in the broker, so they have their own quota.
    size_t sharedCount = liveSession->getSharedSubscriptionCount();
    // Per-filter outcome: <0 means accepted (the granted QoS is recorded later),
    // otherwise the reason code reported for a rejected filter.
    std::vector<int> rejected(subscribe->subscriptions.size(), acceptMarker);
    // Per-filter "is this a brand-new subscription?" flag, captured before the
    // filter is registered. It drives the retained-message handling decision
    // (MQTT-3.3.1-9) and must reflect each filter independently, not the last
    // one subscribed.
    std::vector<bool> acceptedAsNew(subscribe->subscriptions.size(), false);
    for (size_t i = 0; i < subscribe->subscriptions.size(); ++i) {
        const auto& subscription = subscribe->subscriptions[i];
        // An empty filter is rejected before anything else (MQTT-3.8.3-1);
        // a filter that carries a misplaced wildcard is a protocol error.
        if (!Topic::isValid(subscription.first)) {
            logDebug("client %s: invalid topic filter %s", liveSession->getClientId().c_str(),
                     subscription.first.c_str());
            rejected[i] = isV5 ? static_cast<int>(ReasonCode::TOPIC_FILTER_INVALID) : 0x80;
            continue;
        }
        if (auth && auth->isForbiddenSubscribe(liveSession->getUsername(), subscription.first)) {
            logDebug("client %s: denied subscribe to %s (acl)", liveSession->getClientId().c_str(),
                     subscription.first.c_str());
            rejected[i] = isV5 ? static_cast<int>(ReasonCode::NOT_AUTHORIZED) : 0x80;
            continue;
        }
        const bool isNew = !liveSession->hasSubscription(subscription.first);
        acceptedAsNew[i] = isNew;
        if (isNew && Topic::isShared(subscription.first)) {
            if (sharedCount >= maxSharedSubscriptions) {
                logDebug("client %s: shared subscription quota (%zu) reached, rejecting %s",
                         liveSession->getClientId().c_str(), maxSharedSubscriptions,
                         subscription.first.c_str());
                rejected[i] = isV5 ? static_cast<int>(ReasonCode::QUOTA_EXCEEDED) : 0x80;
                continue;
            }
            ++sharedCount;
        } else if (isNew && liveSession->getSubscriptionCount() >= maxSubscriptions) {
            // New filter would push the session past its quota; a re-subscribe
            // of an existing filter is still allowed (it only updates options).
            logDebug("client %s: subscription quota (%zu) reached, rejecting %s",
                     liveSession->getClientId().c_str(), maxSubscriptions,
                     subscription.first.c_str());
            rejected[i] = isV5 ? static_cast<int>(ReasonCode::QUOTA_EXCEEDED) : 0x80;
            continue;
        }
        liveSession->subscribe(subscription.first, subscription.second);
    }

    SubackPacket suback{subscribe->packetId};
    // Grant the requested QoS, or report the per-filter failure otherwise
    for (size_t i = 0; i < subscribe->subscriptions.size(); ++i) {
        if (rejected[i] != acceptMarker) {
            suback.reasonCodes.push_back(static_cast<ReasonCode>(static_cast<uint8_t>(rejected[i])));
        } else {
            suback.reasonCodes.push_back(static_cast<ReasonCode>(
                static_cast<uint8_t>(subscribe->subscriptions[i].second.maximumQos)));
        }
    }
    sendPacket(suback);

    // Retained messages are delivered after the SUBACK (MQTT-3.3.1-9)
    for (size_t i = 0; i < subscribe->subscriptions.size(); ++i) {
        if (rejected[i] != acceptMarker) {
            continue;
        }
        const auto& subscription = subscribe->subscriptions[i];
        if (!Topic::isShared(subscription.first)) {
            liveSession->deliverRetained(subscription.first, subscription.second,
                                     acceptedAsNew[i]);
        }
    }
    // The queued retained messages are written here, with no lock held
    liveSession->flush();
}

void Connection::handleUnsubscribe(std::shared_ptr<UnsubscribePacket> unsubscribe) {
    auto liveSession = currentSession();
    if (!liveSession) {
        throw std::runtime_error("UNSUBSCRIBE before CONNECT");
    }
    // MQTT-3.10.3-2: an UNSUBSCRIBE with no filters is a protocol error.
    if (unsubscribe->topicFilters.empty()) {
        logInfo("client %s: UNSUBSCRIBE with no topic filters", liveSession->getClientId().c_str());
        closeWithReason(ReasonCode::PROTOCOL_ERROR);
        return;
    }
    // MQTT 5 UNSUBACK (MQTT-3.11.3): one reason code per topic filter, in the
    // order they appeared in the UNSUBSCRIBE. Success (0x00) when a subscription
    // existed and was removed, No Subscription Existed (0x11) otherwise. An
    // invalid Topic Filter is reported with TOPIC_FILTER_INVALID (0x8F).
    // MQTT 3.1.1 answers with a bare packet identifier and no payload.
    UnsubackPacket unsuback{unsubscribe->packetId};
    for (const auto& topic : unsubscribe->topicFilters) {
        if (!Topic::isValid(topic)) {
            if (frame.getVersion() == Version::MQTT5) {
                unsuback.reasonCodes.push_back(ReasonCode::TOPIC_FILTER_INVALID);
            }
            continue;
        }
        const bool removed = liveSession->unsubscribe(topic);
        if (frame.getVersion() == Version::MQTT5) {
            unsuback.reasonCodes.push_back(
                removed ? ReasonCode::SUCCESS : ReasonCode::NO_SUBSCRIPTION_EXISTED);
        }
    }
    sendPacket(unsuback);
}

void Connection::handlePingreq(std::shared_ptr<PingreqPacket> pingreq) {
    PingrespPacket pingResp;
    sendPacket(pingResp);
}

void Connection::handleDisconnect(std::shared_ptr<DisconnectPacket> disconnect) {
    // MQTT-3.1.2-8: the Will Message is deleted only by a DISCONNECT with Reason
    // Code 0x00 (Normal disconnection). Every other reason leaves it armed, and
    // 0x04 (Disconnect with Will Message) exists precisely so a client can leave
    // cleanly yet have its Will published. Keeping willPending set lets the
    // teardown below publish it (honouring the Will Delay Interval). MQTT 3.1.1
    // has no reason code, so a DISCONNECT there is always a normal one.
    const bool normalDisconnection =
        frame.getVersion() != Version::MQTT5 ||
        disconnect->reasonCode == ReasonCode::NORMAL_DISCONNECTION;
    if (normalDisconnection) {
        willPending = false;
    }
    // MQTT 5 Session Expiry Interval on DISCONNECT (MQTT-3.14.4-2): it overrides
    // the CONNECT value for this teardown. When the property is absent the CONNECT
    // value stays in effect; an explicit 0 ends the session immediately.
    if (frame.getVersion() == Version::MQTT5) {
        auto it = disconnect->properties.find(PropertyID::SESSION_EXPIRY_INTERVAL);
        if (it != disconnect->properties.end() &&
            std::holds_alternative<uint32_t>(it->second)) {
            const uint32_t interval = std::get<uint32_t>(it->second);
            // §3.14.2.2.2: "If the Session Expiry Interval in the CONNECT packet
            // was zero, then it is a Protocol Error to set a non-zero Session
            // Expiry Interval in the DISCONNECT packet sent by the Client."
            // Honouring it would let a client turn a live-only connection into a
            // durable session at the last moment, so the request is refused with
            // DISCONNECT 0x82 (§4.13) instead.
            if (connectSessionExpiry == 0 && interval != 0) {
                logInfo("client %s: DISCONNECT raised a zero Session Expiry Interval to %u",
                        sessionClientId.c_str(), interval);
                closeWithReason(ReasonCode::PROTOCOL_ERROR);
                return;
            }
            sessionExpirySeconds = interval;
        }
    }
    state = State::DISCONNECTED;
}

void Connection::handleAuth(std::shared_ptr<AuthPacket> auth) {
    // The broker implements only username/password authentication and always
    // refuses a CONNECT that carries an Authentication Method, so no enhanced
    // authentication can be in progress on this connection. [MQTT-4.12.0-7]
    // then forbids the Client from sending AUTH at all, and an AUTH that was
    // never expected is a Protocol Error, answered with DISCONNECT 0x82
    // (§4.13.1). Answering with an AUTH packet is wrong twice over: a Server
    // sends AUTH only inside a running enhanced-authentication exchange, and
    // AUTH's Reason Code may only be 0x00, 0x18 or 0x19 (MQTT-3.15.2-1), so the
    // 0x8C that used to be sent here was itself a protocol violation.
    (void)auth;
    logDebug("client %s: AUTH without an enhanced authentication method",
             sessionClientId.c_str());
    closeWithReason(ReasonCode::PROTOCOL_ERROR);
}

bool Connection::deliver(const Message &message, uint16_t packetId, QoS qos,
                         const SubscriptionIdentifiers &subscriptionIdentifiers) {
    return handleDeliver(message, packetId, qos, subscriptionIdentifiers);
}

bool Connection::handleDeliver(const Message &message, uint16_t packetId, QoS qos,
                               const SubscriptionIdentifiers &subscriptionIdentifiers) {
    if (state != State::CONNECTED) {
        return false;
    }
    PublishPacket publish{message.topic, message.payload, qos, message.retain};
    publish.packetId = packetId;
    publish.dup = message.dup;
    // MQTT 3.1.1 has no property section, so 3.1.1 deliveries carry none.
    if (frame.getVersion() == Version::MQTT5) {
        // MQTT-3.3.2-4: repeat the properties the publisher attached.
        for (const auto &[id, value] : message.properties) {
            publish.properties[id] = value;
        }
        // MQTT-3.3.2-9: the expiry interval is forwarded as the time that is
        // still left, not as the value the publisher wrote.
        if (message.hasExpiry) {
            const auto now = std::chrono::steady_clock::now();
            if (now >= message.expiryDeadline) {
                logDebug("dropping an expired delivery on %s", message.topic.c_str());
                return false;
            }
            const auto left = std::chrono::duration_cast<std::chrono::seconds>(
                                  message.expiryDeadline - now).count();
            publish.properties[PropertyID::MESSAGE_EXPIRY_INTERVAL] =
                static_cast<uint32_t>(left);
        }
        // MQTT-3.3.4-3/-4: echo the identifier of every subscription that caused
        // this copy. Overlapping subscriptions produce one copy, so the list can
        // hold several (and repeats) and must be sent as several properties.
        publish.subscriptionIdentifiers = subscriptionIdentifiers;
    }
    logDebug("deliver %s qos=%d packetId=%u dup=%d", message.topic.c_str(), (int)qos, packetId,
             (int)message.dup);
    // sendPacket() reports whether the packet actually went out; a packet over
    // the client's Maximum Packet Size is refused there (MQTT-3.1.2-24) and the
    // session must release the inflight slot in that case.
    return sendPacket(publish);
}

void Connection::sendPubrec(uint16_t packetId) {
    if (state != State::CONNECTED) {
        return;
    }
    // The original PUBREC always carried SUCCESS (only successful inbound QoS 2
    // flows are remembered), so the resend does too.
    PubrecPacket pubrec{packetId, ReasonCode::SUCCESS};
    logDebug("resend PUBREC packetId=%u (awaiting PUBREL)", packetId);
    sendPacket(pubrec);
}

void Connection::sendPubrel(uint16_t packetId) {
    if (state != State::CONNECTED) {
        return;
    }
    // A re-sent PUBREL always carries SUCCESS (the PUBREC that opened this leg
    // was accepted), so the client can finish the flow with PUBCOMP.
    // PUBREL has no DUP flag: its fixed-header bits are reserved and MUST be
    // 0010 (MQTT-3.6.1-1); §4.4 resends the PUBREL with its original Packet
    // Identifier but never grants it a DUP bit, so dup stays false on the wire.
    PubrelPacket pubrel{packetId, ReasonCode::SUCCESS};
    logDebug("resend PUBREL packetId=%u (awaiting PUBCOMP)", packetId);
    sendPacket(pubrel);
}

void Connection::closeWithReason(ReasonCode reason) {
    // Record why the connection is ending so teardown counts it once into the
    // right $SYS/broker/disconnect/* category. closeWithReason is the single
    // choke point for every broker-initiated abnormal close (protocol error,
    // keep-alive timeout, oversized packet, ...), so setting it here covers them
    // all without touching each call site.
    disconnectReason = reason;
    // MQTT 5 clients are told why the connection ends (MQTT-4.13). MQTT 3.1.1
    // has no DISCONNECT packet from the server, so the socket simply closes.
    if (frame.getVersion() == Version::MQTT5) {
        if (state == State::CONNECTED) {
            DisconnectPacket disconnect{reason};
            sendPacket(disconnect);
        } else if (state == State::IDLE && !connackSent) {
            // MQTT-3.2.2-2: a CONNECT the server cannot accept is answered with a
            // CONNACK carrying the reason code, not with a DISCONNECT (the client
            // is not in a session yet, so it could not interpret one).
            ConnackPacket connack{false, reason};
            connackSent = true;
            sendPacket(connack);
        }
    }
    state = State::DISCONNECTED;
}

bool Connection::sendPacket(Packet &packet) {
    std::lock_guard<std::recursive_mutex> lock(writeMutex);
    // Not sockfd-based: a QUIC connection has no descriptor. A closed or dead
    // transport reports failure through writeAll below.
    if (!transport) {
        return false;
    }
    auto data = frame.serialize(packet);
    // MQTT-3.1.2-24: never exceed the client's Maximum Packet Size. A packet
    // that would is dropped here; sending it would make the client close the
    // connection, losing every later delivery as well. The caller is told so a
    // QoS>0 delivery can release the inflight slot it reserved.
    if (clientMaxPacketSize != 0 && data.size() > clientMaxPacketSize) {
        logDebug("dropping a %zu-byte packet: the client accepts at most %zu bytes",
                 data.size(), clientMaxPacketSize);
        return false;
    }
    if (writeBufferActive) {
        syncTransportWriteBytesLocked();
        // Once a unit is queued, every later unit must join the tail. Writing a
        // new packet directly here would let it overtake the unsent prefix and
        // corrupt the ordered MQTT byte stream.
        ssize_t n = 0;
        if (writeBuffer.empty()) {
            n = transport->writeSome(data.data(), data.size());
            syncTransportWriteBytesLocked();
            if (n < 0) {
                state = State::DISCONNECTED; // peer gone
                return false;
            }
            if (static_cast<size_t>(n) == data.size()) {
                return true;
            }
        }
        // Would-block, or an earlier unit is still queued: buffer this whole unit
        // for the read loop to drain from the front on POLLOUT.
        const size_t remaining = data.size() - static_cast<size_t>(n);
        if (writeBufferBytes > writeBufferLimit ||
            remaining > writeBufferLimit - writeBufferBytes) {
            // The buffer is full: this reader is genuinely too slow to ever catch
            // up, not just transiently behind. Drop the connection (the session
            // and its inflight state are torn down in run()).
            logInfo("connection closed: outbound write buffer full (slow reader)");
            state = State::DISCONNECTED;
            return false;
        }
        const bool firstBuffered = writeBuffer.empty();
        writeBuffer.emplace_back(data.begin() + n, data.end());
        writePending.store(true, std::memory_order_release);
        const size_t added = data.size() - static_cast<size_t>(n);
        writeBufferBytes += added;
        if (broker) {
            broker->adjustWriteBufferBytes(static_cast<int64_t>(added));
        }
        if (firstBuffered || n > 0) {
            lastWriteProgress = std::chrono::steady_clock::now();
        }
        return true;
    }
    if (!transport->writeAll(data.data(), data.size())) {
        // The write failed or timed out (a stalled TLS write is bounded by
        // SO_SNDTIMEO, see SecureTransport's constructor): the peer is gone or
        // never reads. Stop the connection on the next loop iteration; run()
        // tears the socket down and publishes the Will.
        logDebug("outbound write failed; closing connection after %zu-byte packet",
                 data.size());
        // Mark it as a write-stall drop so teardown sends a RST, not a graceful
        // FIN: the send buffer still holds undeliverable data and a plain
        // close() would leave the FIN stuck behind the peer's closed window, so
        // the stalled reader would never see EOF (see writeStallDrop).
        writeStallDrop = true;
        state = State::DISCONNECTED;
        return false;
    }
    return true;
}

void Connection::drainWriteBuffer() {
    std::unique_lock<std::recursive_mutex> lock(writeMutex);
    syncTransportWriteBytesLocked();
    if (writeBuffer.empty()) {
        return;
    }
    // Drop a peer whose buffer has made no progress for longer than the write
    // stall bound: it is gone, not merely slow.
    const auto now = std::chrono::steady_clock::now();
    const auto stalledFor = std::chrono::duration_cast<std::chrono::milliseconds>(
                                now - lastWriteProgress).count();
    if (stalledFor > writeTimeoutMs) {
        logInfo("connection closed: outbound write stalled %lld ms (slow or dead reader)",
                (long long)stalledFor);
        state = State::DISCONNECTED;
        return;
    }
    while (!writeBuffer.empty()) {
        auto &front = writeBuffer.front();
        const ssize_t n = transport->writeSome(front.data(), front.size());
        syncTransportWriteBytesLocked();
        if (n < 0) {
            state = State::DISCONNECTED;
            return;
        }
        if (static_cast<size_t>(n) < front.size()) {
            // Partial (would-block): keep the remainder buffered and wait for the
            // next POLLOUT. Update the progress marker only when bytes moved.
            if (n > 0) {
                front.erase(front.begin(), front.begin() + n);
                writeBufferBytes -= static_cast<size_t>(n);
                if (broker) {
                    broker->adjustWriteBufferBytes(-static_cast<int64_t>(n));
                }
                lastWriteProgress = std::chrono::steady_clock::now();
            }
            break;
        }
        const size_t done = front.size();
        writeBufferBytes -= done;
        if (broker) {
            broker->adjustWriteBufferBytes(-static_cast<int64_t>(done));
        }
        writeBuffer.pop_front();
        lastWriteProgress = std::chrono::steady_clock::now();
    }
    if (writeBuffer.empty()) {
        writePending.store(false, std::memory_order_release);
    }
    // Fully drained: let the session push anything it had to hold back. Session
    // delivery can call back into sendPacket(), so never hold writeMutex across
    // the Session -> Connection lock direction.
    if (writeBuffer.empty() && transportWriteBytes == 0) {
        auto liveSession = currentSession();
        lock.unlock();
        if (liveSession) {
            liveSession->flush();
        }
    }
}

bool Connection::writeBufferPending() const {
    return writePending.load(std::memory_order_acquire) ||
           (transport && transport->pendingWriteBytes() > 0);
}

void Connection::syncTransportWriteBytesLocked() {
    const size_t actual = transport ? transport->pendingWriteBytes() : 0;
    if (actual > transportWriteBytes) {
        const size_t added = actual - transportWriteBytes;
        writeBufferBytes += added;
        if (broker) {
            broker->adjustWriteBufferBytes(static_cast<int64_t>(added));
        }
    } else if (actual < transportWriteBytes) {
        const size_t removed = transportWriteBytes - actual;
        writeBufferBytes -= removed;
        if (broker) {
            broker->adjustWriteBufferBytes(-static_cast<int64_t>(removed));
        }
        lastWriteProgress = std::chrono::steady_clock::now();
    }
    transportWriteBytes = actual;
}

} // namespace MQTT
