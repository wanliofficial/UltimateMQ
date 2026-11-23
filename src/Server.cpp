#include "Topic.h"
#include "Server.h"
#include "Connection.h"
#include "Env.h"
#include "WebSocket.h"
#include <csignal>
#if defined(FLOWMQ_WITH_QUIC)
#include "QUIC.h"
#endif
#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <string>
#include <limits>
#include <cerrno>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include <thread>
#include "Log.h"

namespace MQTT {

namespace {
//! Self-pipe used by the signal handler; writing to it wakes accept()
int gWakePipe[2] = {-1, -1};

void handleSignal(int) {
    // Only async-signal-safe calls here
    char byte = 1;
    ssize_t ignored = ::write(gWakePipe[1], &byte, 1);
    (void)ignored;
}
} // namespace

Server::Server(int port, size_t maxConnections)
    : maxConnections(maxConnections == 0 ? 1024 : maxConnections) {
    // The primary MQTT port comes from atoi() in main(), which silently yields 0
    // for a non-numeric value and lets a >65535 value wrap through htons(). Every
    // other listener port is range-checked below; do the same here so a mistyped
    // MQTT_PORT fails the startup loudly instead of binding to an unintended
    // port (0 asks the OS for an ephemeral one).
    if (port <= 0 || port > 65535) {
        throw std::runtime_error("invalid MQTT port: " + std::to_string(port));
    }
    // Cap the number of simultaneous client connections. Each holds a thread
    // and a socket, so an unbounded accept loop is a resource-exhaustion lever.
    // The constructor argument (or 1024) is the default; FLOWMQ_MAX_CONNECTIONS
    // overrides it so a deployment can fit its thread/memory budget.
    const char* maxConnectionsEnv = getenv("FLOWMQ_MAX_CONNECTIONS");
    if (maxConnectionsEnv && *maxConnectionsEnv) {
        unsigned long long parsed;
        if (!Env::parseLimit(maxConnectionsEnv,
                             std::numeric_limits<size_t>::max(), parsed)) {
            throw std::runtime_error(std::string("invalid FLOWMQ_MAX_CONNECTIONS: ") +
                                     maxConnectionsEnv);
        }
        this->maxConnections = static_cast<size_t>(parsed);
    }
    listener = std::make_unique<Listener>(port);
    auth = std::make_unique<Auth>();

    // MQTT over WebSocket (opt-in). Without FLOWMQ_WEBSOCKET_PORT the server
    // opens no extra listener and behaves exactly as before; with it, MQTT
    // packets ride in WebSocket binary frames on that port (plain WS, or WSS
    // when a TLS certificate is configured).
    const char* websocketPort = getenv("FLOWMQ_WEBSOCKET_PORT");
    if (websocketPort && *websocketPort) {
        char* end = nullptr;
        long parsed = std::strtol(websocketPort, &end, 10);
        if (end == websocketPort || *end != '\0' || parsed <= 0 || parsed > 65535) {
            throw std::runtime_error(std::string("invalid FLOWMQ_WEBSOCKET_PORT: ") +
                                     websocketPort);
        }
        websocketListener = std::make_unique<Listener>(static_cast<int>(parsed));
        logInfo("websocket listener on port %d", static_cast<int>(parsed));
    }

    std::string error;
    const char* passwordFile = getenv("FLOWMQ_PASSWORD_FILE");
    if (passwordFile && *passwordFile) {
        if (!auth->loadUsers(passwordFile, &error)) {
            throw std::runtime_error("failed to load password file: " + error);
        }
    }
    const char* aclFile = getenv("FLOWMQ_ACL_FILE");
    if (aclFile && *aclFile) {
        if (!auth->loadAcl(aclFile, &error)) {
            throw std::runtime_error("failed to load acl file: " + error);
        }
    }
    logInfo("authentication %s, acl %s", auth->authRequired() ? "enabled" : "disabled",
            auth->aclEnabled() ? "enabled" : "disabled");
    if (auth->aclEnabled() && !auth->authRequired()) {
        logError("acl is enabled without authentication: clients can claim any username");
    }

    // Transport security (opt-in). Without a certificate the listener speaks
    // plaintext, exactly as before; with one it speaks TLS and every credential
    // and payload is encrypted on the wire.
    const char* tlsCert = getenv("FLOWMQ_TLS_CERT");
    const char* tlsKey = getenv("FLOWMQ_TLS_KEY");
    bool tlsVerifyClients = Env::isEnabled("FLOWMQ_TLS_VERIFY_CLIENTS");
    const char* tlsCa = getenv("FLOWMQ_TLS_CA");
    std::string tlsError;
    TLS = TLSContext::load(tlsCert ? tlsCert : "", tlsKey ? tlsKey : "",
                           tlsVerifyClients, tlsCa ? tlsCa : "", &tlsError);
    if (!tlsError.empty()) {
        throw std::runtime_error("failed to configure TLS: " + tlsError);
    }
    logInfo("transport %s", TLS && TLS->enabled() ? "TLS" : "plaintext");
    if (TLS && TLS->enabled() && !auth->authRequired()) {
        logError("TLS is enabled without authentication: the connection is encrypted but the client is anonymous");
    }

    // Durable state (opt-in). The store is created before the broker so that a
    // corrupted state file aborts startup instead of silently losing sessions.
    const char* stateFile = getenv("FLOWMQ_STATE_FILE");
    if (stateFile && *stateFile) {
        store = std::make_unique<Store>(stateFile, std::chrono::milliseconds(1000));
        logInfo("state file: %s", stateFile);
    } else {
        store = std::make_unique<Store>(std::string(), std::chrono::milliseconds(1000));
    }

    broker = new Broker(auth.get(), store.get());

    // MQTT-SN gateway (opt-in). Without FLOWMQ_MQTT_SN_PORT no UDP socket is
    // opened and the broker behaves exactly as before; with it, MQTT-SN 1.2
    // clients are served on that UDP port and share sessions with MQTT clients.
    const char* mqttSnPort = getenv("FLOWMQ_MQTT_SN_PORT");
    if (mqttSnPort && *mqttSnPort) {
        char* end = nullptr;
        long parsed = std::strtol(mqttSnPort, &end, 10);
        if (end == mqttSnPort || *end != '\0' || parsed <= 0 || parsed > 65535) {
            throw std::runtime_error(std::string("invalid FLOWMQ_MQTT_SN_PORT: ") +
                                     mqttSnPort);
        }
        // Predefined topic ids (opt-in): "1=sensors/temp,2=home/#"
        std::map<uint16_t, std::string> predefinedTopics;
        const char* predefined = getenv("FLOWMQ_MQTT_SN_PREDEFINED_TOPICS");
        if (predefined && *predefined) {
            const std::string spec(predefined);
            size_t start = 0;
            while (start < spec.size()) {
                size_t comma = spec.find(',', start);
                size_t endPos = comma == std::string::npos ? spec.size() : comma;
                std::string entry = spec.substr(start, endPos - start);
                size_t equals = entry.find('=');
                if (equals != std::string::npos) {
                    unsigned long id = std::strtoul(entry.substr(0, equals).c_str(),
                                                    nullptr, 10);
                    if (id > 0 && id <= 65535) {
                        predefinedTopics[static_cast<uint16_t>(id)] =
                            entry.substr(equals + 1);
                    }
                }
                if (comma == std::string::npos) {
                    break;
                }
                start = comma + 1;
            }
            logInfo("MQTT-SN predefined topics: %zu", predefinedTopics.size());
        }
        mqttSnGateway = std::make_unique<MQTTSN::Gateway>(broker,
                                                          static_cast<uint16_t>(parsed),
                                                          predefinedTopics);
        logInfo("mqtt-sn gateway on UDP port %d", static_cast<int>(parsed));
    }

#if defined(FLOWMQ_WITH_QUIC)
    // MQTT over QUIC (opt-in). Without FLOWMQ_QUIC_PORT no UDP socket is
    // opened; with it, QUIC clients speak MQTT on their first stream.
    const char* quicPort = getenv("FLOWMQ_QUIC_PORT");
    if (quicPort && *quicPort) {
        char* end = nullptr;
        long parsed = std::strtol(quicPort, &end, 10);
        if (end == quicPort || *end != '\0' || parsed <= 0 || parsed > 65535) {
            throw std::runtime_error(std::string("invalid FLOWMQ_QUIC_PORT: ") +
                                     quicPort);
        }
        quicListener = std::make_unique<QUICListener>(broker, static_cast<uint16_t>(parsed),
                                                      maxConnections);
        logInfo("quic listener on UDP port %d", static_cast<int>(parsed));
    }
#endif


    // Maximum packet size (opt-in). Bounds per-connection buffering so a client
    // cannot stall the broker with a huge packet. The default keeps historical
    // behaviour; only raise it when the deployment needs large payloads.
    const char* maxPacketSize = getenv("FLOWMQ_MAX_PACKET_SIZE");
    if (maxPacketSize && *maxPacketSize) {
        unsigned long long parsed;
        if (!Env::parseLimit(maxPacketSize, std::numeric_limits<size_t>::max(), parsed)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(std::string("invalid FLOWMQ_MAX_PACKET_SIZE: ") +
                                     maxPacketSize);
        }
        broker->setMaxPacketSize(static_cast<size_t>(parsed));
        logInfo("max packet size: %zu bytes", broker->getMaxPacketSize());
    }

    // Stalled-write timeout (opt-in). A client that stops reading would
    // otherwise hold its connection thread forever on a blocking socket; bound
    // how long a write may stall before the connection is dropped.
    const char* writeTimeout = getenv("FLOWMQ_WRITE_TIMEOUT_MS");
    if (writeTimeout && *writeTimeout) {
        char* end = nullptr;
        long parsed = std::strtol(writeTimeout, &end, 10);
        // Reject anything that does not fit the int the broker stores: a value
        // above INT_MAX would pass the > 0 check yet truncate to a bogus (often
        // negative) int, silently disabling the stall bound instead of bounding
        // it. The write timeout is a millisecond count a few hours at most, so
        // an out-of-range value is a configuration error worth failing on.
        if (end == writeTimeout || *end != '\0' || parsed <= 0 ||
            parsed > std::numeric_limits<int>::max()) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(std::string("invalid FLOWMQ_WRITE_TIMEOUT_MS: ") +
                                     writeTimeout);
        }
        broker->setWriteTimeoutMs(static_cast<int>(parsed));
        logInfo("write stall timeout: %d ms", broker->getWriteTimeoutMs());
    }

    // Per-connection outbound write buffer (opt-in). When > 0, a write the socket
    // cannot take immediately (a transiently slow reader) is buffered as whole
    // transport write-units and drained on POLLOUT, so the connection thread is
    // not stalled and the client is not dropped for being briefly behind. 0 (the
    // default) keeps the historical blocking-write + stall-timeout path exactly.
    // Must be set before any client connects.
    const char* writeBuffer = getenv("FLOWMQ_WRITE_BUFFER");
    if (writeBuffer && *writeBuffer) {
        unsigned long long parsed;
        if (!Env::parseLimit(writeBuffer, std::numeric_limits<size_t>::max(), parsed)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(std::string("invalid FLOWMQ_WRITE_BUFFER: ") +
                                     writeBuffer);
        }
        broker->setWriteBufferBytes(static_cast<size_t>(parsed));
        logInfo("outbound write buffer: %zu bytes", broker->getWriteBufferBytes());
    }

    // Per-session subscription limit: bounds how many topic filters one client
    // may register, so it cannot exhaust memory with an unbounded set.
    const char* maxSubscriptions = getenv("FLOWMQ_MAX_SUBSCRIPTIONS");
    if (maxSubscriptions && *maxSubscriptions) {
        unsigned long long parsed;
        if (!Env::parseLimit(maxSubscriptions, std::numeric_limits<size_t>::max(), parsed)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(std::string("invalid FLOWMQ_MAX_SUBSCRIPTIONS: ") +
                                     maxSubscriptions);
        }
        broker->setMaxSubscriptions(static_cast<size_t>(parsed));
        logInfo("max subscriptions per session: %zu", broker->getMaxSubscriptions());
    }

    // Shared subscription limit (opt-in). Shared subscriptions are counted
    // separately from ordinary filters because each one also registers a group
    // member in the broker. Defaults to the subscription limit.
    const char* maxSharedSubscriptions = getenv("FLOWMQ_MAX_SHARED_SUBSCRIPTIONS");
    if (maxSharedSubscriptions && *maxSharedSubscriptions) {
        unsigned long long parsed;
        if (!Env::parseLimit(maxSharedSubscriptions,
                             std::numeric_limits<size_t>::max(), parsed)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(
                std::string("invalid FLOWMQ_MAX_SHARED_SUBSCRIPTIONS: ") +
                maxSharedSubscriptions);
        }
        broker->setMaxSharedSubscriptions(static_cast<size_t>(parsed));
        logInfo("max shared subscriptions per session: %zu",
                broker->getMaxSharedSubscriptions());
    }

    // Retained-message cap (opt-in). Bounds how many distinct topics may hold a
    // retained message, so a client cannot exhaust broker memory by retaining an
    // unbounded stream of topics. 0 (the default) leaves the historical
    // unlimited behaviour unchanged.
    const char* maxRetained = getenv("FLOWMQ_MAX_RETAINED");
    if (maxRetained && *maxRetained) {
        unsigned long long parsed;
        if (!Env::parseLimit(maxRetained, std::numeric_limits<size_t>::max(), parsed)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(std::string("invalid FLOWMQ_MAX_RETAINED: ") +
                                     maxRetained);
        }
        broker->setMaxRetained(static_cast<size_t>(parsed));
        logInfo("max retained messages: %zu", broker->getMaxRetained());
    }

    // Per-client inbound PUBLISH rate limit (opt-in). Bounds how many messages
    // one publisher may inject per second, so a single client cannot saturate
    // the broker's fan-out path with an unbounded burst.
    const char* maxPublishRate = getenv("FLOWMQ_MAX_PUBLISH_RATE");
    if (maxPublishRate && *maxPublishRate) {
        unsigned long long parsed;
        if (!Env::parseLimit(maxPublishRate, std::numeric_limits<size_t>::max(), parsed)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(std::string("invalid FLOWMQ_MAX_PUBLISH_RATE: ") +
                                     maxPublishRate);
        }
        broker->setMaxPublishRate(static_cast<size_t>(parsed));
        logInfo("max publish rate per client: %zu msg/s", broker->getMaxPublishRate());
    }

    // Session delivery-queue backpressure policy (opt-in). When a session's
    // delivery queue is full the default drops the incoming message
    // (FLOWMQ_QUEUE_DROP=newest, the historical behaviour). Setting
    // FLOWMQ_QUEUE_DROP=oldest instead evicts the oldest buffered message so the
    // client always sees the most recent traffic, for real-time feeds that must
    // not be buried in stale messages behind a stalled reader.
    const char* queueDrop = getenv("FLOWMQ_QUEUE_DROP");
    if (queueDrop && *queueDrop) {
        std::string value(queueDrop);
        if (value == "oldest") {
            broker->setQueueDropPolicy(QueueDropPolicy::DropOldest);
            logInfo("session queue drop policy: oldest (keep newest under backpressure)");
        } else if (value == "newest" || value == "0" || value == "false") {
            broker->setQueueDropPolicy(QueueDropPolicy::DropNewest);
            logInfo("session queue drop policy: newest (default)");
        } else {
            delete broker;
            broker = nullptr;
            throw std::runtime_error("invalid FLOWMQ_QUEUE_DROP: " + value +
                                     " (expected 'newest' or 'oldest')");
        }
    }

    // Preferred keep alive (opt-in). Without it a client may ask for 0 (no
    // keep alive) and its dead connection would never be reaped. When set,
    // MQTT 5 clients that ask for more (or for none) are told the broker's
    // value through the Server Keep Alive property.
    const char* serverKeepAlive = getenv("FLOWMQ_SERVER_KEEP_ALIVE");
    if (serverKeepAlive && *serverKeepAlive) {
        unsigned long long parsed;
        if (!Env::parseLimit(serverKeepAlive, 65535, parsed)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error(std::string("invalid FLOWMQ_SERVER_KEEP_ALIVE: ") +
                                     serverKeepAlive);
        }
        broker->setServerKeepAlive(static_cast<uint16_t>(parsed));
        logInfo("server keep alive: %u s", broker->getServerKeepAlive());
    }

    // Optional $SYS/broker monitoring tree (opt-in). Off by default: with it off
    // the broker exposes no $SYS topic and a client publish to "$SYS/..." is just
    // ordinary traffic. When on, the broker publishes its own counters as
    // retained QoS 0 messages and refuses client writes to the tree
    // (MQTT-4.1.2-2).
    const char* sysTopics = getenv("FLOWMQ_SYS_TOPICS");
    if (sysTopics && *sysTopics && std::string(sysTopics) != "0" && std::string(sysTopics) != "false") {
        broker->setSysTopics(true);
        broker->refreshSysTopics();
        logInfo("$SYS/broker monitoring tree enabled");
    }

    if (store->enabled()) {
        PersistedState persisted;
        std::string loadError;
        if (!store->load(persisted, &loadError)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error("failed to load state file: " + loadError);
        }
        broker->restoreState(persisted);
    }
}

Server::~Server() {
    stop();
    shutdownPersistence();
    delete broker;
    broker = nullptr;
}

void Server::start() {
    listener->start();
    if (websocketListener) {
        websocketListener->start();
    }
    if (mqttSnGateway) {
        mqttSnGateway->start();
    }
#if defined(FLOWMQ_WITH_QUIC)
    if (quicListener) {
        quicListener->start();
    }
#endif
    // Start the background writer only once the state has been restored
    store->start([this]() { return broker->snapshotState(); });
    running = true;
    logInfo("MQTT server started (max connections: %zu)", getMaxConnections());
    run();
}

void Server::installSignalHandlers() {
    if (::pipe(gWakePipe) != 0) {
        logError("cannot create signal pipe: %s", std::strerror(errno));
        return;
    }
    // Non-blocking ends so a handler can never stall the process
    for (int fd : gWakePipe) {
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    struct sigaction action {};
    action.sa_handler = handleSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_RESTART;
    ::sigaction(SIGINT, &action, nullptr);
    ::sigaction(SIGTERM, &action, nullptr);
}

void Server::run() {
    while (running) {
        // Join the threads of clients that have gone away. Threads are not
        // detached, so their resources (and the last reference to their
        // Connection) are only released when they are joined.
        reapFinishedClientThreads();
        // Drop any sessions whose disconnect-time expiry has elapsed. Bounding
        // the poll timeout guarantees the loop wakes to run this even when idle.
        if (std::chrono::steady_clock::now() - lastSweep >= sweepInterval) {
            if (broker) {
                broker->sweepExpiredSessions();
            }
            // The MQTT-SN gateway reaps timed-out UDP peers and broadcasts
            // ADVERTISE on the same cadence.
            if (mqttSnGateway) {
                mqttSnGateway->tick();
            }
            lastSweep = std::chrono::steady_clock::now();
        }
        // Wait for either a new client, the periodic wake-up, the shutdown
        // wake-up pipe, or a pending accept-backoff deadline.
        const auto now = std::chrono::steady_clock::now();
        struct pollfd fds[4];
        int count = 0;
        // A listener whose accept() failed with EMFILE/ENFILE stays readable
        // (its connection is still queued), so polling it for POLLIN would spin.
        // Drop the POLLIN mask until its backoff deadline and let the timeout
        // below wake the loop to retry once fds may have freed up.
        const bool listenerBackingOff =
            listenerBackoffUntil_.time_since_epoch().count() != 0 && now < listenerBackoffUntil_;
        fds[count].fd = listener->fd();
        fds[count].events = listenerBackingOff ? 0 : POLLIN;
        ++count;
        int websocketFdIndex = -1;
        if (websocketListener) {
            websocketFdIndex = count;
            const bool backingOff =
                websocketBackoffUntil_.time_since_epoch().count() != 0 &&
                now < websocketBackoffUntil_;
            fds[count].fd = websocketListener->fd();
            fds[count].events = backingOff ? 0 : POLLIN;
            ++count;
        }
        int mqttSnFdIndex = -1;
        if (mqttSnGateway) {
            mqttSnFdIndex = count;
            fds[count].fd = mqttSnGateway->fd();
            fds[count].events = POLLIN;
            ++count;
        }
        if (gWakePipe[0] >= 0) {
            fds[count].fd = gWakePipe[0];
            fds[count].events = POLLIN;
            ++count;
        }
        // Wake at most once per sweep interval so an idle loop still sweeps, but
        // no later than the nearest accept-backoff deadline so a throttled
        // listener is retried promptly once fds may have recovered.
        auto wakeAt = lastSweep + sweepInterval;
        if (listenerBackoffUntil_.time_since_epoch().count() != 0 &&
            listenerBackoffUntil_ < wakeAt) {
            wakeAt = listenerBackoffUntil_;
        }
        if (websocketBackoffUntil_.time_since_epoch().count() != 0 &&
            websocketBackoffUntil_ < wakeAt) {
            wakeAt = websocketBackoffUntil_;
        }
        int pollTimeoutMs = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(wakeAt - now).count());
        if (pollTimeoutMs < 0) {
            pollTimeoutMs = 0;
        }

        int ready = ::poll(fds, count, pollTimeoutMs);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            logError("poll failed: %s", std::strerror(errno));
            break;
        }
        if (!running) {
            break;
        }
        // A wake-up byte means a signal arrived: leave the loop so that the
        // caller can shut down and flush the state. The pipe is always the
        // last entry registered, whatever listeners are active.
        bool signalled = gWakePipe[0] >= 0 && (fds[count - 1].revents & POLLIN) != 0;
        if (signalled) {
            logInfo("shutdown requested");
            break;
        }
        if (websocketFdIndex >= 0 && (fds[websocketFdIndex].revents & POLLIN) != 0) {
            acceptWebSocket();
        }
        if (mqttSnFdIndex >= 0 && (fds[mqttSnFdIndex].revents & POLLIN) != 0) {
            // Never blocks: drains every queued datagram, then returns.
            mqttSnGateway->handleReadable();
        }
        if ((fds[0].revents & POLLIN) == 0) {
            continue;
        }

        int clientSocket = listener->acceptConnection();
        if (clientSocket < 0) {
            if (!running) {
                break;
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            if (errno == EMFILE || errno == ENFILE) {
                // Out of file descriptors: the connection stays queued and the
                // listener stays readable, so backing off (instead of looping)
                // keeps poll() from spinning at 100% CPU until an fd frees up.
                logError("accept failed: %s; pausing accepts briefly", std::strerror(errno));
                listenerBackoffUntil_ = std::chrono::steady_clock::now() + acceptBackoff;
                continue;
            }
            logError("failed to accept connection: %s", std::strerror(errno));
            continue;
        }
        // The descriptor space has room again; stop backing off.
        listenerBackoffUntil_ = std::chrono::steady_clock::time_point{};

        if (getActiveConnections() >= maxConnections) {
            logError("too many connections, rejecting client");
            close(clientSocket);
            continue;
        }

        // The shared_ptr keeps the connection alive for the lifetime of the
        // session callbacks, even if the thread itself has already finished
        auto connection = std::make_shared<Connection>(clientSocket, broker, TLS);
        if (!startClientThread(clientSocket, connection)) {
            // Shutdown started between accept() and here: no thread was
            // started. The Connection we constructed still owns the descriptor
            // and closes it in its destructor, so we must not close it again.
            break;
        }
    }
}

void Server::acceptWebSocket() {
    int clientSocket = websocketListener->acceptConnection();
    if (clientSocket < 0) {
        if (!running) {
            return;
        }
        if (errno == EMFILE || errno == ENFILE) {
            // Same fd-exhaustion back off as the MQTT listener: without it the
            // readable-but-unaccepted socket spins poll() at 100% CPU.
            logError("websocket accept failed: %s; pausing accepts briefly",
                     std::strerror(errno));
            websocketBackoffUntil_ = std::chrono::steady_clock::now() + acceptBackoff;
            return;
        }
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            logError("failed to accept websocket connection: %s", std::strerror(errno));
        }
        return;
    }
    // The descriptor space has room again; stop backing off.
    websocketBackoffUntil_ = std::chrono::steady_clock::time_point{};
    if (getActiveConnections() >= maxConnections) {
        logError("too many connections, rejecting websocket client");
        close(clientSocket);
        return;
    }
    // The HTTP Upgrade handshake (and any TLS handshake for WSS) runs inside
    // Connection::run() through the transport's handshake() hook.
    auto websocket = std::make_unique<WebSocketTransport>(
        std::make_unique<SecureTransport>(
            clientSocket, TLS,
            broker ? broker->getWriteTimeoutMs()
                   : SecureTransport::WRITE_STALL_TIMEOUT_MS));
    auto connection = std::make_shared<Connection>(clientSocket, broker,
                                                   std::move(websocket));
    if (!startClientThread(clientSocket, connection)) {
        // Shutdown started: no thread was started. The Connection we built still
        // owns the descriptor and closes it in its destructor.
    }
}

void Server::handleClient(const std::shared_ptr<Connection> &connection) {
    // The connection was kept alive by the shared_ptr captured here; the
    // descriptor is owned by the Connection and closed exactly once by
    // Connection::run() when it returns.
    connection->run();
}

bool Server::startClientThread(int clientSocket,
                               const std::shared_ptr<Connection> &connection) {
    // The thread is created while the map lock is held so that shutdown cannot
    // miss it: stop() sets running=false and then snapshots the threads, so a
    // thread either appears here before that snapshot or is never started.
    std::shared_ptr<std::atomic<bool>> finished = std::make_shared<std::atomic<bool>>(false);
    std::lock_guard<std::mutex> lock(connectionsMutex);
    if (!running) {
        return false;
    }
    connections[clientSocket] = connection;
    std::thread thread([this, clientSocket, connection, finished]() {
        handleClient(connection);
        unregisterConnection(clientSocket, connection);
        finished->store(true, std::memory_order_release);
    });
    clientThreads.push_back(ClientThread{std::move(thread), std::move(finished)});
    return true;
}

void Server::unregisterConnection(int clientSocket,
                                  const std::shared_ptr<Connection> &connection) {
    std::lock_guard<std::mutex> lock(connectionsMutex);
    // Only drop the entry if it still names this connection. The descriptor may
    // have been closed by run() and reused by a newer accept() before we get
    // here; erasing unconditionally would evict that newer, still-live entry.
    auto it = connections.find(clientSocket);
    if (it != connections.end() && it->second == connection) {
        connections.erase(it);
    }
}

void Server::reapFinishedClientThreads() {
    std::vector<ClientThread> finished;
    {
        std::lock_guard<std::mutex> lock(connectionsMutex);
        for (auto it = clientThreads.begin(); it != clientThreads.end();) {
            if (it->finished->load(std::memory_order_acquire)) {
                finished.push_back(std::move(*it));
                it = clientThreads.erase(it);
            } else {
                ++it;
            }
        }
    }
    // Join outside the lock: a finishing thread takes connectionsMutex on its
    // way out.
    for (auto &client : finished) {
        if (client.thread.joinable()) {
            client.thread.join();
        }
    }
}

void Server::joinClientThreads() {
    std::vector<ClientThread> remaining;
    {
        std::lock_guard<std::mutex> lock(connectionsMutex);
        remaining.swap(clientThreads);
    }
    for (auto &client : remaining) {
        if (client.thread.joinable()) {
            client.thread.join();
        }
    }
}

size_t Server::getActiveConnections() const {
    std::lock_guard<std::mutex> lock(connectionsMutex);
    return connections.size();
}

void Server::closeClient(int clientSocket) {
    // Kept for API compatibility: shutting the socket down lets the owning
    // Connection leave its read loop and close the descriptor itself
    if (clientSocket >= 0) {
        ::shutdown(clientSocket, SHUT_RDWR);
    }
}

void Server::stop() {
    // stop() is called by main and again by the destructor: only do the work once
    if (!running.exchange(false)) {
        return;
    }
    if (listener) {
        listener->stop();
    }
    if (websocketListener) {
        websocketListener->stop();
    }
    if (mqttSnGateway) {
        mqttSnGateway->stop();
    }
#if defined(FLOWMQ_WITH_QUIC)
    if (quicListener) {
        quicListener->stop();
    }
#endif

    std::map<int, std::shared_ptr<Connection>> open;
    {
        std::lock_guard<std::mutex> lock(connectionsMutex);
        open = connections;
    }
    // Shutting the sockets down makes the client threads leave their read loop;
    // each Connection closes its own descriptor exactly once
    for (auto& [socket, connection] : open) {
        closeClient(socket);
    }
    // Wait for the serving threads to finish before anything they use (the
    // broker, the store, this object) is torn down. Detaching them would let a
    // thread outlive the server and unlock a mutex that no longer exists.
    joinClientThreads();

    logInfo("MQTT server stopped");
}

void Server::shutdownPersistence() {
    if (!store) {
        return;
    }
    // Stop the writer first so no snapshot can race with the final flush
    store->stop();
    if (store->enabled() && broker && broker->getStore()) {
        if (store->flushNow()) {
            logInfo("state flushed");
        }
    }
    if (gWakePipe[0] >= 0) {
        ::close(gWakePipe[0]);
        ::close(gWakePipe[1]);
        gWakePipe[0] = gWakePipe[1] = -1;
    }
}

}
