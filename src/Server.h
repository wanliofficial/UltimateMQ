#pragma once
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>
#include "Listener.h"
#include "Broker.h"
#include "Auth.h"
#include "Store.h"
#include "TLS.h"
#include "MQTTSN.h"
#if defined(FLOWMQ_WITH_QUIC)
#include "QUIC.h"
#endif

namespace MQTT {

class Connection;
class Server {
public:
    explicit Server(int port, size_t maxConnections = 1024);
    ~Server();

    void start();
    //! Serve one accepted connection until it closes. The server keeps it alive
    //! for the lifetime of the session callbacks, so the same object is handed
    //! to the serving thread and used to identify it in the registry.
    void handleClient(const std::shared_ptr<Connection> &connection);
    //! Ask a connection to leave its read loop by shutting its descriptor down.
    //! The owning Connection then closes the descriptor itself.
    void closeClient(int clientSocket);
    //! Stop accepting new clients and drop all open connections
    void stop();

    size_t getActiveConnections() const;
    //! The configured maximum number of simultaneous connections
    size_t getMaxConnections() const { return maxConnections; }

    //! Install handlers so SIGINT/SIGTERM stop the server gracefully (which
    //! flushes the state file). Call before start().
    static void installSignalHandlers();

private:
    void run();
    //! Accept one WebSocket client and start serving it. Called when the
    //! optional WebSocket listener has an incoming connection.
    void acceptWebSocket();
    //! Register a connection and start the thread that serves it. Returns false
    //! (and starts nothing) once shutdown has begun.
    bool startClientThread(int clientSocket, const std::shared_ptr<Connection>& connection);
    void unregisterConnection(int clientSocket,
                              const std::shared_ptr<Connection> &connection);
    //! Join the threads of connections that have already finished, so a
    //! long-running server does not accumulate them.
    void reapFinishedClientThreads();
    //! Join every client thread. Called after all sockets are shut down, before
    //! the broker and the server state are destroyed.
    void joinClientThreads();
    //! Flush the durable state and shut the store down
    void shutdownPersistence();

    Broker* broker;
    std::unique_ptr<Auth> auth;
    std::shared_ptr<TLSContext> TLS;
    std::unique_ptr<Store> store;
    std::unique_ptr<Listener> listener;
    //! MQTT over WebSocket listener, created only when FLOWMQ_WEBSOCKET_PORT is
    //! set. Speaks WSS when a TLS certificate is configured, plain WS otherwise.
    std::unique_ptr<Listener> websocketListener;
    //! MQTT-SN gateway (UDP), created only when FLOWMQ_MQTT_SN_PORT is set.
    std::unique_ptr<MQTTSN::Gateway> mqttSnGateway;
#if defined(FLOWMQ_WITH_QUIC)
    //! MQTT over QUIC listener, created only when FLOWMQ_QUIC_PORT is set.
    //! Built only when libmsquic is available (CMake option).
    std::unique_ptr<QUICListener> quicListener;
#endif
    std::atomic<bool> running{false};
    std::atomic<size_t> maxConnections;
    mutable std::mutex connectionsMutex;
    std::map<int, std::shared_ptr<Connection>> connections;
    //! One entry per serving thread. A thread is joined by the accept loop once
    //! it reports finished, and all of them are joined on shutdown: none may
    //! still touch the broker (or this object) after Server is destroyed.
    struct ClientThread {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> finished;
    };
    std::vector<ClientThread> clientThreads;
    //! How often the accept loop wakes to drop sessions whose disconnect-time
    //! expiry has elapsed (MQTT-5 Session Expiry Interval on DISCONNECT).
    std::chrono::milliseconds sweepInterval{1000};
    std::chrono::steady_clock::time_point lastSweep{std::chrono::steady_clock::now()};
    //! While an accept() fails with EMFILE/ENFILE the kernel keeps the connection
    //! queued and the listener readable, so poll() would spin on it at 100% CPU.
    //! Each listener backs off until this deadline before it is polled again; a
    //! successful accept clears it. A null time_point means "not backing off".
    static constexpr std::chrono::milliseconds acceptBackoff{250};
    std::chrono::steady_clock::time_point listenerBackoffUntil_{};
    std::chrono::steady_clock::time_point websocketBackoffUntil_{};
};

} // namespace MQTT
