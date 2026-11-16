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

namespace MQTT {

class Connection;
class Server {
public:
    explicit Server(int port, size_t maxConnections = 1024);
    ~Server();

    void start();
    //! Handle one accepted socket until it closes; the server keeps it alive
    void handleClient(int clientSocket);
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
    //! Register a connection and start the thread that serves it. Returns false
    //! (and starts nothing) once shutdown has begun.
    bool startClientThread(int clientSocket, const std::shared_ptr<Connection>& connection);
    void unregisterConnection(int clientSocket);
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
};

} // namespace MQTT
