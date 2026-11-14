#pragma once
#include <atomic>
#include <chrono>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include "Listener.h"
#include "Broker.h"
#include "Auth.h"
#include "Store.h"
#include "Tls.h"

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
    void registerConnection(int clientSocket, const std::shared_ptr<Connection>& connection);
    void unregisterConnection(int clientSocket);
    //! Flush the durable state and shut the store down
    void shutdownPersistence();

    Broker* broker;
    std::unique_ptr<Auth> auth;
    std::shared_ptr<TlsContext> tls;
    std::unique_ptr<Store> store;
    std::unique_ptr<Listener> listener;
    std::atomic<bool> running{false};
    std::atomic<size_t> maxConnections;
    mutable std::mutex connectionsMutex;
    std::map<int, std::shared_ptr<Connection>> connections;
    //! How often the accept loop wakes to drop sessions whose disconnect-time
    //! expiry has elapsed (MQTT-5 Session Expiry Interval on DISCONNECT).
    std::chrono::milliseconds sweepInterval{1000};
    std::chrono::steady_clock::time_point lastSweep{std::chrono::steady_clock::now()};
};

} // namespace MQTT
