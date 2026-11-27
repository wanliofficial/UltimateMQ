#include "KafkaServer.h"
#include "Log.h"
#include <cerrno>
#include <cstring>
#include <poll.h>
#include <stdexcept>
#include <unistd.h>

namespace Kafka {

volatile std::sig_atomic_t Server::stopRequested = 0;

Server::Server(int port, int32_t brokerID, std::string advertisedHost,
               uint16_t advertisedPort, std::string stateFile, size_t maxMessageBytes,
               size_t maxLogBytes, size_t maxRequestBytes, size_t maxConnections,
               int writeTimeoutMs, std::shared_ptr<MQTT::TLSContext> TLS,
               std::unique_ptr<MQTT::Auth> auth, int connectPort)
    : port(port), connectPort(connectPort), listener(std::make_unique<MQTT::Listener>(port)), TLS(std::move(TLS)),
      auth(std::move(auth)), maxRequestBytes(maxRequestBytes),
      maxConnections(maxConnections == 0 ? 1024 : maxConnections), writeTimeoutMs(writeTimeoutMs) {
    broker = std::make_unique<Broker>(this->auth.get(), brokerID, std::move(stateFile),
                                      maxMessageBytes, maxLogBytes, advertisedHost,
                                      advertisedPort);
    if (!broker->load()) throw std::runtime_error("failed to load Kafka state");
}

Server::~Server() {
    stop();
}

void Server::installSignalHandlers() {
    struct sigaction action {};
    action.sa_handler = [](int) { stopRequested = 1; };
    sigemptyset(&action.sa_mask);
    sigaction(SIGINT, &action, nullptr);
    sigaction(SIGTERM, &action, nullptr);
}

void Server::run() {
    listener->start();
    running = true;
    MQTT::logInfo("Kafka broker %d listening on port %d", broker->brokerID(), port);
    if (connectPort > 0) {
        connect = std::make_unique<Connect>();
        connect->start(connectPort);
    }
    while (running && !stopRequested) {
        reapClients();
        struct pollfd descriptor {listener->fd(), POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, 250);
        if (ready < 0) {
            if (errno == EINTR) continue;
            MQTT::logError("Kafka poll failed: %s", std::strerror(errno));
            break;
        }
        if (ready > 0 && (descriptor.revents & POLLIN) != 0) acceptClient();
    }
    stop();
}

void Server::acceptClient() {
    const int socket = listener->acceptConnection();
    if (socket < 0) {
        if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            MQTT::logError("Kafka accept failed: %s", std::strerror(errno));
        }
        return;
    }
    std::lock_guard<std::mutex> lock(clientsMutex);
    if (!running || connections.size() >= maxConnections) {
        ::close(socket);
        return;
    }
    auto connection = std::make_shared<Connection>(
        socket, broker.get(), TLS, writeTimeoutMs, maxRequestBytes, auth.get(),
        auth->authRequired());
    connections[socket] = connection;
    auto finished = std::make_shared<std::atomic<bool>>(false);
    clients.push_back({connection, {}, finished});
    Client& client = clients.back();
    client.thread = std::thread([this, socket, connection, finished]() {
        try {
            connection->run();
        } catch (const std::exception& error) {
            MQTT::logError("Kafka client error: %s", error.what());
        }
        connection->close();
        {
            std::lock_guard<std::mutex> clientsLock(clientsMutex);
            auto found = connections.find(socket);
            if (found != connections.end() && found->second == connection) connections.erase(found);
        }
        finished->store(true, std::memory_order_release);
    });
}

void Server::reapClients() {
    std::vector<Client> finished;
    {
        std::lock_guard<std::mutex> lock(clientsMutex);
        for (auto it = clients.begin(); it != clients.end();) {
            if (it->finished->load(std::memory_order_acquire)) {
                finished.push_back(std::move(*it));
                it = clients.erase(it);
            } else {
                ++it;
            }
        }
    }
    for (Client& client : finished) {
        if (client.thread.joinable()) client.thread.join();
    }
}

void Server::joinClients() {
    std::vector<Client> remaining;
    {
        std::lock_guard<std::mutex> lock(clientsMutex);
        remaining.swap(clients);
        for (auto& entry : connections) entry.second->close();
        connections.clear();
    }
    for (Client& client : remaining) {
        if (client.thread.joinable()) client.thread.join();
    }
}

void Server::stop() {
    if (!running.exchange(false)) return;
    listener->stop();
    if (connect) connect->stop();
    joinClients();
    if (!broker->flush()) MQTT::logError("failed to flush Kafka state");
    MQTT::logInfo("Kafka broker stopped");
}

} // namespace Kafka
