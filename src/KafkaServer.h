#ifndef KAFKA_SERVER_H
#define KAFKA_SERVER_H

#include "Auth.h"
#include "KafkaBroker.h"
#include "KafkaConnection.h"
#include "KafkaConnect.h"
#include "Listener.h"
#include "TLS.h"
#include <atomic>
#include <csignal>
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace Kafka {

class Server {
public:
    Server(int port, int32_t brokerID, std::string advertisedHost, uint16_t advertisedPort,
           std::string stateFile, size_t maxMessageBytes, size_t maxLogBytes,
           size_t maxRequestBytes, size_t maxConnections, int writeTimeoutMs,
           std::shared_ptr<MQTT::TLSContext> TLS, std::unique_ptr<MQTT::Auth> auth,
           int connectPort = 0);
    ~Server();

    void run();
    void stop();
    static void installSignalHandlers();
    static volatile std::sig_atomic_t stopRequested;

private:
    struct Client {
        std::shared_ptr<Connection> connection;
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> finished;
    };

    void acceptClient();
    void reapClients();
    void joinClients();

    int port;
    int connectPort;
    std::unique_ptr<Broker> broker;
    std::unique_ptr<Connect> connect;
    std::unique_ptr<MQTT::Listener> listener;
    std::shared_ptr<MQTT::TLSContext> TLS;
    std::unique_ptr<MQTT::Auth> auth;
    size_t maxRequestBytes;
    size_t maxConnections;
    int writeTimeoutMs;
    std::atomic<bool> running{false};
    std::mutex clientsMutex;
    std::map<int, std::shared_ptr<Connection>> connections;
    std::vector<Client> clients;
};

} // namespace Kafka

#endif
