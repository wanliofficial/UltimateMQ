#ifndef KAFKA_CONNECT_H
#define KAFKA_CONNECT_H

#include "Listener.h"
#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <thread>

namespace Kafka {

class Connect {
public:
    Connect();
    ~Connect();
    void start(int port);
    void stop();
    bool running() const { return thread.joinable(); }

private:
    struct Connector {
        std::string type;
        std::map<std::string, std::string> config;
    };
    struct Response {
        int status = 200;
        std::string contentType = "application/json";
        std::string body;
    };

    void serve();
    Response request(const std::string& method, const std::string& path,
                     const std::string& body);
    static Response errorResponse(int status, const std::string& message);
    Response connectorResponse(const std::string& name, bool includeStatus) const;

    mutable std::mutex mutex;
    std::map<std::string, Connector> connectors;
    std::unique_ptr<MQTT::Listener> listener;
    std::thread thread;
};

} // namespace Kafka

#endif
