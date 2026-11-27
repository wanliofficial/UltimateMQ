#ifndef KAFKA_CONNECTION_H
#define KAFKA_CONNECTION_H

#include "KafkaBroker.h"
#include "KafkaWire.h"
#include "TLS.h"
#include <atomic>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace Kafka {

struct RequestHeader {
    int16_t apiKey = 0;
    int16_t apiVersion = 0;
    int32_t correlationID = 0;
    std::string clientID;
};

class Connection : public std::enable_shared_from_this<Connection> {
public:
    Connection(int socket, Broker* broker, std::shared_ptr<MQTT::TLSContext> TLS,
               int writeTimeoutMs, size_t maxRequestBytes, MQTT::Auth* auth = nullptr,
               bool authenticationRequired = false);
    ~Connection();
    Connection(const Connection&) = delete;
    Connection& operator=(const Connection&) = delete;

    void run();
    void close();
    void setUsername(std::string username) { user = std::move(username); }

private:
    int descriptor;
    Broker* broker;
    std::shared_ptr<MQTT::TLSContext> TLS;
    std::unique_ptr<MQTT::SecureTransport> transport;
    int writeTimeoutMs;
    size_t maxRequestBytes;
    MQTT::Auth* auth = nullptr;
    bool authenticationRequired;
    bool authenticated = false;
    bool authenticationFailed = false;
    std::atomic<bool> closed{false};
    std::string user;
    bool suppressResponse = false;

    bool readExact(uint8_t* data, size_t size);
    bool writeAll(const uint8_t* data, size_t size);
    std::vector<uint8_t> dispatch(const RequestHeader& header, Reader& reader);
};

} // namespace Kafka

#endif
