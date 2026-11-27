#include "KafkaServer.h"
#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <iostream>
#include <memory>
#include <stdexcept>
#include <string>

namespace {

const char* setting(const char* name, const char* fallback = "") {
    const char* value = std::getenv(name);
    return value && *value ? value : fallback;
}

int64_t integer(const char* name, int64_t fallback, int64_t minimum, int64_t maximum) {
    const char* value = std::getenv(name);
    if (!value || !*value) return fallback;
    char* end = nullptr;
    errno = 0;
    const long long parsed = std::strtoll(value, &end, 10);
    if (errno != 0 || end == value || *end != '\0' || parsed < minimum || parsed > maximum) {
        throw std::runtime_error(std::string("invalid ") + name + ": " + value);
    }
    return parsed;
}

} // namespace

int main() {
    try {
        const int port = static_cast<int>(integer("FLOWMQ_KAFKA_PORT", 9092, 1, 65535));
        const int32_t brokerID = static_cast<int32_t>(integer("FLOWMQ_KAFKA_BROKER_ID", 1, 1, 2147483647));
        const uint16_t advertisedPort = static_cast<uint16_t>(
            integer("FLOWMQ_KAFKA_ADVERTISED_PORT", port, 1, 65535));
        const size_t maxMessageBytes = static_cast<size_t>(
            integer("FLOWMQ_KAFKA_MAX_MESSAGE_BYTES", 1024 * 1024, 1, 2147483647));
        const size_t maxLogBytes = static_cast<size_t>(
            integer("FLOWMQ_KAFKA_MAX_LOG_BYTES", 1024LL * 1024 * 1024, 1, 2147483647));
        const size_t maxRequestBytes = static_cast<size_t>(
            integer("FLOWMQ_KAFKA_MAX_REQUEST_BYTES", 64LL * 1024 * 1024, 8, 2147483647));
        const size_t maxConnections = static_cast<size_t>(
            integer("FLOWMQ_KAFKA_MAX_CONNECTIONS", 1024, 1, 1000000));
        const int writeTimeoutMs = static_cast<int>(
            integer("FLOWMQ_KAFKA_WRITE_TIMEOUT_MS", 30000, 1, 2147483647));

        const int connectPort = static_cast<int>(
            integer("FLOWMQ_KAFKA_CONNECT_PORT", 0, 0, 65535));

        auto auth = std::make_unique<MQTT::Auth>();
        std::string error;
        const char* users = setting("FLOWMQ_PASSWORD_FILE");
        const char* acl = setting("FLOWMQ_ACL_FILE");
        if (*users && !auth->loadUsers(users, &error)) {
            throw std::runtime_error("failed to load password file: " + error);
        }
        if (*acl && !auth->loadAcl(acl, &error)) {
            throw std::runtime_error("failed to load ACL file: " + error);
        }
        if (*acl && !auth->authRequired()) {
            throw std::runtime_error("Kafka ACLs require FLOWMQ_PASSWORD_FILE authentication");
        }

        const char* certificate = setting("FLOWMQ_TLS_CERT");
        const char* key = setting("FLOWMQ_TLS_KEY");
        const char* ca = setting("FLOWMQ_TLS_CA");
        auto TLS = MQTT::TLSContext::load(certificate, key,
                                          std::getenv("FLOWMQ_TLS_VERIFY_CLIENTS") != nullptr,
                                          ca, &error);
        if (!error.empty()) throw std::runtime_error("failed to configure TLS: " + error);

        Kafka::Server::installSignalHandlers();
        Kafka::Server server(port, brokerID, setting("FLOWMQ_KAFKA_ADVERTISED_HOST", "localhost"),
                             advertisedPort, setting("FLOWMQ_KAFKA_STATE_FILE"),
                             maxMessageBytes, maxLogBytes, maxRequestBytes, maxConnections,
                             writeTimeoutMs, TLS, std::move(auth), connectPort);
        server.run();
    } catch (const std::exception& error) {
        std::cerr << "Error: " << error.what() << '\n';
        return 1;
    }
    return 0;
}
