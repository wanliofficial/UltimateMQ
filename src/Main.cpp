
#include "Server.h"
#include <iostream>
#include <cstdlib>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <string>

namespace {

//! Parse a port number strictly, the way the Server constructor validates the
//! FLOWMQ_*_PORT variables. atoi() would silently accept "1883x" as 1883 and
//! collapse a non-numeric value to 0, so a mistyped MQTT_PORT could bind the
//! broker to an unintended port; reject anything that is not a clean 1..65535.
int parsePort(const char *value) {
    if (value == nullptr || *value == '\0') {
        return 1883;
    }
    char *end = nullptr;
    errno = 0;
    const long parsed = std::strtol(value, &end, 10);
    if (end == value || *end != '\0' || errno != 0 || parsed <= 0 || parsed > 65535) {
        throw std::runtime_error(std::string("invalid MQTT_PORT: ") + value);
    }
    return static_cast<int>(parsed);
}

} // namespace

int main(int argc, char* argv[]) {
     try {
         MQTT::Server::installSignalHandlers();
         MQTT::Server server(parsePort(getenv("MQTT_PORT"))); // Default MQTT port
         server.start();
         // start() returns when a shutdown signal arrives: stop cleanly so that
         // the state file is flushed and every connection is closed
         server.stop();
     } catch (const std::exception& e) {
         std::cerr << "Error: " << e.what() << std::endl;
         return 1;
     }
     return 0;
}
