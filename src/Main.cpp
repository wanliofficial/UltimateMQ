
#include "Server.h"
#include <iostream>
#include <cstdlib>

int main(int argc, char* argv[]) {
     try {
        MQTT::Server server(getenv("MQTT_PORT")?atoi(getenv("MQTT_PORT")):1883); // Default MQTT port
        server.start();
    } catch (const std::exception& e) {
        std::cerr << "Error: " << e.what() << std::endl;
        return 1;
    }
    return 0;
}