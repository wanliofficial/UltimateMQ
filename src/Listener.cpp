#include <sys/socket.h>
#include <unistd.h>
#include <cerrno>
#include <cstring>
#include <stdexcept>
#include <iostream>
#include "Listener.h"
#include "Log.h"

namespace MQTT {

Listener::Listener(int port, bool loopback) : port{port}, sockfd{-1} {
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(loopback ? INADDR_LOOPBACK : INADDR_ANY);
    address.sin_port = htons(port);
}

Listener::~Listener() {
    if (sockfd != -1) close(sockfd); 
}

void Listener::start() {
    sockfd = socket(AF_INET, SOCK_STREAM, 0);
    if (sockfd < 0) {
        throw std::runtime_error("Failed to create socket");
    }  

    // Allow the port to be re-bound immediately after a restart. Without this,
    // sockets from the previous instance linger in TIME_WAIT and bind() fails
    // with EADDRINUSE for up to a minute, so a redeploy or crash recovery could
    // not take the port back.
    int reuse = 1;
    if (setsockopt(sockfd, SOL_SOCKET, SO_REUSEADDR, &reuse, sizeof(reuse)) < 0) {
        throw std::runtime_error(std::string("Failed to set SO_REUSEADDR: ") +
                                 std::strerror(errno));
    }

    if (bind(sockfd, (struct sockaddr *)&address, sizeof(address)) < 0) {
        throw std::runtime_error(std::string("Failed to bind to a port: ") +
                                 std::strerror(errno));
    }
    if (listen(sockfd, 10) < 0) {
        throw std::runtime_error("Failed to listen on socket");
    }
    logInfo("MQTT listener on port %d", port);
}

int Listener::acceptConnection() {
    sockaddr_in clientAddr{};
    socklen_t clientAddrLen = sizeof(clientAddr);
    return accept(sockfd, (struct sockaddr*)&clientAddr, &clientAddrLen);
}

void Listener::stop() {
    if (sockfd != -1) {
        close(sockfd);
        sockfd = -1;
    }
}

}