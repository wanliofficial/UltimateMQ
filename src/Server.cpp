#include "Topic.h"
#include "Server.h"
#include "Connection.h"
#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include <thread>
#include "Log.h"

namespace MQTT {
Server::Server(int port, size_t maxConnections)
    : maxConnections(maxConnections == 0 ? 1024 : maxConnections) {
    listener = std::make_unique<Listener>(port);
    auth = std::make_unique<Auth>();
    std::string error;
    const char* passwordFile = getenv("FLOWMQ_PASSWORD_FILE");
    if (passwordFile && *passwordFile) {
        if (!auth->loadUsers(passwordFile, &error)) {
            throw std::runtime_error("failed to load password file: " + error);
        }
    }
    const char* aclFile = getenv("FLOWMQ_ACL_FILE");
    if (aclFile && *aclFile) {
        if (!auth->loadAcl(aclFile, &error)) {
            throw std::runtime_error("failed to load acl file: " + error);
        }
    }
    logInfo("authentication %s, acl %s", auth->authRequired() ? "enabled" : "disabled",
            auth->aclEnabled() ? "enabled" : "disabled");
    if (auth->aclEnabled() && !auth->authRequired()) {
        logError("acl is enabled without authentication: clients can claim any username");
    }
    broker = new Broker(auth.get());
}

Server::~Server() {
    stop();
    delete broker;
}

void Server::start() {
    listener->start();
    running = true;
    logInfo("MQTT server started");
    run();
}

void Server::run() {
    while (running) {
        int clientSocket = listener->acceptConnection();
        if (clientSocket < 0) {
            if (!running) {
                break;
            }
            logError("failed to accept connection");
            continue;
        }

        if (getActiveConnections() >= maxConnections) {
            logError("too many connections, rejecting client");
            close(clientSocket);
            continue;
        }

        // The shared_ptr keeps the connection alive for the lifetime of the
        // session callbacks, even if the thread itself has already finished
        auto connection = std::make_shared<Connection>(clientSocket, broker);
        registerConnection(clientSocket, connection);

        std::thread clientThread([this, clientSocket, connection]() {
            handleClient(clientSocket);
            unregisterConnection(clientSocket);
        });
        clientThread.detach();
    }
}

void Server::handleClient(int clientSocket) {
    std::shared_ptr<Connection> connection;
    {
        std::lock_guard<std::mutex> lock(connectionsMutex);
        auto it = connections.find(clientSocket);
        if (it == connections.end()) {
            return;
        }
        connection = it->second;
    }
    // Connection::run() owns the socket and closes it when it is done
    connection->run();
}

void Server::registerConnection(int clientSocket, const std::shared_ptr<Connection>& connection) {
    std::lock_guard<std::mutex> lock(connectionsMutex);
    connections[clientSocket] = connection;
}

void Server::unregisterConnection(int clientSocket) {
    std::lock_guard<std::mutex> lock(connectionsMutex);
    connections.erase(clientSocket);
}

size_t Server::getActiveConnections() const {
    std::lock_guard<std::mutex> lock(connectionsMutex);
    return connections.size();
}

void Server::closeClient(int clientSocket) {
    // Kept for API compatibility: shutting the socket down lets the owning
    // Connection leave its read loop and close the descriptor itself
    if (clientSocket >= 0) {
        ::shutdown(clientSocket, SHUT_RDWR);
    }
}

void Server::stop() {
    running = false;
    if (listener) {
        listener->stop();
    }

    std::map<int, std::shared_ptr<Connection>> open;
    {
        std::lock_guard<std::mutex> lock(connectionsMutex);
        open = connections;
    }
    // Shutting the sockets down makes the client threads leave their read loop;
    // each Connection closes its own descriptor exactly once
    for (auto& [socket, connection] : open) {
        closeClient(socket);
    }

    logInfo("MQTT server stopped");
}

}
