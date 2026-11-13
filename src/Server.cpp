#include "Topic.h"
#include "Server.h"
#include "Connection.h"
#include <csignal>
#include <stdexcept>
#include <cstring>
#include <cstdlib>
#include <cerrno>
#include <fcntl.h>
#include <poll.h>
#include <unistd.h>
#include <arpa/inet.h>
#include <iostream>
#include <thread>
#include "Log.h"

namespace MQTT {

namespace {
//! Self-pipe used by the signal handler; writing to it wakes accept()
int gWakePipe[2] = {-1, -1};

void handleSignal(int) {
    // Only async-signal-safe calls here
    char byte = 1;
    ssize_t ignored = ::write(gWakePipe[1], &byte, 1);
    (void)ignored;
}
} // namespace

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

    // Durable state (opt-in). The store is created before the broker so that a
    // corrupted state file aborts startup instead of silently losing sessions.
    const char* stateFile = getenv("FLOWMQ_STATE_FILE");
    if (stateFile && *stateFile) {
        store = std::make_unique<Store>(stateFile, std::chrono::milliseconds(1000));
        logInfo("state file: %s", stateFile);
    } else {
        store = std::make_unique<Store>(std::string(), std::chrono::milliseconds(1000));
    }

    broker = new Broker(auth.get(), store.get());

    if (store->enabled()) {
        PersistedState persisted;
        std::string loadError;
        if (!store->load(persisted, &loadError)) {
            delete broker;
            broker = nullptr;
            throw std::runtime_error("failed to load state file: " + loadError);
        }
        broker->restoreState(persisted);
    }
}

Server::~Server() {
    stop();
    shutdownPersistence();
    delete broker;
    broker = nullptr;
}

void Server::start() {
    listener->start();
    // Start the background writer only once the state has been restored
    store->start([this]() { return broker->snapshotState(); });
    running = true;
    logInfo("MQTT server started");
    run();
}

void Server::installSignalHandlers() {
    if (::pipe(gWakePipe) != 0) {
        logError("cannot create signal pipe: %s", std::strerror(errno));
        return;
    }
    // Non-blocking ends so a handler can never stall the process
    for (int fd : gWakePipe) {
        int flags = ::fcntl(fd, F_GETFL, 0);
        ::fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    }
    struct sigaction action {};
    action.sa_handler = handleSignal;
    sigemptyset(&action.sa_mask);
    action.sa_flags = SA_RESTART;
    ::sigaction(SIGINT, &action, nullptr);
    ::sigaction(SIGTERM, &action, nullptr);
}

void Server::run() {
    while (running) {
        // Wait for either a new client or the shutdown wake-up pipe
        struct pollfd fds[2];
        int count = 0;
        fds[count].fd = listener->fd();
        fds[count].events = POLLIN;
        ++count;
        if (gWakePipe[0] >= 0) {
            fds[count].fd = gWakePipe[0];
            fds[count].events = POLLIN;
            ++count;
        }

        int ready = ::poll(fds, count, -1);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            logError("poll failed: %s", std::strerror(errno));
            break;
        }
        if (!running) {
            break;
        }
        // A wake-up byte means a signal arrived: leave the loop so that the
        // caller can shut down and flush the state
        bool signalled = count > 1 && (fds[1].revents & POLLIN) != 0;
        if (signalled) {
            logInfo("shutdown requested");
            break;
        }
        if ((fds[0].revents & POLLIN) == 0) {
            continue;
        }

        int clientSocket = listener->acceptConnection();
        if (clientSocket < 0) {
            if (!running) {
                break;
            }
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                continue;
            }
            logError("failed to accept connection: %s", std::strerror(errno));
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
    // stop() is called by main and again by the destructor: only do the work once
    if (!running.exchange(false)) {
        return;
    }
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

void Server::shutdownPersistence() {
    if (!store) {
        return;
    }
    // Stop the writer first so no snapshot can race with the final flush
    store->stop();
    if (store->enabled() && broker && broker->getStore()) {
        if (store->flushNow()) {
            logInfo("state flushed");
        }
    }
    if (gWakePipe[0] >= 0) {
        ::close(gWakePipe[0]);
        ::close(gWakePipe[1]);
        gWakePipe[0] = gWakePipe[1] = -1;
    }
}

}
