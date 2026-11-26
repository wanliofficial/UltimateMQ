#ifndef AMQP_STORE_H
#define AMQP_STORE_H

#include "AMQPBroker.h"
#include <atomic>
#include <chrono>
#include <functional>
#include <mutex>
#include <string>
#include <thread>

namespace AMQP {

class Store {
public:
    using SnapshotFn = std::function<PersistedState()>;

    Store(std::string path, std::chrono::milliseconds interval);
    ~Store();
    Store(const Store&) = delete;
    Store& operator=(const Store&) = delete;

    bool enabled() const { return !path.empty(); }
    bool load(PersistedState& out, std::string* error = nullptr) const;
    void start(SnapshotFn fn);
    void stop();
    void markDirty() { dirty.store(true, std::memory_order_relaxed); }
    bool flushNow();
    bool flushNow(SnapshotFn fn);

private:
    void loop();
    bool writeSnapshot(const SnapshotFn& fn);
    bool writeLocked(const PersistedState& state);

    std::string path;
    std::chrono::milliseconds interval;
    SnapshotFn snapshot;
    std::atomic<bool> dirty{false};
    std::atomic<bool> running{false};
    std::thread thread;
    std::mutex writeMutex;
};

} // namespace AMQP

#endif
