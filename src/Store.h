#ifndef STORE_H
#define STORE_H
#pragma once

#include <atomic>
#include <chrono>
#include <functional>
#include <map>
#include <mutex>
#include <set>
#include <string>
#include <thread>
#include <utility>
#include <vector>
#include "MQTT.h"
#include "Message.h"

namespace MQTT {

/*!
 * The on-disk form of one persistent session.
 *
 * Only what cannot be re-created is stored: the subscriptions and the
 * unacknowledged QoS>0 messages. The offline delivery queue is deliberately
 * NOT stored: for QoS>0 every queued message is also present in `inflight`,
 * and it is re-queued with DUP=1 when the session resumes, so persisting both
 * would deliver it twice. QoS 0 messages are at-most-once and may be lost.
 */
struct PersistedSession {
    std::string clientId;
    std::string username;
    uint16_t nextPacketId = 1;
    std::map<std::string, SubscriptionOptions> subscriptions;
    std::vector<std::pair<uint16_t, Message>> inflight;
    std::set<uint16_t> awaitingPubrel;
};

struct PersistedState {
    std::map<std::string, Message> retained;
    std::vector<PersistedSession> sessions;
};

/*!
 * Durable state of the broker.
 *
 * The broker marks the store dirty on every mutation (a single atomic store,
 * never I/O under a lock). A background thread then writes a snapshot at most
 * once per interval, atomically (temp file + rename), so a crash can never
 * leave a half-written state file behind.
 *
 * The state file is opt-in: without a path the store is inert and the broker
 * behaves exactly as before.
 */
class Store {
public:
    using SnapshotFn = std::function<PersistedState()>;

    Store(std::string path, std::chrono::milliseconds interval);
    ~Store();
    Store(const Store &) = delete;
    Store &operator=(const Store &) = delete;

    bool enabled() const { return !path.empty(); }

    //! Read the state file into \p out. A missing file is not an error.
    bool load(PersistedState &out, std::string *error = nullptr) const;

    //! Start the background writer. \p fn must stay valid until stop().
    void start(SnapshotFn fn);
    void stop();

    //! Flag the state as changed; cheap enough to call on every mutation
    void markDirty() { dirty.store(true, std::memory_order_relaxed); }

    //! Write a snapshot immediately (used on shutdown). Thread safe.
    bool flushNow();
    //! Write the state produced by \p fn immediately, without installing a
    //! background writer (mainly for tests).
    bool flushNow(SnapshotFn fn);

private:
    void loop();
    bool write(const PersistedState &state);

    std::string path;
    std::chrono::milliseconds interval;
    SnapshotFn snapshot;
    std::atomic<bool> dirty{false};
    std::atomic<bool> running{false};
    std::thread thread;
    std::mutex writeMutex;
};

} // namespace MQTT

#endif
