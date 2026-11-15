#include <gtest/gtest.h>
#include "Broker.h"
#include "Session.h"
#include <atomic>
#include <mutex>

namespace MQTT {

//! Records everything the session hands to its client
class RecordingClient : public Client {
public:
    void deliver(const Message &message, uint16_t packetId, QoS qos,
                 uint32_t subscriptionIdentifier = 0) override {
        // Deliveries can come from any publishing thread
        std::lock_guard<std::mutex> lock(mutex);
        delivered.push_back(message);
        packetIds.push_back(packetId);
        qosValues.push_back(qos);
        subscriptionIdentifiers.push_back(subscriptionIdentifier);
    }

    void sendPubrec(uint16_t packetId) override {
        std::lock_guard<std::mutex> lock(mutex);
        resentPubrecs.push_back(packetId);
    }

    void onSuperseded(Version version) override { (void)version; superseded++; }

    size_t size() {
        std::lock_guard<std::mutex> lock(mutex);
        return delivered.size();
    }

    std::vector<Message> snapshot() {
        std::lock_guard<std::mutex> lock(mutex);
        return delivered;
    }

    std::vector<Message> delivered;
    std::vector<uint16_t> packetIds;
    std::vector<QoS> qosValues;
    std::vector<uint32_t> subscriptionIdentifiers;
    std::vector<uint16_t> resentPubrecs;
    std::atomic<int> superseded{0};

private:
    std::mutex mutex;
};

namespace test {
inline std::shared_ptr<RecordingClient> attach(Session &session) {
    auto client = std::make_shared<RecordingClient>();
    session.bindClient(client);
    return client;
}
} // namespace test

} // namespace MQTT
