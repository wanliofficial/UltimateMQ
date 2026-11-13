#include <gtest/gtest.h>
#include "Broker.h"
#include "Session.h"
#include <mutex>

namespace MQTT {

//! Records everything the session hands to its client
class RecordingClient : public Client {
public:
    void deliver(const Message &message, uint16_t packetId, QoS qos) override {
        // Deliveries can come from any publishing thread
        std::lock_guard<std::mutex> lock(mutex);
        delivered.push_back(message);
        packetIds.push_back(packetId);
        qosValues.push_back(qos);
    }
    size_t size() {
        std::lock_guard<std::mutex> lock(mutex);
        return delivered.size();
    }
    std::vector<Message> delivered;
    std::vector<uint16_t> packetIds;
    std::vector<QoS> qosValues;

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
