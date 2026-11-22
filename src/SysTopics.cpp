#include "SysTopics.h"
#include "Topic.h"

#include <cstdio>
#include <string>
#include <vector>
#ifdef __linux__
#include <sys/sysinfo.h>
#endif
#include <sys/types.h>
#include <unistd.h>

namespace MQTT {

namespace {
//! The $SYS topics the broker publishes. Order is stable (used for publishing
//! and for deterministic subscription matching); matching is by exact name.
const std::vector<std::string> kTopics = {
    "$SYS/broker/clients/connected",
    "$SYS/broker/clients/total",
    "$SYS/broker/subscriptions/count",
    "$SYS/broker/disconnect/keepalive",
    "$SYS/broker/disconnect/protocol",
    "$SYS/broker/disconnect/quota",
    "$SYS/broker/disconnect/auth",
    "$SYS/broker/disconnect/normal",
    "$SYS/broker/disconnect/other",
    "$SYS/broker/messages/sent",
    "$SYS/broker/messages/received",
    "$SYS/broker/messages/dropped",
    "$SYS/broker/inflight/count",
    "$SYS/broker/writebuffer/bytes",
    "$SYS/broker/queued/bytes",
    "$SYS/broker/retaineds/count",
    "$SYS/broker/uptime",
    "$SYS/broker/load",
};

std::string numberPayload(size_t value) {
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%zu", value);
    return std::string(buffer);
}

//! The 1-minute load average, or "0.00" where it cannot be read (some
//! containers do not expose /proc/loadavg).
std::string loadAveragePayload() {
    double loads[3] = {0, 0, 0};
    if (::getloadavg(loads, 3) < 0) {
        return "0.00";
    }
    char buffer[32];
    std::snprintf(buffer, sizeof(buffer), "%.2f", loads[0]);
    return std::string(buffer);
}
} // namespace

namespace SysTopics {

bool isSysTopic(const std::string &topic) {
    return topic.size() >= 5 && topic.compare(0, 5, "$SYS/") == 0;
}

const std::vector<std::string> &allTopics() {
    return kTopics;
}

size_t matchedCount(const std::string &topicFilter) {
    size_t count = 0;
    for (const auto &topic : kTopics) {
        if (Topic::match(topic, topicFilter)) {
            ++count;
        }
    }
    return count;
}

std::string payload(const std::string &topic, size_t connected, size_t sessions,
                    size_t subscriptions, size_t inflight, size_t retained) {
    if (topic == "$SYS/broker/clients/connected") {
        return numberPayload(connected);
    }
    if (topic == "$SYS/broker/clients/total") {
        return numberPayload(sessions);
    }
    if (topic == "$SYS/broker/subscriptions/count") {
        return numberPayload(subscriptions);
    }
    if (topic == "$SYS/broker/inflight/count") {
        return numberPayload(inflight);
    }
    if (topic == "$SYS/broker/disconnect/keepalive") {
        return numberPayload(sysGetDisconnectsKeepAlive());
    }
    if (topic == "$SYS/broker/disconnect/protocol") {
        return numberPayload(sysGetDisconnectsProtocol());
    }
    if (topic == "$SYS/broker/disconnect/quota") {
        return numberPayload(sysGetDisconnectsQuota());
    }
    if (topic == "$SYS/broker/disconnect/auth") {
        return numberPayload(sysGetDisconnectsAuth());
    }
    if (topic == "$SYS/broker/disconnect/normal") {
        return numberPayload(sysGetDisconnectsNormal());
    }
    if (topic == "$SYS/broker/disconnect/other") {
        return numberPayload(sysGetDisconnectsOther());
    }
    if (topic == "$SYS/broker/messages/sent") {
        return numberPayload(sysGetTotalSent());
    }
    if (topic == "$SYS/broker/messages/received") {
        return numberPayload(sysGetTotalReceived());
    }
    if (topic == "$SYS/broker/messages/dropped") {
        return numberPayload(sysGetTotalDropped());
    }
    if (topic == "$SYS/broker/writebuffer/bytes") {
        return numberPayload(sysGetWriteBufferBytes());
    }
    if (topic == "$SYS/broker/queued/bytes") {
        return numberPayload(sysGetQueuedBytes());
    }
    if (topic == "$SYS/broker/retaineds/count") {
        return numberPayload(retained);
    }
    if (topic == "$SYS/broker/uptime") {
        return numberPayload(static_cast<size_t>(sysUptimeSeconds()));
    }
    if (topic == "$SYS/broker/load") {
        return loadAveragePayload();
    }
    return std::string();
}

} // namespace SysTopics

} // namespace MQTT
