#ifndef SYS_TOPICS_H
#define SYS_TOPICS_H

#include <string>
#include <vector>

namespace MQTT {

// Process counters backing the $SYS/broker monitoring topics. They live in
// Broker.cpp; declared here so SysTopics can read them without a back-reference
// to the Broker. Each is a relaxed snapshot of a monotonic counter or gauge.
size_t sysGetTotalSent();
size_t sysGetTotalReceived();
size_t sysGetTotalDropped();
double sysUptimeSeconds();

//! Optional broker-side monitoring topic tree (the "$SYS/..." tree). When the
//! feature is enabled the broker publishes its own counters as retained QoS 0
//! messages; when a client subscribes to a $SYS filter it receives them through
//! the ordinary retained-message path. While the feature is off the broker
//! exposes no $SYS topic and a client publish to "$SYS/..." is ordinary traffic.
//!
//! All $SYS topics are QoS 0 and are never fanned out live to ordinary
//! subscribers. Publishing under "$SYS" is gated behind an explicit, documented
//! switch (FLOWMQ_SYS_TOPICS): the default is off and the broker behaves
//! exactly as before.
//!
//! Implemented topics:
//!   $SYS/broker/clients/connected    number of currently connected clients
//!   $SYS/broker/clients/total        number of sessions the broker holds
//!   $SYS/broker/messages/sent        total PUBLISHes the broker has delivered
//!   $SYS/broker/messages/received    total PUBLISHes the broker has accepted
//!   $SYS/broker/messages/dropped     total PUBLISHes dropped under backpressure
//!   $SYS/broker/retaineds/count      number of retained messages held
//!   $SYS/broker/uptime               seconds since the broker started
//!   $SYS/broker/load                 1-minute load average
//!
//! A leading level wildcard never matches a "$SYS/..." topic, and a "$SYS/#"
//! subscription reaches only the tree itself, so the tree is opt-in from both
//! ends.
namespace SysTopics {

//! Whether a topic name/filter lives inside the $SYS tree.
bool isSysTopic(const std::string &topic);

//! The distinct $SYS topics the broker publishes, in a stable order. Used both
//! to publish them and to answer wildcard subscriptions.
const std::vector<std::string> &allTopics();

//! Count the distinct $SYS topics a subscription filter matches. The filter may
//! be a concrete name or a wildcard; a leading level wildcard matches none.
size_t matchedCount(const std::string &topicFilter);

//! Build the payload of one $SYS topic from the broker's counters. All four
//! counts are read under the broker lock by the caller; the rest use
//! process-wide state, so the payload of a single topic is internally
//! consistent. Returns an empty string for an unknown topic.
std::string payload(const std::string &topic, size_t connected, size_t sessions,
                    size_t retained);

} // namespace SysTopics

} // namespace MQTT

#endif
