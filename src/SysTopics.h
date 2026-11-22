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
//! Live gauges: total outbound write-buffer bytes buffered across all
//! connections (rises while a slow reader is behind), the in-memory bytes held
//! by undelivered messages across all sessions (rises while queued backpressure
//! builds, before anything is dropped or handed to the socket), and the number
//! of currently connected clients.
size_t sysGetWriteBufferBytes();
size_t sysGetQueuedBytes();
size_t sysGetConnectedClients();
//! Cumulative disconnect counters by category, backing the
//! $SYS/broker/disconnect/* topics: why clients leave (keep-alive timeout, a
//! protocol/malformed violation, a rate/connection quota, an authentication
//! refusal, a graceful disconnect, or anything else). Each is a monotonic count
//! of connections that ended for that reason, incremented once at teardown.
size_t sysGetDisconnectsKeepAlive();
size_t sysGetDisconnectsProtocol();
size_t sysGetDisconnectsQuota();
size_t sysGetDisconnectsAuth();
size_t sysGetDisconnectsNormal();
size_t sysGetDisconnectsOther();

//! Optional broker-side monitoring topic tree (the "$SYS/..." tree). When the
//! feature is enabled the broker publishes its own counters as retained QoS 0
//! messages and, as each value changes on the sweep, fans the new value out to
//! the clients already subscribed. A client that subscribes therefore receives
//! the current value once from the retained path and then every subsequent
//! change as a live update (RETAIN cleared, MQTT-3.3.1-8), so a monitoring client
//! sees the gauges move rather than one frozen snapshot. While the feature is off
//! the broker exposes no $SYS topic and a client publish to "$SYS/..." is
//! ordinary traffic.
//!
//! All $SYS topics are QoS 0. The tree is never routed through the ordinary
//! publish path, so its own delivery never counts toward
//! $SYS/broker/messages/sent: the monitoring traffic does not observe itself.
//! Publishing under "$SYS" is gated behind an explicit, documented switch
//! (FLOWMQ_SYS_TOPICS): the default is off and the broker behaves exactly as
//! before.
//!
//! Implemented topics:
//!   $SYS/broker/clients/connected    number of currently connected clients
//!   $SYS/broker/clients/total        number of sessions the broker holds
//!   $SYS/broker/subscriptions/count  total subscription instances the broker routes (normal + shared)
//!   $SYS/broker/disconnect/keepalive cumulative disconnects on keep-alive timeout
//!   $SYS/broker/disconnect/protocol  cumulative disconnects on a protocol/malformed violation
//!   $SYS/broker/disconnect/quota     cumulative disconnects on a rate/connection quota
//!   $SYS/broker/disconnect/auth      cumulative disconnects on an authentication refusal
//!   $SYS/broker/disconnect/normal    cumulative graceful (normal) disconnects
//!   $SYS/broker/disconnect/other     cumulative disconnects for any other reason
//!   $SYS/broker/messages/sent        total PUBLISHes the broker has delivered
//!   $SYS/broker/messages/received    total PUBLISHes the broker has accepted
//!   $SYS/broker/messages/dropped     total PUBLISHes dropped under backpressure
//!   $SYS/broker/inflight/count       QoS 1/2 messages sent but not yet acknowledged (Receive Maximum occupancy)
//!   $SYS/broker/writebuffer/bytes    live outbound write-buffer bytes (backpressure gauge)
//!   $SYS/broker/queued/bytes         live in-memory bytes of undelivered messages (backpressure gauge)
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

//! The broker-level counts the $SYS tree reports, gathered by the caller under
//! the broker lock. A named-field record, not a long positional argument list:
//! every field is a size_t, so appending a metric to a positional signature
//! invites a silent transposition at one of the call sites (swapping, say,
//! subscriptions and inflight), which would report the wrong number under a
//! correct-looking topic.
struct BrokerCounts {
    //! Clients whose connection is currently up.
    size_t connected = 0;
    //! Sessions the broker holds, connected or not.
    size_t sessions = 0;
    //! Subscription instances the broker routes: every (client, filter) normal
    //! subscription plus every shared-subscription member.
    size_t subscriptions = 0;
    //! QoS 1/2 messages handed to a client but not yet acknowledged, across all
    //! sessions (the live Receive Maximum occupancy, MQTT-4.9).
    size_t inflight = 0;
    //! Retained messages held, excluding the broker's own $SYS tree.
    size_t retained = 0;
};

//! Build the payload of one $SYS topic from the broker's counters. The counts in
//! \p counts are read under the broker lock by the caller; the rest use
//! process-wide state, so the payload of a single topic is internally consistent.
//! Returns an empty string for an unknown topic.
std::string payload(const std::string &topic, const BrokerCounts &counts);

} // namespace SysTopics

} // namespace MQTT

#endif
