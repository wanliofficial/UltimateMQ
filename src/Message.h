#ifndef MESSAGE_H
#define MESSAGE_H
#pragma once

#include <string>
#include <vector>
#include <chrono>
#include "MQTT.h"

namespace MQTT {

struct Message {
    std::string topic;
    std::vector<uint8_t> payload;
    QoS qos = QoS::QOS_0;
    bool retain = false;
    // Retransmission flag (MQTT-3.3.1-8): set when a broker resends an
    // unacknowledged QoS>0 message after a reconnect
    bool dup = false;
    //! MQTT 5 Message Expiry Interval, as an absolute deadline. When hasExpiry
    //! is set and now reaches expiryDeadline the message is stale and must not
    //! be delivered (it is dropped instead). The durable state stores the
    //! remaining seconds, which are turned back into a deadline on restore.
    bool hasExpiry = false;
    std::chrono::steady_clock::time_point expiryDeadline;
    //! The ClientID of the session that originated this message, set on the
    //! live fan-out path. Used to honour the MQTT 5 No Local subscription option
    //! (MQTT-3.8.3-3): a subscriber that asked for it must not receive its own
    //! publications, retained ones included. A ClientID (not a Session pointer)
    //! is the right key so the check stays correct across reconnects and for
    //! retained copies outliving the publishing session. Never written to the
    //! durable state, so a restored message has no originator.
    std::string originClientId;
    //! MQTT 5 properties that travel with the publication and must be repeated on
    //! every delivery it causes (MQTT-3.3.2-4): Payload Format Indicator,
    //! Content Type, Response Topic, Correlation Data and User Properties.
    //! Message Expiry is tracked separately in hasExpiry/expiryDeadline because
    //! the remaining interval shrinks while the message waits; Topic Alias and
    //! Subscription Identifier are per-connection and never stored here.
    Properties properties;

    Message() = default;

    Message(const std::string &t, const std::vector<uint8_t> &p, QoS q = QoS::QOS_0, bool r = false)
        : topic(t), payload(std::move(p)), qos(q), retain(r) {}

    // Convenience constructor for string payloads
    Message(const std::string &t, const std::string &p, QoS q = QoS::AT_MOST_ONCE, bool r = false)
        : topic(t), payload(std::vector<uint8_t>(p.begin(), p.end())), qos(q), retain(r) {}

    //! Whether this message is stale at \p now (thread safe: reads only)
    bool isExpired(std::chrono::steady_clock::time_point now) const {
        return hasExpiry && now >= expiryDeadline;
    }

    std::string toString() const;
};

}

#endif