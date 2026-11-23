#ifndef QUIC_H
#define QUIC_H
#pragma once

#include "Transport.h"
#include <chrono>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <string>
#include <vector>

namespace MQTT {

class Broker;

//! The ALPN identifier MQTT-over-QUIC clients negotiate (same value the
//! popular brokers standardized for this binding).
constexpr const char *QUIC_ALPN_MQTT = "mqtt";

/*!
 * MQTT over QUIC (msquic). One QUIC connection is one MQTT network connection:
 * the client opens its first bidirectional stream and speaks the ordinary MQTT
 * byte stream on it, so the Frame parser and the Connection state machine are
 * reused unchanged on top of a QUICTransport.
 *
 * QUIC requires TLS 1.3. The listener reuses FLOWMQ_TLS_CERT/FLOWMQ_TLS_KEY
 * when configured; otherwise it generates an ephemeral self-signed certificate
 * for development, which QUIC clients should connect to with verification
 * disabled or certificate pinning.
 */
class QUICListener {
public:
    QUICListener(Broker *broker, uint16_t port, size_t maxConnections = 1024);
    ~QUICListener();
    QUICListener(const QUICListener &) = delete;
    QUICListener &operator=(const QUICListener &) = delete;

    void start();
    void stop();
    uint16_t port() const { return port_; }

    // Forward-declared only: the accept-path callbacks in QUIC.cpp reach the
    // bookkeeping through it, but no detail leaks past this header.
    struct Impl;

private:
    std::unique_ptr<Impl> impl;
    Broker *broker_;
    uint16_t port_;
};

} // namespace MQTT

#endif
