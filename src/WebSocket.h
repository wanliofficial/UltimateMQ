#ifndef WEBSOCKET_H
#define WEBSOCKET_H
#pragma once

#include "Transport.h"
#include "TLS.h"
#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>
#include <vector>

namespace MQTT {

//! The WebSocket subprotocol name every MQTT-over-WebSocket client offers
//! (OASIS MQTT 3.1.1 §6 / MQTT 5 §7.1.2).
constexpr const char *WEBSOCKET_SUBPROTOCOL = "mqtt";

/*!
 * MQTT carried in WebSocket binary frames (RFC 6455). The transport wraps the
 * (already accepted, optionally TLS-wrapped) socket transport and adds two
 * layers:
 *
 *  1. the HTTP Upgrade handshake, including the Sec-WebSocket-Accept digest
 *     and the "mqtt" subprotocol negotiation;
 *  2. frame (de)composition, so the connection code above keeps reading a
 *     plain MQTT byte stream: readSome() hands out the payload of assembled
 *     binary messages and writeAll() frames whatever it is given.
 *
 * Text frames are not part of the MQTT binding; a client sending one gets a
 * CLOSE 1003 and the connection ends as an orderly close for the code above.
 */
class WebSocketTransport : public ClientTransport {
public:
    //! Takes ownership of the socket transport (plain or TLS). The HTTP
    //! handshake runs in handshake(), after any TLS handshake it delegates.
    explicit WebSocketTransport(std::unique_ptr<SecureTransport> inner);
    ~WebSocketTransport() override = default;
    WebSocketTransport(const WebSocketTransport &) = delete;
    WebSocketTransport &operator=(const WebSocketTransport &) = delete;

    int fd() const override { return inner_->fd(); }
    //! The HTTP Upgrade is itself a handshake, so this is always true.
    bool TLSEnabled() const override { return true; }
    bool handshake(std::string *error = nullptr) override;
    std::string peerIdentity() const override { return inner_->peerIdentity(); }
    bool peerVerified() const override { return inner_->peerVerified(); }

    int readable(int timeoutMs) override;
    ssize_t readSome(uint8_t *buf, size_t max) override;
    bool writeAll(const uint8_t *buf, size_t len) override;
    void teardown() override;

    //! The subprotocol negotiated in the handshake (empty when the client
    //! offered none; then no Sec-WebSocket-Protocol header is answered).
    const std::string &subprotocol() const { return subprotocol_; }

private:
    //! Read more raw bytes from the socket into raw_. Returns false on close
    //! or error.
    bool fillRawBuffer();
    //! Parse one complete frame out of raw_, updating the message assembly
    //! state. Returns false when the peer is gone or the framing is invalid.
    bool nextFrame();
    //! Send one frame with \p opcode and the given payload (unmasked: only
    //! clients mask).
    bool sendFrame(uint8_t opcode, const uint8_t *payload, size_t len);
    //! Best-effort CLOSE frame (unless one was already sent).
    void sendClose(uint16_t status);

    std::unique_ptr<SecureTransport> inner_;
    std::string subprotocol_;
    //! Raw bytes read from the socket but not yet parsed into frames
    std::vector<uint8_t> raw_;
    //! Payload of the binary message currently being assembled
    std::vector<uint8_t> message_;
    size_t messagePos_ = 0;
    bool closedByPeer_ = false;
    bool closeSent_ = false;
};

} // namespace MQTT

#endif
