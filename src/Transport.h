#ifndef TRANSPORT_H
#define TRANSPORT_H
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>
#include <string>

namespace MQTT {

/*!
 * The byte transport of one network connection: an accepted TCP socket (plain
 * or TLS), a WebSocket carrying MQTT frames, or a QUIC stream. The connection
 * code only ever sees this interface, so a new wire transport plugs in without
 * touching the protocol state machine.
 *
 * The descriptor, when there is one, stays owned by the Connection: the
 * transport owns only its own protocol state (the SSL object, WebSocket
 * framing buffers, the QUIC stream handle).
 */
class ClientTransport {
public:
    virtual ~ClientTransport() = default;
    ClientTransport() = default;
    ClientTransport(const ClientTransport &) = delete;
    ClientTransport &operator=(const ClientTransport &) = delete;

    //! The socket descriptor for poll()-able transports, or -1 when there is
    //! none (a QUIC stream signals readiness through readable() itself).
    virtual int fd() const = 0;
    //! True when a protocol-level handshake must run before any MQTT byte is
    //! read (TLS, the WebSocket HTTP upgrade).
    virtual bool TLSEnabled() const = 0;
    //! Complete the transport-level handshake; a no-op when TLSEnabled() is
    //! false. Returns false (with a reason in *error) when the peer cannot be
    //! admitted.
    virtual bool handshake(std::string *error = nullptr) = 0;
    //! Identity of a verified peer certificate for authorisation, or empty.
    virtual std::string peerIdentity() const = 0;
    //! Whether the peer presented a certificate that passed verification.
    virtual bool peerVerified() const = 0;
    //! Wait until read data is available (or timeoutMs elapses).
    //! Returns 1 when data is ready, 0 on timeout, -1 to retry.
    virtual int readable(int timeoutMs) = 0;
    //! Read up to \p max bytes. Returns bytes read (>0), 0 on orderly close,
    //! -1 on error.
    virtual ssize_t readSome(uint8_t *buf, size_t max) = 0;
    //! Write all of \p len bytes. Returns false when the peer is gone.
    virtual bool writeAll(const uint8_t *buf, size_t len) = 0;
    //! Send whatever protocol-level goodbye the transport speaks and release
    //! its per-connection state.
    virtual void teardown() = 0;
};

} // namespace MQTT

#endif
