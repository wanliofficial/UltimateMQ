#ifndef TLS_H
#define TLS_H
#pragma once

#include <memory>
#include <string>
#include <sys/time.h>

struct ssl_ctx_st;
struct ssl_st;
using SSL_CTX = struct ssl_ctx_st;
using SSL = struct ssl_st;

namespace MQTT {

/*!
 * Server-side TLS configuration, built once at startup from a certificate and
 * a private key (and optionally a CA to verify client certificates).
 *
 * The context is immutable after load and shared by every connection, so it
 * owns no per-connection state. When no certificate is configured the broker
 * stays in plaintext mode and this object is empty.
 */
class TLSContext {
public:
    //! Load the certificate/key. Returns an empty context (evaluate with
    //! enabled()) when no certificate path is given. On a real failure returns
    //! nullptr and describes the problem in *error.
    static std::shared_ptr<TLSContext> load(const std::string &certPath,
                                            const std::string &keyPath,
                                            bool verifyClients = false,
                                            const std::string &caPath = std::string(),
                                            std::string *error = nullptr);
    ~TLSContext();
    TLSContext(const TLSContext &) = delete;
    TLSContext &operator=(const TLSContext &) = delete;

    //! True when a certificate was loaded (the listener speaks TLS)
    bool enabled() const { return ctx_ != nullptr; }

    //! Create a new SSL object for one connection, or nullptr when disabled.
    //! Ownership passes to the caller.
    SSL *createSsl() const;

private:
    TLSContext() = default;
    SSL_CTX *ctx_ = nullptr;
};

/*!
 * The byte transport of one connection: the accepted socket, optionally
 * wrapped in TLS. It hides the differences between a plain socket and a TLS
 * session so that the connection code never has to call OpenSSL directly.
 *
 * The transport does not own the socket descriptor: the connection still
 * opens and closes it. It owns only the SSL object (if any).
 */
class SecureTransport {
public:
    //! How long a single write may make no progress before the connection is
    //! treated as stalled and dropped. Bounds how long one slow or absent reader
    //! can pin a connection thread on a blocking socket. Tunable via
    //! FLOWMQ_WRITE_TIMEOUT_MS; this is the default.
    static constexpr int WRITE_STALL_TIMEOUT_MS = 5000;

    //! \p TLS may be null for a plaintext connection. \p writeTimeoutMs bounds a
    //! single stalled write before the connection is dropped.
    SecureTransport(int fd, std::shared_ptr<TLSContext> TLS,
                    int writeTimeoutMs = WRITE_STALL_TIMEOUT_MS);
    ~SecureTransport();
    SecureTransport(const SecureTransport &) = delete;
    SecureTransport &operator=(const SecureTransport &) = delete;

    int fd() const { return fd_; }
    bool TLSEnabled() const { return ssl_ != nullptr; }

    //! Perform the TLS handshake (a no-op for a plaintext connection)
    bool handshake(std::string *error = nullptr);

    //! Wait until the peer has data ready (or timeoutMs elapses).
    //! Returns 1 when data is ready, 0 on timeout, -1 to retry (EINTR).
    int readable(int timeoutMs);

    //! Read up to \p max bytes. Returns bytes read (>0), 0 on orderly close,
    //! -1 on error.
    ssize_t readSome(uint8_t *buf, size_t max);

    //! Write all of \p len bytes. Returns false if the peer is gone.
    bool writeAll(const uint8_t *buf, size_t len);

    //! Send a TLS close_notify and release the SSL object (a no-op if plaintext)
    void teardown();

private:
    int fd_;
    std::shared_ptr<TLSContext> TLS_;
    SSL *ssl_ = nullptr;
    //! Per-connection stalled-write bound (milliseconds)
    int writeTimeoutMs_;
};

} // namespace MQTT

#endif
