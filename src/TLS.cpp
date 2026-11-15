#include "TLS.h"
#include "Log.h"
#include <cerrno>
#include <cstdint>
#include <cstring>
#include <poll.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/err.h>
#include <openssl/ssl.h>

namespace MQTT {

TlsContext::~TlsContext() {
    if (ctx_) {
        SSL_CTX_free(ctx_);
    }
}

std::shared_ptr<TlsContext> TlsContext::load(const std::string &certPath,
                                            const std::string &keyPath,
                                            bool verifyClients,
                                            const std::string &caPath,
                                            std::string *error) {
    if (certPath.empty() && keyPath.empty()) {
        // No TLS requested: plaintext broker
        return nullptr;
    }
    if (certPath.empty() || keyPath.empty()) {
        if (error) {
            *error = "both FLOWMQ_TLS_CERT and FLOWMQ_TLS_KEY must be set";
        }
        return nullptr;
    }
    // Validate the rest of the configuration before touching the files, so the
    // operator sees the missing variable rather than a file error
    if (verifyClients && caPath.empty()) {
        if (error) {
            *error = "FLOWMQ_TLS_CA is required to verify client certificates";
        }
        return nullptr;
    }

    SSL_CTX *ctx = SSL_CTX_new(TLS_server_method());
    if (!ctx) {
        if (error) {
            *error = "SSL_CTX_new failed";
        }
        return nullptr;
    }
    // Modern TLS only
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

    if (SSL_CTX_use_certificate_file(ctx, certPath.c_str(), SSL_FILETYPE_PEM) != 1) {
        if (error) {
            *error = "cannot load certificate file: " + certPath;
        }
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (SSL_CTX_use_PrivateKey_file(ctx, keyPath.c_str(), SSL_FILETYPE_PEM) != 1) {
        if (error) {
            *error = "cannot load private key file: " + keyPath;
        }
        SSL_CTX_free(ctx);
        return nullptr;
    }
    if (SSL_CTX_check_private_key(ctx) != 1) {
        if (error) {
            *error = "private key does not match the certificate";
        }
        SSL_CTX_free(ctx);
        return nullptr;
    }

    if (verifyClients) {
        if (SSL_CTX_load_verify_locations(ctx, caPath.c_str(), nullptr) != 1) {
            if (error) {
                *error = "cannot load CA file: " + caPath;
            }
            SSL_CTX_free(ctx);
            return nullptr;
        }
        SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, nullptr);
    }

    auto context = std::shared_ptr<TlsContext>(new TlsContext());
    context->ctx_ = ctx;
    return context;
}
SSL *TlsContext::createSsl() const {
    if (!ctx_) {
        return nullptr;
    }
    return SSL_new(ctx_);
}

SecureTransport::SecureTransport(int fd, std::shared_ptr<TlsContext> tls,
                                 int writeTimeoutMs)
    : fd_(fd), tls_(std::move(tls)), writeTimeoutMs_(writeTimeoutMs) {
    if (tls_ && tls_->enabled()) {
        ssl_ = tls_->createSsl();
        if (ssl_) {
            SSL_set_fd(ssl_, fd_);
        }
    } else {
        // Plaintext socket: it stays blocking, but a blocking ::send would
        // otherwise wait forever on a client that stops reading, pinning this
        // connection's thread and its queued messages. Bound the wait so a
        // stalled peer is dropped instead of held (a stalled send returns
        // EAGAIN, which writeAll already treats as a dropped connection).
        struct timeval tv;
        tv.tv_sec = writeTimeoutMs_ / 1000;
        tv.tv_usec = (writeTimeoutMs_ % 1000) * 1000;
        ::setsockopt(fd_, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    }
}

SecureTransport::~SecureTransport() {
    teardown();
}

bool SecureTransport::handshake(std::string *error) {
    if (!ssl_) {
        return true;
    }
    // The socket is blocking, so SSL_accept drives the handshake to completion
    // (including any renegotiation round trips) without spinning.
    int rc = SSL_accept(ssl_);
    if (rc == 1) {
        return true;
    }
    if (error) {
        unsigned long code = ERR_get_error();
        char buf[256];
        ERR_error_string_n(code, buf, sizeof(buf));
        *error = std::string("TLS handshake failed: ") + buf;
    }
    return false;
}

int SecureTransport::readable(int timeoutMs) {
    // A TLS session may already hold decrypted bytes in OpenSSL's buffer even
    // when the socket itself has nothing new to read. Report those first, or a
    // poll() would block on an already-complete record.
    if (ssl_ && SSL_pending(ssl_) > 0) {
        return 1;
    }
    struct pollfd pfd;
    pfd.fd = fd_;
    pfd.events = POLLIN;
    pfd.revents = 0;
    int ready = poll(&pfd, 1, timeoutMs);
    if (ready < 0) {
        return errno == EINTR ? -1 : 1; // let readSome surface a real error
    }
    return ready > 0 ? 1 : 0;
}

ssize_t SecureTransport::readSome(uint8_t *buf, size_t max) {
    if (!ssl_) {
        // Plain socket read
        return ::read(fd_, buf, max);
    }
    for (;;) {
        int n = SSL_read(ssl_, buf, static_cast<int>(max));
        if (n > 0) {
            return n;
        }
        int err = SSL_get_error(ssl_, n);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            // Spurious wake-up: wait for the socket and retry
            struct pollfd pfd;
            pfd.fd = fd_;
            pfd.events = (err == SSL_ERROR_WANT_READ) ? POLLIN : POLLOUT;
            pfd.revents = 0;
            int ready = poll(&pfd, 1, writeTimeoutMs_);
            if (ready <= 0) {
                return -1;
            }
            continue;
        }
        if (err == SSL_ERROR_ZERO_RETURN || err == SSL_ERROR_SYSCALL) {
            // Peer closed (clean TLS shutdown or TCP FIN)
            return 0;
        }
        return -1;
    }
}

bool SecureTransport::writeAll(const uint8_t *buf, size_t len) {
    size_t sent = 0;
    while (sent < len) {
        if (!ssl_) {
            ssize_t n = ::send(fd_, buf + sent, len - sent, MSG_NOSIGNAL);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                return false;
            }
            sent += static_cast<size_t>(n);
            continue;
        }
        int n = SSL_write(ssl_, buf + sent, static_cast<int>(len - sent));
        if (n > 0) {
            sent += static_cast<size_t>(n);
            continue;
        }
        int err = SSL_get_error(ssl_, n);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            struct pollfd pfd;
            pfd.fd = fd_;
            pfd.events = (err == SSL_ERROR_WANT_READ) ? POLLIN : POLLOUT;
            pfd.revents = 0;
            int ready = poll(&pfd, 1, writeTimeoutMs_);
            if (ready <= 0) {
                return false;
            }
            continue;
        }
        return false;
    }
    return true;
}

void SecureTransport::teardown() {
    if (!ssl_) {
        return;
    }
    // Best-effort close_notify; ignore errors, the fd is closed by the owner
    SSL_shutdown(ssl_);
    SSL_free(ssl_);
    ssl_ = nullptr;
}

} // namespace MQTT
