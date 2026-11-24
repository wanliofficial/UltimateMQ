#include "TLS.h"
#include "Log.h"
#include <cerrno>
#include <chrono>
#include <cstdint>
#include <cstring>
#include <vector>
#include <poll.h>
#include <pthread.h>
#include <signal.h>
#include <fcntl.h>
#include <sys/socket.h>
#include <unistd.h>
#include <openssl/err.h>
#include <openssl/ssl.h>
#include <openssl/x509.h>
#include <openssl/x509v3.h>

namespace MQTT {

namespace {

//! Upper bound on a single TLS handshake. A client that completes the TCP
//! handshake but never sends its ClientHello (or stalls mid-handshake) must not
//! pin a serving thread and its descriptor forever: that is a slowloris that
//! costs the attacker only a bare TCP connect and can exhaust every connection
//! thread. The plaintext path bounds an idle peer with CONNECT_TIMEOUT_SECONDS;
//! this is the TLS-equivalent bound, applied to the handshake itself.
constexpr int HANDSHAKE_TIMEOUT_MS = 10 * 1000;

//! The SIGPIPE mask as a reusable constant.
sigset_t &sigPipeMask() {
    static sigset_t mask = [] {
        sigset_t set;
        sigemptyset(&set);
        sigaddset(&set, SIGPIPE);
        return set;
    }();
    return mask;
}

/*!
 * Stops the calling thread from ever being killed by SIGPIPE.
 *
 * The plaintext write path passes MSG_NOSIGNAL, but a TLS session goes through
 * OpenSSL's socket BIO, which issues a plain write(2). Writing to a peer that
 * has vanished (an abortive close, an RST) therefore raises SIGPIPE, and with
 * the default disposition that terminates the entire broker: a single client
 * disappearing while a message was in flight for it was enough to kill the
 * process. Blocking the signal for this thread turns such a write into the
 * EPIPE the caller already handles and leaves every other thread unaffected.
 *
 * The mask is installed once per thread (a connection thread does many TLS
 * writes, and a mask switch per write would cost two syscalls each time). A
 * blocked SIGPIPE merely becomes pending - standard signals do not queue, so at
 * most one instance can ever be outstanding - and is discarded when the thread
 * exits, so leaving it blocked for the thread's lifetime is safe.
 */
void suppressSigPipe() {
    thread_local const bool installed = [] {
        return ::pthread_sigmask(SIG_BLOCK, &sigPipeMask(), nullptr) == 0;
    }();
    (void)installed;
}

} // namespace

TLSContext::~TLSContext() {
    if (ctx_) {
        SSL_CTX_free(ctx_);
    }
}

std::shared_ptr<TLSContext> TLSContext::load(const std::string &certPath,
                                            const std::string &keyPath,
                                            bool verifyClients,
                                            const std::string &caPath,
                                            std::string *error) {
    if (certPath.empty() && keyPath.empty()) {
        // No certificate configured. This is a plaintext broker ONLY when TLS
        // was not otherwise requested. If the operator asked for client
        // verification or supplied a CA, they intended mutual TLS; silently
        // falling back to plaintext would run unencrypted with no client auth
        // while the configuration reads as if it were on, so refuse instead.
        if (verifyClients || !caPath.empty()) {
            if (error) {
                *error = "FLOWMQ_TLS_VERIFY_CLIENTS/FLOWMQ_TLS_CA require "
                         "FLOWMQ_TLS_CERT and FLOWMQ_TLS_KEY (refusing to run "
                         "plaintext)";
            }
            return nullptr;
        }
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

    // Load the certificate as a CHAIN, not a single certificate: an operator who
    // points FLOWMQ_TLS_CERT at a PEM bundle (leaf first, then its intermediate
    // CA certificates) expects the intermediate chain to be sent so clients that
    // do not already hold it can build the path to their trust anchor.
    // SSL_CTX_use_certificate_file loads only the FIRST certificate in the file,
    // silently dropping the intermediates, so such a client fails to verify the
    // server. _chain_file also accepts a single-certificate file unchanged, so
    // the common case is unaffected.
    if (SSL_CTX_use_certificate_chain_file(ctx, certPath.c_str()) != 1) {
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

    auto context = std::shared_ptr<TLSContext>(new TLSContext());
    context->ctx_ = ctx;
    return context;
}
SSL *TLSContext::createSsl() const {
    if (!ctx_) {
        return nullptr;
    }
    return SSL_new(ctx_);
}

SecureTransport::SecureTransport(int fd, std::shared_ptr<TLSContext> TLS,
                                 int writeTimeoutMs)
    : fd_(fd), TLS_(std::move(TLS)), writeTimeoutMs_(writeTimeoutMs) {
    if (TLS_ && TLS_->enabled()) {
        ssl_ = TLS_->createSsl();
        if (ssl_) {
            SSL_set_fd(ssl_, fd_);
        }
    }
    // Bound the send wait for BOTH the plaintext and the TLS path. A blocking
    // ::send or SSL_write waits forever on a client that stops reading, pinning
    // this connection's thread and its queued messages. For TLS the socket is
    // blocking again after the handshake, so a stalled SSL_write blocks inside
    // write(2) and never returns WANT_WRITE - the poll() fallback in writeAll()
    // is unreachable without this. SO_SNDTIMEO turns both into EAGAIN, which
    // writeAll() already treats as a dropped connection.
    {
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
    // The socket is normally blocking, so a bare SSL_accept blocks until the
    // peer completes the handshake - forever if the peer connects and then
    // sends nothing. Switch the socket to non-blocking for the duration of the
    // handshake and drive SSL_accept with a bounded poll() loop, then restore
    // blocking mode (readSome/writeAll below poll for their own timeouts and do
    // not expect a non-blocking descriptor).
    //
    // SSL_accept writes the server hello, so a peer that vanished mid-handshake
    // must not raise SIGPIPE either: suppress it for this thread first.
    suppressSigPipe();

    const int originalFlags = ::fcntl(fd_, F_GETFL, 0);
    if (originalFlags < 0 || ::fcntl(fd_, F_SETFL, originalFlags | O_NONBLOCK) < 0) {
        if (error) {
            *error = std::string("TLS handshake failed: cannot configure the socket: ") +
                     std::strerror(errno);
        }
        return false;
    }

    const auto deadline =
        std::chrono::steady_clock::now() + std::chrono::milliseconds(HANDSHAKE_TIMEOUT_MS);
    int rc = -1;
    bool timedOut = false;
    for (;;) {
        ERR_clear_error();
        rc = SSL_accept(ssl_);
        if (rc == 1) {
            break;
        }
        const int sslError = SSL_get_error(ssl_, rc);
        if (sslError != SSL_ERROR_WANT_READ && sslError != SSL_ERROR_WANT_WRITE) {
            break; // a real handshake failure (or a closed peer)
        }
        // The handshake needs more input/output: wait for it, but never past the
        // deadline so a silent or dribbling peer cannot hold the thread.
        const auto now = std::chrono::steady_clock::now();
        if (now >= deadline) {
            timedOut = true;
            break;
        }
        int sliceMs = static_cast<int>(
            std::chrono::duration_cast<std::chrono::milliseconds>(deadline - now).count());
        if (sliceMs < 1) {
            sliceMs = 1;
        }
        struct pollfd pfd;
        pfd.fd = fd_;
        pfd.events = (sslError == SSL_ERROR_WANT_READ) ? POLLIN : POLLOUT;
        pfd.revents = 0;
        int ready = ::poll(&pfd, 1, sliceMs);
        if (ready < 0) {
            if (errno == EINTR) {
                continue;
            }
            break; // poll failed: surface it below
        }
        if (ready == 0) {
            timedOut = true;
            break;
        }
    }

    // Restore the original (blocking) mode before anything else can use the fd.
    ::fcntl(fd_, F_SETFL, originalFlags);

    if (rc == 1) {
        return true;
    }
    if (error) {
        if (timedOut) {
            *error = "TLS handshake timed out";
        } else {
            unsigned long code = ERR_get_error();
            char buf[256];
            ERR_error_string_n(code, buf, sizeof(buf));
            *error = std::string("TLS handshake failed: ") + buf;
        }
    }
    return false;
}

bool SecureTransport::peerVerified() const {
    if (!ssl_) {
        return false;
    }
    // SSL_get_verify_result reports X509_V_OK only when the peer presented a
    // certificate that chained to our CA; with SSL_VERIFY_NONE an absent
    // certificate is also reported as OK, so require a certificate too.
    //
    // SSL_get_peer_certificate is a macro for SSL_get1_peer_certificate in
    // OpenSSL 3.0: it *increments* the reference count and hands the caller a
    // reference it owns. Dropping that reference without X509_free() leaked one
    // certificate per call, so a broker running with FLOWMQ_TLS_CERT_AS_USERNAME
    // (which asks this question on every CONNECT) grew by one X509 per client
    // connection. SSL_get0_peer_certificate borrows the session's reference,
    // which is all this check needs.
    X509 *cert = SSL_get0_peer_certificate(ssl_);
    if (cert == nullptr) {
        return false;
    }
    return SSL_get_verify_result(ssl_) == X509_V_OK;
}

std::string SecureTransport::peerIdentity() const {
    if (!peerVerified()) {
        return std::string();
    }
    // SSL_get0_peer_certificate returns a borrowed pointer: the session keeps
    // the only reference and frees it when the SSL object is freed, so this must
    // not be X509_free()d here.
    X509 *cert = SSL_get0_peer_certificate(ssl_);
    if (!cert) {
        return std::string();
    }
    std::string identity;
    // Prefer the Common Name (CN) of the subject, the conventional client
    // identity. Fall back to the whole subject DN if there is no CN.
    // OpenSSL 4 returns a const X509_NAME*; all calls below take const.
    const X509_NAME *subject = X509_get_subject_name(cert);
    if (subject) {
        int cn = X509_NAME_get_text_by_NID(subject, NID_commonName,
                                           nullptr, 0);
        if (cn > 0) {
            std::vector<char> buf(static_cast<size_t>(cn) + 1);
            if (X509_NAME_get_text_by_NID(subject, NID_commonName, buf.data(),
                                          static_cast<int>(buf.size())) > 0) {
                identity = buf.data();
            }
        }
        if (identity.empty()) {
            char *dn = X509_NAME_oneline(subject, nullptr, 0);
            if (dn) {
                identity = dn;
                OPENSSL_free(dn);
            }
        }
    }
    return identity;
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
        // SSL_read can write as well: alerts, renegotiation, and session
        // tickets all go out through the same socket BIO.
        suppressSigPipe();
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
        // OpenSSL writes through a plain write(2), so a departed peer must be
        // turned into EPIPE rather than SIGPIPE (see suppressSigPipe()).
        suppressSigPipe();
        int n = SSL_write(ssl_, buf + sent, static_cast<int>(len - sent));
        if (n > 0) {
            sent += static_cast<size_t>(n);
            continue;
        }
        int err = SSL_get_error(ssl_, n);
        if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
            // On a blocking socket SO_SNDTIMEO already bounded the preceding
            // write(2): SSL_write only returns WANT_WRITE after that timeout
            // elapsed with the peer still not reading, so the connection is
            // stalled and must be dropped. Polling POLLOUT here would wait a
            // second writeTimeoutMs and double the stall deadline; instead
            // return at once so the drop lands within a single timeout (matching
            // the plaintext path, where ::send + SO_SNDTIMEO returns EAGAIN and
            // writeAll reports failure immediately). A non-blocking socket has no
            // SO_SNDTIMEO, so it keeps the poll: WANT_WRITE then means "try again
            // once writable" rather than "timed out".
            const bool nonBlocking = (::fcntl(fd_, F_GETFL, 0) & O_NONBLOCK) != 0;
            if (err == SSL_ERROR_WANT_WRITE && !nonBlocking) {
                return false;
            }
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

ssize_t SecureTransport::writeSome(const uint8_t *buf, size_t len) {
    if (len == 0) {
        return 0;
    }
    if (!ssl_) {
        // Plaintext: one non-blocking send. EAGAIN/EWOULDBLOCK means the kernel
        // send buffer is full (a slow reader), so report would-block and let the
        // caller buffer the unit. EINTR is retried. A dead peer is -1.
        for (;;) {
            ssize_t n = ::send(fd_, buf, len, MSG_NOSIGNAL | MSG_DONTWAIT);
            if (n < 0) {
                if (errno == EINTR) {
                    continue;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) {
                    return 0;
                }
                return -1;
            }
            return n;
        }
    }
    // TLS: a single SSL_write emits exactly one record. WANT_WRITE means the
    // record did not fit yet: report would-block (0) and let the caller buffer
    // the unit and retry on POLLOUT.
    suppressSigPipe();
    int n = SSL_write(ssl_, buf, static_cast<int>(len));
    if (n > 0) {
        return n;
    }
    int err = SSL_get_error(ssl_, n);
    if (err == SSL_ERROR_WANT_READ || err == SSL_ERROR_WANT_WRITE) {
        return 0;
    }
    return -1;
}

void SecureTransport::teardown() {
    if (!ssl_) {
        return;
    }
    // Best-effort close_notify; the fd is closed by the owner. Guarded for the
    // same reason as writeAll(): the peer may already be gone.
    suppressSigPipe();
    SSL_shutdown(ssl_);
    SSL_free(ssl_);
    ssl_ = nullptr;
}

} // namespace MQTT
