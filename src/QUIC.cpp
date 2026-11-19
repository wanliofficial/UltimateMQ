#include "QUIC.h"
#include "Broker.h"
#include "Connection.h"
#include "Log.h"
#include <msquic.h>
#include <openssl/evp.h>
#include <openssl/pem.h>
#include <openssl/x509.h>
#include <atomic>
#include <cstring>
#include <thread>
#include <unistd.h>
#include <sys/stat.h>
#include <vector>

namespace MQTT {

namespace {

//! Per-connection receive/send state shared between the msquic worker threads
//! (which append bytes) and the Connection thread (which drains them). Held by
//! shared_ptr so a callback that races a teardown still has valid state.
struct QUICStreamState {
    std::mutex mutex;
    std::condition_variable condition;
    std::vector<uint8_t> received;
    std::atomic<bool> closed{false};
    HQUIC stream = nullptr;
    HQUIC connection = nullptr;
};

//! The msquic API table, opened once per process. msquic expects the table to
//! outlive every handle; it stays resident until process exit, which is the
//! only point at which no handle can remain.
const QUIC_API_TABLE *quicApi() {
    static const QUIC_API_TABLE *table = [] {
        const QUIC_API_TABLE *loaded = nullptr;
        if (QUIC_FAILED(MsQuicOpen2(&loaded))) {
            return static_cast<const QUIC_API_TABLE *>(nullptr);
        }
        return loaded;
    }();
    return table;
}

//! A short-lived PEM file pair holding the ephemeral self-signed certificate.
//! The files must outlive msquic's use of them, hence the RAII wrapper.
struct EphemeralCertificate {
    std::string certPath;
    std::string keyPath;
    bool ok = false;

    EphemeralCertificate() {
        EVP_PKEY *pkey = nullptr;
        EVP_PKEY_CTX *ctx = EVP_PKEY_CTX_new_id(EVP_PKEY_RSA, nullptr);
        X509 *x509 = nullptr;
        FILE *certFile = nullptr;
        FILE *keyFile = nullptr;
        do {
            if (!ctx || EVP_PKEY_keygen_init(ctx) <= 0 ||
                EVP_PKEY_CTX_set_rsa_keygen_bits(ctx, 2048) <= 0 ||
                EVP_PKEY_keygen(ctx, &pkey) <= 0 || !pkey) {
                break;
            }
            x509 = X509_new();
            if (!x509) {
                break;
            }
            ASN1_INTEGER_set(X509_get_serialNumber(x509), 1);
            X509_gmtime_adj(X509_get_notBefore(x509), 0);
            X509_gmtime_adj(X509_get_notAfter(x509), 60 * 60 * 24 * 365);
            // Build the subject separately: OpenSSL 4 returns a const borrowed
            // pointer from X509_get_subject_name, so the name is assembled on
            // its own and then set as both subject and issuer.
            X509_NAME *name = X509_NAME_new();
            X509_NAME_add_entry_by_txt(name, "CN", MBSTRING_ASC,
                                       (const unsigned char *)"flowmq-quic", -1, -1, 0);
            X509_set_subject_name(x509, name);
            X509_set_issuer_name(x509, name);
            X509_NAME_free(name);
            X509_set_pubkey(x509, pkey);
            if (!X509_sign(x509, pkey, EVP_sha256())) {
                break;
            }
            certPath = "/tmp/flowmq-quic-cert-XXXXXX.pem";
            keyPath = "/tmp/flowmq-quic-key-XXXXXX.pem";
            std::vector<char> certBuf(certPath.begin(), certPath.end());
            std::vector<char> keyBuf(keyPath.begin(), keyPath.end());
            certBuf.push_back('\0');
            keyBuf.push_back('\0');
            int certFd = mkstemp(certBuf.data());
            int keyFd = mkstemp(keyBuf.data());
            if (certFd < 0 || keyFd < 0) {
                if (certFd >= 0) {
                    close(certFd);
                }
                if (keyFd >= 0) {
                    close(keyFd);
                }
                break;
            }
            close(certFd);
            close(keyFd);
            certPath.assign(certBuf.data());
            keyPath.assign(keyBuf.data());
            certFile = fopen(certPath.c_str(), "wb");
            keyFile = fopen(keyPath.c_str(), "wb");
            if (!certFile || !keyFile || PEM_write_X509(certFile, x509) <= 0 ||
                PEM_write_PrivateKey(keyFile, pkey, nullptr, nullptr, 0, nullptr,
                                     nullptr) <= 0) {
                break;
            }
            fclose(certFile);
            fclose(keyFile);
            certFile = nullptr;
            keyFile = nullptr;
            chmod(certPath.c_str(), 0600);
            chmod(keyPath.c_str(), 0600);
            ok = true;
        } while (false);
        if (certFile) {
            fclose(certFile);
        }
        if (keyFile) {
            fclose(keyFile);
        }
        if (x509) {
            X509_free(x509);
        }
        if (pkey) {
            EVP_PKEY_free(pkey);
        }
        if (ctx) {
            EVP_PKEY_CTX_free(ctx);
        }
    }

    ~EphemeralCertificate() {
        if (ok) {
            unlink(certPath.c_str());
            unlink(keyPath.c_str());
        }
    }
};

//! The listener-side context shared by every accept: which broker serves the
//! connection and where the serving-thread bookkeeping lives.
struct QUICAcceptContext {
    QUICListener::Impl *impl = nullptr;
    Broker *broker = nullptr;
};

struct QUICConnectionContext {
    std::shared_ptr<QUICStreamState> state;
    //! The serving thread is spawned when the client opens its first stream
    std::atomic<bool> accepted{false};
    QUICAcceptContext *accept = nullptr;
};

// The msquic handle lifecycle: StreamClose/ConnectionClose must be called
// from their SHUTDOWN_COMPLETE events, so the transport only requests
// shutdowns and the callbacks close the handles.
QUIC_STATUS QUIC_API streamCallback(HQUIC stream, void *context,
                                    QUIC_STREAM_EVENT *event);
QUIC_STATUS QUIC_API connectionCallback(HQUIC connection, void *context,
                                        QUIC_CONNECTION_EVENT *event);
void serveConnection(QUICConnectionContext *conn);

} // namespace

//! The transport side of one QUIC stream: the same ClientTransport shape as a
//! TCP socket, backed by the shared receive buffer above.
class QUICTransport : public ClientTransport {
public:
    explicit QUICTransport(std::shared_ptr<QUICStreamState> state)
        : state_(std::move(state)) {}
    ~QUICTransport() override {
        requestShutdown();
    }

    int fd() const override { return -1; }
    //! TLS runs at the QUIC connection level, so the Connection layer must
    //! not attempt any handshake of its own.
    bool TLSEnabled() const override { return false; }
    bool handshake(std::string *error = nullptr) override {
        (void)error;
        return true;
    }
    std::string peerIdentity() const override { return std::string(); }
    bool peerVerified() const override { return false; }

    int readable(int timeoutMs) override {
        std::unique_lock<std::mutex> lock(state_->mutex);
        const auto ready = [&] {
            return !state_->received.empty() ||
                   state_->closed.load(std::memory_order_acquire);
        };
        if (ready()) {
            return 1;
        }
        state_->condition.wait_for(lock, std::chrono::milliseconds(timeoutMs), ready);
        return ready() ? 1 : 0;
    }

    ssize_t readSome(uint8_t *buf, size_t max) override {
        // Blocking semantics, mirroring a TCP socket: the connection loop may
        // call readSome directly (Keep Alive 0 means it never polls readable
        // first), so this waits until data or an orderly close instead of
        // reporting an immediate empty read.
        std::unique_lock<std::mutex> lock(state_->mutex);
        while (state_->received.empty() &&
               !state_->closed.load(std::memory_order_acquire)) {
            state_->condition.wait_for(lock, std::chrono::milliseconds(100));
        }
        if (state_->received.empty()) {
            // An orderly shutdown with nothing left to read ends the stream
            return state_->closed.load(std::memory_order_acquire) ? 0 : -1;
        }
        const size_t take = state_->received.size() < max ? state_->received.size() : max;
        std::memcpy(buf, state_->received.data(), take);
        state_->received.erase(state_->received.begin(),
                               state_->received.begin() + static_cast<long>(take));
        return static_cast<ssize_t>(take);
    }

    bool writeAll(const uint8_t *buf, size_t len) override {
        if (len == 0) {
            return true;
        }
        if (state_->closed.load(std::memory_order_acquire)) {
            return false;
        }
        // StreamSend keeps the buffers alive until SEND_COMPLETE, so the
        // payload is copied and freed in the callback above.
        uint8_t *copy = new uint8_t[len];
        std::memcpy(copy, buf, len);
        QUIC_BUFFER quicBuffer;
        quicBuffer.Buffer = copy;
        quicBuffer.Length = static_cast<uint32_t>(len);
        if (QUIC_FAILED(quicApi()->StreamSend(state_->stream, &quicBuffer, 1,
                                                     QUIC_SEND_FLAG_NONE, copy))) {
            delete[] copy;
            state_->closed.store(true, std::memory_order_release);
            return false;
        }
        return true;
    }

    void teardown() override {
        requestShutdown();
    }

private:
    void requestShutdown() {
        if (state_->stream) {
            quicApi()->StreamShutdown(state_->stream, QUIC_STREAM_SHUTDOWN_FLAG_GRACEFUL,
                                      0);
            state_->stream = nullptr;
        }
        if (state_->connection) {
            quicApi()->ConnectionShutdown(state_->connection,
                                          QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
            state_->connection = nullptr;
        }
        state_->closed.store(true, std::memory_order_release);
        state_->condition.notify_all();
    }

    std::shared_ptr<QUICStreamState> state_;
};

struct QUICListener::Impl {
    Broker *broker = nullptr;
    QUIC_HANDLE *registration = nullptr;
    QUIC_HANDLE *configuration = nullptr;
    QUIC_HANDLE *listener = nullptr;
    //! Cap matching the TCP listener's default, so QUIC cannot sidestep the
    //! process-wide connection budget.
    static constexpr size_t MAX_QUIC_CONNECTIONS = 1024;
    std::atomic<bool> stopped{false};
    std::atomic<size_t> activeConnections{0};
    //! Live MQTT-over-QUIC connections, each on its own serving thread (the
    //! same thread-per-client model the TCP listener uses).
    struct Serving {
        std::thread thread;
        std::shared_ptr<std::atomic<bool>> finished;
    };
    std::mutex servingMutex;
    std::vector<Serving> serving;
    std::unique_ptr<EphemeralCertificate> ephemeralCertificate;
    std::unique_ptr<QUICAcceptContext> acceptContext;

    void reapFinished() {
        std::vector<Serving> finished;
        {
            std::lock_guard<std::mutex> lock(servingMutex);
            for (auto it = serving.begin(); it != serving.end();) {
                if (it->finished->load(std::memory_order_acquire)) {
                    finished.push_back(std::move(*it));
                    it = serving.erase(it);
                } else {
                    ++it;
                }
            }
        }
        for (auto &item : finished) {
            if (item.thread.joinable()) {
                item.thread.join();
            }
        }
    }
};

namespace {

//! Spawn the Connection that serves one accepted QUIC stream. Runs on an
//! msquic worker thread; the thread it spawns serves the client exactly like
//! a TCP accept does.
void serveConnection(QUICConnectionContext *conn) {
    QUICListener::Impl *impl = conn->accept->impl;
    Broker *broker = conn->accept->broker;
    if (impl->stopped.load(std::memory_order_acquire)) {
        // The listener is going away: refuse the session instead of spawning
        // a thread that would outlive the shutdown bookkeeping.
        quicApi()->ConnectionShutdown(conn->state->connection,
                                      QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
        return;
    }
    // Thread-per-client, exactly like the TCP accept loop. The serving thread
    // owns the Connection, which owns the transport and its receive state.
    auto finished = std::make_shared<std::atomic<bool>>(false);
    std::lock_guard<std::mutex> lock(impl->servingMutex);
    impl->serving.push_back({std::thread([broker, state = conn->state, finished]() {
        auto transport = std::make_unique<QUICTransport>(state);
        auto connection = std::make_shared<Connection>(-1, broker, std::move(transport));
        connection->run();
        finished->store(true, std::memory_order_release);
    }), finished});
}

QUIC_STATUS QUIC_API connectionCallback(HQUIC connection, void *context,
                                        QUIC_CONNECTION_EVENT *event) {
    auto *conn = static_cast<QUICConnectionContext *>(context);
    switch (event->Type) {
    case QUIC_CONNECTION_EVENT_PEER_STREAM_STARTED:
        if (!conn->accepted.exchange(true)) {
            // The first client bidirectional stream carries the MQTT session.
            conn->state->stream = event->PEER_STREAM_STARTED.Stream;
            conn->state->connection = connection;
            quicApi()->SetCallbackHandler(event->PEER_STREAM_STARTED.Stream,
                                          reinterpret_cast<void *>(streamCallback),
                                          conn->state.get());
            serveConnection(conn);
        } else {
            // Exactly one MQTT stream per connection; extra streams are
            // aborted rather than half-served.
            quicApi()->StreamShutdown(event->PEER_STREAM_STARTED.Stream,
                                      QUIC_STREAM_SHUTDOWN_FLAG_ABORT_SEND |
                                          QUIC_STREAM_SHUTDOWN_FLAG_ABORT_RECEIVE,
                                      QUIC_STATUS_PROTOCOL_ERROR);
        }
        break;
    case QUIC_CONNECTION_EVENT_SHUTDOWN_COMPLETE:
        conn->state->closed.store(true, std::memory_order_release);
        conn->state->condition.notify_all();
        // The accept count drops here, on the worker thread, before the
        // context is freed.
        conn->accept->impl->activeConnections.fetch_sub(1, std::memory_order_relaxed);
        // msquic requires the connection handle to be closed from this event;
        // after this the callback context is gone for good.
        quicApi()->ConnectionClose(connection);
        delete conn;
        break;
    default:
        break;
    }
    return QUIC_STATUS_SUCCESS;
}

QUIC_STATUS QUIC_API streamCallback(HQUIC stream, void *context,
                                    QUIC_STREAM_EVENT *event) {
    auto *state = static_cast<QUICStreamState *>(context);
    (void)stream;
    switch (event->Type) {
    case QUIC_STREAM_EVENT_RECEIVE: {
        std::lock_guard<std::mutex> lock(state->mutex);
        for (uint32_t i = 0; i < event->RECEIVE.BufferCount; ++i) {
            const QUIC_BUFFER &buffer = event->RECEIVE.Buffers[i];
            state->received.insert(state->received.end(), buffer.Buffer,
                                   buffer.Buffer + buffer.Length);
        }
        break;
    }
    case QUIC_STREAM_EVENT_SEND_COMPLETE:
        // The payload handed to StreamSend is copied per send and released
        // here, when msquic no longer references it.
        delete[] static_cast<uint8_t *>(event->SEND_COMPLETE.ClientContext);
        break;
    case QUIC_STREAM_EVENT_SHUTDOWN_COMPLETE:
        state->closed.store(true, std::memory_order_release);
        // Same rule as the connection handle: close from the shutdown event.
        quicApi()->StreamClose(stream);
        break;
    default:
        break;
    }
    state->condition.notify_all();
    return QUIC_STATUS_SUCCESS;
}

QUIC_STATUS QUIC_API listenerCallback(HQUIC /*listener*/, void *context,
                                      QUIC_LISTENER_EVENT *event) {
    auto *accept = static_cast<QUICAcceptContext *>(context);
    switch (event->Type) {
    case QUIC_LISTENER_EVENT_NEW_CONNECTION: {
        if (accept->impl->activeConnections.load(std::memory_order_relaxed) >=
            QUICListener::Impl::MAX_QUIC_CONNECTIONS) {
            logError("too many QUIC connections, refusing client");
            return QUIC_STATUS_CONNECTION_REFUSED;
        }
        auto conn = new QUICConnectionContext();
        conn->state = std::make_shared<QUICStreamState>();
        conn->accept = accept;
        quicApi()->SetCallbackHandler(event->NEW_CONNECTION.Connection,
                                      reinterpret_cast<void *>(connectionCallback), conn);
        if (QUIC_FAILED(quicApi()->ConnectionSetConfiguration(
                event->NEW_CONNECTION.Connection, accept->impl->configuration))) {
            // The shutdown completion fires on the worker thread, closes the
            // handle and deletes the context installed above.
            quicApi()->ConnectionShutdown(event->NEW_CONNECTION.Connection,
                                          QUIC_CONNECTION_SHUTDOWN_FLAG_NONE, 0);
            return QUIC_STATUS_CONNECTION_REFUSED;
        }
        accept->impl->activeConnections.fetch_add(1, std::memory_order_relaxed);
        return QUIC_STATUS_SUCCESS;
    }
    default:
        return QUIC_STATUS_SUCCESS;
    }
}

} // namespace

QUICListener::QUICListener(Broker *broker, uint16_t port)
    : impl(std::make_unique<Impl>()), broker_(broker), port_(port) {
    impl->broker = broker;
    impl->acceptContext = std::make_unique<QUICAcceptContext>(
        QUICAcceptContext{impl.get(), broker});
}

QUICListener::~QUICListener() {
    stop();
}

void QUICListener::start() {
    const QUIC_API_TABLE *api = quicApi();
    if (!api) {
        throw std::runtime_error("failed to open the msquic API table");
    }
    QUIC_REGISTRATION_CONFIG registrationConfig{"flowmq",
                                                QUIC_EXECUTION_PROFILE_LOW_LATENCY};
    if (QUIC_FAILED(api->RegistrationOpen(&registrationConfig,
                                                 &impl->registration))) {
        throw std::runtime_error("failed to open the msquic registration");
    }

    // QUIC always speaks TLS 1.3. Reuse the broker certificate when one is
    // configured; otherwise generate a short-lived self-signed pair so
    // development clients (verification disabled) can connect immediately.
    QUIC_CREDENTIAL_CONFIG credentialConfig{};
    QUIC_CERTIFICATE_FILE certFile{};
    const char *certPath = getenv("FLOWMQ_TLS_CERT");
    const char *keyPath = getenv("FLOWMQ_TLS_KEY");
    if (certPath && keyPath && *certPath && *keyPath) {
        certFile.CertificateFile = certPath;
        certFile.PrivateKeyFile = keyPath;
        credentialConfig.CertificateFile = &certFile;
        credentialConfig.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
        credentialConfig.Flags = QUIC_CREDENTIAL_FLAG_NONE;
    } else {
        impl->ephemeralCertificate = std::make_unique<EphemeralCertificate>();
        if (!impl->ephemeralCertificate->ok) {
            impl->ephemeralCertificate.reset();
            api->RegistrationClose(impl->registration);
            throw std::runtime_error("failed to generate an ephemeral QUIC certificate");
        }
        logInfo("QUIC: no FLOWMQ_TLS_CERT/KEY, using an ephemeral self-signed certificate");
        certFile.CertificateFile = impl->ephemeralCertificate->certPath.c_str();
        certFile.PrivateKeyFile = impl->ephemeralCertificate->keyPath.c_str();
        credentialConfig.CertificateFile = &certFile;
        credentialConfig.Type = QUIC_CREDENTIAL_TYPE_CERTIFICATE_FILE;
        credentialConfig.Flags = QUIC_CREDENTIAL_FLAG_NONE;
    }

    QUIC_SETTINGS settings{};
    settings.IdleTimeoutMs = 60000;
    settings.IsSet.IdleTimeoutMs = TRUE;
    settings.PeerBidiStreamCount = 4;
    settings.IsSet.PeerBidiStreamCount = TRUE;

    // QUIC_BUFFER.Buffer is a mutable pointer; the string literal is readonly
    // storage, so copy it into a mutable buffer instead of casting the const
    // away (a cast away const would let msquic write into the literal).
    auto alpnBytes = std::vector<uint8_t>(
        reinterpret_cast<const uint8_t *>(QUIC_ALPN_MQTT),
        reinterpret_cast<const uint8_t *>(QUIC_ALPN_MQTT) + strlen(QUIC_ALPN_MQTT));
    QUIC_BUFFER alpn{static_cast<uint32_t>(alpnBytes.size()), alpnBytes.data()};
    if (QUIC_FAILED(api->ConfigurationOpen(impl->registration, &alpn, 1,
                                                  &settings, sizeof(settings),
                                                  &credentialConfig,
                                                  &impl->configuration))) {
        impl->ephemeralCertificate.reset();
        api->RegistrationClose(impl->registration);
        throw std::runtime_error("failed to open the msquic configuration");
    }

    if (QUIC_FAILED(api->ListenerOpen(impl->registration, listenerCallback,
                                             impl->acceptContext.get(),
                                             &impl->listener))) {
        api->ConfigurationClose(impl->configuration);
        impl->configuration = nullptr;
        impl->ephemeralCertificate.reset();
        api->RegistrationClose(impl->registration);
        throw std::runtime_error("failed to open the msquic listener");
    }
    QUIC_ADDR address{};
    QuicAddrSetFamily(&address, QUIC_ADDRESS_FAMILY_INET);
    QuicAddrSetPort(&address, port_);
    if (QUIC_FAILED(api->ListenerStart(impl->listener, &alpn, 1, &address))) {
        quicApi()->ListenerClose(impl->listener);
        impl->listener = nullptr;
        api->ConfigurationClose(impl->configuration);
        impl->configuration = nullptr;
        impl->ephemeralCertificate.reset();
        api->RegistrationClose(impl->registration);
        throw std::runtime_error(std::string("failed to start the QUIC listener on port ") +
                                 std::to_string(port_));
    }
    logInfo("MQTT over QUIC listener on UDP port %u", port_);
}

void QUICListener::stop() {
    if (!impl || !quicApi()) {
        return;
    }
    impl->stopped.store(true, std::memory_order_release);
    if (impl->listener) {
        quicApi()->ListenerClose(impl->listener);
        impl->listener = nullptr;
    }
    if (impl->configuration) {
        quicApi()->ConfigurationClose(impl->configuration);
        impl->configuration = nullptr;
    }
    // Join the serving threads first: they own their Connection objects, and
    // tearing those down requests the connection shutdowns whose completion
    // callbacks close the last msquic handles before the registration goes.
    impl->reapFinished();
    for (;;) {
        std::lock_guard<std::mutex> lock(impl->servingMutex);
        if (impl->serving.empty()) {
            break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
        impl->reapFinished();
    }
    if (impl->registration) {
        quicApi()->RegistrationClose(impl->registration);
        impl->registration = nullptr;
    }
    impl->ephemeralCertificate.reset();
}

} // namespace MQTT
