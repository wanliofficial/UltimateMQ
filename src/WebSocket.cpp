#include "WebSocket.h"
#include "Env.h"
#include "SHA1.h"
#include "Log.h"
#include <cctype>
#include <chrono>
#include <cstdlib>
#include <cstring>

namespace MQTT {

namespace {

//! GUID from RFC 6455 §1.3, concatenated with the client's Sec-WebSocket-Key
//! to form the digest that the handshake answer must carry.
constexpr const char *WEBSOCKET_GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11";
//! Bound on the HTTP request a client may spend on the handshake, so an
//! attacker cannot pin a connection thread with an endless header block.
constexpr size_t MAX_HANDSHAKE_BYTES = 8 * 1024;
//! Bound on one assembled WebSocket message. MQTT packets are bounded by the
//! Frame layer anyway; this cap only stops a single frame from growing the
//! buffer before that limit can be applied.
constexpr size_t MAX_MESSAGE_BYTES = 16 * 1024 * 1024;
//! How long the client may take to finish the HTTP upgrade.
constexpr int HANDSHAKE_TIMEOUT_MS = 10 * 1000;

std::string toLower(std::string s) {
    for (char &c : s) {
        c = static_cast<char>(std::tolower(static_cast<unsigned char>(c)));
    }
    return s;
}

//! Case-insensitive find of a comma-separated token in a header value
//! ("Connection: keep-alive, Upgrade").
bool headerContains(const std::string &value, const std::string &token) {
    size_t start = 0;
    while (start <= value.size()) {
        size_t comma = value.find(',', start);
        size_t end = comma == std::string::npos ? value.size() : comma;
        size_t a = start;
        size_t b = end;
        while (a < b && std::isspace(static_cast<unsigned char>(value[a]))) {
            ++a;
        }
        while (b > a && std::isspace(static_cast<unsigned char>(value[b - 1]))) {
            --b;
        }
        if (toLower(value.substr(a, b - a)) == toLower(token)) {
            return true;
        }
        if (comma == std::string::npos) {
            break;
        }
        start = comma + 1;
    }
    return false;
}

std::string base64(const uint8_t *data, size_t length) {
    static const char *alphabet =
        "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    std::string out;
    out.reserve((length + 2) / 3 * 4);
    size_t i = 0;
    while (i + 3 <= length) {
        uint32_t group = (data[i] << 16) | (data[i + 1] << 8) | data[i + 2];
        out.push_back(alphabet[(group >> 18) & 0x3F]);
        out.push_back(alphabet[(group >> 12) & 0x3F]);
        out.push_back(alphabet[(group >> 6) & 0x3F]);
        out.push_back(alphabet[group & 0x3F]);
        i += 3;
    }
    if (length - i == 1) {
        uint32_t group = data[i] << 16;
        out.push_back(alphabet[(group >> 18) & 0x3F]);
        out.push_back(alphabet[(group >> 12) & 0x3F]);
        out.push_back('=');
        out.push_back('=');
    } else if (length - i == 2) {
        uint32_t group = (data[i] << 16) | (data[i + 1] << 8);
        out.push_back(alphabet[(group >> 18) & 0x3F]);
        out.push_back(alphabet[(group >> 12) & 0x3F]);
        out.push_back(alphabet[(group >> 6) & 0x3F]);
        out.push_back('=');
    }
    return out;
}

} // namespace

WebSocketTransport::WebSocketTransport(std::unique_ptr<SecureTransport> inner)
    : inner_(std::move(inner)) {}

bool WebSocketTransport::handshake(std::string *error) {
    // WSS: the TLS handshake runs first, then the HTTP upgrade rides on top of
    // it. A plaintext WebSocket does the HTTP upgrade on the bare socket.
    if (inner_->TLSEnabled() && !inner_->handshake(error)) {
        return false;
    }
    // Read the request headers byte by byte until the blank line, so a huge
    // header block cannot blow the buffer.
    //
    // HANDSHAKE_TIMEOUT_MS bounds the WHOLE upgrade, not each byte. A per-byte
    // budget would let a slowloris dribble one byte just under every timeout and
    // hold the connection for MAX_HANDSHAKE_BYTES * HANDSHAKE_TIMEOUT_MS (~22h
    // with the defaults), which is a cheap way to pin every handler thread. A
    // single absolute deadline makes the entire handshake cost at most that one
    // window no matter how the bytes are spread.
    const auto deadline = std::chrono::steady_clock::now() +
                          std::chrono::milliseconds(HANDSHAKE_TIMEOUT_MS);
    std::string request;
    while (request.find("\r\n\r\n") == std::string::npos) {
        if (request.size() >= MAX_HANDSHAKE_BYTES) {
            std::string response =
                "HTTP/1.1 431 Request Header Fields Too Large\r\n"
                "Content-Length: 0\r\n\r\n";
            inner_->writeAll(reinterpret_cast<const uint8_t *>(response.data()),
                             response.size());
            if (error) {
                *error = "websocket handshake headers too large";
            }
            return false;
        }
        // Refuse to wait past the shared deadline; readable() also takes a
        // timeout, so hand it only the time actually left.
        const auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
            deadline - std::chrono::steady_clock::now());
        if (remaining.count() <= 0) {
            if (error) {
                *error = "websocket handshake timed out";
            }
            return false;
        }
        uint8_t byte = 0;
        if (inner_->readable(static_cast<int>(remaining.count())) != 1) {
            if (error) {
                *error = "websocket handshake timed out";
            }
            return false;
        }
        ssize_t got = inner_->readSome(&byte, 1);
        if (got <= 0) {
            if (error) {
                *error = "peer closed during websocket handshake";
            }
            return false;
        }
        request.push_back(static_cast<char>(byte));
    }

    // Request line and headers, case-insensitive per RFC 9110.
    std::string method = request.substr(0, request.find(' '));
    std::string upgradeHeader;
    std::string connectionHeader;
    std::string keyHeader;
    std::string versionHeader;
    std::string protocolHeader;
    size_t lineStart = request.find("\r\n") + 2;
    size_t headerEnd = request.find("\r\n\r\n");
    while (lineStart < headerEnd) {
        size_t lineEnd = request.find("\r\n", lineStart);
        if (lineEnd == std::string::npos || lineEnd > headerEnd) {
            break;
        }
        std::string line = request.substr(lineStart, lineEnd - lineStart);
        size_t colon = line.find(':');
        if (colon != std::string::npos) {
            std::string name = toLower(line.substr(0, colon));
            size_t valueStart = colon + 1;
            while (valueStart < line.size() &&
                   std::isspace(static_cast<unsigned char>(line[valueStart]))) {
                ++valueStart;
            }
            std::string value = line.substr(valueStart);
            if (name == "upgrade") {
                upgradeHeader = value;
            } else if (name == "connection") {
                connectionHeader = value;
            } else if (name == "sec-websocket-key") {
                keyHeader = value;
            } else if (name == "sec-websocket-version") {
                versionHeader = value;
            } else if (name == "sec-websocket-protocol") {
                protocolHeader = value;
            }
        }
        lineStart = lineEnd + 2;
    }

    // The binding requires an upgrade request; anything else gets a plain HTTP
    // error so a misdirected browser tab sees something intelligible.
    // RFC 6455 §4.2.1 item 3 requires the |Upgrade| header to contain the value
    // "websocket" (case-insensitive). Like |Connection|, |Upgrade| is a
    // comma-separated list of protocol tokens (RFC 9110 §7.8), so match the
    // token rather than a substring: "notwebsocket"/"xwebsocketx" only *contain*
    // the letters and must not be upgraded.
    if (method != "GET" || !headerContains(upgradeHeader, "websocket") ||
        !headerContains(connectionHeader, "upgrade") || keyHeader.empty()) {
        // Derive Content-Length from the body: a hardcoded count drifts from the
        // literal the moment the text changes, and an off-by-one desyncs the
        // HTTP framing (the client waits for a byte that never comes, or reads
        // into the next record).
        const std::string body = "websocket required.\n";
        const std::string response =
            "HTTP/1.1 400 Bad Request\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: " + std::to_string(body.size()) + "\r\n"
            "\r\n" + body;
        inner_->writeAll(reinterpret_cast<const uint8_t *>(response.data()),
                         response.size());
        if (error) {
            *error = "not a websocket upgrade request";
        }
        return false;
    }
    if (versionHeader != "13") {
        std::string response =
            "HTTP/1.1 426 Upgrade Required\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "Content-Length: 0\r\n\r\n";
        inner_->writeAll(reinterpret_cast<const uint8_t *>(response.data()),
                         response.size());
        if (error) {
            *error = "websocket version 13 required";
        }
        return false;
    }

    // RFC 6455 §4.2.1: accept = base64(SHA1(key + GUID)).
    const std::string keyAndGuid = keyHeader + WEBSOCKET_GUID;
    std::array<uint8_t, 20> digest = SHA1::digest(
        reinterpret_cast<const uint8_t *>(keyAndGuid.data()), keyAndGuid.size());
    std::string accept = base64(digest.data(), digest.size());

    // The MQTT-over-WebSocket binding's subprotocol name is "mqtt"; echo it
    // when the client offered it so both sides agree on raw MQTT in binary
    // frames. With FLOWMQ_WS_REQUIRE_SUBPROTOCOL set, a client that did not
    // offer "mqtt" is refused outright, as the binding demands; without the
    // knob such clients are still accepted, as before.
    if (headerContains(protocolHeader, WEBSOCKET_SUBPROTOCOL)) {
        subprotocol_ = WEBSOCKET_SUBPROTOCOL;
    } else if (Env::isEnabled("FLOWMQ_WS_REQUIRE_SUBPROTOCOL")) {
        std::string response =
            "HTTP/1.1 400 Bad Request\r\n"
            "Content-Type: text/plain\r\n"
            "Content-Length: 30\r\n"
            "\r\n"
            "mqtt subprotocol is required.\n";
        inner_->writeAll(reinterpret_cast<const uint8_t *>(response.data()),
                         response.size());
        if (error) {
            *error = "websocket client did not offer the mqtt subprotocol";
        }
        return false;
    }

    std::string response =
        "HTTP/1.1 101 Switching Protocols\r\n"
        "Upgrade: websocket\r\n"
        "Connection: Upgrade\r\n"
        "Sec-WebSocket-Accept: " + accept + "\r\n";
    if (!subprotocol_.empty()) {
        response += std::string("Sec-WebSocket-Protocol: ") + subprotocol_ + "\r\n";
    }
    response += "\r\n";
    if (!inner_->writeAll(reinterpret_cast<const uint8_t *>(response.data()),
                          response.size())) {
        if (error) {
            *error = "failed to send the websocket handshake answer";
        }
        return false;
    }
    logInfo("websocket connection upgraded (subprotocol %s)",
            subprotocol_.empty() ? "none" : subprotocol_.c_str());
    established_ = true;
    return true;
}

int WebSocketTransport::readable(int timeoutMs) {
    // Data already assembled counts as ready even when the socket itself is
    // quiet; ditto an orderly close the caller has not drained yet.
    if (messagePos_ < message_.size() || closedByPeer_) {
        return 1;
    }
    return inner_->readable(timeoutMs);
}

bool WebSocketTransport::fillRawBuffer() {
    uint8_t chunk[4096];
    ssize_t got = inner_->readSome(chunk, sizeof(chunk));
    if (got <= 0) {
        closedByPeer_ = true;
        return false;
    }
    raw_.insert(raw_.end(), chunk, chunk + got);
    return true;
}

std::vector<uint8_t> WebSocketTransport::buildFrame(uint8_t opcode, const uint8_t *payload,
                                                    size_t len) {
    std::vector<uint8_t> frame;
    frame.push_back(0x80 | opcode); // FIN set: every message goes out whole
    if (len < 126) {
        frame.push_back(static_cast<uint8_t>(len));
    } else if (len <= 0xFFFF) {
        frame.push_back(126);
        frame.push_back(static_cast<uint8_t>(len >> 8));
        frame.push_back(static_cast<uint8_t>(len & 0xFF));
    } else {
        frame.push_back(127);
        for (int i = 7; i >= 0; --i) {
            frame.push_back(static_cast<uint8_t>(len >> (8 * i)));
        }
    }
    frame.insert(frame.end(), payload, payload + len);
    return frame;
}

bool WebSocketTransport::sendFrame(uint8_t opcode, const uint8_t *payload, size_t len) {
    const std::vector<uint8_t> frame = buildFrame(opcode, payload, len);
    return inner_->writeAll(frame.data(), frame.size());
}

int WebSocketTransport::flushPending() {
    size_t sent = 0;
    const size_t total = pendingFrame_.size();
    while (sent < total) {
        const ssize_t n = inner_->writeSome(pendingFrame_.data() + sent, total - sent);
        if (n < 0) {
            pendingFrame_.clear();
            return -1; // peer gone
        }
        if (n == 0) {
            // Would-block: drop the bytes already sent so the next call resumes
            // exactly where the socket stopped (the header is not re-added).
            pendingFrame_.erase(pendingFrame_.begin(),
                                pendingFrame_.begin() + static_cast<long>(sent));
            return 0;
        }
        sent += static_cast<size_t>(n);
    }
    pendingFrame_.clear();
    return 1;
}

bool WebSocketTransport::sendFrameNonBlocking(uint8_t opcode, const uint8_t *payload,
                                              size_t len) {
    // Queue the whole frame (header + payload) after any tail still pending from
    // an earlier would-block, then flush as much as the socket takes. The header
    // is built exactly once, so a resumed flush never re-prepends it.
    std::vector<uint8_t> frame = buildFrame(opcode, payload, len);
    pendingFrame_.insert(pendingFrame_.end(), frame.begin(), frame.end());
    return flushPending() != -1; // 0 (would-block) is fine: the tail is retained
}

void WebSocketTransport::sendClose(uint16_t status) {
    if (closeSent_) {
        return;
    }
    closeSent_ = true;
    uint8_t payload[2] = {static_cast<uint8_t>(status >> 8),
                          static_cast<uint8_t>(status & 0xFF)};
    const std::vector<uint8_t> closeFrame = buildFrame(0x08, payload, sizeof(payload));
    if (pendingFrame_.empty()) {
        // Nothing stuck ahead of the CLOSE: block to get it out (teardown path).
        inner_->writeAll(closeFrame.data(), closeFrame.size());
        return;
    }
    // A tail is still pending from an earlier would-block: the CLOSE must not
    // jump ahead of those bytes. Append it and flush the lot non-blockingly;
    // teardown follows regardless, so a short flush here is acceptable.
    pendingFrame_.insert(pendingFrame_.end(), closeFrame.begin(), closeFrame.end());
    flushPending();
}

bool WebSocketTransport::nextFrame() {
    for (;;) {
        // A frame needs at least two bytes before its length can be decoded
        while (raw_.size() < 2) {
            if (!fillRawBuffer()) {
                return false;
            }
        }
        const uint8_t fin = raw_[0] & 0x80;
        const uint8_t opcode = raw_[0] & 0x0F;
        // RFC 6455 §5.2: RSV1/RSV2/RSV3 MUST be 0 unless an extension is
        // negotiated that defines them. The MQTT-over-WebSocket binding
        // negotiates no extensions, so any set RSV bit is a protocol error and
        // the connection must be failed. Checked before the length/mask are
        // buffered so a malformed frame cannot make us read further bytes first.
        if (raw_[0] & 0x70) {
            logDebug("websocket: reserved RSV bit set, closing (1002)");
            sendClose(1002);
            closedByPeer_ = true;
            return false;
        }
        // RFC 6455 §5.2: an unknown or reserved opcode (0x3-0x7 non-control,
        // 0xB-0xF control) MUST fail the connection. The defined opcodes are
        // 0x0-0x2 and 0x8-0xA; 0x1 (text) is rejected further down as it is not
        // part of the MQTT binary framing, but the reserved range is a framing
        // error and is refused here rather than silently dropped.
        if (opcode == 0x03 || opcode == 0x04 || opcode == 0x05 || opcode == 0x06 ||
            opcode == 0x07 || opcode == 0x0B || opcode == 0x0C || opcode == 0x0D ||
            opcode == 0x0E || opcode == 0x0F) {
            logDebug("websocket: reserved opcode 0x%02x, closing (1002)", opcode);
            sendClose(1002);
            closedByPeer_ = true;
            return false;
        }
        const bool masked = (raw_[1] & 0x80) != 0;
        uint64_t length = raw_[1] & 0x7F;
        size_t offset = 2;
        if (length == 126) {
            while (raw_.size() < offset + 2) {
                if (!fillRawBuffer()) {
                    return false;
                }
            }
            length = (static_cast<uint64_t>(raw_[offset]) << 8) | raw_[offset + 1];
            offset += 2;
        } else if (length == 127) {
            while (raw_.size() < offset + 8) {
                if (!fillRawBuffer()) {
                    return false;
                }
            }
            // RFC 6455 §5.2: a 127-prefixed length is an 8-byte unsigned
            // integer whose most significant bit MUST be 0. A set MSB is a
            // framing (protocol) error, so reject it as such with 1002 before
            // decoding; otherwise it would surface only as an over-large length
            // and be answered 1009, i.e. for the wrong reason.
            if (raw_[offset] & 0x80) {
                logDebug("websocket: 64-bit frame length has the MSB set, closing (1002)");
                sendClose(1002);
                closedByPeer_ = true;
                return false;
            }
            length = 0;
            for (int i = 0; i < 8; ++i) {
                length = (length << 8) | raw_[offset + i];
            }
            offset += 8;
        }
        // Only clients mask, and only clients may leave the mask bit set.
        if (!masked) {
            logDebug("websocket: unmasked client frame, closing (1002)");
            sendClose(1002);
            closedByPeer_ = true;
            return false;
        }
        // RFC 6455 §5.5.1: control frames (0x08 CLOSE, 0x09 PING, 0x0A PONG) must
        // not be fragmented, so a control frame with the FIN bit clear is a
        // protocol error rather than the start of a control-frame sequence.
        const bool isControl = opcode == 0x08 || opcode == 0x09 || opcode == 0x0A;
        if (isControl && !fin) {
            logDebug("websocket: fragmented control frame, closing (1002)");
            sendClose(1002);
            closedByPeer_ = true;
            return false;
        }
        // RFC 6455 §5.5: every control frame MUST have a Payload length of 125
        // bytes or less. A longer control frame (CLOSE/PING/PONG) is a protocol
        // error and must fail the connection, not be buffered and answered.
        if (isControl && length > 125) {
            logDebug("websocket: control frame with %llu-byte payload over 125, closing (1002)",
                     static_cast<unsigned long long>(length));
            sendClose(1002);
            closedByPeer_ = true;
            return false;
        }
        // Enforce the per-message byte cap before buffering the payload, so a
        // peer cannot make the broker wait for (and buffer) an advertised length
        // larger than the cap by dribbling the bytes in. Checking it only after
        // the fill loop below left MAX_MESSAGE_BYTES unenforced.
        if (length > MAX_MESSAGE_BYTES ||
            message_.size() + length > MAX_MESSAGE_BYTES) {
            logDebug("websocket: message over the %zu-byte cap, closing (1009)",
                     MAX_MESSAGE_BYTES);
            sendClose(1009);
            closedByPeer_ = true;
            return false;
        }
        while (raw_.size() < offset + 4) {
            if (!fillRawBuffer()) {
                return false;
            }
        }
        uint8_t mask[4];
        std::memcpy(mask, raw_.data() + offset, 4);
        offset += 4;
        while (raw_.size() < offset + length) {
            if (!fillRawBuffer()) {
                return false;
            }
        }

        const uint8_t *payload = raw_.data() + offset;
        // Control frames may be interleaved between fragments and never continue.
        if (opcode == 0x08) { // CLOSE: echo it and signal the orderly end
            sendClose(1000);
            closedByPeer_ = true;
            return false;
        }
        if (opcode == 0x09) { // PING: answer with PONG carrying the same body
            // The payload arrives masked and must go out unmasked (the server
            // never masks), so copy it through the XOR first.
            std::vector<uint8_t> unmasked;
            unmasked.reserve(static_cast<size_t>(length));
            for (uint64_t i = 0; i < length; ++i) {
                unmasked.push_back(payload[i] ^ mask[i % 4]);
            }
            sendFrame(0x0A, unmasked.data(), unmasked.size());
        }
        // PONG (0x0A) and any other control opcode needs no reply. Data frames
        // assemble into message_: a new BINARY message resets it, CONTINUATION
        // appends, and the caller gets data only once FIN ends the message.
        if (opcode == 0x02 || opcode == 0x00) {
            if (opcode == 0x02) {
                // RFC 6455 §5.4: a new data frame must not arrive while a
                // fragmented message is still open; only a CONTINUATION may
                // continue it. Accepting this would silently drop the partial
                // message already in message_ and start a competing one.
                if (fragmenting_) {
                    logDebug("websocket: new data frame during a fragmented message, closing (1002)");
                    sendClose(1002);
                    closedByPeer_ = true;
                    return false;
                }
                message_.clear();
                messagePos_ = 0;
                fragmenting_ = !fin;
            } else {
                // CONTINUATION with no open fragment is a protocol error
                // (§5.4): it has nothing to continue.
                if (!fragmenting_) {
                    logDebug("websocket: continuation frame with no open message, closing (1002)");
                    sendClose(1002);
                    closedByPeer_ = true;
                    return false;
                }
                if (fin) {
                    fragmenting_ = false;
                }
            }
            for (uint64_t i = 0; i < length; ++i) {
                message_.push_back(payload[i] ^ mask[i % 4]);
            }
            if (fin) {
                raw_.erase(raw_.begin(), raw_.begin() + static_cast<long>(offset + length));
                return true;
            }
        } else if (opcode == 0x01) { // TEXT: not part of the MQTT binding
            logDebug("websocket: text frame rejected (1003)");
            sendClose(1003);
            closedByPeer_ = true;
            return false;
        }
        raw_.erase(raw_.begin(), raw_.begin() + static_cast<long>(offset + length));
        if (closedByPeer_) {
            return false;
        }
    }
}

ssize_t WebSocketTransport::readSome(uint8_t *buf, size_t max) {
    while (messagePos_ >= message_.size()) {
        if (closedByPeer_) {
            return 0; // orderly close (a CLOSE frame came in and was answered)
        }
        if (!nextFrame()) {
            return 0;
        }
    }
    const size_t available = message_.size() - messagePos_;
    const size_t take = available < max ? available : max;
    std::memcpy(buf, message_.data() + messagePos_, take);
    messagePos_ += take;
    if (messagePos_ == message_.size()) {
        // Drain fully consumed messages so the next read parses fresh frames
        message_.clear();
        messagePos_ = 0;
    }
    return static_cast<ssize_t>(take);
}

bool WebSocketTransport::writeAll(const uint8_t *buf, size_t len) {
    if (closedByPeer_ && closeSent_) {
        return false;
    }
    // Normally empty here (writeAll is the unbuffered path), but a tail could
    // linger if the write buffer was toggled off mid-stream; push it out first so
    // the new frame does not jump ahead of older bytes.
    if (!pendingFrame_.empty()) {
        while (!pendingFrame_.empty()) {
            if (!inner_->writeAll(pendingFrame_.data(), pendingFrame_.size())) {
                return false;
            }
            pendingFrame_.clear();
        }
    }
    return sendFrame(0x02, buf, len); // one BINARY frame per write
}

ssize_t WebSocketTransport::writeSome(const uint8_t *buf, size_t len) {
    // Non-blocking path for FLOWMQ_WRITE_BUFFER. WebSocket framing means a write
    // unit is one whole frame (header + payload): the caller re-sends a remainder
    // verbatim, so writeSome must never report a partial payload count or the
    // header would be re-prepended and corrupt the stream. Contract:
    //   len  -> the unit is accepted (whole frame flushed, or its tail retained
    //           in pendingFrame_ and resumed by a later call);
    //   0    -> a previously retained tail is still blocked, so this unit was not
    //           consumed at all and must be retried;
    //   -1   -> the peer is gone.
    if (closedByPeer_ && closeSent_) {
        return -1;
    }
    // Flush any tail left by an earlier would-block before framing a new unit.
    if (!pendingFrame_.empty()) {
        const int flushed = flushPending();
        if (flushed < 0) {
            return -1;
        }
        if (flushed == 0) {
            return 0; // still blocked: do not consume this unit
        }
    }
    return sendFrameNonBlocking(0x02, buf, len) ? static_cast<ssize_t>(len) : -1;
}

void WebSocketTransport::teardown() {
    // Only a peer that completed the upgrade is in a WebSocket session; sending
    // CLOSE to one still stuck at the HTTP stage would append WebSocket framing
    // to the HTTP error response already written.
    if (established_) {
        sendClose(1000);
    }
    inner_->teardown();
}

} // namespace MQTT
