#include "AMQPConnection.h"
#include "Log.h"

#include <algorithm>
#include <cerrno>
#include <charconv>
#include <limits>
#include <thread>
#include <sys/socket.h>
#include <unistd.h>

namespace AMQP {
namespace {
constexpr uint16_t REPLY_NO_ROUTE = 312;
constexpr uint16_t REPLY_NO_CONSUMERS = 313;
constexpr uint16_t CONNECTION_FORCED = 320;
constexpr uint16_t INVALID_PATH = 402;
constexpr uint16_t FRAME_ERROR = 501;
constexpr uint16_t CHANNEL_ERROR = 503;
constexpr uint16_t COMMAND_INVALID = 540;
constexpr uint16_t NOT_IMPLEMENTED = 540;
constexpr uint16_t ACCESS_REFUSED = 403;
constexpr uint16_t NOT_FOUND = 404;
constexpr uint16_t RESOURCE_LOCKED = 405;
constexpr uint16_t PRECONDITION_FAILED = 406;
constexpr uint16_t UNEXPECTED_FRAME = 505;
constexpr uint16_t RESOURCE_ERROR = 506;
constexpr uint8_t PROTOCOL_HEADER[] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};

bool bit(Reader& reader, const char* name) {
    uint8_t value = reader.bits();
    if (value > 1) {
        throw std::runtime_error(std::string("invalid ") + name + " bit");
    }
    return value != 0;
}

uint8_t packedBits(Reader& reader, unsigned used, const char* name) {
    uint8_t value = reader.bits();
    uint8_t mask = static_cast<uint8_t>(0xffu << used);
    if (value & mask) {
        throw std::runtime_error(std::string("reserved ") + name + " bits are set");
    }
    return value;
}

bool flag(uint8_t bits, unsigned shift) {
    return (bits & (1u << shift)) != 0;
}

std::chrono::milliseconds heartbeatTimeout(uint16_t value) {
    return value == 0 ? std::chrono::milliseconds(250)
                      : std::chrono::milliseconds(static_cast<int>(value) * 1000 / 2);
}

std::vector<uint8_t> closeArguments(uint16_t code, const std::string& text,
                                    uint16_t classID, uint16_t methodID) {
    Writer writer;
    writer.u16(code).shortString(text).u16(classID).u16(methodID);
    return writer.data();
}

struct Properties {
    std::string userID;
    std::shared_ptr<FieldTable> headers;
    std::optional<std::chrono::steady_clock::time_point> expiration;
};

Properties parseProperties(const std::vector<uint8_t>& bytes) {
    if (bytes.empty()) return {};
    Reader reader(bytes);
    uint16_t flags = reader.u16();
    // basic.content: cluster-id occupies bit 2; only bits 1 and 0 are reserved.
    if ((flags & 0x0003u) != 0) {
        throw std::runtime_error("reserved basic property flag is set");
    }
    Properties result;
    // basic.content: MIME and application identifiers are short strings;
    // only headers is a table and timestamp is an eight-octet value.
    if (flags & 0x8000) reader.shortString();
    if (flags & 0x4000) reader.shortString();
    if (flags & 0x2000) result.headers = reader.table();
    if (flags & 0x1000) reader.u8();
    if (flags & 0x0800) reader.u8();
    if (flags & 0x0400) reader.shortString();
    if (flags & 0x0200) reader.shortString();
    if (flags & 0x0100) {
        std::string expiration = reader.shortString();
        if (!expiration.empty()) {
            unsigned seconds = 0;
            auto parsed = std::from_chars(expiration.data(),
                                          expiration.data() + expiration.size(), seconds);
            if (parsed.ec != std::errc() ||
                parsed.ptr != expiration.data() + expiration.size()) {
                throw std::runtime_error("invalid message expiration");
            }
            result.expiration = std::chrono::steady_clock::now() +
                                std::chrono::seconds(seconds);
        }
    }
    if (flags & 0x0080) reader.shortString();
    if (flags & 0x0040) reader.u64();
    if (flags & 0x0020) reader.shortString();
    if (flags & 0x0010) result.userID = reader.shortString();
    if (flags & 0x0008) reader.shortString();
    if (flags & 0x0004) reader.shortString();
    reader.requireEnd();
    return result;
}

} // namespace

Connection::Connection(int socketFD, Broker* brokerHandle,
                       std::shared_ptr<MQTT::TLSContext> TLSContext,
                       int writeTimeout, size_t maxMessageSize)
    : socket(socketFD), broker(brokerHandle), TLS(std::move(TLSContext)),
      transport(std::make_unique<MQTT::SecureTransport>(socket, TLS, writeTimeout)),
      writeTimeoutMs(writeTimeout), maxMessageBytes(maxMessageSize),
      lastActivity(std::chrono::steady_clock::now()),
      lastWrite(std::chrono::steady_clock::now()) {}

Connection::~Connection() {
    close();
    cleanup();
    transport->teardown();
    if (socket >= 0) {
        ::close(socket);
        socket = -1;
    }
}

void Connection::run() {
    try {
        if (!handshake()) {
            close();
            return;
        }
        while (!closed.load()) {
            auto frame = readFrame();
            if (!frame) {
                continue;
            }
            handleFrame(*frame);
        }
    } catch (const ConnectionError& error) {
        if (!closed.load()) {
            if (opened) {
                (void)sendClose(error.code, error.what());
                awaitClose();
            } else {
                close();
            }
        }
    } catch (const std::exception& error) {
        MQTT::logError("AMQP connection error: %s", error.what());
        if (!closed.load()) {
            if (opened) {
                (void)sendClose(CONNECTION_FORCED, "internal connection error");
                awaitClose();
            } else {
                close();
            }
        }
    }
    close();
    cleanup();
}

void Connection::awaitClose() {
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        if (awaitingClose) return;
        awaitingClose = true;
        closeDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
    }
    while (!closed.load()) {
        if (std::chrono::steady_clock::now() >= closeDeadline) {
            close();
            return;
        }
        try {
            auto frame = readFrame();
            if (!frame) {
                if (closed.load()) return;
                continue;
            }
            handleFrame(*frame);
        } catch (const std::exception&) {
            close();
            return;
        }
    }
}

void Connection::finishChannelClose(uint16_t channel) {
    broker->acknowledge(shared_from_this(), channel,
                        [this, channel]() { return allUnacked(channel); }, true, id);
    broker->removeChannel(shared_from_this(), channel, id);
    std::lock_guard<std::mutex> lock(stateMutex);
    channels.erase(channel);
    pendingPublishes.erase(channel);
    closingChannels.erase(channel);
}

void Connection::close() {
    if (closed.exchange(true)) {
        return;
    }
    if (socket >= 0) {
        ::shutdown(socket, SHUT_RDWR);
    }
}

bool Connection::handshake() {
    if (transport->TLSEnabled()) {
        std::string error;
        if (!transport->handshake(&error)) {
            return false;
        }
    }
    uint8_t header[sizeof(PROTOCOL_HEADER)];
    if (!readExact(header, sizeof(header), std::chrono::seconds(10))) {
        return false;
    }
    if (std::memcmp(header, PROTOCOL_HEADER, sizeof(header)) != 0) {
        const uint8_t invalidHeaderReply[] = {'A', 'M', 'Q', 'P', 0, 0, 9, 1};
        std::lock_guard<std::mutex> lock(writeMutex);
        (void)transport->writeAll(invalidHeaderReply, sizeof(invalidHeaderReply));
        return false;
    }
    Writer servers;
    servers.u8(0).u8(9);
    auto serverProperties = std::make_shared<FieldTable>();
    auto capabilities = std::make_shared<FieldTable>();
    capabilities->emplace("publisher_confirms", true);
    capabilities->emplace("basic.nack", true);
    serverProperties->emplace("capabilities", capabilities);
    serverProperties->emplace("product", std::string("FlowMQ"));
    servers.table(serverProperties);
    servers.longString("PLAIN EXTERNAL");
    servers.longString("en_US");
    if (!sendMethod(CLASS_CONNECTION, 10, servers.data(), 0)) {
        return false;
    }
    phase = Phase::START;
    return true;
}

bool Connection::readExact(uint8_t* data, size_t size,
                           std::chrono::milliseconds timeout) {
    auto deadline = std::chrono::steady_clock::now() + timeout;
    size_t offset = 0;
    while (offset < size && !closed.load()) {
        auto remaining = std::chrono::duration_cast<std::chrono::milliseconds>(
                             deadline - std::chrono::steady_clock::now())
                             .count();
        if (remaining <= 0) {
            return false;
        }
        int ready = transport->readable(static_cast<int>(std::min<int64_t>(remaining, 250)));
        if (ready < 0 && errno == EINTR) {
            continue;
        }
        if (ready < 0 || ready == 0) {
            continue;
        }
        ssize_t count = transport->readSome(data + offset, size - offset);
        if (count > 0) {
            offset += static_cast<size_t>(count);
            lastActivity = std::chrono::steady_clock::now();
        } else if (count == 0) {
            return false;
        } else if (errno != EINTR && errno != EAGAIN && errno != EWOULDBLOCK) {
            return false;
        }
    }
    return offset == size;
}

std::optional<Frame> Connection::readFrame() {
    auto now = std::chrono::steady_clock::now();
    if (phase == Phase::OPEN && (awaitingClose || !closingChannels.empty()) &&
        now >= closeDeadline) {
        close();
        return std::nullopt;
    }
    if (phase == Phase::OPEN && heartbeat != 0) {
        auto timeout = heartbeatTimeout(heartbeat);
        if (now - lastActivity >= timeout) {
            throw ConnectionError(CONNECTION_FORCED, "heartbeat timeout");
        }
        if (now - lastWrite >= std::chrono::seconds(heartbeat)) {
            if (!writeFrame(encodeFrame(FRAME_HEARTBEAT, 0, {}, frameMax))) {
                return std::nullopt;
            }
        }
    } else if (phase != Phase::OPEN && phase != Phase::CLOSED &&
               now - lastActivity >= std::chrono::seconds(10)) {
        throw ConnectionError(CONNECTION_FORCED, "connection negotiation timeout");
    }
    int ready = transport->readable(250);
    if (ready < 0 && errno == EINTR) {
        return std::nullopt;
    }
    if (ready < 0) {
        close();
        return std::nullopt;
    }
    if (ready == 0) {
        return std::nullopt;
    }
    uint8_t header[7];
    if (!readExact(header, sizeof(header), std::chrono::seconds(10))) {
        close();
        return std::nullopt;
    }
    Frame frame;
    frame.type = header[0];
    frame.channel = static_cast<uint16_t>((static_cast<uint16_t>(header[1]) << 8) | header[2]);
    const uint32_t payloadSize = (static_cast<uint32_t>(header[3]) << 24) |
                                 (static_cast<uint32_t>(header[4]) << 16) |
                                 (static_cast<uint32_t>(header[5]) << 8) | header[6];
    // connection.tune-ok.frame-max: count all eight non-payload octets.
    if (frameMax < 8 || payloadSize > frameMax - 8) {
        throw ConnectionError(FRAME_ERROR, "frame exceeds negotiated maximum");
    }
    frame.payload.resize(payloadSize);
    if (frame.type < FRAME_METHOD || frame.type > FRAME_BODY) {
        if (frame.type != FRAME_HEARTBEAT) {
            throw ConnectionError(FRAME_ERROR, "invalid frame type");
        }
    }
    if (frame.type == FRAME_HEARTBEAT && (frame.channel != 0 || !frame.payload.empty())) {
        throw ConnectionError(FRAME_ERROR, "invalid heartbeat frame");
    }
    if (!frame.payload.empty() &&
        !readExact(frame.payload.data(), frame.payload.size(), std::chrono::seconds(10))) {
        close();
        return std::nullopt;
    }
    uint8_t end = 0;
    if (!readExact(&end, 1, std::chrono::seconds(10))) {
        close();
        return std::nullopt;
    }
    if (end != FRAME_END) {
        throw ConnectionError(FRAME_ERROR, "invalid frame end marker");
    }
    return frame;
}

bool Connection::writeFrame(const std::vector<uint8_t>& frame) {
    std::lock_guard<std::mutex> lock(writeMutex);
    if (!transport->writeAll(frame.data(), frame.size())) {
        close();
        return false;
    }
    lastWrite = std::chrono::steady_clock::now();
    return true;
}

bool Connection::sendMethod(uint16_t classID, uint16_t methodID,
                            const std::vector<uint8_t>& arguments,
                            uint16_t channel) {
    return writeFrame(encodeMethod(classID, methodID, arguments, channel, frameMax));
}

bool Connection::sendContent(uint16_t channel, uint16_t classID,
                             uint16_t methodID,
                             const std::vector<uint8_t>& arguments,
                             const Message& message) {
    std::lock_guard<std::mutex> lock(writeMutex);
    if (methodID != 0) {
        auto method = encodeMethod(classID, methodID, arguments, channel, frameMax);
        if (!transport->writeAll(method.data(), method.size())) {
            close();
            return false;
        }
    }
    auto header = encodeContentHeader(channel, classID, methodID, message.body.size(),
                                      message.properties, frameMax);
    if (!transport->writeAll(header.data(), header.size())) {
        close();
        return false;
    }
    size_t offset = 0;
    while (offset < message.body.size()) {
        size_t amount = std::min<size_t>(message.body.size() - offset,
                                         frameMax > 8 ? frameMax - 8 : 0);
        if (amount == 0) {
            close();
            return false;
        }
        auto body = encodeBody(channel, message.body.data() + offset, amount, frameMax);
        if (!transport->writeAll(body.data(), body.size())) {
            close();
            return false;
        }
        offset += amount;
    }
    lastWrite = std::chrono::steady_clock::now();
    return true;
}

bool Connection::sendClose(uint16_t code, const std::string& text,
                           uint16_t classID, uint16_t methodID,
                           uint16_t channel) {
    return sendMethod(CLASS_CONNECTION, 50,
                      closeArguments(code, text, classID, methodID), channel);
}

void Connection::handleFrame(const Frame& frame) {
    if (closed.load()) {
        return;
    }
    // connection.close.stability: discard everything except Close/Close-Ok
    // after sending Close. channel.close.stability does the same per channel.
    if (frame.type == FRAME_HEARTBEAT) {
        lastActivity = std::chrono::steady_clock::now();
        return;
    }
    bool finishClosingChannel = false;
    bool discardClosingChannel = false;
    bool connectionClosing = false;
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        connectionClosing = awaitingClose;
        if (awaitingClose) {
            if (frame.type == FRAME_METHOD && frame.channel == 0) {
                Method closeMethod = decodeMethod(frame);
                if (closeMethod.classID == CLASS_CONNECTION && closeMethod.methodID == 51) {
                    close();
                    return;
                }
                if (closeMethod.classID == CLASS_CONNECTION && closeMethod.methodID == 50) {
                    discardClosingChannel = true;
                }
            }
            if (!discardClosingChannel) return;
        }
        if (closingChannels.count(frame.channel)) {
            discardClosingChannel = true;
            if (frame.type == FRAME_METHOD) {
                Method closeMethod = decodeMethod(frame);
                if (closeMethod.classID == CLASS_CHANNEL && closeMethod.methodID == 41) {
                    finishClosingChannel = true;
                }
            }
        }
    }
    if (discardClosingChannel) {
        if (frame.type == FRAME_METHOD) {
            Method closeMethod = decodeMethod(frame);
            if (connectionClosing && closeMethod.classID == CLASS_CONNECTION &&
                closeMethod.methodID == 50) {
                (void)sendMethod(CLASS_CONNECTION, 51, {}, 0);
                close();
            } else if (!connectionClosing && closeMethod.classID == CLASS_CHANNEL &&
                       closeMethod.methodID == 40) {
                (void)sendMethod(CLASS_CHANNEL, 41, {}, frame.channel);
                finishClosingChannel = true;
            }
        }
        if (finishClosingChannel) finishChannelClose(frame.channel);
        return;
    }
    if (frame.type == FRAME_METHOD) {
        Method method = decodeMethod(frame);
        if (method.classID == CLASS_CONNECTION) {
            if (frame.channel != 0) {
                throw ConnectionError(CHANNEL_ERROR, "connection method on nonzero channel");
            }
            handleConnection(method);
        } else if (frame.channel == 0) {
            throw ConnectionError(CHANNEL_ERROR, "channel method on channel zero");
        } else if (!opened || phase != Phase::OPEN) {
            throw ConnectionError(COMMAND_INVALID, "connection is not open");
        } else {
            handleMethod(method, frame.channel);
        }
        return;
    }
    if (frame.type == FRAME_HEADER) {
        handleHeader(frame);
        return;
    }
    if (frame.type == FRAME_BODY) {
        handleBody(frame);
        return;
    }
    throw ConnectionError(FRAME_ERROR, "unsupported frame type");
}

void Connection::handleMethod(const Method& method, uint16_t channel) {
    try {
        if (method.classID == CLASS_CHANNEL) {
            handleChannelClass(method, channel);
        } else if (method.classID == CLASS_EXCHANGE) {
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                (void)requireChannel(channel, method.classID, method.methodID);
            }
            handleExchange(method, channel);
        } else if (method.classID == CLASS_QUEUE) {
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                (void)requireChannel(channel, method.classID, method.methodID);
            }
            handleQueue(method, channel);
        } else if (method.classID == CLASS_BASIC) {
            handleBasic(method, channel);
        } else if (method.classID == CLASS_CONFIRM) {
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                (void)requireChannel(channel, method.classID, method.methodID);
            }
            handleConfirm(method, channel);
        } else if (method.classID == CLASS_TX) {
            {
                std::lock_guard<std::mutex> lock(stateMutex);
                (void)requireChannel(channel, method.classID, method.methodID);
            }
            handleTX(method, channel);
        } else {
            throw ChannelError(NOT_IMPLEMENTED, "unsupported method", method.classID,
                               method.methodID);
        }
    } catch (const ChannelError& error) {
        failChannel(channel, error);
    } catch (const BrokerError& error) {
        failChannel(channel, ChannelError(error.code, error.what(), method.classID,
                                         method.methodID));
    } catch (const std::exception& error) {
        failChannel(channel, ChannelError(PRECONDITION_FAILED, error.what(),
                                         method.classID, method.methodID));
    }
}

void Connection::failChannel(uint16_t channel, const ChannelError& error) {
    if (!sendMethod(CLASS_CHANNEL, 40,
                    closeArguments(error.code, error.what(), error.classID,
                                   error.methodID),
                    channel)) {
        return;
    }
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        auto it = channels.find(channel);
        if (it != channels.end()) it->second.closing = true;
        closingChannels.insert(channel);
        closeDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(10);
        pendingPublishes.erase(channel);
    }
    broker->acknowledge(shared_from_this(), channel,
                        [this, channel]() { return allUnacked(channel); }, true, id);
    broker->removeChannel(shared_from_this(), channel, id);
    std::lock_guard<std::mutex> lock(stateMutex);
    channels.erase(channel);
}

Connection::Channel& Connection::requireChannel(uint16_t channel, uint16_t classID,
                                     uint16_t methodID) {
    if (channel > channelMax) {
        throw ChannelError(NOT_FOUND, "unknown channel", classID, methodID);
    }
    auto it = channels.find(channel);
    if (it == channels.end() || !it->second.open || it->second.closing) {
        throw ChannelError(NOT_FOUND, "channel is not open", classID, methodID);
    }
    return it->second;
}

Connection::Channel* Connection::findOpenChannel(uint16_t channel) {
    auto it = channels.find(channel);
    return it != channels.end() && it->second.open && !it->second.closing ? &it->second : nullptr;
}

void Connection::handleConnection(const Method& method) {
    Reader reader(method.arguments);
    switch (method.methodID) {
    case 11: {
        if (phase != Phase::START) {
            throw ConnectionError(COMMAND_INVALID, "unexpected connection.start-ok");
        }
        (void)reader.table();
        std::string mechanism = reader.shortString();
        std::string response = reader.longString();
        std::string locale = reader.shortString();
        reader.requireEnd();
        if (mechanism == "PLAIN") {
            if (response.size() < 3 || response[0] != '\0') {
                throw ConnectionError(ACCESS_REFUSED, "invalid SASL PLAIN response");
            }
            size_t separator = response.find('\0', 1);
            if (separator == std::string::npos) {
                throw ConnectionError(ACCESS_REFUSED, "invalid SASL PLAIN response");
            }
            user = response.substr(1, separator - 1);
            std::string password = response.substr(separator + 1);
            if (!broker || !broker->authenticate(user, password)) {
                throw ConnectionError(ACCESS_REFUSED, "authentication failed");
            }
        } else if (mechanism == "EXTERNAL") {
            if (!response.empty() || !transport->peerVerified()) {
                throw ConnectionError(ACCESS_REFUSED, "EXTERNAL requires a verified client certificate");
            }
            user = transport->peerIdentity();
            if (user.empty()) {
                throw ConnectionError(ACCESS_REFUSED, "certificate has no identity");
            }
        } else {
            throw ConnectionError(ACCESS_REFUSED, "unsupported SASL mechanism");
        }
        (void)locale;
        id = broker->registerConnection(shared_from_this());
        broker->setUsername(id, user);
        Writer tune;
        tune.u16(proposedChannelMax).u32(proposedFrameMax).u16(proposedHeartbeat);
        if (!sendMethod(CLASS_CONNECTION, 30, tune.data(), 0)) {
            close();
        }
        phase = Phase::TUNE;
        return;
    }
    case 31: {
        if (phase != Phase::TUNE) {
            throw ConnectionError(COMMAND_INVALID, "unexpected connection.tune-ok");
        }
        uint16_t clientChannels = reader.u16();
        uint32_t clientFrameMax = reader.u32();
        uint16_t clientHeartbeat = reader.u16();
        reader.requireEnd();
        // connection.tune-ok.{channel-max,frame-max}.upper-limit: exceeding
        // either server value closes without a negotiated close.
        if (clientChannels > proposedChannelMax ||
            (clientFrameMax != 0 && clientFrameMax > proposedFrameMax)) {
            throw ConnectionError(COMMAND_INVALID,
                                  "tune-ok exceeds server limits");
        }
        channelMax = clientChannels == 0 ? proposedChannelMax : clientChannels;
        frameMax = clientFrameMax == 0 ? proposedFrameMax : clientFrameMax;
        if (frameMax < MIN_FRAME_MAX) {
            throw ConnectionError(COMMAND_INVALID, "frame-max is below 4096");
        }
        heartbeat = std::min(proposedHeartbeat, clientHeartbeat);
        phase = Phase::OPEN;
        return;
    }
    case 40: {
        if (phase != Phase::OPEN || opened) {
            throw ConnectionError(COMMAND_INVALID, "unexpected connection.open");
        }
        virtualHost = reader.shortString();
        (void)reader.shortString();
        (void)bit(reader, "insist");
        reader.requireEnd();
        if (virtualHost != "/") {
            throw ConnectionError(INVALID_PATH, "unknown virtual host");
        }
        if (!sendMethod(CLASS_CONNECTION, 41, Writer{}.shortString("").data(), 0)) {
            close();
        }
        opened = true;
        lastActivity = std::chrono::steady_clock::now();
        return;
    }
    case 50: {
        uint16_t code = reader.u16();
        std::string text = reader.shortString();
        uint16_t classID = reader.u16();
        uint16_t methodID = reader.u16();
        reader.requireEnd();
        // connection.close: every Close is answered with Close-Ok, including
        // the zero normal-shutdown code used by Pika.
        if (!sendMethod(CLASS_CONNECTION, 51, {}, 0)) {
            close();
        }
        (void)code;
        (void)text;
        (void)classID;
        (void)methodID;
        close();
        return;
    }
    default:
        throw ConnectionError(NOT_IMPLEMENTED, "unsupported connection method");
    }
}

void Connection::handleChannelClass(const Method& method, uint16_t channel) {
    if (!opened) {
        throw ConnectionError(COMMAND_INVALID, "connection.open has not completed");
    }
    Reader reader(method.arguments);
    switch (method.methodID) {
    case 10: {
        (void)reader.shortString();
        reader.requireEnd();
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            if (channels.count(channel)) {
                throw ChannelError(PRECONDITION_FAILED, "channel is already open",
                                   CLASS_CHANNEL, method.methodID);
            }
            if (channel > channelMax || channelMax == 0) {
                throw ChannelError(NOT_FOUND, "channel exceeds channel-max",
                                   CLASS_CHANNEL, method.methodID);
            }
            channels[channel].open = true;
        }
        if (!sendMethod(CLASS_CHANNEL, 11, Writer{}.longString("").data(), channel)) {
            close();
        }
        return;
    }
    case 20: {
        bool active;
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            Channel& state = requireChannel(channel, CLASS_CHANNEL, method.methodID);
            active = bit(reader, "active");
            reader.requireEnd();
            state.flowActive = active;
        }
        // channel.flow-ok: echo the current active state in the response.
        Writer response;
        response.bits(active ? 1 : 0);
        if (!sendMethod(CLASS_CHANNEL, 21, response.data(), channel)) {
            close();
            return;
        }
        broker->dispatch();
        return;
    }
    case 40: {
        uint16_t code;
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            Channel& state = requireChannel(channel, CLASS_CHANNEL, method.methodID);
            code = reader.u16();
            (void)reader.shortString();
            (void)reader.u16();
            (void)reader.u16();
            reader.requireEnd();
            state.closing = true;
        }
        (void)code;
        // channel.close: Close-Ok is the response to either peer's Close.
        if (!sendMethod(CLASS_CHANNEL, 41, {}, channel)) {
            close();
            return;
        }
        finishChannelClose(channel);
        return;
    }
    default:
        throw ChannelError(NOT_IMPLEMENTED, "unsupported channel method",
                           CLASS_CHANNEL, method.methodID);
    }
}

void Connection::handleExchange(const Method& method, uint16_t channel) {
    Reader reader(method.arguments);
    switch (method.methodID) {
    case 10: {
        (void)reader.u16();
        std::string exchange = reader.shortString();
        std::string type = reader.shortString();
        uint8_t flags = packedBits(reader, 5, "exchange.declare");
        bool passive = flag(flags, 0);
        bool durable = flag(flags, 1);
        bool autoDelete = flag(flags, 2);
        bool internal = flag(flags, 3);
        bool noWait = flag(flags, 4);
        auto arguments = reader.table();
        reader.requireEnd();
        broker->declareExchange(exchange, type, passive, durable, autoDelete, internal,
                                id, Writer{}.table(arguments).data());
        if (!noWait && !sendMethod(CLASS_EXCHANGE, 11, {}, channel)) close();
        return;
    }
    case 20: {
        (void)reader.u16();
        std::string exchange = reader.shortString();
        uint8_t flags = packedBits(reader, 2, "exchange.delete");
        bool ifUnused = flag(flags, 0);
        bool noWait = flag(flags, 1);
        reader.requireEnd();
        broker->deleteExchange(exchange, ifUnused, id);
        if (!noWait && !sendMethod(CLASS_EXCHANGE, 21, {}, channel)) close();
        return;
    }
    default:
        throw ChannelError(NOT_IMPLEMENTED, "unsupported exchange method",
                           CLASS_EXCHANGE, method.methodID);
    }
}

void Connection::handleQueue(const Method& method, uint16_t channel) {
    Reader reader(method.arguments);
    switch (method.methodID) {
    case 10: {
        (void)reader.u16();
        std::string queue = reader.shortString();
        uint8_t flags = packedBits(reader, 5, "queue.declare");
        bool passive = flag(flags, 0);
        bool durable = flag(flags, 1);
        bool exclusive = flag(flags, 2);
        bool autoDelete = flag(flags, 3);
        bool noWait = flag(flags, 4);
        auto arguments = reader.table();
        reader.requireEnd();
        std::string name = broker->declareQueue(queue, passive, durable, exclusive,
                                                 autoDelete, id,
                                                 Writer{}.table(arguments).data());
        if (!noWait) {
            Writer response;
            response.shortString(name).u32(broker->queueMessageCount(name))
                    .u32(broker->queueConsumerCount(name));
            if (!sendMethod(CLASS_QUEUE, 11, response.data(), channel)) close();
        }
        return;
    }
    case 20: {
        (void)reader.u16();
        std::string queue = reader.shortString();
        std::string exchange = reader.shortString();
        std::string routingKey = reader.shortString();
        bool noWait = bit(reader, "no-wait");
        auto arguments = reader.table();
        reader.requireEnd();
        broker->bindQueue(queue, exchange, routingKey, id, Writer{}.table(arguments).data());
        if (!noWait && !sendMethod(CLASS_QUEUE, 21, {}, channel)) close();
        return;
    }
    case 30: {
        (void)reader.u16();
        std::string queue = reader.shortString();
        bool noWait = bit(reader, "no-wait");
        reader.requireEnd();
        size_t count = broker->purgeQueue(queue, id);
        if (!noWait && !sendMethod(CLASS_QUEUE, 31, Writer{}.u32(count).data(), channel)) close();
        return;
    }
    case 40: {
        (void)reader.u16();
        std::string queue = reader.shortString();
        uint8_t flags = packedBits(reader, 3, "queue.delete");
        bool ifUnused = flag(flags, 0);
        bool ifEmpty = flag(flags, 1);
        bool noWait = flag(flags, 2);
        reader.requireEnd();
        size_t count = broker->deleteQueue(queue, ifUnused, ifEmpty, id);
        if (!noWait && !sendMethod(CLASS_QUEUE, 41, Writer{}.u32(count).data(), channel)) close();
        return;
    }
    case 50: {
        (void)reader.u16();
        std::string queue = reader.shortString();
        std::string exchange = reader.shortString();
        std::string routingKey = reader.shortString();
        auto arguments = reader.table();
        reader.requireEnd();
        broker->unbindQueue(queue, exchange, routingKey, Writer{}.table(arguments).data());
        if (!sendMethod(CLASS_QUEUE, 51, {}, channel)) close();
        return;
    }
    default:
        throw ChannelError(NOT_IMPLEMENTED, "unsupported queue method",
                           CLASS_QUEUE, method.methodID);
    }
}

void Connection::handleBasic(const Method& method, uint16_t channel) {
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        (void)requireChannel(channel, CLASS_BASIC, method.methodID);
    }
    Reader reader(method.arguments);
    switch (method.methodID) {
    case 10: {
        uint32_t prefetchSize = reader.u32();
        uint16_t prefetchCount = reader.u16();
        bool globalQoS = bit(reader, "global");
        reader.requireEnd();
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            Channel& state = requireChannel(channel, CLASS_BASIC, method.methodID);
            state.prefetchSize = prefetchSize;
            state.prefetchCount = prefetchCount;
            state.globalQoS = globalQoS;
        }
        if (!sendMethod(CLASS_BASIC, 11, {}, channel)) close();
        broker->dispatch();
        return;
    }
    case 20: {
        (void)reader.u16();
        std::string queue = reader.shortString();
        std::string tag = reader.shortString();
        uint8_t flags = packedBits(reader, 4, "basic.consume");
        bool noLocal = flag(flags, 0);
        bool noAck = flag(flags, 1);
        bool exclusive = flag(flags, 2);
        bool noWait = flag(flags, 3);
        (void)reader.table();
        reader.requireEnd();
        tag = broker->consume(shared_from_this(), channel, queue, tag, noLocal,
                              noAck, exclusive, id);
        bool channelClosed = false;
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            auto it = channels.find(channel);
            if (it == channels.end() || !it->second.open || it->second.closing) {
                channelClosed = true;
            } else {
                it->second.consumerQueues[tag] = queue;
            }
        }
        if (channelClosed) {
            broker->cancel(shared_from_this(), channel, tag, id);
            throw ChannelError(NOT_FOUND, "channel is not open", CLASS_BASIC,
                               method.methodID);
        }
        if (!noWait) {
            Writer response;
            response.shortString(tag);
            if (!sendMethod(CLASS_BASIC, 21, response.data(), channel)) close();
        }
        broker->dispatch();
        return;
    }
    case 30: {
        std::string tag = reader.shortString();
        bool noWait = bit(reader, "no-wait");
        reader.requireEnd();
        broker->cancel(shared_from_this(), channel, tag, id);
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            auto it = channels.find(channel);
            if (it != channels.end()) it->second.consumerQueues.erase(tag);
        }
        if (!noWait && !sendMethod(CLASS_BASIC, 31, Writer{}.shortString(tag).data(), channel)) close();
        return;
    }
    case 40: {
        (void)reader.u16();
        PendingPublish pending;
        pending.channel = channel;
        pending.exchange = reader.shortString();
        pending.routingKey = reader.shortString();
        uint8_t flags = packedBits(reader, 2, "basic.publish");
        pending.mandatory = flag(flags, 0);
        pending.immediate = flag(flags, 1);
        reader.requireEnd();
        if (pending.exchange.size() > 255 || pending.routingKey.size() > 255) {
            throw ChannelError(PRECONDITION_FAILED, "exchange or routing key too long",
                               CLASS_BASIC, method.methodID);
        }
        std::lock_guard<std::mutex> lock(stateMutex);
        (void)requireChannel(channel, CLASS_BASIC, method.methodID);
        if (pendingPublishes.count(channel)) {
            throw ChannelError(UNEXPECTED_FRAME, "content header is already pending",
                               CLASS_BASIC, method.methodID);
        }
        pendingPublishes[channel] = std::move(pending);
        return;
    }
    case 70: {
        (void)reader.u16();
        std::string queue = reader.shortString();
        bool noAck = bit(reader, "no-ack");
        reader.requireEnd();
        checkReadPermission(queue, channel, CLASS_BASIC, method.methodID);
        uint64_t deliveryTag = 0;
        auto message = broker->get(queue, id, noAck, [&](const auto& item) {
            deliveryTag = reserveDelivery(channel, queue, item, "", !noAck, false);
            return deliveryTag;
        });
        if (!message) {
            if (!sendMethod(CLASS_BASIC, 72, Writer{}.shortString("").data(), channel)) close();
            return;
        }
        Writer response;
        response.u64(deliveryTag).bits(message->redelivered ? 1 : 0)
                .shortString(message->exchange).shortString(message->routingKey)
                .u32(broker->queueMessageCount(queue));
        if (!sendContent(channel, CLASS_BASIC, 71, response.data(), *message)) close();
        return;
    }
    case 80: {
        uint64_t tag = reader.u64();
        bool multiple = bit(reader, "multiple");
        reader.requireEnd();
        handleAcknowledgement(channel, tag, multiple, false);
        return;
    }
    case 90: {
        uint64_t tag = reader.u64();
        bool requeue = bit(reader, "requeue");
        reader.requireEnd();
        handleAcknowledgement(channel, tag, false, requeue);
        return;
    }
    case 110: {
        bool requeue = bit(reader, "requeue");
        reader.requireEnd();
        // basic.recover-ok: acknowledge the synchronous command before any
        // redeliveries produced by recovery (Errata 28).
        if (!sendMethod(CLASS_BASIC, 111, {}, channel)) close();
        broker->recover(shared_from_this(), channel,
                        [this, channel]() { return allUnacked(channel); }, requeue, id);
        return;
    }
    case 120: {
        uint64_t tag = reader.u64();
        uint8_t flags = packedBits(reader, 2, "basic.nack");
        bool multiple = flag(flags, 1);
        bool requeue = flag(flags, 0);
        reader.requireEnd();
        handleAcknowledgement(channel, tag, multiple, requeue);
        return;
    }
    default:
        throw ChannelError(NOT_IMPLEMENTED, "unsupported basic method",
                           CLASS_BASIC, method.methodID);
    }
}

void Connection::handleHeader(const Frame& frame) {
    if (!opened) {
        throw ConnectionError(COMMAND_INVALID, "connection.open has not completed");
    }
    if (frame.payload.size() < 14) {
        throw ConnectionError(FRAME_ERROR, "short content header");
    }
    Reader reader(frame.payload);
    uint16_t classID = reader.u16();
    uint16_t methodID = reader.u16();
    uint64_t bodySize = reader.u64();
    std::vector<uint8_t> properties(frame.payload.begin() + 12, frame.payload.end());
    if (classID != CLASS_BASIC || methodID != 0) {
        throw ConnectionError(COMMAND_INVALID,
                              "content class/method " + std::to_string(classID) + "/" +
                                  std::to_string(methodID) + " does not follow basic.publish");
    }
    Properties parsed = parseProperties(properties);
    if (!parsed.userID.empty() && parsed.userID != user) {
        throw ConnectionError(ACCESS_REFUSED, "user-id does not match authenticated user");
    }
    std::optional<PendingPublish> completed;
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        Channel* state = findOpenChannel(frame.channel);
        if (!state) {
            throw ConnectionError(CHANNEL_ERROR, "content header on closed channel");
        }
        auto pending = pendingPublishes.find(frame.channel);
        if (pending == pendingPublishes.end()) {
            throw ConnectionError(UNEXPECTED_FRAME, "unexpected content header");
        }
        if (bodySize > maxMessageBytes) {
            pendingPublishes.erase(pending);
            throw ConnectionError(RESOURCE_ERROR, "message exceeds broker limit");
        }
        pending->second.bodySize = bodySize;
        pending->second.received = 0;
        pending->second.message.properties = std::move(properties);
        pending->second.message.headers = std::move(parsed.headers);
        pending->second.message.body.reserve(static_cast<size_t>(bodySize));
        pending->second.message.expires = parsed.expiration.has_value();
        if (parsed.expiration) pending->second.message.deadline = *parsed.expiration;
        if (bodySize == 0) {
            completed = std::move(pending->second);
            pendingPublishes.erase(pending);
        }
    }
    MQTT::logDebug("AMQP content header channel=%u class=%u method=%u body=%llu",
                   frame.channel, classID, methodID,
                   static_cast<unsigned long long>(bodySize));
    if (completed) completePublish(frame.channel, *completed);
}

void Connection::handleBody(const Frame& frame) {
    if (!opened) {
        throw ConnectionError(COMMAND_INVALID, "connection.open has not completed");
    }
    std::optional<PendingPublish> completed;
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        Channel* state = findOpenChannel(frame.channel);
        if (!state) {
            throw ConnectionError(CHANNEL_ERROR, "content body on closed channel");
        }
        auto pending = pendingPublishes.find(frame.channel);
        if (pending == pendingPublishes.end()) {
            throw ConnectionError(UNEXPECTED_FRAME, "unexpected content body");
        }
        if (frame.payload.size() > pending->second.bodySize - pending->second.received) {
            pendingPublishes.erase(pending);
            throw ConnectionError(FRAME_ERROR, "content body exceeds declared size");
        }
        pending->second.message.body.insert(pending->second.message.body.end(),
                                            frame.payload.begin(), frame.payload.end());
        pending->second.received += frame.payload.size();
        if (pending->second.received == pending->second.bodySize) {
            completed = std::move(pending->second);
            pendingPublishes.erase(pending);
        }
    }
    if (completed) completePublish(frame.channel, *completed);
}

void Connection::completePublish(uint16_t channel, PendingPublish& pending) {
    Message message = std::move(pending.message);
    message.exchange = pending.exchange;
    message.routingKey = pending.routingKey;
    message.publisherConnectionID = id;
    checkWritePermission(message.routingKey, channel, CLASS_BASIC, 40);
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        Channel& state = requireChannel(channel, CLASS_BASIC, 40);
        if (state.transactional) {
            state.transactionPublishes.push_back(StagedPublish{std::move(message),
                                                               pending.mandatory,
                                                               pending.immediate});
            return;
        }
    }
    (void)publishNow(channel, message, pending.mandatory, pending.immediate, 0);
}

bool Connection::sendReturn(uint16_t channel, uint16_t code, const std::string& text,
                            const Message& message) {
    Writer arguments;
    arguments.u16(code).shortString(text).shortString(message.exchange)
            .shortString(message.routingKey);
    return sendContent(channel, CLASS_BASIC, 50, arguments.data(), message);
}

bool Connection::publishNow(uint16_t channel, Message& message, bool mandatory,
                            bool immediate, uint64_t confirmSequence) {
    if (confirmSequence == 0) {
        std::lock_guard<std::mutex> lock(stateMutex);
        Channel& state = requireChannel(channel, CLASS_BASIC, 40);
        if (state.confirm) confirmSequence = state.nextPublishSequence++;
    }
    PublishResult result;
    try {
        result = broker->publish(message, user, mandatory, immediate, id,
                                 confirmSequence != 0);
    } catch (const BrokerError& error) {
        failChannel(channel, ChannelError(error.code, error.what(), CLASS_BASIC, 40));
        return false;
    }
    MQTT::logDebug("AMQP publish channel=%u exchange=%s key=%s bytes=%zu routed=%d",
                   channel, message.exchange.c_str(), message.routingKey.c_str(),
                   message.body.size(), result.routed ? 1 : 0);
    // basic.return and §1.1: mandatory unroutable content uses 312 NO_ROUTE
    // (Errata 17); immediate content without a consumer uses 313 NO_CONSUMERS.
    if (mandatory && !result.routed) {
        if (!sendReturn(channel, REPLY_NO_ROUTE, "NO_ROUTE", message)) return false;
    } else if (immediate && result.immediateDeliveries == 0) {
        if (!sendReturn(channel, REPLY_NO_CONSUMERS, "NO_CONSUMERS", message)) return false;
    }
    if (confirmSequence != 0) {
        Writer acknowledgement;
        acknowledgement.u64(confirmSequence).bits(0);
        if (!sendMethod(CLASS_BASIC, 80, acknowledgement.data(), channel)) return false;
    }
    return true;
}

void Connection::handleConfirm(const Method& method, uint16_t channel) {
    Reader reader(method.arguments);
    if (method.methodID != 10) {
        throw ChannelError(NOT_IMPLEMENTED, "unsupported confirm method", CLASS_CONFIRM,
                           method.methodID);
    }
    bool noWait = bit(reader, "no-wait");
    reader.requireEnd();
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        Channel& state = requireChannel(channel, CLASS_CONFIRM, method.methodID);
        if (state.confirm || state.transactional) {
            throw ChannelError(PRECONDITION_FAILED,
                               "publisher confirms and transactions are mutually exclusive",
                               CLASS_CONFIRM, method.methodID);
        }
        state.confirm = true;
    }
    if (!noWait && !sendMethod(CLASS_CONFIRM, 11, {}, channel)) close();
}

void Connection::handleTX(const Method& method, uint16_t channel) {
    // tx class: publications and acknowledgements are batched; servers SHOULD
    // provide single-queue atomicity, while multi-queue atomicity is not guaranteed.
    Reader reader(method.arguments);
    switch (method.methodID) {
    case 10: {
        reader.requireEnd();
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            Channel& state = requireChannel(channel, CLASS_TX, method.methodID);
            if (state.transactional || state.confirm) {
                throw ChannelError(PRECONDITION_FAILED,
                                   "publisher confirms and transactions are mutually exclusive",
                                   CLASS_TX, method.methodID);
            }
            state.transactional = true;
        }
        if (!sendMethod(CLASS_TX, 11, {}, channel)) close();
        return;
    }
    case 20: {
        reader.requireEnd();
        std::vector<StagedPublish> publishes;
        std::vector<TransactionAck> acknowledgements;
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            Channel& state = requireChannel(channel, CLASS_TX, method.methodID);
            if (!state.transactional) {
                // tx.commit.transacted: Commit on a non-transacted channel is
                // a channel precondition failure.
                throw ChannelError(PRECONDITION_FAILED, "channel is not transacted",
                                   CLASS_TX, method.methodID);
            }
            publishes.swap(state.transactionPublishes);
            acknowledgements.reserve(state.transactionAcks.size());
            for (auto& [tag, acknowledgement] : state.transactionAcks) {
                (void)tag;
                acknowledgements.push_back(std::move(acknowledgement));
            }
            state.transactionAcks.clear();
        }
        for (auto& publish : publishes) {
            if (!publishNow(channel, publish.message, publish.mandatory, publish.immediate, 0)) {
                return;
            }
        }
        for (const auto& acknowledgement : acknowledgements) {
            const uint64_t tag = acknowledgement.delivery.deliveryTag;
            broker->acknowledge(shared_from_this(), channel, [this, channel, tag]() {
                auto delivery = discardDelivery(channel, tag);
                return delivery ? std::vector<Delivery>{*delivery} : std::vector<Delivery>{};
            }, acknowledgement.requeue, id);
        }
        if (!broker->flush()) {
            failChannel(channel, ChannelError(RESOURCE_ERROR, "failed to persist transaction",
                                              CLASS_TX, method.methodID));
            return;
        }
        if (!sendMethod(CLASS_TX, 21, {}, channel)) close();
        return;
    }
    case 30: {
        reader.requireEnd();
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            Channel& state = requireChannel(channel, CLASS_TX, method.methodID);
            if (!state.transactional) {
                // tx.rollback.transacted: Rollback on a non-transacted channel
                // is a channel precondition failure.
                throw ChannelError(PRECONDITION_FAILED, "channel is not transacted",
                                   CLASS_TX, method.methodID);
            }
            // tx.rollback: abandoning an ack does not redeliver its message.
            state.transactionPublishes.clear();
            state.transactionAcks.clear();
        }
        if (!sendMethod(CLASS_TX, 31, {}, channel)) close();
        return;
    }
    default:
        throw ChannelError(NOT_IMPLEMENTED, "unsupported tx method", CLASS_TX, method.methodID);
    }
}

void Connection::handleAcknowledgement(uint16_t channel, uint64_t tag, bool multiple,
                                       bool requeue) {
    bool transactional = false;
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        Channel& state = requireChannel(channel, CLASS_BASIC, 80);
        transactional = state.transactional;
    }
    if (!transactional) {
        broker->acknowledge(shared_from_this(), channel, [this, channel, tag, multiple]() {
            return takeUnacked(channel, tag, multiple);
        }, requeue, id);
        return;
    }
    auto deliveries = peekUnacked(channel, tag, multiple);
    std::lock_guard<std::mutex> lock(stateMutex);
    Channel& state = requireChannel(channel, CLASS_BASIC, 80);
    for (const Delivery& delivery : deliveries) {
        state.transactionAcks[delivery.deliveryTag] = TransactionAck{delivery, requeue};
    }
}

bool Connection::deliveryAllowedLocked(const Channel& state,
                                       const std::string& consumerTag,
                                       size_t bytes) const {
    if (!state.flowActive) return false;
    uint32_t outstanding = state.unacked.size();
    size_t outstandingBytes = state.unackedBytes;
    if (!state.globalQoS) {
        auto count = state.consumerUnacked.find(consumerTag);
        auto usedBytes = state.consumerUnackedBytes.find(consumerTag);
        outstanding = count == state.consumerUnacked.end() ? 0 : count->second;
        outstandingBytes = usedBytes == state.consumerUnackedBytes.end() ? 0 : usedBytes->second;
    }
    if (state.prefetchCount != 0 && outstanding >= state.prefetchCount) return false;
    // basic.qos.prefetch-size.01: a lone message is delivered even when it
    // exceeds the requested window; the size limit only blocks the next one.
    return state.prefetchSize == 0 || outstanding == 0 ||
           (bytes <= state.prefetchSize && outstandingBytes <= state.prefetchSize - bytes);
}

bool Connection::canDeliver(uint16_t channel, const std::string& consumerTag,
                            size_t bytes, bool noAck) const {
    std::lock_guard<std::mutex> lock(stateMutex);
    if (closed.load() || awaitingClose) return false;
    auto it = channels.find(channel);
    if (it == channels.end() || !it->second.open || it->second.closing ||
        !it->second.flowActive) {
        return false;
    }
    // basic.qos.{prefetch-count,prefetch-size}: no-ack messages are not
    // flow-controlled by either prefetch limit.
    return noAck || deliveryAllowedLocked(it->second, consumerTag, bytes);
}

uint64_t Connection::reserveDelivery(uint16_t channel, const std::string& queue,
                                     const std::shared_ptr<Message>& message,
                                     const std::string& consumerTag,
                                     bool track, bool enforceQoS) {
    std::lock_guard<std::mutex> lock(stateMutex);
    if (closed.load() || awaitingClose) return 0;
    auto it = channels.find(channel);
    if (it == channels.end() || !it->second.open || it->second.closing) {
        return 0;
    }
    Channel& state = it->second;
    if (enforceQoS && !deliveryAllowedLocked(state, consumerTag, message->body.size())) {
        return 0;
    }
    uint64_t tag = state.nextDeliveryTag++;
    if (track) {
        state.unacked[tag] = Delivery{queue, message, consumerTag, tag};
        state.unackedBytes += message->body.size();
        if (!consumerTag.empty()) {
            ++state.consumerUnacked[consumerTag];
            state.consumerUnackedBytes[consumerTag] += message->body.size();
        }
    }
    return tag;
}

bool Connection::sendDelivery(uint16_t channel, const std::string& consumerTag,
                              const std::shared_ptr<Message>& message,
                              uint64_t deliveryTag, uint16_t classID,
                              uint16_t methodID) {
    {
        std::lock_guard<std::mutex> lock(stateMutex);
        auto it = channels.find(channel);
        if (closed.load() || awaitingClose || it == channels.end() || !it->second.open ||
            it->second.closing || !it->second.consumerQueues.count(consumerTag)) {
            return false;
        }
    }
    Writer arguments;
    arguments.shortString(consumerTag).u64(deliveryTag).bits(message->redelivered ? 1 : 0)
            .shortString(message->exchange).shortString(message->routingKey);
    return sendContent(channel, classID, methodID, arguments.data(), *message);
}

void Connection::releaseDeliveryLocked(Channel& state, const Delivery& delivery) {
    state.unackedBytes -= delivery.message->body.size();
    if (!delivery.consumerTag.empty()) {
        auto count = state.consumerUnacked.find(delivery.consumerTag);
        if (count != state.consumerUnacked.end() && --count->second == 0) {
            state.consumerUnacked.erase(count);
        }
        auto bytes = state.consumerUnackedBytes.find(delivery.consumerTag);
        if (bytes != state.consumerUnackedBytes.end()) {
            bytes->second -= delivery.message->body.size();
            if (bytes->second == 0) state.consumerUnackedBytes.erase(bytes);
        }
    }
}

void Connection::discardQueue(const std::string& queue) {
    std::lock_guard<std::mutex> lock(stateMutex);
    for (auto& [channel, state] : channels) {
        for (auto it = state.consumerQueues.begin(); it != state.consumerQueues.end();) {
            if (it->second == queue) {
                it = state.consumerQueues.erase(it);
            } else {
                ++it;
            }
        }
        for (auto it = state.unacked.begin(); it != state.unacked.end();) {
            if (it->second.queue == queue) {
                releaseDeliveryLocked(state, it->second);
                it = state.unacked.erase(it);
            } else {
                ++it;
            }
        }
    }
}

std::optional<Delivery> Connection::discardDelivery(uint16_t channel,
                                                     uint64_t deliveryTag) {
    std::lock_guard<std::mutex> lock(stateMutex);
    auto channelIt = channels.find(channel);
    if (channelIt == channels.end()) return std::nullopt;
    auto it = channelIt->second.unacked.find(deliveryTag);
    if (it == channelIt->second.unacked.end()) return std::nullopt;
    Delivery delivery = it->second;
    releaseDeliveryLocked(channelIt->second, delivery);
    channelIt->second.unacked.erase(it);
    return delivery;
}

std::vector<Delivery> Connection::takeUnacked(uint16_t channel, uint64_t tag,
                                              bool multiple) {
    std::lock_guard<std::mutex> lock(stateMutex);
    auto channelIt = channels.find(channel);
    if (channelIt == channels.end()) {
        throw ChannelError(NOT_FOUND, "unknown channel", CLASS_BASIC, 80);
    }
    Channel& state = channelIt->second;
    auto exact = state.unacked.find(tag);
    if (exact == state.unacked.end()) {
        throw ChannelError(PRECONDITION_FAILED, "unknown delivery tag", CLASS_BASIC, 80);
    }
    std::vector<Delivery> result;
    auto end = multiple ? state.unacked.upper_bound(tag) : std::next(exact);
    for (auto it = multiple ? state.unacked.begin() : exact; it != end; ++it) {
        result.push_back(it->second);
        releaseDeliveryLocked(state, it->second);
    }
    state.unacked.erase(multiple ? state.unacked.begin() : exact, end);
    return result;
}

std::vector<Delivery> Connection::peekUnacked(uint16_t channel, uint64_t tag,
                                               bool multiple) const {
    std::lock_guard<std::mutex> lock(stateMutex);
    auto channelIt = channels.find(channel);
    if (channelIt == channels.end()) {
        throw ChannelError(NOT_FOUND, "unknown channel", CLASS_BASIC, 80);
    }
    const Channel& state = channelIt->second;
    auto exact = state.unacked.find(tag);
    if (exact == state.unacked.end()) {
        throw ChannelError(PRECONDITION_FAILED, "unknown delivery tag", CLASS_BASIC, 80);
    }
    std::vector<Delivery> result;
    auto end = multiple ? state.unacked.upper_bound(tag) : std::next(exact);
    for (auto it = multiple ? state.unacked.begin() : exact; it != end; ++it) {
        result.push_back(it->second);
    }
    return result;
}

std::vector<Delivery> Connection::snapshotUnacked() const {
    std::lock_guard<std::mutex> lock(stateMutex);
    std::vector<Delivery> result;
    for (const auto& [channelID, state] : channels) {
        (void)channelID;
        for (const auto& [tag, delivery] : state.unacked) {
            (void)tag;
            result.push_back(delivery);
        }
    }
    return result;
}

std::vector<Delivery> Connection::allUnacked(uint16_t channel) {
    std::lock_guard<std::mutex> lock(stateMutex);
    auto it = channels.find(channel);
    if (it == channels.end()) return {};
    std::vector<Delivery> result;
    for (const auto& entry : it->second.unacked) {
        result.push_back(entry.second);
        releaseDeliveryLocked(it->second, entry.second);
    }
    it->second.unacked.clear();
    it->second.unackedBytes = 0;
    return result;
}

std::vector<Delivery> Connection::allUnacked() {
    std::lock_guard<std::mutex> lock(stateMutex);
    std::vector<Delivery> result;
    for (auto& [channel, state] : channels) {
        (void)channel;
        for (const auto& entry : state.unacked) result.push_back(entry.second);
        state.unacked.clear();
        state.unackedBytes = 0;
    }
    return result;
}

void Connection::cleanup() {
    if (cleaned.exchange(true)) return;
    if (broker && id != 0) {
        broker->acknowledge(shared_from_this(), 0, [this]() { return allUnacked(); },
                            true, id);
        std::vector<uint16_t> channelIDs;
        {
            std::lock_guard<std::mutex> lock(stateMutex);
            for (const auto& entry : channels) channelIDs.push_back(entry.first);
        }
        for (uint16_t channel : channelIDs) {
            broker->removeChannel(shared_from_this(), channel, id);
        }
        broker->unregisterConnection(id);
    }
    std::lock_guard<std::mutex> lock(stateMutex);
    channels.clear();
    pendingPublishes.clear();
    phase = Phase::CLOSED;
}

void Connection::checkWritePermission(const std::string& topic, uint16_t channel,
                                      uint16_t classID, uint16_t methodID) {
    (void)topic;
    (void)channel;
    (void)classID;
    (void)methodID;
}

void Connection::checkReadPermission(const std::string& topic, uint16_t channel,
                                     uint16_t classID, uint16_t methodID) {
    (void)topic;
    (void)channel;
    (void)classID;
    (void)methodID;
}

} // namespace AMQP
