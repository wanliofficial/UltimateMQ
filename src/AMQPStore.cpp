#include "AMQPStore.h"
#include "Log.h"
#include <algorithm>
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <unistd.h>

namespace AMQP {
namespace {

constexpr const char* STATE_HEADER = "FLOWMQ-AMQP-STATE-1";

std::string toHex(const std::string& value) {
    static constexpr char digits[] = "0123456789abcdef";
    std::string result;
    result.reserve(value.size() * 2);
    for (unsigned char byte : value) {
        result.push_back(digits[byte >> 4]);
        result.push_back(digits[byte & 15]);
    }
    return result;
}

std::string toHex(const std::vector<uint8_t>& value) {
    return toHex(std::string(value.begin(), value.end()));
}

std::string field(const std::string& value) {
    return value.empty() ? "-" : toHex(value);
}

std::string field(const std::vector<uint8_t>& value) {
    return value.empty() ? "-" : toHex(value);
}

int hexValue(char value) {
    if (value >= '0' && value <= '9') return value - '0';
    if (value >= 'a' && value <= 'f') return value - 'a' + 10;
    if (value >= 'A' && value <= 'F') return value - 'A' + 10;
    return -1;
}

bool decodeField(const std::string& value, std::string& result) {
    if (value == "-") {
        result.clear();
        return true;
    }
    if (value.size() % 2 != 0) return false;
    result.clear();
    result.reserve(value.size() / 2);
    for (size_t index = 0; index < value.size(); index += 2) {
        int high = hexValue(value[index]);
        int low = hexValue(value[index + 1]);
        if (high < 0 || low < 0) return false;
        result.push_back(static_cast<char>((high << 4) | low));
    }
    return true;
}

bool decodeField(const std::string& value, std::vector<uint8_t>& result) {
    std::string decoded;
    if (!decodeField(value, decoded)) return false;
    result.assign(decoded.begin(), decoded.end());
    return true;
}

bool parseBool(const std::string& value, bool& result) {
    if (value == "0") {
        result = false;
        return true;
    }
    if (value == "1") {
        result = true;
        return true;
    }
    return false;
}

bool parseUnsigned(const std::string& value, uint64_t maximum, uint64_t& result) {
    if (value.empty()) return false;
    result = 0;
    for (char character : value) {
        if (character < '0' || character > '9') return false;
        uint64_t digit = static_cast<uint64_t>(character - '0');
        if (result > (maximum - digit) / 10) return false;
        result = result * 10 + digit;
    }
    return true;
}

bool parseRemaining(const std::string& value, int64_t& result) {
    if (value == "-") {
        result = -1;
        return true;
    }
    uint64_t parsed = 0;
    if (!parseUnsigned(value, static_cast<uint64_t>(INT64_MAX), parsed)) return false;
    result = static_cast<int64_t>(parsed);
    return true;
}

} // namespace

Store::Store(std::string statePath, std::chrono::milliseconds writeInterval)
    : path(std::move(statePath)), interval(writeInterval) {}

Store::~Store() {
    stop();
}

void Store::start(SnapshotFn fn) {
    if (!enabled()) return;
    snapshot = std::move(fn);
    running.store(true);
    thread = std::thread([this]() { loop(); });
}

void Store::stop() {
    running.store(false);
    if (thread.joinable()) thread.join();
}

void Store::loop() {
    auto nextWrite = std::chrono::steady_clock::now() + interval;
    while (running.load()) {
        std::this_thread::sleep_for(std::chrono::milliseconds(50));
        if (!running.load()) break;
        if (!dirty.load() || std::chrono::steady_clock::now() < nextWrite) continue;
        dirty.store(false);
        if (!writeSnapshot(snapshot)) MQTT::logError("failed to persist AMQP state");
        nextWrite = std::chrono::steady_clock::now() + interval;
    }
}

bool Store::flushNow() {
    if (!enabled()) return true;
    dirty.store(false);
    return snapshot && writeSnapshot(snapshot);
}

bool Store::flushNow(SnapshotFn fn) {
    if (!enabled()) return true;
    return writeSnapshot(fn);
}

bool Store::writeSnapshot(const SnapshotFn& fn) {
    if (!fn) return false;
    std::lock_guard<std::mutex> lock(writeMutex);
    return writeLocked(fn());
}

bool Store::writeLocked(const PersistedState& state) {
    std::ostringstream output;
    output << STATE_HEADER << '\n';
    for (const auto& exchange : state.exchanges) {
        output << "exchange " << field(exchange.name) << ' ' << field(exchange.type) << ' '
               << (exchange.durable ? 1 : 0) << ' ' << (exchange.autoDelete ? 1 : 0) << ' '
               << (exchange.internal ? 1 : 0) << ' ' << field(exchange.arguments) << '\n';
    }
    for (const auto& queue : state.queues) {
        output << "queue " << field(queue.name) << ' ' << (queue.durable ? 1 : 0) << ' '
               << (queue.autoDelete ? 1 : 0) << ' ' << field(queue.arguments) << '\n';
    }
    for (const auto& binding : state.bindings) {
        output << "binding " << field(binding.exchange) << ' ' << field(binding.queue) << ' '
               << field(binding.routingKey) << ' ' << field(binding.arguments) << '\n';
    }
    for (const auto& queue : state.queues) {
        for (const auto& stored : queue.messages) {
            const Message& message = stored.message;
            output << "message " << field(queue.name) << ' ' << field(message.exchange) << ' '
                   << field(message.routingKey) << ' ' << (message.redelivered ? 1 : 0) << ' '
                   << (message.expires ? 1 : 0) << ' '
                   << (stored.expiryRemainingSeconds < 0
                           ? "-"
                           : std::to_string(stored.expiryRemainingSeconds))
                   << ' ' << (stored.unacked ? 1 : 0) << ' ' << field(message.properties) << ' '
                   << field(message.body) << '\n';
        }
    }
    output << "next-queue " << state.nextQueueID << "\nend\n";
    const std::string data = output.str();

    const std::string temporary = path + ".tmp";
    int descriptor = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (descriptor < 0 && errno == EEXIST) {
        ::unlink(temporary.c_str());
        descriptor = ::open(temporary.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    }
    if (descriptor < 0) {
        MQTT::logError("cannot open AMQP state file %s: %s", temporary.c_str(), std::strerror(errno));
        return false;
    }
    size_t offset = 0;
    while (offset < data.size()) {
        ssize_t written = ::write(descriptor, data.data() + offset, data.size() - offset);
        if (written < 0 && errno == EINTR) continue;
        if (written <= 0) {
            MQTT::logError("cannot write AMQP state file %s: %s", temporary.c_str(), std::strerror(errno));
            ::close(descriptor);
            ::unlink(temporary.c_str());
            return false;
        }
        offset += static_cast<size_t>(written);
    }
    if (::fsync(descriptor) != 0) {
        MQTT::logError("cannot fsync AMQP state file %s: %s", temporary.c_str(), std::strerror(errno));
        ::close(descriptor);
        ::unlink(temporary.c_str());
        return false;
    }
    ::close(descriptor);
    if (::rename(temporary.c_str(), path.c_str()) != 0) {
        MQTT::logError("cannot replace AMQP state file %s: %s", path.c_str(), std::strerror(errno));
        ::unlink(temporary.c_str());
        return false;
    }
    std::string directory = path;
    size_t slash = directory.find_last_of('/');
    if (slash == std::string::npos) directory = ".";
    else if (slash == 0) directory = "/";
    else directory.erase(slash);
    int directoryDescriptor = ::open(directory.c_str(), O_RDONLY);
    if (directoryDescriptor >= 0) {
        ::fsync(directoryDescriptor);
        ::close(directoryDescriptor);
    }
    return true;
}

bool Store::load(PersistedState& state, std::string* error) const {
    state = PersistedState{};
    if (!enabled()) return true;
    std::ifstream input(path);
    if (!input) {
        if (errno == ENOENT) return true;
        if (error) *error = std::strerror(errno);
        return false;
    }
    auto fail = [&](const std::string& reason) {
        if (error) *error = reason;
        return false;
    };
    std::string line;
    if (!std::getline(input, line) || line != STATE_HEADER) {
        return fail("invalid AMQP state header");
    }
    bool ended = false;
    while (std::getline(input, line)) {
        std::istringstream columns(line);
        std::string record;
        columns >> record;
        if (record == "end") {
            std::string trailing;
            if (columns >> trailing) return fail("trailing data after AMQP end record");
            ended = true;
            break;
        }
        if (record == "exchange") {
            std::string name, type, durable, autoDelete, internal, arguments;
            if (!(columns >> name >> type >> durable >> autoDelete >> internal >> arguments) ||
                (columns >> record)) {
                return fail("malformed AMQP exchange record");
            }
            PersistedExchange exchange;
            if (!decodeField(name, exchange.name) || !decodeField(type, exchange.type) ||
                !parseBool(durable, exchange.durable) || !parseBool(autoDelete, exchange.autoDelete) ||
                !parseBool(internal, exchange.internal) || !decodeField(arguments, exchange.arguments)) {
                return fail("invalid AMQP exchange record");
            }
            state.exchanges.push_back(std::move(exchange));
        } else if (record == "queue") {
            std::string name, durable, autoDelete, arguments;
            if (!(columns >> name >> durable >> autoDelete >> arguments) || (columns >> record)) {
                return fail("malformed AMQP queue record");
            }
            PersistedQueue queue;
            if (!decodeField(name, queue.name) || !parseBool(durable, queue.durable) ||
                !parseBool(autoDelete, queue.autoDelete) || !decodeField(arguments, queue.arguments)) {
                return fail("invalid AMQP queue record");
            }
            state.queues.push_back(std::move(queue));
        } else if (record == "binding") {
            std::string exchange, queue, routingKey, arguments;
            if (!(columns >> exchange >> queue >> routingKey >> arguments) || (columns >> record)) {
                return fail("malformed AMQP binding record");
            }
            PersistedBinding binding;
            if (!decodeField(exchange, binding.exchange) || !decodeField(queue, binding.queue) ||
                !decodeField(routingKey, binding.routingKey) ||
                !decodeField(arguments, binding.arguments)) {
                return fail("invalid AMQP binding record");
            }
            state.bindings.push_back(std::move(binding));
        } else if (record == "message") {
            std::string queue, queueName, exchange, routingKey, redelivered, expires, remaining,
                        unacked, properties, body;
            if (!(columns >> queue >> exchange >> routingKey >> redelivered >> expires >> remaining >>
                  unacked >> properties >> body) ||
                (columns >> record)) {
                return fail("malformed AMQP message record");
            }
            PersistedMessage stored;
            if (!decodeField(queue, queueName) || !decodeField(exchange, stored.message.exchange) ||
                !decodeField(routingKey, stored.message.routingKey) ||
                !parseBool(redelivered, stored.message.redelivered) ||
                !parseBool(expires, stored.message.expires) ||
                !parseRemaining(remaining, stored.expiryRemainingSeconds) ||
                !parseBool(unacked, stored.unacked) || !decodeField(properties, stored.message.properties) ||
                !decodeField(body, stored.message.body)) {
                return fail("invalid AMQP message record");
            }
            if ((!stored.message.expires && stored.expiryRemainingSeconds != -1) ||
                (stored.message.expires && stored.expiryRemainingSeconds < 0)) {
                return fail("invalid AMQP message expiration");
            }
            auto target = std::find_if(state.queues.begin(), state.queues.end(),
                                       [&](const PersistedQueue& item) {
                                           return item.name == queueName;
                                       });
            if (target == state.queues.end()) return fail("AMQP message references unknown queue");
            target->messages.push_back(std::move(stored));
        } else if (record == "next-queue") {
            std::string value;
            if (!(columns >> value) || (columns >> record) ||
                !parseUnsigned(value, UINT64_MAX - 1, state.nextQueueID) || state.nextQueueID == 0) {
                return fail("invalid AMQP next-queue record");
            }
        } else {
            return fail("unknown AMQP state record");
        }
    }
    if (ended) {
        while (std::getline(input, line)) {
            if (line.find_first_not_of(" \t\r") != std::string::npos) {
                return fail("trailing data after AMQP end record");
            }
        }
    }
    if (!ended) return fail("AMQP state file has no end record");
    std::set<std::string> queueNames;
    for (const auto& queue : state.queues) {
        if (!queueNames.insert(queue.name).second) return fail("duplicate AMQP queue record");
    }
    for (const auto& binding : state.bindings) {
        if (!queueNames.count(binding.queue)) return fail("AMQP binding references unknown queue");
    }
    return true;
}

} // namespace AMQP
