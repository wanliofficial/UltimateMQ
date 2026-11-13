#include "Store.h"
#include "Log.h"
#include <cerrno>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <fstream>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace MQTT {

namespace {

const char *STATE_HEADER = "FLOWMQ-STATE-1";

std::string toHex(const std::string &in) {
    static const char *digits = "0123456789abcdef";
    std::string out;
    out.reserve(in.size() * 2);
    for (unsigned char c : in) {
        out.push_back(digits[c >> 4]);
        out.push_back(digits[c & 0x0f]);
    }
    return out;
}

std::string toHex(const std::vector<uint8_t> &in) {
    return toHex(std::string(in.begin(), in.end()));
}

int hexVal(char c) {
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

bool fromHex(const std::string &hex, std::string &out) {
    if (hex.size() % 2 != 0) return false;
    out.clear();
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        int hi = hexVal(hex[i]);
        int lo = hexVal(hex[i + 1]);
        if (hi < 0 || lo < 0) return false;
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return true;
}

bool fromHex(const std::string &hex, std::vector<uint8_t> &out) {
    std::string raw;
    if (!fromHex(hex, raw)) return false;
    out.assign(raw.begin(), raw.end());
    return true;
}

Message decodeMessage(const std::string &topicHex, int qos, bool retain, const std::string &payloadHex) {
    Message message;
    fromHex(topicHex, message.topic);
    fromHex(payloadHex, message.payload);
    message.qos = static_cast<QoS>(qos);
    message.retain = retain;
    return message;
}

} // namespace

Store::Store(std::string path, std::chrono::milliseconds interval)
    : path(std::move(path)), interval(interval) {}

Store::~Store() {
    stop();
}

void Store::start(SnapshotFn fn) {
    if (!enabled()) {
        return;
    }
    snapshot = std::move(fn);
    running.store(true);
    thread = std::thread([this]() { loop(); });
}

void Store::stop() {
    running.store(false);
    if (thread.joinable()) {
        thread.join();
    }
}

void Store::loop() {
    // Sleep in small slices so stop() returns promptly even with a long interval
    const auto slice = std::chrono::milliseconds(50);
    auto waited = std::chrono::milliseconds(0);
    while (running.load()) {
        std::this_thread::sleep_for(slice);
        if (!running.load()) {
            break;
        }
        waited += slice;
        if (waited < interval) {
            continue;
        }
        waited = std::chrono::milliseconds(0);
        if (dirty.exchange(false) && snapshot) {
            write(snapshot());
        }
    }
}

bool Store::flushNow() {
    if (!enabled()) {
        // Nothing to write: a no-op is a success, matching load()
        return true;
    }
    dirty.store(false);
    // Without a snapshot callback there is nothing durable to write yet
    return snapshot ? write(snapshot()) : false;
}

bool Store::flushNow(SnapshotFn fn) {
    if (!enabled()) {
        return true;
    }
    return write(fn());
}

bool Store::write(const PersistedState &state) {
    std::lock_guard<std::mutex> lock(writeMutex);

    std::string out;
    out += STATE_HEADER;
    out += '\n';
    for (const auto &entry : state.retained) {
        const Message &m = entry.second;
        out += "retained " + toHex(m.topic) + " " + std::to_string(static_cast<int>(m.qos)) + " " +
               (m.retain ? "1" : "0") + " " + toHex(m.payload) + "\n";
    }
    for (const auto &session : state.sessions) {
        // An empty username must still occupy its column, otherwise the fixed
        // field count would shift. "-" is written literally as the sentinel for
        // "no username"; a real username is hex-encoded.
        const std::string user = session.username.empty() ? "-" : toHex(session.username);
        out += "session " + toHex(session.clientId) + " " + user + " " +
               std::to_string(session.nextPacketId) + "\n";
        for (const auto &sub : session.subscriptions) {
            const SubscriptionOptions &opt = sub.second;
            out += "sub " + toHex(sub.first) + " " + std::to_string(static_cast<int>(opt.maximumQos)) +
                   " " + (opt.noLocal ? "1" : "0") + " " + (opt.retainAsPublished ? "1" : "0") + " " +
                   std::to_string(static_cast<int>(opt.retainHandling)) + "\n";
        }
        for (const auto &inflight : session.inflight) {
            const Message &m = inflight.second;
            out += "inflight " + std::to_string(inflight.first) + " " + toHex(m.topic) + " " +
                   std::to_string(static_cast<int>(m.qos)) + " " + (m.retain ? "1" : "0") + " " +
                   toHex(m.payload) + "\n";
        }
        for (uint16_t id : session.awaitingPubrel) {
            out += "pubrel " + std::to_string(id) + "\n";
        }
    }
    out += "end\n";

    std::string tmp = path + ".tmp";
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0644);
    if (fd < 0) {
        logError("cannot open state file %s: %s", tmp.c_str(), std::strerror(errno));
        return false;
    }
    size_t offset = 0;
    while (offset < out.size()) {
        ssize_t written = ::write(fd, out.data() + offset, out.size() - offset);
        if (written <= 0) {
            if (written < 0 && errno == EINTR) {
                continue;
            }
            logError("cannot write state file %s: %s", tmp.c_str(), std::strerror(errno));
            ::close(fd);
            ::unlink(tmp.c_str());
            return false;
        }
        offset += static_cast<size_t>(written);
    }
    ::fsync(fd);
    ::close(fd);
    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        logError("cannot replace state file %s: %s", path.c_str(), std::strerror(errno));
        ::unlink(tmp.c_str());
        return false;
    }
    return true;
}

bool Store::load(PersistedState &out, std::string *error) const {
    out = PersistedState{};
    if (!enabled()) {
        return true;
    }
    std::ifstream in(path);
    if (!in) {
        // No state yet (first start): not an error
        return true;
    }

    std::string line;
    if (!std::getline(in, line)) {
        // Empty file: nothing was ever written
        return true;
    }
    if (line != STATE_HEADER) {
        if (error) *error = "unrecognised state file header";
        return false;
    }

    PersistedSession *current = nullptr;
    int lineNo = 1;
    while (std::getline(in, line)) {
        ++lineNo;
        if (line.empty() || line == "end") {
            continue;
        }
        std::istringstream ss(line);
        std::string kind;
        ss >> kind;
        if (kind == "retained") {
            std::string topicHex, payloadHex;
            int qos = 0;
            int retain = 0;
            if (!(ss >> topicHex >> qos >> retain >> payloadHex)) {
                if (error) *error = "bad retained record at line " + std::to_string(lineNo);
                return false;
            }
            Message m = decodeMessage(topicHex, qos, retain != 0, payloadHex);
            out.retained[m.topic] = m;
        } else if (kind == "session") {
            std::string clientHex, userHex;
            unsigned nextId = 1;
            // Skip leading blanks so an empty field cannot shift the rest
            // Fields are whitespace separated; an empty username is written as
            // the literal "-" so the column count never shifts
            if (!(ss >> clientHex >> userHex >> nextId)) {
                if (error) *error = "bad session record at line " + std::to_string(lineNo);
                return false;
            }
            PersistedSession session;
            if (!fromHex(clientHex, session.clientId)) {
                if (error) *error = "bad hex in session record at line " + std::to_string(lineNo);
                return false;
            }
            // "-" is the sentinel for "no username"; anything else is hex
            if (userHex == "-") {
                session.username.clear();
            } else if (!fromHex(userHex, session.username)) {
                if (error) *error = "bad hex in session record at line " + std::to_string(lineNo);
                return false;
            }
            session.nextPacketId = static_cast<uint16_t>(nextId);
            out.sessions.push_back(std::move(session));
            current = &out.sessions.back();
        } else if (kind == "sub" || kind == "inflight" || kind == "pubrel") {
            if (current == nullptr) {
                if (error) *error = "record without a session at line " + std::to_string(lineNo);
                return false;
            }
            if (kind == "sub") {
                std::string filterHex;
                int qos = 0, noLocal = 0, rap = 0, handling = 0;
                if (!(ss >> filterHex >> qos >> noLocal >> rap >> handling)) {
                    if (error) *error = "bad sub record at line " + std::to_string(lineNo);
                    return false;
                }
                std::string filter;
                if (!fromHex(filterHex, filter)) {
                    if (error) *error = "bad hex in sub record at line " + std::to_string(lineNo);
                    return false;
                }
                SubscriptionOptions opt;
                opt.maximumQos = static_cast<QoS>(qos);
                opt.noLocal = noLocal != 0;
                opt.retainAsPublished = rap != 0;
                opt.retainHandling = static_cast<RetainHandling>(handling);
                current->subscriptions[filter] = opt;
            } else if (kind == "inflight") {
                unsigned id = 0;
                std::string topicHex, payloadHex;
                int qos = 0, retain = 0;
                if (!(ss >> id >> topicHex >> qos >> retain >> payloadHex)) {
                    if (error) *error = "bad inflight record at line " + std::to_string(lineNo);
                    return false;
                }
                Message m = decodeMessage(topicHex, qos, retain != 0, payloadHex);
                current->inflight.emplace_back(static_cast<uint16_t>(id), std::move(m));
            } else {
                unsigned id = 0;
                if (!(ss >> id)) {
                    if (error) *error = "bad pubrel record at line " + std::to_string(lineNo);
                    return false;
                }
                current->awaitingPubrel.insert(static_cast<uint16_t>(id));
            }
        } else {
            if (error) *error = "unknown record '" + kind + "' at line " + std::to_string(lineNo);
            return false;
        }
    }
    return true;
}

} // namespace MQTT
