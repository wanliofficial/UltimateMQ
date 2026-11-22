#include "Store.h"
#include <type_traits>
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

//! Sentinel for an empty field in the whitespace-delimited records (an empty
//! payload or topic hex-encodes to "", which would collapse the column and shift
//! every later field). "-" never appears inside a hex string, so it is
//! unambiguous.
const char *EMPTY_FIELD = "-";

//! Hex-encode \p in, or return the empty-field sentinel when it is empty so the
//! fixed column count of a record is preserved (see EMPTY_FIELD).
std::string toHexField(const std::string &in) {
    return in.empty() ? std::string(EMPTY_FIELD) : toHex(in);
}

std::string toHexField(const std::vector<uint8_t> &in) {
    return in.empty() ? std::string(EMPTY_FIELD) : toHex(in);
}

//! Decode a hex field that may be the empty-field sentinel ("-") back to "".
//! Returns false only when \p hex is non-empty, not the sentinel, and not valid
//! hex -- so a corrupt file is rejected rather than silently read as empty.
bool fromHexField(const std::string &hex, std::string &out) {
    if (hex == EMPTY_FIELD) {
        out.clear();
        return true;
    }
    return fromHex(hex, out);
}

bool fromHexField(const std::string &hex, std::vector<uint8_t> &out) {
    if (hex == EMPTY_FIELD) {
        out.clear();
        return true;
    }
    return fromHex(hex, out);
}

//! Decode a message record's hex columns into \p message. Returns false when a
//! column is not valid hex (nor the empty-field sentinel), so a corrupt file is
//! rejected rather than silently read as an empty topic/payload.
bool decodeMessage(const std::string &topicHex, int qos, bool retain,
                   const std::string &payloadHex, Message &message) {
    if (!fromHexField(topicHex, message.topic) || !fromHexField(payloadHex, message.payload)) {
        return false;
    }
    message.qos = static_cast<QoS>(qos);
    message.retain = retain;
    return true;
}

//! Encode the forwardable MQTT 5 properties of a message as a single
//! whitespace-free trail field: "<id>:<tag><body>" entries joined by commas.
//! "-" means "no properties". The tag keeps the variant alternative exact so a
//! round trip restores the same type.
std::string encodeMessageProperties(const Properties &properties) {
    if (properties.empty()) {
        return "-";
    }
    std::string out;
    for (const auto &[id, value] : properties) {
        if (!out.empty()) {
            out.push_back(',');
        }
        out += std::to_string(static_cast<int>(id));
        out.push_back(':');
        std::visit([&out](auto &&arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, bool>) {
                out += "B";
                out += arg ? "1" : "0";
            } else if constexpr (std::is_same_v<T, uint8_t>) {
                out += "b";
                out += std::to_string(static_cast<int>(arg));
            } else if constexpr (std::is_same_v<T, uint16_t>) {
                out += "w";
                out += std::to_string(arg);
            } else if constexpr (std::is_same_v<T, uint32_t>) {
                out += "u";
                out += std::to_string(arg);
            } else if constexpr (std::is_same_v<T, std::string>) {
                out += "s";
                out += toHex(arg);
            } else if constexpr (std::is_same_v<T, UserProperties>) {
                // A list of pairs, each key and value hex encoded so any byte
                // survives; '.' separates pairs and '=' separates key/value.
                out += "p";
                bool first = true;
                for (const auto &[key, val] : arg) {
                    if (!first) {
                        out.push_back('.');
                    }
                    first = false;
                    out += toHex(key);
                    out.push_back('=');
                    out += toHex(val);
                }
            }
        }, value);
    }
    return out;
}

bool parseUnsigned(const std::string &text, unsigned long long &out) {
    if (text.empty()) {
        return false;
    }
    out = 0;
    for (char c : text) {
        if (c < '0' || c > '9') {
            return false;
        }
        // Reject before the multiply so a long corrupt field cannot wrap the
        // accumulator: once out would exceed 0xFFFFFFFF (the widest value any
        // field here is stored into), stop.
        const unsigned digit = static_cast<unsigned>(c - '0');
        if (out > (0xFFFFFFFFull - digit) / 10) {
            return false;
        }
        out = out * 10 + digit;
    }
    return true;
}

//! Read one whitespace-delimited decimal and reject it when it falls outside
//! [0, limit]. The state file is data, not code: enumerations stored in it
//! (QoS, Retain Handling) are cast to their types after this check, so a corrupt
//! file cannot smuggle an out-of-range value into a restored message or
//! subscription (e.g. QoS 7, which no packet handler would produce).
bool readBounded(std::istringstream &in, unsigned long long limit,
                 unsigned long long &out) {
    std::string token;
    if (!(in >> token)) {
        return false;
    }
    if (!parseUnsigned(token, out)) {
        return false;
    }
    return out <= limit;
}

//! The canonical variant type a property id may hold, mirroring the decoder in
//! Frame.cpp (e.g. MESSAGE_EXPIRY_INTERVAL is a uint32_t, CONTENT_TYPE a string,
//! USER_PROPERTY a UserProperties). A durable file that stores a property id
//! with the wrong type tag is corrupt: re-serialising it on restore would emit a
//! malformed PUBLISH (a one-byte value under a four-byte property id), so it
//! must be rejected at load rather than trusted.
enum class PropKind { BOOL, UINT8, UINT16, UINT32, STRING, USER };

bool propKindFor(PropertyID id, PropKind &kind) {
    switch (id) {
    case PropertyID::PAYLOAD_FORMAT_INDICATOR:
    case PropertyID::MAXIMUM_QOS:
        kind = PropKind::UINT8;
        return true;
    case PropertyID::REQUEST_PROBLEM_INFORMATION:
    case PropertyID::REQUEST_RESPONSE_INFORMATION:
    case PropertyID::RETAIN_AVAILABLE:
    case PropertyID::WILDCARD_SUBSCRIPTION_AVAILABLE:
    case PropertyID::SUBSCRIPTION_IDENTIFIER_AVAILABLE:
    case PropertyID::SHARED_SUBSCRIPTION_AVAILABLE:
        kind = PropKind::BOOL;
        return true;
    case PropertyID::SERVER_KEEP_ALIVE:
    case PropertyID::RECEIVE_MAXIMUM:
    case PropertyID::TOPIC_ALIAS_MAXIMUM:
    case PropertyID::TOPIC_ALIAS:
        kind = PropKind::UINT16;
        return true;
    case PropertyID::MESSAGE_EXPIRY_INTERVAL:
    case PropertyID::SUBSCRIPTION_IDENTIFIER:
    case PropertyID::SESSION_EXPIRY_INTERVAL:
    case PropertyID::WILL_DELAY_INTERVAL:
    case PropertyID::MAXIMUM_PACKET_SIZE:
        kind = PropKind::UINT32;
        return true;
    case PropertyID::CONTENT_TYPE:
    case PropertyID::RESPONSE_TOPIC:
    case PropertyID::CORRELATION_DATA:
    case PropertyID::REASON_STRING:
    case PropertyID::SERVER_REFERENCE:
    case PropertyID::ASSIGNED_CLIENT_IDENTIFIER:
    case PropertyID::AUTHENTICATION_METHOD:
    case PropertyID::AUTHENTICATION_DATA:
        kind = PropKind::STRING;
        return true;
    case PropertyID::USER_PROPERTY:
        kind = PropKind::USER;
        return true;
    }
    return false; // unknown id: not a property this build ever writes
}

//! Whether \p value's variant alternative matches the canonical type for \p id.
bool propertyTypeMatches(PropertyID id, const PropertyValue &value) {
    PropKind expected;
    if (!propKindFor(id, expected)) {
        return false;
    }
    switch (expected) {
    case PropKind::BOOL:
        return std::holds_alternative<bool>(value);
    case PropKind::UINT8:
        return std::holds_alternative<uint8_t>(value);
    case PropKind::UINT16:
        return std::holds_alternative<uint16_t>(value);
    case PropKind::UINT32:
        return std::holds_alternative<uint32_t>(value);
    case PropKind::STRING:
        return std::holds_alternative<std::string>(value);
    case PropKind::USER:
        return std::holds_alternative<UserProperties>(value);
    }
    return false;
}

bool decodeMessageProperties(const std::string &encoded, Properties &out) {
    if (encoded == "-" || encoded.empty()) {
        return true;
    }
    std::stringstream entries(encoded);
    std::string entry;
    while (std::getline(entries, entry, ',')) {
        const auto colon = entry.find(':');
        if (colon == std::string::npos || colon + 1 >= entry.size()) {
            return false;
        }
        unsigned long long id = 0;
        if (!parseUnsigned(entry.substr(0, colon), id)) {
            return false;
        }
        const char kind = entry[colon + 1];
        const std::string body = entry.substr(colon + 2);
        switch (kind) {
        case 'B':
            out[static_cast<PropertyID>(id)] = (body == "1");
            break;
        case 'b':
        case 'w':
        case 'u': {
            unsigned long long value = 0;
            if (!parseUnsigned(body, value)) {
                return false;
            }
            if (kind == 'b') {
                out[static_cast<PropertyID>(id)] = static_cast<uint8_t>(value);
            } else if (kind == 'w') {
                out[static_cast<PropertyID>(id)] = static_cast<uint16_t>(value);
            } else {
                out[static_cast<PropertyID>(id)] = static_cast<uint32_t>(value);
            }
            break;
        }
        case 's': {
            std::string text;
            if (!fromHex(body, text)) {
                return false;
            }
            out[static_cast<PropertyID>(id)] = text;
            break;
        }
        case 'p': {
            UserProperties pairs;
            std::stringstream list(body);
            std::string pair;
            while (std::getline(list, pair, '.')) {
                const auto equals = pair.find('=');
                if (equals == std::string::npos) {
                    return false;
                }
                std::string key, value;
                if (!fromHex(pair.substr(0, equals), key)) return false;
                if (!fromHex(pair.substr(equals + 1), value)) return false;
                pairs.emplace_back(key, value);
            }
            out[static_cast<PropertyID>(id)] = pairs;
            break;
        }
        default:
            return false;
        }
        // The tag must agree with the property id's canonical type; a mismatch
        // (e.g. MESSAGE_EXPIRY_INTERVAL stored as a bool) would serialise into a
        // malformed PUBLISH on restore, so reject the corrupt record.
        auto stored = out.find(static_cast<PropertyID>(id));
        if (stored == out.end() || !propertyTypeMatches(stored->first, stored->second)) {
            return false;
        }
    }
    return true;
}

//! Remaining Message Expiry as a trail field: whole seconds still left, or "-"
//! when the message carries no expiry. A steady-clock deadline cannot survive a
//! restart, so the remaining interval is stored instead (MQTT-3.3.2-9).
std::string encodeMessageExpiry(const Message &m) {
    if (!m.hasExpiry) {
        return "-";
    }
    const auto left = std::chrono::duration_cast<std::chrono::seconds>(
                          m.expiryDeadline - std::chrono::steady_clock::now()).count();
    return std::to_string(left < 0 ? 0 : left);
}

bool decodeMessageExpiry(const std::string &token, Message &m) {
    if (token == "-" || token.empty()) {
        return true;
    }
    unsigned long long seconds = 0;
    if (!parseUnsigned(token, seconds)) {
        return false;
    }
    m.hasExpiry = true;
    m.expiryDeadline = std::chrono::steady_clock::now() + std::chrono::seconds(seconds);
    return true;
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
        out += "retained " + toHexField(m.topic) + " " + std::to_string(static_cast<int>(m.qos)) + " " +
               (m.retain ? "1" : "0") + " " + toHexField(m.payload) + " " +
               encodeMessageProperties(m.properties) + " " + encodeMessageExpiry(m) + "\n";
    }
    for (const auto &session : state.sessions) {
        // An empty username must still occupy its column, otherwise the fixed
        // field count would shift. "-" is written literally as the sentinel for
        // "no username"; a real username is hex-encoded.
        const std::string user = session.username.empty() ? "-" : toHex(session.username);
        out += "session " + toHexField(session.clientId) + " " + user + " " +
               std::to_string(session.nextPacketId) + " " +
               (session.expiryRemainingSeconds < 0
                    ? "-"
                    : std::to_string(session.expiryRemainingSeconds)) + "\n";
        for (const auto &sub : session.subscriptions) {
            const SubscriptionOptions &opt = sub.second;
            out += "sub " + toHexField(sub.first) + " " + std::to_string(static_cast<int>(opt.maximumQos)) +
                   " " + (opt.noLocal ? "1" : "0") + " " + (opt.retainAsPublished ? "1" : "0") + " " +
                   std::to_string(static_cast<int>(opt.retainHandling)) + " " +
                   std::to_string(opt.subscriptionIdentifier) + "\n";
        }
        for (const auto &inflight : session.inflight) {
            const Message &m = inflight.second;
            out += "inflight " + std::to_string(inflight.first) + " " + toHexField(m.topic) + " " +
                   std::to_string(static_cast<int>(m.qos)) + " " + (m.retain ? "1" : "0") + " " +
                   toHexField(m.payload) + " " + encodeMessageProperties(m.properties) + " " +
                   encodeMessageExpiry(m) + "\n";
        }
        for (const auto &blocked : session.blocked) {
            const Message &m = blocked;
            out += "blocked 0 " + toHexField(m.topic) + " " +
                   std::to_string(static_cast<int>(m.qos)) + " " + (m.retain ? "1" : "0") + " " +
                   toHexField(m.payload) + " " + encodeMessageProperties(m.properties) + " " +
                   encodeMessageExpiry(m) + "\n";
        }
        for (uint16_t id : session.awaitingPubrel) {
            out += "pubrel " + std::to_string(id) + "\n";
        }
        // Outgoing QoS 2 flow in the PUBREL-wait state (broker sent PUBREL, the
        // client's PUBCOMP is still outstanding); re-sent on resume.
        for (uint16_t id : session.pendingPubrel) {
            out += "outpubrel " + std::to_string(id) + "\n";
        }
    }
    out += "end\n";

    std::string tmp = path + ".tmp";
    // 0600: the state file can hold message payloads and session usernames, so it
    // is private to the broker's user. O_EXCL makes a pre-planted symlink at the
    // predictable ".tmp" name fail with EEXIST instead of being followed and
    // truncating whatever it points at.
    int fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    if (fd < 0 && errno == EEXIST) {
        // A stale temp file from a previous crash is the normal case; remove it
        // and retry once so a crash mid-write does not wedge every later write.
        ::unlink(tmp.c_str());
        fd = ::open(tmp.c_str(), O_WRONLY | O_CREAT | O_EXCL, 0600);
    }
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
    // The durability claim rests on this fsync: if it fails, the rename below
    // would commit contents that may only be in the page cache, so report the
    // write as failed rather than claim state that a crash could lose.
    if (::fsync(fd) != 0) {
        logError("cannot fsync state file %s: %s", tmp.c_str(), std::strerror(errno));
        ::close(fd);
        ::unlink(tmp.c_str());
        return false;
    }
    ::close(fd);
    if (::rename(tmp.c_str(), path.c_str()) != 0) {
        logError("cannot replace state file %s: %s", path.c_str(), std::strerror(errno));
        ::unlink(tmp.c_str());
        return false;
    }
    // The fsync above ordered the file's own contents, but the rename also has
    // to be persisted: without fsyncing the containing directory a crash right
    // after rename() can leave the new name uncommitted, so the durability
    // claim holds only for torn writes, not for a lost commit.
    {
        std::string dir = path;
        const auto slash = dir.find_last_of('/');
        if (slash == std::string::npos) {
            dir = ".";
        } else if (slash == 0) {
            dir = "/";
        } else {
            dir.erase(slash);
        }
        int dirFd = ::open(dir.c_str(), O_RDONLY);
        if (dirFd >= 0) {
            // Best effort: some filesystems refuse to fsync a directory. The
            // file's own fsync above still holds; this only sharpens the
            // rename-vs-crash window when the platform supports it.
            ::fsync(dirFd);
            ::close(dirFd);
        }
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
            unsigned long long qos = 0;
            unsigned long long retain = 0;
            unsigned long long maxQos = static_cast<unsigned long long>(QoS::QOS_2);
            if (!(ss >> topicHex) || !readBounded(ss, maxQos, qos) ||
                !readBounded(ss, 1, retain) || !(ss >> payloadHex)) {
                if (error) *error = "bad retained record at line " + std::to_string(lineNo);
                return false;
            }
            Message m;
            if (!decodeMessage(topicHex, static_cast<int>(qos), retain != 0, payloadHex, m)) {
                if (error) *error = "bad hex in retained record at line " + std::to_string(lineNo);
                return false;
            }
            // The property trail field is optional: state written by an older
            // build has no such column and simply carries no properties.
            std::string encodedProperties;
            if (ss >> encodedProperties && !decodeMessageProperties(encodedProperties, m.properties)) {
                if (error) *error = "bad properties in retained record at line " + std::to_string(lineNo);
                return false;
            }
            // The expiry trail field is optional too: older state carries none.
            std::string expiryToken;
            if (ss >> expiryToken && !decodeMessageExpiry(expiryToken, m)) {
                if (error) *error = "bad expiry in retained record at line " + std::to_string(lineNo);
                return false;
            }
            out.retained[m.topic] = m;
        } else if (kind == "session") {
            std::string clientHex, userHex, nextIdToken;
            unsigned nextId = 1;
            // Skip leading blanks so an empty field cannot shift the rest
            // Fields are whitespace separated; an empty username is written as
            // the literal "-" so the column count never shifts
            if (!(ss >> clientHex >> userHex >> nextIdToken)) {
                if (error) *error = "bad session record at line " + std::to_string(lineNo);
                return false;
            }
            PersistedSession session;
            if (!fromHexField(clientHex, session.clientId)) {
                if (error) *error = "bad hex in session record at line " + std::to_string(lineNo);
                return false;
            }
            // "-" is the sentinel for "no username"; anything else is hex
            if (userHex == "-") {
                session.username.clear();
            } else if (!fromHexField(userHex, session.username)) {
                if (error) *error = "bad hex in session record at line " + std::to_string(lineNo);
                return false;
            }
            // nextPacketId is a Packet Identifier (MQTT §1.5.1: 1..65535). The
            // writer only ever emits a uint16_t, so a value above 65535 is a
            // corrupt file: casting it would wrap silently to a different id and
            // could collide with an in-flight message on resume (MQTT-2.2.1-4).
            // 0 is tolerated (restore() maps it to 1) for older files.
            {
                unsigned long long packed = 0;
                if (!parseUnsigned(nextIdToken, packed) || packed > 0xFFFF) {
                    if (error) *error = "nextPacketId out of range in session record at line " +
                                        std::to_string(lineNo);
                    return false;
                }
                nextId = static_cast<unsigned>(packed);
            }
            session.nextPacketId = static_cast<uint16_t>(nextId);
            // Optional remaining Session Expiry interval; absent in older state.
            std::string expiryToken;
            if (ss >> expiryToken && expiryToken != "-") {
                unsigned long long seconds = 0;
                if (!parseUnsigned(expiryToken, seconds)) {
                    if (error) *error = "bad expiry in session record at line " + std::to_string(lineNo);
                    return false;
                }
                session.expiryRemainingSeconds = static_cast<int64_t>(seconds);
            }
            out.sessions.push_back(std::move(session));
            current = &out.sessions.back();
        } else if (kind == "sub" || kind == "inflight" || kind == "blocked" || kind == "pubrel" ||
                   kind == "outpubrel") {
            if (current == nullptr) {
                if (error) *error = "record without a session at line " + std::to_string(lineNo);
                return false;
            }
            if (kind == "sub") {
                std::string filterHex;
                unsigned long long qos = 0, noLocal = 0, rap = 0, handling = 0;
                const unsigned long long maxQos = static_cast<unsigned long long>(QoS::QOS_2);
                // Retain Handling's stored range is 0..2 (§3.8.3.1).
                const unsigned long long maxHandling = 2;
                if (!(ss >> filterHex) || !readBounded(ss, maxQos, qos) ||
                    !readBounded(ss, 1, noLocal) || !readBounded(ss, 1, rap) ||
                    !readBounded(ss, maxHandling, handling)) {
                    if (error) *error = "bad sub record at line " + std::to_string(lineNo);
                    return false;
                }
                // The Subscription Identifier trail field is optional so that a
                // state file written by an older build still loads. When present
                // it MUST be a legal Variable Byte Integer: a VBI holds at most
                // four bytes, so the value cannot exceed 268,435,455 (§1.5.5,
                // §2.2.2.2). Unlike the fixed fields above it is the last
                // column and genuinely optional, so an absent field means "no
                // identifier" (0) rather than an error; a present field, though,
                // is range-checked so a corrupt file cannot smuggle an
                // out-of-range value into the restored subscription. Such a
                // value would otherwise be narrowed into the uint32_t field and
                // re-serialised on resume as a five-byte VBI, which this broker's
                // own parser rejects (MAX_REMAINING_LENGTH_BYTES) — breaking
                // every SUBACK for the session.
                std::string idToken;
                unsigned long long subscriptionIdentifier = 0;
                if (ss >> idToken) {
                    if (!parseUnsigned(idToken, subscriptionIdentifier) ||
                        subscriptionIdentifier > 268435455ull) {
                        if (error) {
                            *error = "bad subscription identifier in sub record at line " +
                                     std::to_string(lineNo);
                        }
                        return false;
                    }
                }
                std::string filter;
                if (!fromHexField(filterHex, filter)) {
                    if (error) *error = "bad hex in sub record at line " + std::to_string(lineNo);
                    return false;
                }
                SubscriptionOptions opt;
                opt.maximumQos = static_cast<QoS>(static_cast<uint8_t>(qos));
                opt.noLocal = noLocal != 0;
                opt.retainAsPublished = rap != 0;
                opt.retainHandling = static_cast<RetainHandling>(static_cast<uint8_t>(handling));
                opt.subscriptionIdentifier = subscriptionIdentifier;
                current->subscriptions[filter] = opt;
            } else if (kind == "inflight") {
                unsigned long long id = 0;
                std::string topicHex, payloadHex;
                unsigned long long qos = 0, retain = 0;
                const unsigned long long maxQos = static_cast<unsigned long long>(QoS::QOS_2);
                if (!readBounded(ss, 65535, id) || !(ss >> topicHex) ||
                    !readBounded(ss, maxQos, qos) || !readBounded(ss, 1, retain) ||
                    !(ss >> payloadHex)) {
                    if (error) *error = "bad inflight record at line " + std::to_string(lineNo);
                    return false;
                }
                Message m;
                if (!decodeMessage(topicHex, static_cast<int>(qos), retain != 0, payloadHex, m)) {
                    if (error) *error = "bad hex in inflight record at line " + std::to_string(lineNo);
                    return false;
                }
                std::string encodedProperties;
                if (ss >> encodedProperties && !decodeMessageProperties(encodedProperties, m.properties)) {
                    if (error) *error = "bad properties in inflight record at line " + std::to_string(lineNo);
                    return false;
                }
                std::string expiryToken;
                if (ss >> expiryToken && !decodeMessageExpiry(expiryToken, m)) {
                    if (error) *error = "bad expiry in inflight record at line " + std::to_string(lineNo);
                    return false;
                }
                current->inflight.emplace_back(static_cast<uint16_t>(id), std::move(m));
            } else if (kind == "blocked") {
                unsigned long long id = 0;
                std::string topicHex, payloadHex;
                unsigned long long qos = 0, retain = 0;
                const unsigned long long maxQos = static_cast<unsigned long long>(QoS::QOS_2);
                if (!readBounded(ss, 0, id) || !(ss >> topicHex) ||
                    !readBounded(ss, maxQos, qos) || !readBounded(ss, 1, retain) ||
                    !(ss >> payloadHex)) {
                    if (error) *error = "bad blocked record at line " + std::to_string(lineNo);
                    return false;
                }
                Message m;
                if (!decodeMessage(topicHex, static_cast<int>(qos), retain != 0, payloadHex, m)) {
                    if (error) *error = "bad hex in blocked record at line " + std::to_string(lineNo);
                    return false;
                }
                std::string encodedProperties;
                if (ss >> encodedProperties && !decodeMessageProperties(encodedProperties, m.properties)) {
                    if (error) *error = "bad properties in blocked record at line " + std::to_string(lineNo);
                    return false;
                }
                std::string expiryToken;
                if (ss >> expiryToken && !decodeMessageExpiry(expiryToken, m)) {
                    if (error) *error = "bad expiry in blocked record at line " + std::to_string(lineNo);
                    return false;
                }
                current->blocked.push_back(std::move(m));
            } else if (kind == "outpubrel") {
                // Outgoing QoS 2 flow awaiting PUBCOMP; re-sent to the client on
                // resume (MQTT-4.4.0-1).
                unsigned long long id = 0;
                if (!readBounded(ss, 65535, id)) {
                    if (error) *error = "bad outpubrel record at line " + std::to_string(lineNo);
                    return false;
                }
                current->pendingPubrel.insert(static_cast<uint16_t>(id));
            } else {
                unsigned long long id = 0;
                if (!readBounded(ss, 65535, id)) {
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
