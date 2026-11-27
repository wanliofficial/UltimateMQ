#include "Auth.h"
#include "Topic.h"
#include <fstream>
#include <sstream>
#include <cstring>
#include <stdexcept>
#include <algorithm>
#include <cerrno>
#include <cstdlib>
#include <openssl/evp.h>

namespace MQTT {

// --- Minimal, self-contained SHA-256 (public domain style implementation) ---
namespace {
struct Sha256 {
    uint32_t state[8];
    uint64_t bitlen = 0;
    uint8_t data[64];
    size_t datalen = 0;

    static uint32_t rotr(uint32_t x, uint32_t n) { return (x >> n) | (x << (32 - n)); }

    void init() {
        state[0] = 0x6a09e667; state[1] = 0xbb67ae85; state[2] = 0x3c6ef372; state[3] = 0xa54ff53a;
        state[4] = 0x510e527f; state[5] = 0x9b05688c; state[6] = 0x1f83d9ab; state[7] = 0x5be0cd19;
        datalen = 0;
        bitlen = 0;
    }

    void transform(const uint8_t* data) {
        static const uint32_t k[64] = {
            0x428a2f98,0x71374491,0xb5c0fbcf,0xe9b5dba5,0x3956c25b,0x59f111f1,0x923f82a4,0xab1c5ed5,
            0xd807aa98,0x12835b01,0x243185be,0x550c7dc3,0x72be5d74,0x80deb1fe,0x9bdc06a7,0xc19bf174,
            0xe49b69c1,0xefbe4786,0x0fc19dc6,0x240ca1cc,0x2de92c6f,0x4a7484aa,0x5cb0a9dc,0x76f988da,
            0x983e5152,0xa831c66d,0xb00327c8,0xbf597fc7,0xc6e00bf3,0xd5a79147,0x06ca6351,0x14292967,
            0x27b70a85,0x2e1b2138,0x4d2c6dfc,0x53380d13,0x650a7354,0x766a0abb,0x81c2c92e,0x92722c85,
            0xa2bfe8a1,0xa81a664b,0xc24b8b70,0xc76c51a3,0xd192e819,0xd6990624,0xf40e3585,0x106aa070,
            0x19a4c116,0x1e376c08,0x2748774c,0x34b0bcb5,0x391c0cb3,0x4ed8aa4a,0x5b9cca4f,0x682e6ff3,
            0x748f82ee,0x78a5636f,0x84c87814,0x8cc70208,0x90befffa,0xa4506ceb,0xbef9a3f7,0xc67178f2
        };
        uint32_t m[64];
        for (int i = 0, j = 0; i < 16; ++i, j += 4)
            m[i] = (data[j] << 24) | (data[j + 1] << 16) | (data[j + 2] << 8) | data[j + 3];
        for (int i = 16; i < 64; ++i) {
            uint32_t s0 = rotr(m[i - 15], 7) ^ rotr(m[i - 15], 18) ^ (m[i - 15] >> 3);
            uint32_t s1 = rotr(m[i - 2], 17) ^ rotr(m[i - 2], 19) ^ (m[i - 2] >> 10);
            m[i] = m[i - 16] + s0 + m[i - 7] + s1;
        }
        uint32_t a = state[0], b = state[1], c = state[2], d = state[3];
        uint32_t e = state[4], f = state[5], g = state[6], h = state[7];
        for (int i = 0; i < 64; ++i) {
            uint32_t S1 = rotr(e, 6) ^ rotr(e, 11) ^ rotr(e, 25);
            uint32_t ch = (e & f) ^ (~e & g);
            uint32_t t1 = h + S1 + ch + k[i] + m[i];
            uint32_t S0 = rotr(a, 2) ^ rotr(a, 13) ^ rotr(a, 22);
            uint32_t maj = (a & b) ^ (a & c) ^ (b & c);
            uint32_t t2 = S0 + maj;
            h = g; g = f; f = e; e = d + t1;
            d = c; c = b; b = a; a = t1 + t2;
        }
        state[0] += a; state[1] += b; state[2] += c; state[3] += d;
        state[4] += e; state[5] += f; state[6] += g; state[7] += h;
    }

    void update(const uint8_t* in, size_t len) {
        for (size_t i = 0; i < len; ++i) {
            data[datalen++] = in[i];
            if (datalen == 64) {
                transform(data);
                bitlen += 512;
                datalen = 0;
            }
        }
    }

    void final(uint8_t* hash) {
        size_t i = datalen;
        data[i++] = 0x80;
        if (datalen < 56) {
            while (i < 56) data[i++] = 0;
        } else {
            while (i < 64) data[i++] = 0;
            transform(data);
            memset(data, 0, 56);
        }
        bitlen += datalen * 8;
        for (int j = 0; j < 8; ++j)
            data[56 + j] = static_cast<uint8_t>(bitlen >> (56 - j * 8));
        transform(data);
        for (int j = 0; j < 8; ++j) {
            hash[j * 4] = static_cast<uint8_t>(state[j] >> 24);
            hash[j * 4 + 1] = static_cast<uint8_t>(state[j] >> 16);
            hash[j * 4 + 2] = static_cast<uint8_t>(state[j] >> 8);
            hash[j * 4 + 3] = static_cast<uint8_t>(state[j]);
        }
    }
};

std::string trim(const std::string& s) {
    size_t a = s.find_first_not_of(" \t\r\n");
    if (a == std::string::npos) return "";
    size_t b = s.find_last_not_of(" \t\r\n");
    return s.substr(a, b - a + 1);
}

//! Decode a hex string to bytes, rejecting malformed input instead of silently
//! stopping at the first bad nibble. The users file is configuration, so a
//! truncated or corrupted hash must be a load error, not a credential that
//! quietly becomes "no password".
bool decodeHexStrict(const std::string& hex, std::string& out) {
    if (hex.size() % 2 != 0) {
        return false;
    }
    out.clear();
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        auto nibble = [](char c) -> int {
            if (c >= '0' && c <= '9') return c - '0';
            if (c >= 'a' && c <= 'f') return c - 'a' + 10;
            if (c >= 'A' && c <= 'F') return c - 'A' + 10;
            return -1;
        };
        const int hi = nibble(hex[i]);
        const int lo = nibble(hex[i + 1]);
        if (hi < 0 || lo < 0) {
            return false;
        }
        out.push_back(static_cast<char>((hi << 4) | lo));
    }
    return true;
}

//! PBKDF2-HMAC-SHA256. Key stretching is what makes a pre-hashed password file
//! resistant to offline cracking: a single salted SHA-256 can be brute-forced
//! at billions of guesses per second on a GPU, while the iteration count here
//! costs an attacker the same work per guess.
std::string pbkdf2Sha256(const std::string& password, const std::string& salt,
                         uint32_t iterations) {
    unsigned char out[32];
    if (PKCS5_PBKDF2_HMAC(password.data(), static_cast<int>(password.size()),
                          reinterpret_cast<const unsigned char*>(salt.data()),
                          static_cast<int>(salt.size()), static_cast<int>(iterations),
                          EVP_sha256(), static_cast<int>(sizeof(out)), out) != 1) {
        // Cannot happen for valid inputs; refuse rather than compare a bogus
        // digest (an empty return never equals a stored hash).
        return std::string();
    }
    static const char* digits = "0123456789abcdef";
    std::string hex;
    hex.reserve(sizeof(out) * 2);
    for (unsigned char c : out) {
        hex.push_back(digits[c >> 4]);
        hex.push_back(digits[c & 0x0f]);
    }
    return hex;
}
} // namespace

std::string Auth::sha256(const std::string& data) {
    Sha256 ctx;
    ctx.init();
    ctx.update(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    uint8_t out[32];
    ctx.final(out);
    return toHex(out, 32);
}

std::string Auth::toHex(const uint8_t* data, size_t len) {
    static const char* digits = "0123456789abcdef";
    std::string out;
    out.reserve(len * 2);
    for (size_t i = 0; i < len; ++i) {
        out.push_back(digits[data[i] >> 4]);
        out.push_back(digits[data[i] & 0x0f]);
    }
    return out;
}

std::vector<uint8_t> Auth::fromHex(const std::string& hex) {
    std::vector<uint8_t> out;
    std::string raw;
    if (!decodeHexStrict(hex, raw)) {
        return out;
    }
    out.assign(raw.begin(), raw.end());
    return out;
}

std::string Auth::hashPassword(const std::string& salt, const std::string& password) {
    return sha256(salt + password);
}

std::string Auth::pbkdf2Password(const std::string& salt, const std::string& password,
                                 uint32_t iterations) {
    return pbkdf2Sha256(password, salt, iterations);
}

bool Auth::loadUsers(const std::string& path, std::string* error) {
    std::ifstream in(path);
    if (!in) {
        if (error) *error = "cannot open password file: " + path;
        return false;
    }
    users_.clear();
    std::string line;
    int lineNo = 0;
    while (std::getline(in, line)) {
        ++lineNo;
        std::string trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#') continue;
        auto colon = trimmed.find(':');
        if (colon == std::string::npos) {
            if (error) *error = "password file line " + std::to_string(lineNo) + ": expected user:password";
            return false;
        }
        std::string user = trim(trimmed.substr(0, colon));
        std::string rest = trim(trimmed.substr(colon + 1));
        if (user.empty()) {
            if (error) *error = "password file line " + std::to_string(lineNo) + ": empty username";
            return false;
        }
        User u;
        static const std::string pbkdf2Prefix = "$pbkdf2-sha256$";
        if (rest.rfind(pbkdf2Prefix, 0) == 0) {
            // $pbkdf2-sha256$<iterations>$<salt-hex>$<hash-hex>
            std::string body = rest.substr(pbkdf2Prefix.size());
            const auto first = body.find('$');
            const auto second = first == std::string::npos ? std::string::npos
                                                          : body.find('$', first + 1);
            if (first == std::string::npos || second == std::string::npos) {
                if (error) *error = "password file line " + std::to_string(lineNo) +
                                    ": expected $pbkdf2-sha256$<iterations>$<salt-hex>$<hash-hex>";
                return false;
            }
            const std::string iterationText = body.substr(0, first);
            const std::string saltHex = body.substr(first + 1, second - first - 1);
            const std::string hashHex = trim(body.substr(second + 1));
            // Reject an unparseable or zero iteration count rather than falling
            // back to an un-stretched hash: the count is the whole point of using
            // this format.
            char* end = nullptr;
            errno = 0;
            const unsigned long long iterations = std::strtoull(iterationText.c_str(), &end, 10);
            if (iterationText.empty() || end == iterationText.c_str() || *end != '\0' ||
                iterations == 0 || iterations > 100000000ull) {
                if (error) *error = "password file line " + std::to_string(lineNo) +
                                    ": invalid PBKDF2 iteration count";
                return false;
            }
            // The salt is kept as the hex text the file carries, and
            // authenticate() derives with those exact bytes, so tooling that
            // computes pbkdf2Password("<salt-hex>", ...) from the file matches.
            // Validate it is well-formed hex all the same, so a typo cannot turn
            // into a silently different salt.
            std::string ignoredSaltBytes;
            if (!decodeHexStrict(saltHex, ignoredSaltBytes)) {
                if (error) *error = "password file line " + std::to_string(lineNo) +
                                    ": malformed hex in $pbkdf2-sha256$ salt";
                return false;
            }
            u.salt = saltHex;
            // Keep the digest as canonical lowercase hex so it compares directly
            // against the hex candidate that authenticate() derives.
            std::string rawHash;
            if (!decodeHexStrict(hashHex, rawHash)) {
                if (error) *error = "password file line " + std::to_string(lineNo) +
                                    ": malformed hex in $pbkdf2-sha256$ hash";
                return false;
            }
            u.hash = toHex(reinterpret_cast<const uint8_t *>(rawHash.data()), rawHash.size());
            u.iterations = static_cast<uint32_t>(iterations);
            u.kind = User::Kind::PBKDF2;
        } else if (rest.rfind("$sha256$", 0) == 0) {
            std::string body = rest.substr(std::string("$sha256$").size());
            auto sep = body.find(':');
            if (sep == std::string::npos) {
                if (error) *error = "password file line " + std::to_string(lineNo) + ": bad $sha256$ entry";
                return false;
            }
            // The salt is used as raw text by the legacy scheme, so keep it
            // verbatim; only the digest is hex, and a malformed digest is a load
            // error rather than an account whose password never matches.
            u.salt = body.substr(0, sep);
            const std::string hashHex = trim(body.substr(sep + 1));
            std::string rawHash;
            if (!decodeHexStrict(hashHex, rawHash)) {
                if (error) *error = "password file line " + std::to_string(lineNo) +
                                    ": malformed hex in $sha256$ entry";
                return false;
            }
            // Canonical lowercase hex, so authenticate()'s hex candidate compares
            // directly (and an upper-case file still matches).
            u.hash = toHex(reinterpret_cast<const uint8_t *>(rawHash.data()), rawHash.size());
            u.kind = User::Kind::SHA256;
        } else {
            u.salt.clear();
            u.hash = rest;
            u.kind = User::Kind::PLAINTEXT;
        }
        users_[user] = std::move(u);
    }
    authRequired_ = !users_.empty();
    return true;
}

bool Auth::loadAcl(const std::string& path, std::string* error) {
    std::ifstream in(path);
    if (!in) {
        if (error) *error = "cannot open acl file: " + path;
        return false;
    }
    acl_.clear();
    kafkaAcl_.clear();
    kafkaAclEnabled_ = false;
    std::string line;
    int lineNo = 0;
    while (std::getline(in, line)) {
        ++lineNo;
        std::string trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#') continue;
        std::stringstream ss(trimmed);
        std::string user, action, resource;
        if (!std::getline(ss, user, ',') || !std::getline(ss, action, ',') || !std::getline(ss, resource)) {
            if (error) *error = "acl file line " + std::to_string(lineNo) + ": expected user,action,resource";
            return false;
        }
        user = trim(user);
        action = trim(action);
        resource = trim(resource);
        if (user.empty() || resource.empty()) {
            if (error) *error = "acl file line " + std::to_string(lineNo) + ": empty user or resource";
            return false;
        }
        if (action == "read" || action == "write") {
            acl_[user].emplace_back(resource, action == "write");
            continue;
        }
        std::optional<KafkaOperation> operation;
        if (action == "group-read") operation = KafkaOperation::GROUP_READ;
        else if (action == "group-write") operation = KafkaOperation::GROUP_WRITE;
        else if (action == "group-describe") operation = KafkaOperation::GROUP_DESCRIBE;
        else if (action == "group-delete") operation = KafkaOperation::GROUP_DELETE;
        else if (action == "txn-id-write") operation = KafkaOperation::TRANSACTIONAL_ID_WRITE;
        else if (action == "cluster-admin") {
            if (resource != "cluster") {
                if (error) *error = "acl file line " + std::to_string(lineNo) + ": cluster resource must be cluster";
                return false;
            }
            operation = KafkaOperation::CLUSTER_ADMIN;
        } else {
            if (error) *error = "acl file line " + std::to_string(lineNo) + ": unsupported action " + action;
            return false;
        }
        kafkaAcl_[user].emplace_back(resource, *operation);
        kafkaAclEnabled_ = true;
    }
    aclEnabled_ = !acl_.empty();
    return true;
}

bool Auth::authenticate(const std::string& username, const std::string& password) const {
    if (!authRequired_) {
        // No user database configured: open broker, everyone is allowed
        return true;
    }
    auto it = users_.find(username);
    if (it == users_.end()) {
        return false;
    }
    const User& u = it->second;
    std::string candidate;
    switch (u.kind) {
    case User::Kind::PLAINTEXT:
        candidate = password;
        break;
    case User::Kind::SHA256:
        // Legacy scheme: a single salted SHA-256. Kept so existing password
        // files keep working; new credentials should use $pbkdf2-sha256$.
        candidate = hashPassword(u.salt, password);
        break;
    case User::Kind::PBKDF2:
        // Stretch the candidate with the same salt and iteration count the file
        // records, so an attacker with the file pays the same per-guess cost.
        candidate = pbkdf2Sha256(password, u.salt, u.iterations);
        break;
    }
    // Constant-time comparison: run the XOR over the common prefix and fold the
    // length mismatch into the accumulator instead of returning early, so the
    // response time does not reveal which stored hash (and therefore which
    // account) the caller hit (CWE-208).
    const size_t common = std::min(candidate.size(), u.hash.size());
    unsigned diff = (candidate.size() == u.hash.size()) ? 0u : 1u;
    for (size_t i = 0; i < common; ++i) {
        diff |= static_cast<unsigned>(candidate[i]) ^ static_cast<unsigned>(u.hash[i]);
    }
    return diff == 0;
}

bool Auth::checkAcl(const std::string& username, const std::string& topic, bool write) const {
    // Check the specific user first, then the wildcard "*" entry, if any
    for (const std::string& key : {username, std::string("*")}) {
        auto it = acl_.find(key);
        if (it == acl_.end()) continue;
        for (const auto& [filter, canWrite] : it->second) {
            if (canWrite != write) continue;
            if (Topic::match(topic, filter)) {
                return true;
            }
        }
    }
    return false;
}

bool Auth::isForbidden(const std::string& username, const std::string& topic, bool write) const {
    if (!aclEnabled_) {
        return false;
    }
    return !checkAcl(username, topic, write);
}

bool Auth::isKafkaForbidden(const std::string& username, const std::string& resource,
                            KafkaOperation operation) const {
    if (!kafkaAclEnabled_) return false;
    for (const std::string& key : {username, std::string("*")}) {
        auto found = kafkaAcl_.find(key);
        if (found == kafkaAcl_.end()) continue;
        for (const auto& rule : found->second) {
            if (rule.second != operation) continue;
            const std::string& pattern = rule.first;
            if (pattern == "*" || pattern == resource ||
                (pattern.back() == '*' && resource.compare(0, pattern.size() - 1, pattern, 0, pattern.size() - 1) == 0)) {
                return false;
            }
        }
    }
    return true;
}

bool Auth::isForbiddenSubscribe(const std::string& username, const std::string& filter) const {
    if (!aclEnabled_) {
        return false;
    }
    // Authorise a shared subscription by its real filter: "$share/group/" is
    // routing metadata, and an ACL rule names a topic, never that prefix.
    const std::string realFilter =
        Topic::isShared(filter) ? Topic::splitShared(filter).second : filter;
    for (const std::string& key : {username, std::string("*")}) {
        auto it = acl_.find(key);
        if (it == acl_.end()) continue;
        for (const auto& [rule, canWrite] : it->second) {
            if (canWrite) continue;
            // The subscription is useful when it can match at least one topic
            // the rule grants. Delivering anything else stays denied per
            // message in isForbidden(), so a broad filter cannot leak.
            if (Topic::filtersOverlap(realFilter, rule)) {
                return false;
            }
        }
    }
    return true;
}

} // namespace MQTT
