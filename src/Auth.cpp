#include "Auth.h"
#include "Topic.h"
#include <fstream>
#include <sstream>
#include <cstring>
#include <stdexcept>
#include <algorithm>

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
    auto val = [](char c) -> int {
        if (c >= '0' && c <= '9') return c - '0';
        if (c >= 'a' && c <= 'f') return c - 'a' + 10;
        if (c >= 'A' && c <= 'F') return c - 'A' + 10;
        return -1;
    };
    std::vector<uint8_t> out;
    out.reserve(hex.size() / 2);
    for (size_t i = 0; i + 1 < hex.size(); i += 2) {
        int hi = val(hex[i]);
        int lo = val(hex[i + 1]);
        if (hi < 0 || lo < 0) break;
        out.push_back(static_cast<uint8_t>((hi << 4) | lo));
    }
    return out;
}

std::string Auth::hashPassword(const std::string& salt, const std::string& password) {
    return sha256(salt + password);
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
        if (rest.rfind("$sha256$", 0) == 0) {
            std::string body = rest.substr(8);
            auto sep = body.find(':');
            if (sep == std::string::npos) {
                if (error) *error = "password file line " + std::to_string(lineNo) + ": bad $sha256$ entry";
                return false;
            }
            u.salt = body.substr(0, sep);
            u.hash = trim(body.substr(sep + 1));
        } else {
            u.salt.clear();
            u.hash = rest;
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
    std::string line;
    int lineNo = 0;
    while (std::getline(in, line)) {
        ++lineNo;
        std::string trimmed = trim(line);
        if (trimmed.empty() || trimmed[0] == '#') continue;
        std::stringstream ss(trimmed);
        std::string user, action, topic;
        if (!std::getline(ss, user, ',') || !std::getline(ss, action, ',') || !std::getline(ss, topic)) {
            if (error) *error = "acl file line " + std::to_string(lineNo) + ": expected user,read|write,topic-filter";
            return false;
        }
        user = trim(user);
        action = trim(action);
        topic = trim(topic);
        bool write;
        if (action == "write") write = true;
        else if (action == "read") write = false;
        else {
            if (error) *error = "acl file line " + std::to_string(lineNo) + ": action must be read or write";
            return false;
        }
        if (user.empty() || topic.empty()) {
            if (error) *error = "acl file line " + std::to_string(lineNo) + ": empty user or topic";
            return false;
        }
        acl_[user].emplace_back(topic, write);
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
    std::string candidate = u.salt.empty() ? password : hashPassword(u.salt, password);
    // Constant time-ish comparison; lengths differ only on malformed stored data
    if (candidate.size() != u.hash.size()) {
        return false;
    }
    unsigned diff = 0;
    for (size_t i = 0; i < candidate.size(); ++i) {
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

} // namespace MQTT
