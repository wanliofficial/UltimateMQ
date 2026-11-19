#ifndef MQTT_SHA1_H
#define MQTT_SHA1_H
#pragma once

#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <string>

namespace MQTT {

//! Minimal SHA-1 (FIPS 180-4) used for the WebSocket handshake only: the
//! Sec-WebSocket-Accept header is base64(SHA1(key + GUID)) (RFC 6455 §4.2.1).
//! This is not a general-purpose hash for authentication; credentials use the
//! salted digests in Auth.
class SHA1 {
public:
    static std::array<uint8_t, 20> digest(const uint8_t *data, size_t length) {
        uint32_t state[5] = {0x67452301, 0xEFCDAB89, 0x98BADCFE, 0x10325476,
                             0xC3D2E1F0};
        uint64_t totalBits = static_cast<uint64_t>(length) * 8;
        size_t offset = 0;
        // Absorb every full 64-byte block
        while (offset + 64 <= length) {
            processBlock(state, data + offset);
            offset += 64;
        }
        // Tail: up to 63 bytes, padded with 0x80 and the 64-bit bit count
        uint8_t tail[128];
        size_t tailLength = length - offset;
        std::memcpy(tail, data + offset, tailLength);
        tail[tailLength++] = 0x80;
        // Zero-pad until the block holds room for the length (mod 64 == 56)
        while (tailLength % 64 != 56) {
            tail[tailLength++] = 0x00;
        }
        for (int i = 0; i < 8; ++i) {
            tail[tailLength++] = static_cast<uint8_t>(totalBits >> (56 - 8 * i));
        }
        processBlock(state, tail);
        if (tailLength > 64) {
            processBlock(state, tail + 64);
        }

        std::array<uint8_t, 20> out{};
        for (int i = 0; i < 5; ++i) {
            out[4 * i] = static_cast<uint8_t>(state[i] >> 24);
            out[4 * i + 1] = static_cast<uint8_t>(state[i] >> 16);
            out[4 * i + 2] = static_cast<uint8_t>(state[i] >> 8);
            out[4 * i + 3] = static_cast<uint8_t>(state[i]);
        }
        return out;
    }

private:
    static uint32_t rotateLeft(uint32_t value, unsigned bits) {
        return (value << bits) | (value >> (32 - bits));
    }

    static void processBlock(uint32_t *state, const uint8_t *block) {
        uint32_t w[80];
        for (int i = 0; i < 16; ++i) {
            w[i] = (static_cast<uint32_t>(block[4 * i]) << 24) |
                   (static_cast<uint32_t>(block[4 * i + 1]) << 16) |
                   (static_cast<uint32_t>(block[4 * i + 2]) << 8) |
                   static_cast<uint32_t>(block[4 * i + 3]);
        }
        for (int i = 16; i < 80; ++i) {
            w[i] = rotateLeft(w[i - 3] ^ w[i - 8] ^ w[i - 14] ^ w[i - 16], 1);
        }
        uint32_t a = state[0], b = state[1], c = state[2], d = state[3], e = state[4];
        for (int i = 0; i < 80; ++i) {
            uint32_t f, k;
            if (i < 20) {
                f = (b & c) | ((~b) & d);
                k = 0x5A827999;
            } else if (i < 40) {
                f = b ^ c ^ d;
                k = 0x6ED9EBA1;
            } else if (i < 60) {
                f = (b & c) | (b & d) | (c & d);
                k = 0x8F1BBCDC;
            } else {
                f = b ^ c ^ d;
                k = 0xCA62C1D6;
            }
            uint32_t temp = rotateLeft(a, 5) + f + e + k + w[i];
            e = d;
            d = c;
            c = rotateLeft(b, 30);
            b = a;
            a = temp;
        }
        state[0] += a;
        state[1] += b;
        state[2] += c;
        state[3] += d;
        state[4] += e;
    }
};

} // namespace MQTT

#endif
