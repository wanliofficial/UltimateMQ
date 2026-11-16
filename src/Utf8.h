#ifndef MQTT_UTF8_H
#define MQTT_UTF8_H

#include <cstddef>
#include <cstdint>
#include <string>

namespace MQTT {

//! Validation of the UTF-8 Encoded String type used by every text field in an
//! MQTT Control Packet (MQTT-1.5.4).
//!
//! The specification makes two separate demands and both are enforced here:
//!
//!  * the character data MUST be well-formed UTF-8 as restated in RFC 3629,
//!    which rules out stray continuation bytes, truncated sequences, overlong
//!    encodings, encoded surrogates U+D800..U+DFFF and anything above U+10FFFF
//!    [MQTT-1.5.4-1];
//!  * the string MUST NOT contain an encoding of U+0000 [MQTT-1.5.4-2].
//!
//! A receiver that gets either is required to treat the packet as malformed
//! ("the server must check, the client should check", OASIS MQTT-216), so the
//! caller reports MALFORMED_PACKET rather than a topic-specific reason code.
class Utf8 {
public:
    //! True when \p data is well-formed UTF-8 and contains no U+0000.
    static bool isValid(const std::string& data) {
        return isValid(reinterpret_cast<const uint8_t*>(data.data()), data.size());
    }

    static bool isValid(const uint8_t* data, size_t length) {
        size_t i = 0;
        while (i < length) {
            const uint8_t lead = data[i];
            if (lead == 0x00) {
                // U+0000 is forbidden outright, well-formed or not.
                return false;
            }
            if (lead < 0x80) {
                ++i;
                continue;
            }
            // Multi-byte sequence: work out how many continuation bytes follow
            // and the bounds the first code point must stay inside so that the
            // encoding is the shortest possible one.
            size_t continuation = 0;
            uint32_t codePoint = 0;
            uint32_t minimum = 0;
            if ((lead & 0xE0) == 0xC0) {
                continuation = 1;
                codePoint = lead & 0x1F;
                minimum = 0x80;          // reject overlong 2-byte forms
            } else if ((lead & 0xF0) == 0xE0) {
                continuation = 2;
                codePoint = lead & 0x0F;
                minimum = 0x800;         // reject overlong 3-byte forms
            } else if ((lead & 0xF8) == 0xF0) {
                continuation = 3;
                codePoint = lead & 0x07;
                minimum = 0x10000;       // reject overlong 4-byte forms
            } else {
                // 0x80..0xBF (stray continuation) or 0xF8..0xFF (never legal).
                return false;
            }
            if (i + continuation >= length) {
                return false;            // truncated sequence
            }
            for (size_t k = 1; k <= continuation; ++k) {
                const uint8_t byte = data[i + k];
                if ((byte & 0xC0) != 0x80) {
                    return false;        // not a continuation byte
                }
                codePoint = (codePoint << 6) | (byte & 0x3F);
            }
            if (codePoint < minimum) {
                return false;            // overlong encoding
            }
            if (codePoint >= 0xD800 && codePoint <= 0xDFFF) {
                return false;            // lone surrogate (MQTT-1.5.4-1)
            }
            if (codePoint > 0x10FFFF) {
                return false;            // outside the Unicode range
            }
            i += continuation + 1;
        }
        return true;
    }
};

} // namespace MQTT

#endif
