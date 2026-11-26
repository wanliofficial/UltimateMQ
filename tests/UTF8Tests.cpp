#include <gtest/gtest.h>
#include "../src/UTF8.h"

namespace MQTT
{

// MQTT-1.5.4-1: the character data must be well-formed UTF-8 as restated in
// RFC 3629. MQTT-1.5.4-2: it must not encode U+0000.

TEST(UTF8Test, AcceptsAsciiAndWellFormedMultiByteSequences)
{
    EXPECT_TRUE(UTF8::isValid(std::string("a/b/c")));
    EXPECT_TRUE(UTF8::isValid(std::string("")));          // empty is legal
    EXPECT_TRUE(UTF8::isValid(std::string("caf\xc3\xa9")));        // U+00E9
    EXPECT_TRUE(UTF8::isValid(std::string("\xe4\xb8\xad\xe6\x96\x87")));  // U+4E2D U+6587
    EXPECT_TRUE(UTF8::isValid(std::string("\xf0\x9f\x98" "\x80")));  // U+1F600 (4 bytes)
    // The largest legal code point, U+10FFFF.
    EXPECT_TRUE(UTF8::isValid(std::string("\xf4\x8f\xbf" "\xbf")));
}

TEST(UTF8Test, RejectsNullCharacter)
{
    // MQTT-1.5.4-2 forbids an encoding of U+0000 anywhere in the string.
    EXPECT_FALSE(UTF8::isValid(std::string("\x00", 1)));
    EXPECT_FALSE(UTF8::isValid(std::string("a\x00b", 3)));
    EXPECT_FALSE(UTF8::isValid(std::string("ab\x00", 3)));
    // An overlong encoding of U+0000 (0xC0 0x80) is ill-formed as well.
    EXPECT_FALSE(UTF8::isValid(std::string("a\xc0\x80", 3)));
}

TEST(UTF8Test, RejectsStrayAndTruncatedSequences)
{
    EXPECT_FALSE(UTF8::isValid(std::string("a\x80" "b", 3)));   // stray continuation
    EXPECT_FALSE(UTF8::isValid(std::string("a\xbf", 2)));
    EXPECT_FALSE(UTF8::isValid(std::string("a\xc3", 2)));    // truncated 2-byte
    EXPECT_FALSE(UTF8::isValid(std::string("a\xe4\xb8", 3))); // truncated 3-byte
    EXPECT_FALSE(UTF8::isValid(std::string("a\xf0\x9f\x98", 4))); // truncated 4-byte
    // A 3-byte lead followed by an ASCII byte is not a continuation.
    EXPECT_FALSE(UTF8::isValid(std::string("a\xe4" "ab", 4)));
    // 0xFE and 0xFF never appear in UTF-8.
    EXPECT_FALSE(UTF8::isValid(std::string("a\xfe", 2)));
    EXPECT_FALSE(UTF8::isValid(std::string("a\xff", 2)));
}

TEST(UTF8Test, RejectsEncodedSurrogates)
{
    // MQTT-1.5.4-1 names U+D800..U+DFFF explicitly: an encoded surrogate is
    // ill-formed UTF-8 even though the three bytes are structurally valid.
    EXPECT_FALSE(UTF8::isValid(std::string("a\xed\xa0\x80", 4)));  // U+D800
    EXPECT_FALSE(UTF8::isValid(std::string("a\xed\xbf\xbf", 4)));  // U+DFFF
}

TEST(UTF8Test, RejectsOverlongEncodings)
{
    // Every sequence must be the shortest one for its code point.
    EXPECT_FALSE(UTF8::isValid(std::string("a\xc0\xaf", 3)));      // '/' as 2 bytes
    EXPECT_FALSE(UTF8::isValid(std::string("a\xc1\xbf", 3)));
    EXPECT_FALSE(UTF8::isValid(std::string("a\xe0\x80\xaf", 4)));  // '/'-like 3 bytes
    EXPECT_FALSE(UTF8::isValid(std::string("a\xf0\x80\x80\xaf", 5)));
}

TEST(UTF8Test, RejectsCodePointsAboveUnicodeMaximum)
{
    EXPECT_FALSE(UTF8::isValid(std::string("a\xf4\x90\x80\x80", 5)));  // U+110000
    EXPECT_FALSE(UTF8::isValid(std::string("a\xf5\x80\x80\x80", 5)));
}

TEST(UTF8Test, ByteOrderMarkIsAccepted)
{
    // MQTT-1.5.4-3: a leading 0xEF 0xBB 0xBF is U+FEFF and must not be stripped,
    // so it also has to be considered well-formed input.
    EXPECT_TRUE(UTF8::isValid(std::string("\xef\xbb\xbf" "topic")));
}

TEST(UTF8Test, ReturnsFalseForBufferWithSize)
{
    // The pointer/length overload must not read past the given length.
    const uint8_t bytes[] = {'a', 'b', 'c'};
    EXPECT_TRUE(UTF8::isValid(bytes, 3));
    EXPECT_TRUE(UTF8::isValid(bytes, 0));
    const uint8_t truncated[] = {'a', 0xE4, 0xB8};
    EXPECT_FALSE(UTF8::isValid(truncated, sizeof(truncated)));
}

} // namespace MQTT
