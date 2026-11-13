#ifndef FLOWMQ_LOG_H
#define FLOWMQ_LOG_H
#pragma once

#include <cstdarg>
#include <cstdio>
#include <cstdlib>

namespace MQTT {

/*!
 * Minimal logging helper.
 *
 * Debug traces are only emitted when the FLOWMQ_DEBUG environment variable is
 * set (to anything), so that a normal run keeps stdout/stderr clean while the
 * traces remain available when diagnosing a problem.
 */
inline bool debugEnabled() {
    static const bool enabled = std::getenv("FLOWMQ_DEBUG") != nullptr;
    return enabled;
}

inline void logDebug(const char *format, ...) {
    if (!debugEnabled()) {
        return;
    }
    va_list args;
    va_start(args, format);
    std::fputs("[flowmq] ", stderr);
    std::vfprintf(stderr, format, args);
    std::fputc('\n', stderr);
    va_end(args);
}

inline void logInfo(const char *format, ...) {
    va_list args;
    va_start(args, format);
    std::vfprintf(stdout, format, args);
    std::fputc('\n', stdout);
    va_end(args);
}

inline void logError(const char *format, ...) {
    va_list args;
    va_start(args, format);
    std::fputs("[flowmq] error: ", stderr);
    std::vfprintf(stderr, format, args);
    std::fputc('\n', stderr);
    va_end(args);
}

} // namespace MQTT

#endif
