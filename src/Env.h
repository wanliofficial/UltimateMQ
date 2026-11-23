#ifndef MQTT_ENV_H
#define MQTT_ENV_H

#include <cerrno>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <strings.h>

namespace MQTT {

//! Parsing helpers for the opt-in behaviour knobs read from the environment.
//!
//! A boolean knob is enabled by presetting the variable to any value except an
//! explicit "off" one - "0", "false", "no" or an empty string (all
//! case-insensitive). Treating mere presence as "on" (getenv() != nullptr) is a
//! trap: FLOWMQ_X=0 reads as "set the variable to zero", which a human means as
//! "off", yet a presence check turns it on. These helpers make =0 mean off while
//! keeping a bare FLOWMQ_X="1" (or any non-empty, non-false value) as on.
class Env {
public:
    //! True when \p name is set to something other than 0/false/no/empty.
    static bool isEnabled(const char *name) {
        const char *value = std::getenv(name);
        if (!value || !*value) {
            return false;
        }
        return !(std::strcmp(value, "0") == 0 ||
                 strcasecmp(value, "false") == 0 ||
                 strcasecmp(value, "no") == 0);
    }

    //! Parse a positive integer limit from an environment value already known to
    //! be set and non-empty. Returns true and writes \p out on success; returns
    //! false for a non-numeric value, trailing junk, a leading '-', zero, an
    //! out-of-range value, or anything above \p max.
    //!
    //! The ERANGE check is the important one: strtoull reports an overflow (a
    //! value larger than it can represent, e.g. a mistyped "effectively
    //! unlimited" 20-digit number) by returning ULLONG_MAX and setting errno,
    //! NOT by clamping. Accepting that silently stores a near-infinite limit and
    //! quietly removes the resource cap the knob exists to provide (max
    //! connections, max packet size, ...). Fail loudly instead so the operator
    //! sees the typo. errno is reset first because strtoull only sets it on
    //! error and a stale ERANGE from an earlier call would misreport success.
    static bool parseLimit(const char *value, unsigned long long max,
                           unsigned long long &out) {
        if (!value || !*value) {
            return false;
        }
        errno = 0;
        char *end = nullptr;
        const unsigned long long parsed = std::strtoull(value, &end, 10);
        if (end == value || *end != '\0' || *value == '-' || parsed == 0 ||
            errno == ERANGE || parsed > max) {
            return false;
        }
        out = parsed;
        return true;
    }
};

} // namespace MQTT

#endif
