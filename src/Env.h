#ifndef MQTT_ENV_H
#define MQTT_ENV_H

#include <cstdlib>
#include <cstring>
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
};

} // namespace MQTT

#endif
