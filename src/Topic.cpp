#include "Topic.h"
#include <sstream>

namespace MQTT {

// Split an MQTT topic into a vector of topic levels
std::vector<std::string> Topic::split(const std::string &topic) {
    std::vector<std::string> result;
    std::string level;
    std::istringstream topicStream(topic);

    while (std::getline(topicStream, level, '/')) {
        result.push_back(level);
    }

    return result;
}

// Join a vector of topic levels into a single MQTT topic
std::string Topic::join(const std::vector<std::string> &topicLevels) {
    bool isFirst = true;
    std::ostringstream topicStream;
    for (const auto &level : topicLevels) {
        topicStream << (isFirst ? "" : "/") << level;
        if (isFirst) isFirst = false;
    }
    return topicStream.str();
}

// Validate if a topic is well-formed
bool Topic::isValid(const std::string &topic) {
    // MQTT-3.3.2-1: a topic (or filter) must not be empty
    if (topic.empty()) {
        return false;
    }

    // A shared subscription has the shape "$share/{ShareName}/{filter}"
    // (§4.8.2). Its ShareName is not a topic level: it must be at least one
    // character long and must not contain '/', '+' or '#' (MQTT-4.8.2-1 /
    // MQTT-4.8.2-2), and a '/' followed by a real Topic Filter must come after
    // it. Without this check "$share//t" and "$share/g" were accepted as
    // ordinary filters that simply never match, and a wildcard in the
    // ShareName was treated as one.
    const std::string sharedPrefix = "$share/";
    if (topic.compare(0, sharedPrefix.size(), sharedPrefix) == 0) {
        const size_t secondSlash = topic.find('/', sharedPrefix.size());
        if (secondSlash == std::string::npos || secondSlash == sharedPrefix.size()) {
            return false;
        }
        const std::string shareName = topic.substr(sharedPrefix.size(),
                                                   secondSlash - sharedPrefix.size());
        if (shareName.find_first_of("+#") != std::string::npos) {
            return false;
        }
        // The remainder is an ordinary Topic Filter and is validated as one.
        return isValid(topic.substr(secondSlash + 1));
    }

    // Split the topic into levels
    std::vector<std::string> levels = split(topic);

    // A wildcard character must occupy an entire level
    for (const auto& level : levels) {
        if (level.find('#') != std::string::npos || level.find('+') != std::string::npos) {
            // The only whole-level wildcards are a bare '+' or a bare '#'
            if (level != "+" && level != "#") {
                return false;
            }
        }
    }

    // A '#' must be the last level (MQTT-3.8.4-1)
    for (size_t i = 0; i < levels.size(); ++i) {
        if (levels[i] == "#" && i != levels.size() - 1) {
            return false;
        }
    }
    return true;
}

//! Whether a string is a legal topic *name* (a PUBLISH topic, or a retained
//! topic): wildcards are not allowed at all (MQTT-3.3.2-1).
bool Topic::isValidTopicName(const std::string &topic) {
    if (topic.empty()) {
        return false;
    }
    return topic.find('#') == std::string::npos && topic.find('+') == std::string::npos;
}

// Check if a topic matches a topic filter (supporting wildcards + and #)
bool Topic::match(const std::string &topic, const std::string &topicFilter) {
    // MQTT-4.7.2-1: a filter whose first level is a wildcard never matches a
    // topic whose first character is '$' (reserved for $SYS and similar). Only
    // an explicit "$..." first level can match such a topic.
    if (!topic.empty() && topic[0] == '$' && !topicFilter.empty() &&
        (topicFilter[0] == '#' || topicFilter[0] == '+')) {
        return false;
    }
    std::vector<std::string> topicLevels = split(topic);
    std::vector<std::string> filterLevels = split(topicFilter);

    size_t topicSize = topicLevels.size();
    size_t filterSize = filterLevels.size();
    size_t i = 0;

    for (; i < filterSize; ++i) {
        if (filterLevels[i] == "#") {
            return true;  // '#' matches any number of levels
        }
        
        if (i >= topicSize) {
            return false;  // Filter is longer than the topic
        }
        
        if (filterLevels[i] != "+" && filterLevels[i] != topicLevels[i]) {
            return false;  // Levels don't match and it's not a '+' wildcard
        }
    }

    // Check if we've matched all levels of both the topic and the filter
    return i == topicSize;
}

bool Topic::isShared(const std::string& topic) {
    // Check if the topic starts with "$share/" followed by a group name and a topic filter
    const std::string prefix = "$share/";
    if (topic.compare(0, prefix.size(), prefix) != 0) {
        return false;
    }
    // There must be a second '/' separating the group name from the actual topic filter
    size_t secondSlash = topic.find('/', prefix.size());
    if (secondSlash == std::string::npos) {
        return false;
    }
    // Group name must not be empty
    return secondSlash > prefix.size();
}

std::pair<std::string, std::string> Topic::splitShared(const std::string& topic) {
    // Check if the topic has the shared subscription prefix at all
    const std::string prefix = "$share/";
    if (topic.compare(0, prefix.size(), prefix) != 0) {
        return {"", topic};
    }

    // Find the second '/' which separates the group name from the actual topic
    size_t secondSlash = topic.find('/', prefix.size());
    if (secondSlash == std::string::npos || secondSlash == prefix.size()) {
        // Invalid shared subscription format
        return {"", ""};
    }

    // Extract the group name and the actual topic
    std::string groupName = topic.substr(7, secondSlash - 7);
    std::string actualTopic = topic.substr(secondSlash + 1);

    return {groupName, actualTopic};
}

bool Topic::filtersOverlap(const std::string& a, const std::string& b) {
    const std::vector<std::string> aLevels = split(a);
    const std::vector<std::string> bLevels = split(b);
    size_t i = 0;
    size_t j = 0;
    while (true) {
        // '#' matches any number of levels (including none), so it can consume
        // the whole remainder of the other filter: the two always overlap.
        if (i < aLevels.size() && aLevels[i] == "#") {
            return true;
        }
        if (j < bLevels.size() && bLevels[j] == "#") {
            return true;
        }
        if (i == aLevels.size() && j == bLevels.size()) {
            return true;  // Both ended together: the filters are the same
        }
        if (i == aLevels.size() || j == bLevels.size()) {
            return false;  // One filter has more levels than the other
        }
        // '+' stands for exactly one level, so it overlaps anything.
        if (aLevels[i] != "+" && bLevels[j] != "+" && aLevels[i] != bLevels[j]) {
            return false;
        }
        ++i;
        ++j;
    }
}

}