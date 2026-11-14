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
    // Check if the topic is empty
    if (topic.empty()) {
        return false;
    }

    // Split the topic into levels
    std::vector<std::string> levels = split(topic);

    // Check each level
    for (const auto& level : levels) {
        // Check for invalid characters
        for (char c : level) {
            if (c == '#' || c == '+') {
                return false;
            }
        }
    }
    return true;
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

}