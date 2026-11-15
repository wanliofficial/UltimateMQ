#ifndef TOPIC_H
#define TOPIC_H
#pragma once

#include <string>
#include <vector>

namespace MQTT {
namespace Topic {

// Split an MQTT topic into a vector of topic levels
std::vector<std::string> split(const std::string& topic);

// Join a vector of topic levels into a single MQTT topic
std::string join(const std::vector<std::string>& topicLevels);

// Validate if a topic is well-formed
bool isValid(const std::string& topic);
//! Whether \p topic is a legal topic *name*: no wildcard characters at all.
//! Filters may carry '+'/'#'; a PUBLISH topic or a retained topic may not.
bool isValidTopicName(const std::string& topic);

// Check if a topic matches a topic filter (supporting wildcards + and #)
bool match(const std::string& topic, const std::string& filter);

bool isShared(const std::string& topic);

std::pair<std::string, std::string> splitShared(const std::string& topic);

//! Whether two topic filters can ever match the same topic name. Used to decide
//! whether a subscription can reach anything an ACL rule grants: a filter that
//! cannot overlap any granted rule is refused outright, while a broader one is
//! granted and filtered per message.
bool filtersOverlap(const std::string& a, const std::string& b);

} 
}

#endif
