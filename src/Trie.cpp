#include "Trie.h"
#include "Topic.h"
#include <sstream>

namespace MQTT {

void Trie::insert(const std::string &topicFilter)
{
    TrieNode *current = root.get();
    auto levels = Topic::split(topicFilter);
    for (const auto &level : levels) {
        if (current->children.find(level) == current->children.end()) {
            current->children[level] = std::make_unique<TrieNode>();
        }
        current = current->children[level].get();
    }
    current->topicFilter = topicFilter;
}

std::vector<std::string> Trie::match(const std::string &topic)
{
    std::vector<std::string> matches;
    const std::vector<std::string> topicLevels = Topic::split(topic);

    // MQTT-4.7.2-1: a wildcard at the first level must not match a topic
    // beginning with '$'. Skipping the wildcard branches at level 0 leaves only
    // exact (and therefore explicit "$...") first levels able to match.
    const bool reservedTopic = !topic.empty() && topic[0] == '$';

    // The trie is walked with an explicit stack rather than a recursive lambda.
    // Recursion depth is the topic's level count, and a level is only bounded by
    // the 65535-byte UTF-8 string limit, so a topic of a few tens of thousands of
    // levels overflowed the 512 KB stack of the connection thread that runs
    // Broker::publish() -> match(). That killed the whole broker process (a
    // remote denial of service) on an ordinary SUBSCRIBE + PUBLISH. An explicit
    // stack is bounded by the trie size and cannot overflow.
    //
    // A frame either walks one node at one level, or (when \c emit is set) only
    // reports one filter that was already matched. The emit frames keep the
    // original visitation order, where a '#' match that fires mid-topic is
    // reported after every deeper branch of the same level.
    struct Frame {
        const TrieNode *node = nullptr;
        size_t level = 0;
        const std::string *emit = nullptr;
    };

    std::vector<Frame> stack;
    stack.push_back({root.get(), 0, nullptr});
    while (!stack.empty()) {
        const Frame frame = stack.back();
        stack.pop_back();
        if (frame.emit != nullptr) {
            matches.push_back(*frame.emit);
            continue;
        }
        const TrieNode *node = frame.node;
        const size_t level = frame.level;

        if (level == topicLevels.size()) {
            if (node->topicFilter.has_value()) {
                matches.push_back(node->topicFilter.value());
            }
            // MQTT-4.7.1-2: the multi-level wildcard "represents the parent and
            // any number of child levels", so "sport/#" also matches "sport".
            // The '#' node hangs one level below the node the whole topic
            // reached, so the wildcard has to be checked here as well.
            auto hash = node->children.find("#");
            if (hash != node->children.end() && hash->second->topicFilter.has_value()) {
                matches.push_back(hash->second->topicFilter.value());
            }
            continue;
        }

        // A '#' node must carry its own filter to be reportable. It normally
        // does: a '#' is only ever the last level, so the node it hangs on is the
        // leaf that insert() stamped. Guard the optional anyway, for the same
        // reason the level-end branch above does - a filter that skipped
        // validation (a restored state file) can leave a '#' node with children
        // but no filter of its own, and calling .value() on it threw
        // std::bad_optional_access out of a plain publish.
        const bool wildcardsAllowed = !(reservedTopic && level == 0);
        auto hash = node->children.find("#");
        const bool hashLeaf =
            wildcardsAllowed && hash != node->children.end() &&
            hash->second->topicFilter.has_value();
        auto plus = node->children.find("+");
        const bool plusLeaf = wildcardsAllowed && plus != node->children.end();
        auto exact = node->children.find(topicLevels[level]);

        // Push in reverse visitation order (the stack pops them back to front):
        // exact child, then '+' child, then the '#' match that is reported last.
        if (hashLeaf) {
            stack.push_back({nullptr, 0, &hash->second->topicFilter.value()});
        }
        if (plusLeaf) {
            stack.push_back({plus->second.get(), level + 1, nullptr});
        }
        if (exact != node->children.end()) {
            stack.push_back({exact->second.get(), level + 1, nullptr});
        }
    }
    return matches;
}

void Trie::remove(const std::string &topicFilter)
{
    std::vector<std::string> levels = Topic::split(topicFilter);
    std::vector<TrieNode *> path;
    TrieNode *current = root.get();

    // Traverse the trie to find the node to remove
    for (const auto &level : levels) {
        auto it = current->children.find(level);
        if (it == current->children.end()) {
            return; // Topic filter not found
        }
        path.push_back(current);
        current = it->second.get();
    }

    // Clear the topicFilter of the leaf node
    current->topicFilter = std::nullopt;

    // Remove unnecessary nodes
    for (int i = path.size() - 1; i >= 0; --i) {
        TrieNode *parent = path[i];
        const auto &level = levels[i];

        if (current->children.empty() && !current->topicFilter.has_value()) {
            parent->children.erase(level);
            current = parent;
        } else {
            break;
        }
    }
}

}