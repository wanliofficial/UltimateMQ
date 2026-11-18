#include "Trie.h"
#include "Topic.h"
#include <sstream>
#include <functional>

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
    std::vector<std::string> topicLevels = Topic::split(topic);

    // MQTT-4.7.2-1: a wildcard at the first level must not match a topic
    // beginning with '$'. Skipping the wildcard branches at level 0 leaves only
    // exact (and therefore explicit "$...") first levels able to match.
    const bool reservedTopic = !topic.empty() && topic[0] == '$';

    // Helper function to perform DFS on the trie
    std::function<void(TrieNode *, size_t)> dfs = [&](TrieNode *node, size_t level) {
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
            return;
        }

        // Check for exact match
        auto it = node->children.find(topicLevels[level]);
        if (it != node->children.end()) {
            dfs(it->second.get(), level + 1);
        }

        if (reservedTopic && level == 0) {
            return;
        }

        // Check for '+' wildcard
        it = node->children.find("+");
        if (it != node->children.end()) {
            dfs(it->second.get(), level + 1);
        }

        // Check for '#' wildcard
        it = node->children.find("#");
        if (it != node->children.end() && it->second->topicFilter.has_value()) {
            // A '#' node must carry its own filter to be reportable. It normally
            // does: a '#' is only ever the last level, so the node it hangs on
            // is the leaf that insert() stamped. Guard the optional anyway, for
            // the same reason the level-end branch above does - a filter that
            // skipped validation (a restored state file) can leave a '#' node
            // with children but no filter of its own, and calling .value() on it
            // threw std::bad_optional_access out of a plain publish.
            matches.push_back(it->second->topicFilter.value());
        }
    };

    dfs(root.get(), 0);
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