#include "KafkaConnect.h"
#include "Log.h"
#include <arpa/inet.h>
#include <cerrno>
#include <cctype>
#include <cstring>
#include <limits>
#include <poll.h>
#include <sstream>
#include <string_view>
#include <stdexcept>
#include <sys/socket.h>
#include <unistd.h>

namespace Kafka {
namespace {

constexpr size_t MAX_REQUEST_BYTES = 1024 * 1024;

std::string jsonString(const std::string& value) {
    std::string result = "\"";
    for (unsigned char ch : value) {
        switch (ch) {
        case '"': result += "\\\""; break;
        case '\\': result += "\\\\"; break;
        case '\b': result += "\\b"; break;
        case '\f': result += "\\f"; break;
        case '\n': result += "\\n"; break;
        case '\r': result += "\\r"; break;
        case '\t': result += "\\t"; break;
        default:
            if (ch < 0x20) {
                const char* digits = "0123456789abcdef";
                result += "\\u00";
                result.push_back(digits[ch >> 4]);
                result.push_back(digits[ch & 15]);
            } else {
                result.push_back(static_cast<char>(ch));
            }
        }
    }
    return result + '"';
}

struct Json {
    enum class Kind { OBJECT, ARRAY, STRING, NUMBER, BOOLEAN, NIL } kind = Kind::NIL;
    std::string text;
    std::map<std::string, Json> object;
    std::vector<Json> array;
};

class JsonParser {
public:
    explicit JsonParser(const std::string& input) : input(input) {}

    Json parse() {
        Json value = parseValue(0);
        space();
        if (position != input.size()) fail();
        return value;
    }

private:
    const std::string& input;
    size_t position = 0;

    [[noreturn]] void fail() const { throw std::runtime_error("invalid JSON"); }
    void space() {
        while (position < input.size() && std::isspace(static_cast<unsigned char>(input[position]))) ++position;
    }
    bool take(char expected) {
        space();
        if (position < input.size() && input[position] == expected) {
            ++position;
            return true;
        }
        return false;
    }
    std::string string() {
        if (!take('"')) fail();
        std::string result;
        while (position < input.size()) {
            const unsigned char ch = static_cast<unsigned char>(input[position++]);
            if (ch == '"') return result;
            if (ch < 0x20) fail();
            if (ch != '\\') {
                result.push_back(static_cast<char>(ch));
                continue;
            }
            if (position == input.size()) fail();
            const char escaped = input[position++];
            switch (escaped) {
            case '"': result.push_back('"'); break;
            case '\\': result.push_back('\\'); break;
            case '/': result.push_back('/'); break;
            case 'b': result.push_back('\b'); break;
            case 'f': result.push_back('\f'); break;
            case 'n': result.push_back('\n'); break;
            case 'r': result.push_back('\r'); break;
            case 't': result.push_back('\t'); break;
            case 'u': {
                if (position + 4 > input.size()) fail();
                unsigned code = 0;
                for (int i = 0; i < 4; ++i) {
                    const char digit = input[position++];
                    code <<= 4;
                    if (digit >= '0' && digit <= '9') code += digit - '0';
                    else if (digit >= 'a' && digit <= 'f') code += digit - 'a' + 10;
                    else if (digit >= 'A' && digit <= 'F') code += digit - 'A' + 10;
                    else fail();
                }
                if (code < 0x80) result.push_back(static_cast<char>(code));
                else if (code < 0x800) {
                    result.push_back(static_cast<char>(0xC0 | (code >> 6)));
                    result.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                } else {
                    result.push_back(static_cast<char>(0xE0 | (code >> 12)));
                    result.push_back(static_cast<char>(0x80 | ((code >> 6) & 0x3F)));
                    result.push_back(static_cast<char>(0x80 | (code & 0x3F)));
                }
                break;
            }
            default: fail();
            }
        }
        fail();
    }
    Json parseValue(unsigned depth) {
        if (depth > 16) fail();
        space();
        if (position == input.size()) fail();
        Json result;
        if (input[position] == '"') {
            result.kind = Json::Kind::STRING;
            result.text = string();
        } else if (input[position] == '{') {
            ++position;
            result.kind = Json::Kind::OBJECT;
            if (take('}')) return result;
            do {
                std::string key = string();
                if (!take(':') || result.object.count(key)) fail();
                result.object.emplace(std::move(key), parseValue(depth + 1));
            } while (take(','));
            if (!take('}')) fail();
        } else if (input[position] == '[') {
            ++position;
            result.kind = Json::Kind::ARRAY;
            if (take(']')) return result;
            do {
                result.array.push_back(parseValue(depth + 1));
            } while (take(','));
            if (!take(']')) fail();
        } else if (input.compare(position, 4, "null") == 0) {
            position += 4;
        } else if (input.compare(position, 4, "true") == 0) {
            position += 4;
            result.kind = Json::Kind::BOOLEAN;
            result.text = "true";
        } else if (input.compare(position, 5, "false") == 0) {
            position += 5;
            result.kind = Json::Kind::BOOLEAN;
            result.text = "false";
        } else {
            const size_t start = position;
            if (input[position] == '-') ++position;
            if (position == input.size()) fail();
            if (input[position] == '0') ++position;
            else {
                if (input[position] < '1' || input[position] > '9') fail();
                while (position < input.size() && std::isdigit(static_cast<unsigned char>(input[position]))) ++position;
            }
            if (position < input.size() && input[position] == '.') {
                ++position;
                const size_t fraction = position;
                while (position < input.size() && std::isdigit(static_cast<unsigned char>(input[position]))) ++position;
                if (fraction == position) fail();
            }
            if (position < input.size() && (input[position] == 'e' || input[position] == 'E')) {
                ++position;
                if (position < input.size() && (input[position] == '+' || input[position] == '-')) ++position;
                const size_t exponent = position;
                while (position < input.size() && std::isdigit(static_cast<unsigned char>(input[position]))) ++position;
                if (exponent == position) fail();
            }
            result.kind = Json::Kind::NUMBER;
            result.text = input.substr(start, position - start);
        }
        return result;
    }
};

std::map<std::string, std::string> configStrings(const Json& value) {
    if (value.kind != Json::Kind::OBJECT) throw std::runtime_error("config must be an object");
    std::map<std::string, std::string> config;
    for (const auto& entry : value.object) {
        if (entry.second.kind == Json::Kind::OBJECT || entry.second.kind == Json::Kind::ARRAY ||
            entry.second.kind == Json::Kind::NIL) {
            throw std::runtime_error("config values must be scalars");
        }
        config.emplace(entry.first, entry.second.text);
    }
    return config;
}

std::string configJson(const std::map<std::string, std::string>& config) {
    std::string result = "{";
    bool first = true;
    for (const auto& entry : config) {
        if (!first) result.push_back(',');
        first = false;
        result += jsonString(entry.first) + ':' + jsonString(entry.second);
    }
    return result + '}';
}

bool validName(const std::string& name) {
    if (name.empty() || name.size() > 128) return false;
    for (unsigned char ch : name) {
        if (!std::isalnum(ch) && ch != '-' && ch != '_') return false;
    }
    return true;
}

std::string decodePath(const std::string& path) {
    std::string decoded;
    for (size_t i = 0; i < path.size(); ++i) {
        if (path[i] != '%') {
            decoded.push_back(path[i]);
            continue;
        }
        if (i + 2 >= path.size()) throw std::runtime_error("invalid path encoding");
        auto nibble = [](char ch) -> int {
            if (ch >= '0' && ch <= '9') return ch - '0';
            if (ch >= 'a' && ch <= 'f') return ch - 'a' + 10;
            if (ch >= 'A' && ch <= 'F') return ch - 'A' + 10;
            return -1;
        };
        const int high = nibble(path[i + 1]);
        const int low = nibble(path[i + 2]);
        if (high < 0 || low < 0) throw std::runtime_error("invalid path encoding");
        const char decodedByte = static_cast<char>((high << 4) | low);
        if (decodedByte == '\0' || decodedByte == '/') throw std::runtime_error("invalid path");
        decoded.push_back(decodedByte);
        i += 2;
    }
    return decoded;
}

} // namespace

Connect::Response Connect::errorResponse(int status, const std::string& message) {
    return {status, "application/json", "{\"error_code\":" + std::to_string(status) +
                                        ",\"message\":" + jsonString(message) + "}"};
}

Connect::Connect() = default;

Connect::~Connect() {
    stop();
}

void Connect::start(int port) {
    if (running()) throw std::runtime_error("Kafka Connect is already running");
    listener = std::make_unique<MQTT::Listener>(port, true);
    listener->start();
    thread = std::thread([this] { serve(); });
    MQTT::logInfo("Kafka Connect registry listening on 127.0.0.1:%d", port);
}

void Connect::serve() {
    while (listener->fd() >= 0) {
        struct pollfd descriptor {listener->fd(), POLLIN, 0};
        const int ready = ::poll(&descriptor, 1, 250);
        if (ready <= 0) continue;
        if ((descriptor.revents & POLLIN) == 0) continue;
        const int socket = listener->acceptConnection();
        if (socket < 0) continue;
        timeval timeout {5, 0};
        ::setsockopt(socket, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
        ::setsockopt(socket, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
        Response response;
        try {
            std::string request;
            char buffer[4096];
            size_t headerEnd = std::string::npos;
            while (request.size() < MAX_REQUEST_BYTES) {
                const ssize_t count = ::recv(socket, buffer, sizeof(buffer), 0);
                if (count <= 0) throw std::runtime_error("incomplete HTTP request");
                request.append(buffer, static_cast<size_t>(count));
                headerEnd = request.find("\r\n\r\n");
                if (headerEnd != std::string::npos) break;
            }
            if (headerEnd == std::string::npos || headerEnd > 16384) {
                throw std::runtime_error("invalid HTTP headers");
            }
            std::istringstream headers(request.substr(0, headerEnd));
            std::string method, target, protocol;
            headers >> method >> target >> protocol;
            if (method.empty() || target.empty() || protocol != "HTTP/1.1" ||
                target.front() != '/' || target.find('#') != std::string::npos) {
                throw std::runtime_error("invalid HTTP request line");
            }
            size_t contentLength = 0;
            std::string line;
            std::getline(headers, line);
            while (std::getline(headers, line)) {
                if (!line.empty() && line.back() == '\r') line.pop_back();
                const size_t colon = line.find(':');
                if (colon == std::string::npos) throw std::runtime_error("invalid HTTP header");
                std::string name = line.substr(0, colon);
                for (char& ch : name) ch = static_cast<char>(std::tolower(static_cast<unsigned char>(ch)));
                std::string value = line.substr(colon + 1);
                const size_t start = value.find_first_not_of(" \t");
                const size_t end = value.find_last_not_of(" \t");
                value = start == std::string::npos ? "" : value.substr(start, end - start + 1);
                if (name == "content-length") {
                    if (value.empty() || value.size() > 10) throw std::runtime_error("invalid content length");
                    size_t parsed = 0;
                    for (char ch : value) {
                        if (ch < '0' || ch > '9') throw std::runtime_error("invalid content length");
                        parsed = parsed * 10 + static_cast<size_t>(ch - '0');
                    }
                    contentLength = parsed;
                } else if (name == "transfer-encoding") {
                    throw std::runtime_error("transfer encoding is unsupported");
                }
            }
            if (contentLength > MAX_REQUEST_BYTES || headerEnd + 4 + contentLength > MAX_REQUEST_BYTES) {
                response = errorResponse(413, "request too large");
            } else {
                const size_t bodyStart = headerEnd + 4;
                const size_t totalSize = bodyStart + contentLength;
                size_t received = std::min(request.size(), totalSize);
                request.resize(totalSize);
                while (received < totalSize) {
                    const ssize_t count = ::recv(socket, &request[received], totalSize - received, 0);
                    if (count <= 0) throw std::runtime_error("incomplete HTTP body");
                    received += static_cast<size_t>(count);
                }
                response = this->request(method, decodePath(target.substr(0, target.find('?'))),
                                   request.substr(bodyStart, contentLength));
            }
        } catch (const std::exception& error) {
            response = errorResponse(400, error.what());
        }
        std::string output = "HTTP/1.1 " + std::to_string(response.status) + " " +
                             (response.status == 204 ? "No Content" :
                              response.status == 201 ? "Created" : "OK") + "\r\n"
                             "Content-Type: " + response.contentType + "\r\n"
                             "Connection: close\r\n"
                             "Content-Length: " + std::to_string(response.body.size()) + "\r\n\r\n" +
                             response.body;
        size_t sent = 0;
        while (sent < output.size()) {
            const ssize_t count = ::send(socket, output.data() + sent, output.size() - sent, MSG_NOSIGNAL);
            if (count <= 0) break;
            sent += static_cast<size_t>(count);
        }
        ::close(socket);
    }
}

void Connect::stop() {
    if (listener) listener->stop();
    if (thread.joinable()) thread.join();
    listener.reset();
}

Connect::Response Connect::connectorResponse(const std::string& name, bool includeStatus) const {
    std::lock_guard<std::mutex> lock(mutex);
    auto found = connectors.find(name);
    if (found == connectors.end()) return errorResponse(404, "connector not found");
    std::string body = "{\"name\":" + jsonString(name) + ",\"config\":" +
                       configJson(found->second.config) + ",\"tasks\":[],\"type\":" + jsonString(found->second.type);
    if (includeStatus) {
        body += ",\"connector\":{\"state\":\"UNASSIGNED\",\"worker_id\":\"localhost:0\"}";
    }
    return {200, "application/json", body + '}'};
}

Connect::Response Connect::request(const std::string& method, const std::string& path,
                                   const std::string& body) {
    if (path == "/") {
        return {200, "application/json", "{\"version\":\"flowmq-basic\",\"commit\":\"unavailable\"}"};
    }
    if (path == "/connectors" && method == "GET") {
        std::lock_guard<std::mutex> lock(mutex);
        std::string result = "[";
        bool first = true;
        for (const auto& entry : connectors) {
            if (!first) result.push_back(',');
            first = false;
            result += jsonString(entry.first);
        }
        return {200, "application/json", result + ']'};
    }
    if (path == "/connectors" && method == "POST") {
        const Json root = JsonParser(body).parse();
        if (root.kind != Json::Kind::OBJECT) return errorResponse(400, "body must be an object");
        auto name = root.object.find("name");
        auto config = root.object.find("config");
        if (name == root.object.end() || name->second.kind != Json::Kind::STRING ||
            !validName(name->second.text) || config == root.object.end()) {
            return errorResponse(400, "name and config are required");
        }
        std::map<std::string, std::string> values;
        try {
            values = configStrings(config->second);
        } catch (const std::exception& error) {
            return errorResponse(400, error.what());
        }
        auto connectorClass = values.find("connector.class");
        if (connectorClass == values.end() || connectorClass->second.empty()) {
            return errorResponse(400, "connector.class is required");
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (connectors.count(name->second.text)) return errorResponse(409, "connector already exists");
            const std::string type = connectorClass->second.find("sink") != std::string::npos ? "sink" : "source";
            connectors.emplace(name->second.text, Connector{type, std::move(values)});
        }
        Response created = connectorResponse(name->second.text, false);
        created.status = 201;
        return created;
    }
    constexpr std::string_view prefix = "/connectors/";
    if (path.compare(0, prefix.size(), prefix) != 0) return errorResponse(404, "not found");
    const size_t slash = path.find('/', prefix.size());
    const std::string name = path.substr(prefix.size(), slash == std::string::npos
                                                           ? std::string::npos : slash - prefix.size());
    const std::string action = slash == std::string::npos ? "" : path.substr(slash + 1);
    if (!validName(name)) return errorResponse(404, "not found");
    if (action.empty() && method == "GET") return connectorResponse(name, true);
    if (action.empty() && method == "DELETE") {
        std::lock_guard<std::mutex> lock(mutex);
        if (!connectors.erase(name)) return errorResponse(404, "connector not found");
        return {204, "application/json", {}};
    }
    if (action == "config" && method == "GET") {
        std::lock_guard<std::mutex> lock(mutex);
        auto found = connectors.find(name);
        if (found == connectors.end()) return errorResponse(404, "connector not found");
        return {200, "application/json", configJson(found->second.config)};
    }
    if ((action == "config" || action.empty()) && (method == "PUT" || method == "POST")) {
        std::map<std::string, std::string> values;
        try {
            values = configStrings(JsonParser(body).parse());
        } catch (const std::exception& error) {
            return errorResponse(400, error.what());
        }
        {
            std::lock_guard<std::mutex> lock(mutex);
            auto found = connectors.find(name);
            if (found == connectors.end()) return errorResponse(404, "connector not found");
            auto connectorClass = values.find("connector.class");
            if (connectorClass == values.end() || connectorClass->second.empty()) {
                return errorResponse(400, "connector.class is required");
            }
            found->second.config = std::move(values);
        }
        return connectorResponse(name, false);
    }
    if (action == "status" && method == "GET") return connectorResponse(name, true);
    return errorResponse(405, "method not allowed");
}

} // namespace Kafka
