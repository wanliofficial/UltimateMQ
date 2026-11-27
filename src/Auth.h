#ifndef AUTH_H
#define AUTH_H
#pragma once

#include <map>
#include <string>
#include <cstdint>
#include <vector>
#include <utility>
#include <optional>
#include "MQTT.h"

namespace MQTT {

/*!
 * Authentication and access control for the broker.
 *
 * Loaded once at startup from optional files (users and ACL rules), then
 * treated as immutable: authenticate()/canPublish()/canRead() are pure
 * const functions over the loaded data, so they are safe to call concurrently
 * without any locking. Parsing/loading is the only phase that touches the
 * internal maps, and it happens before the broker starts serving clients.
 *
 * File formats (both optional; both UTF-8, "#" starts a comment):
 *
 *   Users file (FLOWMQ_PASSWORD_FILE):
 *       user:password
 *       user2:$sha256$<salt-text>:<hash-hex>          -- legacy: single SHA-256
 *       user3:$pbkdf2-sha256$<iterations>$<salt-hex>$<hash-hex>  -- preferred
 *   A pre-hashed password is never compared in plaintext; instead the given
 *   password is derived with the same salt (and iteration count) and the
 *   digests compared. For $pbkdf2-sha256$, the <salt-hex> column is passed to
 *   pbkdf2Password() as the salt string exactly as written (its hex text), so
 *   tooling that hashes from the file reproduces the stored digest. PBKDF2
 *   stretches the derivation, so a leaked password file resists offline
 *   cracking far better than the single SHA-256 scheme, which is retained only
 *   so existing files keep working.
 *
 *   ACL file (FLOWMQ_ACL_FILE):
 *       user,read,<topic-filter>                 -- may appear multiple times
 *       user,write,<topic-filter>
 *   An empty ACL file (or none at all) grants every authenticated user access
 *   to every topic. A rule with the special user "*" applies to everyone.
 *
 * If no user file is configured, authentication is disabled and every client
 * is treated as an anonymous super-user (matches the historical open-broker
 * behaviour, so this stays backward compatible unless explicitly configured).
 */
class Auth {
public:
    enum class Result {
        Allowed,
        BadCredentials,
        NotAuthorized
    };

    //! Parse the users file (may be empty/absent to disable authentication)
    bool loadUsers(const std::string& path, std::string* error = nullptr);
    //! Parse the ACL file (may be empty/absent to allow everything)
    bool loadAcl(const std::string& path, std::string* error = nullptr);

    bool authRequired() const { return authRequired_; }
    bool aclEnabled() const { return aclEnabled_; }

    //! Verify a username/password pair. Always returns true when no users were
    //! configured (auth disabled).
    bool authenticate(const std::string& username, const std::string& password) const;

    //! True when ACL enforcement is on and the user has no rule granting the
    //! requested access. When ACL enforcement is off, this is always false.
    bool isForbidden(const std::string& username, const std::string& topic, bool write) const;

    //! True when a SUBSCRIBE for \p filter must be refused. A filter is granted
    //! when it can match at least one topic a read rule grants; anything it
    //! matches beyond that is still withheld per message by isForbidden(). A
    //! shared subscription is judged by its real filter, since the
    //! "$share/group/" prefix is routing metadata and not part of the topic.
    bool isForbiddenSubscribe(const std::string& username, const std::string& filter) const;

    enum class KafkaOperation {
        GROUP_READ, GROUP_WRITE, GROUP_DESCRIBE, GROUP_DELETE,
        TRANSACTIONAL_ID_WRITE, CLUSTER_ADMIN
    };
    bool isKafkaForbidden(const std::string& username, const std::string& resource,
                          KafkaOperation operation) const;

    //! Resolve the reason code to send back for a failed authenticate() call
    static ReasonCode badCredentialsReason() { return ReasonCode::BAD_USERNAME_OR_PASSWORD; }
    //! Resolve the reason code to send back for a denied ACL check
    static ReasonCode notAuthorizedReason() { return ReasonCode::NOT_AUTHORIZED; }

    //! Hash a password with the given salt (hex) using SHA-256. Exposed so
    //! tests and tooling can pre-compute the values expected by loadUsers().
    //! This is the legacy scheme: it is a single unsalted-costly digest, so
    //! prefer pbkdf2Password() for new credentials.
    static std::string hashPassword(const std::string& salt, const std::string& password);

    //! Derive a password with PBKDF2-HMAC-SHA256, returning the digest as hex.
    //! This is the recommended scheme for a pre-hashed password file: the
    //! iteration count makes an offline dictionary attack cost real work per
    //! guess, which a single salted SHA-256 does not. Exposed so tests and
    //! tooling can pre-compute the values expected by loadUsers().
    static std::string pbkdf2Password(const std::string& salt, const std::string& password,
                                      uint32_t iterations);

private:
    struct User {
        enum class Kind { PLAINTEXT, SHA256, PBKDF2 };
        Kind kind = Kind::PLAINTEXT;
        std::string salt;   // raw bytes; empty for a plaintext password
        std::string hash;   // digest bytes, or the raw password when PLAINTEXT
        uint32_t iterations = 0;  // PBKDF2 only
    };

    bool authRequired_ = false;
    bool aclEnabled_ = false;
    std::map<std::string, User> users_;
    // (user or "*") -> list of (topicFilter, write) grants
    std::map<std::string, std::vector<std::pair<std::string, bool>>> acl_;
    std::map<std::string, std::vector<std::pair<std::string, KafkaOperation>>> kafkaAcl_;
    bool kafkaAclEnabled_ = false;

    bool checkAcl(const std::string& username, const std::string& topic, bool write) const;
    static std::string sha256(const std::string& data);
    static std::string toHex(const uint8_t* data, size_t len);
    static std::vector<uint8_t> fromHex(const std::string& hex);
};

} // namespace MQTT

#endif
