#include <gtest/gtest.h>
#include "../src/Auth.h"
#include "../src/Broker.h"
#include "../src/Session.h"
#include "../src/Message.h"
#include "TestClient.h"
#include <cstdio>
#include <fstream>
#include <string>

namespace MQTT {

namespace {
//! Write `content` to a temporary file and return its path
std::string writeTemp(const std::string &name, const std::string &content) {
    std::string path = "/tmp/flowmq_auth_" + std::to_string(::getpid()) + "_" + name;
    std::ofstream out(path, std::ios::binary);
    out << content;
    out.close();
    return path;
}

SubscriptionOptions optionsFor(QoS qos = QoS::QOS_0) {
    return SubscriptionOptions{qos, false, false,
                               RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
}
} // namespace

TEST(AuthTest, Sha256MatchesKnownVectors)
{
    // Standard test vectors
    EXPECT_EQ(Auth::hashPassword("", ""),
              std::string("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"));
    EXPECT_EQ(Auth::hashPassword("", "abc"),
              std::string("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"));
    EXPECT_EQ(Auth::hashPassword("salt", "password"),
              Auth::hashPassword("salt", "password"));
    EXPECT_NE(Auth::hashPassword("salt1", "password"),
              Auth::hashPassword("salt2", "password"));
}

TEST(AuthTest, AuthDisabledAllowsEveryone)
{
    Auth auth;
    EXPECT_FALSE(auth.authRequired());
    EXPECT_TRUE(auth.authenticate("anyone", ""));
    EXPECT_TRUE(auth.authenticate("", ""));
    EXPECT_FALSE(auth.aclEnabled());
    EXPECT_FALSE(auth.isForbidden("", "a/b", true));
    EXPECT_FALSE(auth.isForbidden("", "a/b", false));
}

TEST(AuthTest, PlaintextUsers)
{
    auto path = writeTemp("users_plain", "alice:secret\n# a comment\nbob:pw2\n");
    Auth auth;
    std::string error;
    ASSERT_TRUE(auth.loadUsers(path, &error)) << error;
    ::remove(path.c_str());

    EXPECT_TRUE(auth.authRequired());
    EXPECT_TRUE(auth.authenticate("alice", "secret"));
    EXPECT_TRUE(auth.authenticate("bob", "pw2"));
    EXPECT_FALSE(auth.authenticate("alice", "wrong"));
    EXPECT_FALSE(auth.authenticate("alice", ""));
    EXPECT_FALSE(auth.authenticate("mallory", "secret"));
    // No ACL file: authenticated users may do anything
    EXPECT_FALSE(auth.isForbidden("alice", "a/b", true));
}

TEST(AuthTest, HashedUsers)
{
    std::string salt = "deadbeef";
    std::string hash = Auth::hashPassword(salt, "s3cret");
    auto path = writeTemp("users_hash", "carol:$sha256$" + salt + ":" + hash + "\n");
    Auth auth;
    std::string error;
    ASSERT_TRUE(auth.loadUsers(path, &error)) << error;
    ::remove(path.c_str());

    EXPECT_TRUE(auth.authenticate("carol", "s3cret"));
    EXPECT_FALSE(auth.authenticate("carol", "s3cret2"));
}

TEST(AuthTest, MalformedUserFileIsRejected)
{
    auto path = writeTemp("users_bad", "no-colon-here\n");
    Auth auth;
    std::string error;
    EXPECT_FALSE(auth.loadUsers(path, &error));
    EXPECT_FALSE(error.empty());
    ::remove(path.c_str());
}

TEST(AuthTest, MissingFileIsReported)
{
    Auth auth;
    std::string error;
    EXPECT_FALSE(auth.loadUsers("/nonexistent/users.txt", &error));
    EXPECT_FALSE(error.empty());
    EXPECT_FALSE(auth.loadAcl("/nonexistent/acl.txt", &error));
    EXPECT_FALSE(error.empty());
}

TEST(AuthTest, AclAllowsAndDeniesPerDirection)
{
    auto users = writeTemp("users_acl", "alice:pw\nbob:pw\n");
    auto acl = writeTemp("acl_rules",
                         "alice,read,sensors/#\n"
                         "alice,write,commands/alice\n"
                         "bob,read,#\n");
    Auth auth;
    ASSERT_TRUE(auth.loadUsers(users));
    ASSERT_TRUE(auth.loadAcl(acl));
    ::remove(users.c_str());
    ::remove(acl.c_str());

    EXPECT_TRUE(auth.aclEnabled());

    // alice may read anything under sensors/ and write only to her command topic
    EXPECT_FALSE(auth.isForbidden("alice", "sensors/temp", false));
    EXPECT_FALSE(auth.isForbidden("alice", "sensors/a/b", false));
    EXPECT_TRUE(auth.isForbidden("alice", "other/topic", false));
    EXPECT_FALSE(auth.isForbidden("alice", "commands/alice", true));
    EXPECT_TRUE(auth.isForbidden("alice", "commands/bob", true));
    // read and write are separate rights
    EXPECT_TRUE(auth.isForbidden("alice", "sensors/temp", true));
    EXPECT_TRUE(auth.isForbidden("alice", "commands/alice", false));

    // bob may read everything but write nothing
    EXPECT_FALSE(auth.isForbidden("bob", "anything/at/all", false));
    EXPECT_TRUE(auth.isForbidden("bob", "anything/at/all", true));
}

TEST(AuthTest, AclWildcardUserAppliesToEveryone)
{
    auto users = writeTemp("users_wild", "alice:pw\n");
    auto acl = writeTemp("acl_wild", "*,read,public/#\n");
    Auth auth;
    ASSERT_TRUE(auth.loadUsers(users));
    ASSERT_TRUE(auth.loadAcl(acl));
    ::remove(users.c_str());
    ::remove(acl.c_str());

    EXPECT_FALSE(auth.isForbidden("alice", "public/news", false));
    EXPECT_FALSE(auth.isForbidden("nobody", "public/news", false));
    EXPECT_TRUE(auth.isForbidden("alice", "private/secret", false));
}

//! End-to-end: read ACL is enforced when a session hands a message to its client
TEST(AuthTest, SessionDeliveryHonoursReadAcl)
{
    auto users = writeTemp("users_deliver", "alice:pw\n");
    auto acl = writeTemp("acl_deliver", "alice,read,allowed/#\n");
    Auth auth;
    ASSERT_TRUE(auth.loadUsers(users));
    ASSERT_TRUE(auth.loadAcl(acl));
    ::remove(users.c_str());
    ::remove(acl.c_str());

    Broker broker(&auth);
    auto session = std::make_shared<Session>(&broker, "acl-sub");
    session->setUsername("alice");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);
    session->subscribe("allowed/t", optionsFor());
    session->subscribe("denied/t", optionsFor());

    broker.publish(Message("allowed/t", "seen"));
    broker.publish(Message("denied/t", "hidden"));

    ASSERT_EQ(client->size(), 1u);
    EXPECT_EQ(std::string(client->delivered[0].payload.begin(), client->delivered[0].payload.end()),
              "seen");
}

TEST(AuthTest, SessionWithoutAclAllowsEverything)
{
    Broker broker;  // no Auth attached
    auto session = std::make_shared<Session>(&broker, "open-sub");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);
    session->subscribe("any/t", optionsFor());

    broker.publish(Message("any/t", "x"));

    EXPECT_EQ(client->size(), 1u);
}

//! End-to-end: a retained message on a topic the user may not read is hidden
TEST(AuthTest, RetainedMessagesAreFilteredByReadAcl)
{
    auto users = writeTemp("users_ret", "alice:pw\n");
    auto acl = writeTemp("acl_ret",
                         "alice,read,public/#\n"
                         "alice,read,private/ok\n");
    Auth auth;
    ASSERT_TRUE(auth.loadUsers(users));
    ASSERT_TRUE(auth.loadAcl(acl));
    ::remove(users.c_str());
    ::remove(acl.c_str());

    Broker broker(&auth);
    // Stored by an "author" that is allowed to publish everywhere (no auth on
    // the broker side when publishing directly, as in a system publisher)
    broker.publish(Message("public/news", "visible", QoS::QOS_0, true));
    broker.publish(Message("private/ok", "visible", QoS::QOS_0, true));
    broker.publish(Message("private/secret", "hidden", QoS::QOS_0, true));

    auto session = std::make_shared<Session>(&broker, "ret-sub");
    session->setUsername("alice");
    session->connect(session, /*persistent=*/false);
    auto client = test::attach(*session);
    auto options = SubscriptionOptions{QoS::QOS_0, false, false,
                                       RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE};

    // Wildcard filter covering both public and private topics
    session->subscribe("#", options);
    session->deliverRetained("#", options);
    session->flush();

    ASSERT_EQ(client->size(), 2u);
    for (const auto &message : client->delivered) {
        EXPECT_NE(message.topic, "private/secret");
    }
}

} // namespace MQTT
