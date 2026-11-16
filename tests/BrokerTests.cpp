#include <gtest/gtest.h>
#include "Broker.h"
#include "Session.h"
#include "TestClient.h"
#include <cstdio>
#include <fstream>
#include <algorithm>

class BrokerTest : public ::testing::Test
{
protected:
    MQTT::Broker instance;
    MQTT::Broker *broker = &instance;
};

TEST_F(BrokerTest, InitializationTest)
{
    EXPECT_NO_THROW(MQTT::Broker());
}

TEST_F(BrokerTest, MaxPacketSizeDefaultsToOneMebibyte)
{
    EXPECT_EQ(broker->getMaxPacketSize(), MQTT::Frame::DEFAULT_MAX_PACKET_SIZE);
}

TEST_F(BrokerTest, MaxPacketSizeIsConfigurable)
{
    broker->setMaxPacketSize(2 * 1024 * 1024);
    EXPECT_EQ(broker->getMaxPacketSize(), 2u * 1024 * 1024);
}

TEST_F(BrokerTest, ConnectClientTest)
{
    auto session = std::make_shared<MQTT::Session>(broker, "client1");
    session->connect(session);
    EXPECT_EQ(broker->getConnectedClients(), 1);
}

TEST_F(BrokerTest, DisconnectClientTest)
{
    auto session = std::make_shared<MQTT::Session>(broker, "client1");
    session->connect(session);
    auto client = MQTT::test::attach(*session);
    session->close(client);
    EXPECT_EQ(broker->getConnectedClients(), 0);
}

TEST_F(BrokerTest, PublishMessageTest)
{
    //broker.connectClient("subscriber");
    broker->subscribe("subscriber", "test/topic");
    MQTT::Message message("test/topic", "Hello, MQTT!");
    broker->publish(message);
}

TEST_F(BrokerTest, SubscribeTest)
{
    broker->subscribe("client1", "test/topic");
    EXPECT_TRUE(broker->isSubscribed("client1", "test/topic"));
}

TEST_F(BrokerTest, UnsubscribeTest)
{
    broker->subscribe("client1", "test/topic");
    broker->unsubscribe("client1", "test/topic");
    EXPECT_FALSE(broker->isSubscribed("client1", "test/topic"));
}

TEST_F(BrokerTest, RetainedMessageIsStoredAndDeliveredOnSubscribe)
{
    MQTT::Message retained("sensors/temp", "21.5", MQTT::QoS::QOS_0, true);
    broker->publish(retained);
    EXPECT_EQ(broker->getRetainedCount(), 1u);

    auto session = std::make_shared<MQTT::Session>(broker, "late-subscriber");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE};

    auto client = MQTT::test::attach(*session);

    session->subscribe("sensors/+", options);
    session->deliverRetained("sensors/+", options, /*isNewSubscription=*/true);
    session->flush();

    ASSERT_EQ(client->size(), 1u);
    EXPECT_EQ(client->delivered[0].topic, "sensors/temp");
    EXPECT_TRUE(client->delivered[0].retain);
}

TEST_F(BrokerTest, EmptyRetainedPayloadRemovesMessage)
{
    broker->publish(MQTT::Message("a/b", "value", MQTT::QoS::QOS_0, true));
    EXPECT_EQ(broker->getRetainedCount(), 1u);

    broker->publish(MQTT::Message("a/b", "", MQTT::QoS::QOS_0, true));
    EXPECT_EQ(broker->getRetainedCount(), 0u);
}

TEST_F(BrokerTest, LiveForwardedMessageHasRetainFlagCleared)
{
    auto session = std::make_shared<MQTT::Session>(broker, "sub");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    auto client = MQTT::test::attach(*session);
    session->connect(session);
    session->subscribe("live/#", options);

    // Broker::publish flushes the delivery to the bound client itself
    broker->publish(MQTT::Message("live/x", "hi", MQTT::QoS::QOS_0, true));

    ASSERT_EQ(client->size(), 1u);
    EXPECT_FALSE(client->delivered[0].retain);
}

TEST_F(BrokerTest, RetainHandlingDoNotSendRetained)
{
    broker->publish(MQTT::Message("r/t", "v", MQTT::QoS::QOS_0, true));

    auto session = std::make_shared<MQTT::Session>(broker, "sub2");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    auto client = MQTT::test::attach(*session);

    session->subscribe("r/t", options);
    session->deliverRetained("r/t", options, /*isNewSubscription=*/true);
    session->flush();

    EXPECT_EQ(client->size(), 0u);
}

TEST_F(BrokerTest, OverlappingSubscriptionsDeliverOnce)
{
    auto session = std::make_shared<MQTT::Session>(broker, "overlap");
    session->connect(session, /*persistent=*/false);
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    auto client = MQTT::test::attach(*session);
    session->subscribe("a/#", options);
    session->subscribe("a/b", options);

    broker->publish(MQTT::Message("a/b", "x"));

    // Both filters match, but the client must receive a single copy
    EXPECT_EQ(client->size(), 1u);
}

TEST_F(BrokerTest, DelayedWillIsNotPublishedUntilDue)
{
    // A Will armed with a delay must not be delivered before its deadline,
    // then must be delivered once the sweeper runs past it.
    auto sub = std::make_shared<MQTT::Session>(broker, "dw-sub");
    sub->connect(sub, /*persistent=*/true);
    sub->subscribe("dw/#", MQTT::SubscriptionOptions{MQTT::QoS::QOS_0, false, false,
                                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES});
    auto subClient = MQTT::test::attach(*sub);

    broker->armWill("dw-will", MQTT::Message("dw/goodbye", "bye"), MQTT::QoS::QOS_0, false, 1);
    broker->sweepExpiredSessions();
    EXPECT_EQ(subClient->size(), 0u) << "delayed Will fired before its interval elapsed";

    // After the deadline passes, the sweeper publishes it
    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    broker->sweepExpiredSessions();
    EXPECT_EQ(subClient->size(), 1u);
}

TEST_F(BrokerTest, DelayedWillExpiryCountsFromPublication)
{
    // MQTT-3.3.2-6 / MQTT-3.3.2-9: the Will's Message Expiry Interval must be
    // applied when it is published. Arming it must not stamp a deadline (that
    // would let the Will Delay eat into the lifetime) and the published copy
    // must carry the full interval minus the time since publication.
    auto sub = std::make_shared<MQTT::Session>(broker, "dw-exp-sub");
    sub->connect(sub, /*persistent=*/true);
    sub->subscribe("dw/exp", MQTT::SubscriptionOptions{MQTT::QoS::QOS_0, false, false,
                                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES});
    auto subClient = MQTT::test::attach(*sub);

    MQTT::Message will("dw/exp", "bye");
    broker->armWill("dw-exp-will", will, MQTT::QoS::QOS_0, false, /*delaySeconds=*/1,
                    /*hasExpiry=*/true, /*expiryInterval=*/60);
    broker->sweepExpiredSessions();
    EXPECT_EQ(subClient->size(), 0u) << "the Will fired before its delay elapsed";

    std::this_thread::sleep_for(std::chrono::milliseconds(1100));
    broker->sweepExpiredSessions();
    ASSERT_EQ(subClient->size(), 1u);
    auto delivered = subClient->snapshot();
    ASSERT_TRUE(delivered[0].hasExpiry) << "the published Will lost its expiry";
    const auto left = std::chrono::duration_cast<std::chrono::seconds>(
                          delivered[0].expiryDeadline - std::chrono::steady_clock::now())
                          .count();
    // 60 s minus ~1 s of publication time; far more than the 59 s a deadline
    // armed at armWill() time would have left (which would also be wrong once
    // the delay is long).
    EXPECT_GE(left, 58) << "the Will Delay was subtracted from the Message Expiry";
    EXPECT_LE(left, 60);
}

TEST_F(BrokerTest, WillWithZeroExpiryExpiresImmediately)
{
    // MQTT-3.3.2-9: an interval of 0 means the message expires immediately, so
    // a Will published with it must not reach even a live subscriber.
    auto sub = std::make_shared<MQTT::Session>(broker, "wz-sub");
    sub->connect(sub, /*persistent=*/false);
    sub->subscribe("wz/#", MQTT::SubscriptionOptions{MQTT::QoS::QOS_0, false, false,
                                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES});
    auto subClient = MQTT::test::attach(*sub);

    MQTT::Message will("wz/gone", "bye");
    will.hasExpiry = true;
    will.expiryDeadline = std::chrono::steady_clock::now();
    broker->publish(will);
    sub->flush();
    EXPECT_EQ(subClient->size(), 0u) << "an already expired Will was delivered";
}

TEST_F(BrokerTest, CancelWillPreventsPublication){
    auto sub = std::make_shared<MQTT::Session>(broker, "cw-sub");
    sub->connect(sub, /*persistent=*/true);
    sub->subscribe("cw/#", MQTT::SubscriptionOptions{MQTT::QoS::QOS_0, false, false,
                                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES});
    auto subClient = MQTT::test::attach(*sub);

    broker->armWill("cw-will", MQTT::Message("cw/goodbye", "bye"), MQTT::QoS::QOS_0, false, 5);
    broker->cancelWill("cw-will");
    broker->sweepExpiredSessions();
    EXPECT_EQ(subClient->size(), 0u) << "a cancelled Will must never be published";
}

namespace {
//! A connected session holding a shared subscription, with a recording client
struct SharedMember {
    std::shared_ptr<MQTT::Session> session;
    std::shared_ptr<MQTT::RecordingClient> client;
};

SharedMember makeSharedMember(MQTT::Broker *broker, const std::string &clientId,
                              const std::string &topicFilter)
{
    SharedMember member;
    member.session = std::make_shared<MQTT::Session>(broker, clientId);
    member.session->connect(member.session, /*persistent=*/false);
    member.client = MQTT::test::attach(*member.session);
    member.session->subscribe(topicFilter,
                              MQTT::SubscriptionOptions{MQTT::QoS::QOS_0, false, false,
                                                        MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES});
    return member;
}
} // namespace

TEST_F(BrokerTest, DistinctSharedGroupsEachReceiveOneCopy)
{
    // $share/g1/x and $share/g2/x are different groups: a publish must reach
    // one member of EACH group. Sharing one pool across groups would deliver a
    // single copy in total and silently drop the other group.
    auto g1 = makeSharedMember(broker, "g1-a", "$share/g1/x");
    auto g2 = makeSharedMember(broker, "g2-a", "$share/g2/x");

    broker->publish(MQTT::Message("x", "hello"));

    EXPECT_EQ(g1.client->size(), 1u) << "group g1 received no copy";
    EXPECT_EQ(g2.client->size(), 1u) << "group g2 received no copy";
}

TEST_F(BrokerTest, SharedGroupReceivesExactlyOneCopyPerGroup)
{
    // Two members of the SAME group share one message between them.
    auto a = makeSharedMember(broker, "same-a", "$share/g/x");
    auto b = makeSharedMember(broker, "same-b", "$share/g/x");

    broker->publish(MQTT::Message("x", "hello"));

    EXPECT_EQ(a.client->size() + b.client->size(), 1u)
        << "a shared group must deliver a single copy";
}

TEST_F(BrokerTest, SharedGroupRotatesBetweenMembers)
{
    // Distribution inside a group is round-robin, so every member is used in
    // turn instead of one member (or random selection) starving the others.
    auto a = makeSharedMember(broker, "rr-a", "$share/rr/x");
    auto b = makeSharedMember(broker, "rr-b", "$share/rr/x");

    for (int i = 0; i < 6; ++i) {
        broker->publish(MQTT::Message("x", "m" + std::to_string(i)));
    }

    EXPECT_EQ(a.client->size(), 3u) << "round-robin should split 6 messages evenly";
    EXPECT_EQ(b.client->size(), 3u) << "round-robin should split 6 messages evenly";
}

TEST_F(BrokerTest, UnsubscribingOneSharedMemberKeepsTheGroup)
{
    // Removing a member must not break delivery to the rest of its group.
    auto a = makeSharedMember(broker, "keep-a", "$share/k/x");
    auto b = makeSharedMember(broker, "keep-b", "$share/k/x");

    b.session->unsubscribe("$share/k/x");

    broker->publish(MQTT::Message("x", "1"));
    broker->publish(MQTT::Message("x", "2"));

    EXPECT_EQ(b.client->size(), 0u) << "the unsubscribed member must get nothing";
    EXPECT_EQ(a.client->size(), 2u) << "the remaining member must get every message";
}

TEST_F(BrokerTest, SharedSubscriptionMatchesWildcardFilter)
{
    // The group's filter keeps its wildcards: $share/g/sensors/+ matches one level.
    auto member = makeSharedMember(broker, "wc-a", "$share/g/sensors/+");

    broker->publish(MQTT::Message("sensors/a", "yes"));
    broker->publish(MQTT::Message("sensors/a/b", "no"));

    EXPECT_EQ(member.client->size(), 1u)
        << "only the single-level match should be delivered";
}

TEST_F(BrokerTest, RemovingASharedGroupKeepsAPlainSubscriptionOnTheSameFilter)
{
    // A plain subscription on "shared/x" and a shared group on the same filter
    // both live in the routing table. Dropping the group must not take the
    // plain subscriber's routing entry with it.
    auto plain = std::make_shared<MQTT::Session>(broker, "plain-sub");
    plain->connect(plain, /*persistent=*/false);
    auto plainClient = MQTT::test::attach(*plain);
    plain->subscribe("shared/x", MQTT::SubscriptionOptions{MQTT::QoS::QOS_0, false, false,
                                                           MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES});

    auto shared = makeSharedMember(broker, "grp-sub", "$share/g/shared/x");
    shared.session->unsubscribe("$share/g/shared/x");

    broker->publish(MQTT::Message("shared/x", "hi"));

    EXPECT_EQ(plainClient->size(), 1u) << "the plain subscriber lost its routing entry";
    EXPECT_EQ(shared.client->size(), 0u);
}

TEST_F(BrokerTest, SharedRoundRobinSkipsDeletedMembers)
{
    // A member whose session has been torn down must not consume a turn.
    auto a = makeSharedMember(broker, "skip-a", "$share/s/x");
    auto b = makeSharedMember(broker, "skip-b", "$share/s/x");

    // Drop b's session from the broker: it is no longer a delivery target
    broker->removeSession("skip-b");

    broker->publish(MQTT::Message("x", "1"));
    broker->publish(MQTT::Message("x", "2"));

    EXPECT_EQ(a.client->size(), 2u) << "every message must still reach the live member";
    EXPECT_EQ(b.client->size(), 0u) << "a removed session must not receive anything";
}

namespace {
//! A broker with an ACL loaded, plus the Auth object that owns it
struct AclBroker {
    MQTT::Auth auth;
    MQTT::Broker broker;

    explicit AclBroker(const std::string &acl)
    {
        const std::string path = "/tmp/flowmq_broker_acl_" + std::to_string(::getpid());
        std::ofstream out(path, std::ios::binary);
        out << acl;
        out.close();
        std::string error;
        EXPECT_TRUE(auth.loadAcl(path, &error)) << error;
        std::remove(path.c_str());
        broker.setAuth(&auth);
    }
};

//! A session that authenticated as \p username, with a recording client
std::shared_ptr<MQTT::RecordingClient> aclSubscriber(MQTT::Broker &broker,
                                                     const std::string &clientId,
                                                     const std::string &username,
                                                     const std::string &filter,
                                                     MQTT::SubscriptionOptions options)
{
    auto session = std::make_shared<MQTT::Session>(&broker, clientId);
    session->connect(session, /*persistent=*/false);
    session->setUsername(username);
    auto client = MQTT::test::attach(*session);
    session->subscribe(filter, options);
    return client;
}

MQTT::SubscriptionOptions opts(MQTT::QoS qos, bool noLocal = false,
                               bool retainAsPublished = false)
{
    return MQTT::SubscriptionOptions{qos, noLocal, retainAsPublished,
                                     MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
}
} // namespace

TEST(BrokerAclTest, AWildcardSubscriptionSeesEveryTopicTheAclGrants)
{
    // The ACL grants "sensors/#". A subscriber to "#" must receive a matching
    // message: the read check is about the concrete topic, never the filter the
    // client happened to use.
    AclBroker env("alice,read,sensors/#\n");
    auto client = aclSubscriber(env.broker, "wild", "alice", "#", opts(MQTT::QoS::QOS_0));

    env.broker.publish(MQTT::Message("sensors/temp", "21"));

    EXPECT_EQ(client->size(), 1u)
        << "a wildcard subscription was denied a topic its ACL grants";
}

TEST(BrokerAclTest, ASharedGroupSeesEveryTopicTheAclGrants)
{
    // Same rule through the shared-subscription path.
    AclBroker env("alice,read,sensors/#\n");
    auto client = aclSubscriber(env.broker, "shared-wild", "alice", "$share/g/#",
                                opts(MQTT::QoS::QOS_0));

    env.broker.publish(MQTT::Message("sensors/temp", "21"));

    EXPECT_EQ(client->size(), 1u)
        << "a shared subscription was denied a topic its ACL grants";
}

TEST(BrokerAclTest, ADeniedTopicIsStillWithheldFromAWildcardSubscription)
{
    // The mirror case: a topic the ACL does not grant must not be delivered.
    AclBroker env("alice,read,sensors/#\n");
    auto client = aclSubscriber(env.broker, "wild-deny", "alice", "#", opts(MQTT::QoS::QOS_0));

    env.broker.publish(MQTT::Message("private/secret", "x"));

    EXPECT_EQ(client->size(), 0u) << "a topic outside the ACL was delivered";
}

TEST(BrokerSharedOptionsTest, SharedSubscriptionAppliesMaximumQos)
{
    // A shared subscription granted QoS 1 must downgrade a QoS 2 publish.
    auto broker = std::make_shared<MQTT::Broker>();
    auto member = makeSharedMember(broker.get(), "qos-member", "$share/g/qos/x");
    // Re-subscribe with the QoS we want to exercise
    member.session->subscribe("$share/g/qos/x", opts(MQTT::QoS::QOS_1));

    broker->publish(MQTT::Message("qos/x", "hi", MQTT::QoS::QOS_2));

    ASSERT_EQ(member.client->size(), 1u);
    EXPECT_EQ(member.client->qosValues[0], MQTT::QoS::QOS_1)
        << "the granted Maximum QoS must cap the delivered QoS";
}

TEST(BrokerSharedOptionsTest, SharedSubscriptionHonoursNoLocal)
{
    // A member that publishes to its own shared subscription must not receive
    // the message back (the session is the originator).
    auto broker = std::make_shared<MQTT::Broker>();
    auto member = makeSharedMember(broker.get(), "nolocal", "$share/g/nl/x");
    member.session->subscribe("$share/g/nl/x", opts(MQTT::QoS::QOS_0, /*noLocal=*/true));

    MQTT::Message message("nl/x", "self");
    message.originSession = member.session.get();
    broker->publish(message);

    EXPECT_EQ(member.client->size(), 0u)
        << "No Local must suppress the client's own message";
}

TEST(BrokerSharedOptionsTest, SharedSubscriptionClearsTheRetainFlagByDefault)
{
    // Forwarding a live retained publish keeps RETAIN clear unless the
    // subscription asked for Retain As Published.
    auto broker = std::make_shared<MQTT::Broker>();
    auto member = makeSharedMember(broker.get(), "rap", "$share/g/rap/x");
    member.session->subscribe("$share/g/rap/x", opts(MQTT::QoS::QOS_0));

    broker->publish(MQTT::Message("rap/x", "hi", MQTT::QoS::QOS_0, /*retain=*/true));

    ASSERT_EQ(member.client->size(), 1u);
    EXPECT_FALSE(member.client->delivered[0].retain)
        << "a live forward must clear RETAIN unless Retain As Published is set";
}

TEST(BrokerSharedOptionsTest, SharedSubscriptionHonoursRetainAsPublished)
{
    // With Retain As Published the flag travels with the message.
    auto broker = std::make_shared<MQTT::Broker>();
    auto member = makeSharedMember(broker.get(), "rap-on", "$share/g/rap2/x");
    member.session->subscribe("$share/g/rap2/x", opts(MQTT::QoS::QOS_0, false, /*rap=*/true));

    broker->publish(MQTT::Message("rap2/x", "hi", MQTT::QoS::QOS_0, /*retain=*/true));

    ASSERT_EQ(member.client->size(), 1u);
    EXPECT_TRUE(member.client->delivered[0].retain)
        << "Retain As Published must forward the RETAIN flag";
}

TEST_F(BrokerTest, ADollarTopicSubscriptionIsNotMistakenForASharedOne)
{
    // "$SYS/#" begins with '$' but is an ordinary subscription. A session
    // holding it plus an overlapping filter must still receive a single copy,
    // exactly like any other pair of overlapping ordinary subscriptions.
    auto session = std::make_shared<MQTT::Session>(broker, "sys-sub");
    session->connect(session, /*persistent=*/false);
    auto client = MQTT::test::attach(*session);
    session->subscribe("$SYS/#", opts(MQTT::QoS::QOS_0));
    session->subscribe("$SYS/broker/uptime", opts(MQTT::QoS::QOS_0));

    broker->publish(MQTT::Message("$SYS/broker/uptime", "42"));

    EXPECT_EQ(client->size(), 1u) << "overlapping ordinary subscriptions must deliver once";
}

TEST_F(BrokerTest, AnOrdinaryAndASharedSubscriptionOnTheSameTopicEachDeliver)
{
    // The two are independent subscriptions, so the client gets two copies.
    auto session = std::make_shared<MQTT::Session>(broker, "both-sub");
    session->connect(session, /*persistent=*/false);
    auto client = MQTT::test::attach(*session);
    session->subscribe("both/x", opts(MQTT::QoS::QOS_0));
    session->subscribe("$share/g/both/x", opts(MQTT::QoS::QOS_0));

    broker->publish(MQTT::Message("both/x", "hi"));

    EXPECT_EQ(client->size(), 2u)
        << "an ordinary and a shared subscription must each deliver a copy";
}

TEST_F(BrokerTest, LiveDeliveryCarriesTheSubscriptionIdentifier)
{
    auto session = std::make_shared<MQTT::Session>(broker, "sid-sub");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    options.subscriptionIdentifier = 42;
    auto client = MQTT::test::attach(*session);
    session->connect(session);
    session->subscribe("sid/#", options);

    broker->publish(MQTT::Message("sid/x", "hi"));

    ASSERT_EQ(client->size(), 1u);
    ASSERT_EQ(client->identifiersPerDelivery.size(), 1u);
    ASSERT_EQ(client->identifiersPerDelivery[0].size(), 1u);
    EXPECT_EQ(client->identifiersPerDelivery[0][0], 42u);
    // The identifier belongs to the subscription, not the message
    EXPECT_TRUE(client->delivered[0].properties.empty());
}

TEST_F(BrokerTest, LiveDeliveryForwardsApplicationProperties)
{
    // MQTT-3.3.2-4: Content Type and friends belong to the application message
    // and must reach every subscriber.
    auto session = std::make_shared<MQTT::Session>(broker, "prop-sub");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    auto client = MQTT::test::attach(*session);
    session->connect(session);
    session->subscribe("prop/#", options);

    MQTT::Message message("prop/x", "hi");
    message.properties[MQTT::PropertyID::CONTENT_TYPE] = std::string("text/plain");
    message.properties[MQTT::PropertyID::USER_PROPERTY] =
        MQTT::UserProperties{{"trace", "abc"}};
    broker->publish(message);

    ASSERT_EQ(client->size(), 1u);
    const auto &delivered = client->delivered[0];
    ASSERT_EQ(delivered.properties.size(), 2u);
    EXPECT_EQ(std::get<std::string>(delivered.properties.at(MQTT::PropertyID::CONTENT_TYPE)),
              "text/plain");
    const auto &pairs =
        std::get<MQTT::UserProperties>(delivered.properties.at(MQTT::PropertyID::USER_PROPERTY));
    EXPECT_EQ(pairs.at("trace"), "abc");
}

TEST_F(BrokerTest, ASubscriptionWithoutAnIdentifierDeliversNone)
{
    auto session = std::make_shared<MQTT::Session>(broker, "plain-sub");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    auto client = MQTT::test::attach(*session);
    session->connect(session);
    session->subscribe("plain/#", options);

    broker->publish(MQTT::Message("plain/x", "hi"));

    ASSERT_EQ(client->size(), 1u);
    ASSERT_EQ(client->identifiersPerDelivery.size(), 1u);
    EXPECT_TRUE(client->identifiersPerDelivery[0].empty());
}

TEST_F(BrokerTest, OverlappingSubscriptionsDeliverAtTheHighestMaximumQos)
{
    // MQTT-3.3.4-2: when several subscriptions match, the copy that is
    // delivered must respect the maximum QoS of all of them. "a/+" is reached
    // before "a/#" by the routing trie, so an implementation that applied only
    // the first match would deliver at QoS 0 here.
    auto session = std::make_shared<MQTT::Session>(broker, "overlap-qos");
    session->connect(session, /*persistent=*/false);
    auto client = MQTT::test::attach(*session);
    session->subscribe("a/#", opts(MQTT::QoS::QOS_2));
    session->subscribe("a/+", opts(MQTT::QoS::QOS_0));

    broker->publish(MQTT::Message("a/b", "hi", MQTT::QoS::QOS_2));

    ASSERT_EQ(client->size(), 1u) << "overlapping subscriptions must produce one copy";
    EXPECT_EQ(client->qosValues[0], MQTT::QoS::QOS_2)
        << "the delivered QoS must be the highest Maximum QoS of the matching subscriptions";
}

TEST_F(BrokerTest, OverlappingSubscriptionsCarryEverySubscriptionIdentifier)
{
    // MQTT-3.3.4-3/-4: a single copy must list the identifier of every matching
    // subscription, keeping repeats (a client may reuse one identifier).
    auto session = std::make_shared<MQTT::Session>(broker, "overlap-sid");
    session->connect(session, /*persistent=*/false);
    auto client = MQTT::test::attach(*session);
    auto withId = [](MQTT::QoS qos, uint32_t id) {
        MQTT::SubscriptionOptions options{qos, false, false,
                                          MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
        options.subscriptionIdentifier = id;
        return options;
    };
    session->subscribe("a/#", withId(MQTT::QoS::QOS_0, 7));
    session->subscribe("a/+", withId(MQTT::QoS::QOS_0, 7));
    session->subscribe("a/b", withId(MQTT::QoS::QOS_0, 9));

    broker->publish(MQTT::Message("a/b", "hi"));

    ASSERT_EQ(client->size(), 1u);
    ASSERT_EQ(client->identifiersPerDelivery.size(), 1u);
    // MQTT-3.3.4-4: "their order is not significant".
    auto identifiers = client->identifiersPerDelivery[0];
    std::sort(identifiers.begin(), identifiers.end());
    EXPECT_EQ(identifiers, (MQTT::SubscriptionIdentifiers{7, 7, 9}))
        << "the single copy must carry one identifier per matching subscription";
}

TEST_F(BrokerTest, NoLocalOnOneOverlappingSubscriptionStillDelivers)
{
    // MQTT-3.8.3-3: No Local removes only the subscriptions that asked for it.
    // The message must still arrive through the other matching subscription.
    auto session = std::make_shared<MQTT::Session>(broker, "overlap-nl");
    session->connect(session, /*persistent=*/false);
    auto client = MQTT::test::attach(*session);
    session->subscribe("a/+", opts(MQTT::QoS::QOS_0, /*noLocal=*/true));
    session->subscribe("a/#", opts(MQTT::QoS::QOS_0, /*noLocal=*/false));

    MQTT::Message message("a/b", "self");
    message.originSession = session.get();
    broker->publish(message);

    EXPECT_EQ(client->size(), 1u)
        << "one overlapping subscription without No Local still wants the message";
}

TEST_F(BrokerTest, NoLocalOnEveryOverlappingSubscriptionSuppressesTheCopy)
{
    auto session = std::make_shared<MQTT::Session>(broker, "overlap-nl2");
    session->connect(session, /*persistent=*/false);
    auto client = MQTT::test::attach(*session);
    session->subscribe("a/+", opts(MQTT::QoS::QOS_0, /*noLocal=*/true));
    session->subscribe("a/#", opts(MQTT::QoS::QOS_0, /*noLocal=*/true));

    MQTT::Message message("a/b", "self");
    message.originSession = session.get();
    broker->publish(message);

    EXPECT_EQ(client->size(), 0u)
        << "every matching subscription asked for No Local, so nothing may be delivered";
}

TEST_F(BrokerTest, RetainAsPublishedOnOneOverlappingSubscriptionKeepsRetain)
{
    // MQTT-3.3.1-8: the flag travels with the copy if any matching subscription
    // asked for it. "a/+" is matched first, so looking only at it would clear
    // the flag while the overlapping "a/#" wants it kept.
    auto session = std::make_shared<MQTT::Session>(broker, "overlap-rap");
    session->connect(session, /*persistent=*/false);
    auto client = MQTT::test::attach(*session);
    session->subscribe("a/+", opts(MQTT::QoS::QOS_0, false, /*rap=*/false));
    session->subscribe("a/#", opts(MQTT::QoS::QOS_0, false, /*rap=*/true));

    broker->publish(MQTT::Message("a/b", "hi", MQTT::QoS::QOS_0, /*retain=*/true));

    ASSERT_EQ(client->size(), 1u);
    EXPECT_TRUE(client->delivered[0].retain)
        << "Retain As Published must be honoured when any matching subscription asks for it";
}
