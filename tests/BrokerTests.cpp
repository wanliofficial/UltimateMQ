#include <gtest/gtest.h>
#include "Broker.h"
#include "Session.h"
#include "TestClient.h"

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
    session->deliverRetained("sensors/+", options);
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
    session->deliverRetained("r/t", options);
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

TEST_F(BrokerTest, CancelWillPreventsPublication)
{
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
