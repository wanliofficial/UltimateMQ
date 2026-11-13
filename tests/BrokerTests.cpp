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
    session->disconnect();
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
