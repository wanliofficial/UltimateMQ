#include <gtest/gtest.h>
#include "Broker.h"
#include "Session.h"

class BrokerTest : public ::testing::Test
{
protected:
    MQTT::Broker *broker = new MQTT::Broker();
};

TEST_F(BrokerTest, InitializationTest)
{
    EXPECT_NO_THROW(MQTT::Broker());
}

TEST_F(BrokerTest, ConnectClientTest)
{
    MQTT::Session session(broker, "client1");
    session.connect();
    EXPECT_EQ(broker->getConnectedClients(), 1);
}

TEST_F(BrokerTest, DisconnectClientTest)
{
    MQTT::Session session(broker, "client1");
    session.connect();
    session.disconnect();
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

    MQTT::Session session(broker, "late-subscriber");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE};

    std::vector<MQTT::Message> delivered;
    session.setDeliverCallback([&delivered](const MQTT::Message &message, uint16_t, MQTT::QoS) {
        delivered.push_back(message);
    });

    session.subscribe("sensors/+", options);
    session.deliverRetained("sensors/+", options);

    ASSERT_EQ(delivered.size(), 1u);
    EXPECT_EQ(delivered[0].topic, "sensors/temp");
    EXPECT_TRUE(delivered[0].retain);
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
    MQTT::Session session(broker, "sub");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    std::vector<MQTT::Message> delivered;
    session.setDeliverCallback([&delivered](const MQTT::Message &message, uint16_t, MQTT::QoS) {
        delivered.push_back(message);
    });
    session.connect();
    session.subscribe("live/#", options);

    broker->publish(MQTT::Message("live/x", "hi", MQTT::QoS::QOS_0, true));

    ASSERT_EQ(delivered.size(), 1u);
    EXPECT_FALSE(delivered[0].retain);
}

TEST_F(BrokerTest, RetainHandlingDoNotSendRetained)
{
    broker->publish(MQTT::Message("r/t", "v", MQTT::QoS::QOS_0, true));

    MQTT::Session session(broker, "sub2");
    MQTT::SubscriptionOptions options{MQTT::QoS::QOS_0, false, false,
                                      MQTT::RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
    std::vector<MQTT::Message> delivered;
    session.setDeliverCallback([&delivered](const MQTT::Message &message, uint16_t, MQTT::QoS) {
        delivered.push_back(message);
    });

    session.subscribe("r/t", options);
    session.deliverRetained("r/t", options);

    EXPECT_TRUE(delivered.empty());
}
