#include <gtest/gtest.h>
#include "../src/Store.h"
#include "../src/Broker.h"
#include "../src/Session.h"
#include "../src/Message.h"
#include "TestClient.h"
#include <cstdio>
#include <fstream>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

namespace MQTT {

namespace {

std::string tempPath(const std::string &name) {
    std::string path = "/tmp/flowmq_store_" + std::to_string(::getpid()) + "_" + name;
    ::remove(path.c_str());
    return path;
}

SubscriptionOptions optionsFor(QoS qos) {
    return SubscriptionOptions{qos, false, false,
                               RetainHandling::DO_NOT_SEND_RETAINED_MESSAGES};
}

Message messageFor(const std::string &topic, const std::string &payload, QoS qos, bool retain = false) {
    Message message;
    message.topic = topic;
    message.payload.assign(payload.begin(), payload.end());
    message.qos = qos;
    message.retain = retain;
    return message;
}

//! One populated state, used by both the store and the broker tests
PersistedState sampleState() {
    PersistedState state;
    state.retained["a/b"] = messageFor("a/b", "kept", QoS::QOS_0, /*retain=*/true);
    state.retained["bin/\x01\x02"] = messageFor("bin/\x01\x02", std::string("\x00\xff", 2), QoS::QOS_1);

    PersistedSession session;
    session.clientId = "client-1";
    session.username = "alice";
    session.nextPacketId = 7;
    session.subscriptions["a/#"] = optionsFor(QoS::QOS_2);
    session.subscriptions["$/shared/g/x"] = SubscriptionOptions{QoS::QOS_1, true, true,
                                                                RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE_IF_NEW};
    session.inflight.emplace_back(3, messageFor("a/b", std::string("\x00\x01\x02", 3), QoS::QOS_1));
    session.blocked.push_back(messageFor("a/c", std::string("\x10\x11", 2), QoS::QOS_2));
    session.awaitingPubrel.insert(3);
    session.awaitingPubrel.insert(9);
    // Outgoing QoS 2 flow awaiting PUBCOMP (broker sent PUBREL)
    session.pendingPubrel.insert(11);
    session.pendingPubrel.insert(12);
    state.sessions.push_back(session);
    return state;
}

void writeRaw(const std::string &path, const std::string &content) {
    std::ofstream out(path, std::ios::binary | std::ios::trunc);
    out << content;
}

} // namespace

TEST(StoreTest, RoundTripsTheWholeState)
{
    const std::string path = tempPath("roundtrip.dat");
    Store store(path, std::chrono::milliseconds(10));
    ASSERT_TRUE(store.enabled());

    const PersistedState expected = sampleState();
    ASSERT_TRUE(store.flushNow([] { return sampleState(); }));

    PersistedState actual;
    std::string error;
    ASSERT_TRUE(store.load(actual, &error)) << error;

    ASSERT_EQ(actual.retained.size(), expected.retained.size());
    for (const auto &entry : expected.retained) {
        auto it = actual.retained.find(entry.first);
        ASSERT_NE(it, actual.retained.end()) << entry.first;
        EXPECT_EQ(it->second.payload, entry.second.payload);
        EXPECT_EQ(it->second.qos, entry.second.qos);
        EXPECT_EQ(it->second.retain, entry.second.retain);
    }

    ASSERT_EQ(actual.sessions.size(), 1U);
    const PersistedSession &session = actual.sessions[0];
    EXPECT_EQ(session.clientId, "client-1");
    EXPECT_EQ(session.username, "alice");
    EXPECT_EQ(session.nextPacketId, 7U);
    ASSERT_EQ(session.subscriptions.size(), 2U);
    EXPECT_EQ(session.subscriptions.at("a/#").maximumQos, QoS::QOS_2);
    const SubscriptionOptions &shared = session.subscriptions.at("$/shared/g/x");
    EXPECT_EQ(shared.maximumQos, QoS::QOS_1);
    EXPECT_TRUE(shared.noLocal);
    EXPECT_TRUE(shared.retainAsPublished);
    EXPECT_EQ(shared.retainHandling, RetainHandling::SEND_RETAINED_MESSAGES_AT_SUBSCRIBE_IF_NEW);

    ASSERT_EQ(session.inflight.size(), 1U);
    EXPECT_EQ(session.inflight[0].first, 3U);
    EXPECT_EQ(session.inflight[0].second.payload, (std::vector<uint8_t>{0x00, 0x01, 0x02}));
    ASSERT_EQ(session.blocked.size(), 1U);
    EXPECT_EQ(session.blocked[0].topic, "a/c");
    EXPECT_EQ(session.blocked[0].payload, (std::vector<uint8_t>{0x10, 0x11}));
    EXPECT_EQ(session.blocked[0].qos, QoS::QOS_2);
    EXPECT_EQ(session.awaitingPubrel, (std::set<uint16_t>{3, 9}));
    EXPECT_EQ(session.pendingPubrel, (std::set<uint16_t>{11, 12}));

    ::remove(path.c_str());
}

TEST(StoreTest, PreservesAnEmptyUsername)
{
    const std::string path = tempPath("anon.dat");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    PersistedSession session;
    session.clientId = "anon";
    session.username = ""; // anonymous client: the field must not shift
    session.nextPacketId = 2;
    session.subscriptions["t"] = optionsFor(QoS::QOS_0);
    state.sessions.push_back(session);

    ASSERT_TRUE(store.flushNow([&state] { return state; }));

    PersistedState loaded;
    std::string error;
    ASSERT_TRUE(store.load(loaded, &error)) << error;
    ASSERT_EQ(loaded.sessions.size(), 1U);
    EXPECT_EQ(loaded.sessions[0].clientId, "anon");
    EXPECT_TRUE(loaded.sessions[0].username.empty());
    EXPECT_EQ(loaded.sessions[0].nextPacketId, 2U);
    EXPECT_EQ(loaded.sessions[0].subscriptions.count("t"), 1U);

    ::remove(path.c_str());
}

TEST(StoreTest, MissingFileLoadsAsEmptyState)
{
    const std::string path = tempPath("missing.dat");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    std::string error;
    EXPECT_TRUE(store.load(state, &error)) << error;
    EXPECT_TRUE(error.empty());
    EXPECT_TRUE(state.retained.empty());
    EXPECT_TRUE(state.sessions.empty());
}

TEST(StoreTest, EmptyFileLoadsAsEmptyState)
{
    const std::string path = tempPath("empty.dat");
    writeRaw(path, "");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    std::string error;
    EXPECT_TRUE(store.load(state, &error)) << error;
    EXPECT_TRUE(state.sessions.empty());

    ::remove(path.c_str());
}

TEST(StoreTest, RejectsACorruptHeader)
{
    const std::string path = tempPath("corrupt.dat");
    writeRaw(path, "NOT-A-FLOWMQ-STATE\nsession 6162 2d 1\n");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    std::string error;
    EXPECT_FALSE(store.load(state, &error));
    EXPECT_FALSE(error.empty()) << "a corrupt file must report an error";

    ::remove(path.c_str());
}

TEST(StoreTest, RejectsAnUnknownRecord)
{
    const std::string path = tempPath("unknown.dat");
    writeRaw(path, "FLOWMQ-STATE-1\nbogus 1 2 3\n");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    std::string error;
    EXPECT_FALSE(store.load(state, &error));
    EXPECT_NE(error.find("bogus"), std::string::npos) << error;

    ::remove(path.c_str());
}

TEST(StoreTest, RejectsAnOutOfRangeQos)
{
    // A corrupt file must not smuggle QoS 7 into a restored message: no handler
    // would ever produce it, and later packet code assumes 0..2.
    const std::string path = tempPath("badenum.dat");
    writeRaw(path, "FLOWMQ-STATE-1\n"
                   "retained 612f62 7 0 76\n");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    std::string error;
    EXPECT_FALSE(store.load(state, &error));
    EXPECT_FALSE(error.empty());

    ::remove(path.c_str());
}

TEST(StoreTest, RejectsAnOutOfRangeSubscriptionOption)
{
    const std::string path = tempPath("badsub.dat");
    // sub <filter-hex> <qos> <noLocal> <rap> <handling>; handling 3 is invalid.
    writeRaw(path, "FLOWMQ-STATE-1\n"
                   "session 636c 2d 1\n"
                   "sub 612f23 0 0 0 3\n");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    std::string error;
    EXPECT_FALSE(store.load(state, &error));
    EXPECT_FALSE(error.empty());

    ::remove(path.c_str());
}

TEST(StoreTest, StateFileIsPrivateToTheOwner)
{
    // Payloads and session usernames live in the file, so it must not be
    // world-readable.
    const std::string path = tempPath("mode.dat");
    Store store(path, std::chrono::milliseconds(10));
    ASSERT_TRUE(store.flushNow([] { return sampleState(); }));

    struct stat st {};
    ASSERT_EQ(::stat(path.c_str(), &st), 0);
    EXPECT_EQ(st.st_mode & 0777, 0600u);

    ::remove(path.c_str());
}

TEST(StoreTest, StaleTempFileDoesNotWedgeLaterWrites)
{
    // A crash between open() and rename() leaves "<path>.tmp" behind. With
    // O_EXCL the next write must clear the stale file and succeed, not fail
    // forever with EEXIST.
    const std::string path = tempPath("stale.dat");
    writeRaw(path + ".tmp", "leftover garbage from a crashed write\n");
    Store store(path, std::chrono::milliseconds(10));
    EXPECT_TRUE(store.flushNow([] { return sampleState(); }));

    PersistedState state;
    EXPECT_TRUE(store.load(state));
    EXPECT_EQ(state.sessions.size(), 1u);

    ::remove(path.c_str());
    ::remove((path + ".tmp").c_str());
}

TEST(StoreTest, DisabledStoreIsInert)
{
    Store store(std::string(), std::chrono::milliseconds(10));
    EXPECT_FALSE(store.enabled());
    // Flushing a disabled store succeeds without writing anything
    EXPECT_TRUE(store.flushNow([] { return sampleState(); }));

    PersistedState state;
    EXPECT_TRUE(store.load(state));
    EXPECT_TRUE(state.sessions.empty());
}

TEST(BrokerStoreTest, SnapshotAndRestoreRoundTrip)
{
    Broker source;
    auto session = std::make_shared<Session>(&source, "durable", /*cleanStart=*/false);
    // Build the durable state in one go: restore() replaces username and
    // subscriptions, so it must run before the session is inserted
    PersistedSession sourceSnapshot;
    sourceSnapshot.clientId = "durable";
    sourceSnapshot.username = "alice";
    sourceSnapshot.nextPacketId = 5;
    sourceSnapshot.subscriptions["a/#"] = optionsFor(QoS::QOS_1);
    sourceSnapshot.inflight.emplace_back(4, messageFor("a/b", "pending", QoS::QOS_1));
    session->restore(sourceSnapshot);
    source.insertSession("durable", session);
    // A retained message published into the broker must be part of the snapshot
    source.publish(messageFor("a/b", "kept", QoS::QOS_0, /*retain=*/true));

    const PersistedState snapshot = source.snapshotState();
    ASSERT_EQ(snapshot.sessions.size(), 1U);
    ASSERT_EQ(snapshot.retained.count("a/b"), 1U);

    // Restore into a fresh broker, as a restart would
    Broker restored;
    restored.restoreState(snapshot);

    EXPECT_EQ(restored.snapshotState().sessions.size(), 1U);
    auto session2 = restored.findSession("durable");
    ASSERT_NE(session2, nullptr);
    EXPECT_TRUE(session2->isPersistent());
    EXPECT_EQ(session2->getUsername(), "alice");

    // The restored session still routes: a publish reaches it while offline
    auto subscriber = test::attach(*session2);
    restored.publish(messageFor("a/b", "live", QoS::QOS_0));
    session2->flush();
    ASSERT_EQ(subscriber->size(), 1U);
    EXPECT_EQ(subscriber->snapshot()[0].topic, "a/b");
    EXPECT_EQ(subscriber->snapshot()[0].payload, std::vector<uint8_t>({'l', 'i', 'v', 'e'}));
}

TEST(BrokerStoreTest, CleanSessionsAreNotPersisted)
{
    Broker broker;
    auto session = std::make_shared<Session>(&broker, "ephemeral", /*cleanStart=*/true);
    session->setPersistent(false);
    broker.insertSession("ephemeral", session);

    EXPECT_TRUE(broker.snapshotState().sessions.empty());
}

TEST(BrokerStoreTest, EveryMutationMarksTheStoreDirty)
{
    const std::string path = tempPath("dirty.dat");
    Store store(path, std::chrono::milliseconds(10));
    Broker broker(nullptr, &store);

    auto session = std::make_shared<Session>(&broker, "c", /*cleanStart=*/false);
    session->setPersistent(true);
    broker.insertSession("c", session);

    // Subscribe is a durable change: a snapshot taken afterwards must contain it
    session->subscribe("x/y", optionsFor(QoS::QOS_0));
    PersistedState state = broker.snapshotState();
    ASSERT_EQ(state.sessions.size(), 1U);
    EXPECT_EQ(state.sessions[0].subscriptions.count("x/y"), 1U);

    session->unsubscribe("x/y");
    state = broker.snapshotState();
    ASSERT_EQ(state.sessions.size(), 1U);
    EXPECT_EQ(state.sessions[0].subscriptions.count("x/y"), 0U);

    ::remove(path.c_str());
}

} // namespace MQTT

TEST(BrokerStoreTest, RestoredSharedGroupsStillRoute)
{
    using namespace MQTT;
    // A shared subscription is stored under its full "$share/group/filter" name
    // but routed by the real filter. Restoring it must register the routing
    // entry under the real filter, or the group would never match again.
    MQTT::Broker source;
    MQTT::PersistedSession sharedSnapshot;
    sharedSnapshot.clientId = "grp";
    sharedSnapshot.subscriptions["$share/g/topic"] = optionsFor(MQTT::QoS::QOS_0);
    auto session = std::make_shared<MQTT::Session>(&source, "grp", /*cleanStart=*/false);
    session->restore(sharedSnapshot);
    source.insertSession("grp", session);

    const MQTT::PersistedState snapshot = source.snapshotState();
    ASSERT_EQ(snapshot.sessions.size(), 1U);

    MQTT::Broker restored;
    restored.restoreState(snapshot);
    auto restoredSession = restored.findSession("grp");
    ASSERT_NE(restoredSession, nullptr);

    auto subscriber = MQTT::test::attach(*restoredSession);
    restored.publish(messageFor("topic", "after-restart", MQTT::QoS::QOS_0));
    restoredSession->flush();

    ASSERT_EQ(subscriber->size(), 1U)
        << "the restored share group did not route a matching publish";
}

namespace MQTT {

TEST(BrokerStoreTest, AnInvalidRestoredFilterIsDroppedInsteadOfBreakingRouting)
{
    // A filter in a state file never went through the SUBSCRIBE validation that
    // rejects a misplaced wildcard, so a hand-written or damaged file can hold
    // one that is not a legal Topic Filter at all. Registering it used to leave
    // an unreachable node in the routing trie that made every publish through it
    // fail; it must simply be dropped, and the rest of the session must survive.
    MQTT::Broker source;
    MQTT::PersistedSession snapshot;
    snapshot.clientId = "restored";
    snapshot.subscriptions["a/#/b"] = optionsFor(MQTT::QoS::QOS_0);   // illegal
    snapshot.subscriptions["good/topic"] = optionsFor(MQTT::QoS::QOS_0);
    auto session = std::make_shared<MQTT::Session>(&source, "restored", /*cleanStart=*/false);
    session->restore(snapshot);
    source.insertSession("restored", session);
    const MQTT::PersistedState persisted = source.snapshotState();

    MQTT::Broker restored;
    ASSERT_NO_THROW(restored.restoreState(persisted));

    auto restoredSession = restored.findSession("restored");
    ASSERT_NE(restoredSession, nullptr) << "the whole session was lost";
    auto subscriber = MQTT::test::attach(*restoredSession);

    // The legal subscription still works...
    ASSERT_NO_THROW(restored.publish(messageFor("good/topic", "hi", MQTT::QoS::QOS_0)));
    restoredSession->flush();
    EXPECT_EQ(subscriber->size(), 1U) << "the legal restored filter stopped routing";

    // ...and publishing through the poisoned subtree is harmless.
    ASSERT_NO_THROW(restored.publish(messageFor("a/x", "poison", MQTT::QoS::QOS_0)));
    restoredSession->flush();
}

TEST(StoreTest, RoundTripsForwardedMessageProperties)
{
    // MQTT 5 properties travel with the publication, so a restored retained or
    // queued message must still carry them to the subscriber.
    const std::string path = tempPath("props.dat");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    Message retained = messageFor("p/q", "kept", QoS::QOS_0, /*retain=*/true);
    retained.properties[PropertyID::CONTENT_TYPE] = std::string("text/plain");
    retained.properties[PropertyID::RESPONSE_TOPIC] = std::string("p/reply");
    retained.properties[PropertyID::PAYLOAD_FORMAT_INDICATOR] = static_cast<uint8_t>(1);
    retained.properties[PropertyID::USER_PROPERTY] = UserProperties{{"k1", "v1"}, {"k2", "v2"}};
    state.retained["p/q"] = retained;

    PersistedSession session;
    session.clientId = "prop-client";
    session.nextPacketId = 4;
    session.inflight.emplace_back(2, retained);
    session.blocked.push_back(retained);
    state.sessions.push_back(session);

    ASSERT_TRUE(store.flushNow([&state] { return state; }));

    PersistedState loaded;
    std::string error;
    ASSERT_TRUE(store.load(loaded, &error)) << error;
    ASSERT_EQ(loaded.retained.size(), 1U);
    const Message &back = loaded.retained.at("p/q");
    ASSERT_EQ(back.properties.size(), 4U);
    EXPECT_EQ(std::get<std::string>(back.properties.at(PropertyID::CONTENT_TYPE)), "text/plain");
    EXPECT_EQ(std::get<std::string>(back.properties.at(PropertyID::RESPONSE_TOPIC)), "p/reply");
    EXPECT_EQ(std::get<uint8_t>(back.properties.at(PropertyID::PAYLOAD_FORMAT_INDICATOR)), 1);
    const auto &pairs = std::get<UserProperties>(back.properties.at(PropertyID::USER_PROPERTY));
    ASSERT_EQ(pairs.size(), 2U);
    EXPECT_EQ(pairs.at("k1"), "v1");
    EXPECT_EQ(pairs.at("k2"), "v2");

    ASSERT_EQ(loaded.sessions.size(), 1U);
    ASSERT_EQ(loaded.sessions[0].inflight.size(), 1U);
    ASSERT_EQ(loaded.sessions[0].blocked.size(), 1U);
    EXPECT_EQ(loaded.sessions[0].inflight[0].second.properties.size(), 4U);
    EXPECT_EQ(loaded.sessions[0].blocked[0].properties.size(), 4U);

    ::remove(path.c_str());
}

TEST(StoreTest, LoadsAStateFileWithoutMessagePropertyColumns)
{
    // Backward compatibility: records written before the property column
    // existed must still load, just with no properties.
    const std::string path = tempPath("legacy.dat");
    writeRaw(path,
             "FLOWMQ-STATE-1\n"
             "retained 742f6f6c64 0 1 6b657074\n"
             "session 6c6567616379 2d 3\n"
             "sub 76 1 0 0 2\n"
             "end\n");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    std::string error;
    ASSERT_TRUE(store.load(state, &error)) << error;
    ASSERT_EQ(state.retained.size(), 1U);
    EXPECT_EQ(state.retained.at("t/old").payload.size(), 4U);
    EXPECT_TRUE(state.retained.at("t/old").properties.empty());
    ASSERT_EQ(state.sessions.size(), 1U);
    EXPECT_EQ(state.sessions[0].subscriptions.count("v"), 1U);

    ::remove(path.c_str());
}

TEST(StoreTest, RoundTripsSubscriptionIdentifier)
{
    const std::string path = tempPath("subid.dat");
    Store store(path, std::chrono::milliseconds(10));

    PersistedState state;
    PersistedSession session;
    session.clientId = "sid-client";
    session.nextPacketId = 2;
    SubscriptionOptions options = optionsFor(QoS::QOS_1);
    options.subscriptionIdentifier = 300;
    session.subscriptions["s/t"] = options;
    state.sessions.push_back(session);

    ASSERT_TRUE(store.flushNow([&state] { return state; }));

    PersistedState loaded;
    std::string error;
    ASSERT_TRUE(store.load(loaded, &error)) << error;
    ASSERT_EQ(loaded.sessions.size(), 1U);
    ASSERT_EQ(loaded.sessions[0].subscriptions.count("s/t"), 1U);
    EXPECT_EQ(loaded.sessions[0].subscriptions.at("s/t").subscriptionIdentifier, 300U);

    ::remove(path.c_str());
}

} // namespace MQTT
