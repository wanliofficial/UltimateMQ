# UltimateMQ

![MQTT Logo](docs/assets/mqtt-logo.png)

UltimateMQ is an experimental MQTT broker written in C++ with cursor.

## Prerequisites

- C++ compiler with C++17 support
- CMake 3.12 or higher
- OpenSSL development headers (`libssl-dev`); the broker still runs in
  plaintext when no certificate is configured

## Building

```bash
mkdir build && cd build
cmake ..
make
```

## Run

```bash
./flowmq
```

### Configuration

All configuration is read from environment variables at startup.

| Variable | Default | Description |
| --- | --- | --- |
| `MQTT_PORT` | `1883` | TCP port the broker listens on |
| `FLOWMQ_DEBUG` | unset | Emit debug traces to stderr |
| `FLOWMQ_PASSWORD_FILE` | unset | User database; enables authentication when present |
| `FLOWMQ_ACL_FILE` | unset | Per-user topic access rules; enables ACL enforcement when present |
| `FLOWMQ_STATE_FILE` | unset | Durable state file; retained messages and persistent sessions survive a restart when set |
| `FLOWMQ_MAX_PACKET_SIZE` | `1048576` (1 MiB) | Largest Remaining Length accepted from a single client, in bytes; also advertised to MQTT 5 clients |
| `FLOWMQ_WRITE_TIMEOUT_MS` | `5000` (5 s) | How long a single write may make no progress before the connection is dropped (bounds a stalled reader) |
| `FLOWMQ_MAX_SUBSCRIPTIONS` | `1024` | Maximum number of topic filters a single session may hold; further filters are rejected |
| `FLOWMQ_MAX_SHARED_SUBSCRIPTIONS` | `FLOWMQ_MAX_SUBSCRIPTIONS` | Maximum number of shared (`$share/...`) subscriptions a single session may hold |
| `FLOWMQ_MAX_RETAINED` | unlimited | Maximum number of distinct topics that may hold a retained message; further distinct-topic retains are dropped. Replacing an existing topic's retained message is always allowed |
| `FLOWMQ_MAX_PUBLISH_RATE` | unlimited | Per-client inbound `PUBLISH` rate in messages/second; a publisher that exceeds it is disconnected |
| `FLOWMQ_SERVER_KEEP_ALIVE` | unset | Preferred keep alive in seconds; MQTT 5 clients asking for more (or for none) are told this value in CONNACK |
| `FLOWMQ_SYS_TOPICS` | unset | Publish the optional `$SYS/broker` monitoring tree (see below); off by default |
| `FLOWMQ_MAX_CONNECTIONS` | `1024` | Maximum number of simultaneous client connections; further connections are accepted then closed |
| `FLOWMQ_TLS_CERT` | unset | Server certificate (PEM); enables TLS when set together with `FLOWMQ_TLS_KEY` |
| `FLOWMQ_TLS_KEY` | unset | Private key (PEM) matching `FLOWMQ_TLS_CERT` |
| `FLOWMQ_TLS_CA` | unset | CA bundle used to verify client certificates |
| `FLOWMQ_TLS_VERIFY_CLIENTS` | unset | When set, a client certificate signed by `FLOWMQ_TLS_CA` is required |
| `FLOWMQ_TLS_CERT_AS_USERNAME` | unset | With mutual TLS, authorise a client that sends no MQTT username under its certificate CN |
| `FLOWMQ_WEBSOCKET_PORT` | unset | Additional TCP port serving MQTT over WebSocket (`ws://`, or `wss://` when TLS is configured) |
| `FLOWMQ_WS_REQUIRE_SUBPROTOCOL` | unset | When set, a WebSocket upgrade that does not offer the `mqtt` subprotocol is refused with HTTP 400 (the binding's requirement); otherwise such clients are still accepted |
| `FLOWMQ_MQTT_SN_PORT` | unset | UDP port serving MQTT-SN 1.2 clients through the built-in gateway |
| `FLOWMQ_MQTT_SN_PREDEFINED_TOPICS` | unset | Predefined MQTT-SN topic ids, e.g. `1=sensors/temp,2=home/#` |
| `FLOWMQ_QUIC_PORT` | unset | UDP port serving MQTT over QUIC; requires a build with libmsquic (see below) |

#### MQTT over WebSocket (`FLOWMQ_WEBSOCKET_PORT`)

When the variable is set the broker opens an additional TCP listener that
speaks the OASIS MQTT-over-WebSocket binding: the client performs the standard
HTTP Upgrade handshake offering the `mqtt` subprotocol, then carries ordinary
MQTT 3.1.1/5 packets in WebSocket binary frames. With a TLS certificate
configured the same listener speaks WSS. Everything else (sessions, retained
messages, QoS, ACLs) is shared with plain TCP clients.

#### MQTT-SN gateway (`FLOWMQ_MQTT_SN_PORT`)

When the variable is set the broker serves MQTT-SN 1.2 clients on that UDP
port. The gateway maps MQTT-SN datagrams onto ordinary broker sessions, so
sensor clients share subscriptions, retained messages and QoS 1/2 flows with
MQTT clients. Supported: CONNECT with the WILLTOPIC/WILLMSG handshake,
REGISTER with gateway-assigned topic ids, PUBLISH by topic id / predefined id
/ short name, SUBSCRIBE by topic name / predefined id / short name, QoS 0/1/2,
retained messages, keep alive (1.5× duration), sleeping DISCONNECT with a
duration, SEARCHGW/GWINFO and periodic ADVERTISE broadcasts. Notes: an
MQTT-SN PUBLISH never carries a topic name, so clients must REGISTER a topic
name first and then publish by its topic id; the gateway serves anonymous
deployments only (a broker with `FLOWMQ_PASSWORD_FILE` refuses SN connects);
predefined topic ids come from `FLOWMQ_MQTT_SN_PREDEFINED_TOPICS`
(`id=topic` pairs, comma separated).

#### MQTT over QUIC (`FLOWMQ_QUIC_PORT`)

Built only when libmsquic is found at configure time (otherwise the feature is
compiled out and the variable is unknown). When enabled and the variable is
set, one QUIC connection is one MQTT network connection: the client opens its
first bidirectional stream and speaks ordinary MQTT 3.1.1/5 on it. QUIC always
speaks TLS 1.3, reusing `FLOWMQ_TLS_CERT`/`FLOWMQ_TLS_KEY` when configured;
without them the listener generates an ephemeral self-signed certificate in a
private temporary directory for development, which clients should connect to
with certificate verification disabled or pinning.

The listener binds its UDP port, completes the QUIC/TLS 1.3 handshake, accepts
the client's first bidirectional stream, parses the MQTT CONNECT on it and
establishes the session exactly like a TCP client. Note that this path depends
on msquic's stream API, which has platform-specific behaviour: with the
Homebrew libmsquic 2.6.x build on Intel macOS the server side accepts and reads
stream data but the bytes written back do not reach the peer (a loopback test
using only msquic's own client and server segfaults), so end-to-end delivery
could not be verified on that platform. Linux builds with the same API — and
any platform where msquic's stream send path is sound — should not be affected.


When neither file is set the broker stays fully open (every client may
connect and use every topic), which keeps the historical behaviour.

#### Users (`FLOWMQ_PASSWORD_FILE`)

One entry per line, `#` starts a comment:

```
# plaintext
alice:secret
# legacy pre-hashed: $sha256$<salt-text>:<sha256(salt + password)>
bob:$sha256$a1b2c3d4:9f2c...c41d
# preferred: $pbkdf2-sha256$<iterations>$<salt-hex>$<hash-hex>
carol:$pbkdf2-sha256$210000$0011223344556677$8f14...9ac1
```

Prefer the `$pbkdf2-sha256$` form: it is PBKDF2-HMAC-SHA256 with the given
iteration count, so a leaked password file is expensive to crack offline,
whereas a single salted `$sha256$` digest is cheap to brute-force. The salt in
the `$pbkdf2-sha256$` entry is used as written (its hex text is the salt
string), so deriving the digest from the file with `Auth::pbkdf2Password()`
reproduces the stored value. The legacy `$sha256$` form stays supported so
existing files keep working unchanged. A malformed hex salt/hash is a load
error rather than a credential that silently never matches.

Once a users file is loaded every client must send a username and password;
a missing or wrong credential is answered with CONNACK `0x86` (bad user name
or password, MQTT 5) / `0x04`-style failure for 3.1.1 and the socket is closed.
An explicitly configured but empty users file therefore rejects every client;
leaving the setting unset is the only way to disable password authentication.

#### Access control (`FLOWMQ_ACL_FILE`)

```
# user,action,topic-filter
alice,read,sensors/#
alice,write,commands/alice
bob,read,#
*,read,public/#
```

* `read` controls subscriptions (and therefore deliveries and retained
  messages); `write` controls PUBLISH and Will messages.
* Topic filters use the normal MQTT wildcard syntax.
* The special user `*` applies to every client.
* If an ACL file is loaded, anything not explicitly granted is denied. An
  explicitly configured but empty ACL file consequently denies every read and
  write; leaving the setting unset disables ACL enforcement. A denied SUBSCRIBE
  returns SUBACK `0x87` (MQTT 5) or `0x80` (3.1.1); a denied PUBLISH returns
  PUBACK/PUBREC `0x87` and the message is not routed; a denied QoS 0 PUBLISH
  disconnects an MQTT 5 client and is dropped for 3.1.1.
* **A subscription is granted when it can reach *some* granted topic**, and each
  delivery is then checked against the concrete topic. With the rules above,
  `alice` subscribing to `sensors/+` receives `sensors/temp` but not
  `private/secret`; she may also subscribe to `#` and still only receive
  `sensors/...`. A filter that can reach nothing she may read (e.g. `other/#`)
  is refused with `0x87`.
* **Shared subscriptions are judged by their real filter**: `$share/g/sensors/#`
  is authorised as `sensors/#`, since the `$share/group/` prefix is routing
  metadata and never part of a topic. Without this a shared subscription could
  not be used under an ACL at all.
* A session is bound to the user that created it: connecting with the same
  client id as a different user discards the old session instead of handing
  over its subscriptions and buffered messages.

#### Authentication methods (enhanced authentication)

The broker implements username/password authentication only and supports no
enhanced authentication method, and it says so instead of pretending otherwise:

* a `CONNECT` carrying an **Authentication Method** property is refused with
  CONNACK `BAD_AUTHENTICATION_METHOD` (`0x8C`);
* **Authentication Data** without a Method is a protocol error
  (`PROTOCOL_ERROR`, `0x82`), since MQTT-3.1.2-17 requires the pair to travel
  together;
* an unexpected `AUTH` packet is answered with `DISCONNECT` `0x82`
  (`PROTOCOL_ERROR`) and the connection is closed: no enhanced authentication
  exchange is in progress (MQTT-4.12.0-7, §4.13.1).

#### Durable state (`FLOWMQ_STATE_FILE`)

Without this variable the broker keeps everything in memory, exactly as before.
With it, the following survive a restart:

* retained messages;
* every session created with `cleanStart=false`, together with its
  subscriptions, its next packet id and its unacknowledged QoS 1/2 messages.
  An unacknowledged message is retransmitted with `DUP=1` on reconnect.
* an inbound QoS 2 flow that is still awaiting its `PUBREL`: on reconnect (or
  after a restart) the broker resends the `PUBREC` so the client can finish the
  handshake with its `PUBREL`, instead of abandoning the flow.

The file is written by a background thread at most once per second and again on
`SIGINT`/`SIGTERM`, always atomically (a temporary file is renamed into place),
so a crash never leaves a half-written state file behind. Only a missing file
means first startup; an empty, truncated, malformed, or duplicate record aborts
startup instead of silently discarding sessions. New snapshots use the
`FLOWMQ-STATE-2` header, which adds explicit origin metadata and inflight
Subscription Identifiers. Version 1 files remain readable; downgrading after a
version 2 snapshot is unsupported because older binaries cannot preserve those
fields.

```bash
FLOWMQ_STATE_FILE=/var/lib/flowmq/state.dat ./flowmq
```

QoS 0 messages are at-most-once and are not persisted. The offline queue of a
session is not stored separately: QoS>0 messages already live in its
unacknowledged set and are re-queued on reconnect, which avoids duplicates.

#### Inbound QoS 2 packet identifiers

An in-flight QoS 2 flow is tied to its packet identifier until the client's
`PUBREL` has been answered, and the broker reports the problems MQTT defines
instead of silently accepting them (MQTT-3.3.4-1, MQTT-4.3.3-5):

* a second `PUBLISH` reusing an identifier whose flow has not completed is
  answered with `PUBREC` `PACKET_IDENTIFIER_IN_USE` (`0x91`) and is **not**
  routed — otherwise its `PUBREL` would have no counterpart;
* a `PUBREL` for an identifier that started no flow, and a `PUBREC` for an
  outstanding delivery that does not exist, are answered with
  `PACKET_IDENTIFIER_NOT_FOUND` (`0x92`). The connection stays open, so a
  client that lost track of its state can recover.

MQTT 3.1.1 acks (`PUBACK`, `PUBREC`, `PUBREL`, `PUBCOMP`) are always exactly a
packet identifier — two bytes of body, with no reason code and no property
section — whatever the broker's outcome was.

#### Session expiry on disconnect (MQTT 5)

A persistent session is normally kept when its connection ends so it can be
resumed. MQTT 5 lets a client control this on the way out: a `DISCONNECT` may
carry a **Session Expiry Interval**, and the broker honours it
(MQTT-3.14.4-2):

* interval `0` — the session is discarded immediately, even if it was
  persistent, so a later reconnect starts fresh;
* a positive interval — the session is kept and can be resumed **until that many
  seconds after the disconnect pass**; a background sweep then drops it if no
  client reconnects, so an abandoned session cannot accumulate indefinitely.
  Reconnecting before the deadline revives it;
* no `DISCONNECT` (an abrupt disconnect or a keep-alive timeout) — the session
  is kept if it was created with `cleanStart=false`, exactly as before.

MQTT 3.1.1 has no such property, so a graceful `DISCONNECT` ends the session.

> Note: a client's own `CONNECT` session-expiry and the DISCONNECT interval use
> the same wire property; a malformed property (e.g. a wrong length byte) causes
> the broker to drop the connection rather than mis-parse it. Integer properties
> (session-expiry, message-expiry, receive-maximum, topic-alias, …) are read in
> big-endian wire order, so a value such as `3600` seconds is honoured exactly.

The sweep runs from the accept loop about once a second; an idle server therefore
wakes periodically just to expire sessions, at negligible cost. The same sweep
also drops any retained message that has passed its expiry.

#### Message expiry (MQTT 5)

A `PUBLISH` may carry a **Message Expiry Interval**; the broker records the
absolute time at which the message goes stale (MQTT-3.3.2-9) and drops it rather
than delivering it once that time has passed:

* to a **live subscriber** whose fan-out happens after the deadline;
* from an **offline subscriber's** buffered queue, when it is finally drained on
  reconnect;
* from the **retained set**, where it is additionally purged by the background
  sweep so it is never re-delivered to a future subscriber.

The interval is in-memory only: it is never written to the durable state file, so
a message restored after a restart is treated as having no expiry.

#### Receive Maximum / flow control (MQTT 5)

A subscriber tells the broker how much work it is willing to owe at once with the
**Receive Maximum** property on `CONNECT` (MQTT-4.9). The broker caps the number of
QoS 1/2 messages it keeps *outstanding* (sent but not yet acknowledged) to that
value:

* while the outstanding set is below the limit, QoS 1/2 messages are delivered and
  counted as inflight as usual;
* once the limit is reached, further QoS 1/2 messages for that client are **parked**
  (no packet id, not inflight) until an acknowledgement frees a slot — `PUBACK`,
  `PUBREC` or `PUBCOMP` then releases the next parked message in order;
* **QoS 0 is never flow-controlled**: it has no acknowledgement, so there is no
  outstanding count to bound.

The default is `65535`, i.e. effectively unlimited, and a value of `0` is a protocol
violation (the broker answers with a `PROTOCOL_ERROR` CONNACK and closes the
connection). The parked backlog is part of the durable session, so it survives a
restart and is delivered once the client reconnects and the window allows again.

#### Topic aliases (MQTT 5)

A client that publishes many messages to the same topic can send the full topic
name once and then refer to it by a two-byte **Topic Alias**, which saves
bandwidth on the wire. The broker advertises the number of aliases it will accept
through the `Topic Alias Maximum` CONNACK property (currently `16`):

* a `PUBLISH` that carries both a topic name and an alias **registers or updates**
  that alias (MQTT-3.3.2-9);
* a `PUBLISH` with an **empty topic name** and an alias is routed to whatever
  topic that alias last mapped to (MQTT-3.3.2-11);
* an alias of `0`, an alias above the advertised maximum, or an alias that was
  never mapped is a protocol error: the broker replies with a `DISCONNECT`
  (`TOPIC_ALIAS_INVALID`, `0x94`) and closes the connection.

Alias mappings are scoped to a single **network connection**, not to the session:
a reconnect with the same client id starts with an empty table, as MQTT requires.
MQTT 3.1.1 has no property section, so aliases are simply never present there.

#### Subscription identifiers (MQTT 5)

An MQTT 5 client may tag a `SUBSCRIBE` with a **Subscription Identifier**. The
identifier is stored with every filter in that request, survives a persistent
session and a broker restart, and is echoed on each `PUBLISH` the subscription
causes (MQTT-3.3.2-5), including retained deliveries and messages replayed for an
offline session. A subscriber can therefore tell *which* of its subscriptions
matched without inspecting the topic.

* The identifier is a Variable Byte Integer with no length prefix
  (MQTT-3.8.2.1.2); values up to 268 435 455 are accepted.
* A value of `0` is a protocol error: the broker answers with a
  `PROTOCOL_ERROR` `DISCONNECT` and closes the connection (MQTT-3.8.2-16).
* Subscribing without the property clears any identifier previously stored for
  that filter, so a re-subscribe always reflects the latest request.
* Shared subscriptions carry the identifier too, so each member of a share group
  sees it on the copy it receives.

MQTT 3.1.1 has no property section, so no identifier is ever stored or emitted.

#### Client-declared `Maximum Packet Size` (MQTT 5)

The `FLOWMQ_MAX_PACKET_SIZE` knob below bounds what the broker **reads**. The
matching knob in the other direction is the client's own `Maximum Packet Size`
CONNECT property (MQTT-3.1.2-24): it is the largest packet the client is willing
to receive. The broker honours it on every outbound packet, `PUBLISH` included.

* A packet that would exceed the declared limit is **dropped** rather than sent.
  Sending it would make a conforming client close the connection and lose every
  later delivery as well, so withholding the oversized message keeps the rest of
  the session alive.
* A declared value of `0` is a protocol error: the broker answers with a
  `PROTOCOL_ERROR` CONNACK and closes the connection.
* Clients that declare nothing keep receiving packets of any size (up to the
  broker's own limit), so behaviour is unchanged for them.

#### Application properties on delivery (MQTT 5)

Properties an MQTT 5 publisher attaches to a message belong to the
**application message**, not to the connection, so the broker forwards them
unchanged to every subscriber (MQTT-3.3.2-4):

* `Payload Format Indicator`, `Content Type`, `Response Topic`,
  `Correlation Data` and `User Properties` are carried verbatim.
* `Message Expiry Interval` is forwarded as the time that is **left**, not the
  value the publisher wrote (MQTT-3.3.2-9). A message that expired while it
  waited is dropped instead of delivered.
* `Topic Alias` and `Subscription Identifier` are per-connection and are never
  copied from the publisher: the alias table is local to each link, and the
  identifier comes from the subscription that caused the delivery.

These properties are part of the durable session too, so a queue that is
restored after a restart still forwards them.

MQTT 3.1.1 has no property section, so 3.1.1 deliveries carry none and a
3.1.1 publisher's messages simply arrive without them.

#### Outbound `PUBLISH` properties (MQTT 5)

Every property the broker attaches to a delivery — `Subscription Identifier`,
`Message Expiry Interval`, `Payload Format Indicator`, `Content Type`,
`Response Topic`, `Correlation Data`, `Topic Alias` and `User Properties` — is
serialised into the `PUBLISH` property section, whose length is a Variable Byte
Integer. MQTT 3.1.1 deliveries carry no property section at all.

#### Transport security (`FLOWMQ_TLS_CERT` / `FLOWMQ_TLS_KEY`)

By default the listener speaks plaintext, so with authentication enabled the
username and password cross the network in the clear. Supplying a certificate
turns the listener into a TLS endpoint (TLS 1.2 or newer) and every byte,
credentials included, is then encrypted:

```bash
FLOWMQ_TLS_CERT=/etc/flowmq/server.crt \
FLOWMQ_TLS_KEY=/etc/flowmq/server.key \
FLOWMQ_PASSWORD_FILE=/etc/flowmq/users.txt \
./flowmq
```

* `FLOWMQ_TLS_CERT` and `FLOWMQ_TLS_KEY` (PEM) must be given together; setting
  only one aborts startup with an error naming the missing variable.
* The cipher is chosen by OpenSSL; TLS 1.3 is negotiated when the client
  supports it.
* A plaintext client connecting to a TLS listener fails during the handshake
  and never reaches the MQTT layer.
* Set `FLOWMQ_TLS_VERIFY_CLIENTS` together with `FLOWMQ_TLS_CA` to also require
  a client certificate signed by that CA (mutual TLS). A client without a valid
  certificate is dropped at the handshake.
* With mutual TLS in place, `FLOWMQ_TLS_CERT_AS_USERNAME` lets the certificate
  stand in for the MQTT username (see below).

##### Certificate identity (`FLOWMQ_TLS_CERT_AS_USERNAME`)

A verified client certificate already proves who the client is, so having to
send a username **and** a password on top of it is redundant. When
`FLOWMQ_TLS_CERT_AS_USERNAME` is set, a client that presents no MQTT username is
authorised under its certificate's subject Common Name (CN), and the ACL is
enforced for that identity exactly as for a username:

```bash
FLOWMQ_TLS_CERT=/etc/flowmq/server.crt \
FLOWMQ_TLS_KEY=/etc/flowmq/server.key \
FLOWMQ_TLS_CA=/etc/flowmq/clients-ca.crt \
FLOWMQ_TLS_VERIFY_CLIENTS=1 \
FLOWMQ_TLS_CERT_AS_USERNAME=1 \
FLOWMQ_PASSWORD_FILE=/etc/flowmq/users.txt \
FLOWMQ_ACL_FILE=/etc/flowmq/acl.txt \
./flowmq

# acl.txt: what the holder of CN=telemetry-01 may do
# telemetry-01,read,sensors/#
# telemetry-01,write,sensors/telemetry-01
```

* The identity is taken from the **verified** certificate only: a plaintext
  connection, a client that presented no certificate, or one whose chain failed
  to verify yields no identity and is never authorised this way.
* The CN is used as-is; if the certificate has no CN the whole subject DN is
  used instead.
* A client that **does** send a username is still authenticated by
  username/password, which takes precedence over its certificate.
* When authentication is enabled and neither a username nor a certificate
  identity is available, the connection is refused with CONNACK
  `BAD_USERNAME_OR_PASSWORD` (`0x86`), as before.
* Unset by default: without it, mutual TLS only proves the client holds a valid
  certificate, and a username is still required to be authorised.

#### Maximum packet size (`FLOWMQ_MAX_PACKET_SIZE`)

A client can declare any Remaining Length up to 256 MiB in an MQTT header. Without
a cap the broker would buffer that declaration for a client that dribbles the body
out (or never finishes), so one connection can be made to pin a large amount of
memory. `FLOWMQ_MAX_PACKET_SIZE` bounds this: as soon as a packet's Remaining
Length is read and found to exceed the limit, the connection is dropped before any
body byte is buffered.

```bash
FLOWMQ_MAX_PACKET_SIZE=8388608 ./flowmq   # allow up to 8 MiB per packet
```

* The limit defaults to 1 MiB, which comfortably covers normal telemetry.
* The limit is applied to every packet, including the first `CONNECT`.
* MQTT 5 clients are told the value through the `Maximum Packet Size` CONNACK
  property, so well-behaved clients keep their own packets within it. MQTT 3.1.1
  has no property section and is simply enforced on the wire.
* A non-numeric or zero value aborts startup with an error naming the variable.

#### Stalled-write timeout (`FLOWMQ_WRITE_TIMEOUT_MS`)

Each connection is served by its own thread, and writes to a client are blocking:
if a subscriber stops reading, its TCP receive window closes and the broker's
`send` waits until the peer drains the data. Without a bound, one slow or hung
reader can pin a connection thread (and everything queued behind it) forever.
`FLOWMQ_WRITE_TIMEOUT_MS` bounds that wait: when a single write makes no progress
for the timeout, the connection is treated as gone and dropped, freeing its
thread. TLS connections already bounded the wait this way; this applies the same
bound to plaintext sockets.

```bash
FLOWMQ_WRITE_TIMEOUT_MS=2000 ./flowmq   # drop a stalled reader after 2 s
```

* The timeout defaults to 5 s, long enough for a transiently slow reader to
  catch up but short enough that a hung client cannot hold a thread indefinitely.
* It only fires when a write makes **no** progress for the whole window; a client
  that keeps draining data is never dropped, however large the transfer.
* A non-numeric or non-positive value aborts startup with an error naming the
  variable.

#### Subscription limit (`FLOWMQ_MAX_SUBSCRIPTIONS`)

A client may send any number of `SUBSCRIBE` packets, and every distinct topic
filter it registers costs memory (in the session and in the routing trie).
Without a bound, one client could exhaust memory by subscribing to an unbounded
set of filters. `FLOWMQ_MAX_SUBSCRIPTIONS` caps how many distinct filters a
single session may hold.

```bash
FLOWMQ_MAX_SUBSCRIPTIONS=256 ./flowmq   # allow up to 256 filters per session
```

* The limit defaults to 1024, well above what most clients need.
* Filters beyond the limit are rejected in the `SUBACK` with `QUOTA_EXCEEDED`
  (MQTT 5) or `0x80` failure (MQTT 3.1.1) and are not registered; the rest of a
  multi-topic `SUBSCRIBE` is still processed.
* Re-subscribing an **already-held** filter is always allowed — it only updates
  the QoS / options and does not consume a new slot.
* `UNSUBSCRIBE` frees slots, so a session at its limit can release filters and
  subscribe to new ones.
* A non-numeric, negative or zero value aborts startup naming the variable.

#### Shared subscription limit (`FLOWMQ_MAX_SHARED_SUBSCRIPTIONS`)

A shared subscription (`$share/group/filter`) is counted **separately** from
ordinary filters: one entry also registers a group member in the broker, so an
unbounded set of them would grow broker memory independently of the plain
subscription quota. `FLOWMQ_MAX_SHARED_SUBSCRIPTIONS` caps how many distinct
shared subscriptions a single session may hold.

```bash
FLOWMQ_MAX_SHARED_SUBSCRIPTIONS=32 ./flowmq   # up to 32 shared subscriptions
```

* Defaults to the value of `FLOWMQ_MAX_SUBSCRIPTIONS`, so raising the plain limit
  raises the shared one too. Set it explicitly to bound them differently.
* Filters beyond the limit are rejected in the `SUBACK` with `QUOTA_EXCEEDED`
  (MQTT 5) or `0x80` failure (MQTT 3.1.1); the rest of a multi-topic
  `SUBSCRIBE` is still processed.
* Re-subscribing an **already-held** shared filter is always allowed, and
  `UNSUBSCRIBE` frees a slot.
* A non-numeric, negative or zero value aborts startup naming the variable.

#### Subscription options (`No Local`, `Retain As Published`, `Retain Handling`)

Each `SUBSCRIBE` entry carries subscription options, all honoured on the live
delivery path:

* **No Local** (MQTT-3.8.2-3): a subscription with this option set does not
  receive messages the *same client* published. The comparison uses the session
  that originated the message, so a client still receives other clients' messages
  on the same topic. Retained messages delivered on subscribe have no originator
  and are therefore always delivered.
* **Retain As Published** (MQTT-3.3.1-9): by default the broker clears the retain
  flag when forwarding a live message; set this to forward it exactly as received.
* **Retain Handling** (MQTT-3.3.1-9): `0` sends retained messages on subscribe,
  `1` only if the subscription is new, `2` never sends retained messages.
* **Maximum QoS**: the delivered QoS is the minimum of the published QoS and the
  granted QoS.

#### Shared subscriptions (`$share/group/filter`)

A shared subscription splits the messages for a topic filter across a group of
sessions, so a pool of workers can share one queue. Groups are independent of
each other and of ordinary subscriptions:

* **Each group receives one copy.** `$share/g1/topic` and `$share/g2/topic` are
  two different groups on the same topic, so a publish reaches one member of
  `g1` *and* one member of `g2` — two deliveries in total (MQTT-4.8.2).
* **A plain subscription is a group of its own.** A client subscribed to
  `topic` and a group subscribed to `$share/g/topic` each get a copy.
* **Distribution inside a group is round-robin** (MQTT-4.8.1). Members take
  turns, so a pool splits traffic evenly instead of one member receiving most of
  it. A member whose session is gone is skipped rather than consuming a turn.
* **The filter keeps its wildcards**: `$share/g/sensors/+` matches
  `sensors/a` but not `sensors/a/b`. The `$share/group` prefix is only routing
  metadata and is never part of the matched topic.
* **Group membership is part of a persistent session**, so it survives a
  restart together with its subscriptions.

#### Publish rate limit (`FLOWMQ_MAX_PUBLISH_RATE`)

Every inbound `PUBLISH` is fanned out to all matching subscribers, so one fast
publisher can saturate every connection thread the broker runs. The keep-alive
timer bounds a silent client, and the write-stall timeout bounds a slow reader,
but nothing bounded how many messages a single healthy client could inject per
second. `FLOWMQ_MAX_PUBLISH_RATE` does.

```bash
FLOWMQ_MAX_PUBLISH_RATE=500 ./flowmq   # at most 500 messages/second per client
```

* Unset by default, which keeps the historical behaviour of no limit.
* The limit is a **token bucket** per connection: it starts with one second's
  allowance (so a short burst is tolerated) and refills at the configured rate.
  Bursts beyond that are refused rather than queued.
* A publisher that exceeds its allowance is **disconnected**: MQTT 5 clients get
  a `DISCONNECT` with `QUOTA_EXCEEDED` (`0x97`), MQTT 3.1.1 connections are
  simply closed. The messages it already sent are delivered normally.
* Other clients are unaffected: the limit applies per connection and resets on
  reconnect.
* A non-numeric, negative or zero value aborts startup naming the variable.

#### Server keep alive (`FLOWMQ_SERVER_KEEP_ALIVE`)

The keep alive is a client promise: "I will send something at least this often,
otherwise consider me dead". The broker reaps a connection after 1.5 times the
promised interval, but a client is free to ask for `0`, meaning *no* keep alive,
and such a connection is **never** reaped — a crashed client can hold a socket
and a thread indefinitely. `FLOWMQ_SERVER_KEEP_ALIVE` lets the broker impose its
own value.

```bash
FLOWMQ_SERVER_KEEP_ALIVE=60 ./flowmq   # no client may idle longer than 60 s
```

* Unset by default, which keeps the historical behaviour of honouring exactly
  what the client asked for.
* When set, an MQTT 5 client that asks for **more** than this value, or for
  `0`, is answered with the `Server Keep Alive` CONNACK property and must use
  the broker's value instead (MQTT-3.1.2-21). The broker then reaps the
  connection at 1.5x the override.
* A client asking for **less** than the broker's preference is left alone: a
  shorter promise is already stricter, and the broker never loosens it.
* MQTT 3.1.1 has no property section, so 3.1.1 clients keep their own value.
* A non-numeric, negative, zero or greater-than-`65535` value aborts startup
  naming the variable.

#### Connection limit (`FLOWMQ_MAX_CONNECTIONS`)

Each connected client holds a thread and a socket, so an unbounded accept loop
is a resource-exhaustion lever. `FLOWMQ_MAX_CONNECTIONS` caps how many client
connections the broker serves at once.

```bash
FLOWMQ_MAX_CONNECTIONS=512 ./flowmq   # serve at most 512 clients at a time
```

* The limit defaults to 1024.
* Once it is reached, newly accepted connections are closed immediately (the
  client never receives a `CONNACK`); already-established clients are unaffected.
* Disconnecting a client frees a slot for a new one.
* A non-numeric, negative or zero value aborts startup naming the variable.

#### Restarting on the same port

The listener sets `SO_REUSEADDR`, so a restart (a redeploy, a crash recovery, or
any quick stop/start) can take the port back **immediately**. Without it, the
previous instance's sockets linger in `TIME_WAIT` and `bind()` would fail with
`EADDRINUSE` for up to a minute. A bind failure now also reports the OS error,
so a genuine conflict (another process already listening) is obvious. This only
allows reusing ports that are in `TIME_WAIT`; it does not let two live processes
share the port.

#### Reserved `$` topics

Topics whose first character is `$` (such as `$SYS/...`) are reserved by MQTT
and are matched according to MQTT-4.7.2-1:

* a subscription whose **first level is a wildcard** (`#`, `+/state`, `+`) does
  **not** receive those topics, nor their retained messages;
* a subscription that names the first level explicitly (`$SYS/#`,
  `$SYS/+/uptime`) does receive them.

A `$` anywhere other than the first character is an ordinary character, so
`a/$x` still matches `a/+`.

#### Topic name and filter validation

A PUBLISH topic, a Will topic and a SUBSCRIBE filter are all validated before
they touch the routing path:

* a **topic name** (PUBLISH or Will) must contain no wildcard character —
  `a/+` and `a/#` are illegal. A QoS 1/2 PUBLISH gets the reason code in its
  ack (`TOPIC_NAME_INVALID`); MQTT 5 QoS 0 is answered with a `DISCONNECT`
  (`TOPIC_NAME_INVALID`, `0x90`); MQTT 3.1.1 is dropped silently, and a bad Will
  topic is refused at CONNECT;
* a **topic filter** (SUBSCRIBE) must be well-formed — `#` only as the final
  level and never sharing a level with other characters (`a/#` and `+/b/#` are
  fine, `a/b/c#` and `#/a` are not). A bad filter is reported per-filter in the
  SUBACK as `TOPIC_FILTER_INVALID` (MQTT 5, `0x8f`) or `0x80` (MQTT 3.1.1), and
  the remaining filters in the same SUBSCRIBE are still processed.

#### Anonymous clients (zero-length Client ID)

A `CONNECT` may carry an empty Client ID. The broker then **names the client
itself** (MQTT-3.1.3-6) and reports the chosen value in the CONNACK's
`Assigned Client Identifier` property (MQTT-3.1.3-7), so the client can come
back under that name:

* the generated name (`auto-` plus a random suffix) is checked against the
  sessions already held, so two anonymous clients never share one session;
* an **anonymous** client that asks for `cleanStart=false` is refused —
  `CLIENT_IDENTIFIER_NOT_VALID` (`0x85`) on MQTT 5, return code `0x02` on
  MQTT 3.1.1 — because there is no name the session could be resumed under;
* MQTT 3.1.1 has no property section, so it cannot be told the assigned name;
  the connection still works, it just cannot be resumed by name.

#### Malformed packets and protocol errors

An MQTT 5 client that sends something the broker cannot accept is told why,
instead of watching the socket vanish (MQTT-4.13):

| Situation | Reported as |
|---|---|
| A packet that cannot be parsed, a duplicated property, or a boolean property that is neither 0 nor 1 | `DISCONNECT` `MALFORMED_PACKET` (`0x81`) |
| A packet larger than `FLOWMQ_MAX_PACKET_SIZE` | `DISCONNECT` `PACKET_TOO_LARGE` (`0x95`) |
| A `SUBSCRIBE` with no filters, a subscription Maximum QoS of 3, Retain Handling 3, or No Local on a shared subscription | `DISCONNECT` `PROTOCOL_ERROR` (`0x82`) |
| An `UNSUBSCRIBE` with no filters | `DISCONNECT` `PROTOCOL_ERROR` (`0x82`) |
| Reserved bits 6–7 set in the subscription options | `DISCONNECT` `MALFORMED_PACKET` (`0x81`) |

MQTT 3.1.1 has no server-sent `DISCONNECT`, so in every one of these cases the
connection is simply closed. Each property may appear at most once per packet
(User Property excepted, which is a list), and an empty topic filter is refused
per-filter with `TOPIC_FILTER_INVALID` (`0x8f`) rather than failing the whole
`SUBSCRIBE`.

#### Session takeover

A `CONNECT` with `cleanStart=false` for a client id whose session is still held
**takes it over** (MQTT-3.1.4-1): the new connection becomes the session's live
client and the previous one is detached, so two connections can never interleave
writes on one session. (With `cleanStart=true` the old session is simply
discarded instead.) The hand-over follows MQTT-3.1.4-3:

* the displaced connection is shut down — an MQTT 5 client receives
  `DISCONNECT` `SESSION_TAKEN_OVER` (`0x8E`); MQTT 3.1.1 has no such reason and
  is just closed;
* the displaced connection's **Will message is discarded** — the client has not
  died, it has moved, and publishing its Will would report a false disconnect to
  subscribers (a genuine drop still publishes it);
* a `cleanStart=false` connect with the same client id resumes the session: its
  subscriptions, unacknowledged QoS 1/2 messages and any inbound QoS 2 flow
  awaiting a `PUBREL` carry over to the new connection;
* a `cleanStart=true` connect starts a fresh session and drops the old state.

#### Will Delay Interval (MQTT 5)

A client may set a **Will Delay Interval** in its CONNECT Will properties. When
it does, an abnormal disconnect does not publish the Will immediately — the
broker holds it and publishes it only once the interval elapses (MQTT-3.1.4-6 /
5.4.3.2). This gives a client a window to reconnect and cancel the Will:

* the delayed Will is held by the broker (keyed by client id), so it survives the
  disconnect even of a non-persistent session, and is published by the same
  background sweeper that enforces session expiry;
* **reconnecting within the window cancels the Will** — any new `CONNECT` for the
  same client id drops a pending delayed Will before it fires, whether the session
  is resumed or replaced;
* a Will with no delay (or on MQTT 3.1.1) keeps the original behaviour: it is
  published immediately on an abnormal disconnect and discarded on a graceful one
  or a session takeover.

#### User Properties and other MQTT 5 properties

MQTT 5 lets any packet carry **User Properties** (arbitrary key/value pairs) and
other broker-directed properties. These are accepted on every packet type —
CONNECT, PUBLISH, SUBSCRIBE, DISCONNECT and the acknowledgement packets — so a
client that sets them is not rejected as malformed. User Properties are stored
per packet and are **not** forwarded to other clients (or written to durable
state); a human-readable `REASON_STRING` a client attaches to a DISCONNECT or ack
is likewise accepted and dropped.

#### System monitoring topics (`$SYS/broker`)

Setting `FLOWMQ_SYS_TOPICS` (to any value other than `0`/`false`) makes the
broker publish its own counters as a **monitoring topic tree** (MQTT-4.1.2).
This is opt-in: with the variable unset the broker exposes no `$SYS` topic and
`$SYS/...` traffic is treated as ordinary, user-defined traffic.

When enabled, the broker publishes the following **retained QoS 0** messages and
keeps them current on the normal sweep interval:

| Topic | Payload |
| --- | --- |
| `$SYS/broker/clients/connected` | number of currently connected clients (live gauge) |
| `$SYS/broker/clients/total` | number of sessions the broker holds |
| `$SYS/broker/subscriptions/count` | subscription instances routed, normal + shared |
| `$SYS/broker/inflight/count` | QoS 1/2 messages sent but not yet acknowledged (Receive Maximum occupancy) |
| `$SYS/broker/messages/sent` | total `PUBLISH`es delivered to subscribers |
| `$SYS/broker/messages/received` | total `PUBLISH`es accepted from clients |
| `$SYS/broker/messages/dropped` | total `PUBLISH`es dropped under backpressure |
| `$SYS/broker/retaineds/count` | number of retained messages held (excluding the tree) |
| `$SYS/broker/writebuffer/bytes` | live outbound socket write-buffer bytes (live gauge) |
| `$SYS/broker/queued/bytes` | live in-memory bytes of undelivered messages (live gauge) |
| `$SYS/broker/uptime` | seconds since the broker started |
| `$SYS/broker/load` | 1-minute load average |
| `$SYS/broker/disconnect/{keepalive,protocol,quota,auth,normal,other}` | connections ended, by cause |

The three backpressure gauges tell one story in sequence: `queued/bytes` rises
first (messages queued in-process for a slow consumer), then
`writebuffer/bytes` (buffered for the socket), and only after a real loss does
`messages/dropped` move. Watching them together shows pressure building *before*
anything is discarded.

A client reads them by subscribing to a `$SYS` filter, e.g. `$SYS/#`:

```
mosquitto_sub -t '$SYS/#' -v
```

Two rules apply (MQTT-4.7.2-1 / 4.1.2-2): a leading-level wildcard (`#`,
`+`) never matches a `$SYS` topic, so only an explicit `$SYS/...` subscription
reaches the tree; and while the feature is on, a client **publish** into the
tree is refused (QoS 1/2 get `NOT_AUTHORIZED`, QoS 0 a `DISCONNECT`), so only
the broker can write it.

A subscriber receives the current value once from the retained path and then
every subsequent change as a **live update** (with `RETAIN` cleared, as
MQTT-3.3.1-8 requires when forwarding), so the gauges move in a monitoring
client instead of freezing at subscribe time. The tree is never routed through
the ordinary publish path, so its own traffic does not count toward
`$SYS/broker/messages/sent`.

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License, Version 2.0 - see the LICENSE file for details.


## Code agents

developed by the following agent:

+ qoder Qwen4 Max
+ codebuddy Hy4 flash
+ zcode GLM 5.3 flash
+ claudecode Deepseek V4.1 flash
