# UltimateMQ

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
| `FLOWMQ_MAX_PUBLISH_RATE` | unlimited | Per-client inbound `PUBLISH` rate in messages/second; a publisher that exceeds it is disconnected |
| `FLOWMQ_SERVER_KEEP_ALIVE` | unset | Preferred keep alive in seconds; MQTT 5 clients asking for more (or for none) are told this value in CONNACK |
| `FLOWMQ_MAX_CONNECTIONS` | `1024` | Maximum number of simultaneous client connections; further connections are accepted then closed |
| `FLOWMQ_TLS_CERT` | unset | Server certificate (PEM); enables TLS when set together with `FLOWMQ_TLS_KEY` |
| `FLOWMQ_TLS_KEY` | unset | Private key (PEM) matching `FLOWMQ_TLS_CERT` |
| `FLOWMQ_TLS_CA` | unset | CA bundle used to verify client certificates |
| `FLOWMQ_TLS_VERIFY_CLIENTS` | unset | When set, a client certificate signed by `FLOWMQ_TLS_CA` is required |

When neither file is set the broker stays fully open (every client may
connect and use every topic), which keeps the historical behaviour.#### Users (`FLOWMQ_PASSWORD_FILE`)

One entry per line, `#` starts a comment:

```
# plaintext
alice:secret
# pre-hashed: $sha256$<salt-hex>:<sha256(salt + password)>
bob:$sha256$a1b2c3d4:9f2c...c41d
```

Once a users file is loaded every client must send a username and password;
a missing or wrong credential is answered with CONNACK `0x86` (bad user name
or password, MQTT 5) / `0x04`-style failure for 3.1.1 and the socket is closed.

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
* If an ACL file is loaded, anything not explicitly granted is denied. A
  denied SUBSCRIBE returns SUBACK `0x87` (MQTT 5) or `0x80` (3.1.1); a denied
  PUBLISH returns PUBACK/PUBREC `0x87` and the message is not routed; a denied
  QoS 0 PUBLISH disconnects an MQTT 5 client and is dropped for 3.1.1.
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
* an `AUTH` packet is answered with `AUTH` `0x8C`
  (`BAD_AUTHENTICATION_METHOD`) and the connection is closed, as MQTT-4.12.1-1
  requires once no method has been agreed.

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
so a crash never leaves a half-written state file behind. A state file with an
unrecognised header aborts startup instead of silently discarding sessions.

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

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License, Version 2.0 - see the LICENSE file for details.
