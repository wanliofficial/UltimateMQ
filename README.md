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

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License, Version 2.0 - see the LICENSE file for details.
