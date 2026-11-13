# UltimateMQ

UltimateMQ is an experimental MQTT broker written in C++ with cursor.

## Prerequisites

- C++ compiler with C++17 support
- CMake 3.10 or higher

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

When neither file is set the broker stays fully open (every client may
connect and use every topic), which keeps the historical behaviour.

#### Users (`FLOWMQ_PASSWORD_FILE`)

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

## Contributing

Please read CONTRIBUTING.md for details on our code of conduct and the process for submitting pull requests.

## License

This project is licensed under the Apache License, Version 2.0 - see the LICENSE file for details.
