# Wire tests

End-to-end MQTT conformance probes. Unlike the gtest suite under `tests/`,
these launch the real `flowmq` broker binary and drive it over TCP, exercising
the full path: packet parsing, session state, flow control and the socket
write loop.

Each probe is a self-contained Python script that takes `<binary> <port>` and
exits non-zero if any case fails.

## Running

```bash
cmake --build build -j                 # build the broker
python3 wire/run_wire_tests.py         # run all probes (auto-picks free ports)
```

Or run one directly (the broker takes its port from `MQTT_PORT`):

```bash
python3 wire/recvmax_window_test.py build/flowmq 18840
```

## Probes

| Script | Covers |
|---|---|
| `recvmax_window_test.py` | MQTT-4.9 / §3.1.2.11.9: the Receive Maximum window holds a QoS 2 message until PUBCOMP (not PUBREC), and at QoS 1 until PUBACK. |
| `sys_topics_test.py` | `FLOWMQ_SYS_TOPICS`: the `$SYS` tree is delivered retained and then keeps updating live; MQTT-4.7.2-1 (a root `#` never reaches it), MQTT-4.1.2-2 (a client publish into it is refused `0x87`), and the feature being off by default. |
| `websocket_framing_test.py` | RFC 6455 §5.5.1 / §5.2: a fragmented control frame is refused with CLOSE 1002, and an oversized frame is refused with CLOSE 1009 before its payload is buffered. |
| `websocket_sys_topics_test.py` | `$SYS/broker` monitoring tree over MQTT-over-WebSocket: the retained snapshot and live fan-out both reach a WebSocket subscriber (RETAIN cleared, MQTT-3.3.1-8), and a client PUBLISH into the tree is refused `0x87` (MQTT-4.1.2-2). |
| `mqttsn_large_publish_test.py` | MQTT-SN §5.2.1: a message of 256+ octets uses the three-octet Length field (first octet `0x01`, total counting the length field itself) and reaches an MQTT subscriber intact; a shorter one keeps the one-octet form. |
| `mqttsn_subscribe_topicid_test.py` | MQTT-SN gateway topic-id assignment (§5.3.1, §5.3.10): a subscribe-by-name returns a non-zero assigned id in the SUBACK, a publish carrying only that id is routed back to the topic name and reaches an MQTT subscriber, and re-subscribing by the same name reuses the id. |
| `topic_alias_test.py` | MQTT 5 Topic Alias over a real connection (§3.3.2.7/8, §3.3.2.3.4, OASIS MQTT-474): a named PUBLISH registers an alias and an alias-only PUBLISH reuses it; an unmapped alias or a zero-length Topic Name is refused `0x82`, and alias 0 or an alias above the advertised maximum `0x94`. |

MQTT-SN probes are UDP: they set `FLOWMQ_MQTT_SN_PORT` to a port derived from
the TCP port the runner passes, so several probes can run back to back.

## Adding a probe

1. Drop a new `<name>_test.py` here following the `recvmax_window_test.py`
   template: start the broker in a `tempfile.mkdtemp` workdir with an isolated
   `FLOWMQ_STATE_FILE`, drive it over a socket, and `stop()` in a `finally`.
2. Register it in `PROBES` in `run_wire_tests.py`.
3. Cite the exact MQTT clause the assertion pins down in the module docstring.

A probe that fails first because of a wrong test assertion (not a broker bug)
should be fixed like any other test; a probe that exposes a real defect should
be reported before the broker is changed.
