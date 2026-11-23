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
| `mqtt311_test.py` | MQTT 3.1.1 (protocol level 4) over the wire, a path no other probe exercises: CONNECT/SUBSCRIBE/PUBLISH(QoS 0,1)/PUBACK round-trip with no property section; an invalid topic filter is reported as 0x80 in the SUBACK (not a reason code) and the connection stays open; a QoS 0 publish to an invalid topic is silently dropped without a DISCONNECT. |
| `retained_messages_test.py` | Core retained-message delivery on the wire: a RETAIN=1 publish reaches a brand-new subscriber with the RETAIN flag still set (§3.3.1-9); a live delivery to an already-subscribed client has RETAIN cleared (§3.3.1-8); a zero-length RETAIN=1 payload clears the retained message; a wildcard subscription receives every matching retained message. |
| `qos2_exactly_once_test.py` | End-to-end QoS 2 exactly-once over the wire (§4.3.3/§4.3.4): publisher and subscriber each complete the full PUBLISH/PUBREC/PUBREL/PUBCOMP handshake and the payload arrives exactly once; a DUP retransmit of an in-flight PUBLISH is answered PUBREC/SUCCESS and is not forwarded a second time. |
| `session_resume_test.py` | Session resume over a real disconnect (§4.6 / MQTT-4.4.0-1): a client that drops with an unacked QoS 1 message reconnects with Clean Start = 0, the broker reports sessionPresent = 1, and the message is retransmitted with the DUP flag; the new flow is completed with PUBACK. |
| `sys_topics_test.py` | `FLOWMQ_SYS_TOPICS`: the `$SYS` tree is delivered retained and then keeps updating live; MQTT-4.7.2-1 (a root `#` never reaches it), MQTT-4.1.2-2 (a client publish into it is refused `0x87`), and the feature being off by default. |
| `websocket_framing_test.py` | RFC 6455 §5.5.1 / §5.2: a fragmented control frame is refused with CLOSE 1002, and an oversized frame is refused with CLOSE 1009 before its payload is buffered. |
| `websocket_reject_response_test.py` | The non-WebSocket upgrade 400 reply: `Content-Length` equals the real body length (no off-by-one) and no WebSocket CLOSE frame is appended to the plain HTTP error response. |
| `websocket_handshake_deadline_test.py` | The HTTP upgrade uses one absolute deadline: a slowloris dribbling one byte every 2s is dropped at ~10s, not kept alive ~22h by a per-byte timeout; a prompt upgrade still gets 101. |
| `tls_handshake_timeout_test.py` | A TLS peer that opens the port and never sends its ClientHello is dropped after the handshake timeout instead of pinning a thread forever. |
| `tls_write_timeout_test.py` | A TLS client that stops reading is dropped once its stalled send exceeds `FLOWMQ_WRITE_TIMEOUT_MS` (SO_SNDTIMEO is set on the TLS path too, not just plaintext); a client that keeps reading is served, not dropped. |
| `env_bool_knob_test.py` | Boolean env knobs treat `=0`/`=false` as OFF, not "set, therefore on": `FLOWMQ_WS_REQUIRE_SUBPROTOCOL=0` still upgrades a client with no subprotocol, `=1` refuses it. |
| `websocket_sys_topics_test.py` | `$SYS/broker` monitoring tree over MQTT-over-WebSocket: the retained snapshot and live fan-out both reach a WebSocket subscriber (RETAIN cleared, MQTT-3.3.1-8), and a client PUBLISH into the tree is refused `0x87` (MQTT-4.1.2-2). |
| `mqttsn_large_publish_test.py` | MQTT-SN §5.2.1: a message of 256+ octets uses the three-octet Length field (first octet `0x01`, total counting the length field itself) and reaches an MQTT subscriber intact; a shorter one keeps the one-octet form. |
| `mqttsn_subscribe_topicid_test.py` | MQTT-SN gateway topic-id assignment (§5.3.1, §5.3.10): a subscribe-by-name returns a non-zero assigned id in the SUBACK, a publish carrying only that id is routed back to the topic name and reaches an MQTT subscriber, and re-subscribing by the same name reuses the id. |
| `mqttsn_register_topicid_test.py` | MQTT-SN REGISTER/REGACK (§5.3.8/§5.3.9): a valid topic name is assigned an id (echoed in the REGACK) that a later id-only PUBLISH uses to reach an MQTT subscriber; a wildcard topic name is refused with REGACK INVALID_TOPIC_ID and id 0. |
| `mqttsn_qos1_ping_will_test.py` | MQTT-SN gateway over UDP: a QoS 1 PUBLISH by a predefined topic id is bridged to an MQTT subscriber and answered with a PUBACK carrying the topic id, message id and ACCEPTED code (§5.3.6); a PINGREQ is answered with an empty PINGRESP; and a peer silent past 1.5× its keep-alive is reaped as an abnormal end that publishes its Will (§6.1). |
| `mqttsn_will_qos3_retained_test.py` | MQTT-SN §5.3.1: a Will declared with the raw QoS bits `3` ("QoS -1", fire-and-forget) is normalised to QoS 0 at the point it is read, exactly as the publish path already does. The Will fires on an abnormal end and is stored retained, and on a fresh process the same `FLOWMQ_STATE_FILE` restarts and serves it — proving the file was not poisoned by an out-of-range QoS that the Store parser (bounded by `QOS_2`) would otherwise reject and abort startup over. |
| `mqttsn_qos2_reject_test.py` | MQTT-SN §6.6: a QoS 2 PUBLISH that must be rejected (an unmapped NORMAL topic id) is answered with a **PUBACK carrying ReturnCode = INVALID_TOPIC_ID**, not a bare PUBREC byte-for-byte identical to a QoS 2 acceptance. The rejected payload never reaches a subscriber, even if the client completes PUBREL/PUBCOMP — so a client cannot believe a rejected message was delivered exactly-once. |
| `durable_state_restart_test.py` | `FLOWMQ_STATE_FILE` durability across a real restart: a graceful SIGTERM flushes the store, and on a fresh process the same file restores a retained message (delivered RETAIN=1) and a persistent session's subscription (Clean Start = 0 reports sessionPresent = 1 and routes a later publish without re-subscribing); a clean session is not persisted. |
| `will_delay_cancel_test.py` | MQTT 5 Will Delay (§3.1.3.2): a device that drops with a Will Delay and reconnects with the same client id **inside** the window has its pending Will cancelled (MQTT-3.1.4-6) and it is never published; the control case shows that with no reconnect the same delayed Will does fire after the delay (proving the cancellation is real, not vacuous). |
| `sys_will_bypass_test.py` | MQTT-4.1.2-2 with `FLOWMQ_SYS_TOPICS`: a client whose **Will** targets a `$SYS` topic cannot inject a message into the broker's monitoring tree (the PUBLISH-handler refusal used to be bypassed by the Will path, which reaches `Broker::publish()` directly), while ordinary fan-out and the genuine `$SYS` tree keep working. |
| `will_session_end_test.py` | MQTT 5 §3.1.2.11.3: a Will Delay is cut short when the **Session ends**. A persistent client arms a 6 s Will Delay, then sends DISCONNECT 0x04 (Disconnect with Will Message) carrying Session Expiry = 0, ending the session - the Will must be published immediately, not after 6 s. The control case keeps Session Expiry = 3600 (session survives) and shows the same delay is then honoured. |
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
