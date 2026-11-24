#!/usr/bin/env python3
"""Run the wire/ probe suite against a freshly built broker.

Each probe takes <binary> <port>; this runner picks an OS-assigned free port for
each probe so they can run back to back without colliding, then reports an
aggregate pass/fail.

Usage: run_wire_tests.py [build_dir]
Defaults to ./build relative to the repo root.
"""
import os
import random
import socket
import subprocess
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.dirname(HERE)
BUILD = sys.argv[1] if len(sys.argv) > 1 else os.path.join(REPO, "build")
if not os.path.isabs(BUILD):
    BUILD = os.path.join(REPO, BUILD)
BINARY = os.path.join(BUILD, "flowmq")


def free_port():
    """Return an unused TCP port that the broker can bind without racing an
    outgoing connection.

    bind(("127.0.0.1", 0)) hands back a port from the OS's ephemeral range
    (49152-65535 on macOS) - the very range the kernel also auto-assigns to
    *outgoing* connections. The runner closes its probe socket and passes the
    number to a subprocess that binds it a moment later; in that window an
    unrelated outgoing connection can take the port, so the broker's bind()
    fails with EADDRINUSE and exits (a probe then reports "server did not
    start" at random). Pick a port from below the ephemeral range instead and
    probe it for availability, so nothing on the machine is competing for it.
    """
    for _ in range(200):
        port = random.randint(20000, 45000)
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            s.bind(("127.0.0.1", port))
        except OSError:
            s.close()
            continue
        s.close()
        return port
    # Fall back to the kernel's pick if the band is somehow exhausted.
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


PROBES = [
    ("durable_state_restart_test", "durable state: retained messages and persistent sessions survive a broker restart (FLOWMQ_STATE_FILE)"),
    ("sys_will_bypass_test", "MQTT-4.1.2-2: a client Will on a $SYS topic cannot forge the monitoring tree"),
    ("will_delay_cancel_test", "MQTT 5 Will Delay: a reconnect inside the delay window cancels the pending Will (MQTT-3.1.4-6), while one with no reconnect does fire"),
    ("will_session_end_test", "MQTT 5 §3.1.2.11.3: ending the Session at DISCONNECT publishes an armed Will at once, not after its Will Delay"),
    ("mqttsn_qos1_ping_will_test", "MQTT-SN QoS 1 PUBACK round-trip, PINGREQ/PINGRESP, Will published on abnormal keep-alive disconnect"),
    ("mqttsn_will_qos3_retained_test", "MQTT-SN §5.3.1: a Will with a raw QoS of 3 (\"QoS -1\") is normalised to QoS 0, so a retained Will cannot poison FLOWMQ_STATE_FILE into a startup abort"),
    ("mqttsn_qos2_reject_test", "MQTT-SN §6.6: a QoS 2 PUBLISH that must be rejected (unknown topic id) is answered with a PUBACK carrying the ReturnCode, not a bare PUBREC indistinguishable from an acceptance, so the client never believes a rejected message was delivered exactly-once"),
    ("mqttsn_short_topic_test", "MQTT-SN §5.3.1: a SHORT-topic PUBLISH carrying wildcard bytes (\"++\") is rejected PUBACK+INVALID_TOPIC_ID, matching how a SHORT SUBSCRIBE is already refused"),
    ("nolocal_retained_test", "MQTT 5 No Local: a No Local=1 subscriber is not handed its own retained message back (MQTT-3.8.3-3)"),
    ("publish_missing_packetid_test", "MQTT-2.2.1-3: a QoS>0 PUBLISH with no Packet Identifier is a Protocol Error (DISCONNECT 0x82), not Malformed (0x81)"),
    ("utf8_invalid_string_test", "MQTT-1.5.4: an ill-formed UTF-8 Encoded String (lone continuation, encoded surrogate, overlong U+0000, truncated sequence) in the Client Id is refused with CONNACK 0x81 (MALFORMED_PACKET), while a well-formed multi-byte string is accepted"),
    ("connect_zero_property_test", "MQTT 3.1.2-10 / 3.1.2-12: a CONNECT whose Receive Maximum or Maximum Packet Size property is 0 is refused with CONNACK 0x82 (PROTOCOL_ERROR), while non-zero values connect normally"),
    ("subscribe_options_unsubscribe_test", "MQTT-3.8.4-1 / MQTT-3.11.3: a v5/3.1.1 SUBSCRIBE with reserved option bits set is malformed (DISCONNECT 0x81 / connection close), and an MQTT 5 UNSUBSCRIBE of a never-subscribed filter gets UNSUBACK 0x11 (NO_SUBSCRIPTION_EXISTED)"),
    ("publish_invalid_topic_test", "MQTT-3.3.2-1: a PUBLISH whose Topic Name carries a wildcard is refused with TOPIC_NAME_INVALID (0x90) in its PUBACK/PUBREC (or a DISCONNECT at QoS 0), and the message is never routed; a legal topic name is still accepted"),
    ("ack_reason_code_test", "MQTT 3.4.2.1 / 3.5.2.1 / 3.6.2.1 / 3.7.2.1: a v5 acknowledgement with an out-of-range Reason Code is refused DISCONNECT 0x81 (MALFORMED_PACKET), a 3.1.1 ack carrying any trailing byte closes the connection, and a legal reason code is accepted"),
    ("disconnect_will_test", "MQTT-3.1.2-8 / §3.14.2.2.2: a DISCONNECT 0x00 (Normal) deletes the Will, 0x04 (Disconnect with Will Message) still publishes it, and a DISCONNECT raising a zero CONNECT Session Expiry Interval to non-zero is refused with DISCONNECT 0x82"),
    ("publish_property_forward_test", "MQTT-3.3.2-4 / 3.3.2-8: a forwarded PUBLISH carries Response Topic, Correlation Data, Content Type, Payload Format Indicator and User Property unchanged, while a per-connection Topic Alias is not forwarded"),
    ("retain_handling_test", "MQTT 3.8.3-1: SUBSCRIBE Retain Handling 0 sends retained on subscribe, 2 suppresses it entirely, and 1 sends it on a new subscription but not on a re-subscribe of an established one"),
    ("publish_subscription_id_test", "MQTT-3.3.4-6: a client PUBLISH carrying a Subscription Identifier (a server-to-subscriber property) is refused with DISCONNECT 0x82 (PROTOCOL_ERROR), while a clean PUBLISH is still accepted"),
    ("keepalive_timeout_test", "MQTT-3.1.2-24 / 3.1.2-20: a client silent past 1.5x Keep Alive is closed with DISCONNECT 0x8D and its Will is published, while a PINGREQ inside every window resets the timer and keeps the connection open"),
    ("subscription_identifier_outbound_test", "MQTT-3.3.4-3 / 3.3.4-4: a subscription's Subscription Identifier (multi-byte VBI) is included on the forwarded PUBLISH, one combined copy carries the identifiers of every matching subscription, and a subscription without one adds none"),
    ("shared_subscription_roundrobin_test", "MQTT-4.8.2: a shared group delivers each message to exactly one member and spreads the load across members, while two different share names are independent groups each receiving every message once"),
    ("message_expiry_test", "MQTT-3.3.2-9 / 3.3.1-11: a forwarded PUBLISH carries the remaining (counted-down) Message Expiry Interval, a retained message whose expiry has elapsed is not delivered to a new subscriber, and a retained message without expiry still is"),
    ("maximum_packet_size_outbound_test", "MQTT-3.1.2-24: a PUBLISH whose serialized size exceeds the client's declared Maximum Packet Size is dropped for that subscriber (while a small one still arrives on the same connection), and an uncapped subscriber receives the same large PUBLISH intact"),
    ("zero_client_id_test", "MQTT-3.1.3-6/-7/-8: a v5 empty Client Id with Clean Start is assigned one and returned as the Assigned Client Identifier (0x12), a v5 empty id without Clean Start is refused with CONNACK 0x85, and an MQTT 3.1.1 empty id without CleanSession is refused with CONNACK 0x02"),
    ("connack_topic_alias_max_test", "MQTT-3.1.2.9.6 / §3.3.2.3.4: a successful CONNACK carries a positive Topic Alias Maximum property, and that advertised value is the exact enforcement boundary -- an alias at the maximum is accepted and routed while one above it is refused with DISCONNECT 0x94"),
    ("pubrel_dup_bit_test", "MQTT-3.6.1-1: a PUBREL re-sent after reconnect keeps the reserved bits 0010 (0x62), never a DUP bit (0x6A)"),
    ("receive_maximum_resume_test", "MQTT-4.9.0-2: a resumed session is not sent more than Receive Maximum unacked QoS>0 PUBLISHes, even when buffered offline under the default window"),
    ("env_limit_overflow_test", "config: an out-of-range FLOWMQ_* resource limit (strtoull ERANGE) is rejected at startup instead of silently becoming ULLONG_MAX and removing the cap"),
    ("mqttsn_topicid_exhaustion_test", "MQTT-SN §5.3.9: exhausting a peer's 16-bit topic-id space answers REGISTER with REJECTED_CONGESTION instead of spinning forever in the broker event loop (whole-broker DoS)"),
    ("subscribe_nested_share_test", "MQTT-4.8.2: a deeply nested \\\"$share/a/$share/a/...\\\" SUBSCRIBE filter is answered instead of overflowing the connection thread's stack (remote DoS)"),
    ("trie_deep_match_test", "MQTT-4.7: Trie::match walks the routing trie per topic level; a ~40 KB topic with 20000 levels overflowed the connection-thread stack and killed the broker on an ordinary SUBSCRIBE+PUBLISH (remote DoS)"),
    ("retained_messages_test", "retained: snapshot-to-new-subscriber with RETAIN=1, live delivery clears RETAIN, empty payload clears, wildcard match"),
    ("mqtt311_test", "MQTT 3.1.1: protocol level 4 round-trip, 0x80 SUBACK refusal, silent QoS 0 drop"),
    ("qos2_exactly_once_test", "QoS 2: full PUBLISH/PUBREC/PUBREL/PUBCOMP handshake delivers exactly once + a DUP retransmit gets PUBREC/SUCCESS (not re-routed)"),
    ("qos2_unknown_id_test", "MQTT-4.3.3-5: a PUBREL/PUBREC for an identifier that started no QoS 2 flow is answered PUBCOMP/PUBREL 0x92 (PACKET_IDENTIFIER_NOT_FOUND), not silently accepted"),
    ("session_resume_test", "session resume: an unacked QoS 1 message is retransmitted with DUP on reconnect (sessionPresent=1), and a fresh session is not"),
    ("recvmax_window_test", "MQTT-4.9 Receive Maximum window (QoS 2 / QoS 1)"),
    ("websocket_framing_test", "WebSocket framing: cap-before-buffer + no fragmented control frames"),
    ("websocket_upgrade_token_test", "WebSocket upgrade: the |Upgrade| token is matched exactly, not as a substring"),
    ("websocket_length_msb_test", "WebSocket: a 64-bit frame length with the MSB set is rejected with CLOSE 1002, not 1009"),
    ("websocket_close_echo_test", "WebSocket CLOSE: the reply echoes the peer's status code (and none when it sent none)"),
    ("websocket_reject_response_test", "WebSocket reject: clean 400 (Content-Length exact) and no CLOSE frame appended"),
    ("websocket_handshake_deadline_test", "WebSocket upgrade: one absolute deadline drops a slowloris (not per-byte)"),
    ("websocket_write_buffer_test", "WebSocket: FLOWMQ_WRITE_BUFFER buffers/ serves a slow reader (writeSome non-blocking)"),
    ("env_bool_knob_test", "env bool knobs: =0/=false mean OFF, not 'set, therefore on'"),
    ("websocket_sys_topics_test", "WebSocket $SYS: monitoring tree reaches a subscriber + §4.1.2-2"),
    ("accept_backoff_test", "accept(): EMFILE/ENFILE backs off instead of spinning"),
    ("tls_handshake_timeout_test", "TLS handshake: a silent peer is dropped, not held forever"),
    ("tls_write_timeout_test", "TLS write: a client that stops reading is dropped for its stalled write"),
    ("tls_mtls_without_cert_test", "TLS: VERIFY_CLIENTS/CA without a certificate is refused, not plaintext"),
    ("tls_cert_chain_test", "TLS: a leaf+intermediate PEM bundle is sent as a chain"),
    ("write_timeout_range_test", "FLOWMQ_WRITE_TIMEOUT_MS above INT_MAX is refused, not truncated"),
    ("mqttsn_large_publish_test", "MQTT-SN §5.2.1 three-octet Length field"),
    ("mqttsn_subscribe_topicid_test", "MQTT-SN gateway: subscribe-by-name assigns a topic id, publish-by-id is routed back to the name, id is reused on re-subscribe"),
    ("mqttsn_register_topicid_test", "MQTT-SN REGISTER/REGACK (§5.3.8/§5.3.9): a valid name is assigned an id a publish-by-id uses, a wildcard topic name is refused with INVALID_TOPIC_ID"),
    ("write_buffer_test", "FLOWMQ_WRITE_BUFFER: a slow reader is buffered, not dropped"),
    ("sys_topics_test", "FLOWMQ_SYS_TOPICS: $SYS tree live updates, §4.7.2-1, §4.1.2-2, off by default"),
    ("topic_alias_test", "MQTT 5 Topic Alias: register/reuse over a connection + §3.3.2.7/8, §3.3.2.3.4, OASIS MQTT-474 refusals (0x82/0x94)"),
]


def main():
    if not os.path.exists(BINARY):
        print("broker binary not found at %s; build it first "
              "(cmake --build build -j)" % BINARY)
        return 2
    failures = 0
    for script, desc in PROBES:
        port = free_port()
        print("== %s (%s) ==" % (script, desc))
        rc = subprocess.call([sys.executable, os.path.join(HERE, script + ".py"), BINARY, str(port)])
        if rc != 0:
            failures += 1
    print("\nWIRE SUITE: %d/%d probes passed" % (len(PROBES) - failures, len(PROBES)))
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
