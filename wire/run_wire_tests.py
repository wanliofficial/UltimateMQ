#!/usr/bin/env python3
"""Run the wire/ probe suite against a freshly built broker.

Each probe takes <binary> <port>; this runner picks an OS-assigned free port for
each probe so they can run back to back without colliding, then reports an
aggregate pass/fail.

Usage: run_wire_tests.py [build_dir]
Defaults to ./build relative to the repo root.
"""
import os
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
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind(("127.0.0.1", 0))
    port = s.getsockname()[1]
    s.close()
    return port


PROBES = [
    ("mqtt311_test", "MQTT 3.1.1: protocol level 4 round-trip, 0x80 SUBACK refusal, silent QoS 0 drop"),
    ("qos2_exactly_once_test", "QoS 2: full PUBLISH/PUBREC/PUBREL/PUBCOMP handshake delivers exactly once + a DUP retransmit gets PUBREC/SUCCESS (not re-routed)"),
    ("session_resume_test", "session resume: an unacked QoS 1 message is retransmitted with DUP on reconnect (sessionPresent=1), and a fresh session is not"),
    ("recvmax_window_test", "MQTT-4.9 Receive Maximum window (QoS 2 / QoS 1)"),
    ("websocket_framing_test", "WebSocket framing: cap-before-buffer + no fragmented control frames"),
    ("websocket_sys_topics_test", "WebSocket $SYS: monitoring tree reaches a subscriber + §4.1.2-2"),
    ("accept_backoff_test", "accept(): EMFILE/ENFILE backs off instead of spinning"),
    ("tls_handshake_timeout_test", "TLS handshake: a silent peer is dropped, not held forever"),
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
