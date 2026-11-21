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
    ("recvmax_window_test", "MQTT-4.9 Receive Maximum window (QoS 2 / QoS 1)"),
    ("websocket_framing_test", "WebSocket framing: cap-before-buffer + no fragmented control frames"),
    ("accept_backoff_test", "accept(): EMFILE/ENFILE backs off instead of spinning"),
    ("mqttsn_large_publish_test", "MQTT-SN §5.2.1 three-octet Length field"),
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
