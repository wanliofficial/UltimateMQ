#!/usr/bin/env python3
"""FLOWMQ_* resource limits must reject an out-of-range value, not silently
become "unlimited".

The size limits are parsed with strtoull, which reports an overflow (a value
larger than it can represent, e.g. a mistyped "effectively unlimited" 20-digit
number) by returning ULLONG_MAX and setting errno=ERANGE -- it does NOT clamp.
The old parse only rejected a leading '-', non-numeric input, trailing junk and
zero, so an out-of-range FLOWMQ_MAX_CONNECTIONS (or MAX_PACKET_SIZE,
MAX_SUBSCRIPTIONS, MAX_RETAINED, ...) passed validation and stored ULLONG_MAX,
silently removing the resource-exhaustion cap the knob exists to provide. The
broker is supposed to fail loudly on a bad config value instead.

The test sets each limit to a value above ULLONG_MAX and asserts the broker
refuses to start (non-zero exit). A control confirms a sane in-range value still
starts the broker.

Usage: env_limit_overflow_test.py <binary>
Exit code is 0 only when every case passes.
"""
import os
import signal
import socket
import subprocess
import sys
import time

BIN = sys.argv[1]

# A value larger than ULLONG_MAX (18446744073709551615): 20 nines.
OVERFLOW = "9" * 25

# (env var, an in-range value that must still start the broker)
LIMITS = [
    ("FLOWMQ_MAX_CONNECTIONS", "100"),
    ("FLOWMQ_MAX_PACKET_SIZE", "4096"),
    ("FLOWMQ_WRITE_BUFFER", "1024"),
    ("FLOWMQ_MAX_SUBSCRIPTIONS", "50"),
    ("FLOWMQ_MAX_SHARED_SUBSCRIPTIONS", "50"),
    ("FLOWMQ_MAX_RETAINED", "100"),
    ("FLOWMQ_MAX_PUBLISH_RATE", "1000"),
    ("FLOWMQ_SERVER_KEEP_ALIVE", "60"),
]

results = []


def start_with_env(env_overrides, expect_start):
    # Pick a free port the broker can actually bind (MQTT_PORT=0 is rejected).
    free = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    free.bind(("127.0.0.1", 0))
    port = free.getsockname()[1]
    free.close()
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    for k, v in env_overrides.items():
        env[k] = v
    try:
        proc = subprocess.Popen([BIN], env=env, cwd=os.path.dirname(BIN),
                                stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    except Exception as exc:  # noqa: BLE001
        return False, "failed to launch: %r" % (exc,)
    # Give it a moment to either start listening or exit with an error.
    try:
        rc = proc.wait(timeout=3)
        started = False
    except subprocess.TimeoutExpired:
        started = True
        proc.send_signal(signal.SIGTERM)
        try:
            proc.wait(timeout=3)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()
    if expect_start:
        return started, ("broker started" if started else "broker exited rc=%s" % rc)
    else:
        return (not started), ("broker refused to start" if not started
                               else "broker started despite the bad limit")


def case(name, fn):
    try:
        info = fn()
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
        return
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()
        return
    results.append((name, True))
    print("PASS " + name + ((" :: " + info) if info else ""))


def make_overflow_case(var):
    def run():
        ok, info = start_with_env({var: OVERFLOW}, expect_start=False)
        assert ok, "%s=%s should have been rejected: %s" % (var, OVERFLOW, info)
        return "%s=%s rejected at startup" % (var, OVERFLOW)
    return run


def make_control_case(var, good):
    def run():
        ok, info = start_with_env({var: good}, expect_start=True)
        assert ok, "%s=%s should have started the broker: %s" % (var, good, info)
        return "%s=%s still starts the broker" % (var, good)
    return run


for var, good in LIMITS:
    case("%s rejects an out-of-range value instead of silently becoming unlimited" % var,
         make_overflow_case(var))
for var, good in LIMITS:
    case("%s (control): an in-range value still starts the broker" % var,
         make_control_case(var, good))

failed = [n for n, ok in results if not ok]
print("\nSUMMARY: %d/%d passed" % (len(results) - len(failed), len(results)))
sys.exit(1 if failed else 0)
