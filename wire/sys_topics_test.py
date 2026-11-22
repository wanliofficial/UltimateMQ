#!/usr/bin/env python3
"""$SYS/broker monitoring tree over a real connection.

The $SYS tree is opt-in (FLOWMQ_SYS_TOPICS). When on, the broker publishes its
own counters as retained QoS 0 messages and refuses client writes to the tree
(MQTT-4.1.2-2); when off, it exposes no $SYS topic at all and a client publish
to "$SYS/..." is ordinary traffic. The gtest suite exercises the tree through
the library API only, so this probe pins the end-to-end behaviour that an
operator actually relies on:

  1. "$SYS/#" reaches every advertised topic, each with a decimal payload and
     the RETAIN flag set (they are delivered through the retained-message path,
     MQTT-3.3.1-8 / MQTT-3.3.1-2).
  2. A root-level wildcard ("#") does NOT reach the tree: a filter whose first
     level is a wildcard never matches a topic starting with '$'
     (MQTT-4.7.2-1). The tree is therefore opt-in from both ends.
  3. A client PUBLISH to a $SYS topic is refused (NOT_AUTHORIZED, 0x87) and the
     broker's value is unchanged -- only the broker may write the tree.
  4. With the feature off, "$SYS/#" yields nothing and a client publish to
     "$SYS/..." is accepted as ordinary traffic: the default is unchanged.

Usage: sys_topics_test.py <binary> <port>
Exit code is 0 only when every case passes.
"""
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import time

HOST = "127.0.0.1"
BIN = sys.argv[1]
PORT = [int(sys.argv[2])]


def enc_len(n):
    out = b""
    while True:
        d = n % 128
        n //= 128
        if n:
            d |= 0x80
        out += bytes([d])
        if not n:
            return out


def utf8(x):
    if isinstance(x, str):
        x = x.encode()
    return struct.pack("!H", len(x)) + x


def connect(cid, clean_start=True):
    flags = 0x02 if clean_start else 0x00
    body = utf8("MQTT") + bytes([5, flags]) + struct.pack("!H", 60) + enc_len(0) + utf8(cid)
    return bytes([0x10]) + enc_len(len(body)) + body


def subscribe(pid, filt, qos=0):
    body = struct.pack("!H", pid) + enc_len(0) + utf8(filt) + bytes([qos])
    return bytes([0x82]) + enc_len(len(body)) + body


def publish(topic, payload, qos=0, pid=1):
    body = utf8(topic)
    if qos:
        body += struct.pack("!H", pid)
    body += enc_len(0) + payload
    header = 0x30 | (qos << 1)
    return bytes([header]) + enc_len(len(body)) + body


def read_pkt(c, timeout=1.5):
    c.settimeout(timeout)
    try:
        h = c.recv(1)
    except socket.timeout:
        return ("timeout",)
    except Exception as exc:  # noqa: BLE001
        return ("err", repr(exc))
    if not h:
        return ("eof",)
    n, mult = 0, 1
    while True:
        try:
            b = c.recv(1)
        except socket.timeout:
            return ("timeout",)
        if not b:
            return ("eof",)
        b = b[0]
        n += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    data = b""
    while len(data) < n:
        chunk = c.recv(n - len(data))
        if not chunk:
            return ("eof",)
        data += chunk
    return ("pkt", h[0], data)


def parse_publish(header, body):
    """Return (topic, qos, retain, payload) for a PUBLISH packet body."""
    qos = (header >> 1) & 0x03
    retain = header & 0x01
    tlen = struct.unpack("!H", body[0:2])[0]
    topic = body[2:2 + tlen].decode()
    off = 2 + tlen
    if qos:
        off += 2  # Packet Identifier
    # Properties: Variable Byte Integer length, then that many bytes.
    plen, mult = 0, 1
    while True:
        b = body[off]
        off += 1
        plen += (b & 127) * mult
        mult *= 128
        if not b & 128:
            break
    off += plen
    return topic, qos, retain, body[off:]


def collect_publishes(c, budget, idle=1.0):
    """Read PUBLISH packets until `budget` are seen or the connection goes idle.

    Returns {(topic, retain): (qos, retain, payload)} keyed so both the retained
    snapshot (retain=1) and live updates (retain=0) for the same topic can be
    captured and told apart.
    """
    got = {}
    for _ in range(budget):
        r = read_pkt(c, idle)
        if r[0] != "pkt" or (r[1] & 0xF0) != 0x30:
            break
        topic, qos, retain, payload = parse_publish(r[1], r[2])
        got[(topic, retain)] = (qos, retain, payload)
    return got


def snapshot_of(got, topic):
    """The retained snapshot for `topic` (retain=1), if it was delivered."""
    return got.get((topic, 1))


def live_values(got, topic):
    """Every live update for `topic` (retain=0), in arrival order."""
    return [v for (t, retain), v in got.items() if t == topic and retain == 0]


def wait_for_live_update(c, topic, idle=2.5):
    """Read until a live (retain=0) PUBLISH on `topic` arrives, returning it.

    A $SYS value is written to the retained set on the 1s sweep, so the value a
    subscriber sees on first delivery can be up to one sweep old. An operator
    needs the tree to keep moving, so this waits for the next live update rather
    than trusting the snapshot.
    """
    deadline = time.time() + 6.0
    while time.time() < deadline:
        r = read_pkt(c, idle)
        if r[0] != "pkt" or (r[1] & 0xF0) != 0x30:
            continue
        t, qos, retain, payload = parse_publish(r[1], r[2])
        if t == topic and retain == 0:
            return (qos, retain, payload)
    raise AssertionError("no live update on %s within the deadline" % topic)


def start(sys_topics):
    PORT[0] += 1
    port = PORT[0]
    workdir = tempfile.mkdtemp(prefix="flowmq-sys-")
    env = dict(os.environ)
    env["MQTT_PORT"] = str(port)
    env["FLOWMQ_STATE_FILE"] = os.path.join(workdir, "state.db")
    if sys_topics:
        env["FLOWMQ_SYS_TOPICS"] = "1"
    else:
        env.pop("FLOWMQ_SYS_TOPICS", None)
    proc = subprocess.Popen([BIN], env=env, cwd=workdir,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    for _ in range(100):
        try:
            c = socket.create_connection((HOST, port), timeout=0.2)
            c.close()
            time.sleep(0.3)
            return proc, port, workdir
        except Exception:  # noqa: BLE001
            time.sleep(0.05)
    proc.kill()
    raise RuntimeError("server did not start")


def stop(proc, workdir):
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    time.sleep(0.6)


def new_client(port, cid, clean_start=True):
    c = socket.create_connection((HOST, port), timeout=5)
    c.sendall(connect(cid, clean_start))
    r = read_pkt(c)
    assert r[0] == "pkt" and r[1] & 0xF0 == 0x20, "no CONNACK for %s" % cid
    return c


results = []


def case(name, fn):
    try:
        info = fn()
        results.append((name, True))
        print("PASS " + name + ((" :: " + info) if info else ""))
    except AssertionError as exc:
        results.append((name, False))
        print("FAIL " + name + " :: assert: %s" % exc)
    except Exception as exc:  # noqa: BLE001
        import traceback
        results.append((name, False))
        print("FAIL " + name + " :: %s: %r" % (type(exc).__name__, exc))
        traceback.print_exc()


def case_tree_is_delivered_retained():
    """A "$SYS/#" subscription receives the whole tree: every topic carries a
    decimal payload and the RETAIN flag, because the tree is published as
    retained messages and replayed on subscribe (MQTT-3.3.1-2)."""
    proc, port, workdir = start(sys_topics=True)
    try:
        c = new_client(port, "sys-sub")
        c.sendall(subscribe(1, "$SYS/#"))
        r = read_pkt(c)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        got = collect_publishes(c, 64)
        assert got, "a $SYS/# subscription received no monitoring topics"

        # Every value must be a decimal number (all current topics are counts,
        # uptime or load average). The first delivery of each is the retained
        # snapshot, so it must carry RETAIN=1 (MQTT-3.3.1-2).
        snapshots = {}
        for (topic, retain), (qos, _, payload) in got.items():
            assert qos == 0, "%s delivered at QoS %d, expected 0" % (topic, qos)
            text = payload.decode()
            assert text, "%s had an empty payload" % topic
            assert text.replace(".", "", 1).isdigit(), (
                "%s payload is not a decimal number: %r" % (topic, text))
            if retain == 1:
                snapshots[topic] = text
        assert snapshots, "no topic was delivered with the RETAIN flag set"

        # The core topics an operator subscribes for must all be present.
        for topic in ("$SYS/broker/clients/connected",
                      "$SYS/broker/clients/total",
                      "$SYS/broker/subscriptions/count",
                      "$SYS/broker/inflight/count",
                      "$SYS/broker/messages/sent",
                      "$SYS/broker/messages/received",
                      "$SYS/broker/messages/dropped",
                      "$SYS/broker/queued/bytes",
                      "$SYS/broker/writebuffer/bytes",
                      "$SYS/broker/retaineds/count",
                      "$SYS/broker/uptime",
                      "$SYS/broker/disconnect/keepalive"):
            assert topic in snapshots, "%s missing from the tree (got %r)" % (
                topic, sorted(snapshots))
        c.close()
        return "%d topics retained, decimal" % len(snapshots)
    finally:
        stop(proc, workdir)


def case_tree_keeps_updating_after_subscribe():
    """A $SYS subscription must keep receiving updates as the gauges move, not
    just the one snapshot delivered at subscribe time. Uptime advances every
    second, so a live subscriber has to see the value climb; a snapshot-only tree
    would show the same number forever, which is useless for watching
    backpressure or in-flight pressure build. Live updates forward with RETAIN=0
    (MQTT-3.3.1-8), unlike the retained snapshot."""
    proc, port, workdir = start(sys_topics=True)
    try:
        c = new_client(port, "live-sub")
        c.sendall(subscribe(1, "$SYS/broker/uptime"))
        r = read_pkt(c)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        # First read the retained snapshot (RETAIN=1), then require live updates.
        snap = None
        deadline = time.time() + 3.0
        while time.time() < deadline and snap is None:
            r = read_pkt(c, 1.0)
            if r[0] == "pkt" and (r[1] & 0xF0) == 0x30:
                t, _, retain, payload = parse_publish(r[1], r[2])
                if retain == 1:
                    snap = int(payload)
        assert snap is not None, "no retained uptime snapshot on subscribe"

        first = wait_for_live_update(c, "$SYS/broker/uptime")
        assert first[1] == 0, "a live update must clear RETAIN (MQTT-3.3.1-8)"
        first_value = int(first[2])
        assert first_value >= snap, (
            "live uptime %d went backwards from the snapshot %d"
            % (first_value, snap))

        # A second live update proves the tree keeps moving, not just one flush.
        second = wait_for_live_update(c, "$SYS/broker/uptime")
        assert int(second[2]) > first_value, (
            "uptime did not advance across live updates: %s then %s"
            % (first[2].decode(), second[2].decode()))

        c.close()
        return "snapshot=%d then live %d -> %d" % (
            snap, first_value, int(second[2]))
    finally:
        stop(proc, workdir)


def case_root_wildcard_does_not_reach_the_tree():
    """MQTT-4.7.2-1: a filter whose first level is a wildcard never matches a
    topic starting with '$'. A "#" subscriber must therefore see none of the
    tree, so monitoring topics never leak into ordinary application traffic."""
    proc, port, workdir = start(sys_topics=True)
    try:
        c = new_client(port, "root-sub")
        c.sendall(subscribe(1, "#"))
        r = read_pkt(c)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        got = collect_publishes(c, 4, idle=0.8)
        leaked = [topic for (topic, _) in got if topic.startswith("$")]
        assert not leaked, (
            "# matched $SYS topics %r, violating MQTT-4.7.2-1" % leaked)
        c.close()
        return "# saw no $SYS topic"
    finally:
        stop(proc, workdir)


def case_single_topic_is_readable():
    """An operator subscribes to one topic, not the wildcard: the exact name must
    resolve to a live count (here: at least the subscribing client itself). A
    $SYS value is refreshed on the 1s sweep, so the snapshot may lag; keep reading
    until a live update confirms the current value."""
    proc, port, workdir = start(sys_topics=True)
    try:
        c = new_client(port, "one-sub")
        c.sendall(subscribe(1, "$SYS/broker/clients/connected"))
        r = read_pkt(c)
        assert r[0] == "pkt" and r[1] & 0xF0 == 0x90, "no SUBACK"

        got = collect_publishes(c, 1)
        assert snapshot_of(got, "$SYS/broker/clients/connected") is not None, (
            "the exact topic was not delivered: %r" % (got,))

        # The snapshot can be one sweep old (it was written before this client
        # connected); a live update is the authoritative current value.
        _, _, payload = wait_for_live_update(c, "$SYS/broker/clients/connected")
        assert int(payload) >= 1, (
            "connected count is %r, but at least this client is connected"
            % payload.decode())
        c.close()
        return "connected=%s (live)" % payload.decode()
    finally:
        stop(proc, workdir)


def case_client_publish_is_refused():
    """MQTT-4.1.2-2: while the tree is enabled a client must not write it. A QoS 1
    PUBLISH to a $SYS topic is refused with NOT_AUTHORIZED (0x87) in its PUBACK.
    The broker's own value is unaffected: after the spoof attempt a live update
    still reports a value the broker computed (never the spoofed one)."""
    proc, port, workdir = start(sys_topics=True)
    try:
        reader = new_client(port, "ref-reader")
        reader.sendall(subscribe(1, "$SYS/broker/uptime"))
        assert read_pkt(reader)[1] & 0xF0 == 0x90, "no SUBACK"
        snap = collect_publishes(reader, 1)
        assert snapshot_of(snap, "$SYS/broker/uptime") is not None, "no uptime topic"

        spoofer = new_client(port, "ref-spoofer")
        spoofer.sendall(publish("$SYS/broker/uptime", b"999999", qos=1, pid=7))
        r = read_pkt(spoofer)
        assert r[0] == "pkt" and (r[1] & 0xF0) == 0x40, (
            "no PUBACK for the $SYS publish: %r" % (r,))
        pid = struct.unpack("!H", r[2][0:2])[0]
        reason = r[2][2] if len(r[2]) > 2 else 0x00
        assert pid == 7, "PUBACK carried the wrong Packet Identifier"
        assert reason == 0x87, (
            "a client $SYS publish was not refused with NOT_AUTHORIZED (0x87); "
            "reason was 0x%02x" % reason)

        # The broker's own value must still be its own: a live update arrives on
        # the next sweep with a real uptime, never the spoofed 999999.
        _, _, payload = wait_for_live_update(reader, "$SYS/broker/uptime")
        value = payload.decode()
        assert value != "999999", "a client write truncated the broker's uptime topic"
        assert int(value) > 0, "uptime is not monotonic: %r" % value

        reader.close()
        spoofer.close()
        return "refused 0x87, value still %s" % value
    finally:
        stop(proc, workdir)


def case_feature_off_changes_nothing():
    """With FLOWMQ_SYS_TOPICS unset the broker exposes no $SYS topic, and a client
    publish to "$SYS/..." is ordinary traffic accepted and routed normally: the
    default path is byte-for-byte the historical behaviour."""
    proc, port, workdir = start(sys_topics=False)
    try:
        sub = new_client(port, "off-sub")
        sub.sendall(subscribe(1, "$SYS/#"))
        assert read_pkt(sub)[1] & 0xF0 == 0x90, "no SUBACK"
        got = collect_publishes(sub, 3, idle=0.6)
        assert not got, (
            "the $SYS tree was delivered with the feature off: %r" % sorted(got))
        # A client publish under $SYS is ordinary traffic: it must be accepted
        # (normal PUBACK, reason SUCCESS) and routed to a matching subscriber.
        app = new_client(port, "off-app")
        app.sendall(subscribe(1, "$SYS/custom/#"))
        assert read_pkt(app)[1] & 0xF0 == 0x90, "no SUBACK"

        pub = new_client(port, "off-pub")
        pub.sendall(publish("$SYS/custom/x", b"hello", qos=1, pid=3))
        r = read_pkt(pub)
        assert r[0] == "pkt" and (r[1] & 0xF0) == 0x40, "no PUBACK"
        reason = r[2][2] if len(r[2]) > 2 else 0x00
        assert reason == 0x00, (
            "with the feature off a $SYS publish must be accepted, got 0x%02x"
            % reason)

        r = read_pkt(app, 1.5)
        assert r[0] == "pkt" and (r[1] & 0xF0) == 0x30, (
            "the $SYS/custom publish was not routed: %r" % (r,))
        topic, _, _, payload = parse_publish(r[1], r[2])
        assert topic == "$SYS/custom/x" and payload == b"hello", (
            "routed message was wrong: %r %r" % (topic, payload))

        sub.close(); app.close(); pub.close()
        return "tree absent, $SYS publish routed as ordinary traffic"
    finally:
        stop(proc, workdir)


case("$SYS/# receives the whole tree, decimal and RETAINed", case_tree_is_delivered_retained)
case("$SYS: the tree keeps updating after subscribe (not a stale snapshot)",
     case_tree_keeps_updating_after_subscribe)
case("MQTT-4.7.2-1: a root '#' wildcard does not reach the $SYS tree",
     case_root_wildcard_does_not_reach_the_tree)
case("$SYS: an exact topic name resolves to a live count", case_single_topic_is_readable)
case("MQTT-4.1.2-2: a client PUBLISH to $SYS is refused (0x87), value unchanged",
     case_client_publish_is_refused)
case("$SYS off by default: no tree, and a $SYS publish is ordinary traffic",
     case_feature_off_changes_nothing)

print("\nSUMMARY: %d/%d passed" % (sum(1 for _, ok in results if ok), len(results)))
sys.exit(0 if all(ok for _, ok in results) else 1)
