# Agent Guide

Guidance for automated agents and contributors working in this repository.

## Project

`flowmq` is a C++17 MQTT broker (branded "UltimateMQ"). It speaks MQTT 3.1.1
and MQTT 5 over plaintext or TLS, with authentication, ACLs, durable sessions,
retained messages and QoS 1/2 flows.

Build and test:

```bash
cmake --build build -j8        # build the broker and unit tests
./build/tests/flowmq_tests      # run the unit-test suite
```

## Naming conventions

**Abbreviations are written in full upper case.** Every acronym/initialism is
capitalised in full, the same way `MQTT` already is. This applies to file names,
class/type names, method names, member variables and parameters alike.

- Correct: `TLS`, `TLSContext`, `TLSEnabled()`, `SecureTransport`
- Wrong: `Tls`, `TlsContext`, `tlsEnabled()`, `SecureTransport`

When you introduce or rename a file, type or identifier, apply this rule to the
whole word, not just the file name. For example, renaming the transport-layer
security module also means `TlsContext` → `TLSContext`, not only `Tls.cpp` →
`TLS.cpp`.

Well-known initialisms used in this project and their canonical forms:

| Initialism | Canonical form |
|-----------|----------------|
| MQTT      | `MQTT`         |
| TLS       | `TLS`          |
| SSL       | `SSL`          |
| QoS       | `QoS`          |
| ACL       | `ACL`          |
| CA        | `CA`           |
| URI / ID  | `URI` / `ID`   |

Notes:

- Keep the convention even inside compound identifiers: `MQTTTests`,
  `TLSTests`, `FlowMQ`, etc.
- Environment-variable strings and on-disk paths are **not** identifiers and
  keep their established spelling (e.g. `FLOWMQ_TLS_CERT`). Descriptive local
  variables that merely echo such a setting name (e.g. `tlsCert`,
  `tlsCa`) may keep lower-case spelling for readability, since they are not
  standalone uses of the initialism as a name.

## Versioning

Releases use the **CoolVer** scheme (syntactically SemVer-compatible), formatted:

```
REGULAR.HOTFIX[-PRERELEASE]
```

where `REGULAR = VANITY.RELEASE`:

- **VANITY** — bumped when there are notable or exciting changes. Bumping it
  **resets RELEASE** back to 0.
- **RELEASE** — the ordinary feature-release counter; incremented for each
  regular release, reset to 0 when VANITY is bumped.
- **HOTFIX** — `0` for a regular release; otherwise a counter that increases
  monotonically across the whole history (used to fix bugs in an existing
  release). A backport to an older base `B` of hotfix `H` on release `R` is
  tagged `V.B.H`.
- **PRERELEASE** — an optional `CHANNEL.NUMBER` suffix (`alpha`, `beta`, `rc`)
  for unstable testing builds; dropped for a final release.

Examples: `0.0.0-alpha.0` → `0.1.0` (first stable) → `0.1.2` (hotfix) →
`0.2.0` (next feature release) → `1.0.0-rc.1` → `1.0.0`.

When cutting a release:

- decide the bump from the changes since the last tag (new features → bump
  RELEASE and reset HOTFIX to 0; a bug fix to an already-shipped release →
  bump HOTFIX);
- tag the release commit with an **annotated** tag (`git tag -a <version> -m ...`)
  carrying a short changelog, then `git push origin <version>`;
- the version is a git tag, not a string baked into the source.

## Backward compatibility

Opt-in behaviour must not change when no new environment variable or config is
set. A new feature is gated behind an explicit, documented knob and the default
path stays identical to before.

## Specification conformance

This broker implements normative specifications, so **specification conformance
is the acceptance criterion for any wire-level change**: don't implement from
memory or from what a peer client happens to tolerate — implement against the
spec text, cite it, and verify it.

Authoritative documents (all linked from https://mqtt.org/mqtt-specification/):

| Spec | Edition | Link |
|------|---------|------|
| MQTT 5.0 | OASIS Standard | <https://docs.oasis-open.org/mqtt/mqtt/v5.0/mqtt-v5.0.html> |
| MQTT 3.1.1 | OASIS Standard | <http://docs.oasis-open.org/mqtt/mqtt/v3.1.1/os/mqtt-v3.1.1-os.html> |
| MQTT-SN 1.2 | OASIS Standard | <https://www.oasis-open.org/committees/document.php?document_id=66091&wg_abbrev=mqtt> |

(MQTT 3.1 is kept for historical reference only and is not a target.)

Rules of thumb, learned the hard way:

- **Quote the requirement number in code.** Every normative rule enforced by
  the codec or the protocol handlers carries its tag in a comment (e.g.
  `MQTT-3.3.1-8`, `§2.2.2.2`). Keep the tag accurate when editing; a wrong tag
  is worse than none because it sends the next reader to the wrong clause.
- **Respect the modal verbs.** A `MUST` is a bug when violated; a `SHOULD` is
  a decision to document; a `MAY` is a policy choice. Do not silently promote
  any of them.
- **Watch the encode/decode edge cases.** Minimal Variable Byte Integer
  encoding, property-length fields, trailing-byte rejection, per-version return
  code tables (3.1.1's `0x00`–`0x05` vs. MQTT 5 reason codes), and
  version-gated fields (no properties section in 3.1.1) are where
  spec-deviations hide. When in doubt about a byte layout, write a probe that
  serializes and re-parses the packet, and keep it as a test.
- **A clean parse is not conformance.** Accepting a packet the spec says to
  reject (or rejecting one it says to accept) is only visible in the response
  the broker sends: check the CONNACK / SUBACK / DISCONNECT reason codes and
  the close-vs-answer behaviour for each version.
- When a spec clause is deliberately not implemented (e.g. enhanced
  authentication), refuse it explicitly with the right reason code and say so
  in a comment, rather than half-implementing it or failing silently.

## Testing

Run the unit suite and, for wire-level changes, the Python suites under the
agent's scratch area before claiming a change is done. Verify, don't assume:
build, run tests, and confirm the output.
