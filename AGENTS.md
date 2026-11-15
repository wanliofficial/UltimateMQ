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

## Backward compatibility

Opt-in behaviour must not change when no new environment variable or config is
set. A new feature is gated behind an explicit, documented knob and the default
path stays identical to before.

## Testing

Run the unit suite and, for wire-level changes, the Python suites under the
agent's scratch area before claiming a change is done. Verify, don't assume:
build, run tests, and confirm the output.
