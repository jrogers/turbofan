<h1 align="center">turbofan</h1>

<p align="center">
  <a href="https://github.com/jrogers/turbofan/actions/workflows/ci.yml"><img src="https://github.com/jrogers/turbofan/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License: Apache-2.0"></a>
</p>

> [!WARNING]
> **Pre-1.0, and not used in production yet.** It's simulated, fuzzed and tested over a real
> network, but its APIs will change. It is provided as is, without warranty of any kind.

**Turbofan** is a set of Rust components for building fault-tolerant systems. It has the aims of being safe,
thoroughly tested, and fast.

## Crates

| Crate | What it is |
|---|---|
| [`turbofan-cluster`](crates/turbofan-cluster) | A Raft-replicated state machine framework, after Aeron Cluster. A leader orders client input into a log, a quorum makes it durable, and every node applies it to the same deterministic service, so the service survives node failures without losing or repeating a message. |
| [`turbofan-sbe`](crates/turbofan-sbe) | SBE (Simple Binary Encoding): codecs generated from message schemas in a `build.rs`, and a schemaless API for reading and writing fields one at a time. |

## Building and testing

```sh
scripts/check.sh   # fmt, clippy -D warnings, tests, rustdoc, cargo deny
SIM_SEEDS=1000 cargo test -p turbofan-cluster --release --test sim   # a longer simulation
scripts/fuzz.sh    # fuzz the cluster's network inputs (nightly and cargo-fuzz)
```

A simulation failure prints `SIM_SEED=<n>`; setting it replays that run exactly.

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or
<http://www.apache.org/licenses/LICENSE-2.0>).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any
additional terms or conditions.

### Third-party data

`turbofan-sbe`'s tests use SBE's example schema and a message encoded from it by real-logic's SBE
codecs, licensed under the Apache License, Version 2.0 (see `crates/turbofan-sbe/tests/sbe/NOTICE`).
