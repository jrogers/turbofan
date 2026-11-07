<p align="center"><img src="https://raw.githubusercontent.com/jrogers/turbofan/main/docs/assets/logo.svg" width="120" alt=""></p>

<h1 align="center">turbofan</h1>

<p align="center">
  <a href="https://crates.io/crates/turbofan"><img src="https://img.shields.io/crates/v/turbofan" alt="crates.io"></a>
  <a href="https://docs.rs/turbofan"><img src="https://img.shields.io/docsrs/turbofan" alt="docs.rs"></a>
  <a href="https://github.com/jrogers/turbofan/actions/workflows/ci.yml"><img src="https://github.com/jrogers/turbofan/actions/workflows/ci.yml/badge.svg?branch=main" alt="CI"></a>
  <a href="#license"><img src="https://img.shields.io/badge/license-Apache--2.0-blue" alt="License: Apache-2.0"></a>
</p>

<p align="center">
  <a href="https://jrogers.github.io/turbofan/">Website</a> ·
  <a href="https://jrogers.github.io/turbofan/guide/">Guide</a> ·
  <a href="https://docs.rs/turbofan">API</a>
</p>

> [!WARNING]
> **Pre-1.0, and not used in production yet.** It's simulated, fuzzed and tested over a real
> network, but its APIs will change. It is provided as is, without warranty of any kind.

**Turbofan** is a set of Rust components for building fault-tolerant systems. It has the aims of being safe,
thoroughly tested, and fast.

## What it's for

Systems whose state fits in memory, where every change must happen exactly once, in one order
every machine agrees on, and quickly:

- **Replacing a consensus stack**: application servers with a coordination service for leases, a
  log to order their inputs, a database, and deduplication to stitch them together, become one
  replicated service, with no lease to check and no message applied twice.
- **Trading systems**: sequencers, matching engines, order management and pre-trade risk, where
  the order of events is the system, every report goes out once, and a day replays offline,
  exactly.
- **Ledgers, reservations, quotas and schedulers**: a balance checked and debited in one step,
  the last seat won by one request, a timer fired once though the node that set it fails.

[What it's for](crates/turbofan/docs/guide/use-cases.md), the guide's first chapter,
goes into each, and when it isn't the right tool.

## Crates

| Crate | What it is |
|---|---|
| [`turbofan`](crates/turbofan) | A Raft-replicated state machine framework. A leader orders client input into a log, a quorum makes it durable, and every node applies it to the same deterministic service, so the service survives node failures without losing or repeating a message. |
| [`turbofan-sbe`](crates/turbofan-sbe) | SBE (Simple Binary Encoding): codecs generated from message schemas in a `build.rs`, and a schemaless API for reading and writing fields one at a time. |

[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) is an overview of how they fit
together, and [the guide](crates/turbofan/docs/guide/README.md) goes from a first service to
running a cluster in production.

## Building and testing

```sh
scripts/check.sh   # fmt, clippy -D warnings, tests, rustdoc, cargo deny
SIM_SEEDS=1000 cargo test -p turbofan --release --test sim   # a longer simulation
scripts/fuzz.sh    # fuzz the cluster's network inputs (nightly and cargo-fuzz)
```

A simulation failure prints `SIM_SEED=<n>`; setting it replays that run exactly.

## Releases and security

Releases and what changed in each are in [CHANGELOG.md](CHANGELOG.md). Report security problems
privately, as [SECURITY.md](SECURITY.md) describes, not in a public issue. CI checks every
dependency's licence and known advisories with `cargo deny` ([deny.toml](deny.toml)).

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](LICENSE) or
<http://www.apache.org/licenses/LICENSE-2.0>).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any
additional terms or conditions.

### Third-party data

`turbofan-sbe`'s tests use SBE's example schema and a message encoded from it by real-logic's SBE
codecs, licensed under the Apache License, Version 2.0 (see `crates/turbofan-sbe/tests/sbe/NOTICE`).
