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

A leader orders client input into a log, a quorum makes each entry durable, and every node
applies the log to the same deterministic `Service`. If the leader fails, another node takes over
with the same state, and clients carry on without losing or repeating a message. The framework
never looks inside the service's messages: to it, they're opaque bytes.

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

[What it's for](https://github.com/jrogers/turbofan/blob/main/docs/guide/use-cases.md), the guide's first chapter,
goes into each, and when it isn't the right tool.

## A service

```rust
use std::io::{self, Read};

use turbofan::{Ctx, Service, SessionId, SnapshotWriter};

/// Answers each message with how many its session has sent.
#[derive(Debug, Default)]
struct Counter {
    counts: std::collections::BTreeMap<SessionId, u64>,
}

impl Service for Counter {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        let count = self.counts.entry(session).or_default();
        *count += 1;
        ctx.send(session, count.to_string().as_bytes()).expect("a count is a few bytes");
    }
    fn snapshot(&self) -> SnapshotWriter {
        let counts = self.counts.clone();
        Box::new(move |out| { /* write `counts`, as bytes */ Ok(()) })
    }
    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> { /* read them back */ Ok(()) }
}

fn main() -> std::process::ExitCode {
    turbofan::run(1, Counter::default())
}
```

A service must be deterministic: every node runs it over the same log and must reach the same
state. It may use only what it's given. Time comes from `Ctx::now`, the leader's clock as recorded
in the log, and it can't use other clocks, randomness, IO, or the iteration order of a hash map.

Besides `on_message`, a service can handle sessions opening and closing, schedule and cancel timers,
close a session, and propose a message of its own (`Ctx::propose`), for work that follows from
another: it comes back through the log to `on_proposal`, once, even across a failover. A snapshot is a frozen copy of the state, here a clone, which the node writes
out on another thread while it goes on applying. `crates/turbofan/examples/counter.rs` is the whole of the example
above.

## Running a cluster

`run` gives a service binary its command line. Start three nodes:

```sh
cargo run --example counter -- serve --id 1 --data /tmp/counter/1 \
    --peers   1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
    --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
    --admin   1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
```

Run the same command for nodes 2 and 3. The same binary's other commands operate the cluster:
`status`, `snapshot`, `add-member`, `remove-member`, `set-role`, `transfer-leadership`,
`activate`, `promote`, `shutdown`, and `capture` and `replay` (a node's log, replayed offline).

## Clients

```rust
let mut client = ClusterClient::connect(addrs, 1, credentials, Duration::from_millis(500))?;
client.send(b"hello".to_vec()).await?;
while let Some(event) = client.recv().await { /* Event::Message(bytes), ... */ }
```

A `ClusterClient` keeps one session open across failovers: while it lasts, messages are applied
exactly once and in order, and responses arrive exactly once and in order.

## The guide

[The guide](https://github.com/jrogers/turbofan/blob/main/docs/guide/README.md), also in the crate docs as `turbofan::guide`, goes
from [what it's for](https://github.com/jrogers/turbofan/blob/main/docs/guide/use-cases.md) and a first service to running a cluster in
production: [services](https://github.com/jrogers/turbofan/blob/main/docs/guide/services.md),
[clients](https://github.com/jrogers/turbofan/blob/main/docs/guide/clients.md), [the command line, configuration, durability, tokens and
TLS](https://github.com/jrogers/turbofan/blob/main/docs/guide/running.md), [members, learners and standbys](https://github.com/jrogers/turbofan/blob/main/docs/guide/membership.md),
[operations and metrics](https://github.com/jrogers/turbofan/blob/main/docs/guide/operations.md), [upgrades](https://github.com/jrogers/turbofan/blob/main/docs/guide/upgrades.md) and
[performance](https://github.com/jrogers/turbofan/blob/main/docs/guide/performance.md). Its examples run as doctests.

## How it's built

Paths are in `crates/turbofan/src`. The core is sans-IO: it takes inputs (messages, the time, notice that writes are durable) and
returns outputs (what to persist, what to send). It does no IO and never reads a clock itself.

- `raft/` replicates opaque log records: elections with pre-vote, log replication and
  snapshot install.
- `node/` adds client sessions, timers and snapshots on top, and runs the service.
- `pipeline.rs` is how a driver carries out a node's outputs without waiting on the disk: what
  goes at once, what waits for a write, and what's written together. The driver and the
  simulation both run it.
- `client.rs` is the client's half of a session.
- `driver.rs` runs a node on tokio, with TCP and files. It doesn't wait on the disk: the node goes
  on with its inputs while a write syncs, and what it takes meanwhile is written next, together,
  which gives group commit. A leader sends its new records to followers while it syncs them
  itself, so a commit waits for the slower of the two rather than both in turn.
- `remote.rs` runs a client on tokio.

The wire protocol is `crates/turbofan/schema/cluster.xml`, generated into `wire` with `turbofan-sbe`. A log record
is stored on disk exactly as it's sent in `AppendEntries`, with a checksum the follower checks
before appending.

## Testing

- **Simulation:** `crates/turbofan/tests/sim.rs` runs a whole cluster and its clients in one thread, deterministically
  from a seed. It delays, reorders, drops and duplicates messages, partitions the network, resets
  client connections, and crashes nodes, including partway through writing a batch. After every
  event it checks the cluster's guarantees. A failure prints `SIM_SEED=<n>`, which replays it. Run
  more seeds with `SIM_SEEDS=1000 cargo test -p turbofan --release --test sim`.
- **Fuzzing:** `crates/turbofan/fuzz` fuzzes what the cluster reads from the network, with `scripts/fuzz.sh`.
- **Real network and disk:** `crates/turbofan/tests/cluster.rs` runs three nodes over real TCP and real files. While
  a client keeps sending, it kills each node in turn (the leader among them) and restarts it from
  disk.

```sh
scripts/check.sh   # fmt, clippy -D warnings, tests, rustdoc, cargo deny
SIM_SEEDS=1000 cargo test -p turbofan --release --test sim   # a longer simulation
scripts/fuzz.sh    # fuzz the cluster's network inputs (nightly and cargo-fuzz)
```

## Crates

| Crate | What it is |
|---|---|
| [`turbofan`](https://github.com/jrogers/turbofan/tree/main/crates/turbofan) | A Raft-replicated state machine framework. A leader orders client input into a log, a quorum makes it durable, and every node applies it to the same deterministic service, so the service survives node failures without losing or repeating a message. |
| [`turbofan-sbe`](https://github.com/jrogers/turbofan/tree/main/crates/turbofan-sbe) | SBE (Simple Binary Encoding): codecs generated from message schemas in a `build.rs`, and a schemaless API for reading and writing fields one at a time. |

[docs/ARCHITECTURE.md](https://github.com/jrogers/turbofan/blob/main/docs/ARCHITECTURE.md) is an overview of how they fit
together.

## Releases and security

Releases and what changed in each are in [CHANGELOG.md](https://github.com/jrogers/turbofan/blob/main/CHANGELOG.md). Report security problems
privately, as [SECURITY.md](https://github.com/jrogers/turbofan/blob/main/SECURITY.md) describes, not in a public issue. CI checks every
dependency's licence and known advisories with `cargo deny` ([deny.toml](https://github.com/jrogers/turbofan/blob/main/deny.toml)).

## License

Licensed under the Apache License, Version 2.0 ([LICENSE](https://github.com/jrogers/turbofan/blob/main/LICENSE) or
<http://www.apache.org/licenses/LICENSE-2.0>).

Unless you explicitly state otherwise, any contribution intentionally submitted for inclusion in
the work by you, as defined in the Apache-2.0 license, shall be licensed as above, without any
additional terms or conditions.

### Third-party data

`turbofan-sbe`'s tests use SBE's example schema and a message encoded from it by real-logic's SBE
codecs, licensed under the Apache License, Version 2.0 (see `crates/turbofan-sbe/tests/sbe/NOTICE`).
