# turbofan-cluster

A Raft-replicated state machine framework. A leader orders client input into a log, a quorum
makes each entry durable, and every node applies the log to the same deterministic `Service`. If
the leader fails, another node takes over with the same state, and clients carry on without
losing or repeating a message.

The framework never looks inside the service's messages: to it, they're opaque bytes.

## A service

```rust
use std::io::{self, Read};

use turbofan_cluster::{Ctx, Service, SessionId, SnapshotWriter};

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
    turbofan_cluster::run(1, Counter::default())
}
```

A service must be deterministic: every node runs it over the same log and must reach the same
state. It may use only what it's given. Time comes from `Ctx::now`, the leader's clock as recorded
in the log, and it can't use other clocks, randomness, IO, or the iteration order of a hash map.

Besides `on_message`, a service can handle sessions opening and closing, schedule and cancel timers,
close a session, and propose a message of its own (`Ctx::propose`), for work that follows from
another: it comes back through the log to `on_proposal`, once, even across a failover. A snapshot is a frozen copy of the state, here a clone, which the node writes
out on another thread while it goes on applying. `examples/counter.rs` is the whole of the example
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
`activate`, `promote` and `shutdown`.

## Clients

```rust
let mut client = ClusterClient::connect(addrs, 1, credentials, Duration::from_millis(500))?;
client.send(b"hello".to_vec()).await?;
while let Some(event) = client.recv().await { /* Event::Message(bytes), ... */ }
```

A `ClusterClient` keeps one session open across failovers: while it lasts, messages are applied
exactly once and in order, and responses arrive exactly once and in order.

## The guide

[`docs/guide`](docs/guide/README.md), also in the crate docs as `turbofan_cluster::guide`, goes
from a first service to running a cluster in production: [services](docs/guide/services.md),
[clients](docs/guide/clients.md), [the command line, configuration, durability, tokens and
TLS](docs/guide/running.md), [members, learners and standbys](docs/guide/membership.md),
[operations and metrics](docs/guide/operations.md), [upgrades](docs/guide/upgrades.md) and
[performance](docs/guide/performance.md). Its examples run as doctests.

## How it's built

The core is sans-IO: it takes inputs (messages, the time, notice that writes are durable) and
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

The wire protocol is `schema/cluster.xml`, generated into `wire` with `turbofan-sbe`. A log record
is stored on disk exactly as it's sent in `AppendEntries`, with a checksum the follower checks
before appending.

## Testing

- **Simulation:** `tests/sim.rs` runs a whole cluster and its clients in one thread, deterministically
  from a seed. It delays, reorders, drops and duplicates messages, partitions the network, resets
  client connections, and crashes nodes, including partway through writing a batch. After every
  event it checks the cluster's guarantees. A failure prints `SIM_SEED=<n>`, which replays it. Run
  more seeds with `SIM_SEEDS=1000 cargo test -p turbofan-cluster --release --test sim`.
- **Fuzzing:** `fuzz/` fuzzes what the cluster reads from the network, with `scripts/fuzz.sh`.
- **Real network and disk:** `tests/cluster.rs` runs three nodes over real TCP and real files. While
  a client keeps sending, it kills each node in turn (the leader among them) and restarts it from
  disk.

## Roadmap

[`ROADMAP.md`](docs/ROADMAP.md): what's left before production, from known ceilings to membership
changes and a stable API.
