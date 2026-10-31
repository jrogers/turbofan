# turbofan-cluster

A Raft-replicated state machine framework, modelled on Aeron Cluster. A leader orders client input
into a log, a quorum makes each entry durable, and every node applies the log to the same
deterministic `Service`. If the leader fails, another node takes over with the same state, and
clients carry on without losing or repeating a message.

The framework never looks inside the service's messages: to it, they're opaque bytes.

## A service

```rust
use turbofan_cluster::{Ctx, Service, SessionId};

/// Answers each message with how many its session has sent.
#[derive(Debug, Default)]
struct Counter {
    counts: std::collections::BTreeMap<SessionId, u64>,
}

impl Service for Counter {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        let count = self.counts.entry(session).or_default();
        *count += 1;
        ctx.send(session, count.to_string().as_bytes());
    }
    fn snapshot(&self, out: &mut Vec<u8>) { /* the whole state, as bytes */ }
    fn restore(&mut self, bytes: &[u8]) { /* back from those bytes */ }
}

fn main() -> std::process::ExitCode {
    turbofan_cluster::run(1, Counter::default())
}
```

A service must be deterministic: every node runs it over the same log and must reach the same
state. It may use only what it's given. Time comes from `Ctx::now`, the leader's clock as recorded
in the log, and it can't use other clocks, randomness, IO, or the iteration order of a hash map.

Besides `on_message`, a service can handle sessions opening and closing, schedule and cancel timers,
and close a session. `examples/counter.rs` is the whole of the example above, and
`crates/oms` is a full order book.

## Running a cluster

`run` gives a service binary its command line. Start three nodes:

```sh
cargo run --example counter -- serve --id 1 --data /tmp/counter/1 \
    --peers   1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
    --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
    --admin   1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
```

Run the same command for nodes 2 and 3. Each node keeps its log, Raft metadata and latest snapshot
in `--data`. An operator can ask for a snapshot with `snapshot --admin <addr>`. For control over
timeouts and the rest of the configuration, call `driver::serve` with a `driver::Config`.

## Clients

```rust
let mut client = ClusterClient::connect(addrs, 1, Duration::from_millis(500));
client.send(b"hello".to_vec()).await?;
while let Some(event) = client.recv().await { /* Event::Message(bytes), ... */ }
```

A `ClusterClient` keeps one session open across failovers. It finds the leader, follows redirects,
resends whatever the cluster hasn't yet applied, and drops responses it has already seen. While the
session lasts, messages are applied exactly once and in order, and responses arrive exactly once
and in order.

A client that's away longer than the cluster keeps responses for gets `Event::Lost`, naming the
first response it missed. A session the leader hears nothing from for a while is closed.
`client::Client` is the same logic without tokio, for a caller that does its own IO.

## How it's built

The core is sans-IO: it takes inputs (messages, the time, notice that writes are durable) and
returns outputs (what to persist, what to send). It does no IO and never reads a clock itself.

- `raft.rs` replicates opaque log records: elections with pre-vote, log replication and
  snapshot install.
- `node.rs` adds client sessions, timers and snapshots on top, and runs the service.
- `client.rs` is the client's half of a session.
- `driver.rs` runs a node on tokio, with TCP and files. It batches inputs while a write is syncing,
  which gives group commit.
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
