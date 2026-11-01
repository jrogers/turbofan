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
        ctx.send(session, count.to_string().as_bytes());
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
above, and the [OMS](https://github.com/jrogers/oms) is a full order book.

## Running a cluster

`run` gives a service binary its command line. Start three nodes:

```sh
cargo run --example counter -- serve --id 1 --data /tmp/counter/1 \
    --peers   1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
    --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
    --admin   1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
```

Run the same command for nodes 2 and 3. Each node keeps its log, Raft metadata and latest snapshot
in `--data`. An operator can ask for a snapshot with `snapshot --admin <addr>`.

Set `TURBOFAN_ADMIN_TOKEN` to the same secret for every node and every operator's command: a node
refuses an operator without it (`serve` warns when it's unset, and then refuses no one). It's
checked, not encrypted, so keep the admin listener on a private network all the same.
`TURBOFAN_PEER_TOKEN` is the same for the nodes' hellos to each other: set it, on every node, so
that nothing else that reaches the peer listener can act as a node. To turn it on in a running
cluster, restart the nodes with it one at a time: until the last, the ones that have it don't hear
those that don't, which is like them being down, so wait for each to catch up before the next.
For control over timeouts and the rest of the configuration, call `driver::serve` with a
`driver::Config`.

The members change one at a time, while the cluster runs. To add a fourth node, start it with
`--join` (its lists are then just where to find the cluster), and ask for it:

```sh
cargo run --example counter -- serve --id 4 --data /tmp/counter/4 --join \
    --peers   1=127.0.0.1:7001,4=127.0.0.1:7004 \
    --clients 1=127.0.0.1:7101,4=127.0.0.1:7104 \
    --admin   1=127.0.0.1:7201,4=127.0.0.1:7204
cargo run --example counter -- add-member --admin 127.0.0.1:7201 \
    --member 4=127.0.0.1:7004,127.0.0.1:7104,127.0.0.1:7204
```

It's sent the log, or a snapshot, without a vote until it has caught up, then joins. `remove-member
--admin <addr> --id <n>` takes one out; stop it once that's answered. Replacing a node whose disk
died is a remove, then an add of a new one.

A node that finds a damaged record at the end of its log, after a crash, drops it: a write that
power cut off partway, it's never been acknowledged. But bit rot in the last record looks the
same, and that may have been, so the node doubts its log (`turbofan_in_doubt`) until a leader
has caught it up, and an election needs a vote more than a majority for each voter in doubt.
Usually that passes unnoticed. But after a power cut every node may doubt, and then an election
needs every member; or in a three-node cluster with one node's disk dead and another in doubt,
there's no leader, so the dead one can't be removed. If you know a node in doubt only lost power,
delete the `doubt` file in its `--data` directory's `log/`, and restart it.

Before stopping the leader, for a rolling restart, `transfer-leadership --admin <addr> [--to <n>]`
hands over to a caught-up member: a quick election instead of waiting out an election timeout.
A node reports its role, term, indices, sessions, followers' lag and commit latency through the
[`metrics`](https://docs.rs/metrics) facade (`src/metrics.rs` lists them): install a recorder, a
Prometheus exporter say, in the binary before `run` to collect them.

A rolling upgrade that changes a service's behaviour ships code with the old behaviour and the
new (`Service::version` says which it has), node by node, then switches: `activate --admin <addr>
--service <n>`, refused while a member doesn't have it. Every node switches at the same point in
the log, and `Ctx::service_version` tells the service which runs. [`WIRE.md`](docs/WIRE.md) has
the cluster's own rules for changing what nodes send each other.

`shutdown --admin <addr>` stops the whole cluster: every member snapshots at the same index, then
exits, so a restart loads the snapshot instead of replaying the log.

## Clients

```rust
let mut client = ClusterClient::connect(addrs, 1, credentials, Duration::from_millis(500));
client.send(b"hello".to_vec()).await?;
while let Some(event) = client.recv().await { /* Event::Message(bytes), ... */ }
```

A client carries credentials, opaque bytes the service checks in `Service::authenticate` on the
leader: refused, it gets no session; accepted, the session opens for the principal the service
names, which `on_session_open` is given and only which may reattach to it.

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
