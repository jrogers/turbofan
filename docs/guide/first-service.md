# A first service

Turbofan replicates a state machine. A leader orders the messages clients send into a log, a
quorum of nodes makes each entry durable, and every node applies the log, in order, to the same
`Service`. If the leader fails, another node takes over with the same state, and clients carry on
without losing or repeating a message.

The framework never looks inside the service's messages: to it, they're opaque bytes. Encode them
however you like: `turbofan-sbe` generates SBE codecs from a schema, as the cluster's own are.

## Dependencies

Nodes and clients run on [tokio](https://docs.rs/tokio):

```toml
[dependencies]
turbofan = "0.9"
tokio = { version = "1", features = ["rt-multi-thread", "macros"] }
```

The `tls` feature adds TLS on every connection (see [Running a cluster][running]).

## A service

A service implements `Service`. This one counts each session's messages and answers each with the
count so far:

```rust
use std::collections::BTreeMap;
use std::io::{self, Read};

use turbofan::{CloseReason, Ctx, Service, SessionId, SnapshotWriter};

#[derive(Debug, Default)]
struct Counter {
    counts: BTreeMap<SessionId, u64>,
}

impl Service for Counter {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        let count = self.counts.entry(session).or_default();
        *count += 1;
        ctx.send(session, count.to_string()).expect("a count is a few bytes");
    }

    fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, _reason: CloseReason) {
        self.counts.remove(&session);
    }

    /// A copy of the counts, which the node writes out on another thread while it goes on.
    fn snapshot(&self) -> SnapshotWriter {
        let counts = self.counts.clone();
        Box::new(move |out| {
            for (session, count) in &counts {
                out.write_all(&session.0.to_le_bytes())?;
                out.write_all(&count.to_le_bytes())?;
            }
            Ok(())
        })
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let words: Vec<u64> = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w)).collect();
        self.counts = words.as_chunks::<2>().0.iter().map(|&[session, count]| (SessionId(session), count)).collect();
        Ok(())
    }
}
```

`on_message`, `snapshot` and `restore` are the callbacks you must write; [Services in
depth][services] has the rest. `Ctx` is how a callback acts: `send` answers a session, and what it
does takes effect once the callback returns.

## Determinism

Every node runs the service over the same log and must reach the same state, byte for byte. So a
service depends only on what it's given:

- **Time** comes from `Ctx::now`: the leader's clock, in nanoseconds, when the entry being applied
  was logged. It's the same on every node, and never goes backwards. Never read a clock.
- **No randomness**, unless seeded from the state. No IO: no files, no network, no environment.
- **No hash map iteration order**, which differs between processes. Iterate a `BTreeMap`, or sort.
- **No panics on what a client sends.** Every node applies the same entry, so a panic stops them
  all, and stops them again each time they replay it. That's why `Ctx::send` returns an error for
  a payload that's too long rather than panicking.

What a service needs from outside (prices, a venue's acknowledgement) comes in as a message from a
client, which puts it in the log. `Service::authenticate` is the exception: only the leader calls
it, before anything is logged, so it may read a key file.

## Snapshots

A node can't keep its log forever. Every `snapshot_every_entries` entries (100,000 by default)
each node asks the service for a snapshot, then drops the log before it. A node that restarts
loads its latest snapshot and replays the log after it; one that's too far behind is sent the
leader's.

`snapshot` doesn't write the state: it returns a closure that does, later, on another thread,
while the node goes on applying. So the closure owns a frozen copy: a clone, as here, or
persistent data structures that share most of their memory with the live state. The same state
must write the same bytes on every node: here, a `BTreeMap` writes in key order. `restore`
replaces the whole state with what a snapshot wrote.

## A cluster and a client in one program

`driver::serve` runs a node on tokio, with TCP and files. A one-node cluster is a cluster: it
elects itself and commits on its own. Here one runs alongside a `ClusterClient`, which opens a
session, sends three messages and reads the counts back; then an `Admin` shuts the cluster down:

```rust
# use std::collections::BTreeMap;
# use std::io::{self, Read};
# use turbofan::{Ctx, Service, SessionId, SnapshotWriter};
# #[derive(Debug, Default)]
# struct Counter { counts: BTreeMap<SessionId, u64> }
# impl Service for Counter {
#     fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
#         let count = self.counts.entry(session).or_default();
#         *count += 1;
#         ctx.send(session, count.to_string()).expect("a count is a few bytes");
#     }
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
# }
use std::net::{SocketAddr, TcpListener};
use std::time::Duration;

use turbofan::client::Event;
use turbofan::driver;
use turbofan::{Admin, ClusterClient, Member, NodeConfig, NodeId};

/// This cluster's service ID: a client or node of another service is refused.
const SERVICE_ID: u32 = 1;

/// A free local address, for the example. A real cluster's are fixed.
fn local_addr() -> io::Result<SocketAddr> {
    TcpListener::bind("127.0.0.1:0")?.local_addr()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let (peer, client, admin) = (local_addr()?, local_addr()?, local_addr()?);
    let id = NodeId(1);
    let member = Member {
        id,
        peer: peer.to_string(),
        client: client.to_string(),
        admin: admin.to_string(),
        learner: false,
    };
    let data = tempfile::tempdir()?;
    let node = NodeConfig::new(id, vec![member]);
    let config = driver::Config::new(node, SERVICE_ID, data.path(), vec![(id, peer)], client, admin);
    let node = tokio::spawn(driver::serve(config, Counter::default()));

    // Credentials are for the service to check; Counter takes anyone.
    let mut session = ClusterClient::connect([client.to_string()], SERVICE_ID, b"", Duration::from_secs(1))?;
    for _ in 0..3 {
        session.send(b"hello".to_vec()).await?;
    }
    let mut counts = Vec::new();
    while counts.len() < 3 {
        match session.recv().await.ok_or("the session ended")? {
            Event::Opened(id) => println!("session {id} open"),
            Event::Message(count) => counts.push(String::from_utf8(count)?),
            other => return Err(format!("unexpected {other:?}").into()),
        }
    }
    assert_eq!(counts, ["1", "2", "3"]);

    // Every member snapshots, then stops: `serve` returns.
    Admin::new(admin.to_string(), SERVICE_ID).shutdown().await?;
    node.await??;
    Ok(())
}
```

The client connected before the node was listening, and sent before its session was open: it
retries the connection, and holds what's sent until the session opens. Its first event is
`Opened`, with the session's ID; then each response, once, in order.

## Three nodes

For a real cluster, `turbofan::run` gives a service binary its command line, so its `main`
is one call:

```rust,no_run
# use std::io::{self, Read};
# use turbofan::{Ctx, Service, SessionId, SnapshotWriter};
# #[derive(Debug, Default)]
# struct Counter;
# impl Service for Counter {
#     fn on_message(&mut self, _ctx: &mut Ctx, _session: SessionId, _payload: &[u8]) {}
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
# }
fn main() -> std::process::ExitCode {
    turbofan::run(1, Counter::default())
}
```

That's `examples/counter.rs` in the repository. Start three nodes, each in its own terminal:

```sh
cargo run --example counter -- serve --id 1 --data /tmp/counter/1 \
    --peers   1=127.0.0.1:7001,2=127.0.0.1:7002,3=127.0.0.1:7003 \
    --clients 1=127.0.0.1:7101,2=127.0.0.1:7102,3=127.0.0.1:7103 \
    --admin   1=127.0.0.1:7201,2=127.0.0.1:7202,3=127.0.0.1:7203
```

and the same with `--id 2` and `--id 3`. A client is given every node's client address, and finds
the leader. Kill the leader while a client sends, and the client carries on with the next one,
with nothing lost or repeated. [Running a cluster][running] has the rest of the command line.

## How it's layered

The core is sans-IO: it takes inputs (messages, the time, notice that a write is durable) and
returns outputs (what to write, what to send), and does no IO and reads no clock. That's what lets
`tests/sim.rs` run a whole cluster in one thread from a seed, crashing nodes and cutting the
network, and replay any failure exactly.

- `raft/` replicates opaque log records: elections, replication, membership.
- `node/` adds client sessions, timers and snapshots on top, and runs the service.
- `driver` runs a node on tokio, with TCP and files; `ClusterClient` runs a client.

[ARCHITECTURE.md](https://github.com/jrogers/turbofan/blob/main/docs/ARCHITECTURE.md) goes through it in full.

[running]: running.md
[services]: services.md
