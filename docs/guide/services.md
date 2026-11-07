# Services in depth

[A first service][first-service] wrote the three callbacks a service must have. This chapter
covers the rest of `Service` and `Ctx`: who may open a session, sessions opening and closing,
timers, work the service hands itself through the log, and testing a service without a cluster.

## The callbacks

Every callback but `authenticate` runs on every node, as its entry is applied, in log order, one
at a time. Each is given a `Ctx`, and what it does through the `Ctx` takes effect once it returns,
in order:

| Callback | When |
|---|---|
| `authenticate(credentials)` | A client opens a session, or reattaches to one: on the leader only, before anything is logged. |
| `on_session_open(session, principal)` | A session opened, for the principal `authenticate` named. |
| `on_message(session, payload)` | A message from a session: each exactly once, in the order the client sent them. |
| `on_session_close(session, reason)` | A session closed. Sends to it are dropped from here on. |
| `on_timer(id)` | A timer the service scheduled fired. |
| `on_proposal(payload)` | A payload the service proposed came back through the log. |
| `on_activate(version)` | An operator switched the cluster to a new version of the service's behaviour (see [Upgrades][upgrades]). |
| `snapshot()`, `restore(from)` | The node saves the state, or replaces it with a saved one. |

And what a callback can do:

| `Ctx` | Does |
|---|---|
| `now()` | Cluster time in nanoseconds: the leader's clock when the entry was logged. |
| `send(session, payload)` | Sends a response, delivered exactly once and in order if the session is open. |
| `schedule_timer(id, at)`, `cancel_timer(id)` | Fires `on_timer(id)` at cluster time `at`, or doesn't. |
| `close_session(session)` | Closes a session, with `CloseReason::Service`. |
| `propose(payload)` | Puts a payload in the log, for `on_proposal`. |
| `service_version()` | The version of the service's behaviour the cluster runs. |

## Sessions and principals

A client opens a session with credentials: opaque bytes, a token, say. The leader passes them to
`Service::authenticate`, which returns the client's principal (a user name, at most
`MAX_PRINCIPAL_BYTES`), or `None` to refuse it. Only the principal goes in the log, never the
credentials, and since only the leader asks, before anything is logged, `authenticate` may read
anything: the state, a file of tokens, a clock. By default it accepts everyone, with an empty
principal.

```rust
use std::collections::BTreeMap;

use turbofan::{Ctx, Service, SessionId};
# use std::io::{self, Read};
# use turbofan::SnapshotWriter;

/// Clients who say who they are, and the sessions each has open.
#[derive(Debug, Default)]
struct Desk {
    principals: BTreeMap<SessionId, String>,
}

impl Service for Desk {
    /// Credentials are `name:secret`; a real service would check the secret.
    fn authenticate(&self, credentials: &[u8]) -> Option<Vec<u8>> {
        let (name, _secret) = std::str::from_utf8(credentials).ok()?.split_once(':')?;
        Some(name.as_bytes().to_vec())
    }

    fn on_session_open(&mut self, ctx: &mut Ctx, session: SessionId, principal: &[u8]) {
        let name = String::from_utf8_lossy(principal).into_owned();
        ctx.send(session, format!("hello, {name}")).expect("a short greeting");
        self.principals.insert(session, name);
    }

    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let name = &self.principals[&session];
        ctx.send(session, format!("{name} said {}", String::from_utf8_lossy(payload))).ok();
    }
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
}
```

A refused client gets no session: its `ClusterClient` sees `Event::Closed` with
`CloseReason::Refused`. A `SessionId` is the log index of the entry that opened it, so it's easy
to guess: a client reattaching after a failover sends its credentials again, and only one the
service gives the same principal may take the session back.

Key what outlives a session by principal, not session. A gateway that restarts opens a new
session, and should find its orders where it left them.

A session closes when:

- the client closes it (`CloseReason::Client`);
- the leader hears nothing from it for `session_timeout`, 10 s by default
  (`CloseReason::Timeout`): a `ClusterClient` sends keepalives while idle, so this is a client
  that's gone;
- the service closes it, with `Ctx::close_session` (`CloseReason::Service`).

`on_session_close` gets the reason. At most `MAX_SESSIONS` (10,000) are open at once; past that,
a new one is refused with `CloseReason::NoSession`.

## Responses

`Ctx::send` queues a response for a session, and every node keeps the latest
`egress_ring_entries` (8192 by default) for each, in its state and its snapshots. A client that
reconnects after a failover tells the new leader the last response it saw, and is sent the rest
from there, so it sees each once, in order. A client away while more than that were sent misses
some, and is told so (see [Clients][clients]).

A response is at most `codec::MAX_PAYLOAD_BYTES` (64 KiB); `send` refuses a longer one with
`PayloadTooLong`, and sends nothing. It's an error, not a panic, because a panic stops every node
that applies the entry. Decide once what a service does with it, and don't `unwrap` it on input
a client controls.

## Timers

`Ctx::schedule_timer(id, at)` fires `on_timer(id)` at cluster time `at`, in nanoseconds, or soon
after; scheduling the same ID again replaces the timer, and `cancel_timer` drops it. Timers are
part of the replicated state, so they survive a failover and a restart. The leader watches the
deadlines against its own clock, and when one passes, logs that the timer fired; every node then
fires it as it applies that entry, with `Ctx::now` at or after `at`. A timer cancelled or moved
later in the meantime doesn't fire.

So a timer fires a commit's latency after its deadline, and later with no leader. Use one for
an order's expiry, a session's lease or a day's end, not for anything that needs microseconds.

## Proposals

Some work follows from a callback but belongs in an entry of its own: matching an order book
after an order changes it, say, or the next step of a batch. `Ctx::propose(payload)` puts a
payload in the log, and `on_proposal(payload)` applies it, on every node, once each, in the order
proposed. It's held in the replicated state until the leader, whichever node that is by then, has
logged it, so a failover doesn't lose it, nor log it twice.

```rust
# use std::io::{self, Read};
# use turbofan::{Ctx, Service, SessionId, SnapshotWriter};
/// Answers each message, then counts it, a step later.
#[derive(Debug, Default)]
struct Tally {
    total: u64,
}

impl Service for Tally {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        ctx.send(session, b"noted".to_vec()).ok();
        ctx.propose(b"count".to_vec()).expect("a short proposal");
    }

    fn on_proposal(&mut self, _ctx: &mut Ctx, _payload: &[u8]) {
        self.total += 1;
    }
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
}
```

Other entries may come between the callback and its proposal, so a proposal says what it's for:
an order's ID, not "the order just changed".

## Testing a service

A service is a plain struct, and `Ctx::new(now)` makes a context without a cluster: call the
callbacks with it, then look at what they did with `Ctx::take_effects`.

```rust
use turbofan::{Ctx, Effect, Service, SessionId};
# use std::collections::BTreeMap;
# use std::io::{self, Read};
# use turbofan::SnapshotWriter;

/// Answers "ring" a second after each message.
#[derive(Debug, Default)]
struct Alarm {
    waiting: BTreeMap<u64, SessionId>,
}

const SECOND: u64 = 1_000_000_000;

impl Service for Alarm {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        // The session's ID is unique, so it serves as the timer's.
        self.waiting.insert(session.0, session);
        ctx.schedule_timer(session.0, ctx.now() + SECOND);
    }

    fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
        if let Some(session) = self.waiting.remove(&id) {
            ctx.send(session, b"ring".to_vec()).ok();
        }
    }
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
}

let mut alarm = Alarm::default();
let session = SessionId(7);
let mut ctx = Ctx::new(5 * SECOND);
alarm.on_message(&mut ctx, session, b"wake me");
assert_eq!(ctx.take_effects(), [Effect::Schedule { id: 7, at: 6 * SECOND }]);

let mut ctx = Ctx::new(6 * SECOND);
alarm.on_timer(&mut ctx, 7);
assert_eq!(ctx.take_effects(), [Effect::Send(session, b"ring".to_vec())]);
```

Test `snapshot` and `restore` the same way: snapshot a state, restore it into a new service,
and check the two behave alike, and that snapshotting the copy writes the same bytes.
`Ctx::with_service_version` makes a context as if the cluster ran a given version, for testing
an upgrade.

The framework's own simulation tests a cluster under failures; it doesn't know your service. A
test of your own that kills and restarts nodes under load, and checks nothing is lost or doubled,
is worth having for any service.

### A cluster in memory

`turbofan::testing`, with the `testing` feature (in your dev-dependencies), runs a whole
cluster of your service in one thread: the nodes' logs and snapshots in memory, the network a
queue, and the clock yours to move. A failover takes milliseconds, and a run is the same every
time. Its clients are the same as a gateway's, so they resend after a failover as a gateway does:

```toml
[dev-dependencies]
turbofan = { version = "0.6", features = ["testing"] }
```

```rust
# #[cfg(feature = "testing")]
# fn main() {
use std::time::Duration;
use turbofan::client::Event;
use turbofan::testing::Cluster;
use turbofan::{Ctx, Service, SessionId};
# use std::io::{self, Read};
# use turbofan::SnapshotWriter;

/// Counts the messages it's sent, and answers each with the count.
#[derive(Debug, Default)]
struct Counter(u64);

impl Service for Counter {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        self.0 += 1;
        ctx.send(session, self.0.to_string()).ok();
    }
#     fn snapshot(&self) -> SnapshotWriter { let n = self.0; Box::new(move |out| out.write_all(&n.to_le_bytes())) }
#     fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
#         let mut n = [0; 8];
#         from.read_exact(&mut n)?;
#         self.0 = u64::from_le_bytes(n);
#         Ok(())
#     }
}

let mut cluster = Cluster::new(3, |_| Counter::default());
let alice = cluster.connect(b"alice").unwrap();
cluster.send(alice, b"one".to_vec()).unwrap();
cluster.run_for(Duration::from_millis(10));

// The leader crashes; another is elected, and the client resends on its session.
cluster.crash(cluster.leader().unwrap());
cluster.send(alice, b"two".to_vec()).unwrap();
assert!(cluster.run_until(Duration::from_secs(2), |c| c.client(alice).unacked() == 0));

let answers: Vec<Event> = cluster.events(alice).into_iter().filter(|e| matches!(e, Event::Message(_))).collect();
assert_eq!(answers, [Event::Message(b"1".to_vec()), Event::Message(b"2".to_vec())]);
# }
# #[cfg(not(feature = "testing"))]
# fn main() {}
```

`Cluster::crash` and `restart` stop and start a node from what it had written, `isolate` and
`heal` cut it off from the others, and `transfer_leadership` and `take_snapshot` ask what an
operator would. `Cluster::service` is a node's service, to check every node applied the same
state, and `peek_events` what a client has been told without taking it, for `run_until` to wait
on. `testing::Builder` sets the seed, the latencies and the nodes' configuration (snapshot
often, say, to test `restore`). It doesn't model damaged disks, changes of members or upgrades;
its API may change before 1.0. The edges that reach a cluster over the network, with
`ClusterClient`, are still tested against nodes over TCP: `tests/cluster.rs` does that for an
echo service.

[clients]: clients.md
[first-service]: first-service.md
[upgrades]: upgrades.md
