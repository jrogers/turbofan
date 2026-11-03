# Clients

Clients reach a cluster through a session. Usually the client is a gateway: a process that
speaks the outside world's protocol (FIX, HTTP, a feed) on one side and holds a session with the
cluster on the other. `ClusterClient` keeps that session open across failovers; `client::Client`
is the same logic without tokio, for a caller that does its own IO.

## A session

```rust,no_run
use std::time::Duration;

use turbofan_cluster::ClusterClient;
use turbofan_cluster::client::Event;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let members = ["10.0.0.1:7101", "10.0.0.2:7101", "10.0.0.3:7101"];
    let mut session = ClusterClient::connect(members, 1, b"desk1:secret", Duration::from_millis(500))?;
    session.send(b"hello".to_vec()).await?;
    while let Some(event) = session.recv().await {
        match event {
            Event::Opened(id) => println!("session {id} open"),
            Event::Message(response) => println!("{} bytes", response.len()),
            Event::Lost { from, to } => println!("missed responses {from} to {}", to - 1),
            Event::Closed { reason, unapplied } => {
                println!("closed: {reason:?}, {unapplied} messages never applied");
                break;
            }
        }
    }
    Ok(())
}
```

`connect` takes every member's client address, the cluster's service ID, the credentials the
service's `authenticate` checks, and how often to send a keepalive while idle: well under the
cluster's session timeout (10 s by default), or the leader closes the session as gone.

It returns at once. A task of its own connects to the members in turn until one answers; a
follower redirects it to the leader, and with no leader yet it tries the next member after a
pause. Dropping the `ClusterClient` stops the task; `close` ends the session first, so the
service sees `CloseReason::Client` at once rather than `Timeout` later.

## What it guarantees

While a session lasts, through any number of failovers and dropped connections:

- **Every message is applied exactly once, in the order sent.** `send` returns the message's
  `client_seq`, and the client holds it until the cluster says it's applied. After a failover it
  reattaches to the same session on the new leader and resends what's held; the cluster drops
  any it has already applied.
- **Every response arrives exactly once, in order.** The cluster numbers each session's
  responses, and keeps the latest for a client that reattaches, which tells it the last it saw.
  The client drops any it has seen.

`Event::Opened` comes again after each failover, with the same session ID.

`applied()` is a `tokio::sync::watch` of the highest `client_seq` the cluster has applied: every
message up to it is in the log, committed and applied, whether or not the service answered it. A
gateway that forwards messages from elsewhere uses it to know when to let go of one: acknowledge
it upstream, or drop it from a journal of its own.

```rust,no_run
# use std::time::Duration;
# use turbofan_cluster::ClusterClient;
# async fn forward(session: &ClusterClient) -> Result<(), Box<dyn std::error::Error>> {
let seq = session.send(b"order".to_vec()).await?;
let mut applied = session.applied();
applied.wait_for(|&done| done >= seq).await?;
// The order is in the cluster: tell whoever sent it.
# Ok(())
# }
```

## When it doesn't hold

A session is the unit of the guarantee. It ends when:

- **The client closes it**, or the process holding it stops. Messages it held but the cluster
  hadn't applied are gone: `Event::Closed` counts them as `unapplied`. A gateway that restarts
  opens a new session, and must find out what the cluster has (by asking the service) before it
  sends again. Keep what outlives a session by principal, not session, in the service, so the
  new session can see it.
- **The leader hears nothing from it for the session timeout**, a client cut off too long: the
  session closes with `CloseReason::Timeout`.
- **The service closes it**: `CloseReason::Service`.
- **The service refuses its credentials**: `CloseReason::Refused`, with no session opened. A
  cluster at `MAX_SESSIONS` refuses with `CloseReason::NoSession`, which is also the answer to
  reattaching to a session that has since closed.

And within a session, responses can be lost: the cluster keeps `egress_ring_entries` (8192 by
default) for each session, so a client away while more were sent misses some. It gets
`Event::Lost { from, to }`: responses `from` up to `to`, not including `to`, never arrive. Size
the ring for a gateway's messages in flight times the responses each can cause, and have the
service answer a request for what was missed (an order's state, say) if it matters.

## Backpressure

- `send` refuses past `client::MAX_UNACKED` (4096) messages held but not yet applied:
  `SendError::Full`. Read responses and try again.
- The client queues 4096 events for `recv`; past that, it stops reading the connection until
  they're taken. Keep reading.
- A leader with 64 MiB of proposals not yet committed (`max_uncommitted_bytes`) takes nothing
  more from clients until they commit. Clients see this as latency, not an error.
- A client too slow to read what the leader sends it is disconnected. It reconnects, and the
  responses it missed are sent again from the ring.

A message is at most `codec::MAX_PAYLOAD_BYTES` (64 KiB): `send` refuses a longer one with
`SendError::TooLong`.

## TLS

A cluster with TLS (see [Running a cluster][running]) takes `ClusterClient::connect_tls`, with a
`tls::ClientTls` loaded from the CA that signed the nodes' certificates. Clients present no
certificate: they're who their credentials say.

```rust,no_run
# use std::path::Path;
# use std::time::Duration;
# use turbofan_cluster::ClusterClient;
use turbofan_cluster::tls::ClientTls;
# async fn tls() -> Result<(), Box<dyn std::error::Error>> {
let tls = ClientTls::load(Path::new("ca.pem"), None)?;
let session = ClusterClient::connect_tls(["10.0.0.1:7101"], 1, b"desk1:secret", Duration::from_millis(500), tls)?;
# Ok(())
# }
```

## Doing your own IO

`client::Client` is the session logic alone, sans-IO, as the cluster's core is: for a gateway on
another runtime, or one that polls its sockets itself. The caller:

1. connects to `Client::target()`, and calls `on_connected`;
2. feeds it every frame read with `on_frame`, and writes what `take_frames` returns;
3. calls `on_tick` by `next_deadline`, for keepalives;
4. drops the connection and connects to the new target when `take_reconnect` says so (at once,
   or after a pause), and calls `on_disconnected` when a connection fails or drops;
5. takes what the cluster said with `poll_event`.

`send`, `applied` and `close` are as on `ClusterClient`. The task behind `ClusterClient`, `run`
in `src/remote.rs`, is this loop on tokio, and the place to start.
