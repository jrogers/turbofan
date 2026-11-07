# Running a cluster

A cluster is three or five nodes, usually: it stays up while a majority of its voters are, so
three survive one failure and five survive two. Each node is the service's binary, run with
`serve`.

## The command line

`turbofan::run(service_id, service)` is a service binary's whole `main` (see [A first
service][first-service]). Its `serve` command starts a node:

```sh
my-service serve --id 1 --data /var/lib/my-service \
    --peers   1=10.0.0.1:7001,2=10.0.0.2:7001,3=10.0.0.3:7001 \
    --clients 1=10.0.0.1:7101,2=10.0.0.2:7101,3=10.0.0.3:7101 \
    --admin   1=10.0.0.1:7201,2=10.0.0.2:7201,3=10.0.0.3:7201
```

Each node listens on three addresses, and each list gives every member's:

- **`--peers`**, where the nodes talk to each other: Raft's messages and the log.
- **`--clients`**, where clients connect.
- **`--admin`**, where operators connect: the commands in [Operations][operations]. Keep it on a
  private network.

The three lists name the same members, `--id` among them, and every node of a new cluster is
started with the same lists: they're its first members, written into its log. After that the log
says who the members are, and the lists only matter to a node joining (see [Members][membership]).
`--learners 3` starts the cluster with node 3 as a learner, on every node.

An address is `host:port`, the host an IP address or a name. A node looks a name up each time it
dials it, so a node that moves keeps its place in the cluster as long as it keeps its name. A
node binds its own addresses if they're IP addresses; if one is a name, `--listen <ip>` says which
interface its three listeners bind, on their ports (`--listen 0.0.0.0` for all of them), and
without it `serve` refuses to start rather than guess.

On Kubernetes, that's a StatefulSet and a headless Service, each pod named by its ordinal and
reached at `<pod>.<service>`:

```sh
my-service serve --id $((${HOSTNAME##*-} + 1)) --data /var/lib/my-service --listen 0.0.0.0 \
    --peers   1=my-service-0.my-service:7001,2=my-service-1.my-service:7001,3=my-service-2.my-service:7001 \
    --clients 1=my-service-0.my-service:7101,2=my-service-1.my-service:7101,3=my-service-2.my-service:7101 \
    --admin   1=my-service-0.my-service:7201,2=my-service-1.my-service:7201,3=my-service-2.my-service:7201
```

Node IDs start at 1 and ordinals at 0, hence the `+ 1`. The data directory is the pod's
persistent volume. Set `publishNotReadyAddresses: true` on the Service: the nodes must find each
other before any is ready, or no leader is elected. Spread the pods over zones (a topology spread
constraint) for the reason in [Durability](#durability). Scaling the StatefulSet doesn't change the
members: add and remove them as [Members][membership] says. A pod stopped in a rolling update gets
SIGTERM, and a leader hands its leadership over before it exits (see [Operations][operations]),
well within the default grace period.

The service ID is the first argument to `run`. Every connection's hello carries it, and a node
refuses a client, operator or peer of another service, so two clusters can't be confused for one.

## The data directory

`--data` holds the node's durable state:

| Path | What |
|---|---|
| `log/` | The log, in segment files of up to 64 MiB, named by their first index. |
| `log/lock` | Held while the node runs: a second process on the same directory is refused. |
| `log/doubt` | There while the node doubts its log (see [Members][membership]). |
| `meta` | The current term and vote. |
| `snapshots/` | The latest snapshot, or the latest `node.snapshots_kept`. |

A node that restarts loads its snapshot, replays the log after it and rejoins. Each snapshot
drops the log before it, but for a tail of 100,000 entries (`log_tail_entries`) kept for followers
a little behind. Nothing outside the directory is needed, and a node whose disk is lost is
replaced, not repaired: see [Members][membership].

## Durability

The log isn't synced to disk before an entry is acknowledged, by default. The other nodes' copies
are the redundancy: a process that crashes loses nothing (the operating system still has the
writes), and a machine that's lost is caught up from the others. It's still synced behind the
writes, every 4 MiB, on a thread nothing waits for, so that unwritten data doesn't pile up and
stall the disk. What it doesn't survive is a majority of nodes losing power at
once, which may lose entries already committed. Spread the nodes over power domains, or run with
`serve --fsync`, which syncs the log once a batch before acknowledging it, at a cost in latency
(see [Performance][performance]). The term and vote, and snapshots, are always synced.

## Configuration

`run` uses `NodeConfig::new`'s defaults. `run_with` changes them in code, for the settings that
are the service's to choose; `serve`'s flags then apply over them, for what an operator chooses
for a deployment (`--election-timeout <ms>`, `--heartbeat <ms>`, `--keep-snapshots <n>`,
`--fsync`):

```rust,no_run
# use std::io::{self, Read};
# use std::time::Duration;
# use turbofan::{Ctx, Service, SessionId, SnapshotWriter};
# #[derive(Debug, Default)]
# struct Counter;
# impl Service for Counter {
#     fn on_message(&mut self, _ctx: &mut Ctx, _session: SessionId, _payload: &[u8]) {}
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
# }
fn main() -> std::process::ExitCode {
    turbofan::run_with(1, Counter::default(), |config| {
        // Its gateways reconnect within 30 s, and may miss up to 20,000 responses meanwhile.
        config.node.session_timeout = Duration::from_secs(30);
        config.node.egress_ring_entries = 20_000;
    })
}
```

For full control, call `driver::serve` with a `driver::Config` from a `main` of your own:

| Setting | Default | |
|---|---|---|
| `node.raft.election_timeout` | 300 ms | A follower that hears no leader for between this and twice this stands for election: the time a failover takes. `--election-timeout`. |
| `node.raft.heartbeat` | 50 ms | How often a leader heartbeats: under half the election timeout. `--heartbeat`. |
| `node.raft.seed` | the node's ID | Seeds the election timeout's jitter: make it differ between nodes. `run` mixes in the time. |
| `node.raft.log_window_bytes` | 64 MiB | Applied records kept in memory, for followers a little behind; older ones are read back from disk. |
| `node.raft.log_tail_entries` | 100,000 | Entries kept before a snapshot, so a follower that far behind is sent the log, not the snapshot. |
| `node.raft.max_uncommitted_bytes` | 64 MiB | A leader with this much proposed and not yet committed takes nothing more from clients. |
| `node.session_timeout` | 10 s | A session the leader hears nothing from for this long is closed, and so is a client connection any node hears nothing on. |
| `node.snapshot_every_entries` | 100,000 | Each node snapshots once this many entries follow its last snapshot. |
| `node.snapshots_kept` | 1 | The newest snapshots each node keeps on disk (`--keep-snapshots`): see [Operations][operations]. |
| `node.egress_ring_entries` | 8192 | Responses kept per session for a client that reattaches (up to `MAX_EGRESS_RING`). From protocol 1, the leader's at the latest `activate`, for every node: see [Upgrades][upgrades]. |
| `log_fsync` | `Fsync::Never` | `Fsync::EachBatch` is `--fsync`. |
| `admin_token`, `peer_token` | none | See below. |
| `tls` | none | See below. |

```rust,no_run
# use std::io::{self, Read};
# use std::time::Duration;
# use turbofan::{Ctx, Service, SessionId, SnapshotWriter};
# #[derive(Debug, Default)]
# struct Counter;
# impl Service for Counter {
#     fn on_message(&mut self, _ctx: &mut Ctx, _session: SessionId, _payload: &[u8]) {}
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
# }
use turbofan::driver::{self, Fsync};
use turbofan::{Member, NodeConfig, NodeId};

#[tokio::main]
async fn main() -> io::Result<()> {
    let ip = |n: u8| format!("10.0.0.{n}");
    let members: Vec<Member> = (1..=3)
        .map(|n| Member::new(NodeId(n), format!("{}:7001", ip(n)), format!("{}:7101", ip(n)), format!("{}:7201", ip(n))))
        .collect();
    let id = NodeId(1);
    let mut node = NodeConfig::new(id, members.clone());
    node.session_timeout = Duration::from_secs(30);
    node.snapshot_every_entries = 1_000_000;
    let addr = |s: &str| s.parse().expect("an address");
    let peers = members.iter().map(|m| (m.id, addr(&m.peer))).collect();
    let (client, admin) = (addr(&members[0].client), addr(&members[0].admin));
    let mut config = driver::Config::new(node, 1, "/var/lib/my-service", peers, client, admin);
    config.log_fsync = Fsync::EachBatch;
    config.admin_token = std::env::var("TURBOFAN_ADMIN_TOKEN").ok().map(String::into_bytes);
    config.peer_token = std::env::var("TURBOFAN_PEER_TOKEN").ok().map(String::into_bytes);
    driver::serve(config, Counter::default()).await
}
```

`serve` returns `Ok` once an operator shuts the cluster down. It returns an error if the data
directory can't be opened or a listener can't bind, and if a write or sync fails: the node's
durable state is then unknown, so it stops, and recovers from what's on disk when restarted.

Every node of a cluster should run the same settings, but for its ID, addresses and seed.

## Tokens

Without TLS, anyone who can reach a node's admin listener can run operators' commands, and anyone
who can reach its peer listener can act as a node. Two shared secrets close that:

- **`TURBOFAN_ADMIN_TOKEN`**: `serve` refuses an operator without it, and the operators' commands
  send it. Set it for every node and every operator.
- **`TURBOFAN_PEER_TOKEN`**: every node sends it to its peers, and refuses a peer without it. Set
  it on every node.

They're read from the environment, not flags, to keep them out of the process list, and `serve`
warns when either is unset. They're checked, not encrypted: anyone who can watch the network can
read them, so keep the admin and peer listeners on a private network all the same, or use TLS.

To turn the peer token on in a running cluster, restart the nodes with it one at a time (see
[Operations][operations]). Until the last, the nodes with it don't hear those without, which is
like them being down, so wait for each to catch up before the next.

## TLS

Built with the `tls` feature, a node can put TLS 1.3 (rustls) on all three listeners and on its
connections to peers:

```sh
my-service serve ... \
    --tls-cert node.pem --tls-key node-key.pem --tls-peer-ca peer-ca.pem --tls-admin-ca admin-ca.pem
```

or `driver::Config::tls`, a `tls::NodeTls::new` of the same four files. Then:

- **Peers** check each other's certificates against the peer CA, both ways. A node's certificate
  is its client certificate too when it dials a peer, so if it has an extended key usage, that
  must allow both `serverAuth` and `clientAuth`.
- **Operators** present a certificate the admin CA signed: the commands take `--tls-ca <pem>
  --tls-cert <pem> --tls-key <pem>`. Leave out `--tls-admin-ca` (`admin_ca: None`) and they
  present none, as clients don't: `--tls-ca <pem>` alone, and `TURBOFAN_ADMIN_TOKEN` says who
  they are.
- **Clients** check the node's certificate against the CA they're given, and present none: they
  say who they are by their credentials, as without TLS.

A certificate is checked against the address dialled, so a node's must name, as subject
alternative names, every host it's reached at: its peer, client and admin addresses'.

A peer certificate stands for the cluster, not one node: any node holding one may say it's any
member, as with the peer token. Unless `--tls-peer-ids` (`NodeTls::peer_ids`): then each node's
certificate names its node ID, as the DNS name `node-<id>.turbofan` beside its hosts', and a peer
is taken only as the node its certificate names. A node refuses a peer whose hello says another
ID, and won't send to one whose certificate doesn't name the member it dialled. To turn it on in a
running cluster, give every node a certificate naming its ID first, then restart the nodes with the
flag one at a time. A node rereads its certificate, key and CA files every minute
(`--tls-reload <secs>`, `NodeTls::reload_every`), and when one has changed, loads them again for
the connections it makes and takes after; those open keep what they had. So a certificate or CA is
replaced by rewriting its file, as cert-manager, a Vault agent or SPIRE does, with no restart.
Files that don't load, half written, say, are left until they do, the node keeping what it had:
`turbofan_tls_reloads{result="failed"}` counts them. Adding or taking away a CA, which changes
what TLS is on, is still a restart. A node with TLS and one without can't talk, so a cluster turns TLS on or off all
at once.

Leave out `--tls-peer-ca` (`tls::NodeTls::admin_only`, with no peer CA) and only the admin listener
has TLS: peers and clients stay on plain TCP, keep the peer token. That's each node's own, so a
running cluster can take it one node at a time.

Without the `tls` feature the API is the same, but loading any TLS configuration is an error, and
the flags are refused: there's never TLS to run, nor a TLS dependency to license.

[first-service]: first-service.md
[membership]: membership.md
[operations]: operations.md
[performance]: performance.md
[upgrades]: upgrades.md
