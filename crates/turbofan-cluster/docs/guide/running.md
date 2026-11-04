# Running a cluster

A cluster is three or five nodes, usually: it stays up while a majority of its voters are, so
three survive one failure and five survive two. Each node is the service's binary, run with
`serve`.

## The command line

`turbofan_cluster::run(service_id, service)` is a service binary's whole `main` (see [A first
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

`run` uses `NodeConfig::new`'s defaults. For any other, call `driver::serve` with a
`driver::Config` from a `main` of your own:

| Setting | Default | |
|---|---|---|
| `node.raft.election_timeout` | 300 ms | A follower that hears no leader for between this and twice this stands for election: the time a failover takes. |
| `node.raft.heartbeat` | 50 ms | How often a leader heartbeats: well under the election timeout. |
| `node.raft.seed` | the node's ID | Seeds the election timeout's jitter: make it differ between nodes. `run` mixes in the time. |
| `node.raft.log_window_bytes` | 64 MiB | Applied records kept in memory, for followers a little behind; older ones are read back from disk. |
| `node.raft.log_tail_entries` | 100,000 | Entries kept before a snapshot, so a follower that far behind is sent the log, not the snapshot. |
| `node.raft.max_uncommitted_bytes` | 64 MiB | A leader with this much proposed and not yet committed takes nothing more from clients. |
| `node.session_timeout` | 10 s | A session the leader hears nothing from for this long is closed. |
| `node.snapshot_every_entries` | 100,000 | Each node snapshots once this many entries follow its last snapshot. |
| `node.snapshots_kept` | 1 | The newest snapshots each node keeps on disk (`--keep-snapshots`): see [Operations][operations]. |
| `node.egress_ring_entries` | 8192 | Responses kept per session for a client that reattaches (up to `MAX_EGRESS_RING`). |
| `log_fsync` | `Fsync::Never` | `Fsync::EachBatch` is `--fsync`. |
| `admin_token`, `peer_token` | none | See below. |
| `tls` | none | See below. |

```rust,no_run
# use std::io::{self, Read};
# use std::time::Duration;
# use turbofan_cluster::{Ctx, Service, SessionId, SnapshotWriter};
# #[derive(Debug, Default)]
# struct Counter;
# impl Service for Counter {
#     fn on_message(&mut self, _ctx: &mut Ctx, _session: SessionId, _payload: &[u8]) {}
#     fn snapshot(&self) -> SnapshotWriter { Box::new(|_| Ok(())) }
#     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> { Ok(()) }
# }
use turbofan_cluster::driver::{self, Fsync};
use turbofan_cluster::{Member, NodeConfig, NodeId};

#[tokio::main]
async fn main() -> io::Result<()> {
    let ip = |n: u8| format!("10.0.0.{n}");
    let members: Vec<Member> = (1..=3)
        .map(|n| Member {
            id: NodeId(n),
            peer: format!("{}:7001", ip(n)),
            client: format!("{}:7101", ip(n)),
            admin: format!("{}:7201", ip(n)),
            learner: false,
        })
        .collect();
    let id = NodeId(1);
    let mut node = NodeConfig::new(id, members.clone());
    node.session_timeout = Duration::from_secs(30);
    node.snapshot_every_entries = 1_000_000;
    let addr = |s: &str| s.parse().expect("an address");
    let config = driver::Config {
        node,
        service_id: 1,
        data_dir: "/var/lib/my-service".into(),
        log_fsync: Fsync::EachBatch,
        peer_addrs: members.iter().map(|m| (m.id, addr(&m.peer))).collect(),
        client_listen: addr(&members[0].client),
        admin_listen: addr(&members[0].admin),
        admin_token: std::env::var("TURBOFAN_ADMIN_TOKEN").ok().map(String::into_bytes),
        peer_token: std::env::var("TURBOFAN_PEER_TOKEN").ok().map(String::into_bytes),
        tls: None,
    };
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

or `driver::Config::tls`, a `tls::NodeTls` of the same four files. Then:

- **Peers** check each other's certificates against the peer CA, both ways. A node's certificate
  is its client certificate too when it dials a peer, so if it has an extended key usage, that
  must allow both `serverAuth` and `clientAuth`.
- **Operators** present a certificate the admin CA signed: the commands take `--tls-ca <pem>
  --tls-cert <pem> --tls-key <pem>`.
- **Clients** check the node's certificate against the CA they're given, and present none: they
  say who they are by their credentials, as without TLS.

A certificate is checked against the address dialled, so a node's must name, as subject
alternative names, every host it's reached at: its peer, client and admin addresses'.

A peer certificate stands for the cluster, not one node: any node holding one may say it's any
member, as with the peer token. Certificates are read when the node starts, so replacing one is a
rolling restart. A node with TLS and one without can't talk, so a cluster turns TLS on or off all
at once.

Without the `tls` feature the API is the same, but loading any TLS configuration is an error, and
the flags are refused: there's never TLS to run, nor a TLS dependency to license.
