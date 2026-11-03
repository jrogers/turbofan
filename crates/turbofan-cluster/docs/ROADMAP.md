# turbofan-cluster roadmap

What's left before the framework is fit for production, roughly in order. Each item names where it
lives today. Items marked `ponytail:` in the code are known ceilings, taken on purpose: pay them
down when they show.

## 1. Operating a cluster

Members can be added and removed while the cluster runs (`add-member`, `remove-member`, at most
`MAX_MEMBERS` = 7), the leadership handed over (`transfer-leadership`), and the cluster shut
down (`shutdown`); otherwise the admin operations are a snapshot. Nodes report through the
`metrics` crate (`src/metrics.rs`).

- **Security.** TLS is optional (`src/tls.rs`, the `tls` feature): with it, all three listeners are encrypted, and
  peers and operators are authenticated by certificate, against a peer CA and an admin CA. Left:
  a peer certificate stands for the whole cluster, not one node ID; certificates load at start,
  so rotating one is a rolling restart; and a cluster turns TLS on or off all at once, since a
  node with it and one without can't talk.
- **Backup.** Archive snapshots and closed log segments somewhere off the node, and restore a
  cluster from them.
- **A standby in another region.** A cluster that can take over, in another region or cloud, if
  the main one is lost. Learners are in: `add-member --learner` adds a member that takes the log and snapshots but doesn't vote, so the main cluster's
  commits never wait on the other region. Left, in order:
  - *Promotion:* an admin `promote` on a learner makes it the one member of a new cluster, from
    its log and snapshot as they are, to grow with `add-member`. Never automatic: from the other
    region a dead cluster and a cut link look alike, and two clusters taking orders fill twice.
    An operator fences the old one first.
  - *A cluster epoch:* the hello carries the service ID and node ID only, so a node of the old
    cluster coming back would talk to the promoted one. Promotion mints a new epoch that peers
    and clients check.
  - *Clients:* sessions are replicated, so clients reattach to the promoted cluster and resend
    what it hasn't applied. What committed but never reached the learner is lost, and clients
    can't always tell: a response they'd seen means its `egressSeq` is reused, and the new
    response is dropped as seen. An operator can count the loss from the old leader's commit
    index and the learner's last index.

  A cluster stretched over three regions would fail over by itself and lose nothing, but every
  commit would wait a round trip between regions.

## 2. The service API

A service upgrades by `Service::version` and `activate`, and the cluster's protocol changes by
`WIRE.md`'s rules.

## 3. Toward 1.0

- **Benchmark regressions.** CI runs `benches/cluster.rs` (throughput, and latency to p99.9, of
  three nodes over real TCP and disks) into each run's summary, but nothing compares it with
  main's last run. Keep main's results and flag a run well below them; shared runners are noisy,
  so only by a wide margin.
- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`, and publish to
  crates.io.

## Not planned

- **Reads without the log.** A query goes through the log like any message. Lease or ReadIndex
  reads would be faster but add a second path through the service; add them if a service needs
  read throughput the log can't give.
- **Several services per cluster.** Run one cluster per service.
