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
