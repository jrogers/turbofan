# turbofan-cluster roadmap

What's left before the framework is fit for production, roughly in order. Each item names where it
lives today. Items marked `ponytail:` in the code are known ceilings, taken on purpose: pay them
down when they show.

## 1. Operating a cluster

Members can be added and removed while the cluster runs (`add-member`, `remove-member`, at most
`MAX_MEMBERS` = 7), the leadership handed over (`transfer-leadership`), and the cluster shut
down (`shutdown`), and each member's progress shown (`status`); otherwise the admin operations
are a snapshot. Nodes report through the
`metrics` crate (`src/metrics.rs`).

- **Security.** TLS is optional (`src/tls.rs`, the `tls` feature): with it, all three listeners are encrypted, and
  peers and operators are authenticated by certificate, against a peer CA and an admin CA. Left:
  a peer certificate stands for the whole cluster, not one node ID; certificates load at start,
  so rotating one is a rolling restart; and a cluster turns TLS on or off all at once, since a
  node with it and one without can't talk.
- **Backup.** Archive snapshots and closed log segments somewhere off the node, and restore a
  cluster from them.
- **Durability.** The log isn't fsynced by default (`Fsync::Never`, `src/log.rs`): the other
  nodes' copies are the redundancy, so a crashed process or a lost machine loses nothing, but a
  majority losing power together may lose committed entries. Spread the nodes over power
  domains, and cover a whole site with a standby (below) or `serve --fsync`.
- **A standby in another region.** A cluster that can take over, in another region or cloud, if
  the main one is lost. Learners are in: members that take the log and snapshot but don't vote,
  so the main cluster's commits never wait on the other region (`add-member --learner`, or
  `serve --learners` to start with some). So is a cluster epoch: a new cluster's first leader
  names one in the log (an `Epoch` entry, kept in snapshots), every hello carries it, and peers
  that know different ones don't talk. And promotion: with the main cluster lost, `promote`
  on a learner makes it the one member of a new cluster of a new epoch, from its log as it is
  (what it holds past its commit index commits), to grow with `add-member`; the old cluster's
  nodes, back, can't talk to it. Refused while the learner hears its leader, but never
  automatic: from the other region a dead cluster and a cut link look alike, and two clusters
  taking orders fill twice, so an operator fences the old one first. Nothing waits for a learner
  to catch up, but `status` shows how far behind it is, and `add-member --learner --wait <n>`
  returns once it's within `n` entries. Left:
  - *A log tail past the snapshot:* a snapshot trims the log to its index (`on_snapshot_saved`,
    `src/raft/snapshots.rs`), so a follower one entry behind it is sent the whole snapshot: over
    a WAN, a learner's brief cut costs a full transfer. Keep so many entries past it, and send a
    follower within them the log instead. Compaction drops whole segments, so most of the tail is
    on disk already.
  - *A member's role changed in place:* a learner made a voter, or a voter a learner, is one
    `Members` entry that adds or removes one voter, safe as a single-server change once the
    learner has caught up. Today it's `remove-member` then `add-member`, which ships the whole log
    again.
  - *Clients:* the hello tells them the epoch, which they don't check yet. Sessions are
    replicated, so clients reattach to the promoted cluster and resend what it hasn't applied.
    What committed but never reached the learner is lost, and clients can't always tell: a
    response they'd seen means its `egressSeq` is reused, and the new response is dropped as
    seen. An operator can count the loss from the old leader's commit index and the learner's
    last index.

  A cluster stretched over three regions would fail over by itself and lose nothing, but every
  commit would wait a round trip between regions.

## 2. The service API

A service upgrades by `Service::version` and `activate`, and the cluster's protocol changes by
`WIRE.md`'s rules.

## 3. Toward 1.0

- **Benchmark regressions.** CI runs `benches/cluster.rs` (throughput, and latency to p99.9, of
  three nodes over real TCP and disks) into each run's summary, but nothing compares it with
  main's last run. Keep main's results and flag a run well below them; shared runners are noisy,
  so only by a wide margin. And time each entry's stages, not only proposed to committed
  (`commit_latencies`): persisted, committed, applied, answered, so a regression says where.
- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`, and publish to
  crates.io.

## 4. Docs and outreach

What Turbojet did for its own launch (its `site/` and `docs/guide/`), done again here.

- **A guide.** The crate README is an overview and the crate docs are reference material, with
  no examples in them. Wanted: a guide that goes from a first service (`examples/counter.rs`) to
  a running cluster, and then to operating it: members, snapshots, TLS, learners and promotion,
  and metrics. Also examples in the crate docs for `Service`, `Ctx` and `ClusterClient`.
- **A public website.** A landing page, the guide and ARCHITECTURE.md as a book, the benchmark
  numbers, and links to the crates and the repository. Build it from the repository's own docs,
  as Turbojet's `site/build.py` does, so it can't drift from them, with `llms.txt` beside it.
- **Announcement posts.** Posts introducing Turbofan where Rust and trading-systems developers
  read, linking to the website and guide. Wait until those are up, and until the crates are on
  crates.io (section 3), so a reader can `cargo add` it.

## Not planned

- **Reads without the log.** A query goes through the log like any message. Lease or ReadIndex
  reads would be faster but add a second path through the service; add them if a service needs
  read throughput the log can't give.
- **Several services per cluster.** Run one cluster per service.
