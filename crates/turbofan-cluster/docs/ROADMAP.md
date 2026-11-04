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
  returns once it's within `n` entries. A snapshot keeps a tail of the log before it, so a
  learner briefly cut off is sent the entries it missed rather than the whole snapshot, and
  `set-role` makes a learner a voter, or a voter a learner, in place. Left:
  - *Clients:* sessions are replicated, so clients reattach to the promoted cluster and resend
    what it hasn't applied. A client whose messages or responses the learner lost (`Opened`'s
    `appliedSeq` or `lastEgressSeq` behind what it saw) closes its session,
    `CloseReason::Diverged`, rather than go on with a gap. An operator can count the loss from
    the old leader's commit index and the learner's last index.

  A cluster stretched over three regions would fail over by itself and lose nothing, but every
  commit would wait a round trip between regions.

## 2. The service API

A service upgrades by `Service::version` and `activate`, and the cluster's protocol changes by
`WIRE.md`'s rules.

## 3. Toward 1.0

- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`. The crates are
  on crates.io from 0.1.0, but until 1.0 a minor version may break the API (CHANGELOG.md says how).

## 4. Docs and outreach

What Turbojet did for its own launch (its `site/` and `docs/guide/`), done again here.

- **A guide.** Done: `docs/guide`, in the crate docs as `turbofan_cluster::guide`, with its
  examples run as doctests, and examples on `Service`, `Ctx` and `ClusterClient`.
- **A public website.** Done: a landing page, and the guide and ARCHITECTURE.md as a book,
  built from the repository's docs by `site/build.py`, with `llms.txt` beside them. Left: the
  benchmark's numbers, measured somewhere steadier than a laptop.
- **Announcement posts.** Posts introducing Turbofan where Rust and trading-systems developers
  read, linking to the website and guide. Wait until those are up.

## Not planned

- **Reads without the log.** A query goes through the log like any message. Lease or ReadIndex
  reads would be faster but add a second path through the service; add them if a service needs
  read throughput the log can't give.
- **Several services per cluster.** Run one cluster per service.
