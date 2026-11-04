# turbofan-cluster roadmap

What's left before the framework is fit for production, roughly in order. Each item names where it
lives today. Items marked `ponytail:` in the code are known ceilings, taken on purpose: pay them
down when they show.

## 1. Operating a cluster

- **Security.** TLS (`src/tls.rs`, the `tls` feature): a peer certificate stands for the whole
  cluster, not one node ID; certificates load at start, so rotating one is a rolling restart; and
  a cluster turns TLS on or off all at once, since a node with it and one without can't talk.
- **Durability.** The log isn't fsynced by default (`Fsync::Never`, `src/log.rs`): the other
  nodes' copies are the redundancy, so a crashed process or a lost machine loses nothing, but a
  majority losing power together may lose committed entries. Spread the nodes over power
  domains, and cover a whole site with a standby (a learner in another region, to `promote`) or
  `serve --fsync`.

## 2. Known issues

From a review of the crate, still open:

- **A power cut mid-batch can leave a log that won't open.** A batch is one write then a sync,
  and the disk may keep a later page of it and lose an earlier one. `open` takes whole records
  after damage for synced data rotted, not a torn write, and refuses to start (`whole_record_in`,
  `src/log.rs`). Unsynced (`Fsync::Never`), a segment isn't synced when the next starts, so a power
  cut can leave a gap between them, refused too. Nothing wrong is served, but the node needs
  wiping. A fix: know how far the log is synced, and take damage past it for a torn write; or,
  unsynced, truncate at damage and doubt the log, which that mode already allows.
- **Admin requests answered once.** A request sent again while it's under way is answered on the
  new connection; one that finished first isn't remembered, so a member change retried after it
  committed is refused, though it was made (`src/node/admin.rs`).
- **The egress ring is each node's setting.** `egress_ring_entries` is replicated state sized by
  local config: members with different values replay different responses to a client after a
  failover. It could be logged, as a version is activated.

## 3. Toward 1.0

- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`. The crates are
  on crates.io from 0.1.0, but until 1.0 a minor version may break the API (CHANGELOG.md says how).

## 4. Docs and outreach

- **Benchmark numbers** in the guide's performance chapter and on the website's landing page,
  measured somewhere steadier than a laptop.
- **Announcement posts.** Posts introducing Turbofan where Rust and trading-systems developers
  read, linking to the website and guide. Wait for the benchmark numbers.

## Not planned

- **Reads without the log.** A query goes through the log like any message. Lease or ReadIndex
  reads would be faster but add a second path through the service; add them if a service needs
  read throughput the log can't give.
- **Several services per cluster.** Run one cluster per service.
