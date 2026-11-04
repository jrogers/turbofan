# turbofan-cluster roadmap

What's left before the framework is fit for production, roughly in order. Each item names where it
lives today, in `crates/turbofan-cluster`. Items marked `ponytail:` in the code are known ceilings, taken on purpose: pay them
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
- **Capture and replay.** Copy a cluster's log, from a snapshot on, and replay it into a service
  offline: to reproduce a bug from production, check a new service version against real traffic
  (its responses compared with what the cluster sent), or audit what happened. Applying is
  already deterministic, with time in the entries (`src/node/apply.rs`), and a snapshot copied
  off a node restores (`driver::restore`). Missing: capturing the committed entries from a node's
  segments (`src/log.rs`) or a learner, as a file that names its snapshot and epoch; and a replay
  that restores the snapshot and applies the entries through `Node` without Raft, with the
  service's outputs to compare.

## 2. Toward 1.0

- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`. The crates are
  on crates.io from 0.1.0, but until 1.0 a minor version may break the API (CHANGELOG.md says how).

## 3. Docs and outreach

- **Benchmark numbers** in the guide's performance chapter and on the website's landing page,
  measured somewhere steadier than a laptop.
- **Announcement posts.** Posts introducing Turbofan where Rust and trading-systems developers
  read, linking to the website and guide. Wait for the benchmark numbers.

## Not planned

- **Reads without the log.** A query goes through the log like any message. Lease or ReadIndex
  reads would be faster but add a second path through the service; add them if a service needs
  read throughput the log can't give.
- **Several services per cluster.** Run one cluster per service.
