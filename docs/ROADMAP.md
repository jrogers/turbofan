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

## 2. Toward 1.0

- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`. The crates are
  on crates.io from 0.1.0, but until 1.0 a minor version may break the API (CHANGELOG.md says how).

## 3. Docs and outreach

- **Benchmark numbers from a Linux server**, beside the Mac mini's in the guide's performance
  chapter: macOS's `F_FULLFSYNC` says little of fsync on a server's NVMe drive.
- **Announcement posts.** Posts introducing Turbofan where Rust and trading-systems developers
  read, linking to the website and guide.

## Not planned

- **Reads without the log.** A query goes through the log like any message. Lease or ReadIndex
  reads would be faster but add a second path through the service; add them if a service needs
  read throughput the log can't give.
- **Several services per cluster.** Run one cluster per service.
