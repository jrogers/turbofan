# turbofan-cluster roadmap

What's left before the framework is fit for production, roughly in order. Each item names where it
lives today, in `crates/turbofan-cluster`. Items marked `ponytail:` in the code are known ceilings, taken on purpose: pay them
down when they show.

## 1. Operating a cluster

- **Security.** TLS (`src/tls.rs`, the `tls` feature): a peer certificate stands for the whole
  cluster, not one node ID, unless `--tls-peer-ids`; and
  a cluster turns TLS on or off all at once, since a node with it and one without can't talk.
- **Durability.** The log isn't fsynced by default (`Fsync::Never`, `src/log.rs`): the other
  nodes' copies are the redundancy, so a crashed process or a lost machine loses nothing, but a
  majority losing power together may lose committed entries. Spread the nodes over power
  domains, and cover a whole site with a standby (a learner in another region, to `promote`) or
  `serve --fsync`.
- **A slow write.** Without fsync, a node writes a few appends on its own task, a copy to the page
  cache (`writes_inline`, `src/driver.rs`), which is quicker than handing them to a thread. But a
  write the kernel holds up (Linux throttles writers when too much is dirty) holds up the node's
  heartbeats with it: one past the election timeout is an election. Hand writes to a thread for
  a while after one is slow, or make writing on the task a setting.

## 2. Toward 1.0

- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`. The crates are
  on crates.io from 0.1.0, but until 1.0 a minor version may break the API (CHANGELOG.md says how).
- **A cluster in memory, for a service's tests.** A service is tested on real nodes: the OMS's
  tests start them over TCP, in temporary directories, on ports picked so they don't collide
  (`../oms/tests/common/mod.rs`). A `testing` module could run one to three nodes in one process,
  the log in memory, the network channels and the clock the test's: quick, the same every run, and
  a failover or a killed node one call, with no sockets to wait on. The simulation (`tests/sim.rs`)
  drives the node and the pipeline this way already; what's left is making it public, an API to
  freeze with the rest.
- **Faults the simulation doesn't reach.** The simulation (`tests/sim.rs`) runs the node and the
  pipeline, not the driver, the client or TLS, which only the cluster tests and the soak
  (`tests/soak.rs`) run, over real TCP. The soak kills nodes; nothing yet partitions them, or
  stalls, slows or drops what a connection carries under load. A proxy between the soak's nodes
  that does is the cheaper start, the driver's IO in the simulation the thorough one; and the soak
  over TLS too.

## 3. Docs and outreach

- **Benchmark numbers from a Linux server**, beside the Mac mini's in the guide's performance
  chapter: macOS's `F_FULLFSYNC` says little of fsync on a server's NVMe drive. With the clients
  on another machine: the benchmark runs them in the nodes' process, where they take the CPUs the
  nodes would, and a node's own ceiling is hidden behind theirs. And a run of the core alone, on
  the cluster in memory (above): no sockets or files, as openraft's benchmark measures (33,000
  a second one at a time, 3.5 million with 4,096 clients, on a Mac). Its gap to the TCP run is the
  driver's cost, and it's the number to set beside theirs.
- **Announcement posts.** Posts introducing Turbofan where Rust and trading-systems developers
  read, linking to the website and guide.

## Not planned

- **Reads without the log.** A query goes through the log like any message. Lease or ReadIndex
  reads would be faster but add a second path through the service; add them if a service needs
  read throughput the log can't give.
- **Several services per cluster.** Run one cluster per service.
- **Fewer hops between a client and the leader.** Of a quiet cluster's 56 µs a message, about 25
  are tasks waking each other: a connection's reader and writer are tasks of their own, at both
  ends, so a peer slow to read never stops one writing, nor one slow to write one reading. Fewer
  would cost that.
