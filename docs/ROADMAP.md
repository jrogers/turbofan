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

## 2. Toward 1.0

- **A stable API.** Freeze `Service`, `Ctx`, `ClusterClient` and `driver::Config`. The crates are
  on crates.io from 0.1.0, but until 1.0 a minor version may break the API (CHANGELOG.md says how).
- **A cluster in memory, for a service's tests.** `turbofan_cluster::testing` (the `testing`
  feature) runs a service's nodes in one thread, in memory, on the test's clock. What's left: the
  OMS's failover tests moved onto it (`../oms/tests/failover.rs` starts real nodes over TCP);
  changes of members and upgrades in it, which it doesn't do yet; and its API settled, to freeze
  with the rest. It hands out `Node`, which isn't part of the API: a test should need only what
  `Cluster` says of a node.
- **Faults the simulation doesn't reach.** The simulation (`tests/sim.rs`) runs the node and the
  pipeline, not the driver, the client or TLS, which only the cluster tests and the soak
  (`tests/soak.rs`) run, over real TCP. The soak kills nodes, and through a proxy between them
  (`tests/common/proxy.rs`) cuts, stalls and slows the links between them under load. What's left:
  faults between clients and nodes, which the proxy doesn't stand between; the soak over TLS,
  where the proxy can't read a hello to tell one link from another; and the driver's IO in the
  simulation, the thorough way.
- **Linearizability, checked across clients.** The simulation checks each session on its own:
  every message and response once, in order, and every node with the same state. Nothing checks
  real-time order between clients: a message answered before another was sent, from any client,
  must come before it in the log, or a stale leader answering, or a response ahead of its
  commit, goes unseen. The simulation knows the log's order and owns the clock, so it's an
  assertion on what it already has, with none of the search Jepsen's checkers (Knossos, Elle)
  need to find an order.
- **Clock skew.** Every node in the simulation reads the same clock. The cluster's time is the
  leader's, as it logged it, held still when a new leader's is behind, and the driver slews its
  clock toward the wall clock; timers and session timeouts ride on that through failovers. Give
  each simulated node an offset and a drift of its own, and jumps, as NTP and a VM resuming make.
- **A node paused, then resumed.** A long garbage collection or a VM stalled freezes a whole
  node, which then resumes with a stale view of the cluster: a leader that doesn't know it's
  been replaced, its clock jumped. The simulation stalls disks and delays messages, but never
  freezes a node and lets it resume. Model it there, and in the soak by holding a node's task.
- **Jepsen, once, before 1.0.** The production binaries as processes on machines of their own,
  faulted at the operating system (iptables, tc, `SIGKILL`, `SIGSTOP`) and checked black-box:
  what in-process tests can't reach, the driver, the command line and a real fsync among it, and
  the check trading systems' developers trust. An audit at 1.0 rather than CI, which a cluster
  of VMs and a Clojure harness would make costly to keep.

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
- **Fewer hops between a client and the leader.** Of a quiet cluster's 65 µs a message, about 25
  are tasks waking each other: a connection's reader and writer are tasks of their own, at both
  ends, so a peer slow to read never stops one writing, nor one slow to write one reading. About
  10 more are a write handed to a thread and back, on the leader and on the follower whose ack
  commits it, so a write the disk holds up never holds up the node. Fewer would cost those.
