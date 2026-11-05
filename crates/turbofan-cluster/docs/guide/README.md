# The Turbofan guide

This guide goes from a first service to what running a cluster in production asks for: clients,
members, operations, upgrades and performance. Each chapter's examples are compiled and run as
tests, so they work as shown. The API reference has the details of each type, and
[ARCHITECTURE.md](https://github.com/jrogers/turbofan/blob/main/docs/ARCHITECTURE.md) how it's built.

1. [What it's for][use-cases]: the shape of system it fits, replacing a stack of coordination
   service, log and database, trading systems (sequencers, matching engines, order management),
   and when it isn't the right tool.
2. [A first service][first-service]: a `Service`, the rules that keep it deterministic,
   snapshots, and a one-node cluster with a client, in one program.
3. [Services in depth][services]: sessions and who opens them, timers, proposals of the service's
   own, the limits, and testing a service, alone or on a cluster in memory.
4. [Clients][clients]: `ClusterClient`, what it guarantees across a failover, when responses are
   lost, backpressure, and `client::Client` for a caller that does its own IO.
5. [Running a cluster][running]: the command line, the data directory, configuration and its
   defaults, durability, tokens and TLS.
6. [Members][membership]: adding and removing nodes, learners, replacing a dead one, a log in
   doubt, and a standby that takes over a lost cluster.
7. [Operations][operations]: status, snapshots, leadership transfer, rolling restarts, shutting
   down, and metrics.
8. [Upgrades][upgrades]: changing a service's behaviour, and the cluster's protocol, while it
   runs.
9. [Performance][performance]: where the time goes, what to tune, and the benchmark.

[use-cases]: use-cases.md
[first-service]: first-service.md
[services]: services.md
[clients]: clients.md
[running]: running.md
[membership]: membership.md
[operations]: operations.md
[upgrades]: upgrades.md
[performance]: performance.md
