# The Turbofan guide

This guide goes from a first service to what running a cluster in production asks for: clients,
members, operations, upgrades and performance. Each chapter's examples are compiled and run as
tests, so they work as shown. The API reference has the details of each type, and
[ARCHITECTURE.md](https://github.com/jrogers/turbofan/blob/main/docs/ARCHITECTURE.md) how it's built.

1. [A first service][first-service]: a `Service`, the rules that keep it deterministic,
   snapshots, and a one-node cluster with a client, in one program.
2. [Services in depth][services]: sessions and who opens them, timers, proposals of the service's
   own, the limits, and testing a service without a cluster.
3. [Clients][clients]: `ClusterClient`, what it guarantees across a failover, when responses are
   lost, backpressure, and `client::Client` for a caller that does its own IO.
4. [Running a cluster][running]: the command line, the data directory, configuration and its
   defaults, durability, tokens and TLS.
5. [Members][membership]: adding and removing nodes, learners, replacing a dead one, a log in
   doubt, and a standby that takes over a lost cluster.
6. [Operations][operations]: status, snapshots, leadership transfer, rolling restarts, shutting
   down, and metrics.
7. [Upgrades][upgrades]: changing a service's behaviour, and the cluster's protocol, while it
   runs.
8. [Performance][performance]: where the time goes, what to tune, and the benchmark.

[first-service]: first-service.md
[services]: services.md
[clients]: clients.md
[running]: running.md
[membership]: membership.md
[operations]: operations.md
[upgrades]: upgrades.md
[performance]: performance.md
