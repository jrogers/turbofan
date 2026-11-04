# Performance

## Where the time goes

A message's path through the cluster:

1. The client sends it to the leader.
2. The leader appends it to its log, and sends it to the followers at once, while it writes it to
   its own disk.
3. Each follower writes it, and acknowledges.
4. Once a majority, the leader among them, has it durable, it's committed: the leader applies it
   to the service, and sends the response.

So the latency of a message is a round trip from client to leader, plus the slower of the
leader's write and a round trip to the nearest follower and its write, plus the service's time to
apply it. The leader's `turbofan_persist_latency_seconds`, `turbofan_commit_latency_seconds` and
`turbofan_apply_latency_seconds` split it (see [Operations][operations]).

Throughput comes from batching. While a write is under way, the node goes on taking messages, and
writes everything that arrived meanwhile in one batch, with one sync if it syncs at all: group
commit. The busier the cluster, the bigger the batches.

## What to tune

- **Network.** The commit waits for a round trip to a follower, so put the voters close together:
  the same region, ideally the same data centre over separate power. A standby elsewhere is a
  learner, which commits don't wait for.
- **fsync.** Without `--fsync` (the default), a write is the page cache's, and the follower's
  copy is the redundancy (see [Running a cluster][running]). With it, every batch waits for the
  disk: on a fast NVMe drive a little, on network storage a lot. Choose with the benchmark on your
  hardware.
- **The service.** Every message is applied on every node, one at a time, on the node's one task.
  A slow callback holds up everything after it. Keep callbacks to the state change and the
  responses; work that can wait goes in a proposal of its own, and work outside the state machine
  goes in the gateway.
- **Snapshots.** `snapshot` must be quick, since the node waits for it to return; the writing
  happens on another thread. A clone of a big state is a pause: persistent data structures, which
  share most of their memory with the live state, avoid it.
- **Clients in flight.** One client with one message in flight measures latency, not throughput.
  A gateway sends without waiting for each response, up to `client::MAX_UNACKED`, and several
  gateways keep the leader's batches full.
- **TLS** costs encryption on every connection, including the followers'. The benchmark measures
  it too.

## The benchmark

```sh
cargo bench -p turbofan-cluster --bench cluster
cargo bench -p turbofan-cluster --bench cluster --features tls
```

It runs three nodes on one machine, over real TCP and files, with and without fsync (and with
`--features tls`, over TLS too). For each, one client with one message in flight, for latency,
then several clients with many in flight, for throughput: messages per second, and latency from
p50 to the maximum. A second table splits the time between persisted, committed and applied.
`BENCH_SECS` sets how long each run measures (10 s by default).

Three nodes on one machine share its disk and CPUs, and talk over loopback, so the numbers are
for comparing changes and configurations, not a promise for a real cluster: measure your own.
CI runs it on every push, and its summary has each run's table.

## Results

On an Apple M4 Pro Mac mini: 12 cores, 24 GB, its internal SSD, macOS 27.2, Rust 1.98.1, at
commit `d381a58` (after 0.3.0), November 2026. Each figure is the median of three runs of 10 s,
`--features tls`.

| run | clients × in flight | msgs/s | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|
| latency | 1 × 1 | 14,633 | 66 µs | 105 µs | 139 µs |
| throughput | 8 × 256 | 469,445 | 4.00 ms | 10.09 ms | 13.18 ms |
| latency, fsync | 1 × 1 | 131 | 7.15 ms | 14.20 ms | 16.00 ms |
| throughput, fsync | 8 × 256 | 113,482 | 17.70 ms | 28.52 ms | 38.08 ms |
| latency, TLS | 1 × 1 | 13,979 | 69 µs | 108 µs | 143 µs |
| throughput, TLS | 8 × 256 | 239,577 | 8.47 ms | 11.90 ms | 15.39 ms |
| latency, TLS, fsync | 1 × 1 | 128 | 7.06 ms | 14.03 ms | 16.08 ms |
| throughput, TLS, fsync | 8 × 256 | 110,617 | 18.08 ms | 30.09 ms | 35.45 ms |

Without fsync, a message is committed in tens of microseconds, and the leader's one task is what
limits throughput. With it, the disk is: on macOS a sync is `F_FULLFSYNC`, which empties the
drive's own cache, some milliseconds each. Group commit is why throughput with fsync is still
over 100,000 a second: each sync covers every message that arrived during the one before. A
Linux server's NVMe drive, with power-loss protection, syncs much faster; measure yours.
