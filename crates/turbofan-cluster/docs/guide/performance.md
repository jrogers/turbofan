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
  hardware. Without it, a node writes a few appends on its own task, a copy to the page cache,
  rather than handing them to a thread and back, which took longer than the write: a quiet
  cluster's commits are the quicker for it.
- **The service.** Every message is applied on every node, one at a time, on the node's one task.
  A slow callback holds up everything after it. Keep callbacks to the state change and the
  responses; work that can wait goes in a proposal of its own, and work outside the state machine
  goes in the gateway.
- **Snapshots.** `snapshot` must be quick, since the node waits for it to return; the writing
  happens on another thread. A clone of a big state is a pause: persistent data structures, which
  share most of their memory with the live state, avoid it.
- **Clients in flight.** One client with one message in flight measures latency, not throughput.
  A gateway sends without waiting for each response, up to `client::MAX_UNACKED`, and several
  gateways keep the leader's batches full. With many to send at once, `send_batch` hands them to
  the client's task in one call, written together, rather than one each (see [Clients][clients]).
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
`BENCH_SECS` sets how long each run measures (10 s by default), and
`BENCH_THROUGHPUT=<clients>x<in flight>` the throughput run's shape (`8x256` by default).

Three nodes on one machine share its disk and CPUs, and talk over loopback, so the numbers are
for comparing changes and configurations, not a promise for a real cluster: measure your own.
CI runs it on every push, and its summary has each run's table.

## Results

On an Apple M4 Pro Mac mini: 12 cores, 24 GB, its internal SSD, macOS 27.2, Rust 1.98.1, at
commit `1d63233` (after 0.5.0), November 2026. Each figure is the median of three runs of 10 s,
`--features tls`.

| run | clients × in flight | msgs/s | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|
| latency | 1 × 1 | 17,372 | 56 µs | 82 µs | 140 µs |
| throughput | 8 × 256 | 1,097,041 | 1.05 ms | 7.43 ms | 10.32 ms |
| latency, fsync | 1 × 1 | 120 | 7.99 ms | 14.07 ms | 16.03 ms |
| throughput, fsync | 8 × 256 | 116,590 | 17.23 ms | 29.49 ms | 40.40 ms |
| latency, TLS | 1 × 1 | 16,325 | 59 µs | 88 µs | 150 µs |
| throughput, TLS | 8 × 256 | 1,098,952 | 1.05 ms | 8.04 ms | 11.14 ms |
| latency, TLS, fsync | 1 × 1 | 127 | 7.95 ms | 13.78 ms | 14.97 ms |
| throughput, TLS, fsync | 8 × 256 | 113,910 | 17.84 ms | 29.13 ms | 39.36 ms |

Without fsync, a message is committed in tens of microseconds, and no one task is busy: the
throughput run is bound by its own window, 2,048 messages in flight over a round trip of about a
millisecond. More in flight goes faster: 1,350,000 a second with 8 clients of 1,024
(`BENCH_THROUGHPUT=8x1024`), and 1,130,000 with 64 of 256. TLS costs little, its connections
buffered as plain TCP's are. With fsync, the disk is what limits it: on macOS a sync is
`F_FULLFSYNC`, which empties the drive's own cache, some milliseconds each. Group commit is why
throughput with fsync is still over 100,000 a second: each sync covers every message that arrived
during the one before. A Linux server's NVMe drive, with power-loss protection, syncs much faster;
measure yours.
