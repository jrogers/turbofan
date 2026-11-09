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
  hardware. Either way, a write is made on a thread of its own, the node going on meanwhile, so
  a write the disk holds up (Linux throttles writers when too much is waiting to be written back)
  never holds up the node's heartbeats, and with them the cluster's leader.
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
- **The allocator, on Linux.** A message's path allocates buffers of a few kilobytes and frees
  them soon after, which glibc's malloc does slowly: on Linux it costs a third to half of the
  throughput (half the queries'), against jemalloc on the same machine. Run the service on
  jemalloc or mimalloc, preloaded (`LD_PRELOAD=/usr/lib/x86_64-linux-gnu/libjemalloc.so.2`, from
  Debian and Ubuntu's `libjemalloc2`) or linked (`#[global_allocator]`). macOS's own is as quick.
- **The build.** `lto = "fat"` and `codegen-units = 1` in the service's `[profile.release]` give
  10 to 18% more throughput, for a slower build; the profile is the service's own, not inherited
  from turbofan's.

## The benchmark

```sh
cargo bench -p turbofan --bench cluster
cargo bench -p turbofan --bench cluster --features tls
```

It runs three nodes on one machine, over real TCP and files, with and without fsync (and with
`--features tls`, over TLS too). For each, one client with one message in flight, for latency,
then several clients with many in flight, for throughput: messages per second, and latency from
p50 to the maximum. Then, without fsync, which they don't touch, queries the same two ways: they
cost a round trip to a quorum but no write, so they're the reads a service can make without the
log. A second table splits the messages' time between persisted, committed and applied.
`BENCH_SECS` sets how long each run measures (10 s by default), and
`BENCH_THROUGHPUT=<clients>x<in flight>` the throughput run's shape (`8x256` by default).

First come the messages' two runs on the core alone, `in memory`: the cluster a service's tests
use (`turbofan::testing`), its nodes and clients on one thread, no sockets or files, its network
and writes taking none of its time, timed by the wall's clock. That's what openraft's benchmark
measures, and the gap to the runs over TCP is what the driver costs: the system calls, the
loopback and the tasks' wake-ups. The in-memory cluster hands its nodes each input on its own,
where the driver hands them a batch, so it's measured with the same clients sending batches, not
with many clients sending one each.

Three nodes on one machine share its disk and CPUs, and talk over loopback, so the numbers are
for comparing changes and configurations, not a promise for a real cluster: measure your own.
CI runs it on every push, and its summary has each run's table.

To measure a real cluster, run the benchmark's service as nodes of their own (`examples/echo.rs`,
with `serve`'s usual flags), and the benchmark on another machine, driving them:

```sh
cargo build --release -p turbofan --example echo   # then, on each node's machine:
target/release/examples/echo serve --id 1 --data ./n1 --peers ... --clients ... --admin ... [--fsync]
# and on the clients' machine:
BENCH_CLUSTER=10.0.0.1:7101,10.0.0.2:7101,10.0.0.3:7101 cargo bench -p turbofan --bench cluster
```

`BENCH_FSYNC=1` says the nodes run with `--fsync`, and `BENCH_TLS_CA=<pem>` that they're over
TLS, signed by that CA. It runs only what the nodes are configured for, and not the second table,
which comes from the nodes' own metrics.

## Results

On an Apple M4 Pro Mac mini: 12 cores, 24 GB, its internal SSD, macOS 27.2, Rust 1.98.1, at
commit `93195a5` (0.10.0), November 2026. Each figure is the median of three runs of 10 s,
`--features tls`.

| run | clients × in flight | msgs/s | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|
| latency, in memory | 1 × 1 | 433,625 | 2 µs | 2 µs | 2 µs |
| throughput, in memory | 8 × 256 | 499,097 | 3.83 ms | 11.98 ms | 12.82 ms |
| latency | 1 × 1 | 14,956 | 64 µs | 101 µs | 131 µs |
| throughput | 8 × 256 | 1,198,294 | 894 µs | 7.95 ms | 10.73 ms |
| query latency | 1 × 1 | 22,650 | 44 µs | 61 µs | 109 µs |
| query throughput | 8 × 256 | 2,830,641 | 706 µs | 1.17 ms | 1.43 ms |
| latency, fsync | 1 × 1 | 125 | 7.84 ms | 14.02 ms | 16.10 ms |
| throughput, fsync | 8 × 256 | 114,939 | 17.76 ms | 31.96 ms | 38.17 ms |
| latency, TLS | 1 × 1 | 14,436 | 66 µs | 105 µs | 137 µs |
| throughput, TLS | 8 × 256 | 1,170,253 | 919 µs | 8.09 ms | 11.17 ms |
| query latency, TLS | 1 × 1 | 22,713 | 43 µs | 61 µs | 112 µs |
| query throughput, TLS | 8 × 256 | 2,721,277 | 735 µs | 1.21 ms | 1.48 ms |
| latency, TLS, fsync | 1 × 1 | 128 | 7.89 ms | 13.97 ms | 16.09 ms |
| throughput, TLS, fsync | 8 × 256 | 113,730 | 17.94 ms | 32.29 ms | 38.20 ms |

Without fsync, a message is committed in tens of microseconds, and no one task is busy: the
throughput run is bound by its own window, 2,048 messages in flight over a round trip of about a
millisecond. More in flight goes faster (at `cf027a9`): 1,375,000 a second with 8 clients of 1,024
(`BENCH_THROUGHPUT=8x1024`), and 1,145,000 with 64 of 256. TLS costs little, its connections
buffered as plain TCP's are. With fsync, the disk is what limits it: on macOS a sync is
`F_FULLFSYNC`, which empties the drive's own cache, some milliseconds each, and its runs vary
the most from one to the next. Group commit is why throughput with fsync is still over 100,000 a
second: each sync covers every message that arrived during the one before. A Linux server's NVMe
drive syncs much faster (below); measure yours.

A query skips the write, so one on its own takes two thirds of a message's time, and with many in
flight they go more than twice as fast as messages: 2.8 million a second.

The core alone, the `in memory` runs: 433,625 messages a second one at a time, a commit through
all three nodes in 2 µs at p50, and 499,097 a second at 8 × 256, on its one thread (openraft's
benchmark, on a Mac: 33,000 one at a time, 3.5 million with 4,096 clients on several). So without
fsync, nearly all of a message's 64 µs on its own over TCP is the driver's and the loopback's,
not the core's.

### On Linux servers

Three nodes on machines of their own, and the benchmark driving them from a fourth
(`BENCH_CLUSTER`), on three AWS types of 8 vCPUs with local NVMe instance storage, one cluster
placement group in us-east-1, Linux 7.0, Rust 1.99.1, at commit `07ea8ba`, November 2026: fat LTO,
nodes and clients on jemalloc (see the allocator, above), with `scripts/aws-bench.sh`. `c8id` and
`c9gd` are the median of three runs of 10 s, each on instances of its own; `c6id` one run, at
`f50fb3d`. The nodes' logs on the instance storage, except the EBS row, on the root volume (gp3).

| run, nodes apart | `c6id`, Xeon Ice Lake | `c8id`, Xeon Granite Rapids | `c9gd`, Graviton5 |
|---|---:|---:|---:|
| latency, 1 × 1 (p50) | 232 µs | 326 µs | 130 µs |
| throughput, 8 × 256 | 1,083,686 | 1,285,232 | 1,535,851 |
| query throughput | 2,135,321 | 2,356,813 | 3,142,472 |
| latency, fsync (p50) | 340 µs | 450 µs | 134 µs |
| throughput, fsync | 971,121 | 1,130,939 | 1,403,091 |
| throughput, TLS | 1,093,138 | 1,273,055 | 1,547,213 |
| throughput, TLS, fsync | 946,607 | 1,139,758 | 1,414,762 |
| throughput, fsync, EBS | 260,191 | 223,386 | 281,898 |

Throughput, messages and queries a second, varies by a few per cent from one run to the next. A
message on its own doesn't: it's a round trip over the network, client to leader and leader to
follower, and that depends on where the placement group lands. On `c9gd` its p50 was 83 µs, 130 µs and
261 µs in the three runs, steady within each; on `c8id`, 326 µs every time, with a long tail
(p99 642 µs). The Mac's 64 µs is over loopback.

A sync on the instances' NVMe drives takes about a tenth of a millisecond, where the Mac's
`F_FULLFSYNC` took milliseconds: fsync costs a tenth of the throughput, where on the Mac it cost
nine tenths. On `c9gd` it adds next to nothing to a message on its own, the leader's sync
overlapping its round trip to a follower. On EBS, network storage, a sync is a round trip of its
own, some milliseconds: throughput with fsync falls to a fifth, group commit still filling each
sync. TLS costs next to nothing.

The same benchmark in one process on the clients' machine, the three nodes and the clients sharing
its 8 vCPUs, as on the Mac (12 cores):

| run, one machine | `c6id` | `c8id` | `c9gd` | Mac mini, M4 Pro |
|---|---:|---:|---:|---:|
| throughput, in memory | 213,606 | 318,156 | 379,289 | 499,097 |
| latency, 1 × 1 (p50) | 82 µs | 75 µs | 40 µs | 64 µs |
| throughput, 8 × 256 | 791,108 | 1,130,083 | 1,558,024 | 1,198,294 |
| query throughput | 1,871,507 | 2,390,634 | 3,560,196 | 2,830,641 |
| latency, fsync (p50) | 194 µs | 207 µs | 83 µs | 7.84 ms |

The core alone, `in memory`, on one thread, follows each CPU's speed on one core: Graviton5 is three
quarters of the M4's, the newest Xeon two thirds, Ice Lake under half. Over TCP Graviton5 passes
the Mac, Linux's loopback and wake-ups being cheaper than macOS's. With the clients on a machine
of their own, the Xeons' nodes go faster again; Graviton5's don't, already as fast sharing it.

On glibc's malloc and without LTO (`c6id`, at `ee40689`), the cluster did 699,316 messages a second
nodes apart, and 1,024,529 queries: the allocator and the build are half again the throughput, and
twice the queries'.

[clients]: clients.md
[operations]: operations.md
[running]: running.md
