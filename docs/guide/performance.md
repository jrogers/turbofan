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
- **The NIC's interrupt moderation.** A NIC holds back its interrupts to take more packets in each,
  which costs a message on its own its wait. On EC2 (the `ena` driver: adaptive on receive, 20 µs,
  and 64 µs on send) a query on its own took a fixed 130 µs on `c9gd`; without moderation, 72 µs,
  and a message 73 µs, against 75. Each packet then interrupts, which costs CPU: messages a second
  were 3% more (synced, 10%), queries 5% fewer. Where a request's latency matters, turn it off on
  each node, at every boot (a systemd unit, say), for the NIC the cluster's traffic goes out of:

  ```sh
  sudo ethtool -C ens5 adaptive-rx off rx-usecs 0 tx-usecs 0   # its name: ip route get <a peer>
  ```

  Measure it with your load. `scripts/aws-bench.sh` does this with `NIC_COALESCE_OFF=1`.
- **fsync.** By default every batch waits for the disk: on a local NVMe drive a little, on
  network storage a lot. With `--no-fsync`, a write is the page cache's, and the follower's copy
  is the redundancy (see [Running a cluster][running]). Choose with the benchmark on your
  hardware. A write that syncs, or a big one, is made on a thread of its own, the node going on
  meanwhile, so a write the disk holds up (Linux throttles writers when too much is waiting to be
  written back) doesn't hold up the node's heartbeats, and with them the cluster's leader. Without
  fsync, a batch of appends up to 4 KiB is written on the node's own task, quicker than waking a
  thread for it; the log is synced behind the writes, every 4 MiB, so little is ever waiting.
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
- **Graviton.** Rust's default for 64-bit Arm Linux calls a function for every atomic operation,
  and for the log's checksum instruction, in case the CPU lacks them. Every Graviton since the
  second has both, so build for them, in the service's `.cargo/config.toml`:

  ```toml
  [target.aarch64-unknown-linux-gnu]
  rustflags = ["-C", "target-feature=+lse,+crc"]
  ```

  On `c9gd` that's 8 to 10% more messages a second over the network. It applies only to that
  target, so x86 and Mac builds are unchanged: x86 has both inline already. A binary built this
  way stops with an illegal instruction on the first Graviton (`a1`). A `RUSTFLAGS` variable, if
  set, replaces these.

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
target/release/examples/echo serve --id 1 --data ./n1 --peers ... --clients ... --admin ... [--no-fsync]
# and on the clients' machine:
BENCH_CLUSTER=10.0.0.1:7101,10.0.0.2:7101,10.0.0.3:7101 cargo bench -p turbofan --bench cluster
```

`BENCH_FSYNC=0` says the nodes run with `--no-fsync`, and `BENCH_TLS_CA=<pem>` that they're over
TLS, signed by that CA. It runs only what the nodes are configured for, and not the second table,
which comes from the nodes' own metrics.

## Results

### On Linux servers

Three nodes on machines of their own, and the benchmark driving them from a fourth
(`BENCH_CLUSTER`), on three AWS types of 8 vCPUs with local NVMe instance storage, one cluster
placement group in us-east-1, Linux 7.0, Rust 1.99.1, at commit `c4d1f5f`, November 2026: fat LTO,
nodes and clients on jemalloc (see the allocator, above), `c9gd` built for its CPU (see Graviton,
above), with `scripts/aws-bench.sh`. `c8id` and `c9gd` are the median of three runs of 10 s, each
on instances of its own; `c6id` one run. The nodes' logs on the instance storage, except the EBS
row, on the root volume (gp3).

| run, nodes apart | `c6id`, Xeon Ice Lake | `c8id`, Xeon Granite Rapids | `c9gd`, Graviton5 |
|---|---:|---:|---:|
| latency, 1 × 1 (p50) | 212 µs | 282 µs | 75 µs |
| throughput, 8 × 256 | 1,300,327 | 1,773,523 | 2,717,578 |
| query throughput | 2,517,234 | 2,834,325 | 4,415,206 |
| latency, fsync (p50) | 355 µs | 442 µs | 135 µs |
| throughput, fsync | 1,254,172 | 1,656,306 | 2,502,024 |
| throughput, TLS | 1,324,590 | 1,816,690 | 2,696,146 |
| throughput, TLS, fsync | 1,339,659 | 1,600,509 | 2,589,416 |
| throughput, fsync, EBS | 261,072 | 225,137 | 278,213 |

On AWS c9gd.2xlarge (Graviton5): every run, nodes apart, the median of three, at `4332231`
(0.12.1), the NICs' interrupt moderation off (see above, `NIC_COALESCE_OFF=1`). The site's landing
page shows these.

| run | clients × in flight | msgs/s | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|
| latency | 1 × 1 | 13,332 | 73 µs | 86 µs | 92 µs |
| throughput | 8 × 256 | 2,794,749 | 641 µs | 2.06 ms | 5.55 ms |
| query latency | 1 × 1 | 13,680 | 72 µs | 82 µs | 90 µs |
| query throughput | 8 × 256 | 4,214,653 | 481 µs | 729 µs | 869 µs |
| latency, fsync | 1 × 1 | 7,560 | 131 µs | 149 µs | 156 µs |
| throughput, fsync | 8 × 256 | 2,754,102 | 686 µs | 2.13 ms | 4.90 ms |
| latency, TLS | 1 × 1 | 12,818 | 77 µs | 86 µs | 92 µs |
| throughput, TLS | 8 × 256 | 2,756,549 | 661 µs | 2.08 ms | 5.61 ms |
| query latency, TLS | 1 × 1 | 13,032 | 76 µs | 85 µs | 90 µs |
| query throughput, TLS | 8 × 256 | 4,222,423 | 477 µs | 744 µs | 893 µs |
| latency, TLS, fsync | 1 × 1 | 7,456 | 133 µs | 152 µs | 158 µs |
| throughput, TLS, fsync | 8 × 256 | 2,793,902 | 674 µs | 2.41 ms | 4.94 ms |

With moderation on, as in the first table, a query on its own took 130 µs (7,658 a second in each
of the three runs) and queries a second were 4,415,206; messages 2,717,578, synced 2,502,024.

Throughput, messages and queries a second, varies by a few per cent from one run to the next. A
message on its own doesn't: it's a round trip over the network, client to leader and leader to
follower, and that depends on where the placement group lands. On `c9gd` its p50 was 75 to 80 µs
in the three runs, all in one zone (at `07ea8ba`, in three placements, 83 µs, 130 µs and
261 µs); on `c8id`, 282 µs at the median, with a long tail (p99 695 µs). The Mac's 53 µs (below)
is over loopback.

A sync on the instances' NVMe drives takes about a tenth of a millisecond, where the Mac's
`F_FULLFSYNC` took milliseconds: fsync costs a tenth of the throughput or less, where on the Mac
it cost nine tenths. On `c9gd` it adds 60 µs to a message on its own, 135 µs against 75: the
leader's sync overlaps its round trip to a follower, but the follower's own sync is in it. On EBS,
network storage, a sync is a round trip of its own, some milliseconds: throughput with fsync falls
to a tenth, group commit still filling each sync. TLS costs next to nothing.

The same benchmark in one process on the clients' machine, the three nodes and the clients sharing
its 8 vCPUs, as on the Mac (12 cores, at `50418c5`, below):

| run, one machine | `c6id` | `c8id` | `c9gd` | Mac mini, M4 Pro |
|---|---:|---:|---:|---:|
| throughput, in memory | 235,520 | 335,052 | 398,336 | 536,371 |
| latency, 1 × 1 (p50) | 51 µs | 49 µs | 29 µs | 53 µs |
| throughput, 8 × 256 | 768,357 | 1,191,734 | 1,474,389 | 1,177,211 |
| query throughput | 2,083,319 | 2,683,653 | 3,923,032 | 3,039,131 |
| latency, fsync (p50) | 195 µs | 209 µs | 84 µs | 8.00 ms |

The core alone, `in memory`, on one thread, follows each CPU's speed on one core: the newest Xeon
does five sixths of Graviton5's, Ice Lake three fifths. Over TCP Graviton5 passes the Mac,
Linux's loopback and wake-ups being cheaper than macOS's. With the clients on a machine of their
own, every type's nodes go faster: the Xeons' half again, Graviton5's nearly twice as fast. Sharing
one machine's CPUs, the nodes take smaller batches at a cost (see `MAX_BATCH_INPUTS` in
`driver.rs`): at `c4d1f5f`, `c9gd` did a few per cent fewer messages a second on one machine than
at `07ea8ba`, and nearly twice as many apart.

On glibc's malloc and without LTO (`c6id`, at `ee40689`), the cluster did 699,316 messages a second
nodes apart, and 1,024,529 queries: the allocator and the build are half again the throughput, and
twice the queries'.

### On a Mac

On an Apple M4 Pro Mac mini: 12 cores, 24 GB, its internal SSD, macOS 27.2, Rust 1.98.1, at
commit `50418c5` (after 0.12.0), November 2026. Each figure is the median of three runs of 10 s,
`--features tls`.

| run | clients × in flight | msgs/s | p50 | p99 | p99.9 |
|---|---|---:|---:|---:|---:|
| latency, in memory | 1 × 1 | 448,573 | 2 µs | 2 µs | 2 µs |
| throughput, in memory | 8 × 256 | 536,371 | 3.64 ms | 9.00 ms | 9.40 ms |
| latency | 1 × 1 | 18,238 | 53 µs | 78 µs | 135 µs |
| throughput | 8 × 256 | 1,177,211 | 867 µs | 8.15 ms | 11.55 ms |
| query latency | 1 × 1 | 22,537 | 44 µs | 61 µs | 109 µs |
| query throughput | 8 × 256 | 3,039,131 | 655 µs | 1.11 ms | 1.34 ms |
| latency, fsync | 1 × 1 | 120 | 8.00 ms | 14.09 ms | 16.78 ms |
| throughput, fsync | 8 × 256 | 115,272 | 17.85 ms | 29.92 ms | 41.33 ms |
| latency, TLS | 1 × 1 | 16,979 | 57 µs | 83 µs | 143 µs |
| throughput, TLS | 8 × 256 | 1,187,430 | 864 µs | 8.40 ms | 12.36 ms |
| query latency, TLS | 1 × 1 | 22,732 | 43 µs | 62 µs | 115 µs |
| query throughput, TLS | 8 × 256 | 2,880,027 | 691 µs | 1.17 ms | 1.43 ms |
| latency, TLS, fsync | 1 × 1 | 125 | 7.93 ms | 14.03 ms | 16.13 ms |
| throughput, TLS, fsync | 8 × 256 | 113,817 | 17.93 ms | 30.00 ms | 38.04 ms |

Without fsync, a message is committed in tens of microseconds, and no one task is busy: the
throughput run is bound by its own window, 2,048 messages in flight over a round trip of about a
millisecond. More in flight goes faster (at `cf027a9`): 1,375,000 a second with 8 clients of 1,024
(`BENCH_THROUGHPUT=8x1024`), and 1,145,000 with 64 of 256. TLS costs little, its connections
buffered as plain TCP's are. With fsync, the disk is what limits it: on macOS a sync is
`F_FULLFSYNC`, which empties the drive's own cache, some milliseconds each, and its runs vary
the most from one to the next. Nodes on one drive share those flushes (after 0.12.1): each log
`fsync`s its own file, then waits for one flush started after that, rather than each flushing in
turn. Group commit is why throughput with fsync is still over 100,000 a
second: each sync covers every message that arrived during the one before. A Linux server's NVMe
drive syncs much faster (above); measure yours.

A query skips the write, so one on its own takes four fifths of a message's time, and with many in
flight they go more than twice as fast as messages: 3.0 million a second.

The core alone, the `in memory` runs: 448,573 messages a second one at a time, a commit through
all three nodes in 2 µs at p50, and 536,371 a second at 8 × 256, on its one thread (openraft's
benchmark, on a Mac: 33,000 one at a time, 3.5 million with 4,096 clients on several). So without
fsync, nearly all of a message's 53 µs on its own over TCP is the driver's and the loopback's,
not the core's.

[clients]: clients.md
[operations]: operations.md
[running]: running.md
