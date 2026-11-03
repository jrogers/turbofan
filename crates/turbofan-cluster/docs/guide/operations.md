# Operations

A running cluster needs little: it elects its own leaders, snapshots on its own, and catches
restarted nodes up. This chapter covers what an operator does: looking at it, restarting nodes
one at a time, stopping it, and watching it. The commands go to any member's admin listener, as
in [Members][membership], with `TURBOFAN_ADMIN_TOKEN` set.

## Status

```sh
$ my-service status --admin 10.0.0.1:7201
leader 2, last index 1830412, commit index 1830412
member 1 voter: matched 1830412, 0 behind
member 2 voter: matched 1830412, 0 behind
member 3 voter: matched 1830398, 14 behind
member 4 learner: matched 1790002, 40410 behind
```

It's the cluster as its leader knows it: the end of the leader's log, its commit index, and how
far each member has the log. Use it to tell that a node has caught up before relying on it: after
a restart, before restarting the next, before making a learner a voter.

## Snapshots

Each node snapshots on its own every `snapshot_every_entries` entries. `snapshot` asks every node
for one now, at the same index, and answers once a quorum has it durable:

```sh
$ my-service snapshot --admin 10.0.0.1:7201
snapshot taken at index 1830413
```

A snapshot is what a restarting node loads, so a fresh one makes restarts quicker. It's also
where to look for the service's state at a known index: the latest is in each node's
`snapshots/` directory.

## Rolling restarts

To restart every node (for a new binary, a configuration change, or a new certificate), restart
them one at a time, so a majority is always up:

1. Restart a follower. Wait until `status` shows it caught up.
2. Repeat for each follower.
3. Hand the leadership over, then restart the old leader:

   ```sh
   my-service transfer-leadership --admin 10.0.0.1:7201 --to 3
   ```

   Without `--to`, it goes to the member most caught up. The leader sends the target the rest of
   its log, then tells it to stand for election at once, so the cluster is without a leader for
   a round trip rather than an election timeout. Stopping the leader without it works too, at the
   cost of an election timeout (300 ms to 600 ms by default) with no leader.

Clients carry on throughout: a `ClusterClient` follows the leader and resends what wasn't
applied. A release that changes what the service does, or what the nodes send each other, takes
an extra step: see [Upgrades][upgrades].

## Shutting down

```sh
$ my-service shutdown --admin 10.0.0.1:7201
snapshot taken at index 1830502: the members are stopping
```

Every member snapshots at the same index, then stops, and `serve` returns. A restart loads the
snapshot rather than replaying the log. Start the nodes again with the same command line: their
data directories say who the members are.

## Metrics

A node reports through the [`metrics`](https://docs.rs/metrics) facade. Install a recorder in the
service's binary before `run` (a Prometheus exporter, say) to collect them; without one, they go
nowhere. The main ones:

| Name | |
|---|---|
| `turbofan_role{role}` | 1 for this node's role (`leader`, `follower`, ...), 0 for the others. |
| `turbofan_leader`, `turbofan_term` | The leader's ID, 0 if none is known; the current term. |
| `turbofan_commit_index`, `turbofan_applied_index`, `turbofan_last_index` | How far the node has the log. |
| `turbofan_follower_lag_entries{peer}` | On the leader, how far behind each member is. |
| `turbofan_snapshot_age_seconds` | Since the node's latest snapshot. |
| `turbofan_sessions` | Client sessions open. |
| `turbofan_in_doubt` | 1 while the node doubts its log (see [Members][membership]). |
| `turbofan_persist_latency_seconds`, `turbofan_commit_latency_seconds`, `turbofan_apply_latency_seconds` | On the leader, how long its entries take to be durable on its own disk, committed, and applied and answered. |

`src/metrics.rs` lists them all. Worth an alert:

- **No leader**, or the leader changing often: `turbofan_leader` 0, or `turbofan_term` climbing.
  A node that can't reach the others stands for election over and over.
- **A follower falling behind**: `turbofan_follower_lag_entries` growing. A slow disk or network,
  or a node about to need a snapshot. Watch a standby learner's especially: it's what a
  promotion would lose.
- **A node in doubt**, for longer than it takes to catch up.
- **No snapshot for a long time**, on a busy cluster: the log is growing, and restarts slowing.
- **Latency**, from the histograms: commit latency is the cluster's, apply latency adds the
  service's.

Every node reports the same applied state, from when it started; scrape them all, and read the
leader's for the latencies.
