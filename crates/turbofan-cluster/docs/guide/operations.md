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
`snapshots/` directory, and `serve --keep-snapshots <n>` keeps the newest `n` there, not just one.

## Backup and restore

A snapshot is a backup: one file of the whole state (the service's, its sessions and timers, the
members), checked against a checksum, and written whole or not at all. To take one, ask for it,
then copy the newest snapshot in any node's `snapshots/` directory off the machine, with whatever
you copy files with:

```sh
$ my-service snapshot --admin 10.0.0.1:7201
snapshot taken at index 1830413
$ ssh 10.0.0.1 'ls /var/lib/my-service/snapshots/*.snapshot | tail -1'
/var/lib/my-service/snapshots/00000000000001830413.snapshot
$ scp 10.0.0.1:/var/lib/my-service/snapshots/00000000000001830413.snapshot backups/
```

Copy the newest rather than the one named by the index: a node snapshots on its own too, and
then deletes the older one, unless `--keep-snapshots` keeps it. `snapshot` answers once a
quorum has it, so a node not among them may take a moment more: the index is in the file's name,
so wait until the newest's is the one you asked for or later. Any whole snapshot is a backup, just
an older one. Run this on a schedule; what's applied after the last backup is what a restore loses.
For a loss measured in milliseconds, keep a standby instead (see [Members][membership]).

With the cluster lost, `restore` makes a data directory from a backup, for one node, with
`serve`'s flags; `serve` then runs it, as the one member of a new cluster:

```sh
my-service restore --from backups/00000000000001830413.snapshot --id 1 --data /var/lib/my-service \
    --peers 1=10.0.0.1:7001 --clients 1=10.0.0.1:7101 --admin 1=10.0.0.1:7201
my-service serve --id 1 --data /var/lib/my-service \
    --peers 1=10.0.0.1:7001 --clients 1=10.0.0.1:7101 --admin 1=10.0.0.1:7201
```

It refuses a data directory that isn't empty. The new cluster has the snapshot's state, and a new
epoch, as a promoted standby does: the old cluster's nodes, if any come back, can't talk to it.
Grow it with `add-member`, each new node with an empty data directory. Sessions are in the
snapshot, so clients reattach; one that saw messages or responses from after it closes its
session (`CloseReason::Diverged`, see [Clients][clients]), to open another and ask the service
what it has.

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
