# Members

The members of a cluster change while it runs, one change at a time, through the log: adding a
node, removing one, or switching one between voter and learner. A cluster has at most
`MAX_MEMBERS` (7).

The commands here, and in [Operations][operations], are the service binary's own (`run` gives it
them), sent to any member's admin listener and passed on to the leader. Set
`TURBOFAN_ADMIN_TOKEN` for them as for the nodes, and over TLS give each `--tls-ca`, `--tls-cert`
and `--tls-key`. `Admin` is the same in code.

## Voters and learners

A **voter** votes in elections, may lead, and counts toward the majority that commits an entry.
A **learner** takes the log and snapshots, and applies them, but never votes or stands, and the
cluster's commits don't wait for it. A learner is a copy of the state that costs the cluster
nothing: a standby in another region, a node catching up before it becomes a voter, or a replica
to read from.

Three voters keep going with one down; five, with two. An even number buys nothing: four voters
need three for a majority, and so survive only one failure, as three do.

## Adding a node

Start the new node with `--join`. Its lists are then just where to find the cluster: its own
addresses, and at least one member's. Then ask the cluster to add it:

```sh
my-service serve --id 4 --data /var/lib/my-service --join \
    --peers   1=10.0.0.1:7001,4=10.0.0.4:7001 \
    --clients 1=10.0.0.1:7101,4=10.0.0.4:7101 \
    --admin   1=10.0.0.1:7201,4=10.0.0.4:7201
my-service add-member --admin 10.0.0.1:7201 --member 4=10.0.0.4:7001,10.0.0.4:7101,10.0.0.4:7201
```

`--member` gives its ID, then its peer, client and admin addresses. The leader sends it the log,
or its snapshot if it's far behind, and adds it once it has caught up, so the change never leaves
the cluster waiting on a voter that has nothing. `add-member` returns once the change is
committed, and waits up to two minutes for that. A voter that hasn't caught up within 100
election timeouts (30 s by default) isn't added: for a big state over a slow link, add it as a
learner, which nothing waits for, then make it a voter with `set-role` once it's caught up.

`add-member --learner` adds it as a learner instead. Nothing waits for a learner to catch up, so
`add-member` returns at once; `--wait <entries>` waits until it's within that many entries of the
leader's log, printing how far behind it is meanwhile.

## Removing a node

```sh
my-service remove-member --admin 10.0.0.1:7201 --id 4
```

Stop the node once that's answered. Removing the leader works: it steps down once the change
commits, and the others elect a new one. The last voter can't be removed.

## Changing a role

`set-role --id <n> --voter` makes a learner a voter, and `--learner` makes a voter a learner, in
place, without shipping it the log again. A learner made a voter catches up first, as a new
member does, but check `status` (see [Operations][operations]) that it's close before you count
on it.

## Replacing a node

A node whose disk is lost, or that finds damage as it runs (reading back a record or a snapshot
for a follower), is replaced, not repaired:
remove it, then add a new one with an empty data directory. In a three-node cluster, that leaves
two voters for a while, and no room for another failure: add first, then remove, if the cluster
has room for one more member.

## A log in doubt

A node that finds a damaged record in its log as it starts drops it, and everything after it:
it's a write that power cut off partway, never acknowledged. (A batch is one write, and the disk
may keep a later page of it and lose an earlier, so whole records can follow the damage; and
unsynced, a segment may lose its tail and the next one keep its records.) But bit rot looks the
same, and what's dropped may have been acknowledged. So the node doubts its log, durably (a
`doubt` file in its `log/` directory), and its votes say so, until a leader has caught it up. While any voter
doubts, an election needs a vote more than a majority for each voter in doubt, so that the voters
still include one that holds whatever was committed. The `turbofan_in_doubt` metric is 1 while a
node doubts.

Usually that passes unnoticed. But after a power cut every node may doubt, and then an election
needs every member; and in a three-node cluster with one disk dead and another node in doubt,
there's no leader, so the dead one can't be removed. If you know a node in doubt only lost power,
delete its `doubt` file, and restart it.

## A standby that takes over

A learner in another region is a standby for the whole cluster: it has the state, and the
cluster's commits never wait for it. If the main cluster is lost, `promote` makes it the one
member of a new cluster, from its log as far as it knew it committed:

```sh
my-service promote --admin <the learner's admin address>
```

It's refused while the learner hears from a leader. But from another region a dead cluster and a
cut link look alike, and two clusters taking the same clients' orders would fill them twice, so
promotion is never automatic: fence the old cluster first (stop it, or cut it off from its
clients), then promote.

The new cluster has a new epoch, which every hello carries: the old cluster's nodes, if they come
back, can't talk to it. It grows with `add-member` as any cluster does. Sessions are replicated,
so clients reattach to it and resend what it hasn't applied. What the old cluster committed past
what the learner knew committed is lost: the learner drops any of its log past its commit index,
which the old cluster may not have committed, or may have committed other entries in place of.
Count it from the old leader's commit index and the learner's, from `status` before the loss,
or the metrics. A client whose messages or responses were among it closes its session,
`CloseReason::Diverged` (see [Clients][clients]), to open another and ask the service what it
has: judged by what its session had as the new cluster began, so a response the new cluster
sends since, before the client reattaches, doesn't make up for one it lost. Keep the learner
close: watch its lag.

[clients]: clients.md
[operations]: operations.md
