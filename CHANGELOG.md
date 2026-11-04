# Changelog

Notable changes to the published crates, `turbofan-cluster` and `turbofan-sbe`, which are released
together at the same version. Before 1.0 a minor version (0.2.0) may break the API, and says how;
a patch version (0.1.1) doesn't.

## 0.5.0 (2026-11-04)

### Added

- Capture and replay: `capture --admin <addr>` (`Admin::capture`) has that node put its latest
  snapshot and its log from there to its commit index in `<data>/captures/`, mostly hard links;
  `replay --capture <dir> [--to <n>]` (`capture::replay`) restores the snapshot into the service
  and applies the entries, printing every effect, to reproduce a bug from production or compare
  two versions on real traffic. An older node doesn't answer `Capture`.
- TLS on the admin listener only: `serve` without `--tls-peer-ca` (`NodeTls::admin_only`) puts it
  there, and leaves peers and clients on plain TCP. Each node's own, so it can go on one node at a
  time.

### Breaking

- `AdminError` has `CaptureFailed`.
- `NodeTls::peer_ca` is an `Option`: none for TLS on the admin listener only. `NodeTls::new` is as
  it was.

## 0.4.0 (2026-11-04)

Once every node runs 0.4.0, switch protocol 1 on with `activate --protocol 1` (see the egress ring,
under Fixed).

### Breaking

- `wire::Opened` and `wire::Reattach` have an `epoch`, `wire::RequestVote` and `wire::Vote` a
  `round`, `wire::NodeState` an `epoch_since`, and `wire::Status` the
  `protocol` and `service_version` active: code building them sets them. `ClusterStatus` has them
  too, and is `#[non_exhaustive]`.

### Changed

- `activate` without `--protocol` or `--service` leaves that version as it is, from the leader's
  `status`, which shows them now. Before, one left out was 0, refused as a step back once higher.
- A log is truncated on open at its first damaged record, wherever it is, and at a gap between
  segments, the records and segments after it dropped, and doubted: the node catches up from the
  others. Before, only damage with nothing whole after it was truncated, and any other refused to
  start, for the node to be wiped and replaced. Bit rot of synced records is now taken the same
  way, which doubt keeps safe however much is dropped.
- Connections over plain TCP are buffered, as TLS already was: each frame was a system call to
  write and two to read. Throughput under load (the benchmark's 8 × 256) about triples, from
  155,000 messages a second to 455,000 on an M4 Mac mini, and is no longer below TLS's.

### Fixed

- `egress_ring_entries`, the responses each session keeps, was each node's own setting, though
  the ring is replicated state: members with different values replayed different responses to a
  client after a failover. Protocol 1 (`node::PROTOCOL`) logs the leader's in each `Activated`
  entry, and every node keeps that many; until `activate --protocol 1`, each its own, as before.
  `activate` leaves a version it isn't given as it is.
- An operator's request retried after it was made, its answer lost or its leader gone, was
  refused: a member change, say, though the members showed it. A change the members already show,
  committed, is answered `MembersChanged` at the latest change's index, by any leader; a transfer
  to the node that leads, with it; a promotion, as it was. So `add-member` of a member already
  in, with the same addresses and role, or `remove-member` of one that isn't, succeeds now.
- A power cut mid-batch could leave a log that wouldn't open: the disk may keep a later page of a
  write and lose an earlier one, leaving whole records after a torn one, which was taken for bit
  rot; and unsynced, a segment's tail lost and the next one kept, a gap. Both are truncated now,
  as above.
- A client reattaching after a recovery could be given another client's session. A session's
  ID is the index it opened at, and a cluster promoted from a standby or restored from a backup
  logs on from its own last index, so it may open a session under an ID the old cluster had
  given one after they split. `Opened` says the cluster's epoch, a `Reattach` names it back, and
  the snapshot keeps the index the epoch began at (schema version 5): a reattach naming another
  epoch, to a session opened since, is refused (`CloseReason::NoSession`). A client naming none
  (an older one), or a cluster whose snapshot an older node wrote, is taken as before.
- A pre-vote granted in an earlier round counted in a later one, so one delayed past an election
  timeout could let a node cut off from the leader win a pre-vote, then depose it. `RequestVote`
  names its round and `Vote` echoes it (schema version 5); a vote from an older node, without
  one, counts as before.
- Connections idle after their hello were kept for ever, and as many as came, so they could use
  up a node's file descriptors. A client connection silent for `session_timeout` is closed (its
  session is gone by then), and each listener keeps at most so many at once: `MAX_SESSIONS`
  clients, 256 peers, 256 operators. Peer and operator connections aren't timed out for being
  idle, so keep strangers off them with `peer_token`, `admin_token` or TLS.

## 0.3.0 (2026-11-04)

### Breaking

- `driver::Config`, `node::Config`, `raft::Config` and `tls::NodeTls` are `#[non_exhaustive]`,
  so that a new setting is no longer a breaking change. Outside the crate they can't be built
  field by field, nor with `..`: build them with `driver::Config::new`, `node::Config::new` and
  `tls::NodeTls::new`, which are new but for `node::Config`'s, then set what differs.

### Added

- Backup and restore. A snapshot copied off a node is a backup; `driver::restore` (`restore
  --from <file>` with `serve`'s flags) makes a data directory of it for one node, which `serve`
  runs as the one member of a new cluster, of a new epoch, to grow with `add-member`.
- `run_with`: `run` with a closure that changes each node's configuration before it serves, for
  the settings that are the service's to choose. `serve --election-timeout <ms> --heartbeat <ms>`
  apply over it, for an operator's network; a heartbeat not under half the timeout is refused.
- `node::Config::snapshots_kept` (`serve --keep-snapshots <n>`): the newest snapshots each node
  keeps on disk, for copying off it or restoring from. 1 by default, as before.
- `Client::is_closed`: the session is over.

### Fixed

- A follower that crashed installing a leader's snapshot in place of its log was stuck: after
  the snapshot was durable but before the log was reset, it kept a log disagreeing with the
  snapshot and rejected the leader's appends for ever; partway through the reset, its log had a
  gap and it wouldn't start. A log that disagrees with the snapshot is now reset on restart, and
  a reset removes the old segments, newest first, before it starts the new one.
- A client never noticed a leader that went silent (powered off, its link cut), waiting on the
  socket until TCP gave up, by when its session had timed out. Three keepalives unanswered, it
  now tries the next member.
- A session's responses kept for a reattach (`egress_ring_entries`) grew without bound once over
  the limit, from a node that kept more or the setting lowered, until the snapshot was too big
  to read.
- A learner could be promoted as soon as it started, before it had a chance to hear its leader.
- An operator's request sent again after its connection dropped is answered on the new one; a
  snapshot or shutdown was never answered, and a member change was refused though it went on.
  `Admin` goes back to its address after a failure, not on to the leader it was redirected to.
- `NodeId` parses from 1: `serve --id 0` was a voter no peer would talk to.
- `add-member --wait`, `transfer-leadership --to` and `activate`'s flags without a value are
  refused, not taken as absent.
- `ClusterClient` stops once its session is closed: `recv` returns `None`, as it says.
- A leader that removed itself counted its own snapshot towards an operator's snapshot quorum.
- An append whose records' terms fell, or passed the leader's, panicked a follower: it's now
  ignored as malformed.
- Heartbeats to a peer that had stopped acknowledging carried up to a megabyte of records each,
  queued for it while it was gone, up to gigabytes. Past what it may have in flight, they carry
  none.
- A client stops reading while it's blocked writing no longer: a leader slow to read disconnected
  it once its responses backed up. `ClusterClient` writes on a task of its own.
- Redirects back and forth, between nodes that disagree on the leader mid-election, are paced
  rather than followed in a tight loop, by `ClusterClient` and `Admin`.
- Accepting a connection that fails (out of file descriptors, say) is retried after a pause, not
  in a loop that holds a core.
- The directories a node creates are synced into their parents, and a connection that closes
  before its session opens is forgotten; one that opens a second session leaves the first. A
  snapshot file's length is checked without overflowing.

## 0.2.0 (2026-11-04)

### Breaking

- `CloseReason` has a new value, `Diverged`: a `match` on it without a wildcard needs an arm for it.

### Fixed

- A client that reattached to a promoted standby missing some of the session was stuck. Lost
  messages it had let go of once applied, so it couldn't resend them, and the cluster dropped
  every later one as out of order; lost responses made the cluster reuse their `egressSeq`, and
  the client dropped every later one as seen. It now closes the session, with `Event::Closed`
  and `CloseReason::Diverged`, counting what the new cluster never applied in `unapplied`.
  `Opened` carries the session's last `egressSeq` for it (schema version 4): from an older node,
  only lost messages are caught.

### Docs

- Examples on `Service`, `Ctx` and `ClusterClient`.

## 0.1.1 (2026-11-03)

Documentation only: the READMEs, guide and architecture overview no longer link to the OMS, whose
repository is private. The code is as in 0.1.0.

## 0.1.0 (2026-11-03)

The first release.

### `turbofan-cluster`

A Raft-replicated state machine framework: a deterministic `Service`, run on every node over the
same log, survives node failures without losing or repeating a message.

- **Services:** sessions with credentials and principals, timers, proposals of the service's own,
  snapshots written off the apply path, and `Ctx::new` for testing a service without a cluster.
- **Clients:** `ClusterClient` keeps a session open across failovers, with messages applied and
  responses delivered exactly once and in order; `client::Client` is the same without tokio.
- **Running a cluster:** a service binary's command line (`run`), or `driver::serve`; membership
  changes, learners, leadership transfer, promotion of a standby, versioned rolling upgrades
  (`activate`), shutdown, status, and metrics through the `metrics` facade.
- **Security:** admin and peer tokens, and TLS on every connection with the `tls` feature.
- **The guide:** from a first service to running a cluster in production, as
  `turbofan_cluster::guide`.

### `turbofan-sbe`

SBE codecs generated from message schemas in a `build.rs`, the runtime they call, and a
schemaless API for reading and writing fields one at a time.
