# Changelog

Notable changes to the published crates, `turbofan` and `turbofan-sbe`, which are released
together at the same version. Before 1.0 a minor version (0.2.0) may break the API, and says how;
a patch version (0.1.1) doesn't.

## Unreleased

### Added

- TLS turned on or off over a running cluster, a node at a time (#6): `NodeTls::plain`
  (`serve --tls-plain accept|dial`) takes plain TCP beside TLS on the peer and client listeners,
  told apart by how a connection begins, and with `Plain::Dialled` dials peers over plain TCP.
  Every node to `dial`, then to `accept`, then to TLS only, a restart each; off is the same
  backwards. `turbofan_plain_connections{listener}` counts the connections still plain. Not on
  the admin listener, nor with `peer_ids`.

## 0.10.0 (2026-11-07)

The API made ready to freeze for 1.0 (#8): what may grow can, without breaking a caller.

### Breaking

- `wire` (the codecs generated from the cluster's schema) and `codec` (its framing) are no longer
  part of the API, hidden from the docs: a message or field added to the schema then breaks no
  one. `CloseReason` stays, at the crate root. `client::Client::on_frame` takes a frame's bytes,
  as read, and returns `Result<(), MalformedFrame>`: drop the connection on an error.
- Enums that may gain variants are `#[non_exhaustive]`, so a `match` on one needs a `_` arm:
  `client::Event`, `Reconnect`, `SendError`, `QueryError` and `ClientConfigError`; `Effect`;
  `Role`; and `CloseReason`, as is every enum `turbofan-sbe` generates (the crate the code is
  generated into matches its own as before). So are `Event::Closed` and `Event::Lost`, which may
  say more: a pattern on one ends in `..`.
- Structs that may gain fields are `#[non_exhaustive]`, so another crate can't build one with a
  struct expression: `Metrics`, `MemberStatus`, `client::Answer` and `PayloadTooLong`, which only
  the cluster makes, and `Member`, made with `Member::new(id, peer, client, admin)` (set
  `learner` after).
- `Metrics::publish` and `metrics::record_latency` are the driver's own.

## 0.9.0 (2026-11-07)

### Added

- Queries, answered without logging them (ReadIndex, #19): `ClusterClient::query` (and
  `Client::query`, `testing::Cluster::query`) asks, and `Service::on_query` answers from the
  state as applied, on the leader, once it has confirmed it still leads by a round trip to a
  quorum. A query is asked again of a new leader after a failover, and answered once. Every
  node must be of this version before a client queries: a node before it skips a query
  unanswered, and a voter before it doesn't echo the rounds that confirm one. Schema version 8:
  `Query`, `QueryAnswer` and `QueryRefused` for clients, and `readRound` on `AppendEntries` and
  `AppendAck`.
- `Role`, the type of `Metrics::role`, is exported, as `turbofan::Role`.

### Fixed

- A client whose session a leader deposed without knowing it yet reattached, from less of the
  session than the client had seen from the new leader, closed the session as `Diverged`, as
  if the cluster had been lost: telling the gateway its messages not yet applied never would
  be, when the cluster could still apply them. A cluster recovered from a loss has an epoch of
  its own, so the client now takes an `Opened` behind what it saw, of the epoch its session was
  opened in, for a stale node, and goes elsewhere.

### Breaking

- `testing::Cluster::node` is gone: it handed out the core's `Node`, which isn't part of the API.
  `Cluster::metrics` says what a test needs of a node, as its metrics endpoint does
  (`last_index`, `snapshot_index`, `role` and the rest), or `None` while it's down;
  `Cluster::service` is as it was.

## 0.8.0 (2026-11-07)

### Added

- Addresses may be host names: `serve --peers 1=oms-0.oms:7001,...`, and `add-member`'s. A node
  looks a name up each time it dials it, so a node that moves and keeps its name, as a
  Kubernetes StatefulSet's pod does, is found again. With a name among its own addresses,
  `serve --listen <ip>` says which interface its listeners bind (`0.0.0.0` for all of them): a
  name isn't bound to an interface the operator didn't name. `driver::Config::peer_listen`
  does the same for a service calling `serve` itself.
- `serve` stops on SIGTERM, as Kubernetes or systemd stops a process, or Ctrl-C, and a leader
  first hands its leadership to the member most caught up, as `transfer-leadership` does: the
  cluster is led again in a round trip, not after an election timeout. A leader killed by
  SIGTERM was gone at once. `driver::serve_until` takes the future that stops it, for a service
  calling it itself.

### Fixed

- A node told to stop by a shutdown, but short of the snapshot it was to stop with, stopped
  whenever it later got a snapshot that far: after the cluster had been started again, a member
  being added or a standby being caught up stopped partway, and a standby told to stop couldn't
  be promoted. It now forgets the stop in a new term.
- A follower sent a snapshot that didn't match the leader's checksum, damaged in the leader's
  file as it was read, say, stopped as if its own disk were damaged, until an operator replaced
  it. It now checks the checksum as the last chunk arrives, and asks for the snapshot again.
- A leader never caught up a follower whose log had lost entries it acknowledged (a damaged
  tail dropped on restart), and answered each refusal with the same append, as fast as they
  went back and forth. It now sends the follower its log again from where it refused.
- A node in doubt (its log dropped a damaged tail on restart) stopped doubting on an append
  reaching its leader's last index, though one sent before the restart, arriving late, says less
  than it had acknowledged: a leader could then count its earlier acknowledgement of entries it
  had lost, and commit an entry a majority didn't hold. It now trusts its log again only from an
  entry or snapshot of a later term.
- A leader answered each refusal of the appends in flight to a follower behind it, sending more
  than were answered, round after round, until it ran out of memory. A refusal now says which
  append it refuses (`AppendAck`'s `prevIndex`, schema version 7), and after one the leader
  probes with one append at a time, ignoring refusals of older ones. A refusal from an older
  node, without it, is answered as before.

### Breaking

- `turbofan-cluster` is now `turbofan`: depend on `turbofan` and `use turbofan::...` in place of
  `turbofan_cluster::...`; nothing else in the API changes for it. The sections below for 0.7.0
  and before name it as it was.
- `driver::Config::peer_addrs` holds `host:port` strings, not `SocketAddr`s. `Config::new`
  takes `SocketAddr`s as before.

## 0.7.0 (2026-11-05)

### Changed

- Every write is made on a thread again, as before 0.6.0, which wrote a few appends on the node's
  own task when the log didn't sync. A write the kernel held up there (Linux throttles writers
  when too much is waiting to be written back) held the node's heartbeats up with it, and one
  held past the election timeout was an election. A quiet cluster's message takes about 10 µs
  longer for it (p50 55 µs to 65 µs on the benchmark); a busy one's throughput is the same.

### Added

- A cluster in memory, for a service's tests: `turbofan_cluster::testing`, with the `testing`
  feature. Its nodes run in one thread, their logs and snapshots in memory, on a clock the test
  moves, with the same clients a gateway has: a failover takes milliseconds, and a run is the
  same every time. Its API may change before 1.0. See the guide's services chapter.

### Fixed

- A client of a lost cluster reattaching to a standby promoted, or to a cluster restored from a
  snapshot, is told what its session had as the new cluster began, and closes it as diverged if
  it saw more. It was told what the session had by the time it reattached, and the new cluster
  may have sent it responses of its own meanwhile (a timer, another session's notice) that made
  up the number of one it lost: the client carried on, missing a response and seeing another
  twice. Each session carried over into the new epoch is marked as it stood, in the snapshot's
  `SessionState` (schema version 6). And a learner promoted drops its log past its commit index,
  which the lost cluster may not have committed: it began from it as it was.
- A client whose session was still being opened when the leader stepped down is redirected to
  the new one. It was left waiting on a session that never opened, until its keepalives went
  unanswered, some seconds.

- A member being added votes once it has some of the log, though it doesn't yet have the entry
  that adds it, and so knows no members. It refused, and with voters in doubt, which take every
  member's vote, no election could be won: the cluster had no leader until a node was restarted
  with its `doubt` file deleted. A node with no members and no log still votes for no one.

### Breaking

- `wire::SessionState` has `carried_seq` and `carried_egress`: code building one sets them
  (`u64::MAX` for a session with none).

## 0.6.1 (2026-11-04)

### Fixed

- A snapshot being saved holds the node's data directory locked until it's written. `serve`
  stopped (its task aborted, say) left the thread saving it running, but let go of the lock: a
  node started again in the same process could then save its own beside it, one of them failing
  with `NotFound` and stopping its node. A process stopped can't, its threads going with it.

## 0.6.0 (2026-11-04)

### Added

- TLS without operators' certificates: `serve` without `--tls-admin-ca` (`NodeTls::admin_ca`
  `None`) encrypts the admin listener and has operators present no certificate, the admin token
  saying who they are, as a client's credentials do. The operators' commands take `--tls-ca`
  alone for it.
- Per-node identity: `serve --tls-peer-ids` (`NodeTls::peer_ids`) has each node's certificate name
  its node ID, as the DNS name `node-<id>.turbofan`, and takes a peer only as the node its
  certificate names: a node with another's certificate can't say it's that one, nor be sent its
  appends. Off by default; turn it on once every node's certificate names its ID.
- Certificates reloaded without a restart: a node rereads its TLS files every minute
  (`--tls-reload <secs>`, `NodeTls::reload_every`), and when they've changed and load, uses them
  for the connections after. Files that don't load are left until they do, counted in
  `turbofan_tls_reloads`.
- `ClusterClient::send_batch` (and `Client::send_batch`) sends many messages in one call, in
  order, all or none, returning their `client_seq`s; `ClusterClient::recv_many` takes the
  responses that have arrived, up to a limit. The benchmark's clients use them: its throughput
  run goes from about 870,000 messages a second to 970,000.

### Changed

- `ClusterClient` keeps one timer rather than making one a message, whose registering, under
  tokio's timer lock, held up several clients at once, and writes what queued together under one
  flush: the benchmark's throughput nearly doubles.
- Client connections over TLS, both ends, buffer what they write and read: each frame was a TLS
  record and a system call of its own, made holding the lock the connection's reader waited on.
  The benchmark's throughput over TLS is four times what it was, as it is without TLS.
- Without fsync, a node writes a few appends on its own task rather than handing them to a
  blocking thread, whose handing over and back took three times as long as the write: the
  benchmark's latency, 66 µs at p50 and 105 µs at p99, is 56 µs and about 85 µs.
- A client's connection hands the node the frames it has read whole together, up to 256, not one
  at a time: a busy node's connections queued for it, and many clients went slower than a few.
  The benchmark's throughput run, 8 clients of 256 in flight, goes from 940,000 messages a second
  to 1,045,000; 64 clients of 256, from 555,000 to 1,100,000.
- A frame is encoded into room made for it, not a buffer grown from empty four or five times:
  the benchmark's throughput goes from 1,040,000 messages a second to 1,110,000.

### Breaking

- `NodeTls::admin_ca` is an `Option`. `NodeTls::new` and `NodeTls::admin_only` are as they were.

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
