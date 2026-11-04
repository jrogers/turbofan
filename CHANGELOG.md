# Changelog

Notable changes to the published crates, `turbofan-cluster` and `turbofan-sbe`, which are released
together at the same version. Before 1.0 a minor version (0.2.0) may break the API, and says how;
a patch version (0.1.1) doesn't.

## Unreleased

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
