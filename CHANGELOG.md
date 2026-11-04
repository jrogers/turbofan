# Changelog

Notable changes to the published crates, `turbofan-cluster` and `turbofan-sbe`, which are released
together at the same version. Before 1.0 a minor version (0.2.0) may break the API, and says how;
a patch version (0.1.1) doesn't.

## Unreleased

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
