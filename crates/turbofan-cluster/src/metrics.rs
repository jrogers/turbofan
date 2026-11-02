//! What a node reports through the [`metrics`] facade, to whichever recorder the binary installs
//! (a Prometheus exporter, say): none, and they go nowhere. The core only counts, into
//! [`Metrics`] from `Node::metrics` and the commit latencies in
//! `Outputs::commit_latencies`; the driver publishes them.
//!
//! | Name | Kind | |
//! |---|---|---|
//! | `turbofan_role` | gauge, by `role` | 1 for this node's role, 0 for the others |
//! | `turbofan_term` | gauge | the current Raft term |
//! | `turbofan_leader` | gauge | the leader's ID, 0 if none is known |
//! | `turbofan_members` | gauge | how many members the cluster has |
//! | `turbofan_commit_index` | gauge | the highest index known committed |
//! | `turbofan_applied_index` | gauge | the index applied to the service |
//! | `turbofan_last_index` | gauge | the last index in the log |
//! | `turbofan_snapshot_index` | gauge | the index the latest snapshot covers to |
//! | `turbofan_snapshot_age_seconds` | gauge | since the latest snapshot was taken or installed |
//! | `turbofan_sessions` | gauge | client sessions open |
//! | `turbofan_in_doubt` | gauge | 1 while the node doubts its log, which dropped a damaged tail |
//! | `turbofan_follower_lag_entries` | gauge, by `peer` | as leader, how far behind its log each member is |
//! | `turbofan_commit_latency_seconds` | histogram | as leader, from an entry proposed to committed |

use metrics::{gauge, histogram};

use crate::raft::{NodeId, Role};

/// A node's state, at a moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Metrics {
    /// Its role.
    pub role: Role,
    /// The current term.
    pub term: u64,
    /// The leader it knows of.
    pub leader: Option<NodeId>,
    /// How many members there are.
    pub members: usize,
    /// The highest index known committed.
    pub commit: u64,
    /// The index applied to the service.
    pub applied: u64,
    /// The last index in its log.
    pub last_index: u64,
    /// The index its latest snapshot covers to.
    pub snapshot_index: u64,
    /// How long since it took, or was sent, its latest snapshot (or started, with none), in
    /// nanoseconds.
    pub snapshot_age_ns: u64,
    /// Client sessions open.
    pub sessions: usize,
    /// It doubts its log (`Entries::doubt`).
    pub in_doubt: bool,
    /// As leader, how many entries each other member is behind its log.
    pub follower_lag: Vec<(NodeId, u64)>,
}

impl Metrics {
    /// Sets the gauges.
    #[allow(
        clippy::cast_precision_loss,
        clippy::as_conversions,
        reason = "gauges are f64; indices that big don't happen"
    )]
    pub fn publish(&self) {
        for (role, name) in [
            (Role::Follower, "follower"),
            (Role::PreCandidate, "pre_candidate"),
            (Role::Candidate, "candidate"),
            (Role::Leader, "leader"),
        ] {
            gauge!("turbofan_role", "role" => name).set(f64::from(u8::from(self.role == role)));
        }
        gauge!("turbofan_term").set(self.term as f64);
        gauge!("turbofan_leader").set(f64::from(NodeId::to_wire(self.leader)));
        gauge!("turbofan_members").set(self.members as f64);
        gauge!("turbofan_commit_index").set(self.commit as f64);
        gauge!("turbofan_applied_index").set(self.applied as f64);
        gauge!("turbofan_last_index").set(self.last_index as f64);
        gauge!("turbofan_snapshot_index").set(self.snapshot_index as f64);
        gauge!("turbofan_snapshot_age_seconds").set(seconds(self.snapshot_age_ns));
        gauge!("turbofan_sessions").set(self.sessions as f64);
        gauge!("turbofan_in_doubt").set(f64::from(u8::from(self.in_doubt)));
        for &(peer, lag) in &self.follower_lag {
            gauge!("turbofan_follower_lag_entries", "peer" => peer.to_string()).set(lag as f64);
        }
    }
}

/// Records an entry's time from proposed to committed, in nanoseconds.
pub fn record_commit_latency(ns: u64) {
    histogram!("turbofan_commit_latency_seconds").record(seconds(ns));
}

#[allow(clippy::cast_precision_loss, clippy::as_conversions, reason = "nanoseconds to seconds, for a metric")]
fn seconds(ns: u64) -> f64 {
    ns as f64 / 1e9
}
