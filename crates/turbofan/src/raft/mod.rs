//! Raft, sans-IO, over opaque records: elections with pre-vote, log replication and commit.
//!
//! It knows nothing of sessions or services: [`Raft::propose`] takes a payload and the caller
//! reads committed payloads back with [`Raft::entry`]. Inputs are `on_*` calls with the time;
//! [`Raft::take_outputs`] returns what to persist and what to send.
//!
//! The log since the snapshot is on disk, and a tail before it ([`Config::log_tail_entries`]), so
//! a follower just behind the snapshot is sent the log rather than all of it; memory holds every
//! entry's term, and the records of
//! the entries not yet applied and of a window of the latest applied ([`Config::log_window_bytes`]).
//! When a lagging follower or a restart needs older records, [`Outputs::reads`] asks for them,
//! and the driver reads them back from disk and hands them to [`Raft::on_read`].
//!
//! **Members** change one at a time, through the log (Raft's single-server change): a `Members`
//! entry takes effect on each node as it's appended, and is undone if it's truncated. A new
//! member first catches up without a vote ([`Raft::add_member`]); a leader that removes itself
//! leads until the change commits, then stands down. A node that knows members stands for election,
//! even before it's among them (added, it may have the most of the log before it has the entry
//! that adds it), counting only their votes; one joining, that knows none, stays quiet. A
//! removed one disrupts nothing: members refuse its pre-votes while they hear a leader.
//!
//! **Learners** ([`Member::learner`]) are members that take the log but don't vote: they're
//! added and removed as members are, without catching up first, but never stand for election, and their acks don't count
//! towards a commit, check-quorum or a snapshot's quorum, so a quorum never waits on one in
//! another region. Nor is the leadership handed to one. One changes role in place
//! ([`Raft::set_role`]): made a voter, it catches up first. A voter made a learner by a change
//! it hasn't seen committed still stands, as a removed one does: it may have the most of the log.
//!
//! **Leadership transfer** ([`Raft::transfer_leadership`]): the leader sends no more of its log
//! than it had when the transfer began, and once the target has that, sends it `TimeoutNow`.
//! It stands for election at once, skipping the pre-vote, and the others, their logs no newer,
//! vote for it.
//!
//! **Versions** ([`Raft::advertise`]): each node tells the leader, on every ack, the highest
//! protocol and service versions its code supports, which Raft carries without reading; the
//! leader knows the lowest among the members ([`Raft::lowest_versions`]), and doesn't add a
//! member below those the cluster requires ([`Raft::require`]).
//!
//! **Reads** ([`Raft::read_index`]): a leader confirms a read without logging it, by a quorum's
//! acks to a round of appends started after it came in, and hands it back with the commit index
//! its answer must reflect (ReadIndex, `reads.rs`).
//!
//! **Stopping** ([`Raft::stop`]): a leader shutting the cluster down sends its peers `Stop` with
//! its heartbeats for a couple of them, then stops; a follower stops on `Stop` once it has the
//! snapshot it names durable. Nothing in the log stops a node, so a restart never stops again.
//!
//! **Doubt** ([`Entries::doubt`]): a node whose log dropped a damaged tail on restart may have
//! lost entries it acknowledged, since bit rot in the last record looks like a torn write. It
//! can't tell, so it doubts until it has everything it could have acknowledged: an entry, or a
//! snapshot, of a term after the one it restarted in (by log matching, it then has the log of a
//! leader elected since). Not an append reaching its leader's last index: one sent before the
//! restart, arriving late, says less than the node acknowledged. The driver then ends the doubt
//! on disk ([`Persist::Trust`]).
//! Meanwhile its votes say so, and an election needs a vote more than a majority for each voter
//! in doubt, its own included (or every member): however many of those lost a committed entry,
//! the voters then still include one that holds it, if any node still does, and turns down a
//! candidate without it. After a power cut that tears every node's log, all of them doubt, and
//! an election needs every member up.
//!
//! **The driver's one rule:** perform each [`Outputs`]' `persist` operations in order, batch
//! after batch, and make them durable; then call [`Raft::on_persisted`] with the
//! [`Raft::log_end`] it noted when it took that batch, and only then send its `sends`, and those
//! of the batches before it. A vote or an ack then never goes out ahead of the state it promises.
//! It needn't wait: inputs may arrive, and more batches be taken, while one is persisting, but a
//! batch's `sends` wait for the batches before it too, whose state they may promise.
//!
//! A leader's `appends` are the exception: they go out at once, while `persist` is under way, so
//! followers write the leader's new records while it does. The leader counts itself towards a
//! commit only once they're durable, and its term was durable before it asked for votes. If it
//! crashes first, it comes back a follower without them, and can't lead that term again.

use std::collections::VecDeque;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use crate::codec::{self, MAX_ENTRIES_PER_APPEND, Record};
use crate::meta::Meta;
use crate::wire;

mod election;
mod entries;
mod membership;
mod reads;
mod replication;
mod snapshots;

pub use entries::Entries;
pub use reads::{MAX_WAITING_READS, ReadIndex};

/// A cluster member's ID, from 1.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct NodeId(pub u8);

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl NodeId {
    /// An ID off the wire, where 0 means none.
    pub(crate) fn from_wire(id: u8) -> Option<Self> {
        (id != 0).then_some(Self(id))
    }

    /// An ID for the wire, where 0 means none.
    pub(crate) fn to_wire(id: Option<Self>) -> u8 {
        id.map_or(0, |id| id.0)
    }
}

impl From<NodeId> for u64 {
    fn from(id: NodeId) -> Self {
        Self::from(id.0)
    }
}

impl From<NodeId> for usize {
    fn from(id: NodeId) -> Self {
        Self::from(id.0)
    }
}

impl std::str::FromStr for NodeId {
    type Err = std::num::ParseIntError;

    /// From 1: 0 is the wire's "none", and an error.
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(|id: std::num::NonZeroU8| Self(id.get()))
    }
}

/// A frame for a peer, shared: an append goes to every peer at the same index as one frame.
pub type Frame = Arc<Vec<u8>>;

/// The most members a cluster can have.
pub const MAX_MEMBERS: usize = 7;
/// The most record bytes one `AppendEntries` carries, leaving room for its header in a frame.
const MAX_APPEND_BYTES: usize = 1_000_000;
/// How far a leader replicates to a peer ahead of its acks: then it waits for them, sending
/// heartbeats without records.
const MAX_IN_FLIGHT: u64 = 4096;
/// A new member that hasn't caught up in this many election timeouts isn't added.
const CATCH_UP_TIMEOUTS: u64 = 100;
/// How far a leader sends a snapshot to a peer ahead of its acks, as reads of up to
/// [`MAX_READ_BYTES`].
const MAX_SNAPSHOT_IN_FLIGHT: u64 = 8 * crate::widen(MAX_READ_BYTES);
/// The most record bytes one of [`Outputs::reads`] returns, past its first record.
pub const MAX_READ_BYTES: usize = MAX_APPEND_BYTES;

const _: () = assert!(MAX_APPEND_BYTES < codec::MAX_FRAME_BYTES - 1024);
const _: () = assert!(MAX_IN_FLIGHT >= crate::widen(MAX_ENTRIES_PER_APPEND));

/// The longest timeout a configuration may give: far beyond any sensible one, and far enough
/// under `u64::MAX` nanoseconds that the cluster's clock plus one can't overflow.
pub const MAX_TIMEOUT: Duration = Duration::from_secs(365 * 24 * 60 * 60);

/// `duration` in nanoseconds, as the core keeps time.
///
/// # Panics
///
/// If it's over 584 years, which [`MAX_TIMEOUT`] rules out for a configured one.
pub(crate) fn nanos(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).expect("under 584 years")
}

/// Timing and identity, in [`crate::NodeConfig::raft`], which [`crate::NodeConfig::new`] fills in.
///
/// Non-exhaustive, so that a new setting isn't a breaking change: set what differs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// This node.
    pub id: NodeId,
    /// The members a new cluster starts with, this node among them; none for a node joining a
    /// running cluster, which learns them from the leader. Once the log or a snapshot has
    /// members, they're what counts.
    pub members: Vec<Member>,
    /// A follower stands for election after hearing nothing for between this and twice this.
    pub election_timeout: Duration,
    /// How often a leader heartbeats; well under the election timeout.
    pub heartbeat: Duration,
    /// Seeds the election-timeout jitter; make it differ between nodes.
    pub seed: u64,
    /// The most bytes of applied records kept in memory. Older ones are read back from disk
    /// when a lagging follower needs them.
    pub log_window_bytes: usize,
    /// Entries kept before a snapshot this node takes, so a follower that far behind it is sent
    /// the log rather than the whole snapshot (a learner's brief cut over a WAN, say). Their terms
    /// stay in memory, a few bytes a term; their records on disk. Fewer are kept past a change
    /// of members: none before the latest at or before the snapshot.
    pub log_tail_entries: u64,
    /// A leader with this many bytes of its proposals not yet committed is backlogged
    /// ([`Raft::backlogged`]): its driver takes no more from clients until they commit.
    pub max_uncommitted_bytes: usize,
}

/// Where the log ended when a batch of outputs was taken ([`Raft::log_end`]), for
/// [`Raft::on_persisted`] once the batch is durable.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LogEnd {
    /// The last index.
    pub index: u64,
    /// Its term.
    pub term: u64,
}

/// Bytes of a snapshot to read, for [`Raft::on_snapshot_read`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SnapshotRead {
    /// The index the snapshot is of.
    pub index: u64,
    /// Where in it to read from.
    pub offset: u64,
}

/// A node's part in the current term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Role {
    /// Following a leader, or waiting for one.
    Follower,
    /// Asking whether it could win an election, without disturbing anyone's term.
    PreCandidate,
    /// Standing for election.
    Candidate,
    /// Leading the term.
    Leader,
}

/// A cluster member: its ID, and where it's reached, which Raft carries without using.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Member {
    /// Its ID.
    pub id: NodeId,
    /// Where peers reach it.
    pub peer: String,
    /// Where clients do.
    pub client: String,
    /// Where operators do.
    pub admin: String,
    /// It takes the log but doesn't vote.
    pub learner: bool,
}

/// A durable snapshot: the state machine as of a committed index, which stands in for the log
/// up to it. Its bytes are on disk; Raft ships them, read back a chunk at a time, without
/// reading them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The last index it covers.
    pub index: u64,
    /// That entry's term.
    pub term: u64,
    /// How many bytes it is.
    pub len: u64,
    /// The checksum of its bytes and its members, as its file has it: a follower checks what it
    /// was sent against it.
    pub checksum: u64,
    /// The members as of `index`.
    pub members: Vec<Member>,
}

/// A change to make durable, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Persist {
    /// A snapshot this node saved ([`Raft::on_snapshot_saved`]) is durable: drop the log up to
    /// `log`, which is at or before it, and older snapshots than `snapshot`.
    Compact {
        /// The last index of the log to drop.
        log: u64,
        /// The snapshot's index.
        snapshot: u64,
    },
    /// Part of a snapshot arriving from the leader: write it at `offset`. Offset 0 starts it
    /// afresh.
    SnapshotChunk {
        /// The snapshot's index.
        index: u64,
        /// Where in it.
        offset: u64,
        /// The bytes.
        bytes: Vec<u8>,
    },
    /// The snapshot arriving from the leader is whole: make it the latest, atomically, then drop
    /// the log up to its index. With `replace_log`, drop the whole log: the next record is
    /// `index + 1`.
    InstallSnapshot {
        /// The snapshot.
        snapshot: Snapshot,
        /// Discard the whole log.
        replace_log: bool,
    },
    /// The term and vote.
    Meta(Meta),
    /// Whole records, continuing the log.
    Append(Vec<u8>),
    /// Remove the records from this index on.
    TruncateFrom(u64),
    /// The log is no longer in doubt ([`Entries::doubt`]), once the operations before this one
    /// are durable.
    Trust,
}

/// What one batch of inputs produced.
#[derive(Debug, Default)]
pub struct Outputs {
    /// Persist these first, in order.
    pub persist: Vec<Persist>,
    /// A leader's `AppendEntries`: send these at once, without waiting for `persist`.
    pub appends: Vec<(NodeId, Frame)>,
    /// Once `persist` is durable, send these frames.
    pub sends: Vec<(NodeId, Frame)>,
    /// Once `persist` is written, read the records from each of these indices on: at least one,
    /// up to [`MAX_READ_BYTES`] or the end of a log file. Give them to [`Raft::on_read`], then take
    /// outputs. A batch may ask again for a read an earlier one asked for and is still under way.
    pub reads: Vec<u64>,
    /// Once `persist` is written, read the latest snapshot's bytes from each of these offsets: up
    /// to [`MAX_READ_BYTES`], or its end. Give them to [`Raft::on_snapshot_read`], then take
    /// outputs.
    pub snapshot_reads: Vec<SnapshotRead>,
    /// This node became leader in this batch: the caller should propose an entry of the new
    /// term, so that the entries of earlier terms commit.
    pub became_leader: bool,
    /// The peers to connect to changed, to these: the members, and a member being added.
    pub peers: Option<Vec<Member>>,
    /// The `Members` entry for a change [`Raft::add_member`] or [`Raft::remove_member`] began
    /// was proposed at this index.
    pub change_proposed: Option<u64>,
    /// A member being added didn't catch up, or this node stopped leading first: the change
    /// was dropped.
    pub change_failed: bool,
    /// As leader, how long each of its entries took from proposed to each [`Stage`] it reached
    /// in this batch, in nanoseconds.
    pub latencies: Vec<(Stage, u64)>,
    /// As leader, reads ([`Raft::read_index`]) confirmed: answer each once the state has applied
    /// up to its index.
    pub confirmed_reads: Vec<ReadIndex>,
    /// Reads dropped, as this node stopped leading: ask the new leader.
    pub dropped_reads: Vec<u64>,
}

/// A stage a leader's own entry reaches, timed from when it was proposed: see
/// [`Outputs::latencies`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum Stage {
    /// The leader's own copy is durable.
    Persisted,
    /// A quorum has it durable.
    Committed,
    /// The node has applied it ([`Raft::on_applied`]), and so answered it.
    Applied,
}

/// Times the entries proposed at, up to `index`, as having reached `stage` by `now`.
fn reached(waiting: &mut VecDeque<(u64, u64)>, index: u64, stage: Stage, now: u64, out: &mut Outputs) {
    while let Some(&(_, at)) = waiting.front().filter(|&&(i, _)| i <= index) {
        waiting.pop_front();
        out.latencies.push((stage, now.saturating_sub(at)));
    }
}

/// The highest of `values` that `quorum` of them reach (the `quorum`th highest), if there are that
/// many, without allocating: it's taken on every ack. One per member, at most [`MAX_MEMBERS`]: past
/// that, `None`, which holds back what waits for a quorum rather than panic on members off the wire.
fn quorum_highest(values: impl IntoIterator<Item = u64>, quorum: usize) -> Option<u64> {
    let mut all = [0; MAX_MEMBERS];
    let mut len = 0;
    for value in values {
        *all.get_mut(len)? = value;
        len += 1;
    }
    let nth = quorum.checked_sub(1).filter(|&nth| nth < len)?;
    Some(*all[..len].select_nth_unstable_by(nth, |a, b| b.cmp(a)).1)
}

#[derive(Debug)]
struct Peer {
    id: NodeId,
    /// The next index to send it.
    next: u64,
    /// The highest index known to be durable on it.
    matched: u64,
    /// When it last answered this leader.
    heard_at: u64,
    /// The index of its latest durable snapshot, as it last said.
    snapshot_index: u64,
    /// Sending it a snapshot.
    sending: Option<Sending>,
    /// Chunks for it are being read, to send once they are.
    chunk_wanted: bool,
    /// The protocol and service versions it supports, as it last said.
    versions: Option<(u32, u32)>,
    /// After a refusal, probing where its log matches: the `prevIndex` of the one append to it,
    /// sent from `next` (not moved on) until it's answered. Nothing is pipelined meanwhile.
    probe: Option<u64>,
    /// The probe went without its records, on disk only: it goes again once they're read.
    probe_read: bool,
    /// The latest read round it echoed, in this term ([`Raft::read_index`]).
    read_round: u64,
}

/// A leadership transfer under way.
#[derive(Debug, Clone, Copy)]
struct Transfer {
    to: NodeId,
    /// Given up then, if this node still leads.
    by: u64,
    /// The last index when it began: `TimeoutNow` goes once `to` has it.
    upto: u64,
    sent: bool,
}

/// A member being added, catching up before it's proposed: in rounds, each to the last index as
/// the round began. One done within an election timeout means it has caught up.
#[derive(Debug)]
struct CatchUp {
    member: Member,
    started: u64,
    round_start: u64,
    round_end: u64,
}

/// A snapshot on its way to a peer.
#[derive(Debug, Clone, Copy)]
struct Sending {
    /// Which, by index.
    index: u64,
    /// How much of it the peer has, as it last said.
    acked: u64,
    /// How much has been sent it.
    sent: u64,
    /// It acked more since the last heartbeat.
    progressed: bool,
    /// What it had acked when chunks were last sent again from there.
    rewound: Option<u64>,
}

/// One member's Raft state.
#[derive(Debug)]
pub struct Raft {
    config: Config,
    meta: Meta,
    role: Role,
    leader: Option<NodeId>,
    log: Entries,
    /// The term of the entry before the log's first, which a snapshot covers (0 for none).
    base_term: u64,
    /// The members as of the entry before the log's first: the snapshot's, or the bootstrap
    /// ones. The log's `Members` entries follow.
    base_members: Vec<Member>,
    catching_up: Option<CatchUp>,
    /// Handing the leadership over.
    transfer: Option<Transfer>,
    /// The protocol and service versions this node supports, and those the cluster requires.
    advertised: (u32, u32),
    required: (u32, u32),
    /// Stopping once the snapshot of this index is durable, and, as leader, telling peers until
    /// this time.
    stop: Option<(u64, u64)>,
    /// The latest input's time.
    now: u64,
    /// As leader, each of its entries not yet committed: its index, when it was proposed, and
    /// its payload's length; and those lengths summed.
    proposed_at: VecDeque<(u64, u64, usize)>,
    /// As leader, its entries not yet durable here, and not yet applied: each index, and when it
    /// was proposed.
    unpersisted: VecDeque<(u64, u64)>,
    unapplied: VecDeque<(u64, u64)>,
    uncommitted_bytes: usize,
    /// Records read back from disk, by the index of the first in each run, until the next
    /// [`Raft::take_outputs`].
    fetched: Vec<(u64, Vec<Vec<u8>>)>,
    durable: u64,
    commit: u64,
    peers: Vec<Peer>,
    votes: Vec<NodeId>,
    /// How many of `votes` are in doubt: winning takes one more for each.
    doubted: usize,
    /// The pre-vote under way: when it started, which is never the same twice, nor before a
    /// restart, since the time never goes backwards.
    round: u64,
    /// In doubt since restarting in this term ([`Entries::doubt`]).
    doubt: Option<u64>,
    election_at: u64,
    heartbeat_at: u64,
    /// When a leader was last heard from, for refusing pre-votes and promotion while it's alive.
    /// Starting counts: one may be alive that hasn't been heard yet.
    leader_seen_at: Option<u64>,
    rng: u64,
    out: Outputs,
    /// The latest durable snapshot, for followers that need what it compacted.
    snapshot: Option<Snapshot>,
    /// Chunks of it read back from disk, by offset, until the next [`Raft::take_outputs`].
    fetched_chunks: Vec<(u64, Vec<u8>)>,
    /// The latest append with records, by everything in it but its records, the index after its
    /// last, and its frame: for the next peer at the same index, until [`Raft::take_outputs`].
    shared_append: Option<([u64; 6], u64, Frame)>,
    /// A snapshot arriving from the leader, chunk by chunk, and how much of it has.
    incoming: Option<Snapshot>,
    /// The checksum of its bytes so far, to check against the leader's once they're all here.
    incoming_sum: u64,
    /// A snapshot from the leader replaced the state since [`Raft::take_installed`].
    installed: bool,
    /// As leader, the reads waiting to be confirmed.
    reads: reads::Reads,
}

impl Raft {
    /// A member restarting from its durable `meta`, latest `snapshot` and `log`, all durable.
    /// Entries the snapshot covers are dropped.
    ///
    /// # Panics
    ///
    /// If the configuration is impossible, or the records are damaged or don't continue from the
    /// snapshot.
    #[must_use]
    pub fn new(config: Config, meta: Meta, snapshot: Option<Snapshot>, log: Entries, now: u64) -> Self {
        assert!(config.members.len() <= MAX_MEMBERS);
        assert!(config.members.is_empty() || config.members.iter().any(|m| !m.learner), "a new cluster has a voter");
        assert!(
            config.members.is_empty() || config.members.iter().any(|m| m.id == config.id),
            "a node starting a cluster is one of its members"
        );
        assert!(config.members.iter().all(Member::fits), "addresses fit the wire");
        assert!(config.heartbeat * 2 < config.election_timeout);
        assert!(config.election_timeout < MAX_TIMEOUT, "times in nanoseconds can't overflow");
        let (base_index, mut base_term) = snapshot.as_ref().map_or((0, 0), |s| (s.index, s.term));
        let base_members = snapshot.as_ref().map_or_else(|| config.members.clone(), |s| s.members.clone());
        let doubt = log.doubt.then_some(meta.term);
        let mut log = log;
        // What's on disk of the tail before the snapshot is kept, as when the snapshot was taken.
        let to = if log.terms.is_empty() { base_index } else { log.compact_point(base_index, config.log_tail_entries) };
        if to < base_index {
            base_term = log.term_at(to).expect("the compaction point is in the log");
        }
        log.trim_to(to);
        if log.terms.is_empty() {
            log = Entries::starting_at(base_index + 1);
        }
        assert!(
            log.first == base_index + 1 || (log.first <= base_index && log.last() >= base_index),
            "the log continues from the snapshot, or a tail before it"
        );
        // Mixed first: `seed | 1` alone makes seeds 2 and 3 the same, and two nodes with the same
        // jitter split every vote.
        let rng = config.seed.wrapping_add(0x9e37_79b9_7f4a_7c15).wrapping_mul(0xbf58_476d_1ce4_e5b9) | 1;
        let mut raft = Self {
            config,
            meta,
            role: Role::Follower,
            leader: None,
            log,
            base_term,
            base_members,
            catching_up: None,
            transfer: None,
            advertised: (0, 0),
            required: (0, 0),
            stop: None,
            now,
            proposed_at: VecDeque::new(),
            unpersisted: VecDeque::new(),
            unapplied: VecDeque::new(),
            uncommitted_bytes: 0,
            fetched: Vec::new(),
            durable: 0,
            commit: base_index,
            peers: Vec::new(),
            votes: Vec::new(),
            doubted: 0,
            round: 0,
            doubt,
            election_at: 0,
            heartbeat_at: 0,
            leader_seen_at: Some(now),
            rng,
            out: Outputs::default(),
            snapshot,
            fetched_chunks: Vec::new(),
            shared_append: None,
            incoming: None,
            incoming_sum: crate::codec::CHECKSUM_START,
            installed: false,
            reads: reads::Reads::default(),
        };
        raft.durable = raft.last_index();
        raft.rebuild_peers();
        raft.reset_election(now);
        raft
    }

    /// Stops this node once it has the snapshot of `index` durable; as leader, after telling its
    /// peers to stop too, with its next two heartbeats.
    pub fn stop(&mut self, index: u64) {
        let until = self.now + 2 * nanos(self.config.heartbeat);
        self.stop = Some((index, until));
        self.heartbeat_at = self.now;
    }

    /// As leader, how many entries each other member is behind its log.
    #[must_use]
    pub fn follower_lag(&self) -> Vec<(NodeId, u64)> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        let last = self.last_index();
        self.peers.iter().filter(|p| self.is_member(p.id)).map(|p| (p.id, last.saturating_sub(p.matched))).collect()
    }

    /// As leader, each member, and the highest index known durable on it.
    #[must_use]
    pub fn progress(&self) -> Vec<(&Member, u64)> {
        if self.role != Role::Leader {
            return Vec::new();
        }
        let matched = |id| {
            if id == self.config.id {
                self.durable
            } else {
                self.peers.iter().find(|p| p.id == id).map_or(0, |p| p.matched)
            }
        };
        self.members().iter().map(|m| (m, matched(m.id))).collect()
    }

    /// The node has stopped: it should do nothing more.
    #[must_use]
    pub fn stopped(&self) -> bool {
        self.stop.is_some_and(|(index, until)| self.snapshot_index() >= index && self.now >= until)
    }

    /// Whether this node doubts its log ([`Entries::doubt`]).
    #[must_use]
    pub fn in_doubt(&self) -> bool {
        self.doubt.is_some()
    }

    /// This node's ID.
    #[must_use]
    pub fn id(&self) -> NodeId {
        self.config.id
    }

    /// Its role.
    #[must_use]
    pub fn role(&self) -> Role {
        self.role
    }

    /// The current term.
    #[must_use]
    pub fn term(&self) -> u64 {
        self.meta.term
    }

    /// The leader of the current term, if known.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        self.leader
    }

    /// The highest index known committed.
    #[must_use]
    pub fn commit_index(&self) -> u64 {
        self.commit
    }

    /// As leader, whether its proposals not yet committed come to
    /// [`Config::max_uncommitted_bytes`] or more. It still takes proposals: this is for the
    /// driver, to stop taking more from clients until a quorum catches up.
    #[must_use]
    pub fn backlogged(&self) -> bool {
        self.uncommitted_bytes >= self.config.max_uncommitted_bytes
    }

    /// The last index in the log.
    #[must_use]
    pub fn last_index(&self) -> u64 {
        self.log.last()
    }

    /// The last index in the log, and its term: note it on taking outputs, and give it to
    /// [`Raft::on_persisted`] once they're durable.
    #[must_use]
    pub fn log_end(&self) -> LogEnd {
        LogEnd { index: self.last_index(), term: self.last_term() }
    }

    /// The term of the entry at `index`, if the log holds it (or a snapshot covers it).
    #[must_use]
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index + 1 == self.log.first {
            return Some(self.base_term);
        }
        self.log.term_at(index)
    }

    /// The index the latest snapshot covers to (0 for none). The log holds what follows it, and
    /// may hold a tail before it ([`Config::log_tail_entries`]).
    #[must_use]
    pub fn snapshot_index(&self) -> u64 {
        self.snapshot.as_ref().map_or(0, |s| s.index)
    }

    /// The latest snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// When [`Raft::on_tick`] next has something to do.
    #[must_use]
    pub fn next_deadline(&self) -> u64 {
        if self.role == Role::Leader { self.heartbeat_at } else { self.election_at }
    }

    /// Appends `payload` to the log if this node leads, returning its index.
    ///
    /// # Panics
    ///
    /// If `payload` is over [`codec::MAX_PAYLOAD_BYTES`]; callers check what they're given.
    pub fn propose(&mut self, payload: &[u8]) -> Option<u64> {
        if self.role != Role::Leader {
            return None;
        }
        let index = self.last_index() + 1;
        self.append(Record { term: self.meta.term, index, payload });
        self.proposed_at.push_back((index, self.now, payload.len()));
        self.unpersisted.push_back((index, self.now));
        self.unapplied.push_back((index, self.now));
        self.uncommitted_bytes += payload.len();
        if !self.peers.iter().any(|p| self.is_voter(p.id)) {
            self.advance_commit();
        }
        Some(index)
    }

    /// The time is `now`, for timing what's proposed next, without acting on it as
    /// [`Raft::on_tick`] does.
    pub fn set_time(&mut self, now: u64) {
        self.now = now;
    }

    /// Time passed.
    pub fn on_tick(&mut self, now: u64) {
        self.now = now;
        match self.role {
            Role::Leader if now >= self.heartbeat_at => {
                let timeout = nanos(self.config.election_timeout);
                let peers = self.peers.iter().filter(|p| self.is_voter(p.id) && p.heard_at + timeout >= now);
                let heard = peers.count() + usize::from(self.is_voter(self.config.id));
                if heard < self.quorum() {
                    // Check quorum: a leader cut off from a majority stands down, so clients
                    // find the leader the majority elects.
                    self.become_follower(self.meta.term, now);
                    return;
                }
                for i in 0..self.peers.len() {
                    self.send_append(i, true);
                }
                if let Some((index, _)) = self.stop {
                    for i in 0..self.peers.len() {
                        self.send(self.peers[i].id, &wire::Stop { term: self.meta.term, index });
                    }
                }
                self.heartbeat_at = now + nanos(self.config.heartbeat);
                if self.catching_up.as_ref().is_some_and(|c| now > c.started + CATCH_UP_TIMEOUTS * timeout) {
                    self.drop_change();
                }
                if self.transfer.is_some_and(|t| now > t.by) {
                    self.transfer = None;
                }
            }
            Role::Follower | Role::PreCandidate | Role::Candidate if now >= self.election_at => {
                // One that knows members stands, even if it isn't among them yet: added, it may
                // have the most of the log before it has the entry that adds it. A learner
                // doesn't ([`Raft::may_stand`]).
                if !self.may_stand() {
                    // Joining, or with its leader gone, it doesn't know who leads: clients it would
                    // send to a stale leader try elsewhere.
                    self.leader = None;
                    self.reset_election(now);
                } else {
                    self.start_pre_vote(now);
                }
            }
            _ => {}
        }
    }

    /// A peer's message. Messages that aren't Raft's are ignored, as are acks from nodes this
    /// one isn't sending to. Any node may lead: a member this node hasn't heard was added.
    pub fn on_message(&mut self, from: NodeId, message: &wire::Decoded<'_>, now: u64) {
        self.now = now;
        if from == self.config.id {
            return;
        }
        let peer = self.peers.iter().position(|p| p.id == from);
        match (message, peer) {
            (wire::Decoded::RequestVote(m), _) => self.on_request_vote(from, m, now),
            (wire::Decoded::Vote(m), _) => self.on_vote(from, m, now),
            (wire::Decoded::AppendEntries(m), _) => self.on_append_entries(from, m, now),
            (wire::Decoded::InstallSnapshot(m), _) => self.on_install_snapshot(from, m, now),
            (wire::Decoded::TimeoutNow(m), _) => self.on_timeout_now(from, m, now),
            (wire::Decoded::Stop(m), _) if m.term() == self.meta.term && self.leader == Some(from) => {
                self.stop = Some((m.index(), now));
            }
            (wire::Decoded::AppendAck(m), Some(peer)) => self.on_append_ack(peer, m, now),
            (wire::Decoded::SnapshotAck(m), Some(peer)) => self.on_snapshot_ack(peer, m, now),
            _ => {}
        }
    }

    /// The batches up to one taken when the log ended at `end` ([`Raft::log_end`]) are durable,
    /// at `now`.
    /// If the entry there has been replaced since (truncated, or the log replaced by a snapshot),
    /// what's durable of the log now is unknown, and a later batch's will say.
    pub fn on_persisted(&mut self, end: LogEnd, now: u64) {
        // Never before the input it follows.
        self.now = self.now.max(now);
        // Log matching: the same entry there, the same log up to it.
        if self.term_at(end.index) == Some(end.term) {
            self.durable = end.index;
            reached(&mut self.unpersisted, self.durable, Stage::Persisted, now, &mut self.out);
        }
        if self.role == Role::Leader {
            self.advance_commit();
        }
    }

    /// What to persist and send. A leader replicates anything new here, once per batch.
    pub fn take_outputs(&mut self) -> Outputs {
        if self.role == Role::Leader {
            // First, so the appends below carry a round it starts.
            self.confirm_reads();
            for i in 0..self.peers.len() {
                // A peer behind the snapshot moves on by its snapshot acks and heartbeats. Each
                // append moves `next` on, to `matched + MAX_IN_FLIGHT` at most, so that bounds the
                // loop; a probe, which doesn't, isn't pipelined.
                for _ in 0..=MAX_IN_FLIGHT {
                    let p = &self.peers[i];
                    let pipelined = p.next >= self.log.first
                        && p.next <= self.replicate_to()
                        && p.next <= p.matched + MAX_IN_FLIGHT
                        && p.probe.is_none();
                    if !pipelined {
                        break;
                    }
                    // Records on disk only: they're read, and sent in the next round.
                    if !self.send_append(i, false) {
                        break;
                    }
                }
                if self.peers[i].probe_read {
                    self.send_append(i, false);
                }
                if self.peers[i].chunk_wanted {
                    self.send_snapshot_chunks(i);
                }
            }
        }
        self.fetched.clear();
        self.fetched_chunks.clear();
        self.shared_append = None;
        std::mem::take(&mut self.out)
    }
}

impl Member {
    /// Voter `id`, reached at these addresses. Make it a learner by setting
    /// [`Member::learner`].
    #[must_use]
    pub fn new(id: NodeId, peer: impl Into<String>, client: impl Into<String>, admin: impl Into<String>) -> Self {
        Self { id, peer: peer.into(), client: client.into(), admin: admin.into(), learner: false }
    }

    /// From the wire, where an address that isn't UTF-8 is mangled, not refused: it only
    /// fails to connect.
    fn read(id: NodeId, learner: u8, peer: &[u8], client: &[u8], admin: &[u8]) -> Self {
        let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
        Self { id, peer: text(peer), client: text(client), admin: text(admin), learner: learner == 1 }
    }

    /// Its addresses fit the wire's.
    fn fits(&self) -> bool {
        [&self.peer, &self.client, &self.admin].iter().all(|a| a.len() <= usize::from(u8::MAX))
    }
}

impl Peer {
    fn new(id: NodeId, next: u64, now: u64) -> Self {
        Self {
            id,
            next,
            matched: 0,
            heard_at: now,
            snapshot_index: 0,
            sending: None,
            chunk_wanted: false,
            versions: None,
            probe: None,
            probe_read: false,
            read_round: 0,
        }
    }
}

#[cfg(test)]
mod tests;
