//! Raft, sans-IO, over opaque records: elections with pre-vote, log replication and commit.
//!
//! It knows nothing of sessions or services: [`Raft::propose`] takes a payload and the caller
//! reads committed payloads back with [`Raft::entry`]. Inputs are `on_*` calls with the time;
//! [`Raft::take_outputs`] returns what to persist and what to send.
//!
//! The log since the snapshot is on disk; memory holds every entry's term, and the records of
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
//! **Stopping** ([`Raft::stop`]): a leader shutting the cluster down sends its peers `Stop` with
//! its heartbeats for a couple of them, then stops; a follower stops on `Stop` once it has the
//! snapshot it names durable. Nothing in the log stops a node, so a restart never stops again.
//!
//! **Doubt** ([`Entries::doubt`]): a node whose log dropped a damaged tail on restart may have
//! lost entries it acknowledged, since bit rot in the last record looks like a torn write. It
//! can't tell, so it doubts until it has everything it could have acknowledged: an entry, or a
//! snapshot, of a term after the one it restarted in (by log matching, it then has the log of a
//! leader elected since), or the whole log of the leader it follows, as an append that reaches
//! that leader's last index says (every entry it acknowledged is in that leader's log, or
//! committed and so there too). The driver then ends the doubt on disk ([`Persist::Trust`]).
//! Meanwhile its votes say so, and an election needs a vote more than a majority for each voter
//! in doubt, its own included (or every member): however many of those lost a committed entry,
//! the voters then still include one that holds it and turns down a candidate without it. After
//! a power cut that tears every node's log, all of them doubt, and an election needs every
//! member up.
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
use std::time::Duration;

use crate::codec::{self, MAX_ENTRIES_PER_APPEND, Record};
use crate::meta::Meta;
use crate::wire;

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

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        s.parse().map(Self)
    }
}

/// The most members a cluster can have.
pub const MAX_MEMBERS: usize = 7;
/// The most record bytes one `AppendEntries` carries, leaving room for its header in a frame.
const MAX_APPEND_BYTES: usize = 1_000_000;
/// How far a leader replicates to a peer ahead of its acks before it waits for a heartbeat.
const MAX_IN_FLIGHT: u64 = 4096;
/// A new member that hasn't caught up in this many election timeouts isn't added.
const CATCH_UP_TIMEOUTS: u64 = 100;
/// How far a leader sends a snapshot to a peer ahead of its acks, as reads of up to
/// [`MAX_READ_BYTES`].
const MAX_SNAPSHOT_IN_FLIGHT: u64 = 8 * MAX_READ_BYTES as u64;
/// The most record bytes one of [`Outputs::reads`] returns, past its first record.
pub const MAX_READ_BYTES: usize = MAX_APPEND_BYTES;

const _: () = assert!(MAX_APPEND_BYTES < codec::MAX_FRAME_BYTES - 1024);
const _: () = assert!(MAX_IN_FLIGHT >= MAX_ENTRIES_PER_APPEND as u64);

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

/// Timing and identity.
#[derive(Debug, Clone)]
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
}

/// A node's part in the current term.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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
pub struct Member {
    /// Its ID.
    pub id: NodeId,
    /// Where peers reach it.
    pub peer: String,
    /// Where clients do.
    pub client: String,
    /// Where operators do.
    pub admin: String,
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
    /// this index.
    Compact(u64),
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
    pub appends: Vec<(NodeId, Vec<u8>)>,
    /// Once `persist` is durable, send these frames.
    pub sends: Vec<(NodeId, Vec<u8>)>,
    /// Once `persist` is written, read the records from each of these indices on: at least one,
    /// up to [`MAX_READ_BYTES`] or the end of a log file. Give them to [`Raft::on_read`], then take
    /// outputs. A batch may ask again for a read an earlier one asked for and is still under way.
    pub reads: Vec<u64>,
    /// Once `persist` is written, read the latest snapshot's bytes from each of these offsets: up
    /// to [`MAX_READ_BYTES`], or its end. Give them to [`Raft::on_snapshot_read`], then take
    /// outputs.
    pub snapshot_reads: Vec<(u64, u64)>,
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
    /// As leader, how long each entry committed in this batch took from proposed, in
    /// nanoseconds.
    pub commit_latencies: Vec<u64>,
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
    /// As leader, when each of its entries not yet committed was proposed.
    proposed_at: VecDeque<(u64, u64)>,
    /// Records read back from disk, by the index of the first in each run, until the next
    /// [`Raft::take_outputs`].
    fetched: Vec<(u64, Vec<Vec<u8>>)>,
    durable: u64,
    commit: u64,
    peers: Vec<Peer>,
    votes: Vec<NodeId>,
    /// How many of `votes` are in doubt: winning takes one more for each.
    doubted: usize,
    /// In doubt since restarting in this term ([`Entries::doubt`]).
    doubt: Option<u64>,
    election_at: u64,
    heartbeat_at: u64,
    /// When a leader was last heard from, for refusing pre-votes while it's alive.
    leader_seen_at: Option<u64>,
    rng: u64,
    out: Outputs,
    /// The latest durable snapshot, for followers that need what it compacted.
    snapshot: Option<Snapshot>,
    /// Chunks of it read back from disk, by offset, until the next [`Raft::take_outputs`].
    fetched_chunks: Vec<(u64, Vec<u8>)>,
    /// A snapshot arriving from the leader, chunk by chunk, and how much of it has.
    incoming: Option<Snapshot>,
    /// A snapshot from the leader replaced the state since [`Raft::take_installed`].
    installed: bool,
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
        assert!(
            config.members.is_empty() || config.members.iter().any(|m| m.id == config.id),
            "a node starting a cluster is one of its members"
        );
        assert!(config.members.iter().all(Member::fits), "addresses fit the wire");
        assert!(config.heartbeat * 2 < config.election_timeout);
        assert!(config.election_timeout < MAX_TIMEOUT, "times in nanoseconds can't overflow");
        let (base_index, base_term) = snapshot.as_ref().map_or((0, 0), |s| (s.index, s.term));
        let base_members = snapshot.as_ref().map_or_else(|| config.members.clone(), |s| s.members.clone());
        let doubt = log.doubt.then_some(meta.term);
        let mut log = log;
        log.trim_to(base_index);
        if log.terms.is_empty() {
            log = Entries::starting_at(base_index + 1);
        }
        assert_eq!(log.first, base_index + 1, "the log continues from the snapshot");
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
            fetched: Vec::new(),
            durable: 0,
            commit: base_index,
            peers: Vec::new(),
            votes: Vec::new(),
            doubted: 0,
            doubt,
            election_at: 0,
            heartbeat_at: 0,
            leader_seen_at: None,
            rng,
            out: Outputs::default(),
            snapshot,
            fetched_chunks: Vec::new(),
            incoming: None,
            installed: false,
        };
        raft.durable = raft.last_index();
        raft.rebuild_peers();
        raft.reset_election(now);
        raft
    }

    /// The peers to connect to: the members, and a member being added.
    #[must_use]
    pub fn peers(&self) -> Vec<Member> {
        let learner = self.catching_up.as_ref().map(|c| c.member.clone());
        self.members().iter().cloned().chain(learner).collect()
    }

    /// The members, as of the latest `Members` entry in the log, or the snapshot.
    #[must_use]
    pub fn members(&self) -> &[Member] {
        self.log.configs.last().map_or(&self.base_members, |(_, members)| members)
    }

    /// The members as of `index`, which is in the log or the snapshot.
    #[must_use]
    pub fn members_at(&self, index: u64) -> Vec<Member> {
        let config = self.log.configs.iter().rev().find(|(at, _)| *at <= index);
        config.map_or_else(|| self.base_members.clone(), |(_, members)| members.clone())
    }

    /// Starts adding `member`: it's sent the log, without a vote, until it has caught up, then
    /// the members with it are proposed ([`Outputs::change_proposed`]), or, if it doesn't, the
    /// change is dropped ([`Outputs::change_failed`]). Refused (false) unless this node leads,
    /// has committed an entry of its term, no other change is under way, and there's room.
    pub fn add_member(&mut self, member: Member) -> bool {
        if !self.can_change() || member.id.0 == 0 || self.is_member(member.id) || self.members().len() >= MAX_MEMBERS {
            return false;
        }
        if !member.fits() {
            return false;
        }
        let (now, last) = (self.now, self.last_index());
        self.catching_up = Some(CatchUp { member, started: now, round_start: now, round_end: last });
        self.peers_changed();
        true
    }

    /// Starts handing the leadership to member `to`, or with `None` to the member most caught up,
    /// returning which. It's sent `TimeoutNow` once it has the log as it is now; this node gives
    /// up after an election timeout ([`Raft::transferring`] goes false while it still leads).
    /// Refused (`None`) unless this node leads, isn't transferring already, and `to` is another
    /// member, heard from within an election timeout.
    pub fn transfer_leadership(&mut self, to: Option<NodeId>) -> Option<NodeId> {
        if self.role != Role::Leader || self.transfer.is_some() || self.stop.is_some() {
            return None;
        }
        let alive = self.now.saturating_sub(nanos(self.config.election_timeout));
        let voters = self.peers.iter().filter(|p| self.is_member(p.id) && p.heard_at >= alive);
        let target = if let Some(to) = to {
            voters.map(|p| p.id).find(|&id| id == to)?
        } else {
            voters.max_by_key(|p| p.matched)?.id
        };
        let (by, upto) = (self.now + nanos(self.config.election_timeout), self.last_index());
        self.transfer = Some(Transfer { to: target, by, upto, sent: false });
        self.try_transfer();
        Some(target)
    }

    /// As leader, not handing over, whether every member heard from within an election timeout,
    /// this node included, and a quorum, has the snapshot of `index` durable.
    #[must_use]
    pub fn all_have_snapshot(&self, index: u64) -> bool {
        if self.role != Role::Leader || self.transfer.is_some() || self.snapshot_index() < index {
            return false;
        }
        let alive = self.now.saturating_sub(nanos(self.config.election_timeout));
        let members = self.peers.iter().filter(|p| self.is_member(p.id));
        let (mut have, mut missing) = (usize::from(self.is_member(self.config.id)), false);
        for p in members {
            if p.snapshot_index >= index {
                have += 1;
            } else if p.heard_at >= alive {
                missing = true;
            }
        }
        !missing && have >= self.quorum()
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

    /// The node has stopped: it should do nothing more.
    #[must_use]
    pub fn stopped(&self) -> bool {
        self.stop.is_some_and(|(index, until)| self.snapshot_index() >= index && self.now >= until)
    }

    /// The protocol and service versions this node's code supports, which it tells the leader.
    pub fn advertise(&mut self, protocol: u32, service: u32) {
        self.advertised = (protocol, service);
    }

    /// The protocol and service versions the cluster runs: a member that doesn't support them
    /// isn't added.
    pub fn require(&mut self, protocol: u32, service: u32) {
        self.required = (protocol, service);
    }

    /// As leader, the lowest protocol and service versions the members support, as they last
    /// said: 0 for one not heard from yet.
    #[must_use]
    pub fn lowest_versions(&self) -> (u32, u32) {
        let mine = self.is_member(self.config.id).then_some(self.advertised);
        let peers = self.peers.iter().filter(|p| self.is_member(p.id)).map(|p| p.versions.unwrap_or((0, 0)));
        mine.into_iter().chain(peers).fold((u32::MAX, u32::MAX), |(p, s), (q, t)| (p.min(q), s.min(t)))
    }

    /// Whether this node doubts its log ([`Entries::doubt`]).
    #[must_use]
    pub fn in_doubt(&self) -> bool {
        self.doubt.is_some()
    }

    /// A leadership transfer is under way.
    #[must_use]
    pub fn transferring(&self) -> bool {
        self.transfer.is_some()
    }

    /// Proposes the members without `id` ([`Outputs::change_proposed`]). Refused (false) unless
    /// this node leads, has committed an entry of its term, no other change is under way, `id`
    /// is a member, and others would be left.
    pub fn remove_member(&mut self, id: NodeId) -> bool {
        if !self.can_change() || !self.is_member(id) || self.members().len() == 1 {
            return false;
        }
        let members: Vec<Member> = self.members().iter().filter(|m| m.id != id).cloned().collect();
        self.propose_members(&members);
        true
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

    /// The last index in the log.
    #[must_use]
    pub fn last_index(&self) -> u64 {
        self.log.last()
    }

    /// The last index in the log, and its term: note it on taking outputs, and give it to
    /// [`Raft::on_persisted`] once they're durable.
    #[must_use]
    pub fn log_end(&self) -> (u64, u64) {
        (self.last_index(), self.last_term())
    }

    /// The term of the entry at `index`, if the log holds it (or a snapshot covers it).
    #[must_use]
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index + 1 == self.log.first {
            return Some(self.base_term);
        }
        self.log.term_at(index)
    }

    /// The index the latest snapshot covers to (0 for none). The log holds what follows it.
    #[must_use]
    pub fn snapshot_index(&self) -> u64 {
        self.log.first - 1
    }

    /// The latest snapshot.
    #[must_use]
    pub fn snapshot(&self) -> Option<&Snapshot> {
        self.snapshot.as_ref()
    }

    /// The highest snapshot index a quorum (this node included) has durable, as a leader knows.
    #[must_use]
    pub fn quorum_snapshot_index(&self) -> u64 {
        let mut indices: Vec<u64> = self.peers.iter().map(|p| p.snapshot_index).collect();
        indices.push(self.snapshot_index());
        indices.sort_unstable_by(|a, b| b.cmp(a));
        indices[self.quorum() - 1]
    }

    /// Whether a snapshot from the leader replaced the state machine since the last call: the
    /// caller then restores it from [`Raft::snapshot`] before applying anything more.
    pub fn take_installed(&mut self) -> bool {
        std::mem::take(&mut self.installed)
    }

    /// A snapshot of committed state this node took is durable: it replaces the log up to its
    /// index. One no newer than the latest (a snapshot from the leader overtook it) is ignored.
    ///
    /// # Panics
    ///
    /// If its index isn't committed, or its term isn't the log's there.
    pub fn on_snapshot_saved(&mut self, snapshot: Snapshot) {
        if snapshot.index <= self.snapshot_index() {
            return;
        }
        assert!(snapshot.index <= self.commit, "only committed state is snapshotted");
        assert_eq!(self.term_at(snapshot.index), Some(snapshot.term));
        assert_eq!(snapshot.members, self.members_at(snapshot.index), "a snapshot's members are those at its index");
        self.log.trim_to(snapshot.index);
        self.base_term = snapshot.term;
        self.base_members.clone_from(&snapshot.members);
        self.out.persist.push(Persist::Compact(snapshot.index));
        self.use_snapshot(snapshot);
    }

    /// The latest snapshot's bytes from `offset`, read for [`Outputs::snapshot_reads`]. They're
    /// kept until the next [`Raft::take_outputs`].
    pub fn on_snapshot_read(&mut self, index: u64, offset: u64, bytes: &[u8]) {
        if self.snapshot.as_ref().is_some_and(|s| s.index == index) {
            self.fetched_chunks.push((offset, bytes.to_vec()));
        }
    }

    /// The payload of the entry at `index`, if its record is in memory: in the window, or read
    /// back since the last [`Raft::take_outputs`]. For one the log holds only on disk,
    /// [`Raft::fetch`] it.
    #[must_use]
    pub fn entry(&self, index: u64) -> Option<&[u8]> {
        let (record, _) = Record::decode(self.record(index)?).ok().flatten().expect("records are checked");
        Some(record.payload)
    }

    /// Asks for the records from `index` on, if the log holds them only on disk: see
    /// [`Outputs::reads`].
    pub fn fetch(&mut self, index: u64) {
        let on_disk = index >= self.log.first && index < self.log.window_first;
        if on_disk && !self.out.reads.contains(&index) {
            self.out.reads.push(index);
        }
    }

    /// Records read back from disk for [`Outputs::reads`]. They're kept until the next
    /// [`Raft::take_outputs`]; any that the log doesn't hold, or holds in memory, are ignored.
    pub fn on_read(&mut self, records: &[u8]) {
        let (mut first, mut run, mut at) = (None, Vec::new(), 0);
        while let Ok(Some((record, len))) = Record::decode(&records[at..]) {
            let index = first.map_or(record.index, |f| f + run.len() as u64);
            let on_disk = index >= self.log.first && index < self.log.window_first;
            if record.index != index || !on_disk || self.log.term_at(index) != Some(record.term) {
                break;
            }
            first.get_or_insert(index);
            run.push(records[at..at + len].to_vec());
            at += len;
        }
        if let Some(first) = first {
            self.fetched.push((first, run));
        }
    }

    /// Entries up to `index` are applied: their records, past the window, can leave memory once
    /// they're durable, so a read can find them.
    pub fn on_applied(&mut self, index: u64) {
        self.log.evict(index.min(self.commit).min(self.durable), self.config.log_window_bytes);
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
        self.proposed_at.push_back((index, self.now));
        if !self.peers.iter().any(|p| self.is_member(p.id)) {
            self.advance_commit();
        }
        Some(index)
    }

    /// Time passed.
    pub fn on_tick(&mut self, now: u64) {
        self.now = now;
        match self.role {
            Role::Leader if now >= self.heartbeat_at => {
                let timeout = nanos(self.config.election_timeout);
                let peers = self.peers.iter().filter(|p| self.is_member(p.id) && p.heard_at + timeout >= now);
                let heard = peers.count() + usize::from(self.is_member(self.config.id));
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
                // have the most of the log before it has the entry that adds it.
                if self.members().is_empty() {
                    // Joining, it doesn't know who leads: clients it would send to a stale leader
                    // try elsewhere.
                    self.leader = None;
                    self.reset_election(now);
                } else {
                    self.start_pre_vote(now);
                }
            }
            _ => {}
        }
    }

    /// A peer's message. Messages that aren't Raft's are ignored.
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

    /// The batches up to one taken when the log ended at `end` ([`Raft::log_end`]) are durable.
    /// If the entry there has been replaced since (truncated, or the log replaced by a snapshot),
    /// what's durable of the log now is unknown, and a later batch's will say.
    pub fn on_persisted(&mut self, end: (u64, u64)) {
        // Log matching: the same entry there, the same log up to it.
        if self.term_at(end.0) == Some(end.1) {
            self.durable = end.0;
        }
        if self.role == Role::Leader {
            self.advance_commit();
        }
    }

    /// What to persist and send. A leader replicates anything new here, once per batch.
    pub fn take_outputs(&mut self) -> Outputs {
        if self.role == Role::Leader {
            for i in 0..self.peers.len() {
                // A peer behind the snapshot moves on by its snapshot acks and heartbeats.
                while self.peers[i].next >= self.log.first
                    && self.peers[i].next <= self.replicate_to()
                    && self.peers[i].next <= self.peers[i].matched + MAX_IN_FLIGHT
                {
                    // Records on disk only: they're read, and sent in the next round.
                    if !self.send_append(i, false) {
                        break;
                    }
                }
                if self.peers[i].chunk_wanted {
                    self.send_snapshot_chunks(i);
                }
            }
        }
        self.fetched.clear();
        self.fetched_chunks.clear();
        std::mem::take(&mut self.out)
    }

    fn on_request_vote(&mut self, from: NodeId, m: &wire::RequestVoteRef<'_>, now: u64) {
        // A node that knows no members yet, joining (perhaps wiped, and started again under its old
        // ID), doesn't vote: with nothing in its log, its vote could elect a removed node, standing
        // in members the cluster has left. One that has learned members votes, even before it's
        // among them, as Raft has a new member do; its log then turns a stale candidate down.
        let up_to_date =
            (m.last_term(), m.last_index()) >= (self.last_term(), self.last_index()) && !self.members().is_empty();
        let pre_vote = m.pre_vote() == 1;
        let granted = if pre_vote {
            let leader_alive = self.role == Role::Leader
                || self.leader_seen_at.is_some_and(|at| at + nanos(self.config.election_timeout) > now);
            m.term() > self.meta.term && up_to_date && !leader_alive
        } else {
            if m.term() > self.meta.term {
                self.become_follower(m.term(), now);
            }
            let free = self.meta.vote.is_none_or(|v| v == from);
            let granted = m.term() == self.meta.term && free && up_to_date;
            if granted && self.meta.vote.is_none() {
                self.meta.vote = Some(from);
                self.out.persist.push(Persist::Meta(self.meta));
            }
            if granted {
                self.reset_election(now);
            }
            granted
        };
        let doubt = u8::from(self.doubt.is_some());
        let vote = wire::Vote { term: self.meta.term, granted: u8::from(granted), pre_vote: u8::from(pre_vote), doubt };
        self.send(from, &vote);
    }

    fn on_vote(&mut self, from: NodeId, m: &wire::VoteRef<'_>, now: u64) {
        if m.term() > self.meta.term && m.granted() == 0 {
            self.become_follower(m.term(), now);
            return;
        }
        let counts = match self.role {
            Role::PreCandidate => m.pre_vote() == 1,
            Role::Candidate => m.pre_vote() == 0 && m.term() == self.meta.term,
            Role::Follower | Role::Leader => false,
        };
        if !counts || m.granted() == 0 || self.votes.contains(&from) || !self.is_member(from) {
            return;
        }
        self.votes.push(from);
        self.doubted += usize::from(m.doubt() == Some(1));
        if self.won() {
            if self.role == Role::PreCandidate {
                self.become_candidate(now);
            } else {
                self.become_leader(now);
            }
        }
    }

    fn on_append_entries(&mut self, from: NodeId, m: &wire::AppendEntriesRef<'_>, now: u64) {
        if m.term() < self.meta.term {
            self.ack(from, false, self.last_index() + 1);
            return;
        }
        if m.term() > self.meta.term || self.role != Role::Follower {
            self.become_follower(m.term(), now);
        }
        self.leader = Some(from);
        self.leader_seen_at = Some(now);
        self.reset_election(now);

        let prev = m.prev_index();
        if prev > self.last_index() {
            self.ack(from, false, self.last_index() + 1);
            return;
        }
        if prev >= self.log.first - 1 && self.term_at(prev) != Some(m.prev_term()) {
            let hint = self.first_index_of_term_at(prev).max(self.commit + 1);
            self.ack(from, false, hint);
            return;
        }
        let Some(matched) = self.accept_records(m) else { return };
        // The leader's whole log, which has every entry this node acknowledged: by the leader
        // of this term, or of an earlier one, and then committed (or never to be).
        if m.last_index().is_some_and(|last| matched >= last) {
            self.end_doubt();
        }
        // A reordered, older append can match less than is already known committed.
        self.commit = self.commit.max(m.commit().min(matched));
        self.ack(from, true, matched);
    }

    /// Appends `m`'s records where they're new, truncating a conflicting suffix first. Returns
    /// the index up to which this log now matches the leader's, or `None` if the records are
    /// malformed (and so ignored).
    fn accept_records(&mut self, m: &wire::AppendEntriesRef<'_>) -> Option<u64> {
        let prev = m.prev_index();
        let mut expected = prev + 1;
        for record in codec::records(m.entries()) {
            let Ok(record) = record else { return None };
            if record.index != expected {
                return None;
            }
            expected += 1;
        }
        if expected - prev - 1 != u64::from(m.count()) {
            return None;
        }
        for record in codec::records(m.entries()) {
            let record = record.expect("checked above");
            if record.index < self.log.first {
                continue;
            }
            if record.index <= self.last_index() {
                if self.term_at(record.index) == Some(record.term) {
                    continue;
                }
                self.truncate_from(record.index);
            }
            self.append(record);
        }
        Some(prev + u64::from(m.count()))
    }

    fn on_append_ack(&mut self, peer: usize, m: &wire::AppendAckRef<'_>, now: u64) {
        if m.term() > self.meta.term {
            self.become_follower(m.term(), now);
            return;
        }
        if m.term() < self.meta.term || self.role != Role::Leader {
            return;
        }
        let last = self.last_index();
        let p = &mut self.peers[peer];
        p.heard_at = now;
        p.snapshot_index = p.snapshot_index.max(m.snapshot_index());
        p.versions = Some((m.protocol(), m.service_version()));
        let id = p.id;
        if self.catching_up.as_ref().is_some_and(|c| c.member.id == id) && self.held_back(peer) {
            // Too old to run what the cluster does: it isn't added, nor sent the log.
            self.drop_change();
            return;
        }
        let p = &mut self.peers[peer];
        if m.success() == 1 {
            p.sending = None;
            p.chunk_wanted = false;
            debug_assert!(m.index() <= last, "a follower can't have more than this leader sent");
            p.matched = p.matched.max(m.index().min(last));
            p.next = p.next.max(p.matched + 1);
            let (id, matched) = (p.id, p.matched);
            self.advance_commit();
            self.check_catch_up(id, matched);
            self.try_transfer();
        } else {
            p.next = m.index().clamp(p.matched + 1, last + 1);
            self.send_append(peer, true);
        }
    }

    /// Sends a peer the records from its `next`. If the first is on disk only, it's fetched and
    /// this returns false, having sent nothing, or with `probe`, an append of no records: a
    /// heartbeat, which also finds where the peer's log matches.
    fn send_append(&mut self, peer: usize, probe: bool) -> bool {
        let (next, id) = (self.peers[peer].next, self.peers[peer].id);
        if self.held_back(peer) {
            // Nothing of the log until it's known new enough, which its ack to this will say.
            if probe {
                let prev = self.log.first - 1;
                let heartbeat = wire::AppendEntries {
                    term: self.meta.term,
                    leader: self.config.id.0,
                    prev_index: prev,
                    prev_term: self.base_term,
                    commit: self.commit.min(prev),
                    count: 0,
                    entries: &[],
                    last_index: self.last_index(),
                };
                self.send_append_frame(id, &heartbeat);
            }
            return false;
        }
        if next < self.log.first {
            self.resume_snapshot(peer);
            return true;
        }
        let prev_term = self.term_at(next - 1).expect("next is past the snapshot");
        let mut bytes = Vec::new();
        let mut count = 0_u32;
        let mut index = next;
        while index <= self.replicate_to() && (count as usize) < MAX_ENTRIES_PER_APPEND {
            let Some(record) = self.record(index) else { break };
            if !bytes.is_empty() && bytes.len() + record.len() > MAX_APPEND_BYTES {
                break;
            }
            bytes.extend_from_slice(record);
            count += 1;
            index += 1;
        }
        if count == 0 && next <= self.replicate_to() {
            self.fetch(next);
            if !probe {
                return false;
            }
        }
        let append = wire::AppendEntries {
            term: self.meta.term,
            leader: self.config.id.0,
            prev_index: next - 1,
            prev_term,
            commit: self.commit,
            count,
            entries: &bytes,
            last_index: self.last_index(),
        };
        self.send_append_frame(id, &append);
        self.peers[peer].next = index;
        true
    }

    fn send_append_frame(&mut self, to: NodeId, append: &wire::AppendEntries<'_>) {
        let mut frame = Vec::new();
        codec::encode_frame(append, &mut frame).expect("an append is within a frame");
        self.out.appends.push((to, frame));
    }

    /// A heartbeat, or a refused append, for a peer behind the snapshot: starts sending it the
    /// snapshot, or, if it acked nothing since the last heartbeat (a chunk was lost), sends again
    /// from what it has.
    fn resume_snapshot(&mut self, peer: usize) {
        let index = self.snapshot.as_ref().expect("a log that starts past 1 has a snapshot").index;
        match &mut self.peers[peer].sending {
            Some(s) if s.index == index => {
                if !s.progressed {
                    s.sent = s.acked;
                }
                s.progressed = false;
            }
            sending => *sending = Some(Sending { index, acked: 0, sent: 0, progressed: false, rewound: None }),
        }
        self.send_snapshot_chunks(peer);
    }

    /// Sends a peer the snapshot's chunks from where it's sent to, as far as
    /// [`MAX_SNAPSHOT_IN_FLIGHT`] past what it has. Those not read back from disk yet are read,
    /// the rest of the window with them, and sent in the next round.
    fn send_snapshot_chunks(&mut self, peer: usize) {
        self.peers[peer].chunk_wanted = false;
        let snapshot = self.snapshot.as_ref().expect("a log that starts past 1 has a snapshot");
        let Some(mut s) = self.peers[peer].sending.filter(|s| s.index == snapshot.index) else { return };
        let window = |s: &Sending| s.acked + MAX_SNAPSHOT_IN_FLIGHT;
        while s.sent < snapshot.len && s.sent < window(&s) {
            let Some((_, chunk)) =
                self.fetched_chunks.iter().find(|(o, _)| *o == s.sent).filter(|(_, c)| !c.is_empty())
            else {
                let mut offset = s.sent;
                while offset < snapshot.len && offset < window(&s) {
                    if !self.out.snapshot_reads.contains(&(snapshot.index, offset)) {
                        self.out.snapshot_reads.push((snapshot.index, offset));
                    }
                    offset += MAX_READ_BYTES as u64;
                }
                self.peers[peer].chunk_wanted = true;
                break;
            };
            let addrs: Vec<wire::InstallSnapshotMember<'_>> = snapshot
                .members
                .iter()
                .map(|m| wire::InstallSnapshotMember {
                    id: m.id.0,
                    peer: m.peer.as_bytes(),
                    client: m.client.as_bytes(),
                    admin: m.admin.as_bytes(),
                })
                .collect();
            let message = wire::InstallSnapshot {
                term: self.meta.term,
                leader: self.config.id.0,
                last_index: snapshot.index,
                last_term: snapshot.term,
                offset: s.sent,
                done: u8::from(s.sent + chunk.len() as u64 >= snapshot.len),
                checksum: snapshot.checksum,
                member: &addrs,
                chunk,
            };
            let mut frame = Vec::new();
            codec::encode_frame(&message, &mut frame).expect("a chunk is within a frame");
            s.sent += chunk.len() as u64;
            self.out.sends.push((self.peers[peer].id, frame));
        }
        self.peers[peer].sending = Some(s);
    }

    fn on_snapshot_ack(&mut self, peer: usize, m: &wire::SnapshotAckRef<'_>, now: u64) {
        if m.term() != self.meta.term || self.role != Role::Leader {
            return;
        }
        self.peers[peer].heard_at = now;
        let Some(s) = &mut self.peers[peer].sending else { return };
        if m.offset() > s.acked {
            s.acked = m.offset();
            s.sent = s.sent.max(s.acked);
            s.progressed = true;
            self.send_snapshot_chunks(peer);
        } else if m.offset() == 0 && s.acked > 0 {
            // It has none of it (it restarted): from the start again. Any other ack that's behind
            // is a stale one, overtaken by those after it.
            (s.acked, s.sent, s.rewound) = (0, 0, None);
            self.send_snapshot_chunks(peer);
        } else if m.offset() == s.acked && s.sent > s.acked && s.rewound != Some(s.acked) {
            // It refused a chunk, out of order: one before it was lost or overtaken. Send again
            // from what it has, once for each place it stops.
            (s.sent, s.rewound) = (s.acked, Some(s.acked));
            self.send_snapshot_chunks(peer);
        }
    }

    /// A chunk of the leader's snapshot. Once whole, it replaces this node's log and state.
    fn on_install_snapshot(&mut self, from: NodeId, m: &wire::InstallSnapshotRef<'_>, now: u64) {
        if m.term() < self.meta.term {
            self.ack(from, false, self.last_index() + 1);
            return;
        }
        if m.term() > self.meta.term || self.role != Role::Follower {
            self.become_follower(m.term(), now);
        }
        self.leader = Some(from);
        self.leader_seen_at = Some(now);
        self.reset_election(now);
        if m.last_index() <= self.commit {
            // Already has all it covers.
            self.ack(from, true, m.last_index());
            return;
        }
        // A first chunk starts a snapshot, unless it's one already arriving: then it's a resend.
        if m.offset() == 0 && self.incoming.as_ref().is_none_or(|s| s.index != m.last_index()) {
            let members = m.member().map(|a| Member::read(NodeId(a.id()), a.peer(), a.client(), a.admin())).collect();
            let (index, term, checksum) = (m.last_index(), m.last_term(), m.checksum());
            self.incoming = Some(Snapshot { index, term, len: 0, checksum, members });
        }
        let Some(incoming) = self.incoming.as_mut().filter(|s| s.index == m.last_index()) else {
            // Restarted, say: the leader starts again.
            self.send(from, &wire::SnapshotAck { term: self.meta.term, offset: 0 });
            return;
        };
        if m.offset() == incoming.len {
            let bytes = m.chunk().to_vec();
            incoming.len += bytes.len() as u64;
            self.out.persist.push(Persist::SnapshotChunk { index: m.last_index(), offset: m.offset(), bytes });
        }
        if m.done() == 1 && m.offset() + m.chunk().len() as u64 == incoming.len {
            let incoming = self.incoming.take().expect("just matched");
            self.install(incoming);
            self.ack(from, true, self.commit);
        } else {
            let ack = wire::SnapshotAck { term: self.meta.term, offset: incoming.len };
            self.send(from, &ack);
        }
    }

    fn install(&mut self, snapshot: Snapshot) {
        assert!(snapshot.index > self.commit);
        // A log that already has the snapshot's last entry keeps what follows it: this node may
        // have acknowledged those entries, and the leader counted them.
        let matches = self.term_at(snapshot.index) == Some(snapshot.term);
        if matches {
            self.log.trim_to(snapshot.index);
        } else {
            self.log = Entries::starting_at(snapshot.index + 1);
            self.durable = snapshot.index;
        }
        self.base_term = snapshot.term;
        self.base_members.clone_from(&snapshot.members);
        self.commit = snapshot.index;
        self.out.persist.push(Persist::InstallSnapshot { snapshot: snapshot.clone(), replace_log: !matches });
        self.trust(snapshot.term);
        self.use_snapshot(snapshot);
        self.installed = true;
        self.peers_changed();
    }

    /// Ships `snapshot` from now on. Reads of the one before are dropped: its file goes once
    /// this batch is durable. Peers waiting for a chunk are sent this one's, from the start.
    fn use_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshot = Some(snapshot);
        self.out.snapshot_reads.clear();
        self.fetched_chunks.clear();
    }

    fn advance_commit(&mut self) {
        let mut matched: Vec<u64> = self.peers.iter().filter(|p| self.is_member(p.id)).map(|p| p.matched).collect();
        if self.is_member(self.config.id) {
            matched.push(self.durable);
        }
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let Some(&candidate) = matched.get(self.quorum() - 1) else { return };
        // Only an entry of this term commits by counting; earlier ones commit beneath it.
        if candidate > self.commit && self.term_at(candidate) == Some(self.meta.term) {
            self.commit = candidate;
            while let Some(&(index, at)) = self.proposed_at.front().filter(|&&(index, _)| index <= candidate) {
                self.proposed_at.pop_front();
                debug_assert!(index <= self.commit);
                self.out.commit_latencies.push(self.now.saturating_sub(at));
            }
        }
        // A leader that removed itself leads until that's committed, then stands down.
        if self.role == Role::Leader && !self.is_member(self.config.id) && self.commit >= self.config_index() {
            self.become_follower(self.meta.term, self.now);
        }
    }

    /// How far the log is sent to peers: all of it, but while the leadership is being handed
    /// over, only as far as it was when that began, so no voter's log is newer than the
    /// target's will be. What's proposed meanwhile goes once the transfer gives up, or is
    /// dropped by the new leader, for clients to resend.
    fn replicate_to(&self) -> u64 {
        self.transfer.map_or(self.last_index(), |t| t.upto)
    }

    /// A member being added isn't sent the log until it's known to support the versions the
    /// cluster runs: applying what it's sent, older code would run behaviour it doesn't have.
    fn held_back(&self, peer: usize) -> bool {
        let p = &self.peers[peer];
        let adding = self.catching_up.as_ref().is_some_and(|c| c.member.id == p.id);
        let new_enough = p.versions.is_some_and(|(q, s)| q >= self.required.0 && s >= self.required.1);
        adding && !new_enough
    }

    /// Sends the transfer's target `TimeoutNow` once it has the log as it was when it began.
    fn try_transfer(&mut self) {
        let Some(t) = self.transfer.as_mut().filter(|t| !t.sent) else { return };
        let (to, upto) = (t.to, t.upto);
        if self.peers.iter().any(|p| p.id == to && p.matched >= upto) {
            t.sent = true;
            self.send(to, &wire::TimeoutNow { term: self.meta.term });
        }
    }

    /// The leader of this term hands over: stand for election now.
    fn on_timeout_now(&mut self, from: NodeId, m: &wire::TimeoutNowRef<'_>, now: u64) {
        if m.term() == self.meta.term && self.leader == Some(from) && self.is_member(self.config.id) {
            self.become_candidate(now);
        }
    }

    fn is_member(&self, id: NodeId) -> bool {
        self.members().iter().any(|m| m.id == id)
    }

    /// The index of the latest `Members` entry, or of the snapshot if the log has none.
    fn config_index(&self) -> u64 {
        self.log.configs.last().map_or(self.snapshot_index(), |&(index, _)| index)
    }

    /// A change of members is under way: one being added, or one proposed and not committed.
    #[must_use]
    pub fn changing_members(&self) -> bool {
        self.catching_up.is_some() || self.config_index() > self.commit
    }

    /// Whether this leader can start a change: one at a time, each committed before the next,
    /// and not before an entry of its own term is committed.
    fn can_change(&self) -> bool {
        self.role == Role::Leader
            && self.catching_up.is_none()
            && self.config_index() <= self.commit
            && self.term_at(self.commit) == Some(self.meta.term)
    }

    /// A member being added has `matched` entries: once it catches up a round within an
    /// election timeout, the members with it are proposed.
    fn check_catch_up(&mut self, id: NodeId, matched: u64) {
        let Some(c) = self.catching_up.as_mut().filter(|c| c.member.id == id && matched >= c.round_end) else {
            return;
        };
        if self.now - c.round_start > nanos(self.config.election_timeout) {
            (c.round_start, c.round_end) = (self.now, self.log.last());
            return;
        }
        // Caught up, but too old to run what the cluster does.
        let versions = self.peers.iter().find(|p| p.id == id).and_then(|p| p.versions).unwrap_or((0, 0));
        if versions.0 < self.required.0 || versions.1 < self.required.1 {
            self.drop_change();
            return;
        }
        let member = self.catching_up.take().expect("just matched").member;
        let mut members = self.members().to_vec();
        members.push(member);
        members.sort_unstable_by_key(|m| m.id);
        self.propose_members(&members);
    }

    fn drop_change(&mut self) {
        if self.catching_up.take().is_some() {
            self.out.change_failed = true;
            self.peers_changed();
        }
    }

    fn propose_members(&mut self, members: &[Member]) {
        let addrs: Vec<wire::MembersMember<'_>> = members
            .iter()
            .map(|m| wire::MembersMember {
                id: m.id.0,
                peer: m.peer.as_bytes(),
                client: m.client.as_bytes(),
                admin: m.admin.as_bytes(),
            })
            .collect();
        let mut payload = Vec::new();
        wire::Members { timestamp: self.now, member: &addrs }.encode_into(&mut payload).expect("addresses fit");
        let index = self.propose(&payload).expect("this node leads");
        self.out.change_proposed = Some(index);
    }

    /// The members, or the member being added, changed: peers follow, and the caller is told.
    fn peers_changed(&mut self) {
        self.rebuild_peers();
        self.out.peers = Some(self.peers());
    }

    /// One peer for each member but this node, and for a member being added; those already there
    /// keep their state.
    fn rebuild_peers(&mut self) {
        let (next, now) = (self.last_index() + 1, self.now);
        let learner = self.catching_up.as_ref().map(|c| c.member.id);
        let mut ids: Vec<NodeId> = self.members().iter().map(|m| m.id).chain(learner).collect();
        ids.retain(|&id| id != self.config.id);
        self.peers.retain(|p| ids.contains(&p.id));
        for id in ids {
            if !self.peers.iter().any(|p| p.id == id) {
                self.peers.push(Peer::new(id, next, now));
            }
        }
    }

    fn start_pre_vote(&mut self, now: u64) {
        self.role = Role::PreCandidate;
        self.leader = None;
        // Only the members' votes count: its own too, if it's one.
        self.votes = if self.is_member(self.config.id) { vec![self.config.id] } else { Vec::new() };
        self.doubted = usize::from(self.doubt.is_some() && !self.votes.is_empty());
        self.reset_election(now);
        let request = wire::RequestVote {
            term: self.meta.term + 1,
            candidate: self.config.id.0,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre_vote: 1,
        };
        for i in 0..self.peers.len() {
            self.send(self.peers[i].id, &request);
        }
        if self.won() {
            self.become_candidate(now);
        }
    }

    fn become_candidate(&mut self, now: u64) {
        self.role = Role::Candidate;
        self.meta = Meta { term: self.meta.term + 1, vote: Some(self.config.id) };
        self.out.persist.push(Persist::Meta(self.meta));
        // Only the members' votes count: its own too, if it's one.
        self.votes = if self.is_member(self.config.id) { vec![self.config.id] } else { Vec::new() };
        self.doubted = usize::from(self.doubt.is_some() && !self.votes.is_empty());
        self.reset_election(now);
        let request = wire::RequestVote {
            term: self.meta.term,
            candidate: self.config.id.0,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre_vote: 0,
        };
        for i in 0..self.peers.len() {
            self.send(self.peers[i].id, &request);
        }
        if self.won() {
            self.become_leader(now);
        }
    }

    fn become_leader(&mut self, now: u64) {
        assert_eq!(self.meta.vote, Some(self.config.id));
        self.role = Role::Leader;
        self.leader = Some(self.config.id);
        self.peers.clear();
        self.rebuild_peers();
        self.heartbeat_at = now;
        self.out.became_leader = true;
    }

    fn become_follower(&mut self, term: u64, now: u64) {
        assert!(term >= self.meta.term);
        if term > self.meta.term {
            self.meta = Meta { term, vote: None };
            self.out.persist.push(Persist::Meta(self.meta));
            self.leader = None;
        }
        if self.role == Role::Leader {
            self.leader = None;
            self.drop_change();
            self.transfer = None;
            self.proposed_at.clear();
        }
        self.role = Role::Follower;
        self.votes.clear();
        self.reset_election(now);
    }

    fn append(&mut self, record: Record<'_>) {
        assert_eq!(record.index, self.last_index() + 1);
        assert!(record.term >= self.last_term());
        let configs = self.log.configs.len();
        let bytes = self.log.push(record);
        match self.out.persist.last_mut() {
            Some(Persist::Append(batch)) => batch.extend_from_slice(bytes),
            _ => self.out.persist.push(Persist::Append(bytes.to_vec())),
        }
        self.trust(record.term);
        if self.log.configs.len() != configs {
            self.peers_changed();
        }
    }

    fn truncate_from(&mut self, index: u64) {
        assert!(index > self.commit, "a committed entry is never replaced");
        let configs = self.log.configs.len();
        self.log.truncate_from(index);
        if self.log.configs.len() != configs {
            self.peers_changed();
        }
        self.durable = self.durable.min(index - 1);
        self.out.persist.push(Persist::TruncateFrom(index));
    }

    fn ack(&mut self, to: NodeId, success: bool, index: u64) {
        let ack = wire::AppendAck {
            term: self.meta.term,
            success: u8::from(success),
            index,
            snapshot_index: self.snapshot_index(),
            protocol: self.advertised.0,
            service_version: self.advertised.1,
        };
        self.send(to, &ack);
    }

    fn send(&mut self, to: NodeId, message: &impl turbofan_sbe::Encode) {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).expect("Raft's messages are within their limits");
        self.out.sends.push((to, frame));
    }

    /// The first index of the run of entries with the same term as `index`'s.
    fn first_index_of_term_at(&self, index: u64) -> u64 {
        if index < self.log.first {
            return index;
        }
        self.log.terms[self.log.run_of(index)].0
    }

    /// The record at `index`, if it's in memory.
    fn record(&self, index: u64) -> Option<&[u8]> {
        if index < self.log.first || index > self.last_index() {
            return None;
        }
        self.log.record(index).or_else(|| {
            self.fetched
                .iter()
                .find_map(|(first, run)| run.get(usize::try_from(index.checked_sub(*first)?).ok()?).map(Vec::as_slice))
        })
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).expect("the last entry is in the log or the snapshot")
    }

    fn quorum(&self) -> usize {
        self.members().len() / 2 + 1
    }

    /// The votes win the election: a majority, and one more for each voter in doubt.
    fn won(&self) -> bool {
        self.votes.len() >= (self.quorum() + self.doubted).min(self.members().len())
    }

    /// The log, as of an entry or snapshot of `term`, has everything this node could have
    /// acknowledged before it restarted in doubt, if that's a later term.
    fn trust(&mut self, term: u64) {
        if self.doubt.is_some_and(|since| term > since) {
            self.end_doubt();
        }
    }

    /// The log has everything this node could have acknowledged before it restarted in doubt.
    fn end_doubt(&mut self) {
        if self.doubt.take().is_some() {
            self.out.persist.push(Persist::Trust);
        }
    }

    fn reset_election(&mut self, now: u64) {
        // xorshift64: deterministic jitter, so a seed replays a run exactly.
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let timeout = nanos(self.config.election_timeout);
        self.election_at = now + timeout + self.rng % timeout;
    }
}

/// The log since the snapshot: every entry's term, and the records of the latest. Older records
/// are on disk only.
#[derive(Debug)]
pub struct Entries {
    /// The first index in the log.
    first: u64,
    /// Where each run of entries of one term starts, and its term; empty for an empty log.
    terms: Vec<(u64, u64)>,
    /// The index of `records[0]`; the records run to the last entry.
    window_first: u64,
    records: VecDeque<Vec<u8>>,
    bytes: usize,
    /// The `Members` entries, by index.
    configs: Vec<(u64, Vec<Member>)>,
    /// The log on disk dropped a damaged tail ([`Entries::doubt`]).
    doubt: bool,
}

impl Default for Entries {
    fn default() -> Self {
        Self::starting_at(1)
    }
}

impl Entries {
    fn starting_at(first: u64) -> Self {
        let (terms, records, configs) = (Vec::new(), VecDeque::new(), Vec::new());
        Self { first, terms, window_first: first, records, bytes: 0, configs, doubt: false }
    }

    /// The log on disk dropped a damaged tail on restart ([`crate::log::Log::in_doubt`]), which
    /// may have held entries this node acknowledged: Raft doubts its own log until it's caught up.
    pub fn doubt(&mut self) {
        self.doubt = true;
    }

    /// Adds the next record of the log as found on disk, keeping at most `window_bytes` of
    /// records in memory.
    ///
    /// # Panics
    ///
    /// If it doesn't continue the log.
    pub fn recover(&mut self, record: Record<'_>, window_bytes: usize) {
        if self.terms.is_empty() {
            *self = Self::starting_at(record.index);
        }
        self.push(record);
        self.evict(record.index, window_bytes);
    }

    /// The log of `records`, whole and continuing each other, as [`Entries::recover`] keeps it.
    ///
    /// # Panics
    ///
    /// If they aren't.
    #[must_use]
    pub fn from_records(records: &[u8], window_bytes: usize) -> Self {
        let mut log = Self::default();
        for record in codec::records(records) {
            log.recover(record.expect("whole, valid records"), window_bytes);
        }
        log
    }

    fn last(&self) -> u64 {
        self.window_first + self.records.len() as u64 - 1
    }

    /// Which of `terms` holds `index`, which is in the log.
    fn run_of(&self, index: u64) -> usize {
        self.terms.partition_point(|&(first, _)| first <= index) - 1
    }

    fn term_at(&self, index: u64) -> Option<u64> {
        (self.first..=self.last()).contains(&index).then(|| self.terms[self.run_of(index)].1)
    }

    fn record(&self, index: u64) -> Option<&[u8]> {
        let i = usize::try_from(index.checked_sub(self.window_first)?).ok()?;
        self.records.get(i).map(Vec::as_slice)
    }

    /// Appends `record`, returning its bytes as stored and shipped.
    fn push(&mut self, record: Record<'_>) -> &[u8] {
        assert_eq!(record.index, self.last() + 1, "records continue the log");
        if self.terms.last().is_none_or(|&(_, term)| term != record.term) {
            self.terms.push((record.index, record.term));
        }
        if let Ok((wire::Decoded::Members(m), _)) = wire::decode(record.payload) {
            let members = m.member().map(|a| Member::read(NodeId(a.id()), a.peer(), a.client(), a.admin())).collect();
            self.configs.push((record.index, members));
        }
        let mut bytes = Vec::with_capacity(codec::RECORD_HEADER + record.payload.len());
        record.encode_into(&mut bytes);
        self.bytes += bytes.len();
        self.records.push_back(bytes);
        self.records.back().expect("just pushed")
    }

    /// Removes the entries from `index` on. After a restart, their records may be on disk only.
    fn truncate_from(&mut self, index: u64) {
        assert!(index >= self.first);
        let keep = usize::try_from(index.saturating_sub(self.window_first)).expect("in the log");
        for record in self.records.drain(keep.min(self.records.len())..) {
            self.bytes -= record.len();
        }
        self.window_first = self.window_first.min(index);
        self.terms.retain(|&(first, _)| first < index);
        self.configs.retain(|&(at, _)| at < index);
    }

    /// Drops the entries up to `index`, which a snapshot covers.
    fn trim_to(&mut self, index: u64) {
        if index < self.first {
            return;
        }
        let drop = usize::try_from((index + 1).saturating_sub(self.window_first)).expect("in the log");
        for record in self.records.drain(..drop.min(self.records.len())) {
            self.bytes -= record.len();
        }
        self.window_first = self.window_first.max(index + 1);
        self.first = index + 1;
        self.configs.retain(|&(at, _)| at > index);
        while self.terms.len() > 1 && self.terms[1].0 <= self.first {
            self.terms.remove(0);
        }
        if self.first > self.last() {
            self.terms.clear();
        } else if let Some(run) = self.terms.first_mut() {
            run.0 = self.first;
        }
    }

    /// Lets the records up to `index` leave memory, oldest first, while there are more than
    /// `max_bytes`.
    fn evict(&mut self, index: u64, max_bytes: usize) {
        while self.bytes > max_bytes && self.window_first <= index {
            let record = self.records.pop_front().expect("the window holds index");
            self.bytes -= record.len();
            self.window_first += 1;
        }
    }
}

impl Member {
    /// From the wire, where an address that isn't UTF-8 is mangled, not refused: it only
    /// fails to connect.
    fn read(id: NodeId, peer: &[u8], client: &[u8], admin: &[u8]) -> Self {
        let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
        Self { id, peer: text(peer), client: text(client), admin: text(admin) }
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    fn member(id: u8) -> Member {
        Member { id: NodeId(id), peer: format!("p{id}"), client: format!("c{id}"), admin: format!("a{id}") }
    }

    fn config(id: u8, members: &[u8]) -> Config {
        Config {
            id: NodeId(id),
            members: members.iter().map(|&m| member(m)).collect(),
            election_timeout: Duration::from_millis(150),
            heartbeat: Duration::from_millis(50),
            seed: u64::from(id),
            log_window_bytes: usize::MAX,
        }
    }

    #[test]
    fn a_lone_member_elects_itself_and_commits() {
        let mut raft = Raft::new(config(1, &[1]), Meta::default(), None, Entries::default(), 0);
        raft.on_tick(raft.next_deadline());
        assert_eq!((raft.role(), raft.term()), (Role::Leader, 1));
        let out = raft.take_outputs();
        assert!(out.became_leader);
        assert_eq!(out.persist, [Persist::Meta(Meta { term: 1, vote: Some(NodeId(1)) })]);
        assert_eq!(raft.propose(b"a"), Some(1));
        assert_eq!(raft.commit_index(), 0, "not durable yet");
        raft.on_persisted(raft.log_end());
        assert_eq!((raft.commit_index(), raft.entry(1)), (1, Some(&b"a"[..])));
    }

    #[test]
    fn a_restarted_node_has_its_log_but_learns_the_commit_again() {
        let mut records = Vec::new();
        Record { term: 2, index: 1, payload: b"x" }.encode_into(&mut records);
        let raft = Raft::new(
            config(1, &[1, 2, 3]),
            Meta { term: 2, vote: None },
            None,
            Entries::from_records(&records, usize::MAX),
            0,
        );
        assert_eq!((raft.last_index(), raft.term_at(1), raft.commit_index()), (1, Some(2), 0));
    }

    /// Records of the given terms, from index 1.
    fn log(terms: &[u64]) -> Vec<u8> {
        let mut out = Vec::new();
        for (i, &term) in terms.iter().enumerate() {
            Record { term, index: i as u64 + 1, payload: b"x" }.encode_into(&mut out);
        }
        out
    }

    /// A log of the given terms, from index 1, all in memory.
    fn entries(terms: &[u64]) -> Entries {
        Entries::from_records(&log(terms), usize::MAX)
    }

    fn decode(frame: &[u8]) -> wire::Decoded<'_> {
        codec::decode_frame(frame).unwrap().unwrap().0
    }

    /// Makes the batch durable and returns its appends and sends, as a driver would.
    fn flush(raft: &mut Raft) -> Vec<(NodeId, Vec<u8>)> {
        let mut out = raft.take_outputs();
        raft.on_persisted(raft.log_end());
        out.appends.append(&mut out.sends);
        out.appends
    }

    /// Delivers `sends` from `from`, and everything they cause, between the nodes in `up`.
    fn deliver(rafts: &mut [Raft], up: &[u8], from: u8, sends: Vec<(NodeId, Vec<u8>)>, now: u64) {
        let mut queue: Vec<_> = sends.into_iter().map(|(to, frame)| (NodeId(from), to, frame)).collect();
        while let Some((from, to, frame)) = queue.pop() {
            if !up.contains(&to.0) {
                continue;
            }
            let raft = &mut rafts[usize::from(to) - 1];
            raft.on_message(from, &decode(&frame), now);
            queue.extend(flush(raft).into_iter().map(|(next, frame)| (to, next, frame)));
        }
    }

    /// Times `id` out from `now` and runs its election among `up`, retrying as a real node
    /// would: a stale leader first stands down, and a pre-vote refused for a stale term teaches
    /// it the term.
    fn elect(rafts: &mut [Raft], up: &[u8], id: u8, now: u64) {
        let mut now = now;
        for _ in 0..3 {
            let raft = &mut rafts[usize::from(id) - 1];
            now = now.max(raft.next_deadline());
            raft.on_tick(now);
            let sends = flush(raft);
            deliver(rafts, up, id, sends, now);
            if rafts[usize::from(id) - 1].role() == Role::Leader {
                return;
            }
        }
        panic!("node {id} wasn't elected");
    }

    fn cluster(n: u8) -> Vec<Raft> {
        let members: Vec<u8> = (1..=n).collect();
        members
            .iter()
            .map(|&id| Raft::new(config(id, &members), Meta::default(), None, Entries::default(), 0))
            .collect()
    }

    /// A leader's appends go out before its own records are durable, and it counts itself
    /// towards a commit only once they are.
    #[test]
    fn a_leader_counts_itself_towards_a_commit_only_once_durable() {
        let mut rafts = cluster(3);
        let now = 1000 * MS;
        elect(&mut rafts, &[1, 2, 3], 1, now);
        let committed = rafts[0].commit_index();
        let index = rafts[0].propose(b"x").unwrap();
        let out = rafts[0].take_outputs();
        let end = rafts[0].log_end();
        assert!(matches!(out.persist[..], [Persist::Append(_)]), "{:?}", out.persist);
        let (_, append) = out.appends.into_iter().find(|&(to, _)| to == NodeId(2)).expect("an append for node 2");
        rafts[1].on_message(NodeId(1), &decode(&append), now);
        for (_, ack) in flush(&mut rafts[1]) {
            rafts[0].on_message(NodeId(2), &decode(&ack), now);
        }
        assert_eq!(rafts[0].commit_index(), committed, "node 2 alone is one of three");
        assert_eq!(end.0, index);
        rafts[0].on_persisted(end);
        assert_eq!(rafts[0].commit_index(), index, "with node 1 durable, two of three");
    }

    /// Raft's figure 8: a leader can't commit an entry of an earlier term by counting replicas,
    /// since a node that lacks it may still win an election and overwrite it.
    #[test]
    fn an_earlier_terms_entry_does_not_commit_by_counting_replicas() {
        let mut rafts = cluster(3);
        let second = 1000 * MS;
        elect(&mut rafts, &[1, 2, 3], 1, second);
        rafts[0].propose(b"one");
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &[1, 2, 3], 1, sends, second);
        assert_eq!(rafts[0].commit_index(), 1);

        // Node 1 appends index 2 in term 1 and reaches no one.
        rafts[0].propose(b"lost");
        flush(&mut rafts[0]);
        // Node 2 is elected in term 2, appends index 2 of its own, and reaches no one.
        elect(&mut rafts, &[2, 3], 2, 2 * second);
        rafts[1].propose(b"term 2");
        flush(&mut rafts[1]);
        // Node 1 is elected in term 3 and replicates its term-1 index 2 to node 3: a majority.
        elect(&mut rafts, &[1, 3], 1, 3 * second);
        assert_eq!(rafts[0].term_at(2), Some(1));
        let now = rafts[0].next_deadline();
        rafts[0].on_tick(now);
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &[1, 3], 1, sends, now);
        assert_eq!(rafts[2].term_at(2), Some(1), "node 3 has it");
        assert_eq!(rafts[0].commit_index(), 1, "but it isn't committed");

        // An entry of term 3 above it commits both.
        rafts[0].propose(b"term 3");
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &[1, 3], 1, sends, now);
        assert_eq!(rafts[0].commit_index(), 3);
    }

    #[test]
    fn a_vote_is_persisted_in_the_batch_that_grants_it() {
        let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 4, vote: None }, None, entries(&[1]), 0);
        let mut frame = Vec::new();
        let request = wire::RequestVote { term: 5, candidate: 2, last_index: 1, last_term: 1, pre_vote: 0 };
        codec::encode_frame(&request, &mut frame).unwrap();
        raft.on_message(NodeId(2), &decode(&frame), 0);
        let out = raft.take_outputs();
        assert_eq!(out.persist.last(), Some(&Persist::Meta(Meta { term: 5, vote: Some(NodeId(2)) })));
        let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
        assert_eq!((vote.term(), vote.granted()), (5, 1));

        // And not again to someone else in the term.
        let request = wire::RequestVote { term: 5, candidate: 3, last_index: 1, last_term: 1, pre_vote: 0 };
        frame.clear();
        codec::encode_frame(&request, &mut frame).unwrap();
        raft.on_message(NodeId(3), &decode(&frame), 0);
        let out = raft.take_outputs();
        let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
        assert_eq!(vote.granted(), 0);
    }

    /// A node that knows no members yet, as one wiped and joining again under its old ID does,
    /// votes for no one: not even a candidate whose log is ahead of its own, which may be a removed
    /// node standing in members the cluster has left.
    #[test]
    fn a_node_that_knows_no_members_doesnt_vote() {
        let mut raft = Raft::new(config(3, &[]), Meta::default(), None, Entries::default(), 0);
        for pre_vote in [1, 0] {
            let mut frame = Vec::new();
            let request = wire::RequestVote { term: 5, candidate: 1, last_index: 9, last_term: 4, pre_vote };
            codec::encode_frame(&request, &mut frame).unwrap();
            raft.on_message(NodeId(1), &decode(&frame), 0);
            let out = raft.take_outputs();
            let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
            assert_eq!(vote.granted(), 0, "pre-vote {pre_vote}");
        }
    }

    /// A node whose log dropped a damaged tail may have lost an entry it acknowledged: an
    /// election counting its vote needs one more than a majority, until it holds an entry of a
    /// later term than it restarted in, when it trusts its log again.
    #[test]
    fn a_node_in_doubt_needs_one_more_vote_until_it_catches_up() {
        let mut rafts = cluster(3);
        let mut log = entries(&[1]);
        log.doubt();
        rafts[0] = Raft::new(config(1, &[1, 2, 3]), Meta { term: 1, vote: None }, None, log, 0);
        assert!(rafts[0].in_doubt());
        // Its own vote and node 2's are a majority, but its own is in doubt.
        let mut now = 1000 * MS;
        for _ in 0..3 {
            now = now.max(rafts[0].next_deadline());
            rafts[0].on_tick(now);
            let sends = flush(&mut rafts[0]);
            deliver(&mut rafts, &[1, 2], 1, sends, now);
            assert_ne!(rafts[0].role(), Role::Leader);
        }
        elect(&mut rafts, &[1, 2, 3], 1, now);
        assert!(rafts[0].in_doubt(), "nothing of the new term yet");
        rafts[0].propose(b"y").unwrap();
        let out = rafts[0].take_outputs();
        assert!(matches!(out.persist[..], [Persist::Append(_), Persist::Trust]), "{:?}", out.persist);
        assert!(!rafts[0].in_doubt());
    }

    /// Under a leader of the term it restarted in, which never sends an entry of a later one, a
    /// node in doubt trusts its log once an append reaches the leader's last index.
    #[test]
    fn a_node_in_doubt_trusts_its_log_once_it_has_the_leaders_whole_log() {
        let mut doubting = entries(&[1]);
        doubting.doubt();
        let mut raft = Raft::new(config(2, &[1, 2, 3]), Meta { term: 2, vote: None }, None, doubting, 0);
        let mut send = |prev_index: u64, prev_term: u64, records: &[u8], last_index: u64| {
            let count = u32::try_from(codec::records(records).count()).unwrap();
            let append = wire::AppendEntries {
                term: 2,
                leader: 1,
                prev_index,
                prev_term,
                commit: 0,
                count,
                entries: records,
                last_index,
            };
            let mut frame = Vec::new();
            codec::encode_frame(&append, &mut frame).unwrap();
            raft.on_message(NodeId(1), &decode(&frame), 0);
            raft.take_outputs();
            raft.in_doubt()
        };
        assert!(send(0, 0, &log(&[1, 2, 2]), 5), "the leader has two more");
        assert!(!send(3, 2, &[], 3), "all of it");
    }

    /// Two voters in doubt take two more votes than a majority: either may have lost an entry.
    #[test]
    fn each_voter_in_doubt_takes_one_more_vote() {
        let mut raft = Raft::new(config(1, &[1, 2, 3, 4, 5]), Meta { term: 1, vote: None }, None, entries(&[1]), 0);
        raft.on_tick(raft.next_deadline());
        raft.take_outputs();
        assert_eq!(raft.role(), Role::PreCandidate);
        for (from, doubt) in [(2, 1), (3, 1), (4, 0)] {
            let mut frame = Vec::new();
            codec::encode_frame(&wire::Vote { term: 2, granted: 1, pre_vote: 1, doubt }, &mut frame).unwrap();
            raft.on_message(NodeId(from), &decode(&frame), 0);
        }
        assert_eq!(raft.role(), Role::PreCandidate, "four of five, two in doubt");
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Vote { term: 2, granted: 1, pre_vote: 1, doubt: 0 }, &mut frame).unwrap();
        raft.on_message(NodeId(5), &decode(&frame), 0);
        assert_eq!(raft.role(), Role::Candidate, "every member");
    }

    /// A vote from a node in doubt says so; an older node's, without the field, doesn't.
    #[test]
    fn a_vote_from_a_node_in_doubt_says_so() {
        let mut log = entries(&[1]);
        log.doubt();
        let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 1, vote: None }, None, log, 0);
        let mut frame = Vec::new();
        let request = wire::RequestVote { term: 2, candidate: 2, last_index: 1, last_term: 1, pre_vote: 0 };
        codec::encode_frame(&request, &mut frame).unwrap();
        raft.on_message(NodeId(2), &decode(&frame), 0);
        let out = raft.take_outputs();
        let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
        assert_eq!((vote.granted(), vote.doubt()), (1, Some(1)));

        // As an older node sends it: version 0, without `doubt`.
        let mut old = out.sends[0].1.clone();
        let len = old.len() - 1;
        old.truncate(len);
        old[..4].copy_from_slice(&u32::try_from(len - 4).unwrap().to_le_bytes());
        let block_len = u16::from_le_bytes([old[4], old[5]]) - 1;
        old[4..6].copy_from_slice(&block_len.to_le_bytes());
        old[10..12].copy_from_slice(&0_u16.to_le_bytes());
        let wire::Decoded::Vote(vote) = decode(&old) else { panic!("not a vote") };
        assert_eq!((vote.granted(), vote.doubt()), (1, None));
    }

    #[test]
    fn a_vote_is_refused_to_a_candidate_with_an_older_log() {
        let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 2, vote: None }, None, entries(&[1, 2]), 0);
        for (last_index, last_term) in [(1, 2), (5, 1)] {
            let mut frame = Vec::new();
            let request = wire::RequestVote { term: 3, candidate: 2, last_index, last_term, pre_vote: 0 };
            codec::encode_frame(&request, &mut frame).unwrap();
            raft.on_message(NodeId(2), &decode(&frame), 0);
            let out = raft.take_outputs();
            let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
            assert_eq!(vote.granted(), 0, "({last_index}, {last_term})");
        }
    }

    /// A follower with a stale tail acks, and commits, only as far as the leader's log is known
    /// to match its own.
    #[test]
    fn a_follower_acks_and_commits_only_what_matches_the_leader() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, entries(&[1, 1, 1]), 0);
        let mut frame = Vec::new();
        let append = wire::AppendEntries {
            term: 2,
            leader: 2,
            prev_index: 1,
            prev_term: 1,
            commit: 3,
            count: 0,
            entries: &[],
            last_index: 3,
        };
        codec::encode_frame(&append, &mut frame).unwrap();
        raft.on_message(NodeId(2), &decode(&frame), 0);
        let out = raft.take_outputs();
        let wire::Decoded::AppendAck(ack) = decode(&out.sends[0].1) else { panic!("not an ack") };
        assert_eq!((ack.success(), ack.index()), (1, 1));
        assert_eq!(raft.commit_index(), 1);
    }

    /// A batch still being written when a new leader replaces its entries: it being durable says
    /// nothing of those that replaced them.
    #[test]
    fn an_earlier_batch_durable_does_not_count_entries_that_replaced_its_own() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, entries(&[1]), 0);
        let mut append = |term: u64, leader: u8, records: &[(u64, u64)]| {
            let mut bytes = Vec::new();
            for &(index, term) in records {
                Record { term, index, payload: b"x" }.encode_into(&mut bytes);
            }
            let count = u32::try_from(records.len()).unwrap();
            let append = wire::AppendEntries {
                term,
                leader,
                prev_index: 1,
                prev_term: 1,
                commit: 1,
                count,
                entries: &bytes,
                last_index: 0,
            };
            let mut frame = Vec::new();
            codec::encode_frame(&append, &mut frame).unwrap();
            raft.on_message(NodeId(leader), &decode(&frame), 0);
            raft.take_outputs();
            raft.log_end()
        };
        let writing = append(1, 1, &[(2, 1), (3, 1)]);
        let replaced = append(2, 2, &[(2, 2)]);
        raft.on_persisted(writing);
        assert_eq!(raft.durable, 1, "2 and 3 of term 1 were written, not 2 of term 2");
        raft.on_persisted(replaced);
        assert_eq!(raft.durable, 2);
    }

    #[test]
    fn a_reordered_older_append_does_not_take_the_commit_back() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, Entries::default(), 0);
        let mut send = |prev_index: u64, commit: u64, entries: &[u8]| {
            let count = u32::try_from(codec::records(entries).count()).unwrap();
            let prev_term = u64::from(prev_index > 0);
            let append = wire::AppendEntries {
                term: 1,
                leader: 1,
                prev_index,
                prev_term,
                commit,
                count,
                entries,
                last_index: 0,
            };
            let mut frame = Vec::new();
            codec::encode_frame(&append, &mut frame).unwrap();
            raft.on_message(NodeId(1), &decode(&frame), 0);
            raft.take_outputs();
            raft.commit_index()
        };
        assert_eq!(send(0, 2, &log(&[1, 1, 1])), 2);
        // Sent after the first, delivered after it, matching only up to 1, with a later commit.
        assert_eq!(send(1, 3, &[]), 2, "the commit index went back");
    }

    fn install_snapshot(raft: &mut Raft, last_index: u64, last_term: u64) -> Outputs {
        let chunk = wire::InstallSnapshot {
            term: 2,
            leader: 1,
            last_index,
            last_term,
            offset: 0,
            done: 1,
            checksum: 0,
            member: &[],
            chunk: b"state",
        };
        let mut frame = Vec::new();
        codec::encode_frame(&chunk, &mut frame).unwrap();
        raft.on_message(NodeId(1), &decode(&frame), 0);
        raft.take_outputs()
    }

    /// A snapshot arriving late, after the follower caught up by appends, mustn't take away the
    /// entries after it: the follower acknowledged them.
    #[test]
    fn a_snapshot_the_log_already_matches_keeps_the_entries_after_it() {
        let mut raft =
            Raft::new(config(3, &[1, 2, 3]), Meta { term: 2, vote: None }, None, entries(&[1, 1, 2, 2, 2]), 0);
        let out = install_snapshot(&mut raft, 3, 2);
        assert!(
            matches!(
                out.persist[..],
                [Persist::SnapshotChunk { .. }, Persist::InstallSnapshot { replace_log: false, .. }]
            ),
            "{:?}",
            out.persist
        );
        assert_eq!((raft.snapshot_index(), raft.last_index(), raft.commit_index()), (3, 5, 3));
        assert!(raft.take_installed());
    }

    #[test]
    fn a_snapshot_the_log_doesnt_match_replaces_it() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 2, vote: None }, None, entries(&[1, 1, 1, 1]), 0);
        let out = install_snapshot(&mut raft, 3, 2);
        assert!(
            matches!(
                out.persist[..],
                [Persist::SnapshotChunk { .. }, Persist::InstallSnapshot { replace_log: true, .. }]
            ),
            "{:?}",
            out.persist
        );
        assert_eq!((raft.snapshot_index(), raft.last_index(), raft.commit_index()), (3, 3, 3));
    }

    /// A leader that keeps no applied records in memory reads them back for a follower that
    /// missed them.
    #[test]
    fn a_follower_behind_the_window_is_sent_records_read_back() {
        let mut rafts = cluster(3);
        rafts[0].config.log_window_bytes = 0;
        let now = 1000 * MS;
        elect(&mut rafts, &[1, 2], 1, now);
        for payload in [b"a", b"b", b"c"] {
            rafts[0].propose(payload);
        }
        let out = rafts[0].take_outputs();
        let [Persist::Append(disk)] = &out.persist[..] else { panic!("{:?}", out.persist) };
        let disk = disk.clone();
        let end = rafts[0].log_end();
        rafts[0].on_persisted(end);
        deliver(&mut rafts, &[1, 2], 1, out.appends, now);
        rafts[0].on_applied(3);
        assert_eq!((rafts[0].commit_index(), rafts[0].entry(1)), (3, None), "committed, and out of memory");

        // Node 3 is back, and refuses a heartbeat past its log. The leader goes back, and asks for
        // the records from disk; they go in the next round.
        let now = rafts[0].next_deadline();
        rafts[0].on_tick(now);
        let out = rafts[0].take_outputs();
        let (_, heartbeat) = out.appends.into_iter().find(|&(to, _)| to == NodeId(3)).expect("a heartbeat for node 3");
        rafts[2].on_message(NodeId(1), &decode(&heartbeat), now);
        for (_, ack) in flush(&mut rafts[2]) {
            rafts[0].on_message(NodeId(3), &decode(&ack), now);
        }
        assert_eq!(rafts[0].take_outputs().reads, [1]);
        rafts[0].on_read(&disk);
        let sends = rafts[0].take_outputs().appends;
        deliver(&mut rafts, &[1, 3], 1, sends, now);
        assert_eq!((rafts[2].last_index(), rafts[2].entry(2)), (3, Some(&b"b"[..])));
        assert_eq!(rafts[0].entry(1), None, "read records are kept for one round");
    }

    /// A leader sends a follower several chunks of a snapshot before hearing back, and sends
    /// again from what the follower has when one is lost: at once if a later one shows it, or
    /// after a heartbeat with no progress.
    #[test]
    fn snapshot_chunks_are_pipelined_and_a_lost_one_is_sent_again() {
        const CHUNK: u64 = MAX_READ_BYTES as u64;
        let len = 3 * CHUNK + 10;
        let bytes: Vec<u8> = (0..len).map(|i| (i % 251) as u8).collect();
        let snapshot = Snapshot { index: 10, term: 1, len, checksum: 0, members: vec![member(1), member(2)] };
        let mut rafts = vec![
            Raft::new(config(1, &[1, 2]), Meta { term: 1, vote: None }, Some(snapshot), Entries::default(), 0),
            Raft::new(config(2, &[1, 2]), Meta::default(), None, Entries::default(), 0),
        ];
        let now = 1000 * MS;
        elect(&mut rafts, &[1, 2], 1, now);
        // Node 2 refuses a heartbeat past its log, and the leader goes back past its snapshot.
        rafts[0].on_tick(now);
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &[1, 2], 1, sends, now);
        let send_chunks = |raft: &mut Raft| {
            let out = raft.take_outputs();
            for &(index, offset) in &out.snapshot_reads {
                let start = usize::try_from(offset).unwrap();
                raft.on_snapshot_read(index, offset, &bytes[start..(start + MAX_READ_BYTES).min(bytes.len())]);
            }
            let reads = out.snapshot_reads.iter().map(|&(_, offset)| offset).collect::<Vec<_>>();
            (
                reads,
                raft.take_outputs()
                    .sends
                    .into_iter()
                    .filter(|&(to, _)| to == NodeId(2))
                    .map(|(_, f)| f)
                    .collect::<Vec<_>>(),
            )
        };
        let (reads, chunks) = send_chunks(&mut rafts[0]);
        assert_eq!(reads, [0, CHUNK, 2 * CHUNK, 3 * CHUNK]);
        assert_eq!(chunks.len(), 4, "the whole snapshot in flight before an ack");

        let deliver_chunks = |rafts: &mut [Raft], chunks: &[&Vec<u8>], now: u64| {
            for chunk in chunks {
                rafts[1].on_message(NodeId(1), &decode(chunk), now);
                for (_, ack) in flush(&mut rafts[1]) {
                    rafts[0].on_message(NodeId(2), &decode(&ack), now);
                }
            }
        };
        // The second chunk is lost. Node 2 takes the first and refuses the third as out of order,
        // acking the first again: the leader sends again from there at once.
        deliver_chunks(&mut rafts, &[&chunks[0], &chunks[2]], now);
        let (reads, chunks) = send_chunks(&mut rafts[0]);
        assert_eq!(reads, [CHUNK, 2 * CHUNK, 3 * CHUNK]);
        // Now the last is lost, with nothing after it to show it. A heartbeat after the acks sees
        // progress; the next sees none, and goes back.
        deliver_chunks(&mut rafts, &[&chunks[0], &chunks[1]], now);
        assert_eq!(rafts[1].snapshot_index(), 0);
        let mut now = now;
        for _ in 0..2 {
            now = rafts[0].next_deadline();
            rafts[0].on_tick(now);
        }
        let (reads, chunks) = send_chunks(&mut rafts[0]);
        assert_eq!(reads, [3 * CHUNK]);
        deliver_chunks(&mut rafts, &chunks.iter().collect::<Vec<_>>(), now);
        assert_eq!(rafts[1].snapshot_index(), 10, "installed");
    }

    fn ids(members: &[Member]) -> Vec<u8> {
        members.iter().map(|m| m.id.0).collect()
    }

    /// Heartbeats from `id`, and all they cause, among `up`.
    fn heartbeat(rafts: &mut [Raft], up: &[u8], id: u8) -> u64 {
        let raft = &mut rafts[usize::from(id) - 1];
        let now = raft.next_deadline();
        raft.on_tick(now);
        let sends = flush(raft);
        deliver(rafts, up, id, sends, now);
        now
    }

    /// A node started with `--join` is sent the log without a vote; once it has caught up, the
    /// members with it are proposed and committed, everywhere.
    #[test]
    fn a_member_is_added_once_it_has_caught_up() {
        let mut rafts = cluster(3);
        rafts.push(Raft::new(config(4, &[]), Meta::default(), None, Entries::default(), 0));
        let all = [1, 2, 3, 4];
        elect(&mut rafts, &all, 1, 1000 * MS);
        assert!(!rafts[0].add_member(member(4)), "not before an entry of its term is committed");
        rafts[0].propose(b"x");
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &all, 1, sends, 1000 * MS);
        assert!(rafts[0].add_member(member(4)));
        assert!(!rafts[0].add_member(member(5)), "one change at a time");
        assert_eq!(ids(rafts[0].members()), [1, 2, 3], "not a member until it has caught up");

        // The first heartbeat learns it's new enough; then it's sent the log, and is added.
        for _ in 0..3 {
            heartbeat(&mut rafts, &all, 1);
        }
        for raft in &rafts {
            assert_eq!(ids(raft.members()), [1, 2, 3, 4], "node {}", raft.id());
        }
        assert_eq!(rafts[3].commit_index(), rafts[0].commit_index());
        assert!(rafts[0].can_change(), "committed");
    }

    /// A node whose code is older than what the cluster runs isn't sent the log, which applying
    /// it would run behaviour it doesn't have, nor added.
    #[test]
    fn a_member_older_than_the_cluster_runs_is_not_added() {
        let mut rafts = cluster(3);
        rafts.push(Raft::new(config(4, &[]), Meta::default(), None, Entries::default(), 0));
        for raft in &mut rafts[..3] {
            raft.advertise(1, 1);
        }
        rafts[0].require(1, 1);
        let all = [1, 2, 3, 4];
        elect(&mut rafts, &all, 1, 1000 * MS);
        rafts[0].propose(b"x");
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &all, 1, sends, 1000 * MS);
        assert_eq!(rafts[0].lowest_versions(), (1, 1), "the members, as they said");
        assert!(rafts[0].add_member(member(4)));
        heartbeat(&mut rafts, &all, 1);
        heartbeat(&mut rafts, &all, 1);
        assert_eq!(rafts[3].last_index(), 0, "not sent the log, which it would apply");
        assert!(!rafts[0].changing_members(), "dropped");
        assert_eq!(ids(rafts[0].members()), [1, 2, 3]);
    }

    /// A leader that removes itself leads until that commits, then stands down; the others elect a
    /// leader between them.
    #[test]
    fn a_leader_that_removes_itself_stands_down_once_committed() {
        let mut rafts = cluster(3);
        let all = [1, 2, 3];
        elect(&mut rafts, &all, 1, 1000 * MS);
        rafts[0].propose(b"x");
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &all, 1, sends, 1000 * MS);
        assert!(rafts[0].remove_member(NodeId(1)));
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &all, 1, sends, 1000 * MS);
        assert_eq!(rafts[0].role(), Role::Follower);
        assert_eq!(ids(rafts[1].members()), [2, 3]);

        // The others elect a leader between them.
        elect(&mut rafts, &[2, 3], 2, 5000 * MS);
    }

    /// A leader hands over to the follower most caught up: it stands for election at once, without
    /// a pre-vote, and wins the next term.
    #[test]
    fn the_leadership_is_handed_to_a_caught_up_follower() {
        let mut rafts = cluster(3);
        let all = [1, 2, 3];
        elect(&mut rafts, &all, 1, 1000 * MS);
        rafts[0].propose(b"x");
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &[1, 2], 1, sends, 1000 * MS);
        assert_eq!(rafts[0].transfer_leadership(Some(NodeId(1))), None, "not to itself");
        assert_eq!(rafts[0].transfer_leadership(Some(NodeId(9))), None, "not to a node that isn't a member");
        assert_eq!(rafts[0].transfer_leadership(None), Some(NodeId(2)), "the one with the log");
        assert_eq!(rafts[0].transfer_leadership(Some(NodeId(3))), None, "one at a time");
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &all, 1, sends, 1000 * MS);
        assert_eq!((rafts[1].role(), rafts[1].term()), (Role::Leader, 2));
        assert_eq!(rafts[0].role(), Role::Follower);
        heartbeat(&mut rafts, &all, 2);
        assert_eq!(rafts[0].leader(), Some(NodeId(2)), "the old leader hears from the new");
    }

    /// A `Members` entry takes effect when it's appended, and is undone if it's truncated.
    #[test]
    fn a_truncated_members_entry_is_undone() {
        let mut members = Vec::new();
        let three: Vec<wire::MembersMember<'_>> =
            [1, 2, 3].map(|id| wire::MembersMember { id, peer: b"p", client: b"c", admin: b"a" }).to_vec();
        wire::Members { timestamp: 0, member: &three }.encode_into(&mut members).unwrap();
        let mut records = log(&[1]);
        Record { term: 1, index: 2, payload: &members }.encode_into(&mut records);
        let mut raft = Raft::new(
            config(3, &[1, 3]),
            Meta { term: 1, vote: None },
            None,
            Entries::from_records(&records, usize::MAX),
            0,
        );
        assert_eq!(ids(raft.members()), [1, 2, 3]);
        assert_eq!(ids(&raft.members_at(1)), [1, 3]);

        // A leader of term 2 has another entry at 2: the members go back.
        let entries = log(&[1, 2]);
        let append = wire::AppendEntries {
            term: 2,
            leader: 1,
            prev_index: 0,
            prev_term: 0,
            commit: 0,
            count: 2,
            entries: &entries,
            last_index: 2,
        };
        let mut frame = Vec::new();
        codec::encode_frame(&append, &mut frame).unwrap();
        raft.on_message(NodeId(1), &decode(&frame), 0);
        assert_eq!(ids(raft.members()), [1, 3]);
        assert_eq!(raft.take_outputs().peers.map(|m| ids(&m)), Some(vec![1, 3]));
    }

    /// Nodes with nearby seeds time out at different times: the same jitter splits every vote.
    #[test]
    fn nearby_seeds_give_different_election_timeouts() {
        let deadlines: Vec<u64> = (1..=4)
            .map(|id| {
                Raft::new(config(id, &[1, 2, 3, 4]), Meta::default(), None, Entries::default(), 0).next_deadline()
            })
            .collect();
        let distinct: std::collections::BTreeSet<_> = deadlines.iter().collect();
        assert_eq!(distinct.len(), deadlines.len(), "{deadlines:?}");
    }
}
