//! Raft, sans-IO, over opaque records: elections with pre-vote, log replication and commit.
//!
//! It knows nothing of sessions or services: [`Raft::propose`] takes a payload and the caller
//! reads committed payloads back with [`Raft::entry`]. Inputs are `on_*` calls with the time;
//! [`Raft::take_outputs`] returns what to persist and what to send.
//!
//! **The driver's one rule:** perform an [`Outputs`]' `persist` operations in order and make them
//! durable, then call [`Raft::on_persisted`], and only then send its `sends`. A vote or an ack
//! then never goes out ahead of the state it promises.

use std::collections::VecDeque;

use crate::codec::{self, MAX_ENTRIES_PER_APPEND, Record};
use crate::meta::Meta;
use crate::wire;

/// A cluster member's ID, from 1.
pub type NodeId = u8;

/// The most members a cluster can have.
pub const MAX_MEMBERS: usize = 7;
/// The most record bytes one `AppendEntries` carries, leaving room for its header in a frame.
const MAX_APPEND_BYTES: usize = 1_000_000;
/// How far a leader replicates to a peer ahead of its acks before it waits for a heartbeat.
const MAX_IN_FLIGHT: u64 = 4096;
/// The largest snapshot a follower accepts from a leader.
pub const MAX_SNAPSHOT_BYTES: usize = 256 << 20;

const _: () = assert!(MAX_APPEND_BYTES < codec::MAX_FRAME_BYTES - 1024);
const _: () = assert!(MAX_IN_FLIGHT >= MAX_ENTRIES_PER_APPEND as u64);

/// Timing and identity.
#[derive(Debug, Clone)]
pub struct Config {
    /// This node.
    pub id: NodeId,
    /// Every member, this node included.
    pub members: Vec<NodeId>,
    /// A follower stands for election after hearing nothing for between this and twice this.
    pub election_timeout_ns: u64,
    /// How often a leader heartbeats; well under the election timeout.
    pub heartbeat_ns: u64,
    /// Seeds the election-timeout jitter; make it differ between nodes.
    pub seed: u64,
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

/// The state machine as of a committed index, which stands in for the log up to it. Raft
/// stores and ships its bytes without reading them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    /// The last index it covers.
    pub index: u64,
    /// That entry's term.
    pub term: u64,
    /// The service's and the node's state, encoded by the node.
    pub bytes: Vec<u8>,
}

/// A change to make durable, in order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Persist {
    /// Save the snapshot, atomically, then drop the log up to its index. With `replace_log`, a
    /// snapshot from the leader, drop the whole log: the next record is `index + 1`.
    Snapshot {
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
}

/// What one batch of inputs produced.
#[derive(Debug, Default)]
pub struct Outputs {
    /// Persist these first, in order.
    pub persist: Vec<Persist>,
    /// Then send these frames.
    pub sends: Vec<(NodeId, Vec<u8>)>,
    /// This node became leader in this batch: the caller should propose an entry of the new
    /// term, so that the entries of earlier terms commit.
    pub became_leader: bool,
}

#[derive(Debug)]
struct Entry {
    term: u64,
    /// The record as stored and shipped.
    record: Vec<u8>,
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
    /// Sending it a snapshot: which (by index), and how much it has.
    sending: Option<(u64, u64)>,
}

/// One member's Raft state.
#[derive(Debug)]
pub struct Raft {
    config: Config,
    meta: Meta,
    role: Role,
    leader: Option<NodeId>,
    /// The index of `entries[0]`.
    first_index: u64,
    /// The term of `first_index - 1`, which a snapshot covers (0 for none).
    base_term: u64,
    // ponytail: the log since the last snapshot is held in memory; read it from the log files
    // if snapshots are too far apart for that.
    entries: VecDeque<Entry>,
    durable: u64,
    commit: u64,
    peers: Vec<Peer>,
    votes: Vec<NodeId>,
    election_at: u64,
    heartbeat_at: u64,
    /// When a leader was last heard from, for refusing pre-votes while it's alive.
    leader_seen_at: Option<u64>,
    rng: u64,
    out: Outputs,
    /// The latest snapshot, for followers that need what it compacted.
    snapshot: Option<Snapshot>,
    /// A snapshot arriving from the leader, chunk by chunk.
    incoming: Option<Snapshot>,
    /// A snapshot from the leader replaced the state since [`Raft::take_installed`].
    installed: bool,
}

impl Raft {
    /// A member restarting from its durable `meta`, latest `snapshot` and the `records` in its
    /// log, all durable. Records the snapshot covers are skipped.
    ///
    /// # Panics
    ///
    /// If the configuration is impossible, or the records are damaged or don't continue from the
    /// snapshot.
    #[must_use]
    pub fn new(config: Config, meta: Meta, snapshot: Option<Snapshot>, records: &[u8], now: u64) -> Self {
        assert!(!config.members.is_empty() && config.members.len() <= MAX_MEMBERS);
        assert!(config.members.contains(&config.id), "a node is one of the members");
        assert!(config.heartbeat_ns * 2 < config.election_timeout_ns);
        let mut entries = VecDeque::new();
        let (base_index, base_term) = snapshot.as_ref().map_or((0, 0), |s| (s.index, s.term));
        let first_index = base_index + 1;
        for record in codec::records(records) {
            let record = record.expect("the log checked its records");
            if record.index <= base_index {
                continue;
            }
            assert_eq!(record.index, first_index + entries.len() as u64, "the log continues from the snapshot");
            let mut bytes = Vec::with_capacity(codec::RECORD_HEADER + record.payload.len());
            record.encode_into(&mut bytes);
            entries.push_back(Entry { term: record.term, record: bytes });
        }
        let peers = config.members.iter().filter(|&&m| m != config.id).map(|&id| Peer::new(id, 1, now)).collect();
        // Mixed first: `seed | 1` alone makes seeds 2 and 3 the same, and two nodes with the same
        // jitter split every vote.
        let rng = config.seed.wrapping_add(0x9e37_79b9_7f4a_7c15).wrapping_mul(0xbf58_476d_1ce4_e5b9) | 1;
        let mut raft = Self {
            config,
            meta,
            role: Role::Follower,
            leader: None,
            first_index,
            base_term,
            entries,
            durable: 0,
            commit: base_index,
            peers,
            votes: Vec::new(),
            election_at: 0,
            heartbeat_at: 0,
            leader_seen_at: None,
            rng,
            out: Outputs::default(),
            snapshot,
            incoming: None,
            installed: false,
        };
        raft.durable = raft.last_index();
        raft.reset_election(now);
        raft
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
        self.first_index + self.entries.len() as u64 - 1
    }

    /// The term of the entry at `index`, if the log holds it (or a snapshot covers it).
    #[must_use]
    pub fn term_at(&self, index: u64) -> Option<u64> {
        if index + 1 == self.first_index {
            return Some(self.base_term);
        }
        let i = usize::try_from(index.checked_sub(self.first_index)?).ok()?;
        self.entries.get(i).map(|e| e.term)
    }

    /// The index the latest snapshot covers to (0 for none). The log holds what follows it.
    #[must_use]
    pub fn snapshot_index(&self) -> u64 {
        self.first_index - 1
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

    /// Replaces the log up to `snapshot.index`, which must be committed, with `snapshot`.
    ///
    /// # Panics
    ///
    /// If the index isn't committed, or isn't past the current snapshot.
    pub fn compact(&mut self, snapshot: Snapshot) {
        assert!(snapshot.index <= self.commit, "only committed state is snapshotted");
        assert!(snapshot.index > self.snapshot_index());
        assert_eq!(self.term_at(snapshot.index), Some(snapshot.term));
        let drop = usize::try_from(snapshot.index - self.snapshot_index()).expect("in the log");
        self.entries.drain(..drop);
        self.first_index = snapshot.index + 1;
        self.base_term = snapshot.term;
        self.out.persist.push(Persist::Snapshot { snapshot: snapshot.clone(), replace_log: false });
        self.snapshot = Some(snapshot);
    }

    /// The payload of the entry at `index`, if the log holds it.
    #[must_use]
    pub fn entry(&self, index: u64) -> Option<&[u8]> {
        let i = usize::try_from(index.checked_sub(self.first_index)?).ok()?;
        let (record, _) = Record::decode(&self.entries.get(i)?.record).ok().flatten().expect("records are checked");
        Some(record.payload)
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
        if self.peers.is_empty() {
            self.advance_commit();
        }
        Some(index)
    }

    /// Time passed.
    pub fn on_tick(&mut self, now: u64) {
        match self.role {
            Role::Leader if now >= self.heartbeat_at => {
                let timeout = self.config.election_timeout_ns;
                let heard = self.peers.iter().filter(|p| p.heard_at + timeout >= now).count() + 1;
                if heard < self.quorum() {
                    // Check quorum: a leader cut off from a majority stands down, so clients
                    // find the leader the majority elects.
                    self.become_follower(self.meta.term, now);
                    return;
                }
                for i in 0..self.peers.len() {
                    self.send_append(i);
                }
                self.heartbeat_at = now + self.config.heartbeat_ns;
            }
            Role::Follower | Role::PreCandidate | Role::Candidate if now >= self.election_at => {
                self.start_pre_vote(now);
            }
            _ => {}
        }
    }

    /// A peer's message. Messages that aren't Raft's are ignored.
    pub fn on_message(&mut self, from: NodeId, message: &wire::Decoded<'_>, now: u64) {
        let Some(peer) = self.peers.iter().position(|p| p.id == from) else { return };
        match message {
            wire::Decoded::RequestVote(m) => self.on_request_vote(from, m, now),
            wire::Decoded::Vote(m) => self.on_vote(from, m, now),
            wire::Decoded::AppendEntries(m) => self.on_append_entries(from, m, now),
            wire::Decoded::AppendAck(m) => self.on_append_ack(peer, m, now),
            wire::Decoded::InstallSnapshot(m) => self.on_install_snapshot(from, m, now),
            wire::Decoded::SnapshotAck(m) => self.on_snapshot_ack(peer, m),
            _ => {}
        }
    }

    /// Everything persisted so far is durable, up to log index `index`.
    pub fn on_persisted(&mut self, index: u64) {
        self.durable = index.min(self.last_index());
        if self.role == Role::Leader {
            self.advance_commit();
        }
    }

    /// What to persist and send. A leader replicates anything new here, once per batch.
    pub fn take_outputs(&mut self) -> Outputs {
        if self.role == Role::Leader {
            for i in 0..self.peers.len() {
                // A peer behind the snapshot moves on by its snapshot acks and heartbeats.
                while self.peers[i].next >= self.first_index
                    && self.peers[i].next <= self.last_index()
                    && self.peers[i].next <= self.peers[i].matched + MAX_IN_FLIGHT
                {
                    self.send_append(i);
                }
            }
        }
        std::mem::take(&mut self.out)
    }

    fn on_request_vote(&mut self, from: NodeId, m: &wire::RequestVoteRef<'_>, now: u64) {
        let up_to_date = (m.last_term(), m.last_index()) >= (self.last_term(), self.last_index());
        let pre_vote = m.pre_vote() == 1;
        let granted = if pre_vote {
            let leader_alive = self.role == Role::Leader
                || self.leader_seen_at.is_some_and(|at| at + self.config.election_timeout_ns > now);
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
        let vote = wire::Vote { term: self.meta.term, granted: u8::from(granted), pre_vote: u8::from(pre_vote) };
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
        if !counts || m.granted() == 0 || self.votes.contains(&from) {
            return;
        }
        self.votes.push(from);
        if self.votes.len() >= self.quorum() {
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
        if prev >= self.first_index - 1 && self.term_at(prev) != Some(m.prev_term()) {
            let hint = self.first_index_of_term_at(prev).max(self.commit + 1);
            self.ack(from, false, hint);
            return;
        }
        let Some(matched) = self.accept_records(m) else { return };
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
            if record.index < self.first_index {
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
        if m.success() == 1 {
            p.sending = None;
            debug_assert!(m.index() <= last, "a follower can't have more than this leader sent");
            p.matched = p.matched.max(m.index().min(last));
            p.next = p.next.max(p.matched + 1);
            self.advance_commit();
        } else {
            p.next = m.index().clamp(p.matched + 1, last + 1);
            self.send_append(peer);
        }
    }

    fn send_append(&mut self, peer: usize) {
        let (next, id) = (self.peers[peer].next, self.peers[peer].id);
        if next < self.first_index {
            self.send_snapshot_chunk(peer);
            return;
        }
        let prev_term = self.term_at(next - 1).expect("next is past the snapshot");
        let mut bytes = Vec::new();
        let mut count = 0_u32;
        let mut index = next;
        while index <= self.last_index() && (count as usize) < MAX_ENTRIES_PER_APPEND {
            let record = &self.entry_ref(index).record;
            if !bytes.is_empty() && bytes.len() + record.len() > MAX_APPEND_BYTES {
                break;
            }
            bytes.extend_from_slice(record);
            count += 1;
            index += 1;
        }
        let append = wire::AppendEntries {
            term: self.meta.term,
            leader: self.config.id,
            prev_index: next - 1,
            prev_term,
            commit: self.commit,
            count,
            entries: &bytes,
        };
        self.send(id, &append);
        self.peers[peer].next = index;
    }

    /// Sends a peer the chunk of the snapshot it's waiting for, one at a time.
    // ponytail: stop and wait, a chunk per round trip; pipeline chunks if snapshots get big.
    fn send_snapshot_chunk(&mut self, peer: usize) {
        let snapshot = self.snapshot.as_ref().expect("a log that starts past 1 has a snapshot");
        let offset = match self.peers[peer].sending {
            Some((index, offset)) if index == snapshot.index => offset,
            _ => 0,
        };
        self.peers[peer].sending = Some((snapshot.index, offset));
        let start = usize::try_from(offset).expect("within the snapshot");
        let end = (start + MAX_APPEND_BYTES).min(snapshot.bytes.len());
        let chunk = wire::InstallSnapshot {
            term: self.meta.term,
            leader: self.config.id,
            last_index: snapshot.index,
            last_term: snapshot.term,
            offset,
            done: u8::from(end == snapshot.bytes.len()),
            chunk: &snapshot.bytes[start..end],
        };
        let mut frame = Vec::new();
        codec::encode_frame(&chunk, &mut frame).expect("a chunk is within a frame");
        self.out.sends.push((self.peers[peer].id, frame));
    }

    fn on_snapshot_ack(&mut self, peer: usize, m: &wire::SnapshotAckRef<'_>) {
        if m.term() != self.meta.term || self.role != Role::Leader {
            return;
        }
        let Some((index, offset)) = self.peers[peer].sending else { return };
        if m.offset() > offset {
            self.peers[peer].sending = Some((index, m.offset()));
            self.send_snapshot_chunk(peer);
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
        if m.offset() == 0 {
            self.incoming = Some(Snapshot { index: m.last_index(), term: m.last_term(), bytes: Vec::new() });
        }
        let Some(incoming) = self.incoming.as_mut().filter(|s| s.index == m.last_index()) else { return };
        if m.offset() == incoming.bytes.len() as u64 && incoming.bytes.len() + m.chunk().len() <= MAX_SNAPSHOT_BYTES {
            incoming.bytes.extend_from_slice(m.chunk());
        }
        if m.done() == 1 && m.offset() + m.chunk().len() as u64 == incoming.bytes.len() as u64 {
            let snapshot = self.incoming.take().expect("just matched");
            self.install(snapshot);
            self.ack(from, true, self.commit);
        } else {
            let ack = wire::SnapshotAck { term: self.meta.term, offset: incoming.bytes.len() as u64 };
            self.send(from, &ack);
        }
    }

    fn install(&mut self, snapshot: Snapshot) {
        assert!(snapshot.index > self.commit);
        // A log that already has the snapshot's last entry keeps what follows it: this node may
        // have acknowledged those entries, and the leader counted them.
        let matches = self.term_at(snapshot.index) == Some(snapshot.term);
        if matches {
            let drop = usize::try_from(snapshot.index - self.snapshot_index()).expect("in the log");
            self.entries.drain(..drop);
        } else {
            self.entries.clear();
            self.durable = snapshot.index;
        }
        self.first_index = snapshot.index + 1;
        self.base_term = snapshot.term;
        self.commit = snapshot.index;
        self.out.persist.push(Persist::Snapshot { snapshot: snapshot.clone(), replace_log: !matches });
        self.snapshot = Some(snapshot);
        self.installed = true;
    }

    fn advance_commit(&mut self) {
        let mut matched: Vec<u64> = self.peers.iter().map(|p| p.matched).collect();
        matched.push(self.durable);
        matched.sort_unstable_by(|a, b| b.cmp(a));
        let candidate = matched[self.quorum() - 1];
        // Only an entry of this term commits by counting; earlier ones commit beneath it.
        if candidate > self.commit && self.term_at(candidate) == Some(self.meta.term) {
            self.commit = candidate;
        }
    }

    fn start_pre_vote(&mut self, now: u64) {
        self.role = Role::PreCandidate;
        self.leader = None;
        self.votes = vec![self.config.id];
        self.reset_election(now);
        let request = wire::RequestVote {
            term: self.meta.term + 1,
            candidate: self.config.id,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre_vote: 1,
        };
        for i in 0..self.peers.len() {
            self.send(self.peers[i].id, &request);
        }
        if self.votes.len() >= self.quorum() {
            self.become_candidate(now);
        }
    }

    fn become_candidate(&mut self, now: u64) {
        self.role = Role::Candidate;
        self.meta = Meta { term: self.meta.term + 1, vote: Some(self.config.id) };
        self.out.persist.push(Persist::Meta(self.meta));
        self.votes = vec![self.config.id];
        self.reset_election(now);
        let request = wire::RequestVote {
            term: self.meta.term,
            candidate: self.config.id,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre_vote: 0,
        };
        for i in 0..self.peers.len() {
            self.send(self.peers[i].id, &request);
        }
        if self.votes.len() >= self.quorum() {
            self.become_leader(now);
        }
    }

    fn become_leader(&mut self, now: u64) {
        assert_eq!(self.meta.vote, Some(self.config.id));
        self.role = Role::Leader;
        self.leader = Some(self.config.id);
        let next = self.last_index() + 1;
        for p in &mut self.peers {
            *p = Peer::new(p.id, next, now);
        }
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
        }
        self.role = Role::Follower;
        self.votes.clear();
        self.reset_election(now);
    }

    fn append(&mut self, record: Record<'_>) {
        assert_eq!(record.index, self.last_index() + 1);
        assert!(record.term >= self.last_term());
        let mut bytes = Vec::with_capacity(codec::RECORD_HEADER + record.payload.len());
        record.encode_into(&mut bytes);
        match self.out.persist.last_mut() {
            Some(Persist::Append(batch)) => batch.extend_from_slice(&bytes),
            _ => self.out.persist.push(Persist::Append(bytes.clone())),
        }
        self.entries.push_back(Entry { term: record.term, record: bytes });
    }

    fn truncate_from(&mut self, index: u64) {
        assert!(index > self.commit, "a committed entry is never replaced");
        assert!(index >= self.first_index);
        let keep = usize::try_from(index - self.first_index).expect("in the log");
        self.entries.truncate(keep);
        self.durable = self.durable.min(index - 1);
        self.out.persist.push(Persist::TruncateFrom(index));
    }

    fn ack(&mut self, to: NodeId, success: bool, index: u64) {
        let ack = wire::AppendAck {
            term: self.meta.term,
            success: u8::from(success),
            index,
            snapshot_index: self.snapshot_index(),
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
        let term = self.term_at(index);
        let mut first = index;
        while first > self.first_index && self.term_at(first - 1) == term {
            first -= 1;
        }
        first
    }

    fn entry_ref(&self, index: u64) -> &Entry {
        &self.entries[usize::try_from(index - self.first_index).expect("in the log")]
    }

    fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).expect("the last entry is in the log or the snapshot")
    }

    fn quorum(&self) -> usize {
        self.config.members.len() / 2 + 1
    }

    fn reset_election(&mut self, now: u64) {
        // xorshift64: deterministic jitter, so a seed replays a run exactly.
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let timeout = self.config.election_timeout_ns;
        self.election_at = now + timeout + self.rng % timeout;
    }
}

impl Peer {
    fn new(id: NodeId, next: u64, now: u64) -> Self {
        Self { id, next, matched: 0, heard_at: now, snapshot_index: 0, sending: None }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;

    fn config(id: NodeId, members: &[NodeId]) -> Config {
        Config {
            id,
            members: members.to_vec(),
            election_timeout_ns: 150 * MS,
            heartbeat_ns: 50 * MS,
            seed: u64::from(id),
        }
    }

    #[test]
    fn a_lone_member_elects_itself_and_commits() {
        let mut raft = Raft::new(config(1, &[1]), Meta::default(), None, &[], 0);
        raft.on_tick(raft.next_deadline());
        assert_eq!((raft.role(), raft.term()), (Role::Leader, 1));
        let out = raft.take_outputs();
        assert!(out.became_leader);
        assert_eq!(out.persist, [Persist::Meta(Meta { term: 1, vote: Some(1) })]);
        assert_eq!(raft.propose(b"a"), Some(1));
        assert_eq!(raft.commit_index(), 0, "not durable yet");
        raft.on_persisted(1);
        assert_eq!((raft.commit_index(), raft.entry(1)), (1, Some(&b"a"[..])));
    }

    #[test]
    fn a_restarted_node_has_its_log_but_learns_the_commit_again() {
        let mut records = Vec::new();
        Record { term: 2, index: 1, payload: b"x" }.encode_into(&mut records);
        let raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 2, vote: None }, None, &records, 0);
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

    fn decode(frame: &[u8]) -> wire::Decoded<'_> {
        codec::decode_frame(frame).unwrap().unwrap().0
    }

    /// Makes the batch durable and returns its sends, as a driver would.
    fn flush(raft: &mut Raft) -> Vec<(NodeId, Vec<u8>)> {
        let out = raft.take_outputs();
        raft.on_persisted(raft.last_index());
        out.sends
    }

    /// Delivers `sends` from `from`, and everything they cause, between the nodes in `up`.
    fn deliver(rafts: &mut [Raft], up: &[NodeId], from: NodeId, sends: Vec<(NodeId, Vec<u8>)>, now: u64) {
        let mut queue: Vec<_> = sends.into_iter().map(|(to, frame)| (from, to, frame)).collect();
        while let Some((from, to, frame)) = queue.pop() {
            if !up.contains(&to) {
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
    fn elect(rafts: &mut [Raft], up: &[NodeId], id: NodeId, now: u64) {
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

    fn cluster(n: NodeId) -> Vec<Raft> {
        let members: Vec<NodeId> = (1..=n).collect();
        members.iter().map(|&id| Raft::new(config(id, &members), Meta::default(), None, &[], 0)).collect()
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
        let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 4, vote: None }, None, &log(&[1]), 0);
        let mut frame = Vec::new();
        let request = wire::RequestVote { term: 5, candidate: 2, last_index: 1, last_term: 1, pre_vote: 0 };
        codec::encode_frame(&request, &mut frame).unwrap();
        raft.on_message(2, &decode(&frame), 0);
        let out = raft.take_outputs();
        assert_eq!(out.persist.last(), Some(&Persist::Meta(Meta { term: 5, vote: Some(2) })));
        let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
        assert_eq!((vote.term(), vote.granted()), (5, 1));

        // And not again to someone else in the term.
        let request = wire::RequestVote { term: 5, candidate: 3, last_index: 1, last_term: 1, pre_vote: 0 };
        frame.clear();
        codec::encode_frame(&request, &mut frame).unwrap();
        raft.on_message(3, &decode(&frame), 0);
        let out = raft.take_outputs();
        let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
        assert_eq!(vote.granted(), 0);
    }

    #[test]
    fn a_vote_is_refused_to_a_candidate_with_an_older_log() {
        let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 2, vote: None }, None, &log(&[1, 2]), 0);
        for (last_index, last_term) in [(1, 2), (5, 1)] {
            let mut frame = Vec::new();
            let request = wire::RequestVote { term: 3, candidate: 2, last_index, last_term, pre_vote: 0 };
            codec::encode_frame(&request, &mut frame).unwrap();
            raft.on_message(2, &decode(&frame), 0);
            let out = raft.take_outputs();
            let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
            assert_eq!(vote.granted(), 0, "({last_index}, {last_term})");
        }
    }

    /// A follower with a stale tail acks, and commits, only as far as the leader's log is known
    /// to match its own.
    #[test]
    fn a_follower_acks_and_commits_only_what_matches_the_leader() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, &log(&[1, 1, 1]), 0);
        let mut frame = Vec::new();
        let append =
            wire::AppendEntries { term: 2, leader: 2, prev_index: 1, prev_term: 1, commit: 3, count: 0, entries: &[] };
        codec::encode_frame(&append, &mut frame).unwrap();
        raft.on_message(2, &decode(&frame), 0);
        let out = raft.take_outputs();
        let wire::Decoded::AppendAck(ack) = decode(&out.sends[0].1) else { panic!("not an ack") };
        assert_eq!((ack.success(), ack.index()), (1, 1));
        assert_eq!(raft.commit_index(), 1);
    }

    #[test]
    fn a_reordered_older_append_does_not_take_the_commit_back() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, &[], 0);
        let mut send = |prev_index: u64, commit: u64, entries: &[u8]| {
            let count = u32::try_from(codec::records(entries).count()).unwrap();
            let prev_term = u64::from(prev_index > 0);
            let append = wire::AppendEntries { term: 1, leader: 1, prev_index, prev_term, commit, count, entries };
            let mut frame = Vec::new();
            codec::encode_frame(&append, &mut frame).unwrap();
            raft.on_message(1, &decode(&frame), 0);
            raft.take_outputs();
            raft.commit_index()
        };
        assert_eq!(send(0, 2, &log(&[1, 1, 1])), 2);
        // Sent after the first, delivered after it, matching only up to 1, with a later commit.
        assert_eq!(send(1, 3, &[]), 2, "the commit index went back");
    }

    fn install_snapshot(raft: &mut Raft, last_index: u64, last_term: u64) -> Outputs {
        let chunk =
            wire::InstallSnapshot { term: 2, leader: 1, last_index, last_term, offset: 0, done: 1, chunk: b"state" };
        let mut frame = Vec::new();
        codec::encode_frame(&chunk, &mut frame).unwrap();
        raft.on_message(1, &decode(&frame), 0);
        raft.take_outputs()
    }

    /// A snapshot arriving late, after the follower caught up by appends, mustn't take away the
    /// entries after it: the follower acknowledged them.
    #[test]
    fn a_snapshot_the_log_already_matches_keeps_the_entries_after_it() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 2, vote: None }, None, &log(&[1, 1, 2, 2, 2]), 0);
        let out = install_snapshot(&mut raft, 3, 2);
        assert!(matches!(out.persist[..], [Persist::Snapshot { replace_log: false, .. }]), "{:?}", out.persist);
        assert_eq!((raft.snapshot_index(), raft.last_index(), raft.commit_index()), (3, 5, 3));
        assert!(raft.take_installed());
    }

    #[test]
    fn a_snapshot_the_log_doesnt_match_replaces_it() {
        let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 2, vote: None }, None, &log(&[1, 1, 1, 1]), 0);
        let out = install_snapshot(&mut raft, 3, 2);
        assert!(matches!(out.persist[..], [Persist::Snapshot { replace_log: true, .. }]), "{:?}", out.persist);
        assert_eq!((raft.snapshot_index(), raft.last_index(), raft.commit_index()), (3, 3, 3));
    }

    /// Nodes with nearby seeds time out at different times: the same jitter splits every vote.
    #[test]
    fn nearby_seeds_give_different_election_timeouts() {
        let deadlines: Vec<u64> = (1..=4)
            .map(|id| Raft::new(config(id, &[1, 2, 3, 4]), Meta::default(), None, &[], 0).next_deadline())
            .collect();
        let distinct: std::collections::BTreeSet<_> = deadlines.iter().collect();
        assert_eq!(distinct.len(), deadlines.len(), "{deadlines:?}");
    }
}
