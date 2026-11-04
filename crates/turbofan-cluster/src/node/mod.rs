//! A cluster member: [`Raft`] plus what the log means. Client sessions, de-duplication, each
//! session's egress ring and the service's timers, applied to a [`Service`].
//!
//! Like [`Raft`], it does no IO and reads no clock. The driver passes the time with every input
//! and carries out [`Node::take_outputs`] with Raft's rule: persist, read what it asks to and
//! give it to [`Node::on_read`], then [`Node::on_persisted`], then send to peers. It needn't wait
//! for the disk: inputs may go on arriving, and outputs be taken, while a batch persists (see
//! [`crate::raft`]). Clients are sent to at once: nothing they're told rests on this node's
//! disk, only on what's committed, which a quorum has durable.
//!
//! A snapshot is taken off the apply path: [`Service::snapshot`] gives a frozen copy of the state
//! that the driver writes out on another thread ([`Outputs::save`]) while the node goes on
//! applying, then tells the node with [`Node::on_snapshot_saved`]. Its log is compacted then. A
//! node restores from a snapshot with [`Node::restore`], when it starts and when its leader sends
//! one ([`Outputs::restore`]).
//!
//! **Replicated state** (`State`) changes only when a committed entry is applied, so it is the same on
//! every node: sessions, their last applied `client_seq`, their egress rings, timers, and the
//! service's outbox ([`Ctx::propose`]), which whichever node leads logs. **Leader
//! state** (`Local`) is this node's view as leader: connections, session liveness, and the opens,
//! closes and timer firings it has proposed but not yet seen applied. It is rebuilt when a node
//! becomes leader.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::io::{self, Read};
use std::time::Duration;

use crate::client::MAX_UNACKED;
use crate::codec::{self, MAX_PAYLOAD_BYTES};
use crate::meta::Meta;
use crate::metrics::Metrics;
use crate::raft::{self, Entries, LogEnd, Member, NodeId, Persist, Raft, Role, Snapshot, SnapshotRead};
use crate::wire::{self, CloseReason};

mod admin;
mod apply;
mod service;
mod sessions;
mod state;

pub use service::{Ctx, Effect, PayloadTooLong, Service, SessionId};
pub(crate) use state::with_epoch;
use state::{Session, State, decode_state, encode_state};

/// A client connection's ID, which the driver assigns.
pub type ConnId = u64;

/// The most responses a session can keep for a client that reattaches after a failover: the
/// ceiling on [`Config::egress_ring_entries`].
pub const MAX_EGRESS_RING: usize = 65_536;
/// The most sessions open at once.
pub const MAX_SESSIONS: usize = 10_000;
/// The highest protocol version this code supports: see `WIRE.md`, and [`Config::protocol`].
pub const PROTOCOL: u32 = 0;
/// The longest principal [`Service::authenticate`] can give.
pub const MAX_PRINCIPAL_BYTES: usize = 255;

/// Writes a snapshot of a service's state, frozen when it was made, to the writer it's given.
pub type SnapshotWriter = Box<dyn FnOnce(&mut dyn io::Write) -> io::Result<()> + Send>;

/// A snapshot for the driver to write, off the apply path.
pub struct SaveSnapshot {
    /// The index it's of.
    pub index: u64,
    /// That entry's term.
    pub term: u64,
    /// The members as of `index`, which go with it.
    pub members: Vec<Member>,
    /// Writes it: the node's state, then the service's.
    pub writer: SnapshotWriter,
}

impl fmt::Debug for SaveSnapshot {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SaveSnapshot").field("index", &self.index).field("term", &self.term).finish_non_exhaustive()
    }
}

/// A node's configuration.
///
/// Non-exhaustive, so that a new setting isn't a breaking change: build it with `new`, then set
/// what differs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// Identity and Raft timing.
    pub raft: raft::Config,
    /// A session the leader hears nothing from for this long is closed.
    pub session_timeout: Duration,
    /// Each node snapshots, and compacts its log, once this many entries follow its last snapshot.
    pub snapshot_every_entries: u64,
    /// How many of its snapshots each node keeps on disk, the newest: past 1, older ones for an
    /// operator to copy off the node, or restore from ([`crate::driver::restore`]). Each holds the
    /// whole state. The driver deletes the rest; the node itself only ever reads the newest.
    pub snapshots_kept: usize,
    /// How many responses each session keeps for a client that reattaches after a failover. A
    /// client away while more than this are sent misses some ([`crate::client::Event::Lost`]),
    /// so it must cover a gateway's requests in flight times the responses each can cause.
    /// Every node holds this many per session, and snapshots them.
    pub egress_ring_entries: usize,
    /// The highest protocol version this node runs: [`PROTOCOL`], or lower to run as an older
    /// node would.
    pub protocol: u32,
}

impl Config {
    /// The defaults for node `id` of a cluster that starts with `members`, this node among them,
    /// or of none to join a running one: a 300 ms election timeout, a 50 ms heartbeat, 64 MiB of
    /// applied records kept in memory, clients held back past 64 MiB of proposals uncommitted, a
    /// 10 s session timeout, a snapshot every 100,000 entries with 100,000 kept before it, one
    /// snapshot kept on disk, and 8192 responses kept per session. Change any field after.
    #[must_use]
    pub fn new(id: NodeId, members: Vec<Member>) -> Self {
        Self {
            raft: raft::Config {
                id,
                members,
                election_timeout: Duration::from_millis(300),
                heartbeat: Duration::from_millis(50),
                seed: u64::from(id),
                log_window_bytes: 64 << 20,
                log_tail_entries: 100_000,
                max_uncommitted_bytes: 64 << 20,
            },
            session_timeout: Duration::from_secs(10),
            snapshot_every_entries: 100_000,
            snapshots_kept: 1,
            egress_ring_entries: 8192,
            protocol: PROTOCOL,
        }
    }
}

/// What one batch of inputs produced. Send `peer_appends` and `client_sends` at once, note
/// [`Raft::log_end`], and start on `persist`, after any earlier batch's; once it's written, carry
/// out `reads` and give each to [`Node::on_read`]; once it's durable, a restore, then
/// [`Node::on_persisted`] with the end noted, then `peer_sends`, after any earlier batch's. Then
/// take outputs again. Inputs may arrive meanwhile, except while a restore waits.
/// [`crate::pipeline::Pipeline`] does all this, but the IO.
#[derive(Debug, Default)]
pub struct Outputs {
    /// Persist these, in order.
    pub persist: Vec<Persist>,
    /// Read the log's records from these indices on, after `persist`: see
    /// [`raft::Outputs::reads`].
    pub reads: Vec<u64>,
    /// A leader's `AppendEntries`, for peers: send these at once, without waiting for `persist`
    /// (see [`crate::raft`]).
    pub peer_appends: Vec<(NodeId, Vec<u8>)>,
    /// Frames for peers, once `persist`, and every earlier batch's, is durable.
    pub peer_sends: Vec<(NodeId, Vec<u8>)>,
    /// Frames for clients, at once.
    pub client_sends: Vec<(ConnId, Vec<u8>)>,
    /// Read the latest snapshot from these offsets, after `persist`: see
    /// [`raft::Outputs::snapshot_reads`]. Give each to [`Node::on_snapshot_read`].
    pub snapshot_reads: Vec<SnapshotRead>,
    /// Write this snapshot, apart from the batches, and tell [`Node::on_snapshot_saved`] once
    /// it's durable. There's one at a time.
    pub save: Option<SaveSnapshot>,
    /// After `persist`, [`Node::restore`] from the latest snapshot, which is of this index. Until
    /// then, give the node no input: a snapshot installed after this one would leave this one's
    /// file gone.
    pub restore: Option<u64>,
    /// The peers to connect to changed, to these: the members, and a member being added.
    pub peers: Option<Vec<Member>>,
    /// As leader, how long each of its entries took from proposed to each stage it reached, in
    /// nanoseconds: see [`crate::metrics`].
    pub latencies: Vec<(raft::Stage, u64)>,
}

/// This node's, as leader.
#[derive(Debug, Default)]
struct Local {
    /// The leader has applied an entry of its own term, so its state is current.
    ready: bool,
    was_leader: bool,
    /// Client connections, and the session each is attached to.
    conns: BTreeMap<ConnId, Option<SessionId>>,
    attached: BTreeMap<SessionId, ConnId>,
    /// `SessionOpen` entries proposed, by index, for the connection that asked.
    opening: BTreeMap<SessionId, ConnId>,
    /// When the leader last heard from each session.
    activity: BTreeMap<SessionId, u64>,
    closing: BTreeSet<SessionId>,
    /// The highest `client_seq` proposed for each session this term.
    proposed: BTreeMap<SessionId, u64>,
    /// The highest outbox ID logged this term.
    logged: u64,
    firing: BTreeSet<u64>,
    /// Manual snapshots asked for: request ID to the admin connection, before and after the
    /// `Snapshot` entry is applied (then with its index).
    requested: BTreeMap<u64, ConnId>,
    awaiting: BTreeMap<u64, (ConnId, u64)>,
    /// A change of members asked for: the admin connection, the request ID, and its `Members`
    /// entry's index once proposed.
    changing: Option<(ConnId, u64, Option<u64>)>,
    /// A shutdown asked for: the admin connection, the request ID, and its `Snapshot` entry's
    /// index once applied.
    shutting: Option<(ConnId, u64, Option<u64>)>,
    /// An activation asked for: the admin connection, the request ID, and its entry's index.
    activating: Option<(ConnId, u64, u64)>,
    /// Sessions with messages applied since the last `Egress` to them, which carries their
    /// `applied_seq`: told it in a `KeepAliveAck` once this batch of entries is applied, so a
    /// client waiting on its messages being applied (to release what it forwarded) needn't wait
    /// for its next keepalive.
    unacked: BTreeSet<SessionId>,
    sends: Vec<(ConnId, Vec<u8>)>,
}

/// A cluster member running service `S`.
#[derive(Debug)]
pub struct Node<S> {
    raft: Raft,
    service: S,
    config: Config,
    state: State,
    local: Local,
    ctx: Ctx,
    now: u64,
    scratch: Vec<u8>,
    /// The snapshot being written, by index, and one is wanted once it's done.
    saving: Option<u64>,
    want_snapshot: bool,
    save: Option<SaveSnapshot>,
    /// Waiting for [`Node::restore`] from the snapshot of this index: nothing is applied till then.
    restoring: Option<u64>,
    /// A leadership transfer asked for, on this admin connection with this request ID: answered
    /// once another node leads, which is after this one stops leading.
    transferring: Option<(ConnId, u64)>,
    /// A promotion asked for, on this admin connection with this request ID, to this epoch:
    /// answered once it applies.
    promoting: Option<(ConnId, u64, u64)>,
    /// When the latest snapshot was taken or installed, or the node started.
    snapshot_at: u64,
}

impl<S: Service> Node<S> {
    /// A member starting from its durable `meta`, latest `snapshot` and `log`, with
    /// `service` new. With a snapshot, [`Node::restore`] from it next.
    #[must_use]
    pub fn new(config: Config, service: S, meta: Meta, snapshot: Option<Snapshot>, log: Entries, now: u64) -> Self {
        assert!(config.session_timeout > config.raft.election_timeout * 2, "sessions outlive an election");
        assert!(config.session_timeout < raft::MAX_TIMEOUT, "times in nanoseconds can't overflow");
        assert!(config.snapshot_every_entries > 0);
        assert!((1..=MAX_EGRESS_RING).contains(&config.egress_ring_entries));
        let restoring = snapshot.as_ref().map(|s| s.index);
        let mut raft = Raft::new(config.raft.clone(), meta, snapshot, log, now);
        raft.advertise(config.protocol, service.version());
        Self {
            raft,
            service,
            config,
            state: State::default(),
            local: Local::default(),
            ctx: Ctx::default(),
            now,
            scratch: Vec::new(),
            saving: None,
            want_snapshot: false,
            save: None,
            restoring,
            transferring: None,
            promoting: None,
            snapshot_at: now,
        }
    }

    /// Its state and counts, at `now`.
    #[must_use]
    pub fn metrics(&self, now: u64) -> Metrics {
        let raft = &self.raft;
        Metrics {
            role: raft.role(),
            term: raft.term(),
            leader: raft.leader(),
            members: raft.members().len(),
            commit: raft.commit_index(),
            applied: self.state.applied,
            last_index: raft.last_index(),
            snapshot_index: raft.snapshot_index(),
            snapshot_age_ns: now.saturating_sub(self.snapshot_at),
            sessions: self.state.sessions.len(),
            in_doubt: raft.in_doubt(),
            follower_lag: raft.follower_lag(),
        }
    }

    /// Replaces the replicated state, and the service's, with the snapshot [`Node::new`] or
    /// [`Outputs::restore`] named, read from `from`.
    ///
    /// # Errors
    ///
    /// If reading fails or the bytes aren't that snapshot. The node's state is then unknown: drop
    /// it.
    ///
    /// # Panics
    ///
    /// If no restore is waiting.
    pub fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let index = self.restoring.expect("a restore is waiting");
        let state = decode_state(from)?;
        if state.applied != index {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "a snapshot of another index"));
        }
        self.service.restore(from)?;
        self.raft.require(state.protocol, state.service_version);
        self.state = state;
        self.restoring = None;
        self.apply();
        Ok(())
    }

    /// The snapshot from [`Outputs::save`] is durable.
    pub fn on_snapshot_saved(&mut self, snapshot: Snapshot) {
        self.saving = None;
        self.snapshot_at = self.now;
        self.raft.on_snapshot_saved(snapshot);
        if self.want_snapshot {
            self.take_snapshot();
        }
        self.answer_snapshot_requests();
    }

    /// The latest snapshot's bytes from `offset`, read for [`Outputs::snapshot_reads`].
    pub fn on_snapshot_read(&mut self, index: u64, offset: u64, bytes: &[u8]) {
        self.raft.on_snapshot_read(index, offset, bytes);
    }

    /// It has stopped, shut down with the cluster: drop it.
    #[must_use]
    pub fn stopped(&self) -> bool {
        self.raft.stopped()
    }

    /// Its Raft state.
    #[must_use]
    pub fn raft(&self) -> &Raft {
        &self.raft
    }

    /// The service, as applied so far.
    #[must_use]
    pub fn service(&self) -> &S {
        &self.service
    }

    /// The index applied to the service.
    #[must_use]
    pub fn applied(&self) -> u64 {
        self.state.applied
    }

    /// The cluster's epoch, as applied: 0 until its first leader's is. Peers that know different
    /// ones are of different clusters (one promoted from the other), and don't talk.
    #[must_use]
    pub fn epoch(&self) -> u64 {
        self.state.epoch
    }

    /// A peer's message.
    pub fn on_peer(&mut self, from: NodeId, message: &wire::Decoded<'_>, now: u64) {
        self.raft.on_message(from, message, now);
        self.after(now);
    }

    /// Time passed.
    pub fn on_tick(&mut self, now: u64) {
        self.raft.on_tick(now);
        self.after(now);
        if self.is_ready() {
            self.expire_sessions(now);
            self.fire_timers(now);
        }
    }

    /// The batches up to one taken when the log ended at `end` are durable: see
    /// [`Raft::on_persisted`].
    pub fn on_persisted(&mut self, end: LogEnd, now: u64) {
        self.now = self.now.max(now);
        self.raft.on_persisted(end, now);
        self.apply();
        self.answer_snapshot_requests();
    }

    /// The records read for [`Outputs::reads`].
    pub fn on_read(&mut self, records: &[u8]) {
        self.raft.on_read(records);
        self.apply();
    }

    /// A client connection closed. Its session stays open for the client to reattach to.
    pub fn on_disconnect(&mut self, conn: ConnId) {
        if let Some(Some(session)) = self.local.conns.remove(&conn) {
            self.local.attached.remove(&session);
        }
    }

    /// A client's message on connection `conn`.
    pub fn on_client(&mut self, conn: ConnId, message: &wire::Decoded<'_>, now: u64) {
        self.after(now);
        self.local.conns.entry(conn).or_insert(None);
        if !self.is_ready() {
            self.redirect(conn);
            return;
        }
        match message {
            wire::Decoded::Connect(m) => self.open(conn, m.credentials()),
            wire::Decoded::Reattach(m) => {
                self.reattach(conn, SessionId(m.session()), m.last_egress_seq(), m.credentials())
            }
            wire::Decoded::Message(m) => {
                if let Some(session) = self.check_attached(conn, SessionId(m.session())) {
                    self.submit(conn, session, m.client_seq(), m.payload());
                }
            }
            wire::Decoded::KeepAlive(m) => {
                if let Some(session) = self.check_attached(conn, SessionId(m.session())) {
                    self.local.activity.insert(session, now);
                    let applied_seq = self.state.sessions[&session].last_seq;
                    self.send_client(conn, &wire::KeepAliveAck { applied_seq });
                }
            }
            wire::Decoded::Close(m) => {
                if let Some(session) = self.check_attached(conn, SessionId(m.session())) {
                    self.propose_close(session, CloseReason::Client);
                }
            }
            _ => {}
        }
    }

    /// What to persist and send.
    pub fn take_outputs(&mut self) -> Outputs {
        let mut out = self.raft.take_outputs();
        if out.became_leader {
            self.become_leader();
            let more = self.raft.take_outputs();
            out.persist.extend(more.persist);
            out.reads.extend(more.reads);
            out.snapshot_reads.extend(more.snapshot_reads);
            out.appends.extend(more.appends);
            out.sends.extend(more.sends);
            out.peers = more.peers.or(out.peers);
            out.latencies.extend(more.latencies);
        }
        if let (Some(index), Some((_, _, at))) = (out.change_proposed, self.local.changing.as_mut()) {
            *at = Some(index);
        }
        if out.change_failed
            && let Some((conn, request_id, _)) = self.local.changing.take()
        {
            self.send_client(conn, &wire::MembersRefused { request_id });
        }
        Outputs {
            persist: out.persist,
            reads: out.reads,
            peer_appends: out.appends,
            peer_sends: out.sends,
            client_sends: std::mem::take(&mut self.local.sends),
            snapshot_reads: out.snapshot_reads,
            save: self.save.take(),
            restore: self.restoring,
            peers: out.peers,
            latencies: out.latencies,
        }
    }

    /// When [`Node::on_tick`] next has something to do.
    #[must_use]
    pub fn next_deadline(&self) -> u64 {
        let mut deadline = self.raft.next_deadline();
        if self.is_ready() {
            let timer = self.state.deadlines.iter().find(|(_, id)| !self.local.firing.contains(id));
            if let Some(&(at, _)) = timer {
                deadline = deadline.min(at);
            }
            let quiet = self.local.activity.iter().filter(|(s, _)| !self.local.closing.contains(s)).map(|(_, &at)| at);
            if let Some(at) = quiet.min() {
                deadline = deadline.min(at + raft::nanos(self.config.session_timeout));
            }
        }
        deadline
    }

    fn is_ready(&self) -> bool {
        self.local.ready && self.raft.role() == Role::Leader
    }

    /// After any input: apply what's committed, and notice losing the leadership.
    fn after(&mut self, now: u64) {
        self.now = now;
        self.raft.set_time(now);
        if self.raft.take_installed() {
            // A snapshot being saved is older: it's ignored once saved.
            self.restoring = Some(self.raft.snapshot_index());
            self.snapshot_at = now;
        }
        self.apply();
        if self.local.was_leader && self.raft.role() != Role::Leader {
            self.step_down();
        }
        // Handed over once another node is heard leading; not, if this one leads with no
        // transfer under way (it gave up, or was elected again).
        if let Some((conn, request_id)) = self.transferring {
            let other = self.raft.leader().filter(|&l| l != self.raft.id());
            let gave_up = self.raft.role() == Role::Leader && !self.raft.transferring();
            if other.is_some() || gave_up {
                self.transferring = None;
                self.send_client(conn, &wire::LeadershipTransferred { request_id, leader: NodeId::to_wire(other) });
            }
        }
        self.answer_snapshot_requests();
    }

    /// Snapshots the state as applied so far, for the driver to write; or, while one is being
    /// written, once it's done.
    fn take_snapshot(&mut self) {
        if self.saving.is_some() {
            self.want_snapshot = true;
            return;
        }
        self.want_snapshot = false;
        let index = self.state.applied;
        if index <= self.raft.snapshot_index() || self.restoring.is_some() {
            return;
        }
        let term = self.raft.term_at(index).expect("the applied entry is in the log");
        let head = encode_state(&self.state);
        let service = self.service.snapshot();
        let writer: SnapshotWriter = Box::new(move |out| {
            out.write_all(&head)?;
            service(out)
        });
        self.saving = Some(index);
        let members = self.raft.members_at(index);
        self.save = Some(SaveSnapshot { index, term, members, writer });
    }

    fn become_leader(&mut self) {
        self.local = Local { was_leader: true, sends: std::mem::take(&mut self.local.sends), ..Local::default() };
        // Every session gets a full timeout from now: the old leader's view of them is gone.
        self.local.activity = self.state.sessions.keys().map(|&s| (s, self.now)).collect();
        self.propose(&wire::NewTerm { timestamp: self.now });
        // A new cluster's first leader names it. Another leader may too, before this applies: the
        // first applied counts.
        if self.state.epoch == 0 {
            let epoch = mint_epoch(self.now, self.raft.id());
            self.propose(&wire::Epoch { timestamp: self.now, epoch, promoted: 0 });
        }
    }

    /// Clients attached here, and operators waiting on an answer from this leader, go and find
    /// the new one.
    fn step_down(&mut self) {
        let conns: Vec<ConnId> = self.local.conns.keys().copied().collect();
        let mut admins: Vec<ConnId> = self.local.requested.values().copied().collect();
        admins.extend(self.local.awaiting.values().map(|&(conn, _)| conn));
        admins.extend(self.local.changing.map(|(conn, _, _)| conn));
        admins.extend(self.local.shutting.map(|(conn, _, _)| conn));
        admins.extend(self.local.activating.map(|(conn, _, _)| conn));
        let sends = std::mem::take(&mut self.local.sends);
        self.local = Local { sends, ..Local::default() };
        for conn in conns {
            self.redirect(conn);
        }
        for conn in admins {
            self.redirect_to(conn, |m| &m.admin);
        }
    }

    fn propose(&mut self, entry: &impl turbofan_sbe::Encode) -> Option<u64> {
        self.scratch.clear();
        entry.encode_into(&mut self.scratch).expect("entries are within their limits");
        self.raft.propose(&self.scratch)
    }
}

/// An epoch no other cluster has: from the time, and the node naming it, so two learners
/// promoted at once don't name the same. Never 0, which is none.
pub(crate) fn mint_epoch(now: u64, id: NodeId) -> u64 {
    (now ^ (u64::from(id.0) << 56)).max(1)
}

#[cfg(test)]
mod tests;
