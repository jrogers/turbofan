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
use crate::raft::{self, Entries, Member, NodeId, Persist, Raft, Role, Snapshot};
use crate::wire::{self, CloseReason};

/// A client session's ID: the log index of the entry that opened it.
pub type SessionId = u64;
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

/// The application: a deterministic state machine that every node runs over the same log.
///
/// It must depend only on what it's given, never on a clock, randomness, IO or iteration order
/// of a hash map. Time comes from [`Ctx::now`], which is the leader's clock stamped into the
/// log.
pub trait Service {
    /// Whether a client with `credentials` may open a session, or reattach to one, and who it
    /// is: its principal (a user name, say, at most [`MAX_PRINCIPAL_BYTES`]), or `None` to refuse
    /// it. Only the leader asks, before anything is logged, so the answer may depend on anything
    /// (the state, a key file) without breaking determinism; it's the principal that's logged,
    /// never the credentials. Accepts everyone by default.
    fn authenticate(&self, _credentials: &[u8]) -> Option<Vec<u8>> {
        Some(Vec::new())
    }
    /// A session opened, for `principal`.
    fn on_session_open(&mut self, _ctx: &mut Ctx, _session: SessionId, _principal: &[u8]) {}
    /// A session closed. Sends to it are dropped from here on.
    fn on_session_close(&mut self, _ctx: &mut Ctx, _session: SessionId, _reason: CloseReason) {}
    /// A message from a session, each exactly once and in the order the client sent them.
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]);
    /// A timer from [`Ctx::schedule_timer`] fired.
    fn on_timer(&mut self, _ctx: &mut Ctx, _id: u64) {}
    /// A payload from [`Ctx::propose`], back through the log: each once, in the order proposed.
    fn on_proposal(&mut self, _ctx: &mut Ctx, _payload: &[u8]) {}
    /// The highest version of its behaviour this code has. An operator's `activate` switches the
    /// cluster to a version once every member has it ([`Ctx::service_version`]): so a rolling
    /// upgrade ships code with the old behaviour and the new, then activates the new.
    fn version(&self) -> u32 {
        0
    }
    /// The cluster switched to `version` of the service's behaviour, here in the log: on every
    /// node at the same point, for whatever has to change with it.
    fn on_activate(&mut self, _ctx: &mut Ctx, _version: u32) {}
    /// A frozen copy of its whole state, which writes it out later, on another thread, while the
    /// node goes on applying: a clone, say, or persistent structures shared with the live state.
    /// The same state must write the same bytes, on every node.
    fn snapshot(&self) -> SnapshotWriter;
    /// Replaces its whole state with what a [`Service::snapshot`] wrote, read from `from` to its
    /// end.
    ///
    /// # Errors
    ///
    /// If reading fails, or what's read isn't a snapshot.
    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()>;
}

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

/// What a [`Service`] can do from a callback. Its effects take place after the callback
/// returns, in order.
#[derive(Debug, Default)]
pub struct Ctx {
    now: u64,
    service_version: u32,
    effects: Vec<Effect>,
}

/// Something a [`Service`] callback did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// [`Ctx::send`]: a payload for a session.
    Send(SessionId, Vec<u8>),
    /// [`Ctx::schedule_timer`]: a timer ID and its deadline.
    Schedule(u64, u64),
    /// [`Ctx::cancel_timer`].
    Cancel(u64),
    /// [`Ctx::close_session`].
    Close(SessionId),
    /// [`Ctx::propose`].
    Propose(Vec<u8>),
}

impl Ctx {
    /// A context at cluster time `now`, for testing a service without a cluster: call its
    /// callbacks with it, then look at [`Ctx::take_effects`].
    #[must_use]
    pub fn new(now: u64) -> Self {
        Self { now, service_version: 0, effects: Vec::new() }
    }

    /// The same, as if the cluster ran `version` of the service's behaviour.
    #[must_use]
    pub fn with_service_version(mut self, version: u32) -> Self {
        self.service_version = version;
        self
    }

    /// What the callbacks did since the last call, in order.
    pub fn take_effects(&mut self) -> Vec<Effect> {
        std::mem::take(&mut self.effects)
    }

    /// Cluster time in nanoseconds: the leader's clock when the entry being applied was logged.
    /// It never goes backwards.
    #[must_use]
    pub fn now(&self) -> u64 {
        self.now
    }

    /// The version of the service's behaviour the cluster runs, as activated: the same on every
    /// node for the same entry. 0 until one is.
    #[must_use]
    pub fn service_version(&self) -> u32 {
        self.service_version
    }

    /// Sends `payload` to `session`. Delivered exactly once, in order, if the session is open.
    ///
    /// # Errors
    ///
    /// If `payload` is over [`MAX_PAYLOAD_BYTES`]: nothing is sent. It's an error rather than a
    /// panic because every node applies the same entry, so a panic would stop them all, and
    /// stop them again each time they replayed it.
    pub fn send(&mut self, session: SessionId, payload: &[u8]) -> Result<(), PayloadTooLong> {
        PayloadTooLong::check(payload)?;
        self.effects.push(Effect::Send(session, payload.to_vec()));
        Ok(())
    }

    /// Fires [`Service::on_timer`] with `id` at cluster time `at` or soon after, replacing a
    /// timer with that ID.
    pub fn schedule_timer(&mut self, id: u64, at: u64) {
        self.effects.push(Effect::Schedule(id, at));
    }

    /// Cancels the timer with `id`, if there is one.
    pub fn cancel_timer(&mut self, id: u64) {
        self.effects.push(Effect::Cancel(id));
    }

    /// Closes `session`, with [`CloseReason::Service`].
    pub fn close_session(&mut self, session: SessionId) {
        self.effects.push(Effect::Close(session));
    }

    /// Puts `payload` in the log, for [`Service::on_proposal`] to apply later, on every node:
    /// work that follows from this callback, done as an entry of its own. It's held in the
    /// replicated state until the leader, whichever node that is by then, has logged it, so it
    /// isn't lost to a failover, and it's applied once.
    ///
    /// # Errors
    ///
    /// If `payload` is over [`MAX_PAYLOAD_BYTES`]: nothing is proposed. As with [`Ctx::send`].
    pub fn propose(&mut self, payload: &[u8]) -> Result<(), PayloadTooLong> {
        PayloadTooLong::check(payload)?;
        self.effects.push(Effect::Propose(payload.to_vec()));
        Ok(())
    }
}

/// A payload over [`MAX_PAYLOAD_BYTES`], which [`Ctx::send`] or [`Ctx::propose`] refused.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PayloadTooLong {
    /// Its length.
    pub len: usize,
}

impl PayloadTooLong {
    fn check(payload: &[u8]) -> Result<(), Self> {
        if payload.len() > MAX_PAYLOAD_BYTES { Err(Self { len: payload.len() }) } else { Ok(()) }
    }
}

impl fmt::Display for PayloadTooLong {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "payload of {} bytes is over MAX_PAYLOAD_BYTES ({MAX_PAYLOAD_BYTES})", self.len)
    }
}

impl std::error::Error for PayloadTooLong {}

/// A node's configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Identity and Raft timing.
    pub raft: raft::Config,
    /// A session the leader hears nothing from for this long is closed.
    pub session_timeout_ns: u64,
    /// Each node snapshots, and compacts its log, once this many entries follow its last snapshot.
    pub snapshot_every_entries: u64,
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
    /// applied records kept in memory, a 10 s session timeout, a snapshot every 100,000 entries,
    /// and 8192 responses kept per session. Change any field after.
    #[must_use]
    pub fn new(id: NodeId, members: Vec<Member>) -> Self {
        let nanos = |d: Duration| u64::try_from(d.as_nanos()).expect("small");
        Self {
            raft: raft::Config {
                id,
                members,
                election_timeout_ns: nanos(Duration::from_millis(300)),
                heartbeat_ns: nanos(Duration::from_millis(50)),
                seed: u64::from(id),
                log_window_bytes: 64 << 20,
            },
            session_timeout_ns: nanos(Duration::from_secs(10)),
            snapshot_every_entries: 100_000,
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
    pub snapshot_reads: Vec<(u64, u64)>,
    /// Write this snapshot, apart from the batches, and tell [`Node::on_snapshot_saved`] once
    /// it's durable. There's one at a time.
    pub save: Option<SaveSnapshot>,
    /// After `persist`, [`Node::restore`] from the latest snapshot, which is of this index. Until
    /// then, give the node no input: a snapshot installed after this one would leave this one's
    /// file gone.
    pub restore: Option<u64>,
    /// The peers to connect to changed, to these: the members, and a member being added.
    pub peers: Option<Vec<Member>>,
    /// As leader, how long each entry committed took from proposed, in nanoseconds: see
    /// [`crate::metrics`].
    pub commit_latencies: Vec<u64>,
}

#[derive(Debug, Default)]
struct Session {
    /// The last `client_seq` applied: the next must be one more.
    last_seq: u64,
    /// The `egress_seq` the next response gets.
    next_egress: u64,
    ring: VecDeque<(u64, Vec<u8>)>,
    /// Who opened it: only it may reattach.
    principal: Vec<u8>,
}

/// Replicated: the same on every node at the same applied index.
#[derive(Debug, Default)]
struct State {
    sessions: BTreeMap<SessionId, Session>,
    /// Timer ID to its deadline, and the same ordered by deadline.
    timers: BTreeMap<u64, u64>,
    deadlines: BTreeSet<(u64, u64)>,
    /// The protocol and service versions the cluster runs, as activated.
    protocol: u32,
    service_version: u32,
    /// What the service proposed, by ID, until it's applied; and the next ID.
    // ponytail: unbounded, like timers; bound it if a service can propose faster than the log
    // commits.
    outbox: BTreeMap<u64, Vec<u8>>,
    next_proposal: u64,
    applied: u64,
    time: u64,
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
    opening: BTreeMap<u64, ConnId>,
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
    /// When the latest snapshot was taken or installed, or the node started.
    snapshot_at: u64,
}

impl<S: Service> Node<S> {
    /// A member starting from its durable `meta`, latest `snapshot` and `log`, with
    /// `service` new. With a snapshot, [`Node::restore`] from it next.
    #[must_use]
    pub fn new(config: Config, service: S, meta: Meta, snapshot: Option<Snapshot>, log: Entries, now: u64) -> Self {
        assert!(config.session_timeout_ns > config.raft.election_timeout_ns * 2, "sessions outlive an election");
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

    /// An operator's request on `conn`. `SnapshotRequest` asks for a snapshot at the next index,
    /// on every node, and is answered `SnapshotTaken` once a quorum has it durable. `AddMember`
    /// and `RemoveMember` change the members, one at a time, and are answered `MembersChanged`
    /// once the change is committed, or `MembersRefused`.
    pub fn on_admin(&mut self, conn: ConnId, message: &wire::Decoded<'_>, now: u64) {
        self.after(now);
        let admin = matches!(
            message,
            wire::Decoded::SnapshotRequest(_)
                | wire::Decoded::AddMember(_)
                | wire::Decoded::RemoveMember(_)
                | wire::Decoded::TransferLeadership(_)
                | wire::Decoded::Shutdown(_)
                | wire::Decoded::Activate(_)
        );
        if !admin {
            return;
        }
        if !self.is_ready() {
            self.redirect_to(conn, |m| &m.admin);
            return;
        }
        match message {
            wire::Decoded::SnapshotRequest(m) => {
                let id = m.request_id();
                if self.local.requested.contains_key(&id) || self.local.awaiting.contains_key(&id) {
                    return;
                }
                self.local.requested.insert(id, conn);
                self.propose(&wire::Snapshot { timestamp: now, request_id: id });
            }
            wire::Decoded::AddMember(m) => {
                let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
                let member =
                    Member { id: m.id(), peer: text(m.peer()), client: text(m.client()), admin: text(m.admin()) };
                let free = self.local.changing.is_none() && self.local.activating.is_none();
                let started = free && self.raft.add_member(member);
                self.start_change(conn, m.request_id(), started);
            }
            wire::Decoded::RemoveMember(m) => {
                let started = self.local.changing.is_none() && self.raft.remove_member(m.id());
                self.start_change(conn, m.request_id(), started);
            }
            wire::Decoded::Activate(m) => self.activate(conn, m.request_id(), m.protocol(), m.service_version()),
            wire::Decoded::Shutdown(m) => {
                if self.local.shutting.is_none() {
                    self.local.shutting = Some((conn, m.request_id(), None));
                    self.propose(&wire::Snapshot { timestamp: now, request_id: m.request_id() });
                }
            }
            wire::Decoded::TransferLeadership(m) => {
                let request_id = m.request_id();
                let free = self.transferring.is_none() && self.local.shutting.is_none();
                if free && self.raft.transfer_leadership(m.to()).is_some() {
                    self.transferring = Some((conn, request_id));
                } else {
                    self.send_client(conn, &wire::LeadershipTransferred { request_id, leader: 0 });
                }
            }
            _ => unreachable!("matched above"),
        }
    }

    /// Logs a switch to these versions, unless a member doesn't support them, or it's a step
    /// back, or one is under way.
    fn activate(&mut self, conn: ConnId, request_id: u64, protocol: u32, service_version: u32) {
        let (lowest_protocol, lowest_service_version) = self.raft.lowest_versions();
        let supported = protocol <= lowest_protocol && service_version <= lowest_service_version;
        let forward = protocol >= self.state.protocol && service_version >= self.state.service_version;
        // Not while a member is being added: it could join, too old, after this is logged.
        let free = self.local.activating.is_none() && !self.raft.changing_members();
        let index = (supported && forward && free)
            .then(|| self.propose(&wire::Activated { timestamp: self.now, protocol, service_version }))
            .flatten();
        match index {
            Some(index) => self.local.activating = Some((conn, request_id, index)),
            None => {
                let refused = wire::Activation { request_id, index: 0, lowest_protocol, lowest_service_version };
                self.send_client(conn, &refused);
            }
        }
    }

    fn start_change(&mut self, conn: ConnId, request_id: u64, started: bool) {
        if started {
            self.local.changing = Some((conn, request_id, None));
        } else {
            self.send_client(conn, &wire::MembersRefused { request_id });
        }
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
    pub fn on_persisted(&mut self, end: (u64, u64)) {
        self.raft.on_persisted(end);
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
            wire::Decoded::Reattach(m) => self.reattach(conn, m.session(), m.last_egress_seq(), m.credentials()),
            wire::Decoded::Message(m) => {
                if let Some(session) = self.check_attached(conn, m.session()) {
                    self.submit(conn, session, m.client_seq(), m.payload());
                }
            }
            wire::Decoded::KeepAlive(m) => {
                if let Some(session) = self.check_attached(conn, m.session()) {
                    self.local.activity.insert(session, now);
                    let applied_seq = self.state.sessions[&session].last_seq;
                    self.send_client(conn, &wire::KeepAliveAck { applied_seq });
                }
            }
            wire::Decoded::Close(m) => {
                if let Some(session) = self.check_attached(conn, m.session()) {
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
            out.commit_latencies.extend(more.commit_latencies);
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
            commit_latencies: out.commit_latencies,
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
                deadline = deadline.min(at + self.config.session_timeout_ns);
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
                self.send_client(conn, &wire::LeadershipTransferred { request_id, leader: other.unwrap_or(0) });
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

    /// Answers each manual snapshot a quorum, this leader among it, now has durable.
    fn answer_snapshot_requests(&mut self) {
        // Shutting down: once the members have the snapshot, answered, and stop.
        if let Some((conn, request_id, Some(index))) = self.local.shutting
            && self.raft.all_have_snapshot(index)
        {
            self.local.shutting = None;
            self.send_client(conn, &wire::ShutDown { request_id, index });
            self.raft.stop(index);
        }
        if self.local.awaiting.is_empty() {
            return;
        }
        let durable = self.raft.quorum_snapshot_index().min(self.raft.snapshot_index());
        let done: Vec<(u64, (ConnId, u64))> =
            self.local.awaiting.iter().filter(|(_, (_, index))| *index <= durable).map(|(&r, &v)| (r, v)).collect();
        for (request_id, (conn, index)) in done {
            self.local.awaiting.remove(&request_id);
            self.send_client(conn, &wire::SnapshotTaken { request_id, index, quorum_acked: 1 });
        }
    }

    fn become_leader(&mut self) {
        self.local = Local { was_leader: true, sends: std::mem::take(&mut self.local.sends), ..Local::default() };
        // Every session gets a full timeout from now: the old leader's view of them is gone.
        self.local.activity = self.state.sessions.keys().map(|&s| (s, self.now)).collect();
        self.propose(&wire::NewTerm { timestamp: self.now });
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

    fn open(&mut self, conn: ConnId, credentials: &[u8]) {
        if self.state.sessions.len() + self.local.opening.len() >= MAX_SESSIONS {
            self.send_client(conn, &wire::Closed { session: 0, reason: CloseReason::NoSession });
            return;
        }
        let Some(principal) = self.authenticate(credentials) else {
            self.send_client(conn, &wire::Closed { session: 0, reason: CloseReason::Refused });
            return;
        };
        if let Some(index) = self.propose(&wire::SessionOpen { timestamp: self.now, principal: &principal }) {
            self.local.opening.insert(index, conn);
        }
    }

    /// The principal the service gives `credentials`, if it accepts them and it's short enough.
    fn authenticate(&self, credentials: &[u8]) -> Option<Vec<u8>> {
        self.service.authenticate(credentials).filter(|p| p.len() <= MAX_PRINCIPAL_BYTES)
    }

    fn reattach(&mut self, conn: ConnId, session: SessionId, last_egress_seq: u64, credentials: &[u8]) {
        let Some(s) = self.state.sessions.get(&session) else {
            self.send_client(conn, &wire::Closed { session, reason: CloseReason::NoSession });
            return;
        };
        // A session's ID is easy to guess: its owner proves it's still who opened it.
        if self.authenticate(credentials).as_ref() != Some(&s.principal) {
            self.send_client(conn, &wire::Closed { session, reason: CloseReason::Refused });
            return;
        }
        let s = &self.state.sessions[&session];
        if let Some(old) = self.local.attached.insert(session, conn) {
            self.local.conns.insert(old, None);
        }
        self.local.conns.insert(conn, Some(session));
        self.local.activity.insert(session, self.now);
        let applied_seq = s.last_seq;
        let replay: Vec<(u64, Vec<u8>)> = s.ring.iter().filter(|(seq, _)| *seq > last_egress_seq).cloned().collect();
        self.send_client(conn, &wire::Opened { session, applied_seq });
        for (egress_seq, payload) in replay {
            self.send_client(conn, &wire::Egress { session, egress_seq, applied_seq, payload: &payload });
        }
    }

    /// The session `conn` is attached to, if it's `session`; otherwise tells the client.
    fn check_attached(&mut self, conn: ConnId, session: SessionId) -> Option<SessionId> {
        if self.local.conns.get(&conn) == Some(&Some(session)) && self.state.sessions.contains_key(&session) {
            return Some(session);
        }
        self.send_client(conn, &wire::Closed { session, reason: CloseReason::NoSession });
        None
    }

    fn submit(&mut self, conn: ConnId, session: SessionId, client_seq: u64, payload: &[u8]) {
        self.local.activity.insert(session, self.now);
        let last_seq = self.state.sessions[&session].last_seq;
        if client_seq <= last_seq {
            // A resend of something applied: tell the client so it stops resending.
            self.send_client(conn, &wire::KeepAliveAck { applied_seq: last_seq });
            return;
        }
        // Propose each message once a term, in order, and at most `MAX_UNACKED` ahead of what's
        // applied: the window `Client` keeps to, so a client can't outrun the quorum. What's
        // already proposed will apply; a gap or a message past the window is dropped, for the
        // client to resend.
        let next = self.local.proposed.get(&session).map_or(last_seq, |&p| p.max(last_seq)) + 1;
        if client_seq != next || client_seq - last_seq > MAX_UNACKED as u64 {
            return;
        }
        if self.propose(&wire::SessionMessage { timestamp: self.now, session, client_seq, payload }).is_some() {
            self.local.proposed.insert(session, client_seq);
        }
    }

    fn propose_close(&mut self, session: SessionId, reason: CloseReason) {
        if self.local.closing.insert(session) {
            self.propose(&wire::SessionClose { timestamp: self.now, session, reason });
        }
    }

    fn expire_sessions(&mut self, now: u64) {
        let timeout = self.config.session_timeout_ns;
        let expired: Vec<SessionId> =
            self.local.activity.iter().filter(|&(_, &at)| at + timeout <= now).map(|(&s, _)| s).collect();
        for session in expired {
            self.propose_close(session, CloseReason::Timeout);
        }
    }

    fn fire_timers(&mut self, now: u64) {
        let due: Vec<u64> = self.state.deadlines.iter().take_while(|(at, _)| *at <= now).map(|&(_, id)| id).collect();
        for id in due {
            if self.local.firing.insert(id) {
                self.propose(&wire::TimerFired { timestamp: now, id });
            }
        }
    }

    fn propose(&mut self, entry: &impl turbofan_sbe::Encode) -> Option<u64> {
        self.scratch.clear();
        entry.encode_into(&mut self.scratch).expect("entries are within their limits");
        self.raft.propose(&self.scratch)
    }

    /// Applies committed entries, in order, as far as their records are in memory; fetches the
    /// next from disk if it isn't.
    fn apply(&mut self) {
        if self.restoring.is_some() {
            return;
        }
        while self.state.applied < self.raft.commit_index() {
            let index = self.state.applied + 1;
            let ours = self.raft.role() == Role::Leader && self.raft.term_at(index) == Some(self.raft.term());
            let Some(payload) = self.raft.entry(index).map(<[u8]>::to_vec) else {
                self.raft.fetch(index);
                break;
            };
            // An entry this node can't read was logged by a newer one before every member could
            // read it, which WIRE.md's rules forbid.
            let (entry, _) = wire::decode(&payload).expect("a log entry of a version this node knows");
            self.state.applied = index;
            self.apply_entry(index, &entry, ours);
            let wanted = matches!(entry, wire::Decoded::Snapshot(_));
            let last = self.raft.snapshot_index().max(self.saving.unwrap_or(0));
            if wanted || index >= last + self.config.snapshot_every_entries {
                self.take_snapshot();
            }
        }
        self.raft.on_applied(self.state.applied);
        self.log_outbox();
    }

    fn apply_entry(&mut self, index: u64, entry: &wire::Decoded<'_>, ours: bool) {
        match entry {
            wire::Decoded::NewTerm(e) => {
                self.advance_time(e.timestamp());
                self.local.ready |= ours;
            }
            wire::Decoded::Activated(e) => {
                self.advance_time(e.timestamp());
                self.apply_activated(index, e.protocol(), e.service_version());
            }
            wire::Decoded::ServiceProposal(e) => {
                self.advance_time(e.timestamp());
                // Logged twice, by leaders either side of a failover: applied the first time.
                if self.state.outbox.remove(&e.id()).is_some() {
                    self.call(|service, ctx| service.on_proposal(ctx, e.payload()));
                }
            }
            wire::Decoded::Members(e) => {
                self.advance_time(e.timestamp());
                if let Some((conn, request_id, Some(at))) = self.local.changing
                    && at == index
                {
                    self.local.changing = None;
                    self.send_client(conn, &wire::MembersChanged { request_id, index });
                }
            }
            wire::Decoded::Snapshot(e) => {
                self.advance_time(e.timestamp());
                if let Some(conn) = self.local.requested.remove(&e.request_id()) {
                    self.local.awaiting.insert(e.request_id(), (conn, index));
                }
                if let Some((_, request_id, at @ None)) = self.local.shutting.as_mut()
                    && *request_id == e.request_id()
                {
                    *at = Some(index);
                }
            }
            wire::Decoded::SessionOpen(e) => {
                self.advance_time(e.timestamp());
                self.apply_open(index, e.principal());
            }
            wire::Decoded::SessionMessage(e) => {
                self.advance_time(e.timestamp());
                let Some(s) = self.state.sessions.get_mut(&e.session()) else { return };
                // Exactly once, in order: a resend or one after a gap the client will resend is dropped.
                if e.client_seq() != s.last_seq + 1 {
                    return;
                }
                s.last_seq = e.client_seq();
                self.call(|service, ctx| service.on_message(ctx, e.session(), e.payload()));
            }
            wire::Decoded::SessionClose(e) => {
                self.advance_time(e.timestamp());
                self.apply_close(e.session(), e.reason());
            }
            wire::Decoded::TimerFired(e) => {
                self.advance_time(e.timestamp());
                self.local.firing.remove(&e.id());
                // A timer rescheduled later, or cancelled, since this was logged doesn't fire.
                if self.state.timers.get(&e.id()).is_some_and(|&at| at <= self.state.time) {
                    self.remove_timer(e.id());
                    self.call(|service, ctx| service.on_timer(ctx, e.id()));
                }
            }
            _ => panic!("a log entry this node knows, at {index}: see WIRE.md"),
        }
    }

    /// From `index` on, the cluster runs these versions.
    fn apply_activated(&mut self, index: u64, protocol: u32, service_version: u32) {
        self.state.protocol = protocol;
        self.raft.require(protocol, service_version);
        if let Some((conn, request_id, at)) = self.local.activating
            && at == index
        {
            self.local.activating = None;
            let (lowest_protocol, lowest_service_version) = self.raft.lowest_versions();
            self.send_client(conn, &wire::Activation { request_id, index, lowest_protocol, lowest_service_version });
        }
        if service_version != self.state.service_version {
            self.state.service_version = service_version;
            self.call(|service, ctx| service.on_activate(ctx, service_version));
        }
    }

    fn apply_open(&mut self, session: SessionId, principal: &[u8]) {
        let opened = Session { last_seq: 0, next_egress: 1, ring: VecDeque::new(), principal: principal.to_vec() };
        self.state.sessions.insert(session, opened);
        if let Some(conn) = self.local.opening.remove(&session) {
            self.local.conns.insert(conn, Some(session));
            self.local.attached.insert(session, conn);
            self.send_client(conn, &wire::Opened { session, applied_seq: 0 });
        }
        if self.local.was_leader {
            self.local.activity.insert(session, self.now);
        }
        self.call(|service, ctx| service.on_session_open(ctx, session, principal));
    }

    fn apply_close(&mut self, session: SessionId, reason: CloseReason) {
        self.local.closing.remove(&session);
        self.local.activity.remove(&session);
        self.local.proposed.remove(&session);
        if self.state.sessions.remove(&session).is_none() {
            return;
        }
        if let Some(conn) = self.local.attached.remove(&session) {
            self.local.conns.insert(conn, None);
            self.send_client(conn, &wire::Closed { session, reason });
        }
        self.call(|service, ctx| service.on_session_close(ctx, session, reason));
    }

    fn advance_time(&mut self, timestamp: u64) {
        // A new leader's clock may be behind the old one's: cluster time holds still instead.
        self.state.time = self.state.time.max(timestamp);
    }

    /// Runs a service callback, then its effects.
    fn call(&mut self, callback: impl FnOnce(&mut S, &mut Ctx)) {
        self.ctx.now = self.state.time;
        self.ctx.service_version = self.state.service_version;
        callback(&mut self.service, &mut self.ctx);
        let effects = std::mem::take(&mut self.ctx.effects);
        for effect in effects {
            match effect {
                Effect::Send(session, payload) => self.egress(session, payload),
                Effect::Schedule(id, at) => {
                    self.remove_timer(id);
                    self.state.timers.insert(id, at);
                    self.state.deadlines.insert((at, id));
                }
                Effect::Cancel(id) => self.remove_timer(id),
                Effect::Close(session) => self.apply_close(session, CloseReason::Service),
                Effect::Propose(payload) => {
                    self.state.next_proposal += 1;
                    self.state.outbox.insert(self.state.next_proposal, payload);
                }
            }
        }
    }

    /// As a ready leader, logs what's in the outbox and not yet logged this term, in order.
    fn log_outbox(&mut self) {
        if !self.is_ready() {
            return;
        }
        let waiting: Vec<(u64, Vec<u8>)> =
            self.state.outbox.range(self.local.logged + 1..).map(|(&id, p)| (id, p.clone())).collect();
        for (id, payload) in waiting {
            self.local.logged = id;
            self.propose(&wire::ServiceProposal { timestamp: self.now, id, payload: &payload });
        }
    }

    fn egress(&mut self, session: SessionId, payload: Vec<u8>) {
        let Some(s) = self.state.sessions.get_mut(&session) else { return };
        let egress_seq = s.next_egress;
        s.next_egress += 1;
        if let Some(&conn) = self.local.attached.get(&session) {
            let message = wire::Egress { session, egress_seq, applied_seq: s.last_seq, payload: &payload };
            let mut frame = Vec::new();
            codec::encode_frame(&message, &mut frame).expect("payloads are within their limit");
            self.local.sends.push((conn, frame));
        }
        if s.ring.len() == self.config.egress_ring_entries {
            s.ring.pop_front();
        }
        s.ring.push_back((egress_seq, payload));
    }

    fn remove_timer(&mut self, id: u64) {
        if let Some(at) = self.state.timers.remove(&id) {
            self.state.deadlines.remove(&(at, id));
        }
    }

    fn redirect(&mut self, conn: ConnId) {
        self.redirect_to(conn, |m| &m.client);
    }

    /// Points `conn` at the leader's address, the one `addr` picks, or nowhere if there's no
    /// leader yet, or it isn't a member this node knows.
    fn redirect_to(&mut self, conn: ConnId, addr: impl Fn(&Member) -> &String) {
        let leader = self.raft.leader().filter(|&l| l != self.raft.id());
        let member = leader.and_then(|l| self.raft.members().iter().find(|m| m.id == l));
        let addr = member.map_or(&[][..], |m| addr(m).as_bytes());
        let message = wire::Redirect { leader: leader.unwrap_or(0), addr };
        let mut frame = Vec::new();
        codec::encode_frame(&message, &mut frame).expect("addresses are short");
        self.local.sends.push((conn, frame));
    }

    fn send_client(&mut self, conn: ConnId, message: &impl turbofan_sbe::Encode) {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).expect("client messages are within their limits");
        self.local.sends.push((conn, frame));
    }
}

/// Encodes the replicated state, which a snapshot starts with: SBE messages, each framed as on
/// the wire (`schema/cluster.xml` has their order). The service's state follows, to the end.
fn encode_state(state: &State) -> Vec<u8> {
    fn put(out: &mut Vec<u8>, message: &impl turbofan_sbe::Encode) {
        codec::encode_frame(message, out).expect("snapshot messages are within their limits");
    }
    let mut out = Vec::new();
    let node = wire::NodeState {
        applied: state.applied,
        time: state.time,
        next_proposal: state.next_proposal,
        sessions: u32::try_from(state.sessions.len()).expect("at most MAX_SESSIONS"),
        timers: state.timers.len() as u64,
        proposals: state.outbox.len() as u64,
        protocol: state.protocol,
        service_version: state.service_version,
    };
    put(&mut out, &node);
    for (&session, s) in &state.sessions {
        let ring = u32::try_from(s.ring.len()).expect("at most MAX_EGRESS_RING");
        let (last_seq, next_egress) = (s.last_seq, s.next_egress);
        put(&mut out, &wire::SessionState { session, last_seq, next_egress, ring, principal: &s.principal });
        for (egress_seq, payload) in &s.ring {
            put(&mut out, &wire::EgressState { egress_seq: *egress_seq, payload });
        }
    }
    for (&id, &at) in &state.timers {
        put(&mut out, &wire::TimerState { id, at });
    }
    for (&id, payload) in &state.outbox {
        put(&mut out, &wire::ProposalState { id, payload });
    }
    out
}

/// Reads what [`encode_state`] wrote, leaving `from` at the service's state.
///
/// # Errors
///
/// If reading fails, or the messages run out, aren't the ones expected, or hold counts past
/// their limits.
fn decode_state(from: &mut dyn Read) -> io::Result<State> {
    let frame = read_message(from)?;
    let wire::Decoded::NodeState(n) = decoded(&frame)? else { return Err(bad("no NodeState")) };
    let mut state = State {
        applied: n.applied(),
        time: n.time(),
        next_proposal: n.next_proposal(),
        protocol: n.protocol(),
        service_version: n.service_version(),
        ..State::default()
    };
    let (sessions, timers, proposals) = (n.sessions(), n.timers(), n.proposals());
    if usize::try_from(sessions).map_or(true, |n| n > MAX_SESSIONS) {
        return Err(bad("too many sessions"));
    }
    for _ in 0..sessions {
        let frame = read_message(from)?;
        let wire::Decoded::SessionState(s) = decoded(&frame)? else { return Err(bad("no SessionState")) };
        if usize::try_from(s.ring()).map_or(true, |n| n > MAX_EGRESS_RING) {
            return Err(bad("too long a ring"));
        }
        let mut session = Session {
            last_seq: s.last_seq(),
            next_egress: s.next_egress(),
            ring: VecDeque::new(),
            principal: s.principal().to_vec(),
        };
        for _ in 0..s.ring() {
            let frame = read_message(from)?;
            let wire::Decoded::EgressState(e) = decoded(&frame)? else { return Err(bad("no EgressState")) };
            session.ring.push_back((e.egress_seq(), e.payload().to_vec()));
        }
        state.sessions.insert(s.session(), session);
    }
    for _ in 0..timers {
        let frame = read_message(from)?;
        let wire::Decoded::TimerState(t) = decoded(&frame)? else { return Err(bad("no TimerState")) };
        state.timers.insert(t.id(), t.at());
        state.deadlines.insert((t.at(), t.id()));
    }
    for _ in 0..proposals {
        let frame = read_message(from)?;
        let wire::Decoded::ProposalState(p) = decoded(&frame)? else { return Err(bad("no ProposalState")) };
        state.outbox.insert(p.id(), p.payload().to_vec());
    }
    Ok(state)
}

/// The next framed message of a snapshot, read no further.
fn read_message(from: &mut dyn Read) -> io::Result<Vec<u8>> {
    let mut len = [0; 4];
    from.read_exact(&mut len)?;
    let len = usize::try_from(u32::from_le_bytes(len)).expect("u32 fits usize");
    if len > codec::MAX_FRAME_BYTES {
        return Err(bad("a message over MAX_FRAME_BYTES"));
    }
    let mut frame = vec![0; 4 + len];
    frame[..4].copy_from_slice(&u32::try_from(len).expect("checked").to_le_bytes());
    from.read_exact(&mut frame[4..])?;
    Ok(frame)
}

fn decoded(frame: &[u8]) -> io::Result<wire::Decoded<'_>> {
    codec::decode_frame(frame).ok().flatten().map(|(message, _)| message).ok_or_else(|| bad("a malformed message"))
}

fn bad(what: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, format!("snapshot: {what}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    const MS: u64 = 1_000_000;
    const TIMEOUT: u64 = 1000 * MS;

    /// Logs every callback. A message `at <id> <ms>` schedules a timer and `cancel <id>`
    /// cancels one; any message is echoed.
    #[derive(Debug, Default)]
    struct Recorder(Vec<String>);

    impl Service for Recorder {
        /// Refuses `bad`; anything else is its own principal.
        fn authenticate(&self, credentials: &[u8]) -> Option<Vec<u8>> {
            (credentials != b"bad").then(|| credentials.to_vec())
        }
        fn on_session_open(&mut self, _ctx: &mut Ctx, session: SessionId, principal: &[u8]) {
            self.0.push(format!("open {session} {}", String::from_utf8_lossy(principal)));
        }
        fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, reason: CloseReason) {
            self.0.push(format!("close {session} {reason:?}"));
        }
        fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
            let text = std::str::from_utf8(payload).unwrap();
            self.0.push(format!("message {session} {text}"));
            if ctx.service_version() > 0 {
                self.0.push(format!("as version {}", ctx.service_version()));
            }
            match text.split(' ').collect::<Vec<_>>()[..] {
                ["at", id, ms] => ctx.schedule_timer(id.parse().unwrap(), ms.parse::<u64>().unwrap() * MS),
                ["cancel", id] => ctx.cancel_timer(id.parse().unwrap()),
                ["propose", what] => ctx.propose(what.as_bytes()).expect("a client payload fits"),
                _ => {}
            }
            ctx.send(session, payload).expect("a client payload fits");
        }
        fn on_proposal(&mut self, _ctx: &mut Ctx, payload: &[u8]) {
            self.0.push(format!("proposal {}", std::str::from_utf8(payload).unwrap()));
        }
        fn version(&self) -> u32 {
            1
        }
        fn on_activate(&mut self, _ctx: &mut Ctx, version: u32) {
            self.0.push(format!("activate {version}"));
        }
        fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
            self.0.push(format!("timer {id} at {}", ctx.now() / MS));
        }
        fn snapshot(&self) -> SnapshotWriter {
            let text = self.0.join("\n");
            Box::new(move |out| out.write_all(text.as_bytes()))
        }
        fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
            let mut text = String::new();
            from.read_to_string(&mut text)?;
            self.0 = if text.is_empty() { Vec::new() } else { text.split('\n').map(String::from).collect() };
            Ok(())
        }
    }

    fn member(id: NodeId) -> Member {
        Member { id, peer: format!("p{id}"), client: format!("n{id}"), admin: format!("a{id}") }
    }

    /// A payload over the limit is refused, not sent or proposed, and not a panic.
    #[test]
    fn ctx_refuses_a_payload_over_the_limit() {
        let mut ctx = Ctx::new(0);
        let long = vec![0; MAX_PAYLOAD_BYTES + 1];
        assert_eq!(ctx.send(1, &long), Err(PayloadTooLong { len: MAX_PAYLOAD_BYTES + 1 }));
        assert_eq!(ctx.propose(&long), Err(PayloadTooLong { len: MAX_PAYLOAD_BYTES + 1 }));
        assert_eq!(ctx.send(1, &long[1..]), Ok(()));
        assert_eq!(ctx.take_effects(), [Effect::Send(1, long[1..].to_vec())]);
    }

    /// A lone member, elected and ready.
    fn leader() -> Node<Recorder> {
        let config = Config {
            raft: raft::Config {
                id: 1,
                members: vec![member(1)],
                election_timeout_ns: 150 * MS,
                heartbeat_ns: 50 * MS,
                seed: 1,
                log_window_bytes: usize::MAX,
            },
            session_timeout_ns: TIMEOUT,
            snapshot_every_entries: 1000,
            egress_ring_entries: 1024,
            protocol: PROTOCOL,
        };
        let mut node = Node::new(config, Recorder::default(), Meta::default(), None, Entries::default(), 0);
        let deadline = node.next_deadline();
        node.on_tick(deadline);
        drive(&mut node);
        assert!(node.is_ready());
        node
    }

    /// Carries out the outputs as a driver would, a snapshot saved at once, returning the client
    /// frames, decoded to text.
    fn drive(node: &mut Node<Recorder>) -> Vec<String> {
        let mut sends = Vec::new();
        loop {
            let out = node.take_outputs();
            sends.extend(out.client_sends);
            if let Some(save) = out.save {
                let (index, term, members) = (save.index, save.term, save.members.clone());
                let len = written(save).len() as u64;
                node.on_snapshot_saved(Snapshot { index, term, len, checksum: 0, members });
                continue;
            }
            if out.persist.is_empty() {
                break;
            }
            node.on_persisted(node.raft().log_end());
        }
        sends.iter().map(|(conn, f)| format!("{conn}: {:?}", codec::decode_frame(f).unwrap().unwrap().0)).collect()
    }

    /// What a snapshot's writer writes.
    fn written(save: SaveSnapshot) -> Vec<u8> {
        let mut bytes = Vec::new();
        (save.writer)(&mut bytes).unwrap();
        bytes
    }

    fn client(node: &mut Node<Recorder>, conn: ConnId, message: &impl turbofan_sbe::Encode, now: u64) -> Vec<String> {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).unwrap();
        node.on_client(conn, &codec::decode_frame(&frame).unwrap().unwrap().0, now);
        drive(node)
    }

    fn open(node: &mut Node<Recorder>, conn: ConnId, now: u64) -> SessionId {
        let sent = client(node, conn, &wire::Connect { credentials: b"" }, now);
        assert!(sent[0].contains("Opened"), "{sent:?}");
        node.local.attached.iter().find(|&(_, &c)| c == conn).map(|(&s, _)| s).unwrap()
    }

    #[test]
    fn messages_apply_once_in_order_and_a_reattach_replays_responses() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        let message = |client_seq, payload| wire::Message { session, client_seq, payload };
        assert!(client(&mut node, 7, &message(1, b"one"), 0)[0].contains("Egress"));
        // A resend of 1 is acknowledged, not applied; 3 before 2 is dropped for the client to resend.
        assert!(
            client(&mut node, 7, &message(1, b"one"), 0)[0]
                .contains("KeepAliveAck(KeepAliveAckRef { applied_seq: 1 })")
        );
        assert!(client(&mut node, 7, &message(3, b"three"), 0).is_empty());
        client(&mut node, 7, &message(2, b"two"), 0);
        let messages: Vec<&String> = node.service.0.iter().filter(|e| e.starts_with("message")).collect();
        assert_eq!(messages, [&format!("message {session} one"), &format!("message {session} two")]);

        // After a failover the client reattaches on a new connection, having seen response 1.
        node.on_disconnect(7);
        let sent = client(&mut node, 8, &wire::Reattach { session, last_egress_seq: 1, credentials: b"" }, 0);
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert!(sent[0].contains("Opened") && sent[0].contains("applied_seq: 2"));
        assert!(sent[1].contains("egress_seq: 2"));
        // The old connection is no longer the session's.
        assert!(client(&mut node, 7, &message(3, b"three"), 0)[0].contains("NoSession"));
    }

    /// A resend of a message proposed but not yet applied, as a client reattaching to the same
    /// leader sends, isn't logged again; neither is one past the client's window.
    #[test]
    fn a_session_proposes_each_message_once_within_its_window() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        let submit = |node: &mut Node<Recorder>, client_seq: u64| {
            let mut frame = Vec::new();
            codec::encode_frame(&wire::Message { session, client_seq, payload: b"m" }, &mut frame).unwrap();
            let before = node.raft().last_index();
            node.on_client(7, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
            node.raft().last_index() - before
        };
        assert_eq!(submit(&mut node, 1), 1);
        assert_eq!(submit(&mut node, 1), 0, "a resend of what's in flight");
        for client_seq in 2..=MAX_UNACKED as u64 {
            assert_eq!(submit(&mut node, client_seq), 1);
        }
        assert_eq!(submit(&mut node, MAX_UNACKED as u64 + 1), 0, "past the window");
        drive(&mut node);
        assert_eq!(node.service.0.iter().filter(|e| e.starts_with("message")).count(), MAX_UNACKED);
        // Once they apply, the window moves on.
        assert_eq!(submit(&mut node, MAX_UNACKED as u64 + 1), 1);
    }

    /// A client the service refuses gets no session; one it accepts opens one as its principal,
    /// and only that principal may reattach to it.
    #[test]
    fn credentials_open_a_session_and_reattach_to_it() {
        let mut node = leader();
        let sent = client(&mut node, 1, &wire::Connect { credentials: b"bad" }, 0);
        assert!(sent[0].contains("Refused"), "{sent:?}");
        client(&mut node, 2, &wire::Connect { credentials: b"alice" }, 0);
        let session = node.local.attached.iter().find(|&(_, &c)| c == 2).map(|(&s, _)| s).unwrap();
        assert!(node.service.0.contains(&format!("open {session} alice")));

        let reattach = |credentials| wire::Reattach { session, last_egress_seq: 0, credentials };
        assert!(client(&mut node, 3, &reattach(b"mallory"), 0)[0].contains("Refused"), "a guessed session");
        assert!(client(&mut node, 4, &reattach(b"alice"), 0)[0].contains("Opened"));
    }

    #[test]
    fn a_silent_session_times_out_and_a_keepalive_holds_it_open() {
        let mut node = leader();
        let quiet = open(&mut node, 1, 0);
        let kept = open(&mut node, 2, 0);
        client(&mut node, 2, &wire::KeepAlive { session: kept }, TIMEOUT / 2);
        assert!(node.next_deadline() <= TIMEOUT);
        node.on_tick(TIMEOUT);
        let sent = drive(&mut node);
        assert!(sent.iter().any(|s| s.starts_with("1: ") && s.contains("Timeout")), "{sent:?}");
        assert!(node.service.0.contains(&format!("close {quiet} Timeout")));
        assert!(node.state.sessions.contains_key(&kept));
    }

    #[test]
    fn a_rescheduled_timer_fires_once_at_its_new_time_and_a_cancelled_one_never() {
        let mut node = leader();
        let session = open(&mut node, 1, 0);
        for (client_seq, text) in [(1, "at 5 100"), (2, "at 6 100"), (3, "at 5 300"), (4, "cancel 6")] {
            client(&mut node, 1, &wire::Message { session, client_seq, payload: text.as_bytes() }, 0);
        }
        // A keepalive stops the session timing out over the wait.
        for now in [100, 200, 300, 400].map(|ms| ms * MS) {
            client(&mut node, 1, &wire::KeepAlive { session }, now);
            node.on_tick(now);
            drive(&mut node);
        }
        let timers: Vec<&String> = node.service.0.iter().filter(|e| e.starts_with("timer")).collect();
        assert_eq!(timers, ["timer 5 at 300"]);
    }

    #[test]
    fn a_follower_redirects_clients_to_the_leader() {
        let config = Config {
            raft: raft::Config {
                id: 2,
                members: vec![member(1), member(2), member(3)],
                election_timeout_ns: 150 * MS,
                heartbeat_ns: 50 * MS,
                seed: 1,
                log_window_bytes: usize::MAX,
            },
            session_timeout_ns: TIMEOUT,
            snapshot_every_entries: 1000,
            egress_ring_entries: 1024,
            protocol: PROTOCOL,
        };
        let mut node = Node::new(config, Recorder::default(), Meta::default(), None, Entries::default(), 0);
        let mut frame = Vec::new();
        let append = wire::AppendEntries {
            term: 1,
            leader: 1,
            prev_index: 0,
            prev_term: 0,
            commit: 0,
            count: 0,
            entries: &[],
            last_index: 0,
        };
        codec::encode_frame(&append, &mut frame).unwrap();
        node.on_peer(1, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        drive(&mut node);
        let sent = client(&mut node, 1, &wire::Connect { credentials: b"" }, 0);
        assert!(sent[0].contains("leader: 1, addr: [110, 49]"), "{sent:?}");
    }

    /// An operator switches the cluster to versions every member has, at a point in the log;
    /// not to ones a member lacks, or back.
    #[test]
    fn versions_activate_once_every_member_has_them() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        let activate = |node: &mut Node<Recorder>, protocol, service_version| {
            let mut frame = Vec::new();
            codec::encode_frame(&wire::Activate { request_id: 1, protocol, service_version }, &mut frame).unwrap();
            node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
            drive(node).join(" ")
        };
        let refused = activate(&mut node, PROTOCOL + 1, 0);
        assert!(refused.contains("index: 0, lowest_protocol: 0, lowest_service_version: 1"), "{refused}");
        assert!(!activate(&mut node, 0, 1).contains("index: 0"), "this node has version 1");
        assert!(node.service.0.contains(&"activate 1".to_owned()));
        client(&mut node, 7, &wire::Message { session, client_seq: 1, payload: b"later" }, 0);
        assert!(node.service.0.contains(&"as version 1".to_owned()));
        assert!(activate(&mut node, 0, 0).contains("index: 0"), "not back");

        let bytes = encode_state(&node.state);
        let state = decode_state(&mut &bytes[..]).unwrap();
        assert_eq!((state.protocol, state.service_version), (0, 1), "kept in snapshots");
    }

    /// What a service proposes comes back through the log, once, even when it's logged twice.
    #[test]
    fn a_proposal_is_applied_once_through_the_log() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        client(&mut node, 7, &wire::Message { session, client_seq: 1, payload: b"propose hi" }, 0);
        drive(&mut node);
        // As a leader before a failover would have: the same proposal, logged again.
        node.propose(&wire::ServiceProposal { timestamp: 0, id: 1, payload: b"hi" });
        drive(&mut node);
        let proposals: Vec<&String> = node.service.0.iter().filter(|e| e.starts_with("proposal")).collect();
        assert_eq!(proposals, ["proposal hi"]);
        assert!(node.state.outbox.is_empty());
    }

    /// A node restarted from a snapshot has the same sessions, rings, timers and service, as of
    /// when the snapshot was taken, though it was written after more was applied.
    #[test]
    fn a_snapshot_restores_the_whole_state_as_it_was_taken() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        for (client_seq, text) in [(1, "one"), (2, "at 9 500")] {
            client(&mut node, 7, &wire::Message { session, client_seq, payload: text.as_bytes() }, 0);
        }
        node.take_snapshot();
        let save = node.take_outputs().save.unwrap();
        let (index, term, members) = (save.index, save.term, save.members.clone());
        assert_eq!(index, node.applied());
        let (state, service) = (encode_state(&node.state), node.service.0.clone());
        client(&mut node, 7, &wire::Message { session, client_seq: 3, payload: b"later" }, 0);
        let bytes = written(save);

        let config = node.config.clone();
        let snapshot = Snapshot { index, term, len: bytes.len() as u64, checksum: 0, members };
        let mut restored =
            Node::new(config, Recorder::default(), Meta::default(), Some(snapshot), Entries::default(), 0);
        restored.restore(&mut &bytes[..]).unwrap();
        assert_eq!(encode_state(&restored.state), state);
        assert_eq!(restored.state.sessions[&session].ring.len(), 2);
        assert_eq!(restored.state.timers.get(&9), Some(&(500 * MS)));
        assert_eq!(restored.service.0, service);
    }

    /// A snapshot cut short anywhere is refused, never misread or a panic.
    #[test]
    fn a_truncated_snapshot_is_refused() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        client(&mut node, 7, &wire::Message { session, client_seq: 1, payload: b"at 3 100" }, 0);
        node.state.outbox.insert(4, b"waiting".to_vec());
        node.state.next_proposal = 4;
        let bytes = encode_state(&node.state);
        assert_eq!(encode_state(&decode_state(&mut &bytes[..]).unwrap()), bytes);
        for cut in 0..bytes.len() {
            assert!(decode_state(&mut &bytes[..cut]).is_err(), "cut at {cut}");
        }
    }

    /// A leader times its commits, and reports its state.
    #[test]
    fn a_leader_times_its_commits_and_reports_its_state() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Message { session, client_seq: 1, payload: b"one" }, &mut frame).unwrap();
        node.on_client(7, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        node.take_outputs();
        let now = node.next_deadline();
        node.on_tick(now);
        node.on_persisted(node.raft().log_end());
        assert_eq!(node.take_outputs().commit_latencies.len(), 1, "the one message");
        let metrics = node.metrics(now + 5 * MS);
        assert_eq!((metrics.role, metrics.sessions, metrics.members), (Role::Leader, 1, 1));
        assert_eq!((metrics.commit, metrics.applied), (node.raft().last_index(), node.raft().last_index()));
        assert_eq!(metrics.snapshot_age_ns, now + 5 * MS, "no snapshot since it started, at 0");
    }

    /// Shut down, a node snapshots, answers, and stops after telling its peers to, for two
    /// heartbeats.
    #[test]
    fn a_shutdown_snapshots_then_stops() {
        let mut node = leader();
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Shutdown { request_id: 7 }, &mut frame).unwrap();
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        let sent = drive(&mut node);
        let index = node.raft().snapshot_index();
        assert_eq!(sent, [format!("99: ShutDown(ShutDownRef {{ request_id: 7, index: {index} }})")]);
        assert!(index > 0 && !node.stopped());
        for _ in 0..3 {
            let deadline = node.next_deadline();
            node.on_tick(deadline);
            drive(&mut node);
        }
        assert!(node.stopped());
    }

    #[test]
    fn an_operator_snapshot_is_answered_once_durable() {
        let mut node = leader();
        let mut frame = Vec::new();
        codec::encode_frame(&wire::SnapshotRequest { request_id: 42 }, &mut frame).unwrap();
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        // The Snapshot entry commits and applies, the snapshot is saved, then the answer.
        let sent = drive(&mut node);
        let index = node.raft().snapshot_index();
        assert!(index > 0 && index == node.applied());
        assert_eq!(
            sent,
            [format!("99: SnapshotTaken(SnapshotTakenRef {{ request_id: 42, index: {index}, quorum_acked: 1 }})")]
        );
    }
}
