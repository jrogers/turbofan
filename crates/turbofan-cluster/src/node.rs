//! A cluster member: [`Raft`] plus what the log means. Client sessions, de-duplication, each
//! session's egress ring and the service's timers, applied to a [`Service`].
//!
//! Like [`Raft`], it does no IO and reads no clock. The driver passes the time with every input
//! and carries out [`Node::take_outputs`] with Raft's rule: persist, then
//! [`Node::on_persisted`], then send. Applying what that made durable can produce more to
//! persist (a snapshot), so the driver repeats take, persist and `on_persisted` until a batch
//! has nothing to persist, and only then sends what all the batches produced.
//!
//! **Replicated state** (`State`) changes only when a committed entry is applied, so it is the same on
//! every node: sessions, their last applied `client_seq`, their egress rings, and timers. **Leader
//! state** (`Local`) is this node's view as leader: connections, session liveness, and the opens,
//! closes and timer firings it has proposed but not yet seen applied. It is rebuilt when a node
//! becomes leader.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use crate::codec::{self, MAX_PAYLOAD_BYTES};
use crate::meta::Meta;
use crate::raft::{self, NodeId, Persist, Raft, Role, Snapshot};
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

/// The application: a deterministic state machine that every node runs over the same log.
///
/// It must depend only on what it's given, never on a clock, randomness, IO or iteration order
/// of a hash map. Time comes from [`Ctx::now`], which is the leader's clock stamped into the
/// log.
pub trait Service {
    /// A session opened.
    fn on_session_open(&mut self, _ctx: &mut Ctx, _session: SessionId) {}
    /// A session closed. Sends to it are dropped from here on.
    fn on_session_close(&mut self, _ctx: &mut Ctx, _session: SessionId, _reason: CloseReason) {}
    /// A message from a session, each exactly once and in the order the client sent them.
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]);
    /// A timer from [`Ctx::schedule_timer`] fired.
    fn on_timer(&mut self, _ctx: &mut Ctx, _id: u64) {}
    /// Appends its whole state to `out`. The same state must give the same bytes, on every node.
    fn snapshot(&self, out: &mut Vec<u8>);
    /// Replaces its whole state with what [`Service::snapshot`] wrote.
    fn restore(&mut self, bytes: &[u8]);
}

/// What a [`Service`] can do from a callback. Its effects take place after the callback
/// returns, in order.
#[derive(Debug, Default)]
pub struct Ctx {
    now: u64,
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
}

impl Ctx {
    /// A context at cluster time `now`, for testing a service without a cluster: call its
    /// callbacks with it, then look at [`Ctx::take_effects`].
    #[must_use]
    pub fn new(now: u64) -> Self {
        Self { now, effects: Vec::new() }
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

    /// Sends `payload` to `session`. Delivered exactly once, in order, if the session is open.
    ///
    /// # Panics
    ///
    /// If `payload` is over [`MAX_PAYLOAD_BYTES`].
    pub fn send(&mut self, session: SessionId, payload: &[u8]) {
        assert!(payload.len() <= MAX_PAYLOAD_BYTES, "payload of {} bytes is over MAX_PAYLOAD_BYTES", payload.len());
        self.effects.push(Effect::Send(session, payload.to_vec()));
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
}

/// A node's configuration.
#[derive(Debug, Clone)]
pub struct Config {
    /// Identity and Raft timing.
    pub raft: raft::Config,
    /// Where clients reach each member, for redirecting them to the leader.
    pub client_addrs: Vec<(NodeId, String)>,
    /// Where operators reach each member's admin listener, for redirecting them to the leader.
    pub admin_addrs: Vec<(NodeId, String)>,
    /// A session the leader hears nothing from for this long is closed.
    pub session_timeout_ns: u64,
    /// Each node snapshots, and compacts its log, once this many entries follow its last snapshot.
    pub snapshot_every_entries: u64,
    /// How many responses each session keeps for a client that reattaches after a failover. A
    /// client away while more than this are sent misses some ([`crate::client::Event::Lost`]),
    /// so it must cover a gateway's requests in flight times the responses each can cause.
    /// Every node holds this many per session, and snapshots them.
    pub egress_ring_entries: usize,
}

/// What one batch of inputs produced. Carry out in this order: `persist`, then
/// [`Node::on_persisted`]; take outputs again until `persist` is empty; then all the sends.
#[derive(Debug, Default)]
pub struct Outputs {
    /// Persist these first, in order.
    pub persist: Vec<Persist>,
    /// Frames for peers.
    pub peer_sends: Vec<(NodeId, Vec<u8>)>,
    /// Frames for clients.
    pub client_sends: Vec<(ConnId, Vec<u8>)>,
}

#[derive(Debug, Default)]
struct Session {
    /// The last `client_seq` applied: the next must be one more.
    last_seq: u64,
    /// The `egress_seq` the next response gets.
    next_egress: u64,
    ring: VecDeque<(u64, Vec<u8>)>,
}

/// Replicated: the same on every node at the same applied index.
#[derive(Debug, Default)]
struct State {
    sessions: BTreeMap<SessionId, Session>,
    /// Timer ID to its deadline, and the same ordered by deadline.
    timers: BTreeMap<u64, u64>,
    deadlines: BTreeSet<(u64, u64)>,
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
    firing: BTreeSet<u64>,
    /// Manual snapshots asked for: request ID to the admin connection, before and after the
    /// `Snapshot` entry is applied (then with its index).
    requested: BTreeMap<u64, ConnId>,
    awaiting: BTreeMap<u64, (ConnId, u64)>,
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
}

impl<S: Service> Node<S> {
    /// A member starting from its durable `meta`, latest `snapshot` and log `records`, with
    /// `service` new, to be restored from the snapshot.
    ///
    /// # Panics
    ///
    /// If the snapshot doesn't decode: its file's checksum passed, so it was written wrong.
    #[must_use]
    pub fn new(config: Config, service: S, meta: Meta, snapshot: Option<Snapshot>, records: &[u8], now: u64) -> Self {
        assert!(config.session_timeout_ns > config.raft.election_timeout_ns * 2, "sessions outlive an election");
        let addrs = config.client_addrs.iter().chain(&config.admin_addrs);
        assert!(addrs.into_iter().all(|(_, a)| a.len() <= usize::from(u8::MAX)), "addresses fit Redirect");
        assert!(config.snapshot_every_entries > 0);
        assert!((1..=MAX_EGRESS_RING).contains(&config.egress_ring_entries));
        let raft = Raft::new(config.raft.clone(), meta, snapshot, records, now);
        let mut node = Self {
            raft,
            service,
            config,
            state: State::default(),
            local: Local::default(),
            ctx: Ctx::default(),
            now,
            scratch: Vec::new(),
        };
        if node.raft.snapshot().is_some() {
            node.restore();
        }
        node
    }

    /// Asks for a snapshot at the next index, on every node: what an operator's
    /// `SnapshotRequest` does. Replies `SnapshotTaken` on `conn` once a quorum has it durable.
    pub fn on_admin(&mut self, conn: ConnId, message: &wire::Decoded<'_>, now: u64) {
        self.after(now);
        let wire::Decoded::SnapshotRequest(m) = message else { return };
        if !self.is_ready() {
            self.redirect_to(conn, &self.config.admin_addrs.clone());
            return;
        }
        if self.local.requested.contains_key(&m.request_id()) || self.local.awaiting.contains_key(&m.request_id()) {
            return;
        }
        self.local.requested.insert(m.request_id(), conn);
        self.propose(&wire::Snapshot { timestamp: now, request_id: m.request_id() });
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

    /// Everything persisted so far is durable, up to log index `index`.
    pub fn on_persisted(&mut self, index: u64) {
        self.raft.on_persisted(index);
        self.apply();
        self.answer_snapshot_requests();
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
            wire::Decoded::Connect(_) => self.open(conn),
            wire::Decoded::Reattach(m) => self.reattach(conn, m.session(), m.last_egress_seq()),
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
            out.sends.extend(more.sends);
        }
        Outputs { persist: out.persist, peer_sends: out.sends, client_sends: std::mem::take(&mut self.local.sends) }
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
            self.restore();
        }
        self.apply();
        if self.local.was_leader && self.raft.role() != Role::Leader {
            self.step_down();
        }
        self.answer_snapshot_requests();
    }

    /// Replaces the replicated state, and the service's, with Raft's snapshot.
    fn restore(&mut self) {
        let snapshot = self.raft.snapshot().expect("there is a snapshot to restore");
        let (state, service) = decode_state(&snapshot.bytes).expect("snapshots are written by nodes");
        assert_eq!(state.applied, snapshot.index, "a snapshot is of the state at its index");
        self.service.restore(service);
        self.state = state;
    }

    /// Snapshots the state as applied so far, and compacts the log to it.
    fn take_snapshot(&mut self) {
        let index = self.state.applied;
        let mut service = Vec::new();
        self.service.snapshot(&mut service);
        let bytes = encode_state(&self.state, &service);
        let term = self.raft.term_at(index).expect("the applied entry is in the log");
        self.raft.compact(Snapshot { index, term, bytes });
    }

    /// Answers each manual snapshot a quorum, this leader among it, now has durable.
    fn answer_snapshot_requests(&mut self) {
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

    /// Clients attached here go and find the new leader.
    fn step_down(&mut self) {
        let conns: Vec<ConnId> = self.local.conns.keys().copied().collect();
        let sends = std::mem::take(&mut self.local.sends);
        self.local = Local { sends, ..Local::default() };
        for conn in conns {
            self.redirect(conn);
        }
    }

    fn open(&mut self, conn: ConnId) {
        if self.state.sessions.len() + self.local.opening.len() >= MAX_SESSIONS {
            self.send_client(conn, &wire::Closed { session: 0, reason: CloseReason::NoSession });
            return;
        }
        if let Some(index) = self.propose(&wire::SessionOpen { timestamp: self.now }) {
            self.local.opening.insert(index, conn);
        }
    }

    fn reattach(&mut self, conn: ConnId, session: SessionId, last_egress_seq: u64) {
        let Some(s) = self.state.sessions.get(&session) else {
            self.send_client(conn, &wire::Closed { session, reason: CloseReason::NoSession });
            return;
        };
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
        // ponytail: no backpressure beyond TCP's; bound proposals in flight per session if a
        // client can outrun the quorum.
        self.propose(&wire::SessionMessage { timestamp: self.now, session, client_seq, payload });
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

    /// Applies committed entries, in order.
    fn apply(&mut self) {
        while self.state.applied < self.raft.commit_index() {
            let index = self.state.applied + 1;
            let ours = self.raft.role() == Role::Leader && self.raft.term_at(index) == Some(self.raft.term());
            let payload = self.raft.entry(index).expect("committed entries are in the log").to_vec();
            let (entry, _) = wire::decode(&payload).expect("the leader logged a valid entry");
            self.state.applied = index;
            self.apply_entry(index, &entry, ours);
            let wanted = matches!(entry, wire::Decoded::Snapshot(_));
            if wanted || index >= self.raft.snapshot_index() + self.config.snapshot_every_entries {
                self.take_snapshot();
            }
        }
    }

    fn apply_entry(&mut self, index: u64, entry: &wire::Decoded<'_>, ours: bool) {
        match entry {
            wire::Decoded::NewTerm(e) => {
                self.advance_time(e.timestamp());
                self.local.ready |= ours;
            }
            wire::Decoded::Snapshot(e) => {
                self.advance_time(e.timestamp());
                if let Some(conn) = self.local.requested.remove(&e.request_id()) {
                    self.local.awaiting.insert(e.request_id(), (conn, index));
                }
            }
            wire::Decoded::SessionOpen(e) => {
                self.advance_time(e.timestamp());
                self.apply_open(index);
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
            _ => unreachable!("only log entries are logged"),
        }
    }

    fn apply_open(&mut self, session: SessionId) {
        self.state.sessions.insert(session, Session { last_seq: 0, next_egress: 1, ring: VecDeque::new() });
        if let Some(conn) = self.local.opening.remove(&session) {
            self.local.conns.insert(conn, Some(session));
            self.local.attached.insert(session, conn);
            self.send_client(conn, &wire::Opened { session, applied_seq: 0 });
        }
        if self.local.was_leader {
            self.local.activity.insert(session, self.now);
        }
        self.call(|service, ctx| service.on_session_open(ctx, session));
    }

    fn apply_close(&mut self, session: SessionId, reason: CloseReason) {
        self.local.closing.remove(&session);
        self.local.activity.remove(&session);
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
            }
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
        self.redirect_to(conn, &self.config.client_addrs.clone());
    }

    /// Points `conn` at the leader's address in `addrs`, or nowhere if there's no leader yet.
    fn redirect_to(&mut self, conn: ConnId, addrs: &[(NodeId, String)]) {
        let leader = self.raft.leader().filter(|&l| l != self.raft.id());
        let addr = leader.and_then(|l| addrs.iter().find(|(id, _)| *id == l)).map(|(_, a)| a);
        let addr = addr.map_or(&[][..], |a| a.as_bytes());
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

/// Encodes the replicated state and the service's snapshot: little-endian, counts before lists.
fn encode_state(state: &State, service: &[u8]) -> Vec<u8> {
    let mut out = Vec::new();
    let put = |out: &mut Vec<u8>, v: u64| out.extend_from_slice(&v.to_le_bytes());
    put(&mut out, state.applied);
    put(&mut out, state.time);
    put(&mut out, state.sessions.len() as u64);
    for (&id, session) in &state.sessions {
        for v in [id, session.last_seq, session.next_egress, session.ring.len() as u64] {
            put(&mut out, v);
        }
        for (seq, payload) in &session.ring {
            put(&mut out, *seq);
            put(&mut out, payload.len() as u64);
            out.extend_from_slice(payload);
        }
    }
    put(&mut out, state.timers.len() as u64);
    for (&id, &at) in &state.timers {
        put(&mut out, id);
        put(&mut out, at);
    }
    put(&mut out, service.len() as u64);
    out.extend_from_slice(service);
    out
}

/// The state and the service's bytes, or `None` if `bytes` aren't a whole, sane encoding.
fn decode_state(bytes: &[u8]) -> Option<(State, &[u8])> {
    let mut r = Reader(bytes);
    let mut state = State { applied: r.u64()?, time: r.u64()?, ..State::default() };
    for _ in 0..r.count(MAX_SESSIONS)? {
        let id = r.u64()?;
        let mut session = Session { last_seq: r.u64()?, next_egress: r.u64()?, ring: VecDeque::new() };
        for _ in 0..r.count(MAX_EGRESS_RING)? {
            let seq = r.u64()?;
            let len = r.count(MAX_PAYLOAD_BYTES)?;
            session.ring.push_back((seq, r.take(len)?.to_vec()));
        }
        state.sessions.insert(id, session);
    }
    for _ in 0..r.count(usize::MAX)? {
        let (id, at) = (r.u64()?, r.u64()?);
        state.timers.insert(id, at);
        state.deadlines.insert((at, id));
    }
    let len = r.count(usize::MAX)?;
    let service = r.take(len)?;
    r.0.is_empty().then_some((state, service))
}

/// Reads a snapshot, every read checked against what's left.
struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn take(&mut self, len: usize) -> Option<&'a [u8]> {
        let (head, rest) = self.0.split_at_checked(len)?;
        self.0 = rest;
        Some(head)
    }

    fn u64(&mut self) -> Option<u64> {
        Some(u64::from_le_bytes(self.take(8)?.try_into().ok()?))
    }

    /// A count or length, at most `max` and never more than the bytes left could hold.
    fn count(&mut self, max: usize) -> Option<usize> {
        let n = usize::try_from(self.u64()?).ok()?;
        (n <= max && n <= self.0.len()).then_some(n)
    }
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
        fn on_session_open(&mut self, _ctx: &mut Ctx, session: SessionId) {
            self.0.push(format!("open {session}"));
        }
        fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, reason: CloseReason) {
            self.0.push(format!("close {session} {reason:?}"));
        }
        fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
            let text = std::str::from_utf8(payload).unwrap();
            self.0.push(format!("message {session} {text}"));
            match text.split(' ').collect::<Vec<_>>()[..] {
                ["at", id, ms] => ctx.schedule_timer(id.parse().unwrap(), ms.parse::<u64>().unwrap() * MS),
                ["cancel", id] => ctx.cancel_timer(id.parse().unwrap()),
                _ => {}
            }
            ctx.send(session, payload);
        }
        fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
            self.0.push(format!("timer {id} at {}", ctx.now() / MS));
        }
        fn snapshot(&self, out: &mut Vec<u8>) {
            out.extend_from_slice(self.0.join("\n").as_bytes());
        }
        fn restore(&mut self, bytes: &[u8]) {
            let text = std::str::from_utf8(bytes).unwrap();
            self.0 = if text.is_empty() { Vec::new() } else { text.split('\n').map(String::from).collect() };
        }
    }

    /// A lone member, elected and ready.
    fn leader() -> Node<Recorder> {
        let config = Config {
            raft: raft::Config {
                id: 1,
                members: vec![1],
                election_timeout_ns: 150 * MS,
                heartbeat_ns: 50 * MS,
                seed: 1,
            },
            client_addrs: vec![(1, "n1".into())],
            admin_addrs: vec![(1, "a1".into())],
            session_timeout_ns: TIMEOUT,
            snapshot_every_entries: 1000,
            egress_ring_entries: 1024,
        };
        let mut node = Node::new(config, Recorder::default(), Meta::default(), None, &[], 0);
        let deadline = node.next_deadline();
        node.on_tick(deadline);
        drive(&mut node);
        assert!(node.is_ready());
        node
    }

    /// Carries out the outputs as a driver would, returning the client frames, decoded to text.
    fn drive(node: &mut Node<Recorder>) -> Vec<String> {
        let mut out = node.take_outputs();
        node.on_persisted(node.raft().last_index());
        out.client_sends.extend(node.take_outputs().client_sends);
        out.client_sends
            .iter()
            .map(|(conn, f)| format!("{conn}: {:?}", codec::decode_frame(f).unwrap().unwrap().0))
            .collect()
    }

    fn client(node: &mut Node<Recorder>, conn: ConnId, message: &impl turbofan_sbe::Encode, now: u64) -> Vec<String> {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).unwrap();
        node.on_client(conn, &codec::decode_frame(&frame).unwrap().unwrap().0, now);
        drive(node)
    }

    fn open(node: &mut Node<Recorder>, conn: ConnId, now: u64) -> SessionId {
        let sent = client(node, conn, &wire::Connect {}, now);
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
        let sent = client(&mut node, 8, &wire::Reattach { session, last_egress_seq: 1 }, 0);
        assert_eq!(sent.len(), 2, "{sent:?}");
        assert!(sent[0].contains("Opened") && sent[0].contains("applied_seq: 2"));
        assert!(sent[1].contains("egress_seq: 2"));
        // The old connection is no longer the session's.
        assert!(client(&mut node, 7, &message(3, b"three"), 0)[0].contains("NoSession"));
    }

    /// A client that reconnects while its message is uncommitted resends it, and the log holds
    /// it twice: it applies once.
    #[test]
    fn a_message_logged_twice_applies_once() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        let message = wire::Message { session, client_seq: 1, payload: b"one" };
        let mut frame = Vec::new();
        codec::encode_frame(&message, &mut frame).unwrap();
        node.on_client(7, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        node.on_client(7, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        let sent = drive(&mut node);
        assert_eq!(sent.iter().filter(|s| s.contains("Egress")).count(), 1, "{sent:?}");
        assert_eq!(node.service.0.iter().filter(|e| e.starts_with("message")).count(), 1);
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
                members: vec![1, 2, 3],
                election_timeout_ns: 150 * MS,
                heartbeat_ns: 50 * MS,
                seed: 1,
            },
            client_addrs: vec![(1, "n1".into()), (2, "n2".into()), (3, "n3".into())],
            admin_addrs: vec![],
            session_timeout_ns: TIMEOUT,
            snapshot_every_entries: 1000,
            egress_ring_entries: 1024,
        };
        let mut node = Node::new(config, Recorder::default(), Meta::default(), None, &[], 0);
        let mut frame = Vec::new();
        let append =
            wire::AppendEntries { term: 1, leader: 1, prev_index: 0, prev_term: 0, commit: 0, count: 0, entries: &[] };
        codec::encode_frame(&append, &mut frame).unwrap();
        node.on_peer(1, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        drive(&mut node);
        let sent = client(&mut node, 1, &wire::Connect {}, 0);
        assert!(sent[0].contains("leader: 1, addr: [110, 49]"), "{sent:?}");
    }

    /// A node restarted from a snapshot has the same sessions, rings, timers and service.
    #[test]
    fn a_snapshot_restores_the_whole_state() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        for (client_seq, text) in [(1, "one"), (2, "at 9 500")] {
            client(&mut node, 7, &wire::Message { session, client_seq, payload: text.as_bytes() }, 0);
        }
        node.take_snapshot();
        let snapshot = node.raft().snapshot().unwrap().clone();
        assert_eq!(snapshot.index, node.applied());

        let config = node.config.clone();
        let restored = Node::new(config, Recorder::default(), Meta::default(), Some(snapshot), &[], 0);
        let encode = |n: &Node<Recorder>| {
            let mut service = Vec::new();
            n.service.snapshot(&mut service);
            encode_state(&n.state, &service)
        };
        assert_eq!(encode(&restored), encode(&node));
        assert_eq!(restored.state.sessions[&session].ring.len(), 2);
        assert_eq!(restored.state.timers.get(&9), Some(&(500 * MS)));
        assert_eq!(restored.service.0, node.service.0);
    }

    /// A snapshot cut short anywhere is refused, never misread or a panic.
    #[test]
    fn a_truncated_snapshot_is_refused() {
        let mut node = leader();
        let session = open(&mut node, 7, 0);
        client(&mut node, 7, &wire::Message { session, client_seq: 1, payload: b"at 3 100" }, 0);
        let bytes = encode_state(&node.state, b"service");
        assert!(decode_state(&bytes).is_some());
        for cut in 0..bytes.len() {
            assert!(decode_state(&bytes[..cut]).is_none(), "cut at {cut}");
        }
    }

    #[test]
    fn an_operator_snapshot_is_answered_once_durable() {
        let mut node = leader();
        let mut frame = Vec::new();
        codec::encode_frame(&wire::SnapshotRequest { request_id: 42 }, &mut frame).unwrap();
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        // The Snapshot entry commits and applies, the snapshot is persisted, then the answer.
        let mut sent = drive(&mut node);
        sent.extend(drive(&mut node));
        let index = node.raft().snapshot_index();
        assert!(index > 0 && index == node.applied());
        assert_eq!(
            sent,
            [format!("99: SnapshotTaken(SnapshotTakenRef {{ request_id: 42, index: {index}, quorum_acked: 1 }})")]
        );
    }
}
