//! A gateway's session with the cluster, sans-IO: it finds the leader, keeps a session open
//! across failovers, resends what the cluster hasn't applied, and drops responses it has seen.
//!
//! The caller connects to [`Client::target`], calls [`Client::on_connected`], feeds it every
//! frame with [`Client::on_frame`], writes what [`Client::take_frames`] returns, and reconnects
//! to the new target whenever [`Client::take_reconnect`] says so or the connection drops.
//! Messages are applied exactly once and in order, and responses arrive exactly once and in
//! order, while the session lasts.

use std::collections::VecDeque;
use std::time::Duration;

use crate::codec::{self, MAX_PAYLOAD_BYTES};
use crate::node::SessionId;
use crate::raft::MAX_TIMEOUT;
use crate::wire::{self, CloseReason};

/// The most messages sent but not yet known applied. [`Client::send`] refuses more.
pub const MAX_UNACKED: usize = 4096;

/// Keepalives in a row that the cluster may leave unanswered before the connection is taken for
/// dead: a leader answers each at once, and one that lost power leaves a socket that never errs.
const SILENT_KEEPALIVES: u32 = 3;

/// What the cluster told the client.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The session is open (again, after a failover).
    Opened(SessionId),
    /// A response, exactly once and in order.
    Message(Vec<u8>),
    /// Responses were lost: the client was away longer than the cluster keeps them for.
    Lost {
        /// The first `egress_seq` missed.
        from: u64,
        /// The first one received after the gap.
        to: u64,
    },
    /// The session closed. Messages not yet applied (`unapplied` of them) never will be.
    Closed {
        /// Why. [`CloseReason::Diverged`] is the client's own: it reattached to a cluster that has
        /// less of the session than it saw, messages applied or responses (a standby promoted
        /// without all of it).
        reason: CloseReason,
        /// Sent but not applied: for `Diverged`, not applied by the cluster it reattached to,
        /// which counts what that cluster lost.
        unapplied: usize,
    },
}

/// Where to reconnect, after a redirect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconnect {
    /// To the leader, now.
    ToLeader,
    /// There's no leader yet, or the one connected to went silent: to the next member, after a
    /// pause.
    Later,
}

/// Why [`Client::send`] refused a message.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SendError {
    /// [`MAX_UNACKED`] messages are waiting to be applied: wait for responses.
    Full,
    /// The session closed.
    Closed,
    /// The payload is over [`MAX_PAYLOAD_BYTES`].
    TooLong,
}

impl std::fmt::Display for SendError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Full => "too many messages waiting to be applied",
            Self::Closed => "the session is closed",
            Self::TooLong => "payload over MAX_PAYLOAD_BYTES",
        })
    }
}

impl std::error::Error for SendError {}

/// Why a [`Client`] or [`ClusterClient`](crate::ClusterClient) couldn't be made.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClientConfigError {
    /// No members' addresses were given.
    NoAddresses,
    /// The credentials are over [`MAX_PAYLOAD_BYTES`].
    CredentialsTooLong,
}

impl std::fmt::Display for ClientConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::NoAddresses => "no members' addresses",
            Self::CredentialsTooLong => "credentials over MAX_PAYLOAD_BYTES",
        })
    }
}

impl std::error::Error for ClientConfigError {}

/// One session with the cluster.
#[derive(Debug)]
pub struct Client {
    addrs: Vec<String>,
    /// Sent on every `Connect` and `Reattach`, for the service to check.
    credentials: Vec<u8>,
    target: usize,
    session: Option<SessionId>,
    /// The current connection is attached to the session.
    open: bool,
    closed: bool,
    next_seq: u64,
    unacked: VecDeque<(u64, Vec<u8>)>,
    /// The highest `client_seq` the cluster has said it applied.
    applied: u64,
    last_egress: u64,
    keepalive_ns: u64,
    keepalive_at: u64,
    /// Keepalives due since the cluster last sent anything.
    silent: u32,
    reconnect: Option<Reconnect>,
    frames: Vec<Vec<u8>>,
    events: VecDeque<Event>,
}

impl Client {
    /// A client of the cluster whose members clients reach at `addrs`, with `credentials` for
    /// the service to check (see [`crate::Service::authenticate`]), sending a keepalive every
    /// `keepalive` while idle (well under the cluster's session timeout). A cluster that
    /// refuses them closes the session: [`Event::Closed`] with [`CloseReason::Refused`].
    ///
    /// # Errors
    ///
    /// If `addrs` is empty, or `credentials` are over [`MAX_PAYLOAD_BYTES`].
    pub fn new(
        addrs: impl IntoIterator<Item = impl Into<String>>,
        credentials: impl Into<Vec<u8>>,
        keepalive: Duration,
    ) -> Result<Self, ClientConfigError> {
        let addrs: Vec<String> = addrs.into_iter().map(Into::into).collect();
        let credentials = credentials.into();
        if addrs.is_empty() {
            return Err(ClientConfigError::NoAddresses);
        }
        if credentials.len() > MAX_PAYLOAD_BYTES {
            return Err(ClientConfigError::CredentialsTooLong);
        }
        Ok(Self {
            addrs,
            credentials,
            target: 0,
            session: None,
            open: false,
            closed: false,
            next_seq: 1,
            unacked: VecDeque::new(),
            applied: 0,
            last_egress: 0,
            keepalive_ns: crate::raft::nanos(keepalive.min(MAX_TIMEOUT)),
            keepalive_at: 0,
            silent: 0,
            reconnect: None,
            frames: Vec::new(),
            events: VecDeque::new(),
        })
    }

    /// Where to connect.
    #[must_use]
    pub fn target(&self) -> &str {
        &self.addrs[self.target]
    }

    /// The session, once the cluster has opened it.
    #[must_use]
    pub fn session(&self) -> Option<SessionId> {
        self.session
    }

    /// The session is over: the cluster closed it ([`Event::Closed`]), and nothing more will come.
    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Messages sent but not yet known applied.
    #[must_use]
    pub fn unacked(&self) -> usize {
        self.unacked.len()
    }

    /// A connection to [`Client::target`] is up: open or reattach the session.
    pub fn on_connected(&mut self, now: u64) {
        self.open = false;
        self.reconnect = None;
        self.keepalive_at = now + self.keepalive_ns;
        self.silent = 0;
        match self.session {
            None => push_frame(&mut self.frames, &wire::Connect { credentials: &self.credentials }),
            Some(session) => {
                let reattach = wire::Reattach {
                    session: session.0,
                    last_egress_seq: self.last_egress,
                    credentials: &self.credentials,
                };
                push_frame(&mut self.frames, &reattach);
            }
        }
    }

    /// The connection dropped, or couldn't be made. A session that was open tries the same
    /// member again, since a reset needn't mean it's gone; otherwise, the next member.
    pub fn on_disconnected(&mut self) {
        if !self.open {
            self.target = (self.target + 1) % self.addrs.len();
        }
        self.open = false;
        self.frames.clear();
    }

    /// A frame from the cluster.
    pub fn on_frame(&mut self, message: &wire::Decoded<'_>) {
        self.silent = 0;
        match message {
            wire::Decoded::Redirect(m) => {
                self.open = false;
                self.frames.clear();
                let addr = String::from_utf8_lossy(m.addr());
                (self.target, self.reconnect) = match self.addrs.iter().position(|a| *a == addr) {
                    Some(i) => (i, Some(Reconnect::ToLeader)),
                    None => ((self.target + 1) % self.addrs.len(), Some(Reconnect::Later)),
                };
            }
            // Has less of the session than it saw: a standby promoted without all of it (a leader
            // of the same cluster takes clients only once it has all its predecessors applied).
            // Messages it lost can't be resent (they're let go of once applied), and later ones
            // would wait on them for ever; responses would be dropped as seen. The session's
            // guarantees are over, so it's closed. An older node doesn't say its last response.
            wire::Decoded::Opened(m)
                if m.applied_seq() < self.applied || m.last_egress_seq().is_some_and(|e| e < self.last_egress) =>
            {
                self.frame(&wire::Close { session: m.session() });
                self.open = false;
                self.closed = true;
                self.unacked.clear();
                let unapplied = usize::try_from(self.next_seq - 1 - m.applied_seq()).unwrap_or(usize::MAX);
                self.events.push_back(Event::Closed { reason: CloseReason::Diverged, unapplied });
            }
            wire::Decoded::Opened(m) => {
                self.session = Some(SessionId(m.session()));
                self.open = true;
                self.trim(m.applied_seq());
                self.events.push_back(Event::Opened(SessionId(m.session())));
                let resend: Vec<(u64, Vec<u8>)> = self.unacked.iter().cloned().collect();
                for (client_seq, payload) in resend {
                    self.frame(&wire::Message { session: m.session(), client_seq, payload: &payload });
                }
            }
            wire::Decoded::Egress(m) if Some(SessionId(m.session())) == self.session => {
                self.trim(m.applied_seq());
                if m.egress_seq() <= self.last_egress {
                    return;
                }
                if m.egress_seq() != self.last_egress + 1 {
                    self.events.push_back(Event::Lost { from: self.last_egress + 1, to: m.egress_seq() });
                }
                self.last_egress = m.egress_seq();
                self.events.push_back(Event::Message(m.payload().to_vec()));
            }
            wire::Decoded::KeepAliveAck(m) => self.trim(m.applied_seq()),
            wire::Decoded::Closed(m) if !self.closed && self.session.is_none_or(|s| s == SessionId(m.session())) => {
                self.open = false;
                self.closed = true;
                let unapplied = self.unacked.len();
                self.unacked.clear();
                self.events.push_back(Event::Closed { reason: m.reason(), unapplied });
            }
            _ => {}
        }
    }

    /// Sends `payload`, returning its `client_seq`. It's held, and resent after a failover,
    /// until the cluster has applied it.
    ///
    /// # Errors
    ///
    /// If too many are waiting, the session closed, or the payload is too long.
    pub fn send(&mut self, payload: impl Into<Vec<u8>>) -> Result<u64, SendError> {
        let payload = payload.into();
        if self.closed {
            return Err(SendError::Closed);
        }
        if payload.len() > MAX_PAYLOAD_BYTES {
            return Err(SendError::TooLong);
        }
        if self.unacked.len() >= MAX_UNACKED {
            return Err(SendError::Full);
        }
        let client_seq = self.next_seq;
        self.next_seq += 1;
        if let (true, Some(session)) = (self.open, self.session) {
            self.frame(&wire::Message { session: session.0, client_seq, payload: &payload });
        }
        self.unacked.push_back((client_seq, payload));
        Ok(client_seq)
    }

    /// The highest `client_seq` (as [`Client::send`] returned) the cluster has applied: every
    /// message up to it has been, exactly once. The cluster says so as it applies them, with a
    /// response or without.
    #[must_use]
    pub fn applied(&self) -> u64 {
        self.applied
    }

    /// Closes the session.
    pub fn close(&mut self) {
        if let (true, Some(session)) = (self.open, self.session) {
            self.frame(&wire::Close { session: session.0 });
        }
    }

    /// Time passed: a keepalive, if one is due; or, the cluster silent for
    /// three of them, [`Client::take_reconnect`] says to try the next member.
    pub fn on_tick(&mut self, now: u64) {
        if now >= self.keepalive_at {
            self.keepalive_at = now + self.keepalive_ns;
            self.silent += 1;
            if self.silent > SILENT_KEEPALIVES {
                (self.open, self.silent) = (false, 0);
                self.frames.clear();
                self.target = (self.target + 1) % self.addrs.len();
                self.reconnect = Some(Reconnect::Later);
                return;
            }
            if let (true, Some(session)) = (self.open, self.session) {
                self.frame(&wire::KeepAlive { session: session.0 });
            }
        }
    }

    /// When [`Client::on_tick`] next has something to do.
    #[must_use]
    pub fn next_deadline(&self) -> u64 {
        self.keepalive_at
    }

    /// Frames to write to the connection.
    pub fn take_frames(&mut self) -> Vec<Vec<u8>> {
        std::mem::take(&mut self.frames)
    }

    /// The cluster said to go elsewhere: drop the connection and connect to [`Client::target`],
    /// now or after a pause.
    pub fn take_reconnect(&mut self) -> Option<Reconnect> {
        self.reconnect.take()
    }

    /// The next thing the cluster said.
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    fn trim(&mut self, applied_seq: u64) {
        self.applied = self.applied.max(applied_seq);
        while self.unacked.front().is_some_and(|(seq, _)| *seq <= applied_seq) {
            self.unacked.pop_front();
        }
    }

    fn frame(&mut self, message: &impl turbofan_sbe::Encode) {
        push_frame(&mut self.frames, message);
    }
}

fn push_frame(frames: &mut Vec<Vec<u8>>, message: &impl turbofan_sbe::Encode) {
    let mut frame = Vec::new();
    codec::encode_frame(message, &mut frame).expect("client messages are within their limits");
    frames.push(frame);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(message: &impl turbofan_sbe::Encode) -> Vec<u8> {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).unwrap();
        frame
    }

    fn deliver(client: &mut Client, frame: &[u8]) {
        let (decoded, _) = codec::decode_frame(frame).unwrap().unwrap();
        client.on_frame(&decoded);
    }

    /// A client with session 9 open, four messages sent, three of them applied and two responses
    /// seen, reconnected: then given `opened`, what a standby promoted with some of that says.
    fn reattached_to(opened: &[u8]) -> Client {
        let mut client = Client::new(["a", "b"], b"me".to_vec(), Duration::from_secs(1)).unwrap();
        client.on_connected(0);
        deliver(&mut client, &frame(&wire::Opened { session: 9, applied_seq: 0, last_egress_seq: 0 }));
        for _ in 0..4 {
            client.send(b"order".to_vec()).unwrap();
        }
        for egress_seq in 1..=2 {
            deliver(&mut client, &frame(&wire::Egress { session: 9, egress_seq, applied_seq: 3, payload: b"fill" }));
        }
        client.on_disconnected();
        client.on_connected(1);
        while client.poll_event().is_some() {}
        client.take_frames();
        deliver(&mut client, opened);
        client
    }

    /// A client with session 9 open on its first connection, and what it wrote taken.
    fn open() -> Client {
        let mut client = Client::new(["a", "b"], b"me".to_vec(), Duration::from_secs(1)).unwrap();
        client.on_connected(0);
        deliver(&mut client, &frame(&wire::Opened { session: 9, applied_seq: 0, last_egress_seq: 0 }));
        while client.poll_event().is_some() {}
        client.take_frames();
        client
    }

    fn decoded(frames: &[Vec<u8>]) -> Vec<String> {
        frames.iter().map(|f| format!("{:?}", codec::decode_frame(f).unwrap().unwrap().0)).collect()
    }

    /// A keepalive's ack says what's applied, as a response does: a service that doesn't answer
    /// still lets the client let go of what it sent, and `applied` advance.
    #[test]
    fn a_keepalive_ack_lets_go_of_what_is_applied() {
        let mut client = open();
        for _ in 0..3 {
            client.send(b"quiet".to_vec()).unwrap();
        }
        deliver(&mut client, &frame(&wire::KeepAliveAck { applied_seq: 2 }));
        assert_eq!((client.applied(), client.unacked()), (2, 1));
        deliver(&mut client, &frame(&wire::KeepAliveAck { applied_seq: 1 }));
        assert_eq!((client.applied(), client.unacked()), (2, 1), "never back");
    }

    /// `close` says so to the cluster, once a session is open on the connection.
    #[test]
    fn close_closes_an_open_session() {
        let mut client = open();
        client.close();
        assert_eq!(decoded(&client.take_frames()), ["Close(CloseRef { session: 9 })"]);
        client.on_disconnected();
        client.close();
        assert!(client.take_frames().is_empty(), "no connection with the session open");
    }

    /// An idle session sends a keepalive once a `keepalive` (1 s) has passed, and is next due a
    /// `keepalive` after.
    #[test]
    fn an_idle_session_keeps_alive() {
        let mut client = open();
        assert_eq!(client.session(), Some(SessionId(9)));
        let second = 1_000_000_000;
        assert_eq!(client.next_deadline(), second);
        client.on_tick(second - 1);
        assert!(client.take_frames().is_empty(), "not yet");
        client.on_tick(second);
        assert_eq!(decoded(&client.take_frames()), ["KeepAlive(KeepAliveRef { session: 9 })"]);
        assert_eq!(client.next_deadline(), 2 * second);
    }

    /// A cluster that answers nothing for three keepalives is left for the next member; one that
    /// answers anything is kept.
    #[test]
    fn a_silent_cluster_is_left() {
        let second = 1_000_000_000;
        let mut client = open();
        for tick in 1..=3 {
            client.on_tick(tick * second);
            assert_eq!(client.take_reconnect(), None);
        }
        deliver(&mut client, &frame(&wire::KeepAliveAck { applied_seq: 0 }));
        for tick in 4..=6 {
            client.on_tick(tick * second);
            assert_eq!(client.take_reconnect(), None, "it answered");
        }
        client.take_frames();
        client.on_tick(7 * second);
        assert_eq!(client.take_reconnect(), Some(Reconnect::Later));
        assert_eq!(client.target(), "b");
        assert!(client.take_frames().is_empty(), "no keepalive into the silence");
    }

    /// A dropped connection is tried again at the same member if the session was open there,
    /// since a reset needn't mean it's gone; one that never opened tries the next, round.
    #[test]
    fn a_dropped_connection_tries_the_same_member_or_the_next() {
        let mut client = open();
        client.on_disconnected();
        assert_eq!(client.target(), "a", "the session was open there");
        client.on_disconnected();
        assert_eq!(client.target(), "b");
        client.on_disconnected();
        assert_eq!(client.target(), "a", "round again");
    }

    /// A gap in the responses is reported as the ones missed, then the next delivered.
    #[test]
    fn a_gap_in_the_responses_is_reported() {
        let mut client = open();
        let egress = |egress_seq| frame(&wire::Egress { session: 9, egress_seq, applied_seq: 0, payload: b"r" });
        deliver(&mut client, &egress(1));
        deliver(&mut client, &egress(4));
        let events: Vec<Event> = std::iter::from_fn(|| client.poll_event()).collect();
        let message = Event::Message(b"r".to_vec());
        assert_eq!(events, [message.clone(), Event::Lost { from: 2, to: 4 }, message]);
    }

    /// What's for another session is ignored: its responses, and its closing.
    #[test]
    fn another_sessions_frames_are_ignored() {
        let mut client = open();
        deliver(&mut client, &frame(&wire::Egress { session: 8, egress_seq: 1, applied_seq: 0, payload: b"r" }));
        deliver(&mut client, &frame(&wire::Closed { session: 8, reason: CloseReason::Client }));
        assert_eq!(client.poll_event(), None);
        assert!(client.send(b"order".to_vec()).is_ok(), "still open");
    }

    /// A payload of the most a payload can be is sent; one byte more is refused.
    #[test]
    fn a_payload_of_the_limit_is_sent() {
        let mut client = open();
        assert!(client.send(vec![0; MAX_PAYLOAD_BYTES]).is_ok());
        assert_eq!(client.send(vec![0; MAX_PAYLOAD_BYTES + 1]), Err(SendError::TooLong));
    }

    fn opened(applied_seq: u64, last_egress_seq: u64) -> Vec<u8> {
        frame(&wire::Opened { session: 9, applied_seq, last_egress_seq })
    }

    /// A cluster with everything the client saw carries the session on, and is sent what it
    /// hasn't applied.
    #[test]
    fn a_cluster_with_all_of_the_session_carries_it_on() {
        let mut client = reattached_to(&opened(3, 2));
        assert_eq!(client.poll_event(), Some(Event::Opened(SessionId(9))));
        assert_eq!(client.take_frames().len(), 1, "the fourth, resent");
        assert!(client.send(b"order".to_vec()).is_ok());
    }

    /// One that has applied fewer of its messages (a standby promoted without all of it) closes
    /// the session: the client has let go of what it lost, and couldn't resend it.
    #[test]
    fn a_cluster_missing_messages_closes_the_session() {
        let mut client = reattached_to(&opened(1, 2));
        assert_eq!(client.poll_event(), Some(Event::Closed { reason: CloseReason::Diverged, unapplied: 3 }));
        let frames = client.take_frames();
        assert_eq!(frames.len(), 1);
        let (close, _) = codec::decode_frame(&frames[0]).unwrap().unwrap();
        assert!(matches!(close, wire::Decoded::Close(c) if c.session() == 9), "only a close");
        assert_eq!(client.send(b"order".to_vec()), Err(SendError::Closed));
        // The cluster closing it in turn says nothing new.
        deliver(&mut client, &frame(&wire::Closed { session: 9, reason: CloseReason::Client }));
        assert_eq!(client.poll_event(), None);
    }

    /// So does one missing responses the client saw: its next ones would reuse their
    /// `egress_seq`, and be dropped as seen.
    #[test]
    fn a_cluster_missing_responses_closes_the_session() {
        let mut client = reattached_to(&opened(3, 1));
        assert_eq!(client.poll_event(), Some(Event::Closed { reason: CloseReason::Diverged, unapplied: 1 }));
    }

    /// An older node's `Opened` (version 3) doesn't say the last response: only what's applied
    /// is checked.
    #[test]
    fn an_older_nodes_opened_is_checked_for_messages_only() {
        let mut older = opened(3, 1);
        // Without `lastEgressSeq`, the block's last 8 bytes, and saying version 3, as an older
        // node writes it.
        older.truncate(older.len() - 8);
        let len = u32::try_from(older.len() - 4).unwrap();
        older[..4].copy_from_slice(&len.to_le_bytes());
        older[4..6].copy_from_slice(&16_u16.to_le_bytes());
        older[10..12].copy_from_slice(&3_u16.to_le_bytes());
        let mut client = reattached_to(&older);
        assert_eq!(client.poll_event(), Some(Event::Opened(SessionId(9))), "a lost response it can't tell");
    }
}
