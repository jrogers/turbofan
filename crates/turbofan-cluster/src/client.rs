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
        /// Why.
        reason: CloseReason,
        /// Sent but not applied.
        unapplied: usize,
    },
}

/// Where to reconnect, after a redirect.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Reconnect {
    /// To the leader, now.
    ToLeader,
    /// There's no leader yet: to the next member, after a pause.
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
            wire::Decoded::Closed(m) if self.session.is_none_or(|s| s == SessionId(m.session())) => {
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

    /// Time passed: a keepalive, if one is due.
    pub fn on_tick(&mut self, now: u64) {
        if now >= self.keepalive_at {
            self.keepalive_at = now + self.keepalive_ns;
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
