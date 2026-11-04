//! What a service sees: the [`Service`] trait it implements, and the [`Ctx`] it acts through.

use super::*;

/// A client session's ID: the log index of the entry that opened it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct SessionId(pub u64);

impl fmt::Display for SessionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

/// The application: a deterministic state machine that every node runs over the same log.
///
/// It must depend only on what it's given, never on a clock, randomness, IO or iteration order
/// of a hash map. Time comes from [`Ctx::now`], which is the leader's clock stamped into the
/// log. It's `Send`, as the driver runs it on a tokio task.
///
/// # Examples
///
/// A service that answers each message with the message, and has no state to snapshot. The
/// [guide](crate::guide::first_service) builds up one with state.
///
/// ```no_run
/// use std::io::{self, Read};
/// use std::process::ExitCode;
///
/// use turbofan_cluster::{Ctx, Service, SessionId, SnapshotWriter};
///
/// struct Echo;
///
/// impl Service for Echo {
///     fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
///         // No longer than what came in, so never too long to send.
///         ctx.send(session, payload).expect("as long as a message in");
///     }
///
///     fn snapshot(&self) -> SnapshotWriter {
///         Box::new(|_out| Ok(()))
///     }
///
///     fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> {
///         Ok(())
///     }
/// }
///
/// fn main() -> ExitCode {
///     turbofan_cluster::run(1, Echo)
/// }
/// ```
pub trait Service: Send {
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

/// What a [`Service`] can do from a callback. Its effects take place after the callback
/// returns, in order.
///
/// # Examples
///
/// A context of its own, [`Ctx::new`], tests a service without a cluster: call a callback with
/// it, then look at what it did.
///
/// ```
/// use turbofan_cluster::{Ctx, Effect, SessionId};
///
/// let session = SessionId(7);
/// let mut ctx = Ctx::new(1_000_000_000);
/// // What a service's `on_message` might do: answer, and wake itself a second later.
/// ctx.send(session, b"ack".to_vec())?;
/// ctx.schedule_timer(1, ctx.now() + 1_000_000_000);
/// assert_eq!(
///     ctx.take_effects(),
///     [Effect::Send(session, b"ack".to_vec()), Effect::Schedule { id: 1, at: 2_000_000_000 }],
/// );
/// # Ok::<(), turbofan_cluster::PayloadTooLong>(())
/// ```
#[derive(Debug, Default)]
pub struct Ctx {
    pub(super) now: u64,
    pub(super) service_version: u32,
    pub(super) effects: Vec<Effect>,
}

/// Something a [`Service`] callback did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Effect {
    /// [`Ctx::send`]: a payload for a session.
    Send(SessionId, Vec<u8>),
    /// [`Ctx::schedule_timer`].
    Schedule {
        /// The timer's ID.
        id: u64,
        /// Its deadline, in cluster time.
        at: u64,
    },
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
    pub fn send(&mut self, session: SessionId, payload: impl Into<Vec<u8>>) -> Result<(), PayloadTooLong> {
        let payload = payload.into();
        PayloadTooLong::check(&payload)?;
        self.effects.push(Effect::Send(session, payload));
        Ok(())
    }

    /// Fires [`Service::on_timer`] with `id` at cluster time `at` or soon after, replacing a
    /// timer with that ID.
    pub fn schedule_timer(&mut self, id: u64, at: u64) {
        self.effects.push(Effect::Schedule { id, at });
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
    pub fn propose(&mut self, payload: impl Into<Vec<u8>>) -> Result<(), PayloadTooLong> {
        let payload = payload.into();
        PayloadTooLong::check(&payload)?;
        self.effects.push(Effect::Propose(payload));
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
