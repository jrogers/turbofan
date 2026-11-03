//! The replicated state, the same on every node at the same applied index, and how a snapshot
//! holds it.

use super::*;

#[derive(Debug, Default)]
pub(super) struct Session {
    /// The last `client_seq` applied: the next must be one more.
    pub(super) last_seq: u64,
    /// The `egress_seq` the next response gets.
    pub(super) next_egress: u64,
    pub(super) ring: VecDeque<(u64, Vec<u8>)>,
    /// Who opened it: only it may reattach.
    pub(super) principal: Vec<u8>,
}

/// Replicated: the same on every node at the same applied index.
#[derive(Debug, Default)]
pub(super) struct State {
    pub(super) sessions: BTreeMap<SessionId, Session>,
    /// Timer ID to its deadline, and the same ordered by deadline.
    pub(super) timers: BTreeMap<u64, u64>,
    pub(super) deadlines: BTreeSet<(u64, u64)>,
    /// The protocol and service versions the cluster runs, as activated.
    pub(super) protocol: u32,
    pub(super) service_version: u32,
    /// What the service proposed, by ID, until it's applied; and the next ID.
    // ponytail: unbounded, like timers; bound it if a service can propose faster than the log
    // commits.
    pub(super) outbox: BTreeMap<u64, Vec<u8>>,
    pub(super) next_proposal: u64,
    pub(super) applied: u64,
    pub(super) time: u64,
    /// The cluster's epoch: 0 until its first leader's `Epoch` entry is applied.
    pub(super) epoch: u64,
}

/// Encodes the replicated state, which a snapshot starts with: SBE messages, each framed as on
/// the wire (`schema/cluster.xml` has their order). The service's state follows, to the end.
pub(super) fn encode_state(state: &State) -> Vec<u8> {
    fn put(out: &mut Vec<u8>, message: &impl turbofan_sbe::Encode) {
        codec::encode_frame(message, out).expect("snapshot messages are within their limits");
    }
    let mut out = Vec::new();
    let node = wire::NodeState {
        applied: state.applied,
        time: state.time,
        next_proposal: state.next_proposal,
        sessions: u32::try_from(state.sessions.len()).expect("at most MAX_SESSIONS"),
        timers: crate::widen(state.timers.len()),
        proposals: crate::widen(state.outbox.len()),
        protocol: state.protocol,
        service_version: state.service_version,
        epoch: state.epoch,
    };
    put(&mut out, &node);
    for (&session, s) in &state.sessions {
        let ring = u32::try_from(s.ring.len()).expect("at most MAX_EGRESS_RING");
        let (last_seq, next_egress) = (s.last_seq, s.next_egress);
        put(&mut out, &wire::SessionState { session: session.0, last_seq, next_egress, ring, principal: &s.principal });
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
pub(super) fn decode_state(from: &mut dyn Read) -> io::Result<State> {
    let frame = read_message(from)?;
    let wire::Decoded::NodeState(n) = decoded(&frame)? else { return Err(bad("no NodeState")) };
    let mut state = State {
        applied: n.applied(),
        time: n.time(),
        next_proposal: n.next_proposal(),
        protocol: n.protocol(),
        service_version: n.service_version(),
        epoch: n.epoch(),
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
        state.sessions.insert(SessionId(s.session()), session);
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
