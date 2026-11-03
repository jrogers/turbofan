//! Client sessions as the leader keeps them: opening, reattaching, messages, closing, expiry, and
//! what it sends clients.

use super::*;

impl<S: Service> Node<S> {
    pub(super) fn open(&mut self, conn: ConnId, credentials: &[u8]) {
        if self.state.sessions.len() + self.local.opening.len() >= MAX_SESSIONS {
            self.send_client(conn, &wire::Closed { session: 0, reason: CloseReason::NoSession });
            return;
        }
        let Some(principal) = self.authenticate(credentials) else {
            self.send_client(conn, &wire::Closed { session: 0, reason: CloseReason::Refused });
            return;
        };
        if let Some(index) = self.propose(&wire::SessionOpen { timestamp: self.now, principal: &principal }) {
            self.local.opening.insert(SessionId(index), conn);
        }
    }

    /// The principal the service gives `credentials`, if it accepts them and it's short enough.
    pub(super) fn authenticate(&self, credentials: &[u8]) -> Option<Vec<u8>> {
        self.service.authenticate(credentials).filter(|p| p.len() <= MAX_PRINCIPAL_BYTES)
    }

    pub(super) fn reattach(&mut self, conn: ConnId, session: SessionId, last_egress_seq: u64, credentials: &[u8]) {
        let Some(s) = self.state.sessions.get(&session) else {
            self.send_client(conn, &wire::Closed { session: session.0, reason: CloseReason::NoSession });
            return;
        };
        // A session's ID is easy to guess: its owner proves it's still who opened it.
        if self.authenticate(credentials).as_ref() != Some(&s.principal) {
            self.send_client(conn, &wire::Closed { session: session.0, reason: CloseReason::Refused });
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
        self.send_client(conn, &wire::Opened { session: session.0, applied_seq });
        for (egress_seq, payload) in replay {
            self.send_client(conn, &wire::Egress { session: session.0, egress_seq, applied_seq, payload: &payload });
        }
    }

    /// The session `conn` is attached to, if it's `session`; otherwise tells the client.
    pub(super) fn check_attached(&mut self, conn: ConnId, session: SessionId) -> Option<SessionId> {
        if self.local.conns.get(&conn) == Some(&Some(session)) && self.state.sessions.contains_key(&session) {
            return Some(session);
        }
        self.send_client(conn, &wire::Closed { session: session.0, reason: CloseReason::NoSession });
        None
    }

    pub(super) fn submit(&mut self, conn: ConnId, session: SessionId, client_seq: u64, payload: &[u8]) {
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
        if client_seq != next || client_seq - last_seq > crate::widen(MAX_UNACKED) {
            return;
        }
        if self
            .propose(&wire::SessionMessage { timestamp: self.now, session: session.0, client_seq, payload })
            .is_some()
        {
            self.local.proposed.insert(session, client_seq);
        }
    }

    pub(super) fn propose_close(&mut self, session: SessionId, reason: CloseReason) {
        if self.local.closing.insert(session) {
            self.propose(&wire::SessionClose { timestamp: self.now, session: session.0, reason });
        }
    }

    pub(super) fn expire_sessions(&mut self, now: u64) {
        let timeout = raft::nanos(self.config.session_timeout);
        let expired: Vec<SessionId> =
            self.local.activity.iter().filter(|&(_, &at)| at + timeout <= now).map(|(&s, _)| s).collect();
        for session in expired {
            self.propose_close(session, CloseReason::Timeout);
        }
    }

    pub(super) fn egress(&mut self, session: SessionId, payload: Vec<u8>) {
        let Some(s) = self.state.sessions.get_mut(&session) else { return };
        let egress_seq = s.next_egress;
        s.next_egress += 1;
        self.local.unacked.remove(&session);
        if let Some(&conn) = self.local.attached.get(&session) {
            let message = wire::Egress { session: session.0, egress_seq, applied_seq: s.last_seq, payload: &payload };
            let mut frame = Vec::new();
            codec::encode_frame(&message, &mut frame).expect("payloads are within their limit");
            self.local.sends.push((conn, frame));
        }
        if s.ring.len() == self.config.egress_ring_entries {
            s.ring.pop_front();
        }
        s.ring.push_back((egress_seq, payload));
    }

    pub(super) fn redirect(&mut self, conn: ConnId) {
        self.redirect_to(conn, |m| &m.client);
    }

    /// Points `conn` at the leader's address, the one `addr` picks, or nowhere if there's no
    /// leader yet, or it isn't a member this node knows.
    pub(super) fn redirect_to(&mut self, conn: ConnId, addr: impl Fn(&Member) -> &String) {
        let leader = self.raft.leader().filter(|&l| l != self.raft.id());
        let member = leader.and_then(|l| self.raft.members().iter().find(|m| m.id == l));
        let addr = member.map_or(&[][..], |m| addr(m).as_bytes());
        let message = wire::Redirect { leader: NodeId::to_wire(leader), addr };
        let mut frame = Vec::new();
        codec::encode_frame(&message, &mut frame).expect("addresses are short");
        self.local.sends.push((conn, frame));
    }

    pub(super) fn send_client(&mut self, conn: ConnId, message: &impl turbofan_sbe::Encode) {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).expect("client messages are within their limits");
        self.local.sends.push((conn, frame));
    }
}
