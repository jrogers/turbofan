//! Applying committed entries, in order, to the replicated state and the service; and its timers.

use super::*;

impl<S: Service> Node<S> {
    pub(super) fn fire_timers(&mut self, now: u64) {
        let due: Vec<u64> = self.state.deadlines.iter().take_while(|(at, _)| *at <= now).map(|&(_, id)| id).collect();
        for id in due {
            if self.local.firing.insert(id) {
                self.propose(&wire::TimerFired { timestamp: now, id });
            }
        }
    }

    /// Applies committed entries, in order, as far as their records are in memory; fetches the
    /// next from disk if it isn't.
    pub(super) fn apply(&mut self) {
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
        for session in std::mem::take(&mut self.local.unacked) {
            if let (Some(&conn), Some(s)) = (self.local.attached.get(&session), self.state.sessions.get(&session)) {
                let applied_seq = s.last_seq;
                self.send_client(conn, &wire::KeepAliveAck { applied_seq });
            }
        }
        self.log_outbox();
    }

    pub(super) fn apply_entry(&mut self, index: u64, entry: &wire::Decoded<'_>, ours: bool) {
        match entry {
            wire::Decoded::NewTerm(e) => {
                self.advance_time(e.timestamp());
                self.local.ready |= ours;
            }
            wire::Decoded::Epoch(e) => {
                self.advance_time(e.timestamp());
                self.apply_epoch(index, e.epoch(), e.promoted() == 1);
            }
            wire::Decoded::Activated(e) => {
                self.advance_time(e.timestamp());
                self.apply_activated(index, e.protocol(), e.service_version(), e.egress_ring().unwrap_or(0));
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
                self.apply_open(SessionId(index), e.principal());
            }
            wire::Decoded::SessionMessage(e) => {
                self.advance_time(e.timestamp());
                let Some(s) = self.state.sessions.get_mut(&SessionId(e.session())) else { return };
                // Exactly once, in order: a resend or one after a gap the client will resend is dropped.
                if e.client_seq() != s.last_seq + 1 {
                    return;
                }
                s.last_seq = e.client_seq();
                self.local.unacked.insert(SessionId(e.session()));
                self.call(|service, ctx| service.on_message(ctx, SessionId(e.session()), e.payload()));
            }
            wire::Decoded::SessionClose(e) => {
                self.advance_time(e.timestamp());
                self.apply_close(SessionId(e.session()), e.reason());
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

    /// The cluster's epoch from `index` on, if it has none yet or this one's promoted: answered,
    /// if it's this node's promotion.
    pub(super) fn apply_epoch(&mut self, index: u64, epoch: u64, promoted: bool) {
        if self.state.epoch == 0 || promoted {
            self.state.epoch = epoch;
            // The first epoch has every session; a promoted standby's, those from here on.
            self.state.epoch_since = if promoted { index } else { 0 };
        }
        if let Some((conn, request_id, at)) = self.promoting
            && at == epoch
        {
            self.promoting = None;
            self.promoted = Some((request_id, index, epoch));
            self.send_client(conn, &wire::Promoted { request_id, index, epoch });
        }
    }

    /// From `index` on, the cluster runs these versions, and from protocol 1 each session keeps
    /// `egress_ring` responses (0 for each node's own number).
    pub(super) fn apply_activated(&mut self, index: u64, protocol: u32, service_version: u32, egress_ring: u32) {
        self.state.protocol = protocol;
        if protocol >= 1 {
            self.state.egress_ring = egress_ring;
        }
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

    pub(super) fn apply_open(&mut self, session: SessionId, principal: &[u8]) {
        let opened = Session { last_seq: 0, next_egress: 1, ring: VecDeque::new(), principal: principal.to_vec() };
        self.state.sessions.insert(session, opened);
        if let Some(conn) = self.local.opening.remove(&session) {
            self.local.conns.insert(conn, Some(session));
            self.local.attached.insert(session, conn);
            let epoch = self.state.epoch;
            self.send_client(conn, &wire::Opened { session: session.0, applied_seq: 0, last_egress_seq: 0, epoch });
        }
        if self.local.was_leader {
            self.local.activity.insert(session, self.now);
        }
        self.call(|service, ctx| service.on_session_open(ctx, session, principal));
    }

    pub(super) fn apply_close(&mut self, session: SessionId, reason: CloseReason) {
        self.local.closing.remove(&session);
        self.local.activity.remove(&session);
        self.local.proposed.remove(&session);
        if self.state.sessions.remove(&session).is_none() {
            return;
        }
        if let Some(conn) = self.local.attached.remove(&session) {
            self.local.conns.insert(conn, None);
            self.send_client(conn, &wire::Closed { session: session.0, reason });
        }
        self.call(|service, ctx| service.on_session_close(ctx, session, reason));
    }

    pub(super) fn advance_time(&mut self, timestamp: u64) {
        // A new leader's clock may be behind the old one's: cluster time holds still instead.
        self.state.time = self.state.time.max(timestamp);
    }

    /// Runs a service callback, then its effects.
    pub(super) fn call(&mut self, callback: impl FnOnce(&mut S, &mut Ctx)) {
        self.ctx.now = self.state.time;
        self.ctx.service_version = self.state.service_version;
        callback(&mut self.service, &mut self.ctx);
        let effects = std::mem::take(&mut self.ctx.effects);
        if let Some(replayed) = &mut self.replayed {
            replayed.extend(effects.iter().cloned());
        }
        for effect in effects {
            match effect {
                Effect::Send(session, payload) => self.egress(session, payload),
                Effect::Schedule { id, at } => {
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
    pub(super) fn log_outbox(&mut self) {
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

    pub(super) fn remove_timer(&mut self, id: u64) {
        if let Some(at) = self.state.timers.remove(&id) {
            self.state.deadlines.remove(&(at, id));
        }
    }
}
