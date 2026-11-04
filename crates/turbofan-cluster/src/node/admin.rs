//! Operators' requests: snapshots, members, activation, shutdown, leadership transfer, promotion,
//! and status.

use super::*;

impl<S: Service> Node<S> {
    /// An operator's request on `conn`. `SnapshotRequest` asks for a snapshot at the next index,
    /// on every node, and is answered `SnapshotTaken` once a quorum has it durable. `AddMember`,
    /// `RemoveMember` and `SetRole` change the members, one at a time, and are answered `MembersChanged`
    /// once the change is committed, or `MembersRefused`. `Promote` is for a learner, not the
    /// leader: see [`Raft::promote`]. `StatusRequest` is answered `Status`, each member's progress.
    pub fn on_admin(&mut self, conn: ConnId, message: &wire::Decoded<'_>, now: u64) {
        self.after(now);
        // A request under way, sent again after its connection dropped: answered here instead.
        if let Some(id) = request_id(message)
            && self.answer_on(conn, id)
        {
            return;
        }
        // To a learner, which never leads: not for the leader, if there's one.
        if let wire::Decoded::Promote(m) = message {
            self.promote(conn, m.request_id());
            return;
        }
        if request_id(message).is_none() && !matches!(message, wire::Decoded::StatusRequest(_)) {
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
            wire::Decoded::AddMember(_) | wire::Decoded::RemoveMember(_) | wire::Decoded::SetRole(_) => {
                self.members_request(conn, message);
            }
            wire::Decoded::Activate(m) => self.activate(conn, m.request_id(), m.protocol(), m.service_version()),
            wire::Decoded::Shutdown(m) => {
                if self.local.shutting.is_none() {
                    self.local.shutting = Some((conn, m.request_id(), None));
                    self.propose(&wire::Snapshot { timestamp: now, request_id: m.request_id() });
                }
            }
            // To this node: sent again after the transfer, to the new leader.
            wire::Decoded::TransferLeadership(m) if NodeId::from_wire(m.to()) == Some(self.raft.id()) => {
                let leader = self.raft.id().0;
                self.send_client(conn, &wire::LeadershipTransferred { request_id: m.request_id(), leader });
            }
            wire::Decoded::TransferLeadership(m) => {
                let request_id = m.request_id();
                let free = self.transferring.is_none() && self.local.shutting.is_none();
                if free && self.raft.transfer_leadership(NodeId::from_wire(m.to())).is_some() {
                    self.transferring = Some((conn, request_id));
                } else {
                    self.send_client(conn, &wire::LeadershipTransferred { request_id, leader: 0 });
                }
            }
            wire::Decoded::StatusRequest(m) => self.status(conn, m.request_id()),
            _ => unreachable!("matched above"),
        }
    }

    /// Moves the answer to request `id`, if it's under way, to `conn`.
    fn answer_on(&mut self, conn: ConnId, id: u64) -> bool {
        let local = &mut self.local;
        let slots = [
            local.requested.get_mut(&id),
            local.awaiting.get_mut(&id).map(|(c, _)| c),
            local.changing.as_mut().filter(|(_, r, _)| *r == id).map(|(c, _, _)| c),
            local.shutting.as_mut().filter(|(_, r, _)| *r == id).map(|(c, _, _)| c),
            local.activating.as_mut().filter(|(_, r, _)| *r == id).map(|(c, _, _)| c),
            self.transferring.as_mut().filter(|(_, r)| *r == id).map(|(c, _)| c),
            self.promoting.as_mut().filter(|(_, r, _)| *r == id).map(|(c, _, _)| c),
        ];
        let mut found = false;
        for slot in slots.into_iter().flatten() {
            *slot = conn;
            found = true;
        }
        found
    }

    /// Logs a switch to these versions, unless a member doesn't support them, or it's a step
    /// back, or one is under way.
    pub(super) fn activate(&mut self, conn: ConnId, request_id: u64, protocol: u32, service_version: u32) {
        let (lowest_protocol, lowest_service_version) = self.raft.lowest_versions();
        let supported = protocol <= lowest_protocol && service_version <= lowest_service_version;
        let forward = protocol >= self.state.protocol && service_version >= self.state.service_version;
        // Not while a member is being added: it could join, too old, after this is logged.
        let free = self.local.activating.is_none() && !self.raft.changing_members();
        let index = (supported && forward && free)
            .then(|| {
                let egress_ring = if protocol >= 1 { self.config.egress_ring_entries } else { 0 };
                let egress_ring = u32::try_from(egress_ring).expect("at most MAX_EGRESS_RING");
                self.propose(&wire::Activated { timestamp: self.now, protocol, service_version, egress_ring })
            })
            .flatten();
        match index {
            Some(index) => self.local.activating = Some((conn, request_id, index)),
            None => {
                let refused = wire::Activation { request_id, index: 0, lowest_protocol, lowest_service_version };
                self.send_client(conn, &refused);
            }
        }
    }

    /// Makes this learner a cluster of its own, of a new epoch ([`Raft::promote`]): answered
    /// `Promoted` once the epoch applies, after the rest of its log.
    pub(super) fn promote(&mut self, conn: ConnId, request_id: u64) {
        // Sent again after it was answered.
        if let Some((_, index, epoch)) = self.promoted.filter(|&(r, ..)| r == request_id) {
            self.send_client(conn, &wire::Promoted { request_id, index, epoch });
            return;
        }
        if self.promoting.is_some() || !self.raft.promote(self.now) {
            self.send_client(conn, &wire::Promoted { request_id, index: 0, epoch: 0 });
            return;
        }
        let epoch = mint_epoch(self.now, self.raft.id());
        self.propose(&wire::Epoch { timestamp: self.now, epoch, promoted: 1 });
        self.promoting = Some((conn, request_id, epoch));
    }

    /// Answers with each member's progress ([`Raft::progress`]).
    pub(super) fn status(&mut self, conn: ConnId, request_id: u64) {
        let progress = self.raft.progress();
        let member: Vec<_> = progress
            .iter()
            .map(|&(m, matched)| wire::StatusMember { id: m.id.0, learner: u8::from(m.learner), matched })
            .collect();
        let (leader, last_index, commit_index) = (self.raft.id().0, self.raft.last_index(), self.raft.commit_index());
        self.send_client(conn, &wire::Status { request_id, leader, last_index, commit_index, member: &member });
    }

    /// `AddMember`, `RemoveMember` or `SetRole`.
    fn members_request(&mut self, conn: ConnId, message: &wire::Decoded<'_>) {
        match message {
            wire::Decoded::AddMember(m) => {
                let text = |b: &[u8]| String::from_utf8_lossy(b).into_owned();
                let member = Member {
                    id: NodeId(m.id()),
                    peer: text(m.peer()),
                    client: text(m.client()),
                    admin: text(m.admin()),
                    learner: m.learner() == 1,
                };
                if !self.already_changed(conn, m.request_id(), |members| members.contains(&member)) {
                    self.change_members(conn, m.request_id(), |raft| raft.add_member(member));
                }
            }
            wire::Decoded::RemoveMember(m) => {
                let id = NodeId(m.id());
                if !self.already_changed(conn, m.request_id(), |members| members.iter().all(|m| m.id != id)) {
                    let started = self.local.changing.is_none() && self.raft.remove_member(id);
                    self.start_change(conn, m.request_id(), started);
                }
            }
            wire::Decoded::SetRole(m) => {
                let (id, learner) = (NodeId(m.id()), m.learner() == 1);
                let done = |members: &[Member]| members.iter().any(|m| m.id == id && m.learner == learner);
                if !self.already_changed(conn, m.request_id(), done) {
                    self.change_members(conn, m.request_id(), |raft| raft.set_role(id, learner));
                }
            }
            _ => unreachable!("a change of members"),
        }
    }

    /// The members are already as a change asks (`done`), and committed: a request sent again after
    /// it was made, its answer lost or its leader gone, is answered as made. Nothing is kept of
    /// answered requests, so any leader answers it so.
    fn already_changed(&mut self, conn: ConnId, request_id: u64, done: impl Fn(&[Member]) -> bool) -> bool {
        if self.raft.changing_members() || !done(self.raft.members()) {
            return false;
        }
        self.send_client(conn, &wire::MembersChanged { request_id, index: self.raft.config_index() });
        true
    }

    /// Starts a change of members by `change`, unless one is under way, or an activation, which
    /// a member too old for it could otherwise slip past.
    fn change_members(&mut self, conn: ConnId, request_id: u64, change: impl FnOnce(&mut Raft) -> bool) {
        let free = self.local.changing.is_none() && self.local.activating.is_none();
        let started = free && change(&mut self.raft);
        self.start_change(conn, request_id, started);
    }

    pub(super) fn start_change(&mut self, conn: ConnId, request_id: u64, started: bool) {
        if started {
            self.local.changing = Some((conn, request_id, None));
        } else {
            self.send_client(conn, &wire::MembersRefused { request_id });
        }
    }

    /// Answers each manual snapshot a quorum, this leader among it, now has durable.
    pub(super) fn answer_snapshot_requests(&mut self) {
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
}

/// The request ID of an operator's request.
fn request_id(message: &wire::Decoded<'_>) -> Option<u64> {
    Some(match message {
        wire::Decoded::SnapshotRequest(m) => m.request_id(),
        wire::Decoded::AddMember(m) => m.request_id(),
        wire::Decoded::RemoveMember(m) => m.request_id(),
        wire::Decoded::SetRole(m) => m.request_id(),
        wire::Decoded::TransferLeadership(m) => m.request_id(),
        wire::Decoded::Shutdown(m) => m.request_id(),
        wire::Decoded::Activate(m) => m.request_id(),
        wire::Decoded::Promote(m) => m.request_id(),
        _ => return None,
    })
}
