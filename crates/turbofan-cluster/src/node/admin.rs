//! Operators' requests: snapshots, members, activation, shutdown, leadership transfer, promotion,
//! and status.

use super::*;

impl<S: Service> Node<S> {
    /// An operator's request on `conn`. `SnapshotRequest` asks for a snapshot at the next index,
    /// on every node, and is answered `SnapshotTaken` once a quorum has it durable. `AddMember`
    /// and `RemoveMember` change the members, one at a time, and are answered `MembersChanged`
    /// once the change is committed, or `MembersRefused`. `Promote` is for a learner, not the
    /// leader: see [`Raft::promote`]. `StatusRequest` is answered `Status`, each member's progress.
    pub fn on_admin(&mut self, conn: ConnId, message: &wire::Decoded<'_>, now: u64) {
        self.after(now);
        // To a learner, which never leads: not for the leader, if there's one.
        if let wire::Decoded::Promote(m) = message {
            self.promote(conn, m.request_id());
            return;
        }
        let admin = matches!(
            message,
            wire::Decoded::SnapshotRequest(_)
                | wire::Decoded::AddMember(_)
                | wire::Decoded::RemoveMember(_)
                | wire::Decoded::TransferLeadership(_)
                | wire::Decoded::Shutdown(_)
                | wire::Decoded::Activate(_)
                | wire::Decoded::StatusRequest(_)
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
                let member = Member {
                    id: NodeId(m.id()),
                    peer: text(m.peer()),
                    client: text(m.client()),
                    admin: text(m.admin()),
                    learner: m.learner() == 1,
                };
                let free = self.local.changing.is_none() && self.local.activating.is_none();
                let started = free && self.raft.add_member(member);
                self.start_change(conn, m.request_id(), started);
            }
            wire::Decoded::RemoveMember(m) => {
                let started = self.local.changing.is_none() && self.raft.remove_member(NodeId(m.id()));
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

    /// Logs a switch to these versions, unless a member doesn't support them, or it's a step
    /// back, or one is under way.
    pub(super) fn activate(&mut self, conn: ConnId, request_id: u64, protocol: u32, service_version: u32) {
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

    /// Makes this learner a cluster of its own, of a new epoch ([`Raft::promote`]): answered
    /// `Promoted` once the epoch applies, after the rest of its log.
    pub(super) fn promote(&mut self, conn: ConnId, request_id: u64) {
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
