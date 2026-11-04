//! Members: adding (after catching up, unless a learner), removing them and changing their role,
//! versions, leadership transfer, and promotion.

use super::*;

impl Raft {
    /// The peers to connect to: the members, and a member being added.
    #[must_use]
    pub fn peers(&self) -> Vec<Member> {
        let learner = self.adding().map(|c| c.member.clone());
        self.members().iter().cloned().chain(learner).collect()
    }

    /// The members, as of the latest `Members` entry in the log, or the snapshot.
    #[must_use]
    pub fn members(&self) -> &[Member] {
        self.log.configs.last().map_or(&self.base_members, |(_, members)| members)
    }

    /// The members as of `index`, which is in the log or the snapshot.
    #[must_use]
    pub fn members_at(&self, index: u64) -> Vec<Member> {
        let config = self.log.configs.iter().rev().find(|(at, _)| *at <= index);
        config.map_or_else(|| self.base_members.clone(), |(_, members)| members.clone())
    }

    /// Starts adding `member`: it's sent the log, without a vote, until it has caught up, then
    /// the members with it are proposed ([`Outputs::change_proposed`]), or, if it doesn't, the
    /// change is dropped ([`Outputs::change_failed`]). A learner ([`Member::learner`]) needn't
    /// catch up: it's proposed once it answers, new enough, and catches up as a member. Refused
    /// (false) unless this node leads, has committed an entry of its term, no other change is
    /// under way, and there's room.
    pub fn add_member(&mut self, member: Member) -> bool {
        if !self.can_change() || member.id.0 == 0 || self.is_member(member.id) || self.members().len() >= MAX_MEMBERS {
            return false;
        }
        if !member.fits() {
            return false;
        }
        let (now, last) = (self.now, self.last_index());
        self.catching_up = Some(CatchUp { member, started: now, round_start: now, round_end: last });
        self.peers_changed();
        true
    }

    /// Starts handing the leadership to voter `to`, or with `None` to the voter most caught up,
    /// returning which. It's sent `TimeoutNow` once it has the log as it is now; this node gives
    /// up after an election timeout ([`Raft::transferring`] goes false while it still leads).
    /// Refused (`None`) unless this node leads, isn't transferring already, and `to` is another
    /// voter, heard from within an election timeout.
    pub fn transfer_leadership(&mut self, to: Option<NodeId>) -> Option<NodeId> {
        if self.role != Role::Leader || self.transfer.is_some() || self.stop.is_some() {
            return None;
        }
        let alive = self.now.saturating_sub(nanos(self.config.election_timeout));
        let voters = self.peers.iter().filter(|p| self.is_voter(p.id) && p.heard_at >= alive);
        let target = if let Some(to) = to {
            voters.map(|p| p.id).find(|&id| id == to)?
        } else {
            voters.max_by_key(|p| p.matched)?.id
        };
        let (by, upto) = (self.now + nanos(self.config.election_timeout), self.last_index());
        self.transfer = Some(Transfer { to: target, by, upto, sent: false });
        self.try_transfer();
        Some(target)
    }

    /// The protocol and service versions this node's code supports, which it tells the leader.
    pub fn advertise(&mut self, protocol: u32, service: u32) {
        self.advertised = (protocol, service);
    }

    /// The protocol and service versions the cluster runs: a member that doesn't support them
    /// isn't added.
    pub fn require(&mut self, protocol: u32, service: u32) {
        self.required = (protocol, service);
    }

    /// As leader, the lowest protocol and service versions the members support, as they last
    /// said: 0 for one not heard from yet.
    #[must_use]
    pub fn lowest_versions(&self) -> (u32, u32) {
        let mine = self.is_member(self.config.id).then_some(self.advertised);
        let peers = self.peers.iter().filter(|p| self.is_member(p.id)).map(|p| p.versions.unwrap_or((0, 0)));
        mine.into_iter().chain(peers).fold((u32::MAX, u32::MAX), |(p, s), (q, t)| (p.min(q), s.min(t)))
    }

    /// A leadership transfer is under way.
    #[must_use]
    pub fn transferring(&self) -> bool {
        self.transfer.is_some()
    }

    /// Makes this learner the one member of a new cluster, with its cluster lost: it leads a new
    /// term, without an election, and proposes the members as itself, a voter, which take effect
    /// as they're appended. Its log stays as it is: what follows its commit index commits beneath
    /// that. Refused (false) unless it's a learner that hasn't heard a leader for an election
    /// timeout: never automatic, since a lost cluster and a cut link look alike from here.
    pub fn promote(&mut self, now: u64) -> bool {
        self.now = now;
        let timeout = nanos(self.config.election_timeout);
        let leader_alive = self.leader_seen_at.is_some_and(|at| at + timeout > self.now);
        if self.role != Role::Follower || !self.is_learner(self.config.id) || leader_alive || self.stop.is_some() {
            return false;
        }
        let me = self.members().iter().find(|m| m.id == self.config.id).expect("a learner is a member");
        let me = Member { learner: false, ..me.clone() };
        self.meta = Meta { term: self.meta.term + 1, vote: Some(self.config.id) };
        self.out.persist.push(Persist::Meta(self.meta));
        self.become_leader(self.now);
        self.propose_members(&[me]);
        true
    }

    /// Proposes the members without `id` ([`Outputs::change_proposed`]). Refused (false) unless
    /// this node leads, has committed an entry of its term, no other change is under way, `id`
    /// is a member, and a voter would be left.
    pub fn remove_member(&mut self, id: NodeId) -> bool {
        if !self.can_change() || !self.is_member(id) || !self.members().iter().any(|m| m.id != id && !m.learner) {
            return false;
        }
        let members: Vec<Member> = self.members().iter().filter(|m| m.id != id).cloned().collect();
        self.propose_members(&members);
        true
    }

    /// Makes member `id` a learner, or a voter, in place: one change of members. A voter made a
    /// learner is proposed at once ([`Outputs::change_proposed`]); a learner made a voter first
    /// catches up, as a member being added does ([`Raft::add_member`]), so that a quorum doesn't
    /// wait on it. Refused (false) unless this node leads, has committed an entry of its term, no
    /// other change is under way, `id` is a member of the other role, and a voter would be left.
    pub fn set_role(&mut self, id: NodeId, learner: bool) -> bool {
        let Some(member) = self.members().iter().find(|m| m.id == id && m.learner != learner).cloned() else {
            return false;
        };
        if !self.can_change() || (learner && self.voters() == 1) {
            return false;
        }
        let member = Member { learner, ..member };
        if learner {
            let members: Vec<Member> =
                self.members().iter().map(|m| if m.id == id { member.clone() } else { m.clone() }).collect();
            self.propose_members(&members);
        } else {
            let (now, last) = (self.now, self.last_index());
            self.catching_up = Some(CatchUp { member, started: now, round_start: now, round_end: last });
        }
        true
    }

    /// The member being added, not one changing role.
    fn adding(&self) -> Option<&CatchUp> {
        self.catching_up.as_ref().filter(|c| !self.is_member(c.member.id))
    }

    /// A member being added isn't sent the log until it's known to support the versions the
    /// cluster runs: applying what it's sent, older code would run behaviour it doesn't have.
    pub(super) fn held_back(&self, peer: usize) -> bool {
        let p = &self.peers[peer];
        let adding = self.adding().is_some_and(|c| c.member.id == p.id);
        let new_enough = p.versions.is_some_and(|(q, s)| q >= self.required.0 && s >= self.required.1);
        adding && !new_enough
    }

    /// Sends the transfer's target `TimeoutNow` once it has the log as it was when it began.
    pub(super) fn try_transfer(&mut self) {
        let Some(t) = self.transfer.as_mut().filter(|t| !t.sent) else { return };
        let (to, upto) = (t.to, t.upto);
        if self.peers.iter().any(|p| p.id == to && p.matched >= upto) {
            t.sent = true;
            self.send(to, &wire::TimeoutNow { term: self.meta.term });
        }
    }

    /// The leader of this term hands over: stand for election now.
    pub(super) fn on_timeout_now(&mut self, from: NodeId, m: &wire::TimeoutNowRef<'_>, now: u64) {
        if m.term() == self.meta.term && self.leader == Some(from) && self.is_voter(self.config.id) {
            self.become_candidate(now);
        }
    }

    /// Whether this node stands for election: it knows members, and isn't a learner, unless a
    /// change it hasn't seen committed made it one. Then it may have the most of the log, which
    /// the others, still counting it a voter, may need to elect anyone: it stands, its own vote
    /// not counted, and once leading, leads until the change commits, then stands down, as a
    /// leader that removed itself does.
    pub(super) fn may_stand(&self) -> bool {
        let me = self.config.id;
        let demoted = || {
            let at = self.config_index();
            at > self.commit && self.members_at(at - 1).iter().any(|m| m.id == me && !m.learner)
        };
        !self.members().is_empty() && (!self.is_learner(me) || demoted())
    }

    pub(super) fn is_member(&self, id: NodeId) -> bool {
        self.members().iter().any(|m| m.id == id)
    }

    pub(super) fn is_voter(&self, id: NodeId) -> bool {
        self.members().iter().any(|m| m.id == id && !m.learner)
    }

    pub(super) fn is_learner(&self, id: NodeId) -> bool {
        self.members().iter().any(|m| m.id == id && m.learner)
    }

    pub(super) fn voters(&self) -> usize {
        self.members().iter().filter(|m| !m.learner).count()
    }

    /// The index of the latest `Members` entry, or of the snapshot if the log has none.
    #[must_use]
    pub fn config_index(&self) -> u64 {
        self.log.configs.last().map_or(self.snapshot_index(), |&(index, _)| index)
    }

    /// A change of members is under way: one being added, or one proposed and not committed.
    #[must_use]
    pub fn changing_members(&self) -> bool {
        self.catching_up.is_some() || self.config_index() > self.commit
    }

    /// Whether this leader can start a change: one at a time, each committed before the next,
    /// and not before an entry of its own term is committed.
    pub(super) fn can_change(&self) -> bool {
        self.role == Role::Leader
            && self.catching_up.is_none()
            && self.config_index() <= self.commit
            && self.term_at(self.commit) == Some(self.meta.term)
    }

    /// A member being added has `matched` entries: once it catches up a round within an
    /// election timeout, the members with it are proposed.
    pub(super) fn check_catch_up(&mut self, id: NodeId, matched: u64) {
        let Some(c) = self.catching_up.as_mut().filter(|c| c.member.id == id && matched >= c.round_end) else {
            return;
        };
        if self.now - c.round_start > nanos(self.config.election_timeout) {
            (c.round_start, c.round_end) = (self.now, self.log.last());
            return;
        }
        // Caught up, but too old to run what the cluster does.
        let versions = self.peers.iter().find(|p| p.id == id).and_then(|p| p.versions).unwrap_or((0, 0));
        if versions.0 < self.required.0 || versions.1 < self.required.1 {
            self.drop_change();
            return;
        }
        self.join();
    }

    /// Proposes the members with the one being added, or changing role, as it will be.
    pub(super) fn join(&mut self) {
        let member = self.catching_up.take().expect("one is being added").member;
        let mut members = self.members().to_vec();
        members.retain(|m| m.id != member.id);
        members.push(member);
        members.sort_unstable_by_key(|m| m.id);
        self.propose_members(&members);
    }

    pub(super) fn drop_change(&mut self) {
        if self.catching_up.take().is_some() {
            self.out.change_failed = true;
            self.peers_changed();
        }
    }

    pub(super) fn propose_members(&mut self, members: &[Member]) {
        let addrs: Vec<wire::MembersMember<'_>> = members
            .iter()
            .map(|m| wire::MembersMember {
                id: m.id.0,
                learner: u8::from(m.learner),
                peer: m.peer.as_bytes(),
                client: m.client.as_bytes(),
                admin: m.admin.as_bytes(),
            })
            .collect();
        let mut payload = Vec::new();
        wire::Members { timestamp: self.now, member: &addrs }.encode_into(&mut payload).expect("addresses fit");
        let index = self.propose(&payload).expect("this node leads");
        self.out.change_proposed = Some(index);
    }

    /// The members, or the member being added, changed: peers follow, and the caller is told.
    pub(super) fn peers_changed(&mut self) {
        self.rebuild_peers();
        self.out.peers = Some(self.peers());
    }

    /// One peer for each member but this node, and for a member being added; those already there
    /// keep their state.
    pub(super) fn rebuild_peers(&mut self) {
        let (next, now) = (self.last_index() + 1, self.now);
        let learner = self.adding().map(|c| c.member.id);
        let mut ids: Vec<NodeId> = self.members().iter().map(|m| m.id).chain(learner).collect();
        ids.retain(|&id| id != self.config.id);
        self.peers.retain(|p| ids.contains(&p.id));
        for id in ids {
            if !self.peers.iter().any(|p| p.id == id) {
                self.peers.push(Peer::new(id, next, now));
            }
        }
    }
}
