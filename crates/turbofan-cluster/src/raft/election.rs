//! Elections: pre-vote, votes and their quorum, standing and stepping down, and doubt.

use super::*;

impl Raft {
    pub(super) fn on_request_vote(&mut self, from: NodeId, m: &wire::RequestVoteRef<'_>, now: u64) {
        // A node joining with nothing (perhaps wiped, and started again under its old ID) doesn't
        // vote: with no log, its vote could elect a removed node, standing in members the cluster
        // has left. One with members or a log votes, even before it's among them, as Raft has a
        // new member do; its log then turns a stale candidate down. A member being added may have
        // the log before the entry that adds it, and so no members: its vote may be one an
        // election needs, the voters in doubt taking every member's.
        let knows = !self.members().is_empty() || self.last_index() > 0;
        let up_to_date = (m.last_term(), m.last_index()) >= (self.last_term(), self.last_index()) && knows;
        let pre_vote = m.pre_vote() == 1;
        let granted = if pre_vote {
            let leader_alive = self.role == Role::Leader
                || self.leader_seen_at.is_some_and(|at| at + nanos(self.config.election_timeout) > now);
            m.term() > self.meta.term && up_to_date && !leader_alive
        } else {
            if m.term() > self.meta.term {
                self.become_follower(m.term(), now);
            }
            let free = self.meta.vote.is_none_or(|v| v == from);
            let granted = m.term() == self.meta.term && free && up_to_date;
            if granted && self.meta.vote.is_none() {
                self.meta.vote = Some(from);
                self.out.persist.push(Persist::Meta(self.meta));
            }
            if granted {
                self.reset_election(now);
            }
            granted
        };
        let doubt = u8::from(self.doubt.is_some());
        let (term, granted, pre_vote) = (self.meta.term, u8::from(granted), u8::from(pre_vote));
        let vote = wire::Vote { term, granted, pre_vote, doubt, round: m.round().unwrap_or(0) };
        self.send(from, &vote);
    }

    pub(super) fn on_vote(&mut self, from: NodeId, m: &wire::VoteRef<'_>, now: u64) {
        if m.term() > self.meta.term && m.granted() == 0 {
            self.become_follower(m.term(), now);
            return;
        }
        let counts = match self.role {
            // An older node's, without a round, counts in any.
            Role::PreCandidate => m.pre_vote() == 1 && m.round().is_none_or(|round| round == self.round),
            Role::Candidate => m.pre_vote() == 0 && m.term() == self.meta.term,
            Role::Follower | Role::Leader => false,
        };
        if !counts || m.granted() == 0 || self.votes.contains(&from) || !self.is_voter(from) {
            return;
        }
        self.votes.push(from);
        self.doubted += usize::from(m.doubt() == Some(1));
        if self.won() {
            if self.role == Role::PreCandidate {
                self.become_candidate(now);
            } else {
                self.become_leader(now);
            }
        }
    }

    pub(super) fn start_pre_vote(&mut self, now: u64) {
        self.role = Role::PreCandidate;
        self.leader = None;
        // Only the voters' votes count: its own too, if it's one.
        self.votes = if self.is_voter(self.config.id) { vec![self.config.id] } else { Vec::new() };
        self.doubted = usize::from(self.doubt.is_some() && !self.votes.is_empty());
        self.reset_election(now);
        assert!(now > self.round || self.round == 0, "a pre-vote round named twice");
        self.round = now;
        let request = wire::RequestVote {
            term: self.meta.term + 1,
            candidate: self.config.id.0,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre_vote: 1,
            round: now,
        };
        for i in 0..self.peers.len() {
            self.send(self.peers[i].id, &request);
        }
        if self.won() {
            self.become_candidate(now);
        }
    }

    pub(super) fn become_candidate(&mut self, now: u64) {
        self.role = Role::Candidate;
        self.forget_stop();
        self.meta = Meta { term: self.meta.term + 1, vote: Some(self.config.id) };
        self.out.persist.push(Persist::Meta(self.meta));
        // Only the voters' votes count: its own too, if it's one.
        self.votes = if self.is_voter(self.config.id) { vec![self.config.id] } else { Vec::new() };
        self.doubted = usize::from(self.doubt.is_some() && !self.votes.is_empty());
        self.reset_election(now);
        let request = wire::RequestVote {
            term: self.meta.term,
            candidate: self.config.id.0,
            last_index: self.last_index(),
            last_term: self.last_term(),
            pre_vote: 0,
            round: 0,
        };
        for i in 0..self.peers.len() {
            self.send(self.peers[i].id, &request);
        }
        if self.won() {
            self.become_leader(now);
        }
    }

    pub(super) fn become_leader(&mut self, now: u64) {
        assert_eq!(self.meta.vote, Some(self.config.id));
        self.role = Role::Leader;
        self.leader = Some(self.config.id);
        self.peers.clear();
        self.rebuild_peers();
        self.heartbeat_at = now;
        self.out.became_leader = true;
    }

    pub(super) fn become_follower(&mut self, term: u64, now: u64) {
        assert!(term >= self.meta.term);
        if term > self.meta.term {
            self.meta = Meta { term, vote: None };
            self.out.persist.push(Persist::Meta(self.meta));
            self.leader = None;
            self.forget_stop();
        }
        if self.role == Role::Leader {
            self.leader = None;
            self.drop_change();
            self.transfer = None;
            self.proposed_at.clear();
            self.unpersisted.clear();
            self.unapplied.clear();
            self.uncommitted_bytes = 0;
        }
        self.role = Role::Follower;
        self.votes.clear();
        self.reset_election(now);
    }

    /// A follower told to stop, but short of the snapshot it was to stop with, carries on in a
    /// new term: that shutdown was its leader's, and the cluster has been started since, or
    /// another leader elected. Else it stops when, much later, it gets a snapshot that far. A
    /// leader keeps its own: it answered that it's shut down.
    fn forget_stop(&mut self) {
        if self.role != Role::Leader {
            self.stop = None;
        }
    }

    /// A majority of the voters.
    pub(super) fn quorum(&self) -> usize {
        self.voters() / 2 + 1
    }

    /// The votes win the election: a majority, and one more for each voter in doubt.
    pub(super) fn won(&self) -> bool {
        self.votes.len() >= (self.quorum() + self.doubted).min(self.voters())
    }

    /// The log, as of an entry or snapshot of `term`, has everything this node could have
    /// acknowledged before it restarted in doubt, if that's a later term.
    pub(super) fn trust(&mut self, term: u64) {
        if self.doubt.is_some_and(|since| term > since) {
            self.end_doubt();
        }
    }

    /// The log has everything this node could have acknowledged before it restarted in doubt.
    pub(super) fn end_doubt(&mut self) {
        if self.doubt.take().is_some() {
            self.out.persist.push(Persist::Trust);
        }
    }

    pub(super) fn reset_election(&mut self, now: u64) {
        // xorshift64: deterministic jitter, so a seed replays a run exactly.
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        let timeout = nanos(self.config.election_timeout);
        self.election_at = now + timeout + self.rng % timeout;
    }
}
