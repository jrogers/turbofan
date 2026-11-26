//! Reads without the log (ReadIndex, the Raft thesis §6.4). A leader answers a read at its commit
//! index once it knows two things. It has committed an entry of its own term, so its commit index
//! holds every write completed before the read came in. And it still leads: a quorum of voters
//! has acked an append of a round started after the read came in, so no other leader can have
//! been elected before then, since a majority would have refused that append's term.
//!
//! A round is a number each append carries (`AppendEntries.readRound`) and each ack echoes. The
//! leader starts one only while none is in flight, so the reads arriving while one is share the
//! next. A round lost on the way goes again with the next heartbeat, which carries it too. A
//! follower before 0.9.0 doesn't echo it, and confirms no round: a cluster takes reads once every
//! voter is new enough.
//!
//! A read is never written, so one dropped (this node stopped leading) is asked again of the new
//! leader, and one answered twice is harmless.
//!
//! **A lease** ([`Config::read_lease`]) skips the round trip. Every voter, for an election timeout
//! after hearing a leader, refuses a candidate the leader didn't hand over to (`election.rs`), and
//! says so on its acks (`leaseWindow`). So once a quorum of them, saying so for a lease's length or
//! more, has acked a round started at `t`, no other leader can be elected before `t` plus the
//! lease, on this node's clock, while the clocks' rates differ by less than the lease's margin
//! under the election timeout: each voter heard the round after `t`. Till then, this leader
//! answers each read at its commit index as it comes in. With a lease, a round goes with every
//! heartbeat, to renew it. Read in the batch it comes in, at the batch's time, which is after it
//! arrived: the leader led then, its commit index has every write completed before.
//!
//! The lease is given up for the rest of the term once a transfer begins: the target's
//! candidacy, voted for by voters that refuse others, may be under way however the transfer
//! ends. And it's earned again after a change of members, by a round started after it: a quorum of
//! the voters as they were needn't be one of the voters as they are, nor of those after the next
//! change. A follower older than this promises nothing, and grants no lease.
//!
//! A peer being sent a snapshot gets no appends, so it confirms no round until it has it.
//! ponytail: the quorum's other voters confirm meanwhile; carry the round on `InstallSnapshot`
//! too if a cluster of three with one far behind needs reads to go on while the other stalls.

use super::*;

/// The most reads waiting on a leader: a round's worth from every client, far more than any
/// client keeps in flight. Past it, [`Raft::read_index`] refuses, and the caller asks again.
pub const MAX_WAITING_READS: usize = 1 << 16;

/// A read a leader has confirmed ([`Outputs::confirmed_reads`]): answer it once the state has
/// applied up to `index`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReadIndex {
    /// The caller's, from [`Raft::read_index`].
    pub id: u64,
    /// The commit index the answer must reflect.
    pub index: u64,
}

/// A leader's reads, for one term.
#[derive(Debug, Default)]
pub(super) struct Reads {
    /// The latest round this leader started, from 1 in its term.
    pub(super) round: u64,
    /// Reads waiting, in order: each one's ID, and the round that confirms it, the first started
    /// after it came in.
    waiting: VecDeque<(u64, u64)>,
    /// When the latest round started.
    round_at: u64,
    /// Reads are answered at once till this time ([`Config::read_lease`]).
    pub(super) lease_until: u64,
    /// The first round that may grant a lease: none, `u64::MAX`, once it's given up for the term.
    lease_from: u64,
}

impl Reads {
    /// Gives the lease up, for the rest of the term.
    pub(super) fn forfeit_lease(&mut self) {
        (self.lease_until, self.lease_from) = (0, u64::MAX);
    }

    /// Gives the lease up, till a round started after now grants another.
    pub(super) fn restart_lease(&mut self) {
        self.lease_until = 0;
        if self.lease_from != u64::MAX {
            self.lease_from = self.round + 1;
        }
    }
}

impl Raft {
    /// As leader, a read, confirmed in a batch's [`Outputs::confirmed_reads`] (a round trip to a
    /// quorum later), or dropped in [`Outputs::dropped_reads`] if this node stops leading first.
    /// `id` is the caller's, to know which. Returns false, doing nothing, if this node doesn't
    /// lead or [`MAX_WAITING_READS`] are waiting.
    pub fn read_index(&mut self, id: u64) -> bool {
        if self.role != Role::Leader || self.reads.waiting.len() >= MAX_WAITING_READS {
            return false;
        }
        // The round in flight, if one is, may have started before this came in: the next one.
        self.reads.waiting.push_back((id, self.reads.round + 1));
        true
    }

    /// Once per batch, as leader: hands out the reads confirmed, and starts a round for those
    /// waiting if none is in flight.
    pub(super) fn confirm_reads(&mut self) {
        self.renew_lease();
        self.release_reads();
        let wanted = self.reads.waiting.back().is_some_and(|&(_, round)| round > self.reads.round);
        if wanted && !self.round_in_flight() {
            self.start_read_round();
            for i in 0..self.peers.len() {
                if self.is_voter(self.peers[i].id) {
                    self.send_append(i, true);
                }
            }
            // A lone voter confirms its own round.
            self.renew_lease();
            self.release_reads();
        }
        debug_assert!(self.reads.waiting.iter().all(|&(_, round)| round <= self.reads.round + 1));
    }

    /// Stops leading: the reads waiting are dropped, for the caller to ask the new leader.
    pub(super) fn drop_reads(&mut self) {
        let reads = std::mem::take(&mut self.reads);
        self.out.dropped_reads.extend(reads.waiting.into_iter().map(|(id, _)| id));
    }

    /// As a heartbeat goes, with a lease: a round with it, unless one is in flight, to renew it.
    pub(super) fn heartbeat_read_round(&mut self) {
        if self.config.read_lease.is_some() && !self.round_in_flight() {
            self.start_read_round();
        }
    }

    fn start_read_round(&mut self) {
        self.reads.round += 1;
        // The appends that carry it go out after this: it's no later than they leave.
        self.reads.round_at = self.now;
    }

    fn round_in_flight(&self) -> bool {
        self.confirmed_round() < self.reads.round
    }

    /// The latest round, acked by a quorum of the voters promising a lease's length, grants one.
    fn renew_lease(&mut self) {
        let Some(lease) = self.config.read_lease else { return };
        let peers = self.peers.iter().filter(|p| self.is_voter(p.id)).map(|p| p.lease_round);
        // Leading, it votes for no one: standing down, it holds no lease.
        let mine = self.is_voter(self.config.id).then_some(self.reads.round);
        let leased = quorum_highest(peers.chain(mine), self.quorum()).unwrap_or(0);
        // Round 0 is none: there's no round to have acked.
        if leased > 0 && leased == self.reads.round && leased >= self.reads.lease_from {
            self.reads.lease_until = self.reads.lease_until.max(self.reads.round_at + nanos(lease));
        }
    }

    fn release_reads(&mut self) {
        // Until an entry of its own term commits, its commit index may lack what an earlier
        // leader committed.
        if self.term_at(self.commit) != Some(self.meta.term) {
            return;
        }
        let confirmed = if self.now < self.reads.lease_until { u64::MAX } else { self.confirmed_round() };
        while let Some(&(id, _)) = self.reads.waiting.front().filter(|&&(_, round)| round <= confirmed) {
            self.reads.waiting.pop_front();
            self.out.confirmed_reads.push(ReadIndex { id, index: self.commit });
        }
    }

    /// The latest round a quorum of the voters has echoed, this leader among them if it votes.
    fn confirmed_round(&self) -> u64 {
        let peers = self.peers.iter().filter(|p| self.is_voter(p.id)).map(|p| p.read_round);
        let mine = self.is_voter(self.config.id).then_some(self.reads.round);
        quorum_highest(peers.chain(mine), self.quorum()).unwrap_or(0)
    }
}
