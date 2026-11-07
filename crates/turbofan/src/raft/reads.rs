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
        self.release_reads();
        let in_flight = self.confirmed_round() < self.reads.round;
        let wanted = self.reads.waiting.back().is_some_and(|&(_, round)| round > self.reads.round);
        if wanted && !in_flight {
            self.reads.round += 1;
            for i in 0..self.peers.len() {
                if self.is_voter(self.peers[i].id) {
                    self.send_append(i, true);
                }
            }
            // A lone voter confirms its own round.
            self.release_reads();
        }
        debug_assert!(self.reads.waiting.iter().all(|&(_, round)| round <= self.reads.round + 1));
    }

    /// Stops leading: the reads waiting are dropped, for the caller to ask the new leader.
    pub(super) fn drop_reads(&mut self) {
        let reads = std::mem::take(&mut self.reads);
        self.out.dropped_reads.extend(reads.waiting.into_iter().map(|(id, _)| id));
    }

    fn release_reads(&mut self) {
        // Until an entry of its own term commits, its commit index may lack what an earlier
        // leader committed.
        if self.term_at(self.commit) != Some(self.meta.term) {
            return;
        }
        let confirmed = self.confirmed_round();
        while let Some(&(id, _)) = self.reads.waiting.front().filter(|&&(_, round)| round <= confirmed) {
            self.reads.waiting.pop_front();
            self.out.confirmed_reads.push(ReadIndex { id, index: self.commit });
        }
    }

    /// The latest round a quorum of the voters has echoed, this leader among them if it votes.
    fn confirmed_round(&self) -> u64 {
        let mut echoed: Vec<u64> = self.peers.iter().filter(|p| self.is_voter(p.id)).map(|p| p.read_round).collect();
        if self.is_voter(self.config.id) {
            echoed.push(self.reads.round);
        }
        echoed.sort_unstable_by(|a, b| b.cmp(a));
        echoed.get(self.quorum() - 1).copied().unwrap_or(0)
    }
}
