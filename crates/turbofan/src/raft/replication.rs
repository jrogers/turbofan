//! Replication: appends and their acks, commit, the log's own changes, and reading back what's on
//! disk only.

use super::*;

impl Raft {
    /// The payload of the entry at `index`, if its record is in memory: in the window, or read
    /// back since the last [`Raft::take_outputs`]. For one the log holds only on disk,
    /// [`Raft::fetch`] it.
    #[must_use]
    pub fn entry(&self, index: u64) -> Option<&[u8]> {
        let (record, _) = Record::decode(self.record(index)?).ok().flatten().expect("records are checked");
        Some(record.payload)
    }

    /// Asks for the records from `index` on, if the log holds them only on disk: see
    /// [`Outputs::reads`].
    pub fn fetch(&mut self, index: u64) {
        let on_disk = index >= self.log.first && index < self.log.window_first;
        if on_disk && !self.out.reads.contains(&index) {
            self.out.reads.push(index);
        }
    }

    /// Records read back from disk for [`Outputs::reads`]. They're kept until the next
    /// [`Raft::take_outputs`]; any that the log doesn't hold, or holds in memory, are ignored.
    pub fn on_read(&mut self, records: &[u8]) {
        let (mut first, mut run, mut at) = (None, Vec::new(), 0);
        while let Ok(Some((record, len))) = Record::decode(&records[at..]) {
            let index = first.map_or(record.index, |f| f + crate::widen(run.len()));
            let on_disk = index >= self.log.first && index < self.log.window_first;
            if record.index != index || !on_disk || self.log.term_at(index) != Some(record.term) {
                break;
            }
            first.get_or_insert(index);
            run.push(records[at..at + len].to_vec());
            at += len;
        }
        if let Some(first) = first {
            self.fetched.push((first, run));
        }
    }

    /// Entries up to `index` are applied: their records, past the window, can leave memory once
    /// they're durable, so a read can find them.
    pub fn on_applied(&mut self, index: u64) {
        reached(&mut self.unapplied, index, Stage::Applied, self.now, &mut self.out);
        self.log.evict(index.min(self.commit).min(self.durable), self.config.log_window_bytes);
    }

    pub(super) fn on_append_entries(&mut self, from: NodeId, m: &wire::AppendEntriesRef<'_>, now: u64) {
        if m.term() < self.meta.term {
            self.ack(from, false, self.last_index() + 1, m.prev_index(), 0);
            return;
        }
        if m.term() > self.meta.term || self.role != Role::Follower {
            self.become_follower(m.term(), now);
        }
        self.leader = Some(from);
        self.leader_seen_at = Some(now);
        self.reset_election(now);

        let prev = m.prev_index();
        // An append of this term, refused or not: the leader is still this node's.
        let round = m.read_round().unwrap_or(0);
        if prev > self.last_index() {
            self.ack(from, false, self.last_index() + 1, prev, round);
            return;
        }
        if prev >= self.log.first - 1 && self.term_at(prev) != Some(m.prev_term()) {
            let hint = self.first_index_of_term_at(prev).max(self.commit + 1);
            self.ack(from, false, hint, prev, round);
            return;
        }
        let Some(matched) = self.accept_records(m) else { return };
        // Reaching the leader's `lastIndex` doesn't end a doubt: an append sent before this node
        // restarted, arriving late, says less than this node acknowledged before it did.
        // A reordered, older append can match less than is already known committed.
        self.commit = self.commit.max(m.commit().min(matched));
        self.ack(from, true, matched, prev, round);
    }

    /// Appends `m`'s records where they're new, truncating a conflicting suffix first. Returns
    /// the index up to which this log now matches the leader's, or `None` if the records are
    /// malformed (and so ignored).
    pub(super) fn accept_records(&mut self, m: &wire::AppendEntriesRef<'_>) -> Option<u64> {
        let prev = m.prev_index();
        let (mut expected, mut term) = (prev + 1, m.prev_term());
        for record in codec::records(m.entries()) {
            let Ok(record) = record else { return None };
            // Indices in order, and terms that never fall nor pass the leader's.
            if record.index != expected || record.term < term || record.term > m.term() {
                return None;
            }
            (expected, term) = (expected + 1, record.term);
        }
        if expected - prev - 1 != u64::from(m.count()) {
            return None;
        }
        for record in codec::records(m.entries()) {
            let record = record.expect("checked above");
            if record.index < self.log.first {
                continue;
            }
            if record.index <= self.last_index() {
                if self.term_at(record.index) == Some(record.term) {
                    continue;
                }
                self.truncate_from(record.index);
            }
            self.append(record);
        }
        Some(prev + u64::from(m.count()))
    }

    pub(super) fn on_append_ack(&mut self, peer: usize, m: &wire::AppendAckRef<'_>, now: u64) {
        if m.term() > self.meta.term {
            self.become_follower(m.term(), now);
            return;
        }
        if m.term() < self.meta.term || self.role != Role::Leader {
            return;
        }
        let last = self.last_index();
        let p = &mut self.peers[peer];
        p.heard_at = now;
        p.snapshot_index = p.snapshot_index.max(m.snapshot_index());
        p.versions = Some((m.protocol(), m.service_version()));
        // No further than the rounds started: a peer can't echo one it wasn't sent.
        p.read_round = p.read_round.max(m.read_round().unwrap_or(0).min(self.reads.round));
        let id = p.id;
        if self.catching_up.as_ref().is_some_and(|c| c.member.id == id) && self.held_back(peer) {
            // Too old to run what the cluster does: it isn't added, nor sent the log.
            self.drop_change();
            return;
        }
        let p = &mut self.peers[peer];
        if m.success() == 1 {
            p.sending = None;
            (p.probe, p.probe_read) = (None, false);
            p.chunk_wanted = false;
            debug_assert!(m.index() <= last, "a follower can't have more than this leader sent");
            p.matched = p.matched.max(m.index().min(last));
            p.next = p.next.max(p.matched + 1);
            let (id, matched) = (p.id, p.matched);
            self.advance_commit();
            self.check_catch_up(id, matched);
            self.try_transfer();
        } else if let Some(prev) = m.prev_index() {
            // A refusal of an append other than the probe in flight is of an older one,
            // overtaken: each pipelined append is refused, and answering each would send more
            // than are answered, round after round.
            if p.probe.is_some_and(|probe| probe != prev) {
                return;
            }
            // Refused from no further than it had matched: it lost entries it acknowledged, a
            // damaged tail its log dropped on restart (it doubts), and is sent them again. (A
            // refusal reordered past a later success costs a probe.)
            if m.index() <= p.matched {
                p.matched = m.index().saturating_sub(1);
            }
            // Then one append at a time, till one is taken.
            p.next = m.index().clamp(p.matched + 1, last + 1);
            p.probe = Some(p.next - 1);
            self.send_append(peer, true);
        } else {
            // From an older node, which doesn't say what it refuses: where it says.
            if m.index() <= p.matched {
                p.matched = m.index().saturating_sub(1);
            }
            let next = m.index().clamp(p.matched + 1, last + 1);
            if next < p.next {
                p.next = next;
                self.send_append(peer, true);
            }
        }
        // A learner joins once it's known new enough, caught up or not: the quorum doesn't wait
        // on it, so neither does adding it. It catches up as a member.
        if self.catching_up.as_ref().is_some_and(|c| c.member.id == id && c.member.learner) {
            self.join();
        }
    }

    /// Sends a peer the records from its `next`. If the first is on disk only, it's fetched and
    /// nothing is sent, or with `probe`, an append of no records: a heartbeat, which also finds
    /// where the peer's log matches. Returns whether `next` moved on, so that more may follow: not
    /// for a probe, nor a snapshot, nor records still to be read.
    pub(super) fn send_append(&mut self, peer: usize, probe: bool) -> bool {
        let (next, id) = (self.peers[peer].next, self.peers[peer].id);
        if self.held_back(peer) {
            // Nothing of the log until it's known new enough, which its ack to this will say.
            if probe {
                let prev = self.log.first - 1;
                let heartbeat = wire::AppendEntries {
                    term: self.meta.term,
                    leader: self.config.id.0,
                    prev_index: prev,
                    prev_term: self.base_term,
                    commit: self.commit.min(prev),
                    count: 0,
                    entries: &[],
                    last_index: self.last_index(),
                    read_round: self.reads.round,
                };
                let _ = self.send_append_frame(id, &heartbeat);
            }
            return false;
        }
        if next < self.log.first {
            self.resume_snapshot(peer);
            return false;
        }
        // A heartbeat to a peer too far behind on its acks (gone, say) carries no records: each
        // would otherwise queue another for it, up to a megabyte.
        let to = if next > self.peers[peer].matched + MAX_IN_FLIGHT { 0 } else { self.replicate_to() };
        // Every field of the append, but its records, which these fix: the same for each peer at
        // `next`, so the one made for the first goes to the rest.
        let key = [next, to, self.commit, self.reads.round, self.last_index(), self.meta.term];
        let (index, count) = if let Some((_, end, frame)) = self.shared_append.as_ref().filter(|s| s.0 == key) {
            let (end, frame) = (*end, Arc::clone(frame));
            self.out.appends.push((id, frame));
            (end, end - next)
        } else {
            let prev_term = self.term_at(next - 1).expect("next is past the snapshot");
            let (bytes, count) = self.records_for_append(next, to);
            let index = next + u64::from(count);
            if count == 0 && next <= to {
                self.fetch(next);
                self.peers[peer].probe_read = self.peers[peer].probe.is_some();
                if !probe {
                    return false;
                }
            }
            let append = wire::AppendEntries {
                term: self.meta.term,
                leader: self.config.id.0,
                prev_index: next - 1,
                prev_term,
                commit: self.commit,
                count,
                entries: &bytes,
                last_index: self.last_index(),
                read_round: self.reads.round,
            };
            let frame = self.send_append_frame(id, &append);
            // Records are what's worth sharing: a heartbeat is a few dozen bytes.
            if count > 0 {
                self.shared_append = Some((key, index, frame));
            }
            (index, u64::from(count))
        };
        let p = &mut self.peers[peer];
        if p.probe.is_some() {
            // It goes again from where it is, by heartbeat, until it's answered; its records
            // went, unless they're still being read.
            p.probe_read &= count == 0;
            return false;
        }
        p.next = index;
        index > next
    }

    /// The records in memory from `next`, up to `to`, as many as one append carries, and how many.
    fn records_for_append(&self, next: u64, to: u64) -> (Vec<u8>, u32) {
        let (mut bytes, mut count) = (Vec::new(), 0_u32);
        let mut index = next;
        while index <= to && u64::from(count) < crate::widen(MAX_ENTRIES_PER_APPEND) {
            let Some(record) = self.record(index) else { break };
            if !bytes.is_empty() && bytes.len() + record.len() > MAX_APPEND_BYTES {
                break;
            }
            bytes.extend_from_slice(record);
            count += 1;
            index += 1;
        }
        (bytes, count)
    }

    /// Queues `append` for `to`, returning its frame for others it may go to.
    pub(super) fn send_append_frame(&mut self, to: NodeId, append: &wire::AppendEntries<'_>) -> Frame {
        let mut frame = Vec::new();
        codec::encode_frame(append, &mut frame).expect("an append is within a frame");
        let frame = Arc::new(frame);
        self.out.appends.push((to, Arc::clone(&frame)));
        frame
    }

    pub(super) fn advance_commit(&mut self) {
        let peers = self.peers.iter().filter(|p| self.is_voter(p.id)).map(|p| p.matched);
        let mine = self.is_voter(self.config.id).then_some(self.durable);
        let Some(candidate) = quorum_highest(peers.chain(mine), self.quorum()) else { return };
        // Only an entry of this term commits by counting; earlier ones commit beneath it.
        if candidate > self.commit && self.term_at(candidate) == Some(self.meta.term) {
            self.commit = candidate;
            while let Some(&(index, at, len)) = self.proposed_at.front().filter(|&&(index, ..)| index <= candidate) {
                self.proposed_at.pop_front();
                self.uncommitted_bytes -= len;
                debug_assert!(index <= self.commit);
                self.out.latencies.push((Stage::Committed, self.now.saturating_sub(at)));
            }
        }
        // A leader that removed itself leads until that's committed, then stands down.
        if self.role == Role::Leader && !self.is_voter(self.config.id) && self.commit >= self.config_index() {
            self.become_follower(self.meta.term, self.now);
        }
    }

    /// How far the log is sent to peers: all of it, but while the leadership is being handed
    /// over, only as far as it was when that began, so no voter's log is newer than the
    /// target's will be. What's proposed meanwhile goes once the transfer gives up, or is
    /// dropped by the new leader, for clients to resend.
    pub(super) fn replicate_to(&self) -> u64 {
        self.transfer.map_or(self.last_index(), |t| t.upto)
    }

    pub(super) fn append(&mut self, record: Record<'_>) {
        assert_eq!(record.index, self.last_index() + 1);
        assert!(record.term >= self.last_term());
        let configs = self.log.configs.len();
        let bytes = self.log.push(record);
        match self.out.persist.last_mut() {
            Some(Persist::Append(batch)) => batch.extend_from_slice(bytes),
            _ => self.out.persist.push(Persist::Append(bytes.to_vec())),
        }
        self.trust(record.term);
        if self.log.configs.len() != configs {
            self.peers_changed();
        }
    }

    pub(super) fn truncate_from(&mut self, index: u64) {
        assert!(index > self.commit, "a committed entry is never replaced");
        let configs = self.log.configs.len();
        self.log.truncate_from(index);
        if self.log.configs.len() != configs {
            self.peers_changed();
        }
        self.durable = self.durable.min(index - 1);
        self.out.persist.push(Persist::TruncateFrom(index));
    }

    /// Answers an append (or a snapshot, `read_round` 0), echoing its round.
    pub(super) fn ack(&mut self, to: NodeId, success: bool, index: u64, prev_index: u64, read_round: u64) {
        let ack = wire::AppendAck {
            term: self.meta.term,
            success: u8::from(success),
            index,
            snapshot_index: self.snapshot_index(),
            protocol: self.advertised.0,
            service_version: self.advertised.1,
            prev_index,
            read_round,
        };
        self.send(to, &ack);
    }

    pub(super) fn send(&mut self, to: NodeId, message: &impl turbofan_sbe::Encode) {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).expect("Raft's messages are within their limits");
        self.out.sends.push((to, Arc::new(frame)));
    }

    /// The first index of the run of entries with the same term as `index`'s.
    pub(super) fn first_index_of_term_at(&self, index: u64) -> u64 {
        if index < self.log.first {
            return index;
        }
        self.log.terms[self.log.run_of(index)].0
    }

    /// The record at `index`, if it's in memory.
    pub(super) fn record(&self, index: u64) -> Option<&[u8]> {
        if index < self.log.first || index > self.last_index() {
            return None;
        }
        self.log.record(index).or_else(|| {
            self.fetched
                .iter()
                .find_map(|(first, run)| run.get(usize::try_from(index.checked_sub(*first)?).ok()?).map(Vec::as_slice))
        })
    }

    pub(super) fn last_term(&self) -> u64 {
        self.term_at(self.last_index()).expect("the last entry is in the log or the snapshot")
    }
}
