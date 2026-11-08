//! Snapshots: this node's own, which compact its log, and one shipped to or from a follower.

use super::*;

impl Raft {
    /// As leader, not handing over, whether every member heard from within an election timeout,
    /// this node included, and a quorum of voters, has the snapshot of `index` durable.
    #[must_use]
    pub fn all_have_snapshot(&self, index: u64) -> bool {
        if self.role != Role::Leader || self.transfer.is_some() || self.snapshot_index() < index {
            return false;
        }
        let alive = self.now.saturating_sub(nanos(self.config.election_timeout));
        let members = self.peers.iter().filter(|p| self.is_member(p.id));
        let (mut have, mut missing) = (usize::from(self.is_voter(self.config.id)), false);
        for p in members {
            if p.snapshot_index >= index {
                have += usize::from(self.is_voter(p.id));
            } else if p.heard_at >= alive {
                missing = true;
            }
        }
        !missing && have >= self.quorum()
    }

    /// The highest snapshot index a quorum of voters (this node among them, if it's one) has
    /// durable, as a leader knows: one that removed itself leads until that commits.
    #[must_use]
    pub fn quorum_snapshot_index(&self) -> u64 {
        let peers = self.peers.iter().filter(|p| self.is_voter(p.id)).map(|p| p.snapshot_index);
        let mine = self.is_voter(self.config.id).then_some(self.snapshot_index());
        quorum_highest(peers.chain(mine), self.quorum()).unwrap_or(0)
    }

    /// Whether a snapshot from the leader replaced the state machine since the last call: the
    /// caller then restores it from [`Raft::snapshot`] before applying anything more.
    pub fn take_installed(&mut self) -> bool {
        std::mem::take(&mut self.installed)
    }

    /// A snapshot of committed state this node took is durable: it replaces the log up to its
    /// index, but for a tail of [`Config::log_tail_entries`] before it. One no newer than the
    /// latest (a snapshot from the leader overtook it) is ignored.
    ///
    /// # Panics
    ///
    /// If its index isn't committed, or its term isn't the log's there.
    pub fn on_snapshot_saved(&mut self, snapshot: Snapshot) {
        if snapshot.index <= self.snapshot_index() {
            return;
        }
        assert!(snapshot.index <= self.commit, "only committed state is snapshotted");
        assert_eq!(self.term_at(snapshot.index), Some(snapshot.term));
        assert_eq!(snapshot.members, self.members_at(snapshot.index), "a snapshot's members are those at its index");
        let to = self.log.compact_point(snapshot.index, self.config.log_tail_entries);
        self.base_term = self.term_at(to).expect("the compaction point is in the log");
        self.log.trim_to(to);
        // No change of members after `to` and up to the snapshot: they're the snapshot's.
        self.base_members.clone_from(&snapshot.members);
        self.out.persist.push(Persist::Compact { log: to, snapshot: snapshot.index });
        self.use_snapshot(snapshot);
    }

    /// The latest snapshot's bytes from `offset`, read for [`Outputs::snapshot_reads`]. They're
    /// kept until the next [`Raft::take_outputs`].
    pub fn on_snapshot_read(&mut self, index: u64, offset: u64, bytes: &[u8]) {
        if self.snapshot.as_ref().is_some_and(|s| s.index == index) {
            self.fetched_chunks.push((offset, bytes.to_vec()));
        }
    }

    /// A heartbeat, or a refused append, for a peer behind the snapshot: starts sending it the
    /// snapshot, or, if it acked nothing since the last heartbeat (a chunk was lost), sends again
    /// from what it has.
    pub(super) fn resume_snapshot(&mut self, peer: usize) {
        let index = self.snapshot.as_ref().expect("a log that starts past 1 has a snapshot").index;
        match &mut self.peers[peer].sending {
            Some(s) if s.index == index => {
                if !s.progressed {
                    s.sent = s.acked;
                }
                s.progressed = false;
            }
            sending => *sending = Some(Sending { index, acked: 0, sent: 0, progressed: false, rewound: None }),
        }
        self.send_snapshot_chunks(peer);
    }

    /// Sends a peer the snapshot's chunks from where it's sent to, as far as
    /// [`MAX_SNAPSHOT_IN_FLIGHT`] past what it has. Those not read back from disk yet are read,
    /// the rest of the window with them, and sent in the next round.
    pub(super) fn send_snapshot_chunks(&mut self, peer: usize) {
        self.peers[peer].chunk_wanted = false;
        let snapshot = self.snapshot.as_ref().expect("a log that starts past 1 has a snapshot");
        let Some(mut s) = self.peers[peer].sending.filter(|s| s.index == snapshot.index) else { return };
        let window = |s: &Sending| s.acked + MAX_SNAPSHOT_IN_FLIGHT;
        while s.sent < snapshot.len && s.sent < window(&s) {
            let Some((_, chunk)) =
                self.fetched_chunks.iter().find(|(o, _)| *o == s.sent).filter(|(_, c)| !c.is_empty())
            else {
                let mut offset = s.sent;
                while offset < snapshot.len && offset < window(&s) {
                    let read = SnapshotRead { index: snapshot.index, offset };
                    if !self.out.snapshot_reads.contains(&read) {
                        self.out.snapshot_reads.push(read);
                    }
                    offset += crate::widen(MAX_READ_BYTES);
                }
                self.peers[peer].chunk_wanted = true;
                break;
            };
            let addrs: Vec<wire::InstallSnapshotMember<'_>> = snapshot
                .members
                .iter()
                .map(|m| wire::InstallSnapshotMember {
                    id: m.id.0,
                    learner: u8::from(m.learner),
                    peer: m.peer.as_bytes(),
                    client: m.client.as_bytes(),
                    admin: m.admin.as_bytes(),
                })
                .collect();
            let message = wire::InstallSnapshot {
                term: self.meta.term,
                leader: self.config.id.0,
                last_index: snapshot.index,
                last_term: snapshot.term,
                offset: s.sent,
                done: u8::from(s.sent + crate::widen(chunk.len()) >= snapshot.len),
                checksum: snapshot.checksum,
                member: &addrs,
                chunk,
            };
            let mut frame = Vec::new();
            codec::encode_frame(&message, &mut frame).expect("a chunk is within a frame");
            s.sent += crate::widen(chunk.len());
            self.out.sends.push((self.peers[peer].id, frame));
        }
        self.peers[peer].sending = Some(s);
    }

    pub(super) fn on_snapshot_ack(&mut self, peer: usize, m: &wire::SnapshotAckRef<'_>, now: u64) {
        if m.term() != self.meta.term || self.role != Role::Leader {
            return;
        }
        self.peers[peer].heard_at = now;
        // An ack of a snapshot this leader has moved on from says nothing of how much the peer has
        // of this one.
        let Some(s) = self.peers[peer].sending.as_mut().filter(|s| s.index == m.index()) else { return };
        if m.offset() > s.acked {
            s.acked = m.offset();
            s.sent = s.sent.max(s.acked);
            s.progressed = true;
            self.send_snapshot_chunks(peer);
        } else if m.offset() == 0 && s.acked > 0 {
            // It has none of it (it restarted): from the start again. Any other ack that's behind
            // is a stale one, overtaken by those after it.
            (s.acked, s.sent, s.rewound) = (0, 0, None);
            self.send_snapshot_chunks(peer);
        } else if m.offset() == s.acked && s.sent > s.acked && s.rewound != Some(s.acked) {
            // It refused a chunk, out of order: one before it was lost or overtaken. Send again
            // from what it has, once for each place it stops.
            (s.sent, s.rewound) = (s.acked, Some(s.acked));
            self.send_snapshot_chunks(peer);
        }
    }

    /// A chunk of the leader's snapshot. Once whole, it replaces this node's log and state.
    pub(super) fn on_install_snapshot(&mut self, from: NodeId, m: &wire::InstallSnapshotRef<'_>, now: u64) {
        if m.term() < self.meta.term {
            self.ack(from, false, self.last_index() + 1, m.last_index(), 0);
            return;
        }
        if m.term() > self.meta.term || self.role != Role::Follower {
            self.become_follower(m.term(), now);
        }
        self.leader = Some(from);
        self.leader_seen_at = Some(now);
        self.reset_election(now);
        if m.last_index() <= self.commit {
            // Already has all it covers.
            self.ack(from, true, m.last_index(), m.last_index(), 0);
            return;
        }
        // A first chunk starts a snapshot, unless it's one already arriving: then it's a resend.
        if m.offset() == 0 && self.incoming.as_ref().is_none_or(|s| s.index != m.last_index()) {
            let members = m
                .member()
                .map(|a| Member::read(NodeId(a.id()), a.learner(), a.peer(), a.client(), a.admin()))
                .collect();
            let (index, term, checksum) = (m.last_index(), m.last_term(), m.checksum());
            self.incoming = Some(Snapshot { index, term, len: 0, checksum, members });
            self.incoming_sum = crate::codec::CHECKSUM_START;
        }
        let Some(incoming) = self.incoming.as_mut().filter(|s| s.index == m.last_index()) else {
            // Restarted, say: the leader starts again.
            self.send(from, &wire::SnapshotAck { term: self.meta.term, offset: 0, index: m.last_index() });
            return;
        };
        if m.offset() == incoming.len {
            let bytes = m.chunk().to_vec();
            incoming.len += crate::widen(bytes.len());
            self.incoming_sum = crate::codec::checksum_on(self.incoming_sum, &bytes);
            self.out.persist.push(Persist::SnapshotChunk { index: m.last_index(), offset: m.offset(), bytes });
        }
        if m.done() == 1 && m.offset() + crate::widen(m.chunk().len()) == incoming.len {
            let incoming = self.incoming.take().expect("just matched");
            if crate::snapshot::sealed(self.incoming_sum, &incoming) != incoming.checksum {
                // Damaged before it got here, in the leader's file, say: not this node's disk.
                // It's asked for again, from the start.
                self.send(from, &wire::SnapshotAck { term: self.meta.term, offset: 0, index: incoming.index });
                return;
            }
            self.install(incoming);
            self.ack(from, true, self.commit, m.last_index(), 0);
        } else {
            let ack = wire::SnapshotAck { term: self.meta.term, offset: incoming.len, index: m.last_index() };
            self.send(from, &ack);
        }
    }

    pub(super) fn install(&mut self, snapshot: Snapshot) {
        assert!(snapshot.index > self.commit);
        // A log that already has the snapshot's last entry keeps what follows it: this node may
        // have acknowledged those entries, and the leader counted them.
        let matches = self.term_at(snapshot.index) == Some(snapshot.term);
        if matches {
            self.log.trim_to(snapshot.index);
        } else {
            self.log = Entries::starting_at(snapshot.index + 1);
            self.durable = snapshot.index;
        }
        self.base_term = snapshot.term;
        self.base_members.clone_from(&snapshot.members);
        self.commit = snapshot.index;
        self.out.persist.push(Persist::InstallSnapshot { snapshot: snapshot.clone(), replace_log: !matches });
        self.trust(snapshot.term);
        self.use_snapshot(snapshot);
        self.installed = true;
        self.peers_changed();
    }

    /// Ships `snapshot` from now on. Reads of the one before are dropped: its file goes once
    /// this batch is durable. Peers waiting for a chunk are sent this one's, from the start.
    pub(super) fn use_snapshot(&mut self, snapshot: Snapshot) {
        self.snapshot = Some(snapshot);
        self.out.snapshot_reads.clear();
        self.fetched_chunks.clear();
    }
}
