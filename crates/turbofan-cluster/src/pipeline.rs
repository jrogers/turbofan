//! How a driver carries out a node's outputs without waiting on its disk: the rules, without the
//! IO, so the simulation runs the same ones as the driver.
//!
//! [`Pipeline::take`] takes the node's outputs. What can go at once does: a leader's appends,
//! frames for clients, and frames for peers if nothing is being written or waits to be. The rest
//! is queued as a batch, behind the write under way, if there is one. When there isn't, the
//! queued batches are written together, with one sync ([`Taken::write`]): that's the group commit.
//! Meanwhile the node goes on with its inputs, and more batches queue.
//!
//! Once the write is durable, [`Pipeline::written`] tells the node, and returns the frames for
//! peers that waited for it: those of its batches, which may promise what they persist, or what
//! a batch before them does. Then take outputs again.
//!
//! A batch that restores from a snapshot stops the inputs ([`Pipeline::takes_inputs`]) until it's
//! written and restored: a snapshot installed meanwhile would leave the one it restores from gone.

use std::io::{self, Read};

use crate::node::{ConnId, Node, SaveSnapshot, Service};
use crate::raft::{LogEnd, Member, NodeId, Persist, SnapshotRead};

/// What a write does for one batch, in order: persist, then read.
#[derive(Debug)]
pub struct Batch {
    /// Persist these, in order, after the batches before.
    pub persist: Vec<Persist>,
    /// Then read the log's records from these indices on: see [`crate::raft::Outputs::reads`].
    pub reads: Vec<u64>,
    /// And the latest snapshot from these offsets: see [`crate::raft::Outputs::snapshot_reads`].
    pub snapshot_reads: Vec<SnapshotRead>,
}

/// What waits for a batch's write.
#[derive(Debug)]
struct Waiting {
    restore: Option<u64>,
    peer_sends: Vec<(NodeId, Vec<u8>)>,
    /// [`crate::raft::Raft::log_end`] as the batch was taken.
    log_end: LogEnd,
}

/// One take of the node's outputs: what to do at once.
#[derive(Debug)]
pub struct Taken {
    /// A leader's appends, for peers.
    pub peer_appends: Vec<(NodeId, Vec<u8>)>,
    /// Frames for clients.
    pub client_sends: Vec<(ConnId, Vec<u8>)>,
    /// Frames for peers that wait for nothing.
    pub peer_sends: Vec<(NodeId, Vec<u8>)>,
    /// See [`crate::node::Outputs::save`].
    pub save: Option<SaveSnapshot>,
    /// See [`crate::node::Outputs::peers`].
    pub peers: Option<Vec<Member>>,
    /// See [`crate::node::Outputs::latencies`].
    pub latencies: Vec<(crate::raft::Stage, u64)>,
    /// Write these, each batch's reads after its operations, then sync; then, with what was read,
    /// [`Pipeline::written`].
    pub write: Option<Vec<Batch>>,
}

/// A node's batches, between being taken and durable.
#[derive(Debug, Default)]
pub struct Pipeline {
    /// What waits for the write under way, if one is.
    writing: Option<Vec<Waiting>>,
    queued: Vec<(Batch, Waiting)>,
    restore_waits: bool,
}

impl Pipeline {
    /// Takes the node's outputs.
    pub fn take<S: Service>(&mut self, node: &mut Node<S>) -> Taken {
        let out = node.take_outputs();
        let log_end = node.raft().log_end();
        let disk = !out.persist.is_empty() || !out.reads.is_empty() || !out.snapshot_reads.is_empty();
        let batch = Batch { persist: out.persist, reads: out.reads, snapshot_reads: out.snapshot_reads };
        let waiting = Waiting { restore: out.restore, peer_sends: out.peer_sends, log_end };
        let mut peer_sends = Vec::new();
        if !disk && waiting.restore.is_none() && self.writing.is_none() && self.queued.is_empty() {
            // Nothing new to make durable, nor waiting to be.
            peer_sends = waiting.peer_sends;
        } else if disk || waiting.restore.is_some() || !waiting.peer_sends.is_empty() {
            self.restore_waits |= waiting.restore.is_some();
            self.queued.push((batch, waiting));
        }
        let write = (self.writing.is_none() && !self.queued.is_empty()).then(|| {
            let (batches, waiting) = std::mem::take(&mut self.queued).into_iter().unzip();
            self.writing = Some(waiting);
            batches
        });
        Taken {
            peer_appends: out.peer_appends,
            client_sends: out.client_sends,
            peer_sends,
            save: out.save,
            peers: out.peers,
            latencies: out.latencies,
            write,
        }
    }

    /// The write under way is durable, having read `reads` and `chunks` for it, in order: tells
    /// the node, restoring it `from` the snapshot [`Pipeline::restore`] names, and returns the
    /// frames for peers that waited for it. Take outputs next.
    ///
    /// # Errors
    ///
    /// If restoring fails: see [`Node::restore`].
    ///
    /// # Panics
    ///
    /// If no write is under way, or it restores and there's no `from`.
    pub fn written<S: Service>(
        &mut self,
        node: &mut Node<S>,
        reads: &[Vec<u8>],
        chunks: &[(u64, u64, Vec<u8>)],
        from: Option<&mut dyn Read>,
        now: u64,
    ) -> io::Result<Vec<(NodeId, Vec<u8>)>> {
        let waiting = self.writing.take().expect("a write is under way");
        for records in reads {
            node.on_read(records);
        }
        for (index, offset, bytes) in chunks {
            node.on_snapshot_read(*index, *offset, bytes);
        }
        if waiting.iter().any(|w| w.restore.is_some()) {
            node.restore(from.expect("the snapshot to restore from"))?;
            self.restore_waits = false;
        }
        node.on_persisted(waiting.last().expect("a write has batches").log_end, now);
        Ok(waiting.into_iter().flat_map(|w| w.peer_sends).collect())
    }

    /// The index of the snapshot the write under way restores from, once it's durable: load it,
    /// for [`Pipeline::written`]. Every batch taken while a restore waits carries it: it's one.
    #[must_use]
    pub fn restore(&self) -> Option<u64> {
        self.writing.iter().flatten().find_map(|w| w.restore)
    }

    /// A write is under way.
    #[must_use]
    pub fn writing(&self) -> bool {
        self.writing.is_some()
    }

    /// Give the node inputs: unless a restore waits for a write.
    #[must_use]
    pub fn takes_inputs(&self) -> bool {
        !self.restore_waits
    }
}
