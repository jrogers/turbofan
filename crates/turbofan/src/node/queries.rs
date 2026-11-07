//! Queries: a session's reads of the state, answered without logging them ([`Service::on_query`]).
//! The leader asks Raft to confirm each one ([`Raft::read_index`]), which hands it back with the
//! commit index its answer must reflect, then answers it once it has applied that far. A query is
//! kept out of the session's egress ring and never replayed: one dropped (the leader stepped
//! down, or the connection went) is asked again by its client.
//!
//! Only from protocol [`QUERY_PROTOCOL`], once every member echoes the rounds that confirm reads
//! (`readRound`): before, an older voter's acks would never confirm one.

use super::*;

/// The protocol from which a leader takes queries ([`PROTOCOL`]).
pub const QUERY_PROTOCOL: u32 = 2;
/// The most bytes of queries waiting on a leader, to be confirmed or applied up to: a round trip's
/// worth from many clients. Past it, a query is refused as busy, for its client to ask again.
pub const MAX_WAITING_QUERY_BYTES: usize = 16 << 20;

const _: () = assert!(MAX_WAITING_QUERY_BYTES >= MAX_PAYLOAD_BYTES);

/// A query waiting: for Raft to confirm its read, then for the state to apply up to its index.
#[derive(Debug)]
pub(super) struct Waiting {
    conn: ConnId,
    session: SessionId,
    query_id: u64,
    payload: Vec<u8>,
}

impl<S: Service> Node<S> {
    /// A query from `session`, attached on `conn`.
    pub(super) fn query(&mut self, conn: ConnId, session: SessionId, query_id: u64, payload: &[u8]) {
        self.local.activity.insert(session, self.now);
        if self.state.protocol < QUERY_PROTOCOL {
            self.refuse_query(conn, session, query_id, true);
            return;
        }
        let read = self.next_read;
        if self.local.query_bytes + payload.len() > MAX_WAITING_QUERY_BYTES || !self.raft.read_index(read) {
            self.refuse_query(conn, session, query_id, false);
            return;
        }
        self.next_read += 1;
        self.local.query_bytes += payload.len();
        let waiting = Waiting { conn, session, query_id, payload: payload.to_vec() };
        self.local.queries.insert(read, waiting);
    }

    /// Reads Raft confirmed: each is answered once the state has applied up to its index.
    pub(super) fn on_confirmed_reads(&mut self, reads: &[raft::ReadIndex]) {
        for read in reads {
            debug_assert!(self.local.answerable.back().is_none_or(|&(index, _)| index <= read.index));
            self.local.answerable.push_back((read.index, read.id));
        }
        self.answer_queries();
    }

    /// Answers the queries confirmed at an index applied, in order.
    pub(super) fn answer_queries(&mut self) {
        if self.restoring.is_some() {
            return;
        }
        while let Some(&(_, read)) = self.local.answerable.front().filter(|&&(index, _)| index <= self.state.applied) {
            self.local.answerable.pop_front();
            // Gone with its connection.
            let Some(waiting) = self.local.queries.remove(&read) else { continue };
            self.local.query_bytes -= waiting.payload.len();
            let attached = self.local.conns.get(&waiting.conn) == Some(&Some(waiting.session));
            if !attached || !self.state.sessions.contains_key(&waiting.session) {
                continue;
            }
            let Waiting { conn, session, query_id, payload } = waiting;
            match self.service.on_query(session, &payload) {
                Some(answer) if answer.len() <= MAX_PAYLOAD_BYTES => {
                    let answer = wire::QueryAnswer { session: session.0, query_id, payload: &answer };
                    self.send_client(conn, &answer);
                }
                // An answer too long to send would be as long again: the service's to fix.
                _ => self.refuse_query(conn, session, query_id, true),
            }
        }
    }

    /// The queries waiting on `conn`, which closed, are dropped.
    pub(super) fn drop_queries(&mut self, conn: ConnId) {
        let mut freed = 0;
        self.local.queries.retain(|_, w| {
            let keep = w.conn != conn;
            if !keep {
                freed += w.payload.len();
            }
            keep
        });
        self.local.query_bytes -= freed;
    }

    fn refuse_query(&mut self, conn: ConnId, session: SessionId, query_id: u64, unsupported: bool) {
        let refused = wire::QueryRefused { session: session.0, query_id, unsupported: u8::from(unsupported) };
        self.send_client(conn, &refused);
    }

    /// Whether this node takes a `Query`, as `Opened` says.
    pub(super) fn takes_queries(&self) -> u8 {
        u8::from(self.config.protocol >= QUERY_PROTOCOL)
    }
}
