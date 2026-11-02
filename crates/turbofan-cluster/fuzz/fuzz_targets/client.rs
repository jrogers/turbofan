//! A single-node cluster fed fuzzed client and admin steps: any connection sending `Connect`,
//! `Reattach`, `Message`, `KeepAlive` or `Close` for any session and sequence number, dropping,
//! snapshot requests, and time passing (so sessions time out and timers fire). Clients are
//! untrusted, so nothing may panic; every frame the node sends must decode; and the service must
//! see each session's messages once and in order, whatever the client sends.

#![no_main]

use std::collections::BTreeMap;
use std::io::{self, Read};

use arbitrary::Arbitrary;
use libfuzzer_sys::fuzz_target;
use turbofan_cluster::meta::Meta;
use turbofan_cluster::node::{self, ConnId, Node};
use turbofan_cluster::{CloseReason, Ctx, Service, SessionId, SnapshotWriter, codec, raft, wire};
use turbofan_sbe::Encode;

const MS: u64 = 1_000_000;

#[derive(Debug, Arbitrary)]
enum Step {
    Connect { conn: u8 },
    Reattach { conn: u8, session: u8, last_egress_seq: u64 },
    // `op` is what the service does with it: see `Checker::on_message`.
    Message { conn: u8, session: u8, client_seq: u64, op: u8, arg: u8 },
    KeepAlive { conn: u8, session: u8 },
    Close { conn: u8, session: u8 },
    Disconnect { conn: u8 },
    Snapshot { conn: u8, request_id: u8 },
    Tick { ms: u16 },
}

/// Checks each session's messages arrive once and in order: a message's payload starts with
/// its `client_seq`, which the node checks but the service never sees.
#[derive(Debug, Default)]
struct Checker {
    last_seq: BTreeMap<SessionId, u64>,
}

impl Service for Checker {
    fn on_session_open(&mut self, _ctx: &mut Ctx, session: SessionId, _principal: &[u8]) {
        assert!(self.last_seq.insert(session, 0).is_none(), "session {session} opened twice");
    }
    fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, _reason: CloseReason) {
        assert!(self.last_seq.remove(&session).is_some(), "session {session} closed but not open");
    }
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let seq = u64::from_le_bytes(payload[..8].try_into().unwrap());
        let last = self.last_seq.get_mut(&session).expect("messages are for open sessions");
        assert_eq!(seq, *last + 1, "session {session} out of order");
        *last = seq;
        let (op, arg) = (payload[8], u64::from(payload[9]));
        match op % 4 {
            0 => ctx.schedule_timer(arg, ctx.now() + arg * MS),
            1 => ctx.cancel_timer(arg),
            2 => ctx.close_session(session),
            _ => {}
        }
        ctx.send(session, payload).expect("echoes a client payload");
    }
    fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
        if let Some(&session) = self.last_seq.keys().next() {
            ctx.send(session, &id.to_le_bytes()).expect("8 bytes");
        }
    }
    fn snapshot(&self) -> SnapshotWriter {
        let last_seq = self.last_seq.clone();
        Box::new(move |out| {
            for (session, seq) in &last_seq {
                out.write_all(&session.to_le_bytes())?;
                out.write_all(&seq.to_le_bytes())?;
            }
            Ok(())
        })
    }
    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let word = |c: &[u8]| u64::from_le_bytes(c.try_into().unwrap());
        self.last_seq = bytes.chunks(16).map(|c| (word(&c[..8]), word(&c[8..]))).collect();
        Ok(())
    }
}

fn frame(message: &impl Encode) -> Vec<u8> {
    let mut frame = Vec::new();
    codec::encode_frame(message, &mut frame).unwrap();
    frame
}

/// Carries out the outputs as the driver does, a lone member's writes durable at once.
fn drive(node: &mut Node<Checker>) {
    loop {
        let out = node.take_outputs();
        for (_, frame) in &out.client_sends {
            let decoded = codec::decode_frame(frame).expect("the node sends valid frames");
            assert_eq!(decoded.map(|(_, used)| used), Some(frame.len()), "one whole frame");
        }
        if let Some(save) = out.save {
            let mut bytes = Vec::new();
            (save.writer)(&mut bytes).unwrap();
            let len = bytes.len() as u64;
            let (index, term, members) = (save.index, save.term, save.members);
            node.on_snapshot_saved(raft::Snapshot { index, term, len, checksum: 0, members });
            continue;
        }
        if out.persist.is_empty() {
            return;
        }
        node.on_persisted(node.raft().log_end());
    }
}

fuzz_target!(|steps: Vec<Step>| {
    let config = node::Config {
        raft: raft::Config {
            id: 1,
            members: vec![raft::Member { id: 1, peer: "p1".into(), client: "n1".into(), admin: "a1".into() }],
            election_timeout_ns: 150 * MS,
            heartbeat_ns: 50 * MS,
            seed: 1,
            log_window_bytes: usize::MAX,
        },
        session_timeout_ns: 1000 * MS,
        // Small, so snapshots and a full egress ring are within reach.
        snapshot_every_entries: 16,
        egress_ring_entries: 4,
        protocol: node::PROTOCOL,
    };
    let mut node = Node::new(config, Checker::default(), Meta::default(), None, raft::Entries::default(), 0);
    let mut now = node.next_deadline();
    node.on_tick(now);
    drive(&mut node);

    for step in steps {
        let client = |node: &mut Node<Checker>, conn: u8, frame: Vec<u8>, now| {
            let (message, _) = codec::decode_frame(&frame).unwrap().unwrap();
            node.on_client(ConnId::from(conn), &message, now);
        };
        match step {
            Step::Connect { conn } => client(&mut node, conn, frame(&wire::Connect { credentials: b"" }), now),
            Step::Reattach { conn, session, last_egress_seq } => {
                let session = u64::from(session);
                client(&mut node, conn, frame(&wire::Reattach { session, last_egress_seq, credentials: b"" }), now);
            }
            Step::Message { conn, session, client_seq, op, arg } => {
                let mut payload = client_seq.to_le_bytes().to_vec();
                payload.extend([op, arg]);
                let message = wire::Message { session: u64::from(session), client_seq, payload: &payload };
                client(&mut node, conn, frame(&message), now);
            }
            Step::KeepAlive { conn, session } => {
                client(&mut node, conn, frame(&wire::KeepAlive { session: u64::from(session) }), now);
            }
            Step::Close { conn, session } => {
                client(&mut node, conn, frame(&wire::Close { session: u64::from(session) }), now);
            }
            Step::Disconnect { conn } => node.on_disconnect(ConnId::from(conn)),
            Step::Snapshot { conn, request_id } => {
                let frame = frame(&wire::SnapshotRequest { request_id: u64::from(request_id) });
                node.on_admin(ConnId::from(conn), &codec::decode_frame(&frame).unwrap().unwrap().0, now);
            }
            Step::Tick { ms } => {
                now += u64::from(ms) * MS;
                node.on_tick(now);
            }
        }
        drive(&mut node);
    }
});
