//! What released versions wrote, read by this one: for each in `tests/golden/<version>/`, a
//! snapshot (a backup, to restore from) and the client protocol's frames, which old and new
//! clients and nodes exchange during an upgrade. A snapshot must restore with its sessions,
//! state and epoch, and every frame decode to what it said. From 0.12.0, also what a rolling
//! upgrade has a new node read: an old node's data directory (`data/`: its log, meta file and
//! snapshots, and `data.txt`, what they held), which must read to the same entries, term and vote,
//! and which a node of this version must serve from; and the peer protocol's frames
//! (`peers.bin`), which old and new members exchange. See WIRE.md.
//!
//! At a release, `GOLDEN_WRITE=1 cargo test -p turbofan --test golden` writes the
//! version's own, from a one-node cluster over TCP, to check in; `GOLDEN_OUT=<dir>` writes them
//! there instead. The writer must keep working against older code too, as it was run there for
//! the versions before it was written: 0.12.0's data directory and peer frames were written from
//! its tag.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use turbofan::client::Event;
use turbofan::meta::MetaFile;
use turbofan::node::{self, Node};
use turbofan::raft::{self, NodeId, Role};
use turbofan::{Admin, CloseReason, ClusterClient, Ctx, Service, SessionId, SnapshotWriter, codec, driver};
use turbofan::{snapshot, wire};

const SERVICE_ID: u32 = 7;
/// Messages the golden session sent, each answered with the count so far and given a timer.
const MESSAGES: u64 = 5;
/// Messages it sent after the snapshot, which the data directory's log holds, the first a timer
/// due at once.
const AFTER: u64 = 2;

/// Counts each session's messages, answering each with the count, and schedules a timer for
/// each, far off, so the snapshot holds sessions, responses, timers and the service's state.
#[derive(Debug, Default)]
struct Tally {
    counts: BTreeMap<SessionId, u64>,
}

impl Service for Tally {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let count = self.counts.entry(session).or_default();
        *count += 1;
        ctx.send(session, count.to_string().into_bytes()).expect("a count is short");
        // `soon`: one due at once, for the log to hold its firing.
        let at = if payload == b"soon" { ctx.now() } else { u64::MAX / 2 };
        ctx.schedule_timer(*count, at);
    }

    fn snapshot(&self) -> SnapshotWriter {
        let counts = self.counts.clone();
        Box::new(move |out| {
            for (session, count) in &counts {
                out.write_all(&session.0.to_le_bytes())?;
                out.write_all(&count.to_le_bytes())?;
            }
            Ok(())
        })
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let words: Vec<u64> = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w)).collect();
        self.counts = words.as_chunks::<2>().0.iter().map(|&[session, count]| (SessionId(session), count)).collect();
        Ok(())
    }

    /// The count of the session the query names, in decimal.
    fn on_query(&self, _session: SessionId, query: &[u8]) -> Option<Vec<u8>> {
        let session = SessionId(std::str::from_utf8(query).ok()?.parse().ok()?);
        Some(self.counts.get(&session).copied().unwrap_or(0).to_string().into_bytes())
    }
}

/// The client protocol's messages, both ways, each with every field set.
fn frames() -> Vec<u8> {
    fn put(out: &mut Vec<u8>, message: &impl turbofan_sbe::Encode) {
        codec::encode_frame(message, out).unwrap();
    }
    let mut out = Vec::new();
    let role = wire::Role::Client;
    put(&mut out, &wire::Hello { magic: codec::MAGIC, service_id: SERVICE_ID, role, node_id: 0, epoch: 0, token: b"" });
    put(&mut out, &wire::Connect { credentials: b"desk" });
    put(&mut out, &wire::Reattach { session: 9, last_egress_seq: 4, epoch: 3, credentials: b"desk" });
    put(&mut out, &wire::Message { session: 9, client_seq: 5, payload: b"order" });
    put(&mut out, &wire::KeepAlive { session: 9 });
    put(&mut out, &wire::Close { session: 9 });
    put(&mut out, &wire::Query { session: 9, query_id: 6, payload: b"book" });
    put(&mut out, &wire::Redirect { leader: 2, addr: b"10.0.0.2:7101" });
    put(&mut out, &wire::Opened { session: 9, applied_seq: 5, last_egress_seq: 4, epoch: 3 });
    put(&mut out, &wire::Egress { session: 9, egress_seq: 5, applied_seq: 5, payload: b"fill" });
    put(&mut out, &wire::KeepAliveAck { applied_seq: 5 });
    put(&mut out, &wire::Closed { session: 9, reason: CloseReason::Timeout });
    put(&mut out, &wire::QueryAnswer { session: 9, query_id: 6, payload: b"bids" });
    put(&mut out, &wire::QueryRefused { session: 9, query_id: 7, unsupported: 1 });
    out
}

/// What [`frames`] wrote, as this version reads it. A version before 0.2.0 (schema version 4)
/// didn't say the session's last response in `Opened`, nor one before 0.4.0 (schema version 5)
/// the epoch in `Opened` and `Reattach`, nor one before 0.9.0 (schema version 8) queries.
fn expected(version: &str) -> Vec<String> {
    let minor: u32 = version.split('.').nth(1).unwrap().parse().unwrap();
    let last_egress = if minor < 2 { "None" } else { "Some(4)" };
    let epoch = if minor < 4 { "None" } else { "Some(3)" };
    let queries = minor >= 9;
    let mut expected: Vec<String> = vec![
        // Secrets are kept out of debug output: they're checked apart.
        "Hello(HelloRef { magic: 1414745155, service_id: 7, role: Client, node_id: 0, epoch: 0, token: *** })".into(),
        "Connect(ConnectRef { credentials: *** })".into(),
        format!("Reattach(ReattachRef {{ session: 9, last_egress_seq: 4, epoch: {epoch}, credentials: *** }})"),
        "Message(MessageRef { session: 9, client_seq: 5, payload: [111, 114, 100, 101, 114] })".into(),
        "KeepAlive(KeepAliveRef { session: 9 })".into(),
        "Close(CloseRef { session: 9 })".into(),
        "Query(QueryRef { session: 9, query_id: 6, payload: [98, 111, 111, 107] })".into(),
        "Redirect(RedirectRef { leader: 2, addr: [49, 48, 46, 48, 46, 48, 46, 50, 58, 55, 49, 48, 49] })".into(),
        format!("Opened(OpenedRef {{ session: 9, applied_seq: 5, last_egress_seq: {last_egress}, epoch: {epoch} }})"),
        "Egress(EgressRef { session: 9, egress_seq: 5, applied_seq: 5, payload: [102, 105, 108, 108] })".into(),
        "KeepAliveAck(KeepAliveAckRef { applied_seq: 5 })".into(),
        "Closed(ClosedRef { session: 9, reason: Timeout })".into(),
        "QueryAnswer(QueryAnswerRef { session: 9, query_id: 6, payload: [98, 105, 100, 115] })".into(),
        "QueryRefused(QueryRefusedRef { session: 9, query_id: 7, unsupported: 1 })".into(),
    ];
    if !queries {
        expected.retain(|m| !m.starts_with("Query"));
    }
    expected
}

/// The peer protocol's messages, each with every field set.
fn peer_frames() -> Vec<u8> {
    fn put(out: &mut Vec<u8>, message: &impl turbofan_sbe::Encode) {
        codec::encode_frame(message, out).unwrap();
    }
    let mut out = Vec::new();
    let role = wire::Role::Peer;
    put(&mut out, &wire::Hello { magic: codec::MAGIC, service_id: SERVICE_ID, role, node_id: 2, epoch: 3, token: b"" });
    put(
        &mut out,
        &wire::RequestVote { term: 4, candidate: 2, last_index: 9, last_term: 3, pre_vote: 1, round: 5, handover: 1 },
    );
    put(&mut out, &wire::Vote { term: 4, granted: 1, pre_vote: 1, doubt: 1, round: 5 });
    let (prev_index, prev_term, commit, last_index, read_round) = (8, 3, 7, 9, 6);
    let entries = b"entries";
    put(
        &mut out,
        &wire::AppendEntries {
            term: 4,
            leader: 1,
            prev_index,
            prev_term,
            commit,
            count: 1,
            last_index,
            read_round,
            entries,
        },
    );
    put(
        &mut out,
        &wire::AppendAck {
            term: 4,
            success: 1,
            index: 9,
            snapshot_index: 5,
            protocol: 1,
            service_version: 2,
            prev_index,
            read_round,
            lease_window: 8,
        },
    );
    let member = [wire::InstallSnapshotMember { id: 1, learner: 1, peer: b"p", client: b"c", admin: b"a" }];
    put(
        &mut out,
        &wire::InstallSnapshot {
            term: 4,
            leader: 1,
            last_index: 9,
            last_term: 3,
            offset: 10,
            done: 1,
            checksum: 11,
            member: &member,
            chunk: b"chunk",
        },
    );
    put(&mut out, &wire::SnapshotAck { term: 4, offset: 10, index: 9 });
    put(&mut out, &wire::TimeoutNow { term: 4 });
    put(&mut out, &wire::Stop { term: 4, index: 9 });
    out
}

/// What [`peer_frames`] wrote, as this version reads it: after 0.14.0, a vote's `handover` and an
/// ack's `leaseWindow` too.
fn peer_expected(version: &str) -> Vec<String> {
    let release: Vec<u32> = version.split('.').map(|n| n.parse().unwrap()).collect();
    let (handover, lease_window) = if release <= vec![0, 14, 0] { ("None", "None") } else { ("Some(1)", "Some(8)") };
    let frames = [
        "Hello(HelloRef { magic: 1414745155, service_id: 7, role: Peer, node_id: 2, epoch: 3, token: *** })",
        "RequestVote(RequestVoteRef { term: 4, candidate: 2, last_index: 9, last_term: 3, pre_vote: 1, round: Some(5), handover: HANDOVER })",
        "Vote(VoteRef { term: 4, granted: 1, pre_vote: 1, doubt: Some(1), round: Some(5) })",
        "AppendEntries(AppendEntriesRef { term: 4, leader: 1, prev_index: 8, prev_term: 3, commit: 7, count: 1, \
     last_index: Some(9), read_round: Some(6), entries: [101, 110, 116, 114, 105, 101, 115] })",
        "AppendAck(AppendAckRef { term: 4, success: 1, index: 9, snapshot_index: 5, protocol: 1, service_version: 2, \
     prev_index: Some(8), read_round: Some(6), lease_window: LEASE_WINDOW })",
        "InstallSnapshot(InstallSnapshotRef { term: 4, leader: 1, last_index: 9, last_term: 3, offset: 10, done: 1, \
     checksum: 11, member: [InstallSnapshotMemberRef { id: 1, learner: 1, peer: [112], client: [99], admin: [97] }], \
     chunk: [99, 104, 117, 110, 107] })",
        "SnapshotAck(SnapshotAckRef { term: 4, offset: 10, index: 9 })",
        "TimeoutNow(TimeoutNowRef { term: 4 })",
        "Stop(StopRef { term: 4, index: 9 })",
    ];
    frames.iter().map(|f| f.replace("HANDOVER", handover).replace("LEASE_WINDOW", lease_window)).collect()
}

/// Every version's golden files: its version and directory.
fn versions() -> Vec<(String, PathBuf)> {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden");
    let mut versions: Vec<(String, PathBuf)> = std::fs::read_dir(&root)
        .unwrap()
        .map(|e| e.unwrap().path())
        .map(|p| (p.file_name().unwrap().to_str().unwrap().to_owned(), p))
        .collect();
    versions.sort();
    assert!(!versions.is_empty(), "no golden files in {}", root.display());
    versions
}

#[test]
fn every_versions_client_frames_decode() {
    for (version, dir) in versions() {
        let bytes = std::fs::read(dir.join("frames.bin")).unwrap();
        let mut decoded = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            let (message, used) = codec::decode_frame(&bytes[at..]).unwrap().unwrap();
            match &message {
                wire::Decoded::Hello(m) => assert_eq!(m.token(), b"", "{version}"),
                wire::Decoded::Connect(m) => assert_eq!(m.credentials(), b"desk", "{version}"),
                wire::Decoded::Reattach(m) => assert_eq!(m.credentials(), b"desk", "{version}"),
                _ => {}
            }
            decoded.push(format!("{message:?}"));
            at += used;
        }
        assert_eq!(decoded, expected(&version), "{version}");
    }
}

/// Each version's snapshot restores into a node of this one: its state, its epoch, and its
/// session, which a client reattaches to, and is sent the responses it kept.
#[test]
fn every_versions_snapshot_restores() {
    for (version, dir) in versions() {
        let (snapshot, mut bytes) = snapshot::open(&dir.join("snapshot")).unwrap();
        let session: u64 = std::fs::read_to_string(dir.join("session")).unwrap().trim().parse().unwrap();
        let config = node::Config::new(NodeId(1), Vec::new());
        let (index, members) = (snapshot.index, snapshot.members.clone());
        let mut node = Node::new(config, Tally::default(), Default::default(), Some(snapshot), Default::default(), 0);
        node.restore(&mut bytes).unwrap();
        assert_eq!(node.applied(), index, "{version}");
        assert_ne!(node.epoch(), 0, "{version}: its epoch");
        assert_eq!(members.iter().map(|m| m.id).collect::<Vec<_>>(), [NodeId(1)], "{version}");
        assert_eq!(node.service().counts, BTreeMap::from([(SessionId(session), MESSAGES)]), "{version}");

        // The one member: it leads, and takes the client back.
        let mut now = 0;
        while node.raft().role() != Role::Leader {
            now = node.next_deadline().max(now + 1);
            node.on_tick(now);
            drive(&mut node, now);
        }
        let reattach = wire::Reattach { session, last_egress_seq: 0, epoch: node.epoch(), credentials: b"" };
        let mut frame = Vec::new();
        codec::encode_frame(&reattach, &mut frame).unwrap();
        node.on_client(1, &codec::decode_frame(&frame).unwrap().unwrap().0, now);
        let sent: Vec<String> = node
            .take_outputs()
            .client_sends
            .iter()
            .map(|(_, f)| format!("{:?}", codec::decode_frame(f).unwrap().unwrap().0))
            .collect();
        let last = format!("last_egress_seq: Some({MESSAGES})");
        assert!(sent[0].contains(&format!("applied_seq: {MESSAGES}, {last}")), "{version}: {sent:?}");
        let replayed: Vec<String> = (1..=MESSAGES).map(|k| format!("egress_seq: {k}")).collect();
        assert_eq!(sent.len(), 1 + replayed.len(), "{version}: {sent:?}");
        assert!(sent[1..].iter().zip(&replayed).all(|(s, r)| s.contains(r)), "{version}: {sent:?}");
    }
}

/// Each version's peer frames, from 0.12.0, decode to what they said.
#[test]
fn every_versions_peer_frames_decode() {
    for (version, dir) in versions() {
        let Ok(bytes) = std::fs::read(dir.join("peers.bin")) else { continue };
        let mut decoded = Vec::new();
        let mut at = 0;
        while at < bytes.len() {
            let (message, used) = codec::decode_frame(&bytes[at..]).unwrap().unwrap();
            if let wire::Decoded::Hello(m) = &message {
                assert_eq!(m.token(), b"", "{version}");
            }
            decoded.push(format!("{message:?}"));
            at += used;
        }
        assert_eq!(decoded, peer_expected(&version), "{version}");
    }
}

/// Each version's data directory, from 0.12.0, reads as it did: its term and vote, and its log's
/// entries. And a node of this version serves from it: the golden session's count as it was, and
/// a new session's message.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn every_versions_data_directory_reads_and_serves() {
    for (version, dir) in versions() {
        let Ok(held) = std::fs::read_to_string(dir.join("data.txt")) else { continue };
        let (session, expected) = held.split_once('\n').unwrap();
        let session = session.strip_prefix("session ").unwrap();
        let data = tempfile::tempdir().unwrap();
        copy_dir(&dir.join("data"), data.path());
        assert_eq!(read_data(data.path()), expected, "{version}");

        let ports = ports();
        let me = raft::Member::new(NodeId(1), ports[0].to_string(), ports[1].to_string(), ports[2].to_string());
        let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
        let config = driver_config(me, data.path(), &ports);
        let node = tokio::spawn(driver::serve_until(config, Tally::default(), async { drop(stopped.await) }));
        let mut client =
            ClusterClient::connect([ports[1].to_string()], SERVICE_ID, b"".to_vec(), Duration::from_secs(1)).unwrap();
        client.send(b"order".to_vec()).await.unwrap();
        let answer = loop {
            match tokio::time::timeout(Duration::from_secs(20), client.recv()).await.expect("answered in time") {
                Some(Event::Message(payload)) => break payload,
                Some(Event::Opened(_)) => {}
                event => panic!("{version}: {event:?}"),
            }
        };
        assert_eq!(answer, b"1", "{version}: a new session's first");
        let count = client.query(session.as_bytes().to_vec()).await.unwrap();
        assert_eq!(count, (MESSAGES + AFTER).to_string().into_bytes(), "{version}: the golden session's");
        stop.send(()).unwrap();
        assert!(node.await.unwrap().is_ok(), "{version}: stopped cleanly");
    }
}

/// What a data directory holds, as this version reads it: its term and vote, and its log's
/// entries, each's index, term and type.
fn read_data(dir: &Path) -> String {
    let (_, meta) = MetaFile::open(&dir.join("meta")).unwrap();
    let mut text = format!("term {} vote {:?}\n", meta.term, meta.vote.map(|v| v.0));
    turbofan::log::Log::open(&dir.join("log"), driver::Fsync::Never, |record| {
        let (entry, _) = wire::decode(record.payload).unwrap();
        let entry = format!("{entry:?}");
        let name = entry.split('(').next().unwrap();
        text.push_str(&format!("{} {} {name}\n", record.index, record.term));
    })
    .unwrap();
    text
}

/// `from`'s files into `to`, as deep, but for a lock, which a node takes on its directory.
fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let name = path.file_name().unwrap();
        if path.is_dir() {
            copy_dir(&path, &to.join(name));
        } else if name != "lock" {
            std::fs::copy(&path, to.join(name)).unwrap();
        }
    }
}

/// Free ports, all held until each is read, so they differ: peer, client, admin.
fn ports() -> Vec<SocketAddr> {
    let listeners: Vec<TcpListener> = (0..3).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

/// Carries out a node's outputs as a driver would, with nothing to save, until it has none.
fn drive(node: &mut Node<Tally>, now: u64) {
    loop {
        let out = node.take_outputs();
        assert!(out.save.is_none() && out.reads.is_empty(), "nothing to save or read back");
        if out.persist.is_empty() {
            return;
        }
        node.on_persisted(node.raft().log_end(), now);
    }
}

/// Writes this version's golden files, with `GOLDEN_WRITE=1`: a snapshot of a one-node cluster
/// whose one session sent [`MESSAGES`], that session's ID, and [`frames`]; then, after a learner
/// added and removed and [`AFTER`] more messages, the node's data directory once it stopped, and
/// what it held; and [`peer_frames`].
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_this_versions_golden_files() {
    if std::env::var_os("GOLDEN_WRITE").is_none() {
        return;
    }
    let out = std::env::var_os("GOLDEN_OUT").map_or_else(
        || Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden").join(env!("CARGO_PKG_VERSION")),
        PathBuf::from,
    );
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("frames.bin"), frames()).unwrap();
    std::fs::write(out.join("peers.bin"), peer_frames()).unwrap();

    let ports = ports();
    let me = raft::Member::new(NodeId(1), ports[0].to_string(), ports[1].to_string(), ports[2].to_string());
    let data = tempfile::tempdir().unwrap();
    let config = driver_config(me, data.path(), &ports);
    let (stop, stopped) = tokio::sync::oneshot::channel::<()>();
    let node = tokio::spawn(driver::serve_until(config, Tally::default(), async { drop(stopped.await) }));

    let mut client =
        ClusterClient::connect([ports[1].to_string()], SERVICE_ID, b"".to_vec(), Duration::from_secs(1)).unwrap();
    for _ in 0..MESSAGES {
        client.send(b"order".to_vec()).await.unwrap();
    }
    let (mut session, mut answered) = (None, 0);
    while answered < MESSAGES {
        match client.recv().await.unwrap() {
            Event::Opened(id) => session = Some(id),
            Event::Message(_) => answered += 1,
            event => panic!("{event:?}"),
        }
    }
    let admin = Admin::new(ports[2].to_string(), SERVICE_ID);
    admin.request_snapshot().await.unwrap();
    let newest = std::fs::read_dir(data.path().join("snapshots"))
        .unwrap()
        .map(|e| e.unwrap().path())
        .filter(|p| p.extension().is_some_and(|e| e == "snapshot"))
        .max()
        .unwrap();
    std::fs::copy(newest, out.join("snapshot")).unwrap();
    let session = session.unwrap().0;
    std::fs::write(out.join("session"), format!("{session}\n")).unwrap();

    // For the log past the snapshot: a change of members each way, a learner's, a node started to
    // be added; and more messages.
    let theirs = self::ports();
    let mut learner = raft::Member::new(NodeId(2), theirs[0].to_string(), theirs[1].to_string(), theirs[2].to_string());
    learner.learner = true;
    let joining = tempfile::tempdir().unwrap();
    let peers = vec![(NodeId(1), ports[0]), (NodeId(2), theirs[0])];
    let config = node::Config::new(NodeId(2), Vec::new());
    let config = driver::Config::new(config, SERVICE_ID, joining.path(), peers, theirs[1], theirs[2]);
    let second = tokio::spawn(driver::serve(config, Tally::default()));
    admin.add_member(&learner).await.unwrap();
    admin.remove_member(NodeId(2)).await.unwrap();
    second.abort();
    client.send(b"soon".to_vec()).await.unwrap();
    for _ in 1..AFTER {
        client.send(b"order".to_vec()).await.unwrap();
    }
    for _ in 0..AFTER {
        assert!(matches!(client.recv().await, Some(Event::Message(_))));
    }
    // Time for the timer, due at once, to be logged as fired and applied.
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.send(()).unwrap();
    node.await.unwrap().unwrap();
    let _ = std::fs::remove_dir_all(out.join("data"));
    copy_dir(data.path(), &out.join("data"));
    std::fs::write(out.join("data.txt"), format!("session {session}\n{}", read_data(data.path()))).unwrap();
}

/// A one-node cluster's configuration, on `ports` (peer, client, admin).
fn driver_config(me: raft::Member, data: &Path, ports: &[SocketAddr]) -> driver::Config {
    let node = node::Config::new(NodeId(1), vec![me]);
    driver::Config::new(node, SERVICE_ID, data, vec![(NodeId(1), ports[0])], ports[1], ports[2])
}
