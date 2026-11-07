//! What released versions wrote, read by this one: for each in `tests/golden/<version>/`, a
//! snapshot (a backup, to restore from) and the client protocol's frames, which old and new
//! clients and nodes exchange during an upgrade. A snapshot must restore with its sessions,
//! state and epoch, and every frame decode to what it said. See WIRE.md.
//!
//! At a release, `GOLDEN_WRITE=1 cargo test -p turbofan --test golden` writes the
//! version's own, from a one-node cluster over TCP, to check in. The writer must keep working
//! against older code too, as it was run there for the versions before it was written.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::{Path, PathBuf};
use std::time::Duration;

use turbofan::client::Event;
use turbofan::node::{self, Node};
use turbofan::raft::{self, NodeId, Role};
use turbofan::{Admin, CloseReason, ClusterClient, Ctx, Service, SessionId, SnapshotWriter, codec, driver};
use turbofan::{snapshot, wire};

const SERVICE_ID: u32 = 7;
/// Messages the golden session sent, each answered with the count so far and given a timer.
const MESSAGES: u64 = 5;

/// Counts each session's messages, answering each with the count, and schedules a timer for
/// each, far off, so the snapshot holds sessions, responses, timers and the service's state.
#[derive(Debug, Default)]
struct Tally {
    counts: BTreeMap<SessionId, u64>,
}

impl Service for Tally {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, _payload: &[u8]) {
        let count = self.counts.entry(session).or_default();
        *count += 1;
        ctx.send(session, count.to_string().into_bytes()).expect("a count is short");
        ctx.schedule_timer(*count, u64::MAX / 2);
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
    put(&mut out, &wire::Redirect { leader: 2, addr: b"10.0.0.2:7101" });
    put(&mut out, &wire::Opened { session: 9, applied_seq: 5, last_egress_seq: 4, epoch: 3 });
    put(&mut out, &wire::Egress { session: 9, egress_seq: 5, applied_seq: 5, payload: b"fill" });
    put(&mut out, &wire::KeepAliveAck { applied_seq: 5 });
    put(&mut out, &wire::Closed { session: 9, reason: CloseReason::Timeout });
    out
}

/// What [`frames`] wrote, as this version reads it. A version before 0.2.0 (schema version 4)
/// didn't say the session's last response in `Opened`, nor one before 0.4.0 (schema version 5)
/// the epoch in `Opened` and `Reattach`.
fn expected(version: &str) -> Vec<String> {
    let last_egress = if version.starts_with("0.1.") { "None" } else { "Some(4)" };
    let epoch = if ["0.1.", "0.2.", "0.3."].iter().any(|v| version.starts_with(v)) { "None" } else { "Some(3)" };
    [
        // Secrets are kept out of debug output: they're checked apart.
        "Hello(HelloRef { magic: 1414745155, service_id: 7, role: Client, node_id: 0, epoch: 0, token: *** })".into(),
        "Connect(ConnectRef { credentials: *** })".into(),
        format!("Reattach(ReattachRef {{ session: 9, last_egress_seq: 4, epoch: {epoch}, credentials: *** }})"),
        "Message(MessageRef { session: 9, client_seq: 5, payload: [111, 114, 100, 101, 114] })".into(),
        "KeepAlive(KeepAliveRef { session: 9 })".into(),
        "Close(CloseRef { session: 9 })".into(),
        "Redirect(RedirectRef { leader: 2, addr: [49, 48, 46, 48, 46, 48, 46, 50, 58, 55, 49, 48, 49] })".into(),
        format!("Opened(OpenedRef {{ session: 9, applied_seq: 5, last_egress_seq: {last_egress}, epoch: {epoch} }})"),
        "Egress(EgressRef { session: 9, egress_seq: 5, applied_seq: 5, payload: [102, 105, 108, 108] })".into(),
        "KeepAliveAck(KeepAliveAckRef { applied_seq: 5 })".into(),
        "Closed(ClosedRef { session: 9, reason: Timeout })".into(),
    ]
    .into()
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
/// whose one session sent [`MESSAGES`], that session's ID, and [`frames`].
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn write_this_versions_golden_files() {
    if std::env::var_os("GOLDEN_WRITE").is_none() {
        return;
    }
    let out = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/golden").join(env!("CARGO_PKG_VERSION"));
    std::fs::create_dir_all(&out).unwrap();
    std::fs::write(out.join("frames.bin"), frames()).unwrap();

    // Free ports, all held until each is read, so they differ.
    let listeners: Vec<TcpListener> = (0..3).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let ports: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    drop(listeners);
    let me = raft::Member {
        id: NodeId(1),
        peer: ports[0].to_string(),
        client: ports[1].to_string(),
        admin: ports[2].to_string(),
        learner: false,
    };
    let data = tempfile::tempdir().unwrap();
    let config = driver_config(me, data.path(), &ports);
    tokio::spawn(driver::serve(config, Tally::default()));

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
    std::fs::write(out.join("session"), format!("{}\n", session.unwrap().0)).unwrap();
}

/// A one-node cluster's configuration, on `ports` (peer, client, admin).
fn driver_config(me: raft::Member, data: &Path, ports: &[SocketAddr]) -> driver::Config {
    let node = node::Config::new(NodeId(1), vec![me]);
    driver::Config::new(node, SERVICE_ID, data, vec![(NodeId(1), ports[0])], ports[1], ports[2])
}
