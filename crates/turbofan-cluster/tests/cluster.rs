//! Three nodes over real TCP and real files, with a gateway client: messages answered once and in
//! order while every node in turn, the leader among them, is killed and restarted from disk, and
//! while a fourth joins and the first leaves; a fourth, a learner, promoted to a cluster of its
//! own once the three are lost, that they can't take back; the three, lost, restored from a
//! copy of a snapshot; and a node's capture of its log replayed offline.

use std::collections::BTreeMap;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::Duration;

use tokio::task::JoinHandle;
use turbofan_cluster::client::Event;
use turbofan_cluster::driver::{self, serve};
use turbofan_cluster::raft::{self, NodeId};
#[cfg(feature = "tls")]
use turbofan_cluster::tls::ClientTls;
use turbofan_cluster::tls::NodeTls;
use turbofan_cluster::{
    Admin, AdminError, CloseReason, ClusterClient, Ctx, Effect, Service, SessionId, SnapshotWriter, capture, node,
};

const SERVICE_ID: u32 = 7;
/// The operators' token.
const TOKEN: &[u8] = b"let me in";
/// The nodes' token, to each other.
const PEER_TOKEN: &[u8] = b"one of us";

/// Answers each message with its own text, and checks they come once each, in order.
#[derive(Debug, Default)]
struct Echo {
    last: BTreeMap<SessionId, u64>,
}

impl Service for Echo {
    /// Only clients that say `ok`.
    fn authenticate(&self, credentials: &[u8]) -> Option<Vec<u8>> {
        (credentials == b"ok").then(Vec::new)
    }

    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let k: u64 = std::str::from_utf8(payload).unwrap().parse().unwrap();
        let last = self.last.entry(session).or_default();
        assert_eq!(k, *last + 1, "applied out of order");
        *last = k;
        ctx.send(session, payload).expect("echoes a client payload");
    }

    fn snapshot(&self) -> SnapshotWriter {
        let last = self.last.clone();
        Box::new(move |out| {
            for (session, last) in &last {
                out.write_all(&session.0.to_le_bytes())?;
                out.write_all(&last.to_le_bytes())?;
            }
            Ok(())
        })
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let words: Vec<u64> = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w)).collect();
        self.last = words.as_chunks::<2>().0.iter().map(|&[session, last]| (SessionId(session), last)).collect();
        Ok(())
    }
}

/// Free local ports: bound, read and released, so there's a small race with other processes.
fn ports(n: usize) -> Vec<SocketAddr> {
    let listeners: Vec<TcpListener> = (0..n).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

struct Cluster {
    /// Peers', then clients', then operators' addresses, four of each.
    addrs: Vec<SocketAddr>,
    configs: Vec<driver::Config>,
    nodes: Vec<Option<JoinHandle<std::io::Result<()>>>>,
}

impl Cluster {
    /// Three members started, and a fourth that can join.
    fn new(dir: &Path, tls: Option<NodeTls>) -> Self {
        let addrs = ports(12);
        let ids: Vec<NodeId> = [1, 2, 3, 4].map(NodeId).into();
        let addr = |offset: usize, id: NodeId| addrs[offset + usize::from(id) - 1];
        let members: Vec<raft::Member> = ids[..3].iter().map(|&id| member(&addrs, id)).collect();
        let configs = ids
            .iter()
            .map(|&id| {
                let mut node = node::Config::new(id, if id == NodeId(4) { Vec::new() } else { members.clone() });
                node.snapshot_every_entries = 50;
                node.egress_ring_entries = 1024;
                // The first three know nothing of the fourth until it's added.
                let peers = ids.iter().filter(|&&m| m <= id.max(NodeId(3))).map(|&m| (m, addr(0, m))).collect();
                let mut config =
                    driver::Config::new(node, SERVICE_ID, dir.join(id.to_string()), peers, addr(4, id), addr(8, id));
                config.admin_token = Some(TOKEN.to_vec());
                config.peer_token = Some(PEER_TOKEN.to_vec());
                config.tls = tls.clone();
                config
            })
            .collect();
        let mut cluster = Self { addrs, configs, nodes: vec![None, None, None, None] };
        for i in 0..3 {
            cluster.start(i);
        }
        cluster
    }

    fn start(&mut self, i: usize) {
        let config = self.configs[i].clone();
        self.nodes[i] = Some(tokio::spawn(async move {
            // A killed node's blocking write may still hold the directory's lock for a moment.
            for _ in 0..100 {
                match serve(config.clone(), Echo::default()).await {
                    Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    other => return other,
                }
            }
            serve(config, Echo::default()).await
        }));
    }

    async fn kill(&mut self, i: usize) {
        let node = self.nodes[i].take().unwrap();
        node.abort();
        assert!(node.await.unwrap_err().is_cancelled(), "node {} stopped on its own", i + 1);
    }

    fn client_addrs(&self) -> Vec<String> {
        self.configs.iter().map(|c| c.client_listen.to_string()).collect()
    }
}

/// Member `id` of a cluster on `addrs`: peers, then clients, then operators, four of each.
fn member(addrs: &[SocketAddr], id: NodeId) -> raft::Member {
    let addr = |offset: usize| addrs[offset + usize::from(id) - 1].to_string();
    raft::Member { id, peer: addr(0), client: addr(4), admin: addr(8), learner: false }
}

/// Sends `from..=to`, and waits for each to come back, once and in order. Not in time, it names
/// any node of `cluster` that stopped on its own, which is likelier the cause than the wait.
async fn exchange(cluster: &mut Cluster, client: &mut ClusterClient, from: u64, to: u64) {
    for k in from..=to {
        client.send(k.to_string().into_bytes()).await.unwrap();
    }
    let mut expected = from;
    while expected <= to {
        let Ok(event) = tokio::time::timeout(Duration::from_secs(20), client.recv()).await else {
            let mut stopped = Vec::new();
            for (i, node) in cluster.nodes.iter_mut().enumerate() {
                if node.as_ref().is_some_and(JoinHandle::is_finished) {
                    stopped.push(format!("node {}: {:?}", i + 1, node.take().unwrap().await));
                }
            }
            panic!("{expected} not answered in time; stopped on their own: {stopped:?}");
        };
        match event.expect("session open") {
            Event::Opened(_) => {}
            Event::Message(payload) => {
                assert_eq!(String::from_utf8(payload).unwrap(), expected.to_string(), "answered out of order");
                expected += 1;
            }
            event => panic!("{event:?}"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_answers_every_message_once_through_failovers() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    // A client with the wrong credentials gets no session.
    let mut stranger =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"no".to_vec(), Duration::from_millis(500)).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(10), stranger.recv()).await.unwrap();
    assert!(matches!(event, Some(Event::Closed { reason: CloseReason::Refused, .. })), "{event:?}");
    let after = tokio::time::timeout(Duration::from_secs(10), stranger.recv()).await.unwrap();
    assert!(after.is_none(), "the session is over: {after:?}");

    let anyone =
        Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID).with_timeout(Duration::from_secs(10));
    // Without the token, an operator is refused at once, not left to time out.
    for token in [&b""[..], b"let me out"] {
        let refused = anyone.clone().with_token(token).request_snapshot().await.unwrap_err();
        assert!(
            matches!(&refused, AdminError::Denied(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
            "{token:?}: {refused:?}"
        );
    }
    // One too long for a hello is an error, not a panic.
    let long = anyone.clone().with_token([b'x'; 256]).request_snapshot().await.unwrap_err();
    assert!(matches!(long, AdminError::TokenTooLong), "{long:?}");
    let admin = anyone.with_token(TOKEN);
    let index = admin.request_snapshot().await.unwrap();
    assert!(index > 100, "a snapshot after the first hundred: {index}");

    // Handed over, the new leader answers at once.
    let leader = admin.transfer_leadership(None).await.unwrap();
    assert!((1..=3).contains(&leader.0));
    let mut next = 101;
    for i in 0..3 {
        cluster.kill(i).await;
        exchange(&mut cluster, &mut client, next, next + 99).await;
        cluster.start(i);
        next += 100;
    }
    // Everyone back: one more round, then a snapshot every node can take.
    exchange(&mut cluster, &mut client, next, next + 99).await;
    admin.request_snapshot().await.unwrap();

    // Shut down, every node has the same snapshot and stops; started again, the session goes on.
    let index = admin.shutdown().await.unwrap();
    for i in 0..3 {
        let node = cluster.nodes[i].take().unwrap();
        tokio::time::timeout(Duration::from_secs(5), node).await.expect("stopped").unwrap().unwrap();
        let snapshots = cluster.configs[i].data_dir.join("snapshots");
        let (snapshot, _) = turbofan_cluster::snapshot::load(&snapshots).unwrap().unwrap();
        assert_eq!(snapshot.index, index, "node {}", i + 1);
    }
    for i in 0..3 {
        cluster.start(i);
    }
    exchange(&mut cluster, &mut client, next + 100, next + 199).await;
}

/// A fourth node joins, and the first leaves, while a client's messages are answered once each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_joins_and_another_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    cluster.start(3);
    let admin = Admin::new(cluster.configs[1].admin_listen.to_string(), SERVICE_ID)
        .with_token(TOKEN)
        .with_timeout(Duration::from_secs(20));
    let fourth = member(&cluster.addrs, NodeId(4));
    let added = admin.add_member(&fourth).await.unwrap();
    exchange(&mut cluster, &mut client, 101, 200).await;
    // Asked again, as after a lost answer: answered as made, at the change or a snapshot since.
    // Elsewhere, refused.
    let again = admin.add_member(&fourth).await.unwrap();
    assert!(again >= added, "already a member: {again} from {added}");
    let elsewhere = raft::Member { peer: "127.0.0.1:1".into(), ..fourth.clone() };
    let again = admin.add_member(&elsewhere).await.unwrap_err();
    assert!(matches!(again, AdminError::MembersRefused), "a member 4 elsewhere: {again:?}");

    admin.remove_member(NodeId(1)).await.unwrap();
    cluster.kill(0).await;
    exchange(&mut cluster, &mut client, 201, 300).await;
    // Two of the three that are left are a majority: one more can go down.
    cluster.kill(1).await;
    exchange(&mut cluster, &mut client, 301, 400).await;
}

/// A node's capture of its log, from its snapshot to its commit index, replayed offline into the
/// service, echoes what the cluster echoed after the snapshot, in order, and ends with the
/// service as the cluster had it: each message once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capture_replays_what_the_cluster_applied() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    let addrs: Vec<String> = cluster.configs.iter().map(|c| c.admin_listen.to_string()).collect();
    let admin = |i: usize| Admin::new(&addrs[i], SERVICE_ID).with_token(TOKEN).with_timeout(Duration::from_secs(20));
    let asked = admin(0).request_snapshot().await.unwrap();
    exchange(&mut cluster, &mut client, 101, 200).await;

    // Any member: the one asked answers, of what it knows committed, which may lag the leader's.
    let (captured, echoed, echo) = loop {
        let captured = admin(1).capture().await.unwrap();
        let mut echoed = Vec::new();
        let replayed = capture::replay(Path::new(&captured.dir), Echo::default(), None, |_, effects| {
            for effect in effects {
                let Effect::Send(_, payload) = effect else { panic!("{effect:?}") };
                echoed.push(String::from_utf8(payload.clone()).unwrap().parse::<u64>().unwrap());
            }
        })
        .unwrap();
        if replayed.last.values().any(|&k| k == 200) {
            break (captured, echoed, replayed);
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    assert!(captured.snapshot_index >= asked, "from its latest snapshot");
    // From whichever snapshot is the node's latest (it takes its own every 50 entries): Echo
    // checks each message follows on from the state it restored.
    let first = echoed[0];
    assert_eq!(echoed, (first..=200).collect::<Vec<_>>(), "echoed once each, in order");
    assert_eq!(echo.last.len(), 1, "one session");

    // Stopped short, at an entry before the last; past what's committed, refused.
    let to = captured.to - 1;
    let mut last = 0;
    capture::replay(Path::new(&captured.dir), Echo::default(), Some(to), |index, _| last = index).unwrap();
    assert_eq!(last, to);
    let past = capture::replay(Path::new(&captured.dir), Echo::default(), Some(captured.to + 1), |_, _| {});
    assert!(past.is_err(), "past its commit index");
}

/// A learner, promoted once its cluster is lost, leads a cluster of its own. The old cluster,
/// back, a majority of its own, can't take it back: of another epoch, it's refused, and a session
/// opened on the new cluster goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_promoted_learner_leads_a_cluster_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    cluster.start(3);
    let addrs: Vec<String> = cluster.configs.iter().map(|c| c.admin_listen.to_string()).collect();
    let admin = |i: usize| Admin::new(&addrs[i], SERVICE_ID).with_token(TOKEN).with_timeout(Duration::from_secs(20));
    let learner = raft::Member { learner: true, ..member(&cluster.addrs, NodeId(4)) };
    admin(1).add_member(&learner).await.unwrap();
    exchange(&mut cluster, &mut client, 101, 200).await;
    // Commits don't wait on a learner: wait for it to catch up, as an operator would, before the
    // cluster is lost. The leader reports every member.
    loop {
        let status = admin(1).status().await.unwrap();
        assert_eq!(status.members.len(), 4);
        assert!(status.members.iter().any(|m| m.id == NodeId(4) && m.learner));
        if status.lag(NodeId(4)) == Some(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let refused = admin(3).promote().await.unwrap_err();
    assert!(matches!(refused, AdminError::NotPromoted), "it hears its leader: {refused:?}");

    // The cluster is lost; once its leader has been silent a while, the learner is promoted.
    for i in 0..3 {
        cluster.kill(i).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    admin(3).promote().await.unwrap();
    let fourth = vec![cluster.configs[3].client_listen.to_string()];
    let mut promoted = ClusterClient::connect(fourth, SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut promoted, 1, 100).await;

    // The old cluster comes back, three of its four, and elects a leader of its own; the new one
    // goes on as it was.
    for i in 0..3 {
        cluster.start(i);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    exchange(&mut cluster, &mut promoted, 101, 200).await;
}

/// Backup and restore: a snapshot taken on demand and copied off a node restores the cluster,
/// lost whole, as a cluster of one of a new epoch, which grows with `add-member`. A client that
/// saw what came after the snapshot closes its session; the old nodes, back, can't take it over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_cluster_is_restored_from_a_copy_of_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    let addrs: Vec<String> = cluster.configs.iter().map(|c| c.admin_listen.to_string()).collect();
    let admin = |i: usize| Admin::new(&addrs[i], SERVICE_ID).with_token(TOKEN).with_timeout(Duration::from_secs(20));
    let asked = admin(0).request_snapshot().await.unwrap();
    // The newest snapshot in its directory, once it's the one asked for or later, as an operator
    // checks by its name: a quorum has it, this node perhaps only in a moment.
    let snapshots = cluster.configs[0].data_dir.join("snapshots");
    let newest = loop {
        let newest = std::fs::read_dir(&snapshots)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "snapshot"))
            .max()
            .unwrap();
        let index: u64 = newest.file_stem().unwrap().to_str().unwrap().parse().unwrap();
        if index >= asked {
            break newest;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let backup = dir.path().join("backup.snapshot");
    std::fs::copy(newest, &backup).unwrap();
    // Applied after the backup: lost with the cluster.
    exchange(&mut cluster, &mut client, 101, 150).await;
    for i in 0..3 {
        cluster.kill(i).await;
    }

    let mut restored = cluster.configs[0].clone();
    restored.data_dir = dir.path().join("restored");
    let (index, _) = driver::restore(&backup, &restored).unwrap();
    assert!(index >= asked, "the snapshot copied, {index}, is at least the one asked for, {asked}");
    let again = driver::restore(&backup, &restored).unwrap_err();
    assert_eq!(again.kind(), io::ErrorKind::AlreadyExists, "a data directory is never overwritten");
    cluster.configs[0] = restored;
    cluster.start(0);

    // The client saw 150 applied, the restored cluster has at most 100 of them: it can't go on.
    let closed = tokio::time::timeout(Duration::from_secs(20), client.recv()).await.unwrap();
    let Some(Event::Closed { reason: CloseReason::Diverged, unapplied }) = closed else { panic!("{closed:?}") };
    assert!((50..=150).contains(&unapplied), "{unapplied} the restored cluster never applied");
    let restored = vec![cluster.configs[0].client_listen.to_string()];
    let mut client = ClusterClient::connect(restored, SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    // The old cluster's other two come back, a majority of its three, and elect a leader of their
    // own; the restored one, of another epoch, goes on as it was, and grows.
    for i in 1..3 {
        cluster.start(i);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    exchange(&mut cluster, &mut client, 101, 150).await;
    assert_eq!(admin(0).status().await.unwrap().members.len(), 1, "it alone");
    cluster.start(3);
    admin(0).add_member(&member(&cluster.addrs, NodeId(4))).await.unwrap();
    exchange(&mut cluster, &mut client, 151, 200).await;
    assert_eq!(admin(0).status().await.unwrap().members.len(), 2);
}

/// A CA's certificate and key, and a certificate it signs for `127.0.0.1`, under `dir`, as PEM:
/// `<name>-ca.pem`, `<name>.pem`, `<name>.key`.
#[cfg(feature = "tls")]
fn certify(dir: &Path, name: &str) {
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    let key = KeyPair::generate().unwrap();
    // Both, as a node's must be: it serves its listeners and dials its peers with the one.
    let mut params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
    let cert = params.signed_by(&key, &ca).unwrap();
    std::fs::write(dir.join(format!("{name}-ca.pem")), ca.pem()).unwrap();
    std::fs::write(dir.join(format!("{name}.pem")), cert.pem()).unwrap();
    std::fs::write(dir.join(format!("{name}.key")), key.serialize_pem()).unwrap();
}

/// With TLS, nodes replicate to each other, and a client and an operator get through; a client
/// without TLS, an operator without a certificate, and one with a node's, don't.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_over_tls_takes_only_who_it_should() {
    let dir = tempfile::tempdir().unwrap();
    let certs = dir.path().join("certs");
    std::fs::create_dir(&certs).unwrap();
    certify(&certs, "node");
    certify(&certs, "operator");
    let file = |name: &str| certs.join(name);
    let tls = NodeTls::new(file("node.pem"), file("node.key"), file("node-ca.pem"), file("operator-ca.pem"));
    let mut cluster = Cluster::new(dir.path(), Some(tls));

    // A hundred messages committed: the peers took each other's appends.
    let gateway = ClientTls::load(&file("node-ca.pem"), None).unwrap();
    let mut client = ClusterClient::connect_tls(
        cluster.client_addrs(),
        SERVICE_ID,
        b"ok".to_vec(),
        Duration::from_millis(500),
        gateway,
    )
    .unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    let mut plain =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), plain.recv()).await.is_err(), "no session without TLS");

    let admin = Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID)
        .with_token(TOKEN)
        .with_timeout(Duration::from_secs(10));
    let operator =
        |cert: &str, key: &str| ClientTls::load(&file("node-ca.pem"), Some((&file(cert), &file(key)))).unwrap();
    let ok = operator("operator.pem", "operator.key");
    assert!(admin.clone().with_tls(ok).request_snapshot().await.unwrap() > 100);
    // Refused at once, not left to time out.
    let refused = [ClientTls::load(&file("node-ca.pem"), None).unwrap(), operator("node.pem", "node.key")];
    for tls in refused {
        let started = std::time::Instant::now();
        let error = admin.clone().with_tls(tls).request_snapshot().await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5), "{error}");
        assert!(matches!(error, AdminError::Denied(_)), "{error:?}");
    }
}

/// With TLS on the admin listener only, peers replicate and a client gets through over plain
/// TCP; an operator gets through with its certificate, and not without TLS.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_on_the_admin_listener_alone() {
    let dir = tempfile::tempdir().unwrap();
    let certs = dir.path().join("certs");
    std::fs::create_dir(&certs).unwrap();
    certify(&certs, "node");
    certify(&certs, "operator");
    let file = |name: &str| certs.join(name);
    let tls = NodeTls::admin_only(file("node.pem"), file("node.key"), file("operator-ca.pem"));
    let mut cluster = Cluster::new(dir.path(), Some(tls));

    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    let admin = Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID)
        .with_token(TOKEN)
        .with_timeout(Duration::from_secs(5));
    let operator = ClientTls::load(&file("node-ca.pem"), Some((&file("operator.pem"), &file("operator.key")))).unwrap();
    assert!(admin.clone().with_tls(operator).request_snapshot().await.unwrap() > 100);
    assert!(admin.request_snapshot().await.is_err(), "an operator without TLS");
}
