//! Three nodes over real TCP and real files, with a gateway client: messages answered once and in
//! order while every node in turn, the leader among them, is killed and restarted from disk, and
//! while a fourth joins and the first leaves.

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
    CloseReason, ClusterClient, Ctx, Service, SessionId, SnapshotWriter, add_member, node, remove_member,
    request_snapshot, shutdown, transfer_leadership,
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
            .map(|&id| driver::Config {
                node: node::Config {
                    raft: raft::Config {
                        id,
                        members: if id == NodeId(4) { Vec::new() } else { members.clone() },
                        election_timeout: Duration::from_millis(300),
                        heartbeat: Duration::from_millis(50),
                        seed: u64::from(id),
                        log_window_bytes: 64 << 20,
                    },
                    session_timeout: Duration::from_millis(10_000),
                    snapshot_every_entries: 50,
                    egress_ring_entries: 1024,
                    protocol: node::PROTOCOL,
                },
                service_id: SERVICE_ID,
                data_dir: dir.join(id.to_string()),
                // The first three know nothing of the fourth until it's added.
                peer_addrs: ids.iter().filter(|&&m| m <= id.max(NodeId(3))).map(|&m| (m, addr(0, m))).collect(),
                client_listen: addr(4, id),
                admin_listen: addr(8, id),
                admin_token: Some(TOKEN.to_vec()),
                peer_token: Some(PEER_TOKEN.to_vec()),
                tls: tls.clone(),
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
    raft::Member { id, peer: addr(0), client: addr(4), admin: addr(8) }
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
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500));
    exchange(&mut cluster, &mut client, 1, 100).await;
    // A client with the wrong credentials gets no session.
    let mut stranger =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"no".to_vec(), Duration::from_millis(500));
    let event = tokio::time::timeout(Duration::from_secs(10), stranger.recv()).await.unwrap();
    assert!(matches!(event, Some(Event::Closed { reason: CloseReason::Refused, .. })), "{event:?}");

    let admin = cluster.configs[0].admin_listen.to_string();
    // Without the token, an operator is refused at once, not left to time out.
    for token in [&b""[..], b"let me out"] {
        let refused = request_snapshot(&admin, SERVICE_ID, token, None, Duration::from_secs(10)).await.unwrap_err();
        assert_eq!(refused.kind(), std::io::ErrorKind::PermissionDenied, "{token:?}");
    }
    // One too long for a hello is an error, not a panic.
    let long = request_snapshot(&admin, SERVICE_ID, &[b'x'; 256], None, Duration::from_secs(10)).await.unwrap_err();
    assert_eq!(long.kind(), std::io::ErrorKind::InvalidInput);
    let index = request_snapshot(&admin, SERVICE_ID, TOKEN, None, Duration::from_secs(10)).await.unwrap();
    assert!(index > 100, "a snapshot after the first hundred: {index}");

    // Handed over, the new leader answers at once.
    let leader = transfer_leadership(&admin, SERVICE_ID, TOKEN, None, None, Duration::from_secs(10)).await.unwrap();
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
    request_snapshot(&admin, SERVICE_ID, TOKEN, None, Duration::from_secs(10)).await.unwrap();

    // Shut down, every node has the same snapshot and stops; started again, the session goes on.
    let index = shutdown(&admin, SERVICE_ID, TOKEN, None, Duration::from_secs(10)).await.unwrap();
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
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500));
    exchange(&mut cluster, &mut client, 1, 100).await;

    cluster.start(3);
    let admin = cluster.configs[1].admin_listen.to_string();
    let (timeout, fourth) = (Duration::from_secs(20), member(&cluster.addrs, NodeId(4)));
    add_member(&admin, SERVICE_ID, TOKEN, None, &fourth, timeout).await.unwrap();
    exchange(&mut cluster, &mut client, 101, 200).await;
    assert!(add_member(&admin, SERVICE_ID, TOKEN, None, &fourth, timeout).await.is_err(), "already a member");

    remove_member(&admin, SERVICE_ID, TOKEN, None, NodeId(1), timeout).await.unwrap();
    cluster.kill(0).await;
    exchange(&mut cluster, &mut client, 201, 300).await;
    // Two of the three that are left are a majority: one more can go down.
    cluster.kill(1).await;
    exchange(&mut cluster, &mut client, 301, 400).await;
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
    let tls = NodeTls {
        cert: file("node.pem"),
        key: file("node.key"),
        peer_ca: file("node-ca.pem"),
        admin_ca: file("operator-ca.pem"),
    };
    let mut cluster = Cluster::new(dir.path(), Some(tls));

    // A hundred messages committed: the peers took each other's appends.
    let gateway = ClientTls::load(&file("node-ca.pem"), None).unwrap();
    let mut client = ClusterClient::connect_tls(
        cluster.client_addrs(),
        SERVICE_ID,
        b"ok".to_vec(),
        Duration::from_millis(500),
        gateway,
    );
    exchange(&mut cluster, &mut client, 1, 100).await;
    let mut plain =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500));
    assert!(tokio::time::timeout(Duration::from_secs(2), plain.recv()).await.is_err(), "no session without TLS");

    let admin = cluster.configs[0].admin_listen.to_string();
    let operator =
        |cert: &str, key: &str| ClientTls::load(&file("node-ca.pem"), Some((&file(cert), &file(key)))).unwrap();
    let timeout = Duration::from_secs(10);
    let ok = operator("operator.pem", "operator.key");
    assert!(request_snapshot(&admin, SERVICE_ID, TOKEN, Some(&ok), timeout).await.unwrap() > 100);
    // Refused at once, not left to time out.
    let refused = [ClientTls::load(&file("node-ca.pem"), None).unwrap(), operator("node.pem", "node.key")];
    for tls in &refused {
        let started = std::time::Instant::now();
        let error = request_snapshot(&admin, SERVICE_ID, TOKEN, Some(tls), timeout).await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5), "{error}");
    }
}
