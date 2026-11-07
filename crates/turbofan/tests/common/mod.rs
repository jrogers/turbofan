//! What the tests of a cluster over real TCP and files share: a cluster of three, and a fourth to
//! add, running a service that echoes and checks each session's messages come once each, in order.

use std::collections::{BTreeMap, BTreeSet};
use std::future::Future;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::Duration;

use tokio::task::JoinHandle;
use turbofan::client::Event;
use turbofan::driver::{self, serve_until};
use turbofan::raft::{self, NodeId};
use turbofan::tls::NodeTls;
use turbofan::{ClusterClient, Ctx, Service, SessionId, SnapshotWriter, node};

pub mod proxy;

pub const SERVICE_ID: u32 = 7;
/// The operators' token.
pub const TOKEN: &[u8] = b"let me in";
/// The nodes' token, to each other.
pub const PEER_TOKEN: &[u8] = b"one of us";

/// Answers each message with its own text, and checks they come once each, in order.
#[derive(Debug, Default)]
pub struct Echo {
    pub last: BTreeMap<SessionId, u64>,
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

    /// The session's last message applied.
    fn on_query(&self, session: SessionId, _query: &[u8]) -> Option<Vec<u8>> {
        Some(self.last.get(&session).copied().unwrap_or(0).to_string().into_bytes())
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

/// Free local ports, none handed out before in this process: bound, read and released, so another
/// process may take one first, but this one's tests, which run at once, never share one. Linux
/// hands a port just released out again, to the next test asking: one of two clusters would then
/// fail to bind it, and the other test's requests to it reach the wrong cluster, or none.
pub fn ports(n: usize) -> Vec<SocketAddr> {
    static GIVEN: Mutex<BTreeSet<u16>> = Mutex::new(BTreeSet::new());
    let mut given = GIVEN.lock().unwrap();
    let mut ports = Vec::new();
    while ports.len() < n {
        let addr = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
        if given.insert(addr.port()) {
            ports.push(addr);
        }
    }
    ports
}

pub struct Cluster {
    /// Peers', then clients', then operators' addresses, four of each; and, with a proxy, the
    /// peer listeners' own, then the client listeners', behind it.
    pub addrs: Vec<SocketAddr>,
    pub configs: Vec<driver::Config>,
    pub nodes: Vec<Option<JoinHandle<std::io::Result<()>>>>,
    /// The proxy between the nodes, if there's one ([`Cluster::proxied`]).
    pub proxy: Option<proxy::Proxy>,
    /// Each node's pause ([`Cluster::pause`]).
    pub pauses: Vec<Arc<Pause>>,
}

/// Holds a node's task, as a long garbage collection or a stalled VM does: its main loop isn't
/// polled, its timers included, while its connections' tasks go on reading what arrives into its
/// queues, until they fill and TCP holds the rest back. Resumed, it finds all of it waiting, and
/// its clock moved on.
#[derive(Debug, Default)]
pub struct Pause {
    /// Paused, and the task to wake on resuming, once it's been polled.
    state: Mutex<(bool, Option<Waker>)>,
}

impl Pause {
    pub fn set(&self, paused: bool) {
        let mut state = self.state.lock().unwrap();
        state.0 = paused;
        if !paused && let Some(waker) = state.1.take() {
            waker.wake();
        }
    }

    /// `task`, held while paused.
    fn hold<F: Future + Send>(self: Arc<Self>, task: F) -> impl Future<Output = F::Output> + Send {
        let mut task = Box::pin(task);
        std::future::poll_fn(move |cx: &mut Context<'_>| {
            {
                let mut state = self.state.lock().unwrap();
                if state.0 {
                    state.1 = Some(cx.waker().clone());
                    return Poll::Pending;
                }
            }
            Pin::as_mut(&mut task).poll(cx)
        })
    }
}

impl Cluster {
    /// Three members started, and a fourth that can join.
    pub fn new(dir: &Path, tls: impl Fn(NodeId) -> Option<NodeTls>) -> Self {
        Self::with(dir, tls, false)
    }

    /// As [`Cluster::new`], with a [`proxy::Proxy`] between the nodes, and between them and their
    /// clients: the members' peer and client addresses are the proxy's, which forwards to each
    /// node's own. Over TLS, it faults only links from [`proxy::PEERS`] between the nodes.
    pub fn proxied(dir: &Path, tls: impl Fn(NodeId) -> Option<NodeTls>) -> Self {
        Self::with(dir, tls, true)
    }

    fn with(dir: &Path, tls: impl Fn(NodeId) -> Option<NodeTls>, proxied: bool) -> Self {
        let addrs = ports(if proxied { 20 } else { 12 });
        let ids: Vec<NodeId> = [1, 2, 3, 4].map(NodeId).into();
        let addr = |offset: usize, id: NodeId| addrs[offset + usize::from(id) - 1];
        let members: Vec<raft::Member> = ids[..3].iter().map(|&id| member(&addrs, id)).collect();
        let configs: Vec<driver::Config> = ids
            .iter()
            .map(|&id| {
                let mut node = node::Config::new(id, if id == NodeId(4) { Vec::new() } else { members.clone() });
                node.snapshot_every_entries = 50;
                node.egress_ring_entries = 1024;
                // The first three know nothing of the fourth until it's added. A node listens at its
                // own address, behind the proxy if there is one, and dials the others' through it.
                let known = ids.iter().filter(|&&m| m <= id.max(NodeId(3)));
                let peers = known.map(|&m| (m, if proxied && m == id { addr(12, m) } else { addr(0, m) })).collect();
                let client = addr(if proxied { 16 } else { 4 }, id);
                let mut config =
                    driver::Config::new(node, SERVICE_ID, dir.join(id.to_string()), peers, client, addr(8, id));
                config.admin_token = Some(TOKEN.to_vec());
                config.peer_token = Some(PEER_TOKEN.to_vec());
                config.tls = tls(id);
                config
            })
            .collect();
        let over_tls = configs.iter().any(|c| c.tls.is_some());
        let proxy = proxied.then(|| {
            let peers = ids.iter().map(|&id| (id, proxy::Listener::Peer, addr(0, id), addr(12, id)));
            let clients = ids.iter().map(|&id| (id, proxy::Listener::Client, addr(4, id), addr(16, id)));
            proxy::Proxy::start(peers.chain(clients).collect(), over_tls)
        });
        let pauses = (0..4).map(|_| Arc::default()).collect();
        let mut cluster = Self { addrs, configs, nodes: vec![None, None, None, None], proxy, pauses };
        for i in 0..3 {
            cluster.start(i);
        }
        cluster
    }

    pub fn start(&mut self, i: usize) {
        self.start_until(i, std::future::pending());
    }

    /// Starts node `i`, to stop when `stop` completes ([`driver::serve_until`]).
    pub fn start_until(&mut self, i: usize, stop: impl Future<Output = ()> + Send + 'static) {
        let config = self.configs[i].clone();
        self.pauses[i].set(false);
        self.nodes[i] = Some(tokio::spawn(self.pauses[i].clone().hold(async move {
            // A killed node's blocking write may still hold the directory's lock for a moment, and
            // its listeners their ports: their tasks, aborted with it, are dropped after it is.
            let mut stop = Box::pin(stop);
            for _ in 0..100 {
                match serve_until(config.clone(), Echo::default(), &mut stop).await {
                    Err(e) if matches!(e.kind(), std::io::ErrorKind::WouldBlock | std::io::ErrorKind::AddrInUse) => {
                        tokio::time::sleep(Duration::from_millis(20)).await
                    }
                    other => return other,
                }
            }
            serve_until(config, Echo::default(), stop).await
        })));
    }

    /// Holds node `i`'s task, or lets it go on: see [`Pause`].
    pub fn pause(&self, i: usize, paused: bool) {
        self.pauses[i].set(paused);
    }

    pub async fn kill(&mut self, i: usize) {
        let node = self.nodes[i].take().unwrap();
        node.abort();
        assert!(node.await.unwrap_err().is_cancelled(), "node {} stopped on its own", i + 1);
    }

    /// The nodes that stopped on their own, and why: for a test that timed out to say.
    pub async fn stopped(&mut self) -> Vec<String> {
        let mut stopped = Vec::new();
        for (i, node) in self.nodes.iter_mut().enumerate() {
            if node.as_ref().is_some_and(JoinHandle::is_finished) {
                stopped.push(format!("node {}: {:?}", i + 1, node.take().unwrap().await));
            }
        }
        stopped
    }

    /// The members' client addresses: the proxy's, if there's one.
    pub fn client_addrs(&self) -> Vec<String> {
        self.addrs[4..8].iter().map(ToString::to_string).collect()
    }
}

/// Member `id` of a cluster on `addrs`: peers, then clients, then operators, four of each.
pub fn member(addrs: &[SocketAddr], id: NodeId) -> raft::Member {
    let addr = |offset: usize| addrs[offset + usize::from(id) - 1].to_string();
    raft::Member { id, peer: addr(0), client: addr(4), admin: addr(8), learner: false }
}

/// Sends `from..=to`, and waits for each to come back, once and in order. Not in time, it names
/// any node of `cluster` that stopped on its own, which is likelier the cause than the wait.
pub async fn exchange(cluster: &mut Cluster, client: &mut ClusterClient, from: u64, to: u64) {
    for k in from..=to {
        client.send(k.to_string().into_bytes()).await.unwrap();
    }
    let mut expected = from;
    while expected <= to {
        let Ok(event) = tokio::time::timeout(Duration::from_secs(20), client.recv()).await else {
            panic!("{expected} not answered in time; stopped on their own: {:?}", cluster.stopped().await);
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

/// A CA's certificate and key, and a certificate it signs for `127.0.0.1`, under `dir`, as PEM:
/// `<name>-ca.pem`, `<name>.pem`, `<name>.key`.
#[cfg(feature = "tls")]
pub fn certify(dir: &Path, name: &str) {
    certify_named(dir, name, &[(name, &[])]);
}

/// A CA, `<ca>-ca.pem`, and for each of `certs` a certificate it signs, `<name>.pem` and
/// `<name>.key`, for `127.0.0.1` and the DNS names given, under `dir`.
#[cfg(feature = "tls")]
pub fn certify_named(dir: &Path, ca_name: &str, certs: &[(&str, &[&str])]) {
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    std::fs::write(dir.join(format!("{ca_name}-ca.pem")), ca.pem()).unwrap();
    for (name, dns) in certs {
        let key = KeyPair::generate().unwrap();
        let names = ["127.0.0.1"].iter().chain(*dns).map(|&n| n.to_owned()).collect::<Vec<_>>();
        // Both, as a node's must be: it serves its listeners and dials its peers with the one.
        let mut params = CertificateParams::new(names).unwrap();
        params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
        let cert = params.signed_by(&key, &ca).unwrap();
        std::fs::write(dir.join(format!("{name}.pem")), cert.pem()).unwrap();
        std::fs::write(dir.join(format!("{name}.key")), key.serialize_pem()).unwrap();
    }
}
