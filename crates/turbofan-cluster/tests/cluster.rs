//! Three nodes over real TCP and real files, with a gateway client: messages answered once and in
//! order while every node in turn, the leader among them, is killed and restarted from disk.

use std::collections::BTreeMap;
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::Duration;

use tokio::task::JoinHandle;
use turbofan_cluster::client::Event;
use turbofan_cluster::driver::{self, serve};
use turbofan_cluster::raft::{self, NodeId};
use turbofan_cluster::{ClusterClient, Ctx, Service, SessionId, node, request_snapshot};

const SERVICE_ID: u32 = 7;
const MS: u64 = 1_000_000;

/// Answers each message with its own text, and checks they come once each, in order.
#[derive(Debug, Default)]
struct Echo {
    last: BTreeMap<SessionId, u64>,
}

impl Service for Echo {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let k: u64 = std::str::from_utf8(payload).unwrap().parse().unwrap();
        let last = self.last.entry(session).or_default();
        assert_eq!(k, *last + 1, "applied out of order");
        *last = k;
        ctx.send(session, payload);
    }

    fn snapshot(&self, out: &mut Vec<u8>) {
        for (session, last) in &self.last {
            out.extend_from_slice(&session.to_le_bytes());
            out.extend_from_slice(&last.to_le_bytes());
        }
    }

    fn restore(&mut self, bytes: &[u8]) {
        let words: Vec<u64> = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w)).collect();
        self.last = words.as_chunks::<2>().0.iter().map(|&[session, last]| (session, last)).collect();
    }
}

/// Free local ports: bound, read and released, so there's a small race with other processes.
fn ports(n: usize) -> Vec<SocketAddr> {
    let listeners: Vec<TcpListener> = (0..n).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

struct Cluster {
    configs: Vec<driver::Config>,
    nodes: Vec<Option<JoinHandle<std::io::Result<()>>>>,
}

impl Cluster {
    fn new(dir: &Path) -> Self {
        let addrs = ports(9);
        let members: Vec<NodeId> = vec![1, 2, 3];
        let list = |offset: usize| members.iter().map(|&m| (m, addrs[offset + usize::from(m) - 1])).collect::<Vec<_>>();
        let strings = |offset: usize| list(offset).into_iter().map(|(m, a)| (m, a.to_string())).collect();
        let configs = members
            .iter()
            .map(|&id| driver::Config {
                node: node::Config {
                    raft: raft::Config {
                        id,
                        members: members.clone(),
                        election_timeout_ns: 300 * MS,
                        heartbeat_ns: 50 * MS,
                        seed: u64::from(id),
                    },
                    client_addrs: strings(3),
                    admin_addrs: strings(6),
                    session_timeout_ns: 10_000 * MS,
                    snapshot_every_entries: 50,
                    egress_ring_entries: 1024,
                },
                service_id: SERVICE_ID,
                data_dir: dir.join(id.to_string()),
                peer_addrs: list(0),
                client_listen: addrs[3 + usize::from(id) - 1],
                admin_listen: addrs[6 + usize::from(id) - 1],
            })
            .collect();
        let mut cluster = Self { configs, nodes: vec![None, None, None] };
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

/// Sends `from..=to`, and waits for each to come back, once and in order.
async fn exchange(client: &mut ClusterClient, from: u64, to: u64) {
    for k in from..=to {
        client.send(k.to_string().into_bytes()).await.unwrap();
    }
    let mut expected = from;
    while expected <= to {
        let event = tokio::time::timeout(Duration::from_secs(20), client.recv()).await.expect("answered in time");
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
    let mut cluster = Cluster::new(dir.path());
    let mut client = ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, Duration::from_millis(500));
    exchange(&mut client, 1, 100).await;

    let admin = cluster.configs[0].admin_listen.to_string();
    let index = request_snapshot(&admin, SERVICE_ID, Duration::from_secs(10)).await.unwrap();
    assert!(index > 100, "a snapshot after the first hundred: {index}");

    let mut next = 101;
    for i in 0..3 {
        cluster.kill(i).await;
        exchange(&mut client, next, next + 99).await;
        cluster.start(i);
        next += 100;
    }
    // Everyone back: one more round, then a snapshot every node can take.
    exchange(&mut client, next, next + 99).await;
    request_snapshot(&admin, SERVICE_ID, Duration::from_secs(10)).await.unwrap();
}
