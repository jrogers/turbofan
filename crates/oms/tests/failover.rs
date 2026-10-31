//! The OMS on three nodes over TCP, with the gateway example's orders and venue running while
//! every node in turn, the leader among them, is killed and restarted from disk. Every order must
//! still get its reports once each, in order.

use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::Duration;

use tokio::task::JoinHandle;
use turbofan::driver::{self, serve};
use turbofan::node;
use turbofan::raft::{self, NodeId};

#[path = "../examples/gateway.rs"]
mod gateway;

const MS: u64 = 1_000_000;
/// Enough to still be trading when the last node is killed, about 2 s in.
const ORDERS: u64 = 10_000;

/// Free local ports: bound, read and released, so there's a small race with other processes.
fn ports(n: usize) -> Vec<SocketAddr> {
    let listeners: Vec<TcpListener> = (0..n).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    listeners.iter().map(|l| l.local_addr().unwrap()).collect()
}

fn configs(dir: &Path) -> Vec<driver::Config> {
    let addrs = ports(9);
    let members: Vec<NodeId> = vec![1, 2, 3];
    let list = |offset: usize| members.iter().map(|&m| (m, addrs[offset + usize::from(m) - 1])).collect::<Vec<_>>();
    let strings = |offset: usize| list(offset).into_iter().map(|(m, a)| (m, a.to_string())).collect();
    members
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
                snapshot_every_entries: 1000,
                egress_ring_entries: 8192,
            },
            service_id: oms::SERVICE_ID,
            data_dir: dir.join(id.to_string()),
            peer_addrs: list(0),
            client_listen: addrs[3 + usize::from(id) - 1],
            admin_listen: addrs[6 + usize::from(id) - 1],
        })
        .collect()
}

fn start(config: driver::Config) -> JoinHandle<std::io::Result<()>> {
    tokio::spawn(async move {
        // A killed node's blocking write may still hold the directory's lock for a moment.
        loop {
            match serve(config.clone(), oms::Oms::default()).await {
                Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => {
                    tokio::time::sleep(Duration::from_millis(20)).await
                }
                other => return other,
            }
        }
    })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn orders_fill_once_each_while_nodes_fail() {
    let dir = tempfile::tempdir().unwrap();
    let configs = configs(dir.path());
    let mut nodes: Vec<_> = configs.iter().cloned().map(start).collect();
    let addrs: Vec<String> = configs.iter().map(|c| c.client_listen.to_string()).collect();
    let trading = tokio::spawn(gateway::trade(addrs, ORDERS));

    for (i, config) in configs.iter().enumerate() {
        tokio::time::sleep(Duration::from_millis(300)).await;
        assert!(!trading.is_finished(), "trading finished before node {} was killed: make ORDERS bigger", i + 1);
        nodes[i].abort();
        assert!((&mut nodes[i]).await.unwrap_err().is_cancelled(), "node {} stopped on its own", i + 1);
        tokio::time::sleep(Duration::from_millis(300)).await;
        nodes[i] = start(config.clone());
    }
    let summary = trading.await.unwrap().unwrap();
    assert_eq!((summary.orders, summary.reports), (ORDERS, ORDERS * 4));
    eprintln!("{summary:?}");
}
