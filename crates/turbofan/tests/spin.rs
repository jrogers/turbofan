//! Three nodes with `driver::Config::spin`, each on threads of its own that never sleep: in a
//! binary of its own, so that its nine spinning threads don't hold back other tests' clusters.

use std::time::Duration;

use turbofan::ClusterClient;

#[allow(dead_code, reason = "the tests share it; this uses only some of it")]
mod common;
use common::*;

/// Messages and a query answered; and each node, told to stop, stops, its threads with it: its
/// listeners closed, for a node started again to bind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_spinning_cluster_answers_and_stops() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut stops = Vec::new();
    for i in 0..3 {
        cluster.kill(i).await;
        cluster.configs[i].spin = true;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        cluster.start_until(i, async move { drop(rx.await) });
        stops.push(tx);
    }
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    let answer = tokio::time::timeout(Duration::from_secs(10), client.query(b"last".to_vec())).await;
    assert_eq!(answer.expect("answered in time"), Ok(b"100".to_vec()));
    for name in ["turbofan-node", "turbofan-peer", "turbofan-client"] {
        assert_threads(name, 3);
    }

    for (i, stop) in stops.into_iter().enumerate() {
        stop.send(()).unwrap();
        let stopped = tokio::time::timeout(Duration::from_secs(10), cluster.nodes[i].take().unwrap()).await;
        assert!(stopped.expect("stopped in time").unwrap().is_ok(), "node {} stopped cleanly", i + 1);
        for addr in [cluster.addrs[i], cluster.addrs[4 + i], cluster.addrs[8 + i]] {
            std::net::TcpListener::bind(addr).unwrap_or_else(|e| panic!("node {}'s {addr} let go: {e}", i + 1));
        }
        assert_threads("turbofan-peer", 2 - i);
    }
    for name in ["turbofan-node", "turbofan-peer", "turbofan-client"] {
        assert_threads(name, 0);
    }
}

/// That this process has `n` threads named `name`: on Linux, which names them in /proc. The
/// node's thread may be a moment ending after it reports, so it's given that moment.
fn assert_threads(name: &str, n: usize) {
    if !cfg!(target_os = "linux") {
        return;
    }
    let count = || {
        let tasks = std::fs::read_dir("/proc/self/task").unwrap();
        tasks
            .filter(|t| {
                std::fs::read_to_string(t.as_ref().unwrap().path().join("comm")).is_ok_and(|c| c.trim() == name)
            })
            .count()
    };
    for _ in 0..100 {
        if count() == n {
            return;
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    assert_eq!(count(), n, "threads named {name}");
}
