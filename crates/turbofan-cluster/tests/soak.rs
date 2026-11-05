//! Many busy clients through failovers, over real TCP and files: a test of its own, so its load
//! doesn't slow the cluster tests beside it, which run in a process of their own.

use std::time::Duration;

use turbofan_cluster::ClusterClient;
use turbofan_cluster::client::Event;

#[allow(dead_code, reason = "the tests share it; this one uses only some of it")]
mod common;
use common::{Cluster, SERVICE_ID};

/// Many clients, each keeping many messages in flight in batches, have every one answered once
/// and in order while every node in turn, the leader among them, is killed and restarted from disk,
/// about once a second, for `SOAK_SECS` (6 by default): a failover, or dropped connections and a
/// reattach, mid-batch. The service checks each session's messages are applied once each, in
/// order; the clients that their responses come so, none lost, and none left unanswered.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_clients_lose_nothing_through_failovers() {
    const CLIENTS: usize = 8;
    const IN_FLIGHT: u64 = 64;
    let secs = std::env::var("SOAK_SECS").map_or(6, |s| s.parse().expect("SOAK_SECS is a number"));
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let until = tokio::time::Instant::now() + Duration::from_secs(secs);
    let clients: Vec<_> = (0..CLIENTS)
        .map(|_| {
            let addrs = cluster.client_addrs();
            let client = ClusterClient::connect(addrs, SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
            tokio::spawn(keep_busy(client, IN_FLIGHT, until))
        })
        .collect();
    let mut kills = 0;
    while tokio::time::Instant::now() < until {
        tokio::time::sleep(Duration::from_millis(700)).await;
        let i = kills % 3;
        cluster.kill(i).await;
        tokio::time::sleep(Duration::from_millis(300)).await;
        cluster.start(i);
        kills += 1;
    }
    let mut answered = 0;
    for client in clients {
        let Ok(done) = tokio::time::timeout(Duration::from_secs(30), client).await else {
            panic!("a client not done in time; stopped on their own: {:?}", cluster.stopped().await);
        };
        let (sent, reattached) = done.expect("answered once each, in order");
        assert!(sent > IN_FLIGHT, "it went on sending: {sent}");
        answered += sent;
        eprintln!("a client: {sent} answered, reattached {reattached} times");
    }
    assert!(kills >= 3, "every node killed: {kills}");
    eprintln!("{answered} answered through {kills} kills");
}

/// Keeps `in_flight` messages in flight on `client` in batches, as each is answered, until
/// `until`, then waits for the last to be: how many it sent, every one answered once and in order
/// (else it panics), and how many times it reattached.
async fn keep_busy(mut client: ClusterClient, in_flight: u64, until: tokio::time::Instant) -> (u64, usize) {
    let batch = |from: u64, to: u64| (from..to).map(|k| k.to_string().into_bytes());
    client.send_batch(batch(1, in_flight + 1)).await.unwrap();
    let (mut sent, mut answered, mut opened, mut events) = (in_flight, 0, 0, Vec::new());
    while answered < sent {
        let read = tokio::time::timeout(Duration::from_secs(20), client.recv_many(&mut events, 256)).await;
        assert!(read.expect("answered in time") > 0, "the session lasts");
        for event in events.drain(..) {
            match event {
                Event::Opened(_) => opened += 1,
                Event::Message(payload) => {
                    answered += 1;
                    assert_eq!(String::from_utf8(payload).unwrap(), answered.to_string(), "once each, in order");
                }
                event => panic!("{event:?}"),
            }
        }
        if tokio::time::Instant::now() < until {
            let more = in_flight - (sent - answered);
            client.send_batch(batch(sent + 1, sent + 1 + more)).await.unwrap();
            sent += more;
        }
    }
    (sent, opened - 1)
}
