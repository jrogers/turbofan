//! Many busy clients through failovers, over real TCP and files: a test of its own, so its load
//! doesn't slow the cluster tests beside it, which run in a process of their own.

use std::time::Duration;

use turbofan_cluster::client::Event;
use turbofan_cluster::raft::NodeId;
use turbofan_cluster::{Admin, ClusterClient};

#[allow(dead_code, reason = "the tests share it; this one uses only some of it")]
mod common;
use common::proxy::Link;
use common::{Cluster, SERVICE_ID, TOKEN};

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

/// The same busy clients, through faults in the network between the nodes, over a proxy: each
/// node in turn, about once a second for `SOAK_SECS`, is cut off from the others both ways, or
/// from one of them one way only, or has what it's sent stalled, or slowed, for a few hundred
/// milliseconds; then it's healed. Leaders lose their quorum and step down, followers miss
/// appends and catch up, and every message must still be answered once each, in order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_clients_lose_nothing_through_partitions_and_stalls() {
    const CLIENTS: usize = 8;
    const IN_FLIGHT: u64 = 64;
    let secs = std::env::var("SOAK_SECS").map_or(6, |s| s.parse().expect("SOAK_SECS is a number"));
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::proxied(dir.path());
    let until = tokio::time::Instant::now() + Duration::from_secs(secs);
    let clients: Vec<_> = (0..CLIENTS)
        .map(|_| {
            let addrs = cluster.client_addrs();
            let client = ClusterClient::connect(addrs, SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
            tokio::spawn(keep_busy(client, IN_FLIGHT, until))
        })
        .collect();
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    let proxy = cluster.proxy.as_ref().expect("proxied");
    let mut faults = 0;
    while tokio::time::Instant::now() < until {
        let (id, other) = (ids[faults % 3], ids[(faults + 1) % 3]);
        // What the others send it.
        let into: Vec<Link> = ids.iter().filter(|&&o| o != id).map(|&o| (o, id)).collect();
        match faults % 4 {
            0 => proxy.isolate(id, &ids),
            1 => proxy.fault(|f| {
                f.cut.insert((id, other));
            }),
            2 => proxy.fault(|f| f.stalled.extend(into)),
            _ => proxy.fault(|f| f.delay.extend(into.into_iter().map(|link| (link, Duration::from_millis(20))))),
        }
        tokio::time::sleep(Duration::from_millis(400)).await;
        proxy.heal();
        tokio::time::sleep(Duration::from_millis(600)).await;
        faults += 1;
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
    assert!(faults >= 4, "every kind of fault: {faults}");
    eprintln!("{answered} answered through {faults} faults");
}

/// The same busy clients, while the leader, about once a second for `SOAK_SECS`, is paused, as a
/// long garbage collection or a stalled VM pauses one, then resumed: for less than an election
/// timeout, so that it resumes still leading; for longer, so that it's been replaced without
/// knowing it; or for two seconds, its clients reattached elsewhere and its queues full. Resumed,
/// it finds everything that arrived meanwhile, its timers long due.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn busy_clients_lose_nothing_through_pauses() {
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
    let admins: Vec<Admin> =
        cluster.configs.iter().map(|c| Admin::new(c.admin_listen.to_string(), SERVICE_ID).with_token(TOKEN)).collect();
    let (mut pauses, mut replaced) = (0, 0);
    while tokio::time::Instant::now() < until {
        let Some(paused) = leader(&admins).await else { continue };
        let i = usize::from(paused) - 1;
        cluster.pause(i, true);
        tokio::time::sleep(Duration::from_millis([200, 900, 2000][pauses % 3])).await;
        cluster.pause(i, false);
        tokio::time::sleep(Duration::from_millis(500)).await;
        replaced += usize::from(leader(&admins).await.is_some_and(|now| now != paused));
        pauses += 1;
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
    assert!(pauses >= 3, "every length of pause: {pauses}");
    assert!(replaced >= 1, "a leader paused long is replaced: {replaced} of {pauses}");
    eprintln!("{answered} answered through {pauses} pauses of the leader, {replaced} replaced meanwhile");
}

/// The leader, as the first node to answer within a second says: asked with every node going, as
/// one asked redirects to the leader it knows.
async fn leader(admins: &[Admin]) -> Option<NodeId> {
    for admin in admins {
        if let Ok(Ok(status)) = tokio::time::timeout(Duration::from_secs(1), admin.status()).await {
            return Some(status.leader);
        }
    }
    None
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
