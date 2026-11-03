//! Throughput and latency of a three-node cluster on one machine, over real TCP and files,
//! synced as in production. `cargo bench -p turbofan-cluster --bench cluster` prints a Markdown
//! table, which CI puts in its summary.
//!
//! Two runs. One client with one message in flight: the latency of a commit when nothing
//! queues. Then several clients each keeping many in flight: throughput, the latency then mostly
//! waiting in line. Messages are 64 bytes, answered with 8. Each run measures for `BENCH_SECS`
//! (10 by default) after a second's warm-up.
//!
//! With `--features tls`, both runs again on a cluster over TLS, everywhere: peers, clients.

use std::collections::VecDeque;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::time::{Duration, Instant};

use turbofan_cluster::client::Event;
use turbofan_cluster::driver::{self, serve};
use turbofan_cluster::raft::{self, NodeId};
use turbofan_cluster::tls::{ClientTls, NodeTls};
use turbofan_cluster::{ClusterClient, Ctx, Service, SessionId, SnapshotWriter, node};

const SERVICE_ID: u32 = 9;
const PAYLOAD_BYTES: usize = 64;
const MS: u64 = 1_000_000;

/// Answers each message with its first 8 bytes, keeping nothing.
struct Ack;

impl Service for Ack {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        ctx.send(session, payload.get(..8).unwrap_or(payload)).expect("at most 8 bytes");
    }

    fn snapshot(&self) -> SnapshotWriter {
        Box::new(|_| Ok(()))
    }

    fn restore(&mut self, _from: &mut dyn Read) -> io::Result<()> {
        Ok(())
    }
}

#[tokio::main]
async fn main() {
    let secs = std::env::var("BENCH_SECS").map_or(10, |s| s.parse().expect("BENCH_SECS is a number"));
    let cpus = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);
    println!(
        "Three nodes on one machine ({}, {cpus} CPUs), {PAYLOAD_BYTES}-byte messages, {secs}s a run.",
        std::env::consts::OS
    );
    println!();
    println!("| run | clients × in flight | msgs/s | p50 | p99 | p99.9 | max |");
    println!("|---|---|---:|---:|---:|---:|---:|");
    let dir = tempfile::tempdir().unwrap();
    for tls in [false, true] {
        if tls && !cfg!(feature = "tls") {
            break;
        }
        let dir = dir.path().join(if tls { "tls" } else { "plain" });
        let (addrs, tls) = start(&dir, tls);
        // Once a client has a session, there's a leader.
        drop(connect(&addrs, tls.as_ref()).await);
        bench(&addrs, tls.as_ref(), secs).await;
    }
}

/// Both runs on the cluster at `addrs`, a row each.
async fn bench(addrs: &[String], tls: Option<&ClientTls>, secs: u64) {
    let over = if tls.is_some() { ", TLS" } else { "" };
    for (name, clients, window) in [("latency", 1, 1), ("throughput", 8, 256)] {
        let mut latencies = run(addrs, tls, clients, window, secs).await;
        latencies.sort_unstable();
        // Per mille; none answered in time (a leader change, a slow runner) shows as -.
        let at =
            |q: usize| latencies.get(latencies.len().saturating_sub(1) * q / 1000).map_or("-".into(), |&ns| micros(ns));
        println!(
            "| {name}{over} | {clients} × {window} | {} | {} | {} | {} | {} |",
            latencies.len() as u64 / secs,
            at(500),
            at(990),
            at(999),
            at(1000),
        );
    }
}

/// Starts three nodes on free local ports, in `dir`, over TLS if `tls`, returning where they take
/// clients, and what a client connects with if TLS.
fn start(dir: &Path, tls: bool) -> (Vec<String>, Option<ClientTls>) {
    let certs = dir.join("certs");
    let node_tls = tls.then(|| certify(&certs));
    // Bound, read and released: a small race with other processes.
    let listeners: Vec<TcpListener> = (0..9).map(|_| TcpListener::bind("127.0.0.1:0").unwrap()).collect();
    let addrs: Vec<SocketAddr> = listeners.iter().map(|l| l.local_addr().unwrap()).collect();
    drop(listeners);
    // Peers, then clients, then operators.
    let addr = |offset: usize, id: NodeId| addrs[offset + usize::from(id) - 1];
    let ids: [NodeId; 3] = [1, 2, 3].map(NodeId);
    let members: Vec<raft::Member> = ids
        .iter()
        .map(|&id| raft::Member {
            id,
            peer: addr(0, id).to_string(),
            client: addr(3, id).to_string(),
            admin: addr(6, id).to_string(),
            learner: false,
        })
        .collect();
    for id in ids {
        let config = driver::Config {
            node: node::Config {
                raft: raft::Config {
                    id,
                    members: members.clone(),
                    election_timeout: Duration::from_millis(300),
                    heartbeat: Duration::from_millis(50),
                    seed: u64::from(id),
                    log_window_bytes: 64 << 20,
                    max_uncommitted_bytes: 64 << 20,
                },
                session_timeout: Duration::from_millis(10_000),
                snapshot_every_entries: 100_000,
                egress_ring_entries: 1024,
                protocol: node::PROTOCOL,
            },
            service_id: SERVICE_ID,
            data_dir: dir.join(id.to_string()),
            peer_addrs: ids.iter().map(|&m| (m, addr(0, m))).collect(),
            client_listen: addr(3, id),
            admin_listen: addr(6, id),
            admin_token: None,
            peer_token: None,
            tls: node_tls.clone(),
        };
        tokio::spawn(async move { serve(config, Ack).await.unwrap() });
    }
    let client_tls = node_tls.map(|t| ClientTls::load(&t.peer_ca, None).unwrap());
    (ids.iter().map(|&id| addr(3, id).to_string()).collect(), client_tls)
}

/// A CA under `dir`, and a certificate for `127.0.0.1` it signed, for every node, peer and
/// operator alike.
fn certify(dir: &Path) -> NodeTls {
    use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair};
    std::fs::create_dir_all(dir).unwrap();
    let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
    params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
    let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
    let key = KeyPair::generate().unwrap();
    let mut params = CertificateParams::new(vec!["127.0.0.1".to_owned()]).unwrap();
    params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
    let cert = params.signed_by(&key, &ca).unwrap();
    let file = |name: &str, pem: String| {
        std::fs::write(dir.join(name), pem).unwrap();
        dir.join(name)
    };
    let peer_ca = file("ca.pem", ca.pem());
    NodeTls {
        cert: file("node.pem", cert.pem()),
        key: file("node.key", key.serialize_pem()),
        admin_ca: peer_ca.clone(),
        peer_ca,
    }
}

/// A client with its session open.
async fn connect(addrs: &[String], tls: Option<&ClientTls>) -> ClusterClient {
    let keepalive = Duration::from_millis(500);
    let mut client = match tls {
        Some(tls) => {
            ClusterClient::connect_tls(addrs.to_vec(), SERVICE_ID, Vec::new(), keepalive, tls.clone()).unwrap()
        }
        None => ClusterClient::connect(addrs.to_vec(), SERVICE_ID, Vec::new(), keepalive).unwrap(),
    };
    match client.recv().await {
        Some(Event::Opened(_)) => client,
        other => panic!("no session: {other:?}"),
    }
}

/// `clients` sessions, each keeping `window` messages in flight, for a warm-up second then
/// `secs`: the latency of each message sent after the warm-up and answered before the end, in
/// nanoseconds.
async fn run(addrs: &[String], tls: Option<&ClientTls>, clients: usize, window: usize, secs: u64) -> Vec<u64> {
    let mut sessions = Vec::new();
    for _ in 0..clients {
        sessions.push(connect(addrs, tls).await);
    }
    let warm = Instant::now() + Duration::from_secs(1);
    let end = warm + Duration::from_secs(secs);
    let tasks: Vec<_> = sessions.into_iter().map(|c| tokio::spawn(keep_sending(c, window, warm, end))).collect();
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }
    latencies
}

/// Keeps `window` messages in flight until `end`, sending the next as each is answered.
async fn keep_sending(mut client: ClusterClient, window: usize, warm: Instant, end: Instant) -> Vec<u64> {
    let payload = vec![7; PAYLOAD_BYTES];
    let mut in_flight = VecDeque::new();
    for _ in 0..window {
        in_flight.push_back(Instant::now());
        client.send(payload.clone()).await.unwrap();
    }
    let mut latencies = Vec::new();
    loop {
        let event = client.recv().await.expect("the session lasts");
        let Event::Message(_) = event else { panic!("unexpected {event:?}") };
        let (sent, now) = (in_flight.pop_front().unwrap(), Instant::now());
        if now >= end {
            return latencies;
        }
        if sent >= warm {
            latencies.push(u64::try_from((now - sent).as_nanos()).unwrap());
        }
        in_flight.push_back(Instant::now());
        client.send(payload.clone()).await.unwrap();
    }
}

/// In µs, or ms from 1 ms.
fn micros(ns: u64) -> String {
    if ns < MS { format!("{} µs", ns / 1000) } else { format!("{}.{:02} ms", ns / MS, ns % MS / 10_000) }
}
