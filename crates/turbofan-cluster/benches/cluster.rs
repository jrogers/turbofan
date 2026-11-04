//! Throughput and latency of a three-node cluster on one machine, over real TCP and files,
//! without fsync (the default) and with it. `cargo bench -p turbofan-cluster --bench cluster` prints a Markdown
//! table, which CI puts in its summary.
//!
//! Two runs a mode. One client with one message in flight: the latency of a commit when nothing
//! queues. Then several clients each keeping many in flight: throughput, the latency then mostly
//! waiting in line. Messages are 64 bytes, answered with 8. Each run measures for `BENCH_SECS`
//! (10 by default) after a second's warm-up.
//!
//! With `--features tls`, all the runs again on a cluster over TLS, everywhere: peers, clients.
//!
//! `BENCH_RESULTS=<file>` writes each run's msgs/s there, and `BENCH_BASELINE=<file>`, such a file
//! from an earlier run, adds a column comparing them, flagging a run below
//! [`REGRESSION_PERCENT`] of it, and a GitHub `::warning` line after the table for each. CI keeps
//! main's last results to compare every run with. On a latency run msgs/s is one over the mean
//! latency, so it catches either getting worse.
//!
//! A second table says where the time goes: from an entry proposed on the leader to its own copy
//! durable, to committed, and to applied (and so answered), from the node's metrics, beside the
//! client's round trip.

use std::collections::VecDeque;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use turbofan_cluster::client::Event;
use turbofan_cluster::driver::{self, serve};
use turbofan_cluster::raft::{self, NodeId};
use turbofan_cluster::tls::{ClientTls, NodeTls};
use turbofan_cluster::{ClusterClient, Ctx, Service, SessionId, SnapshotWriter, node};

const SERVICE_ID: u32 = 9;
const PAYLOAD_BYTES: usize = 64;
const MS: u64 = 1_000_000;
/// A run below this share of the baseline's msgs/s is flagged: shared CI runners are noisy, so
/// only a wide margin means anything.
const REGRESSION_PERCENT: u64 = 70;

/// The stages' histograms, as `metrics::record_latency` names them: persisted, committed, applied.
const STAGES: [&str; 3] =
    ["turbofan_persist_latency_seconds", "turbofan_commit_latency_seconds", "turbofan_apply_latency_seconds"];

/// What a histogram recorded, in seconds.
#[derive(Debug, Default)]
struct Samples(Mutex<Vec<f64>>);

impl metrics::HistogramFn for Samples {
    fn record(&self, seconds: f64) {
        self.0.lock().unwrap().push(seconds);
    }
}

/// The leader's stage latencies, a [`Samples`] each; every other metric goes nowhere.
#[derive(Debug, Default, Clone)]
struct Stages([Arc<Samples>; 3]);

impl Stages {
    /// What each stage recorded since the last take, in nanoseconds, sorted.
    #[allow(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "latencies, whole nanoseconds from seconds, are small and never negative"
    )]
    fn take(&self) -> [Vec<u64>; 3] {
        self.0.each_ref().map(|samples| {
            let mut ns: Vec<u64> =
                std::mem::take(&mut *samples.0.lock().unwrap()).into_iter().map(|s| (s * 1e9) as u64).collect();
            ns.sort_unstable();
            ns
        })
    }
}

impl metrics::Recorder for Stages {
    fn describe_counter(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_gauge(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn describe_histogram(&self, _: metrics::KeyName, _: Option<metrics::Unit>, _: metrics::SharedString) {}
    fn register_counter(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Counter {
        metrics::Counter::noop()
    }
    fn register_gauge(&self, _: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Gauge {
        metrics::Gauge::noop()
    }
    fn register_histogram(&self, key: &metrics::Key, _: &metrics::Metadata<'_>) -> metrics::Histogram {
        match STAGES.iter().position(|&name| name == key.name()) {
            Some(i) => metrics::Histogram::from_arc(self.0[i].clone()),
            None => metrics::Histogram::noop(),
        }
    }
}

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
    let baseline = std::env::var("BENCH_BASELINE").ok().and_then(|path| std::fs::read_to_string(path).ok());
    let baseline: Option<Vec<(&str, u64)>> = baseline.as_deref().map(|text| {
        text.lines().filter_map(|line| line.split_once('\t')).filter_map(|(r, n)| Some((r, n.parse().ok()?))).collect()
    });
    let vs = if baseline.is_some() { (" vs main |", "---:|") } else { ("", "") };
    println!("| run | clients × in flight | msgs/s | p50 | p99 | p99.9 | max |{}", vs.0);
    println!("|---|---|---:|---:|---:|---:|---:|{}", vs.1);
    let (mut results, mut warnings, mut stage_rows) = (String::new(), Vec::new(), Vec::new());
    let stages = Stages::default();
    metrics::set_global_recorder(stages.clone()).unwrap();
    let dir = tempfile::tempdir().unwrap();
    for tls in [false, true] {
        if tls && !cfg!(feature = "tls") {
            break;
        }
        for fsync in [driver::Fsync::Never, driver::Fsync::EachBatch] {
            let dir = dir.path().join(format!("{}-{fsync:?}", if tls { "tls" } else { "plain" }));
            let (addrs, tls) = start(&dir, tls, fsync);
            // Once a client has a session, there's a leader.
            drop(connect(&addrs, tls.as_ref()).await);
            for (name, msgs, row, stage_row, reattached) in bench(&addrs, tls.as_ref(), fsync, secs, &stages).await {
                stage_rows.push(stage_row);
                if reattached > 0 {
                    warnings.push(format!(
                        "::warning title=Benchmark failover::{name}: clients reattached {reattached} times, after a \
                         leader change or a dropped connection"
                    ));
                }
                results.push_str(&format!("{name}\t{msgs}\n"));
                let Some(baseline) = &baseline else {
                    println!("{row}");
                    continue;
                };
                let Some(&(_, then)) = baseline.iter().find(|&&(r, _)| r == name) else {
                    println!("{row} new |");
                    continue;
                };
                let percent = msgs * 100 / then.max(1);
                let flagged = percent < REGRESSION_PERCENT;
                println!("{row} {percent}%{} |", if flagged { " ⚠️" } else { "" });
                if flagged {
                    warnings.push(format!(
                        "::warning title=Benchmark regression::{name}: {msgs} msgs/s, {percent}% of main's {then}"
                    ));
                }
            }
        }
    }
    println!();
    println!("Where the time goes, from proposed on the leader: p50 / p99.");
    println!();
    println!("| run | persisted | committed | applied | client's round trip |");
    println!("|---|---:|---:|---:|---:|");
    for row in stage_rows {
        println!("{row}");
    }
    if let Ok(path) = std::env::var("BENCH_RESULTS") {
        std::fs::write(path, results).unwrap();
    }
    if !warnings.is_empty() {
        println!();
        println!("{}", warnings.join("\n"));
    }
}

/// Both runs on the cluster at `addrs`: each one's name, msgs/s, table row, row of stages, and
/// how many times its clients reattached.
async fn bench(
    addrs: &[String],
    tls: Option<&ClientTls>,
    fsync: driver::Fsync,
    secs: u64,
    stages: &Stages,
) -> Vec<(String, u64, String, String, usize)> {
    let mut rows = Vec::new();
    let over = if tls.is_some() { ", TLS" } else { "" };
    let over = if fsync == driver::Fsync::EachBatch { format!("{over}, fsync") } else { over.to_owned() };
    for (name, clients, window) in [("latency", 1, 1), ("throughput", 8, 256)] {
        let (mut latencies, by_stage, reattached) = run(addrs, tls, clients, window, secs, stages).await;
        latencies.sort_unstable();
        let at = |q: usize| per_mille(&latencies, q);
        let msgs = latencies.len() as u64 / secs;
        let failover = if reattached > 0 { format!(" ⚠️ {reattached} reattached") } else { String::new() };
        let row = format!(
            "| {name}{over}{failover} | {clients} × {window} | {msgs} | {} | {} | {} | {} |",
            at(500),
            at(990),
            at(999),
            at(1000),
        );
        let both = |sorted: &[u64]| format!("{} / {}", per_mille(sorted, 500), per_mille(sorted, 990));
        let [persisted, committed, applied] = by_stage.each_ref().map(|s| both(s));
        let stage_row = format!("| {name}{over} | {persisted} | {committed} | {applied} | {} |", both(&latencies));
        rows.push((format!("{name}{over}"), msgs, row, stage_row, reattached));
    }
    rows
}

/// Starts three nodes on free local ports, in `dir`, over TLS if `tls`, syncing as `fsync`, returning where they take
/// clients, and what a client connects with if TLS.
fn start(dir: &Path, tls: bool, fsync: driver::Fsync) -> (Vec<String>, Option<ClientTls>) {
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
        let mut node = node::Config::new(id, members.clone());
        node.egress_ring_entries = 1024;
        let peers = ids.iter().map(|&m| (m, addr(0, m))).collect();
        let mut config =
            driver::Config::new(node, SERVICE_ID, dir.join(id.to_string()), peers, addr(3, id), addr(6, id));
        config.log_fsync = fsync;
        config.tls = node_tls.clone();
        tokio::spawn(async move { serve(config, Ack).await.unwrap() });
    }
    let client_tls = node_tls.map(|t| ClientTls::load(t.peer_ca.as_deref().expect("a peer CA"), None).unwrap());
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
    NodeTls::new(file("node.pem", cert.pem()), file("node.key", key.serialize_pem()), peer_ca.clone(), peer_ca)
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
/// `secs`: the latency of each message sent after the warm-up and answered before the end, and
/// what the leader timed of each stage meanwhile, in nanoseconds; and how many times the clients
/// reattached.
async fn run(
    addrs: &[String],
    tls: Option<&ClientTls>,
    clients: usize,
    window: usize,
    secs: u64,
    stages: &Stages,
) -> (Vec<u64>, [Vec<u64>; 3], usize) {
    let mut sessions = Vec::new();
    for _ in 0..clients {
        sessions.push(connect(addrs, tls).await);
    }
    let warm = Instant::now() + Duration::from_secs(1);
    let end = warm + Duration::from_secs(secs);
    let tasks: Vec<_> = sessions.into_iter().map(|c| tokio::spawn(keep_sending(c, window, warm, end))).collect();
    tokio::time::sleep_until(warm.into()).await;
    stages.take();
    let (mut latencies, mut reattached) = (Vec::new(), 0);
    for task in tasks {
        let (some, times) = task.await.unwrap();
        latencies.extend(some);
        reattached += times;
    }
    (latencies, stages.take(), reattached)
}

/// The `q`th per mille of `sorted`; none (a leader change, a slow runner) shows as -.
fn per_mille(sorted: &[u64], q: usize) -> String {
    sorted.get(sorted.len().saturating_sub(1) * q / 1000).map_or("-".into(), |&ns| micros(ns))
}

/// Keeps `window` messages in flight until `end`, sending the next as each is answered. Returns
/// their latencies, and how many times the client reattached: its session goes on, each response
/// still once and in order, so the timings hold.
async fn keep_sending(mut client: ClusterClient, window: usize, warm: Instant, end: Instant) -> (Vec<u64>, usize) {
    let payload = vec![7; PAYLOAD_BYTES];
    let mut in_flight = VecDeque::new();
    for _ in 0..window {
        in_flight.push_back(Instant::now());
        client.send(payload.clone()).await.unwrap();
    }
    let (mut latencies, mut reattached) = (Vec::new(), 0);
    loop {
        let event = client.recv().await.expect("the session lasts");
        if let Event::Opened(session) = event {
            eprintln!("session {session:?} reattached {:.1?} after the warm-up", Instant::now().duration_since(warm));
            reattached += 1;
            continue;
        }
        let Event::Message(_) = event else { panic!("unexpected {event:?}") };
        let (sent, now) = (in_flight.pop_front().unwrap(), Instant::now());
        if now >= end {
            return (latencies, reattached);
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
