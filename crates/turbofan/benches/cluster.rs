//! Throughput and latency of a three-node cluster on one machine, over real TCP and files,
//! without fsync and with it (the default). `cargo bench -p turbofan --bench cluster` prints a Markdown
//! table, which CI puts in its summary.
//!
//! Two runs a mode. One client with one message in flight: the latency of a commit when nothing
//! queues. Then several clients each keeping many in flight, as a gateway does, answering the
//! responses that have arrived with a batch of as many: throughput, the latency then mostly
//! waiting in line. Messages are 64 bytes, answered with 8. Each run measures for `BENCH_SECS`
//! (10 by default) after a second's warm-up. `BENCH_THROUGHPUT=<clients>x<in flight>` sets the
//! throughput run's shape (`8x256` by default), each client's at most
//! [`MAX_UNACKED`](turbofan::client::MAX_UNACKED).
//!
//! Then, without fsync, which they don't touch, queries (`ClusterClient::query`, never logged)
//! the same two ways: one in flight, then as many as the throughput run's messages, each a client's
//! query of 64 bytes answered with 8.
//!
//! With `--features tls`, all the runs again on a cluster over TLS, everywhere: peers, clients.
//!
//! First, though, the messages' two runs on the core alone: the cluster in memory
//! ([`turbofan::testing`]), its nodes and clients on one thread, no sockets or files, its network
//! and writes taking none of its time, timed by the wall's clock. That's what openraft's benchmark
//! measures, so it's the number to set beside theirs, and its gap to the runs over TCP is the
//! driver's cost.
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
//!
//! `BENCH_CLUSTER=<addr,...>` drives a cluster running elsewhere instead, the clients alone in
//! this process: the client addresses of nodes of `examples/echo.rs`, on machines of their own, so
//! the clients don't take the nodes' CPUs. The nodes' runs alone, as they're configured:
//! `BENCH_FSYNC=0` says they `serve --no-fsync`, and `BENCH_TLS_CA=<pem>` that they're over TLS,
//! their certificates signed by that CA. Not the stages, which are the nodes' metrics.

use std::collections::VecDeque;
use std::io::{self, Read};
use std::net::{SocketAddr, TcpListener};
use std::path::Path;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use turbofan::client::{Event, MAX_UNACKED};
use turbofan::driver::{self, serve};
use turbofan::raft::{self, NodeId};
use turbofan::testing::{self, ClientId};
use turbofan::tls::{ClientTls, NodeTls};
use turbofan::{ClusterClient, Ctx, Service, SessionId, SnapshotWriter, node};

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

/// Answers each message, and each query, with its first 8 bytes, keeping nothing.
struct Ack;

impl Service for Ack {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        ctx.send(session, payload.get(..8).unwrap_or(payload)).expect("at most 8 bytes");
    }

    fn on_query(&self, _session: SessionId, query: &[u8]) -> Option<Vec<u8>> {
        Some(query.get(..8).unwrap_or(query).to_vec())
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
    let shape = throughput_shape();
    let cpus = std::thread::available_parallelism().map_or(0, std::num::NonZero::get);
    let remote = remote();
    let place = match &remote {
        Some((addrs, ..)) => format!("A cluster at {}, its clients on this machine", addrs.join(", ")),
        None => "Three nodes on one machine".to_owned(),
    };
    println!("{place} ({}, {cpus} CPUs), {PAYLOAD_BYTES}-byte messages, {secs}s a run.", std::env::consts::OS);
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
    let mut report = |(name, msgs, row, stage_row, reattached): (String, u64, String, Option<String>, usize)| {
        stage_rows.extend(stage_row);
        if reattached > 0 {
            warnings.push(format!(
                "::warning title=Benchmark failover::{name}: clients reattached {reattached} times, after a leader \
                 change or a dropped connection"
            ));
        }
        results.push_str(&format!("{name}\t{msgs}\n"));
        println!("{row}{}", compared(&name, msgs, baseline.as_deref(), &mut warnings));
    };
    if let Some((addrs, tls, fsync)) = &remote {
        // Once a client has a session, there's a leader.
        drop(connect(addrs, tls.as_ref()).await);
        for run in bench(addrs, tls.as_ref(), *fsync, secs, shape, &stages).await {
            report(run);
        }
    } else {
        // Before the clusters over TCP, which go on heartbeating once they're done with.
        for (name, clients, window) in [("latency, in memory", 1, 1), ("throughput, in memory", shape.0, shape.1)] {
            let mut latencies = in_memory(clients, window, secs);
            latencies.sort_unstable();
            let (msgs, row) = table_row(name, clients, window, &latencies, secs);
            report((name.to_owned(), msgs, row, None, 0));
        }
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
                for run in bench(&addrs, tls.as_ref(), fsync, secs, shape, &stages).await {
                    report(run);
                }
            }
        }
    }
    print_stages(&stage_rows);
    if let Ok(path) = std::env::var("BENCH_RESULTS") {
        std::fs::write(path, results).unwrap();
    }
    if !warnings.is_empty() {
        println!();
        println!("{}", warnings.join("\n"));
    }
}

/// The second table, of where the time goes: none from a cluster elsewhere, whose stages are its
/// nodes' metrics.
fn print_stages(rows: &[String]) {
    if rows.is_empty() {
        return;
    }
    println!();
    println!("Where the time goes, from proposed on the leader: p50 / p99.");
    println!();
    println!("| run | persisted | committed | applied | client's round trip |");
    println!("|---|---:|---:|---:|---:|");
    for row in rows {
        println!("{row}");
    }
}

/// A run's cell comparing its `msgs` with `baseline`'s, if there's a baseline; a regression is
/// flagged there, and warned of.
fn compared(name: &str, msgs: u64, baseline: Option<&[(&str, u64)]>, warnings: &mut Vec<String>) -> String {
    let Some(baseline) = baseline else { return String::new() };
    let Some(&(_, then)) = baseline.iter().find(|&&(r, _)| r == name) else { return " new |".into() };
    let percent = msgs * 100 / then.max(1);
    let flagged = percent < REGRESSION_PERCENT;
    if flagged {
        warnings
            .push(format!("::warning title=Benchmark regression::{name}: {msgs} msgs/s, {percent}% of main's {then}"));
    }
    format!(" {percent}%{} |", if flagged { " ⚠️" } else { "" })
}

/// The cluster elsewhere `BENCH_CLUSTER` names, if it does: its client addresses, what a client
/// connects to it with over TLS (`BENCH_TLS_CA`), and how its nodes sync (`BENCH_FSYNC`).
fn remote() -> Option<(Vec<String>, Option<ClientTls>, driver::Fsync)> {
    let addrs: Vec<String> = std::env::var("BENCH_CLUSTER").ok()?.split(',').map(str::to_owned).collect();
    let tls = std::env::var("BENCH_TLS_CA").ok().map(|ca| ClientTls::load(Path::new(&ca), None).expect("BENCH_TLS_CA"));
    let fsync = match std::env::var("BENCH_FSYNC").as_deref() {
        Err(_) | Ok("1") => driver::Fsync::EachBatch,
        Ok("0") => driver::Fsync::Never,
        Ok(other) => panic!("BENCH_FSYNC is 1 or 0, not {other}"),
    };
    Some((addrs, tls, fsync))
}

/// The throughput run's clients and messages in flight each, from `BENCH_THROUGHPUT`.
fn throughput_shape() -> (usize, usize) {
    let shape = std::env::var("BENCH_THROUGHPUT").unwrap_or_else(|_| "8x256".into());
    shape
        .split_once('x')
        .and_then(|(clients, window)| Some((clients.parse().ok()?, window.parse().ok()?)))
        .filter(|&(clients, window)| clients > 0 && (1..=MAX_UNACKED).contains(&window))
        .expect("BENCH_THROUGHPUT is <clients>x<in flight>, at most MAX_UNACKED in flight a client")
}

/// Both runs on the cluster at `addrs`, the throughput run's `(clients, in flight)` as
/// `shape`, and without fsync the queries' two: each one's name, msgs/s, table row, row of stages
/// (none for queries, which aren't logged), and how many times its clients reattached.
async fn bench(
    addrs: &[String],
    tls: Option<&ClientTls>,
    fsync: driver::Fsync,
    secs: u64,
    shape: (usize, usize),
    stages: &Stages,
) -> Vec<(String, u64, String, Option<String>, usize)> {
    let mut rows = Vec::new();
    let over = if tls.is_some() { ", TLS" } else { "" };
    let over = if fsync == driver::Fsync::EachBatch { format!("{over}, fsync") } else { over.to_owned() };
    for (name, clients, window) in [("latency", 1, 1), ("throughput", shape.0, shape.1)] {
        let (mut latencies, by_stage, reattached) = run(addrs, tls, clients, window, secs, stages).await;
        latencies.sort_unstable();
        let failover = if reattached > 0 { format!(" ⚠️ {reattached} reattached") } else { String::new() };
        let (msgs, row) = table_row(&format!("{name}{over}{failover}"), clients, window, &latencies, secs);
        let both = |sorted: &[u64]| format!("{} / {}", per_mille(sorted, 500), per_mille(sorted, 990));
        let [persisted, committed, applied] = by_stage.each_ref().map(|s| both(s));
        // The stages are the nodes' metrics: none from a cluster elsewhere.
        let stage_row = (!by_stage[0].is_empty())
            .then(|| format!("| {name}{over} | {persisted} | {committed} | {applied} | {} |", both(&latencies)));
        rows.push((format!("{name}{over}"), msgs, row, stage_row, reattached));
    }
    if fsync == driver::Fsync::Never {
        for (name, clients, window) in [("query latency", 1, 1), ("query throughput", shape.0, shape.1)] {
            let mut latencies = run_queries(addrs, tls, clients, window, secs).await;
            latencies.sort_unstable();
            let (msgs, row) = table_row(&format!("{name}{over}"), clients, window, &latencies, secs);
            rows.push((format!("{name}{over}"), msgs, row, None, 0));
        }
    }
    rows
}

/// A run's msgs/s, and its row of the table, from its latencies over `secs`, `sorted`.
fn table_row(name: &str, clients: usize, window: usize, sorted: &[u64], secs: u64) -> (u64, String) {
    let msgs = sorted.len() as u64 / secs;
    let at = |q: usize| per_mille(sorted, q);
    let row =
        format!("| {name} | {clients} × {window} | {msgs} | {} | {} | {} | {} |", at(500), at(990), at(999), at(1000));
    (msgs, row)
}

/// The core alone: three nodes in memory, `clients` sessions each keeping `window` messages in
/// flight as [`keep_sending`] does, the cluster run between their batches as fast as one thread
/// runs it, for a warm-up second then `secs` of the wall's time. Its network and writes take none
/// of the cluster's time, which so never moves: no timer fires, and no session times out. The
/// latency of each message sent after the warm-up, by the wall's clock.
fn in_memory(clients: usize, window: usize, secs: u64) -> Vec<u64> {
    let mut cluster = testing::Builder::new(3)
        .network_latency(Duration::ZERO)
        .write_latency(Duration::ZERO)
        // Every response a client can have in flight, kept for it, as over TCP.
        .configure(|node| node.egress_ring_entries = MAX_UNACKED)
        .build(|_| Ack);
    let ids: Vec<ClientId> = (0..clients).map(|_| cluster.connect(Vec::new()).unwrap()).collect();
    let opened = cluster.run_until(Duration::from_secs(10), |c| ids.iter().all(|&id| c.session(id).is_some()));
    assert!(opened, "every client has a session");
    let payload = vec![7; PAYLOAD_BYTES];
    let mut in_flight = Vec::new();
    for &id in &ids {
        cluster.events(id);
        cluster.send_batch(id, std::iter::repeat_n(payload.clone(), window)).unwrap();
        in_flight.push(std::iter::repeat_n(Instant::now(), window).collect::<VecDeque<_>>());
    }
    let warm = Instant::now() + Duration::from_secs(1);
    let end = warm + Duration::from_secs(secs);
    let mut latencies = Vec::new();
    while Instant::now() < end {
        cluster.run_for(Duration::ZERO);
        for (&id, sent) in ids.iter().zip(&mut in_flight) {
            let (now, mut answered) = (Instant::now(), 0);
            for event in cluster.events(id) {
                let Event::Message(_) = event else { panic!("unexpected {event:?}") };
                let at = sent.pop_front().unwrap();
                if at >= warm {
                    latencies.push(u64::try_from((now - at).as_nanos()).unwrap());
                }
                answered += 1;
            }
            sent.extend(std::iter::repeat_n(Instant::now(), answered));
            cluster.send_batch(id, std::iter::repeat_n(payload.clone(), answered)).unwrap();
        }
    }
    latencies
}

/// `clients` sessions, each keeping `window` queries in flight, for a warm-up second then `secs`:
/// the latency of each query asked after the warm-up and answered before the end.
async fn run_queries(addrs: &[String], tls: Option<&ClientTls>, clients: usize, window: usize, secs: u64) -> Vec<u64> {
    let warm = Instant::now() + Duration::from_secs(1);
    let end = warm + Duration::from_secs(secs);
    let mut tasks = Vec::new();
    for _ in 0..clients {
        let client = Arc::new(connect(addrs, tls).await);
        for _ in 0..window {
            tasks.push(tokio::spawn(keep_querying(Arc::clone(&client), warm, end)));
        }
    }
    let mut latencies = Vec::new();
    for task in tasks {
        latencies.extend(task.await.unwrap());
    }
    latencies
}

/// Asks one query after another until `end`: their latencies, from the warm-up on.
async fn keep_querying(client: Arc<ClusterClient>, warm: Instant, end: Instant) -> Vec<u64> {
    let query = vec![7; PAYLOAD_BYTES];
    let mut latencies = Vec::new();
    loop {
        let sent = Instant::now();
        client.query(query.clone()).await.expect("answered");
        let now = Instant::now();
        if now >= end {
            return latencies;
        }
        if sent >= warm {
            latencies.push(u64::try_from((now - sent).as_nanos()).unwrap());
        }
    }
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
        .map(|&id| raft::Member::new(id, addr(0, id).to_string(), addr(3, id).to_string(), addr(6, id).to_string()))
        .collect();
    for id in ids {
        let mut node = node::Config::new(id, members.clone());
        // Every response a client can have in flight, kept for it.
        node.egress_ring_entries = MAX_UNACKED;
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

/// Keeps `window` messages in flight until `end`, sending as many as were answered, together,
/// once the responses that have arrived are read. Returns their latencies, and how many times the
/// client reattached: its session goes on, each response still once and in order, so the timings
/// hold.
async fn keep_sending(mut client: ClusterClient, window: usize, warm: Instant, end: Instant) -> (Vec<u64>, usize) {
    let payload = vec![7; PAYLOAD_BYTES];
    let mut in_flight: VecDeque<Instant> = std::iter::repeat_n(Instant::now(), window).collect();
    client.send_batch(std::iter::repeat_n(payload.clone(), window)).await.unwrap();
    let (mut latencies, mut reattached, mut events) = (Vec::new(), 0, Vec::new());
    loop {
        assert!(client.recv_many(&mut events, window).await > 0, "the session lasts");
        let mut answered = 0;
        for event in events.drain(..) {
            if let Event::Opened(session) = event {
                eprintln!(
                    "session {session:?} reattached {:.1?} after the warm-up",
                    Instant::now().duration_since(warm)
                );
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
            answered += 1;
        }
        in_flight.extend(std::iter::repeat_n(Instant::now(), answered));
        client.send_batch(std::iter::repeat_n(payload.clone(), answered)).await.unwrap();
    }
}

/// In µs, or ms from 1 ms.
fn micros(ns: u64) -> String {
    if ns < MS { format!("{} µs", ns / 1000) } else { format!("{}.{:02} ms", ns / MS, ns % MS / 10_000) }
}
