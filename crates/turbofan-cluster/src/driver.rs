//! Runs a [`Node`] on tokio: TCP for peers, clients and operators, TLS over it if configured
//! ([`crate::tls`]), and the log, meta and snapshots on disk.
//!
//! One task owns the node and does everything in order. Connection tasks only frame bytes and
//! check handshakes, and hand frames to it over a channel. It carries out the node's outputs
//! by the node's rule, without waiting on the disk: a blocking thread writes a batch, reads back
//! the log records the node asks for, and syncs, while the node goes on with its inputs, and
//! then the task tells the node and sends what waited for it. The batches taken meanwhile are
//! written together, with one sync, which is the group commit. Except a few appends when the
//! log doesn't sync: a copy to the page cache, made on the task, since the handing over and back
//! took three times as long, on the path of every commit of a quiet cluster.
//!
//! Peer connections go one way: each node dials every other to send to it, and reads what
//! arrives on the connections dialled to it. It dials the members, as they change, and the
//! peers in its configuration: for a node joining a cluster, the way to the leader before it
//! knows the members. A frame that can't be sent (the peer is down or
//! slow) is dropped, and Raft sends it again.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

use crate::codec::{self, MAGIC, MAX_FRAME_BYTES};
pub use crate::log::Fsync;
use crate::log::Log;
use crate::meta::MetaFile;
use crate::metrics;
use crate::node::{self, ConnId, Node, Service};
use crate::pipeline::{self, Pipeline};
use crate::raft::{Entries, MAX_READ_BYTES, Member, NodeId, Persist, SnapshotRead};
use crate::tls::{self, NodeTls};
use crate::{snapshot, wire};

/// Inputs waiting for the node, from clients and from the rest: past this, connections wait to
/// hand theirs over.
const MAX_QUEUED_INPUTS: usize = 4096;
/// Inputs taken into one batch before persisting it, a client's or an operator's counted by its
/// frames.
const MAX_BATCH_INPUTS: usize = 1024;
/// The most frames a connection hands the node at once: those it has read whole, together.
const MAX_FRAMES_AN_INPUT: usize = 256;
/// The most a write made on the node's own task appends (see [`writes_inline`]).
const MAX_INLINE_WRITE_BYTES: usize = 4096;
/// Frames waiting to go to one peer or connection. Past this a peer's are dropped (Raft resends)
/// and a client too slow to read is disconnected.
const MAX_QUEUED_FRAMES: usize = 4096;
/// How long to wait before dialling a peer again, doubled each time it fails, up to
/// [`MAX_REDIAL`], and back to this once it's connected.
const REDIAL: Duration = Duration::from_millis(100);
/// The longest wait before dialling an unreachable peer again.
const MAX_REDIAL: Duration = Duration::from_secs(1);
/// How long to wait to accept again after accepting failed.
const ACCEPT_RETRY: Duration = Duration::from_millis(50);
/// How long a node that has stopped waits, for what it sent last to go out.
const STOP_GRACE: Duration = Duration::from_millis(200);
/// How often the node's gauges are published.
const METRICS_EVERY: Duration = Duration::from_secs(1);
/// How long a new connection has to say hello.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);
/// Client connections kept at once: one a session, and there are at most [`node::MAX_SESSIONS`].
/// At the limit a listener accepts no more until one closes, so the file descriptors a node holds
/// are bounded (raise its `ulimit -n` to match).
const MAX_CLIENT_CONNECTIONS: usize = node::MAX_SESSIONS;
/// Peer or operator connections kept at once: a cluster has a few nodes and fewer operators.
/// Neither is timed out for being idle (a follower says nothing to another while a leader lasts,
/// and an operator waits on a snapshot), so keep strangers out with tokens or TLS.
const MAX_OTHER_CONNECTIONS: usize = 256;

/// Where a node keeps its state, and where it listens.
///
/// Non-exhaustive, so that a new setting isn't a breaking change: build it with `new`, then set
/// what differs.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct Config {
    /// The node's own configuration.
    pub node: node::Config,
    /// Which service this cluster runs: a connection for another is refused.
    pub service_id: u32,
    /// The log, meta and snapshots.
    pub data_dir: PathBuf,
    /// Whether the log is synced: by default not, the other nodes' copies being the redundancy.
    /// The term and vote, and snapshots, are synced regardless.
    pub log_fsync: Fsync,
    /// Peers' addresses, this node's included: it listens on its own, and dials the others as
    /// well as the members.
    pub peer_addrs: Vec<(NodeId, SocketAddr)>,
    /// Where it listens for clients.
    pub client_listen: SocketAddr,
    /// Where it listens for operators. Without TLS on it, keep it on a private interface.
    pub admin_listen: SocketAddr,
    /// The token an operator's hello must carry, at most [`MAX_TOKEN_BYTES`]: a connection without
    /// it is refused. With none, any is taken.
    pub admin_token: Option<Vec<u8>>,
    /// The token every node's hello to a peer carries, and must, at most [`MAX_TOKEN_BYTES`]: the
    /// same on every node. With none, a node sends none and takes any peer.
    pub peer_token: Option<Vec<u8>>,
    /// TLS on all three listeners and to peers, or with no peer CA, on the admin listener only.
    /// With none, everything is plain TCP. Every node of the cluster must agree on TLS to peers
    /// and clients; the admin listener's is each node's own.
    pub tls: Option<NodeTls>,
}

impl Config {
    /// A node of `node`'s configuration, serving cluster `service_id`, with its state in
    /// `data_dir`, listening for peers on its own of `peer_addrs` (which it dials the others of),
    /// for clients on `client_listen` and for operators on `admin_listen`. Its log isn't synced,
    /// and it has no tokens and no TLS: set them after.
    #[must_use]
    pub fn new(
        node: node::Config,
        service_id: u32,
        data_dir: impl Into<PathBuf>,
        peer_addrs: Vec<(NodeId, SocketAddr)>,
        client_listen: SocketAddr,
        admin_listen: SocketAddr,
    ) -> Self {
        Self {
            node,
            service_id,
            data_dir: data_dir.into(),
            log_fsync: Fsync::Never,
            peer_addrs,
            client_listen,
            admin_listen,
            admin_token: None,
            peer_token: None,
            tls: None,
        }
    }
}

/// Refuses a token over [`MAX_TOKEN_BYTES`].
fn check_tokens(config: &Config) -> io::Result<()> {
    for (name, token) in [("admin_token", &config.admin_token), ("peer_token", &config.peer_token)] {
        if token.as_ref().is_some_and(|t| t.len() > MAX_TOKEN_BYTES) {
            let message = format!("{name}: at most {MAX_TOKEN_BYTES} bytes");
            return Err(io::Error::new(io::ErrorKind::InvalidInput, message));
        }
    }
    Ok(())
}

/// The longest token a hello carries.
pub const MAX_TOKEN_BYTES: usize = 255;

/// Starts a new cluster from the snapshot file `from`: a copy of a node's, taken off it to back
/// the cluster up. Writes `config.data_dir` for [`serve`] to run, with the same `config`, as the
/// one member of the new cluster: this node, from the snapshot's state, of a new epoch so that
/// nodes of the old cluster can't talk to it. It grows with [`Admin::add_member`]. What the old
/// cluster applied after the snapshot is lost; a client that saw any of it closes its session
/// ([`CloseReason::Diverged`](crate::CloseReason::Diverged)). Returns the snapshot's index and
/// the new epoch.
///
/// [`Admin::add_member`]: crate::Admin::add_member
///
/// # Errors
///
/// If `data_dir` holds anything (it's never overwritten), this node isn't among `config`'s
/// members, the snapshot can't be read or is damaged, or writing fails: then empty `data_dir`
/// and try again.
pub fn restore(from: &Path, config: &Config) -> io::Result<(u64, u64)> {
    let dir = &config.data_dir;
    if dir.exists() && std::fs::read_dir(dir)?.next().is_some() {
        return Err(io::Error::new(io::ErrorKind::AlreadyExists, format!("{} isn't empty", dir.display())));
    }
    let id = config.node.raft.id;
    let me = config.node.raft.members.iter().find(|m| m.id == id);
    let me = me.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "this node isn't among the members"))?;
    let me = Member { learner: false, ..me.clone() };
    let (snapshot, mut bytes) = snapshot::open(from)?;
    let epoch = node::mint_epoch(wall_ns(), id);
    let state = node::with_epoch(&mut bytes, epoch)?;
    let snapshots = dir.join("snapshots");
    crate::log::create_dir_durably(&snapshots)?;
    let writer: node::SnapshotWriter = Box::new(move |out| {
        out.write_all(&state)?;
        io::copy(&mut bytes, out).map(drop)
    });
    snapshot::save(&snapshots, snapshot.index, snapshot.term, vec![me], writer, true)?;
    // Its first election is of a term after the snapshot's, as the old cluster's next would be.
    let (mut meta, _) = MetaFile::open(&dir.join("meta"))?;
    meta.save(crate::meta::Meta { term: snapshot.term, vote: None })?;
    Ok((snapshot.index, epoch))
}

enum Input {
    Peer(NodeId, Vec<u8>),
    /// Frames a connection read together: up to [`MAX_FRAMES_AN_INPUT`].
    Client(ConnId, Vec<Vec<u8>>),
    Admin(ConnId, Vec<Vec<u8>>),
    Open(ConnId, mpsc::Sender<Vec<u8>>),
    Closed(ConnId),
    /// A snapshot from [`node::Outputs::save`] is written, or writing it failed.
    SnapshotSaved(io::Result<crate::raft::Snapshot>),
}

/// Runs a node of `service` until the cluster is shut down (`Ok`), or an IO error stops it.
///
/// # Errors
///
/// If the data directory can't be opened (or another process has it), a listener can't bind,
/// or the disk fails. A failed write or sync leaves the node's durable state unknown, so it
/// stops; restart it, and it recovers from what's on disk.
pub async fn serve<S: Service + 'static>(config: Config, service: S) -> io::Result<()> {
    check_tokens(&config)?;
    let (tls, reloading) = tls::start(config.tls.clone())?;
    let peer_ids = tls.borrow().as_ref().is_some_and(|t| t.peer_ids);
    let id = config.node.raft.id;
    let (node, storage, lock, clock) = open_node(&config, service)?;
    let snapshots = storage.snapshots.clone();

    let own = config.peer_addrs.iter().find(|(m, _)| *m == id).map(|&(_, a)| a);
    let own = own.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no peer address for this node"))?;
    let (peer_listener, client_listener, admin_listener) = (
        TcpListener::bind(own).await?,
        TcpListener::bind(config.client_listen).await?,
        TcpListener::bind(config.admin_listen).await?,
    );
    let (inputs, rx) = mpsc::channel(MAX_QUEUED_INPUTS);
    // Clients' apart, so a backlogged leader can stop taking them and still take peers' acks.
    let (client_inputs, client_rx) = mpsc::channel(MAX_QUEUED_INPUTS);
    // Every task is in here, so they all stop when `serve` does.
    let mut tasks = JoinSet::new();
    let epoch = Arc::new(AtomicU64::new(node.epoch()));
    let hello = Hello { service_id: config.service_id, node_id: Some(id), epoch };
    let peer_token: Option<Arc<[u8]>> = config.peer_token.as_deref().map(Arc::from);
    tasks.spawn(reloading);
    let peers = accept_peers(peer_listener, hello.clone(), peer_token.clone(), tls.clone(), peer_ids, inputs.clone());
    tasks.spawn(peers);
    let next_conn = Arc::new(AtomicU64::new(1));
    let idle = Some(config.node.session_timeout);
    let clients = Gate {
        role: wire::Role::Client,
        hello: hello.clone(),
        token: None,
        tls: tls.clone(),
        inputs: client_inputs,
        idle,
    };
    let token = config.admin_token.as_deref().map(Arc::from);
    let operators = Gate { role: wire::Role::Admin, token, inputs: inputs.clone(), idle: None, ..clients.clone() };
    tasks.spawn(accept(client_listener, clients, next_conn.clone()));
    tasks.spawn(accept(admin_listener, operators, next_conn));
    let contacts = config.peer_addrs.iter().map(|&(m, a)| (m, a.to_string())).collect();
    let mut core = Core {
        node,
        storage: Some(storage),
        pipeline: Pipeline::default(),
        writing: None,
        snapshots,
        lock,
        inputs,
        clock,
        published: Instant::now(),
        hello,
        peer_token: peer_token.unwrap_or_else(|| Arc::from(&[][..])),
        tls,
        peer_ids,
        contacts,
        peers: HashMap::new(),
        dials: JoinSet::new(),
        conns: HashMap::new(),
        inline_writes: config.log_fsync == Fsync::Never,
        written_inline: None,
    };
    let peers = core.node.raft().peers();
    core.dial(&peers);
    core.run(rx, client_rx).await
}

/// The node, and everything it sends through.
struct Core<S> {
    node: Node<S>,
    /// Away on a blocking thread while it persists, unless the write is made here.
    storage: Option<Storage>,
    /// The node's batches, between taken and durable.
    pipeline: Pipeline,
    /// The write under way, and the storage back with it.
    writing: Option<JoinHandle<Written>>,
    /// Whether a small write is made on this task ([`writes_inline`]): only when the log doesn't
    /// sync, so a write is a copy to the page cache.
    inline_writes: bool,
    /// A write made on this task, for the node to be told of before it takes more inputs.
    written_inline: Option<Written>,
    /// Where snapshots are saved, apart from the batches, reporting back on `inputs`.
    snapshots: PathBuf,
    /// The node's directory lock ([`Log::lock`]), for a snapshot being saved to hold: else, the
    /// node's task stopped, it goes with the storage, and a node started again in this process
    /// saves its own beside it.
    lock: Arc<File>,
    inputs: mpsc::Sender<Input>,
    clock: Clock,
    /// When the gauges were last published.
    published: Instant,
    hello: Hello,
    /// What its hellos to peers carry: [`Config::peer_token`], or nothing.
    peer_token: Arc<[u8]>,
    /// Its TLS, as it stands: what it dials peers with, if it has it.
    tls: tls::Current,
    /// Whether a peer's certificate must name the member dialled ([`NodeTls::peer_ids`]).
    peer_ids: bool,
    /// Peers to dial whatever the members are.
    contacts: Vec<(NodeId, String)>,
    /// The peers dialled: where, the frames for them, and the task dialling.
    peers: HashMap<NodeId, (String, mpsc::Sender<Vec<u8>>, AbortHandle)>,
    dials: JoinSet<()>,
    conns: HashMap<ConnId, mpsc::Sender<Vec<u8>>>,
}

impl<S: Service + 'static> Core<S> {
    async fn run(mut self, mut rx: mpsc::Receiver<Input>, mut client_rx: mpsc::Receiver<Input>) -> io::Result<()> {
        loop {
            self.flush();
            if let Some(written) = self.written_inline.take() {
                self.written(written)?;
                continue;
            }
            if self.published.elapsed() >= METRICS_EVERY {
                self.published = Instant::now();
                self.node.metrics(self.clock.now()).publish();
            }
            let stopped = self.node.stopped();
            if stopped && self.writing.is_none() {
                tokio::time::sleep(STOP_GRACE).await;
                return Ok(());
            }
            let take_inputs = !stopped && self.pipeline.takes_inputs();
            let wake = self.clock.instant(self.node.next_deadline());
            tokio::select! {
                written = async { self.writing.as_mut().expect("guarded").await }, if self.writing.is_some() => {
                    self.writing = None;
                    self.written(written.map_err(io::Error::other)?)?;
                }
                input = rx.recv(), if take_inputs => {
                    let Some(input) = input else { return Ok(()) };
                    let mut taken = self.handle(input)?;
                    while taken < MAX_BATCH_INPUTS {
                        let Ok(input) = rx.try_recv() else { break };
                        taken += self.handle(input)?;
                    }
                }
                // A backlogged leader leaves clients waiting, so TCP holds them back, until what
                // it has proposed commits.
                input = client_rx.recv(), if take_inputs && !self.node.raft().backlogged() => {
                    let Some(input) = input else { return Ok(()) };
                    let mut taken = self.handle(input)?;
                    while taken < MAX_BATCH_INPUTS {
                        if self.node.raft().backlogged() {
                            break;
                        }
                        let Ok(input) = client_rx.try_recv() else { break };
                        taken += self.handle(input)?;
                    }
                }
                () = tokio::time::sleep_until(wake.into()), if take_inputs => self.node.on_tick(self.clock.now()),
            }
        }
    }

    /// Hands `input` to the node, returning how many inputs it counts as: a client's or an
    /// operator's, its frames.
    fn handle(&mut self, input: Input) -> io::Result<usize> {
        let now = self.clock.now();
        match input {
            Input::Peer(from, frame) => self.node.on_peer(from, &decode(&frame), now),
            Input::Client(conn, frames) => {
                for frame in &frames {
                    self.node.on_client(conn, &decode(frame), now);
                }
                return Ok(frames.len());
            }
            Input::Admin(conn, frames) => {
                for frame in &frames {
                    self.node.on_admin(conn, &decode(frame), now);
                }
                return Ok(frames.len());
            }
            Input::Open(conn, tx) => {
                self.conns.insert(conn, tx);
            }
            Input::Closed(conn) => {
                self.conns.remove(&conn);
                self.node.on_disconnect(conn);
            }
            Input::SnapshotSaved(saved) => self.node.on_snapshot_saved(saved?),
        }
        Ok(1)
    }

    /// Takes the node's outputs, and carries out what goes at once (see [`crate::pipeline`]),
    /// starting a write if there's one to start. A leader's appends go first, so followers write
    /// them while it does.
    fn flush(&mut self) {
        let taken = self.pipeline.take(&mut self.node);
        self.hello.epoch.store(self.node.epoch(), Ordering::Relaxed);
        for &(stage, ns) in &taken.latencies {
            metrics::record_latency(stage, ns);
        }
        self.send_peers(taken.peer_appends);
        self.send_clients(taken.client_sends);
        self.send_peers(taken.peer_sends);
        if let Some(peers) = &taken.peers {
            self.dial(peers);
        }
        if let Some(save) = taken.save {
            let (dir, inputs, lock) = (self.snapshots.clone(), self.inputs.clone(), self.lock.clone());
            tokio::task::spawn_blocking(move || {
                // Held to the end: stopped, a node's task goes, but not this thread.
                let _lock = lock;
                let saved = snapshot::save(&dir, save.index, save.term, save.members, save.writer, true);
                // Gone only if the node stopped.
                let _ = inputs.blocking_send(Input::SnapshotSaved(saved));
            });
        }
        if let Some(batches) = taken.write {
            let mut storage = self.storage.take().expect("storage is back between writes");
            if self.inline_writes && writes_inline(&batches) {
                let done = storage.write(&batches);
                self.written_inline = Some(Written { storage, done });
                return;
            }
            self.writing = Some(tokio::task::spawn_blocking(move || {
                let done = storage.write(&batches);
                Written { storage, done }
            }));
        }
    }

    /// The write is durable: tells the node, and sends what waited for it.
    fn written(&mut self, written: Written) -> io::Result<()> {
        let Written { storage, done } = written;
        self.storage = Some(storage);
        let (reads, chunks, captured) = done?;
        self.send_clients(captured);
        let mut snapshot = None;
        if let Some(index) = self.pipeline.restore() {
            // ponytail: reads the snapshot on the node's task, which waits for it anyway; a
            // blocking thread if a big restore starves the connection tasks.
            let loaded = snapshot::load(&self.snapshots)?.filter(|(s, _)| s.index == index);
            let (_, bytes) = loaded.ok_or_else(|| io::Error::other("the installed snapshot is gone"))?;
            snapshot = Some(bytes);
        }
        let from = snapshot.as_mut().map(|b| -> &mut dyn io::Read { b });
        let now = self.clock.now();
        let sends = self.pipeline.written(&mut self.node, &reads, &chunks, from, now)?;
        self.send_peers(sends);
        Ok(())
    }

    fn send_clients(&mut self, sends: Vec<(ConnId, Vec<u8>)>) {
        for (conn, frame) in sends {
            let Some(tx) = self.conns.get(&conn) else { continue };
            if tx.try_send(frame).is_err() {
                // Too slow to read, or gone: its task ends when its sender is dropped.
                self.conns.remove(&conn);
                self.node.on_disconnect(conn);
            }
        }
    }

    /// Dials `members` and the contacts, a member's address taking precedence, and stops dialling
    /// any other peer.
    fn dial(&mut self, members: &[Member]) {
        let mut wanted: HashMap<NodeId, String> = self.contacts.iter().cloned().collect();
        wanted.extend(members.iter().map(|m| (m.id, m.peer.clone())));
        wanted.retain(|&id, _| Some(id) != self.hello.node_id);
        self.peers.retain(|id, (addr, _, task)| {
            let keep = wanted.get(id) == Some(addr);
            if !keep {
                task.abort();
            }
            keep
        });
        for (id, addr) in wanted {
            if !self.peers.contains_key(&id) {
                let (tx, rx) = mpsc::channel(MAX_QUEUED_FRAMES);
                let task = self.dials.spawn(dial_peer(
                    addr.clone(),
                    self.hello.clone(),
                    self.peer_token.clone(),
                    self.tls.clone(),
                    self.peer_ids.then_some(id),
                    rx,
                ));
                self.peers.insert(id, (addr, tx, task));
            }
        }
    }

    fn send_peers(&self, frames: Vec<(NodeId, Vec<u8>)>) {
        for (to, frame) in frames {
            if let Some((_, tx, _)) = self.peers.get(&to) {
                // Full or down: Raft resends.
                let _ = tx.try_send(frame);
            }
        }
    }
}

/// A frame from a connection task, which already checked it decodes.
fn decode(frame: &[u8]) -> wire::Decoded<'_> {
    codec::decode_frame(frame).ok().flatten().expect("connection tasks pass only whole, valid frames").0
}

/// The log, the meta file and the snapshot directory.
#[derive(Debug)]
struct Storage {
    dir: PathBuf,
    log: Log,
    meta: MetaFile,
    snapshots: PathBuf,
    /// [`node::Config::snapshots_kept`].
    snapshots_kept: usize,
}

impl Storage {
    /// Opens a node's durable state, keeping `window_bytes` of the log's records in memory.
    #[allow(clippy::type_complexity, reason = "the four parts of a node's durable state")]
    fn open(
        dir: &Path,
        fsync: Fsync,
        window_bytes: usize,
    ) -> io::Result<(Self, crate::meta::Meta, Option<(crate::raft::Snapshot, impl io::Read + use<>)>, Entries)> {
        let snapshots = dir.join("snapshots");
        crate::log::create_dir_durably(&snapshots)?;
        let snapshot = snapshot::load(&snapshots)?;
        let mut entries = Entries::default();
        let mut log = Log::open(&dir.join("log"), fsync, |record| entries.recover(record, window_bytes))?;
        log.resume(snapshot.as_ref().map(|(s, _)| (s.index, s.term)), &mut entries)?;
        let (meta_file, meta) = MetaFile::open(&dir.join("meta"))?;
        Ok((Self { dir: dir.to_owned(), log, meta: meta_file, snapshots, snapshots_kept: 1 }, meta, snapshot, entries))
    }

    /// Writes `batches` in order, each one's reads and captures after its operations, then syncs.
    /// Returns the records and snapshot chunks read, a read that an earlier batch asked for once,
    /// and the answers to the captures.
    ///
    /// Appends running on from one batch to the next are written together: a leader gives a
    /// small batch nearly every turn, and a write of hundreds, each its own syscall, took far
    /// longer than its sync.
    fn write(&mut self, batches: &[pipeline::Batch]) -> io::Result<Done> {
        let (mut reads, mut chunks, mut captured) = (Vec::new(), Vec::new(), Vec::new());
        let (mut froms, mut offsets) = (HashSet::new(), HashSet::new());
        let mut appends = Vec::new();
        for batch in batches {
            for op in &batch.persist {
                if let Persist::Append(records) = op {
                    appends.extend_from_slice(records);
                } else {
                    self.append(&mut appends)?;
                    self.apply(op)?;
                }
            }
            if !batch.reads.is_empty() || !batch.snapshot_reads.is_empty() || !batch.captures.is_empty() {
                self.append(&mut appends)?;
            }
            for &from in &batch.reads {
                if froms.insert(from) {
                    reads.push(self.log.read(from, MAX_READ_BYTES)?);
                }
            }
            for &read @ SnapshotRead { index, offset } in &batch.snapshot_reads {
                if offsets.insert(read) {
                    chunks.push((index, offset, snapshot::read(&self.snapshots, index, offset, MAX_READ_BYTES)?));
                }
            }
            captured.extend(batch.captures.iter().map(|c| (c.conn, self.capture(c))));
        }
        self.append(&mut appends)?;
        self.log.sync()?;
        Ok((reads, chunks, captured))
    }

    /// Makes `capture`, and answers it: a capture that fails is refused, saying why.
    fn capture(&self, capture: &node::Capture) -> Vec<u8> {
        let (refused, snapshot_index, detail) = match crate::capture::make(&self.dir, &self.log, capture.to) {
            Ok((dir, snapshot)) => (0, snapshot, dir.display().to_string()),
            Err(e) => (1, 0, format!("capture: {e}")),
        };
        // A path or an error, far shorter than a payload: cut to fit, should one not be.
        let detail = &detail.as_bytes()[..detail.len().min(codec::MAX_PAYLOAD_BYTES)];
        let mut frame = Vec::new();
        let (request_id, to) = (capture.request_id, capture.to);
        let captured = wire::Captured { request_id, refused, snapshot_index, to, detail };
        codec::encode_frame(&captured, &mut frame).expect("a payload's length fits");
        frame
    }

    /// Writes the `records` gathered so far, if any, and empties them.
    fn append(&mut self, records: &mut Vec<u8>) -> io::Result<()> {
        if !records.is_empty() {
            self.log.append(records)?;
            records.clear();
        }
        Ok(())
    }

    fn apply(&mut self, op: &Persist) -> io::Result<()> {
        match op {
            Persist::Meta(meta) => self.meta.save(*meta)?,
            Persist::Append(records) => self.log.append(records)?,
            Persist::TruncateFrom(index) => self.log.truncate_from(*index)?,
            Persist::Trust => self.log.trust()?,
            Persist::Compact { log, snapshot } => {
                self.log.compact_to(*log)?;
                snapshot::remove_older(&self.snapshots, *snapshot, self.snapshots_kept)?;
            }
            Persist::SnapshotChunk { offset, bytes, .. } => snapshot::write_chunk(&self.snapshots, *offset, bytes)?,
            Persist::InstallSnapshot { snapshot, replace_log } => {
                snapshot::install(&self.snapshots, snapshot, true)?;
                snapshot::remove_older(&self.snapshots, snapshot.index, self.snapshots_kept)?;
                if *replace_log {
                    self.log.reset(snapshot.index + 1)?;
                } else {
                    self.log.compact_to(snapshot.index)?;
                }
            }
        }
        Ok(())
    }
}

/// A write done: what was read for it, or the error that stops the node.
struct Written {
    storage: Storage,
    done: io::Result<Done>,
}

/// What a write read: records, snapshot chunks (index, offset, bytes), and the answers to
/// captures, for admin connections.
type Done = (Vec<Vec<u8>>, Vec<(u64, u64, Vec<u8>)>, Vec<(ConnId, Vec<u8>)>);

/// Whether to make the write of `batches` on the node's own task rather than a blocking thread:
/// a few appends, nothing read back or captured. Handed to a thread and back, such a write took
/// three times as long as it does here: on the path of every commit when the cluster is quiet.
/// A busy node's batches are bigger, and go to a thread, for the node to go on while they're
/// written.
fn writes_inline(batches: &[pipeline::Batch]) -> bool {
    let mut bytes = 0;
    for batch in batches {
        if !batch.reads.is_empty() || !batch.snapshot_reads.is_empty() || !batch.captures.is_empty() {
            return false;
        }
        for op in &batch.persist {
            let Persist::Append(records) = op else { return false };
            bytes += records.len();
        }
    }
    bytes <= MAX_INLINE_WRITE_BYTES
}

/// How far the clock is slewed toward the wall clock: up to this many parts per million faster
/// or slower than the monotonic clock, as NTP slews.
const SLEW_PPM: u64 = 500;

/// Cluster time, what the leader stamps into the log: nanoseconds since the Unix epoch. It
/// starts at the wall clock and runs on the monotonic clock, slewed toward the wall clock by up
/// to [`SLEW_PPM`]. So it tracks the wall clock, and other nodes', over a long uptime, but never
/// jumps (Raft's timeouts run on it too) and never goes backwards: a wall clock stepped back is
/// slowly waited for. The node holds cluster time still if a new leader's is behind.
#[derive(Debug, Clone, Copy)]
struct Clock {
    start: Instant,
    /// The monotonic clock, since `start`, and cluster time, when last read.
    mono: u64,
    ns: u64,
}

impl Clock {
    fn new() -> Self {
        Self { start: Instant::now(), mono: 0, ns: wall_ns() }
    }

    fn now(&mut self) -> u64 {
        let mono = u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX / 2);
        self.advance(mono, wall_ns())
    }

    /// Cluster time at monotonic time `mono`, slewed toward wall time `wall`.
    fn advance(&mut self, mono: u64, wall: u64) -> u64 {
        let elapsed = mono.saturating_sub(self.mono);
        let slew = elapsed / (1_000_000 / SLEW_PPM);
        let free = self.ns + elapsed;
        let ns = if wall > free { free + (wall - free).min(slew) } else { free - (free - wall).min(slew) };
        (self.mono, self.ns) = (self.mono.max(mono), ns);
        ns
    }

    /// When the monotonic clock reaches cluster time `ns`, near enough: a tick that's early by
    /// the slew finds nothing due and waits again.
    fn instant(&self, ns: u64) -> Instant {
        self.start + Duration::from_nanos(self.mono + ns.saturating_sub(self.ns))
    }
}

fn wall_ns() -> u64 {
    let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
    u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX / 2)
}

/// What a node says, and checks, in the handshake.
#[derive(Debug, Clone)]
pub(crate) struct Hello {
    pub(crate) service_id: u32,
    /// None for a client or an operator.
    pub(crate) node_id: Option<NodeId>,
    /// The cluster's epoch ([`Node::epoch`]), as the node last applied it: 0 for none yet, and
    /// for a client or an operator.
    pub(crate) epoch: Arc<AtomicU64>,
}

impl Hello {
    /// A hello as `role`, with a `token` (empty for none).
    ///
    /// # Panics
    ///
    /// If the token is over [`MAX_TOKEN_BYTES`].
    pub(crate) fn frame(&self, role: wire::Role, token: &[u8]) -> Vec<u8> {
        let mut frame = Vec::new();
        let hello = wire::Hello {
            magic: MAGIC,
            service_id: self.service_id,
            role,
            node_id: NodeId::to_wire(self.node_id),
            epoch: self.epoch(),
            token,
        };
        codec::encode_frame(&hello, &mut frame).expect("a token of at most 255 bytes");
        frame
    }

    fn epoch(&self) -> u64 {
        self.epoch.load(Ordering::Relaxed)
    }

    /// Whether a peer that knows epoch `theirs` is of this cluster: unless both know one, and
    /// they differ.
    fn same_cluster(&self, theirs: u64) -> bool {
        let ours = self.epoch();
        ours == 0 || theirs == 0 || ours == theirs
    }

    /// The hello in `frame`, if it's one for this service from `role`.
    pub(crate) fn check<'a>(&self, frame: &'a [u8], role: wire::Role) -> Option<wire::HelloRef<'a>> {
        let Ok(Some((wire::Decoded::Hello(h), _))) = codec::decode_frame(frame) else { return None };
        (h.magic() == MAGIC && h.service_id() == self.service_id && h.role() == role).then_some(h)
    }
}

/// Whether `a` is `b`, comparing every byte: how long a wrong token takes says nothing of how
/// much of it was right (only, perhaps, of its length).
fn same(a: &[u8], b: &[u8]) -> bool {
    a.len() == b.len() && a.iter().zip(b).fold(0, |diff, (x, y)| diff | (x ^ y)) == 0
}

/// Writes `frames` and flushes them: TLS may hold some back until then.
pub(crate) async fn write_frames<'a>(
    writer: &mut (impl AsyncWrite + Unpin),
    frames: impl IntoIterator<Item = &'a [u8]>,
) -> io::Result<()> {
    for frame in frames {
        writer.write_all(frame).await?;
    }
    writer.flush().await
}

/// Reads one whole frame, length and all, refusing one over [`MAX_FRAME_BYTES`].
///
/// # Errors
///
/// If reading fails, the connection closes, or the frame is too long.
pub(crate) async fn read_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let len = reader.read_u32_le().await?;
    let len = usize::try_from(len).map_err(io::Error::other)?;
    if len > MAX_FRAME_BYTES {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "frame over MAX_FRAME_BYTES"));
    }
    let mut frame = vec![0; 4 + len];
    frame[..4].copy_from_slice(&u32::try_from(len).expect("checked").to_le_bytes());
    reader.read_exact(&mut frame[4..]).await?;
    Ok(frame)
}

/// Reads a frame that decodes, skipping any of a type this node doesn't know (from a newer
/// version): a malformed one ends the connection.
async fn read_valid_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    loop {
        let frame = read_frame(reader).await?;
        match codec::decode_frame(&frame) {
            Ok(Some(_)) => return Ok(frame),
            Err(e) if e.is_unknown_message() => {}
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed frame")),
        }
    }
}

/// How long a connection may go without a frame, timed by one timer for its life. A timer a frame
/// would register and drop an entry in tokio's timer, under its lock, for every message. This one
/// moves on only when it fires, to the limit after the last frame: once a limit at most.
struct Idle {
    limit: Duration,
    last_frame: tokio::time::Instant,
    timer: Pin<Box<tokio::time::Sleep>>,
}

impl Idle {
    fn new(limit: Duration) -> Self {
        let now = tokio::time::Instant::now();
        Self { limit, last_frame: now, timer: Box::pin(tokio::time::sleep_until(now + limit)) }
    }
}

/// Reads into `frames` those of `reader`'s that have arrived whole, waiting for none, until there
/// are [`MAX_FRAMES_AN_INPUT`]; skipping any of a type this node doesn't know, as
/// [`read_valid_frame`] does. A malformed one fails, after the frames before it.
async fn read_buffered(reader: &mut tls::ReadHalf, frames: &mut Vec<Vec<u8>>) -> io::Result<()> {
    while frames.len() < MAX_FRAMES_AN_INPUT && whole_frame(reader.buffer()) {
        // In the buffer: read without waiting on the connection.
        let frame = read_frame(reader).await?;
        match codec::decode_frame(&frame) {
            Ok(Some(_)) => frames.push(frame),
            Err(e) if e.is_unknown_message() => {}
            _ => return Err(io::Error::new(io::ErrorKind::InvalidData, "malformed frame")),
        }
    }
    Ok(())
}

/// Whether `buffered` starts with a whole frame, its length and all.
fn whole_frame(buffered: &[u8]) -> bool {
    let Some(len) = buffered.first_chunk::<4>().map(|len| u32::from_le_bytes(*len)) else { return false };
    usize::try_from(len).is_ok_and(|len| buffered.len() - 4 >= len)
}

/// [`read_valid_frame`], failing if none comes within `idle`'s limit of the last. The read lives
/// through the timer firing early: a frame half read isn't lost.
async fn read_within(reader: &mut (impl AsyncRead + Unpin), idle: Option<&mut Idle>) -> io::Result<Vec<u8>> {
    let Some(idle) = idle else { return read_valid_frame(reader).await };
    let read = read_valid_frame(reader);
    tokio::pin!(read);
    loop {
        tokio::select! {
            frame = &mut read => {
                idle.last_frame = tokio::time::Instant::now();
                return frame;
            }
            () = idle.timer.as_mut() => {
                let due = idle.last_frame + idle.limit;
                if tokio::time::Instant::now() >= due {
                    return Err(io::Error::from(io::ErrorKind::TimedOut));
                }
                idle.timer.as_mut().reset(due);
            }
        }
    }
}

/// Reads from peers: any node of this service whose hello has `token`, if there is one, since a
/// member this node hasn't heard was added may lead; and with `peer_ids`, whose certificate names
/// the node its hello says.
async fn accept_peers(
    listener: TcpListener,
    hello: Hello,
    token: Option<Arc<[u8]>>,
    tls: tls::Current,
    peer_ids: bool,
    inputs: mpsc::Sender<Input>,
) {
    let mut conns = JoinSet::new();
    loop {
        room(&mut conns, MAX_OTHER_CONNECTIONS).await;
        let Ok((stream, _)) = listener.accept().await else {
            // Out of file descriptors, say: not again at once, in a loop that holds a core.
            tokio::time::sleep(ACCEPT_RETRY).await;
            continue;
        };
        let tls = tls.borrow().as_ref().and_then(|t| t.peer.clone());
        let (inputs, token, hello) = (inputs.clone(), token.clone(), hello.clone());
        conns.spawn(async move {
            let handshake = async {
                let (mut stream, cert) = tls::accept(stream, tls.as_ref()).await?;
                let first = read_frame(&mut stream).await?;
                io::Result::Ok((stream, cert, first))
            };
            let Ok(Ok((mut stream, cert, first))) = tokio::time::timeout(HELLO_TIMEOUT, handshake).await else {
                return;
            };
            let theirs = hello.check(&first, wire::Role::Peer);
            let theirs = theirs.filter(|h| token.as_ref().is_none_or(|token| same(h.token(), token)));
            let theirs = theirs.filter(|h| hello.same_cluster(h.epoch()));
            let Some((from, epoch)) = theirs.and_then(|h| Some((NodeId::from_wire(h.node_id())?, h.epoch()))) else {
                return;
            };
            if Some(from) == hello.node_id || (peer_ids && !tls::names(cert.as_ref(), from)) {
                return;
            }
            while let Ok(frame) = read_valid_frame(&mut stream).await {
                // Checked again as this node learns its epoch, or it changes: a connection from
                // another cluster's node is dropped then.
                if !hello.same_cluster(epoch) || inputs.send(Input::Peer(from, frame)).await.is_err() {
                    return;
                }
            }
        });
    }
}

/// Waits until `conns` has fewer than `max` running, reaping the finished.
async fn room(conns: &mut JoinSet<()>, max: usize) {
    while conns.try_join_next().is_some() {}
    while conns.len() >= max {
        conns.join_next().await;
    }
}

/// What a client or operator listener's connections share.
#[derive(Clone)]
struct Gate {
    role: wire::Role,
    hello: Hello,
    /// The token their hellos must have, if there is one.
    token: Option<Arc<[u8]>>,
    /// The node's TLS, as it stands.
    tls: tls::Current,
    inputs: mpsc::Sender<Input>,
    /// How long a connection may say nothing: for a client the session timeout, by which its
    /// session is gone.
    idle: Option<Duration>,
}

/// Takes clients or operators, through `gate`.
async fn accept(listener: TcpListener, gate: Gate, next_conn: Arc<AtomicU64>) {
    let max = if gate.role == wire::Role::Client { MAX_CLIENT_CONNECTIONS } else { MAX_OTHER_CONNECTIONS };
    let mut conns = JoinSet::new();
    loop {
        room(&mut conns, max).await;
        let Ok((stream, _)) = listener.accept().await else {
            // Out of file descriptors, say: not again at once, in a loop that holds a core.
            tokio::time::sleep(ACCEPT_RETRY).await;
            continue;
        };
        let conn = next_conn.fetch_add(1, Ordering::Relaxed);
        conns.spawn(serve_connection(stream, conn, gate.clone()));
    }
}

/// A client's or operator's connection: hello each way, then frames in to the node and out from
/// it, until either side closes or nothing comes for `idle`.
async fn serve_connection(stream: TcpStream, conn: ConnId, gate: Gate) {
    let Gate { role, hello, token, tls, inputs, idle } = gate;
    let admin = role == wire::Role::Admin;
    let tls = tls.borrow().as_ref().and_then(|t| if admin { Some(t.admin.clone()) } else { t.client.clone() });
    let handshake = async {
        let (mut stream, _) = tls::accept(stream, tls.as_ref()).await?;
        let first = read_frame(&mut stream).await?;
        io::Result::Ok((stream, first))
    };
    let Ok(Ok((stream, first))) = tokio::time::timeout(HELLO_TIMEOUT, handshake).await else { return };
    let (mut reader, mut writer) = stream.split();
    let Some(theirs) = hello.check(&first, role) else { return };
    if token.is_some_and(|token| !same(theirs.token(), &token)) {
        let mut refusal = Vec::new();
        codec::encode_frame(&wire::Unauthorized {}, &mut refusal).expect("empty");
        let _ = write_frames(&mut writer, [&refusal[..]]).await;
        return;
    }
    if write_frames(&mut writer, [&hello.frame(role, &[])[..]]).await.is_err() {
        return;
    }
    let (tx, mut rx) = mpsc::channel::<Vec<u8>>(MAX_QUEUED_FRAMES);
    if inputs.send(Input::Open(conn, tx)).await.is_err() {
        return;
    }
    let mut write = tokio::spawn(async move {
        while let Some(frame) = rx.recv().await {
            if writer.write_all(&frame).await.is_err() {
                return;
            }
            // Flushed (TLS may hold some back) once there's nothing more to write.
            if rx.is_empty() && writer.flush().await.is_err() {
                return;
            }
        }
    });
    let mut idle = idle.map(Idle::new);
    loop {
        tokio::select! {
            frame = read_within(&mut reader, idle.as_mut()) => {
                let Ok(frame) = frame else { break };
                // With the rest read whole meanwhile: one input for them all, not one a frame, which
                // a busy node's connections queued for.
                let mut frames = vec![frame];
                let rest = read_buffered(&mut reader, &mut frames).await;
                let input = if role == wire::Role::Admin { Input::Admin(conn, frames) } else { Input::Client(conn, frames) };
                if inputs.send(input).await.is_err() || rest.is_err() {
                    break;
                }
            }
            // The node dropped the connection, or writing failed.
            _ = &mut write => break,
        }
    }
    write.abort();
    let _ = inputs.send(Input::Closed(conn)).await;
}

/// The node `config` describes, from what's in its data directory: with its storage, a handle on
/// the directory's lock ([`Log::lock`]), and the clock it started at.
fn open_node<S: Service>(config: &Config, service: S) -> io::Result<(Node<S>, Storage, Arc<File>, Clock)> {
    let (mut storage, meta, snapshot, log) =
        Storage::open(&config.data_dir, config.log_fsync, config.node.raft.log_window_bytes)?;
    storage.snapshots_kept = config.node.snapshots_kept;
    let mut clock = Clock::new();
    let mut node =
        Node::new(config.node.clone(), service, meta, snapshot.as_ref().map(|s| s.0.clone()), log, clock.now());
    if let Some((_, mut bytes)) = snapshot {
        node.restore(&mut bytes)?;
    }
    let lock = Arc::new(storage.log.lock()?);
    Ok((node, storage, lock, clock))
}

/// The wait before dialling again after one of `redial` failed.
fn backoff(redial: Duration) -> Duration {
    (redial * 2).min(MAX_REDIAL)
}

/// Keeps a connection to a peer and writes what the node sends it, redialling when it drops,
/// less often the longer it's unreachable.
async fn dial_peer(
    addr: String,
    hello: Hello,
    token: Arc<[u8]>,
    tls: tls::Current,
    node: Option<NodeId>,
    mut rx: mpsc::Receiver<Vec<u8>>,
) {
    let mut batch = Vec::new();
    let mut redial = REDIAL;
    loop {
        let dial = tls.borrow().as_ref().and_then(|t| t.dial.clone());
        let connected = tokio::time::timeout(HELLO_TIMEOUT, tls::connect(&addr, dial.as_ref(), node)).await;
        let Ok(Ok(mut stream)) = connected else {
            tokio::time::sleep(redial).await;
            redial = backoff(redial);
            continue;
        };
        let epoch = hello.epoch();
        if write_frames(&mut stream, [&hello.frame(wire::Role::Peer, &token)[..]]).await.is_err() {
            tokio::time::sleep(redial).await;
            redial = backoff(redial);
            continue;
        }
        redial = REDIAL;
        loop {
            let Some(frame) = rx.recv().await else { return };
            // Its epoch changed since its hello said: dialled again, for the peer to check the new
            // one. Else one that said none would go on being taken for one of any cluster.
            if hello.epoch() != epoch {
                break;
            }
            batch.clear();
            batch.extend_from_slice(&frame);
            while batch.len() < MAX_FRAME_BYTES {
                let Ok(frame) = rx.try_recv() else { break };
                batch.extend_from_slice(&frame);
            }
            if write_frames(&mut stream, [&batch[..]]).await.is_err() {
                break;
            }
        }
        tokio::time::sleep(REDIAL).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Appends from several batches are written together, and a batch's read still sees every
    /// append before it, its own included.
    #[test]
    fn a_writes_appends_are_joined_and_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, ..) = Storage::open(dir.path(), Fsync::Never, usize::MAX).unwrap();
        let append = |index| {
            let mut records = Vec::new();
            crate::codec::Record { term: 1, index, payload: b"x" }.encode_into(&mut records);
            let (reads, snapshot_reads, captures) = (Vec::new(), Vec::new(), Vec::new());
            pipeline::Batch { persist: vec![Persist::Append(records)], reads, snapshot_reads, captures }
        };
        let read = pipeline::Batch { reads: vec![1], ..append(3) };
        let (reads, ..) = storage.write(&[append(1), append(2), read, append(4)]).unwrap();
        let indices: Vec<u64> = crate::codec::records(&reads[0]).map(|r| r.unwrap().index).collect();
        assert_eq!(indices, [1, 2, 3], "the read follows its own batch's append, not the next");
        assert_eq!(storage.log.next_index(), 5);
    }

    /// A write is made on the node's task only if it's appends alone, of at most
    /// `MAX_INLINE_WRITE_BYTES` across its batches: anything else, or more, goes to a thread.
    #[test]
    fn only_a_few_appends_are_written_inline() {
        let append = |bytes| {
            let (reads, snapshot_reads, captures) = (Vec::new(), Vec::new(), Vec::new());
            pipeline::Batch { persist: vec![Persist::Append(vec![0; bytes])], reads, snapshot_reads, captures }
        };
        let half = MAX_INLINE_WRITE_BYTES / 2;
        assert!(writes_inline(&[append(half), append(half)]), "at the limit, across batches");
        assert!(!writes_inline(&[append(half), append(half + 1)]), "a byte over");
        assert!(writes_inline(&[]));
        let compact = Persist::Compact { log: 1, snapshot: 1 };
        let not_inline = [
            pipeline::Batch { persist: vec![compact], ..append(1) },
            pipeline::Batch { reads: vec![1], ..append(1) },
            pipeline::Batch { snapshot_reads: vec![SnapshotRead { index: 1, offset: 0 }], ..append(1) },
            pipeline::Batch { captures: vec![node::Capture { conn: 1, request_id: 1, to: 1 }], ..append(1) },
        ];
        for batch in not_inline {
            assert!(!writes_inline(&[append(1), batch]), "not appends alone");
        }
    }

    /// A capture in a batch sees the batch's appends, and starts after the snapshot: here a log
    /// that holds nothing before it.
    #[test]
    fn a_capture_sees_its_batchs_appends() {
        let dir = tempfile::tempdir().unwrap();
        let (mut storage, ..) = Storage::open(dir.path(), Fsync::Never, usize::MAX).unwrap();
        snapshot::save(&dir.path().join("snapshots"), 3, 1, Vec::new(), Box::new(|_| Ok(())), false).unwrap();
        storage.log.reset(4).unwrap();
        let mut records = Vec::new();
        for index in [4, 5] {
            crate::codec::Record { term: 1, index, payload: b"x" }.encode_into(&mut records);
        }
        let capture = node::Capture { conn: 9, request_id: 1, to: 5 };
        let (reads, snapshot_reads) = (Vec::new(), Vec::new());
        let batch =
            pipeline::Batch { persist: vec![Persist::Append(records)], reads, snapshot_reads, captures: vec![capture] };
        let (.., captured) = storage.write(&[batch]).unwrap();
        let (conn, frame) = &captured[0];
        let Some((wire::Decoded::Captured(m), _)) = codec::decode_frame(frame).unwrap() else { panic!() };
        let detail = String::from_utf8_lossy(m.detail()).into_owned();
        assert_eq!((*conn, m.refused(), m.snapshot_index(), m.to()), (9, 0, 3, 5), "{detail}");
        assert!(Path::new(&detail).join("log").join(format!("{:020}.log", 4)).exists());
    }

    /// An unreachable peer is dialled less and less often, but at least once a [`MAX_REDIAL`].
    #[test]
    fn redials_back_off_to_a_ceiling() {
        let waits: Vec<_> = std::iter::successors(Some(REDIAL), |&r| Some(backoff(r))).take(6).collect();
        let ms = |n| Duration::from_millis(n);
        assert_eq!(waits, [ms(100), ms(200), ms(400), ms(800), ms(1000), ms(1000)]);
    }

    /// Peers are of one cluster unless both know its epoch, and they differ: a node that knows
    /// none yet (joining, or a new cluster's before its first leader names it) talks to any.
    #[test]
    fn peers_of_another_epoch_are_of_another_cluster() {
        let hello = |epoch| Hello { service_id: 1, node_id: Some(NodeId(1)), epoch: Arc::new(AtomicU64::new(epoch)) };
        assert!(hello(7).same_cluster(7));
        assert!(!hello(7).same_cluster(8));
        assert!(hello(7).same_cluster(0));
        assert!(hello(0).same_cluster(8));
        let ours = hello(0);
        let theirs = 8;
        assert!(ours.same_cluster(theirs));
        ours.epoch.store(7, Ordering::Relaxed);
        assert!(!ours.same_cluster(theirs), "once this node learns its epoch");
    }

    /// What has arrived whole is read without waiting: the frames after the first, an unknown
    /// one skipped, up to one only half arrived, which waits; at most `MAX_FRAMES_AN_INPUT`; and
    /// a malformed one fails after those before it.
    #[tokio::test]
    async fn frames_read_whole_are_taken_together() {
        let keepalive = |session| {
            let mut frame = Vec::new();
            codec::encode_frame(&wire::KeepAlive { session }, &mut frame).unwrap();
            frame
        };
        let mut unknown = keepalive(9);
        unknown[6..8].copy_from_slice(&999_u16.to_le_bytes());
        let half = keepalive(4);
        let bytes = [keepalive(1), keepalive(2), unknown, keepalive(3), half[..half.len() - 1].to_vec()].concat();
        let reader =
            |bytes: Vec<u8>| -> tls::ReadHalf { tokio::io::BufReader::new(Box::new(std::io::Cursor::new(bytes))) };
        let mut ours = reader(bytes);
        let mut frames = vec![read_frame(&mut ours).await.unwrap()];
        read_buffered(&mut ours, &mut frames).await.unwrap();
        assert_eq!(frames, [keepalive(1), keepalive(2), keepalive(3)]);
        assert!(!whole_frame(ours.buffer()), "the half arrived waits");

        // A buffer is filled by the read of a connection's first frame.
        let mut ours = reader(vec![keepalive(1); MAX_FRAMES_AN_INPUT + 1].concat());
        let mut frames = vec![read_frame(&mut ours).await.unwrap()];
        read_buffered(&mut ours, &mut frames).await.unwrap();
        assert_eq!(frames.len(), MAX_FRAMES_AN_INPUT, "though more had arrived");
        assert!(whole_frame(ours.buffer()));

        let mut malformed = keepalive(2);
        malformed[8..10].copy_from_slice(&7_u16.to_le_bytes());
        let mut ours = reader([keepalive(1), malformed, keepalive(3)].concat());
        let mut frames = vec![read_frame(&mut ours).await.unwrap()];
        assert!(read_buffered(&mut ours, &mut frames).await.is_err());
        assert_eq!(frames, [keepalive(1)], "those before it");
    }

    /// A connection from a newer node isn't dropped for a message this one doesn't know: it's
    /// skipped, and the next read.
    #[tokio::test]
    async fn a_frame_of_an_unknown_type_is_skipped() {
        let mut unknown = Vec::new();
        codec::encode_frame(&wire::KeepAlive { session: 1 }, &mut unknown).unwrap();
        unknown[6..8].copy_from_slice(&999_u16.to_le_bytes());
        let mut known = Vec::new();
        codec::encode_frame(&wire::KeepAlive { session: 2 }, &mut known).unwrap();
        let bytes = [unknown, known.clone()].concat();
        assert_eq!(read_valid_frame(&mut &bytes[..]).await.unwrap(), known);

        let mut malformed = known.clone();
        malformed[8..10].copy_from_slice(&7_u16.to_le_bytes());
        assert!(read_valid_frame(&mut &malformed[..]).await.is_err(), "another schema's");
    }

    /// A listener at its limit waits for a connection to close before it takes another.
    #[tokio::test]
    async fn a_listener_at_its_limit_waits_for_room() {
        let mut conns = JoinSet::new();
        let (close, closed) = tokio::sync::oneshot::channel::<()>();
        conns.spawn(async {
            let _ = closed.await;
        });
        conns.spawn(std::future::pending());
        let wait = Duration::from_millis(50);
        tokio::time::timeout(wait, room(&mut conns, 3)).await.expect("room under the limit");
        assert!(tokio::time::timeout(wait, room(&mut conns, 2)).await.is_err(), "none at the limit");
        close.send(()).unwrap();
        tokio::time::timeout(Duration::from_secs(5), room(&mut conns, 2)).await.expect("room once one closes");
        assert_eq!(conns.len(), 1, "the closed one reaped");
    }

    /// A connection that says nothing for `idle` fails to read; with no `idle`, it waits on.
    #[tokio::test]
    async fn a_silent_connection_times_out() {
        let (_theirs, mut ours) = tokio::io::duplex(64);
        let idle = Duration::from_millis(50);
        let read = read_within(&mut ours, Some(&mut Idle::new(idle))).await;
        assert_eq!(read.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert!(tokio::time::timeout(idle * 2, read_within(&mut ours, None)).await.is_err(), "no idle limit");
    }

    /// Frames coming within the limit of each other keep a connection open well past it, its timer
    /// firing early and moving on; a frame written in halves either side of a firing is read
    /// whole; and once they stop, it times out, not before the limit after the last.
    #[tokio::test]
    async fn frames_keep_a_connection_open() {
        let (mut theirs, mut ours) = tokio::io::duplex(1024);
        let limit = Duration::from_millis(100);
        let mut idle = Idle::new(limit);
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Unauthorized {}, &mut frame).unwrap();
        let start = tokio::time::Instant::now();
        // In tenths of the limit, from the start: whole frames 0.6 apart, the second in halves
        // either side of the timer's first firing, at 1.0.
        let (first, second) = frame.split_at(3);
        let writes =
            [(6, &frame[..]), (8, first), (12, second), (18, &frame), (24, &frame), (30, &frame), (36, &frame)];
        let writer = {
            let writes = writes.map(|(at, bytes)| (start + limit * at / 10, bytes.to_vec()));
            tokio::spawn(async move {
                for (at, bytes) in writes {
                    tokio::time::sleep_until(at).await;
                    theirs.write_all(&bytes).await.unwrap();
                }
                theirs
            })
        };
        for _ in 0..6 {
            assert_eq!(read_within(&mut ours, Some(&mut idle)).await.unwrap(), frame);
        }
        assert!(start.elapsed() > limit * 3, "open past the limit");
        let _theirs = writer.await.unwrap();
        let last = tokio::time::Instant::now();
        let read = read_within(&mut ours, Some(&mut idle)).await;
        assert_eq!(read.unwrap_err().kind(), io::ErrorKind::TimedOut);
        assert!(last.elapsed() >= limit * 9 / 10, "not before the limit after the last frame");
    }

    const SECOND: u64 = 1_000_000_000;

    /// Slewed toward the wall clock at most 500 ppm, either way, so it closes a gap without
    /// jumping and never goes backwards.
    #[test]
    fn the_clock_slews_toward_the_wall_clock() {
        let base = 1_700_000_000 * SECOND;
        let mut clock = Clock { start: Instant::now(), mono: 0, ns: base };
        // The wall clock is a millisecond ahead: closed over two seconds, half a millisecond each.
        assert_eq!(clock.advance(SECOND, base + SECOND + 1_000_000), base + SECOND + 500_000);
        assert_eq!(clock.advance(2 * SECOND, base + 2 * SECOND + 1_000_000), base + 2 * SECOND + 1_000_000);
        // Stepped back an hour: it runs slow, never back.
        let at = clock.advance(3 * SECOND, base - 3600 * SECOND);
        assert_eq!(at, base + 3 * SECOND + 1_000_000 - 500_000);
        // A monotonic reading behind the last changes nothing.
        assert_eq!(clock.advance(SECOND, base), at);
    }

    /// With `peer_ids`, a peer dialling in is taken only as the node its certificate names: one
    /// with node 1's, saying it's node 1, is; saying it's node 3, it's dropped, whatever it sends.
    /// The dialling side checks the node it dials, so a cluster test can't see this side's check.
    #[cfg(feature = "tls")]
    #[tokio::test]
    async fn a_peer_dialling_in_is_taken_only_as_the_node_its_certificate_names() {
        use rcgen::{BasicConstraints, CertificateParams, CertifiedIssuer, ExtendedKeyUsagePurpose, IsCa, KeyPair};
        let dir = tempfile::tempdir().unwrap();
        let file = |name: &str| dir.path().join(name);
        let mut params = CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = IsCa::Ca(BasicConstraints::Unconstrained);
        let ca = CertifiedIssuer::self_signed(params, KeyPair::generate().unwrap()).unwrap();
        std::fs::write(file("ca.pem"), ca.pem()).unwrap();
        for id in [1, 2] {
            let key = KeyPair::generate().unwrap();
            let mut params = CertificateParams::new(["127.0.0.1".to_owned(), format!("node-{id}.turbofan")]).unwrap();
            params.extended_key_usages = vec![ExtendedKeyUsagePurpose::ServerAuth, ExtendedKeyUsagePurpose::ClientAuth];
            std::fs::write(file(&format!("node-{id}.pem")), params.signed_by(&key, &ca).unwrap().pem()).unwrap();
            std::fs::write(file(&format!("node-{id}.key")), key.serialize_pem()).unwrap();
        }
        let mut node_tls = NodeTls::new(file("node-2.pem"), file("node-2.key"), file("ca.pem"), file("ca.pem"));
        node_tls.peer_ids = true;
        let (current, _reloading) = tls::start(Some(node_tls)).unwrap();
        let hello = |id| Hello { service_id: 7, node_id: Some(NodeId(id)), epoch: Arc::new(AtomicU64::new(0)) };
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap().to_string();
        let (inputs, mut received) = mpsc::channel(16);
        tokio::spawn(accept_peers(listener, hello(2), None, current, true, inputs));

        let dialler = tls::ClientTls::load(&file("ca.pem"), Some((&file("node-1.pem"), &file("node-1.key")))).unwrap();
        for (claims, taken) in [(1, true), (3, false)] {
            let mut conn = tls::connect(&addr, Some(&dialler), None).await.unwrap();
            let theirs = hello(claims).frame(wire::Role::Peer, b"");
            // After the hello, a frame that decodes: what a peer taken hands the node.
            write_frames(&mut conn, [&theirs[..], &theirs[..]]).await.unwrap();
            let input = tokio::time::timeout(Duration::from_secs(2), received.recv()).await;
            match input {
                Ok(Some(Input::Peer(from, _))) => assert!(taken, "taken as node {from}"),
                Err(_) => assert!(!taken, "node {claims} wasn't taken"),
                Ok(_) => panic!("not a peer's input"),
            }
            if !taken {
                let closed = tokio::time::timeout(Duration::from_secs(2), read_frame(&mut conn)).await;
                assert!(closed.expect("dropped, not left open").is_err(), "a frame from a node that only reads");
            }
        }
    }
}
