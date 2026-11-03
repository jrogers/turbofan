//! Runs a [`Node`] on tokio: TCP for peers, clients and operators, TLS over it if configured
//! ([`crate::tls`]), and the log, meta and snapshots on disk.
//!
//! One task owns the node and does everything in order. Connection tasks only frame bytes and
//! check handshakes, and hand frames to it over a channel. It carries out the node's outputs
//! by the node's rule, without waiting on the disk: a blocking thread writes a batch, reads back
//! the log records the node asks for, and syncs, while the node goes on with its inputs, and
//! then the task tells the node and sends what waited for it. The batches taken meanwhile are
//! written together, with one sync, which is the group commit.
//!
//! Peer connections go one way: each node dials every other to send to it, and reads what
//! arrives on the connections dialled to it. It dials the members, as they change, and the
//! peers in its configuration: for a node joining a cluster, the way to the leader before it
//! knows the members. A frame that can't be sent (the peer is down or
//! slow) is dropped, and Raft sends it again.

use std::collections::{HashMap, HashSet};
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::{AbortHandle, JoinHandle, JoinSet};

use crate::codec::{self, MAGIC, MAX_FRAME_BYTES};
use crate::log::Log;
use crate::meta::MetaFile;
use crate::metrics;
use crate::node::{self, ConnId, Node, Service};
use crate::pipeline::{self, Pipeline};
use crate::raft::{Entries, MAX_READ_BYTES, Member, NodeId, Persist, SnapshotRead};
use crate::tls::{self, Acceptor as TlsAcceptor, ClientTls, NodeTls};
use crate::{snapshot, wire};

/// Inputs waiting for the node, from clients and from the rest: past this, connections wait to
/// hand theirs over.
const MAX_QUEUED_INPUTS: usize = 4096;
/// Inputs taken into one batch before persisting it.
const MAX_BATCH_INPUTS: usize = 1024;
/// Frames waiting to go to one peer or connection. Past this a peer's are dropped (Raft resends)
/// and a client too slow to read is disconnected.
const MAX_QUEUED_FRAMES: usize = 4096;
/// How long to wait before dialling a peer again.
const REDIAL: Duration = Duration::from_millis(100);
/// How long a node that has stopped waits, for what it sent last to go out.
const STOP_GRACE: Duration = Duration::from_millis(200);
/// How often the node's gauges are published.
const METRICS_EVERY: Duration = Duration::from_secs(1);
/// How long a new connection has to say hello.
const HELLO_TIMEOUT: Duration = Duration::from_secs(5);

/// Where a node keeps its state, and where it listens.
#[derive(Debug, Clone)]
pub struct Config {
    /// The node's own configuration.
    pub node: node::Config,
    /// Which service this cluster runs: a connection for another is refused.
    pub service_id: u32,
    /// The log, meta and snapshots.
    pub data_dir: PathBuf,
    /// Peers' addresses, this node's included: it listens on its own, and dials the others as
    /// well as the members.
    pub peer_addrs: Vec<(NodeId, SocketAddr)>,
    /// Where it listens for clients.
    pub client_listen: SocketAddr,
    /// Where it listens for operators. Without TLS, keep it on a private interface.
    pub admin_listen: SocketAddr,
    /// The token an operator's hello must carry, at most [`MAX_TOKEN_BYTES`]: a connection without
    /// it is refused. With none, any is taken.
    pub admin_token: Option<Vec<u8>>,
    /// The token every node's hello to a peer carries, and must, at most [`MAX_TOKEN_BYTES`]: the
    /// same on every node. With none, a node sends none and takes any peer.
    pub peer_token: Option<Vec<u8>>,
    /// TLS on all three listeners and to peers. With none, everything is plain TCP, and every
    /// node of the cluster must agree.
    pub tls: Option<NodeTls>,
}

/// The longest token a hello carries.
pub const MAX_TOKEN_BYTES: usize = 255;

enum Input {
    Peer(NodeId, Vec<u8>),
    Client(ConnId, Vec<u8>),
    Admin(ConnId, Vec<u8>),
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
    for (name, token) in [("admin_token", &config.admin_token), ("peer_token", &config.peer_token)] {
        if token.as_ref().is_some_and(|t| t.len() > MAX_TOKEN_BYTES) {
            let message = format!("{name}: at most {MAX_TOKEN_BYTES} bytes");
            return Err(io::Error::new(io::ErrorKind::InvalidInput, message));
        }
    }
    let tls = config.tls.as_ref().map(tls::Loaded::load).transpose()?;
    let id = config.node.raft.id;
    let (storage, meta, snapshot, log) = Storage::open(&config.data_dir, config.node.raft.log_window_bytes)?;
    let mut clock = Clock::new();
    let mut node =
        Node::new(config.node.clone(), service, meta, snapshot.as_ref().map(|s| s.0.clone()), log, clock.now());
    if let Some((_, mut bytes)) = snapshot {
        node.restore(&mut bytes)?;
    }
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
    let acceptor = |pick: fn(&tls::Loaded) -> &TlsAcceptor| tls.as_ref().map(|t| pick(t).clone());
    tasks.spawn(accept_peers(peer_listener, hello.clone(), peer_token.clone(), acceptor(|t| &t.peer), inputs.clone()));
    let next_conn = Arc::new(AtomicU64::new(1));
    let (role, tls_client) = (wire::Role::Client, acceptor(|t| &t.client));
    tasks.spawn(accept(client_listener, role, hello.clone(), None, tls_client, next_conn.clone(), client_inputs));
    let token = config.admin_token.as_deref().map(Arc::from);
    let (role, tls_admin) = (wire::Role::Admin, acceptor(|t| &t.admin));
    tasks.spawn(accept(admin_listener, role, hello.clone(), token, tls_admin, next_conn, inputs.clone()));
    let contacts = config.peer_addrs.iter().map(|&(m, a)| (m, a.to_string())).collect();
    let mut core = Core {
        node,
        storage: Some(storage),
        pipeline: Pipeline::default(),
        writing: None,
        snapshots,
        inputs,
        clock,
        published: Instant::now(),
        hello,
        peer_token: peer_token.unwrap_or_else(|| Arc::from(&[][..])),
        tls: tls.map(|t| t.dial),
        contacts,
        peers: HashMap::new(),
        dials: JoinSet::new(),
        conns: HashMap::new(),
    };
    let peers = core.node.raft().peers();
    core.dial(&peers);
    core.run(rx, client_rx).await
}

/// The node, and everything it sends through.
struct Core<S> {
    node: Node<S>,
    /// Away on a blocking thread while it persists.
    storage: Option<Storage>,
    /// The node's batches, between taken and durable.
    pipeline: Pipeline,
    /// The write under way, and the storage back with it.
    writing: Option<JoinHandle<Written>>,
    /// Where snapshots are saved, apart from the batches, reporting back on `inputs`.
    snapshots: PathBuf,
    inputs: mpsc::Sender<Input>,
    clock: Clock,
    /// When the gauges were last published.
    published: Instant,
    hello: Hello,
    /// What its hellos to peers carry: [`Config::peer_token`], or nothing.
    peer_token: Arc<[u8]>,
    /// What it dials peers with, if it has TLS.
    tls: Option<ClientTls>,
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
                    self.handle(input)?;
                    for _ in 1..MAX_BATCH_INPUTS {
                        let Ok(input) = rx.try_recv() else { break };
                        self.handle(input)?;
                    }
                }
                // A backlogged leader leaves clients waiting, so TCP holds them back, until what
                // it has proposed commits.
                input = client_rx.recv(), if take_inputs && !self.node.raft().backlogged() => {
                    let Some(input) = input else { return Ok(()) };
                    self.handle(input)?;
                    for _ in 1..MAX_BATCH_INPUTS {
                        if self.node.raft().backlogged() {
                            break;
                        }
                        let Ok(input) = client_rx.try_recv() else { break };
                        self.handle(input)?;
                    }
                }
                () = tokio::time::sleep_until(wake.into()), if take_inputs => self.node.on_tick(self.clock.now()),
            }
        }
    }

    fn handle(&mut self, input: Input) -> io::Result<()> {
        let now = self.clock.now();
        match input {
            Input::Peer(from, frame) => self.node.on_peer(from, &decode(&frame), now),
            Input::Client(conn, frame) => self.node.on_client(conn, &decode(&frame), now),
            Input::Admin(conn, frame) => self.node.on_admin(conn, &decode(&frame), now),
            Input::Open(conn, tx) => {
                self.conns.insert(conn, tx);
            }
            Input::Closed(conn) => {
                self.conns.remove(&conn);
                self.node.on_disconnect(conn);
            }
            Input::SnapshotSaved(saved) => self.node.on_snapshot_saved(saved?),
        }
        Ok(())
    }

    /// Takes the node's outputs, and carries out what goes at once (see [`crate::pipeline`]),
    /// starting a write if there's one to start. A leader's appends go first, so followers write
    /// them while it does.
    fn flush(&mut self) {
        let taken = self.pipeline.take(&mut self.node);
        self.hello.epoch.store(self.node.epoch(), Ordering::Relaxed);
        for &ns in &taken.commit_latencies {
            metrics::record_commit_latency(ns);
        }
        self.send_peers(taken.peer_appends);
        self.send_clients(taken.client_sends);
        self.send_peers(taken.peer_sends);
        if let Some(peers) = &taken.peers {
            self.dial(peers);
        }
        if let Some(save) = taken.save {
            let (dir, inputs) = (self.snapshots.clone(), self.inputs.clone());
            tokio::task::spawn_blocking(move || {
                let saved = snapshot::save(&dir, save.index, save.term, save.members, save.writer, true);
                // Gone only if the node stopped.
                let _ = inputs.blocking_send(Input::SnapshotSaved(saved));
            });
        }
        if let Some(batches) = taken.write {
            let mut storage = self.storage.take().expect("storage is back between writes");
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
        let (reads, chunks) = done?;
        let mut snapshot = None;
        if let Some(index) = self.pipeline.restore() {
            // ponytail: reads the snapshot on the node's task, which waits for it anyway; a
            // blocking thread if a big restore starves the connection tasks.
            let loaded = snapshot::load(&self.snapshots)?.filter(|(s, _)| s.index == index);
            let (_, bytes) = loaded.ok_or_else(|| io::Error::other("the installed snapshot is gone"))?;
            snapshot = Some(bytes);
        }
        let from = snapshot.as_mut().map(|b| -> &mut dyn io::Read { b });
        let sends = self.pipeline.written(&mut self.node, &reads, &chunks, from)?;
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
    log: Log,
    meta: MetaFile,
    snapshots: PathBuf,
}

impl Storage {
    /// Opens a node's durable state, keeping `window_bytes` of the log's records in memory.
    #[allow(clippy::type_complexity, reason = "the four parts of a node's durable state")]
    fn open(
        dir: &Path,
        window_bytes: usize,
    ) -> io::Result<(Self, crate::meta::Meta, Option<(crate::raft::Snapshot, impl io::Read + use<>)>, Entries)> {
        let snapshots = dir.join("snapshots");
        std::fs::create_dir_all(&snapshots)?;
        let snapshot = snapshot::load(&snapshots)?;
        let mut entries = Entries::default();
        let mut log = Log::open(&dir.join("log"), |record| entries.recover(record, window_bytes))?;
        log.resume(snapshot.as_ref().map(|(s, _)| s.index), &mut entries)?;
        let (meta_file, meta) = MetaFile::open(&dir.join("meta"))?;
        Ok((Self { log, meta: meta_file, snapshots }, meta, snapshot, entries))
    }

    /// Writes `batches` in order, each one's reads after its operations, then syncs. Returns the
    /// records and snapshot chunks read, a read that an earlier batch asked for once.
    #[allow(clippy::type_complexity, reason = "the two kinds of read")]
    fn write(&mut self, batches: &[pipeline::Batch]) -> io::Result<(Vec<Vec<u8>>, Vec<(u64, u64, Vec<u8>)>)> {
        let (mut reads, mut chunks) = (Vec::new(), Vec::new());
        let (mut froms, mut offsets) = (HashSet::new(), HashSet::new());
        for batch in batches {
            for op in &batch.persist {
                self.apply(op)?;
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
        }
        self.log.sync()?;
        Ok((reads, chunks))
    }

    fn apply(&mut self, op: &Persist) -> io::Result<()> {
        match op {
            Persist::Meta(meta) => self.meta.save(*meta)?,
            Persist::Append(records) => self.log.append(records)?,
            Persist::TruncateFrom(index) => self.log.truncate_from(*index)?,
            Persist::Trust => self.log.trust()?,
            Persist::Compact(index) => {
                self.log.compact_to(*index)?;
                snapshot::remove_older(&self.snapshots, *index)?;
            }
            Persist::SnapshotChunk { offset, bytes, .. } => snapshot::write_chunk(&self.snapshots, *offset, bytes)?,
            Persist::InstallSnapshot { snapshot, replace_log } => {
                snapshot::install(&self.snapshots, snapshot, true)?;
                snapshot::remove_older(&self.snapshots, snapshot.index)?;
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
    #[allow(clippy::type_complexity, reason = "the two kinds of read")]
    done: io::Result<(Vec<Vec<u8>>, Vec<(u64, u64, Vec<u8>)>)>,
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

/// Reads from peers: any node of this service whose hello has `token`, if there is one, since a
/// member this node hasn't heard was added may lead.
async fn accept_peers(
    listener: TcpListener,
    hello: Hello,
    token: Option<Arc<[u8]>>,
    tls: Option<TlsAcceptor>,
    inputs: mpsc::Sender<Input>,
) {
    let mut conns = JoinSet::new();
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let (inputs, token, tls, hello) = (inputs.clone(), token.clone(), tls.clone(), hello.clone());
        conns.spawn(async move {
            let handshake = async {
                let mut stream = tls::accept(stream, tls.as_ref()).await?;
                let first = read_frame(&mut stream).await?;
                io::Result::Ok((stream, first))
            };
            let Ok(Ok((mut stream, first))) = tokio::time::timeout(HELLO_TIMEOUT, handshake).await else { return };
            let theirs = hello.check(&first, wire::Role::Peer);
            let theirs = theirs.filter(|h| token.as_ref().is_none_or(|token| same(h.token(), token)));
            let theirs = theirs.filter(|h| hello.same_cluster(h.epoch()));
            let Some((from, epoch)) = theirs.and_then(|h| Some((NodeId::from_wire(h.node_id())?, h.epoch()))) else {
                return;
            };
            if Some(from) == hello.node_id {
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
        // Reap finished connections.
        while conns.try_join_next().is_some() {}
    }
}

/// Takes clients or operators: those whose hello has `token`, if there is one.
async fn accept(
    listener: TcpListener,
    role: wire::Role,
    hello: Hello,
    token: Option<Arc<[u8]>>,
    tls: Option<TlsAcceptor>,
    next_conn: Arc<AtomicU64>,
    inputs: mpsc::Sender<Input>,
) {
    let mut conns = JoinSet::new();
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let conn = next_conn.fetch_add(1, Ordering::Relaxed);
        conns.spawn(serve_connection(stream, conn, role, hello.clone(), token.clone(), tls.clone(), inputs.clone()));
        while conns.try_join_next().is_some() {}
    }
}

/// A client's or operator's connection: hello each way, then frames in to the node and out from
/// it, until either side closes.
async fn serve_connection(
    stream: TcpStream,
    conn: ConnId,
    role: wire::Role,
    hello: Hello,
    token: Option<Arc<[u8]>>,
    tls: Option<TlsAcceptor>,
    inputs: mpsc::Sender<Input>,
) {
    let handshake = async {
        let mut stream = tls::accept(stream, tls.as_ref()).await?;
        let first = read_frame(&mut stream).await?;
        io::Result::Ok((stream, first))
    };
    let Ok(Ok((stream, first))) = tokio::time::timeout(HELLO_TIMEOUT, handshake).await else { return };
    let (mut reader, mut writer) = tokio::io::split(stream);
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
    loop {
        tokio::select! {
            frame = read_valid_frame(&mut reader) => {
                let Ok(frame) = frame else { break };
                let input = if role == wire::Role::Admin { Input::Admin(conn, frame) } else { Input::Client(conn, frame) };
                if inputs.send(input).await.is_err() {
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

/// Keeps a connection to a peer and writes what the node sends it, redialling when it drops.
async fn dial_peer(
    addr: String,
    hello: Hello,
    token: Arc<[u8]>,
    tls: Option<ClientTls>,
    mut rx: mpsc::Receiver<Vec<u8>>,
) {
    let mut batch = Vec::new();
    loop {
        let connected = tokio::time::timeout(HELLO_TIMEOUT, tls::connect(&addr, tls.as_ref())).await;
        let Ok(Ok(mut stream)) = connected else {
            tokio::time::sleep(REDIAL).await;
            continue;
        };
        if write_frames(&mut stream, [&hello.frame(wire::Role::Peer, &token)[..]]).await.is_err() {
            tokio::time::sleep(REDIAL).await;
            continue;
        }
        loop {
            let Some(frame) = rx.recv().await else { return };
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
}
