//! Runs a [`Node`] on tokio: TCP for peers, clients and operators, and the log, meta and
//! snapshots on disk.
//!
//! One task owns the node and does everything in order. Connection tasks only frame bytes and
//! check handshakes, and hand frames to it over a channel. It carries out the node's outputs
//! by the node's rule: persist on a blocking thread and sync, tell the node, repeat while
//! there's more to persist, then send. While it waits on the disk, inputs queue up and go into
//! the next batch, which is the group commit.
//!
//! Peer connections go one way: each node dials every other to send to it, and reads what
//! arrives on the connections dialled to it. A frame that can't be sent (the peer is down or
//! slow) is dropped, and Raft sends it again.

use std::collections::HashMap;
use std::io;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc;
use tokio::task::JoinSet;

use crate::codec::{self, MAGIC, MAX_FRAME_BYTES};
use crate::log::Log;
use crate::meta::MetaFile;
use crate::node::{self, ConnId, Node, Service};
use crate::raft::{NodeId, Persist};
use crate::{snapshot, wire};

/// Inputs waiting for the node: past this, connections wait to hand theirs over.
const MAX_QUEUED_INPUTS: usize = 4096;
/// Inputs taken into one batch before persisting it.
const MAX_BATCH_INPUTS: usize = 1024;
/// Frames waiting to go to one peer or connection. Past this a peer's are dropped (Raft resends)
/// and a client too slow to read is disconnected.
const MAX_QUEUED_FRAMES: usize = 4096;
/// How long to wait before dialling a peer again.
const REDIAL: Duration = Duration::from_millis(100);
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
    /// Every member's peer address, this node's included: it listens on its own.
    pub peer_addrs: Vec<(NodeId, SocketAddr)>,
    /// Where it listens for clients.
    pub client_listen: SocketAddr,
    /// Where it listens for operators. Keep it on a private interface: it isn't authenticated.
    pub admin_listen: SocketAddr,
}

enum Input {
    Peer(NodeId, Vec<u8>),
    Client(ConnId, Vec<u8>),
    Admin(ConnId, Vec<u8>),
    Open(ConnId, mpsc::Sender<Vec<u8>>),
    Closed(ConnId),
}

/// Runs a node of `service` until an IO error stops it.
///
/// # Errors
///
/// If the data directory can't be opened (or another process has it), a listener can't bind,
/// or the disk fails. A failed write or sync leaves the node's durable state unknown, so it
/// stops; restart it, and it recovers from what's on disk.
pub async fn serve<S: Service + Send + 'static>(config: Config, service: S) -> io::Result<()> {
    let id = config.node.raft.id;
    let (storage, meta, snapshot, records) = Storage::open(&config.data_dir)?;
    let clock = Clock::new();
    let node = Node::new(config.node.clone(), service, meta, snapshot, &records, clock.now());
    drop(records);

    let own = config.peer_addrs.iter().find(|(m, _)| *m == id).map(|&(_, a)| a);
    let own = own.ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "no peer address for this node"))?;
    let (peer_listener, client_listener, admin_listener) = (
        TcpListener::bind(own).await?,
        TcpListener::bind(config.client_listen).await?,
        TcpListener::bind(config.admin_listen).await?,
    );
    let (inputs, rx) = mpsc::channel(MAX_QUEUED_INPUTS);
    // Every task is in here, so they all stop when `serve` does.
    let mut tasks = JoinSet::new();
    let members: Vec<NodeId> = config.peer_addrs.iter().map(|&(m, _)| m).collect();
    let hello = Hello { service_id: config.service_id, node_id: id };
    tasks.spawn(accept_peers(peer_listener, hello, members, inputs.clone()));
    let next_conn = Arc::new(AtomicU64::new(1));
    tasks.spawn(accept(client_listener, wire::Role::Client, hello, next_conn.clone(), inputs.clone()));
    tasks.spawn(accept(admin_listener, wire::Role::Admin, hello, next_conn, inputs));
    let mut peers = HashMap::new();
    for &(peer, addr) in config.peer_addrs.iter().filter(|(m, _)| *m != id) {
        let (tx, rx) = mpsc::channel(MAX_QUEUED_FRAMES);
        peers.insert(peer, tx);
        tasks.spawn(dial_peer(addr, hello, rx));
    }
    Core { node, storage: Some(storage), clock, peers, conns: HashMap::new() }.run(rx).await
}

/// The node, and everything it sends through.
struct Core<S> {
    node: Node<S>,
    /// Away on a blocking thread while it persists.
    storage: Option<Storage>,
    clock: Clock,
    peers: HashMap<NodeId, mpsc::Sender<Vec<u8>>>,
    conns: HashMap<ConnId, mpsc::Sender<Vec<u8>>>,
}

impl<S: Service + Send + 'static> Core<S> {
    async fn run(mut self, mut rx: mpsc::Receiver<Input>) -> io::Result<()> {
        loop {
            self.flush().await?;
            let wake = self.clock.instant(self.node.next_deadline());
            tokio::select! {
                input = rx.recv() => {
                    let Some(input) = input else { return Ok(()) };
                    self.handle(input);
                    for _ in 1..MAX_BATCH_INPUTS {
                        let Ok(input) = rx.try_recv() else { break };
                        self.handle(input);
                    }
                }
                () = tokio::time::sleep_until(wake.into()) => self.node.on_tick(self.clock.now()),
            }
        }
    }

    fn handle(&mut self, input: Input) {
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
        }
    }

    /// Persists, then sends, what the node has for the disk and the network. A leader's appends go
    /// first, so followers write them while it does.
    async fn flush(&mut self) -> io::Result<()> {
        let (mut peer_sends, mut client_sends) = (Vec::new(), Vec::new());
        loop {
            let out = self.node.take_outputs();
            self.send_peers(out.peer_appends);
            peer_sends.extend(out.peer_sends);
            client_sends.extend(out.client_sends);
            if out.persist.is_empty() {
                break;
            }
            let mut storage = self.storage.take().expect("storage is back between batches");
            let ops = out.persist;
            let (storage, last) = tokio::task::spawn_blocking(move || {
                let last = storage.apply(&ops);
                (storage, last)
            })
            .await
            .map_err(io::Error::other)?;
            self.storage = Some(storage);
            self.node.on_persisted(last?);
        }
        self.send_peers(peer_sends);
        for (conn, frame) in client_sends {
            let Some(tx) = self.conns.get(&conn) else { continue };
            if tx.try_send(frame).is_err() {
                // Too slow to read, or gone: its task ends when its sender is dropped.
                self.conns.remove(&conn);
                self.node.on_disconnect(conn);
            }
        }
        Ok(())
    }

    fn send_peers(&self, frames: Vec<(NodeId, Vec<u8>)>) {
        for (to, frame) in frames {
            if let Some(tx) = self.peers.get(&to) {
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
    #[allow(clippy::type_complexity, reason = "the four parts of a node's durable state")]
    fn open(dir: &Path) -> io::Result<(Self, crate::meta::Meta, Option<crate::raft::Snapshot>, Vec<u8>)> {
        let snapshots = dir.join("snapshots");
        std::fs::create_dir_all(&snapshots)?;
        let (log, records) = Log::open(&dir.join("log"))?;
        let (meta_file, meta) = MetaFile::open(&dir.join("meta"))?;
        let snapshot = snapshot::load(&snapshots)?;
        Ok((Self { log, meta: meta_file, snapshots }, meta, snapshot, records))
    }

    /// Carries out `ops` in order and syncs, returning the last index in the log.
    fn apply(&mut self, ops: &[Persist]) -> io::Result<u64> {
        for op in ops {
            match op {
                Persist::Meta(meta) => self.meta.save(*meta)?,
                Persist::Append(records) => self.log.append(records)?,
                Persist::TruncateFrom(index) => self.log.truncate_from(*index)?,
                Persist::Snapshot { snapshot, replace_log } => {
                    snapshot::save(&self.snapshots, snapshot)?;
                    if *replace_log {
                        self.log.reset(snapshot.index + 1)?;
                    } else {
                        self.log.compact_to(snapshot.index)?;
                    }
                }
            }
        }
        self.log.sync()?;
        Ok(self.log.next_index() - 1)
    }
}

/// Nanoseconds since the Unix epoch, read once at startup and advanced by the monotonic clock,
/// so they never jump. They're what the leader stamps into the log: cluster time.
// ponytail: drifts from the wall clock over a long uptime; resync if services need wall time.
#[derive(Debug, Clone, Copy)]
struct Clock {
    start: Instant,
    base_ns: u64,
}

impl Clock {
    fn new() -> Self {
        let since_epoch = SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default();
        Self { start: Instant::now(), base_ns: u64::try_from(since_epoch.as_nanos()).unwrap_or(u64::MAX / 2) }
    }

    fn now(self) -> u64 {
        self.base_ns + u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX / 2)
    }

    fn instant(self, ns: u64) -> Instant {
        self.start + Duration::from_nanos(ns.saturating_sub(self.base_ns))
    }
}

/// What a node says, and checks, in the handshake.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Hello {
    pub(crate) service_id: u32,
    pub(crate) node_id: NodeId,
}

impl Hello {
    pub(crate) fn frame(self, role: wire::Role) -> Vec<u8> {
        let mut frame = Vec::new();
        let hello = wire::Hello { magic: MAGIC, service_id: self.service_id, role, node_id: self.node_id };
        codec::encode_frame(&hello, &mut frame).expect("a hello is small");
        frame
    }

    /// The node ID in `frame` if it's a hello for this service from `role`.
    pub(crate) fn check(self, frame: &[u8], role: wire::Role) -> Option<NodeId> {
        let Ok(Some((wire::Decoded::Hello(h), _))) = codec::decode_frame(frame) else { return None };
        (h.magic() == MAGIC && h.service_id() == self.service_id && h.role() == role).then_some(h.node_id())
    }
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

/// Reads a frame that must decode: a malformed one ends the connection.
async fn read_valid_frame(reader: &mut (impl AsyncRead + Unpin)) -> io::Result<Vec<u8>> {
    let frame = read_frame(reader).await?;
    match codec::decode_frame(&frame) {
        Ok(Some(_)) => Ok(frame),
        _ => Err(io::Error::new(io::ErrorKind::InvalidData, "malformed frame")),
    }
}

async fn accept_peers(listener: TcpListener, hello: Hello, members: Vec<NodeId>, inputs: mpsc::Sender<Input>) {
    let mut conns = JoinSet::new();
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let (members, inputs) = (members.clone(), inputs.clone());
        conns.spawn(async move {
            let mut stream = stream;
            let Ok(Ok(first)) = tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut stream)).await else { return };
            let Some(from) =
                hello.check(&first, wire::Role::Peer).filter(|p| members.contains(p) && *p != hello.node_id)
            else {
                return;
            };
            while let Ok(frame) = read_valid_frame(&mut stream).await {
                if inputs.send(Input::Peer(from, frame)).await.is_err() {
                    return;
                }
            }
        });
        // Reap finished connections.
        while conns.try_join_next().is_some() {}
    }
}

async fn accept(
    listener: TcpListener,
    role: wire::Role,
    hello: Hello,
    next_conn: Arc<AtomicU64>,
    inputs: mpsc::Sender<Input>,
) {
    let mut conns = JoinSet::new();
    loop {
        let Ok((stream, _)) = listener.accept().await else { continue };
        let conn = next_conn.fetch_add(1, Ordering::Relaxed);
        conns.spawn(serve_connection(stream, conn, role, hello, inputs.clone()));
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
    inputs: mpsc::Sender<Input>,
) {
    let _ = stream.set_nodelay(true);
    let (mut reader, mut writer) = stream.into_split();
    let Ok(Ok(first)) = tokio::time::timeout(HELLO_TIMEOUT, read_frame(&mut reader)).await else { return };
    if hello.check(&first, role).is_none() || writer.write_all(&hello.frame(role)).await.is_err() {
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
async fn dial_peer(addr: SocketAddr, hello: Hello, mut rx: mpsc::Receiver<Vec<u8>>) {
    let mut batch = Vec::new();
    loop {
        let Ok(mut stream) = TcpStream::connect(addr).await else {
            tokio::time::sleep(REDIAL).await;
            continue;
        };
        let _ = stream.set_nodelay(true);
        if stream.write_all(&hello.frame(wire::Role::Peer)).await.is_err() {
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
            if stream.write_all(&batch).await.is_err() {
                break;
            }
        }
        tokio::time::sleep(REDIAL).await;
    }
}
