//! Talking to a cluster over TCP: [`ClusterClient`] for gateways, [`request_snapshot`] for
//! operators.

use std::io;
use std::time::{Duration, Instant};

use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::client::{Client, Event, Reconnect, SendError};
use crate::codec;
use crate::driver::{Hello, read_frame};
use crate::wire;

/// How long to wait for a connection to a member before trying the next.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(1);
/// How long to wait before trying again when no member answers, or there's no leader yet.
const RETRY: Duration = Duration::from_millis(50);
/// Responses waiting for [`ClusterClient::recv`]; past this the client stops reading the
/// connection until they're taken.
const MAX_QUEUED_EVENTS: usize = 4096;

enum Command {
    Send(Vec<u8>, oneshot::Sender<Result<u64, SendError>>),
    Close,
}

/// A session with a cluster, kept open across failovers: messages are applied exactly once, in
/// order, and responses arrive exactly once, in order. It runs on a task of its own, which
/// stops when this is dropped.
#[derive(Debug)]
pub struct ClusterClient {
    commands: mpsc::Sender<Command>,
    events: mpsc::Receiver<Event>,
    task: JoinHandle<()>,
}

impl ClusterClient {
    /// A session with the cluster of `service_id` whose members take clients at `addrs`.
    ///
    /// # Panics
    ///
    /// If `addrs` is empty, or this isn't called on a tokio runtime.
    #[must_use]
    pub fn connect(addrs: Vec<String>, service_id: u32, keepalive: Duration) -> Self {
        let client = Client::new(addrs, u64::try_from(keepalive.as_nanos()).unwrap_or(u64::MAX));
        let (commands, rx) = mpsc::channel(64);
        let (tx, events) = mpsc::channel(MAX_QUEUED_EVENTS);
        let task = tokio::spawn(run(client, service_id, rx, tx));
        Self { commands, events, task }
    }

    /// Sends `payload`, returning its `client_seq`. It's resent after a failover until the
    /// cluster has applied it.
    ///
    /// # Errors
    ///
    /// If [`crate::client::MAX_UNACKED`] messages wait to be applied (read responses, then try
    /// again), the session closed, or the payload is too long.
    pub async fn send(&self, payload: Vec<u8>) -> Result<u64, SendError> {
        let (reply, rx) = oneshot::channel();
        self.commands.send(Command::Send(payload, reply)).await.map_err(|_| SendError::Closed)?;
        rx.await.map_err(|_| SendError::Closed)?
    }

    /// The next thing the cluster said, or `None` once the session is over.
    pub async fn recv(&mut self) -> Option<Event> {
        self.events.recv().await
    }

    /// Closes the session.
    pub async fn close(&self) {
        let _ = self.commands.send(Command::Close).await;
    }
}

impl Drop for ClusterClient {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn run(mut client: Client, service_id: u32, mut commands: mpsc::Receiver<Command>, events: mpsc::Sender<Event>) {
    let start = Instant::now();
    let now = || u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let hello = Hello { service_id, node_id: 0 };
    loop {
        let Some(stream) = handshake(client.target(), hello, wire::Role::Client).await else {
            client.on_disconnected();
            tokio::time::sleep(RETRY).await;
            continue;
        };
        let (mut reader, mut writer) = stream.into_split();
        let (frames_tx, mut frames) = mpsc::channel(256);
        let read = tokio::spawn(async move {
            while let Ok(frame) = read_frame(&mut reader).await {
                if frames_tx.send(frame).await.is_err() {
                    return;
                }
            }
        });
        client.on_connected(now());
        let mut up = write(&mut client, &mut writer).await;
        while up {
            let wake = start + Duration::from_nanos(client.next_deadline());
            tokio::select! {
                command = commands.recv() => match command {
                    None => return,
                    Some(Command::Send(payload, reply)) => { let _ = reply.send(client.send(&payload)); }
                    Some(Command::Close) => client.close(),
                },
                frame = frames.recv() => {
                    let message = frame.as_deref().and_then(|f| codec::decode_frame(f).ok().flatten());
                    let Some((message, _)) = message else {
                        client.on_disconnected();
                        break;
                    };
                    client.on_frame(&message);
                }
                () = tokio::time::sleep_until(wake.into()) => client.on_tick(now()),
            }
            while let Some(event) = client.poll_event() {
                if events.send(event).await.is_err() {
                    read.abort();
                    return;
                }
            }
            up = write(&mut client, &mut writer).await;
            // Redirected: the target already points where to go.
            match client.take_reconnect() {
                Some(Reconnect::ToLeader) => break,
                Some(Reconnect::Later) => {
                    tokio::time::sleep(RETRY).await;
                    break;
                }
                None => {}
            }
        }
        read.abort();
    }
}

/// Writes what the client has to send. `false`, and the client told, if the connection failed.
async fn write(client: &mut Client, writer: &mut OwnedWriteHalf) -> bool {
    for frame in client.take_frames() {
        if writer.write_all(&frame).await.is_err() {
            client.on_disconnected();
            return false;
        }
    }
    true
}

/// A connection to `addr` that has said hello for `role`.
async fn handshake(addr: &str, hello: Hello, role: wire::Role) -> Option<TcpStream> {
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await.ok()?.ok()?;
    let _ = stream.set_nodelay(true);
    stream.write_all(&hello.frame(role)).await.ok()?;
    let reply = tokio::time::timeout(CONNECT_TIMEOUT, read_frame(&mut stream)).await.ok()?.ok()?;
    hello.check(&reply, role)?;
    Some(stream)
}

/// Asks the cluster to snapshot every node at the next index, through the admin listener at
/// `addr` (redirected to the leader's). Returns the index, once a quorum has it durable.
///
/// # Errors
///
/// If no leader answers within `timeout`.
pub async fn request_snapshot(addr: &str, service_id: u32, timeout: Duration) -> io::Result<u64> {
    let request_id = u64::try_from(
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos(),
    )
    .unwrap_or_default();
    let hello = Hello { service_id, node_id: 0 };
    let mut target = addr.to_owned();
    let attempt = async {
        loop {
            match ask(&target, hello, request_id).await {
                Some(Ok(index)) => return index,
                Some(Err(leader)) if !leader.is_empty() => target = leader,
                _ => tokio::time::sleep(RETRY * 4).await,
            }
        }
    };
    tokio::time::timeout(timeout, attempt)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no leader answered"))
}

/// One try: `Ok(index)`, or `Err(address)` to try instead (empty for none known yet).
async fn ask(addr: &str, hello: Hello, request_id: u64) -> Option<Result<u64, String>> {
    let mut stream = handshake(addr, hello, wire::Role::Admin).await?;
    let mut frame = Vec::new();
    codec::encode_frame(&wire::SnapshotRequest { request_id }, &mut frame).expect("small");
    stream.write_all(&frame).await.ok()?;
    let reply = read_frame(&mut stream).await.ok()?;
    match codec::decode_frame(&reply).ok()??.0 {
        wire::Decoded::SnapshotTaken(m) if m.request_id() == request_id => Some(Ok(m.index())),
        wire::Decoded::Redirect(m) => Some(Err(String::from_utf8_lossy(m.addr()).into_owned())),
        _ => None,
    }
}
