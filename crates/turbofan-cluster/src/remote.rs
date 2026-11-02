//! Talking to a cluster over TCP: [`ClusterClient`] for gateways; [`request_snapshot`],
//! [`add_member`], [`remove_member`], [`transfer_leadership`], [`activate`] and [`shutdown`] for
//! operators. Each goes over TLS when given a [`ClientTls`].

use std::io;
use std::time::{Duration, Instant};

use tokio::io::WriteHalf;
use tokio::sync::{mpsc, oneshot};
use tokio::task::JoinHandle;

use crate::client::{Client, Event, Reconnect, SendError};
use crate::codec;
use crate::driver::{Hello, MAX_TOKEN_BYTES, read_frame, write_frames};
use crate::raft::{Member, NodeId};
use crate::tls::{self, ClientTls, Conn};
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
    /// A session with the cluster of `service_id` whose members take clients at `addrs`, with
    /// `credentials` for the service to check.
    ///
    /// # Panics
    ///
    /// If `addrs` is empty, the credentials are too long, or this isn't called on a tokio runtime.
    #[must_use]
    pub fn connect(addrs: Vec<String>, service_id: u32, credentials: Vec<u8>, keepalive: Duration) -> Self {
        Self::start(addrs, service_id, credentials, keepalive, None)
    }

    /// As [`ClusterClient::connect`], over TLS: for a cluster with
    /// [`driver::Config::tls`](crate::driver::Config::tls).
    ///
    /// # Panics
    ///
    /// As [`ClusterClient::connect`].
    #[must_use]
    pub fn connect_tls(
        addrs: Vec<String>,
        service_id: u32,
        credentials: Vec<u8>,
        keepalive: Duration,
        tls: ClientTls,
    ) -> Self {
        Self::start(addrs, service_id, credentials, keepalive, Some(tls))
    }

    fn start(
        addrs: Vec<String>,
        service_id: u32,
        credentials: Vec<u8>,
        keepalive: Duration,
        tls: Option<ClientTls>,
    ) -> Self {
        let client = Client::new(addrs, credentials, keepalive);
        let (commands, rx) = mpsc::channel(64);
        let (tx, events) = mpsc::channel(MAX_QUEUED_EVENTS);
        let task = tokio::spawn(run(client, service_id, tls, rx, tx));
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

async fn run(
    mut client: Client,
    service_id: u32,
    tls: Option<ClientTls>,
    mut commands: mpsc::Receiver<Command>,
    events: mpsc::Sender<Event>,
) {
    let start = Instant::now();
    let now = || u64::try_from(start.elapsed().as_nanos()).unwrap_or(u64::MAX);
    let hello = Hello { service_id, node_id: None };
    loop {
        let Ok(stream) = handshake(client.target(), hello, wire::Role::Client, &[], tls.as_ref()).await else {
            client.on_disconnected();
            tokio::time::sleep(RETRY).await;
            continue;
        };
        let (mut reader, mut writer) = tokio::io::split(stream);
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
                    let decoded = frame.as_deref().map(codec::decode_frame);
                    match decoded {
                        Some(Ok(Some((message, _)))) => client.on_frame(&message),
                        // From a newer cluster: nothing this client knows to act on.
                        Some(Err(e)) if e.is_unknown_message() => {}
                        _ => {
                            client.on_disconnected();
                            break;
                        }
                    }
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
async fn write(client: &mut Client, writer: &mut WriteHalf<Conn>) -> bool {
    let frames = client.take_frames();
    if !frames.is_empty() && write_frames(writer, frames.iter().map(Vec::as_slice)).await.is_err() {
        client.on_disconnected();
        return false;
    }
    true
}

/// A connection to `addr` that has said hello for `role`, with an operator's `token` (empty for
/// none), over TLS if there's `tls`. `PermissionDenied` if the node refused the token.
async fn handshake(
    addr: &str,
    hello: Hello,
    role: wire::Role,
    token: &[u8],
    tls: Option<&ClientTls>,
) -> io::Result<Conn> {
    let timed_out = |_| io::Error::from(io::ErrorKind::TimedOut);
    let mut stream = tokio::time::timeout(CONNECT_TIMEOUT, tls::connect(addr, tls)).await.map_err(timed_out)??;
    write_frames(&mut stream, [&hello.frame(role, token)[..]]).await?;
    let reply = tokio::time::timeout(CONNECT_TIMEOUT, read_frame(&mut stream)).await.map_err(timed_out)??;
    if let Ok(Some((wire::Decoded::Unauthorized(_), _))) = codec::decode_frame(&reply) {
        return Err(io::Error::new(io::ErrorKind::PermissionDenied, "the admin token was refused"));
    }
    hello.check(&reply, role).ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "no hello for this service"))?;
    Ok(stream)
}

/// Asks the cluster to snapshot every node at the next index, through the admin listener at
/// `addr` (redirected to the leader's). Returns the index, once a quorum has it durable.
///
/// # Errors
///
/// If the node refuses `token`, the cluster's admin token (empty if it has none), or it's over
/// [`MAX_TOKEN_BYTES`](crate::driver::MAX_TOKEN_BYTES). If no leader
/// answers within `timeout`.
pub async fn request_snapshot(
    addr: &str,
    service_id: u32,
    token: &[u8],
    tls: Option<&ClientTls>,
    timeout: Duration,
) -> io::Result<u64> {
    let request = |request_id| frame(&wire::SnapshotRequest { request_id });
    admin(addr, service_id, token, tls, timeout, request, |reply, request_id| match reply {
        wire::Decoded::SnapshotTaken(m) if m.request_id() == request_id => Some(Ok(m.index())),
        _ => None,
    })
    .await
}

/// Adds `member` to the cluster, through the admin listener at `addr`: it catches up from the
/// leader, then joins. Start it first, with `--join`. Returns the index of the change, once
/// it's committed.
///
/// # Errors
///
/// If the node refuses `token`, the cluster's admin token (empty if it has none), or it's over
/// [`MAX_TOKEN_BYTES`](crate::driver::MAX_TOKEN_BYTES). If the cluster
/// refuses (another change is under way, `member` is one already, there are
/// [`MAX_MEMBERS`](crate::MAX_MEMBERS), or it didn't catch up), or no leader answers within `timeout`.
pub async fn add_member(
    addr: &str,
    service_id: u32,
    token: &[u8],
    tls: Option<&ClientTls>,
    member: &Member,
    timeout: Duration,
) -> io::Result<u64> {
    let request = |request_id| {
        let (peer, client, admin) = (member.peer.as_bytes(), member.client.as_bytes(), member.admin.as_bytes());
        frame(&wire::AddMember { request_id, id: member.id.0, peer, client, admin })
    };
    admin(addr, service_id, token, tls, timeout, request, changed).await
}

/// Removes member `id` from the cluster, through the admin listener at `addr`. Returns the index
/// of the change, once it's committed: stop the node then.
///
/// # Errors
///
/// If the node refuses `token`, the cluster's admin token (empty if it has none), or it's over
/// [`MAX_TOKEN_BYTES`](crate::driver::MAX_TOKEN_BYTES). If the cluster
/// refuses (another change is under way, `id` isn't a member, or it's the last), or no leader
/// answers within `timeout`.
pub async fn remove_member(
    addr: &str,
    service_id: u32,
    token: &[u8],
    tls: Option<&ClientTls>,
    id: NodeId,
    timeout: Duration,
) -> io::Result<u64> {
    let request = |request_id| frame(&wire::RemoveMember { request_id, id: id.0 });
    admin(addr, service_id, token, tls, timeout, request, changed).await
}

/// Switches the cluster to `protocol` and version `service` of the service's behaviour, through
/// the admin listener at `addr`, once a rolling upgrade has every member running code that has
/// them. Returns the index they're active from.
///
/// # Errors
///
/// If the node refuses `token`, the cluster's admin token (empty if it has none), or it's over
/// [`MAX_TOKEN_BYTES`](crate::driver::MAX_TOKEN_BYTES). If the cluster
/// refuses (a member doesn't support them: the message says the lowest supported; or they're lower
/// than what runs), or no leader answers within `timeout`.
pub async fn activate(
    addr: &str,
    service_id: u32,
    token: &[u8],
    tls: Option<&ClientTls>,
    protocol: u32,
    service: u32,
    timeout: Duration,
) -> io::Result<u64> {
    let request = |request_id| frame(&wire::Activate { request_id, protocol, service_version: service });
    admin(addr, service_id, token, tls, timeout, request, |reply, request_id| match reply {
        wire::Decoded::Activation(m) if m.request_id() == request_id && m.index() == 0 => {
            Some(Err(io::Error::other(format!(
                "refused: the members support protocol {} and service version {} at most, or that's a step back",
                m.lowest_protocol(),
                m.lowest_service_version()
            ))))
        }
        wire::Decoded::Activation(m) if m.request_id() == request_id => Some(Ok(m.index())),
        _ => None,
    })
    .await
}

/// Shuts the cluster down, through the admin listener at `addr`: every member snapshots at the
/// next index, so that a restart loads the snapshot rather than replaying the log, then stops.
/// Returns the index, once the members have it durable.
///
/// # Errors
///
/// If the node refuses `token`, the cluster's admin token (empty if it has none), or it's over
/// [`MAX_TOKEN_BYTES`](crate::driver::MAX_TOKEN_BYTES). If no leader
/// answers within `timeout`.
pub async fn shutdown(
    addr: &str,
    service_id: u32,
    token: &[u8],
    tls: Option<&ClientTls>,
    timeout: Duration,
) -> io::Result<u64> {
    let request = |request_id| frame(&wire::Shutdown { request_id });
    admin(addr, service_id, token, tls, timeout, request, |reply, request_id| match reply {
        wire::Decoded::ShutDown(m) if m.request_id() == request_id => Some(Ok(m.index())),
        _ => None,
    })
    .await
}

/// Hands the leadership to member `to`, or with `None` to the one most caught up, through the admin
/// listener at `addr`: before stopping the leader, so a restart costs a quick election rather
/// than an election timeout. Returns the new leader.
///
/// # Errors
///
/// If the node refuses `token`, the cluster's admin token (empty if it has none), or it's over
/// [`MAX_TOKEN_BYTES`](crate::driver::MAX_TOKEN_BYTES). If the cluster
/// refuses (`to` isn't a member, or the leader is transferring already), no new leader is elected
/// in time, or no leader answers within `timeout`.
pub async fn transfer_leadership(
    addr: &str,
    service_id: u32,
    token: &[u8],
    tls: Option<&ClientTls>,
    to: Option<NodeId>,
    timeout: Duration,
) -> io::Result<NodeId> {
    let request = |request_id| frame(&wire::TransferLeadership { request_id, to: NodeId::to_wire(to) });
    let leader = admin(addr, service_id, token, tls, timeout, request, |reply, request_id| match reply {
        wire::Decoded::LeadershipTransferred(m) if m.request_id() == request_id && m.leader() == 0 => {
            Some(Err(io::Error::other("the leadership wasn't transferred")))
        }
        wire::Decoded::LeadershipTransferred(m) if m.request_id() == request_id => Some(Ok(u64::from(m.leader()))),
        _ => None,
    })
    .await?;
    Ok(NodeId(u8::try_from(leader).expect("a node ID")))
}

fn changed(reply: &wire::Decoded<'_>, request_id: u64) -> Option<io::Result<u64>> {
    match reply {
        wire::Decoded::MembersChanged(m) if m.request_id() == request_id => Some(Ok(m.index())),
        wire::Decoded::MembersRefused(m) if m.request_id() == request_id => {
            Some(Err(io::Error::other("the cluster refused the change")))
        }
        _ => None,
    }
}

fn frame(message: &impl turbofan_sbe::Encode) -> Vec<u8> {
    let mut frame = Vec::new();
    codec::encode_frame(message, &mut frame).expect("admin requests are small");
    frame
}

/// Sends the `request` for a new request ID to the leader, following redirects from `addr`,
/// until `answer` makes something of a reply.
async fn admin(
    addr: &str,
    service_id: u32,
    token: &[u8],
    tls: Option<&ClientTls>,
    timeout: Duration,
    request: impl Fn(u64) -> Vec<u8>,
    answer: impl Fn(&wire::Decoded<'_>, u64) -> Option<io::Result<u64>>,
) -> io::Result<u64> {
    if token.len() > MAX_TOKEN_BYTES {
        let message = format!("the admin token is over {MAX_TOKEN_BYTES} bytes");
        return Err(io::Error::new(io::ErrorKind::InvalidInput, message));
    }
    let request_id = u64::try_from(
        std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos(),
    )
    .unwrap_or_default();
    let hello = Hello { service_id, node_id: None };
    let mut target = addr.to_owned();
    let attempt = async {
        loop {
            match ask(&target, hello, token, tls, &request(request_id), |reply| answer(reply, request_id)).await {
                Some(Ok(answer)) => return answer,
                Some(Err(leader)) if !leader.is_empty() => target = leader,
                _ => tokio::time::sleep(RETRY * 4).await,
            }
        }
    };
    tokio::time::timeout(timeout, attempt)
        .await
        .map_err(|_| io::Error::new(io::ErrorKind::TimedOut, "no leader answered"))?
}

/// Whether a node refused the token (`PermissionDenied`) or a certificate, either way
/// (`InvalidData`), or the address names no host a certificate can (`InvalidInput`).
fn refused(e: &io::Error) -> bool {
    matches!(e.kind(), io::ErrorKind::PermissionDenied | io::ErrorKind::InvalidData | io::ErrorKind::InvalidInput)
}

/// One try: `Ok(answer)`, or `Err(address)` to try instead (empty for none known yet).
async fn ask(
    addr: &str,
    hello: Hello,
    token: &[u8],
    tls: Option<&ClientTls>,
    request: &[u8],
    answer: impl Fn(&wire::Decoded<'_>) -> Option<io::Result<u64>>,
) -> Option<Result<io::Result<u64>, String>> {
    let mut stream = match handshake(addr, hello, wire::Role::Admin, token, tls).await {
        Ok(stream) => stream,
        // Every node has the same token and CAs: no use trying another.
        Err(e) if refused(&e) => return Some(Ok(Err(e))),
        Err(_) => return None,
    };
    write_frames(&mut stream, [request]).await.ok()?;
    let reply = read_frame(&mut stream).await.ok()?;
    let reply = codec::decode_frame(&reply).ok()??.0;
    if let wire::Decoded::Redirect(m) = &reply {
        return Some(Err(String::from_utf8_lossy(m.addr()).into_owned()));
    }
    answer(&reply).map(Ok)
}
