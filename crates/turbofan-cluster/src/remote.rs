//! Talking to a cluster over TCP: [`ClusterClient`] for gateways, [`Admin`] for operators. Each
//! goes over TLS when given a [`ClientTls`].

use std::fmt;
use std::io;
use std::time::{Duration, Instant};

use tokio::io::WriteHalf;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;

use crate::client::{Client, ClientConfigError, Event, Reconnect, SendError};
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
    applied: watch::Receiver<u64>,
    task: JoinHandle<()>,
}

impl ClusterClient {
    /// A session with the cluster of `service_id` whose members take clients at `addrs`, with
    /// `credentials` for the service to check, sending a keepalive every `keepalive` while idle.
    ///
    /// # Errors
    ///
    /// As [`Client::new`]: if `addrs` is empty, or the credentials are too long.
    ///
    /// # Panics
    ///
    /// If this isn't called on a tokio runtime.
    pub fn connect(
        addrs: impl IntoIterator<Item = impl Into<String>>,
        service_id: u32,
        credentials: impl Into<Vec<u8>>,
        keepalive: Duration,
    ) -> Result<Self, ClientConfigError> {
        Ok(Self::start(Client::new(addrs, credentials, keepalive)?, service_id, None))
    }

    /// As [`ClusterClient::connect`], over TLS: for a cluster with
    /// [`driver::Config::tls`](crate::driver::Config::tls).
    ///
    /// # Errors
    ///
    /// As [`ClusterClient::connect`].
    ///
    /// # Panics
    ///
    /// As [`ClusterClient::connect`].
    pub fn connect_tls(
        addrs: impl IntoIterator<Item = impl Into<String>>,
        service_id: u32,
        credentials: impl Into<Vec<u8>>,
        keepalive: Duration,
        tls: ClientTls,
    ) -> Result<Self, ClientConfigError> {
        Ok(Self::start(Client::new(addrs, credentials, keepalive)?, service_id, Some(tls)))
    }

    fn start(client: Client, service_id: u32, tls: Option<ClientTls>) -> Self {
        let (commands, rx) = mpsc::channel(64);
        let (tx, events) = mpsc::channel(MAX_QUEUED_EVENTS);
        let (applied_tx, applied) = watch::channel(0);
        let task = tokio::spawn(run(client, service_id, tls, rx, tx, applied_tx));
        Self { commands, events, applied, task }
    }

    /// Sends `payload`, returning its `client_seq`. It's resent after a failover until the
    /// cluster has applied it.
    ///
    /// # Errors
    ///
    /// If [`crate::client::MAX_UNACKED`] messages wait to be applied (read responses, then try
    /// again), the session closed, or the payload is too long.
    pub async fn send(&self, payload: impl Into<Vec<u8>>) -> Result<u64, SendError> {
        let (reply, rx) = oneshot::channel();
        self.commands.send(Command::Send(payload.into(), reply)).await.map_err(|_| SendError::Closed)?;
        rx.await.map_err(|_| SendError::Closed)?
    }

    /// The highest `client_seq` (as [`ClusterClient::send`] returned) the cluster has applied,
    /// as [`Client::applied`]: watch it to know when what was sent is in the cluster, to let go
    /// of a message a gateway is forwarding, say.
    #[must_use]
    pub fn applied(&self) -> watch::Receiver<u64> {
        self.applied.clone()
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
    applied: watch::Sender<u64>,
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
                    Some(Command::Send(payload, reply)) => { let _ = reply.send(client.send(payload)); }
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
            applied.send_if_modified(|seq| std::mem::replace(seq, client.applied()) != client.applied());
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

/// How long [`Admin`] waits for a leader's answer, unless [`Admin::with_timeout`] says otherwise.
const ADMIN_TIMEOUT: Duration = Duration::from_secs(30);

/// An operator's requests to a cluster, through the admin listener at an address and redirected
/// to the leader's: snapshots, membership changes, upgrades, leadership transfers and shutdown.
#[derive(Debug, Clone)]
pub struct Admin {
    addr: String,
    service_id: u32,
    token: Vec<u8>,
    tls: Option<ClientTls>,
    timeout: Duration,
}

impl Admin {
    /// Requests to the cluster of `service_id` through the admin listener at `addr`: with no
    /// token, over plain TCP, waiting 30 s for an answer.
    #[must_use]
    pub fn new(addr: impl Into<String>, service_id: u32) -> Self {
        Self { addr: addr.into(), service_id, token: Vec::new(), tls: None, timeout: ADMIN_TIMEOUT }
    }

    /// The same, with the cluster's admin token, at most [`MAX_TOKEN_BYTES`].
    #[must_use]
    pub fn with_token(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.token = token.into();
        self
    }

    /// The same, over TLS, with an operator's certificate: for a cluster with
    /// [`driver::Config::tls`](crate::driver::Config::tls).
    #[must_use]
    pub fn with_tls(mut self, tls: ClientTls) -> Self {
        self.tls = Some(tls);
        self
    }

    /// The same, waiting `timeout` for a leader's answer. A new member may take a while to catch
    /// up, so allow [`Admin::add_member`] longer.
    #[must_use]
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Asks the cluster to snapshot every node at the next index. Returns the index, once a
    /// quorum has it durable.
    ///
    /// # Errors
    ///
    /// [`AdminError::TokenTooLong`], [`AdminError::Denied`] or [`AdminError::TimedOut`].
    pub async fn request_snapshot(&self) -> Result<u64, AdminError> {
        let request = |request_id| frame(&wire::SnapshotRequest { request_id });
        self.ask(request, |reply, request_id| match reply {
            wire::Decoded::SnapshotTaken(m) if m.request_id() == request_id => Some(Ok(m.index())),
            _ => None,
        })
        .await
    }

    /// Adds `member` to the cluster: it catches up from the leader, then joins. Start it first,
    /// with `--join`. Returns the index of the change, once it's committed.
    ///
    /// # Errors
    ///
    /// [`AdminError::MembersRefused`] if another change is under way, `member` is one already,
    /// there are [`MAX_MEMBERS`](crate::MAX_MEMBERS), or it didn't catch up; or
    /// [`AdminError::TokenTooLong`], [`AdminError::Denied`] or [`AdminError::TimedOut`].
    pub async fn add_member(&self, member: &Member) -> Result<u64, AdminError> {
        let request = |request_id| {
            let (peer, client, admin) = (member.peer.as_bytes(), member.client.as_bytes(), member.admin.as_bytes());
            frame(&wire::AddMember { request_id, id: member.id.0, peer, client, admin })
        };
        self.ask(request, changed).await
    }

    /// Removes member `id` from the cluster. Returns the index of the change, once it's
    /// committed: stop the node then.
    ///
    /// # Errors
    ///
    /// [`AdminError::MembersRefused`] if another change is under way, `id` isn't a member, or
    /// it's the last; or [`AdminError::TokenTooLong`], [`AdminError::Denied`] or
    /// [`AdminError::TimedOut`].
    pub async fn remove_member(&self, id: NodeId) -> Result<u64, AdminError> {
        let request = |request_id| frame(&wire::RemoveMember { request_id, id: id.0 });
        self.ask(request, changed).await
    }

    /// Switches the cluster to `protocol` and `service_version` of the service's behaviour, once
    /// a rolling upgrade has every member running code that has them. Returns the index they're
    /// active from.
    ///
    /// # Errors
    ///
    /// [`AdminError::ActivationRefused`] if a member doesn't support them, or they're lower than
    /// what runs; or [`AdminError::TokenTooLong`], [`AdminError::Denied`] or
    /// [`AdminError::TimedOut`].
    pub async fn activate(&self, protocol: u32, service_version: u32) -> Result<u64, AdminError> {
        let request = |request_id| frame(&wire::Activate { request_id, protocol, service_version });
        self.ask(request, |reply, request_id| match reply {
            wire::Decoded::Activation(m) if m.request_id() == request_id && m.index() == 0 => {
                Some(Err(AdminError::ActivationRefused {
                    lowest_protocol: m.lowest_protocol(),
                    lowest_service_version: m.lowest_service_version(),
                }))
            }
            wire::Decoded::Activation(m) if m.request_id() == request_id => Some(Ok(m.index())),
            _ => None,
        })
        .await
    }

    /// Shuts the cluster down: every member snapshots at the next index, so that a restart loads
    /// the snapshot rather than replaying the log, then stops. Returns the index, once the members
    /// have it durable.
    ///
    /// # Errors
    ///
    /// [`AdminError::TokenTooLong`], [`AdminError::Denied`] or [`AdminError::TimedOut`].
    pub async fn shutdown(&self) -> Result<u64, AdminError> {
        let request = |request_id| frame(&wire::Shutdown { request_id });
        self.ask(request, |reply, request_id| match reply {
            wire::Decoded::ShutDown(m) if m.request_id() == request_id => Some(Ok(m.index())),
            _ => None,
        })
        .await
    }

    /// Hands the leadership to member `to`, or with `None` to the one most caught up: before
    /// stopping the leader, so a restart costs a quick election rather than an election timeout.
    /// Returns the new leader.
    ///
    /// # Errors
    ///
    /// [`AdminError::NotTransferred`] if `to` isn't a member, the leader is transferring already,
    /// or no new leader was elected in time; or [`AdminError::TokenTooLong`],
    /// [`AdminError::Denied`] or [`AdminError::TimedOut`].
    pub async fn transfer_leadership(&self, to: Option<NodeId>) -> Result<NodeId, AdminError> {
        let request = |request_id| frame(&wire::TransferLeadership { request_id, to: NodeId::to_wire(to) });
        let leader = self
            .ask(request, |reply, request_id| match reply {
                wire::Decoded::LeadershipTransferred(m) if m.request_id() == request_id => {
                    Some(NodeId::from_wire(m.leader()).map(u64::from).ok_or(AdminError::NotTransferred))
                }
                _ => None,
            })
            .await?;
        Ok(NodeId(u8::try_from(leader).expect("a node ID")))
    }

    /// Sends the `request` for a new request ID to the leader, following redirects from the
    /// address, until `answer` makes something of a reply.
    async fn ask(
        &self,
        request: impl Fn(u64) -> Vec<u8>,
        answer: impl Fn(&wire::Decoded<'_>, u64) -> Option<Result<u64, AdminError>>,
    ) -> Result<u64, AdminError> {
        if self.token.len() > MAX_TOKEN_BYTES {
            return Err(AdminError::TokenTooLong);
        }
        let request_id = u64::try_from(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos(),
        )
        .unwrap_or_default();
        let hello = Hello { service_id: self.service_id, node_id: None };
        let (token, tls) = (&self.token[..], self.tls.as_ref());
        let mut target = self.addr.clone();
        let attempt = async {
            loop {
                match ask(&target, hello, token, tls, &request(request_id), |reply| answer(reply, request_id)).await {
                    Some(Ok(answer)) => return answer,
                    Some(Err(leader)) if !leader.is_empty() => target = leader,
                    _ => tokio::time::sleep(RETRY * 4).await,
                }
            }
        };
        tokio::time::timeout(self.timeout, attempt).await.map_err(|_| AdminError::TimedOut)?
    }
}

/// Why an [`Admin`] request failed.
#[derive(Debug)]
#[non_exhaustive]
pub enum AdminError {
    /// The admin token is over [`MAX_TOKEN_BYTES`].
    TokenTooLong,
    /// A node refused the token (`PermissionDenied`) or a certificate, either way
    /// (`InvalidData`), or the address names no host a certificate can (`InvalidInput`). Every
    /// node would, so no other is tried.
    Denied(io::Error),
    /// The cluster refused a membership change.
    MembersRefused,
    /// The cluster refused an activation: these are the highest its members all support.
    ActivationRefused {
        /// The highest protocol every member supports.
        lowest_protocol: u32,
        /// The highest service version every member supports.
        lowest_service_version: u32,
    },
    /// The leadership wasn't transferred.
    NotTransferred,
    /// No leader answered in time.
    TimedOut,
}

impl fmt::Display for AdminError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::TokenTooLong => write!(f, "the admin token is over {MAX_TOKEN_BYTES} bytes"),
            Self::Denied(error) => write!(f, "refused: {error}"),
            Self::MembersRefused => f.write_str("the cluster refused the change"),
            Self::ActivationRefused { lowest_protocol, lowest_service_version } => write!(
                f,
                "refused: the members support protocol {lowest_protocol} and service version \
                 {lowest_service_version} at most, or that's a step back"
            ),
            Self::NotTransferred => f.write_str("the leadership wasn't transferred"),
            Self::TimedOut => f.write_str("no leader answered"),
        }
    }
}

impl std::error::Error for AdminError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Denied(error) => Some(error),
            _ => None,
        }
    }
}

fn changed(reply: &wire::Decoded<'_>, request_id: u64) -> Option<Result<u64, AdminError>> {
    match reply {
        wire::Decoded::MembersChanged(m) if m.request_id() == request_id => Some(Ok(m.index())),
        wire::Decoded::MembersRefused(m) if m.request_id() == request_id => Some(Err(AdminError::MembersRefused)),
        _ => None,
    }
}

fn frame(message: &impl turbofan_sbe::Encode) -> Vec<u8> {
    let mut frame = Vec::new();
    codec::encode_frame(message, &mut frame).expect("admin requests are small");
    frame
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
    answer: impl Fn(&wire::Decoded<'_>) -> Option<Result<u64, AdminError>>,
) -> Option<Result<Result<u64, AdminError>, String>> {
    let mut stream = match handshake(addr, hello, wire::Role::Admin, token, tls).await {
        Ok(stream) => stream,
        // Every node has the same token and CAs: no use trying another.
        Err(e) if refused(&e) => return Some(Ok(Err(AdminError::Denied(e)))),
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
