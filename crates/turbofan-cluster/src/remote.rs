//! Talking to a cluster over TCP: [`ClusterClient`] for gateways, [`Admin`] for operators. Each
//! goes over TLS when given a [`ClientTls`].

use std::fmt;
use std::io;
use std::ops::Range;
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    SendBatch(Vec<Vec<u8>>, oneshot::Sender<Result<Range<u64>, SendError>>),
    Close,
}

/// A session with a cluster, kept open across failovers: messages are applied exactly once, in
/// order, and responses arrive exactly once, in order. It runs on a task of its own, which
/// stops when this is dropped.
///
/// # Examples
///
/// Send a message to the cluster of service 1, and print the responses until the session
/// closes. The [guide](crate::guide::clients) says what's guaranteed, and when it isn't.
///
/// ```no_run
/// use std::time::Duration;
///
/// use turbofan_cluster::ClusterClient;
/// use turbofan_cluster::client::Event;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let members = ["10.0.0.1:7101", "10.0.0.2:7101", "10.0.0.3:7101"];
///     let mut session = ClusterClient::connect(members, 1, b"desk1:secret", Duration::from_millis(500))?;
///     session.send(b"hello".to_vec()).await?;
///     while let Some(event) = session.recv().await {
///         match event {
///             Event::Message(response) => println!("{} bytes", response.len()),
///             Event::Closed { reason, .. } => {
///                 println!("closed: {reason:?}");
///                 break;
///             }
///             _ => {}
///         }
///     }
///     Ok(())
/// }
/// ```
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

    /// Sends each of `payloads`, in order, returning their `client_seq`s, as [`Client::send_batch`]:
    /// all or none. Handed to the client's task once, and written together: for a gateway with
    /// many to send, far less than a [`ClusterClient::send`] each.
    ///
    /// # Errors
    ///
    /// As [`Client::send_batch`]: if there isn't room for them all under
    /// [`crate::client::MAX_UNACKED`] (read responses, then try again), the session closed, or any
    /// payload is too long.
    pub async fn send_batch(
        &self,
        payloads: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> Result<Range<u64>, SendError> {
        let payloads = payloads.into_iter().map(Into::into).collect();
        let (reply, rx) = oneshot::channel();
        self.commands.send(Command::SendBatch(payloads, reply)).await.map_err(|_| SendError::Closed)?;
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

    /// What the cluster said next, waiting for something, as [`ClusterClient::recv`]: up to
    /// `limit` events, appended to `events`, and how many. Zero once the session is over (or for
    /// a `limit` of zero). For a gateway that answers what arrives with a
    /// [`ClusterClient::send_batch`], a batch at a time.
    pub async fn recv_many(&mut self, events: &mut Vec<Event>, limit: usize) -> usize {
        self.events.recv_many(events, limit).await
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
    let hello = Hello { service_id, node_id: None, epoch: Arc::default() };
    let mut redirected_at: Option<Instant> = None;
    loop {
        let Ok(stream) = handshake(client.target(), &hello, wire::Role::Client, &[], tls.as_ref()).await else {
            client.on_disconnected();
            tokio::time::sleep(RETRY).await;
            continue;
        };
        let mut conn = Connection::new(stream);
        client.on_connected(now());
        conn.write(&mut client);
        // One timer, moved only when the deadline does: a new one a pass takes tokio's timer lock twice a message.
        let mut sleep = Box::pin(tokio::time::sleep_until(start.into()));
        loop {
            let wake = (start + Duration::from_nanos(client.next_deadline())).into();
            (sleep.deadline() != wake).then(|| sleep.as_mut().reset(wake));
            tokio::select! {
                command = commands.recv() => match command {
                    None => return,
                    Some(Command::Send(payload, reply)) => { let _ = reply.send(client.send(payload)); }
                    Some(Command::SendBatch(payloads, reply)) => { let _ = reply.send(client.send_batch(payloads)); }
                    Some(Command::Close) => client.close(),
                },
                frame = conn.frames.recv() => {
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
                () = &mut sleep => client.on_tick(now()),
                _ = &mut conn.written => {
                    client.on_disconnected();
                    break;
                }
            }
            applied.send_if_modified(|seq| std::mem::replace(seq, client.applied()) != client.applied());
            while let Some(event) = client.poll_event() {
                if events.send(event).await.is_err() {
                    return;
                }
            }
            // Over: `recv` returns `None` once the events are read, and nothing reattaches.
            if client.is_closed() {
                return;
            }
            conn.write(&mut client);
            // Redirected: the target already points where to go.
            match client.take_reconnect() {
                Some(Reconnect::ToLeader) => {
                    // Nodes that disagree on the leader, mid-election, send it back and forth:
                    // once at once, then paced.
                    if redirected_at.is_some_and(|at| at.elapsed() < Duration::from_secs(1)) {
                        tokio::time::sleep(RETRY).await;
                    }
                    redirected_at = Some(Instant::now());
                    break;
                }
                Some(Reconnect::Later) => {
                    tokio::time::sleep(RETRY).await;
                    break;
                }
                None => {}
            }
        }
    }
}

/// A client's connection, read and written on tasks of their own, which stop when it's dropped.
/// Written apart, a node slow to read doesn't stop the client reading what the node sends: it
/// would then drop the client, whose frames back up. What waits to be written is bounded by what
/// the client holds anyway, its unacknowledged messages.
struct Connection {
    frames: mpsc::Receiver<Vec<u8>>,
    out: mpsc::UnboundedSender<Vec<Vec<u8>>>,
    read: JoinHandle<()>,
    /// Done once writing failed.
    written: JoinHandle<()>,
}

impl Connection {
    fn new(stream: Conn) -> Self {
        let (mut reader, mut writer) = stream.split();
        let (frames_tx, frames) = mpsc::channel(256);
        let read = tokio::spawn(async move {
            while let Ok(frame) = read_frame(&mut reader).await {
                if frames_tx.send(frame).await.is_err() {
                    return;
                }
            }
        });
        let (out, mut to_write) = mpsc::unbounded_channel::<Vec<Vec<u8>>>();
        let written = tokio::spawn(async move {
            while let Some(mut frames) = to_write.recv().await {
                // Whatever else queued meanwhile goes with it, under one flush.
                while let Ok(more) = to_write.try_recv() {
                    frames.extend(more);
                }
                if write_frames(&mut writer, frames.iter().map(Vec::as_slice)).await.is_err() {
                    return;
                }
            }
        });
        Self { frames, out, read, written }
    }

    /// Hands what the client has to send to the writer. Should it have stopped, the connection
    /// failed, which [`Connection::written`] says.
    fn write(&self, client: &mut Client) {
        let frames = client.take_frames();
        if !frames.is_empty() {
            let _ = self.out.send(frames);
        }
    }
}

impl Drop for Connection {
    fn drop(&mut self) {
        self.read.abort();
        self.written.abort();
    }
}

/// A connection to `addr` that has said hello for `role`, with an operator's `token` (empty for
/// none), over TLS if there's `tls`. `PermissionDenied` if the node refused the token.
async fn handshake(
    addr: &str,
    hello: &Hello,
    role: wire::Role,
    token: &[u8],
    tls: Option<&ClientTls>,
) -> io::Result<Conn> {
    let timed_out = |_| io::Error::from(io::ErrorKind::TimedOut);
    let mut stream =
        tokio::time::timeout(CONNECT_TIMEOUT, tls::connect(addr, tls, None)).await.map_err(timed_out)??;
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

    /// Adds `member` to the cluster, as a learner if it's [`Member::learner`]: it catches up from
    /// the leader, then joins. Start it first, with `--join`. Returns the index of the change,
    /// once it's committed; or, if `member` is one already, as it is, of the latest change (or
    /// a snapshot since), so that a request retried after it was made isn't refused.
    ///
    /// # Errors
    ///
    /// [`AdminError::MembersRefused`] if another change is under way, a member has its ID with
    /// other addresses or role, there are [`MAX_MEMBERS`](crate::MAX_MEMBERS), or it didn't catch up; or
    /// [`AdminError::TokenTooLong`], [`AdminError::Denied`] or [`AdminError::TimedOut`].
    pub async fn add_member(&self, member: &Member) -> Result<u64, AdminError> {
        let request = |request_id| {
            let (peer, client, admin) = (member.peer.as_bytes(), member.client.as_bytes(), member.admin.as_bytes());
            let learner = u8::from(member.learner);
            frame(&wire::AddMember { request_id, id: member.id.0, learner, peer, client, admin })
        };
        self.ask(request, changed).await
    }

    /// Removes member `id` from the cluster. Returns the index of the change, once it's
    /// committed: stop the node then. If `id` isn't a member, that of the latest change (or a
    /// snapshot since).
    ///
    /// # Errors
    ///
    /// [`AdminError::MembersRefused`] if another change is under way, or it's the last; or [`AdminError::TokenTooLong`], [`AdminError::Denied`] or
    /// [`AdminError::TimedOut`].
    pub async fn remove_member(&self, id: NodeId) -> Result<u64, AdminError> {
        let request = |request_id| frame(&wire::RemoveMember { request_id, id: id.0 });
        self.ask(request, changed).await
    }

    /// Makes member `id` a learner, or a voter, in place, rather than removing it and adding it
    /// again, which ships it the whole log. A learner made a voter catches up first: check
    /// [`Admin::status`] that it's close. Returns the index of the change, once it's committed;
    /// if it has that role already, that of the latest change (or a snapshot since).
    ///
    /// # Errors
    ///
    /// [`AdminError::MembersRefused`] if another change is under way, `id` isn't a member, it's
    /// the last voter, or it didn't catch up; or [`AdminError::TokenTooLong`],
    /// [`AdminError::Denied`] or [`AdminError::TimedOut`], which a leader too old to answer
    /// gives too.
    pub async fn set_role(&self, id: NodeId, learner: bool) -> Result<u64, AdminError> {
        let request = |request_id| frame(&wire::SetRole { request_id, id: id.0, learner: u8::from(learner) });
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

    /// The cluster as its leader knows it: how far each member has the log, so an operator can
    /// tell a learner has caught up before it's needed.
    ///
    /// # Errors
    ///
    /// [`AdminError::TokenTooLong`], [`AdminError::Denied`] or [`AdminError::TimedOut`], which
    /// a leader too old to answer gives too.
    pub async fn status(&self) -> Result<ClusterStatus, AdminError> {
        let request = |request_id| frame(&wire::StatusRequest { request_id });
        self.ask(request, |reply, request_id| match reply {
            wire::Decoded::Status(m) if m.request_id() == request_id => Some(Ok(ClusterStatus {
                leader: NodeId(m.leader()),
                last_index: m.last_index(),
                commit_index: m.commit_index(),
                protocol: m.protocol(),
                service_version: m.service_version(),
                members: m
                    .member()
                    .map(|m| MemberStatus { id: NodeId(m.id()), learner: m.learner() == 1, matched: m.matched() })
                    .collect(),
            })),
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
    /// Returns the new leader; `to`, if it leads already. (A request with `None` retried after
    /// it was made hands it on again.)
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

    /// Makes the learner at this address the one member of a new cluster, of a new epoch, from
    /// its log as it is, with its cluster lost: fence the old one first, since from here a lost
    /// cluster and a cut link look alike. The old cluster's nodes can't talk to the new one, which
    /// grows with [`Admin::add_member`]. Returns the index the new epoch was logged at: what the
    /// old cluster committed past what this learner had is lost.
    ///
    /// # Errors
    ///
    /// [`AdminError::NotPromoted`] if it isn't a learner, or heard a leader within an election
    /// timeout; or [`AdminError::TokenTooLong`], [`AdminError::Denied`] or
    /// [`AdminError::TimedOut`].
    pub async fn promote(&self) -> Result<u64, AdminError> {
        let request = |request_id| frame(&wire::Promote { request_id });
        self.ask(request, |reply, request_id| match reply {
            wire::Decoded::Promoted(m) if m.request_id() == request_id => {
                Some(if m.index() == 0 { Err(AdminError::NotPromoted) } else { Ok(m.index()) })
            }
            _ => None,
        })
        .await
    }

    /// Has the node at this address capture its log: its latest snapshot and its log from there
    /// to its commit index, in a directory of its data directory, to copy off it and replay
    /// offline ([`crate::capture`]). Any member answers, learners too, of what it has.
    ///
    /// # Errors
    ///
    /// [`AdminError::CaptureFailed`] if it couldn't, saying why; or [`AdminError::TokenTooLong`],
    /// [`AdminError::Denied`] or [`AdminError::TimedOut`], which a node too old to capture gives
    /// too.
    pub async fn capture(&self) -> Result<Captured, AdminError> {
        let request = |request_id| frame(&wire::Capture { request_id });
        self.ask(request, |reply, request_id| match reply {
            wire::Decoded::Captured(m) if m.request_id() == request_id => {
                let detail = String::from_utf8_lossy(m.detail()).into_owned();
                Some(match m.refused() {
                    0 => Ok(Captured { snapshot_index: m.snapshot_index(), to: m.to(), dir: detail }),
                    _ => Err(AdminError::CaptureFailed(detail)),
                })
            }
            _ => None,
        })
        .await
    }

    /// Sends the `request` for a new request ID to the leader, following redirects from the
    /// address, until `answer` makes something of a reply.
    async fn ask<T>(
        &self,
        request: impl Fn(u64) -> Vec<u8>,
        answer: impl Fn(&wire::Decoded<'_>, u64) -> Option<Result<T, AdminError>>,
    ) -> Result<T, AdminError> {
        if self.token.len() > MAX_TOKEN_BYTES {
            return Err(AdminError::TokenTooLong);
        }
        let request_id = u64::try_from(
            std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap_or_default().as_nanos(),
        )
        .unwrap_or_default();
        let hello = Hello { service_id: self.service_id, node_id: None, epoch: Arc::default() };
        let (token, tls) = (&self.token[..], self.tls.as_ref());
        let mut target = self.addr.clone();
        let attempt = async {
            let mut redirected = false;
            loop {
                match ask(&target, &hello, token, tls, &request(request_id), |reply| answer(reply, request_id)).await {
                    Some(Ok(answer)) => return answer,
                    Some(Err(leader)) if !leader.is_empty() => {
                        // Redirected again: nodes that disagree on the leader, mid-election.
                        if std::mem::replace(&mut redirected, true) {
                            tokio::time::sleep(RETRY).await;
                        }
                        target = leader;
                    }
                    // The leader it was sent to may be gone: the address finds the next.
                    _ => {
                        target.clone_from(&self.addr);
                        tokio::time::sleep(RETRY * 4).await;
                    }
                }
            }
        };
        tokio::time::timeout(self.timeout, attempt).await.map_err(|_| AdminError::TimedOut)?
    }
}

/// The cluster as its leader knows it, from [`Admin::status`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ClusterStatus {
    /// The leader that answered.
    pub leader: NodeId,
    /// The end of the leader's log.
    pub last_index: u64,
    /// The leader's commit index.
    pub commit_index: u64,
    /// The protocol active ([`Admin::activate`]), or `None` from a leader older than 0.4.0.
    pub protocol: Option<u32>,
    /// The service version active, or `None` from a leader older than 0.4.0.
    pub service_version: Option<u32>,
    /// Each member, the leader among them.
    pub members: Vec<MemberStatus>,
}

impl ClusterStatus {
    /// How many entries member `id` is behind the leader's log, if it's a member.
    #[must_use]
    pub fn lag(&self, id: NodeId) -> Option<u64> {
        self.members.iter().find(|m| m.id == id).map(|m| self.last_index.saturating_sub(m.matched))
    }
}

/// A member, as the leader knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MemberStatus {
    /// Its ID.
    pub id: NodeId,
    /// It takes the log but doesn't vote.
    pub learner: bool,
    /// The highest index known durable on it.
    pub matched: u64,
}

/// A capture of a node's log, from [`Admin::capture`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct Captured {
    /// The snapshot it starts from (0: none, from the start of the log).
    pub snapshot_index: u64,
    /// The last entry it holds, committed.
    pub to: u64,
    /// Its directory, on the node.
    pub dir: String,
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
    /// The node isn't a learner, or heard a leader within an election timeout.
    NotPromoted,
    /// The node couldn't capture its log: this says why.
    CaptureFailed(String),
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
            Self::NotPromoted => f.write_str("refused: not a learner, or its leader is alive"),
            Self::CaptureFailed(why) => f.write_str(why),
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
async fn ask<T>(
    addr: &str,
    hello: &Hello,
    token: &[u8],
    tls: Option<&ClientTls>,
    request: &[u8],
    answer: impl Fn(&wire::Decoded<'_>) -> Option<Result<T, AdminError>>,
) -> Option<Result<Result<T, AdminError>, String>> {
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
