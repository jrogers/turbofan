//! A cluster in memory, for a service's tests (the `testing` feature): its nodes in one process,
//! on one thread, their logs, metas and snapshots in memory, the network between them and their
//! clients a queue of messages, and the clock the test's to move. Nothing waits on a socket or
//! a disk, so a test of a failover takes milliseconds, and a run is the same every time: the
//! seed ([`Builder::seed`]) varies the election timeouts, and nothing else varies at all.
//!
//! The nodes run the same core and write pipeline the driver does, and the clients the same
//! sans-IO [`Client`] a gateway does, so sessions, resends after a failover, timers, snapshots
//! and restarts behave as they do over TCP:
//!
//! ```ignore
//! let mut cluster = Cluster::new(3, |_| MyService::default());
//! let alice = cluster.connect(b"alice").unwrap();
//! cluster.send(alice, b"order 1").unwrap();
//! cluster.run_for(Duration::from_millis(10));
//! assert!(matches!(cluster.events(alice)[..], [Event::Opened(_), Event::Message(_)]));
//! cluster.crash(cluster.leader().unwrap());
//! assert!(cluster.run_until(Duration::from_secs(2), |c| c.leader().is_some()));
//! cluster.send(alice, b"order 2").unwrap(); // on the same session, through the new leader
//! ```
//!
//! Members are added, removed and made learners or voters ([`Cluster::add_member`]), and a node
//! upgraded to a new version of the service, which the cluster then activates
//! ([`Cluster::upgrade`], [`Cluster::activate`]): a rolling upgrade, node by node.
//!
//! What it doesn't model: a disk that tears or rots, and messages dropped or reordered between
//! two nodes. The simulation (`tests/sim.rs`) does those, to test the framework; this is for
//! testing a service on it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::io;
use std::time::Duration;

use crate::client::{self, Answer, Client, ClientConfigError, Event, QueryError, Reconnect, SendError};
use crate::codec::{self, Record};
use crate::meta::Meta;
use crate::metrics::Metrics;
use crate::node::{ConnId, Node, SaveSnapshot, Service, SessionId};
use crate::pipeline::{Batch, Pipeline};
use crate::raft::{self, Entries, MAX_MEMBERS, MAX_READ_BYTES, Member, NodeId, Persist, Snapshot, SnapshotRead};
use crate::{NodeConfig, snapshot, wire};

/// The most client events a client holds before the test takes them ([`Cluster::events`]): a
/// test that never does would otherwise grow them without end.
pub const MAX_CLIENT_EVENTS: usize = 1 << 20;
/// The most events the cluster holds in flight. Messages that beget more than one each (a bug)
/// would otherwise grow them without end in no time of the cluster's, which bounds the runs.
pub const MAX_QUEUED_EVENTS: usize = 1 << 20;
/// How long a client waits to dial again when there's no node to dial, or no leader yet.
const REDIAL: Duration = Duration::from_millis(50);
/// How long [`Builder::build`] waits for the first leader.
const ELECTION_WAIT: Duration = Duration::from_secs(10);

/// A client of a [`Cluster`], as [`Cluster::connect`] made it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ClientId(usize);

/// How a [`Cluster`] is built: its timing and its nodes' configuration.
pub struct Builder {
    nodes: u8,
    seed: u64,
    network_latency: Duration,
    write_latency: Duration,
    configure: Box<dyn Fn(&mut NodeConfig)>,
}

impl Builder {
    /// A cluster of `nodes` members, to configure before it's built.
    ///
    /// # Panics
    ///
    /// If `nodes` is 0 or more than [`MAX_MEMBERS`].
    #[must_use]
    pub fn new(nodes: u8) -> Self {
        assert!((1..=MAX_MEMBERS).contains(&usize::from(nodes)), "1 to {MAX_MEMBERS} nodes");
        Self {
            nodes,
            seed: 0,
            network_latency: Duration::from_micros(100),
            write_latency: Duration::from_micros(50),
            configure: Box::new(|_| {}),
        }
    }

    /// Varies the nodes' election timeouts, and so which is elected when: the same seed, the
    /// same run. 0 by default.
    #[must_use]
    pub fn seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// How long a message takes from one node to another, or between a node and a client: 100
    /// µs by default.
    #[must_use]
    pub fn network_latency(mut self, latency: Duration) -> Self {
        self.network_latency = latency;
        self
    }

    /// How long a node's write takes to be durable: 50 µs by default.
    #[must_use]
    pub fn write_latency(mut self, latency: Duration) -> Self {
        self.write_latency = latency;
        self
    }

    /// Changes each node's configuration as it starts: its timeouts, how often it snapshots.
    /// Not its ID or its members.
    #[must_use]
    pub fn configure(mut self, configure: impl Fn(&mut NodeConfig) + 'static) -> Self {
        self.configure = Box::new(configure);
        self
    }

    /// Starts the nodes, each with the service `service` makes for it, and runs until one leads.
    ///
    /// # Panics
    ///
    /// If none is elected within ten seconds of the cluster's time: the timeouts configured
    /// can't elect one.
    pub fn build<S: Service>(self, service: impl FnMut(NodeId) -> S + 'static) -> Cluster<S> {
        let members: Vec<Member> = (1..=self.nodes).map(|id| member(NodeId(id))).collect();
        let mut cluster = Cluster {
            service: Box::new(service),
            configure: self.configure,
            seed: self.seed,
            network_latency: raft::nanos(self.network_latency),
            write_latency: raft::nanos(self.write_latency),
            now: 0,
            events: BTreeMap::new(),
            next_event: 0,
            nodes: members.iter().map(|m| (m.id, TestNode::default())).collect(),
            members,
            cut: BTreeSet::new(),
            clients: Vec::new(),
            conns: BTreeMap::new(),
            next_conn: 1,
        };
        for id in cluster.members.iter().map(|m| m.id).collect::<Vec<_>>() {
            cluster.start(id);
        }
        assert!(cluster.run_until(ELECTION_WAIT, |c| c.leader().is_some()), "no leader elected");
        cluster
    }
}

impl fmt::Debug for Builder {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Builder").field("nodes", &self.nodes).field("seed", &self.seed).finish_non_exhaustive()
    }
}

/// Nodes in memory, and their clients: see the [module](self).
pub struct Cluster<S> {
    service: Box<dyn FnMut(NodeId) -> S>,
    configure: Box<dyn Fn(&mut NodeConfig)>,
    seed: u64,
    network_latency: u64,
    write_latency: u64,
    now: u64,
    /// What happens next, by when, then the order it was scheduled in.
    events: BTreeMap<(u64, u64), Scheduled>,
    next_event: u64,
    nodes: BTreeMap<NodeId, TestNode<S>>,
    members: Vec<Member>,
    /// Pairs of nodes that can't reach each other, each way round.
    cut: BTreeSet<(NodeId, NodeId)>,
    clients: Vec<TestClient>,
    conns: BTreeMap<ConnId, Conn>,
    next_conn: ConnId,
}

impl<S: Service> Cluster<S> {
    /// A cluster of `nodes` members, each with the service `service` makes for it, a leader
    /// elected: as [`Builder::new`] builds it by default.
    ///
    /// # Panics
    ///
    /// As [`Builder::build`], or if `nodes` is 0 or more than [`MAX_MEMBERS`].
    pub fn new(nodes: u8, service: impl FnMut(NodeId) -> S + 'static) -> Self {
        Builder::new(nodes).build(service)
    }

    /// The cluster's time: from 0 when it was built, moved only by running it.
    #[must_use]
    pub fn now(&self) -> Duration {
        Duration::from_nanos(self.now)
    }

    /// Runs for `duration` of the cluster's time.
    pub fn run_for(&mut self, duration: Duration) {
        let end = self.now.saturating_add(raft::nanos(duration));
        while let Some((&(at, seq), _)) = self.events.first_key_value()
            && at <= end
        {
            let event = self.events.remove(&(at, seq)).expect("just seen");
            self.now = at;
            self.handle(event);
        }
        self.now = end;
    }

    /// Runs until `done` says so, checked after each event, but for at most `limit` of the
    /// cluster's time: whether it did.
    pub fn run_until(&mut self, limit: Duration, mut done: impl FnMut(&Self) -> bool) -> bool {
        let end = self.now.saturating_add(raft::nanos(limit));
        while !done(self) {
            let Some((&(at, seq), _)) = self.events.first_key_value().filter(|(key, _)| key.0 <= end) else {
                self.now = end;
                return false;
            };
            let event = self.events.remove(&(at, seq)).expect("just seen");
            self.now = at;
            self.handle(event);
        }
        true
    }

    /// The node leading and taking clients, if one is: of the highest term,
    /// if a leader cut off from the others hasn't yet seen it's been replaced.
    #[must_use]
    pub fn leader(&self) -> Option<NodeId> {
        let up = self.nodes.iter().filter_map(|(&id, n)| Some((id, n.node.as_ref()?)));
        let leaders = up.filter(|(_, n)| n.is_ready());
        leaders.max_by_key(|(_, n)| n.raft().term()).map(|(id, _)| id)
    }

    /// The nodes' IDs: 1 to the number it was built with, and those [`Cluster::add_member`]
    /// started since, members or not.
    #[must_use]
    pub fn ids(&self) -> Vec<NodeId> {
        self.nodes.keys().copied().collect()
    }

    /// The members, as the leader has committed them, if there's one: a change counts once it's
    /// done, and the next can begin.
    #[must_use]
    pub fn members(&self) -> Option<Vec<Member>> {
        self.leader().and_then(|id| self.node(id)).map(|n| n.raft().members_at(n.raft().commit_index()))
    }

    /// Node `id`'s state, as its metrics endpoint has it, unless the node is down.
    ///
    /// # Panics
    ///
    /// If there's no node `id`.
    #[must_use]
    pub fn metrics(&self, id: NodeId) -> Option<Metrics> {
        self.node(id).map(|n| n.metrics(self.now))
    }

    fn node(&self, id: NodeId) -> Option<&Node<S>> {
        self.nodes.get(&id).expect("a member").node.as_ref()
    }

    /// Node `id`'s service, as it has applied the log, unless the node is down.
    ///
    /// # Panics
    ///
    /// If there's no node `id`.
    #[must_use]
    pub fn service(&self, id: NodeId) -> Option<&S> {
        self.node(id).map(Node::service)
    }

    /// A client with `credentials`, which connects to the cluster as the cluster runs: its
    /// [`Event::Opened`] comes once its session is open.
    ///
    /// # Errors
    ///
    /// If `credentials` are too long ([`Client::new`]).
    pub fn connect(&mut self, credentials: impl Into<Vec<u8>>) -> Result<ClientId, ClientConfigError> {
        // Every node's that could be a member, as a gateway configured for the cluster's growth.
        let addrs: Vec<String> = ids().map(|id| member(id).client).collect();
        let keepalive = self.config(NodeId(1)).session_timeout / 4;
        let client = Client::new(addrs, credentials, keepalive)?;
        let id = self.clients.len();
        self.clients.push(TestClient { client, conn: None, events: VecDeque::new(), tick_at: None, dialing: true });
        self.schedule(0, Scheduled::Dial(id));
        Ok(ClientId(id))
    }

    /// Sends `payload` from `client`: see [`Client::send`].
    ///
    /// # Errors
    ///
    /// As [`Client::send`].
    pub fn send(&mut self, client: ClientId, payload: impl Into<Vec<u8>>) -> Result<u64, SendError> {
        let sent = self.clients[client.0].client.send(payload);
        self.flush_client(client.0);
        sent
    }

    /// Sends each of `payloads` from `client`: see [`Client::send_batch`].
    ///
    /// # Errors
    ///
    /// As [`Client::send_batch`].
    pub fn send_batch(
        &mut self,
        client: ClientId,
        payloads: impl IntoIterator<Item = impl Into<Vec<u8>>>,
    ) -> Result<std::ops::Range<u64>, SendError> {
        let sent = self.clients[client.0].client.send_batch(payloads);
        self.flush_client(client.0);
        sent
    }

    /// Queries `payload` of the state from `client`: see [`Client::query`]. Its answer comes in
    /// [`Cluster::answers`].
    ///
    /// # Errors
    ///
    /// As [`Client::query`].
    pub fn query(&mut self, client: ClientId, payload: impl Into<Vec<u8>>) -> Result<u64, QueryError> {
        let sent = self.clients[client.0].client.query(payload);
        self.flush_client(client.0);
        sent
    }

    /// The queries `client` has had answered, or refused, since this was last asked.
    pub fn answers(&mut self, client: ClientId) -> Vec<Answer> {
        std::iter::from_fn(|| self.clients[client.0].client.poll_answer()).collect()
    }

    /// Closes `client`'s session: see [`Client::close`].
    pub fn close(&mut self, client: ClientId) {
        self.clients[client.0].client.close();
        self.flush_client(client.0);
    }

    /// What `client` has been told since this was last asked, in order.
    pub fn events(&mut self, client: ClientId) -> Vec<Event> {
        self.clients[client.0].events.drain(..).collect()
    }

    /// What [`Cluster::events`] would take, left for it: for a [`Cluster::run_until`] to wait on
    /// what a client is told, `|c| c.peek_events(venue).any(..)`.
    pub fn peek_events(&self, client: ClientId) -> impl Iterator<Item = &Event> {
        self.clients[client.0].events.iter()
    }

    /// `client`'s session, once one is open.
    #[must_use]
    pub fn session(&self, client: ClientId) -> Option<SessionId> {
        self.clients[client.0].client.session()
    }

    /// `client` itself: what it has waiting to be applied, say.
    #[must_use]
    pub fn client(&self, client: ClientId) -> &Client {
        &self.clients[client.0].client
    }

    /// Node `id` crashes: what it was writing is lost, what it had written kept, and its
    /// clients reconnect. A node down already stays down.
    pub fn crash(&mut self, id: NodeId) {
        let test_node = self.nodes.get_mut(&id).expect("a member");
        if test_node.node.take().is_none() {
            return;
        }
        test_node.incarnation += 1;
        (test_node.pipeline, test_node.writing, test_node.tick_at) = (Pipeline::default(), Vec::new(), None);
        let dropped: Vec<ConnId> = self.conns.iter().filter(|(_, c)| c.node == id && c.up).map(|(&k, _)| k).collect();
        for conn in dropped {
            self.drop_conn(conn, REDIAL, false);
        }
    }

    /// Node `id` starts again, from what it had written. A node up already carries on.
    pub fn restart(&mut self, id: NodeId) {
        if self.nodes.get(&id).expect("a member").node.is_none() {
            self.start(id);
        }
    }

    /// Node `id` can't reach the other nodes, nor they it, until [`Cluster::heal`]. Its clients
    /// still reach it.
    pub fn isolate(&mut self, id: NodeId) {
        for other in self.ids().into_iter().filter(|&o| o != id) {
            self.cut.insert((id, other));
            self.cut.insert((other, id));
        }
    }

    /// Every node reaches every other again.
    pub fn heal(&mut self) {
        self.cut.clear();
    }

    /// Asks the leader to hand the leadership to `to`, or to the follower most caught up: whether
    /// there was a leader to ask. The handover follows as the cluster runs.
    pub fn transfer_leadership(&mut self, to: Option<NodeId>) -> bool {
        self.admin(&wire::TransferLeadership { request_id: 1, to: NodeId::to_wire(to) })
    }

    /// Asks the leader to take a snapshot, which each node does at the same index: whether there
    /// was a leader to ask.
    pub fn take_snapshot(&mut self) -> bool {
        self.admin(&wire::SnapshotRequest { request_id: 1 })
    }

    /// Asks the leader to add node `id` as a member, a learner if `learner` (it votes once
    /// [`Cluster::set_role`] makes it a voter): whether there was a leader to ask. A node `id`
    /// hasn't been one of the cluster's is started first, with an empty disk, to join it; one
    /// removed earlier comes back with its disk as it was. The change follows as the cluster runs,
    /// once the node has caught up; the leader turns it down if another is under way.
    ///
    /// # Panics
    ///
    /// If `id` is 0 or more than [`MAX_MEMBERS`].
    pub fn add_member(&mut self, id: NodeId, learner: bool) -> bool {
        assert!(ids().any(|i| i == id), "node IDs are 1 to {MAX_MEMBERS}");
        if self.leader().is_none() {
            return false;
        }
        if let std::collections::btree_map::Entry::Vacant(e) = self.nodes.entry(id) {
            e.insert(TestNode::default());
            self.start(id);
        }
        let m = member(id);
        let (peer, client, admin) = (m.peer.as_bytes(), m.client.as_bytes(), m.admin.as_bytes());
        self.admin(&wire::AddMember { request_id: 1, id: id.0, learner: u8::from(learner), peer, client, admin })
    }

    /// Asks the leader to remove member `id`, perhaps itself: whether there was a leader to ask.
    /// The node runs on, no longer a member, until it's crashed.
    pub fn remove_member(&mut self, id: NodeId) -> bool {
        self.admin(&wire::RemoveMember { request_id: 1, id: id.0 })
    }

    /// Asks the leader to make member `id` a learner, or a voter: whether there was a leader
    /// to ask.
    pub fn set_role(&mut self, id: NodeId, learner: bool) -> bool {
        self.admin(&wire::SetRole { request_id: 1, id: id.0, learner: u8::from(learner) })
    }

    /// Node `id` is upgraded: stopped as a crash stops it, and started again, from what it had
    /// written, on the service `service` makes, then and each time it starts after. Its
    /// [`Service::version`] counts once [`Cluster::activate`] switches the cluster to it.
    ///
    /// # Panics
    ///
    /// If there's no node `id`.
    pub fn upgrade(&mut self, id: NodeId, service: impl FnMut(NodeId) -> S + 'static) {
        self.crash(id);
        self.nodes.get_mut(&id).expect("a member").service = Some(Box::new(service));
        self.start(id);
    }

    /// Asks the leader to switch the cluster to `service_version` of the service's behaviour,
    /// and the framework's to the newest every member runs: whether there was a leader to ask.
    /// The leader turns it down if a member's service is older ([`Service::version`]), or hasn't
    /// been heard from yet; once it's applied, each node's [`Service::on_activate`] is called.
    pub fn activate(&mut self, service_version: u32) -> bool {
        let Some(protocol) = self.leader().and_then(|id| self.node(id)).map(|n| n.raft().lowest_versions().0) else {
            return false;
        };
        self.admin(&wire::Activate { request_id: 1, protocol, service_version })
    }

    fn admin(&mut self, request: &impl turbofan_sbe::Encode) -> bool {
        let Some(id) = self.leader() else { return false };
        let mut frame = Vec::new();
        codec::encode_frame(request, &mut frame).expect("an operator's request is small");
        let conn = self.next_conn;
        self.next_conn += 1;
        self.conns.insert(conn, Conn { node: id, client: None, up: true, to_node_at: 0, to_client_at: 0 });
        self.send_conn(conn, frame, true);
        true
    }

    /// Node `id`'s configuration: one it was built with begins the cluster with the others; one
    /// added later begins with no members, to join it.
    fn config(&self, id: NodeId) -> NodeConfig {
        let members = if self.members.iter().any(|m| m.id == id) { self.members.clone() } else { Vec::new() };
        let mut config = NodeConfig::new(id, members.clone());
        config.raft.seed = self.seed ^ (u64::from(id) << 32);
        (self.configure)(&mut config);
        assert_eq!(config.raft.id, id, "a node keeps its ID");
        assert_eq!(config.raft.members, members, "a node keeps its members");
        config
    }

    fn start(&mut self, id: NodeId) {
        let mut config = self.config(id);
        let test_node = self.nodes.get_mut(&id).expect("a member");
        config.raft.seed ^= test_node.incarnation;
        let (log, snapshot) = test_node.disk.open(config.raft.log_window_bytes);
        let test_node = self.nodes.get_mut(&id).expect("a member");
        let service = match test_node.service.as_mut() {
            Some(upgraded) => upgraded(id),
            None => (self.service)(id),
        };
        let test_node = self.nodes.get_mut(&id).expect("a member");
        let meta = test_node.disk.meta;
        let mut node = Node::new(config, service, meta, snapshot.as_ref().map(|(s, _)| s.clone()), log, self.now);
        if let Some((_, bytes)) = &snapshot {
            node.restore(&mut bytes.as_slice()).expect("the service restores the snapshot it took");
        }
        test_node.node = Some(node);
        self.flush(id);
    }

    fn handle(&mut self, event: Scheduled) {
        match event {
            Scheduled::Deliver { from, to, frame } => {
                let now = self.now;
                if self.nodes[&to].node.is_some() && !self.nodes[&to].pipeline.takes_inputs() {
                    // Restoring a snapshot: again once it has.
                    self.schedule(self.write_latency, Scheduled::Deliver { from, to, frame });
                    return;
                }
                let Some(node) = self.nodes.get_mut(&to).expect("a member").node.as_mut() else { return };
                node.on_peer(from, &decode(&frame), now);
                self.flush(to);
            }
            Scheduled::Tick(id) => {
                self.nodes.get_mut(&id).expect("a member").tick_at = None;
                let now = self.now;
                let Some(node) = self.nodes.get_mut(&id).expect("a member").node.as_mut() else { return };
                node.on_tick(now);
                self.flush(id);
            }
            Scheduled::Written { id, incarnation } => {
                if self.nodes[&id].incarnation == incarnation {
                    self.written(id);
                }
            }
            Scheduled::Saved { id, incarnation, save } => {
                if self.nodes[&id].incarnation == incarnation {
                    self.saved(id, save);
                }
            }
            Scheduled::ToNode { conn, frame } => self.deliver_to_node(conn, &frame),
            Scheduled::ToClient { conn, frame } => self.deliver_to_client(conn, &frame),
            Scheduled::Dial(c) => self.dial(c),
            Scheduled::ClientTick(c) => {
                self.clients[c].tick_at = None;
                self.clients[c].client.on_tick(self.now);
                self.flush_client(c);
            }
        }
    }

    /// Node `id`, if it's up and takes inputs: one restoring a snapshot waits for that write, so
    /// what came meanwhile is handed it once it's done.
    fn live(&mut self, id: NodeId) -> Option<&mut Node<S>> {
        let test_node = self.nodes.get_mut(&id).expect("a member");
        if test_node.node.is_some() && !test_node.pipeline.takes_inputs() {
            return None;
        }
        test_node.node.as_mut()
    }

    /// Carries out what node `id` has to do, as the driver would.
    fn flush(&mut self, id: NodeId) {
        let test_node = self.nodes.get_mut(&id).expect("a member");
        let Some(node) = test_node.node.as_mut() else { return };
        let taken = test_node.pipeline.take(node);
        let incarnation = test_node.incarnation;
        if let Some(batches) = taken.write {
            test_node.writing = batches;
            self.schedule(self.write_latency, Scheduled::Written { id, incarnation });
        }
        if let Some(save) = taken.save {
            self.schedule(self.write_latency, Scheduled::Saved { id, incarnation, save });
        }
        for (to, frame) in taken.peer_appends.into_iter().chain(taken.peer_sends) {
            self.send_peer(id, to, frame);
        }
        for (conn, frame) in taken.client_sends {
            self.send_conn(conn, frame, false);
        }
        let test_node = self.nodes.get_mut(&id).expect("a member");
        let node = test_node.node.as_ref().expect("up");
        if node.stopped() && !test_node.pipeline.writing() {
            // Shut down by an operator: down, with its disk as it left it.
            self.crash(id);
            return;
        }
        let deadline = node.next_deadline().max(self.now + 1);
        if test_node.tick_at.is_none_or(|at| deadline < at) {
            test_node.tick_at = Some(deadline);
            self.schedule(deadline - self.now, Scheduled::Tick(id));
        }
    }

    /// Node `id`'s write is durable: its batches reach the disk, what they asked for is read,
    /// the node told, and what waited for them sent.
    fn written(&mut self, id: NodeId) {
        let test_node = self.nodes.get_mut(&id).expect("a member");
        let (mut reads, mut chunks) = (Vec::new(), Vec::new());
        for batch in std::mem::take(&mut test_node.writing) {
            test_node.disk.write(&batch, &mut reads, &mut chunks);
        }
        let restored = test_node.pipeline.restore().map(|index| test_node.disk.snapshot_bytes(index));
        let mut from = restored.as_deref();
        let from = from.as_mut().map(|bytes| -> &mut dyn io::Read { bytes });
        let node = test_node.node.as_mut().expect("written only while up");
        let sends = test_node.pipeline.written(node, &reads, &chunks, from, self.now);
        for (to, frame) in sends.expect("the service restores the leader's snapshot") {
            self.send_peer(id, to, frame);
        }
        self.flush(id);
    }

    /// Node `id`'s snapshot, taken earlier, is written now, as a state as it was when taken.
    fn saved(&mut self, id: NodeId, save: SaveSnapshot) {
        let SaveSnapshot { index, term, members, writer } = save;
        let mut bytes = Vec::new();
        writer(&mut bytes).expect("the service writes its snapshot");
        let checksum = snapshot::checksum(index, term, &members, &bytes);
        let snapshot = Snapshot { index, term, len: crate::widen(bytes.len()), checksum, members };
        let test_node = self.nodes.get_mut(&id).expect("a member");
        test_node.disk.snapshots.insert(index, (snapshot.clone(), bytes));
        test_node.node.as_mut().expect("saved only while up").on_snapshot_saved(snapshot);
        self.flush(id);
    }

    fn send_peer(&mut self, from: NodeId, to: NodeId, frame: raft::Frame) {
        if !self.cut.contains(&(from, to)) {
            self.schedule(self.network_latency, Scheduled::Deliver { from, to, frame });
        }
    }

    /// Sends on a connection, in order behind what it already carries.
    fn send_conn(&mut self, conn: ConnId, frame: Vec<u8>, to_node: bool) {
        let earliest = self.now + self.network_latency;
        let Some(c) = self.conns.get_mut(&conn).filter(|c| c.up) else { return };
        let slot = if to_node { &mut c.to_node_at } else { &mut c.to_client_at };
        let at = earliest.max(*slot);
        *slot = at;
        let event = if to_node { Scheduled::ToNode { conn, frame } } else { Scheduled::ToClient { conn, frame } };
        self.schedule(at - self.now, event);
    }

    fn deliver_to_node(&mut self, conn: ConnId, frame: &[u8]) {
        let Some(&Conn { node: id, client, up: true, .. }) = self.conns.get(&conn) else { return };
        let now = self.now;
        let Some(node) = self.live(id) else {
            // Restoring a snapshot: again once it has.
            let frame = frame.to_vec();
            self.schedule(self.write_latency, Scheduled::ToNode { conn, frame });
            return;
        };
        if client.is_some() {
            node.on_client(conn, &decode(frame), now);
        } else {
            node.on_admin(conn, &decode(frame), now);
        }
        self.flush(id);
    }

    fn deliver_to_client(&mut self, conn: ConnId, frame: &[u8]) {
        let Some(&Conn { client: Some(c), up: true, .. }) = self.conns.get(&conn) else { return };
        self.clients[c].client.on_frame(frame).expect("the cluster's frames are whole");
        self.flush_client(c);
    }

    fn dial(&mut self, c: usize) {
        self.clients[c].dialing = false;
        let target = self.clients[c].client.target();
        let id = ids().find(|&id| member(id).client == target).expect("a node's address");
        if !self.nodes.contains_key(&id) {
            // Never started: refused at once, as a host that doesn't listen.
            self.clients[c].client.on_disconnected();
            self.dial_later(c, Duration::ZERO);
            return;
        }
        if self.nodes[&id].node.is_none() {
            self.clients[c].client.on_disconnected();
            self.dial_later(c, REDIAL);
            return;
        }
        let conn = self.next_conn;
        self.next_conn += 1;
        self.conns.insert(conn, Conn { node: id, client: Some(c), up: true, to_node_at: 0, to_client_at: 0 });
        self.clients[c].conn = Some(conn);
        self.clients[c].client.on_connected(self.now);
        self.flush_client(c);
    }

    fn dial_later(&mut self, c: usize, after: Duration) {
        if !self.clients[c].dialing {
            self.clients[c].dialing = true;
            self.schedule(raft::nanos(after), Scheduled::Dial(c));
        }
    }

    /// Both ends see connection `conn` drop; its client, if it's one, dials again `after`. A
    /// client that was redirected dropped it itself, already pointed at where to go: it isn't
    /// told it dropped, which would point it at the next node instead.
    fn drop_conn(&mut self, conn: ConnId, after: Duration, redirected: bool) {
        let c = self.conns.get_mut(&conn).expect("a connection");
        c.up = false;
        let (id, client) = (c.node, c.client);
        if let Some(node) = self.nodes.get_mut(&id).expect("a member").node.as_mut() {
            node.on_disconnect(conn);
        }
        if let Some(client) = client
            && self.clients[client].conn == Some(conn)
        {
            self.clients[client].conn = None;
            if !redirected {
                self.clients[client].client.on_disconnected();
            }
            self.dial_later(client, after);
        }
    }

    /// Carries out what client `c` has to do: its frames sent, a redirect followed, its events
    /// kept for the test.
    fn flush_client(&mut self, c: usize) {
        let frames = self.clients[c].client.take_frames();
        if let Some(conn) = self.clients[c].conn {
            for frame in frames {
                self.send_conn(conn, frame, true);
            }
            match self.clients[c].client.take_reconnect() {
                Some(Reconnect::ToLeader) => self.drop_conn(conn, Duration::ZERO, true),
                Some(Reconnect::Later) => self.drop_conn(conn, REDIAL, true),
                None => {}
            }
        }
        let test_client = &mut self.clients[c];
        while let Some(event) = test_client.client.poll_event() {
            assert!(test_client.events.len() < MAX_CLIENT_EVENTS, "a client's events are taken now and then");
            test_client.events.push_back(event);
        }
        let deadline = test_client.client.next_deadline().max(self.now + 1);
        if test_client.tick_at.is_none_or(|at| deadline < at) {
            test_client.tick_at = Some(deadline);
            self.schedule(deadline - self.now, Scheduled::ClientTick(c));
        }
    }

    fn schedule(&mut self, after: u64, event: Scheduled) {
        assert!(self.events.len() < MAX_QUEUED_EVENTS, "a storm of messages at {:?}", Duration::from_nanos(self.now));
        self.events.insert((self.now + after, self.next_event), event);
        self.next_event += 1;
    }
}

impl<S> fmt::Debug for Cluster<S> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let up: Vec<NodeId> = self.nodes.iter().filter(|(_, n)| n.node.is_some()).map(|(&id, _)| id).collect();
        f.debug_struct("Cluster")
            .field("now", &Duration::from_nanos(self.now))
            .field("up", &up)
            .field("clients", &self.clients.len())
            .finish_non_exhaustive()
    }
}

enum Scheduled {
    Deliver {
        from: NodeId,
        to: NodeId,
        frame: raft::Frame,
    },
    Tick(NodeId),
    /// A node's write is durable, unless it crashed since.
    Written {
        id: NodeId,
        incarnation: u64,
    },
    /// A node's snapshot is written, unless it crashed since.
    Saved {
        id: NodeId,
        incarnation: u64,
        save: SaveSnapshot,
    },
    ToNode {
        conn: ConnId,
        frame: Vec<u8>,
    },
    ToClient {
        conn: ConnId,
        frame: Vec<u8>,
    },
    Dial(usize),
    ClientTick(usize),
}

struct TestNode<S> {
    node: Option<Node<S>>,
    /// What makes its service, once it's upgraded: the cluster's until then.
    service: Option<Box<dyn FnMut(NodeId) -> S>>,
    disk: Disk,
    pipeline: Pipeline,
    /// The batches being written.
    writing: Vec<Batch>,
    /// How many times it has crashed: what a crash cut short is ignored when it comes.
    incarnation: u64,
    tick_at: Option<u64>,
}

impl<S> Default for TestNode<S> {
    fn default() -> Self {
        Self {
            node: None,
            service: None,
            disk: Disk::default(),
            pipeline: Pipeline::default(),
            writing: Vec::new(),
            incarnation: 0,
            tick_at: None,
        }
    }
}

struct TestClient {
    client: Client,
    conn: Option<ConnId>,
    events: VecDeque<client::Event>,
    tick_at: Option<u64>,
    /// A dial is scheduled.
    dialing: bool,
}

struct Conn {
    node: NodeId,
    /// The client's, or none for an operator's.
    client: Option<usize>,
    up: bool,
    /// When the last frame each way arrives: the next comes no sooner.
    to_node_at: u64,
    to_client_at: u64,
}

/// What a node has durably written: its meta, its log's records, encoded, and its snapshots.
struct Disk {
    meta: Meta,
    /// The index of `records[0]`.
    first: u64,
    records: Vec<Vec<u8>>,
    /// By index, each with its bytes.
    snapshots: BTreeMap<u64, (Snapshot, Vec<u8>)>,
    /// A snapshot arriving from the leader, its chunks written where they go.
    incoming: Vec<u8>,
}

impl Default for Disk {
    fn default() -> Self {
        Self { meta: Meta::default(), first: 1, records: Vec::new(), snapshots: BTreeMap::new(), incoming: Vec::new() }
    }
}

impl Disk {
    /// The log as a node starting finds it, and its latest snapshot: as the driver opens its
    /// files, a log that ends before the snapshot, or disagrees with it, going on from it.
    fn open(&mut self, window_bytes: usize) -> (Entries, Option<(Snapshot, Vec<u8>)>) {
        let mut log = Entries::default();
        for bytes in &self.records {
            log.recover(Record::decode(bytes).expect("written whole").expect("a record").0, window_bytes);
        }
        let latest = self.snapshots.last_key_value().map(|(_, s)| s.clone());
        if let Some((snapshot, _)) = &latest {
            let next = self.first + crate::widen(self.records.len());
            if next <= snapshot.index || log.term_at(snapshot.index).is_some_and(|t| t != snapshot.term) {
                (self.records, self.first, log) = (Vec::new(), snapshot.index + 1, Entries::default());
            }
        }
        (log, latest)
    }

    /// Writes `batch`, then reads what it asked for into `reads` and `chunks`.
    fn write(&mut self, batch: &Batch, reads: &mut Vec<Vec<u8>>, chunks: &mut Vec<(u64, u64, Vec<u8>)>) {
        for op in &batch.persist {
            self.apply(op);
        }
        reads.extend(batch.reads.iter().map(|&from| self.read(from)));
        for &SnapshotRead { index, offset } in &batch.snapshot_reads {
            let bytes = self.snapshot_bytes(index);
            let start = usize::try_from(offset).expect("within the snapshot").min(bytes.len());
            let end = bytes.len().min(start + MAX_READ_BYTES);
            chunks.push((index, offset, bytes[start..end].to_vec()));
        }
    }

    fn apply(&mut self, op: &Persist) {
        match op {
            Persist::Meta(meta) => self.meta = *meta,
            Persist::Append(records) => {
                for record in codec::records(records) {
                    let record = record.expect("a node appends whole records");
                    assert_eq!(record.index, self.first + crate::widen(self.records.len()), "appended in order");
                    let mut bytes = Vec::new();
                    record.encode_into(&mut bytes);
                    self.records.push(bytes);
                }
            }
            Persist::Trust => {}
            Persist::TruncateFrom(index) => {
                assert!(*index >= self.first, "truncated within the log");
                self.records.truncate(usize::try_from(index - self.first).expect("within the log"));
            }
            Persist::Compact { log, snapshot } => {
                self.compact_to(*log);
                self.snapshots.retain(|&index, _| index >= *snapshot);
            }
            Persist::SnapshotChunk { offset, bytes, .. } => {
                let offset = usize::try_from(*offset).expect("a chunk within memory");
                if offset == 0 {
                    self.incoming.clear();
                }
                let end = offset + bytes.len();
                self.incoming.resize(self.incoming.len().max(end), 0);
                self.incoming[offset..end].copy_from_slice(bytes);
            }
            Persist::InstallSnapshot { snapshot, replace_log } => self.install(snapshot, *replace_log),
        }
    }

    fn install(&mut self, snapshot: &Snapshot, replace_log: bool) {
        let len = usize::try_from(snapshot.len).expect("a snapshot within memory");
        assert!(self.incoming.len() >= len, "the leader's snapshot arrived whole");
        let bytes = self.incoming[..len].to_vec();
        let sum = snapshot::checksum(snapshot.index, snapshot.term, &snapshot.members, &bytes);
        assert_eq!(sum, snapshot.checksum, "the leader's snapshot arrived as it sent it");
        self.snapshots.insert(snapshot.index, (snapshot.clone(), bytes));
        self.snapshots.retain(|&index, _| index >= snapshot.index);
        if replace_log {
            (self.records, self.first) = (Vec::new(), snapshot.index + 1);
        } else {
            self.compact_to(snapshot.index);
        }
    }

    /// Drops the records up to `index`, which a snapshot covers.
    fn compact_to(&mut self, index: u64) {
        let dropped = (index + 1).saturating_sub(self.first);
        let dropped = usize::try_from(dropped).expect("within the log").min(self.records.len());
        self.records.drain(..dropped);
        self.first += crate::widen(dropped);
    }

    /// Whole records from `from` on: at least one, then up to [`MAX_READ_BYTES`].
    fn read(&self, from: u64) -> Vec<u8> {
        assert!(from >= self.first, "the log holds {from}");
        let at = usize::try_from(from - self.first).expect("within the log");
        assert!(at < self.records.len(), "the log holds {from}");
        let mut out = Vec::new();
        for record in &self.records[at..] {
            if !out.is_empty() && out.len() + record.len() > MAX_READ_BYTES {
                break;
            }
            out.extend_from_slice(record);
        }
        out
    }

    fn snapshot_bytes(&self, index: u64) -> Vec<u8> {
        self.snapshots.get(&index).expect("a snapshot it has").1.clone()
    }
}

/// The IDs a node of a cluster can have.
fn ids() -> impl Iterator<Item = NodeId> {
    (1..=u8::try_from(MAX_MEMBERS).expect("a node ID")).map(NodeId)
}

fn member(id: NodeId) -> Member {
    Member {
        id,
        peer: format!("peer-{id}"),
        client: format!("client-{id}"),
        admin: format!("admin-{id}"),
        learner: false,
    }
}

fn decode(frame: &[u8]) -> wire::Decoded<'_> {
    codec::decode_frame(frame).expect("frames the cluster made").expect("a whole frame").0
}

#[cfg(test)]
mod tests;
