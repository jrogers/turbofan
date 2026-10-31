//! A deterministic simulation of a cluster and its clients. One seed drives every delay, drop,
//! partition and crash, and invariants are checked after every event. A failure names its
//! seed, and `SIM_SEED=<seed> cargo test --test sim` replays it exactly. `SIM_SEEDS=<n>` runs
//! more seeds, from `SIM_BASE`.
//!
//! Faults: peer messages delayed, reordered, dropped and duplicated; partitions; client
//! connections reset; nodes crashing between events, as leader with appends in flight, or
//! partway through persisting a batch (a prefix of its operations, and of its records, reaching
//! the disk), then restarting from what's on disk.
//!
//! Nodes snapshot every few hundred entries and when an operator asks, compacting their logs,
//! so restarts recover from snapshots and lagging followers are sent one. Every snapshot of an
//! index must be the same bytes on every node.
//!
//! The service is an echo with timers. Clients send numbered messages through failovers, and
//! must see every message applied once and in order, every response once and in order, and every
//! timer fire once.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap};

use turbofan_cluster::client::{self, Client};
use turbofan_cluster::codec::{self, Record};
use turbofan_cluster::meta::Meta;
use turbofan_cluster::node::{self, ConnId, Node};
use turbofan_cluster::raft::{Config, NodeId, Persist, Role, Snapshot};
use turbofan_cluster::{Ctx, Service, SessionId};

const MS: u64 = 1_000_000;
const FAULT_PHASE: u64 = 20_000 * MS;
const QUIET_PHASE: u64 = 10_000 * MS;
const CLIENTS: usize = 3;
/// Every this many messages, the echo service sets a timer.
const TIMER_EVERY: u64 = 7;
const TIMER_SPACE: u64 = 1_000_000;
/// The admin's connection, which [`Sim::conns`] marks with this client index.
const ADMIN: usize = usize::MAX;

#[test]
fn simulate() {
    let seeds: Vec<u64> = match std::env::var("SIM_SEED") {
        Ok(seed) => vec![seed.parse().expect("SIM_SEED is a number")],
        Err(_) => {
            let n = std::env::var("SIM_SEEDS").map_or(16, |n| n.parse().expect("SIM_SEEDS is a number"));
            let base = std::env::var("SIM_BASE").map_or(0, |b| b.parse().expect("SIM_BASE is a number"));
            (base..base + n).collect()
        }
    };
    for seed in seeds {
        let result = std::panic::catch_unwind(|| Sim::new(seed).run());
        if let Err(panic) = result {
            eprintln!("simulation failed: SIM_SEED={seed}");
            std::panic::resume_unwind(panic);
        }
    }
}

/// Echoes `a<k>` for message `k`, and every [`TIMER_EVERY`]th sets a timer that sends `t<k>`.
/// Panics if a session's messages arrive other than once each, in order.
#[derive(Debug, Default, PartialEq, Eq)]
struct Echo {
    last: BTreeMap<SessionId, u64>,
    fired: u64,
}

impl Service for Echo {
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let k: u64 = std::str::from_utf8(payload).unwrap().parse().unwrap();
        let last = self.last.entry(session).or_default();
        assert_eq!(k, *last + 1, "session {session}: message {k} applied after {last}");
        *last = k;
        ctx.send(session, format!("a{k}").as_bytes());
        if k.is_multiple_of(TIMER_EVERY) {
            ctx.schedule_timer(session * TIMER_SPACE + k, ctx.now() + 30 * MS);
        }
    }

    fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
        self.fired += 1;
        ctx.send(id / TIMER_SPACE, format!("t{}", id % TIMER_SPACE).as_bytes());
    }

    fn snapshot(&self, out: &mut Vec<u8>) {
        out.extend_from_slice(&self.fired.to_le_bytes());
        for (session, last) in &self.last {
            out.extend_from_slice(&session.to_le_bytes());
            out.extend_from_slice(&last.to_le_bytes());
        }
    }

    fn restore(&mut self, bytes: &[u8]) {
        let words: Vec<u64> = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w)).collect();
        self.fired = words[0];
        self.last = words[1..].as_chunks::<2>().0.iter().map(|&[session, last]| (session, last)).collect();
    }
}

/// SplitMix64.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, n: u64) -> u64 {
        self.next() % n
    }

    /// True one time in `n`.
    fn one_in(&mut self, n: u64) -> bool {
        self.below(n) == 0
    }

    fn pick<T: Copy>(&mut self, from: &[T]) -> T {
        from[usize::try_from(self.below(from.len() as u64)).unwrap()]
    }
}

enum Event {
    Deliver { from: NodeId, to: NodeId, frame: Vec<u8> },
    Tick(NodeId),
    Restart(NodeId),
    Partition,
    Heal,
    ToNode { conn: ConnId, frame: Vec<u8> },
    ToClient { conn: ConnId, frame: Vec<u8> },
    Dial(usize),
    ClientTick(usize),
    ClientSend(usize),
    AdminRequest,
}

/// What a node has durably written.
struct Disk {
    meta: Meta,
    snapshot: Option<Snapshot>,
    /// The index of `log[0]`.
    first: u64,
    /// The records from `first`.
    log: Vec<Vec<u8>>,
}

impl Disk {
    /// `keep_compacted`: a compacted prefix stays on disk, as the log's whole segments do until
    /// they're past the snapshot.
    fn apply(&mut self, op: &Persist, keep_compacted: bool) {
        match op {
            Persist::Meta(meta) => self.meta = *meta,
            Persist::Append(records) => {
                for record in codec::records(records) {
                    self.append(record.unwrap());
                }
            }
            Persist::TruncateFrom(index) => self.log.truncate(usize::try_from(index - self.first).unwrap()),
            Persist::Snapshot { snapshot, replace_log } => {
                if *replace_log {
                    self.log.clear();
                    self.first = snapshot.index + 1;
                } else if !keep_compacted {
                    let drop = usize::try_from(snapshot.index + 1 - self.first).unwrap().min(self.log.len());
                    self.log.drain(..drop);
                    self.first = snapshot.index + 1;
                }
                self.snapshot = Some(snapshot.clone());
            }
        }
    }

    fn append(&mut self, record: Record<'_>) {
        assert_eq!(record.index, self.first + self.log.len() as u64, "appends continue the log");
        let mut bytes = Vec::new();
        record.encode_into(&mut bytes);
        self.log.push(bytes);
    }

    /// The payload at `index`, if the log on disk has it.
    fn record(&self, index: u64) -> Option<&[u8]> {
        let bytes = self.log.get(usize::try_from(index.checked_sub(self.first)?).unwrap())?;
        Some(Record::decode(bytes).unwrap().unwrap().0.payload)
    }

    /// Whether the entry at `index` is durable here: in the log, or covered by the snapshot.
    fn has(&self, index: u64, payload: &[u8]) -> bool {
        self.snapshot.as_ref().is_some_and(|s| s.index >= index) || self.record(index) == Some(payload)
    }
}

struct SimNode {
    node: Option<Node<Echo>>,
    disk: Disk,
    /// Committed entries checked against `Sim::committed`.
    checked: u64,
    tick_at: Option<u64>,
    incarnation: u64,
}

struct SimClient {
    client: Client,
    conn: Option<ConnId>,
    dialing: bool,
    tick_at: Option<u64>,
    sent: u64,
    /// The last `a<k>` response.
    answered: u64,
    timers: BTreeSet<u64>,
}

/// A client connection: in order each way, like TCP.
struct Conn {
    client: usize,
    node: NodeId,
    up: bool,
    to_node_at: u64,
    to_client_at: u64,
}

struct Sim {
    seed: u64,
    rng: Rng,
    now: u64,
    queue: BinaryHeap<(Reverse<u64>, Reverse<u64>)>,
    events: BTreeMap<u64, Event>,
    next_event: u64,
    members: Vec<NodeId>,
    nodes: BTreeMap<NodeId, SimNode>,
    clients: Vec<SimClient>,
    conns: BTreeMap<ConnId, Conn>,
    next_conn: ConnId,
    /// `cut[a][b]`: peer messages from a to b are lost.
    cut: [[bool; 8]; 8],
    faults: bool,
    /// The payload committed at each index, by whichever node saw it committed first, and the
    /// term it was committed in (that node's: the leader's).
    committed: Vec<Vec<u8>>,
    commit_terms: Vec<u64>,
    leaders: BTreeMap<u64, NodeId>,
    crashes: u64,
    /// The bytes of every snapshot taken, by index: every node's must match.
    snapshots: BTreeMap<u64, Vec<u8>>,
    installs: u64,
    admin_requests: u64,
    admin_answers: u64,
}

impl Sim {
    fn new(seed: u64) -> Self {
        let mut rng = Rng(seed);
        let n = if rng.one_in(2) { 3 } else { 5 };
        let members: Vec<NodeId> = (1..=n).collect();
        let addrs: Vec<String> = members.iter().map(|id| format!("n{id}")).collect();
        let clients = (0..CLIENTS)
            .map(|_| SimClient {
                client: Client::new(addrs.clone(), 500 * MS),
                conn: None,
                dialing: false,
                tick_at: None,
                sent: 0,
                answered: 0,
                timers: BTreeSet::new(),
            })
            .collect();
        let mut sim = Self {
            seed,
            rng,
            now: 0,
            queue: BinaryHeap::new(),
            events: BTreeMap::new(),
            next_event: 0,
            members: members.clone(),
            nodes: BTreeMap::new(),
            clients,
            conns: BTreeMap::new(),
            next_conn: 1,
            cut: [[false; 8]; 8],
            faults: true,
            committed: Vec::new(),
            commit_terms: Vec::new(),
            leaders: BTreeMap::new(),
            crashes: 0,
            snapshots: BTreeMap::new(),
            installs: 0,
            admin_requests: 0,
            admin_answers: 0,
        };
        for id in members {
            let disk = Disk { meta: Meta::default(), snapshot: None, first: 1, log: Vec::new() };
            let node = SimNode { node: None, disk, checked: 0, tick_at: None, incarnation: 0 };
            sim.nodes.insert(id, node);
            sim.start(id);
        }
        for c in 0..CLIENTS {
            sim.dial_later(c);
            sim.schedule(10 * MS, Event::ClientSend(c));
        }
        sim.schedule(1000 * MS, Event::Partition);
        sim.schedule(2000 * MS, Event::AdminRequest);
        sim
    }

    fn run(mut self) {
        while let Some((Reverse(at), Reverse(seq))) = self.queue.pop() {
            // Stop once every node has heard the last commit, which could be a heartbeat away.
            if at > FAULT_PHASE + QUIET_PHASE && (self.converged() || at > FAULT_PHASE + QUIET_PHASE + 1000 * MS) {
                break;
            }
            if self.faults && at > FAULT_PHASE {
                self.end_faults();
            }
            self.now = at;
            let event = self.events.remove(&seq).expect("each event is queued once");
            self.handle(event);
            self.check();
        }
        self.check_end();
    }

    fn handle(&mut self, event: Event) {
        match event {
            Event::Deliver { from, to, frame } => {
                let Some(node) = self.nodes.get_mut(&to).unwrap().node.as_mut() else { return };
                node.on_peer(from, &codec::decode_frame(&frame).unwrap().unwrap().0, self.now);
                self.flush(to);
            }
            Event::Tick(id) => {
                let sim_node = self.nodes.get_mut(&id).unwrap();
                sim_node.tick_at = None;
                let Some(node) = sim_node.node.as_mut() else { return };
                node.on_tick(self.now);
                self.flush(id);
            }
            Event::Restart(id) => self.start(id),
            Event::Partition => self.partition(),
            Event::Heal => {
                self.cut = [[false; 8]; 8];
                if self.faults {
                    let after = 500 * MS + self.rng.below(3000 * MS);
                    self.schedule(after, Event::Partition);
                }
            }
            Event::ToNode { conn, frame } => self.deliver_to_node(conn, &frame),
            Event::ToClient { conn, frame } => self.deliver_to_client(conn, &frame),
            Event::Dial(c) => self.dial(c),
            Event::ClientTick(c) => {
                self.clients[c].tick_at = None;
                self.clients[c].client.on_tick(self.now);
                self.flush_client(c);
            }
            Event::ClientSend(c) => self.client_send(c),
            Event::AdminRequest => self.admin_request(),
        }
        if self.faults && self.rng.one_in(300) {
            let id = self.rng.pick(&self.members);
            self.crash(id);
        }
        // A leader dying with its appends in flight: some followers get them, some don't.
        if self.faults
            && self.rng.one_in(600)
            && let Some(leader) = self.leader()
        {
            self.crash(leader);
        }
    }

    /// An operator asks the leader for a snapshot, on a connection of its own.
    fn admin_request(&mut self) {
        if self.now < FAULT_PHASE + QUIET_PHASE / 2 {
            let after = 1000 * MS + self.rng.below(3000 * MS);
            self.schedule(after, Event::AdminRequest);
        }
        let Some(id) = self.leader() else { return };
        self.admin_requests += 1;
        let conn = self.next_conn;
        self.next_conn += 1;
        self.conns.insert(conn, Conn { client: ADMIN, node: id, up: true, to_node_at: 0, to_client_at: 0 });
        let mut frame = Vec::new();
        codec::encode_frame(&turbofan_cluster::wire::SnapshotRequest { request_id: self.admin_requests }, &mut frame)
            .unwrap();
        let node = self.nodes.get_mut(&id).unwrap().node.as_mut().unwrap();
        node.on_admin(conn, &codec::decode_frame(&frame).unwrap().unwrap().0, self.now);
        self.flush(id);
    }

    fn converged(&self) -> bool {
        let mut nodes = self.nodes.values().filter_map(|n| n.node.as_ref());
        let first = nodes.next().map(|n| (n.raft().commit_index(), n.applied()));
        nodes.all(|n| Some((n.raft().commit_index(), n.applied())) == first)
    }

    fn leader(&self) -> Option<NodeId> {
        self.nodes
            .iter()
            .find(|(_, n)| n.node.as_ref().is_some_and(|n| n.raft().role() == Role::Leader))
            .map(|(&id, _)| id)
    }

    fn partition(&mut self) {
        if !self.faults {
            return;
        }
        // Cut some nodes off from the rest, sometimes one way only.
        let isolated: Vec<NodeId> = self.members.iter().copied().filter(|_| self.rng.one_in(2)).collect();
        for &a in &self.members {
            for &b in &self.members {
                let one_way = self.rng.one_in(8);
                if isolated.contains(&a) != isolated.contains(&b) && (!one_way || a < b) {
                    self.cut[usize::from(a)][usize::from(b)] = true;
                }
            }
        }
        let after = 100 * MS + self.rng.below(2000 * MS);
        self.schedule(after, Event::Heal);
    }

    /// Carries out a node's outputs as a driver must: send a leader's appends, persist (perhaps
    /// crashing partway, the appends already sent), report what's durable, and again while that
    /// produces more to persist (applying entries can snapshot); then send the rest.
    fn flush(&mut self, id: NodeId) {
        let (mut peer_sends, mut client_sends) = (Vec::new(), Vec::new());
        loop {
            let out = self.nodes.get_mut(&id).unwrap().node.as_mut().unwrap().take_outputs();
            for (to, frame) in out.peer_appends {
                self.send_peer(id, to, frame);
            }
            // A snapshot from the leader that replaces the log: what it covers is checked by its
            // bytes, and the disk's log, still to be replaced, may hold uncommitted entries there.
            for op in &out.persist {
                if let Persist::Snapshot { snapshot, replace_log: true } = op {
                    let checked = &mut self.nodes.get_mut(&id).unwrap().checked;
                    *checked = (*checked).max(snapshot.index);
                }
            }
            // Before this batch's snapshot compacts the disk: what the node committed and
            // compacted from memory, since it last persisted, is still there.
            self.check_committed(id);
            if self.faults && !out.persist.is_empty() && self.rng.one_in(150) {
                self.crash_while_persisting(id, &out.persist);
                return;
            }
            for op in &out.persist {
                self.check_snapshot(op);
                let keep = self.rng.one_in(2);
                self.nodes.get_mut(&id).unwrap().disk.apply(op, keep);
            }
            let node = self.nodes.get_mut(&id).unwrap().node.as_mut().unwrap();
            node.on_persisted(node.raft().last_index());
            peer_sends.extend(out.peer_sends);
            client_sends.extend(out.client_sends);
            if out.persist.is_empty() {
                break;
            }
        }
        for (to, frame) in peer_sends {
            self.send_peer(id, to, frame);
        }
        for (conn, frame) in client_sends {
            self.send_conn(conn, frame, false);
        }
        let sim_node = self.nodes.get_mut(&id).unwrap();
        let deadline = sim_node.node.as_ref().unwrap().next_deadline().max(self.now + 1);
        if sim_node.tick_at.is_none_or(|at| deadline < at) {
            sim_node.tick_at = Some(deadline);
            self.schedule(deadline - self.now, Event::Tick(id));
        }
    }

    /// Every node's snapshot of an index is the same bytes.
    fn check_snapshot(&mut self, op: &Persist) {
        let Persist::Snapshot { snapshot, replace_log } = op else { return };
        self.installs += u64::from(*replace_log);
        let first = self.snapshots.entry(snapshot.index).or_insert_with(|| snapshot.bytes.clone());
        assert!(*first == snapshot.bytes, "seed {}: snapshots of {} differ", self.seed, snapshot.index);
    }

    fn send_peer(&mut self, from: NodeId, to: NodeId, frame: Vec<u8>) {
        if self.cut[usize::from(from)][usize::from(to)] || (self.faults && self.rng.one_in(50)) {
            return;
        }
        if self.faults && self.rng.one_in(100) {
            let after = MS + self.rng.below(5 * MS);
            self.schedule(after, Event::Deliver { from, to, frame: frame.clone() });
        }
        // Mostly prompt, now and then later than an election timeout: stale votes and appends.
        let after =
            if self.faults && self.rng.one_in(10) { self.rng.below(400 * MS) } else { MS / 2 + self.rng.below(5 * MS) };
        self.schedule(after, Event::Deliver { from, to, frame });
    }

    /// Sends on a client connection, in order; now and then the connection resets instead.
    fn send_conn(&mut self, conn: ConnId, frame: Vec<u8>, to_node: bool) {
        if !self.conns[&conn].up {
            return;
        }
        if self.faults && self.rng.one_in(300) {
            self.reset(conn);
            return;
        }
        let delay = MS / 2 + self.rng.below(3 * MS);
        let c = self.conns.get_mut(&conn).unwrap();
        let slot = if to_node { &mut c.to_node_at } else { &mut c.to_client_at };
        let at = (self.now + delay).max(*slot);
        *slot = at;
        let event = if to_node { Event::ToNode { conn, frame } } else { Event::ToClient { conn, frame } };
        self.schedule(at - self.now, event);
    }

    fn deliver_to_node(&mut self, conn: ConnId, frame: &[u8]) {
        let Conn { up, node: id, .. } = self.conns[&conn];
        if !up {
            return;
        }
        let node = self.nodes.get_mut(&id).unwrap().node.as_mut().expect("a crash resets its connections");
        node.on_client(conn, &codec::decode_frame(frame).unwrap().unwrap().0, self.now);
        self.flush(id);
    }

    fn deliver_to_client(&mut self, conn: ConnId, frame: &[u8]) {
        let Conn { up, client: c, .. } = self.conns[&conn];
        if !up {
            return;
        }
        if c == ADMIN {
            let (message, _) = codec::decode_frame(frame).unwrap().unwrap();
            self.admin_answers += u64::from(matches!(message, turbofan_cluster::wire::Decoded::SnapshotTaken(_)));
            return;
        }
        self.clients[c].client.on_frame(&codec::decode_frame(frame).unwrap().unwrap().0);
        self.flush_client(c);
    }

    fn dial(&mut self, c: usize) {
        self.clients[c].dialing = false;
        let target: NodeId = self.clients[c].client.target()[1..].parse().unwrap();
        if self.nodes[&target].node.is_none() {
            self.clients[c].client.on_disconnected();
            self.dial_later(c);
            return;
        }
        let conn = self.next_conn;
        self.next_conn += 1;
        self.conns.insert(conn, Conn { client: c, node: target, up: true, to_node_at: 0, to_client_at: 0 });
        self.clients[c].conn = Some(conn);
        self.clients[c].client.on_connected(self.now);
        self.flush_client(c);
    }

    fn dial_later(&mut self, c: usize) {
        if !self.clients[c].dialing {
            self.clients[c].dialing = true;
            // Sometimes straight back, while what it sent is still uncommitted: it resends that,
            // and the log gets it twice.
            let after = if self.rng.one_in(3) { self.rng.below(MS) } else { 5 * MS + self.rng.below(40 * MS) };
            self.schedule(after, Event::Dial(c));
        }
    }

    /// Both ends of a client connection see it drop.
    fn reset(&mut self, conn: ConnId) {
        let c = self.conns.get_mut(&conn).unwrap();
        if !c.up {
            return;
        }
        c.up = false;
        let (client, id) = (c.client, c.node);
        if client == ADMIN {
            return;
        }
        if let Some(node) = self.nodes.get_mut(&id).unwrap().node.as_mut() {
            node.on_disconnect(conn);
        }
        let sim_client = &mut self.clients[client];
        if sim_client.conn == Some(conn) {
            sim_client.conn = None;
            sim_client.client.on_disconnected();
            self.dial_later(client);
        }
    }

    fn client_send(&mut self, c: usize) {
        if self.now > FAULT_PHASE + QUIET_PHASE / 2 {
            return;
        }
        let k = self.clients[c].sent + 1;
        match self.clients[c].client.send(k.to_string().as_bytes()) {
            Ok(_) => self.clients[c].sent = k,
            Err(client::SendError::Full) => {}
            Err(error) => panic!("seed {}: client {c} can't send: {error}", self.seed),
        }
        self.flush_client(c);
        let after = MS + self.rng.below(15 * MS);
        self.schedule(after, Event::ClientSend(c));
    }

    /// Carries out a client's outputs and checks what it was told.
    fn flush_client(&mut self, c: usize) {
        let frames = self.clients[c].client.take_frames();
        if let Some(conn) = self.clients[c].conn {
            for frame in frames {
                self.send_conn(conn, frame, true);
            }
            if self.clients[c].client.take_reconnect().is_some() {
                self.reset_redirected(c, conn);
            }
        }
        while let Some(event) = self.clients[c].client.poll_event() {
            self.check_client_event(c, event);
        }
        let deadline = self.clients[c].client.next_deadline().max(self.now + 1);
        if self.clients[c].tick_at.is_none_or(|at| deadline < at) {
            self.clients[c].tick_at = Some(deadline);
            self.schedule(deadline - self.now, Event::ClientTick(c));
        }
    }

    /// A redirected client drops its connection, already pointed at the new target.
    fn reset_redirected(&mut self, c: usize, conn: ConnId) {
        let state = self.conns.get_mut(&conn).unwrap();
        state.up = false;
        let id = state.node;
        if let Some(node) = self.nodes.get_mut(&id).unwrap().node.as_mut() {
            node.on_disconnect(conn);
        }
        self.clients[c].conn = None;
        self.dial_later(c);
    }

    fn check_client_event(&mut self, c: usize, event: client::Event) {
        let seed = self.seed;
        let sim_client = &mut self.clients[c];
        match event {
            client::Event::Opened(_) => {}
            client::Event::Message(payload) => {
                let text = String::from_utf8(payload).unwrap();
                let k: u64 = text[1..].parse().unwrap();
                if text.starts_with('a') {
                    assert_eq!(
                        k,
                        sim_client.answered + 1,
                        "seed {seed}: client {c} got {text} after a{}",
                        sim_client.answered
                    );
                    sim_client.answered = k;
                } else {
                    assert!(sim_client.timers.insert(k), "seed {seed}: client {c} got {text} twice");
                }
            }
            event => panic!("seed {seed}: client {c}: {event:?}"),
        }
    }

    /// Committed entries are the same on every node, and durable on a majority.
    fn check_committed(&mut self, id: NodeId) {
        loop {
            let sim_node = &self.nodes[&id];
            let raft = sim_node.node.as_ref().unwrap().raft();
            assert!(sim_node.checked <= raft.commit_index(), "seed {}: node {id}'s commit index went back", self.seed);
            if sim_node.checked == raft.commit_index() {
                return;
            }
            let index = sim_node.checked + 1;
            let Some(payload) = raft.entry(index).or_else(|| sim_node.disk.record(index)).map(<[u8]>::to_vec) else {
                // Restarted from, or sent, a snapshot: what it covers is checked by its bytes.
                assert!(index <= raft.snapshot_index(), "seed {}: node {id} lacks committed {index}", self.seed);
                self.nodes.get_mut(&id).unwrap().checked = raft.snapshot_index();
                continue;
            };
            let i = usize::try_from(index - 1).unwrap();
            if let Some(first) = self.committed.get(i) {
                assert_eq!(first, &payload, "seed {}: node {id} committed a different entry at {index}", self.seed);
            } else {
                assert_eq!(i, self.committed.len());
                let durable = self.nodes.values().filter(|n| n.disk.has(index, &payload)).count();
                assert!(
                    durable > self.members.len() / 2,
                    "seed {}: entry {index} committed but durable on only {durable} nodes",
                    self.seed
                );
                self.committed.push(payload);
                self.commit_terms.push(raft.term());
            }
            self.nodes.get_mut(&id).unwrap().checked = index;
        }
    }

    fn crash(&mut self, id: NodeId) {
        let sim_node = self.nodes.get_mut(&id).unwrap();
        if sim_node.node.take().is_none() {
            return;
        }
        self.crashes += 1;
        sim_node.incarnation += 1;
        // Sometimes straight back, in the same election.
        let after = if self.rng.one_in(3) { MS + self.rng.below(20 * MS) } else { 50 * MS + self.rng.below(2000 * MS) };
        self.schedule(after, Event::Restart(id));
        let conns: Vec<ConnId> = self.conns.iter().filter(|(_, c)| c.up && c.node == id).map(|(&k, _)| k).collect();
        for conn in conns {
            self.reset(conn);
        }
    }

    /// Power lost partway through a batch: some prefix of its operations reaches the disk, the
    /// last perhaps only some of its records.
    fn crash_while_persisting(&mut self, id: NodeId, ops: &[Persist]) {
        let whole = usize::try_from(self.rng.below(ops.len() as u64)).unwrap();
        let disk = &mut self.nodes.get_mut(&id).unwrap().disk;
        let keep = self.rng.one_in(2);
        for op in &ops[..whole] {
            disk.apply(op, keep);
        }
        if let Persist::Append(records) = &ops[whole] {
            let all: Vec<Record<'_>> = codec::records(records).map(Result::unwrap).collect();
            let some = usize::try_from(self.rng.below(all.len() as u64)).unwrap();
            for record in &all[..some] {
                disk.append(*record);
            }
        }
        self.crash(id);
    }

    fn start(&mut self, id: NodeId) {
        let sim_node = self.nodes.get_mut(&id).unwrap();
        if sim_node.node.is_some() {
            return;
        }
        let config = node::Config {
            raft: Config {
                id,
                members: self.members.clone(),
                election_timeout_ns: 150 * MS,
                heartbeat_ns: 50 * MS,
                seed: self.seed ^ (u64::from(id) << 32) ^ sim_node.incarnation,
            },
            client_addrs: self.members.iter().map(|&m| (m, format!("n{m}"))).collect(),
            admin_addrs: Vec::new(),
            session_timeout_ns: 10_000 * MS,
            snapshot_every_entries: 300,
            egress_ring_entries: 1024,
        };
        let log = sim_node.disk.log.concat();
        let snapshot = sim_node.disk.snapshot.clone();
        sim_node.node = Some(Node::new(config, Echo::default(), sim_node.disk.meta, snapshot, &log, self.now));
        sim_node.checked = 0;
        sim_node.tick_at = None;
        self.flush(id);
    }

    fn end_faults(&mut self) {
        self.faults = false;
        self.cut = [[false; 8]; 8];
        for id in self.members.clone() {
            self.start(id);
        }
    }

    fn schedule(&mut self, after: u64, event: Event) {
        let seq = self.next_event;
        self.next_event += 1;
        self.events.insert(seq, event);
        self.queue.push((Reverse(self.now + after), Reverse(seq)));
    }

    /// Election safety: at most one leader per term, ever. Leader completeness: a new leader's
    /// log holds every entry committed in an earlier term. (Not one committed in a later term: a
    /// vote delivered late can elect a leader of a term the cluster has moved past.)
    fn check(&mut self) {
        for (&id, sim_node) in &self.nodes {
            let Some(node) = &sim_node.node else { continue };
            let raft = node.raft();
            if raft.role() == Role::Leader {
                let new = !self.leaders.contains_key(&raft.term());
                let leader = *self.leaders.entry(raft.term()).or_insert(id);
                assert_eq!(leader, id, "seed {}: two leaders in term {}", self.seed, raft.term());
                // Only for a new leader: this walks the whole log.
                let committed =
                    self.committed.iter().zip(&self.commit_terms).enumerate().take(if new { usize::MAX } else { 0 });
                for (i, (payload, _)) in committed.filter(|&(_, (_, &t))| new && t < raft.term()) {
                    let index = i as u64 + 1;
                    if index <= raft.snapshot_index() {
                        continue;
                    }
                    assert_eq!(
                        raft.entry(index),
                        Some(payload.as_slice()),
                        "seed {}: leader {id} of term {} lacks committed entry {index}",
                        self.seed,
                        raft.term()
                    );
                }
            }
            assert!(raft.commit_index() <= raft.last_index());
        }
    }

    /// After the faults stop: one leader, the same log and service state everywhere, and every
    /// client's messages answered and timers fired, once each.
    fn check_end(&self) {
        let seed = self.seed;
        let nodes: Vec<&Node<Echo>> = self.nodes.values().map(|n| n.node.as_ref().expect("all up")).collect();
        let leaders = nodes.iter().filter(|n| n.raft().role() == Role::Leader).count();
        assert_eq!(leaders, 1, "seed {seed}: {leaders} leaders after the faults stopped");
        let commit = nodes[0].raft().commit_index();
        for node in &nodes {
            let (id, role, term, last) =
                (node.raft().id(), node.raft().role(), node.raft().term(), node.raft().last_index());
            let got = (node.raft().commit_index(), node.applied());
            assert_eq!(
                got,
                (commit, commit),
                "seed {seed}: node {id} ({role:?}, term {term}, last {last}) not caught up"
            );
            assert_eq!(node.service(), nodes[0].service(), "seed {seed}: services differ");
            for index in node.raft().snapshot_index() + 1..=commit {
                assert_eq!(
                    node.raft().entry(index),
                    Some(self.committed[usize::try_from(index - 1).unwrap()].as_slice())
                );
            }
        }
        for (c, sim_client) in self.clients.iter().enumerate() {
            assert!(sim_client.sent > 100, "seed {seed}: client {c} sent only {}", sim_client.sent);
            assert_eq!(sim_client.answered, sim_client.sent, "seed {seed}: client {c} unanswered");
            assert_eq!(sim_client.client.unacked(), 0);
            let expected: BTreeSet<u64> = (1..=sim_client.sent).filter(|k| k.is_multiple_of(TIMER_EVERY)).collect();
            assert_eq!(sim_client.timers, expected, "seed {seed}: client {c} timers");
        }
        assert!(self.admin_answers > 0, "seed {seed}: no manual snapshot was answered of {}", self.admin_requests);
        let sent: u64 = self.clients.iter().map(|c| c.sent).sum();
        eprintln!(
            "seed {seed}: {} members, {} terms, {} crashes, {commit} committed, {sent} messages, {} snapshots, {} installed, {}/{} manual",
            nodes.len(),
            self.leaders.len(),
            self.crashes,
            self.snapshots.len(),
            self.installs,
            self.admin_answers,
            self.admin_requests
        );
    }
}
