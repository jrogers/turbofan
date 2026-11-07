use super::*;

const MS: u64 = 1_000_000;
const TIMEOUT: u64 = 1000 * MS;

/// Logs every callback. A message `at <id> <ms>` schedules a timer and `cancel <id>`
/// cancels one, `propose <what>` proposes it, and `close` closes the session; any but `quiet`
/// and `close` is echoed.
#[derive(Debug, Default)]
struct Recorder(Vec<String>);

impl Service for Recorder {
    /// Refuses `bad`; anything else is its own principal.
    fn authenticate(&self, credentials: &[u8]) -> Option<Vec<u8>> {
        (credentials != b"bad").then(|| credentials.to_vec())
    }
    fn on_session_open(&mut self, _ctx: &mut Ctx, session: SessionId, principal: &[u8]) {
        self.0.push(format!("open {session} {}", String::from_utf8_lossy(principal)));
    }
    fn on_session_close(&mut self, _ctx: &mut Ctx, session: SessionId, reason: CloseReason) {
        self.0.push(format!("close {session} {reason:?}"));
    }
    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        let text = std::str::from_utf8(payload).unwrap();
        self.0.push(format!("message {session} {text}"));
        if ctx.service_version() > 0 {
            self.0.push(format!("as version {}", ctx.service_version()));
        }
        match text.split(' ').collect::<Vec<_>>()[..] {
            ["at", id, ms] => ctx.schedule_timer(id.parse().unwrap(), ms.parse::<u64>().unwrap() * MS),
            ["cancel", id] => ctx.cancel_timer(id.parse().unwrap()),
            ["propose", what] => ctx.propose(what.as_bytes()).expect("a client payload fits"),
            ["quiet"] => return,
            ["close"] => return ctx.close_session(session),
            _ => {}
        }
        ctx.send(session, payload).expect("a client payload fits");
    }
    fn on_proposal(&mut self, _ctx: &mut Ctx, payload: &[u8]) {
        self.0.push(format!("proposal {}", std::str::from_utf8(payload).unwrap()));
    }
    fn version(&self) -> u32 {
        1
    }
    fn on_activate(&mut self, _ctx: &mut Ctx, version: u32) {
        self.0.push(format!("activate {version}"));
    }
    fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
        self.0.push(format!("timer {id} at {}", ctx.now() / MS));
    }
    fn snapshot(&self) -> SnapshotWriter {
        let text = self.0.join("\n");
        Box::new(move |out| out.write_all(text.as_bytes()))
    }
    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut text = String::new();
        from.read_to_string(&mut text)?;
        self.0 = if text.is_empty() { Vec::new() } else { text.split('\n').map(String::from).collect() };
        Ok(())
    }
}

fn member(id: u8) -> Member {
    Member { id: NodeId(id), peer: format!("p{id}"), client: format!("n{id}"), admin: format!("a{id}"), learner: false }
}

/// A payload over the limit is refused, not sent or proposed, and not a panic.
#[test]
fn ctx_refuses_a_payload_over_the_limit() {
    let mut ctx = Ctx::new(0);
    let long = vec![0_u8; MAX_PAYLOAD_BYTES + 1];
    assert_eq!(ctx.send(SessionId(1), long.clone()), Err(PayloadTooLong { len: MAX_PAYLOAD_BYTES + 1 }));
    assert_eq!(ctx.propose(long.clone()), Err(PayloadTooLong { len: MAX_PAYLOAD_BYTES + 1 }));
    assert_eq!(ctx.send(SessionId(1), &long[1..]), Ok(()));
    assert_eq!(ctx.take_effects(), [Effect::Send(SessionId(1), long[1..].to_vec())]);
}

/// A lone member, elected and ready.
fn leader() -> Node<Recorder> {
    let config = Config {
        raft: raft::Config {
            id: NodeId(1),
            members: vec![member(1)],
            election_timeout: Duration::from_millis(150),
            heartbeat: Duration::from_millis(50),
            seed: 1,
            log_window_bytes: usize::MAX,
            log_tail_entries: 0,
            max_uncommitted_bytes: usize::MAX,
        },
        session_timeout: Duration::from_nanos(TIMEOUT),
        snapshot_every_entries: 1000,
        snapshots_kept: 1,
        egress_ring_entries: 1024,
        protocol: PROTOCOL,
    };
    let mut node = Node::new(config, Recorder::default(), Meta::default(), None, Entries::default(), 0);
    let deadline = node.next_deadline();
    node.on_tick(deadline);
    drive(&mut node);
    assert!(node.is_ready());
    node
}

/// Carries out the outputs as a driver would, a snapshot saved at once, returning the client
/// frames, decoded to text.
fn drive(node: &mut Node<Recorder>) -> Vec<String> {
    let mut sends = Vec::new();
    loop {
        let out = node.take_outputs();
        sends.extend(out.client_sends);
        if let Some(save) = out.save {
            let (index, term, members) = (save.index, save.term, save.members.clone());
            let len = crate::widen(written(save).len());
            node.on_snapshot_saved(Snapshot { index, term, len, checksum: 0, members });
            continue;
        }
        if out.persist.is_empty() {
            break;
        }
        node.on_persisted(node.raft().log_end(), node.now);
    }
    sends.iter().map(|(conn, f)| format!("{conn}: {:?}", codec::decode_frame(f).unwrap().unwrap().0)).collect()
}

/// What a snapshot's writer writes.
fn written(save: SaveSnapshot) -> Vec<u8> {
    let mut bytes = Vec::new();
    (save.writer)(&mut bytes).unwrap();
    bytes
}

fn client(node: &mut Node<Recorder>, conn: ConnId, message: &impl turbofan_sbe::Encode, now: u64) -> Vec<String> {
    let mut frame = Vec::new();
    codec::encode_frame(message, &mut frame).unwrap();
    node.on_client(conn, &codec::decode_frame(&frame).unwrap().unwrap().0, now);
    drive(node)
}

fn open(node: &mut Node<Recorder>, conn: ConnId, now: u64) -> SessionId {
    let sent = client(node, conn, &wire::Connect { credentials: b"" }, now);
    assert!(sent[0].contains("Opened"), "{sent:?}");
    node.local.attached.iter().find(|&(_, &c)| c == conn).map(|(&s, _)| s).unwrap()
}

#[test]
fn messages_apply_once_in_order_and_a_reattach_replays_responses() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let message = |client_seq, payload| wire::Message { session: session.0, client_seq, payload };
    assert!(client(&mut node, 7, &message(1, b"one"), 0)[0].contains("Egress"));
    // A resend of 1 is acknowledged, not applied; 3 before 2 is dropped for the client to resend.
    assert!(
        client(&mut node, 7, &message(1, b"one"), 0)[0].contains("KeepAliveAck(KeepAliveAckRef { applied_seq: 1 })")
    );
    assert!(client(&mut node, 7, &message(3, b"three"), 0).is_empty());
    client(&mut node, 7, &message(2, b"two"), 0);
    let messages: Vec<&String> = node.service.0.iter().filter(|e| e.starts_with("message")).collect();
    assert_eq!(messages, [&format!("message {session} one"), &format!("message {session} two")]);

    // After a failover the client reattaches on a new connection, having seen response 1.
    node.on_disconnect(7);
    let sent =
        client(&mut node, 8, &wire::Reattach { session: session.0, last_egress_seq: 1, epoch: 0, credentials: b"" }, 0);
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert!(sent[0].contains("Opened") && sent[0].contains("applied_seq: 2"), "{sent:?}");
    // The last response it sent, which a client that saw more knows the cluster lost.
    assert!(sent[0].contains("last_egress_seq: Some(2)"), "{sent:?}");
    assert!(sent[1].contains("egress_seq: 2"));
    // The old connection is no longer the session's.
    assert!(client(&mut node, 7, &message(3, b"three"), 0)[0].contains("NoSession"));
}

/// A ring longer than this node keeps (a snapshot from a node that kept more, or the setting
/// lowered) is cut to it at the next response, not left to grow.
#[test]
fn a_ring_over_the_limit_is_cut_to_it() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let s = node.state.sessions.get_mut(&session).unwrap();
    s.ring = (1..=2000).map(|seq| (seq, b"old".to_vec())).collect();
    s.next_egress = 2001;
    node.egress(session, b"new".to_vec());
    let ring = &node.state.sessions[&session].ring;
    assert_eq!((ring.len(), ring.back().map(|(seq, _)| *seq)), (1024, Some(2001)));
}

/// A message the service doesn't answer is acknowledged as soon as it's applied, not at the
/// client's next keepalive; one it answers needs nothing but the `Egress`.
#[test]
fn an_applied_message_is_acknowledged_with_or_without_a_response() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let message = |client_seq, payload| wire::Message { session: session.0, client_seq, payload };
    let sent = client(&mut node, 7, &message(1, b"quiet"), 0);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert!(sent[0].contains("KeepAliveAck(KeepAliveAckRef { applied_seq: 1 })"), "{sent:?}");
    let sent = client(&mut node, 7, &message(2, b"two"), 0);
    assert_eq!(sent.len(), 1, "{sent:?}");
    assert!(sent[0].contains("Egress") && sent[0].contains("applied_seq: 2"), "{sent:?}");
}

/// A resend of a message proposed but not yet applied, as a client reattaching to the same
/// leader sends, isn't logged again; neither is one past the client's window.
#[test]
fn a_session_proposes_each_message_once_within_its_window() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let submit = |node: &mut Node<Recorder>, client_seq: u64| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Message { session: session.0, client_seq, payload: b"m" }, &mut frame).unwrap();
        let before = node.raft().last_index();
        node.on_client(7, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        node.raft().last_index() - before
    };
    assert_eq!(submit(&mut node, 1), 1);
    assert_eq!(submit(&mut node, 1), 0, "a resend of what's in flight");
    for client_seq in 2..=crate::widen(MAX_UNACKED) {
        assert_eq!(submit(&mut node, client_seq), 1);
    }
    assert_eq!(submit(&mut node, crate::widen(MAX_UNACKED) + 1), 0, "past the window");
    drive(&mut node);
    assert_eq!(node.service.0.iter().filter(|e| e.starts_with("message")).count(), MAX_UNACKED);
    // Once they apply, the window moves on.
    assert_eq!(submit(&mut node, crate::widen(MAX_UNACKED) + 1), 1);
}

/// A service closes a session: the client is told why, and the service called back.
#[test]
fn the_service_closes_a_session() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let sent = client(&mut node, 7, &wire::Message { session: session.0, client_seq: 1, payload: b"close" }, 0);
    assert!(sent.iter().any(|s| s.contains("Closed") && s.contains("Service")), "{sent:?}");
    assert!(node.service.0.contains(&format!("close {session} Service")));
    let reattach = wire::Reattach { session: session.0, last_egress_seq: 0, epoch: 0, credentials: b"" };
    assert!(client(&mut node, 8, &reattach, 0)[0].contains("NoSession"), "gone");
}

/// Sessions open up to `MAX_SESSIONS`; past it, the next is refused, until one closes.
#[test]
fn sessions_open_up_to_the_limit() {
    let mut node = leader();
    let first = open(&mut node, 0, 0);
    for conn in 1..crate::widen(MAX_SESSIONS) {
        open(&mut node, conn, 0);
    }
    let past = crate::widen(MAX_SESSIONS);
    assert!(client(&mut node, past, &wire::Connect { credentials: b"" }, 0)[0].contains("NoSession"));
    client(&mut node, 0, &wire::Close { session: first.0 }, 0);
    assert!(client(&mut node, past, &wire::Connect { credentials: b"" }, 0)[0].contains("Opened"));
}

/// A snapshot's counts are refused past their limits, and read up to them: what's read at the
/// limit runs out of messages instead, which is a different error.
#[test]
fn a_snapshots_counts_are_checked_at_their_limits() {
    let node_state = |sessions: usize| {
        let sessions = u32::try_from(sessions).unwrap();
        let node = wire::NodeState {
            applied: 1,
            time: 0,
            next_proposal: 0,
            sessions,
            timers: 0,
            proposals: 0,
            protocol: 0,
            service_version: 0,
            epoch: 1,
            epoch_since: 0,
            egress_ring: 0,
        };
        let mut bytes = Vec::new();
        codec::encode_frame(&node, &mut bytes).unwrap();
        bytes
    };
    let error = |bytes: Vec<u8>| decode_state(&mut &bytes[..]).unwrap_err().to_string();
    assert!(error(node_state(MAX_SESSIONS + 1)).contains("too many sessions"));
    assert!(!error(node_state(MAX_SESSIONS)).contains("too many sessions"));

    let ring = |ring: usize| {
        let mut bytes = node_state(1);
        let ring = u32::try_from(ring).unwrap();
        let (carried_seq, carried_egress) = (u64::MAX, u64::MAX);
        let session = wire::SessionState {
            session: 1,
            last_seq: 0,
            next_egress: 1,
            ring,
            carried_seq,
            carried_egress,
            principal: b"",
        };
        codec::encode_frame(&session, &mut bytes).unwrap();
        bytes
    };
    assert!(error(ring(MAX_EGRESS_RING + 1)).contains("too long a ring"));
    assert!(!error(ring(MAX_EGRESS_RING)).contains("too long a ring"));

    let frame_of = |len: usize| {
        let mut bytes = u32::try_from(len).unwrap().to_le_bytes().to_vec();
        bytes.resize(4 + 16, 0);
        bytes
    };
    assert!(error(frame_of(codec::MAX_FRAME_BYTES + 1)).contains("over MAX_FRAME_BYTES"));
    assert!(!error(frame_of(codec::MAX_FRAME_BYTES)).contains("over MAX_FRAME_BYTES"));
}

/// A client the service refuses gets no session; one it accepts opens one as its principal,
/// and only that principal may reattach to it.
#[test]
fn credentials_open_a_session_and_reattach_to_it() {
    let mut node = leader();
    let sent = client(&mut node, 1, &wire::Connect { credentials: b"bad" }, 0);
    assert!(sent[0].contains("Refused"), "{sent:?}");
    client(&mut node, 2, &wire::Connect { credentials: b"alice" }, 0);
    let session = node.local.attached.iter().find(|&(_, &c)| c == 2).map(|(&s, _)| s).unwrap();
    assert!(node.service.0.contains(&format!("open {session} alice")));

    let reattach = |credentials| wire::Reattach { session: session.0, last_egress_seq: 0, epoch: 0, credentials };
    assert!(client(&mut node, 3, &reattach(b"mallory"), 0)[0].contains("Refused"), "a guessed session");
    assert!(client(&mut node, 4, &reattach(b"alice"), 0)[0].contains("Opened"));
}

#[test]
fn a_silent_session_times_out_and_a_keepalive_holds_it_open() {
    let mut node = leader();
    let quiet = open(&mut node, 1, 0);
    let kept = open(&mut node, 2, 0);
    client(&mut node, 2, &wire::KeepAlive { session: kept.0 }, TIMEOUT / 2);
    assert!(node.next_deadline() <= TIMEOUT);
    node.on_tick(TIMEOUT);
    let sent = drive(&mut node);
    assert!(sent.iter().any(|s| s.starts_with("1: ") && s.contains("Timeout")), "{sent:?}");
    assert!(node.service.0.contains(&format!("close {quiet} Timeout")));
    assert!(node.state.sessions.contains_key(&kept));
}

#[test]
fn a_rescheduled_timer_fires_once_at_its_new_time_and_a_cancelled_one_never() {
    let mut node = leader();
    let session = open(&mut node, 1, 0);
    for (client_seq, text) in [(1, "at 5 100"), (2, "at 6 100"), (3, "at 5 300"), (4, "cancel 6")] {
        client(&mut node, 1, &wire::Message { session: session.0, client_seq, payload: text.as_bytes() }, 0);
    }
    // A keepalive stops the session timing out over the wait.
    for now in [100, 200, 300, 400].map(|ms| ms * MS) {
        client(&mut node, 1, &wire::KeepAlive { session: session.0 }, now);
        node.on_tick(now);
        drive(&mut node);
    }
    let timers: Vec<&String> = node.service.0.iter().filter(|e| e.starts_with("timer")).collect();
    assert_eq!(timers, ["timer 5 at 300"]);
}

#[test]
fn a_follower_redirects_clients_to_the_leader() {
    let config = Config {
        raft: raft::Config {
            id: NodeId(2),
            members: vec![member(1), member(2), member(3)],
            election_timeout: Duration::from_millis(150),
            heartbeat: Duration::from_millis(50),
            seed: 1,
            log_window_bytes: usize::MAX,
            log_tail_entries: 0,
            max_uncommitted_bytes: usize::MAX,
        },
        session_timeout: Duration::from_nanos(TIMEOUT),
        snapshot_every_entries: 1000,
        snapshots_kept: 1,
        egress_ring_entries: 1024,
        protocol: PROTOCOL,
    };
    let mut node = Node::new(config, Recorder::default(), Meta::default(), None, Entries::default(), 0);
    let mut frame = Vec::new();
    let append = wire::AppendEntries {
        term: 1,
        leader: 1,
        prev_index: 0,
        prev_term: 0,
        commit: 0,
        count: 0,
        entries: &[],
        last_index: 0,
    };
    codec::encode_frame(&append, &mut frame).unwrap();
    node.on_peer(NodeId(1), &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
    drive(&mut node);
    let sent = client(&mut node, 1, &wire::Connect { credentials: b"" }, 0);
    assert!(sent[0].contains("leader: 1, addr: [110, 49]"), "{sent:?}");
}

/// What a node had to send its clients still goes when it becomes leader, or stops leading,
/// before the driver takes it: a redirect from before its election, and an ack of a keepalive
/// before a leader of a later term was heard.
#[test]
fn sends_queued_across_a_change_of_leader_go() {
    let mut node = leader();
    let session = open(&mut node, 1, 0);
    let mut keepalive = Vec::new();
    codec::encode_frame(&wire::KeepAlive { session: session.0 }, &mut keepalive).unwrap();
    node.on_client(1, &codec::decode_frame(&keepalive).unwrap().unwrap().0, 1);
    let mut frame = Vec::new();
    let append = wire::AppendEntries {
        term: 2,
        leader: 2,
        prev_index: 0,
        prev_term: 0,
        commit: 0,
        count: 0,
        entries: &[],
        last_index: 0,
    };
    codec::encode_frame(&append, &mut frame).unwrap();
    node.on_peer(NodeId(2), &codec::decode_frame(&frame).unwrap().unwrap().0, 2);
    assert_ne!(node.raft().role(), Role::Leader);
    let sent = drive(&mut node);
    assert!(sent[0].contains("KeepAliveAck"), "{sent:?}");
    assert!(sent[1].contains("Redirect"), "{sent:?}");

    // A node not yet elected redirects a client, then is elected before that's taken.
    let mut node = Node::new(leader().config, Recorder::default(), Meta::default(), None, Entries::default(), 0);
    let mut connect = Vec::new();
    codec::encode_frame(&wire::Connect { credentials: b"" }, &mut connect).unwrap();
    node.on_client(1, &codec::decode_frame(&connect).unwrap().unwrap().0, 0);
    let deadline = node.next_deadline();
    node.on_tick(deadline);
    let sent = drive(&mut node);
    assert!(sent.first().is_some_and(|s| s.contains("Redirect")), "{sent:?}");
    assert!(node.is_ready());
}

/// A client whose session is still being opened when the leader steps down is redirected, as
/// one attached is: otherwise it waits on a session that never opens, until its keepalives go
/// unanswered.
#[test]
fn a_connection_opening_a_session_is_redirected_when_the_leader_steps_down() {
    let mut node = leader();
    let mut connect = Vec::new();
    codec::encode_frame(&wire::Connect { credentials: b"" }, &mut connect).unwrap();
    node.on_client(1, &codec::decode_frame(&connect).unwrap().unwrap().0, 1);
    let mut frame = Vec::new();
    let append = wire::AppendEntries {
        term: 2,
        leader: 2,
        prev_index: 0,
        prev_term: 0,
        commit: 0,
        count: 0,
        entries: &[],
        last_index: 0,
    };
    codec::encode_frame(&append, &mut frame).unwrap();
    node.on_peer(NodeId(2), &codec::decode_frame(&frame).unwrap().unwrap().0, 2);
    let sent = drive(&mut node);
    assert!(sent.iter().any(|s| s.starts_with("1: ") && s.contains("Redirect")), "{sent:?}");
    assert!(!sent.iter().any(|s| s.contains("Opened")), "{sent:?}");
}

/// An operator switches the cluster to versions every member has, at a point in the log;
/// not to ones a member lacks, or back.
#[test]
fn versions_activate_once_every_member_has_them() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let activate = |node: &mut Node<Recorder>, protocol, service_version| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Activate { request_id: 1, protocol, service_version }, &mut frame).unwrap();
        // Its leader silent an election timeout since it started.
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 1_000_000_000);
        drive(node).join(" ")
    };
    let refused = activate(&mut node, PROTOCOL + 1, 0);
    assert!(
        refused.contains(&format!("index: 0, lowest_protocol: {PROTOCOL}, lowest_service_version: 1")),
        "{refused}"
    );
    assert!(!activate(&mut node, 0, 1).contains("index: 0"), "this node has version 1");
    assert!(node.service.0.contains(&"activate 1".to_owned()));
    client(&mut node, 7, &wire::Message { session: session.0, client_seq: 1, payload: b"later" }, 0);
    assert!(node.service.0.contains(&"as version 1".to_owned()));
    assert!(activate(&mut node, 0, 0).contains("index: 0"), "not back");
    let mut frame = Vec::new();
    codec::encode_frame(&wire::StatusRequest { request_id: 2 }, &mut frame).unwrap();
    node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 1_000_000_000);
    let status = drive(&mut node).join(" ");
    assert!(status.contains("protocol: Some(0), service_version: Some(1)"), "status shows them: {status}");

    let bytes = encode_state(&node.state);
    let state = decode_state(&mut &bytes[..]).unwrap();
    assert_eq!((state.protocol, state.service_version), (0, 1), "kept in snapshots");
}

/// Each node keeps its own number of responses a session, until an activation of protocol 1 or
/// later logs the leader's for all; one of 0 leaves it. Snapshots keep it.
#[test]
fn an_activation_logs_the_egress_ring_for_every_node() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let activate = |node: &mut Node<Recorder>, protocol| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Activate { request_id: 1, protocol, service_version: 0 }, &mut frame).unwrap();
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 1_000_000_000);
        drive(node);
    };
    let ring = |node: &mut Node<Recorder>| {
        for k in 1..=3 {
            let payload = format!("{k}");
            let message = wire::Message {
                session: session.0,
                client_seq: node.state.sessions[&session].last_seq + 1,
                payload: payload.as_bytes(),
            };
            client(node, 7, &message, 0);
        }
        node.state.sessions[&session].ring.len()
    };
    node.config.egress_ring_entries = 2;
    activate(&mut node, 0);
    assert_eq!((node.state.egress_ring, ring(&mut node)), (0, 2), "its own");
    activate(&mut node, 1);
    assert_eq!(node.state.egress_ring, 2, "the leader's");
    node.config.egress_ring_entries = 5;
    assert_eq!(ring(&mut node), 2, "the cluster's, not its own");

    let bytes = encode_state(&node.state);
    assert_eq!(decode_state(&mut &bytes[..]).unwrap().egress_ring, 2, "kept in snapshots");
}

/// A new cluster's first leader names its epoch; one named later, by a leader that hadn't
/// applied that, doesn't count. Snapshots keep it.
#[test]
fn the_first_leader_names_the_epoch() {
    let mut node = leader();
    let epoch = node.epoch();
    assert_ne!(epoch, 0);
    node.propose(&wire::Epoch { timestamp: 1, epoch: epoch + 1, promoted: 0 });
    drive(&mut node);
    assert_eq!(node.epoch(), epoch, "only the first counts");

    let bytes = encode_state(&node.state);
    assert_eq!(decode_state(&mut &bytes[..]).unwrap().epoch, epoch, "kept in snapshots");
    assert_ne!(mint_epoch(5, NodeId(1)), mint_epoch(5, NodeId(2)), "two nodes at once");
    assert_ne!(mint_epoch(0, NodeId(0)), 0);
}

/// A cluster promoted or restored logs on from its own last index, so it may give a session the
/// ID of one the old cluster opened after: a client of that one, naming the old epoch, isn't let
/// into it. A session from before the new epoch began was carried over, and is; and a client
/// naming no epoch (an older one) is taken as before.
#[test]
fn a_session_of_another_epoch_is_not_reattached_to() {
    let mut node = leader();
    let old = node.epoch();
    let carried = open(&mut node, 1, 0);
    node.propose(&wire::Epoch { timestamp: 1, epoch: old + 1, promoted: 1 });
    drive(&mut node);
    assert_eq!(node.epoch(), old + 1);
    assert!(carried.0 < node.state.epoch_since, "it began after");
    let reused = open(&mut node, 2, 0);
    let reattach =
        |session: SessionId, epoch| wire::Reattach { session: session.0, last_egress_seq: 0, epoch, credentials: b"" };
    let sent = client(&mut node, 3, &reattach(reused, old), 0);
    assert!(sent[0].contains("NoSession"), "another epoch's: {sent:?}");
    for epoch in [old + 1, 0] {
        assert!(client(&mut node, 4, &reattach(reused, epoch), 0)[0].contains("Opened"), "epoch {epoch}");
    }
    let sent = client(&mut node, 5, &reattach(carried, old), 0);
    assert!(sent[0].contains(&format!("epoch: Some({})", old + 1)), "carried over, and told the new: {sent:?}");

    let bytes = encode_state(&node.state);
    assert_eq!(decode_state(&mut &bytes[..]).unwrap().epoch_since, node.state.epoch_since, "kept in snapshots");
    let mut rest = &bytes[..];
    let mut restored = with_epoch(&mut rest, 9).unwrap();
    restored.extend_from_slice(rest);
    let restored = decode_state(&mut &restored[..]).unwrap();
    assert_eq!((restored.epoch, restored.epoch_since), (9, node.applied() + 1), "restored, from after the snapshot");
}

/// A client of an earlier epoch is told what its session had as the new epoch began, not what it
/// has since: a promoted standby may lack the lost cluster's last response, and send one of its
/// own since that makes up the number. A client that saw that lost one is told less than it saw,
/// and closes the session as diverged; one that didn't carries on, and is sent the new one. A
/// client of this epoch, or naming none, is told what the session has. The marks are kept in
/// snapshots, and a cluster restored from one marks every session it carries over.
#[test]
fn a_client_of_an_earlier_epoch_is_judged_by_what_its_session_had_as_the_epoch_began() {
    let mut node = leader();
    let old = node.epoch();
    let session = open(&mut node, 1, 0);
    let message = |client_seq, payload| wire::Message { session: session.0, client_seq, payload };
    client(&mut node, 1, &message(1, b"one"), 0);
    client(&mut node, 1, &message(2, b"two"), 0);
    node.propose(&wire::Epoch { timestamp: 1, epoch: old + 1, promoted: 1 });
    drive(&mut node);
    assert_eq!(node.state.sessions[&session].carried, Some((2, 2)), "as it stood");
    // The new epoch's own third response.
    assert!(client(&mut node, 1, &message(3, b"three"), 0).join(" ").contains("egress_seq: 3"));

    let reattach =
        |last_egress_seq, epoch| wire::Reattach { session: session.0, last_egress_seq, epoch, credentials: b"" };
    let opened = |sent: &[String]| sent.iter().find(|s| s.contains("Opened")).cloned().unwrap();
    let sent = client(&mut node, 2, &reattach(3, old), 0);
    assert!(opened(&sent).contains("applied_seq: 2, last_egress_seq: Some(2)"), "it saw a third this lacks: {sent:?}");
    let sent = client(&mut node, 3, &reattach(2, old), 0);
    assert!(opened(&sent).contains("last_egress_seq: Some(2)"), "{sent:?}");
    assert!(sent.iter().any(|s| s.contains("egress_seq: 3")), "and the new third, replayed: {sent:?}");
    for epoch in [old + 1, 0] {
        let sent = client(&mut node, 4, &reattach(3, epoch), 0);
        assert!(opened(&sent).contains("applied_seq: 3, last_egress_seq: Some(3)"), "epoch {epoch}: {sent:?}");
    }

    let bytes = encode_state(&node.state);
    assert_eq!(decode_state(&mut &bytes[..]).unwrap().sessions[&session].carried, Some((2, 2)), "kept");
    let mut rest = &bytes[..];
    let restored = decode_state(&mut &with_epoch(&mut rest, 9).unwrap()[..]).unwrap();
    assert_eq!(restored.sessions[&session].carried, Some((3, 3)), "restored, carried over as it stood");
}

/// A learner promoted leads alone, of a new epoch that replaces its cluster's, and says so
/// once it applies; a new cluster's naming of one doesn't replace it. A voter isn't promoted.
#[test]
fn a_promoted_learner_names_a_new_epoch() {
    let promote = |node: &mut Node<Recorder>| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Promote { request_id: 9 }, &mut frame).unwrap();
        // Its leader silent an election timeout since it started.
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 1_000_000_000);
        drive(node).join(" ")
    };
    assert!(promote(&mut leader()).contains("Promoted(PromotedRef { request_id: 9, index: 0, epoch: 0 })"));

    let mut config = leader().config;
    config.raft.id = NodeId(2);
    config.raft.members = vec![member(1), Member { learner: true, ..member(2) }];
    let mut node = Node::new(config, Recorder::default(), Meta::default(), None, Entries::default(), 0);
    node.state.epoch = 5;
    // Asked first on a connection that then dropped: answered on the one it's asked again on.
    let mut frame = Vec::new();
    codec::encode_frame(&wire::Promote { request_id: 9 }, &mut frame).unwrap();
    node.on_admin(98, &codec::decode_frame(&frame).unwrap().unwrap().0, 1_000_000_000);
    let answer = promote(&mut node);
    assert!(answer.starts_with("99: ") && !answer.contains("index: 0,"), "{answer}");
    assert!(node.is_ready(), "it leads");
    assert_ne!(node.epoch(), 5, "a new epoch");
    let promoted = format!("index: {}, epoch: {}", node.applied() - 1, node.epoch());
    assert!(answer.contains(&promoted), "{answer}");
    let epoch = node.epoch();
    node.propose(&wire::Epoch { timestamp: 1, epoch: 6, promoted: 0 });
    drive(&mut node);
    assert_eq!(node.epoch(), epoch, "a new cluster's naming doesn't replace it");
}

/// What a service proposes comes back through the log, once, even when it's logged twice.
#[test]
fn a_proposal_is_applied_once_through_the_log() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    client(&mut node, 7, &wire::Message { session: session.0, client_seq: 1, payload: b"propose hi" }, 0);
    drive(&mut node);
    // As a leader before a failover would have: the same proposal, logged again.
    node.propose(&wire::ServiceProposal { timestamp: 0, id: 1, payload: b"hi" });
    drive(&mut node);
    let proposals: Vec<&String> = node.service.0.iter().filter(|e| e.starts_with("proposal")).collect();
    assert_eq!(proposals, ["proposal hi"]);
    assert!(node.state.outbox.is_empty());
}

/// A node restarted from a snapshot has the same sessions, rings, timers and service, as of
/// when the snapshot was taken, though it was written after more was applied.
#[test]
fn a_snapshot_restores_the_whole_state_as_it_was_taken() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    for (client_seq, text) in [(1, "one"), (2, "at 9 500")] {
        client(&mut node, 7, &wire::Message { session: session.0, client_seq, payload: text.as_bytes() }, 0);
    }
    node.take_snapshot();
    let save = node.take_outputs().save.unwrap();
    let (index, term, members) = (save.index, save.term, save.members.clone());
    assert_eq!(index, node.applied());
    let (state, service) = (encode_state(&node.state), node.service.0.clone());
    client(&mut node, 7, &wire::Message { session: session.0, client_seq: 3, payload: b"later" }, 0);
    let bytes = written(save);

    let config = node.config.clone();
    let snapshot = Snapshot { index, term, len: crate::widen(bytes.len()), checksum: 0, members };
    let mut restored = Node::new(config, Recorder::default(), Meta::default(), Some(snapshot), Entries::default(), 0);
    restored.restore(&mut &bytes[..]).unwrap();
    assert_eq!(encode_state(&restored.state), state);
    assert_eq!(restored.state.sessions[&session].ring.len(), 2);
    assert_eq!(restored.state.timers.get(&9), Some(&(500 * MS)));
    assert_eq!(restored.service.0, service);
}

/// A snapshot cut short anywhere is refused, never misread or a panic.
#[test]
fn a_truncated_snapshot_is_refused() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    client(&mut node, 7, &wire::Message { session: session.0, client_seq: 1, payload: b"at 3 100" }, 0);
    node.state.outbox.insert(4, b"waiting".to_vec());
    node.state.next_proposal = 4;
    let bytes = encode_state(&node.state);
    assert_eq!(encode_state(&decode_state(&mut &bytes[..]).unwrap()), bytes);
    for cut in 0..bytes.len() {
        assert!(decode_state(&mut &bytes[..cut]).is_err(), "cut at {cut}");
    }
}

/// A leader times its commits, and reports its state.
#[test]
fn a_leader_times_its_commits_and_reports_its_state() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let mut frame = Vec::new();
    codec::encode_frame(&wire::Message { session: session.0, client_seq: 1, payload: b"one" }, &mut frame).unwrap();
    node.on_client(7, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
    node.take_outputs();
    let now = node.next_deadline();
    node.on_tick(now);
    node.on_persisted(node.raft().log_end(), now + MS);
    let mut latencies = node.take_outputs().latencies;
    latencies.sort_unstable();
    let stages = [raft::Stage::Persisted, raft::Stage::Committed, raft::Stage::Applied];
    assert_eq!(latencies, stages.map(|s| (s, now + MS)), "the one message, proposed at 0, each stage once");
    let metrics = node.metrics(now + 5 * MS);
    assert_eq!((metrics.role, metrics.sessions, metrics.members), (Role::Leader, 1, 1));
    assert_eq!((metrics.commit, metrics.applied), (node.raft().last_index(), node.raft().last_index()));
    assert_eq!(metrics.snapshot_age_ns, now + 5 * MS, "no snapshot since it started, at 0");
}

/// Shut down, a node snapshots, answers, and stops after telling its peers to, for two
/// heartbeats.
#[test]
fn a_shutdown_snapshots_then_stops() {
    let mut node = leader();
    let mut frame = Vec::new();
    codec::encode_frame(&wire::Shutdown { request_id: 7 }, &mut frame).unwrap();
    node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
    let sent = drive(&mut node);
    let index = node.raft().snapshot_index();
    assert_eq!(sent, [format!("99: ShutDown(ShutDownRef {{ request_id: 7, index: {index} }})")]);
    assert!(index > 0 && !node.stopped());
    for _ in 0..3 {
        let deadline = node.next_deadline();
        node.on_tick(deadline);
        drive(&mut node);
    }
    assert!(node.stopped());
}

#[test]
fn an_operator_snapshot_is_answered_once_durable() {
    let mut node = leader();
    let mut frame = Vec::new();
    codec::encode_frame(&wire::SnapshotRequest { request_id: 42 }, &mut frame).unwrap();
    node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
    // The Snapshot entry commits and applies, the snapshot is saved, then the answer.
    let sent = drive(&mut node);
    let index = node.raft().snapshot_index();
    assert!(index > 0 && index == node.applied());
    assert_eq!(
        sent,
        [format!("99: SnapshotTaken(SnapshotTakenRef {{ request_id: 42, index: {index}, quorum_acked: 1 }})")]
    );
}

/// An operator's request sent again, its connection dropped while it was under way, is answered
/// once, on the new connection: not ignored as one already under way.
#[test]
fn a_request_sent_again_is_answered_on_the_new_connection() {
    let (mut snapshot, mut shutdown, mut activate) = (Vec::new(), Vec::new(), Vec::new());
    codec::encode_frame(&wire::SnapshotRequest { request_id: 42 }, &mut snapshot).unwrap();
    codec::encode_frame(&wire::Shutdown { request_id: 43 }, &mut shutdown).unwrap();
    let versions = wire::Activate { request_id: 44, protocol: PROTOCOL, service_version: 0 };
    codec::encode_frame(&versions, &mut activate).unwrap();
    for (frame, answer) in [(snapshot, "SnapshotTaken"), (shutdown, "ShutDown"), (activate, "Activation")] {
        let mut node = leader();
        for conn in [99, 100] {
            node.on_admin(conn, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        }
        let sent = drive(&mut node);
        assert_eq!(sent.len(), 1, "{sent:?}");
        assert!(sent[0].starts_with("100: ") && sent[0].contains(answer), "{sent:?}");
        assert!(!sent[0].contains("index: 0,"), "not refused: {sent:?}");
    }

    // A change of members sent again isn't refused as one already under way.
    let mut node = leader();
    let m = member(2);
    let (peer, client, admin) = (m.peer.as_bytes(), m.client.as_bytes(), m.admin.as_bytes());
    let mut add = Vec::new();
    codec::encode_frame(&wire::AddMember { request_id: 45, id: 2, learner: 1, peer, client, admin }, &mut add).unwrap();
    for conn in [99, 100] {
        node.on_admin(conn, &codec::decode_frame(&add).unwrap().unwrap().0, 0);
    }
    let sent = drive(&mut node);
    assert!(!sent.iter().any(|s| s.contains("MembersRefused")), "{sent:?}");
    assert_eq!(node.local.changing.map(|(conn, id, _)| (conn, id)), Some((100, 45)), "under way, for 100");
}

/// A request sent again after it was made (its answer lost, or its leader gone, and nothing kept
/// of it) is answered as made: a change of members the members already show, a transfer to the
/// leader, a promotion this node made. One the members don't show yet is still refused.
#[test]
fn a_request_sent_again_after_it_was_made_is_answered_as_made() {
    fn ask(node: &mut Node<Recorder>, message: &impl turbofan_sbe::Encode) -> String {
        let mut frame = Vec::new();
        codec::encode_frame(message, &mut frame).unwrap();
        node.on_admin(7, &codec::decode_frame(&frame).unwrap().unwrap().0, node.now);
        drive(node).join(" ")
    }
    let mut node = leader();
    let index = node.raft().config_index();
    let changed = format!("MembersChanged(MembersChangedRef {{ request_id: 50, index: {index} }})");
    let m = member(1);
    let (peer, client, admin) = (m.peer.as_bytes(), m.client.as_bytes(), m.admin.as_bytes());
    let add = wire::AddMember { request_id: 50, id: 1, learner: 0, peer, client, admin };
    assert!(ask(&mut node, &add).contains(&changed), "added");
    assert!(ask(&mut node, &wire::RemoveMember { request_id: 50, id: 2 }).contains(&changed), "removed");
    let set_role = |learner| wire::SetRole { request_id: 50, id: 1, learner };
    assert!(ask(&mut node, &set_role(0)).contains(&changed), "a voter");
    assert!(ask(&mut node, &set_role(1)).contains("MembersRefused"), "not a learner yet, and the only voter");
    let elsewhere = wire::AddMember { peer: b"p9", ..add };
    assert!(ask(&mut node, &elsewhere).contains("MembersRefused"), "a member 1 at another address");

    let transfer = ask(&mut node, &wire::TransferLeadership { request_id: 50, to: 1 });
    assert!(transfer.contains("request_id: 50, leader: 1"), "{transfer}");

    node.promoted = Some((50, 9, 8));
    let promoted = ask(&mut node, &wire::Promote { request_id: 50 });
    assert!(promoted.contains("request_id: 50, index: 9, epoch: 8"), "{promoted}");
    node.promoted = Some((49, 9, 8));
    assert!(ask(&mut node, &wire::Promote { request_id: 50 }).contains("index: 0"), "another's");
}

/// A connection that opens a second session leaves the first for its client to reattach to; one
/// that closes before its session opens is forgotten, not attached to it.
#[test]
fn a_connection_holds_one_session_and_a_closed_one_none() {
    let mut node = leader();
    let first = open(&mut node, 7, 0);
    let second = open(&mut node, 7, 0);
    assert_ne!(first, second);
    assert_eq!((node.local.attached.get(&first), node.local.attached.get(&second)), (None, Some(&7)));

    let mut frame = Vec::new();
    codec::encode_frame(&wire::Connect { credentials: b"" }, &mut frame).unwrap();
    node.on_client(8, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
    node.on_disconnect(8);
    assert!(drive(&mut node).is_empty(), "nothing sent to it");
    assert!(!node.local.conns.contains_key(&8) && !node.local.attached.values().any(|&c| c == 8));
}
