use super::*;

const MS: u64 = 1_000_000;
const TIMEOUT: u64 = 1000 * MS;

/// Logs every callback. A message `at <id> <ms>` schedules a timer and `cancel <id>`
/// cancels one; any message is echoed.
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
    let sent = client(&mut node, 8, &wire::Reattach { session: session.0, last_egress_seq: 1, credentials: b"" }, 0);
    assert_eq!(sent.len(), 2, "{sent:?}");
    assert!(sent[0].contains("Opened") && sent[0].contains("applied_seq: 2"));
    assert!(sent[1].contains("egress_seq: 2"));
    // The old connection is no longer the session's.
    assert!(client(&mut node, 7, &message(3, b"three"), 0)[0].contains("NoSession"));
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

    let reattach = |credentials| wire::Reattach { session: session.0, last_egress_seq: 0, credentials };
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

/// An operator switches the cluster to versions every member has, at a point in the log;
/// not to ones a member lacks, or back.
#[test]
fn versions_activate_once_every_member_has_them() {
    let mut node = leader();
    let session = open(&mut node, 7, 0);
    let activate = |node: &mut Node<Recorder>, protocol, service_version| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Activate { request_id: 1, protocol, service_version }, &mut frame).unwrap();
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        drive(node).join(" ")
    };
    let refused = activate(&mut node, PROTOCOL + 1, 0);
    assert!(refused.contains("index: 0, lowest_protocol: 0, lowest_service_version: 1"), "{refused}");
    assert!(!activate(&mut node, 0, 1).contains("index: 0"), "this node has version 1");
    assert!(node.service.0.contains(&"activate 1".to_owned()));
    client(&mut node, 7, &wire::Message { session: session.0, client_seq: 1, payload: b"later" }, 0);
    assert!(node.service.0.contains(&"as version 1".to_owned()));
    assert!(activate(&mut node, 0, 0).contains("index: 0"), "not back");

    let bytes = encode_state(&node.state);
    let state = decode_state(&mut &bytes[..]).unwrap();
    assert_eq!((state.protocol, state.service_version), (0, 1), "kept in snapshots");
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

/// A learner promoted leads alone, of a new epoch that replaces its cluster's, and says so
/// once it applies; a new cluster's naming of one doesn't replace it. A voter isn't promoted.
#[test]
fn a_promoted_learner_names_a_new_epoch() {
    let promote = |node: &mut Node<Recorder>| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Promote { request_id: 9 }, &mut frame).unwrap();
        node.on_admin(99, &codec::decode_frame(&frame).unwrap().unwrap().0, 0);
        drive(node).join(" ")
    };
    assert!(promote(&mut leader()).contains("Promoted(PromotedRef { request_id: 9, index: 0, epoch: 0 })"));

    let mut config = leader().config;
    config.raft.id = NodeId(2);
    config.raft.members = vec![member(1), Member { learner: true, ..member(2) }];
    let mut node = Node::new(config, Recorder::default(), Meta::default(), None, Entries::default(), 0);
    node.state.epoch = 5;
    let answer = promote(&mut node);
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
