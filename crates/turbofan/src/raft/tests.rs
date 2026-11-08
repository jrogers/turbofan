use super::*;

const MS: u64 = 1_000_000;

fn member(id: u8) -> Member {
    Member { id: NodeId(id), peer: format!("p{id}"), client: format!("c{id}"), admin: format!("a{id}"), learner: false }
}

fn config(id: u8, members: &[u8]) -> Config {
    Config {
        id: NodeId(id),
        members: members.iter().map(|&m| member(m)).collect(),
        election_timeout: Duration::from_millis(150),
        heartbeat: Duration::from_millis(50),
        seed: u64::from(id),
        log_window_bytes: usize::MAX,
        log_tail_entries: 0,
        max_uncommitted_bytes: usize::MAX,
    }
}

#[test]
fn a_lone_member_elects_itself_and_commits() {
    let mut raft = Raft::new(config(1, &[1]), Meta::default(), None, Entries::default(), 0);
    raft.on_tick(raft.next_deadline());
    assert_eq!((raft.role(), raft.term()), (Role::Leader, 1));
    let out = raft.take_outputs();
    assert!(out.became_leader);
    assert_eq!(out.persist, [Persist::Meta(Meta { term: 1, vote: Some(NodeId(1)) })]);
    assert_eq!(raft.propose(b"a"), Some(1));
    assert_eq!(raft.commit_index(), 0, "not durable yet");
    raft.on_persisted(raft.log_end(), raft.now);
    assert_eq!((raft.commit_index(), raft.entry(1)), (1, Some(&b"a"[..])));
}

#[test]
fn a_restarted_node_has_its_log_but_learns_the_commit_again() {
    let mut records = Vec::new();
    Record { term: 2, index: 1, payload: b"x" }.encode_into(&mut records);
    let raft = Raft::new(
        config(1, &[1, 2, 3]),
        Meta { term: 2, vote: None },
        None,
        Entries::from_records(&records, usize::MAX),
        0,
    );
    assert_eq!((raft.last_index(), raft.term_at(1), raft.commit_index()), (1, Some(2), 0));
}

/// Records of the given terms, from index 1.
fn log(terms: &[u64]) -> Vec<u8> {
    let mut out = Vec::new();
    for (i, &term) in terms.iter().enumerate() {
        Record { term, index: crate::widen(i) + 1, payload: b"x" }.encode_into(&mut out);
    }
    out
}

/// A log of the given terms, from index 1, all in memory.
fn entries(terms: &[u64]) -> Entries {
    Entries::from_records(&log(terms), usize::MAX)
}

fn decode(frame: &[u8]) -> wire::Decoded<'_> {
    codec::decode_frame(frame).unwrap().unwrap().0
}

/// Makes the batch durable and returns its appends and sends, as a driver would.
fn flush(raft: &mut Raft) -> Vec<(NodeId, Vec<u8>)> {
    let mut out = raft.take_outputs();
    raft.on_persisted(raft.log_end(), raft.now);
    out.appends.append(&mut out.sends);
    out.appends
}

/// Delivers `sends` from `from`, and everything they cause, between the nodes in `up`. Returns
/// the reads they confirmed, and dropped, on the way.
fn deliver(rafts: &mut [Raft], up: &[u8], from: u8, sends: Vec<(NodeId, Vec<u8>)>, now: u64) -> Reads {
    /// Far more than any exchange here takes: past it, messages go back and forth for ever.
    const MAX_DELIVERED: usize = 100_000;
    let mut queue: Vec<_> = sends.into_iter().map(|(to, frame)| (NodeId(from), to, frame)).collect();
    let (mut delivered, mut reads) = (0, Reads::default());
    while let Some((from, to, frame)) = queue.pop() {
        delivered += 1;
        assert!(delivered <= MAX_DELIVERED, "messages going back and forth for ever: {} queued", queue.len());
        if !up.contains(&to.0) {
            continue;
        }
        let raft = &mut rafts[usize::from(to) - 1];
        raft.on_message(from, &decode(&frame), now);
        let mut out = raft.take_outputs();
        reads.confirmed.append(&mut out.confirmed_reads);
        reads.dropped.append(&mut out.dropped_reads);
        raft.on_persisted(raft.log_end(), raft.now);
        out.appends.append(&mut out.sends);
        queue.extend(out.appends.into_iter().map(|(next, frame)| (to, next, frame)));
    }
    reads
}

/// Reads confirmed and dropped, by [`deliver`].
#[derive(Debug, Default)]
struct Reads {
    confirmed: Vec<ReadIndex>,
    dropped: Vec<u64>,
}

/// Times `id` out from `now` and runs its election among `up`, retrying as a real node
/// would: a stale leader first stands down, and a pre-vote refused for a stale term teaches
/// it the term.
fn elect(rafts: &mut [Raft], up: &[u8], id: u8, now: u64) {
    let mut now = now;
    for _ in 0..3 {
        let raft = &mut rafts[usize::from(id) - 1];
        now = now.max(raft.next_deadline());
        raft.on_tick(now);
        let sends = flush(raft);
        deliver(rafts, up, id, sends, now);
        if rafts[usize::from(id) - 1].role() == Role::Leader {
            return;
        }
    }
    panic!("node {id} wasn't elected");
}

fn cluster(n: u8) -> Vec<Raft> {
    let members: Vec<u8> = (1..=n).collect();
    members.iter().map(|&id| Raft::new(config(id, &members), Meta::default(), None, Entries::default(), 0)).collect()
}

/// A leader's appends go out before its own records are durable, and it counts itself
/// towards a commit only once they are.
#[test]
fn a_leader_counts_itself_towards_a_commit_only_once_durable() {
    let mut rafts = cluster(3);
    let now = 1000 * MS;
    elect(&mut rafts, &[1, 2, 3], 1, now);
    let committed = rafts[0].commit_index();
    let index = rafts[0].propose(b"x").unwrap();
    let out = rafts[0].take_outputs();
    let end = rafts[0].log_end();
    assert!(matches!(out.persist[..], [Persist::Append(_)]), "{:?}", out.persist);
    let (_, append) = out.appends.into_iter().find(|&(to, _)| to == NodeId(2)).expect("an append for node 2");
    rafts[1].on_message(NodeId(1), &decode(&append), now);
    for (_, ack) in flush(&mut rafts[1]) {
        rafts[0].on_message(NodeId(2), &decode(&ack), now);
    }
    assert_eq!(rafts[0].commit_index(), committed, "node 2 alone is one of three");
    assert_eq!(end.index, index);
    let now = rafts[0].now;
    rafts[0].on_persisted(end, now);
    assert_eq!(rafts[0].commit_index(), index, "with node 1 durable, two of three");
}

/// A leader is backlogged while its uncommitted proposals reach the limit, and not once a
/// quorum has them; one that stands down forgets them.
#[test]
fn a_leader_is_backlogged_until_its_proposals_commit() {
    let mut rafts = cluster(3);
    rafts[0].config.max_uncommitted_bytes = 4;
    let now = 1000 * MS;
    elect(&mut rafts, &[1, 2, 3], 1, now);
    rafts[0].propose(b"abc");
    assert!(!rafts[0].backlogged(), "3 bytes of 4");
    rafts[0].propose(b"d");
    assert!(rafts[0].backlogged(), "4 bytes of 4");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, now);
    assert!(!rafts[0].backlogged(), "committed");

    rafts[0].propose(b"abcd");
    assert!(rafts[0].backlogged());
    elect(&mut rafts, &[2, 3], 2, 2 * now);
    let heartbeat = rafts[1].next_deadline();
    rafts[1].on_tick(heartbeat);
    let sends = flush(&mut rafts[1]);
    deliver(&mut rafts, &[1, 2, 3], 2, sends, heartbeat);
    assert_eq!(rafts[0].role(), Role::Follower);
    assert!(!rafts[0].backlogged(), "a follower proposes nothing");
}

/// Raft's figure 8: a leader can't commit an entry of an earlier term by counting replicas,
/// since a node that lacks it may still win an election and overwrite it.
#[test]
fn an_earlier_terms_entry_does_not_commit_by_counting_replicas() {
    let mut rafts = cluster(3);
    let second = 1000 * MS;
    elect(&mut rafts, &[1, 2, 3], 1, second);
    rafts[0].propose(b"one");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, second);
    assert_eq!(rafts[0].commit_index(), 1);

    // Node 1 appends index 2 in term 1 and reaches no one.
    rafts[0].propose(b"lost");
    flush(&mut rafts[0]);
    // Node 2 is elected in term 2, appends index 2 of its own, and reaches no one.
    elect(&mut rafts, &[2, 3], 2, 2 * second);
    rafts[1].propose(b"term 2");
    flush(&mut rafts[1]);
    // Node 1 is elected in term 3 and replicates its term-1 index 2 to node 3: a majority.
    elect(&mut rafts, &[1, 3], 1, 3 * second);
    assert_eq!(rafts[0].term_at(2), Some(1));
    let now = rafts[0].next_deadline();
    rafts[0].on_tick(now);
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 3], 1, sends, now);
    assert_eq!(rafts[2].term_at(2), Some(1), "node 3 has it");
    assert_eq!(rafts[0].commit_index(), 1, "but it isn't committed");

    // An entry of term 3 above it commits both.
    rafts[0].propose(b"term 3");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 3], 1, sends, now);
    assert_eq!(rafts[0].commit_index(), 3);
}

#[test]
fn a_vote_is_persisted_in_the_batch_that_grants_it() {
    let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 4, vote: None }, None, entries(&[1]), 0);
    let mut frame = Vec::new();
    let request = wire::RequestVote { term: 5, candidate: 2, last_index: 1, last_term: 1, pre_vote: 0, round: 0 };
    codec::encode_frame(&request, &mut frame).unwrap();
    raft.on_message(NodeId(2), &decode(&frame), 0);
    let out = raft.take_outputs();
    assert_eq!(out.persist.last(), Some(&Persist::Meta(Meta { term: 5, vote: Some(NodeId(2)) })));
    let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
    assert_eq!((vote.term(), vote.granted()), (5, 1));

    // And not again to someone else in the term.
    let request = wire::RequestVote { term: 5, candidate: 3, last_index: 1, last_term: 1, pre_vote: 0, round: 0 };
    frame.clear();
    codec::encode_frame(&request, &mut frame).unwrap();
    raft.on_message(NodeId(3), &decode(&frame), 0);
    let out = raft.take_outputs();
    let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
    assert_eq!(vote.granted(), 0);
}

/// A node with no members and no log yet, as one wiped and joining again under its old ID has,
/// votes for no one: not even a candidate whose log is ahead of its own, which may be a removed
/// node standing in members the cluster has left.
#[test]
fn a_node_that_knows_no_members_doesnt_vote() {
    let mut raft = Raft::new(config(3, &[]), Meta::default(), None, Entries::default(), 0);
    for pre_vote in [1, 0] {
        let mut frame = Vec::new();
        let request = wire::RequestVote { term: 5, candidate: 1, last_index: 9, last_term: 4, pre_vote, round: 0 };
        codec::encode_frame(&request, &mut frame).unwrap();
        raft.on_message(NodeId(1), &decode(&frame), 0);
        let out = raft.take_outputs();
        let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
        assert_eq!(vote.granted(), 0, "pre-vote {pre_vote}");
    }
}

/// A node whose log dropped a damaged tail may have lost an entry it acknowledged: an
/// election counting its vote needs one more than a majority, until it holds an entry of a
/// later term than it restarted in, when it trusts its log again.
#[test]
fn a_node_in_doubt_needs_one_more_vote_until_it_catches_up() {
    let mut rafts = cluster(3);
    let mut log = entries(&[1]);
    log.doubt();
    rafts[0] = Raft::new(config(1, &[1, 2, 3]), Meta { term: 1, vote: None }, None, log, 0);
    assert!(rafts[0].in_doubt());
    // Its own vote and node 2's are a majority, but its own is in doubt.
    let mut now = 1000 * MS;
    for _ in 0..3 {
        now = now.max(rafts[0].next_deadline());
        rafts[0].on_tick(now);
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &[1, 2], 1, sends, now);
        assert_ne!(rafts[0].role(), Role::Leader);
    }
    elect(&mut rafts, &[1, 2, 3], 1, now);
    assert!(rafts[0].in_doubt(), "nothing of the new term yet");
    rafts[0].propose(b"y").unwrap();
    let out = rafts[0].take_outputs();
    assert!(matches!(out.persist[..], [Persist::Append(_), Persist::Trust]), "{:?}", out.persist);
    assert!(!rafts[0].in_doubt());
}

/// An append reaching its leader's last index doesn't end a doubt: one sent before the node
/// restarted, arriving late, says less than it acknowledged. An entry of a later term does.
#[test]
fn a_node_in_doubt_trusts_its_log_only_from_a_later_term() {
    let mut doubting = entries(&[1]);
    doubting.doubt();
    let mut raft = Raft::new(config(2, &[1, 2, 3]), Meta { term: 2, vote: None }, None, doubting, 0);
    let mut send = |term: u64, prev_index: u64, prev_term: u64, records: &[u8], last_index: u64| {
        let count = u32::try_from(codec::records(records).count()).unwrap();
        let append = wire::AppendEntries {
            term,
            leader: 1,
            prev_index,
            prev_term,
            commit: 0,
            count,
            entries: records,
            last_index,
            read_round: 0,
        };
        let mut frame = Vec::new();
        codec::encode_frame(&append, &mut frame).unwrap();
        raft.on_message(NodeId(1), &decode(&frame), 0);
        raft.take_outputs();
        raft.in_doubt()
    };
    assert!(send(2, 0, 0, &log(&[1, 2, 2]), 3), "the leader's whole log, as it was");
    let mut later = Vec::new();
    Record { term: 3, index: 4, payload: b"x" }.encode_into(&mut later);
    assert!(!send(3, 3, 2, &later, 4), "an entry of term 3");
}

/// Two voters in doubt take two more votes than a majority: either may have lost an entry.
#[test]
fn each_voter_in_doubt_takes_one_more_vote() {
    let mut raft = Raft::new(config(1, &[1, 2, 3, 4, 5]), Meta { term: 1, vote: None }, None, entries(&[1]), 0);
    let round = raft.next_deadline();
    raft.on_tick(round);
    raft.take_outputs();
    assert_eq!(raft.role(), Role::PreCandidate);
    for (from, doubt) in [(2, 1), (3, 1), (4, 0)] {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Vote { term: 2, granted: 1, pre_vote: 1, doubt, round }, &mut frame).unwrap();
        raft.on_message(NodeId(from), &decode(&frame), 0);
    }
    assert_eq!(raft.role(), Role::PreCandidate, "four of five, two in doubt");
    let mut frame = Vec::new();
    codec::encode_frame(&wire::Vote { term: 2, granted: 1, pre_vote: 1, doubt: 0, round }, &mut frame).unwrap();
    raft.on_message(NodeId(5), &decode(&frame), 0);
    assert_eq!(raft.role(), Role::Candidate, "every member");
}

/// A member being added has the log before the entry that adds it, so it knows no members, but
/// it votes: when the voters in doubt take every member's vote, an election needs its, and
/// without it none is ever won (a nightly simulation's seed 420189338040480).
#[test]
fn a_member_that_has_the_log_but_not_its_members_votes() {
    let mut adding = Vec::new();
    let four: Vec<wire::MembersMember<'_>> =
        [1, 2, 3, 4].map(|id| wire::MembersMember { id, learner: 0, peer: b"p", client: b"c", admin: b"a" }).to_vec();
    wire::Members { timestamp: 0, member: &four }.encode_into(&mut adding).unwrap();
    let mut records = log(&[1]);
    Record { term: 1, index: 2, payload: &adding }.encode_into(&mut records);
    let mut leader_log = Entries::from_records(&records, usize::MAX);
    leader_log.doubt();
    let mut follower_log = entries(&[1]);
    follower_log.doubt();
    let meta = Meta { term: 1, vote: None };
    let mut rafts = vec![
        Raft::new(config(1, &[1, 2, 3]), meta, None, leader_log, 0),
        Raft::new(config(2, &[1, 2, 3]), meta, None, follower_log, 0),
        Raft::new(config(3, &[1, 2, 3]), meta, None, entries(&[1]), 0),
        Raft::new(config(4, &[]), meta, None, entries(&[1]), 0),
    ];
    assert_eq!(ids(rafts[0].members()), [1, 2, 3, 4]);
    assert!(rafts[3].members().is_empty());
    // Four voters, two in doubt: a majority of three and two more, so all four.
    elect(&mut rafts, &[1, 2, 3, 4], 1, 0);
}

/// A pre-vote delayed past its round doesn't count in the next: granted while no leader was
/// heard, it would let a node cut off from one depose it. An older node's, with no round, counts.
#[test]
fn a_pre_vote_counts_only_in_its_own_round() {
    let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 1, vote: None }, None, entries(&[1]), 0);
    let first = raft.next_deadline();
    raft.on_tick(first);
    let second = raft.next_deadline();
    raft.on_tick(second);
    let requests = raft.take_outputs().sends;
    assert_eq!(requests.len(), 4, "two rounds of two");
    let wire::Decoded::RequestVote(request) = decode(&requests[3].1) else { panic!() };
    assert_eq!((request.pre_vote(), request.round()), (1, Some(second)));
    let vote = |round| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::Vote { term: 1, granted: 1, pre_vote: 1, doubt: 0, round }, &mut frame).unwrap();
        frame
    };
    raft.on_message(NodeId(2), &decode(&vote(first)), second);
    assert_eq!(raft.role(), Role::PreCandidate, "the first round's");
    // Cut to before `round`, as an older node sends it.
    let mut older = vote(second);
    older.truncate(older.len() - 8);
    let len = u32::try_from(older.len() - 4).unwrap();
    older[..4].copy_from_slice(&len.to_le_bytes());
    let block_len = u16::from_le_bytes([older[4], older[5]]) - 8;
    older[4..6].copy_from_slice(&block_len.to_le_bytes());
    older[10..12].copy_from_slice(&4_u16.to_le_bytes());
    raft.on_message(NodeId(2), &decode(&older), second);
    assert_eq!(raft.role(), Role::Candidate, "an older node's");
}

/// A vote from a node in doubt says so; an older node's, without the field, doesn't.
#[test]
fn a_vote_from_a_node_in_doubt_says_so() {
    let mut log = entries(&[1]);
    log.doubt();
    let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 1, vote: None }, None, log, 0);
    let mut frame = Vec::new();
    let request = wire::RequestVote { term: 2, candidate: 2, last_index: 1, last_term: 1, pre_vote: 0, round: 0 };
    codec::encode_frame(&request, &mut frame).unwrap();
    raft.on_message(NodeId(2), &decode(&frame), 0);
    let out = raft.take_outputs();
    let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
    assert_eq!((vote.granted(), vote.doubt()), (1, Some(1)));

    // As an older node sends it: version 0, without `doubt`.
    let mut old = out.sends[0].1.clone();
    let len = old.len() - 1;
    old.truncate(len);
    old[..4].copy_from_slice(&u32::try_from(len - 4).unwrap().to_le_bytes());
    let block_len = u16::from_le_bytes([old[4], old[5]]) - 1;
    old[4..6].copy_from_slice(&block_len.to_le_bytes());
    old[10..12].copy_from_slice(&0_u16.to_le_bytes());
    let wire::Decoded::Vote(vote) = decode(&old) else { panic!("not a vote") };
    assert_eq!((vote.granted(), vote.doubt()), (1, None));
}

#[test]
fn a_vote_is_refused_to_a_candidate_with_an_older_log() {
    let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 2, vote: None }, None, entries(&[1, 2]), 0);
    for (last_index, last_term) in [(1, 2), (5, 1)] {
        let mut frame = Vec::new();
        let request = wire::RequestVote { term: 3, candidate: 2, last_index, last_term, pre_vote: 0, round: 0 };
        codec::encode_frame(&request, &mut frame).unwrap();
        raft.on_message(NodeId(2), &decode(&frame), 0);
        let out = raft.take_outputs();
        let wire::Decoded::Vote(vote) = decode(&out.sends[0].1) else { panic!("not a vote") };
        assert_eq!(vote.granted(), 0, "({last_index}, {last_term})");
    }
}

/// A follower with a stale tail acks, and commits, only as far as the leader's log is known
/// to match its own.
#[test]
fn a_follower_acks_and_commits_only_what_matches_the_leader() {
    let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, entries(&[1, 1, 1]), 0);
    let mut frame = Vec::new();
    let append = wire::AppendEntries {
        term: 2,
        leader: 2,
        prev_index: 1,
        prev_term: 1,
        commit: 3,
        count: 0,
        entries: &[],
        last_index: 3,
        read_round: 0,
    };
    codec::encode_frame(&append, &mut frame).unwrap();
    raft.on_message(NodeId(2), &decode(&frame), 0);
    let out = raft.take_outputs();
    let wire::Decoded::AppendAck(ack) = decode(&out.sends[0].1) else { panic!("not an ack") };
    assert_eq!((ack.success(), ack.index()), (1, 1));
    assert_eq!(raft.commit_index(), 1);
}

/// A batch still being written when a new leader replaces its entries: it being durable says
/// nothing of those that replaced them.
#[test]
fn an_earlier_batch_durable_does_not_count_entries_that_replaced_its_own() {
    let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, entries(&[1]), 0);
    let mut append = |term: u64, leader: u8, records: &[(u64, u64)]| {
        let mut bytes = Vec::new();
        for &(index, term) in records {
            Record { term, index, payload: b"x" }.encode_into(&mut bytes);
        }
        let count = u32::try_from(records.len()).unwrap();
        let append = wire::AppendEntries {
            term,
            leader,
            prev_index: 1,
            prev_term: 1,
            commit: 1,
            count,
            entries: &bytes,
            last_index: 0,
            read_round: 0,
        };
        let mut frame = Vec::new();
        codec::encode_frame(&append, &mut frame).unwrap();
        raft.on_message(NodeId(leader), &decode(&frame), 0);
        raft.take_outputs();
        raft.log_end()
    };
    let writing = append(1, 1, &[(2, 1), (3, 1)]);
    let replaced = append(2, 2, &[(2, 2)]);
    raft.on_persisted(writing, raft.now);
    assert_eq!(raft.durable, 1, "2 and 3 of term 1 were written, not 2 of term 2");
    raft.on_persisted(replaced, raft.now);
    assert_eq!(raft.durable, 2);
}

/// An append whose records' terms fall, or pass the leader's, is malformed: ignored, not taken
/// into the log (where it would break it) nor a panic.
#[test]
fn an_append_with_terms_out_of_order_is_ignored() {
    let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, entries(&[1]), 0);
    let mut append = |records: &[(u64, u64)]| {
        let mut bytes = Vec::new();
        for &(index, term) in records {
            Record { term, index, payload: b"x" }.encode_into(&mut bytes);
        }
        let count = u32::try_from(records.len()).unwrap();
        let append = wire::AppendEntries {
            term: 2,
            leader: 1,
            prev_index: 1,
            prev_term: 1,
            commit: 1,
            count,
            entries: &bytes,
            last_index: 0,
            read_round: 0,
        };
        let mut frame = Vec::new();
        codec::encode_frame(&append, &mut frame).unwrap();
        raft.on_message(NodeId(1), &decode(&frame), 0);
        raft.take_outputs();
        raft.last_index()
    };
    assert_eq!(append(&[(2, 2), (3, 1)]), 1, "falling");
    assert_eq!(append(&[(2, 3)]), 1, "past the leader's");
    assert_eq!(append(&[(2, 1), (3, 2)]), 3);
}

/// A peer that stops acking (gone) is sent no further than `MAX_IN_FLIGHT` past what it acked,
/// heartbeats included: they carry no records then, but still find where its log matches.
#[test]
fn heartbeats_to_a_peer_gone_carry_no_records() {
    let mut rafts = cluster(3);
    elect(&mut rafts, &[1, 2, 3], 1, 1000 * MS);
    let leader = &mut rafts[0];
    let to_3 = |sends: &[(NodeId, Vec<u8>)]| -> Vec<u32> {
        let appends = sends.iter().filter(|(to, _)| *to == NodeId(3));
        appends
            .filter_map(|(_, f)| if let wire::Decoded::AppendEntries(m) = decode(f) { Some(m.count()) } else { None })
            .collect()
    };
    let mut sent = 0;
    for _ in 0..MAX_IN_FLIGHT + 100 {
        leader.propose(b"x");
        sent += to_3(&flush(leader)).iter().map(|&n| u64::from(n)).sum::<u64>();
    }
    assert!(sent <= MAX_IN_FLIGHT + crate::widen(MAX_ENTRIES_PER_APPEND), "{sent}");
    for _ in 0..3 {
        let now = leader.next_deadline();
        leader.on_tick(now);
        let counts = to_3(&flush(leader));
        assert!(!counts.is_empty() && counts.iter().all(|&n| n == 0), "{counts:?}");
    }
}

#[test]
fn a_reordered_older_append_does_not_take_the_commit_back() {
    let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 1, vote: None }, None, Entries::default(), 0);
    let mut send = |prev_index: u64, commit: u64, entries: &[u8]| {
        let count = u32::try_from(codec::records(entries).count()).unwrap();
        let prev_term = u64::from(prev_index > 0);
        let append = wire::AppendEntries {
            term: 1,
            leader: 1,
            prev_index,
            prev_term,
            commit,
            count,
            entries,
            last_index: 0,
            read_round: 0,
        };
        let mut frame = Vec::new();
        codec::encode_frame(&append, &mut frame).unwrap();
        raft.on_message(NodeId(1), &decode(&frame), 0);
        raft.take_outputs();
        raft.commit_index()
    };
    assert_eq!(send(0, 2, &log(&[1, 1, 1])), 2);
    // Sent after the first, delivered after it, matching only up to 1, with a later commit.
    assert_eq!(send(1, 3, &[]), 2, "the commit index went back");
}

fn install_snapshot(raft: &mut Raft, last_index: u64, last_term: u64) -> Outputs {
    install_snapshot_summed(
        raft,
        last_index,
        last_term,
        crate::snapshot::checksum(last_index, last_term, &[], b"state"),
    )
}

fn install_snapshot_summed(raft: &mut Raft, last_index: u64, last_term: u64, checksum: u64) -> Outputs {
    let chunk = wire::InstallSnapshot {
        term: 2,
        leader: 1,
        last_index,
        last_term,
        offset: 0,
        done: 1,
        checksum,
        member: &[],
        chunk: b"state",
    };
    let mut frame = Vec::new();
    codec::encode_frame(&chunk, &mut frame).unwrap();
    raft.on_message(NodeId(1), &decode(&frame), 0);
    raft.take_outputs()
}

/// A snapshot arriving late, after the follower caught up by appends, mustn't take away the
/// entries after it: the follower acknowledged them.
#[test]
fn a_snapshot_the_log_already_matches_keeps_the_entries_after_it() {
    let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 2, vote: None }, None, entries(&[1, 1, 2, 2, 2]), 0);
    let out = install_snapshot(&mut raft, 3, 2);
    assert!(
        matches!(out.persist[..], [Persist::SnapshotChunk { .. }, Persist::InstallSnapshot { replace_log: false, .. }]),
        "{:?}",
        out.persist
    );
    assert_eq!((raft.snapshot_index(), raft.last_index(), raft.commit_index()), (3, 5, 3));
    assert!(raft.take_installed());
}

#[test]
fn a_snapshot_the_log_doesnt_match_replaces_it() {
    let mut raft = Raft::new(config(3, &[1, 2, 3]), Meta { term: 2, vote: None }, None, entries(&[1, 1, 1, 1]), 0);
    let out = install_snapshot(&mut raft, 3, 2);
    assert!(
        matches!(out.persist[..], [Persist::SnapshotChunk { .. }, Persist::InstallSnapshot { replace_log: true, .. }]),
        "{:?}",
        out.persist
    );
    assert_eq!((raft.snapshot_index(), raft.last_index(), raft.commit_index()), (3, 3, 3));
}

/// A leader that keeps no applied records in memory reads them back for a follower that
/// missed them.
#[test]
fn a_follower_behind_the_window_is_sent_records_read_back() {
    let mut rafts = cluster(3);
    rafts[0].config.log_window_bytes = 0;
    let now = 1000 * MS;
    elect(&mut rafts, &[1, 2], 1, now);
    for payload in [b"a", b"b", b"c"] {
        rafts[0].propose(payload);
    }
    let out = rafts[0].take_outputs();
    let [Persist::Append(disk)] = &out.persist[..] else { panic!("{:?}", out.persist) };
    let disk = disk.clone();
    let end = rafts[0].log_end();
    let now = rafts[0].now;
    rafts[0].on_persisted(end, now);
    deliver(&mut rafts, &[1, 2], 1, out.appends, now);
    rafts[0].on_applied(3);
    assert_eq!((rafts[0].commit_index(), rafts[0].entry(1)), (3, None), "committed, and out of memory");

    // Node 3 is back, and refuses a heartbeat past its log. The leader goes back, and asks for
    // the records from disk; they go in the next round.
    let now = rafts[0].next_deadline();
    rafts[0].on_tick(now);
    let out = rafts[0].take_outputs();
    let (_, heartbeat) = out.appends.into_iter().find(|&(to, _)| to == NodeId(3)).expect("a heartbeat for node 3");
    rafts[2].on_message(NodeId(1), &decode(&heartbeat), now);
    for (_, ack) in flush(&mut rafts[2]) {
        rafts[0].on_message(NodeId(3), &decode(&ack), now);
    }
    assert_eq!(rafts[0].take_outputs().reads, [1]);
    rafts[0].on_read(&disk);
    let sends = rafts[0].take_outputs().appends;
    let again = rafts[0].take_outputs();
    assert!(again.appends.is_empty() && again.reads.is_empty(), "sent once it's read, nor read again");
    deliver(&mut rafts, &[1, 3], 1, sends, now);
    assert_eq!((rafts[2].last_index(), rafts[2].entry(2)), (3, Some(&b"b"[..])));
    assert_eq!(rafts[0].entry(1), None, "read records are kept for one round");
}

/// A follower behind the leader's snapshot, but within the tail it keeps before it, is sent the
/// log; without a tail, the snapshot. Restarted, the leader keeps the tail it has on disk.
#[test]
fn a_follower_within_the_tail_is_sent_the_log_not_the_snapshot() {
    for tail in [3, 0] {
        let mut rafts = cluster(3);
        rafts[0].config.log_tail_entries = tail;
        let now = 1000 * MS;
        elect(&mut rafts, &[1, 2, 3], 1, now);
        for (payload, up) in [(b"a", &[1, 2, 3][..]), (b"b", &[1, 2, 3]), (b"c", &[1, 2]), (b"d", &[1, 2])] {
            rafts[0].propose(payload);
            let sends = flush(&mut rafts[0]);
            deliver(&mut rafts, up, 1, sends, now);
        }
        assert_eq!((rafts[0].commit_index(), rafts[2].last_index()), (4, 2), "node 3 missed 3 and 4");
        let members = rafts[0].members_at(4);
        let term = rafts[0].term_at(4).unwrap();
        rafts[0].on_snapshot_saved(Snapshot { index: 4, term, len: 1, checksum: 0, members });
        let out = rafts[0].take_outputs();
        let kept = if tail == 3 { 1 } else { 4 };
        assert_eq!(out.persist, [Persist::Compact { log: kept, snapshot: 4 }]);
        assert_eq!((rafts[0].snapshot_index(), rafts[0].term_at(kept)), (4, Some(term)));

        heartbeat(&mut rafts, &[1, 2, 3], 1);
        let sending = rafts[0].peers.iter().find(|p| p.id == NodeId(3)).unwrap().sending.is_some();
        if tail == 3 {
            assert!(!sending, "sent the log");
            assert_eq!((rafts[2].last_index(), rafts[2].entry(4)), (4, Some(&b"d"[..])));
        } else {
            assert!(sending, "sent the snapshot");
            assert_eq!(rafts[2].last_index(), 2);
        }
    }

    // Restarted with the whole log on disk, it keeps a tail of 3, its first entry's term known.
    let snapshot = Snapshot { index: 4, term: 2, len: 1, checksum: 0, members: vec![member(1)] };
    let mut config = config(1, &[1]);
    config.log_tail_entries = 3;
    let raft = Raft::new(config, Meta { term: 2, vote: None }, Some(snapshot), entries(&[1, 1, 2, 2, 2]), 0);
    assert_eq!((raft.snapshot_index(), raft.commit_index(), raft.last_index()), (4, 4, 5));
    assert_eq!((raft.term_at(0), raft.term_at(1), raft.term_at(3)), (None, Some(1), Some(2)), "kept from 2");
}

/// A leader sends a follower several chunks of a snapshot before hearing back, and sends
/// again from what the follower has when one is lost: at once if a later one shows it, or
/// after a heartbeat with no progress.
#[test]
fn snapshot_chunks_are_pipelined_and_a_lost_one_is_sent_again() {
    const CHUNK: u64 = crate::widen(MAX_READ_BYTES);
    let len = 3 * CHUNK + 10;
    let bytes: Vec<u8> = (0..len).map(|i| u8::try_from(i % 251).unwrap()).collect();
    let members = vec![member(1), member(2)];
    let checksum = crate::snapshot::checksum(10, 1, &members, &bytes);
    let snapshot = Snapshot { index: 10, term: 1, len, checksum, members };
    let mut rafts = vec![
        Raft::new(config(1, &[1, 2]), Meta { term: 1, vote: None }, Some(snapshot), Entries::default(), 0),
        Raft::new(config(2, &[1, 2]), Meta::default(), None, Entries::default(), 0),
    ];
    let now = 1000 * MS;
    elect(&mut rafts, &[1, 2], 1, now);
    // Node 2 refuses a heartbeat past its log, and the leader goes back past its snapshot.
    rafts[0].on_tick(now);
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2], 1, sends, now);
    let send_chunks = |raft: &mut Raft| {
        let out = raft.take_outputs();
        for &SnapshotRead { index, offset } in &out.snapshot_reads {
            let start = usize::try_from(offset).unwrap();
            raft.on_snapshot_read(index, offset, &bytes[start..(start + MAX_READ_BYTES).min(bytes.len())]);
        }
        let reads = out.snapshot_reads.iter().map(|r| r.offset).collect::<Vec<_>>();
        (
            reads,
            raft.take_outputs()
                .sends
                .into_iter()
                .filter(|&(to, _)| to == NodeId(2))
                .map(|(_, f)| f)
                .collect::<Vec<_>>(),
        )
    };
    let (reads, chunks) = send_chunks(&mut rafts[0]);
    assert_eq!(reads, [0, CHUNK, 2 * CHUNK, 3 * CHUNK]);
    assert_eq!(chunks.len(), 4, "the whole snapshot in flight before an ack");

    let deliver_chunks = |rafts: &mut [Raft], chunks: &[&Vec<u8>], now: u64| {
        for chunk in chunks {
            rafts[1].on_message(NodeId(1), &decode(chunk), now);
            for (_, ack) in flush(&mut rafts[1]) {
                rafts[0].on_message(NodeId(2), &decode(&ack), now);
            }
        }
    };
    // The second chunk is lost. Node 2 takes the first and refuses the third as out of order,
    // acking the first again: the leader sends again from there at once.
    deliver_chunks(&mut rafts, &[&chunks[0], &chunks[2]], now);
    let (reads, chunks) = send_chunks(&mut rafts[0]);
    assert_eq!(reads, [CHUNK, 2 * CHUNK, 3 * CHUNK]);
    // Now the last is lost, with nothing after it to show it. A heartbeat after the acks sees
    // progress; the next sees none, and goes back.
    deliver_chunks(&mut rafts, &[&chunks[0], &chunks[1]], now);
    assert_eq!(rafts[1].snapshot_index(), 0);
    let mut now = now;
    for _ in 0..2 {
        now = rafts[0].next_deadline();
        rafts[0].on_tick(now);
    }
    let (reads, chunks) = send_chunks(&mut rafts[0]);
    assert_eq!(reads, [3 * CHUNK]);
    deliver_chunks(&mut rafts, &chunks.iter().collect::<Vec<_>>(), now);
    assert_eq!(rafts[1].snapshot_index(), 10, "installed");
}

/// An ack of a snapshot the leader has moved on from, in flight as it took a newer one,
/// isn't counted towards the newer: the peer has only what it acked of that.
#[test]
fn an_ack_of_an_older_snapshot_is_not_counted() {
    let snapshot = Snapshot { index: 10, term: 1, len: 100, checksum: 0, members: vec![member(1), member(2)] };
    let mut rafts = vec![
        Raft::new(config(1, &[1, 2]), Meta { term: 1, vote: None }, Some(snapshot), Entries::default(), 0),
        Raft::new(config(2, &[1, 2]), Meta::default(), None, Entries::default(), 0),
    ];
    let now = 1000 * MS;
    elect(&mut rafts, &[1, 2], 1, now);
    rafts[0].on_tick(now);
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2], 1, sends, now);
    let acked = |raft: &Raft| raft.peers[0].sending.map(|s| (s.index, s.acked));
    assert_eq!(acked(&rafts[0]), Some((10, 0)), "sending node 2 the snapshot");

    let ack = |offset, index| {
        let mut frame = Vec::new();
        codec::encode_frame(&wire::SnapshotAck { term: rafts[0].term(), offset, index }, &mut frame).unwrap();
        frame
    };
    let (older, this) = (ack(60, 9), ack(40, 10));
    rafts[0].on_message(NodeId(2), &decode(&older), now);
    assert_eq!(acked(&rafts[0]), Some((10, 0)), "not of this snapshot");
    rafts[0].on_message(NodeId(2), &decode(&this), now);
    assert_eq!(acked(&rafts[0]), Some((10, 40)));
}

fn ids(members: &[Member]) -> Vec<u8> {
    members.iter().map(|m| m.id.0).collect()
}

/// Heartbeats from `id`, and all they cause, among `up`.
fn heartbeat(rafts: &mut [Raft], up: &[u8], id: u8) -> u64 {
    let raft = &mut rafts[usize::from(id) - 1];
    let now = raft.next_deadline();
    raft.on_tick(now);
    let sends = flush(raft);
    deliver(rafts, up, id, sends, now);
    now
}

/// A node started with `--join` is sent the log without a vote; once it has caught up, the
/// members with it are proposed and committed, everywhere.
#[test]
fn a_member_is_added_once_it_has_caught_up() {
    let mut rafts = cluster(3);
    rafts.push(Raft::new(config(4, &[]), Meta::default(), None, Entries::default(), 0));
    let all = [1, 2, 3, 4];
    elect(&mut rafts, &all, 1, 1000 * MS);
    assert!(!rafts[0].add_member(member(4)), "not before an entry of its term is committed");
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &all, 1, sends, 1000 * MS);
    assert!(rafts[0].add_member(member(4)));
    assert!(!rafts[0].add_member(member(5)), "one change at a time");
    assert_eq!(ids(rafts[0].members()), [1, 2, 3], "not a member until it has caught up");

    // The first heartbeat learns it's new enough; then it's sent the log, and is added.
    for _ in 0..3 {
        heartbeat(&mut rafts, &all, 1);
    }
    for raft in &rafts {
        assert_eq!(ids(raft.members()), [1, 2, 3, 4], "node {}", raft.id());
    }
    assert_eq!(rafts[3].commit_index(), rafts[0].commit_index());
    assert!(rafts[0].can_change(), "committed");
}

/// A learner is proposed as soon as it answers, new enough, before it has any of the log: the
/// quorum doesn't wait on it. It catches up as a member.
#[test]
fn a_learner_is_added_without_catching_up() {
    let mut rafts = cluster(3);
    rafts.push(Raft::new(config(4, &[]), Meta::default(), None, Entries::default(), 0));
    let all = [1, 2, 3, 4];
    elect(&mut rafts, &[1, 2, 3], 1, 1000 * MS);
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, 1000 * MS);
    assert!(rafts[0].add_member(Member { learner: true, ..member(4) }));

    // Its answer to the first heartbeat, which sends it nothing of the log.
    let now = rafts[0].next_deadline();
    rafts[0].on_tick(now);
    for (to, frame) in flush(&mut rafts[0]).into_iter().filter(|(to, _)| to.0 == 4) {
        rafts[3].on_message(NodeId(1), &decode(&frame), now);
        for (_, ack) in flush(&mut rafts[3]) {
            rafts[0].on_message(to, &decode(&ack), now);
        }
    }
    assert_eq!(rafts[3].last_index(), 0, "it has none of the log");
    let learners: Vec<_> = rafts[0].members().iter().filter(|m| m.learner).map(|m| m.id.0).collect();
    assert_eq!(learners, [4], "but is proposed");

    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &all, 1, sends, now);
    heartbeat(&mut rafts, &all, 1);
    assert!(!rafts[0].changing_members(), "committed");
    assert_eq!(rafts[3].commit_index(), rafts[0].commit_index(), "and it caught up");
    assert!(rafts[3].members().iter().any(|m| m.id.0 == 4 && m.learner));
}

/// A node whose code is older than what the cluster runs isn't sent the log, which applying
/// it would run behaviour it doesn't have, nor added.
#[test]
fn a_member_older_than_the_cluster_runs_is_not_added() {
    let mut rafts = cluster(3);
    rafts.push(Raft::new(config(4, &[]), Meta::default(), None, Entries::default(), 0));
    for raft in &mut rafts[..3] {
        raft.advertise(1, 1);
    }
    rafts[0].require(1, 1);
    let all = [1, 2, 3, 4];
    elect(&mut rafts, &all, 1, 1000 * MS);
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &all, 1, sends, 1000 * MS);
    assert_eq!(rafts[0].lowest_versions(), (1, 1), "the members, as they said");
    assert!(rafts[0].add_member(member(4)));
    heartbeat(&mut rafts, &all, 1);
    heartbeat(&mut rafts, &all, 1);
    assert_eq!(rafts[3].last_index(), 0, "not sent the log, which it would apply");
    assert!(!rafts[0].changing_members(), "dropped");
    assert_eq!(ids(rafts[0].members()), [1, 2, 3]);
}

/// Nodes 1 and 2 vote; 3 is a learner.
fn with_learner() -> Vec<Raft> {
    let config = |id| {
        let mut config = config(id, &[1, 2, 3]);
        config.members[2].learner = true;
        config
    };
    (1..=3).map(|id| Raft::new(config(id), Meta::default(), None, Entries::default(), 0)).collect()
}

fn roles(raft: &Raft) -> Vec<(u8, bool)> {
    raft.members().iter().map(|m| (m.id.0, m.learner)).collect()
}

/// A learner made a voter in place catches up first, then the members with it a voter are
/// proposed, everywhere; a voter made a learner is proposed at once. The last voter stays one.
#[test]
fn a_member_changes_role_in_place() {
    let mut rafts = with_learner();
    let all = [1, 2, 3];
    elect(&mut rafts, &[1, 2], 1, 1000 * MS);
    assert!(!rafts[0].set_role(NodeId(3), false), "not before an entry of its term is committed");
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2], 1, sends, 1000 * MS);
    assert!(!rafts[0].set_role(NodeId(2), false), "a voter already");
    assert!(!rafts[0].set_role(NodeId(4), false), "not a member");

    // Node 3 has none of the log: it isn't a voter until it has caught up.
    assert!(rafts[0].set_role(NodeId(3), false));
    assert!(!rafts[0].set_role(NodeId(2), true), "one change at a time");
    assert_eq!(roles(&rafts[0]), [(1, false), (2, false), (3, true)]);
    assert_eq!(rafts[0].peers().len(), 3, "no second peer for it");
    heartbeat(&mut rafts, &all, 1);
    heartbeat(&mut rafts, &all, 1);
    assert!(!rafts[0].changing_members(), "committed");
    for raft in &rafts {
        assert_eq!(roles(raft), [(1, false), (2, false), (3, false)]);
    }

    assert!(rafts[0].set_role(NodeId(2), true));
    assert_eq!(roles(&rafts[0]), [(1, false), (2, true), (3, false)], "at once");
    let (sends, now) = (flush(&mut rafts[0]), rafts[0].now);
    deliver(&mut rafts, &all, 1, sends, now);
    heartbeat(&mut rafts, &all, 1);
    assert!(!rafts[0].changing_members());
    assert!(rafts[0].set_role(NodeId(3), true));
    let (sends, now) = (flush(&mut rafts[0]), rafts[0].now);
    deliver(&mut rafts, &all, 1, sends, now);
    heartbeat(&mut rafts, &all, 1);
    assert!(!rafts[0].set_role(NodeId(1), true), "the last voter");
}

/// A voter made a learner by a change it hasn't seen committed still stands: it may hold the
/// most of the log, which the others, counting it a voter, may need to elect anyone.
#[test]
fn a_voter_demoted_by_an_uncommitted_change_still_stands() {
    let members = [member(1), member(2), member(3)];
    let demoted: Vec<wire::MembersMember<'_>> = members
        .iter()
        .map(|m| wire::MembersMember {
            id: m.id.0,
            learner: u8::from(m.id.0 == 1),
            peer: m.peer.as_bytes(),
            client: m.client.as_bytes(),
            admin: m.admin.as_bytes(),
        })
        .collect();
    let mut payload = Vec::new();
    wire::Members { timestamp: 0, member: &demoted }.encode_into(&mut payload).unwrap();
    let mut records = Vec::new();
    Record { term: 1, index: 1, payload: &payload }.encode_into(&mut records);
    let log = Entries::from_records(&records, usize::MAX);
    let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 1, vote: None }, None, log, 0);
    assert!(raft.is_learner(NodeId(1)));
    raft.on_tick(raft.next_deadline());
    assert_eq!(raft.role(), Role::PreCandidate, "it stands");
    assert!(raft.votes.is_empty(), "its own vote not counted");
}

/// A learner takes the log, but its ack doesn't commit, it never stands, and the leadership
/// isn't handed to it.
#[test]
fn a_learner_takes_the_log_but_has_no_say() {
    let mut rafts = with_learner();
    let learner = &mut rafts[2];
    learner.on_tick(learner.next_deadline());
    assert_eq!(learner.role(), Role::Follower, "it never stands");
    assert!(flush(learner).is_empty());

    elect(&mut rafts, &[1, 2], 1, 1000 * MS);
    let committed = rafts[0].commit_index();
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 3], 1, sends, 1000 * MS);
    assert_eq!(rafts[2].last_index(), rafts[0].last_index(), "the learner has it");
    assert_eq!(rafts[0].commit_index(), committed, "but a voter doesn't");
    assert_eq!(rafts[0].transfer_leadership(Some(NodeId(3))), None, "not to a learner");

    // Without node 2, the leader hears no quorum, and stands down.
    let now = rafts[0].next_deadline() + 2 * 150 * MS;
    rafts[0].on_tick(now);
    assert_eq!(rafts[0].role(), Role::Follower);
}

/// A learner promoted, its leader silent an election timeout, leads a cluster of its own:
/// itself, a voter, its log cut to its commit index. The tail past it, which its old cluster may
/// never have committed (and may have committed other entries in place of), is dropped, not
/// committed. Not a voter, nor a learner that hears its leader.
#[test]
fn a_promoted_learner_leads_a_cluster_of_its_own() {
    let mut rafts = with_learner();
    elect(&mut rafts, &[1, 2], 1, 1000 * MS);
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 3], 1, sends, 1000 * MS);
    let (tail, committed) = (rafts[2].last_index(), rafts[2].commit_index());
    assert!(tail > committed, "the learner holds what its cluster hasn't committed");
    assert!(!rafts[2].promote(1000 * MS), "it hears its leader");
    assert!(!rafts[1].promote(5000 * MS), "not a learner");

    let learner = &mut rafts[2];
    assert!(learner.promote(1000 * MS + 2 * 150 * MS));
    assert_eq!((learner.role(), learner.term()), (Role::Leader, 2));
    let me: Vec<_> = learner.members().iter().map(|m| (m.id.0, m.learner)).collect();
    assert_eq!(me, [(3, false)]);
    let out = learner.take_outputs();
    assert_eq!(out.persist[0], Persist::TruncateFrom(committed + 1), "{:?}", out.persist);
    learner.on_persisted(learner.log_end(), learner.now);
    assert_eq!(learner.commit_index(), learner.last_index(), "alone, it commits on its own");
    assert_eq!(learner.last_index(), committed + 1, "its members, after what it had committed");
    assert_ne!(learner.entry(tail), Some(&b"x"[..]), "not the tail");
}

/// A learner just started hasn't heard its leader yet, which may well be alive: it isn't
/// promoted until it's been silent an election timeout.
#[test]
fn a_learner_just_started_is_not_promoted() {
    let mut learner = with_learner().remove(2);
    assert!(!learner.promote(MS));
    assert!(learner.promote(2 * 150 * MS));
}

/// A leader that removed itself, leading until that commits, doesn't count its own snapshot
/// towards a quorum of the voters: only they do.
#[test]
fn a_leader_not_voting_does_not_count_towards_a_snapshot_quorum() {
    let mut rafts = cluster(3);
    elect(&mut rafts, &[1, 2, 3], 1, 1000 * MS);
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, 1000 * MS);
    let leader = &mut rafts[0];
    let snapshot = |leader: &mut Raft| {
        leader.snapshot = Some(Snapshot { index: 2, term: 1, len: 0, checksum: 0, members: Vec::new() });
        for peer in &mut leader.peers {
            peer.snapshot_index = if peer.id == NodeId(2) { 2 } else { 0 };
        }
    };
    snapshot(leader);
    assert_eq!(leader.quorum_snapshot_index(), 2, "itself and node 2");
    leader.snapshot = None;
    assert!(leader.remove_member(NodeId(1)));
    snapshot(leader);
    assert_eq!(leader.quorum_snapshot_index(), 0, "node 2 alone, of 2 and 3");
}

/// The last voter isn't removed, though learners would be left.
#[test]
fn the_last_voter_is_not_removed() {
    let mut config = config(1, &[1, 2]);
    config.members[1].learner = true;
    let mut raft = Raft::new(config, Meta::default(), None, Entries::default(), 0);
    raft.on_tick(raft.next_deadline());
    assert_eq!(raft.role(), Role::Leader, "a learner isn't needed for a quorum");
    raft.propose(b"x");
    raft.on_persisted(raft.log_end(), raft.now);
    assert!(!raft.remove_member(NodeId(1)));
    assert!(raft.remove_member(NodeId(2)));
}

/// A leader that removes itself leads until that commits, then stands down; the others elect a
/// leader between them.
#[test]
fn a_leader_that_removes_itself_stands_down_once_committed() {
    let mut rafts = cluster(3);
    let all = [1, 2, 3];
    elect(&mut rafts, &all, 1, 1000 * MS);
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &all, 1, sends, 1000 * MS);
    assert!(rafts[0].remove_member(NodeId(1)));
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &all, 1, sends, 1000 * MS);
    assert_eq!(rafts[0].role(), Role::Follower);
    assert_eq!(ids(rafts[1].members()), [2, 3]);

    // The others elect a leader between them.
    elect(&mut rafts, &[2, 3], 2, 5000 * MS);
}

/// A leader hands over to the follower most caught up: it stands for election at once, without
/// a pre-vote, and wins the next term.
#[test]
fn the_leadership_is_handed_to_a_caught_up_follower() {
    let mut rafts = cluster(3);
    let all = [1, 2, 3];
    elect(&mut rafts, &all, 1, 1000 * MS);
    rafts[0].propose(b"x");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2], 1, sends, 1000 * MS);
    assert_eq!(rafts[0].transfer_leadership(Some(NodeId(1))), None, "not to itself");
    assert_eq!(rafts[0].transfer_leadership(Some(NodeId(9))), None, "not to a node that isn't a member");
    assert_eq!(rafts[0].transfer_leadership(None), Some(NodeId(2)), "the one with the log");
    assert_eq!(rafts[0].transfer_leadership(Some(NodeId(3))), None, "one at a time");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &all, 1, sends, 1000 * MS);
    assert_eq!((rafts[1].role(), rafts[1].term()), (Role::Leader, 2));
    assert_eq!(rafts[0].role(), Role::Follower);
    heartbeat(&mut rafts, &all, 2);
    assert_eq!(rafts[0].leader(), Some(NodeId(2)), "the old leader hears from the new");
}

/// A `Members` entry takes effect when it's appended, and is undone if it's truncated.
#[test]
fn a_truncated_members_entry_is_undone() {
    let mut members = Vec::new();
    let three: Vec<wire::MembersMember<'_>> =
        [1, 2, 3].map(|id| wire::MembersMember { id, learner: 0, peer: b"p", client: b"c", admin: b"a" }).to_vec();
    wire::Members { timestamp: 0, member: &three }.encode_into(&mut members).unwrap();
    let mut records = log(&[1]);
    Record { term: 1, index: 2, payload: &members }.encode_into(&mut records);
    let mut raft = Raft::new(
        config(3, &[1, 3]),
        Meta { term: 1, vote: None },
        None,
        Entries::from_records(&records, usize::MAX),
        0,
    );
    assert_eq!(ids(raft.members()), [1, 2, 3]);
    assert_eq!(ids(&raft.members_at(1)), [1, 3]);

    // A leader of term 2 has another entry at 2: the members go back.
    let entries = log(&[1, 2]);
    let append = wire::AppendEntries {
        term: 2,
        leader: 1,
        prev_index: 0,
        prev_term: 0,
        commit: 0,
        count: 2,
        entries: &entries,
        last_index: 2,
        read_round: 0,
    };
    let mut frame = Vec::new();
    codec::encode_frame(&append, &mut frame).unwrap();
    raft.on_message(NodeId(1), &decode(&frame), 0);
    assert_eq!(ids(raft.members()), [1, 3]);
    assert_eq!(raft.take_outputs().peers.map(|m| ids(&m)), Some(vec![1, 3]));
}

/// Nodes with nearby seeds time out at different times: the same jitter splits every vote.
#[test]
fn nearby_seeds_give_different_election_timeouts() {
    let deadlines: Vec<u64> = (1..=4)
        .map(|id| Raft::new(config(id, &[1, 2, 3, 4]), Meta::default(), None, Entries::default(), 0).next_deadline())
        .collect();
    let distinct: std::collections::BTreeSet<_> = deadlines.iter().collect();
    assert_eq!(distinct.len(), deadlines.len(), "{deadlines:?}");
}

/// A follower told to stop with a snapshot it doesn't have yet forgets it in a later term: else,
/// caught up there, it stops long after the cluster started again.
#[test]
fn a_stop_is_forgotten_in_a_later_term() {
    let mut raft = Raft::new(config(1, &[1, 2, 3]), Meta { term: 1, vote: None }, None, Entries::default(), 0);
    let send = |raft: &mut Raft, from: u8, message: &dyn Fn(&mut Vec<u8>)| {
        let mut frame = Vec::new();
        message(&mut frame);
        raft.on_message(NodeId(from), &decode(&frame), MS);
    };
    let heartbeat = |term| wire::AppendEntries {
        term,
        leader: 2,
        prev_index: 0,
        prev_term: 0,
        commit: 0,
        count: 0,
        entries: &[],
        last_index: 0,
        read_round: 0,
    };
    send(&mut raft, 2, &|f| codec::encode_frame(&heartbeat(1), f).unwrap());
    send(&mut raft, 2, &|f| codec::encode_frame(&wire::Stop { term: 1, index: 100 }, f).unwrap());
    assert_eq!(raft.stop, Some((100, MS)), "to stop once it has snapshot 100");
    send(&mut raft, 2, &|f| codec::encode_frame(&heartbeat(2), f).unwrap());
    assert_eq!(raft.stop, None);
}

/// A snapshot whose bytes don't match the leader's checksum was damaged on its way, in the
/// leader's file, say: it isn't installed, as a disk this node then finds damaged, and it's asked
/// for again from the start.
#[test]
fn a_snapshot_damaged_on_its_way_is_asked_for_again() {
    let mut raft = Raft::new(config(2, &[1, 2, 3]), Meta { term: 2, vote: None }, None, Entries::default(), 0);
    let sum = crate::snapshot::checksum(5, 2, &[], b"state");
    let out = install_snapshot_summed(&mut raft, 5, 2, sum ^ 1);
    assert!(!out.persist.iter().any(|op| matches!(op, Persist::InstallSnapshot { .. })), "{:?}", out.persist);
    assert_eq!(raft.snapshot_index(), 0);
    let acks: Vec<_> = out.sends.iter().map(|(_, f)| format!("{:?}", decode(f))).collect();
    assert_eq!(acks, ["SnapshotAck(SnapshotAckRef { term: 2, offset: 0, index: 5 })"]);
    // Sent again, whole: installed.
    let out = install_snapshot_summed(&mut raft, 5, 2, sum);
    assert!(out.persist.iter().any(|op| matches!(op, Persist::InstallSnapshot { .. })));
    assert_eq!(raft.snapshot_index(), 5);
}

/// A follower that lost entries it acknowledged, its log cut back at a damaged tail, is sent them
/// again: the leader goes back past what it had matched, rather than sending the same append to
/// each refusal, for ever.
#[test]
fn a_follower_that_lost_acknowledged_entries_is_sent_them_again() {
    let mut rafts = cluster(3);
    elect(&mut rafts, &[1, 2, 3], 1, 0);
    for payload in [b"a", b"b", b"c"] {
        rafts[0].propose(payload);
    }
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, MS);
    let last = rafts[0].last_index();
    assert_eq!(rafts[1].last_index(), last);
    // Node 2 restarts with its last two entries gone, in doubt.
    let terms: Vec<u64> = (1..=last - 2).map(|i| rafts[0].term_at(i).unwrap()).collect();
    let mut log = entries(&terms);
    log.doubt();
    rafts[1] = Raft::new(config(2, &[1, 2, 3]), Meta { term: rafts[0].term(), vote: None }, None, log, MS);
    // The next entry's append, and what follows, a bounded number of messages.
    let now = 2 * MS;
    rafts[0].propose(b"d");
    let mut queue: Vec<(u8, NodeId, Vec<u8>)> = flush(&mut rafts[0]).into_iter().map(|(to, f)| (1, to, f)).collect();
    for _ in 0..50 {
        let Some((from, to, frame)) = queue.pop() else { break };
        if to.0 == 3 {
            continue;
        }
        let raft = &mut rafts[usize::from(to) - 1];
        raft.on_message(NodeId(from), &decode(&frame), now);
        queue.extend(flush(raft).into_iter().map(|(next, f)| (to.0, next, f)));
    }
    assert!(queue.is_empty(), "still refusing and resending: {} in flight", queue.len());
    assert_eq!(rafts[1].last_index(), last + 1, "sent what it lost");
}

/// Each of the appends in flight to a follower that lost its log is refused: the leader answers
/// the first with a probe, and the rest, refusals of older appends, not at all. Answering each
/// sent more than were answered, round after round.
#[test]
fn refusals_of_appends_in_flight_are_answered_with_one_probe() {
    let mut rafts = cluster(3);
    elect(&mut rafts, &[1, 2, 3], 1, 0);
    rafts[0].propose(b"a");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, MS);
    // Node 2 restarts with nothing, and the leader pipelines three appends to it.
    rafts[1] =
        Raft::new(config(2, &[1, 2, 3]), Meta { term: rafts[0].term(), vote: None }, None, Entries::default(), MS);
    let mut appends = Vec::new();
    for payload in [b"b", b"c", b"d"] {
        rafts[0].propose(payload);
        appends.extend(flush(&mut rafts[0]).into_iter().filter(|&(to, _)| to == NodeId(2)));
    }
    assert_eq!(appends.len(), 3);
    let mut refusals = Vec::new();
    for (_, append) in appends {
        rafts[1].on_message(NodeId(1), &decode(&append), 2 * MS);
        refusals.extend(flush(&mut rafts[1]));
    }
    for (_, refusal) in &refusals {
        let wire::Decoded::AppendAck(ack) = decode(refusal) else { panic!() };
        assert_eq!((ack.success(), ack.index()), (0, 1), "from its log's end, empty");
    }
    for (_, refusal) in &refusals {
        rafts[0].on_message(NodeId(2), &decode(refusal), 2 * MS);
    }
    let resent: Vec<_> = flush(&mut rafts[0]).into_iter().filter(|&(to, _)| to == NodeId(2)).collect();
    assert_eq!(resent.len(), 1, "one probe for {} refusals", refusals.len());
    // Taken, it ends the probe: the follower has the whole log.
    deliver(&mut rafts, &[1, 2], 1, resent, 3 * MS);
    assert_eq!(rafts[1].last_index(), rafts[0].last_index());
}

/// A refusal from a node older than `prevIndex` (version 6) doesn't say what it refuses: it's
/// answered from where it says, as before.
#[test]
fn an_older_followers_refusal_is_answered_from_where_it_says() {
    let mut rafts = cluster(3);
    elect(&mut rafts, &[1, 2, 3], 1, 0);
    rafts[0].propose(b"a");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, MS);
    rafts[1] =
        Raft::new(config(2, &[1, 2, 3]), Meta { term: rafts[0].term(), vote: None }, None, Entries::default(), MS);
    rafts[0].propose(b"b");
    let (_, append) = flush(&mut rafts[0]).into_iter().find(|&(to, _)| to == NodeId(2)).unwrap();
    rafts[1].on_message(NodeId(1), &decode(&append), 2 * MS);
    let Ok([(_, mut refusal)]) = <[_; 1]>::try_from(flush(&mut rafts[1])) else { panic!() };
    // Without `prevIndex`, the block 8 bytes shorter, and saying version 6.
    refusal.truncate(refusal.len() - 8);
    let len = u32::try_from(refusal.len() - 4).unwrap();
    refusal[..4].copy_from_slice(&len.to_le_bytes());
    let block = u16::from_le_bytes([refusal[4], refusal[5]]) - 8;
    refusal[4..6].copy_from_slice(&block.to_le_bytes());
    refusal[10..12].copy_from_slice(&6_u16.to_le_bytes());
    let wire::Decoded::AppendAck(read) = decode(&refusal) else { panic!() };
    assert_eq!((read.success(), read.prev_index()), (0, None));
    rafts[0].on_message(NodeId(2), &decode(&refusal), 2 * MS);
    let resent: Vec<_> = flush(&mut rafts[0]).into_iter().filter(|&(to, _)| to == NodeId(2)).collect();
    deliver(&mut rafts, &[1, 2], 1, resent, 3 * MS);
    assert_eq!(rafts[1].last_index(), rafts[0].last_index());
}

/// A probe refused too, the follower's log diverging further back than its first refusal says,
/// is answered: the leader goes back again, as far as it must.
#[test]
fn a_refused_probe_goes_back_again() {
    let members = [1, 2, 3];
    let leader = [1, 1, 4, 4, 4];
    let mut rafts: Vec<Raft> = [&leader[..], &[1, 1, 2, 2, 3, 3, 3], &leader]
        .iter()
        .zip(1..)
        .map(|(terms, id)| Raft::new(config(id, &members), Meta { term: 4, vote: None }, None, entries(terms), 0))
        .collect();
    elect(&mut rafts, &[1, 2, 3], 1, 0);
    let mut now = rafts[0].now;
    for _ in 0..5 {
        now = rafts[0].next_deadline().max(now + MS);
        rafts[0].on_tick(now);
        let sends = flush(&mut rafts[0]);
        deliver(&mut rafts, &[1, 2, 3], 1, sends, now);
    }
    let last = rafts[0].last_index();
    assert_eq!(rafts[1].last_index(), last);
    assert!((1..=last).all(|i| rafts[1].term_at(i) == rafts[0].term_at(i)), "the leader's log");
}

/// Proposals more than one append holds go out in one round, pipelined, not one append a round.
#[test]
fn appends_are_pipelined() {
    let mut rafts = cluster(3);
    elect(&mut rafts, &[1, 2, 3], 1, 0);
    flush(&mut rafts[0]);
    for _ in 0..2 * MAX_ENTRIES_PER_APPEND + 1 {
        rafts[0].propose(b"x");
    }
    let to_2 = flush(&mut rafts[0]).into_iter().filter(|&(to, _)| to == NodeId(2)).count();
    assert_eq!(to_2, 3, "three appends at once");
}

/// A three-node cluster led by node 1, with an entry of its term committed everywhere.
fn led_and_committed(now: u64) -> Vec<Raft> {
    let mut rafts = cluster(3);
    elect(&mut rafts, &[1, 2, 3], 1, now);
    rafts[0].propose(b"new term");
    let sends = flush(&mut rafts[0]);
    deliver(&mut rafts, &[1, 2, 3], 1, sends, now);
    assert_eq!(rafts[0].commit_index(), 1);
    rafts
}

/// The read round each append carries, by whom it's to.
fn read_rounds(sends: &[(NodeId, Vec<u8>)]) -> Vec<(NodeId, u64)> {
    let round = |frame| match decode(frame) {
        wire::Decoded::AppendEntries(m) => m.read_round(),
        _ => None,
    };
    sends.iter().filter_map(|(to, frame)| Some((*to, round(frame)?))).collect()
}

/// A lone voter confirms a read itself, but not before an entry of its own term commits: until
/// then its commit index may lack what an earlier leader committed.
#[test]
fn a_lone_voter_confirms_a_read_once_its_term_has_committed() {
    let mut raft = Raft::new(config(1, &[1]), Meta::default(), None, Entries::default(), 0);
    raft.on_tick(raft.next_deadline());
    raft.take_outputs();
    assert!(raft.read_index(7));
    assert_eq!(raft.take_outputs().confirmed_reads, [], "nothing of its term committed");
    raft.propose(b"new term");
    raft.on_persisted(raft.log_end(), raft.now);
    assert_eq!(raft.take_outputs().confirmed_reads, [ReadIndex { id: 7, index: 1 }]);
}

/// A leader starts a round for a read, which its appends carry, and a quorum's acks to it
/// (the leader's own among them) confirm the read at the commit index.
#[test]
fn a_read_is_confirmed_by_a_quorums_acks_to_its_round() {
    let now = 1000 * MS;
    let mut rafts = led_and_committed(now);
    assert!(rafts[0].read_index(1));
    let sends = flush(&mut rafts[0]);
    assert_eq!(read_rounds(&sends), [(NodeId(2), 1), (NodeId(3), 1)]);
    let reads = deliver(&mut rafts, &[1, 2], 1, sends, now);
    assert_eq!(reads.confirmed, [ReadIndex { id: 1, index: 1 }], "node 2 and the leader, two of three");
}

/// A read that comes in while a round is in flight waits for the next: that round may have
/// started before it. The next starts once the one in flight is confirmed.
#[test]
fn a_read_waits_for_the_round_after_the_one_in_flight() {
    let now = 1000 * MS;
    let mut rafts = led_and_committed(now);
    rafts[0].read_index(1);
    let first = flush(&mut rafts[0]);
    rafts[0].read_index(2);
    assert_eq!(read_rounds(&flush(&mut rafts[0])), [], "no round while one is in flight");
    for (to, append) in first {
        let follower = &mut rafts[usize::from(to) - 1];
        follower.on_message(NodeId(1), &decode(&append), now);
        for (_, ack) in flush(follower) {
            rafts[0].on_message(to, &decode(&ack), now);
        }
    }
    let out = rafts[0].take_outputs();
    assert_eq!(out.confirmed_reads, [ReadIndex { id: 1, index: 1 }], "the second isn't, by the first round");
    assert_eq!(read_rounds(&out.appends), [(NodeId(2), 2), (NodeId(3), 2)], "the next round, at once");
    let reads = deliver(&mut rafts, &[1, 2, 3], 1, out.appends, now);
    assert_eq!(reads.confirmed, [ReadIndex { id: 2, index: 1 }]);
}

/// A leader cut off while the others elect another can't confirm a read: a majority refuses
/// its round's term. It learns of the new term and drops the read, for the client to ask again.
#[test]
fn a_deposed_leader_confirms_no_read_and_drops_it() {
    let now = 1000 * MS;
    let mut rafts = led_and_committed(now);
    elect(&mut rafts, &[2, 3], 2, 2 * now);
    assert_eq!(rafts[0].role(), Role::Leader, "it hasn't heard");
    assert!(rafts[0].read_index(9));
    let sends = flush(&mut rafts[0]);
    let reads = deliver(&mut rafts, &[1, 2, 3], 1, sends, 2 * now);
    assert_eq!(reads.confirmed, []);
    assert_eq!(reads.dropped, [9]);
    assert_eq!(rafts[0].role(), Role::Follower);
}

/// A peer's echo of a round the leader hasn't started counts for no more than its latest: it
/// can't confirm, in advance, a read yet to come.
#[test]
fn an_echo_of_a_round_not_started_confirms_no_later_read() {
    let now = 1000 * MS;
    let mut rafts = led_and_committed(now);
    let ack = wire::AppendAck {
        term: rafts[0].term(),
        success: 1,
        index: 1,
        snapshot_index: 0,
        protocol: 0,
        service_version: 0,
        prev_index: 1,
        read_round: 5,
    };
    let mut frame = Vec::new();
    codec::encode_frame(&ack, &mut frame).unwrap();
    rafts[0].on_message(NodeId(2), &decode(&frame), now);
    rafts[0].read_index(1);
    assert_eq!(rafts[0].take_outputs().confirmed_reads, [], "its round 1 not yet acked");
}

/// Only a leader takes reads, and only so many at once.
#[test]
fn reads_are_a_leaders_and_bounded() {
    let now = 1000 * MS;
    let mut rafts = led_and_committed(now);
    assert!(!rafts[1].read_index(1), "a follower");
    for id in 0..crate::widen(MAX_WAITING_READS) {
        assert!(rafts[0].read_index(id));
    }
    assert!(!rafts[0].read_index(0), "past MAX_WAITING_READS");
}

/// The value a quorum reaches is the quorum's-th highest, in any order; with too few values, or
/// more than a cluster has members, there's none.
#[test]
fn what_a_quorum_reaches_is_its_quorumth_highest() {
    assert_eq!(quorum_highest([3, 9, 5], 2), Some(5));
    assert_eq!(quorum_highest([9, 3, 5, 7, 1], 3), Some(5));
    assert_eq!(quorum_highest([4], 1), Some(4));
    assert_eq!(quorum_highest([4], 2), None, "too few");
    assert_eq!(quorum_highest([], 1), None);
    assert_eq!(quorum_highest([1; MAX_MEMBERS], MAX_MEMBERS), Some(1));
    assert_eq!(quorum_highest([1; MAX_MEMBERS + 1], 1), None, "more than a cluster's members");
}
