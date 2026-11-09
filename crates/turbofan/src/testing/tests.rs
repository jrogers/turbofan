use std::io::Read;

use super::*;
use crate::node::{Ctx, SnapshotWriter};

const MS: Duration = Duration::from_millis(1);

/// Keeps every message it's sent, and answers each with how many it has; a message `timer`
/// sets a timer that answers `fired` a millisecond later.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
struct Tally {
    messages: Vec<Vec<u8>>,
    /// The session each timer answers, by its ID.
    timers: BTreeMap<u64, SessionId>,
    /// The version of its behaviour it has, and those the cluster switched to.
    version: u32,
    activated: Vec<u32>,
}

impl Service for Tally {
    fn version(&self) -> u32 {
        self.version
    }

    fn on_activate(&mut self, _ctx: &mut Ctx, version: u32) {
        self.activated.push(version);
    }

    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        self.messages.push(payload.to_vec());
        if payload == b"timer" {
            let id = crate::widen(self.messages.len());
            self.timers.insert(id, session);
            ctx.schedule_timer(id, ctx.now() + raft::nanos(MS));
        }
        ctx.send(session, self.messages.len().to_string()).expect("a short answer");
    }

    fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
        let session = self.timers.remove(&id).expect("a timer it set");
        ctx.send(session, "fired").expect("a short answer");
    }

    /// How many messages it has.
    fn on_query(&self, _session: SessionId, _query: &[u8]) -> Option<Vec<u8>> {
        Some(self.messages.len().to_string().into_bytes())
    }

    fn snapshot(&self) -> SnapshotWriter {
        let messages = self.messages.clone();
        Box::new(move |out| {
            for message in &messages {
                out.write_all(&u32::try_from(message.len()).unwrap().to_le_bytes())?;
                out.write_all(message)?;
            }
            Ok(())
        })
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let mut rest = &bytes[..];
        self.messages.clear();
        while !rest.is_empty() {
            let len = usize::try_from(u32::from_le_bytes(rest[..4].try_into().unwrap())).unwrap();
            self.messages.push(rest[4..4 + len].to_vec());
            rest = &rest[4 + len..];
        }
        Ok(())
    }
}

/// The answers `events` hold, as text, after the session opened.
fn answers(events: &[Event]) -> Vec<String> {
    let opened = events.iter().filter(|e| matches!(e, Event::Opened(_))).count();
    assert!(opened <= 1, "opened once: {events:?}");
    let answer = |e: &Event| match e {
        Event::Message(m) => Some(String::from_utf8(m.clone()).unwrap()),
        _ => None,
    };
    events.iter().filter_map(answer).collect()
}

/// Every node that's up has applied the same messages, `count` of them.
fn applied_everywhere(cluster: &Cluster<Tally>, count: usize) -> bool {
    cluster.ids().iter().all(|&id| cluster.service(id).is_none_or(|s| s.messages.len() == count))
}

fn numbered(from: usize, to: usize) -> Vec<Vec<u8>> {
    (from..to).map(|k| k.to_string().into_bytes()).collect()
}

/// A client's messages are applied once and in order on every node, and each answered once.
#[test]
fn messages_are_applied_on_every_node_and_answered() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    cluster.send_batch(alice, numbered(0, 3)).unwrap();
    assert!(cluster.run_until(Duration::from_secs(1), |c| applied_everywhere(c, 3) && c.client(alice).unacked() == 0));
    let answered = |c: &Cluster<Tally>| c.peek_events(alice).filter(|e| matches!(e, Event::Message(_))).count() == 3;
    assert!(cluster.run_until(Duration::from_secs(1), answered));
    assert_eq!(cluster.peek_events(alice).count(), 4, "peeked, not taken");
    let events = cluster.events(alice);
    assert!(matches!(events[0], Event::Opened(_)), "{events:?}");
    assert_eq!(answers(&events), ["1", "2", "3"]);
    assert!(cluster.events(alice).is_empty(), "taken");
    assert_eq!(cluster.service(NodeId(1)).unwrap().messages, numbered(0, 3));
}

/// The leader crashes with messages in flight: a new one is elected, the client resends on its
/// session, and nothing is lost or applied twice; the old leader, restarted, catches up.
#[test]
fn a_failover_loses_nothing_and_applies_nothing_twice() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    cluster.send_batch(alice, numbered(0, 10)).unwrap();
    cluster.run_for(MS / 4);
    let old = cluster.leader().unwrap();
    cluster.crash(old);
    assert!(cluster.metrics(old).is_none());
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.leader().is_some_and(|l| l != old)));
    cluster.send_batch(alice, numbered(10, 20)).unwrap();
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.client(alice).unacked() == 0));
    let expected: Vec<String> = (1..=20).map(|k| k.to_string()).collect();
    assert_eq!(answers(&cluster.events(alice)), expected);

    cluster.restart(old);
    assert!(cluster.run_until(Duration::from_secs(2), |c| applied_everywhere(c, 20)));
    assert_eq!(cluster.service(old).unwrap().messages, numbered(0, 20));
}

/// A follower that crashed starts again from its snapshot and log, and one cut off while the
/// leader compacted past it is sent the leader's snapshot: both end with the same state.
#[test]
fn nodes_behind_catch_up_from_snapshots() {
    let mut cluster = Builder::new(3)
        .configure(|config| {
            config.snapshot_every_entries = 10;
            config.raft.log_tail_entries = 0;
        })
        .build(|_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    let leader = cluster.leader().unwrap();
    let mut followers = cluster.ids().into_iter().filter(|&id| id != leader);
    let (crashed, cut_off) = (followers.next().unwrap(), followers.next().unwrap());
    cluster.send_batch(alice, numbered(0, 25)).unwrap();
    assert!(cluster.run_until(Duration::from_secs(1), |c| applied_everywhere(c, 25)));
    cluster.run_for(10 * MS);
    assert!(cluster.metrics(crashed).unwrap().snapshot_index > 0, "it snapshotted");

    cluster.crash(crashed);
    cluster.restart(crashed);
    assert_eq!(cluster.service(crashed).unwrap().messages.len(), 25, "from its snapshot and its log");

    cluster.isolate(cut_off);
    cluster.send_batch(alice, numbered(25, 60)).unwrap();
    assert!(cluster.run_until(Duration::from_secs(1), |c| c.service(leader).unwrap().messages.len() == 60));
    cluster.run_for(10 * MS);
    let behind = cluster.metrics(cut_off).unwrap().last_index;
    assert!(cluster.metrics(leader).unwrap().snapshot_index > behind, "compacted past it");
    cluster.heal();
    assert!(cluster.run_until(Duration::from_secs(2), |c| applied_everywhere(c, 60)));
    assert!(cluster.metrics(cut_off).unwrap().snapshot_index > behind, "sent the leader's");
    assert_eq!(cluster.service(cut_off), cluster.service(leader));
}

/// A timer the service sets fires once, on the cluster's clock.
#[test]
fn a_timer_fires_on_the_clusters_clock() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    cluster.send(alice, "timer").unwrap();
    assert!(cluster.run_until(Duration::from_secs(1), |c| c.client(alice).unacked() == 0));
    let before = answers(&cluster.events(alice));
    assert_eq!(before, ["1"], "not yet fired");
    cluster.run_for(10 * MS);
    assert_eq!(answers(&cluster.events(alice)), ["fired"]);
    // Followers apply it once a heartbeat brings them the commit.
    cluster.run_for(100 * MS);
    assert!(cluster.ids().iter().all(|&id| cluster.service(id).unwrap().timers.is_empty()));
}

/// The leader hands over to the node asked, and a client follows it.
#[test]
fn the_leadership_is_handed_over() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    let to = cluster.ids().into_iter().find(|&id| Some(id) != cluster.leader()).unwrap();
    assert!(cluster.transfer_leadership(Some(to)));
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.leader() == Some(to)));
    cluster.send(alice, "after").unwrap();
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.client(alice).unacked() == 0));
    assert_eq!(answers(&cluster.events(alice)), ["1"]);
}

/// The same seed, the same run: leaders, times and answers alike.
#[test]
fn a_seed_replays_a_run() {
    let run = |seed| {
        let mut cluster = Builder::new(3).seed(seed).build(|_| Tally::default());
        let alice = cluster.connect(b"alice").unwrap();
        cluster.send_batch(alice, numbered(0, 5)).unwrap();
        cluster.run_for(MS / 4);
        let first = cluster.leader().unwrap();
        cluster.crash(first);
        cluster.run_until(Duration::from_secs(2), |c| c.client(alice).unacked() == 0);
        (first, cluster.leader(), cluster.now(), answers(&cluster.events(alice)))
    };
    assert_eq!(run(7), run(7));
    assert_eq!(run(8), run(8));
}

/// Waits for `client`'s next answer.
fn answer(cluster: &mut Cluster<Tally>, client: ClientId) -> Answer {
    let mut answers = Vec::new();
    let answered = cluster.run_until(Duration::from_secs(5), |c| c.client(client).queries() == 0);
    assert!(answered, "a query answered in time");
    answers.extend(cluster.answers(client));
    assert_eq!(answers.len(), 1, "{answers:?}");
    answers.remove(0)
}

/// The leader answers a query from the messages applied, logging nothing.
#[test]
fn a_query_is_answered_from_what_is_applied() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    cluster.send_batch(alice, [b"a", b"b", b"c"]).unwrap();
    assert!(cluster.run_until(Duration::from_secs(1), |c| c.client(alice).applied() == 3));
    let leader = cluster.leader().unwrap();
    let last = cluster.metrics(leader).unwrap().last_index;
    let id = cluster.query(alice, b"count").unwrap();
    assert_eq!(answer(&mut cluster, alice), Answer { query: id, answer: Ok(b"3".to_vec()) });
    assert_eq!(cluster.metrics(leader).unwrap().last_index, last, "nothing logged");
}

/// A query in flight as the leader crashes is asked again of the new one, which answers it.
#[test]
fn a_query_in_flight_through_a_failover_is_answered_by_the_new_leader() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    cluster.send_batch(alice, [b"a", b"b"]).unwrap();
    assert!(cluster.run_until(Duration::from_secs(1), |c| c.client(alice).applied() == 2));
    let old = cluster.leader().unwrap();
    let id = cluster.query(alice, b"count").unwrap();
    cluster.crash(old);
    assert_eq!(answer(&mut cluster, alice), Answer { query: id, answer: Ok(b"2".to_vec()) });
    assert_ne!(cluster.leader(), Some(old));
}

/// A node is added and made a voter, and the leader removed: the new node leads, the clients
/// following it there, and has every message.
#[test]
fn a_member_is_added_and_the_leader_removed() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    cluster.send_batch(alice, numbered(0, 5)).unwrap();
    assert!(cluster.run_until(Duration::from_secs(1), |c| c.client(alice).unacked() == 0));
    assert_eq!(answers(&cluster.events(alice)), ["1", "2", "3", "4", "5"]);
    let new = NodeId(4);
    let is_member = |c: &Cluster<Tally>, id, learner| {
        c.members().is_some_and(|m| m.iter().any(|m| m.id == id && m.learner == learner))
    };
    assert!(cluster.add_member(new, true));
    assert!(cluster.run_until(Duration::from_secs(2), |c| is_member(c, new, true)));
    assert_eq!(cluster.ids(), [NodeId(1), NodeId(2), NodeId(3), new]);
    assert!(cluster.set_role(new, false));
    assert!(cluster.run_until(Duration::from_secs(2), |c| is_member(c, new, false)));

    let old = cluster.leader().unwrap();
    assert!(cluster.remove_member(old));
    let gone = |c: &Cluster<Tally>| c.members().is_some_and(|m| m.len() == 3 && m.iter().all(|m| m.id != old));
    assert!(cluster.run_until(Duration::from_secs(2), gone));
    cluster.crash(old);
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.leader().is_some_and(|l| l != old)));
    assert!(cluster.transfer_leadership(Some(new)));
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.leader() == Some(new)));
    cluster.send_batch(alice, numbered(5, 10)).unwrap();
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.client(alice).unacked() == 0));
    assert_eq!(answers(&cluster.events(alice)), ["6", "7", "8", "9", "10"], "on the same session");
    assert!(cluster.run_until(Duration::from_secs(1), |c| applied_everywhere(c, 10)));
    assert_eq!(cluster.service(new).unwrap().messages, numbered(0, 10));
}

/// A rolling upgrade: the new version is turned down while a member runs the old, and
/// activated on every node once all run the new; a node restarting after keeps it.
#[test]
fn a_rolling_upgrade_activates_the_new_version() {
    let mut cluster = Cluster::new(3, |_| Tally::default());
    let alice = cluster.connect(b"alice").unwrap();
    let v1 = |_| Tally { version: 1, ..Tally::default() };
    let ids = cluster.ids();
    for &id in &ids[..2] {
        cluster.upgrade(id, v1);
        assert!(cluster.run_until(Duration::from_secs(2), |c| c.leader().is_some()));
    }
    cluster.run_for(100 * MS);
    assert!(cluster.activate(1));
    cluster.run_for(100 * MS);
    assert!(ids.iter().all(|&id| cluster.service(id).unwrap().activated.is_empty()), "one runs the old");

    cluster.upgrade(ids[2], v1);
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.leader().is_some()));
    cluster.run_for(100 * MS);
    assert!(cluster.activate(1));
    let activated = |c: &Cluster<Tally>| ids.iter().all(|&id| c.service(id).unwrap().activated == [1]);
    assert!(cluster.run_until(Duration::from_secs(1), activated));
    cluster.crash(ids[0]);
    cluster.restart(ids[0]);
    assert_eq!(cluster.service(ids[0]).unwrap().version, 1, "upgraded for good");
    cluster.send(alice, "after").unwrap();
    assert!(cluster.run_until(Duration::from_secs(2), |c| c.client(alice).unacked() == 0));
}
