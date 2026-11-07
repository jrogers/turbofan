//! Three nodes over real TCP and real files, with a gateway client: messages answered once and in
//! order while every node in turn, the leader among them, is killed and restarted from disk, and
//! while a fourth joins and the first leaves; a fourth, a learner, promoted to a cluster of its
//! own once the three are lost, that they can't take back; the three, lost, restored from a
//! copy of a snapshot; a leader cut off from the others, over a proxy between the nodes,
//! replaced; and a node's capture of its log replayed offline.

use std::io;
use std::path::Path;
use std::time::Duration;

use turbofan::client::Event;
use turbofan::driver;
use turbofan::raft::{self, NodeId};
#[cfg(feature = "tls")]
use turbofan::tls::{ClientTls, NodeTls};
use turbofan::{Admin, AdminError, CloseReason, ClusterClient, Effect, capture};

#[allow(dead_code, reason = "the tests share it; these use only some of it")]
mod common;
use common::*;

/// Over a proxy between the nodes: the leader, cut off from the others, is replaced by one of
/// them, while a client's messages go on being answered; healed, it follows the new leader, and
/// catches up. The proxy's own check, too: its cut really cuts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_isolated_leader_is_replaced_and_follows_once_healed() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::proxied(dir.path(), |_| None);
    let admins: Vec<Admin> = cluster
        .configs
        .iter()
        .map(|c| {
            Admin::new(c.admin_listen.to_string(), SERVICE_ID).with_token(TOKEN).with_timeout(Duration::from_secs(10))
        })
        .collect();
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 50).await;
    let old = admins[0].status().await.unwrap().leader;
    let ids = [NodeId(1), NodeId(2), NodeId(3)];
    cluster.proxy.as_ref().unwrap().isolate(old, &ids);

    // Asked through another node, which redirects to the leader it knows.
    let other = &admins[usize::from(if old == NodeId(1) { NodeId(2) } else { NodeId(1) }) - 1];
    let mut new = old;
    for _ in 0..100 {
        if let Ok(status) = other.status().await
            && status.leader != old
        {
            new = status.leader;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert_ne!(new, old, "another node elected");
    exchange(&mut cluster, &mut client, 51, 100).await;

    cluster.proxy.as_ref().unwrap().heal();
    exchange(&mut cluster, &mut client, 101, 150).await;
    let mut caught_up = false;
    for _ in 0..100 {
        let status = other.status().await.unwrap();
        assert_eq!(status.leader, new, "the new leader stays");
        if status.lag(old) == Some(0) {
            caught_up = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(caught_up, "the old leader caught up");
}

/// A leader told to stop hands leadership over first: another leads, and it returns, well within
/// the election timeout the others would otherwise have waited out in silence. A long one (2 s),
/// so that a slow machine's delays can't be taken for a handover, nor a handover for an election.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leader_told_to_stop_hands_over_first() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut stops = Vec::new();
    let timeout = Duration::from_secs(2);
    for i in 0..3 {
        cluster.kill(i).await;
        cluster.configs[i].node.raft.election_timeout = timeout;
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        cluster.start_until(i, async move { drop(rx.await) });
        stops.push(Some(tx));
    }
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 50).await;
    let admins: Vec<String> = cluster.configs.iter().map(|c| c.admin_listen.to_string()).collect();
    let admin = |i: usize| Admin::new(admins[i].clone(), SERVICE_ID).with_token(TOKEN);
    let old = usize::from(admin(0).status().await.unwrap().leader) - 1;

    let started = std::time::Instant::now();
    stops[old].take().unwrap().send(()).unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(5), cluster.nodes[old].take().unwrap()).await;
    assert!(stopped.unwrap().unwrap().is_ok(), "stopped cleanly");
    // Asked until it names a leader: one elected the slow way takes an election timeout.
    let new = usize::from(admin((old + 1) % 3).status().await.unwrap().leader) - 1;
    let took = started.elapsed();
    assert_ne!(new, old, "another leads");
    assert!(took < timeout / 2, "led again in {took:?}");
    exchange(&mut cluster, &mut client, 51, 100).await;

    // A follower told to stop just stops.
    let follower = (0..3).find(|&i| i != old && i != new).unwrap();
    stops[follower].take().unwrap().send(()).unwrap();
    let stopped = tokio::time::timeout(Duration::from_secs(5), cluster.nodes[follower].take().unwrap()).await;
    assert!(stopped.unwrap().unwrap().is_ok());
    assert_eq!(usize::from(admin(new).status().await.unwrap().leader) - 1, new, "the leader stays");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_answers_every_message_once_through_failovers() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    // A client with the wrong credentials gets no session.
    let mut stranger =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"no".to_vec(), Duration::from_millis(500)).unwrap();
    let event = tokio::time::timeout(Duration::from_secs(10), stranger.recv()).await.unwrap();
    assert!(matches!(event, Some(Event::Closed { reason: CloseReason::Refused, .. })), "{event:?}");
    let after = tokio::time::timeout(Duration::from_secs(10), stranger.recv()).await.unwrap();
    assert!(after.is_none(), "the session is over: {after:?}");

    let anyone =
        Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID).with_timeout(Duration::from_secs(10));
    // Without the token, an operator is refused at once, not left to time out.
    for token in [&b""[..], b"let me out"] {
        let refused = anyone.clone().with_token(token).request_snapshot().await.unwrap_err();
        assert!(
            matches!(&refused, AdminError::Denied(e) if e.kind() == std::io::ErrorKind::PermissionDenied),
            "{token:?}: {refused:?}"
        );
    }
    // One too long for a hello is an error, not a panic.
    let long = anyone.clone().with_token([b'x'; 256]).request_snapshot().await.unwrap_err();
    assert!(matches!(long, AdminError::TokenTooLong), "{long:?}");
    let admin = anyone.with_token(TOKEN);
    let index = admin.request_snapshot().await.unwrap();
    assert!(index > 100, "a snapshot after the first hundred: {index}");

    // Handed over, the new leader answers at once.
    let leader = admin.transfer_leadership(None).await.unwrap();
    assert!((1..=3).contains(&leader.0));
    let mut next = 101;
    for i in 0..3 {
        cluster.kill(i).await;
        exchange(&mut cluster, &mut client, next, next + 99).await;
        cluster.start(i);
        next += 100;
    }
    // Everyone back: one more round, then a snapshot every node can take.
    exchange(&mut cluster, &mut client, next, next + 99).await;
    admin.request_snapshot().await.unwrap();

    // Shut down, every node has the same snapshot and stops; started again, the session goes on.
    let index = admin.shutdown().await.unwrap();
    for i in 0..3 {
        let node = cluster.nodes[i].take().unwrap();
        tokio::time::timeout(Duration::from_secs(5), node).await.expect("stopped").unwrap().unwrap();
        let snapshots = cluster.configs[i].data_dir.join("snapshots");
        let (snapshot, _) = turbofan::snapshot::load(&snapshots).unwrap().unwrap();
        assert_eq!(snapshot.index, index, "node {}", i + 1);
    }
    for i in 0..3 {
        cluster.start(i);
    }
    exchange(&mut cluster, &mut client, next + 100, next + 199).await;
}

/// A batch is applied and answered in order, read back a batch at a time, its `client_seq`s the
/// next ones: then a message sent alone follows on from it (the service checks each follows the
/// last).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batch_is_answered_in_order() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 10).await;
    let seqs = client.send_batch((11..=1000_u64).map(|k| k.to_string().into_bytes())).await.unwrap();
    assert_eq!(seqs, 11..1001);
    let (mut events, mut expected) = (Vec::new(), 11);
    while expected <= 1000 {
        let read = tokio::time::timeout(Duration::from_secs(20), client.recv_many(&mut events, 256)).await;
        assert!(read.expect("answered in time") > 0, "session open");
        for event in events.drain(..) {
            let Event::Message(payload) = event else { panic!("{event:?}") };
            assert_eq!(String::from_utf8(payload).unwrap(), expected.to_string(), "answered out of order");
            expected += 1;
        }
    }
    exchange(&mut cluster, &mut client, 1001, 1010).await;
}

/// A gateway's query sees every message it has had answered, without logging it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_query_sees_what_was_applied() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 10).await;
    let answer = tokio::time::timeout(Duration::from_secs(10), client.query(b"last".to_vec())).await;
    assert_eq!(answer.expect("answered in time"), Ok(b"10".to_vec()));
}

/// A fourth node joins, and the first leaves, while a client's messages are answered once each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_member_joins_and_another_leaves() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    cluster.start(3);
    let admin = Admin::new(cluster.configs[1].admin_listen.to_string(), SERVICE_ID)
        .with_token(TOKEN)
        .with_timeout(Duration::from_secs(20));
    let fourth = member(&cluster.addrs, NodeId(4));
    let added = match admin.add_member(&fourth).await {
        Ok(index) => index,
        Err(e) => panic!("adding node 4: {e:?}; stopped on their own: {:?}", cluster.stopped().await),
    };
    exchange(&mut cluster, &mut client, 101, 200).await;
    // Asked again, as after a lost answer: answered as made, at the change or a snapshot since.
    // Elsewhere, refused.
    let again = admin.add_member(&fourth).await.unwrap();
    assert!(again >= added, "already a member: {again} from {added}");
    let elsewhere = raft::Member { peer: "127.0.0.1:1".into(), ..fourth.clone() };
    let again = admin.add_member(&elsewhere).await.unwrap_err();
    assert!(matches!(again, AdminError::MembersRefused), "a member 4 elsewhere: {again:?}");

    admin.remove_member(NodeId(1)).await.unwrap();
    cluster.kill(0).await;
    exchange(&mut cluster, &mut client, 201, 300).await;
    // Two of the three that are left are a majority: one more can go down.
    cluster.kill(1).await;
    exchange(&mut cluster, &mut client, 301, 400).await;
}

/// A node's capture of its log, from its snapshot to its commit index, replayed offline into the
/// service, echoes what the cluster echoed after the snapshot, in order, and ends with the
/// service as the cluster had it: each message once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_capture_replays_what_the_cluster_applied() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    let addrs: Vec<String> = cluster.configs.iter().map(|c| c.admin_listen.to_string()).collect();
    let admin = |i: usize| Admin::new(&addrs[i], SERVICE_ID).with_token(TOKEN).with_timeout(Duration::from_secs(20));
    let asked = admin(0).request_snapshot().await.unwrap();
    exchange(&mut cluster, &mut client, 101, 200).await;

    // Any member: the one asked answers, of what it knows committed, which may lag the leader's,
    // from its latest snapshot, which may not yet be the one asked for (a quorum took that).
    let mut sent = 200;
    let (captured, echoed, echo) = loop {
        let captured = admin(1).capture().await.unwrap();
        let mut echoed = Vec::new();
        let replayed = capture::replay(Path::new(&captured.dir), Echo::default(), None, |_, effects| {
            for effect in effects {
                let Effect::Send(_, payload) = effect else { panic!("{effect:?}") };
                echoed.push(String::from_utf8(payload.clone()).unwrap().parse::<u64>().unwrap());
            }
        })
        .unwrap();
        if captured.snapshot_index >= asked && replayed.last.values().any(|&k| k == sent) {
            if !echoed.is_empty() {
                break (captured, echoed, replayed);
            }
            // Its latest snapshot took the last message, leaving the log nothing to replay: one
            // more, for it to hold.
            sent += 1;
            exchange(&mut cluster, &mut client, sent, sent).await;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    // From whichever snapshot is the node's latest (it takes its own every 50 entries): Echo
    // checks each message follows on from the state it restored.
    let first = echoed[0];
    assert_eq!(echoed, (first..=sent).collect::<Vec<_>>(), "echoed once each, in order");
    assert_eq!(echo.last.len(), 1, "one session: {:?}", echo.last);

    // Stopped short, at an entry before the last; past what's committed, refused. The entry
    // before the last may be the snapshot's own, leaving nothing to replay.
    let to = captured.to - 1;
    let mut last = captured.snapshot_index;
    capture::replay(Path::new(&captured.dir), Echo::default(), Some(to), |index, _| last = index).unwrap();
    assert_eq!(last, to);
    let past = capture::replay(Path::new(&captured.dir), Echo::default(), Some(captured.to + 1), |_, _| {});
    assert!(past.is_err(), "past its commit index");
}

/// A learner, promoted once its cluster is lost, leads a cluster of its own. The old cluster,
/// back, a majority of its own, can't take it back: of another epoch, it's refused, and a session
/// opened on the new cluster goes on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_promoted_learner_leads_a_cluster_of_its_own() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    cluster.start(3);
    let addrs: Vec<String> = cluster.configs.iter().map(|c| c.admin_listen.to_string()).collect();
    let admin = |i: usize| Admin::new(&addrs[i], SERVICE_ID).with_token(TOKEN).with_timeout(Duration::from_secs(20));
    let learner = raft::Member { learner: true, ..member(&cluster.addrs, NodeId(4)) };
    admin(1).add_member(&learner).await.unwrap();
    exchange(&mut cluster, &mut client, 101, 200).await;
    // Commits don't wait on a learner: wait for it to catch up, as an operator would, before the
    // cluster is lost. The leader reports every member.
    loop {
        let status = admin(1).status().await.unwrap();
        assert_eq!(status.members.len(), 4);
        assert!(status.members.iter().any(|m| m.id == NodeId(4) && m.learner));
        if status.lag(NodeId(4)) == Some(0) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let refused = admin(3).promote().await.unwrap_err();
    assert!(matches!(refused, AdminError::NotPromoted), "it hears its leader: {refused:?}");

    // The cluster is lost; once its leader has been silent a while, the learner is promoted.
    for i in 0..3 {
        cluster.kill(i).await;
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    admin(3).promote().await.unwrap();
    let fourth = vec![cluster.configs[3].client_listen.to_string()];
    let mut promoted = ClusterClient::connect(fourth, SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut promoted, 1, 100).await;

    // The old cluster comes back, three of its four, and elects a leader of its own; the new one
    // goes on as it was.
    for i in 0..3 {
        cluster.start(i);
    }
    tokio::time::sleep(Duration::from_secs(3)).await;
    exchange(&mut cluster, &mut promoted, 101, 200).await;
}

/// Backup and restore: a snapshot taken on demand and copied off a node restores the cluster,
/// lost whole, as a cluster of one of a new epoch, which grows with `add-member`. A client that
/// saw what came after the snapshot closes its session; the old nodes, back, can't take it over.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_cluster_is_restored_from_a_copy_of_a_snapshot() {
    let dir = tempfile::tempdir().unwrap();
    let mut cluster = Cluster::new(dir.path(), |_| None);
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    let addrs: Vec<String> = cluster.configs.iter().map(|c| c.admin_listen.to_string()).collect();
    let admin = |i: usize| Admin::new(&addrs[i], SERVICE_ID).with_token(TOKEN).with_timeout(Duration::from_secs(20));
    let asked = admin(0).request_snapshot().await.unwrap();
    // The newest snapshot in its directory, once it's the one asked for or later, as an operator
    // checks by its name: a quorum has it, this node perhaps only in a moment, and perhaps none
    // before it.
    let snapshots = cluster.configs[0].data_dir.join("snapshots");
    let newest = loop {
        let newest = std::fs::read_dir(&snapshots)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "snapshot"))
            .max();
        let index = |p: &Path| -> u64 { p.file_stem().unwrap().to_str().unwrap().parse().unwrap() };
        if let Some(newest) = newest.filter(|p| index(p) >= asked) {
            break newest;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    };
    let backup = dir.path().join("backup.snapshot");
    std::fs::copy(newest, &backup).unwrap();
    // Applied after the backup: lost with the cluster.
    exchange(&mut cluster, &mut client, 101, 150).await;
    for i in 0..3 {
        cluster.kill(i).await;
    }

    let mut restored = cluster.configs[0].clone();
    restored.data_dir = dir.path().join("restored");
    let (index, _) = driver::restore(&backup, &restored).unwrap();
    assert!(index >= asked, "the snapshot copied, {index}, is at least the one asked for, {asked}");
    let again = driver::restore(&backup, &restored).unwrap_err();
    assert_eq!(again.kind(), io::ErrorKind::AlreadyExists, "a data directory is never overwritten");
    cluster.configs[0] = restored;
    cluster.start(0);

    // The client saw 150 applied, the restored cluster has at most 100 of them: it can't go on.
    let closed = tokio::time::timeout(Duration::from_secs(20), client.recv()).await.unwrap();
    let Some(Event::Closed { reason: CloseReason::Diverged, unapplied }) = closed else { panic!("{closed:?}") };
    assert!((50..=150).contains(&unapplied), "{unapplied} the restored cluster never applied");
    let restored = vec![cluster.configs[0].client_listen.to_string()];
    let mut client = ClusterClient::connect(restored, SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    // The old cluster's other two come back, a majority of its three, and elect a leader of their
    // own; the restored one, of another epoch, goes on as it was, and grows.
    for i in 1..3 {
        cluster.start(i);
    }
    tokio::time::sleep(Duration::from_secs(2)).await;
    exchange(&mut cluster, &mut client, 101, 150).await;
    assert_eq!(admin(0).status().await.unwrap().members.len(), 1, "it alone");
    cluster.start(3);
    admin(0).add_member(&member(&cluster.addrs, NodeId(4))).await.unwrap();
    exchange(&mut cluster, &mut client, 151, 200).await;
    assert_eq!(admin(0).status().await.unwrap().members.len(), 2);
}

/// With TLS, nodes replicate to each other, and a client and an operator get through; a client
/// without TLS, an operator without a certificate, and one with a node's, don't.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cluster_over_tls_takes_only_who_it_should() {
    let dir = tempfile::tempdir().unwrap();
    let certs = dir.path().join("certs");
    std::fs::create_dir(&certs).unwrap();
    certify(&certs, "node");
    certify(&certs, "operator");
    let file = |name: &str| certs.join(name);
    let tls = NodeTls::new(file("node.pem"), file("node.key"), file("node-ca.pem"), file("operator-ca.pem"));
    let mut cluster = Cluster::new(dir.path(), |_| Some(tls.clone()));

    // A hundred messages committed: the peers took each other's appends.
    let gateway = ClientTls::load(&file("node-ca.pem"), None).unwrap();
    let mut client = ClusterClient::connect_tls(
        cluster.client_addrs(),
        SERVICE_ID,
        b"ok".to_vec(),
        Duration::from_millis(500),
        gateway,
    )
    .unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;
    let mut plain =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    assert!(tokio::time::timeout(Duration::from_secs(2), plain.recv()).await.is_err(), "no session without TLS");

    let admin = Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID)
        .with_token(TOKEN)
        .with_timeout(Duration::from_secs(10));
    let operator =
        |cert: &str, key: &str| ClientTls::load(&file("node-ca.pem"), Some((&file(cert), &file(key)))).unwrap();
    let ok = operator("operator.pem", "operator.key");
    assert!(admin.clone().with_tls(ok).request_snapshot().await.unwrap() > 100);
    // Refused at once, not left to time out.
    let refused = [ClientTls::load(&file("node-ca.pem"), None).unwrap(), operator("node.pem", "node.key")];
    for tls in refused {
        let started = std::time::Instant::now();
        let error = admin.clone().with_tls(tls).request_snapshot().await.unwrap_err();
        assert!(started.elapsed() < Duration::from_secs(5), "{error}");
        assert!(matches!(error, AdminError::Denied(_)), "{error:?}");
    }
}

/// With TLS on the admin listener only, peers replicate and a client gets through over plain
/// TCP; an operator gets through with its certificate, and not without TLS.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_on_the_admin_listener_alone() {
    let dir = tempfile::tempdir().unwrap();
    let certs = dir.path().join("certs");
    std::fs::create_dir(&certs).unwrap();
    certify(&certs, "node");
    certify(&certs, "operator");
    let file = |name: &str| certs.join(name);
    let tls = NodeTls::admin_only(file("node.pem"), file("node.key"), file("operator-ca.pem"));
    let mut cluster = Cluster::new(dir.path(), |_| Some(tls.clone()));

    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    let admin = Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID)
        .with_token(TOKEN)
        .with_timeout(Duration::from_secs(5));
    let operator = ClientTls::load(&file("node-ca.pem"), Some((&file("operator.pem"), &file("operator.key")))).unwrap();
    assert!(admin.clone().with_tls(operator).request_snapshot().await.unwrap() > 100);
    assert!(admin.request_snapshot().await.is_err(), "an operator without TLS");
}

/// Without an admin CA, an operator over TLS presents no certificate, and its token says who it
/// is: one with the token gets through, one without it doesn't.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_without_an_admin_ca_takes_operators_by_their_token() {
    let dir = tempfile::tempdir().unwrap();
    let certs = dir.path().join("certs");
    std::fs::create_dir(&certs).unwrap();
    certify(&certs, "node");
    let file = |name: &str| certs.join(name);
    let mut tls = NodeTls::admin_only(file("node.pem"), file("node.key"), file("node-ca.pem"));
    tls.admin_ca = None;
    let mut cluster = Cluster::new(dir.path(), |_| Some(tls.clone()));
    let mut client =
        ClusterClient::connect(cluster.client_addrs(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500)).unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    let no_cert = ClientTls::load(&file("node-ca.pem"), None).unwrap();
    let admin = Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID)
        .with_tls(no_cert)
        .with_timeout(Duration::from_secs(5));
    assert!(admin.clone().with_token(TOKEN).request_snapshot().await.unwrap() > 100);
    let error = admin.with_token(b"wrong").request_snapshot().await.unwrap_err();
    assert!(matches!(error, AdminError::Denied(_)), "{error:?}");
}

/// With `peer_ids`, each node's certificate names its node ID: nodes 1 and 2, whose do, replicate
/// to each other; node 3, given a certificate naming node 1, is taken by neither, nor reached.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn peer_ids_take_a_peer_only_as_the_node_its_certificate_names() {
    let dir = tempfile::tempdir().unwrap();
    let certs = dir.path().join("certs");
    std::fs::create_dir(&certs).unwrap();
    let names = [("node-1", &["node-1.turbofan"][..]), ("node-2", &["node-2.turbofan"][..])];
    certify_named(&certs, "node", &names);
    let file = |name: &str| certs.join(name);
    let tls = |id: NodeId| {
        // Node 3 with node 1's certificate, as if misissued or stolen.
        let cert = if id == NodeId(2) { "node-2" } else { "node-1" };
        let mut tls =
            NodeTls::new(file(&format!("{cert}.pem")), file(&format!("{cert}.key")), file("node-ca.pem"), file("x"));
        (tls.admin_ca, tls.peer_ids) = (None, true);
        Some(tls)
    };
    let mut cluster = Cluster::new(dir.path(), tls);

    let gateway = ClientTls::load(&file("node-ca.pem"), None).unwrap();
    let mut client = ClusterClient::connect_tls(
        cluster.client_addrs()[..2].to_vec(),
        SERVICE_ID,
        b"ok".to_vec(),
        Duration::from_millis(500),
        gateway.clone(),
    )
    .unwrap();
    exchange(&mut cluster, &mut client, 1, 100).await;

    let admin = Admin::new(cluster.configs[0].admin_listen.to_string(), SERVICE_ID)
        .with_token(TOKEN)
        .with_tls(gateway)
        .with_timeout(Duration::from_secs(10));
    let status = admin.status().await.unwrap();
    let matched = |id| status.members.iter().find(|m| m.id == id).unwrap().matched;
    assert!(status.leader != NodeId(3));
    assert!(matched(NodeId(1)) > 100 && matched(NodeId(2)) > 100, "{status:?}");
    assert_eq!(matched(NodeId(3)), 0, "node 3 was reached");
}

/// A certificate and CA rewritten in place are loaded again: after the node's files are swapped
/// for a new CA's, a client trusting only the new CA gets through, and one trusting only the old
/// doesn't.
#[cfg(feature = "tls")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tls_files_rewritten_are_loaded_again() {
    let dir = tempfile::tempdir().unwrap();
    let (certs, rotated) = (dir.path().join("certs"), dir.path().join("rotated"));
    std::fs::create_dir(&certs).unwrap();
    std::fs::create_dir(&rotated).unwrap();
    certify(&certs, "node");
    certify(&rotated, "node");
    let file = |name: &str| certs.join(name);
    let mut tls = NodeTls::new(file("node.pem"), file("node.key"), file("node-ca.pem"), file("node-ca.pem"));
    tls.reload_every = Duration::from_millis(100);
    let mut cluster = Cluster::new(dir.path(), |_| Some(tls.clone()));
    let addrs = cluster.client_addrs();
    let client = |ca: &Path| {
        let tls = ClientTls::load(ca, None).unwrap();
        ClusterClient::connect_tls(addrs.clone(), SERVICE_ID, b"ok".to_vec(), Duration::from_millis(500), tls).unwrap()
    };
    let old_ca = dir.path().join("old-ca.pem");
    std::fs::copy(file("node-ca.pem"), &old_ca).unwrap();
    let mut before = client(&old_ca);
    exchange(&mut cluster, &mut before, 1, 50).await;

    for name in ["node.pem", "node.key", "node-ca.pem"] {
        std::fs::copy(rotated.join(name), file(name)).unwrap();
    }
    tokio::time::sleep(Duration::from_secs(1)).await;
    let mut after = client(&file("node-ca.pem"));
    exchange(&mut cluster, &mut after, 1, 50).await;
    let mut stale = client(&old_ca);
    assert!(tokio::time::timeout(Duration::from_secs(2), stale.recv()).await.is_err(), "the old CA still taken");
}
