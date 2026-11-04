//! A deterministic simulation of a cluster and its clients. One seed drives every delay, drop,
//! partition and crash, and invariants are checked after every event. A failure names its
//! seed, and `SIM_SEED=<seed> cargo test --test sim` replays it exactly. `SIM_SEEDS=<n>` runs
//! more seeds, from `SIM_BASE`.
//!
//! Faults: peer messages delayed, reordered, dropped and duplicated; partitions; client connections
//! reset; nodes crashing between events, as leader with appends in flight, or partway through
//! persisting (a prefix of the operations being written, and of their records, reaching the disk,
//! the next record torn), then restarting from what's on disk; nodes persisting as the driver
//! does, batches taken while one is written queued behind it and written together, and inputs
//! handled meanwhile; slow disks, what waits for a write late; and log records and snapshot files
//! rotting on disk; a crash tearing a record with the rest of its append whole after it, as a disk
//! keeping a later page and losing an earlier does. A node that finds its disk damaged as it runs
//! stops, as the driver does, and is replaced: removed from the members, wiped, and started again
//! to join, as an operator would. On restart, a log is cut at a rotten or torn record, as the log
//! does, and the node doubts it; if that leaves no leader to replace a broken member with, the
//! operator ends the doubt of nodes that only lost power.
//!
//! Each node's log, meta and snapshots are real files, as the driver writes them, the log in small
//! segments so they roll and compact. Nodes snapshot every few hundred entries and when an operator
//! asks, compacting their logs, so restarts recover from snapshots and lagging followers are sent
//! one. A snapshot is written some time after it's taken, the node applying more meanwhile, or lost
//! in a crash; every snapshot of an index must be the same bytes on every node. Nodes keep few or
//! no applied records in memory, per seed, so lagging followers and restarts read them back from
//! disk.
//!
//! Seven nodes run, three or five of them the first members. An operator adds and removes
//! members now and then, between three and seven of them, the leader among those removed and
//! removed ones among those added back, some as learners, the voters kept to three or more. And hands the leadership over now and then. The nodes
//! start on an old version and are upgraded one by one, restarted on the new; the operator asks
//! to activate the new version until it's let, which mustn't be while any member is old. Once the
//! faults stop, it shuts the cluster down, and every member stops with the same snapshot; then
//! they're all started again.
//!
//! Then the cluster is lost, with the clients sending, and recovered as an operator would: by
//! promoting a learner, cut off for a while first (as one in another region is) or not at all, or
//! by restoring a copy of a snapshot taken a while before onto a node of its own. Each client
//! either carries on, every message and response once and in order as before, or closes its
//! session as `Diverged`, which it must exactly when the new cluster lacks some of what it saw:
//! then it opens another, as a gateway would. One client goes idle shortly before, so the new
//! cluster has all its messages, but perhaps not the notifications others' lost messages caused
//! it: it diverges by its responses alone.
//!
//! The service is an echo with timers, which notifies the first session of some of the others'
//! messages. Clients send numbered messages through failovers, and
//! must see every message applied once and in order, every response once and in order, every
//! timer fire once, and every message the service proposed applied once.

use std::cmp::Reverse;
use std::collections::{BTreeMap, BTreeSet, BinaryHeap, VecDeque};
use std::io::{self, Read};
use std::time::Duration;

use turbofan_cluster::client::{self, Client};
use turbofan_cluster::codec::{self, Record};
use turbofan_cluster::log::{Log, MAX_SEGMENT_BYTES};
use turbofan_cluster::meta::{Meta, MetaFile};
use turbofan_cluster::node::{self, ConnId, Node, SaveSnapshot};
use turbofan_cluster::pipeline::{Batch, Pipeline};
use turbofan_cluster::raft::{
    Entries, MAX_MEMBERS, MAX_READ_BYTES, Member, NodeId, Persist, Role, Snapshot, SnapshotRead,
};
use turbofan_cluster::{CloseReason, Ctx, Service, SessionId, SnapshotWriter, driver, snapshot};

const MS: u64 = 1_000_000;
/// Marks an admin request's ID as a `SetRole`'s, to count its answer.
const ROLE_REQUEST: u64 = 1 << 63;
const FAULT_PHASE: u64 = 20_000 * MS;
const QUIET_PHASE: u64 = 10_000 * MS;
/// After the cluster is recovered from its loss and the clients stop sending, for it to settle.
const DISASTER_QUIET: u64 = 3000 * MS;
const CLIENTS: usize = 3;
/// Every this many messages, the echo service sets a timer.
const TIMER_EVERY: u64 = 7;
/// Every this many messages, the echo service proposes one of its own, which sends `p<k>`.
const PROPOSE_EVERY: u64 = 5;
const TIMER_SPACE: u64 = 1_000_000;
/// Every this many messages, the echo service notifies the first session of it, `n<k>`: a
/// response one session's message causes another, as an order's fill does.
const NOTIFY_EVERY: u64 = 11;
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
    let mut covered = Coverage::default();
    let several = seeds.len() > 1;
    for seed in seeds {
        let result = std::panic::catch_unwind(|| Sim::new(seed).run());
        match result {
            Ok(coverage) => covered.add(&coverage),
            Err(panic) => {
                eprintln!("simulation failed: SIM_SEED={seed}");
                std::panic::resume_unwind(panic);
            }
        }
    }
    // What a seed may by chance not get to, every run of several does.
    if several {
        assert!(covered.manual_snapshots > 0, "no manual snapshot was answered");
        assert!(covered.member_changes > 0, "the members never changed");
        assert!(covered.learners > 0, "no learner was ever a member");
        assert!(covered.role_changes > 0, "no member's role was ever changed");
        assert!(covered.transfers > 0, "the leadership was never handed over");
        assert!(covered.activations > 0, "a new version was never activated");
        assert!(covered.shutdowns > 0, "the cluster was never shut down");
        assert!(covered.torn > 0, "no crash tore an append");
        assert!(covered.torn_mid_batch > 0, "no crash tore an append with records whole after the tear");
        assert!(covered.rot_cut > 0, "no rotten record was cut from a log with records after it");
        assert!(covered.torn_resets > 0, "no crash interrupted a snapshot replacing the log");
        assert!(covered.segments_compacted > 0, "no log segment was compacted");
        assert!(covered.promotions > 0, "a lost cluster was never recovered by promotion");
        assert!(covered.restores > 0, "a lost cluster was never restored from a snapshot");
        assert!(covered.diverged > 0, "no client's session diverged");
        assert!(covered.carried_on > 0, "no client carried on through a recovery");
        assert!(covered.lost_responses > 0, "no client's session diverged by its responses alone");
    }
}

/// What a seed got to do, of what it tries.
#[derive(Debug, Default)]
struct Coverage {
    manual_snapshots: u64,
    member_changes: u64,
    /// Changes of members made while a learner was one.
    learners: u64,
    /// Members made a learner or a voter in place.
    role_changes: u64,
    transfers: u64,
    activations: u64,
    shutdowns: u64,
    /// Appends a crash tore, the log's tail left partway through a record.
    torn: u64,
    /// Of them, those with whole records after the torn one.
    torn_mid_batch: u64,
    /// Logs cut at a rotten record on restart, with records after it.
    rot_cut: u64,
    /// Crashes installing a leader's snapshot in place of the log, before or partway through
    /// the reset.
    torn_resets: u64,
    /// Times a log's first segment was removed.
    segments_compacted: u64,
    /// Lost clusters recovered by promoting a learner, and by restoring a snapshot.
    promotions: u64,
    restores: u64,
    /// Clients that closed their sessions as `Diverged` on the recovered cluster, and that
    /// carried on with them.
    diverged: u64,
    carried_on: u64,
    /// Of those that diverged, ones whose cluster had all their messages, but not all the
    /// responses they saw.
    lost_responses: u64,
}

impl Coverage {
    fn add(&mut self, other: &Self) {
        self.manual_snapshots += other.manual_snapshots;
        self.member_changes += other.member_changes;
        self.learners += other.learners;
        self.role_changes += other.role_changes;
        self.transfers += other.transfers;
        self.activations += other.activations;
        self.shutdowns += other.shutdowns;
        self.torn += other.torn;
        self.torn_mid_batch += other.torn_mid_batch;
        self.rot_cut += other.rot_cut;
        self.torn_resets += other.torn_resets;
        self.segments_compacted += other.segments_compacted;
        self.promotions += other.promotions;
        self.restores += other.restores;
        self.diverged += other.diverged;
        self.carried_on += other.carried_on;
        self.lost_responses += other.lost_responses;
    }
}

/// Echoes `a<k>` for message `k`, every [`TIMER_EVERY`]th sets a timer that sends `t<k>`,
/// every [`PROPOSE_EVERY`]th proposes a message of its own that sends `p<k>`, and every
/// [`NOTIFY_EVERY`]th of another session's notifies the first session, `n<k>`.
/// Panics if a session's messages arrive other than once each, in order.
#[derive(Debug, Default)]
struct Echo {
    last: BTreeMap<SessionId, u64>,
    /// Responses sent to each session: its last `egress_seq`, while it's open.
    responses: BTreeMap<SessionId, u64>,
    fired: u64,
    /// Messages applied as version 1, which only new code has.
    as_new: u64,
    /// The version this code has: 0 old, 1 new. Not state.
    version: u32,
}

/// The state, not which version of the code holds it.
impl PartialEq for Echo {
    fn eq(&self, other: &Self) -> bool {
        (&self.last, &self.responses, self.fired, self.as_new)
            == (&other.last, &other.responses, other.fired, other.as_new)
    }
}

impl Service for Echo {
    /// Each client is its own principal: every reattach is checked against it.
    fn authenticate(&self, credentials: &[u8]) -> Option<Vec<u8>> {
        Some(credentials.to_vec())
    }

    fn version(&self) -> u32 {
        self.version
    }

    fn on_activate(&mut self, _ctx: &mut Ctx, version: u32) {
        assert!(version <= self.version, "old code activated as version {version}");
    }

    fn on_message(&mut self, ctx: &mut Ctx, session: SessionId, payload: &[u8]) {
        assert!(ctx.service_version() <= self.version, "old code run as version {}", ctx.service_version());
        self.as_new += u64::from(ctx.service_version() >= 1);
        let k: u64 = std::str::from_utf8(payload).unwrap().parse().unwrap();
        let last = self.last.entry(session).or_default();
        assert_eq!(k, *last + 1, "session {session}: message {k} applied after {last}");
        *last = k;
        self.send(ctx, session, format!("a{k}"));
        if k.is_multiple_of(PROPOSE_EVERY) {
            ctx.propose(format!("{session} {k}").as_bytes()).unwrap();
        }
        if k.is_multiple_of(NOTIFY_EVERY)
            && let Some(&first) = self.last.keys().next()
            && first != session
        {
            self.send(ctx, first, format!("n{k}"));
        }
        if k.is_multiple_of(TIMER_EVERY) {
            ctx.schedule_timer(session.0 * TIMER_SPACE + k, ctx.now() + 30 * MS);
        }
    }

    fn on_proposal(&mut self, ctx: &mut Ctx, payload: &[u8]) {
        let text = std::str::from_utf8(payload).unwrap();
        let (session, k) = text.split_once(' ').unwrap();
        self.send(ctx, SessionId(session.parse().unwrap()), format!("p{k}"));
    }

    fn on_timer(&mut self, ctx: &mut Ctx, id: u64) {
        self.fired += 1;
        self.send(ctx, SessionId(id / TIMER_SPACE), format!("t{}", id % TIMER_SPACE));
    }

    /// Each session's last message and responses, and the two counts, as words.
    fn snapshot(&self) -> SnapshotWriter {
        let (fired, as_new) = (self.fired, self.as_new);
        let sessions: BTreeSet<SessionId> = self.last.keys().chain(self.responses.keys()).copied().collect();
        let rows: Vec<[u64; 3]> = sessions
            .into_iter()
            .map(|s| [s.0, self.last.get(&s).copied().unwrap_or(0), self.responses.get(&s).copied().unwrap_or(0)])
            .collect();
        Box::new(move |out| {
            out.write_all(&fired.to_le_bytes())?;
            out.write_all(&as_new.to_le_bytes())?;
            for word in rows.iter().flatten() {
                out.write_all(&word.to_le_bytes())?;
            }
            Ok(())
        })
    }

    fn restore(&mut self, from: &mut dyn Read) -> io::Result<()> {
        let mut bytes = Vec::new();
        from.read_to_end(&mut bytes)?;
        let words: Vec<u64> = bytes.as_chunks::<8>().0.iter().map(|w| u64::from_le_bytes(*w)).collect();
        (self.fired, self.as_new) = (words[0], words[1]);
        let rows = words[2..].as_chunks::<3>().0;
        self.last = rows.iter().filter(|r| r[1] > 0).map(|&[session, last, _]| (SessionId(session), last)).collect();
        self.responses = rows.iter().filter(|r| r[2] > 0).map(|&[session, _, n]| (SessionId(session), n)).collect();
        Ok(())
    }
}

impl Echo {
    fn send(&mut self, ctx: &mut Ctx, session: SessionId, text: String) {
        *self.responses.entry(session).or_default() += 1;
        ctx.send(session, text.into_bytes()).unwrap();
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
    Deliver {
        from: NodeId,
        to: NodeId,
        frame: Vec<u8>,
    },
    Tick(NodeId),
    Restart(NodeId),
    Partition,
    Heal,
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
    ClientSend(usize),
    AdminRequest,
    ChangeMembers,
    TransferLeadership,
    /// A node is upgraded: restarted on the new version.
    Upgrade,
    /// The operator asks to switch to the new version.
    Activate,
    /// A snapshot file rots.
    Rot,
    /// The operator replaces a node whose disk is damaged.
    Replace(NodeId),
    Shutdown,
    /// Every node is started again, after a shutdown.
    StartAll,
    /// The operator copies the newest snapshot off a member, to restore from.
    Backup,
    /// The standby, a learner, is cut off from the cluster: it falls behind.
    CutStandby,
    /// Every node but the standby is lost, for good.
    Lose,
    /// The operator promotes the standby, or restores the backup.
    Recover,
    /// An operator's request again, redirected or with no leader to send it to.
    AdminRetry {
        frame: Vec<u8>,
        tries: u32,
    },
    /// A node's write is done, unless it crashed since.
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
}

/// What a node has durably written, in real files written and read as the driver does: its log
/// by [`Log`], its meta by [`MetaFile`], its snapshots by [`turbofan_cluster::snapshot`]. Writes
/// aren't synced (crashes are modelled, not made), and `meta` and `log` are what was written, to
/// check the files against: a restart must recover exactly that, less a torn tail.
struct Disk {
    /// Where the log and meta are.
    dir: tempfile::TempDir,
    /// The log and meta file, while the node that opened them runs.
    files: Option<(Log, MetaFile)>,
    segment_bytes: u64,
    meta: Meta,
    /// Where the snapshot files are.
    snapshots: tempfile::TempDir,
    /// The latest snapshot, as [`snapshot::load`] finds it.
    snapshot: Option<Snapshot>,
    /// The index of a snapshot of its that rotted, while it's the latest.
    rotten: Option<u64>,
    /// The index of a record that rotted, while the log holds it.
    rotten_record: Option<u64>,
    /// The log doubts itself for a rotten last record it dropped, not only a torn write: it's
    /// damaged until it's trusted again.
    rot_doubt: bool,
    /// The index of `log[0]`: the log's first segment's.
    first: u64,
    /// The records from `first`.
    log: Vec<Vec<u8>>,
}

impl Disk {
    fn new(segment_bytes: u64) -> Self {
        let (dir, snapshots) = (tempfile::tempdir().unwrap(), tempfile::tempdir().unwrap());
        let (meta, snapshot, rotten, rotten_record) = (Meta::default(), None, None, None);
        Self {
            rot_doubt: false,
            dir,
            files: None,
            segment_bytes,
            meta,
            snapshots,
            snapshot,
            rotten,
            rotten_record,
            first: 1,
            log: Vec::new(),
        }
    }

    /// What [`driver::restore`] wrote in `dir`, as a node's disk: the snapshot, the meta, and no
    /// log yet.
    fn restored(dir: tempfile::TempDir, segment_bytes: u64) -> Self {
        let snapshots = tempfile::tempdir().unwrap();
        for entry in std::fs::read_dir(dir.path().join("snapshots")).unwrap() {
            let path = entry.unwrap().path();
            std::fs::rename(&path, snapshots.path().join(path.file_name().unwrap())).unwrap();
        }
        let (snapshot, _) = snapshot::load(snapshots.path()).unwrap().expect("restored");
        let (_, meta) = MetaFile::open_simulated(&dir.path().join("meta")).unwrap();
        Self {
            rot_doubt: false,
            dir,
            files: None,
            segment_bytes,
            meta,
            snapshots,
            snapshot: Some(snapshot),
            rotten: None,
            rotten_record: None,
            first: 1,
            log: Vec::new(),
        }
    }

    /// Opens the log and meta, as a restarted node does, closing them first if a crash left them
    /// open. Returns the log's records, and whether it cut away whole records after a rotten one;
    /// or the error that stops the node: damage.
    fn open(&mut self, window: usize) -> io::Result<(Entries, bool)> {
        self.files = None;
        let (mut entries, mut recovered) = (Entries::default(), Vec::new());
        let log = Log::open_simulated(&self.dir.path().join("log"), self.segment_bytes, |record| {
            entries.recover(record, window);
            record.encode_into(&mut recovered);
        })?;
        // Damage looks like a torn write: the log is cut there, with nothing said, and doubted.
        let mut cut = false;
        if let Some(rotten) = self.rotten_record.take() {
            let at = usize::try_from(rotten - self.first).unwrap();
            cut = at + 1 < self.log.len();
            self.log.truncate(at);
            self.rot_doubt = true;
        }
        assert!(recovered == self.log.concat(), "the log recovers what was written");
        assert_eq!(log.next_index(), self.first + self.log.len() as u64);
        // As the driver does: a log that ends before the snapshot, or disagrees with it, goes on
        // from it.
        let mut log = log;
        let snapshot = self.snapshot.as_ref().map(|s| (s.index, s.term));
        if log.resume(snapshot, &mut entries)? {
            (self.log, self.first) = (Vec::new(), snapshot.expect("reset to follow it").0 + 1);
        }
        let (meta_file, meta) = MetaFile::open_simulated(&self.dir.path().join("meta"))?;
        assert_eq!(meta, self.meta, "the meta recovers what was written");
        self.files = Some((log, meta_file));
        Ok((entries, cut))
    }

    fn apply(&mut self, op: &Persist) -> io::Result<()> {
        let (log, meta_file) = self.files.as_mut().expect("open while its node runs");
        match op {
            Persist::Meta(meta) => {
                meta_file.save(*meta)?;
                self.meta = *meta;
            }
            Persist::Append(records) => {
                log.append(records)?;
                self.log.extend(codec::records(records).map(|r| r.unwrap()).map(encode));
            }
            Persist::Trust => {
                log.trust()?;
                self.rot_doubt = false;
            }
            Persist::TruncateFrom(index) => {
                log.truncate_from(*index)?;
                self.log.truncate(usize::try_from(index - self.first).unwrap());
            }
            Persist::Compact { log: to, snapshot } => {
                let first = log.compact_to(*to)?;
                self.compact_to(first);
                snapshot::remove_older(self.snapshots.path(), *snapshot, 1).unwrap();
            }
            Persist::SnapshotChunk { offset, bytes, .. } => {
                snapshot::write_chunk(self.snapshots.path(), *offset, bytes).unwrap();
            }
            Persist::InstallSnapshot { snapshot, replace_log } => {
                snapshot::install(self.snapshots.path(), snapshot, false)?;
                snapshot::remove_older(self.snapshots.path(), snapshot.index, 1).unwrap();
                if *replace_log {
                    log.reset(snapshot.index + 1)?;
                    self.log.clear();
                    self.first = snapshot.index + 1;
                } else {
                    let first = log.compact_to(snapshot.index)?;
                    self.compact_to(first);
                }
                self.snapshot = Some(snapshot.clone());
            }
        }
        // A newer snapshot: the rotten one is no longer read. A record compacted or truncated away.
        self.rotten = self.rotten.filter(|&r| self.snapshot.as_ref().is_some_and(|s| s.index == r));
        let end = self.first + self.log.len() as u64;
        self.rotten_record = self.rotten_record.filter(|r| (self.first..end).contains(r));
        Ok(())
    }

    /// Power lost installing a leader's `snapshot` in place of the log: the snapshot reaches the
    /// disk, and some of the old log's newest segments are gone, perhaps none, perhaps all.
    fn tear_reset(&mut self, snapshot: &Snapshot, rng: &mut Rng) -> io::Result<()> {
        snapshot::install(self.snapshots.path(), snapshot, false)?;
        snapshot::remove_older(self.snapshots.path(), snapshot.index, 1).unwrap();
        self.snapshot = Some(snapshot.clone());
        self.files = None;
        let segments = self.segments();
        let gone = usize::try_from(rng.below(segments.len() as u64 + 1)).unwrap();
        let kept = segments.len() - gone;
        for (_, path) in &segments[kept..] {
            std::fs::remove_file(path).unwrap();
        }
        if kept == 0 {
            (self.log, self.first) = (Vec::new(), 1);
        } else if let Some(&(first, _)) = segments.get(kept) {
            self.log.truncate(usize::try_from(first - self.first).unwrap());
        }
        self.rotten = self.rotten.filter(|&r| r == snapshot.index);
        let end = self.first + self.log.len() as u64;
        self.rotten_record = self.rotten_record.filter(|r| (self.first..end).contains(r));
        Ok(())
    }

    /// Whole segments compacted: the log now starts at `first`.
    fn compact_to(&mut self, first: u64) {
        self.log.drain(..usize::try_from(first - self.first).unwrap());
        self.first = first;
    }

    /// Power lost partway through appending `records`: the first `whole` reach the disk, and
    /// some of the next one's bytes after them; or, now and then, the disk keeping later pages
    /// and losing an earlier, the next one's with the rest of it garbage (every byte changed:
    /// zeros could match its own), and the ones after it whole. Returns whether it was that.
    fn tear(&mut self, records: &[Record<'_>], whole: usize, rng: &mut Rng) -> bool {
        let (log, _) = self.files.as_mut().expect("open while its node runs");
        let bytes: Vec<u8> = records[..whole].iter().copied().flat_map(encode).collect();
        log.append(&bytes).unwrap();
        self.log.extend(records[..whole].iter().copied().map(encode));
        self.files = None;
        let mut torn = encode(records[whole]);
        let cut = 1 + usize::try_from(rng.below(torn.len() as u64 - 1)).unwrap();
        let later = whole + 1 < records.len() && rng.one_in(3);
        if later {
            torn[cut..].iter_mut().for_each(|b| *b ^= 0xa5);
            torn.extend(records[whole + 1..].iter().copied().flat_map(encode));
        } else {
            torn.truncate(cut);
        }
        let (_, path) = self.segments().pop().unwrap();
        let mut file = std::fs::OpenOptions::new().append(true).open(path).unwrap();
        std::io::Write::write_all(&mut file, &torn).unwrap();
        later
    }

    /// The log's segment files, by their first index, oldest first.
    fn segments(&self) -> Vec<(u64, std::path::PathBuf)> {
        let mut segments: Vec<_> = std::fs::read_dir(self.dir.path().join("log"))
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter_map(|p| Some((p.to_str()?.strip_suffix(".log")?.rsplit('/').next()?.parse().ok()?, p.clone())))
            .collect();
        segments.sort();
        segments
    }

    /// The payload at `index`, if the log on disk has it.
    fn record(&self, index: u64) -> Option<&[u8]> {
        let bytes = self.log.get(usize::try_from(index.checked_sub(self.first)?).unwrap())?;
        Some(Record::decode(bytes).unwrap().unwrap().0.payload)
    }

    /// Records from `from` on, read back from the log for a follower: what was written, unless
    /// it's damaged.
    fn read(&mut self, from: u64) -> io::Result<Vec<u8>> {
        let (log, _) = self.files.as_mut().expect("open while its node runs");
        let records = log.read(from, MAX_READ_BYTES)?;
        let at = usize::try_from(from - self.first).unwrap();
        let n = codec::records(&records).count();
        assert!(records == self.log[at..at + n].concat(), "the log reads back what was written");
        Ok(records)
    }

    /// Whether the entry at `index` is durable here: in the log, or covered by the snapshot.
    fn has(&self, index: u64, payload: &[u8]) -> bool {
        self.snapshot.as_ref().is_some_and(|s| s.index >= index) || self.record(index) == Some(payload)
    }

    /// The bytes of the snapshot of `index`, from its file.
    fn snapshot_bytes(&self, index: u64) -> Vec<u8> {
        snapshot::read(self.snapshots.path(), index, 0, usize::MAX).unwrap()
    }

    /// The latest snapshot, and its bytes to restore from.
    fn load_snapshot(&self) -> io::Result<Option<(Snapshot, impl Read + use<>)>> {
        let loaded = snapshot::load(self.snapshots.path())?;
        assert_eq!(loaded.as_ref().map(|(s, _)| s), self.snapshot.as_ref(), "the latest snapshot is found");
        Ok(loaded)
    }

    /// Flips a bit of the latest snapshot's file, as bit rot would.
    fn rot(&mut self, rng: &mut Rng) {
        let Some(snapshot) = &self.snapshot else { return };
        self.rotten = Some(snapshot.index);
        let path = self.snapshots.path().join(format!("{:020}.snapshot", snapshot.index));
        let mut bytes = std::fs::read(&path).unwrap();
        let at = usize::try_from(rng.below(bytes.len() as u64)).unwrap();
        bytes[at] ^= 1 << rng.below(8);
        std::fs::write(&path, bytes).unwrap();
    }

    /// Flips a bit of a record in the log, as bit rot would.
    fn rot_record(&mut self, rng: &mut Rng) {
        if self.log.is_empty() {
            return;
        }
        // Often the last, which a restart takes for a torn write.
        let n = self.log.len() as u64;
        let index = self.first + if rng.one_in(2) { n - 1 } else { rng.below(n) };
        let (first, path) = self.segments().into_iter().rfind(|&(first, _)| first <= index).unwrap();
        let mut bytes = std::fs::read(&path).unwrap();
        let at: usize = self.log[usize::try_from(first - self.first).unwrap()..]
            [..usize::try_from(index - first).unwrap()]
            .iter()
            .map(Vec::len)
            .sum();
        let len = self.log[usize::try_from(index - self.first).unwrap()].len();
        bytes[at + usize::try_from(rng.below(len as u64)).unwrap()] ^= 1 << rng.below(8);
        std::fs::write(&path, bytes).unwrap();
        self.rotten_record = Some(index);
    }
}

fn encode(record: Record<'_>) -> Vec<u8> {
    let mut bytes = Vec::new();
    record.encode_into(&mut bytes);
    bytes
}

struct SimNode {
    node: Option<Node<Echo>>,
    disk: Disk,
    /// Committed entries checked against `Sim::committed`.
    checked: u64,
    tick_at: Option<u64>,
    incarnation: u64,
    /// Runs the new version.
    upgraded: bool,
    /// It waits on its disk, a restore with what's being written: it takes no input until then,
    /// and what it sends goes no sooner.
    stalled_until: u64,
    /// Its batches between taken and durable, by the driver's rules.
    pipeline: Pipeline,
    /// The batches being written, until `written_at`: not yet on disk.
    writing: Vec<Batch>,
    written_at: Option<u64>,
    /// Its disk is damaged: it stopped, and stays down until it's replaced.
    broken: bool,
    /// When it last broke.
    broken_at: u64,
    /// Wiped and started again: it joins, rather than starting a cluster.
    wiped: bool,
    /// Lost with its cluster: it never starts again.
    lost: bool,
}

impl SimNode {
    fn new(disk: Disk) -> Self {
        let (checked, tick_at, incarnation, stalled_until) = (0, None, 0, 0);
        let (upgraded, broken, wiped) = (false, false, false);
        Self {
            node: None,
            disk,
            checked,
            tick_at,
            incarnation,
            upgraded,
            stalled_until,
            pipeline: Pipeline::default(),
            writing: Vec::new(),
            written_at: None,
            broken,
            broken_at: 0,
            wiped,
            lost: false,
        }
    }
}

struct SimClient {
    client: Client,
    conn: Option<ConnId>,
    dialing: bool,
    tick_at: Option<u64>,
    sent: u64,
    /// The last `a<k>` response.
    answered: u64,
    /// Every response: its last `egress_seq`.
    responses: u64,
    timers: BTreeSet<u64>,
    proposals: BTreeSet<u64>,
    /// What the node had of the session as it sent the last `Opened` the client read.
    opened_with: (u64, u64),
}

/// A client connection: in order each way, like TCP.
struct Conn {
    client: usize,
    node: NodeId,
    up: bool,
    to_node_at: u64,
    to_client_at: u64,
    /// What the node had of the session as it sent each `Opened` still on its way: the last
    /// message applied, and the responses. It may apply more before the client reads it.
    opened: VecDeque<(u64, u64)>,
}

struct Sim {
    seed: u64,
    rng: Rng,
    now: u64,
    queue: BinaryHeap<(Reverse<u64>, Reverse<u64>)>,
    events: BTreeMap<u64, Event>,
    next_event: u64,
    /// Every node, member or not.
    members: Vec<NodeId>,
    /// How many were the first members, from 1.
    initial: u8,
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
    member_changes: u64,
    transfers: u64,
    /// The index the new version was activated at.
    activated: Option<u64>,
    stalls: u64,
    rots: u64,
    broken: u64,
    /// Nodes whose doubt the operator ended.
    overrides: u64,
    /// The log's faults this seed got to: [`Coverage::torn`] and the two after.
    covered: Coverage,
    /// The shutdown's index, once answered, and the members then; the nodes it stopped, with
    /// their snapshots' indices.
    shut_down: Option<(u64, Vec<NodeId>)>,
    stopped: Vec<(NodeId, u64)>,
    /// The members each stopped node knew when it stopped.
    stopped_members: BTreeMap<NodeId, Vec<NodeId>>,
    started_again: bool,
    /// Each admin connection's request, to send again if it's redirected.
    admin_frames: BTreeMap<ConnId, Vec<u8>>,
    /// Every node's [`Config::log_window_bytes`].
    window: usize,
    /// Every node's log segments close at this size: small, so they roll and compact often.
    segment_bytes: u64,
    /// How much of a snapshot a read returns: small, so a snapshot goes in many chunks.
    chunk: usize,
    reads: u64,
    /// Clients send until then.
    send_until: u64,
    /// The cluster's loss and recovery, once begun.
    disaster: Option<Disaster>,
}

/// How a lost cluster is recovered, and how far that's got.
struct Disaster {
    /// Restored from a snapshot's copy, onto `standby`; else `standby` is a learner, promoted.
    restore: bool,
    standby: NodeId,
    /// The lost cluster's epoch, which the new one mustn't have.
    old_epoch: u64,
    /// The copy of a snapshot, to restore from.
    backup: Option<(tempfile::TempDir, std::path::PathBuf)>,
    recovered: bool,
    /// A client that stops sending, and when: shortly before the backup or the standby's cut, so
    /// the new cluster has its messages, but perhaps not the responses others' messages cause it.
    idle: (usize, u64),
}

/// Client `c`, its session `again` if it's the second it opened.
fn new_client(c: usize, again: bool) -> SimClient {
    let addrs: Vec<String> = (1..=7).map(|id| format!("n{id}")).collect();
    let name = if again { format!("client {c} again") } else { format!("client {c}") };
    SimClient {
        client: Client::new(&addrs, name, Duration::from_millis(500)).unwrap(),
        conn: None,
        dialing: false,
        tick_at: None,
        sent: 0,
        answered: 0,
        responses: 0,
        timers: BTreeSet::new(),
        proposals: BTreeSet::new(),
        opened_with: (0, 0),
    }
}

impl Sim {
    fn new(seed: u64) -> Self {
        let mut rng = Rng(seed);
        let n: u8 = if rng.one_in(2) { 3 } else { 5 };
        let window = [0, 2048, usize::MAX][usize::try_from(rng.below(3)).unwrap()];
        let chunk = if rng.one_in(2) { 4096 } else { MAX_READ_BYTES };
        let segment_bytes = if rng.one_in(4) { MAX_SEGMENT_BYTES } else { 4096 << rng.below(4) };
        let members: Vec<NodeId> = (1..=7).map(NodeId).collect();
        let clients = (0..CLIENTS).map(|c| new_client(c, false)).collect();
        let mut sim = Self {
            seed,
            rng,
            now: 0,
            queue: BinaryHeap::new(),
            events: BTreeMap::new(),
            next_event: 0,
            members: members.clone(),
            initial: n,
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
            member_changes: 0,
            transfers: 0,
            activated: None,
            stalls: 0,
            rots: 0,
            broken: 0,
            overrides: 0,
            covered: Coverage::default(),
            admin_frames: BTreeMap::new(),
            shut_down: None,
            stopped: Vec::new(),
            stopped_members: BTreeMap::new(),
            started_again: false,
            window,
            segment_bytes,
            chunk,
            reads: 0,
            send_until: FAULT_PHASE + QUIET_PHASE / 2,
            disaster: None,
        };
        for id in members {
            sim.nodes.insert(id, SimNode::new(Disk::new(segment_bytes)));
            sim.start(id);
        }
        for c in 0..CLIENTS {
            sim.dial_later(c);
            sim.schedule(10 * MS, Event::ClientSend(c));
        }
        sim.schedule_operator();
        sim
    }

    /// The faults and the operator's work, each of which goes on to schedule more.
    fn schedule_operator(&mut self) {
        self.schedule(1000 * MS, Event::Partition);
        self.schedule(2000 * MS, Event::AdminRequest);
        self.schedule(1500 * MS, Event::ChangeMembers);
        self.schedule(1200 * MS, Event::TransferLeadership);
        self.schedule(1000 * MS, Event::Upgrade);
        self.schedule(2000 * MS, Event::Activate);
        self.schedule(3000 * MS, Event::Rot);
        // Late in the quiet phase: transfers and changes of members first, the clients still sending.
        let after = FAULT_PHASE + 3500 * MS + self.rng.below(1000 * MS);
        self.schedule(after, Event::Shutdown);
    }

    fn run(mut self) -> Coverage {
        self.run_until(FAULT_PHASE + QUIET_PHASE);
        self.check_end();
        let end = self.begin_disaster();
        self.run_until(end);
        self.check_end();
        self.covered.carried_on = CLIENTS as u64 - self.covered.diverged;
        let disaster = self.disaster.as_ref().unwrap();
        let how = if disaster.restore { "restored from a snapshot" } else { "promoted a learner" };
        let (diverged, by_responses) = (self.covered.diverged, self.covered.lost_responses);
        eprintln!(
            "seed {}: lost, {how}: {diverged} of {CLIENTS} clients diverged, {by_responses} by responses",
            self.seed
        );
        Coverage {
            manual_snapshots: self.admin_answers,
            member_changes: self.member_changes,
            transfers: self.transfers,
            activations: u64::from(self.activated.is_some()),
            shutdowns: u64::from(self.shut_down.is_some()),
            ..self.covered
        }
    }

    /// Runs the events until `end`, and then until every node has heard the last commit, which
    /// could be a heartbeat away.
    fn run_until(&mut self, end: u64) {
        while let Some((Reverse(at), Reverse(seq))) = self.queue.pop() {
            if at > end && (self.converged() || at > end + 1000 * MS) {
                self.queue.push((Reverse(at), Reverse(seq)));
                return;
            }
            if self.faults && at > FAULT_PHASE {
                self.end_faults();
            }
            self.now = at;
            let event = self.events.remove(&seq).expect("each event is queued once");
            self.handle(event);
            self.check();
        }
    }

    fn handle(&mut self, event: Event) {
        let Some(event) = self.unless_stalled(event) else { return };
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
            Event::ChangeMembers => self.change_members(),
            Event::Upgrade => self.upgrade(),
            Event::Activate => self.activate(),
            Event::Rot => self.rot(),
            Event::Replace(id) => self.replace(id),
            Event::AdminRetry { frame, tries } => self.admin_frame(frame, tries),
            Event::Shutdown => {
                let request_id = self.rng.next();
                self.admin(&turbofan_cluster::wire::Shutdown { request_id });
            }
            Event::StartAll => self.start_all(),
            Event::TransferLeadership => self.transfer_leadership(),
            Event::Backup => self.backup(),
            Event::CutStandby => self.cut_standby(),
            Event::Lose => self.lose(),
            Event::Recover => self.recover(),
            Event::Saved { id, incarnation, save } => self.saved(id, incarnation, save),
            Event::Written { id, incarnation } => {
                let sim_node = &self.nodes[&id];
                if sim_node.incarnation == incarnation && sim_node.node.is_some() {
                    self.written(id);
                }
            }
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
        self.admin_requests += 1;
        self.admin(&turbofan_cluster::wire::SnapshotRequest { request_id: self.admin_requests });
    }

    /// An operator adds a member, or removes one (the leader, perhaps), keeping three to seven,
    /// and three voters or more. Some it adds are learners; some changes make a learner a voter,
    /// or a voter a learner, in place.
    fn change_members(&mut self) {
        if self.now < FAULT_PHASE + QUIET_PHASE / 2 {
            let after = 500 * MS + self.rng.below(1000 * MS);
            self.schedule(after, Event::ChangeMembers);
        }
        let Some(leader) = self.leader() else { return };
        let raft = self.nodes[&leader].node.as_ref().unwrap().raft();
        let members: Vec<NodeId> = raft.members().iter().map(|m| m.id).collect();
        let learners: Vec<NodeId> = raft.members().iter().filter(|m| m.learner).map(|m| m.id).collect();
        self.covered.learners += u64::from(!learners.is_empty());
        let others: Vec<NodeId> = self.members.iter().copied().filter(|id| !members.contains(id)).collect();
        let voters = members.len() - learners.len();
        let request_id = self.rng.next() & !ROLE_REQUEST;
        if self.rng.one_in(3) && (!learners.is_empty() || voters > 3) {
            let (id, learner) =
                if learners.is_empty() { (self.rng.pick(&members), true) } else { (self.rng.pick(&learners), false) };
            let request_id = request_id | ROLE_REQUEST;
            self.admin(&turbofan_cluster::wire::SetRole { request_id, id: id.0, learner: u8::from(learner) });
        } else if members.len() < MAX_MEMBERS && (voters <= 3 || self.rng.one_in(2)) {
            let m = member(self.rng.pick(&others));
            let (peer, client, admin) = (m.peer.as_bytes(), m.client.as_bytes(), m.admin.as_bytes());
            let learner = u8::from(self.rng.one_in(3));
            self.admin(&turbofan_cluster::wire::AddMember { request_id, id: m.id.0, learner, peer, client, admin });
        } else {
            let id = self.rng.pick(if voters <= 3 { &learners } else { &members });
            self.admin(&turbofan_cluster::wire::RemoveMember { request_id, id: id.0 });
        }
    }

    /// Sends `request` to the leader, on an admin connection of its own.
    fn admin(&mut self, request: &impl turbofan_sbe::Encode) {
        let mut frame = Vec::new();
        codec::encode_frame(request, &mut frame).unwrap();
        self.admin_frame(frame, 0);
    }

    /// Sends an operator's request to the leader, as [`turbofan_cluster::remote`] does: again,
    /// a while later, if there's none, a few times.
    fn admin_frame(&mut self, frame: Vec<u8>, tries: u32) {
        if let Some(until) = self.leader().map(|id| self.nodes[&id].stalled_until).filter(|&t| t > self.now) {
            self.schedule(until - self.now, Event::AdminRetry { frame, tries });
            return;
        }
        let Some(id) = self.leader() else {
            if tries < 20 {
                let after = 20 * MS + self.rng.below(100 * MS);
                self.schedule(after, Event::AdminRetry { frame, tries: tries + 1 });
            }
            return;
        };
        let conn = self.next_conn;
        self.next_conn += 1;
        self.conns.insert(
            conn,
            Conn { client: ADMIN, node: id, up: true, to_node_at: 0, to_client_at: 0, opened: VecDeque::new() },
        );
        self.admin_frames.insert(conn, frame.clone());
        let node = self.nodes.get_mut(&id).unwrap().node.as_mut().unwrap();
        node.on_admin(conn, &codec::decode_frame(&frame).unwrap().unwrap().0, self.now);
        self.flush(id);
    }

    /// The members, as the leader has them, if there's one.
    fn final_members(&self) -> Option<Vec<NodeId>> {
        let leader = self.nodes[&self.leader()?].node.as_ref()?;
        Some(leader.raft().members().iter().map(|m| m.id).collect())
    }

    /// How many of the leader's members are voters, or 0 if there's no leader.
    fn final_voters(&self) -> usize {
        let leader = self.leader().and_then(|id| self.nodes[&id].node.as_ref());
        leader.map_or(0, |n| n.raft().members().iter().filter(|m| !m.learner).count())
    }

    fn converged(&self) -> bool {
        let Some(members) = self.final_members() else { return false };
        let mut nodes = members.iter().filter_map(|id| self.nodes[id].node.as_ref());
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

    /// Takes a node's outputs, by the driver's rules ([`Pipeline`]): sends what goes at once,
    /// and starts a write if there's one to start.
    fn flush(&mut self, id: NodeId) {
        let sim_node = self.nodes.get_mut(&id).unwrap();
        let taken = sim_node.pipeline.take(sim_node.node.as_mut().unwrap());
        for (to, frame) in taken.peer_appends.into_iter().chain(taken.peer_sends) {
            self.send_peer(id, to, frame);
        }
        for (conn, frame) in taken.client_sends {
            if let turbofan_cluster::wire::Decoded::Opened(m) = codec::decode_frame(&frame).unwrap().unwrap().0 {
                let service = self.nodes[&id].node.as_ref().unwrap().service();
                let session = SessionId(m.session());
                let had = (
                    service.last.get(&session).copied().unwrap_or(0),
                    service.responses.get(&session).copied().unwrap_or(0),
                );
                self.conns.get_mut(&conn).unwrap().opened.push_back(had);
            }
            self.send_conn(conn, frame, false);
        }
        // A snapshot from the leader that replaced the log: what it covers is checked by its
        // bytes, and the disk's log, still to be replaced, may hold uncommitted entries there.
        // (One this node saved covers only what it had checked: it commits, then snapshots.)
        let sim_node = self.nodes.get_mut(&id).unwrap();
        sim_node.checked = sim_node.checked.max(sim_node.node.as_ref().unwrap().raft().snapshot_index());
        self.check_committed(id);
        if let Some(save) = taken.save {
            let incarnation = self.nodes[&id].incarnation;
            let after = if self.rng.one_in(4) { self.rng.below(500 * MS) } else { self.rng.below(20 * MS) };
            self.schedule(after, Event::Saved { id, incarnation, save });
        }
        if let Some(batches) = taken.write {
            // Mostly quick; now and then the disk is slow.
            let after = if self.faults && self.rng.one_in(40) {
                self.stalls += 1;
                5 * MS + self.rng.below(300 * MS)
            } else {
                self.rng.below(2 * MS)
            };
            let sim_node = self.nodes.get_mut(&id).unwrap();
            (sim_node.writing, sim_node.written_at) = (batches, Some(self.now + after));
            let incarnation = sim_node.incarnation;
            self.schedule(after, Event::Written { id, incarnation });
        }
        let sim_node = self.nodes.get_mut(&id).unwrap();
        if !sim_node.pipeline.takes_inputs() {
            let until = sim_node.written_at.expect("a restore waits on a write");
            sim_node.stalled_until = sim_node.stalled_until.max(until);
        }
        self.after_flush(id);
    }

    /// A node's write is done: the batches reach the disk, perhaps crashing partway, what they
    /// asked for is read, the node told, and what waited for them sent.
    fn written(&mut self, id: NodeId) {
        self.check_committed(id);
        let sim_node = self.nodes.get_mut(&id).unwrap();
        sim_node.written_at = None;
        let mut batches = std::mem::take(&mut sim_node.writing);
        let (mut reads, mut chunks) = (Vec::new(), Vec::new());
        for i in 0..batches.len() {
            // Often while a leader's snapshot replaces the log: rare, and where a crash does the
            // most harm.
            let resets =
                batches[i].persist.iter().any(|op| matches!(op, Persist::InstallSnapshot { replace_log: true, .. }));
            if self.faults && !batches[i].persist.is_empty() && self.rng.one_in(if resets { 3 } else { 150 }) {
                // Power lost: what it hadn't written yet, it's writing as it crashes.
                self.nodes.get_mut(&id).unwrap().writing = batches.split_off(i);
                self.crash(id);
                return;
            }
            for op in &batches[i].persist {
                let disk = &mut self.nodes.get_mut(&id).unwrap().disk;
                let first = disk.first;
                if disk.apply(op).is_err() {
                    self.break_disk(id);
                    return;
                }
                self.covered.segments_compacted +=
                    u64::from(disk.first > first && disk.segment_bytes < MAX_SEGMENT_BYTES);
                self.check_snapshot(id, op);
            }
            // A record or snapshot it would send is damaged: it stops rather than send it.
            let Ok((more, more_chunks)) = self.read(id, &batches[i].reads, &batches[i].snapshot_reads) else {
                self.break_disk(id);
                return;
            };
            reads.extend(more);
            chunks.extend(more_chunks);
        }
        self.reads += (reads.len() + chunks.len()) as u64;
        let sim_node = self.nodes.get_mut(&id).unwrap();
        let mut snapshot = None;
        if let Some(index) = sim_node.pipeline.restore() {
            // Just installed, and checked: it doesn't rot between.
            let (loaded, bytes) = sim_node.disk.load_snapshot().unwrap().expect("the installed snapshot");
            assert_eq!(loaded.index, index);
            snapshot = Some(bytes);
        }
        let from = snapshot.as_mut().map(|b| b as &mut dyn Read);
        let node = sim_node.node.as_mut().unwrap();
        let sends = sim_node.pipeline.written(node, &reads, &chunks, from, self.now).unwrap();
        for (to, frame) in sends {
            self.send_peer(id, to, frame);
        }
        self.flush(id);
    }

    /// A node that stopped is taken down, once its writes are done; otherwise its next tick is
    /// scheduled.
    fn after_flush(&mut self, id: NodeId) {
        if self.nodes[&id].node.as_ref().unwrap().stopped() && !self.nodes[&id].pipeline.writing() {
            self.stop(id);
            return;
        }
        let sim_node = self.nodes.get_mut(&id).unwrap();
        let deadline = sim_node.node.as_ref().unwrap().next_deadline().max(self.now + 1);
        if sim_node.tick_at.is_none_or(|at| deadline < at) {
            sim_node.tick_at = Some(deadline);
            self.schedule(deadline - self.now, Event::Tick(id));
        }
    }

    /// A snapshot's writer runs now, while its node may have applied more since it was taken: it
    /// must write the state as it was.
    fn saved(&mut self, id: NodeId, incarnation: u64, save: SaveSnapshot) {
        let sim_node = &self.nodes[&id];
        if sim_node.incarnation != incarnation || sim_node.node.is_none() {
            return;
        }
        let disk = &self.nodes[&id].disk;
        let path = disk.snapshots.path();
        let snapshot = snapshot::save(path, save.index, save.term, save.members, save.writer, false).unwrap();
        let bytes = disk.snapshot_bytes(snapshot.index);
        self.check_snapshot_bytes(snapshot.index, &bytes);
        let sim_node = self.nodes.get_mut(&id).unwrap();
        // A newer one, from the leader, may have overtaken it: the node then ignores it.
        if sim_node.disk.snapshot.as_ref().is_none_or(|s| s.index < snapshot.index) {
            sim_node.disk.snapshot = Some(snapshot.clone());
            sim_node.disk.rotten = None;
        }
        sim_node.node.as_mut().unwrap().on_snapshot_saved(snapshot);
        self.flush(id);
    }

    /// The records and snapshot chunks a node asked to read, from its files.
    #[allow(clippy::type_complexity, reason = "the two kinds of read")]
    fn read(
        &mut self,
        id: NodeId,
        reads: &[u64],
        chunks: &[SnapshotRead],
    ) -> io::Result<(Vec<Vec<u8>>, Vec<(u64, u64, Vec<u8>)>)> {
        let disk = &mut self.nodes.get_mut(&id).unwrap().disk;
        let records = reads.iter().map(|&from| disk.read(from)).collect::<io::Result<_>>()?;
        let dir = disk.snapshots.path();
        let chunks = chunks
            .iter()
            .map(|&SnapshotRead { index: i, offset: o }| snapshot::read(dir, i, o, self.chunk).map(|b| (i, o, b)));
        Ok((records, chunks.collect::<io::Result<_>>()?))
    }

    /// Every node's snapshot of an index is the same bytes.
    fn check_snapshot(&mut self, id: NodeId, op: &Persist) {
        let Persist::InstallSnapshot { snapshot, .. } = op else { return };
        self.installs += 1;
        let bytes = self.nodes[&id].disk.snapshot_bytes(snapshot.index);
        self.check_snapshot_bytes(snapshot.index, &bytes);
    }

    fn check_snapshot_bytes(&mut self, index: u64, bytes: &[u8]) {
        let first = self.snapshots.entry(index).or_insert_with(|| bytes.to_vec());
        assert!(first == bytes, "seed {}: snapshots of {index} differ", self.seed);
    }

    fn send_peer(&mut self, from: NodeId, to: NodeId, frame: Vec<u8>) {
        if self.cut[usize::from(from)][usize::from(to)] || (self.faults && self.rng.one_in(50)) {
            return;
        }
        let stall = self.stall(from);
        if self.faults && self.rng.one_in(100) {
            let after = stall + MS + self.rng.below(5 * MS);
            self.schedule(after, Event::Deliver { from, to, frame: frame.clone() });
        }
        // Mostly prompt, now and then later than an election timeout: stale votes and appends.
        let after =
            if self.faults && self.rng.one_in(10) { self.rng.below(400 * MS) } else { MS / 2 + self.rng.below(5 * MS) };
        self.schedule(stall + after, Event::Deliver { from, to, frame });
    }

    /// How long a node has yet to wait on its disk.
    fn stall(&self, id: NodeId) -> u64 {
        self.nodes[&id].stalled_until.saturating_sub(self.now)
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
        let stall = if to_node { 0 } else { self.stall(self.conns[&conn].node) };
        let delay = stall + MS / 2 + self.rng.below(3 * MS);
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
            if let turbofan_cluster::wire::Decoded::MembersChanged(m) = &message {
                self.member_changes += 1;
                self.covered.role_changes += u64::from(m.request_id() & ROLE_REQUEST != 0);
            }
            if let turbofan_cluster::wire::Decoded::Activation(m) = message
                && m.index() > 0
                && self.activated.is_none()
            {
                // Let only once every member runs the new version.
                let leader = self.conns[&conn].node;
                let members =
                    self.nodes[&leader].node.as_ref().map(|n| n.raft().members().iter().map(|m| m.id).collect());
                let members: Vec<NodeId> = members.or_else(|| self.stopped_members.get(&leader).cloned()).unwrap();
                let old: Vec<NodeId> = members.into_iter().filter(|id| !self.nodes[id].upgraded).collect();
                assert!(old.is_empty(), "seed {}: activated with {old:?} on the old version", self.seed);
                self.activated = Some(m.index());
            }
            if let turbofan_cluster::wire::Decoded::LeadershipTransferred(m) = message {
                self.transfers += u64::from(m.leader() != 0);
            }
            if let turbofan_cluster::wire::Decoded::ShutDown(m) = message
                && self.shut_down.is_none()
            {
                // The members as the leader that answered knew them, stopped by now or not.
                let leader = self.conns[&conn].node;
                let members =
                    self.nodes[&leader].node.as_ref().map(|n| n.raft().members().iter().map(|m| m.id).collect());
                let then = members.or_else(|| self.stopped_members.get(&leader).cloned()).expect("it was up");
                self.shut_down = Some((m.index(), then));
                self.schedule(300 * MS, Event::StartAll);
            }
            // The leader stepped down before it answered: ask the new one.
            if matches!(message, turbofan_cluster::wire::Decoded::Redirect(_))
                && let Some(frame) = self.admin_frames.remove(&conn)
            {
                self.schedule(MS, Event::AdminRetry { frame, tries: 0 });
            }
            return;
        }
        let message = codec::decode_frame(frame).unwrap().unwrap().0;
        if matches!(message, turbofan_cluster::wire::Decoded::Opened(_)) {
            self.clients[c].opened_with =
                self.conns.get_mut(&conn).unwrap().opened.pop_front().expect("recorded as sent");
        }
        self.clients[c].client.on_frame(&message);
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
        self.conns.insert(
            conn,
            Conn { client: c, node: target, up: true, to_node_at: 0, to_client_at: 0, opened: VecDeque::new() },
        );
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
        let idle = self.disaster.as_ref().is_some_and(|d| d.idle.0 == c && self.now > d.idle.1 && !d.recovered);
        if self.now > self.send_until || idle {
            if idle {
                // Again once it's recovered, on whichever session it then has.
                self.schedule(100 * MS, Event::ClientSend(c));
            }
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
        if let client::Event::Closed { reason: CloseReason::Diverged, unapplied } = event {
            return self.diverged(c, unapplied);
        }
        let seed = self.seed;
        let sim_client = &mut self.clients[c];
        match event {
            client::Event::Opened(_) => self.carried_on(c),
            client::Event::Message(payload) => {
                sim_client.responses += 1;
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
                } else if !text.starts_with('n') {
                    let seen = if text.starts_with('p') { &mut sim_client.proposals } else { &mut sim_client.timers };
                    assert!(seen.insert(k), "seed {seed}: client {c} got {text} twice");
                }
            }
            event => panic!("seed {seed}: client {c}: {event:?}"),
        }
    }

    /// A client's session opened again on the recovered cluster: it had all the client saw of it,
    /// messages applied and responses, as it sent the `Opened`, or the client should have closed
    /// it as diverged. (Before, the node may since have crashed, or restarted behind what it said.)
    fn carried_on(&mut self, c: usize) {
        if !self.disaster.as_ref().is_some_and(|d| d.recovered) {
            return;
        }
        let (seed, client) = (self.seed, &self.clients[c]);
        let (applied, responses) = client.opened_with;
        assert!(
            applied >= client.client.applied() && responses >= client.responses,
            "seed {seed}: client {c} carried on, having seen {} applied and {} responses, the cluster {applied} and {responses}",
            client.client.applied(),
            client.responses,
        );
    }

    /// A client closed its session on the recovered cluster as `Diverged`: only if that cluster
    /// lacked some of what it saw as it sent the `Opened`, and counting what it never applied. A gateway then opens another.
    fn diverged(&mut self, c: usize, unapplied: usize) {
        let seed = self.seed;
        assert!(self.disaster.as_ref().is_some_and(|d| d.recovered), "seed {seed}: client {c} diverged");
        let conn = self.clients[c].conn.expect("told on its connection");
        let (had, responses) = self.clients[c].opened_with;
        let (sent, seen) = (self.clients[c].sent, self.clients[c].responses);
        // The client's own idea of what's applied is what it had: it let go of no more since.
        let applied = self.clients[c].client.applied();
        assert!(had < applied || responses < seen, "seed {seed}: client {c} diverged from a cluster with all it saw");
        if had >= applied {
            self.covered.lost_responses += 1;
        }
        assert_eq!(unapplied as u64, sent - had, "seed {seed}: client {c} sent {sent}, the cluster applied {had}");
        self.covered.diverged += 1;
        self.clients[c] = new_client(c, true);
        // What's still to come on the old session's connection isn't the new one's.
        self.reset(conn);
        self.dial_later(c);
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
                // Committed by a majority of the voters of some config from the entry's on: as of
                // the entry, or as of now if they've changed since.
                let majority = |members: &[Member]| {
                    let voters: Vec<&Member> = members.iter().filter(|m| !m.learner).collect();
                    let durable = voters.iter().filter(|m| self.nodes[&m.id].disk.has(index, &payload)).count();
                    durable > voters.len() / 2
                };
                assert!(
                    majority(&raft.members_at(index)) || majority(raft.members()),
                    "seed {}: entry {index} committed but not durable on a majority",
                    self.seed
                );
                self.committed.push(payload);
                self.commit_terms.push(raft.term());
            }
            self.nodes.get_mut(&id).unwrap().checked = index;
        }
    }

    /// The event, unless it's for a node stalled on its disk: that takes it once it's done.
    fn unless_stalled(&mut self, event: Event) -> Option<Event> {
        let target = match &event {
            Event::Deliver { to, .. } => Some(*to),
            Event::Tick(id) | Event::Saved { id, .. } => Some(*id),
            Event::ToNode { conn, .. } => Some(self.conns[conn].node),
            _ => None,
        };
        if let Some(until) = target.map(|id| self.nodes[&id].stalled_until).filter(|&t| t > self.now) {
            self.schedule(until - self.now, event);
            return None;
        }
        Some(event)
    }

    /// The operator asks to switch to the new version, until it's let.
    fn activate(&mut self) {
        if self.activated.is_none() && self.now < FAULT_PHASE + QUIET_PHASE / 2 {
            let after = 500 * MS + self.rng.below(1500 * MS);
            self.schedule(after, Event::Activate);
        }
        let request_id = self.rng.next();
        self.admin(&turbofan_cluster::wire::Activate { request_id, protocol: 1, service_version: 1 });
    }

    /// The operator hands the leadership over: to the member most caught up, or a given one
    /// (perhaps not caught up, or not a member at all: refused).
    fn transfer_leadership(&mut self) {
        if self.now < FAULT_PHASE + QUIET_PHASE / 2 {
            let after = 1000 * MS + self.rng.below(2000 * MS);
            self.schedule(after, Event::TransferLeadership);
        }
        let members = self.final_members().unwrap_or_default();
        let to = match self.rng.below(4) {
            0 | 1 => 0,
            2 if !members.is_empty() => self.rng.pick(&members).0,
            _ => self.rng.pick(&self.members).0,
        };
        let request_id = self.rng.next();
        self.admin(&turbofan_cluster::wire::TransferLeadership { request_id, to });
    }

    /// Upgrades a node not yet upgraded, at random: restarted on the new version, if it's up.
    fn upgrade(&mut self) {
        let old: Vec<NodeId> = self.members.iter().copied().filter(|id| !self.nodes[id].upgraded).collect();
        // Not so late that a node restarted for it isn't back by the end.
        if old.is_empty() || self.now > FAULT_PHASE + QUIET_PHASE / 2 {
            return;
        }
        let id = self.rng.pick(&old);
        self.nodes.get_mut(&id).unwrap().upgraded = true;
        if self.nodes[&id].node.is_some() {
            self.crash(id);
        }
        let after = 1000 * MS + self.rng.below(1500 * MS);
        self.schedule(after, Event::Upgrade);
    }

    /// A node stopped in a shutdown: down, with its snapshot, until it's started again.
    fn stop(&mut self, id: NodeId) {
        let sim_node = self.nodes.get_mut(&id).unwrap();
        let members = sim_node.node.take().unwrap().raft().members().iter().map(|m| m.id).collect();
        self.stopped_members.insert(id, members);
        sim_node.incarnation += 1;
        let snapshot = sim_node.disk.snapshot.as_ref().map_or(0, |s| s.index);
        self.stopped.push((id, snapshot));
        // One that stopped late, its snapshot still being written when the rest restarted.
        if self.started_again {
            self.schedule(300 * MS, Event::Restart(id));
        }
        let conns: Vec<ConnId> = self.conns.iter().filter(|(_, c)| c.up && c.node == id).map(|(&k, _)| k).collect();
        for conn in conns {
            self.reset(conn);
        }
    }

    fn crash(&mut self, id: NodeId) {
        let sim_node = self.nodes.get_mut(&id).unwrap();
        if sim_node.node.take().is_none() {
            return;
        }
        let ops: Vec<Persist> = std::mem::take(&mut sim_node.writing).into_iter().flat_map(|b| b.persist).collect();
        if !ops.is_empty() {
            self.tear_writes(id, &ops);
        }
        let sim_node = self.nodes.get_mut(&id).unwrap();
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

    /// A node finds its disk damaged and stops, as the driver does, until it's replaced.
    fn break_disk(&mut self, id: NodeId) {
        let sim_node = self.nodes.get_mut(&id).unwrap();
        sim_node.node = None;
        sim_node.incarnation += 1;
        if !sim_node.broken {
            (sim_node.broken, sim_node.broken_at) = (true, self.now);
            self.broken += 1;
            self.schedule(200 * MS, Event::Replace(id));
        }
        let conns: Vec<ConnId> = self.conns.iter().filter(|(_, c)| c.up && c.node == id).map(|(&k, _)| k).collect();
        for conn in conns {
            self.reset(conn);
        }
    }

    /// The operator replaces a node with a damaged disk: removes it from the members, and only once
    /// that's committed (with its vote forgotten, it mustn't vote again) wipes it and starts it
    /// again, to join like a new node.
    fn replace(&mut self, id: NodeId) {
        let Some(members) = self.final_members() else {
            self.override_doubt(id);
            self.schedule(200 * MS, Event::Replace(id));
            return;
        };
        if members.contains(&id) {
            let request_id = self.rng.next() & !ROLE_REQUEST;
            self.admin(&turbofan_cluster::wire::RemoveMember { request_id, id: id.0 });
            self.schedule(500 * MS, Event::Replace(id));
            return;
        }
        let sim_node = self.nodes.get_mut(&id).unwrap();
        sim_node.disk = Disk::new(self.segment_bytes);
        (sim_node.broken, sim_node.wiped) = (false, true);
        self.start(id);
    }

    /// Still no leader a while after `broken` broke, perhaps because a member in doubt needs one
    /// vote more: the operator, knowing which nodes only lost power, ends their doubt, removing
    /// the `doubt` file and restarting them. Never one that dropped a rotten record.
    fn override_doubt(&mut self, broken: NodeId) {
        if self.now < self.nodes[&broken].broken_at + 3000 * MS {
            return;
        }
        let torn = |n: &SimNode| n.node.as_ref().is_some_and(|n| n.raft().in_doubt()) && !n.disk.rot_doubt;
        let ids: Vec<NodeId> = self.nodes.iter().filter(|(_, n)| torn(n)).map(|(&id, _)| id).collect();
        for id in ids {
            std::fs::remove_file(self.nodes[&id].disk.dir.path().join("log/doubt")).unwrap();
            self.overrides += 1;
            self.crash(id);
        }
    }

    /// Bit rot in a node's latest snapshot, which it finds when it next needs it.
    fn rot(&mut self) {
        if !self.faults {
            return;
        }
        let after = 500 * MS + self.rng.below(1000 * MS);
        self.schedule(after, Event::Rot);
        // One node's disk damaged at a time, and of three voters or more: two of three lost at
        // once would lose what only they had, past what Raft tolerates.
        let Some(members) = self.final_members().filter(|_| self.final_voters() >= 3) else { return };
        // Damage on a node that's left the members counts too: the leader's members may not be
        // committed, and the node not gone.
        let damaged = self
            .nodes
            .values()
            .any(|n| n.broken || n.disk.rotten.is_some() || n.disk.rotten_record.is_some() || n.disk.rot_doubt);
        // A snapshot, or a record of the log.
        let snapshot = self.rng.one_in(2);
        let candidates: Vec<NodeId> =
            members
                .iter()
                .copied()
                .filter(|id| {
                    if snapshot { self.nodes[id].disk.snapshot.is_some() } else { !self.nodes[id].disk.log.is_empty() }
                })
                .collect();
        if damaged || candidates.is_empty() {
            return;
        }
        let id = self.rng.pick(&candidates);
        let disk = &mut self.nodes.get_mut(&id).unwrap().disk;
        if snapshot {
            disk.rot(&mut self.rng);
        } else {
            disk.rot_record(&mut self.rng);
        }
        self.rots += u64::from(disk.rotten.is_some() || disk.rotten_record.is_some());
    }

    /// Power lost partway through writing `ops`: some prefix of them reaches the disk, the last
    /// perhaps only some of its records.
    fn tear_writes(&mut self, id: NodeId, ops: &[Persist]) {
        let mut whole = usize::try_from(self.rng.below(ops.len() as u64)).unwrap();
        // Often in a leader's snapshot replacing the log, as there.
        let reset = ops.iter().position(|op| matches!(op, Persist::InstallSnapshot { replace_log: true, .. }));
        if let Some(at) = reset.filter(|_| self.rng.below(2) == 0) {
            whole = at;
        }
        let disk = &mut self.nodes.get_mut(&id).unwrap().disk;
        for op in &ops[..whole] {
            // Crashing anyway.
            if disk.apply(op).is_err() {
                break;
            }
        }
        match &ops[whole] {
            Persist::Append(records) => {
                let all: Vec<Record<'_>> = codec::records(records).map(Result::unwrap).collect();
                let some = usize::try_from(self.rng.below(all.len() as u64)).unwrap();
                self.covered.torn_mid_batch += u64::from(disk.tear(&all, some, &mut self.rng));
                self.covered.torn += 1;
            }
            Persist::InstallSnapshot { snapshot, replace_log: true } => {
                self.covered.torn_resets += u64::from(disk.tear_reset(snapshot, &mut self.rng).is_ok());
            }
            _ => {}
        }
    }

    fn start(&mut self, id: NodeId) {
        let sim_node = self.nodes.get_mut(&id).unwrap();
        if sim_node.node.is_some() || sim_node.broken || sim_node.lost {
            return;
        }
        let members = if id.0 <= self.initial && !sim_node.wiped {
            (1..=self.initial).map(|id| member(NodeId(id))).collect()
        } else {
            Vec::new()
        };
        let mut config = node::Config::new(id, members);
        config.raft.election_timeout = Duration::from_millis(150);
        config.raft.seed = self.seed ^ (u64::from(id) << 32) ^ sim_node.incarnation;
        config.raft.log_window_bytes = self.window;
        // None, some, or more than a snapshot's interval.
        config.raft.log_tail_entries = [0, 50, 1000][usize::try_from(self.seed % 3).unwrap()];
        config.snapshot_every_entries = 300;
        // Covers a client's messages in flight, each with its response and some a timer's.
        config.egress_ring_entries = 2 * client::MAX_UNACKED;
        config.protocol = u32::from(sim_node.upgraded);
        // Its log or snapshot is damaged: it refuses to start.
        let log = match sim_node.disk.open(self.window) {
            Ok((log, cut)) => {
                self.covered.rot_cut += u64::from(cut);
                log
            }
            Err(e) if e.kind() == io::ErrorKind::InvalidData => {
                self.break_disk(id);
                return;
            }
            Err(e) => panic!("seed {}: node {id} can't open its log: {e}", self.seed),
        };
        let sim_node = self.nodes.get_mut(&id).unwrap();
        let Ok(loaded) = sim_node.disk.load_snapshot() else {
            self.break_disk(id);
            return;
        };
        let sim_node = self.nodes.get_mut(&id).unwrap();
        let snapshot = loaded.as_ref().map(|(s, _)| s.clone());
        let echo = Echo { version: u32::from(sim_node.upgraded), ..Echo::default() };
        let mut node = Node::new(config, echo, sim_node.disk.meta, snapshot, log, self.now);
        if let Some((_, mut bytes)) = loaded {
            node.restore(&mut bytes).unwrap();
        }
        sim_node.node = Some(node);
        sim_node.checked = 0;
        sim_node.tick_at = None;
        sim_node.stalled_until = 0;
        (sim_node.pipeline, sim_node.writing, sim_node.written_at) = (Pipeline::default(), Vec::new(), None);
        self.flush(id);
    }

    /// The cluster, settled, is about to be lost, with the clients sending again. Picks how
    /// it'll be recovered, and readies that: a learner to promote, made one if need be, on the
    /// new version; or a snapshot, copied off a member a while before the loss. Returns when
    /// the clients will have stopped and the recovered cluster settled.
    fn begin_disaster(&mut self) -> u64 {
        let leader = self.leader().expect("settled");
        let raft = self.nodes[&leader].node.as_ref().unwrap().raft();
        let old_epoch = self.nodes[&leader].node.as_ref().unwrap().epoch();
        let members: Vec<NodeId> = raft.members().iter().map(|m| m.id).collect();
        let learners: Vec<NodeId> = raft.members().iter().filter(|m| m.learner).map(|m| m.id).collect();
        let others: Vec<NodeId> = self.members.iter().copied().filter(|id| !members.contains(id)).collect();
        let restore = self.rng.one_in(2);
        let lose_after = 2000 * MS + self.rng.below(1000 * MS);
        // The one others' messages notify: once idle, what it's sent comes of theirs.
        let first = self.nodes[&leader].node.as_ref().unwrap().service().last.keys().next().copied();
        let idle_client = (0..CLIENTS).find(|&c| self.clients[c].client.session() == first).expect("a client is first");
        let mut idle_at = u64::MAX;
        let standby = if restore {
            let backup_at = 1000 * MS + self.rng.below(500 * MS);
            idle_at = self.now + backup_at - 600 * MS;
            self.schedule(backup_at - 500 * MS, Event::AdminRequest);
            self.schedule(backup_at, Event::Backup);
            self.rng.pick(&self.members)
        } else {
            let request_id = self.rng.next() & !ROLE_REQUEST;
            let standby = if let Some(&learner) = learners.first() {
                learner
            } else if let Some(&other) = others.first() {
                let m = member(other);
                let (peer, client, admin) = (m.peer.as_bytes(), m.client.as_bytes(), m.admin.as_bytes());
                let id = m.id.0;
                self.admin(&turbofan_cluster::wire::AddMember { request_id, id, learner: 1, peer, client, admin });
                other
            } else {
                let voter = *members.iter().find(|&&id| id != leader).expect("more than one member");
                self.admin(&turbofan_cluster::wire::SetRole { request_id, id: voter.0, learner: 1 });
                voter
            };
            // It takes over as the new version, whatever it ran: restarted on that.
            let sim_node = self.nodes.get_mut(&standby).unwrap();
            if !sim_node.upgraded {
                sim_node.upgraded = true;
                sim_node.node = None;
                sim_node.incarnation += 1;
                self.schedule(10 * MS, Event::Restart(standby));
            }
            // Cut off for a while before the loss, as a link to another region is, or not at all.
            if !self.rng.one_in(3) {
                let cut_at = 1000 * MS + self.rng.below(lose_after - 1000 * MS);
                idle_at = self.now + cut_at - 300 * MS - self.rng.below(300 * MS);
                self.schedule(cut_at, Event::CutStandby);
            }
            standby
        };
        self.schedule(lose_after, Event::Lose);
        let recover_after = lose_after + 500 * MS;
        self.schedule(recover_after, Event::Recover);
        let idle = (idle_client, idle_at);
        self.disaster = Some(Disaster { restore, standby, old_epoch, backup: None, recovered: false, idle });
        self.send_until = self.now + recover_after + 1500 * MS;
        // Their sends stopped with the run: they start again.
        for c in 0..CLIENTS {
            let after = MS + self.rng.below(15 * MS);
            self.schedule(after, Event::ClientSend(c));
        }
        self.send_until + DISASTER_QUIET
    }

    /// Every node is started again, after a shutdown.
    fn start_all(&mut self) {
        self.started_again = true;
        for id in self.members.clone() {
            self.start(id);
        }
    }

    /// The standby, a learner, is cut off from the cluster: it falls behind.
    fn cut_standby(&mut self) {
        let standby = self.disaster.as_ref().unwrap().standby;
        for from in 0..8 {
            self.cut[from][usize::from(standby)] = true;
        }
    }

    /// The operator copies the newest snapshot off a member, as a backup does.
    fn backup(&mut self) {
        let members = self.final_members().expect("settled");
        let id = self.rng.pick(&members);
        let dir = self.nodes[&id].disk.snapshots.path();
        let newest = std::fs::read_dir(dir)
            .unwrap()
            .map(|e| e.unwrap().path())
            .filter(|p| p.extension().is_some_and(|e| e == "snapshot"))
            .max()
            .expect("a member has snapshotted");
        let to = tempfile::tempdir().unwrap();
        let backup = to.path().join("backup.snapshot");
        std::fs::copy(newest, &backup).unwrap();
        self.disaster.as_mut().unwrap().backup = Some((to, backup));
    }

    /// Every node is lost but the standby, if it's a learner, and stays down: the operator
    /// fences the old cluster before recovering.
    fn lose(&mut self) {
        let disaster = self.disaster.as_ref().unwrap();
        let kept = (!disaster.restore).then_some(disaster.standby);
        for id in self.members.clone().into_iter().filter(|&id| Some(id) != kept) {
            let sim_node = self.nodes.get_mut(&id).unwrap();
            (sim_node.node, sim_node.lost) = (None, true);
            sim_node.incarnation += 1;
            let conns: Vec<ConnId> = self.conns.iter().filter(|(_, c)| c.up && c.node == id).map(|(&k, _)| k).collect();
            for conn in conns {
                self.reset(conn);
            }
        }
        self.cut = [[false; 8]; 8];
    }

    /// The operator recovers: promotes the standby, or restores the backup onto it, as a node
    /// of its own. The new cluster's history is its own from there: what the lost one committed
    /// after is no longer what its entries must match, and its terms and snapshots are new.
    fn recover(&mut self) {
        let seed = self.seed;
        let disaster = self.disaster.as_mut().unwrap();
        disaster.recovered = true;
        let id = disaster.standby;
        self.leaders.clear();
        self.snapshots.clear();
        if disaster.restore {
            let (_, backup) = disaster.backup.as_ref().expect("a backup was taken");
            let restored = tempfile::tempdir().unwrap();
            let unused = "127.0.0.1:1".parse().unwrap();
            let node = node::Config::new(id, vec![member(id)]);
            let config = driver::Config::new(node, 1, restored.path(), vec![(id, unused)], unused, unused);
            let (index, _) = driver::restore(backup, &config).unwrap();
            let sim_node = self.nodes.get_mut(&id).unwrap();
            sim_node.disk = Disk::restored(restored, self.segment_bytes);
            (sim_node.lost, sim_node.broken, sim_node.wiped, sim_node.upgraded) = (false, false, false, true);
            self.committed.truncate(usize::try_from(index).unwrap());
            self.commit_terms.truncate(usize::try_from(index).unwrap());
            self.covered.restores += 1;
            self.start(id);
        } else {
            let node = self.nodes[&id].node.as_ref().unwrap_or_else(|| panic!("seed {seed}: the standby is down"));
            let learner = node.raft().members().iter().any(|m| m.id == id && m.learner);
            assert!(learner, "seed {seed}: the standby, {id}, isn't a learner");
            let commit = usize::try_from(node.raft().commit_index()).unwrap();
            self.committed.truncate(commit);
            self.commit_terms.truncate(commit);
            self.covered.promotions += 1;
            let conn = self.next_conn;
            self.next_conn += 1;
            self.conns.insert(
                conn,
                Conn { client: ADMIN, node: id, up: true, to_node_at: 0, to_client_at: 0, opened: VecDeque::new() },
            );
            let mut frame = Vec::new();
            codec::encode_frame(&turbofan_cluster::wire::Promote { request_id: self.rng.next() }, &mut frame).unwrap();
            let node = self.nodes.get_mut(&id).unwrap().node.as_mut().unwrap();
            node.on_admin(conn, &codec::decode_frame(&frame).unwrap().unwrap().0, self.now);
            self.flush(id);
        }
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
                        raft.entry(index).or_else(|| sim_node.disk.record(index)),
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

    /// Every client's messages answered and applied, its timers fired and proposals applied,
    /// once each.
    fn check_clients_end(&self) {
        let seed = self.seed;
        for (c, sim_client) in self.clients.iter().enumerate() {
            // A second session, opened after a recovery, has had less time.
            let least = if self.disaster.is_some() { 10 } else { 100 };
            assert!(sim_client.sent > least, "seed {seed}: client {c} sent only {}", sim_client.sent);
            assert_eq!(sim_client.answered, sim_client.sent, "seed {seed}: client {c} unanswered");
            assert_eq!(sim_client.client.unacked(), 0);
            assert_eq!(sim_client.client.applied(), sim_client.sent, "seed {seed}: client {c} applied");
            let expected: BTreeSet<u64> = (1..=sim_client.sent).filter(|k| k.is_multiple_of(TIMER_EVERY)).collect();
            assert_eq!(sim_client.timers, expected, "seed {seed}: client {c} timers");
            let expected: BTreeSet<u64> = (1..=sim_client.sent).filter(|k| k.is_multiple_of(PROPOSE_EVERY)).collect();
            assert_eq!(sim_client.proposals, expected, "seed {seed}: client {c} proposals");
        }
    }

    /// After the faults stop: one leader, the same log and service state on every member, and
    /// every client's messages answered and timers fired, once each.
    fn check_end(&self) {
        let seed = self.seed;
        let up = self.nodes.values().filter(|n| !n.lost).map(|n| n.node.as_ref().expect("all up but the lost"));
        let leaders = up.filter(|n| n.raft().role() == Role::Leader);
        assert_eq!(leaders.count(), 1, "seed {seed}: not one leader after the faults stopped");
        let members = self.final_members().expect("a leader");
        let nodes: Vec<&Node<Echo>> = members.iter().map(|id| self.nodes[id].node.as_ref().unwrap()).collect();
        let disks: Vec<&Disk> = members.iter().map(|id| &self.nodes[id].disk).collect();
        let commit = nodes[0].raft().commit_index();
        assert_ne!(nodes[0].epoch(), 0, "seed {seed}: no epoch");
        if let Some(disaster) = &self.disaster {
            assert_ne!(nodes[0].epoch(), disaster.old_epoch, "seed {seed}: recovered with the lost cluster's epoch");
        }
        for (node, disk) in nodes.iter().zip(&disks) {
            assert_eq!(node.epoch(), nodes[0].epoch(), "seed {seed}: node {} of another epoch", node.raft().id());
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
                    node.raft().entry(index).or_else(|| disk.record(index)),
                    Some(self.committed[usize::try_from(index - 1).unwrap()].as_slice())
                );
            }
        }
        self.check_clients_end();
        if let Some((index, then)) = self.shut_down.clone() {
            let stopped = then.iter().filter(|id| self.stopped.iter().any(|(s, _)| s == *id)).count();
            assert!(stopped * 2 > then.len(), "seed {seed}: of {then:?}, only {:?} stopped", self.stopped);
            for &(id, snapshot) in &self.stopped {
                assert!(snapshot >= index, "seed {seed}: node {id} stopped without the shutdown's snapshot");
            }
        }
        let sent: u64 = self.clients.iter().map(|c| c.sent).sum();
        eprintln!(
            "seed {seed}: members {members:?} after {} changes from {}, {} handovers, {} terms, {} crashes, {commit} committed, {sent} messages, {} snapshots, {} installed, {}/{} manual, {} reads (window {}, chunk {}), {} stalls, {} rots, {} broken, {} cut at rot, {} torn ({} mid-batch), {} overridden, {} segments compacted (of {} bytes)",
            self.member_changes,
            self.initial,
            self.transfers,
            self.leaders.len(),
            self.crashes,
            self.snapshots.len(),
            self.installs,
            self.admin_answers,
            self.admin_requests,
            self.reads,
            self.window,
            self.chunk,
            self.stalls,
            self.rots,
            self.broken,
            self.covered.rot_cut,
            self.covered.torn,
            self.covered.torn_mid_batch,
            self.overrides,
            self.covered.segments_compacted,
            self.segment_bytes
        );
    }
}

/// Node `id`'s addresses, which the simulation names rather than uses.
fn member(id: NodeId) -> Member {
    Member { id, peer: format!("p{id}"), client: format!("n{id}"), admin: format!("a{id}"), learner: false }
}
