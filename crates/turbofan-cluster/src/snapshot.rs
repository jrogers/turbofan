//! Snapshot files: `<index>.snapshot` in the data directory, a header (magic, index, term,
//! length, checksum), the bytes, then the members as of the index (`SnapshotMembers`, framed as
//! on the wire). The checksum covers the bytes, the members, then the header's index, term and
//! length. Each is written to a temporary file, synced, renamed into
//! place and the directory synced, so a crash leaves the previous snapshot or the new one,
//! whole. A node's own snapshots are written by [`save`], on a thread of their own while the node
//! goes on; one arriving from the leader by [`write_chunk`] and [`install`]. Older ones are
//! removed by [`remove_older`], but the newest so many kept, once the node has moved on to a
//! newer one, since until then it may still be reading chunks of them for a follower. A copy
//! taken off the node is a backup, read back by [`open`].

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::codec::{self, CHECKSUM_START, checksum_on};
use crate::node::SnapshotWriter;
use crate::raft::{Member, NodeId, Snapshot};
use crate::wire;

const MAGIC: &[u8; 8] = b"CLSNAP01";
/// magic · index · term · length · checksum.
const HEADER: usize = 8 + 8 * 4;
/// Where a node writes its own snapshot, and one arriving from the leader.
const SAVING: &str = "saving.tmp";
const INCOMING: &str = "incoming.tmp";

/// Writes the snapshot of `index`, with `members`, that `writer` makes, and returns it: synced,
/// if `durable`, which only a test that models crashes itself would skip.
///
/// # Errors
///
/// If `writer`, writing, syncing or renaming fails. The previous snapshot is then still in place.
pub fn save(
    dir: &Path,
    index: u64,
    term: u64,
    members: Vec<Member>,
    writer: SnapshotWriter,
    durable: bool,
) -> io::Result<Snapshot> {
    let temp = dir.join(SAVING);
    let mut file = File::create(&temp)?;
    file.write_all(&[0; HEADER])?;
    let mut body = Summed { out: BufWriter::new(file), len: 0, sum: CHECKSUM_START };
    writer(&mut body)?;
    let len = body.len;
    body.write_all(&encode_members(&members))?;
    let file = body.out.into_inner().map_err(io::IntoInnerError::into_error)?;
    let checksum = seal(body.sum, index, term, len);
    let snapshot = Snapshot { index, term, len, checksum, members };
    finish(dir, &temp, file, &snapshot, durable)?;
    Ok(snapshot)
}

/// Writes part of a snapshot arriving from the leader, unsynced. Offset 0 starts it afresh.
///
/// # Errors
///
/// If writing fails.
pub fn write_chunk(dir: &Path, offset: u64, bytes: &[u8]) -> io::Result<()> {
    let temp = dir.join(INCOMING);
    let mut file = if offset == 0 { File::create(&temp)? } else { OpenOptions::new().write(true).open(&temp)? };
    file.seek(SeekFrom::Start(crate::widen(HEADER) + offset))?;
    file.write_all(bytes)
}

/// Makes the snapshot that [`write_chunk`] wrote, whole, the latest: synced, if `durable`.
///
/// # Errors
///
/// If it isn't all there, or isn't the leader's (its checksum doesn't match), or reading,
/// syncing or renaming fails.
pub fn install(dir: &Path, snapshot: &Snapshot, durable: bool) -> io::Result<()> {
    let temp = dir.join(INCOMING);
    let mut file = OpenOptions::new().read(true).write(true).open(&temp)?;
    if sealed(sum_body(&mut file, snapshot.len)?, snapshot) != snapshot.checksum {
        let what = format!("snapshot {}: not the leader's, by its checksum", snapshot.index);
        return Err(io::Error::new(io::ErrorKind::InvalidData, what));
    }
    let members = encode_members(&snapshot.members);
    file.seek(SeekFrom::Start(crate::widen(HEADER) + snapshot.len))?;
    file.write_all(&members)?;
    file.set_len(crate::widen(HEADER) + snapshot.len + crate::widen(members.len()))?;
    finish(dir, &temp, file, snapshot, durable)
}

/// Removes the snapshots older than `index`, but the newest `kept - 1` of them: `kept` with the
/// one of `index`, for an operator to copy off the node or restore from.
///
/// # Errors
///
/// If one can't be removed.
pub fn remove_older(dir: &Path, index: u64, kept: usize) -> io::Result<()> {
    let older: Vec<u64> = list(dir)?.into_iter().filter(|&i| i < index).collect();
    for &old in &older[..older.len().saturating_sub(kept.saturating_sub(1))] {
        fs::remove_file(path(dir, old))?;
    }
    Ok(())
}

/// The latest snapshot in `dir`, if there is one, checked whole, and its bytes to read.
///
/// # Errors
///
/// If it can't be read or is damaged: it was synced, so that's the disk's fault, and the node
/// should be rebuilt from its peers.
pub fn load(dir: &Path) -> io::Result<Option<(Snapshot, io::Take<BufReader<File>>)>> {
    let Some(&index) = list(dir)?.last() else { return Ok(None) };
    let (snapshot, file) = open_checked(dir, index)?;
    let len = snapshot.len;
    Ok(Some((snapshot, BufReader::new(file).take(len))))
}

/// The latest snapshot's index, and its file, if there is one: unchecked.
///
/// # Errors
///
/// If `dir` can't be read.
pub fn latest(dir: &Path) -> io::Result<Option<(u64, PathBuf)>> {
    Ok(list(dir)?.last().map(|&index| (index, path(dir, index))))
}

/// The snapshot file at `file`, wherever it is (a copy taken off a node, say), checked whole, and
/// its bytes to read.
///
/// # Errors
///
/// If it can't be read, isn't a snapshot, or is damaged.
pub fn open(file: &Path) -> io::Result<(Snapshot, io::Take<BufReader<File>>)> {
    let (snapshot, file) = open_file(file, None)?;
    let len = snapshot.len;
    Ok((snapshot, BufReader::new(file).take(len)))
}

/// The snapshot of `index`, checked whole, and its file at its bytes.
fn open_checked(dir: &Path, index: u64) -> io::Result<(Snapshot, File)> {
    open_file(&path(dir, index), Some(index))
}

/// The snapshot at `at`, of `index` if given, checked whole, and its file at its bytes.
fn open_file(at: &Path, index: Option<u64>) -> io::Result<(Snapshot, File)> {
    let name = at.display();
    let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("snapshot {name}: {what}"));
    let mut file = File::open(at)?;
    let mut header = [0; HEADER];
    file.read_exact(&mut header).map_err(|_| invalid("not a snapshot"))?;
    let word = |i: usize| u64::from_le_bytes(header[8 + i * 8..16 + i * 8].try_into().expect("eight bytes"));
    let (stored, term, len, sum) = (word(0), word(1), word(2), word(3));
    if &header[..8] != MAGIC || index.is_some_and(|index| index != stored) {
        return Err(invalid("not a snapshot"));
    }
    let index = stored;
    // From a header not yet checked: a garbled length mustn't overflow.
    let end = crate::widen(HEADER).checked_add(len).ok_or_else(|| invalid("wrong length"))?;
    if file.metadata()?.len() < end {
        return Err(invalid("wrong length"));
    }
    let body_sum = sum_body(&mut file, len)?;
    let mut tail = Vec::new();
    file.read_to_end(&mut tail)?;
    if seal(checksum_on(body_sum, &tail), index, term, len) != sum {
        return Err(invalid("checksum mismatch"));
    }
    let members = decode_members(&tail).ok_or_else(|| invalid("bad members"))?;
    file.seek(SeekFrom::Start(crate::widen(HEADER)))?;
    Ok((Snapshot { index, term, len, checksum: sum, members }, file))
}

/// Up to `max` bytes of the snapshot of `index`, from `offset`: of its bytes, not the members
/// after them. Reading from the start, as for a follower it's to be sent to, checks the whole
/// file first: damage is never served.
///
/// # Errors
///
/// If it can't be read, or it's damaged.
pub fn read(dir: &Path, index: u64, offset: u64, max: usize) -> io::Result<Vec<u8>> {
    if offset == 0 {
        open_checked(dir, index)?;
    }
    let mut file = File::open(path(dir, index))?;
    let mut header = [0; HEADER];
    file.read_exact(&mut header)?;
    let len = u64::from_le_bytes(header[24..32].try_into().expect("eight bytes"));
    file.seek(SeekFrom::Start(crate::widen(HEADER) + offset))?;
    let mut out = Vec::new();
    file.take(crate::widen(max).min(len.saturating_sub(offset))).read_to_end(&mut out)?;
    Ok(out)
}

/// Writes the header, syncs if `durable`, and renames `temp` into place.
fn finish(dir: &Path, temp: &Path, mut file: File, snapshot: &Snapshot, durable: bool) -> io::Result<()> {
    file.seek(SeekFrom::Start(0))?;
    file.write_all(MAGIC)?;
    for v in [snapshot.index, snapshot.term, snapshot.len, snapshot.checksum] {
        file.write_all(&v.to_le_bytes())?;
    }
    if durable {
        file.sync_all()?;
    }
    fs::rename(temp, path(dir, snapshot.index))?;
    if durable {
        File::open(dir)?.sync_all()?;
    }
    Ok(())
}

/// The checksum of the snapshot of `index` and `term`, with `members`, whose bytes are `body`:
/// what [`save`] writes in its header, for one kept in memory (`crate::testing`).
#[cfg_attr(not(any(test, feature = "testing")), expect(dead_code, reason = "only a cluster in memory needs it"))]
pub(crate) fn checksum(index: u64, term: u64, members: &[Member], body: &[u8]) -> u64 {
    let snapshot = Snapshot { index, term, len: crate::widen(body.len()), checksum: 0, members: members.to_vec() };
    sealed(checksum_on(CHECKSUM_START, body), &snapshot)
}

/// The checksum of `snapshot`, its bytes' being `body_sum` (from [`CHECKSUM_START`]): what the
/// leader's header says, if they arrived as it sent them.
pub(crate) fn sealed(body_sum: u64, snapshot: &Snapshot) -> u64 {
    let sum = checksum_on(body_sum, &encode_members(&snapshot.members));
    seal(sum, snapshot.index, snapshot.term, snapshot.len)
}

fn encode_members(members: &[Member]) -> Vec<u8> {
    let member: Vec<wire::SnapshotMembersMember<'_>> = members
        .iter()
        .map(|m| wire::SnapshotMembersMember {
            id: m.id.0,
            learner: u8::from(m.learner),
            peer: m.peer.as_bytes(),
            client: m.client.as_bytes(),
            admin: m.admin.as_bytes(),
        })
        .collect();
    let mut out = Vec::new();
    codec::encode_frame(&wire::SnapshotMembers { member: &member }, &mut out).expect("addresses fit the wire");
    out
}

fn decode_members(bytes: &[u8]) -> Option<Vec<Member>> {
    let (wire::Decoded::SnapshotMembers(m), used) = codec::decode_frame(bytes).ok()?? else { return None };
    let text = |b: &[u8]| String::from_utf8(b.to_vec()).ok();
    let members = m.member().map(|a| {
        let (peer, client, admin) = (text(a.peer())?, text(a.client())?, text(a.admin())?);
        Some(Member { id: NodeId(a.id()), peer, client, admin, learner: a.learner() == 1 })
    });
    (used == bytes.len()).then_some(members.collect::<Option<Vec<_>>>()?)
}

/// The checksum of the bytes and members, `sum`, then of the header's fields, so that they're
/// checked too.
fn seal(sum: u64, index: u64, term: u64, len: u64) -> u64 {
    let fields: Vec<u8> = [index, term, len].iter().flat_map(|v| v.to_le_bytes()).collect();
    checksum_on(sum, &fields)
}

/// The checksum of the `len` bytes after the header, leaving the file after them.
fn sum_body(file: &mut File, len: u64) -> io::Result<u64> {
    file.seek(SeekFrom::Start(crate::widen(HEADER)))?;
    let mut reader = BufReader::new(&mut *file).take(len);
    let (mut sum, mut read) = (CHECKSUM_START, 0);
    loop {
        let buf = reader.fill_buf()?;
        if buf.is_empty() {
            break;
        }
        (sum, read) = (checksum_on(sum, buf), read + crate::widen(buf.len()));
        let n = buf.len();
        reader.consume(n);
    }
    if read != len {
        return Err(io::Error::new(io::ErrorKind::UnexpectedEof, "snapshot cut short"));
    }
    drop(reader);
    file.seek(SeekFrom::Start(crate::widen(HEADER) + len))?;
    Ok(sum)
}

/// Writes through, counting and summing what it writes.
struct Summed<W> {
    out: W,
    len: u64,
    sum: u64,
}

impl<W: Write> Write for Summed<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let n = self.out.write(buf)?;
        self.len += crate::widen(n);
        self.sum = checksum_on(self.sum, &buf[..n]);
        Ok(n)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.out.flush()
    }
}

fn list(dir: &Path) -> io::Result<Vec<u64>> {
    let mut indices = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        if let Some(index) = name.to_str().and_then(|n| n.strip_suffix(".snapshot")).and_then(|n| n.parse().ok()) {
            indices.push(index);
        }
    }
    indices.sort_unstable();
    Ok(indices)
}

fn path(dir: &Path, index: u64) -> PathBuf {
    dir.join(format!("{index:020}.snapshot"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn members() -> Vec<Member> {
        let member = |id, learner| Member {
            id: NodeId(id),
            peer: format!("p{id}"),
            client: format!("c{id}"),
            admin: String::new(),
            learner,
        };
        vec![member(1, false), member(2, true)]
    }

    fn writer(bytes: &'static [u8]) -> SnapshotWriter {
        Box::new(move |out| out.write_all(bytes))
    }

    fn load_bytes(dir: &Path) -> Option<(Snapshot, Vec<u8>)> {
        let (snapshot, mut reader) = load(dir).unwrap()?;
        let mut bytes = Vec::new();
        reader.read_to_end(&mut bytes).unwrap();
        Some((snapshot, bytes))
    }

    #[test]
    fn the_latest_snapshot_is_loaded_and_older_ones_removed() {
        let dir = tempfile::tempdir().unwrap();
        assert!(load_bytes(dir.path()).is_none());
        save(dir.path(), 10, 2, members(), writer(b"ten"), true).unwrap();
        let second = save(dir.path(), 20, 3, members(), writer(b"twenty"), true).unwrap();
        assert_eq!((second.index, second.term, second.len, second.members.clone()), (20, 3, 6, members()));
        assert_eq!(load_bytes(dir.path()), Some((second, b"twenty".to_vec())));
        assert_eq!(read(dir.path(), 20, 2, 3).unwrap(), b"ent");
        assert_eq!(read(dir.path(), 20, 4, 100).unwrap(), b"ty", "not the members after it");
        assert_eq!(list(dir.path()).unwrap(), [10, 20], "kept until the node moves on");
        remove_older(dir.path(), 20, 1).unwrap();
        assert_eq!(list(dir.path()).unwrap(), [20]);
    }

    /// A snapshot kept in memory has the checksum its file would.
    #[test]
    fn a_snapshot_in_memory_is_checksummed_as_its_file() {
        let dir = tempfile::tempdir().unwrap();
        let saved = save(dir.path(), 10, 2, members(), writer(b"state"), true).unwrap();
        assert_eq!(checksum(10, 2, &members(), b"state"), saved.checksum);
        assert_ne!(checksum(10, 3, &members(), b"state"), saved.checksum, "its term counts");
    }

    /// Kept: the newest so many, the one moved on to among them; never one it may still read.
    #[test]
    fn so_many_snapshots_are_kept() {
        let dir = tempfile::tempdir().unwrap();
        for index in [10, 20, 30, 40] {
            save(dir.path(), index, 2, members(), writer(b"state"), true).unwrap();
        }
        remove_older(dir.path(), 30, 2).unwrap();
        assert_eq!(list(dir.path()).unwrap(), [20, 30, 40], "the newest of those before 30, and 30 on");
        remove_older(dir.path(), 40, 3).unwrap();
        assert_eq!(list(dir.path()).unwrap(), [20, 30, 40]);
        remove_older(dir.path(), 40, 0).unwrap();
        assert_eq!(list(dir.path()).unwrap(), [40], "none is one: the newest");
    }

    /// A file that isn't the snapshot its name says is refused, each way it can fail on its own:
    /// another magic, another index, or cut short.
    #[test]
    fn a_snapshot_that_isnt_one_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        save(dir.path(), 10, 2, members(), writer(b"state"), true).unwrap();
        let good = fs::read(path(dir.path(), 10)).unwrap();
        let refused = |bytes: &[u8], what: &str| {
            fs::write(path(dir.path(), 10), bytes).unwrap();
            let error = load(dir.path()).unwrap_err();
            assert_eq!(error.kind(), io::ErrorKind::InvalidData);
            assert!(error.to_string().contains(what), "{error}");
        };
        let mut magic = good.clone();
        magic[0] ^= 1;
        refused(&magic, "not a snapshot");
        let mut index = good.clone();
        index[8] ^= 1;
        refused(&index, "not a snapshot");
        refused(&good[..good.len() - 1], "checksum mismatch");
        refused(&good[..HEADER + 4], "wrong length");
        fs::write(path(dir.path(), 10), &good).unwrap();
        assert!(load(dir.path()).unwrap().is_some(), "and whole, it's read");
    }

    /// A copy of a snapshot, wherever it's put, opens checked, and damaged it doesn't.
    #[test]
    fn a_copied_snapshot_opens_checked() {
        let dir = tempfile::tempdir().unwrap();
        save(dir.path(), 10, 2, members(), writer(b"state"), true).unwrap();
        let copy = dir.path().join("backup");
        fs::copy(path(dir.path(), 10), &copy).unwrap();
        let (snapshot, mut bytes) = open(&copy).unwrap();
        let mut read = Vec::new();
        bytes.read_to_end(&mut read).unwrap();
        assert_eq!((snapshot.index, snapshot.term, snapshot.members, read), (10, 2, members(), b"state".to_vec()));
        let mut damaged = fs::read(&copy).unwrap();
        damaged[HEADER] ^= 1;
        fs::write(&copy, damaged).unwrap();
        assert_eq!(open(&copy).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn a_snapshot_from_the_leader_is_installed_from_its_chunks() {
        let dir = tempfile::tempdir().unwrap();
        write_chunk(dir.path(), 0, b"stale").unwrap();
        write_chunk(dir.path(), 0, b"thi").unwrap();
        write_chunk(dir.path(), 3, b"rty").unwrap();
        let checksum = seal(checksum_on(crate::codec::checksum(b"thirty"), &encode_members(&members())), 30, 4, 6);
        let wrong = Snapshot { index: 30, term: 4, len: 6, checksum: checksum ^ 1, members: members() };
        assert!(install(dir.path(), &wrong, true).is_err(), "not what the leader sent");
        let snapshot = Snapshot { checksum, ..wrong };
        install(dir.path(), &snapshot, true).unwrap();
        assert_eq!(load_bytes(dir.path()), Some((snapshot, b"thirty".to_vec())));
        write_chunk(dir.path(), 0, b"fo").unwrap();
        let short = Snapshot { index: 40, term: 4, len: 5, checksum: 0, members: Vec::new() };
        assert!(install(dir.path(), &short, true).is_err(), "cut short");
    }

    #[test]
    fn a_damaged_snapshot_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        save(dir.path(), 10, 2, members(), writer(b"ten"), true).unwrap();
        let file = path(dir.path(), 10);
        let mut bytes = fs::read(&file).unwrap();
        bytes[HEADER] ^= 1;
        fs::write(&file, bytes).unwrap();
        assert_eq!(load(dir.path()).err().unwrap().kind(), io::ErrorKind::InvalidData);
        assert!(read(dir.path(), 10, 0, 100).is_err(), "nor sent to a follower");

        // Its term, in the header, is checked too.
        save(dir.path(), 11, 2, members(), writer(b"eleven"), true).unwrap();
        let file = path(dir.path(), 11);
        let mut bytes = fs::read(&file).unwrap();
        bytes[16 + 6] ^= 1 << 4;
        fs::write(&file, bytes).unwrap();
        assert_eq!(load(dir.path()).err().unwrap().kind(), io::ErrorKind::InvalidData, "a damaged term");

        // Its length, past any file's, is an error, not an overflow.
        let mut bytes = fs::read(&file).unwrap();
        bytes[24..32].copy_from_slice(&u64::MAX.to_le_bytes());
        fs::write(&file, bytes).unwrap();
        assert_eq!(load(dir.path()).err().unwrap().kind(), io::ErrorKind::InvalidData, "a damaged length");
    }
}
