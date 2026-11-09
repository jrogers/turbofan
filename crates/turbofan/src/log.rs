//! The log on disk: [`Record`]s appended to segment files named by their first index.
//!
//! Appends are written straight away and, with [`Fsync::EachBatch`], made durable by [`Log::sync`],
//! which the driver calls once per batch (group commit), the default. With [`Fsync::Never`] the log
//! isn't synced: a crashed process loses nothing, but a machine losing power may lose what it
//! acknowledged, and the copies on the other nodes are what keep it.
//!
//! On open, the log is truncated at its first damaged record, and at a gap between segments: the
//! records after it, and the segments, go. A power cut mid-write leaves that: a torn last record;
//! or, the disk keeping a later page of a batch and losing an earlier one, a torn record with
//! whole ones after it; or, unsynced, a segment's tail lost and the next one kept. Bit rot looks
//! the same, so what's truncated may have been synced and acknowledged. The log is then [in
//! doubt](Log::in_doubt), durably, until Raft has caught it up ([`Log::trust`]): a `doubt` file,
//! written before anything is truncated. However much was dropped, the doubt keeps elections
//! safe, and the node catches up from the others rather than refusing to start.
//!
//! [`Log::read`] reads records back, for a follower that's behind what the node keeps in memory,
//! and checks them: damage is an error there too, never served.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::mpsc;

use crate::codec::{CodecError, MAX_RECORD_BYTES, Record};
use crate::raft::Entries;

/// A segment is closed and a new one started once it holds this much.
pub const MAX_SEGMENT_BYTES: u64 = 64 << 20;

/// With [`Fsync::Never`], each time this much is written, the segment's data is synced on a thread
/// of its own that nothing waits for. Else the OS lets unwritten data pile up, then makes every
/// write wait while it writes it out: for seconds on a slow disk, on every node that shares it.
const FLUSH_BEHIND_BYTES: u64 = 4 << 20;

/// Whether the log is synced to disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Fsync {
    /// Never before acknowledging: the cluster's copies on other machines are the redundancy;
    /// a majority losing power together may lose committed entries. The log is still synced
    /// behind the writes, on a thread of its own, so that unwritten data doesn't pile up.
    Never,
    /// Once a batch, before it's acknowledged: committed entries survive any crash.
    #[default]
    EachBatch,
}

/// An open log directory, locked against a second process.
#[derive(Debug)]
pub struct Log {
    dir: PathBuf,
    /// First index of each segment, oldest first; the last is being appended to.
    segments: Vec<u64>,
    file: File,
    file_len: u64,
    /// A segment is closed once it holds this much: [`MAX_SEGMENT_BYTES`] unless simulated.
    segment_bytes: u64,
    /// Syncs are made: with [`Fsync::EachBatch`], never simulated.
    sync: bool,
    /// A damaged tail was truncated, and the log hasn't been trusted since.
    doubt: bool,
    /// The index the next record must have.
    next_index: u64,
    /// A segment was created or removed since the directory was last synced.
    dir_dirty: bool,
    /// Where the last read stopped: its segment, the next index and its offset there.
    cursor: Option<(u64, u64, u64)>,
    /// With [`Fsync::Never`], where to send a handle on the segment to sync behind the writes,
    /// and how much has been written since the last.
    flush_behind: Option<mpsc::SyncSender<File>>,
    unflushed: u64,
    /// Held while the log is open, and while any handle [`Log::lock`] made is: one node's files,
    /// one node's at a time.
    lock: File,
}

impl Log {
    /// Opens or creates the log in `dir`, passing `each` every record it holds, in order. Only
    /// one segment is in memory at a time.
    ///
    /// # Errors
    ///
    /// If the directory is locked by another process, or can't be read or truncated.
    pub fn open(dir: &Path, fsync: Fsync, each: impl FnMut(Record<'_>)) -> io::Result<Self> {
        let mut log = Self::open_with(dir, MAX_SEGMENT_BYTES, fsync == Fsync::EachBatch, each)?;
        if fsync == Fsync::Never {
            log.flush_behind = Some(flusher()?);
        }
        Ok(log)
    }

    /// [`Log::open`], for a simulation, whose crashes are modelled: segments close once they
    /// hold `segment_bytes`, small so that many roll and compact, and nothing is synced.
    ///
    /// # Errors
    ///
    /// As [`Log::open`].
    pub fn open_simulated(dir: &Path, segment_bytes: u64, each: impl FnMut(Record<'_>)) -> io::Result<Self> {
        Self::open_with(dir, segment_bytes, false, each)
    }

    fn open_with(dir: &Path, segment_bytes: u64, sync: bool, mut each: impl FnMut(Record<'_>)) -> io::Result<Self> {
        if sync {
            create_dir_durably(dir)?
        } else {
            fs::create_dir_all(dir)?
        }
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("lock"))?;
        lock.try_lock().map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "log directory is in use"))?;
        let mut segments = list_segments(dir)?;
        if segments.is_empty() {
            segments.push(1);
            File::create(segment_path(dir, 1))?;
            if sync {
                sync_dir(dir)?;
            }
        }
        let mut bytes = Vec::new();
        let mut next_index = segments[0];
        let (mut file_len, mut damaged, mut kept) = (0, false, segments.len());
        for (i, &first) in segments.iter().enumerate() {
            // A gap: the segment before lost its tail, and this one and the rest go.
            if first != next_index {
                (damaged, kept) = (true, i);
                break;
            }
            bytes.clear();
            File::open(segment_path(dir, first))?.read_to_end(&mut bytes)?;
            let valid = scan(&bytes, &mut next_index);
            for record in crate::codec::records(&bytes[..valid]) {
                each(record.expect("scanned"));
            }
            file_len = crate::widen(valid);
            if valid < bytes.len() {
                (damaged, kept) = (true, i + 1);
                break;
            }
        }
        let doubt = dir.join("doubt").exists() || damaged;
        if damaged {
            cut_segments(dir, &mut segments, kept, sync)?;
        }
        let file = OpenOptions::new().write(true).open(segment_path(dir, segments[kept - 1]))?;
        // Drops what follows the damage; a no-op otherwise.
        file.set_len(file_len)?;
        if sync {
            file.sync_all()?;
        }
        let mut log = Self {
            dir: dir.into(),
            segments,
            file,
            file_len,
            segment_bytes,
            sync,
            doubt,
            next_index,
            dir_dirty: false,
            cursor: None,
            flush_behind: None,
            unflushed: 0,
            lock,
        };
        log.file.seek(SeekFrom::End(0))?;
        Ok(log)
    }

    /// Whether a damaged tail was truncated, which may have held records this node acknowledged,
    /// and the log hasn't been [trusted](Log::trust) since.
    #[must_use]
    pub fn in_doubt(&self) -> bool {
        self.doubt
    }

    /// Syncs, then ends the doubt: Raft has caught the log up.
    ///
    /// # Errors
    ///
    /// If syncing or removing the `doubt` file fails. One already gone, an operator's doing, is
    /// what's wanted.
    pub fn trust(&mut self) -> io::Result<()> {
        if self.doubt {
            self.sync()?;
            match fs::remove_file(self.dir.join("doubt")) {
                Err(e) if e.kind() != io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
            self.dir_dirty = true;
            self.sync()?;
            self.doubt = false;
        }
        Ok(())
    }

    /// Readies a log just opened, whose records are `entries`, for Raft. One that ends before the
    /// snapshot `(index, term)`, or holds another term at its index, goes on from the snapshot,
    /// without its records: a crash between installing a leader's snapshot and resetting the log
    /// (or partway through the reset), or a damaged tail the snapshot covers, dropped. Then
    /// `entries` carry its doubt. Returns whether it was reset.
    ///
    /// # Errors
    ///
    /// If resetting it fails.
    pub fn resume(&mut self, snapshot: Option<(u64, u64)>, entries: &mut Entries) -> io::Result<bool> {
        let reset = snapshot
            .filter(|&(index, term)| self.next_index <= index || entries.term_at(index).is_some_and(|t| t != term))
            .map(|(index, _)| index);
        if let Some(index) = reset {
            self.reset(index + 1)?;
            *entries = Entries::default();
        }
        if self.doubt {
            entries.doubt();
        }
        Ok(reset.is_some())
    }

    /// A handle on the directory's lock, which keeps it locked while it's open, though the log is
    /// dropped: for work on the node's files that may outlive it, a snapshot still being saved by
    /// a node whose task was stopped, so that a node started again in the same process waits for
    /// it rather than writing beside it.
    ///
    /// # Errors
    ///
    /// If the lock's file can't be opened again.
    pub(crate) fn lock(&self) -> io::Result<File> {
        self.lock.try_clone()
    }

    /// The index the next record must have.
    #[must_use]
    pub fn next_index(&self) -> u64 {
        self.next_index
    }

    /// Writes `records`, whole records continuing from [`Log::next_index`], without syncing them.
    ///
    /// # Errors
    ///
    /// If writing fails. The log should then be closed and reopened, which drops a partial write.
    pub fn append(&mut self, records: &[u8]) -> io::Result<()> {
        // Written a segment's run at a time, not a record: one syscall for a batch.
        let (mut at, mut run, mut count) = (0, 0, 0);
        while at < records.len() {
            let (record, len) = Record::decode(&records[at..])
                .ok()
                .flatten()
                .expect("append is given whole, valid records by the node");
            assert_eq!(record.index, self.next_index + count, "records must continue the log");
            if self.file_len + crate::widen(at - run) >= self.segment_bytes {
                self.write_run(&records[run..at], count)?;
                self.roll(record.index)?;
                (run, count) = (at, 0);
            }
            count += 1;
            at += len;
        }
        self.write_run(&records[run..], count)
    }

    /// Writes `count` whole records, `bytes`, at the end of the current segment.
    fn write_run(&mut self, bytes: &[u8], count: u64) -> io::Result<()> {
        self.file.write_all(bytes)?;
        self.file_len += crate::widen(bytes.len());
        self.next_index += count;
        self.unflushed += crate::widen(bytes.len());
        if self.unflushed >= FLUSH_BEHIND_BYTES
            && let Some(flush) = &self.flush_behind
        {
            self.unflushed = 0;
            // Still busy with the last, it's catching up anyway: this one's skipped.
            if let Ok(file) = self.file.try_clone() {
                let _ = flush.try_send(file);
            }
        }
        Ok(())
    }

    /// Reads whole records from `from` on: at least one, then up to `max_bytes`, or to the end
    /// of `from`'s segment.
    ///
    /// # Errors
    ///
    /// If reading fails.
    ///
    /// # Panics
    ///
    /// If the log doesn't hold `from`.
    pub fn read(&mut self, from: u64, max_bytes: usize) -> io::Result<Vec<u8>> {
        assert!(from >= self.segments[0] && from < self.next_index, "the log holds {from}");
        let at = self.segments.partition_point(|&first| first <= from) - 1;
        let segment = self.segments[at];
        // Where its records end: the next segment's first, or the log's end.
        let end = self.segments.get(at + 1).copied().unwrap_or(self.next_index);
        // A follower catching up reads on from where the last read stopped.
        let (mut index, mut offset) = match self.cursor {
            Some((s, index, offset)) if s == segment && index <= from => (index, offset),
            _ => (segment, 0),
        };
        let mut file = BufReader::new(File::open(segment_path(&self.dir, segment))?);
        file.seek(SeekFrom::Start(offset))?;
        let mut out = Vec::new();
        while index < self.next_index {
            let mut len = [0; 4];
            match file.read_exact(&mut len) {
                // The file ends before its records do: a damaged length skipped past them.
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof && index < end => {
                    return Err(invalid(format!("damaged record before {index} in segment {segment}")));
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                result => result?,
            }
            let len = 4 + usize::try_from(u32::from_le_bytes(len)).expect("u32 fits usize");
            // A damaged length isn't trusted with an allocation.
            if len > MAX_RECORD_BYTES {
                return Err(invalid(format!("damaged record {index} in segment {segment}")));
            }
            if index < from {
                file.seek_relative(i64::try_from(len - 4).expect("a record is small"))?;
            } else {
                if !out.is_empty() && out.len() + len > max_bytes {
                    break;
                }
                let start = out.len();
                out.resize(start + len, 0);
                out[start..start + 4].copy_from_slice(&u32::try_from(len - 4).expect("read as u32").to_le_bytes());
                file.read_exact(&mut out[start + 4..])?;
                let whole =
                    Record::decode(&out[start..]).ok().flatten().is_some_and(|(r, l)| r.index == index && l == len);
                if !whole {
                    return Err(invalid(format!("damaged record {index} in segment {segment}")));
                }
            }
            index += 1;
            offset += crate::widen(len);
        }
        self.cursor = Some((segment, index, offset));
        Ok(out)
    }

    /// Makes everything appended so far durable: a no-op with [`Fsync::Never`].
    ///
    /// # Errors
    ///
    /// If syncing fails; what was appended since the last sync may then be lost.
    pub fn sync(&mut self) -> io::Result<()> {
        if !self.sync {
            return Ok(());
        }
        self.file.sync_data()?;
        if self.dir_dirty {
            sync_dir(&self.dir)?;
            self.dir_dirty = false;
        }
        Ok(())
    }

    /// Removes the records from `index` on, which a new leader's log has replaced, and syncs.
    ///
    /// # Errors
    ///
    /// If the files can't be changed.
    pub fn truncate_from(&mut self, index: u64) -> io::Result<()> {
        assert!(index >= self.segments[0], "can't truncate into the compacted prefix");
        self.cursor = None;
        if index >= self.next_index {
            return Ok(());
        }
        while self.segments.len() > 1 && *self.segments.last().expect("not empty") >= index {
            let first = self.segments.pop().expect("not empty");
            fs::remove_file(segment_path(&self.dir, first))?;
            self.dir_dirty = true;
        }
        let first = *self.segments.last().expect("never empty");
        let mut bytes = Vec::new();
        File::open(segment_path(&self.dir, first))?.read_to_end(&mut bytes)?;
        let mut keep = 0;
        let mut at_index = first;
        while at_index < index {
            let Some((_, len)) = Record::decode(&bytes[keep..]).ok().flatten() else {
                return Err(invalid(format!("damaged record {at_index} in segment {first}")));
            };
            keep += len;
            at_index += 1;
        }
        self.file = OpenOptions::new().write(true).open(segment_path(&self.dir, first))?;
        self.file.set_len(crate::widen(keep))?;
        self.file.seek(SeekFrom::End(0))?;
        self.file_len = crate::widen(keep);
        self.next_index = index;
        if self.sync {
            self.file.sync_all()?;
        }
        self.sync()
    }

    /// Deletes the segments holding only records up to `index`, which a snapshot now covers.
    /// Returns the first index the log still holds.
    ///
    /// # Errors
    ///
    /// If a segment can't be removed.
    pub fn compact_to(&mut self, index: u64) -> io::Result<u64> {
        self.cursor = None;
        while self.segments.len() > 1 && self.segments[1] <= index + 1 {
            fs::remove_file(segment_path(&self.dir, self.segments[0]))?;
            self.segments.remove(0);
            self.dir_dirty = true;
        }
        Ok(self.segments[0])
    }

    /// Puts the segments holding `from` to `to` in `into`: hard links to the closed ones, which
    /// are never written again, and a copy of the one being appended to. Records outside that
    /// range come too; what reads them skips them.
    ///
    /// # Errors
    ///
    /// If the log no longer holds `from`, or doesn't hold `to` yet, or linking or copying fails.
    pub fn capture(&self, into: &Path, from: u64, to: u64) -> io::Result<()> {
        if to < from {
            return Ok(());
        }
        if from < self.segments[0] || to >= self.next_index {
            let held = format!("{}..{}", self.segments[0], self.next_index);
            return Err(io::Error::other(format!("the log holds {held}, not {from}..={to}")));
        }
        for (i, &first) in self.segments.iter().enumerate() {
            let next = self.segments.get(i + 1).copied();
            if next.is_some_and(|next| next <= from) || first > to {
                continue;
            }
            let (segment, linked) = (segment_path(&self.dir, first), segment_path(into, first));
            match next {
                Some(_) => fs::hard_link(segment, linked)?,
                None => drop(fs::copy(segment, linked)?),
            }
        }
        Ok(())
    }

    /// Discards the whole log, which a snapshot from the leader replaces: the next record is
    /// `next_index`.
    ///
    /// # Errors
    ///
    /// If the files can't be changed.
    pub fn reset(&mut self, next_index: u64) -> io::Result<()> {
        self.cursor = None;
        // Newest first, and all gone before the new one: a crash partway leaves the start of the
        // old log, which [`Log::resume`] resets, never a gap.
        for first in std::mem::take(&mut self.segments).into_iter().rev() {
            fs::remove_file(segment_path(&self.dir, first))?;
        }
        if self.sync {
            sync_dir(&self.dir)?;
        }
        self.file =
            OpenOptions::new().create(true).truncate(true).write(true).open(segment_path(&self.dir, next_index))?;
        if self.sync {
            self.file.sync_all()?;
        }
        self.segments = vec![next_index];
        self.file_len = 0;
        self.next_index = next_index;
        self.dir_dirty = true;
        self.sync()
    }

    fn roll(&mut self, first: u64) -> io::Result<()> {
        if self.sync {
            self.file.sync_data()?;
        }
        self.file = OpenOptions::new().create_new(true).write(true).open(segment_path(&self.dir, first))?;
        self.segments.push(first);
        self.file_len = 0;
        self.dir_dirty = true;
        Ok(())
    }
}

/// Doubts the log in `dir`, durably, then removes its `segments` from the `kept`th on: newest
/// first, so a crash partway leaves a prefix of the log, which opens the same.
fn cut_segments(dir: &Path, segments: &mut Vec<u64>, kept: usize, sync: bool) -> io::Result<()> {
    if !dir.join("doubt").exists() {
        File::create(dir.join("doubt"))?;
    }
    if sync {
        sync_dir(dir)?;
    }
    for &first in segments[kept..].iter().rev() {
        fs::remove_file(segment_path(dir, first))?;
    }
    segments.truncate(kept);
    if sync {
        sync_dir(dir)?;
    }
    Ok(())
}

/// How many bytes of `bytes`, from the start, are whole, valid records continuing from
/// `next_index`: fewer than all of them if damage stops them.
fn scan(bytes: &[u8], next_index: &mut u64) -> usize {
    let mut at = 0;
    while at < bytes.len() {
        match Record::decode(&bytes[at..]) {
            Ok(Some((record, len))) if record.index == *next_index => {
                at += len;
                *next_index += 1;
            }
            Ok(_) | Err(CodecError::Checksum | CodecError::Short | CodecError::TooLong { .. }) => break,
            Err(_) => unreachable!("Record::decode returns only record errors"),
        }
    }
    at
}

fn list_segments(dir: &Path) -> io::Result<Vec<u64>> {
    let mut segments = Vec::new();
    for entry in fs::read_dir(dir)? {
        let name = entry?.file_name();
        let Some(first) = name.to_str().and_then(|n| n.strip_suffix(".log")) else { continue };
        segments.push(first.parse().map_err(|_| invalid(format!("not a segment: {}", name.display())))?);
    }
    segments.sort_unstable();
    Ok(segments)
}

fn segment_path(dir: &Path, first: u64) -> PathBuf {
    dir.join(format!("{first:020}.log"))
}

/// A thread that syncs the data of each file it's sent, until the log is dropped. A failed sync is
/// no worse than none: nothing waits on it.
fn flusher() -> io::Result<mpsc::SyncSender<File>> {
    let (tx, rx) = mpsc::sync_channel::<File>(1);
    std::thread::Builder::new().name("turbofan-log-flush".into()).spawn(move || {
        for file in rx {
            let _ = file.sync_data();
        }
    })?;
    Ok(tx)
}

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// Creates `dir`, and its parents that are missing, each synced into its parent: else a crash
/// could lose the directory a synced file is in.
///
/// # Errors
///
/// If creating or syncing one fails.
pub(crate) fn create_dir_durably(dir: &Path) -> io::Result<()> {
    let missing: Vec<&Path> = dir.ancestors().take_while(|d| !d.as_os_str().is_empty() && !d.exists()).collect();
    fs::create_dir_all(dir)?;
    for created in missing.iter().rev() {
        let parent = created.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new("."));
        sync_dir(parent)?;
    }
    Ok(())
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}

/// Encodes records `first..first + n` of term 1, for tests.
#[cfg(test)]
pub(crate) fn test_records(first: u64, n: u64) -> Vec<u8> {
    let mut out = Vec::new();
    for index in first..first + n {
        Record { term: 1, index, payload: format!("entry {index}").as_bytes() }.encode_into(&mut out);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::{self, RECORD_HEADER};

    fn indices(bytes: &[u8]) -> Vec<u64> {
        codec::records(bytes).map(|r| r.unwrap().index).collect()
    }

    /// Opens the log, with the indices of the records it holds.
    fn open(dir: &Path) -> (Log, Vec<u64>) {
        let mut found = Vec::new();
        let log = Log::open(dir, Fsync::EachBatch, |r| found.push(r.index)).unwrap();
        (log, found)
    }

    /// Unsynced, a log handed to the flushing thread several times over reads back whole.
    #[test]
    fn a_log_flushed_behind_reads_back_whole() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), Fsync::Never, |_| {}).unwrap();
        assert!(log.flush_behind.is_some());
        let payload = vec![7; 64 << 10];
        let mut records = Vec::new();
        for index in 1..=300 {
            records.clear();
            Record { term: 1, index, payload: &payload }.encode_into(&mut records);
            log.append(&records).unwrap();
        }
        drop(log);
        let mut found = Vec::new();
        Log::open(dir.path(), Fsync::Never, |r| found.push(r.index)).unwrap();
        assert_eq!(found, (1..=300).collect::<Vec<_>>());
    }

    #[test]
    fn records_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, found) = open(dir.path());
        assert!(found.is_empty());
        log.append(&test_records(1, 3)).unwrap();
        log.sync().unwrap();
        drop(log);
        let (log, found) = open(dir.path());
        assert_eq!((found, log.next_index()), (vec![1, 2, 3], 4));
    }

    #[test]
    fn a_reset_log_continues_from_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.append(&test_records(1, 3)).unwrap();
        log.reset(10).unwrap();
        log.append(&test_records(10, 2)).unwrap();
        log.sync().unwrap();
        drop(log);
        let (log, found) = open(dir.path());
        assert_eq!((found, log.next_index()), (vec![10, 11], 12));
    }

    /// Opens the log as the driver does, resumed from the snapshot `(index, term)`: whether it
    /// was reset, and its next index.
    fn resume(dir: &Path, snapshot: (u64, u64)) -> (bool, u64) {
        let mut entries = Entries::default();
        let mut log = Log::open(dir, Fsync::EachBatch, |r| entries.recover(r, usize::MAX)).unwrap();
        (log.resume(Some(snapshot), &mut entries).unwrap(), log.next_index())
    }

    /// Records 1 to 6 of term 1, in segments at 1, 3 and 5.
    fn three_segments(dir: &Path) {
        let (mut log, _) = open(dir);
        log.append(&test_records(1, 2)).unwrap();
        log.file_len = MAX_SEGMENT_BYTES;
        log.append(&test_records(3, 2)).unwrap();
        log.file_len = MAX_SEGMENT_BYTES;
        log.append(&test_records(5, 2)).unwrap();
        log.sync().unwrap();
        assert_eq!(log.segments, [1, 3, 5]);
    }

    /// A crash after a leader's snapshot was installed, before the log it replaces was reset:
    /// the log holds another term at the snapshot's index, and goes on from the snapshot.
    #[test]
    fn a_log_disagreeing_with_the_snapshot_is_reset() {
        let dir = tempfile::tempdir().unwrap();
        three_segments(dir.path());
        assert_eq!(resume(dir.path(), (4, 1)), (false, 7), "the same term: kept");
        assert_eq!(resume(dir.path(), (4, 2)), (true, 5));
        assert_eq!(open(dir.path()).1, Vec::<u64>::new());
    }

    /// Missing directories are made, parents and all.
    #[test]
    fn directories_are_created_parents_and_all() {
        let dir = tempfile::tempdir().unwrap();
        let deep = dir.path().join("a/b/c");
        create_dir_durably(&deep).unwrap();
        create_dir_durably(&deep).unwrap();
        assert!(deep.is_dir());
    }

    /// A crash partway through a reset, which removes the newest segments first: what's left
    /// opens, and goes on from the snapshot.
    #[test]
    fn a_reset_cut_short_resumes_from_the_snapshot() {
        for gone in 1..=3 {
            let dir = tempfile::tempdir().unwrap();
            three_segments(dir.path());
            for first in [5, 3, 1].into_iter().take(gone) {
                fs::remove_file(segment_path(dir.path(), first)).unwrap();
            }
            assert_eq!(resume(dir.path(), (4, 2)), (true, 5), "{gone} gone");
        }
    }

    #[test]
    fn a_second_process_is_locked_out() {
        let dir = tempfile::tempdir().unwrap();
        let _log = open(dir.path());
        assert_eq!(Log::open(dir.path(), Fsync::EachBatch, |_| {}).unwrap_err().kind(), io::ErrorKind::WouldBlock);
    }

    /// A handle on the lock keeps the directory locked after the log is dropped, until it's
    /// dropped too.
    #[test]
    fn a_handle_on_the_lock_outlives_the_log() {
        let dir = tempfile::tempdir().unwrap();
        let (log, _) = open(dir.path());
        let lock = log.lock().unwrap();
        drop(log);
        assert_eq!(Log::open(dir.path(), Fsync::EachBatch, |_| {}).unwrap_err().kind(), io::ErrorKind::WouldBlock);
        drop(lock);
        assert!(Log::open(dir.path(), Fsync::EachBatch, |_| {}).is_ok(), "free once it's dropped");
    }

    /// Power lost mid-write leaves any prefix of the last record: it's dropped, and the log goes
    /// on from there.
    #[test]
    fn a_torn_tail_is_truncated_at_every_cut() {
        let records = test_records(1, 3);
        let whole_two = codec::records(&records).take(2).map(|r| r.unwrap().payload.len() + codec::RECORD_HEADER).sum();
        for cut in whole_two..records.len() {
            let dir = tempfile::tempdir().unwrap();
            fs::write(segment_path(dir.path(), 1), &records[..cut]).unwrap();
            let (mut log, found) = open(dir.path());
            assert_eq!(found, [1, 2], "cut at {cut}");
            // Only what was truncated may have been acknowledged.
            assert_eq!(log.in_doubt(), cut > whole_two, "cut at {cut}");
            log.append(&test_records(3, 1)).unwrap();
            drop(log);
            // The tail's gone, and the doubt with it is still there.
            let (mut log, found) = open(dir.path());
            assert_eq!((found, log.in_doubt()), (vec![1, 2, 3], cut > whole_two), "cut at {cut}");
            log.trust().unwrap();
            drop(log);
            assert!(!open(dir.path()).0.in_doubt(), "cut at {cut}");
        }
    }

    /// An operator who deleted the `doubt` file while the node ran: trusting the log is still fine.
    #[test]
    fn a_doubt_file_already_gone_is_trusted_all_the_same() {
        let records = test_records(1, 2);
        let dir = tempfile::tempdir().unwrap();
        fs::write(segment_path(dir.path(), 1), &records[..records.len() - 1]).unwrap();
        let (mut log, _) = open(dir.path());
        assert!(log.in_doubt());
        fs::remove_file(dir.path().join("doubt")).unwrap();
        log.trust().unwrap();
        assert!(!log.in_doubt());
    }

    /// A power cut mid-batch that kept a later page and lost an earlier: a damaged record with
    /// whole ones after it. The log is truncated at the damage, in doubt, and goes on from there.
    #[test]
    fn damage_with_records_after_it_is_truncated() {
        let dir = tempfile::tempdir().unwrap();
        let mut records = test_records(1, 3);
        records[RECORD_HEADER + 2] ^= 1;
        fs::write(segment_path(dir.path(), 1), records).unwrap();
        let (mut log, found) = open(dir.path());
        assert_eq!((found, log.in_doubt()), (vec![], true));
        log.append(&test_records(1, 1)).unwrap();
        drop(log);
        assert_eq!(open(dir.path()).1, [1], "the damage and what followed it gone");
    }

    /// What's read back for a follower is checked: damage is an error, not served.
    #[test]
    fn a_damaged_record_is_not_read_back() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.append(&test_records(1, 3)).unwrap();
        log.sync().unwrap();
        assert_eq!(indices(&log.read(1, usize::MAX).unwrap()), [1, 2, 3]);
        let path = segment_path(dir.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        let second = codec::records(&bytes).next().map(|r| RECORD_HEADER + r.unwrap().payload.len()).unwrap();
        bytes[second + RECORD_HEADER] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert_eq!(log.read(1, usize::MAX).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    /// A damaged length is an error, not a read of that many bytes.
    #[test]
    fn a_damaged_length_is_not_read() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.append(&test_records(1, 2)).unwrap();
        let path = segment_path(dir.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[..4].copy_from_slice(&u32::MAX.to_le_bytes());
        fs::write(&path, bytes).unwrap();
        assert_eq!(log.read(1, usize::MAX).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    /// A damaged length that skips the rest of the file is an error, not an empty read: the
    /// records after it are there, damaged, not missing.
    #[test]
    fn a_damaged_length_skipped_over_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.append(&test_records(1, 3)).unwrap();
        let path = segment_path(dir.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        let to_end = u32::try_from(bytes.len() - 4).unwrap();
        bytes[..4].copy_from_slice(&to_end.to_le_bytes());
        fs::write(&path, bytes).unwrap();
        assert_eq!(log.read(3, usize::MAX).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    /// Truncating reads the records it keeps: damage there is an error, not a panic.
    #[test]
    fn truncating_past_damage_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.append(&test_records(1, 3)).unwrap();
        let path = segment_path(dir.path(), 1);
        let mut bytes = fs::read(&path).unwrap();
        bytes[RECORD_HEADER] ^= 1;
        fs::write(&path, bytes).unwrap();
        assert_eq!(log.truncate_from(3).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    /// Damage before the last segment, or a gap between segments (unsynced, a segment's tail lost
    /// and the next kept): truncated there, the later segments gone, in doubt.
    #[test]
    fn damage_or_a_gap_before_the_last_segment_drops_the_rest() {
        let damaged = || {
            let mut first = test_records(1, 2);
            let second = codec::records(&first).next().unwrap().unwrap().payload.len() + RECORD_HEADER;
            first[second + 10] ^= 1;
            first
        };
        let lost_tail = || {
            let first = test_records(1, 2);
            first[..codec::records(&first).next().unwrap().unwrap().payload.len() + RECORD_HEADER].to_vec()
        };
        for first in [damaged(), lost_tail()] {
            let dir = tempfile::tempdir().unwrap();
            fs::write(segment_path(dir.path(), 1), first).unwrap();
            fs::write(segment_path(dir.path(), 3), test_records(3, 1)).unwrap();
            fs::write(segment_path(dir.path(), 4), test_records(4, 1)).unwrap();
            let (log, found) = open(dir.path());
            assert_eq!((found, log.in_doubt(), log.next_index()), (vec![1], true, 2));
            assert_eq!(list_segments(dir.path()).unwrap(), [1], "the later segments gone");
        }
    }

    #[test]
    fn truncating_across_segments_and_compacting() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.append(&test_records(1, 2)).unwrap();
        // Force segments at 3 and 5.
        log.file_len = MAX_SEGMENT_BYTES;
        log.append(&test_records(3, 2)).unwrap();
        log.file_len = MAX_SEGMENT_BYTES;
        log.append(&test_records(5, 2)).unwrap();
        assert_eq!(log.segments, [1, 3, 5]);

        log.truncate_from(4).unwrap();
        assert_eq!((log.segments.clone(), log.next_index()), (vec![1, 3], 4));
        log.append(&test_records(4, 1)).unwrap();
        assert_eq!(log.compact_to(2).unwrap(), 3);
        log.sync().unwrap();
        // Reads stop at a segment's end, and at `max_bytes` after the first record.
        assert_eq!(indices(&log.read(3, usize::MAX).unwrap()), [3, 4]);
        assert_eq!(indices(&log.read(3, 0).unwrap()), [3]);
        assert_eq!(indices(&log.read(4, 0).unwrap()), [4], "on from where the last stopped");
        assert_eq!(log.read(4, 0).unwrap(), test_records(4, 1));
        drop(log);
        let (log, found) = open(dir.path());
        assert_eq!((found, log.next_index()), (vec![3, 4], 5));
    }

    /// A capture takes the segments holding its range: the closed ones linked, so they're the
    /// same files, the last copied, so the log appending to it later doesn't change it.
    #[test]
    fn a_capture_takes_the_segments_holding_its_range() {
        use std::os::unix::fs::MetadataExt;
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = open(dir.path());
        log.append(&test_records(1, 2)).unwrap();
        for first in [3, 5] {
            log.file_len = MAX_SEGMENT_BYTES;
            log.append(&test_records(first, 2)).unwrap();
        }
        assert_eq!(log.segments, [1, 3, 5]);
        let into = tempfile::tempdir().unwrap();
        log.capture(into.path(), 4, 5).unwrap();
        let taken = |name: u64| into.path().join(format!("{name:020}.log"));
        assert!(!taken(1).exists(), "before the range");
        assert_eq!(fs::metadata(taken(3)).unwrap().nlink(), 2, "closed: linked");
        assert_eq!(fs::metadata(taken(5)).unwrap().nlink(), 1, "appended to: copied");
        log.append(&test_records(7, 1)).unwrap();
        let (_, found) = open(into.path());
        assert_eq!(found, [3, 4, 5, 6], "as the log was");

        assert!(log.capture(into.path(), 4, 8).is_err(), "not yet held");
        log.compact_to(4).unwrap();
        assert!(log.capture(into.path(), 2, 5).is_err(), "no longer held");
        let one = tempfile::tempdir().unwrap();
        log.capture(one.path(), 6, 6).unwrap();
        assert_eq!(fs::read_dir(one.path()).unwrap().count(), 1, "one entry, its segment");
        let none = tempfile::tempdir().unwrap();
        log.capture(none.path(), 8, 7).unwrap();
        assert_eq!(fs::read_dir(none.path()).unwrap().count(), 0, "an empty range takes nothing");
    }
}
