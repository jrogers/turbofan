//! The log on disk: [`Record`]s appended to segment files named by their first index.
//!
//! Appends are written straight away and made durable by [`Log::sync`], which the driver calls
//! once per batch (group commit). On open, the last segment's tail is scanned and a torn record
//! (power lost mid-write) truncated: damage with nothing whole after it. Damage anywhere else is an
//! error, since those records were synced and may be committed and acknowledged: a node with it
//! refuses to start, to be replaced, rather than lose them.
//!
//! A torn tail and bit rot in the last record look the same, so a truncated tail may have been
//! synced and acknowledged. The log is then [in doubt](Log::in_doubt), durably, until Raft has
//! caught it up ([`Log::trust`]): a `doubt` file, written before the tail is truncated.
//!
//! [`Log::read`] reads records back, for a follower that's behind what the node keeps in memory,
//! and checks them: damage is an error there too, never served.

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::codec::{CodecError, MAX_RECORD_BYTES, Record};
use crate::raft::Entries;

/// A segment is closed and a new one started once it holds this much.
pub const MAX_SEGMENT_BYTES: u64 = 64 << 20;

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
    /// Syncs are made: unless simulated.
    sync: bool,
    /// A damaged tail was truncated, and the log hasn't been trusted since.
    doubt: bool,
    /// The index the next record must have.
    next_index: u64,
    /// A segment was created or removed since the directory was last synced.
    dir_dirty: bool,
    /// Where the last read stopped: its segment, the next index and its offset there.
    cursor: Option<(u64, u64, u64)>,
    _lock: File,
}

impl Log {
    /// Opens or creates the log in `dir`, passing `each` every record it holds, in order. Only
    /// one segment is in memory at a time.
    ///
    /// # Errors
    ///
    /// If the directory is locked by another process, can't be read, or holds damaged records
    /// before the last segment's tail.
    pub fn open(dir: &Path, each: impl FnMut(Record<'_>)) -> io::Result<Self> {
        Self::open_with(dir, MAX_SEGMENT_BYTES, true, each)
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
        fs::create_dir_all(dir)?;
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
        let last = segments.len() - 1;
        let mut file_len = 0;
        let mut doubt = dir.join("doubt").exists();
        for (i, &first) in segments.iter().enumerate() {
            if first != next_index {
                return Err(invalid(format!("segment {first} doesn't follow index {}", next_index - 1)));
            }
            bytes.clear();
            File::open(segment_path(dir, first))?.read_to_end(&mut bytes)?;
            let valid = match scan(&bytes, &mut next_index) {
                Ok(len) => len,
                // A torn write leaves nothing whole after it; synced records damaged do.
                Err(len) if i == last && !whole_record_in(&bytes[len..]) => len,
                Err(_) => return Err(invalid(format!("damaged record in segment {first}"))),
            };
            for record in crate::codec::records(&bytes[..valid]) {
                each(record.expect("scanned"));
            }
            file_len = crate::widen(valid);
            if valid < bytes.len() && !doubt {
                // Durably, before the tail goes.
                File::create(dir.join("doubt"))?;
                if sync {
                    sync_dir(dir)?;
                }
                doubt = true;
            }
        }
        let file = OpenOptions::new().write(true).open(segment_path(dir, segments[last]))?;
        // Drops a torn tail; a no-op otherwise.
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
            _lock: lock,
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
    /// snapshot of `snapshot_index` (a crash between installing it and resetting the log, or a
    /// damaged tail it covers, dropped) goes on from the snapshot, without its records. Then
    /// `entries` carry its doubt. Returns whether it was reset.
    ///
    /// # Errors
    ///
    /// If resetting it fails.
    pub fn resume(&mut self, snapshot_index: Option<u64>, entries: &mut Entries) -> io::Result<bool> {
        let reset = snapshot_index.filter(|&index| self.next_index <= index);
        if let Some(index) = reset {
            self.reset(index + 1)?;
            *entries = Entries::default();
        }
        if self.doubt {
            entries.doubt();
        }
        Ok(reset.is_some())
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
        let segment = self.segments[self.segments.partition_point(|&first| first <= from) - 1];
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

    /// Makes everything appended so far durable.
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

    /// Discards the whole log, which a snapshot from the leader replaces: the next record is
    /// `next_index`.
    ///
    /// # Errors
    ///
    /// If the files can't be changed.
    pub fn reset(&mut self, next_index: u64) -> io::Result<()> {
        self.cursor = None;
        self.file =
            OpenOptions::new().create(true).truncate(true).write(true).open(segment_path(&self.dir, next_index))?;
        if self.sync {
            self.file.sync_all()?;
        }
        for first in std::mem::take(&mut self.segments) {
            if first != next_index {
                fs::remove_file(segment_path(&self.dir, first))?;
            }
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

/// How many bytes of `bytes` are whole, valid records continuing from `next_index`: `Err` if
/// they stop before the end.
fn scan(bytes: &[u8], next_index: &mut u64) -> Result<usize, usize> {
    let mut at = 0;
    while at < bytes.len() {
        match Record::decode(&bytes[at..]) {
            Ok(Some((record, len))) if record.index == *next_index => {
                at += len;
                *next_index += 1;
            }
            Ok(_) | Err(CodecError::Checksum | CodecError::Short | CodecError::TooLong { .. }) => return Err(at),
            Err(_) => unreachable!("Record::decode returns only record errors"),
        }
    }
    Ok(at)
}

/// Whether a whole record starts anywhere in `bytes`: after damage, the sign it's corruption of
/// what was synced, not a torn write.
fn whole_record_in(bytes: &[u8]) -> bool {
    (1..bytes.len()).any(|at| matches!(Record::decode(&bytes[at..]), Ok(Some(_))))
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

fn sync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
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
        let log = Log::open(dir, |r| found.push(r.index)).unwrap();
        (log, found)
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

    #[test]
    fn a_second_process_is_locked_out() {
        let dir = tempfile::tempdir().unwrap();
        let _log = open(dir.path());
        assert_eq!(Log::open(dir.path(), |_| {}).unwrap_err().kind(), io::ErrorKind::WouldBlock);
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

    /// Synced records damaged in the last segment, with whole ones after them, aren't taken for a
    /// torn write and truncated: the node refuses to start, and doesn't lose them.
    #[test]
    fn damage_with_records_after_it_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut records = test_records(1, 3);
        records[RECORD_HEADER + 2] ^= 1;
        fs::write(segment_path(dir.path(), 1), records).unwrap();
        assert_eq!(Log::open(dir.path(), |_| {}).unwrap_err().kind(), io::ErrorKind::InvalidData);
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

    #[test]
    fn damage_before_the_last_segment_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut first = test_records(1, 2);
        first[10] ^= 1;
        fs::write(segment_path(dir.path(), 1), first).unwrap();
        fs::write(segment_path(dir.path(), 3), test_records(3, 1)).unwrap();
        assert_eq!(Log::open(dir.path(), |_| {}).unwrap_err().kind(), io::ErrorKind::InvalidData);
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
}
