//! The log on disk: [`Record`]s appended to segment files named by their first index.
//!
//! Appends are written straight away and made durable by [`Log::sync`], which the driver calls
//! once per batch (group commit). On open, the last segment's tail is scanned and a torn record
//! (power lost mid-write) truncated; damage anywhere else is an error, since those records were
//! synced and may be committed.

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::codec::{CodecError, Record};

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
    /// The index the next record must have.
    next_index: u64,
    /// A segment was created or removed since the directory was last synced.
    dir_dirty: bool,
    _lock: File,
}

impl Log {
    /// Opens or creates the log in `dir`, returning it and every record it holds, concatenated.
    ///
    /// # Errors
    ///
    /// If the directory is locked by another process, can't be read, or holds damaged records
    /// before the last segment's tail.
    pub fn open(dir: &Path) -> io::Result<(Self, Vec<u8>)> {
        fs::create_dir_all(dir)?;
        let lock = OpenOptions::new().create(true).truncate(false).write(true).open(dir.join("lock"))?;
        lock.try_lock().map_err(|_| io::Error::new(io::ErrorKind::WouldBlock, "log directory is in use"))?;
        let mut segments = list_segments(dir)?;
        if segments.is_empty() {
            segments.push(1);
            File::create(segment_path(dir, 1))?;
            sync_dir(dir)?;
        }
        let mut bytes = Vec::new();
        let mut next_index = segments[0];
        let last = segments.len() - 1;
        let mut file_len = 0;
        for (i, &first) in segments.iter().enumerate() {
            if first != next_index {
                return Err(invalid(format!("segment {first} doesn't follow index {}", next_index - 1)));
            }
            let start = bytes.len();
            File::open(segment_path(dir, first))?.read_to_end(&mut bytes)?;
            let valid = scan(&bytes[start..], &mut next_index);
            let valid = match valid {
                Ok(len) => len,
                Err(len) if i == last => len,
                Err(_) => return Err(invalid(format!("damaged record in segment {first}"))),
            };
            bytes.truncate(start + valid);
            file_len = valid as u64;
        }
        let file = OpenOptions::new().write(true).open(segment_path(dir, segments[last]))?;
        // Drops a torn tail; a no-op otherwise.
        file.set_len(file_len)?;
        file.sync_all()?;
        let mut log = Self { dir: dir.into(), segments, file, file_len, next_index, dir_dirty: false, _lock: lock };
        log.file.seek(SeekFrom::End(0))?;
        Ok((log, bytes))
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
        let mut at = 0;
        while at < records.len() {
            let (record, len) = Record::decode(&records[at..])
                .ok()
                .flatten()
                .expect("append is given whole, valid records by the node");
            assert_eq!(record.index, self.next_index, "records must continue the log");
            if self.file_len >= MAX_SEGMENT_BYTES {
                self.roll(record.index)?;
            }
            self.file.write_all(&records[at..at + len])?;
            self.file_len += len as u64;
            self.next_index += 1;
            at += len;
        }
        Ok(())
    }

    /// Makes everything appended so far durable.
    ///
    /// # Errors
    ///
    /// If syncing fails; what was appended since the last sync may then be lost.
    pub fn sync(&mut self) -> io::Result<()> {
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
            let (_, len) = Record::decode(&bytes[keep..]).ok().flatten().expect("synced records are whole");
            keep += len;
            at_index += 1;
        }
        self.file = OpenOptions::new().write(true).open(segment_path(&self.dir, first))?;
        self.file.set_len(keep as u64)?;
        self.file.seek(SeekFrom::End(0))?;
        self.file_len = keep as u64;
        self.next_index = index;
        self.file.sync_all()?;
        self.sync()
    }

    /// Deletes the segments holding only records up to `index`, which a snapshot now covers.
    /// Returns the first index the log still holds.
    ///
    /// # Errors
    ///
    /// If a segment can't be removed.
    pub fn compact_to(&mut self, index: u64) -> io::Result<u64> {
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
        self.file =
            OpenOptions::new().create(true).truncate(true).write(true).open(segment_path(&self.dir, next_index))?;
        self.file.sync_all()?;
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
        self.file.sync_data()?;
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
    use crate::codec;

    fn indices(bytes: &[u8]) -> Vec<u64> {
        codec::records(bytes).map(|r| r.unwrap().index).collect()
    }

    #[test]
    fn records_survive_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, bytes) = Log::open(dir.path()).unwrap();
        assert!(bytes.is_empty());
        log.append(&test_records(1, 3)).unwrap();
        log.sync().unwrap();
        drop(log);
        let (log, bytes) = Log::open(dir.path()).unwrap();
        assert_eq!((indices(&bytes), log.next_index()), (vec![1, 2, 3], 4));
    }

    #[test]
    fn a_reset_log_continues_from_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = Log::open(dir.path()).unwrap();
        log.append(&test_records(1, 3)).unwrap();
        log.reset(10).unwrap();
        log.append(&test_records(10, 2)).unwrap();
        log.sync().unwrap();
        drop(log);
        let (log, bytes) = Log::open(dir.path()).unwrap();
        assert_eq!((indices(&bytes), log.next_index()), (vec![10, 11], 12));
    }

    #[test]
    fn a_second_process_is_locked_out() {
        let dir = tempfile::tempdir().unwrap();
        let _log = Log::open(dir.path()).unwrap();
        assert_eq!(Log::open(dir.path()).unwrap_err().kind(), io::ErrorKind::WouldBlock);
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
            let (mut log, bytes) = Log::open(dir.path()).unwrap();
            assert_eq!(indices(&bytes), [1, 2], "cut at {cut}");
            log.append(&test_records(3, 1)).unwrap();
            drop(log);
            assert_eq!(indices(&Log::open(dir.path()).unwrap().1), [1, 2, 3], "cut at {cut}");
        }
    }

    #[test]
    fn damage_before_the_last_segment_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let mut first = test_records(1, 2);
        first[10] ^= 1;
        fs::write(segment_path(dir.path(), 1), first).unwrap();
        fs::write(segment_path(dir.path(), 3), test_records(3, 1)).unwrap();
        assert_eq!(Log::open(dir.path()).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }

    #[test]
    fn truncating_across_segments_and_compacting() {
        let dir = tempfile::tempdir().unwrap();
        let (mut log, _) = Log::open(dir.path()).unwrap();
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
        drop(log);
        let (log, bytes) = Log::open(dir.path()).unwrap();
        assert_eq!((indices(&bytes), log.next_index()), (vec![3, 4], 5));
    }
}
