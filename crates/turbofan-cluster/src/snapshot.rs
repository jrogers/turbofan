//! Snapshot files: `<index>.snapshot` in the data directory, written to a temporary file,
//! synced, renamed into place and the directory synced, so a crash leaves the previous snapshot
//! or the new one, whole. Only the latest is kept.

use std::fs::{self, File};
use std::io::{self, Write};
use std::path::{Path, PathBuf};

use crate::codec::checksum;
use crate::raft::{MAX_SNAPSHOT_BYTES, Snapshot};

const MAGIC: &[u8; 8] = b"CLSNAP01";
/// magic · index · term · length · checksum.
const HEADER: usize = 8 + 8 * 4;

/// Makes `snapshot` durable and removes older ones.
///
/// # Errors
///
/// If writing, syncing or renaming fails. The previous snapshot is then still in place.
pub fn save(dir: &Path, snapshot: &Snapshot) -> io::Result<()> {
    let temp = dir.join("snapshot.tmp");
    let mut file = File::create(&temp)?;
    file.write_all(MAGIC)?;
    for v in [snapshot.index, snapshot.term, snapshot.bytes.len() as u64, checksum(&snapshot.bytes)] {
        file.write_all(&v.to_le_bytes())?;
    }
    file.write_all(&snapshot.bytes)?;
    file.sync_all()?;
    fs::rename(&temp, path(dir, snapshot.index))?;
    File::open(dir)?.sync_all()?;
    for index in list(dir)? {
        if index < snapshot.index {
            fs::remove_file(path(dir, index))?;
        }
    }
    Ok(())
}

/// The latest snapshot in `dir`, if there is one.
///
/// # Errors
///
/// If it can't be read or is damaged: it was synced, so that's the disk's fault, and the node
/// should be rebuilt from its peers.
pub fn load(dir: &Path) -> io::Result<Option<Snapshot>> {
    let Some(&index) = list(dir)?.last() else { return Ok(None) };
    let bytes = fs::read(path(dir, index))?;
    let invalid = |what: &str| io::Error::new(io::ErrorKind::InvalidData, format!("snapshot {index}: {what}"));
    if bytes.len() < HEADER || &bytes[..8] != MAGIC {
        return Err(invalid("not a snapshot"));
    }
    let word = |i: usize| u64::from_le_bytes(bytes[8 + i * 8..16 + i * 8].try_into().expect("eight bytes"));
    let (stored_index, term, len, sum) = (word(0), word(1), word(2), word(3));
    let body = &bytes[HEADER..];
    if stored_index != index || body.len() as u64 != len || body.len() > MAX_SNAPSHOT_BYTES {
        return Err(invalid("wrong length or index"));
    }
    if checksum(body) != sum {
        return Err(invalid("checksum mismatch"));
    }
    Ok(Some(Snapshot { index, term, bytes: body.to_vec() }))
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

    #[test]
    fn the_latest_snapshot_is_loaded_and_older_ones_removed() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(load(dir.path()).unwrap(), None);
        let first = Snapshot { index: 10, term: 2, bytes: b"ten".to_vec() };
        let second = Snapshot { index: 20, term: 3, bytes: b"twenty".to_vec() };
        save(dir.path(), &first).unwrap();
        save(dir.path(), &second).unwrap();
        assert_eq!(load(dir.path()).unwrap(), Some(second));
        assert_eq!(list(dir.path()).unwrap(), [20]);
    }

    #[test]
    fn a_damaged_snapshot_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        save(dir.path(), &Snapshot { index: 10, term: 2, bytes: b"ten".to_vec() }).unwrap();
        let file = path(dir.path(), 10);
        let mut bytes = fs::read(&file).unwrap();
        *bytes.last_mut().unwrap() ^= 1;
        fs::write(&file, bytes).unwrap();
        assert_eq!(load(dir.path()).unwrap_err().kind(), io::ErrorKind::InvalidData);
    }
}
