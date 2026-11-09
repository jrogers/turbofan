//! A node's Raft term and vote, which must be durable before it answers in a term.
//!
//! Two fixed slots, written alternately and synced, each with a generation and checksum, after
//! Turbojet's `.seqnums`: a write torn by power loss leaves the other slot whole.

use std::fs::{File, OpenOptions};
use std::io;
use std::os::unix::fs::FileExt;
use std::path::Path;

use crate::codec::checksum;
use crate::raft::NodeId;

/// generation · term · vote · checksum.
const SLOT: usize = 32;

/// A node's durable Raft state.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct Meta {
    /// The latest term the node has seen.
    pub term: u64,
    /// The node it voted for in `term`, if any.
    pub vote: Option<NodeId>,
}

/// The file holding a [`Meta`].
#[derive(Debug)]
pub struct MetaFile {
    file: File,
    generation: u64,
    /// Saves are synced: unless simulated.
    sync: bool,
}

impl MetaFile {
    /// Opens or creates `path`, returning it and the meta it holds (the default for a new file).
    ///
    /// # Errors
    ///
    /// If the file can't be opened or read.
    pub fn open(path: &Path) -> io::Result<(Self, Meta)> {
        Self::open_with(path, true)
    }

    /// [`MetaFile::open`], for a simulation, whose crashes are modelled: saves aren't synced.
    ///
    /// # Errors
    ///
    /// As [`MetaFile::open`].
    pub fn open_simulated(path: &Path) -> io::Result<(Self, Meta)> {
        Self::open_with(path, false)
    }

    fn open_with(path: &Path, sync: bool) -> io::Result<(Self, Meta)> {
        let file = OpenOptions::new().read(true).write(true).create(true).truncate(false).open(path)?;
        if sync {
            // Else a new file's entry may be lost with power, and its synced vote with it.
            crate::log::sync_dir(path.parent().filter(|p| !p.as_os_str().is_empty()).unwrap_or(Path::new(".")))?;
        }
        let mut slots = [0; 2 * SLOT];
        let len = usize::try_from(file.metadata()?.len()).unwrap_or(usize::MAX).min(slots.len());
        file.read_exact_at(&mut slots[..len], 0)?;
        let best =
            [&slots[..SLOT], &slots[SLOT..]].into_iter().filter_map(parse).max_by_key(|&(generation, _)| generation);
        let (generation, meta) = best.unwrap_or_default();
        Ok((Self { file, generation, sync }, meta))
    }

    /// Makes `meta` durable.
    ///
    /// # Errors
    ///
    /// If writing or syncing fails; the previous meta is then still on disk.
    pub fn save(&mut self, meta: Meta) -> io::Result<()> {
        let generation = self.generation + 1;
        let mut slot = [0; SLOT];
        slot[..8].copy_from_slice(&generation.to_le_bytes());
        slot[8..16].copy_from_slice(&meta.term.to_le_bytes());
        slot[16..24].copy_from_slice(&meta.vote.map_or(0, |v| u64::from(v.0) + 1).to_le_bytes());
        let sum = checksum(&slot[..24]);
        slot[24..].copy_from_slice(&sum.to_le_bytes());
        let at = if generation.is_multiple_of(2) { 0 } else { crate::widen(SLOT) };
        self.file.write_all_at(&slot, at)?;
        if self.sync {
            self.file.sync_data()?;
        }
        self.generation = generation;
        Ok(())
    }
}

fn parse(slot: &[u8]) -> Option<(u64, Meta)> {
    let word = |i: usize| u64::from_le_bytes(slot[i * 8..i * 8 + 8].try_into().expect("eight bytes"));
    if word(0) == 0 || checksum(&slot[..24]) != word(3) {
        return None;
    }
    let vote = match word(2) {
        0 => None,
        v => Some(NodeId(u8::try_from(v - 1).ok()?)),
    };
    Some((word(0), Meta { term: word(1), vote }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_latest_save_survives_a_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta");
        let (mut file, meta) = MetaFile::open(&path).unwrap();
        assert_eq!(meta, Meta::default());
        file.save(Meta { term: 3, vote: Some(NodeId(2)) }).unwrap();
        file.save(Meta { term: 4, vote: None }).unwrap();
        drop(file);
        assert_eq!(MetaFile::open(&path).unwrap().1, Meta { term: 4, vote: None });
    }

    /// A save torn at any byte leaves the previous meta.
    #[test]
    fn a_torn_save_leaves_the_previous_meta() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("meta");
        let (mut file, _) = MetaFile::open(&path).unwrap();
        file.save(Meta { term: 3, vote: Some(NodeId(2)) }).unwrap();
        file.save(Meta { term: 4, vote: Some(NodeId(1)) }).unwrap();
        let whole = std::fs::read(&path).unwrap();
        // Generation 2 went to slot 0. Tear it: any of its bytes left as zero.
        for byte in 0..SLOT {
            let mut torn = whole.clone();
            torn[byte] ^= 0xFF;
            std::fs::write(&path, &torn).unwrap();
            assert_eq!(MetaFile::open(&path).unwrap().1, Meta { term: 3, vote: Some(NodeId(2)) }, "byte {byte}");
        }
    }
}
