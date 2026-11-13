//! Making a log's writes durable, one drive flush shared by the logs syncing at once.
//!
//! On macOS `fsync` only hands a file's writes to the drive, which may hold them in its cache
//! through a power loss; `F_FULLFSYNC` (std's `sync_data`) also flushes that cache, the whole
//! drive's, and costs milliseconds, and concurrent ones barely overlap: nodes on one machine (the
//! benchmark, a developer's cluster) sync one after another. So a sync there `fsync`s its own
//! file, then waits for a full flush that started after its `fsync` returned: one flushes the
//! drive for every log waiting, whichever file it's issued on. Elsewhere `fdatasync` is per file
//! and the journal merges concurrent ones, so a log just syncs. Copied from Turbojet's.

use std::fs::File;
use std::io;

#[cfg(target_vendor = "apple")]
pub(crate) use apple::DriveFlush;

/// Syncs a file's data, without anything shared (not on macOS).
#[cfg(not(target_vendor = "apple"))]
#[derive(Debug, Clone)]
pub(crate) struct DriveFlush;

#[cfg(not(target_vendor = "apple"))]
impl DriveFlush {
    /// The flush for files under `dir`.
    pub(crate) fn for_dir(_dir: &std::path::Path) -> io::Result<Self> {
        Ok(Self)
    }

    /// Makes `file`'s writes durable.
    pub(crate) fn sync_data(&self, file: &File) -> io::Result<()> {
        file.sync_data()
    }
}

#[cfg(target_vendor = "apple")]
mod apple {
    use std::collections::BTreeMap;
    use std::os::unix::fs::MetadataExt;
    use std::path::Path;
    use std::sync::{Arc, Condvar, Mutex, PoisonError};

    use super::{File, io};

    /// The flushes of each drive (by device number) this process has logs on: one each, so
    /// bounded by the drives.
    static DRIVES: Mutex<BTreeMap<u64, Arc<Drive>>> = Mutex::new(BTreeMap::new());

    /// Syncs a file's data, sharing a full flush of its drive with the other logs waiting for
    /// one.
    #[derive(Debug, Clone)]
    pub(crate) struct DriveFlush(Arc<Drive>);

    impl DriveFlush {
        /// The flush for files under `dir`: its drive's.
        pub(crate) fn for_dir(dir: &Path) -> io::Result<Self> {
            let dev = std::fs::metadata(dir)?.dev();
            let mut drives = DRIVES.lock().unwrap_or_else(PoisonError::into_inner);
            Ok(Self(drives.entry(dev).or_default().clone()))
        }

        /// Makes `file`'s writes durable: on the drive by `fsync`, then through its cache by a
        /// full flush started after that.
        pub(crate) fn sync_data(&self, file: &File) -> io::Result<()> {
            rustix::fs::fsync(file)?;
            self.0.flush(file)
        }
    }

    #[derive(Debug, Default)]
    struct Drive {
        state: Mutex<Flushes>,
        finished: Condvar,
    }

    /// Full flushes of one drive, numbered from 1 as they start.
    #[derive(Debug, Default)]
    struct Flushes {
        started: u64,
        finished: u64,
        /// The last that succeeded, and how the last that failed did.
        succeeded: u64,
        failed: Option<io::ErrorKind>,
        running: bool,
    }

    impl Drive {
        /// Returns once a full flush started after the call has finished, issuing it on `file`
        /// if none is running: a flush already running may have started before the caller's
        /// writes reached the drive.
        fn flush(&self, file: &File) -> io::Result<()> {
            let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
            let needed = state.started + 1;
            while state.finished < needed {
                if state.running {
                    state = self.finished.wait(state).unwrap_or_else(PoisonError::into_inner);
                    continue;
                }
                state.running = true;
                state.started += 1;
                let number = state.started;
                drop(state);
                let result = rustix::fs::fcntl_fullfsync(file).map_err(io::Error::from);
                state = self.state.lock().unwrap_or_else(PoisonError::into_inner);
                state.running = false;
                state.finished = number;
                match &result {
                    Ok(()) => state.succeeded = number,
                    Err(e) => state.failed = Some(e.kind()),
                }
                self.finished.notify_all();
                // The first flush to start after the call is the caller's own.
                debug_assert_eq!(number, needed);
                return result;
            }
            if state.succeeded >= needed {
                Ok(())
            } else {
                let kind = state.failed.unwrap_or(io::ErrorKind::Other);
                Err(io::Error::new(kind, "the drive flush this sync waited for failed"))
            }
        }
    }

    #[cfg(test)]
    mod tests {
        use std::io::Write;

        use super::*;

        /// Syncs at once on one drive share flushes, and each still returns only after one that
        /// started after it was called.
        #[test]
        fn syncs_at_once_share_a_drive_flush() {
            let dir = tempfile::tempdir().unwrap();
            let flush = DriveFlush::for_dir(dir.path()).unwrap();
            assert!(Arc::ptr_eq(&flush.0, &DriveFlush::for_dir(dir.path()).unwrap().0), "one per drive");
            let before = flush.0.state.lock().unwrap().started;
            let threads = 16;
            std::thread::scope(|scope| {
                for n in 0..threads {
                    let (dir, flush) = (dir.path(), flush.clone());
                    scope.spawn(move || {
                        let mut file = File::create(dir.join(n.to_string())).unwrap();
                        for _ in 0..4 {
                            file.write_all(b"record").unwrap();
                            let called = flush.0.state.lock().unwrap().started;
                            flush.sync_data(&file).unwrap();
                            assert!(flush.0.state.lock().unwrap().succeeded > called);
                        }
                    });
                }
            });
            let state = flush.0.state.lock().unwrap();
            assert!(state.started - before <= threads * 4);
            assert!(!state.running && state.finished == state.started);
        }
    }
}
