//! Captures of a cluster's log, replayed offline: to reproduce a bug from production, check a
//! new version of a service against real traffic, or audit what happened.
//!
//! An operator asks a node for one ([`crate::Admin::capture`]). The node puts its latest snapshot
//! and its log from there to its commit index in a directory of its data directory,
//! `captures/<snapshot>-<to>`, laid out as a data directory is: `snapshots/`, hard linked, and
//! `log/`, its segments hard linked but for the one being appended to, which is copied. So it
//! costs little, but holds disk space the node would have freed: copy it off the node, then
//! delete it. Its `committed` file names the last entry it holds that was committed: a node's
//! log may run past its commit index, and those entries may never be.
//!
//! [`replay`] restores the snapshot into a service and applies the entries to it in order,
//! through a [`Node`] that isn't running Raft, as every member applied them: their time is
//! what's in them. So the service ends as it was on the cluster, and makes the same effects, if
//! it's the same code. Code changed since makes others: replay with each, and compare.

use std::fs;
use std::io;
use std::path::{Path, PathBuf};

use crate::log::{Fsync, Log};
use crate::meta::Meta;
use crate::node::{self, Effect, Node, Service};
use crate::raft::{Entries, NodeId};
use crate::snapshot;

/// The file naming the last entry a capture holds that was committed.
const COMMITTED: &str = "committed";

/// Makes a capture of the node with `data_dir` and `log`: its latest snapshot, and the log after
/// it to `to`, committed. Returns its directory, and the snapshot's index (0: none).
///
/// # Errors
///
/// If the log no longer holds the entries after the snapshot, or the files can't be read or
/// written.
pub(crate) fn make(data_dir: &Path, log: &Log, to: u64) -> io::Result<(PathBuf, u64)> {
    let latest = snapshot::latest(&data_dir.join("snapshots"))?;
    let index = latest.as_ref().map_or(0, |(index, _)| *index);
    let dir = data_dir.join("captures").join(format!("{index}-{to}"));
    // Asked again: made again, whole.
    if dir.exists() {
        fs::remove_dir_all(&dir)?;
    }
    fs::create_dir_all(dir.join("snapshots"))?;
    fs::create_dir_all(dir.join("log"))?;
    if let Some((_, file)) = &latest {
        fs::hard_link(file, dir.join("snapshots").join(file.file_name().expect("a snapshot file")))?;
    }
    log.capture(&dir.join("log"), index + 1, to)?;
    fs::write(dir.join(COMMITTED), format!("{to}\n"))?;
    Ok((dir, index))
}

/// Replays the capture in `dir` into `service`, new: restores its snapshot, if it has one, then
/// applies its entries in order to `to`, or all it holds committed, calling `each` with each
/// entry's index and the effects the service made applying it. Returns the service as it ends.
///
/// # Errors
///
/// If the capture can't be read, is damaged, or ends before `to`; or an entry is of a protocol
/// newer than this code.
pub fn replay<S: Service>(
    dir: &Path,
    service: S,
    to: Option<u64>,
    mut each: impl FnMut(u64, &[Effect]),
) -> io::Result<S> {
    let invalid = |what: String| io::Error::new(io::ErrorKind::InvalidData, what);
    let committed = fs::read_to_string(dir.join(COMMITTED))?;
    let committed: u64 = committed.trim().parse().map_err(|_| invalid(format!("{COMMITTED}: not an index")))?;
    let to = to.unwrap_or(committed);
    if to > committed {
        return Err(invalid(format!("the capture holds entries committed to {committed}, not {to}")));
    }
    let (snapshot, bytes) = snapshot::load(&dir.join("snapshots"))?.unzip();
    // The snapshot names the members; none, a new cluster's come in its log, but go unused.
    let config = node::Config::new(NodeId(1), Vec::new());
    let mut node = Node::new(config, service, Meta::default(), snapshot, Entries::default(), 0);
    if let Some(mut bytes) = bytes {
        node.restore(&mut bytes)?;
    }
    if node.applied() > to {
        return Err(invalid(format!("the capture starts after {to}, at {}", node.applied() + 1)));
    }
    let mut failed = None;
    // Opened as a node opens its own: whatever's damaged is cut off, and replay ends there.
    let _log = Log::open(&dir.join("log"), Fsync::Never, |record| {
        if failed.is_none() && record.index == node.applied() + 1 && record.index <= to {
            match node.replay(record.index, record.payload) {
                Ok(effects) => each(record.index, &effects),
                Err(e) => failed = Some(e),
            }
        }
    })?;
    if let Some(e) = failed {
        return Err(e);
    }
    if node.applied() < to {
        return Err(invalid(format!("the capture ends at {}, before {to}", node.applied())));
    }
    Ok(node.into_service())
}
