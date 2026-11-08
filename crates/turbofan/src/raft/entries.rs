//! The log in memory: every entry's term, the records of the latest, and its `Members` entries.

use super::*;

/// The log since the snapshot: every entry's term, and the records of the latest. Older records
/// are on disk only.
#[derive(Debug)]
pub struct Entries {
    /// The first index in the log.
    pub(super) first: u64,
    /// Where each run of entries of one term starts, and its term; empty for an empty log.
    pub(super) terms: Vec<(u64, u64)>,
    /// The index of `records[0]`; the records run to the last entry.
    pub(super) window_first: u64,
    pub(super) records: VecDeque<Vec<u8>>,
    pub(super) bytes: usize,
    /// The `Members` entries, by index.
    pub(super) configs: Vec<(u64, Vec<Member>)>,
    /// The log on disk dropped a damaged tail ([`Entries::doubt`]).
    pub(super) doubt: bool,
}

impl Default for Entries {
    fn default() -> Self {
        Self::starting_at(1)
    }
}

impl Entries {
    pub(super) fn starting_at(first: u64) -> Self {
        let (terms, records, configs) = (Vec::new(), VecDeque::new(), Vec::new());
        Self { first, terms, window_first: first, records, bytes: 0, configs, doubt: false }
    }

    /// The log on disk dropped a damaged tail on restart ([`crate::log::Log::in_doubt`]), which
    /// may have held entries this node acknowledged: Raft doubts its own log until it's caught up.
    pub fn doubt(&mut self) {
        self.doubt = true;
    }

    /// Adds the next record of the log as found on disk, keeping at most `window_bytes` of
    /// records in memory.
    ///
    /// # Panics
    ///
    /// If it doesn't continue the log.
    pub fn recover(&mut self, record: Record<'_>, window_bytes: usize) {
        if self.terms.is_empty() {
            *self = Self::starting_at(record.index);
        }
        self.push(record);
        self.evict(record.index, window_bytes);
    }

    /// The log of `records`, whole and continuing each other, as [`Entries::recover`] keeps it.
    ///
    /// # Panics
    ///
    /// If they aren't.
    #[must_use]
    pub fn from_records(records: &[u8], window_bytes: usize) -> Self {
        let mut log = Self::default();
        for record in codec::records(records) {
            log.recover(record.expect("whole, valid records"), window_bytes);
        }
        log
    }

    pub(super) fn last(&self) -> u64 {
        self.window_first + crate::widen(self.records.len()) - 1
    }

    /// Which of `terms` holds `index`, which is in the log.
    pub(super) fn run_of(&self, index: u64) -> usize {
        self.terms.partition_point(|&(first, _)| first <= index) - 1
    }

    /// The term of the entry at `index`, if it's in the log.
    #[must_use]
    pub fn term_at(&self, index: u64) -> Option<u64> {
        (self.first..=self.last()).contains(&index).then(|| self.terms[self.run_of(index)].1)
    }

    pub(super) fn record(&self, index: u64) -> Option<&[u8]> {
        let i = usize::try_from(index.checked_sub(self.window_first)?).ok()?;
        self.records.get(i).map(Vec::as_slice)
    }

    /// Appends `record`, returning its bytes as stored and shipped.
    pub(super) fn push(&mut self, record: Record<'_>) -> &[u8] {
        assert_eq!(record.index, self.last() + 1, "records continue the log");
        if self.terms.last().is_none_or(|&(_, term)| term != record.term) {
            self.terms.push((record.index, record.term));
        }
        // Decoded only if it's a `Members` entry: nearly every one is a service's, for the node.
        let header = record.payload.get(..wire::MessageHeader::SIZE).map(wire::MessageHeader::decode);
        if header.is_some_and(|h| h.template_id == wire::Members::TEMPLATE_ID)
            && let Ok((wire::Decoded::Members(m), _)) = wire::decode(record.payload)
        {
            let members = m
                .member()
                .map(|a| Member::read(NodeId(a.id()), a.learner(), a.peer(), a.client(), a.admin()))
                .collect();
            self.configs.push((record.index, members));
        }
        let mut bytes = Vec::with_capacity(codec::RECORD_HEADER + record.payload.len());
        record.encode_into(&mut bytes);
        self.bytes += bytes.len();
        self.records.push_back(bytes);
        self.records.back().expect("just pushed")
    }

    /// Removes the entries from `index` on. After a restart, their records may be on disk only.
    pub(super) fn truncate_from(&mut self, index: u64) {
        assert!(index >= self.first);
        let keep = usize::try_from(index.saturating_sub(self.window_first)).expect("in the log");
        for record in self.records.drain(keep.min(self.records.len())..) {
            self.bytes -= record.len();
        }
        self.window_first = self.window_first.min(index);
        self.terms.retain(|&(first, _)| first < index);
        self.configs.retain(|&(at, _)| at < index);
    }

    /// Drops the entries up to `index`, which a snapshot covers.
    pub(super) fn trim_to(&mut self, index: u64) {
        if index < self.first {
            return;
        }
        let drop = usize::try_from((index + 1).saturating_sub(self.window_first)).expect("in the log");
        for record in self.records.drain(..drop.min(self.records.len())) {
            self.bytes -= record.len();
        }
        self.window_first = self.window_first.max(index + 1);
        self.first = index + 1;
        self.configs.retain(|&(at, _)| at > index);
        while self.terms.len() > 1 && self.terms[1].0 <= self.first {
            self.terms.remove(0);
        }
        if self.first > self.last() {
            self.terms.clear();
        } else if let Some(run) = self.terms.first_mut() {
            run.0 = self.first;
        }
    }

    /// Where to compact to for a snapshot of `index`, keeping as many as `tail` entries before
    /// it: never before the log's first entry, whose term is known, nor before a change of members
    /// at or before `index`, so the members before what's kept are the snapshot's.
    pub(super) fn compact_point(&self, index: u64, tail: u64) -> u64 {
        let config = self.configs.iter().rev().map(|&(at, _)| at).find(|&at| at <= index).unwrap_or(0);
        self.first.max(index.saturating_sub(tail)).max(config).min(index)
    }

    /// Lets the records up to `index` leave memory, oldest first, while there are more than
    /// `max_bytes`.
    pub(super) fn evict(&mut self, index: u64, max_bytes: usize) {
        while self.bytes > max_bytes && self.window_first <= index {
            let record = self.records.pop_front().expect("the window holds index");
            self.bytes -= record.len();
            self.window_first += 1;
        }
    }
}
