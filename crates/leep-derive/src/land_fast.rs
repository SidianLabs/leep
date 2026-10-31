//! The land fast path (ADR 0066): the leader of a ref's lands remembers
//! which paths the ref's recent changesets wrote, so a land none of whose
//! paths the ref touched since its base is copied onto the head as is,
//! without a three-way merge.
//!
//! The map is a cache of what the ref's history says, keyed by the head it
//! describes; nothing depends on it being there. It is checked against the
//! head every batch attempt reads, and anything it cannot answer (a cold map
//! after a restart or a lost lease, a base older than its window, a base off
//! the ref's first-parent line) goes the slow way, which is the full replay.

use std::collections::{BTreeMap, HashMap, VecDeque};

use leep_core::{ChangesetId, FileChange, RepoPath, Result};

use crate::Repo;

/// First-parent changesets of the ref the map remembers writes for. A land
/// based further back than this replays the slow way. Both bounds are small
/// under test, so the tests reach them.
pub(crate) const MAP_TIPS: usize = if cfg!(test) { 48 } else { 4096 };
/// Paths and directories the map holds at most. Past it the map drops what
/// only lands based before its oldest tip could need, and if that is not
/// enough, starts again from the head.
pub(crate) const MAP_ENTRIES: usize = if cfg!(test) { 600 } else { 1 << 20 };
/// Longest first-parent chain a move is followed down to the head it left;
/// a longer one (a large fast-forward) starts the map again from the new
/// head.
const MAX_TRAIL: usize = 1024;

/// Why a land took the path it took, for `leep_land_fast_map_total`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Lookup {
    /// The base is a tip the map knows, and the land is disjoint.
    Hit,
    /// The base is known, and the land writes where the ref wrote since.
    Overlap,
    /// The map does not reach the base: it started at a later head.
    Cold,
    /// The base is older than the map's window.
    Window,
    /// Not a land the fast path takes: no base on the ref's first-parent
    /// line (a root changeset, or a stack on a landed copy of its own).
    Ineligible,
}

impl Lookup {
    pub(crate) fn label(self) -> &'static str {
        match self {
            Lookup::Hit => "hit",
            Lookup::Overlap => "overlap",
            Lookup::Cold => "cold",
            Lookup::Window => "window",
            Lookup::Ineligible => "ineligible",
        }
    }

    pub(crate) fn record(self) {
        metrics::counter!("leep_land_fast_map_total", "result" => self.label()).increment(1);
    }
}

/// For the first-parent line of one ref ending at `head`: the sequence
/// number of each tip, the newest tip that wrote each path, and the newest
/// tip that wrote below each directory.
#[derive(Default)]
pub(crate) struct WriteMap {
    /// The head this map describes; `None` before it is seeded.
    head: Option<ChangesetId>,
    tips: HashMap<ChangesetId, u64>,
    /// Tips oldest first, for eviction.
    order: VecDeque<ChangesetId>,
    next: u64,
    /// Tips were evicted since the map was last seeded.
    evicted: bool,
    paths: HashMap<Box<str>, u64>,
    /// Directories (never the root) by the newest write strictly below them.
    dirs: HashMap<Box<str>, u64>,
}

impl WriteMap {
    pub(crate) fn head(&self) -> Option<ChangesetId> {
        self.head
    }

    /// Make the map describe `start`, the head a batch attempt read: kept
    /// when it already does (a head names its whole history), otherwise
    /// started again with `start` as its only tip.
    pub(crate) fn sync(&mut self, start: Option<ChangesetId>) {
        if self.head == start && start.is_some() {
            return;
        }
        if self.head.is_some() {
            metrics::counter!("leep_land_fast_map_resets_total").increment(1);
        }
        self.reset(start);
    }

    fn reset(&mut self, head: Option<ChangesetId>) {
        *self = WriteMap::default();
        if let Some(h) = head {
            self.head = Some(h);
            self.tips.insert(h, 0);
            self.order.push_back(h);
            self.next = 1;
        }
    }

    /// The sequence number of `base` when the map knows it as a tip.
    pub(crate) fn base(&self, base: ChangesetId) -> std::result::Result<u64, Lookup> {
        match self.tips.get(&base) {
            Some(&s) => Ok(s),
            None if self.evicted => Err(Lookup::Window),
            None => Err(Lookup::Cold),
        }
    }

    /// Whether a change at `path` may meet something the ref wrote after
    /// tip `since`: the same path, a file where one of its directories is,
    /// or anything below it.
    pub(crate) fn touched_since(&self, since: u64, path: &str) -> bool {
        let after = |m: &HashMap<Box<str>, u64>, k: &str| m.get(k).is_some_and(|&s| s > since);
        if after(&self.paths, path) || after(&self.dirs, path) {
            return true;
        }
        path.match_indices('/')
            .any(|(i, _)| after(&self.paths, &path[..i]))
    }

    /// Append `tip`, a child of the head on its first-parent line, which
    /// wrote `paths` relative to it.
    fn push<'a>(&mut self, tip: ChangesetId, paths: impl Iterator<Item = &'a RepoPath>) {
        let seq = self.next;
        self.next += 1;
        for p in paths {
            let s = p.as_str();
            self.paths.insert(s.into(), seq);
            for (i, _) in s.match_indices('/') {
                self.dirs.insert(s[..i].into(), seq);
            }
        }
        self.tips.insert(tip, seq);
        self.order.push_back(tip);
        self.head = Some(tip);
    }

    /// Keep the map within [`MAP_TIPS`] and [`MAP_ENTRIES`].
    fn trim(&mut self) {
        while self.order.len() > MAP_TIPS {
            if let Some(old) = self.order.pop_front() {
                self.tips.remove(&old);
                self.evicted = true;
            }
        }
        if self.paths.len() + self.dirs.len() <= MAP_ENTRIES {
            return;
        }
        // A land the map can answer is based on a tip it still holds, so a
        // write at or before the oldest tip never blocks one.
        let floor = self
            .order
            .front()
            .and_then(|t| self.tips.get(t))
            .copied()
            .unwrap_or(0);
        self.paths.retain(|_, s| *s > floor);
        self.dirs.retain(|_, s| *s > floor);
        if self.paths.len() + self.dirs.len() > MAP_ENTRIES / 2 {
            metrics::counter!("leep_land_fast_map_resets_total").increment(1);
            let head = self.head;
            self.reset(head);
        }
    }

    /// Entries held, for tests of the bounds.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> usize {
        self.paths.len() + self.dirs.len()
    }

    #[cfg(test)]
    pub(crate) fn tips(&self) -> usize {
        self.order.len()
    }
}

impl Repo {
    /// Bring `map` from `from` to `to`, a move of the ref a batch just made:
    /// the first-parent chain from `to` down to `from` is appended, each
    /// with the paths its changes name. A chain that does not reach `from`
    /// in [`MAX_TRAIL`] steps starts the map again at `to`.
    ///
    /// A failed read starts the map again at `to` too: the map is only ever
    /// less warm for it, never wrong.
    pub(crate) async fn follow_move(
        &self,
        map: &mut WriteMap,
        from: Option<ChangesetId>,
        to: ChangesetId,
    ) {
        if self.chain_to(map, from, to).await.is_err() {
            map.reset(Some(to));
        }
    }

    async fn chain_to(
        &self,
        map: &mut WriteMap,
        from: Option<ChangesetId>,
        to: ChangesetId,
    ) -> Result<()> {
        let mut chain = Vec::new();
        let mut cur = to;
        let reached = loop {
            if Some(cur) == from {
                break true;
            }
            if chain.len() >= MAX_TRAIL || from.is_none() {
                break false;
            }
            let cs = self.get_changeset(cur).await?;
            let parent = cs.parents.first().copied();
            chain.push((cur, cs.changes));
            match parent {
                Some(p) => cur = p,
                None => break false,
            }
        };
        if !reached || map.head != from {
            map.reset(Some(to));
            return Ok(());
        }
        for (id, changes) in chain.iter().rev() {
            map.push(*id, changes.keys());
        }
        map.trim();
        Ok(())
    }

    /// `changes` (relative to `parent`) less the modifies that leave the
    /// file as `parent` has it: what a three-way merge of a land nothing
    /// else touched gives, since the tree diff it starts from omits them.
    pub(crate) async fn effective_changes(
        &self,
        parent: ChangesetId,
        changes: &BTreeMap<RepoPath, FileChange>,
    ) -> Result<BTreeMap<RepoPath, FileChange>> {
        let modified: Vec<&RepoPath> = changes
            .iter()
            .filter(|(_, c)| matches!(c, FileChange::Modify { .. }))
            .map(|(p, _)| p)
            .collect();
        if modified.is_empty() {
            return Ok(changes.clone());
        }
        let had = self
            .files_at(self.root_tree(parent).await?, &modified)
            .await?;
        Ok(changes
            .iter()
            .filter(|(p, c)| match c {
                FileChange::Modify { content, kind, .. } => !matches!(
                    had.get(*p),
                    Some(Some(i)) if i.content == *content && i.kind == *kind
                ),
                FileChange::Delete => true,
            })
            .map(|(p, c)| (p.clone(), c.clone()))
            .collect())
    }
}
