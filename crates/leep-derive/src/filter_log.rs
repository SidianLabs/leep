//! Log filtered by author or `extra` without a path, answered with the
//! `log-filter-v1` index (ADR 0047).
//!
//! The walk is the plain newest-first walk over all ancestors, ordered by
//! generation then id, with one change: before expanding a changeset it asks
//! the index whether the stretch below it can hold a match, and if not, it
//! queues the first-parent ancestor at the far end of the stretch instead of
//! the parents. Every match left is still below something queued, and an
//! ancestor ranks below its descendants, so the heap yields matches in the
//! order and with the limit the plain walk does.
//!
//! A filter is first run as a plain scan of [`SCAN_FIRST`] changesets, longer
//! while matches keep coming (see [`SCAN_RATIO`]), which answers common values
//! (a frequent author, a popular model) faster than the index could; the walk
//! then continues from the same heap with the index.

use std::collections::hash_map::Entry;
use std::collections::{BinaryHeap, HashMap, HashSet};

use leep_core::api::ChangesetInfo;
use std::sync::Arc;

use leep_core::{Changeset, ChangesetId, Hash, Result, TreeNode};

use crate::derived::{
    author_fingerprint, key_fingerprint, value_fingerprint, DerivedKind, FilterRow, Level,
    LogFilterIndex, SkipEntry, Skiplist, CLASS_AUTHOR, CLASS_KEY, CLASS_VALUE, INDEXED_VALUES,
};
use crate::history::{LogFilter, Step, Walk};
use crate::Repo;

/// Changesets scanned plainly before the index is consulted.
pub(crate) const SCAN_FIRST: usize = 512;

/// The scan goes on past [`SCAN_FIRST`] while at least one changeset in
/// this many matches: the indexed walk visits about 20 changesets per match,
/// each a round trip, where the scan reads changesets in batches.
pub(crate) const SCAN_RATIO: usize = 150;

/// Most distinct authors an author filter may match and still use the index;
/// a broader substring is frequent enough for the scan.
pub(crate) const MAX_AUTHOR_CANDIDATES: usize = 4096;

/// What a stretch must contain to hold a match: per part of the filter, one
/// of a set of fingerprints of one class.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Probe {
    parts: Vec<(u64, Vec<u64>)>,
}

impl Probe {
    fn may_match(&self, level: &Level) -> bool {
        self.parts
            .iter()
            .all(|(class, fps)| level.may_contain_any(*class, fps))
    }
}

impl Repo {
    /// [`Repo::log_filtered`] without a path and with a non-empty filter.
    pub(crate) async fn log_by_filter(
        &self,
        id: ChangesetId,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        self.log_by_filter_with(id, limit, filter, SCAN_FIRST).await
    }

    /// [`Repo::log_by_filter`], scanning `scan_first` changesets plainly.
    pub(crate) async fn log_by_filter_with(
        &self,
        id: ChangesetId,
        limit: usize,
        filter: &LogFilter,
        scan_first: usize,
    ) -> Result<Vec<ChangesetInfo>> {
        let mut out = Vec::new();
        let mut walk = Walk::start(self, id, false, usize::MAX).await?;
        let mut scanned = 0;
        // Keep scanning while matches come often enough that the scan is the
        // cheaper way to the limit (see `SCAN_RATIO`).
        let mut budget = scan_first;
        while out.len() < limit && scanned < budget {
            let Some(step) = walk.next(self, usize::MAX).await? else {
                break;
            };
            scanned += 1;
            push_match(&mut out, step, filter);
            if scanned == budget && out.len() * SCAN_RATIO >= scanned {
                budget += scan_first;
            }
        }
        if out.len() < limit && !walk.is_done() {
            match self.filter_probe(id, filter).await? {
                Some(probe) => {
                    let (seen, heap) = (
                        std::mem::take(&mut walk.seen),
                        std::mem::take(&mut walk.heap),
                    );
                    self.walk_indexed(seen, heap, &probe, limit, filter, &mut out)
                        .await?;
                }
                None => {
                    while out.len() < limit {
                        let Some(step) = walk.next(self, usize::MAX).await? else {
                            break;
                        };
                        push_match(&mut out, step, filter);
                    }
                }
            }
        }
        let ids: Vec<ChangesetId> = out.iter().map(|i| i.id).collect();
        for (info, root) in out.iter_mut().zip(self.root_trees(&ids).await?) {
            info.root_tree = root;
        }
        Ok(out)
    }

    /// Continue a walk from `heap`, skipping stretches the index rules out.
    /// Returns how many changesets it visited.
    ///
    /// Expanding a merge queues its other parents *bounded*: they only have
    /// to find matches the first parent cannot reach. The row names, per
    /// side parent, the first changeset on its first-parent chain that the
    /// first parent reaches; everything below it on that chain is the first
    /// parent's walk's business, so the side's walk ends there. A bounded
    /// walk only follows first parents and jumps (a merge on it bounds its
    /// own sides afresh), so it stays on that chain. Without this, the walk
    /// into a branch that merges mainline back would follow the branch all
    /// the way down, since the branch's levels include what it merged. A
    /// changeset also queued with another bound becomes unbounded, and a
    /// dropped one is forgotten, so the walk responsible can still queue it.
    async fn walk_indexed(
        &self,
        mut seen: HashSet<ChangesetId>,
        mut heap: BinaryHeap<(u64, ChangesetId)>,
        probe: &Probe,
        limit: usize,
        filter: &LogFilter,
        out: &mut Vec<ChangesetInfo>,
    ) -> Result<usize> {
        let mut bounds: HashMap<ChangesetId, Bound> = HashMap::new();
        let mut ahead = Ahead::default();
        let mut pops = 0;
        while out.len() < limit {
            let Some((generation, x)) = heap.pop() else {
                break;
            };
            pops += 1;
            #[cfg(test)]
            tests::POPS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            let bound = bounds.remove(&x);
            if bound.is_some_and(|b| (generation, x) <= b.stop) {
                seen.remove(&x);
                continue;
            }
            if !ahead.has_rows(x) {
                ahead.read_rows(self, x, &heap).await?;
            }
            let (row, sk) = ahead.take(x);
            // Levels nest, so the widest stretch without a match is the
            // highest level that rules one out.
            if let Some(j) = (0..row.levels.len())
                .rev()
                .find(|&j| !probe.may_match(&row.levels[j]))
            {
                if let Some(e) = sk.get(j) {
                    queue(&mut seen, &mut heap, &mut bounds, e.generation, e.id, bound);
                }
                continue;
            }
            if !ahead.css.contains_key(&x) {
                ahead.read_changesets(self, x, &heap, probe).await?;
            }
            let cs = match ahead.css.remove(&x) {
                Some(cs) => cs,
                None => self.get_changeset(x).await?,
            };
            for (i, &p) in cs.parents.iter().enumerate() {
                let g = match sk.first() {
                    Some(e) if i == 0 => e.generation,
                    _ if seen.contains(&p) && !bounds.contains_key(&p) => continue,
                    _ => self.generation(p).await?,
                };
                let b = if i == 0 {
                    bound
                } else {
                    // A side this walk's own bound already reaches (a
                    // branch merging mainline back) is not its business.
                    if let Some(outer) = bound {
                        if self.on_chain(p, g, outer.first).await? {
                            continue;
                        }
                    }
                    row.stops.get(i - 1).copied().flatten().map(|stop| Bound {
                        stop,
                        first: cs.parents[0],
                    })
                };
                queue(&mut seen, &mut heap, &mut bounds, g, p, b);
            }
            if filter.matches(&cs) {
                out.push(ChangesetInfo {
                    id: x,
                    changeset: cs,
                    generation,
                    root_tree: TreeNode::empty_hash(),
                    path_at: None,
                    renamed: None,
                });
            }
        }
        Ok(pops)
    }

    /// Whether `ancestor`, of generation `target`, is on `descendant`'s
    /// first-parent chain, riding it down by skiplists kept between queries.
    async fn on_chain(
        &self,
        ancestor: ChangesetId,
        target: u64,
        descendant: ChangesetId,
    ) -> Result<bool> {
        let mut x = descendant;
        loop {
            if x == ancestor {
                return Ok(true);
            }
            let sk = match self.cached_skips(x) {
                Some(sk) => sk,
                None => {
                    let sk = self.skiplist(x).await?;
                    self.cache_skips(x, sk.clone());
                    sk
                }
            };
            match sk.iter().rev().find(|e| e.generation >= target) {
                Some(e) => x = e.id,
                None => return Ok(false),
            }
        }
    }

    /// The index probe for `filter` at `head`, or `None` if no part of it
    /// narrows the walk.
    pub(crate) async fn filter_probe(
        &self,
        head: ChangesetId,
        filter: &LogFilter,
    ) -> Result<Option<Probe>> {
        let mut parts = Vec::new();
        for m in &filter.extra {
            parts.push(match &m.value {
                Some(v) if INDEXED_VALUES.contains(&m.key.as_str()) => {
                    (CLASS_VALUE, vec![value_fingerprint(&m.key, v)])
                }
                _ => (CLASS_KEY, vec![key_fingerprint(&m.key)]),
            });
        }
        if let Some(a) = &filter.author {
            if let Some(fps) = self.author_candidates(head, a).await? {
                parts.push((CLASS_AUTHOR, fps));
            }
        }
        Ok((!parts.is_empty()).then_some(Probe { parts }))
    }

    /// Fingerprints of the authors reachable from `head` whose name contains
    /// `query` (case-insensitive), sorted; `None` when that is too broad to
    /// help or cannot be told.
    ///
    /// The widest levels down `head`'s first-parent chain cover all of its
    /// ancestors, so their author sets together name every author it can
    /// reach. An author whose name row is missing is kept: it may match.
    async fn author_candidates(&self, head: ChangesetId, query: &str) -> Result<Option<Vec<u64>>> {
        let query = query.to_lowercase();
        if query.is_empty() {
            return Ok(None);
        }
        let exact = author_fingerprint(&query);
        let mut all: Vec<u64> = Vec::new();
        let mut x = head;
        loop {
            let row = self.filter_row(x).await?;
            let Some(j) = (0..row.levels.len())
                .rev()
                .find(|&j| row.levels[j].class(CLASS_AUTHOR).is_some())
            else {
                return Ok(None);
            };
            all.extend_from_slice(row.levels[j].class(CLASS_AUTHOR).expect("found above"));
            if row.depth == 0 {
                break;
            }
            x = self.skiplist(x).await?[j].id;
        }
        all.sort_unstable();
        all.dedup();
        let names = self.filter_names(&all).await?;
        let found: Vec<u64> = all
            .into_iter()
            .zip(names)
            .filter(|(fp, name)| *fp == exact || name.as_deref().is_none_or(|n| n.contains(&query)))
            .map(|(fp, _)| fp)
            .collect();
        Ok((found.len() <= MAX_AUTHOR_CANDIDATES).then_some(found))
    }
}

/// Changesets read ahead per batch by [`Ahead`].
const AHEAD: usize = 16;

/// Rows, skiplists and changesets for the walk, read a batch at a time for
/// the changesets nearest the top of the heap: a merge queues several
/// parents, and each pop otherwise waits for its own round trips. Guesses
/// never change the walk; whatever a batch misses is read singly.
#[derive(Default)]
struct Ahead {
    rows: HashMap<ChangesetId, Arc<FilterRow>>,
    sks: HashMap<ChangesetId, Arc<[SkipEntry]>>,
    css: HashMap<ChangesetId, Changeset>,
}

impl Ahead {
    fn has_rows(&self, id: ChangesetId) -> bool {
        self.rows.contains_key(&id) && self.sks.contains_key(&id)
    }

    /// Take what was read for `id`.
    fn take(&mut self, id: ChangesetId) -> (Arc<FilterRow>, Arc<[SkipEntry]>) {
        (
            self.rows.remove(&id).expect("read"),
            self.sks.remove(&id).expect("read"),
        )
    }

    /// `first` and the queued changesets ranking highest after it that pass
    /// `keep`, at most [`AHEAD`] in all.
    fn nearest(
        first: ChangesetId,
        heap: &BinaryHeap<(u64, ChangesetId)>,
        keep: impl Fn(ChangesetId) -> bool,
    ) -> Vec<ChangesetId> {
        let mut rest: Vec<(u64, ChangesetId)> =
            heap.iter().copied().filter(|e| keep(e.1)).collect();
        if rest.len() > AHEAD - 1 {
            rest.select_nth_unstable_by(AHEAD - 2, |a, b| b.cmp(a));
            rest.truncate(AHEAD - 1);
        }
        let mut ids = vec![first];
        ids.extend(rest.into_iter().map(|e| e.1).filter(|&id| id != first));
        ids
    }

    async fn read_rows(
        &mut self,
        repo: &Repo,
        first: ChangesetId,
        heap: &BinaryHeap<(u64, ChangesetId)>,
    ) -> Result<()> {
        if let (Some(row), Some(sk)) = (repo.cached_filter_row(first), repo.cached_skips(first)) {
            self.rows.insert(first, row);
            self.sks.insert(first, sk);
            return Ok(());
        }
        let ids = Self::nearest(first, heap, |id| {
            !(self.rows.contains_key(&id) && self.sks.contains_key(&id))
        });
        let keys: Vec<Hash> = ids.iter().map(|id| id.0).collect();
        let meta = repo.meta();
        let (rows, sks) = tokio::join!(
            meta.get_derived_many(LogFilterIndex::NAME, &keys),
            meta.get_derived_many(Skiplist::NAME, &keys)
        );
        let rows = rows.ok().filter(|r| r.len() == ids.len());
        let sks = sks.ok().filter(|r| r.len() == ids.len());
        for (i, &id) in ids.iter().enumerate() {
            let row = rows
                .as_ref()
                .and_then(|r| r[i].as_deref())
                .and_then(|b| FilterRow::decode(b).ok());
            let sk = sks
                .as_ref()
                .and_then(|r| r[i].as_deref())
                .and_then(|b| Skiplist::decode(b).ok());
            if let Some(row) = row {
                let row = Arc::new(row);
                repo.cache_filter_row(id, row.clone());
                self.rows.insert(id, row);
            }
            if let Some(sk) = sk {
                for e in &sk {
                    repo.remember_generation(e.id, e.generation);
                }
                let sk: Arc<[SkipEntry]> = sk.into();
                repo.cache_skips(id, sk.clone());
                self.sks.insert(id, sk);
            }
        }
        if let Entry::Vacant(e) = self.rows.entry(first) {
            e.insert(repo.filter_row(first).await?);
        }
        if let Entry::Vacant(e) = self.sks.entry(first) {
            let sk = repo.skiplist(first).await?;
            repo.cache_skips(first, sk.clone());
            e.insert(sk);
        }
        Ok(())
    }

    /// Changesets of `first` and of the nearest queued changesets whose rows
    /// say they will be expanded, with their parents' generations.
    async fn read_changesets(
        &mut self,
        repo: &Repo,
        first: ChangesetId,
        heap: &BinaryHeap<(u64, ChangesetId)>,
        probe: &Probe,
    ) -> Result<()> {
        let ids = Self::nearest(first, heap, |id| {
            !self.css.contains_key(&id)
                && self
                    .rows
                    .get(&id)
                    .is_some_and(|r| r.levels.iter().all(|l| probe.may_match(l)))
        });
        let Ok(found) = repo.meta().get_changesets(&ids).await else {
            return Ok(());
        };
        if found.len() != ids.len() {
            return Ok(());
        }
        let mut parents = Vec::new();
        for (id, cs) in ids.into_iter().zip(found) {
            if let Some(cs) = cs {
                parents.extend(cs.parents.iter().skip(1).copied());
                repo.remember_parents(id, cs.parents.as_slice().into());
                self.css.insert(id, cs);
            }
        }
        repo.prefetch_generations(&parents).await;
        Ok(())
    }
}

/// A walk down one side of a merge: the merge's first parent reaches
/// `stop` and everything below it on this chain, so the walk ends there.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Bound {
    stop: (u64, ChangesetId),
    /// The merge's first parent.
    first: ChangesetId,
}

/// Queue `id` for a walk with `bound` (`None`: unbounded). Already queued
/// by a walk with a different bound, it becomes unbounded.
fn queue(
    seen: &mut HashSet<ChangesetId>,
    heap: &mut BinaryHeap<(u64, ChangesetId)>,
    bounds: &mut HashMap<ChangesetId, Bound>,
    generation: u64,
    id: ChangesetId,
    bound: Option<Bound>,
) {
    if seen.insert(id) {
        heap.push((generation, id));
        if let Some(b) = bound {
            bounds.insert(id, b);
        }
    } else if let Some(old) = bounds.get(&id) {
        if bound != Some(*old) {
            bounds.remove(&id);
        }
    }
}

/// Keep `step` if it matches; its root tree is filled in at the end, in one
/// batch for all matches.
fn push_match(out: &mut Vec<ChangesetInfo>, step: Step, filter: &LogFilter) {
    if filter.matches(&step.cs) {
        out.push(ChangesetInfo {
            id: step.id,
            changeset: step.cs,
            generation: step.generation,
            root_tree: TreeNode::empty_hash(),
            path_at: None,
            renamed: None,
        });
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::Arc;

    use bytes::Bytes;
    use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_PROMPT, EXTRA_AGENT_SESSION};
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{Changeset, FileChange, FileKind, RepoPath};

    use super::*;
    use crate::derived::derive;

    /// Changesets the indexed walks popped, for the profile below.
    pub(super) static POPS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);
    use crate::history::ExtraMatch;

    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.0 >> 33) as usize) % n
        }

        /// Roughly Zipf over `0..n`: small values are common.
        fn zipf(&mut self, n: usize) -> usize {
            let r = (self.below(1 << 20) + 1) as f64 / (1 << 20) as f64;
            ((n as f64).powf(r) as usize).saturating_sub(1).min(n - 1)
        }
    }

    fn repo() -> Repo {
        Repo::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        )
    }

    /// A random DAG with branches that live long, merge the main line back
    /// in, merge each other (criss-cross) and are merged into the main line,
    /// octopus merges and a few extra roots. Returns every id; the last is the
    /// main line's head.
    async fn random_dag(r: &Repo, rng: &mut Lcg, n: usize, authors: usize) -> Vec<ChangesetId> {
        let mut ids: Vec<ChangesetId> = Vec::new();
        let mut branches: Vec<ChangesetId> = Vec::new();
        for i in 0..n {
            let parents: Vec<ChangesetId> = if branches.is_empty() {
                vec![]
            } else {
                let b = rng.below(branches.len());
                let mut ps = vec![branches[b]];
                match rng.below(100) {
                    0..=1 if i < n / 5 => ps.clear(),
                    2..=14 => {
                        let o = branches[rng.below(branches.len())];
                        if !ps.contains(&o) {
                            ps.push(o);
                        }
                    }
                    15..=17 => {
                        let o = ids[rng.below(ids.len())];
                        if !ps.contains(&o) {
                            ps.push(o);
                        }
                        let o = ids[ids.len() - 1 - rng.below(ids.len().min(20))];
                        if !ps.contains(&o) {
                            ps.push(o);
                        }
                    }
                    _ => {}
                }
                ps
            };
            let data = format!("{i}");
            let content = r.blobs().put(Bytes::from(data.clone())).await.unwrap();
            let mut extra = BTreeMap::new();
            if rng.below(3) == 0 {
                extra.insert(
                    EXTRA_AGENT_MODEL.to_string(),
                    format!("model-{}", rng.zipf(6)),
                );
                extra.insert(
                    EXTRA_AGENT_SESSION.to_string(),
                    format!("s{}", rng.below(n / 4 + 1)),
                );
                extra.insert(EXTRA_AGENT_PROMPT.to_string(), format!("p{i}"));
            }
            if rng.below(40) == 0 {
                extra.insert("ci.flaky".to_string(), "yes".to_string());
            }
            let a = rng.zipf(authors);
            let cs = Changeset {
                parents,
                author: format!("Dev{a} Name <dev{a}@example.com>"),
                timestamp: i as i64,
                message: format!("c{i}"),
                changes: BTreeMap::from([(
                    RepoPath::new(format!("f{}", rng.below(5))).unwrap(),
                    FileChange::Modify {
                        content,
                        kind: FileKind::Regular,
                        size: data.len() as u64,
                    },
                )]),
                extra,
            };
            let id = r.create_changeset(cs).await.unwrap();
            ids.push(id);
            if branches.is_empty() || rng.below(12) == 0 {
                branches.push(id);
            } else {
                let b = rng.below(branches.len());
                branches[b] = id;
            }
            if branches.len() > 6 {
                branches.remove(1 + rng.below(branches.len() - 1));
            }
        }
        ids.push(branches[0]);
        ids
    }

    fn filters(authors: usize) -> Vec<LogFilter> {
        let author = |s: String| LogFilter {
            author: Some(s),
            extra: vec![],
        };
        let extra = |s: &str| LogFilter {
            author: None,
            extra: vec![ExtraMatch::parse(s).unwrap()],
        };
        vec![
            author("dev0 name".into()),
            author(format!(
                "DEV{} Name <dev{}@example.com>",
                authors - 1,
                authors - 1
            )),
            author(format!("dev{}", authors / 2)),
            author("dev1".into()),
            author("@example".into()),
            author("nobody".into()),
            author(String::new()),
            extra(EXTRA_AGENT_MODEL),
            extra(&format!("{EXTRA_AGENT_MODEL}:model-4")),
            extra(&format!("{EXTRA_AGENT_MODEL}:model-0")),
            extra(&format!("{EXTRA_AGENT_SESSION}:s3")),
            extra(&format!("{EXTRA_AGENT_PROMPT}:p7")),
            extra("ci.flaky:yes"),
            extra("never.set"),
            LogFilter {
                author: Some("dev2".into()),
                extra: vec![ExtraMatch::parse(&format!("{EXTRA_AGENT_MODEL}:model-1")).unwrap()],
            },
        ]
    }

    #[tokio::test]
    async fn indexed_log_equals_the_scan_on_random_histories() {
        let seeds: u64 = std::env::var("LEEP_FILTER_SEEDS")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(10);
        for seed in 1..=seeds {
            let mut rng = Lcg(seed);
            let r = repo();
            let n = 50 + rng.below(350);
            let authors = 3 + rng.below(40);
            let ids = random_dag(&r, &mut rng, n, authors).await;
            let mut tips = vec![*ids.last().unwrap()];
            for _ in 0..3 {
                tips.push(ids[rng.below(ids.len())]);
            }
            for &tip in &tips {
                for filter in filters(authors) {
                    for limit in [0, 1, 3, 10, 100, usize::MAX] {
                        let want = r.log_scan(tip, None, limit, &filter).await.unwrap();
                        for scan_first in [0, 5, SCAN_FIRST] {
                            let got = r
                                .log_by_filter_with(tip, limit, &filter, scan_first)
                                .await
                                .unwrap();
                            assert_eq!(
                                got, want,
                                "seed {seed}, tip {tip}, limit {limit}, scan {scan_first}, {filter:?}"
                            );
                        }
                    }
                }
            }
        }
    }

    /// Paging by asking for more from the same head returns a prefix of the
    /// longer answer, as the scan does.
    #[tokio::test]
    async fn longer_limits_extend_shorter_ones() {
        let mut rng = Lcg(99);
        let r = repo();
        let ids = random_dag(&r, &mut rng, 600, 30).await;
        let tip = *ids.last().unwrap();
        let filter = LogFilter {
            author: Some("dev7 ".into()),
            extra: vec![],
        };
        let all = r
            .log_by_filter_with(tip, usize::MAX, &filter, 0)
            .await
            .unwrap();
        assert_eq!(
            all,
            r.log_scan(tip, None, usize::MAX, &filter).await.unwrap()
        );
        for limit in 0..all.len() {
            let page = r.log_by_filter_with(tip, limit, &filter, 0).await.unwrap();
            assert_eq!(page[..], all[..limit]);
        }
    }

    /// Name rows only narrow the walk: without them every author may match,
    /// and answers stay the same.
    #[tokio::test]
    async fn lost_name_rows_cost_speed_not_answers() {
        let mut rng = Lcg(17);
        let r = repo();
        let ids = random_dag(&r, &mut rng, 300, 25).await;
        let tip = *ids.last().unwrap();
        derive::<LogFilterIndex>(&r, tip).await.unwrap();
        r.meta()
            .delete_derived_kind(crate::derived::LOG_FILTER_NAME_KIND)
            .await
            .unwrap();
        let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
        for filter in filters(25) {
            assert_eq!(
                fresh.log_by_filter_with(tip, 50, &filter, 0).await.unwrap(),
                r.log_scan(tip, None, 50, &filter).await.unwrap(),
                "{filter:?}"
            );
        }
    }

    #[tokio::test]
    async fn deriving_twice_gives_identical_rows() {
        let mut rng = Lcg(5);
        let a = repo();
        let ids = random_dag(&a, &mut rng, 300, 20).await;
        let tip = *ids.last().unwrap();
        derive::<LogFilterIndex>(&a, tip).await.unwrap();
        let b = repo();
        let again = random_dag(&b, &mut Lcg(5), 300, 20).await;
        assert_eq!(ids, again);
        // Derive the other one from several heads first, in another order.
        for &id in ids.iter().step_by(37) {
            derive::<LogFilterIndex>(&b, id).await.unwrap();
        }
        derive::<LogFilterIndex>(&b, tip).await.unwrap();
        // `b` also derived heads `tip` does not reach; every row `a` has is
        // the same there.
        for kind in [LogFilterIndex::NAME, crate::derived::LOG_FILTER_NAME_KIND] {
            let rows = a.meta().list_derived(kind, None, usize::MAX).await.unwrap();
            assert!(!rows.is_empty());
            for (k, v) in rows {
                assert_eq!(
                    b.meta().get_derived(kind, &k).await.unwrap(),
                    Some(v),
                    "{kind}"
                );
            }
        }
    }

    /// Deriving several changesets at once (ADR 0050) stores exactly the
    /// rows of one at a time, on a history with octopus merges, criss-cross
    /// merges and several roots, both kinds deriving side by side.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn parallel_derivation_stores_the_serial_rows() {
        use crate::derived::{HistoryTree, LastMerge, HISTORY_NODE_KIND, LOG_FILTER_NAME_KIND};
        async fn rows(r: &Repo) -> Vec<(&'static str, Vec<(Hash, Bytes)>)> {
            let mut out = Vec::new();
            for kind in [
                HistoryTree::NAME,
                HISTORY_NODE_KIND,
                LogFilterIndex::NAME,
                LOG_FILTER_NAME_KIND,
                LastMerge::NAME,
            ] {
                let rows = r.meta().list_derived(kind, None, usize::MAX).await.unwrap();
                out.push((kind, rows));
            }
            out
        }
        let serial = repo().with_derive_jobs(1);
        let ids = random_dag(&serial, &mut Lcg(23), 700, 30).await;
        for &id in &ids {
            derive::<HistoryTree>(&serial, id).await.unwrap();
            derive::<LogFilterIndex>(&serial, id).await.unwrap();
        }
        let want = rows(&serial).await;
        assert_eq!(want[0].1.len(), 700);
        for jobs in [2, 8] {
            let r = repo().with_derive_jobs(jobs);
            assert_eq!(random_dag(&r, &mut Lcg(23), 700, 30).await, ids);
            // Newest first, so most derivations start on a long missing range.
            for &id in ids.iter().rev() {
                let (a, b) = futures_util::join!(
                    derive::<HistoryTree>(&r, id),
                    derive::<LogFilterIndex>(&r, id)
                );
                a.unwrap();
                b.unwrap();
            }
            for ((kind, want), (_, got)) in want.iter().zip(rows(&r).await) {
                if *kind == LastMerge::NAME {
                    // Which of these get stored depends on what asked for
                    // them, not their values.
                    let want: std::collections::HashMap<_, _> = want.iter().cloned().collect();
                    assert!(got.iter().all(|(k, v)| want.get(k).is_none_or(|w| w == v)));
                } else {
                    assert!(*want == got, "{kind} rows differ with {jobs} jobs");
                }
            }
        }
    }

    /// A rare author far back on a long chain is found by skipping, with a
    /// read per level rather than per changeset.
    #[tokio::test]
    async fn a_deep_chain_derives_and_skips() {
        let r = repo();
        let mut parent: Option<ChangesetId> = None;
        let n = 12_000;
        let mut rare = Vec::new();
        for i in 0..n {
            let author = if i % 5000 == 17 {
                "Rare One"
            } else {
                "Busy Bee"
            };
            let cs = Changeset {
                parents: parent.into_iter().collect(),
                author: author.into(),
                timestamp: i,
                message: String::new(),
                changes: BTreeMap::new(),
                extra: BTreeMap::new(),
            };
            let id = r.create_changeset(cs).await.unwrap();
            if author == "Rare One" {
                rare.push(id);
            }
            parent = Some(id);
        }
        let tip = parent.unwrap();
        let filter = LogFilter {
            author: Some("rare".into()),
            extra: vec![],
        };
        let got = r.log_by_filter(tip, 100, &filter).await.unwrap();
        let ids: Vec<ChangesetId> = got.iter().map(|i| i.id).collect();
        rare.reverse();
        assert_eq!(ids, rare);
        let probe = r.filter_probe(tip, &filter).await.unwrap().unwrap();
        let mut out = Vec::new();
        let pops = r
            .walk_indexed(
                HashSet::from([tip]),
                BinaryHeap::from([(r.generation(tip).await.unwrap(), tip)]),
                &probe,
                usize::MAX,
                &filter,
                &mut out,
            )
            .await
            .unwrap();
        assert!(pops < 200, "{pops} changesets visited");
        assert_eq!(out.len(), rare.len());
    }

    /// Shaped like the `log_filter_profile` example: mainline merging
    /// long-lived branches that merge it back. Prints changesets visited by
    /// the scan and by the indexed walk per query.
    #[tokio::test]
    #[ignore]
    async fn profile_branchy_history() {
        let n: usize = std::env::var("LEEP_PROFILE_N")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(30_000);
        let authors = n / 50;
        let r = repo();
        let mut rng = Lcg(0x10_6f11);
        let branches = 24;
        let mut heads: Vec<Option<ChangesetId>> = vec![None; branches];
        let mut session = 0;
        for i in 0..n {
            let b = if heads[0].is_none() {
                0
            } else {
                rng.below(branches)
            };
            let mut parents: Vec<ChangesetId> = heads[b].or(heads[0]).into_iter().collect();
            let roll = rng.below(1000);
            let other = if b == 0 && roll < 300 {
                heads[1 + rng.below(branches - 1)]
            } else if b != 0 && roll < 15 {
                heads[0]
            } else {
                None
            };
            if let Some(o) = other {
                if !parents.contains(&o) {
                    parents.push(o);
                }
            }
            let mut extra = BTreeMap::new();
            if rng.below(3) == 0 {
                if rng.below(20) == 0 {
                    session += 1;
                }
                extra.insert(
                    EXTRA_AGENT_MODEL.to_string(),
                    format!("model-{}", rng.zipf(8)),
                );
                extra.insert(
                    EXTRA_AGENT_SESSION.to_string(),
                    format!("session-{session}"),
                );
            }
            let a = rng.zipf(authors);
            let id = r
                .create_changeset(Changeset {
                    parents,
                    author: format!("Developer {a} <dev{a}@x>"),
                    timestamp: i as i64,
                    message: String::new(),
                    changes: BTreeMap::new(),
                    extra,
                })
                .await
                .unwrap();
            heads[b] = Some(id);
        }
        let main = heads[0].unwrap();
        let mut parents = vec![main];
        parents.extend(heads.iter().skip(1).flatten().filter(|h| **h != main));
        let tip = r
            .create_changeset(Changeset {
                parents,
                author: "x".into(),
                timestamp: 0,
                message: String::new(),
                changes: BTreeMap::new(),
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        let t = std::time::Instant::now();
        derive::<LogFilterIndex>(&r, tip).await.unwrap();
        println!("derived in {:.2} s", t.elapsed().as_secs_f64());
        let rows = r
            .meta()
            .list_derived(LogFilterIndex::NAME, None, usize::MAX)
            .await
            .unwrap();
        let bytes: usize = rows.iter().map(|(_, v)| v.len()).sum();
        println!(
            "{} rows, {:.0} bytes/changeset",
            rows.len(),
            bytes as f64 / rows.len() as f64
        );
        let order: Vec<ChangesetId> = r
            .log(tip, None, usize::MAX)
            .await
            .unwrap()
            .iter()
            .map(|i| i.id)
            .collect();
        let author = |s: &str| LogFilter {
            author: Some(s.into()),
            extra: vec![],
        };
        let extra = |s: &str| LogFilter {
            author: None,
            extra: vec![ExtraMatch::parse(s).unwrap()],
        };
        for (name, f) in [
            ("rank 0", author("Developer 0 <")),
            ("rank 10", author("Developer 10 <")),
            ("rank 50", author("Developer 50 <")),
            ("rank 300", author(&format!("Developer {} <", authors / 2))),
            ("nobody", author("nobody")),
            ("model-0", extra("agent.model:model-0")),
            ("model-7", extra("agent.model:model-7")),
            ("session-17", extra("agent.session:session-17")),
        ] {
            POPS.store(0, std::sync::atomic::Ordering::Relaxed);
            let got = r.log_by_filter_with(tip, 100, &f, 0).await.unwrap();
            let pops = POPS.load(std::sync::atomic::Ordering::Relaxed);
            let scan = got.last().map_or(order.len(), |l| {
                order.iter().position(|i| *i == l.id).unwrap() + 1
            });
            let scan = if got.len() < 100 { order.len() } else { scan };
            println!(
                "{name:>12}: {:>3} matches, scan visits {scan:>6}, index visits {pops:>6}",
                got.len()
            );
        }
    }
}
