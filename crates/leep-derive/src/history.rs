//! Log with optional path and metadata filtering, and provenance summaries.
//!
//! Path queries use the path-history index (`path_history`). Filters on
//! `extra` and `author` are applied to the changesets that walk yields, and
//! without a path they are answered with the `log-filter-v1` index
//! (`filter_log`), which skips stretches of history without a match.

use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};

use leep_core::api::{AgentStat, ChangesetInfo, HumanStat, ProvenanceResponse};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::{Changeset, ChangesetId, Error, Hash, RepoPath, Result, TreeNode};

use crate::derived::{decode_hash, DerivedKind, RootTree};
use crate::prefetch::ReadAhead;
use crate::Repo;

/// One `extra` predicate: the key must be present, and equal to `value` if
/// given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtraMatch {
    pub key: String,
    pub value: Option<String>,
}

impl ExtraMatch {
    /// Parse `key:value` (exact match) or `key` (present, any value). The
    /// split is at the first `:`.
    pub fn parse(s: &str) -> Result<Self> {
        let (key, value) = match s.split_once(':') {
            Some((k, v)) => (k, Some(v.to_string())),
            None => (s, None),
        };
        if key.is_empty() {
            return Err(Error::Invalid(format!("bad extra filter {s:?}: empty key")));
        }
        Ok(ExtraMatch {
            key: key.to_string(),
            value,
        })
    }
}

/// Metadata predicate for [`Repo::log_filtered`]; all parts are ANDed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogFilter {
    /// Case-insensitive substring of the author.
    pub author: Option<String>,
    pub extra: Vec<ExtraMatch>,
}

impl LogFilter {
    pub fn is_empty(&self) -> bool {
        self.author.is_none() && self.extra.is_empty()
    }

    pub fn matches(&self, cs: &Changeset) -> bool {
        if let Some(a) = &self.author {
            if !cs.author.to_lowercase().contains(&a.to_lowercase()) {
                return false;
            }
        }
        self.extra
            .iter()
            .all(|m| match (cs.extra.get(&m.key), &m.value) {
                (Some(have), Some(want)) => have == want,
                (Some(_), None) => true,
                (None, _) => false,
            })
    }
}

/// Iterative newest-first walk over all parents; histories are too deep to
/// recurse over. Reads ahead in batches (see [`ReadAhead`]) without changing
/// the order or the set of changesets it yields.
pub(crate) struct Walk {
    pub(crate) seen: HashSet<ChangesetId>,
    pub(crate) heap: BinaryHeap<(u64, ChangesetId)>,
    ahead: ReadAhead,
}

/// One changeset a [`Walk`] yields, with its root tree if it was read ahead.
pub(crate) struct Step {
    pub(crate) generation: u64,
    pub(crate) id: ChangesetId,
    pub(crate) cs: Changeset,
    pub(crate) root: Option<Hash>,
}

impl Walk {
    /// `roots`: also read ahead root trees. `want`: how many changesets the
    /// caller expects to visit, which bounds the first read-ahead.
    pub(crate) async fn start(
        repo: &Repo,
        id: ChangesetId,
        roots: bool,
        want: usize,
    ) -> Result<Self> {
        let mut ahead = ReadAhead::new(roots);
        ahead.ensure(repo, &[id], u64::MAX, want).await;
        Ok(Walk {
            seen: HashSet::from([id]),
            heap: BinaryHeap::from([(ahead.generation(repo, id).await?, id)]),
            ahead,
        })
    }

    pub(crate) fn is_done(&self) -> bool {
        self.heap.is_empty()
    }

    /// The next changeset; `want` as in [`Walk::start`], counting this one.
    pub(crate) async fn next(&mut self, repo: &Repo, want: usize) -> Result<Option<Step>> {
        let Some((generation, cid)) = self.heap.pop() else {
            return Ok(None);
        };
        self.ahead.ensure(repo, &[cid], generation, want).await;
        let (cs, root) = self.ahead.take(cid);
        let cs = match cs {
            Some(cs) => cs,
            None => repo.get_changeset(cid).await?,
        };
        let new: Vec<ChangesetId> = cs
            .parents
            .iter()
            .copied()
            .filter(|p| !self.seen.contains(p))
            .collect();
        self.ahead.ensure(repo, &new, generation, want).await;
        for &p in &cs.parents {
            if self.seen.insert(p) {
                self.heap.push((self.ahead.generation(repo, p).await?, p));
            }
        }
        Ok(Some(Step {
            generation,
            id: cid,
            cs,
            root,
        }))
    }
}

fn norm(path: Option<&str>) -> Option<&str> {
    path.map(|p| p.trim_matches('/')).filter(|p| !p.is_empty())
}

#[derive(Default)]
struct Tally {
    changesets: u64,
    files: HashSet<RepoPath>,
    first: i64,
    last: i64,
}

impl Tally {
    fn add(&mut self, cs: &Changeset, prefix: &str) {
        if self.changesets == 0 {
            (self.first, self.last) = (cs.timestamp, cs.timestamp);
        }
        self.first = self.first.min(cs.timestamp);
        self.last = self.last.max(cs.timestamp);
        self.changesets += 1;
        self.files
            .extend(cs.changes.keys().filter(|p| p.is_within(prefix)).cloned());
    }
}

impl Repo {
    /// Ancestors of `id` (inclusive, all parents), newest first by generation
    /// then id. With `path`, only changesets that changed that file or
    /// directory relative to their first parent.
    pub async fn log(
        &self,
        id: ChangesetId,
        path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ChangesetInfo>> {
        self.log_filtered(id, path, limit, &LogFilter::default())
            .await
    }

    /// [`Repo::log`] keeping only changesets that satisfy `filter`. `limit`
    /// counts matches, not changesets scanned.
    pub async fn log_filtered(
        &self,
        id: ChangesetId,
        path: Option<&str>,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        match norm(path) {
            Some(path) => self.log_indexed(id, path, limit, filter).await,
            None if filter.is_empty() => self.log_scan(id, None, limit, filter).await,
            None => self.log_by_filter(id, limit, filter).await,
        }
    }

    /// Reference implementation of [`Repo::log_filtered`]: scans every
    /// ancestor and compares its entry at `path` with its first parent's.
    /// Linear in history, so it only serves as the oracle for the index in
    /// tests and as the baseline in benchmarks.
    #[doc(hidden)]
    pub async fn log_scan(
        &self,
        id: ChangesetId,
        path: Option<&str>,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        let path = norm(path);
        let mut out = Vec::new();
        // Without a filter every changeset visited is returned, so the walk
        // can read ahead exactly as far as `limit`; with one, as far as it may.
        let want = |found: usize| match (path, filter.is_empty()) {
            (None, true) => limit - found,
            _ => usize::MAX,
        };
        // A metadata filter alone may skip most changesets, so their root
        // trees are not read ahead; the matches' are read in one batch at the
        // end instead.
        let read_roots = path.is_some() || filter.is_empty();
        let mut later = Vec::new();
        let mut walk = Walk::start(self, id, read_roots, want(0)).await?;
        while out.len() < limit {
            let Some(Step {
                generation,
                id: cid,
                cs,
                root,
            }) = walk.next(self, want(out.len())).await?
            else {
                break;
            };
            // Cheap metadata test first; the path test reads trees.
            if !filter.matches(&cs) {
                continue;
            }
            let root_tree = match root {
                Some(r) => r,
                None if read_roots => self.root_tree(cid).await?,
                None => {
                    later.push(out.len());
                    TreeNode::empty_hash()
                }
            };
            if let Some(path) = path {
                let parent_tree = match cs.parents.first() {
                    Some(p) => self.root_tree(*p).await?,
                    None => TreeNode::empty_hash(),
                };
                if root_tree == parent_tree
                    || self.entry_at(root_tree, path).await?
                        == self.entry_at(parent_tree, path).await?
                {
                    continue;
                }
            }
            out.push(ChangesetInfo {
                id: cid,
                changeset: cs,
                generation,
                root_tree,
                path_at: None,
                renamed: None,
            });
        }
        let ids: Vec<ChangesetId> = later.iter().map(|&i| out[i].id).collect();
        for (i, root) in later.into_iter().zip(self.root_trees(&ids).await?) {
            out[i].root_tree = root;
        }
        Ok(out)
    }

    /// Root trees of `ids` with one batched read, deriving any not stored.
    pub(crate) async fn root_trees(&self, ids: &[ChangesetId]) -> Result<Vec<Hash>> {
        let keys: Vec<Hash> = ids.iter().map(|id| id.0).collect();
        let rows = match self.meta().get_derived_many(RootTree::NAME, &keys).await {
            Ok(rows) if rows.len() == ids.len() => rows,
            _ => vec![None; ids.len()],
        };
        let mut out = Vec::with_capacity(ids.len());
        for (id, row) in ids.iter().zip(rows) {
            out.push(match row.as_deref().map(decode_hash) {
                Some(Ok(h)) => h,
                _ => self.root_tree(*id).await?,
            });
        }
        Ok(out)
    }

    /// Summarize which agents and humans made the changesets reachable from
    /// `id`, scanning at most `limit` of them newest first. With `prefix`,
    /// only changesets whose own changes touch it count.
    pub async fn provenance(
        &self,
        id: ChangesetId,
        prefix: Option<&str>,
        limit: usize,
    ) -> Result<ProvenanceResponse> {
        let prefix = norm(prefix).unwrap_or("");
        let mut agents: HashMap<(String, Option<String>), Tally> = HashMap::new();
        let mut humans: HashMap<String, Tally> = HashMap::new();
        let mut walk = Walk::start(self, id, false, limit).await?;
        let mut scanned = 0u64;
        while (scanned as usize) < limit {
            let Some(Step { cs, .. }) = walk.next(self, limit - scanned as usize).await? else {
                break;
            };
            scanned += 1;
            if !cs.changes.keys().any(|p| p.is_within(prefix)) {
                continue;
            }
            let model = cs.extra.get(EXTRA_AGENT_MODEL);
            let session = cs.extra.get(EXTRA_AGENT_SESSION);
            if model.is_some() || session.is_some() {
                let key = (
                    model.cloned().unwrap_or_else(|| "unknown".into()),
                    session.cloned(),
                );
                agents.entry(key).or_default().add(&cs, prefix);
            } else {
                humans
                    .entry(cs.author.clone())
                    .or_default()
                    .add(&cs, prefix);
            }
        }
        let by_count = |a: &(u64, String), b: &(u64, String)| b.0.cmp(&a.0).then(a.1.cmp(&b.1));
        let mut agents: Vec<_> = agents
            .into_iter()
            .map(|((model, session), t)| AgentStat {
                model,
                session,
                changesets: t.changesets,
                files_touched: t.files.len() as u64,
                first: t.first,
                last: t.last,
            })
            .collect();
        agents.sort_by(|a, b| {
            by_count(
                &(a.changesets, format!("{}\0{:?}", a.model, a.session)),
                &(b.changesets, format!("{}\0{:?}", b.model, b.session)),
            )
        });
        let mut humans: Vec<_> = humans
            .into_iter()
            .map(|(author, t)| HumanStat {
                author,
                changesets: t.changesets,
                files_touched: t.files.len() as u64,
                first: t.first,
                last: t.last,
            })
            .collect();
        humans.sort_by(|a, b| {
            by_count(
                &(a.changesets, a.author.clone()),
                &(b.changesets, b.author.clone()),
            )
        });
        Ok(ProvenanceResponse {
            agents,
            humans,
            scanned,
            truncated: !walk.is_done(),
        })
    }
}

/// Keep only `agent.*` keys.
pub(crate) fn agent_keys(extra: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    extra
        .iter()
        .filter(|(k, _)| k.starts_with("agent."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

#[cfg(test)]
mod read_ahead_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;

    use bytes::Bytes;
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{FileChange, FileKind, Hash, MetaStore, RefLogEntry, RefUpdateContext, Result};

    use super::*;

    /// Counts reads, and with `fail_batches` refuses the batched ones, so the
    /// walk must fall back to single reads.
    struct Probe {
        inner: Arc<dyn MetaStore>,
        fail_batches: bool,
        singles: AtomicUsize,
        batches: AtomicUsize,
    }

    #[async_trait::async_trait]
    impl MetaStore for Probe {
        async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
            self.inner.put_changeset(cs).await
        }
        async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
            self.singles.fetch_add(1, Ordering::Relaxed);
            self.inner.get_changeset(id).await
        }
        async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
            self.batches.fetch_add(1, Ordering::Relaxed);
            if self.fail_batches {
                return Err(Error::Storage("no batches".into()));
            }
            self.inner.get_changesets(ids).await
        }
        async fn list_changesets(
            &self,
            after: Option<ChangesetId>,
            limit: usize,
        ) -> Result<Vec<ChangesetId>> {
            self.inner.list_changesets(after, limit).await
        }
        async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
            self.inner.get_ref(name).await
        }
        async fn cas_ref_logged(
            &self,
            name: &str,
            expected: Option<ChangesetId>,
            new: Option<ChangesetId>,
            ctx: &RefUpdateContext,
        ) -> Result<bool> {
            self.inner.cas_ref_logged(name, expected, new, ctx).await
        }
        async fn ref_log(
            &self,
            name: Option<&str>,
            before_seq: Option<u64>,
            limit: usize,
        ) -> Result<Vec<RefLogEntry>> {
            self.inner.ref_log(name, before_seq, limit).await
        }
        async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
            self.inner.list_refs().await
        }
        async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
            self.singles.fetch_add(1, Ordering::Relaxed);
            self.inner.get_derived(kind, key).await
        }
        async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
            self.batches.fetch_add(1, Ordering::Relaxed);
            if self.fail_batches {
                return Err(Error::Storage("no batches".into()));
            }
            self.inner.get_derived_many(kind, keys).await
        }
        async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
            self.inner.put_derived(kind, key, value).await
        }
        async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
            self.inner.delete_derived_kind(kind).await
        }
    }

    fn probe(fail_batches: bool) -> (Repo, Arc<Probe>) {
        let p = Arc::new(Probe {
            inner: Arc::new(MemoryMetaStore::new()),
            fail_batches,
            singles: AtomicUsize::new(0),
            batches: AtomicUsize::new(0),
        });
        (Repo::new(Arc::new(MemoryBlobStore::new()), p.clone()), p)
    }

    struct Lcg(u64);

    impl Lcg {
        fn below(&mut self, n: usize) -> usize {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            ((self.0 >> 33) as usize) % n
        }
    }

    /// A random DAG: a main line, side branches off recent changesets,
    /// merges of two or three parents and, early on, a few extra roots. The
    /// last id is the main line's head (it also appears earlier).
    async fn random_dag(r: &Repo, rng: &mut Lcg, n: usize) -> Vec<ChangesetId> {
        let mut ids: Vec<ChangesetId> = Vec::new();
        let mut main: Option<ChangesetId> = None;
        for i in 0..n {
            let len = ids.len();
            let (parents, on_main) = match (main, rng.below(100)) {
                (None, _) => (vec![], true),
                (Some(_), 0..=2) if len < n / 10 => (vec![], false),
                (Some(m), 3..=27) => {
                    let mut ps = vec![m];
                    for _ in 0..1 + rng.below(2) {
                        let p = ids[len - 1 - rng.below(len.min(30))];
                        if !ps.contains(&p) {
                            ps.push(p);
                        }
                    }
                    (ps, true)
                }
                (Some(_), 28..=60) => (vec![ids[len - 1 - rng.below(len.min(30))]], false),
                (Some(m), _) => (vec![m], true),
            };
            let data = format!("{i}");
            let content = r.blobs().put(Bytes::from(data.clone())).await.unwrap();
            let changes = BTreeMap::from([(
                RepoPath::new(format!("d{}/f", rng.below(4))).unwrap(),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size: data.len() as u64,
                },
            )]);
            let mut extra = BTreeMap::new();
            if rng.below(3) == 0 {
                extra.insert(EXTRA_AGENT_MODEL.to_string(), format!("m{}", rng.below(2)));
            }
            let cs = Changeset {
                parents,
                author: ["ann", "bob", "cy"][rng.below(3)].into(),
                timestamp: i as i64,
                message: format!("c{i}"),
                changes,
                extra,
            };
            let id = r.create_changeset(cs).await.unwrap();
            if on_main {
                main = Some(id);
            }
            ids.push(id);
        }
        ids.push(main.expect("n > 0"));
        ids
    }

    /// The walk as it was before reading ahead: one changeset, then each new
    /// parent's generation, one at a time.
    async fn serial_log(
        r: &Repo,
        id: ChangesetId,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        let mut seen = HashSet::from([id]);
        let mut heap = BinaryHeap::from([(r.generation(id).await?, id)]);
        let mut out = Vec::new();
        while out.len() < limit {
            let Some((generation, cid)) = heap.pop() else {
                break;
            };
            let cs = r.get_changeset(cid).await?;
            for &p in &cs.parents {
                if seen.insert(p) {
                    heap.push((r.generation(p).await?, p));
                }
            }
            if !filter.matches(&cs) {
                continue;
            }
            out.push(ChangesetInfo {
                id: cid,
                root_tree: r.root_tree(cid).await?,
                changeset: cs,
                generation,
                path_at: None,
                renamed: None,
            });
        }
        Ok(out)
    }

    fn filters() -> Vec<LogFilter> {
        let extra = |s: &str| LogFilter {
            author: None,
            extra: vec![ExtraMatch::parse(s).unwrap()],
        };
        vec![
            LogFilter::default(),
            LogFilter {
                author: Some("BOB".into()),
                extra: vec![],
            },
            extra(EXTRA_AGENT_MODEL),
            extra(&format!("{EXTRA_AGENT_MODEL}:m1")),
            LogFilter {
                author: Some("nobody".into()),
                extra: vec![],
            },
        ]
    }

    #[tokio::test]
    async fn batched_log_matches_the_serial_walk_on_random_dags() {
        for seed in 1..=8u64 {
            let mut rng = Lcg(seed);
            let (r, _) = probe(seed % 4 == 0);
            let n = 40 + rng.below(260);
            let ids = random_dag(&r, &mut rng, n).await;
            if seed % 3 == 0 {
                // Rows the read-ahead misses are derived again on demand.
                r.meta().delete_derived_kind("root_tree").await.unwrap();
                r.meta().delete_derived_kind("skiplist").await.unwrap();
            }
            let mut tips = vec![*ids.last().unwrap()];
            for _ in 0..3 {
                tips.push(ids[rng.below(ids.len())]);
            }
            for &tip in &tips {
                for filter in filters() {
                    for limit in [0, 1, 2, 7, 40, 255, 256, 257, usize::MAX] {
                        let want = serial_log(&r, tip, limit, &filter).await.unwrap();
                        let got = r.log_filtered(tip, None, limit, &filter).await.unwrap();
                        assert_eq!(
                            got, want,
                            "seed {seed}, tip {tip}, limit {limit}, {filter:?}"
                        );
                    }
                }
                // Every changeset the walk can reach, in the serial order,
                // whatever it is told to expect.
                let all = serial_log(&r, tip, usize::MAX, &LogFilter::default())
                    .await
                    .unwrap();
                for want in [1, 3, 64, usize::MAX] {
                    let mut walk = Walk::start(&r, tip, want % 2 == 0, want).await.unwrap();
                    let mut order = Vec::new();
                    while let Some(s) = walk.next(&r, want).await.unwrap() {
                        order.push((s.generation, s.id));
                    }
                    let serial: Vec<_> = all.iter().map(|i| (i.generation, i.id)).collect();
                    assert_eq!(order, serial, "seed {seed}, tip {tip}, want {want}");
                }
            }
        }
    }

    #[tokio::test]
    async fn provenance_is_unchanged_by_reading_ahead() {
        let mut rng = Lcg(42);
        let (r, _) = probe(false);
        let ids = random_dag(&r, &mut rng, 150).await;
        let (plain, _) = probe(true);
        let plain_ids = random_dag(&plain, &mut Lcg(42), 150).await;
        assert_eq!(ids, plain_ids);
        let tip = *ids.last().unwrap();
        for limit in [0, 1, 10, 149, 1000] {
            for prefix in [None, Some("d1")] {
                assert_eq!(
                    r.provenance(tip, prefix, limit).await.unwrap(),
                    plain.provenance(tip, prefix, limit).await.unwrap(),
                    "limit {limit}, prefix {prefix:?}"
                );
            }
        }
    }

    #[tokio::test]
    async fn missing_changesets_fail_as_before() {
        let missing = ChangesetId(Hash::of_blob(b"not stored"));
        let (r, _) = probe(false);
        for limit in [0, 1, 10] {
            let err = r.log(missing, None, limit).await.unwrap_err();
            let want = serial_log(&r, missing, limit, &LogFilter::default())
                .await
                .unwrap_err();
            assert_eq!(err.to_string(), want.to_string());
        }
    }

    #[tokio::test]
    async fn a_long_log_reads_in_batches() {
        let (r, p) = probe(false);
        let mut rng = Lcg(7);
        let ids = random_dag(&r, &mut rng, 2000).await;
        let tip = *ids.last().unwrap();
        let reachable = serial_log(&r, tip, usize::MAX, &LogFilter::default())
            .await
            .unwrap()
            .len();
        let (singles, batches) = (
            p.singles.load(Ordering::Relaxed),
            p.batches.load(Ordering::Relaxed),
        );
        let log = r.log(tip, None, usize::MAX).await.unwrap();
        assert_eq!(log.len(), reachable);
        let singles = p.singles.load(Ordering::Relaxed) - singles;
        let batches = p.batches.load(Ordering::Relaxed) - batches;
        assert_eq!(singles, 0, "everything came from batches");
        assert!(
            batches * 20 < reachable,
            "{batches} batched reads for {reachable} changesets"
        );
    }
}
