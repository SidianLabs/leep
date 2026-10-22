//! Queries that never derive (ADR 0058) against the fully indexed answers.
//!
//! Every history is built twice, with the same ids: once fully indexed (the
//! reference) and once with a random parent-closed subset of index rows,
//! made by deriving the indexes of random changesets (each brings its
//! ancestors). Queries on the second store, bounded by a live index lease,
//! must give the reference's entries, order, `truncated` and `next`
//! presence, page by page; answer [`Error::Indexing`] exactly when the
//! un-indexed part exceeds the budget; and write no index rows.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    Changeset, ChangesetId, Error, FileChange, FileKind, LeaseClaim, LeaseKey, LeaseTiming,
    MetaStore, RepoPath,
};

use crate::derived::{self, DerivedKind, HistoryTree, LogFilterIndex};
use crate::indexing::{
    Budgets, Freshness, IndexGroup, IndexLeaseConfig, IndexLeases, IndexMode, IndexPolicy,
};
use crate::search::SearchQuery;
use crate::{LogFilter, LogParams, Repo};

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: usize) -> usize {
        self.0 = self
            .0
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        ((self.0 >> 33) as usize) % n.max(1)
    }
}

/// Paths a history picks from; `k` and `k/z` change kind, `d/e` nests.
const POOL: &[&str] = &[
    "a", "b", "c.txt", "d/e/f", "d/e/g", "d/h", "k", "k/z", "m/n", "m/o", "p",
];
const AUTHORS: &[&str] = &["alice", "bob", "carol"];

/// A file of a few lines from a small pool, so edits keep and move lines.
fn text(rng: &mut Rng) -> String {
    let n = 1 + rng.below(5);
    (0..n).map(|_| format!("line {}\n", rng.below(8))).collect()
}

fn memory_repo() -> (Repo, Arc<MemoryMetaStore>) {
    // A frozen lease clock: a lease taken once stays live.
    let meta = Arc::new(MemoryMetaStore::new().with_lease_clock(Arc::new(|| 1_000)));
    (
        Repo::new(Arc::new(MemoryBlobStore::new()), meta.clone()).with_derive_jobs(2),
        meta,
    )
}

/// Build the history of `seed`: the same changesets in any repo.
async fn build(r: &Repo, seed: u64, n: usize) -> Vec<ChangesetId> {
    let mut rng = Rng(seed);
    let mut all: Vec<ChangesetId> = Vec::new();
    let mut files: HashMap<ChangesetId, BTreeMap<String, String>> = HashMap::new();
    for step in 0..n {
        let parents: Vec<ChangesetId> = match all.len() {
            0 => vec![],
            len => {
                let first = all[len - 1 - rng.below(len.min(4))];
                match rng.below(8) {
                    0 | 1 if len > 2 => {
                        let mut p = vec![first];
                        for _ in 0..1 + usize::from(rng.below(6) == 0) {
                            let other = all[rng.below(len)];
                            if !p.contains(&other) {
                                p.push(other);
                            }
                        }
                        p
                    }
                    _ => vec![first],
                }
            }
        };
        let mut state = parents
            .first()
            .map(|p| files[p].clone())
            .unwrap_or_default();
        let base = state.clone();
        let mut changes: BTreeMap<RepoPath, FileChange> = BTreeMap::new();
        let ops = if parents.len() > 1 {
            rng.below(3)
        } else {
            1 + rng.below(3)
        };
        for _ in 0..ops {
            let existing: Vec<String> = state.keys().cloned().collect();
            match rng.below(7) {
                // Rename: same or nearly the same content under a new name.
                0 if !existing.is_empty() => {
                    let from = existing[rng.below(existing.len())].clone();
                    let to = format!("{}{}", POOL[rng.below(POOL.len())], step);
                    if state.keys().any(|k| k.starts_with(&format!("{to}/"))) {
                        continue;
                    }
                    let mut content = state.remove(&from).expect("listed");
                    if rng.below(3) == 0 {
                        content.push_str("renamed\n");
                    }
                    delete(&mut changes, &base, &from);
                    put(r, &mut changes, &mut state, &base, &to, content).await;
                }
                1 if !existing.is_empty() => {
                    let gone = existing[rng.below(existing.len())].clone();
                    state.remove(&gone);
                    delete(&mut changes, &base, &gone);
                }
                // A no-op edit: the content already there.
                2 if !existing.is_empty() => {
                    let same = existing[rng.below(existing.len())].clone();
                    let content = state[&same].clone();
                    put(r, &mut changes, &mut state, &base, &same, content).await;
                }
                _ => {
                    let path = POOL[rng.below(POOL.len())].to_string();
                    let content = text(&mut rng);
                    put(r, &mut changes, &mut state, &base, &path, content).await;
                }
            }
        }
        let mut extra = BTreeMap::new();
        if rng.below(3) == 0 {
            extra.insert("agent.model".to_string(), format!("m{}", rng.below(2)));
        }
        let cs = Changeset {
            parents: parents.clone(),
            author: AUTHORS[rng.below(AUTHORS.len())].into(),
            timestamp: step as i64,
            message: format!("c{step}"),
            changes,
            extra,
        };
        let id = r.create_changeset(cs).await.unwrap();
        files.insert(id, state);
        all.push(id);
    }
    all
}

/// Delete `path` in this changeset: a delete if the parent has it, else
/// undo its addition.
fn delete(
    changes: &mut BTreeMap<RepoPath, FileChange>,
    base: &BTreeMap<String, String>,
    path: &str,
) {
    let p = RepoPath::new(path).unwrap();
    if base.contains_key(path) {
        changes.insert(p, FileChange::Delete);
    } else {
        changes.remove(&p);
    }
}

/// Write `content` at `path`, deleting whatever stands in the way: a file
/// where a directory goes, or the files below where a file goes.
async fn put(
    r: &Repo,
    changes: &mut BTreeMap<RepoPath, FileChange>,
    state: &mut BTreeMap<String, String>,
    base: &BTreeMap<String, String>,
    path: &str,
    content: String,
) {
    let below: Vec<String> = state
        .keys()
        .filter(|k| k.starts_with(&format!("{path}/")))
        .cloned()
        .collect();
    for k in below {
        state.remove(&k);
        delete(changes, base, &k);
    }
    let mut prefix = String::new();
    for comp in path.split('/').collect::<Vec<_>>().split_last().unwrap().1 {
        if !prefix.is_empty() {
            prefix.push('/');
        }
        prefix.push_str(comp);
        if state.remove(&prefix).is_some() {
            delete(changes, base, &prefix);
        }
    }
    let hash = r.blobs().put(Bytes::from(content.clone())).await.unwrap();
    changes.insert(
        RepoPath::new(path).unwrap(),
        FileChange::Modify {
            content: hash,
            kind: FileKind::Regular,
            size: content.len() as u64,
        },
    );
    state.insert(path.to_string(), content);
}

/// Take every index lease for a holder that never renews: the frozen lease
/// clock keeps them live.
async fn hold_leases(meta: &dyn MetaStore) {
    let timing = LeaseTiming {
        ttl: Duration::from_secs(15),
        stale_after: Duration::from_secs(6),
    };
    for g in IndexGroup::ALL {
        let key = LeaseKey::index(g.lease_name());
        let l = meta
            .acquire_lease(
                &LeaseClaim {
                    key: &key,
                    holder: "indexer",
                    address: "",
                    protocol: 1,
                    meta: "{\"rate\":50.0,\"backlog\":0}",
                },
                timing,
            )
            .await
            .unwrap();
        assert_eq!(l.holder, "indexer");
    }
}

fn bounded(r: &Repo, budgets: Budgets) -> Repo {
    bounded_scanning(r, budgets, crate::filter_log::SCAN_FIRST)
}

/// [`bounded`], scanning `scan_first` changesets of a filtered log before
/// its index.
fn bounded_scanning(r: &Repo, budgets: Budgets, scan_first: usize) -> Repo {
    let leases = IndexLeases::new(r.meta().clone(), IndexLeaseConfig::new("query", ""));
    let policy = IndexPolicy::new(IndexMode::Off, leases)
        .with_budgets(budgets)
        .with_index_wait(Duration::ZERO, 4)
        .with_scan_first(scan_first);
    r.clone().with_index_policy(Arc::new(policy))
}

/// Index rows of every index kind, to tell that queries wrote none.
async fn index_rows(meta: &dyn MetaStore) -> usize {
    let mut n = 0;
    for kind in [
        HistoryTree::NAME,
        derived::HISTORY_NODE_KIND,
        LogFilterIndex::NAME,
        derived::LOG_FILTER_NAME_KIND,
        crate::search::TREE_KIND,
        crate::search::BLOB_KIND,
    ] {
        n += meta
            .list_derived(kind, None, usize::MAX)
            .await
            .unwrap()
            .len();
    }
    n
}

/// Changesets reachable from `starts` without a stored row of `kind`,
/// counted by a plain walk.
async fn missing(r: &Repo, kind: &str, starts: &[ChangesetId]) -> usize {
    let mut seen: HashSet<ChangesetId> = starts.iter().copied().collect();
    let mut todo: Vec<ChangesetId> = starts.to_vec();
    let mut n = 0;
    while let Some(id) = todo.pop() {
        if r.meta().get_derived(kind, &id.0).await.unwrap().is_some() {
            continue;
        }
        n += 1;
        for p in r.get_changeset(id).await.unwrap().parents {
            if seen.insert(p) {
                todo.push(p);
            }
        }
    }
    n
}

/// The newest first-parent ancestor of `id` with a row of `kind`, and the
/// first-parent ancestors above it without one.
async fn newest_with_row(r: &Repo, kind: &str, id: ChangesetId) -> (Option<ChangesetId>, usize) {
    let mut x = Some(id);
    let mut steps = 0;
    while let Some(c) = x {
        if r.meta().get_derived(kind, &c.0).await.unwrap().is_some() {
            return (Some(c), steps);
        }
        steps += 1;
        x = r.get_changeset(c).await.unwrap().parents.first().copied();
    }
    (None, steps)
}

/// Pages of a log as (entry ids, whether `next` was given).
type Pages = Vec<(Vec<ChangesetId>, bool)>;

async fn reference_pages(r: &Repo, id: ChangesetId, params: &LogParams, limit: usize) -> Pages {
    let mut out = Vec::new();
    let mut after: Option<String> = None;
    loop {
        let page = r
            .log_page(id, params, limit, after.as_deref())
            .await
            .unwrap();
        out.push((
            page.entries.iter().map(|e| e.id).collect(),
            page.next.is_some(),
        ));
        match page.next {
            Some(n) if out.len() < 200 => after = Some(n),
            _ => return out,
        }
    }
}

/// A partially indexed copy: rows for the ancestors of a few random
/// changesets, per index kind independently.
async fn partial(seed: u64, all_ids: &[ChangesetId], n: usize) -> (Repo, Arc<MemoryMetaStore>) {
    let (b, meta) = memory_repo();
    let ids = build(&b, seed, n).await;
    assert_eq!(ids, all_ids);
    let mut rng = Rng(seed ^ 0xb0b);
    for _ in 0..rng.below(3) {
        let c = ids[rng.below(ids.len())];
        derived::derive::<HistoryTree>(&b, c).await.unwrap();
    }
    for _ in 0..rng.below(3) {
        let c = ids[rng.below(ids.len())];
        derived::derive::<LogFilterIndex>(&b, c).await.unwrap();
    }
    for _ in 0..rng.below(3) {
        let c = ids[rng.below(ids.len())];
        b.derive_search(c).await.unwrap();
    }
    (b, meta)
}

async fn full(seed: u64, n: usize) -> (Repo, Vec<ChangesetId>) {
    let (a, _) = memory_repo();
    let ids = build(&a, seed, n).await;
    for &c in &ids {
        derived::derive::<HistoryTree>(&a, c).await.unwrap();
        derived::derive::<LogFilterIndex>(&a, c).await.unwrap();
        a.derive_search(c).await.unwrap();
    }
    (a, ids)
}

fn budgets(rng: &mut Rng) -> Budgets {
    Budgets {
        scan: [0, 2, 6, 15, 10_000][rng.below(5)],
        blame: [0, 1, 3, 10_000][rng.below(4)],
        search_files: [0, 2, 10_000][rng.below(3)],
        search_bytes: 1 << 30,
        legacy_factor: 8,
    }
}

/// Log pages, last-change and blame on random histories with random
/// budgets: equal to the reference within budget, `Indexing` exactly over
/// it, stale answers equal to the reference at `answered_at`, and no index
/// row written.
#[tokio::test]
async fn bounded_history_queries_match_the_indexed_answers() {
    for seed in 1..=8u64 {
        let n = 60;
        let (a, ids) = full(seed, n).await;
        let (b, meta) = partial(seed, &ids, n).await;
        hold_leases(meta.as_ref()).await;
        let mut rng = Rng(seed * 7919);
        let rows_before = index_rows(meta.as_ref()).await;
        let mut compared = 0;
        let mut over = 0;
        let mut hybrid = 0;
        for _ in 0..10 {
            let tip = ids[ids.len() - 1 - rng.below(ids.len().min(20))];
            let q = bounded(&b, budgets(&mut rng));
            let budget = q.index_policy().unwrap().budgets;
            let path = POOL[rng.below(POOL.len())];
            let dir = ["d", "d/e", "m", "k"][rng.below(4)];
            for p in [path, dir] {
                let params = LogParams {
                    path: Some(p.into()),
                    ..Default::default()
                };
                let limit = 1 + rng.below(4);
                let want = reference_pages(&a, tip, &params, limit).await;
                let gap = missing(&b, HistoryTree::NAME, &[tip]).await;
                let mut after: Option<String> = None;
                let mut got: Pages = Vec::new();
                loop {
                    match q
                        .log_page_indexed(tip, &params, limit, after.as_deref(), Freshness::Retry)
                        .await
                    {
                        Ok(page) => {
                            assert_eq!(page.answered_at, None);
                            hybrid += usize::from(page.scanned > 0);
                            got.push((
                                page.value.entries.iter().map(|e| e.id).collect(),
                                page.value.next.is_some(),
                            ));
                            match page.value.next {
                                Some(n) => after = Some(n),
                                None => break,
                            }
                        }
                        Err(Error::Indexing { kind, .. }) => {
                            assert_eq!(kind, HistoryTree::NAME);
                            assert!(got.is_empty() || after.is_some());
                            break;
                        }
                        Err(e) => panic!("seed {seed}: {e}"),
                    }
                }
                if gap > budget.scan {
                    assert!(
                        got.is_empty(),
                        "seed {seed}: {p} at {tip}: answered over budget"
                    );
                    over += 1;
                    // stale-ok: the newest indexed first-parent ancestor.
                    let (at, _) = newest_with_row(&b, HistoryTree::NAME, tip).await;
                    match q
                        .log_page_indexed(tip, &params, limit, None, Freshness::StaleOk)
                        .await
                    {
                        Ok(page) => {
                            let at = at.expect("an indexed ancestor");
                            assert_eq!(page.answered_at, Some(at));
                            let behind =
                                b.generation(tip).await.unwrap() - b.generation(at).await.unwrap();
                            assert_eq!(page.behind, Some(behind));
                            let want_at = reference_pages(&a, at, &params, limit).await;
                            assert_eq!(
                                page.value.entries.iter().map(|e| e.id).collect::<Vec<_>>(),
                                want_at[0].0
                            );
                            assert_eq!(page.value.next.is_some(), want_at[0].1);
                        }
                        Err(Error::Indexing { .. }) => assert!(at.is_none()),
                        Err(e) => panic!("{e}"),
                    }
                } else {
                    assert_eq!(got, want, "seed {seed}: {p} at {tip}, limit {limit}");
                    compared += 1;
                }
                // last-change
                match (
                    a.last_change(tip, p).await,
                    q.last_change_indexed(tip, p, Freshness::Retry).await,
                ) {
                    (Ok(x), Ok(y)) => {
                        assert!(gap <= budget.scan);
                        let mut c1 = x.1.clone();
                        let mut c2 = y.value.1.clone();
                        c1.sort();
                        c2.sort();
                        assert_eq!((x.0.id, c1), (y.value.0.id, c2), "seed {seed}: {p}");
                    }
                    (Err(Error::NotFound(_)), Err(Error::NotFound(_))) => {}
                    (_, Err(Error::Indexing { .. })) => assert!(gap > budget.scan),
                    (x, y) => panic!("seed {seed}: {p} at {tip}: {x:?} vs {y:?}"),
                }
            }
            // blame of every file at the tip
            let (_, steps) = newest_with_row(&b, HistoryTree::NAME, tip).await;
            for f in a.list_files(tip, "").await.unwrap() {
                let want = a.blame(tip, &f.path).await.unwrap();
                match q.blame_indexed(tip, &f.path, Freshness::Retry).await {
                    Ok(got) => {
                        assert!(steps <= budget.blame);
                        assert_eq!(got.value.lines, want.lines, "seed {seed}: {}", f.path);
                    }
                    Err(Error::Indexing { .. }) => assert!(steps > budget.blame),
                    Err(e) => panic!("{e}"),
                }
            }
        }
        assert_eq!(
            index_rows(meta.as_ref()).await,
            rows_before,
            "seed {seed}: a bounded query wrote index rows"
        );
        assert!(compared > 0, "seed {seed} compared nothing");
        eprintln!(
            "seed {seed}: compared {compared}, over budget {over}, pages with a scan {hybrid}"
        );
        HYBRID.fetch_add(hybrid, std::sync::atomic::Ordering::Relaxed);
    }
}

static HYBRID: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// `log --follow` across renames, page by page.
#[tokio::test]
async fn bounded_follow_matches_the_indexed_answers() {
    for seed in 20..=27u64 {
        let n = 50;
        let (a, ids) = full(seed, n).await;
        let (b, meta) = partial(seed, &ids, n).await;
        hold_leases(meta.as_ref()).await;
        let q = bounded(
            &b,
            Budgets {
                scan: 10_000,
                ..Budgets::default()
            },
        );
        let mut rng = Rng(seed);
        let mut scanned = 0;
        for _ in 0..4 {
            let tip = ids[ids.len() - 1 - rng.below(10)];
            for f in a.list_files(tip, "").await.unwrap() {
                let params = LogParams {
                    path: Some(f.path.as_str().into()),
                    follow: true,
                    ..Default::default()
                };
                let limit = 1 + rng.below(3);
                let want = reference_pages(&a, tip, &params, limit).await;
                let mut got: Pages = Vec::new();
                let mut after: Option<String> = None;
                loop {
                    let answered = q
                        .log_page_indexed(tip, &params, limit, after.as_deref(), Freshness::Retry)
                        .await
                        .unwrap();
                    scanned += answered.scanned;
                    let page = answered.value;
                    got.push((
                        page.entries.iter().map(|e| e.id).collect(),
                        page.next.is_some(),
                    ));
                    match page.next {
                        Some(n) => after = Some(n),
                        None => break,
                    }
                }
                assert_eq!(got, want, "seed {seed}: follow {} at {tip}", f.path);
            }
        }
        eprintln!("follow seed {seed}: scanned {scanned}");
    }
}

/// Filtered logs (author, `extra`) without a path, on the log-filter index.
#[tokio::test]
async fn bounded_filtered_logs_match_the_indexed_answers() {
    for seed in 40..=47u64 {
        let n = 70;
        let (a, ids) = full(seed, n).await;
        let (b, meta) = partial(seed, &ids, n).await;
        hold_leases(meta.as_ref()).await;
        let mut rng = Rng(seed);
        let filters = [
            LogFilter {
                author: Some("alice".into()),
                ..Default::default()
            },
            LogFilter {
                author: Some("o".into()),
                ..Default::default()
            },
            LogFilter {
                extra: vec![crate::ExtraMatch::parse("agent.model:m1").unwrap()],
                ..Default::default()
            },
            LogFilter {
                author: Some("carol".into()),
                extra: vec![crate::ExtraMatch::parse("agent.model").unwrap()],
            },
        ];
        for _ in 0..6 {
            let tip = ids[ids.len() - 1 - rng.below(15)];
            let scan_first = [0, 1, 4, 512][rng.below(4)];
            let q = bounded_scanning(&b, budgets(&mut rng), scan_first);
            let budget = q.index_policy().unwrap().budgets;
            for filter in &filters {
                let params = LogParams {
                    filter: filter.clone(),
                    ..Default::default()
                };
                let limit = 1 + rng.below(4);
                let want = reference_pages(&a, tip, &params, limit).await;
                let gap = missing(&b, LogFilterIndex::NAME, &[tip]).await;
                let mut got: Pages = Vec::new();
                let mut after: Option<String> = None;
                let mut refused = false;
                loop {
                    match q
                        .log_page_indexed(tip, &params, limit, after.as_deref(), Freshness::Retry)
                        .await
                    {
                        Ok(page) => {
                            if page.scanned > 0 {
                                eprintln!("filtered seed {seed}: scanned {}", page.scanned);
                            }
                            let page = page.value;
                            got.push((
                                page.entries.iter().map(|e| e.id).collect(),
                                page.next.is_some(),
                            ));
                            match page.next {
                                Some(n) => after = Some(n),
                                None => break,
                            }
                        }
                        Err(Error::Indexing { .. }) => {
                            refused = true;
                            break;
                        }
                        Err(e) => panic!("{e}"),
                    }
                }
                if gap > budget.scan {
                    assert!(
                        refused && got.is_empty(),
                        "seed {seed}: over budget answered"
                    );
                } else if !refused {
                    assert_eq!(got, want, "seed {seed}: {filter:?} at {tip}, limit {limit}");
                }
            }
        }
    }
}

/// A plain scan and the index-backed walk give the same pages, `next`
/// included: a filtered page carries `next` exactly when another match
/// follows.
#[tokio::test]
async fn filtered_pages_carry_next_only_before_another_match() {
    let (a, ids) = full(99, 60).await;
    let tip = *ids.last().unwrap();
    let filter = LogFilter {
        author: Some("bob".into()),
        ..Default::default()
    };
    let all = a.log_scan(tip, None, usize::MAX, &filter).await.unwrap();
    let params = LogParams {
        filter,
        ..Default::default()
    };
    let pages = reference_pages(&a, tip, &params, 1).await;
    assert_eq!(pages.len(), all.len().max(1));
    for (i, (entries, next)) in pages.iter().enumerate() {
        assert_eq!(entries.len(), usize::from(!all.is_empty()));
        assert_eq!(*next, i + 1 < all.len(), "page {i}");
    }
}

/// Code search: matches, `truncated`, `next` and the stats the ADR keeps
/// are the indexed answer's; `files_unindexed` counts what was fetched to
/// stand in for missing rows.
#[tokio::test]
async fn bounded_search_matches_the_indexed_answers() {
    for seed in 60..=67u64 {
        let n = 40;
        let (a, ids) = full(seed, n).await;
        let (b, meta) = partial(seed, &ids, n).await;
        hold_leases(meta.as_ref()).await;
        let rows_before = index_rows(meta.as_ref()).await;
        let mut rng = Rng(seed);
        for _ in 0..8 {
            let tip = ids[ids.len() - 1 - rng.below(10)];
            let q = bounded(&b, budgets(&mut rng));
            let rev = tip.to_string();
            let pattern = format!("line {}", rng.below(8));
            for (path, limit) in [("", 3), ("d", 2), ("a", 1), ("", 1000)] {
                let mut query = SearchQuery::new(pattern.clone());
                query.path = path.into();
                query.limit = limit;
                let want = a.search_page(&rev, &query, None).await.unwrap();
                match q
                    .search_page_indexed(&rev, &query, None, Freshness::Retry)
                    .await
                {
                    Ok(got) => {
                        if got.scanned > 0 {
                            eprintln!("search seed {seed}: unindexed files {}", got.scanned);
                        }
                        let got = got.value;
                        assert_eq!(
                            got.matches, want.matches,
                            "seed {seed}: {pattern} in {path:?}"
                        );
                        assert_eq!(got.truncated, want.truncated);
                        assert_eq!(got.next.is_some(), want.next.is_some());
                        assert_eq!(got.stats.files_considered, want.stats.files_considered);
                        assert_eq!(got.stats.files_skipped, want.stats.files_skipped);
                        assert_eq!(got.stats.indexed, want.stats.indexed);
                        assert_eq!(
                            (got.stats.blob_rows_derived, got.stats.tree_rows_derived),
                            (0, 0)
                        );
                        if let Some(next) = got.next {
                            let page2 = q
                                .search_page_indexed(&rev, &query, Some(&next), Freshness::Retry)
                                .await;
                            let want2 = a
                                .search_page(&rev, &query, want.next.as_deref())
                                .await
                                .unwrap();
                            if let Ok(p2) = page2 {
                                assert_eq!(p2.value.matches, want2.matches);
                            }
                        }
                    }
                    Err(Error::Indexing { kind, .. }) => {
                        assert_eq!(kind, crate::search::TREE_KIND);
                    }
                    Err(e) => panic!("{e}"),
                }
            }
        }
        assert_eq!(index_rows(meta.as_ref()).await, rows_before);
    }
}

/// A bounded search computes missing directory filters into its own
/// `SearchScan`; only filters read from the store reach the shared cache,
/// so a computed row never outlives its query.
#[tokio::test]
async fn bounded_search_caches_only_stored_filters() {
    let mut unstored_roots = 0;
    for seed in 60..=67u64 {
        let n = 40;
        let (a, ids) = full(seed, n).await;
        let (b, meta) = partial(seed, &ids, n).await;
        hold_leases(meta.as_ref()).await;
        let q = bounded(
            &b,
            Budgets {
                search_files: 10_000,
                ..Budgets::default()
            },
        );
        for &tip in ids.iter().rev().take(10) {
            let root = a.root_tree(tip).await.unwrap();
            let stored = meta
                .get_derived(crate::search::TREE_KIND, &root)
                .await
                .unwrap()
                .is_some();
            let got = q
                .search_page_indexed(
                    &tip.to_string(),
                    &SearchQuery::new("line 3"),
                    None,
                    Freshness::Retry,
                )
                .await
                .unwrap();
            assert_eq!(got.value.stats.tree_rows_derived, 0);
            if !stored {
                unstored_roots += 1;
                assert!(b.search_trees.entries().iter().all(|e| e.0 != root));
            }
        }
        for (h, row, _) in b.search_trees.entries() {
            let stored = meta
                .get_derived(crate::search::TREE_KIND, &h)
                .await
                .unwrap();
            assert_eq!(stored.as_ref(), Some(&*row), "seed {seed}");
        }
    }
    assert!(
        unstored_roots > 0,
        "some searches computed their root's filter"
    );
}

/// Pages concatenate to one walk while the index grows between pages, and
/// after a `repair drop` of the kind between pages.
#[tokio::test]
async fn pages_continue_across_index_changes() {
    for seed in 80..=85u64 {
        let n = 60;
        let (a, ids) = full(seed, n).await;
        let (b, meta) = partial(seed, &ids, n).await;
        hold_leases(meta.as_ref()).await;
        let q = bounded_scanning(
            &b,
            Budgets {
                scan: 10_000,
                ..Budgets::default()
            },
            2,
        );
        let mut rng = Rng(seed);
        let tip = *ids.last().unwrap();
        for p in ["d", "a", "m", "k"] {
            for filter in [
                LogFilter::default(),
                LogFilter {
                    author: Some("bob".into()),
                    ..Default::default()
                },
            ] {
                let params = LogParams {
                    path: filter.author.is_none().then(|| p.to_string()),
                    filter,
                    ..Default::default()
                };
                let want: Vec<ChangesetId> = reference_pages(&a, tip, &params, 1000)
                    .await
                    .into_iter()
                    .flat_map(|(e, _)| e)
                    .collect();
                let mut got = Vec::new();
                let mut after: Option<String> = None;
                let mut page_no = 0;
                loop {
                    let page = q
                        .log_page_indexed(tip, &params, 2, after.as_deref(), Freshness::Retry)
                        .await
                        .unwrap()
                        .value;
                    got.extend(page.entries.iter().map(|e| e.id));
                    // Between pages the index grows, or is dropped.
                    match (page_no % 3, rng.below(2)) {
                        (0, _) => {
                            let c = ids[rng.below(ids.len())];
                            derived::derive::<HistoryTree>(&b, c).await.unwrap();
                            derived::derive::<LogFilterIndex>(&b, c).await.unwrap();
                        }
                        (1, 0) => {
                            for k in [
                                HistoryTree::NAME,
                                derived::HISTORY_NODE_KIND,
                                LogFilterIndex::NAME,
                                derived::LOG_FILTER_NAME_KIND,
                            ] {
                                meta.delete_derived_kind(k).await.unwrap();
                            }
                        }
                        _ => {}
                    }
                    page_no += 1;
                    match page.next {
                        Some(n) => after = Some(n),
                        None => break,
                    }
                }
                assert_eq!(got, want, "seed {seed}: {params:?}");
            }
        }
    }
}

/// Without a live lease, queries derive inline as in `local` mode; with
/// one, a request without the header waits, then scans with the larger
/// budget, and only then answers `Indexing`.
#[tokio::test]
async fn no_lease_derives_inline_and_old_clients_get_larger_budgets() {
    let n = 40;
    let (a, ids) = full(5, n).await;
    let (b, meta) = memory_repo();
    assert_eq!(build(&b, 5, n).await, ids);
    let tip = *ids.last().unwrap();
    let params = LogParams {
        path: Some("d".into()),
        ..Default::default()
    };
    let want = reference_pages(&a, tip, &params, 1000).await;
    let q = bounded(
        &b,
        Budgets {
            scan: 1,
            ..Budgets::default()
        },
    );
    // No lease: inline derivation, rows written.
    let page = q
        .log_page_indexed(tip, &params, 1000, None, Freshness::Retry)
        .await
        .unwrap();
    assert_eq!(
        page.value.entries.iter().map(|e| e.id).collect::<Vec<_>>(),
        want[0].0
    );
    assert!(b
        .meta()
        .get_derived(HistoryTree::NAME, &tip.0)
        .await
        .unwrap()
        .is_some());

    // A live lease and a gap of `gap`: retry refuses, legacy scans 8x.
    let (b, meta2) = memory_repo();
    assert_eq!(build(&b, 5, n).await, ids);
    derived::derive::<HistoryTree>(&b, ids[n - 6])
        .await
        .unwrap();
    hold_leases(meta2.as_ref()).await;
    let gap = missing(&b, HistoryTree::NAME, &[tip]).await;
    assert!(gap >= 2, "gap {gap}");
    let q = bounded(
        &b,
        Budgets {
            scan: gap - 1,
            legacy_factor: 8,
            ..Budgets::default()
        },
    );
    assert!(matches!(
        q.log_page_indexed(tip, &params, 1000, None, Freshness::Retry)
            .await,
        Err(Error::Indexing { .. })
    ));
    let legacy = q
        .log_page_indexed(tip, &params, 1000, None, Freshness::Legacy)
        .await
        .unwrap();
    assert_eq!(
        legacy
            .value
            .entries
            .iter()
            .map(|e| e.id)
            .collect::<Vec<_>>(),
        want[0].0
    );
    assert!(legacy.scanned > 0);
    let q = bounded(
        &b,
        Budgets {
            scan: 0,
            legacy_factor: 8,
            ..Budgets::default()
        },
    );
    assert!(matches!(
        q.log_page_indexed(tip, &params, 1000, None, Freshness::Legacy)
            .await,
        Err(Error::Indexing { .. })
    ));
    drop(meta);
}

/// An old client waits for the indexer while a waiter's place is free, at
/// most `--index-wait` and its remaining request time less a second; one
/// that finds the places taken does not wait.
#[tokio::test]
async fn old_clients_wait_within_the_cap_and_their_time() {
    let n = 30;
    let (_, ids) = full(6, n).await;
    let (b, meta) = memory_repo();
    assert_eq!(build(&b, 6, n).await, ids);
    hold_leases(meta.as_ref()).await;
    let tip = *ids.last().unwrap();
    let params = LogParams {
        path: Some("d".into()),
        ..Default::default()
    };
    let leases = IndexLeases::new(b.meta().clone(), IndexLeaseConfig::new("query", ""));
    let policy = IndexPolicy::new(IndexMode::Off, leases)
        .with_budgets(Budgets {
            scan: 0,
            legacy_factor: 1,
            ..Budgets::default()
        })
        .with_index_wait(Duration::from_secs(5), 1)
        .with_request_timeout(Duration::from_secs(3));
    let q = b.clone().with_index_policy(Arc::new(policy));
    // The indexer catches up after 1.5 s: the waiting request answers.
    let indexer = b.clone();
    let catch_up = tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(1500)).await;
        derived::derive::<HistoryTree>(&indexer, tip).await.unwrap();
    });
    let started = std::time::Instant::now();
    let (first, second) = tokio::join!(
        q.log_page_indexed(tip, &params, 10, None, Freshness::Legacy),
        async {
            tokio::time::sleep(Duration::from_millis(200)).await;
            let t = std::time::Instant::now();
            let r = q
                .log_page_indexed(tip, &params, 10, None, Freshness::Legacy)
                .await;
            (r, t.elapsed())
        }
    );
    catch_up.await.unwrap();
    assert!(first.is_ok(), "{first:?}");
    let waited = started.elapsed();
    assert!(waited >= Duration::from_millis(1400) && waited < Duration::from_secs(3));
    // The second found the only place taken: it did not wait.
    let (second, took) = second;
    assert!(matches!(second, Err(Error::Indexing { .. })), "{second:?}");
    assert!(took < Duration::from_millis(900), "{took:?}");

    // The wait stops a second before the request's time is up.
    let (b, meta) = memory_repo();
    assert_eq!(build(&b, 6, n).await, ids);
    hold_leases(meta.as_ref()).await;
    let leases = IndexLeases::new(b.meta().clone(), IndexLeaseConfig::new("query", ""));
    let policy = IndexPolicy::new(IndexMode::Off, leases)
        .with_budgets(Budgets {
            scan: 0,
            legacy_factor: 1,
            ..Budgets::default()
        })
        .with_index_wait(Duration::from_secs(10), 4)
        .with_request_timeout(Duration::from_millis(2500));
    let q = b.clone().with_index_policy(Arc::new(policy));
    let t = std::time::Instant::now();
    assert!(q
        .log_page_indexed(tip, &params, 10, None, Freshness::Legacy)
        .await
        .is_err());
    let took = t.elapsed();
    assert!(
        took >= Duration::from_millis(1400) && took < Duration::from_millis(2400),
        "{took:?}"
    );
}

/// History nodes deleted under a bounded query (a drop racing it) answer
/// `Indexing` (503), as a vanished `history-tree-v4` row does, not a
/// storage error.
#[tokio::test]
async fn vanished_history_nodes_answer_indexing() {
    let (r, meta) = memory_repo();
    let mut changes = BTreeMap::new();
    for i in 0..3000 {
        let body = format!("file {i}\n");
        let content = r.blobs().put(Bytes::from(body.clone())).await.unwrap();
        changes.insert(
            RepoPath::new(format!("d{}/e{}/f{i}", i % 40, i % 7)).unwrap(),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size: body.len() as u64,
            },
        );
    }
    let cs = Changeset {
        parents: vec![],
        author: "a".into(),
        timestamp: 1,
        message: "wide".into(),
        changes,
        extra: BTreeMap::new(),
    };
    let tip = r.create_changeset(cs).await.unwrap();
    derived::derive::<HistoryTree>(&r, tip).await.unwrap();
    let nodes = meta
        .list_derived(derived::HISTORY_NODE_KIND, None, usize::MAX)
        .await
        .unwrap();
    assert!(!nodes.is_empty(), "the pack has more than one part");
    meta.delete_derived_kind(derived::HISTORY_NODE_KIND)
        .await
        .unwrap();
    hold_leases(meta.as_ref()).await;
    // A fresh handle: nothing cached.
    let fresh = Repo::new(r.blobs().clone(), meta.clone());
    let q = bounded(&fresh, Budgets::default());
    let mut indexing = 0;
    for i in 0..40 {
        let params = LogParams {
            path: Some(format!("d{i}/e{}/f{i}", i % 7)),
            ..Default::default()
        };
        match q
            .log_page_indexed(tip, &params, 10, None, Freshness::Retry)
            .await
        {
            Ok(_) => {}
            Err(Error::Indexing { .. }) => indexing += 1,
            Err(e) => panic!("{e}"),
        }
    }
    assert!(indexing > 0);
}

/// A bounded search's scan computes, over random trees and random partial
/// indexes, the very rows derivation would store (byte for byte what
/// `recompute_tree_row` gives), and stores none of them.
#[tokio::test]
async fn bounded_scan_computes_the_stored_rows_and_stores_none() {
    use crate::search::tests::{commit_files, index_some, level_tree, tree_shape};
    for seed in 1..=30u64 {
        let (r, meta) = memory_repo();
        let files = level_tree(seed, 10 + (seed as usize * 7) % 60);
        let c = commit_files(&r, &[], &files).await;
        let root = r.root_tree(c).await.unwrap();
        if seed % 3 != 0 {
            index_some(&r, seed, root).await;
        }
        let rows_before = index_rows(meta.as_ref()).await;
        let scan = Arc::new(crate::indexing::bounded::SearchScan::new(&Budgets {
            search_files: 10_000,
            search_bytes: 1 << 30,
            ..Budgets::default()
        }));
        let counters = crate::search::index::Counters::scanning(scan.clone());
        let got = r.tree_row(root, &counters).await.unwrap();
        assert_eq!(index_rows(meta.as_ref()).await, rows_before, "seed {seed}");
        assert_eq!(
            (counters.blob_rows(), counters.tree_rows()),
            (0, 0),
            "seed {seed}"
        );
        let fresh = Repo::new(r.blobs().clone(), Arc::new(MemoryMetaStore::new()));
        let mut memo = crate::search::index::TreeRowMemo::new();
        assert_eq!(
            got,
            fresh.recompute_tree_row(root, &mut memo).await.unwrap(),
            "seed {seed}"
        );
        for (h, ..) in tree_shape(&r, root).await {
            let row = match scan.get(true, &h) {
                Some(row) => row,
                None => r
                    .meta()
                    .get_derived(crate::search::TREE_KIND, &h)
                    .await
                    .unwrap()
                    .expect("a filter the scan did not compute is stored"),
            };
            let want = fresh.recompute_tree_row(h, &mut memo).await.unwrap();
            assert_eq!(row, want, "seed {seed}: filter of {h}");
        }
    }
}

/// Over its file budget a bounded search stops fetching at once (the files
/// in flight aside), answers `Indexing`, and stores no row; within it, it
/// answers and still stores none.
#[tokio::test]
async fn bounded_scan_over_budget_stops_early_and_stores_nothing() {
    let (r, meta) = memory_repo();
    let mut files = crate::search::tests::Files::new();
    for d in 0..40 {
        for f in 0..10 {
            let body = format!("needle {d} {f}\n").into_bytes();
            files.insert(format!("d{d}/e/f{f}"), (body, FileKind::Regular));
        }
    }
    let c = crate::search::tests::commit_files(&r, &[], &files).await;
    hold_leases(meta.as_ref()).await;
    let rows_before = index_rows(meta.as_ref()).await;
    let budgets = Budgets {
        search_files: 20,
        ..Budgets::default()
    };
    let scan = Arc::new(crate::indexing::bounded::SearchScan::new(&budgets));
    let counters = crate::search::index::Counters::scanning(scan.clone());
    let root = r.root_tree(c).await.unwrap();
    match r.tree_row(root, &counters).await {
        Err(Error::Indexing { kind, .. }) => assert_eq!(kind, crate::search::TREE_KIND),
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert!(scan.over.load(std::sync::atomic::Ordering::Relaxed));
    let fetched = scan.files.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        fetched > 20 && fetched <= 20 + crate::search::index::CONCURRENCY as u64,
        "{fetched} of 400 files fetched"
    );
    let q = bounded(&r, budgets);
    let query = SearchQuery::new("needle 3 ");
    match q
        .search_page_indexed(&c.to_string(), &query, None, Freshness::Retry)
        .await
    {
        Err(Error::Indexing { kind, .. }) => assert_eq!(kind, crate::search::TREE_KIND),
        other => panic!("{:?}", other.map(|_| ())),
    }
    assert_eq!(index_rows(meta.as_ref()).await, rows_before);
    let q = bounded(
        &r,
        Budgets {
            search_files: 10_000,
            ..Budgets::default()
        },
    );
    let got = q
        .search_page_indexed(&c.to_string(), &query, None, Freshness::Retry)
        .await
        .unwrap();
    assert_eq!(got.value.matches.len(), 10);
    assert_eq!(got.scanned, 400);
    assert_eq!(index_rows(meta.as_ref()).await, rows_before);
}

/// Counts the tree nodes and blobs a repo reads.
struct CountingBlobs {
    inner: MemoryBlobStore,
    /// `get_many` calls (tree reads) and keys asked.
    many: std::sync::atomic::AtomicU64,
    many_keys: std::sync::atomic::AtomicU64,
    /// Single `get`s (file contents).
    gets: std::sync::atomic::AtomicU64,
}

#[async_trait::async_trait]
impl leep_core::BlobStore for CountingBlobs {
    async fn put_with_hash(&self, hash: leep_core::Hash, data: Bytes) -> leep_core::Result<()> {
        self.inner.put_with_hash(hash, data).await
    }
    async fn get(&self, hash: &leep_core::Hash) -> leep_core::Result<Option<Bytes>> {
        self.gets.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        self.inner.get(hash).await
    }
    async fn get_many(&self, hashes: &[leep_core::Hash]) -> leep_core::Result<Vec<Option<Bytes>>> {
        use std::sync::atomic::Ordering::SeqCst;
        self.many.fetch_add(1, SeqCst);
        self.many_keys.fetch_add(hashes.len() as u64, SeqCst);
        self.inner.get_many(hashes).await
    }
    async fn list_page(
        &self,
        after: Option<leep_core::Hash>,
        limit: usize,
    ) -> leep_core::Result<Vec<leep_core::BlobMeta>> {
        self.inner.list_page(after, limit).await
    }
    async fn delete(&self, hash: &leep_core::Hash) -> leep_core::Result<()> {
        self.inner.delete(hash).await
    }
}

/// Over budget on a wide cold tree, a bounded scan gives up after reading
/// the first levels of it, not every tree node and filter below the root,
/// and before fetching any file: a retry a second later then costs as
/// little. Within budget the same scan reads the whole tree.
#[tokio::test]
async fn bounded_scan_over_budget_reads_a_bounded_part_of_a_cold_tree() {
    use crate::indexing::bounded::{SearchScan, DISCOVERY_FACTOR, DISCOVERY_FILES};
    use crate::search::tests::{commit_files, tree_shape, Files, Probe};
    use std::sync::atomic::Ordering::SeqCst;
    let blobs = Arc::new(CountingBlobs {
        inner: MemoryBlobStore::new(),
        many: Default::default(),
        many_keys: Default::default(),
        gets: Default::default(),
    });
    let probe = Probe::new(Duration::ZERO);
    let r = Repo::new(blobs.clone(), probe.clone());
    // 12 directories of 12 of 12, two files in each directory: 1,885 tree
    // nodes and 3,770 files.
    let mut files = Files::new();
    let mut add = |dir: String| {
        for f in 0..2 {
            let body = format!("{dir} {f}\n").into_bytes();
            files.insert(format!("{dir}/f{f}"), (body, FileKind::Regular));
        }
    };
    for a in 0..12 {
        add(format!("a{a}"));
        for b in 0..12 {
            add(format!("a{a}/b{b}"));
            for c in 0..12 {
                add(format!("a{a}/b{b}/c{c}"));
            }
        }
    }
    let c = commit_files(&r, &[], &files).await;
    let root = r.root_tree(c).await.unwrap();
    let nodes = tree_shape(&r, root).await.len() as u64;
    assert_eq!(nodes, 1 + 12 + 144 + 1_728);

    let scan_with = |search_files: usize| {
        Arc::new(SearchScan::new(&Budgets {
            search_files,
            ..Budgets::default()
        }))
    };
    let count = || {
        (
            blobs.many_keys.load(SeqCst),
            probe.keys_read.load(SeqCst),
            blobs.gets.load(SeqCst),
        )
    };
    // Over: a budget of 4 files lets discovery find 4 * 8 + 256 = 288.
    assert_eq!(4 * DISCOVERY_FACTOR + DISCOVERY_FILES, 288);
    let scan = scan_with(4);
    let before = count();
    let counters = crate::search::index::Counters::scanning(scan.clone());
    match r.tree_row(root, &counters).await {
        Err(Error::Indexing { kind, .. }) => assert_eq!(kind, crate::search::TREE_KIND),
        other => panic!("{:?}", other.map(|_| ())),
    }
    let after = count();
    let (read_nodes, read_rows, fetched) =
        (after.0 - before.0, after.1 - before.1, after.2 - before.2);
    eprintln!("over budget: {read_nodes} tree nodes, {read_rows} filter keys, {fetched} blobs");
    assert!(scan.over.load(SeqCst));
    assert_eq!(fetched, 0, "nothing fetched");
    assert_eq!(scan.files.load(SeqCst), 0);
    // The root and two levels (312 files found), of 1,885 nodes.
    assert!(read_nodes <= 1 + 12 + 144, "{read_nodes} tree nodes read");
    assert!(read_rows <= 1 + 12 + 144, "{read_rows} filter keys read");

    // Within budget the scan reads everything, and fetches every file.
    let scan = scan_with(10_000);
    let before = count();
    let counters = crate::search::index::Counters::scanning(scan.clone());
    r.tree_row(root, &counters).await.unwrap();
    let after = count();
    assert!(after.0 - before.0 >= nodes);
    assert_eq!(scan.files.load(SeqCst), files.len() as u64);
    assert_eq!(index_rows(probe.as_ref()).await, 0, "nothing stored");
}
