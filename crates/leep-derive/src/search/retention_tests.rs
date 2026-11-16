//! Retention of blob trigram rows (ADR 0102).

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileKind, Hash, MetaStore, Result};

use super::index::{BLOB_KIND, RETIRED_BLOB_KIND};
use super::retention::{RetentionOptions, RetentionState};
use super::tests::{assert_closed, commit_files, Files};
use super::SearchQuery;
use crate::Repo;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// A time past every ref log entry.
fn later() -> i64 {
    now() + 10
}

/// Retire everything no current head holds: a window of zero, at
/// [`later`].
fn at_once() -> RetentionOptions {
    RetentionOptions {
        keep: std::time::Duration::ZERO,
        batch: 7,
        rest_factor: 0,
        skew: std::time::Duration::ZERO,
        ..RetentionOptions::default()
    }
}

fn ok() -> Result<()> {
    Ok(())
}

fn file(body: &str) -> (Vec<u8>, FileKind) {
    (body.as_bytes().to_vec(), FileKind::Regular)
}

async fn blob_rows(r: &Repo) -> HashSet<Hash> {
    let mut out = HashSet::new();
    for kind in [BLOB_KIND, RETIRED_BLOB_KIND] {
        for (k, _) in r.meta().list_derived(kind, None, usize::MAX).await.unwrap() {
            out.insert(k);
        }
    }
    out
}

/// Every blob below `id`'s tree.
async fn blobs_of(r: &Repo, id: ChangesetId) -> HashSet<Hash> {
    r.list_files(id, "")
        .await
        .unwrap()
        .into_iter()
        .map(|f| f.content)
        .collect()
}

/// A base of many files and `lands` edits of one file on `main`, each
/// derived as the warmer derives it.
async fn chain(r: &Repo, lands: usize) -> Vec<ChangesetId> {
    let mut files = Files::new();
    for d in 0..4 {
        for f in 0..5 {
            files.insert(
                format!("src/d{d}/f{f}.rs"),
                file(&format!("fn d{d}_f{f}() {{ needle_{d}_{f} }}\n").repeat(20)),
            );
        }
    }
    let base = commit_files(r, &[], &files).await;
    r.meta().cas_ref("main", None, Some(base)).await.unwrap();
    r.derive_search(base).await.unwrap();
    let mut ids = vec![base];
    let mut body = files["src/d1/f1.rs"].0.clone();
    for i in 0..lands {
        body.extend(format!("fn v{i}() {{ edit_{i} }}\n").as_bytes());
        let one = Files::from([("src/d1/f1.rs".into(), (body.clone(), FileKind::Regular))]);
        let id = commit_files(r, &[*ids.last().unwrap()], &one).await;
        r.meta()
            .cas_ref("main", ids.last().copied(), Some(id))
            .await
            .unwrap();
        r.derive_search(id).await.unwrap();
        ids.push(id);
    }
    ids
}

/// Past the window, the rows of blobs no head holds are deleted and those
/// of every head's blobs are kept; an old revision still searches right,
/// deriving what it lacks, and the head derives nothing.
#[tokio::test]
async fn rows_no_head_holds_are_deleted_after_the_window() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let ids = chain(&r, 12).await;
    let head = *ids.last().unwrap();
    let before = blob_rows(&r).await;
    assert_eq!(before.len(), 20 + 12);

    // Inside the window: nothing goes.
    let kept = r
        .retire_search_rows(
            &RetentionOptions::default(),
            now(),
            &mut RetentionState::default(),
            &ok,
        )
        .await
        .unwrap();
    assert_eq!(kept.deleted, 0);
    assert_eq!(blob_rows(&r).await, before);

    // A pass a day later with a window of an hour: only the head's rows stay.
    let opts = RetentionOptions {
        keep: std::time::Duration::from_secs(3600),
        ..at_once()
    };
    let report = r
        .retire_search_rows(&opts, now() + 86_400, &mut RetentionState::default(), &ok)
        .await
        .unwrap();
    let live = blobs_of(&r, head).await;
    assert_eq!(report.deleted, 12);
    assert_eq!(report.examined, 32);
    assert_eq!(report.restored, 0);
    assert_eq!(blob_rows(&r).await, live);
    assert_closed(&r, r.root_tree(head).await.unwrap(), "head").await;

    let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
    let at_head = fresh
        .search(head, &SearchQuery::new("edit_3"))
        .await
        .unwrap();
    assert_eq!(at_head.matches.len(), 1);
    assert_eq!(at_head.stats.blob_rows_derived, 0);
    let old = fresh
        .search(ids[5], &SearchQuery::new("edit_3"))
        .await
        .unwrap();
    assert_eq!(old.matches.len(), 1);
    assert_eq!(old.stats.blob_rows_derived, 1, "derived again on demand");
    let again = fresh
        .search(ids[5], &SearchQuery::new("edit_3"))
        .await
        .unwrap();
    assert_eq!(again.stats.blob_rows_derived, 0, "and stored");
}

/// Heads current at any time within the window keep their rows: those the
/// ref log names as a move's old head, and those of deleted refs.
#[tokio::test]
async fn heads_inside_the_window_keep_their_rows() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let ids = chain(&r, 4).await;
    // A branch at ids[2], deleted again.
    r.meta().cas_ref("topic", None, Some(ids[2])).await.unwrap();
    r.meta().cas_ref("topic", Some(ids[2]), None).await.unwrap();
    let report = r
        .retire_search_rows(
            &RetentionOptions::default(),
            now(),
            &mut RetentionState::default(),
            &ok,
        )
        .await
        .unwrap();
    assert_eq!(report.deleted, 0);
    assert_eq!(report.heads, 5, "every head main held, and topic's");
}

/// The retired kind's rows are retired by the same rule.
#[tokio::test]
async fn retired_v1_rows_are_retired_too() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let ids = chain(&r, 3).await;
    let head = *ids.last().unwrap();
    let stray = Hash::of_blob(b"a blob only an old server indexed");
    r.meta()
        .put_derived(RETIRED_BLOB_KIND, &stray, Bytes::from_static(&[1]))
        .await
        .unwrap();
    for b in blobs_of(&r, head).await {
        r.meta()
            .put_derived(RETIRED_BLOB_KIND, &b, Bytes::from_static(&[1]))
            .await
            .unwrap();
    }
    r.retire_search_rows(&at_once(), later(), &mut RetentionState::default(), &ok)
        .await
        .unwrap();
    let v1: HashSet<Hash> = r
        .meta()
        .list_derived(RETIRED_BLOB_KIND, None, usize::MAX)
        .await
        .unwrap()
        .into_iter()
        .map(|(k, _)| k)
        .collect();
    assert_eq!(v1, blobs_of(&r, head).await);
}

/// A check that fails stops the pass before it deletes anything.
#[tokio::test]
async fn a_failed_check_deletes_nothing() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    chain(&r, 3).await;
    let before = blob_rows(&r).await;
    let lost = || Err(leep_core::Error::Conflict("lost the lease".into()));
    assert!(r
        .retire_search_rows(&at_once(), later(), &mut RetentionState::default(), &lost)
        .await
        .is_err());
    assert_eq!(blob_rows(&r).await, before);
}

/// When the hook runs, relative to a pass's delete.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum When {
    /// While the pass lists the keys it may delete.
    Listing,
    /// Inside the delete, just before it takes effect.
    Deleting,
}

/// A store that moves `main` back to `revert` once, at `when`: a land of
/// old content racing the pass.
struct Racing {
    inner: Arc<MemoryMetaStore>,
    when: When,
    revert: Mutex<Option<(ChangesetId, ChangesetId)>>,
    /// A delete fires the hook only if it names this blob.
    target: Mutex<Option<Hash>>,
}

impl Racing {
    async fn fire(&self, at: When) {
        if at != self.when {
            return;
        }
        let Some((from, to)) = self.revert.lock().unwrap().take() else {
            return;
        };
        assert!(self
            .inner
            .cas_ref("main", Some(from), Some(to))
            .await
            .unwrap());
    }
}

#[async_trait::async_trait]
impl MetaStore for Racing {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
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
        ctx: &leep_core::RefUpdateContext,
    ) -> Result<bool> {
        self.inner.cas_ref_logged(name, expected, new, ctx).await
    }
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<leep_core::RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }
    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.inner.get_derived_many(kind, keys).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_if_absent(&self, kind: &str, key: &Hash, value: Bytes) -> Result<Bytes> {
        self.inner.put_derived_if_absent(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[leep_core::DerivedRow]) -> Result<()> {
        self.inner.put_derived_many(rows).await
    }
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.inner.list_derived(kind, after, limit).await
    }
    async fn list_derived_keys(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<Hash>> {
        let keys = self.inner.list_derived_keys(kind, after, limit).await?;
        self.fire(When::Listing).await;
        Ok(keys)
    }
    async fn estimate_derived(&self, kind: &str) -> Result<leep_core::RowCount> {
        self.inner.estimate_derived(kind).await
    }
    async fn delete_derived_keys(&self, kinds: &[&str], keys: &[Hash]) -> Result<u64> {
        let target = *self.target.lock().unwrap();
        if target.is_some_and(|t| keys.contains(&t)) {
            self.fire(When::Deleting).await;
        }
        self.inner.delete_derived_keys(kinds, keys).await
    }

    async fn cas_derived(
        &self,
        kind: &str,
        key: &Hash,
        expected: Option<&[u8]>,
        new: Bytes,
    ) -> Result<bool> {
        self.inner.cas_derived(kind, key, expected, new).await
    }
}

/// A land of old content racing a pass, before it deletes or during the
/// delete, never leaves the new head without a row: the pass reads the refs
/// before and after each delete, keeps what a new head holds, and derives
/// again what it deleted that one holds.
#[tokio::test]
async fn a_head_that_appears_during_a_pass_keeps_every_row() {
    for when in [When::Listing, When::Deleting] {
        let meta = Arc::new(MemoryMetaStore::new());
        let racing = Arc::new(Racing {
            inner: meta.clone(),
            when,
            revert: Mutex::new(None),
            target: Mutex::new(None),
        });
        let r = Repo::new(Arc::new(MemoryBlobStore::new()), racing.clone());
        let ids = chain(&r, 8).await;
        let head = *ids.last().unwrap();
        // The revert: ids[2]'s content as a new changeset on the head.
        let mut old = Files::new();
        for f in r.list_files(ids[2], "").await.unwrap() {
            let data = r.blobs().get(&f.content).await.unwrap().unwrap();
            old.insert(f.path.as_str().to_string(), (data.to_vec(), f.kind));
        }
        let revert = commit_files(&r, &[head], &old).await;
        *racing.revert.lock().unwrap() = Some((head, revert));
        // The one blob the revert brings back that the head lacks.
        let back: Vec<Hash> = blobs_of(&r, revert)
            .await
            .difference(&blobs_of(&r, head).await)
            .copied()
            .collect();
        assert_eq!(back.len(), 1);
        *racing.target.lock().unwrap() = Some(back[0]);
        let report = r
            .retire_search_rows(&at_once(), later(), &mut RetentionState::default(), &ok)
            .await
            .unwrap();
        assert_eq!(
            meta.get_ref("main").await.unwrap(),
            Some(revert),
            "{when:?}"
        );
        let stored = blob_rows(&r).await;
        for b in blobs_of(&r, revert).await {
            assert!(stored.contains(&b), "{when:?}: a row the new head holds");
        }
        match when {
            When::Listing => assert_eq!(report.restored, 0, "kept, not deleted"),
            When::Deleting => assert_eq!(report.restored, 1, "derived again: {report:?}"),
        }
        // The warmer then derives the new head as usual.
        r.derive_search(revert).await.unwrap();
        assert_closed(&r, r.root_tree(revert).await.unwrap(), "revert").await;
    }
}

/// A head whose directory filters are stored but whose blob rows are not
/// (a branch from content older than the window, a rollback, a dropped
/// kind) gets its rows back from the backfill, a bounded number a pass,
/// and a pass with nothing new to do is skipped.
#[tokio::test]
async fn the_backfill_restores_rows_under_stored_filters() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let ids = chain(&r, 3).await;
    let head = *ids.last().unwrap();
    let all: Vec<Hash> = blob_rows(&r).await.into_iter().collect();
    r.meta()
        .delete_derived_keys(&[BLOB_KIND], &all)
        .await
        .unwrap();
    // The warmer finds the root's filter stored and derives nothing.
    assert_eq!(r.derive_search(head).await.unwrap().blob_rows, 0);
    assert!(blob_rows(&r).await.is_empty());
    let opts = RetentionOptions {
        backfill_rows: 8,
        ..RetentionOptions::default()
    };
    let mut state = RetentionState::default();
    let first = r
        .retire_search_rows(&opts, now(), &mut state, &ok)
        .await
        .unwrap();
    assert_eq!(first.backfilled, 8, "bounded: {first:?}");
    let mut passes = 1;
    loop {
        let rep = r
            .retire_search_rows(&opts, now(), &mut state, &ok)
            .await
            .unwrap();
        passes += 1;
        if rep.backfilled == 0 {
            break;
        }
        assert!(passes < 10);
    }
    assert_eq!(blob_rows(&r).await, blobs_of(&r, head).await);
    assert_closed(&r, r.root_tree(head).await.unwrap(), "backfilled").await;
    let quiet = r
        .retire_search_rows(&opts, now(), &mut state, &ok)
        .await
        .unwrap();
    assert!(quiet.skipped, "{quiet:?}");
    // A land wakes it up again.
    let one = Files::from([("new.rs".to_string(), file("fn x() {}\n"))]);
    let next = commit_files(&r, &[head], &one).await;
    r.meta()
        .cas_ref("main", Some(head), Some(next))
        .await
        .unwrap();
    let woke = r
        .retire_search_rows(&opts, now(), &mut state, &ok)
        .await
        .unwrap();
    assert!(!woke.skipped);
}

/// One server at a time runs a pass: while another holds the lease, a
/// pass deletes nothing and says so; once it is released, the pass runs.
#[tokio::test]
async fn a_pass_waits_for_the_lease_another_server_holds() {
    let meta: Arc<dyn MetaStore> = Arc::new(MemoryMetaStore::new());
    let blobs = Arc::new(MemoryBlobStore::new());
    let one = Repo::new(blobs.clone(), meta.clone());
    let other = Repo::new(blobs, meta.clone());
    chain(&one, 3).await;
    let before = blob_rows(&one).await;
    let held = crate::gc_lease::GcLease::acquire_key(
        &meta,
        super::retention::lease_key(),
        "search-retention",
    )
    .await
    .unwrap()
    .expect("memory store has leases");
    let mut state = RetentionState::default();
    let busy = super::retention::retention_pass(&other, &at_once(), later(), &mut state)
        .await
        .unwrap();
    assert!(busy.is_none());
    assert_eq!(blob_rows(&one).await, before);
    held.release().await.unwrap();
    let ran = super::retention::retention_pass(&other, &at_once(), later(), &mut state)
        .await
        .unwrap()
        .expect("the lease is free");
    assert_eq!(ran.deleted, 3);
}

/// Dropping the retired kind copies its rows to `blob-trigrams-v2` first:
/// a file unchanged since the upgrade has only its v1 row, and the
/// directory filters above it stay, so nothing would derive it again.
#[tokio::test]
async fn dropping_v1_rows_copies_them_to_v2_first() {
    let r = Repo::new(
        Arc::new(MemoryBlobStore::new()),
        Arc::new(MemoryMetaStore::new()),
    );
    let ids = chain(&r, 2).await;
    let head = *ids.last().unwrap();
    // What an older server left: v1 rows only.
    let v2 = r
        .meta()
        .list_derived(BLOB_KIND, None, usize::MAX)
        .await
        .unwrap();
    for (k, v) in &v2 {
        r.meta()
            .put_derived(RETIRED_BLOB_KIND, k, v.clone())
            .await
            .unwrap();
    }
    let keys: Vec<Hash> = v2.iter().map(|(k, _)| *k).collect();
    r.meta()
        .delete_derived_keys(&[BLOB_KIND], &keys)
        .await
        .unwrap();
    let report =
        crate::integrity::drop_kinds(&r, &[RETIRED_BLOB_KIND], false, &crate::integrity::Quiet)
            .await
            .unwrap();
    assert_eq!(report.copied, v2.len() as u64);
    assert_eq!(report.rows[RETIRED_BLOB_KIND], v2.len() as u64);
    let after: Vec<(Hash, Bytes)> = r
        .meta()
        .list_derived(BLOB_KIND, None, usize::MAX)
        .await
        .unwrap();
    assert_eq!(after, v2, "the same bytes, as v2 rows");
    let fresh = Repo::new(r.blobs().clone(), r.meta().clone());
    let got = fresh
        .search(head, &SearchQuery::new("needle_2_3"))
        .await
        .unwrap();
    assert_eq!(got.matches.len(), 20);
    assert_eq!(got.stats.blob_rows_derived, 0);
}

/// One seed of the retention fault sweep (`fault_tests`): a pass over a
/// store whose calls fail at random (deletes applied then failed among
/// them), with a land of old content racing its deletes and, now and then,
/// the server's state lost as by a restart, is retried until one pass
/// completes; then every blob of the current head has a row. Returns the
/// injected failures it met.
pub(crate) async fn retention_fault_sweep(seed: u64) -> u64 {
    use leep_testkit::{is_injected, Fault, FaultPlan, FaultyBlobStore, FaultyMetaStore, Op};
    let meta = Arc::new(MemoryMetaStore::new());
    let blobs: Arc<dyn leep_core::BlobStore> = Arc::new(MemoryBlobStore::new());
    let racing = Arc::new(Racing {
        inner: meta.clone(),
        when: When::Deleting,
        revert: Mutex::new(None),
        target: Mutex::new(None),
    });
    let clean = Repo::new(blobs.clone(), racing.clone());
    let ids = chain(&clean, 8).await;
    let head = *ids.last().unwrap();
    let mut old = Files::new();
    for f in clean.list_files(ids[2], "").await.unwrap() {
        let data = clean.blobs().get(&f.content).await.unwrap().unwrap();
        old.insert(f.path.as_str().to_string(), (data.to_vec(), f.kind));
    }
    let revert = commit_files(&clean, &[head], &old).await;
    let back: Vec<Hash> = blobs_of(&clean, revert)
        .await
        .difference(&blobs_of(&clean, head).await)
        .copied()
        .collect();
    *racing.revert.lock().unwrap() = Some((head, revert));
    *racing.target.lock().unwrap() = back.first().copied();

    let plan = FaultPlan::new(seed);
    plan.rate(Op::META, 60, Fault::Fail)
        .rate(&[Op::PutDerived], 80, Fault::FailAfter)
        .rate(&[Op::DeleteChangesets], 250, Fault::FailAfter)
        .rate(&[Op::DeleteChangesets], 100, Fault::Fail)
        .rate(Op::BLOB, 40, Fault::Fail);
    let faulty_blobs: Arc<dyn leep_core::BlobStore> =
        Arc::new(FaultyBlobStore::new(blobs.clone(), plan.clone()));
    let faulty_meta: Arc<dyn MetaStore> =
        Arc::new(FaultyMetaStore::new(racing.clone(), plan.clone()));
    let mut rng = leep_testkit::Rng::new(seed ^ 0x0102);
    let mut state = RetentionState::default();
    let mut failures = 0;
    // Derived segment passes beside it (ADR 0106), through faults of their
    // own and killed at random points: they seal the live search rows and
    // retire them, under their own lease; retention touches only rows.
    use crate::derived_segments::{compact_pass, CompactOptions, CompactState, Features, Mode};
    let cplan = FaultPlan::new(seed ^ 0x5EA1);
    cplan
        .rate(Op::META, 40, Fault::Fail)
        .rate(Op::WRITES, 30, Fault::FailAfter)
        .rate(Op::BLOB, 40, Fault::Fail);
    let compactor = Repo::new(
        Arc::new(FaultyBlobStore::new(blobs.clone(), cplan.clone())),
        Arc::new(FaultyMetaStore::new(meta.clone(), cplan.clone())),
    );
    let copts = CompactOptions {
        content_keep: Some(std::time::Duration::ZERO),
        content_skew: std::time::Duration::ZERO,
        content_now: Some(later()),
        ..CompactOptions::small(Mode::Retire)
    };
    let features = Features {
        record: Arc::new(|_| Box::pin(async { Ok(()) })),
        recorded: Arc::new(|_| Box::pin(async { Ok(true) })),
    };
    let mut cstate = CompactState::default();
    loop {
        if rng.below(2) == 0 {
            let kill = if rng.below(3) == 0 {
                rng.below(60)
            } else {
                usize::MAX
            };
            let _ = leep_testkit::cancel_after_polls(
                compact_pass(&compactor, &copts, &mut cstate, Some(&features)),
                kill,
            )
            .await;
        }
        // A process whose view of the derived segments is fresh: a pass
        // retires a row only once its segment was live far longer than a
        // reader's manifest may be old (ADR 0104), which these passes,
        // retiring at once, stand in for.
        let faulty = Repo::new(faulty_blobs.clone(), faulty_meta.clone());
        match faulty
            .retire_search_rows(&at_once(), later(), &mut state, &ok)
            .await
        {
            Ok(_) => break,
            Err(e) if is_injected(&e) => {
                failures += 1;
                if rng.below(4) == 0 {
                    state = RetentionState::default();
                }
            }
            Err(e) => panic!("seed {seed}: {e}"),
        }
        assert!(failures < 500, "seed {seed}: no pass completes");
    }
    let now_head = meta.get_ref("main").await.unwrap().unwrap();
    let reader = Repo::new(blobs.clone(), meta.clone());
    // A row, or a live segment's value: retention never deletes a sealed
    // value, and a pass retires a row only once a segment holds it.
    let sealed = |r: Repo| async move {
        r.segments().refresh(&**r.row_meta()).await.unwrap();
        r.segments()
            .family(crate::derived_segments::FAMILY_CONTENT)
            .keys_with(crate::derived_segments::COL_BLOB_TRIGRAMS)
    };
    let mut stored = blob_rows(&reader).await;
    stored.extend(sealed(Repo::new(blobs.clone(), meta.clone())).await);
    for b in blobs_of(&reader, now_head).await {
        assert!(
            stored.contains(&b),
            "seed {seed}: the head's blob {b} lost its row after {failures} failures"
        );
    }
    // Fault-free passes settle, and the head's rows stay readable.
    let clean_passes = Repo::new(blobs.clone(), meta.clone());
    for _ in 0..20 {
        let p = compact_pass(&clean_passes, &copts, &mut cstate, Some(&features))
            .await
            .unwrap()
            .unwrap();
        if p.sealed_segments + p.merges + p.retired_rows + p.dropped == 0 {
            break;
        }
    }
    let mut stored = blob_rows(&reader).await;
    stored.extend(sealed(Repo::new(blobs.clone(), meta.clone())).await);
    for b in blobs_of(&reader, now_head).await {
        assert!(
            stored.contains(&b),
            "seed {seed}: the head's blob {b} lost its row"
        );
    }
    assert_closed(&reader, reader.root_tree(now_head).await.unwrap(), "sweep").await;
    failures
}
