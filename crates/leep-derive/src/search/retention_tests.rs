//! Retention of blob trigram rows (ADR 0102).

use std::collections::{BTreeMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{Changeset, ChangesetId, FileKind, Hash, MetaStore, Result};

use super::index::{BLOB_KIND, RETIRED_BLOB_KIND};
use super::retention::RetentionOptions;
use super::tests::{assert_closed, commit_files, Files};
use super::SearchQuery;
use crate::Repo;

fn now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs() as i64
}

/// A time past every ref log entry by more than the clock skew a pass
/// allows for.
fn later() -> i64 {
    now() + 3 * 300
}

/// Retire everything no current head holds: a window of zero, at
/// [`later`].
fn at_once() -> RetentionOptions {
    RetentionOptions {
        keep: std::time::Duration::ZERO,
        batch: 7,
        rest_factor: 0,
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
        .retire_search_rows(&RetentionOptions::default(), now(), &ok)
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
        .retire_search_rows(&opts, now() + 86_400, &ok)
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
        .retire_search_rows(&RetentionOptions::default(), now(), &ok)
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
    r.retire_search_rows(&at_once(), later(), &ok)
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
        .retire_search_rows(&at_once(), later(), &lost)
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
            .retire_search_rows(&at_once(), later(), &ok)
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
