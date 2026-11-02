//! Finding a missing range down first-parent chains (ADR 0077): a long
//! linear gap costs a few metadata reads per batch of changesets, not two
//! per changeset, and derives exactly what the frontier walk did.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
use leep_core::{
    BlobStore, Changeset, ChangesetId, FileChange, FileKind, Hash, MetaStore, RefLogEntry,
    RefUpdateContext, RepoPath, Result,
};

use crate::derived::{self, DerivedKind, HistoryTree, LastMerge, SkipEntry, Skiplist};
use crate::Repo;

/// Counts metadata read statements.
struct Counter {
    inner: Arc<MemoryMetaStore>,
    reads: AtomicUsize,
}

impl Counter {
    fn note(&self) {
        self.reads.fetch_add(1, Ordering::Relaxed);
    }

    fn take(&self) -> usize {
        self.reads.swap(0, Ordering::Relaxed)
    }
}

#[async_trait::async_trait]
impl MetaStore for Counter {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }
    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.note();
        self.inner.get_changeset(id).await
    }
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.note();
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
        self.note();
        self.inner.get_derived(kind, key).await
    }
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.note();
        self.inner.get_derived_many(kind, keys).await
    }
    async fn get_derived_kinds(
        &self,
        reads: &[(&str, &[Hash])],
    ) -> Result<Vec<Vec<Option<Bytes>>>> {
        self.note();
        self.inner.get_derived_kinds(reads).await
    }
    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }
    async fn put_derived_many(&self, rows: &[leep_core::DerivedRow]) -> Result<()> {
        self.inner.put_derived_many(rows).await
    }
    async fn delete_derived_keys(&self, kinds: &[&str], keys: &[Hash]) -> Result<u64> {
        self.inner.delete_derived_keys(kinds, keys).await
    }
    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        self.inner.delete_derived_kind(kind).await
    }
}

/// A history built twice: in `store`, with no rows of `K` (but the other
/// rows written with each changeset), and in `reference`, where each changeset's rows of
/// `K` were derived as it was created (a frontier of one changeset).
struct Built {
    blobs: Arc<MemoryBlobStore>,
    store: Arc<MemoryMetaStore>,
    reference: Repo,
    ids: Vec<ChangesetId>,
}

/// Shapes of history: each step names its parents by index in the steps so
/// far (none for a root).
fn linear(len: usize) -> Vec<Vec<usize>> {
    (0..len)
        .map(|i| if i == 0 { vec![] } else { vec![i - 1] })
        .collect()
}

/// A main line with a side branch merged every `every` changesets: the
/// branch forks `back` changesets earlier and is `side` changesets long.
/// Some merges take the main line's own ancestor as second parent. Returns
/// the steps, the tip, and the side branches' changesets.
fn merges(
    len: usize,
    every: usize,
    back: usize,
    side: usize,
) -> (Vec<Vec<usize>>, usize, Vec<usize>) {
    let mut steps: Vec<Vec<usize>> = vec![vec![]];
    let mut sides = Vec::new();
    let mut tip = 0;
    for n in 1..len {
        if n % every == 0 && tip >= back {
            let mut b = tip - back;
            if n % (3 * every) == 0 {
                // A merge of a first-parent ancestor.
                steps.push(vec![tip, b]);
            } else {
                for _ in 0..side {
                    steps.push(vec![b]);
                    b = steps.len() - 1;
                    sides.push(b);
                }
                steps.push(vec![tip, b]);
            }
        } else {
            steps.push(vec![tip]);
        }
        tip = steps.len() - 1;
    }
    (steps, tip, sides)
}

async fn build<K: DerivedKind>(steps: &[Vec<usize>]) -> Built {
    let blobs = Arc::new(MemoryBlobStore::new());
    let store = Arc::new(MemoryMetaStore::new());
    let writer = Repo::new(blobs.clone(), store.clone());
    let reference = Repo::new(blobs.clone(), Arc::new(MemoryMetaStore::new()));
    let mut ids = Vec::with_capacity(steps.len());
    for (n, ps) in steps.iter().enumerate() {
        let text = format!("{n}\n");
        blobs.put(Bytes::from(text.clone())).await.unwrap();
        let mut changes = BTreeMap::new();
        changes.insert(
            RepoPath::new(format!("f{}", n % 7)).unwrap(),
            FileChange::Modify {
                content: Hash::of_blob(text.as_bytes()),
                kind: FileKind::Regular,
                size: text.len() as u64,
            },
        );
        let cs = Changeset {
            parents: ps.iter().map(|&p| ids[p]).collect(),
            author: "t".into(),
            timestamp: n as i64,
            message: format!("c{n}"),
            changes,
            extra: BTreeMap::new(),
        };
        let id = writer.create_changeset(cs.clone()).await.unwrap();
        assert_eq!(reference.create_changeset(cs).await.unwrap(), id);
        derived::derive::<K>(&reference, id).await.unwrap();
        ids.push(id);
    }
    // Some kinds are written with the changeset; start without any.
    store.delete_derived_kind(K::NAME).await.unwrap();
    Built {
        blobs,
        store,
        reference,
        ids,
    }
}

impl Built {
    /// A new process over `store`, counting its reads: no skiplists cached.
    fn cold(&self) -> (Repo, Arc<Counter>) {
        let counter = Arc::new(Counter {
            inner: self.store.clone(),
            reads: AtomicUsize::new(0),
        });
        (Repo::new(self.blobs.clone(), counter.clone()), counter)
    }

    /// Rows of `K` in `store`, by index, beside the reference's.
    async fn rows<K: DerivedKind>(&self) -> Vec<(Option<Bytes>, Option<Bytes>)> {
        let mut out = Vec::new();
        for id in &self.ids {
            out.push((
                self.store.get_derived(K::NAME, &id.0).await.unwrap(),
                self.reference
                    .meta()
                    .get_derived(K::NAME, &id.0)
                    .await
                    .unwrap(),
            ));
        }
        out
    }

    /// Every row of `K` in `store` equals the reference's, and those at
    /// `absent` are not there.
    async fn check<K: DerivedKind>(&self, absent: &[usize]) {
        for (i, (got, want)) in self.rows::<K>().await.into_iter().enumerate() {
            if absent.contains(&i) {
                assert_eq!(got, None, "{} row of {i} derived", K::NAME);
            } else {
                assert_eq!(got, want, "{} row of {i}", K::NAME);
            }
        }
    }
}

fn calls(r: &Repo) -> usize {
    r.derive_calls.load(Ordering::Relaxed)
}

/// A gap of 5,000 linear changesets derives in a few reads per batch of
/// 256 (the frontier walk made two per changeset, about 10,000), each
/// changeset once, with the reference's values.
#[tokio::test(flavor = "multi_thread")]
async fn a_long_linear_gap_derives_in_batches() {
    let len = 5000;
    let b = build::<LastMerge>(&linear(len)).await;
    let (r, counter) = b.cold();
    let tip = *b.ids.last().unwrap();
    derived::derive_heads::<LastMerge>(&r, &[tip])
        .await
        .unwrap();
    let reads = counter.take();
    assert_eq!(calls(&r), len);
    assert!(reads <= 150, "{reads} reads for a gap of {len}");
    b.check::<LastMerge>(&[]).await;

    // Nothing left: one read finds the head indexed.
    derived::derive_heads::<LastMerge>(&r, &[tip])
        .await
        .unwrap();
    assert_eq!(calls(&r), len);
    assert!(counter.take() <= 1);
}

/// The history index (what the warmer derives) over merges, side branches
/// and merges of a first-parent ancestor, cold: the reference's rows (its
/// derivations also derive other kinds, so only rows are compared).
#[tokio::test(flavor = "multi_thread")]
async fn merges_and_side_branches_derive_every_changeset_once() {
    let (steps, tip, _) = merges(900, 40, 25, 30);
    let b = build::<HistoryTree>(&steps).await;
    let (r, _) = b.cold();
    derived::derive_heads::<HistoryTree>(&r, &[b.ids[tip]])
        .await
        .unwrap();
    b.check::<HistoryTree>(&[]).await;
}

/// A gap with parts already derived (a ref inside the gap, a side branch,
/// heads at different depths of one chain) derives exactly the rest.
#[tokio::test(flavor = "multi_thread")]
async fn a_partly_derived_gap_derives_only_the_rest() {
    let (steps, tip, sides) = merges(3000, 100, 60, 20);
    let b = build::<LastMerge>(&steps).await;
    let (r, counter) = b.cold();
    // A changeset early on the main line and one on a side branch.
    let side = sides[sides.len() / 2 + 5];
    let early = 1200;
    derived::derive_heads::<LastMerge>(&r, &[b.ids[early], b.ids[side]])
        .await
        .unwrap();
    let before = calls(&r);
    let mut have = 0;
    for (got, _) in b.rows::<LastMerge>().await {
        have += got.is_some() as usize;
    }
    assert_eq!(before, have);
    counter.take();
    let mid = 2400;
    derived::derive_heads::<LastMerge>(&r, &[b.ids[tip], b.ids[mid], b.ids[mid - 7]])
        .await
        .unwrap();
    assert_eq!(calls(&r), steps.len());
    let reads = counter.take();
    assert!(reads <= 250, "{reads} reads for {}", steps.len() - before);
    b.check::<LastMerge>(&[]).await;
}

/// Skiplist rows that are gone, or that name a changeset off the chain,
/// only shorten the chains followed: the range found is the same.
#[tokio::test(flavor = "multi_thread")]
async fn damaged_skiplists_change_nothing_derived() {
    let len = 1500;
    let mut steps = linear(len);
    // A branch off the root that nothing reaches from the tip.
    steps.push(vec![0]);
    steps.push(vec![len]);
    let stray = len + 1;
    let b = build::<LastMerge>(&steps).await;
    let gone: Vec<Hash> = (100..1400).step_by(97).map(|i| b.ids[i].0).collect();
    b.store
        .delete_derived_keys(&[Skiplist::NAME], &gone)
        .await
        .unwrap();
    // A row at 1,450 whose entries jump to the stray branch.
    let bogus = Skiplist::encode(&[
        SkipEntry {
            id: b.ids[stray],
            generation: 3,
        },
        SkipEntry {
            id: b.ids[len],
            generation: 2,
        },
    ]);
    b.store
        .put_derived(Skiplist::NAME, &b.ids[1450].0, bogus)
        .await
        .unwrap();
    let (r, _) = b.cold();
    derived::derive_heads::<LastMerge>(&r, &[b.ids[len - 1]])
        .await
        .unwrap();
    assert_eq!(calls(&r), len);
    b.check::<LastMerge>(&[len, stray]).await;
}
