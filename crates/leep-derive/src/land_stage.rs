//! Staged writes for a land batch (ADR 0043). The replays of a batch write
//! into memory, read their own writes back, and are stored with one blob
//! write wave and one metadata write just before the ref moves, instead of a
//! round of fsyncs and a transaction per land.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::store::{BlobMeta, DerivedRow, RefLogEntry, RefUpdateContext};
use leep_core::{BlobStore, Changeset, ChangesetId, Error, Hash, MetaStore, Result};

use crate::derived::{DerivedKind, Generation, RootTree};
use crate::gc::BLOB_CONDEMNED;
use crate::signatures::REBASED_ORIGIN_KIND;
use crate::Repo;

/// Blobs written at once when a batch is stored. A batch rewrites a few
/// directories per land, so a full batch of shallow paths is one wave: the
/// filesystem then commits the writes' syncs together.
const FLUSH_WRITES_IN_FLIGHT: usize = 256;

/// What the replays of one batch attempt wrote, plus the immutable rows they
/// read, so a land does not read again what the land before it read.
#[derive(Default)]
pub(crate) struct Staging {
    inner: Mutex<Staged>,
}

#[derive(Default)]
struct Staged {
    blobs: HashMap<Hash, Bytes>,
    blob_order: Vec<Hash>,
    changesets: HashMap<ChangesetId, Changeset>,
    changeset_order: Vec<ChangesetId>,
    rows: HashMap<(String, Hash), Bytes>,
    row_order: Vec<(String, Hash)>,
    /// Stored changesets read through this staging.
    read_changesets: HashMap<ChangesetId, Changeset>,
    /// Stored rows of kinds whose value never changes once written.
    read_rows: HashMap<(String, Hash), Bytes>,
}

impl Staged {
    fn put_row(&mut self, kind: &str, key: Hash, value: Bytes) {
        let k = (kind.to_string(), key);
        if self.rows.insert(k.clone(), value).is_none() {
            self.row_order.push(k);
        }
    }

    fn row(&self, kind: &str, key: &Hash) -> Option<Bytes> {
        let k = (kind.to_string(), *key);
        self.rows
            .get(&k)
            .or_else(|| self.read_rows.get(&k))
            .cloned()
    }

    fn changeset(&self, id: &ChangesetId) -> Option<Changeset> {
        self.changesets
            .get(id)
            .or_else(|| self.read_changesets.get(id))
            .cloned()
    }
}

/// Rows a staging may keep after reading them: written once with their
/// changeset and never changed, so a copy read earlier in the batch is still
/// the stored value. Rows that move (GC marks, claims) are always read.
fn immutable_kind(kind: &str) -> bool {
    kind == RootTree::NAME_STR || kind == Generation::NAME || kind == REBASED_ORIGIN_KIND
}

fn no_refs() -> Error {
    Error::Other(
        "a staged land batch cannot read or move refs other than through its leader".into(),
    )
}

impl Staging {
    fn lock(&self) -> MutexGuard<'_, Staged> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// A handle on `repo` (same caches and policy) whose writes stage here.
    pub(crate) fn repo(self: &Arc<Self>, repo: &Repo) -> Repo {
        repo.with_stores(
            Arc::new(StagedBlobs {
                inner: Arc::clone(repo.blobs()),
                staging: Arc::clone(self),
            }),
            Arc::new(StagedMeta {
                inner: Arc::clone(repo.meta()),
                staging: Arc::clone(self),
            }),
        )
    }

    /// Read the changesets a batch lands and the root trees of them and of
    /// their first parents (where this process knows the parents) in one
    /// round of batched reads, for the replays to find in memory. A failed
    /// read is left to the replays, which read singly.
    pub(crate) async fn prefetch(&self, repo: &Repo, ids: &[ChangesetId]) {
        let mut seen = HashSet::new();
        let keys: Vec<Hash> = ids
            .iter()
            .copied()
            .chain(
                ids.iter()
                    .filter_map(|id| repo.cached_parents(*id)?.first().copied()),
            )
            .map(|id| id.0)
            .filter(|k| seen.insert(*k))
            .collect();
        let meta = repo.meta();
        let (found, trees) = tokio::join!(
            meta.get_changesets(ids),
            meta.get_derived_many(RootTree::NAME_STR, &keys)
        );
        let mut s = self.lock();
        if let Ok(found) = found {
            for (id, cs) in ids.iter().zip(found) {
                if let Some(cs) = cs {
                    s.read_changesets.insert(*id, cs);
                }
            }
        }
        if let Ok(trees) = trees {
            for (k, v) in keys.into_iter().zip(trees) {
                if let Some(v) = v {
                    s.read_rows.insert((RootTree::NAME_STR.to_string(), k), v);
                }
            }
        }
    }

    /// Store what the batch wrote, in the order a lone write keeps: blobs
    /// (through the GC rescue check) before the changesets and rows that
    /// reference them, all of it before the caller moves the ref. Stopping
    /// part way leaves only unreferenced objects, as an interrupted lone land
    /// does.
    pub(crate) async fn flush(&self, repo: &Repo) -> Result<()> {
        let (blobs, changesets, rows) = {
            let s = self.lock();
            let blobs: Vec<(Hash, Bytes)> = s
                .blob_order
                .iter()
                .map(|h| (*h, s.blobs[h].clone()))
                .collect();
            let changesets: Vec<Changeset> = s
                .changeset_order
                .iter()
                .map(|id| s.changesets[id].clone())
                .collect();
            let rows: Vec<DerivedRow> = s
                .row_order
                .iter()
                .map(|k| DerivedRow::new(k.0.clone(), k.1, s.rows[k].clone()))
                .collect();
            (blobs, changesets, rows)
        };
        if !blobs.is_empty() {
            repo.put_rescuing_many_in_flight(blobs, FLUSH_WRITES_IN_FLIGHT)
                .await?;
        }
        if !changesets.is_empty() || !rows.is_empty() {
            repo.meta()
                .put_changesets_with_derived(&changesets, &rows)
                .await?;
        }
        Ok(())
    }
}

struct StagedBlobs {
    inner: Arc<dyn BlobStore>,
    staging: Arc<Staging>,
}

#[async_trait]
impl BlobStore for StagedBlobs {
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        let mut s = self.staging.lock();
        if s.blobs.insert(hash, data).is_none() {
            s.blob_order.push(hash);
        }
        Ok(())
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        if let Some(b) = self.staging.lock().blobs.get(hash) {
            return Ok(Some(b.clone()));
        }
        self.inner.get(hash).await
    }

    /// A staged blob counts: it is stored before anything that references it.
    async fn contains(&self, hash: &Hash) -> Result<bool> {
        if self.staging.lock().blobs.contains_key(hash) {
            return Ok(true);
        }
        self.inner.contains(hash).await
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.inner.list_page(after, limit).await
    }

    async fn delete(&self, _: &Hash) -> Result<()> {
        Err(Error::Other(
            "a staged land batch cannot delete blobs".into(),
        ))
    }
}

struct StagedMeta {
    inner: Arc<dyn MetaStore>,
    staging: Arc<Staging>,
}

#[async_trait]
impl MetaStore for StagedMeta {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        let id = cs.id();
        let mut s = self.staging.lock();
        if let std::collections::hash_map::Entry::Vacant(e) = s.changesets.entry(id) {
            e.insert(cs.clone());
            s.changeset_order.push(id);
        }
        Ok(id)
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        if let Some(cs) = self.staging.lock().changeset(id) {
            return Ok(Some(cs));
        }
        let cs = self.inner.get_changeset(id).await?;
        if let Some(cs) = &cs {
            self.staging.lock().read_changesets.insert(*id, cs.clone());
        }
        Ok(cs)
    }

    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        let mut out: Vec<Option<Changeset>> = {
            let s = self.staging.lock();
            ids.iter().map(|id| s.changeset(id)).collect()
        };
        let missing: Vec<ChangesetId> = ids
            .iter()
            .zip(&out)
            .filter(|(_, c)| c.is_none())
            .map(|(id, _)| *id)
            .collect();
        if missing.is_empty() {
            return Ok(out);
        }
        let mut read = self.inner.get_changesets(&missing).await?.into_iter();
        let mut s = self.staging.lock();
        for (id, slot) in ids.iter().zip(out.iter_mut()) {
            if slot.is_none() {
                *slot = read.next().flatten();
                if let Some(cs) = slot {
                    s.read_changesets.insert(*id, cs.clone());
                }
            }
        }
        Ok(out)
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
        _: &str,
        _: Option<ChangesetId>,
        _: Option<ChangesetId>,
        _: &RefUpdateContext,
    ) -> Result<bool> {
        Err(no_refs())
    }

    async fn cas_ref_logged_chain(
        &self,
        _: &str,
        _: Option<ChangesetId>,
        _: &[leep_core::RefStep],
    ) -> Result<usize> {
        Err(no_refs())
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.inner.ref_log(name, before_seq, limit).await
    }

    async fn list_refs(&self) -> Result<std::collections::BTreeMap<String, ChangesetId>> {
        self.inner.list_refs().await
    }

    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.inner.list_derived(kind, after, limit).await
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        Ok(self.get_derived_many(kind, &[*key]).await?.pop().flatten())
    }

    /// A staged blob reads as not condemned: its condemnation is checked
    /// when the batch stores it, after the write, as `put_rescuing` requires.
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let (mut out, skip): (Vec<Option<Bytes>>, Vec<bool>) = {
            let s = self.staging.lock();
            keys.iter()
                .map(|k| {
                    let staged_blob = kind == BLOB_CONDEMNED && s.blobs.contains_key(k);
                    (s.row(kind, k), staged_blob)
                })
                .unzip()
        };
        let missing: Vec<Hash> = keys
            .iter()
            .zip(out.iter().zip(&skip))
            .filter(|(_, (v, skip))| v.is_none() && !**skip)
            .map(|(k, _)| *k)
            .collect();
        if missing.is_empty() {
            return Ok(out);
        }
        let mut read = self
            .inner
            .get_derived_many(kind, &missing)
            .await?
            .into_iter();
        let keep = immutable_kind(kind);
        let mut s = self.staging.lock();
        for ((k, slot), skip) in keys.iter().zip(out.iter_mut()).zip(skip) {
            if slot.is_none() && !skip {
                *slot = read.next().flatten();
                if let (true, Some(v)) = (keep, slot.as_ref()) {
                    s.read_rows.insert((kind.to_string(), *k), v.clone());
                }
            }
        }
        Ok(out)
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.staging.lock().put_row(kind, *key, value);
        Ok(())
    }

    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        let mut s = self.staging.lock();
        for r in rows {
            s.put_row(&r.kind, r.key, r.value.clone());
        }
        Ok(())
    }

    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        let id = self.put_changeset(cs).await?;
        self.put_derived_many(rows).await?;
        Ok(id)
    }

    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        for cs in changesets {
            self.put_changeset(cs).await?;
        }
        self.put_derived_many(rows).await
    }

    async fn schema_version(&self) -> Result<Option<u64>> {
        self.inner.schema_version().await
    }
}
