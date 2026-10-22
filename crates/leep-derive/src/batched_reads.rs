//! Reads made at once by many tasks, sent to the stores as batched reads.
//!
//! Blame walks dozens of stretches of history at once (see `blame.rs`), and
//! each step of each walk reads a few rows through code that reads one row
//! at a time (the path-history index, skiplists, generations), and each
//! stretch reads files in batches of its own. One query a row, or a small
//! batch per stretch, caps the walk at what the store answers a query at a
//! time, a few thousand a second on a busy Postgres, however many walks run;
//! and each small blob batch spreads over blocking tasks of its own. Here
//! the reads that arrive while a batch is out are gathered into the next
//! one, so the walks advance a step per round trip together.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::prefix::IdPrefix;
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, DeleteStep, DerivedRow, Error, Hash, MetaStore,
    RefLogEntry, RefStep, RefUpdateContext, Result,
};
use tokio::sync::oneshot;

/// Batches out at once. While they are, reads queue for the next.
const IN_FLIGHT: usize = 2;

/// Keys one batch asks for, at most (but one read asking for more is sent
/// whole).
const MAX_BATCH: usize = 1024;

/// Answers to one batch of keys of one group.
type Answer = Result<Vec<Option<Bytes>>>;

/// Where gathered reads go: one batched read of keys of one group (a
/// derived kind, or blobs).
#[async_trait]
trait Source: Send + Sync {
    async fn fetch(&self, group: &str, keys: &[Hash]) -> Answer;
}

/// Reads queued for the next batch, and who sends batches.
#[derive(Default)]
struct Gather {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    queue: Vec<Waiting>,
    /// Tasks sending batches.
    flushing: usize,
}

struct Waiting {
    group: String,
    keys: Vec<Hash>,
    tx: oneshot::Sender<Answer>,
}

impl Gather {
    /// The values of `keys` of `group`, read in a batch with whatever else
    /// is asked for meanwhile.
    async fn read(&self, source: &dyn Source, group: &str, keys: &[Hash]) -> Answer {
        if keys.is_empty() {
            return Ok(Vec::new());
        }
        let (tx, rx) = oneshot::channel();
        let lead = {
            let mut s = self.state.lock().unwrap();
            s.queue.push(Waiting {
                group: group.to_string(),
                keys: keys.to_vec(),
                tx,
            });
            let lead = s.flushing < IN_FLIGHT;
            if lead {
                s.flushing += 1;
            }
            lead
        };
        if lead {
            // Let the tasks ready to run queue their reads first.
            tokio::task::yield_now().await;
            self.flush(source).await;
        }
        rx.await
            .map_err(|_| Error::Other("a batched read was dropped".into()))?
    }

    /// Send batches until the queue is empty. Called by a reader that found
    /// fewer than [`IN_FLIGHT`] batches out; others wait for it.
    async fn flush(&self, source: &dyn Source) {
        // Gives the flusher's place back if the reading task is dropped
        // mid-batch (that batch's waiters then see an error). Otherwise the
        // place is given back under the same lock that finds the queue
        // empty, so a read queued just after is never left without one.
        struct Flushing<'a>(&'a Mutex<State>, bool);
        impl Drop for Flushing<'_> {
            fn drop(&mut self) {
                if self.1 {
                    self.0.lock().unwrap().flushing -= 1;
                }
            }
        }
        let mut flushing = Flushing(&self.state, true);
        loop {
            let batch: Vec<Waiting> = {
                let mut s = self.state.lock().unwrap();
                if s.queue.is_empty() {
                    s.flushing -= 1;
                    flushing.1 = false;
                    return;
                }
                let (mut n, mut keys) = (0, 0);
                while n < s.queue.len() && (n == 0 || keys + s.queue[n].keys.len() <= MAX_BATCH) {
                    keys += s.queue[n].keys.len();
                    n += 1;
                }
                s.queue.drain(..n).collect()
            };
            let mut by_group: BTreeMap<String, Vec<Waiting>> = BTreeMap::new();
            for w in batch {
                by_group.entry(w.group.clone()).or_default().push(w);
            }
            let reads = by_group.into_iter().map(|(group, waiting)| async move {
                let keys: Vec<Hash> = waiting.iter().flat_map(|w| w.keys.clone()).collect();
                match source.fetch(&group, &keys).await {
                    Ok(found) if found.len() == keys.len() => {
                        let mut found = found.into_iter();
                        for w in waiting {
                            let mine = found.by_ref().take(w.keys.len()).collect();
                            let _ = w.tx.send(Ok(mine));
                        }
                    }
                    Ok(found) => {
                        for w in waiting {
                            let _ = w.tx.send(Err(Error::Storage(format!(
                                "a batched read answered {} of {} keys",
                                found.len(),
                                keys.len()
                            ))));
                        }
                    }
                    Err(e) => {
                        for w in waiting {
                            let _ = w.tx.send(Err(copy(&e)));
                        }
                    }
                }
            });
            futures_util::future::join_all(reads).await;
        }
    }
}

/// The same error again, for each read of a failed batch.
fn copy(e: &Error) -> Error {
    match e {
        Error::InvalidPath(p, why) => Error::InvalidPath(p.clone(), why),
        Error::InvalidHash(m) => Error::InvalidHash(m.clone()),
        Error::Invalid(m) => Error::Invalid(m.clone()),
        Error::Conflict(m) => Error::Conflict(m.clone()),
        Error::LandIdReused { refusal, requested } => Error::LandIdReused {
            refusal: refusal.clone(),
            requested: *requested,
        },
        Error::NotFound(m) => Error::NotFound(m.clone()),
        Error::Encoding(m) => Error::Encoding(m.clone()),
        Error::Storage(m) => Error::Storage(m.clone()),
        Error::Other(m) => Error::Other(m.clone()),
        Error::Indexing {
            kind,
            behind,
            retry_after_ms,
        } => Error::Indexing {
            kind: kind.clone(),
            behind: *behind,
            retry_after_ms: *retry_after_ms,
        },
        Error::Refused {
            refused,
            retry_after_ms,
            message,
        } => Error::Refused {
            refused,
            retry_after_ms: *retry_after_ms,
            message: message.clone(),
        },
    }
}

/// A [`MetaStore`] that gathers concurrent `get_derived` and
/// `get_derived_many` calls into `get_derived_many` calls on `inner`;
/// everything else goes straight through.
pub(crate) struct BatchedMeta {
    inner: Arc<dyn MetaStore>,
    gather: Gather,
}

impl BatchedMeta {
    pub(crate) fn new(inner: Arc<dyn MetaStore>) -> Self {
        BatchedMeta {
            inner,
            gather: Gather::default(),
        }
    }
}

#[async_trait]
impl Source for BatchedMeta {
    async fn fetch(&self, kind: &str, keys: &[Hash]) -> Answer {
        self.inner.get_derived_many(kind, keys).await
    }
}

#[async_trait]
impl MetaStore for BatchedMeta {
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        let mut found = self.gather.read(self, kind, &[*key]).await?;
        Ok(found.pop().flatten())
    }

    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.gather.read(self, kind, keys).await
    }

    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.inner.put_changeset(cs).await
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.inner.get_changeset(id).await
    }

    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.inner.get_changesets(ids).await
    }

    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.list_changesets(after, limit).await
    }

    async fn changesets_with_prefix(
        &self,
        prefix: &IdPrefix,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.inner.changesets_with_prefix(prefix, limit).await
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.inner.get_ref(name).await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        self.inner.cas_ref(name, expected, new).await
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

    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.inner.cas_ref_logged_chain(name, expected, steps).await
    }

    async fn find_lands(
        &self,

        name: &str,

        ids: &[leep_core::LandId],
    ) -> Result<Vec<Option<leep_core::LandRecord>>> {
        self.inner.find_lands(name, ids).await
    }

    async fn changesets_stored_at(&self, ids: &[ChangesetId]) -> Result<Vec<Option<u64>>> {
        self.inner.changesets_stored_at(ids).await
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

    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.inner.list_derived(kind, after, limit).await
    }

    async fn count_derived(&self, kind: &str) -> Result<u64> {
        self.inner.count_derived(kind).await
    }

    async fn estimate_derived(&self, kind: &str) -> Result<leep_core::RowCount> {
        self.inner.estimate_derived(kind).await
    }

    async fn delete_derived_step(
        &self,
        kinds: &[&str],
        cursor: Option<&[u8]>,
    ) -> Result<DeleteStep> {
        self.inner.delete_derived_step(kinds, cursor).await
    }

    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        self.inner.delete_derived_kind(kind).await
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        self.inner.put_derived(kind, key, value).await
    }

    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        self.inner.put_derived_many(rows).await
    }

    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        self.inner.put_changeset_with_derived(cs, rows).await
    }

    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        self.inner
            .put_changesets_with_derived(changesets, rows)
            .await
    }

    async fn schema_version(&self) -> Result<Option<u64>> {
        self.inner.schema_version().await
    }
}

/// A [`BlobStore`] that gathers concurrent `get_many` calls into larger
/// `get_many` calls on `inner`; everything else goes straight through.
pub(crate) struct BatchedBlobs {
    inner: Arc<dyn BlobStore>,
    gather: Gather,
}

impl BatchedBlobs {
    pub(crate) fn new(inner: Arc<dyn BlobStore>) -> Self {
        BatchedBlobs {
            inner,
            gather: Gather::default(),
        }
    }
}

#[async_trait]
impl Source for BatchedBlobs {
    async fn fetch(&self, _: &str, keys: &[Hash]) -> Answer {
        self.inner.get_many(keys).await
    }
}

#[async_trait]
impl BlobStore for BatchedBlobs {
    async fn get_many(&self, hashes: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.gather.read(self, "", hashes).await
    }

    async fn put(&self, data: Bytes) -> Result<Hash> {
        self.inner.put(data).await
    }

    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.inner.put_with_hash(hash, data).await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.inner.get(hash).await
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        self.inner.contains(hash).await
    }

    async fn contains_many(&self, hashes: &[Hash]) -> Result<Vec<bool>> {
        self.inner.contains_many(hashes).await
    }

    async fn flush(&self, named: &[Hash]) -> Result<()> {
        self.inner.flush(named).await
    }

    async fn flush_blobs(&self, hashes: &[Hash]) -> Result<()> {
        self.inner.flush_blobs(hashes).await
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.inner.list_page(after, limit).await
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.inner.delete(hash).await
    }
}
