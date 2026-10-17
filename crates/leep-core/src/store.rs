//! Storage contracts. Implementations live in `leep-store` (blobs) and
//! `leep-meta` (changesets, refs, derived data); in-memory versions for tests
//! live in [`crate::memory`].

use std::collections::BTreeMap;

use async_trait::async_trait;
use bytes::Bytes;
use serde::{Deserialize, Serialize};

use crate::prefix::IdPrefix;
use crate::{Changeset, ChangesetId, Error, Hash, Result};

/// Unix seconds now, the server clock every ref log entry is stamped with.
pub fn unix_now() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Who moved a ref and why, recorded in the ref log with the move itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefUpdateContext {
    /// Token name of the caller, or `None` when authentication is off.
    pub actor: Option<String>,
    /// Short free-form label: `land`, `cas`, `undo`, `import`, ...
    pub operation: String,
    pub message: Option<String>,
}

impl RefUpdateContext {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            actor: None,
            operation: operation.into(),
            message: None,
        }
    }

    pub fn actor(mut self, actor: Option<impl Into<String>>) -> Self {
        self.actor = actor.map(Into::into);
        self
    }

    pub fn message(mut self, message: impl Into<String>) -> Self {
        self.message = Some(message.into());
        self
    }
}

/// One move of [`MetaStore::cas_ref_logged_chain`]: where the ref goes next
/// and the context its log entry records.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefStep {
    pub new: Option<ChangesetId>,
    pub ctx: RefUpdateContext,
}

/// One recorded ref move. `old = None` is a create, `new = None` a delete.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RefLogEntry {
    /// Increases with every entry in the store, so it orders moves across refs.
    pub seq: u64,
    pub name: String,
    pub old: Option<ChangesetId>,
    pub new: Option<ChangesetId>,
    /// Unix seconds by the server that applied the move.
    pub time: i64,
    pub actor: Option<String>,
    pub operation: String,
    #[serde(default)]
    pub message: Option<String>,
}

/// One derived row to write: `(kind, key) -> value`, as in
/// [`MetaStore::put_derived`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DerivedRow {
    pub kind: String,
    pub key: Hash,
    pub value: Bytes,
}

impl DerivedRow {
    pub fn new(kind: impl Into<String>, key: Hash, value: Bytes) -> Self {
        Self {
            kind: kind.into(),
            key,
            value,
        }
    }
}

/// What a blob listing reports about one stored blob.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BlobMeta {
    pub hash: Hash,
    pub size: u64,
    /// Last write time (unix seconds), refreshed when the same blob is put
    /// again. `None` if the backend cannot tell; garbage collection treats
    /// those blobs as too young to delete.
    pub modified: Option<i64>,
}

/// Immutable content-addressed bytes: file contents and tree nodes.
#[async_trait]
pub trait BlobStore: Send + Sync + 'static {
    /// Store file contents; returns `Hash::of_blob(data)`. Idempotent.
    async fn put(&self, data: Bytes) -> Result<Hash> {
        let h = Hash::of_blob(&data);
        self.put_with_hash(h, data).await?;
        Ok(h)
    }

    /// Store bytes under a caller-computed address (used for tree nodes and
    /// other derived objects). Callers guarantee `hash` is correct for `data`.
    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()>;

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>>;

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        Ok(self.get(hash).await?.is_some())
    }

    /// Up to `limit` blobs with hash strictly greater than `after`, in
    /// ascending hash order. Paged so garbage collection can scan stores too
    /// large to list at once.
    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>>;

    /// Remove a blob. Idempotent: deleting an absent blob succeeds. Only
    /// garbage collection should call this.
    async fn delete(&self, hash: &Hash) -> Result<()>;
}

/// Changesets, refs and derived data. Changesets and derived data are
/// immutable once written; refs are the only mutable state.
#[async_trait]
pub trait MetaStore: Send + Sync + 'static {
    /// Store a changeset. Idempotent; returns its id.
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId>;

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>>;

    /// [`get_changeset`](Self::get_changeset) for many ids in one round trip,
    /// so history walks pay one call per batch instead of one per changeset
    /// (ADR 0030). The answer has one slot per id, in order: `None` for an
    /// absent id, and a repeated id is answered at every position.
    ///
    /// The default makes one call per id; database backends override it.
    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        let mut out = Vec::with_capacity(ids.len());
        for id in ids {
            out.push(self.get_changeset(id).await?);
        }
        Ok(out)
    }

    /// Up to `limit` changeset ids strictly greater than `after`, in
    /// ascending byte order. Paged for garbage collection and other full
    /// scans.
    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>>;

    /// Up to `limit` changeset ids that start with `prefix`, in ascending
    /// byte order: how a short id is resolved (ADR 0035). Asking for two
    /// tells a unique prefix from an ambiguous one.
    ///
    /// The default reads one page of [`list_changesets`](Self::list_changesets)
    /// from just below the prefix's range, which is already a range scan on
    /// stores that order ids; database backends override it with a bounded
    /// range query.
    async fn changesets_with_prefix(
        &self,
        prefix: &IdPrefix,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        if limit == 0 {
            return Ok(Vec::new());
        }
        let page = self.list_changesets(prefix.before_lower(), limit).await?;
        Ok(page
            .into_iter()
            .take_while(|id| prefix.matches(id))
            .collect())
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>>;

    /// [`cas_ref_logged`](Self::cas_ref_logged) with no actor and the
    /// operation `cas`. For callers that have no better context.
    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        self.cas_ref_logged(name, expected, new, &RefUpdateContext::new("cas"))
            .await
    }

    /// Atomically set `name` to `new` iff it currently equals `expected`
    /// (`None` = absent). `new = None` deletes the ref. Returns whether the
    /// swap happened.
    ///
    /// A swap that happens appends one [`RefLogEntry`] in the same atomic
    /// step, so a ref never moves unrecorded. A lost swap, and a swap that
    /// changes nothing (`expected == new`), write no entry.
    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool>;

    /// Move `name` from `expected` through every step in order, logging one
    /// entry per step whose `old` is the previous step's `new` (the first's is
    /// `expected`), as if each step were its own swap. Group commit of lands
    /// uses it so that each land keeps its own entry, actor and message while
    /// the ref moves once.
    ///
    /// Returns how many steps applied, always a prefix: `0` when `name` is not
    /// `expected`. Every step must change the ref, and the last must not
    /// return it to `expected`; anything else is `Invalid`.
    ///
    /// Backends override this to apply every step in one atomic step, so the
    /// answer is `0` or `steps.len()`. The default applies the steps as
    /// consecutive [`cas_ref_logged`](Self::cas_ref_logged) calls, which stays
    /// correct but lets readers see the intermediate values and can stop part
    /// way if another writer moves the ref in between.
    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        check_ref_chain(expected, steps)?;
        let mut old = expected;
        for (done, step) in steps.iter().enumerate() {
            if !self.cas_ref_logged(name, old, step.new, &step.ctx).await? {
                return Ok(done);
            }
            old = step.new;
        }
        Ok(steps.len())
    }

    /// Ref log entries, newest first: those of `name` or of every ref, with
    /// `seq` below `before_seq` when given, at most `limit`.
    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>>;

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>>;

    /// Up to `limit` derived rows of `kind` with key strictly greater than
    /// `after`, in ascending key (byte) order. Paged so verification and
    /// repair can scan a kind too large to hold in memory.
    ///
    /// Every backend must implement this; the default exists only so test
    /// doubles that never list rows need not.
    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        let _ = (kind, after, limit);
        Err(Error::Invalid(
            "this metadata store cannot list derived rows".into(),
        ))
    }

    /// Delete every row of `kind` and return how many were removed. Idempotent,
    /// and rows of other kinds are untouched. Only the repair tools call it,
    /// and only for kinds that derive again from history (ADR 0025); in
    /// bounded batches, so an interrupted call leaves the kind partly dropped.
    ///
    /// Every backend must implement this (see [`list_derived`](Self::list_derived)).
    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        let _ = kind;
        Err(Error::Invalid(
            "this metadata store cannot delete derived rows".into(),
        ))
    }

    /// Derived data is keyed by (kind, key), e.g. ("root_tree", changeset id).
    /// Values are opaque bytes owned by `leep-derive`. Writing the same key
    /// twice must store the same value, so last-writer-wins is fine.
    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>>;

    /// [`get_derived`](Self::get_derived) of one kind for many keys in one
    /// round trip (ADR 0030). One slot per key, in order, as in
    /// [`get_changesets`](Self::get_changesets).
    ///
    /// The default makes one call per key; database backends override it.
    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let mut out = Vec::with_capacity(keys.len());
        for key in keys {
            out.push(self.get_derived(kind, key).await?);
        }
        Ok(out)
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()>;

    /// [`put_derived`](Self::put_derived) for many rows in one round trip
    /// (ADR 0031). The result is that of putting the rows one by one in
    /// order, so a key given twice keeps its last value.
    ///
    /// The default makes one call per row; database backends override it with
    /// one atomic statement or transaction. Callers must not depend on that:
    /// after an error any prefix of the rows may be stored.
    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        for r in rows {
            self.put_derived(&r.kind, &r.key, r.value.clone()).await?;
        }
        Ok(())
    }

    /// Store a changeset together with derived rows computed for it, in one
    /// round trip (ADR 0031). Idempotent like [`put_changeset`](Self::put_changeset);
    /// the rows are written as by [`put_derived_many`](Self::put_derived_many)
    /// even when the changeset was already stored.
    ///
    /// Database backends override it with one atomic write, so the changeset
    /// is never visible without its rows. The default writes the changeset,
    /// then the rows in order, the order changeset creation always used: a
    /// failure in between leaves the changeset with some rows missing, which
    /// derivation fills in later. Callers must stay correct under either.
    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        let id = self.put_changeset(cs).await?;
        self.put_derived_many(rows).await?;
        Ok(id)
    }

    /// Version of the newest schema migration applied (ADR 0023), for
    /// diagnostics. `None` for a store without a versioned schema.
    async fn schema_version(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}

/// The rules every [`MetaStore::cas_ref_logged_chain`] applies before moving
/// anything: steps exist, each changes the ref, and the chain does not end
/// where it started.
pub fn check_ref_chain(expected: Option<ChangesetId>, steps: &[RefStep]) -> Result<()> {
    let mut old = expected;
    for step in steps {
        if step.new == old {
            return Err(Error::Invalid("a ref chain step must move the ref".into()));
        }
        old = step.new;
    }
    match steps.last() {
        None => Err(Error::Invalid("a ref chain needs at least one step".into())),
        Some(last) if last.new == expected => Err(Error::Invalid(
            "a ref chain must not end where it started".into(),
        )),
        Some(_) => Ok(()),
    }
}
