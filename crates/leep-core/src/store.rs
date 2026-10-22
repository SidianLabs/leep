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

/// A client's name for one logical land (ADR 0057). The server records it
/// with the ref move, so a retry of the land is recognised at any age
/// instead of only while its rebased copy is near the head. 16 to 32 bytes,
/// lowercase hex on the wire; clients pick it at random.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LandId(Vec<u8>);

impl LandId {
    pub const MIN_LEN: usize = 16;
    pub const MAX_LEN: usize = 32;

    pub fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if !(Self::MIN_LEN..=Self::MAX_LEN).contains(&bytes.len()) {
            return Err(Error::Invalid(format!(
                "a land id is {} to {} bytes, not {}",
                Self::MIN_LEN,
                Self::MAX_LEN,
                bytes.len()
            )));
        }
        Ok(Self(bytes.to_vec()))
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

impl std::fmt::Display for LandId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&hex::encode(&self.0))
    }
}

impl std::str::FromStr for LandId {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self> {
        // Lowercase only, so one id has one spelling.
        if s.bytes().any(|b| b.is_ascii_uppercase()) {
            return Err(Error::Invalid(format!(
                "land id {s:?} is not lowercase hex"
            )));
        }
        let bytes =
            hex::decode(s).map_err(|_| Error::Invalid(format!("land id {s:?} is not hex")))?;
        Self::from_bytes(&bytes)
    }
}

impl Serialize for LandId {
    fn serialize<S: serde::Serializer>(&self, s: S) -> std::result::Result<S::Ok, S::Error> {
        s.collect_str(self)
    }
}

impl<'de> Deserialize<'de> for LandId {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> std::result::Result<Self, D::Error> {
        let s = String::deserialize(d)?;
        s.parse().map_err(serde::de::Error::custom)
    }
}

/// The land a ref move completes: the client's id for it and the changeset
/// it asked to land. Stored with the move's log entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LandKey {
    pub id: LandId,
    pub changeset: ChangesetId,
}

/// What [`MetaStore::find_lands`] found for one land id.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LandRecord {
    /// The changeset the land asked for.
    pub changeset: ChangesetId,
    /// Where the move took the ref: `changeset` or its rebased copy.
    pub landed: ChangesetId,
    /// The `seq` of the move's log entry.
    pub seq: u64,
}

/// Timing of lease calls, measured by the store's clock. Each scope picks
/// its own (land coordination: 2 s TTL renewed every 500 ms, ADR 0056).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct LeaseTiming {
    /// How long a lease stays live after it is taken or renewed.
    pub ttl: std::time::Duration,
    /// Another holder may take a lease whose last renewal is older than
    /// this, even before `ttl` runs out. Land coordination passes `ttl`.
    pub stale_after: std::time::Duration,
}

/// The scope of land coordination leases (ADR 0056): one per ref, shard 0.
pub const LAND_LEASE_SCOPE: &str = "land";

/// The scope of index leases (ADR 0058): one per index group, named by its
/// primary kind, in repository 0 because index rows are shared by every
/// repository of a store (ADR 0054).
pub const INDEX_LEASE_SCOPE: &str = "index";

/// The scope of the garbage collection lease (ADR 0059 section 6): one,
/// named [`GC_LEASE_NAME`], store-wide, so blob GC and the changeset sweep
/// never run at once.
pub const GC_LEASE_SCOPE: &str = "gc";

/// The name of the one [`GC_LEASE_SCOPE`] lease.
pub const GC_LEASE_NAME: &str = "store";

/// Names one lease: what kind of work it coordinates (`scope`), in which
/// repository (1 until ADR 0054), over what (`name`, for example a ref) and
/// which part of it (`shard`).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct LeaseKey {
    pub scope: String,
    pub repo_id: i64,
    pub name: String,
    pub shard: i32,
}

impl LeaseKey {
    /// Shard 0 of `name` in `scope`, in the default repository.
    pub fn new(scope: impl Into<String>, name: impl Into<String>) -> Self {
        LeaseKey {
            scope: scope.into(),
            repo_id: 1,
            name: name.into(),
            shard: 0,
        }
    }

    /// The land coordination lease of `ref_name`.
    pub fn land(ref_name: impl Into<String>) -> Self {
        Self::new(LAND_LEASE_SCOPE, ref_name)
    }

    /// The index lease of the group whose primary kind is `kind` (ADR 0058):
    /// store-wide, so repository 0, and shard 0 until shards are needed.
    pub fn index(kind: impl Into<String>) -> Self {
        LeaseKey {
            scope: INDEX_LEASE_SCOPE.into(),
            repo_id: 0,
            name: kind.into(),
            shard: 0,
        }
    }

    /// The garbage collection lease: store-wide, so repository 0.
    pub fn gc() -> Self {
        LeaseKey {
            scope: GC_LEASE_SCOPE.into(),
            repo_id: 0,
            name: GC_LEASE_NAME.into(),
            shard: 0,
        }
    }

    pub fn with_shard(mut self, shard: i32) -> Self {
        self.shard = shard;
        self
    }
}

/// Which server holds one lease (ADR 0056). A lease only decides where work
/// is done; whatever it protects must stay correct when two servers both
/// believe they hold it (refs, for one, move only by compare-and-swap).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lease {
    pub key: LeaseKey,
    /// Random per server process start.
    pub holder: String,
    /// Where the holder takes requests for the work (its `--advertise-url`).
    pub address: String,
    /// Protocol the holder speaks for this scope.
    pub protocol: u32,
    /// Scope-specific detail the holder publishes; empty for lands.
    pub meta: String,
    /// Counts holders: changes whenever another holder takes the lease.
    pub epoch: u64,
    /// Time until another server may take it, by the store's clock at the
    /// call. Zero means it may be taken now.
    pub remaining: std::time::Duration,
}

impl Lease {
    /// Whether another server must leave it alone.
    pub fn is_live(&self) -> bool {
        !self.remaining.is_zero()
    }
}

/// A server asking for a lease.
#[derive(Clone, Copy, Debug)]
pub struct LeaseClaim<'a> {
    pub key: &'a LeaseKey,
    pub holder: &'a str,
    pub address: &'a str,
    pub protocol: u32,
    pub meta: &'a str,
}

fn no_leases() -> Error {
    Error::Invalid("this metadata store does not support leases".into())
}

/// When a lease row may be taken by another holder, and how long is left
/// until then, from its two timestamps (store clock, milliseconds).
pub fn lease_remaining_ms(now: i64, renewed_at: i64, expires_at: i64, timing: LeaseTiming) -> i64 {
    let stale = i64::try_from(timing.stale_after.as_millis()).unwrap_or(i64::MAX);
    expires_at
        .min(renewed_at.saturating_add(stale))
        .saturating_sub(now)
        .max(0)
}

/// Who moved a ref and why, recorded in the ref log with the move itself.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RefUpdateContext {
    /// Token name of the caller, or `None` when authentication is off.
    pub actor: Option<String>,
    /// Short free-form label: `land`, `cas`, `undo`, `import`, ...
    pub operation: String,
    pub message: Option<String>,
    /// The client land this move completes, if the client named it.
    pub land: Option<LandKey>,
}

impl RefUpdateContext {
    pub fn new(operation: impl Into<String>) -> Self {
        Self {
            actor: None,
            operation: operation.into(),
            message: None,
            land: None,
        }
    }

    pub fn land(mut self, land: Option<LandKey>) -> Self {
        self.land = land;
        self
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
/// JSON on the wire (`GET /v1/reflog`) and SQL columns at rest; never hashed
/// or encoded with postcard, so it may grow optional fields.
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
    /// The client's id for the land this move completed (ADR 0057), when it
    /// named one. Absent otherwise and from older servers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub land_id: Option<LandId>,
    /// The changeset that land asked for; `new` is it or its rebased copy.
    /// Set exactly when `land_id` is.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub land_changeset: Option<ChangesetId>,
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

/// What [`MetaStore::estimate_derived`] found.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RowCount {
    /// Rows; 0 only when there are none, even when estimated.
    pub rows: u64,
    /// Counted, not estimated.
    pub exact: bool,
}

/// One bounded step of [`MetaStore::delete_derived_step`].
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DeleteStep {
    /// Rows this step deleted, one count per kind asked for, in that order.
    pub deleted: Vec<u64>,
    /// The cursor for the next step; `None` once the kinds are gone.
    pub next: Option<Vec<u8>>,
    /// How far the whole delete has got, `done` of `total` in `unit`s
    /// ("rows", "pages": whatever the backend walks), for progress and ETA.
    pub done: u64,
    pub total: u64,
    pub unit: &'static str,
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

/// How many `contains` calls the default [`BlobStore::contains_many`] keeps
/// in flight.
pub const CONTAINS_IN_FLIGHT: usize = 64;

/// How many `get` calls the default [`BlobStore::get_many`] keeps in flight.
/// Bounded so a batch of large blobs does not hold all of them at once
/// before the first is used.
pub const GET_IN_FLIGHT: usize = 32;

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

    /// [`get`](Self::get) for many hashes, answered in order and each as
    /// exactly. Walks that know a whole level of a tree before reading any of
    /// it call this, so a store whose reads wait on a disk or a network can
    /// have many in flight instead of one. The default asks
    /// [`GET_IN_FLIGHT`] at a time; backends that can do better override
    /// it.
    async fn get_many(&self, hashes: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let mut out = Vec::with_capacity(hashes.len());
        for chunk in hashes.chunks(GET_IN_FLIGHT) {
            let mut asks = Vec::with_capacity(chunk.len());
            for h in chunk {
                asks.push(self.get(h));
            }
            out.extend(futures_util::future::try_join_all(asks).await?);
        }
        Ok(out)
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        Ok(self.get(hash).await?.is_some())
    }

    /// [`contains`](Self::contains) for many hashes, answered in order and
    /// each as exactly. The default asks [`CONTAINS_IN_FLIGHT`] at a time;
    /// backends that can answer a batch more cheaply override it.
    async fn contains_many(&self, hashes: &[Hash]) -> Result<Vec<bool>> {
        let mut out = Vec::with_capacity(hashes.len());
        for chunk in hashes.chunks(CONTAINS_IN_FLIGHT) {
            let mut asks = Vec::with_capacity(chunk.len());
            for h in chunk {
                asks.push(self.contains(h));
            }
            out.extend(futures_util::future::try_join_all(asks).await?);
        }
        Ok(out)
    }

    /// Make durable, before returning, every blob this store has written, or
    /// found already stored, since the last flush, and the stored copies of
    /// `named`. A backend may acknowledge a put before the blob would survive
    /// a power loss (the filesystem store defers its directory syncs, ADR
    /// 0044); callers flush before acknowledging anything that depends on
    /// those blobs, such as storing a changeset that names them or sealing a
    /// backup. `named` covers blobs another process, or this one before a
    /// restart, wrote. The default does nothing, for backends whose puts are
    /// durable when they return.
    async fn flush(&self, named: &[Hash]) -> Result<()> {
        let _ = named;
        Ok(())
    }

    /// Make the stored copies of `hashes` durable before returning, without
    /// waiting for anything else written since the last flush. Callers that
    /// know every blob a write depends on (the nodes they wrote and the
    /// contents they name) use this instead of [`flush`](Self::flush), so a
    /// create does not wait for other requests' uploads (ADR 0048). The
    /// default is `flush(hashes)`, which covers at least as much.
    async fn flush_blobs(&self, hashes: &[Hash]) -> Result<()> {
        self.flush(hashes).await
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

    /// When each of `ids` was first stored, in unix seconds by the store's
    /// writer (one slot per id, in order; `None` for an absent id). It ages
    /// changesets for garbage collection (ADR 0059), which must not trust
    /// `Changeset::timestamp`: the client sets that. Every `put_changeset*`
    /// records the time on the first store and keeps it on later ones. A
    /// changeset stored before a backend recorded times reads as stored when
    /// the backend started recording them (its migration's `applied_at`).
    ///
    /// Every backend must implement this (see
    /// [`list_derived`](Self::list_derived)): a wrapper that does not forward
    /// it fails loudly instead of making every changeset look new or old.
    async fn changesets_stored_at(&self, ids: &[ChangesetId]) -> Result<Vec<Option<u64>>> {
        let _ = ids;
        Err(Error::Invalid(
            "this metadata store cannot report when changesets were stored".into(),
        ))
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
    ///
    /// `ctx.land` is recorded with the entry. A land key on a delete is
    /// `Invalid`; one whose id `name` already recorded makes a swap that
    /// would happen fail with `Conflict` and change nothing.
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

    /// The moves of `name` recorded under each of `ids` (one answer per id,
    /// in order), at any age: a move whose context carries a
    /// [`LandKey`] stores it with its log entry, atomically with the move,
    /// and keeps it as long as the entry (ADR 0057).
    ///
    /// Every backend must implement this (see
    /// [`list_derived`](Self::list_derived)); `land` calls it only for lands
    /// that carry an id.
    async fn find_lands(&self, name: &str, ids: &[LandId]) -> Result<Vec<Option<LandRecord>>> {
        let _ = (name, ids);
        Err(Error::Invalid(
            "this metadata store cannot look up land ids".into(),
        ))
    }

    /// Delete the changesets `ids` and their derived rows of `kinds` keyed by
    /// the same ids, in one transaction: a reader or a backup snapshot sees
    /// all of it or none. Ids not stored are skipped, so a repeat is a no-op.
    /// Returns how many changesets were deleted. Only the changeset sweep
    /// calls it (ADR 0059), and only for changesets it has condemned and
    /// fenced; it deletes children before parents across calls.
    ///
    /// Every backend must implement this. The default refuses, so a wrapper
    /// that does not forward it fails loudly instead of deleting nothing.
    async fn delete_changesets(&self, ids: &[ChangesetId], kinds: &[&str]) -> Result<u64> {
        let _ = (ids, kinds);
        Err(Error::Invalid(
            "this metadata store cannot delete changesets".into(),
        ))
    }

    /// Delete the derived rows of `kinds` under each of `keys`, in one
    /// transaction; absent rows are skipped. Returns how many were deleted.
    /// The changeset sweep's tombstone pass removes rows written for a
    /// changeset after it was deleted (ADR 0059 section 3).
    ///
    /// Every backend must implement this (see
    /// [`delete_changesets`](Self::delete_changesets)).
    async fn delete_derived_keys(&self, kinds: &[&str], keys: &[Hash]) -> Result<u64> {
        let _ = (kinds, keys);
        Err(Error::Invalid(
            "this metadata store cannot delete derived rows by key".into(),
        ))
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

    /// How many rows of `kind` are stored. Database backends count without
    /// reading values (ADR 0052); the default pages through
    /// [`list_derived`](Self::list_derived).
    async fn count_derived(&self, kind: &str) -> Result<u64> {
        let mut n = 0;
        let mut after = None;
        loop {
            let page = self.list_derived(kind, after, 1000).await?;
            let Some((last, _)) = page.last() else {
                return Ok(n);
            };
            after = Some(*last);
            n += page.len() as u64;
        }
    }

    /// About how many rows of `kind` are stored, for plans and progress
    /// that should not scan a large kind to count it. Whether there are any
    /// is always exact. The default counts.
    async fn estimate_derived(&self, kind: &str) -> Result<RowCount> {
        Ok(RowCount {
            rows: self.count_derived(kind).await?,
            exact: true,
        })
    }

    /// Delete every row of `kinds`, one bounded step at a time (ADR 0052):
    /// start with `cursor` `None` and pass each step's `next` back until it
    /// is `None`. Each step is short and commits on its own, so the delete
    /// runs beside servers and an interrupted one is finished by starting
    /// again. Rows of other kinds are untouched. A row of these kinds
    /// written while the delete runs may survive it, as one written just
    /// after it would. Only the repair tools call it, and only for kinds
    /// that derive again from history (ADR 0025).
    ///
    /// Every backend must implement this (see [`list_derived`](Self::list_derived)).
    async fn delete_derived_step(
        &self,
        kinds: &[&str],
        cursor: Option<&[u8]>,
    ) -> Result<DeleteStep> {
        let _ = (kinds, cursor);
        Err(Error::Invalid(
            "this metadata store cannot delete derived rows".into(),
        ))
    }

    /// Delete every row of `kind` and return how many were removed: all
    /// [`delete_derived_step`](Self::delete_derived_step)s of it. Idempotent,
    /// and rows of other kinds are untouched.
    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        let mut total = 0;
        let mut cursor: Option<Vec<u8>> = None;
        loop {
            let step = self.delete_derived_step(&[kind], cursor.as_deref()).await?;
            total += step.deleted.iter().sum::<u64>();
            match step.next {
                Some(next) => cursor = Some(next),
                None => return Ok(total),
            }
        }
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

    /// Store many changesets and derived rows in one round trip: what a land
    /// batch built (ADR 0043). Idempotent; rows are written as by
    /// [`put_derived_many`](Self::put_derived_many).
    ///
    /// Database backends override it with one atomic write, so none of the
    /// changesets is visible without the rows. The default writes the
    /// changesets in order, then the rows, which callers must also survive
    /// stopping part way.
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

    /// Whether the lease methods below work. Without them every land is
    /// batched by the server that received it, as before ADR 0056.
    fn supports_leases(&self) -> bool {
        false
    }

    /// The lease row of `key`, live or not.
    async fn lease(&self, key: &LeaseKey, timing: LeaseTiming) -> Result<Option<Lease>> {
        let _ = (key, timing);
        Err(no_leases())
    }

    /// Take or keep the lease `claim.key` for `claim.holder`, in one atomic
    /// step: it succeeds when there is none, when it is the holder's own, or
    /// when its expiry has passed or its last renewal is older than
    /// `timing.stale_after`; a live lease of another holder is never taken.
    /// The epoch grows by one whenever the holder changes, and never falls.
    /// Returns the lease as it is afterwards: the claimant's when it got it.
    async fn acquire_lease(&self, claim: &LeaseClaim<'_>, timing: LeaseTiming) -> Result<Lease> {
        let _ = (claim, timing);
        Err(no_leases())
    }

    /// Renew every `(key, epoch)` lease `holder` still holds, in one atomic
    /// step. One slot per lease, in order: the time left after the renewal,
    /// or `None` when the lease is no longer `holder`'s at that epoch.
    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(LeaseKey, u64)],
        timing: LeaseTiming,
    ) -> Result<Vec<Option<std::time::Duration>>> {
        let _ = (holder, held, timing);
        Err(no_leases())
    }

    /// Give the lease up so another server can take it at once, if it is
    /// still `holder`'s at `epoch`. The row stays, so epochs never repeat.
    async fn release_lease(&self, key: &LeaseKey, holder: &str, epoch: u64) -> Result<bool> {
        let _ = (key, holder, epoch);
        Err(no_leases())
    }

    /// Version of the newest schema migration applied (ADR 0023), for
    /// diagnostics. `None` for a store without a versioned schema.
    async fn schema_version(&self) -> Result<Option<u64>> {
        Ok(None)
    }
}

/// A land key names where a land took the ref, so it never rides on a delete.
pub fn check_land_key(new: Option<ChangesetId>, ctx: &RefUpdateContext) -> Result<()> {
    if new.is_none() && ctx.land.is_some() {
        return Err(Error::Invalid(
            "a ref delete cannot record a land id".into(),
        ));
    }
    Ok(())
}

/// The error a store returns for a move whose land id the ref already
/// recorded.
pub fn land_id_taken(name: &str, id: &LandId) -> Error {
    Error::Conflict(format!("land id {id} is already recorded for ref {name:?}"))
}

/// The rules every [`MetaStore::cas_ref_logged_chain`] applies before moving
/// anything: steps exist, each changes the ref, and the chain does not end
/// where it started.
pub fn check_ref_chain(expected: Option<ChangesetId>, steps: &[RefStep]) -> Result<()> {
    let mut old = expected;
    let mut lands = std::collections::HashSet::new();
    for step in steps {
        if step.new == old {
            return Err(Error::Invalid("a ref chain step must move the ref".into()));
        }
        check_land_key(step.new, &step.ctx)?;
        if let Some(land) = &step.ctx.land {
            if !lands.insert(&land.id) {
                return Err(Error::Invalid(format!(
                    "land id {} appears twice in one ref chain",
                    land.id
                )));
            }
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
