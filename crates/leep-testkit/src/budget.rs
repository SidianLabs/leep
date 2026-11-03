//! Operation budgets (ADR 0081): what a flow asks of its stores, counted in
//! process, so a change that alters the shape of a flow's storage work fails
//! a test on any machine instead of a benchmark on one quiet box.
//!
//! Wrap a repository's stores in one [`Budget`] (it is both a
//! [`MetaStore`] and a [`BlobStore`]), [`start`](Budget::start) it, run the
//! flow and [`stop`](Budget::stop) it for a [`Usage`]: calls, keys and bytes
//! per store method and per derived kind, and the most calls in flight at
//! once overall, per [`Lane`] and per actor. A call in flight on a pooled
//! backend holds a connection, so the in-flight counts are the connections
//! a flow would hold at once. [`check`] compares a usage with a budget table
//! and fails both ways: over a budget, and more than [`SLACK_PERCENT`] under
//! it, so an improvement is written down and kept.
//!
//! Every method forwards to the same method of the inner store, so the
//! inner store sees the calls the flow made, in the shape it made them.

use std::cell::Cell;
use std::collections::BTreeMap;
use std::fmt::Write as _;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use async_trait::async_trait;
use bytes::Bytes;
use leep_core::lane::{self, Lane};
use leep_core::prefix::IdPrefix;
use leep_core::{
    BlobMeta, BlobStore, Changeset, ChangesetId, DeleteStep, DerivedRow, Hash, LandId, LandRecord,
    Lease, LeaseClaim, LeaseKey, LeaseTiming, MetaStore, RefLogEntry, RefStep, RefUpdateContext,
    Result, RowCount,
};

/// How far under its budget a measurement may fall before [`check`] asks
/// for the budget to be lowered.
pub const SLACK_PERCENT: u64 = 20;

/// Calls, keys and bytes of one store method or one derived kind.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Tally {
    pub calls: u64,
    /// Ids, hashes or rows the calls named.
    pub keys: u64,
    /// Bytes read or written: blob and derived values, encoded changesets.
    pub bytes: u64,
}

impl Tally {
    fn add(&mut self, keys: u64, bytes: u64) {
        self.calls += 1;
        self.keys += keys;
        self.bytes += bytes;
    }
}

/// What a flow asked of its stores between [`Budget::start`] and
/// [`Budget::stop`].
#[derive(Clone, Debug, Default)]
pub struct Usage {
    /// By store method: `meta.get_changesets`, `blob.put_many`, ...
    pub ops: BTreeMap<&'static str, Tally>,
    /// Derived reads and writes by kind; a `get_derived_kinds` call counts
    /// once for each kind it names.
    pub kinds: BTreeMap<String, Tally>,
    /// The most metadata calls in flight at once.
    pub meta_in_flight: usize,
    /// The most blob calls in flight at once.
    pub blob_in_flight: usize,
    /// The most metadata calls in flight at once, per lane (`shared`,
    /// `land`, `index`): what each Postgres pool would lend at once.
    pub meta_in_flight_by_lane: BTreeMap<&'static str, usize>,
    /// The most metadata calls in flight at once per actor (see [`actor`]).
    /// Calls made outside any actor count under `""`.
    pub meta_in_flight_by_actor: BTreeMap<&'static str, usize>,
}

impl Usage {
    /// Every metadata call.
    pub fn meta_calls(&self) -> u64 {
        self.sum("meta.", |t| t.calls)
    }

    /// Every blob store call.
    pub fn blob_calls(&self) -> u64 {
        self.sum("blob.", |t| t.calls)
    }

    /// Keys every metadata call named.
    pub fn meta_keys(&self) -> u64 {
        self.sum("meta.", |t| t.keys)
    }

    /// Bytes every metadata call read or wrote.
    pub fn meta_bytes(&self) -> u64 {
        self.sum("meta.", |t| t.bytes)
    }

    /// Bytes every blob call read or wrote.
    pub fn blob_bytes(&self) -> u64 {
        self.sum("blob.", |t| t.bytes)
    }

    /// Calls of one method, by its full name (`meta.get_ref`).
    pub fn calls(&self, op: &str) -> u64 {
        self.ops.get(op).map_or(0, |t| t.calls)
    }

    /// Calls that read or wrote derived rows of `kind`.
    pub fn kind_calls(&self, kind: &str) -> u64 {
        self.kinds.get(kind).map_or(0, |t| t.calls)
    }

    /// The most metadata calls in flight at once in `lane`.
    pub fn lane_in_flight(&self, lane: Lane) -> usize {
        self.meta_in_flight_by_lane
            .get(lane_name(lane))
            .copied()
            .unwrap_or(0)
    }

    /// The most metadata calls in flight at once for `actor`.
    pub fn actor_in_flight(&self, actor: &str) -> usize {
        self.meta_in_flight_by_actor
            .get(actor)
            .copied()
            .unwrap_or(0)
    }

    fn sum(&self, prefix: &str, f: impl Fn(&Tally) -> u64) -> u64 {
        self.ops
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(_, t)| f(t))
            .sum()
    }
}

impl std::fmt::Display for Usage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(
            f,
            "meta: {} calls, {} keys, {} bytes, at most {} in flight {:?} {:?}",
            self.meta_calls(),
            self.meta_keys(),
            self.meta_bytes(),
            self.meta_in_flight,
            self.meta_in_flight_by_lane,
            self.meta_in_flight_by_actor
        )?;
        writeln!(
            f,
            "blob: {} calls, {} bytes, at most {} in flight",
            self.blob_calls(),
            self.blob_bytes(),
            self.blob_in_flight
        )?;
        for (op, t) in &self.ops {
            writeln!(
                f,
                "  {op:<36} calls {:>5} keys {:>6} bytes {:>8}",
                t.calls, t.keys, t.bytes
            )?;
        }
        for (kind, t) in &self.kinds {
            writeln!(
                f,
                "  kind {kind:<31} calls {:>5} keys {:>6} bytes {:>8}",
                t.calls, t.keys, t.bytes
            )?;
        }
        Ok(())
    }
}

fn lane_name(lane: Lane) -> &'static str {
    match lane {
        Lane::Shared => "shared",
        Lane::Land => "land",
        Lane::Index => "index",
    }
}

thread_local! {
    /// Set only while an [`Acting`] future is polled, like the lane.
    static ACTOR: Cell<&'static str> = const { Cell::new("") };
}

/// Run `f` as `name`, so [`Usage::meta_in_flight_by_actor`] tells its calls
/// from those of flows running beside it. Task-local: tasks `f` spawns
/// count under `""`.
pub fn actor<F: Future>(name: &'static str, f: F) -> Acting<F> {
    Acting {
        name,
        inner: Box::pin(f),
    }
}

/// A future polled as an actor; see [`actor`].
pub struct Acting<F> {
    name: &'static str,
    inner: Pin<Box<F>>,
}

impl<F: Future> Future for Acting<F> {
    type Output = F::Output;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        struct Restore(&'static str);
        impl Drop for Restore {
            fn drop(&mut self) {
                ACTOR.with(|a| a.set(self.0));
            }
        }
        let this = self.get_mut();
        let _restore = Restore(ACTOR.with(|a| a.replace(this.name)));
        this.inner.as_mut().poll(cx)
    }
}

#[derive(Default)]
struct State {
    usage: Usage,
    meta_now: usize,
    blob_now: usize,
    lane_now: BTreeMap<&'static str, usize>,
    actor_now: BTreeMap<&'static str, usize>,
}

/// Both stores of a repository, counted. Passes calls through uncounted
/// until [`start`](Self::start).
pub struct Budget {
    blobs: Arc<dyn BlobStore>,
    meta: Arc<dyn MetaStore>,
    armed: AtomicBool,
    state: Mutex<State>,
}

/// Which store a call goes to.
#[derive(Clone, Copy)]
enum Store {
    Meta,
    Blob,
}

impl Budget {
    pub fn new(blobs: Arc<dyn BlobStore>, meta: Arc<dyn MetaStore>) -> Arc<Budget> {
        Arc::new(Budget {
            blobs,
            meta,
            armed: AtomicBool::new(false),
            state: Mutex::default(),
        })
    }

    /// The uncounted stores, to set up or check a flow without counting.
    pub fn inner(&self) -> (&Arc<dyn BlobStore>, &Arc<dyn MetaStore>) {
        (&self.blobs, &self.meta)
    }

    /// Count from now on, from zero.
    pub fn start(&self) {
        *self.state.lock().unwrap() = State::default();
        self.armed.store(true, Ordering::SeqCst);
    }

    /// Stop counting and return what was counted since [`start`](Self::start).
    pub fn stop(&self) -> Usage {
        self.armed.store(false, Ordering::SeqCst);
        std::mem::take(&mut self.state.lock().unwrap().usage)
    }

    /// Run `call` as one counted call of `op`. While counting, a call yields
    /// once after it is counted in flight, so calls a flow makes
    /// concurrently overlap even on stores that never suspend.
    async fn run<T>(
        &self,
        store: Store,
        op: &'static str,
        keys: u64,
        bytes_in: u64,
        call: impl Future<Output = Result<T>>,
        bytes_out: impl FnOnce(&T) -> u64,
    ) -> Result<T> {
        if !self.armed.load(Ordering::SeqCst) {
            return call.await;
        }
        let lane = lane_name(lane::current());
        let actor = ACTOR.with(Cell::get);
        self.enter(store, lane, actor);
        // Leaves the in-flight counts right even if the caller drops the call.
        let _leave = Leave {
            budget: self,
            store,
            lane,
            actor,
        };
        tokio::task::yield_now().await;
        let out = call.await;
        let out_bytes = out.as_ref().map_or(0, bytes_out);
        if self.armed.load(Ordering::SeqCst) {
            let mut s = self.state.lock().unwrap();
            s.usage
                .ops
                .entry(op)
                .or_default()
                .add(keys, bytes_in + out_bytes);
        }
        out
    }

    fn enter(&self, store: Store, lane: &'static str, actor: &'static str) {
        let mut s = self.state.lock().unwrap();
        match store {
            Store::Blob => {
                s.blob_now += 1;
                s.usage.blob_in_flight = s.usage.blob_in_flight.max(s.blob_now);
            }
            Store::Meta => {
                s.meta_now += 1;
                s.usage.meta_in_flight = s.usage.meta_in_flight.max(s.meta_now);
                let n = {
                    let c = s.lane_now.entry(lane).or_default();
                    *c += 1;
                    *c
                };
                let m = s.usage.meta_in_flight_by_lane.entry(lane).or_default();
                *m = (*m).max(n);
                let n = {
                    let c = s.actor_now.entry(actor).or_default();
                    *c += 1;
                    *c
                };
                let m = s.usage.meta_in_flight_by_actor.entry(actor).or_default();
                *m = (*m).max(n);
            }
        }
    }

    fn leave(&self, store: Store, lane: &'static str, actor: &'static str) {
        let mut s = self.state.lock().unwrap();
        let dec = |n: &mut usize| *n = n.saturating_sub(1);
        match store {
            Store::Blob => dec(&mut s.blob_now),
            Store::Meta => {
                dec(&mut s.meta_now);
                dec(s.lane_now.entry(lane).or_default());
                dec(s.actor_now.entry(actor).or_default());
            }
        }
    }

    /// Count derived rows of `kind` read or written.
    fn kind(&self, kind: &str, keys: u64, bytes: u64) {
        if self.armed.load(Ordering::SeqCst) {
            self.state
                .lock()
                .unwrap()
                .usage
                .kinds
                .entry(kind.to_string())
                .or_default()
                .add(keys, bytes);
        }
    }

    async fn meta<T>(
        &self,
        op: &'static str,
        keys: u64,
        bytes_in: u64,
        call: impl Future<Output = Result<T>>,
        bytes_out: impl FnOnce(&T) -> u64,
    ) -> Result<T> {
        self.run(Store::Meta, op, keys, bytes_in, call, bytes_out)
            .await
    }

    async fn blob<T>(
        &self,
        op: &'static str,
        keys: u64,
        bytes_in: u64,
        call: impl Future<Output = Result<T>>,
        bytes_out: impl FnOnce(&T) -> u64,
    ) -> Result<T> {
        self.run(Store::Blob, op, keys, bytes_in, call, bytes_out)
            .await
    }
}

struct Leave<'a> {
    budget: &'a Budget,
    store: Store,
    lane: &'static str,
    actor: &'static str,
}

impl Drop for Leave<'_> {
    fn drop(&mut self) {
        self.budget.leave(self.store, self.lane, self.actor);
    }
}

fn none<T>(_: &T) -> u64 {
    0
}

fn opt_len(v: &Option<Bytes>) -> u64 {
    v.as_ref().map_or(0, |b| b.len() as u64)
}

fn opts_len(v: &[Option<Bytes>]) -> u64 {
    v.iter().map(opt_len).sum()
}

fn cs_len(cs: &Changeset) -> u64 {
    cs.encode().len() as u64
}

fn rows_len(rows: &[DerivedRow]) -> u64 {
    rows.iter().map(|r| r.value.len() as u64).sum()
}

fn n(len: usize) -> u64 {
    len as u64
}

#[async_trait]
impl BlobStore for Budget {
    async fn put(&self, data: Bytes) -> Result<Hash> {
        self.blob("blob.put", 1, n(data.len()), self.blobs.put(data), none)
            .await
    }

    async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.blob(
            "blob.put_with_hash",
            1,
            n(data.len()),
            self.blobs.put_with_hash(hash, data),
            none,
        )
        .await
    }

    async fn put_many(&self, items: Vec<(Hash, Bytes)>) -> Result<Vec<Hash>> {
        let (keys, bytes) = (n(items.len()), items.iter().map(|(_, d)| n(d.len())).sum());
        self.blob(
            "blob.put_many",
            keys,
            bytes,
            self.blobs.put_many(items),
            none,
        )
        .await
    }

    async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.blob("blob.get", 1, 0, self.blobs.get(hash), opt_len)
            .await
    }

    async fn get_many(&self, hashes: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        self.blob(
            "blob.get_many",
            n(hashes.len()),
            0,
            self.blobs.get_many(hashes),
            |v| opts_len(v),
        )
        .await
    }

    async fn contains(&self, hash: &Hash) -> Result<bool> {
        self.blob("blob.contains", 1, 0, self.blobs.contains(hash), none)
            .await
    }

    async fn warm(&self) -> Result<()> {
        self.blobs.warm().await
    }

    async fn get_stored(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.blob(
            "blob.get_stored",
            1,
            0,
            self.blobs.get_stored(hash),
            opt_len,
        )
        .await
    }

    async fn get_exact(&self, hash: &Hash) -> Result<Option<Bytes>> {
        self.blob("blob.get_exact", 1, 0, self.blobs.get_exact(hash), opt_len)
            .await
    }

    async fn contains_many(&self, hashes: &[Hash]) -> Result<Vec<bool>> {
        self.blob(
            "blob.contains_many",
            n(hashes.len()),
            0,
            self.blobs.contains_many(hashes),
            none,
        )
        .await
    }

    async fn flush(&self, named: &[Hash]) -> Result<()> {
        self.blob(
            "blob.flush",
            n(named.len()),
            0,
            self.blobs.flush(named),
            none,
        )
        .await
    }

    async fn flush_blobs(&self, hashes: &[Hash]) -> Result<()> {
        self.blob(
            "blob.flush_blobs",
            n(hashes.len()),
            0,
            self.blobs.flush_blobs(hashes),
            none,
        )
        .await
    }

    async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
        self.blob(
            "blob.list_page",
            0,
            0,
            self.blobs.list_page(after, limit),
            none,
        )
        .await
    }

    async fn delete(&self, hash: &Hash) -> Result<()> {
        self.blob("blob.delete", 1, 0, self.blobs.delete(hash), none)
            .await
    }
}

#[async_trait]
impl MetaStore for Budget {
    async fn put_changeset(&self, cs: &Changeset) -> Result<ChangesetId> {
        self.meta(
            "meta.put_changeset",
            1,
            cs_len(cs),
            self.meta.put_changeset(cs),
            none,
        )
        .await
    }

    async fn get_changeset(&self, id: &ChangesetId) -> Result<Option<Changeset>> {
        self.meta(
            "meta.get_changeset",
            1,
            0,
            self.meta.get_changeset(id),
            |v| v.as_ref().map_or(0, cs_len),
        )
        .await
    }

    async fn get_changesets(&self, ids: &[ChangesetId]) -> Result<Vec<Option<Changeset>>> {
        self.meta(
            "meta.get_changesets",
            n(ids.len()),
            0,
            self.meta.get_changesets(ids),
            |v| v.iter().flatten().map(cs_len).sum(),
        )
        .await
    }

    async fn list_changesets(
        &self,
        after: Option<ChangesetId>,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.meta(
            "meta.list_changesets",
            0,
            0,
            self.meta.list_changesets(after, limit),
            none,
        )
        .await
    }

    async fn changesets_with_prefix(
        &self,
        prefix: &IdPrefix,
        limit: usize,
    ) -> Result<Vec<ChangesetId>> {
        self.meta(
            "meta.changesets_with_prefix",
            0,
            0,
            self.meta.changesets_with_prefix(prefix, limit),
            none,
        )
        .await
    }

    async fn changesets_stored_at(&self, ids: &[ChangesetId]) -> Result<Vec<Option<u64>>> {
        self.meta(
            "meta.changesets_stored_at",
            n(ids.len()),
            0,
            self.meta.changesets_stored_at(ids),
            none,
        )
        .await
    }

    async fn get_ref(&self, name: &str) -> Result<Option<ChangesetId>> {
        self.meta("meta.get_ref", 1, 0, self.meta.get_ref(name), none)
            .await
    }

    async fn cas_ref(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
    ) -> Result<bool> {
        self.meta(
            "meta.cas_ref",
            1,
            0,
            self.meta.cas_ref(name, expected, new),
            none,
        )
        .await
    }

    async fn cas_ref_logged(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        new: Option<ChangesetId>,
        ctx: &RefUpdateContext,
    ) -> Result<bool> {
        self.meta(
            "meta.cas_ref_logged",
            1,
            0,
            self.meta.cas_ref_logged(name, expected, new, ctx),
            none,
        )
        .await
    }

    async fn cas_ref_logged_chain(
        &self,
        name: &str,
        expected: Option<ChangesetId>,
        steps: &[RefStep],
    ) -> Result<usize> {
        self.meta(
            "meta.cas_ref_logged_chain",
            n(steps.len()),
            0,
            self.meta.cas_ref_logged_chain(name, expected, steps),
            none,
        )
        .await
    }

    async fn find_lands(&self, name: &str, ids: &[LandId]) -> Result<Vec<Option<LandRecord>>> {
        self.meta(
            "meta.find_lands",
            n(ids.len()),
            0,
            self.meta.find_lands(name, ids),
            none,
        )
        .await
    }

    async fn delete_changesets(&self, ids: &[ChangesetId], kinds: &[&str]) -> Result<u64> {
        self.meta(
            "meta.delete_changesets",
            n(ids.len()),
            0,
            self.meta.delete_changesets(ids, kinds),
            none,
        )
        .await
    }

    async fn delete_derived_keys(&self, kinds: &[&str], keys: &[Hash]) -> Result<u64> {
        self.meta(
            "meta.delete_derived_keys",
            n(keys.len()),
            0,
            self.meta.delete_derived_keys(kinds, keys),
            none,
        )
        .await
    }

    async fn ref_log(
        &self,
        name: Option<&str>,
        before_seq: Option<u64>,
        limit: usize,
    ) -> Result<Vec<RefLogEntry>> {
        self.meta(
            "meta.ref_log",
            0,
            0,
            self.meta.ref_log(name, before_seq, limit),
            none,
        )
        .await
    }

    async fn list_refs(&self) -> Result<BTreeMap<String, ChangesetId>> {
        self.meta("meta.list_refs", 0, 0, self.meta.list_refs(), none)
            .await
    }

    async fn list_derived(
        &self,
        kind: &str,
        after: Option<Hash>,
        limit: usize,
    ) -> Result<Vec<(Hash, Bytes)>> {
        self.meta(
            "meta.list_derived",
            0,
            0,
            self.meta.list_derived(kind, after, limit),
            |v| v.iter().map(|(_, b)| n(b.len())).sum(),
        )
        .await
    }

    async fn count_derived(&self, kind: &str) -> Result<u64> {
        self.meta(
            "meta.count_derived",
            0,
            0,
            self.meta.count_derived(kind),
            none,
        )
        .await
    }

    async fn estimate_derived(&self, kind: &str) -> Result<RowCount> {
        self.meta(
            "meta.estimate_derived",
            0,
            0,
            self.meta.estimate_derived(kind),
            none,
        )
        .await
    }

    async fn delete_derived_step(
        &self,
        kinds: &[&str],
        cursor: Option<&[u8]>,
    ) -> Result<DeleteStep> {
        self.meta(
            "meta.delete_derived_step",
            0,
            0,
            self.meta.delete_derived_step(kinds, cursor),
            none,
        )
        .await
    }

    async fn delete_derived_kind(&self, kind: &str) -> Result<u64> {
        self.meta(
            "meta.delete_derived_kind",
            0,
            0,
            self.meta.delete_derived_kind(kind),
            none,
        )
        .await
    }

    async fn get_derived(&self, kind: &str, key: &Hash) -> Result<Option<Bytes>> {
        let v = self
            .meta(
                "meta.get_derived",
                1,
                0,
                self.meta.get_derived(kind, key),
                opt_len,
            )
            .await?;
        self.kind(kind, 1, opt_len(&v));
        Ok(v)
    }

    async fn get_derived_many(&self, kind: &str, keys: &[Hash]) -> Result<Vec<Option<Bytes>>> {
        let v = self
            .meta(
                "meta.get_derived_many",
                n(keys.len()),
                0,
                self.meta.get_derived_many(kind, keys),
                |v| opts_len(v),
            )
            .await?;
        self.kind(kind, n(keys.len()), opts_len(&v));
        Ok(v)
    }

    async fn get_derived_kinds(
        &self,
        reads: &[(&str, &[Hash])],
    ) -> Result<Vec<Vec<Option<Bytes>>>> {
        let keys = reads.iter().map(|(_, k)| n(k.len())).sum();
        let v = self
            .meta(
                "meta.get_derived_kinds",
                keys,
                0,
                self.meta.get_derived_kinds(reads),
                |v| v.iter().map(|r| opts_len(r)).sum(),
            )
            .await?;
        for ((kind, keys), rows) in reads.iter().zip(&v) {
            self.kind(kind, n(keys.len()), opts_len(rows));
        }
        Ok(v)
    }

    async fn put_derived(&self, kind: &str, key: &Hash, value: Bytes) -> Result<()> {
        let len = n(value.len());
        self.meta(
            "meta.put_derived",
            1,
            len,
            self.meta.put_derived(kind, key, value),
            none,
        )
        .await?;
        self.kind(kind, 1, len);
        Ok(())
    }

    async fn put_derived_many(&self, rows: &[DerivedRow]) -> Result<()> {
        self.meta(
            "meta.put_derived_many",
            n(rows.len()),
            rows_len(rows),
            self.meta.put_derived_many(rows),
            none,
        )
        .await?;
        self.kind_rows(rows);
        Ok(())
    }

    async fn put_changeset_with_derived(
        &self,
        cs: &Changeset,
        rows: &[DerivedRow],
    ) -> Result<ChangesetId> {
        let id = self
            .meta(
                "meta.put_changeset_with_derived",
                1 + n(rows.len()),
                cs_len(cs) + rows_len(rows),
                self.meta.put_changeset_with_derived(cs, rows),
                none,
            )
            .await?;
        self.kind_rows(rows);
        Ok(id)
    }

    async fn put_changesets_with_derived(
        &self,
        changesets: &[Changeset],
        rows: &[DerivedRow],
    ) -> Result<()> {
        self.meta(
            "meta.put_changesets_with_derived",
            n(changesets.len() + rows.len()),
            changesets.iter().map(cs_len).sum::<u64>() + rows_len(rows),
            self.meta.put_changesets_with_derived(changesets, rows),
            none,
        )
        .await?;
        self.kind_rows(rows);
        Ok(())
    }

    fn supports_leases(&self) -> bool {
        self.meta.supports_leases()
    }

    async fn lease(&self, key: &LeaseKey, timing: LeaseTiming) -> Result<Option<Lease>> {
        self.meta("meta.lease", 1, 0, self.meta.lease(key, timing), none)
            .await
    }

    async fn acquire_lease(&self, claim: &LeaseClaim<'_>, timing: LeaseTiming) -> Result<Lease> {
        self.meta(
            "meta.acquire_lease",
            1,
            0,
            self.meta.acquire_lease(claim, timing),
            none,
        )
        .await
    }

    async fn renew_leases(
        &self,
        holder: &str,
        held: &[(LeaseKey, u64)],
        timing: LeaseTiming,
    ) -> Result<Vec<Option<std::time::Duration>>> {
        self.meta(
            "meta.renew_leases",
            n(held.len()),
            0,
            self.meta.renew_leases(holder, held, timing),
            none,
        )
        .await
    }

    async fn release_lease(&self, key: &LeaseKey, holder: &str, epoch: u64) -> Result<bool> {
        self.meta(
            "meta.release_lease",
            1,
            0,
            self.meta.release_lease(key, holder, epoch),
            none,
        )
        .await
    }

    async fn schema_version(&self) -> Result<Option<u64>> {
        self.meta.schema_version().await
    }
}

impl Budget {
    fn kind_rows(&self, rows: &[DerivedRow]) {
        for r in rows {
            self.kind(&r.kind, 1, n(r.value.len()));
        }
    }
}

/// One line of a budget table: the most `metric` may reach in `flow`, as
/// `base + per * k` for a flow sized by `k` (lands in a batch, versions of
/// a file), or just `base` with `per` 0.
#[derive(Clone, Copy, Debug)]
pub struct Limit {
    pub flow: &'static str,
    pub metric: &'static str,
    pub base: u64,
    pub per: u64,
}

impl Limit {
    pub const fn fixed(flow: &'static str, metric: &'static str, max: u64) -> Limit {
        Limit {
            flow,
            metric,
            base: max,
            per: 0,
        }
    }

    pub const fn linear(flow: &'static str, metric: &'static str, base: u64, per: u64) -> Limit {
        Limit {
            flow,
            metric,
            base,
            per,
        }
    }

    fn at(&self, k: u64) -> u64 {
        self.base + self.per * k
    }
}

/// Measurements of one run of a flow: `(metric, k, value)`, `k` being the
/// size the flow ran at (0 for fixed limits).
pub type Measured = [(&'static str, u64, u64)];

/// Check every measurement of `flow` against its line in `table`, both
/// ways, and panic with every breach at once, printing `usage` (a
/// [`Usage`], or several runs' as text). `table_at` names where the table
/// lives, for the message.
///
/// Over budget fails: the flow does more storage work than it did, which
/// is how a resource-shape regression shows before any benchmark. More than
/// [`SLACK_PERCENT`] under budget also fails, until the line is lowered:
/// otherwise a later regression could spend the gain unseen. A measurement
/// with no line fails too.
pub fn check(
    table: &[Limit],
    table_at: &str,
    flow: &str,
    measured: &Measured,
    usage: &dyn std::fmt::Display,
) {
    let mut breaches = String::new();
    for &(metric, k, value) in measured {
        let Some(limit) = table.iter().find(|l| l.flow == flow && l.metric == metric) else {
            let _ = writeln!(
                breaches,
                "  {flow} / {metric}: measured {value} with no budget; add a line \
                 `Limit::fixed(\"{flow}\", \"{metric}\", {value})` to {table_at}"
            );
            continue;
        };
        let max = limit.at(k);
        let at = if limit.per > 0 {
            format!(" at k = {k} ({} + {} x {k})", limit.base, limit.per)
        } else {
            String::new()
        };
        if value > max {
            let _ = writeln!(
                breaches,
                "  {flow} / {metric}: {value} is over its budget of {max}{at}. This change \
                 makes the flow do more storage work. Find the call that grew in the usage \
                 below; if the growth is intended, raise the line in {table_at} and say \
                 why in the commit message"
            );
        } else if value * 100 < max * (100 - SLACK_PERCENT) {
            let _ = writeln!(
                breaches,
                "  {flow} / {metric}: {value} is more than {SLACK_PERCENT}% under its budget \
                 of {max}{at}. Lower the line in {table_at} to the measured value so the \
                 gain is kept (for a linear line, lower `base` or `per` until it fits)"
            );
        }
    }
    assert!(
        breaches.is_empty(),
        "operation budget of `{flow}` breached:\n{breaches}usage:\n{usage}"
    );
}

#[cfg(test)]
mod tests {
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};

    use super::*;

    fn budget() -> Arc<Budget> {
        Budget::new(
            Arc::new(MemoryBlobStore::new()),
            Arc::new(MemoryMetaStore::new()),
        )
    }

    #[tokio::test]
    async fn counts_calls_keys_bytes_and_overlap() {
        let b = budget();
        let k = Hash::of_blob(b"k");
        b.put_derived("x", &k, Bytes::from_static(b"abc"))
            .await
            .unwrap();
        let keys = [k, k];
        b.start();
        let (one, two) = tokio::join!(
            actor("a", b.get_derived("x", &k)),
            actor("b", b.get_derived_many("x", &keys)),
        );
        one.unwrap();
        two.unwrap();
        b.put(Bytes::from_static(b"12345")).await.unwrap();
        let u = b.stop();
        assert_eq!(u.meta_calls(), 2);
        assert_eq!(u.meta_keys(), 3);
        assert_eq!(u.meta_bytes(), 9);
        assert_eq!(u.blob_calls(), 1);
        assert_eq!(u.blob_bytes(), 5);
        assert_eq!(
            u.kinds["x"],
            Tally {
                calls: 2,
                keys: 3,
                bytes: 9
            }
        );
        assert_eq!(u.meta_in_flight, 2);
        assert_eq!(u.actor_in_flight("a"), 1);
        assert_eq!(u.actor_in_flight("b"), 1);
        assert_eq!(u.lane_in_flight(Lane::Shared), 2);
        // Uncounted once stopped.
        b.get_ref("main").await.unwrap();
        b.start();
        assert_eq!(b.stop().meta_calls(), 0);
    }

    #[tokio::test]
    async fn lanes_are_counted_apart() {
        let b = budget();
        b.start();
        let (x, y) = tokio::join!(
            lane::scope(Lane::Land, b.get_ref("main")),
            b.get_ref("main"),
        );
        x.unwrap();
        y.unwrap();
        let u = b.stop();
        assert_eq!(u.lane_in_flight(Lane::Land), 1);
        assert_eq!(u.lane_in_flight(Lane::Shared), 1);
        assert_eq!(u.meta_in_flight, 2);
    }

    const TABLE: &[Limit] = &[
        Limit::fixed("f", "calls", 10),
        Limit::linear("g", "calls", 2, 3),
    ];

    #[test]
    fn check_passes_inside_the_band() {
        let u = Usage::default();
        check(TABLE, "here", "f", &[("calls", 0, 10)], &u);
        check(TABLE, "here", "f", &[("calls", 0, 8)], &u);
        check(TABLE, "here", "g", &[("calls", 4, 14), ("calls", 1, 5)], &u);
    }

    #[test]
    #[should_panic(expected = "over its budget of 10")]
    fn check_fails_over_budget() {
        check(TABLE, "here", "f", &[("calls", 0, 11)], &Usage::default());
    }

    #[test]
    #[should_panic(expected = "more than 20% under its budget of 14 at k = 4")]
    fn check_fails_far_under_budget() {
        check(TABLE, "here", "g", &[("calls", 4, 11)], &Usage::default());
    }

    #[test]
    #[should_panic(expected = "with no budget")]
    fn check_fails_without_a_line() {
        check(TABLE, "here", "h", &[("calls", 0, 1)], &Usage::default());
    }
}
