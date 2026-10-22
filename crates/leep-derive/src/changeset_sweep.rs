//! The changeset sweep (ADR 0059 phase 2): condemn the changesets no root
//! reaches, wait out a fence, then delete those still condemned with their
//! per-changeset rows, children before parents.
//!
//! The protocol is blob GC's (ADR 0012) with changesets for blobs; the
//! writers' side is in `changeset_gc`. Two rules keep the stored set closed
//! under ancestry at every instant, whatever is killed when:
//!
//! - **Kept propagates to ancestors.** Collect works out, over the whole
//!   unreachable set before its first delete, every changeset it will not
//!   delete (rescued, marked, too young, not condemned) and keeps all their
//!   condemned ancestors too. A batch that finds a rescue keeps that
//!   changeset's ancestors for the rest of the run.
//! - **Children first, by parent edges.** What is left is deleted in
//!   reverse topological order, computed from parents, never from
//!   `generation` rows, which can be missing. A run cut short by
//!   `max_delete`, a failure or a kill leaves only ancestors of what it
//!   kept.
//!
//! Each batch is one `delete_changesets` transaction. Its doom rows say
//! *deleting* before and *deleted* after; a tombstone row (deletion time
//! and root tree) is written before the delete, so `gc --full` can keep the
//! deleted root trees a backup in flight still needs. The tombstone pass
//! then removes per-changeset rows written after a delete (a late indexer
//! write) and, past a retention, the tombstones themselves.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use bytes::Bytes;
use leep_core::{ChangesetId, DerivedRow, Error, Hash, Result};

use crate::changeset_gc::{
    ancestors_first, fence_flag_key, CS_CONDEMNED, CS_DOOM, CS_RESCUE, GC_FENCE_ACTIVE,
};
use crate::changeset_reach::{self, ReachOptions};
use crate::gc::{decode_time, doom_value, time_value, Clock, Doom};
use crate::gc_lease::{self, GcLease};
use crate::gc_unreachable::PER_CHANGESET_KINDS;
use crate::Repo;

/// A deleted changeset (key: its id). Value: the deletion time (unix
/// seconds, 8 bytes big-endian), then its root tree's hash (32 bytes) when
/// it had a `root_tree` row. `gc --full` keeps the root trees of changesets
/// deleted within `--backup-fence` (ADR 0059 section 6).
pub const CS_TOMBSTONE: &str = "cs-tombstone-v1";

/// Rows read and written per call.
const PAGE: usize = 1000;

#[derive(Clone, Debug)]
pub struct SweepOptions {
    /// Changesets stored less than this long ago are roots (7 days).
    pub grace: Duration,
    /// Collect deletes only condemnations at least this old (15 minutes).
    /// The caller checks it against the servers' bounds (ADR 0059 §11).
    pub fence: Duration,
    /// Allow a zero fence: only with no server running.
    pub unsafe_no_fence: bool,
    /// Changesets per delete transaction (500).
    pub batch: usize,
    /// Changesets deleted per second at most; 0 is no limit (500).
    pub rate: u64,
    /// Changesets one collect deletes at most (100,000).
    pub max_delete: u64,
    /// How long the tombstone pass waits for staged index writes in flight
    /// (ADR 0058's index fence, 60 s).
    pub index_fence: Duration,
    /// How long tombstones are kept (7 days). `gc --full --backup-fence`
    /// must not exceed it.
    pub tombstone_retention: Duration,
    /// Walk reachability from every root instead of trusting `cs-reach-v1`.
    pub full: bool,
}

impl Default for SweepOptions {
    fn default() -> Self {
        SweepOptions {
            grace: Duration::from_secs(7 * 86_400),
            fence: Duration::from_secs(15 * 60),
            unsafe_no_fence: false,
            batch: 500,
            rate: 500,
            max_delete: 100_000,
            index_fence: Duration::from_secs(60),
            tombstone_retention: Duration::from_secs(7 * 86_400),
            full: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepCondemnReport {
    /// Stored changesets the mark listed.
    pub stored: u64,
    /// Unreachable and older than grace.
    pub unreachable: u64,
    /// Kept only by a scan window, attestation, mirror map or grace.
    pub transient: u64,
    pub condemned: u64,
    /// Condemned by an earlier run and not rescued since.
    pub already_condemned: u64,
    pub reach_rows_written: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepCollectReport {
    pub stored: u64,
    pub unreachable: u64,
    /// Condemnations younger than the fence, left for a later run.
    pub fence_pending: u64,
    /// Condemned changesets a root reaches again: rescued by collect.
    pub rescued_by_reference: u64,
    /// Condemned and fenced, but an ancestor of something not deleted.
    pub kept_by_descendant: u64,
    /// Announced, then found rescued: the delete was abandoned.
    pub abandoned: u64,
    pub deleted: u64,
    /// Canonical encoding of the deleted changesets.
    pub deleted_bytes: u64,
    /// Left for the next run by `max_delete`.
    pub left_by_cap: u64,
    /// Deletes a killed run announced and this one finished.
    pub finished_from_earlier: u64,
    /// Per-changeset rows the tombstone pass removed.
    pub tombstone_rows_deleted: u64,
    pub tombstones_dropped: u64,
    /// Longest batch from doom *deleting* to *deleted*.
    pub max_batch: Duration,
    /// Condemnations still unresolved after this run.
    pub condemned_pending: u64,
    /// Whether this run cleared `gc-fence-active-v1`.
    pub flag_cleared: bool,
    /// Blame rows keyed by the deleted changesets, estimated from the
    /// store's blame rows per changeset: they leak until
    /// `repair drop --kind blame` (ADR 0059 section 1).
    pub blame_rows_of_deleted_estimate: u64,
}

fn secs(d: Duration) -> i64 {
    i64::try_from(d.as_secs()).unwrap_or(i64::MAX)
}

fn check_fence(opts: &SweepOptions) -> Result<()> {
    if opts.fence.is_zero() && !opts.unsafe_no_fence {
        return Err(Error::Invalid(
            "a zero fence can delete a changeset a request is about to name; pass a fence \
             or --unsafe-no-fence (only with no server running)"
                .into(),
        ));
    }
    if opts.batch == 0 {
        return Err(Error::Invalid("--batch must be at least 1".into()));
    }
    Ok(())
}

/// Value of a [`CS_TOMBSTONE`] row.
fn tombstone_value(at: i64, root: Option<Hash>) -> Bytes {
    let mut v = at.to_be_bytes().to_vec();
    if let Some(r) = root {
        v.extend_from_slice(r.as_bytes());
    }
    Bytes::from(v)
}

/// Decode a [`CS_TOMBSTONE`] row: deletion time and root tree.
pub fn decode_tombstone(v: &[u8]) -> Result<(i64, Option<Hash>)> {
    let bad = || Error::Other(format!("corrupt {CS_TOMBSTONE} row"));
    let (t, rest) = v.split_at_checked(8).ok_or_else(bad)?;
    let t = decode_time(CS_TOMBSTONE, t)?;
    match rest.len() {
        0 => Ok((t, None)),
        32 => Ok((t, Some(Hash(rest.try_into().map_err(|_| bad())?)))),
        _ => Err(bad()),
    }
}

/// Every row of `kind`, paged.
async fn all_rows(repo: &Repo, kind: &str) -> Result<Vec<(Hash, Bytes)>> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = repo.meta().list_derived(kind, after, PAGE).await?;
        let Some((last, _)) = page.last() else {
            return Ok(out);
        };
        after = Some(*last);
        out.extend(page);
    }
}

/// Which of `ids` are stored.
async fn stored(repo: &Repo, ids: &[ChangesetId]) -> Result<Vec<bool>> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(PAGE) {
        out.extend(
            repo.meta()
                .get_changesets(chunk)
                .await?
                .iter()
                .map(Option::is_some),
        );
    }
    Ok(out)
}

/// Phase one: raise the fence flag, mark reachability, and condemn every
/// stored changeset no root reaches. Deletes nothing.
pub async fn condemn(
    repo: &Repo,
    opts: &SweepOptions,
    clock: &dyn Clock,
) -> Result<SweepCondemnReport> {
    let lease = GcLease::acquire(repo.meta(), "changeset-condemn").await?;
    let result = condemn_leased(repo, opts, clock, &lease).await;
    gc_lease::release(lease, result).await
}

async fn condemn_leased(
    repo: &Repo,
    opts: &SweepOptions,
    clock: &dyn Clock,
    lease: &Option<GcLease>,
) -> Result<SweepCondemnReport> {
    let mut report = SweepCondemnReport::default();
    let started = clock.now();
    // Before anything is condemned: writers that read the flag down act on
    // it for at most its TTL, which the fence covers.
    let flag = repo
        .meta()
        .get_derived(GC_FENCE_ACTIVE, &fence_flag_key())
        .await?;
    let up = flag
        .as_deref()
        .is_some_and(|v| decode_time(GC_FENCE_ACTIVE, v).map_or(true, |t| t != 0));
    if !up {
        repo.meta()
            .put_derived(GC_FENCE_ACTIVE, &fence_flag_key(), time_value(started))
            .await?;
    }
    let reach = changeset_reach::mark(
        repo,
        &ReachOptions {
            now: started,
            grace: opts.grace,
            full: opts.full,
        },
    )
    .await?;
    report.stored = reach.stored;
    report.unreachable = reach.unreachable.len() as u64;
    report.transient = reach.transient;
    report.reach_rows_written = reach.rows_written;
    for chunk in reach.unreachable.chunks(PAGE) {
        gc_lease::check(lease)?;
        let fences = repo.cs_fences(chunk).await?;
        let mut rows = Vec::new();
        for (id, f) in chunk.iter().zip(fences) {
            if f.is_condemned() {
                report.already_condemned += 1;
                continue;
            }
            // Strictly after any earlier condemnation, so any older rescue
            // is stale, even if the clock stepped back.
            let at = f.condemned.map_or(0, |c| c + 1).max(started);
            rows.push(DerivedRow::new(CS_CONDEMNED, id.0, time_value(at)));
        }
        repo.meta().put_derived_many(&rows).await?;
        report.condemned += rows.len() as u64;
    }
    metrics::counter!("leep_gc_changesets_condemned_total").increment(report.condemned);
    Ok(report)
}

/// Phase two: mark again, then delete every condemnation at least `fence`
/// old that nothing rescued and nothing kept, children first; then the
/// tombstone pass, and clear the flag when nothing is left condemned.
pub async fn collect(
    repo: &Repo,
    opts: &SweepOptions,
    clock: &dyn Clock,
) -> Result<SweepCollectReport> {
    check_fence(opts)?;
    let lease = GcLease::acquire(repo.meta(), "changeset-collect").await?;
    let result = collect_leased(repo, opts, clock, &lease).await;
    gc_lease::release(lease, result).await
}

async fn collect_leased(
    repo: &Repo,
    opts: &SweepOptions,
    clock: &dyn Clock,
    lease: &Option<GcLease>,
) -> Result<SweepCollectReport> {
    let mut report = SweepCollectReport::default();
    let started = clock.now();
    let eligible_before = started.saturating_sub(secs(opts.fence));
    let reach = changeset_reach::mark(
        repo,
        &ReachOptions {
            now: started,
            grace: opts.grace,
            full: opts.full,
        },
    )
    .await?;
    report.stored = reach.stored;
    report.unreachable = reach.unreachable.len() as u64;
    let unreachable: HashSet<ChangesetId> = reach.unreachable.iter().copied().collect();

    // Condemnations a root reaches again, or whose changeset is gone.
    let condemned_ids: Vec<ChangesetId> = all_rows(repo, CS_CONDEMNED)
        .await?
        .into_iter()
        .map(|(k, _)| ChangesetId(k))
        .filter(|id| !unreachable.contains(id))
        .collect();
    for chunk in condemned_ids.chunks(PAGE) {
        gc_lease::check(lease)?;
        let (fences, here) = (repo.cs_fences(chunk).await?, stored(repo, chunk).await?);
        let mut rows = Vec::new();
        for ((id, f), here) in chunk.iter().zip(fences).zip(here) {
            let Some(c) = f.condemned else { continue };
            if here {
                if f.is_condemned() {
                    // Stored by a writer that checked before the condemnation.
                    rows.push(DerivedRow::new(CS_RESCUE, id.0, time_value(c)));
                    report.rescued_by_reference += 1;
                }
                if f.doom_state() == Some(Doom::Deleting) {
                    rows.push(DerivedRow::new(
                        CS_DOOM,
                        id.0,
                        doom_value(c, Doom::Abandoned),
                    ));
                }
            } else if f.doom_state() == Some(Doom::Deleting) {
                // A killed collect deleted it and died before saying so.
                rows.push(DerivedRow::new(CS_DOOM, id.0, doom_value(c, Doom::Deleted)));
                report.finished_from_earlier += 1;
            }
        }
        repo.meta().put_derived_many(&rows).await?;
    }

    // The unreachable set: parents, sizes, fence states.
    let ids = reach.unreachable;
    let mut parents: HashMap<ChangesetId, Vec<ChangesetId>> = HashMap::with_capacity(ids.len());
    let mut sizes: HashMap<ChangesetId, u64> = HashMap::with_capacity(ids.len());
    let mut condemned_at: HashMap<ChangesetId, i64> = HashMap::new();
    for chunk in ids.chunks(PAGE) {
        let found = repo.meta().get_changesets(chunk).await?;
        let fences = repo.cs_fences(chunk).await?;
        for ((id, cs), f) in chunk.iter().zip(found).zip(fences) {
            // Listed by the mark, gone since: nothing to do.
            let Some(cs) = cs else { continue };
            sizes.insert(*id, cs.encode().len() as u64);
            parents.insert(*id, cs.parents);
            match f.condemned {
                Some(c) if f.is_condemned() && c <= eligible_before => {
                    condemned_at.insert(*id, c);
                }
                Some(_) if f.is_condemned() => report.fence_pending += 1,
                _ => {}
            }
        }
    }

    // Keep every condemned ancestor of anything this run will not delete,
    // before the first delete, so no cut-short run strands a child.
    let mut kept: HashSet<ChangesetId> = HashSet::new();
    let mut stack: Vec<ChangesetId> = parents
        .keys()
        .filter(|id| !condemned_at.contains_key(id))
        .copied()
        .collect();
    keep_ancestors(&mut stack, &parents, &mut kept);
    report.kept_by_descendant = condemned_at.keys().filter(|id| kept.contains(id)).count() as u64;
    let todo: Vec<ChangesetId> = {
        let mut v: Vec<ChangesetId> = condemned_at
            .keys()
            .filter(|id| !kept.contains(id))
            .copied()
            .collect();
        v.sort_unstable();
        let mut order = ancestors_first(&v, &parents);
        order.reverse();
        order
    };
    let cap = usize::try_from(opts.max_delete).unwrap_or(usize::MAX);
    report.left_by_cap = todo.len().saturating_sub(cap) as u64;
    let todo = &todo[..todo.len().min(cap)];

    let mut skip: HashSet<ChangesetId> = HashSet::new();
    for batch in todo.chunks(opts.batch) {
        gc_lease::check(lease)?;
        let batch: Vec<ChangesetId> = batch
            .iter()
            .filter(|id| !skip.contains(id))
            .copied()
            .collect();
        if batch.is_empty() {
            continue;
        }
        let began = std::time::Instant::now();
        // Handshake: announce, then look for rescues. A writer writes its
        // rescue and then looks for the announcement.
        let announce: Vec<DerivedRow> = batch
            .iter()
            .map(|id| DerivedRow::new(CS_DOOM, id.0, doom_value(condemned_at[id], Doom::Deleting)))
            .collect();
        repo.meta().put_derived_many(&announce).await?;
        let now = repo.cs_fences(&batch).await?;
        let mut rescued: Vec<ChangesetId> = Vec::new();
        for (id, f) in batch.iter().zip(&now) {
            if f.condemned != Some(condemned_at[id]) || !f.is_condemned() {
                rescued.push(*id);
            }
        }
        if !rescued.is_empty() {
            let mut stack = rescued.clone();
            let before = skip.len();
            keep_ancestors(&mut stack, &parents, &mut skip);
            report.abandoned += rescued.len() as u64;
            report.kept_by_descendant += (skip.len() - before).saturating_sub(rescued.len()) as u64;
        }
        let doomed: Vec<ChangesetId> = batch
            .iter()
            .filter(|id| !skip.contains(id))
            .copied()
            .collect();
        if !doomed.is_empty() {
            let keys: Vec<Hash> = doomed.iter().map(|id| id.0).collect();
            let roots = repo.meta().get_derived_many("root_tree", &keys).await?;
            let at = clock.now();
            let stones: Vec<DerivedRow> = doomed
                .iter()
                .zip(roots)
                .map(|(id, r)| {
                    let root = r.and_then(|v| <[u8; 32]>::try_from(v.as_ref()).ok().map(Hash));
                    DerivedRow::new(CS_TOMBSTONE, id.0, tombstone_value(at, root))
                })
                .collect();
            repo.meta().put_derived_many(&stones).await?;
            gc_lease::check(lease)?;
            repo.meta()
                .delete_changesets(&doomed, PER_CHANGESET_KINDS)
                .await?;
        }
        let settle: Vec<DerivedRow> = batch
            .iter()
            .map(|id| {
                let state = if skip.contains(id) {
                    Doom::Abandoned
                } else {
                    Doom::Deleted
                };
                DerivedRow::new(CS_DOOM, id.0, doom_value(condemned_at[id], state))
            })
            .collect();
        repo.meta().put_derived_many(&settle).await?;
        report.max_batch = report.max_batch.max(began.elapsed());
        metrics::histogram!("leep_gc_changeset_collect_batch_seconds")
            .record(began.elapsed().as_secs_f64());
        for id in &doomed {
            report.deleted += 1;
            report.deleted_bytes += sizes.get(id).copied().unwrap_or(0);
        }
        if opts.rate > 0 {
            clock
                .sleep(Duration::from_secs_f64(
                    doomed.len() as f64 / opts.rate as f64,
                ))
                .await;
        }
    }
    metrics::counter!("leep_gc_changesets_deleted_total").increment(report.deleted);
    metrics::counter!("leep_gc_changeset_bytes_deleted_total").increment(report.deleted_bytes);

    tombstone_pass(repo, opts, clock, lease, started, &mut report).await?;

    // Clear the flag once nothing stored is left condemned.
    let mut pending = 0u64;
    let rows = all_rows(repo, CS_CONDEMNED).await?;
    let ids: Vec<ChangesetId> = rows.iter().map(|(k, _)| ChangesetId(*k)).collect();
    for chunk in ids.chunks(PAGE) {
        let (fences, here) = (repo.cs_fences(chunk).await?, stored(repo, chunk).await?);
        pending += fences
            .iter()
            .zip(here)
            .filter(|(f, here)| {
                *here && (f.is_condemned() || f.doom_state() == Some(Doom::Deleting))
            })
            .count() as u64;
    }
    report.condemned_pending = pending;
    if pending == 0 {
        gc_lease::check(lease)?;
        repo.meta()
            .put_derived(GC_FENCE_ACTIVE, &fence_flag_key(), time_value(0))
            .await?;
        report.flag_cleared = true;
    }

    if report.deleted > 0 && report.stored > 0 {
        let blame = repo
            .meta()
            .estimate_derived(crate::integrity::kinds::BLAME_KIND)
            .await?;
        report.blame_rows_of_deleted_estimate =
            (blame.rows as u128 * report.deleted as u128 / report.stored as u128) as u64;
    }
    Ok(report)
}

/// Add every changeset reachable from `stack` through `parents` to `kept`,
/// iteratively. `parents` holds only the unreachable set, so the walk stays
/// inside it.
fn keep_ancestors(
    stack: &mut Vec<ChangesetId>,
    parents: &HashMap<ChangesetId, Vec<ChangesetId>>,
    kept: &mut HashSet<ChangesetId>,
) {
    while let Some(id) = stack.pop() {
        if !kept.insert(id) {
            continue;
        }
        for p in parents.get(&id).into_iter().flatten() {
            if parents.contains_key(p) && !kept.contains(p) {
                stack.push(*p);
            }
        }
    }
}

/// After the index fence, remove the per-changeset rows of every deleted
/// changeset (a late indexer write leaves an orphan), and drop tombstones
/// past their retention. Only ids that are not stored: a client may have
/// stored a deleted changeset again.
async fn tombstone_pass(
    repo: &Repo,
    opts: &SweepOptions,
    clock: &dyn Clock,
    lease: &Option<GcLease>,
    started: i64,
    report: &mut SweepCollectReport,
) -> Result<()> {
    let stones = all_rows(repo, CS_TOMBSTONE).await?;
    if stones.is_empty() {
        return Ok(());
    }
    clock.sleep(opts.index_fence).await;
    let retain_after = started.saturating_sub(secs(opts.tombstone_retention));
    let mut kinds: Vec<&str> = PER_CHANGESET_KINDS.to_vec();
    kinds.extend([CS_CONDEMNED, CS_RESCUE]);
    for chunk in stones.chunks(PAGE) {
        gc_lease::check(lease)?;
        let ids: Vec<ChangesetId> = chunk.iter().map(|(k, _)| ChangesetId(*k)).collect();
        let here = stored(repo, &ids).await?;
        let mut absent: Vec<Hash> = Vec::new();
        let mut old: Vec<Hash> = Vec::new();
        // Stored again by a client, or a delete a killed run never made:
        // the changeset is stored, so a full mark keeps its trees anyway.
        let mut stale: Vec<Hash> = Vec::new();
        for (((key, v), _), here) in chunk.iter().zip(&ids).zip(here) {
            if here {
                stale.push(*key);
                continue;
            }
            absent.push(*key);
            // An undecodable tombstone is kept: it only keeps a root tree.
            if decode_tombstone(v).is_ok_and(|(t, _)| t < retain_after) {
                old.push(*key);
            }
        }
        report.tombstone_rows_deleted += repo.meta().delete_derived_keys(&kinds, &absent).await?;
        repo.meta()
            .delete_derived_keys(&[CS_TOMBSTONE], &stale)
            .await?;
        if !old.is_empty() {
            repo.meta()
                .delete_derived_keys(&[CS_TOMBSTONE, CS_DOOM], &old)
                .await?;
            report.tombstones_dropped += old.len() as u64;
        }
    }
    metrics::counter!("leep_gc_tombstone_rows_deleted_total")
        .increment(report.tombstone_rows_deleted);
    Ok(())
}

/// Root trees of changesets deleted at or after `since`: `gc --full` keeps
/// them so a backup that snapshotted before the delete still finds its
/// tree nodes (ADR 0059 section 6).
pub async fn tombstoned_roots(repo: &Repo, since: i64) -> Result<Vec<Hash>> {
    let mut out = Vec::new();
    for (_, v) in all_rows(repo, CS_TOMBSTONE).await? {
        match decode_tombstone(&v) {
            Ok((t, Some(root))) if t >= since => out.push(root),
            Ok(_) => {}
            // Damaged: its time is unknown, so keep nothing more than
            // a later full mark would; report through verify.
            Err(_) => {}
        }
    }
    Ok(out)
}
