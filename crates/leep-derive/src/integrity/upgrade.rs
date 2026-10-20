//! `upgrade-index`: bring a store's derived indexes to what this version
//! reads, in one command that can be stopped and run again.
//!
//! It is an orchestration of existing paths, not a new one. The plan counts
//! rows with [`MetaStore::estimate_derived`] (ADR 0052), which on Postgres
//! estimates a large kind from planner statistics instead of reading its
//! whole index, and compares them with the changesets reachable from refs; missing rows are derived through
//! `derive`, as `repair rederive --missing` does (ADR 0050), so they are
//! byte-identical to what a query or the warmer would store; retired kinds go
//! through `repair drop`'s walk. Rows are stored after their parents', so the
//! rows of a kind are closed under ancestry and deriving from each ref's head
//! finds everything missing; only a store whose rows were deleted part way
//! (an interrupted `repair drop` of a current kind) needs the slower check of
//! every reachable changeset, and the counts say when. Decisions that rest
//! on a count use an exact one: whether a kind has rows at all (always
//! exact), whether a kind whose heads all have rows is short (an estimate
//! below the reachable changesets is counted before calling it partial),
//! and, after deriving, whether the slow check is needed. Estimates only
//! size the work and label the output. A kind whose rows were deleted part
//! way while its statistics still show them looks complete until an
//! `ANALYZE`; [`PlanOptions::exact_counts`] counts everything.

use std::collections::{BTreeMap, HashSet};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use leep_core::{ChangesetId, Hash, MetaStore, Result, RowCount};
use serde::Serialize;

use super::kinds::{self, DropPolicy, KindClass, KindInfo, Rederive};
use super::problems::{Damage, Progress};
use super::repair::{drop_kinds, human_secs, DropReport};
use super::rows::{derive_kind, PAGE};
use super::walk::{plan, Unreadable};
use crate::Repo;

/// Rows read per kind to estimate the size of its values.
const SAMPLE: usize = 64;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum IndexStatus {
    /// Every ref's head has a row and there are at least as many rows as
    /// reachable changesets.
    Complete,
    /// No rows at all.
    Missing,
    /// Some rows.
    Partial,
}

/// A per-changeset kind this version reads.
#[derive(Clone, Debug, Serialize)]
pub struct CurrentKind {
    pub kind: &'static str,
    /// Rows stored, including those of changesets no ref reaches.
    pub rows: u64,
    /// `rows` is an estimate (large kinds on Postgres).
    pub rows_estimated: bool,
    /// Ref heads without a row.
    pub heads_missing: u64,
    /// Reachable changesets without a row: an estimate from the counts.
    pub missing: u64,
    pub status: IndexStatus,
    /// Mean bytes of values per changeset, the kind's row and the rows that
    /// go with it (`history-node-v4` for `history-tree-v4`), from a sample;
    /// `None` without rows.
    pub value_bytes_per_changeset: Option<u64>,
}

/// A retired kind that still has rows, with the kinds dropped beside it.
#[derive(Clone, Debug, Serialize)]
pub struct RetiredKind {
    pub kind: &'static str,
    /// Rows of the kind and of each kind dropped with it.
    pub rows: BTreeMap<String, u64>,
    /// Some of `rows` are estimates.
    pub rows_estimated: bool,
    /// Bytes of values, estimated from a sample; `None` when the store
    /// cannot list rows.
    pub value_bytes: Option<u64>,
}

impl RetiredKind {
    pub fn total_rows(&self) -> u64 {
        self.rows.values().sum()
    }
}

/// What `upgrade-index` found, and so what it would do.
#[derive(Clone, Debug, Serialize)]
pub struct UpgradePlan {
    pub refs: usize,
    /// Readable changesets reachable from the refs.
    pub reachable: u64,
    /// Changesets the walk could not read (`verify` names them).
    pub unreadable: u64,
    pub current: Vec<CurrentKind>,
    pub retired: Vec<RetiredKind>,
    pub elapsed_secs: f64,
    #[serde(skip)]
    heads: Vec<ChangesetId>,
    /// Reachable changesets, parents first.
    #[serde(skip)]
    order: Vec<ChangesetId>,
    #[serde(skip)]
    exact_counts: bool,
}

impl UpgradePlan {
    /// Current kinds with rows to derive, in dependency order.
    pub fn to_derive(&self) -> Vec<&'static str> {
        self.current
            .iter()
            .filter(|k| k.status != IndexStatus::Complete)
            .map(|k| k.kind)
            .collect()
    }

    /// Retired kinds `repair drop` would be given (their companions go too).
    pub fn to_drop(&self) -> Vec<&'static str> {
        self.retired.iter().map(|k| k.kind).collect()
    }

    pub fn nothing_to_do(&self) -> bool {
        self.to_derive().is_empty() && self.retired.is_empty()
    }
}

/// How [`plan_upgrade_with`] counts.
#[derive(Clone, Copy, Debug, Default)]
pub struct PlanOptions {
    /// Count every kind's rows instead of estimating large ones.
    pub exact_counts: bool,
}

#[derive(Clone, Debug)]
pub struct UpgradeOptions {
    /// Changesets derived at once (ADR 0050).
    pub jobs: usize,
    /// Drop retired kinds once the current ones are complete. The caller
    /// must know no older server reads them.
    pub drop_retired: bool,
}

#[derive(Debug, Serialize)]
pub struct UpgradeReport {
    /// Rows derived per kind.
    pub derived: BTreeMap<String, u64>,
    pub dropped: Option<DropReport>,
    /// The counts once it finished.
    pub after: UpgradePlan,
    pub elapsed_secs: f64,
}

struct CountUnreadable(u64);

#[async_trait]
impl Unreadable for CountUnreadable {
    async fn broken(
        &mut self,
        _: ChangesetId,
        _: Option<ChangesetId>,
        _: Damage,
        _: String,
    ) -> Result<()> {
        self.0 += 1;
        Ok(())
    }
}

/// The per-changeset kinds this version reads and can derive, in dependency
/// order.
fn current_kinds() -> impl Iterator<Item = &'static KindInfo> {
    kinds::kinds()
        .iter()
        .filter(|k| k.class == KindClass::PerChangeset && k.rederive == Rederive::Yes)
}

fn companions(info: &KindInfo) -> &'static [&'static str] {
    match info.drop {
        DropPolicy::Allowed { also } => also,
        DropPolicy::Refused(_) => &[],
    }
}

/// Mean value length of `kind` from its first rows in key order (keys are
/// hashes, so that is a fair sample), times `rows`.
async fn value_bytes(meta: &dyn MetaStore, kind: &str, rows: u64) -> Option<u64> {
    if rows == 0 {
        return Some(0);
    }
    let sample = meta.list_derived(kind, None, SAMPLE).await.ok()?;
    if sample.is_empty() {
        return Some(0);
    }
    let total: u64 = sample.iter().map(|(_, v)| v.len() as u64).sum();
    Some(total * rows / sample.len() as u64)
}

/// Rows of `kind`, counted when `exact`, else estimated where the store can.
async fn rows_of(meta: &dyn MetaStore, kind: &str, exact: bool) -> Result<RowCount> {
    if exact {
        Ok(RowCount {
            rows: meta.count_derived(kind).await?,
            exact: true,
        })
    } else {
        meta.estimate_derived(kind).await
    }
}

/// [`plan_upgrade_with`] with estimates allowed.
pub async fn plan_upgrade(repo: &Repo, progress: &dyn Progress) -> Result<UpgradePlan> {
    plan_upgrade_with(repo, PlanOptions::default(), progress).await
}

/// Walk the history of every ref and count what each derived kind has.
/// Reads every reachable changeset once and writes nothing.
pub async fn plan_upgrade_with(
    repo: &Repo,
    opts: PlanOptions,
    progress: &dyn Progress,
) -> Result<UpgradePlan> {
    let started = Instant::now();
    let meta = repo.meta().clone();
    let refs = meta.list_refs().await?;
    let mut heads: Vec<ChangesetId> = refs.values().copied().collect();
    heads.sort();
    heads.dedup();
    progress.info(&format!(
        "upgrade-index: reading the history of {} ref(s)",
        refs.len()
    ));
    let mut unreadable = CountUnreadable(0);
    let dag = plan(&*meta, &heads, &mut unreadable).await?;
    let order: Vec<ChangesetId> = (0..dag.len()).map(|i| dag.id(i)).collect();
    drop(dag);
    // A head that could not be read is reported as unreadable, not missing.
    let wanted: HashSet<ChangesetId> = heads.iter().copied().collect();
    let readable: HashSet<ChangesetId> = order
        .iter()
        .filter(|id| wanted.contains(id))
        .copied()
        .collect();
    heads.retain(|h| readable.contains(h));
    progress.info(&format!(
        "upgrade-index: {} changesets reachable ({:.1}s); counting rows",
        order.len(),
        started.elapsed().as_secs_f64()
    ));
    let mut p = UpgradePlan {
        refs: refs.len(),
        reachable: order.len() as u64,
        unreadable: unreadable.0,
        current: Vec::new(),
        retired: Vec::new(),
        elapsed_secs: 0.0,
        heads,
        order,
        exact_counts: opts.exact_counts,
    };
    p.recount(&*meta).await?;
    p.elapsed_secs = started.elapsed().as_secs_f64();
    Ok(p)
}

impl UpgradePlan {
    /// Count rows again against the same history.
    async fn recount(&mut self, meta: &dyn MetaStore) -> Result<()> {
        self.current.clear();
        for info in current_kinds() {
            let mut count = rows_of(meta, info.name, self.exact_counts).await?;
            let mut heads_missing = 0u64;
            for chunk in self.heads.chunks(PAGE) {
                let keys: Vec<Hash> = chunk.iter().map(|id| id.0).collect();
                let have = meta.get_derived_many(info.name, &keys).await?;
                heads_missing += have.iter().filter(|v| v.is_none()).count() as u64;
            }
            // With every head stored, the count alone decides between
            // complete and partial; a lagging estimate must not invent work.
            if heads_missing == 0 && !count.exact && count.rows < self.reachable {
                count = rows_of(meta, info.name, true).await?;
            }
            let rows = count.rows;
            let status = if self.reachable == 0 || (heads_missing == 0 && rows >= self.reachable) {
                IndexStatus::Complete
            } else if rows == 0 {
                IndexStatus::Missing
            } else {
                IndexStatus::Partial
            };
            let missing = match status {
                IndexStatus::Complete => 0,
                _ => self.reachable.saturating_sub(rows).max(heads_missing),
            };
            let mut bytes = if rows == 0 {
                None
            } else {
                value_bytes(meta, info.name, rows).await
            };
            for also in companions(info) {
                if let Some(b) = bytes {
                    let n = rows_of(meta, also, self.exact_counts).await?.rows;
                    bytes = value_bytes(meta, also, n).await.map(|a| a + b);
                }
            }
            self.current.push(CurrentKind {
                kind: info.name,
                rows,
                rows_estimated: !count.exact,
                heads_missing,
                missing,
                status,
                value_bytes_per_changeset: bytes.map(|b| b / rows.max(1)),
            });
        }
        self.retired.clear();
        for info in kinds::retired() {
            let DropPolicy::Allowed { also } = info.drop else {
                continue;
            };
            let mut rows = BTreeMap::new();
            let mut bytes = Some(0u64);
            let mut estimated = false;
            for kind in std::iter::once(info.name).chain(also.iter().copied()) {
                // Whether any remain is exact either way.
                let count = rows_of(meta, kind, self.exact_counts).await?;
                estimated |= !count.exact;
                let n = count.rows;
                rows.insert(kind.to_string(), n);
                if let Some(b) = bytes {
                    bytes = value_bytes(meta, kind, n).await.map(|v| v + b);
                }
            }
            if rows.values().any(|n| *n > 0) {
                self.retired.push(RetiredKind {
                    kind: info.name,
                    rows,
                    rows_estimated: estimated,
                    value_bytes: bytes,
                });
            }
        }
        Ok(())
    }
}

/// Carry out `plan`: derive every incomplete current kind (and, when asked,
/// drop the retired ones), then count again. Stopping it at any point leaves
/// rows a later run builds on.
pub async fn run_upgrade(
    repo: &Repo,
    plan: &UpgradePlan,
    opts: &UpgradeOptions,
    progress: &dyn Progress,
) -> Result<UpgradeReport> {
    let started = Instant::now();
    let meta = repo.meta().clone();
    let to_drop = plan.to_drop();
    let derive_repo = repo.clone().with_derive_jobs(opts.jobs);
    let mut derived = BTreeMap::new();
    for k in plan
        .current
        .iter()
        .filter(|k| k.status != IndexStatus::Complete)
    {
        progress.info(&format!(
            "upgrade-index: deriving missing {} rows from {} ref head(s), {} at a time",
            k.kind,
            plan.heads.len(),
            opts.jobs
        ));
        let mut n = fill(&derive_repo, k.kind, &plan.heads, k.missing, progress).await?;
        let rows = meta.count_derived(k.kind).await?;
        if rows < plan.reachable {
            // Rows behind stored ones are gone: a delete stopped part way.
            // Children first, so one derivation fills a whole gap.
            let short = plan.reachable - rows;
            progress.info(&format!(
                "upgrade-index: {} has {short} fewer rows than reachable changesets; \
                 checking each of the {} reachable changesets",
                k.kind, plan.reachable
            ));
            let children_first: Vec<ChangesetId> = plan.order.iter().rev().copied().collect();
            n += fill(&derive_repo, k.kind, &children_first, short, progress).await?;
        }
        derived.insert(k.kind.to_string(), n);
    }
    let dropped = if opts.drop_retired && !to_drop.is_empty() {
        Some(drop_kinds(repo, &to_drop, false, progress).await?)
    } else {
        None
    };
    let mut after = UpgradePlan {
        refs: plan.refs,
        reachable: plan.reachable,
        unreadable: plan.unreadable,
        current: Vec::new(),
        retired: Vec::new(),
        elapsed_secs: 0.0,
        heads: plan.heads.clone(),
        order: Vec::new(),
        exact_counts: plan.exact_counts,
    };
    after.recount(&*meta).await?;
    Ok(UpgradeReport {
        derived,
        dropped,
        after,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

/// Derive `kind` wherever it is missing for `starts` and their ancestors, as
/// a query would; stored rows are skipped without being read again. Returns
/// the rows derived.
async fn fill(
    repo: &Repo,
    kind: &'static str,
    starts: &[ChangesetId],
    expect: u64,
    progress: &dyn Progress,
) -> Result<u64> {
    let meta = repo.meta().clone();
    let before = repo.derived_count();
    let started = Instant::now();
    let mut ticks = tokio::time::interval(Duration::from_secs(10));
    ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    ticks.tick().await;
    for chunk in starts.chunks(PAGE) {
        let keys: Vec<Hash> = chunk.iter().map(|id| id.0).collect();
        let have = meta.get_derived_many(kind, &keys).await?;
        for (id, v) in chunk.iter().zip(have) {
            if v.is_some() {
                continue;
            }
            let derivation = derive_kind(kind, repo, *id);
            tokio::pin!(derivation);
            loop {
                tokio::select! {
                    done = &mut derivation => {
                        done?;
                        break;
                    }
                    _ = ticks.tick() => progress.info(&progress_line(
                        kind,
                        repo.derived_count() - before,
                        expect,
                        started.elapsed(),
                    )),
                }
            }
        }
    }
    Ok(repo.derived_count() - before)
}

/// "upgrade-index: history-tree-v4: 52000 of about 1480000 rows derived (412/s), about 58m left"
fn progress_line(kind: &str, done: u64, expect: u64, elapsed: Duration) -> String {
    let secs = elapsed.as_secs_f64().max(1e-3);
    let rate = done as f64 / secs;
    let mut line = format!("upgrade-index: {kind}: {done}");
    if expect > 0 {
        line += &format!(" of about {expect}");
    }
    line += &format!(" rows derived ({rate:.0}/s)");
    if expect > done && rate > 0.0 {
        line += &format!(", about {} left", human_secs((expect - done) as f64 / rate));
    }
    line
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn progress_lines_give_rate_and_eta() {
        assert_eq!(
            progress_line(
                "history-tree-v4",
                52_000,
                1_480_000,
                Duration::from_secs(130)
            ),
            "upgrade-index: history-tree-v4: 52000 of about 1480000 rows derived (400/s), \
             about 60m left"
        );
        assert_eq!(
            progress_line("generation", 10, 0, Duration::from_secs(10)),
            "upgrade-index: generation: 10 rows derived (1/s)"
        );
        assert_eq!(
            progress_line("generation", 0, 5, Duration::from_secs(10)),
            "upgrade-index: generation: 0 of about 5 rows derived (0/s)"
        );
    }
}
