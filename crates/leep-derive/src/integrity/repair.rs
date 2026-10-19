//! `repair`: bring derived rows back in line with history (ADR 0025).
//!
//! A derived row is a pure function of content, so overwriting a row with its
//! recomputation can only make it right, and a kind that derives again on
//! demand can be dropped outright. Nothing else is touched: changesets, blobs
//! and refs are never rewritten, and kinds that history cannot regenerate
//! (signatures, reviews, rebase links, garbage collection state) are refused.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use futures_util::future::BoxFuture;
use leep_core::{Changeset, ChangesetId, Error, Hash, MetaStore, Result};
use serde::Serialize;

use super::kinds::{self, DropPolicy, KindClass};
use super::problems::{recoverable, Damage, Problem, ProblemLog, Progress, Ticker};
use super::rows::{derive_kind, expected_blob_row, prepare, recompute, RowScan, PAGE};
use super::shadow::{ShadowBlobs, ShadowMeta};
use super::walk::{plan, Unreadable};
use crate::derived::history_pack::same_part;
use crate::derived::wavefront::{self, Dag, Preparer, Wave, Window, OUTSIDE};
use crate::derived::{DerivedKind, HistoryTree};
use crate::search::index::TreeRowMemo;
use crate::search::{BLOB_KIND as BLOB_TRIGRAMS, TREE_KIND as TREE_TRIGRAMS};
use crate::Repo;

/// Which changesets a per-changeset rederivation covers.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Scope {
    /// Every stored changeset.
    All,
    /// The history of these refs.
    Refs(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct RederiveOptions {
    /// Kinds to recompute. Kinds they depend on are recomputed too.
    pub kinds: Vec<&'static str>,
    /// Applies to per-changeset kinds; rows keyed by content are always all
    /// of the stored rows.
    pub scope: Scope,
    /// Count what would change and write nothing.
    pub dry_run: bool,
    pub max_problems: usize,
    /// Changesets recomputed at once (ADR 0050). A dry run uses one.
    pub jobs: usize,
    /// Only derive rows that are absent, as queries and the warmer would,
    /// and leave stored rows unchecked: an initial build that can stop and
    /// resume where it was.
    pub missing_only: bool,
}

/// What a rederivation did to one kind. In a dry run `filled` and
/// `overwritten` are what would be written.
#[derive(Debug, Default, Clone, Serialize)]
pub struct RederiveKindReport {
    pub examined: u64,
    pub matched: u64,
    /// Rows that were absent and are now written.
    pub filled: u64,
    /// Rows that were wrong and are now rewritten.
    pub overwritten: u64,
    /// Rows that could not be recomputed because an input is damaged that
    /// this run does not repair.
    pub failed: u64,
    /// Rows whose subject is gone (a collected blob or tree node).
    pub skipped: u64,
}

#[derive(Debug, Serialize)]
pub struct RederiveReport {
    pub dry_run: bool,
    /// Kinds processed, dependencies included, in the order they ran.
    pub kinds: Vec<String>,
    pub kind_reports: BTreeMap<String, RederiveKindReport>,
    pub problems: Vec<Problem>,
    pub problems_total: u64,
    pub notes: Vec<String>,
    pub elapsed_secs: f64,
}

impl RederiveReport {
    /// Rows written (or that would be).
    pub fn changed(&self) -> u64 {
        self.kind_reports
            .values()
            .map(|r| r.filled + r.overwritten)
            .sum()
    }

    pub fn failed(&self) -> u64 {
        self.kind_reports.values().map(|r| r.failed).sum()
    }
}

struct Rederive<'a> {
    real: &'a Repo,
    /// Where recomputation runs: `real`, or a shadow in a dry run.
    compute: Repo,
    shadow_meta: Option<Arc<ShadowMeta>>,
    shadow_blobs: Option<Arc<ShadowBlobs>>,
    progress: &'a dyn Progress,
    changeset_kinds: Vec<&'static str>,
    reports: BTreeMap<&'static str, RederiveKindReport>,
    log: ProblemLog,
    ticker: Ticker,
    visited: u64,
}

/// Recompute derived rows and write the ones that are missing or wrong.
pub async fn rederive(
    repo: &Repo,
    opts: &RederiveOptions,
    progress: &dyn Progress,
) -> Result<RederiveReport> {
    let started = Instant::now();
    for k in &opts.kinds {
        kinds::check_rederivable(k)?;
    }
    let all = kinds::with_dependencies(&opts.kinds);
    let (compute, shadow_meta, shadow_blobs) = if opts.dry_run {
        let meta = ShadowMeta::new(repo.meta().clone());
        let blobs = ShadowBlobs::new(repo.blobs().clone());
        (
            Repo::new(blobs.clone(), meta.clone()),
            Some(meta),
            Some(blobs),
        )
    } else {
        (repo.clone(), None, None)
    };
    let changeset_kinds: Vec<&'static str> = all
        .iter()
        .copied()
        .filter(|k| kinds::lookup(k).is_some_and(|i| i.class == KindClass::PerChangeset))
        .collect();
    let mut r = Rederive {
        real: repo,
        compute,
        shadow_meta,
        shadow_blobs,
        progress,
        changeset_kinds,
        reports: BTreeMap::new(),
        log: ProblemLog::new(opts.max_problems),
        ticker: Ticker::new(Duration::from_secs(5)),
        visited: 0,
    };
    let mut notes = Vec::new();
    let verb = if opts.dry_run {
        "checking"
    } else {
        "rederiving"
    };
    progress.info(&format!("repair: {verb} {}", all.join(" ")));

    if !r.changeset_kinds.is_empty() {
        let meta = repo.meta().clone();
        let starts = match &opts.scope {
            Scope::Refs(names) => {
                let refs = meta.list_refs().await?;
                let mut starts = Vec::with_capacity(names.len());
                for name in names {
                    let id = refs
                        .get(name)
                        .ok_or_else(|| Error::NotFound(format!("ref {name:?}")))?;
                    starts.push(*id);
                }
                starts
            }
            Scope::All => {
                let mut starts = Vec::new();
                let mut after = None;
                loop {
                    let page = meta.list_changesets(after, PAGE).await?;
                    let Some(last) = page.last().copied() else {
                        break;
                    };
                    after = Some(last);
                    starts.extend(page);
                }
                starts
            }
        };
        if opts.missing_only && !opts.dry_run {
            r.fill_missing(&starts, opts.jobs).await?;
            notes.push(
                "only missing rows were derived and stored rows were not checked; `examined` \
                 counts the starting changesets and `filled` every row derived"
                    .into(),
            );
        } else {
            let jobs = if opts.dry_run { 1 } else { opts.jobs };
            let dag = plan(&*meta, &starts, &mut r).await?;
            progress.info(&format!(
                "repair: {} changesets, {jobs} at a time",
                dag.len()
            ));
            let mut wave = RederiveWave { r: &mut r };
            let compute = wave.r.compute.clone();
            wavefront::run(&compute, &dag, jobs, &mut wave).await?;
        }
    } else if matches!(opts.scope, Scope::Refs(_)) {
        notes
            .push("--ref has no effect: rows keyed by content are always rederived in full".into());
    }
    if all.contains(&BLOB_TRIGRAMS) {
        r.progress.info("repair: blob-trigrams-v1 rows");
        r.blob_trigrams().await?;
    }
    if all.contains(&TREE_TRIGRAMS) {
        r.progress.info("repair: tree-trigrams-v1 rows");
        r.tree_trigrams().await?;
    }
    let (problems, problems_total) = r.log.into_parts();
    Ok(RederiveReport {
        dry_run: opts.dry_run,
        kinds: all.iter().map(|k| k.to_string()).collect(),
        kind_reports: r
            .reports
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
        problems,
        problems_total,
        notes,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

impl Rederive<'_> {
    fn report(&mut self, kind: &'static str) -> &mut RederiveKindReport {
        self.reports.entry(kind).or_default()
    }

    async fn write(&mut self, kind: &'static str, key: &Hash, value: bytes::Bytes) -> Result<()> {
        match &self.shadow_meta {
            // A dry run still records the value, so later recomputations see
            // what a real run would have written.
            Some(shadow) => shadow.set(kind, key, value),
            None => self.real.meta().put_derived(kind, key, value).await?,
        }
        Ok(())
    }

    fn clear_shadow(&self) {
        if let Some(b) = &self.shadow_blobs {
            b.clear();
        }
    }

    fn tick(&mut self) {
        if self.ticker.due() {
            self.progress.info(&format!(
                "repair: {} changesets visited, {} problems",
                self.visited,
                self.log.total()
            ));
        }
    }

    async fn blob_trigrams(&mut self) -> Result<()> {
        let kind = BLOB_TRIGRAMS;
        let meta = self.real.meta().clone();
        let mut scan = RowScan::new(kind, true, 0);
        while let Some(page) = scan.next(&*meta).await? {
            for (key, value) in page {
                self.report(kind).examined += 1;
                let data = match self.real.blobs().get(&key).await? {
                    Some(d) if Hash::of_blob(&d) == key => d,
                    // Collected, or damaged (and then not ours to fix).
                    _ => {
                        self.report(kind).skipped += 1;
                        continue;
                    }
                };
                let expected = expected_blob_row(&data);
                if expected == value {
                    self.report(kind).matched += 1;
                } else {
                    self.report(kind).overwritten += 1;
                    self.write(kind, &key, expected).await?;
                }
            }
            self.tick();
        }
        Ok(())
    }

    async fn tree_trigrams(&mut self) -> Result<()> {
        let kind = TREE_TRIGRAMS;
        let meta = self.real.meta().clone();
        let mut scan = RowScan::new(kind, true, 0);
        let mut memo = TreeRowMemo::new();
        while let Some(page) = scan.next(&*meta).await? {
            for (key, value) in page {
                self.report(kind).examined += 1;
                let expected = match self.compute.recompute_tree_row(key, &mut memo).await {
                    Ok(v) => v,
                    Err(e) if recoverable(&e) => {
                        self.report(kind).skipped += 1;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                self.clear_shadow();
                if expected == value {
                    self.report(kind).matched += 1;
                } else {
                    self.report(kind).overwritten += 1;
                    self.write(kind, &key, expected).await?;
                }
            }
            self.tick();
        }
        Ok(())
    }
}

/// What recomputing one kind's row of one changeset found.
enum Outcome {
    Matched,
    Filled,
    Overwritten,
    Failed(Problem),
}

/// Recompute every kind of `id` (kinds in dependency order) and write the
/// rows that differ. Runs as a task of its own, after the changeset's
/// parents' rows are written.
async fn recompute_all(
    real: Repo,
    compute: Repo,
    shadow: Option<Arc<ShadowMeta>>,
    kinds: Vec<&'static str>,
    id: ChangesetId,
    cs: Arc<Changeset>,
) -> Result<Vec<Outcome>> {
    let mut out = Vec::with_capacity(kinds.len());
    for kind in kinds {
        let expected = match recompute(kind, &compute, id, &cs).await {
            Ok(v) => v,
            Err(e) if recoverable(&e) => {
                out.push(Outcome::Failed(
                    Problem::new(
                        "derived",
                        "rederive_failed",
                        format!("the {kind} row of {id} cannot be recomputed: {e}"),
                    )
                    .kind(kind)
                    .key(&id.0)
                    .changeset(id),
                ));
                continue;
            }
            Err(e) => return Err(e),
        };
        match real.meta().get_derived(kind, &id.0).await? {
            Some(stored)
                if stored == expected
                    || (kind == HistoryTree::NAME && same_part(&stored, &expected)) =>
            {
                out.push(Outcome::Matched)
            }
            stored => {
                out.push(if stored.is_some() {
                    Outcome::Overwritten
                } else {
                    Outcome::Filled
                });
                match &shadow {
                    // A dry run still records the value, so later
                    // recomputations see what a real run would have written.
                    Some(shadow) => shadow.set(kind, &id.0, expected),
                    None => real.meta().put_derived(kind, &id.0, expected).await?,
                }
            }
        }
    }
    Ok(out)
}

/// A rederivation of per-changeset kinds as a [`wavefront`] run: each
/// changeset's rows are recomputed, compared and written in a task, and the
/// counts and problems are recorded here, parents first.
struct RederiveWave<'r, 'a> {
    r: &'r mut Rederive<'a>,
}

#[async_trait]
impl Wave for RederiveWave<'_, '_> {
    type Out = Vec<Outcome>;

    fn preparer(&self) -> Preparer {
        let kinds = self.r.changeset_kinds.clone();
        Arc::new(move |repo: Repo, window: Window| {
            let kinds = kinds.clone();
            Box::pin(async move {
                let pairs: Vec<(ChangesetId, &Changeset)> =
                    window.iter().map(|(id, cs)| (*id, &**cs)).collect();
                prepare(&kinds, &repo, &pairs).await;
            })
        })
    }

    fn start(
        &mut self,
        dag: &Dag,
        i: usize,
        cs: Arc<Changeset>,
    ) -> Result<BoxFuture<'static, Result<Vec<Outcome>>>> {
        let id = dag.id(i);
        let kinds = self.r.changeset_kinds.clone();
        if dag.parents(i).contains(&OUTSIDE) {
            // Every readable ancestor is in the plan, so a parent outside it
            // could not be read.
            let out = kinds
                .into_iter()
                .map(|kind| {
                    Outcome::Failed(
                        Problem::new(
                            "derived",
                            "rederive_failed",
                            format!("the {kind} row of {id} depends on an unreadable parent"),
                        )
                        .kind(kind)
                        .key(&id.0)
                        .changeset(id),
                    )
                })
                .collect();
            return Ok(Box::pin(async move { Ok(out) }));
        }
        Ok(Box::pin(recompute_all(
            self.r.real.clone(),
            self.r.compute.clone(),
            self.r.shadow_meta.clone(),
            kinds,
            id,
            cs,
        )))
    }

    async fn finish(&mut self, _: &Dag, _: usize, out: Vec<Outcome>) -> Result<()> {
        let r = &mut *self.r;
        r.visited += 1;
        for (kind, outcome) in r.changeset_kinds.clone().into_iter().zip(out) {
            let report = r.report(kind);
            report.examined += 1;
            match outcome {
                Outcome::Matched => report.matched += 1,
                Outcome::Filled => report.filled += 1,
                Outcome::Overwritten => report.overwritten += 1,
                Outcome::Failed(p) => {
                    report.failed += 1;
                    r.log.push(p);
                }
            }
        }
        // Only a dry run writes to the shadow, and it runs one job, so no
        // recomputation is under way.
        r.clear_shadow();
        r.tick();
        Ok(())
    }
}

impl Rederive<'_> {
    /// Derive each kind where it is missing for `starts` and their
    /// ancestors, as queries would, `jobs` changesets at a time; stored rows
    /// are not read again, so a stopped run resumes where it was.
    async fn fill_missing(&mut self, starts: &[ChangesetId], jobs: usize) -> Result<()> {
        let repo = self.real.clone().with_derive_jobs(jobs);
        let meta = repo.meta().clone();
        for kind in self.changeset_kinds.clone() {
            self.progress.info(&format!(
                "repair: deriving missing {kind} rows, {jobs} at a time"
            ));
            let before = repo.derived_count();
            let mut checked = 0u64;
            for chunk in starts.chunks(PAGE) {
                let keys: Vec<Hash> = chunk.iter().map(|id| id.0).collect();
                let have = meta.get_derived_many(kind, &keys).await?;
                for (id, v) in chunk.iter().zip(have) {
                    if v.is_some() {
                        continue;
                    }
                    let derivation = derive_kind(kind, &repo, *id);
                    tokio::pin!(derivation);
                    let mut ticks = tokio::time::interval(Duration::from_secs(5));
                    ticks.tick().await;
                    loop {
                        tokio::select! {
                            done = &mut derivation => {
                                done?;
                                break;
                            }
                            _ = ticks.tick() => self.progress.info(&format!(
                                "repair: {} {kind} rows derived",
                                repo.derived_count() - before
                            )),
                        }
                    }
                }
                checked += chunk.len() as u64;
                self.visited = checked;
                let filled = repo.derived_count() - before;
                let report = self.report(kind);
                report.examined = checked;
                report.filled = filled;
                self.tick();
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Unreadable for Rederive<'_> {
    async fn broken(
        &mut self,
        id: ChangesetId,
        referenced_by: Option<ChangesetId>,
        damage: Damage,
        detail: String,
    ) -> Result<()> {
        let what = match damage {
            Damage::Corrupt => "corrupt",
            Damage::Missing => "missing",
        };
        let mut p = Problem::new(
            "history",
            "rederive_failed",
            format!("changeset {id} is {what} ({detail}), so nothing that depends on it can be rederived"),
        )
        .key(&id.0);
        if let Some(c) = referenced_by {
            p = p.changeset(c);
        }
        self.log.push(p);
        Ok(())
    }
}

/// What `drop` removed, or would.
#[derive(Debug, Serialize)]
pub struct DropReport {
    pub dry_run: bool,
    /// Rows per kind, in the order the kinds were (or would be) dropped.
    pub rows: BTreeMap<String, u64>,
}

/// Delete every row of each named kind (and the kinds that only make sense
/// with it), so they derive again on demand. All names are checked before
/// anything is deleted; one refusal refuses the whole command.
pub async fn drop_kinds(
    repo: &Repo,
    names: &[&str],
    dry_run: bool,
    progress: &dyn Progress,
) -> Result<DropReport> {
    let mut order: Vec<&'static str> = Vec::new();
    for name in names {
        let info = kinds::check_droppable(name)?;
        let also = match info.drop {
            DropPolicy::Allowed { also } => also,
            DropPolicy::Refused(_) => unreachable!("checked above"),
        };
        for k in std::iter::once(info.name).chain(also.iter().copied()) {
            if !order.contains(&k) {
                order.push(k);
            }
        }
    }
    let meta = repo.meta();
    let mut rows = BTreeMap::new();
    for kind in order {
        let n = if dry_run {
            count_rows(&**meta, kind).await?
        } else {
            progress.info(&format!("repair: dropping {kind}"));
            meta.delete_derived_kind(kind).await?
        };
        rows.insert(kind.to_string(), n);
    }
    Ok(DropReport { dry_run, rows })
}

async fn count_rows(meta: &dyn MetaStore, kind: &str) -> Result<u64> {
    let mut n = 0;
    let mut after = None;
    loop {
        let page = meta.list_derived(kind, after, 1000).await?;
        let Some((last, _)) = page.last() else {
            return Ok(n);
        };
        after = Some(*last);
        n += page.len() as u64;
    }
}
