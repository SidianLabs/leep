//! Changeset compaction run by the server itself (ADR 0101), so a store
//! never waits for an operator to run `leep-server compact-changesets`
//! (ADR 0053).
//!
//! Each [`AutoCompactor::step`] does two things:
//!
//! - **Train when the store has outgrown its dictionary.** Dictionaries are
//!   trained in tiers by changeset count ([`TIERS`]): 16 KiB from 100
//!   changesets, 60 KiB at 1,000, and at 10,000 the 64 KiB that
//!   `compact-changesets` trains, on at most 50,000 samples. A
//!   dictionary's tier is read back from its size, so the schedule needs no
//!   state of its own, survives restarts and replicas, and leaves a
//!   dictionary an operator trained (64 KiB by default) alone. A candidate
//!   is stored only when the samples compressed with it, plus the
//!   dictionary itself, are at least 5% smaller than with the dictionary in
//!   force (or plain), so nothing is recorded for a store it would not
//!   help.
//! - **Compress the rows not yet on the newest dictionary**: once after
//!   each new dictionary, and once more after every server has had time to
//!   learn it (they look every minute), since servers write plain rows
//!   until they look. A pass reads only the first bytes of each row and
//!   fetches the rows it rewrites.
//!
//! Crash safety is that of `compact-changesets`: the format feature is
//! recorded before the first dictionary, the dictionary before any row that
//! needs it, and each batch of rows is one statement whose rows decode to
//! the same changesets before and after. A step killed anywhere leaves a
//! store every reader of this version reads, and the next step redoes what
//! is left.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use leep_core::{Error, Result};

use crate::codec::{self, Codec};
use crate::compact::Rows;
use crate::features::CHANGESET_ZSTD_DICT;
use crate::{to_id, PostgresMetaStore, SchemaMode, SqliteMetaStore};

/// A training tier: from `rows` changesets on, a dictionary of at most
/// `dict_bytes`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Tier {
    pub rows: u64,
    pub dict_bytes: usize,
}

/// The tiers, smallest first. The first tier's `rows` is the default of
/// [`AutoCompactOptions::min_rows`]. A dictionary is in the lowest tier
/// whose `dict_bytes` it fits, so the last tier's size must be what
/// `compact-changesets` trains by default: an operator's dictionary is
/// never replaced.
pub const TIERS: [Tier; 3] = [
    Tier {
        rows: 100,
        dict_bytes: 16 << 10,
    },
    Tier {
        rows: 1_000,
        dict_bytes: 60 << 10,
    },
    Tier {
        rows: 10_000,
        dict_bytes: 64 << 10,
    },
];

/// Changesets a dictionary is trained on at most, as `compact-changesets`.
pub const MAX_SAMPLES: usize = 50_000;

/// A candidate must make the samples, plus its own bytes, at least this
/// much smaller than the dictionary in force (or plain rows) to be stored.
const MIN_GAIN: f64 = 0.05;

/// What a step does.
#[derive(Debug, Clone)]
pub struct AutoCompactOptions {
    /// No dictionary is trained below this many changesets.
    pub min_rows: u64,
    /// Rows read and rewritten per statement.
    pub batch: usize,
    /// A pass rests after each batch for `idle` times as long as the batch
    /// took, and at least `pause`, so it keeps the database busy at most a
    /// quarter of the time (with the default 3) and leaves it to lands.
    pub pause: Duration,
    pub idle: u32,
    /// How old a dictionary must be before a pass counts as covering it for
    /// good: servers look for a new one every minute and write plain rows
    /// until they do. Lower only in tests.
    pub settle: Duration,
    /// Testing only: after this many batches of a pass rewrote rows, print
    /// `leep-compact: paused at batch N` to stderr and stop for good, so a
    /// test can kill the process inside a pass.
    pub debug_pause_after: Option<u64>,
}

impl Default for AutoCompactOptions {
    fn default() -> Self {
        AutoCompactOptions {
            min_rows: TIERS[0].rows,
            batch: 250,
            pause: Duration::from_millis(10),
            idle: 3,
            settle: codec::REFRESH + Duration::from_secs(15),
            debug_pause_after: None,
        }
    }
}

/// What one step did.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AutoCompactReport {
    /// Changesets counted, up to the last tier's `rows`.
    pub rows: u64,
    /// The dictionary new rows are written with after the step; 0 for none.
    pub dictionary: u32,
    /// A dictionary this step trained and stored: its id, bytes and samples.
    pub trained: Option<(u32, u64, u64)>,
    /// A dictionary this step trained and did not store, as it gained too
    /// little.
    pub rejected: bool,
    /// Whether the step ran a pass over the rows.
    pub passed: bool,
    /// Rows the pass rewrote, and their bytes before and after.
    pub rewritten: u64,
    pub bytes_before: u64,
    pub bytes_after: u64,
}

/// Runs the steps against one store, remembering between steps which
/// dictionary its passes have covered.
pub struct AutoCompactor {
    store: Arc<dyn Rows + Send + Sync>,
    opts: AutoCompactOptions,
    /// The dictionary a pass has covered for good: one that started after
    /// every server had looked for it. `None` at start, so each process
    /// passes once (one killed inside a pass leaves rows for the next).
    covered: Option<u32>,
    /// Tiers whose candidate gained too little, with the row count it was
    /// trained at: tried again only once the store has doubled.
    rejected: BTreeMap<usize, u64>,
}

impl AutoCompactor {
    /// Open the store at `url` for the compactor, on a connection of its
    /// own (one at a time), counted in metrics as pool `compact`. `None` for a store it does not apply to:
    /// `mem://` and in-memory SQLite, whose rows a second connection would
    /// not see.
    pub async fn connect(url: &str, opts: AutoCompactOptions) -> Result<Option<AutoCompactor>> {
        let store: Arc<dyn Rows + Send + Sync> = if url.starts_with("sqlite:") {
            if url.contains(":memory:") || url.contains("mode=memory") {
                return Ok(None);
            }
            let mut s = SqliteMetaStore::connect_with(url, SchemaMode::RequireCurrent).await?;
            s.data_label = "compact";
            Arc::new(s)
        } else if url.starts_with("postgres://") || url.starts_with("postgresql://") {
            let mut s = PostgresMetaStore::connect_with_floor(
                url,
                SchemaMode::RequireCurrent,
                1,
                0,
                0,
                0,
                None,
            )
            .await?;
            s.data_label = "compact";
            Arc::new(s)
        } else {
            return Ok(None);
        };
        if !store.has_dicts() {
            return Ok(None);
        }
        Ok(Some(AutoCompactor::new(store, opts)))
    }

    pub(crate) fn new(store: Arc<dyn Rows + Send + Sync>, opts: AutoCompactOptions) -> Self {
        AutoCompactor {
            store,
            opts,
            covered: None,
            rejected: BTreeMap::new(),
        }
    }

    /// Train if the store has outgrown its dictionary, then compress the
    /// rows a pass has not covered yet.
    pub async fn step(&mut self) -> Result<AutoCompactReport> {
        let store = self.store.clone();
        store.load_dicts().await?;
        let mut report = AutoCompactReport {
            rows: store.count_upto(TIERS[TIERS.len() - 1].rows).await?,
            ..AutoCompactReport::default()
        };
        if let Some(want) = wanted_tier(report.rows, self.opts.min_rows) {
            let have = tier_of(store.codec().newest_len());
            let retry = self
                .rejected
                .get(&want)
                .is_none_or(|at| report.rows >= at.saturating_mul(2));
            if have.is_none_or(|h| h < want) && retry {
                match self.train(want, &mut report).await? {
                    true => {
                        self.rejected.remove(&want);
                    }
                    false => {
                        self.rejected.insert(want, report.rows);
                    }
                }
            }
        }
        let newest = store.codec().newest_id();
        report.dictionary = newest;
        if newest == 0 {
            return Ok(report);
        }
        if self.covered == Some(newest) {
            return Ok(report);
        }
        // Rows written before every server looked may still be plain; a
        // pass that starts after that covers them for good.
        let created = store.dict_created_at(i64::from(newest)).await?;
        let age = leep_core::store::unix_now().saturating_sub(created);
        let settled = age >= self.opts.settle.as_secs() as i64;
        self.pass(newest, &mut report).await?;
        if settled {
            self.covered = Some(newest);
        }
        Ok(report)
    }

    /// Train a tier-`tier` dictionary and store it if it gains enough;
    /// whether it was stored.
    async fn train(&self, tier: usize, report: &mut AutoCompactReport) -> Result<bool> {
        let store = &*self.store;
        // Rows are counted only up to the last tier: past it, sample all
        // there are, up to the cap.
        let n = if report.rows >= TIERS[TIERS.len() - 1].rows {
            MAX_SAMPLES
        } else {
            usize::try_from(report.rows).unwrap_or(MAX_SAMPLES)
        };
        let samples = crate::compact::sample(store, n, self.opts.batch).await?;
        if samples.len() < 2 {
            return Ok(false);
        }
        let id = store.max_dict().await? + 1;
        let id32 =
            u32::try_from(id).map_err(|_| Error::Storage("dictionary ids exhausted".into()))?;
        let dict = match codec::train(&samples, TIERS[tier].dict_bytes, id32) {
            Ok(d) => d,
            // Too few or too uniform samples: try again when there are more.
            Err(e) => {
                tracing::debug!(error = %e, "no changeset dictionary from these samples");
                report.rejected = true;
                return Ok(false);
            }
        };
        let candidate = Codec::default();
        candidate.add(vec![(id, dict.clone())])?;
        let (mut now, mut then) = (0u64, 0u64);
        for s in &samples {
            let cs = leep_core::Changeset::decode(s)?;
            now += store.codec().encode(&cs).len() as u64;
            then += candidate.encode(&cs).len() as u64;
        }
        // The dictionary is a row too, kept for good.
        then += dict.len() as u64;
        if then as f64 > now as f64 * (1.0 - MIN_GAIN) {
            report.rejected = true;
            return Ok(false);
        }
        // Before the dictionary: servers start compressing new rows with it
        // within a minute (ADR 0055).
        store.add_feature(CHANGESET_ZSTD_DICT).await?;
        if !store.insert_dict(id, &dict).await? {
            // Another server, or `compact-changesets`, stored one first:
            // the next step sees it.
            store.load_dicts().await?;
            return Ok(true);
        }
        store.load_dicts().await?;
        report.trained = Some((id32, dict.len() as u64, samples.len() as u64));
        Ok(true)
    }

    /// Rewrite every row not on dictionary `target` whose bytes change.
    async fn pass(&self, target: u32, report: &mut AutoCompactReport) -> Result<()> {
        let store = &*self.store;
        report.passed = true;
        let mut after = Vec::new();
        let mut batches = 0u64;
        let mut recorded = false;
        let mut started: Option<Instant> = None;
        loop {
            // Rest in proportion to the last page's work.
            if let Some(t) = started {
                let rest = (t.elapsed() * self.opts.idle).max(self.opts.pause);
                if !rest.is_zero() {
                    tokio::time::sleep(rest).await;
                }
            }
            started = Some(Instant::now());
            let heads = store.heads(&after, self.opts.batch).await?;
            let Some(last) = heads.last() else { break };
            after = last.0.clone();
            let ids: Vec<Vec<u8>> = heads
                .into_iter()
                .filter(|(_, head)| !Codec::uses(head, target))
                .map(|(id, _)| id)
                .collect();
            if ids.is_empty() {
                continue;
            }
            let rows = store.rows(&ids).await?;
            if store
                .codec()
                .missing(rows.iter().map(|r| r.1.as_slice()))
                .is_some()
            {
                store.load_dicts().await?;
            }
            let mut changed = Vec::new();
            for (id, data) in rows {
                // Plain rows are rewritten only where compression helps.
                if Codec::uses(&data, target) {
                    continue;
                }
                let cs = store.codec().decode_checked(&to_id(&id)?, &data)?;
                let new = store.codec().encode(&cs);
                if new != data {
                    report.bytes_before += data.len() as u64;
                    report.bytes_after += new.len() as u64;
                    changed.push((id, new));
                }
            }
            if changed.is_empty() {
                continue;
            }
            report.rewritten += changed.len() as u64;
            if !recorded {
                store.add_feature(CHANGESET_ZSTD_DICT).await?;
                recorded = true;
            }
            store.rewrite(&changed).await?;
            batches += 1;
            if self.opts.debug_pause_after == Some(batches) {
                eprintln!("leep-compact: paused at batch {batches}");
                std::future::pending::<()>().await;
            }
        }
        Ok(())
    }
}

/// The highest tier `rows` changesets reach, if any.
fn wanted_tier(rows: u64, min_rows: u64) -> Option<usize> {
    if rows < min_rows.max(2) {
        return None;
    }
    Some(TIERS.iter().rposition(|t| rows >= t.rows).unwrap_or(0))
}

/// The tier of a dictionary of `len` bytes (0 for none): the lowest whose
/// size it fits; larger ones are in the last.
pub fn tier_of(len: usize) -> Option<usize> {
    if len == 0 {
        return None;
    }
    Some(
        TIERS
            .iter()
            .position(|t| len <= t.dict_bytes)
            .unwrap_or(TIERS.len() - 1),
    )
}
