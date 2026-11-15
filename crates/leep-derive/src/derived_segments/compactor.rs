//! The compactor (ADR 0104, section "The compactor"): one pass seals rows
//! not yet in a live segment, merges segments by tiers of four, and in mode
//! `retire` deletes rows a live segment holds the same value for. Passes
//! hold the `derived-segments` lease, so one server at a time runs one;
//! every manifest write is a compare-and-swap from the bytes the pass last
//! read or wrote, and blobs are durable before the manifest names them.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use leep_core::{ChangesetId, Error, Hash, Result};
use tokio::time::Instant;

use super::codec::{self, Manifest, SealRow, SegmentRef, COL_HISTORY_TREE, COL_SKIPLIST};
use super::view::{Found, Live, ManifestRead};
use super::{lease_key, read_manifest, swap_manifest, MANIFEST_KIND};
use crate::derived::history_pack::same_part;
use crate::derived::{decode_u64, DerivedKind, Generation};
use crate::gc_lease::{self, GcLease};
use crate::Repo;

/// What a server does with derived segments (`--derived-segments`).
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum Mode {
    /// No passes; existing segments are still read.
    Off,
    /// Seal and merge; rows stay. Free to roll back.
    #[default]
    Build,
    /// Also delete rows a live segment holds: the one-way door. The store
    /// feature `derived-segments` is recorded before the first delete.
    Retire,
}

impl std::str::FromStr for Mode {
    type Err = Error;
    fn from_str(s: &str) -> Result<Mode> {
        match s {
            "off" => Ok(Mode::Off),
            "build" => Ok(Mode::Build),
            "retire" => Ok(Mode::Retire),
            _ => Err(Error::Invalid(format!(
                "bad --derived-segments {s:?}; use off, build or retire"
            ))),
        }
    }
}

impl std::fmt::Display for Mode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Mode::Off => "off",
            Mode::Build => "build",
            Mode::Retire => "retire",
        })
    }
}

/// Records the store feature [`super::FEATURE`] (the server does it by URL,
/// since `leep-derive` names no concrete store). Called before the first
/// row delete of every process; idempotent.
pub type FeatureHook = Arc<dyn Fn() -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Limits of a pass. The defaults are ADR 0104's starting values.
#[derive(Clone, Debug)]
pub struct CompactOptions {
    pub mode: Mode,
    /// Seal once this many changesets are unsealed...
    pub seal_rows: usize,
    /// ...or this many, once none arrived for `idle`.
    pub idle_rows: usize,
    pub idle: Duration,
    /// A changeset with one of the two rows waits this long for the other
    /// before it is sealed with the one.
    pub lone_wait: Duration,
    /// Most keys in one segment, and most bytes of its data blob: a segment
    /// is read whole with one `get`.
    pub max_rows: usize,
    pub max_bytes: usize,
    /// Segments of a tier merged at once.
    pub fanout: usize,
    /// Rows are deleted only once their segment was seen live this long.
    pub retire_after: Duration,
    /// Rows read or deleted per statement.
    pub batch: usize,
    /// Rest after each delete step, as a multiple of how long it took
    /// (ADR 0052's bounded steps).
    pub rest_factor: u32,
}

impl Default for CompactOptions {
    fn default() -> Self {
        CompactOptions {
            mode: Mode::Build,
            seal_rows: 1024,
            idle_rows: 64,
            idle: Duration::from_secs(600),
            lone_wait: Duration::from_secs(600),
            max_rows: 16_384,
            max_bytes: 4 << 20,
            fanout: 4,
            retire_after: super::RETIRE_AFTER,
            batch: 1000,
            rest_factor: 1,
        }
    }
}

impl CompactOptions {
    /// Small limits, so small histories seal, merge and retire.
    pub fn small(mode: Mode) -> Self {
        CompactOptions {
            mode,
            seal_rows: 4,
            idle_rows: 2,
            idle: Duration::ZERO,
            lone_wait: Duration::ZERO,
            max_rows: 64,
            max_bytes: 1 << 20,
            fanout: 4,
            retire_after: Duration::ZERO,
            batch: 7,
            rest_factor: 0,
        }
    }

    /// The tier of a segment of `rows` keys: `floor(log_fanout(rows /
    /// seal_rows))`, so a seal is tier 0 and a merge of a tier's segments
    /// lands about one tier up.
    fn tier(&self, rows: u32) -> u32 {
        let f = self.fanout.max(2) as u64;
        let mut t = 0;
        let mut cap = (self.seal_rows.max(1) as u64).saturating_mul(f);
        while (rows as u64) >= cap && t < 16 {
            t += 1;
            cap = cap.saturating_mul(f);
        }
        t
    }
}

/// What a process remembers between passes.
#[derive(Default)]
pub struct CompactState {
    /// Keys with one of the two rows, since first seen so.
    lone_since: HashMap<Hash, Instant>,
    /// Live segments, since first seen live by this process.
    live_since: HashMap<Hash, Instant>,
    /// When the unsealed count last grew.
    last_growth: Option<Instant>,
    last_unsealed: usize,
    feature_recorded: bool,
    /// Runs whose merge came out over the byte cap: not tried again.
    oversize: HashSet<Vec<Hash>>,
}

/// What one pass did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    /// Rows not in a live segment when the pass looked (both kinds).
    pub unsealed_rows: u64,
    pub sealed_segments: u64,
    /// Changesets sealed.
    pub sealed_keys: u64,
    pub merges: u64,
    /// Segments merges replaced.
    pub merged_away: u64,
    /// Live segments that could not be read and were dropped from the
    /// manifest (their rows seal again, or derive again).
    pub dropped: u64,
    /// Rows deleted (mode `retire`).
    pub retired_rows: u64,
    /// Rows unequal to their segment's value, kept.
    pub mismatched: u64,
    /// Row values that do not decode, never sealed.
    pub bad_values: u64,
    pub live_segments: u64,
    pub live_bytes: u64,
    /// Whether the manifest named in the row could not be decoded.
    pub damaged_manifest: bool,
}

const KINDS: [(u8, &str); 2] = [
    (COL_SKIPLIST, crate::derived::Skiplist::NAME),
    (COL_HISTORY_TREE, crate::derived::HistoryTree::NAME),
];

fn other(col: u8) -> u8 {
    if col == COL_SKIPLIST {
        COL_HISTORY_TREE
    } else {
        COL_SKIPLIST
    }
}

fn bit(col: u8) -> u8 {
    1 << col
}

/// One pass under [`lease_key`]'s lease; `None` when another server holds
/// it. `feature` records [`super::FEATURE`]; mode `retire` refuses to
/// delete without it.
pub async fn compact_pass(
    repo: &Repo,
    opts: &CompactOptions,
    state: &mut CompactState,
    feature: Option<&FeatureHook>,
) -> Result<Option<PassReport>> {
    if opts.mode == Mode::Off {
        return Ok(Some(PassReport::default()));
    }
    let lease = match GcLease::acquire_key(repo.row_meta(), lease_key(), "derived-segments").await {
        Ok(l) => l,
        Err(Error::Conflict(_)) => return Ok(None),
        Err(e) => return Err(e),
    };
    let result = async {
        let mut p = Pass::load(repo, opts, &lease).await?;
        p.run(state, feature).await?;
        Ok(p.report)
    }
    .await;
    gc_lease::release(lease, result).await.map(Some)
}

pub(crate) struct Pass<'a> {
    repo: &'a Repo,
    opts: &'a CompactOptions,
    lease: &'a Option<GcLease>,
    /// The manifest row as this pass last read or wrote it.
    raw: Option<Bytes>,
    man: Manifest,
    /// Loaded live segments, by index hash.
    segs: HashMap<Hash, Arc<Live>>,
    /// Keys with rows, and which of the two kinds they have rows of.
    rows: Option<HashMap<Hash, u8>>,
    report: PassReport,
}

impl<'a> Pass<'a> {
    pub(crate) async fn load(
        repo: &'a Repo,
        opts: &'a CompactOptions,
        lease: &'a Option<GcLease>,
    ) -> Result<Pass<'a>> {
        let (raw, read) = read_manifest(repo).await?;
        let mut report = PassReport::default();
        let man = match read {
            ManifestRead::Absent => Manifest::new(codec::FAMILY_CHANGESET),
            ManifestRead::Read(m) => m,
            ManifestRead::Damaged => {
                report.damaged_manifest = true;
                if opts.mode == Mode::Retire {
                    return Err(Error::Other(format!(
                        "the {MANIFEST_KIND} row does not decode; in mode retire it may name the \
                         only copy of deleted rows, so it is not replaced automatically (see \
                         \"Derived segments\" in RUNBOOKS.md)"
                    )));
                }
                // In build mode every row is still stored: start over.
                tracing::warn!("the {MANIFEST_KIND} row does not decode; replacing it");
                Manifest::new(codec::FAMILY_CHANGESET)
            }
        };
        let view = repo.segments();
        view.apply(raw.clone()).await?;
        let segs = view
            .segments()
            .into_iter()
            .map(|l| (l.r.index, l))
            .collect();
        Ok(Pass {
            repo,
            opts,
            lease,
            raw,
            man,
            segs,
            rows: None,
            report,
        })
    }

    pub(crate) async fn run(
        &mut self,
        state: &mut CompactState,
        feature: Option<&FeatureHook>,
    ) -> Result<()> {
        self.check_live().await?;
        let now = Instant::now();
        let live: HashSet<Hash> = self.man.live.iter().map(|s| s.index).collect();
        state.live_since.retain(|h, _| live.contains(h));
        for h in live {
            state.live_since.entry(h).or_insert(now);
        }
        self.seal(state).await?;
        self.merge(state).await?;
        if self.opts.mode == Mode::Retire {
            self.retire(state, feature).await?;
        }
        self.report.live_segments = self.man.live.len() as u64;
        self.report.live_bytes = self.man.live.iter().map(|s| s.data_len as u64).sum::<u64>();
        Ok(())
    }

    /// Drop live segments that cannot be read (their keys then read as rows
    /// or derive again, and seal again), forget retired blobs no longer
    /// stored, and replace a manifest that did not decode.
    async fn check_live(&mut self) -> Result<()> {
        let mut gone = Vec::new();
        for s in &self.man.live {
            let readable =
                self.segs.contains_key(&s.index) && self.repo.blobs().contains(&s.data).await?;
            if !readable {
                gone.push(*s);
            }
        }
        let mut retired = Vec::with_capacity(self.man.retired.len());
        for h in &self.man.retired {
            if self.repo.blobs().contains(h).await? {
                retired.push(*h);
            }
        }
        let pruned = retired.len() != self.man.retired.len();
        self.man.retired = retired;
        if !gone.is_empty() {
            tracing::warn!(
                segments = gone.len(),
                "derived segments named by the manifest cannot be read; dropping them, their \
                 rows seal again"
            );
            self.report.dropped = gone.len() as u64;
            let drop: HashSet<Hash> = gone.iter().map(|s| s.index).collect();
            self.man.live.retain(|s| !drop.contains(&s.index));
            self.man.retire(gone);
        }
        if self.report.dropped > 0 || pruned || self.report.damaged_manifest {
            self.write().await?;
        }
        Ok(())
    }

    async fn write(&mut self) -> Result<()> {
        let bytes = swap_manifest(self.repo, self.lease, self.raw.as_deref(), &self.man).await?;
        self.raw = Some(bytes);
        self.segs = self
            .repo
            .segments()
            .segments()
            .into_iter()
            .map(|l| (l.r.index, l))
            .collect();
        Ok(())
    }

    /// The newest live segment holding `key`'s value of `col`.
    fn holder(&self, key: &Hash, col: u8) -> Option<(Arc<Live>, codec::Loc)> {
        self.man.live.iter().rev().find_map(|s| {
            if !s.has(col) {
                return None;
            }
            let l = self.segs.get(&s.index)?;
            l.idx.locate(key, col).map(|loc| (l.clone(), loc))
        })
    }

    /// Every key with a row of either kind.
    async fn list_rows(&mut self) -> Result<&HashMap<Hash, u8>> {
        if self.rows.is_none() {
            let mut rows: HashMap<Hash, u8> = HashMap::new();
            for (col, kind) in KINDS {
                let mut after = None;
                loop {
                    let page = self
                        .repo
                        .row_meta()
                        .list_derived_keys(kind, after, 10_000)
                        .await?;
                    let Some(last) = page.last().copied() else {
                        break;
                    };
                    after = Some(last);
                    for k in page {
                        *rows.entry(k).or_default() |= bit(col);
                    }
                }
            }
            self.rows = Some(rows);
        }
        Ok(self.rows.as_ref().expect("listed above"))
    }

    async fn seal(&mut self, state: &mut CompactState) -> Result<()> {
        let now = Instant::now();
        let rows = self.list_rows().await?.clone();
        let mut unsealed_rows = 0u64;
        let mut lone_now: HashSet<Hash> = HashSet::new();
        let mut cands: Vec<(Hash, u8)> = Vec::new();
        for (k, have) in &rows {
            let mut open = 0u8;
            for (col, _) in KINDS {
                if have & bit(col) != 0 && self.holder(k, col).is_none() {
                    open |= bit(col);
                }
            }
            if open == 0 {
                continue;
            }
            unsealed_rows += open.count_ones() as u64;
            let both = bit(COL_SKIPLIST) | bit(COL_HISTORY_TREE);
            if open == both {
                cands.push((*k, open));
                continue;
            }
            let col = if open == bit(COL_SKIPLIST) {
                COL_SKIPLIST
            } else {
                COL_HISTORY_TREE
            };
            if self.holder(k, other(col)).is_some() {
                cands.push((*k, open));
                continue;
            }
            // The other row is not derived yet (the warmer is behind):
            // wait for it a while, then seal the one.
            lone_now.insert(*k);
            let since = *state.lone_since.entry(*k).or_insert(now);
            if now.duration_since(since) >= self.opts.lone_wait {
                cands.push((*k, open));
            }
        }
        state.lone_since.retain(|k, _| lone_now.contains(k));
        self.report.unsealed_rows = unsealed_rows;
        let unsealed = rows.len();
        if unsealed > state.last_unsealed || state.last_growth.is_none() {
            state.last_growth = Some(now);
        }
        state.last_unsealed = unsealed;
        let idle = state
            .last_growth
            .is_some_and(|t| now.duration_since(t) >= self.opts.idle);
        let n = cands.len();
        if n == 0 || (n < self.opts.seal_rows && !(n >= self.opts.idle_rows && idle)) {
            return Ok(());
        }
        let ids: Vec<Hash> = cands.iter().map(|(k, _)| *k).collect();
        let gens = generations(self.repo, &ids, self.opts.batch).await?;
        let mut order: Vec<(u64, Hash, u8)> = cands
            .iter()
            .zip(gens)
            .filter_map(|((k, m), g)| g.map(|g| (g, *k, *m)))
            .collect();
        order.sort_unstable();
        for page in order.chunks(self.opts.max_rows.max(1)) {
            let mut seal: Vec<SealRow> = Vec::with_capacity(page.len());
            let mut values: HashMap<Hash, Vec<(u8, Bytes)>> = HashMap::new();
            for (col, kind) in KINDS {
                let keys: Vec<Hash> = page
                    .iter()
                    .filter(|(_, _, m)| m & bit(col) != 0)
                    .map(|(_, k, _)| *k)
                    .collect();
                for chunk in keys.chunks(self.opts.batch.max(1)) {
                    let got = self.repo.row_meta().get_derived_many(kind, chunk).await?;
                    for (k, v) in chunk.iter().zip(got) {
                        if let Some(v) = v {
                            values.entry(*k).or_default().push((col, v));
                        }
                    }
                }
            }
            for (g, k, _) in page {
                if let Some(v) = values.remove(k) {
                    seal.push(SealRow {
                        key: *k,
                        generation: *g,
                        values: v,
                    });
                }
            }
            if seal.is_empty() {
                continue;
            }
            let mut out = Vec::new();
            encode_capped(self.opts.max_bytes, &seal, &mut out)?;
            for (enc, bad) in out {
                for b in &bad {
                    tracing::warn!(key = %b.key, column = b.col, "derived row not sealed: {}", b.why);
                }
                self.report.bad_values += bad.len() as u64;
                if enc.seg.rows == 0 {
                    continue;
                }
                self.store(&enc).await?;
                self.man.live.push(enc.seg);
                self.write().await?;
                state.live_since.insert(enc.seg.index, Instant::now());
                self.report.sealed_segments += 1;
                self.report.sealed_keys += enc.seg.rows as u64;
            }
        }
        Ok(())
    }

    /// Make both blobs durable before anything names them. Written like
    /// any server blob, so a copy an older `gc` condemned is rescued.
    async fn store(&self, enc: &codec::Encoded) -> Result<()> {
        gc_lease::check(self.lease)?;
        self.repo
            .put_rescuing_many_loose(vec![enc.data.clone(), enc.index.clone()], &[])
            .await
    }

    /// Merge runs of [`CompactOptions::fanout`] adjacent live segments of
    /// one tier while their keys and bytes fit the caps. Adjacent, so a key
    /// in several segments still answers from the newest. Keys of
    /// changesets no longer stored (the changeset sweep) are left out.
    async fn merge(&mut self, state: &mut CompactState) -> Result<()> {
        let f = self.opts.fanout.max(2);
        'runs: loop {
            let live = self.man.live.clone();
            // The first run of `f` adjacent loaded segments of one tier;
            // of it, the longest prefix (two or more) within the caps.
            let fits = |run: &[SegmentRef]| -> usize {
                let (mut rows, mut bytes, mut k) = (0usize, 0usize, 0);
                for s in run {
                    rows += s.rows as usize;
                    bytes += s.data_len as usize;
                    if rows > self.opts.max_rows || bytes > self.opts.max_bytes {
                        break;
                    }
                    k += 1;
                }
                k
            };
            let found = (0..live.len().saturating_sub(f - 1)).find_map(|i| {
                let run = &live[i..i + f];
                let t = self.opts.tier(run[0].rows);
                if !run
                    .iter()
                    .all(|s| self.opts.tier(s.rows) == t && self.segs.contains_key(&s.index))
                {
                    return None;
                }
                let k = fits(run);
                let ids: Vec<Hash> = run[..k].iter().map(|s| s.index).collect();
                (k >= 2 && !state.oversize.contains(&ids)).then_some((i, k))
            });
            let Some((at, k)) = found else {
                return Ok(());
            };
            let run: Vec<SegmentRef> = live[at..at + k].to_vec();
            // Oldest first, so a newer segment's value replaces an older's.
            let mut values: HashMap<(Hash, u8), Bytes> = HashMap::new();
            for s in &run {
                let l = self.segs[&s.index].clone();
                let Some(data) = self.repo.segments().data(&l).await? else {
                    // Not stored or damaged: drop it, and look again.
                    self.man.live.retain(|x| x.index != s.index);
                    self.man.retire([*s]);
                    self.report.dropped += 1;
                    self.write().await?;
                    continue 'runs;
                };
                for (k, col, v) in codec::read_all(&l.idx, &data)? {
                    if s.has(col) {
                        values.insert((k, col), v);
                    }
                }
            }
            let mut keys: Vec<Hash> = values.keys().map(|(k, _)| *k).collect();
            keys.sort_unstable();
            keys.dedup();
            let gens = generations(self.repo, &keys, self.opts.batch).await?;
            let mut rows = Vec::with_capacity(keys.len());
            for (k, g) in keys.iter().zip(gens) {
                let Some(g) = g else { continue };
                let mut v: Vec<(u8, Bytes)> = KINDS
                    .iter()
                    .filter_map(|(c, _)| values.remove(&(*k, *c)).map(|b| (*c, b)))
                    .collect();
                v.sort_by_key(|(c, _)| *c);
                rows.push(SealRow {
                    key: *k,
                    generation: g,
                    values: v,
                });
            }
            let (enc, bad) = codec::encode(codec::FAMILY_CHANGESET, &rows, codec::nonce())?;
            self.report.bad_values += bad.len() as u64;
            if enc.seg.data_len as usize > self.opts.max_bytes {
                state.oversize.insert(run.iter().map(|s| s.index).collect());
                continue;
            }
            if enc.seg.rows > 0 {
                self.store(&enc).await?;
            }
            let mut next = self.man.live[..at].to_vec();
            if enc.seg.rows > 0 {
                next.push(enc.seg);
            }
            next.extend_from_slice(&self.man.live[at + k..]);
            self.man.live = next;
            self.man.retire(run.iter().copied());
            self.write().await?;
            state.live_since.insert(enc.seg.index, Instant::now());
            self.report.merges += 1;
            self.report.merged_away += k as u64;
        }
    }

    /// Delete rows a live segment, seen live for at least
    /// [`CompactOptions::retire_after`], holds the same value for. The
    /// feature is recorded first; the manifest is read again under the
    /// lease and must be the one this pass holds; each step checks the
    /// lease, compares every row with its segment value and deletes only
    /// equal ones.
    async fn retire(
        &mut self,
        state: &mut CompactState,
        feature: Option<&FeatureHook>,
    ) -> Result<()> {
        let now = Instant::now();
        let ripe: HashSet<Hash> = state
            .live_since
            .iter()
            .filter(|(_, t)| now.duration_since(**t) >= self.opts.retire_after)
            .map(|(h, _)| *h)
            .collect();
        if ripe.is_empty() {
            return Ok(());
        }
        let (raw, _) = read_manifest(self.repo).await?;
        if raw != self.raw {
            return Err(Error::Conflict(format!(
                "the {MANIFEST_KIND} row changed under this pass; stopping before deleting rows"
            )));
        }
        let rows = self.list_rows().await?.clone();
        for (col, kind) in KINDS {
            let mut keys: Vec<(Hash, Arc<Live>, codec::Loc)> = rows
                .iter()
                .filter(|(_, m)| *m & bit(col) != 0)
                .filter_map(|(k, _)| self.holder(k, col).map(|(l, loc)| (*k, l, loc)))
                .filter(|(_, l, _)| ripe.contains(&l.r.index))
                .collect();
            keys.sort_unstable_by_key(|(k, _, _)| *k);
            for chunk in keys.chunks(self.opts.batch.max(1)) {
                let started = std::time::Instant::now();
                let ks: Vec<Hash> = chunk.iter().map(|(k, _, _)| *k).collect();
                let got = self.repo.row_meta().get_derived_many(kind, &ks).await?;
                let mut del = Vec::with_capacity(ks.len());
                for ((k, l, loc), row) in chunk.iter().zip(got) {
                    let Some(row) = row else { continue };
                    let seg = match self.repo.segments().read_at(l, *loc, col).await? {
                        Found::Yes(v) => v,
                        _ => continue,
                    };
                    let equal = if col == COL_HISTORY_TREE {
                        same_part(&row, &seg)
                    } else {
                        row == seg
                    };
                    if equal {
                        del.push(*k);
                    } else {
                        self.report.mismatched += 1;
                        metrics::counter!("leep_derived_segment_mismatch_total", "kind" => kind)
                            .increment(1);
                        tracing::warn!(
                            key = %k,
                            kind,
                            "a {kind} row differs from its derived segment's value; the row                              stays and wins (run `leep-server verify --derived {kind}`)"
                        );
                    }
                }
                if del.is_empty() {
                    continue;
                }
                if !state.feature_recorded {
                    let Some(f) = feature else {
                        return Err(Error::Invalid(
                            "--derived-segments retire needs the store feature recorded first,                              and this handle cannot record it"
                                .into(),
                        ));
                    };
                    f().await?;
                    state.feature_recorded = true;
                }
                gc_lease::check(self.lease)?;
                let n = self
                    .repo
                    .row_meta()
                    .delete_derived_keys(&[kind], &del)
                    .await?;
                self.report.retired_rows += n;
                metrics::counter!("leep_derived_segment_rows_retired_total", "kind" => kind)
                    .increment(n);
                let rest = started.elapsed() * self.opts.rest_factor;
                if !rest.is_zero() {
                    tokio::time::sleep(rest).await;
                }
            }
        }
        Ok(())
    }
}

/// Encode `rows` as segments whose data blobs fit in `max_bytes`, halving
/// a page until each fits. The depth is log2 of the page: not a history
/// walk.
fn encode_capped(
    max_bytes: usize,
    rows: &[SealRow],
    out: &mut Vec<(codec::Encoded, Vec<codec::BadValue>)>,
) -> Result<()> {
    let (enc, bad) = codec::encode(codec::FAMILY_CHANGESET, rows, codec::nonce())?;
    if enc.seg.data_len as usize > max_bytes && rows.len() > 1 {
        let (a, b) = rows.split_at(rows.len() / 2);
        encode_capped(max_bytes, a, out)?;
        return encode_capped(max_bytes, b, out);
    }
    out.push((enc, bad));
    Ok(())
}

/// The generations of `ids`: stored rows in batches, a missing one derived.
/// `None` for a changeset that is no longer stored.
async fn generations(repo: &Repo, ids: &[Hash], batch: usize) -> Result<Vec<Option<u64>>> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(batch.max(1)) {
        let rows = repo
            .row_meta()
            .get_derived_many(Generation::NAME, chunk)
            .await?;
        for (id, row) in chunk.iter().zip(rows) {
            out.push(match row.and_then(|v| decode_u64(&v).ok()) {
                Some(g) => Some(g),
                None => match repo.generation(ChangesetId(*id)).await {
                    Ok(g) => Some(g),
                    Err(Error::NotFound(_)) => None,
                    Err(e) => return Err(e),
                },
            });
        }
    }
    Ok(out)
}
