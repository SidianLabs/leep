//! The compactor (ADR 0104, section "The compactor"): one pass seals rows
//! not yet in a live segment, merges segments by tiers of four, and in mode
//! `retire` deletes rows a live segment holds the same value for. Passes
//! hold the `derived-segments` lease, so one server at a time runs one;
//! every manifest write is a compare-and-swap from the bytes the pass last
//! read or wrote, and blobs are durable before the manifest names them.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use leep_core::{ChangesetId, Error, Hash, Result};
use tokio::time::Instant;

use super::codec::{
    self, Block, Loc, Manifest, SealRow, SegmentData, SegmentRef, COL_HISTORY_TREE, COL_SKIPLIST,
};
use super::view::{load_indexes, Live, ManifestRead};
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

/// Whether the store has the feature [`super::FEATURE`] recorded: rows may
/// have been deleted, whatever this process's mode.
pub type FeatureProbe = Arc<dyn Fn() -> BoxFuture<'static, Result<bool>> + Send + Sync>;

/// The store feature, as a pass needs it. Without one (memory stores,
/// tests) a store is taken to have no feature recorded, and mode `retire`
/// refuses to delete.
#[derive(Clone)]
pub struct Features {
    pub record: FeatureHook,
    pub recorded: FeatureProbe,
}

/// Called at named points of a pass ("seal-stored", "merge-stored",
/// "retire-deleted"); a crash test's server stops there to be killed.
#[derive(Clone)]
pub struct PauseHook(pub Arc<dyn Fn(&'static str) -> BoxFuture<'static, ()> + Send + Sync>);

impl std::fmt::Debug for PauseHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PauseHook")
    }
}

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
    /// Most pages of `max_rows` one pass seals: a first seal of a large
    /// store then gives the lease up between passes, so `repair` is not
    /// kept waiting for the whole backfill.
    pub max_seal_pages: usize,
    /// Longest a pass spends on a backfill's generations and pages (it
    /// finishes the page it is on): with the listing, a pass stays well
    /// under the two minutes `repair` waits for the lease.
    pub max_seal_time: Duration,
    /// Most rows one pass deletes, and the longest it spends deleting: a
    /// pass then gives the lease up, so `repair` (which waits up to two
    /// minutes for it) is not starved, and the next pass goes on soon.
    pub max_retire_rows: usize,
    pub max_retire_time: Duration,
    /// In mode `build`, the rows are listed again only once their estimated
    /// count grew by `seal_rows` since the last listing, or this long after
    /// it (so a quiet store's idle seal still happens).
    pub list_every: Duration,
    /// For crash tests only.
    pub pause: Option<PauseHook>,
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
            max_seal_pages: 4,
            max_seal_time: Duration::from_secs(60),
            max_retire_rows: 65_536,
            max_retire_time: Duration::from_secs(60),
            list_every: Duration::from_secs(600),
            pause: None,
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
            max_seal_pages: 64,
            max_seal_time: Duration::from_secs(3600),
            max_retire_rows: usize::MAX,
            max_retire_time: Duration::from_secs(3600),
            list_every: Duration::ZERO,
            pause: None,
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
    /// Live segments, since first seen live (or published) by this process.
    live_since: HashMap<Hash, Instant>,
    /// When the number of keys with rows last grew.
    last_growth: Option<Instant>,
    last_unsealed: usize,
    feature_recorded: bool,
    /// Runs whose merge came out over the byte cap: not tried again.
    oversize: HashSet<Vec<Hash>>,
    /// Live segments this process read whole and found sound: later passes
    /// only check their blobs are still stored.
    verified: HashSet<Hash>,
    /// Live segments the last pass found unreadable but kept: on a store
    /// with the feature recorded, a segment is dropped only when two passes
    /// in a row agree.
    suspect: HashSet<Hash>,
    /// Rows found unequal to their holding segment's value, with that
    /// segment: not read and reported again while it holds them.
    mismatched: HashMap<(Hash, u8), Hash>,
    /// Keys with rows whose changeset is not stored (the changeset sweep
    /// was killed between rows and changeset): skipped until the next
    /// listing after `list_every`.
    unstored: HashSet<Hash>,
    /// Rows whose value does not decode, by key and column: not sealed,
    /// and skipped like `unstored` (cleared with it), so they do not make
    /// every listing a backfill again.
    bad: HashSet<(Hash, u8)>,
    /// When `unstored` was last cleared.
    unstored_cleared: Option<Instant>,
    /// The last listing: when, and the estimated row count then.
    listed: Option<(Instant, u64)>,
    /// A seal backfill under way: the candidates of a listing, consumed
    /// page by page by the following passes without listing again.
    backfill: Option<Backfill>,
    /// Rows a retirement that hit its bound has still to compare, by
    /// holding segment: the following passes go on without listing again.
    retire_left: Option<RetireWork>,
    /// The live segments (with their column masks) the carried work above
    /// accounts for: those live at its listing, and those this process
    /// published since. A live segment outside it (another server's pass,
    /// a `repair drop`), or a dropped one, makes a pass forget the work
    /// and list again.
    known: HashMap<Hash, u64>,
}

/// Rows to compare and retire: by holding segment, each list by column and
/// key.
type RetireWork = VecDeque<(Hash, Vec<(Hash, u8)>)>;

/// Seal candidates carried across passes.
struct Backfill {
    /// Candidates whose generation is not read yet.
    pending: Vec<(Hash, u8)>,
    /// Candidates with their generations; sorted by `(generation, id)` once
    /// `pending` is empty, then sealed from `next` on.
    order: Vec<(u64, Hash, u8)>,
    sorted: bool,
    next: usize,
}

impl Backfill {
    fn rows_left(&self) -> u64 {
        let pending = self.pending.iter().map(|(_, m)| m.count_ones() as u64);
        let order = self.order[self.next.min(self.order.len())..]
            .iter()
            .map(|(_, _, m)| m.count_ones() as u64);
        pending.sum::<u64>() + order.sum::<u64>()
    }
}

/// A key with rows, as a pass sees it.
#[derive(Clone, Copy)]
struct RowKey {
    /// Bit `c`: it has a row of column `c`.
    have: u8,
    /// Per column (by [`slot`]), the position in the manifest's live list
    /// of its newest holding segment, or [`NONE`].
    holder: [u32; 2],
}

const NONE: u32 = u32::MAX;

fn slot(col: u8) -> usize {
    usize::from(col != COL_SKIPLIST)
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
    /// Rows found unequal to their segment's value, kept.
    pub mismatched: u64,
    /// Row values that do not decode, never sealed.
    pub bad_values: u64,
    pub live_segments: u64,
    pub live_bytes: u64,
    /// Whether the manifest named in the row could not be decoded.
    pub damaged_manifest: bool,
    /// Whether a newer binary wrote the manifest: the pass did nothing.
    pub newer_manifest: bool,
    /// Whether the rows were listed (in mode `build` only when they grew).
    pub listed: bool,
    /// Whether the pass made progress and stopped with work left (a
    /// backfill's candidates beyond its pages or time, rows beyond its
    /// retire bound): the next pass should run soon.
    pub more: bool,
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
/// it. `features` records and reads [`super::FEATURE`]; mode `retire`
/// refuses to delete without it.
pub async fn compact_pass(
    repo: &Repo,
    opts: &CompactOptions,
    state: &mut CompactState,
    features: Option<&Features>,
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
        let mut p = Pass::load(repo, opts, &lease, features).await?;
        p.run(state).await?;
        Ok(p.report)
    }
    .await;
    gc_lease::release(lease, result).await.map(Some)
}

async fn feature_recorded(features: Option<&Features>) -> Result<bool> {
    match features {
        Some(f) => (f.recorded)().await,
        None => Ok(false),
    }
}

pub(crate) struct Pass<'a> {
    repo: &'a Repo,
    opts: &'a CompactOptions,
    lease: &'a Option<GcLease>,
    features: Option<&'a Features>,
    /// The manifest row as this pass last read or wrote it.
    raw: Option<Bytes>,
    man: Manifest,
    /// Live segments whose index this pass loaded itself, by index hash.
    segs: HashMap<Hash, Arc<Live>>,
    /// Data blobs and blocks this pass read (one segment at a time where it
    /// reads many).
    data: HashMap<Hash, Arc<SegmentData>>,
    blocks: HashMap<(Hash, usize, usize), Arc<Block>>,
    /// Keys with rows, and which of the two kinds they have rows of.
    rows: Option<HashMap<Hash, RowKey>>,
    /// Live segments whose decoded index came from the read view, not from
    /// a read of its blob by this pass.
    reused: HashSet<Hash>,
    started: std::time::Instant,
    /// Live segments whose data this pass could not read: no merge or
    /// retire uses them again in this pass, and the next pass probes them
    /// whole.
    unreadable: HashSet<Hash>,
    report: PassReport,
}

impl<'a> Pass<'a> {
    pub(crate) async fn load(
        repo: &'a Repo,
        opts: &'a CompactOptions,
        lease: &'a Option<GcLease>,
        features: Option<&'a Features>,
    ) -> Result<Pass<'a>> {
        let (raw, read) = read_manifest(repo).await?;
        let mut report = PassReport::default();
        let man = match read {
            ManifestRead::Absent => Manifest::new(codec::FAMILY_CHANGESET),
            ManifestRead::Read(m) => m,
            ManifestRead::Newer => {
                // Never damage: a newer binary's format. Touch nothing.
                tracing::warn!(
                    "the {MANIFEST_KIND} row was written by a newer leep-server; this one \
                     runs no derived segment pass"
                );
                report.newer_manifest = true;
                Manifest::new(codec::FAMILY_CHANGESET)
            }
            ManifestRead::Damaged => {
                report.damaged_manifest = true;
                // Replaced only where every row is certainly still stored:
                // this process is in mode build and the store has never
                // recorded the feature (another server may have retired).
                if opts.mode == Mode::Retire || feature_recorded(features).await? {
                    return Err(Error::Other(format!(
                        "the {MANIFEST_KIND} row does not decode, and rows may have been \
                         retired (the store feature {} is recorded, or this server runs \
                         retire), so it may name the only copy of deleted rows; it is not \
                         replaced automatically (see LeepDerivedSegmentPassesFailing in \
                         RUNBOOKS.md)",
                        super::FEATURE
                    )));
                }
                tracing::warn!("the {MANIFEST_KIND} row does not decode; replacing it");
                Manifest::new(codec::FAMILY_CHANGESET)
            }
        };
        let mut p = Pass {
            repo,
            opts,
            lease,
            features,
            raw,
            man,
            segs: HashMap::new(),
            data: HashMap::new(),
            blocks: HashMap::new(),
            rows: None,
            reused: HashSet::new(),
            started: std::time::Instant::now(),
            unreadable: HashSet::new(),
            report,
        };
        if !p.report.newer_manifest {
            p.load_segments().await?;
        }
        Ok(p)
    }

    /// Load the live segments' indexes itself, not through the shared read
    /// view (whose state a reader's transient error or a late read may
    /// have moved). A transient read error fails the pass.
    async fn load_segments(&mut self) -> Result<()> {
        // Decoded indexes the view already holds are reused: an index is
        // immutable and checked by address when loaded, so only the view's
        // list of segments and its broken set are its own state.
        let held: HashMap<Hash, Arc<codec::SegmentIndex>> = self
            .repo
            .segments()
            .segments()
            .into_iter()
            .map(|l| (l.r.index, l.idx.clone()))
            .collect();
        for s in &self.man.live {
            if self.segs.contains_key(&s.index) {
                continue;
            }
            if let Some(idx) = held.get(&s.index) {
                if idx.data == s.data && idx.len() == s.rows as usize {
                    self.reused.insert(s.index);
                    self.segs.insert(
                        s.index,
                        Arc::new(Live {
                            r: *s,
                            idx: idx.clone(),
                        }),
                    );
                }
            }
        }
        let missing: Vec<SegmentRef> = self
            .man
            .live
            .iter()
            .filter(|s| !self.segs.contains_key(&s.index))
            .copied()
            .collect();
        for (h, idx) in load_indexes(&**self.repo.blobs(), &missing).await? {
            if let (Some(idx), Some(r)) = (idx, self.man.live.iter().find(|s| s.index == h)) {
                self.segs.insert(h, Arc::new(Live { r: *r, idx }));
            }
        }
        // Column masks come from the manifest.
        for s in &self.man.live {
            if let Some(l) = self.segs.get_mut(&s.index) {
                if l.r != *s {
                    *l = Arc::new(Live {
                        r: *s,
                        idx: l.idx.clone(),
                    });
                }
            }
        }
        let live: HashSet<Hash> = self.man.live.iter().map(|s| s.index).collect();
        self.segs.retain(|h, _| live.contains(h));
        self.reused.retain(|h| live.contains(h));
        Ok(())
    }

    pub(crate) async fn run(&mut self, state: &mut CompactState) -> Result<()> {
        if self.report.newer_manifest {
            return Ok(());
        }
        self.check_live(state).await?;
        let now = Instant::now();
        let live: HashSet<Hash> = self.man.live.iter().map(|s| s.index).collect();
        state.live_since.retain(|h, _| live.contains(h));
        for h in &live {
            state.live_since.entry(*h).or_insert(now);
        }
        state.verified.retain(|h| live.contains(h));
        state.mismatched.retain(|_, h| live.contains(h));
        let accounted = self
            .man
            .live
            .iter()
            .all(|s| state.known.get(&s.index) == Some(&s.columns));
        if self.report.dropped > 0 || !accounted {
            if state.backfill.is_some() || state.retire_left.is_some() {
                // List again now, not after `list_every`.
                state.listed = None;
            }
            state.backfill = None;
            state.retire_left = None;
        }
        self.seal(state).await?;
        self.merge(state).await?;
        // Not during a backfill: each pass would list every row again to
        // find the few newly ripe ones.
        if self.opts.mode == Mode::Retire && state.backfill.is_none() {
            self.retire(state).await?;
        }
        self.report.live_segments = self.man.live.len() as u64;
        self.report.live_bytes = self.man.live.iter().map(|s| s.data_len as u64).sum::<u64>();
        Ok(())
    }

    /// Whether `s`'s blobs read, probed by this pass itself: `Ok(false)`
    /// only for definite loss or damage (not stored, address or structure
    /// wrong, a read the store reports corrupt); any other error fails the
    /// pass. A segment read whole once in this process is later only checked
    /// for presence, as is an index taken from the read view.
    async fn probe(&mut self, s: &SegmentRef, state: &mut CompactState) -> Result<bool> {
        let Some(live) = self.segs.get(&s.index).cloned() else {
            // `load_indexes` gives `None` only for definite loss or damage.
            return Ok(false);
        };
        if self.reused.contains(&s.index) && !self.repo.blobs().contains(&s.index).await? {
            return Ok(false);
        }
        if state.verified.contains(&s.index) {
            return self.repo.blobs().contains(&s.data).await;
        }
        let bytes = match self.repo.blobs().get(&s.data).await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(false),
            Err(e) if crate::gc_segments::segment_reads_corrupt(&e) => return Ok(false),
            Err(e) => return Err(e),
        };
        let sound = codec::decode_data(&live.idx, &s.data, bytes)
            .and_then(|d| codec::read_all(&live.idx, &d).map(|_| d));
        match sound {
            Ok(_) => {
                state.verified.insert(s.index);
                Ok(true)
            }
            Err(_) => Ok(false),
        }
    }

    /// Drop live segments that cannot be read (their keys then read as rows
    /// or derive again, and seal again), forget retired blobs no longer
    /// stored, and replace a manifest that did not decode. On a store with
    /// the feature recorded a segment is dropped only when this pass and the
    /// last both found it unreadable.
    async fn check_live(&mut self, state: &mut CompactState) -> Result<()> {
        let mut bad = Vec::new();
        for s in self.man.live.clone() {
            if !self.probe(&s, state).await? {
                bad.push(s);
            }
        }
        let careful =
            !bad.is_empty() && (state.feature_recorded || feature_recorded(self.features).await?);
        let mut gone = Vec::new();
        let mut suspect = HashSet::new();
        for s in bad {
            if careful && !state.suspect.contains(&s.index) {
                tracing::warn!(
                    segment = %s.index,
                    "a derived segment cannot be read; dropping it if the next pass agrees"
                );
                suspect.insert(s.index);
            } else {
                gone.push(s);
            }
        }
        state.suspect = suspect;
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
        let bytes =
            swap_manifest(self.repo, self.lease, self.raw.as_deref(), &mut self.man).await?;
        self.raw = Some(bytes);
        self.load_segments().await
    }

    /// The newest live segment holding `key`'s value of `col`, as its
    /// position in the live list.
    fn holder(&self, key: &Hash, col: u8) -> u32 {
        self.man
            .live
            .iter()
            .enumerate()
            .rev()
            .find_map(|(i, s)| {
                let l = self.segs.get(&s.index)?;
                (s.has(col) && l.idx.has(key, col)).then_some(i as u32)
            })
            .unwrap_or(NONE)
    }

    /// Set every key's newest holder of each column it has a row of. Few
    /// rows look each key up in every segment; many walk every segment's
    /// keys once, newest first, so a backfill is linear in the keys, not
    /// keys times segments.
    fn place_holders(&self, rows: &mut HashMap<Hash, RowKey>) {
        let sealed: usize = self.man.live.iter().map(|s| s.rows as usize).sum();
        if rows.len().saturating_mul(self.man.live.len()) <= sealed {
            for (k, r) in rows.iter_mut() {
                for (col, _) in KINDS {
                    r.holder[slot(col)] = if r.have & bit(col) != 0 {
                        self.holder(k, col)
                    } else {
                        NONE
                    };
                }
            }
            return;
        }
        for r in rows.values_mut() {
            r.holder = [NONE; 2];
        }
        for (i, s) in self.man.live.iter().enumerate().rev() {
            let Some(l) = self.segs.get(&s.index) else {
                continue;
            };
            for (col, _) in KINDS {
                if !s.has(col) {
                    continue;
                }
                for k in l.idx.keys_with(col) {
                    if let Some(r) = rows.get_mut(k) {
                        if r.have & bit(col) != 0 && r.holder[slot(col)] == NONE {
                            r.holder[slot(col)] = i as u32;
                        }
                    }
                }
            }
        }
    }

    /// A live segment whose data this pass could not read: skipped for the
    /// rest of the pass, and read whole by the next pass's probe (which
    /// drops it, or on a store with the feature counts it toward the two
    /// passes that must agree).
    fn mark_unreadable(&mut self, index: Hash, state: &mut CompactState) {
        if self.unreadable.insert(index) {
            tracing::warn!(segment = %index, "a derived segment's data cannot be read now");
        }
        state.verified.remove(&index);
    }

    /// The data blob of `live`, read once per pass. `None`: not readable.
    async fn data_of(&mut self, live: &Live) -> Result<Option<Arc<SegmentData>>> {
        if let Some(d) = self.data.get(&live.r.data) {
            return Ok(Some(d.clone()));
        }
        let bytes = match self.repo.blobs().get(&live.r.data).await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(e) if crate::gc_segments::segment_reads_corrupt(&e) => return Ok(None),
            Err(e) => return Err(e),
        };
        match codec::decode_data(&live.idx, &live.r.data, bytes) {
            Ok(d) => {
                let d = Arc::new(d);
                self.data.insert(live.r.data, d.clone());
                Ok(Some(d))
            }
            Err(_) => Ok(None),
        }
    }

    /// Forget the data and blocks read so far (between segments).
    fn forget_data(&mut self) {
        self.data.clear();
        self.blocks.clear();
    }

    /// The value at `loc` of `live`; `None` if it cannot be read.
    async fn value_at(&mut self, live: &Live, loc: Loc, col: u8) -> Result<Option<Bytes>> {
        let Some(data) = self.data_of(live).await? else {
            return Ok(None);
        };
        let key = (live.r.data, loc.col, loc.block);
        let block = match self.blocks.get(&key) {
            Some(b) => b.clone(),
            None => match codec::read_block(&live.idx, &data, loc.col, loc.block) {
                Ok(b) => {
                    let b = Arc::new(b);
                    self.blocks.insert(key, b.clone());
                    b
                }
                Err(_) => return Ok(None),
            },
        };
        Ok(block
            .row(loc.rank)
            .and_then(|row| codec::row_value(&live.idx, &data, col, loc.ord, row).ok()))
    }

    /// The keys with rows of either kind: listed again in mode `retire`
    /// unless a retirement is carried over (only unsealed rows remain), in
    /// mode `build` once their estimated count grew by `seal_rows` or
    /// `list_every` passed.
    async fn list_rows(&mut self, state: &mut CompactState) -> Result<()> {
        if self.rows.is_some() {
            return Ok(());
        }
        let mut estimate = 0;
        for (_, kind) in KINDS {
            estimate += self.repo.row_meta().estimate_derived(kind).await?.rows;
        }
        let due = match state.listed {
            None => true,
            Some((at, n)) => {
                (self.opts.mode == Mode::Retire && state.retire_left.is_none())
                    || estimate >= n + self.opts.seal_rows as u64
                    || at.elapsed() >= self.opts.list_every
            }
        };
        if !due {
            self.rows = Some(HashMap::new());
            return Ok(());
        }
        if state
            .unstored_cleared
            .is_none_or(|at| at.elapsed() >= self.opts.list_every)
        {
            state.unstored.clear();
            state.bad.clear();
            state.unstored_cleared = Some(Instant::now());
        }
        state.listed = Some((Instant::now(), estimate));
        self.report.listed = true;
        let mut rows: HashMap<Hash, RowKey> = HashMap::new();
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
                    if !state.unstored.contains(&k) && !state.bad.contains(&(k, col)) {
                        rows.entry(k)
                            .or_insert(RowKey {
                                have: 0,
                                holder: [NONE; 2],
                            })
                            .have |= bit(col);
                    }
                }
            }
        }
        // Work still carried accounts for every live segment (`run` checked),
        // so it stays valid; from here on so does what this listing finds.
        state.known = self.man.live.iter().map(|s| (s.index, s.columns)).collect();
        self.rows = Some(rows);
        Ok(())
    }

    /// The keys to seal from a fresh listing, once there are enough of
    /// them: rows no live segment holds, a lone one once it waited.
    fn seal_candidates(&mut self, state: &mut CompactState) -> Vec<(Hash, u8)> {
        let now = Instant::now();
        let mut rows = self.rows.take().unwrap_or_default();
        self.place_holders(&mut rows);
        let mut unsealed_rows = 0u64;
        let mut lone_now: HashSet<Hash> = HashSet::new();
        let mut cands: Vec<(Hash, u8)> = Vec::new();
        for (k, r) in &rows {
            let mut open = 0u8;
            for (col, _) in KINDS {
                if r.have & bit(col) != 0 && r.holder[slot(col)] == NONE {
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
            let other_held = if r.have & bit(other(col)) != 0 {
                r.holder[slot(other(col))] != NONE
            } else {
                self.holder(k, other(col)) != NONE
            };
            if other_held {
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
        let keyed = rows.len();
        self.rows = Some(rows);
        state.lone_since.retain(|k, _| lone_now.contains(k));
        self.report.unsealed_rows = unsealed_rows;
        if keyed > state.last_unsealed || state.last_growth.is_none() {
            state.last_growth = Some(now);
        }
        state.last_unsealed = keyed;
        let idle = state
            .last_growth
            .is_some_and(|t| now.duration_since(t) >= self.opts.idle);
        let n = cands.len();
        if n == 0 || (n < self.opts.seal_rows && !(n >= self.opts.idle_rows && idle)) {
            return Vec::new();
        }
        cands
    }

    /// Seal what a listing found unsealed. A listing's candidates become a
    /// backfill the following passes go on with, without listing again:
    /// first their generations (in batches, within `max_seal_time`), then,
    /// sorted by `(generation, id)`, at most `max_seal_pages` pages a pass.
    async fn seal(&mut self, state: &mut CompactState) -> Result<()> {
        if state.backfill.is_none() {
            self.list_rows(state).await?;
            if !self.report.listed {
                return Ok(());
            }
            let pending = self.seal_candidates(state);
            if pending.is_empty() {
                return Ok(());
            }
            state.backfill = Some(Backfill {
                pending,
                order: Vec::new(),
                sorted: false,
                next: 0,
            });
        }
        let Some(mut bf) = state.backfill.take() else {
            return Ok(());
        };
        let deadline = self.started + self.opts.max_seal_time;
        let mut progress = false;
        let chunk = self.opts.batch.max(1) * 16;
        while !bf.pending.is_empty() {
            if progress && std::time::Instant::now() >= deadline {
                break;
            }
            let part = bf.pending.split_off(bf.pending.len().saturating_sub(chunk));
            let ids: Vec<Hash> = part.iter().map(|(k, _)| *k).collect();
            let gens = generations(self.repo, &ids, self.opts.batch).await?;
            for ((k, m), g) in part.into_iter().zip(gens) {
                match g {
                    Some(g) => bf.order.push((g, k, m)),
                    None => {
                        state.unstored.insert(k);
                    }
                }
            }
            progress = true;
        }
        if bf.pending.is_empty() && !bf.sorted {
            bf.order.sort_unstable();
            bf.sorted = true;
        }
        let page_rows = self.opts.max_rows.max(1);
        let mut pages = 0;
        while bf.sorted && bf.next < bf.order.len() && pages < self.opts.max_seal_pages.max(1) {
            if progress && std::time::Instant::now() >= deadline {
                break;
            }
            let end = (bf.next + page_rows).min(bf.order.len());
            let page = bf.order[bf.next..end].to_vec();
            self.seal_page(&page, state).await?;
            bf.next = end;
            pages += 1;
            progress = true;
        }
        if !self.report.listed {
            self.report.unsealed_rows = bf.rows_left();
        }
        if !bf.sorted || bf.next < bf.order.len() {
            // Every pass that runs this does some of it (a batch of
            // generations or a page), so a backfill always ends.
            self.report.more |= progress;
            state.backfill = Some(bf);
        }
        Ok(())
    }

    /// Read the rows of one page and seal them, in segments that fit
    /// `max_bytes`. A row gone meanwhile is left out; one whose value does
    /// not decode is remembered and not listed again for a while.
    async fn seal_page(
        &mut self,
        page: &[(u64, Hash, u8)],
        state: &mut CompactState,
    ) -> Result<()> {
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
            return Ok(());
        }
        let mut out = Vec::new();
        encode_capped(self.opts.max_bytes, &seal, &mut out)?;
        for (enc, bad) in out {
            for b in &bad {
                tracing::warn!(key = %b.key, column = b.col, "derived row not sealed: {}", b.why);
                state.bad.insert((b.key, b.col));
            }
            self.report.bad_values += bad.len() as u64;
            if enc.seg.rows == 0 {
                continue;
            }
            self.store(&enc).await?;
            self.pause("seal-stored").await;
            self.man.live.push(enc.seg);
            self.write().await?;
            state.known.insert(enc.seg.index, enc.seg.columns);
            state.live_since.insert(enc.seg.index, Instant::now());
            state.verified.insert(enc.seg.index);
            self.report.sealed_segments += 1;
            self.report.sealed_keys += enc.seg.rows as u64;
        }
        Ok(())
    }

    async fn pause(&self, at: &'static str) {
        if let Some(p) = &self.opts.pause {
            (p.0)(at).await;
        }
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
        // Each round merges (the live list shrinks) or skips a segment (the
        // skip set grows), so it ends; the bound is defense in depth.
        for _ in 0..4 * self.man.live.len() + 16 {
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
                if !run.iter().all(|s| {
                    self.opts.tier(s.rows) == t
                        && self.segs.contains_key(&s.index)
                        && !self.unreadable.contains(&s.index)
                }) {
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
            let mut unreadable = None;
            // Oldest first, so a newer segment's value replaces an older's.
            let mut values: HashMap<(Hash, u8), Bytes> = HashMap::new();
            for s in &run {
                let l = self.segs[&s.index].clone();
                let Some(data) = self.data_of(&l).await? else {
                    unreadable = Some(s.index);
                    break;
                };
                for (key, col, v) in codec::read_all(&l.idx, &data)? {
                    if s.has(col) {
                        values.insert((key, col), v);
                    }
                }
            }
            self.forget_data();
            if let Some(index) = unreadable {
                // Unreadable now: no run uses it again in this pass, and
                // the next pass reads it whole and decides about it.
                self.mark_unreadable(index, state);
                continue;
            }
            let mut keys: Vec<Hash> = values.keys().map(|(k, _)| *k).collect();
            keys.sort_unstable();
            keys.dedup();
            let gens = generations(self.repo, &keys, self.opts.batch).await?;
            let mut rows = Vec::with_capacity(keys.len());
            for (key, g) in keys.iter().zip(gens) {
                let Some(g) = g else { continue };
                let mut v: Vec<(u8, Bytes)> = KINDS
                    .iter()
                    .filter_map(|(c, _)| values.remove(&(*key, *c)).map(|b| (*c, b)))
                    .collect();
                v.sort_by_key(|(c, _)| *c);
                rows.push(SealRow {
                    key: *key,
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
            self.pause("merge-stored").await;
            let mut next = self.man.live[..at].to_vec();
            if enc.seg.rows > 0 {
                next.push(enc.seg);
            }
            next.extend_from_slice(&self.man.live[at + k..]);
            self.man.live = next;
            self.man.retire(run.iter().copied());
            self.write().await?;
            if enc.seg.rows > 0 {
                state.known.insert(enc.seg.index, enc.seg.columns);
            }
            state.live_since.insert(enc.seg.index, Instant::now());
            state.verified.insert(enc.seg.index);
            self.report.merges += 1;
            self.report.merged_away += k as u64;
        }
        tracing::warn!("derived segment merges stopped at their round bound");
        Ok(())
    }

    /// Delete rows a live segment, seen live for at least
    /// [`CompactOptions::retire_after`], holds the same value for. The
    /// feature is recorded first; the manifest is read again under the
    /// lease and must be the one this pass holds; each step checks the
    /// lease, compares every row with its segment value, and deletes a row
    /// only if it still holds the bytes compared (one statement), so a row
    /// `repair` rewrote meanwhile stays.
    async fn retire(&mut self, state: &mut CompactState) -> Result<()> {
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
        let mut work = match state.retire_left.take() {
            Some(w) => w,
            None => self.retire_work(&ripe, state).await?,
        };
        let retire_started = std::time::Instant::now();
        let mut progress = false;
        while let Some((seg, mut cands)) = work.pop_front() {
            // Carried work names segments by index: one no longer live,
            // ripe or readable is left for the next listing.
            let Some(l) = self.segs.get(&seg).cloned() else {
                continue;
            };
            if !ripe.contains(&seg) || self.unreadable.contains(&seg) {
                continue;
            }
            let mut i = 0;
            while i < cands.len() {
                if self.report.retired_rows as usize >= self.opts.max_retire_rows
                    || retire_started.elapsed() >= self.opts.max_retire_time
                {
                    cands.drain(..i);
                    work.push_front((seg, cands));
                    state.retire_left = Some(work);
                    self.report.more |= progress;
                    return Ok(());
                }
                if self.unreadable.contains(&seg) {
                    break;
                }
                let col = cands[i].1;
                let mut j = i;
                while j < cands.len() && j - i < self.opts.batch.max(1) && cands[j].1 == col {
                    j += 1;
                }
                let chunk: Vec<(Hash, Loc)> = cands[i..j]
                    .iter()
                    .filter(|(k, _)| l.r.has(col) && state.mismatched.get(&(*k, col)) != Some(&seg))
                    .filter_map(|(k, _)| l.idx.locate(k, col).map(|loc| (*k, loc)))
                    .collect();
                i = j;
                let kind = if col == COL_SKIPLIST {
                    KINDS[0].1
                } else {
                    KINDS[1].1
                };
                let n = self.retire_chunk(&l, col, kind, &chunk, state).await?;
                progress |= n > 0;
            }
            self.forget_data();
        }
        Ok(())
    }

    /// The rows to retire, from a listing: each row whose newest holding
    /// segment is ripe, by that segment (in index order, so one data blob
    /// is in memory at a time), sorted by column and key.
    async fn retire_work(
        &mut self,
        ripe: &HashSet<Hash>,
        state: &mut CompactState,
    ) -> Result<RetireWork> {
        self.list_rows(state).await?;
        let mut rows = self.rows.take().unwrap_or_default();
        self.place_holders(&mut rows);
        let mut by_seg: HashMap<Hash, Vec<(Hash, u8)>> = HashMap::new();
        for (k, r) in &rows {
            for (col, _) in KINDS {
                let at = r.holder[slot(col)];
                if r.have & bit(col) == 0 || at == NONE {
                    continue;
                }
                let seg = self.man.live[at as usize].index;
                if !ripe.contains(&seg)
                    || self.unreadable.contains(&seg)
                    || state.mismatched.get(&(*k, col)) == Some(&seg)
                {
                    continue;
                }
                by_seg.entry(seg).or_default().push((*k, col));
            }
        }
        self.rows = Some(rows);
        let mut work: Vec<(Hash, Vec<(Hash, u8)>)> = by_seg.into_iter().collect();
        work.sort_unstable_by_key(|(seg, _)| *seg);
        for (_, cands) in &mut work {
            cands.sort_unstable_by_key(|(k, c)| (*c, *k));
        }
        Ok(work.into())
    }

    /// One step of a retirement: compare the rows of `chunk` with the
    /// values `live` holds and delete the equal ones still holding the
    /// bytes compared. A value that cannot be read marks the segment
    /// unreadable and ends the step (the rows compared before it are still
    /// deleted). Returns the rows deleted or found unequal.
    async fn retire_chunk(
        &mut self,
        live: &Arc<Live>,
        col: u8,
        kind: &'static str,
        chunk: &[(Hash, Loc)],
        state: &mut CompactState,
    ) -> Result<u64> {
        if chunk.is_empty() {
            return Ok(0);
        }
        let started = std::time::Instant::now();
        let ks: Vec<Hash> = chunk.iter().map(|(k, _)| *k).collect();
        let got = self.repo.row_meta().get_derived_many(kind, &ks).await?;
        let mut del = Vec::with_capacity(ks.len());
        let mut unequal = 0;
        for ((k, loc), row) in chunk.iter().zip(got) {
            let Some(row) = row else { continue };
            let Some(value) = self.value_at(live, *loc, col).await? else {
                self.mark_unreadable(live.r.index, state);
                break;
            };
            let equal = if col == COL_HISTORY_TREE {
                same_part(&row, &value)
            } else {
                row == value
            };
            if equal {
                del.push((*k, row));
            } else {
                unequal += 1;
                self.report.mismatched += 1;
                state.mismatched.insert((*k, col), live.r.index);
                metrics::counter!("leep_derived_segment_mismatch_total", "kind" => kind)
                    .increment(1);
                tracing::warn!(
                    key = %k,
                    kind,
                    "a {kind} row differs from its derived segment's value; the \
                     row stays and wins (run `leep-server verify --derived {kind}`)"
                );
            }
        }
        if del.is_empty() {
            return Ok(unequal);
        }
        if !state.feature_recorded {
            let Some(f) = self.features else {
                return Err(Error::Invalid(
                    "--derived-segments retire needs the store feature recorded \
                     first, and this handle cannot record it"
                        .into(),
                ));
            };
            (f.record)().await?;
            state.feature_recorded = true;
        }
        gc_lease::check(self.lease)?;
        let n = self
            .repo
            .row_meta()
            .delete_derived_values(kind, &del)
            .await?;
        self.report.retired_rows += n;
        self.pause("retire-deleted").await;
        metrics::counter!("leep_derived_segment_rows_retired_total", "kind" => kind).increment(n);
        let rest = started.elapsed() * self.opts.rest_factor;
        if !rest.is_zero() {
            tokio::time::sleep(rest).await;
        }
        Ok(n + unequal)
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
