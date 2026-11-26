//! The compactor (ADR 0104, section "The compactor"; ADR 0105): one pass,
//! for each family in turn, seals rows not yet in a live segment, merges
//! segments by tiers of four, and in mode `retire` deletes rows a live
//! segment holds the same value for. Passes hold the `derived-segments`
//! lease, so one server at a time runs one; every manifest write is a
//! compare-and-swap from the bytes the pass last read or wrote, and blobs
//! are durable before the manifest names them.
//!
//! Families are driven by their table ([`codec::FAMILIES`]): the kinds they
//! hold, and the columns a key is expected to have before it is sealed
//! without waiting. What a process carries between passes (a backfill, a
//! retirement, the segments it accounts for) is kept per family.
//!
//! Family `content-v1` (ADR 0106) differs by its table entry and a few
//! policies: keys are ordered by a walk of the heads' trees, only rows
//! search retention keeps are sealed (keyframes always), a merge leaves out
//! what retention no longer keeps and a third-dead segment is rewritten,
//! segments are capped at 1 MiB and merged in two tiers by bytes, and rows
//! are compared by the set or filter they stand for.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::future::BoxFuture;
use leep_core::{ChangesetId, Error, Hash, Result};
use tokio::time::Instant;

use super::codec::{
    self, Block, Equality, Family, Keys, Loc, Manifest, SealRow, SegmentData, SegmentRef, FAMILIES,
};
use super::view::{get_segment_blob, load_indexes, segment_blob_stored, Live, ManifestRead};
use super::{lease_key, read_manifest, swap_manifest, MANIFEST_KIND};
use crate::derived::{decode_u64, DerivedKind, Generation};
use crate::gc_lease::{self, GcLease};
use crate::search::retention::ContentLive;
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

/// Records a store feature, a family's [`Family::feature`] (the server does
/// it by URL, since `leep-derive` names no concrete store). Called before
/// the first row delete of the family in every process; idempotent.
pub type FeatureHook = Arc<dyn Fn(&'static str) -> BoxFuture<'static, Result<()>> + Send + Sync>;

/// Whether the store has a family's feature recorded: rows of the family
/// may have been deleted, whatever this process's mode.
pub type FeatureProbe = Arc<dyn Fn(&'static str) -> BoxFuture<'static, Result<bool>> + Send + Sync>;

/// The store features, as a pass needs them. Without them (memory stores,
/// tests) a store is taken to have no feature recorded, and mode `retire`
/// refuses to delete.
#[derive(Clone)]
pub struct Features {
    pub record: FeatureHook,
    pub recorded: FeatureProbe,
}

/// Called at named points of a pass ("seal-stored", "seal-swapped",
/// "merge-stored", "retire-deleted"; for `changeset-v2` the same names
/// after `changeset-v2:`); a crash test's server stops there to be killed.
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
    /// A key without a row of every column its family expects waits this
    /// long for the others before it is sealed with what it has.
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
    /// Longest a pass spends on backfills' generations and pages (it
    /// finishes the page it is on), counted from the start of the pass over
    /// every family: with the listings, a pass stays well under the two
    /// minutes `repair` waits for the lease.
    pub max_seal_time: Duration,
    /// Most rows one pass deletes, and the longest it spends deleting, over
    /// every family: a pass then gives the lease up, so `repair` (which
    /// waits up to two minutes for it) is not starved, and the next pass
    /// goes on soon.
    pub max_retire_rows: usize,
    pub max_retire_time: Duration,
    /// In mode `build`, a family's rows are listed again only once their
    /// estimated count grew by `seal_rows` times its columns since the last
    /// listing, or this long after it (so a quiet store's idle seal still
    /// happens).
    pub list_every: Duration,
    /// In mode `retire`, the families (by name) this server may pass the
    /// one-way door of: record their store feature and delete their rows.
    /// A family not named deletes rows only once its feature is already
    /// recorded in the store (another server, or an earlier run, passed
    /// it), so a fleet in `retire` does not pass a new family's door by
    /// upgrading (ADR 0105). The default names `changeset-v1` alone, as
    /// ADR 0104's `retire` did.
    pub retire_families: Vec<String>,
    /// Family `content-v1` (ADR 0106): how long search row retention keeps
    /// a row after the last head that held its blob or tree (its
    /// `--search-retention`); `None` when retention is off, so every row is
    /// live. Only live rows are sealed, and a merge leaves the others out.
    pub content_keep: Option<Duration>,
    /// The clock skew retention allows in the ref log's times.
    pub content_skew: Duration,
    /// Most bytes of a `content-v1` data blob: a cold search reads a whole
    /// data blob for one row, so its segments are smaller than the others'.
    pub content_max_bytes: usize,
    /// Most keys in a `content-v1` segment (16,384 by default, as the
    /// others; more in the small limits, where its bytes cap binds).
    pub content_max_rows: usize,
    /// For tests only: the time (Unix seconds) retention's window is judged
    /// at, instead of the clock.
    pub content_now: Option<i64>,
    /// A live segment whose blobs fail to read (not "not stored", not
    /// damage: an I/O error, a failing object store key) while another of
    /// the family's segments reads whole is dropped once it failed
    /// [`FAILING_PASSES`] passes in a row over at least this long: its keys
    /// then seal again from rows or derive again (after `retire`, derive
    /// again: its rows exist nowhere else), instead of the family keeping a
    /// segment no pass can read. `None`: never dropped for that
    /// (`--derived-segments-drop-failing-after`, default a day: a partial
    /// outage of the store must not throw good segments away).
    pub drop_failing_after: Option<Duration>,
    /// A failing segment's streak of passes restarts when this process's
    /// last pass of the family ended longer ago than this (about two pass
    /// intervals): a lease handoff, or a quiet spell, then one blip does not
    /// count as a streak.
    pub failing_gap: Duration,
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
            retire_families: vec![codec::FAMILY_CHANGESET_NAME.to_string()],
            content_keep: Some(crate::search::retention::DEFAULT_KEEP),
            content_skew: Duration::from_secs(300),
            content_max_bytes: 1 << 20,
            content_max_rows: 16_384,
            content_now: None,
            drop_failing_after: Some(Duration::from_secs(86_400)),
            failing_gap: Duration::from_secs(600),
            pause: None,
        }
    }
}

impl CompactOptions {
    /// Small limits, so small histories seal, merge and retire, every
    /// family.
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
            retire_families: FAMILIES.iter().map(|f| f.name.to_string()).collect(),
            content_keep: Some(crate::search::retention::DEFAULT_KEEP),
            content_skew: Duration::from_secs(300),
            content_max_bytes: 128 << 10,
            content_max_rows: 1_024,
            content_now: None,
            drop_failing_after: Some(Duration::ZERO),
            failing_gap: Duration::from_secs(3600),
            pause: None,
        }
    }

    /// Parse `--derived-segments-retire-families`: family names, comma
    /// separated (`changeset-v1` is always included), or `all`.
    pub fn parse_retire_families(s: &str) -> Result<Vec<String>> {
        let mut out = vec![codec::FAMILY_CHANGESET_NAME.to_string()];
        for name in s.split(',').map(str::trim).filter(|n| !n.is_empty()) {
            if name == "all" {
                return Ok(FAMILIES.iter().map(|f| f.name.to_string()).collect());
            }
            if codec::family_by_name(name).is_none() {
                return Err(Error::Invalid(format!(
                    "unknown derived segment family {name:?}; known: {}",
                    FAMILIES
                        .iter()
                        .map(|f| f.name)
                        .collect::<Vec<_>>()
                        .join(", ")
                )));
            }
            if !out.iter().any(|n| n == name) {
                out.push(name.to_string());
            }
        }
        Ok(out)
    }

    /// The tier of a segment of `rows` keys: `floor(log_fanout(rows /
    /// base))`, `base` the smallest seal (an idle one), so a merge of a
    /// tier's segments lands about one tier up. Counted from the full seal
    /// instead, idle seals and their merges all sat in tier 0 and a
    /// growing segment there was rewritten with every three new seals,
    /// about 20 times before it left the tier.
    fn tier(&self, rows: u32) -> u32 {
        let f = self.fanout.max(2) as u64;
        let base = self.idle_rows.min(self.seal_rows).max(1) as u64;
        let mut t = 0;
        let mut cap = base.saturating_mul(f);
        while (rows as u64) >= cap && t < 16 {
            t += 1;
            cap = cap.saturating_mul(f);
        }
        t
    }
}

/// About the most row bytes a `content-v1` seal or retirement reads at once
/// (a dense directory filter is 64 KiB, and a key has up to four columns):
/// the keys read at once adapt to it.
pub(crate) const READ_BYTES: usize = 4 << 20;

/// Keys a `content-v1` seal or retirement reads at first, before it knows
/// how large the family's rows are.
const CONTENT_FIRST_STEP: usize = 16;

/// Most keys a family's state remembers to skip (`unstored`, `bad`)
/// between listings: past it they are forgotten at the next listing, which
/// only costs looking at them again.
pub(crate) const SKIPPED_CAP: usize = 65_536;

/// Passes in a row a live segment must fail to read in before it is dropped
/// (and [`CompactOptions::drop_failing_after`] must have passed).
pub const FAILING_PASSES: u32 = 3;

/// A failing segment's streak.
#[derive(Clone, Copy, Debug)]
struct Failing {
    /// The first failure of the streak.
    since: Instant,
    /// Passes of the streak that failed to read it.
    passes: u32,
}

/// What a probe of a live segment's blobs found.
enum Probe {
    Sound,
    /// Not stored, or damaged: definite.
    Lost,
    /// A read failed with any other error, for now.
    Failing(Error),
}

/// What a process remembers between passes, per family: carried work
/// (a backfill, a retirement) and the live segments it accounts for are
/// one family's, so another family's segments never invalidate it.
#[derive(Default)]
pub struct CompactState {
    families: HashMap<u8, FamilyState>,
    /// Time and rows the current pass spent, shared by its families.
    budget: PassBudget,
    /// Which family goes first: rotated each pass, so a family's bounded
    /// work never always waits behind another's.
    rotate: usize,
}

/// What one pass may spend over every family.
#[derive(Default, Clone, Copy)]
struct PassBudget {
    started: Option<std::time::Instant>,
    retire_started: Option<std::time::Instant>,
    retired_rows: u64,
}

#[derive(Default)]
struct FamilyState {
    /// Keys missing a row of a column the family expects, since first seen
    /// so.
    lone_since: HashMap<Hash, Instant>,
    /// Live segments, since first seen live (or published) by this process.
    live_since: HashMap<Hash, Instant>,
    /// When the number of keys with rows last grew.
    last_growth: Option<Instant>,
    last_unsealed: usize,
    feature_recorded: bool,
    /// Whether this process said once that `retire` holds the family back
    /// (its feature unrecorded and the family not named).
    held_back_warned: bool,
    /// When the store last answered that the family's feature is not
    /// recorded: a held-back family asks again only after
    /// [`FEATURE_RECHECK`], not every pass (each ask opens a connection).
    unrecorded_at: Option<Instant>,
    /// Live segments whose blob condemnation this process has logged.
    condemned_warned: HashSet<Hash>,
    /// Runs whose merge came out over the byte cap: not tried again.
    oversize: HashSet<Vec<Hash>>,
    /// Live segments this process read whole and found sound: later passes
    /// only check their blobs are still stored.
    verified: HashSet<Hash>,
    /// Live segments the last pass found unreadable but kept: on a store
    /// with the feature recorded, a segment is dropped only when two passes
    /// in a row agree.
    suspect: HashSet<Hash>,
    /// Live segments whose blobs failed to read (an error that is neither
    /// "not stored" nor damage) in every pass of a streak.
    failing: HashMap<Hash, Failing>,
    /// Rows found unequal to their holding segment's value, with that
    /// segment: not read and reported again while it holds them.
    mismatched: HashMap<(Hash, u8), Hash>,
    /// Rows whose holding segment's value cannot be resolved (`content-v1`:
    /// a patch whose keyframe is lost), with that segment: not compared
    /// again while it holds them.
    unresolved: HashMap<(Hash, u8), Hash>,
    /// Rows that resolve while their holding segment's value does not:
    /// sealed again into a new segment, which then answers for them.
    reseal: HashSet<(Hash, u8)>,
    /// Keys with rows whose changeset is not stored (the changeset sweep
    /// was killed between rows and changeset): skipped until the next
    /// listing after `list_every`, and at most [`SKIPPED_CAP`] of them.
    unstored: HashSet<Hash>,
    /// Rows whose value does not decode, by key and column: not sealed,
    /// and skipped like `unstored` (cleared with it), so they do not make
    /// every listing a backfill again.
    bad: HashSet<(Hash, u8)>,
    /// When this process's last pass of the family ended.
    last_pass_end: Option<Instant>,
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
    /// `content-v1`: what retention keeps live, and when it was walked.
    content: Option<(Arc<ContentLive>, Instant)>,
    /// The family's live segments (with their column masks) the carried work
    /// above accounts for: those live at its listing, and those this
    /// process published since. A live segment outside it (another server's pass,
    /// a `repair drop`), or a dropped one, makes a pass forget the work
    /// and list again.
    known: HashMap<Hash, u64>,
}

/// Rows to compare and retire: by holding segment, each list by column and
/// key.
type RetireWork = VecDeque<(Hash, Vec<(Hash, u8)>)>;

/// Seal candidates carried across passes.
struct Backfill {
    /// Candidates (and the columns to seal) whose generation is not read
    /// yet.
    pending: Vec<(Hash, u64)>,
    /// Candidates with their generations; sorted by `(generation, id)` once
    /// `pending` is empty, then sealed from `next` on.
    order: Vec<(u64, Hash, u64)>,
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
    have: u64,
    /// Per column number, the position in the manifest's live list of its
    /// newest holding segment, or [`NONE`].
    holder: [u32; SLOTS],
}

const NONE: u32 = u32::MAX;

/// Column numbers a family this binary writes may use (`codec` tests check
/// every known column is below it).
pub(crate) const SLOTS: usize = 8;

fn slot(col: u8) -> usize {
    col as usize
}

/// What one pass did: totals over every family, and each family's own.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PassReport {
    /// Rows not in a live segment when the pass looked (every kind).
    pub unsealed_rows: u64,
    pub sealed_segments: u64,
    /// Keys (changesets) sealed.
    pub sealed_keys: u64,
    pub merges: u64,
    /// Segments merges replaced.
    pub merged_away: u64,
    /// Live segments that could not be read and were dropped from the
    /// manifest (their rows seal again, or derive again).
    pub dropped: u64,
    /// Live segments whose blobs failed to read in this pass with an error
    /// that is neither "not stored" nor damage: kept, and left out of the
    /// pass's merges and retirement.
    pub failing: u64,
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
    /// Keys a listing kept in memory: those with a row to seal, or (mode
    /// `retire`) one a ripe segment holds; never every key listed.
    pub listed_keys: u64,
    /// Whether the pass made progress and stopped with work left (a
    /// backfill's candidates beyond its pages or time, rows beyond its
    /// retire bound): the next pass should run soon.
    pub more: bool,
    /// Each family's report, by name (only in the total a pass returns).
    pub families: Vec<(&'static str, PassReport)>,
}

impl PassReport {
    fn add(&mut self, r: &PassReport) {
        self.unsealed_rows += r.unsealed_rows;
        self.sealed_segments += r.sealed_segments;
        self.sealed_keys += r.sealed_keys;
        self.merges += r.merges;
        self.merged_away += r.merged_away;
        self.dropped += r.dropped;
        self.failing += r.failing;
        self.retired_rows += r.retired_rows;
        self.mismatched += r.mismatched;
        self.bad_values += r.bad_values;
        self.live_segments += r.live_segments;
        self.live_bytes += r.live_bytes;
        self.damaged_manifest |= r.damaged_manifest;
        self.newer_manifest |= r.newer_manifest;
        self.listed |= r.listed;
        self.listed_keys += r.listed_keys;
        self.more |= r.more;
    }

    /// The report of family `name`, if the pass ran it.
    pub fn family(&self, name: &str) -> Option<&PassReport> {
        self.families
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, r)| r)
    }
}

fn bit(col: u8) -> u64 {
    1 << col
}

/// One pass under [`lease_key`]'s lease, every family in turn; `None` when
/// another server holds the lease. `features` records and reads each
/// family's store feature; mode `retire` refuses to delete without it. A
/// family whose pass fails does not stop the others (a damaged manifest of
/// one must not freeze the rest); the first error is returned once all
/// ran, except a lost lease or manifest swap, which stops at once.
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
    state.budget = PassBudget {
        started: Some(std::time::Instant::now()),
        ..PassBudget::default()
    };
    let first = state.rotate % FAMILIES.len();
    state.rotate = state.rotate.wrapping_add(1);
    let result = async {
        let mut total = PassReport::default();
        let mut failed = None;
        for fam in FAMILIES[first..].iter().chain(&FAMILIES[..first]) {
            let run = async {
                let mut p = Pass::load(repo, fam, opts, &lease, features).await?;
                p.run(state).await?;
                Ok::<_, Error>(p.report)
            }
            .await;
            match run {
                Ok(r) => {
                    total.add(&r);
                    total.families.push((fam.name, r));
                }
                Err(e @ Error::Conflict(_)) => return Err(e),
                Err(e) => {
                    tracing::warn!(family = fam.name, error = %e, "derived segments pass failed");
                    failed.get_or_insert(e);
                }
            }
        }
        match failed {
            Some(e) => Err(e),
            None => Ok(total),
        }
    }
    .await;
    gc_lease::release(lease, result).await.map(Some)
}

/// How long a held-back family trusts the store's answer that its feature
/// is not recorded. Another server recording it is seen this late, which
/// only delays this one's retirement.
pub(crate) const FEATURE_RECHECK: Duration = Duration::from_secs(60);

async fn feature_recorded(features: Option<&Features>, name: &'static str) -> Result<bool> {
    match features {
        Some(f) => (f.recorded)(name).await,
        None => Ok(false),
    }
}

/// The name of pause point `at` of `fam`, as the pause hook is given it:
/// as it is for `changeset-v1`, after `<family>:` for the others.
fn point(fam: &Family, at: &'static str) -> &'static str {
    match (fam.id, at) {
        (codec::FAMILY_CHANGESET, _) => at,
        (codec::FAMILY_CHANGESET_V2, "seal-stored") => "changeset-v2:seal-stored",
        (codec::FAMILY_CHANGESET_V2, "seal-swapped") => "changeset-v2:seal-swapped",
        (codec::FAMILY_CHANGESET_V2, "merge-stored") => "changeset-v2:merge-stored",
        (codec::FAMILY_CHANGESET_V2, "retire-deleted") => "changeset-v2:retire-deleted",
        (codec::FAMILY_CHANGESET_V2, "feature-recorded") => "changeset-v2:feature-recorded",
        (codec::FAMILY_CONTENT, "seal-stored") => "content-v1:seal-stored",
        (codec::FAMILY_CONTENT, "seal-swapped") => "content-v1:seal-swapped",
        (codec::FAMILY_CONTENT, "merge-stored") => "content-v1:merge-stored",
        (codec::FAMILY_CONTENT, "retire-deleted") => "content-v1:retire-deleted",
        (codec::FAMILY_CONTENT, "feature-recorded") => "content-v1:feature-recorded",
        _ => at,
    }
}

/// How long a pass of `content-v1` trusts the live set it walked while the
/// ref log has not moved: heads leave the retention window as time passes.
const CONTENT_LIVE_AGE: Duration = Duration::from_secs(3600);

/// The order of a `content-v1` key no current head holds (a keyframe of an
/// old version, a row sealed while live): after every walked one.
const UNWALKED: u64 = 1 << 40;

pub(crate) struct Pass<'a> {
    repo: &'a Repo,
    fam: &'static Family,
    opts: &'a CompactOptions,
    lease: &'a Option<GcLease>,
    features: Option<&'a Features>,
    /// The manifest row as this pass last read or wrote it.
    raw: Option<Bytes>,
    man: Manifest,
    /// Live segments whose index this pass loaded itself, by index hash.
    segs: HashMap<Hash, Arc<Live>>,
    /// Live segments whose index blob failed to read in this pass, with
    /// the error (neither "not stored" nor damage).
    index_errors: HashMap<Hash, Error>,
    /// Data blobs and blocks this pass read (one segment at a time where it
    /// reads many).
    data: HashMap<Hash, Arc<SegmentData>>,
    blocks: HashMap<(Hash, usize, usize), Arc<Block>>,
    /// Keys with rows, and which of the family's kinds they have rows of.
    rows: Option<HashMap<Hash, RowKey>>,
    /// Live segments whose decoded index came from the read view, not from
    /// a read of its blob by this pass.
    reused: HashSet<Hash>,
    started: std::time::Instant,
    /// The pass's shared budget while this family runs.
    budget: PassBudget,
    /// Live segments whose data this pass could not read: no merge or
    /// retire uses them again in this pass, and the next pass probes them
    /// whole.
    unreadable: HashSet<Hash>,
    /// Live segments found condemned by blob GC in this pass: no more of
    /// their rows are retired in it.
    condemned: HashSet<Hash>,
    /// `content-v1`: what retention keeps live, once this pass needed it,
    /// and whether the ref log has not moved since it was walked (a merge
    /// leaves keys out only then).
    live: Option<Arc<ContentLive>>,
    live_current: bool,
    /// Keys read at once when sealing or retiring `content-v1` rows, kept
    /// near [`READ_BYTES`] a read.
    read_step: usize,
    report: PassReport,
}

impl<'a> Pass<'a> {
    pub(crate) async fn load(
        repo: &'a Repo,
        fam: &'static Family,
        opts: &'a CompactOptions,
        lease: &'a Option<GcLease>,
        features: Option<&'a Features>,
    ) -> Result<Pass<'a>> {
        let (raw, read) = read_manifest(repo, fam.id).await?;
        let mut report = PassReport::default();
        let man = match read {
            ManifestRead::Absent => Manifest::new(fam.id),
            ManifestRead::Read(m) => m,
            ManifestRead::Newer => {
                // Never damage: a newer binary's format. Touch nothing.
                tracing::warn!(
                    family = fam.name,
                    "the {MANIFEST_KIND} row was written by a newer leep-server; this one \
                     runs no derived segment pass for the family"
                );
                report.newer_manifest = true;
                Manifest::new(fam.id)
            }
            ManifestRead::Damaged => {
                report.damaged_manifest = true;
                // Replaced only where every row is certainly still stored:
                // the store has never recorded the family's feature (another
                // server may have retired), and this process would not
                // retire the family itself. A `retire` server holding the
                // family back deletes none of its rows, so it may replace it.
                let would_retire =
                    opts.mode == Mode::Retire && opts.retire_families.iter().any(|n| n == fam.name);
                if would_retire || feature_recorded(features, fam.feature).await? {
                    return Err(Error::Other(format!(
                        "the {MANIFEST_KIND} row of family {} does not decode, and rows may \
                         have been retired (the store feature {} is recorded, or this server \
                         retires the family), so it may name the only copy of deleted rows; \
                         it is not replaced automatically (see LeepDerivedSegmentPassesFailing \
                         in RUNBOOKS.md)",
                        fam.name, fam.feature
                    )));
                }
                tracing::warn!(
                    family = fam.name,
                    "the {MANIFEST_KIND} row does not decode; replacing it"
                );
                Manifest::new(fam.id)
            }
        };
        let mut p = Pass {
            repo,
            fam,
            opts,
            lease,
            features,
            raw,
            man,
            segs: HashMap::new(),
            index_errors: HashMap::new(),
            data: HashMap::new(),
            blocks: HashMap::new(),
            rows: None,
            reused: HashSet::new(),
            started: std::time::Instant::now(),
            budget: PassBudget::default(),
            unreadable: HashSet::new(),
            condemned: HashSet::new(),
            live: None,
            live_current: false,
            read_step: match fam.keys {
                Keys::Changesets => opts.batch.max(1),
                Keys::Content => CONTENT_FIRST_STEP.min(opts.batch.max(1)),
            },
            report,
        };
        if !p.report.newer_manifest {
            p.load_segments().await?;
        }
        Ok(p)
    }

    /// Load the live segments' indexes itself, not through the shared read
    /// view (whose state a reader's transient error or a late read may
    /// have moved). An index that fails to read is remembered for
    /// [`Pass::check_live`]; the others are used.
    async fn load_segments(&mut self) -> Result<()> {
        // Decoded indexes the view already holds are reused: an index is
        // immutable and checked by address when loaded, so only the view's
        // list of segments and its broken set are its own state.
        let held: HashMap<Hash, Arc<codec::SegmentIndex>> = self
            .repo
            .segments()
            .family(self.fam.id)
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
        for (h, idx) in load_indexes(&**self.repo.blobs(), self.fam.id, &missing).await {
            match idx {
                Ok(Some(idx)) => {
                    if let Some(r) = self.man.live.iter().find(|s| s.index == h) {
                        self.index_errors.remove(&h);
                        self.segs.insert(h, Arc::new(Live { r: *r, idx }));
                    }
                }
                Ok(None) => {
                    self.index_errors.remove(&h);
                }
                Err(e) => {
                    self.index_errors.insert(h, e);
                }
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
        self.budget = state.budget;
        self.started = *self
            .budget
            .started
            .get_or_insert_with(std::time::Instant::now);
        let fam_state = state.families.entry(self.fam.id).or_default();
        let r = self.run_family(fam_state).await;
        fam_state.last_pass_end = Some(Instant::now());
        self.budget.retired_rows += self.report.retired_rows;
        state.budget = self.budget;
        r
    }

    async fn run_family(&mut self, state: &mut FamilyState) -> Result<()> {
        self.check_live(state).await?;
        let now = Instant::now();
        let live: HashSet<Hash> = self.man.live.iter().map(|s| s.index).collect();
        state.live_since.retain(|h, _| live.contains(h));
        for h in &live {
            state.live_since.entry(*h).or_insert(now);
        }
        state.verified.retain(|h| live.contains(h));
        state.condemned_warned.retain(|h| live.contains(h));
        state.mismatched.retain(|_, h| live.contains(h));
        state.unresolved.retain(|_, h| live.contains(h));
        state
            .oversize
            .retain(|run| run.iter().all(|h| live.contains(h)));
        let accounted = self
            .man
            .live
            .iter()
            .all(|s| state.known.get(&s.index) == Some(&s.columns));
        // Another server's pass only added segments (its seals): the carried
        // work stays valid once their keys are taken out of it, so two
        // servers taking turns do not each list every row again.
        let only_added = !accounted
            && self.report.dropped == 0
            && state.known.iter().all(|(h, cols)| {
                self.man
                    .live
                    .iter()
                    .any(|s| s.index == *h && s.columns == *cols)
            })
            && self
                .man
                .live
                .iter()
                .filter(|s| !state.known.contains_key(&s.index))
                .all(|s| self.segs.contains_key(&s.index));
        if only_added {
            let added: Vec<Arc<Live>> = self
                .man
                .live
                .iter()
                .filter(|s| !state.known.contains_key(&s.index))
                .filter_map(|s| self.segs.get(&s.index).cloned())
                .collect();
            if let Some(bf) = state.backfill.as_mut() {
                let held = |k: &Hash, m: u64| -> u64 {
                    let mut m = m;
                    for l in &added {
                        for c in self.fam.columns {
                            if m & bit(c.id) != 0 && l.r.has(c.id) && l.idx.has(k, c.id) {
                                m &= !bit(c.id);
                            }
                        }
                    }
                    m
                };
                for (k, m) in bf.pending.iter_mut() {
                    *m = held(k, *m);
                }
                bf.pending.retain(|(_, m)| *m != 0);
                let next = bf.next.min(bf.order.len());
                for (_, k, m) in bf.order[next..].iter_mut() {
                    *m = held(k, *m);
                }
            }
            for l in &added {
                state.known.insert(l.r.index, l.r.columns);
            }
        } else if self.report.dropped > 0 || !accounted {
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
        if self.opts.mode == Mode::Retire
            && state.backfill.is_none()
            && self.may_retire(state).await?
        {
            self.retire(state).await?;
        }
        self.report.live_segments = self.man.live.len() as u64;
        self.report.live_bytes = self.man.live.iter().map(|s| s.data_len as u64).sum::<u64>();
        Ok(())
    }

    /// Whether `retire` may delete this family's rows: the family is named
    /// in [`CompactOptions::retire_families`], or its store feature is
    /// already recorded (so the door is passed). Otherwise it is held back,
    /// said once per process.
    async fn may_retire(&mut self, state: &mut FamilyState) -> Result<bool> {
        if state.feature_recorded || self.opts.retire_families.iter().any(|n| n == self.fam.name) {
            return Ok(true);
        }
        if state
            .unrecorded_at
            .is_some_and(|t| t.elapsed() < FEATURE_RECHECK)
        {
            return Ok(false);
        }
        if feature_recorded(self.features, self.fam.feature).await? {
            state.feature_recorded = true;
            return Ok(true);
        }
        state.unrecorded_at = Some(Instant::now());
        if !state.held_back_warned {
            state.held_back_warned = true;
            tracing::warn!(
                family = self.fam.name,
                feature = self.fam.feature,
                "--derived-segments retire deletes no {} row: the store feature {} is not \
                 recorded and the family is not named in --derived-segments-retire-families; \
                 name it once every binary runs this release (ADR 0105)",
                self.fam.name,
                self.fam.feature
            );
        }
        Ok(false)
    }

    /// Whether `s`'s blobs read, probed by this pass itself: `Lost` only
    /// for definite loss or damage (not stored, address or structure wrong,
    /// a read the store reports corrupt), `Failing` for any other read
    /// error. A segment read whole once in this process is later only
    /// checked for presence, as is an index taken from the read view. Every
    /// "not stored" is exact (a blob packed a moment ago by another process
    /// is not taken for lost).
    async fn probe(&mut self, s: &SegmentRef, state: &mut FamilyState) -> Probe {
        let blobs = self.repo.blobs().clone();
        let Some(live) = self.segs.get(&s.index).cloned() else {
            return match self.index_errors.remove(&s.index) {
                Some(e) => Probe::Failing(e),
                // `load_indexes` gives `None` only for definite loss or damage.
                None => Probe::Lost,
            };
        };
        let present = |h: Hash| {
            let blobs = blobs.clone();
            async move {
                match segment_blob_stored(&*blobs, &h).await {
                    Ok(true) => Probe::Sound,
                    Ok(false) => Probe::Lost,
                    Err(e) => Probe::Failing(e),
                }
            }
        };
        if self.reused.contains(&s.index) {
            match present(s.index).await {
                Probe::Sound => {}
                other => return other,
            }
        }
        if state.verified.contains(&s.index) {
            return present(s.data).await;
        }
        let read = self.read_whole(&live).await;
        if matches!(read, Probe::Sound) {
            state.verified.insert(s.index);
        }
        read
    }

    /// Read `live`'s data blob whole and decode every value: a real read,
    /// not an existence check.
    async fn read_whole(&self, live: &Live) -> Probe {
        let s = &live.r;
        let bytes = match get_segment_blob(&**self.repo.blobs(), &s.data).await {
            Ok(Some(b)) => b,
            Ok(None) => return Probe::Lost,
            Err(e) if crate::gc_segments::segment_reads_corrupt(&e) => return Probe::Lost,
            Err(e) => return Probe::Failing(e),
        };
        match codec::decode_data(&live.idx, &s.data, bytes)
            .and_then(|d| codec::read_all(&live.idx, &d).map(|_| d))
        {
            Ok(_) => Probe::Sound,
            Err(_) => Probe::Lost,
        }
    }

    /// Drop live segments that cannot be read (their keys then read as rows
    /// or derive again, and seal again), forget retired blobs no longer
    /// stored, and replace a manifest that did not decode. On a store with
    /// the feature recorded a lost segment is dropped only when this pass
    /// and the last both found it unreadable. A segment whose blobs fail to
    /// read is kept and left out of this pass's work, and dropped once it
    /// failed [`FAILING_PASSES`] passes in a row over at least
    /// [`CompactOptions::drop_failing_after`]; when no live segment reads
    /// whole (a family whose only live segment fails included), the store
    /// is taken to be failing and the pass fails, dropping nothing.
    async fn check_live(&mut self, state: &mut FamilyState) -> Result<()> {
        let mut bad = Vec::new();
        let mut failing = Vec::new();
        let mut sound: Vec<SegmentRef> = Vec::new();
        let mut first_error = None;
        for s in self.man.live.clone() {
            match self.probe(&s, state).await {
                Probe::Sound => sound.push(s),
                Probe::Lost => bad.push(s),
                Probe::Failing(e) => {
                    failing.push(s);
                    first_error.get_or_insert(e);
                }
            }
        }
        let now = Instant::now();
        // Streaks end for segments that read again, and all of them when
        // this process ran no pass of the family for longer than the gap
        // (passes elsewhere, or none), measured from the end of its last
        // pass, so a pass longer than the interval does not end them.
        let recent = state
            .last_pass_end
            .is_some_and(|end| now.duration_since(end) <= self.opts.failing_gap);
        state
            .failing
            .retain(|h, _| recent && failing.iter().any(|s| s.index == *h));
        if let Some(e) = first_error {
            // Only a family with a segment that reads whole in this pass
            // tells a failing segment from a failing store: an existence
            // check (a segment read whole earlier) is not enough, since a
            // store can answer those while its reads fail. Otherwise (a
            // lone live segment included) the pass fails and drops nothing.
            let mut whole = false;
            for s in &sound {
                let Some(live) = self.segs.get(&s.index).cloned() else {
                    continue;
                };
                if matches!(self.read_whole(&live).await, Probe::Sound) {
                    whole = true;
                    break;
                }
            }
            if !whole {
                return Err(e);
            }
            self.report.failing = failing.len() as u64;
            for s in &failing {
                let f = state.failing.entry(s.index).or_insert(Failing {
                    since: now,
                    passes: 0,
                });
                f.passes += 1;
                let f = *f;
                self.unreadable.insert(s.index);
                state.verified.remove(&s.index);
                metrics::counter!(
                    "leep_derived_segment_errors_total",
                    "what" => "failing", "family" => self.fam.name
                )
                .increment(1);
                let due = self
                    .opts
                    .drop_failing_after
                    .is_some_and(|after| now.duration_since(f.since) >= after);
                if f.passes >= FAILING_PASSES && due {
                    tracing::error!(
                        segment = %s.index,
                        family = self.fam.name,
                        passes = f.passes,
                        error = %e,
                        "a derived segment's blobs failed to read in every pass for a while; \
                         dropping it, its keys seal again from rows or derive again"
                    );
                    bad.push(*s);
                } else {
                    tracing::warn!(
                        segment = %s.index,
                        family = self.fam.name,
                        passes = f.passes,
                        error = %e,
                        "a derived segment's blobs cannot be read now; it is kept and skipped"
                    );
                }
            }
        }
        let careful = !bad.is_empty()
            && (state.feature_recorded
                || feature_recorded(self.features, self.fam.feature).await?);
        let mut gone = Vec::new();
        let mut suspect = HashSet::new();
        for s in bad {
            let failed = state
                .failing
                .get(&s.index)
                .is_some_and(|f| f.passes >= FAILING_PASSES);
            if careful && !failed && !state.suspect.contains(&s.index) {
                tracing::warn!(
                    segment = %s.index,
                    family = self.fam.name,
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
            // Forgotten only once surely gone: a retired blob that fails to
            // read stays listed (GC collects it as retired).
            if self.repo.blobs().contains(h).await.unwrap_or(true) {
                retired.push(*h);
            }
        }
        let pruned = retired.len() != self.man.retired.len();
        self.man.retired = retired;
        if !gone.is_empty() {
            tracing::warn!(
                segments = gone.len(),
                family = self.fam.name,
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

    /// Most bytes of one of the family's data blobs.
    fn max_bytes(&self) -> usize {
        match self.fam.keys {
            Keys::Changesets => self.opts.max_bytes,
            Keys::Content => self.opts.content_max_bytes,
        }
    }

    /// Most keys in one of the family's segments.
    fn max_rows(&self) -> usize {
        match self.fam.keys {
            Keys::Changesets => self.opts.max_rows,
            Keys::Content => self.opts.content_max_rows,
        }
    }

    /// The tier `s` merges in; `None`: never merged by tier. `content-v1`
    /// has two tiers (ADR 0106): segments under half of both caps (bytes
    /// and keys) merge with their neighbours while the result fits, so
    /// segments end between half the cap and the cap; full ones are only
    /// rewritten once a third of them is dead. A segment stopped by the key
    /// cap does not stay small.
    fn tier(&self, s: &SegmentRef) -> Option<u32> {
        match self.fam.keys {
            Keys::Changesets => Some(self.opts.tier(s.rows)),
            Keys::Content => ((s.data_len as usize).saturating_mul(2) < self.max_bytes()
                && (s.rows as usize).saturating_mul(2) < self.max_rows())
            .then_some(0),
        }
    }

    /// `content-v1`: what retention keeps live, walked again when the ref
    /// log moved (and the last walk is older than `list_every`) or the last
    /// walk is older than [`CONTENT_LIVE_AGE`].
    async fn content_live(&mut self, state: &mut FamilyState) -> Result<Arc<ContentLive>> {
        if let Some(l) = &self.live {
            return Ok(l.clone());
        }
        let seq = self.repo.newest_ref_seq().await?;
        if let Some((l, at)) = &state.content {
            let same = l.seq == seq;
            if (same && at.elapsed() < CONTENT_LIVE_AGE) || at.elapsed() < self.opts.list_every {
                self.live_current = same;
                self.live = Some(l.clone());
                return Ok(l.clone());
            }
        }
        // The old set goes before the new one is built: one at a time.
        state.content = None;
        let now = self.opts.content_now.unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_or(0, |d| d.as_secs() as i64)
        });
        let l = Arc::new(
            self.repo
                .content_live(self.opts.content_keep, self.opts.content_skew, now)
                .await?,
        );
        self.live_current = l.seq == seq;
        state.content = Some((l.clone(), Instant::now()));
        self.live = Some(l.clone());
        Ok(l)
    }

    /// The columns of `key` a seal may take: every column of a changeset
    /// family; in `content-v1` the keyframes always (a live patch may stand
    /// on the keyframe of a version no head holds), and a blob or tree row
    /// only while retention keeps it live.
    fn keep_mask(&self, key: &Hash) -> u64 {
        match (self.fam.keys, &self.live) {
            (Keys::Changesets, _) => u64::MAX,
            (Keys::Content, Some(l)) if l.live(key) => u64::MAX,
            (Keys::Content, Some(_)) => self.fam.keyframe_mask(),
            (Keys::Content, None) => u64::MAX,
        }
    }

    /// The columns of `key` a merge keeps: [`Pass::keep_mask`], and in
    /// `content-v1` every directory filter too, live or not
    /// ([`Family::merge_kept_mask`]).
    fn merge_keep_mask(&self, key: &Hash) -> u64 {
        self.keep_mask(key) | self.fam.merge_kept_mask()
    }

    /// Whether the ref log has not moved since the walk this pass judges
    /// liveness by: a merge leaves keys out only then. Read once a merge
    /// that leaves keys out is encoded, before its segment is stored, so
    /// a ref moving between this read and the swap is not seen; only blob
    /// rows are left out, and retention's backfill derives those again.
    async fn walk_still_current(&self) -> Result<bool> {
        let Some(l) = &self.live else {
            return Ok(false);
        };
        Ok(self.repo.newest_ref_seq().await? == l.seq)
    }

    /// The sort keys of `ids` in a segment of the family: the generations of
    /// stored changesets (`None` for one that is not, left out), or for
    /// `content-v1` the place in the heads' walk (after it for a key it does
    /// not reach).
    async fn order_keys(
        &mut self,
        ids: &[Hash],
        state: &mut FamilyState,
    ) -> Result<Vec<Option<u64>>> {
        match self.fam.keys {
            Keys::Changesets => generations(self.repo, ids, self.opts.batch).await,
            Keys::Content => {
                let live = self.content_live(state).await?;
                Ok(ids
                    .iter()
                    .map(|k| Some(live.order(k).map_or(UNWALKED, u64::from)))
                    .collect())
            }
        }
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
                for c in self.fam.columns {
                    let col = c.id;
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
            r.holder = [NONE; SLOTS];
        }
        for (i, s) in self.man.live.iter().enumerate().rev() {
            let Some(l) = self.segs.get(&s.index) else {
                continue;
            };
            for c in self.fam.columns {
                let col = c.id;
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
    fn mark_unreadable(&mut self, index: Hash, state: &mut FamilyState) {
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
        let bytes = match get_segment_blob(&**self.repo.blobs(), &live.r.data).await {
            Ok(Some(b)) => b,
            Ok(None) => return Ok(None),
            Err(e) if crate::gc_segments::segment_reads_corrupt(&e) => return Ok(None),
            Err(e) => {
                // Left out of the rest of the pass like a lost one; the next
                // pass's probe decides about it.
                tracing::warn!(segment = %live.r.index, error = %e, "a derived segment's data cannot be read now");
                return Ok(None);
            }
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

    /// The keys with rows of the family's kinds: listed again in mode
    /// `retire` unless a retirement is carried over (only unsealed rows
    /// remain), in mode `build` once their estimated count grew by
    /// `seal_rows` or `list_every` passed.
    async fn list_rows(&mut self, state: &mut FamilyState) -> Result<()> {
        if self.rows.is_some() {
            return Ok(());
        }
        let mut estimate = 0;
        for c in self.fam.columns {
            estimate += self.repo.row_meta().estimate_derived(c.kind).await?.rows;
        }
        let due = match state.listed {
            None => true,
            Some((at, n)) => {
                // In mode retire only unsealed rows, rows retention no longer
                // keeps (search rows never sealed) and rows awaiting their
                // segment's age remain: listed again when their count moved,
                // a segment ripened since, or `list_every` passed, not every
                // pass (ADR 0106).
                let retire_due = self.opts.mode == Mode::Retire
                    && state.retire_left.is_none()
                    && (estimate != n || {
                        let now = Instant::now();
                        state.live_since.values().any(|t| {
                            let ripe = *t + self.opts.retire_after;
                            ripe > at && ripe <= now
                        })
                    });
                retire_due
                    || estimate >= n + (self.opts.seal_rows * self.fam.columns.len().max(1)) as u64
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
            || state.unstored.len() + state.bad.len() > SKIPPED_CAP
        {
            state.unstored.clear();
            state.bad.clear();
            state.unstored_cleared = Some(Instant::now());
        }
        state.listed = Some((Instant::now(), estimate));
        self.report.listed = true;
        if self.fam.keys == Keys::Content && !self.man.live.is_empty() {
            // Liveness decides which listed keys are kept, below.
            self.content_live(state).await?;
        }
        // Segments whose rows a retirement would delete now.
        let now = Instant::now();
        let ripe: HashSet<u32> = if self.opts.mode == Mode::Retire {
            self.man
                .live
                .iter()
                .enumerate()
                .filter(|(_, s)| {
                    state
                        .live_since
                        .get(&s.index)
                        .is_some_and(|t| now.duration_since(*t) >= self.opts.retire_after)
                })
                .map(|(i, _)| i as u32)
                .collect()
        } else {
            HashSet::new()
        };
        // Only keys a seal or a retirement needs are kept: a row no live
        // segment holds (or one to seal again) that the family keeps, or
        // one a ripe segment holds in mode retire. In mode build every
        // sealed row stays, so keeping them all would hold every key of the
        // family in memory at each listing.
        let mut rows: HashMap<Hash, RowKey> = HashMap::new();
        for c in self.fam.columns {
            let (col, kind) = (c.id, c.kind);
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
                let held = self.page_holders(&page, col);
                for (k, at) in page.into_iter().zip(held) {
                    if state.unstored.contains(&k) || state.bad.contains(&(k, col)) {
                        continue;
                    }
                    let open = at == NONE || state.reseal.contains(&(k, col));
                    let wanted = (open && self.keep_mask(&k) & bit(col) != 0)
                        || (at != NONE && ripe.contains(&at));
                    if !wanted {
                        continue;
                    }
                    let r = rows.entry(k).or_insert(RowKey {
                        have: 0,
                        holder: [NONE; SLOTS],
                    });
                    r.have |= bit(col);
                    r.holder[slot(col)] = at;
                }
            }
        }
        // Work still carried accounts for every live segment (`run` checked),
        // so it stays valid; from here on so does what this listing finds.
        state.known = self.man.live.iter().map(|s| (s.index, s.columns)).collect();
        self.report.listed_keys = rows.len() as u64;
        self.rows = Some(rows);
        Ok(())
    }

    /// The newest live segment holding each of `page`'s keys (ascending, as
    /// listed) for `col`, as its position in the live list: each segment's
    /// keys in the page's range are walked once, so a whole listing costs
    /// about the sealed keys plus a few searches per page and segment.
    fn page_holders(&self, page: &[Hash], col: u8) -> Vec<u32> {
        let mut out = vec![NONE; page.len()];
        let (Some(lo), Some(hi)) = (page.first(), page.last()) else {
            return out;
        };
        for (i, s) in self.man.live.iter().enumerate().rev() {
            if !s.has(col) {
                continue;
            }
            let Some(l) = self.segs.get(&s.index) else {
                continue;
            };
            let keys = l.idx.keys();
            let start = keys.partition_point(|k| k < lo);
            for k in keys[start..].iter().take_while(|k| *k <= hi) {
                if let Ok(j) = page.binary_search(k) {
                    if out[j] == NONE && l.idx.has(k, col) {
                        out[j] = i as u32;
                    }
                }
            }
        }
        out
    }

    /// The keys to seal from a fresh listing, once there are enough of
    /// them: rows no live segment holds, a key missing a row of a column
    /// its family expects once it waited.
    fn seal_candidates(&mut self, state: &mut FamilyState) -> Vec<(Hash, u64)> {
        let now = Instant::now();
        let mut rows = self.rows.take().unwrap_or_default();
        self.place_holders(&mut rows);
        // A row to seal again stays one only while it is listed and its
        // holder is still the segment it could not be resolved against (its
        // row deleted, gone dead, or its holder merged or dropped: forgotten).
        state.reseal.retain(|(k, col)| {
            rows.get(k).is_some_and(|r| {
                let at = r.holder[slot(*col)];
                r.have & bit(*col) != 0
                    && at != NONE
                    && state.unresolved.get(&(*k, *col)) == Some(&self.man.live[at as usize].index)
            })
        });
        let mut unsealed_rows = 0u64;
        let mut lone_now: HashSet<Hash> = HashSet::new();
        let mut cands: Vec<(Hash, u64)> = Vec::new();
        let expected = self.fam.expected;
        let mut dead_now: Vec<Hash> = Vec::new();
        let mut resealing = false;
        for (k, r) in &rows {
            let mut open = 0u64;
            let mut present = r.have;
            for c in self.fam.columns {
                let col = c.id;
                if r.have & bit(col) != 0
                    && (r.holder[slot(col)] == NONE || state.reseal.contains(&(*k, col)))
                {
                    open |= bit(col);
                }
            }
            // `content-v1` seals only what retention keeps (ADR 0106); the
            // listing already left out keys with nothing it keeps, so they
            // cost no memory between listings.
            let kept = open & self.keep_mask(k);
            if open != 0 && kept == 0 {
                dead_now.push(*k);
            }
            open = kept;
            if open == 0 {
                continue;
            }
            resealing |= self
                .fam
                .columns
                .iter()
                .any(|c| open & bit(c.id) != 0 && state.reseal.contains(&(*k, c.id)));
            unsealed_rows += open.count_ones() as u64;
            // Columns with no row may be sealed already.
            for c in self.fam.columns {
                if present & bit(c.id) == 0 && self.holder(k, c.id) != NONE {
                    present |= bit(c.id);
                }
            }
            if present & expected == expected {
                cands.push((*k, open));
                continue;
            }
            // A row is not derived yet (the warmer, or a query, is behind):
            // wait for it a while, then seal what there is.
            lone_now.insert(*k);
            let since = *state.lone_since.entry(*k).or_insert(now);
            if now.duration_since(since) >= self.opts.lone_wait {
                cands.push((*k, open));
            }
        }
        let keyed = rows.len();
        self.rows = Some(rows);
        if !state.reseal.is_empty() {
            let dead: HashSet<&Hash> = dead_now.iter().collect();
            state.reseal.retain(|(k, _)| !dead.contains(k));
        }
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
        // Rows to seal again (their sealed value cannot be resolved) among
        // the candidates are sealed at the listing that finds them, whatever
        // the thresholds: they are few, and are not compared meanwhile.
        if n == 0 || (n < self.opts.seal_rows && !(n >= self.opts.idle_rows && idle) && !resealing)
        {
            return Vec::new();
        }
        cands
    }

    /// Seal what a listing found unsealed. A listing's candidates become a
    /// backfill the following passes go on with, without listing again:
    /// first their generations (in batches, within `max_seal_time`), then,
    /// sorted by `(generation, id)`, at most `max_seal_pages` pages a pass.
    async fn seal(&mut self, state: &mut FamilyState) -> Result<()> {
        if state.backfill.is_none() {
            self.list_rows(state).await?;
            if !self.report.listed {
                return Ok(());
            }
            if self.fam.keys == Keys::Content {
                self.content_live(state).await?;
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
            let gens = self.order_keys(&ids, state).await?;
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
        let page_rows = self.max_rows().max(1);
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
    /// not decode is remembered and not listed again for a while. A page of
    /// `content-v1` (rows of up to 64 KiB, four columns) is read a few keys
    /// at a time, as many as keep a read near [`READ_BYTES`], and sealed
    /// once its rows reach about the cap, so a pass holds about one
    /// segment's rows at once.
    async fn seal_page(
        &mut self,
        page: &[(u64, Hash, u64)],
        state: &mut FamilyState,
    ) -> Result<()> {
        let (mut step, cut) = match self.fam.keys {
            Keys::Changesets => (page.len().max(1), usize::MAX),
            Keys::Content => (self.read_step, self.max_bytes() / 3 * 4),
        };
        let mut seal: Vec<SealRow> = Vec::new();
        let mut raw = 0usize;
        let mut at = 0;
        while at < page.len() {
            let part = &page[at..(at + step).min(page.len())];
            at += part.len();
            let rows = self.read_seal_rows(part).await?;
            if self.fam.keys == Keys::Content {
                let read: usize = rows
                    .iter()
                    .flat_map(|r| r.values.iter().map(|(_, v)| v.len()))
                    .sum();
                step = self.adapt_step(read);
            }
            for row in rows {
                raw += row.values.iter().map(|(_, v)| v.len()).sum::<usize>();
                seal.push(row);
                if raw >= cut {
                    self.seal_rows(std::mem::take(&mut seal), state).await?;
                    raw = 0;
                }
            }
        }
        self.seal_rows(seal, state).await
    }

    /// The keys to read at once next, after a read of `read` bytes: halved
    /// past [`READ_BYTES`], doubled (up to the batch) under a quarter of it.
    /// Changeset rows are small; this only steers `content-v1`.
    fn adapt_step(&mut self, read: usize) -> usize {
        let batch = self.opts.batch.max(1);
        if read > READ_BYTES {
            self.read_step = (self.read_step / 2).max(1);
        } else if read < READ_BYTES / 4 {
            self.read_step = (self.read_step * 2).min(batch);
        }
        self.read_step = self.read_step.min(batch);
        self.read_step
    }

    /// The rows of `part` (keys with the columns to seal), as stored now.
    async fn read_seal_rows(&self, part: &[(u64, Hash, u64)]) -> Result<Vec<SealRow>> {
        let mut values: HashMap<Hash, Vec<(u8, Bytes)>> = HashMap::new();
        for c in self.fam.columns {
            let (col, kind) = (c.id, c.kind);
            let keys: Vec<Hash> = part
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
        Ok(part
            .iter()
            .filter_map(|(g, k, _)| {
                values.remove(k).map(|v| SealRow {
                    key: *k,
                    order: *g,
                    values: v,
                })
            })
            .collect())
    }

    /// Seal `seal` in segments that fit the family's cap.
    async fn seal_rows(&mut self, seal: Vec<SealRow>, state: &mut FamilyState) -> Result<()> {
        if seal.is_empty() {
            return Ok(());
        }
        let (fam, cap) = (self.fam.id, self.max_bytes());
        // zstd at level 17 takes a while: off the async workers.
        let out = tokio::task::spawn_blocking(move || {
            let mut out = Vec::new();
            encode_capped(fam, cap, &seal, &mut out).map(|()| out)
        })
        .await
        .map_err(|e| Error::Other(format!("encoding a derived segment: {e}")))??;
        for (enc, bad, keys) in out {
            for b in &bad {
                tracing::warn!(
                    key = %b.key,
                    family = self.fam.name,
                    column = b.col,
                    "derived row not sealed: {}",
                    b.why
                );
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
            self.pause("seal-swapped").await;
            // Only this segment's rows: a later one that fails to store
            // leaves its rows to seal again.
            if !state.reseal.is_empty() {
                let keys: HashSet<Hash> = keys.into_iter().collect();
                state
                    .reseal
                    .retain(|(k, c)| !(keys.contains(k) && enc.seg.has(*c)));
            }
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
            (p.0)(point(self.fam, at)).await;
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
    /// changesets no longer stored (the changeset sweep) are left out. In
    /// `content-v1`, rows retention no longer keeps are left out too (only
    /// while the walk that found them is current), and a segment of which a
    /// third is such rows is rewritten alone (ADR 0106).
    async fn merge(&mut self, state: &mut FamilyState) -> Result<()> {
        let f = self.opts.fanout.max(2);
        let mut omit = if self.fam.keys == Keys::Content && !self.man.live.is_empty() {
            self.content_live(state).await?;
            self.live_current
        } else {
            false
        };
        let cap = self.max_bytes();
        let max_rows = self.max_rows();
        // Segments a third dead, found once a pass: each is rewritten alone.
        let mut dead: VecDeque<Hash> = if self.fam.keys == Keys::Content {
            self.man
                .live
                .iter()
                .filter(|s| self.mostly_dead(s, omit))
                .map(|s| s.index)
                .collect()
        } else {
            VecDeque::new()
        };
        // Each round merges (the live list shrinks), rewrites a segment once
        // (the dead list shrinks) or skips one (the skip set grows), so it
        // ends; the bound is defense in depth.
        let deadline = self.started + self.opts.max_seal_time;
        let mut merged = false;
        for _ in 0..4 * self.man.live.len() + 16 {
            // Bounded like a backfill: with work left, the next pass soon.
            if merged && std::time::Instant::now() >= deadline {
                self.report.more = true;
                return Ok(());
            }
            let live = self.man.live.clone();
            let found = {
                let mergeable = |s: &SegmentRef| {
                    // A segment holding a column this binary does not know
                    // is never merged: the merge could not carry the column
                    // (ADR 0105). A newer binary merges it.
                    self.segs
                        .get(&s.index)
                        .is_some_and(|l| !l.idx.has_unknown_columns())
                        && !self.unreadable.contains(&s.index)
                };
                // The first run of `f` adjacent loaded segments of one
                // tier; of it, the longest prefix (two or more) within the
                // caps.
                let fits = |run: &[SegmentRef]| -> usize {
                    let (mut rows, mut bytes, mut k) = (0usize, 0usize, 0);
                    for s in run {
                        rows += s.rows as usize;
                        bytes += s.data_len as usize;
                        if rows > max_rows || bytes > cap {
                            break;
                        }
                        k += 1;
                    }
                    k
                };
                let tiered = if self.fam.keys == Keys::Content {
                    self.content_run(&live, &mergeable, &fits, &state.oversize)
                } else {
                    (0..live.len().saturating_sub(f - 1)).find_map(|i| {
                        let run = &live[i..i + f];
                        let t = self.tier(&run[0])?;
                        if !run.iter().all(|s| self.tier(s) == Some(t) && mergeable(s)) {
                            return None;
                        }
                        let k = fits(run);
                        let ids: Vec<Hash> = run[..k].iter().map(|s| s.index).collect();
                        (k >= 2 && !state.oversize.contains(&ids)).then_some((i, k))
                    })
                };
                tiered.or_else(|| {
                    while let Some(h) = dead.pop_front() {
                        if let Some(i) = live.iter().position(|s| s.index == h && mergeable(s)) {
                            return Some((i, 1));
                        }
                    }
                    None
                })
            };
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
            let before = values.len();
            if omit {
                values.retain(|(key, col), _| self.merge_keep_mask(key) & bit(*col) != 0);
            }
            let omitted = values.len() != before;
            let mut keys: Vec<Hash> = values.keys().map(|(k, _)| *k).collect();
            keys.sort_unstable();
            keys.dedup();
            let orders = self.order_keys(&keys, state).await?;
            let mut rows = Vec::with_capacity(keys.len());
            for (key, g) in keys.iter().zip(orders) {
                let Some(g) = g else { continue };
                let v: Vec<(u8, Bytes)> = self
                    .fam
                    .columns
                    .iter()
                    .filter_map(|c| values.remove(&(*key, c.id)).map(|b| (c.id, b)))
                    .collect();
                rows.push(SealRow {
                    key: *key,
                    order: g,
                    values: v,
                });
            }
            let fam = self.fam.id;
            let (enc, bad) =
                tokio::task::spawn_blocking(move || codec::encode(fam, &rows, codec::nonce()))
                    .await
                    .map_err(|e| Error::Other(format!("encoding a derived segment: {e}")))??;
            self.report.bad_values += bad.len() as u64;
            if enc.seg.data_len as usize > cap {
                state.oversize.insert(run.iter().map(|s| s.index).collect());
                continue;
            }
            // Keys are left out only while the walk that judged them is
            // current: a ref that moved meanwhile (a revert, a branch at an
            // old revision) may hold them again. Then nothing more is left
            // out in this pass, and this run is merged again keeping
            // everything.
            if omitted && !self.walk_still_current().await? {
                omit = false;
                self.live_current = false;
                dead.clear();
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
            for s in &run {
                state.known.remove(&s.index);
            }
            state.live_since.insert(enc.seg.index, Instant::now());
            state.verified.insert(enc.seg.index);
            self.report.merges += 1;
            self.report.merged_away += k as u64;
            merged = true;
        }
        tracing::warn!("derived segment merges stopped at their round bound");
        Ok(())
    }

    /// `content-v1`: the run to merge, if any. First a run of two or more
    /// adjacent small segments (under half of both caps), up to `fanout`,
    /// its longest prefix that fits; else a small segment between larger
    /// ones (not the newest, which waits for the next seal) with its
    /// smaller neighbour, if the two fit. Adjacent pairs that cannot merge
    /// exceed one cap together, so the live segments number about
    /// 2·bytes/cap + 2·keys/keycap + 1 at most, plus those never merged
    /// (unknown columns, unreadable, a run found oversize).
    pub(crate) fn content_run(
        &self,
        live: &[SegmentRef],
        mergeable: &dyn Fn(&SegmentRef) -> bool,
        fits: &dyn Fn(&[SegmentRef]) -> usize,
        oversize: &HashSet<Vec<Hash>>,
    ) -> Option<(usize, usize)> {
        let f = self.opts.fanout.max(2);
        let small = |s: &SegmentRef| self.tier(s).is_some() && mergeable(s);
        let ok = |at: usize, k: usize| {
            let ids: Vec<Hash> = live[at..at + k].iter().map(|s| s.index).collect();
            !oversize.contains(&ids)
        };
        for i in 0..live.len() {
            if !small(&live[i]) {
                continue;
            }
            let mut j = i;
            while j < live.len() && j - i < f && small(&live[j]) {
                j += 1;
            }
            let k = fits(&live[i..j]);
            if k >= 2 && ok(i, k) {
                return Some((i, k));
            }
        }
        for i in 0..live.len().saturating_sub(1) {
            if !small(&live[i]) {
                continue;
            }
            let mut sides: Vec<usize> = Vec::new();
            if i > 0 && mergeable(&live[i - 1]) {
                sides.push(i - 1);
            }
            if mergeable(&live[i + 1]) {
                sides.push(i + 1);
            }
            sides.sort_by_key(|&n| live[n].data_len);
            for n in sides {
                let at = i.min(n);
                if fits(&live[at..at + 2]) == 2 && ok(at, 2) {
                    return Some((at, 2));
                }
            }
        }
        None
    }

    /// `content-v1`: whether a third or more of `s`'s values are dead:
    /// values of a column `repair drop` cleared from its mask, and with
    /// `by_liveness` rows retention no longer keeps (keyframes always count
    /// as kept).
    fn mostly_dead(&self, s: &SegmentRef, by_liveness: bool) -> bool {
        let Some(l) = self.segs.get(&s.index) else {
            return false;
        };
        let (mut total, mut dead) = (0usize, 0usize);
        for c in self.fam.columns {
            let n = l.idx.keys_with(c.id).count();
            if !s.has(c.id) {
                total += n;
                dead += n;
                continue;
            }
            for k in l.idx.keys_with(c.id) {
                total += 1;
                if by_liveness && self.merge_keep_mask(k) & bit(c.id) == 0 {
                    dead += 1;
                }
            }
        }
        total > 0 && dead * 3 >= total
    }

    /// Delete rows a live segment, seen live for at least
    /// [`CompactOptions::retire_after`], holds the same value for. The
    /// feature is recorded first; the manifest is read again under the
    /// lease and must be the one this pass holds; each step checks the
    /// lease, compares every row with its segment value, reads the
    /// segment's blob condemnations again (after the feature is recorded),
    /// and deletes a row only if it still holds the bytes compared (one
    /// statement), so a row `repair` rewrote meanwhile stays.
    async fn retire(&mut self, state: &mut FamilyState) -> Result<()> {
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
        let (raw, _) = read_manifest(self.repo, self.fam.id).await?;
        if raw != self.raw {
            return Err(Error::Conflict(format!(
                "the {MANIFEST_KIND} row changed under this pass; stopping before deleting rows"
            )));
        }
        let mut work = match state.retire_left.take() {
            Some(w) => w,
            None => self.retire_work(&ripe, state).await?,
        };
        let retire_started = *self
            .budget
            .retire_started
            .get_or_insert_with(std::time::Instant::now);
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
            // Spares comparing a condemned segment's rows; each step reads
            // the condemnations again before it deletes.
            if self.condemned(&l, state).await? {
                continue;
            }
            let mut i = 0;
            while i < cands.len() {
                if (self.budget.retired_rows + self.report.retired_rows) as usize
                    >= self.opts.max_retire_rows
                    || retire_started.elapsed() >= self.opts.max_retire_time
                {
                    cands.drain(..i);
                    work.push_front((seg, cands));
                    state.retire_left = Some(work);
                    self.report.more |= progress;
                    return Ok(());
                }
                if self.unreadable.contains(&seg) || self.condemned.contains(&seg) {
                    break;
                }
                let col = cands[i].1;
                let mut j = i;
                while j < cands.len() && j - i < self.read_step && cands[j].1 == col {
                    j += 1;
                }
                let chunk: Vec<(Hash, Loc)> = cands[i..j]
                    .iter()
                    .filter(|(k, _)| {
                        l.r.has(col)
                            && state.mismatched.get(&(*k, col)) != Some(&seg)
                            && state.unresolved.get(&(*k, col)) != Some(&seg)
                    })
                    .filter_map(|(k, _)| l.idx.locate(k, col).map(|loc| (*k, loc)))
                    .collect();
                i = j;
                let Some(column) = self.fam.column(col) else {
                    continue;
                };
                let n = self.retire_chunk(&l, column, &chunk, state).await?;
                progress |= n > 0;
            }
            self.forget_data();
        }
        Ok(())
    }

    /// Whether either blob of `l` carries a blob GC condemnation that
    /// stands (not rescued since, or a delete in flight). A blob GC of a
    /// binary that cannot read this family (one that started before the
    /// family's feature was recorded) may have condemned the segment as an
    /// orphan: its rows are not deleted while it does. They go once a
    /// collect rescues the segment (its condemnation row stays, answered by
    /// a rescue row), or stay with the segment dropped and sealed again.
    /// Logged once per segment per process.
    async fn condemned(&mut self, l: &Live, state: &mut FamilyState) -> Result<bool> {
        let hashes = [l.r.index, l.r.data];
        let marks = self
            .repo
            .row_meta()
            .get_derived_many(crate::gc::BLOB_CONDEMNED, &hashes)
            .await?;
        let mut stands = false;
        for (h, m) in hashes.iter().zip(&marks) {
            if m.is_some() && self.repo.blob_condemned_now(h).await? {
                stands = true;
                break;
            }
        }
        if !stands {
            return Ok(false);
        }
        self.condemned.insert(l.r.index);
        if state.condemned_warned.insert(l.r.index) {
            tracing::warn!(
                segment = %l.r.index,
                family = self.fam.name,
                "a derived segment's blob is condemned by blob GC; its rows are not retired"
            );
        }
        Ok(true)
    }

    /// The rows to retire, from a listing: each row whose newest holding
    /// segment is ripe, by that segment (in index order, so one data blob
    /// is in memory at a time), sorted by column and key.
    async fn retire_work(
        &mut self,
        ripe: &HashSet<Hash>,
        state: &mut FamilyState,
    ) -> Result<RetireWork> {
        self.list_rows(state).await?;
        let mut rows = self.rows.take().unwrap_or_default();
        self.place_holders(&mut rows);
        let mut by_seg: HashMap<Hash, Vec<(Hash, u8)>> = HashMap::new();
        for (k, r) in &rows {
            for c in self.fam.columns {
                let col = c.id;
                let at = r.holder[slot(col)];
                if r.have & bit(col) == 0 || at == NONE {
                    continue;
                }
                let seg = self.man.live[at as usize].index;
                if !ripe.contains(&seg)
                    || self.unreadable.contains(&seg)
                    || state.mismatched.get(&(*k, col)) == Some(&seg)
                    || state.unresolved.get(&(*k, col)) == Some(&seg)
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
        column: &'static codec::Column,
        chunk: &[(Hash, Loc)],
        state: &mut FamilyState,
    ) -> Result<u64> {
        let (col, kind, family) = (column.id, column.kind, self.fam.name);
        if chunk.is_empty() {
            return Ok(0);
        }
        let started = std::time::Instant::now();
        let ks: Vec<Hash> = chunk.iter().map(|(k, _)| *k).collect();
        let got = self.repo.row_meta().get_derived_many(kind, &ks).await?;
        if self.fam.keys == Keys::Content {
            let read: usize = got.iter().flatten().map(|v| v.len()).sum();
            self.adapt_step(read);
        }
        let mut pairs = Vec::with_capacity(ks.len());
        for ((k, loc), row) in chunk.iter().zip(got) {
            let Some(row) = row else { continue };
            let Some(value) = self.value_at(live, *loc, col).await? else {
                self.mark_unreadable(live.r.index, state);
                break;
            };
            pairs.push((*k, row, value));
        }
        // A patch (`content-v1`) is compared by the set or filter it stands
        // for, its keyframe read as any read would (a row, or a live
        // segment's value).
        let keyframes = self.keyframes_of(&column.equal, &pairs).await?;
        let mut del = Vec::with_capacity(pairs.len());
        let mut unequal = 0;
        for (k, row, value) in pairs {
            match column
                .equal
                .same(&row, &value, &|h| keyframes.get(h).cloned())
            {
                Some(true) => del.push((k, row)),
                // A keyframe is not stored: they are not known equal, so
                // the row stays, and is not compared again while this
                // segment holds it (also when the row's own keyframe is the
                // one missing: residue until a merge rewrites the segment
                // or the process restarts, the row winning meanwhile). If the row resolves alone (whole, or
                // its keyframe found) it is sealed again into a new
                // segment, which then answers for it and lets it go.
                None => {
                    state.unresolved.insert((k, col), live.r.index);
                    let resolves = match column.equal.base(&row) {
                        None => true,
                        Some(b) => keyframes.contains_key(&b),
                    };
                    if resolves {
                        state.reseal.insert((k, col));
                    }
                }
                Some(false) => {
                    unequal += 1;
                    self.report.mismatched += 1;
                    state.mismatched.insert((k, col), live.r.index);
                    metrics::counter!(
                        "leep_derived_segment_mismatch_total",
                        "kind" => kind, "family" => family
                    )
                    .increment(1);
                    tracing::warn!(
                        key = %k,
                        kind,
                        "a {kind} row differs from its derived segment's value; the \
                         row stays and wins (run `leep-server verify --derived {kind}`)"
                    );
                }
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
            (f.record)(self.fam.feature).await?;
            state.feature_recorded = true;
            self.pause("feature-recorded").await;
        }
        // Read after the feature is recorded: binaries that cannot read the
        // family refuse the store from then on, so only a `gc` already
        // under way could condemn later (ADR 0105, section 4).
        if self.condemned(live, state).await? {
            return Ok(unequal);
        }
        gc_lease::check(self.lease)?;
        let n = self
            .repo
            .row_meta()
            .delete_derived_values(kind, &del)
            .await?;
        self.report.retired_rows += n;
        self.pause("retire-deleted").await;
        metrics::counter!(
            "leep_derived_segment_rows_retired_total",
            "kind" => kind, "family" => family
        )
        .increment(n);
        let rest = started.elapsed() * self.opts.rest_factor;
        if !rest.is_zero() {
            tokio::time::sleep(rest).await;
        }
        Ok(n + unequal)
    }
}

impl Pass<'_> {
    /// The keyframes the patches among `pairs` (rows and segment values)
    /// stand on, read through the view: a row, or a live segment's value.
    async fn keyframes_of(
        &self,
        equal: &Equality,
        pairs: &[(Hash, Bytes, Bytes)],
    ) -> Result<HashMap<Hash, Bytes>> {
        let Some(kind) = equal.keyframe_kind() else {
            return Ok(HashMap::new());
        };
        let mut bases: Vec<Hash> = pairs
            .iter()
            .flat_map(|(_, r, v)| [equal.base(r), equal.base(v)])
            .flatten()
            .collect();
        bases.sort_unstable();
        bases.dedup();
        if bases.is_empty() {
            return Ok(HashMap::new());
        }
        let got = self.repo.meta().get_derived_many(kind, &bases).await?;
        Ok(bases
            .into_iter()
            .zip(got)
            .filter_map(|(h, v)| Some((h, v?)))
            .collect())
    }
}

/// Encode `rows` as segments whose data blobs fit in `max_bytes`, halving
/// a page until each fits. The depth is log2 of the page: not a history
/// walk.
fn encode_capped(
    family: u8,
    max_bytes: usize,
    rows: &[SealRow],
    out: &mut Vec<(codec::Encoded, Vec<codec::BadValue>, Vec<Hash>)>,
) -> Result<()> {
    let (enc, bad) = codec::encode(family, rows, codec::nonce())?;
    if enc.seg.data_len as usize > max_bytes && rows.len() > 1 {
        let (a, b) = rows.split_at(rows.len() / 2);
        encode_capped(family, max_bytes, a, out)?;
        return encode_capped(family, max_bytes, b, out);
    }
    out.push((enc, bad, rows.iter().map(|r| r.key).collect()));
    Ok(())
}

/// Which of `ids` are stored changesets, asked of the changesets
/// themselves: a segment value outlives its changeset (the changeset sweep,
/// ADR 0059), and once `generation` is sealed a generation proves nothing.
async fn stored(repo: &Repo, ids: &[Hash]) -> Result<Vec<bool>> {
    let cs: Vec<ChangesetId> = ids.iter().map(|h| ChangesetId(*h)).collect();
    match repo.row_meta().changesets_stored_at(&cs).await {
        Ok(at) if at.len() == cs.len() => Ok(at.iter().map(Option::is_some).collect()),
        Ok(_) => Err(Error::Storage("changesets_stored_at answered short".into())),
        // A store that cannot say when: read the changesets.
        Err(Error::Invalid(_)) => Ok(repo
            .row_meta()
            .get_changesets(&cs)
            .await?
            .iter()
            .map(Option::is_some)
            .collect()),
        Err(e) => Err(e),
    }
}

/// The generations of the stored changesets among `ids`, which order a
/// segment; `None` for a changeset that is no longer stored (so a seal
/// skips it and a merge leaves it out). The generations are read through
/// the view, so that once `generation` itself is sealed and its rows
/// retired (family `changeset-v2`) they come from the segments, and a
/// missing one is derived. Any order decodes correctly; this one makes
/// references small.
async fn generations(repo: &Repo, ids: &[Hash], batch: usize) -> Result<Vec<Option<u64>>> {
    let mut out = Vec::with_capacity(ids.len());
    for chunk in ids.chunks(batch.max(1)) {
        let here = stored(repo, chunk).await?;
        let rows = repo
            .meta()
            .get_derived_many(Generation::NAME, chunk)
            .await?;
        for ((id, row), here) in chunk.iter().zip(rows).zip(here) {
            if !here {
                out.push(None);
                continue;
            }
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
