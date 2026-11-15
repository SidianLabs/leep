//! Derived segments (ADR 0104, 0105): derived rows that are pure functions
//! of history sealed into immutable blobs in the blob store, named by one
//! manifest row per family swapped only by compare-and-swap, exactly the
//! pattern of ADR 0103.
//!
//! - [`codec`]: the families and their columns, the index blob, the data
//!   blob and the manifest.
//! - [`view`]: the read path, a `MetaStore` that `Repo::new` installs.
//! - [`compactor`]: the background pass that seals, merges and (in mode
//!   `retire`) deletes rows a live segment holds, family by family.
//!
//! Families: `changeset-v1` (`skiplist`, `history-tree-v4`; ADR 0104) and
//! `changeset-v2` (`generation`, `last_merge-v1`, `root_tree`,
//! `log-filter-v1`; ADR 0105).
//!
//! Invariants: kind names, keys and values do not change (a segment gives
//! back the value a row read would); a segment only holds values read from
//! rows; every failure reads as "row missing", which derives again.

pub mod codec;
pub mod compactor;
pub mod view;

#[cfg(test)]
mod family_tests;
#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use leep_core::{Error, Hash, LeaseKey, Result};

pub use codec::{
    Family, Manifest, SegmentRef, COL_GENERATION, COL_HISTORY_TREE, COL_LAST_MERGE, COL_LOG_FILTER,
    COL_ROOT_TREE, COL_SKIPLIST, FAMILIES, FAMILY_CHANGESET, FAMILY_CHANGESET_V2,
};
pub use compactor::{
    compact_pass, CompactOptions, CompactState, FeatureHook, FeatureProbe, Features, Mode,
    PassReport, PauseHook,
};
pub use view::{ManifestRead, SegmentView, Segments};

use crate::gc_lease::{self, GcLease};
use crate::Repo;

/// Derived kind of the manifest rows, one per family.
pub const MANIFEST_KIND: &str = "derived-segments-v1";

/// The manifest key of `family`.
pub fn manifest_key(family: u8) -> Hash {
    codec::family(family)
        .expect("a family this binary knows")
        .manifest_key()
}

/// Store feature recorded before the first `changeset-v1` row is deleted
/// (mode `retire`): a binary that predates segments would derive whole
/// histories again without the rows, and its `gc` would delete live
/// segments.
pub const FEATURE: &str = "derived-segments";

/// Store feature recorded before the first row of a family added by ADR
/// 0105 (`changeset-v2`) is deleted: an ADR 0104 binary reads only
/// `changeset-v1`, so without the rows it would derive those kinds again
/// from the root, and its `gc` would delete the new family's live
/// segments as orphans.
pub const FEATURE_2: &str = "derived-segments-2";

/// A row is deleted only once its segment has been live this long, as seen
/// by the deleting process: every reader re-reads the manifest at least
/// every [`view::REFRESH`], so none still misses the segment.
pub const RETIRE_AFTER: Duration = Duration::from_secs(600);

/// The store-wide lease the compactor holds, so one server at a time runs
/// it; `repair` holds it too while it changes these kinds. One lease covers
/// every family; each family's manifest is swapped on its own.
pub fn lease_key() -> LeaseKey {
    LeaseKey {
        scope: "maintenance".into(),
        repo_id: 0,
        name: "derived-segments".into(),
        shard: 0,
    }
}

/// `family`'s manifest row as stored (from the rows, never a view), and
/// what it says.
pub async fn read_manifest(repo: &Repo, family: u8) -> Result<(Option<Bytes>, ManifestRead)> {
    let raw = repo
        .row_meta()
        .get_derived(MANIFEST_KIND, &manifest_key(family))
        .await?;
    let read = ManifestRead::of(family, raw.as_deref());
    Ok((raw, read))
}

/// Read every family's manifest and load every live segment's index into
/// `repo`'s view: a server does this in the background at start, so its
/// first miss does not wait for every index blob. Returns the live
/// segments in use.
pub async fn warm(repo: &Repo) -> Result<usize> {
    repo.segments().refresh(&**repo.row_meta()).await?;
    Ok(repo
        .segments()
        .views()
        .map(|v| v.segments().len())
        .sum::<usize>())
}

/// Write `next` (of `next.family`) iff the row still holds `expected`
/// (`None`: no row), under a lease checked first. A holder that lost its
/// lease and wakes after another wrote the row fails here with `Conflict`
/// and stops.
pub(crate) async fn swap_manifest(
    repo: &Repo,
    lease: &Option<GcLease>,
    expected: Option<&[u8]>,
    next: &mut Manifest,
) -> Result<Bytes> {
    gc_lease::check(lease)?;
    next.seq = next_seq(next.seq);
    let bytes = next.encode();
    if !repo
        .row_meta()
        .cas_derived(
            MANIFEST_KIND,
            &manifest_key(next.family),
            expected,
            bytes.clone(),
        )
        .await?
    {
        return Err(Error::Conflict(format!(
            "the {MANIFEST_KIND} row changed under this pass (another holder wrote it after \
             this one lost its lease); stopping"
        )));
    }
    // Readers of this process see it at once.
    repo.segments()
        .family(next.family)
        .apply(Some(bytes.clone()))
        .await?;
    Ok(bytes)
}

/// The `seq` of a manifest written over one with `prev`: one more, or the
/// clock in microseconds if that is more, so a manifest written over a
/// damaged row (whose `seq` is unknown) still sorts after what readers hold.
fn next_seq(prev: u64) -> u64 {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |d| d.as_micros() as u64);
    prev.saturating_add(1).max(now)
}

/// What blob GC must know: the blobs of live segments (never condemned)
/// and of retired ones (collected like orphans, reported apart), over
/// every family. A manifest that does not decode, one a newer binary
/// wrote, or a manifest row of a family this binary does not know is an
/// error: GC cannot tell live segments from orphans then, and after
/// `retire` they hold the only copy of their rows.
pub(crate) struct GcView {
    pub live: HashSet<Hash>,
    pub retired: HashSet<Hash>,
    pub segments: u64,
}

pub(crate) async fn gc_view(repo: &Repo) -> Result<GcView> {
    let mut view = GcView {
        live: HashSet::new(),
        retired: HashSet::new(),
        segments: 0,
    };
    // Every manifest row there is (one per family), so one a newer binary
    // wrote for a family this one does not know stops GC instead of leaving
    // its live segments to be collected as orphans.
    let mut rows = Vec::new();
    let mut after = None;
    loop {
        let page = repo
            .row_meta()
            .list_derived(MANIFEST_KIND, after, 100)
            .await?;
        let short = page.len() < 100;
        let Some((last, _)) = page.last() else {
            break;
        };
        after = Some(*last);
        rows.extend(page);
        if short {
            break;
        }
    }
    for (key, raw) in rows {
        let Some(fam) = FAMILIES.iter().find(|f| f.manifest_key() == key) else {
            return Err(Error::Other(format!(
                "a {MANIFEST_KIND} row of a derived segment family this leep-server does not \
                 know ({key}) was written by a newer one, so this garbage collection cannot \
                 tell its live segments from orphans; run the newer gc"
            )));
        };
        match ManifestRead::of(fam.id, Some(&raw)) {
            ManifestRead::Absent => {}
            ManifestRead::Read(m) => {
                view.live.extend(m.live_blobs());
                view.retired.extend(m.retired.iter().copied());
                view.segments += m.live.len() as u64;
            }
            ManifestRead::Damaged => {
                return Err(Error::Other(format!(
                    "the {MANIFEST_KIND} row of family {} does not decode, so garbage \
                     collection cannot tell live derived segments from orphans; see \
                     LeepDerivedSegmentPassesFailing in RUNBOOKS.md",
                    fam.name
                )))
            }
            ManifestRead::Newer => {
                return Err(Error::Other(format!(
                    "the {MANIFEST_KIND} row of family {} was written by a newer leep-server, \
                     so this garbage collection cannot tell live derived segments from \
                     orphans; run the newer gc",
                    fam.name
                )))
            }
        }
    }
    // A blob live in one family is never collected as another's retired.
    let live = view.live.clone();
    view.retired.retain(|h| !live.contains(h));
    Ok(view)
}

/// Take [`lease_key`]'s lease for an operator command, waiting out a
/// compactor pass (they are short) for up to about two minutes.
pub(crate) async fn acquire_for(repo: &Repo, job: &str) -> Result<Option<GcLease>> {
    let mut tries = 0;
    loop {
        match GcLease::acquire_key(repo.row_meta(), lease_key(), job).await {
            Err(Error::Conflict(m)) if tries < 60 => {
                tries += 1;
                tracing::info!("waiting for the derived-segments lease: {m}");
                tokio::time::sleep(Duration::from_secs(2)).await;
            }
            other => return other,
        }
    }
}

/// `repair drop` of kinds a family holds: stop every segment of `family`
/// answering for the columns `cols` (cleared in the manifest by swap; a
/// segment left with no column is retired), so nothing answers for the
/// dropped rows and they derive again. A column this binary does not know
/// keeps its bit (ADR 0105: never dropped). A manifest that does not decode
/// is replaced by an empty one, which stops the segments answering for
/// every kind of the family, so it is refused unless `cols` names all of
/// them (their rows are dropped too, and derive again). Call with
/// [`lease_key`]'s lease held across the row delete too, so no pass seals
/// the rows being dropped.
pub(crate) async fn drop_columns(
    repo: &Repo,
    lease: &Option<GcLease>,
    family: u8,
    cols: &[u8],
) -> Result<u64> {
    let (raw, read) = read_manifest(repo, family).await?;
    let mut m = match read {
        ManifestRead::Absent => return Ok(0),
        ManifestRead::Read(m) => m,
        ManifestRead::Damaged => {
            let fam = codec::family(family).expect("a family this binary knows");
            let missing: Vec<&str> = fam
                .columns
                .iter()
                .filter(|c| !cols.contains(&c.id))
                .map(|c| c.kind)
                .collect();
            if !missing.is_empty() {
                return Err(Error::Invalid(format!(
                    "the {MANIFEST_KIND} row of family {} does not decode; replacing it drops \
                     every kind it holds, so drop them together (add --kind {})",
                    fam.name,
                    missing.join(" --kind ")
                )));
            }
            Manifest::new(family)
        }
        ManifestRead::Newer => {
            return Err(Error::Invalid(format!(
                "the {MANIFEST_KIND} row was written by a newer leep-server; run its repair"
            )))
        }
    };
    let mask = cols.iter().fold(0u64, |m, c| m | 1 << c);
    let mut changed = 0;
    let mut gone = Vec::new();
    for s in &mut m.live {
        if s.columns & mask != 0 {
            s.columns &= !mask;
            changed += 1;
            if s.columns == 0 {
                gone.push(*s);
            }
        }
    }
    m.live.retain(|s| s.columns != 0);
    m.retire(gone);
    if changed > 0
        || matches!(
            ManifestRead::of(family, raw.as_deref()),
            ManifestRead::Damaged
        )
    {
        swap_manifest(repo, lease, raw.as_deref(), &mut m).await?;
    }
    Ok(changed)
}
