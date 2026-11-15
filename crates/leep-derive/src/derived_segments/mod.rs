//! Derived segments (ADR 0104): `skiplist` and `history-tree-v4` rows
//! sealed into immutable blobs in the blob store, named by one manifest
//! row swapped only by compare-and-swap, exactly the pattern of ADR 0103.
//!
//! - [`codec`]: the index blob, the data blob and the manifest.
//! - [`view`]: the read path, a `MetaStore` that `Repo::new` installs.
//! - [`compactor`]: the background pass that seals, merges and (in mode
//!   `retire`) deletes rows a live segment holds.
//!
//! Invariants: kind names, keys and values do not change (a segment gives
//! back the value a row read would); a segment only holds values read from
//! rows; every failure reads as "row missing", which derives again.

pub mod codec;
pub mod compactor;
pub mod view;

#[cfg(test)]
mod tests;

use std::collections::HashSet;
use std::time::Duration;

use bytes::Bytes;
use leep_core::{Error, Hash, LeaseKey, Result};

pub use codec::{Manifest, SegmentRef, COL_HISTORY_TREE, COL_SKIPLIST};
pub use compactor::{compact_pass, CompactOptions, CompactState, FeatureHook, Mode, PassReport};
pub use view::{ManifestRead, SegmentView};

use crate::gc_lease::{self, GcLease};
use crate::Repo;

/// Derived kind of the manifest rows, one per family.
pub const MANIFEST_KIND: &str = "derived-segments-v1";

/// The manifest key of family `changeset-v1`.
pub fn manifest_key() -> Hash {
    Hash::of_object(
        "derived-segment-family",
        codec::FAMILY_CHANGESET_NAME.as_bytes(),
    )
}

/// Store feature recorded before the first row is deleted (mode
/// `retire`): a binary that predates segments would derive whole histories
/// again without the rows, and its `gc` would delete live segments.
pub const FEATURE: &str = "derived-segments";

/// A row is deleted only once its segment has been live this long, as seen
/// by the deleting process: every reader re-reads the manifest at least
/// every [`view::REFRESH`], so none still misses the segment.
pub const RETIRE_AFTER: Duration = Duration::from_secs(600);

/// The store-wide lease the compactor holds, so one server at a time runs
/// it; `repair` holds it too while it changes these kinds.
pub fn lease_key() -> LeaseKey {
    LeaseKey {
        scope: "maintenance".into(),
        repo_id: 0,
        name: "derived-segments".into(),
        shard: 0,
    }
}

/// The manifest row as stored (from the rows, never a view), and what it
/// says.
pub async fn read_manifest(repo: &Repo) -> Result<(Option<Bytes>, ManifestRead)> {
    let raw = repo
        .row_meta()
        .get_derived(MANIFEST_KIND, &manifest_key())
        .await?;
    let read = ManifestRead::of(raw.as_deref());
    Ok((raw, read))
}

/// Write `next` iff the row still holds `expected` (`None`: no row), under
/// a lease checked first. A holder that lost its lease and wakes after
/// another wrote the row fails here with `Conflict` and stops.
pub(crate) async fn swap_manifest(
    repo: &Repo,
    lease: &Option<GcLease>,
    expected: Option<&[u8]>,
    next: &Manifest,
) -> Result<Bytes> {
    gc_lease::check(lease)?;
    let bytes = next.encode();
    if !repo
        .row_meta()
        .cas_derived(MANIFEST_KIND, &manifest_key(), expected, bytes.clone())
        .await?
    {
        return Err(Error::Conflict(format!(
            "the {MANIFEST_KIND} row changed under this pass (another holder wrote it after \
             this one lost its lease); stopping"
        )));
    }
    // Readers of this process see it at once.
    repo.segments().apply(Some(bytes.clone())).await?;
    Ok(bytes)
}

/// What blob GC must know: the blobs of live segments (never condemned)
/// and of retired ones (collected like orphans, reported apart). A manifest
/// that does not decode is an error: GC cannot tell live segments from
/// orphans then, and after `retire` they hold the only copy of their rows.
pub(crate) struct GcView {
    pub live: HashSet<Hash>,
    pub retired: HashSet<Hash>,
    pub segments: u64,
}

pub(crate) async fn gc_view(repo: &Repo) -> Result<GcView> {
    match read_manifest(repo).await?.1 {
        ManifestRead::Absent => Ok(GcView {
            live: HashSet::new(),
            retired: HashSet::new(),
            segments: 0,
        }),
        ManifestRead::Read(m) => Ok(GcView {
            live: m.live_blobs().collect(),
            retired: m.retired.iter().copied().collect(),
            segments: m.live.len() as u64,
        }),
        ManifestRead::Damaged => Err(Error::Other(format!(
            "the {MANIFEST_KIND} row does not decode, so garbage collection cannot tell live \
             derived segments from orphans; see \"Derived segments\" in RUNBOOKS.md"
        ))),
    }
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

/// `repair drop` of `skiplist` or `history-tree-v4`: stop every segment
/// answering for the columns `cols` (cleared in the manifest by swap; a
/// segment left with no column is retired), so nothing answers for the
/// dropped rows and they derive again. A manifest that does not decode is
/// replaced by an empty one: the drop already asks for everything to derive
/// again. Call with [`lease_key`]'s lease held across the row delete too,
/// so no pass seals the rows being dropped.
pub(crate) async fn drop_columns(repo: &Repo, lease: &Option<GcLease>, cols: &[u8]) -> Result<u64> {
    let (raw, read) = read_manifest(repo).await?;
    let mut m = match read {
        ManifestRead::Absent => return Ok(0),
        ManifestRead::Read(m) => m,
        ManifestRead::Damaged => Manifest::new(codec::FAMILY_CHANGESET),
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
    if changed > 0 || matches!(ManifestRead::of(raw.as_deref()), ManifestRead::Damaged) {
        swap_manifest(repo, lease, raw.as_deref(), &m).await?;
    }
    Ok(changed)
}
