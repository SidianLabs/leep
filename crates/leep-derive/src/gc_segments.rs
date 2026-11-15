//! Garbage collection marks as sorted segments in the blob store (ADR 0103).
//!
//! A mark (ADR 0033) records, for every object it walked, that a stored
//! changeset references it and that everything below it is recorded too.
//! Instead of one `gc-mark-v1` row per object (about 167 bytes in Postgres to
//! say one bit), a run writes the hashes it marked as an immutable *segment*:
//! a blob-store object holding sorted 32-byte hashes. A *manifest*, one
//! derived row, names the live segments in the order they were published.
//!
//! Closure: a run emits marks in post-order, so every prefix of what it
//! emitted is closed (each mark's closure is in the prefix, or was recorded
//! before the run). A segment holds such a prefix and is published only once
//! it is durable, by a manifest write that appends it. So every prefix of the
//! manifest's list (plus the `gc-mark-v1` rows until they are retired) is
//! closed, and a reader trusts the longest prefix it can read in full: a
//! segment it cannot read ends what it trusts, and costs only more walking.
//! Merging adjacent segments keeps every remaining prefix closed.
//!
//! A segment is never consulted approximately: lookups are exact binary
//! searches. A false "marked" tree node would stop the walk and condemn its
//! subtree.

use std::collections::HashSet;

use bytes::Bytes;
use leep_core::{Error, Hash, Result};

use crate::gc::GC_MARK;
use crate::Repo;

/// Derived kind of the manifest: one row, keyed by [`MANIFEST_KEY`].
pub const GC_MARK_SEGMENTS: &str = "gc-mark-segments-v1";

/// The object kind whose preimage prefix starts every segment, so no
/// uploaded blob can take a segment's address (ADR 0041).
pub const SEGMENT_KIND: &str = "gc-mark-segment-v1";

/// The manifest row's key.
pub const MANIFEST_KEY: Hash = Hash([0; 32]);

/// The store feature recorded before the `gc-mark-v1` rows are deleted
/// (ADR 0055): an older `gc` would trust what is left of them half way.
pub const MARK_ROWS_RETIRED_FEATURE: &str = "gc-mark-segments";

/// First byte of a segment.
const SEGMENT_VERSION: u8 = 1;
/// First byte of a manifest.
const MANIFEST_VERSION: u8 = 1;
/// Manifest flag: the segments are closed without the `gc-mark-v1` rows,
/// which readers then ignore.
const FLAG_ROWS_RETIRED: u8 = 1;

/// Marks a run holds before it publishes them as a segment: a run killed
/// later keeps what it published (8 MiB of hashes). Tests publish every few
/// marks, so every test runs checkpoints and merges.
pub(crate) const CHECKPOINT_MARKS: usize = if cfg!(test) { 7 } else { 1 << 18 };

/// Marks a merge may produce at most (4 Mi marks, 128 MiB): beyond this,
/// segments stay apart, so no merge reads, sorts and rewrites the whole
/// record of a large store at once. Tests use a small cap so they reach it.
pub(crate) const MAX_SEGMENT_MARKS: u64 = if cfg!(test) { 20 } else { 1 << 22 };

/// Overrides of [`CHECKPOINT_MARKS`] and [`MAX_SEGMENT_MARKS`] (0: none).
static LIMITS: [std::sync::atomic::AtomicU64; 2] = [
    std::sync::atomic::AtomicU64::new(0),
    std::sync::atomic::AtomicU64::new(0),
];

/// For integration tests only: make every run in this process checkpoint
/// every `checkpoint` marks and cap merges at `max_segment` marks, so that
/// checkpoints and merges run against real backends with small histories.
#[doc(hidden)]
pub fn set_limits_for_tests(checkpoint: usize, max_segment: u64) {
    use std::sync::atomic::Ordering::Relaxed;
    LIMITS[0].store(checkpoint as u64, Relaxed);
    LIMITS[1].store(max_segment, Relaxed);
}

/// Marks a run holds before it publishes them.
pub(crate) fn checkpoint_marks() -> usize {
    match LIMITS[0].load(std::sync::atomic::Ordering::Relaxed) {
        0 => CHECKPOINT_MARKS,
        n => n as usize,
    }
}

/// Marks a merge may produce at most.
fn max_segment_marks() -> u64 {
    match LIMITS[1].load(std::sync::atomic::Ordering::Relaxed) {
        0 => MAX_SEGMENT_MARKS,
        n => n,
    }
}

/// Retired segments the manifest remembers, for reporting. Beyond this the
/// oldest are forgotten and collected as plain orphans.
const MAX_RETIRED: usize = 1024;

/// One live segment: its address and how many hashes it holds.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SegmentRef {
    pub hash: Hash,
    pub marks: u64,
}

/// The manifest row's content.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Manifest {
    /// Live segments, oldest first. Every prefix is closed.
    pub live: Vec<SegmentRef>,
    /// Whether the segments are closed on their own, so the `gc-mark-v1`
    /// rows are ignored (and may be deleted once the feature is recorded).
    pub rows_retired: bool,
    /// Segments no longer live and perhaps still stored, oldest first. Blob
    /// GC collects them like orphans; this list only reports them apart.
    pub retired: Vec<Hash>,
}

impl Manifest {
    /// `version, flags, u32 live count, (hash, u64 marks)*, u32 retired
    /// count, hash*, BLAKE3 of everything before`. Integers big-endian.
    pub fn encode(&self) -> Bytes {
        let mut out =
            Vec::with_capacity(2 + 4 + self.live.len() * 40 + 4 + self.retired.len() * 32 + 32);
        out.push(MANIFEST_VERSION);
        out.push(if self.rows_retired {
            FLAG_ROWS_RETIRED
        } else {
            0
        });
        out.extend_from_slice(&(self.live.len() as u32).to_be_bytes());
        for s in &self.live {
            out.extend_from_slice(&s.hash.0);
            out.extend_from_slice(&s.marks.to_be_bytes());
        }
        out.extend_from_slice(&(self.retired.len() as u32).to_be_bytes());
        for h in &self.retired {
            out.extend_from_slice(&h.0);
        }
        let sum = Hash::of_blob(&out);
        out.extend_from_slice(&sum.0);
        Bytes::from(out)
    }

    pub fn decode(v: &[u8]) -> Result<Manifest> {
        let bad = |what: &str| Error::Other(format!("corrupt {GC_MARK_SEGMENTS} row: {what}"));
        if v.len() < 2 + 4 + 4 + 32 {
            return Err(bad("short"));
        }
        let (body, sum) = v.split_at(v.len() - 32);
        if Hash::of_blob(body).0 != sum {
            return Err(bad("checksum"));
        }
        if body[0] != MANIFEST_VERSION {
            return Err(bad("unknown version"));
        }
        if body[1] & !FLAG_ROWS_RETIRED != 0 {
            return Err(bad("unknown flags"));
        }
        let rows_retired = body[1] & FLAG_ROWS_RETIRED != 0;
        let mut at = 2;
        let mut take = |n: usize| -> Result<&[u8]> {
            let s = body.get(at..at + n).ok_or_else(|| bad("truncated"))?;
            at += n;
            Ok(s)
        };
        let n = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
        let mut live = Vec::with_capacity(n.min(4096));
        for _ in 0..n {
            let hash = Hash(take(32)?.try_into().unwrap());
            let marks = u64::from_be_bytes(take(8)?.try_into().unwrap());
            live.push(SegmentRef { hash, marks });
        }
        let r = u32::from_be_bytes(take(4)?.try_into().unwrap()) as usize;
        let mut retired = Vec::with_capacity(r.min(4096));
        for _ in 0..r {
            retired.push(Hash(take(32)?.try_into().unwrap()));
        }
        if at != body.len() {
            return Err(bad("trailing bytes"));
        }
        Ok(Manifest {
            live,
            rows_retired,
            retired,
        })
    }

    fn retire(&mut self, hashes: impl IntoIterator<Item = Hash>) {
        for h in hashes {
            if !self.retired.contains(&h) {
                self.retired.push(h);
            }
        }
        let live: HashSet<Hash> = self.live.iter().map(|s| s.hash).collect();
        self.retired.retain(|h| !live.contains(h));
        if self.retired.len() > MAX_RETIRED {
            let extra = self.retired.len() - MAX_RETIRED;
            self.retired.drain(..extra);
        }
    }
}

/// Encode sorted, distinct hashes as a segment: the object preimage prefix
/// `leep:gc-mark-segment-v1\0`, one version byte, an 8-byte nonce, then the
/// hashes. Its address is `Hash::of_blob` of those bytes, which is
/// `Hash::of_object(SEGMENT_KIND, ..)` of what follows the prefix: to the
/// blob store it is a blob like any other (packed, copied by backups, read by
/// every version), and no upload can take its address, since uploads that
/// start with an object prefix are refused (ADR 0041). The nonce makes every
/// written segment a new object, so publishing the same marks again never
/// lands on a stored copy that reads damaged (a content-addressed put keeps
/// what is there). Returns its address and bytes.
pub fn encode_segment(sorted: &[Hash], nonce: u64) -> (Hash, Bytes) {
    debug_assert!(sorted.windows(2).all(|w| w[0] < w[1]));
    let mut out = Vec::with_capacity(SEGMENT_HEADER + sorted.len() * 32);
    out.extend_from_slice(&segment_prefix());
    out.push(SEGMENT_VERSION);
    out.extend_from_slice(&nonce.to_be_bytes());
    for h in sorted {
        out.extend_from_slice(&h.0);
    }
    (Hash::of_blob(&out), Bytes::from(out))
}

/// `leep:gc-mark-segment-v1\0`.
fn segment_prefix() -> Vec<u8> {
    let mut p = leep_core::hash::OBJECT_DOMAIN.to_vec();
    p.extend_from_slice(SEGMENT_KIND.as_bytes());
    p.push(0);
    p
}

/// Bytes before a segment's hashes: the prefix, the version, the nonce.
pub const SEGMENT_HEADER: usize = 5 + SEGMENT_KIND.len() + 1 + 1 + 8;

/// A fresh nonce: process-random keys mixed with the time.
fn nonce() -> u64 {
    use std::hash::{BuildHasher, Hasher};
    let mut h = std::collections::hash_map::RandomState::new().build_hasher();
    h.write_u128(
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0),
    );
    h.finish()
}

/// Decode a segment read from `hash`, checking its address, prefix, version
/// and that its hashes are strictly ascending.
pub fn decode_segment(hash: &Hash, data: &[u8]) -> Result<Vec<Hash>> {
    let bad = |what: &str| Error::Other(format!("corrupt gc mark segment {hash}: {what}"));
    if Hash::of_blob(data) != *hash {
        return Err(bad("address mismatch"));
    }
    let prefix = segment_prefix();
    if !data.starts_with(&prefix) || data.get(prefix.len()) != Some(&SEGMENT_VERSION) {
        return Err(bad("unknown kind or version"));
    }
    let Some(rest) = data.get(SEGMENT_HEADER..) else {
        return Err(bad("short"));
    };
    if rest.len() % 32 != 0 {
        return Err(bad("length"));
    }
    let hashes: Vec<Hash> = rest
        .chunks_exact(32)
        .map(|c| Hash(c.try_into().unwrap()))
        .collect();
    if !hashes.windows(2).all(|w| w[0] < w[1]) {
        return Err(bad("not sorted"));
    }
    Ok(hashes)
}

/// Whether `data` stored at `hash` is a segment.
pub fn is_segment(hash: &Hash, data: &[u8]) -> bool {
    data.starts_with(&segment_prefix()) && Hash::of_blob(data) == *hash
}

/// What reading the manifest row found.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ManifestRead {
    /// No row: no segment was ever published, the rows are not retired.
    Absent,
    Read(Manifest),
    /// The row does not decode. Nothing about the record can be trusted:
    /// not the segments, and not the rows, which may be half deleted.
    Damaged,
}

pub async fn read_manifest(repo: &Repo) -> Result<ManifestRead> {
    Ok(read_manifest_raw(repo).await?.1)
}

/// [`read_manifest`], with the row's bytes: a later write swaps from them.
async fn read_manifest_raw(repo: &Repo) -> Result<(Option<Bytes>, ManifestRead)> {
    let raw = repo
        .meta()
        .get_derived(GC_MARK_SEGMENTS, &MANIFEST_KEY)
        .await?;
    let read = match &raw {
        None => ManifestRead::Absent,
        Some(v) => match Manifest::decode(v) {
            Ok(m) => ManifestRead::Read(m),
            Err(_) => ManifestRead::Damaged,
        },
    };
    Ok((raw, read))
}

/// Write `next` as the manifest iff the row is still `expected` (the bytes
/// the writer last read or wrote; `None` = no row). A writer that loses its
/// lease and wakes up after another holder wrote the row then fails here
/// instead of putting back a record that, say, still counts rows that were
/// deleted since (ADR 0103 section 8). Returns the bytes written.
async fn swap_manifest(
    repo: &Repo,
    lease: &Option<crate::gc_lease::GcLease>,
    expected: Option<&[u8]>,
    next: &Manifest,
) -> Result<Bytes> {
    crate::gc_lease::check(lease)?;
    let bytes = next.encode();
    if !repo
        .meta()
        .cas_derived(GC_MARK_SEGMENTS, &MANIFEST_KEY, expected, bytes.clone())
        .await?
    {
        return Err(Error::Conflict(format!(
            "the {GC_MARK_SEGMENTS} row changed under this run (another garbage \
             collection wrote it after this one lost its lease); this run stopped, \
             run it again"
        )));
    }
    Ok(bytes)
}

/// Merge sorted, distinct lists into one.
fn merge(a: &[Hash], b: &[Hash]) -> Vec<Hash> {
    let mut out = Vec::with_capacity(a.len() + b.len());
    let (mut i, mut j) = (0, 0);
    while i < a.len() && j < b.len() {
        match a[i].cmp(&b[j]) {
            std::cmp::Ordering::Less => {
                out.push(a[i]);
                i += 1;
            }
            std::cmp::Ordering::Greater => {
                out.push(b[j]);
                j += 1;
            }
            std::cmp::Ordering::Equal => {
                out.push(a[i]);
                i += 1;
                j += 1;
            }
        }
    }
    out.extend_from_slice(&a[i..]);
    out.extend_from_slice(&b[j..]);
    out
}

/// Statistics of one run's record, for the reports.
#[derive(Clone, Debug, Default)]
pub(crate) struct SegmentStats {
    /// Live segments this run trusted.
    pub trusted: u64,
    /// Live segments the manifest named that this run could not read.
    pub unreadable: u64,
    pub published: u64,
    pub merged: u64,
}

/// One run's view of the record: what it trusts, and how it publishes what
/// it marks.
pub(crate) struct MarkRecord {
    /// The manifest as stored now (after this run's writes), or `None` when
    /// it did not decode and this run has not replaced it yet.
    stored: Option<Manifest>,
    /// The manifest row's bytes as this run last read or wrote them (`None`:
    /// no row). Every write swaps from exactly these.
    raw: Option<Bytes>,
    /// The segments this run trusts and the ones it published, with their
    /// hashes, in manifest order. When `replace`, only this run's own, which
    /// no manifest names until [`finish`](Self::finish).
    chain: Vec<(SegmentRef, Vec<Hash>)>,
    /// Segments a replacing run wrote and then merged away before its
    /// manifest: never named, retired by that manifest.
    unpublished_gone: Vec<Hash>,
    /// Whether the rows of `gc-mark-v1` count as marks.
    pub(crate) rows: bool,
    /// Whether this run ignores the earlier record and its manifest
    /// replaces the stored one: a full mark, or a damaged manifest.
    replace: bool,
    /// Whether this run writes at all (not a dry run).
    write: bool,
    pub(crate) stats: SegmentStats,
}

impl MarkRecord {
    /// Read the manifest, and unless `trust` is false, every live segment
    /// in order up to the first that cannot be read.
    pub(crate) async fn load(repo: &Repo, trust: bool, write: bool) -> Result<MarkRecord> {
        let (raw, read) = read_manifest_raw(repo).await?;
        let (stored, damaged) = match read {
            ManifestRead::Absent => (Some(Manifest::default()), false),
            ManifestRead::Read(m) => (Some(m), false),
            ManifestRead::Damaged => (None, true),
        };
        let replace = !trust || damaged;
        let mut rec = MarkRecord {
            rows: !replace && stored.as_ref().is_some_and(|m| !m.rows_retired),
            stored,
            raw,
            chain: Vec::new(),
            unpublished_gone: Vec::new(),
            replace,
            write,
            stats: SegmentStats::default(),
        };
        if replace {
            return Ok(rec);
        }
        let live = rec
            .stored
            .as_ref()
            .map(|m| m.live.clone())
            .unwrap_or_default();
        for (i, s) in live.iter().enumerate() {
            // A segment that is gone, that the store reports damaged, or
            // that does not decode ends the trusted prefix. Any other error
            // (transient: a lost connection) aborts the run, like every
            // other read, rather than throwing the record away.
            let hashes = match repo.blobs().get(&s.hash).await {
                Ok(Some(data)) => match decode_segment(&s.hash, &data) {
                    Ok(h) => Some(h),
                    Err(e) => {
                        tracing::warn!(segment = %s.hash, "gc mark segment not trusted: {e}");
                        None
                    }
                },
                Ok(None) => {
                    tracing::warn!(
                        segment = %s.hash,
                        "gc mark segment named by the manifest is not stored; \
                         walking what it and the segments after it recorded"
                    );
                    None
                }
                Err(e) if crate::integrity::problems::damage_of(&e).is_some() => {
                    tracing::warn!(segment = %s.hash, "gc mark segment reads damaged: {e}");
                    None
                }
                Err(e) => return Err(e),
            };
            match hashes {
                Some(h) => {
                    rec.chain.push((
                        SegmentRef {
                            hash: s.hash,
                            marks: h.len() as u64,
                        },
                        h,
                    ));
                    rec.stats.trusted += 1;
                }
                None => {
                    rec.stats.unreadable = (live.len() - i) as u64;
                    break;
                }
            }
        }
        Ok(rec)
    }

    /// Whether this run consults what earlier runs recorded.
    pub(crate) fn trusts_earlier(&self) -> bool {
        !self.replace
    }

    /// Whether an earlier run's segment records `h`. Exact.
    pub(crate) fn contains(&self, h: &Hash) -> bool {
        self.chain.iter().any(|(_, s)| s.binary_search(h).is_ok())
    }

    /// The segments the stored manifest names as live: blob GC keeps them.
    pub(crate) fn live(&self) -> HashSet<Hash> {
        self.stored
            .as_ref()
            .map(|m| m.live.iter().map(|s| s.hash).collect())
            .unwrap_or_default()
    }

    /// Retired segments the stored manifest still remembers.
    pub(crate) fn retired(&self) -> HashSet<Hash> {
        self.stored
            .as_ref()
            .map(|m| m.retired.iter().copied().collect())
            .unwrap_or_default()
    }

    pub(crate) fn live_count(&self) -> u64 {
        self.stored.as_ref().map_or(0, |m| m.live.len() as u64)
    }

    /// Publish `marks` (a post-order prefix of what this run emitted since
    /// its last publish) as a segment, then merge the newest segments while
    /// the newest is at least half the size of the one before it and the
    /// merge stays within [`MAX_SEGMENT_MARKS`]. A replacing run only writes
    /// its segments here: its manifest names them in [`finish`](Self::finish),
    /// so the record it replaces stands until then.
    pub(crate) async fn publish(
        &mut self,
        repo: &Repo,
        lease: &Option<crate::gc_lease::GcLease>,
        mut marks: Vec<Hash>,
    ) -> Result<()> {
        if !self.write || marks.is_empty() {
            return Ok(());
        }
        marks.sort_unstable();
        marks.dedup();
        let seg = write_segment(repo, &marks).await?;
        self.stats.published += 1;
        self.chain.push((seg, marks));
        let mut gone = Vec::new();
        while self.chain.len() >= 2 {
            let n = self.chain.len();
            let (a, b) = (self.chain[n - 2].0.marks, self.chain[n - 1].0.marks);
            if b.saturating_mul(2) < a || a.saturating_add(b) > max_segment_marks() {
                break;
            }
            let (b, bh) = self.chain.pop().unwrap();
            let (a, ah) = self.chain.pop().unwrap();
            let m = merge(&ah, &bh);
            let seg = write_segment(repo, &m).await?;
            self.stats.merged += 1;
            gone.extend([a.hash, b.hash]);
            self.chain.push((seg, m));
        }
        if self.replace {
            self.unpublished_gone.extend(gone);
            return Ok(());
        }
        self.write_manifest(repo, lease, gone).await
    }

    /// At the end of a run. A replacing run publishes its manifest here, and
    /// only here, even if it marked nothing, so a damaged manifest does not
    /// stay. It first merges runs of adjacent segments of its own whose
    /// marks fit in [`MAX_SEGMENT_MARKS`]: adjacent, so every prefix of what
    /// it names stays closed.
    pub(crate) async fn finish(
        &mut self,
        repo: &Repo,
        lease: &Option<crate::gc_lease::GcLease>,
    ) -> Result<()> {
        if !self.write {
            return Ok(());
        }
        if !self.replace {
            // Stop naming segments this run could not read, even if it
            // published nothing: they are retired and collected.
            if self.stats.unreadable > 0 && self.stats.published == 0 {
                self.write_manifest(repo, lease, Vec::new()).await?;
            }
            return Ok(());
        }
        let mut gone = std::mem::take(&mut self.unpublished_gone);
        let mut groups: Vec<Vec<(SegmentRef, Vec<Hash>)>> = Vec::new();
        for (s, h) in std::mem::take(&mut self.chain) {
            match groups.last_mut() {
                Some(g)
                    if g.iter().map(|(s, _)| s.marks).sum::<u64>() + s.marks
                        <= max_segment_marks() =>
                {
                    g.push((s, h))
                }
                _ => groups.push(vec![(s, h)]),
            }
        }
        for mut g in groups {
            if g.len() == 1 {
                self.chain.push(g.pop().unwrap());
                continue;
            }
            let mut all: Vec<Hash> = Vec::new();
            for (s, h) in g {
                gone.push(s.hash);
                all = merge(&all, &h);
            }
            let seg = write_segment(repo, &all).await?;
            self.stats.merged += 1;
            self.chain.push((seg, all));
        }
        self.write_manifest(repo, lease, gone).await
    }

    async fn write_manifest(
        &mut self,
        repo: &Repo,
        lease: &Option<crate::gc_lease::GcLease>,
        gone: Vec<Hash>,
    ) -> Result<()> {
        let old = self.stored.clone().unwrap_or_default();
        let mut next = Manifest {
            live: self.chain.iter().map(|(s, _)| *s).collect(),
            // A replacing run's segments are closed on their own. Otherwise
            // its segments may rest on rows, which then must still count.
            rows_retired: self.replace || old.rows_retired,
            retired: old.retired.clone(),
        };
        // Whatever the old manifest named and this one does not: merged
        // away, untrusted, or replaced.
        let kept: HashSet<Hash> = next.live.iter().map(|s| s.hash).collect();
        let dropped = old
            .live
            .iter()
            .map(|s| s.hash)
            .chain(gone)
            .filter(|h| !kept.contains(h));
        next.retire(dropped.collect::<Vec<_>>());
        self.swap(repo, lease, next).await
    }

    /// Swap the manifest from what this run last read or wrote to `next`.
    async fn swap(
        &mut self,
        repo: &Repo,
        lease: &Option<crate::gc_lease::GcLease>,
        next: Manifest,
    ) -> Result<()> {
        let bytes = swap_manifest(repo, lease, self.raw.as_deref(), &next).await?;
        self.raw = Some(bytes);
        self.stored = Some(next);
        Ok(())
    }

    /// Forget the retired segments `keep` rejects: no longer stored, or
    /// deleted by this run.
    pub(crate) async fn prune_retired(
        &mut self,
        repo: &Repo,
        lease: &Option<crate::gc_lease::GcLease>,
        keep: impl Fn(&Hash) -> bool,
    ) -> Result<()> {
        if !self.write {
            return Ok(());
        }
        let Some(stored) = &self.stored else {
            return Ok(());
        };
        let mut next = stored.clone();
        next.retired.retain(|h| keep(h));
        if next.retired.len() == stored.retired.len() {
            return Ok(());
        }
        self.swap(repo, lease, next).await
    }

    #[cfg(test)]
    pub(crate) fn all_hashes(&self) -> impl Iterator<Item = &Hash> {
        self.chain.iter().flat_map(|(_, h)| h.iter())
    }
}

/// Write a segment and make it durable before anything names it. Written
/// like any server blob (`put_rescuing_many_loose`), so a segment whose
/// identical bytes a GC condemned is rescued.
async fn write_segment(repo: &Repo, sorted: &[Hash]) -> Result<SegmentRef> {
    let (hash, data) = encode_segment(sorted, nonce());
    repo.put_rescuing_many_loose(vec![(hash, data)], &[])
        .await?;
    Ok(SegmentRef {
        hash,
        marks: sorted.len() as u64,
    })
}

/// What [`retire_mark_rows`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct RetireRowsReport {
    /// `gc-mark-v1` rows copied into a segment (0 if already retired).
    pub imported: u64,
    /// Whether the rows were already retired before this run.
    pub already_retired: bool,
    /// Rows deleted.
    pub deleted: u64,
}

/// Copy every `gc-mark-v1` row into one segment published first in the
/// manifest, and mark the rows retired: from then on readers ignore them.
/// Holds the GC lease, so no GC of this release writes the manifest
/// meanwhile, and swaps the manifest from what it read, so one that lost
/// its lease cannot put back a record that counts the rows. Refused on a
/// store without leases. Precondition: no `gc` from before ADR 0103 runs
/// (it writes rows without the lease). The rows' key order is the
/// segment's order, so this is one pass.
///
/// The segment is one, whatever its size: the rows have no post-order, so
/// only all of them together are closed.
pub async fn import_mark_rows(repo: &Repo) -> Result<RetireRowsReport> {
    if !repo.meta().supports_leases() {
        return Err(Error::Invalid(format!(
            "retiring the {GC_MARK} rows needs a metadata store with leases, so that no \
             garbage collection runs meanwhile"
        )));
    }
    let lease = crate::gc_lease::GcLease::acquire(repo.meta(), "blob-mark-import").await?;
    let result = import_leased(repo, &lease).await;
    crate::gc_lease::release(lease, result).await
}

async fn import_leased(
    repo: &Repo,
    lease: &Option<crate::gc_lease::GcLease>,
) -> Result<RetireRowsReport> {
    let (raw, read) = read_manifest_raw(repo).await?;
    let mut stored = match read {
        ManifestRead::Absent => Manifest::default(),
        ManifestRead::Read(m) => m,
        ManifestRead::Damaged => {
            return Err(Error::Other(format!(
                "the {GC_MARK_SEGMENTS} row does not decode; run `leep-server gc --full` \
                 first, which replaces it"
            )))
        }
    };
    if stored.rows_retired {
        return Ok(RetireRowsReport {
            already_retired: true,
            ..RetireRowsReport::default()
        });
    }
    let mut keys: Vec<Hash> = Vec::new();
    let mut after: Option<Hash> = None;
    loop {
        let page = repo
            .meta()
            .list_derived_keys(GC_MARK, after, 10_000)
            .await?;
        let Some(last) = page.last().copied() else {
            break;
        };
        after = Some(last);
        keys.extend(page);
    }
    keys.sort_unstable();
    keys.dedup();
    let imported = keys.len() as u64;
    if !keys.is_empty() {
        let seg = write_segment(repo, &keys).await?;
        // First: the later segments may rest on the rows, which it holds.
        stored.live.insert(0, seg);
    }
    stored.rows_retired = true;
    let live: HashSet<Hash> = stored.live.iter().map(|s| s.hash).collect();
    stored.retired.retain(|h| !live.contains(h));
    swap_manifest(repo, lease, raw.as_deref(), &stored).await?;
    Ok(RetireRowsReport {
        imported,
        ..RetireRowsReport::default()
    })
}

/// Delete the `gc-mark-v1` rows once the manifest says they are retired.
/// The caller records [`MARK_ROWS_RETIRED_FEATURE`] first: a `gc` binary
/// that predates segments trusts whatever rows are left, and a delete cut
/// short leaves rows whose subtrees lost theirs. `step` sees each step's
/// progress.
pub async fn delete_mark_rows(
    repo: &Repo,
    step: &mut (dyn FnMut(&leep_core::DeleteStep) + Send),
) -> Result<u64> {
    let lease = crate::gc_lease::GcLease::acquire(repo.meta(), "blob-mark-delete").await?;
    let result = async {
        // Read under the lease: the manifest cannot change between this
        // check and the delete except by a holder that lost its lease, and
        // every manifest write swaps from what its writer read.
        match read_manifest(repo).await? {
            ManifestRead::Read(m) if m.rows_retired => {}
            _ => {
                return Err(Error::Invalid(format!(
                    "the {GC_MARK} rows are not retired yet; import them first"
                )))
            }
        }
        let mut cursor: Option<Vec<u8>> = None;
        let mut total = 0;
        loop {
            crate::gc_lease::check(&lease)?;
            let s = repo
                .meta()
                .delete_derived_step(&[GC_MARK], cursor.as_deref())
                .await?;
            total += s.deleted.iter().sum::<u64>();
            step(&s);
            match s.next {
                Some(c) => cursor = Some(c),
                None => break,
            }
        }
        Ok(total)
    }
    .await;
    crate::gc_lease::release(lease, result).await
}

#[cfg(test)]
mod tests {
    use super::*;

    fn h(b: u8) -> Hash {
        Hash([b; 32])
    }

    #[test]
    fn segment_round_trip_and_golden_bytes() {
        let (addr, data) = encode_segment(&[h(1), h(2)], 0x0102_0304_0506_0708);
        let head = SEGMENT_HEADER;
        assert_eq!(head, 33);
        assert_eq!(data.len(), head + 64);
        assert_eq!(&data[..24], b"leep:gc-mark-segment-v1\0");
        assert_eq!(&data[24..head], &[1, 1, 2, 3, 4, 5, 6, 7, 8]);
        assert_eq!(&data[head..head + 32], &[1u8; 32]);
        assert_eq!(decode_segment(&addr, &data).unwrap(), vec![h(1), h(2)]);
        assert!(is_segment(&addr, &data));
        // The address of a blob, and of the object after the prefix; an
        // upload of these bytes is refused (ADR 0041).
        assert_eq!(addr, Hash::of_blob(&data));
        assert_eq!(addr, Hash::of_object("gc-mark-segment-v1", &data[24..]));
        assert!(leep_core::hash::is_object_preimage(&data));
        // Golden: changing the encoding or the kind orphans every segment.
        assert_eq!(
            addr.to_hex(),
            "df3a7f9c61d26c0b56adc2eb92ebb5f1b6b73063516f244a3cba5dea1074d50a"
        );
        let (empty, bytes) = encode_segment(&[], 0);
        assert_eq!(&bytes[24..], &[1u8, 0, 0, 0, 0, 0, 0, 0, 0]);
        // A new nonce, a new object.
        assert_ne!(encode_segment(&[], 1).0, empty);
        assert_ne!(nonce(), nonce());
        assert!(decode_segment(&empty, &bytes).unwrap().is_empty());
        // Not a segment: a blob without the prefix.
        assert!(!is_segment(&Hash::of_blob(b"x"), b"x"));
    }

    #[test]
    fn damaged_segments_are_refused() {
        let (addr, data) = encode_segment(&[h(1), h(2)], 9);
        for bit in 0..data.len() * 8 {
            let mut d = data.to_vec();
            d[bit / 8] ^= 1 << (bit % 8);
            assert!(decode_segment(&addr, &d).is_err(), "bit {bit}");
        }
        assert!(decode_segment(&addr, &data[..SEGMENT_HEADER + 8]).is_err());
        // Unsorted bytes at their own address are still refused.
        let mut d = data[..SEGMENT_HEADER].to_vec();
        d.extend_from_slice(&h(2).0);
        d.extend_from_slice(&h(1).0);
        assert!(decode_segment(&Hash::of_blob(&d), &d).is_err());
        // Right bytes at their address, but without the prefix.
        let bare = &data[24..];
        assert!(decode_segment(&Hash::of_blob(bare), bare).is_err());
    }

    #[test]
    fn manifest_round_trip_and_damage() {
        let m = Manifest {
            live: vec![
                SegmentRef {
                    hash: h(3),
                    marks: 7,
                },
                SegmentRef {
                    hash: h(4),
                    marks: 1,
                },
            ],
            rows_retired: true,
            retired: vec![h(9)],
        };
        let v = m.encode();
        assert_eq!(Manifest::decode(&v).unwrap(), m);
        assert_eq!(v[0], 1);
        assert_eq!(v[1], 1);
        assert_eq!(&v[2..6], &[0, 0, 0, 2]);
        for bit in 0..v.len() * 8 {
            let mut d = v.to_vec();
            d[bit / 8] ^= 1 << (bit % 8);
            assert!(Manifest::decode(&d).is_err(), "bit {bit}");
        }
        let empty = Manifest::default().encode();
        assert_eq!(empty.len(), 2 + 4 + 4 + 32);
        assert_eq!(Manifest::decode(&empty).unwrap(), Manifest::default());
    }

    #[test]
    fn merge_unions_sorted_lists() {
        assert_eq!(
            merge(&[h(1), h(3), h(5)], &[h(2), h(3), h(6)]),
            vec![h(1), h(2), h(3), h(5), h(6)]
        );
        assert_eq!(merge(&[], &[h(1)]), vec![h(1)]);
    }

    #[test]
    fn retired_list_is_bounded_and_never_names_a_live_segment() {
        let mut m = Manifest {
            live: vec![SegmentRef {
                hash: h(1),
                marks: 1,
            }],
            ..Manifest::default()
        };
        m.retire([h(1), h(2)]);
        assert_eq!(m.retired, vec![h(2)]);
        let many: Vec<Hash> = (0..MAX_RETIRED as u32 + 5)
            .map(|i| {
                let mut b = [0u8; 32];
                b[..4].copy_from_slice(&i.to_be_bytes());
                b[31] = 7;
                Hash(b)
            })
            .collect();
        m.retire(many.clone());
        assert_eq!(m.retired.len(), MAX_RETIRED);
        assert_eq!(*m.retired.last().unwrap(), *many.last().unwrap());
    }
}
