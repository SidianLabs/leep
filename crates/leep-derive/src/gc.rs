//! Blob garbage collection.
//!
//! Changesets are never deleted, so everything they reference is live: every
//! `Modify` content hash, plus every tree node and file blob reachable from
//! each changeset's root tree. Any other blob is garbage, provided it is older
//! than a grace window that protects uploads whose changeset is not stored
//! yet. Deleting is two-phase and fenced (ADR 0012): [`condemn`] marks
//! candidates in meta, [`collect`] deletes the ones still condemned after a
//! fence, so no delete races a request that might reference the blob.
//!
//! The mark set holds one 32-byte hash per live blob and tree node, so memory
//! grows linearly with the number of distinct live blobs (roughly 50 bytes
//! each). Subtrees shared between snapshots are walked once.

use std::collections::HashSet;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::future::try_join_all;
use leep_core::{ChangesetId, DerivedRow, Error, FileChange, Hash, Result, TreeEntry};

use crate::Repo;

/// Page size for changeset and blob listings.
const PAGE: usize = 1000;

/// Blob writes in flight when several are stored together. Each is
/// independent and content addressed, so order does not matter, and a store
/// that syncs every write gets them done together.
const BLOB_WRITES_IN_FLIGHT: usize = 4;

/// Derived-data kind recording when a client was last told a blob already
/// exists, or uploaded it (value: unix seconds, big-endian). A client that
/// hears "present" skips the upload and references the blob in a changeset it
/// creates later, so the blob's own modification time says nothing about that
/// use. GC keeps claimed blobs for the grace window like freshly uploaded
/// ones.
pub const BLOB_CLAIM: &str = "blob-claim-v1";

/// Derived-data kind marking a blob GC has condemned (value: unix seconds,
/// big-endian, from the GC host's clock). A condemned blob is reported missing
/// and rejected in changesets until it is uploaded again.
pub const BLOB_CONDEMNED: &str = "blob-condemned-v1";

/// Derived-data kind recording that a condemnation was answered by an upload
/// (value: the condemnation time it answers, big-endian). A blob is condemned
/// iff its condemnation time is strictly greater than this. The value is the
/// observed condemnation time, not the server's clock, so racing writers store
/// the same bytes and server clock skew cannot undo a rescue.
pub const BLOB_RESCUE: &str = "blob-rescue-v1";

/// Derived-data kind for the delete handshake (value: condemnation time,
/// big-endian, then one state byte). Collect sets it before its final rescue
/// check and uploads read it after writing their rescue, so one of the two
/// always sees the other.
pub const BLOB_DOOM: &str = "blob-doom-v1";

/// How long an upload waits for an in-progress delete of the same blob before
/// giving up with a retryable conflict.
const DOOM_WAIT: Duration = Duration::from_secs(30);
const DOOM_POLL: Duration = Duration::from_millis(25);

fn unix_now() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs() as i64)
}

/// Time source for GC, so tests can run a fence without waiting.
#[async_trait]
pub trait Clock: Send + Sync {
    /// Unix seconds.
    fn now(&self) -> i64;
    async fn sleep(&self, d: Duration);
}

pub struct SystemClock;

#[async_trait]
impl Clock for SystemClock {
    fn now(&self) -> i64 {
        unix_now()
    }

    async fn sleep(&self, d: Duration) {
        tokio::time::sleep(d).await;
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Doom {
    Deleting = 0,
    Abandoned = 1,
    Deleted = 2,
}

/// What meta says about one blob's condemnation, read in the order the
/// protocol needs: condemnation, rescue, then doom.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct BlobFence {
    condemned: Option<i64>,
    rescued: Option<i64>,
    doom: Option<(i64, Doom)>,
}

impl BlobFence {
    fn is_condemned(&self) -> bool {
        match self.condemned {
            Some(c) => self.rescued.is_none_or(|r| c > r),
            None => false,
        }
    }

    fn doom_state(&self) -> Option<Doom> {
        match (self.condemned, self.doom) {
            (Some(c), Some((e, d))) if e == c => Some(d),
            _ => None,
        }
    }

    /// Callers must not rely on the stored copy: it is condemned, or a delete
    /// of it may be in flight.
    fn blocked(&self) -> bool {
        self.is_condemned() || self.doom_state() == Some(Doom::Deleting)
    }
}

fn time_value(t: i64) -> Bytes {
    Bytes::copy_from_slice(&t.to_be_bytes())
}

fn decode_time(kind: &str, v: &[u8]) -> Result<i64> {
    <[u8; 8]>::try_from(v)
        .map(i64::from_be_bytes)
        .map_err(|_| Error::Other(format!("corrupt {kind} row")))
}

fn decode_doom(v: &[u8]) -> Result<(i64, Doom)> {
    let bad = || Error::Other(format!("corrupt {BLOB_DOOM} row"));
    let (t, state) = v.split_at_checked(8).ok_or_else(bad)?;
    let doom = match state {
        [0] => Doom::Deleting,
        [1] => Doom::Abandoned,
        [2] => Doom::Deleted,
        _ => return Err(bad()),
    };
    Ok((decode_time(BLOB_DOOM, t)?, doom))
}

/// Decode a GC row of `kind` into its time and, for `blob-doom-v1`, its state
/// byte. Public only so the fuzz harness can reach the private decoders.
#[doc(hidden)]
pub fn decode_row(kind: &str, v: &[u8]) -> Result<(i64, Option<u8>)> {
    if kind == BLOB_DOOM {
        let (t, d) = decode_doom(v)?;
        Ok((t, Some(d as u8)))
    } else {
        Ok((decode_time(kind, v)?, None))
    }
}

fn doom_value(epoch: i64, state: Doom) -> Bytes {
    let mut v = epoch.to_be_bytes().to_vec();
    v.push(state as u8);
    Bytes::from(v)
}

impl Repo {
    /// The subset of `hashes` the caller must upload. A condemned blob counts
    /// as missing and is not claimed. Every other stored hash is claimed
    /// first (see [`BLOB_CLAIM`]), so a concurrent GC cannot delete a blob a
    /// client was just told it may reuse.
    ///
    /// The condemnation rows are read in one batch and the claims written in
    /// another; each hash's fence is still read before its existence and
    /// both before its claim.
    pub async fn missing_blobs(&self, hashes: &[Hash]) -> Result<Vec<Hash>> {
        let now = time_value(unix_now());
        // A failed batch reads every fence singly.
        let condemned = self
            .meta()
            .get_derived_many(BLOB_CONDEMNED, hashes)
            .await
            .ok();
        let mut missing = Vec::new();
        let mut claims = Vec::new();
        for (i, h) in hashes.iter().enumerate() {
            let maybe = condemned.as_ref().is_none_or(|c| c[i].is_some());
            // Fence first, then existence: see `check_reusable`.
            let blocked = maybe && self.fence_state(h).await?.blocked();
            if blocked || !self.blobs().contains(h).await? {
                missing.push(*h);
            } else {
                claims.push(DerivedRow::new(BLOB_CLAIM, *h, now.clone()));
            }
        }
        self.meta().put_derived_many(&claims).await?;
        Ok(missing)
    }

    /// Store an uploaded blob and claim it. Rescues a condemned blob.
    pub async fn put_blob(&self, data: Bytes) -> Result<Hash> {
        let h = Hash::of_blob(&data);
        self.put_rescuing(h, data).await?;
        self.meta()
            .put_derived(BLOB_CLAIM, &h, time_value(unix_now()))
            .await?;
        Ok(h)
    }

    /// [`Repo::put_blob`] for several blobs: their condemnations are read in
    /// one batch after all of them are written, and their claims written in
    /// one batch after that. Returns their hashes in order.
    pub async fn put_blobs(&self, data: Vec<Bytes>) -> Result<Vec<Hash>> {
        let items: Vec<(Hash, Bytes)> = data.into_iter().map(|d| (Hash::of_blob(&d), d)).collect();
        let hashes: Vec<Hash> = items.iter().map(|(h, _)| *h).collect();
        self.put_rescuing_many(items).await?;
        let now = time_value(unix_now());
        let claims: Vec<DerivedRow> = hashes
            .iter()
            .map(|h| DerivedRow::new(BLOB_CLAIM, *h, now.clone()))
            .collect();
        self.meta().put_derived_many(&claims).await?;
        Ok(hashes)
    }

    /// Store `data` under `hash` such that GC cannot delete it afterwards
    /// without the caller noticing. Every server-side write of a blob that a
    /// changeset may reference goes through here, [`Repo::put_rescuing_many`]
    /// or [`Repo::put_blob`].
    ///
    /// If the blob is condemned, write the rescue, then read the delete
    /// handshake: collect writes the handshake and then reads the rescue, so
    /// either collect abandons the delete or this call sees it and waits.
    pub(crate) async fn put_rescuing(&self, hash: Hash, data: Bytes) -> Result<()> {
        self.blobs().put_with_hash(hash, data.clone()).await?;
        let Some(c) = self.condemned_row(&hash).await? else {
            return Ok(());
        };
        self.rescue(hash, data, c).await
    }

    /// [`Repo::put_rescuing`] for many blobs. Every blob is written before
    /// the condemnation rows are read, in one batch, so each blob's write
    /// still precedes its own check; the rare condemned one is then rescued
    /// as `put_rescuing` would.
    pub(crate) async fn put_rescuing_many(&self, items: Vec<(Hash, Bytes)>) -> Result<()> {
        for chunk in items.chunks(BLOB_WRITES_IN_FLIGHT) {
            let writes: Vec<_> = chunk
                .iter()
                .map(|(h, d)| self.blobs().put_with_hash(*h, d.clone()))
                .collect();
            try_join_all(writes).await?;
        }
        let hashes: Vec<Hash> = items.iter().map(|(h, _)| *h).collect();
        let batch = self.meta().get_derived_many(BLOB_CONDEMNED, &hashes).await;
        for (i, (hash, data)) in items.into_iter().enumerate() {
            let row = match &batch {
                Ok(rows) => rows[i]
                    .as_ref()
                    .map(|v| decode_time(BLOB_CONDEMNED, v))
                    .transpose()?,
                // As with every batched read, a failed batch is read singly.
                Err(_) => self.condemned_row(&hash).await?,
            };
            if let Some(c) = row {
                self.rescue(hash, data, c).await?;
            }
        }
        Ok(())
    }

    /// The part of [`Repo::put_rescuing`] after the blob is written and found
    /// condemned at `c`.
    async fn rescue(&self, hash: Hash, data: Bytes, c: i64) -> Result<()> {
        self.meta()
            .put_derived(BLOB_RESCUE, &hash, time_value(c))
            .await?;
        let mut waited = Duration::ZERO;
        loop {
            match self.fence_state(&hash).await?.doom_state() {
                Some(Doom::Deleting) if waited >= DOOM_WAIT => {
                    return Err(Error::Conflict(format!(
                        "blob {hash} is being garbage collected; retry"
                    )));
                }
                Some(Doom::Deleting) => {
                    tokio::time::sleep(DOOM_POLL).await;
                    waited += DOOM_POLL;
                }
                // The delete finished after our write; write again. No second
                // delete can start for this condemnation: the rescue stands.
                Some(Doom::Deleted) => return self.blobs().put_with_hash(hash, data).await,
                _ => return Ok(()),
            }
        }
    }

    /// Reject `content` if it is condemned or being deleted. Call before
    /// checking that the blob exists: a delete that finished before this read
    /// shows as missing, and one that starts later must first see the rescue
    /// (ADR 0012).
    pub(crate) async fn check_reusable(&self, content: &Hash, what: &str) -> Result<()> {
        if self.fence_state(content).await?.blocked() {
            return Err(Error::Invalid(format!("missing blob {content} for {what}")));
        }
        Ok(())
    }

    async fn condemned_row(&self, hash: &Hash) -> Result<Option<i64>> {
        self.meta()
            .get_derived(BLOB_CONDEMNED, hash)
            .await?
            .map(|v| decode_time(BLOB_CONDEMNED, &v))
            .transpose()
    }

    /// Reads from durable meta only; a stale answer here loses blobs.
    async fn fence_state(&self, hash: &Hash) -> Result<BlobFence> {
        let Some(condemned) = self.condemned_row(hash).await? else {
            return Ok(BlobFence::default());
        };
        let rescued = self
            .meta()
            .get_derived(BLOB_RESCUE, hash)
            .await?
            .map(|v| decode_time(BLOB_RESCUE, &v))
            .transpose()?;
        let doom = self
            .meta()
            .get_derived(BLOB_DOOM, hash)
            .await?
            .map(|v| decode_doom(&v))
            .transpose()?;
        Ok(BlobFence {
            condemned: Some(condemned),
            rescued,
            doom,
        })
    }

    /// Close a "deleting" announcement left by a collect that died after
    /// deleting the blob (or before). The blob is listed, so a later upload
    /// stored it again and rescued it; the announcement would otherwise block
    /// the hash for good, because only listed blobs are revisited. Only
    /// called for blobs this run will not delete, and runs never overlap.
    async fn settle_doom(&self, hash: &Hash, state: &BlobFence) -> Result<()> {
        if let (Some(c), Some(Doom::Deleting)) = (state.condemned, state.doom_state()) {
            self.meta()
                .put_derived(BLOB_DOOM, hash, doom_value(c, Doom::Abandoned))
                .await?;
        }
        Ok(())
    }

    async fn claimed_since(&self, hash: &Hash, cutoff: i64) -> Result<bool> {
        Ok(match self.meta().get_derived(BLOB_CLAIM, hash).await? {
            Some(v) => <[u8; 8]>::try_from(v.as_ref())
                .map(|b| i64::from_be_bytes(b) >= cutoff)
                // An unreadable claim errs on the side of keeping the blob.
                .unwrap_or(true),
            None => false,
        })
    }
}

#[derive(Clone, Debug)]
pub struct GcOptions {
    /// Unreferenced blobs modified or claimed within this window before the
    /// run started are not condemned. Zero disables the protection, including
    /// for blobs whose modification time is unknown. Must exceed the longest
    /// time a client takes from upload to commit and the servers' request
    /// timeout.
    pub grace: Duration,
    /// Collect deletes only condemnations at least this old. Must exceed the
    /// servers' request timeout plus clock skew (ADR 0012).
    pub fence: Duration,
    /// Allow a zero fence. Only safe with no server running.
    pub unsafe_no_fence: bool,
    /// Report what would be condemned or deleted without writing anything.
    pub dry_run: bool,
}

impl Default for GcOptions {
    fn default() -> Self {
        GcOptions {
            grace: Duration::from_secs(24 * 3600),
            fence: Duration::from_secs(10 * 60),
            unsafe_no_fence: false,
            dry_run: false,
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CondemnReport {
    pub changesets_scanned: u64,
    /// Distinct blobs and tree nodes referenced by stored changesets.
    pub marked: u64,
    pub blobs_scanned: u64,
    /// Blobs newly condemned, or that would be in a dry run.
    pub condemned: u64,
    pub condemned_bytes: u64,
    /// Candidates already condemned by an earlier run and not rescued since;
    /// their original condemnation time stands.
    pub already_condemned: u64,
    /// Unreferenced blobs kept because they are younger than the grace window.
    pub kept_by_grace: u64,
    /// Unreferenced blobs kept because the store reports no modification time.
    pub kept_unknown_mtime: u64,
    /// Unreferenced old blobs kept because a client was recently told they
    /// exist and may be about to reference them.
    pub kept_by_claim: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CollectReport {
    pub changesets_scanned: u64,
    pub marked: u64,
    pub blobs_scanned: u64,
    /// Condemnations at least a fence old that this run examined.
    pub condemned: u64,
    /// Condemnations younger than the fence, left for a later run.
    pub fence_pending: u64,
    /// Condemned blobs a newer upload rescued (a rescue row exists).
    pub rescued_by_claim: u64,
    /// Condemned blobs a changeset stored since now references.
    pub rescued_by_reference: u64,
    /// Condemned blobs whose store modification time is newer than the
    /// condemnation, i.e. written again by something that records no rescue.
    pub rescued_by_upload: u64,
    /// Blobs deleted, or that would be in a dry run.
    pub deleted: u64,
    pub deleted_bytes: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct GcReport {
    pub condemn: CondemnReport,
    /// Default (all zero) for a dry run, which never collects.
    pub collect: CollectReport,
}

/// Condemn, wait out the fence, then collect.
pub async fn run(repo: &Repo, opts: &GcOptions, clock: &dyn Clock) -> Result<GcReport> {
    check_fence(opts)?;
    let condemn = condemn(repo, opts, clock).await?;
    if opts.dry_run {
        return Ok(GcReport {
            condemn,
            collect: CollectReport::default(),
        });
    }
    clock.sleep(opts.fence).await;
    let collect = collect(repo, opts, clock).await?;
    Ok(GcReport { condemn, collect })
}

fn check_fence(opts: &GcOptions) -> Result<()> {
    if opts.fence.is_zero() && !opts.unsafe_no_fence && !opts.dry_run {
        return Err(Error::Invalid(
            "a zero fence can lose blobs while servers run; pass a positive fence \
             (longer than the servers' request timeout) or --unsafe-no-fence"
                .into(),
        ));
    }
    Ok(())
}

fn secs(d: Duration) -> i64 {
    i64::try_from(d.as_secs()).unwrap_or(i64::MAX)
}

/// Every blob and tree node referenced by any stored changeset. Any error
/// aborts before anything is deleted, so an incomplete mark set never loses a
/// blob.
async fn mark(repo: &Repo, changesets_scanned: &mut u64) -> Result<HashSet<Hash>> {
    let mut marked: HashSet<Hash> = HashSet::new();
    let mut after: Option<ChangesetId> = None;
    loop {
        let page = repo.meta().list_changesets(after, PAGE).await?;
        let Some(last) = page.last().copied() else {
            return Ok(marked);
        };
        after = Some(last);
        for id in page {
            *changesets_scanned += 1;
            let cs = repo.get_changeset(id).await?;
            for change in cs.changes.values() {
                if let FileChange::Modify { content, .. } = change {
                    marked.insert(*content);
                }
            }
            // Tree and blob hashes are domain-separated, so one set doubles as
            // the visited set: a tree already in it was fully walked (or is
            // queued below) via an earlier snapshot.
            let mut stack = vec![repo.root_tree(id).await?];
            while let Some(tree) = stack.pop() {
                if !marked.insert(tree) {
                    continue;
                }
                for entry in repo.read_tree(tree).await?.entries.into_values() {
                    match entry {
                        TreeEntry::File { content, .. } => {
                            marked.insert(content);
                        }
                        TreeEntry::Dir { tree } => {
                            if !marked.contains(&tree) {
                                stack.push(tree);
                            }
                        }
                    }
                }
            }
        }
    }
}

/// Phase one: mark, then write a condemnation for every unreferenced blob
/// that is old and unclaimed. Deletes nothing.
pub async fn condemn(repo: &Repo, opts: &GcOptions, clock: &dyn Clock) -> Result<CondemnReport> {
    let mut report = CondemnReport::default();
    let started_at = clock.now();
    let marked = mark(repo, &mut report.changesets_scanned).await?;
    report.marked = marked.len() as u64;
    let grace = secs(opts.grace);
    let cutoff = started_at.saturating_sub(grace);
    let mut after: Option<Hash> = None;
    loop {
        let page = repo.blobs().list_page(after, PAGE).await?;
        let Some(last) = page.last().map(|m| m.hash) else {
            return Ok(report);
        };
        after = Some(last);
        for blob in page {
            report.blobs_scanned += 1;
            if marked.contains(&blob.hash) {
                continue;
            }
            match blob.modified {
                Some(t) if t > cutoff => {
                    report.kept_by_grace += 1;
                    continue;
                }
                None if grace > 0 => {
                    report.kept_unknown_mtime += 1;
                    continue;
                }
                _ => {}
            }
            if repo.claimed_since(&blob.hash, cutoff).await? {
                report.kept_by_claim += 1;
                continue;
            }
            let state = repo.fence_state(&blob.hash).await?;
            if state.is_condemned() {
                report.already_condemned += 1;
                continue;
            }
            if !opts.dry_run {
                // Strictly after any earlier condemnation, hence after any
                // rescue of it, even if the clock stepped back.
                let at = state.condemned.map_or(0, |c| c + 1).max(clock.now());
                repo.meta()
                    .put_derived(BLOB_CONDEMNED, &blob.hash, time_value(at))
                    .await?;
            }
            report.condemned += 1;
            report.condemned_bytes += blob.size;
        }
    }
}

/// Phase two: delete blobs whose condemnation is at least `fence` older than
/// this run's start and that nothing has rescued. The mark runs after the
/// fence, so it sees every changeset stored by a request that passed its
/// check before the condemnation.
pub async fn collect(repo: &Repo, opts: &GcOptions, clock: &dyn Clock) -> Result<CollectReport> {
    check_fence(opts)?;
    let mut report = CollectReport::default();
    let started_at = clock.now();
    let eligible_before = started_at.saturating_sub(secs(opts.fence));
    let marked = mark(repo, &mut report.changesets_scanned).await?;
    report.marked = marked.len() as u64;
    let mut after: Option<Hash> = None;
    loop {
        let page = repo.blobs().list_page(after, PAGE).await?;
        let Some(last) = page.last().map(|m| m.hash) else {
            return Ok(report);
        };
        after = Some(last);
        for blob in page {
            report.blobs_scanned += 1;
            let state = repo.fence_state(&blob.hash).await?;
            let Some(c) = state.condemned else {
                continue;
            };
            if c > eligible_before {
                report.fence_pending += 1;
                continue;
            }
            report.condemned += 1;
            let is_marked = marked.contains(&blob.hash);
            if !state.is_condemned() {
                if !is_marked {
                    report.rescued_by_claim += 1;
                }
                if !opts.dry_run {
                    repo.settle_doom(&blob.hash, &state).await?;
                }
                continue;
            }
            if is_marked {
                // Stored by a request that checked before the condemnation.
                // Clearing the condemnation lets clients reuse the blob.
                if !opts.dry_run {
                    repo.meta()
                        .put_derived(BLOB_RESCUE, &blob.hash, time_value(c))
                        .await?;
                    repo.settle_doom(&blob.hash, &state).await?;
                }
                report.rescued_by_reference += 1;
                continue;
            }
            if blob.modified.is_some_and(|t| t > c) {
                report.rescued_by_upload += 1;
                if !opts.dry_run {
                    repo.settle_doom(&blob.hash, &state).await?;
                }
                continue;
            }
            if opts.dry_run {
                report.deleted += 1;
                report.deleted_bytes += blob.size;
                continue;
            }
            // Handshake: announce the delete, then look for a rescue. An
            // upload writes its rescue and then looks for the announcement.
            repo.meta()
                .put_derived(BLOB_DOOM, &blob.hash, doom_value(c, Doom::Deleting))
                .await?;
            let now = repo.fence_state(&blob.hash).await?;
            if now.condemned != Some(c) || !now.is_condemned() {
                repo.meta()
                    .put_derived(BLOB_DOOM, &blob.hash, doom_value(c, Doom::Abandoned))
                    .await?;
                report.rescued_by_claim += 1;
                continue;
            }
            repo.blobs().delete(&blob.hash).await?;
            repo.meta()
                .put_derived(BLOB_DOOM, &blob.hash, doom_value(c, Doom::Deleted))
                .await?;
            report.deleted += 1;
            report.deleted_bytes += blob.size;
        }
    }
}

/// Parse `0`, `90s`, `30m`, `24h` or `7d`. A bare number is seconds.
pub fn parse_duration(s: &str) -> Result<Duration> {
    let s = s.trim();
    let bad = || Error::Invalid(format!("bad duration {s:?}; use e.g. 0, 90s, 30m, 24h, 7d"));
    let (num, unit) = match s.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((i, _)) => s.split_at(i),
        None => (s, "s"),
    };
    let n: u64 = num.parse().map_err(|_| bad())?;
    let mult = match unit {
        "s" => 1,
        "m" => 60,
        "h" => 3600,
        "d" => 86400,
        _ => return Err(bad()),
    };
    n.checked_mul(mult).map(Duration::from_secs).ok_or_else(bad)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::sync::{Arc, Mutex};

    use bytes::Bytes;
    use leep_core::api::{Authorship, MergeRequest, MergeResponse};
    use leep_core::memory::{MemoryBlobStore, MemoryMetaStore};
    use leep_core::{BlobMeta, BlobStore, Changeset, FileKind, MetaStore, RepoPath, TreeNode};

    use super::*;

    const HOUR: i64 = 3600;

    fn now() -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64
    }

    fn setup() -> (Repo, Arc<MemoryBlobStore>) {
        let blobs = Arc::new(MemoryBlobStore::new());
        let repo = Repo::new(blobs.clone(), Arc::new(MemoryMetaStore::new()));
        (repo, blobs)
    }

    /// A clock that only moves when told to or when GC sleeps.
    struct TestClock(Mutex<i64>);

    impl TestClock {
        fn at(t: i64) -> Self {
            TestClock(Mutex::new(t))
        }

        fn advance(&self, secs: i64) {
            *self.0.lock().unwrap() += secs;
        }
    }

    #[async_trait]
    impl Clock for TestClock {
        fn now(&self) -> i64 {
            *self.0.lock().unwrap()
        }

        async fn sleep(&self, d: Duration) {
            self.advance(d.as_secs() as i64);
        }
    }

    const FENCE: i64 = 600;

    fn opts(grace_hours: u64, dry_run: bool) -> GcOptions {
        GcOptions {
            grace: Duration::from_secs(grace_hours * 3600),
            fence: Duration::from_secs(FENCE as u64),
            unsafe_no_fence: false,
            dry_run,
        }
    }

    /// Condemn, wait out the fence and collect, starting now.
    async fn gc(r: &Repo, grace_hours: u64, dry_run: bool) -> GcReport {
        gc_at(r, grace_hours, dry_run, now()).await
    }

    async fn gc_at(r: &Repo, grace_hours: u64, dry_run: bool, at: i64) -> GcReport {
        run(r, &opts(grace_hours, dry_run), &TestClock::at(at))
            .await
            .unwrap()
    }

    fn rp(s: &str) -> RepoPath {
        RepoPath::new(s).unwrap()
    }

    /// `Some(text)` writes a file, `None` deletes it.
    async fn commit(
        r: &Repo,
        parents: &[ChangesetId],
        files: &[(&str, Option<&str>)],
    ) -> ChangesetId {
        let mut changes = BTreeMap::new();
        for (p, c) in files {
            let ch = match c {
                Some(t) => FileChange::Modify {
                    content: r
                        .blobs()
                        .put(Bytes::copy_from_slice(t.as_bytes()))
                        .await
                        .unwrap(),
                    kind: FileKind::Regular,
                    size: t.len() as u64,
                },
                None => FileChange::Delete,
            };
            changes.insert(rp(p), ch);
        }
        r.create_changeset(Changeset {
            parents: parents.to_vec(),
            author: "t".into(),
            timestamp: 1,
            message: "m".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap()
    }

    async fn merge(r: &Repo, ours: ChangesetId, theirs: ChangesetId) -> ChangesetId {
        let resp = r
            .merge(MergeRequest {
                parents: [ours, theirs],
                message: "merge".into(),
                authorship: Authorship {
                    author: "m".into(),
                    timestamp: Some(5),
                },
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        match resp {
            MergeResponse::Merged { id } => id,
            other => panic!("expected clean merge: {other:?}"),
        }
    }

    /// Age every blob in the store past any grace window.
    async fn age_all(blobs: &MemoryBlobStore) {
        for m in blobs.list_page(None, usize::MAX).await.unwrap() {
            blobs.set_modified(&m.hash, Some(now() - 100 * HOUR));
        }
    }

    async fn all_ids(r: &Repo) -> Vec<ChangesetId> {
        r.meta().list_changesets(None, usize::MAX).await.unwrap()
    }

    /// Every file of every changeset, as (id, path, bytes), read through the
    /// public read path.
    async fn read_everything(r: &Repo) -> Vec<(ChangesetId, String, Bytes)> {
        let mut out = Vec::new();
        for id in all_ids(r).await {
            for f in r.list_files(id, "").await.unwrap() {
                let (_, data) = r
                    .read_file(id, &f.path)
                    .await
                    .unwrap()
                    .expect("listed file is readable");
                out.push((id, f.path.to_string(), data));
            }
        }
        out.sort();
        out
    }

    /// Branching history with merges, deletes, nested dirs, and a file that
    /// exists only in old snapshots.
    async fn build_history(r: &Repo) -> Vec<ChangesetId> {
        let base = commit(
            r,
            &[],
            &[
                ("a.txt", Some("a1\n")),
                ("gone.txt", Some("gone\n")),
                ("d/x.txt", Some("x1\n")),
                ("d/e/y.txt", Some("y1\n")),
            ],
        )
        .await;
        let left1 = commit(r, &[base], &[("a.txt", Some("a2\n")), ("gone.txt", None)]).await;
        let left2 = commit(
            r,
            &[left1],
            &[("d/x.txt", Some("x2\n")), ("l.txt", Some("l\n"))],
        )
        .await;
        let right1 = commit(
            r,
            &[base],
            &[("d/e/y.txt", Some("y2\n")), ("r/new.txt", Some("n\n"))],
        )
        .await;
        let right2 = commit(
            r,
            &[right1],
            &[("r/new.txt", None), ("r2.txt", Some("r2\n"))],
        )
        .await;
        let m1 = merge(r, left2, right2).await;
        let tip = commit(
            r,
            &[m1],
            &[("d/e/y.txt", None), ("final.txt", Some("fin\n"))],
        )
        .await;
        vec![base, left1, left2, right1, right2, m1, tip]
    }

    #[tokio::test]
    async fn never_deletes_anything_reachable_from_history() {
        let (r, blobs) = setup();
        let ids = build_history(&r).await;
        r.meta().cas_ref("main", None, Some(ids[6])).await.unwrap();
        // Unlanded changeset: stored but referenced by no ref.
        let unlanded = commit(&r, &[ids[0]], &[("only-here.txt", Some("unlanded\n"))]).await;
        let before = read_everything(&r).await;
        let blobs_before = blobs.len();
        assert!(before.iter().any(|(_, p, _)| p == "gone.txt"));
        age_all(&blobs).await;

        // No garbage yet: grace 0 must delete nothing at all.
        let report = gc(&r, 0, false).await;
        assert_eq!(report.collect.deleted, 0);
        assert_eq!(report.condemn.condemned, 0);
        assert_eq!(report.condemn.changesets_scanned, 8);
        assert_eq!(report.condemn.blobs_scanned as usize, blobs_before);
        assert_eq!(report.condemn.marked as usize, blobs_before);
        assert_eq!(blobs.len(), blobs_before);

        // Add garbage of every kind, then collect.
        let orphan = r.blobs().put(Bytes::from("orphan upload")).await.unwrap();
        let orphan_tree = TreeNode {
            entries: BTreeMap::from([(
                "f".to_string(),
                TreeEntry::File {
                    content: orphan,
                    kind: FileKind::Regular,
                    size: 13,
                },
            )]),
        };
        r.blobs()
            .put_with_hash(orphan_tree.hash(), Bytes::from(orphan_tree.encode()))
            .await
            .unwrap();
        // A commit that fails validation after uploading its blob.
        let failed = Changeset {
            parents: vec![ids[0]],
            author: "t".into(),
            timestamp: 1,
            message: "bad".into(),
            changes: BTreeMap::from([
                (
                    rp("never.txt"),
                    FileChange::Modify {
                        content: r.blobs().put(Bytes::from("failed commit")).await.unwrap(),
                        kind: FileKind::Regular,
                        size: 13,
                    },
                ),
                (rp("absent.txt"), FileChange::Delete),
            ]),
            extra: BTreeMap::new(),
        };
        assert!(r.create_changeset(failed).await.is_err());
        age_all(&blobs).await;
        let report = gc(&r, 24, false).await;
        assert_eq!(report.collect.deleted, 3, "{report:?}");
        assert_eq!(blobs.len(), blobs_before);
        assert!(!blobs.contains(&orphan).await.unwrap());
        assert!(!blobs.contains(&orphan_tree.hash()).await.unwrap());

        assert_eq!(read_everything(&r).await, before);
        assert!(r
            .read_file(unlanded, &rp("only-here.txt"))
            .await
            .unwrap()
            .is_some());
        // Idempotent.
        let again = gc(&r, 0, false).await;
        assert_eq!(again.collect.deleted, 0);
        assert_eq!(read_everything(&r).await, before);
    }

    #[tokio::test]
    async fn derived_trees_are_rebuilt_not_trusted_missing() {
        // Tree nodes of old snapshots survive even when only the changesets
        // (not derived root trees) were stored, e.g. after a derived-data wipe.
        let (r, blobs) = setup();
        let ids = build_history(&r).await;
        let before = read_everything(&r).await;
        // A fresh Repo over the same blobs but a meta store with only the
        // changesets: root trees are derived during the mark phase.
        let meta = Arc::new(MemoryMetaStore::new());
        for id in &ids {
            meta.put_changeset(&r.get_changeset(*id).await.unwrap())
                .await
                .unwrap();
        }
        let r2 = Repo::new(blobs.clone(), meta);
        age_all(&blobs).await;
        let report = gc(&r2, 0, false).await;
        assert_eq!(report.collect.deleted, 0);
        assert_eq!(read_everything(&r2).await, before);
    }

    #[tokio::test]
    async fn grace_window_protects_young_orphans() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("fresh upload")).await.unwrap();
        age_all(&blobs).await;
        blobs.set_modified(&orphan, Some(now() - HOUR));

        let report = gc(&r, 24, false).await;
        assert_eq!(
            (report.collect.deleted, report.condemn.kept_by_grace),
            (0, 1)
        );
        assert!(blobs.contains(&orphan).await.unwrap());

        // Re-uploading a stale orphan restarts its clock.
        blobs.set_modified(&orphan, Some(now() - 30 * HOUR));
        r.blobs().put(Bytes::from("fresh upload")).await.unwrap();
        let report = gc(&r, 24, false).await;
        assert_eq!(
            (report.collect.deleted, report.condemn.kept_by_grace),
            (0, 1)
        );

        blobs.set_modified(&orphan, Some(now() - 25 * HOUR));
        let report = gc(&r, 24, false).await;
        assert_eq!(
            (report.collect.deleted, report.collect.deleted_bytes),
            (1, 12)
        );
        assert!(!blobs.contains(&orphan).await.unwrap());
    }

    #[tokio::test]
    async fn orphan_becomes_live_if_a_changeset_references_it() {
        let (r, blobs) = setup();
        let base = commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let h = r.blobs().put(Bytes::from("late commit")).await.unwrap();
        age_all(&blobs).await;
        // The client finally commits, long after upload.
        let id = r
            .create_changeset(Changeset {
                parents: vec![base],
                author: "t".into(),
                timestamp: 1,
                message: "late".into(),
                changes: BTreeMap::from([(
                    rp("late.txt"),
                    FileChange::Modify {
                        content: h,
                        kind: FileKind::Regular,
                        size: 11,
                    },
                )]),
                extra: BTreeMap::new(),
            })
            .await
            .unwrap();
        let report = gc(&r, 0, false).await;
        assert_eq!(report.collect.deleted, 0);
        assert!(r.read_file(id, &rp("late.txt")).await.unwrap().is_some());
    }

    #[tokio::test]
    async fn dry_run_writes_nothing() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("orphan")).await.unwrap();
        age_all(&blobs).await;
        let n = blobs.len();
        let report = gc(&r, 0, true).await;
        assert_eq!(
            (report.condemn.condemned, report.condemn.condemned_bytes),
            (1, 6)
        );
        assert_eq!(report.collect, CollectReport::default());
        assert_eq!(blobs.len(), n);
        for kind in [BLOB_CONDEMNED, BLOB_RESCUE, BLOB_DOOM] {
            assert!(r.meta().get_derived(kind, &orphan).await.unwrap().is_none());
        }

        // A dry collect of a real condemnation changes nothing either.
        let t = now() + 100;
        condemn(&r, &opts(0, false), &TestClock::at(t))
            .await
            .unwrap();
        let dry = collect(&r, &opts(0, true), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        assert_eq!((dry.deleted, dry.deleted_bytes), (1, 6));
        assert_eq!(blobs.len(), n);
        assert!(r
            .meta()
            .get_derived(BLOB_DOOM, &orphan)
            .await
            .unwrap()
            .is_none());

        let report = gc_at(&r, 0, false, t + 100).await;
        assert_eq!(report.collect.deleted, 1);
        assert_eq!(blobs.len(), n - 1);
    }

    #[tokio::test]
    async fn unknown_mtime_is_kept_unless_grace_is_zero() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("mystery")).await.unwrap();
        blobs.set_modified(&orphan, None);
        let report = gc(&r, 24, false).await;
        assert_eq!(
            (report.collect.deleted, report.condemn.kept_unknown_mtime),
            (0, 1)
        );
        assert!(blobs.contains(&orphan).await.unwrap());
        let report = gc(&r, 0, false).await;
        assert_eq!(report.collect.deleted, 1);
        assert!(!blobs.contains(&orphan).await.unwrap());
    }

    #[tokio::test]
    async fn pages_through_more_than_one_page_of_blobs() {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        for i in 0..(PAGE * 2 + 17) {
            r.blobs()
                .put(Bytes::from(format!("orphan {i}")))
                .await
                .unwrap();
        }
        age_all(&blobs).await;
        let report = gc(&r, 1, false).await;
        assert_eq!(report.collect.deleted as usize, PAGE * 2 + 17);
        assert_eq!(report.condemn.blobs_scanned as usize, PAGE * 2 + 17 + 2);
        assert_eq!(blobs.len(), 2);
    }

    #[tokio::test]
    async fn missing_tree_node_aborts_without_deleting() {
        let (r, blobs) = setup();
        let id = commit(&r, &[], &[("d/a.txt", Some("a\n"))]).await;
        let orphan = r.blobs().put(Bytes::from("orphan")).await.unwrap();
        age_all(&blobs).await;
        let sub = match r
            .read_tree(r.root_tree(id).await.unwrap())
            .await
            .unwrap()
            .entries["d"]
        {
            TreeEntry::Dir { tree } => tree,
            _ => unreachable!(),
        };
        blobs.delete(&sub).await.unwrap();
        let clock = TestClock::at(now());
        assert!(run(&r, &opts(0, false), &clock).await.is_err());
        assert!(blobs.contains(&orphan).await.unwrap());
    }

    #[test]
    fn durations() {
        assert_eq!(parse_duration("0").unwrap(), Duration::ZERO);
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("30m").unwrap(), Duration::from_secs(1800));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86400));
        assert_eq!(parse_duration("7d").unwrap(), Duration::from_secs(604800));
        assert_eq!(parse_duration("45").unwrap(), Duration::from_secs(45));
        for bad in ["", "h", "-1h", "1.5h", "5x", "1h30m"] {
            assert!(parse_duration(bad).is_err(), "{bad}");
        }
    }

    /// A client told "present" may reference an old orphan without uploading
    /// it again; GC must not delete it out from under the coming changeset.
    #[tokio::test]
    async fn claimed_orphan_survives_until_claim_expires() {
        let (repo, blobs) = setup();
        let orphan = blobs
            .put(Bytes::from_static(b"shared content\n"))
            .await
            .unwrap();
        blobs.set_modified(&orphan, Some(now() - 100 * HOUR));
        let absent = Hash::of_blob(b"never uploaded");

        assert_eq!(
            repo.missing_blobs(&[orphan, absent]).await.unwrap(),
            [absent]
        );
        assert!(repo
            .meta()
            .get_derived(BLOB_CLAIM, &absent)
            .await
            .unwrap()
            .is_none());

        let r = gc(&repo, 24, false).await;
        assert_eq!((r.collect.deleted, r.condemn.kept_by_claim), (0, 1));
        assert!(blobs.contains(&orphan).await.unwrap());

        // The client's changeset arrives after the run: the blob is live now.
        commit_existing(&repo, orphan, 15).await;
        let r = gc_at(&repo, 24, false, now() + 48 * HOUR).await;
        assert_eq!(r.collect.deleted, 0);

        // A claim that is never followed up expires with the grace window.
        let stale = blobs.put(Bytes::from_static(b"abandoned\n")).await.unwrap();
        blobs.set_modified(&stale, Some(now() - 100 * HOUR));
        repo.missing_blobs(&[stale]).await.unwrap();
        let r = gc_at(&repo, 24, false, now() + 48 * HOUR).await;
        assert_eq!((r.collect.deleted, r.condemn.kept_by_claim), (1, 0));
        assert!(!blobs.contains(&stale).await.unwrap());
    }

    async fn commit_existing(r: &Repo, content: Hash, size: u64) {
        let mut changes = BTreeMap::new();
        changes.insert(
            rp("reused.txt"),
            FileChange::Modify {
                content,
                kind: FileKind::Regular,
                size,
            },
        );
        r.create_changeset(Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 1,
            message: "reuse".into(),
            changes,
            extra: BTreeMap::new(),
        })
        .await
        .unwrap();
    }

    /// An aged, unreferenced blob in a repo that has one commit.
    async fn orphan_setup(content: &'static str) -> (Repo, Arc<MemoryBlobStore>, Hash, i64) {
        let (r, blobs) = setup();
        commit(&r, &[], &[("a.txt", Some("a\n"))]).await;
        let orphan = blobs.put(Bytes::from(content)).await.unwrap();
        age_all(&blobs).await;
        (r, blobs, orphan, now() + 1000)
    }

    fn client_commit(content: Hash, size: u64, path: &str) -> Changeset {
        Changeset {
            parents: vec![],
            author: "t".into(),
            timestamp: 1,
            message: "m".into(),
            changes: BTreeMap::from([(
                rp(path),
                FileChange::Modify {
                    content,
                    kind: FileKind::Regular,
                    size,
                },
            )]),
            extra: BTreeMap::new(),
        }
    }

    fn is_missing_blob<T>(r: Result<T>) -> bool {
        matches!(r, Err(Error::Invalid(m)) if m.starts_with("missing blob"))
    }

    #[tokio::test]
    async fn row_encodings_are_stable() {
        // Golden bytes: changing them orphans every condemnation.
        assert_eq!(
            &time_value(1_700_000_000)[..],
            [0, 0, 0, 0, 0x65, 0x53, 0xf1, 0x00]
        );
        assert_eq!(
            &doom_value(1_700_000_000, Doom::Deleting)[..],
            [0, 0, 0, 0, 0x65, 0x53, 0xf1, 0x00, 0]
        );
        assert_eq!(doom_value(1, Doom::Abandoned)[8], 1);
        assert_eq!(doom_value(1, Doom::Deleted)[8], 2);
        assert_eq!(
            decode_doom(&doom_value(-5, Doom::Deleted)).unwrap(),
            (-5, Doom::Deleted)
        );
        assert!(decode_doom(&[0; 8]).is_err());
        assert!(decode_doom(&[0, 0, 0, 0, 0, 0, 0, 0, 9]).is_err());
        assert!(decode_time(BLOB_RESCUE, &[1, 2, 3]).is_err());

        let (r, _, orphan, t) = orphan_setup("golden").await;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        let row = r.meta().get_derived(BLOB_CONDEMNED, &orphan).await.unwrap();
        assert_eq!(row.unwrap().as_ref(), t.to_be_bytes());
    }

    #[tokio::test]
    async fn untouched_condemned_orphan_is_deleted_after_the_fence() {
        let (r, blobs, orphan, t) = orphan_setup("orphan").await;
        let c = condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        assert_eq!((c.condemned, c.condemned_bytes), (1, 6));
        assert!(blobs.contains(&orphan).await.unwrap());

        // Too early: the fence has not passed.
        let early = collect(&r, &opts(1, false), &TestClock::at(t + FENCE - 1))
            .await
            .unwrap();
        assert_eq!((early.deleted, early.fence_pending), (0, 1));
        assert!(blobs.contains(&orphan).await.unwrap());

        // A separate process, later: condemnation age comes from the row.
        let done = collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        assert_eq!((done.deleted, done.deleted_bytes), (1, 6));
        assert!(!blobs.contains(&orphan).await.unwrap());
        // The next run has nothing left, and a deleted blob is still missing.
        assert_eq!(gc_at(&r, 1, false, t + 2 * FENCE).await.collect.deleted, 0);
        assert_eq!(r.missing_blobs(&[orphan]).await.unwrap(), [orphan]);
    }

    #[tokio::test]
    async fn changeset_stored_during_the_fence_keeps_the_blob() {
        let (r, blobs, orphan, t) = orphan_setup("raced commit").await;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        // A request that passed its check before the condemnation stores its
        // changeset now.
        let id = r
            .store_server_changeset(client_commit(orphan, 12, "raced.txt"))
            .await
            .unwrap();
        let done = collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        assert_eq!((done.deleted, done.rescued_by_reference), (0, 1));
        assert!(blobs.contains(&orphan).await.unwrap());
        assert!(r.read_file(id, &rp("raced.txt")).await.unwrap().is_some());
        // The condemnation is cleared, so clients can reuse the blob again.
        assert!(r.missing_blobs(&[orphan]).await.unwrap().is_empty());
        r.create_changeset(client_commit(orphan, 12, "again.txt"))
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn reupload_during_the_fence_rescues() {
        let (r, blobs, orphan, t) = orphan_setup("same bytes").await;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        assert_eq!(r.missing_blobs(&[orphan]).await.unwrap(), [orphan]);
        assert_eq!(r.put_blob(Bytes::from("same bytes")).await.unwrap(), orphan);
        assert!(r.missing_blobs(&[orphan]).await.unwrap().is_empty());
        r.create_changeset(client_commit(orphan, 10, "b.txt"))
            .await
            .unwrap();

        // Even if the commit never comes, the blob survives this run.
        let (r2, blobs2, orphan2, t2) = orphan_setup("same bytes 2").await;
        condemn(&r2, &opts(1, false), &TestClock::at(t2))
            .await
            .unwrap();
        r2.put_blob(Bytes::from("same bytes 2")).await.unwrap();
        let done = collect(&r2, &opts(1, false), &TestClock::at(t2 + FENCE))
            .await
            .unwrap();
        assert_eq!((done.deleted, done.rescued_by_claim), (0, 1));
        assert!(blobs2.contains(&orphan2).await.unwrap());
        assert!(blobs.contains(&orphan).await.unwrap());

        // Condemned again later (the upload's grace has run out): the new
        // condemnation is newer than the rescue, so it counts again.
        let late = t2 + 2 * 24 * 3600;
        let again = condemn(&r2, &opts(1, false), &TestClock::at(late))
            .await
            .unwrap();
        assert_eq!(again.condemned, 1);
        assert_eq!(r2.missing_blobs(&[orphan2]).await.unwrap(), [orphan2]);
    }

    #[tokio::test]
    async fn condemned_blob_is_missing_unclaimed_and_rejected_in_commits() {
        let (r, blobs, orphan, t) = orphan_setup("condemned").await;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        let claim_before = r.meta().get_derived(BLOB_CLAIM, &orphan).await.unwrap();
        assert_eq!(r.missing_blobs(&[orphan]).await.unwrap(), [orphan]);
        assert_eq!(
            r.meta().get_derived(BLOB_CLAIM, &orphan).await.unwrap(),
            claim_before
        );
        assert!(is_missing_blob(
            r.create_changeset(client_commit(orphan, 9, "c.txt")).await
        ));
        // Untouched, so the collect deletes it despite the failed attempts.
        let done = collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        assert_eq!(done.deleted, 1);
        assert!(!blobs.contains(&orphan).await.unwrap());
    }

    #[tokio::test]
    async fn crashed_run_leaves_harmless_rows() {
        let (r, blobs, orphan, t) = orphan_setup("crash").await;
        let c = condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        assert_eq!(c.condemned, 1);
        // A later run keeps the original condemnation and then collects.
        let report = gc_at(&r, 1, false, t + 3600).await;
        assert_eq!(report.condemn.already_condemned, 1);
        assert_eq!(report.condemn.condemned, 0);
        assert_eq!(report.collect.deleted, 1);
        assert!(!blobs.contains(&orphan).await.unwrap());

        // A crash between "announce" and "delete" is finished by a re-run.
        let (r, blobs, orphan, t) = orphan_setup("crash 2").await;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        r.meta()
            .put_derived(BLOB_DOOM, &orphan, doom_value(t, Doom::Deleting))
            .await
            .unwrap();
        let done = collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        assert_eq!(done.deleted, 1);
        assert!(!blobs.contains(&orphan).await.unwrap());
        let state = r.fence_state(&orphan).await.unwrap();
        assert_eq!(state.doom_state(), Some(Doom::Deleted));

        // A condemnation rescued before the crash stays rescued.
        let (r, blobs, orphan, t) = orphan_setup("crash 3").await;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        r.put_blob(Bytes::from("crash 3")).await.unwrap();
        let done = gc_at(&r, 1, false, t + 10).await;
        assert_eq!(done.collect.deleted, 0);
        assert!(blobs.contains(&orphan).await.unwrap());
    }

    #[tokio::test]
    async fn zero_fence_needs_an_explicit_opt_out() {
        let (r, blobs, orphan, t) = orphan_setup("no fence").await;
        let mut o = opts(1, false);
        o.fence = Duration::ZERO;
        assert!(run(&r, &o, &TestClock::at(t)).await.is_err());
        assert!(collect(&r, &o, &TestClock::at(t)).await.is_err());
        assert!(blobs.contains(&orphan).await.unwrap());
        // Condemning alone needs no fence, and a dry run never collects.
        o.dry_run = true;
        run(&r, &o, &TestClock::at(t)).await.unwrap();
        o.dry_run = false;
        o.unsafe_no_fence = true;
        let report = run(&r, &o, &TestClock::at(t)).await.unwrap();
        assert_eq!(report.collect.deleted, 1);
    }

    #[tokio::test]
    async fn upload_waits_for_an_announced_delete() {
        let (r, blobs, orphan, t) = orphan_setup("announced").await;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        r.meta()
            .put_derived(BLOB_DOOM, &orphan, doom_value(t, Doom::Deleting))
            .await
            .unwrap();
        let up = {
            let r = r.clone();
            tokio::spawn(async move { r.put_blob(Bytes::from("announced")).await })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(
            !up.is_finished(),
            "upload must wait while a delete is announced"
        );
        // Collect saw the rescue and gave up.
        r.meta()
            .put_derived(BLOB_DOOM, &orphan, doom_value(t, Doom::Abandoned))
            .await
            .unwrap();
        assert_eq!(up.await.unwrap().unwrap(), orphan);
        assert!(blobs.contains(&orphan).await.unwrap());
    }

    type DeleteHook = Box<dyn Fn(Hash) + Send + Sync>;

    /// A blob store that lets a test act just before each delete, the moment
    /// after collect's final check.
    struct HookedBlobs {
        inner: Arc<MemoryBlobStore>,
        before_delete: Mutex<Option<DeleteHook>>,
    }

    #[async_trait]
    impl BlobStore for HookedBlobs {
        async fn put_with_hash(&self, hash: Hash, data: Bytes) -> Result<()> {
            self.inner.put_with_hash(hash, data).await
        }
        async fn get(&self, hash: &Hash) -> Result<Option<Bytes>> {
            self.inner.get(hash).await
        }
        async fn contains(&self, hash: &Hash) -> Result<bool> {
            self.inner.contains(hash).await
        }
        async fn list_page(&self, after: Option<Hash>, limit: usize) -> Result<Vec<BlobMeta>> {
            self.inner.list_page(after, limit).await
        }
        async fn delete(&self, hash: &Hash) -> Result<()> {
            let spawned = match self.before_delete.lock().unwrap().as_ref() {
                Some(f) => {
                    f(*hash);
                    true
                }
                None => false,
            };
            if spawned {
                // Let the spawned client run up to its first wait.
                tokio::task::yield_now().await;
            }
            self.inner.delete(hash).await
        }
    }

    /// A client that uploads and commits `data`, retrying like the CLI does
    /// when the server says a blob is missing.
    async fn client_flow(r: Repo, data: String, path: String) -> bool {
        for _ in 0..3 {
            let h = r.put_blob(Bytes::from(data.clone())).await.unwrap();
            let cs = client_commit(h, data.len() as u64, &path);
            match r.create_changeset(cs).await {
                Ok(_) => return true,
                Err(Error::Invalid(m)) if m.starts_with("missing blob") => continue,
                Err(e) => panic!("{e}"),
            }
        }
        false
    }

    fn hooked_repo() -> (Repo, Arc<HookedBlobs>, Arc<MemoryBlobStore>) {
        let mem = Arc::new(MemoryBlobStore::new());
        let hooked = Arc::new(HookedBlobs {
            inner: mem.clone(),
            before_delete: Mutex::new(None),
        });
        let repo = Repo::new(hooked.clone(), Arc::new(MemoryMetaStore::new()));
        (repo, hooked, mem)
    }

    /// No stored changeset may point at a blob or tree that is gone.
    async fn assert_no_dangling(r: &Repo) {
        let mut n = 0;
        let marked = mark(r, &mut n).await.unwrap();
        for h in marked {
            if h == TreeNode::empty_hash() {
                continue;
            }
            assert!(r.blobs().contains(&h).await.unwrap(), "dangling {h}");
        }
    }

    #[tokio::test]
    async fn upload_racing_the_final_delete_is_not_lost() {
        let (r, hooked, mem) = hooked_repo();
        let base = r.put_blob(Bytes::from("base")).await.unwrap();
        r.create_changeset(client_commit(base, 4, "base.txt"))
            .await
            .unwrap();
        let victim = mem.put(Bytes::from("victim")).await.unwrap();
        age_all(&mem).await;
        let t = now() + 1000;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();

        let handles = Arc::new(Mutex::new(Vec::new()));
        let (rr, hs) = (r.clone(), handles.clone());
        *hooked.before_delete.lock().unwrap() = Some(Box::new(move |h| {
            assert_eq!(h, victim);
            hs.lock().unwrap().push(tokio::spawn(client_flow(
                rr.clone(),
                "victim".into(),
                "late.txt".into(),
            )));
        }));
        let done = collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        // The upload arrived after collect's final check, so collect deleted
        // and the upload noticed and wrote the blob again.
        assert_eq!(done.deleted, 1);
        let hs: Vec<_> = handles.lock().unwrap().drain(..).collect();
        assert_eq!(hs.len(), 1);
        for h in hs {
            assert!(h.await.unwrap());
        }
        assert!(mem.contains(&victim).await.unwrap());
        assert_no_dangling(&r).await;
    }

    #[tokio::test]
    async fn acknowledged_upload_exists_after_a_racing_delete() {
        let (r, hooked, mem) = hooked_repo();
        let victim = mem.put(Bytes::from("victim")).await.unwrap();
        age_all(&mem).await;
        let t = now() + 1000;
        condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        let up = Arc::new(Mutex::new(None));
        let (rr, slot) = (r.clone(), up.clone());
        *hooked.before_delete.lock().unwrap() = Some(Box::new(move |_| {
            let r = rr.clone();
            *slot.lock().unwrap() = Some(tokio::spawn(async move {
                r.put_blob(Bytes::from("victim")).await
            }));
        }));
        collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        let handle = up.lock().unwrap().take().unwrap();
        assert_eq!(handle.await.unwrap().unwrap(), victim);
        assert!(mem.contains(&victim).await.unwrap());
        assert!(r.missing_blobs(&[victim]).await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn server_merge_may_reuse_a_condemned_blob() {
        let (r, blobs) = setup();
        let base = commit(&r, &[], &[("f.txt", Some("1\n2\n3\n"))]).await;
        let ours = commit(&r, &[base], &[("f.txt", Some("A\n2\n3\n"))]).await;
        let theirs = commit(&r, &[base], &[("f.txt", Some("1\n2\nC\n"))]).await;
        // An orphan that happens to hold the merge result.
        let orphan = blobs.put(Bytes::from("A\n2\nC\n")).await.unwrap();
        age_all(&blobs).await;
        let t = now() + 1000;
        let c = condemn(&r, &opts(1, false), &TestClock::at(t))
            .await
            .unwrap();
        assert_eq!(c.condemned, 1);
        let m = merge(&r, ours, theirs).await;
        let (_, data) = r.read_file(m, &rp("f.txt")).await.unwrap().unwrap();
        assert_eq!(&data[..], b"A\n2\nC\n");
        let done = collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
            .await
            .unwrap();
        assert_eq!(done.deleted, 0);
        assert!(blobs.contains(&orphan).await.unwrap());
        assert_no_dangling(&r).await;
    }

    struct Rng(u64);

    impl Rng {
        fn next(&mut self) -> u64 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            self.0
        }

        fn below(&mut self, n: usize) -> usize {
            (self.next() % n as u64) as usize
        }
    }

    /// Random interleavings of uploads, commits, dedup checks, condemnation
    /// and collection, with clients racing the delete itself. No changeset
    /// that was stored may ever lose a blob.
    #[tokio::test]
    async fn random_interleavings_never_lose_a_referenced_blob() {
        for seed in 1..=150u64 {
            let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
            let (r, hooked, mem) = hooked_repo();
            let names: Vec<String> = (0..6).map(|i| format!("blob {seed} {i}")).collect();
            let mut paths = 0;
            let mut pending: Vec<Changeset> = Vec::new();
            let mut clients: Vec<tokio::task::JoinHandle<bool>> = Vec::new();

            // Some blobs start as live data, some as aged orphans.
            for n in &names {
                match rng.below(3) {
                    0 => {
                        let h = r.put_blob(Bytes::from(n.clone())).await.unwrap();
                        paths += 1;
                        let cs = client_commit(h, n.len() as u64, &format!("seed{paths}"));
                        r.create_changeset(cs).await.unwrap();
                    }
                    1 => {
                        mem.put(Bytes::from(n.clone())).await.unwrap();
                    }
                    _ => {}
                }
            }
            age_all(&mem).await;

            let mut t = now() + 1000;

            // Phase 1: traffic before the condemnation.
            for _ in 0..(2 + rng.below(8)) {
                let n = names[rng.below(names.len())].clone();
                let h = Hash::of_blob(n.as_bytes());
                paths += 1;
                match rng.below(4) {
                    0 => {
                        r.put_blob(Bytes::from(n)).await.unwrap();
                    }
                    1 => {
                        r.missing_blobs(&[h]).await.unwrap();
                    }
                    2 => {
                        // Begin a commit: the check passes now, the store comes later.
                        let cs = client_commit(h, n.len() as u64, &format!("q{paths}"));
                        if r.check_reusable(&h, "x").await.is_ok()
                            && r.blobs().contains(&h).await.unwrap()
                        {
                            pending.push(cs);
                        }
                    }
                    _ => {
                        if let Some(cs) = pending.pop() {
                            r.store_server_changeset(cs).await.unwrap();
                        }
                    }
                }
            }
            condemn(&r, &opts(1, false), &TestClock::at(t))
                .await
                .unwrap();
            t += 1;

            // Phase 2: traffic during the fence.
            for _ in 0..(2 + rng.below(8)) {
                let n = names[rng.below(names.len())].clone();
                let h = Hash::of_blob(n.as_bytes());
                paths += 1;
                match rng.below(4) {
                    0 => {
                        r.put_blob(Bytes::from(n)).await.unwrap();
                    }
                    1 => {
                        r.missing_blobs(&[h]).await.unwrap();
                    }
                    2 => {
                        let cs = client_commit(h, n.len() as u64, &format!("r{paths}"));
                        // Checked like the server does; may be rejected.
                        let _ = r.create_changeset(cs).await;
                    }
                    _ => {
                        // Requests that began before the condemnation finish
                        // inside the fence.
                        if let Some(cs) = pending.pop() {
                            r.store_server_changeset(cs).await.unwrap();
                        }
                    }
                }
            }
            // The fence guarantees these are done before collect starts.
            for cs in pending.drain(..) {
                r.store_server_changeset(cs).await.unwrap();
            }

            // Phase 3: collect, with clients arriving at the final delete.
            let (rr, hs) = (r.clone(), Arc::new(Mutex::new(Vec::new())));
            let hs2 = hs.clone();
            let flows = rng.below(3) + 1;
            let names2 = names.clone();
            let pick = rng.below(names.len());
            *hooked.before_delete.lock().unwrap() = Some(Box::new(move |h| {
                for k in 0..flows {
                    let n = if k == 0 {
                        names2
                            .iter()
                            .find(|n| Hash::of_blob(n.as_bytes()) == h)
                            .cloned()
                    } else {
                        Some(names2[pick].clone())
                    };
                    if let Some(n) = n {
                        hs2.lock().unwrap().push(tokio::spawn(client_flow(
                            rr.clone(),
                            n,
                            format!("flow{k}-{h}"),
                        )));
                    }
                }
            }));
            collect(&r, &opts(1, false), &TestClock::at(t + FENCE))
                .await
                .unwrap();
            clients.extend(hs.lock().unwrap().drain(..));
            for c in clients {
                assert!(c.await.unwrap(), "seed {seed}: client gave up");
            }
            assert_no_dangling(&r).await;
        }
    }
}
