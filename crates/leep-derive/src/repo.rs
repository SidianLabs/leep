//! The [`Repo`] handle and changeset creation.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use leep_core::api::ChangesetInfo;
use leep_core::prefix::IdPrefix;
use leep_core::refname;
use leep_core::{
    BlobStore, Changeset, ChangesetId, DerivedRow, Error, FileChange, Hash, MetaStore, RepoPath,
    Result, TreeNode,
};

use crate::derived::{
    self,
    history_pack::{Part, RawNode},
    DerivedKind, Generation, LastMerge, NodeRef, RootTree, SkipEntry, Skiplist, StoredHistoryNode,
};
use crate::signatures::{LandPolicy, PolicySet};
use crate::tree_ops::{apply_changes, TreeView};

/// Longest path, in bytes, a client changeset may name (Linux's `PATH_MAX`).
pub const MAX_PATH_BYTES: usize = 4096;
/// Most distinct directories a client changeset's paths may name, and their
/// total length. Building the new tree keeps every changed directory and its
/// full path in memory, so unbounded paths cost quadratic memory: one request
/// with a few megabytes of `a/a/a/...` could exhaust the server.
const MAX_CHANGED_DIRS: usize = 1_000_000;
const MAX_CHANGED_DIR_BYTES: usize = 256 << 20;

/// Refuse paths that would make tree building blow up (see the constants).
fn check_path_bounds<'a>(paths: impl Iterator<Item = &'a RepoPath>) -> Result<()> {
    let mut dirs = HashSet::new();
    let mut bytes = 0usize;
    for path in paths {
        let s = path.as_str();
        if s.len() > MAX_PATH_BYTES {
            return Err(Error::Invalid(format!(
                "path of {} bytes; the limit is {MAX_PATH_BYTES}",
                s.len()
            )));
        }
        for (i, _) in s.match_indices('/') {
            if dirs.insert(&s[..i]) {
                bytes += i;
                if dirs.len() > MAX_CHANGED_DIRS || bytes > MAX_CHANGED_DIR_BYTES {
                    return Err(Error::Invalid(format!(
                        "changeset touches too many directories (limit {MAX_CHANGED_DIRS} \
                         directories or {MAX_CHANGED_DIR_BYTES} bytes of directory names)"
                    )));
                }
            }
        }
    }
    Ok(())
}

/// What [`Repo::read_create_inputs`] found, one slot per parent or content.
/// `None` means not supplied: the caller reads or derives it singly.
struct CreateInputs {
    parents_stored: Option<Vec<bool>>,
    condemned: Option<Vec<Option<Bytes>>>,
    parent_generations: Vec<Option<u64>>,
    base_tree: Option<Hash>,
    parent_skiplist: Option<Arc<[SkipEntry]>>,
    /// The first parent's changeset, when it was read.
    first_parent: Option<Changeset>,
    /// The `last_merge-v1` values of the first parent and of its first
    /// parent, when stored.
    last_merges: [Option<u64>; 2],
}

/// A repository: content-addressed blobs plus changeset metadata.
#[derive(Clone)]
pub struct Repo {
    blobs: Arc<dyn BlobStore>,
    meta: Arc<dyn MetaStore>,
    /// Per-ref queues that group concurrent lands into batches (see `land`).
    pub(crate) land_queues: Arc<crate::land::LandQueues>,
    pub(crate) land_policy: Option<Arc<dyn LandPolicy>>,
    /// Batches this server's lands with every other server's on the same ref
    /// (ADR 0056); `None` batches only the lands this process receives.
    pub(crate) land_coordinator: Option<Arc<crate::land_coord::LandCoordinator>>,
    /// Parents and generations of changesets seen by history walks.
    pub(crate) ancestry: Arc<AncestryCache>,
    /// Decoded path-history nodes, keyed by `NodeRef::cache_key` (see
    /// `read_history_row`).
    pub(crate) history_rows: Arc<HistoryRowCache>,
    /// Decoded patches that still name their keyframe's entries by
    /// position: a node evicted from `history_rows` is named again from here
    /// without decoding its part.
    pub(crate) history_raw: Arc<HistoryRawCache>,
    /// Nodes of packs derived but not yet stored (see `stage_history_pack`).
    pub(crate) history_staged: Arc<Mutex<StagedNodes>>,
    /// Parts of packs read, by row key, whose nodes are decoded when asked
    /// for (see `read_history_row`).
    pub(crate) history_parts: Arc<HistoryPartCache>,
    /// The root history node of changesets whose pack was read, by id: a
    /// path lookup at a changeset would otherwise read its value each time.
    pub(crate) history_roots: Arc<HistoryRootCache>,
    /// Tree nodes viewed for lookups (see `read_tree_view`).
    pub(crate) trees: Arc<TreeCache>,
    /// Encoded `tree-trigrams-v1` rows read from the store by searches
    /// (see `Repo::search_tree_rows`).
    pub(crate) search_trees: Arc<SearchTreeCache>,
    /// Skiplists computed, or read while creating changesets (see
    /// `SkiplistCache`).
    pub(crate) skiplists: Arc<SkiplistCache>,
    /// Decoded `log-filter-v1` rows and author names (see `filter_row`).
    pub(crate) log_filter: Arc<crate::derived::LogFilterCaches>,
    /// One lock per derived kind, held while a missing range is derived (see
    /// `derived::derive`).
    pub(crate) derive_locks: Arc<Mutex<HashMap<&'static str, DeriveLock>>>,
    /// Values of staged kinds computed but not stored yet, by kind and
    /// changeset (see `derived::derive`).
    pub(crate) staged_values: Arc<Mutex<HashMap<(&'static str, Hash), Bytes>>>,
    /// Changesets a derivation of a missing range works on at once (ADR
    /// 0050).
    derive_jobs: usize,
    /// Values computed by `derived::derive`, any kind: what `repair
    /// rederive --missing` reports, and tests check for duplicated work.
    pub(crate) derive_calls: Arc<std::sync::atomic::AtomicUsize>,
    /// What queries do about lagging indexes (ADR 0058); `None` is `local`
    /// mode.
    pub(crate) index_policy: Option<Arc<crate::indexing::IndexPolicy>>,
    /// Set on the handle of one bounded query: the changesets it examines
    /// directly, and that it may not derive an index range.
    pub(crate) bounded: Option<Arc<crate::indexing::bounded::Bounded>>,
    /// Asked before each write of a derivation: the warmer's lease (ADR
    /// 0058).
    pub(crate) write_guard: Option<Arc<dyn WriteGuard>>,
    /// The last read of `gc-fence-active-v1` (ADR 0059), shared by clones.
    pub(crate) fence_flag: Arc<crate::changeset_gc::FenceFlag>,
    /// Whether writes through this handle rescue the changesets they name
    /// (ADR 0059). Off for a land batch's staged handle, whose batch rescues
    /// before it moves the ref.
    pub(crate) rescues_on_write: bool,
}

/// Whether a derivation may go on writing rows: the index lease it derives
/// under is still held.
#[async_trait::async_trait]
pub(crate) trait WriteGuard: Send + Sync {
    async fn allowed(&self) -> bool;
}

type DeriveLock = Arc<tokio::sync::Mutex<()>>;

/// Parents and generation of changesets, kept in memory because history
/// walks (`is_ancestor`, `merge_base`) visit thousands of changesets and would
/// otherwise make a metadata round trip, and decode a whole changeset, per
/// step. Both are immutable, so entries never go stale. Bounded: entries live
/// in two generations of maps, and the older one is dropped when the newer
/// fills, so recently used entries survive.
pub(crate) struct AncestryCache {
    inner: Mutex<AncestryMaps>,
    /// Entries per map.
    limit: usize,
}

impl Default for AncestryCache {
    fn default() -> Self {
        Self::scaled(CacheScale::default())
    }
}

#[derive(Default)]
struct AncestryMaps {
    new: HashMap<ChangesetId, AncestryEntry, ByKey>,
    old: HashMap<ChangesetId, AncestryEntry, ByKey>,
}

/// Hashing for maps keyed by content hashes (changeset ids, row and node
/// addresses): they are uniform already, and SipHash over 32 bytes was a
/// visible part of path-history derivation, which looks them up constantly.
pub(crate) type ByKey = std::hash::BuildHasherDefault<KeyHasher>;

/// See [`ByKey`]: an FxHash-style mix of the bytes written.
#[derive(Default)]
pub(crate) struct KeyHasher(u64);

impl std::hash::Hasher for KeyHasher {
    fn finish(&self) -> u64 {
        self.0
    }

    fn write(&mut self, bytes: &[u8]) {
        for chunk in bytes.chunks(8) {
            let mut word = [0u8; 8];
            word[..chunk.len()].copy_from_slice(chunk);
            self.0 = (self.0.rotate_left(5) ^ u64::from_le_bytes(word))
                .wrapping_mul(0x51_7c_c1_b7_27_22_0a_95);
        }
    }
}

#[derive(Clone, Default)]
struct AncestryEntry {
    parents: Option<Arc<[ChangesetId]>>,
    generation: Option<u64>,
    /// Root tree, read ahead for path-history derivation (see
    /// `Repo::root_tree_prefetched`).
    root: Option<Hash>,
}

/// Entries per map; two maps at most, so about 2 * this many changesets.
const ANCESTRY_CACHE_ENTRIES: usize = 1 << 18;

impl AncestryCache {
    fn scaled(s: CacheScale) -> Self {
        AncestryCache {
            inner: Mutex::default(),
            limit: s.of(ANCESTRY_CACHE_ENTRIES),
        }
    }

    /// `f` of the entry of `id` (an empty one if absent). Walks ask for one
    /// field of each changeset they visit, so the entry is not cloned.
    fn get<T>(&self, id: &ChangesetId, f: impl FnOnce(&AncestryEntry) -> T) -> T {
        let mut m = self.inner.lock().unwrap();
        if let Some(e) = m.new.get(id) {
            return f(e);
        }
        match m.old.remove(id) {
            Some(e) => {
                let out = f(&e);
                m.insert(*id, e, self.limit);
                out
            }
            None => f(&AncestryEntry::default()),
        }
    }

    fn update(&self, id: ChangesetId, f: impl FnOnce(&mut AncestryEntry)) {
        let mut m = self.inner.lock().unwrap();
        let mut e = m
            .new
            .remove(&id)
            .or_else(|| m.old.remove(&id))
            .unwrap_or_default();
        f(&mut e);
        m.insert(id, e, self.limit);
    }
}

impl AncestryMaps {
    fn insert(&mut self, id: ChangesetId, e: AncestryEntry, limit: usize) {
        if self.new.len() >= limit {
            self.old = std::mem::take(&mut self.new);
        }
        self.new.insert(id, e);
    }
}

/// Skiplists by changeset. A new changeset's list is built from the lists of
/// its first-parent ancestors 1, 2, 4, ... steps up, one after another, so
/// without this every commit would make about log2(history) sequential reads.
/// The cache holds lists this process computed, and rows read while creating
/// a changeset. Verification and repair never create changesets, and they
/// substitute recomputed rows for damaged ones as they go, so their caches
/// hold only computed lists and never a damaged row read before its
/// substitution. Bounded like [`AncestryCache`]; a list is 40 bytes per
/// entry.
pub(crate) struct SkiplistCache {
    inner: Mutex<SkiplistMaps>,
    /// Lists per map.
    limit: usize,
}

impl Default for SkiplistCache {
    fn default() -> Self {
        Self::scaled(CacheScale::default())
    }
}

#[derive(Default)]
struct SkiplistMaps {
    new: HashMap<ChangesetId, Arc<[SkipEntry]>>,
    old: HashMap<ChangesetId, Arc<[SkipEntry]>>,
}

/// Lists per map, so at most 2 * this many changesets: about 25 MiB at a
/// depth of 100,000 (17 entries a list).
const SKIPLIST_CACHE_ENTRIES: usize = 1 << 14;

impl SkiplistCache {
    fn scaled(s: CacheScale) -> Self {
        SkiplistCache {
            inner: Mutex::default(),
            limit: s.of(SKIPLIST_CACHE_ENTRIES),
        }
    }

    pub(crate) fn get(&self, id: &ChangesetId) -> Option<Arc<[SkipEntry]>> {
        let mut m = self.inner.lock().unwrap();
        if let Some(l) = m.new.get(id) {
            return Some(l.clone());
        }
        let l = m.old.remove(id)?;
        m.insert(*id, l.clone(), self.limit);
        Some(l)
    }

    pub(crate) fn insert(&self, id: ChangesetId, list: Arc<[SkipEntry]>) {
        self.inner.lock().unwrap().insert(id, list, self.limit);
    }
}

impl SkiplistMaps {
    fn insert(&mut self, id: ChangesetId, list: Arc<[SkipEntry]>, limit: usize) {
        if self.new.len() >= limit {
            self.old = std::mem::take(&mut self.new);
        }
        self.new.insert(id, list);
    }
}

/// Decoded path-history nodes (ADR 0045), patches with their entries named.
/// A split node is read one chunk at a time, a patch with its keyframe, and
/// derivation reads the same root and directory nodes for every changeset,
/// so without this each changeset would re-read and re-decode dozens of
/// parts. A pack is a function of its changeset and parts are checked
/// against their checksum before they are cached, so entries never go stale.
pub(crate) type HistoryRowCache = SizedCache<StoredHistoryNode, HISTORY_CACHE_BYTES>;

/// Decoded patches waiting for their keyframe (see `Repo::history_raw`).
pub(crate) type HistoryRawCache = SizedCache<RawNode, HISTORY_RAW_CACHE_BYTES>;

/// Bytes [`HistoryRawCache`] may hold.
const HISTORY_RAW_CACHE_BYTES: usize = 16 << 20;

/// Parts of packs as read (see `Repo::history_parts`).
pub(crate) type HistoryPartCache = SizedCache<Part, HISTORY_PART_CACHE_BYTES>;

/// Root nodes by changeset (see `Repo::history_roots`).
pub(crate) type HistoryRootCache = SizedCache<NodeRef, HISTORY_ROOT_CACHE_BYTES>;

/// Bytes [`HistoryPartCache`] may hold: parts uncompressed, each at most
/// 16 KB, so a few thousand.
const HISTORY_PART_CACHE_BYTES: usize = 32 << 20;

/// Bytes [`HistoryRootCache`] may hold, at 64 per root: about 65,000 roots.
const HISTORY_ROOT_CACHE_BYTES: usize = 4 << 20;

/// Nodes of staged packs by cache key, and the keys of each part's nodes by
/// the part's row key, to forget them once the part is stored.
#[derive(Default)]
pub(crate) struct StagedNodes {
    pub(crate) nodes: HashMap<Hash, Arc<StoredHistoryNode>>,
    pub(crate) by_row: HashMap<Hash, Vec<Hash>>,
}

/// Tree nodes by hash, viewed for lookups, for path-history derivation: each
/// changeset reads the directories its parent's derivation just read (the
/// parent's new tree is its old one), and a node read from a pack decodes a
/// delta chain first. Checked against their hash before they are cached.
pub(crate) type TreeCache = SizedCache<TreeView, TREE_CACHE_BYTES>;

/// Directory filters (`tree-trigrams-v1` rows) as stored, by tree hash. A
/// search reads the filter of every directory it opens, and the upper levels
/// are the same for every query and for every revision that did not change
/// them, so without this each search moves most of its index bytes from the
/// store again. Rows are keyed by content and the kind name carries the
/// encoding's version, so an entry is never stale for a correct row; one
/// damaged in the store and then repaired by another process stays cached
/// until this process restarts (OPERATIONS.md, "Repair").
pub(crate) type SearchTreeCache = SizedCache<Bytes, SEARCH_TREE_CACHE_BYTES>;

/// Bytes [`SearchTreeCache`] may hold: about 1,000 dense 64 KiB filters, or
/// many more sparse ones, which covers the directories one search over a
/// Linux-sized tree opens.
const SEARCH_TREE_CACHE_BYTES: usize = 64 << 20;

/// Content-addressed values by hash, bounded like [`AncestryCache`] but by
/// bytes: each map holds up to half of the budget, `BUDGET` unless the
/// `Repo`'s cache budget scales it.
pub(crate) struct SizedCache<V, const BUDGET: usize> {
    inner: Mutex<SizedMaps<V>>,
    budget: usize,
}

impl<V, const BUDGET: usize> Default for SizedCache<V, BUDGET> {
    fn default() -> Self {
        Self::scaled(CacheScale::default())
    }
}

struct SizedMaps<V> {
    new: HashMap<Hash, (Arc<V>, usize), ByKey>,
    new_bytes: usize,
    old: HashMap<Hash, (Arc<V>, usize), ByKey>,
}

/// Bytes the history row cache may hold, counting each node at about its
/// decoded size (`history_pack::cache_size`). v3 counted rows at twice their
/// encoded size under 64 MiB, which undercounted decoded rows by about 1.7x.
const HISTORY_CACHE_BYTES: usize = 96 << 20;

/// Bytes the tree cache may hold, counted like [`HISTORY_CACHE_BYTES`].
const TREE_CACHE_BYTES: usize = 32 << 20;

impl<V, const BUDGET: usize> SizedCache<V, BUDGET> {
    pub(crate) fn scaled(s: CacheScale) -> Self {
        SizedCache {
            inner: Mutex::new(SizedMaps {
                new: HashMap::default(),
                new_bytes: 0,
                old: HashMap::default(),
            }),
            budget: s.of(BUDGET),
        }
    }

    pub(crate) fn get(&self, hash: &Hash) -> Option<Arc<V>> {
        let mut m = self.inner.lock().unwrap();
        if let Some((n, _)) = m.new.get(hash) {
            return Some(n.clone());
        }
        let (n, size) = m.old.remove(hash)?;
        m.insert(*hash, n.clone(), size, self.budget);
        Some(n)
    }

    pub(crate) fn insert(&self, hash: Hash, value: Arc<V>, encoded_len: usize) {
        self.inner
            .lock()
            .unwrap()
            .insert(hash, value, 2 * encoded_len, self.budget);
    }

    /// [`SizedCache::insert`] of a value that holds `size` bytes in all, for
    /// values whose size is known rather than estimated from an encoding.
    /// A value over half the budget is not kept, so the cache never holds
    /// more than its budget.
    pub(crate) fn insert_sized(&self, hash: Hash, value: Arc<V>, size: usize) {
        if size > self.budget / 2 {
            return;
        }
        self.inner
            .lock()
            .unwrap()
            .insert(hash, value, size, self.budget);
    }

    /// Bytes the entries hold, as counted when they were inserted.
    #[cfg(test)]
    pub(crate) fn held_bytes(&self) -> usize {
        let m = self.inner.lock().unwrap();
        m.new.values().chain(m.old.values()).map(|(_, s)| s).sum()
    }

    #[cfg(test)]
    pub(crate) fn budget(&self) -> usize {
        self.budget
    }

    /// Every entry, with the size it was counted at.
    #[cfg(test)]
    pub(crate) fn entries(&self) -> Vec<(Hash, Arc<V>, usize)> {
        let m = self.inner.lock().unwrap();
        m.new
            .iter()
            .chain(m.old.iter())
            .map(|(h, (v, s))| (*h, v.clone(), *s))
            .collect()
    }
}

impl<V> SizedMaps<V> {
    fn insert(&mut self, hash: Hash, value: Arc<V>, size: usize, budget: usize) {
        if self.new_bytes + size > budget / 2 {
            self.old = std::mem::take(&mut self.new);
            self.new_bytes = 0;
        }
        if self.new.insert(hash, (value, size)).is_none() {
            self.new_bytes += size;
        }
    }
}

/// Bytes the byte-bounded derive caches of one [`Repo`] hold by default:
/// path-history nodes, parts, roots and patches, tree nodes, search's
/// directory filters, and the filtered-log caches. See
/// [`Repo::with_cache_budget`].
pub const DEFAULT_CACHE_BYTES: usize = HISTORY_CACHE_BYTES
    + HISTORY_RAW_CACHE_BYTES
    + HISTORY_PART_CACHE_BYTES
    + HISTORY_ROOT_CACHE_BYTES
    + TREE_CACHE_BYTES
    + SEARCH_TREE_CACHE_BYTES
    + crate::derived::LOG_FILTER_CACHE_BYTES;

/// Every cache's default bound times `total / DEFAULT_CACHE_BYTES`, so the
/// default budget gives exactly the default bounds.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CacheScale {
    total: usize,
}

impl Default for CacheScale {
    fn default() -> Self {
        CacheScale {
            total: DEFAULT_CACHE_BYTES,
        }
    }
}

impl CacheScale {
    pub(crate) fn of(self, default: usize) -> usize {
        (default as u128 * self.total as u128 / DEFAULT_CACHE_BYTES as u128) as usize
    }
}

/// Jobs a derivation uses unless told otherwise: the cores the process may
/// use, up to [`MAX_DEFAULT_DERIVE_JOBS`].
pub fn default_derive_jobs() -> usize {
    std::thread::available_parallelism()
        .map_or(1, |n| n.get())
        .min(MAX_DEFAULT_DERIVE_JOBS)
}

/// Past this many, more jobs mostly wait on the metadata store's connection
/// pool (16 on Postgres) and lock each other out of the shared caches.
pub const MAX_DEFAULT_DERIVE_JOBS: usize = 8;

impl Repo {
    pub fn new(blobs: Arc<dyn BlobStore>, meta: Arc<dyn MetaStore>) -> Self {
        Repo {
            blobs,
            meta,
            land_queues: Arc::default(),
            land_policy: None,
            land_coordinator: None,
            ancestry: Arc::default(),
            history_rows: Arc::default(),
            history_raw: Arc::default(),
            history_staged: Arc::default(),
            history_parts: Arc::default(),
            history_roots: Arc::default(),
            trees: Arc::default(),
            search_trees: Arc::default(),
            skiplists: Arc::default(),
            log_filter: Arc::default(),
            derive_locks: Arc::default(),
            staged_values: Arc::default(),
            derive_jobs: default_derive_jobs(),
            derive_calls: Arc::default(),
            index_policy: None,
            bounded: None,
            write_guard: None,
            fence_flag: Arc::default(),
            rescues_on_write: true,
        }
    }

    /// Bound queries by `policy` (ADR 0058, modes `lease` and `off`).
    pub fn with_index_policy(mut self, policy: Arc<crate::indexing::IndexPolicy>) -> Self {
        self.index_policy = Some(policy);
        self
    }

    /// See [`Repo::with_index_policy`].
    pub fn index_policy(&self) -> Option<&Arc<crate::indexing::IndexPolicy>> {
        self.index_policy.as_ref()
    }

    /// A handle whose derivations stop writing once `guard` says so.
    pub(crate) fn with_write_guard(&self, guard: Arc<dyn WriteGuard>) -> Self {
        Repo {
            write_guard: Some(guard),
            ..self.clone()
        }
    }

    /// Whether a derivation through this handle may write.
    pub(crate) async fn write_allowed(&self) -> bool {
        match &self.write_guard {
            Some(g) => g.allowed().await,
            None => true,
        }
    }

    /// Consult `policy` before every `land`. Called again, it adds to the
    /// policies already set; all must pass.
    pub fn with_land_policy(mut self, policy: Arc<dyn LandPolicy>) -> Self {
        self.land_policy = Some(match self.land_policy.take() {
            None => policy,
            Some(prev) => Arc::new(PolicySet::new(vec![prev, policy])),
        });
        self
    }

    /// Coordinate lands with other servers through `coordinator`'s leases
    /// (ADR 0056), for callers of [`Repo::land_coordinated`].
    pub fn with_land_coordinator(
        mut self,
        coordinator: Arc<crate::land_coord::LandCoordinator>,
    ) -> Self {
        self.land_coordinator = Some(coordinator);
        self
    }

    /// See [`Repo::with_land_coordinator`].
    pub fn land_coordinator(&self) -> Option<&Arc<crate::land_coord::LandCoordinator>> {
        self.land_coordinator.as_ref()
    }

    /// Derive missing ranges `jobs` changesets at a time (at least one).
    /// Rows are the same for any number; more jobs use more cores and more
    /// concurrent store requests.
    pub fn with_derive_jobs(mut self, jobs: usize) -> Self {
        self.derive_jobs = jobs.max(1);
        self
    }

    /// Bound this handle's derive caches by about `bytes` in total instead
    /// of [`DEFAULT_CACHE_BYTES`]: each byte-bounded cache gets its default
    /// share, and the ancestry and skiplist caches, bounded by entries
    /// (on the order of 100 MB together at their defaults), scale by the
    /// same factor.
    /// Call it before cloning: it replaces the caches, which clones share.
    pub fn with_cache_budget(mut self, bytes: usize) -> Self {
        let s = CacheScale { total: bytes };
        self.ancestry = Arc::new(AncestryCache::scaled(s));
        self.history_rows = Arc::new(SizedCache::scaled(s));
        self.history_raw = Arc::new(SizedCache::scaled(s));
        self.history_parts = Arc::new(SizedCache::scaled(s));
        self.history_roots = Arc::new(SizedCache::scaled(s));
        self.trees = Arc::new(SizedCache::scaled(s));
        self.search_trees = Arc::new(SizedCache::scaled(s));
        self.skiplists = Arc::new(SkiplistCache::scaled(s));
        self.log_filter = Arc::new(crate::derived::LogFilterCaches::scaled(s));
        self
    }

    /// A handle on the same stores whose `log-filter-v1` merge walks never
    /// give up (ADR 0051), with caches of its own; for `verify`.
    pub(crate) fn with_unbounded_side_walks(&self) -> Self {
        let caches = crate::derived::LogFilterCaches::default();
        caches.set_side_budget(usize::MAX);
        Repo {
            log_filter: Arc::new(caches),
            ..self.clone()
        }
    }

    /// Values `derived::derive` computed through this handle and its clones.
    pub(crate) fn derived_count(&self) -> u64 {
        self.derive_calls.load(std::sync::atomic::Ordering::Relaxed) as u64
    }

    /// See [`Repo::with_derive_jobs`].
    pub fn derive_jobs(&self) -> usize {
        self.derive_jobs
    }

    /// The land policy in force, if any.
    pub fn land_policy(&self) -> Option<&Arc<dyn LandPolicy>> {
        self.land_policy.as_ref()
    }

    /// A handle on the same stores with `policy` instead of whatever was set.
    /// A server that reloads its policy gives each request a handle with the
    /// snapshot the request started under, so one `land` or ref move never
    /// sees two policies. Land locks stay shared with `self`.
    pub fn with_land_policy_replaced(&self, policy: Option<Arc<dyn LandPolicy>>) -> Self {
        Repo {
            land_policy: policy,
            ..self.clone()
        }
    }

    /// A handle on other stores sharing this one's caches, queues and policy:
    /// the staged stores of a land batch (ADR 0043). The caches hold only
    /// facts fixed by content, true whether or not the staged writes are
    /// ever stored.
    pub(crate) fn with_stores(&self, blobs: Arc<dyn BlobStore>, meta: Arc<dyn MetaStore>) -> Self {
        Repo {
            blobs,
            meta,
            ..self.clone()
        }
    }

    pub fn blobs(&self) -> &Arc<dyn BlobStore> {
        &self.blobs
    }

    pub fn meta(&self) -> &Arc<dyn MetaStore> {
        &self.meta
    }

    /// Validate and store a changeset, deriving its tree and generation.
    /// Idempotent. A blob that garbage collection has condemned counts as
    /// missing, so the client uploads it again (ADR 0012).
    pub async fn create_changeset(&self, cs: Changeset) -> Result<ChangesetId> {
        self.store_changeset(cs, true, Vec::new()).await
    }

    /// [`Repo::create_changeset`], also writing `rows` together with the
    /// changeset (in the same atomic write on stores that have one, after it
    /// otherwise), only if the changeset is valid. For rows that must never
    /// exist without it, such as the git bridge's commit mapping.
    pub async fn create_changeset_with_rows(
        &self,
        cs: Changeset,
        rows: Vec<DerivedRow>,
    ) -> Result<ChangesetId> {
        self.store_changeset(cs, true, rows).await
    }

    /// Like [`Repo::create_changeset`] for changesets the server builds from
    /// stored changesets (merge, rebase): their blobs are referenced by a
    /// stored changeset, so GC keeps them, or were just written through
    /// [`Repo::put_blob`], so a condemnation is already answered.
    pub(crate) async fn store_server_changeset(&self, cs: Changeset) -> Result<ChangesetId> {
        self.store_changeset(cs, false, Vec::new()).await
    }

    /// [`Repo::store_server_changeset`] writing `rows` with the changeset, as
    /// in [`Repo::create_changeset_with_rows`].
    pub(crate) async fn store_server_changeset_with_rows(
        &self,
        cs: Changeset,
        rows: Vec<DerivedRow>,
    ) -> Result<ChangesetId> {
        self.store_changeset(cs, false, rows).await
    }

    /// Validate `cs`, build its tree, and store it with its root tree,
    /// generation and skiplist rows and `extra` in one write (ADR 0031). What
    /// it reads first (parents, their rows, blob condemnations) is read in one
    /// round of batched queries; anything those miss is derived as before.
    /// Condemned parents are rescued before they are read (ADR 0059).
    async fn store_changeset(
        &self,
        cs: Changeset,
        client_blobs: bool,
        extra: Vec<DerivedRow>,
    ) -> Result<ChangesetId> {
        let id = cs.id();
        if client_blobs {
            check_path_bounds(cs.changes.keys())?;
        }
        self.rescue_changesets(&cs.parents, crate::changeset_gc::RescueBy::Create)
            .await?;
        let mut seen = HashSet::new();
        let contents: Vec<(&RepoPath, Hash, u64)> = cs
            .changes
            .iter()
            .filter_map(|(path, change)| match change {
                FileChange::Modify { content, size, .. } if seen.insert(*content) => {
                    Some((path, *content, *size))
                }
                _ => None,
            })
            .collect();
        let pre = self.read_create_inputs(&cs, &contents, client_blobs).await;
        for (i, p) in cs.parents.iter().enumerate() {
            let stored = match &pre.parents_stored {
                Some(s) => s[i],
                None => self.meta.get_changeset(p).await?.is_some(),
            };
            if !stored {
                return Err(Error::Invalid(format!("unknown parent {p}")));
            }
        }
        for (i, (path, content, size)) in contents.iter().enumerate() {
            let maybe_condemned = match &pre.condemned {
                Some(c) => c[i].is_some(),
                None => true,
            };
            if client_blobs && maybe_condemned {
                self.check_reusable(content, path.as_str()).await?;
            }
            match self.blobs.get(content).await? {
                None => return Err(Error::Invalid(format!("missing blob {content} for {path}"))),
                Some(b) if b.len() as u64 != *size => {
                    return Err(Error::Invalid(format!(
                        "size mismatch for {path}: declared {size}, blob is {}",
                        b.len()
                    )))
                }
                Some(_) => {}
            }
            // `get` may be served from a cache tier; existence must come from
            // durable storage, which GC can change from another process.
            if !self.blobs.contains(content).await? {
                return Err(Error::Invalid(format!("missing blob {content} for {path}")));
            }
        }
        let base = match (cs.parents.first(), pre.base_tree) {
            (Some(_), Some(t)) => t,
            (Some(p), None) => self.root_tree(*p).await?,
            (None, _) => TreeNode::empty_hash(),
        };
        // The changeset makes its blobs reachable, so they must survive a
        // power loss before it is stored (ADR 0044): `apply_changes` flushes
        // the tree nodes it writes and every content the changeset names,
        // which may have been uploaded to another process or before a
        // restart, after the checks above found them.
        let tree = apply_changes(self, base, &cs.changes).await?;

        let started = std::time::Instant::now();
        let mut parent_generations = Vec::with_capacity(cs.parents.len());
        for (p, g) in cs.parents.iter().zip(&pre.parent_generations) {
            parent_generations.push(match g {
                Some(g) => *g,
                None => self.generation(*p).await?,
            });
        }
        let generation = parent_generations.iter().max().map_or(1, |g| g + 1);
        derived::record_derivation(Generation::NAME, started);
        let last_merge = Self::write_path_last_merge(&cs, generation, &parent_generations, &pre);
        let started = std::time::Instant::now();
        let skiplist: Arc<[SkipEntry]> = match cs.parents.first() {
            Some(&p) => {
                let parent = SkipEntry {
                    id: p,
                    generation: parent_generations[0],
                };
                if let Some(l) = &pre.parent_skiplist {
                    self.skiplists.insert(p, l.clone());
                }
                Skiplist::compute(self, parent, pre.parent_skiplist, true).await?
            }
            None => Arc::from([]),
        };
        derived::record_derivation(Skiplist::NAME, started);
        let mut rows = vec![
            DerivedRow::new(
                RootTree::NAME_STR,
                id.0,
                Bytes::copy_from_slice(tree.as_bytes()),
            ),
            DerivedRow::new(
                Generation::NAME,
                id.0,
                Bytes::copy_from_slice(&generation.to_be_bytes()),
            ),
            DerivedRow::new(Skiplist::NAME, id.0, Skiplist::encode(&skiplist)),
        ];
        match last_merge {
            Some(v) => rows.push(DerivedRow::new(
                LastMerge::NAME,
                id.0,
                Bytes::copy_from_slice(&v.to_be_bytes()),
            )),
            None => metrics::counter!("leep_create_last_merge_skipped_total").increment(1),
        }
        rows.extend(extra);
        self.meta.put_changeset_with_derived(&cs, &rows).await?;
        self.skiplists.insert(id, skiplist);
        let parents: Arc<[ChangesetId]> = cs.parents.into();
        self.ancestry.update(id, |e| {
            e.parents = Some(parents);
            e.generation = Some(generation);
        });
        Ok(id)
    }

    /// The `last_merge-v1` value of a new changeset (ADR 0058 section 1),
    /// written with it so ancestry queries never derive it: the generation
    /// of the nearest merge on its first-parent chain. From what the batched
    /// read found, and never by deriving: the first parent's row, or the
    /// first parent itself being a merge or a root, or the first parent's
    /// first parent's row. `None` stores the changeset without the row,
    /// which stays derived lazily until `upgrade-index` or the history
    /// index holder fills it.
    fn write_path_last_merge(
        cs: &Changeset,
        generation: u64,
        parent_generations: &[u64],
        pre: &CreateInputs,
    ) -> Option<u64> {
        match cs.parents.len() {
            0 => return Some(0),
            1 => {}
            _ => return Some(generation),
        }
        if let Some(v) = pre.last_merges[0] {
            return Some(v);
        }
        let parent = pre.first_parent.as_ref()?;
        match parent.parents.len() {
            0 => Some(0),
            1 => pre.last_merges[1],
            _ => Some(parent_generations[0]),
        }
    }

    /// The stored state `store_changeset` starts from, read in one round:
    /// whether each parent exists, the parents' generations (from the
    /// ancestry cache where known), the first parent's root tree and skiplist,
    /// and the condemnation rows of the contents (client changesets only;
    /// read before the blobs are checked, as `check_reusable` requires).
    ///
    /// Like the log's read-ahead (ADR 0030), whatever this does not supply (a
    /// failed batch, an absent or undecodable row) the caller reads or derives
    /// singly, at the point it did before, so errors stay where they were.
    async fn read_create_inputs(
        &self,
        cs: &Changeset,
        contents: &[(&RepoPath, Hash, u64)],
        client_blobs: bool,
    ) -> CreateInputs {
        let meta = &self.meta;
        let first = cs.parents.first().copied();
        let cached: Vec<Option<u64>> = cs
            .parents
            .iter()
            .map(|p| self.cached_generation(*p))
            .collect();
        let uncached: Vec<Hash> = cs
            .parents
            .iter()
            .zip(&cached)
            .filter(|(_, g)| g.is_none())
            .map(|(p, _)| p.0)
            .collect();
        let cached_list = first.and_then(|p| self.skiplists.get(&p));
        // The first parent's and, from its cached list, its own first
        // parent's last merges; the list read below can name the latter
        // too late, so without a cached list only the parent's is read.
        let grandparent = cached_list.as_ref().and_then(|l| l.first()).map(|e| e.id);
        let merge_keys: Vec<Hash> = first.into_iter().chain(grandparent).map(|p| p.0).collect();
        let content_keys: Vec<Hash> = contents.iter().map(|(_, h, _)| *h).collect();
        let (stored, condemned, gens, tree, list, merges) = tokio::join!(
            async {
                if cs.parents.is_empty() {
                    return Ok(Vec::new());
                }
                meta.get_changesets(&cs.parents).await
            },
            async {
                if !client_blobs || content_keys.is_empty() {
                    return Ok(vec![None; content_keys.len()]);
                }
                meta.get_derived_many(crate::gc::BLOB_CONDEMNED, &content_keys)
                    .await
            },
            async {
                if uncached.is_empty() {
                    return Ok(Vec::new());
                }
                meta.get_derived_many(Generation::NAME, &uncached).await
            },
            async {
                match first {
                    Some(p) => meta.get_derived(RootTree::NAME_STR, &p.0).await,
                    None => Ok(None),
                }
            },
            async {
                match first {
                    Some(p) if cached_list.is_none() => {
                        meta.get_derived(Skiplist::NAME, &p.0).await
                    }
                    _ => Ok(None),
                }
            },
            async {
                if cs.parents.len() != 1 {
                    return Ok(Vec::new());
                }
                meta.get_derived_many(LastMerge::NAME, &merge_keys).await
            },
        );
        let mut merges = merges
            .ok()
            .filter(|m| m.len() == merge_keys.len())
            .unwrap_or_default()
            .into_iter()
            .map(|v| v.and_then(|v| derived::decode_u64(&v).ok()));
        let last_merges = [merges.next().flatten(), merges.next().flatten()];
        let mut gens = gens.ok().into_iter().flatten();
        let parent_generations = cached
            .into_iter()
            .map(|g| {
                g.or_else(|| {
                    gens.next()
                        .flatten()
                        .and_then(|v| derived::decode_u64(&v).ok())
                })
            })
            .collect();
        let stored = stored.ok();
        let first_parent = stored.as_ref().and_then(|s| s.first().cloned().flatten());
        CreateInputs {
            first_parent,
            last_merges,
            parents_stored: stored.map(|s| s.iter().map(Option::is_some).collect()),
            condemned: condemned.ok(),
            parent_generations,
            base_tree: tree
                .ok()
                .flatten()
                .and_then(|v| derived::decode_hash(&v).ok()),
            parent_skiplist: cached_list.or_else(|| {
                list.ok()
                    .flatten()
                    .and_then(|v| Skiplist::decode(&v).ok())
                    .map(Arc::from)
            }),
        }
    }

    /// Fetch a changeset; `NotFound` if absent.
    pub async fn get_changeset(&self, id: ChangesetId) -> Result<Changeset> {
        self.meta
            .get_changeset(&id)
            .await?
            .ok_or_else(|| Error::NotFound(format!("changeset {id}")))
    }

    pub async fn info(&self, id: ChangesetId) -> Result<ChangesetInfo> {
        Ok(ChangesetInfo {
            id,
            changeset: self.get_changeset(id).await?,
            generation: self.generation(id).await?,
            root_tree: self.root_tree(id).await?,
            path_at: None,
            renamed: None,
        })
    }

    /// Hash of the changeset's root tree node.
    pub async fn root_tree(&self, id: ChangesetId) -> Result<Hash> {
        let v = derived::derive::<RootTree>(self, id).await?;
        derived::decode_hash(&v)
    }

    /// [`Repo::root_tree`], answered from what [`Repo::prefetch_root_trees`]
    /// read when it can. Only path-history derivation uses it: verification
    /// substitutes recomputed root trees as it goes, and a value read before
    /// that must not answer later reads.
    pub(crate) async fn root_tree_prefetched(&self, id: ChangesetId) -> Result<Hash> {
        match self.ancestry.get(&id, |e| e.root) {
            Some(r) => Ok(r),
            None => self.root_tree(id).await,
        }
    }

    /// Read the stored generations of `ids` this process has not seen, in
    /// one round trip. Misses and failures are left to [`Repo::generation`].
    pub(crate) async fn prefetch_generations(&self, ids: &[ChangesetId]) {
        let keys: Vec<Hash> = ids
            .iter()
            .filter(|id| self.cached_generation(**id).is_none())
            .map(|id| id.0)
            .collect();
        if keys.is_empty() {
            return;
        }
        let Ok(values) = self.meta.get_derived_many(Generation::NAME, &keys).await else {
            return;
        };
        if values.len() != keys.len() {
            return;
        }
        for (key, v) in keys.iter().zip(values) {
            if let Some(g) = v.and_then(|v| derived::decode_u64(&v).ok()) {
                self.remember_generation(ChangesetId(*key), g);
            }
        }
    }

    /// Read the stored root trees of `ids` in one round trip, for
    /// [`Repo::root_tree_prefetched`]. Misses and failures are left to it.
    pub(crate) async fn prefetch_root_trees(&self, ids: &[ChangesetId]) {
        let keys: Vec<Hash> = ids
            .iter()
            .filter(|id| self.ancestry.get(id, |e| e.root.is_none()))
            .map(|id| id.0)
            .collect();
        if keys.is_empty() {
            return;
        }
        let Ok(values) = self.meta.get_derived_many(RootTree::NAME, &keys).await else {
            return;
        };
        if values.len() != keys.len() {
            return;
        }
        for (key, v) in keys.iter().zip(values) {
            if let Some(r) = v.and_then(|v| derived::decode_hash(&v).ok()) {
                self.ancestry
                    .update(ChangesetId(*key), |e| e.root = Some(r));
            }
        }
    }

    /// Length of the longest parent chain to a root; roots are 1.
    pub async fn generation(&self, id: ChangesetId) -> Result<u64> {
        if let Some(g) = self.ancestry.get(&id, |e| e.generation) {
            return Ok(g);
        }
        let v = derived::derive::<Generation>(self, id).await?;
        let g = derived::decode_u64(&v)?;
        self.ancestry.update(id, |e| e.generation = Some(g));
        Ok(g)
    }

    /// Generation of `id` if this process has seen it, without a read.
    pub(crate) fn cached_generation(&self, id: ChangesetId) -> Option<u64> {
        self.ancestry.get(&id, |e| e.generation)
    }

    /// Parents of `id` if this process has seen them, without a read.
    pub(crate) fn cached_parents(&self, id: ChangesetId) -> Option<Arc<[ChangesetId]>> {
        self.ancestry.get(&id, |e| e.parents.clone())
    }

    /// Keep parents a batched read returned, for later walks.
    pub(crate) fn remember_parents(&self, id: ChangesetId, parents: Arc<[ChangesetId]>) {
        self.ancestry.update(id, |e| e.parents = Some(parents));
    }

    /// Keep a generation a batched read or a skiplist entry gave.
    pub(crate) fn remember_generation(&self, id: ChangesetId, generation: u64) {
        self.ancestry
            .update(id, |e| e.generation = Some(generation));
    }

    pub(crate) fn derive_lock(&self, kind: &'static str) -> DeriveLock {
        self.derive_locks
            .lock()
            .unwrap()
            .entry(kind)
            .or_default()
            .clone()
    }

    /// Parents of a stored changeset, without decoding it again once seen.
    pub(crate) async fn parents(&self, id: ChangesetId) -> Result<Arc<[ChangesetId]>> {
        if let Some(p) = self.ancestry.get(&id, |e| e.parents.clone()) {
            return Ok(p);
        }
        let p: Arc<[ChangesetId]> = self.get_changeset(id).await?.parents.into();
        self.ancestry.update(id, |e| e.parents = Some(p.clone()));
        Ok(p)
    }

    /// Read one tree node. The empty tree always exists.
    pub async fn read_tree(&self, tree: Hash) -> Result<TreeNode> {
        match self.blobs.get(&tree).await? {
            // A node's address is the hash of its bytes, so damage is
            // detectable. GC marks from these nodes: reading a damaged one as
            // valid would leave live blobs unmarked.
            Some(b) if Hash::of_object(TreeNode::KIND, &b) != tree => {
                Err(Error::Storage(format!("corrupt tree node {tree}")))
            }
            Some(b) => TreeNode::decode(&b),
            None if tree == TreeNode::empty_hash() => Ok(TreeNode::default()),
            None => Err(Error::NotFound(format!("tree {tree}"))),
        }
    }

    /// [`Repo::read_tree`] of each of `trees`, in order, with one
    /// [`BlobStore::get_many`](leep_core::BlobStore::get_many) for the
    /// distinct ones: a walk that knows a whole level of a tree reads it with
    /// many reads in flight instead of one.
    pub async fn read_trees(&self, trees: &[Hash]) -> Result<Vec<TreeNode>> {
        let mut distinct = trees.to_vec();
        distinct.sort_unstable();
        distinct.dedup();
        let found = self.blobs.get_many(&distinct).await?;
        if found.len() != distinct.len() {
            return Err(Error::Storage(format!(
                "get_many answered {} of {} tree nodes",
                found.len(),
                distinct.len()
            )));
        }
        let mut uses: HashMap<Hash, usize> = HashMap::with_capacity(distinct.len());
        for t in trees {
            *uses.entry(*t).or_default() += 1;
        }
        let mut nodes = HashMap::with_capacity(distinct.len());
        for (tree, bytes) in distinct.into_iter().zip(found) {
            let node = match bytes {
                Some(b) if Hash::of_object(TreeNode::KIND, &b) != tree => {
                    return Err(Error::Storage(format!("corrupt tree node {tree}")))
                }
                Some(b) => TreeNode::decode(&b)?,
                None if tree == TreeNode::empty_hash() => TreeNode::default(),
                None => return Err(Error::NotFound(format!("tree {tree}"))),
            };
            nodes.insert(tree, node);
        }
        let mut out = Vec::with_capacity(trees.len());
        for t in trees {
            // The last use of a node takes it; earlier repeats clone.
            let left = uses.get_mut(t).expect("counted");
            *left -= 1;
            out.push(if *left > 0 {
                nodes[t].clone()
            } else {
                nodes.remove(t).expect("each distinct node decoded once")
            });
        }
        Ok(out)
    }

    /// [`Repo::read_tree`] as a [`TreeView`], kept in a bounded cache, for
    /// walks that look up names in the same directories again and again.
    pub(crate) async fn read_tree_view(&self, tree: Hash) -> Result<Arc<TreeView>> {
        if let Some(t) = self.trees.get(&tree) {
            return Ok(t);
        }
        let view = match self.blobs.get(&tree).await? {
            Some(b) if Hash::of_object(TreeNode::KIND, &b) != tree => {
                return Err(Error::Storage(format!("corrupt tree node {tree}")))
            }
            Some(b) => TreeView::parse(b)?,
            None if tree == TreeNode::empty_hash() => TreeView::empty(),
            None => return Err(Error::NotFound(format!("tree {tree}"))),
        };
        let view = Arc::new(view);
        // `insert` counts twice what it is given.
        self.trees.insert(tree, view.clone(), view.size() / 2);
        Ok(view)
    }

    /// Resolve a ref name or full 64-hex changeset id. As a last resort a
    /// short name is looked up as a tag (`v1` for `tags/v1`), so a branch or
    /// id of the same name always wins.
    pub async fn resolve(&self, rev: &str) -> Result<ChangesetId> {
        if let Some(id) = self.meta.get_ref(rev).await? {
            return Ok(id);
        }
        if let Ok(id) = rev.parse::<ChangesetId>() {
            if rev.len() == 64 && self.meta.get_changeset(&id).await?.is_some() {
                return Ok(id);
            }
        }
        if !refname::is_tag(rev) && refname::validate(rev).is_ok() {
            if let Some(id) = self.meta.get_ref(&refname::tag_ref(rev)).await? {
                return Ok(id);
            }
        }
        Err(Error::NotFound(format!("revision {rev:?}")))
    }

    /// The changesets whose id starts with `prefix` (4 to 64 hex digits):
    /// one, or up to [`MAX_PREFIX_CANDIDATES`] when it is ambiguous. No match
    /// is `NotFound`; a malformed prefix is `Invalid` (ADR 0035).
    pub async fn resolve_prefix(&self, prefix: &str) -> Result<PrefixMatch> {
        let p = IdPrefix::parse(prefix)?;
        let mut found = self
            .meta
            .changesets_with_prefix(&p, MAX_PREFIX_CANDIDATES + 1)
            .await?;
        match found.len() {
            0 => Err(Error::NotFound(format!("no changeset id starts with {p}"))),
            1 => Ok(PrefixMatch::One(found[0])),
            n => {
                found.truncate(MAX_PREFIX_CANDIDATES);
                Ok(PrefixMatch::Many {
                    candidates: found,
                    truncated: n > MAX_PREFIX_CANDIDATES,
                })
            }
        }
    }
}

/// Most candidates an ambiguous short id reports.
pub const MAX_PREFIX_CANDIDATES: usize = 10;

/// What a short id resolves to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum PrefixMatch {
    One(ChangesetId),
    /// Several matches, ascending; `truncated` when there were more than
    /// [`MAX_PREFIX_CANDIDATES`].
    Many {
        candidates: Vec<ChangesetId>,
        truncated: bool,
    },
}

#[cfg(test)]
mod tests {
    use super::*;

    fn id(n: usize) -> ChangesetId {
        let mut b = [0u8; 32];
        b[..8].copy_from_slice(&(n as u64).to_le_bytes());
        ChangesetId(Hash(b))
    }

    #[test]
    fn ancestry_cache_is_bounded_and_keeps_recent_entries() {
        let c = AncestryCache::default();
        let total = 2 * ANCESTRY_CACHE_ENTRIES + 10;
        for n in 0..total {
            c.update(id(n), |e| e.generation = Some(n as u64));
            if n % 1000 == 0 {
                // Touching an old entry keeps it alive across rotations.
                assert_eq!(c.get(&id(0), |e| e.generation), Some(0));
            }
        }
        let m = c.inner.lock().unwrap();
        assert!(m.new.len() + m.old.len() <= 2 * ANCESTRY_CACHE_ENTRIES);
        drop(m);
        assert_eq!(
            c.get(&id(total - 1), |e| e.generation),
            Some(total as u64 - 1)
        );
        assert_eq!(c.get(&id(0), |e| e.generation), Some(0));
        assert_eq!(c.get(&id(1), |e| e.generation), None, "evicted");
        c.update(id(5), |e| e.parents = Some(Arc::from([id(4)])));
        let e = c.get(&id(5), AncestryEntry::clone);
        assert_eq!(
            (e.parents.as_deref(), e.generation),
            (Some(&[id(4)][..]), None)
        );
    }

    #[test]
    fn cache_budget_scales_every_cache_from_the_defaults() {
        let d = CacheScale::default();
        for n in [
            ANCESTRY_CACHE_ENTRIES,
            SKIPLIST_CACHE_ENTRIES,
            HISTORY_CACHE_BYTES,
        ] {
            assert_eq!(d.of(n), n);
        }
        assert_eq!(DEFAULT_CACHE_BYTES, 404 << 20);
        let half = CacheScale {
            total: DEFAULT_CACHE_BYTES / 2,
        };
        assert_eq!(half.of(TREE_CACHE_BYTES), TREE_CACHE_BYTES / 2);
        assert_eq!(half.of(SEARCH_TREE_CACHE_BYTES), 32 << 20);
        assert_eq!(half.of(ANCESTRY_CACHE_ENTRIES), ANCESTRY_CACHE_ENTRIES / 2);
        let huge = CacheScale { total: usize::MAX };
        assert!(huge.of(HISTORY_CACHE_BYTES) > HISTORY_CACHE_BYTES);

        // A small budget bounds the entry caches too.
        let c = AncestryCache::scaled(CacheScale {
            total: DEFAULT_CACHE_BYTES >> 10,
        });
        assert_eq!(c.limit, ANCESTRY_CACHE_ENTRIES >> 10);
        for n in 0..10 * c.limit {
            c.update(id(n), |e| e.generation = Some(n as u64));
        }
        let m = c.inner.lock().unwrap();
        assert!(m.new.len() + m.old.len() <= 2 * c.limit);
        let t: SizedCache<u8, TREE_CACHE_BYTES> = SizedCache::scaled(half);
        assert_eq!(t.budget, TREE_CACHE_BYTES / 2);
    }
}
