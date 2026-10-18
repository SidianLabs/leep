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
    self, DerivedKind, Generation, RootTree, SkipEntry, Skiplist, StoredHistoryNode,
};
use crate::signatures::{LandPolicy, PolicySet};
use crate::tree_ops::apply_changes;

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
}

/// A repository: content-addressed blobs plus changeset metadata.
#[derive(Clone)]
pub struct Repo {
    blobs: Arc<dyn BlobStore>,
    meta: Arc<dyn MetaStore>,
    /// Per-ref queues that group concurrent lands into batches (see `land`).
    pub(crate) land_queues: Arc<crate::land::LandQueues>,
    pub(crate) land_policy: Option<Arc<dyn LandPolicy>>,
    /// Parents and generations of changesets seen by history walks.
    pub(crate) ancestry: Arc<AncestryCache>,
    /// Decoded path-history rows (see `read_history_row`).
    pub(crate) history_rows: Arc<HistoryRowCache>,
    /// History rows a derivation computed but has not stored yet (see
    /// `stage_history_rows`).
    pub(crate) history_staged: Arc<Mutex<HashMap<Hash, Arc<StoredHistoryNode>>>>,
    /// Decoded tree nodes (see `read_tree_shared`).
    pub(crate) trees: Arc<TreeCache>,
    /// Skiplists computed, or read while creating changesets (see
    /// `SkiplistCache`).
    pub(crate) skiplists: Arc<SkiplistCache>,
    /// One lock per derived kind, held while a missing range is derived (see
    /// `derived::derive`).
    pub(crate) derive_locks: Arc<Mutex<HashMap<&'static str, DeriveLock>>>,
    /// Values computed by `derived::derive`, any kind; tests check for
    /// duplicated work with it.
    #[cfg(test)]
    pub(crate) derive_calls: Arc<std::sync::atomic::AtomicUsize>,
}

type DeriveLock = Arc<tokio::sync::Mutex<()>>;

/// Parents and generation of changesets, kept in memory because history
/// walks (`is_ancestor`, `merge_base`) visit thousands of changesets and would
/// otherwise make a metadata round trip, and decode a whole changeset, per
/// step. Both are immutable, so entries never go stale. Bounded: entries live
/// in two generations of maps, and the older one is dropped when the newer
/// fills, so recently used entries survive.
#[derive(Default)]
pub(crate) struct AncestryCache {
    inner: Mutex<AncestryMaps>,
}

#[derive(Default)]
struct AncestryMaps {
    new: HashMap<ChangesetId, AncestryEntry>,
    old: HashMap<ChangesetId, AncestryEntry>,
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
    fn get(&self, id: &ChangesetId) -> AncestryEntry {
        let mut m = self.inner.lock().unwrap();
        if let Some(e) = m.new.get(id) {
            return e.clone();
        }
        match m.old.remove(id) {
            Some(e) => {
                m.insert(*id, e.clone());
                e
            }
            None => AncestryEntry::default(),
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
        m.insert(id, e);
    }
}

impl AncestryMaps {
    fn insert(&mut self, id: ChangesetId, e: AncestryEntry) {
        if self.new.len() >= ANCESTRY_CACHE_ENTRIES {
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
#[derive(Default)]
pub(crate) struct SkiplistCache {
    inner: Mutex<SkiplistMaps>,
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
    pub(crate) fn get(&self, id: &ChangesetId) -> Option<Arc<[SkipEntry]>> {
        let mut m = self.inner.lock().unwrap();
        if let Some(l) = m.new.get(id) {
            return Some(l.clone());
        }
        let l = m.old.remove(id)?;
        m.insert(*id, l.clone());
        Some(l)
    }

    pub(crate) fn insert(&self, id: ChangesetId, list: Arc<[SkipEntry]>) {
        self.inner.lock().unwrap().insert(id, list);
    }
}

impl SkiplistMaps {
    fn insert(&mut self, id: ChangesetId, list: Arc<[SkipEntry]>) {
        if self.new.len() >= SKIPLIST_CACHE_ENTRIES {
            self.old = std::mem::take(&mut self.new);
        }
        self.new.insert(id, list);
    }
}

/// Decoded `history-node-v3` rows by hash, as stored. A split node is read
/// one row per chunk, a patch with its keyframe, and derivation reads the
/// same root and directory nodes for every changeset, so without this each
/// changeset would re-read and re-decode dozens of rows. Rows are content
/// addressed and checked against their hash before they are cached, so
/// entries never go stale.
pub(crate) type HistoryRowCache = SizedCache<StoredHistoryNode, HISTORY_CACHE_BYTES>;

/// Decoded tree nodes by hash, for path-history derivation: each changeset
/// reads the directories its parent's derivation just read (the parent's
/// new tree is its old one), and a node read from a pack decodes a delta
/// chain first. Checked against their hash before they are cached.
pub(crate) type TreeCache = SizedCache<TreeNode, TREE_CACHE_BYTES>;

/// Content-addressed values by hash, bounded like [`AncestryCache`] but by
/// bytes: each map holds up to half of `BUDGET`.
pub(crate) struct SizedCache<V, const BUDGET: usize> {
    inner: Mutex<SizedMaps<V>>,
}

impl<V, const BUDGET: usize> Default for SizedCache<V, BUDGET> {
    fn default() -> Self {
        SizedCache {
            inner: Mutex::new(SizedMaps {
                new: HashMap::new(),
                new_bytes: 0,
                old: HashMap::new(),
            }),
        }
    }
}

struct SizedMaps<V> {
    new: HashMap<Hash, (Arc<V>, usize)>,
    new_bytes: usize,
    old: HashMap<Hash, (Arc<V>, usize)>,
}

/// Bytes the history row cache may hold, counting each row at twice its
/// encoded size (about its decoded size).
const HISTORY_CACHE_BYTES: usize = 64 << 20;

/// Bytes the tree cache may hold, counted like [`HISTORY_CACHE_BYTES`].
const TREE_CACHE_BYTES: usize = 32 << 20;

impl<V, const BUDGET: usize> SizedCache<V, BUDGET> {
    pub(crate) fn get(&self, hash: &Hash) -> Option<Arc<V>> {
        let mut m = self.inner.lock().unwrap();
        if let Some((n, _)) = m.new.get(hash) {
            return Some(n.clone());
        }
        let (n, size) = m.old.remove(hash)?;
        m.insert::<BUDGET>(*hash, n.clone(), size);
        Some(n)
    }

    pub(crate) fn insert(&self, hash: Hash, value: Arc<V>, encoded_len: usize) {
        self.inner
            .lock()
            .unwrap()
            .insert::<BUDGET>(hash, value, 2 * encoded_len);
    }
}

impl<V> SizedMaps<V> {
    fn insert<const BUDGET: usize>(&mut self, hash: Hash, value: Arc<V>, size: usize) {
        if self.new_bytes + size > BUDGET / 2 {
            self.old = std::mem::take(&mut self.new);
            self.new_bytes = 0;
        }
        if self.new.insert(hash, (value, size)).is_none() {
            self.new_bytes += size;
        }
    }
}

impl Repo {
    pub fn new(blobs: Arc<dyn BlobStore>, meta: Arc<dyn MetaStore>) -> Self {
        Repo {
            blobs,
            meta,
            land_queues: Arc::default(),
            land_policy: None,
            ancestry: Arc::default(),
            history_rows: Arc::default(),
            history_staged: Arc::default(),
            trees: Arc::default(),
            skiplists: Arc::default(),
            derive_locks: Arc::default(),
            #[cfg(test)]
            derive_calls: Arc::default(),
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
        let content_keys: Vec<Hash> = contents.iter().map(|(_, h, _)| *h).collect();
        let (stored, condemned, gens, tree, list) = tokio::join!(
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
        );
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
        CreateInputs {
            parents_stored: stored.ok().map(|s| s.iter().map(Option::is_some).collect()),
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
        match self.ancestry.get(&id).root {
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
            .filter(|id| self.ancestry.get(id).root.is_none())
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
        if let Some(g) = self.ancestry.get(&id).generation {
            return Ok(g);
        }
        let v = derived::derive::<Generation>(self, id).await?;
        let g = derived::decode_u64(&v)?;
        self.ancestry.update(id, |e| e.generation = Some(g));
        Ok(g)
    }

    /// Generation of `id` if this process has seen it, without a read.
    pub(crate) fn cached_generation(&self, id: ChangesetId) -> Option<u64> {
        self.ancestry.get(&id).generation
    }

    /// Parents of `id` if this process has seen them, without a read.
    pub(crate) fn cached_parents(&self, id: ChangesetId) -> Option<Arc<[ChangesetId]>> {
        self.ancestry.get(&id).parents
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
        if let Some(p) = self.ancestry.get(&id).parents {
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

    /// [`Repo::read_tree`], shared and kept in a bounded cache, for walks
    /// that read the same directories again and again.
    pub(crate) async fn read_tree_shared(&self, tree: Hash) -> Result<Arc<TreeNode>> {
        if let Some(t) = self.trees.get(&tree) {
            return Ok(t);
        }
        let node = Arc::new(self.read_tree(tree).await?);
        // About the encoded size: a name and a 32-byte hash per entry.
        let len = node.entries.keys().map(|n| n.len() + 40).sum();
        self.trees.insert(tree, node.clone(), len);
        Ok(node)
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
                assert_eq!(c.get(&id(0)).generation, Some(0));
            }
        }
        let m = c.inner.lock().unwrap();
        assert!(m.new.len() + m.old.len() <= 2 * ANCESTRY_CACHE_ENTRIES);
        drop(m);
        assert_eq!(c.get(&id(total - 1)).generation, Some(total as u64 - 1));
        assert_eq!(c.get(&id(0)).generation, Some(0));
        assert_eq!(c.get(&id(1)).generation, None, "evicted");
        c.update(id(5), |e| e.parents = Some(Arc::from([id(4)])));
        let e = c.get(&id(5));
        assert_eq!(
            (e.parents.as_deref(), e.generation),
            (Some(&[id(4)][..]), None)
        );
    }
}
