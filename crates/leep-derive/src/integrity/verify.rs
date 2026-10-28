//! `verify`: read-only checks of a whole store (ADR 0025).
//!
//! One walk over history, oldest changeset first, checks everything a
//! changeset owns: that it reads back as stored, that its tree nodes and the
//! blobs they name exist and match their addresses, and that each derived row
//! it owns equals a recomputation from its inputs. Rows keyed by content are
//! scanned afterwards, then attestations, bookkeeping, blame and the ref log.
//! Nothing is written: recomputation runs against in-memory shadows of the
//! stores (see `shadow`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant};

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::{stream, StreamExt};
use leep_core::review::{
    check_comment, check_source, Review, ReviewSource, REVIEW_KIND, REVIEW_SOURCE_KIND,
};
use leep_core::signature::{key_id as signature_key_id, Signature, SIGNATURE_KIND};
use leep_core::{
    BlobStore, Changeset, ChangesetId, Error, FileChange, Hash, MetaStore, RepoPath, Result,
    TreeEntry, TreeNode,
};
use serde::Serialize;

use super::kinds::{self, KindClass, BLAME_KIND};
use super::problems::{damage_of, recoverable, Damage, Problem, ProblemLog, Progress, Ticker};
use super::rows::{describe, expected_blob_row, recompute, RowScan, PAGE};
use super::shadow::{ShadowBlobs, ShadowMeta};
use super::walk::{HistoryWalk, Unreadable, Visitor};
use crate::blame::{cache_key, CachedBlame};
use crate::derived::history_pack::{
    decode_part, decode_root, part_identity, part_key, same_part, RawNode,
};
use crate::derived::{DerivedKind, HistoryTree, LogFilterIndex, NodeRef, StoredHistoryNode};
use crate::gc;
use crate::path_history::{bad_chunk, chunk_fits, load_history_row, part_row, resolve_raw_row};
use crate::search::index::TreeRowMemo;
use crate::search::{BLOB_KIND as BLOB_TRIGRAMS, TREE_KIND as TREE_TRIGRAMS};
use crate::signatures::{storage_key as signature_storage_key, REBASED_ORIGIN_KIND};
use crate::{reviews, Repo};

/// Blobs read at once while checking them.
const BLOB_CONCURRENCY: usize = 8;
/// Blob references gathered before they are checked together.
const BLOB_BATCH: usize = 256;

/// Checks one signature: `Err` says why it is not valid. Supplied by the
/// caller because the cryptography lives outside this crate.
pub type SignatureCheck =
    Arc<dyn Fn(&Signature, &ChangesetId) -> std::result::Result<(), String> + Send + Sync>;

#[derive(Clone)]
pub struct VerifyOptions {
    /// Refs whose history is checked; empty means every ref.
    pub refs: Vec<String>,
    /// Check every stored changeset (not only those reachable from the refs)
    /// and recompute every derived row, instead of a sample.
    pub full: bool,
    /// Re-hash the bytes of every reachable blob, not just check it exists.
    pub rehash: bool,
    /// Derived kinds to check. Kinds they depend on are checked too.
    pub derived: Vec<&'static str>,
    /// Rows per kind recomputed when not `full`, and changesets for the
    /// per-changeset kinds: about this many, chosen by hash, plus the ref
    /// tips and root changesets.
    pub sample: usize,
    /// Problems kept in the report; the rest are only counted.
    pub max_problems: usize,
    /// Verifies stored signatures cryptographically (`full` only). Without
    /// it signature rows are checked for structure.
    pub signature_check: Option<SignatureCheck>,
}

impl Default for VerifyOptions {
    fn default() -> Self {
        VerifyOptions {
            refs: Vec::new(),
            full: false,
            rehash: false,
            derived: kinds::kinds().iter().map(|k| k.name).collect(),
            sample: 256,
            max_problems: 1000,
            signature_check: None,
        }
    }
}

/// What was looked at.
#[derive(Debug, Default, Serialize)]
pub struct Stats {
    pub refs: u64,
    pub changesets_reachable: u64,
    /// Changesets no ref reaches, checked only by `--full`.
    pub changesets_other: u64,
    pub tree_nodes: u64,
    pub blobs_checked: u64,
    pub blobs_rehashed: u64,
    pub bytes_rehashed: u64,
    pub reflog_entries: u64,
    pub reflog_refs_without_entries: u64,
}

/// What was found for one derived kind.
#[derive(Debug, Default, Clone, Serialize)]
pub struct KindStats {
    /// Rows compared with a recomputation.
    pub checked: u64,
    /// Changesets in the checked set that have no row. Not a problem: rows
    /// derive on demand.
    pub missing: u64,
    pub mismatched: u64,
    /// Rows that could not be recomputed because an input is damaged.
    pub unverifiable: u64,
    /// Rows whose subject is gone or could not be checked (a collected blob,
    /// a changeset with an unreadable parent).
    pub skipped: u64,
}

#[derive(Debug, Serialize)]
pub struct VerifyReport {
    pub mode: &'static str,
    pub refs: Vec<String>,
    pub stats: Stats,
    pub derived: BTreeMap<String, KindStats>,
    pub problems: Vec<Problem>,
    pub problems_total: u64,
    pub notes: Vec<String>,
    pub elapsed_secs: f64,
}

impl VerifyReport {
    pub fn ok(&self) -> bool {
        self.problems_total == 0
    }
}

struct BlobRef {
    hash: Hash,
    size: u64,
    changeset: ChangesetId,
    path: String,
}

enum BlobCheck {
    Fine {
        rehashed: Option<u64>,
    },
    Bad(Problem),
    /// The store could not decode the blob's bytes.
    Unreadable(Hash, Problem),
}

async fn check_blob(
    blobs: &dyn leep_core::BlobStore,
    b: BlobRef,
    rehash: bool,
) -> Result<BlobCheck> {
    let missing = |b: &BlobRef| {
        BlobCheck::Bad(
            Problem::new(
                "blob",
                "missing_blob",
                format!(
                    "blob {} is missing; changeset {} has it at {}",
                    b.hash, b.changeset, b.path
                ),
            )
            .key(&b.hash)
            .changeset(b.changeset)
            .path(b.path.clone()),
        )
    };
    if !rehash {
        return Ok(if blobs.contains(&b.hash).await? {
            BlobCheck::Fine { rehashed: None }
        } else {
            missing(&b)
        });
    }
    let data = match blobs.get(&b.hash).await {
        Ok(Some(data)) => data,
        Ok(None) => return Ok(missing(&b)),
        Err(e) => {
            return match damage_of(&e) {
                Some(Damage::Missing) => Ok(missing(&b)),
                Some(Damage::Corrupt) => Ok(BlobCheck::Unreadable(
                    b.hash,
                    Problem::new(
                        "blob",
                        "corrupt_blob",
                        format!(
                            "blob {} cannot be read: {e}; changeset {} has it at {}",
                            b.hash, b.changeset, b.path
                        ),
                    )
                    .key(&b.hash)
                    .changeset(b.changeset)
                    .path(b.path),
                )),
                None => Err(e),
            };
        }
    };
    let len = data.len() as u64;
    let actual = Hash::of_blob(&data);
    if actual != b.hash {
        return Ok(BlobCheck::Bad(
            Problem::new(
                "blob",
                "corrupt_blob",
                format!(
                    "blob {} holds bytes that hash to {actual}; changeset {} has it at {}",
                    b.hash, b.changeset, b.path
                ),
            )
            .key(&b.hash)
            .changeset(b.changeset)
            .path(b.path),
        ));
    }
    if len != b.size {
        return Ok(BlobCheck::Bad(
            Problem::new(
                "blob",
                "blob_size_mismatch",
                format!(
                    "blob {} is {len} bytes but its tree entry says {}; changeset {} has it at {}",
                    b.hash, b.size, b.changeset, b.path
                ),
            )
            .key(&b.hash)
            .changeset(b.changeset)
            .path(b.path),
        ));
    }
    Ok(BlobCheck::Fine {
        rehashed: Some(len),
    })
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

fn id_prefix(h: &Hash) -> u64 {
    u64::from_be_bytes(h.0[..8].try_into().expect("8 bytes"))
}

/// `ids` read in one batch, or one by one when the batch fails: `Some(cs)`
/// for each readable or absent one, `None` for one whose row is damaged
/// (the history walk reports it), so one bad row does not end the run.
async fn readable_changesets(
    meta: &dyn MetaStore,
    ids: &[ChangesetId],
) -> Result<Vec<Option<Option<Changeset>>>> {
    if let Ok(found) = meta.get_changesets(ids).await {
        return Ok(found.into_iter().map(Some).collect());
    }
    let mut out = Vec::with_capacity(ids.len());
    for id in ids {
        out.push(match meta.get_changeset(id).await {
            Ok(cs) => Some(cs),
            Err(Error::Storage(_) | Error::Encoding(_)) => None,
            Err(e) => return Err(e),
        });
    }
    Ok(out)
}

/// Check a store. `Err` means the run could not be completed (a store was
/// unreachable, a named ref does not exist); damage is in the report.
pub async fn verify(
    repo: &Repo,
    opts: &VerifyOptions,
    progress: &dyn Progress,
) -> Result<VerifyReport> {
    let started = Instant::now();
    let kinds = kinds::with_dependencies(&opts.derived);
    let shadow_meta = ShadowMeta::new(repo.meta().clone());
    let shadow_blobs = ShadowBlobs::new(repo.blobs().clone());
    let shadow = Repo::new(shadow_blobs.clone(), shadow_meta.clone());
    shadow.log_filter.copy_side_budget(&repo.log_filter);
    let mut v = Verifier {
        repo,
        opts,
        progress,
        kinds,
        shadow,
        shadow_meta,
        shadow_blobs,
        log: ProblemLog::new(opts.max_problems),
        stats: Stats::default(),
        kind_stats: BTreeMap::new(),
        notes: Vec::new(),
        unbudgeted: 0,
        tips: HashSet::new(),
        sample_cut: u64::MAX,
        in_ref_phase: true,
        current_ref: None,
        visited_trees: HashSet::new(),
        visited_blobs: HashSet::new(),
        visited_hnodes: HashSet::new(),
        damaged_hnodes: HashSet::new(),
        damaged_blobs: HashSet::new(),
        pending: Vec::new(),
        ticker: Ticker::new(Duration::from_secs(5)),
        phase: "starting",
    };
    let refs = v.run().await?;
    if v.unbudgeted > 0 {
        v.notes.push(format!(
            "{} log-filter-v1 merge row(s) hold exactly what the merge brings in, as rows \
             written before the side-walk budget did; this code writes a superset past the \
             budget, and both are valid (ADR 0051)",
            v.unbudgeted
        ));
    }
    let (problems, problems_total) = v.log.into_parts();
    Ok(VerifyReport {
        mode: if opts.full { "full" } else { "sample" },
        refs,
        stats: v.stats,
        derived: v
            .kind_stats
            .into_iter()
            .map(|(k, s)| (k.to_string(), s))
            .collect(),
        problems,
        problems_total,
        notes: v.notes,
        elapsed_secs: started.elapsed().as_secs_f64(),
    })
}

struct Verifier<'a> {
    repo: &'a Repo,
    opts: &'a VerifyOptions,
    progress: &'a dyn Progress,
    /// Kinds to check, dependencies included, in dependency order.
    kinds: Vec<&'static str>,
    /// Derivation runs here: reads see the stores plus substitutes for rows
    /// found damaged, writes stay in memory.
    shadow: Repo,
    shadow_meta: Arc<ShadowMeta>,
    shadow_blobs: Arc<ShadowBlobs>,
    log: ProblemLog,
    stats: Stats,
    kind_stats: BTreeMap<&'static str, KindStats>,
    notes: Vec<String>,
    /// `log-filter-v1` merge rows that differ from a recomputation only
    /// because the side walk gave up (ADR 0051).
    unbudgeted: u64,
    tips: HashSet<ChangesetId>,
    sample_cut: u64,
    in_ref_phase: bool,
    current_ref: Option<String>,
    visited_trees: HashSet<Hash>,
    visited_blobs: HashSet<Hash>,
    visited_hnodes: HashSet<NodeRef>,
    damaged_hnodes: HashSet<NodeRef>,
    /// Blobs already reported unreadable, so a trigram row naming one does
    /// not report it again.
    damaged_blobs: HashSet<Hash>,
    pending: Vec<BlobRef>,
    ticker: Ticker,
    phase: &'static str,
}

impl Verifier<'_> {
    fn has(&self, kind: &str) -> bool {
        self.kinds.contains(&kind)
    }

    fn kstats(&mut self, kind: &'static str) -> &mut KindStats {
        self.kind_stats.entry(kind).or_default()
    }

    fn per_changeset_kinds(&self) -> Vec<&'static str> {
        self.kinds
            .iter()
            .copied()
            .filter(|k| kinds::lookup(k).is_some_and(|i| i.class == KindClass::PerChangeset))
            .collect()
    }

    fn phase(&mut self, name: &'static str) {
        self.phase = name;
        self.progress.info(&format!("verify: {name}"));
    }

    fn tick(&mut self) {
        if self.ticker.due() {
            self.progress.info(&format!(
                "verify: {}: {} changesets, {} tree nodes, {} blobs, {} problems so far",
                self.phase,
                self.stats.changesets_reachable + self.stats.changesets_other,
                self.stats.tree_nodes,
                self.stats.blobs_checked,
                self.log.total(),
            ));
        }
    }

    async fn run(&mut self) -> Result<Vec<String>> {
        let meta = self.repo.meta().clone();
        let all_refs = meta.list_refs().await?;
        let mut selected = BTreeMap::new();
        if self.opts.refs.is_empty() {
            selected = all_refs.clone();
        } else {
            for name in &self.opts.refs {
                let id = all_refs
                    .get(name)
                    .ok_or_else(|| Error::NotFound(format!("ref {name:?}")))?;
                selected.insert(name.clone(), *id);
            }
        }
        self.stats.refs = selected.len() as u64;
        self.tips = selected.values().copied().collect();
        self.progress.info(&format!(
            "verify: {} mode, {} ref(s), derived kinds: {}",
            if self.opts.full { "full" } else { "sample" },
            selected.len(),
            self.kinds.join(" "),
        ));

        let sampling = !self.opts.full && !self.per_changeset_kinds().is_empty();
        if sampling {
            self.sample_cut = self.sample_cut_for(&meta).await?;
        }

        self.phase("history, trees and blobs");
        let mut walk = HistoryWalk::new();
        for (name, tip) in &selected {
            self.current_ref = Some(name.clone());
            walk.run_from(&*meta, *tip, self).await?;
        }
        self.current_ref = None;
        // Named refs scope the run; without any, --full also covers the
        // changesets no ref reaches.
        if self.opts.full && self.opts.refs.is_empty() {
            self.in_ref_phase = false;
            let mut after = None;
            loop {
                let page = meta.list_changesets(after, PAGE).await?;
                let Some(last) = page.last().copied() else {
                    break;
                };
                after = Some(last);
                for id in page {
                    walk.run_from(&*meta, id, self).await?;
                }
            }
        }
        self.flush_blobs().await?;
        drop(walk);
        self.phase("stored parents");
        self.check_stored_parents().await?;
        self.visited_trees = HashSet::new();
        self.visited_blobs = HashSet::new();

        self.phase("derived rows keyed by content");
        if self.has(BLOB_TRIGRAMS) {
            self.scan_blob_trigrams().await?;
        }
        if self.has(TREE_TRIGRAMS) {
            self.scan_tree_trigrams().await?;
        }
        if self.has(kinds::HISTORY_NODE) {
            self.scan_history_nodes().await?;
        }
        self.phase("attestations and bookkeeping rows");
        self.scan_attestations().await?;
        self.scan_rebased_origins().await?;
        self.scan_gc_rows().await?;
        if self.has(BLAME_KIND) {
            self.phase("blame");
            self.check_blame(&selected).await?;
        }
        self.phase("ref log");
        self.check_reflog(&all_refs).await?;
        if sampling {
            self.notes.push(format!(
                "sample mode: about {} changesets and {} rows per kind were recomputed; \
                 use --full to recompute them all",
                self.opts.sample, self.opts.sample
            ));
        }
        Ok(selected.into_keys().collect())
    }

    /// The largest id prefix a changeset may have to be sampled, so about
    /// `sample` of the stored changesets are.
    async fn sample_cut_for(&mut self, meta: &Arc<dyn MetaStore>) -> Result<u64> {
        let mut total = 0u64;
        let mut after = None;
        loop {
            let page = meta.list_changesets(after, 5000).await?;
            let Some(last) = page.last().copied() else {
                break;
            };
            after = Some(last);
            total += page.len() as u64;
        }
        let sample = self.opts.sample as u64;
        Ok(if total <= sample {
            u64::MAX
        } else {
            ((sample as u128 * u64::MAX as u128) / total as u128) as u64
        })
    }

    fn sampled(&self, id: &ChangesetId, cs: &Changeset) -> bool {
        self.opts.full
            || self.tips.contains(id)
            || cs.parents.is_empty()
            || id_prefix(&id.0) <= self.sample_cut
    }

    // --- blobs and trees -------------------------------------------------

    fn blob_ref(&mut self, hash: Hash, size: u64, changeset: ChangesetId, path: &str) {
        if self.visited_blobs.insert(hash) {
            self.pending.push(BlobRef {
                hash,
                size,
                changeset,
                path: path.to_string(),
            });
        }
    }

    async fn flush_blobs(&mut self) -> Result<()> {
        let pending = std::mem::take(&mut self.pending);
        if pending.is_empty() {
            return Ok(());
        }
        let blobs = self.repo.blobs().clone();
        let rehash = self.opts.rehash;
        let results: Vec<Result<BlobCheck>> = stream::iter(pending)
            .map(|b| {
                let blobs = blobs.clone();
                async move { check_blob(&*blobs, b, rehash).await }
            })
            .buffer_unordered(BLOB_CONCURRENCY)
            .collect()
            .await;
        for r in results {
            self.stats.blobs_checked += 1;
            match r? {
                BlobCheck::Fine { rehashed } => {
                    if let Some(n) = rehashed {
                        self.stats.blobs_rehashed += 1;
                        self.stats.bytes_rehashed += n;
                    }
                }
                BlobCheck::Bad(p) => self.log.push(p),
                BlobCheck::Unreadable(hash, p) => {
                    self.damaged_blobs.insert(hash);
                    self.log.push(p);
                }
            }
        }
        Ok(())
    }

    async fn check_tree(&mut self, id: ChangesetId) -> Result<()> {
        let root = match self.shadow.root_tree(id).await {
            Ok(h) => h,
            Err(e) if recoverable(&e) => {
                self.log.push(
                    Problem::new(
                        "tree",
                        "unreadable_root_tree",
                        format!("the root tree of changeset {id} cannot be determined: {e}"),
                    )
                    .changeset(id),
                );
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        let mut stack = vec![(root, String::new())];
        while let Some((tree, dir)) = stack.pop() {
            if !self.visited_trees.insert(tree) {
                continue;
            }
            self.stats.tree_nodes += 1;
            let shown = if dir.is_empty() { "/" } else { dir.as_str() };
            let read = match self.shadow_blobs.get(&tree).await {
                Ok(Some(b)) => Ok(b),
                Ok(None) if tree == TreeNode::empty_hash() => continue,
                Ok(None) => Err(("missing_tree", "is missing".to_string())),
                Err(e) => match damage_of(&e) {
                    Some(Damage::Missing) => Err(("missing_tree", format!("is missing: {e}"))),
                    Some(Damage::Corrupt) => Err(("corrupt_tree", format!("cannot be read: {e}"))),
                    None => return Err(e),
                },
            };
            let bytes = match read {
                Ok(b) => b,
                Err((code, why)) => {
                    self.log.push(
                        Problem::new(
                            "tree",
                            code,
                            format!("tree node {tree} {why}; changeset {id} has it at {shown}"),
                        )
                        .key(&tree)
                        .changeset(id)
                        .path(dir),
                    );
                    continue;
                }
            };
            let actual = Hash::of_object(TreeNode::KIND, &bytes);
            let node = if actual != tree {
                Err(format!("holds bytes that hash to {actual}"))
            } else {
                TreeNode::decode(&bytes).map_err(|e| format!("does not decode: {e}"))
            };
            let node = match node {
                Ok(n) => n,
                Err(why) => {
                    self.log.push(
                        Problem::new(
                            "tree",
                            "corrupt_tree",
                            format!("tree node {tree} {why}; changeset {id} has it at {shown}"),
                        )
                        .key(&tree)
                        .changeset(id)
                        .path(dir),
                    );
                    continue;
                }
            };
            for (name, entry) in node.entries {
                let path = join(&dir, &name);
                match entry {
                    TreeEntry::Dir { tree } => {
                        if !self.visited_trees.contains(&tree) {
                            stack.push((tree, path));
                        }
                    }
                    TreeEntry::File { content, size, .. } => {
                        self.blob_ref(content, size, id, &path)
                    }
                }
            }
        }
        Ok(())
    }

    // --- derived rows owned by a changeset ---------------------------------

    async fn check_changeset_kind(
        &mut self,
        kind: &'static str,
        id: ChangesetId,
        cs: &Changeset,
    ) -> Result<()> {
        let Some(stored) = self.repo.meta().get_derived(kind, &id.0).await? else {
            self.kstats(kind).missing += 1;
            return Ok(());
        };
        let expected = match recompute(kind, &self.shadow, id, cs).await {
            Ok(v) => v,
            Err(e) if recoverable(&e) => {
                self.kstats(kind).unverifiable += 1;
                self.log.push(
                    Problem::new(
                        "derived",
                        "derived_unverifiable",
                        format!("the {kind} row of {id} cannot be recomputed: {e}"),
                    )
                    .kind(kind)
                    .key(&id.0)
                    .changeset(id),
                );
                return Ok(());
            }
            Err(e) => return Err(e),
        };
        self.kstats(kind).checked += 1;
        if kind == HistoryTree::NAME {
            let deep = self.opts.full;
            self.check_history_nodes(id, &stored, deep).await?;
        }
        // Packs are compared by content: zstd versions may compress the same
        // pack differently (ADR 0045).
        let mut same = if kind == HistoryTree::NAME {
            same_part(&expected, &stored)
        } else {
            expected == stored
        };
        if !same && kind == LogFilterIndex::NAME && cs.parents.len() > 1 {
            // Past the side-walk budget this code stores a superset of what
            // the merge brings in; a row of exactly that, as code before the
            // budget wrote, is valid too (ADR 0051). Walk without the budget
            // to tell it from damage.
            let unbounded = self.shadow.with_unbounded_side_walks();
            if recompute(kind, &unbounded, id, cs)
                .await
                .is_ok_and(|exact| exact == stored)
            {
                self.unbudgeted += 1;
                same = true;
            }
        }
        if !same {
            self.kstats(kind).mismatched += 1;
            self.log.push(
                Problem::new(
                    "derived",
                    "derived_mismatch",
                    format!(
                        "the {kind} row of {id} is wrong: {}",
                        describe(kind, &stored, &expected)
                    ),
                )
                .kind(kind)
                .key(&id.0)
                .changeset(id),
            );
            self.shadow_meta.set(kind, &id.0, expected);
        }
        Ok(())
    }

    /// The history nodes a stored `history-tree-v4` row leads to, the chunks
    /// of split ones and the keyframes of patched ones, exist and pass their
    /// parts' checksums: the root only when sampling, all of them when
    /// `deep`.
    async fn check_history_nodes(
        &mut self,
        id: ChangesetId,
        stored: &[u8],
        deep: bool,
    ) -> Result<()> {
        // A value that does not decode is reported as a mismatch.
        let Ok(root) = decode_root(id, stored) else {
            return Ok(());
        };
        let mut stack = vec![root];
        while let Some(node) = stack.pop() {
            if !self.visited_hnodes.insert(node) {
                continue;
            }
            let Some(row) = self.load_history_node(id, node).await? else {
                continue;
            };
            let mut children = Vec::new();
            match &row {
                StoredHistoryNode::Split(refs) => {
                    for (i, r) in refs.iter().enumerate() {
                        let Some(chunk) = self.load_history_node(id, r.node).await? else {
                            continue;
                        };
                        match chunk_fits(refs, i, &chunk) {
                            Some(n) => children.extend(n.entries.values().filter_map(|e| e.node())),
                            None => self.history_damage(id, node, bad_chunk(node, i))?,
                        }
                    }
                }
                StoredHistoryNode::Flat(n) => {
                    children.extend(n.entries.values().filter_map(|e| e.node()))
                }
                _ => unreachable!("resolved rows are Flat or Split"),
            }
            if deep {
                stack.extend(children);
            }
        }
        Ok(())
    }

    /// One history row with its patch applied, read from the real store
    /// without the repo's cache: a cached or shadow-recomputed row would hide
    /// damage on disk. Damage is reported under the address of the row that
    /// is damaged (the patch or its keyframe), and `None` returned.
    async fn load_history_node(
        &mut self,
        id: ChangesetId,
        node: NodeRef,
    ) -> Result<Option<StoredHistoryNode>> {
        let meta = self.repo.meta().clone();
        let row = match load_history_row(&*meta, node).await {
            Ok((row, _)) => row,
            Err(e) => {
                self.history_damage(id, node, e)?;
                return Ok(None);
            }
        };
        let key = match row.base() {
            None => None,
            Some(base) => match load_history_row(&*meta, base).await {
                Ok((RawNode::Ready(key), _)) => Some(key),
                Ok(_) => {
                    let e = Error::Storage(format!(
                        "corrupt history node {node}: its base is not a keyframe"
                    ));
                    self.history_damage(id, node, e)?;
                    return Ok(None);
                }
                Err(e) => {
                    self.history_damage(id, base, e)?;
                    return Ok(None);
                }
            },
        };
        match resolve_raw_row(node, row, key.as_ref()) {
            Ok(r) => Ok(Some(r.node)),
            Err(e) => {
                self.history_damage(id, node, e)?;
                Ok(None)
            }
        }
    }

    /// Report a damaged history node or chunk, or pass on an error that is
    /// not damage.
    fn history_damage(&mut self, id: ChangesetId, node: NodeRef, e: Error) -> Result<()> {
        let Some(d) = damage_of(&e) else {
            return Err(e);
        };
        // A chunk or keyframe shared by many nodes is reported once.
        if !self.damaged_hnodes.insert(node) {
            return Ok(());
        }
        self.log.push(
            Problem::new(
                "derived",
                match d {
                    Damage::Corrupt => "corrupt_history_node",
                    Damage::Missing => "missing_history_node",
                },
                format!(
                    "history node {node} (reached from the {} row of {id}): {e}",
                    HistoryTree::NAME
                ),
            )
            .kind(kinds::HISTORY_NODE)
            .key(&part_row(node.cs, node.part).1)
            .changeset(id),
        );
        Ok(())
    }

    // --- rows keyed by content ----------------------------------------------

    async fn scan_blob_trigrams(&mut self) -> Result<()> {
        let kind = BLOB_TRIGRAMS;
        let meta = self.repo.meta().clone();
        let mut scan = RowScan::new(kind, self.opts.full, self.opts.sample);
        while let Some(page) = scan.next(&*meta).await? {
            for (key, value) in page {
                let data = match self.repo.blobs().get(&key).await {
                    Ok(Some(data)) => data,
                    Ok(None) => {
                        self.kstats(kind).skipped += 1;
                        continue;
                    }
                    Err(e) => match damage_of(&e) {
                        Some(Damage::Missing) => {
                            self.kstats(kind).skipped += 1;
                            continue;
                        }
                        Some(Damage::Corrupt) => {
                            self.kstats(kind).unverifiable += 1;
                            if self.damaged_blobs.insert(key) {
                                self.log.push(
                                    Problem::new(
                                        "blob",
                                        "corrupt_blob",
                                        format!("blob {key} cannot be read: {e}"),
                                    )
                                    .kind(kind)
                                    .key(&key),
                                );
                            }
                            continue;
                        }
                        None => return Err(e),
                    },
                };
                if Hash::of_blob(&data) != key {
                    self.kstats(kind).unverifiable += 1;
                    continue;
                }
                self.kstats(kind).checked += 1;
                let expected = expected_blob_row(&data);
                if expected != value {
                    self.row_mismatch(kind, &key, &value, &expected);
                    self.shadow_meta.set(kind, &key, expected);
                }
            }
            self.tick();
        }
        Ok(())
    }

    async fn scan_tree_trigrams(&mut self) -> Result<()> {
        let kind = TREE_TRIGRAMS;
        let meta = self.repo.meta().clone();
        let mut scan = RowScan::new(kind, self.opts.full, self.opts.sample);
        let mut memo = TreeRowMemo::new();
        while let Some(page) = scan.next(&*meta).await? {
            for (key, value) in page {
                let expected = match self.shadow.recompute_tree_row(key, &mut memo).await {
                    Ok(v) => v,
                    Err(e) if recoverable(&e) => {
                        // The node may have been collected, or is damaged and
                        // reported by the tree check if any changeset reaches it.
                        self.kstats(kind).skipped += 1;
                        continue;
                    }
                    Err(e) => return Err(e),
                };
                self.shadow_blobs.clear();
                self.kstats(kind).checked += 1;
                if expected != value {
                    self.row_mismatch(kind, &key, &value, &expected);
                    self.shadow_meta.set(kind, &key, expected);
                }
            }
            self.tick();
        }
        Ok(())
    }

    fn row_mismatch(&mut self, kind: &'static str, key: &Hash, stored: &Bytes, expected: &Bytes) {
        self.kstats(kind).mismatched += 1;
        self.log.push(
            Problem::new(
                "derived",
                "derived_mismatch",
                format!(
                    "the {kind} row {key} is wrong: {}",
                    describe(kind, stored, expected)
                ),
            )
            .kind(kind)
            .key(key),
        );
    }

    async fn scan_history_nodes(&mut self) -> Result<()> {
        let kind = kinds::HISTORY_NODE;
        let meta = self.repo.meta().clone();
        let mut scan = RowScan::new(kind, self.opts.full, self.opts.sample);
        while let Some(page) = scan.next(&*meta).await? {
            for (key, value) in page {
                self.kstats(kind).checked += 1;
                // A part names its writer and number, which its key and
                // checksum are made from.
                let why = match part_identity(&value) {
                    Err(e) => Some(format!("does not decode: {e}")),
                    Ok((cs, part)) if part == 0 || part_key(cs, part) != key => Some(format!(
                        "holds part {part} of the pack of {cs}, which belongs under another key"
                    )),
                    Ok((cs, part)) => decode_part(&value, cs, part)
                        .err()
                        .map(|e| format!("does not decode: {e}")),
                };
                if let Some(why) = why {
                    self.kstats(kind).mismatched += 1;
                    self.log.push(
                        Problem::new(
                            "derived",
                            "corrupt_history_node",
                            format!("history node {key} {why}"),
                        )
                        .kind(kind)
                        .key(&key),
                    );
                }
            }
            self.tick();
        }
        Ok(())
    }

    // --- attestations and bookkeeping ----------------------------------------

    async fn scan_attestations(&mut self) -> Result<()> {
        let want_sigs = self.has(SIGNATURE_KIND);
        let want_reviews = self.has(REVIEW_KIND);
        let meta = self.repo.meta().clone();
        let full = self.opts.full;
        // Rows that decode, to be matched with their changesets in a full run.
        let mut sig_rows: HashSet<Hash> = HashSet::new();
        let mut key_ids: HashSet<String> = HashSet::new();
        let mut review_rows: HashSet<Hash> = HashSet::new();
        let mut reviewers: HashSet<String> = HashSet::new();

        if want_sigs {
            let mut scan = RowScan::new(SIGNATURE_KIND, full, self.opts.sample);
            while let Some(page) = scan.next(&*meta).await? {
                for (key, value) in page {
                    self.kstats(SIGNATURE_KIND).checked += 1;
                    let bad = match Signature::decode(&value) {
                        Err(e) => Some(format!("does not decode: {e}")),
                        Ok(s) if s.key_id != signature_key_id(&s.public_key) => Some(format!(
                            "names key id {} but its public key has id {}",
                            s.key_id,
                            signature_key_id(&s.public_key)
                        )),
                        Ok(s) => {
                            if full {
                                key_ids.insert(s.key_id);
                                sig_rows.insert(key);
                            }
                            None
                        }
                    };
                    if let Some(why) = bad {
                        self.attestation_problem(
                            SIGNATURE_KIND,
                            &key,
                            format!("signature row {key} {why}"),
                        );
                    }
                }
                self.tick();
            }
        }
        if want_reviews {
            let mut scan = RowScan::new(REVIEW_KIND, full, self.opts.sample);
            while let Some(page) = scan.next(&*meta).await? {
                for (key, value) in page {
                    self.kstats(REVIEW_KIND).checked += 1;
                    let bad = match Review::decode(&value) {
                        Err(e) => Some(format!("does not decode: {e}")),
                        Ok(r) => match check_comment(&r.comment) {
                            Err(e) => Some(format!("is not a valid review: {e}")),
                            Ok(()) => {
                                if full {
                                    reviewers.insert(r.reviewer);
                                    review_rows.insert(key);
                                }
                                None
                            }
                        },
                    };
                    if let Some(why) = bad {
                        self.attestation_problem(
                            REVIEW_KIND,
                            &key,
                            format!("review row {key} {why}"),
                        );
                    }
                }
                self.tick();
            }
        }
        if self.has(REVIEW_SOURCE_KIND) {
            let mut scan = RowScan::new(REVIEW_SOURCE_KIND, full, self.opts.sample);
            while let Some(page) = scan.next(&*meta).await? {
                for (key, value) in page {
                    self.kstats(REVIEW_SOURCE_KIND).checked += 1;
                    let bad = match ReviewSource::decode(&value) {
                        Err(e) => Some(format!("does not decode: {e}")),
                        Ok(s) => check_source(&s.source)
                            .err()
                            .map(|e| format!("is not a valid review source: {e}")),
                    };
                    if let Some(why) = bad {
                        self.attestation_problem(
                            REVIEW_SOURCE_KIND,
                            &key,
                            format!("review source row {key} {why}"),
                        );
                    }
                }
                self.tick();
            }
        }
        if !full || (sig_rows.is_empty() && review_rows.is_empty()) {
            return Ok(());
        }

        // A row's key is a hash of (changeset, who), so the changeset is found
        // by hashing every stored changeset with every key id and reviewer
        // that appears in a row.
        self.progress
            .info("verify: matching attestations with their changesets");
        let mut after = None;
        loop {
            let page = meta.list_changesets(after, PAGE).await?;
            let Some(last) = page.last().copied() else {
                break;
            };
            after = Some(last);
            for id in page {
                for kid in &key_ids {
                    let key = signature_storage_key(id, kid);
                    if sig_rows.remove(&key) {
                        self.check_signature(id, &key).await?;
                    }
                }
                for who in &reviewers {
                    review_rows.remove(&reviews::storage_key(id, who));
                }
            }
            self.tick();
        }
        for key in sig_rows {
            self.attestation_problem(
                SIGNATURE_KIND,
                &key,
                format!("signature row {key} belongs to no stored changeset"),
            );
        }
        for key in review_rows {
            self.attestation_problem(
                REVIEW_KIND,
                &key,
                format!("review row {key} belongs to no stored changeset"),
            );
        }
        Ok(())
    }

    fn attestation_problem(&mut self, kind: &'static str, key: &Hash, message: String) {
        self.kstats(kind).mismatched += 1;
        self.log.push(
            Problem::new("attestation", "bad_attestation", message)
                .kind(kind)
                .key(key),
        );
    }

    async fn check_signature(&mut self, id: ChangesetId, key: &Hash) -> Result<()> {
        let Some(check) = self.opts.signature_check.clone() else {
            return Ok(());
        };
        let Some(value) = self.repo.meta().get_derived(SIGNATURE_KIND, key).await? else {
            return Ok(());
        };
        let Ok(sig) = Signature::decode(&value) else {
            return Ok(());
        };
        if let Err(why) = check(&sig, &id) {
            self.kstats(SIGNATURE_KIND).mismatched += 1;
            self.log.push(
                Problem::new(
                    "attestation",
                    "invalid_signature",
                    format!(
                        "the signature by key {} on {id} does not verify: {why}",
                        sig.key_id
                    ),
                )
                .kind(SIGNATURE_KIND)
                .key(key)
                .changeset(id),
            );
        }
        Ok(())
    }

    async fn scan_rebased_origins(&mut self) -> Result<()> {
        if !self.has(REBASED_ORIGIN_KIND) {
            return Ok(());
        }
        let kind = REBASED_ORIGIN_KIND;
        let meta = self.repo.meta().clone();
        let mut scan = RowScan::new(kind, self.opts.full, self.opts.sample);
        while let Some(page) = scan.next(&*meta).await? {
            for (key, value) in page {
                self.kstats(kind).checked += 1;
                let copy = ChangesetId(key);
                let origin = <[u8; 32]>::try_from(value.as_ref())
                    .ok()
                    .map(|b| ChangesetId(Hash(b)));
                let mut why = None;
                match origin {
                    None => why = Some(format!("is {} bytes, not a changeset id", value.len())),
                    Some(o) => {
                        for (what, id) in [("copy", copy), ("original", o)] {
                            match meta.get_changeset(&id).await {
                                Ok(Some(_)) => {}
                                Ok(None) => {
                                    why =
                                        Some(format!("names the {what} {id}, which does not exist"))
                                }
                                Err(e) if damage_of(&e).is_some() => {
                                    why = Some(format!(
                                        "names the {what} {id}, which is unreadable: {e}"
                                    ))
                                }
                                Err(e) => return Err(e),
                            }
                        }
                    }
                }
                if let Some(why) = why {
                    self.kstats(kind).mismatched += 1;
                    self.log.push(
                        Problem::new(
                            "attestation",
                            "bad_rebased_origin",
                            format!("rebased-origin row {key} {why}"),
                        )
                        .kind(kind)
                        .key(&key),
                    );
                }
            }
            self.tick();
        }
        Ok(())
    }

    /// Every stored changeset's parents are stored: the changeset sweep
    /// (ADR 0059) deletes children before parents, so a stored changeset
    /// naming a deleted one is damage, whether or not a ref reaches it.
    /// Every changeset under `--full`, the sampled ones otherwise.
    async fn check_stored_parents(&mut self) -> Result<()> {
        let meta = self.repo.meta().clone();
        let mut after = None;
        loop {
            let page = meta.list_changesets(after, PAGE).await?;
            let Some(last) = page.last().copied() else {
                return Ok(());
            };
            after = Some(last);
            let found = readable_changesets(&*meta, &page).await?;
            let mut asks: Vec<(ChangesetId, ChangesetId)> = Vec::new();
            for (id, cs) in page.iter().zip(&found) {
                let Some(Some(cs)) = cs else { continue };
                if self.sampled(id, cs) {
                    asks.extend(cs.parents.iter().map(|p| (*id, *p)));
                }
            }
            for chunk in asks.chunks(PAGE) {
                let parents: Vec<ChangesetId> = chunk.iter().map(|(_, p)| *p).collect();
                let stored = readable_changesets(&*meta, &parents).await?;
                for ((child, parent), got) in chunk.iter().zip(stored) {
                    // Unreadable is stored but damaged: the history walk
                    // reports that.
                    if matches!(got, Some(None)) {
                        self.log.push(
                            Problem::new(
                                "history",
                                "stored_parent_missing",
                                format!(
                                    "stored changeset {child} names parent {parent}, which is \
                                     not stored"
                                ),
                            )
                            .key(&parent.0)
                            .changeset(*child),
                        );
                    }
                }
            }
            self.tick();
        }
    }

    async fn scan_gc_rows(&mut self) -> Result<()> {
        let meta = self.repo.meta().clone();
        for kind in [
            gc::BLOB_CLAIM,
            gc::BLOB_CONDEMNED,
            gc::BLOB_RESCUE,
            gc::BLOB_DOOM,
            gc::GC_MARK,
            crate::changeset_gc::CS_CONDEMNED,
            crate::changeset_gc::CS_RESCUE,
            crate::changeset_gc::CS_DOOM,
            crate::changeset_gc::GC_FENCE_ACTIVE,
            crate::changeset_reach::CS_REACH,
            crate::changeset_sweep::CS_TOMBSTONE,
        ] {
            if !self.has(kind) {
                continue;
            }
            let mut scan = RowScan::new(kind, self.opts.full, self.opts.sample);
            while let Some(page) = scan.next(&*meta).await? {
                for (key, value) in page {
                    self.kstats(kind).checked += 1;
                    let decoded = if kind == gc::GC_MARK {
                        gc::decode_mark(&value)
                    } else if kind == crate::changeset_reach::CS_REACH {
                        crate::changeset_reach::decode_reach_row(&key, &value)
                    } else if kind == crate::changeset_sweep::CS_TOMBSTONE {
                        crate::changeset_sweep::decode_tombstone(&value).map(|_| ())
                    } else {
                        gc::decode_row(kind, &value).map(|_| ())
                    };
                    if let Err(e) = decoded {
                        self.kstats(kind).mismatched += 1;
                        self.log.push(
                            Problem::new(
                                "attestation",
                                "bad_gc_row",
                                format!("{kind} row {key} does not decode: {e}"),
                            )
                            .kind(kind)
                            .key(&key),
                        );
                    }
                }
                self.tick();
            }
        }
        Ok(())
    }

    // --- blame --------------------------------------------------------------

    async fn check_blame(&mut self, refs: &BTreeMap<String, ChangesetId>) -> Result<()> {
        let tips: HashSet<ChangesetId> = refs.values().copied().collect();
        let mut tips: Vec<_> = tips.into_iter().collect();
        tips.sort();
        let budget = (self.opts.sample / 8).max(1);
        for tip in tips {
            let files = match self.shadow.list_files(tip, "").await {
                Ok(f) => f,
                Err(e) if recoverable(&e) => {
                    self.kstats(BLAME_KIND).skipped += 1;
                    continue;
                }
                Err(e) => return Err(e),
            };
            let cut = if self.opts.full || files.len() <= budget {
                u64::MAX
            } else {
                ((budget as u128 * u64::MAX as u128) / files.len() as u128) as u64
            };
            for f in files {
                if id_prefix(&Hash::of_blob(f.path.as_str().as_bytes())) > cut {
                    continue;
                }
                self.check_blame_row(tip, &f.path).await?;
            }
            self.shadow_blobs.clear();
            self.tick();
        }
        Ok(())
    }

    async fn check_blame_row(&mut self, tip: ChangesetId, path: &RepoPath) -> Result<()> {
        let kind = BLAME_KIND;
        let key = cache_key(tip, path);
        let Some(stored) = self.repo.meta().get_derived(kind, &key).await? else {
            self.kstats(kind).missing += 1;
            return Ok(());
        };
        self.kstats(kind).checked += 1;
        let problem = |why: String| {
            Problem::new(
                "derived",
                "derived_mismatch",
                format!("the blame row for {path} at {tip} is wrong: {why}"),
            )
            .kind(kind)
            .key(&key)
            .changeset(tip)
            .path(path.as_str())
        };
        let cached: CachedBlame = match postcard::from_bytes(&stored) {
            Ok(c) => c,
            Err(e) => {
                self.kstats(kind).mismatched += 1;
                self.log.push(problem(format!("it does not decode: {e}")));
                return Ok(());
            }
        };
        // What blame must be whatever the diff chose: one entry per line of
        // the file, carrying that line's text, credited to a stored changeset.
        let text = match self.shadow.read_file(tip, path).await {
            Ok(Some((_, data))) => String::from_utf8(data.to_vec()).ok(),
            Ok(None) => None,
            Err(e) if recoverable(&e) => None,
            Err(e) => return Err(e),
        };
        let mut why = None;
        if let Some(text) = &text {
            let lines: Vec<&str> = text
                .split_inclusive('\n')
                .map(|l| {
                    let l = l.strip_suffix('\n').unwrap_or(l);
                    l.strip_suffix('\r').unwrap_or(l)
                })
                .collect();
            if lines.len() != cached.1.len() {
                why = Some(format!(
                    "it has {} lines, the file has {}",
                    cached.1.len(),
                    lines.len()
                ));
            } else if let Some(i) = (0..lines.len()).find(|&i| cached.1[i].text != lines[i]) {
                why = Some(format!("line {} holds different text than the file", i + 1));
            }
        }
        if why.is_none() {
            let mut seen = HashSet::new();
            for l in &cached.1 {
                if seen.insert(l.changeset)
                    && self
                        .repo
                        .meta()
                        .get_changeset(&l.changeset)
                        .await
                        .ok()
                        .flatten()
                        .is_none()
                {
                    why = Some(format!(
                        "it credits {}, which is not a readable changeset",
                        l.changeset
                    ));
                    break;
                }
            }
        }
        if let Some(why) = why {
            self.kstats(kind).mismatched += 1;
            self.log.push(problem(why));
            return Ok(());
        }
        // Blame diffs under a time limit, so a recomputation of a huge file
        // may legitimately credit lines differently; note it, do not fail.
        match self.shadow.blame_walk(tip, path, false, false).await {
            Ok((fresh, _)) => {
                let fresh = postcard::to_stdvec(&fresh)?;
                if fresh != stored.as_ref() {
                    self.notes.push(format!(
                        "blame row for {path} at {tip} differs from a recomputation but is \
                         consistent with the file; blame diffs are time-limited, and \
                         `repair drop --kind blame` refreshes the cache"
                    ));
                }
            }
            Err(e) if recoverable(&e) => self.kstats(kind).unverifiable += 1,
            Err(e) => return Err(e),
        }
        Ok(())
    }

    // --- ref log ------------------------------------------------------------

    async fn check_reflog(&mut self, refs_now: &BTreeMap<String, ChangesetId>) -> Result<()> {
        let meta = self.repo.meta().clone();
        let only: Option<HashSet<&String>> =
            (!self.opts.refs.is_empty()).then(|| self.opts.refs.iter().collect());
        let wanted = |name: &String| only.as_ref().is_none_or(|o| o.contains(name));
        // What the next older entry of a ref must have set it to.
        let mut expect: HashMap<String, Option<ChangesetId>> = HashMap::new();
        // The newest entry of each ref: where the ref should be now.
        let mut newest: HashMap<String, Option<ChangesetId>> = HashMap::new();
        let mut before: Option<u64> = None;
        loop {
            let page = meta.ref_log(None, before, PAGE).await?;
            let Some(last) = page.last() else {
                break;
            };
            before = Some(last.seq);
            for e in &page {
                self.stats.reflog_entries += 1;
                if !wanted(&e.name) {
                    continue;
                }
                match expect.get(&e.name) {
                    None => {
                        newest.insert(e.name.clone(), e.new);
                    }
                    Some(want) if *want != e.new => {
                        self.log.push(
                            Problem::new(
                                "reflog",
                                "reflog_chain_break",
                                format!(
                                    "ref log entry {} of {} set it to {}, but the next entry says it was {}",
                                    e.seq,
                                    e.name,
                                    show(e.new),
                                    show(*want)
                                ),
                            )
                            .name(&e.name),
                        );
                    }
                    Some(_) => {}
                }
                expect.insert(e.name.clone(), e.old);
            }
            self.tick();
        }
        for name in refs_now.keys() {
            if wanted(name) && !newest.contains_key(name) {
                self.stats.reflog_refs_without_entries += 1;
            }
        }
        // A ref that moved while this ran looks diverged; look again.
        let mut suspects: Vec<(String, Option<ChangesetId>)> = newest
            .into_iter()
            .filter(|(name, logged)| refs_now.get(name).copied() != *logged)
            .collect();
        suspects.sort();
        for (name, _) in suspects {
            let mut state = None;
            for attempt in 0..3 {
                let current = meta.get_ref(&name).await?;
                let logged = meta
                    .ref_log(Some(&name), None, 1)
                    .await?
                    .first()
                    .map(|e| e.new);
                state = Some((current, logged));
                if logged == Some(current) {
                    break;
                }
                if attempt < 2 {
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
            }
            let (current, logged) = state.expect("looked at least once");
            match logged {
                Some(l) if l == current => {}
                Some(l) => self.log.push(
                    Problem::new(
                        "reflog",
                        "reflog_divergence",
                        format!(
                            "ref {name} is {} but the ref log says its last move set it to {}",
                            show(current),
                            show(l)
                        ),
                    )
                    .name(&name),
                ),
                None => {}
            }
        }
        Ok(())
    }
}

fn show(id: Option<ChangesetId>) -> String {
    id.map_or("absent".to_string(), |i| i.to_string())
}

#[async_trait]
impl Visitor for Verifier<'_> {
    async fn visit(&mut self, id: ChangesetId, cs: &Changeset, parent_broken: bool) -> Result<()> {
        if self.in_ref_phase {
            self.stats.changesets_reachable += 1;
        } else {
            self.stats.changesets_other += 1;
        }
        for (path, change) in &cs.changes {
            if let FileChange::Modify { content, size, .. } = change {
                self.blob_ref(*content, *size, id, path.as_str());
            }
        }
        self.check_tree(id).await?;
        let per_changeset = self.per_changeset_kinds();
        if !per_changeset.is_empty() && self.sampled(&id, cs) {
            for kind in per_changeset {
                if parent_broken {
                    self.kstats(kind).skipped += 1;
                } else {
                    self.check_changeset_kind(kind, id, cs).await?;
                }
            }
        }
        if self.pending.len() >= BLOB_BATCH {
            self.flush_blobs().await?;
        }
        self.shadow_blobs.clear();
        self.tick();
        Ok(())
    }
}

#[async_trait]
impl Unreadable for Verifier<'_> {
    async fn broken(
        &mut self,
        id: ChangesetId,
        referenced_by: Option<ChangesetId>,
        damage: Damage,
        detail: String,
    ) -> Result<()> {
        let (code, what) = match damage {
            Damage::Corrupt => ("corrupt_changeset", "is corrupt"),
            Damage::Missing => ("missing_changeset", "is missing"),
        };
        let mut p = match referenced_by {
            Some(child) => Problem::new(
                "history",
                code,
                format!("changeset {id} {what} ({detail}); it is a parent of {child}"),
            )
            .changeset(child),
            None => Problem::new("history", code, format!("changeset {id} {what} ({detail})")),
        }
        .key(&id.0);
        if let Some(name) = &self.current_ref {
            p = p.name(name);
        }
        self.log.push(p);
        Ok(())
    }
}
