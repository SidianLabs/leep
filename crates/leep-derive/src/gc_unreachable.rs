//! What garbage collection of unreachable changesets would reclaim, counted
//! without changing anything (ADR 0059, experiment E4).
//!
//! Read-only by construction: it lists and reads changesets, refs, the ref
//! log, derived rows and tree nodes, and never derives a missing row. The
//! roots are ADR 0059's (refs, every ref log entry, scan windows,
//! attestations, mirror maps, grace; land ids are not roots), and a second
//! policy keeps the ref log only for a retention, to size the follow-up ADR
//! on ref log retention. Changeset age is when the store first stored it
//! (`stored_at`, migration 0007), never the client's `timestamp`; rows from
//! before that migration count as stored when it was applied.

use std::collections::{BTreeMap, HashMap, HashSet, VecDeque};
use std::time::Duration;

use leep_core::changeset::EXTRA_SNAPSHOT;
use leep_core::review::{Review, REVIEW_KIND};
use leep_core::signature::{Signature, SIGNATURE_KIND};
use leep_core::{ChangesetId, Error, Hash, Result, TreeEntry, TreeNode};

use crate::signatures::REBASED_ORIGIN_KIND;
use crate::Repo;

const PAGE: usize = 1000;
/// Tree nodes read per `get_many` in the tree walks.
const NODE_BATCH: usize = 512;
/// The git bridge's map from changeset to commit (`leep_git::LEEP_TO_GIT`),
/// which this crate cannot name.
const LEEP_TO_GIT: &str = "leep-to-git-v1";

/// Kinds keyed by a changeset id that the sweep would delete with it.
pub const PER_CHANGESET_KINDS: &[&str] = &[
    "root_tree",
    "generation",
    "skiplist",
    "last_merge-v1",
    REBASED_ORIGIN_KIND,
    "history-tree-v4",
    "log-filter-v1",
    crate::gc::GC_MARK,
];

/// Age buckets of unreachable changesets, by upper bound in days.
pub const AGE_BUCKETS_DAYS: [i64; 3] = [1, 7, 30];

#[derive(Clone, Debug)]
pub struct UnreachableOptions {
    /// Unix seconds the ages and retention are measured from.
    pub now: i64,
    /// Changesets younger than this are roots.
    pub grace: Duration,
    /// Ref log entries older than this are dropped under the second policy
    /// (each ref's newest entry always stays).
    pub ref_log_retention: Duration,
    /// Walk every reachable tree to make the tree counts of both policies
    /// exact, instead of an upper bound.
    pub exact_trees: bool,
}

/// What one policy would reclaim.
#[derive(Clone, Debug, Default)]
pub struct Tally {
    pub changesets: u64,
    /// Originals of rebased lands: some stored copy links to them.
    pub originals: u64,
    /// `leep.snapshot` changesets (`leep sync`, `stash`).
    pub snapshots: u64,
    pub other: u64,
    /// Canonical (uncompressed) encoding; stored rows may be compressed.
    pub changeset_bytes: u64,
    /// Per kind: rows and logical bytes (32-byte key plus value).
    pub rows: BTreeMap<&'static str, (u64, u64)>,
    /// Tree nodes and file contents only these changesets introduced
    /// relative to their first parent and their rebased copy: an upper bound
    /// on what blob GC would then free, unless the policy's `exact_trees`.
    pub tree_nodes: u64,
    pub tree_node_bytes: u64,
    pub contents: u64,
    pub content_bytes: u64,
}

impl Tally {
    pub fn row_bytes(&self) -> u64 {
        self.rows.values().map(|(_, b)| b).sum()
    }

    /// Bytes of metadata: changesets and their rows.
    pub fn meta_bytes(&self) -> u64 {
        self.changeset_bytes + self.row_bytes()
    }
}

#[derive(Clone, Debug, Default)]
pub struct PolicyReport {
    pub reachable: u64,
    /// Unreachable before grace is applied.
    pub unreachable: u64,
    /// Of those, how many are under 1, 7 and 30 days old, and older.
    pub by_age: [u64; 4],
    /// Unreachable and older than grace.
    pub reclaimable: Tally,
    /// Whether the tree counts are exact (else an upper bound).
    pub exact_trees: bool,
}

#[derive(Clone, Debug, Default)]
pub struct UnreachableReport {
    pub changesets: u64,
    /// Parents named by a stored changeset but not stored themselves.
    pub missing_parents: u64,
    pub refs: u64,
    pub ref_log_entries: u64,
    /// Ref log entries with operation `land`.
    pub lands: u64,
    pub rebased_links: u64,
    /// Originals whose copy is in some head's scan window.
    pub window_originals: u64,
    pub attested: u64,
    pub mirrored: u64,
    pub ref_log_prunable_entries: u64,
    /// Estimate: fixed columns plus text, and 64 bytes of land id columns
    /// for every `land` entry.
    pub ref_log_prunable_bytes: u64,
    /// Reclaimable changesets without a `root_tree` row (trees not counted).
    pub without_root_tree: u64,
    /// Tree nodes a walk needed but the blob store lacked.
    pub missing_nodes: u64,
    /// What the lands recorded in the ref log wrote themselves: their copies
    /// and originals with the write path's rows, and their log entries.
    pub land_write: LandWrite,
    /// With `exact_trees`: every tree node and content that some changeset
    /// kept under the kept-log policy references, so blob-store growth per
    /// land can stand beside what is reclaimable.
    pub live_trees: Option<LiveTrees>,
    /// ADR 0059 as designed: the ref log is kept forever.
    pub log_kept: PolicyReport,
    /// The same with the ref log kept only for `ref_log_retention` (each
    /// ref's newest entry always), for the follow-up ADR.
    pub log_pruned: PolicyReport,
}

/// Live tree nodes and contents, sized as [`Tally`] sizes them.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct LiveTrees {
    pub tree_nodes: u64,
    pub tree_node_bytes: u64,
    pub contents: u64,
    pub content_bytes: u64,
}

/// Derived kinds the write path stores, as `leep-swarm` counts them.
pub const WRITE_PATH_KINDS: &[&str] = &["root_tree", "generation", "skiplist", REBASED_ORIGIN_KIND];

/// Logical bytes the lands themselves wrote, encoded like [`Tally`].
#[derive(Clone, Debug, Default)]
pub struct LandWrite {
    /// The changesets a land entry moved its ref to, and their originals.
    pub changesets: u64,
    pub changeset_bytes: u64,
    /// Their rows of [`WRITE_PATH_KINDS`].
    pub row_bytes: u64,
    /// Estimate of the land entries themselves.
    pub log_bytes: u64,
}

impl LandWrite {
    pub fn bytes(&self) -> u64 {
        self.changeset_bytes + self.row_bytes + self.log_bytes
    }
}

/// Changesets as an in-memory graph indexed by position in id order.
struct Graph {
    ids: Vec<ChangesetId>,
    /// Parents of `i` are `parents[offs[i]..offs[i + 1]]`.
    parents: Vec<u32>,
    offs: Vec<u32>,
    first_parent: Vec<u32>,
    time: Vec<i64>,
    bytes: Vec<u32>,
    snapshot: Vec<bool>,
}

const NONE: u32 = u32::MAX;

impl Graph {
    fn idx(&self, id: &ChangesetId) -> Option<u32> {
        self.ids.binary_search(id).ok().map(|i| i as u32)
    }

    fn parents(&self, i: u32) -> &[u32] {
        &self.parents[self.offs[i as usize] as usize..self.offs[i as usize + 1] as usize]
    }

    /// Mark everything reachable from `roots` in `seen`, iteratively.
    fn reach(&self, roots: impl IntoIterator<Item = u32>, seen: &mut [bool]) {
        let mut stack: Vec<u32> = Vec::new();
        for r in roots {
            if !seen[r as usize] {
                seen[r as usize] = true;
                stack.push(r);
            }
        }
        while let Some(i) = stack.pop() {
            for &p in self.parents(i) {
                if !seen[p as usize] {
                    seen[p as usize] = true;
                    stack.push(p);
                }
            }
        }
    }
}

async fn load_graph(repo: &Repo) -> Result<(Graph, u64)> {
    let mut ids = Vec::new();
    let mut after = None;
    loop {
        let page = repo.meta().list_changesets(after, PAGE).await?;
        let Some(last) = page.last().copied() else {
            break;
        };
        after = Some(last);
        ids.extend(page);
    }
    ids.sort_unstable();
    ids.dedup();
    let n = ids.len();
    if n >= NONE as usize {
        return Err(Error::Invalid(format!(
            "{n} changesets is too many to count"
        )));
    }
    let mut g = Graph {
        parents: Vec::with_capacity(n),
        offs: Vec::with_capacity(n + 1),
        first_parent: Vec::with_capacity(n),
        time: Vec::with_capacity(n),
        bytes: Vec::with_capacity(n),
        snapshot: Vec::with_capacity(n),
        ids,
    };
    g.offs.push(0);
    let mut missing = 0u64;
    for chunk in (0..n).step_by(PAGE) {
        let ids = &g.ids[chunk..(chunk + PAGE).min(n)];
        let (found, stored_at) = tokio::join!(
            repo.meta().get_changesets(ids),
            repo.meta().changesets_stored_at(ids)
        );
        let (found, stored_at) = (found?, stored_at?);
        let mut rows: Vec<(Vec<u32>, i64, u32, bool)> = Vec::with_capacity(ids.len());
        for (cs, at) in found.into_iter().zip(stored_at) {
            let Some(cs) = cs else {
                rows.push((Vec::new(), i64::MIN, 0, false));
                continue;
            };
            let mut ps = Vec::with_capacity(cs.parents.len());
            for p in &cs.parents {
                match g.idx(p) {
                    Some(i) => ps.push(i),
                    None => {
                        missing += 1;
                        ps.push(NONE);
                    }
                }
            }
            let bytes = u32::try_from(cs.encode().len()).unwrap_or(u32::MAX);
            // Listed, so stored; a time it cannot tell counts as now.
            let at = at.map_or(i64::MAX, |t| i64::try_from(t).unwrap_or(i64::MAX));
            rows.push((ps, at, bytes, cs.extra.contains_key(EXTRA_SNAPSHOT)));
        }
        for (ps, time, bytes, snap) in rows {
            g.first_parent.push(ps.first().copied().unwrap_or(NONE));
            g.parents.extend(ps.into_iter().filter(|p| *p != NONE));
            g.offs.push(g.parents.len() as u32);
            g.time.push(time);
            g.bytes.push(bytes);
            g.snapshot.push(snap);
        }
    }
    Ok((g, missing))
}

fn decode_id(v: &[u8]) -> Option<ChangesetId> {
    <[u8; 32]>::try_from(v).ok().map(|a| ChangesetId(Hash(a)))
}

/// Every row of `kind`, paged.
async fn all_rows(repo: &Repo, kind: &str) -> Result<Vec<(Hash, bytes::Bytes)>> {
    let mut out = Vec::new();
    let mut after = None;
    loop {
        let page = repo.meta().list_derived(kind, after, PAGE).await?;
        let Some((last, _)) = page.last() else {
            return Ok(out);
        };
        after = Some(*last);
        out.extend(page);
    }
}

/// Which of `cands` have a row of `kind` under `key(id)`, in batches.
async fn having_rows(
    repo: &Repo,
    kind: &str,
    cands: &[u32],
    g: &Graph,
    key: impl Fn(ChangesetId) -> Hash,
) -> Result<Vec<u32>> {
    let mut out = Vec::new();
    for chunk in cands.chunks(PAGE) {
        let keys: Vec<Hash> = chunk.iter().map(|&i| key(g.ids[i as usize])).collect();
        let rows = repo.meta().get_derived_many(kind, &keys).await?;
        out.extend(
            chunk
                .iter()
                .zip(rows)
                .filter(|(_, r)| r.is_some())
                .map(|(i, _)| *i),
        );
    }
    Ok(out)
}

fn age_bucket(now: i64, time: i64) -> usize {
    let age = now.saturating_sub(time);
    AGE_BUCKETS_DAYS
        .iter()
        .position(|d| age < d * 86_400)
        .unwrap_or(AGE_BUCKETS_DAYS.len())
}

/// Count what ADR 0059 would reclaim. Writes nothing.
pub async fn report(repo: &Repo, opts: &UnreachableOptions) -> Result<UnreachableReport> {
    let mut r = UnreachableReport::default();
    let (g, missing) = load_graph(repo).await?;
    let n = g.ids.len();
    r.changesets = n as u64;
    r.missing_parents = missing;

    let refs = repo.meta().list_refs().await?;
    r.refs = refs.len() as u64;
    let heads: Vec<u32> = refs.values().filter_map(|id| g.idx(id)).collect();

    // Copy -> original, for stored pairs.
    let mut origin_of: HashMap<u32, u32> = HashMap::new();
    let mut copy_of: HashMap<u32, u32> = HashMap::new();
    for (key, value) in all_rows(repo, REBASED_ORIGIN_KIND).await? {
        let (Some(copy), Some(orig)) = (
            g.idx(&ChangesetId(key)),
            decode_id(&value).and_then(|o| g.idx(&o)),
        ) else {
            continue;
        };
        origin_of.insert(copy, orig);
        copy_of.entry(orig).or_insert(copy);
    }
    r.rebased_links = origin_of.len() as u64;

    // The ref log, newest first. Each ref's newest entry is always kept.
    let cutoff = opts
        .now
        .saturating_sub(i64::try_from(opts.ref_log_retention.as_secs()).unwrap_or(i64::MAX));
    let mut log_kept: Vec<u32> = Vec::new();
    let mut log_retained: Vec<u32> = Vec::new();
    let mut newest_seen: HashSet<String> = HashSet::new();
    let mut landed: HashSet<u32> = HashSet::new();
    let mut before = None;
    loop {
        let page = repo.meta().ref_log(None, before, PAGE).await?;
        let Some(last) = page.last() else {
            break;
        };
        before = Some(last.seq);
        for e in page {
            r.ref_log_entries += 1;
            let land = e.operation == "land";
            let newest = newest_seen.insert(e.name.clone());
            let retained = newest || e.time >= cutoff;
            let entry_bytes = 8
                + 8
                + e.name.len() as u64
                + 32 * (u64::from(e.old.is_some()) + u64::from(e.new.is_some()))
                + e.actor.as_ref().map_or(0, |a| a.len() as u64)
                + e.operation.len() as u64
                + e.message.as_ref().map_or(0, |m| m.len() as u64)
                + if land { 64 } else { 0 };
            if land {
                r.lands += 1;
                r.land_write.log_bytes += entry_bytes;
                if let Some(c) = e.new.and_then(|c| g.idx(&c)) {
                    landed.insert(c);
                    if let Some(o) = origin_of.get(&c) {
                        landed.insert(*o);
                    }
                }
            }
            if !retained {
                r.ref_log_prunable_entries += 1;
                r.ref_log_prunable_bytes += entry_bytes;
            }
            // Land ids are not roots (ADR 0059 §5): only the targets are.
            let named: Vec<u32> = [e.old, e.new]
                .iter()
                .flatten()
                .filter_map(|id| g.idx(id))
                .collect();
            if retained {
                log_retained.extend(&named);
            }
            log_kept.extend(named);
        }
    }

    let landed: Vec<u32> = landed.into_iter().collect();
    r.land_write.changesets = landed.len() as u64;
    r.land_write.changeset_bytes = landed.iter().map(|&i| u64::from(g.bytes[i as usize])).sum();
    for kind in WRITE_PATH_KINDS {
        for chunk in landed.chunks(PAGE) {
            let keys: Vec<Hash> = chunk.iter().map(|&i| g.ids[i as usize].0).collect();
            for v in repo
                .meta()
                .get_derived_many(kind, &keys)
                .await?
                .into_iter()
                .flatten()
            {
                r.land_write.row_bytes += 32 + v.len() as u64;
            }
        }
    }

    // The landed-copy scan window of every head (root 3): the scan's stack
    // walk visits at most LANDED_COPY_SCAN changesets, each within
    // LANDED_COPY_SCAN - 1 parent hops of the head, which this covers.
    let mut window: HashSet<u32> = HashSet::new();
    for &h in &heads {
        let mut depth: HashMap<u32, usize> = HashMap::from([(h, 0)]);
        let mut queue = VecDeque::from([h]);
        while let Some(i) = queue.pop_front() {
            if let Some(o) = origin_of.get(&i) {
                window.insert(*o);
            }
            let d = depth[&i];
            if d + 1 >= crate::land::LANDED_COPY_SCAN {
                continue;
            }
            for &p in g.parents(i) {
                if let std::collections::hash_map::Entry::Vacant(v) = depth.entry(p) {
                    v.insert(d + 1);
                    queue.push_back(p);
                }
            }
        }
    }

    let base: Vec<u32> = heads.iter().chain(&window).copied().collect();

    // Attestations and mirror maps, asked only about changesets the fewest
    // roots leave unreachable.
    let mut seen = vec![false; n];
    g.reach(base.iter().chain(&log_retained).copied(), &mut seen);
    let cands: Vec<u32> = (0..n as u32).filter(|i| !seen[*i as usize]).collect();
    r.window_originals = window.len() as u64;
    let mut extra: Vec<u32> = Vec::new();
    let mut key_ids: HashSet<String> = HashSet::new();
    for (_, v) in all_rows(repo, SIGNATURE_KIND).await? {
        if let Ok(s) = Signature::decode(&v) {
            key_ids.insert(s.key_id);
        }
    }
    let mut reviewers: HashSet<String> = HashSet::new();
    for (_, v) in all_rows(repo, REVIEW_KIND).await? {
        if let Ok(rv) = Review::decode(&v) {
            reviewers.insert(rv.reviewer);
        }
    }
    let mut attested: HashSet<u32> = HashSet::new();
    for k in &key_ids {
        attested.extend(
            having_rows(repo, SIGNATURE_KIND, &cands, &g, |id| {
                crate::signatures::storage_key(id, k)
            })
            .await?,
        );
    }
    for k in &reviewers {
        attested.extend(
            having_rows(repo, REVIEW_KIND, &cands, &g, |id| {
                crate::reviews::storage_key(id, k)
            })
            .await?,
        );
    }
    r.attested = attested.len() as u64;
    extra.extend(&attested);
    let mirrored = having_rows(repo, LEEP_TO_GIT, &cands, &g, |id| id.0).await?;
    r.mirrored = mirrored.len() as u64;
    extra.extend(mirrored);

    let grace_cut = opts
        .now
        .saturating_sub(i64::try_from(opts.grace.as_secs()).unwrap_or(i64::MAX));
    let policy = |log: &[u32]| -> (PolicyReport, Vec<bool>) {
        let mut seen = vec![false; n];
        g.reach(base.iter().chain(log).chain(&extra).copied(), &mut seen);
        let mut p = PolicyReport::default();
        let mut young = Vec::new();
        for (i, (&s, &time)) in seen.iter().zip(&g.time).enumerate() {
            if !s {
                p.unreachable += 1;
                p.by_age[age_bucket(opts.now, time)] += 1;
                if time > grace_cut {
                    young.push(i as u32);
                }
            }
        }
        g.reach(young, &mut seen);
        p.reachable = seen.iter().filter(|s| **s).count() as u64;
        (p, seen)
    };
    let (mut kept, kept_seen) = policy(&log_kept);
    let (mut pruned, pruned_seen) = policy(&log_retained);

    // Reclaimable under retention is a superset of reclaimable with the log
    // kept, so one pass over it fills both tallies.
    let doomed: Vec<u32> = (0..n as u32)
        .filter(|i| !pruned_seen[*i as usize])
        .collect();
    let in_kept = |i: u32| !kept_seen[i as usize];
    for &i in &doomed {
        for (t, on) in [
            (&mut pruned.reclaimable, true),
            (&mut kept.reclaimable, in_kept(i)),
        ] {
            if !on {
                continue;
            }
            t.changesets += 1;
            t.changeset_bytes += u64::from(g.bytes[i as usize]);
            if copy_of.contains_key(&i) {
                t.originals += 1;
            } else if g.snapshot[i as usize] {
                t.snapshots += 1;
            } else {
                t.other += 1;
            }
        }
    }
    for kind in PER_CHANGESET_KINDS {
        for chunk in doomed.chunks(PAGE) {
            let keys: Vec<Hash> = chunk.iter().map(|&i| g.ids[i as usize].0).collect();
            let rows = repo.meta().get_derived_many(kind, &keys).await?;
            for (&i, row) in chunk.iter().zip(rows) {
                let Some(v) = row else { continue };
                let b = 32 + v.len() as u64;
                for (t, on) in [
                    (&mut pruned.reclaimable, true),
                    (&mut kept.reclaimable, in_kept(i)),
                ] {
                    if on {
                        let e = t.rows.entry(kind).or_default();
                        e.0 += 1;
                        e.1 += b;
                    }
                }
            }
        }
    }

    let trees = tree_candidates(repo, &g, &doomed, &copy_of, &in_kept, &mut r).await?;
    // Exact: what the retention policy keeps is a subset of what the kept
    // log keeps, so one live set, marked for the first and then extended by
    // the rest, answers both without a second full walk.
    let (dead_pruned, dead_kept): (HashSet<Hash>, HashSet<Hash>) = if opts.exact_trees {
        let mut live = Live::default();
        let pruned_live: Vec<u32> = (0..n as u32).filter(|i| pruned_seen[*i as usize]).collect();
        live.mark(repo, &g, &pruned_live, &mut r).await?;
        let dp = trees
            .keys()
            .filter(|h| !live.contains(h))
            .copied()
            .collect();
        let kept_only: Vec<u32> = (0..n as u32)
            .filter(|i| kept_seen[*i as usize] && !pruned_seen[*i as usize])
            .collect();
        live.mark(repo, &g, &kept_only, &mut r).await?;
        let dk = trees
            .iter()
            .filter(|(h, c)| c.in_kept && !live.contains(h))
            .map(|(h, _)| *h)
            .collect();
        r.live_trees = Some(live.tally);
        (dp, dk)
    } else {
        let dk = trees
            .iter()
            .filter(|(_, c)| c.in_kept)
            .map(|(h, _)| *h)
            .collect();
        (trees.keys().copied().collect(), dk)
    };
    for (h, c) in &trees {
        for (t, on) in [
            (&mut pruned.reclaimable, dead_pruned.contains(h)),
            (&mut kept.reclaimable, dead_kept.contains(h)),
        ] {
            if !on {
                continue;
            }
            if c.node {
                t.tree_nodes += 1;
                t.tree_node_bytes += c.size;
            } else {
                t.contents += 1;
                t.content_bytes += c.size;
            }
        }
    }
    pruned.exact_trees = opts.exact_trees;
    kept.exact_trees = opts.exact_trees;
    r.log_kept = kept;
    r.log_pruned = pruned;
    Ok(r)
}

struct Candidate {
    node: bool,
    size: u64,
    in_kept: bool,
}

/// A walk step: a node of a doomed changeset's tree, the nodes at the same
/// path in its reference trees, and whether it counts under the kept-log
/// policy.
type Step = (Hash, Vec<Option<Hash>>, bool);

/// Nodes and contents each doomed changeset introduced relative to its first
/// parent's tree and its rebased copy's tree. Anything shared with either
/// is live (or freed only with that other changeset), so this is an upper
/// bound on what blob GC frees: an object can still be referenced by some
/// unrelated changeset.
async fn tree_candidates(
    repo: &Repo,
    g: &Graph,
    doomed: &[u32],
    copy_of: &HashMap<u32, u32>,
    in_kept: &dyn Fn(u32) -> bool,
    r: &mut UnreachableReport,
) -> Result<HashMap<Hash, Candidate>> {
    let mut out: HashMap<Hash, Candidate> = HashMap::new();
    let empty = TreeNode::empty_hash();
    for chunk in doomed.chunks(PAGE) {
        let mut want: Vec<u32> = chunk.to_vec();
        for &i in chunk {
            let p = g.first_parent[i as usize];
            if p != NONE {
                want.push(p);
            }
            if let Some(c) = copy_of.get(&i) {
                want.push(*c);
            }
        }
        want.sort_unstable();
        want.dedup();
        let keys: Vec<Hash> = want.iter().map(|&i| g.ids[i as usize].0).collect();
        let rows = repo.meta().get_derived_many("root_tree", &keys).await?;
        let roots: HashMap<u32, Hash> = want
            .iter()
            .zip(rows)
            .filter_map(|(&i, v)| {
                v.and_then(|v| <[u8; 32]>::try_from(v.as_ref()).ok())
                    .map(|a| (i, Hash(a)))
            })
            .collect();
        let mut frontier: Vec<Step> = Vec::new();
        for &i in chunk {
            let Some(root) = roots.get(&i) else {
                r.without_root_tree += 1;
                continue;
            };
            let p = g.first_parent[i as usize];
            let refs = vec![
                (p != NONE).then(|| roots.get(&p).copied()).flatten(),
                copy_of.get(&i).and_then(|c| roots.get(c).copied()),
            ];
            frontier.push((*root, refs, in_kept(i)));
        }
        while !frontier.is_empty() {
            let take = frontier.len().min(NODE_BATCH);
            let level: Vec<Step> = frontier.drain(..take).collect();
            let mut read: Vec<Hash> = Vec::new();
            for (a, refs, _) in &level {
                read.push(*a);
                read.extend(refs.iter().flatten());
            }
            read.sort_unstable();
            read.dedup();
            let nodes = read_nodes(repo, &read, r).await?;
            for (a, refs, kept) in level {
                if a == empty || refs.contains(&Some(a)) {
                    continue;
                }
                match out.get_mut(&a) {
                    Some(c) if c.in_kept || !kept => continue,
                    Some(c) => c.in_kept = true,
                    None => {}
                }
                let Some((node, size)) = nodes.get(&a) else {
                    continue;
                };
                out.entry(a).or_insert(Candidate {
                    node: true,
                    size: *size,
                    in_kept: kept,
                });
                let ref_nodes: Vec<Option<&TreeNode>> = refs
                    .iter()
                    .map(|h| h.and_then(|h| nodes.get(&h).map(|(n, _)| n)))
                    .collect();
                for (name, entry) in &node.entries {
                    let same: Vec<Option<&TreeEntry>> = ref_nodes
                        .iter()
                        .map(|n| n.and_then(|n| n.entries.get(name)))
                        .collect();
                    match entry {
                        TreeEntry::File { content, size, .. } => {
                            let shared = same.iter().flatten().any(
                                |e| matches!(e, TreeEntry::File { content: c, .. } if c == content),
                            );
                            if shared {
                                continue;
                            }
                            let c = out.entry(*content).or_insert(Candidate {
                                node: false,
                                size: *size,
                                in_kept: kept,
                            });
                            c.in_kept |= kept;
                        }
                        TreeEntry::Dir { tree } => {
                            let child: Vec<Option<Hash>> = same
                                .iter()
                                .map(|e| match e {
                                    Some(TreeEntry::Dir { tree }) => Some(*tree),
                                    _ => None,
                                })
                                .collect();
                            frontier.push((*tree, child, kept));
                        }
                    }
                }
            }
        }
    }
    Ok(out)
}

/// Decode tree nodes, with their stored sizes. Missing ones are counted and
/// left out; the empty tree need not be stored.
async fn read_nodes(
    repo: &Repo,
    hashes: &[Hash],
    r: &mut UnreachableReport,
) -> Result<HashMap<Hash, (TreeNode, u64)>> {
    let found = repo.blobs().get_many(hashes).await?;
    let mut out = HashMap::with_capacity(hashes.len());
    for (h, b) in hashes.iter().zip(found) {
        match b {
            Some(b) => {
                out.insert(*h, (TreeNode::decode(&b)?, b.len() as u64));
            }
            None if *h == TreeNode::empty_hash() => {
                out.insert(*h, (TreeNode::default(), 0));
            }
            None => r.missing_nodes += 1,
        }
    }
    Ok(out)
}

/// The tree nodes and contents some kept changeset references: the mark of a
/// full blob GC, kept in memory and not recorded.
#[derive(Default)]
struct Live {
    set: HashSet<Hash>,
    tally: LiveTrees,
}

impl Live {
    fn contains(&self, h: &Hash) -> bool {
        self.set.contains(h)
    }

    /// Add everything the trees of `changesets` reference, stopping at
    /// objects already marked.
    async fn mark(
        &mut self,
        repo: &Repo,
        g: &Graph,
        changesets: &[u32],
        r: &mut UnreachableReport,
    ) -> Result<()> {
        let mut stack: Vec<Hash> = Vec::new();
        for chunk in changesets.chunks(PAGE) {
            let keys: Vec<Hash> = chunk.iter().map(|&i| g.ids[i as usize].0).collect();
            for v in repo
                .meta()
                .get_derived_many("root_tree", &keys)
                .await?
                .into_iter()
                .flatten()
            {
                if let Ok(a) = <[u8; 32]>::try_from(v.as_ref()) {
                    if self.set.insert(Hash(a)) {
                        stack.push(Hash(a));
                    }
                }
            }
            while !stack.is_empty() {
                let take = stack.len().min(NODE_BATCH);
                let level: Vec<Hash> = stack.split_off(stack.len() - take);
                for (node, size) in read_nodes(repo, &level, r).await?.into_values() {
                    self.tally.tree_nodes += 1;
                    self.tally.tree_node_bytes += size;
                    for e in node.entries.into_values() {
                        match e {
                            TreeEntry::File { content, size, .. } => {
                                if self.set.insert(content) {
                                    self.tally.contents += 1;
                                    self.tally.content_bytes += size;
                                }
                            }
                            TreeEntry::Dir { tree } => {
                                if self.set.insert(tree) {
                                    stack.push(tree);
                                }
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tests::{commit, repo};
    use leep_core::api::LandResponse;
    use leep_core::RefUpdateContext;

    /// Ten days after the changesets of a test were stored.
    fn opts() -> UnreachableOptions {
        UnreachableOptions {
            now: leep_core::store::unix_now() + 10 * 86_400,
            grace: Duration::from_secs(86_400),
            ref_log_retention: Duration::from_secs(86_400),
            exact_trees: true,
        }
    }

    /// The report writes nothing: every row and changeset is as before.
    async fn snapshot(r: &Repo) -> (Vec<ChangesetId>, Vec<leep_core::store::RefLogEntry>, usize) {
        let ids = r.meta().list_changesets(None, usize::MAX).await.unwrap();
        let log = r.meta().ref_log(None, None, usize::MAX).await.unwrap();
        let mut rows = 0;
        for kind in PER_CHANGESET_KINDS {
            rows += r.meta().count_derived(kind).await.unwrap() as usize;
        }
        (ids, log, rows)
    }

    #[tokio::test]
    async fn counts_originals_of_rebased_lands_and_abandoned_work() {
        let r = repo();
        let base = commit(&r, &[], &[("a/x", Some("0")), ("b/y", Some("0"))], "base").await;
        r.meta()
            .cas_ref_logged("main", None, Some(base), &RefUpdateContext::new("cas"))
            .await
            .unwrap();
        // Two agents on the same base: the second land rebases.
        let one = commit(&r, &[base], &[("a/x", Some("1"))], "one").await;
        let two = commit(&r, &[base], &[("b/y", Some("2"))], "two").await;
        assert!(matches!(
            r.land(one, "main").await.unwrap(),
            LandResponse::Landed { rebased: false, .. }
        ));
        assert!(matches!(
            r.land(two, "main").await.unwrap(),
            LandResponse::Landed { rebased: true, .. }
        ));
        // Work nobody landed.
        commit(&r, &[base], &[("a/x", Some("wip"))], "wip").await;
        let before = snapshot(&r).await;

        let rep = report(&r, &opts()).await.unwrap();
        assert_eq!(snapshot(&r).await, before, "the report must write nothing");
        assert_eq!(rep.changesets, 5); // base, one, two, two's copy, wip
        assert_eq!(rep.lands, 2);
        assert_eq!(rep.rebased_links, 1);
        // The original of the rebased land is in its copy's scan window, so
        // it is kept under both policies; only the abandoned work goes.
        assert_eq!(rep.window_originals, 1);
        assert!(rep.log_pruned.exact_trees && rep.log_kept.exact_trees);
        for p in [&rep.log_kept, &rep.log_pruned] {
            assert_eq!(p.unreachable, 1);
            assert_eq!(p.reclaimable.changesets, 1);
            assert_eq!(p.reclaimable.other, 1);
            assert_eq!(p.reclaimable.rows.get("root_tree").map(|r| r.0), Some(1));
            // Its root and `a/` node, and the "wip" contents.
            assert_eq!(p.reclaimable.tree_nodes, 2);
            assert_eq!(p.reclaimable.contents, 1);
        }
    }

    /// Past the scan window an original is reclaimable, keyed or not: its
    /// land id is answered from the record, so it is not a root.
    #[tokio::test]
    async fn originals_past_the_scan_window_are_reclaimable() {
        let r = repo();
        let base = commit(&r, &[], &[("a/x", Some("0")), ("b/y", Some("0"))], "base").await;
        r.meta()
            .cas_ref_logged("main", None, Some(base), &RefUpdateContext::new("cas"))
            .await
            .unwrap();
        let one = commit(&r, &[base], &[("a/x", Some("1"))], "one").await;
        let two = commit(&r, &[base], &[("b/y", Some("2"))], "two").await;
        r.land(one, "main").await.unwrap();
        r.land(two, "main").await.unwrap();
        let mut head = r.meta().get_ref("main").await.unwrap().unwrap();
        for i in 0..=crate::land::LANDED_COPY_SCAN {
            let next = commit(&r, &[head], &[("a/x", Some(&format!("v{i}")))], "on").await;
            r.meta()
                .cas_ref_logged(
                    "main",
                    Some(head),
                    Some(next),
                    &RefUpdateContext::new("cas"),
                )
                .await
                .unwrap();
            head = next;
        }
        let real_now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let o = UnreachableOptions {
            now: real_now + 2 * 86_400,
            ..opts()
        };
        let rep = report(&r, &o).await.unwrap();
        assert_eq!(rep.window_originals, 0);
        for t in [&rep.log_kept.reclaimable, &rep.log_pruned.reclaimable] {
            assert_eq!((t.changesets, t.originals), (1, 1));
            // Its `b/` node and contents are the copy's too; only its root
            // is its own.
            assert_eq!((t.tree_nodes, t.contents), (1, 0));
        }
        // Both lands' copies and the rebased one's original.
        assert_eq!(rep.land_write.changesets, 3);
        assert!(rep.land_write.row_bytes > 0 && rep.land_write.log_bytes > 0);
        assert!(rep.ref_log_prunable_entries > crate::land::LANDED_COPY_SCAN as u64);
    }

    /// The upper bound counts what a doomed changeset introduced; the exact
    /// count drops what unrelated live history happens to share, under
    /// both policies.
    #[tokio::test]
    async fn exact_trees_drop_objects_live_history_shares() {
        let r = repo();
        let base = commit(&r, &[], &[("a/x", Some("0")), ("b/y", Some("0"))], "base").await;
        r.meta()
            .cas_ref_logged("main", None, Some(base), &RefUpdateContext::new("cas"))
            .await
            .unwrap();
        commit(&r, &[base], &[("a/x", Some("wip"))], "wip").await;
        let side = commit(&r, &[base], &[("b/y", Some("1"))], "side").await;
        r.land(side, "main").await.unwrap();
        // Later, live history arrives at the same `a/` as the abandoned work.
        let head = r.meta().get_ref("main").await.unwrap().unwrap();
        let same = commit(&r, &[head], &[("a/x", Some("wip"))], "same").await;
        r.land(same, "main").await.unwrap();

        let bound = UnreachableOptions {
            exact_trees: false,
            ..opts()
        };
        let rep = report(&r, &bound).await.unwrap();
        assert!(rep.live_trees.is_none());
        for p in [&rep.log_kept, &rep.log_pruned] {
            assert!(!p.exact_trees);
            assert_eq!(p.reclaimable.changesets, 1);
            assert_eq!((p.reclaimable.tree_nodes, p.reclaimable.contents), (2, 1));
        }
        let rep = report(&r, &opts()).await.unwrap();
        for p in [&rep.log_kept, &rep.log_pruned] {
            assert!(p.exact_trees);
            // Only the abandoned root is its own.
            assert_eq!((p.reclaimable.tree_nodes, p.reclaimable.contents), (1, 0));
        }
        let live = rep.live_trees.unwrap();
        // Roots of base, side and same; `a/` twice and `b/` twice.
        assert_eq!(live.tree_nodes, 7);
        assert_eq!(live.contents, 3);
        assert!(live.tree_node_bytes > 0 && live.content_bytes > 0);
    }

    #[tokio::test]
    async fn grace_keeps_young_changesets_and_their_ancestry() {
        let meta = std::sync::Arc::new(leep_core::memory::MemoryMetaStore::new());
        let r = Repo::new(
            std::sync::Arc::new(leep_core::memory::MemoryBlobStore::new()),
            meta.clone(),
        );
        let base = commit(&r, &[], &[("f", Some("0"))], "base").await;
        r.meta()
            .cas_ref_logged("main", None, Some(base), &RefUpdateContext::new("cas"))
            .await
            .unwrap();
        let old = commit(&r, &[base], &[("f", Some("old"))], "old").await;
        // A young child keeps its old parent.
        let mut cs = r.get_changeset(old).await.unwrap();
        cs.parents = vec![old];
        // The client's clock says long ago; only the store's counts.
        cs.timestamp = 0;
        cs.message = "young".into();
        cs.changes.clear();
        let young = r.create_changeset(cs).await.unwrap();
        let o = opts();
        assert!(meta.set_stored_at(&young, o.now as u64 - 5));
        let rep = report(&r, &o).await.unwrap();
        assert_eq!(rep.log_kept.unreachable, 2);
        // One a few seconds old, one about ten days old.
        assert_eq!(rep.log_kept.by_age, [1, 0, 1, 0]);
        assert_eq!(rep.log_kept.reclaimable.changesets, 0);
    }
}
