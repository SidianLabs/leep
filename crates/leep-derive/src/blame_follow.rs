//! Blame across merges and renames (ADR 0065).
//!
//! A first-parent walk ([`Repo::walk_chain`]) finds the changers of one path
//! down one chain. At a merge that changed the file, the lines its first
//! parent does not explain may come unchanged from another parent; at the
//! changeset that created the path, the file may have been renamed from a
//! file of its first parent. Each such parent starts another walk, which
//! stops where it meets a changer an earlier walk found, so the walks
//! together form one graph of file versions, every version linked to the
//! versions it was made from. The graph is replayed oldest first: a version
//! takes the credit of its first parent's (or rename source's) lines that a
//! diff keeps, then of each other parent's in order for the lines still
//! unexplained, and credits the rest to itself, as `git blame` passes blame
//! to parents.
//!
//! Walks start oldest first (by the generation of the changeset they start
//! from), and the blame at each walk's start is cached once it is complete,
//! so a blame that stops at [`FOLLOW_WALKS`] leaves the oldest part of the
//! graph cached and the next request starts its walks above it.

use std::cmp::{Ordering, Reverse};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::sync::Arc;

use bytes::Bytes;
use leep_core::api::{BlameLine, FileInfo};
use leep_core::rename::RenameOptions;
use leep_core::{ChangesetId, Error, FileChange, Hash, RepoPath, Result};
use similar::DiffTag;

use crate::blame::{
    cache_key, diff_pairs, split_lines, strip_eol, CachedBlame, Cuts, End, Found, CONTENT_BATCH,
    CONTENT_BYTES, KIND,
};
use crate::derived::{decode_u64, DerivedKind, HistoryTree, LastMerge};
use crate::indexing::bounded::{newest_indexed, Bounded};
use crate::Repo;

/// Walks one blame starts below merges and renames, at most. Past this the
/// newest merges not yet followed keep the lines they brought in, and the
/// answer is not cached; the complete blames at the starts of the walks
/// done are, so a later request starts at most this many walks above them.
pub(crate) const FOLLOW_WALKS: usize = 1024;

/// Walks run at once. Each is a first-parent walk with its own batched
/// reads; most stop after a few steps, at a changer another walk found.
const WALKS_IN_FLIGHT: usize = 16;

/// The versions of a file a blame is made of, and how they derive.
pub(crate) struct Graph {
    nodes: Vec<Node>,
    /// The version at the changeset blamed.
    top: usize,
    /// The newest changer of the file on the blamed changeset's first-parent
    /// chain, whose blame is the same.
    pub(crate) latest: Option<ChangesetId>,
    pub(crate) examined: usize,
    /// Walks started below merges and renames.
    pub(crate) walks: usize,
    /// Cache the complete blames at the walks' starts.
    cache: bool,
    /// Where walks started, by the version they reached: their blames are
    /// cached there once complete.
    starts: Vec<(ChangesetId, usize)>,
}

/// A changer's version of the file, or a cached blame.
struct Node {
    cs: ChangesetId,
    info: FileInfo,
    /// Orders the replay: a version's sources are its ancestors' versions,
    /// with lower generations. 0 for a cached blame, which has no sources.
    generation: u64,
    cached: Option<CachedBlame>,
    /// The version at the first parent, or the rename source there.
    base: Option<usize>,
    /// `base` is a rename source.
    renamed: bool,
    /// The versions at other parents, by parent number.
    sides: Vec<(usize, usize)>,
    /// The bounded handle (ADR 0058) the walk that found it ran on.
    bounded: Option<Arc<Bounded>>,
    /// A merge or rename of this version was not followed (a bound was
    /// hit): its blame, and every blame made from it, is incomplete.
    cut: bool,
}

/// Where a walk's first version goes.
#[derive(Clone, Copy)]
enum Slot {
    Top,
    Renamed(usize),
    Side(usize, usize),
}

/// A walk to start: down `at`'s first parents, where the file is `info`.
struct Want {
    at: ChangesetId,
    path: RepoPath,
    info: FileInfo,
    bounded: Option<Arc<Bounded>>,
    /// Down a merged branch: a bounded blame (ADR 0058) gives the walk its
    /// own bounds when it starts ([`Repo::side_bounds`]).
    side: bool,
    slot: Slot,
}

/// A queued [`Want`], popped oldest first: by the generation of `at`, then
/// in the order queued.
struct Queued {
    generation: u64,
    seq: usize,
    want: Want,
}

impl Queued {
    fn key(&self) -> (Reverse<u64>, Reverse<usize>) {
        (Reverse(self.generation), Reverse(self.seq))
    }
}

impl PartialEq for Queued {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for Queued {}

impl PartialOrd for Queued {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Queued {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

/// The walks still to start.
#[derive(Default)]
struct Queue {
    heap: BinaryHeap<Queued>,
    seq: usize,
}

impl Queue {
    fn push(&mut self, generation: u64, want: Want) {
        self.seq += 1;
        self.heap.push(Queued {
            generation,
            seq: self.seq,
            want,
        });
    }
}

#[derive(Default)]
struct Builder {
    nodes: Vec<Node>,
    /// Changer versions by (changeset, path).
    changers: HashMap<(ChangesetId, RepoPath), usize>,
    /// The same, as the sets walks stop at, by path.
    stop_at: HashMap<RepoPath, Arc<HashSet<ChangesetId>>>,
    /// Walks done, by where they started.
    starts: HashMap<(ChangesetId, RepoPath), usize>,
    top: Option<usize>,
    latest: Option<ChangesetId>,
    examined: usize,
    walks: usize,
    /// Where walks below merges and renames started, and their versions.
    walked: Vec<(ChangesetId, usize)>,
}

impl Builder {
    /// A bound kept `slot` from being followed.
    fn cut(&mut self, slot: Slot) {
        match slot {
            Slot::Top => {}
            Slot::Renamed(to) | Slot::Side(to, _) => self.nodes[to].cut = true,
        }
    }

    fn attach(&mut self, slot: Slot, n: usize) {
        match slot {
            Slot::Top => self.top = Some(n),
            Slot::Renamed(to) => {
                self.nodes[to].base = Some(n);
                self.nodes[to].renamed = true;
            }
            Slot::Side(to, k) => self.nodes[to].sides.push((k, n)),
        }
    }

    fn add(&mut self, path: &RepoPath, node: Node) -> usize {
        let n = self.nodes.len();
        self.changers.insert((node.cs, path.clone()), n);
        Arc::make_mut(self.stop_at.entry(path.clone()).or_default()).insert(node.cs);
        self.nodes.push(node);
        n
    }

    /// Add what one walk found; returns the new changer versions.
    fn add_walk(&mut self, w: Want, found: Found) -> Result<Vec<usize>> {
        let Found {
            chain,
            end,
            examined,
        } = found;
        self.examined += examined;
        if matches!(w.slot, Slot::Top) {
            self.latest = chain.first().map(|(c, _)| *c);
        }
        let mut fresh = Vec::new();
        let mut first: Option<usize> = None;
        let mut prev: Option<usize> = None;
        let mut link = |b: &mut Builder, n: usize, prev: Option<usize>| match prev {
            Some(p) => b.nodes[p].base = Some(n),
            None => first = Some(n),
        };
        let mut met = false;
        for (x, info) in chain {
            // A walk of the same batch found the rest.
            if let Some(&n) = self.changers.get(&(x, w.path.clone())) {
                link(self, n, prev);
                met = true;
                break;
            }
            let n = self.add(
                &w.path,
                Node {
                    cs: x,
                    info,
                    generation: 0,
                    cached: None,
                    base: None,
                    renamed: false,
                    sides: Vec::new(),
                    bounded: w.bounded.clone(),
                    cut: false,
                },
            );
            fresh.push(n);
            link(self, n, prev);
            prev = Some(n);
        }
        if !met {
            let below = match end {
                End::Start => None,
                End::Known(x) => {
                    Some(*self.changers.get(&(x, w.path.clone())).ok_or_else(|| {
                        Error::Other(format!("blame walk stopped at unknown changer {x}"))
                    })?)
                }
                End::Cached(x, blame, info) => {
                    Some(match self.changers.get(&(x, w.path.clone())) {
                        Some(&n) => n,
                        None => self.add(
                            &w.path,
                            Node {
                                cs: x,
                                info,
                                generation: 0,
                                cached: Some(blame),
                                base: None,
                                renamed: false,
                                sides: Vec::new(),
                                bounded: None,
                                cut: false,
                            },
                        ),
                    })
                }
            };
            if let Some(n) = below {
                link(self, n, prev);
            }
        }
        let first = first.ok_or_else(|| {
            Error::Other(format!(
                "blame walk of {} from {} found no version",
                w.path, w.at
            ))
        })?;
        if !matches!(w.slot, Slot::Top) {
            self.walked.push((w.at, first));
        }
        self.starts.insert((w.at, w.path), first);
        self.attach(w.slot, first);
        Ok(fresh)
    }
}

/// What [`Repo::replay_graph`] gives: a cached blame as it was stored, or
/// a new one.
pub(crate) enum Replayed {
    Cached(CachedBlame),
    Fresh(CachedBlame),
}

/// A replayed version kept for the versions made from it.
struct Held {
    text: Option<Arc<String>>,
    /// The credit of each line, or the path of a version on its first-parent
    /// line that is not UTF-8.
    state: std::result::Result<Vec<(ChangesetId, u32)>, RepoPath>,
    /// Versions still to be made from it.
    left: usize,
}

impl Held {
    /// A good version of this window, before it is replayed.
    fn placeholder(text: &Arc<String>, left: usize) -> Held {
        Held {
            text: Some(text.clone()),
            state: Ok(Vec::new()),
            left,
        }
    }
}

/// The text of version `d`, replayed before the one being judged, or why
/// it fails.
fn source(
    bad: &HashMap<usize, RepoPath>,
    held: &HashMap<usize, Held>,
    d: usize,
) -> std::result::Result<Arc<String>, RepoPath> {
    if let Some(p) = bad.get(&d) {
        return Err(p.clone());
    }
    let h = &held[&d];
    match (&h.state, &h.text) {
        (Err(p), _) => Err(p.clone()),
        (Ok(_), Some(t)) => Ok(t.clone()),
        (Ok(_), None) => unreachable!("a good version has its text"),
    }
}

impl Repo {
    fn with_bounded_opt(&self, b: Option<Arc<Bounded>>) -> Repo {
        let mut r = self.clone();
        r.bounded = b;
        r
    }

    /// The versions blame of `path` at `id` (where the file is `tip`) is
    /// made of: the first-parent walk from `id`, and the walks below the
    /// merges and renames it and they find, breadth first.
    pub(crate) async fn blame_graph(
        &self,
        id: ChangesetId,
        tip: FileInfo,
        path: &RepoPath,
        cuts: Option<Cuts>,
        cache: bool,
        most_walks: usize,
    ) -> Result<Graph> {
        let mut b = Builder::default();
        let mut queue = Queue::default();
        queue.push(
            u64::MAX,
            Want {
                at: id,
                path: path.clone(),
                info: tip,
                bounded: self.bounded.clone(),
                side: false,
                slot: Slot::Top,
            },
        );
        while !queue.heap.is_empty() {
            let mut batch: Vec<Want> = Vec::new();
            while batch.len() < WALKS_IN_FLIGHT {
                let Some(Queued { want: w, .. }) = queue.heap.pop() else {
                    break;
                };
                let key = (w.at, w.path.clone());
                if let Some(&n) = b.starts.get(&key).or_else(|| b.changers.get(&key)) {
                    b.attach(w.slot, n);
                    continue;
                }
                batch.push(w);
            }
            // A complete blame cached at a walk's start (by an earlier
            // request that stopped at its bounds) needs no walk.
            let below: Vec<usize> = (0..batch.len())
                .filter(|&i| !matches!(batch[i].slot, Slot::Top))
                .collect();
            let mut hits: Vec<Option<CachedBlame>> = vec![None; batch.len()];
            if cache && !below.is_empty() {
                let keys: Vec<Hash> = below
                    .iter()
                    .map(|&i| cache_key(batch[i].at, &batch[i].path))
                    .collect();
                let found = self.meta().get_derived_many(KIND, &keys).await?;
                for (&i, v) in below.iter().zip(found) {
                    if let Some(v) = v {
                        hits[i] = Some(postcard::from_bytes(&v)?);
                    }
                }
            }
            let mut walk: Vec<Want> = Vec::new();
            for (w, hit) in batch.into_iter().zip(hits) {
                if let Some(blame) = hit {
                    let n = b.nodes.len();
                    b.nodes.push(Node {
                        cs: w.at,
                        info: w.info,
                        generation: 0,
                        cached: Some(blame),
                        base: None,
                        renamed: false,
                        sides: Vec::new(),
                        bounded: None,
                        cut: false,
                    });
                    b.starts.insert((w.at, w.path), n);
                    b.attach(w.slot, n);
                    continue;
                }
                if !matches!(w.slot, Slot::Top) {
                    if b.walks >= most_walks {
                        b.cut(w.slot);
                        continue;
                    }
                    b.walks += 1;
                }
                let mut w = w;
                if w.side && self.bounded.is_some() {
                    match self.side_bounds(w.at).await? {
                        Some(bounds) => w.bounded = Some(bounds),
                        None => {
                            b.cut(w.slot);
                            continue;
                        }
                    }
                }
                walk.push(w);
            }
            let walks = walk.iter().map(|w| {
                let repo = self.with_bounded_opt(w.bounded.clone());
                let stop = b.stop_at.get(&w.path).cloned().unwrap_or_default();
                let (at, info, path) = (w.at, w.info.clone(), w.path.clone());
                async move { repo.walk_chain(at, info, &path, cuts, cache, stop).await }
            });
            let found = futures_util::future::try_join_all(walks).await?;
            let mut fresh = Vec::new();
            for (w, f) in walk.into_iter().zip(found) {
                fresh.extend(b.add_walk(w, f)?);
            }
            self.follow(&mut b, &fresh, &mut queue).await?;
        }
        let top = b.top.expect("the top walk attaches");
        Ok(Graph {
            nodes: b.nodes,
            top,
            latest: b.latest,
            examined: b.examined,
            walks: b.walks,
            cache,
            starts: b.walked,
        })
    }

    /// Queue the walks below the new versions `fresh`: at each merge, from
    /// every other parent whose version of the file differs from the first
    /// parent's; at a version that created the path, from the rename
    /// source in its first parent, if any.
    async fn follow(&self, b: &mut Builder, fresh: &[usize], queue: &mut Queue) -> Result<()> {
        if fresh.is_empty() {
            return Ok(());
        }
        let ids: Vec<ChangesetId> = fresh.iter().map(|&n| b.nodes[n].cs).collect();
        let gens =
            futures_util::future::try_join_all(ids.iter().map(|&c| self.generation(c))).await?;
        // Merges are rare: `last_merge-v1` tells them apart without reading
        // every changer, and a missing row only costs a changeset read.
        let keys: Vec<Hash> = ids.iter().map(|c| c.0).collect();
        let merges = self.meta().get_derived_many(LastMerge::NAME, &keys).await?;
        let mut read: Vec<usize> = Vec::new();
        for (i, &n) in fresh.iter().enumerate() {
            b.nodes[n].generation = gens[i];
            let merge = match merges.get(i).cloned().flatten() {
                Some(v) => decode_u64(&v)? == gens[i],
                None => true,
            };
            if merge || b.nodes[n].base.is_none() {
                read.push(n);
            }
        }
        if read.is_empty() {
            return Ok(());
        }
        let ids: Vec<ChangesetId> = read.iter().map(|&n| b.nodes[n].cs).collect();
        let changesets = self.meta().get_changesets(&ids).await?;
        if changesets.len() != ids.len() {
            return Err(Error::Storage(format!(
                "get_changesets answered {} of {} changesets",
                changesets.len(),
                ids.len()
            )));
        }
        let mut sides: Vec<(usize, usize, ChangesetId)> = Vec::new();
        // Creations that may be renames: (version, changeset, first parent).
        let mut created: Vec<(usize, ChangesetId, ChangesetId)> = Vec::new();
        for (&n, cs) in read.iter().zip(changesets) {
            let x = b.nodes[n].cs;
            let cs = cs.ok_or_else(|| Error::NotFound(format!("changeset {x}")))?;
            for (k, &p) in cs.parents.iter().enumerate().skip(1) {
                sides.push((n, k, p));
            }
            // A creation can only be a rename if something was deleted.
            if let Some(&p1) = cs.parents.first() {
                if b.nodes[n].base.is_none()
                    && cs.changes.values().any(|c| matches!(c, FileChange::Delete))
                {
                    created.push((n, x, p1));
                }
            }
        }
        let merged_in = self.follow_sides(b, sides, queue).await?;
        for (n, x, p1) in created {
            // A merge that took the file from another parent is not searched
            // for renames: its diff against the first parent holds the whole
            // branch, and the other parent already explains the file.
            if merged_in.contains(&n) {
                continue;
            }
            let path = b.nodes[n].info.path.clone();
            let entries = self.diff(Some(p1), x, "").await?;
            let found = self
                .detect_renames(&entries, &RenameOptions::default())
                .await?;
            let Some(from) = found.renames.into_iter().find(|r| r.to == path) else {
                continue;
            };
            if let Some(info) = self.lookup_file(p1, &from.from).await? {
                let generation = self.generation(p1).await?;
                queue.push(
                    generation,
                    Want {
                        at: p1,
                        path: from.from,
                        info,
                        bounded: b.nodes[n].bounded.clone(),
                        side: false,
                        slot: Slot::Renamed(n),
                    },
                );
            }
        }
        Ok(())
    }

    /// Queue the walks from merges' other parents `sides`, as (version,
    /// parent number, parent). Returns the versions with the file at some
    /// other parent.
    async fn follow_sides(
        &self,
        b: &mut Builder,
        sides: Vec<(usize, usize, ChangesetId)>,
        queue: &mut Queue,
    ) -> Result<HashSet<usize>> {
        let mut merged_in = HashSet::new();
        let mut by_path: HashMap<RepoPath, Vec<(usize, usize, ChangesetId)>> = HashMap::new();
        for s in sides {
            by_path
                .entry(b.nodes[s.0].info.path.clone())
                .or_default()
                .push(s);
        }
        let mut by_path: Vec<_> = by_path.into_iter().collect();
        by_path.sort_by(|a, b| a.0.cmp(&b.0));
        for (path, sides) in by_path {
            let parents: Vec<ChangesetId> = sides.iter().map(|s| s.2).collect();
            let infos = self.lookup_files(&parents, &path).await?;
            for ((n, k, p), info) in sides.into_iter().zip(infos) {
                let Some(info) = info else {
                    continue;
                };
                merged_in.insert(n);
                // The same version as the first parent's explains nothing
                // the first parent does not.
                let base = b.nodes[n].base.map(|m| b.nodes[m].info.content);
                if base == Some(info.content) {
                    continue;
                }
                let generation = self.generation(p).await?;
                queue.push(
                    generation,
                    Want {
                        at: p,
                        path: path.clone(),
                        info,
                        bounded: None,
                        side: true,
                        slot: Slot::Side(n, k),
                    },
                );
            }
        }
        Ok(merged_in)
    }

    /// For a bounded blame (ADR 0058), the bounds of a walk down a merged
    /// branch from `p`: its newest indexed first-parent ancestor (rows are
    /// parent-closed, so everything below it is indexed), its un-indexed
    /// steps counted into the blame's own handle. `None` when those steps
    /// would take the branches walked past what the blame's scan budget
    /// (`--blame-scan-budget`) leaves after its own first-parent chain.
    async fn side_bounds(&self, p: ChangesetId) -> Result<Option<Arc<Bounded>>> {
        let top = self.bounded.clone().expect("a bounded blame");
        let (found, steps) = newest_indexed(self, HistoryTree::NAME, p).await?;
        if !top.charge_side(steps) {
            return Ok(None);
        }
        Ok(Some(Arc::new(Bounded {
            floor: Some(found.map_or(0, |e| e.generation)),
            count_into: Some(top),
            ..Default::default()
        })))
    }

    /// The text of each of `infos`, in order, with one `get_many`: `None`
    /// for one that is not UTF-8.
    async fn texts_of(&self, infos: &[&FileInfo]) -> Result<Vec<Option<Arc<String>>>> {
        let hashes: Vec<Hash> = infos.iter().map(|i| i.content).collect();
        let found = self.blobs().get_many(&hashes).await?;
        if found.len() != infos.len() {
            return Err(Error::Storage(format!(
                "get_many answered {} of {} blobs",
                found.len(),
                infos.len()
            )));
        }
        infos
            .iter()
            .zip(found)
            .map(|(info, data)| {
                let data =
                    data.ok_or_else(|| Error::Storage(format!("blob {} missing", info.content)))?;
                Ok(String::from_utf8(data.to_vec()).ok().map(Arc::new))
            })
            .collect()
    }

    /// Replay `g` oldest first into the blame of its top version.
    ///
    /// Lines are tracked as (changeset, line in it) and given their text
    /// from the top version at the end. The versions are read
    /// [`CONTENT_BATCH`] (and at most [`CONTENT_BYTES`]) at a time, each
    /// window while the one before is diffed on several threads; a version
    /// is kept only until every version made from it is replayed, so a
    /// long chain holds two windows. A version that is not UTF-8 makes the
    /// versions descending from it by first parents fail, and is not
    /// followed into from a merge or rename (the merge keeps those lines).
    pub(crate) async fn replay_graph(
        &self,
        mut g: Graph,
        path: &RepoPath,
    ) -> Result<(Replayed, bool)> {
        if let Some(c) = g.nodes[g.top].cached.take() {
            return Ok((Replayed::Cached(c), true));
        }
        let n = g.nodes.len();
        let mut left = vec![0usize; n];
        let mut leaves: HashMap<usize, Vec<(ChangesetId, u32)>> = HashMap::new();
        for (i, node) in g.nodes.iter_mut().enumerate() {
            node.sides.sort_unstable();
            if let Some(c) = node.cached.take() {
                leaves.insert(
                    i,
                    c.1.iter().map(|l| (l.changeset, l.origin_line)).collect(),
                );
            }
        }
        for node in &g.nodes {
            for d in node.base.iter().chain(node.sides.iter().map(|(_, s)| s)) {
                left[*d] += 1;
            }
        }
        left[g.top] += 1;
        let nodes = &g.nodes;
        let mut order: Vec<usize> = (0..n).collect();
        order.sort_by_key(|&i| (nodes[i].generation, i));
        // A blame is complete when no bound cut it or anything it is made
        // from; only complete blames are cached.
        let mut complete = vec![false; n];
        for &i in &order {
            let node = &nodes[i];
            complete[i] = !node.cut
                && node
                    .base
                    .iter()
                    .chain(node.sides.iter().map(|(_, s)| s))
                    .all(|&d| complete[d]);
        }
        let mut cache_at: HashMap<usize, Vec<ChangesetId>> = HashMap::new();
        if g.cache {
            for &(at, i) in &g.starts {
                if complete[i] && !leaves.contains_key(&i) {
                    let keys = cache_at.entry(i).or_insert_with(|| vec![nodes[i].cs]);
                    if !keys.contains(&at) {
                        keys.push(at);
                    }
                }
            }
        }
        let mut windows: Vec<&[usize]> = Vec::new();
        let (mut start, mut bytes) = (0, 0);
        for (i, &x) in order.iter().enumerate() {
            let size = nodes[x].info.size;
            if i > start && (i - start == CONTENT_BATCH || bytes + size > CONTENT_BYTES) {
                windows.push(&order[start..i]);
                (start, bytes) = (i, 0);
            }
            bytes += size;
        }
        if start < order.len() {
            windows.push(&order[start..]);
        }
        let windows = &windows;
        let read = |k: usize| async move {
            match windows.get(k) {
                Some(w) => {
                    let infos: Vec<&FileInfo> = w.iter().map(|&i| &nodes[i].info).collect();
                    self.texts_of(&infos).await.map(Some)
                }
                None => Ok(None),
            }
        };
        let mut held: HashMap<usize, Held> = HashMap::new();
        let mut next = read(0).await?;
        for (k, window) in windows.iter().enumerate() {
            let texts = next.take().expect("a window read for each window");
            let mut arena: Vec<Arc<String>> = Vec::new();
            let mut at: HashMap<usize, usize> = HashMap::new();
            let mut text_of = |i: usize, t: &Arc<String>, arena: &mut Vec<Arc<String>>| {
                *at.entry(i).or_insert_with(|| {
                    arena.push(t.clone());
                    arena.len() - 1
                })
            };
            // Which versions fail, and the diffs the others need.
            let mut bad: HashMap<usize, RepoPath> = HashMap::new();
            let mut pairs: Vec<(usize, usize)> = Vec::new();
            let mut base_pair: Vec<Option<usize>> = vec![None; window.len()];
            let mut side_pairs: Vec<Vec<(usize, usize)>> = vec![Vec::new(); window.len()];
            for (j, &i) in window.iter().enumerate() {
                let node = &nodes[i];
                let Some(own) = &texts[j] else {
                    bad.insert(i, node.info.path.clone());
                    continue;
                };
                if leaves.contains_key(&i) {
                    held.insert(i, Held::placeholder(own, left[i]));
                    continue;
                }
                let own_at = text_of(i, own, &mut arena);
                if let Some(d) = node.base {
                    match source(&bad, &held, d) {
                        Ok(t) => {
                            base_pair[j] = Some(pairs.len());
                            pairs.push((text_of(d, &t, &mut arena), own_at));
                        }
                        // A rename source that fails is not followed.
                        Err(_) if node.renamed => {}
                        Err(p) => {
                            bad.insert(i, p);
                            continue;
                        }
                    }
                }
                for &(_, d) in &node.sides {
                    if let Ok(t) = source(&bad, &held, d) {
                        side_pairs[j].push((d, pairs.len()));
                        pairs.push((text_of(d, &t, &mut arena), own_at));
                    }
                }
                // Versions of this window are held before the next is
                // judged, so `source` finds them.
                held.insert(i, Held::placeholder(own, left[i]));
            }
            let (ops, ahead) =
                futures_util::future::join(diff_pairs(Arc::new(arena), pairs), read(k + 1)).await;
            let ops = ops?;
            next = ahead?;
            for (j, &i) in window.iter().enumerate() {
                let node = &nodes[i];
                let state = if let Some(p) = bad.get(&i) {
                    Err(p.clone())
                } else if let Some(lines) = leaves.remove(&i) {
                    Ok(lines)
                } else {
                    let own = texts[j].as_ref().expect("a good version has its text");
                    let mut lines: Vec<(ChangesetId, u32)> = Vec::new();
                    let mut mine: Vec<usize> = Vec::new();
                    match (base_pair[j], node.base) {
                        (Some(p), Some(d)) => {
                            let from = held[&d].state.as_ref().expect("a good base");
                            for (tag, old, new) in &ops[p] {
                                match tag {
                                    DiffTag::Equal => lines.extend_from_slice(&from[old.clone()]),
                                    DiffTag::Delete => {}
                                    DiffTag::Insert | DiffTag::Replace => {
                                        for l in new.clone() {
                                            mine.push(l);
                                            lines.push((node.cs, l as u32 + 1));
                                        }
                                    }
                                }
                            }
                        }
                        _ => {
                            for l in 0..split_lines(own).count() {
                                mine.push(l);
                                lines.push((node.cs, l as u32 + 1));
                            }
                        }
                    }
                    for &(d, p) in &side_pairs[j] {
                        if mine.is_empty() {
                            break;
                        }
                        let from = held[&d].state.as_ref().expect("a good side");
                        let mut kept: Vec<Option<usize>> = vec![None; lines.len()];
                        for (tag, old, new) in &ops[p] {
                            if *tag == DiffTag::Equal {
                                for (o, l) in old.clone().zip(new.clone()) {
                                    kept[l] = Some(o);
                                }
                            }
                        }
                        mine.retain(|&l| match kept[l] {
                            Some(o) => {
                                lines[l] = from[o];
                                false
                            }
                            None => true,
                        });
                    }
                    Ok(lines)
                };
                for d in node.base.iter().chain(node.sides.iter().map(|(_, s)| s)) {
                    let h = held.get_mut(d).expect("a source is held until used");
                    h.left -= 1;
                    if h.left == 0 {
                        held.remove(d);
                    }
                }
                if let (Some(keys), Ok(lines), Some(text)) = (cache_at.get(&i), &state, &texts[j]) {
                    let value = postcard::to_stdvec(&credited(&node.info.path, text, lines)?)?;
                    let value = Bytes::from(value);
                    for &at in keys {
                        self.meta()
                            .put_derived(KIND, &cache_key(at, &node.info.path), value.clone())
                            .await?;
                    }
                }
                let text = texts[j].clone();
                held.insert(
                    i,
                    Held {
                        text,
                        state,
                        left: left[i],
                    },
                );
            }
        }
        let top = held.remove(&g.top).expect("the top version is held");
        let lines = top
            .state
            .map_err(|p| Error::Invalid(format!("{p} is not valid UTF-8")))?;
        let text = top.text.expect("a good version has its text");
        let resp = credited(path, &text, &lines)?;
        Ok((Replayed::Fresh(resp), complete[g.top]))
    }
}

/// The blame of `path` whose version is `text`, its lines credited as
/// `lines`.
fn credited(path: &RepoPath, text: &str, lines: &[(ChangesetId, u32)]) -> Result<CachedBlame> {
    let texts: Vec<&str> = split_lines(text).collect();
    if texts.len() != lines.len() {
        return Err(Error::Other(format!(
            "blame of {path} has {} lines for {} lines of text",
            lines.len(),
            texts.len()
        )));
    }
    let lines = lines
        .iter()
        .zip(texts)
        .map(|(&(changeset, origin_line), text)| BlameLine {
            changeset,
            origin_line,
            text: strip_eol(text).to_string(),
        })
        .collect();
    Ok((path.clone(), lines))
}
