//! Line-level blame, cached as derived data: first-parent walks, joined at
//! merges and renames into one graph of file versions (`blame_follow`).

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
use futures_util::StreamExt;
use leep_core::api::{BlameLine, BlameResponse, ChangesetSummary, FileInfo};
use leep_core::{ChangesetId, Error, Hash, RepoPath, Result, TreeEntry};
use similar::{Algorithm, DiffTag};

use crate::batched_reads::{BatchedBlobs, BatchedMeta};
use crate::blame_follow::Replayed;
use crate::derived::{self, DerivedKind, HistoryEntry, RootTree, SkipEntry, Skiplist};
use crate::history::agent_keys;
use crate::tree_ops::file_info;
use crate::Repo;

/// Longest one line diff of a blame may run. Myers' diff is quadratic in the
/// worst case (two unrelated 40k-line versions take seconds) and runs on an
/// async worker the request timeout cannot interrupt; past this the diff
/// settles for a correct but less minimal answer, which only blurs which
/// changeset gets credit for a line.
const DIFF_TIMEOUT: Duration = Duration::from_millis(500);

/// Changers collected before their cached blames are looked up, in one
/// batched read. A hit ends the walk, so at most this many steps past it
/// are wasted; a miss, the usual case for a first blame, costs one read
/// per batch instead of one per changer.
const CACHE_CHECK_BATCH: usize = 32;

/// File versions read at once (one `get_many`) when replaying them, at
/// most. With [`CONTENT_BYTES`], bounds the memory held for a file with
/// thousands of versions: two windows, the one diffed and the one read
/// ahead.
pub(crate) const CONTENT_BATCH: usize = 64;

/// Bytes of file versions read at once when replaying them, at most (but
/// one version at a time may be larger).
pub(crate) const CONTENT_BYTES: u64 = 32 << 20;

/// Most blocking tasks one window of versions is diffed on.
const DIFF_TASKS: usize = 8;

/// Steps of an indexed blame walk whose parents' files are read together.
const LOOKUP_BATCH: usize = 64;

/// Stretches a first-parent chain is cut into for a blame, at most about:
/// one per this fraction of the tip's generation, so a walk costs a bounded
/// number of reads per stretch however old the file, plus a few per change.
/// On git/git's `Makefile` (1,501 changes in 82,513 changesets), 256 was
/// faster than 64 or 128 and about as fast as 512 or 1,024, which cost
/// rarely changed files more.
const STRETCHES: u64 = 256;

/// Fewest generations a stretch spans (but those near the tip), so a short
/// history is walked as one or a few stretches.
const MIN_STRETCH_SPAN: u64 = 32;

/// Steps walked from the tip before the stretches below are: enough for
/// most files, which change rarely, and for a cached blame near the tip.
/// On git/git, 32 kept a file of 17 changes as fast as before stretches
/// (16 cost it 30 ms cold), and cost files of hundreds of changes nothing
/// measurable.
const HEAD_STEPS: usize = 32;

/// Stretches walked at once. Their reads are gathered into batched reads,
/// so this bounds memory and tasks rather than store connections.
const STRETCHES_IN_FLIGHT: usize = 64;

/// What is cached under [`KIND`]. Kept at the original `{path, lines}` shape so
/// entries written before changeset summaries existed still decode; summaries
/// are attached on every read.
/// A tuple, because postcard encodes it identically to the old struct and
/// this crate has no serde dependency of its own.
pub(crate) type CachedBlame = (RepoPath, Vec<BlameLine>);

/// The kind cached blames are stored under. `blame` credited
/// lines over first parents only; `blame-v2` (ADR 0065) follows renames and
/// merged branches, so the old rows are retired rather than read.
pub(crate) const KIND: &str = crate::integrity::kinds::BLAME_KIND;

pub(crate) fn cache_key(id: ChangesetId, path: &RepoPath) -> Hash {
    let mut b = id.0.as_bytes().to_vec();
    b.extend_from_slice(path.as_str().as_bytes());
    Hash::of_object("blame", &b)
}

/// `line` of [`split_lines`] without its terminator. A `\r` belongs to the
/// line unless a `\n` follows it.
pub(crate) fn strip_eol(line: &str) -> &str {
    match line.strip_suffix('\n') {
        Some(l) => l.strip_suffix('\r').unwrap_or(l),
        None => line,
    }
}

impl Repo {
    /// The newest of `changers` (newest first) with a cached blame of
    /// `path`: its position and the blame.
    async fn newest_cached(
        &self,
        path: &RepoPath,
        changers: &[(ChangesetId, FileInfo)],
    ) -> Result<Option<(usize, CachedBlame)>> {
        if changers.is_empty() {
            return Ok(None);
        }
        let keys: Vec<Hash> = changers.iter().map(|(c, _)| cache_key(*c, path)).collect();
        let found = self.meta().get_derived_many(KIND, &keys).await?;
        for (i, v) in found.into_iter().enumerate() {
            if let Some(v) = v {
                return Ok(Some((i, postcard::from_bytes(&v)?)));
            }
        }
        Ok(None)
    }

    /// Attribute each line of `path` at `id` to the changeset that wrote it,
    /// across renames and through merges (ADR 0065), and describe each
    /// distinct changeset.
    pub async fn blame(&self, id: ChangesetId, path: &RepoPath) -> Result<BlameResponse> {
        let (path, lines) = self.blame_lines(id, path).await?;
        let mut ids: Vec<ChangesetId> = lines.iter().map(|l| l.changeset).collect();
        ids.sort_unstable();
        ids.dedup();
        // One batched read: a long-lived file's lines name hundreds of
        // changesets.
        let found = self.meta().get_changesets(&ids).await?;
        if found.len() != ids.len() {
            return Err(Error::Storage(format!(
                "get_changesets answered {} of {} changesets",
                found.len(),
                ids.len()
            )));
        }
        let mut changesets = BTreeMap::new();
        for (cid, cs) in ids.into_iter().zip(found) {
            let cs = cs.ok_or_else(|| Error::NotFound(format!("changeset {cid}")))?;
            changesets.insert(
                cid,
                ChangesetSummary {
                    author: cs.author,
                    timestamp: cs.timestamp,
                    message_first_line: cs.message.lines().next().unwrap_or("").to_string(),
                    agent: agent_keys(&cs.extra),
                },
            );
        }
        Ok(BlameResponse {
            path,
            lines,
            changesets,
            answered_at: None,
            behind: None,
        })
    }

    async fn blame_lines(&self, id: ChangesetId, path: &RepoPath) -> Result<CachedBlame> {
        Ok(self.blame_walk(id, path, true, true).await?.0)
    }

    /// Where blame goes next from `cur`, given `entry`, the path-history
    /// index entry of the path at `cur`: the lowest first-parent ancestor of
    /// `cur` (itself included) it must examine, as no changeset above it
    /// changed the path. Without this, blame visits every ancestor down to
    /// the changeset that last changed the file, which is the whole history
    /// for a file that has not been touched in a long time.
    ///
    /// The index entry holds the maximal changers of the path under ancestry,
    /// so every changer's generation is at most the largest one, `g`. A
    /// first-parent ancestor with a generation above `g` cannot be a changer,
    /// and [`FirstParents::lowest_at_or_above`] finds the last one at or
    /// above `g`. With `g` at or below `bottom`, nothing above the generation
    /// `bottom` changed the path.
    async fn skip_unchanged(
        &self,
        chain: &mut FirstParents,
        entry: Lookup,
        bottom: u64,
    ) -> Result<Skip> {
        // A step without a row (ADR 0058) is examined, as every step is
        // without the index.
        let Lookup::Entry(Some(entry)) = entry else {
            return Ok(Skip::Here);
        };
        let gens =
            futures_util::future::try_join_all(entry.changed().iter().map(|&c| self.generation(c)))
                .await?;
        let Some(g) = gens.into_iter().max() else {
            return Ok(Skip::Here);
        };
        if g <= bottom {
            return Ok(Skip::Past);
        }
        chain.lowest_at_or_above(self, g).await.map(Skip::To)
    }

    /// The first-parent walk of [`Repo::blame_walk`] with skipping, over one
    /// stretch of the chain: from `top` down to the first-parent ancestor at
    /// generation `bottom` or below (0: to the chain's root), which it does
    /// not examine. From each changeset, the path-history index and the
    /// skiplists say which first-parent ancestor to examine next, without
    /// reading files. The files at the examined changesets' parents, which
    /// decide whether each changed the file and where the walk ends, are
    /// then read a batch of steps at a time ([`Repo::lookup_files`]). A step
    /// past the end of the file's history costs only index reads, and at
    /// most a batch of them is wasted.
    ///
    /// The walk does not know the file at `top`, which is the file the
    /// stretch above ends with, so it cannot tell whether its first step
    /// changed the file; [`Repo::blame_stretches`] decides that. The cached
    /// blame of that step is looked up all the same, and those of the steps
    /// that changed the file a batch at a time, the walk ending at the first
    /// found.
    async fn walk_stretch(
        &self,
        path: &RepoPath,
        cache: bool,
        stretch: Stretch,
    ) -> Result<StretchWalk> {
        let Stretch {
            top,
            bottom,
            most,
            mut first_parents,
            known,
        } = stretch;
        let mut walk = StretchWalk {
            steps: Vec::new(),
            first_hit: None,
            hit: None,
            resume: None,
            absent: false,
        };
        // Steps that changed the file (or, the first, may have) whose cached
        // blames were not looked up yet.
        let mut unchecked: Vec<usize> = Vec::new();
        let mut entry = self.entry_and_list(&mut first_parents, top, path).await?;
        if matches!(entry, Lookup::Entry(None)) && most == usize::MAX {
            // The file never existed at `top`, so the walk ends above it.
            walk.absent = true;
            return Ok(walk);
        }
        let mut cur = top;
        let mut pending: Vec<(ChangesetId, Option<ChangesetId>)> = Vec::new();
        loop {
            // Nothing changed between `cur` and the changeset skipped to, so
            // the file there is the one the last step found.
            let mut finished = true;
            let next = match self
                .skip_unchanged(
                    &mut first_parents,
                    std::mem::replace(&mut entry, Lookup::Entry(None)),
                    bottom,
                )
                .await?
            {
                Skip::To(x) => Some(x),
                Skip::Here => Some(cur),
                Skip::Past => None,
            };
            if let Some(x) = next {
                let parent = first_parents.parent(self, x).await?;
                pending.push((x, parent.map(|p| p.id)));
                if let Some(p) = parent.filter(|p| p.generation > bottom) {
                    if walk.steps.len() + pending.len() < most {
                        entry = self.entry_and_list(&mut first_parents, p.id, path).await?;
                        cur = p.id;
                        finished = false;
                    } else {
                        walk.resume = Some(p);
                    }
                }
            }
            // No entry at the parent: the file never existed there, which
            // the step finds and stops at.
            if pending.len() < LOOKUP_BATCH && !finished && !matches!(entry, Lookup::Entry(None)) {
                continue;
            }
            let parents: Vec<ChangesetId> = pending.iter().filter_map(|(_, p)| *p).collect();
            let mut infos = self.lookup_files(&parents, path).await?.into_iter();
            let mut ended = false;
            for (x, parent) in pending.drain(..) {
                let parent_info = match parent {
                    Some(_) => infos.next().expect("one file per parent"),
                    None => None,
                };
                // The file at `x` is the one at the last step's parent.
                let changed = match walk.steps.last() {
                    None => true,
                    Some(Step {
                        parent_info: Some(here),
                        ..
                    }) => parent_info
                        .as_ref()
                        .is_none_or(|p| p.content != here.content),
                    Some(_) => unreachable!("a step without a file at its parent ends the walk"),
                };
                let i = walk.steps.len();
                ended = parent.is_none() || parent_info.is_none();
                walk.steps.push(Step {
                    x,
                    has_parent: parent.is_some(),
                    parent_info,
                });
                if changed && known.contains(&x) {
                    // Another walk of this blame has the file's history
                    // from here on.
                    if i > 0 {
                        walk.hit = Some((i, Hit::Known));
                        walk.resume = None;
                        return Ok(walk);
                    }
                    walk.first_hit = Some(Hit::Known);
                } else if changed {
                    unchecked.push(i);
                }
                if ended {
                    break;
                }
            }
            let done = ended || finished;
            if cache && !unchecked.is_empty() && (done || unchecked.len() >= CACHE_CHECK_BATCH) {
                let keys: Vec<Hash> = unchecked
                    .iter()
                    .map(|&i| cache_key(walk.steps[i].x, path))
                    .collect();
                let found = self.meta().get_derived_many(KIND, &keys).await?;
                for (i, v) in unchecked.drain(..).zip(found) {
                    let Some(v) = v else {
                        continue;
                    };
                    let blame: CachedBlame = postcard::from_bytes(&v)?;
                    if i == 0 {
                        walk.first_hit = Some(Hit::Cached(blame));
                    } else {
                        walk.steps.truncate(i + 1);
                        walk.hit = Some((i, Hit::Cached(blame)));
                        return Ok(walk);
                    }
                }
            }
            if done {
                if ended {
                    walk.resume = None;
                }
                return Ok(walk);
            }
        }
    }

    /// The index entry of `path` at `cur`, a changeset of `chain`, read
    /// together with `cur`'s skiplist: the walk needs the list next, to go
    /// to `cur`'s parent if `cur` changed the file, or to start its descent
    /// to the next change from near `cur`.
    async fn entry_and_list(
        &self,
        chain: &mut FirstParents,
        cur: ChangesetId,
        path: &RepoPath,
    ) -> Result<Lookup> {
        if let Some(floor) = self.bounded.as_ref().and_then(|b| b.floor) {
            // A bounded blame (ADR 0058): first-parent ancestors above the
            // newest indexed one have no row.
            if self.generation(cur).await? > floor {
                if let Some(b) = &self.bounded {
                    b.examined(1);
                }
                chain.list(self, cur).await?;
                return Ok(Lookup::Unknown);
            }
        }
        let (entry, list) = futures_util::future::join(
            self.history_entry(cur, path.as_str()),
            chain.list(self, cur),
        )
        .await;
        list?;
        entry.map(Lookup::Entry)
    }

    /// The changers of `path` from `id` (where the file is `tip`), newest
    /// first, down to the file's creation or to the newest changer with a
    /// cached blame, which is returned with its file: the walk of
    /// [`Repo::walk_stretch`] over the whole first-parent chain, cut into
    /// stretches walked at once.
    ///
    /// Each step of a walk needs the step before it (the index entry at a
    /// changer's parent says where the next change is), so one walk makes a
    /// few round trips per change: thousands, one after another, for a file
    /// changed thousands of times. A stretch needs only its top, so the chain
    /// is cut at first-parent ancestors spread over its generations (found a
    /// level of skiplists at a time, [`Repo::plan_stretches`]), and up to
    /// [`STRETCHES_IN_FLIGHT`] stretches are walked at once, their reads
    /// gathered into batched reads ([`crate::batched_reads`]). Their steps
    /// are then taken in order, newest first, each stretch's first step
    /// judged against the file the one above ended with, down to the first
    /// step that ends the walk (at the file's creation, the chain's root or
    /// a cached blame); the walks below it are dropped. The steps are those
    /// of one walk, so the answer is too.
    ///
    /// Cutting costs a few reads per stretch, wasted on a file changed only
    /// a few times or with a cached blame near `id`. So the first
    /// `cuts.head` steps are walked from `id` while the cuts are planned,
    /// and the stretches are walked only below where those steps stop, if
    /// the walk did not end there.
    ///
    /// A changer in `stop_at` ends the walk as a cached blame does: another
    /// walk of the same blame already has the history below it.
    async fn blame_stretches(
        &self,
        id: ChangesetId,
        tip: FileInfo,
        path: &RepoPath,
        cache: bool,
        cuts: Cuts,
        stop_at: Arc<HashSet<ChangesetId>>,
    ) -> Result<Found> {
        // Derives the index first if it is missing, with the stores as they
        // are: derivation batches its own reads. A bounded blame (ADR 0058)
        // derives nothing and walks the steps without rows one by one.
        if self.bounded.is_none() {
            self.history_root(id).await?;
        }
        let walker = self.with_stores(
            Arc::new(BatchedBlobs::new(self.blobs().clone())),
            Arc::new(BatchedMeta::new(self.meta().clone())),
        );
        let tip_gen = self.generation(id).await?;
        let head = walker.walk_stretch(
            path,
            cache,
            Stretch {
                top: id,
                bottom: 0,
                most: cuts.head,
                first_parents: FirstParents {
                    known: BTreeMap::from([(tip_gen, id)]),
                    lists: HashMap::new(),
                },
                known: stop_at.clone(),
            },
        );
        let plan = self.plan_stretches(id, tip_gen, cuts.min_span);
        let (head, plan) = match futures_util::future::select(Box::pin(head), Box::pin(plan)).await
        {
            futures_util::future::Either::Left((head, plan)) => {
                let head = head?;
                if head.resume.is_none() {
                    (head, None)
                } else {
                    (head, Some(plan.await?))
                }
            }
            futures_util::future::Either::Right((plan, head)) => (head.await?, Some(plan?)),
        };
        let mut found = Found {
            chain: Vec::new(),
            end: End::Start,
            examined: 0,
        };
        let mut cur_info = tip;
        let resume = head.resume;
        if found.take(head, &mut cur_info) {
            return Ok(found);
        }
        let (Some(resume), Some((cuts, mut known, lists))) = (resume, plan) else {
            return Ok(found);
        };
        known.insert(resume.generation, resume.id);
        // The stretch from where the head stopped, then those of the cuts
        // below it.
        let tops: Vec<SkipEntry> = std::iter::once(resume)
            .chain(
                cuts.into_iter()
                    .filter(|c| c.generation < resume.generation),
            )
            .collect();
        let stretches: Vec<Stretch> = tops
            .iter()
            .enumerate()
            .map(|(i, top)| {
                let bottom = tops.get(i + 1).map_or(0, |c| c.generation);
                let known: BTreeMap<u64, ChangesetId> = known
                    .range(bottom..=top.generation)
                    .map(|(g, c)| (*g, *c))
                    .collect();
                let lists = known
                    .values()
                    .filter_map(|c| lists.get(c).map(|l| (*c, l.clone())))
                    .collect();
                Stretch {
                    top: top.id,
                    bottom,
                    most: usize::MAX,
                    first_parents: FirstParents { known, lists },
                    known: stop_at.clone(),
                }
            })
            .collect();
        let mut walks = futures_util::stream::iter(stretches)
            .map(|s| {
                let (repo, path) = (walker.clone(), path.clone());
                let task = Spawned(tokio::spawn(async move {
                    repo.walk_stretch(&path, cache, s).await
                }));
                async move {
                    task.await
                        .map_err(|e| Error::Other(format!("blame walk failed: {e}")))?
                }
            })
            .buffered(STRETCHES_IN_FLIGHT);
        while let Some(walk) = walks.next().await {
            if found.take(walk?, &mut cur_info) {
                break;
            }
        }
        Ok(found)
    }

    /// Cuts for [`Repo::blame_stretches`]: first-parent ancestors of `id`
    /// (itself first), newest first, at most about the larger of `min_span`
    /// and 1/[`STRETCHES`] of `id`'s generation apart, and closer together
    /// near `id`, where a cached blame is most likely to end the walk. Also
    /// returns every first-parent ancestor seen by generation and the
    /// skiplists read, which the stretches' walks start from.
    ///
    /// The skiplist of a cut names ancestors 1, 2, 4, ... steps below it, so
    /// one names a point at least halfway down to the next cut; each round
    /// reads, in one batched read, the lists of the cuts above a gap still
    /// too wide (and of the lowest, until the chain's root is found), and
    /// about log2 of the number of stretches rounds suffice.
    async fn plan_stretches(
        &self,
        id: ChangesetId,
        tip_gen: u64,
        min_span: u64,
    ) -> Result<(Vec<SkipEntry>, BTreeMap<u64, ChangesetId>, SkipLists)> {
        let span = (tip_gen / STRETCHES).max(min_span).max(1);
        let mut known = BTreeMap::from([(tip_gen, id)]);
        let mut lists: SkipLists = HashMap::new();
        loop {
            let mut want: Vec<ChangesetId> = Vec::new();
            let mut below: Option<u64> = None;
            for (&g, &c) in known.iter() {
                // Read the list of a point with a wide gap below it, or with
                // nothing known below it.
                let wide = match below {
                    Some(b) => g - b > span,
                    None => g > span,
                };
                if wide && !lists.contains_key(&c) {
                    want.push(c);
                }
                below = Some(g);
            }
            if want.is_empty() {
                break;
            }
            let keys: Vec<Hash> = want.iter().map(|c| c.0).collect();
            let found = self.meta().get_derived_many(Skiplist::NAME, &keys).await?;
            for (i, c) in want.into_iter().enumerate() {
                let list = match found.get(i).cloned().flatten() {
                    Some(v) => Skiplist::decode(&v)?,
                    None => Skiplist::decode(&derived::derive::<Skiplist>(self, c).await?)?,
                };
                for e in &list {
                    known.insert(e.generation, e.id);
                    self.remember_generation(e.id, e.generation);
                }
                lists.insert(c, Arc::new(list));
            }
        }
        // Keep a point as a cut once it is far enough below the last cut:
        // `span`, or half its own distance from `id`, whichever is less.
        let mut cuts = vec![SkipEntry {
            id,
            generation: tip_gen,
        }];
        for (&g, &c) in known.iter().rev().skip(1) {
            let last = cuts[cuts.len() - 1].generation;
            if last - g >= span.min((tip_gen - g) / 2).max(1) {
                cuts.push(SkipEntry {
                    id: c,
                    generation: g,
                });
            }
        }
        Ok((cuts, known, lists))
    }

    /// [`Repo::lookup_file`] of `path` at each of `ids`, in order: their root
    /// trees in one batched read, then each directory level of the path for
    /// all of them in one [`Repo::read_trees`].
    pub(crate) async fn lookup_files(
        &self,
        ids: &[ChangesetId],
        path: &RepoPath,
    ) -> Result<Vec<Option<FileInfo>>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let keys: Vec<Hash> = ids.iter().map(|id| id.0).collect();
        let stored = self.meta().get_derived_many(RootTree::NAME, &keys).await?;
        if stored.len() != ids.len() {
            return Err(Error::Storage(format!(
                "get_derived_many answered {} of {} root trees",
                stored.len(),
                ids.len()
            )));
        }
        let mut entries: Vec<Option<TreeEntry>> = Vec::with_capacity(ids.len());
        for (id, v) in ids.iter().zip(stored) {
            let tree = match v {
                Some(v) => derived::decode_hash(&v)?,
                None => self.root_tree(*id).await?,
            };
            entries.push(Some(TreeEntry::Dir { tree }));
        }
        for comp in path.as_str().split('/').filter(|c| !c.is_empty()) {
            let dirs: Vec<Hash> = entries
                .iter()
                .filter_map(|e| match e {
                    Some(TreeEntry::Dir { tree }) => Some(*tree),
                    _ => None,
                })
                .collect();
            let mut nodes = self.read_trees(&dirs).await?.into_iter();
            for e in entries.iter_mut() {
                *e = match e {
                    Some(TreeEntry::Dir { .. }) => nodes
                        .next()
                        .expect("one node per directory")
                        .entries
                        .remove(comp),
                    _ => None,
                };
            }
        }
        Ok(entries
            .into_iter()
            .map(|e| e.and_then(|e| file_info(path.clone(), &e)))
            .collect())
    }

    /// Blame of `path` at `id`. `skip` jumps over stretches of first parents
    /// where the index says the file did not change, and walks many
    /// stretches at once; without it every ancestor is read, which tests use
    /// as the reference. `cache` reads and writes cached blames. Returns the
    /// blame and the number of changesets examined.
    pub(crate) async fn blame_walk(
        &self,
        id: ChangesetId,
        path: &RepoPath,
        skip: bool,
        cache: bool,
    ) -> Result<(CachedBlame, usize)> {
        let skip = skip.then_some(Cuts {
            head: HEAD_STEPS,
            min_span: MIN_STRETCH_SPAN,
        });
        self.blame_walk_in(id, path, skip, cache).await
    }

    /// [`Repo::blame_walk`], with skipping if `cuts` are given (tests make
    /// them small to cut short histories into many stretches).
    pub(crate) async fn blame_walk_in(
        &self,
        id: ChangesetId,
        path: &RepoPath,
        cuts: Option<Cuts>,
        cache: bool,
    ) -> Result<(CachedBlame, usize)> {
        let run = self
            .blame_run(id, path, cuts, cache, crate::blame_follow::FOLLOW_WALKS)
            .await?;
        Ok((run.blame, run.examined))
    }

    /// [`Repo::blame_walk_in`] starting at most `most_walks` walks below
    /// merges and renames, and what it did.
    pub(crate) async fn blame_run(
        &self,
        id: ChangesetId,
        path: &RepoPath,
        cuts: Option<Cuts>,
        cache: bool,
        most_walks: usize,
    ) -> Result<BlameRun> {
        let tip = self
            .lookup_file(id, path)
            .await?
            .ok_or_else(|| Error::NotFound(format!("file {path} at {id}")))?;
        if cache {
            if let Some(v) = self.meta().get_derived(KIND, &cache_key(id, path)).await? {
                return Ok(BlameRun {
                    blame: postcard::from_bytes(&v)?,
                    examined: 0,
                    walks: 0,
                    complete: true,
                });
            }
        }
        let graph = self
            .blame_graph(id, tip, path, cuts, cache, most_walks)
            .await?;
        let (examined, walks, latest) = (graph.examined, graph.walks, graph.latest);
        let (resp, complete) = self.replay_graph(graph, path).await?;
        let resp = match resp {
            Replayed::Cached(blame) => {
                return Ok(BlameRun {
                    blame,
                    examined,
                    walks,
                    complete,
                })
            }
            Replayed::Fresh(resp) => resp,
        };
        // A blame that gave up following some merge or rename is not the
        // blame of this changeset, so it is not cached (ADR 0065).
        if cache && complete {
            let value = Bytes::from(postcard::to_stdvec(&resp)?);
            for key_id in latest.into_iter().chain([id]) {
                self.meta()
                    .put_derived(KIND, &cache_key(key_id, path), value.clone())
                    .await?;
            }
        }
        Ok(BlameRun {
            blame: resp,
            examined,
            walks,
            complete,
        })
    }

    /// The changers of `path` down `start`'s first-parent chain, newest
    /// first, where the file at `start` is `tip`: down to the file's
    /// creation, a cached blame or a changer in `stop_at`.
    pub(crate) async fn walk_chain(
        &self,
        start: ChangesetId,
        tip: FileInfo,
        path: &RepoPath,
        cuts: Option<Cuts>,
        cache: bool,
        stop_at: Arc<HashSet<ChangesetId>>,
    ) -> Result<Found> {
        match cuts {
            Some(cuts) => {
                self.blame_stretches(start, tip, path, cache, cuts, stop_at)
                    .await
            }
            None => {
                let mut acc = Changers {
                    chain: Vec::new(),
                    checked: 0,
                    cur_info: tip,
                    end: End::Start,
                    stop_at,
                };
                let (mut cur, mut examined) = (start, 0);
                loop {
                    examined += 1;
                    let parent = self.get_changeset(cur).await?.parents.first().copied();
                    let parent_info = match parent {
                        Some(p) => self.lookup_file(p, path).await?,
                        None => None,
                    };
                    if !acc
                        .step(self, path, cache, cur, parent.is_some(), parent_info)
                        .await?
                    {
                        return Ok(acc.found(examined));
                    }
                    cur = parent.expect("a step continues only with a parent");
                }
            }
        }
    }
}

/// What one blame computed: the blame, the changesets its first-parent
/// walks examined, the walks it started below merges and renames, and
/// whether it followed all of them.
pub(crate) struct BlameRun {
    pub(crate) blame: CachedBlame,
    pub(crate) examined: usize,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) walks: usize,
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) complete: bool,
}

/// One diff of consecutive versions, as `(tag, old lines, new lines)`.
pub(crate) type Ops = Vec<(DiffTag, std::ops::Range<usize>, std::ops::Range<usize>)>;

/// The line diffs of each version in `versions` against the one before
/// ([`diff_pairs`] over consecutive versions). Returns the versions too.
#[cfg(test)]
pub(crate) async fn diff_versions(versions: Vec<String>) -> Result<(Vec<Ops>, Vec<String>)> {
    let texts: Vec<Arc<String>> = versions.iter().cloned().map(Arc::new).collect();
    let pairs = (1..texts.len()).map(|i| (i - 1, i)).collect();
    Ok((diff_pairs(Arc::new(texts), pairs).await?, versions))
}

/// The line diff of each pair `(old, new)` of `texts`, computed on several
/// cores: they do not depend on each other, only applying them does.
///
/// Each diff is the one `TextDiff::configure().timeout(DIFF_TIMEOUT)
/// .diff_slices` makes over [`split_lines`], but cheaper: that numbers the
/// distinct lines for every pair, so every version was split and hashed
/// twice. Here each task splits and numbers each text it diffs once (a
/// chain's pairs share their texts), and diffs the numbers with the same
/// algorithm, which compares lines only for equality and so gives the same
/// diff.
pub(crate) async fn diff_pairs(
    texts: Arc<Vec<Arc<String>>>,
    pairs: Vec<(usize, usize)>,
) -> Result<Vec<Ops>> {
    let pairs = Arc::new(pairs);
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get());
    let per_task = pairs.len().div_ceil(cores.min(DIFF_TASKS)).max(1);
    let mut tasks = Vec::new();
    for start in (0..pairs.len()).step_by(per_task) {
        let (texts, pairs) = (texts.clone(), pairs.clone());
        let end = (start + per_task).min(pairs.len());
        tasks.push(tokio::task::spawn_blocking(move || {
            let mut ids: HashMap<&str, u32> = HashMap::new();
            let mut numbered: HashMap<usize, Vec<u32>> = HashMap::new();
            let mut out = Vec::with_capacity(end - start);
            for &(old, new) in &pairs[start..end] {
                for t in [old, new] {
                    numbered.entry(t).or_insert_with(|| {
                        split_lines(&texts[t])
                            .map(|line| {
                                let next = ids.len() as u32;
                                *ids.entry(line).or_insert(next)
                            })
                            .collect()
                    });
                }
                let (a, b) = (&numbered[&old], &numbered[&new]);
                let deadline = std::time::Instant::now().checked_add(DIFF_TIMEOUT);
                out.push(
                    similar::capture_diff_deadline(
                        Algorithm::Myers,
                        &a[..],
                        0..a.len(),
                        &b[..],
                        0..b.len(),
                        deadline,
                    )
                    .iter()
                    .map(|op| op.as_tag_tuple())
                    .collect::<Ops>(),
                );
            }
            out
        }));
    }
    let mut out = Vec::with_capacity(pairs.len());
    for t in tasks {
        out.extend(
            t.await
                .map_err(|e| Error::Other(format!("blame diff task failed: {e}")))?,
        );
    }
    Ok(out)
}

/// The lines of `text`, each with its `\n`. The one splitter for the diff and
/// for the text blame reports, so their line counts cannot disagree. Only
/// `\n` ends a line, as in the rest of Leep (`\r\n` is `\n` after a `\r`);
/// splitting at a lone `\r` as `similar` does made blame fail on such files.
pub(crate) fn split_lines(text: &str) -> impl Iterator<Item = &str> {
    text.split_inclusive('\n')
}

/// The changesets that changed the file, newest first, as a blame walk
/// finds them.
struct Changers {
    chain: Vec<(ChangesetId, FileInfo)>,
    /// How many of `chain` were looked up in the blame cache.
    checked: usize,
    /// The file at the changeset the walk examines next.
    cur_info: FileInfo,
    /// Where the walk ended.
    end: End,
    /// Changers whose history another walk has.
    stop_at: Arc<HashSet<ChangesetId>>,
}

/// How a blame walk is cut into stretches: steps walked from the tip before
/// the stretches below are, and the fewest generations a stretch spans.
#[derive(Clone, Copy)]
pub(crate) struct Cuts {
    pub(crate) head: usize,
    pub(crate) min_span: u64,
}

/// What a blame's walk found: the changers, newest first, down to `end`;
/// and how many changesets it examined.
pub(crate) struct Found {
    pub(crate) chain: Vec<(ChangesetId, FileInfo)>,
    pub(crate) end: End,
    pub(crate) examined: usize,
}

/// Where a walk down a first-parent chain ended, below its last changer.
pub(crate) enum End {
    /// The file's history starts at the last changer: its first parent
    /// lacks the file, or it has none.
    Start,
    /// At a changer with a cached blame, and the file there.
    Cached(ChangesetId, CachedBlame, FileInfo),
    /// At a changer another walk of the same blame found.
    Known(ChangesetId),
}

/// A step that ends a walk before the file's creation.
enum Hit {
    Cached(CachedBlame),
    Known,
}

impl Found {
    /// Take the steps of the next stretch down, where the file at the top is
    /// `cur_info` (and, after, at the bottom). Returns whether the walk ends
    /// in this stretch.
    fn take(&mut self, walk: StretchWalk, cur_info: &mut FileInfo) -> bool {
        let StretchWalk {
            steps,
            mut first_hit,
            mut hit,
            absent,
            ..
        } = walk;
        if absent {
            return true;
        }
        for (i, step) in steps.into_iter().enumerate() {
            self.examined += 1;
            let changed = step
                .parent_info
                .as_ref()
                .is_none_or(|p| p.content != cur_info.content);
            if changed {
                let found = match &hit {
                    _ if i == 0 => first_hit.take(),
                    Some((j, _)) if *j == i => hit.take().map(|(_, h)| h),
                    _ => None,
                };
                if let Some(h) = found {
                    self.end = match h {
                        Hit::Cached(blame) => End::Cached(step.x, blame, cur_info.clone()),
                        Hit::Known => End::Known(step.x),
                    };
                    return true;
                }
                self.chain.push((step.x, cur_info.clone()));
            }
            match step.parent_info {
                Some(info) if step.has_parent => *cur_info = info,
                _ => return true,
            }
        }
        false
    }
}

/// One examined changeset of a walk, whether it has a first parent, and the
/// file there.
struct Step {
    x: ChangesetId,
    has_parent: bool,
    parent_info: Option<FileInfo>,
}

/// What [`Repo::walk_stretch`] found: the steps, newest first, down to the
/// bottom of the stretch or the end of the walk, and what ends the walk if
/// it changed the file: at the first step, and at the step the walk ended
/// at.
struct StretchWalk {
    steps: Vec<Step>,
    first_hit: Option<Hit>,
    hit: Option<(usize, Hit)>,
    /// Where the walk stopped after its most steps, with the stretch below
    /// still to walk.
    resume: Option<SkipEntry>,
    /// The file never existed at the top: the walk ends above.
    absent: bool,
}

/// One stretch of the first-parent chain for [`Repo::walk_stretch`].
struct Stretch {
    top: ChangesetId,
    /// The generation of the next stretch's top, 0 for none.
    bottom: u64,
    /// Steps to take at most.
    most: usize,
    /// Seeded with the chain's ancestors known between `top` and `bottom`.
    first_parents: FirstParents,
    /// Changers that end the walk (see [`End::Known`]).
    known: Arc<HashSet<ChangesetId>>,
}

/// The path-history entry of a step: `Unknown` when the step has no row in
/// a bounded blame (ADR 0058).
enum Lookup {
    Entry(Option<HistoryEntry>),
    Unknown,
}

/// Where [`Repo::skip_unchanged`] says to go.
enum Skip {
    /// Examine this first-parent ancestor next.
    To(ChangesetId),
    /// Examine the current changeset (the index has no entry for the path).
    Here,
    /// Nothing changed the path above the stretch's bottom.
    Past,
}

/// Skiplists read, by changeset.
type SkipLists = HashMap<ChangesetId, Arc<Vec<SkipEntry>>>;

impl Changers {
    fn found(self, examined: usize) -> Found {
        Found {
            chain: self.chain,
            end: self.end,
            examined,
        }
    }

    /// Look the changers not yet looked up in the blame cache up, and drop
    /// those past the newest found. Returns whether one was found.
    async fn check_cache(&mut self, repo: &Repo, path: &RepoPath) -> Result<bool> {
        if let Some((i, blame)) = repo
            .newest_cached(path, &self.chain[self.checked..])
            .await?
        {
            let i = self.checked + i;
            self.end = End::Cached(self.chain[i].0, blame, self.chain[i].1.clone());
            self.chain.truncate(i);
            return Ok(true);
        }
        self.checked = self.chain.len();
        Ok(false)
    }

    /// Record one examined changeset `x`, whose first parent (if
    /// `has_parent`) holds `parent_info`. Cached blames are looked up a batch
    /// of changers at a time, and the changers past the newest one found are
    /// dropped. Returns whether the walk goes on to the parent.
    async fn step(
        &mut self,
        repo: &Repo,
        path: &RepoPath,
        cache: bool,
        x: ChangesetId,
        has_parent: bool,
        parent_info: Option<FileInfo>,
    ) -> Result<bool> {
        let changed = parent_info
            .as_ref()
            .is_none_or(|p| p.content != self.cur_info.content);
        if changed && self.stop_at.contains(&x) {
            self.end = End::Known(x);
            return Ok(false);
        }
        if changed {
            self.chain.push((x, self.cur_info.clone()));
        }
        let more = has_parent && parent_info.is_some();
        if cache
            && (self.chain.len() - self.checked >= CACHE_CHECK_BATCH || !more)
            && self.check_cache(repo, path).await?
        {
            return Ok(false);
        }
        if let Some(info) = parent_info.filter(|_| more) {
            self.cur_info = info;
        }
        Ok(more)
    }
}

/// The first-parent ancestors of one changeset that a blame walk has seen,
/// by generation (strictly decreasing down a first-parent chain, so one id
/// per generation), and the skiplists read so far. Every skiplist names
/// ancestors 1, 2, 4, ... steps further down the same chain, so the lists
/// read to skip down to one change also name points near the next ones;
/// starting each descent from the nearest known point instead of from the
/// top costs a few skiplist reads per change instead of one per halving of
/// the distance.
struct FirstParents {
    known: BTreeMap<u64, ChangesetId>,
    lists: SkipLists,
}

impl FirstParents {
    async fn list(&mut self, repo: &Repo, id: ChangesetId) -> Result<&[SkipEntry]> {
        if !self.lists.contains_key(&id) {
            let list = Skiplist::decode(&derived::derive::<Skiplist>(repo, id).await?)?;
            for e in &list {
                self.known.insert(e.generation, e.id);
                // A changer is often on this chain; its generation then
                // needs no read of its own.
                repo.remember_generation(e.id, e.generation);
            }
            self.lists.insert(id, Arc::new(list));
        }
        Ok(&self.lists[&id])
    }

    /// The first parent of `id`, a changeset of this chain.
    async fn parent(&mut self, repo: &Repo, id: ChangesetId) -> Result<Option<SkipEntry>> {
        Ok(self.list(repo, id).await?.first().copied())
    }

    /// The lowest changeset of the chain with a generation of at least `g`.
    /// The tip is at least that high: `g` is the generation of an ancestor.
    async fn lowest_at_or_above(&mut self, repo: &Repo, g: u64) -> Result<ChangesetId> {
        let (_, &start) =
            self.known.range(g..).next().ok_or_else(|| {
                Error::Other(format!("no first-parent ancestor at generation {g}"))
            })?;
        let mut x = start;
        loop {
            // The known point nearest above `g` has no known point between it
            // and `g`, so only its own list can name one.
            match self
                .list(repo, x)
                .await?
                .iter()
                .rev()
                .find(|e| e.generation >= g)
            {
                Some(e) => x = e.id,
                None => return Ok(x),
            }
        }
    }
}

/// A spawned task, aborted when dropped: a blame that stops early (at a
/// cached blame or the file's creation) does not leave walks running.
struct Spawned<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for Spawned<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl<T> std::future::Future for Spawned<T> {
    type Output = std::result::Result<T, tokio::task::JoinError>;

    fn poll(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Self::Output> {
        std::pin::Pin::new(&mut self.0).poll(cx)
    }
}
