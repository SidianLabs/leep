//! Line-level blame over first-parent history, cached as derived data.

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use bytes::Bytes;
use leep_core::api::{BlameLine, BlameResponse, ChangesetSummary, FileInfo};
use leep_core::{ChangesetId, Error, Hash, RepoPath, Result, TreeEntry};
use similar::{DiffTag, TextDiff};

use crate::derived::{self, DerivedKind, HistoryEntry, RootTree, Skiplist};
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

/// File versions read at once (one `get_many`) when replaying them. Bounds
/// the memory held for a file with thousands of versions.
const CONTENT_BATCH: usize = 64;

/// Blocking tasks one window of versions is diffed on.
const DIFF_TASKS: usize = 4;

/// Steps of an indexed blame walk whose parents' files are read together.
const LOOKUP_BATCH: usize = 64;

/// What is cached under `blame`. Kept at the original `{path, lines}` shape so
/// entries written before changeset summaries existed still decode; summaries
/// are attached on every read.
/// A tuple, because postcard encodes it identically to the old struct and
/// this crate has no serde dependency of its own.
pub(crate) type CachedBlame = (RepoPath, Vec<BlameLine>);

pub(crate) fn cache_key(id: ChangesetId, path: &RepoPath) -> Hash {
    let mut b = id.0.as_bytes().to_vec();
    b.extend_from_slice(path.as_str().as_bytes());
    Hash::of_object("blame", &b)
}

fn strip_eol(line: &str) -> String {
    let l = line.strip_suffix('\n').unwrap_or(line);
    l.strip_suffix('\r').unwrap_or(l).to_string()
}

impl Repo {
    async fn utf8_content(&self, info: &FileInfo) -> Result<String> {
        let data = self
            .blobs()
            .get(&info.content)
            .await?
            .ok_or_else(|| Error::Storage(format!("blob {} missing", info.content)))?;
        String::from_utf8(data.to_vec())
            .map_err(|_| Error::Invalid(format!("{} is not valid UTF-8", info.path)))
    }

    /// [`Repo::utf8_content`] of each of `infos`, in order, with one
    /// `get_many`.
    async fn utf8_contents(&self, infos: impl Iterator<Item = &FileInfo>) -> Result<Vec<String>> {
        let infos: Vec<&FileInfo> = infos.collect();
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
            .into_iter()
            .zip(found)
            .map(|(info, data)| {
                let data =
                    data.ok_or_else(|| Error::Storage(format!("blob {} missing", info.content)))?;
                String::from_utf8(data.to_vec())
                    .map_err(|_| Error::Invalid(format!("{} is not valid UTF-8", info.path)))
            })
            .collect()
    }

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
        let found = self.meta().get_derived_many("blame", &keys).await?;
        for (i, v) in found.into_iter().enumerate() {
            if let Some(v) = v {
                return Ok(Some((i, postcard::from_bytes(&v)?)));
            }
        }
        Ok(None)
    }

    /// Attribute each line of `path` at `id` to the changeset that introduced
    /// it, following first parents, and describe each distinct changeset.
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
        })
    }

    async fn blame_lines(&self, id: ChangesetId, path: &RepoPath) -> Result<CachedBlame> {
        Ok(self.blame_walk(id, path, true, true).await?.0)
    }

    /// The lowest first-parent ancestor of `cur` (itself included) that blame
    /// must examine, given `entry`, the path-history index entry of the path
    /// at `cur`: no changeset above it changed the path. Without this, blame
    /// visits every ancestor down to the changeset that last changed the
    /// file, which is the whole history for a file that has not been touched
    /// in a long time.
    ///
    /// The index entry holds the maximal changers of the path under ancestry,
    /// so every changer's generation is at most the largest one, `g`. A
    /// first-parent ancestor with a generation above `g` cannot be a changer,
    /// and [`FirstParents::lowest_at_or_above`] finds the last one at or
    /// above `g`.
    async fn skip_unchanged(
        &self,
        chain: &mut FirstParents,
        entry: Option<HistoryEntry>,
    ) -> Result<Option<ChangesetId>> {
        let Some(entry) = entry else {
            return Ok(None);
        };
        let gens =
            futures_util::future::try_join_all(entry.changed().iter().map(|&c| self.generation(c)))
                .await?;
        let Some(g) = gens.into_iter().max() else {
            return Ok(None);
        };
        chain.lowest_at_or_above(self, g).await.map(Some)
    }

    /// The first-parent walk of [`Repo::blame_walk`] with skipping: from
    /// each changeset, the path-history index and the skiplists say which
    /// first-parent ancestor to examine next, without reading files. The
    /// files at the examined changesets' parents, which decide whether each
    /// changed the file and where the walk ends, are then read a batch of
    /// steps at a time ([`Repo::lookup_files`]), and the steps replayed in
    /// order. A step past the end of the file's history costs only index
    /// reads, and at most a batch of them is wasted.
    async fn blame_walk_indexed(
        &self,
        id: ChangesetId,
        path: &RepoPath,
        cache: bool,
        acc: &mut Changers,
    ) -> Result<usize> {
        let mut first_parents = FirstParents::new(self, id).await?;
        let mut entry = self.history_entry(id, path.as_str()).await?;
        let mut cur = id;
        let mut pending: Vec<(ChangesetId, Option<ChangesetId>)> = Vec::new();
        let mut examined = 0;
        loop {
            // Nothing changed between `cur` and the changeset skipped to, so
            // the file there is the one the last step found.
            let x = match self
                .skip_unchanged(&mut first_parents, entry.take())
                .await?
            {
                Some(x) => x,
                None => cur,
            };
            let parent = first_parents.parent(self, x).await?;
            pending.push((x, parent));
            if let Some(p) = parent {
                entry = self.history_entry(p, path.as_str()).await?;
                cur = p;
            }
            // No entry at the parent: the file never existed there, which
            // the replay finds and stops at.
            if pending.len() < LOOKUP_BATCH && parent.is_some() && entry.is_some() {
                continue;
            }
            let parents: Vec<ChangesetId> = pending.iter().filter_map(|(_, p)| *p).collect();
            let mut infos = self.lookup_files(&parents, path).await?.into_iter();
            for (x, parent) in pending.drain(..) {
                examined += 1;
                let parent_info = match parent {
                    Some(_) => infos.next().expect("one file per parent"),
                    None => None,
                };
                if !acc
                    .step(self, path, cache, x, parent.is_some(), parent_info)
                    .await?
                {
                    return Ok(examined);
                }
            }
        }
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

    /// Blame by walking first parents. `skip` jumps over stretches where the
    /// index says the file did not change; without it every ancestor is read,
    /// which tests use as the reference. `cache` reads and writes cached
    /// blames. Returns the blame and the number of changesets examined.
    pub(crate) async fn blame_walk(
        &self,
        id: ChangesetId,
        path: &RepoPath,
        skip: bool,
        cache: bool,
    ) -> Result<(CachedBlame, usize)> {
        let tip = self
            .lookup_file(id, path)
            .await?
            .ok_or_else(|| Error::NotFound(format!("file {path} at {id}")))?;
        if cache {
            if let Some(v) = self
                .meta()
                .get_derived("blame", &cache_key(id, path))
                .await?
            {
                return Ok((postcard::from_bytes(&v)?, 0));
            }
        }
        // Walk first parents collecting the changesets that changed the file,
        // stopping at its creation or at a cached blame.
        let mut acc = Changers {
            chain: Vec::new(),
            checked: 0,
            cur_info: tip,
            cached: None,
        };
        let examined = if skip {
            self.blame_walk_indexed(id, path, cache, &mut acc).await?
        } else {
            let (mut cur, mut examined) = (id, 0);
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
                    break examined;
                }
                cur = parent.expect("a step continues only with a parent");
            }
        };
        let Changers { chain, cached, .. } = acc;
        let latest = chain.first().map(|(c, _)| *c);
        // Lines are tracked as (changeset, line in it) while the versions are
        // replayed, and given their text from the last version at the end:
        // a line kept by a diff has the same text in both versions, so
        // copying texts through every version only cost time.
        let (mut lines, mut old_text): (Vec<(ChangesetId, u32)>, String) = match cached {
            Some((resp, info)) => {
                if chain.is_empty() {
                    return Ok((resp, examined));
                }
                let lines = resp
                    .1
                    .iter()
                    .map(|l| (l.changeset, l.origin_line))
                    .collect();
                (lines, self.utf8_content(&info).await?)
            }
            None => (Vec::new(), String::new()),
        };
        let oldest_first: Vec<(ChangesetId, FileInfo)> = chain.into_iter().rev().collect();
        for window in oldest_first.chunks(CONTENT_BATCH) {
            let texts = self.utf8_contents(window.iter().map(|(_, i)| i)).await?;
            let mut versions = Vec::with_capacity(texts.len() + 1);
            versions.push(std::mem::take(&mut old_text));
            versions.extend(texts);
            let (ops, mut versions) = diff_versions(versions).await?;
            for ((cid, _), ops) in window.iter().zip(ops) {
                let mut next = Vec::with_capacity(lines.len());
                for (tag, old, new) in ops {
                    match tag {
                        DiffTag::Equal => next.extend_from_slice(&lines[old]),
                        DiffTag::Delete => {}
                        DiffTag::Insert | DiffTag::Replace => {
                            next.extend(new.map(|i| (*cid, i as u32 + 1)));
                        }
                    }
                }
                lines = next;
            }
            old_text = versions.pop().expect("the newest version");
        }
        let texts: Vec<&str> = old_text.split_inclusive('\n').collect();
        if texts.len() != lines.len() {
            return Err(Error::Other(format!(
                "blame of {path} has {} lines for {} lines of text",
                lines.len(),
                texts.len()
            )));
        }
        let lines: Vec<BlameLine> = lines
            .into_iter()
            .zip(texts)
            .map(|((changeset, origin_line), text)| BlameLine {
                changeset,
                origin_line,
                text: strip_eol(text),
            })
            .collect();
        let resp: CachedBlame = (path.clone(), lines);
        if cache {
            let value = Bytes::from(postcard::to_stdvec(&resp)?);
            for key_id in latest.into_iter().chain([id]) {
                self.meta()
                    .put_derived("blame", &cache_key(key_id, path), value.clone())
                    .await?;
            }
        }
        Ok((resp, examined))
    }
}

/// One diff of consecutive versions, as `(tag, old lines, new lines)`.
type Ops = Vec<(DiffTag, std::ops::Range<usize>, std::ops::Range<usize>)>;

/// The line diffs of each version in `versions` against the one before,
/// computed on several cores: they do not depend on each other, only
/// applying them does. Returns the versions too.
async fn diff_versions(versions: Vec<String>) -> Result<(Vec<Ops>, Vec<String>)> {
    let versions = std::sync::Arc::new(versions);
    let pairs = versions.len().saturating_sub(1);
    let per_task = pairs.div_ceil(DIFF_TASKS).max(1);
    let mut tasks = Vec::new();
    for start in (0..pairs).step_by(per_task) {
        let versions = versions.clone();
        let end = (start + per_task).min(pairs);
        tasks.push(tokio::task::spawn_blocking(move || {
            (start..end)
                .map(|i| {
                    TextDiff::configure()
                        .timeout(DIFF_TIMEOUT)
                        .diff_lines(&versions[i], &versions[i + 1])
                        .ops()
                        .iter()
                        .map(|op| op.as_tag_tuple())
                        .collect::<Ops>()
                })
                .collect::<Vec<Ops>>()
        }));
    }
    let mut out = Vec::with_capacity(pairs);
    for t in tasks {
        out.extend(
            t.await
                .map_err(|e| Error::Other(format!("blame diff task failed: {e}")))?,
        );
    }
    let versions = std::sync::Arc::try_unwrap(versions).unwrap_or_else(|v| (*v).clone());
    Ok((out, versions))
}

/// The changesets that changed the file, newest first, as a blame walk
/// finds them.
struct Changers {
    chain: Vec<(ChangesetId, FileInfo)>,
    /// How many of `chain` were looked up in the blame cache.
    checked: usize,
    /// The file at the changeset the walk examines next.
    cur_info: FileInfo,
    /// The newest cached blame found, and the file it is of; the walk ends
    /// there.
    cached: Option<(CachedBlame, FileInfo)>,
}

impl Changers {
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
        if changed {
            self.chain.push((x, self.cur_info.clone()));
        }
        let more = has_parent && parent_info.is_some();
        if cache && (self.chain.len() - self.checked >= CACHE_CHECK_BATCH || !more) {
            if let Some((i, blame)) = repo
                .newest_cached(path, &self.chain[self.checked..])
                .await?
            {
                let i = self.checked + i;
                self.cached = Some((blame, self.chain[i].1.clone()));
                self.chain.truncate(i);
                return Ok(false);
            }
            self.checked = self.chain.len();
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
    lists: HashMap<ChangesetId, Vec<derived::SkipEntry>>,
}

impl FirstParents {
    async fn new(repo: &Repo, tip: ChangesetId) -> Result<Self> {
        let mut known = BTreeMap::new();
        known.insert(repo.generation(tip).await?, tip);
        Ok(FirstParents {
            known,
            lists: HashMap::new(),
        })
    }

    async fn list(&mut self, repo: &Repo, id: ChangesetId) -> Result<&[derived::SkipEntry]> {
        if !self.lists.contains_key(&id) {
            let list = Skiplist::decode(&derived::derive::<Skiplist>(repo, id).await?)?;
            for e in &list {
                self.known.insert(e.generation, e.id);
                // A changer is often on this chain; its generation then
                // needs no read of its own.
                repo.remember_generation(e.id, e.generation);
            }
            self.lists.insert(id, list);
        }
        Ok(&self.lists[&id])
    }

    /// The first parent of `id`, a changeset of this chain.
    async fn parent(&mut self, repo: &Repo, id: ChangesetId) -> Result<Option<ChangesetId>> {
        Ok(self.list(repo, id).await?.first().map(|e| e.id))
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
