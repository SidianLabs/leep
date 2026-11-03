//! Blames kept current as changes land (ADR 0083).
//!
//! A blame of a file changed thousands of times takes seconds to a minute
//! cold, and on hot history every land changes the hot files, so the next
//! request walks and diffs every changer since the last cached blame. Here
//! the files people blame are tracked ([`BlameTracker`], seeded by
//! answered blame requests, bounded and forgotten when nobody asks for a
//! while), and [`BlameWarmer`] follows each ref's first-parent chain: for
//! each landed changeset and each tracked file it wrote, it reads the
//! cached blame of the file's newest changer at the changeset's parent and
//! makes the changeset's own with one line diff ([`Repo::blame_extend`]).
//! No walks: a file whose chain of cached blames is broken (a merge, a
//! rename, a restart, a gap) is left cold on that ref until a request
//! blames it again.
//!
//! The warmer runs as a loop of its own beside the index warmers, on the
//! shared connections (it writes caches, as requests do, and must not take
//! the index lane's), and only reads the path-history index where it is
//! already derived, so it never holds back lands or the index.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use bytes::Bytes;
use futures_util::StreamExt;
use leep_core::{Changeset, ChangesetId, FileChange, Hash, RepoPath, Result};

use crate::blame::{apply_base, cache_key, diff_pairs, split_lines, CachedBlame, KIND};
use crate::blame_flight::BlameHold;
use crate::blame_follow::credited;
use crate::derived::{DerivedKind, HistoryTree};
use crate::warm::RefTail;
use crate::Repo;

/// Files tracked at once by default; the least recently blamed go first.
pub const TRACKED_PATHS: usize = 1024;

/// A tracked file nobody blamed for this long is forgotten.
pub const TRACK_FOR: Duration = Duration::from_secs(30 * 60);

/// Changesets a ref may move past the warmer before it gives up on them and
/// starts again at the head: the gap's files go cold until blamed again.
const MAX_BEHIND: u64 = 1024;

/// Work one pass does before it leaves the rest to the next, so a burst of
/// lands never keeps the warmer busy for long.
const PASS_BUDGET: Duration = Duration::from_millis(200);

/// Files of one changeset extended at once.
const FILES_AT_ONCE: usize = 4;

/// Larger versions are left to requests: they are where diffs cost.
const MAX_FILE_BYTES: u64 = 1 << 20;

/// Changesets remembered as extended, so refs sharing history (a review
/// ref and its base) extend each changeset once.
const EXTENDED_MEMORY: usize = 4096;

/// What a pass can ask before working: a `blame` read-class permit on a
/// server (ADR 0074), so the warmer's work counts against the class and
/// backs off while requests fill it. `None` refuses the pass.
pub type WarmGate = Arc<dyn Fn() -> Option<BlameHold> + Send + Sync>;

/// The files whose blames are kept current: those blamed recently, at
/// most `capacity`, shared by clones of a [`Repo`].
pub(crate) struct BlameTracker {
    capacity: usize,
    ttl: Duration,
    paths: Mutex<HashMap<RepoPath, Tracked>>,
}

struct Tracked {
    blamed: Instant,
    /// Refs where the file's chain of cached blames broke since it was
    /// last blamed: skipped there without a read.
    cold: HashSet<String>,
}

impl BlameTracker {
    pub(crate) fn new(capacity: usize, ttl: Duration) -> Self {
        BlameTracker {
            capacity,
            ttl,
            paths: Mutex::default(),
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<RepoPath, Tracked>> {
        self.paths.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// `path` was blamed: track it, warm again on every ref.
    pub(crate) fn track(&self, path: &RepoPath) {
        if self.capacity == 0 {
            return;
        }
        let mut paths = self.lock();
        paths.insert(
            path.clone(),
            Tracked {
                blamed: Instant::now(),
                cold: HashSet::new(),
            },
        );
        if paths.len() > self.capacity {
            let oldest = paths
                .iter()
                .min_by_key(|(_, t)| t.blamed)
                .map(|(p, _)| p.clone());
            if let Some(p) = oldest {
                paths.remove(&p);
            }
        }
        metrics::gauge!("leep_blame_tracked_paths").set(paths.len() as f64);
    }

    /// Whether `path`'s blame is kept current on `r`.
    fn wants(&self, path: &RepoPath, r: &str) -> bool {
        self.lock()
            .get(path)
            .is_some_and(|t| t.blamed.elapsed() < self.ttl && !t.cold.contains(r))
    }

    /// `path`'s chain of cached blames broke on `r`.
    fn cool(&self, path: &RepoPath, r: &str) {
        if let Some(t) = self.lock().get_mut(path) {
            t.cold.insert(r.to_string());
        }
    }

    /// Forget expired files; whether any remain.
    fn any(&self) -> bool {
        let mut paths = self.lock();
        paths.retain(|_, t| t.blamed.elapsed() < self.ttl);
        metrics::gauge!("leep_blame_tracked_paths").set(paths.len() as f64);
        !paths.is_empty()
    }

    #[cfg(test)]
    pub(crate) fn len(&self) -> usize {
        self.lock().len()
    }

    #[cfg(test)]
    pub(crate) fn wants_for_tests(&self, path: &RepoPath, r: &str) -> bool {
        self.wants(path, r)
    }

    #[cfg(test)]
    pub(crate) fn cool_for_tests(&self, path: &RepoPath, r: &str) {
        self.cool(path, r)
    }

    #[cfg(test)]
    pub(crate) fn any_for_tests(&self) -> bool {
        self.any()
    }
}

impl Default for BlameTracker {
    fn default() -> Self {
        BlameTracker::new(TRACKED_PATHS, TRACK_FOR)
    }
}

/// What [`Repo::blame_extend`] did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Extended {
    /// Wrote the changeset's blame of the file.
    Written,
    /// The parent's newest changer has no cached blame: the chain broke.
    Missed,
    /// Not one diff away (a creation or rename, a file that is not UTF-8
    /// or too large): left to requests.
    Skipped,
}

impl Extended {
    fn label(self) -> &'static str {
        match self {
            Extended::Written => "written",
            Extended::Missed => "missed",
            Extended::Skipped => "skipped",
        }
    }
}

impl Repo {
    /// Cache the blame of `path` at `id`, which has the single parent
    /// `parent` and writes `content` there, from the cached blame at the
    /// parent's newest changer of `path` and one line diff, when that row
    /// exists and the file is not new at `id`. The row is the one a request
    /// for this blame would cache: a version that changed a file present at
    /// its first parent is replayed from that version alone
    /// ([`crate::blame::apply_base`]), and the parent's version is its
    /// newest changer's, whose cached blame a request's walk would end at.
    pub(crate) async fn blame_extend(
        &self,
        id: ChangesetId,
        parent: ChangesetId,
        path: &RepoPath,
        content: Hash,
        size: u64,
    ) -> Result<Extended> {
        if size > MAX_FILE_BYTES {
            return Ok(Extended::Skipped);
        }
        let Some(old) = self.lookup_file(parent, path).await? else {
            // New here, or renamed: the blame follows another file.
            return Ok(Extended::Skipped);
        };
        let Some((_, (_, lines))) = self.newest_changer_blame(parent, path).await? else {
            return Ok(Extended::Missed);
        };
        let mut texts = self
            .read_contents(&[old.content, content])
            .await?
            .into_iter()
            .map(|b| b.and_then(|b| String::from_utf8(b.to_vec()).ok()));
        let (Some(Some(old_text)), Some(Some(new_text))) = (texts.next(), texts.next()) else {
            // A request's blame fails on a version that is not UTF-8.
            return Ok(Extended::Skipped);
        };
        if split_lines(&old_text).count() != lines.len() {
            return Err(leep_core::Error::Storage(format!(
                "cached blame of {path} at the newest changer below {parent} does not match its file"
            )));
        }
        let from: Vec<(ChangesetId, u32)> =
            lines.iter().map(|l| (l.changeset, l.origin_line)).collect();
        let new_text = Arc::new(new_text);
        let ops = diff_pairs(
            Arc::new(vec![Arc::new(old_text), new_text.clone()]),
            vec![(0, 1)],
        )
        .await?
        .pop()
        .expect("one diff per pair");
        let (lines, _) = apply_base(&ops, &from, id);
        let blame: CachedBlame = credited(path, &new_text, &lines)?;
        let value = Bytes::from(postcard::to_stdvec(&blame)?);
        self.meta()
            .put_derived(KIND, &cache_key(id, path), value)
            .await?;
        Ok(Extended::Written)
    }
}

/// What one [`BlameWarmer::pass`] did.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct BlameWarmReport {
    /// Changesets looked at.
    pub changesets: usize,
    /// Blames written.
    pub written: usize,
    /// Tracked files whose chain broke.
    pub missed: usize,
    /// Tracked files left to requests.
    pub skipped: usize,
    /// The gate refused the pass.
    pub shed: bool,
}

/// Keeps the cached blames of tracked files current as refs move (see the
/// module docs).
pub struct BlameWarmer {
    repo: Repo,
    tail: RefTail,
    /// Each ref's head, as last seen.
    heads: HashMap<String, ChangesetId>,
    /// The changeset each ref's blames were brought up to.
    at: HashMap<String, ChangesetId>,
    /// Changesets extended lately, on any ref.
    extended: HashSet<ChangesetId>,
    gate: Option<WarmGate>,
    budget: Duration,
}

impl BlameWarmer {
    pub fn new(repo: Repo) -> Self {
        BlameWarmer {
            repo,
            tail: RefTail::default(),
            heads: HashMap::new(),
            at: HashMap::new(),
            extended: HashSet::new(),
            gate: None,
            budget: PASS_BUDGET,
        }
    }

    /// Take a permit from `gate` before each pass that has work.
    pub fn with_gate(mut self, gate: WarmGate) -> Self {
        self.gate = Some(gate);
        self
    }

    /// List every ref this often instead of the default (tests).
    pub fn with_full_listing_every(mut self, every: Duration) -> Self {
        self.tail.full_every = every;
        self
    }

    /// Bring the tracked files' blames up to every moved ref's head, as far
    /// as the path-history index reaches and the pass's budget allows.
    pub async fn pass(&mut self) -> Result<BlameWarmReport> {
        let started = Instant::now();
        let moves = self.tail.moved(self.repo.meta()).await?;
        if moves.full {
            self.heads.retain(|name, _| moves.moved.contains_key(name));
            self.at.retain(|name, _| moves.moved.contains_key(name));
        }
        for name in &moves.deleted {
            self.heads.remove(name);
            self.at.remove(name);
        }
        for (name, m) in moves.moved {
            self.heads.insert(name, m.head);
        }
        let mut report = BlameWarmReport::default();
        let tracking = self.repo.blame_tracker.any();
        let mut hold: Option<BlameHold> = None;
        let behind: Vec<(String, ChangesetId)> = self
            .heads
            .iter()
            .filter(|(name, head)| self.at.get(*name) != Some(head))
            .map(|(name, head)| (name.clone(), *head))
            .collect();
        for (name, head) in behind {
            let from = match self.at.get(&name) {
                Some(&h) if tracking => h,
                // A ref seen first, or nothing tracked: start at its head.
                _ => {
                    self.at.insert(name, head);
                    continue;
                }
            };
            if started.elapsed() >= self.budget {
                break;
            }
            if hold.is_none() {
                if let Some(gate) = &self.gate {
                    match gate() {
                        Some(h) => hold = Some(h),
                        None => {
                            report.shed = true;
                            metrics::counter!("leep_warm_blame_shed_total").increment(1);
                            break;
                        }
                    }
                }
            }
            let Some(new) = self.landed(from, head).await? else {
                self.at.insert(name, head);
                continue;
            };
            // The index answers at a parent only once derived there; the
            // rest waits for a later pass.
            let parents: Vec<Hash> = new.iter().map(|(_, cs)| cs.parents[0].0).collect();
            let rows = self
                .repo
                .meta()
                .get_derived_many(HistoryTree::NAME, &parents)
                .await?;
            for ((id, cs), row) in new.into_iter().zip(rows) {
                if row.is_none() || started.elapsed() >= self.budget {
                    break;
                }
                report.changesets += 1;
                if self.extended.insert(id) {
                    if let Err(e) = self.extend(&name, id, &cs, &mut report).await {
                        self.extended.remove(&id);
                        return Err(e);
                    }
                }
                self.at.insert(name.clone(), id);
            }
            if self.extended.len() > EXTENDED_MEMORY {
                self.extended.clear();
            }
        }
        drop(hold);
        if report.changesets > 0 {
            metrics::histogram!("leep_warm_duration_seconds", "group" => "blame")
                .record(started.elapsed().as_secs_f64());
        }
        Ok(report)
    }

    /// The changesets on `head`'s first-parent chain above `from`, oldest
    /// first. `None` when `from` is not on that chain (the ref was moved
    /// back or sideways) or lies more than [`MAX_BEHIND`] below.
    async fn landed(
        &self,
        from: ChangesetId,
        head: ChangesetId,
    ) -> Result<Option<Vec<(ChangesetId, Changeset)>>> {
        let (low, high) =
            futures_util::future::try_join(self.repo.generation(from), self.repo.generation(head))
                .await?;
        if high <= low || high - low > MAX_BEHIND {
            return Ok(None);
        }
        let mut out = Vec::with_capacity((high - low) as usize);
        let mut cur = head;
        for _ in low..high {
            let cs = self.repo.get_changeset(cur).await?;
            let Some(&parent) = cs.parents.first() else {
                return Ok(None);
            };
            out.push((cur, cs));
            cur = parent;
            if cur == from {
                out.reverse();
                return Ok(Some(out));
            }
        }
        Ok(None)
    }

    /// Extend the blames of the tracked files `cs` (at `id`, on ref `r`)
    /// wrote.
    async fn extend(
        &self,
        r: &str,
        id: ChangesetId,
        cs: &Changeset,
        report: &mut BlameWarmReport,
    ) -> Result<()> {
        let tracker = &self.repo.blame_tracker;
        let files: Vec<(RepoPath, Hash, u64)> = cs
            .changes
            .iter()
            .filter_map(|(p, c)| match c {
                FileChange::Modify { content, size, .. } if tracker.wants(p, r) => {
                    Some((p.clone(), *content, *size))
                }
                _ => None,
            })
            .collect();
        if files.is_empty() {
            return Ok(());
        }
        let results: Vec<(RepoPath, Result<Extended>)> = match cs.parents.as_slice() {
            // A merge's blame needs a walk of the merged branch.
            &[parent] => {
                futures_util::stream::iter(files)
                    .map(|(p, content, size)| {
                        let repo = self.repo.clone();
                        async move {
                            let r = repo.blame_extend(id, parent, &p, content, size).await;
                            (p, r)
                        }
                    })
                    .buffered(FILES_AT_ONCE)
                    .collect()
                    .await
            }
            _ => files
                .into_iter()
                .map(|(p, _, _)| (p, Ok(Extended::Skipped)))
                .collect(),
        };
        for (path, result) in results {
            let done = result?;
            match done {
                Extended::Written => report.written += 1,
                Extended::Missed => report.missed += 1,
                Extended::Skipped => report.skipped += 1,
            }
            if done != Extended::Written {
                tracker.cool(&path, r);
            }
            metrics::counter!("leep_warm_blame_total", "result" => done.label()).increment(1);
        }
        Ok(())
    }

    /// Run passes forever, `interval` apart, on the shared connections.
    /// Aborting the handle stops them.
    pub fn spawn(mut self, interval: Duration) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            loop {
                if let Err(e) = self.pass().await {
                    tracing::warn!(error = %e, "keeping blames current failed");
                }
                tokio::time::sleep(interval).await;
            }
        })
    }
}
