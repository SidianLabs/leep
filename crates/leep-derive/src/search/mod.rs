//! Server-side code search over a revision (ADR 0014).
//!
//! Two derived kinds make it fast: `blob-trigrams-v1` (the trigram set of one
//! text blob) and `tree-trigrams-v1` (a bloom filter over every trigram below
//! a directory). A query names trigrams every matching file must contain
//! ([`plan`]); directories and files that lack them are skipped unread, and
//! only the survivors are fetched and matched line by line. Both kinds are
//! keyed by content hash, so a revision shares rows with its parent for every
//! subtree and blob it did not touch.

pub(crate) mod index;
#[doc(hidden)]
pub mod plan;
pub mod trigram;

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use bytes::Bytes;
use futures_util::{stream, StreamExt};
use globset::{GlobBuilder, GlobMatcher};
use leep_core::api::{SearchMatch, SearchResponse, SearchStats};
use leep_core::{ChangesetId, Error, FileKind, Hash, RepoPath, Result, TreeEntry, TreeNode};

pub use index::{BLOB_KIND, TREE_KIND};
pub use trigram::MAX_INDEXED_BYTES;

use index::{Counters, CONCURRENCY};
use plan::Compiled;
use trigram::is_text;

use crate::cursor::{self, CursorKind, Fingerprint, SearchPos};
use crate::Repo;

pub const DEFAULT_LIMIT: usize = 100;
pub const MAX_LIMIT: usize = 1000;
/// Bytes of blob content one search may fetch before it stops and reports
/// `truncated`. Bounds a query the index cannot narrow.
pub const DEFAULT_MAX_SCAN_BYTES: u64 = 256 << 20;

/// What to look for and where.
#[derive(Clone, Debug)]
pub struct SearchQuery {
    pub pattern: String,
    /// Treat `pattern` as a regular expression instead of a literal.
    pub regex: bool,
    pub case_insensitive: bool,
    /// Only files at or below this path; empty means the whole tree.
    pub path: String,
    /// `globset` pattern matched against the full path; `*` stays within one
    /// path level and `**` crosses levels.
    pub glob: Option<String>,
    /// Clamped to `1..=MAX_LIMIT`.
    pub limit: usize,
    pub max_scan_bytes: u64,
    /// False scans every file without consulting the index (the baseline the
    /// index must agree with).
    pub use_index: bool,
}

impl SearchQuery {
    pub fn new(pattern: impl Into<String>) -> Self {
        SearchQuery {
            pattern: pattern.into(),
            regex: false,
            case_insensitive: false,
            path: String::new(),
            glob: None,
            limit: DEFAULT_LIMIT,
            max_scan_bytes: DEFAULT_MAX_SCAN_BYTES,
            use_index: true,
        }
    }
}

/// Rows `derive_search` had to compute.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SearchIndexReport {
    pub blob_rows: u64,
    pub tree_rows: u64,
}

struct FileRef {
    path: String,
    content: Hash,
    kind: FileKind,
    size: u64,
    /// The file's `blob-trigrams-v1` row, when the walk read it with its
    /// directory's batch; `None` means look it up (and derive it if missing).
    row: Option<Bytes>,
    /// Lines up to this one were shown by an earlier page.
    skip_lines: u64,
}

enum Outcome {
    Pruned,
    Skipped {
        fetched: u64,
    },
    Scanned {
        fetched: u64,
        matches: Vec<SearchMatch>,
    },
}

impl Outcome {
    fn fetched(&self) -> u64 {
        match self {
            Outcome::Pruned => 0,
            Outcome::Skipped { fetched } | Outcome::Scanned { fetched, .. } => *fetched,
        }
    }
}

/// Path and glob restrictions, and where a resumed page starts.
struct Filter {
    glob: Option<GlobMatcher>,
    /// Leading directories every match of the glob lies under.
    glob_dir: String,
    /// A resumed page: only matches after this line of this path.
    after: Option<(String, u64)>,
}

impl Filter {
    fn new(glob: Option<&str>, after: Option<(String, u64)>) -> Result<Self> {
        let Some(g) = glob else {
            return Ok(Filter {
                glob: None,
                glob_dir: String::new(),
                after,
            });
        };
        let m = GlobBuilder::new(g)
            .literal_separator(true)
            .build()
            .map_err(|e| Error::Invalid(format!("bad glob {g:?}: {e}")))?
            .compile_matcher();
        let comps: Vec<&str> = g.split('/').collect();
        let glob_dir = comps[..comps.len() - 1]
            .iter()
            .take_while(|c| !c.contains(['*', '?', '[', ']', '{', '}', '\\']))
            .copied()
            .collect::<Vec<_>>()
            .join("/");
        Ok(Filter {
            glob: Some(m),
            glob_dir,
            after,
        })
    }

    fn accepts_file(&self, path: &str) -> bool {
        let unseen = match &self.after {
            Some((p, line)) => path > p.as_str() || (path == p && *line != u64::MAX),
            None => true,
        };
        unseen && self.glob.as_ref().is_none_or(|g| g.is_match(path))
    }

    /// Lines of `path` an earlier page already covered.
    fn skip_lines(&self, path: &str) -> u64 {
        match &self.after {
            Some((p, line)) if p == path => *line,
            _ => 0,
        }
    }

    /// Whether some file under `dir` could match the glob and come after a
    /// resumed page's position (files are visited in path order, and every
    /// path below `dir` starts with `dir/`).
    fn may_contain(&self, dir: &str) -> bool {
        if let Some((p, _)) = &self.after {
            let prefix = format!("{dir}/");
            if prefix.as_str() < p.as_str() && !p.starts_with(&prefix) {
                return false;
            }
        }
        let lp = self.glob_dir.as_str();
        lp.is_empty()
            || dir == lp
            || dir.strip_prefix(lp).is_some_and(|r| r.starts_with('/'))
            || lp.strip_prefix(dir).is_some_and(|r| r.starts_with('/'))
    }
}

fn join(dir: &str, name: &str) -> String {
    if dir.is_empty() {
        name.to_string()
    } else {
        format!("{dir}/{name}")
    }
}

/// Counts the walk keeps while the pipeline consumes it.
#[derive(Default)]
struct WalkStats {
    considered: AtomicU64,
    dirs_pruned: AtomicU64,
}

/// A child of an opened directory that the walk will visit: a file, or a
/// directory whose filter did not rule it out, with its node already read.
enum Child {
    Dir {
        path: String,
        node: TreeNode,
    },
    /// A directory being opened ahead of the walk.
    Opening {
        path: String,
        task: Opening,
    },
    File(FileRef),
}

/// An opened directory: its children in path order, and how many of its
/// subdirectories their filters ruled out.
struct Opened {
    children: Vec<Child>,
    pruned: u64,
}

/// A directory opened on its own task, which is aborted if the walk is
/// dropped before it gets there (a search that has its matches).
struct Opening(tokio::task::JoinHandle<Result<Opened>>);

impl Drop for Opening {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// Directories opened ahead of the walk at once. Each is two batched
/// metadata reads and one batched tree read, so four leave half of a
/// 16-connection pool to other requests.
const OPEN_AHEAD: usize = 4;

/// What opening a directory needs, shared by the walk and the tasks that
/// open directories ahead of it.
struct Opener {
    repo: Repo,
    filter: Arc<Filter>,
    compiled: Arc<Compiled>,
    indexed: bool,
    counters: Arc<Counters>,
    ahead: Arc<tokio::sync::Semaphore>,
}

impl Opener {
    /// Whether a directory filter admits the query.
    fn admits(&self, row: &[u8]) -> Result<bool> {
        let present = self.compiled.probe.tree(row)?;
        Ok(self.compiled.required.eval(&|t| present.has(t)))
    }

    /// The children of the directory `dir` (node `node`) that the walk will
    /// visit, in path order. Reads what they need in batches: one
    /// `get_derived_many` for the filters of the subdirectories and one for
    /// the rows of the files, then one `read_trees` for the subdirectories
    /// that survive.
    async fn open(&self, dir: &str, node: TreeNode) -> Result<Opened> {
        let mut entries: Vec<(String, TreeEntry)> = node.entries.into_iter().collect();
        entries.sort_by_cached_key(|(name, e)| match e {
            TreeEntry::Dir { .. } => format!("{name}/"),
            TreeEntry::File { .. } => name.clone(),
        });
        let mut dirs: Vec<(usize, String, Hash)> = Vec::new();
        let mut files: Vec<(usize, FileRef)> = Vec::new();
        for (i, (name, entry)) in entries.into_iter().enumerate() {
            let path = join(dir, &name);
            match entry {
                TreeEntry::Dir { tree } => {
                    if self.filter.may_contain(&path) {
                        dirs.push((i, path, tree));
                    }
                }
                TreeEntry::File {
                    content,
                    kind,
                    size,
                } => {
                    if self.filter.accepts_file(&path) {
                        let skip_lines = self.filter.skip_lines(&path);
                        files.push((
                            i,
                            FileRef {
                                path,
                                content,
                                kind,
                                size,
                                row: None,
                                skip_lines,
                            },
                        ));
                    }
                }
            }
        }
        let mut pruned = 0;
        if self.indexed {
            // Files that `check_file` skips without a row need none.
            let wanted: Vec<usize> = (0..files.len())
                .filter(|&k| needs_row(&files[k].1))
                .collect();
            let blob_keys: Vec<Hash> = wanted.iter().map(|&k| files[k].1.content).collect();
            let tree_keys: Vec<Hash> = dirs.iter().map(|d| d.2).collect();
            let meta = self.repo.meta();
            let (tree_rows, blob_rows) = futures_util::try_join!(
                many(meta.as_ref(), TREE_KIND, &tree_keys),
                many(meta.as_ref(), BLOB_KIND, &blob_keys),
            )?;
            for (k, row) in wanted.into_iter().zip(blob_rows) {
                files[k].1.row = row;
            }
            let mut kept = Vec::with_capacity(dirs.len());
            for (d, row) in dirs.into_iter().zip(tree_rows) {
                let row = match row {
                    Some(row) => row,
                    None => self.repo.tree_row(d.2, &self.counters).await?,
                };
                if self.admits(&row)? {
                    kept.push(d);
                } else {
                    pruned += 1;
                }
            }
            dirs = kept;
        }
        let hashes: Vec<Hash> = dirs.iter().map(|d| d.2).collect();
        let nodes = self.repo.read_trees(&hashes).await?;
        let mut out: Vec<(usize, Child)> = Vec::with_capacity(dirs.len() + files.len());
        out.extend(
            dirs.into_iter()
                .zip(nodes)
                .map(|((i, path, _), node)| (i, Child::Dir { path, node })),
        );
        out.extend(files.into_iter().map(|(i, f)| (i, Child::File(f))));
        out.sort_unstable_by_key(|c| c.0);
        Ok(Opened {
            children: out.into_iter().map(|(_, c)| c).collect(),
            pruned,
        })
    }

    /// Start opening each subdirectory among `children` on its own task, at
    /// most [`OPEN_AHEAD`] at a time across the walk, in path order.
    fn open_ahead(self: &Arc<Self>, children: Vec<Child>) -> Vec<Child> {
        children
            .into_iter()
            .map(|child| {
                let Child::Dir { path, node } = child else {
                    return child;
                };
                let opener = self.clone();
                let dir = path.clone();
                // The semaphore is fair, so directories start in walk order.
                let task = tokio::spawn(async move {
                    let _permit = opener
                        .ahead
                        .clone()
                        .acquire_owned()
                        .await
                        .map_err(|e| Error::Other(format!("search walk: {e}")))?;
                    opener.open(&dir, node).await
                });
                Child::Opening {
                    path,
                    task: Opening(task),
                }
            })
            .collect()
    }
}

/// Depth-first walk that yields files lazily, in path order, so a search that
/// has enough matches stops reading directories too. Directories are visited
/// in the order of their full paths (a directory `a` sorts as `a/`, so it
/// follows a sibling file `a.txt`); this is the same order as sorting the
/// file paths as strings. Uses an explicit stack.
///
/// Opening a directory reads its children's index rows and nodes in batches
/// ([`Opener::open`]), and when the walk enters a directory it starts opening
/// that directory's subdirectories ahead of it, a few at a time. Probed one at
/// a time, these reads were sequential round trips per directory and per file,
/// which set the pace of a search over a large tree. What the walk yields, and
/// in what order, does not depend on how far ahead it has read.
///
/// Owns its inputs (cheap clones and `Arc`s) because a stream closure that
/// takes a borrowing state makes the search future fail the `Send` check that
/// axum handlers need.
struct Walk {
    opener: Arc<Opener>,
    stats: Arc<WalkStats>,
    start: Option<(String, Hash)>,
    first_file: Option<FileRef>,
    stack: Vec<std::vec::IntoIter<Child>>,
}

impl Walk {
    /// Make `opened` the directory the walk is in.
    fn descend(&mut self, opened: Opened) {
        let Opened { children, pruned } = opened;
        self.stats.dirs_pruned.fetch_add(pruned, Ordering::Relaxed);
        let children = self.opener.open_ahead(children);
        self.stack.push(children.into_iter());
    }

    async fn next(&mut self) -> Result<Option<FileRef>> {
        if let Some(f) = self.first_file.take() {
            return Ok(Some(f));
        }
        if let Some((path, tree)) = self.start.take() {
            let o = &self.opener;
            let admitted = !o.indexed || {
                let row = o.repo.tree_row(tree, &o.counters).await?;
                o.admits(&row)?
            };
            if admitted {
                let node = o.repo.read_tree(tree).await?;
                let opened = o.open(&path, node).await?;
                self.descend(opened);
            } else {
                self.stats.dirs_pruned.fetch_add(1, Ordering::Relaxed);
            }
        }
        loop {
            let Some(children) = self.stack.last_mut() else {
                return Ok(None);
            };
            match children.next() {
                None => {
                    self.stack.pop();
                }
                Some(Child::Dir { path, node }) => {
                    let opened = self.opener.open(&path, node).await?;
                    self.descend(opened);
                }
                Some(Child::Opening { path, mut task }) => {
                    let opened = (&mut task.0).await.map_err(|e| {
                        Error::Other(format!("search walk failed in {path}: {e}"))
                    })??;
                    self.descend(opened);
                }
                Some(Child::File(f)) => {
                    self.stats.considered.fetch_add(1, Ordering::Relaxed);
                    return Ok(Some(f));
                }
            }
        }
    }
}

/// Whether `check_file` consults the file's index row.
fn needs_row(f: &FileRef) -> bool {
    f.kind != FileKind::Symlink && f.size <= MAX_INDEXED_BYTES
}

/// `get_derived_many` that skips the round trip for no keys and checks the
/// answer's length.
async fn many(
    meta: &dyn leep_core::MetaStore,
    kind: &str,
    keys: &[Hash],
) -> Result<Vec<Option<Bytes>>> {
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let rows = meta.get_derived_many(kind, keys).await?;
    if rows.len() != keys.len() {
        return Err(Error::Storage(format!(
            "get_derived_many answered {} of {} {kind} rows",
            rows.len(),
            keys.len()
        )));
    }
    Ok(rows)
}

impl Repo {
    /// Search the tree of `id`. Matches come ordered by path, then line, so
    /// the same query on the same revision always returns the same answer,
    /// with or without the index.
    pub async fn search(&self, id: ChangesetId, q: &SearchQuery) -> Result<SearchResponse> {
        Ok(self.search_from(id, q, None).await?.0)
    }

    /// Search `rev` (a ref name or full id), one page at a time: `after` is
    /// the `next` cursor of the previous page, which pins the revision the
    /// first page resolved, so later pages search the same tree even if the
    /// ref moved. `Invalid` if `after` is malformed or belongs to another
    /// query.
    pub async fn search_page(
        &self,
        rev: &str,
        q: &SearchQuery,
        after: Option<&str>,
    ) -> Result<SearchResponse> {
        let (id, pos) = self.search_resume(rev, q, after).await?;
        self.search_page_at(rev, id, q, pos.as_ref()).await
    }

    /// The query's fingerprint for search cursors.
    fn search_fingerprint(rev: &str, q: &SearchQuery) -> Fingerprint {
        Fingerprint::of(
            CursorKind::Search,
            &(
                rev,
                q.pattern.as_str(),
                q.regex,
                q.case_insensitive,
                q.path.trim_matches('/'),
                q.glob.as_deref(),
            ),
        )
    }

    /// The revision a search page reads (`rev` resolved, or the one an
    /// earlier page's cursor pins) and where in it the page starts.
    pub(crate) async fn search_resume(
        &self,
        rev: &str,
        q: &SearchQuery,
        after: Option<&str>,
    ) -> Result<(ChangesetId, Option<SearchPos>)> {
        let fp = Self::search_fingerprint(rev, q);
        match after {
            None => Ok((self.resolve(rev).await?, None)),
            Some(text) => {
                let pos: SearchPos = cursor::decode(text, CursorKind::Search, fp)?;
                match self.root_tree(pos.id).await {
                    Ok(_) => {}
                    Err(Error::NotFound(_)) => {
                        return Err(Error::Invalid(
                            "bad cursor: it names an unknown changeset".into(),
                        ))
                    }
                    Err(e) => return Err(e),
                }
                Ok((pos.id, Some(pos)))
            }
        }
    }

    /// One page of the search of the tree of `id`, whose cursor pins `id`.
    pub(crate) async fn search_page_at(
        &self,
        rev: &str,
        id: ChangesetId,
        q: &SearchQuery,
        pos: Option<&SearchPos>,
    ) -> Result<SearchResponse> {
        let fp = Self::search_fingerprint(rev, q);
        let (mut resp, resume) = self.search_from(id, q, pos).await?;
        if let Some((path, line)) = resume {
            resp.next = cursor::encode(CursorKind::Search, fp, &SearchPos { id, path, line });
        }
        Ok(resp)
    }

    /// Whether a search with `q` consults the index at all.
    pub(crate) fn search_uses_index(q: &SearchQuery) -> Result<bool> {
        let compiled = Compiled::new(&q.pattern, q.regex, q.case_insensitive)?;
        Ok(q.use_index && !compiled.required.is_true())
    }

    /// [`Repo::search`] after `after`, also returning where the next page
    /// starts when this one stopped early: after the last match it kept, or
    /// after the last file it finished when the scan budget ran out. `None`
    /// when the search ended, or when it could not get past a single file.
    async fn search_from(
        &self,
        id: ChangesetId,
        q: &SearchQuery,
        after: Option<&SearchPos>,
    ) -> Result<(SearchResponse, Option<(String, u64)>)> {
        let started = std::time::Instant::now();
        let compiled = Arc::new(Compiled::new(&q.pattern, q.regex, q.case_insensitive)?);
        let after = after.map(|p| (p.path.clone(), p.line));
        let filter = Arc::new(Filter::new(q.glob.as_deref(), after)?);
        let limit = q.limit.clamp(1, MAX_LIMIT);
        let indexed = q.use_index && !compiled.required.is_true();
        // A bounded search (ADR 0058) computes missing rows in memory.
        let scan = self.bounded.as_ref().and_then(|b| b.search.clone());
        let counters = Arc::new(match &scan {
            Some(scan) => Counters::scanning(scan.clone()),
            None => Counters::default(),
        });
        let walk_stats = Arc::new(WalkStats::default());
        let mut stats = SearchStats {
            indexed,
            ..SearchStats::default()
        };

        let root = self.root_tree(id).await?;
        let start = q.path.trim_matches('/');
        let mut walk = Walk {
            opener: Arc::new(Opener {
                repo: self.clone(),
                filter: filter.clone(),
                compiled: compiled.clone(),
                indexed,
                counters: counters.clone(),
                ahead: Arc::new(tokio::sync::Semaphore::new(OPEN_AHEAD)),
            }),
            stats: walk_stats.clone(),
            start: None,
            first_file: None,
            stack: Vec::new(),
        };
        match self.entry_at(root, start).await? {
            None => {}
            Some(TreeEntry::Dir { tree }) => walk.start = Some((start.to_string(), tree)),
            Some(TreeEntry::File {
                content,
                kind,
                size,
            }) if filter.accepts_file(start) => {
                walk_stats.considered.fetch_add(1, Ordering::Relaxed);
                walk.first_file = Some(FileRef {
                    path: start.to_string(),
                    content,
                    kind,
                    size,
                    row: None,
                    skip_lines: filter.skip_lines(start),
                });
            }
            Some(TreeEntry::File { .. }) => {}
        }
        // After an error the walk is emptied, so the stream ends.
        let files = stream::unfold(walk, |mut w| async move {
            match w.next().await {
                Ok(Some(f)) => Some((Ok(f), w)),
                Ok(None) => None,
                Err(e) => {
                    (w.start, w.first_file, w.stack) = (None, None, Vec::new());
                    Some((Err(e), w))
                }
            }
        });
        // Owned items: a closure over `&FileRef` makes the future fail the
        // `Send` check that axum handlers need.
        let outcomes = files
            .map(|f| {
                let (compiled, counters) = (&compiled, &counters);
                async move {
                    let f = f?;
                    let path = f.path.clone();
                    let outcome = self
                        .check_file(f, compiled, indexed, limit + 1, counters)
                        .await?;
                    Ok::<_, Error>((path, outcome))
                }
            })
            .buffered(CONCURRENCY);
        let mut outcomes = std::pin::pin!(outcomes);

        let mut matches: Vec<SearchMatch> = Vec::new();
        let mut truncated = false;
        let mut resume = None;
        // The last file finished, for a page the scan budget cut short.
        let mut done: Option<String> = None;
        while let Some(outcome) = outcomes.next().await {
            let (path, outcome) = outcome?;
            if stats.bytes_scanned + outcome.fetched() > q.max_scan_bytes {
                truncated = true;
                resume = done.map(|p| (p, u64::MAX));
                break;
            }
            stats.bytes_scanned += outcome.fetched();
            match outcome {
                Outcome::Pruned => stats.files_pruned += 1,
                Outcome::Skipped { .. } => stats.files_skipped += 1,
                Outcome::Scanned { matches: m, .. } => {
                    stats.files_scanned += 1;
                    matches.extend(m);
                    if matches.len() > limit {
                        matches.truncate(limit);
                        truncated = true;
                        let last = matches.last().expect("limit is at least 1");
                        resume = Some((last.path.as_str().to_string(), last.line_number));
                        break;
                    }
                }
            }
            done = Some(path);
        }
        stats.files_considered = walk_stats.considered.load(Ordering::Relaxed);
        stats.dirs_pruned = walk_stats.dirs_pruned.load(Ordering::Relaxed);
        stats.blob_rows_derived = counters.blob_rows();
        stats.tree_rows_derived = counters.tree_rows();
        if let Some(scan) = &scan {
            stats.files_unindexed = scan.files.load(Ordering::Relaxed);
        }
        metrics::counter!("leep_search_total", "indexed" => indexed.to_string()).increment(1);
        metrics::histogram!("leep_search_duration_seconds").record(started.elapsed().as_secs_f64());
        Ok((
            SearchResponse {
                matches,
                truncated,
                stats,
                next: None,
                answered_at: None,
                behind: None,
            },
            resume,
        ))
    }

    /// Compute every missing index row for the tree of `id`, so later
    /// searches pay nothing for it. Idempotent.
    pub async fn derive_search(&self, id: ChangesetId) -> Result<SearchIndexReport> {
        let root = self.root_tree(id).await?;
        let counters = Counters::default();
        self.tree_row(root, &counters).await?;
        Ok(SearchIndexReport {
            blob_rows: counters.blob_rows(),
            tree_rows: counters.tree_rows(),
        })
    }

    /// Decide one file: skip it, rule it out by its trigram set, or fetch and
    /// match it. At most `max` matches are kept, since no more can be used.
    async fn check_file(
        &self,
        mut f: FileRef,
        compiled: &Arc<Compiled>,
        indexed: bool,
        max: usize,
        counters: &Counters,
    ) -> Result<Outcome> {
        if !needs_row(&f) {
            return Ok(Outcome::Skipped { fetched: 0 });
        }
        if indexed {
            let row = match f.row.take() {
                Some(row) => row,
                None => self.blob_row_bytes(f.content, f.size, counters).await?,
            };
            match compiled.probe.blob(&row)? {
                None => return Ok(Outcome::Skipped { fetched: 0 }),
                Some(present) => {
                    if !compiled.required.eval(&|t| present.has(t)) {
                        return Ok(Outcome::Pruned);
                    }
                }
            }
        }
        let data =
            self.blobs().get(&f.content).await?.ok_or_else(|| {
                Error::Storage(format!("blob {} missing for {}", f.content, f.path))
            })?;
        let fetched = data.len() as u64;
        let path = RepoPath::new(f.path.as_str())?;
        let skip_lines = f.skip_lines;
        // Matching is the CPU part of a search; off the async workers, files
        // in flight are matched on as many threads as the runtime allows.
        let compiled = compiled.clone();
        let scanned = tokio::task::spawn_blocking(move || {
            if !is_text(&data) {
                return None;
            }
            let text = std::str::from_utf8(&data).expect("checked by is_text");
            let mut matches = Vec::new();
            compiled.scan_after(&path, text, skip_lines, max, &mut matches);
            Some(matches)
        })
        .await
        .map_err(|e| Error::Other(format!("search scan failed: {e}")))?;
        let Some(matches) = scanned else {
            return Ok(Outcome::Skipped { fetched });
        };
        Ok(Outcome::Scanned { fetched, matches })
    }
}
