//! Server-side code search over a revision (ADR 0014).
//!
//! Two derived kinds make it fast: `blob-trigrams-v1` (the trigram set of one
//! text blob) and `tree-trigrams-v1` (a bloom filter over every trigram below
//! a directory). A query names trigrams every matching file must contain
//! ([`plan`]); directories and files that lack them are skipped unread, and
//! only the survivors are fetched and matched line by line. Both kinds are
//! keyed by content hash, so a revision shares rows with its parent for every
//! subtree and blob it did not touch.

mod index;
#[doc(hidden)]
pub mod plan;
pub mod trigram;

#[cfg(test)]
mod tests;

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use futures_util::{stream, StreamExt};
use globset::{GlobBuilder, GlobMatcher};
use leep_core::api::{SearchMatch, SearchResponse, SearchStats};
use leep_core::{ChangesetId, Error, FileKind, Hash, RepoPath, Result, TreeEntry};

pub use index::{BLOB_KIND, TREE_KIND};
pub use trigram::MAX_INDEXED_BYTES;

use index::{Counters, CONCURRENCY};
use plan::Compiled;
use trigram::is_text;

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

/// Path and glob restrictions.
struct Filter {
    glob: Option<GlobMatcher>,
    /// Leading directories every match of the glob lies under.
    glob_dir: String,
}

impl Filter {
    fn new(glob: Option<&str>) -> Result<Self> {
        let Some(g) = glob else {
            return Ok(Filter {
                glob: None,
                glob_dir: String::new(),
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
        })
    }

    fn accepts_file(&self, path: &str) -> bool {
        self.glob.as_ref().is_none_or(|g| g.is_match(path))
    }

    /// Whether some file under `dir` could match the glob.
    fn may_contain(&self, dir: &str) -> bool {
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

type DirEntries = std::vec::IntoIter<(String, TreeEntry)>;

/// Depth-first walk that yields files lazily, in path order, so a search that
/// has enough matches stops reading directories too. Directories are visited
/// in the order of their full paths (a directory `a` sorts as `a/`, so it
/// follows a sibling file `a.txt`); this is the same order as sorting the
/// file paths as strings. Uses an explicit stack.
///
/// Owns its inputs (cheap clones and `Arc`s) because a stream closure that
/// takes a borrowing state makes the search future fail the `Send` check that
/// axum handlers need.
struct Walk {
    repo: Repo,
    filter: Arc<Filter>,
    compiled: Arc<Compiled>,
    indexed: bool,
    counters: Arc<Counters>,
    stats: Arc<WalkStats>,
    start: Option<(String, Hash)>,
    first_file: Option<FileRef>,
    stack: Vec<(String, DirEntries)>,
}

impl Walk {
    /// Open a directory, or `None` if its summary rules it out.
    async fn enter(&self, path: String, tree: Hash) -> Result<Option<(String, DirEntries)>> {
        if self.indexed {
            let row = self.repo.tree_row(tree, &self.counters).await?;
            let present = self.compiled.probe.tree(&row)?;
            if !self.compiled.required.eval(&|t| present.has(t)) {
                self.stats.dirs_pruned.fetch_add(1, Ordering::Relaxed);
                return Ok(None);
            }
        }
        let mut entries: Vec<(String, TreeEntry)> = self
            .repo
            .read_tree(tree)
            .await?
            .entries
            .into_iter()
            .collect();
        entries.sort_by_cached_key(|(name, e)| match e {
            TreeEntry::Dir { .. } => format!("{name}/"),
            TreeEntry::File { .. } => name.clone(),
        });
        Ok(Some((path, entries.into_iter())))
    }

    async fn next(&mut self) -> Result<Option<FileRef>> {
        if let Some(f) = self.first_file.take() {
            return Ok(Some(f));
        }
        if let Some((path, tree)) = self.start.take() {
            if let Some(frame) = self.enter(path, tree).await? {
                self.stack.push(frame);
            }
        }
        loop {
            let Some((dir, entries)) = self.stack.last_mut() else {
                return Ok(None);
            };
            let Some((name, entry)) = entries.next() else {
                self.stack.pop();
                continue;
            };
            let path = join(dir, &name);
            match entry {
                TreeEntry::Dir { tree } => {
                    if self.filter.may_contain(&path) {
                        if let Some(frame) = self.enter(path, tree).await? {
                            self.stack.push(frame);
                        }
                    }
                }
                TreeEntry::File {
                    content,
                    kind,
                    size,
                } => {
                    if self.filter.accepts_file(&path) {
                        self.stats.considered.fetch_add(1, Ordering::Relaxed);
                        return Ok(Some(FileRef {
                            path,
                            content,
                            kind,
                            size,
                        }));
                    }
                }
            }
        }
    }
}

impl Repo {
    /// Search the tree of `id`. Matches come ordered by path, then line, so
    /// the same query on the same revision always returns the same answer,
    /// with or without the index.
    pub async fn search(&self, id: ChangesetId, q: &SearchQuery) -> Result<SearchResponse> {
        let started = std::time::Instant::now();
        let compiled = Arc::new(Compiled::new(&q.pattern, q.regex, q.case_insensitive)?);
        let filter = Arc::new(Filter::new(q.glob.as_deref())?);
        let limit = q.limit.clamp(1, MAX_LIMIT);
        let indexed = q.use_index && !compiled.required.is_true();
        let counters = Arc::new(Counters::default());
        let walk_stats = Arc::new(WalkStats::default());
        let mut stats = SearchStats {
            indexed,
            ..SearchStats::default()
        };

        let root = self.root_tree(id).await?;
        let start = q.path.trim_matches('/');
        let mut walk = Walk {
            repo: self.clone(),
            filter: filter.clone(),
            compiled: compiled.clone(),
            indexed,
            counters: counters.clone(),
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
                    self.check_file(f?, compiled, indexed, limit + 1, counters)
                        .await
                }
            })
            .buffered(CONCURRENCY);
        let mut outcomes = std::pin::pin!(outcomes);

        let mut matches = Vec::new();
        let mut truncated = false;
        while let Some(outcome) = outcomes.next().await {
            let outcome = outcome?;
            if stats.bytes_scanned + outcome.fetched() > q.max_scan_bytes {
                truncated = true;
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
                        break;
                    }
                }
            }
        }
        stats.files_considered = walk_stats.considered.load(Ordering::Relaxed);
        stats.dirs_pruned = walk_stats.dirs_pruned.load(Ordering::Relaxed);
        stats.blob_rows_derived = counters.blob_rows();
        stats.tree_rows_derived = counters.tree_rows();
        metrics::counter!("leep_search_total", "indexed" => indexed.to_string()).increment(1);
        metrics::histogram!("leep_search_duration_seconds").record(started.elapsed().as_secs_f64());
        Ok(SearchResponse {
            matches,
            truncated,
            stats,
        })
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
        f: FileRef,
        compiled: &Compiled,
        indexed: bool,
        max: usize,
        counters: &Counters,
    ) -> Result<Outcome> {
        if f.kind == FileKind::Symlink || f.size > MAX_INDEXED_BYTES {
            return Ok(Outcome::Skipped { fetched: 0 });
        }
        if indexed {
            let row = self.blob_row_bytes(f.content, f.size, counters).await?;
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
        if !is_text(&data) {
            return Ok(Outcome::Skipped { fetched });
        }
        let text = std::str::from_utf8(&data).expect("checked by is_text");
        let mut matches = Vec::new();
        compiled.scan(&RepoPath::new(f.path.as_str())?, text, max, &mut matches);
        Ok(Outcome::Scanned { fetched, matches })
    }
}
