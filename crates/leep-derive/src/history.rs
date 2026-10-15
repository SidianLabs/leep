//! Log with optional path and metadata filtering, and provenance summaries.
//!
//! Path queries use the path-history index (`path_history`). Filters on
//! `extra` and `author` are applied to the changesets that walk yields, and
//! without a path they are evaluated by a linear walk over history, so a
//! selective filter on a deep history scans all of it. An index over
//! `agent.*` keys is future work.

use std::collections::{BTreeMap, BinaryHeap, HashMap, HashSet};

use leep_core::api::{AgentStat, ChangesetInfo, HumanStat, ProvenanceResponse};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::{Changeset, ChangesetId, Error, RepoPath, Result, TreeNode};

use crate::Repo;

/// One `extra` predicate: the key must be present, and equal to `value` if
/// given.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ExtraMatch {
    pub key: String,
    pub value: Option<String>,
}

impl ExtraMatch {
    /// Parse `key:value` (exact match) or `key` (present, any value). The
    /// split is at the first `:`.
    pub fn parse(s: &str) -> Result<Self> {
        let (key, value) = match s.split_once(':') {
            Some((k, v)) => (k, Some(v.to_string())),
            None => (s, None),
        };
        if key.is_empty() {
            return Err(Error::Invalid(format!("bad extra filter {s:?}: empty key")));
        }
        Ok(ExtraMatch {
            key: key.to_string(),
            value,
        })
    }
}

/// Metadata predicate for [`Repo::log_filtered`]; all parts are ANDed.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct LogFilter {
    /// Case-insensitive substring of the author.
    pub author: Option<String>,
    pub extra: Vec<ExtraMatch>,
}

impl LogFilter {
    pub fn is_empty(&self) -> bool {
        self.author.is_none() && self.extra.is_empty()
    }

    pub fn matches(&self, cs: &Changeset) -> bool {
        if let Some(a) = &self.author {
            if !cs.author.to_lowercase().contains(&a.to_lowercase()) {
                return false;
            }
        }
        self.extra
            .iter()
            .all(|m| match (cs.extra.get(&m.key), &m.value) {
                (Some(have), Some(want)) => have == want,
                (Some(_), None) => true,
                (None, _) => false,
            })
    }
}

/// Iterative newest-first walk over all parents; histories are too deep to
/// recurse over.
struct Walk {
    seen: HashSet<ChangesetId>,
    heap: BinaryHeap<(u64, ChangesetId)>,
}

impl Walk {
    async fn start(repo: &Repo, id: ChangesetId) -> Result<Self> {
        Ok(Walk {
            seen: HashSet::from([id]),
            heap: BinaryHeap::from([(repo.generation(id).await?, id)]),
        })
    }

    fn is_done(&self) -> bool {
        self.heap.is_empty()
    }

    async fn next(&mut self, repo: &Repo) -> Result<Option<(u64, ChangesetId, Changeset)>> {
        let Some((generation, cid)) = self.heap.pop() else {
            return Ok(None);
        };
        let cs = repo.get_changeset(cid).await?;
        for &p in &cs.parents {
            if self.seen.insert(p) {
                self.heap.push((repo.generation(p).await?, p));
            }
        }
        Ok(Some((generation, cid, cs)))
    }
}

fn norm(path: Option<&str>) -> Option<&str> {
    path.map(|p| p.trim_matches('/')).filter(|p| !p.is_empty())
}

#[derive(Default)]
struct Tally {
    changesets: u64,
    files: HashSet<RepoPath>,
    first: i64,
    last: i64,
}

impl Tally {
    fn add(&mut self, cs: &Changeset, prefix: &str) {
        if self.changesets == 0 {
            (self.first, self.last) = (cs.timestamp, cs.timestamp);
        }
        self.first = self.first.min(cs.timestamp);
        self.last = self.last.max(cs.timestamp);
        self.changesets += 1;
        self.files
            .extend(cs.changes.keys().filter(|p| p.is_within(prefix)).cloned());
    }
}

impl Repo {
    /// Ancestors of `id` (inclusive, all parents), newest first by generation
    /// then id. With `path`, only changesets that changed that file or
    /// directory relative to their first parent.
    pub async fn log(
        &self,
        id: ChangesetId,
        path: Option<&str>,
        limit: usize,
    ) -> Result<Vec<ChangesetInfo>> {
        self.log_filtered(id, path, limit, &LogFilter::default())
            .await
    }

    /// [`Repo::log`] keeping only changesets that satisfy `filter`. `limit`
    /// counts matches, not changesets scanned.
    pub async fn log_filtered(
        &self,
        id: ChangesetId,
        path: Option<&str>,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        match norm(path) {
            Some(path) => self.log_indexed(id, path, limit, filter).await,
            None => self.log_scan(id, None, limit, filter).await,
        }
    }

    /// Reference implementation of [`Repo::log_filtered`]: scans every
    /// ancestor and compares its entry at `path` with its first parent's.
    /// Linear in history, so it only serves as the oracle for the index in
    /// tests and as the baseline in benchmarks.
    #[doc(hidden)]
    pub async fn log_scan(
        &self,
        id: ChangesetId,
        path: Option<&str>,
        limit: usize,
        filter: &LogFilter,
    ) -> Result<Vec<ChangesetInfo>> {
        let path = norm(path);
        let mut out = Vec::new();
        let mut walk = Walk::start(self, id).await?;
        while out.len() < limit {
            let Some((generation, cid, cs)) = walk.next(self).await? else {
                break;
            };
            // Cheap metadata test first; the path test reads trees.
            if !filter.matches(&cs) {
                continue;
            }
            let root_tree = self.root_tree(cid).await?;
            if let Some(path) = path {
                let parent_tree = match cs.parents.first() {
                    Some(p) => self.root_tree(*p).await?,
                    None => TreeNode::empty_hash(),
                };
                if root_tree == parent_tree
                    || self.entry_at(root_tree, path).await?
                        == self.entry_at(parent_tree, path).await?
                {
                    continue;
                }
            }
            out.push(ChangesetInfo {
                id: cid,
                changeset: cs,
                generation,
                root_tree,
                path_at: None,
                renamed: None,
            });
        }
        Ok(out)
    }

    /// Summarize which agents and humans made the changesets reachable from
    /// `id`, scanning at most `limit` of them newest first. With `prefix`,
    /// only changesets whose own changes touch it count.
    pub async fn provenance(
        &self,
        id: ChangesetId,
        prefix: Option<&str>,
        limit: usize,
    ) -> Result<ProvenanceResponse> {
        let prefix = norm(prefix).unwrap_or("");
        let mut agents: HashMap<(String, Option<String>), Tally> = HashMap::new();
        let mut humans: HashMap<String, Tally> = HashMap::new();
        let mut walk = Walk::start(self, id).await?;
        let mut scanned = 0u64;
        while (scanned as usize) < limit {
            let Some((_, _, cs)) = walk.next(self).await? else {
                break;
            };
            scanned += 1;
            if !cs.changes.keys().any(|p| p.is_within(prefix)) {
                continue;
            }
            let model = cs.extra.get(EXTRA_AGENT_MODEL);
            let session = cs.extra.get(EXTRA_AGENT_SESSION);
            if model.is_some() || session.is_some() {
                let key = (
                    model.cloned().unwrap_or_else(|| "unknown".into()),
                    session.cloned(),
                );
                agents.entry(key).or_default().add(&cs, prefix);
            } else {
                humans
                    .entry(cs.author.clone())
                    .or_default()
                    .add(&cs, prefix);
            }
        }
        let by_count = |a: &(u64, String), b: &(u64, String)| b.0.cmp(&a.0).then(a.1.cmp(&b.1));
        let mut agents: Vec<_> = agents
            .into_iter()
            .map(|((model, session), t)| AgentStat {
                model,
                session,
                changesets: t.changesets,
                files_touched: t.files.len() as u64,
                first: t.first,
                last: t.last,
            })
            .collect();
        agents.sort_by(|a, b| {
            by_count(
                &(a.changesets, format!("{}\0{:?}", a.model, a.session)),
                &(b.changesets, format!("{}\0{:?}", b.model, b.session)),
            )
        });
        let mut humans: Vec<_> = humans
            .into_iter()
            .map(|(author, t)| HumanStat {
                author,
                changesets: t.changesets,
                files_touched: t.files.len() as u64,
                first: t.first,
                last: t.last,
            })
            .collect();
        humans.sort_by(|a, b| {
            by_count(
                &(a.changesets, a.author.clone()),
                &(b.changesets, b.author.clone()),
            )
        });
        Ok(ProvenanceResponse {
            agents,
            humans,
            scanned,
            truncated: !walk.is_done(),
        })
    }
}

/// Keep only `agent.*` keys.
pub(crate) fn agent_keys(extra: &BTreeMap<String, String>) -> BTreeMap<String, String> {
    extra
        .iter()
        .filter(|(k, _)| k.starts_with("agent."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}
