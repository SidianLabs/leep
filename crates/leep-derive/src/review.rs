//! Review decisions: which changed paths need a human, and who may approve
//! them (ADR 0011). The semantic rules here are pure; `Repo::review_changeset`
//! feeds them file contents and the owners read from the target ref.
//!
//! Per changed path, in this order:
//! 1. a reject from an owner of the path (from anyone, when nothing owns it)
//!    blocks it, whatever else holds;
//! 2. on a merge, a change equal to the same path in one of `parents[1..]` is
//!    covered, because that side's own history is reviewed separately;
//! 3. an enabled auto rule (`docs_only`, `comment_only`, `whitespace_only`)
//!    that holds for the path approves it;
//! 4. an approval from an owner (from anyone, when nothing owns it) does;
//! 5. otherwise the path is pending.
//!
//! A changeset is approved when no path is pending or rejected.
//!
//! Verdicts on another changeset X can carry to this one (ADR 0085), but only
//! for paths whose change is identical in both: see [`Repo::carried_paths`].

use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use globset::{Glob, GlobBuilder, GlobSet, GlobSetBuilder};
use leep_core::api::FileInfo;
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::review::{Review, Verdict, OWNERS_FILE};
use leep_core::{ChangesetId, Error, FileChange, FileKind, RepoPath, Result};

use crate::comments::strip_comments;
use crate::owners::OwnersResolver;
use crate::Repo;

/// Largest file (per side) the content rules read; bigger files need a human.
const MAX_AUTO_BYTES: u64 = 2 * 1024 * 1024;

/// Most changesets [`Repo::carry_candidates`] names. Each costs a few reads,
/// and a client writes the hints, so the walk is bounded however they nest.
/// The CLI names up to 64 ids (a resolved stack), so the bound matches it.
pub const MAX_CARRY_CANDIDATES: usize = 64;

/// Most changed paths a candidate may have for [`Repo::carried_paths`] to
/// compare it; a bigger one carries nothing, so its cost stays bounded.
pub const MAX_CARRY_PATHS: usize = 1024;

/// A rule that approves a path without a human.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum AutoRule {
    /// Only comments differ, in a language with a known comment syntax.
    CommentOnly,
    /// Only ASCII whitespace differs.
    WhitespaceOnly,
    /// The path matches a docs glob.
    DocsOnly,
}

impl AutoRule {
    pub const ALL: [AutoRule; 3] = [
        AutoRule::CommentOnly,
        AutoRule::WhitespaceOnly,
        AutoRule::DocsOnly,
    ];

    pub fn name(self) -> &'static str {
        match self {
            AutoRule::CommentOnly => "comment_only",
            AutoRule::WhitespaceOnly => "whitespace_only",
            AutoRule::DocsOnly => "docs_only",
        }
    }

    pub fn parse(s: &str) -> Option<AutoRule> {
        AutoRule::ALL.into_iter().find(|r| r.name() == s)
    }
}

/// The enabled auto rules and the docs globs.
#[derive(Clone, Debug, Default)]
pub struct ReviewRules {
    auto: BTreeSet<AutoRule>,
    docs: Option<GlobSet>,
}

impl ReviewRules {
    /// `docs_globs` use `*` within one directory level, `**` across levels
    /// (`**/*.md` matches `README.md` and `a/b.md`; `docs/**` everything under
    /// `docs/`). `docs_only` needs at least one glob, and globs need `docs_only`.
    pub fn new(auto: impl IntoIterator<Item = AutoRule>, docs_globs: &[String]) -> Result<Self> {
        let auto: BTreeSet<_> = auto.into_iter().collect();
        let docs_on = auto.contains(&AutoRule::DocsOnly);
        if docs_on && docs_globs.is_empty() {
            return Err(Error::Invalid(
                "auto rule docs_only needs at least one docs_globs entry".into(),
            ));
        }
        if !docs_on && !docs_globs.is_empty() {
            return Err(Error::Invalid(
                "docs_globs is set but docs_only is not in auto_approve".into(),
            ));
        }
        let docs = if docs_on {
            let mut b = GlobSetBuilder::new();
            for g in docs_globs {
                let glob: Glob = GlobBuilder::new(g)
                    .literal_separator(true)
                    .build()
                    .map_err(|e| Error::Invalid(format!("bad docs glob {g:?}: {e}")))?;
                b.add(glob);
            }
            Some(
                b.build()
                    .map_err(|e| Error::Invalid(format!("bad docs globs: {e}")))?,
            )
        } else {
            None
        };
        Ok(ReviewRules { auto, docs })
    }

    pub fn enabled(&self, rule: AutoRule) -> bool {
        self.auto.contains(&rule)
    }

    pub fn is_docs(&self, path: &str) -> bool {
        self.docs.as_ref().is_some_and(|g| g.is_match(path))
    }
}

/// Equal line by line once trailing spaces, tabs and CRs and trailing blank
/// lines are dropped. Leading and interior whitespace can carry meaning
/// (Python blocks, string literals), so they count as changes.
pub fn whitespace_only(old: &[u8], new: &[u8]) -> bool {
    let (Ok(old), Ok(new)) = (std::str::from_utf8(old), std::str::from_utf8(new)) else {
        return false;
    };
    let lines = |t: &str| -> Vec<String> {
        let mut v: Vec<String> = t
            .split('\n')
            .map(|l| l.trim_end_matches([' ', '\t', '\r']).to_string())
            .collect();
        while v.last().is_some_and(|l| l.is_empty()) {
            v.pop();
        }
        v
    };
    lines(old) == lines(new)
}

/// Whether only comments differ. `path`'s extension picks the language
/// (see `comments`); unknown languages, non-UTF-8 and anything the lexer
/// finds ambiguous are not comment-only.
pub fn comment_only(path: &str, old: &[u8], new: &[u8]) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    let Some((stem, ext)) = name.rsplit_once('.') else {
        return false;
    };
    if stem.is_empty() {
        return false;
    }
    let (Ok(old), Ok(new)) = (std::str::from_utf8(old), std::str::from_utf8(new)) else {
        return false;
    };
    match (strip_comments(ext, old), strip_comments(ext, new)) {
        (Some(a), Some(b)) => a == b,
        _ => false,
    }
}

/// How one changed path stands.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PathRule {
    Auto(AutoRule),
    Owner,
    MergeParent,
    Pending,
    Rejected,
}

impl PathRule {
    /// `auto:comment_only`, `owner`, `merge_parent`, `pending`, `rejected`.
    pub fn label(self) -> String {
        match self {
            PathRule::Auto(r) => format!("auto:{}", r.name()),
            PathRule::Owner => "owner".into(),
            PathRule::MergeParent => "merge_parent".into(),
            PathRule::Pending => "pending".into(),
            PathRule::Rejected => "rejected".into(),
        }
    }

    pub fn satisfied(self) -> bool {
        !matches!(self, PathRule::Pending | PathRule::Rejected)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PathStatus {
    pub path: RepoPath,
    pub rule: PathRule,
    /// Owners from the target ref's head; empty when nothing covers the path.
    pub owners: Vec<String>,
    /// Approvers who count for this path: its owners, or anyone when it has none.
    pub approved_by: Vec<String>,
    pub rejected_by: Vec<String>,
    /// The changeset whose verdicts this path counted besides its own, when
    /// one of them is in `approved_by` or `rejected_by`.
    pub carried_from: Option<ChangesetId>,
}

/// Verdicts on `from` that count for the paths in `paths` of the changeset
/// being judged, because it makes exactly the change `from` made there.
#[derive(Clone, Debug)]
pub struct Carried {
    pub from: ChangesetId,
    pub paths: BTreeSet<RepoPath>,
    /// Reviews of `from`, withdrawals included, by reviewers with no review
    /// of their own of the changeset being judged.
    pub reviews: Vec<Review>,
}

/// Whether `a` is a later word than `b` from the same reviewer: by
/// timestamp, and on a tie the more cautious verdict, so a reject posted in
/// the same second as an approval wins.
pub fn later(a: &Review, b: &Review) -> bool {
    let rank = |v: Verdict| match v {
        Verdict::Approve => 0,
        Verdict::Withdrawn => 1,
        Verdict::Reject => 2,
    };
    (a.timestamp, rank(a.verdict)) > (b.timestamp, rank(b.verdict))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ChangesetReview {
    pub id: ChangesetId,
    /// In path order.
    pub paths: Vec<PathStatus>,
}

impl ChangesetReview {
    pub fn approved(&self) -> bool {
        self.first_blocker().is_none()
    }

    /// The first path (in path order) that is pending or rejected.
    pub fn first_blocker(&self) -> Option<&PathStatus> {
        self.paths.iter().find(|p| !p.rule.satisfied())
    }
}

/// The merge bases of a merge's first parent with each side parent, found
/// once per review: a review asks for them at every changed path, and in a
/// criss-cross each answer is a walk.
#[derive(Default)]
struct SideBases(HashMap<(ChangesetId, ChangesetId), Vec<ChangesetId>>);

impl SideBases {
    async fn get(
        &mut self,
        repo: &Repo,
        ours: ChangesetId,
        side: ChangesetId,
    ) -> Result<&[ChangesetId]> {
        Ok(match self.0.entry((ours, side)) {
            Entry::Occupied(e) => e.into_mut(),
            Entry::Vacant(e) => e.insert(repo.merge_bases(ours, side).await?),
        })
    }
}

impl Repo {
    /// Judge changeset `id` against `reviews` (at most one per reviewer, all
    /// about `id` exactly) and the owners `owners` reads from the target ref.
    pub async fn review_changeset(
        &self,
        id: ChangesetId,
        rules: &ReviewRules,
        reviews: &[Review],
        owners: &mut OwnersResolver<'_>,
    ) -> Result<ChangesetReview> {
        self.review_changeset_carried(id, rules, reviews, &[], owners)
            .await
    }

    /// [`Self::review_changeset`], also counting `carried`'s verdicts on
    /// their paths (found with [`Self::carried_paths`]). Where several carry
    /// to a path, each reviewer's newest review among them decides, so the
    /// order a client named them in never does; a newest withdrawal carries
    /// nothing.
    pub async fn review_changeset_carried(
        &self,
        id: ChangesetId,
        rules: &ReviewRules,
        reviews: &[Review],
        carried: &[Carried],
        owners: &mut OwnersResolver<'_>,
    ) -> Result<ChangesetReview> {
        let cs = self.get_changeset(id).await?;
        let by = |reviews: &'_ [Review], v: Verdict| -> BTreeSet<String> {
            reviews
                .iter()
                .filter(|r| r.verdict == v)
                .map(|r| r.reviewer.clone())
                .collect()
        };
        let (approvals, rejections) = (by(reviews, Verdict::Approve), by(reviews, Verdict::Reject));
        let mut bases = SideBases::default();
        let dropped = self
            .dropped_side_changes(&cs.parents, id, &mut bases)
            .await?;
        let mut judged: Vec<(&RepoPath, Option<&FileChange>)> = cs
            .changes
            .iter()
            .map(|(p, c)| (p, Some(c)))
            .chain(dropped.iter().map(|p| (p, None)))
            .collect();
        judged.sort_by(|a, b| a.0.cmp(b.0));
        let mut paths = Vec::with_capacity(judged.len());
        for (path, change) in judged {
            let owned = owners.owners_of(path).await?;
            let counts = |r: &str| owned.is_empty() || owned.contains(r);
            let mut approved: BTreeSet<String> =
                approvals.iter().filter(|r| counts(r)).cloned().collect();
            let mut rejected: BTreeSet<String> =
                rejections.iter().filter(|r| counts(r)).cloned().collect();
            // Carried verdicts count only where the change is the one they
            // were given on, and never for a dropped side change. Callers
            // leave out reviewers with a review of `id` itself.
            let mut newest: BTreeMap<&str, (&Review, ChangesetId)> = BTreeMap::new();
            for c in carried
                .iter()
                .filter(|c| change.is_some() && c.paths.contains(path))
            {
                for r in &c.reviews {
                    match newest.entry(r.reviewer.as_str()) {
                        BTreeEntry::Vacant(e) => {
                            e.insert((r, c.from));
                        }
                        BTreeEntry::Occupied(mut e) if later(r, e.get().0) => {
                            e.insert((r, c.from));
                        }
                        BTreeEntry::Occupied(_) => {}
                    }
                }
            }
            let mut carried_from = None;
            for (who, (r, from)) in newest {
                if !counts(who) {
                    continue;
                }
                let set = match r.verdict {
                    Verdict::Approve => &mut approved,
                    Verdict::Reject => &mut rejected,
                    Verdict::Withdrawn => continue,
                };
                if set.insert(who.to_string()) {
                    carried_from.get_or_insert(from);
                }
            }
            let (approved_by, rejected_by): (Vec<String>, Vec<String>) = (
                approved.into_iter().collect(),
                rejected.into_iter().collect(),
            );
            let rule = if !rejected_by.is_empty() {
                PathRule::Rejected
            } else if let Some(change) = change {
                if self
                    .covered_by_merge_parent(&cs.parents, path, change, &mut bases)
                    .await?
                {
                    PathRule::MergeParent
                } else if let Some(r) = self
                    .auto_rule(rules, path, cs.parents.first().copied(), change)
                    .await?
                {
                    PathRule::Auto(r)
                } else if !approved_by.is_empty() {
                    PathRule::Owner
                } else {
                    PathRule::Pending
                }
            } else if !approved_by.is_empty() {
                PathRule::Owner
            } else {
                PathRule::Pending
            };
            paths.push(PathStatus {
                path: path.clone(),
                rule,
                owners: owned.into_iter().collect(),
                approved_by,
                rejected_by,
                carried_from,
            });
        }
        Ok(ChangesetReview { id, paths })
    }

    /// Changesets `id` says it was made from, nearest first: what its
    /// `leep.rebased_from` extra names (comma-separated ids), the
    /// server-recorded rebase origin, and theirs in turn, at most
    /// [`MAX_CARRY_CANDIDATES`]. Only hints: a client writes the extra, so
    /// nothing here is trusted, and [`Self::carried_paths`] decides by
    /// content alone. A changeset without the extra has no candidates (the
    /// server's copies always carry it), which keeps the common case free.
    pub async fn carry_candidates(&self, id: ChangesetId) -> Result<Vec<ChangesetId>> {
        let mut out = Vec::new();
        let mut seen = HashSet::from([id]);
        let mut queue = VecDeque::from([id]);
        while let Some(cur) = queue.pop_front() {
            let cs = match self.get_changeset(cur).await {
                Ok(cs) => cs,
                Err(Error::NotFound(_)) => continue,
                Err(e) => return Err(e),
            };
            let Some(hint) = cs.extra.get(EXTRA_REBASED_FROM) else {
                continue;
            };
            let named = hint
                .split(',')
                .take(MAX_CARRY_CANDIDATES)
                .filter_map(|s| s.trim().parse::<ChangesetId>().ok());
            let origin = self.rebased_origin(cur).await?;
            for c in origin.into_iter().chain(named) {
                if out.len() >= MAX_CARRY_CANDIDATES {
                    return Ok(out);
                }
                if seen.insert(c) {
                    out.push(c);
                    queue.push_back(c);
                }
            }
        }
        Ok(out)
    }

    /// The paths where `id` makes exactly the change `from` made (the same
    /// old content and kind, each relative to its own first parent, and the
    /// same new content and kind), when every other path `from` changed is
    /// one an auto rule approves in `from`. Same old and new means the file
    /// is byte for byte what was reviewed and the base did not move under
    /// it. `None` when a path of `from` that needed a reviewer is changed
    /// differently in `id`, or missing from it (so a reviewed path never
    /// carries without the others it was approved with), when `from` is a
    /// merge or changes more than [`MAX_CARRY_PATHS`] paths. An empty set
    /// means every path of `from` is one an auto rule approves there and
    /// none is reproduced: nothing carries a verdict (callers that carry
    /// verdicts skip it), but nothing reviewed was lost either, which is
    /// what closing a review request asks (ADR 0087). Paths only `id`
    /// changes are judged on their own.
    ///
    /// Cheap by construction: a path whose new content differs is refused
    /// before any read unless an auto rule could hold for it, then each
    /// matching path costs two file lookups, and only the paths that differ
    /// read contents, for the content rules.
    pub async fn carried_paths(
        &self,
        from: ChangesetId,
        id: ChangesetId,
        rules: &ReviewRules,
    ) -> Result<Option<BTreeSet<RepoPath>>> {
        let theirs = self.get_changeset(from).await?;
        // A merge's changes are not all its own (`merge_parent`, dropped
        // side changes); its paths are not compared.
        if theirs.parents.len() > 1 || theirs.changes.len() > MAX_CARRY_PATHS {
            return Ok(None);
        }
        let mine = self.get_changeset(id).await?;
        let new_key = |c: &FileChange| match c {
            FileChange::Modify { content, kind, .. } => Some((*content, *kind)),
            FileChange::Delete => None,
        };
        let content_rules =
            rules.enabled(AutoRule::CommentOnly) || rules.enabled(AutoRule::WhitespaceOnly);
        // Could an auto rule approve this path in `from`? Decided without a
        // read where it can be: never for `OWNERS`, always for a docs path.
        let may_be_auto = |p: &RepoPath, c: &FileChange| {
            p.file_name() != OWNERS_FILE
                && ((rules.enabled(AutoRule::DocsOnly) && rules.is_docs(p.as_str()))
                    || (content_rules && matches!(c, FileChange::Modify { .. })))
        };
        let mut same_new = Vec::new();
        let mut differ = Vec::new();
        for (p, c) in &theirs.changes {
            if mine.changes.get(p).map(new_key) == Some(new_key(c)) {
                same_new.push((p, c));
            } else if may_be_auto(p, c) {
                differ.push((p, c));
            } else {
                return Ok(None);
            }
        }
        let (px, py) = (
            theirs.parents.first().copied(),
            mine.parents.first().copied(),
        );
        let old = |parent: Option<ChangesetId>, p: &RepoPath| {
            let p = p.clone();
            async move {
                Ok::<_, Error>(match parent {
                    Some(parent) => self
                        .lookup_file(parent, &p)
                        .await?
                        .map(|f| (f.content, f.kind)),
                    None => None,
                })
            }
        };
        let mut out = BTreeSet::new();
        for (p, c) in same_new {
            if old(px, p).await? == old(py, p).await? {
                out.insert(p.clone());
            } else if may_be_auto(p, c) {
                differ.push((p, c));
            } else {
                return Ok(None);
            }
        }
        for (p, c) in differ {
            if self.auto_rule(rules, p, px, c).await?.is_none() {
                return Ok(None);
            }
        }
        Ok(Some(out))
    }

    /// Whether `id` puts the reviewed change of `from` on its ref, the
    /// content rule a review request for `from` closes by (ADR 0087):
    /// [`Self::carried_paths`] holds and reproduces at least one path, or
    /// every path of `from` is one an auto rule approves there and `id`
    /// changes each of them in a form an auto rule approves in `id`. Like
    /// carrying, it is decided by content alone, so a forged hint closes
    /// nothing whose reviewed change did not land.
    pub async fn lands_reviewed_change(
        &self,
        from: ChangesetId,
        id: ChangesetId,
        rules: &ReviewRules,
    ) -> Result<bool> {
        let paths = match self.carried_paths(from, id, rules).await {
            Ok(Some(paths)) => paths,
            Ok(None) | Err(Error::NotFound(_)) => return Ok(false),
            Err(e) => return Err(e),
        };
        if !paths.is_empty() {
            return Ok(true);
        }
        let theirs = self.get_changeset(from).await?;
        let mine = self.get_changeset(id).await?;
        if theirs.changes.is_empty() || mine.parents.len() > 1 {
            return Ok(false);
        }
        let parent = mine.parents.first().copied();
        for p in theirs.changes.keys() {
            let Some(c) = mine.changes.get(p) else {
                return Ok(false);
            };
            if self.auto_rule(rules, p, parent, c).await?.is_none() {
                return Ok(false);
            }
        }
        Ok(true)
    }

    /// Paths where merge `id` keeps its first parent's version although a
    /// side parent changed the path since their merge base. They are not
    /// among the merge's own changes, which are relative to the first parent,
    /// yet the merge throws the side's change away: with an old first parent
    /// and the target ref's head as the side, it rewinds the ref's files.
    async fn dropped_side_changes(
        &self,
        parents: &[ChangesetId],
        id: ChangesetId,
        side_bases: &mut SideBases,
    ) -> Result<Vec<RepoPath>> {
        let Some((&ours, sides)) = parents.split_first() else {
            return Ok(Vec::new());
        };
        let key = |f: &Option<FileInfo>| f.as_ref().map(|f| (f.content, f.kind));
        let mine = self.root_tree(id).await?;
        let mut dropped = BTreeSet::new();
        for &side in sides {
            // In a criss-cross, a change dropped relative to any of the
            // merge bases counts (ADR 0071).
            let bases = side_bases.get(self, ours, side).await?.to_vec();
            for e in self
                .diff_trees(self.root_tree(side).await?, mine, "")
                .await?
            {
                let kept_ours = key(&self.lookup_file(ours, &e.path).await?) == key(&e.new);
                if !kept_ours || dropped.contains(&e.path) {
                    continue;
                }
                let mut at_bases = Vec::new();
                for &b in &bases {
                    at_bases.push(key(&self.lookup_file(b, &e.path).await?));
                }
                if at_bases.is_empty() {
                    at_bases.push(None);
                }
                if at_bases.iter().any(|at| *at != key(&e.old)) {
                    dropped.insert(e.path);
                }
            }
        }
        Ok(dropped.into_iter().collect())
    }

    /// On a merge, `change` takes the path as it is in a side parent, and
    /// taking it is what a clean three-way merge would do: the first parent
    /// left the path as it was at the merge base. Otherwise the merge
    /// discards a change already on the first parent (for example puts back
    /// an old version from a side that is in the ref's history), and that is
    /// a change nobody has reviewed.
    async fn covered_by_merge_parent(
        &self,
        parents: &[ChangesetId],
        path: &RepoPath,
        change: &FileChange,
        side_bases: &mut SideBases,
    ) -> Result<bool> {
        let Some((&ours, sides)) = parents.split_first() else {
            return Ok(false);
        };
        let key = |f: Option<FileInfo>| f.map(|f| (f.content, f.kind));
        let mut ours_now = None;
        for side in sides {
            let there = self.lookup_file(*side, path).await?;
            let same = match (change, &there) {
                (FileChange::Delete, None) => true,
                (FileChange::Modify { content, kind, .. }, Some(f)) => {
                    f.content == *content && f.kind == *kind
                }
                _ => false,
            };
            if !same {
                continue;
            }
            let ours_now = match ours_now {
                Some(v) => v,
                None => *ours_now.insert(key(self.lookup_file(ours, path).await?)),
            };
            // Covered only if the first parent left the path as every merge
            // base has it: in a criss-cross one base can hide a change
            // another shows (ADR 0071).
            let bases = side_bases.get(self, ours, *side).await?.to_vec();
            let mut covered = true;
            if bases.is_empty() {
                covered = ours_now.is_none();
            }
            for b in bases {
                if key(self.lookup_file(b, path).await?) != ours_now {
                    covered = false;
                    break;
                }
            }
            if covered {
                return Ok(true);
            }
        }
        Ok(false)
    }

    /// The first enabled auto rule that holds for `change` to `path`.
    async fn auto_rule(
        &self,
        rules: &ReviewRules,
        path: &RepoPath,
        parent: Option<ChangesetId>,
        change: &FileChange,
    ) -> Result<Option<AutoRule>> {
        // Who owns a directory is never a docs or formatting question, even
        // when a docs glob such as `docs/**` matches the file.
        if path.file_name() == OWNERS_FILE {
            return Ok(None);
        }
        if rules.enabled(AutoRule::DocsOnly) && rules.is_docs(path.as_str()) {
            return Ok(Some(AutoRule::DocsOnly));
        }
        let content_rules =
            rules.enabled(AutoRule::CommentOnly) || rules.enabled(AutoRule::WhitespaceOnly);
        let (
            FileChange::Modify {
                content,
                kind,
                size,
            },
            Some(parent),
            true,
        ) = (change, parent, content_rules)
        else {
            return Ok(None);
        };
        if *kind == FileKind::Symlink || *size > MAX_AUTO_BYTES {
            return Ok(None);
        }
        let Some(old) = self.lookup_file(parent, path).await? else {
            return Ok(None);
        };
        if old.kind != *kind || old.size > MAX_AUTO_BYTES {
            return Ok(None);
        }
        let (Some(old), Some(new)) = (
            self.read_content(&old.content).await?,
            self.read_content(content).await?,
        ) else {
            return Ok(None);
        };
        // The narrower rule first, so status names the most specific reason.
        if rules.enabled(AutoRule::WhitespaceOnly) && whitespace_only(&old, &new) {
            return Ok(Some(AutoRule::WhitespaceOnly));
        }
        if rules.enabled(AutoRule::CommentOnly) && comment_only(path.as_str(), &old, &new) {
            return Ok(Some(AutoRule::CommentOnly));
        }
        Ok(None)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn whitespace_only_ignores_trailing_whitespace_and_line_endings() {
        assert!(whitespace_only(b"a b  \n", b"a b"));
        assert!(whitespace_only(b"a\r\nb\r\n", b"a\nb\n"));
        assert!(whitespace_only(b"a\n\n\n", b"a"));
        assert!(whitespace_only(b"", b" \n "));
        assert!(!whitespace_only(b"a b", b"a c"));
        assert!(!whitespace_only(b"ab", b"abc"));
        // Interior and leading whitespace can carry meaning.
        assert!(!whitespace_only(b"a b\n", b"ab\n"));
        assert!(!whitespace_only(b"s = \"a b\"\n", b"s = \"a  b\"\n"));
        assert!(!whitespace_only(
            b"if x:\n    a()\n    b()\n",
            b"if x:\n    a()\nb()\n"
        ));
        assert!(!whitespace_only(b"a\n\nb", b"a\nb"));
        assert!(!whitespace_only("a\u{a0}".as_bytes(), b"a"));
    }

    #[test]
    fn whitespace_only_needs_utf8_on_both_sides() {
        assert!(!whitespace_only(&[0xff, b' '], &[0xff]));
        assert!(!whitespace_only(b"a", &[b'a', 0xc3]));
    }

    #[test]
    fn comment_only_goes_by_extension() {
        assert!(comment_only("src/a.rs", b"fn a(){} // x\n", b"fn a(){}\n"));
        assert!(comment_only("a.PY", b"x=1 # x\n", b"x=1\n"));
        assert!(!comment_only("a.txt", b"x // y\n", b"x\n"));
        assert!(!comment_only("Makefile", b"a # b\n", b"a\n"));
        assert!(!comment_only("dir.rs/Makefile", b"a // b\n", b"a\n"));
        assert!(!comment_only(".rs", b"a // b\n", b"a\n"));
        assert!(!comment_only("a.rs", &[0xff], &[0xff]));
        assert!(!comment_only("a.rs", b"fn a(){} // x\n", b"fn b(){}\n"));
    }

    #[test]
    fn rules_validate_their_inputs() {
        assert!(ReviewRules::new([AutoRule::DocsOnly], &[]).is_err());
        assert!(ReviewRules::new([AutoRule::CommentOnly], &["docs/**".into()]).is_err());
        assert!(ReviewRules::new([AutoRule::DocsOnly], &["[".into()]).is_err());
        let r = ReviewRules::new([AutoRule::DocsOnly], &["docs/**".into()]).unwrap();
        assert!(r.enabled(AutoRule::DocsOnly) && !r.enabled(AutoRule::CommentOnly));
        assert!(ReviewRules::default().auto.is_empty());
    }

    #[test]
    fn docs_globs_respect_directories() {
        let r = ReviewRules::new(
            [AutoRule::DocsOnly],
            &["**/*.md".into(), "docs/**".into(), "*.txt".into()],
        )
        .unwrap();
        assert!(r.is_docs("README.md"));
        assert!(r.is_docs("a/b/c.md"));
        assert!(r.is_docs("docs/x/y.png"));
        assert!(r.is_docs("notes.txt"));
        assert!(!r.is_docs("src/notes.txt"));
        assert!(!r.is_docs("src/lib.rs"));
        assert!(!r.is_docs("mydocs/x"));
    }

    #[test]
    fn rule_names_round_trip() {
        for r in AutoRule::ALL {
            assert_eq!(AutoRule::parse(r.name()), Some(r));
        }
        assert_eq!(AutoRule::parse("nope"), None);
        assert_eq!(
            PathRule::Auto(AutoRule::CommentOnly).label(),
            "auto:comment_only"
        );
        assert!(PathRule::Owner.satisfied() && PathRule::MergeParent.satisfied());
        assert!(!PathRule::Pending.satisfied() && !PathRule::Rejected.satisfied());
    }
}
