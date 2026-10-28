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

use std::collections::BTreeSet;

use globset::{Glob, GlobBuilder, GlobSet, GlobSetBuilder};
use leep_core::api::FileInfo;
use leep_core::review::{Review, Verdict, OWNERS_FILE};
use leep_core::{ChangesetId, Error, FileChange, FileKind, RepoPath, Result};

use crate::comments::strip_comments;
use crate::owners::OwnersResolver;
use crate::Repo;

/// Largest file (per side) the content rules read; bigger files need a human.
const MAX_AUTO_BYTES: u64 = 2 * 1024 * 1024;

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
        let cs = self.get_changeset(id).await?;
        let by = |v: Verdict| -> BTreeSet<&str> {
            reviews
                .iter()
                .filter(|r| r.verdict == v)
                .map(|r| r.reviewer.as_str())
                .collect()
        };
        let (approvals, rejections) = (by(Verdict::Approve), by(Verdict::Reject));
        let dropped = self.dropped_side_changes(&cs.parents, id).await?;
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
            let relevant = |set: &BTreeSet<&str>| -> Vec<String> {
                set.iter()
                    .filter(|r| owned.is_empty() || owned.contains(**r))
                    .map(|r| r.to_string())
                    .collect()
            };
            let (approved_by, rejected_by) = (relevant(&approvals), relevant(&rejections));
            let rule = if !rejected_by.is_empty() {
                PathRule::Rejected
            } else if let Some(change) = change {
                if self
                    .covered_by_merge_parent(&cs.parents, path, change)
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
            });
        }
        Ok(ChangesetReview { id, paths })
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
    ) -> Result<Vec<RepoPath>> {
        let Some((&ours, sides)) = parents.split_first() else {
            return Ok(Vec::new());
        };
        let key = |f: &Option<FileInfo>| f.as_ref().map(|f| (f.content, f.kind));
        let mine = self.root_tree(id).await?;
        let mut dropped = BTreeSet::new();
        for &side in sides {
            let base = self.merge_base(ours, side).await?;
            for e in self
                .diff_trees(self.root_tree(side).await?, mine, "")
                .await?
            {
                let kept_ours = key(&self.lookup_file(ours, &e.path).await?) == key(&e.new);
                if !kept_ours || dropped.contains(&e.path) {
                    continue;
                }
                let at_base = match base {
                    Some(b) => key(&self.lookup_file(b, &e.path).await?),
                    None => None,
                };
                if at_base != key(&e.old) {
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
            let at_base = match self.merge_base(ours, *side).await? {
                Some(b) => key(self.lookup_file(b, path).await?),
                None => None,
            };
            if at_base == ours_now {
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
            self.blobs().get(&old.content).await?,
            self.blobs().get(content).await?,
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
