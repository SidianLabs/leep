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
//! for paths whose change is identical in both, or what the server's own
//! rebase of X's change would make (ADR 0089): see [`Repo::carried_paths`].
//! Paths declared identical-only carry only identical changes (ADR 0092):
//! see [`CarryScope`].

use std::collections::btree_map::Entry as BTreeEntry;
use std::collections::hash_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet, VecDeque};

use globset::{Glob, GlobBuilder, GlobSet, GlobSetBuilder};
use leep_core::api::FileInfo;
use leep_core::changeset::EXTRA_REBASED_FROM;
use leep_core::review::{Review, Verdict, OWNERS_FILE};
use leep_core::{ChangesetId, Error, FileChange, FileKind, RepoPath, Result};

use crate::carry::{self, CarryRule};
use crate::comments::strip_comments;
use crate::owners::{CarryDirective, OwnersResolver};
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

/// Most paths of one candidate [`Repo::carried_paths`] merges to compare
/// (ADR 0089): each is one three-way text merge. With more, nothing carries.
pub const MAX_CARRY_MERGED_PATHS: usize = 32;

/// Why [`Repo::carry_check`] carries nothing from a candidate.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CarryRefusal {
    /// The candidate is a merge; its paths are not compared.
    Merge,
    /// It changes more than [`MAX_CARRY_PATHS`] paths.
    TooManyPaths,
    /// More than [`MAX_CARRY_MERGED_PATHS`] of its paths would need a merge.
    TooManyMerges,
    /// This reviewed path is missing from the judged changeset or changed
    /// differently there.
    Differs(RepoPath),
    /// The candidate is not stored (never was, or collected since).
    Gone,
    /// This reviewed path's change is not identical in the judged changeset,
    /// and only an identical change carries there (ADR 0092).
    IdenticalOnly {
        path: RepoPath,
        declared_by: CarrySource,
    },
}

/// What makes a path identical-only for carrying (ADR 0092), strictest
/// first.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CarrySource {
    /// The target ref never merges file contents (ADR 0066, 0089).
    StrictRef,
    /// The review policy file's `identical_only_globs` entry that matches.
    Policy(String),
    /// The deepest `OWNERS` file on the path's owners walk, read from the
    /// target ref's head, with a `carry identical` line.
    Owners(String),
}

impl std::fmt::Display for CarrySource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CarrySource::StrictRef => write!(f, "strict land ref"),
            CarrySource::Policy(g) => write!(f, "policy glob {g}"),
            CarrySource::Owners(file) => write!(f, "{file}"),
        }
    }
}

impl std::fmt::Display for CarryRefusal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CarryRefusal::Merge => write!(f, "it is a merge"),
            CarryRefusal::TooManyPaths => {
                write!(f, "it changes more than {MAX_CARRY_PATHS} paths")
            }
            CarryRefusal::TooManyMerges => write!(
                f,
                "more than {MAX_CARRY_MERGED_PATHS} of its paths moved under it"
            ),
            CarryRefusal::Differs(p) => write!(f, "{p} differs"),
            CarryRefusal::Gone => write!(f, "it is no longer stored"),
            CarryRefusal::IdenticalOnly { path, declared_by } => {
                write!(f, "{path} is identical-only ({declared_by})")
            }
        }
    }
}

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

/// The enabled auto rules, the docs globs and the identical-only globs.
#[derive(Clone, Debug, Default)]
pub struct ReviewRules {
    auto: BTreeSet<AutoRule>,
    docs: Option<GlobSet>,
    /// Paths whose verdicts carry only to identical changes (ADR 0092),
    /// with the globs as written, to name the one that matched.
    identical_only: Option<(GlobSet, Vec<String>)>,
}

fn glob_set(globs: &[String], what: &str) -> Result<GlobSet> {
    let mut b = GlobSetBuilder::new();
    for g in globs {
        let glob: Glob = GlobBuilder::new(g)
            .literal_separator(true)
            .build()
            .map_err(|e| Error::Invalid(format!("bad {what} glob {g:?}: {e}")))?;
        b.add(glob);
    }
    b.build()
        .map_err(|e| Error::Invalid(format!("bad {what} globs: {e}")))
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
            Some(glob_set(docs_globs, "docs")?)
        } else {
            None
        };
        Ok(ReviewRules {
            auto,
            docs,
            identical_only: None,
        })
    }

    /// These rules with `globs` as the paths whose verdicts carry only to
    /// identical changes (ADR 0092), matched as `docs_globs` are.
    pub fn with_identical_only(mut self, globs: &[String]) -> Result<Self> {
        self.identical_only = if globs.is_empty() {
            None
        } else {
            Some((glob_set(globs, "identical_only")?, globs.to_vec()))
        };
        Ok(self)
    }

    /// The first `identical_only_globs` entry matching `path`, in the order
    /// written.
    pub fn identical_only_glob(&self, path: &str) -> Option<&str> {
        let (set, globs) = self.identical_only.as_ref()?;
        set.matches(path)
            .into_iter()
            .min()
            .map(|i| globs[i].as_str())
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
    /// How this path reproduces `carried_from`'s change, when that is set.
    pub carried_rule: Option<CarryRule>,
}

/// Verdicts on `from` that count for the paths in `paths` of the changeset
/// being judged, because it makes the change `from` made there (each with
/// how, see [`Repo::carried_paths`]).
#[derive(Clone, Debug)]
pub struct Carried {
    pub from: ChangesetId,
    pub paths: BTreeMap<RepoPath, CarryRule>,
    /// Reviews of `from`, withdrawals included, by reviewers with no review
    /// of their own of the changeset being judged.
    pub reviews: Vec<Review>,
}

/// Which carries a judgement allows besides identical changes (ADR 0089,
/// 0092): the target ref's own rule, the policy's identical-only globs and
/// the `carry` lines of the `OWNERS` files in the target ref's head. Each
/// can only make a path identical-only, never undo another's, so the rule
/// is the same whichever of them a reader looks at first.
///
/// Carrying a verdict and closing a review request use one scope per
/// target ref, so a request closes exactly when its verdicts would carry.
pub struct CarryScope<'o, 'a> {
    merges: bool,
    declared: bool,
    owners: Option<&'o mut OwnersResolver<'a>>,
}

impl<'o, 'a> CarryScope<'o, 'a> {
    /// The scope of a land on a ref: `merges` from
    /// [`Repo::carry_merges`], `owners` reading the ref's head. Without
    /// `owners` (a listing that names no ref), `OWNERS` lines are not read.
    pub fn for_ref(merges: bool, owners: Option<&'o mut OwnersResolver<'a>>) -> Self {
        CarryScope {
            merges,
            declared: true,
            owners,
        }
    }

    /// Merges everywhere, whatever any ref or file declares: for taking a
    /// verdict back, where more than any ref would carry is the safe side
    /// (ADR 0085, 0089).
    pub fn widest() -> Self {
        CarryScope {
            merges: true,
            declared: false,
            owners: None,
        }
    }

    /// Why only an identical change of `path` carries, or `None` when a
    /// rebase or union may.
    pub async fn identical_only(
        &mut self,
        rules: &ReviewRules,
        path: &RepoPath,
    ) -> Result<Option<CarrySource>> {
        if !self.merges {
            return Ok(Some(CarrySource::StrictRef));
        }
        if !self.declared {
            return Ok(None);
        }
        if let Some(g) = rules.identical_only_glob(path.as_str()) {
            return Ok(Some(CarrySource::Policy(g.to_string())));
        }
        if let Some(owners) = self.owners.as_deref_mut() {
            if let Some((CarryDirective::Identical, file)) = owners.carry_of(path).await? {
                return Ok(Some(CarrySource::Owners(file)));
            }
        }
        Ok(None)
    }

    /// The refusal for the first of `paths` that is identical-only.
    async fn refusal(
        &mut self,
        rules: &ReviewRules,
        paths: &[&RepoPath],
    ) -> Result<Option<CarryRefusal>> {
        for p in paths {
            if let Some(declared_by) = self.identical_only(rules, p).await? {
                return Ok(Some(CarryRefusal::IdenticalOnly {
                    path: (*p).clone(),
                    declared_by,
                }));
            }
        }
        Ok(None)
    }
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
            let mut newest: BTreeMap<&str, (&Review, ChangesetId, CarryRule)> = BTreeMap::new();
            for (c, how) in carried.iter().filter_map(|c| {
                let how = c.paths.get(path).filter(|_| change.is_some())?;
                Some((c, *how))
            }) {
                for r in &c.reviews {
                    match newest.entry(r.reviewer.as_str()) {
                        BTreeEntry::Vacant(e) => {
                            e.insert((r, c.from, how));
                        }
                        BTreeEntry::Occupied(mut e) if later(r, e.get().0) => {
                            e.insert((r, c.from, how));
                        }
                        BTreeEntry::Occupied(_) => {}
                    }
                }
            }
            let mut carried_from = None;
            let mut carried_rule = None;
            for (who, (r, from, how)) in newest {
                if !counts(who) {
                    continue;
                }
                let set = match r.verdict {
                    Verdict::Approve => &mut approved,
                    Verdict::Reject => &mut rejected,
                    Verdict::Withdrawn => continue,
                };
                if set.insert(who.to_string()) && carried_from.is_none() {
                    carried_from = Some(from);
                    carried_rule = Some(how);
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
                carried_rule,
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

    /// The paths where `id` makes the change `from` made, each with how,
    /// when every other path `from` changed is one an auto rule approves in
    /// `from`. A path reproduces the change when (ADR 0085, 0089):
    ///
    /// - **identical**: the same old content and kind, each relative to its
    ///   own first parent, and the same new content and kind, so the file is
    ///   byte for byte what was reviewed and the base did not move under it;
    /// - with `merges`, **rebased**: the base moved, and `id`'s new content
    ///   is exactly what the server's rebase of `from`'s change onto `id`'s
    ///   first parent makes (`diffy::merge(from's old, id's old, from's
    ///   new)`, clean); or **union**: that merge conflicts only where both
    ///   sides inserted lines at one place, and `id` keeps both insertions,
    ///   one side's first in every region. Text files on both sides with
    ///   one kind throughout (no symlinks, no kind change), each at most
    ///   [`MAX_AUTO_BYTES`] and [`carry::MAX_CARRY_LINES`] lines, never
    ///   `OWNERS`, at most
    ///   [`MAX_CARRY_MERGED_PATHS`] per candidate. Not where `scope` makes
    ///   the path identical-only: on refs whose lands never merge file
    ///   contents (`--strict-land-ref`), and on paths the policy's globs or
    ///   an `OWNERS` `carry identical` line declare so (ADR 0092).
    ///
    /// `None` when a path of `from` that needed a reviewer is changed
    /// otherwise in `id`, or missing from it (so a reviewed path never
    /// carries without the others it was approved with), when `from` is a
    /// merge or changes more than [`MAX_CARRY_PATHS`] paths. An empty map
    /// means every path of `from` is one an auto rule approves there and
    /// none is reproduced: nothing carries a verdict (callers that carry
    /// verdicts skip it), but nothing reviewed was lost either, which is
    /// what closing a review request asks (ADR 0087). Paths only `id`
    /// changes are judged on their own.
    pub async fn carried_paths(
        &self,
        from: ChangesetId,
        id: ChangesetId,
        rules: &ReviewRules,
        scope: &mut CarryScope<'_, '_>,
    ) -> Result<Option<BTreeMap<RepoPath, CarryRule>>> {
        Ok(self.carry_check(from, id, rules, scope).await?.ok())
    }

    /// Whether verdicts carried to a land on `target_ref` may rest on a
    /// merge (ADR 0089): only where the server's own lands merge file
    /// contents, so a carry never trusts a merge the ref would refuse.
    pub fn carry_merges(&self, target_ref: &str) -> bool {
        self.land_conflicts().for_ref(target_ref) == crate::ConflictPolicy::Merge
    }

    /// [`Self::carried_paths`], saying why nothing carries when it does not.
    ///
    /// Cheap by construction: a path whose new content differs is refused
    /// before any read unless an auto rule could hold for it or it could be
    /// merged, then each matching path costs two file lookups, only the
    /// paths that differ read contents, for the content rules, and a merge
    /// reads three blobs once per process (outcomes are cached by content).
    pub async fn carry_check(
        &self,
        from: ChangesetId,
        id: ChangesetId,
        rules: &ReviewRules,
        scope: &mut CarryScope<'_, '_>,
    ) -> Result<std::result::Result<BTreeMap<RepoPath, CarryRule>, CarryRefusal>> {
        // A candidate with reviews may have been collected: it carries
        // nothing, and the judgement goes on without it.
        let theirs = match self.get_changeset(from).await {
            Ok(cs) => cs,
            Err(Error::NotFound(_)) => return Ok(Err(CarryRefusal::Gone)),
            Err(e) => return Err(e),
        };
        // A merge's changes are not all its own (`merge_parent`, dropped
        // side changes); its paths are not compared.
        if theirs.parents.len() > 1 {
            return Ok(Err(CarryRefusal::Merge));
        }
        if theirs.changes.len() > MAX_CARRY_PATHS {
            return Ok(Err(CarryRefusal::TooManyPaths));
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
        let (px, py) = (
            theirs.parents.first().copied(),
            mine.parents.first().copied(),
        );
        // Could a merge reproduce it? Both modify the file, never `OWNERS`,
        // on different bases (on the same one the merge is `from`'s file).
        // Whether the scope allows it is asked only of these paths, below.
        let may_merge = |p: &RepoPath, c: &FileChange| {
            px != py
                && p.file_name() != OWNERS_FILE
                && matches!(c, FileChange::Modify { .. })
                && matches!(mine.changes.get(p), Some(FileChange::Modify { .. }))
        };
        let mut same_new = Vec::new();
        let mut differ = Vec::new();
        let mut merge = Vec::new();
        for (p, c) in &theirs.changes {
            if mine.changes.get(p).map(new_key) == Some(new_key(c)) {
                same_new.push((p, c));
            } else if may_be_auto(p, c) {
                differ.push((p, c));
            } else if may_merge(p, c) {
                merge.push(p);
            } else {
                return Ok(Err(CarryRefusal::Differs(p.clone())));
            }
        }
        if let Some(refused) = scope.refusal(rules, &merge).await? {
            return Ok(Err(refused));
        }
        if merge.len() > MAX_CARRY_MERGED_PATHS {
            return Ok(Err(CarryRefusal::TooManyMerges));
        }
        let asked = merge.len();
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
        let mut out = BTreeMap::new();
        for (p, c) in same_new {
            if old(px, p).await? == old(py, p).await? {
                out.insert(p.clone(), CarryRule::Identical);
            } else if may_be_auto(p, c) {
                differ.push((p, c));
            } else if may_merge(p, c) {
                merge.push(p);
            } else {
                return Ok(Err(CarryRefusal::Differs(p.clone())));
            }
        }
        for (p, c) in differ {
            if self.auto_rule(rules, p, px, c).await?.is_some() {
                continue;
            }
            if !may_merge(p, c) {
                return Ok(Err(CarryRefusal::Differs(p.clone())));
            }
            merge.push(p);
        }
        if let Some(refused) = scope.refusal(rules, &merge[asked..]).await? {
            return Ok(Err(refused));
        }
        if merge.len() > MAX_CARRY_MERGED_PATHS {
            return Ok(Err(CarryRefusal::TooManyMerges));
        }
        for p in merge {
            let (Some(x_new), Some(y_new)) = (theirs.changes.get(p), mine.changes.get(p)) else {
                return Ok(Err(CarryRefusal::Differs(p.clone())));
            };
            match self.rebased_rule(px, py, p, x_new, y_new).await? {
                Some(rule) => {
                    out.insert(p.clone(), rule);
                }
                None => return Ok(Err(CarryRefusal::Differs(p.clone()))),
            }
        }
        Ok(Ok(out))
    }

    /// How `y_new`, made on `py`, reproduces the change `x_new` made to `p`
    /// on `px` though the file differs between them (ADR 0089): the
    /// server's rebase result, or a union of insertions. `None` for anything
    /// else, and for files the merge does not take: a missing side, a kind
    /// that differs anywhere or is a symlink, more than [`MAX_AUTO_BYTES`],
    /// a blob that cannot be read or is not UTF-8.
    async fn rebased_rule(
        &self,
        px: Option<ChangesetId>,
        py: Option<ChangesetId>,
        p: &RepoPath,
        x_new: &FileChange,
        y_new: &FileChange,
    ) -> Result<Option<CarryRule>> {
        let (
            FileChange::Modify {
                content: xn,
                kind: xk,
                size: xs,
            },
            FileChange::Modify {
                content: yn,
                kind: yk,
                size: ys,
            },
        ) = (x_new, y_new)
        else {
            return Ok(None);
        };
        let (Some(px), Some(py)) = (px, py) else {
            return Ok(None);
        };
        let (Some(xo), Some(yo)) = (
            self.lookup_file(px, p).await?,
            self.lookup_file(py, p).await?,
        ) else {
            return Ok(None);
        };
        let one_kind = [xo.kind, yo.kind, *yk].iter().all(|k| k == xk);
        if !one_kind || *xk == FileKind::Symlink {
            return Ok(None);
        }
        // `y_new` is only hashed, never read, but a bigger result than the
        // bound could not come from inputs within it anyway; refuse alike.
        if [xo.size, yo.size, *xs, *ys]
            .iter()
            .any(|s| *s > MAX_AUTO_BYTES)
        {
            return Ok(None);
        }
        // The file did not move: the merge is `from`'s new file, which
        // `y_new` is not (it would have matched as identical).
        if xo.content == yo.content {
            return Ok(None);
        }
        let key = [xo.content, yo.content, *xn];
        let merged = carry::merged(key, || async move {
            let mut texts = Vec::with_capacity(3);
            for h in key {
                match self
                    .read_content(&h)
                    .await?
                    .and_then(|b| String::from_utf8(b.to_vec()).ok())
                {
                    Some(t) => texts.push(t),
                    // Unreadable or binary: no text merge, as for the
                    // server's rebase.
                    None => return Ok(None),
                }
            }
            Ok(<[String; 3]>::try_from(texts).ok())
        })
        .await?;
        Ok(merged.rule_for(*yn))
    }

    /// Whether `id` puts the reviewed change of `from` on its ref, the
    /// content rule a review request for `from` closes by (ADR 0087):
    /// [`Self::carried_paths`] (in the ref's `scope`, as carrying a verdict
    /// there uses) holds and reproduces at least one path, or
    /// every path of `from` is one an auto rule approves there and `id`
    /// changes each of them in a form an auto rule approves in `id`. Like
    /// carrying, it is decided by content alone, so a forged hint closes
    /// nothing whose reviewed change did not land.
    pub async fn lands_reviewed_change(
        &self,
        from: ChangesetId,
        id: ChangesetId,
        rules: &ReviewRules,
        scope: &mut CarryScope<'_, '_>,
    ) -> Result<bool> {
        let paths = match self.carried_paths(from, id, rules, scope).await {
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
