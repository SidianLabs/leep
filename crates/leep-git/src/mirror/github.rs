//! GitHub pull request reviews into Leep's review gate (ADR 0063).
//!
//! For each two-way prefix ref whose branch has an open pull request, the
//! mirror reads the pull request's reviews, keeps the latest per login, and
//! posts the matching Leep review for the mapped principal on every
//! changeset of the ref's unlanded stack, through a `review_for` token:
//!
//! | latest GitHub review by a mapped login | Leep review |
//! |---|---|
//! | `APPROVED` on the current head commit | `Approve` |
//! | `CHANGES_REQUESTED` | `Reject` |
//! | `APPROVED` on an older commit, `DISMISSED`, or gone where one was posted | `Withdrawn` |
//!
//! The mirror keeps no record of what it posted: it reads Leep's reviews
//! (its own are the ones whose `source` names this repository) and posts
//! only differences, so a crash anywhere is recovered by the next poll.
//! A review a person posted in Leep directly is never touched.

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use anyhow::{bail, Context, Result};
use leep_core::api::ReviewEntry;
use leep_core::review::Verdict;
use leep_core::ChangesetId;
use serde::Deserialize;

use super::{log, Mirror};

/// An open pull request for a branch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Pull {
    pub number: u64,
    /// The head commit the pull request shows now.
    pub head_sha: String,
    /// The branch it merges into, without `refs/heads/`.
    pub base_branch: String,
}

/// One review on a pull request, as GitHub lists them (oldest first).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GhReview {
    pub id: u64,
    pub login: String,
    /// `APPROVED`, `CHANGES_REQUESTED`, `DISMISSED`, `COMMENTED`, ...
    pub state: String,
    /// The head commit the review was given on.
    pub commit_id: String,
}

/// What the mirror needs from GitHub. Implemented over HTTPS by
/// [`GitHubClient`]; tests implement it over a map.
pub trait GitHubApi {
    /// The open pull request whose head is `branch` in `owner/repo`.
    fn open_pull(&self, owner: &str, repo: &str, branch: &str) -> Result<Option<Pull>>;
    /// Every review on the pull request, oldest first.
    fn reviews(&self, owner: &str, repo: &str, number: u64) -> Result<Vec<GhReview>>;
}

/// How the mirror reaches GitHub.
#[derive(Clone, Debug)]
pub struct GitHubConfig {
    pub owner: String,
    pub repo: String,
    /// GitHub login to Leep principal. Only mapped logins count.
    pub users: BTreeMap<String, String>,
    /// A token with read access to pull requests.
    pub token: Option<String>,
    /// `https://api.github.com`, or a GitHub Enterprise API root.
    pub api_url: String,
}

impl GitHubConfig {
    /// The `source` every review this mirror posts starts with, which is
    /// how it tells its own reviews from a person's.
    pub fn source_prefix(&self) -> String {
        format!("github:{}/{}#", self.owner, self.repo)
    }
}

/// `owner/repo` from the remote URL of a GitHub repository: `https://`
/// (with or without credentials), `ssh://git@`, and `git@host:` forms, with
/// or without `.git`.
pub fn repo_from_remote(url: &str) -> Option<(String, String)> {
    let rest = if let Some((_, r)) = url.split_once("://") {
        // host[:port]/owner/repo, with an optional user@ before the host.
        let r = r.rsplit_once('@').map_or(r, |(_, h)| h);
        r.split_once('/')?.1
    } else if let Some((_, r)) = url.split_once(':') {
        // git@host:owner/repo
        r
    } else {
        return None;
    };
    let rest = rest.trim_end_matches('/');
    let rest = rest.strip_suffix(".git").unwrap_or(rest);
    let (owner, repo) = rest.split_once('/')?;
    if owner.is_empty() || repo.is_empty() || repo.contains('/') {
        return None;
    }
    Some((owner.to_string(), repo.to_string()))
}

/// `login = principal` per line; `#` starts a comment. A login or
/// principal listed twice is an error, so one review means one thing.
pub fn parse_users(text: &str) -> Result<BTreeMap<String, String>> {
    let mut users = BTreeMap::new();
    let mut principals = BTreeSet::new();
    for (n, raw) in text.lines().enumerate() {
        let line = raw.split('#').next().unwrap_or("").trim();
        if line.is_empty() {
            continue;
        }
        let Some((login, principal)) = line.split_once('=') else {
            bail!("line {}: expected `login = principal`, got {raw:?}", n + 1);
        };
        let (login, principal) = (login.trim(), principal.trim());
        if login.is_empty() || principal.is_empty() {
            bail!("line {}: expected `login = principal`, got {raw:?}", n + 1);
        }
        if users
            .insert(login.to_string(), principal.to_string())
            .is_some()
        {
            bail!("line {}: login {login:?} is listed twice", n + 1);
        }
        if !principals.insert(principal.to_string()) {
            bail!("line {}: principal {principal:?} is listed twice", n + 1);
        }
    }
    Ok(users)
}

/// The GitHub REST API over HTTPS.
pub struct GitHubClient {
    http: reqwest::blocking::Client,
    api_url: String,
    token: Option<String>,
}

#[derive(Deserialize)]
struct PullJson {
    number: u64,
    head: RefJson,
    base: RefJson,
}

#[derive(Deserialize)]
struct RefJson {
    sha: Option<String>,
    #[serde(rename = "ref")]
    name: String,
}

#[derive(Deserialize)]
struct ReviewJson {
    id: u64,
    user: Option<UserJson>,
    state: String,
    commit_id: Option<String>,
}

#[derive(Deserialize)]
struct UserJson {
    login: String,
}

impl GitHubClient {
    pub fn new(api_url: &str, token: Option<String>, timeout: Duration) -> Result<GitHubClient> {
        let http = reqwest::blocking::Client::builder()
            .user_agent("leep-git-mirror")
            .timeout(timeout)
            .build()
            .context("building the GitHub client")?;
        Ok(GitHubClient {
            http,
            api_url: api_url.trim_end_matches('/').to_string(),
            token,
        })
    }

    fn get<T: serde::de::DeserializeOwned>(
        &self,
        path: &str,
        query: &[(&str, String)],
    ) -> Result<T> {
        let mut req = self
            .http
            .get(format!("{}{path}", self.api_url))
            .header("Accept", "application/vnd.github+json")
            .header("X-GitHub-Api-Version", "2022-11-28")
            .query(query);
        if let Some(t) = &self.token {
            req = req.bearer_auth(t);
        }
        let resp = req.send().context("GitHub request")?;
        let status = resp.status();
        if !status.is_success() {
            let body: String = resp.text().unwrap_or_default().chars().take(200).collect();
            bail!("GitHub answered {status} to {path}: {body}");
        }
        resp.json().context("GitHub answer")
    }
}

impl GitHubApi for GitHubClient {
    fn open_pull(&self, owner: &str, repo: &str, branch: &str) -> Result<Option<Pull>> {
        let pulls: Vec<PullJson> = self.get(
            &format!("/repos/{owner}/{repo}/pulls"),
            &[
                ("head", format!("{owner}:{branch}")),
                ("state", "open".into()),
                ("per_page", "5".into()),
            ],
        )?;
        Ok(pulls.into_iter().next().and_then(|p| {
            Some(Pull {
                number: p.number,
                head_sha: p.head.sha?,
                base_branch: p.base.name,
            })
        }))
    }

    fn reviews(&self, owner: &str, repo: &str, number: u64) -> Result<Vec<GhReview>> {
        let mut out = Vec::new();
        for page in 1..=20u32 {
            let batch: Vec<ReviewJson> = self.get(
                &format!("/repos/{owner}/{repo}/pulls/{number}/reviews"),
                &[("per_page", "100".into()), ("page", page.to_string())],
            )?;
            let n = batch.len();
            out.extend(batch.into_iter().filter_map(|r| {
                Some(GhReview {
                    id: r.id,
                    login: r.user?.login,
                    state: r.state,
                    commit_id: r.commit_id.unwrap_or_default(),
                })
            }));
            if n < 100 {
                break;
            }
        }
        Ok(out)
    }
}

/// The Leep verdict a login's latest review asks for.
fn wanted(latest: &GhReview, head_sha: &str) -> Option<Verdict> {
    match latest.state.as_str() {
        "APPROVED" if latest.commit_id == head_sha => Some(Verdict::Approve),
        "APPROVED" | "DISMISSED" => Some(Verdict::Withdrawn),
        "CHANGES_REQUESTED" => Some(Verdict::Reject),
        _ => None,
    }
}

/// Latest review per login, in the order GitHub lists them; comments and
/// other states that are no verdict are skipped.
fn latest_per_login(reviews: &[GhReview]) -> BTreeMap<&str, &GhReview> {
    let mut latest: BTreeMap<&str, &GhReview> = BTreeMap::new();
    for r in reviews {
        if matches!(
            r.state.as_str(),
            "APPROVED" | "CHANGES_REQUESTED" | "DISMISSED"
        ) {
            latest.insert(r.login.as_str(), r);
        }
    }
    latest
}

impl Mirror<'_> {
    /// Bring every open pull request's reviews into Leep. Called after each
    /// remote poll. Failures are logged and counted; the next poll tries
    /// again from current values.
    pub(super) fn sync_github_reviews(&mut self) {
        let Some(cfg) = self.cfg.github.clone() else {
            return;
        };
        if self.github.is_none() {
            return;
        }
        for i in 0..self.refs.len() {
            if self.refs[i].prefix.is_none() || self.refs[i].bad_name.is_some() {
                continue;
            }
            let (Some(l), Some(_)) = (self.refs[i].leep, self.refs[i].remote) else {
                continue;
            };
            if let Err(e) = self.sync_pull_reviews(i, l, &cfg) {
                self.count_error("github");
                log(format!("{}: GitHub reviews: {e:#}", self.refs[i].spec.leep));
            }
        }
    }

    fn sync_pull_reviews(&mut self, i: usize, l: ChangesetId, cfg: &GitHubConfig) -> Result<()> {
        let api = self.github.as_deref().expect("checked");
        let name = self.refs[i].spec.leep.clone();
        let branch = self.refs[i]
            .spec
            .git
            .strip_prefix("refs/heads/")
            .unwrap_or(&self.refs[i].spec.git)
            .to_string();
        let Some(pull) = api.open_pull(&cfg.owner, &cfg.repo, &branch)? else {
            return Ok(());
        };
        // The stack: the ref's first-parent chain down to where it joins
        // the pull request's base, which must be a mirrored ref.
        let base_git = format!("refs/heads/{}", pull.base_branch);
        let Some(base) = self
            .refs
            .iter()
            .find(|r| r.configured && r.spec.git == base_git)
            .and_then(|r| r.leep)
        else {
            log(format!(
                "{name}: pull request #{} targets {}, which is not a mirrored ref; its reviews \
                 are not brought into Leep",
                pull.number, pull.base_branch
            ));
            return Ok(());
        };
        let Some(join) = self.leep.merge_base(l, base)? else {
            return Ok(());
        };
        let mut stack = Vec::new();
        let mut cur = l;
        while cur != join && stack.len() < 1024 {
            stack.push(cur);
            let cs = self.leep.changeset(&cur)?;
            let Some(p) = cs.parents.first() else { break };
            cur = *p;
        }
        if stack.is_empty() {
            return Ok(());
        }

        let reviews = api.reviews(&cfg.owner, &cfg.repo, pull.number)?;
        let latest = latest_per_login(&reviews);
        let mut desired: BTreeMap<&str, (Verdict, String)> = BTreeMap::new();
        let mut unmapped = BTreeSet::new();
        for (login, r) in &latest {
            let Some(principal) = cfg.users.get(*login) else {
                unmapped.insert(*login);
                continue;
            };
            if let Some(v) = wanted(r, &pull.head_sha) {
                let source = format!("{}{}/reviews/{}", cfg.source_prefix(), pull.number, r.id);
                desired.insert(principal.as_str(), (v, source));
            }
        }
        let prefix = cfg.source_prefix();
        let mut posted = (0u64, 0u64, 0u64);
        for cs in stack {
            let current: Vec<ReviewEntry> = self.leep.reviews(cs)?;
            let mine =
                |e: &ReviewEntry| e.source.as_deref().is_some_and(|s| s.starts_with(&prefix));
            for (principal, (verdict, source)) in &desired {
                match current.iter().find(|e| e.reviewer == *principal) {
                    // A review the person posted in Leep themselves wins.
                    Some(e) if !mine(e) => continue,
                    Some(e) if e.verdict == *verdict => continue,
                    // Nothing to take back.
                    None if *verdict == Verdict::Withdrawn => continue,
                    _ => {}
                }
                self.leep
                    .post_review(cs, principal, *verdict, source)
                    .with_context(|| format!("posting {verdict:?} for {principal} on {cs}"))?;
                match verdict {
                    Verdict::Approve => posted.0 += 1,
                    Verdict::Reject => posted.1 += 1,
                    Verdict::Withdrawn => posted.2 += 1,
                }
            }
            // Mirror-posted verdicts whose GitHub review is gone.
            for e in current.iter().filter(|e| mine(e)) {
                if desired.contains_key(e.reviewer.as_str()) || e.verdict == Verdict::Withdrawn {
                    continue;
                }
                let source = format!("{}{}/reviews/gone", prefix, pull.number);
                self.leep
                    .post_review(cs, &e.reviewer, Verdict::Withdrawn, &source)
                    .with_context(|| format!("withdrawing {}'s review on {cs}", e.reviewer))?;
                posted.2 += 1;
            }
        }
        let r = &mut self.refs[i];
        let c = &mut r.st.github_reviews;
        c.approved += posted.0;
        c.rejected += posted.1;
        c.withdrawn += posted.2;
        c.unmapped += unmapped.len() as u64;
        r.st.pull_request = Some(pull.number);
        if posted != (0, 0, 0) {
            log(format!(
                "{name}: pull request #{}: posted {} approvals, {} rejections and {} withdrawals \
                 to Leep",
                pull.number, posted.0, posted.1, posted.2
            ));
        }
        for login in unmapped {
            log(format!(
                "{name}: pull request #{}: review by {login}, which --github-users does not map; \
                 ignored",
                pull.number
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn repositories_are_read_from_every_remote_form() {
        let want = Some(("acme".to_string(), "app".to_string()));
        assert_eq!(repo_from_remote("https://github.com/acme/app.git"), want);
        assert_eq!(repo_from_remote("https://github.com/acme/app"), want);
        assert_eq!(
            repo_from_remote("https://bot:tok@github.com/acme/app.git"),
            want
        );
        assert_eq!(repo_from_remote("ssh://git@github.com/acme/app.git"), want);
        assert_eq!(repo_from_remote("git@github.com:acme/app.git"), want);
        assert_eq!(repo_from_remote("git@github.com:acme/app/"), want);
        assert_eq!(repo_from_remote("/var/tmp/r.git"), None);
        assert_eq!(repo_from_remote("https://github.com/acme"), None);
        assert_eq!(
            repo_from_remote("https://gh.example.com/org/sub/app.git"),
            None
        );
    }

    #[test]
    fn user_files_map_logins_once() {
        let u = parse_users("# reviewers\nada = alice\n grace=bob # ops\n\n").unwrap();
        assert_eq!(u["ada"], "alice");
        assert_eq!(u["grace"], "bob");
        assert!(parse_users("ada alice").is_err());
        assert!(
            parse_users("ada = alice\nada = bob").is_err(),
            "login twice"
        );
        assert!(
            parse_users("ada = alice\ngrace = alice").is_err(),
            "principal twice"
        );
        assert!(parse_users("= alice").is_err());
    }

    #[test]
    fn the_latest_review_per_login_decides() {
        let r = |id, login: &str, state: &str, sha: &str| GhReview {
            id,
            login: login.into(),
            state: state.into(),
            commit_id: sha.into(),
        };
        let reviews = [
            r(1, "ada", "APPROVED", "old"),
            r(2, "ada", "COMMENTED", "new"),
            r(3, "grace", "CHANGES_REQUESTED", "new"),
            r(4, "grace", "APPROVED", "new"),
            r(5, "linus", "APPROVED", "new"),
            r(6, "linus", "DISMISSED", "new"),
        ];
        let latest = latest_per_login(&reviews);
        assert_eq!(latest["ada"].id, 1, "a comment does not replace a verdict");
        assert_eq!(wanted(latest["ada"], "new"), Some(Verdict::Withdrawn));
        assert_eq!(wanted(latest["ada"], "old"), Some(Verdict::Approve));
        assert_eq!(wanted(latest["grace"], "new"), Some(Verdict::Approve));
        assert_eq!(wanted(latest["linus"], "new"), Some(Verdict::Withdrawn));
        assert_eq!(wanted(&r(7, "x", "COMMENTED", "new"), "new"), None);
    }
}
