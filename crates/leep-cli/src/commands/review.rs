//! `leep review` and `leep reviews`: record verdicts and see what a changeset
//! still needs before it can land (ADR 0011).

use std::time::{Duration, Instant};

use clap::{Args, Subcommand};
use leep_core::api::{
    AddReviewRequest, AddReviewResponse, OriginStatus, PathReview, ReviewStatusResponse,
    ReviewsResponse,
};
use leep_core::review::Verdict;
use leep_core::{outln, ChangesetId};

use super::{print_json, Remote};
use crate::error::{exit, Result};
use crate::fmt::{plural, short, utc_time};

#[derive(Subcommand, Debug)]
pub enum ReviewCmd {
    /// Approve a changeset as the authenticated user, then say whether it
    /// may now land on the target ref or which paths still wait.
    Approve(ApproveArgs),
    /// Reject a changeset; an owner's rejection blocks it from landing.
    /// Says where that leaves it, as `approve` does.
    Reject(RejectArgs),
    /// Take back your earlier approval or rejection; it then counts as no review.
    Withdraw(ApproveArgs),
    /// Show, per changed path, who still has to approve (exit 1 if not
    /// approved; exit 0 once the ref already holds it). `--wait` polls until
    /// it is approved.
    Status(StatusArgs),
    /// List the reviews a changeset has.
    List(ListArgs),
    /// Ask for a review of a changeset before it lands (default reviewers:
    /// the owners of the paths still pending).
    Request(super::review_request::RequestArgs),
    /// List the review requests waiting for you (--mine: the ones you made).
    Inbox(super::review_request::InboxArgs),
    /// Withdraw a review request you made.
    Cancel(super::review_request::CancelArgs),
}

#[derive(Args, Debug)]
pub struct ApproveArgs {
    /// Changeset id, ref name, or `@` for the working copy base.
    pub rev: String,
    /// Optional comment.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
}

#[derive(Args, Debug)]
pub struct RejectArgs {
    pub rev: String,
    /// Why it is rejected.
    #[arg(short = 'm', long)]
    pub message: String,
}

#[derive(Args, Debug)]
pub struct StatusArgs {
    pub rev: String,
    /// Ref whose owners and head to judge against (default: the target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
    /// Ask again, less and less often, until it is approved (exit 0), a
    /// path is rejected or SECS pass (exit 1). Default 3600; 0 waits
    /// without limit.
    #[arg(
        long,
        value_name = "SECS",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "3600"
    )]
    pub wait: Option<u64>,
}

#[derive(Args, Debug)]
pub struct ListArgs {
    pub rev: String,
}

pub fn run(r: &Remote, cmd: &ReviewCmd) -> Result<i32> {
    match cmd {
        ReviewCmd::Approve(a) => verdict(r, &a.rev, Verdict::Approve, a.message.clone()),
        ReviewCmd::Reject(a) => verdict(r, &a.rev, Verdict::Reject, Some(a.message.clone())),
        ReviewCmd::Withdraw(a) => verdict(r, &a.rev, Verdict::Withdrawn, a.message.clone()),
        ReviewCmd::Status(a) => status(r, a),
        ReviewCmd::List(a) => list(r, a),
        ReviewCmd::Request(a) => super::review_request::request(r, a),
        ReviewCmd::Inbox(a) => super::review_request::inbox(r, a),
        ReviewCmd::Cancel(a) => super::review_request::cancel(r, a),
    }
}

fn verdict(r: &Remote, rev: &str, v: Verdict, comment: Option<String>) -> Result<i32> {
    let id = r.resolve(rev)?;
    let resp = r.client.add_review(
        &id,
        &AddReviewRequest {
            verdict: v,
            comment: comment.unwrap_or_default(),
            reviewer: None,
            source: None,
        },
    )?;
    if r.json {
        print_json(&resp)?;
    } else {
        let word = match v {
            Verdict::Approve => "Approved",
            Verdict::Reject => "Rejected",
            Verdict::Withdrawn => "Withdrew review of",
        };
        outln!("{word} {} as {}", short(id), resp.reviewer);
        for line in verdict_notes(v, &resp) {
            outln!("{line}");
        }
        // Only a hint: the verdict is recorded whatever this says. Where it
        // already landed, the land is past and the status says nothing new.
        if resp.on_refs.iter().any(|x| x == r.target_ref) {
            return Ok(0);
        }
        match r.client.review_status(&id, r.target_ref) {
            Ok(st) => {
                for line in verdict_follow_up(&st) {
                    outln!("{line}");
                }
            }
            Err(e) => eprintln!(
                "(could not read the review status on {}: {e})",
                r.target_ref
            ),
        }
    }
    Ok(0)
}

/// What the verdict's answer adds: that the changeset already landed, and
/// which originals the same word was written on.
pub fn verdict_notes(v: Verdict, resp: &AddReviewResponse) -> Vec<String> {
    let mut out = Vec::new();
    if !resp.on_refs.is_empty() {
        out.push(format!(
            "already on {}: verdicts now only affect copies (cherry-picks, rebases)",
            resp.on_refs.join(", ")
        ));
    }
    let done = match v {
        Verdict::Reject => "rejected",
        _ => "withdrawn",
    };
    for x in &resp.taken_back {
        out.push(format!(
            "also {done} on {} (your approval of it carried here)",
            short(*x)
        ));
    }
    out
}

pub fn status(r: &Remote, args: &StatusArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let target = args.target_ref.as_deref().unwrap_or(r.target_ref);
    if let Some(secs) = args.wait {
        return wait(r, id, target, &Backoff::waiting(secs));
    }
    let resp = r.client.review_status(&id, target)?;
    report(r, &resp)?;
    Ok(status_exit(&resp))
}

fn report(r: &Remote, resp: &ReviewStatusResponse) -> Result<()> {
    if r.json {
        print_json(resp)?;
    } else {
        for line in status_lines(resp) {
            outln!("{line}");
        }
    }
    Ok(())
}

/// A predicate: "not approved" is exit 1, not an error (ADR 0034). A
/// changeset the ref already holds has nothing left to wait for.
fn status_exit(resp: &ReviewStatusResponse) -> i32 {
    if resp.approved || landed(resp) {
        exit::OK
    } else {
        exit::USER
    }
}

fn landed(resp: &ReviewStatusResponse) -> bool {
    resp.on_refs.contains(&resp.target_ref)
}

fn rejected(resp: &ReviewStatusResponse) -> bool {
    resp.paths.iter().any(|p| p.rule == "rejected")
}

/// How `--wait` paces its questions: the first after `first`, each later
/// one twice as late, never more than `max` apart, for at most `limit`.
#[derive(Clone, Debug)]
pub struct Backoff {
    pub first: Duration,
    pub max: Duration,
    /// `None` waits without limit.
    pub limit: Option<Duration>,
}

impl Backoff {
    /// `--wait=SECS`: 0 means no limit.
    pub fn waiting(secs: u64) -> Backoff {
        Backoff {
            first: Duration::from_secs(2),
            max: Duration::from_secs(60),
            limit: (secs > 0).then(|| Duration::from_secs(secs)),
        }
    }
}

/// Ask for the review status of `id` on `target` until it is approved or
/// the ref holds it (0), a path is rejected (1, at once: someone has to act
/// first) or the limit passes (1). Prints the last status either way.
pub fn wait(r: &Remote, id: ChangesetId, target: &str, b: &Backoff) -> Result<i32> {
    let start = Instant::now();
    let mut delay = b.first;
    let mut said = false;
    loop {
        let resp = r.client.review_status(&id, target)?;
        if resp.approved || landed(&resp) || rejected(&resp) {
            report(r, &resp)?;
            if rejected(&resp) && !resp.approved && !landed(&resp) {
                eprintln!(
                    "stopped waiting: {} was rejected for {target};                      `leep review list {}` shows why",
                    short(id),
                    short(id)
                );
            }
            return Ok(status_exit(&resp));
        }
        let left = b.limit.map(|l| l.saturating_sub(start.elapsed()));
        if left == Some(Duration::ZERO) {
            report(r, &resp)?;
            eprintln!(
                "timed out after {}s waiting for approval of {} for {target}",
                start.elapsed().as_secs(),
                short(id)
            );
            return Ok(exit::USER);
        }
        if !said {
            said = true;
            let pending = resp.paths.iter().filter(|p| p.rule == "pending").count();
            eprintln!(
                "waiting for approval of {} for {target} ({} pending{})...",
                short(id),
                plural(pending, "path"),
                match b.limit {
                    Some(l) => format!(", up to {}s", l.as_secs()),
                    None => String::new(),
                }
            );
        }
        std::thread::sleep(left.map_or(delay, |l| l.min(delay)));
        delay = (delay * 2).min(b.max);
    }
}

pub fn list(r: &Remote, args: &ListArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let resp = r.client.reviews(&id)?;
    if r.json {
        print_json(&resp)?;
    } else {
        for line in review_lines(id, &resp) {
            outln!("{line}");
        }
    }
    Ok(0)
}

/// Human-readable review status, one line per changed path, then how to
/// see the change. The server's rule names stay in `--json`; here each path
/// says what satisfied it or what it still needs.
pub fn status_lines(resp: &ReviewStatusResponse) -> Vec<String> {
    let mut out = vec![format!(
        "changeset {} on {}: {}",
        short(resp.changeset),
        resp.target_ref,
        if landed(resp) {
            // Its land is past: "not approved" would read as a problem.
            format!("landed on {}", resp.target_ref)
        } else if resp.approved {
            "approved".into()
        } else {
            "not approved".into()
        }
    )];
    let width = resp
        .paths
        .iter()
        .map(|p| p.path.as_str().chars().count())
        .max()
        .unwrap_or(0);
    for p in &resp.paths {
        let (state, why) = path_state(p);
        out.push(format!("  {state:<8} {:<width$}  {why}", p.path.as_str()));
    }
    if resp.paths.is_empty() {
        out.push("  (no changed paths)".into());
    }
    for o in &resp.origins {
        out.push(origin_line(resp.changeset, o));
    }
    out.push(format!("diff: leep diff {}", short(resp.changeset)));
    out
}

/// What became of the verdicts on an origin the changeset names
/// (`leep.rebased_from`): carried, or why not and how to compare.
fn origin_line(id: ChangesetId, o: &OriginStatus) -> String {
    let mut by: Vec<String> = Vec::new();
    for v in [Verdict::Approve, Verdict::Reject, Verdict::Withdrawn] {
        let names: Vec<&str> = o
            .reviews
            .iter()
            .filter(|r| r.verdict == v)
            .map(|r| r.reviewer.as_str())
            .collect();
        if !names.is_empty() {
            let word = match v {
                Verdict::Approve => "approved",
                Verdict::Reject => "rejected",
                Verdict::Withdrawn => "withdrawn",
            };
            by.push(format!("{word} by {}", names.join(", ")));
        }
    }
    let mut line = format!("replaces {}", short(o.from));
    if !by.is_empty() {
        line.push_str(&format!(", {}", by.join(", ")));
    }
    if o.carried {
        line.push_str("; carried");
        return line;
    }
    let why = if !o.differs.is_empty() {
        let paths: Vec<&str> = o.differs.iter().map(|p| p.as_str()).collect();
        format!("{} differs", paths.join(", "))
    } else {
        o.reason
            .clone()
            .unwrap_or_else(|| "nothing it reviewed is reproduced".into())
    };
    line.push_str(&format!(
        "; not carried: {why} (leep diff {}; leep diff {})",
        short(o.from),
        short(id)
    ));
    line
}

/// How a carried verdict's path reproduces the reviewed change, in words.
fn carried_note(p: &PathReview) -> Option<String> {
    let from = p.carried_from?;
    let how = match p.carried_rule.as_deref() {
        Some("identical") => " (identical)",
        Some("rebased") => " (rebased onto the new base)",
        Some("union") => " (both sides' insertions kept)",
        _ => "",
    };
    Some(format!("; carried from {}{how}", short(from)))
}

/// The state column and the explanation for one path, saying where a
/// carried verdict came from.
fn path_state(p: &PathReview) -> (&'static str, String) {
    let (state, mut why) = path_rule_state(p);
    if matches!(p.rule.as_str(), "owner" | "rejected") {
        if let Some(note) = carried_note(p) {
            why.push_str(&note);
        }
    }
    (state, why)
}

fn path_rule_state(p: &PathReview) -> (&'static str, String) {
    let who = |names: &[String]| names.join(", ");
    let whose = if p.owners.is_empty() {
        "any reviewer"
    } else {
        "owner"
    };
    let rejected = || {
        let mut why = format!("rejected by {}", who(&p.rejected_by));
        if !p.approved_by.is_empty() {
            why.push_str(&format!("; approved by {}", who(&p.approved_by)));
        }
        why
    };
    match p.rule.as_str() {
        "pending" if p.owners.is_empty() => {
            ("PENDING", "needs an approval from any reviewer".into())
        }
        "pending" => ("PENDING", format!("needs one of: {}", who(&p.owners))),
        "rejected" => ("REJECTED", rejected()),
        "owner" => (
            "ok",
            format!("approved by {} ({whose})", who(&p.approved_by)),
        ),
        "merge_parent" => ("ok", "unchanged from a merge parent".into()),
        rule if rule.starts_with("auto:") => ("ok", format!("needs no review ({rule})")),
        // A rule this client does not know: say it as the server does.
        rule => ("?", format!("rule {rule}")),
    }
}

/// After a verdict: whether the changeset may now land on `target_ref`, or
/// which paths still hold it back.
pub fn verdict_follow_up(resp: &ReviewStatusResponse) -> Vec<String> {
    if resp.approved {
        return vec![format!("now approved for {}", resp.target_ref)];
    }
    let mut out = vec![format!("not yet approved for {}:", resp.target_ref)];
    for p in &resp.paths {
        let (state, why) = path_state(p);
        if state != "ok" {
            out.push(format!("  {state:<8} {}  {why}", p.path));
        }
    }
    out
}

/// Human-readable review list.
pub fn review_lines(id: leep_core::ChangesetId, resp: &ReviewsResponse) -> Vec<String> {
    if resp.reviews.is_empty() {
        return vec![format!("changeset {} has no reviews", short(id))];
    }
    let mut out = vec![format!(
        "{} on {}:",
        plural(resp.reviews.len(), "review"),
        short(id)
    )];
    // A carried review counts for landing; the note would then be false.
    let carried = resp.reviews.iter().any(|r| r.carried_from.is_some());
    if let Some(via) = resp.via.filter(|_| !carried) {
        out.push(format!(
            "  (reviews of {}, which this changeset was rebased from; they do not count for landing)",
            short(via)
        ));
    }
    for r in &resp.reviews {
        let mut line = format!(
            "  {} {} {}",
            r.reviewer,
            r.verdict.as_str(),
            utc_time(r.timestamp)
        );
        if !r.comment.is_empty() {
            line.push_str(&format!(" {:?}", r.comment));
        }
        match (&r.source, &r.via) {
            (Some(s), Some(v)) => line.push_str(&format!(" [{s} via {v}]")),
            (Some(s), None) => line.push_str(&format!(" [{s}]")),
            (None, Some(v)) => line.push_str(&format!(" [via {v}]")),
            (None, None) => {}
        }
        if let Some(from) = r.carried_from {
            line.push_str(&format!(" (carried from {})", short(from)));
        }
        out.push(line);
    }
    out
}

#[cfg(test)]
mod tests {
    use leep_core::api::ReviewEntry;
    use leep_core::review::Review;
    use leep_core::{ChangesetId, Hash, RepoPath};

    use super::*;

    fn id() -> ChangesetId {
        ChangesetId(Hash::of_blob(b"x"))
    }

    fn path(p: &str, rule: &str, owners: &[&str], ok: &[&str], no: &[&str]) -> PathReview {
        let v = |s: &[&str]| s.iter().map(|x| x.to_string()).collect();
        PathReview {
            path: RepoPath::new(p).unwrap(),
            rule: rule.into(),
            owners: v(owners),
            approved_by: v(ok),
            rejected_by: v(no),
            carried_from: None,
            carried_rule: None,
        }
    }

    #[test]
    fn status_says_what_each_path_needs() {
        let resp = ReviewStatusResponse {
            changeset: id(),
            target_ref: "main".into(),
            approved: false,
            paths: vec![
                path("a.rs", "auto:comment_only", &["alice"], &[], &[]),
                path("b.rs", "pending", &["alice", "bob"], &[], &[]),
                path("c.rs", "rejected", &[], &["carol"], &["dan"]),
                path("d.rs", "owner", &[], &["rhea"], &[]),
                path("e.rs", "owner", &["alice"], &["alice"], &[]),
                path("f.rs", "pending", &[], &[], &[]),
                path("g.rs", "merge_parent", &[], &[], &[]),
                path("h.rs", "future_rule", &[], &[], &[]),
            ],
            on_refs: Default::default(),
            origins: Default::default(),
        };
        let lines = status_lines(&resp);
        let short = short(id());
        assert_eq!(
            lines,
            [
                format!("changeset {short} on main: not approved"),
                "  ok       a.rs  needs no review (auto:comment_only)".into(),
                "  PENDING  b.rs  needs one of: alice, bob".into(),
                "  REJECTED c.rs  rejected by dan; approved by carol".into(),
                "  ok       d.rs  approved by rhea (any reviewer)".into(),
                "  ok       e.rs  approved by alice (owner)".into(),
                "  PENDING  f.rs  needs an approval from any reviewer".into(),
                "  ok       g.rs  unchanged from a merge parent".into(),
                "  ?        h.rs  rule future_rule".into(),
                format!("diff: leep diff {short}"),
            ]
        );
        for l in &lines {
            assert!(
                !l.contains("PENDING  pending") && !l.contains(" owner "),
                "{l}"
            );
        }
        let follow = verdict_follow_up(&resp);
        assert_eq!(follow[0], "not yet approved for main:");
        assert_eq!(follow.len(), 5, "{follow:?}");
        assert!(
            follow[1].contains("b.rs") && follow[3].contains("f.rs"),
            "{follow:?}"
        );
    }

    #[test]
    fn empty_status_says_so() {
        let resp = ReviewStatusResponse {
            changeset: id(),
            target_ref: "main".into(),
            approved: true,
            paths: vec![],
            on_refs: Default::default(),
            origins: Default::default(),
        };
        let lines = status_lines(&resp);
        assert!(lines[0].ends_with(": approved"));
        assert_eq!(lines[1], "  (no changed paths)");
        assert_eq!(verdict_follow_up(&resp), ["now approved for main"]);
    }

    #[test]
    fn review_list_mentions_via() {
        let none = ReviewsResponse {
            reviews: vec![],
            via: None,
        };
        assert!(review_lines(id(), &none)[0].contains("no reviews"));
        let resp = ReviewsResponse {
            reviews: vec![ReviewEntry {
                source: Some("github:o/r#1".into()),
                via: Some("mirror".into()),
                review: Review {
                    reviewer: "alice".into(),
                    verdict: Verdict::Approve,
                    comment: "ok".into(),
                    timestamp: 0,
                },
                carried_from: Default::default(),
            }],
            via: Some(ChangesetId(Hash::of_blob(b"orig"))),
        };
        let lines = review_lines(id(), &resp);
        assert!(lines[0].starts_with("1 review "), "{lines:?}");
        assert!(lines[1].contains("rebased from"));
        assert!(lines[2].contains("alice approve 1970-01-01") && lines[2].contains("\"ok\""));
        assert!(lines[2].ends_with("[github:o/r#1 via mirror]"), "{lines:?}");
    }

    #[test]
    fn a_landed_changeset_says_so_instead_of_not_approved() {
        let mut resp = ReviewStatusResponse {
            changeset: id(),
            target_ref: "main".into(),
            approved: false,
            paths: vec![path("a.rs", "pending", &[], &[], &[])],
            on_refs: vec!["release".into()],
            origins: vec![],
        };
        // Held by another ref only: still judged for main.
        assert!(status_lines(&resp)[0].ends_with(": not approved"));
        assert_eq!(status_exit(&resp), exit::USER);
        resp.on_refs.push("main".into());
        assert!(status_lines(&resp)[0].ends_with(": landed on main"));
        assert_eq!(status_exit(&resp), exit::OK);
    }

    #[test]
    fn verdict_notes_name_the_refs_and_what_was_taken_back() {
        let x = ChangesetId(Hash::of_blob(b"orig"));
        let mut resp = AddReviewResponse {
            reviewer: "rhea".into(),
            verdict: Verdict::Withdrawn,
            on_refs: vec![],
            taken_back: vec![],
        };
        assert!(verdict_notes(Verdict::Withdrawn, &resp).is_empty());
        resp.on_refs = vec!["main".into(), "release".into()];
        resp.taken_back = vec![x];
        assert_eq!(
            verdict_notes(Verdict::Withdrawn, &resp),
            [
                "already on main, release: verdicts now only affect copies (cherry-picks, rebases)"
                    .to_string(),
                format!(
                    "also withdrawn on {} (your approval of it carried here)",
                    short(x)
                ),
            ]
        );
        assert!(verdict_notes(Verdict::Reject, &resp)[1].starts_with("also rejected on "));
    }

    #[test]
    fn carried_reviews_say_where_from_and_count() {
        let from = ChangesetId(Hash::of_blob(b"orig"));
        let resp = ReviewsResponse {
            reviews: vec![ReviewEntry {
                source: None,
                via: None,
                review: Review {
                    reviewer: "rhea".into(),
                    verdict: Verdict::Approve,
                    comment: String::new(),
                    timestamp: 0,
                },
                carried_from: Some(from),
            }],
            // Even beside a `via` (from an older server mixing both), the
            // "do not count" note would be false.
            via: Some(from),
        };
        let lines = review_lines(id(), &resp);
        assert_eq!(lines.len(), 2, "{lines:?}");
        assert!(
            lines[1].ends_with(&format!("(carried from {})", short(from))),
            "{lines:?}"
        );
        assert!(
            !lines.iter().any(|l| l.contains("do not count")),
            "{lines:?}"
        );
    }

    #[test]
    fn status_says_how_a_verdict_carried_and_why_one_did_not() {
        use leep_core::api::OriginReview;
        let x = ChangesetId(Hash::of_blob(b"orig"));
        let x2 = ChangesetId(Hash::of_blob(b"orig2"));
        let mut a = path("a.rs", "owner", &["rhea"], &["rhea"], &[]);
        a.carried_from = Some(x);
        a.carried_rule = Some("rebased".into());
        let origin = |from, carried, differs: &[&str], reason: Option<&str>| OriginStatus {
            from,
            reviews: vec![
                OriginReview {
                    reviewer: "rhea".into(),
                    verdict: Verdict::Approve,
                },
                OriginReview {
                    reviewer: "bo".into(),
                    verdict: Verdict::Reject,
                },
            ],
            carried,
            differs: differs.iter().map(|p| RepoPath::new(*p).unwrap()).collect(),
            reason: reason.map(String::from),
        };
        let resp = ReviewStatusResponse {
            changeset: id(),
            target_ref: "main".into(),
            approved: false,
            paths: vec![a, path("b.rs", "pending", &["rhea"], &[], &[])],
            on_refs: vec![],
            origins: vec![
                origin(x, true, &[], None),
                origin(x2, false, &["b.rs"], None),
                origin(x2, false, &[], Some("it is a merge")),
            ],
        };
        let (y, sx, sx2) = (short(id()), short(x), short(x2));
        assert_eq!(
            status_lines(&resp)[1..],
            [
                format!(
                    "  ok       a.rs  approved by rhea (owner); carried from {sx} (rebased onto the new base)"
                ),
                "  PENDING  b.rs  needs one of: rhea".to_string(),
                format!("replaces {sx}, approved by rhea, rejected by bo; carried"),
                format!(
                    "replaces {sx2}, approved by rhea, rejected by bo; not carried: b.rs differs \
                     (leep diff {sx2}; leep diff {y})"
                ),
                format!(
                    "replaces {sx2}, approved by rhea, rejected by bo; not carried: it is a merge \
                     (leep diff {sx2}; leep diff {y})"
                ),
                format!("diff: leep diff {y}"),
            ]
        );
    }

    #[test]
    fn waiting_backs_off_to_a_minute_and_zero_means_no_limit() {
        let b = Backoff::waiting(0);
        assert_eq!(b.limit, None);
        assert_eq!(
            (b.first, b.max),
            (Duration::from_secs(2), Duration::from_secs(60))
        );
        assert_eq!(Backoff::waiting(90).limit, Some(Duration::from_secs(90)));
    }
}
