//! `leep review request`, `leep review inbox` and `leep review cancel`:
//! ask for a review through the server and see what waits for you
//! (ADR 0086).

use std::time::Instant;

use clap::Args;
use leep_core::api::{
    CreateReviewRequest, ReviewRequestEntry, ReviewRequestState, ReviewRequestsResponse,
};
use leep_core::outln;

use super::review::Backoff;
use super::{print_json, Remote};
use crate::error::{exit, CliError, ErrorKind, Result};
use crate::fmt::{plural, short, utc_time};

#[derive(Args, Debug)]
pub struct RequestArgs {
    /// Changeset to review: id, ref name, or `@` for the working copy base.
    pub rev: String,
    /// Reviewer to ask, by token name; repeat for more. Default: the owners
    /// of the paths still waiting for approval, else anyone who may review.
    #[arg(long = "to", value_name = "NAME")]
    pub to: Vec<String>,
    /// A note for the reviewers.
    #[arg(short = 'm', long)]
    pub message: Option<String>,
    /// Ref the changeset is meant to land on (default: the target ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
    /// Then wait for the verdict, as `leep review status --wait[=SECS]`.
    #[arg(
        long,
        value_name = "SECS",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "3600",
        value_parser = crate::cli::parse_secs
    )]
    pub wait: Option<u64>,
}

#[derive(Args, Debug)]
pub struct InboxArgs {
    /// Requests you made, instead of the ones waiting for you.
    #[arg(long)]
    pub mine: bool,
    /// Include closed requests (landed or withdrawn) and, without --mine,
    /// the ones you already approved or rejected.
    #[arg(long)]
    pub all: bool,
    /// Show at most this many (default 50).
    #[arg(short = 'n', long, value_name = "N")]
    pub limit: Option<usize>,
    /// Wait until something is listed, up to SECS (default 3600; 0 means
    /// no limit): exit 0 with the list, or 7 (nothing yet) when the time
    /// runs out. `--wait SECS` works too.
    #[arg(
        long,
        value_name = "SECS",
        num_args = 0..=1,
        require_equals = true,
        default_missing_value = "3600",
        value_parser = crate::cli::parse_secs
    )]
    pub wait: Option<u64>,
}

#[derive(Args, Debug)]
pub struct CancelArgs {
    /// The request's number, as `leep review inbox --mine` shows it.
    pub id: u64,
}

pub fn request(r: &Remote, args: &RequestArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let target = args.target_ref.as_deref().unwrap_or(r.target_ref);
    let resp = r
        .client
        .request_review(&CreateReviewRequest {
            changeset: id,
            target_ref: target.to_string(),
            reviewers: args.to.clone(),
            message: args.message.clone().unwrap_or_default(),
        })
        .map_err(unsupported)?;
    let q = &resp.request.request;
    if r.json {
        // One JSON document per run: with --wait, the verdict's.
        if args.wait.is_none() {
            print_json(&resp)?;
        }
    } else {
        for line in requested_lines(&resp.request, resp.existing, args.wait.is_some()) {
            outln!("{line}");
        }
    }
    match args.wait {
        Some(secs) => {
            super::review::wait(r, id, &q.target_ref, &super::review::Backoff::waiting(secs))
        }
        None => Ok(0),
    }
}

/// What `leep review request` says: the request, then (unless it is about
/// to wait) how to follow it up.
pub fn requested_lines(e: &ReviewRequestEntry, existing: bool, waiting: bool) -> Vec<String> {
    let q = &e.request;
    let verb = if existing {
        "Already requested"
    } else {
        "Requested"
    };
    let mut out = vec![format!(
        "{verb} review of {} on {} from {} (#{})",
        short(q.changeset),
        q.target_ref,
        who_reviews(&q.reviewers),
        q.id
    )];
    if !waiting {
        out.push(format!(
            "`leep review status {} --ref {} --wait` waits for the verdict; \
             `leep review cancel {}` withdraws the request.",
            short(q.changeset),
            q.target_ref,
            q.id
        ));
    }
    out
}

pub fn inbox(r: &Remote, args: &InboxArgs) -> Result<i32> {
    let me = r
        .client
        .whoami()?
        .name
        .ok_or_else(|| CliError::user("review requests need a token: set LEEP_TOKEN"))?;
    let (resp, hidden, code) = match args.wait {
        None => {
            let (resp, hidden) = inbox_page(r, args, &me)?;
            (resp, hidden, exit::OK)
        }
        Some(secs) => wait_for_inbox(r, args, &me, &Backoff::waiting(secs))?,
    };
    if r.json {
        print_json(&resp)?;
        return Ok(code);
    }
    if resp.requests.is_empty() {
        outln!(
            "{}",
            match (args.mine, args.all) {
                (true, false) => "You have no open review requests.",
                (true, true) => "You have made no review requests.",
                (false, false) => "No review requests are waiting for you.",
                (false, true) => "No review requests name you.",
            }
        );
    }
    for e in &resp.requests {
        for line in entry_lines(e, Some(&me)) {
            outln!("{line}");
        }
    }
    if hidden > 0 {
        outln!(
            "({} you already answered not shown; `leep review inbox --all` shows them)",
            plural(hidden, "request")
        );
    }
    if resp.next.is_some() {
        outln!("(more requests follow; run again with -n larger)");
    }
    Ok(code)
}

/// One page of the inbox, without the requests `me` already answered
/// (unless `--mine` or `--all`), and how many those were.
fn inbox_page(r: &Remote, args: &InboxArgs, me: &str) -> Result<(ReviewRequestsResponse, usize)> {
    waiting_requests(
        r.client,
        me,
        args.mine,
        args.all,
        args.limit.unwrap_or(DEFAULT_INBOX),
    )
    .map_err(unsupported)
}

/// Requests listed per inbox when no limit is given.
pub const DEFAULT_INBOX: usize = 50;
/// Pages an inbox reads looking for requests not yet answered.
const INBOX_PAGES: usize = 10;

/// Up to `limit` review requests for `me`: open ones naming it (`mine`:
/// that it made; `all`: closed ones too), newest first. Unless `mine` or
/// `all`, the requests `me` already answered are left out, and further
/// pages are read (at most [`INBOX_PAGES`]) so that answered ones do not
/// hide older unanswered ones. `next` is set when more may follow. Answers
/// the list and how many answered requests were left out.
pub fn waiting_requests(
    client: &crate::client::Client,
    me: &str,
    mine: bool,
    all: bool,
    limit: usize,
) -> Result<(ReviewRequestsResponse, usize)> {
    let limit = limit.max(1);
    let mut base = vec![(if mine { "requester" } else { "reviewer" }, me.to_string())];
    if !all {
        base.push(("open", "true".into()));
    }
    base.push(("limit", limit.to_string()));
    let filter = !mine && !all;
    let mut out = ReviewRequestsResponse {
        requests: Vec::new(),
        next: None,
    };
    let mut hidden = 0;
    let mut before: Option<u64> = None;
    for _ in 0..INBOX_PAGES {
        let mut query = base.clone();
        if let Some(b) = before {
            query.push(("before", b.to_string()));
        }
        let page = client.review_requests(&query)?;
        for e in page.requests {
            // What waits for you is what you have not answered yet. A
            // withdrawn verdict is no answer, so the request comes back.
            if filter && answered_by(&e, me) {
                hidden += 1;
            } else if out.requests.len() < limit {
                out.requests.push(e);
            } else {
                // A whole page was not needed: more may follow from here.
                out.next = out.requests.last().map(|e| e.request.id);
                return Ok((out, hidden));
            }
        }
        out.next = page.next;
        if out.requests.len() >= limit || !filter {
            break;
        }
        match page.next {
            Some(n) => before = Some(n),
            None => break,
        }
    }
    Ok((out, hidden))
}

/// `--wait`: ask until a page lists something (exit 0) or the limit passes
/// (exit 7, nothing yet, with the last, empty, page), backing off as `review status
/// --wait` does.
fn wait_for_inbox(
    r: &Remote,
    args: &InboxArgs,
    me: &str,
    b: &Backoff,
) -> Result<(ReviewRequestsResponse, usize, i32)> {
    let start = Instant::now();
    let mut delay = b.first;
    let mut said = false;
    loop {
        let (resp, hidden) = inbox_page(r, args, me)?;
        if !resp.requests.is_empty() {
            return Ok((resp, hidden, exit::OK));
        }
        let left = b.limit.map(|l| l.saturating_sub(start.elapsed()));
        if left == Some(std::time::Duration::ZERO) {
            eprintln!(
                "timed out after {}s waiting for review requests",
                start.elapsed().as_secs()
            );
            return Ok((resp, hidden, exit::NOTHING_YET));
        }
        if !said {
            said = true;
            eprintln!(
                "waiting for review requests{}...",
                match b.limit {
                    Some(l) => format!(" (up to {}s)", l.as_secs()),
                    None => String::new(),
                }
            );
        }
        std::thread::sleep(left.map_or(delay, |l| l.min(delay)));
        delay = (delay * 2).min(b.max);
    }
}

/// Whether `me` has approved or rejected the request's changeset.
pub fn answered_by(e: &ReviewRequestEntry, me: &str) -> bool {
    e.approved_by.iter().chain(&e.rejected_by).any(|n| n == me)
}

pub fn cancel(r: &Remote, args: &CancelArgs) -> Result<i32> {
    let e = r
        .client
        .withdraw_review_request(args.id)
        .map_err(unsupported)?;
    if r.json {
        print_json(&e)?;
        return Ok(0);
    }
    match e.request.state {
        ReviewRequestState::Withdrawn => outln!("Withdrew review request #{}", e.request.id),
        s => outln!("Review request #{} is already {}", e.request.id, s.as_str()),
    }
    Ok(0)
}

/// Most paths an inbox entry names before `(+N)`.
const ENTRY_FILES: usize = 5;

/// `a.rs, b.rs (+3)`: the first paths a request's changeset changes and how
/// many more. `None` from an older server, which sends no paths.
fn files_line(paths: &[leep_core::RepoPath], changed: usize) -> Option<String> {
    if paths.is_empty() {
        return None;
    }
    let shown: Vec<&str> = paths.iter().take(ENTRY_FILES).map(|p| p.as_str()).collect();
    let more = changed.max(paths.len()) - shown.len();
    let mut line = shown.join(", ");
    if more > 0 {
        line.push_str(&format!(" (+{more})"));
    }
    Some(line)
}

fn who_reviews(reviewers: &[String]) -> String {
    if reviewers.is_empty() {
        "any reviewer".into()
    } else {
        reviewers.join(", ")
    }
}

/// An older server has no `/v1/review-requests`.
fn unsupported(e: CliError) -> CliError {
    if e.kind() == ErrorKind::NotFound && e.server_code().is_none() {
        return CliError::user("this server does not take review requests; upgrade it (ADR 0086)");
    }
    e
}

/// How `leep review inbox` shows one request: a header line, then the
/// changeset's title and the requester's note, indented. `me` is named
/// "you" among those who answered.
pub fn entry_lines(e: &ReviewRequestEntry, me: Option<&str>) -> Vec<String> {
    let q = &e.request;
    let mut head = format!(
        "#{} {} {} -> {} by {}",
        q.id,
        q.state.as_str(),
        short(q.changeset),
        q.target_ref,
        q.requester
    );
    if let Some(c) = e.created_by.as_ref().filter(|c| c.principal != q.requester) {
        head.push_str(&format!(" (created by {})", c.principal));
    }
    // ADR 0086 asks the pending paths' owners; none were asked when no
    // pending path had one.
    let whom = if q.reviewers.is_empty() {
        "any reviewer, no owner pending".to_string()
    } else {
        q.reviewers.join(", ")
    };
    head.push_str(&format!(", for {whom} ({})", utc_time(q.created)));
    if let Some(at) = q.landed_as.filter(|l| *l != q.changeset) {
        head.push_str(&format!(", landed as {}", short(at)));
    }
    let names = |list: &[String]| {
        list.iter()
            .map(|n| if Some(n.as_str()) == me { "you" } else { n })
            .collect::<Vec<_>>()
            .join(", ")
    };
    for (word, list) in [("approved", &e.approved_by), ("rejected", &e.rejected_by)] {
        if !list.is_empty() {
            head.push_str(&format!(", {word} by {}", names(list)));
        }
    }
    let mut out = vec![head];
    if let Some(t) = e.title.as_deref().filter(|t| !t.is_empty()) {
        out.push(format!("    {t}"));
    }
    if let Some(files) = files_line(&e.paths, e.changed) {
        out.push(format!("    files: {files}"));
    }
    for line in q.message.lines() {
        out.push(format!("    > {line}"));
    }
    out
}

#[cfg(test)]
mod tests {
    use leep_core::api::{CreatedBy, ReviewRequest};
    use leep_core::{ChangesetId, Hash};

    use super::*;

    #[test]
    fn entries_show_requester_creator_reviewers_and_note() {
        let c = ChangesetId(Hash::of_blob(b"c"));
        let mut e = ReviewRequestEntry {
            request: ReviewRequest {
                id: 7,
                changeset: c,
                target_ref: "main".into(),
                requester: "ada".into(),
                reviewers: vec!["bo".into(), "rhea".into()],
                message: "api change\nsee ADR".into(),
                created: 0,
                state: ReviewRequestState::Open,
                closed: None,
                landed_as: None,
            },
            created_by: Some(CreatedBy {
                principal: "ada".into(),
                timestamp: 0,
            }),
            title: Some("core: add a field".into()),
            approved_by: Default::default(),
            rejected_by: Default::default(),
            paths: vec![],
            changed: 0,
        };
        let lines = entry_lines(&e, Some("bo"));
        assert_eq!(
            lines,
            [
                format!(
                    "#7 open {} -> main by ada, for bo, rhea ({})",
                    short(c),
                    utc_time(0)
                ),
                "    core: add a field".to_string(),
                "    > api change".to_string(),
                "    > see ADR".to_string(),
            ]
        );
        e.created_by.as_mut().unwrap().principal = "cy".into();
        e.request.reviewers.clear();
        let copy = ChangesetId(Hash::of_blob(b"copy"));
        e.request.state = ReviewRequestState::Landed;
        e.request.landed_as = Some(copy);
        assert_eq!(
            entry_lines(&e, None)[0],
            format!(
                "#7 landed {} -> main by ada (created by cy), for any reviewer, no owner pending ({}), landed as {}",
                short(c),
                utc_time(0),
                short(copy)
            )
        );
    }

    #[test]
    fn entries_say_who_answered_and_name_the_caller_you() {
        let c = ChangesetId(Hash::of_blob(b"c"));
        let e = ReviewRequestEntry {
            request: ReviewRequest {
                id: 3,
                changeset: c,
                target_ref: "main".into(),
                requester: "ada".into(),
                reviewers: vec!["rhea".into()],
                message: String::new(),
                created: 0,
                state: ReviewRequestState::Open,
                closed: None,
                landed_as: None,
            },
            created_by: None,
            title: None,
            approved_by: vec!["rhea".into()],
            rejected_by: vec!["bo".into()],
            paths: vec![],
            changed: 0,
        };
        let base = format!(
            "#3 open {} -> main by ada, for rhea ({})",
            short(c),
            utc_time(0)
        );
        // The requester's view (--mine) names the reviewers.
        assert_eq!(
            entry_lines(&e, Some("ada")),
            [format!("{base}, approved by rhea, rejected by bo")]
        );
        // The reviewer's view (--all) says "you".
        assert_eq!(
            entry_lines(&e, Some("rhea")),
            [format!("{base}, approved by you, rejected by bo")]
        );
        assert!(answered_by(&e, "rhea") && answered_by(&e, "bo"));
        assert!(!answered_by(&e, "ada"));
    }

    #[test]
    fn a_request_says_how_to_follow_it_up() {
        let c = ChangesetId(Hash::of_blob(b"c"));
        let e = ReviewRequestEntry {
            request: ReviewRequest {
                id: 4,
                changeset: c,
                target_ref: "main".into(),
                requester: "ada".into(),
                reviewers: vec![],
                message: String::new(),
                created: 0,
                state: ReviewRequestState::Open,
                closed: None,
                landed_as: None,
            },
            created_by: None,
            title: None,
            approved_by: vec![],
            rejected_by: vec![],
            paths: vec![],
            changed: 0,
        };
        let s = short(c);
        assert_eq!(
            requested_lines(&e, false, false),
            [
                format!("Requested review of {s} on main from any reviewer (#4)"),
                format!(
                    "`leep review status {s} --ref main --wait` waits for the verdict; \
                     `leep review cancel 4` withdraws the request."
                ),
            ]
        );
        // About to wait: no advice to do what it is doing.
        assert_eq!(
            requested_lines(&e, true, true),
            [format!(
                "Already requested review of {s} on main from any reviewer (#4)"
            )]
        );
    }

    #[test]
    fn entries_name_the_first_files_and_how_many_more() {
        let p = |s: &str| leep_core::RepoPath::new(s).unwrap();
        assert_eq!(files_line(&[], 4), None);
        assert_eq!(files_line(&[p("a.rs")], 1).as_deref(), Some("a.rs"));
        let many: Vec<_> = (0..16).map(|i| p(&format!("f{i:02}"))).collect();
        assert_eq!(
            files_line(&many, 40).as_deref(),
            Some("f00, f01, f02, f03, f04 (+35)")
        );
        let c = ChangesetId(Hash::of_blob(b"c"));
        let e = ReviewRequestEntry {
            request: ReviewRequest {
                id: 9,
                changeset: c,
                target_ref: "main".into(),
                requester: "ada".into(),
                reviewers: vec!["rhea".into()],
                message: String::new(),
                created: 0,
                state: ReviewRequestState::Open,
                closed: None,
                landed_as: None,
            },
            created_by: None,
            title: Some("core: x".into()),
            approved_by: vec![],
            rejected_by: vec![],
            paths: vec![p("a.rs"), p("b.rs")],
            changed: 5,
        };
        assert_eq!(
            entry_lines(&e, None)[1..],
            ["    core: x", "    files: a.rs, b.rs (+3)"]
        );
    }
}
