//! `leep review show`: one view of a change under review, so a reviewer
//! does not piece it together from `review inbox`, `show`, `review status`
//! and `diff`.

use clap::Args;
use leep_core::api::{
    ChangesetInfo, DiffResponse, PathReview, ReviewRequestEntry, ReviewStatusResponse,
};
use serde::Serialize;

use super::review::{path_state, status_lines};
use super::review_request::entry_lines;
use super::{print_json, Remote};
use crate::error::{ErrorKind, Result};
use crate::fmt::{plural, utc_time};

#[derive(Args, Debug)]
pub struct ShowArgs {
    /// A review request (`#N`, or `N` when such a request exists) or a
    /// changeset: id, ref name, or `@` for the working copy base.
    pub id: String,
    /// Ref to judge against (default: the request's ref, else the target
    /// ref).
    #[arg(long = "ref")]
    pub target_ref: Option<String>,
    /// Also print the whole diff.
    #[arg(long)]
    pub diff: bool,
}

/// Lines added and removed in one file; `None` for a binary file.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct FileStat {
    pub path: String,
    pub added: Option<usize>,
    pub removed: Option<usize>,
}

/// `--json`: the parts, each as its own command prints it.
#[derive(Serialize)]
struct Out<'a> {
    changeset: &'a ChangesetInfo,
    requests: &'a [ReviewRequestEntry],
    status: &'a ReviewStatusResponse,
    stat: &'a [FileStat],
    #[serde(skip_serializing_if = "Option::is_none")]
    diff: Option<String>,
}

/// A request number: `#N`, or digits short enough not to be meant as a
/// changeset prefix first.
fn request_number(id: &str) -> Option<(u64, bool)> {
    if let Some(n) = id.strip_prefix('#') {
        return n.parse().ok().map(|n| (n, true));
    }
    (id.len() <= 6 && !id.is_empty() && id.bytes().all(|b| b.is_ascii_digit()))
        .then(|| id.parse().ok().map(|n| (n, false)))
        .flatten()
}

pub fn show(r: &Remote, args: &ShowArgs) -> Result<i32> {
    let asked = match request_number(&args.id) {
        Some((n, sure)) => match r.client.review_request(n) {
            Ok(e) => Some(e),
            Err(e) if !sure && e.kind() == ErrorKind::NotFound => None,
            Err(e) => return Err(e),
        },
        None => None,
    };
    let id = match &asked {
        Some(e) => e.request.changeset,
        None => r.resolve(&args.id)?,
    };
    // Every request for the changeset, the asked one first. An older
    // server without requests has none.
    let mut requests: Vec<ReviewRequestEntry> = asked.into_iter().collect();
    if let Ok(all) = r.client.review_requests(&[("changeset", id.to_string())]) {
        for e in all.requests {
            if !requests.iter().any(|q| q.request.id == e.request.id) {
                requests.push(e);
            }
        }
    }
    let target = args
        .target_ref
        .clone()
        .or_else(|| requests.first().map(|e| e.request.target_ref.clone()))
        .unwrap_or_else(|| r.target_ref.to_string());
    let info = r.client.get_changeset(&id)?;
    let st = r.client.review_status(&id, &target)?;
    let parent = info.changeset.parents.first().copied();
    let resp = r.client.diff(parent.as_ref(), &id, None)?;
    let stat = file_stats(r, &resp)?;
    let diff = if args.diff {
        Some(super::diff::diff_text(r, &resp)?)
    } else {
        None
    };
    if r.json {
        print_json(&Out {
            changeset: &info,
            requests: &requests,
            status: &st,
            stat: &stat,
            diff,
        })?;
        return Ok(0);
    }
    let mut out = show_lines(&info, &requests, &st, &stat).join("\n");
    out.push('\n');
    if let Some(d) = diff {
        out.push('\n');
        out.push_str(&crate::term::diff(&d));
    }
    crate::term::page(&out, false)?;
    Ok(0)
}

/// Count each entry's lines with the same diff `leep diff` prints.
fn file_stats(r: &Remote, resp: &DiffResponse) -> Result<Vec<FileStat>> {
    let mut out = Vec::with_capacity(resp.entries.len());
    for e in &resp.entries {
        let fetch = |f: &Option<leep_core::api::FileInfo>| -> Result<Option<(Vec<u8>, _)>> {
            match f {
                Some(f) => Ok(Some((r.cache.fetch(r.client, &f.content)?, f.kind))),
                None => Ok(None),
            }
        };
        let (old, new) = (fetch(&e.old)?, fetch(&e.new)?);
        let text = crate::fmt::file_diff(e.path.as_str(), side(&old), side(&new));
        out.push(count(e.path.as_str(), &text));
    }
    Ok(out)
}

fn side(c: &Option<(Vec<u8>, leep_core::FileKind)>) -> Option<(&[u8], leep_core::FileKind)> {
    c.as_ref().map(|(d, k)| (d.as_slice(), *k))
}

/// `+` and `-` lines of one file's unified diff; a diff without hunks
/// that is not empty names a binary file.
pub fn count(path: &str, text: &str) -> FileStat {
    let (mut added, mut removed, mut hunks) = (0, 0, false);
    for line in text.lines() {
        if line.starts_with("@@") {
            hunks = true;
        } else if line.starts_with("+++ ") || line.starts_with("--- ") {
        } else if line.starts_with('+') {
            added += 1;
        } else if line.starts_with('-') {
            removed += 1;
        }
    }
    let binary = !hunks && text.lines().any(|l| l.starts_with("Binary"));
    FileStat {
        path: path.to_string(),
        added: (!binary).then_some(added),
        removed: (!binary).then_some(removed),
    }
}

/// What a path's status adds in `review show`: where the owners come from
/// is in [`path_state`]; here whether only an identical change carries a
/// verdict to it, and why.
fn carry_note(p: &PathReview) -> String {
    match (p.carry.as_deref(), p.carry_source.as_deref()) {
        (Some("identical"), Some(src)) => format!("; identical-only ({src})"),
        (Some("identical"), None) => "; identical-only".into(),
        _ => String::new(),
    }
}

/// The human view: changeset header and message, requests, per-path
/// status with the diff stat beside each file, how to see the diff.
pub fn show_lines(
    info: &ChangesetInfo,
    requests: &[ReviewRequestEntry],
    st: &ReviewStatusResponse,
    stat: &[FileStat],
) -> Vec<String> {
    let cs = &info.changeset;
    let mut out = vec![format!("changeset {}", info.id)];
    let mut by = format!("Author: {}", cs.author);
    if let Some(c) = &info.created_by {
        by.push_str(&format!(" (created by {})", c.principal));
    }
    out.push(by);
    out.push(format!("Date:   {}", utc_time(cs.timestamp)));
    out.push(String::new());
    for line in cs.message.lines() {
        out.push(if line.is_empty() {
            String::new()
        } else {
            format!("    {line}")
        });
    }
    out.push(String::new());
    if requests.is_empty() {
        out.push("No review requests (`leep review request` asks for one).".into());
    }
    for e in requests {
        out.extend(entry_lines(e, None));
    }
    out.push(String::new());
    // The status as `review status` says it, with each path's stat and
    // carry rule beside it.
    let lines = status_lines(st);
    out.push(lines[0].clone());
    let width = st
        .paths
        .iter()
        .map(|p| p.path.as_str().chars().count())
        .max()
        .unwrap_or(0);
    for p in &st.paths {
        let (state, why) = path_state(p);
        let delta = stat
            .iter()
            .find(|s| s.path == p.path.as_str())
            .map(stat_word)
            .unwrap_or_default();
        out.push(format!(
            "  {state:<8} {:<width$}  {delta:<11} {why}{}",
            p.path.as_str(),
            carry_note(p)
        ));
    }
    if st.paths.is_empty() {
        out.push("  (no changed paths)".into());
    }
    // The origin, refusal and diff lines of `review status`.
    out.extend(lines.into_iter().skip(1 + st.paths.len().max(1)));
    let (added, removed): (usize, usize) = stat
        .iter()
        .map(|s| (s.added.unwrap_or(0), s.removed.unwrap_or(0)))
        .fold((0, 0), |a, b| (a.0 + b.0, a.1 + b.1));
    out.push(format!(
        "{} changed, {added} insertion{}(+), {removed} deletion{}(-); `leep review show {} --diff` shows it",
        plural(stat.len(), "file"),
        if added == 1 { "" } else { "s" },
        if removed == 1 { "" } else { "s" },
        crate::fmt::short(info.id)
    ));
    out
}

fn stat_word(s: &FileStat) -> String {
    match (s.added, s.removed) {
        (Some(a), Some(r)) => format!("+{a} -{r}"),
        _ => "binary".into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_numbers_are_hash_or_short_digits() {
        assert_eq!(request_number("#12"), Some((12, true)));
        assert_eq!(request_number("12"), Some((12, false)));
        assert_eq!(request_number("1234567"), None);
        assert_eq!(request_number("abc"), None);
        assert_eq!(request_number("#x"), None);
        assert_eq!(request_number(""), None);
    }

    #[test]
    fn counts_lines_and_binaries() {
        let text = "--- a/x\n+++ b/x\n@@ -1,2 +1,2 @@\n-a\n+b\n+c\n d\n";
        let s = count("x", text);
        assert_eq!((s.added, s.removed), (Some(2), Some(1)));
        let s = count("x", "Binary files a/x and b/x differ\n");
        assert_eq!((s.added, s.removed), (None, None));
        assert_eq!(stat_word(&s), "binary");
    }
}
