//! Read-only queries: log, last-change, show, cat, blame, provenance, refs.

use std::io::Write;

use clap::Args;
use leep_core::api::{BlameResponse, ChangesetInfo, ProvenanceResponse, SignaturesResponse};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};

use super::{print_json, Remote};
use crate::client::LogQuery;
use crate::error::{CliError, Result};
use crate::fmt::{rename_line, short, utc_time};

#[derive(Args, Debug)]
pub struct LogArgs {
    /// Starting revision (default: working copy base, else target ref).
    pub rev: Option<String>,
    /// Only changesets touching this file or directory (answered from the
    /// server's path-history index, so cost does not grow with history).
    #[arg(long)]
    pub path: Option<String>,
    /// Like `--path` for one file, but keep going under the file's old name
    /// when it was renamed (found by similarity, as in `leep diff`).
    #[arg(long, value_name = "FILE", conflicts_with = "path")]
    pub follow: Option<String>,
    /// Maximum number of entries.
    #[arg(short = 'n', long, default_value_t = 20)]
    pub limit: u32,
    /// Only changesets whose author contains this text (case-insensitive).
    #[arg(long)]
    pub author: Option<String>,
    /// Only changesets made by an agent (have `agent.model`).
    #[arg(long)]
    pub agent: bool,
    /// Only changesets made by this agent model.
    #[arg(long, value_name = "MODEL")]
    pub agent_model: Option<String>,
    /// Only changesets from this agent session.
    #[arg(long, value_name = "SESSION")]
    pub agent_session: Option<String>,
    /// Only changesets with metadata `KEY=VALUE` (or just `KEY` for any
    /// value); repeatable, all must match.
    #[arg(long, value_name = "KEY[=VALUE]")]
    pub extra: Vec<String>,
}

impl LogArgs {
    /// The server query these flags stand for. All filters are ANDed. Without
    /// `--path`, filtering scans history linearly on the server.
    pub fn query(&self) -> Result<LogQuery> {
        let mut extra = Vec::new();
        if self.agent {
            extra.push(EXTRA_AGENT_MODEL.to_string());
        }
        if let Some(m) = &self.agent_model {
            extra.push(format!("{EXTRA_AGENT_MODEL}:{m}"));
        }
        if let Some(s) = &self.agent_session {
            extra.push(format!("{EXTRA_AGENT_SESSION}:{s}"));
        }
        for e in &self.extra {
            extra.push(match e.split_once('=') {
                Some(("", _)) => {
                    return Err(CliError::user(format!("bad --extra {e:?}: empty key")))
                }
                Some((k, v)) => format!("{k}:{v}"),
                None if e.is_empty() => return Err(CliError::user("bad --extra: empty key")),
                None => e.clone(),
            });
        }
        Ok(LogQuery {
            path: self.follow.clone().or_else(|| self.path.clone()),
            limit: Some(self.limit),
            author: self.author.clone(),
            extra,
            follow: self.follow.is_some(),
        })
    }
}

#[derive(Args, Debug)]
pub struct ProvenanceArgs {
    /// Starting revision (default: working copy base, else target ref).
    pub rev: Option<String>,
    /// Only count changesets touching this path or directory.
    #[arg(long)]
    pub path: Option<String>,
    /// Maximum number of changesets to scan (server default and cap: 10000).
    #[arg(short = 'n', long)]
    pub limit: Option<u32>,
}

#[derive(Args, Debug)]
pub struct LastChangeArgs {
    /// File or directory, repository-relative.
    pub path: String,
    /// Revision to look from (default: working copy base, else target ref).
    #[arg(long)]
    pub rev: Option<String>,
}

#[derive(Args, Debug)]
pub struct ShowArgs {
    /// Ref or tag name, changeset id (full, or 4+ leading hex digits), or `@`.
    pub rev: String,
}

#[derive(Args, Debug)]
pub struct CatArgs {
    /// Ref or tag name, changeset id (full, or 4+ leading hex digits), or `@`.
    pub rev: String,
    /// Repository-relative path.
    pub path: String,
}

#[derive(Args, Debug)]
pub struct BlameArgs {
    /// `[REV] PATH`
    #[arg(num_args = 1..=2, value_name = "[REV] PATH")]
    pub args: Vec<String>,
    /// Annotate each line with its author and agent model instead of the
    /// changeset id alone.
    #[arg(long)]
    pub who: bool,
}

/// `abbreviate` prints the 12-digit short id, which every command accepts
/// back; `--json` always has the full id.
fn header(info: &ChangesetInfo, abbreviate: bool) {
    if abbreviate {
        println!("changeset {}", short(info.id));
    } else {
        println!("changeset {}", info.id);
    }
    println!("Author: {}", info.changeset.author);
    println!("Date:   {}", utc_time(info.changeset.timestamp));
    for (k, v) in &info.changeset.extra {
        println!("{k}: {v}");
    }
    for line in follow_lines(info) {
        println!("{line}");
    }
    println!();
    for line in info.changeset.message.lines() {
        println!("    {line}");
    }
}

/// What `log --follow` adds to an entry: the file's name there, and the
/// rename that gave it that name.
fn follow_lines(info: &ChangesetInfo) -> Vec<String> {
    let mut out = Vec::new();
    if let Some(p) = &info.path_at {
        out.push(format!("File:   {p}"));
    }
    if let Some(r) = &info.renamed {
        out.push(rename_line(r.from.as_str(), r.to.as_str(), r.similarity));
    }
    out
}

pub fn log(r: &Remote, args: &LogArgs) -> Result<i32> {
    let id = r.resolve_default(args.rev.as_deref())?;
    let resp = r.client.log(&id, &args.query()?)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for info in &resp.entries {
        header(info, true);
        println!();
    }
    Ok(0)
}

pub fn last_change(r: &Remote, args: &LastChangeArgs) -> Result<i32> {
    let id = r.resolve_default(args.rev.as_deref())?;
    let resp = r.client.last_change(&id, &args.path)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    header(&resp.changeset, false);
    for c in &resp.concurrent {
        println!("also changed concurrently in {c}");
    }
    Ok(0)
}

pub fn show(r: &Remote, args: &ShowArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let info = r.client.get_changeset(&id)?;
    if r.json {
        print_json(&info)?;
        return Ok(0);
    }
    for p in &info.changeset.parents {
        println!("parent {p}");
    }
    header(&info, false);
    // Older servers lack the endpoint; a changeset is still worth showing.
    if let Ok(sigs) = r.client.signatures(&id) {
        for line in signature_lines(&sigs) {
            println!("{line}");
        }
    }
    println!();
    // Ask the server so additions and modifications are told apart.
    let parent = info.changeset.parents.first().copied();
    for e in r.client.diff(parent.as_ref(), &id, None)?.entries {
        let letter = match (&e.old, &e.new) {
            (None, Some(_)) => 'A',
            (Some(_), None) => 'D',
            _ => 'M',
        };
        println!("{letter} {}", e.path);
    }
    Ok(0)
}

/// `Signed-by` lines for `leep show`, one per trusted signer.
pub fn signature_lines(resp: &SignaturesResponse) -> Vec<String> {
    let mut out: Vec<String> = resp
        .signatures
        .iter()
        .map(|s| format!("Signed-by: {} (key {})", s.name, s.key_id))
        .collect();
    if let (Some(via), false) = (resp.via, out.is_empty()) {
        out.push(format!("Signed-via: rebased from {via}"));
    }
    if !resp.verified {
        out.push("Signed-warning: a stored signature failed to verify".into());
    }
    out
}

pub fn cat(r: &Remote, args: &CatArgs) -> Result<i32> {
    let id = r.resolve(&args.rev)?;
    let (data, _) = r.client.file(&id, &args.path)?;
    let mut out = std::io::stdout().lock();
    out.write_all(&data)?;
    out.flush()?;
    Ok(0)
}

pub fn blame(r: &Remote, args: &BlameArgs) -> Result<i32> {
    let (rev, path) = match args.args.as_slice() {
        [p] => (None, p),
        [rev, p] => (Some(rev.as_str()), p),
        _ => return Err(CliError::user("usage: leep blame [REV] PATH")),
    };
    let id = r.resolve_default(rev)?;
    let resp = r.client.blame(&id, path)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for line in blame_lines(&resp, args.who) {
        println!("{line}");
    }
    Ok(0)
}

/// Blame output rows; with `who`, author and `agent.model` (`-` for humans)
/// are aligned between the id and the line number.
pub fn blame_lines(resp: &BlameResponse, who: bool) -> Vec<String> {
    let who_cols = |l: &leep_core::api::BlameLine| {
        let s = resp.changesets.get(&l.changeset);
        let author = s.map_or("?", |s| s.author.as_str());
        let model = s
            .and_then(|s| s.agent.get(EXTRA_AGENT_MODEL))
            .map_or("-", String::as_str);
        (author.to_string(), model.to_string())
    };
    let (mut aw, mut mw) = (0, 0);
    if who {
        for l in &resp.lines {
            let (a, m) = who_cols(l);
            aw = aw.max(a.chars().count());
            mw = mw.max(m.chars().count());
        }
    }
    resp.lines
        .iter()
        .enumerate()
        .map(|(i, l)| {
            if who {
                let (a, m) = who_cols(l);
                format!(
                    "{} {a:<aw$} {m:<mw$} {:>4} | {}",
                    short(l.changeset),
                    i + 1,
                    l.text
                )
            } else {
                format!("{} {:>4} | {}", short(l.changeset), i + 1, l.text)
            }
        })
        .collect()
}

pub fn provenance(r: &Remote, args: &ProvenanceArgs) -> Result<i32> {
    let id = r.resolve_default(args.rev.as_deref())?;
    let resp = r.client.provenance(&id, args.path.as_deref(), args.limit)?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    print!("{}", provenance_table(&resp));
    Ok(0)
}

fn table(header: &[&str], rows: &[Vec<String>]) -> String {
    let widths: Vec<usize> = (0..header.len())
        .map(|c| {
            rows.iter()
                .map(|r| r[c].chars().count())
                .chain([header[c].len()])
                .max()
                .unwrap_or(0)
        })
        .collect();
    let line = |cells: Vec<&str>| {
        let cols: Vec<String> = cells
            .iter()
            .zip(&widths)
            .enumerate()
            .map(|(i, (c, w))| {
                // Counts right-aligned, everything else left.
                if matches!(header[i], "CHANGESETS" | "FILES") {
                    format!("{c:>w$}")
                } else {
                    format!("{c:<w$}")
                }
            })
            .collect();
        format!("{}\n", cols.join("  ").trim_end())
    };
    let mut out = line(header.to_vec());
    for r in rows {
        out.push_str(&line(r.iter().map(String::as_str).collect()));
    }
    out
}

/// Agents and humans as two small tables, plus a note if the scan was cut off.
pub fn provenance_table(p: &ProvenanceResponse) -> String {
    let mut out = String::new();
    if p.agents.is_empty() && p.humans.is_empty() {
        out.push_str("no matching changesets\n");
    }
    let stats = |c: u64, f: u64, first: i64, last: i64| {
        [
            c.to_string(),
            f.to_string(),
            utc_time(first),
            utc_time(last),
        ]
    };
    if !p.agents.is_empty() {
        let rows: Vec<Vec<String>> = p
            .agents
            .iter()
            .map(|a| {
                let mut r = vec![
                    a.model.clone(),
                    a.session.clone().unwrap_or_else(|| "-".into()),
                ];
                r.extend(stats(a.changesets, a.files_touched, a.first, a.last));
                r
            })
            .collect();
        out.push_str(&table(
            &[
                "AGENT MODEL",
                "SESSION",
                "CHANGESETS",
                "FILES",
                "FIRST",
                "LAST",
            ],
            &rows,
        ));
    }
    if !p.humans.is_empty() {
        if !out.is_empty() {
            out.push('\n');
        }
        let rows: Vec<Vec<String>> = p
            .humans
            .iter()
            .map(|h| {
                let mut r = vec![h.author.clone()];
                r.extend(stats(h.changesets, h.files_touched, h.first, h.last));
                r
            })
            .collect();
        out.push_str(&table(
            &["AUTHOR", "CHANGESETS", "FILES", "FIRST", "LAST"],
            &rows,
        ));
    }
    out.push_str(&format!(
        "\nscanned {}{}\n",
        crate::fmt::plural(p.scanned as usize, "changeset"),
        if p.truncated {
            " (truncated: older history not counted; use --limit)"
        } else {
            ""
        }
    ));
    out
}

pub fn refs(r: &Remote) -> Result<i32> {
    let resp = r.client.refs()?;
    if r.json {
        print_json(&resp)?;
        return Ok(0);
    }
    for (name, id) in &resp.refs {
        println!("{name} {id}");
    }
    Ok(0)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use leep_core::api::{AgentStat, BlameLine, ChangesetSummary, HumanStat};
    use leep_core::{ChangesetId, Hash, RepoPath};

    use super::*;

    fn log_args() -> LogArgs {
        LogArgs {
            rev: None,
            path: None,
            follow: None,
            limit: 20,
            author: None,
            agent: false,
            agent_model: None,
            agent_session: None,
            extra: vec![],
        }
    }

    #[test]
    fn log_flags_map_to_query_params() {
        assert_eq!(
            log_args().query().unwrap().params(),
            [("limit", "20".to_string())]
        );
        let q = LogArgs {
            path: Some("src".into()),
            author: Some("ann".into()),
            agent: true,
            agent_model: Some("claude-x".into()),
            agent_session: Some("42".into()),
            extra: vec!["ci=green".into(), "reviewed".into(), "k=a=b".into()],
            ..log_args()
        }
        .query()
        .unwrap();
        let p = q.params();
        let extras: Vec<_> = p
            .iter()
            .filter(|(k, _)| *k == "extra")
            .map(|(_, v)| v.as_str())
            .collect();
        assert_eq!(
            extras,
            [
                "agent.model",
                "agent.model:claude-x",
                "agent.session:42",
                "ci:green",
                "reviewed",
                "k:a=b"
            ]
        );
        assert!(p.contains(&("path", "src".into())));
        assert!(p.contains(&("author", "ann".into())));
        assert!(p.contains(&("limit", "20".into())));
    }

    #[test]
    fn follow_asks_for_one_file_with_the_follow_flag() {
        let q = LogArgs {
            follow: Some("src/a.rs".into()),
            ..log_args()
        }
        .query()
        .unwrap();
        assert!(q.follow);
        let p = q.params();
        assert!(p.contains(&("path", "src/a.rs".into())));
        assert!(p.contains(&("follow", "true".into())));
        // Plain log never sends it.
        assert!(!log_args()
            .query()
            .unwrap()
            .params()
            .iter()
            .any(|(k, _)| *k == "follow"));
    }

    #[test]
    fn follow_entries_show_the_name_and_the_rename() {
        let id = cid(1);
        let mut info = ChangesetInfo {
            id,
            changeset: leep_core::Changeset {
                parents: vec![],
                author: "a".into(),
                timestamp: 0,
                message: "m".into(),
                changes: BTreeMap::new(),
                extra: BTreeMap::new(),
            },
            generation: 1,
            root_tree: Hash::of_blob(b""),
            path_at: None,
            renamed: None,
        };
        assert!(follow_lines(&info).is_empty());
        info.path_at = Some(RepoPath::new("b.txt").unwrap());
        info.renamed = Some(leep_core::api::RenameEntry {
            from: RepoPath::new("a.txt").unwrap(),
            to: RepoPath::new("b.txt").unwrap(),
            similarity: 91,
            exact: false,
        });
        assert_eq!(
            follow_lines(&info),
            ["File:   b.txt", "renamed: a.txt -> b.txt (91%)"]
        );
    }

    #[test]
    fn empty_extra_key_is_a_user_error() {
        for bad in ["=x", ""] {
            let a = LogArgs {
                extra: vec![bad.into()],
                ..log_args()
            };
            assert!(a.query().is_err(), "{bad:?}");
        }
    }

    fn cid(n: u8) -> ChangesetId {
        ChangesetId(Hash::of_blob(&[n]))
    }

    #[test]
    fn blame_who_annotates_author_and_model() {
        let (human, bot) = (cid(1), cid(2));
        let line = |c, t: &str| BlameLine {
            changeset: c,
            origin_line: 1,
            text: t.into(),
        };
        let summary = |author: &str, model: Option<&str>| ChangesetSummary {
            author: author.into(),
            timestamp: 0,
            message_first_line: String::new(),
            agent: model
                .map(|m| BTreeMap::from([(EXTRA_AGENT_MODEL.to_string(), m.to_string())]))
                .unwrap_or_default(),
        };
        let resp = BlameResponse {
            path: RepoPath::new("f").unwrap(),
            lines: vec![line(human, "a"), line(bot, "b")],
            changesets: BTreeMap::from([
                (human, summary("ann", None)),
                (bot, summary("robot", Some("m1"))),
            ]),
        };
        let plain = blame_lines(&resp, false);
        assert_eq!(plain[0], format!("{}    1 | a", short(human)));
        let who = blame_lines(&resp, true);
        assert_eq!(who[0], format!("{} ann   -     1 | a", short(human)));
        assert_eq!(who[1], format!("{} robot m1    2 | b", short(bot)));
        // A server without summaries degrades to placeholders.
        let bare = BlameResponse {
            changesets: BTreeMap::new(),
            ..resp
        };
        assert!(blame_lines(&bare, true)[0].contains(" ? - "));
    }

    #[test]
    fn provenance_table_lists_agents_humans_and_truncation() {
        let p = ProvenanceResponse {
            agents: vec![AgentStat {
                model: "m1".into(),
                session: Some("s1".into()),
                changesets: 12,
                files_touched: 3,
                first: 0,
                last: 86_400,
            }],
            humans: vec![HumanStat {
                author: "ann".into(),
                changesets: 1,
                files_touched: 1,
                first: 5,
                last: 5,
            }],
            scanned: 13,
            truncated: true,
        };
        let t = provenance_table(&p);
        let lines: Vec<&str> = t.lines().collect();
        assert_eq!(
            lines[0],
            "AGENT MODEL  SESSION  CHANGESETS  FILES  FIRST                LAST"
        );
        assert_eq!(
            lines[1],
            "m1           s1               12      3  1970-01-01 00:00:00  1970-01-02 00:00:00"
        );
        assert!(lines[3].starts_with("AUTHOR"));
        assert!(lines[4].starts_with("ann"));
        assert!(t.ends_with(
            "scanned 13 changesets (truncated: older history not counted; use --limit)\n"
        ));
        let empty = ProvenanceResponse {
            agents: vec![],
            humans: vec![],
            scanned: 0,
            truncated: false,
        };
        assert_eq!(
            provenance_table(&empty),
            "no matching changesets\n\nscanned 0 changesets\n"
        );
    }
}
