//! Read-only tools.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Write;

use leep_core::api::{ChangesetInfo, FileInfo, LogSummaryEntry, RenameEntry};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::{ChangesetId, FileKind, RepoPath};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use similar::TextDiff;

use super::{
    as_text, parse_args, short, truncate, ToolError, ToolOutput, ToolResult, DEFAULT_REF,
    MAX_DIFF_FILE_BYTES, MAX_TEXT_BYTES,
};
use crate::Server;

const DEFAULT_LOG_LIMIT: u32 = 20;
const MAX_LOG_LIMIT: u32 = 200;
const MAX_LISTED: usize = 5000;
const MAX_BLAME_LINES: usize = 5000;
const DEFAULT_SEARCH_LIMIT: u32 = 50;
const MAX_SEARCH_LIMIT: u32 = 200;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LogArgs {
    rev: Option<String>,
    path: Option<String>,
    follow: Option<bool>,
    limit: Option<u32>,
    author: Option<String>,
    agent_model: Option<String>,
    agent_session: Option<String>,
    cursor: Option<String>,
    fresh: Option<bool>,
    /// Whole messages and all of `extra`, from full log entries.
    full: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ShowArgs {
    rev: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListArgs {
    rev: Option<String>,
    prefix: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct PathArgs {
    rev: Option<String>,
    path: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct BlameArgs {
    rev: Option<String>,
    path: String,
    fresh: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SearchArgs {
    query: String,
    rev: Option<String>,
    regex: Option<bool>,
    case_insensitive: Option<bool>,
    path: Option<String>,
    glob: Option<String>,
    limit: Option<u32>,
    cursor: Option<String>,
    fresh: Option<bool>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReviewStatusArgs {
    rev: String,
    #[serde(rename = "ref")]
    target_ref: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct DiffArgs {
    from: Option<String>,
    to: String,
    prefix: Option<String>,
    renames: Option<bool>,
    rename_threshold: Option<u8>,
}

/// Changeset summary with provenance split out of `extra`.
fn summary(info: &ChangesetInfo) -> Value {
    let cs = &info.changeset;
    let mut agent = Map::new();
    let mut extra = Map::new();
    for (k, v) in &cs.extra {
        match k.strip_prefix("agent.") {
            Some(name) => agent.insert(name.to_string(), json!(v)),
            None => extra.insert(k.clone(), json!(v)),
        };
    }
    let mut v = json!({
        "id": info.id.to_string(),
        "parents": cs.parents.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
        "author": cs.author,
        "timestamp": cs.timestamp,
        "message": cs.message,
        "files_changed": cs.changes.len(),
    });
    if !agent.is_empty() {
        v["agent"] = Value::Object(agent);
    }
    if !extra.is_empty() {
        v["extra"] = Value::Object(extra);
    }
    follow_fields(&mut v, info.path_at.as_ref(), info.renamed.as_ref());
    v
}

/// A log entry: [`summary`]'s shape from a summary entry, so `message` is
/// the first line and only `agent.*` metadata is kept (`leep_show` has the
/// rest).
fn log_entry(e: &LogSummaryEntry) -> Value {
    let agent: Map<String, Value> = e
        .agent
        .iter()
        .filter_map(|(k, v)| Some((k.strip_prefix("agent.")?.to_string(), json!(v))))
        .collect();
    let mut v = json!({
        "id": e.id.to_string(),
        "parents": e.parents.iter().map(|p| p.to_string()).collect::<Vec<_>>(),
        "author": e.author,
        "timestamp": e.timestamp,
        "message": e.message_first_line,
        "files_changed": e.files_changed,
    });
    if !agent.is_empty() {
        v["agent"] = Value::Object(agent);
    }
    follow_fields(&mut v, e.path_at.as_ref(), e.renamed.as_ref());
    v
}

/// What a `follow` log adds to an entry.
fn follow_fields(v: &mut Value, path_at: Option<&RepoPath>, renamed: Option<&RenameEntry>) {
    if let Some(p) = path_at {
        v["path_at"] = json!(p.as_str());
    }
    if let Some(r) = renamed {
        v["renamed"] = json!({
            "from": r.from.as_str(),
            "to": r.to.as_str(),
            "similarity": r.similarity,
            "exact": r.exact,
        });
    }
}

/// Add `answered_at` and `behind` when the answer is for an older changeset
/// than asked (ADR 0058).
fn mark_stale(v: &mut Value, answered_at: Option<ChangesetId>, behind: Option<u64>) {
    if let Some(at) = answered_at {
        v["answered_at"] = json!(at.to_string());
        v["behind"] = json!(behind.unwrap_or(0));
    }
}

fn file_json(f: &FileInfo) -> Value {
    json!({ "path": f.path.to_string(), "size": f.size, "kind": f.kind })
}

impl Server {
    /// The client for an index-backed query: `stale-ok` by default, so an
    /// agent gets a slightly older answer now; with `fresh`, `retry`, with
    /// one retry within 5 s (ADR 0058).
    fn index_client(&self, fresh: bool) -> leep_cli::client::Client {
        if fresh {
            self.client
                .clone()
                .with_stale_ok(false)
                .with_index_retries(std::time::Duration::from_secs(5), 1)
        } else {
            self.client.clone().with_stale_ok(true)
        }
    }

    pub(super) fn refs(&self) -> ToolResult {
        let refs = self.client.refs()?.refs;
        let map: BTreeMap<String, String> =
            refs.into_iter().map(|(k, v)| (k, v.to_string())).collect();
        Ok(ToolOutput::json(json!({ "refs": map })))
    }

    pub(super) fn log(&self, args: Value) -> ToolResult {
        let a: LogArgs = parse_args(args)?;
        let id = self.resolve_rev(a.rev.as_deref())?;
        let limit = a.limit.unwrap_or(DEFAULT_LOG_LIMIT).clamp(1, MAX_LOG_LIMIT);
        let non_empty = |v: Option<String>| v.filter(|v| !v.is_empty());
        let mut extra = Vec::new();
        if let Some(m) = non_empty(a.agent_model) {
            extra.push(format!("{EXTRA_AGENT_MODEL}:{m}"));
        }
        if let Some(sess) = non_empty(a.agent_session) {
            extra.push(format!("{EXTRA_AGENT_SESSION}:{sess}"));
        }
        let path = non_empty(a.path);
        let follow = a.follow.unwrap_or(false);
        if follow && path.is_none() {
            return Err(ToolError::new("follow needs `path`, the file to follow"));
        }
        let query = leep_cli::client::LogQuery {
            path,
            limit: Some(limit),
            author: non_empty(a.author),
            extra,
            follow,
            after: non_empty(a.cursor),
        };
        let client = self.index_client(a.fresh.unwrap_or(false));
        let (entries, next, answered_at, behind): (Vec<Value>, _, _, _) = if a.full.unwrap_or(false)
        {
            let log = client.log(&id, &query)?;
            let entries = log.entries.iter().map(summary).collect();
            (entries, log.next, log.answered_at, log.behind)
        } else {
            let log = client.log_summary(&id, &query)?;
            let entries = log.entries.iter().map(log_entry).collect();
            (entries, log.next, log.answered_at, log.behind)
        };
        let mut v = json!({ "from": id.to_string(), "entries": entries });
        mark_stale(&mut v, answered_at, behind);
        if let Some(next) = next {
            v["next"] = json!(next);
            v["note"] = json!(
                "More history follows. For the next page, call again with the same arguments, \
                 `rev` set to `from`, and `cursor` set to `next`."
            );
        }
        Ok(ToolOutput::json(v))
    }

    pub(super) fn show(&self, args: Value) -> ToolResult {
        let a: ShowArgs = parse_args(args)?;
        let id = self.resolve_rev(Some(&a.rev))?;
        let info = self.client.get_changeset(&id)?;
        let parent = info.changeset.parents.first();
        let diff = self.client.diff(parent, &id, None)?;
        let mut changes = Vec::new();
        for e in diff.entries.iter().take(MAX_LISTED) {
            let status = match (&e.old, &e.new) {
                (None, Some(_)) => "A",
                (Some(_), None) => "D",
                _ => "M",
            };
            changes.push(json!({ "status": status, "path": e.path.to_string() }));
        }
        let mut v = summary(&info);
        v["changes"] = Value::Array(changes);
        if diff.entries.len() > MAX_LISTED {
            v["changes_truncated"] = json!(true);
        }
        Ok(ToolOutput::json(v))
    }

    pub(super) fn list_files(&self, args: Value) -> ToolResult {
        let a: ListArgs = parse_args(args)?;
        let id = self.resolve_rev(a.rev.as_deref())?;
        let prefix = a.prefix.as_deref().unwrap_or("").trim_matches('/');
        let files = self.client.files(&id, prefix)?.files;
        let total = files.len();
        let listed: Vec<Value> = files.iter().take(MAX_LISTED).map(file_json).collect();
        let mut v = json!({ "rev": id.to_string(), "total": total, "files": listed });
        if total > MAX_LISTED {
            v["truncated"] = json!(true);
            v["note"] = json!(format!(
                "Showing the first {MAX_LISTED} of {total} files; narrow with `prefix`."
            ));
        }
        Ok(ToolOutput::json(v))
    }

    pub(super) fn read_file(&self, args: Value) -> ToolResult {
        let a: PathArgs = parse_args(args)?;
        let id = self.resolve_rev(a.rev.as_deref())?;
        let (bytes, kind) = self.client.file(&id, &a.path)?;
        let Some(text) = as_text(&bytes) else {
            return Ok(ToolOutput::text(format!(
                "{} is a binary file ({} bytes); its content is not returned.",
                a.path,
                bytes.len()
            )));
        };
        let (shown, cut) = truncate(text, MAX_TEXT_BYTES);
        let mut out = shown.to_string();
        if kind == Some(FileKind::Symlink) {
            out = format!("[symlink to] {out}");
        }
        if cut {
            let _ = write!(
                out,
                "\n\n[truncated: showed the first {} of {} bytes; the cap is {} KiB]",
                shown.len(),
                bytes.len(),
                MAX_TEXT_BYTES / 1024
            );
        }
        Ok(ToolOutput::text(out))
    }

    pub(super) fn diff(&self, args: Value) -> ToolResult {
        let a: DiffArgs = parse_args(args)?;
        let to = self.resolve_rev(Some(&a.to))?;
        let from: Option<ChangesetId> = match a.from.as_deref().filter(|f| !f.is_empty()) {
            Some(f) => Some(self.resolve_rev(Some(f))?),
            None => self
                .client
                .get_changeset(&to)?
                .changeset
                .parents
                .first()
                .copied(),
        };
        let prefix = a
            .prefix
            .as_deref()
            .map(|p| p.trim_matches('/'))
            .filter(|p| !p.is_empty());
        let resp = if a.renames.unwrap_or(false) || a.rename_threshold.is_some() {
            self.client
                .diff_renames(from.as_ref(), &to, prefix, a.rename_threshold)?
        } else {
            self.client.diff(from.as_ref(), &to, prefix)?
        };
        let entries = resp.entries;
        if entries.is_empty() {
            return Ok(ToolOutput::text("No differences."));
        }
        let mut out = String::new();
        let mut cut = false;
        for e in &entries {
            if out.len() >= MAX_TEXT_BYTES {
                cut = true;
                break;
            }
            let path = e.path.as_str();
            // A rename replaces its delete and add, shown at the new path.
            if e.new.is_none() && resp.renames.iter().any(|r| r.from == e.path) {
                continue;
            }
            let moved = resp
                .renames
                .iter()
                .find(|r| r.to == e.path && e.old.is_none());
            let old_file = match moved {
                Some(r) => entries
                    .iter()
                    .find(|d| d.path == r.from)
                    .and_then(|d| d.old.clone()),
                None => e.old.clone(),
            };
            let old_path = moved.map_or(path, |r| r.from.as_str());
            let _ = writeln!(out, "diff --leep a/{old_path} b/{path}");
            if let Some(r) = moved {
                let _ = writeln!(
                    out,
                    "rename from {}\nrename to {path}\nsimilarity index {}%",
                    r.from, r.similarity
                );
            }
            if moved.is_some_and(|r| r.exact) {
                continue;
            }
            let too_big = [&old_file, &e.new]
                .into_iter()
                .flatten()
                .any(|f| f.size > MAX_DIFF_FILE_BYTES);
            if too_big {
                out.push_str("(file too large to diff)\n");
                continue;
            }
            let old = self.side(old_file.as_ref())?;
            let new = self.side(e.new.as_ref())?;
            let (Some(old), Some(new)) = (old, new) else {
                out.push_str("Binary files differ\n");
                continue;
            };
            let (from_name, to_name) = (
                if old_file.is_some() {
                    format!("a/{old_path}")
                } else {
                    "/dev/null".into()
                },
                if e.new.is_some() {
                    format!("b/{path}")
                } else {
                    "/dev/null".into()
                },
            );
            let d = TextDiff::from_lines(old.as_str(), new.as_str());
            out.push_str(
                &d.unified_diff()
                    .context_radius(3)
                    .header(&from_name, &to_name)
                    .to_string(),
            );
        }
        if cut {
            out.push_str("\n[diff truncated; narrow with `prefix`]\n");
        }
        Ok(ToolOutput::text(out))
    }

    /// File text for one side of a diff: empty if absent, `None` if binary.
    fn side(&self, f: Option<&FileInfo>) -> Result<Option<String>, ToolError> {
        let Some(f) = f else {
            return Ok(Some(String::new()));
        };
        let bytes = self.blob(&f.content)?;
        Ok(as_text(&bytes).map(str::to_string))
    }

    pub(super) fn search(&self, args: Value) -> ToolResult {
        let a: SearchArgs = parse_args(args)?;
        if a.query.is_empty() {
            return Err(ToolError::new("query must not be empty"));
        }
        let id = self.resolve_rev(a.rev.as_deref())?;
        let non_empty = |v: Option<String>| v.filter(|v| !v.is_empty());
        let query = leep_cli::client::SearchQuery {
            query: a.query,
            regex: a.regex.unwrap_or(false),
            ignore_case: a.case_insensitive.unwrap_or(false),
            path: non_empty(a.path),
            glob: non_empty(a.glob),
            limit: Some(
                a.limit
                    .unwrap_or(DEFAULT_SEARCH_LIMIT)
                    .clamp(1, MAX_SEARCH_LIMIT),
            ),
            after: non_empty(a.cursor),
        };
        let resp = self
            .index_client(a.fresh.unwrap_or(false))
            .search(&id, &query)?;
        let mut v = json!({
            "rev": id.to_string(),
            "matches": resp.matches,
            "truncated": resp.truncated,
            "stats": resp.stats,
        });
        mark_stale(&mut v, resp.answered_at, resp.behind);
        if let Some(next) = resp.next {
            v["next"] = json!(next);
            v["note"] = json!(
                "More matches may follow. Narrow the query with `path` or `glob`, or call again \
                 with the same arguments, `rev` set to `rev`, and `cursor` set to `next`."
            );
        } else if resp.truncated {
            v["note"] = json!(
                "More matches exist, or the server stopped scanning. Narrow the query with \
                 `path` or `glob`, or raise `limit`."
            );
        }
        Ok(ToolOutput::json(v))
    }

    pub(super) fn review_status(&self, args: Value) -> ToolResult {
        let a: ReviewStatusArgs = parse_args(args)?;
        let id = self.resolve_rev(Some(&a.rev))?;
        let target = a
            .target_ref
            .as_deref()
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .unwrap_or(DEFAULT_REF);
        let status = self.client.review_status(&id, target)?;
        let blocked: Vec<&str> = status
            .paths
            .iter()
            .filter(|p| p.rule == "pending" || p.rule == "rejected")
            .map(|p| p.path.as_str())
            .collect();
        let mut v = serde_json::to_value(&status).map_err(|e| ToolError(e.to_string()))?;
        v["blocked_paths"] = json!(blocked);
        if !status.approved {
            v["next_step"] = json!(format!(
                "It cannot land on {target} yet. Owners of the blocked paths must approve it \
                 (`leep review approve {}`); a path's owners are listed per path.",
                short(&id)
            ));
        }
        Ok(ToolOutput::json(v))
    }

    pub(super) fn blame(&self, args: Value) -> ToolResult {
        let a: BlameArgs = parse_args(args)?;
        let id = self.resolve_rev(a.rev.as_deref())?;
        let blame = self
            .index_client(a.fresh.unwrap_or(false))
            .blame(&id, &a.path)?;
        let mut authors: BTreeMap<ChangesetId, String> = BTreeMap::new();
        let mut out = String::new();
        for (n, line) in blame.lines.iter().enumerate() {
            if n >= MAX_BLAME_LINES {
                let _ = writeln!(
                    out,
                    "[truncated: {} lines shown of {}]",
                    MAX_BLAME_LINES,
                    blame.lines.len()
                );
                break;
            }
            if let Entry::Vacant(e) = authors.entry(line.changeset) {
                e.insert(self.client.get_changeset(&line.changeset)?.changeset.author);
            }
            let _ = writeln!(
                out,
                "{} {} {}: {}",
                short(&line.changeset),
                authors[&line.changeset],
                n + 1,
                line.text.trim_end_matches(['\n', '\r'])
            );
        }
        let mut header = match blame.answered_at {
            Some(at) => format!(
                "blame of {} at {} (answered_at {}, {} behind the requested {}; changeset, \
                 author, line: text)\n",
                a.path,
                short(&at),
                at,
                blame.behind.unwrap_or(0),
                short(&id)
            ),
            None => format!(
                "blame of {} at {} (changeset, author, line: text)\n",
                a.path,
                short(&id)
            ),
        };
        header.push_str(&out);
        Ok(ToolOutput::text(header))
    }
}
