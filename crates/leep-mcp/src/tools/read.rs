//! Read-only tools.

use std::collections::btree_map::Entry;
use std::collections::BTreeMap;
use std::fmt::Write;

use leep_core::api::{ChangesetInfo, FileInfo};
use leep_core::changeset::{EXTRA_AGENT_MODEL, EXTRA_AGENT_SESSION};
use leep_core::{ChangesetId, FileKind};
use serde::Deserialize;
use serde_json::{json, Map, Value};
use similar::TextDiff;

use super::{
    as_text, parse_args, short, truncate, ToolError, ToolOutput, ToolResult, MAX_DIFF_FILE_BYTES,
    MAX_TEXT_BYTES,
};
use crate::Server;

const DEFAULT_LOG_LIMIT: u32 = 20;
const MAX_LOG_LIMIT: u32 = 200;
const MAX_LISTED: usize = 5000;
const MAX_BLAME_LINES: usize = 5000;

#[derive(Deserialize)]
struct LogArgs {
    rev: Option<String>,
    path: Option<String>,
    limit: Option<u32>,
    author: Option<String>,
    agent_model: Option<String>,
    agent_session: Option<String>,
}

#[derive(Deserialize)]
struct ShowArgs {
    rev: String,
}

#[derive(Deserialize)]
struct ListArgs {
    rev: Option<String>,
    prefix: Option<String>,
}

#[derive(Deserialize)]
struct PathArgs {
    rev: Option<String>,
    path: String,
}

#[derive(Deserialize)]
struct DiffArgs {
    from: Option<String>,
    to: String,
    prefix: Option<String>,
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
    v
}

fn file_json(f: &FileInfo) -> Value {
    json!({ "path": f.path.to_string(), "size": f.size, "kind": f.kind })
}

impl Server {
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
        let query = leep_cli::client::LogQuery {
            path: non_empty(a.path),
            limit: Some(limit),
            author: non_empty(a.author),
            extra,
        };
        let log = self.client.log(&id, &query)?;
        let entries: Vec<Value> = log.entries.iter().map(summary).collect();
        Ok(ToolOutput::json(
            json!({ "from": id.to_string(), "entries": entries }),
        ))
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
        let entries = self.client.diff(from.as_ref(), &to, prefix)?.entries;
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
            let _ = writeln!(out, "diff --leep a/{path} b/{path}");
            let too_big = [&e.old, &e.new]
                .into_iter()
                .flatten()
                .any(|f| f.size > MAX_DIFF_FILE_BYTES);
            if too_big {
                out.push_str("(file too large to diff)\n");
                continue;
            }
            let old = self.side(e.old.as_ref())?;
            let new = self.side(e.new.as_ref())?;
            let (Some(old), Some(new)) = (old, new) else {
                out.push_str("Binary files differ\n");
                continue;
            };
            let (from_name, to_name) = (
                if e.old.is_some() {
                    format!("a/{path}")
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

    pub(super) fn blame(&self, args: Value) -> ToolResult {
        let a: PathArgs = parse_args(args)?;
        let id = self.resolve_rev(a.rev.as_deref())?;
        let blame = self.client.blame(&id, &a.path)?;
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
        let mut header = format!(
            "blame of {} at {} (changeset, author, line: text)\n",
            a.path,
            short(&id)
        );
        header.push_str(&out);
        Ok(ToolOutput::text(header))
    }
}
