//! Read tools over refs and history: the ref log, rev resolution, merge
//! bases and provenance. Outputs are compact, since every byte lands in the
//! agent's context.

use leep_cli::error::ErrorKind;
use leep_core::prefix::looks_like_prefix;
use leep_core::ChangesetId;
use serde::Deserialize;
use serde_json::{json, Map, Value};

use super::{describe, parse_args, short, ToolError, ToolOutput, ToolResult, DEFAULT_REF};
use crate::Server;

const DEFAULT_REFLOG_LIMIT: usize = 20;
const MAX_REFLOG_LIMIT: usize = 200;
const DEFAULT_PROVENANCE_SCAN: u32 = 1000;
const MAX_PROVENANCE_SCAN: u32 = 10_000;
const DEFAULT_PROVENANCE_TOP: usize = 10;
const MAX_PROVENANCE_TOP: usize = 100;

#[derive(Deserialize)]
struct ReflogArgs {
    #[serde(rename = "ref")]
    name: Option<String>,
    all: Option<bool>,
    limit: Option<usize>,
    cursor: Option<String>,
    full_ids: Option<bool>,
}

#[derive(Deserialize)]
struct ResolveArgs {
    rev: String,
}

#[derive(Deserialize)]
struct MergeBaseArgs {
    a: String,
    b: String,
}

#[derive(Deserialize)]
struct ProvenanceArgs {
    rev: Option<String>,
    path: Option<String>,
    limit: Option<u32>,
    top: Option<usize>,
}

fn non_empty(v: Option<String>) -> Option<String> {
    v.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

impl Server {
    pub(super) fn reflog(&self, args: Value) -> ToolResult {
        let a: ReflogArgs = parse_args(args)?;
        let all = a.all.unwrap_or(false);
        let name = non_empty(a.name);
        if all && name.is_some() {
            return Err(ToolError::new("give either ref or all:true, not both"));
        }
        let name = if all {
            None
        } else {
            Some(name.unwrap_or_else(|| DEFAULT_REF.to_string()))
        };
        let limit = a
            .limit
            .unwrap_or(DEFAULT_REFLOG_LIMIT)
            .clamp(1, MAX_REFLOG_LIMIT);
        let cursor = non_empty(a.cursor);
        let full = a.full_ids.unwrap_or(false);
        let id = |c: &ChangesetId| {
            if full {
                c.to_string()
            } else {
                short(c)
            }
        };
        let page = self
            .client
            .ref_log_page(name.as_deref(), None, cursor.as_deref(), limit)?;
        let entries: Vec<Value> = page
            .entries
            .iter()
            .map(|e| {
                let mut v = Map::new();
                v.insert("seq".into(), json!(e.seq));
                if name.is_none() {
                    v.insert("ref".into(), json!(e.name));
                }
                v.insert("op".into(), json!(e.operation));
                v.insert("old".into(), json!(e.old.as_ref().map(id)));
                v.insert("new".into(), json!(e.new.as_ref().map(id)));
                v.insert("time".into(), json!(e.time));
                if let Some(actor) = &e.actor {
                    v.insert("actor".into(), json!(actor));
                }
                if let Some(m) = e.message.as_deref().filter(|m| !m.is_empty()) {
                    v.insert("message".into(), json!(m));
                }
                if let Some(l) = &e.land_id {
                    v.insert("land_id".into(), json!(l.to_string()));
                }
                // The changeset the land asked for, when the ref took a
                // rebased copy of it.
                if let Some(c) = e.land_changeset.filter(|c| Some(*c) != e.new) {
                    v.insert("land_changeset".into(), json!(id(&c)));
                }
                Value::Object(v)
            })
            .collect();
        let mut v = json!({ "entries": entries });
        if let Some(n) = &name {
            v["ref"] = json!(n);
        }
        if !full {
            v["ids"] = json!("12-digit prefixes; every rev argument accepts them");
        }
        if let Some(next) = page.next {
            v["next"] = json!(next);
            v["note"] = json!(
                "Older entries follow: call again with the same arguments and `cursor` set to \
                 `next`."
            );
        }
        Ok(ToolOutput::json(v))
    }

    pub(super) fn resolve(&self, args: Value) -> ToolResult {
        let a: ResolveArgs = parse_args(args)?;
        let rev = a.rev.trim();
        if rev.is_empty() {
            return Err(ToolError::new("rev must not be empty"));
        }
        let fail = |e: leep_cli::error::CliError| {
            ToolError(format!(
                "cannot resolve `{rev}`: {}. Use a ref name from leep_refs, a changeset id, or \
                 a unique prefix of one (at least 4 hex digits).",
                describe(&e)
            ))
        };
        let (id, via) = match self.client.resolve(rev) {
            Ok(id) if id.to_string() == rev.to_ascii_lowercase() => (id, "id"),
            Ok(id) => (id, "ref"),
            Err(e) if e.kind() == ErrorKind::NotFound && looks_like_prefix(rev) => {
                match self.client.resolve_prefix(rev) {
                    Ok(id) => (id, "prefix"),
                    Err(p) if p.kind() == ErrorKind::NotFound => return Err(fail(e)),
                    Err(p) => return Err(fail(p)),
                }
            }
            Err(e) => return Err(fail(e)),
        };
        Ok(ToolOutput::json(json!({
            "rev": rev,
            "id": id.to_string(),
            "via": via,
        })))
    }

    pub(super) fn merge_base(&self, args: Value) -> ToolResult {
        let a: MergeBaseArgs = parse_args(args)?;
        let x = self.resolve_rev(Some(&a.a))?;
        let y = self.resolve_rev(Some(&a.b))?;
        let base = self.client.merge_base(&x, &y)?.base;
        let mut v = json!({
            "a": x.to_string(),
            "b": y.to_string(),
            "base": base.map(|b| b.to_string()),
            "a_is_ancestor_of_b": base == Some(x),
            "b_is_ancestor_of_a": base == Some(y),
        });
        if base.is_none() {
            v["note"] = json!("No common ancestor: the histories are unrelated.");
        }
        Ok(ToolOutput::json(v))
    }

    pub(super) fn provenance(&self, args: Value) -> ToolResult {
        let a: ProvenanceArgs = parse_args(args)?;
        let id = self.resolve_rev(a.rev.as_deref())?;
        let path = non_empty(a.path)
            .map(|p| p.trim_matches('/').to_string())
            .filter(|p| !p.is_empty());
        let scan = a
            .limit
            .unwrap_or(DEFAULT_PROVENANCE_SCAN)
            .clamp(1, MAX_PROVENANCE_SCAN);
        let top = a
            .top
            .unwrap_or(DEFAULT_PROVENANCE_TOP)
            .clamp(1, MAX_PROVENANCE_TOP);
        let p = self.client.provenance(&id, path.as_deref(), Some(scan))?;
        let agent_changesets: u64 = p.agents.iter().map(|s| s.changesets).sum();
        let human_changesets: u64 = p.humans.iter().map(|s| s.changesets).sum();
        let agents: Vec<Value> = p
            .agents
            .iter()
            .take(top)
            .map(|s| {
                let mut v = json!({
                    "model": s.model,
                    "changesets": s.changesets,
                    "files_touched": s.files_touched,
                    "first": s.first,
                    "last": s.last,
                });
                if let Some(sess) = &s.session {
                    v["session"] = json!(sess);
                }
                v
            })
            .collect();
        let humans: Vec<Value> = p
            .humans
            .iter()
            .take(top)
            .map(|s| {
                json!({
                    "author": s.author,
                    "changesets": s.changesets,
                    "files_touched": s.files_touched,
                    "first": s.first,
                    "last": s.last,
                })
            })
            .collect();
        let mut v = json!({
            "rev": id.to_string(),
            "scanned": p.scanned,
            "truncated": p.truncated,
            "agent_changesets": agent_changesets,
            "human_changesets": human_changesets,
            "agents": agents,
            "humans": humans,
        });
        if let Some(path) = path {
            v["path"] = json!(path);
        }
        if p.agents.len() > top {
            v["agents_omitted"] = json!(p.agents.len() - top);
        }
        if p.humans.len() > top {
            v["humans_omitted"] = json!(p.humans.len() - top);
        }
        if p.truncated {
            v["note"] = json!(format!(
                "Counted the newest {} changesets only; raise `limit` (max {MAX_PROVENANCE_SCAN}) \
                 to scan more.",
                p.scanned
            ));
        }
        Ok(ToolOutput::json(v))
    }
}
