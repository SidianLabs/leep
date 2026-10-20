//! Tool definitions and dispatch. Failures are returned as tool results with
//! `isError: true`, never as protocol errors, so the agent can read and act
//! on them.

mod commit;
mod read;

use leep_cli::error::{CliError, ErrorKind};
use leep_core::prefix::looks_like_prefix;
use leep_core::ChangesetId;
use serde::de::DeserializeOwned;
use serde_json::{json, Value};

use crate::Server;

/// Largest text returned for a single file or blob, in bytes.
pub const MAX_TEXT_BYTES: usize = 256 * 1024;
/// Largest file the diff tool will fetch, per side.
pub const MAX_DIFF_FILE_BYTES: u64 = 1024 * 1024;
pub(super) const DEFAULT_REF: &str = "main";

#[derive(Debug)]
pub struct ToolError(pub String);

impl From<CliError> for ToolError {
    fn from(e: CliError) -> Self {
        ToolError(e.to_string())
    }
}

impl From<leep_core::Error> for ToolError {
    fn from(e: leep_core::Error) -> Self {
        ToolError(e.to_string())
    }
}

impl ToolError {
    pub fn new(m: impl Into<String>) -> Self {
        ToolError(m.into())
    }
}

pub type ToolResult = Result<ToolOutput, ToolError>;

pub struct ToolOutput {
    pub text: String,
    pub structured: Option<Value>,
    pub is_error: bool,
}

impl ToolOutput {
    pub fn text(text: impl Into<String>) -> Self {
        ToolOutput {
            text: text.into(),
            structured: None,
            is_error: false,
        }
    }

    pub fn error(text: impl Into<String>) -> Self {
        ToolOutput {
            text: text.into(),
            structured: None,
            is_error: true,
        }
    }

    /// Structured data, also serialized as text for clients that ignore
    /// `structuredContent`.
    pub fn json(v: Value) -> Self {
        ToolOutput {
            text: serde_json::to_string_pretty(&v).unwrap_or_default(),
            structured: Some(v),
            is_error: false,
        }
    }

    pub fn into_json(self) -> Value {
        let mut v = json!({
            "content": [{ "type": "text", "text": self.text }],
            "isError": self.is_error,
        });
        if let Some(s) = self.structured {
            v["structuredContent"] = s;
        }
        v
    }
}

pub fn parse_args<T: DeserializeOwned>(args: Value) -> Result<T, ToolError> {
    let args = if args.is_null() { json!({}) } else { args };
    serde_json::from_value(args).map_err(|e| ToolError(format!("invalid arguments: {e}")))
}

pub fn short(id: &ChangesetId) -> String {
    id.to_string()[..12].to_string()
}

/// Text view of bytes, or `None` for binary content (NUL or invalid UTF-8).
pub fn as_text(bytes: &[u8]) -> Option<&str> {
    if bytes.contains(&0) {
        return None;
    }
    std::str::from_utf8(bytes).ok()
}

/// Cut `s` to at most `max` bytes on a char boundary.
pub fn truncate(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

impl Server {
    /// Resolve a ref name, full id or short id (4 or more hex digits, ADR
    /// 0035); empty or absent means `main`. A ref wins over a short id that
    /// spells the same, as in the CLI.
    pub(crate) fn resolve_rev(&self, rev: Option<&str>) -> Result<ChangesetId, ToolError> {
        let rev = rev
            .map(str::trim)
            .filter(|r| !r.is_empty())
            .unwrap_or(DEFAULT_REF);
        let fail = |e: CliError| {
            ToolError(format!(
                "cannot resolve rev `{rev}`: {e}. Use a ref name from leep_refs, a changeset id, \
                 or a unique prefix of one (at least 4 hex digits)."
            ))
        };
        match self.client.resolve(rev) {
            Err(e) if e.kind() == ErrorKind::NotFound && looks_like_prefix(rev) => {
                match self.client.resolve_prefix(rev) {
                    Err(p) if p.kind() == ErrorKind::NotFound => Err(fail(e)),
                    other => other.map_err(fail),
                }
            }
            other => other.map_err(fail),
        }
    }

    pub(crate) fn blob(&self, hash: &leep_core::Hash) -> Result<Vec<u8>, ToolError> {
        if let Some(c) = &self.cfg.cache {
            return Ok(c.fetch(&self.client, hash)?);
        }
        Ok(self.client.get_blob(hash)?)
    }

    /// `None` when `name` is not one of our tools.
    pub fn call_tool(&self, name: &str, args: Value) -> Option<ToolResult> {
        Some(match name {
            "leep_refs" => self.refs(),
            "leep_log" => self.log(args),
            "leep_show" => self.show(args),
            "leep_list_files" => self.list_files(args),
            "leep_read_file" => self.read_file(args),
            "leep_diff" => self.diff(args),
            "leep_blame" => self.blame(args),
            "leep_search" => self.search(args),
            "leep_review_status" => self.review_status(args),
            "leep_commit_and_land" => self.commit_and_land(args),
            _ => return None,
        })
    }
}

fn tool(name: &str, title: &str, description: &str, schema: Value, read_only: bool) -> Value {
    json!({
        "name": name,
        "title": title,
        "description": description,
        "inputSchema": schema,
        "annotations": { "readOnlyHint": read_only, "openWorldHint": false },
    })
}

const REV_DOC: &str =
    "Branch name (default `main`), `tags/<name>` or a tag's short name, or full 64-hex changeset id.";

pub fn definitions() -> Vec<Value> {
    vec![
        tool(
            "leep_refs",
            "List refs",
            "List all refs and the changeset id each points to. Tags are the refs under `tags/` and never move.",
            json!({ "type": "object", "properties": {}, "additionalProperties": false }),
            true,
        ),
        tool(
            "leep_log",
            "Show history",
            "Show history newest first. Filters (all optional, combined with AND): a path the \
             change touched, author text, agent model, agent session. Each entry has id, author, \
             message and `agent` metadata (model, session, prompt, tests) when an agent made the \
             change. With `follow` and a file `path`, history continues under the file's old name \
             when it was renamed (each entry then has `path_at`, and `renamed` where the rename \
             happened). When more history follows, the result has `next`: pass it back as \
             `cursor` for the next page.",
            json!({
                "type": "object",
                "properties": {
                    "rev": { "type": "string", "description": REV_DOC },
                    "path": { "type": "string", "description": "Only changesets that touched this file path." },
                    "follow": { "type": "boolean", "description": "Follow renames of the file `path` (default false; `path` must be a file)." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200, "description": "Max matching entries (default 20)." },
                    "author": { "type": "string", "description": "Only changesets whose author contains this text (case-insensitive)." },
                    "agent_model": { "type": "string", "description": "Only changesets made by this agent model (exact match on agent.model)." },
                    "agent_session": { "type": "string", "description": "Only changesets from this agent session (exact match on agent.session)." },
                    "cursor": { "type": "string", "description": "The `next` value of the previous page, to continue it (repeat the other arguments, with `rev` set to that page's `from`)." }
                },
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_show",
            "Show a changeset",
            "Show one changeset: author, message, parents, agent metadata, and the paths it changed \
             (A added, M modified, D deleted) relative to its first parent.",
            json!({
                "type": "object",
                "properties": { "rev": { "type": "string", "description": "Changeset id or ref name (required)." } },
                "required": ["rev"],
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_list_files",
            "List files",
            "List every file (recursively) in the repository at a revision, with sizes. Use `prefix` \
             to restrict to a directory.",
            json!({
                "type": "object",
                "properties": {
                    "rev": { "type": "string", "description": REV_DOC },
                    "prefix": { "type": "string", "description": "Directory path such as `src/api`." }
                },
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_read_file",
            "Read a file",
            "Read a text file at a revision. Output is capped at 256 KiB (a note says when it was \
             cut); binary files are reported, not returned.",
            json!({
                "type": "object",
                "properties": {
                    "rev": { "type": "string", "description": REV_DOC },
                    "path": { "type": "string", "description": "File path, e.g. `src/main.rs`." }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_diff",
            "Diff revisions",
            "Unified diff between two revisions for text files; binary files are listed only. If \
             `from` is omitted, `to` is compared with its first parent (the changes `to` made). \
             With `renames`, a deleted and an added file that are mostly the same show as one \
             rename (git-style headers, then the content diff).",
            json!({
                "type": "object",
                "properties": {
                    "from": { "type": "string", "description": "Older rev; default is the first parent of `to`." },
                    "to": { "type": "string", "description": "Newer rev (required)." },
                    "prefix": { "type": "string", "description": "Only paths under this directory." },
                    "renames": { "type": "boolean", "description": "Detect renames (default false)." },
                    "rename_threshold": { "type": "integer", "minimum": 0, "maximum": 100, "description": "Minimum similarity percent for a rename (default 50)." }
                },
                "required": ["to"],
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_blame",
            "Blame a file",
            "For each line of a file, the changeset that last changed it, with its author.",
            json!({
                "type": "object",
                "properties": {
                    "rev": { "type": "string", "description": REV_DOC },
                    "path": { "type": "string", "description": "File path." }
                },
                "required": ["path"],
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_search",
            "Search file contents",
            "Search the text files of a revision, like grep, without reading them one by one. \
             Matching is per line. By default `query` is a literal; set `regex` for a regular \
             expression (Rust syntax) and `case_insensitive` to ignore case. Narrow with `path` \
             (a directory or file) and `glob` (`*` stays within one directory, `**` crosses \
             them, e.g. `**/*.rs`). Results are ordered by path then line, each with the file, \
             line number, the line (cut at 1024 bytes) and the byte ranges that matched. \
             Binary, non-UTF-8 and files over 1 MiB are skipped (`stats.files_skipped`). \
             `truncated` means more matches exist or the server's scan budget ran out; then \
             `next`, passed back as `cursor`, continues where the page stopped. The first \
             search of a large revision can be slow while the server builds its index.",
            json!({
                "type": "object",
                "properties": {
                    "query": { "type": "string", "description": "Text or regular expression to find (required)." },
                    "rev": { "type": "string", "description": REV_DOC },
                    "regex": { "type": "boolean", "description": "Treat `query` as a regular expression (default false)." },
                    "case_insensitive": { "type": "boolean", "description": "Ignore case (default false)." },
                    "path": { "type": "string", "description": "Only files at or below this directory or file path." },
                    "glob": { "type": "string", "description": "Only paths matching this glob, e.g. `**/*.rs` or `src/*.toml`." },
                    "limit": { "type": "integer", "minimum": 1, "maximum": 200, "description": "Max matching lines (default 50)." },
                    "cursor": { "type": "string", "description": "The `next` value of the previous page, to continue it (repeat the other arguments, with `rev` set to that page's `rev`)." }
                },
                "required": ["query"],
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_review_status",
            "Review status",
            "Whether a changeset could land on a ref that requires review, per changed path: the \
             rule that satisfies it (`auto:comment_only`, `auto:whitespace_only`, `auto:docs_only`, \
             `owner`, `merge_parent`), or `pending` / `rejected`, with the path's owners and who \
             approved or rejected. Owners are read from the ref's current head. Needs a server \
             with authentication.",
            json!({
                "type": "object",
                "properties": {
                    "rev": { "type": "string", "description": "Changeset id or ref name (required)." },
                    "ref": { "type": "string", "description": "Ref whose owners and head to judge against (default `main`)." }
                },
                "required": ["rev"],
                "additionalProperties": false
            }),
            true,
        ),
        tool(
            "leep_commit_and_land",
            "Commit and land changes",
            "Write changes to the repository without a working copy: give the full new content of \
             each file to create or replace, or delete:true to remove one. Creates one changeset \
             (recording agent provenance) and lands it on target_ref. `base` is the revision you \
             read the files from (default: current head of target_ref); if the ref has moved, \
             non-overlapping changes are merged automatically. On conflict NOTHING is landed and the \
             result lists each conflicting path with base/ours/theirs text; resolve them and call \
             again with base set to the reported `onto` id, including all your changes (clean \
             paths too). Landing onto a ref that does not exist needs create_ref:true. Text \
             files only. If a call fails without an answer (timeout, lost connection), it may \
             have landed: call again with `changeset` set to the id the error names, or with \
             the same `request_id`; that never lands the change twice.",
            json!({
                "type": "object",
                "properties": {
                    "message": { "type": "string", "description": "Commit message. Required unless `changeset` is given." },
                    "changes": {
                        "type": "array",
                        "description": "Required unless `changeset` is given.",
                        "minItems": 1,
                        "items": {
                            "type": "object",
                            "properties": {
                                "path": { "type": "string", "description": "Repo-relative file path." },
                                "content": { "type": "string", "description": "Entire new file content (UTF-8 text)." },
                                "delete": { "type": "boolean", "description": "Delete the file instead of writing it." },
                                "executable": { "type": "boolean", "description": "true or false to set the executable bit (with content). Omit to keep the file's current kind (executable, symlink, or regular for new files)." }
                            },
                            "required": ["path"],
                            "additionalProperties": false
                        }
                    },
                    "base": { "type": "string", "description": "Revision the changes are based on. Default: head of target_ref." },
                    "target_ref": { "type": "string", "description": "Ref to land on (default `main`)." },
                    "create_ref": { "type": "boolean", "description": "Create target_ref if it does not exist (default false, so a mistyped ref is an error). The first land onto `main` of an empty repository creates it without this." },
                    "prompt": { "type": "string", "description": "The request that led to this change, recorded as provenance." },
                    "tests": { "type": "string", "description": "Tests run and their results, recorded as provenance." },
                    "changeset": { "type": "string", "description": "Land this existing changeset (id or unique prefix) instead of creating one, e.g. to finish a call that timed out. Give no changes with it." },
                    "request_id": { "type": "string", "description": "Your own id for this request, unique per change. A retry with the same id lands the changeset the first call made instead of making another. Use a new id after a conflict." }
                },
                // `message` and `changes` are required unless `changeset` is
                // given. Not expressed with a top-level `anyOf`, which some
                // model APIs refuse in a tool schema; the tool checks it.
                "additionalProperties": false
            }),
            false,
        ),
    ]
}
