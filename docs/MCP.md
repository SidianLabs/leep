# Using Leep from AI agents (MCP)

`leep-mcp` is a [Model Context Protocol](https://modelcontextprotocol.io)
server that lets an agent read a Leep repository and land changes without a
working copy. It speaks MCP `2025-06-18` over stdio and talks to
`leep-server` over HTTP. Design: [ADR 0009](adr/0009-mcp-server.md).

## Install

```sh
cargo install --path crates/leep-mcp   # or use the leep-mcp binary from the Docker image
```

## Configure

| Variable | Meaning |
|---|---|
| `LEEP_SERVER` | Server URL, e.g. `http://127.0.0.1:7070` (required) |
| `LEEP_TOKEN` | Bearer token. A `read` token makes the agent read-only |
| `LEEP_AUTHOR` | Author on commits (default `$USER`, else `agent`) |
| `LEEP_AGENT_MODEL`, `LEEP_AGENT_SESSION` | Recorded as `agent.model` / `agent.session` on every commit |
| `LEEP_AGENT_PROMPT`, `LEEP_AGENT_TESTS` | Optional defaults for `agent.prompt` / `agent.tests` |
| `LEEP_CACHE_DIR` | Blob cache directory (shared with the `leep` CLI) |

Add it to any MCP client that takes a stdio server config:

```json
{
  "mcpServers": {
    "leep": {
      "command": "leep-mcp",
      "env": {
        "LEEP_SERVER": "http://127.0.0.1:7070",
        "LEEP_TOKEN": "<token>",
        "LEEP_AUTHOR": "build-agent",
        "LEEP_AGENT_MODEL": "<model name>",
        "LEEP_AGENT_SESSION": "<session id>"
      }
    }
  }
}
```

Logs go to stderr; stdout carries protocol messages only.

## Tools

`rev` is a branch name (default `main`), a tag (`tags/v1`, or `v1` when no branch has that name), a full 64-hex changeset id, or a unique prefix of one of at least 4 hex digits (ADR 0035; a ref wins over a prefix that spells the same).

| Tool | Arguments | Result |
|---|---|---|
| `leep_refs` | none | refs and their changeset ids; tags are the names under `tags/` |
| `leep_log` | `rev?`, `path?`, `follow?`, `limit?` (default 20, max 200), `author?`, `agent_model?`, `agent_session?` (filters combine with AND), `cursor?` | entries with author, message, `agent` metadata, and `next` when more history follows (call again with the same arguments, `rev` set to the result's `from` and `cursor` set to `next`); with `follow` (needs a file `path`) history continues under the file's old name across renames, and entries carry `path_at` and, where the rename happened, `renamed {from, to, similarity, exact}` |
| `leep_show` | `rev` | one changeset and its changed paths (A/M/D) |
| `leep_list_files` | `rev?`, `prefix?` | recursive file list with sizes |
| `leep_read_file` | `rev?`, `path` | text content; capped at 256 KiB; binary is reported, not returned |
| `leep_diff` | `from?`, `to`, `prefix?`, `renames?`, `rename_threshold?` (0 to 100, default 50) | unified diff; `from` defaults to the first parent of `to`. With `renames` (default false) a moved file shows once, with `rename from` / `rename to` / `similarity index` lines and a content diff only if it changed (ADR 0016) |
| `leep_blame` | `rev?`, `path` | per-line changeset and author |
| `leep_search` | `query`, `rev?`, `regex?`, `case_insensitive?`, `path?`, `glob?`, `limit?` (default 50, max 200), `cursor?` | matching lines `{path, line_number, line, ranges}` ordered by path then line, `truncated`, `stats`, and `next` when the page stopped early (pass it back as `cursor`, with `rev` set to the result's `rev`); per line, UTF-8 text files up to 1 MiB (others counted in `stats.files_skipped`) |
| `leep_review_status` | `rev`, `ref?` (default `main`) | per changed path: the rule that satisfies it (`auto:*`, `owner`, `merge_parent`) or `pending` / `rejected`, with owners and who approved or rejected, plus `approved`, `blocked_paths` and `next_step`; needs a server with authentication |
| `leep_commit_and_land` | `message`, `changes[]`, `base?`, `target_ref?`, `create_ref?`, `prompt?`, `tests?`, `request_id?`; or `changeset` alone | landed id, or a conflict report |

Data tools return JSON as text and as `structuredContent`; file and diff
tools return plain text. Failures (bad arguments, 401, 403, 404, unreachable
server) come back as results with `isError: true`.

### Search

`leep_log`, `leep_blame` and `leep_search` also take `fresh?`. When the
server's index lags behind recent lands (ADR 0058), they answer by default at
the newest indexed changeset and add `answered_at` and `behind` (changesets of
lag) to the result: a slightly older answer now. With `fresh: true` they ask
for the requested revision, retry once within 5 s, and otherwise return an
error naming the index, its lag and when to retry.

`leep_search` greps a revision on the server, so an agent need not list and read
every file. `query` is a literal by default; `regex: true` uses Rust regex syntax
and `case_insensitive: true` ignores case. `path` limits it to a directory or
file and `glob` filters paths (`*` stays within one directory, `**` crosses
them, so use `**/*.rs` for every Rust file). Matches are per line and carry the
byte `ranges` that matched. When `truncated` is true there are more matches, or
the server stopped after scanning its byte budget: narrow with `path`/`glob`.
The first search of a revision that was never indexed can take a while (the
operator can run `leep-server derive-search <rev>` ahead of time); later ones
are fast. `leep_search` is read-only and works with a `read` token.

### Review

On a ref that requires review (ADR 0011), `leep_commit_and_land` fails with
`changeset X needs review: path P needs approval from one of [...]` unless
every path is approved by its owners or by an auto rule. Land on another ref
(`target_ref`), then use `leep_review_status` with `rev` set to the landed id
and `ref` set to the protected ref to see which paths are blocked and who can
approve them. Approving needs a human or CI token: an agent with a `read`
token cannot review, and an agent should not approve its own change.

### Writing: `leep_commit_and_land`

`changes` is a list of `{path, content}` (entire new text of the file),
`{path, delete: true}`, with optional `executable: true|false` (omit it to keep an existing file's executable bit or symlink kind). `base` is the
revision the agent read the files from (default: head of `target_ref`, which
defaults to `main`). The tool uploads missing blobs, creates one changeset
with `agent.*` provenance, and lands it. If `main` moved and the edits do not
overlap, the server rebases automatically (`rebased: true`).

**Retrying.** The server's request timeout (408) can cancel a land after it
moved the ref, so a failed call does not mean nothing landed. When the land
itself fails that way (timeout, lost connection, 5xx) the error names the
changeset the call made; calling again with `changeset` set to that id lands
it at most once, since the server answers a changeset that is already on the
ref, or landed there as a rebased copy, with the earlier result. Or give every
call a `request_id` (recorded as `agent.request_id`): a second call with the
same id, to the same MCP server process, lands the changeset the first call
made instead of building a new one on the moved head. A conflict forgets the
id, since the resolving call carries new content; using one id for two
different changes is otherwise a mistake the tool cannot see.

Landing onto a ref that does not exist fails (`isError: true`, nothing landed)
unless `create_ref: true` is given, so a mistyped `target_ref` cannot create a
new ref (ADR 0036). The first land onto `main` of an empty repository creates
it without the flag, as `leep land` does.

On overlapping edits nothing lands, `main` is unchanged, and the result
(`isError: true`, `status: "conflict"`) contains:

- `onto`: the current head the change must be based on;
- `conflicts[]`: `path`, `base`, `ours` (what `main` has), `theirs` (the
  agent's version) as text, and `merged_with_markers` when available;
- `clean_paths`: paths that merged cleanly;
- `guidance`.

The agent resolves each conflict itself, then calls the tool again with
`base` set to `onto` and the resolved content for the conflicting paths plus
its original changes for every other file. Leep never resolves conflicts for
you.
