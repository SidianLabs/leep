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

`rev` is a ref name (default `main`) or a full 64-hex changeset id.

| Tool | Arguments | Result |
|---|---|---|
| `leep_refs` | none | refs and their changeset ids |
| `leep_log` | `rev?`, `path?`, `limit?` (default 20, max 200) | entries with author, message, `agent` metadata |
| `leep_show` | `rev` | one changeset and its changed paths (A/M/D) |
| `leep_list_files` | `rev?`, `prefix?` | recursive file list with sizes |
| `leep_read_file` | `rev?`, `path` | text content; capped at 256 KiB; binary is reported, not returned |
| `leep_diff` | `from?`, `to`, `prefix?` | unified diff; `from` defaults to the first parent of `to` |
| `leep_blame` | `rev?`, `path` | per-line changeset and author |
| `leep_commit_and_land` | `message`, `changes[]`, `base?`, `target_ref?`, `prompt?`, `tests?` | landed id, or a conflict report |

Data tools return JSON as text and as `structuredContent`; file and diff
tools return plain text. Failures (bad arguments, 401, 403, 404, unreachable
server) come back as results with `isError: true`.

### Writing: `leep_commit_and_land`

`changes` is a list of `{path, content}` (entire new text of the file),
`{path, delete: true}`, with optional `executable: true|false` (omit it to keep an existing file's executable bit or symlink kind). `base` is the
revision the agent read the files from (default: head of `target_ref`, which
defaults to `main`). The tool uploads missing blobs, creates one changeset
with `agent.*` provenance, and lands it. If `main` moved and the edits do not
overlap, the server rebases automatically (`rebased: true`).

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
