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
| `leep_log` | `rev?`, `path?`, `follow?`, `limit?` (default 20, max 200), `author?`, `agent_model?`, `agent_session?` (filters combine with AND), `cursor?`, `full?` | entries with author, the message's first line (`leep_show` has the whole message), `files_changed`, `agent` metadata, and `next` when more history follows (call again with the same arguments, `rev` set to the result's `from` and `cursor` set to `next`); with `follow` (needs a file `path`) history continues under the file's old name across renames, and entries carry `path_at` and, where the rename happened, `renamed {from, to, similarity, exact}`; `full: true` gives each entry the whole message and its non-agent metadata as `extra`, as before summaries |
| `leep_show` | `rev` | one changeset and its changed paths (A/M/D) |
| `leep_list_files` | `rev?`, `prefix?` | recursive file list with sizes |
| `leep_read_file` | `rev?`, `path` | text content; capped at 256 KiB; binary is reported, not returned |
| `leep_diff` | `from?`, `to`, `prefix?`, `renames?`, `rename_threshold?` (0 to 100, default 50) | unified diff; `from` defaults to the first parent of `to`. With `renames` (default false) a moved file shows once, with `rename from` / `rename to` / `similarity index` lines and a content diff only if it changed (ADR 0016) |
| `leep_blame` | `rev?`, `path` | per-line changeset and author |
| `leep_search` | `query`, `rev?`, `regex?`, `case_insensitive?`, `path?`, `glob?`, `limit?` (default 50, max 200), `cursor?` | matching lines `{path, line_number, line, ranges}` ordered by path then line, `truncated`, `stats`, and `next` when the page stopped early (pass it back as `cursor`, with `rev` set to the result's `rev`); per line, UTF-8 text files up to 1 MiB (others counted in `stats.files_skipped`) |
| `leep_review_status` | `rev`, `ref?` (default `main`) | per changed path: the rule that satisfies it (`auto:*`, `owner`, `merge_parent`) or `pending` / `rejected`, with owners and who approved or rejected, plus `approved`, `blocked_paths` and `next_step`; needs a server with authentication |
| `leep_reflog` | `ref?` (default `main`) or `all?`, `limit?` (default 20, max 200), `cursor?`, `full_ids?` | the ref's moves newest first: `seq`, `op`, `old`, `new` (12-digit id prefixes unless `full_ids`), `time`, `actor`, `message`, and `land_id` for a land that named itself (`land_changeset` when the ref took a rebased copy); `ref` per entry with `all`; `next` when older entries follow (pass it back as `cursor`) |
| `leep_resolve` | `rev` | `{rev, id, via}`: the full id and whether it came from a ref (or tag), a full `id` or a `prefix`; an ambiguous prefix is an error listing the candidates |
| `leep_merge_base` | `a`, `b` | `base` (the newest common ancestor, null if unrelated) and `a_is_ancestor_of_b` / `b_is_ancestor_of_a` |
| `leep_provenance` | `rev?`, `path?`, `limit?` (changesets scanned, default 1000, max 10000), `top?` (rows per group, default 10, max 100) | per agent model and session, and per human author: changesets, distinct files touched, first and last timestamps; `agent_changesets` / `human_changesets` totals, `*_omitted` counts past `top`, `scanned` and `truncated` |
| `leep_commit_and_land` | `message`, `changes[]`, `base?`, `target_ref?`, `create_ref?`, `prompt?`, `tests?`, `request_id?`; or `changeset` alone | landed id, or a conflict report |
| `leep_resolve_and_land` | `conflict_id` (or `onto`), `resolutions[]` (`{path, content}` or `{path, delete: true}`, `executable?`), `message?` | finishes a conflict report sending only the conflicting files; same result shape as `leep_commit_and_land` |

Data tools return JSON as text and as `structuredContent`; file and diff
tools return plain text. Failures (bad arguments, 401, 403, 404, unreachable
server) come back as results with `isError: true`. A failure from a server
request ends with `(server request id: ID)`, after the server's stable error code as
`[code: CODE]` when it sent one (`not_found`, `ref_missing`,
`ambiguous_prefix`, ...; FORMATS.md, "Error codes"), so an agent can tell
conditions apart without matching text. An ambiguous short id also lists
`(candidates: ...)`. Each tool call sends its own
`X-Request-Id`, and the server logs the request under that id (see
[RUNBOOKS.md](RUNBOOKS.md#collecting-a-bug-report)). It is unrelated to the
`request_id` argument of `leep_commit_and_land`.

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
call a `request_id` (recorded as `agent.request_id`).

**`request_id` lands at most once, across restarts.** The land is sent with
a land id (ADR 0057) derived from the request and who sends it: the first 16
bytes of
`blake3("leep-mcp-land-id\0" || author || "\0" || session || "\0" || target_ref || "\0" || request_id)`,
where `author` is `LEEP_AUTHOR` (the commit author) and `session` is
`LEEP_AGENT_SESSION`, or empty when it is not set. The NUL separators are
unambiguous: environment variables cannot hold NUL, ref names cannot hold
control characters (`refname::validate`), and `request_id` comes last. Every
MCP server process with the same author and session derives the same id, and the server keeps land ids in the
ref log for as long as the log, so a second call with the same `request_id`
and `target_ref` is answered with the first call's land however long ago it
happened, however many lands came between, and whether or not the MCP server
restarted in between (a new process makes a fresh changeset, the server
refuses its id as already landed, and the tool answers with the recorded land
instead). Such an answer carries `from_record: true` and the original
`changeset` (unless the new changeset came out identical to the original,
which then simply lands nothing new). Within one process the tool also remembers the changeset a
`request_id` made and lands that one again without building another.

- A `request_id` needs to be unique per author (and per session, when
  `LEEP_AGENT_SESSION` is set), not globally: two agents that pick the same
  `request_id` land independently. The guarantee holds across restarts that
  keep the author and session; an MCP server restarted with a different
  `LEEP_AGENT_SESSION` treats the request as new.
- The guarantee is per `target_ref`: the same `request_id` on another ref is
  a new request there.
- A conflict lands nothing and records nothing, so the call that resolves it
  may keep the `request_id` (`leep_resolve_and_land` does).
- If the server refuses the id as already landed but neither its answer nor
  the ref log (newest 10,000 entries) names the earlier changeset, the call
  fails, nothing lands, and the error says to look in `leep_reflog` and use a
  new `request_id` for a new change.
- If the earlier changeset is no longer stored (changeset GC, ADR 0059), the
  answer still has `from_record: true`, without comparing changes.
- Reusing a `request_id` for a different change is a mistake: if the earlier
  land's changes differ from this call's, the result is `isError: true`,
  `status: "already_landed"`, with the earlier `changeset` and the
  `differing_paths`, and nothing new lands. Use a new id per change.
- Against a server older than land ids (no `land-id` in `GET /v1/ready`
  `features`), the id is ignored and only the in-process memory and the
  server's landed-copy scan (ADR 0021) prevent a second land.

Landing onto a ref that does not exist fails (`isError: true`, nothing landed)
unless `create_ref: true` is given, so a mistyped `target_ref` cannot create a
new ref (ADR 0036). The first land onto `main` of an empty repository creates
it without the flag, as `leep land` does.

On overlapping edits nothing lands, `main` is unchanged, and the result
(`isError: true`, `status: "conflict"`) contains:

- `conflict_id`: names this conflict for `leep_resolve_and_land`;
- `onto`: the current head the change must be based on;
- `conflicts[]`: `path`, `base`, `ours` (what `main` has), `theirs` (the
  agent's version) as text, and `merged_with_markers` when available;
- `clean_paths`: paths that merged cleanly;
- `guidance`.

The agent resolves each conflict itself and calls `leep_resolve_and_land`
with the `conflict_id` and `resolutions`: the full resolved content (or
`delete: true`) of every conflicting path, and nothing else. The MCP server
remembers the rest from the conflict: the server's `clean` changes, relative
to `onto`, for every path that merged cleanly (including paths both sides
changed in different places, whose merged text keeps the other side's
edits). It builds one changeset on `onto` from those plus the resolutions,
with the conflicted change's message (or `message`) and `agent.*`
provenance, and lands it under the same `request_id`, so a retried
resolution lands at most once (without a `request_id`, the first conflict
names the land, and its key `conflict:<conflict_id>` is recorded as
`agent.request_id`). A resolution may also list other paths to change them. If
the ref moved again and the resolution conflicts, the result is a new
conflict report with a new `conflict_id`. `onto` instead of `conflict_id`
picks the one remembered conflict onto that id.

A server that does not list `land-clean-blobs` in `GET /v1/ready` `features`
computes a conflict in a staged replay and stores the merged text of a path
both sides changed only when a land moves the ref, so that text is missing
after a conflict. Against such a server the tool checks the clean changes'
blobs first and, when some are missing, has the server merge the two
changesets (`POST /v1/merge`), which stores them; it refuses, landing
nothing, if they are still missing. A server with the feature stores them
before answering, and its `clean` changes are used as they are.

Conflicts are remembered in the MCP server process only (the last 64, one
per request). If it restarted, `leep_resolve_and_land` fails with an error
saying so and nothing lands: call `leep_commit_and_land` again with `base`
set to `onto` and every file the change touches, the resolved content for
the conflicting paths and the clean paths' content as they should end up,
keeping the `request_id`.

For a 20-file change (files of about 2.4 KB) with one conflicting file, the
agent's second call is 2,465 bytes with `leep_resolve_and_land` against
46,492 bytes resending every file to `leep_commit_and_land` (test
`resolve_and_land_sends_only_the_conflicting_files`). Resending also risks
undoing the other side's edits to a path that merged cleanly, if the agent
sends its own version of it.

Leep never resolves conflicts for you.
