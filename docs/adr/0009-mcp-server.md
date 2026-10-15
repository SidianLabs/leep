# 0009. MCP server
Status: Accepted
Date: 2026-10-15

## Context
Agents are the main writers Leep is built for, but today they would need a
shell, a working copy and the `leep` CLI. The v0.3 roadmap calls for an MCP
(Model Context Protocol) server so any MCP client can read history and land
changes directly.

## Decision
- New crate `leep-mcp` with binary `leep-mcp`. It speaks MCP (protocol version
  `2025-06-18`) over stdio: newline-delimited JSON-RPC 2.0, stdout for
  protocol messages only, logs on stderr. Implemented methods: `initialize`,
  `notifications/initialized`, `ping`, `tools/list`, `tools/call`. Anything
  else is a JSON-RPC `-32601`; bad JSON is `-32700`.
- We implement the protocol directly on `serde_json` instead of using an MCP
  SDK. The surface is five methods and static tool schemas, so an SDK would
  add a large async dependency tree and a version-coupling risk for little
  code saved, and the handler stays a plain function that tests can drive
  in-process.
- It is a thin HTTP client like the CLI. It depends on `leep-core` and on the
  `leep-cli` library for `client::Client` (and the blob `Cache`), so error
  messages for 401/403/404 match the CLI. It does not depend on derive, store,
  meta or server; `leep-server` and `leep-derive` are dev-dependencies for
  end-to-end tests only. The client is blocking, so the loop is synchronous
  and handles one request at a time.
- Configuration is environment only: `LEEP_SERVER` (required), `LEEP_TOKEN`,
  `LEEP_AUTHOR`, and defaults for `LEEP_AGENT_MODEL`, `LEEP_AGENT_SESSION`,
  `LEEP_AGENT_PROMPT`, `LEEP_AGENT_TESTS` recorded as `agent.*` in
  `Changeset::extra` (the same keys as `leep commit`).
- Tools: `leep_refs`, `leep_log`, `leep_show`, `leep_list_files`,
  `leep_read_file`, `leep_diff`, `leep_blame`, and the single write tool
  `leep_commit_and_land`. The default rev is the `main` ref.
- `leep_commit_and_land` needs no working copy. The agent sends full file
  contents; the tool resolves the base (default: head of the target ref, or no
  parent if the ref does not exist yet), uploads only missing blobs, creates one
  changeset with provenance in `extra` only (commit messages are stored
  verbatim), and calls `POST /v1/land`.
- Conflicts are never resolved on the server or by the tool. A conflicting land
  returns a tool result with `isError: true` and structured data: `onto` (the
  head the change must be rebased on), each conflicting path with base, ours
  and theirs text plus markers when the server has them, the paths that merged
  cleanly, and instructions to retry with `base = onto` and resolved content for
  all files. The target ref is untouched. A `-32602` protocol error is used
  only for unknown tools or a malformed `tools/call`; everything else
  (bad arguments, 401, 403, 404, unreachable server) is `isError: true`.
- Limits protect the agent's context: file reads are capped at 256 KiB, diffs
  at 256 KiB and 1 MiB per side, listings at 5000 entries. Binary files are
  reported but not returned, and writes are text only.
- No `/v1` API change and no persisted format change.

## Consequences
- Agents get full read and write access with one binary and three variables.
- Access control is the server's: use a `read` token for read-only agents.
- `leep_diff` defaults `from` to the first parent of `to`, whereas the HTTP API
  treats an omitted `from` as the empty tree, because that is what an agent
  asking "what did this change do" wants.
- Binary writes, streaming, and MCP resources/prompts are not supported yet;
  adding them is additive.
- Because the CLI library is a dependency, `leep-cli` must keep `client` and
  `cache` public and free of terminal assumptions.
