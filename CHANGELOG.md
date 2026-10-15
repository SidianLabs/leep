# Changelog

All notable changes to Leep are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Leep has not
had a release; versions stay below 0.1.0 until the maintainer cuts one
(see [docs/ROADMAP.md](docs/ROADMAP.md)).

## [Unreleased]

### Core loop (M1)
- Content-addressed data model (BLAKE3, postcard), storage traits and API
  types.
- Blob stores: filesystem, S3-compatible and an LRU cache. Metadata
  stores: SQLite and Postgres.
- Derived data: trees, generation numbers, skiplists, diff, log, blame,
  merge and rebase.
- `land` without a merge queue: replays the whole unlanded stack, reports
  conflicts instead of resolving them, and retries lost races with backoff.
- HTTP server, a CLI with lazy sparse checkouts, and a git bridge that
  imports and exports with full round-trip.

### Operable (M2)
- Token auth with read and write roles (ADR 0007).
- JSON logs and Prometheus metrics.
- Docker image and a compose stack with Postgres.
- Blob garbage collection (ADR 0008).
- Benchmarks for commit, land and merge-base.

### Agent-native (M3)
- MCP server exposing Leep to AI agents (ADR 0009).
- Provenance queries over `agent.*` metadata.
- Signed changesets and a `--require-signatures` land policy (ADR 0010).
- Review service with `OWNERS` files and semantic auto-approval
  (ADR 0011).
- Fenced two-phase blob GC (ADR 0012).

### History and search (M4)
- Path-history index and `leep last-change` (ADR 0013).
- Server-side code search: `leep search`, `GET /v1/search` and the MCP
  `leep_search` tool (ADR 0014).

### Project
- Workspace version set to 0.0.1, with a CI check that keeps it below
  0.1.0.
- Apache-2.0 license text, security policy and this changelog.
- Product direction (`docs/PRODUCT.md`) and a draft contributor license
  agreement (`CLA.md`). Pull requests are merged by the maintainer only.
