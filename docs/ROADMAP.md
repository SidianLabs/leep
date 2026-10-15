# Leep roadmap

Milestones are ordered. Each one should leave `main` releasable.

## v0.1: Core loop (in progress)
- [x] Core data model, storage traits, API types (`leep-core`)
- [x] Blob backends: filesystem, S3/MinIO, LRU cache (`leep-store`)
- [x] Metadata backends: SQLite, Postgres (`leep-meta`)
- [x] Derived data and operations: tree, generation, skiplist, diff, log, blame, merge, rebase, land (`leep-derive`)
- [x] HTTP server (`leep-server`)
- [x] CLI with lazy sparse checkouts (`leep-cli`)
- [x] Git import and export (`leep-git`)
- [x] End-to-end test: import a git repo, run 50 concurrent agent lands, export back to git

## v0.2: Operable
- [x] Token auth (static bearer tokens, read/write roles; ADR 0007)
- [x] Structured logs (`--log-format json`, one line per request)
- [x] Prometheus metrics (cache hit rates, land retries, derive latency; `--metrics-listen`)
- [x] Docker image and a docker-compose file (server + Postgres; `docs/DEPLOY.md`)
- [x] Blob garbage collection (mark from all stored changesets plus a retention window; ADR 0008)
- [x] Benchmarks: commit and land latency versus repository size (`docs/PERFORMANCE.md`)

## v0.3: Agent-native
- Fenced two-phase blob GC that closes the last claim/delete race (ADR 0008)
- [x] Provenance: `leep log --agent`, and queries over `agent.*` metadata (`leep provenance`, `leep blame --who`)
- Signed changesets (author keys)
- Review service with semantic approval rules (for example, comment-only changes auto-approve) and per-directory owners
- MCP server exposing Leep operations to agents

## v0.4: Scale
- Regional read-through cache tier
- FUSE virtual filesystem (`leep mount`)
- Path-history index and server-side code search as derived data
- Distributed SQL `MetaStore` backend

## Later / research
- DeltaDB-style live edit streams (CRDT worktrees) between commits
- Linking each change to the agent conversation that produced it
