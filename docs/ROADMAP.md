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
- Token auth; structured logs; Prometheus metrics (cache hit rates, land retries, derive latency)
- Docker images and a docker-compose file (server + Postgres + MinIO)
- Blob garbage collection (mark from refs plus a retention window)
- Benchmarks: commit and land latency versus repository size

## v0.3: Agent-native
- Provenance: `leep log --agent`, and queries over `agent.*` metadata
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
