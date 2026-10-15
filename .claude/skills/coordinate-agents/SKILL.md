---
name: coordinate-agents
description: Split a large Leep task across parallel subagents without them stepping on each other — crate ownership, contract-first ordering, separate build dirs, and integration review. Use when a task spans several crates or is big enough to parallelize.
---

# Coordinate parallel agents on Leep

Leep's crate boundaries are its unit of parallel work.

## Order work by dependency

```
wave 0 (coordinator):  leep-core contract — types, traits, api.rs, docs
wave 1 (parallel):     leep-store │ leep-meta │ leep-derive
wave 2 (parallel):     leep-server │ leep-cli │ leep-git
wave 3:                end-to-end tests, docs, release
```
Never start a crate before the contracts it uses are committed. If two
agents need a new shared type, the coordinator adds it to `leep-core` first.

## Briefing each agent

Each subagent prompt must be self-contained and include:
- what to read first: `CLAUDE.md`, `docs/ARCHITECTURE.md`, and the
  `leep-core` files it uses;
- **exactly one owned crate**, with "edit only files inside `crates/<name>`".
  The agent must not edit `leep-core`, other crates, the root `Cargo.toml`
  or the docs. It proposes contract changes in its report instead;
- the public API the next wave will call, written as signatures;
- its own build dir, `CARGO_TARGET_DIR=<scratch>/target-<crate>`, plus
  `-p <crate>` on every cargo command. Parallel agents sharing a target dir
  block on its lock;
- no commits or pushes. The coordinator commits after review;
- the definition of done: tests, clippy `-D warnings` and fmt for that crate,
  and a short report with the API as built, test results, deviations and
  concerns.

## Integrating

1. Read each report, then read the diff. Don't trust the summary alone.
2. Check that the dependency rules hold (ARCHITECTURE.md §3), and run
   `cargo tree -p leep-cli` to confirm the CLI pulls in no other leep crate.
3. Run the full workspace fmt, clippy and tests in the shared target dir.
4. Commit each crate separately (`store: ...`, `meta: ...`) so history stays
   bisectable.
5. Fold any proposed contract changes into `leep-core` yourself before the
   next wave starts.
