# Leep: guide for agents

Leep is a server-centric, content-addressed version control system written
in Rust. Read [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before
non-trivial work, and [docs/FORMATS.md](docs/FORMATS.md) before touching
anything that is persisted or sent over the wire.

## Commands

```sh
cargo build --workspace
cargo test --workspace                       # Postgres tests skip unless LEEP_TEST_POSTGRES_URL is set
cargo test -p <crate>                        # one crate
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
cargo bench -p leep-derive                   # criterion; LEEP_BENCH_LARGE=1 adds 100k cases; see docs/PERFORMANCE.md
```
CI runs fmt check, clippy with `-D warnings`, and tests against Postgres 16.
A change isn't done until all three pass locally.

End-to-end tests drive the real binaries and need them built first:
`cargo build --workspace && cargo test -p leep-e2e`
(Postgres variant runs when `LEEP_TEST_POSTGRES_URL` is set).

## Never

- Change the field order, variant order or serde attributes of `Changeset`,
  `FileChange`, `FileKind`, `TreeNode`, `TreeEntry` or `Hash`. Doing so
  rewrites every hash. Follow the `change-persistent-format` skill instead.
- Use `#[serde(tag = ...)]` on persisted types (postcard can't decode them).
- Read-then-write a ref. Refs move only through `MetaStore::cas_ref`.
- Recurse over history on the call stack. Walk iteratively; histories are
  deep.
- Make `leep-cli` depend on anything except `leep-core`, or make
  `leep-derive` depend on a concrete backend.
- Remove or rename a `/v1` API field or endpoint. Only add, using
  `#[serde(default)]` for new fields.
- Silently resolve merge conflicts on the server.
- Bump the workspace version, tag a release, or mark a roadmap milestone
  closed. Those are maintainer decisions; see "How versions move" in
  [docs/ROADMAP.md](docs/ROADMAP.md).

## Where things go

| Change | Crate | Skill |
|---|---|---|
| New type or API shape | `leep-core` | `add-api-endpoint` |
| New index or history query | `leep-derive` | `add-derived-data-kind` |
| New database or object store | `leep-meta` / `leep-store` | `add-storage-backend` |
| Change to anything persisted | `leep-core` + ADR | `change-persistent-format` |
| Run the stack by hand | — | `run-leep-locally` |
| Split a large task across subagents | — | `coordinate-agents` |
| Commit, branch or open a PR | — | `commit-and-pr` |

## Conventions

- Errors: `leep_core::Error`. `Invalid` → 400, `NotFound` → 404,
  `Conflict` and ref CAS loss → 409, `Storage`/`Other` → 500.
- Async traits use `async-trait`; stores are passed around as
  `Arc<dyn BlobStore>` / `Arc<dyn MetaStore>`.
- Test logic against `leep_core::memory` stores. Every backend must call the
  `check_*_contract` functions.
- Doc comments are short and say *why*. Avoid narrating comments.
- Shared dependency versions go in the root `[workspace.dependencies]`.
- Architectural decisions get an ADR in `docs/adr/`. Update
  `docs/ARCHITECTURE.md` in the same change.
- Commits, branches and PRs follow the `commit-and-pr` skill:
  `<area>: <imperative summary>` subjects, `<area>/<kebab-desc>` branches,
  and squash-merged PRs with Summary / Why / Changes / Testing /
  Compatibility sections.
- **Never credit AI tools.** No `Co-Authored-By` for Claude or any AI, no
  session links, no "Generated with ..." footers, and no model names, in
  commits, PRs, code or docs.
