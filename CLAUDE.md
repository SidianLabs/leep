# Leep: guide for agents

Leep is a server-centric, content-addressed version control system written
in Rust, built first for teams running many coding agents on one codebase
([docs/PRODUCT.md](docs/PRODUCT.md)). Read
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) before non-trivial work, and
[docs/FORMATS.md](docs/FORMATS.md) before touching anything that is
persisted or sent over the wire.

## Commands

```sh
cargo build --workspace
cargo test --workspace                       # Postgres tests skip unless LEEP_TEST_POSTGRES_URL is set
cargo test -p <crate>                        # one crate
cargo clippy --workspace --all-targets -- -D warnings
cargo fmt --all
cargo bench -p leep-derive                   # criterion; LEEP_BENCH_LARGE=1 adds 100k cases; see docs/PERFORMANCE.md
```
CI runs fmt check, `scripts/check-version.sh` (versions stay below 0.1.0),
clippy with `-D warnings`, and tests against Postgres 16.
A change isn't done until all of them pass locally.

End-to-end tests drive the real binaries and need them built first:
`cargo build --workspace && cargo test -p leep-e2e`
(Postgres variant runs when `LEEP_TEST_POSTGRES_URL` is set).

After touching `land`, GC, a store backend or the ref log, also run the fault
sweeps and the SIGKILL crash test (`docs/OPERATIONS.md`, "Crash and fault
tests"): `LEEP_FAULT_SEEDS=300 cargo test -p leep-derive fault_tests` and
`cargo test -p leep-e2e --test crash`.
For leaks, slow drift and long-history bugs, run a soak (`docs/OPERATIONS.md`,
"Soak tests"): `cargo build --release --workspace && target/release/leep-soak`.

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
- Move `leep-main` any other way than landing in Leep after the PR's CI is
  green. Changes reach `leep-main` only by a land in Leep; the maintainer
  reviews on GitHub. Never merge a pull request on GitHub, and never push to
  GitHub `main` or a `review/*` branch.
- Squash with `leep reset` on a stale base (see "Working in Leep").
- Bump the workspace version, tag a release, or mark a roadmap milestone
  closed. Those are maintainer decisions; see "How versions move" in
  [docs/ROADMAP.md](docs/ROADMAP.md).

## Working in Leep

Leep is developed on a live Leep server. Leep ref `leep-main` is the source
of truth; GitHub `main` is its mirror. The flow, in full in
[docs/OPERATIONS.md](docs/OPERATIONS.md) ("Dogfooding: Leep on Leep"):

1. `leep switch leep-main`, then `leep switch -c review/<area>-<kebab-desc>`.
2. Commit and `leep land` on the review ref. The mirror pushes it to the
   GitHub branch of the same name; open the PR from it into `main`.
3. Answer review by landing more commits on the review ref.
4. Once CI is green, squash and land on `leep-main`, close the PR with
   `Landed as <sha>`, and delete the review ref (`leep branch -D`).

**Squash hazard.** Squash with `leep reset` + `leep commit` only when the
stack's base (the newest `leep-main` changeset in the review ref's history)
equals the current `leep-main` head, checked explicitly. Otherwise
`leep merge leep-main`, land again on the review ref, wait for CI to pass
again, then squash. A reset without `--hard` keeps the working files, so
squashing on a moved base would silently revert everything landed on
`leep-main` since. Do it exactly like this, stopping at the first failure:

```sh
x=review/<area>-<desc>
leep switch "$x"        # the review ref's tip, not whatever is on disk
leep --json status | jq -e '.changes == [] and .merging == null and .picking == null'
head=$(leep --json log -n 1 leep-main | jq -r '.entries[0].id')
leep --json log -n 1000 "$x" | jq -e --arg h "$head" 'any(.entries[]; .id == $h)'
leep reset "$head"      # the id checked above, never the name leep-main
squash=$(leep commit -m "<PR title>")
leep --json diff "$x" "$squash" | jq -e '.entries == []'
leep land --ref leep-main
```

- `leep switch` and a clean status first: the squash commits the working
  files, so commits someone else landed on the review ref would otherwise
  be dropped.
- Reset to the id the check captured, never to the name `leep-main`, which
  `leep reset` resolves again.
- The empty diff proves the squash holds exactly the review ref's tree.
- If `leep-main` moved after the check, the server rebases the single
  squash commit (its diff is only the review's changes) or refuses on a
  conflict (exit 5). On a conflict, `leep reset --hard --force "$x"`, merge
  `leep-main` into the review ref, wait for CI and start again.

Set `LEEP_AGENT_MODEL` to the model you are, so every commit records its
provenance in `agent.model`; that is what the feature is for (below).

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
  `<area>: <imperative summary>` subjects, Leep review refs
  `review/<area>-<kebab-desc>`, and squash-and-landed PRs with Summary / Why
  / Changes / Testing / Compatibility sections.
- **Credit the agent.** A commit made with an AI agent ends with a
  `Co-Authored-By:` trailer naming it (for Claude:
  `Co-Authored-By: Claude <noreply@anthropic.com>`), and sets
  `LEEP_AGENT_MODEL` so Leep records the model. PRs may keep the tool's
  "Generated with ..." footer. Leep is built on agent provenance; its own
  history should carry it.
