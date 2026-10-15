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

### Everyday workflow
- `leep branch` lists, creates and deletes branches. `branch -d` deletes
  only a branch already merged into the working copy's target ref; `-D`
  deletes it anyway. Both are compare-and-swap, so a concurrent move fails
  with a 409 instead of being overwritten.
- `leep switch BRANCH` (and `switch -c NAME [REV]`) moves a clean working
  copy to another branch.
- `leep tag` lists, creates (`tag NAME [REV]`) and deletes (`tag -d NAME`)
  tags, stored as the refs `tags/NAME`. The server never lets a tag move,
  and `land` onto `tags/` is refused.
- Revisions accept `tags/NAME`, and a short tag name when no branch has that
  name.
- The server checks every ref name written through `POST /v1/refs/{name}`
  and `land` (git-like rules, `docs/FORMATS.md`) and answers 400 for an
  invalid one. Existing valid names keep working.
- Deleting a ref that a land policy (signatures, review) covers is now
  refused with 400.
- Rename detection when displaying (ADR 0016): `leep diff` and `leep status`
  show `renamed: old -> new (NN%)`, with `--no-renames` and `-M[=N]`;
  `GET /v1/diff` takes `renames` and `rename_threshold`; the MCP `leep_diff`
  tool takes `renames`.
- `leep log --follow FILE` continues a file's history under its old names;
  `GET /v1/log` takes `follow`, and the MCP `leep_log` tool takes `follow`.
- Ref log (ADR 0018): every ref create, move and delete is recorded with the
  actor, operation and time, in the same transaction as the change. `leep
  reflog [REF] [-n N]` shows it, `GET /v1/reflog[/{name}]` serves it to read
  tokens, and `POST /v1/refs/{name}` accepts optional `operation` and
  `message` labels. `leep undo [REF] [--to SEQ]` moves a ref back to its
  previous value through the normal policy checks; the redo is another undo.
  `MetaStore` gains `cas_ref_logged` and `ref_log` (`cas_ref` keeps working).
  The `ref_log` table is created when the server first opens an existing
  database, so upgrade every server before relying on the log.

- `leep sync` no longer needs a clean working copy. It saves uncommitted
  changes as a snapshot changeset, has the server rebase it onto the new head
  and puts the result back as uncommitted changes, with conflict markers
  (and `commit`/`land` refusing) where both sides edited the same text. Local
  unlanded commits are rebased onto the head too, and a failed sync leaves
  the working copy as it was and prints the snapshot id. `sync --no-rebase`
  keeps the old rule (ADR 0017).
- Conflicts with no text merge (binary files, delete versus modify), from
  `sync`, `stash apply` and `land`, keep your version but are marked
  conflicted: `status` shows them, `commit` and `land` refuse, and
  `leep resolve PATH...` (or `leep restore`) clears the mark. `resolve` also
  refuses files that still contain conflict markers unless `--force`.
- `leep stash` (`push`, `list`, `show`, `apply`, `pop`, `drop`). A stash is a
  changeset on the server marked `leep.snapshot = "stash"` with a local list;
  `apply` and `pop` rebase it onto the current base and also accept a
  snapshot id.
- `leep restore PATH...` discards uncommitted changes to files or
  directories, or with `--source REV` takes their content from another
  revision. New files are left alone unless `--force`.
- `leep amend` folds uncommitted changes and a new message into an unlanded
  base changeset, and refuses a landed one. Old signatures do not carry over.
- `leep reset [REV]` moves the working copy's base without touching refs:
  soft by default (keeps the files as uncommitted changes, so no REV
  undoes the last commit), `--hard` also discards local changes.
- `leep status` shows the number of stashes and how many commits the base is
  ahead of the target ref.
- Files are now written through a temp file and renamed, so an interrupted
  checkout, land or sync cannot leave a half-written file.
- `leep merge REV`, `leep rebase [ONTO]`, `leep cherry-pick REV` and
  `leep revert REV` build local, unlanded commits; `land` moves the ref as
  usual. A clean merge fast-forwards when it can (`--no-ff` always makes a
  merge commit). A conflicted merge, cherry-pick or revert leaves markers (or
  `unresolved` paths) on top of the unchanged base; `status` shows it, `commit`
  finishes it (message and, for a merge, second parent default from the
  operation), and `--abort` undoes it. `rebase` moves the unlanded stack, like
  `sync`, and stops with nothing changed on a conflict. A revert is the inverse
  change stored as a temporary changeset and rebased onto the base, so edits
  made since to other lines survive; merges cannot be reverted yet (ADR 0019).
- `POST /v1/merge` returns `clean` (the cleanly merged changes relative to
  `parents[0]`) with its conflicts, like `rebase` and `land` already did. The
  field is additive; `leep merge` refuses a server that omits it rather than
  drop the other side's changes.
- `leep commit -m` may be left out while a merge, cherry-pick or revert is
  waiting for its commit.

### Hardening
- Fuzz targets for every decoder and parser: persisted objects, derived
  rows, API bodies, the router, `OWNERS` and the server's config files, the
  comment lexer, rename detection, ref names, paths and the search planner.
  They run as seeded smoke tests inside `cargo test`, and nightly under
  libFuzzer in CI (`docs/FUZZING.md`).
- The `comment_only` auto-approval rule no longer treats a vertical tab or form
  feed as a blank in shell and YAML files, where `a<FF>b` is one word and
  `a<FF># x` has no comment. It also reads a Rust or C character literal
  only if its escape is well formed, so `//` after a stray `'\` is a comment
  again. Both made the lexer disagree with the language; it now gives up
  (so a human reviews) or reads the text as the language does.
- Per-token request limits (ADR 0020): a token-bucket rate (`--rate-limit`,
  `--rate-burst`), a cap on requests in flight (`--max-concurrent-per-token`)
  and a body size cap for read tokens (`--read-max-body-bytes`), each
  overridable per token with `rate`, `burst`, `concurrency` and `max_body` in
  the tokens file (`"unlimited"` opts a token out). Without a tokens file the
  limits apply per client address. Refusals are 429 with `Retry-After`, or a
  JSON 413 for a body over a token's cap. Off unless configured. New metrics
  `leep_requests_limited_total` and `leep_requests_in_flight`.
- The CLI retries a rate-limited `GET` (up to 4 tries, honoring
  `Retry-After`) and reports a wait over 10 seconds instead of sitting
  through it.

### Project
- Workspace version set to 0.0.1, with a CI check that keeps it below
  0.1.0.
- Apache-2.0 license text, security policy and this changelog.
- Product direction (`docs/PRODUCT.md`) and a draft contributor license
  agreement (`CLA.md`). Pull requests are merged by the maintainer only.
