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
- Fault injection for the storage traits (ADR 0021): the dev-only
  `leep-testkit` crate wraps any blob or meta store and injects seeded
  errors, applied-then-failed writes, delays, stalls and damaged reads, and
  can cancel a call at a chosen suspension point. Fault-tolerance tests cover
  `land`, derived data, GC, the ref log, the filesystem store and the cache.
- Crash-consistency test (`cargo test -p leep-e2e --test crash`): SIGKILLs
  the real server and `gc collect` at seeded points under concurrent HTTP and
  CLI traffic on SQLite and Postgres, then checks refs, history, the ref log
  and every landed commit from outside. `LEEP_CRASH_ROUNDS` runs it longer
  (`docs/OPERATIONS.md`).
- Fixed: a `land` whose commit applied but whose answer was lost, repeated by
  the client, landed its stack a second time. `land` now recognises its own
  earlier result and re-drives a failed commit from a fresh read.
- Fixed: a damaged tree node or path-history node is now an error instead of
  being read as valid (it could leave live blobs unmarked by GC).
- Fixed: a `gc collect` killed after deleting a blob but before recording it
  could leave that blob hash answering 409 to uploads until it was
  re-condemned; the next collect now closes the stale delete announcement.
- Security review (`docs/SECURITY-REVIEW.md`). The fixes below each have a
  regression test; accepted and deferred findings are noted in their ADRs.
- Review: a merge path counts as `merge_parent` only where the first parent
  left it as it was at the merge base. Before, a hand-made merge with an old
  commit as its side parent could restore an owned file's old content (undo a
  fix) without owner approval.
- Review: `comment_only` now gives up on string interpolation that can nest
  quotes (Python f-strings, C#, Kotlin, Scala, Swift, Ruby, shell), C line
  splices and trigraphs, Java and Scala `\u` escapes, shell `#` after an
  operator, Ruby and Perl `#` without a blank before it, Ruby `=begin`, Perl
  POD, JavaScript HTML-like comments, cgo preambles and more; each let a code
  change pass as a comment change. Rust doc comments count as code, since
  their examples run as doctests. No auto rule applies to an `OWNERS` file,
  even under a docs glob.
- CLI: checkout, sync, restore, merge and land refuse to write a repository
  path with a `.leep` or `.git` component (in any case or spelling the
  filesystem treats as the same) or through a directory that is a symbolic
  link, so a crafted tree or a malicious server cannot write outside the
  working copy, plant git hooks or replace `.leep/config.json`.
- Behavior change: a ref that `--require-signatures` or `--require-review`
  protects now only moves forward. `POST /v1/refs/{name}` refuses (400, `only
  moves forward`) a move to a changeset that does not descend from the
  current value, so `leep undo` of a land on a protected ref is refused and
  suggests `leep revert` and landing the revert instead. Before, any writer
  could rewind a protected ref and drop reviewed or signed commits, such as a
  security fix, without review. Unprotected refs are unaffected.
- `POST /v1/changesets` refuses (400) a path over 4096 bytes, or paths that
  together name over a million directories or 256 MiB of directory names;
  one request with a very deep path could exhaust the server's memory.
- `blame` bounds each line diff to 500 ms, so blaming a file whose versions
  share nothing no longer occupies a server worker for minutes.
- The server refuses to start when `--require-review`,
  `--require-signatures` or the review policy's `refs` name an invalid ref
  (such as `release` with a leading space from a comma-separated variable),
  which protected nothing, and warns about protected refs that do not exist.
- Soak test (`leep-soak`, ADR 0022): an agent swarm (HTTP agents plus real
  `leep` processes) lands, branches, merges, tags, signs and gets reviewed
  against a real server on Postgres for a set time, while GC, index warmups
  and optional server restarts (`--chaos`) run beside it. It checks every
  acknowledged land, history, the ref log, search, path history, blame and
  GC reachability as it goes, and writes a JSON report and summary with
  throughput, latency, RSS and database growth trends. It fails on any
  invariant break, unattributed 5xx, hang, land p99 or RSS slope over its
  bound (`docs/OPERATIONS.md`). A ten-second run is part of `cargo test`;
  `.github/workflows/soak.yml` runs 30 minutes weekly and on demand.
- Fixed: `blame` read every first-parent ancestor down to the last change of
  the file, so a file untouched for a long time cost the whole history (1.4 s
  at 4400 changesets, over the request timeout at some tens of thousands),
  and a swarm's blame requests took enough CPU to queue every land behind
  them. It now jumps between changes with the path-history index and the
  first-parent skip list (the answer is unchanged), and checks its cache before
  walking.

### Operations
- Versioned schema migrations for SQLite and Postgres (ADR 0023). The
  metadata schema is now an ordered list of migrations compiled into the
  binary, recorded in a `schema_migrations` table with checksums, and applied
  in one transaction under a lock, so servers starting together migrate once
  and a failed migration leaves the database untouched. Existing databases are
  adopted without any change to their data: the first server to open one
  records version 1 (a database from before the ref log also gains that table).
  Applied migrations are immutable, and every schema change now needs a new
  migration plus a fixture of the schema it upgrades from.
- A server refuses to start on a database that a newer release migrated
  (`database schema vN is newer than this server`) or whose applied migration
  was edited, before it writes anything.
- `leep-server migrate [--dry-run|--status]` applies or previews migrations,
  and `--no-migrate` (`LEEP_NO_MIGRATE=1`) makes the server, `gc`,
  `derive-search`, `verify` and `repair` refuse to run while any are pending. Servers still migrate
  on start by default. See `docs/OPERATIONS.md`, "Upgrading".
- `leep_schema_version` gauge, and `MetaStore::schema_version()` (a provided
  method, so existing implementations keep compiling).
- Config reload without restart (ADR 0024): SIGHUP re-reads the tokens file
  (including per-token limits), the signing keys file and the review policy
  file, so tokens and keys rotate, tokens are revoked and protected refs and
  review rules change with no downtime. Every file is read and validated first
  with the startup checks; if any is invalid the old config stays in force and
  an error is logged, otherwise everything swaps at once. A request finishes
  under the config it started with, and a `land` uses one policy for its whole
  duration. Limiter state is kept for tokens whose limits did not change.
  Flags, listeners and stores still need a restart. New metrics:
  `leep_config_reload_total{result}`, `leep_config_generation` and
  `leep_config_last_reload_timestamp_seconds`, with alert examples in
  `docs/OPERATIONS.md`. There is no reload endpoint and no file watcher.
- A SIGHUP no longer terminates the server.
- `leep-server verify` checks a store without writing to it (ADR 0025): every
  ref's history, tree nodes and blobs exist and match their addresses
  (`--rehash` also re-reads blob bytes), the ref log replays to the refs, and
  derived rows (generation, skiplist, last merge, root tree, path history,
  search, blame, signatures, reviews, GC and rebase bookkeeping) equal a
  recomputation, for a sample or with `--full`. Problems name the kind, key,
  changeset and path. Exit status 0 clean, 1 problems, 2 could not run;
  `--json` for scripts.
- `leep-server repair rederive --kind K` recomputes derived rows and writes the
  missing or wrong ones (`--dry-run`, `--ref`), including the tree and history
  nodes it derives, and `repair drop --kind K` deletes a kind so it derives
  again on demand. Kinds history cannot regenerate (signatures, reviews,
  rebase links, GC fence state, the ref log) are refused.
- `MetaStore` gains `list_derived` and `delete_derived_kind` (data operations,
  no schema change); every backend implements them and the contract tests
  cover them.
- The crash test verifies the store after every kill, and the soak test and
  the git import e2e test verify it at the end (`--full --rehash`).

### Fixes
- Processes opening a new SQLite metadata file at the same moment (servers
  starting together, or `migrate` beside a server) no longer fail with
  "database is locked": switching the file to WAL can report a busy database
  at once instead of waiting, so opening retries for up to the 30 s busy
  timeout.
- `memory_ref_log_race` no longer fails about 1 run in 20: its tasks could
  create the same changeset and win a CAS that moved nothing, which correctly
  writes no ref log entry.
- A `leep init` whose checkout fails part way (server restarting, a bad
  blob) now removes the files it wrote and its `.leep`, so retrying works.
  It used to leave a half-made working copy that refused the retry.
- The CLI treats a 408 (the server's request timeout) like a lost connection:
  exit status 2 with advice to run the command again, instead of 1, which
  means the request was refused. The timeout can cancel a `land` after it
  moved the ref, so a 408 does not mean it failed; running `leep land` again
  sends the same changeset, and the server reports the earlier success.
- The crash test's HTTP agents land the same changeset again after an
  unanswered land, as a real client does, instead of rebuilding the stack on
  the new head, which landed it twice when the first land had applied.

### Project
- Workspace version set to 0.0.1, with a CI check that keeps it below
  0.1.0.
- Apache-2.0 license text, security policy and this changelog.
- Product direction (`docs/PRODUCT.md`) and a draft contributor license
  agreement (`CLA.md`). Pull requests are merged by the maintainer only.
