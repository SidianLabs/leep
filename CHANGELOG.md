# Changelog

All notable changes to Leep are recorded here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/). Leep has not
had a release; versions stay below 0.1.0 until the maintainer cuts one
(see [docs/ROADMAP.md](docs/ROADMAP.md)).

## [Unreleased]

### Summary log entries
- `GET /v1/log/{id}?fields=summary` answers the same entries, order and
  cursors as listings need them: id, parents, author, timestamp,
  `message_first_line`, `agent` (the `agent.*` keys), `files_changed`,
  generation and root tree (plus `path_at`/`renamed` on `follow` logs), a
  quarter of the bytes on git/git. The default (`fields=full`) is unchanged
  byte for byte; other values are 400.

### Faster path logs
- Path logs without a filter no longer read each changeset and root tree one
  after another along the path's history: they read them in one batch per
  page.

### One indexer per index group (ADR 0058)
- `leep-server --indexing local|lease|off` (default `local`, unchanged). In
  `lease` mode each index group (path history, log filter, code search) is
  warmed only by the server holding its index lease (scope `index` of the
  `leases` table; 15 s TTL, renewed every 3 s, taken over 6 s after the last
  renewal); `off` never warms. `--index-kinds` limits the groups a server
  competes for.
- In `lease` and `off` modes, while an index lease is live, path logs
  (`follow` too), `last-change`, blame, filtered logs and search never derive
  an index: they examine the un-indexed part directly within
  `--query-scan-budget` (1,024 changesets), `--blame-scan-budget` (128),
  `--search-scan-budget` (512 files) and `--search-scan-bytes` (16 MiB), with
  the answers a fully indexed store gives. Over budget a request with
  `Leep-Indexing: retry` gets `503` with `Retry-After` and an `indexing` error
  body, one with `stale-ok` an answer at the newest indexed first-parent
  ancestor (`answered_at`, `behind`), and one without the header waits for
  the indexer (`--index-wait` 10 s, `--index-waiters` 64), then scans with 8
  times the budgets (`--legacy-scan-factor`), then gets the 503. Without a
  live lease queries derive inline as before.
- Additive API: request header `Leep-Indexing`, response headers
  `Leep-Indexing` (on the 503) and `Leep-Index-Scanned`, `indexing` on error
  bodies, `answered_at` and `behind` on log, last-change, blame and search
  responses, `stats.files_unindexed` on search, ready feature
  `indexing-retry`.
- A filtered log page (`author`, `extra`) now carries `next` only when
  another match follows, so it is never empty with `next`. To know that, a
  full page walks on to the next match, so a page whose filter matches
  rarely costs about what the following page used to.
- `leep log`, `last-change`, `blame` and `search` send `Leep-Indexing:
  retry`, retry an indexing 503 for up to `LEEP_INDEX_WAIT` seconds (60), then
  fail with error code `indexing` (exit 3, `retry_after_ms` in `--json`);
  `--stale-ok` takes the older answer. MCP read tools take it by default
  and report `answered_at`/`behind`; `fresh: true` insists on the head.
- `repair drop`, `repair rederive --missing` and `upgrade-index --yes` take
  the index leases of the groups they touch and wait `--index-fence` (60 s)
  before touching rows; a live holder is refused unless `--ignore-lease`,
  which `repair drop` and `upgrade-index --drop-first` refuse.
- `last_merge-v1` is written with each changeset.
- In `local` mode the warmer now warms code search side by side with path
  history and the log filter in each pass, instead of after them.
- New metrics: `leep_index_lease_held`, `leep_index_lease_acquired_total`,
  `leep_index_lease_lost_total`, `leep_index_derived_total`,
  `leep_index_backlog_changesets`, `leep_index_lag_seconds`,
  `leep_query_unindexed_scanned_total`, `leep_query_indexing_answers_total`,
  `leep_query_stale_answers_total`, `leep_query_inline_derive_total`,
  `leep_query_index_wait_seconds`, `leep_query_index_waiters`,
  `leep_create_last_merge_skipped_total`; alerts `LeepIndexLagging`,
  `LeepIndexerMissing`, `LeepIndexingAnswersHigh`.
- Code search builds a directory's filter with one batched read of its
  subdirectories' and files' rows (a directory of 500 agent directories
  made every search on a new head do 500 round trips).
- `leep-swarm --indexing-header` and indexing, stale and inline-derivation
  counts in its report.

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
- Cursor pagination for `GET /v1/log`, `GET /v1/search` and `GET /v1/reflog`:
  responses carry an optional opaque `next` cursor and requests accept
  `after=<cursor>`, so a log longer than the 10000-entry cap, a search with
  more than 1000 matches or a long ref log can be read page by page;
  concatenated pages equal one uncapped answer. Malformed cursors, or cursors
  from another query, are 400. `leep log -n`, `leep search -n` and `leep
  reflog -n` follow cursors past one page, `leep log` and `leep search` take
  `--after`, and the MCP `leep_log` and `leep_search` tools return `next` and
  take `cursor`. Requests without `after` are unchanged (additive fields; see
  docs/FORMATS.md, "Page cursors").

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

### CLI for agents
- Documented exit codes (ADR 0034, `docs/CLI.md`): 1 generic failure, 2 usage,
  3 server or network failure (was 2), 4 not found (was 1), 5 conflict (a
  land, sync, merge, cherry-pick, revert or stash apply with conflicts, a
  refusal while conflicts wait, or a lost race; was 1), 6 nothing to do
  (`commit`, `amend` and `stash push` with nothing to record; was 1). Scripts
  that tested for exactly 1 or 2 need updating. An error may add a
  `hint: ...` line, for example where the server URL came from.
- With `--json`, errors are JSON too: `{"error":{"code","message","hint",
  "retryable"}}` on stdout (the text still goes to stderr), with a stable
  `code` such as `not_found`, `conflict`, `server` or `usage`. Usage errors
  follow it when `--json` is anywhere on the command line.
- Short changeset ids (ADR 0035): every command that takes a revision accepts
  4 or more leading hex digits of a changeset id, such as the 12-digit ids the
  CLI prints; an ambiguous one lists the candidates. `leep log` headers now
  print 12-digit ids (`--json` keeps full ids). New `GET
  /v1/resolve-prefix/{prefix}` and `MetaStore::changesets_with_prefix`, with a
  default built on `list_changesets` and range-scan overrides for SQLite and
  Postgres.
- `leep land` no longer creates a ref that does not exist unless given
  `--create` (ADR 0036): `leep land --ref typo` fails with exit 4 and a hint.
  The first land onto `main` of an empty repository still works without it,
  and a land that creates a ref prints `created ref NAME`. `POST /v1/land`
  takes an optional `create`; without it the server creates the ref as
  before, so older clients and scripts are unaffected.
- The MCP server follows the CLI: `leep_commit_and_land` needs
  `create_ref: true` to land onto a ref that does not exist (except the first
  land onto `main` of an empty repository), and every tool that takes a `rev`
  accepts a short changeset id.
- `leep --help` groups the commands (Work, Branches, History, Review, Admin)
  and describes `--server`, `--token` and `--json` once; each command's help
  no longer repeats them and points there instead. The most used commands
  have examples in their `--help`.
- Conflict markers written by the CLI name the sides by role instead of
  `ours` / `theirs`: `<<<<<<< main (landed)` and `>>>>>>> yours` after `land`
  and `sync`, `yours` and `ID (merging)` after `merge`, and similarly for
  `cherry-pick`, `revert` and `stash apply`. Files with the old labels are
  still recognised; the `/v1` text and fields are unchanged.
- Colour and a pager for human output: diffs, status letters and changeset
  headers are coloured when stdout is a terminal, honouring `NO_COLOR`,
  `CLICOLOR_FORCE`, `CLICOLOR` and `--color auto|always|never`; `log`, `diff`
  and `show` page through `LEEP_PAGER`, `PAGER` or `less` on a terminal. Pipes
  and `--json` stay plain and unpaged. No new dependency.

### Hardening
- Land ids (ADR 0057): `POST /v1/land` takes an optional `land_id` (16 to 32
  random bytes, hex), the same on every retry of one land. The server stores
  it in the ref log entry of the move, in the same transaction, and answers a
  later land with that id with the original result, however many lands came
  in between, instead of landing the stack a second time; reusing an id for
  another changeset is 400. Before, a retry was recognised only while its
  rebased copy was among the last 1024 changesets, which a busy ref passes in
  under a minute. `leep land` resends a keyed land after a timeout, a 5xx
  or a lost connection, and keeps the id in `state.json` until it has an
  answer, so running it again resends the same land, but only to a server
  whose `GET /v1/ready` lists `land-id` in its new `features` field (an
  older server gets a message saying the land may have applied). The MCP
  server reuses the id when the same land is called again.
  Migration 0005 adds the nullable `ref_log.land_id` and `land_changeset`
  columns and a unique index; it runs beside v4 servers, and on a large
  Postgres the index should be built `CONCURRENTLY` by hand first (the
  migration's own build blocks lands; OPERATIONS.md, "Upgrading"). Upgrade
  servers before clients rely on land ids.
  `MetaStore` gains `find_lands`, `RefUpdateContext` gains `land`, and
  `MetaStore` wrappers must forward `find_lands`. New metric
  `leep_land_recorded_total`.
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
- Security: a blob could take a tree node's address, since a tree is stored
  in the blob store under `BLAKE3("leep:tree-v1\0" ‖ bytes)` and a blob
  with exactly those bytes hashes the same. Uploading it before the tree
  existed made that tree read as corrupt for good. Blob writes now refuse
  bytes that start with `leep:` and contain a NUL (400); text files that
  only start with `leep:` are unaffected. `leep-git import` stops at such a
  file with an error naming it, a merge that would produce one is a
  conflict, and ADR 0041 weighs the proper fix (separate store namespaces
  for trees).
- Safer server defaults. `leep-server` refuses to start on a non-loopback
  `--listen` without `--tokens-file` unless `--insecure-no-auth`
  (`LEEP_INSECURE_NO_AUTH=1`) is given; the container image listens on
  `0.0.0.0`, so it needs one or the other (the compose dev stack sets the
  variable). `read` tokens' request bodies are capped at 1 MiB by default.
  `POST /v1/blobs/missing` takes at most 100,000 hashes (400 beyond), and the
  CLI and MCP client send 10,000 at a time, which also fits a read token's
  limit.

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
- `leep-server backup`, `restore` and `backup-verify` (ADR 0026): a
  consistent backup of metadata and blobs while servers and GC keep running.
  The metadata is read first in one transaction (`REPEATABLE READ` on
  Postgres) and written as a SQLite file with the normal schema, from either
  backend; the blobs the backup pool lacks are copied second (incremental:
  blobs are immutable and content-addressed); then every changeset of the
  snapshot is walked against the pool, and the result is sealed in
  `manifest.json` (schema version, counts, ref tips, BLAKE3 of the file). A
  blob a stored changeset needs is never deleted (ADR 0012), so no GC pause is
  needed, and a backup whose check finds a missing blob exits 1 and `restore`
  refuses it. `restore` loads blobs, then metadata in one transaction, into
  empty stores only (`--force`, `--resume`), refuses a newer schema, a damaged
  file or a failed snapshot, migrates an older one (ADR 0023), keeps the ref
  log numbering, and checks the result. A snapshot from Postgres restores into
  SQLite and the other way round. `--metrics-file` writes node_exporter
  text-file metrics (last success time). Nothing persisted or on `/v1`
  changes; the backup layout is in `docs/FORMATS.md`.
- The restore drill: `crates/leep-e2e/tests/backup.rs` backs up a server under
  agent load while `gc` runs between the two halves of the backup, restores
  into fresh stores, and compares `verify --full --rehash`, every ref tip,
  history, blame, search, the ref log and a fresh checkout (SQLite, Postgres,
  and across the two). `docs/OPERATIONS.md`, "Backup and restore", has the
  procedure, the scheduled drill, RPO and RTO notes and alerts.
- `docs/RUNBOOKS.md`: a runbook for every alert in `docs/OPERATIONS.md` (meaning
  and urgency, impact, diagnosis with real commands and log lines, likely causes,
  safe fixes and what not to do, what to collect for a bug report). The suggested
  alert rules now carry a `severity` label and a `runbook_url`, the backup alerts
  are named rules (`LeepBackupStale`, `LeepBackupCheckFailed`, `LeepBackupSlow`),
  and new rules cover a server that cannot be scraped, request timeouts, broad
  throttling, lost land compare-and-swaps, schema and version skew, the weekly
  `backup-verify`, the restore drill, GC and disk space. `LeepLandErrors` now
  counts HTTP 5xx on the land route (the `error` outcome of `leep_land_total`
  also counts refused lands), and the land and backup rules no longer miss the
  first event of a counter or a backup that never succeeded.
  `scripts/check-runbooks.sh`, run in CI, fails when an alert and its runbook
  do not match.
- `leep_config_info{fingerprint}` exports a 16-character BLAKE3 fingerprint of
  the loaded config (tokens and limits, trusted keys, review policy, protected
  ref sets, server-wide limit defaults), hashed from the parsed content so file
  formatting and entry order do not change it, and holding no secret. It is
  logged at startup and on each reload; after a reload the previous
  fingerprint's series drops to 0, so `leep_config_info == 1` has one series per
  server. `leep_config_generation` stays but is documented as per process.
  The `LeepConfigGenerationSkew` alert, which fired after any single restart,
  is replaced by `LeepConfigSkew` (`count(count by (fingerprint)
  (leep_config_info == 1)) > 1`); its runbook keeps the old anchor.
- `GET /v1/ready` is a readiness probe: it reads one ref and asks the blob
  store about one fixed hash, concurrently under `--ready-timeout`
  (`LEEP_READY_TIMEOUT`, default 2 s), and answers 200 `{"status":"ready"}` or
  503 naming the failed store (`meta` or `blobs`) with a short reason and no
  connection details. Like `/v1/health` it needs no token and is never
  limited. `/v1/health` is unchanged and stays liveness. New metric
  `leep_ready_checks_total{store,result}`, alert `LeepNotReady` with a runbook,
  and a Probes section in DEPLOY.md with Kubernetes liveness and readiness
  examples.
- The path-history index stores directory nodes of more than 64 entries as
  content-defined chunks of about 32 entries (ADR 0027), so a change rewrites
  one chunk instead of the whole node. On the first 3,795 commits of git/git
  its node bytes fell from 45 MB to 13 MB at the same derivation cost. The
  derived kinds are now `history-tree-v2` and `history-node-v2`: the index
  derives again on the first path query after the upgrade (or with
  `repair rederive --kind history-tree-v2`), and `repair drop --kind
  history-tree-v1` removes the old rows once no older server is running.
- The path-history index stores each changed node, chunk and chunk list as a
  patch on an earlier whole version of it, holding only the entries that
  differ (ADR 0037). On the first 3,795 commits of git/git its node bytes fell
  from 13.0 MB to 3.7 MB (14.5 MB to 5.8 MB in Postgres) and deriving it got
  about 15% faster, with byte-identical answers. The derived kinds are now
  `history-tree-v3` and `history-node-v3`: the warmer derives the index after
  the upgrade (or `repair rederive --kind history-tree-v3` does), and `repair
  drop --kind history-tree-v2` removes the old rows once no older server is
  running.
- The path-history index stores the nodes each changeset's derivation writes
  in one pack (ADR 0045): one row per changeset (its `history-tree-v4` value)
  unless the pack passes 16 KB, nodes addressed by (changeset, part, index),
  changeset ids as indices into a table per part, patches naming the entries
  they replace by position, front-coded names, and zstd. On Linux to v2.6.16
  the index fell from 210 MB in 311,439 rows to 34.6 MB in 22,774 (6.1x;
  200 MB to 43 MB in Postgres after `VACUUM FULL`), and on a synthetic
  late-Linux history from 15.2 KB to 2.6 KB per changeset, with
  byte-identical `log`, `last-change` and `blame` answers, unchanged query
  latency and derivation at least as fast. The derived kinds are now
  `history-tree-v4` and `history-node-v4`: rederive with `repair rederive
  --kind history-tree-v4` before switching servers, then `repair drop --kind
  history-tree-v3` (OPERATIONS.md, "Upgrading").
- Cold reads of many objects are batched: `BlobStore::get_many`, which the
  filesystem store splits over up to 16 tasks that ask the kernel for every
  pack frame of their delta chains before decoding; breadth-first `files` and
  `diff` walks that read each tree level at once; a blame walk that reads no
  files while it skips through the path-history index, then reads parents'
  files, cached blames and versions in batches; and a faster pack index check.
  On a 57,345-changeset Linux-shaped packed store, cold: the full listing
  2.2 s to 0.51 s, a deep diff 3.1 s to 0.73 s, and a first `blame Makefile`
  10.5 s to 5.0 s (PERFORMANCE.md, "Cold queries"). New example
  `cold_queries`.
- Faster code search and long logs, with unchanged answers. A search opens
  a directory with one batched read of its subdirectories' filters, one of
  its files' trigram rows and one of the surviving subdirectories' nodes,
  opens subdirectories ahead of the walk four at a time, and matches files
  on blocking threads. A long `log` reads two read-ahead batches at once on
  separate tasks, batched changeset reads no longer clone each changeset,
  and a log of more than 512 entries is encoded in parallel chunks and
  streamed (the same JSON, byte for byte). On an 82,765-file Linux-shaped
  tree a search for a token near the end of the tree went from 229 to 36 ms
  warm and 324 to 140 ms cold; on all of git/git a rare token 79 to 35 ms
  warm, and `log` limit 10,000 450 to 325 ms warm and 845 to 627 ms cold
  (PERFORMANCE.md, "Search and long logs"). Under truncation, a search's
  `dirs_pruned` also counts pruned siblings of the directories it entered.
- `leep-server repack` packs a filesystem blob store (ADR 0029): every live
  blob and tree node goes into one pack, each file version a zstd delta
  against the previous version at its path and each tree node against the
  same directory in the first parent, then after `--fence` (default 10m) the
  loose copies and old packs are deleted. Safe beside running servers and
  when killed. The first 3,795 commits of git/git go from 91.2 MB of loose
  objects to a 3.1 MB pack and a 1.1 MB index. The store reads loose files
  first, then packs (new `packs/` directory, layout in FORMATS.md); a store
  without packs behaves as before. GC deletes packed blobs through
  `packs/tombstones`, and the next repack drops them. Only filesystem stores
  can be packed so far; backups copy packed blobs as loose ones.
- Pack indexes are about half the size, on disk and in memory (ADR 0038).
  New packs get a `LEEPIDX2` index: hashes in a sorted column behind a
  256-entry fanout table, then one record per hash whose field widths are
  chosen per pack, with bases as record ranks instead of 32-byte hashes and
  modification times relative to the pack's earliest. On the first 3,795
  commits of git/git the index takes 0.51 MB instead of 1.13 MB (43 bytes
  per object instead of 96). At 5 million objects it takes 46 bytes per
  object in memory instead of 104, loads in 1.5 s instead of 11 s, and
  answers a lookup in 0.9 us instead of 1.8 us. Existing `LEEPIDX1` indexes
  still read, now in place as well, and a full repack rewrites them. Upgrade
  every server before repacking: older servers refuse the new indexes.
- `leep-server repack` is incremental by default (ADR 0039). It packs only
  loose objects, walks back from the refs only through the history they
  belong to, and merges small packs by a geometric rule (`--geometric 2`,
  `--max-packs 8`), copying merged entries without recompressing them.
  `--full` keeps the previous behaviour. On a 12,000-commit synthetic
  history, a run after 100 new commits takes 0.4 s against 15 s for a full
  repack, and the store never held more than 5 packs. Object stores are
  still packed in full.
- Backups copy packs as files (ADR 0039). `backup` copies each pack and
  index the pool lacks, checked against the index's recorded hash, plus
  their tombstones, then the loose objects as before. It prunes pool packs
  a source repack merged away once other pool packs cover them. `restore`
  copies the packs back, so a packed store restores packed. Pools from
  before this change still restore. The manifest's `blobs` section gains
  pack counters.
- `leep-server repack` packs `s3://` blob stores too (ADR 0040), with the
  same pack files at `packs/<id>.pack` and `packs/<id>.idx` in the bucket.
  Servers read packed objects with ranged GETs and see a new pack on their
  next miss; GC deletes packed blobs with one empty tombstone object each;
  one repack runs at a time through a lease object (`packs/repack.lease`)
  written with conditional PUTs. Upgrade every server that reads the bucket
  before the first repack: older servers do not read packs.
- `leep-server repack` holds each hash once and refers to objects by `u32`
  ids everywhere else, streams the new index to its file instead of holding
  it, and compresses on every CPU (`--threads`, default one per CPU) with
  the same frame choices, so packs and indexes are byte-identical to before.
  A full repack of a packed store takes 143 bytes per object instead of 462
  (3M synthetic objects: 601 MB peak instead of 2,230 MB), projecting about
  1.6 GB for all of Linux instead of 6.2 GiB. Full repacks of imported
  stores run about 3x faster (Linux to v2.6.16: 71 s instead of 220 s); the
  walk reads changesets and root trees in batches. `repack_publish` takes
  `impl Into<PackHints>` (`leep_core::intern`), and `pack_order` returns
  `PackHints`. New example `repack_scale`.
- `leep-git import` reads and uploads a commit's new blobs in 8 MB slices
  instead of all at once, reopens the git repository every 20,000 commits so
  mapped pack pages are released, and restarts itself with
  `MALLOC_ARENA_MAX=2` (glibc). Importing Linux to v2.6.16 peaks at 212 MB
  instead of 496 MB and takes 3:56 instead of 5:30.
- `log` without a path, `log` filtered by author or `extra`, and
  provenance read history in batches instead of a changeset at a time
  (ADR 0030): on the first 3,795 commits of git/git on Postgres, a log of the
  whole slice takes 0.10 s instead of 0.93 s and a 100-entry log 7 ms instead
  of 27 ms, with the same entries in the same order. `MetaStore` gains
  `get_changesets` and `get_derived_many` (provided methods that loop over
  the single reads, so other implementations keep compiling); Postgres and
  SQLite answer each with one query.
- Storing a changeset takes one round of batched reads and one write
  (ADR 0031): the changeset with its root tree, generation and skiplist rows,
  and a rebase's link or the git importer's mapping, in one statement on
  Postgres and one transaction on SQLite, so a changeset never appears
  without them. Tree node and blob uploads check GC condemnation in batches,
  and skiplists are cached. On the first 3,795 commits of git/git on Postgres,
  `leep-git import` takes 14 s instead of 39 to 43 s (7.8 statements per
  commit instead of 38.3), `POST /v1/changesets` 4 to 5 ms instead of 9 to 13
  ms, and a land that replays a changeset 7.5 to 9 ms instead of 13 to 17 ms;
  the data written is byte-identical. `MetaStore` gains `put_derived_many`
  and `put_changeset_with_derived` (provided methods; batched rows are
  written in `(kind, key)` order so concurrent batches cannot deadlock).
- `merge_base` on merge-heavy histories no longer walks the whole history on
  a fresh server: it stops at the first changeset both sides reach (the
  answer, same as before), returns an input that is a first-parent ancestor
  of the other after a few skiplist reads, and reads the walk in batches.
  On git/git, `main` against `master~1000` took 70 s cold; on the first
  3,795 commits it takes 6 ms instead of 0.92 s and 8 statements instead of
  7,385. `is_ancestor`'s fallback search reads in batches too.
- Concurrent lands on one ref are group committed (ADR 0032): a server queues
  them per ref, and one leader replays a batch of up to 64 in arrival order,
  each onto the previous one's result, then moves the ref once while logging
  one ref log entry per land. Every land gets the answer, and the log the
  entries, it would get if the lands ran one after another; a conflicting or
  refused land is answered alone and the rest still land. With 64 agents
  landing on one ref, Postgres takes 154 lands/s instead of 41 (p99 0.4 s
  instead of 1.7 s); a lone land is unchanged. The slowdown under contention
  was each land's search for an earlier copy of its stack, which the lands of
  a batch now share. `MetaStore` gains `cas_ref_logged_chain` (a provided
  method of consecutive swaps; memory, SQLite and Postgres apply the chain in
  one transaction). New metrics `leep_land_batch_size` and
  `leep_land_cancelled_total`; `leep_land_lock_wait_seconds` now measures the
  time queued. `leep-land-bench` measures land throughput by number of agents.
- Lands on one ref are batched by one server however many servers share the
  metadata store (ADR 0056), with `--land-coordination on` (off by default
  until it is measured on separate hosts; see `docs/OPERATIONS.md`). A per-ref lease
  in the new `leases` table (migration 0006, scope `land`; other kinds of work can reuse it) names the server that
  batches a ref's lands; the others forward lands to it over HTTP with the
  client's own body and token, only to addresses on an allow-list
  (`--advertise-url`, `--peer-url-prefix`, `--peer-cidr`). Before, two or
  three servers landing on one ref left most agents waiting 20 to 30 s while
  one server won every swap. New metrics `leep_land_leases_held`,
  `leep_land_lease_acquired_total`, `leep_land_lease_lost_total`,
  `leep_land_forwarded_total` (with a `reason` on failures) and others, and alerts
  `LeepLandForwardFailing` and `LeepLandLeaseFlapping`.
- A land batch redone after losing its swap keeps the rebase links it
  already read from the store, so its search for earlier copies of its lands
  reads only what the head gained since. Against another server landing on
  the same ref, that search was half of each losing attempt and grew with
  every loss.
- A land batch stages its replays (ADR 0043): replayed copies, their tree
  nodes and rows are kept in memory and read back by the lands after them,
  then stored with one wave of blob writes and one metadata write just before
  the ref moves, and the batch reads its lands' changesets, root trees and
  rebase links in one round of batched reads. With 64 agents on one ref a
  local Postgres run takes 215 to 230 lands/s instead of 58 to 70 (land p50
  0.18 s instead of 0.9 s), SQLite 199 instead of 90 to 96; a lone land is
  unchanged, and so are durability and the order objects are stored in.
  `MetaStore` gains `put_changesets_with_derived` (a provided method; memory,
  SQLite and Postgres write everything atomically).
- Faster uploads and missing-blob checks (ADR 0044). The filesystem blob
  store no longer fsyncs a blob's directory on every put: it records the
  directory, and `BlobStore::flush` (a new provided method) syncs every
  recorded directory in one wave shared by concurrent callers before a
  changeset that names the blobs is stored, a land batch stores its copies,
  or a backup finishes. A create flushes its tree nodes and the contents it
  names together. Stored changesets keep their guarantee that their blobs
  survive power loss; an answered upload alone no longer has it.
  `POST /v1/blobs/missing` checks existence in batches
  (`BlobStore::contains_many`, a new provided method) and reads condemnation
  rows in parallel chunks, keeping each hash's fence, existence and claim in
  order: 90,000 unknown hashes take 0.34 to 0.44 s instead of 7 to 11 s on a
  loaded local machine (2.2 to 2.9 s on the VPS before). Uploads write their
  claim alongside the condemnation read; upload p50 with 64 agents on one
  ref halves. Fixed: two puts creating the same new shard could both skip
  syncing the store root, and a loose copy a repack puts back was not synced.
- Creates no longer wait for other requests' uploads to be synced (ADR 0048).
  `BlobStore::flush_blobs` (a new provided method) makes only the given blobs
  durable; tree writes, land batches and rescues use it, and the filesystem
  store syncs each needed shard directory on its own, sharing syncs already
  running, instead of one wave of every directory dirtied since the last.
  At 64 agents a create's flush takes 3 to 6 ms instead of 20 to 44 and mean
  server time per create falls about 25%. Durability is unchanged.
- GC marks incrementally (ADR 0033): each `gc` run records what it marked in
  `gc-mark-v1` rows, written so that a row always vouches for everything below
  it, and the next run walks only changesets stored since and stops at marked
  tree nodes. Ref and tag moves never force a full walk, since liveness comes
  from stored changesets. `collect` reads condemnation rows in batches. On
  21,000 commits of git/git a GC after 10 new commits takes 1.7 s instead of
  59 s (packed store). `gc --full` walks everything and rewrites the record;
  `repair drop` refuses the kind, and `verify` checks its rows. New example
  `gc_profile` times GC phases and counts store calls.
- Filtered logs use an index (ADR 0047). `GET /v1/log` with `author=` or
  `extra=` and no path (`leep log --author`, `--agent`, `--agent-model`,
  `--agent-session`) used to test every ancestor, so a rare value read the
  whole history (151 s for one author on all of Linux). A new derived kind,
  `log-filter-v1`, stores per changeset the fingerprints of authors,
  `extra` keys and `agent.model`/`agent.session` values for first-parent
  stretches of power-of-two lengths, side branches included, and the walk
  skips stretches that cannot hold a match; names behind author
  fingerprints (`log-filter-name-v1`) keep `author` a case-insensitive
  substring. Answers, order and `limit` are unchanged. On a 200,000-changeset
  synthetic history in Postgres a rare author or session takes 60-70 ms warm
  and 0.3-0.7 s cold instead of 5-7 s; frequent values keep the scan. 859
  bytes per changeset there. The warmer derives it; `verify` checks it,
  `repair rederive`/`drop --kind log-filter-v1` handle it. New example
  `log_filter_profile`.
- Writing a derived row that is already stored with the same bytes no
  longer rewrites it, so duplicate derivations (several servers warming,
  a query and the warmer) add no dead row versions or WAL on Postgres.
- The index warmer follows the ref log (ADR 0058, phase 0). It used to
  list every ref every 30 s, so on a busy ref the path-history and
  log-filter indexes trailed the head by up to 30 s of lands before any
  derivation started (890 generations at 2,000 swarm agents). A pass now
  reads the ref log entries newer than the last it saw, one indexed query
  when nothing moved, and lists every ref every 5 s as a backstop. All
  heads that moved derive as one range per kind, so shared history is
  derived once and new changesets of different refs derive side by side. A
  ref whose warming fails is retried by the next full listing.
- **Default changed:** `--warm-interval` (`LEEP_WARM_INTERVAL`) is now
  `250ms` instead of `30s`, and accepts milliseconds (`250ms`) as well as
  `s`, `m`, `h` and `d`. A server that set `30s` explicitly keeps polling
  every 30 s. With several servers on one metadata store, set `0` on all but
  one until index leases exist (ADR 0058), or every server derives the same
  heads at once.
- New histogram `leep_warm_delay_seconds`: time from a ref move to its
  indexes being stored, at 1-second resolution (ref log times are whole
  seconds), recorded only for moves the ref-log tail found.
- Derivation uses several cores (ADR 0050). A missing range (the warmer's
  catch-up after an import, a first query over un-derived history, `repair
  rederive`) used to derive one changeset after another on one core: about
  nine hours for `history-tree-v4` on all of Linux. Changesets whose parents
  are done now derive side by side, rows are stored in an order that keeps
  every changeset after its parents (so a stopped run resumes cleanly), and
  changesets are read and prepared a window ahead. Rows are byte-identical
  for any number of jobs. `leep-server --derive-jobs N` (`LEEP_DERIVE_JOBS`)
  and `repair rederive --jobs N` set it (default: the cores, up to 8); the
  warmer derives `history-tree-v4` and `log-filter-v1` side by side. New
  `repair rederive --missing` derives only absent rows, without recomputing
  stored ones, so an initial build can be stopped and resumed. On a 4-vCPU
  machine shared with other work, 4 jobs derived a 20,000-changeset
  late-Linux-shaped history in Postgres about twice as fast, the full
  `rederive` about three times as fast, and `rederive --missing` six times
  as fast as the old `rederive`. A name row dropped by a failed derivation
  is now written by the next one (it was lost for the life of the process).
  `history_profile` takes `--jobs` and `--log-filter`.
- Log filter rows at merges come from one walk of both parents' histories
  (ADR 0051) instead of a `merge_base` per merge inside the side, which
  stalled the build for hours at Linux back-merges bringing in a release.
  A walk past 200,000 changesets stores a superset from the side's own
  levels, so every merge is bounded. Rows are otherwise unchanged; stores
  built before resume with `repair rederive --kind log-filter-v1 --missing`.
  `history_profile --releases C` builds a history shaped like Linux's
  release cycles and `--time-back-merges [--cold]` times its back-merges.
- Changeset rows compressed with a dictionary trained on the store's own
  history (ADR 0053): `leep-server compact-changesets` trains and stores the
  dictionary (schema migration 0003 adds `changeset_dicts`), rewrites existing
  rows in place, and servers write new rows compressed from then on. Ids,
  hashes, the API and `Changeset` are unchanged, and every row is still
  checked against its id when read. The `changesets` table shrinks to about
  55-60%: 19.9 to 10.9 MB on git/git v1.7.0 and 262 to 159 MB on the last
  117,680 Linux commits (about 2.9 to 1.75 GB projected for all of Linux),
  with derivation and `log` speed unchanged within noise. **Upgrade:** the
  migration tolerates old servers, but run `compact-changesets` only once
  every server is on this version (OPERATIONS.md, "Upgrading"). Backups
  carry the dictionaries. The `changeset_compression` example measures
  encodings on a store's rows or on a git history.
- `leep-server upgrade-index` upgrades a store's derived indexes in one
  command instead of the manual rederive, switch, drop and reclaim steps.
  Without `--yes` it plans and changes nothing: per current kind the rows
  stored against the changesets reachable from refs (missing, partial or
  complete, from the fast count of ADR 0052 and the ref heads' rows), the
  retired kinds that still have rows with their size, estimated time and
  disk, the free space where it can see it (warning when short), and the
  command to reclaim space; exit status 1 when there is work. `--yes`
  derives what is missing through the `repair rederive --missing` path
  (`--jobs`, resumable, with rate and ETA), drops retired kinds only with
  `--drop-retired --old-servers-stopped` (`--drop-first` for a disk that
  cannot hold both), optionally runs `VACUUM (ANALYZE) derived`
  (`--vacuum`), and never runs `VACUUM FULL`. A run that is stopped
  continues where it was; a store that is up to date prints "nothing to do".
  No new formats or API.
- `repair drop` of a large kind is fast (ADR 0052). It walks the `derived`
  table in storage order, a range of pages per short transaction, instead of
  deleting in key order, which read pages at random and logged each page to
  WAL several times. In a cold-cache reproduction it deletes 35,000 rows a
  second instead of 6,100, with a fifth of the WAL; dropping
  `history-tree-v3` on Linux (26.9M rows) should take 10 to 20 minutes
  instead of 5.5 hours. The kinds of one command go in one walk, with rows,
  rate, position and ETA printed every ten seconds, and a small kind is still
  deleted from the primary key without reading the rest of the table.
  `--dry-run` counts from the index without reading values: 2 s instead of
  154 s in the reproduction, 38 minutes on Linux before. SQLite drops twice
  as fast. New `MetaStore::count_derived` and `delete_derived_step` (bounded,
  resumable steps); `delete_derived_kind` becomes a provided method over them.
- The first blame of a long-lived file walks its history in stretches:
  the first 32 steps from the tip as before, then up to 64 of about 256
  stretches of the first-parent chain at once, their reads gathered into
  batched reads, and the steps taken in order, so answers are unchanged.
  Line diffs number each version's lines once. On git/git, cold, a first
  `blame Makefile` (1,501 changes) went from 3.0 s to 1.1 s and `diff.c`
  from 1.9 s to 0.9 s, with rarely changed files as before
  (PERFORMANCE.md, "First blame in stretches").
- Format features (ADR 0055): schema migration 0004 adds `store_features`,
  and `compact-changesets` records `changeset-zstd-dict` before it stores a
  dictionary (the migration records it for a store already compacted).
  Servers and tools refuse to open a store that names a feature they do not
  know, so a later format change fails at startup on servers that cannot
  read it instead of failing their reads. Servers from before this release
  do not check: still run `compact-changesets` only after every server
  runs ADR 0053's release or newer. Backups carry the table.
- `--meta-pool-size` (`LEEP_META_POOL_SIZE`, default 16) sets the Postgres
  connection pool, and `--derive-cache-bytes` (`LEEP_DERIVE_CACHE_BYTES`,
  default 340 MiB) sets the budget of the derived-index caches, which were
  fixed. Defaults are unchanged (OPERATIONS.md, "Connection pool and
  caches").
- `upgrade-index` plans in seconds on large Postgres stores: kinds over
  100,000 rows are estimated from planner statistics (marked `~`) instead
  of `count(*)`, which took about 32 s a kind on all of Linux. Counts are
  exact where a decision rests on them, and `--exact-counts` counts
  everything. New `MetaStore::estimate_derived`.
- `verify` no longer reports `log-filter-v1` merge rows as damaged when they
  hold exactly what the merge brings in, as rows written before the
  side-walk budget did (ADR 0051). It notes them instead.
- `leep-land-bench` drives several servers that share one database
  (`--server` and `--metrics` take lists) and reports lands per server and
  agents that made no progress. With 2 or 3 servers landing on one ref, one
  instance wins every swap and the other instances' lands wait 20 s or more
  (PERFORMANCE.md, "Several servers on one ref"); route a hot ref's lands to
  one instance.
- `leep-swarm` (in `leep-soak`) is a many-agent workload generator:
  thousands of async agents with think time, a read mix (log, files, blame,
  search, diff), working-copy syncs, uploads, creates on stale bases and
  lands, some on shared hot files, against one or more servers. It reports
  lands per second, p50/p99 per operation, 409s, conflicts, stalled agents
  (a land sent and none answered in the window) and incomplete cycles,
  lands per server, warmer lag and database bytes per land (write path and
  read-built indexes apart), and checks history for lands that landed twice
  or got lost (PERFORMANCE.md, "Many-agent workload", with a smoke test; the
  baseline is still to run).

### Git compatibility (M7)
- Live git mirror (ADR 0046): `leep-git mirror run` keeps a git remote
  (GitHub, GitLab, any https, ssh or file remote) in step with a Leep
  server. Lands reach the remote within about a second.
  - It uses only the HTTP API and the system `git`, so the server gains no
    git code and needs no change.
  - Exports are incremental and deterministic, and match `leep-git export`,
    plus a `Leep-Changeset:` trailer on changesets made in Leep.
  - Pushes are fast-forward only and never forced.
  - With `--import`, fast-forward pushes to the remote land in Leep through
    a ref compare-and-swap that the land policy checks.
  - When both sides moved, the ref is reported `diverged` and Leep's side is
    pushed to `leep-diverged/<branch>` for a merge in git; nothing is
    resolved automatically.
  - Crash safe at every step: decisions come from current values, a repeated
    push is a no-op, and imports are content addressed.
  - `leep-git mirror status`, `--listen` (`/metrics`, `/status`, `/poke` for
    webhooks), and three alerts with runbooks: `LeepMirrorDown`,
    `LeepMirrorLagging` and `LeepMirrorStuck`.
- The git mirror can merge and land (ADR 0049), both opt-in:
  - `--on-divergence merge`: when both sides moved, the mirror imports the
    remote's side, asks `POST /v1/merge` for a merge and lands it by ref
    compare-and-swap if it is clean and the land policy allows, then pushes
    it as a fast-forward. Conflicts and refusals fall back to the
    `diverged` report; nothing is resolved automatically. The merge is
    deterministic (`--merge-author`, a fixed message, the newer parent's
    time), so a crash at any step reproduces the same commit.
  - `--land-branches`: a push to `leep-land/<ref>/<name>` is imported and
    landed onto `<ref>` through `POST /v1/land`, rebased like an agent's
    land, and the branch is deleted with an exact lease. Conflicts and
    refusals stay in `status.json` (`land_branches`) and the new metrics
    `leep_mirror_land_branches_total` and
    `leep_mirror_land_branches_failing`, with the alert
    `LeepMirrorLandBranchFailing`.
  - New metric `leep_mirror_merges_total`; new crash-test steps for
    `LEEP_MIRROR_CRASH_AT`.
  - A changeset whose export is not the commit its `git.commit` names (a
    rebased copy of an imported changeset) now gets the `Leep-Changeset:`
    trailer. Commits of every changeset the mirror exported before are
    unchanged.

### Fixes
- A damaged compressed changeset row whose frame header claims a large size
  no longer makes a read reserve that much memory (up to 256 MiB a row).
  Readers reserve at most 1 MiB on the header's word and decode larger
  frames as a stream, growing the buffer only as bytes come out.
- `leep-git import` and `export` no longer fail with "Entry too large to fit
  in memory" on objects over 16 MiB when the git repository belongs to
  another user (as in the server container): gix's reduced-trust allocation
  cap is replaced by an explicit 4 GiB limit. Found importing Linux.
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
- The filesystem blob store fsyncs the shard directory after renaming a new
  blob into place (and the store root when the shard is new), so a power
  loss can no longer drop a blob a stored changeset names. (Since ADR 0044
  the sync happens at the next flush, before such a changeset is stored.)
- A delete of a packed blob after an earlier tombstone append died part way
  could be lost: the new line was appended to the torn one and readers
  dropped the joined line. The append now cuts the torn line off first,
  under the tombstone lock.
- Landing a changeset built on one that was already landed as a rebased
  copy (land `a`, which lands as `a'`, then land `b` on the original `a`)
  replayed `a` again: an empty duplicate of `a` on the ref, or a false
  conflict if the ref had since changed `a`'s files. The stack walk now
  stops at an entry whose copy is already on the ref.
- MCP `leep_commit_and_land` can be retried safely. A retry after a timeout
  used to build a new changeset (with a new timestamp) on the moved head,
  landing a second, empty copy of the change. It now takes `changeset` (land one that exists;
  ambiguous failures name it) and `request_id` (recorded as
  `agent.request_id`; a repeat lands the changeset the first call made).
- The crash test's HTTP agents land the same changeset again after an
  unanswered land, as a real client does, instead of rebuilding the stack on
  the new head, which landed it twice when the first land had applied.
- Batched derived-row reads on Postgres no longer slow to seconds when an
  index has grown since the last `ANALYZE`: the planner estimated a row or two
  for the kind and scanned all its rows instead of looking the keys up. Found
  deriving path history on Linux (2.3 s per lookup, now 3 ms).
- Postgres statistics no longer go stale while an index is built or after a
  bulk load (ADR 0042). Schema migration 2 makes autovacuum re-analyze
  `derived` after 1% plus 10,000 changed rows (it waited for 10%, 1.1M rows on
  Linux, which a new 512k-row kind never reached) and `changesets` after 2%
  plus 10,000; `leep-git import` and `leep-server restore` run `ANALYZE` when
  done. Deleting a derived kind (`repair drop`) no longer stalls on a kind
  added since the last `ANALYZE`: with a one-row estimate Postgres compared
  every row of the kind with each batch, taking 17 s for a 100-row batch on a
  300,000-row kind and minutes for a full one; each batch now takes 30-50 ms
  whatever the statistics. Servers older than this change refuse the
  migrated database, as for any migration.

### Project
- Workspace version set to 0.0.1, with a CI check that keeps it below
  0.1.0.
- Apache-2.0 license text, security policy and this changelog.
- Product direction (`docs/PRODUCT.md`) and a draft contributor license
  agreement (`CLA.md`). Pull requests are merged by the maintainer only.
