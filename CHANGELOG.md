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
- GC marks incrementally (ADR 0033): each `gc` run records what it marked in
  `gc-mark-v1` rows, written so that a row always vouches for everything below
  it, and the next run walks only changesets stored since and stops at marked
  tree nodes. Ref and tag moves never force a full walk, since liveness comes
  from stored changesets. `collect` reads condemnation rows in batches. On
  21,000 commits of git/git a GC after 10 new commits takes 1.7 s instead of
  59 s (packed store). `gc --full` walks everything and rewrites the record;
  `repair drop` refuses the kind, and `verify` checks its rows. New example
  `gc_profile` times GC phases and counts store calls.

### Fixes
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
- The crash test's HTTP agents land the same changeset again after an
  unanswered land, as a real client does, instead of rebuilding the stack on
  the new head, which landed it twice when the first land had applied.
<<<<<<< HEAD
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
=======
- Batched derived-row reads on Postgres no longer slow to seconds when an
  index has grown since the last `ANALYZE`: the planner estimated a row or two
  for the kind and scanned all its rows instead of looking the keys up. Found
  deriving path history on Linux (2.3 s per lookup, now 3 ms).
>>>>>>> 1cbe85245467b16a60ef54b3cfd0dd518e6e9b33

### Project
- Workspace version set to 0.0.1, with a CI check that keeps it below
  0.1.0.
- Apache-2.0 license text, security policy and this changelog.
- Product direction (`docs/PRODUCT.md`) and a draft contributor license
  agreement (`CLA.md`). Pull requests are merged by the maintainer only.
