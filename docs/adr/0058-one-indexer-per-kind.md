# 0058. One indexer per kind, and queries that do not derive
Status: Accepted (phases 0 and 1 implemented; see "Implementation notes")
Date: 2026-10-20

## Context
Indexes other than the write path's rows (`root_tree`, `generation`,
`skiplist`, written with each changeset, ADR 0031) are built in two places,
both per process:

- **The warmer** (ADR 0028). Until phase 0 of this ADR, every server listed
  every ref every `--warm-interval` (30 s) and derived `history-tree-v4` and
  `log-filter-v1` for each moved head, and the search rows of its tree.
- **The query path.** `log` with a path, `last-change`, `blame`, filtered
  `log` and `search` call `derived::derive` for the head they were given. A
  head the warmer has not reached is derived inside the request, under the
  request timeout.

The only coordination is `Repo::derive_locks`, a mutex per kind per
process. N servers on one store derive the same ranges N times (ADR 0028
says so). ADR 0050 made each derivation parallel, which does not change who
derives.

The many-agent smoke test (`leep-swarm`, PERFORMANCE.md "Many-agent
workload") shows the cost. With one server and 2,000 agents, path history
trailed the head by 890 generations (p50, max 1,448) and the log filter by
1,020 (max 1,578). 1,063 searches, 1,165 logs and 799 blames timed out at
30 s. Much of the lag was the poll itself: at 22 lands/s, a 30 s interval
leaves up to 660 generations between passes before any derivation starts.
The rest was derivation competing with thousands of requests for four
shared cores, and queries waiting on the kind's lock, or deriving the same
range themselves.

An architecture review for 500 to 500,000 agents on one repository
recommended "one elected indexer per kind (lease in the store) with
derive_jobs across its cores; servers stop warming. Query servers read rows,
and a missing range returns 'indexing, retry-after' instead of deriving
inline." A first draft of this ADR was reviewed; "Review changes" lists
what the review changed.

Facts this design rests on:

- **Rows are pure and idempotent.** A row is a function of the changeset
  and its parents' rows (ARCHITECTURE.md section 5), so two processes that
  derive the same changeset write the same bytes. Since phase 0, writing
  identical bytes again changes nothing. Every stored row has its parents'
  rows, so a stopped derivation leaves a parent-closed set that the next one
  continues (ADR 0050).
- **Indexes only accelerate.** Every index-backed answer has an exact
  definition without the index:
  - `log_scan` is the oracle for path logs and filtered logs;
  - search is "identical with or without the index";
  - blame without the index visits every first-parent step.

  Rows decide speed, never answers.
- **A ref built by lands is a chain.** Lands rebase, so an agents' trunk is
  a first-parent chain one changeset wide. The wavefront has nothing to run
  side by side on it, and one ref's index grows at the rate one derivation
  follows its own chain.
  - **Measured** (synthetic, fsync off, 1–3 files per changeset, single
    runs on a loaded VM; see "Hot chain rate" in PERFORMANCE.md): 160 to 190
    changesets a second following one at a time on Postgres, and 380 to 460
    a second catching up in batches. On those chains that is above the 200
    to 250 lands a second one ref accepts (ADR 0056).
  - **Not measured:** real histories' contents, or a store loaded by a
    swarm.
- **Derived rows are shared across repositories** (ADR 0054 section 5).
  Only ref heads are per repository.
- **ADR 0056 owns leases.** PR #106 (migration 0006, merged as 8171c42)
  generalises `land_leases` into one
  `leases(scope, repo_id, name, shard, holder, address, protocol, meta,
  epoch, renewed_at, expires_at)` table, primary key `(scope, repo_id, name,
  shard)`. It comes with:
  - the `MetaStore` methods `supports_leases`, `lease`, `acquire_lease`,
    `renew_leases` and `release_lease`, keyed by `LeaseKey`, with the timing
    passed on each call;
  - `check_lease_contract`;
  - a dedicated pool of two connections for lease statements on Postgres;
  - each lease taken by one conditional statement, with times from the
    store's clock;
  - an epoch that grows on every change of holder;
  - a live lease never stolen;
  - the holder's view of its lease taken from the store's answer.

## Decision

### 1. Kinds by who builds them
| Class | Kinds | Built by |
|---|---|---|
| Write path | `root_tree`, `generation`, `skiplist` and, new, `last_merge-v1` | `store_changeset` (ADR 0031) |
| Indexes | `history-tree-v4` with `history-node-v4`; `log-filter-v1` with `log-filter-name-v1`; `blob-trigrams-v1` with `tree-trigrams-v1` | the index lease holder (section 2) |
| Result caches | `blame` | the query that computes the blame, as today |

**`last_merge-v1` moves to the write path.** Its value comes from the
parents' values and whether the changeset is a merge, at constant cost and
with no format change. With it, every kind that ancestry queries need
(`merge_base`, `is_ancestor`) exists when the changeset does. A create must
never derive history inline, so a missing parent row is handled like this:

- `store_changeset` reads the parents' `last_merge-v1` rows in the batched
  read it already makes for generations and skiplists.
- A parent lacking its row gets one more batched read: up to 256 of its
  first-parent ancestors, as `derive_missing` reads them. If that finds a
  row or a merge, the value follows without deriving anything. Otherwise
  the new changeset is stored without the row, which stays lazily derived
  as today, and `leep_create_last_merge_skipped_total` counts it.
- The gap closes from both sides:
  - `upgrade-index` lists `last_merge-v1` among the kinds it backfills, and
    the rollout (section 7) runs it before any server leaves `local` mode;
  - the history group's holder derives `last_merge-v1` as a dependency, as
    the warmer does;
  - `verify` reports reachable changesets without one.

### 2. Who builds indexes: one lease per index group
An **index lease** is a row of ADR 0056's `leases` table:

- **`scope = 'index'`**;
- **`repo_id = 0`**: index rows are store-wide (ADR 0054);
- **`name`** is the group's primary kind, versioned: `history-tree-v4`,
  `log-filter-v1` or `tree-trigrams-v1`;
- **`shard = 0`**: shards are deferred, section 4;
- **`address`** is the holder's `--advertise-url` when it has one, for
  operators only; nothing is forwarded;
- **`meta`** carries the holder's recent rate and backlog per group, which
  is what `Retry-After` and waits use (section 5).

The holder is the only process that builds that group, for every repository
and ref. It takes, renews and releases the lease through PR #106's methods
with `LeaseKey { scope: "index", repo_id: 0, name, shard: 0 }`, built
explicitly rather than with `LeaseKey::new`, which names the default
repository (1), passing the
timing below on each call. No index table of its own is added, and no
`MetaStore` method.

- **Timing of the `index` scope:** TTL 15 s, renew every 3 s, and the lease
  may be taken once its renewal is 6 s old (`stale_after`). Indexing
  tolerates seconds of handover, while a new holder starts with cold caches,
  so flapping would cost more than a short failover saves.
- **Candidates.** Servers in `--indexing lease` mode compete for every group
  they may hold (`--index-kinds`, default all). They take free leases on
  each renewal tick, never take a live one, and release on shutdown. A
  dedicated `leep-server indexer` process is deferred (section 7).

**The loop** (`Warmer`, the same code in every mode). Phase 0 has
implemented steps 1 and 2.

1. **Learn what moved by tailing the ref log.** Every `--warm-interval`
   (default 250 ms), a pass reads the entries newer than the last it saw,
   newest first, 256 per round trip, which is one indexed query when nothing
   moved.
   - **Commit order is `seq` order.** Postgres takes a global advisory lock
     (`LOG_LOCK`) after changing a ref and before inserting its log row,
     held to commit, and SQLite and memory have one writer. An entry
     committed after a read therefore never carries a `seq` below what that
     read saw, and the tail cannot skip one.
   - **Backstop.** Every ref is listed every 5 s anyway, and the tail falls
     back to a listing when more than 4,096 entries arrived between passes.
     A ref whose warming failed is retried by that listing, not on every
     pass.
2. **Derive every moved head of a pass as one range per group**
   (`derived::derive_heads`). The union of the heads' missing changesets
   goes on one ADR 0050 wavefront: shared history once, different refs'
   chains side by side, rows staged and written as before, so every prefix
   stored is parent-closed. Deriving the newest head of a busy ref once per
   pass is also what makes a chain index at the catch-up rate (section 8;
   synthetic, fsync off, 1–3 files per changeset, single runs on a loaded
   VM; see "Hot chain rate" in PERFORMANCE.md).
3. **Two lanes** (phase 1).
   - **Tip lane:** heads with at most 8,192 missing changesets.
   - **Backfill lane:** a fresh import, a new kind version, a restored
     store. It gets at most half the jobs.

   The lanes run at once, so a Linux backfill never holds agents' new heads
   behind it. Repositories take turns within a lane (ADR 0054 section 7).
4. **Hold the lease** (next; the `leases` table merged with PR #106). In `lease` mode
   a pass runs only for the groups this process holds.
   - The holder checks its lease view before each staged write batch and
     between passes. On losing the lease it abandons the wavefront at the
     next window (`DeriveWave::abandon`, as on an error).
   - Writes in flight complete. They are correct rows, so a stale holder's
     write is wasted work, never damage. Section 6 covers what that means
     for tools that delete rows.

Inside a process the per-kind `derive_locks` stay, so the loop and any
query fallback never derive one range twice.

### 3. What query servers do when rows are missing (phase 1, modes `lease` and `off`)
Query servers stop deriving index ranges. They answer from the rows that
exist, and compute the un-indexed part the way the index-free definition
does, storing nothing, up to a budget.

- **The un-indexed part.** Because rows are parent-closed, the changesets
  lacking a row are exactly those above a set of indexed ancestors, the
  *frontier set*.
- **Finding it.** `derive_missing`'s batched walk, stopped at the budget.

| Endpoint | Index | Missing part within budget | Budget flag (default) |
|---|---|---|---|
| `GET /v1/log/{id}?path=` (also `follow`), `GET /v1/last-change/{id}` | `history-tree-v4` | **Hybrid walk.** An un-indexed changeset is examined directly: changed relative to its first parent at the path, tested on `changes` keys and confirmed with tree entries as in `log_scan`. Its parents are pushed. Below the frontier set, the indexed walk takes over from several roots on the same heap. | `--query-scan-budget` (1,024 changesets) |
| `GET /v1/blame/{id}` | `history-tree-v4`, to skip steps | Un-indexed first-parent steps are walked one by one (`Skip::Here`). Each reads the file at the step's parent, as a step without the index does today. | `--blame-scan-budget` (128 file reads) |
| `GET /v1/log/{id}` with `author`/`extra`, no path | `log-filter-v1` | The plain scan (today the first 512 changesets, ADR 0047) runs on through the un-indexed changesets, then hands over to the index at the frontier set. | `--query-scan-budget` |
| `GET /v1/search/{rev}` | trigram rows | A directory without a filter is entered without pruning, and a file without a row is fetched and matched. Both are exact, since filters only prune. | `--search-scan-budget` (512 files without a row) and `--search-scan-bytes` (16 MiB) |
| `log` without filters, `provenance`, `diff`, `files`, `tree`, `merge-base` | none, or write-path rows | unchanged | none |

The defaults are deliberately low. They come from batched-read costs
(about 7 ms per 256 changesets warm on git/git), not from a measurement of
the hybrid walk, and are raised once phase 1 measures it.

**What stays the same.** These are exactly what a fully indexed store gives:

- the entries or matches;
- their order;
- `truncated`;
- whether a `next` cursor is given.

Two things may differ:

- **Cursor text.** Any cursor resumes to the same remaining pages under
  either state, including after the index catches up between pages.
- **Search `stats`.** They describe the work done, so `dirs_pruned`,
  `files_pruned`, `files_scanned` and `bytes_scanned` differ when rows are
  missing. `files_considered`, `files_skipped` and `indexed` keep the indexed
  definition. `blob_rows_derived` and `tree_rows_derived` are 0, since
  nothing is derived. A new optional `files_unindexed` counts files matched
  without a row.

A query in these modes writes no index rows. The only rows it writes are
the `blame` result cache.

**Over the budget**, the request header `Leep-Indexing` decides:

- **`retry`.** The answer is `503` with `Retry-After` in seconds and the
  response header `Leep-Indexing: kind=history-tree-v4; behind=5210`.
  - The body is the usual error with a new optional field (FORMATS.md
    rule 4): `{"error": "indexing: ...", "indexing": {"kind": "...",
    "behind": 5210, "retry_after_ms": 4000}}`.
  - `Retry-After` is the backlog over the holder's rate from the lease's
    `meta`, clamped to 1 to 30 s.
- **`stale-ok`.** The answer is `200` for the newest first-parent ancestor
  of the requested head whose missing part fits the budget (usually the
  newest indexed one).
  - The response gains additive fields: `answered_at` (that changeset) and
    `behind` (generations between it and the requested head). They are on
    `LogResponse`, `LastChangeResponse`, `BlameResponse` and
    `SearchResponse`, omitted when the answer is at the requested head.
  - A cursor from a stale answer pins `answered_at`.
  - Within budget, a `stale-ok` request is answered at the head like any
    other.
- **No header** (every client from before this ADR, `curl`, scripts):
  1. **Wait for the indexer.** While a live index lease exists, the request
     waits for the missing part to come within budget. It re-runs the
     bounded walk once a second, for at most the lesser of `--index-wait`
     (10 s) and the request's remaining timeout minus 1 s.
     `--index-waiters` (64) caps concurrent waiters per process. A request
     that finds the cap full does not wait.
  2. **Scan more.** It then scans with the old-client budgets, 8 times the
     opted-in ones (`--legacy-scan-factor`). Within those it answers
     correctly, more slowly.
  3. **Over those as well**, it gets the same 503 an opted-in client would.
     Older CLIs report it as a retryable server error (ADR 0034). A server
     never derives inline while a live index lease exists. The old-client
     budget covers 8,192 changesets of lag by default; an index further
     behind than that is an incident that `LeepIndexLagging` reports.
- **No live lease for the group** (no candidate running, or every holder
  gone). Every client gets today's inline derivation at once. A missing
  indexer makes queries slower, never unavailable.

**The inline-derivation rule.** Whether a query may derive an index range
inline depends only on the lease, never on the header or on how long it has
waited:

- **A live index lease exists for the group:** inline derivation is
  disabled. This holds after `--index-wait` runs out too: the request goes
  on to the larger scan and then the 503, and never derives.
- **No live lease:** the query derives inline, as in `local` mode.

`leep_query_inline_derive_total` must therefore stay 0 while a lease lives,
and the swarm acceptance run checks that.

`leep_core::Error` gains `Indexing { kind, behind, retry_after }`, mapped to
503, and CLAUDE.md's error table gains the line.

**Why 503.** The service will answer later, and that is what 503 plus
`Retry-After` means to proxies and HTTP libraries. A 429 would blame the
caller's rate, and the CLI would take it for request limits. A 202 would
suggest a job resource that does not exist.

Two consequences of using 503:

- **Load balancers.** Many retry a 503 on another backend, or count it
  against a backend's health. RUNBOOKS.md says to exempt responses carrying
  a `Leep-Indexing` header from passive health checks and balancer retries.
- **Alerts.** `LeepServerErrors` subtracts
  `leep_query_indexing_answers_total`.

**Clients.**

- **CLI.** `leep log`, `blame`, `search`, `last-change` and `log --author`
  send `Leep-Indexing: retry`.
  - On an indexing 503 they print `indexing: N changesets behind, retrying
    in Ss` to stderr, sleep `Retry-After` (capped by the existing 10 s
    `MAX_RETRY_WAIT`) and retry for up to 60 s in all (`LEEP_INDEX_WAIT`).
  - Then they fail with the new error kind `indexing`: retryable, exit code
    `SERVER`, and `retry_after_ms` in the `--json` error.
  - `--stale-ok` sends `stale-ok` instead and prints `(answered at X, N
    behind)`.
- **MCP.** Tools send `stale-ok` by default and put `answered_at` and
  `behind` in the result. An agent usually prefers a minute-old log now to
  a current one later. A tool argument `fresh: true` sends `retry` instead.
  After one retry within 5 s, the tool returns an error naming the kind,
  the lag and when to retry.

**Additive API.** `GET /v1/ready` lists the feature `indexing-retry` when
the server honours the header. A successful indexed read carries
`Leep-Index-Scanned: N`, the number of un-indexed changesets it examined.
`GET /v1/index-status` waits for the frontier table (section 5).

### 4. Several indexers (deferred until measured need)
One lease per group gives up to three busy processes: path history, log
filter, search. Beyond that the design is ready, but not built:

- **Shards by repository.** `shard = hash(repo_id) mod S`, with `S` recorded
  in each lease's `meta`. A candidate whose `S` differs from a live row's
  refuses to start. Rows are shared (ADR 0054), so forks in different shards
  may derive common history twice. That is harmless, and rare once the
  parent repository is indexed.
- **Not per ref within a repository.** Refs share almost all history.
- **Not by generation range.** A changeset needs its parents' rows. The
  wavefront already uses all the width a history has.

Section 8 says when one process stops sufficing. Until a measurement shows
that, `shard` stays 0.

### 5. Freshness
- **Phase 0.**
  - `leep_warm_delay_seconds` (histogram): time from a ref move, by its log
    entry's whole seconds, until that head's indexes are stored.
  - `leep_warm_duration_seconds` per pass.
  - `leep_warm_refs_total{result}`.
- **With leases**, labelled by `kind` (a handful of values), never by ref:
  - `leep_index_lease_held{kind}`
  - `leep_index_lease_acquired_total{kind}`
  - `leep_index_lease_lost_total{kind,reason=taken|store}`
  - `leep_index_derived_total{kind}`
  - `leep_index_backlog_changesets{kind,lane}`
  - `leep_index_lag_seconds{kind}`: the age of the oldest ref move not yet
    indexed, from the ref log's timestamps

  The holder writes its rate (changesets a second over the last minute)
  and backlog into the lease's `meta` on each renewal. Query servers read
  that one row, cached for a second, for `Retry-After`.
- **Phase 1 query metrics:**
  - `leep_query_unindexed_scanned_total{route}`
  - `leep_query_indexing_answers_total{route,mode=retry|legacy}`
  - `leep_query_stale_answers_total{route}`
  - `leep_query_inline_derive_total{route}`
  - `leep_query_index_wait_seconds`
  - `leep_query_index_waiters` (gauge)
- **Alerts**, each with a runbook:
  - `LeepIndexLagging`: lag over 120 s for 10 min. Ticket.
  - `LeepIndexerMissing`: a group without a holder for 5 min while any
    server runs in `lease` or `off` mode. Page, because queries have fallen
    back to inline derivation.
  - `LeepIndexingAnswersHigh`: indexing 503s above 5% of indexed reads for
    10 min. Ticket.
- **Deferred:** an advisory `index_frontier(repo_id, ref_name, kind, head,
  indexed, ...)` table and `GET /v1/index-status`. They are useful for
  operators, but nothing in phases 0 and 1 needs them, since queries test
  rows and waits re-run the bounded walk.

**No back-pressure on land.** Lands never wait for indexing.

- Indexes accelerate reads, and every read has an exact fallback or a
  stale-ok answer.
- Slowing lands would turn an indexer outage, or a backfill, into a write
  outage for every agent.

### 6. Correctness
- **Duplicates are harmless and cheap.** Rows are deterministic, and since
  phase 0 an upsert of the bytes already stored leaves the row alone
  (`WHERE derived.value IS DISTINCT FROM excluded.value` on Postgres, `IS
  NOT` on SQLite). Batches insert in `(kind, key)` order, so racing writers
  queue instead of deadlocking (ADR 0031). Leases make duplicates rare, not
  impossible: a handover may derive one write window (256 changesets or
  4,096 rows) twice.
- **Crash mid-wavefront.** The stored rows are a parent-closed prefix (ADR
  0050). The next holder, after the 6 s stale time, plans from the heads and
  derives only what is missing.
- **Lease lost while paused.** The holder steps down at its next check.
  Staged rows are unstaged and its name rows unmarked, as on a derivation
  error. Rows it wrote are correct.
- **Fence for tools that delete or rebuild rows.** `repair drop`, `repair
  rederive --missing` and `upgrade-index --yes` take the group's lease for
  every shard. They refuse when a live holder has it, unless
  `--ignore-lease`, which allows duplicate work and correct rows (not for a drop; see "Implementation notes"). They keep
  the lease while they run.
  - **The fence.** Having taken the lease, a tool waits `--index-fence`
    before touching rows. The default is 60 s: the 6 s stale time, plus
    more than the longest staged write, at most 4,096 rows in four 1,024-row
    statements. A previous holder whose view had not yet expired may have a
    batch in flight that lands after the lease changed hands, as with GC's
    fence (ADR 0012).
  - **A drop must not race derivation.** A drop interleaved with derivation
    could leave a row whose ancestors' nodes are gone.
  - **Servers in `local` mode** take no leases. A drop of a current kind
    still needs them stopped, or not deriving that kind, as OPERATIONS.md
    says today.
- **Rolling upgrades.** The lease name is the versioned kind name. An old
  holder leads `history-tree-v4` while a new one leads `history-tree-v5`,
  with no protocol between them. `protocol` in the row lets a later change
  refuse to share a lease with an incompatible holder.
- **GC.** The holder reads only changesets and their trees, which GC never
  deletes. A hybrid search fetches blobs as a search does today, under the
  same `--request-timeout` and fence. Nothing changes for `gc --fence`.
- **`derive-search <rev>`** and `verify` are unchanged, apart from the
  `last_merge-v1` check of section 1.

### 7. Modes, flags and phases
`--indexing local|lease|off`, default `local`.

| Mode | Indexing | Queries | Use |
|---|---|---|---|
| `local` (default) | the warmer on every server: ref-log tail every 250 ms, `derive_heads` | derive inline, as today | single server; today's answers |
| `lease` | the warmer, only for the groups this process holds | phase 0: inline, as today; phase 1: bounded (section 3) | fleets: one server warms per group |
| `off` (phase 1) | none | bounded | query servers beside lease holders |

**SQLite and memory.** Both get the `index` scope through ADR 0056's
leases. With one process, `lease` gives it every lease, and it gets the
bounded queries. A separate indexer process against SQLite stays refused:
SQLite serves one process.

**Phase 0**, independent of the lease merge, done on this branch:

- `local` mode tails the ref log every 250 ms (`--warm-interval`, which now
  accepts `ms`), with a 5 s `list_refs` backstop;
- `derive_heads`;
- upserts skip identical bytes;
- `examples/chain_rate.rs`, the hot chain measurement.

**Phase 0**, now that ADR 0056's `leases` table has merged (PR #106): the `index` scope, the
`lease` mode (one server warms each group), lease metrics and the fence in
the repair tools.

**Phase 1:**

- bounded, non-deriving queries with `Error::Indexing`;
- `Leep-Indexing: retry` and `stale-ok`;
- the old-client rules;
- `off` mode;
- `last_merge-v1` on the write path, with its backfill in `upgrade-index`;
- the CLI and MCP changes, RUNBOOKS for balancers, the alerts;
- the swarm acceptance run.

**Deferred until measured need:**

- the `index_frontier` table and `/v1/index-status`;
- a dedicated `leep-server indexer` subcommand, which would be a `lease`
  candidate with no API listener;
- the two lanes, if the tip/backfill split is not needed in practice;
- repository shards;
- directory-dataflow derivation (section 8).

**Rollout.**

1. Run `upgrade-index --yes`.
2. Switch servers to `lease`, then query servers to `off`.
3. Old servers in `local` mode keep warming in the meantime. That costs
   duplicate work, and since phase 0 duplicate writes no longer rewrite
   rows.

To back out, return to `local`. Leases expire in 15 s.

### 8. Expected numbers
**Per chain** (synthetic histories shaped like git/git's tree and late
Linux's, Postgres 16 with fsync off, 1–3 files per changeset, single runs on
a loaded 4-vCPU VM; see "Hot chain rate" in PERFORMANCE.md):

| Derivation | Postgres | memory | CPU per changeset |
|---|---|---|---|
| `history-tree-v4`, following one at a time | 158 to 192/s | 897 to 1,340/s | 2.9 to 3.5 ms (Postgres), 0.6 to 0.9 ms (memory) |
| `history-tree-v4`, catching up in one derivation | 384 to 459/s | 1,123 to 2,285/s | 1.3 to 1.9 ms, 0.4 to 0.8 ms |
| `log-filter-v1`, following | 155 to 262/s | 3,670 to 18,977/s | about 2 ms, 0.05 to 0.2 ms |

- **Round trips set the chain rate.** Following makes 8 store round trips a
  changeset, and catching up makes 2. Jobs make no difference on a chain.
  The Linux shape costs about 1.5 times the git/git shape in memory, and
  the same against Postgres.
- **The first draft's estimate was too low.** It took 40 to 64 a second
  from whole-history builds (the soak's 15.6 ms; about 40/s on all of
  Linux), which are dominated by merges, cold caches and competing
  requests.
- **The warmer gets the catch-up rate.** It derives the newest head once
  per pass, so a busy ref is indexed at that rate: with 250 ms passes, a ref
  landing 200 times a second hands each pass about 50 changesets.

**Sizing.** "Cores" is path-history CPU at 1.3 to 3.5 ms a changeset. One
ref accepts about 200 to 250 lands a second (ADR 0056).

| Agents | Lands/s at 0.05/s per agent (smoke test) | Path-history cores | Lands/s at 0.1/min per agent | Path-history cores |
|---|---|---|---|---|
| 500 | 25 | under 0.1 | 0.8 | ~0 |
| 2,000 | 100 | 0.1 to 0.35 | 3.3 | ~0 |
| 5,000 | 250 (one ref's land ceiling) | 0.3 to 0.9 | 8.3 | ~0 |
| 50,000 | 2,500 (at least 10 refs or repositories) | 3.3 to 8.8 | 83 | 0.1 to 0.3 |
| 500,000 | 25,000 (at least 100) | 33 to 88 | 833 (at least 4 refs) | 1.1 to 2.9 |

The sizing moves 30 times between the two assumptions. 0.05 lands a second
is the smoke test's agent: a 2 s think time and about ten operations a
cycle. 0.1 a minute is an agent that lands once in ten minutes of work.
Neither has been measured on real agent fleets, so phase 1's acceptance run
reports the per-agent land rate it saw.

**When one lease holder stops sufficing:**

- **One chain.** A ref whose land rate exceeds its catch-up rate, about 400
  a second on the synthetic shapes. That is above one ref's land ceiling,
  so on those chains it does not bind. If a real history or a loaded store says otherwise,
  the follow-up steps are:
  1. **Measure hot chain rate** on real histories: git/git and Linux
     contents, changesets touching many files, and a store under swarm
     load. `chain_rate` with real imports.
  2. **Directory-dataflow derivation.** Along a chain, a directory's history
     node depends only on that directory's node at the previous changeset
     that touched it, and on its changed children's nodes at this
     changeset.
     - Tasks keyed by (changeset, directory), leaves first, can therefore
       run across changesets that touch disjoint directories.
     - The root node, and the pack assembly with its part cutting,
       `(writer, part, index)` addresses, id tables and patch or keyframe
       choice, run as a serial tail in chain order.
     - The rows stay byte-identical with no format change. Tests must
       compare every row with the serial build's.
- **One process.** The chains together need more than its cores: at
  1.3 to 3.5 ms a changeset, 8 cores derive about 2,300 to 6,000 changesets
  a second. That is roughly 50,000 to 120,000 agents at 0.05/s, spread over
  enough refs. Then add a group per process (three processes), and after
  that repository shards (section 4). The metadata store also takes 2 round
  trips a changeset per group, about 15,000 a second for three groups at
  2,500 lands a second. Size it for that.

### 9. Testing
- **Phase 0 (done).**
  - The upsert contract: identical bytes leave Postgres's `xmin` and a
    SQLite update trigger untouched, and new bytes replace them.
  - `derive_heads` against serial derivation, with 1 and 4 jobs.
  - Two warmers racing on one store store the serial rows.
  - Passes follow the ref log, looking only at moved refs, and a long gap
    falls back to a listing.
  - A moved head is indexed within a second of its move.
- **Leases.** `check_lease_contract` (ADR 0056, PR #106) runs for the
  `index` scope on memory, SQLite and Postgres, with this scope's timing.
  - Fault sweeps (`LEEP_FAULT_SEEDS=300`): a holder dropped at a random
    point of a wavefront, then a successor finishing; two candidates racing
    one lease, one paused past its TTL; store faults on renewals. After
    every step, each stored row has its parents' rows and the fault-free
    value.
  - A tool's fence holding back a drop while a paused former holder's write
    lands.
- **Bounded queries.**
  - Property tests over random histories (merges, octopus merges, renames,
    deletes, no-op changes). Random parent-closed subsets of rows stand in
    for an index partway through.
  - `log path`, `follow`, `last-change`, `blame`, filtered `log` and
    `search` must give the fully indexed entries, order and `truncated`,
    and the scan oracle's.
  - Pages concatenate to one walk while rows are added between pages, and
    also when a `repair drop` of the kind removes rows between pages: the
    next page then runs on the bounded scan and still continues the same
    walk.
  - Stale-ok answers equal a query at `answered_at`.
  - Over budget gives `Error::Indexing` exactly when the missing part
    exceeds it.
  - A shadow store asserts the queries write no index rows.
  - Old-client waits respect the cap and the remaining request time.
- **End to end** (`leep-e2e`, Postgres).
  - Two servers in `lease` mode: one warms, and the other takes over within
    the stale time plus one pass after a SIGKILL.
  - A server in `off` mode answers path logs correctly throughout a land
    loop.
  - A request without the header gets the right answer, and the CLI handles
    an indexing 503.
- **Swarm acceptance** (`leep-swarm`, the 2,000-agent configuration of
  "Many-agent workload": two servers in `lease` mode, or one `lease` plus
  one `off`). The swarm gains `--indexing-header` and reports indexing and
  stale answers per route, and the per-agent land rate.
  - No log, blame, search or last-change request reaches the 30 s timeout
    (today 1,063 searches, 1,165 logs and 799 blames).
  - Path-history and log-filter lag p50 at most 64 and max at most 512
    generations (today 890 / 1,448 and 1,020 / 1,578).
  - Indexing answers under 1% of indexed reads.
  - `leep_query_inline_derive_total` stays 0 while a lease is live.
  - Index rows derived more than once stay under 1% (`derive_calls`
    against rows).
  - Lands/s at least today's 21.9.

## Alternatives considered
| Option | Why not |
|---|---|
| Keep a warmer per server with a shorter interval | Done for `local` mode in phase 0. N servers still derive each range, and queries still derive inline under the timeout. |
| A table of index leases of its own (first draft) | ADR 0056 is generalising its leases into one table with scopes. A second table would duplicate the statements, the contract and the connection. |
| Postgres advisory locks for the derive lock | Postgres only, a pinned session, released only when TCP notices the session is gone. ADR 0056 rejected it for the same reasons. |
| One lease per ref | Refs share history: N leases derive the shared range N times. |
| Partition by generation range | A range cannot start before the range below it is stored. |
| Queries derive within a small budget and store it | Several query servers would derive the same new heads on the latency path. Scanning the un-indexed part costs about the same for small gaps and writes nothing. |
| Always 503 for missing rows | Breaks old clients, which treat 5xx as failure, and makes an indexer outage a read outage. |
| Old clients derive inline after their wait (first draft) | Brings back the duplicated derivation exactly when the holder is busiest. The larger old-client scan answers them without writing. |
| Slow lands while the index lags | Couples write availability to an accelerator. |
| A durable job queue table for indexing | Ref moves are already in the ref log, which is ordered by commit, and deriving the newest head covers everything below it. |
| An external scheduler (Kubernetes Jobs, etcd election) | A new dependency for what the store already does (ADR 0056). |

## Open questions for review
1. **Budget defaults** (1,024 changesets, 128 blame file reads, 512 files
   and 16 MiB for search, legacy factor 8) are guesses until phase 1
   measures the hybrid walk.
2. **`stale-ok` for blame and search.** Blame's newest indexed ancestor is
   clear. For search, "a revision a minute old" may surprise an agent that
   just landed the file it searches for. Should MCP default to `retry` for
   search?
3. **Lease `meta` contents.** `meta` is text in PR #106. This ADR stores
   the rate, backlog and shard count in it as a small JSON object. They are
   renewed every 3 s with the lease.
4. **Real chain rates.** The sizing rests on synthetic chains. The first
   measurement on a real import may move section 8.

## Review changes
These are the differences from the first draft, made at review:

- Leases are ADR 0056's `leases` rows with `scope = 'index'`. There is no
  `index_leases` table and no migration of this ADR's own.
- Tools that drop or rebuild rows wait a fence after taking the lease.
- `last_merge-v1` on the write path has a bounded fallback and a backfill,
  so a create never derives history.
- "Byte-identical" is narrowed to entries, order, `truncated` and whether
  a cursor is given. Search `stats` describe the work done.
- Old clients: waiters are capped, and waits are bounded by the remaining
  request time. There is no inline derivation while a lease lives; old
  clients get a larger scan budget, then 503.
- The ref-log tail rests on commit order being `seq` order. The backstop
  listing is every 5 s.
- Budgets are flags, blame's is counted in file reads, and the defaults
  are lower.
- `Leep-Indexing: stale-ok` added, the MCP default. Balancers must exempt
  indexing 503s.
- `local` mode adopts the 250 ms tail.
- The chain ceiling is corrected by measurement. Directory-dataflow
  derivation is the follow-up, and the sizing's sensitivity to the land
  rate per agent is stated.
- The phase plan puts phase 0 first. The frontier table, the dedicated
  indexer and shards are deferred.

## Implementation notes
Phases 0 and 1 are built as described, with these differences, each for the
reason given:

- **`last_merge-v1` fallback.** A parent without its row is bridged by the
  parent being a merge or a root, or by the grandparent's row (read in the
  same batched read when the parent's skiplist is cached), not by up to 256
  first-parent ancestors: a chain's ancestors are known only one read at a
  time, so that walk would cost up to 256 round trips on the write path.
  Otherwise the row is skipped and counted, as planned.
- **`stale-ok` target.** Path logs, last-change, filtered logs and blame
  answer at the newest *indexed* first-parent ancestor (found with skiplists
  in a few batched reads), not at the newest one whose missing part fits the
  budget: finding that needs a bounded walk per candidate. Search looks at up
  to 64 first-parent ancestors for one whose tree's trigram rows are all
  stored, since search rows are warmed only for heads.
- **Search without rows** computes the missing trigram rows in memory, a
  directory's from its children's as derivation would, within the budgets,
  and stores nothing. Pruning, `files_considered`, `files_pruned`,
  `files_scanned` and `bytes_scanned` are therefore the indexed ones too;
  `files_unindexed` counts the files fetched for missing rows. Since search
  rows are derived a level at a time, the scan first reads the tree
  structure without filters; it answers 503 as soon as that holds more
  than 8 times the file or byte budget (plus 256 files and 16 MiB), so an
  over-budget search of a cold tree costs about what the old early stop
  at the fetch budget did, not a read of the whole tree.
- **`next` on filtered logs.** To give a fully indexed store's `next`
  whichever way a walk got there, a filtered log page now ends at the next
  match beyond the page (queued again, unexpanded), so it carries `next`
  exactly when another match exists. This holds in every mode. Path walks
  instead settle their queue at the end of a page (each queued changeset
  without a row is examined or replaced by its parents' changes), which
  leaves the indexed walk's queue.
- **Rows deleted under a query** (a `repair drop` without the fence racing
  it) answer 503 with `behind = 0`, never an inline derivation.
- **Timing.** Candidates claim on their 3 s renewal tick, so a SIGKILLed
  holder is replaced within the 6 s stale time plus up to one tick (measured
  6.0 to 6.2 s in the end-to-end test). A holder trusts its view for the
  reported time less half a renew interval (ADR 0056 subtracts a whole one;
  here that would leave no margin between renewals), and renews on the spot
  before giving a write up.
- **Lease `meta`**: `{"rate", "backlog", "shards"}`, backlog being the
  changesets the last pass derived; the two lanes are deferred, so
  `leep_index_backlog_changesets` has `lane="tip"` only.
- **`--ignore-lease`** is refused for `repair drop` and `upgrade-index
  --drop-first`: a drop beside a deriving holder is the race the fence
  exists for, so "duplicate work, correct rows" holds only for the tools that
  derive. A drop also checks its leases right before deleting.
- **Search warming** checks the lease before each write of a level batch
  (a batch's blob rows, then its filters), as the other groups check it
  before each staged write. It first checked before each directory's rows;
  search rows are now derived a level of the tree at a time, and a
  batch's filters are written only after its blob rows and the level
  below, so a holder stopped between writes still leaves every stored
  filter over a stored subtree.
- **Three warm loops.** Each index group warms in a loop of its own, with
  its own ref log tail, its own record of warmed heads and its own reset
  when it takes a group over, so a group that falls behind (path history
  after an import) no longer holds back the others' passes. Section 2's
  "one range per group" holds per loop. `leep_index_derived_total` still
  counts changesets (heads, for search); `leep_index_rows_total{kind}`
  counts the rows the warmer derived.
- **Fence.** A tool waits `--index-fence` only when the group had a lease row
  before (a store whose servers never ran in `lease` mode has no holder to
  wait for). `upgrade-index --yes` takes the leases of the groups it derives.
- **CLAUDE.md's error table** is left for the maintainer to extend with
  `Indexing` → 503.

## Consequences
- With leases, each index range is derived once per fleet. Without them,
  in `local` mode, a moved head is picked up within 250 ms, not 30 s, and
  the heads of a pass share one derivation.
- Query latency stops including derivation in `lease` and `off` modes. A
  head just past the index costs a bounded scan, and further behind,
  opted-in clients choose between `Retry-After` and a stale answer. Old
  clients get correct answers whenever the index is within 8 times the
  budget.
- An indexer outage degrades queries to today's behaviour. It never blocks
  lands or reads.
- New, all additive:
  - a scope in ADR 0056's lease table;
  - one error kind (503);
  - request and response headers;
  - optional fields: `indexing` on errors, `answered_at` and `behind` on
    four responses, `files_unindexed` in search stats;
  - one ready feature;
  - flags and metrics.

  No `/v1` field or path is removed or renamed, and phases 0 and 1 add no
  table.
- One trunk's index keeps up with its lands on the measured shapes. Whether
  real histories do too is the next measurement.
