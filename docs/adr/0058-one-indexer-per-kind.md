# 0058. One indexer per kind, and queries that do not derive
Status: Accepted (design; not implemented yet)
Date: 2026-10-20

## Context
Indexes other than the write path's rows (`root_tree`, `generation`,
`skiplist`, written with each changeset, ADR 0031) are built in two places
today, both per process:

- **The warmer** (ADR 0028). Every server lists every ref every
  `--warm-interval` (30 s) and derives `history-tree-v4` and `log-filter-v1`
  for each moved head, and the search rows of its tree.
- **The query path.** `log` with a path, `last-change`, `blame`, filtered
  `log` and `search` call `derived::derive` for the head they were given. A
  head the warmer has not reached is derived inside the request, under the
  request timeout.

The only coordination is `Repo::derive_locks`, a mutex per kind per
process. N servers on one store derive the same ranges N times (ADR 0028
says so and suggests `LEEP_WARM_INTERVAL=0` on all but one). ADR 0050 made
each derivation parallel, which does not change who derives.

The many-agent smoke test (`leep-swarm`, PERFORMANCE.md "Many-agent
workload") shows what that costs. With one server and 2,000 agents, the
path-history index trailed the head by 890 generations (p50, max 1,448) and
the log filter by 1,020 (max 1,578). 1,063 searches, 1,165 logs and 799
blames timed out at 30 s. Part of the lag is the poll itself: at 22 lands/s,
a 30 s interval leaves up to 660 generations between passes before any
derivation starts. The rest is derivation competing with thousands of
requests for four shared cores, and queries waiting on the kind's lock
behind the warmer, or deriving the same range themselves.

An architecture review for 500 to 500,000 agents on one repository
recommended "one elected indexer per kind (lease in the store) with
derive_jobs across its cores; servers stop warming. Query servers read rows,
and a missing range returns 'indexing, retry-after' instead of deriving
inline."

Facts this design rests on:

- **Rows are pure and idempotent.** A row is a function of the changeset and
  its parents' rows (ARCHITECTURE.md section 5). Two processes that derive
  the same changeset write the same bytes. `put_derived_many` is an upsert.
  Every stored row has its parents' rows, so a stopped derivation leaves a
  parent-closed set that the next one continues (ADR 0050).
- **Indexes only accelerate.** Every index-backed answer has an exact
  definition without the index. `log_scan` is the oracle for path logs and
  filtered logs, search is "identical with or without the index", and blame
  without the index visits every first-parent step. Rows decide speed,
  never answers.
- **A ref built by lands is a chain.** Lands rebase, so an agents' trunk is a
  first-parent chain one changeset wide. ADR 0050's wavefront has nothing to
  run side by side on a chain. One ref's index grows at one core's rate per
  kind, however many jobs or indexers there are. Parallelism comes from
  different refs, different repositories and different kinds.
- **Derived rows are shared across repositories** (ADR 0054 section 5). Only
  ref heads are per repository.
- **ADR 0056** (branch `derive/land-coordinator`, not merged) defines store
  leases for land coordination. A row is taken by one conditional statement.
  Times come from the store's clock. The epoch grows on every change of
  holder. A live lease is never stolen. The holder's view of its lease comes
  from the store's answer. Renewals go over a dedicated connection. This ADR
  reuses all of it.

## Decision

### 1. Kinds by who builds them

| Class | Kinds | Built by |
|---|---|---|
| Write path | `root_tree`, `generation`, `skiplist` and, new, `last_merge-v1` | `store_changeset`, as today (ADR 0031) |
| Indexes | `history-tree-v4` with `history-node-v4`; `log-filter-v1` with `log-filter-name-v1`; `blob-trigrams-v1` with `tree-trigrams-v1` | the indexer (section 2) |
| Result caches | `blame` | the query that computes the blame, as today |

`last_merge-v1` moves to the write path. Its value comes from the parents'
values and whether the changeset is a merge: constant cost, and no format
change. With that, every kind a query needs for ancestry (`merge_base`,
`is_ancestor`, skiplist walks) exists as soon as the changeset does. Queries
never derive history ranges for those kinds.

### 2. Who builds indexes: one lease per kind, held by any candidate
An **index lease** names an index group by its primary kind's versioned name
(`history-tree-v4`, `log-filter-v1`, `tree-trigrams-v1`) and a shard
(section 4). The holder of a lease is the only process that builds that
group for that shard. It builds for every repository and every ref in the
shard.

Two kinds of process can hold leases. Both run the same `Indexer` loop:

- **A dedicated indexer**, `leep-server indexer`. It serves no API. It runs
  only `/metrics` and `GET /v1/health` on `--metrics-listen`, and
  `--index-kinds` limits which groups it competes for (default: all). This
  is the recommended shape for fleets: indexing gets its own cores, sized
  with `--derive-jobs`, and query latency no longer shares a CPU with
  catch-up.
- **A server in `--indexing lease` mode**, which competes for the same
  leases. Small fleets get one indexer with failover and nothing new to
  deploy.

Both shapes may run at once. Whichever candidate takes a free lease holds
it. Neither preempts the other: no stealing, as in ADR 0056.

**The loop.** The holder runs one pass after another, with no fixed
interval:

1. **Learn what moved.** It tails the ref log, newest first, down to the
   last `seq` it handled (`MetaStore::ref_log(None, before_seq, limit)`,
   which already exists). Every 250 ms (`--index-poll`) costs one indexed
   query when nothing moved. Every 30 s, a `list_refs` per repository is a
   backstop for moves the log missed. A server in `lease` mode also wakes
   the loop at once when its own land moves a ref.
2. **Plan one union range per group.** For every moved head of the shard, it
   finds the changesets missing the group's rows, using `derive_missing`'s
   batched walk generalised to several heads (`derive_heads`). The ranges
   are merged into one parents-first `Dag`. History that the heads share
   appears once.
3. **Derive it on ADR 0050's wavefront** with `--derive-jobs` jobs. Chains of
   different refs and repositories run side by side. Shared ancestors run
   once. Rows are staged and written exactly as today, so every prefix
   stored is parent-closed.
4. **Two lanes.** A head with at most 8,192 missing changesets goes in the
   *tip lane*. A larger one, such as a fresh import, a new kind version or a
   restored store, goes in the *backfill lane*. Both lanes run at once, and
   the backfill lane gets at most half the jobs. A backfill of Linux
   therefore never holds agents' new heads behind it. Within a lane,
   repositories take turns (ADR 0054 section 7).
5. **Publish the frontier** (section 5), then go to step 1. Heads that moved
   during a pass are coalesced into the next one. Deriving the newest head
   covers everything below it.

Search rows are keyed by content, not by changeset. The search group's pass
runs `derive_search` for each moved head, side by side. Each head costs only
its new blobs and changed directories.

**The lease.** Migration 0007 adds the table below to both backends. It is
numbered after ADR 0056's 0006; if 0056 merges later, the numbers swap at
merge.

```sql
index_leases(kind TEXT NOT NULL, shard INTEGER NOT NULL,
             holder TEXT NOT NULL, host TEXT NOT NULL, protocol INTEGER NOT NULL,
             shards INTEGER NOT NULL, epoch BIGINT NOT NULL,
             renewed_at BIGINT NOT NULL, expires_at BIGINT NOT NULL,
             PRIMARY KEY (kind, shard))
```

- Statements, the store clock, the epoch rule, release by zeroing the times,
  the holder's view (H7) and the dedicated lease connection are those of
  ADR 0056. One backend helper serves both tables, so the SQL is written
  once.
- `MetaStore` gains `supports_index_leases`, `index_lease`,
  `acquire_index_lease`, `renew_index_leases` and `release_index_lease`. The
  defaults refuse. `check_index_lease_contract` runs on memory, SQLite and
  Postgres.
- **Timing.** TTL 15 s (`--index-lease-ttl`). Renew every 3 s
  (`--index-lease-renew`). A lease may be taken once its renewal is 6 s old.
  Indexing tolerates seconds of handover. A new holder starts with cold
  caches, so flapping would cost more than a short failover saves.
- **Holding.** A candidate tries every free lease it may hold on each
  renewal tick. It releases its leases when shutdown begins. There is no
  idle release: the work never ends.
- **No fencing.** The holder checks its lease view before each staged write
  batch and between passes. On losing the lease it abandons the wavefront at
  the next window (`DeriveWave::abandon`, as on an error). Writes already in
  flight complete. They are correct rows, so a stale holder's write is
  wasted work, never damage.

**Inside the process** the per-kind `derive_locks` stay. They still keep a
holder's own lanes, and the fallback derivations of section 3, from
deriving one range twice.

### 3. What query servers do when rows are missing
Query servers stop deriving index ranges. They answer from the rows that
exist, and compute the un-indexed part the way the index-free definition
does, without storing anything. The un-indexed part is the changesets above
the newest indexed ancestors of the head. This is bounded by a budget.
Because rows are parent-closed, the changesets lacking a row are exactly
those above a set of indexed ancestors, the *frontier set*. The walk that
finds them is `derive_missing`'s batched one, stopped at the budget.

| Endpoint | Index | Missing part within budget | Budget (default) |
|---|---|---|---|
| `GET /v1/log/{id}?path=` (also `follow`) and `GET /v1/last-change/{id}` | `history-tree-v4` | **Hybrid walk.** An un-indexed changeset is examined directly: changed relative to its first parent at the path, tested on `changes` keys and confirmed with tree entries, as in `log_scan`. Its parents are pushed. Below the frontier set the indexed walk takes over, from several roots, merged on the same heap. | 4,096 changesets (`--query-scan-budget`) |
| `GET /v1/blame/{id}` | `history-tree-v4` to skip steps | Un-indexed first-parent steps are walked one by one (`Skip::Here`), reading their files as a step without the index does today | 512 steps (budget / 8) |
| `GET /v1/log/{id}` with `author`/`extra`, no path | `log-filter-v1` | The plain scan (today the first 512 changesets, ADR 0047) runs on through the un-indexed changesets, then hands over to the index at the frontier set | 4,096 changesets |
| `GET /v1/search/{rev}` | trigram rows | A directory without a filter is entered without pruning. A file without a row is fetched and matched. Both are exact, since filters only prune. | 2,048 files without a row, or 64 MiB of them |
| `log` without filters, `provenance`, `diff`, `files`, `tree`, `merge-base` | none, or write-path rows only | unchanged | none |

Answers, their order, `truncated`, `stats` and cursors are byte for byte
what a fully indexed store gives. A cursor made while part of the history
was un-indexed resumes correctly after the indexer has caught up, and the
other way round. A query in this mode writes no index rows. The only rows it
writes are the `blame` result cache.

**Over the budget**, the request's opt-in decides:

- **Client opted in** with the request header `Leep-Indexing: retry`. The
  answer is `503` with `Retry-After` in seconds and the response header
  `Leep-Indexing: kind=history-tree-v4; behind=5210`. The body is the usual
  error with a new optional field (FORMATS.md rule 4):
  `{"error": "indexing: ...", "indexing": {"kind": "...", "behind": 5210,
  "retry_after_ms": 4000}}`. `Retry-After` is the backlog over the
  indexer's recent rate (section 5), clamped to 1 to 30 s.
- **Client without the header**: every client from before this ADR,
  `curl`, scripts. The server waits up to `--index-wait` (default 10 s) for
  the frontier to come within budget, re-reading the frontier row once a
  second, and then answers as within budget. If the wait runs out, it
  derives the range inline as today, under the per-kind lock. Old clients
  get correct answers, slower at worst. `leep_query_inline_derive_total`
  counts how often this happens.
- **No live lease** for the group (no indexer, or all of them down). Both
  kinds of client get today's inline derivation at once. A missing indexer
  makes queries slower. It never makes them unavailable.

`leep_core::Error` gains `Indexing { kind, behind, retry_after }`, mapped to
503. It is only constructed over the budget, for an opted-in request.
CLAUDE.md's error table gains the line.

**Why 503.** The service will answer later, and that is what 503 plus
`Retry-After` means to proxies and HTTP libraries. A 429 would blame the
caller's rate, and the CLI would mistake it for request limits. A 202 would
suggest a job resource that does not exist. `LeepServerErrors` subtracts
`leep_query_indexing_answers_total`, so indexing answers do not page.

**Additive API.**

- `GET /v1/index-status?ref=NAME` (and `/v1/repos/{repo}/index-status` per
  ADR 0054) lists each index group's head, indexed generation, `behind`,
  holder host and `updated_at`.
- `GET /v1/ready` lists the feature `indexing-retry` when the server honours
  the header.
- A successful indexed read carries `Leep-Index-Scanned: N`, the un-indexed
  changesets it examined. No success body changes.

**Clients.**

- **CLI.** `leep log`, `blame`, `search`, `last-change` and `log --author`
  send `Leep-Indexing: retry`. On an indexing 503 they print to stderr
  `indexing: N changesets behind, retrying in Ss`, sleep `Retry-After`
  (capped at the existing 10 s `MAX_RETRY_WAIT`) and retry for up to 60 s in
  all (`LEEP_INDEX_WAIT`). Then they fail with the new error kind
  `indexing`: retryable, exit code `SERVER`, and `retry_after_ms` in the
  `--json` error (ADR 0034).
- **MCP.** Tools send the header and retry once within 5 s at most. Then
  they return a tool error naming the kind, how far behind it is and when
  to retry, so an agent can do other work instead of blocking.

### 4. Several indexers: shards by repository
A lease key is `(kind, shard)`. Shard `s` of `S` (`--index-shards`, default
1) owns the repositories with `hash(repo_id) mod S = s`. Every lease row
records `shards`. A candidate whose `S` differs from a live row's refuses to
start, so a fleet cannot run two partitionings at once. Changing `S` means
stopping the indexers.

- **Per kind** gives up to three independent processes: path history, log
  filter, search.
- **Per repository shard** spreads repositories' chains over processes. Rows
  are shared (ADR 0054), so forks in different shards may derive common
  history twice. That is harmless, and rare once the parent repository is
  indexed.
- **Not per ref within a repository.** Refs share almost all history, and
  each extra lease would derive the shared range again.
- **Not by generation range.** A changeset needs its parents' rows, so a
  range cannot start before the range below it ends. The wavefront already
  uses all the width a history has.

### 5. Freshness: the frontier row and metrics
Migration 0007 also adds:

```sql
index_frontier(repo_id BIGINT NOT NULL DEFAULT 1, ref_name TEXT NOT NULL, kind TEXT NOT NULL,
               head BYTEA NOT NULL, head_generation BIGINT NOT NULL,
               indexed BYTEA NOT NULL, indexed_generation BIGINT NOT NULL,
               rate_milli BIGINT NOT NULL, updated_at BIGINT NOT NULL,
               PRIMARY KEY (repo_id, ref_name, kind))
```

- **Writes.** The holder upserts a ref's row after the rows of `indexed` are
  stored, at most once a second per ref. It deletes the rows of refs that no
  longer exist. `rate_milli` is the changesets per second the group derived
  over the last minute.
- **The row is advisory.** It drives `Retry-After`, the old-client wait,
  `/v1/index-status` and alerts. Correctness never reads it: queries test
  rows. A row that claims more than exists (after `repair drop`, say) costs
  a scan, and `leep_index_frontier_stale_total` counts it.
- **Indexer metrics.** These carry a `kind` label (a handful of values) and
  no ref label:
  - `leep_index_lease_held{kind}`
  - `leep_index_lease_acquired_total{kind}`
  - `leep_index_lease_lost_total{kind,reason=taken|store}`
  - `leep_index_derived_total{kind}`
  - `leep_index_backlog_changesets{kind,lane}`
  - `leep_index_lag_generations{kind}`: the largest over refs
  - `leep_index_lag_seconds{kind}`: the age of the oldest ref move not yet
    indexed, from the ref log's timestamps
- **Query metrics.**
  - `leep_query_unindexed_scanned_total{route}`
  - `leep_query_indexing_answers_total{route}`
  - `leep_query_inline_derive_total{route}`
  - `leep_query_index_wait_seconds`
- **Alerts**, each with a runbook:
  - `LeepIndexLagging`: lag over 120 s for 10 min. Ticket.
  - `LeepIndexerMissing`: some group has no holder for 5 min while any
    server runs in `lease`/`off` mode. Page, because queries have fallen
    back to inline derivation.
  - `LeepIndexingAnswersHigh`: indexing 503s above 5% of indexed reads for
    10 min. Ticket.

**No back-pressure on land.** Lands never wait for indexing.

- Indexes accelerate reads, and every read has an exact fallback.
- Slowing lands would turn an indexer outage or a backfill into a write
  outage for every agent.
- The pressure goes where it belongs instead: lag is measured and alerted,
  opted-in readers are told to come back, and the capacity limits in section
  7 say when to add shards.

### 6. Correctness
- **Duplicates stay harmless.** Rows are deterministic, `put_derived_many`
  upserts the same bytes, and batches insert in `(kind, key)` order (ADR
  0031), so racing writers queue instead of deadlocking. Leases make
  duplicates rare, not impossible. A handover may derive one write window
  (256 changesets or 4,096 rows) twice.
- **Crash mid-wavefront.** The stored rows are a parent-closed prefix (ADR
  0050). The next holder, after the 6 s stale time, plans from the heads and
  derives only what is missing. Frontier rows lag behind, and never run
  ahead, because they are written after the rows.
- **Lease loss while paused.** The process steps down at its next check and
  abandons the wavefront. Staged rows are unstaged and its name rows
  unmarked, as on a derivation error. Rows it wrote are correct.
- **Rolling upgrades.** The lease key is the versioned kind name. An old
  indexer leads `history-tree-v4` while a new one leads `history-tree-v5`,
  with no protocol between them. `protocol` in the row lets a later change
  refuse to share a lease with an incompatible holder.
- **GC.** The indexer reads only changesets and their trees, which GC never
  deletes (changesets are never deleted, and their trees are marked). A
  hybrid search fetches blobs as a search does today, under the same
  `--request-timeout` and fence. Nothing changes for `gc --fence`.
- **`repair drop`** of an index kind takes that group's lease for every
  shard. With a live holder it refuses, unless the holder's process is the
  one stopped. It also deletes the group's frontier rows. A drop interleaved
  with derivation could leave a row whose ancestors' nodes are gone, which is
  why it must not race an indexer.
- **`repair rederive --missing` and `upgrade-index --yes`** take the lease of
  the groups they build, and hold it while they run, so a running indexer
  pauses for those groups instead of deriving the same range. With a live
  holder they report who holds it and exit 1, unless `--ignore-lease`
  (duplicate work, correct rows).
- **`derive-search <rev>`** and `verify` are unchanged. `verify` gains a
  check that every frontier row's `indexed` has its rows.

### 7. Modes, flags and rollout
`--indexing local|lease|off`, default `local`.

| Mode | Indexing | Queries | Use |
|---|---|---|---|
| `local` (default) | ADR 0028's warmer, unchanged (`--warm-interval`, `--warm-search`) | derive inline, as today | single server; today's behaviour exactly |
| `lease` | candidate for index leases (`--index-kinds`, `--index-shards`) | bounded (section 3) | small fleets; a single server that wants bounded queries |
| `off` | none | bounded | query servers beside dedicated indexers |

- `leep-server indexer` always acts as `lease` without an API listener.
  `--warm-*` flags are refused outside `local`.
- **SQLite and memory.** Both implement the lease and frontier tables. With
  one process, `lease` gives the holder every lease, and the server gets the
  event-driven loop and bounded queries. A dedicated `indexer` against
  SQLite is refused at startup: SQLite serves one process (ADR 0056 makes
  the same call).
- **Rollout.**
  1. Migration 0007 creates tables only, so old servers ignore it.
  2. Start one `leep-server indexer`, or switch servers to `lease`, and
     watch `leep_index_lag_*`.
  3. Switch query servers to `off`.
  4. Old servers in `local` mode keep warming during the change. That costs
     duplicate work, not wrong rows.
- **Back out** by returning to `local`. Leases expire in 15 s. Downgrading
  the binary after migration 0007 means restoring a backup (ADR 0023), as
  for 0006.
- **The phase 2 implementation** lands as separate PRs:
  1. Lease and frontier store methods with their contracts.
  2. `last_merge-v1` on the write path.
  3. `derive_heads` and the `Indexer` (mode `lease`, `indexer`).
  4. Bounded queries and `Error::Indexing` (modes `lease`/`off`).
  5. CLI, MCP and `/v1/index-status`.
  6. The swarm acceptance run, docs and runbooks.

ADR 0028 is marked superseded for the `lease` and `off` modes when step 3
merges.

### 8. Expected numbers
Derivation rates for one chain, one core, from PERFORMANCE.md:

| Kind | Rate | Source |
|---|---|---|
| `history-tree-v4`, agent-built repository (3,025 files) | 15.6 ms a changeset, about 64/s | Soak run |
| `history-tree-v4`, git/git | about 4.6 ms of node I/O a changeset after #51, about 150 to 200/s | Real history: git/git |
| `history-tree-v4`, late-Linux synthetic, 1 job | 161 to 173/s; `upgrade-index` plans with 150/s per free core | Parallel derivation |
| `history-tree-v4`, all of Linux (wide directories, merge-heavy) | about 40/s (the review's 35 to 43/s/core), CPU-bound | Path history on Linux; serial `rederive` 45/s |
| `log-filter-v1` | 1,560/s (20,045 changesets, 12.8 s, 1 job); 830/s on the 200k back-merge-heavy synthetic | Parallel derivation; Filtered log |
| search rows | under 1 ms a row; a land adds its new blobs plus the directories on their paths | Soak run |

The model assumes 0.05 lands a second per agent (the smoke test's 500 agents
gave 0.047 on an overloaded host) and one changeset per land. One ref lands
at most about 200 to 250 times a second (ADR 0056's ceiling of one leader).
"Cores" below is path-history cores (core-seconds per second) at 40 / 64 /
150 changesets per second per core.

| Agents | Lands/s | Hot refs needed for land | Path history: cores | Log filter: cores | Indexers |
|---|---|---|---|---|---|
| 500 | 25 | 1 | 0.6 / 0.4 / 0.2 | 0.03 | one candidate in `lease` mode suffices |
| 2,000 | 100 | 1 | 2.5 / 1.6 / 0.7 | 0.1 | one; on a single ref it keeps up only when the chain derives over 100/s (git/git-shaped trees), otherwise the lag grows by 36 to 60 generations/s |
| 5,000 | 250 | at least 1 to 2 | 6.3 / 3.9 / 1.7 | 0.3 | one 8-job indexer, only if the load is spread over at least 2 to 7 chains (refs or repositories) |
| 50,000 | 2,500 | at least 10 to 12 | 63 / 39 / 17 | 3 | 3 to 8 path-history shards of 8 jobs, plus one process for log filter and search |

One indexer stops sufficing in two ways:

- **One chain.** A ref's land rate exceeds what one core derives on that
  tree shape: about 40/s on Linux-like trees, 64/s on the soak's, 150 to
  200/s on git/git's. That is 800 to 4,000 agents on one trunk. No number of
  indexers helps a single chain. The hybrid scan then carries the
  difference, and its 4,096-changeset budget covers about one to two minutes
  of a 36 to 60/s excess before opted-in readers start getting indexing
  answers. Raising this limit is derivation work, not deployment work: split
  `history-tree-v4` derivation into a parallel phase (tree diffs and changed
  paths, independent of history rows) and a serial phase (node edits), and
  measure the chain rate in a hot indexer, whose caches already hold the
  parent's nodes.
- **One process.** The chains together need more than `jobs` cores: about
  8 x 64, or 500 changesets/s, which is about 10,000 agents across at least
  8 chains. Add shards.

Before this change, the smoke test's 890-generation path-history lag was
mostly the 30 s poll. With the 250 ms ref-log tail and dedicated cores, the
expected lag at the smoke test's 22 lands/s is land rate x (poll + pass)
time: about 22 x (0.25 + 0.3), or about 12 generations, while the chain
derives faster than lands arrive.

### 9. Testing
- **Store contracts** (memory, SQLite, Postgres).
  - `check_index_lease_contract`: take, keep, refuse a live lease, take a
    stale or expired one at the next epoch, renew only at
    `(holder, epoch)`, release, racing claimants, shards independent, a
    shard-count mismatch refused.
  - `check_index_frontier_contract`: upsert, read, delete, isolation between
    repositories.
- **Derivation.**
  - `derive_heads` over several refs and repositories stores rows byte
    identical to deriving each head alone, with 1, 2, 4, 8 and 16 jobs.
  - The tip lane finishes a small head while a large backfill runs.
  - `last_merge-v1` written by the write path equals the derived value.
- **Bounded queries.**
  - Property tests over random histories (merges, octopus merges, renames,
    deletes, no-op changes). Random parent-closed subsets of rows stand in
    for an indexer partway through.
  - `log path`, `follow`, `last-change`, `blame`, filtered `log` and
    `search` must equal the fully indexed answer and the scan oracle.
  - Pages concatenate to one walk while rows are added between pages.
  - Over budget gives `Error::Indexing` exactly when the missing part exceeds
    it.
  - A shadow store asserts that the queries write no index rows.
- **Fault sweeps** (`LEEP_FAULT_SEEDS=300 cargo test -p leep-derive
  fault_tests`). After every step they check that each stored row has its
  parents' rows and the fault-free value.
  - An indexer dropped at a random point of a wavefront, then a successor
    finishing.
  - Two indexers racing one lease, one paused past its TTL by tokio's clock.
  - Store faults on renewals, forcing step-down.
  - `repair drop` refused while a holder is live.
  - Duplicate derivation is bounded by one write window per handover.
- **End to end** (`leep-e2e`, Postgres).
  - A server in `off` mode with a `leep-server indexer` answers path logs
    correctly throughout a land loop.
  - The crash test SIGKILLs the indexer mid-backfill and requires a second
    indexer to hold the lease within stale time plus one poll, with every
    row identical to a clean build.
  - A request without the header gets the right answer. The CLI handles an
    indexing 503 by retrying.
  - `indexer` against SQLite is refused.
- **Swarm acceptance** (`leep-swarm`, the 2,000-agent configuration of
  "Many-agent workload": one server in `off` mode plus one `leep-server
  indexer`, same Postgres). The swarm gains `--indexing-header` and reports
  indexing answers and retries per route.
  - No log, blame, search or last-change request reaches the 30 s timeout
    (today 1,063 searches, 1,165 logs and 799 blames).
  - Path-history and log-filter lag p50 at most 64 and max at most 512
    generations (today 890 / 1,448 and 1,020 / 1,578).
  - Indexing answers under 1% of indexed reads.
  - `leep_query_inline_derive_total` stays 0 on the query server.
  - Index rows derived more than once stay under 1% (`derive_calls` against
    rows).
  - Lands/s at least today's 21.9.
  - The same run in `local` mode is reported beside it, and the 500-agent
    `local` run stays within noise of today's. A 3-server run with ADR
    0056's coordination checks that each group is derived once per fleet.

## Alternatives considered
| Option | Why not |
|---|---|
| Keep a warmer per server with a shorter interval | Still N derivations per range. Queries still derive inline under the timeout. |
| Postgres advisory locks for the derive lock | Postgres only, a pinned session, release only when TCP notices the session is gone; ADR 0056 rejected it for the same reasons. |
| One lease per ref | Refs share history: N leases derive the shared range N times, and the holders' in-process locks cannot see each other. |
| Partition by generation range | A range cannot start before the range below it is stored. The wavefront already uses the width there is. |
| Queries derive within a small budget and store it | Several query servers would derive the same new heads, which is the duplication this ADR removes, on the latency path. Scanning the un-indexed part costs about the same for small gaps and writes nothing. |
| Always 503 for missing rows | Breaks old clients, which treat 5xx as failure (ADR 0034), and makes an indexer outage a read outage. |
| Slow lands while the index lags | Couples write availability to an accelerator. Section 5. |
| A durable job queue table for indexing | Ref moves are already in the ref log, and deriving the newest head covers everything below it. A queue would add writes per land for nothing. |
| An external scheduler (Kubernetes Jobs, etcd election) | A new dependency for what the store, which every process already trusts, can do (ADR 0056). |

## Open questions for review
1. **One lease table or two.** Should ADR 0056's `land_leases` become a
   general `leases(scope, repo_id, name, ...)` before it merges, with index
   leases as one scope? This ADR assumes two tables sharing one backend
   helper.
2. **Budgets.** Are 4,096 changesets, 512 blame steps and 2,048 unindexed
   files right? They come from batched-read costs (about 7 ms per 256
   changesets warm on git/git), not from a measurement of the hybrid walk.
   Phase 2 measures them.
3. **A stale-ok read.** On a chain the indexer cannot follow (section 8),
   should an opted-in client be able to ask for the answer at the newest
   indexed ancestor (`at=indexed`, with an additive `answered_at` field)
   instead of waiting? Agents often can use a log that is a minute old.
4. **503 behind balancers.** Some balancers retry or eject a backend on
   503. RUNBOOKS should say to exempt responses carrying `Leep-Indexing`.
   Is a distinct status worth the client confusion it would bring?
5. **The `local` mode.** Should it adopt the ref-log tail and `derive_heads`
   internally? Answers would be the same; the change is how fast warming
   catches up. This ADR keeps it untouched to limit risk.
6. **Chain rate in a hot indexer.** The 2,000 to 5,000-agent estimates hinge
   on it, and it has not been measured. Measure it before sizing.
7. **Rewriting identical rows.** Postgres rewrites a row even when the
   upsert writes the same bytes, which costs WAL and dead tuples on every
   duplicate. `DO UPDATE ... WHERE derived.value IS DISTINCT FROM
   excluded.value` would avoid that. It is a separate change.

## Consequences
- Each index range is derived once per fleet, by the process holding its
  group's lease, on cores sized for it.
- Query latency no longer includes derivation for opted-in clients. A head
  just past the index costs a bounded scan. Old clients get the same correct
  answers, with an inline derivation only after a 10 s wait.
- An indexer outage degrades queries to today's behaviour. It never blocks
  lands or reads.
- New schema (migration 0007, two tables), new `MetaStore` methods with
  contracts, a new error kind (503), and additive API: one request header,
  one response header, one optional error field, one endpoint, one ready
  feature. No existing `/v1` field or path changes.
- On one trunk, path-history derivation runs at one core's rate. Past
  roughly 800 to 4,000 agents on one ref, depending on tree shape, the index
  trails and the hybrid scan carries the gap. Per-chain derivation speed
  becomes the next derivation target.
- Operators gain a process to run (`leep-server indexer`) or a mode to set,
  three alerts and their runbooks.
