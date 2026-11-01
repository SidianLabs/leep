# 0073. Skiplist pre-check for bounded queries and an index lane
Status: Accepted
Date: 2026-11-01

## Context
With `--indexing lease`, a path log, last-change or blame never derives
an index range (ADR 0058). It first finds the changesets reachable from
its start that lack an index row, stopping past `--query-scan-budget`
(1,024), and answers from the rows plus a direct look at that part. Over
the budget it answers 503 (`retry`), at the newest indexed first-parent
ancestor (`stale-ok`), or waits and scans eight budgets more (no header).

The walk that finds the part, `find_missing`, goes a frontier at a time:
one batched row read, then one batched changeset read. On a linear tip
chain the frontier is one changeset, so each step is two serial round
trips, and a query 3,600 changesets behind made about 2,048 of them only
to learn it was over budget (a legacy query about 16,000). The cheap way
to learn the same thing, `newest_indexed` down the skiplists, ran only
after the walk.

The lease-holding warmer drew on the same 32-connection pool as those
walks. In a 2,000-agent `leep-swarm` run (think 2 s, four reads per land,
two server cores, Postgres, `--indexing lease`) the loop fed itself: lag
made every path log and blame scan its full budget, the scans filled the
pool, the warmer queued behind them, and lag grew. One history pass ran
326 s and derived about 350 changesets while about 13,500 landed. Lag sat
near 3,650; create, sync and file reads went from about 28 ms to 1-1.8 s
p50, and about 9,300 log requests timed out (408). Lands stayed under a
second on their own lane (ADR 0067).

## Decision
- **Pre-check down the skiplists.** Before its first changeset read,
  `find_missing` takes the starts that lack a row (at most four) and
  counts each one's first-parent ancestors without a row down the
  skiplists, stopping once the count passes the cap. Every one of them
  is reachable and lacks a row, so a chain longer than the cap proves
  the walk would pass the cap too: the query is over budget, in
  O(log gap) batched reads. A shorter chain proves nothing, and the walk
  runs as before. The 503, `stale-ok` and legacy answers are therefore
  exactly the walk's. Every caller gets it: the first check, the legacy
  larger budget, the legacy wait loop, and the check at a `stale-ok`
  target. `leep_query_scans_skipped_total{kind}` counts the walks
  skipped.
- **An index lane.** `leep_core::lane::Lane::Index` sits beside `Land`.
  `PostgresMetaStore` answers data calls made in it from a third pool,
  opened on first use, of `--meta-index-pool-size`
  (`LEEP_META_INDEX_POOL_SIZE`) connections: by default a sixteenth of
  `--meta-pool-size`, at least 4, so each group's loop can have a read and
  a write in flight. `0` shares the data pool. Its connections get the
  data pool's statement bound, as the warmer's statements had before.
  `Warmer::spawn` runs each group's loop in the lane. Tasks a derivation
  spawns (wavefront loads and steps, staged row writes) keep it through
  `lane::inherit_index`, which passes on `Index` and nothing else.
  SQLite and the memory store ignore lanes.
  `leep_meta_pool_connections` gains `pool="index"`.

## Consequences
- A lagging index costs a path log tens of metadata calls, not thousands,
  so lag no longer fills the pool the warmer needs.
- Queries can no longer starve the warmer of connections. They can still
  compete with it for CPU and for Postgres itself.
- A server opens up to `--meta-index-pool-size` more Postgres connections
  (4 by default), only once it warms. With `--indexing local` every server
  warms, so every server may open them. Count them against
  `max_connections`.
- The pre-check depends on index rows being parent-closed, as `stale-ok`
  already does. While a `repair drop` is deleting a kind that may not
  hold. A query might then be told it is over budget when a walk would
  have fit, but such queries already answer 503 (`rows_vanished`).
- A merge whose un-indexed part sits on a side branch, under a short
  first-parent chain, is still found only by walking.
- A land leader's derivation tasks stay in the shared lane, as before:
  `inherit_index` does not carry `Land`, so the land pool keeps the size
  ADR 0067 chose for it.
- Code that the warmer runs on a task of its own must use
  `lane::inherit_index` (or `lane::inherit`), or it leaves the reserve.

## Alternatives considered
- **Pre-check in `plan()` only.** It would leave the legacy larger budget
  and the wait loop walking, and add a row read to every query whose
  index is current. Inside `find_missing` it reuses the first row read.
- **A lower scan budget.** That makes more queries answer 503 or stale
  while the index is a little behind, and still pays two round trips per
  changeset up to the budget.
- **Raising `--meta-pool-size`.** That moves the point where the warmer
  starves without removing it, and costs Postgres backends for every
  request.
- **Running the warmer in the land lane.** A long derivation would then
  hold connections that every land waits for.
