# 0042. Planner statistics after bulk writes
Status: Accepted
Date: 2026-10-18

## Context

All derived kinds share one `derived` table keyed by `(kind, key)`. On the
full Linux history (11M `derived` rows, 1.48M changesets) the table was last
analyzed before any `history-node-v3` row existed. Autovacuum re-analyzes
after 50 rows plus 10% of the table change, 1.1M rows here, and the kind
stopped at 512k rows, so Postgres kept estimating a row or two for it. With
that estimate a batched key lookup walked every row of the kind (2.3 s
instead of 3 ms per call, fixed in the query itself by passing the kind
through a sub-select), and `delete_derived_kind` planned a nested loop over
every row of the kind times its 10,000-row batch: on a 300k-row reproduction
a 100-row batch took 17 s, and a full batch did not finish in two minutes.

Leep also loads data in bulk in two single processes, `leep-git import` and
`leep-server restore`, after which indexes are usually derived at once,
before autovacuum's next pass.

## Decision

1. Migration 2 (`analyze_thresholds`) sets per-table autovacuum analyze
   thresholds: `derived` at 1% plus 10,000 changed rows, `changesets` at 2%
   plus 10,000. Storage parameters only; the SQLite migration of the same
   number is a no-op so the version numbers stay in step.
2. `leep-git import` (when it imported anything) and `restore` into Postgres
   run `ANALYZE changesets, refs, derived, ref_log` when the load is done,
   through `leep_meta::analyze_meta_store(url)` and
   `PostgresMetaStore::analyze`. It is not a `MetaStore` method: only one
   backend has anything to do, and both callers already hold a URL. A
   failure is a warning, since the load itself has committed.
3. Statements whose plan an estimate can flip into a walk of a whole kind
   are written so it cannot: `delete_derived_kind` selects each batch by one
   ordered walk of the primary key and deletes it by `ctid`.

## Consequences

- An index being built is re-analyzed once about 1% of `derived` has
  changed, while it is still being written. ANALYZE reads a fixed sample
  (30,000 rows at the default statistics target), at most once per
  `autovacuum_naptime` per table, so the extra cost does not grow with the
  table.
- The schema version becomes 2. Servers and backup readers built before this
  change refuse a version-2 database or snapshot, as for any migration;
  migrating needs no downtime (`ALTER TABLE ... SET` does not block reads or
  writes).
- Operators who load data by other means (`pg_restore`, bulk SQL) should run
  `ANALYZE` themselves (docs/OPERATIONS.md).
