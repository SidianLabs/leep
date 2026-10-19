# 0052. Dropping a derived kind by walking the table
Status: Accepted
Date: 2026-10-19

## Context

Upgrading the path-history index from v3 to v4 (ADR 0045) ends with
`leep-server repair drop --kind history-tree-v3`. On all of Linux that is
26.9M rows (25.4M `history-node-v3`, 1.48M `history-tree-v3`, 24 GB of
values) in a 33 GB `derived` table, on Postgres 16 with 2 vCPUs. There
`--dry-run` took 38 minutes just to count, and the delete took 6.7 to 8.5 s
per 10,000-row batch with autovacuum running: about 5.5 hours.

- The dry run counted by paging through `list_derived`, which reads every
  value: 24 GB in key order. Keys are hashes, so key order is unrelated to
  where rows are stored, and that meant reading pages at random.
- The delete took each batch in key order (ADR 0042), so each batch of
  10,000 rows touched about 10,000 pages scattered across the table. With
  the table larger than memory, every batch read its pages from disk at
  random. And Postgres logs a full copy of a page the first time it changes
  after a checkpoint; deleting in key order changes each page several times,
  a checkpoint apart. In a reproduction (Postgres 16, `debug_io_direct =
  data` to defeat the page cache) the old delete wrote 6.5 to 7.7 KB of WAL
  per deleted row, 33 GB for 4.3M rows from a 4.9 GB table, and ran at 4,900
  to 6,100 rows a second. Its dry run, scaled to Linux, predicted 43 minutes
  against the 38 measured.

## Decision

1. `MetaStore::count_derived(kind)`: database backends count with `count(*)`
   over the primary key (an index-only scan in Postgres); no value is read.
   The default pages through `list_derived`, as before. `repair drop
   --dry-run` uses it.
2. `MetaStore::delete_derived_step(kinds, cursor) -> DeleteStep`: one bounded,
   separately committed step of deleting every row of several kinds, with
   an opaque cursor, rows deleted per kind, and `done` of `total` for
   progress. `delete_derived_kind` becomes a provided method looping over
   steps. `repair drop` puts all the kinds it drops (a kind and its `also`
   kinds) in one delete and prints rows, rate, position and an ETA every
   ten seconds.
3. The database backends plan on the first step. If the kinds hold at least
   one row per four pages (rowids in SQLite), they **walk the table in
   physical order**: each statement deletes the rows of the kinds in a range
   of pages (`ctid` range in Postgres, a Tid Range Scan; `rowid` range in
   SQLite). Each page is read once, sequentially, and changed once, so WAL
   is about one page image per page that held a row of the kinds. The range
   starts at 32 pages and doubles or halves to keep each statement between
   0.25 and 1 s. Otherwise, a kind that is a small share of the table is
   deleted in key order in batches of 10,000, as before, but from a cursor,
   so dead index entries of earlier batches are not walked again.
4. The Postgres walk statement runs with index scans, index-only scans and
   bitmap scans switched off for its transaction (`set_config(..., true)`).
   With the one-row estimate ADR 0042 describes, Postgres otherwise deletes
   a page range through the primary key, walking the whole kind for every
   range. A test reproduces the stale estimate and checks the plan.
5. Contract: rows of the kinds that exist when the delete starts and are not
   rewritten while it runs are deleted; other kinds are untouched. A row of
   the kinds written while it runs (behind the walk, or moved by an update)
   may survive, exactly as one written just after a delete would. Starting
   again finishes an interrupted delete; the cursor is not persisted.

Table partitioning by kind would make a drop a `DROP TABLE`, but it is a
schema change that rewrites the 33 GB table once, and every new kind would
need a partition or would fall in a default one. The walk brings the drop to
the cost of reading and logging the table once; not justified now.

## Consequences

- In the cold-cache reproduction, 1.8M rows from a 2.6 GB table: the dry
  run took 2.0 s instead of 154 s; the drop ran at 35,000 rows a second
  instead of 6,100, and wrote 1.2 KB of WAL per row instead of 6.5 KB. With
  a warm cache, 4.3M rows: 30 s instead of 100 s, 5.1 GB of WAL instead of
  28.5 GB. SQLite, 432,000 rows: 41,000 rows a second instead of 21,000; the
  count 61 ms instead of 4.6 s. Details in docs/PERFORMANCE.md.
- On Linux the dry run should take about a minute, and the drop the time to
  read the table once and write about as much WAL and data: 10 to 20 minutes
  on a disk that reads and writes sequentially at 150 to 300 MB/s, against
  5.5 hours. The reproduction, whose direct IO also disables readahead, is
  an upper bound of about an hour.
- A drop of a small kind from a large table still runs in key order and
  never reads the rest of the table, so `repair drop --kind blame` stays
  quick.
- A rerun after an interruption walks the table again from the start: the
  pages already done are read but hold nothing to delete.
- Backends that delete derived rows implement `delete_derived_step`
  (and should implement `count_derived`); `check_meta_store_contract` checks
  multi-kind steps, untouched kinds, idempotence and a delete abandoned
  part way and started again.
- The dead rows are still there afterwards: autovacuum vacuums `derived`
  soon after a large drop (another sequential pass over the table), and only
  `VACUUM FULL` or `pg_repack` returns the space to the operating system.
