# 0050. Parallel derivation
Status: Accepted
Date: 2026-10-18

## Context
Derived kinds are computed oldest first, one changeset after another: the
warmer's catch-up and a query over un-derived history through `derive`, and
`repair rederive` through its own walk. On all of Linux (1.48 million
changesets) `repair rederive --kind history-tree-v4` ran at about 45
changesets a second, about nine hours, using one core and a little of
Postgres. A team adopting Leep imports its history once and then waits that
long for path history and filtered logs, on a machine with many idle cores.

A per-changeset row is a pure function of the changeset and its parents'
rows (section 5 of ARCHITECTURE.md). Whatever else a derivation reads (the
merge base's history at a merge, first-parent ancestors' log-filter rows) is
an ancestor's row. So once every parent of a changeset has its row, the
changeset can be derived beside any other such changeset, and the rows do
not depend on the order. Linux has about seven changesets per generation;
long first-parent stretches have one.

The serial code also kept invariants that any change must keep:
- a row is stored only after its parents' rows, and a changeset's value
  after the rows it names (pack parts, author names), so a run stopped at
  any point leaves rows that later derivations build on, and `derive`'s walk
  for missing rows stops at the right place;
- memory follows the width of history, not its length;
- history is walked iteratively;
- a staged kind's rows are held back and written a window per call, and
  stay readable (through the staging maps) until written.

Options considered:
- **Shard by generation**: derive generation by generation, each level in
  parallel. Simple, but every level waits for its slowest changeset (a
  wide merge takes seconds while its siblings take milliseconds), and on
  Linux's first-parent stretches the levels are one changeset wide.
- **Pipelining inside one changeset only** (prefetch the next changesets'
  trees and rows while one computes). Measured on the synthetic late-Linux
  history, the serial derivation is CPU-bound (0.92 cores busy at one
  job), so hiding reads alone gains about a quarter, not a multiple.
- **A wavefront over the missing range** (chosen), with read-ahead.

## Decision
`derived::wavefront` runs any per-changeset work over a range held as a
`Dag`: the range's changesets in a parents-first order (the post-order
`derive` and `rederive` already computed) with each changeset's parents and
children as positions in that order, about 50 bytes a changeset. A
changeset is *ready* when every parent in the range has finished.

1. **Scheduling.** Ready changesets wait in a heap ordered by position; up
   to `jobs` run at once, each as a Tokio task (so on the runtime's worker
   threads), lowest position first. With one job this is exactly the serial
   order. Finishing a changeset releases its children.
2. **Results in a parents-first order.** Results come back to one
   coordinator, one at a time, in the order changesets finish. A changeset
   finishes only after its parents, so that order lists parents before
   children, and it is the order rows are stored in.
3. **Read-ahead.** Changesets are read a window of 256 at a time and the
   kind's `prepare` (generations, root trees, last merges) runs on each
   window ahead of the work, one window after another, beside the
   derivations of earlier windows. Only a bounded stretch past the oldest
   unfinished changeset is read: `max(8, 4 x jobs)` windows. Parents first
   in a post-order puts a merge's side branch right before the merge, so
   the changesets that can run beside each other lie hundreds of positions
   apart, and a wide merge that takes long holds back only that stretch.
4. **Writes.** For a staged kind (`history-tree-v4`, `log-filter-v1`,
   `last_merge-v1`) a finished changeset's rows (its pack parts or name
   rows, then its value) join a buffer in finishing order, and every 256
   changesets or 4096 rows the buffer goes to a write task: `put_derived_many`
   in batches of 1024, one write in flight while derivation continues, the
   next waiting for it. Any prefix of the buffer holds whole changesets
   after their parents, so a write that fails or a process that dies leaves
   rows a later run builds on. Rows stay readable through the staging maps
   until their write is done; a value is staged before its children start.
   Kinds that are not staged store each value in its task, after the
   parents' values.
5. **Task locals.** A task inherits the derivation's task locals: the kinds
   whose lock it holds (a merge asking for its merge base's value must not
   wait on the lock its own derivation took) and the shared skiplist reads.
6. **Errors.** The first error stops new work; running tasks are awaited,
   their staged rows unstaged and dropped. Name rows dropped this way are
   unmarked, so the next derivation that meets the author stages them
   again (the serial code lost them for the life of the process).
7. **Where it applies.** `derive` for every kind (the warmer, queries,
   `derive-search` is unaffected), with jobs from `Repo::with_derive_jobs`,
   default the cores up to 8 (`leep-server --derive-jobs`,
   `LEEP_DERIVE_JOBS`). The warmer derives `history-tree-v4` and
   `log-filter-v1` side by side. `repair rederive` plans its walk the same
   way (changesets read 256 per round trip, unreadable ones reported as
   before) and recomputes, compares and writes each changeset's kinds in a
   task (`--jobs`, default the cores up to 8; a dry run uses one, since its
   shadow store is cleared between changesets). `repair rederive --missing`
   derives only absent rows through `derive`, without reading stored ones
   again, so a stopped initial build resumes where it was.

## Consequences
- Rows are byte-identical for any number of jobs: tests derive the same
  histories with 1, 2, 4, 8 and 16 jobs and compare every row, and on the
  synthetic late-Linux history in Postgres every run's rows matched the
  serial build's (PERFORMANCE.md, "Parallel derivation").
- On a 4-vCPU machine shared with other work (about two cores free), 4 jobs
  derived the synthetic history about twice as fast as the serial code, the
  full `rederive` 2.9 times as fast, and `rederive --missing` six times as
  fast as the old `rederive`. The work is CPU-bound, so the gain follows the
  idle cores; CPU time per changeset grew by 18% at 4 jobs (shared caches,
  the allocator). More jobs than cores helps when the metadata store is far
  away; past 16 the Postgres pool (16 connections) is the limit.
- Peak memory is about 100 MB above the serial code's at any number of
  jobs, mostly per-thread allocator arenas (`MALLOC_ARENA_MAX=2` removes it
  and slows 4 jobs by half). A thread-oriented allocator is a follow-up.
- A long derivation still holds its kind's lock; it just finishes sooner.
  Servers sharing a store still each warm independently (ADR 0028).
- `jobs` changesets' working sets share the 96 MiB history-row cache and
  the tree cache; histories whose single changesets need more than that
  per job miss more often.
- `repair rederive --dry-run` stays serial.
