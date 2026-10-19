# 0051. One walk per merge for the log filter index
Status: Accepted
Date: 2026-10-19

## Context
A merge's `log-filter-v1` level 0 holds the values of exactly what its other
parents bring in, and its row names per side a *stop*: the first changeset
on that side's first-parent chain that the first parent reaches (ADR 0047).
ADR 0047 computed both by walking each side's first-parent chain down,
covering merge-free stretches with levels and pushing every inner merge's
other parents onto a stack to walk the same way, with one `merge_base` per
chain, and a new one whenever a chain passed the current merge base's rank
more than 2,048 generations below the first parent.

On all of Linux in Postgres, `repair rederive --kind log-filter-v1 --missing
--jobs 2` ran at about 35,000 changesets a minute for 20 minutes, then sat
for over 80 minutes at about 636,000 rows, one thread busy and memory
growing to 3 GB, inside `merge_base` walks reading changesets. The
changesets it was on included back-merges: "Merge tag 'v4.9-rc3' into
xtensa-for-next" brings in 30,248 changesets holding 2,061 merges, and
"Merge tag 'v4.8' into next" 27,895 with 2,317. Each inner merge starts a
chain; each chain of a branch based long before the first parent costs a
`merge_base`, itself a walk of the first parent's history down to that old
base. Thousands of them per back-merge took hours, and memory grew with the
walks. The synthetic histories never had a side of that shape.

Reproduced with `history_profile --releases 8` (release cycles shaped like
Linux's: subsystem branches based on the last release, recent changesets or
releases up to 14 cycles back, with topic merges and merges of mainline
inside, and `for-next` branches merging a release four cycles newer back;
80,886 changesets): the old code made up to 1,423 `merge_base` calls for one
side and spent up to 35 s on one back-merge in Postgres; see PERFORMANCE.md
"Filtered log".

Options considered:
- **Cap the number of `merge_base` calls per merge** and overflow past it.
  Keeps the walk's shape, but every capped merge loses its level, and the
  calls under the cap still each walk the first parent's history again.
- **A reachability index** (interval labels, per-changeset ancestor bloom
  filters) to answer "does the first parent reach this" in O(1). A new kind
  to derive and store for every changeset, for one question.
- **One walk per merge** (chosen), as `git rev-list first..side` does.

## Decision
`merge_side` paints both histories at once, newest first by (generation,
id): the first parent with one mark, the other parents with another. A
changeset pops only after everything that reaches it, so its marks are
final then; one the first parent does not reach is brought in, and its own
values (fingerprints, cached per process like parents and generations) go
into level 0. The walk ends when no changeset marked only by the sides is
queued: everything not yet reached is behind something the first parent
reaches. Each side's stop is where its first-parent chain, followed as the
walk goes, first pops (or is left queued) with the first parent's mark.

The result is the same set as before, so rows are byte for byte what ADR
0047 produced, and the cost is what the side brings in plus the first
parent's history down to the oldest changeset brought in, once per merge,
however many merges the side holds. Parents and generations come from the
ancestry cache and batched read-ahead, which no longer re-reads changesets
whose parents, generation and values the process already holds; the
derivation also reads each window's skiplists in one batch.

**Budget.** A walk that visits more than 200,000 changesets
(`SIDE_BUDGET`) gives up, so one merge takes at most a few seconds and the
wavefront (ADR 0050) is never held for long. Each stop the walk found is
kept; otherwise it is the first changeset both first-parent chains share
(skiplist jumps), which the first parent reaches, or none. Level 0 is then
the union of the levels down each side's first-parent chain to its stop,
which hold everything the side reaches and its stop does not: a superset of
what it brings in, so filtered logs may expand the merge without need but
never miss a match. Levels may always be supersets; queries already treat
them so.

Kept as `log-filter-v1`: the encoding is unchanged and a row means what it
did (each level a superset of its stretch's values, stops on the chain and
reached by the first parent). Below the budget the rows are identical;
above it, a row this code writes differs from the one ADR 0047 would have
written for the same merge, and both are valid. The budget is part of the
format in the sense that rows depend on it: changing it changes the rows of
the merges between the two values (`verify` reports them; queries are
unaffected).

## Consequences
- On the synthetic release history in Postgres, 2 jobs: back-merges take
  0.025-0.11 s instead of 2.3-35 s (0.4-1.6 s with empty caches), and all
  80,886 changesets derive in 60 s instead of 843 s, without the minute-long
  stalls. Rows equal the old code's on every history compared
  (PERFORMANCE.md "Filtered log").
- A merge of a long side costs a walk proportional to what it brings in;
  merge-free stretches of the side are no longer skipped with levels, which
  matters little since read-ahead fetches them in batches.
- Stores built by ADR 0047's code stay valid and can resume with `repair
  rederive --kind log-filter-v1 --missing`; only merges past the budget
  would be stored differently by a full rederive, and `verify` would report
  those.
- `log-filter-v1` derivation no longer asks for `merge_base` or
  `last_merge-v1`.
