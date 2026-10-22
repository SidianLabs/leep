# 0033. Incremental GC mark
Status: Accepted; a damaged mark row counts as a mark since 2026-10-22.
Amended 2026-10-22 by ADR 0059 phase 2: "referenced for good" holds for
tree nodes and blobs only while their changesets are stored. The
changeset sweep deletes a changeset's own `gc-mark-v1` row with it. Rows
of tree nodes and blobs stay, so incremental runs keep those objects, which
is safe. `gc --full` ignores the rows, frees them, and keeps the trees of
changesets deleted within `--backup-fence`, which it now requires.
Date: 2026-10-17

## Context
Both GC phases (ADR 0008, ADR 0012) start with a mark: page through every
stored changeset, read it, read its root tree row, and walk every tree node
not seen yet. `collect` then read one `blob-condemned-v1` row per listed
blob, one round trip each. Every run paid for all of history, although GC is a
scheduled job and between two runs only a few changesets are new.

Measured with `cargo run --release -p leep-server --example gc_profile` on
Postgres 16 and a filesystem blob store (details in `docs/PERFORMANCE.md`,
"Garbage collection"):

| History | condemn | collect | Store calls (condemn + collect) |
|---|---|---|---|
| git/git, first 3,795 commits (11,735 objects), loose | 2.5 s | 4.5 s | 12,552 + 24,287 |
| git/git, first 20,967 commits (70,323 objects), loose | 18.2 s | 29.7 s | 75,085 + 145,408 |
| the same, packed | 24.5 s | 34.6 s | 75,135 + 145,498 |

Per run: one `get_changeset` and one `get_derived` (root tree) per changeset,
one blob read per tree node, and in collect one `get_derived` per blob. On the
82,500-commit git/git this extrapolates to about 4 minutes per GC, growing
with history.

Leep's liveness is not "reachable from refs" (ADR 0008): it is "referenced by
any stored changeset", and changesets are never deleted. So the set of live
objects only grows. A ref that moves backwards, is deleted or recreated, or a
tag that moves, changes nothing about liveness. The hard part of incremental
marking in ref-based systems (finding what became unreachable) does not exist
here, and no fallback to a full mark is needed for ref moves.

## Decision
Record what each mark covered, and let the next run walk only what is not
covered.

### The record: derived kind `gc-mark-v1`
Key: a changeset id, a tree node hash or a blob hash (the three are
domain-separated hashes, so they never collide). Value: the single byte
`0x01`. A row means:

- for a blob: a stored changeset references it;
- for a tree node: a stored changeset references it, and every tree node and
  blob below it has a row;
- for a changeset: its `Modify` contents and its root tree have rows.

The closure clause is what lets a run stop at a marked tree node. It holds
because rows are written in post-order: the walk emits a tree node's row only
after its entries' rows (files when the node is expanded, subtrees when their
walk finishes), and a changeset's row after its contents and its root tree.
Rows are queued in that order and flushed with `put_derived_many` in batches
of 1,000; a batch that fails stores a prefix of its rows (the trait's
contract), and later batches start only after earlier ones succeed. So at any
instant, crashed or not, every stored row's closure is stored.

A missing row only makes a run walk more. ~~Any value other than `0x01`
reads as no row.~~ *Amended 2026-10-22:* a present row counts as a mark
whatever value it reads with. Only marks are written, so a wrong value is a
damaged read of a mark; reading it as no row made the object look
unreferenced to condemn, and a second damaged read in collect deleted a
referenced tree node (the fault sweep found it at seed 2897). `verify`
still reports the bad value.

### The mark (`gc::Mark::Incremental`, the default)
1. List changeset ids (one call per 1,000) and read their `gc-mark-v1` rows
   (one batched read per page). Covered changesets are skipped.
2. For the uncovered ones: read them with `get_changesets` and their root
   trees with one batched `get_derived_many` (ADR 0030), deriving any root
   tree that is missing.
3. Walk with an explicit stack. Before descending into a node's entries, read
   their rows in one batch and skip the marked ones. A page with no covered
   changeset at all (a first run) skips these reads: they would find nothing.
4. The sweep asks "is this blob referenced?" as "did this run walk it, or does
   it have a row?", one batched row read per listing page.

`collect` reads the condemnation rows of each listing page in one batch and
reads the rest of the fence state only for blobs that have one. It asks the
mark only about eligible condemned blobs.

`leep-server gc --full` (`gc::Mark::Full`) ignores the rows, walks everything
as before (now with the batched reads), and writes every row again. A dry run
(`--dry-run`) walks the same way but writes no rows.

### Safety argument
Claim: at mark time *m*, an incremental mark answers "referenced" for an
object X exactly when a full mark at *m* would.

- **No false "referenced".** Rows are written only by marks, only for objects
  walked from changesets that were stored (listed) at the time. Changesets
  are never deleted, so a row's reason never goes away. The run's own walked
  set is likewise only objects reached from listed changesets.
- **No missed object.** Let X be referenced by a changeset C listed at *m*.
  If C has no row, this run walks C: X is a `Modify` content (marked or
  rowed), or lies under C's root tree. The walk descends from the root and
  stops only at nodes with a row or already walked; by the closure clause a
  rowed node's whole subtree is rowed, and a walked node's subtree is walked
  (or rowed) by the time the run ends. If C has a row, by the closure clause
  every object under it has a row. Either way X is walked or rowed.

Since the mark answers exactly what a full mark answers, everything ADR 0012
proves about condemn and collect holds unchanged: in particular case (a),
"a changeset stored before collect's mark began is in the mark", is still
true, because every changeset stored before *m* is listed and is either
covered (and then its closure is rowed) or walked.

What could break the closure clause, and why it does not happen:

- **Dropping part of the kind.** `MetaStore::delete_derived_kind` deletes in
  batches and can stop half way. `repair drop` refuses `gc-mark-v1` (it is
  registered as GC bookkeeping, like the fence rows), and nothing else deletes
  derived rows.
- **Restore.** A metadata snapshot is one read transaction (ADR 0026), so the
  restored rows and changesets are those of one instant, and the clause holds
  in it.
- **Losing blobs outside GC.** The record says "referenced", not "present".
  A tree node lost from the blob store by some other fault is no longer read
  by an incremental run that trusts the parent's row, so GC no longer notices
  it. Finding missing objects is `leep-server verify`'s job (it checks every
  ref's trees and blobs); `gc --full` still reads every tree node.
- **Two runs at once.** Each run's writes keep the clause on their own, so
  overlapping runs (already excluded by ADR 0012) would not break it either.

### Rejected
- **A watermark (generation or insertion order).** Changesets are listed in
  id order and an unlanded changeset can have any generation; a watermark
  would need a new column and migration, and would still need the per-object
  closure record to stop walks.
- **Storing the mark set as one value per run.** It is O(live objects) to
  write and read on every run, and a run killed while writing it leaves
  nothing usable.
- **Ref heads as the record**, with a full mark when a ref moves backwards:
  liveness does not depend on refs, so it would only add full marks.
- **An incremental blob listing.** After the change the listing dominates an
  incremental run on a loose store (about 40 us per loose object, mostly a
  `stat`), but listing only objects newer than the last run needs an index of
  loose objects by time that neither filesystems nor object stores have. On a
  packed store the listing comes from the pack index and costs about 7 us per
  object. `repack` keeps the loose part small, so this is left as is.

## Consequences
- An incremental run costs one changeset listing call and one batched row
  read per 1,000 changesets, one listing call and one batched row read per
  1,000 blobs (plus one more batched read in collect), and a walk of only the
  new changesets' new tree nodes. On the 20,967-commit history above, after 10
  new commits: condemn 0.95 s and collect 0.71 s packed (268 + 186 store
  calls), 3.4 s and 3.3 s loose, against 24.5 s and 34.6 s before.
- The first run after upgrading walks everything once and writes one row per
  referenced object and changeset (about 91,000 rows for the history above,
  1.0 s of writes); it is still faster than before because of the batched
  reads. Meta grows by one small row per live object and changeset.
- Old servers and old `gc` binaries ignore the kind. An old `gc` run after a
  new one is safe (it marks fully and writes no rows); the rows the new one
  wrote stay valid, so the next new run walks only changesets stored since
  the last new run.
- `verify --derived` checks the rows' structure (`bad_gc_row`).
- The marks are not shared with `repack`: its walk orders objects for delta
  bases (ADR 0029) and still reads all of history. An incremental repack of
  loose objects (ADR 0029, phase 3) could keep a record of the same shape;
  that is left for that change.
- Reports gain `changesets_walked` and `marked_earlier`; `marked` counts what
  this run walked (everything in a full run).
