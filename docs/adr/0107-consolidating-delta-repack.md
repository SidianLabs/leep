# 0107. Consolidating delta repack
Status: Accepted
Date: 2026-11-16

Amends [0100](0100-automatic-delta-repack.md) (section 3, how delta packs
merge; its follow-up on chain heads; its one-off `repack --full` after
upgrading) and restates
[0039](0039-incremental-repack-and-pack-backups.md) section 3 (packs are
self-contained) as a decision, with why thin packs were not taken. Storage
PR 7, stage 1.

## Context
A delta run of the automatic repack (ADR 0100) can only compress an object
against a base in the pack it writes: the walk names the previous version,
but when that version is in an older pack the store drops the hint and the
object is written whole (FORMATS.md, "Packs": the base is another entry of
the same pack). So the first new version at each path in each run is
whole. Nothing revisited those frames: a pack a delta run wrote is listed
in `packs/considered` and never wanted again, and the geometric folds and
tiered merges that later bring a head next to its base copy frames.

On edits that touch many different files (corpus B landed head by head,
400 heads with the warmer on), almost every edit is the first at its path
in its run. Measured on that corpus, the blob store held 24.25 MB where a
`repack --full` of the same store gives 12.80 MB: 1,674 file versions and
738 tree nodes were whole only because their base was in an older pack,
10.5 MB of the 14.2 MB the store was over its target, about 5.2 KB a
one-file edit, for good. ADR 0100's 2,192 bytes a land came from a corpus
that edits the same few files and does not hold for wide edits.

Thin packs (an index naming bases in other packs) would avoid the whole
frames, but they are a format change (a new index magic that old readers
refuse, a store feature and two releases, cross-pack chain reads with a cap
on packs touched, and GC, retirement, backup, sync and `verify` keeping a
tombstoned base while a live delta in another pack names it), and they gain
nothing at rest over compressing the frames later.

## Decision

### 1. Packs stay self-contained
A pack's deltas name bases in the same pack only, as ADR 0039 section 3
already says; retirement (ADR 0080), backups, GC and object stores rely on
it. The frames a delta run must store whole are compressed later, when
packs are consolidated, as `git gc` does.

### 2. Consolidation
At each check of the automatic repack, on a filesystem store, when no delta
run is due (nothing waits, or less than every threshold) or right after a
delta run that leaves no further batch (a busy store may never be below
every threshold), the background task judges its **delta packs** (every pack that is not a whole pack
waiting for a delta run) no larger than `--auto-repack-consolidate-max-bytes`.
Let `L` be the largest of them. A consolidation is an incremental repack
that names exactly those packs (`RepackOptions::packs`; every other pack in
`leave`, no loose object), at the delta run's level, given the hints of a
walk:

- `wanted(h)`: `h` is stored whole in one of the packs other than `L`;
- `walk_through(h)`: `h` is loose, in a whole pack, or in one of those
  packs.

So the walk covers the history written since `L` and stops at changesets
whose root tree is in `L`; it never walks all of history. `L` is merged, so
the bases the walk names there are in the table. `write_order_redelta`
gives each whole frame the walk's base, and every delta frame is copied
(ADR 0100 section 2). The writer's chain cap holds where chains join: a
frame whose base is already 63 deltas deep is not copied but written
whole, so a joined chain stores one whole frame per 64. Tombstoned
objects are dropped, and frames whose base GC deleted are compressed
again, as in any merge.

### 3. The largest pack's own frames, and upgrades
Frames whole inside `L` (left there by older tiered merges or geometric
folds, or by servers before this change) are judged once per pack and
process: the first judgement of a pack as `L` walks its history too (it is
then also `wanted` and `walk_through`), and records what its own frames
would save. While `L` stays the largest pack, later walks stop at it and
add that recorded figure. A consolidation that included `L` writes a pack
whose whole frames the walks all listed, recorded as saving nothing; a new
largest pack written otherwise (a large delta run, a geometric fold) is
walked once.

So an upgraded store needs no manual step: on the first start, once nothing
waits, the server walks its delta packs' history (within the size limit and
the 2,000,000-changeset walk limit) and compresses the heads earlier merges
left next to their bases, if the rules below say so. That replaces ADR
0100's "run `repack --full` once after upgrading" for delta packs within
the limit. The cost is one walk of that history per server start, on the
nice-19 thread, after the delta runs; a store whose whole frames cannot
reach the rules' floor (section 4) is not walked at all.

### 4. When it runs
The gain is what the consolidation would save:

- the bytes the walk finds, measured: the frames it lists that are stored
  whole and whose base is in the packs are counted by compressing an
  evenly spread sample of them (256 frames, at most 64 MiB, at the run's
  level) against their bases and scaling the saving to all of them. Not
  their stored size: a writer that had the base at hand already kept whole
  every frame a delta does not shrink (small tree nodes, rewritten files),
  and counting those would rewrite a clean pack on every start. Frames
  whose base is already 63 deltas deep are not counted either: the writer
  keeps them whole at the cap (every 64th version of a hot file); and
- the stored bytes of tombstoned objects in the packs, which the merge
  drops (dead derived-segment and GC-mark segment blobs, history the
  changeset sweep removed), which only `repack --full` dropped before.

It runs when the gain reaches `1 / --auto-repack-consolidate-ratio` (8) of
the bytes of the packs it rewrites, or when it has stayed at least `1/64`
of them (and 64 KiB) for `--auto-repack-consolidate-age` (24 h; `off`
leaves only the ratio). A cheap filter skips the walk when the whole bytes
of the packs (each pack's `PackDetail::whole_bytes`, read once per pack)
plus the tombstoned bytes cannot reach the lower of the two floors, and the
task remembers the gain the walk found for a set of packs and does not
walk again until the set changes. New files never trigger a rewrite: a
first version has no base, so the walk finds no gain in it.

All the delta packs within the limit are rewritten, not only the recent
ones: the design's second option, merging the packs other than `L` when
their own gain passes the rules, was not taken, since right after each
delta run the newest pack's heads are a large share of the small packs and
that option would rewrite them at almost every check, quadratic in the
bytes written between consolidations of `L`. The ratio rule instead bounds
the work: the packs are rewritten once per eighth of their bytes in real
gain, a constant write amplification of a few times the bytes landed (the
geometric fold of ADR 0100 still bounds the pack count between
consolidations). The age rule makes the at-rest size reach the floor in a
day on a store that went quiet. For the same reason the tiered merges and
geometric folds of delta packs (ADR 0100 sections 1 and 3) are kept rather
than stopped: they copy frames, and a head one of them brings next to its
base is found by the next walk, or by the walk of a new largest pack
(section 3).

### 5. The size limit
Delta packs larger than `--auto-repack-consolidate-max-bytes` (2 GiB) are
never rewritten in the background: each is logged once per process ("a
delta pack is larger than ...") and left to `leep-server repack --full`.
The walks stop at their history, and bases in them are not in the table,
so frames whose base is there stay whole. 2 GiB because a consolidation
holds the packs it replaced for the fence: at 2 GiB that stays within half
the 4 GiB ceiling on bytes waiting for cleanup (ADR 0100 section 5), so
delta runs and merges keep room beside it, and the rewrite (mostly copied
frames, every object decoded and checked) takes minutes on one
low-priority core, not hours.

### 6. Disk during the fence
A consolidation may pass the cap on bytes waiting for cleanup (ADR 0100
section 5) when no cleanup but those of small merges (at most
`small_merge_bytes`, 64 MiB, with no loose copies) waits: a store that is
mostly one pack could never consolidate otherwise, and land packs keep
merging under load. When larger cleanups wait and its packs would take the
bytes waiting past the cap, it is held ("pending"), and delta runs wait for
it (`leep_repack_auto_skipped_total{reason="consolidation"}`) so no new
large cleanup starts: it runs within a fence, and loose objects wait that
much longer. Lands never wait.

### 7. Whole frames of land and import packs are compressed again
A delta run (and `leep-server repack`) no longer copies a whole frame from
a whole pack it names, even when the frame stays whole (a first version):
land batches and imports write level-1 frames (ADR 0068), and the run
compresses them at its own level (9) instead. Frames of other packs are
copied as before.

### 8. Metrics
`leep_repack_consolidate_runs_total{outcome = ran | error | held}`,
`leep_repack_consolidate_gain_bytes_total` (the gain the walks expected of
the runs), `leep_repack_consolidate_seconds`, and the gauge
`leep_repack_whole_bytes_outside_largest` (whole bytes in the delta packs
other than the largest, at the last check). The log line "consolidated
delta packs" gives each run's packs, objects, whole frames, bytes before
and after, and the expected gain.

### 9. Unchanged
No persisted or wire format changes: no new file, index, frame, schema,
derived kind or API field, and hash types are untouched; a consolidated
pack is an ordinary pack, recorded in `packs/considered` like any delta
run's. Every object is decoded and checked against its address before it
is written, as in every repack. Crash safety is the publish sequence of
every repack (pack, rename, index, rename, retire, fence, delete): no new
step. Old and new servers share a store; whichever holds `repack.lock`
decides, and an old server's merges only delay the gain. Object stores are
unchanged: they run a full repack on a schedule (ADR 0100 section 4), which
already compresses every head. Lands never wait for any of it.

## Consequences

- Measured on the land-driven corpus on Postgres (PERFORMANCE.md,
  "Consolidating delta repack"): all storage 15.61 MB, 2.46 times Git,
  instead of 26.92 MB (4.24 times). The history objects in the blob store
  end at 5.87 MB instead of 16.56 MB: the frames `repack --full` writes
  (6.07 MB), less 0.20 MB for first versions now compressed at level 9.
  The consolidations saved 99.4% of what their samples predicted (95 to
  110% each).
- On wide edits (storage size guard, a phase of 48 lands each editing the
  next file), blob bytes per land settle at 414 to 422 against 1,900
  without consolidation.
- What remains above 2x is not in packs: derived segments of the indexed
  heads (6.32 MB on that corpus, against 3.03 MB for the whole import) and
  the GC mark segment (0.59 MB). Inside the search retention window the
  corpus is at 2.46 times Git; if the content segments shrink to the
  import's size once retention lapses, about 1.9 times (an estimate).
  Which of the two "at rest" means is a maintainer decision.
- A consolidation rewrites the delta packs within the limit, decoding and
  checking every object: 0.19 to 0.75 s for 4.8 to 12.5 MB on the corpus,
  minutes at the 2 GiB limit, at the lowest CPU priority. The ratio
  rule bounds how often; the age rule can fire daily on a store that keeps
  gaining a little.
- A server start costs one walk of the largest pack's history, once
  nothing waits, unless that pack's whole frames cannot reach the floor.
- Stores over the limit keep their largest packs as they are and need a
  `leep-server repack --full` now and then, as before this change; a store
  that keeps growing past the limit gains one such pack per limit of
  compressed history.
- Follow-ups: derived and GC-mark segment blobs could stay loose instead
  of being packed (they are compressed already, never a base, and
  short-lived; GC would then delete their bytes at once), which is the
  second stage of storage PR 7; and an incremental repack for object
  stores.
