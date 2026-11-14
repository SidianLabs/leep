# 0100. Automatic delta repack
Status: Accepted
Date: 2026-11-14

Amends ADR 0068 section 5 (the background merge) and ADR 0039 (what an
incremental repack packs). Keeps ADR 0080 and ADR 0012 (fences) as they
are.

## Context
Leep stores a file version as a zstd delta against the previous version at
its path, and a tree node against the same directory one commit earlier
(ADR 0029), but only `leep-server repack` writes deltas, and nothing ran it
unless an operator scheduled it. A server with default flags kept:

- every landed object loose and raw (48 KB a land for a one-file land);
- import and land-batch packs (ADR 0068) as whole level-1 zstd frames,
  10.5 times larger than deltas on Leep's own history. The background
  merge copies those frames, and so did the incremental repack: only
  `--full` re-delta'd them;
- the packs a merge replaced, for the fence (6 minutes by default).

Measured on Leep's own history (corpus A, 231 commits, Git 4.84 MB after
`git gc`) and on A plus 2,000 one-file edits (corpus B, Git 6.58 MB), the
default steady state was 112 MB and 168 MB, 23 to 26 times Git; after
`repack --full`, gc and compaction it was 14.4 MB and 23.1 MB. 87% of the
gap was this missing maintenance loop.

Two further constraints:

- An incremental pack cannot name a base in another pack (ADR 0039 §3):
  packs stay self-contained, which retirement (ADR 0080), backups and GC
  rely on. So each incremental run stores the first new version at each
  path whole.
- Object stores implement only full and planned repacks
  (`ObjectBlobStore::repack_publish` refuses `Incremental`).

## Decision

### 1. The background task runs the delta repack itself
`BackgroundRepack` (ADR 0068) gains a second job, on by default
(`--auto-repack`), given a `Repo` to walk (`with_repo`; the server passes
one over the raw blob store, so the walk does not flush the read cache).
At most every `--auto-repack-check-interval` (1 minute) it counts what
waits to be compressed:

- loose objects (a listing of the shard directories, one bucket listing on
  an object store), and
- the live objects of **whole packs**: packs whose index has no delta
  record. That is what `put_many` writes (land batches, imports). Pack
  details (record count, deltas, logical bytes, oldest record) are read
  once per pack id and cached, since packs never change. A pack a delta
  repack wrote, or a tiered merge made from such packs, is recorded in
  `packs/considered` (a bucket's `packs/.considered`): one pack id per
  line, rewritten whole under the repack lock or lease, pruned to the
  published packs at each write. It is never counted as whole again, in
  any process or after a restart, even with no delta (a history with
  nothing to delta against). The record is advisory: losing it costs one
  more pass, and older binaries ignore it; no existing file changes.

Loose copies a run packed stay on disk until its cleanup a fence later;
they are subtracted from the count (and left out of the next run), and so
are loose objects a run found corrupt (left for `verify --rehash`), so
neither keeps triggering runs. The loose objects are listed once per check
into a set, off the async workers, so the walk's predicates are memory
lookups, not a `stat` per object.

It runs when the objects reach `--auto-repack-objects` (1,024), or their
bytes on disk reach `--auto-repack-bytes` (32 MiB), or some have waited
`--auto-repack-max-age` (30 minutes) since the task last found none. Not
every tick: each run stores one whole frame per path it touches, so runs
over more lands store fewer of them per land.

A run on a filesystem store:

1. takes one **batch**: the oldest whole packs (by their earliest record)
   within `--auto-repack-batch-bytes` (1 GiB), 500,000 objects and 256
   packs (always at least one), then loose objects within what the caps
   leave (all of them if they fit, otherwise the oldest the walk lists);
2. walks their history with `pack_order_where`: `wanted` accepts objects
   of the batch, `walk_through` any loose object or object of a whole pack,
   so the walk passes through changesets and tree nodes outside the batch
   to reach it, from every ref tip back to changesets whose root tree is
   in a delta pack already, at most 2,000,000 changesets (an object it
   does not reach is still packed, whole);
3. publishes a `RepackMode::Incremental` repack (geometric factor 2, at
   most 8 packs, as `leep-server repack`) whose `RepackOptions::packs`
   names the batch's packs, `loose_subset` its loose objects, and `leave`
   the whole packs left for later batches (the geometric rule may not fold
   those in), at zstd level 9 (`--auto-repack-level`) on one compression
   thread, on the task's nice-19 thread, and marks the new pack considered;
4. queues its cleanup (loose copies and replaced packs) behind the fence,
   exactly as a merge's.

What the caps leave runs as the next batch at the next tick, without a
threshold, after that tick's sweep, cleanups and merges. Each batch is
published on its own, so a restart keeps every batch done and resumes
with the rest. A large import thus never holds the task for hours: the
memory and time of one run are those of one batch, plus the walk, which
still passes through the whole unpacked history once per batch.

### 2. Whole frames get the walk's bases
An incremental repack writes the merged packs' objects in their old order
first, keeping each frame's base, so loose objects can use them as bases.
It now uses `write_order_redelta`: an object stored **whole** in its old
pack takes the base the walk names for it instead; deltas keep theirs
(their frames are copied). Bases are always placed before their deltas: an
object pulls its chain of bases forward, and a chain that loops (old and
new bases disagree) is cut by writing one object whole. So a whole frame
is compressed once, against its previous version, and the result is as
small as `--full` would make the same objects (the test checks within 5%;
on corpus A the first run's pack was 5,086,011 bytes against 5.09 MB for
`--full`). `leep-server repack` (incremental) names the whole packs too, so
it no longer copies import frames.

### 3. Whole packs and delta packs merge apart
With the delta repack on, the tiered merge plans whole packs and delta
packs separately. A whole land pack merged into a delta pack would hide
its frames from the count of section 1 for good; merged with other whole
packs it stays whole, and the next delta run takes it.

### 4. Object stores: a scheduled full repack
An object store has no incremental repack, and the safer choice is to
reuse the full repack that `leep-server repack` already runs there (lease,
retirement, late tombstones, fault tests) rather than add a mode. A full
repack rewrites the whole bucket, so it is gated differently: it runs once
the bytes waiting reach a quarter of the store (pack plus loose bytes) and
at least 64 MiB, never on age or count alone (a few old or corrupt loose
objects never trigger it), and at most once per
`--auto-repack-object-store-interval` (24 hours). The time of the last one
is written to `packs/.last-full` (unix seconds) while the lease is still
held, and read before each run, so restarts and replicas share the
interval. Between runs, objects stay as they were before this change.
Implementing `Incremental` in `objrepack.rs` is a follow-up.

### 5. Bounded retired bytes, same fence
The fence must exceed the request timeout (ADR 0080, ADR 0012), so it is
not shortened. Instead the bytes waiting for cleanup (replaced packs and
packed loose copies) are capped at `pending_share` (0.5) of the live store
(pack plus loose bytes), at least `min_pending_bytes` (64 MiB) and at most
`max_pending_bytes` (4 GiB, as before). Past the cap no delta run starts,
and no merge of more than 64 MiB (`small_merge_bytes`): small-class merges
of land packs keep running, so the pack count stays bounded after a large
run (a test shows it bounded at the cap, and growing without the
exemption). A held run backs off its listing until the first pending
cleanup is due (or finishes), and an object store's held full repack until
its interval ends. A store is never held at much more than 1.5 times its
size plus the small merges, and small stores keep the old behaviour.

### 6. Lands are untouched
The write path does not change: `put_many` still writes whole level-1
frames, lands never wait for the task, and the task holds the repack lock
or lease for each step as before, so `leep-server repack` beside it is
refused or refuses it. The walk reads the metadata store one request at a
time, outside the land connection reserve (ADR 0067).

### 7. Metrics
`leep_repack_auto_runs_total{outcome}`, `leep_repack_auto_skipped_total{reason
= threshold | pending | interval | busy}`, `leep_repack_auto_seconds`,
`leep_repack_auto_input_bytes_total` (bytes that waited),
`leep_repack_auto_delta_bytes_total` (pack bytes written),
`leep_repack_auto_objects_total`, the gauges
`leep_repack_auto_waiting_objects` and `_bytes`, and
`leep_blob_store_bytes`, `leep_blob_store_logical_bytes` and
`leep_blob_store_size_ratio` (stored over logical bytes of loose objects
and searched packs), updated at each check.

## Consequences

- Measured on Postgres with default flags and no manual repack
  (PERFORMANCE.md, "Automatic delta repack"): corpus A settles at 13.5 MB
  in all (8.09 MB database, 5.38 MB blobs: the `--full` size) instead of
  112 MB, 2.8 times Git; B at 22.5 MB (15.84 + 6.66) instead of 168.5 MB,
  3.4 times Git. Over 400 lands the blob store grew 2,192 bytes a land once
  settled, against 48 to 58 KB before (1,994 after a manual incremental
  repack).
- The first new version at each path per run is stored whole, and so is
  every chain head a later merge brings next to its base: merges copy
  frames with the base they had and do not walk history. Re-delta'ing
  chain heads in merges needs the walk for those objects and is left as a
  follow-up; thin packs (cross-pack bases) would remove the cost but
  change the format.
- CPU and I/O run beside lands, at the lowest priority: the import of A
  (6,665 objects) took 3.5 s, B (16,241) 7.0 s. A first run after a very
  large import walks its whole history and can take hours (a full repack
  of Linux took 4 h 55 min on one VPS); lands are not affected, and
  `--auto-repack off` or a manual `repack` first remain possible.
- Operators no longer need the nightly incremental `repack` cron; `gc` and
  an occasional `repack --full` (which also drops tombstoned objects)
  remain.
- New files: `packs/considered` (filesystem), `packs/.considered` and
  `packs/.last-full` (bucket). Advisory and ignored by older binaries and
  by backups; no existing file or format changes.
- Stores that existed before this change hold whole land and import packs
  merged into delta packs by earlier background merges; those frames are
  no longer whole packs and stay as they are. Run `leep-server repack
  --full` once after upgrading to compress them.
