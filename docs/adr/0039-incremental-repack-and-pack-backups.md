# 0039. Incremental repack and pack backups
Status: Accepted. Amended 2026-10-28 (issue #155): a backup no longer
copies the source's tombstones into the pool, and pruning ignores pool
tombstones (section 6).
Date: 2026-10-17

## Context
Phase 2 of ADR 0029 shipped one kind of repack, the full kind. It walks every
changeset for delta bases and rewrites every live object into one new pack.
That cost grows with the history, not with what changed since the last run.
The walk alone on Linux (1.48M changesets) is far too slow to run daily, and
the write rereads and recompresses every packed object. ADR 0029 left two
phase 3 items open here:

1. An incremental repack that packs only loose objects. Its pack must stay
   self-contained (bases only inside the same pack, as the format requires),
   and the number of packs must stay small, because a read that misses
   searches every index.
2. Backups that copy packs as files (ADR 0029, item 6). `backup` read every
   packed object and wrote it into the pool as a loose file, so a packed
   source gave a pool 20 to 30 times larger than the source, and restoring it
   gave a loose store.

ADR 0040 adds packs to the object store backend in parallel, with a full
repack only. Its bucket layout (`packs/<id>.pack`, `packs/<id>.idx`,
`packs/tombstones/<id>/<hash>`) is what a backup to a bucket must write.

## Decision

### 1. `repack` is incremental by default; `--full` packs everything
```sh
leep-server repack                         # loose objects into a new pack; merge small packs
leep-server repack --geometric 2 --max-packs 8   # the defaults
leep-server repack --full                  # every live object into one pack (phase 2 behaviour)
```
The default changed because the job that runs on a schedule should cost
what changed since the last run. This matches `git repack` without `-a`. On
a store with no packs, both modes pack every loose object. The default walk
only misses changesets that no ref reaches, and their objects are still
packed, whole. `--full` remains the way to drop objects GC deleted from
packs, to rebase every delta on the whole history, and to compact after many
merges. It is worth running weekly or monthly (§5). Object stores (ADR 0040)
are always packed in full for now: `repack` against an `s3://` store does a
full repack whatever the flags, and `ObjectBlobStore::repack_publish`
refuses `RepackMode::Incremental`.

`RepackOptions` gains `mode: RepackMode` (`Full`, or `Incremental { factor,
max_packs }`). The default `Full` keeps the library behaviour of phase 2.

### 2. Delta hints from the new history only
`leep_derive::pack_order::pack_order_loose(repo, is_loose)` walks back from
every ref tip with an explicit stack. It stops at a changeset whose root tree
is not loose. A repack packs every object stored before it, and a tree node
is stored after the nodes and blobs it names, so everything below such a
changeset is packed already. The changesets found are sorted by generation
and diffed against their first parents with the same code as the full walk
(`pack_order`, now sharing a `Walk`). Two differences: an object that is not
loose is not listed, and a subtree whose node is not loose is not entered.
Objects the walk misses are packed whole, as before. These are uploads no
ref reaches, and nodes a concurrent repack listed out of order. The cost is
one `stat` (`FsBlobStore::is_loose`) per visited object and one root-tree
read per changeset on the frontier. It does not depend on the depth of the
history.

### 3. A base in an older pack is not copied; the chain head is whole
The first new version of a file (or the first new node of a directory) has
its base in an older pack, which the format does not allow it to reference.
There were two options: write it whole, or write a whole copy of the base into
the new pack and the version as a delta against it. Measured on every chain
head of 20 incremental repacks (§Measurements), the copy plus the delta took
3.6% more bytes than the version alone. A whole base costs about what a whole
version costs, so copying can only add bytes. Chain heads are written whole.
Later versions in the same run are deltas inside the new pack.

### 4. Pack count: a geometric merge rule and a cap
Each incremental repack decides which existing packs to fold into its new
pack (`leep_store::plan_merge`, a pure function that both backends can use).
Sizes are live object counts, so loose objects and packed ones compare.

- Like `git repack --geometric=2`: the largest packs that already grow by
  `--geometric` each are kept. Everything below the first break in that series
  merges with the loose objects. The merge then absorbs the next pack while
  that pack is less than `factor` times the merged total.
- Then the smallest remaining packs join until at most `--max-packs` remain.
- One pack alone with nothing loose is never rewritten.

The count stays logarithmic in the history: a unit test runs 2,000 simulated
daily runs and never sees more than 8. A merge must not undo earlier
compression, and must not cost a recompression. So objects of merged packs
are written first, in each pack's write order, each with the base it had
there. A packed object whose base does not change is copied frame for frame:
a zstd delta frame decodes the same against the same base in any pack.
`PackSource::stored_frame` in `packwrite.rs` supplies the frame, so the
object store writer can adopt it. Every reused object is still decoded and
checked against its hash first. A full repack reuses frames the same way
wherever its walk gives the same base as before. Loose objects follow the
merged packs, so their hints can use bases from those packs.

### 5. Crash safety and tombstones are unchanged
The incremental repack is the full repack with fewer sources and fewer
replaced packs. It takes the same lock and goes through the same steps:
publish, fence, delete loose copies, delete replaced packs, rewrite
tombstones. Tombstones of merged packs are dropped with their packs, exactly
as a full repack drops them. Tombstones of kept packs stay. A loose copy of
a packed object (a re-upload) is packed again into the new pack, so its
fresher modification time survives (GC's grace window, ADR 0029). The
repack fault sweep now also kills an incremental repack at every
`RepackStep` against a store with a big pack, a small pack and loose
objects, deleted and rescued objects in each. After each kill every live
object reads and every deleted one stays gone. The next incremental repack
leaves exactly the big pack plus one new pack, with no leftovers, and no
tombstone that names a missing pack.

### 6. Backups copy packs as files
`leep_store::packfiles::PackFiles` presents a store's `packs/` as plain
files. `FsPackFiles` covers a directory, and `ObjectPackFiles` covers a
bucket, in ADR 0040's layout. `leep_store::open_pack_files(url)` opens one
for a `file://`/path or `s3://` URL, and returns `None` for `mem://`.
`sync::sync_packs(src, dst, mode)` runs before the existing `sync_blobs`, in
`backup` and in `restore`:

- For each pack the source has published and the destination has not, it
  copies `<id>.pack` and then `<id>.idx`. The index publishes the pack, as in
  a repack, so an interrupted copy leaves an unpublished pack that the next
  run writes again. The same id means the same bytes, so a pack already in
  the destination is skipped without being read.
- The pack is streamed through BLAKE3 and checked against the length and hash
  its index records. The index is checked against its name. A damaged source
  pack is reported (`corrupt_packs` in the manifest) and never published.
  Restore refuses a pool with one. A pack the source deletes while it is
  being copied counts as `vanished`. Its objects are in the pack that
  replaced it, or they are copied loose.
- ~~Tombstones of the packs the destination holds are added: as lines to the
  file on a filesystem, and as one empty object each in a bucket. This is
  safe for older snapshots: changesets are never deleted, so GC never
  deletes what any snapshot needs (ADR 0026), and the closure check of each
  run still proves it.~~ *Amended 2026-10-28 (issue #155):* the premise
  failed with ADR 0059. Once the changeset sweep deletes a changeset and
  `gc --full` deletes its packed tree nodes, the source tombstones them in
  their pack; the next backup copied those tombstones into the pool's copy
  of the pack, so an older sealed snapshot naming them stopped verifying
  and restoring. Each run's closure check covers only its own snapshot, so
  nothing noticed. Now `sync_packs` takes a `PackSyncMode`:
  - **`Pool` (backup)** copies no tombstones. The pool keeps every packed
    object as it keeps every loose one; its object set only grows.
  - **`Restore`** copies the pool's tombstones of the packs the target
    holds, so the target reads exactly as the pool does. A pool written
    since this change has none. One written before may hold some, nearly
    all from blob GC of objects no changeset ever named; they stay as they
    are.
  The manifest's `tombstones_copied` stays, and is 0 for a backup.
- Then `sync_blobs` runs unchanged. A destination that reads packs lists
  their objects with the same logical sizes, so only loose objects are
  copied one by one. A destination that does not read packs still receives
  every object loose.
- **Pruning (backup only).** A source repack that merges packs would leave
  every intermediate pack in the pool forever. A pool pack the source no
  longer has is deleted, index first, when other pool packs hold every
  object it held (*amended 2026-10-28:* every object, not every live one;
  a pool tombstone never makes an object prunable, and the object stays
  readable wherever it was). The object set of the pool still only grows.
  A pack that holds anything no other pack does is kept: for example, an
  object GC deleted and a full repack then dropped. Since the pool no
  longer takes the source's tombstones, that is now every pack that held
  an object the source deleted before its next full repack, so a pool
  keeps somewhat more packs than the source. No prune that knows which
  sealed snapshots still need an object exists; ADR 0026 has the
  retention advice (start a new pool).

`BackupOptions` and `RestoreOptions` gain `source_packs`/`dest_packs`
(`target_packs`). The manifest's blob section gains `packs_listed`,
`packs_already_present`, `packs_copied`, `pack_bytes_copied`,
`packs_pruned`, `tombstones_copied` and `corrupt_packs`, all
`#[serde(default)]`. The manifest format stays 1. `backup-verify` (with
`--rehash`) and the closure checks read through the blob store, which reads
packs on either side, so they needed no change.

**Compatibility.** A pool written before this change holds every object
loose and no `packs/`. It restores exactly as before, and later backups add
packs beside its loose objects; a test covers both. A restore from a pool
with packs into a filesystem or bucket target copies the packs, so a packed
store restores packed.

## Measurements
A synthetic history built by `cargo run --release -p leep-server --example
incremental_repack` (20 directories, three files edited per commit, one in
twenty commits adds a file; SQLite metadata, filesystem blobs). It lands
10,000 commits with an incremental repack every 500, then 20 rounds of 100
commits, each followed by an incremental repack, then one full repack:

| | Walk | Write | Total | Packs after |
|---|---|---|---|---|
| Incremental, 500 new commits (history up to 10,000) | 0.42 to 0.89 s | 0.88 to 1.56 s | 1.7 to 2.9 s | 1 to 3 |
| Incremental, 100 new commits (history 10,100 to 12,000), 15 rounds that only pack loose objects or merge small packs | 0.10 to 0.20 s | 0.17 to 0.32 s | 0.36 to 0.59 s | 2 to 5 |
| The same, the round that merged 5 packs (72,631 objects, 71,945 frames reused) | 0.12 s | 1.35 s | 1.61 s | 1 |
| Full, 12,000 commits | 11.24 s | 4.08 s | 15.34 s | 1 |

The incremental walk stays at about 0.1 s for 100 new commits whatever the
depth of the history. The full walk is linear in it: on Linux's 1.48M
changesets, extrapolating gives over 20 minutes for the walk alone. The pack
count never exceeded 5.

The cost is size. A store maintained only by incremental repacks had
10.44 MB of packs where a full repack gives 6.91 MB (indexes about 3.56 MB in
both). About 256 chain heads per round of 100 commits are written whole
(about 225 KB of a round's 260 KB). Merges copy them as they are, so the gap
stays until the next `--full`. Run `--full` from time to time (OPERATIONS.md,
"Packing blobs").

## Consequences
- A scheduled `repack` costs what changed since the last one: on the
  synthetic history, 0.4 s for 100 commits against 15 s for a full repack
  at 12,000 commits. The pack count stays at most `--max-packs`.
- Incremental packs are larger than a full pack of the same objects (51% in
  the measurement), until a `--full`.
- `repack` without flags no longer drops GC-deleted objects from the large
  packs it keeps; `--full` does.
- A backup of a packed store copies a few files plus the loose objects. Its
  pool is about the size of the source, and a restore gives a packed store.
- `plan_merge` and frame reuse live in the backend-neutral writer, ready for
  an incremental mode in the object store backend.

## Considered and rejected
- **Keep full as the default, add `--incremental`.** The scheduled job would
  then default to the expensive mode, and on a large store an operator who
  copied the documented cron line would rewrite the whole store every night.
- **Copy chain-head bases into the new pack** (§3): measured 3.6% larger.
- **Re-delta merged objects whose base is missing or whole.** Merges have no
  path information for those objects, and getting it would need the walk
  this ADR avoids. `--full` does it.
- **Merge rule by bytes.** Loose objects are uncompressed and packed ones
  are compressed, so byte sizes do not compare. Object counts do, and the
  count also bounds index size.
- **Backups mirror the source's packs exactly**, deleting pool packs the
  source deleted. That would break the pool's rule that its object set only
  grows (ADR 0026). Pruning only covered packs keeps that rule.
- **Copy source tombstones only for objects no sealed snapshot names**
  (issue #155). It would need a closure walk of every kept snapshot on each
  backup, to save the space of objects the source deleted, which the pool
  keeps when they are loose anyway.
- **A new manifest format.** The fields are additions with defaults, and
  older readers ignore them.
