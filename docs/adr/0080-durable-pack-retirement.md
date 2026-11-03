# 0080. Replaced packs are retired durably
Status: Accepted
Date: 2026-11-03

Amends ADR 0068 section 5 (superseded packs and their cleanup) and ADR 0070
(the pack-set stamps gain the retired record).

## Context
A repack that publishes a pack replacing others deletes the replaced packs
a fence later, so readers in other processes that listed them keep finding
every object. Whether a pack was replaced lived only in the memory of the
process that merged it:

- The background repack (ADR 0068) kept its pending cleanups in a
  `VecDeque` (`Waiting { due, pending }`), and the server had no shutdown
  hook that drained it. A replaced pack stayed published on disk (`.idx`
  and `.pack`); only `PackSet::supersede`, also in memory, hid it from
  this process's reads and merge plans.
- After a restart the replaced packs were listed again (`published_ids`),
  counted by `pack_sizes` and merged by `plan_tiered`, in their class,
  beside the pack that already held their objects. Each restart inside a
  fence copied the merged objects once more, and the copies themselves were
  replaced later, so they too waited for a fence that a restart could
  interrupt. One store's packs grew from 1.5 GB to 9.3 GB across server
  restarts and filled a 20 GB disk.
- A failed repack write (ENOSPC) left its `.tmp-*.pack`: only the case of
  an empty pack deleted it. A crash between the pack's rename and its
  index's rename left a `.pack` without an index. Both were deleted only at
  the start of the next repack publish, never at server start or when the
  background repack planned nothing.

The only durable facts the store kept were "published" (an index exists)
and "tombstoned". "Replaced; delete after T" was process state.

## Decision

### 1. The retired record
A filesystem store keeps `packs/retired`: append-only text, one
`<pack id> <unix seconds>\n` line per retired pack. A bucket keeps the
same lines in `packs/.retired`, rewritten whole. Invariants:

- **A retired pack's live objects are all in a published pack.** A repack
  appends the packs it replaced after the replacing pack's index rename and
  directory fsync (after the index upload in a bucket), and after it has
  tombstoned in the new pack what GC deleted meanwhile (ADR 0029's late
  tombstones). A repack whose merged packs held no live object retires them
  with no replacement.
- **Only a holder of the repack lock (or lease) writes it.** Appends are
  fsynced (and the directory when the file is created); a torn last line is
  ignored by readers and cut off before the next append. A cleanup or sweep
  drops the lines of packs whose index is gone by rewriting the file (temp
  file, fsync, rename), as `tombstones` is rewritten, and removes it when
  none remain. In a bucket the lease holder PUTs the whole object and then
  writes `packs/.gen`.
- **No view scanned after reading a line searches or plans its pack.** A
  scan reads the record first, then the tombstones, then the directory (in
  a bucket: the tombstones, `packs/.gen`, the record, then the listing). A
  line is dropped only after its pack's index is deleted, so a scan never
  pairs a listed pack with a record that forgot it. Retired packs stay in
  the view, ordered after the searchable ones like superseded packs, so a
  GC delete still tombstones them for older readers.
- **A retired pack is deleted only by a lock (or lease) holder, after its
  `<unix seconds>` plus the fence.** The fence exceeds the request timeout,
  so a reader with an older view (one that listed the pack before the line
  was written) finishes before the pack goes, as it did before. The time is
  in whole seconds, so a pack waits one second more than the fence; a clock
  set back delays deletion, one set forward shortens the wait.

`PackSet::supersede` stays as the in-process fast path: the merging process
stops searching the replaced packs without a scan.

### 2. The stamps
ADR 0070's filesystem view records a third stamp, the record's (length,
modification time, inode), taken before the tombstones' and the
directory's. A miss `stat`s all three, so a process that holds a pack open
stops searching and planning it at its next miss after the line is
appended. In a bucket, a retirement is followed by a write of `packs/.gen`,
and every listing, full or of new packs only, GETs `packs/.retired` after
the stamp: a reader whose recorded stamp follows the retirement has read the
record after it.

### 3. The sweep
`FsBlobStore::retire_sweep(fence)` and `ObjectBlobStore::retire_sweep(fence)`
try the repack lock (lease) and do nothing if it is taken. Under it they
delete `.tmp-*` files and packs without an index (holding `write.lock`
exclusively on a filesystem; in a bucket only `.pack` objects older than the
lease TTL, as ADR 0040 does), delete each retired pack past the fence the
way a repack's cleanup deletes a replaced pack (index, directory fsync,
pack; in a bucket index, pack, then `packs/.gen`), then drop the record's
lines and the tombstones of packs that are gone. It runs:

- when a server starts (with background repack off too), with
  `--background-repack-fence`;
- at the start of every background repack tick. In a bucket the tick first
  GETs `packs/.retired` and takes the lease only when a retired pack is
  due, so an idle store costs one GET per interval;
- at the start of `leep-server repack`, with its `--fence`.

A merge's in-memory cleanup still deletes its packs at the end of its fence
while the process lives; whichever of the cleanup and the sweep comes first
deletes them, and the other finds them gone. An incremental
`leep-server repack` plans its merges from the searchable packs only, so it
leaves retired packs to the sweep; a full one merges every pack, retired
ones included, and retires them all.

### 4. Temp files on error paths
A repack's `.tmp-*.pack` and `.tmp-*.idx` are removed by a drop guard on
every way out but their rename, so a failed write (a full disk, an I/O
error, a hook's simulated kill) leaves nothing. Only a killed process
leaves one, for the sweep.

## Consequences
- Restarting a server inside a fence no longer merges replaced packs again:
  the pack directory after N restarts holds what the same merges without
  restarts hold, and once the fence passes each object is stored once.
- What a crash or ENOSPC leaves is deleted at the next server start or
  background interval, not only by the next publish.
- **Format.** `packs/retired` and `packs/.retired` are new (FORMATS.md).
  Pack and index bytes, tombstones and `packs/.gen` do not change.
- **Compatibility.** Servers before this ADR ignore the record. During an
  upgrade they keep searching retired packs (harmless: each object is also
  in a searchable pack) and their background merges may merge them again,
  as before; new servers delete them after the fence, which old servers
  reading them through open handles (filesystem) or listing again on a 404
  (bucket) survive, as they survive any repack's cleanup. Packs left
  published by old servers before the upgrade were never retired; a
  `leep-server repack --full` merges and retires them once
  (OPERATIONS.md). Backups copy published packs, retired ones included, but
  not the record; a restored store searches them until a repack retires
  them again.
- A killed process can still leave replaced packs unretired if it dies in
  the milliseconds between the index rename and the append; the next merge
  of their class, or a full repack, folds them in, as before this ADR.
- An object store listing costs one more small GET (`packs/.retired`); a
  filesystem miss one more `stat`.
- New metric `leep_repack_retired_removed_total`; `RepackStep::Retired`
  for fault hooks; `RepackReport::retired_removed`; `SweepReport`;
  `TickReport::swept`.
- Tests: the record's format and torn lines (`retire::tests`); a merge
  retiring its packs for another process, a restarted process and a reader
  with an older view, which reads through the fence and after the sweep
  (`retire_tests`); merges killed at every step from the publish on, then
  three restarts and the fence, leaving one copy of each object and no
  other bytes; restarts inside the fence storing exactly what a run
  without them stores; the sweep's garbage and lock handling; the same for
  a bucket; the repack kill sweep checking no temp file survives an error;
  the background repack restarted eight times inside its fence
  (`land_packs`); and an e2e test SIGKILLing a server with background
  merges four times, then sweeping (`leep-e2e`, `repack`).
