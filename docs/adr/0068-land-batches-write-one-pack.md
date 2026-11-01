# 0068. Land batches write one pack
Status: Accepted
Date: 2026-10-31

## Context
A clean benchmark (release build, Postgres, the server pinned to 2 vCPU,
256 agents) landed about 179 lands/s with the land leader busy nearly all
the time, about 5.6 ms per land: about 3.2 ms replaying (much of it
`store_changeset` reading back every content of every copy to check its
size), 0.4 ms building trees, and about 2 ms committing the batch's new
objects. A batch of 64 lands wrote about 1,150 loose objects, each with a
blocking task, an exclusive create, a write, an fsync and a rename, then
`flush_blobs` synced up to 257 shard directories (ADR 0044, ADR 0048).
Storage paid too: about 38 KB and 18 to 23 loose objects per land, 2.8
times its logical size on disk in 4 KiB blocks, so 60,000 lands made 1.1
million files.

Packs (ADR 0029, 0039, 0040) already store many objects in two files, and
every reader, GC, backup and repack handles them. Until now only `repack`
wrote them.

## Decision

### 1. `BlobStore::put_many`
A new provided method stores many objects at once and returns the hashes
among them that are already durable. The default makes `put_with_hash`
calls, 256 at a time, and returns none, so every backend and wrapper stays
correct and passes `check_blob_store_contract`, which now checks
`put_many` too (small and large batches, repeats, items stored before,
listing sizes and times, a delete followed by a re-put). The cache, the
server's store wrapper and the fault injector forward it; the land staging
store stages it.

The filesystem and object store backends write a batch of at least
`min_objects` (default 16) new objects as one pack, built in memory: every
object a whole zstd frame (level 1, the leader waits for it) in hash order,
`LEEPPAK1` and `LEEPIDX2` exactly as a repack writes them, every index
record carrying the write time as `modified`. Fewer objects stay loose: a
pack and its index cost four fsyncs, and a lone create's three or four tree
nodes would each make a pack. Objects that are not a file blob or a tree
node under their address (readers check every decoded object) are written
loose. The pack and index formats do not change.

- **Filesystem.** Objects with a loose copy have it touched, as
  `put_with_hash` does, and are not packed; the rest go into the pack. The
  `.pack` and then the `.idx` are each written to a synced temp file and
  renamed with a synced `packs/` directory, as a repack publishes, and the
  root is synced once per process for the `packs/` entry itself. The new
  pack is added to this process's view directly; other processes find it
  when a miss rescans `packs/`, as they find a repack's.
- **Object store.** The `.pack` is uploaded, then the `.idx`, which
  publishes it, then it is added to the view. A miss used to list `packs/`
  at most every 5 s, which would hide another server's land pack from a
  read right after its land; a miss now also lists `packs/` (one LIST,
  shared by concurrent misses) and loads only the new packs with their
  tombstones. Tombstones of known packs keep the 5 s interval: `contains`
  checks them exactly anyway.
- **Same pack twice.** The same objects packed within one second make the
  same pack id, and a tombstone on the first would hide the objects written
  again. A writer that finds its id published already indexes the same
  frames with `modified` one second later until the id is new.

### 2. Who uses it
`Repo::put_rescuing_many` (a land batch's store, the merged text of a
conflict answer, batched imports) calls `put_many` once and leaves the
hashes it returns out of its `flush_blobs`, so a packed batch syncs no
shard directory at all. A create's tree nodes go through
`put_rescuing_many_loose`, which writes each on its own as before (4 in
flight): with nested paths a create writes 16 nodes or more, and packing
them made a pack per create, as many packs as lands for the background
repack to merge again (measured: about 2,800 packs for 2,300 lands; with
creates loose, 56). Land batches write at most 256 objects at a time when
they are not packed (the default `put_many`), as before.

### 3. Durability ordering
ADR 0044's rule stands: no changeset is stored before every blob it names
survives a power loss, and every blob is written before its condemnation
is read (ADR 0012). A pack is durable when `put_many` returns: its file
and its index are synced and both renames are synced before the index
rename's directory sync returns, and `put_rescuing_many` reads the
condemnation rows only after `put_many` returned. The changeset is stored
after that read. A write killed or failing at any step leaves either
nothing published (temp files, or a `.pack` without an index, deleted by
the next repack) or a published pack of objects no changeset names yet:
garbage, like loose objects of an interrupted land.

A repack deletes temp files and packs without an index, which a pack write
in progress has. On a filesystem the writer holds `packs/write.lock`
shared from its first temp file to its index rename, and a repack holds it
exclusively while it deletes them, so it never deletes a pack whose index
is about to be published. In a bucket a repack deletes an unindexed pack
only when it is older than the lease TTL (10 minutes, ADR 0040), and a
writer publishes its index milliseconds after its pack.

### 4. GC
Packed objects are condemned, collected and rescued as before: GC
tombstones every pack holding a live copy, then removes the loose copy
(ADR 0029). What differs is where a batch's objects are:

- A land pack records each object's write time, so GC's grace window and
  the "modified after the condemnation" check read it as they read a loose
  file's mtime.
- A rescue writes the blob again with `put_with_hash`, so a rescued object
  is loose, and a loose copy wins over every pack (ADR 0029, ADR 0059).
- An object written again by a later `put_many` goes into a new pack,
  which no older tombstone names, so it is visible again, as a re-upload is.
- On the object store an object that also has a loose copy keeps that
  copy's `modified` in listings rather than the pack's. That only affects
  the grace window, which is a second line of defence: the rescue rows
  `put_rescuing_many` writes after the pack is published protect it.

### 5. The background repack
At full load land batches publish a few packs a second, and a read that
misses its loose copy searches the packs one index at a time, so the
server merges them as they come (`--background-repack`, on by default for
filesystem and object stores):

- Every 5 s (`--background-repack-interval`) it counts the packs reads
  search; above 32 (`--background-repack-packs`) the geometric rule of
  `leep-server repack` (`plan_merge`, factor 4, at most 16 packs) picks
  the packs to merge, loose objects left out.
- Delta bases come from a walk like the incremental repack's: back from
  every ref tip through changesets whose root tree is in those packs (or
  loose, on a filesystem), at most 4,096 changesets, listing their objects
  with the previous version of the same path or directory as base. A wide
  directory node rewritten by every land (`teams/` with 256 entries) is
  stored as a small delta against its previous version. The walk's hints
  are written first; a merged pack's own deltas follow for objects the walk
  did not reach.
- A new repack mode, `RepackMode::Planned`, merges exactly the packs named
  in `RepackOptions::packs` that are still published and searched, with or
  without the loose objects. Object stores accept it too (they had only
  the full repack).
- One merge at a time, on a thread of its own at nice 19, one compression
  thread, zstd level 3 (`--background-repack-level`), so it uses what the
  land leader leaves.

**Superseded packs.** Once the merged pack is published, every live
object of each merged pack is in it: packed copies are always copied, and
a copy GC tombstoned while the merge ran is tombstoned in the merged pack
too (the "late tombstones" of ADR 0029). Reads in this process stop
searching the merged packs at once; they stay in the view, so a GC delete
still tombstones them for other processes, and are deleted a fence later
(`--background-repack-fence`, 10 minutes, as `repack --fence`), so a
reader in another process that listed them keeps finding every object.

**Steps, not runs, exclude each other.** `leep-server repack` held its
lock (or lease) from publish to cleanup, fence included. The background
repack takes it for each step only (`PublishedPack::unlock`, `relock`),
so merges can follow each other while earlier cleanups wait out their
fence. That is safe because each step still runs alone: a repack published
meanwhile may merge packs an earlier one replaced, or pack its loose copies
again, and each cleanup removes a loose copy only after checking, under
the lock, that it is unchanged and that its own published pack holds a
live copy, and removing a pack already removed is a no-op. A cleanup that
finds the lock taken (a `leep-server repack` running) waits for the next
tick. At most 256 cleanups wait at once, which bounds the disk merged packs
hold. Cleanups not run when the process stops leave the merged packs on
disk and published; after a restart they are searched again and merged by
the next background or command line repack.

### 6. Warm-up of a batch
- The batch prefetch asks the store once (`contains_many`) whether the
  contents of every changeset in the batch exist, and keeps what it finds
  for the attempt, as it keeps rows that never change. These contents are
  named by stored changesets, which every GC mark covers.
- A server-built changeset (a merge, a rebased copy) is checked with one
  `contains_many` of its contents, not a read of each to compare its size:
  its contents are those of stored changesets, whose creates checked their
  sizes against the blobs, or merge results the server wrote with their
  sizes. Client changesets keep the size check.
- The staging store answers `get_stored`, `contains` and `contains_many`
  from what the batch staged and found, and `get_derived_kinds` from its
  rows with one call to the store for the rest, instead of a call per kind.

### 7. Batch size and admission
`--land-max-batch` (default 64, as before) sets the lands per batch, so
128 or 256 can be measured. Land admission is bounded in seconds by
default (`--land-queue-seconds`, 5 s): the batches that fit in it at the
ref's moving batch time, at least 2 (one would collapse batches to one
land). A count of batches bounds 64 times more wait at full batches than
at single ones, and a fixed count over-admits slow servers and
under-admits fast ones. `--land-queue-batches N` still works and, when
given, replaces it; the two together are refused. `Retry-After` is
computed as before.

## Consequences
- A land batch commits its new objects with four fsyncs and two files
  instead of about 1,150 files and up to 257 directory syncs, and the
  copies' content checks become one batched call. A rough local run
  (`leep-land-bench`, SQLite, 10 files per changeset, `--subtrees 256`,
  server and bench sharing 4 busy cores) was bound by SQLite and the
  clients either way: 69 to 73 lands/s with packs, 68 to 72 without, at 64
  and 256 agents, 56 packs written for about 2,170 lands, merged to 22
  searched. The clean Postgres benchmark of the context is to be rerun. On disk a land's objects take about
  their compressed size instead of a 4 KiB block each, and the store holds
  a few dozen pack files instead of a file per object.
- Most objects written by land batches are packed from the start, so the
  loose objects a `leep-server repack` finds are uploads and creates' tree
  nodes. The cron schedule of OPERATIONS.md stays useful for those and for
  full repacks.
- A cold read of a recent object costs a pack read instead of a file read,
  and its decoding; the decoded-object and blob caches absorb repeats. Pack
  counts stay near 32 under load.
- Object stores: one LIST per miss (shared by concurrent misses) beyond
  what ADR 0040's table says, and land packs cut PUTs per land from about
  20 to about 2 per batch.
- **Compatibility.** No format changes: land packs are ordinary packs, and
  every Leep that reads packs reads them, refreshing on a miss on a
  filesystem. On an object store, a server built before this ADR may miss
  an object of a land pack another server published less than 5 s before
  (its refresh interval), so upgrade every server before turning
  `--land-packs` on there (it is on by default). `packs/write.lock` is a
  new empty lock file. `--land-queue-batches` keeps its meaning; its
  default is gone in favour of `--land-queue-seconds 5`.
- New metrics: `leep_pack_writes_total`, `leep_pack_write_objects`,
  `leep_pack_write_bytes`, `leep_pack_write_seconds`, `leep_blob_packs`,
  `leep_repack_background_runs_total`,
  `leep_repack_background_packs_merged_total`,
  `leep_repack_background_seconds`,
  `leep_repack_background_cleanup_seconds`.
- Tests: `put_many` in the contract of every backend; a fault sweep that
  fails pack writes at every step under concurrent lands on a real
  filesystem store and reads every reachable object through a new store;
  GC condemning, collecting and rescuing objects in land packs; the
  background repack merging while lands run; the flush order test with a
  packing store; the batch warm-up's single content check; time-based
  admission.
