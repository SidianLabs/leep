# 0029. Packed blob storage
Status: Accepted
Date: 2026-10-16

## Context
Every blob (file contents) and tree node is stored as its own object in the
blob store, uncompressed. On the imported git/git history (82,500 commits,
measured on the VPS of `docs/DEPLOY.md`):

| Objects | Count | Stored | Each compressed alone (zlib, sampled) |
|---|---|---|---|
| File contents | 162,500 | 5.59 GB | about 1.52 GB |
| Tree nodes | 164,231 | 3.09 GB | about 2.59 GB |
| The same history as a git pack | | **0.33 GB** | |

The filesystem store adds 0.7 GB of block overhead on top (9.4 GB on disk), and
an S3 store pays per request and per object. Compressing objects one at a time
would only halve this: tree nodes are mostly hashes and compress 1.2x, and each
file version is stored whole. git is 27x smaller because it stores a version as
a delta against a similar object (the previous version of the same path, or
the same directory one commit earlier) and compresses runs of such objects
together. Leep histories have exactly that shape, at higher rates.

Per-object compression was considered first and rejected: it saves 2x, changes
the stored bytes of every object without getting near git, and breaks the
incremental backup copy (ADR 0026), which treats a size difference between the
source and destination listing as a torn copy, so a compressed destination of
an uncompressed source would be recopied on every run.

## Decision
Add packs below the `BlobStore` trait, so nothing above it changes: hashes,
`TreeNode`, the API and clients stay as they are.

1. **Two kinds of storage in one store.** New writes stay loose objects, as
   today, so `put` latency and crash safety do not change. A pack holds many
   objects; a store answers `get(hash)` from its loose objects first, then from
   its pack index.
2. **Pack files.** `packs/<id>.pack` holds entries in order; `packs/<id>.idx`
   maps each hash to an offset, a length and, for a delta, its base hash. The
   pack id is the hash of the index, so packs are immutable and content
   addressed like everything else. An entry is either the zstd-compressed object
   or the object compressed by zstd against its base as a reference prefix (the
   `--patch-from` mode). Bases are objects in the same pack; chains are capped
   (depth 32) so a read decodes a bounded number of entries, and decoded bases
   go through the existing `CachedBlobStore`.
3. **Choosing bases.** A file version's base is the previous version of the
   same path; a tree node's base is the node of the same directory in the first
   parent. Both come from walking changesets, which the repack does anyway.
   Objects are written grouped by path and in history order, so neighbouring
   entries also compress against each other.
4. **`leep-server repack`.** An offline command like `gc`, safe beside running
   servers: it marks reachable objects (the same mark as `gc`), writes a new
   pack holding the reachable loose objects (and, in a full repack, every
   reachable packed object), makes the pack and index durable, publishes the
   index, waits a fence longer than the request timeout, and only then deletes
   the loose copies and replaced packs. A reader that listed the old layout
   still finds every object until the fence passes. A killed repack leaves an
   unpublished pack (garbage) or both copies (harmless).
5. **GC.** Unreachable loose objects keep the current condemn and collect
   protocol (ADR 0012). Unreachable packed objects are dropped by the next full
   repack, which copies only reachable objects; the condemned and rescue rows
   apply to it the same way, so a blob uploaded again during a repack survives.
6. **Backup and restore.** The incremental copy (ADR 0026) copies packs and
   indexes as files and loose objects as today; a pack is immutable, so the
   same id on both sides means the same bytes. `backup-verify --rehash` decodes
   and rehashes packed objects like loose ones.
7. **Backends.** The filesystem and the object store backend both get packs;
   the in-memory store does not need them.

## Prototype
A throwaway program walked the first 3,795 commits of git/git (a local
Postgres 16 store) oldest generation first and compressed every object with
zstd level 9, as a delta against the previous version of the same path (files)
or the same directory in the first parent (tree nodes) when that was smaller,
with delta chains capped at 32:

| First 3,795 commits of git/git | Stored |
|---|---|
| Loose objects today | 91.2 MB (59.8 MB files, 31.4 MB trees) |
| Packed: zstd with reference-prefix deltas | **3.2 MB** (1.9 MB files, 1.3 MB trees) |
| `git pack-objects` of the same commits | 4.4 MB |

6,087 of 6,791 file versions and 4,697 of 4,944 tree nodes were stored as
deltas; the pass took 12 s. The design clears its bar (within 3x of git's
pack) with room to spare.

## Plan
1. Prototype (done, above).
2. Pack format, reader and a full `repack` for the filesystem backend, with
   golden format tests, the `check_*_contract` suite over a packed store, and a
   fault sweep that kills `repack` at every step.
3. Incremental repack (loose objects only), GC integration, backup copy of
   packs, and the object store backend.

## Consequences
- Storage at or below git's for the same history (the prototype measured
  3.2 MB against git's 4.4 MB on 3,795 commits).
- Reads of packed objects decode deltas; the cache absorbs repeated reads, and
  hot recent objects stay loose until the next repack.
- `repack` becomes a scheduled maintenance job beside `gc` and `backup`.
- The blob store layout gains `packs/`; old stores are read unchanged, and a
  store with no packs behaves exactly as today.

## Implementation (phase 2)
Packs for the filesystem store and a full `leep-server repack` are in
`leep-store` (`pack.rs` for the format, `packset.rs` for reads, `repack.rs`
for the writer) and `leep-derive::pack_order` (the walk); the byte layout is
in [FORMATS.md](../FORMATS.md), "Packs". On the same 3,795 commits the pack
is 3.10 MB (10,795 of 11,735 objects as deltas) plus a 1.13 MB index, written
in 8.4 s; `docs/PERFORMANCE.md` has the read latencies. Where this plan was
silent, or where implementing it changed it:

- **GC deletes packed objects with tombstones, and a full repack packs every
  live object, reachable or not** (instead of item 5's "a full repack copies
  only reachable objects"). Reachability stays GC's call alone, so the
  condemn, rescue and doom protocol of ADR 0012 applies to packed objects
  unchanged and repack needs no mark. `BlobStore::delete` of a packed object
  appends `<pack id> <hash>` to `packs/tombstones` (fsynced, under
  `packs/tombstones.lock`); a tombstoned copy is absent from `get`, `contains`
  and listings. A tombstone names a pack, so it never hides a copy in a pack
  written later, and a loose copy (an upload rescuing the blob) always wins.
  A full repack leaves tombstoned objects out and keeps only the tombstones
  of packs that still exist.
- **Each index record keeps the object's own modification time** (the loose
  file's mtime when it was packed), and listings report it, rather than the
  pack's mtime. With the pack's mtime, every repack would make all packed
  garbage look freshly written: GC's grace window would never pass for it,
  and a repack between condemn and collect would read as a rescue by upload.
- **`put` of an object that is only packed writes a loose copy**, so a
  re-upload refreshes `modified` as the GC contract requires.
- **Deltas use zstd's reference prefix**, not a loaded dictionary: a prefix
  is always raw content, while a dictionary that happens to start with zstd's
  dictionary magic would be parsed as a trained dictionary. The window is set
  to cover base and object; pairs above 128 MiB are stored whole. Readers
  decode with a per-thread decoder and the base as raw content (a prefix for
  a base that starts with the magic).
- **The index binds the pack**: its header holds the pack's length and
  BLAKE3, and the pack id is the BLAKE3 of the whole index, so one id means
  the same pack bytes everywhere (item 6).
  ADR 0038 replaced the 96-byte index records with a compact layout
  (`LEEPIDX2`, about 45 bytes per object). Indexes in this layout still read.
- **Reads check every decoded object** against its address (a file blob or a
  `tree-v1` node), so damage is a storage error, never wrong bytes. Decoded
  objects go through a 64 MiB cache inside the store as well as
  `CachedBlobStore`: a delta chain's bases are not visible to the latter.
- **Removing loose copies after the fence** moves each one aside
  (`.repack-<name>` in its shard), then deletes it only if its mtime is the
  one that was packed and the new pack's copy is not tombstoned; otherwise it
  is put back. GC writes its tombstone before it unlinks, so an upload that
  rescues a blob GC deleted from the new pack keeps its loose copy. A
  leftover `.repack-*` file is a whole object and the next repack puts it
  back.
- **Deletes during a repack.** After publishing, repack tombstones in the new
  pack every object whose loose file GC removed, or whose old packed copy GC
  tombstoned, while the pack was written. A delete that starts after the
  publish finds the new pack itself.
- **One repack at a time**, enforced with a lock on `packs/repack.lock`
  (released by the kernel if the process dies); a second one fails with a
  conflict. Temp files and packs without an index are deleted only under it.
- **Write order.** Objects are written in the walk's order (bases first);
  objects the walk does not reach (unlanded uploads, orphans) follow whole,
  in hash order. Every entry is its own frame, so grouping by path would not
  change the size. The walk orders changesets by generation computed in
  memory from parents, not from derived rows; a file's base is the first
  parent's version at the same path, else the last version listed at that
  path.
- Phase 3 items stay open: incremental repack, copying packs as files in
  backups (until then a backup copies packed objects as loose ones, which is
  correct but not smaller), and packs for the object store backend.
