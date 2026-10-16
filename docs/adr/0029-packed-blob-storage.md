# 0029. Packed blob storage
Status: Proposed
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

## Decision (proposed)
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

## Plan
1. Prototype: write packs for the git/git store offline and report the size,
   the time to repack and the cost of `get` against loose objects. Proceed only
   if the store comes within 3x of git's pack.
2. Pack format, reader and a full `repack` for the filesystem backend, with
   golden format tests, the `check_*_contract` suite over a packed store, and a
   fault sweep that kills `repack` at every step.
3. Incremental repack (loose objects only), GC integration, backup copy of
   packs, and the object store backend.

## Consequences
- Expected storage close to git's for the same history; the prototype decides.
- Reads of packed objects decode deltas; the cache absorbs repeated reads, and
  hot recent objects stay loose until the next repack.
- `repack` becomes a scheduled maintenance job beside `gc` and `backup`.
- The blob store layout gains `packs/`; old stores are read unchanged, and a
  store with no packs behaves exactly as today.
