# 0040. Packs in object stores
Status: Accepted
Date: 2026-10-17

## Context
ADR 0029 added packs below `BlobStore` and built them for the filesystem
store. Phase 3 left the object store backend (`s3://`, MinIO) without them,
and that is where packs pay most: S3 bills every request and every object,
and a loose store of the git/git history is 326,731 objects (9.4 GB) against
one 167 MB pack plus its index.

The filesystem design leans on things buckets do not have. A repack holds
`packs/repack.lock` with `flock`, which the kernel releases if the process
dies. Tombstones are lines appended to one file under another lock. A reader
notices a new pack by rescanning a directory, which costs a `readdir`, and
keeps an open handle on a pack so it reads on after a repack deletes it.
Loose copies are moved aside atomically before cleanup decides whether to
delete them. A bucket offers whole-object PUT, GET with ranges, LIST, DELETE,
bulk DELETE, and conditional PUT (`If-None-Match: *` to create, `If-Match`
on an etag to replace): AWS S3, MinIO, R2 and GCS support both. It offers no
append, no rename, no lock, and no conditional DELETE.

## Decision
`ObjectBlobStore` reads and writes packs with the semantics of the
filesystem store (ADR 0029, "Implementation (phase 2)"), and
`leep-server repack` accepts `s3://` URLs.

1. **Same files, same names.** Loose objects stay at `blobs/<2 hex>/<62 hex>`.
   A pack is `packs/<id>.pack` and `packs/<id>.idx`, byte for byte the
   filesystem format, with the same id (BLAKE3 of the index). A pack copied
   between a bucket and a filesystem store reads on either side, and backups
   copy them by these names.
2. **One object per tombstone**: `packs/tombstones/<pack id>/<hash hex>`,
   empty. A delete writes one per pack holding a live copy, then deletes the
   loose object, as on the filesystem. Writes never conflict (each key is its
   own, and writing one twice is a no-op), so concurrent GC deletes and a
   repack need no coordination, and "is this copy deleted?" is one HEAD.
   A single tombstones object updated with `If-Match` was rejected: every
   delete would read and rewrite it, and concurrent deletes would retry
   against each other. A full repack deletes the tombstones of the packs it
   removed.
3. **A lease instead of the lock**: `packs/repack.lease` holds the holder
   and an expiry (10 minutes ahead). It is created with `PutMode::Create`
   (only if absent), renewed every 2 minutes and before every step that
   deletes or publishes with `PutMode::Update` on the etag this holder last
   wrote, and deleted at the end. A repack that finds a live lease fails with
   a conflict, like a second filesystem repack. An expired lease is taken
   over with the same conditional update, so of two repacks racing for it
   one wins. A repack that loses its lease stops before it deletes anything.
   A killed repack leaves the lease until it expires.
4. **Reads.** `get` and `contains` try the loose object first, then the
   packs. Indexes are fetched once each and kept in memory. A pack entry is
   read with ranged GETs: the store plans the entry's delta chain (stopping
   at the first object in the decoded-object cache), then fetches every
   frame it needs in one `get_ranges` call, which merges ranges less than
   1 MiB apart into one request and fetches the rest in parallel. Every
   decoded object is checked against its address. Listings merge loose and
   packed objects with per-object `modified`, minus tombstoned ones.
5. **Refresh on a miss, at most every 5 s.** A miss (no loose object, no
   live packed copy in the cached view) lists `packs/tombstones/` and then
   `packs/` again, unless a listing started less than the refresh interval
   ago. This is enough because a repack deletes loose copies only after its
   fence (minutes): a miss after the fence is more than an interval after the
   publish, so the listing it may trigger, or a later one, holds the new
   pack. A read that finds its cached pack deleted (404) lists again at once.
   The interval is configurable (`ObjectBlobStore::with_options`) and must
   stay well below the fence.
6. **`contains` is exact across processes.** GC's existence checks rely on
   it (ADR 0012). For a packed copy it HEADs the copy's tombstone, then the
   pack's index: a delete tombstones every pack it lists, so a pack that
   still exists after its tombstone was seen absent was tombstoned by no
   finished delete. HEADing the index first could miss a delete that
   tombstoned only the replacement after this pack was removed.
7. **Repack.** Under the lease: delete `.pack` objects that no index names
   and that are older than the lease TTL (a younger one may belong to a
   repack that has not yet noticed it lost the lease); list loose objects
   (one paginated LIST of `blobs/`) and the packs; download the packs being
   replaced whole to a local temp directory; write the new pack to a local
   temp file with the same writer as the filesystem repack (`packwrite`),
   fetching loose objects 32 ahead of it; upload the pack as a multipart
   upload (visible only once complete), check its size, check the lease,
   then PUT the index, which publishes. Late deletes are handled as on the
   filesystem: objects whose loose copy GC deleted, or whose old packed copy
   it tombstoned, while the pack was written are tombstoned in the new pack.
   A repack whose pack id is already published uploads nothing.
8. **Cleanup after the fence.** Delete each loose copy the pack holds if its
   listing stamp (size, last-modified, etag) is the one that was packed and
   its packed copy is not tombstoned, in bulk deletes of 1000. With no
   conditional delete and no atomic move-aside, the new pack's tombstones are
   listed again after each batch: if one appeared for an object just deleted,
   GC deleted it in between and an upload may have rescued it, so the loose
   copy is written back from the pack. Then delete the replaced packs (index
   first) and the tombstones of packs that no longer exist.

## Request cost
Per read, against S3 (a cached index; a miss within the refresh interval
lists nothing):

| Operation | Requests |
|---|---|
| `get`, loose | 1 GET |
| `get`, packed | 1 GET (the loose miss) + 1 ranged GET per run of the chain's frames more than 1 MiB apart (1 in the common case of a chain written together, or with its base in the decoded cache; at most 32, in parallel) |
| `contains`, packed | 3 HEADs (loose, tombstone, index) |
| `get` or `contains`, absent | 1 GET or HEAD, plus 2 LISTs at most every 5 s |
| first use, or after a publish | 2 LISTs, then 1 GET (index) and 1 HEAD (pack) per new pack |
| `delete` | 1 LIST of `packs/` (so a pack published elsewhere is not missed; tombstones are not listed) + 1 PUT per pack holding a copy + 1 DELETE |
| `list_page` | the loose listing as before, plus 2 LISTs at most every 5 s |

A full repack of N loose objects and P packs (T tombstones) costs N GETs,
P whole-pack GETs, about 5·⌈N/1000⌉ + 3·⌈T/1000⌉ + 12 LISTs (four
listings of `blobs/`, one tombstone listing per delete batch, and the pack
listings), one multipart upload (⌈size/16 MiB⌉ parts) and 1 PUT for the
index, ⌈N/1000⌉ bulk deletes plus 2P DELETEs, and a lease renewal (1 PUT)
per delete batch and per step that deletes. Measured in `objpack_tests::request_counts` (an
in-memory bucket, 200 objects): a read of a packed object at chain depth 32
with nothing cached is 1 GET and 1 ranged GET; the first read also does
2 LISTs, 1 GET of the index and 1 HEAD. At S3's list prices, repacking a
million loose objects costs about $0.43 (the GETs), against $5 for having
uploaded them; the repack then turns a million objects into two.

## Consequences
- `leep-server repack` works on `s3://` stores beside running servers, with
  the same fence and report. Every server reading the bucket must run this
  version before the first repack: an older one does not read packs, and
  loses the objects whose loose copies the repack deletes. The machine running it needs local disk for
  the new pack and the packs it replaces (`TMPDIR`).
- Packed reads cost one round trip more than loose ones (the loose miss),
  plus decoding; hot recent objects stay loose until the next repack, and
  the decoded-object cache and `CachedBlobStore` absorb repeats.
- A killed repack leaves a pack without an index (deleted by the repack
  after next, once older than the lease TTL), an incomplete multipart upload
  (removed by the bucket's lifecycle rule, see OPERATIONS.md), and its lease,
  which blocks the next repack for up to 10 minutes.
- The bucket needs conditional PUT. Without it (an old MinIO, or S3 with
  conditional puts disabled) repack refuses to start; reads are unaffected.
- Remaining windows, all narrow and none losing data that a changeset
  references: a re-upload of a packed object between cleanup's listing and
  its delete loses only the refreshed `modified` (GC's second line of
  defence; the rescue rows of ADR 0012 still protect it); a repack killed
  between deleting a batch and listing the tombstones again could lose a
  blob that GC deleted and an upload rescued in exactly that moment; a GC
  delete that listed packs just before a publish and wrote its tombstones
  just after the repack checked for late deletes leaves the deleted object
  in the new pack, where the next GC finds it again (the filesystem store
  has the same window). On backends without conditional updates (the
  `object_store` local-filesystem backend, used in tests) the lease is
  renewed by read-and-overwrite and an expired lease is taken over by
  delete-and-create, which two repacks racing for the same expired lease
  could both win.
- Incremental repack is not built for object stores yet; it should share
  the filesystem's options and policy when those land.
