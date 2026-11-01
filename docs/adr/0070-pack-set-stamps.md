# 0070. Pack-set stamps make blob-store misses cheap
Status: Accepted
Date: 2026-11-01

Amends ADR 0040 section 5 (when an object store lists its packs on a miss)
and ADR 0068 (a miss no longer lists `packs/` every time).

## Context
A blob store answers a read from its loose copies, then from a cached view
of its packs. Another process may publish a pack (a land batch of another
server, ADR 0068; a `leep-server repack`), so before answering "absent" a
store looked again: the filesystem store took a mutex, read the whole
`packs/tombstones` file and listed `packs/` on every miss; the object
store listed `packs/` once per miss (shared by concurrent misses).

Misses are common. Every create asks which of its contents are missing
(all new ones are), a land batch's warm-up asks for every content of the
batch, and lazy trees (ADR 0069) made misses part of reading an unlanded
changeset. With land packs published about three times a second and
background merges replacing them, the lazy-trees slow path cost 17 ms per
land from misses alone, until it stopped asking the store for nodes known
to be unstored. In the clean benchmark (server pinned to 2 vCPU, Postgres,
20,000-file fixture) at 1,024 agents, the lazy-trees build had a land p99
of 12.4 s and 8,303 503s against 4.5 s and 115 on main: the misses
serialized on the rescan mutex inside the blocking pool, so handlers held
their body-budget permits (the 503s) longer.

## Decision

### 1. Filesystem: stamps of `packs/` and the tombstones
A view records two stamps taken before its scan reads anything: the
tombstones file's (length, modification time, inode) and the `packs/`
directory's (modification time, inode). Publishing a pack (a rename into
`packs/`), deleting one and rewriting the tombstones (a rename) all change
the directory; appending a tombstone changes the file. So writers change
nothing.

A miss of the view `stat`s both, with no lock. If both match, the view
stands and the miss is answered. If either changed, the store scans
again: the tombstones file is read only if its own stamp changed (one
naming a pack the scan lists for the first time was appended after that
pack was published, so after the stamp the view recorded), `packs/` is
listed, and packs already open are reused. Concurrent misses share a scan:
a scan that began after a miss's `stat` answers it, instead of each
queueing for its own behind the mutex. The order stays tombstones before
the directory (a repack deletes replaced indexes before it drops their
tombstones).

**Granularity guard.** Two changes within one timestamp tick leave the
directory's time as it was, so a scan right after the first can miss the
second for good. A view whose directory time was within 2 s of the clock
when it was scanned (FAT's 2 s ticks; Linux filesystems tick once per
jiffy) is *unconfirmed*: a miss on it rescans even with unchanged stamps,
at most every 100 ms. A view one of whose listed packs failed to load (a
pack being deleted) is unconfirmed too. Under load, with packs published
every few hundred milliseconds, views are mostly unconfirmed, and misses
scan at most ten times a second instead of once each.

### 2. Object store: `packs/.gen`
Every writer that publishes or removes packs rewrites one small object,
`packs/.gen`, last writer wins, after the index upload (or the deletes) it
announces and before it returns: `put_many` after it publishes a land
pack (so before the land stores a changeset naming its objects, keeping
ADR 0068's ordering), a repack after it publishes and after it deletes the
packs it replaced, and the pack-file copy of backups after it writes or
deletes an index. Its contents are the pack id, a per-process nonce, a
counter and the time, so no two writes have the same contents.

A view records the contents a GET of `packs/.gen` returned before the
view's listing of `packs/` began. A miss GETs `packs/.gen` (one GET per
50 ms per process, shared by concurrent misses; about 60 bytes, so it
costs what a HEAD would) and lists `packs/` for new packs only if the
contents differ. Any publish whose `put_many` returned before that GET
wrote the stamp after its index: either before the view's GET, so its
index was in the view's listing, or after, so the contents differ. The
contents are compared, not the etag: some backends derive the etag from
metadata (`object_store`'s local filesystem backend uses inode, mtime and
size), which a reused inode, a coarse mtime and a body of the same length
can repeat for two generations. A bucket without `packs/.gen` (only older
servers wrote it), or an empty one, falls back to a listing per miss, as
ADR 0068 did. The full listing (tombstones included)
still runs on a miss at most every 5 s (ADR 0040 section 5), and a read
that finds its pack deleted (404) still lists at once.

### 3. Exactness where a false miss is an error
`BlobStore::get_exact` is `get_stored` with an exact `None`: after a miss
the stamps confirmed, it scans (lists) once more, with a scan that began
after the call (shared by lookups started together), and retries; a second
miss is a real miss. Its provided default is `get_stored`, so every
backend and wrapper is correct; the cache, the server's store wrapper, the
fault injector and derive's wrappers forward it. It covers what a stamp
cannot: the 100 ms and 50 ms windows, a coarse clock, a network filesystem
that caches directory attributes, and older servers that do not write
`packs/.gen`.

What uses it is what something already references:

- tree reads (`read_tree`, `read_trees`, tree views, and the reads lazy
  trees use to find a stored root to derive from) ask with `get` or
  `get_many` first, and `get_exact` only for the nodes they missed, all at
  once;
- a create's check of the contents it names (`get_stored` before), and a
  land batch's check of a server-built changeset's contents, which asks
  `get_exact` for each content its `contains_many` reported missing before
  refusing it;
- reads of file contents a stored changeset or tree names
  (`Repo::read_content`, `read_contents`): file reads, search and its
  indexing, owners, review rules, blame, merges, rename detection, and
  `GET /v1/blobs/{hash}` (clients fetch hashes they read from stored
  trees). They ask `get` (or `get_many`) and only after a miss
  `get_exact`, so a land another server made a moment ago is not a 404 or
  a "blob missing" error within the stamp's window.

A tree this process built or derived unstored and has not stored since (a
lazy create's, ADR 0069) is never asked exactly: a create on an unlanded
lazy create takes its parent's depth from memory instead of reading the
parent's root, a derivation continues past such a parent without asking,
and storing a tree over such a base skips the read of its root. In the
benchmark below these reads were all 962 forced scans, each a certain
miss; on S3 they would have been about 12 serialized LISTs a second. A
tree another process stored meanwhile derives to the same nodes, and a
depth counted from memory can only be too high.

Existence checks before an upload (`blobs/missing`, a create's prefetch, a
land batch's `contains_many` warm-up) use the stamps only: a false
"missing" costs a re-upload or the exact recheck above. So does a create's
check whether the root it built is already stored: nearly every lazy create
misses there, and a root in a pack another process published a moment ago
has its whole tree below it (packs are written whole, roots after their
nodes), so taking it for unstored only makes the new tree lazy over stored
nodes; a root an interrupted write left loose is found exactly, since loose
files are read directly. GC's exactness lives in the tombstones, which
`contains` checks as before. There is no negative cache.

### 4. Repack is unchanged
The filesystem store keeps reading a pack a repack deleted through the
handle it holds; the object store lists again on a 404; tombstones are
still read before the directory. A repack rescans explicitly at each step.

### 5. Metrics
- `leep_blob_pack_scans_total{reason}`: scans of `packs/` (listings in a
  bucket) by why: `first`, `miss_changed` (a stamp changed),
  `miss_unconfirmed` (section 1's guard), `miss_unstamped` (a bucket
  without `packs/.gen`), `miss_forced` (`get_exact`), `tombstones`
  (`contains` saw the tombstones change), `interval` (the 5 s full
  listing), `repack`, and `explicit` (a delete, a pack count, a listing
  for GC, a deleted pack found). A publish adds its own pack to its view
  without a scan; the next miss pays for it under `miss_changed`.
  `publish` was planned as a reason and is not needed.
- `leep_blob_miss_total{outcome=found_after_scan|absent}`: misses of the
  cached view and how they ended.
- `leep_blob_miss_seconds`: time to resolve one (buckets from 10 µs).
- `leep_blob_pack_view_age_seconds`: age of the view that answered the
  last miss.

## Consequences
- A miss on a filesystem store whose packs did not change costs two
  `stat`s and no lock: about 3.6 µs in the pack set in a debug build of the
  test (`packgen_tests::a_miss_on_an_unchanged_directory_does_not_scan`),
  against a mutex, a full tombstones read and a directory listing. Under
  load misses rescan at most ten times a second, and concurrent ones share
  a scan.
- An object store miss costs one small GET per 50 ms per process instead of a
  LIST each; a listing follows each publish once per process. A foreign
  pack is visible to `get` and `contains` within the 50 ms window, at once
  to `get_exact`. On the filesystem the window is 100 ms on an unconfirmed
  view and none on a confirmed one, for `get_exact` none at all.
- **Format.** `packs/.gen` is a new key in buckets (FORMATS.md); readers
  ignored unknown keys under `packs/` already, and the key never names a
  pack. Nothing changes on a filesystem.
- **Compatibility.** A bucket with `packs/.gen` read by an older server
  behaves as before. A newer server reading a bucket that an older server
  also publishes to sees the older server's packs after the 5 s full
  listing, or at once through `get_exact`; upgrade every server, as ADR
  0068 already asks before land packs are turned on.
- A network filesystem that caches directory attributes delays the
  filesystem stamp by its cache time; `get_exact` still scans, so
  referenced objects read exactly, but an existence check may answer
  "missing" for that long.
- The benchmark of the context is rerun in the "Results" section.

## Results
The clean benchmark of the context (release build, server on CPUs 0-1,
`leep-land-bench` on 2-3, Postgres, fresh database and filesystem store,
`--agents 1024 --overlap-pct 5 --base-lag 0 --subtrees 256
--fixture-files 20000 --files 2`, 20 s warm-up, 60 s window):

| | lands/s | land p50 | land p99 | 503s |
|---|---|---|---|---|
| main (before ADR 0069) | | | 4.5 s | 115 |
| lazy create trees (ADR 0069) | | | 12.4 s | 8,303 |
| with stamps (this ADR) | 221.6 | 4.04 s | 4.56 s | 483 |

All 483 503s were land admission refusals within two seconds at the start
of the warm-up, when 1,024 agents arrived at once; none fell in the
measured window. Over the whole run (setup included) the store resolved
19,010 view misses (38,984 hashes, all absent), 91% of them in under 25 µs
and 0.88 s in all, with 1,872 scans: 623 `miss_changed` (217 land packs
and 6 background merges were published), 250 `miss_unconfirmed`, 962
`miss_forced` (exact tree reads of slow-path lands), 36 `repack`, 1
`first`. Before, each miss was a scan. Land latency is now the land
queue: lands waited 3.6 s on average for their batch, about the 5 s
admission bound, with the leader replaying 217 batches of 64.
