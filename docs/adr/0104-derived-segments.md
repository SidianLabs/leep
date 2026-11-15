# 0104. Derived segments: skiplists and history roots leave the database
Status: Accepted
Date: 2026-11-15

Amends [0045](0045-history-packs.md) (where part 0 of a history pack, the
`history-tree-v4` value, may be stored) and the storage of `skiplist`
rows. First step of the storage plan's item A1 (pure-function derived data
as segments in the blob store); supersedes its item M4 (a row encoding of
these two kinds). Follows the pattern of
[0103](0103-gc-mark-segments.md): segment blobs behind an object prefix with
a nonce, a manifest row swapped only by `cas_derived`, blobs durable before
the swap, lease-guarded background work, and blob GC that never condemns a
live segment.

## Context
After ADR 0100 to 0103, corpus B (Leep's history plus 2,000 edits) takes
23.54 MB on Postgres, 3.58 times Git; the plan's target is at most twice.
Two derived kinds hold one row per changeset that is a pure function of
history:

| B, measured | `skiplist` | `history-tree-v4` |
|---|---|---|
| Rows | 2,231 | 2,231 |
| Key and value bytes | 977,832 (438 a row) | 1,839,483 (824 a row) |
| Where the bytes go | 74% ancestor ids, 19% generations | 74% the part's table of changeset ids |

Postgres adds about 130 bytes of row and index overhead to each, so about
0.58 MB more. The ids repeat: each skiplist names about log2(n) ancestors
that other rows name too, and 41,847 table slots of the history roots name
only 2,216 distinct ids, nearly all first-parent ancestors close by. ids do
not compress inside a row (zstd saved 2% where the writer tried).

## Options considered
On B, key and value bytes of the two kinds:

| Option | Total | Why not |
|---|---|---|
| Today | 2.82 MB | |
| Store-wide changeset numbers (a varint per id) | 0.68 to 0.77 MB, plus 8 B and an index entry a changeset | A number is assigned by one store: a value copied, restored or replicated beside a database of another vintage names other changesets with a valid checksum, where today every mix-up is a hash mismatch. It needs a sequence in the land transaction, a backfill, and old binaries that insert without it; a one-way door that makes derived data store-local. |
| Skiplists as one jump pointer and the parent | 2.03 MB | -0.79 MB only, and ancestry reads from the head go from 5.4 rows (max 11) to 17.8 (max 24). |
| zstd of rows with a dictionary | small | ids are random bytes; the repetition is across rows, not in them. |
| Wait for the general A1 change | | These two kinds are the easiest slice (point reads by changeset id, one writer, re-derivable), and the machinery exists. The general change would land the container, compactor, read path and four kinds at once. |
| **Segments in the blob store, ids as positions** | **0.27 MB** in one segment, including 78 KB of key index | chosen |

## Decision

### 1. Invariants
1. Kind names, keys and values do not change. A segment read returns what a
   row read would: `skiplist` byte for byte; `history-tree-v4` the same part
   by ADR 0045's definition (equal checksum and uncompressed bytes, given
   back uncompressed, encoding `0`). No `v5`, no golden byte of an existing
   format changes, no `Changeset`, `TreeNode`, `Hash` or `/v1` change.
2. A segment only holds values that were read from rows, and checked as
   their kind (a skiplist that decodes and does not name its own
   changeset; a history part whose checksum and structure hold). Nothing
   derives into a segment.
3. Every failure reads as "row missing", which already means "derive
   again". No path returns a wrong value: blobs are checked by address,
   rows are compared before they are deleted, and rows win over segments.

### 2. Segments
A segment is two immutable blobs in the blob store, both addressed by
`Hash::of_blob` and starting with an object preimage prefix, so no upload
can take their address (ADR 0041), and both carrying the same 8-byte
nonce, so writing the same rows again never lands on a stored copy that
reads damaged (ADR 0103). Bytes are in FORMATS.md.

- **Index blob** (`leep:derived-segment-index-v1\0`): the family, the keys
  sorted, each key's **ordinal**, and per **column** a presence bitmap by
  ordinal and a directory of blocks (first ordinal, offset, length and
  uncompressed length in the data blob); then the data blob's address and
  length. Servers keep indexes in memory, about 40 bytes a key.
- **Data blob** (`leep:derived-segment-data-v1\0`): a table of external ids,
  then per column zstd-9 blocks of about 64 KiB of `varint length | row`,
  rows in ordinal order.
- **Ordinals** number the keys by `(generation, id)`, so ancestors come
  first. An **id reference** is a varint `r`: even, the key at ordinal
  `own - 1 - r/2`; odd, external id `r >> 1`. On a linear chain a skiplist
  becomes 0, 1, 3, 7, ...: the column falls to about 1.4 bytes a row. Any
  order decodes correctly; the generation order only makes references small.
- **Family** `changeset-v1` (keys are changeset ids) and its **columns**,
  numbered forever: 1 `skiplist` (entry count, then per entry an id
  reference; the first generation, then each the previous minus it, which
  must be positive), 2 `history-tree-v4` part 0 (the table's length and its
  id references, then the part's bytes after its table verbatim; the
  8-byte checksum is left out and computed again on read). Later kinds
  (`log-filter-v1`, `history-node-v4`, git maps) take new column numbers,
  and hash-keyed kinds a new family, without re-encoding a stored segment.
- **Caps**: at most 16,384 keys and 4 MiB of data a segment, so a reader
  takes one whole `get` (packed blobs have no ranged read; the block
  directory allows ranged reads later).

`history-node-v4` (parts after the first) is not in this change: its keys
are part hashes, not changeset ids, so it needs a hash-keyed family; on B
it has no rows.

### 3. The manifest
One derived row per family, kind `derived-segments-v1`, key
`Hash::of_object("derived-segment-family", "changeset-v1")`: version,
flags, family, the live segments in publish order (index and data
addresses, keys, a column mask, data bytes), up to 2,048 blobs of retired
segments, and a trailing BLAKE3. A key in several live segments answers
from the newest whose mask has the column. The row is replaced whole, and
only by `MetaStore::cas_derived` against the bytes its writer last read or
wrote (ADR 0103); a writer that loses the swap stops. `repair drop` refuses
the kind.

### 4. The compactor
A pass runs every `--derived-segments-interval` (5 minutes) in the index
lane (ADR 0073) under the store-wide lease `maintenance/derived-segments`,
one server at a time. It lists the keys of both kinds (bounded pages), and:

1. **Seals** the keys whose rows no live segment holds, once at least 1,024
   changesets are unsealed, or at least 64 and their number has not grown
   for 10 minutes. A changeset with one of its two rows waits 10 minutes for
   the other (the warmer derives `history-tree-v4` after the land), then is
   sealed with the one; its other row is sealed later, in another segment
   (a key may be in two segments with different columns). Rows are read
   with their generations, ordered by `(generation, id)`, cut into pages of
   at most 16,384 and encoded (halved until the data fits 4 MiB); both blobs
   are made durable (`put_rescuing_many_loose`), and only then is the
   manifest swapped to name them. A backfill is the same step, restartable
   anywhere, since "already sealed" is read from the manifest.
2. **Merges** runs of four adjacent live segments of one tier (`floor(log4(keys
   / 1,024))`), the longest prefix of the run that fits the caps (two or
   more), so a row is rewritten about three times at most. Adjacent, so a key
   in several segments still answers from the newest. Keys of changesets no
   longer stored (the changeset sweep, ADR 0059) are left out. The replaced
   segments are retired: still stored, collected by blob GC.
3. **Retires rows** (mode `retire` only): for each row whose newest holding
   segment this process has seen live for at least 10 minutes (counted from
   its own publish), it reads the manifest again under the lease (it must be
   the one the pass holds), reads the row and the segment's value, and
   deletes the row only if they are equal, in steps of 1,000 keys with a
   lease check before each and a rest after (ADR 0052). An unequal row stays
   (rows win) and is counted in `leep_derived_segment_mismatch_total`. The
   store feature `derived-segments` is recorded before the first delete of
   each process.

It also drops from the manifest a live segment whose index is not stored or
does not decode, or whose data blob is not stored (its keys read as rows or
derive again, and seal again), and forgets retired blobs no longer stored.
A transient read error aborts the pass. A manifest that does not decode is
replaced in mode `build`, where every row is still stored, and refused in
mode `retire`, where it may name the only copy of deleted rows.

Lands are untouched: `skiplist` is still written in
`put_changeset_with_derived` and `history-tree-v4` by the warmer.

### 5. The read path
`Repo::new` puts a `MetaStore` view over the store, so every read site
(ancestry, blame, filtered and path logs, prefetch, skip plans, the land
batch's skiplist plans, `derive`, `verify`) reads through it unchanged:

1. the database, exactly the statement issued before;
2. for misses, the in-memory indexes, newest segment first; a hit
   decompresses one block (kept under `--derive-cache-bytes`, as is the
   data blob, read once per process);
3. before a miss is declared, a manifest older than 60 s is read again,
   and the indexes of new segments loaded;
4. only then "missing": derive, or `indexing` for a bounded query.

**Ordering.** A row is deleted at least 10 minutes after its segment was
published, and a miss is judged against a manifest at most 60 s old, so a
reader that misses a row finds its segment. A data blob that is gone
(merged away and collected after the grace and fence, or lost) makes the
reader read the manifest again and look once more, then read as missing.

**Statements.** Per operation unchanged (ADR 0081): a sealed key costs the
same round trip, which now misses, and a memory lookup. The manifest is
read only for a miss, at most once a minute per process (in mode `build`
every row is there, so never), and a `get_derived_kinds` of the two kinds
carries it as one more read of the same call. `budget_tests` measures a
create on a sealed and retired store at the same 13 metadata calls, plus
two blob reads (the index and data blob, once per process).

`count_derived` and `estimate_derived` of the two kinds count rows and
sealed keys together, so `upgrade-index` does not see a retired store as
incomplete; `derived_present` consults the indexes. Listings stay rows.

### 6. Blob GC, the changeset sweep, verify, repair, backups
- **Blob GC** reads the manifest in condemn and again in collect (after the
  fence): live segments' blobs are never condemned (and are rescued if an
  older `gc` condemned them); retired ones are collected like orphans,
  through condemn, fence and collect, and reported apart (`derived
  segments`, `retired derived segment blobs condemned` and `deleted`). A
  manifest that does not decode stops GC, which could not tell live
  segments from orphans.
- **The changeset sweep** still deletes rows; a swept changeset's entries
  stay in segments until a merge leaves them out. They are pure functions
  of the id, so they are right if the changeset is stored again.
- **`verify`** reads through the view, so segment values are checked like
  rows, and checks the manifest (`bad_derived_manifest`) and every live
  segment whole (`bad_derived_segment`); a segment that is not stored is a
  note.
- **`repair drop --kind skiplist|history-tree-v4`** clears that column in
  every live segment's mask by swap first (a segment left with none is
  retired), then deletes the rows, holding the derived-segments lease
  throughout so no pass seals what it drops. With a manifest that does not
  decode, it replaces it with an empty one.
- **`repair rederive`** of the two kinds writes rows where the value read
  (row or segment) is missing or wrong; rows win. It holds the lease, so no
  pass deletes a row it just wrote as equal to the value it replaced.
- **Backups** copy segments as blobs. A snapshot naming a segment the blob
  copy lacks reads those values as missing: they derive again on demand,
  `verify` notes the segment, `repair rederive --missing` fills the rows,
  and the next pass drops it.

### 7. Rollout, mixed fleets, rollback: the one-way door
`--derived-segments off|build|retire`, default `build`.

| Situation | What happens |
|---|---|
| `build`, older servers beside new | Older servers read rows, all still present, and ignore the manifest kind. |
| `build`, an older `gc` | May condemn and delete live segments as orphans. Every row is still there; the next pass finds the segment unreadable, drops it and seals again. Costs a rebuild. |
| Every binary rolled back while in `build` | Free. Leftover segments are orphans, the manifest row is ignored. |
| `off` | No passes; existing segments are still read. |
| **`retire`: the one-way door** | The server records the store feature `derived-segments` (ADR 0055) **before the first delete**. Binaries that predate this ADR then refuse the store: without the rows they would derive whole histories again from the root (hours on a large store), and their `gc` would delete live segments. Going back means restoring a backup taken before the feature was recorded. |
| After `retire`, between releases that know segments | Free in both directions. |
| Binaries from before ADR 0055 | Do not read features: upgrade them first, as for every feature. |

`retire` is opt-in: an operator sets it once every binary that touches the
store runs this release (OPERATIONS.md, "Derived segments"). Postgres and
SQLite reuse the pages of deleted rows for later rows, but return them to
the operating system only on `VACUUM FULL derived` (or `pg_repack`) and
`VACUUM`, as with ADR 0101's pass.

### 8. Concurrency and crashes
- Killed before the manifest swap: unnamed blobs, orphans that blob GC
  collects; the old manifest stands. After it: the segment is durable and
  named, and the rows are still there; the next pass goes on.
- Killed while retiring: every row deleted was first compared with the
  value of a live, durable segment named by the manifest the pass read
  under the lease.
- A stale lease holder (a pass that stalled past the 60 s lease while
  another took it): its manifest swap fails on the compare-and-swap and it
  stops. Its deletes are preceded by a lease check and a fresh read of the
  manifest; the remaining window can only delete a row whose segment was
  just dropped, which reads as missing and derives again.
- A segment lost or damaged (address, prefix, version, structure): its keys
  read as rows or derive again, and the next pass drops it and seals again.
  A transient read error never does: it fails the read or the pass.
- `repair drop` and `repair rederive` hold the same lease as passes.
- The land path, `cas_ref`, the ref log and the changeset transaction are
  not touched.

## Cost
- Memory: about 40 bytes a sealed changeset of indexes per server (B 90 KB;
  1.2 million changesets about 48 MB), plus the block and data caches
  (16 and 32 MiB by default, part of `--derive-cache-bytes`).
- A cold read of a sealed key reads its segment's data blob once per process
  (at most 4 MiB: milliseconds on a filesystem, 50 to 150 ms on an object
  store), and a new manifest makes each process read the new index blobs.
- Write amplification: a row is sealed once and merged about three times.
- Retired segments wait for the operator's `gc`: up to about twice the
  live segment bytes in the meantime.
- In mode `build` a pass lists every key of both kinds each time (2.4
  million keys on a Linux-sized store); in mode `retire` only the unsealed
  ones remain to list.

## Consequences
- `leep-server --derived-segments build` (the default) seals in the
  background and keeps every row: free to roll back, and it costs the
  segments (0.27 MB on B) until the operator sets `retire`. Measured with
  `retire` (docs/PERFORMANCE.md, "Derived segments"): corpus B after
  `VACUUM FULL` takes 15.87 MB on Postgres instead of 19.09 (2.41x Git
  instead of 2.90x) and 15.86 MB on SQLite instead of 19.15; corpus A 11.06
  instead of 11.40 and 11.72 instead of 12.06. Its 2.82 MB of rows became
  one segment of 0.27 MB, as the design's model said; about 1,264 bytes of
  rows a changeset become about 120. Before a vacuum Postgres keeps the
  freed pages for later rows.
- Deviations from the design while implementing: the manifest is read
  again only for a miss (and rides along in a `get_derived_kinds` call),
  not with every read (a single read turned into a batched one changed
  failure behaviour); merges take the longest prefix of a run of four that
  fits the caps, since four seals of slightly over 1,024 would not; a
  segment's "live for 10 minutes" counts from its own publish in the
  publishing process; `history-node-v4` waits for a hash-keyed family; a
  damaged manifest is replaced automatically only in mode `build`.
- `gc` prints derived segments; `verify` gains `bad_derived_manifest` and
  `bad_derived_segment`; `repair drop` refuses `derived-segments-v1`.
- The default `--derive-cache-bytes` grows by 48 MiB (the segment caches).
- New metrics: `leep_derived_segment_passes_total`,
  `leep_derived_segment_reads_total`, `leep_derived_segment_errors_total`,
  `leep_derived_segment_mismatch_total`,
  `leep_derived_segment_rows_retired_total`, and gauges of live segments,
  their bytes and unsealed rows; alert `LeepDerivedSegmentPassesFailing`.
- Follow-ups of A1: more columns in `changeset-v1` (`log-filter-v1`, git
  maps), a hash-keyed family (`history-node-v4`, search), direct segment
  writes from the importer and bulk warmer (which removes the vacuum
  caveat), ranged reads for object stores, and a default of `retire` once
  fleets run this release.
