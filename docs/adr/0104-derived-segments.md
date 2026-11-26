# 0104. Derived segments: skiplists and history roots leave the database
Status: Accepted; amended by [0105](0105-derived-segment-families.md)
(families, forward compatibility, GC over every family)
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
flags, family, a sequence number, the live segments in publish order (index
and data addresses, keys, a column mask, data bytes), up to 2,048 blobs of
retired segments, and a trailing BLAKE3. A key in several live segments
answers from the newest whose mask has the column. The row is replaced
whole, and only by `MetaStore::cas_derived` against the bytes its writer
last read or wrote (ADR 0103); a writer that loses the swap stops. `repair
drop` refuses the kind.

- **Sequence.** Every write sets it to the previous value plus one, or the
  writer's clock in microseconds if that is more (a manifest written over
  a damaged row, whose number is unknown, still sorts after what readers
  hold). A reader never applies a manifest older than the one it uses, so
  a read that raced a newer write cannot take it back to segments a merge
  has retired. Two consequences: if an earlier writer's clock was ahead, a
  manifest written over a damaged row is ignored by running servers until
  the clocks catch up; and a restored database may hold an older manifest
  than servers hold. Restart servers after either (OPERATIONS.md).
- **Damage and newer formats.** A row whose checksum fails is damaged. A
  row whose checksum holds but whose version, flags or family this binary
  does not know was written by a newer binary: it is never treated as
  damage and never replaced. A pass then does nothing, `gc` and `repair
  drop` refuse, `verify` notes it, and readers keep the segments they had
  (or read rows only).

### 4. The compactor
A pass runs every `--derived-segments-interval` (5 minutes) in the index
lane (ADR 0073) under the store-wide lease `maintenance/derived-segments`,
one server at a time. It loads the live segments' indexes itself, never
through the shared read view (whose state a reader's transient error or a
late read may have moved), lists the keys of both kinds (bounded pages,
each joined against the segments' sorted keys in its range, keeping in
memory only the keys a seal or a retirement needs, not every key listed;
in mode `build` only once their estimated count grew by 1,024 since the
last listing or 10 minutes passed, so a quiet store is not listed every
pass), and:

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
   anywhere, since "already sealed" is read from the manifest. A listing's
   candidates are carried to the following passes, which go on without
   listing again: first their generations, in batches, then, sorted, their
   pages. A pass spends at most a minute on them (finishing the page it is
   on) and seals at most four pages, then gives the lease up, so a long
   backfill does not keep `repair` waiting, says it has more and is
   followed 5 s later instead of after the interval. The carried work is
   dropped, and the rows listed again, when a live segment appears that it
   does not account for (another server's pass, a `repair drop`) or one is
   dropped. The holder of each listed key is found by walking every live
   segment's keys once, newest first (for few keys, by looking each up),
   so a backfill costs one listing and one generation read per changeset,
   not one per pass ("Backfill cost" below). Keys whose changeset is no
   longer stored, and rows whose value does not decode, are remembered and
   skipped until the next periodic listing. A pass says it has more only
   if it made progress (generations read, a page consumed, rows deleted or
   found unequal), so nothing can keep passes 5 s apart for ever.
2. **Merges** runs of four adjacent live segments of one tier (`floor(log4(keys
   / 64))`, 64 the smallest seal, an idle one), the longest prefix of the run
   that fits the caps (two or more), so a row is rewritten about four times
   at most (amended 2026-11-25: tiers counted from 1,024 put idle seals and
   their merges all in tier 0, where a growing segment was rewritten with
   every three new seals, about 20 times). Adjacent, so a key
   in several segments still answers from the newest. Keys of changesets no
   longer stored (the changeset sweep, ADR 0059) are left out. The replaced
   segments are retired: still stored, collected by blob GC.
3. **Retires rows** (mode `retire` only): for each row whose newest holding
   segment this process has seen live for at least 10 minutes (counted from
   its own publish), it reads the manifest again under the lease (it must be
   the one the pass holds), reads the row and the segment's value, and
   deletes the row only if they are equal and only if the row still holds
   the bytes it compared (`MetaStore::delete_derived_values`, one statement
   in the SQL backends), in steps of 1,000 keys with a lease check before
   each and a rest after (ADR 0052). An unequal row stays (rows win), is
   counted in `leep_derived_segment_mismatch_total` once, and is not read
   again while the same segment holds the key. The store feature
   `derived-segments` is recorded before the first delete of each process.
   Candidates are taken one holding segment at a time, so one data blob is
   in memory at once; a segment value that cannot be read ends the step
   (the rows compared before it are still deleted). A pass deletes at most
   65,536 rows or for at most a minute, then gives the lease up and is
   followed 5 s later, carrying the rest of its candidates (dropped like a
   backfill's). No retirement runs while a seal backfill is under way:
   each pass would list every row again to find the few newly ripe ones.

It probes every live segment itself: the index blob, and the data blob read
whole and decoded the first time this process sees the segment (only its
presence afterwards). A segment whose blobs are definitely lost or damaged
(not stored, address or structure wrong, a read the store reports corrupt)
is dropped from the manifest (its keys read as rows or derive again, and
seal again); on a store with the feature `derived-segments` recorded, only
when two passes in a row agree, so a blob a repack was moving is not lost
for good (counted as such also on a store that cannot record features
once this process has retired rows). Any other read error fails the pass
and drops nothing. A segment whose data a merge or a retirement cannot read
is left alone for the rest of the pass (a merge never picks it again, so
the pass ends) and read whole by the next pass's probe. Decoded indexes the
read view already holds are reused (immutable, checked by address), and
the pass still checks that their index blobs are stored, so a lost one is
noticed; the view's segment list and broken set are not reused. Retired
blobs no longer stored are forgotten. A manifest that does not decode is
replaced only by a pass in mode `build` on a store without the feature
recorded (asked of the store, not this process's flag: another server may
have retired rows); otherwise the pass refuses, since the manifest may name
the only copy of deleted rows.

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
only for rows not derived yet: a new changeset before the warmer, a root's
`history-tree-v4`), and a `get_derived_kinds` of the two kinds
carries it as one more read of the same call. `budget_tests` measures a
create on a sealed and retired store at the same 13 metadata calls, plus
two blob reads (the index and data blob, once per process).

`count_derived` and `estimate_derived` of the two kinds count rows and
sealed keys together, so `upgrade-index` does not see a retired store as
incomplete (`count_derived` pages through every row key once segments
exist, to count each key once; it is an operator tool's call); `derived_present`
consults the indexes. Listings stay rows.

**Start.** A server loads the live segments' indexes in the background at
start, a few blobs at a time, so its first miss does not wait for all of
them; `leep_derived_segment_index_bytes` reports what they hold.

### 6. Blob GC, the changeset sweep, verify, repair, backups
- **Blob GC** reads the manifest in condemn and again in collect (after the
  fence): live segments' blobs are never condemned (and are rescued if an
  older `gc` condemned them); retired ones are collected like orphans,
  through condemn, fence and collect, and reported apart (`derived
  segments`, `retired derived segment blobs condemned` and `deleted`). A
  manifest that does not decode makes GC keep every blob that starts like
  a segment, since it cannot tell live segments from orphans, and collect
  everything else (amended 2026-11-25: it stopped all blob GC).
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
  decode, it replaces it with an empty one. Other servers keep answering
  from the old manifest until they read it again, up to a minute (they do
  so at their next miss after 60 s): the values are the same pure
  functions, but to make every server derive again at once, restart them
  or wait a minute (OPERATIONS.md, "Derived segments"). Within that minute
  a server may derive and store new rows from a stale segment value, so a
  drop meant to purge wrong values needs the other servers stopped or
  restarted first, or a second drop after a minute.
- **`repair rederive`** of the two kinds writes rows where the value read
  (row or segment) is missing or wrong; rows win. It holds the lease, and a
  pass deletes a row only if it still holds the bytes the pass compared, so
  even a pass that outlived its lease cannot delete a row `repair` just
  rewrote.
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
  manifest, and delete only rows still holding the bytes compared; the
  remaining window can only delete a row whose segment was just dropped,
  which reads as missing and derives again.
- Clocks. The 10 minutes before a row is deleted and the lease's renewal
  are measured on the host's monotonic clock. A host suspended past them (a
  paused VM, a laptop asleep) may resume believing a segment old enough, or
  its lease held, when other servers saw neither: the lease check and the
  value-conditional delete bound what it can do to deleting rows equal to
  a segment named by the manifest it read, and a reader whose manifest is
  older than that segment reads the manifest again before it declares a
  miss, as with ADR 0012's VM freeze.
- A segment lost or damaged (address, prefix, version, structure): its keys
  read as rows or derive again, and the next pass drops it and seals again.
  Every "not stored" is exact (`get_exact`, ADR 0070), so a blob another
  process packed a moment ago is never taken for lost.
- A segment whose blob fails to read with any other error (an I/O error,
  a failing object store key). Amended 2026-11-25: before, one such index
  blob failed every miss of its family on every server, and every pass,
  for as long as the error lasted. Now readers leave that segment alone
  out of their view for a minute (its keys read as rows or derive again,
  every other segment still answers) and try it again; a pass keeps it,
  leaves it out of its merges and retirement, and drops it only once it
  failed three passes in a row over at least
  `--derived-segments-drop-failing-after` (a day; `off` never) while
  another segment of the family reads whole in the same pass (a real read
  of its data, not the existence check a segment read whole earlier gets:
  a store can answer those while its reads fail; passes report it as
  `failing`). A streak restarts when this process's last pass of the
  family ended more than about two pass intervals ago, so a lease handoff
  and one blip do not add up, and a pass longer than the interval does not
  end it.
  The default is long because after `retire` a dropped segment's rows
  exist nowhere else, and a partial outage (a degraded node, timeouts on
  large blobs only) can fail a good segment for hours. When no live
  segment of the family reads whole (a family whose only live segment
  fails included, which therefore fails every pass), the store is taken
  to be failing: the pass fails and drops nothing.
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
- Write amplification: a row is sealed once and merged about four times.
- Retired segments wait for the operator's `gc`: up to about twice the
  live segment bytes in the meantime.
- In mode `build` a pass lists every key of both kinds whenever they grew
  by 1,024 or 10 minutes passed (2.4 million keys on a Linux-sized store);
  in mode `retire` only the unsealed ones remain to list, every pass but
  those carrying a retirement on.
- **Backfill cost.** A first seal of a store with history lists every key
  once and reads every changeset's generation once (batches of 1,000,
  until a minute after the pass started), then seals 65,536 changesets a
  pass, 5 s apart, without listing again; finding each key's holder walks
  the live segments' keys once. The listing is the one step not bounded
  in time. Measured on Postgres 16 with synthetic chains of one kind
  (`crates/leep-server/tests/derived_segments_backfill.rs`): 500,000
  changesets seal in 8 passes, the first 6.2 s and each after it 0.4 to
  0.6 s (10.1 s of passes; listing again every pass, as first released,
  took 4.4 to 5.1 s a pass, 37.4 s, and grew with the store); 2,000,000
  in 31 passes, the first 19.8 s and each after it about 0.8 s (44.7 s).
  Retiring is bound by its deletes and the equal rest after each: 45 s of
  passes for 500,000 rows, 106 s for 2,000,000, at most 5.7 s a pass.
  Every pass stays far below the two minutes `repair` waits for the lease.

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
- Changed after a second review: a merge or retirement skips a segment
  it cannot read for the rest of the pass (it used to pick the same run
  again forever with the lease held); backfills and long retirements are
  bounded per pass and continued 5 s later; passes reuse the view's decoded
  indexes; the Postgres conditional delete locks rows in key order.
- Changed after a third review: a backfill's and a retirement's candidates
  are carried across passes instead of listed again by each (which made a
  backfill quadratic), generations are read once and within a time bound,
  holders are found in one walk of the segments' keys; a pass says it has
  more only if it made progress, and rows that do not decode are skipped
  like unstored ones; reused indexes have their blobs probed; a
  retirement step stops at a value it cannot read; both Postgres
  `derived` deletes by key lock rows in `(kind, key)` order.
- Changed after review, before release: the manifest gained its sequence
  number; a damaged manifest is replaced only where the store has no
  `derived-segments` feature, and a newer one never; passes load and probe
  segments themselves, and on a store with the feature drop one only when
  two passes agree; row deletes are value-conditional.
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
