# 0103. Garbage collection marks as sorted segments in the blob store
Status: Accepted
Date: 2026-11-15

Amends [0033](0033-incremental-gc-mark.md) (where the record of a mark is
kept, and how a run trusts it). The closure rule of 0033 is unchanged and is
restated below for segments. Defers item Q6 of the storage plan (the
`derived` table layout); see "Q6, deferred".

## Context
Blob GC marks incrementally (ADR 0033): every object a mark walks gets a
`gc-mark-v1` row, written in post-order, so a row vouches for everything
below it and the next run stops there. A row holds one bit of information,
but costs a 32-byte key, a one-byte value, the kind name and the row and
index overhead of the `derived` table: about 167 bytes on Postgres. On
corpus B (Leep's own history plus 2,000 edits) that is 3.1 MB, more than
any other bookkeeping, for about 18,500 rows; on SQLite about 110 bytes a
row. The storage plan's target is total storage at most twice Git's
with indexes on, and the plan's item M3 is the largest remaining layout
waste after PRs 1 to 3 (ADR 0100, 0101, 0102).

The marks are written by one process at a time (the `gc` lease, ADR 0059
section 6), read only by GC runs and `verify`, never deleted one by one
(ADR 0033: dropping part of the kind breaks the closure), and their only
query is "is this hash marked?". That is a sorted list of hashes, not a
table.

## Decision

### 1. Segments
A **segment** is an immutable blob in the blob store holding sorted,
distinct 32-byte hashes: the object preimage prefix
`leep:gc-mark-segment-v1` and a NUL, one version byte `0x01`, an 8-byte
nonce, then the hashes in ascending byte order. Its address is the BLAKE3
of those bytes (`Hash::of_blob`), which is also
`Hash::of_object("gc-mark-segment-v1", ..)` of what follows the prefix:

- to the blob store, the repack, backups and every older binary it is a
  blob like any other: it is packed and delta-compressed, copied by
  `backup`, read by every version, and never reads as corrupt (the store
  checks every object as a blob or a tree node). A first draft addressed
  segments as objects of their own kind; repacks then left them loose as
  "corrupt" and backups skipped them, which the restore drill caught;
- no upload can take its address: uploads whose bytes start with an object
  prefix are refused (ADR 0041).

The nonce makes every written segment a new object: publishing the same
marks again never lands on a stored copy that reads damaged, which a
content-addressed put would keep. 32 bytes a mark plus 33 per segment.

A reader checks the address (BLAKE3 of the bytes), the prefix, the
version, the length and that the hashes strictly ascend. Lookups are exact
binary searches. **Never anything approximate**: a false "marked" on a tree node stops the
walk there and condemns its subtree, which is data loss, so Bloom filters
or any other probabilistic structure are ruled out for this record.

### 2. The manifest
One derived row, kind `gc-mark-segments-v1`, key 32 zero bytes, names the
live segments in the order they were published, each with its mark count;
a flag `rows_retired`; and up to 1,024 retired segments, for reporting. It
ends with the BLAKE3 of the rest, so a damaged read is detected (bytes in
FORMATS.md). A manifest row is replaced whole, and **only by a
compare-and-swap** against the exact bytes its writer last read or wrote
(`MetaStore::cas_derived`, one statement in every backend; "no row" is a
valid expected state). A writer that loses the swap fails with `Conflict`
and stops (section 8).

### 3. Publication: the post-order prefix guarantee, restated
A run emits marks in post-order exactly as before (ADR 0033): a tree
node's mark after its entries', a changeset's after its `Modify` contents
and its root tree. So every prefix of what a run emitted is closed: each
mark's closure lies in the prefix or was recorded before the run.

A run collects its marks in memory and **publishes a prefix** every 262,144
marks (8 MiB) and at the end of its mark:

1. sort the prefix and write it as a segment;
2. make it durable (`put_rescuing_many_loose`, which flushes, and rescues
   it if an older GC condemned identical bytes);
3. only then swap the manifest to one naming it, after the segments it
   already named.

A run that replaces the record (section 5) does steps 1 and 2 at each
checkpoint, but not 3: its segments are named only at its end.

Invariant: **every prefix of the manifest's live list (together with the
`gc-mark-v1` rows while they are not retired) is closed.** It holds
initially (no segments) and each step keeps it: an appended segment is a
closed-by-induction prefix of a run whose own reads trusted only an earlier
prefix of the list. A run killed before step 3 leaves an unnamed segment,
an orphan blob GC collects like any other; nothing names it, so it changes
no answer.

A reader trusts **the longest prefix of the live list it can read in full**,
in order. A segment that is not stored, that the store reports damaged or
that does not decode ends what it trusts there: the segments after it may
rest on it. Missing marks only make a run walk more. A transient read
error aborts the run, like every other error before anything is condemned. A run that could not read a segment
publishes a manifest without it and the segments after it (they are
retired), even if it marked nothing new.

### 4. Merges
After a publish, while the newest segment holds at least half as many
marks as the one before it, and the two together hold at most 4,194,304
marks (128 MiB, the segment cap), the two are merged into one (written and
made durable first) and replace the pair in one manifest write. Only
adjacent segments merge, so every remaining prefix is a prefix of the old
list: the invariant holds. Below the cap segment sizes form a geometric
series, so a store keeps about log2(marks) segments plus one per 2 to 4
Mi marks beyond the cap, and a mark is rewritten at most about log2(cap)
times over its life. The cap keeps any one merge (and so any one run's
extra memory and writes) bounded however large the store grows.

The segments a merge replaced are **retired**: no longer named as live,
still stored. Blob GC collects them like orphans, through condemn, fence
and collect (ADR 0012), so they outlive their retirement by at least the
grace window and the fence. Only GC runs and `verify` read segments; GC
runs never overlap (the `gc` lease), so the only readers that can hold an
older manifest are dry runs and `verify`, and if one reads a segment
already collected it trusts a shorter prefix and walks more. `condemn` and
`collect` report retired segments apart (`segments_condemned`,
`segments_deleted`) and leave them out of `blobs_scanned`, `condemned` and
`deleted`; live segments are never condemned, and a live one an older `gc`
condemned is rescued by the next collect.

### 5. Full marks and damaged manifests replace the record
`gc --full` trusts no earlier record. Its checkpoints write segments and
make them durable but name none of them: until its end the old manifest
stands, so a full run killed part way leaves the old record whole and
orphan segments that blob GC collects like any orphan. At the end it
merges runs of its own adjacent segments up to the cap (normally into one)
and swaps in a manifest naming only those, with `rows_retired` set: they
are closed on their own. Everything the old manifest named, and the
segments it merged away, are retired. This also sheds stale marks of
changesets the changeset sweep deleted (below).

After a replacement the `gc-mark-v1` rows (if any) no longer count; they
are dead weight until `gc --retire-mark-rows` deletes them.

A manifest that does not decode tells nothing, not even whether the rows
were retired and may be half deleted. A run that reads one trusts nothing,
rows included, walks everything like a full mark, and replaces it.

### 6. The `gc-mark-v1` rows: dual read, then retirement
New binaries never write `gc-mark-v1`. While the manifest says the rows
are not retired, they still count as marks (dual read): a run asks the
segments in memory first, then the rows of what is left, in one batched
read as before. Segments a new run publishes may rest on the rows, which
is why the rows must count until they are retired.

Retirement is an explicit, one-time operator step,
`leep-server gc --retire-mark-rows`:

1. **Import**, under the `gc` lease: page through the rows in key order
   (already sorted), write them as one segment and put it **first** in the
   live list, then set `rows_retired` in the same manifest swap (from the
   bytes the import read). The rows are closed at any instant (0033), the
   lease keeps any GC of this release from writing the manifest while the
   pass runs, and every segment after the new first one rested only on rows
   it now holds, so the invariant holds without them. The import is refused
   on a store without leases, and needs that no `gc` from before this ADR
   (which writes rows and does not know the manifest) runs meanwhile. The
   imported segment is one whatever its size, above the cap if need be:
   the rows have no post-order, so no part of them is closed on its own.
2. **Record the store feature `gc-mark-segments`** (ADR 0055).
3. **Delete** the rows in bounded steps (ADR 0052), under the lease, after
   reading the manifest under it and finding `rows_retired` set.

The feature comes before the delete because a delete cut short leaves rows
whose subtrees lost theirs, and a `gc` binary from before this ADR trusts
whatever rows it finds. Once the feature is recorded such a binary refuses
to open the store.

### 7. Mixed fleets, rollback, and the one-way point
| Situation | What happens |
|---|---|
| New `gc`, old servers | Servers never read marks; nothing changes for them. |
| Old `gc` after a new one, rows not retired | It trusts the rows (still closed: new binaries never write them), walks what they lack and writes rows. It does not know segments: it may condemn and later delete live ones as orphans. The next new run finds them missing, trusts a shorter prefix (plus the rows) and walks more, then republishes. Safe; costs a walk. |
| New `gc` after an old one | Rows written by the old one count (not retired); its deletions of segments only shorten the trusted prefix. |
| Rollback of every binary before `--retire-mark-rows` | Free. The manifest row is a kind old binaries ignore; leftover segments are collected by the old `gc` as orphans. |
| After `--retire-mark-rows` | **One-way.** Every binary that reads `store_features` but predates this ADR refuses the store, servers included. Rolling back means restoring a backup from before step 2. Binaries from before `store_features` (ADR 0055) do not check: as for every feature, upgrade them first. |
| Restored backup | A metadata snapshot and the blob copy are taken at different instants. A manifest naming a segment the blob copy lacks only shortens the trusted prefix (`verify` notes it); segments the snapshot does not name are orphans. |

Run `--retire-mark-rows` once no binary older than this one will run
against the store. Until then the rows cost what they did.

### 8. Concurrency and crashes
- **GC and land.** As before: a changeset stored after the run listed
  changesets has no mark and the next run walks it; lands never read marks.
- **Crash mid-publish.** Before the manifest write: an unnamed segment
  (orphan), and the previous manifest stands. After it: the new segment is
  durable and named. A merge killed half way likewise leaves orphans.
- **Merges racing readers.** See section 4: only dry runs and `verify` can
  read an older manifest; a collected segment shortens their prefix.
- **A segment that cannot be read.** Not stored (the store answers "no
  such blob"), bytes that fail the checks (address, prefix, version,
  length, order), or a read the store answers as definitely corrupt (a
  packed copy failing its checksum: `Encoding`, or a storage error starting
  "corrupt"): the prefix before it is trusted, the run logs a warning and
  republishes without it and the segments after it, which retires them for
  good. Any other read *error* aborts the run like every other read error,
  so a transient failure does not throw away the record: a lost connection,
  but also `NotFound` and a storage error saying "missing" (a pack listed
  whose file is gone), which a repack caught mid-swap can give. This is
  narrower than the test `verify` uses to file damage, which counts those
  too. `verify` reports a damaged segment as `bad_gc_segment`.
- **Rows deleted under a stale run.** A run that loaded with the rows
  counting answers "marked?" from them for as long as it runs. If it
  outlived its lease while `--retire-mark-rows` imported and deleted them,
  a row read would say "no row" for an object under a row-covered
  changeset, and collect would delete it. So condemn and collect read the
  manifest again before their blob loops, and after every row read they act
  on, and stop with `Conflict` unless it holds exactly the bytes the run
  last read or wrote. The import swaps the manifest before any row is
  deleted, so a row read followed by an unchanged manifest saw every row.
  Dry runs, which hold no lease and delete nothing, are not checked.
- **A stale lease holder.** A run can outlive its lease: it checks the
  lease before each manifest write, but may stall between the check and the
  write past the 60 s TTL while another holder takes the lease. Such a write
  must not land. Example: run A loads `[X1]` with `rows_retired` clear and
  stalls; `gc --retire-mark-rows` imports the rows into `R`, writes
  `[R, X1]` with `rows_retired` set and deletes the rows; if A then wrote
  `[X1, X2]` with `rows_retired` clear, `R` would be dropped, the rows
  gone, and the next run would trust a record that is not closed and
  condemn live objects. So **every manifest write is a compare-and-swap**
  against the bytes its writer last read or wrote (section 2): publish,
  the end of a run, forgetting retired segments, and the import. A's swap
  expects the `[X1]` bytes, finds `[R, X1]`, and fails with `Conflict`. A
  run whose swap fails stops before it condemns or deletes anything (the
  swaps of its mark come before both; one that fails when it forgets
  retired segments at the very end changes nothing). Its segments are
  orphans. The delete of the rows reads the manifest under the lease, and
  the manifest cannot then go back to counting rows: that would take a
  swap from bytes written before the import.

### 9. The changeset sweep
The sweep (ADR 0059) deletes a changeset's `gc-mark-v1` row with it. A
segment is immutable, so a deleted changeset's mark stays in its segment.
That mark, like the tree-node and blob marks ADR 0059 already leaves, only
keeps objects: if the changeset is stored again, its contents and root
tree are still marked, by marks that are never removed one by one. Only a
full mark removes marks, and it removes them all at once by replacing the
record (section 5).

### 10. Cost
A run reads every live segment it trusts, in full, at the start of its
mark, and keeps them in memory: 32 bytes a mark read and held (corpus B
about 0.6 MB; the 82,500-commit git/git of ADR 0033, 326,731 objects and
its changesets, about 13 MB; a Linux-sized store with about 10 million
objects about 320 MB read from the blob store in about three capped
segments plus a few small ones, less memory than a full mark's walked set
already takes). Lookups are binary searches in memory, so an incremental
run makes no row reads for marks once the rows are retired.

A lookup binary-searches every trusted segment in turn until one holds the
hash, so a miss (the common answer for a walked object) probes all of
them: about N/cap capped segments plus about log2(cap) smaller ones, each
probe about log2(segment size) comparisons. That is about 25 probes at
10 million marks, and about 250 at a billion (238 capped segments plus the
small ones, some 5,500 comparisons, a few microseconds of cache misses).
condemn asks once per stored blob and each run once per object it reaches,
so at a billion marks lookups alone would take hours a pass. Far below
that it is noise next to the reads. If stores get that large, the lookup
should become one probe: the blob listing and each page of tree entries
come in hash order, so a cursor per segment (a merge join), or one merged
in-memory index, answers in amortized constant time without changing
anything stored.

On top of that, a run holds the marks it has not published yet (at most
262,144, 8 MiB) and, while it merges, the merged copy: one merge reads
nothing new (both inputs are in memory) and writes at most the cap,
128 MiB. Amortized over its life a mark is rewritten by merges about
log2 times, at most about log2(cap) = 22 times; one run's merges each
write at most the cap, and there are at most about as many as there are
segments below the cap. A full run's end writes its record once more, in
capped segments. A follow-up can read
segments by range if the memory matters.

## Q6, deferred
The plan's item Q6 (store `derived.kind` as a `smallint` through a kind
registry, and make the SQLite table `WITHOUT ROWID`; measured 15.31 to
13.42 MB on corpus B on Postgres) is not in this change. The maintainer
asked for an online upgrade without downtime, and the online form is a
dual-table copy: a new table, writes to both (or to the new one with reads
falling back), a background backfill in batches under a lease that is
crash-safe and restartable, then a swap. In this codebase that means:

- every `derived` statement has a dual form: 22 in the Postgres backend and
  17 in SQLite, plus the snapshot source and sink (ADR 0026), the space
  report, and the physical-order drop walk of ADR 0052, which walks `ctid`
  (Postgres) and `rowid` (SQLite) ranges of one table;
- the land path writes derived rows in the changeset's transaction
  (`put_changeset_with_derived`, ADR 0031); writing both tables there adds
  write amplification to the hottest path for the whole backfill, and a
  read-with-fallback doubles point reads of missing rows;
- a schema migration in both backends with fixtures, and backups of both
  layouts;
- the kind registry must assign ids race-free across replicas and be in
  every backup.

That is a larger change than this PR and touches the land path's crash
safety; its saving (1.9 MB on B) is about the same as M3's. And if the
plan's item A1 (derived data as segments in the blob store) is accepted,
the kinds that move would be rewritten twice. It gets its own ADR.

## Consequences
- New stores never hold `gc-mark-v1` rows. Measured (docs/PERFORMANCE.md,
  "Marks as segments"): after one GC, corpus B takes 23.54 MB on Postgres
  instead of 25.96 MB (3.58x Git instead of 3.95x) and 20.78 MB on SQLite
  instead of 22.25 MB; its marks take 0.59 MB of segments instead of
  3.07 MB (Postgres) and 2.04 MB (SQLite) of rows. A GC after 400 lands
  adds 256 bytes a land instead of 1,311 (Postgres) and 881 (SQLite). GC
  passes got faster: on B, an incremental run 0.25 to 0.20 s (Postgres)
  and 0.32 to 0.20 s (SQLite), a first run 0.84 to 0.70 s and 1.32 to
  0.79 s. Lands are untouched (perf-ratio: server CPU per land 2.9 to
  4.2 ms before, 2.7 to 3.3 ms after).
- Every GC run with new marks writes a segment and a manifest row. A
  segment is a loose object until the background repack packs it.
- `gc` prints `mark segments` and the retired segments it condemned or
  deleted. `verify` checks the manifest (`bad_gc_row`) and the live
  segments (`bad_gc_segment`), and notes a segment that is not stored.
- `repair drop` refuses `gc-mark-segments-v1`.
- The rows of existing stores stay until `gc --retire-mark-rows`.
