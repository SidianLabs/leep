# 0038. Compact pack index
Status: Accepted
Date: 2026-10-17

## Context
A pack's index (ADR 0029) spends 96 bytes on each object: the 32-byte hash,
an 8-byte offset, stored length and logical length, a 32-byte base hash and
an 8-byte modification time. Measured, that is a large share of packed
storage:

| Store | Objects | Pack | Index | Index share of pack + index |
|---|---|---|---|---|
| First 3,795 commits of git/git | 11,735 | 3.1 MB | 1.1 MB | 27% |
| All of git/git | 326,731 | 167 MB | 31 MB | 16% |
| Linux, first 669,466 commits | 4,653,029 | 1.24 GB | 447 MB | 26% |

The index costs memory as well as disk. A store loads every index on first
use and kept it as a `Vec` of 104-byte entries, so the Linux slice took about
480 MB of resident memory per server. Loading did one binary search per delta
base to check it, which takes 11 s for 5 million objects.

Almost all of those bytes are zeros. On the 3,795-commit slice the largest
offset fits in 22 bits, the largest stored length in 15, the largest logical
length in 17, and the modification times span 41 seconds. A base hash only
names another record of the same index, so its rank needs 14 bits.

## Decision
New packs get a `LEEPIDX2` index. The pack file format and the pack id (the
BLAKE3 of the whole index, whose header holds the pack's length and BLAKE3)
do not change. The layout is in [FORMATS.md](../FORMATS.md), "Packs".

1. **Hashes in a column of their own**, sorted, after a 256-entry fanout
   table of cumulative counts by first byte (as in git's index). A lookup
   binary searches about n/256 hashes packed 32 bytes apart.
2. **One record per hash**, in hash order, of five big-endian fields: offset,
   stored length, base, logical length and modification time. Each field's
   width in bytes is chosen per pack as the fewest that hold its largest
   value, and the header records the five widths.
3. **The base is the base record's rank + 1**, or 0 for a whole entry. A
   reader checks that it is in range, not the record itself, and at a lower
   offset, as before. Bases still resolve within the pack, and chain caps do
   not change.
4. **Modification times are stored relative to the pack's earliest**, which
   the header holds, so exact seconds cost 1 to 3 bytes. GC compares exact
   seconds (ADR 0012, ADR 0033), so they are not rounded.
5. **The reader keeps the index as its file bytes** and reads records in
   place, without decoding it into a `Vec`. Memory per object is the index's
   size on disk. Loading checks the checksum, fanout, order, bounds and bases
   in one O(n) pass.
6. **`LEEPIDX1` is still read, and never written.** The same type reads both
   layouts. For a `LEEPIDX1` index it resolves each base hash to a rank once
   at load (4 bytes per object), so lookups do one search either way. A full
   repack rewrites every pack it replaces as `LEEPIDX2`.

## Measurements
On the 3,795-commit slice (`leep-server repack`), the same 3,100,053-byte
pack gets a 505,733-byte index instead of 1,126,648: 43.1 bytes per object
instead of 96 (widths 3, 2, 2, 3, 1 plus the hash). `get` latency did not
change (decoding delta chains dominates it). A synthetic index of 5 million
objects (the `pack_index_scale` example) takes 46 bytes per object on disk
and in memory, against 96 and 104. It loads in 1.5 s instead of 11 s, and a
lookup takes 0.9 us instead of 1.8 us. `docs/PERFORMANCE.md` ("Packed blobs")
has the numbers. Projected from the record counts above, 47 to 49 bytes per
object: about 15.5 MB for all of git/git and about 225 MB for the Linux
slice.

## Considered and rejected
- **Records in pack order with a hash-to-position permutation.** Frames are
  contiguous in pack order, so the stored length would follow from the next
  offset. But the permutation costs what the stored length does (3 bytes at
  these sizes). A chain step must also know its base's hash, for the
  decoded-object cache and the hash check, which needs the inverse map as
  well.
- **Writing the pack in hash order**, so offsets alone would give stored
  lengths, and records would need no permutation: about 3 bytes less.
  Bases would no longer come before their deltas. The writer would have to
  buffer or rewrite the whole pack (1.24 GB for the Linux slice) to reorder
  it, and readers would need a separate cycle check. Not worth 3 bytes.
- **Bit-packed fields** instead of whole bytes: about 1 byte less per object,
  with slower and more error-prone field access.
- **Fixed u32 fields with escape tables for large values** (as git's large
  offsets do). A per-pack width does the same job with one rule. One object
  over 16 MiB widens that pack's length field by one byte, which is at most
  2% of the index.
- **Truncated hashes in the index.** Listings, tombstones and repacks need
  full hashes. A truncated match would also need the pack read to confirm it.
- **A dictionary of distinct modification times.** A long-lived store has
  about one distinct second per write, so the dictionary saves nothing over
  an offset from the earliest time.
- **Dropping the logical length** (the zstd frame header has it). Listings
  report it for every object without reading the pack, and the backup copy
  compares sizes (ADR 0026).
- **Memory-mapping the index.** It would save the copy in memory, but the
  store holds an index for its lifetime and reads it on every lookup, and the
  workspace has no mmap dependency. Reading the file into memory keeps it
  simple, and at 46 bytes per object a 10-million-object index is 460 MB.

## Consequences
- The index is about half its former size on disk and in memory, and loads
  in linear time.
- Old indexes keep reading. A server from before this change cannot read
  `LEEPIDX2` indexes and reports them as corrupt, so downgrading after a
  repack needs that repack undone first (restore the blobs from a backup).
  Upgrade every server before running `repack` with the new binary.
- Backups copy packed objects as loose ones (ADR 0029, phase 3), so they
  are unaffected. A future pack-file backup copies indexes as bytes, and
  either layout works.
