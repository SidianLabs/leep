### Derived segments and blob GC survive more faults
- **Fixed.** One derived segment whose index blob failed to read (an I/O
  error, a failing object store key) failed every missing read of its
  family on every server, lands included, and every compactor pass. Now
  just that segment is left out (its keys read as rows or derive again)
  and tried again each minute; passes skip it and drop it once it failed
  three passes in a row over at least an hour while the family's other
  segments read.
- **Fixed.** Derived segment blobs are read with an exact miss
  (`get_exact` after a `get` miss): a segment another process packed a
  moment ago is no longer dropped by a pass (mode `build`) or hidden from
  a reader for a minute.
- **Fixed.** `content-v1` merges no longer leave out directory filters
  (amends ADR 0106). Dropping a dead filter could leave a stored ancestor
  over a subdirectory with no filter, so a ref created at an old revision
  was taken for indexed and, under `--indexing lease` or `off`, its
  searches answered `indexing` on every retry. Merges also read the ref
  log again before storing a segment that leaves blob rows out, and keep
  everything if a ref moved since the walk.
- **Fixed.** A derived segment that an older `gc` condemned and a collect
  then rescued had its rows kept for good: the compactor took the
  condemnation row alone for a standing condemnation. It now reads the
  rescue and doom rows too, as the rest of blob GC does.
- **Changed.** A `derived-segments-v1` row that does not decode no longer
  stops all blob GC until an operator drops every kind of the family: `gc`
  keeps every blob that starts like a derived segment, collects the rest,
  and reports the family. A row written by a newer binary still stops it.
- **Fixed.** `leep-server backup` copies the live derived and GC mark
  segments its metadata snapshot names right after the snapshot, before
  the listing copy: a merge could retire one and `gc` collect it while a
  long copy was on its way, leaving a restore without the only copy of
  retired rows. The backup's check (and `backup verify`) counts a live
  derived segment missing from the copy as a problem once its family's
  store feature is recorded. The manifest's blob section gains
  `segments_named`, `segments_copied` and `segments_vanished`.
- **Fixed.** Derived segment passes no longer hold every row key of a
  family in memory at each listing (about 150 MB for 1.2 million
  changesets): a listing keeps only the keys a seal or a retirement needs.
  `content-v1` no longer remembers every dead row's key between listings,
  and seals and retirements read as many keys at once as keep a read near
  4 MiB, not 1,000 keys of up to 64 KiB each. Pass reports gain
  `listed_keys`.
- **Fixed.** Blob GC's fence rows (`blob-condemned-v1`, `blob-rescue-v1`,
  `blob-doom-v1`) were never deleted, so every blob GC ever deleted left
  rows behind for good. A collect now prunes those of blobs it deleted
  more than 30 days ago (`fence rows of long-deleted blobs pruned` in its
  report).
- **Fixed.** A repack cleanup killed with a loose copy moved aside left a
  rescued object (deleted from its pack by GC, uploaded again) unreadable
  until an operator ran `leep-server repack`. A server's first sweep now
  puts such copies back, and a cleanup that fails puts its copy back
  itself.
- **Fixed.** A pack merge kept the write time of the first copy of an
  object several packs held, not the latest, so an upload's refresh could
  look old to GC's grace and rescue checks.
- **Fixed.** Smaller derived segment fixes: two servers taking turns on
  the compactor lease no longer drop each other's carried backfill (a
  segment the other sealed only takes its keys out of it); tiers count
  from the smallest (idle) seal, so idle seals no longer make tier 0
  rewrite the same rows about 20 times; `repair drop` checks its lease
  before each delete step; and `verify` and `repair rederive` no longer
  stop a kind's scan early when a page of sealed keys all read as
  missing.
