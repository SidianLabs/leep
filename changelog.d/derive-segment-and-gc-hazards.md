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
