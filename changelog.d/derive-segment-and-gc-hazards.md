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
