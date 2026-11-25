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
