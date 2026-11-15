### GC mark segments: hardening after review
- **Fixed.** A GC run that trusted the `gc-mark-v1` rows and outlived its
  lease while `gc --retire-mark-rows` imported and deleted them now stops
  with a conflict instead of reading the deleted rows as "not referenced":
  condemn and collect check the mark manifest is unchanged before their
  blob loops and after every row read (ADR 0103).
- **Fixed.** Only definite corruption of a mark segment (a decode failure,
  or the store reporting it corrupt) or its absence ends the trusted
  prefix. Other read errors, including ones worded "missing", abort the
  run instead of retiring the rest of the record for good. `verify` is
  unchanged.
- **Changed.** `MetaStore::cas_derived` has no default: every backend and
  wrapper implements it, so a new backend cannot fall back to a
  read-then-write. The contract check also races 16 first writers from no
  row.
- **Changed.** The GC limit overrides for tests are built only with the
  `leep-derive` feature `test-hooks`, which only test crates enable.
- **Docs.** ADR 0103 and docs/PERFORMANCE.md give the cost of a mark lookup
  (about N/cap segments probed, about 250 at a billion marks).
- **Tests.** The closure oracle is shown to reject a record that is not
  closed.
