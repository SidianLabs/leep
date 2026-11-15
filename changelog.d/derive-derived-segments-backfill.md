### Derived segment backfills are linear and passes stay short
- **Fixed.** A derived segment backfill (ADR 0104) no longer lists every
  row, finds every key's segment and reads every generation again in each
  pass: a listing's candidates are carried to the following passes, which
  seal 65,536 changesets each without listing, generations are read once
  (at most a minute a pass), and holders are found in one walk of the
  segments' keys. 500,000 changesets on Postgres: 10.1 s of passes instead
  of 37.4 s, every pass after the first under 0.7 s. Retirements carry
  their candidates the same way and wait for a seal backfill to finish.
- **Fixed.** A pass says it has more (and is followed 5 s later) only if it
  made progress; rows whose values do not decode are skipped like those of
  unstored changesets, so they no longer kept passes 5 s apart for ever.
- **Fixed.** A pass probes the index blob of a segment whose decoded index
  it took from the read view, so a lost index blob is noticed; a
  retirement step stops at the first segment value it cannot read instead
  of reading the bad blob again for every row.
- **Changed.** Postgres `delete_derived_keys` locks rows in `(kind, key)`
  order, like `delete_derived_values` (whose client-side sort is gone: the
  `ORDER BY` under `FOR UPDATE` sets the order; both plans are index scans
  of the primary key).
- **Tests.** Carried backfills, generations over several passes, carried
  work dropped when another server seals, a lost reused index blob,
  undecodable rows ahead of good ones, and a retirement step over an
  unreadable value; `derived_segments_backfill` measures a backfill on
  Postgres (ignored).
