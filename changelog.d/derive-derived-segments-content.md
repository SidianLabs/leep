### Derived segments hold the code search rows
- **Added.** A third derived segment family, `content-v1` (ADR 0106),
  seals the code search rows (`blob-trigrams-v2`, `tree-trigrams-v2`) and
  their keyframes into segments in the blob store, verbatim, ordered by a
  walk of the heads' trees, in data blobs of at most 1 MiB. Values, kinds
  and keys do not change; reads ask the database first. With
  `--derived-segments retire --derived-segments-retire-families all`, `gc`
  and a `VACUUM FULL`, Leep's history plus 2,000 edits (imported, so only
  its head is indexed) takes 12.04 MB on Postgres (1.90x Git's 6.35 MB,
  1.83x the 6.58 MB of ADR 0104) and 12.48 MB on SQLite (1.97x, 1.90x),
  down from 14.31 and 14.69 MB; the same history landed head by head with
  the warmer on is measured in ADR 0106. `build` stays the default: it
  keeps every row and grows the blob store by the segments (2.6 MB on that
  history) until `retire`.
- **Changed.** Search row retention and segments (amends ADR 0102): a pass
  seals a blob or directory row only while retention keeps it (a head now
  or within the lease holder's `--search-retention`, so a fleet must run
  one window; every row with `off`), and every keyframe. Superseded rows
  are not sealed: retention deletes superseded blob rows as before, and
  superseded directory filters stay rows as they always did. A sealed row
  that becomes superseded stays in its segment until a merge leaves it out
  (a segment is rewritten once a third of it is dead), which also drops a
  directory filter sealed while live; a dropped key derives again on
  demand.
- **Changed.** A search row is compared with its segment value by the set
  or filter they stand for, each patch applied to its keyframe; a patch
  whose keyframe is missing is never retired against unless the bytes are
  identical, and a row that resolves while its sealed value does not is
  sealed again into a new segment.
- **Changed.** Retiring `content-v1` records the store feature
  `derived-segments-3` first; binaries of the previous release then refuse
  the store. Like `changeset-v2`, the family passes this door only when
  named in `--derived-segments-retire-families` (or `all`) or once the
  feature is recorded. A `gc` of the previous release refuses to run while
  a `content-v1` manifest row exists, also in the default `build`.
- **Changed.** `verify` and `repair rederive` of the search kinds visit the
  keys only a segment holds too; `repair drop --kind blob-trigrams-v2` (or
  `tree-trigrams-v2`) clears both of its columns, keyframes with patches.
- **Changed.** A read through the derived segment view fails only for the
  family it asks for; another family's failed manifest or index load is
  logged. In mode `retire` a pass lists rows again only when their count
  moved, a segment ripened or `list_every` passed. A batched read's miss
  refreshes the segment manifests with one batched read even for one key;
  `budget_tests`: a search on a new process makes 32 metadata calls (31
  before); search land (11) and create (13) are unchanged.
- **Changed.** `leep-soak` names the command a stalled CLI agent is in.
