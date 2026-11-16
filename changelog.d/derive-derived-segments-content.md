### Derived segments hold the code search rows
- **Added.** A third derived segment family, `content-v1` (ADR 0106),
  seals the code search rows (`blob-trigrams-v2`, `tree-trigrams-v2`) and
  their keyframes into segments in the blob store, verbatim, ordered by a
  walk of the heads' trees, in data blobs of at most 1 MiB. Values, kinds
  and keys do not change; reads ask the database first. With
  `--derived-segments retire --derived-segments-retire-families all`, `gc`
  and a `VACUUM FULL`, Leep's history plus 2,000 edits takes 12.04 MB on
  Postgres (1.90x Git's 6.35 MB, 1.83x the 6.58 MB of ADR 0104) and 12.48
  MB on SQLite (1.97x, 1.90x), down from 14.31 and 14.69 MB. `build` stays
  the default and keeps every row.
- **Changed.** Search row retention and segments (amends ADR 0102): a pass
  seals a blob or directory row only while retention keeps it (a head now
  or within `--search-retention`; every row with `off`), and every
  keyframe. Superseded rows stay rows for retention to delete; a sealed row
  that becomes superseded stays in its segment until a merge leaves it out
  (a segment is rewritten once a third of it is). A dropped key derives
  again on demand.
- **Changed.** A search row is compared with its segment value by the set
  or filter they stand for, each patch applied to its keyframe; a patch
  whose keyframe is missing is never retired against.
- **Changed.** Retiring `content-v1` records the store feature
  `derived-segments-3` first; binaries of the previous release then refuse
  the store. Like `changeset-v2`, the family passes this door only when
  named in `--derived-segments-retire-families` (or `all`) or once the
  feature is recorded. A `gc` of the previous release refuses to run while
  a `content-v1` manifest row exists.
- **Changed.** `verify` and `repair rederive` of the search kinds visit the
  keys only a segment holds too; `repair drop --kind blob-trigrams-v2` (or
  `tree-trigrams-v2`) clears both of its columns, keyframes with patches.
- **Changed.** A batched read's miss refreshes the segment manifests with
  one batched read even for one key; `budget_tests`: a search on a new
  process makes 32 metadata calls (31 before); search land (11) and create
  (13) are unchanged.
