### Derived segments hold generations, last merges, root trees and log filters
- **Added.** A second derived segment family, `changeset-v2` (ADR 0105),
  seals the `generation`, `last_merge-v1`, `root_tree` and `log-filter-v1`
  rows into segments in the blob store, beside `skiplist` and
  `history-tree-v4` (`changeset-v1`, ADR 0104). Values, kinds and keys do
  not change; reads ask the database first. With `--derived-segments
  retire` and a `VACUUM FULL`, Leep's history plus 2,000 edits takes about
  1.5 MB less on Postgres: about 1.6 MB of rows (with row overhead) become
  a segment of 0.16 MB. `build` stays the default and keeps every row.
- **Changed.** `--derived-segments retire` records the store feature
  `derived-segments-2` before it deletes the first `changeset-v2` row:
  servers of the previous release (ADR 0104) then refuse the store, as
  binaries before ADR 0104 do once `derived-segments` is recorded. Set
  `retire` only after every binary runs this release; `build` records
  nothing and stays free to roll back.
- **Changed.** Blob GC reads every family's manifest and stops at one that
  does not decode, was written by a newer binary, or names a family it does
  not know. A family added after ADR 0104 ignores a segment column it does
  not know instead of dropping the segment. One pass runs every family,
  each with its own carried backfill and retirement; a failing family no
  longer stops the other.
- **Changed.** The derived segment metrics are labelled by `family`
  (`leep_derived_segments_live`, `leep_derived_segment_bytes`,
  `leep_derived_segment_unsealed_rows`, `leep_derived_segment_index_bytes`,
  `..._reads_total`, `..._errors_total`, `..._mismatch_total`,
  `..._rows_retired_total`). Queries that sum them are unaffected.
- **Changed.** `budget_tests`: a create on a sealed and retired store makes
  the same 13 metadata calls (both families' manifests ride on the first
  batched read) and two more blob reads, the second family's index and
  data blob, once per process.
- **Added.** `crates/leep-server/examples/derived_breakdown.rs` breaks a
  store's derived bytes down by kind and measures candidate segment
  encodings.
