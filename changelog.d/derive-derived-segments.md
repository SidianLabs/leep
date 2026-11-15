### Skiplists and history roots can leave the database for derived segments
- **Added.** Derived segments (ADR 0104): a background pass seals
  `skiplist` and `history-tree-v4` rows into immutable segments in the
  blob store (an index blob and a data blob, changeset ids stored as
  positions, zstd blocks) named by one `derived-segments-v1` row, which
  only a compare-and-swap rewrites. Values do not change: every read asks
  the database first (a row wins), then the segments; anything a segment
  cannot give reads as a missing row and derives again. Statements per
  operation are unchanged.
- **Added.** `leep-server --derived-segments off|build|retire` (default
  `build`) and `--derived-segments-interval` (`5m`). `build` seals and
  merges (16,384 changesets and 4 MiB a segment at most) and keeps every
  row, so rolling back is free. `retire` also deletes each row a live
  segment has held the same value for at least 10 minutes, after recording
  the store feature `derived-segments`: **one-way**, binaries older than
  this release refuse the store afterwards. Corpus B, after `VACUUM FULL`:
  19.09 to 15.87 MB on Postgres (2.90x to 2.41x Git), 19.15 to 15.86 MB on
  SQLite; its 2.82 MB of rows become one 0.27 MB segment.
- **Changed.** Blob GC never condemns a live derived segment, collects
  merged-away ones like orphans, reports them apart (`derived segments`,
  `retired derived segment blobs condemned` / `deleted`), and refuses to
  run while the manifest row does not decode.
- **Changed.** `verify` checks segment values like rows and reports
  `bad_derived_manifest` and `bad_derived_segment`; `repair drop` of
  `skiplist` or `history-tree-v4` also stops segments answering for the
  kind, and refuses `derived-segments-v1`; `repair drop` and `rederive`
  of those kinds wait for the derived-segments lease.
- **Added.** `MetaStore::delete_derived_values`, a value-conditional delete
  (one statement in Postgres and SQLite), used by `retire`.
- **Changed.** `leep-soak` runs its server with derived segments in
  `retire` and fails a run of a minute or more whose passes sealed or
  retired nothing (`--derived-segments` to change the mode).
- **Changed.** The default `--derive-cache-bytes` is 468 MiB (was 420): it
  now includes 48 MiB for segment blocks and data blobs.
- **Added.** Metrics `leep_derived_segment_*` and the alert
  `LeepDerivedSegmentPassesFailing`.
- **Tests.** Codec round trips, golden bytes, every-bit damage and a fuzz
  target (`derived_segment`); a sealed and retired store answering every
  query as the row-only store; a fault sweep interleaving lands, warming,
  passes, GC and the changeset sweep; SIGKILL during seal, merge and
  retire; a mixed fleet and rollback run and a restore drill.
