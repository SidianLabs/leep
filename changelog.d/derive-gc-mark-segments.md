### GC marks are sorted segments in the blob store
- **Changed.** Blob GC records its incremental mark (ADR 0033) as a few
  immutable segments of sorted 32-byte hashes in the blob store, merged
  geometrically and named by one `gc-mark-segments-v1` row, instead of one
  `gc-mark-v1` row per referenced object: 32 bytes a mark instead of about
  167 on Postgres (ADR 0103). Lookups stay exact. A lost or damaged segment
  only makes the next run walk more. `gc` reports `mark segments` and the
  merged-away segments it condemns and deletes apart from blobs.
- **Added.** `leep-server gc --retire-mark-rows` copies the `gc-mark-v1`
  rows of an upgraded store into a segment, records the store feature
  `gc-mark-segments` and deletes the rows. One-way: older binaries refuse
  the store afterwards. Until it runs, the rows still count and rollback is
  free.
- **Added.** `verify` checks the mark manifest and its segments
  (`bad_gc_row`, `bad_gc_segment`, and a note for a segment not stored).
