### GC marks are sorted segments in the blob store
- **Changed.** Blob GC records its incremental mark (ADR 0033) as a few
  immutable segments of sorted 32-byte hashes in the blob store, merged
  geometrically and named by one `gc-mark-segments-v1` row, instead of one
  `gc-mark-v1` row per referenced object: 32 bytes a mark instead of about
  167 on Postgres (ADR 0103). Lookups stay exact. Merges stop at 4 Mi marks
  (128 MiB) a segment. A lost or damaged segment, or one the store reports
  corrupt, only makes the next run walk more. `gc --full` names its
  segments only at its end, so a full run killed part way leaves the old
  record. Every write of the manifest row is a compare-and-swap, so a run
  that outlived its lease fails with `conflict` instead of overwriting a
  newer record. `gc` reports `mark segments` and the merged-away segments
  it condemns and deletes apart from blobs.
- **Added.** `leep-server gc --retire-mark-rows` copies the `gc-mark-v1`
  rows of an upgraded store into a segment, records the store feature
  `gc-mark-segments` and deletes the rows. One-way: older binaries refuse
  the store afterwards. Until it runs, the rows still count and rollback is
  free. It needs a metadata store with leases and no older `gc` running.
- **Added.** `MetaStore::cas_derived`, a compare-and-swap of one derived
  row, atomic in the Postgres, SQLite and memory stores.
- **Added.** `verify` checks the mark manifest and its segments
  (`bad_gc_row`, `bad_gc_segment`, and a note for a segment not stored).
