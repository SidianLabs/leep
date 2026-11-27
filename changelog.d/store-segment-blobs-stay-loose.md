### Segment blobs stay loose (ADR 0108)
- **Changed.** The delta runs of the background repack, an object store's
  scheduled full repack and `leep-server repack` no longer pack the blobs
  of derived-data segments and GC mark segments (loose objects no history
  walk lists whose bytes start with their object prefix). GC then deletes
  their bytes at once instead of tombstoning them in a pack until a merge
  drops them. A file a changeset names is packed whatever its bytes.
- **Changed.** Loose segment blobs no longer count as objects waiting for
  a delta run, so they never start one (the task reads the first bytes of
  each new loose object once).
- **Added.** `left_loose` in `leep-server repack --json` output.
- **Tests.** Segment blobs stay loose through delta runs, full and
  incremental repacks of filesystem stores and an object store's full
  repack, start no run, and leave the store when deleted; a file shaped
  like one that history names is packed and reads back; bytes GC deleted
  from delta packs under the ratio rule are dropped once the age rule
  passes.
