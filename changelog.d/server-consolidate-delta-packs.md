### The server consolidates its delta packs (ADR 0107)
- **Changed.** When no automatic delta run is due, the background repack
  merges its delta packs and compresses each frame a run had to store whole
  (its previous version was in an older pack) against that version. It
  runs once that saves an eighth of the packs' bytes, or a 64th for a day,
  measured on a sample first; objects GC deleted from those packs count
  and are dropped. On the land-driven corpus (Postgres) all storage falls
  from 26.92 to 15.61 MB, 4.24 to 2.46 times Git, the history objects to
  the size `repack --full` gives, and wide edits cost under a quarter of
  the blob bytes per land they did.
- **Changed.** Upgraded stores need no `repack --full`: the first check
  after a start walks the largest delta pack's history once and compresses
  what earlier merges left whole next to its base. Delta packs over
  `--auto-repack-consolidate-max-bytes` (2 GiB) are logged once and left to
  `repack --full`.
- **Changed.** A delta run compresses the whole frames of the land and
  import packs it takes at its own level instead of copying their level-1
  frames.
- **Added.** `--auto-repack-consolidate`, `--auto-repack-consolidate-ratio`,
  `--auto-repack-consolidate-age`, `--auto-repack-consolidate-max-bytes`
  (and `LEEP_AUTO_REPACK_CONSOLIDATE*`); metrics
  `leep_repack_consolidate_runs_total{outcome}`,
  `leep_repack_consolidate_gain_bytes_total`,
  `leep_repack_consolidate_seconds` and
  `leep_repack_whole_bytes_outside_largest`.
- **Tests.** Store tests for re-delta across packs, joined chains within
  the cap of 63, and a consolidation killed at every repack step after GC
  deleted a base and a head; server tests for the ratio and age rules, new
  files, the walk's bound, restarts, capped chains, deleted objects, the
  pending cap and the size limit; a SIGKILL crash test with eager
  consolidation; a wide-edit phase in the storage size guard.
