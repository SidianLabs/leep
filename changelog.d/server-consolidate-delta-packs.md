### The server consolidates its delta packs (ADR 0107)
- **Changed.** When no automatic delta run is due, the background repack
  merges its delta packs and compresses each frame a run had to store whole
  (its previous version was in an older pack) against that version. It
  runs once that saves an eighth of the packs' bytes, or a 64th once the
  packs holding it are a day old (by a clock restarts do not reset), measured
  on a sample first; objects GC deleted from those packs count and are
  dropped. One consolidation rewrites at most 2 GiB of packs, walks again
  only after enough new whole frames, and backs off after a failure. On the land-driven corpus (Postgres) all storage falls
  from 26.92 to 15.61 MB, 4.24 to 2.46 times Git, the history objects to
  the size `repack --full` gives, and wide edits cost under a quarter of
  the blob bytes per land they did.
- **Changed.** Upgraded stores need no `repack --full`: the first check
  after a start walks the largest delta pack's history once and compresses
  what earlier merges left whole next to its base. Delta packs over
  `--auto-repack-consolidate-max-bytes` (2 GiB) are logged once and left to
  `repack --full`.
- **Fixed.** A delta run could fold a land pack written while it walked
  history into its pack, copying that pack's frames whole and marking them
  compressed, so they stayed level-1 whole frames until a `repack --full`.
  A repack now folds in only the packs its plan listed.
- **Fixed.** The cleanup of a repack deletes only the packs it replaced
  that are still retired. A later merge can publish one of them again
  under the same id (a consolidation of a pack whose added objects GC
  deleted inside the fence reproduces the pack before them), and the
  cleanup used to delete it by name.
- **Fixed.** A backup whose pack copy outlasts a repack's fence lists the
  source's packs again and copies the ones that replaced the packs that
  vanished, instead of copying their objects loose into the pool.
- **Fixed.** A backup pool kept a whole copy of every source pack a repack
  replaced while dropping an object GC had deleted, so pools grew by the
  store's pack bytes at each such repack. The objects only the old pool
  pack holds are now copied into a small pool pack of their own and the
  old pack is deleted; nothing a snapshot may need is dropped.
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
  deleted a base and a head, and a pack published again under its id
  surviving the earlier cleanup (store and background task); server tests for the ratio and age rules, new
  files, the walk's bound, restarts, capped chains, deleted objects, the
  pending cap, the size limit and budget, a single pack left by older
  merges, the age rule across restarts, the back-off after a failure and
  the walks after delta runs; a store test that packs written after a
  repack's plan are left alone; backup pool tests that the pool stays
  bounded, a rescued delta decodes through its chain and an interrupted
  rescue finishes; a SIGKILL crash test whose kills wait for a
  consolidation to start and must land inside at least one; a wide-edit
  phase in the storage size guard.
