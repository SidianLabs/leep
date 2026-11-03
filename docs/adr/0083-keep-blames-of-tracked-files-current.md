# 0083. Keep the blames of tracked files current as changes land
Status: Accepted
Date: 2026-11-03

Supersedes the unmerged experiment ADR 0075 ("Extend cached blames in the
background").

## Context
A cold blame of a hot file is the one operation that does not scale with
depth (PERFORMANCE.md, "One million changesets of history"): about 10,000
changers over a million generations took 61 s, 43 s for a second hot file,
so most of it is per-file work. ADR 0078 made such a blame finish past the
request timeout and resume from checkpoints, but an agent still waits a
minute, and on hot history every land changes the hot files: the next
request walks and diffs every changer since the last cached blame again.

ADR 0075 tried to extend cached blames in the history warm loop for every
file each landed changeset wrote, with a walk of up to 8 changers per file.
In a 1,000-agent swarm 3,158 of those walks found no cached blame near, 28
rows were written, and the step took 124 s of a 300 s run out of history
warming, so the index fell behind. It was not merged. Its lesson: start
only from files people blame, and never walk in the background.

Profile first (PERFORMANCE.md, "Blames kept current as changes land"):
on a 200,000-changeset linear history with Postgres's buffers warm, a cold
blame of a hot file (about 8,200 versions) took 4.6-5.3 s, three quarters
of it the walk (about 0.45 ms a version of dependent index, skiplist,
`last_merge-v1`, generation and tree reads) and 0.7 s the replay, of which
the line diffs were 0.31 s and the blob reads (delta chains through the
pack reader's shared decoded cache) at most 0.34 s beyond them. Sharing
decoded delta bases within a replay window would save under a tenth, so
the replay was left alone. What costs is walking the versions, so the fix
is not to walk them again for every new head.

## Decision
- **Tracked files.** `Repo` keeps the paths of answered blames
  (`BlameTracker`): every blame a server answers (`Repo::blame_held`, so
  cached, joined and bounded blames too) tracks its path. At most
  `--warm-blame-paths` (default 1,024) are tracked; the least recently
  blamed goes first, and one nobody blamed for 30 minutes is forgotten.
  Per path, the tracker keeps the refs where its chain of cached blames
  broke (below); a new blame of the path clears them. In memory only: no
  new persisted format. After a restart the next blame of a file tracks it
  again, and its own cached rows (ADR 0078 checkpoints, its answer) are
  where the chain restarts.
- **The blame warmer.** A loop of its own (`BlameWarmer`), every
  `--warm-interval`, tails the ref log (the index warmers' `RefTail`, now
  shared) and keeps each ref's position: the changeset its blames were
  brought up to. For each ref behind its head it reads the first-parent
  chain from the head down to that position (one changeset read per
  changeset, which it needs for the files it wrote) and, oldest first, for
  each changeset with one parent and each tracked file it wrote
  (`Repo::blame_extend`):
  - the file at the parent (`lookup_file`; absent means a creation or
    rename, left to requests);
  - the newest changer of the file at the parent from the path-history
    index, and its cached blame (`Repo::newest_changer_blame`): the
    changer must be the index's only newest one and lie at or above the
    parent's `last_merge-v1`, which is when a request's walk goes straight
    to it and ends at its row (`Repo::skip_unchanged`);
  - on a hit, the two versions (one `get_many`), one line diff
    (`diff_pairs`, as the replay) and one row write under the changeset's
    `blame-v2` key;
  - on a miss (no row there: the chain broke at a merge, a rename, a gap
    or a restart), the path is marked cold on that ref and costs no read
    there until it is blamed again.
  No walks, so the cost per land is O(1) per tracked file the land wrote
  and nothing for other files. With nothing tracked a pass only moves the
  positions.
- **Identical rows.** The extended row is what a request for that blame
  would cache: a version whose path its first parent has is replayed from
  that version alone, and the replay and the warmer now apply a diff with
  one function (`apply_base`). The text and line ends come from the blob,
  never from the row's stripped lines. `warm_extended_blames_match_cold_blames`
  compares every row against the reference walk byte for byte.
- **Never ahead of the index, never in its way.** The warmer extends a
  changeset only once the path-history index is derived at its parent
  (one batched read of those rows per pass) and never derives; the rest
  waits for a later pass. It runs on the shared connections, not the
  index lane (ADR 0073): its rows are caches, as a request's are, and the
  index warmers keep their reserved pool. A pass works for at most 200 ms
  and goes on next pass. A ref more than 1,024 changesets ahead, or moved
  off its chain, starts again at its head (its files go cold there).
  Merges, files over 1 MiB and versions that are not UTF-8 are skipped.
  Changesets shared by several refs are extended once (the last 4,096 are
  remembered).
- **Admission (ADR 0074).** Before a pass with work the warmer takes a
  `blame` read-class permit if one is free (`ReadLimits::blame_gate`) and
  keeps it for the pass; with none free it skips the pass and catches up
  later (`leep_warm_blame_shed_total`). Its work so counts against the
  class, and it backs off while requests fill it. With the class off it
  runs ungated.
- **Every mode.** The warmer runs in `local`, `lease` and `off` indexing
  modes (unless `--warm-blame-paths 0` or `--warm-interval 0`): the tracked
  files are this server's own requests, the rows are caches any server may
  write, and it only reads the index. In a split deployment the
  query-serving server keeps its blames current from the index the lease
  holder builds.
- **The request finds the row at once.** The head walk of a blame looks up
  the cached blame of its first changer below the tip as soon as the index
  names it, before the walk goes on, and ends there on a hit (a cold blame
  pays one more read; the operation budget is 10 + 3 per version, from
  9). A blame of a tracked file at a new head is then the tip's file, the
  index entry, a few skiplist reads, the changer's row and its parent's
  file, and its answer is kept at the head as before. The stretch cuts are
  planned only once the head walk did not end the blame.
- **Flag and metrics.** `--warm-blame-paths` (`LEEP_WARM_BLAME_PATHS`,
  default 1,024, 0 off). `leep_warm_blame_total{result=written|missed|skipped}`,
  `leep_warm_blame_shed_total`, `leep_warm_duration_seconds{group="blame"}`
  (passes that looked at a changeset) and the gauge
  `leep_blame_tracked_paths`.

## Measurements
PERFORMANCE.md, "Blames kept current as changes land". On the 200,000
store, a blame of `hot/h0` at the head after 2,000 and 8,000 more
changesets (about 4% of which change it): 370 and 437 ms with
`--warm-blame-paths 0`, 15.6 and 19.5 ms with the warmer (88 and 295 rows
written, 0.7-0.9 ms of warmer time a landed changeset). Cold blames are
unchanged (4.5-4.6 s before and after). In a 300-agent swarm with half the
blames on hot files, two runs each: lands/s 45.0-45.4 off, 47.3 on;
history lag p50 / max 6-7 / 15-18 off, 4-5 / 13-18 on; blame p99 169-234 ms
off, 38-46 ms on; blames shed 864-927 off, 318-336 on; the warmer spent
7.2 s in 390 passes and skipped 20-22 for want of a permit.

## Consequences
- A file that someone blamed answers its next blame from a row the warmer
  wrote, however many lands changed it since, as long as nothing broke its
  chain. The first blame of a file is as cold as before.
- `derived` grows by one `blame-v2` row per land per tracked file it
  wrote, about the size of the file plus 36 bytes a line, for as long as
  someone blames the file every 30 minutes. 1,024 tracked files bound it.
  GC treats the rows as any cached blame; `repair drop --kind blame-v2`
  removes them, and the chain restarts at the next blame.
- A merge or rename that changes a tracked file breaks its chain on that
  ref until a request blames it past the merge.
- Several servers each track their own requests; two servers tracking the
  same file write the same rows (identical bytes, so harmless).
- The warmer's permit is one of the `blame` class's few: on a two-core host
  one of two, for up to 200 ms per pass.

## Alternatives
- *ADR 0075: extend every written file with a short walk.* The walks were
  the cost, for files nobody blamed. Rejected by measurement.
- *Catch a broken chain up in the background with a bounded walk.* Bounded
  walks are request work; the next blame does it once, with its permit.
- *Persist the tracked set.* A small derived row would survive restarts,
  but the next blame of a file re-tracks it and its rows are still there;
  not worth a new persisted kind.
- *Keep the newest row per tracked file in memory to skip its read.* One
  read per extension is not where the time goes.
