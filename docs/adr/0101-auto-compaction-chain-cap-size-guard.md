# 0101. Automatic changeset compaction, a longer delta chain, and a size guard
Status: Accepted
Date: 2026-11-14

Amends ADR 0053 (who runs the compaction) and ADR 0029 (the writer's chain
cap). Keeps ADR 0055 (format features) as it is.

## Context
ADR 0100 made a server with default flags keep its blob store near the size
`repack --full` gives: Leep's own history (corpus A, Git 4.84 MB after `git
gc`) settles at 13.5 MB in all, 2.8 times Git, and A plus 2,000 one-file
edits (corpus B, Git 6.58 MB) at 22.5 MB, 3.4 times. The maintainer's target
is at most 2 times Git with indexes on. Three things stood in the way of the
next steps:

- **Nothing guards the size.** A change that stops the automatic repack, or
  writes more per land, shows only in a manual measurement. The ratio bench
  (ADR 0081) counts statements and bytes written per land, not what stays on
  disk.
- **Changeset rows are compressed only by hand.** ADR 0053's dictionary
  halves the `changesets` table (B: 3.41 MB of rows to 1.04 MB), but only
  `leep-server compact-changesets` trains and applies it, and it refuses
  stores under 1,000 changesets (`MIN_TRAINING_ROWS`). No default deployment
  gets the saving.
- **The writer's delta chain cap is 32.** A file edited hundreds of times
  stores every 33rd version whole: on corpus C (the same few files edited
  400 times) contents grew 483 KB where Git grew 70 KB and an uncapped path
  delta 45 KB.

## Decision

### 1. The server compacts changeset rows itself
A task in `leep-server` (`auto_compact`, on by default, `--auto-compact`)
runs `leep_meta::auto_compact::AutoCompactor::step` every
`--auto-compact-interval` (10 minutes; the first 2 minutes plus up to 60 s
of jitter after start, while a new server warms its indexes, and so that
servers started together do not all step at once). It runs
on a thread of its own at the lowest CPU priority, with a single-threaded
runtime and one database connection of its own (not from the server's
pools), so lands and reads never wait for it. On SQLite that is a pool of
one connection. On Postgres each step first takes a session advisory lock
(`pg_try_advisory_lock(0x1EE90101, hashtext(current_schema()))`, held on a
connection of the compactor's lease pool) and skips the step if another
server holds it, so replicas do not repeat each other's training and
rewrites. A step:

1. **Trains when the store has outgrown its dictionary.** Dictionaries come
   in tiers by changeset count (counted up to 10,000, so the count is cheap
   on any table):

   | Changesets | Dictionary |
   |---|---|
   | 100 (`--auto-compact-min-rows`) | 16 KiB |
   | 1,000 | 60 KiB |
   | 10,000 | 64 KiB, what `compact-changesets` trains |

   trained on the first changesets in id order (a uniform sample, as
   `compact-changesets`), at most 50,000 of them and at most 16 MiB of
   canonical bytes, so training costs a few seconds of one core whatever
   the rows' size. Rows that do not decode are left out of the sample. A dictionary's tier is read back
   from its size: at most 16 KiB is the first, at most 60 KiB the second,
   anything larger the last. So the schedule keeps no state of its own and
   holds across restarts and replicas. The compactor cannot tell its own
   dictionaries from an operator's without new state, so the rule is by
   size: a dictionary over 60 KiB (`compact-changesets` trains 64 KiB by
   default, on any store large enough to fill it) is never replaced; a
   smaller one, from `--dict-size` or a small store, counts as the tier its
   size falls in and is replaced at the next tier. Measured on B's rows, a larger
   dictionary always compresses better, but below about 1,000 rows the
   dictionary's own row costs more than the extra it saves (100 rows: 16
   KiB gives 166 KB plus the dictionary, 64 KiB 130 KB plus 64 KiB).
2. **Stores a candidate only if it pays.** The samples compressed with the
   candidate, plus the candidate's own bytes, must be at least 5% smaller
   than plain for a first dictionary, and at least 12% smaller than with
   the dictionary in force for a replacement: a tier change rewrites every
   row (and on Postgres leaves their old versions as free space), which a
   few percent does not repay. A candidate that gains less is dropped and
   nothing is recorded. Its tier is tried again once the store has doubled
   or after 24 hours (the count stops at 10,000, so a large store would
   never double by it). That memory is the process's: a restart tries once
   more, at the bounded cost of one training. A store of a few small
   changesets therefore never gets a dictionary, nor the format feature.
3. **Records the feature, then the dictionary,** exactly as
   `compact-changesets` does (ADR 0055): `changeset-zstd-dict` is inserted
   first, and only when a dictionary is about to be stored, so a store is
   marked only once a dictionary is used. The new dictionary's id is the
   newest id the step started with, plus one, so if another server (or
   `compact-changesets`) stored one meanwhile the primary key refuses this
   one; that is not an error, and the next step sees the other.
4. **Passes over the rows** not on the newest dictionary, compressing plain
   rows where that is smaller and moving rows on an older dictionary to the
   newest, in batches of 250 (`--auto-compact-batch`), each one statement
   (on SQLite too: one `UPDATE ... FROM (VALUES ...)`, so the write lock is
   never held across awaits), ids sorted and rows already equal skipped
   (`IS DISTINCT FROM`), so it cannot deadlock with a concurrent
   `compact-changesets`,
   resting after each batch three times as long as it took (at least 10
   ms), so it keeps the database busy at most a quarter of the time. A
   pass reads only the first 32 bytes
   of each row (a frame header names its dictionary) and fetches the rows
   it rewrites. It runs once after each new dictionary, and again until one
   pass has started after the dictionary was a minute and 15 s old: servers
   look for a new dictionary at most a minute before a write
   (`codec::REFRESH`) and write plain rows until then. After that, steps
   only count rows. Each process passes once at its first step, so a pass a
   crash interrupted is finished by the next process. A row that does not
   decode is left as it is, logged and counted
   (`leep_changeset_compact_corrupt_total`), and the pass goes on: it is a
   matter for `verify` and `repair`, not a reason to stop compacting.

Crash safety is that of `compact-changesets`: the feature before the first
dictionary, the dictionary before any row that names it, and each batch a
single `UPDATE` whose rows decode to the same changesets before and after
(a reader sees old or new bytes; both are checked against the id). A step
killed anywhere leaves a store every reader of this version reads, and the
next step redoes what is left. A feature with no dictionary (killed between
the two) is harmless, as ADR 0055 says.

`compact-changesets` stays, unchanged, for an operator who wants a
dictionary now or a `--retrain`. Its `MIN_TRAINING_ROWS` still applies to
it; the automatic task has its own floor.

Metrics: `leep_changeset_compact_steps_total{outcome = ok | busy | error}`,
`leep_changeset_compact_corrupt_total`,
`leep_changeset_compact_trained_total`,
`leep_changeset_compact_rejected_total`,
`leep_changeset_compact_rows_total` (rows rewritten),
`leep_changeset_compact_saved_bytes_total`, `leep_changeset_compact_seconds`
(steps that passed), and the gauge `leep_changeset_dictionary` (the newest
dictionary's id).

### 2. The writer's chain cap is 63; readers follow 256 frames
`leep_store::pack::MAX_CHAIN` (the writer's cap, for `leep-server repack`,
the automatic delta repack and the background merges) goes from 32 to 63,
and `READ_CHAIN_LIMIT` (the most frames a reader decodes for one object
before calling the pack corrupt) from 64 to 256.

63, not 128: every reader since ADR 0029 decodes at most 64 frames (the
object and 63 bases), so a chain of 63 is the longest that binaries already
deployed, and a downgrade, still read. A chain of 128 would make packs
written by this release unreadable to the release before it, a format
change for a few hundred bytes a land. Raising the reader's limit now lets
a later release raise the writer's cap to 128 or more once no binary older
than this one reads the store, with no format change then either.

Cold reads cost more frames, not more requests: a filesystem read still
plans the chain from the index and asks the kernel for every frame at once,
and an object store reads the chain's frames in one ranged request (ADR
0040), so the store calls the operation budgets count (ADR 0081) do not
change. Measured with `cargo run --release -p leep-store --example
chain_depth_latency` (PERFORMANCE.md, "Delta chain cap"), 260 versions of
a 6 KiB and a 53 KiB file, cold (pages dropped, fresh store): the pack
shrinks 30% at 63 and 50% at 128; the deepest version of the 53 KiB file
takes 0.62 ms at 32, 1.0-2.4 ms at 63 and 1.8-4.7 ms at 128 on a
filesystem, and 43.5, 43.8 and 45.6 ms on a simulated S3 (20 ms a
request, 10 ns a byte), where the request dominates. Neither 63 nor 128
breaks a budget; compatibility decides.

### 3. A size guard in CI
`cargo test -p leep-e2e --test storage_size` generates a small
deterministic history in the test (24 source-like files, 50 commits of one
to three files with real-looking messages), imports it with `leep-git`,
lands 60 more edits through the HTTP API on a server with default storage
flags (only the background work's intervals and the fence are shortened, to
seconds), waits until the blob store and the database stop changing (no
loose object, no retired pack, every changeset row compressed), and checks
blob store bytes per land, database bytes per land, all storage over Git's
`objects/` after `git gc` for the same history, and
`leep_blob_store_size_ratio`, each against a budget of about 1.5 times the
value measured when it was set (PERFORMANCE.md, "Storage size guard"). It
runs on Postgres in CI and on SQLite without `LEEP_TEST_POSTGRES_URL`, in
about 20 s. A change that stops the automatic repack or compaction fails it
too: the store never settles.

`leep-soak` gains `--max-blob-store-ratio` (default 0.9): the run
fails if `leep_blob_store_size_ratio` exceeds it in the second half, on
samples taken after the server's first automatic delta repack (not judged
if none ran). The soak starts its server with `--auto-repack-max-age 2m`
(`--auto-repack-max-age` of the soak) so a run of a few minutes repacks.

## Consequences

- **Sizes** (PERFORMANCE.md, "Automatic changeset compaction, chain cap
  63"; Postgres, defaults, settled, against 827d550 measured the same day):

  | | A (Git 4.84 MB) | B (Git 6.58 MB) |
  |---|---|---|
  | Base | 8.06 + 5.38 = 13.44 MB | 15.86 + 6.66 = 22.52 MB |
  | Now, settled | 9.05 + 5.03 = 14.08 MB | 16.66 + 6.08 = 22.74 MB |
  | Now, after `VACUUM FULL changesets` | 7.58 + 5.03 = 12.61 MB (2.6x) | 14.16 + 6.08 = 20.24 MB (3.1x) |

  Changeset rows fall from 2.09 to 0.77 MB (A) and from 3.49 to 1.13 MB
  (B); packs from 5.08 to 4.73 MB and from 5.94 to 5.36 MB. Per land over
  400 lands (corpus C) the blob store grows 1,515 bytes instead of 2,198
  and the changeset row is 211 bytes instead of 300; the database's growth
  (25.6 KB a land) is the indexes.
- **On Postgres, rewriting rows a store wrote before it had a dictionary
  leaves their old versions as free space** until later rows reuse it, so
  right after the first pass the database is larger, not smaller (the
  "settled" row above: an import's rows all predate the dictionary). New
  changesets fill that space first (C: the table grew 61 bytes a land
  instead of 451); `VACUUM FULL changesets` returns it at once, but holds
  an exclusive lock on the table, so the server does not run it. Rows
  written after the first dictionary are compressed when written and
  never rewritten. An importer that trained before writing would avoid the
  rewrite; that is a follow-up.
- **Mixed versions.** A server of this release stores a dictionary on its
  own within a few minutes of starting on a store of 100 or more
  changesets that a dictionary pays for. Servers since ADR 0053 read such
  rows, and those since ADR 0055 check the feature; binaries older than
  ADR 0053 (October 2026) cannot read them and do not check. So no binary
  older than ADR 0053 may use a database that a server of this release
  uses; start the new servers with `--auto-compact off` until such a
  rollout is finished. Going back to a binary between ADR 0053 and this one
  works: it reads every row, and its own writes use the newest dictionary.
- Packs written by this release have chains of up to 63, which every
  release since ADR 0029 reads; going back is safe.
- On Postgres, each rewritten row leaves its old version as free space
  until vacuum, as `compact-changesets` does: the first pass on a large,
  never-compacted store rewrites every row (about 4 minutes per million
  Linux-sized changesets, plus the pauses), and the table holds about one
  and a half times its compacted size until autovacuum makes the space
  reusable; `VACUUM FULL changesets` gives it back. Stores already
  compacted by hand keep their dictionary and are not rewritten.
- Several servers on one Postgres store take turns: a step runs only
  under the advisory lock, and is skipped (`busy`) while another server
  holds it, so a pass is not done four times by four servers. Without the
  lock (or on SQLite, whose single writer serialises anyway) the
  dictionary id's primary key still lets only one candidate in, and the
  rewrites are idempotent.
- The feature is recorded once, before the first dictionary; a store that
  never gets one is never marked.
- Follow-ups: the size guard's history is small, so its budgets are loose
  against per-land changes of a few hundred bytes; the measured A, B and C
  runs (PERFORMANCE.md) remain the precise check. A chain cap of 128 needs
  one release of readers with the new limit first.
