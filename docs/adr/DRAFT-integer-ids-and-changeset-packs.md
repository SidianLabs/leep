# DRAFT. Server-local changeset numbers and changeset segments
Status: Proposed (draft, not numbered)
Date: 2026-10-18

## Context

The whole Linux history (1,484,762 changesets) on Postgres 16 takes 11.5 GB
against git's 3.8 GB: 3.2 GB of blobs (one 2.74 GB pack plus a 484 MB index)
and an 8.3 GB database. The figures per derived kind below are value bytes
(`sum(length(value))`). They check out exactly: `root_tree` 45 MB is
1.48M × 32 B, `generation` 11 MB is 1.48M × 8 B, `git-to-leep-v1` 45 MB is
1.48M × 32 B and `leep-to-git-v1` 28 MB is 1.48M × 20 B.

| Table / kind | Size | What a row holds today |
|---|---|---|
| `changesets` | 2.9 GB, about 1.96 KB per row with overhead | postcard `Changeset`, about 1.8 KB (1.7 KB measured on the first 21k commits) |
| `skiplist` | 907 MB of values | 40 B per entry (32 B id + 8 B generation). 611 B per row means about 15 entries |
| `root_tree`, `generation` | 45 MB, 11 MB | 32 B hash; 8 B big-endian |
| `git-to-leep-v1`, `leep-to-git-v1` | 45 MB, 28 MB | 32 B changeset id; 20 B git sha |
| `blob-claim-v1` | 24 MB | 8 B per blob (about 3M rows) |
| `history-node-v3` | 166 MB after 240k changesets, still deriving | `changed` sets of 32 B ids, names, 32 B row addresses |

**Row overhead is as large as the values.** Each `derived` row costs about
75 B of heap before its value: a 24 B tuple header, a 4 B line pointer, the
kind text (9 to 16 B) and the 33 B key. It costs another ~85 B in the
`(kind, key)` primary key, where a ~60 B entry sits on leaves about 70%
full because the keys are random. That is about **160 B per row**. Count
5 rows per changeset (root, generation, skiplist and the two mappings),
3M claim rows and about 1.2M history rows. That comes to about 12M rows,
roughly 1.9 GB of overhead beside 1.2 GB of values, which matches "derived
~3+ GB". Each changeset therefore costs about **1.5 KB in `derived`**
(703 B of values plus 5 × 160 B) before any path history. That is
2.2 GB for Linux.

**32-byte ids are most of what remains.** A skiplist entry is 80% id. The
`changed` sets were 55% of history-node bytes on the git/git slice (ADR 0037).
In memory, the ancestry cache costs about 170 B per changeset. It is capped
at 2 × 2^18 entries, so it holds about a third of Linux, and walks on Linux
miss it. Skiplist reads in `merge_base` were 9.3 s of a 97 s path-history
derivation on the 21k slice (35,234 reads at ~0.26 ms each), and they are not
cached.

ADR 0037 rejected small integer ids for history nodes. Its reasons were
that no deterministic number exists without a global counter, that two
servers deriving at once would need coordination and would not produce
byte-identical rows, and that an id prefix would need a lookup per entry.
This draft takes the counter as given. The database already holds the one
piece of shared mutable state Leep has, so it can hold a sequence too. The
sections below answer each of those objections.

## Decision (proposed)

### 1. Changeset numbers

Every stored changeset gets a **number** (`seq`), a positive integer that the
database assigns in the same statement that stores the changeset. The rules,
which go into FORMATS.md:

- **Server-local.** A number is never sent on the wire. No API, CLI, MCP
  output, log line, metric label, error message or ref uses one, so `/v1` is
  unchanged. Refs and the ref log keep 32-byte ids. Short ids stay hash
  prefixes.
- **Parents first.** A parent's number is always lower than its child's. This
  holds by construction: a changeset is stored only after its parents exist
  (an existing invariant), so their numbers were drawn earlier.
- **Never reused.** Gaps are allowed and mean nothing.
- **Per database.** One database is one repository today (the schema has
  no repository column). A multi-repository schema would need a sequence
  per repository.
- **64-bit in SQL** (`BIGINT`). Encodings use varints, and in-memory arrays
  use `u32` with a width check. At 100k changesets a day `u32` lasts a
  century. A swarm writing 10M snapshot changesets a day would exhaust it
  in about 14 months, so `u32` is never a format limit.

### 2. Assignment and consistency

- **Store.** A new table holds the number and the hot fields (§3). The
  Postgres `put_changeset_with_derived` statement gains one more CTE,
  `INSERT INTO changeset_index ... VALUES (nextval('changeset_seq'), $id, ...)`,
  so a changeset never exists without its number (ADR 0031's atomicity).
  SQLite uses `INTEGER PRIMARY KEY AUTOINCREMENT` inside the existing
  transaction. Memory uses a counter under its lock. `check_meta_contract`
  gains three checks: numbers are unique, parents are lower, and storing an
  existing changeset again keeps its number. The insert checks for the id
  first, so most repeated puts draw no number. Racing duplicates still burn
  one, which is harmless.
- **Land and group commit (ADR 0032).** Unaffected. A batch's replayed
  copies are stored one after another, each after its parent, so the chain
  is numbered in order. Refs move by the same CAS on 32-byte ids. Numbers
  are drawn from a sequence and are not part of the CAS, so they add no
  contention. Sequences are non-transactional and cost microseconds.
- **Several servers on one database.** They share the sequence and need no
  other coordination. Numbers follow the order of `nextval`, not the order
  of commit. A transaction holding 101 can commit before the one holding
  100. Only an in-memory or on-disk *copy* of the graph cares (§3, phase 3).
  Such a copy covers a prefix of numbers, refreshes past its high-water
  mark, and retries holes until they are older than twice the request
  timeout. Anything it lacks is read from SQL. Correctness never depends on
  a copy being complete.
- **Crash safety.** The number is written in the changeset's own statement
  or transaction. A crash before commit leaves a gap. Postgres may also skip
  up to 32 values after a crash, because sequences pre-log values in the
  WAL.
- **Backup and restore (ADR 0026).** `meta.db` copies `changeset_index`
  byte for byte like every table. The manifest gains `changeset_seq_last`,
  an additive field (format stays 1), next to `ref_log_last_seq`. Restore
  sets the sequence from it (`setval`, or `sqlite_sequence`), so restored
  numbers, and every derived row built from them, stay valid on any
  backend. A restore that forgets this fails loudly: the primary key
  refuses a duplicate number. The backup pool is untouched as long as
  changeset segments stay in the database (§4).
- **GC.** Changesets are never deleted today (ADR 0008, 0033). If a future
  ADR prunes abandoned snapshot changesets, their numbers become gaps and
  are not reused. Nothing can point at them: path history only names
  ancestors of stored changesets, and an ancestor cannot be pruned while
  its descendants remain.
- **Imports.** The importer already writes parents first, so a git history
  gets a dense range of numbers in topological order. That suits segments
  (§4).
- **Rebuild from scratch** (`leep-server renumber`, offline). Walk every
  changeset in (generation, id) order and assign 1..N, which gives a
  canonical, gap-free, topological numbering. Then rewrite
  `changeset_index`, set the sequence and drop every number-bearing derived
  kind so that it derives again. This is the repair for a damaged or lost
  index. It also compacts gaps. For Linux it costs one read of 2.9 GB of
  changesets plus 1.48M inserts, so minutes, followed by the path-history
  rederive (hours, as today). Every number-bearing row is valid only for the
  numbering it was built under. A random `numbering_epoch`, stored with the
  sequence, guards anything kept outside the database (cache files). Inside
  the database, `renumber` drops the dependent kinds.

### 3. What uses numbers

`changeset_index` gets one row per changeset. It folds in the three rows
that every create already writes together (ADR 0031) and reads together in
batches:

```
changeset_index(seq BIGINT PRIMARY KEY, id BYTEA NOT NULL UNIQUE,
                parents BYTEA NOT NULL,     -- parents' seq, LEB128 varints, in order
                generation BIGINT NOT NULL, root_tree BYTEA NOT NULL,
                skip BYTEA NOT NULL,        -- (seq, generation) varint pairs, entry k = 2^k first-parent steps up
                git_sha BYTEA)              -- imports only; replaces leep-to-git-v1
```

- **Skiplist.** It uses the same algorithm with numbers instead of ids:
  about 6 B per entry instead of 40 (both values fit in 21 bits on Linux).
  `merge_base` and `is_ancestor` code keeps its shape. Ties still go to the
  highest (generation, **id**), and the id is one indexed lookup away, so no
  answer changes. Tests compare against the current code on random DAGs.
- **Path history `history-tree-v4` / `history-node-v4`.** `changed` becomes
  a sorted set of numbers coded as delta varints, the first relative to
  zero. Everything else in ADR 0027 and 0037 stays: chunks, boundaries,
  patches and the `p² <= 128w` rule. The rule now sees smaller rows, so
  keyframe cadence shifts, which is part of why this is a new kind. On
  ADR 0037's objections: rows depend only on content, the stored
  predecessor and numbers that are stored facts, so all servers on one
  database derive identical bytes. Rows are no longer identical across
  databases, which nothing relies on. A lookup that yields numbers turns
  them into ids with one batched read, or from the in-memory graph
  (phase 3), never a round trip per step. Numbers are exact, so collisions
  cannot happen.
- **Git mapping.** `git-to-leep-v2` holds the number as a varint, about 3 B
  instead of 32. `leep-to-git` becomes the `git_sha` column.
- **In-memory graph (phase 3).** Each server keeps, per number, the id
  (32 B), the first parent (4 B), an offset into an array of extra parents,
  the generation (4 B) and the first-parent depth and skip anchor (8 B):
  about 56 B per changeset, or **~85 MB for all of Linux**. It loads from
  `changeset_index` at start (one sequential scan) and tails new numbers.
  Today's cache spends 170 B per changeset and covers a third of Linux.
  `merge_base`, `is_ancestor`, the log heap and the merge ancestry of
  path-history derivation then stop doing round trips. With the whole graph
  in memory, one skew-binary jump pointer per changeset (as Bitcoin's
  `pskip` and Mononoke's commit graph use) could later replace the list.
  `verify` builds its own graph from changesets and never trusts the
  stored one.

### 4. Changesets in segments

Old changesets move from `changesets` into **segments**: zstd frames of
about 32 KB, each holding consecutive numbers (about 40 changesets), with a
small offset table. They are stored in Postgres:

```
changeset_segments(id BIGINT PRIMARY KEY, first_seq BIGINT NOT NULL, data BYTEA NOT NULL)  -- STORAGE EXTERNAL: no second pglz pass
changeset_index gains: segment BIGINT, slot SMALLINT   -- NULL while the changeset is still a row in `changesets`
```

New changesets are still written to `changesets` (the "loose" tier), so the
write path and ADR 0031 do not change. A background `pack-changesets` job
runs one transaction per segment: insert the segment, set the location in
the index rows, delete the loose rows. That is atomic, needs no fence and
leaves a reader with nothing to race. Reads decode the segment, check each
changeset against its id as today, and keep decoded segments in an LRU.
Readers walk in roughly topological order (derivation oldest first, log
newest first), so one segment serves many reads.

Segments stay **in Postgres**, not the blob store. That keeps backup and
restore, `REPEATABLE READ` snapshots and atomic moves exactly as they are.
It also avoids a ranged GET of 20 to 50 ms against S3 on every cold
changeset read in a history walk. Putting segments in blob packs (ADR 0029,
0040) would also cut the database by the segment bytes. But it adds a
two-store consistency argument (a snapshot names a segment that must exist
when the backup copies it) and remote latency. Revisit it only if database
size, not total size, becomes the constraint.

**Compression dictionaries** are not part of this decision. Segments get
most of a dictionary's gain without a versioned, shared dictionary row. For
derived rows, ADR 0037 measured 2.0x that decayed as history moved past the
dictionary. After v4, those rows are mostly names and 32 B row addresses,
and row overhead dwarfs what a dictionary could save (below).

### 5. Formats, kinds and migration

- **Wire:** nothing changes. **Hashes:** nothing changes. `Changeset`,
  `TreeNode` and every id are as before.
- **SQL:** migration 0002 adds `changeset_index` and `changeset_seq`, and
  migration 0003 adds `changeset_segments` and the location columns, both
  for both backends with fixtures. `snapshot.rs` Source and Sink and
  `snapshot_tests.rs` learn both tables.
- **Kinds** (per `change-persistent-format`):

| Kind | Phase | Becomes |
|---|---|---|
| `skiplist`, `generation`, `root_tree` | 1 | columns of `changeset_index`; old rows retired, removed with `repair drop` once the backfill is verified |
| `leep-to-git-v1` | 1 | `git_sha` column |
| `git-to-leep-v1` | 1 | `git-to-leep-v2` (value = number) |
| `history-tree-v3`, `history-node-v3` | 2 | `history-tree-v4`, `history-node-v4`; v3 retired like v1 and v2 |
| `last_merge-v1` | 3 | dropped (the in-memory graph knows it) or kept unchanged |

  `leep_derive::integrity::kinds` learns the columns. `verify` checks that
  each changeset has exactly one index row and that `parents` matches the
  decoded parents' numbers, and it recomputes generation, root and skip as
  it does today.
- **Migration of existing databases: offline backfill.** The operator stops
  every server. Old servers refuse schema 2, but a running one would keep
  writing changesets without numbers. The operator then runs
  `leep-server migrate`, which creates the table and fills it in
  (generation, id) order: parents from the changesets (one scan), root and
  generation copied from the existing rows (batched), skip computed in
  memory. Estimate for Linux: 2.9 GB read plus 1.48M inserts, about 2 to
  10 minutes. Measure on the slices first. Online backfill (new changesets
  numbered above a reserved range while old ones are filled in) is possible.
  It is not worth its states (unknown parent numbers during the fill) at
  this size. Leave it until a deployment cannot take minutes of downtime.
  Path history v4 is derived by the warmer in the background. Queries keep
  reading v3 until the head's v4 row exists.
- **Downgrade:** forward-only, as ADR 0023. Keep the retired rows until
  `repair drop` so a pre-upgrade backup still matches. Rolling back means
  restoring that backup.

## Estimates

Linux, Postgres. Row overhead is taken as 160 B per `derived` row and
~110 B of indexes per `changeset_index` row (~235 B heap). Treat the figures as ±30% until
measured.

| Win | Arithmetic | Saving |
|---|---|---|
| **A. Numbers in values** (skiplist only) | 15.3 entries × (40 → 6 B): 907 → ~140 MB | ~0.75 GB |
| **B. One row per changeset** (needs the index table; A makes it compact) | removes the root, generation, skiplist and leep-to-git rows (4 × 160 B) with their 60 B of other values and the 92 B compact skip value; adds a ~345 B row with its indexes; `git-to-leep` value 32 → 3 B: 1.48M × (640 + 60 + 92 − 345 + 29) | ~0.7 GB |
| A + B together (phase 1) | 2.2 GB of per-changeset rows → ~0.5 GB index + ~0.23 GB `git-to-leep-v2` | **~1.4 GB** |
| **C. History v4** (phase 2) | Leaf with one id, name n: n+35 → n+6 B (~2.8x at n=10); Tree entries and patches keep 32 B row addresses: ~2x on values overall | ~85 MB of the 166 MB so far; a few hundred MB to ~0.8 GB once Linux is fully derived |
| **D. Segments** (phase 4) | 1.8 KB per changeset, of which ~300 B is hashes (parents, blob contents, hex sha) and ~1.5 KB text compressing 3-4x per 32 KB segment: ~0.7 KB (0.6-0.9). Also drops the old table's 160 B per row | 2.9 → ~1.05 GB: **~1.8 GB** |
| E. Dictionaries for derived rows | after C, ~1.5-2x on history values at best; row overhead untouched | ≤ 0.25 GB; not proposed |

Expected end state: Postgres 8.3 → **~4.5-5 GB**, Leep total 11.5 →
**~7.7-8.2 GB** (git 3.8 GB). The blob packs (3.2 GB) do not change. Also
out of scope, and worth its own look: `blob-claim-v1` is 24 MB of values in
about 0.5 GB of rows.

Speed, separated the same way:

- **Numbers plus the in-memory graph (phase 3)** are the speed win. A cold
  `merge_base` drops from up to 64 skiplist reads (~17 ms at 0.26 ms) plus
  walk windows to microseconds. A create skips the 2-3 skiplist reads it
  makes at steady state (log2 of history on a fresh server), about 0.5 ms of
  a 4.1 ms p50. Path-history derivation loses its skiplist reads (~10% on
  the slice) and its ancestry-cache misses, which are worse on Linux because
  the cache holds a third of it: an estimated 10-25%. A create also makes
  fewer random index insertions: today 4 (changeset plus 3 derived keys),
  afterwards 2 plus one append.
- **Numbers without phase 3** save little time. Rows get smaller, but the
  round trips stay the same.
- **Segments** make cold reads of single changesets slightly slower
  (decompressing ~32 KB is 20-40 µs, next to a ~100 µs round trip). Sequential
  walks get faster, because one row read serves ~40 changesets.
- **Repack (620 B per object) and the importer (8.5 GB) are not helped.**
  Repack's maps are keyed by blob and tree hashes, and the importer's by git
  oids and blobs. Each holds a hash several times over (`objects`, `seen`,
  `hints`, `sources`, `live`) plus a `PathBuf` per loose object. The same
  idea applied in-process fixes repack with no format change: intern each
  hash once (32 B plus a 4 B index) and keep everything else as `u32`. That
  is about 100-130 B per object, or about 1-1.3 GB instead of 6.2 GiB for
  10M objects. The importer's growth needs a heap profile before any
  design: the maps found in `import.rs` add up to well under 1 GB.

## References

- **git commit-graph.** Each commit is a *position* in a file (lexicographic
  by oid within a file; split chains offset positions per layer). Parents
  are stored as positions, generation numbers are corrected commit dates
  (v2), and changed-path Bloom filters cover up to 512 paths per commit.
  Positions are local and rebuildable, and they never leave the repository.
  That is the model for phase 3. Leep's path index already answers what the
  Bloom filters approximate (it beats git with a commit-graph on rare paths,
  PERFORMANCE.md), so no filters are proposed.
- **Sapling segmented changelog.** It assigns ids in DFS order so that
  ancestry becomes span arithmetic over "flat segments". A master group
  holds fixed ids and a non-master group holds reassignable ones. The
  server's IdMap translates id and hash. Leep does not need segments at
  ~85 MB for the whole graph in RAM. Segments become interesting at a scale
  where the graph does not fit.
- **Mononoke.** As far as its open-source SQL schemas show, its changesets and commit-graph tables pair an
  auto-increment integer with the bonsai hash and store parent and skip
  edges as integers: a server-local number exactly like this one, never
  exposed. Its segmented-changelog IdMap is versioned, so a rebuilt
  numbering can be swapped in, which is what `numbering_epoch` and
  `renumber` provide here.
- **Mercurial revision numbers.** These are revlog positions in arrival
  order, so the same commit has different numbers in different clones, and
  strip or rebase renumbers. Users who quoted `r1234` in bug reports pointed
  at the wrong commit, and the advice became "use hashes". In Leep the
  failure cannot happen because numbers never leave the server: there is one
  database per repository, no clones and no output that carries them. The
  analogue that remains is a database restored or rebuilt with a different
  numbering. Backup keeps the numbers, and `renumber` drops everything built
  on the old ones.

## Plan

0. **No format change (any time):** intern hashes in repack's walk and
   writer (6.2 GiB → ~1.2 GB for 10M objects). Heap-profile the importer.
1. **Numbers plus `changeset_index`** (migration 0002, offline backfill):
   skiplist, generation, root and the git mappings fold in. This is the
   smallest step that pays for the numbering (~1.4 GB). It touches the
   write path, `Repo::{root_tree, generation, skiplist}`, verify and backup,
   but no deriver logic.
2. **`history-*-v4`** with numbered `changed` sets, derived by the warmer
   while v3 serves.
3. **In-memory graph** loaded from `changeset_index`. Add an mmap cache
   file (with epoch and high-water mark in its header) only if loading at
   start exceeds ~10 s.
4. **Changeset segments** in Postgres, packed in the background.

**Risks.** Numbers become state that must be right. Duplicates are refused
by the primary key. Wrong parent numbers are caught by `verify` and fixed by
`renumber`, which costs a path-history rederive. Upgrading requires stopping
every server. Number-bearing rows cannot move between databases. Answers
that tie-break by id must keep doing so. A number could leak into a
response, which a test over every `api` type and a grep in CI prevent.
Segment read latency on cold, scattered reads needs measuring.

**Measure first on the 3,795-commit git/git slice and the first 21,109
Linux commits (local Postgres 16), before the VPS:**

1. `pg_total_relation_size` per table, row counts and `sum(length(value))`
   per kind, and `pgstattuple` dead tuples (upserts rewrite rows). These
   confirm the 160 B per row figure behind estimate B.
2. The distribution of skiplist entry counts.
3. Changeset compressibility: `sum(pg_column_size(data))` against
   `sum(length(data))` (today's pglz share), then zstd per row, per 32 KB
   segment, and with a dictionary trained on the older half and applied to
   the newer half.
4. The v4 ratio without writing the deriver: re-encode existing v3 rows
   with ids replaced by numbers, and break bytes down into ids, names and
   addresses as ADR 0037 did.
5. Backfill time on each slice, extrapolated to 1.48M. Then a backup,
   restore and `verify --full` drill that checks `changeset_seq_last`.
6. Before and after each phase: cold and warm `merge_base`, path-history
   derivation rate, import rate, create and land p50, `leep-land-bench` at
   64 agents (no new contention expected), and RSS of the in-memory graph
   for 1.48M synthetic changesets.

## Consequences

- If the estimates hold, the database shrinks by about 40%. Most of that
  comes from fewer rows and compressed changesets, not from shorter ids.
- Ancestry stops being a source of round trips once phase 3 lands.
- The server gains a second piece of shared state beside refs: an
  append-only sequence. It is never on the wire, rebuildable from
  changesets, and preserved by backups.
- ADR 0037's rejection of integer ids is superseded for path history (and
  ADR 0037 would get a status note if this is accepted). ADR 0026 gains a
  manifest field, and ADR 0031's write gains a table.
  ARCHITECTURE.md §4-§6 and FORMATS.md change with phase 1.
