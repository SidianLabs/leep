# 0106. Code search rows as derived segments (family `content-v1`)
Status: Accepted
Date: 2026-11-16

Amends [0102](0102-blob-trigram-patches-and-retention.md) (retention of
blob rows: a row sealed while live outlives its window until its segment
is rewritten), [0105](0105-derived-segment-families.md) (its planned Stage
B), and where the rows of [0014](0014-code-search.md),
[0094](0094-patched-directory-filters.md) and 0102 may be stored. Stage B
of storage PR 6; third step of the storage plan's item A1.

## Context
After ADR 0105, corpus B (Leep's history at 89fb9ad plus 2,000 one-file
edits, 2,231 changesets) takes 14.31 MB on Postgres in `retire` after
`VACUUM FULL` (7.23 MB of database above an empty one, 7.07 MB of blobs),
2.17 times Git's 6.58 MB (ADR 0104's figure; 2.25 times the 6.35 MB of the
reconstruction measured here), and 14.69 MB on SQLite. The target is at
most twice Git in that mode (maintainer decision, 2026-11-15). ADR 0105's
Stage 0 put the code search rows at about 4.35 MB of Postgres bytes on B
(`blob-trigrams-v2` 802 rows, `tree-trigrams-v2` 104), about 64% of what
`derived` then held, and their zstd blocks at about 3.05 MB.

The search rows differ from the per-changeset kinds in three ways that
matter to a segment:

- **Keys are content hashes**, shared by every head and agent that holds
  the same bytes; a changeset order means nothing to them.
- **A row is not single-valued.** A blob or directory reached from another
  predecessor may be stored as another patch of the same set or filter
  (ADR 0094, 0102), so a byte comparison before a delete would leave
  permanent residue.
- **Retention deletes them** (ADR 0102): rows of blobs no head has held for
  `--search-retention` go. Retention lists rows, and listings stay rows, so
  a sealed value is invisible to it.

## Decision

### 1. Family `content-v1`
Number 3, manifest key `Hash::of_object("derived-segment-family",
"content-v1")`, in the container of ADR 0104 (index, data and manifest
bytes unchanged). It tolerates unknown columns as `changeset-v2` does.
Columns, numbered forever:

| Column | Kind | In the segment | Equality before delete |
|---|---|---|---|
| 1 | `blob-trigrams-v2` | the row verbatim | the trigram set it stands for |
| 2 | `blob-trigrams-keyframe-v2` | the row verbatim | bytes |
| 3 | `tree-trigrams-v2` | the row verbatim | the filter it stands for |
| 4 | `tree-trigrams-keyframe-v2` | the row verbatim | bytes |

No new codec: a value is sealed only if it parses as its kind (a whole row
that decodes, a patch whose two lists decode, a keyframe that is a whole
set or filter), and read back as the bytes it is, which search reads as it
reads a row. The two candidate codecs of the design (blob sets as ranks in
a per-segment dictionary, filters as residuals over their children) would
each take a new column number later without re-encoding stored segments.

### 2. Order: the heads' walk
Keys are numbered by a depth-first walk of the heads' trees (a directory,
then its files in name order, then its subdirectories), each key at its
first place; keys the walk does not reach (a keyframe of an old version, a
row sealed while live) come last, in key order. A directory's filter and
its files' rows then sit in the same blocks of their columns, and a
directory's filter beside its subdirectories', whose bits it holds: the
blocks compress better (below). `search::retention` exports the walk with
the live set (`Repo::content_live`), one batched read per level of trees,
each distinct node once, through a handle that never fills the node cache.

### 3. Only what retention keeps (amends ADR 0102)
Accepted by the maintainer on 2026-11-15.

- A pass seals a `blob-trigrams-v2` or `tree-trigrams-v2` row only while
  retention keeps it live: its blob or tree is below a head current now
  or within the server's own `--search-retention` (all rows when it is
  `off`). Superseded rows stay rows, for retention to delete as before.
- Keyframes are always sealed and never left out of a merge: a live patch
  may stand on the keyframe of a version no head holds, and retention
  never deletes them either.
- A merge leaves out keys retention no longer keeps, and a segment of
  which a third or more of the values are such keys is rewritten alone. A
  sealed row that becomes superseded therefore outlives its window until
  then. Keys are left out only while the walk that judged them is current
  (the newest ref log entry has not moved since), and the walk is redone
  when the ref log moved (at most every `list_every`, 10 minutes) or
  after an hour.
- Dropping a sealed key is always safe: it reads as missing and is derived
  again on demand. If a head returns to content just left out (a revert
  racing a merge), retention's backfill derives its rows, as it does for
  any head whose filters are stored and blob rows are not (ADR 0102).

### 4. Patches and keyframes together
- A row is deleted only when the row and the segment's value stand for the
  same set or filter, each patch resolved against its keyframe as any read
  finds it (a row, or a live segment's value). A patch whose keyframe is
  not found is equal to nothing: the row stays, uncounted, and is compared
  again after the next listing. So a patch row is never retired against a
  value its keyframe is missing for, and a keyframe row goes only once a
  live segment holds the same bytes.
- A keyframe lost later (its segment dropped as damaged, a `repair drop`
  racing a derivation, a column cleared) is covered by the rule of ADR
  0094 and 0102: a patch whose keyframe is not stored reads as not stored,
  so search derives the row whole and writes it; the pass then keeps that
  row (its sealed patch cannot be resolved).
- `repair drop --kind blob-trigrams-v2` (or `tree-trigrams-v2`) clears both
  of its columns, keyframes with patches, by swap before deleting the rows
  (the kinds' `also`, as before).

### 5. Size and latency
- Data blobs of at most 1 MiB (`changeset-v*` keep 4 MiB): a cold read
  takes a whole data blob for one row.
- Blocks of about 512 KiB of rows at zstd level 17 (the others keep 64 KiB
  at level 9): a dense filter alone is 64 KiB, and the rows repeat across
  rows more than within one. Measured on B's search rows in key order, zstd
  output of 64 KiB blocks at level 9 3.02 MB, 256 KiB at level 17 2.90 MB,
  512 KiB at level 17 2.82 MB, 1 MiB at level 17 2.62 MB (4.2 MB of rows,
  0.8 s of one core at level 17). A block and the zstd level are the
  writer's choice: the block directory says where each lies.
- A seal reads a page a batch of keys at a time and closes a segment once
  its rows reach four thirds of the cap, so a pass holds about one
  segment's rows at once (a dense filter is 64 KiB).
- Two tiers: segments under half of both caps (1 MiB, and 16,384 keys)
  merge with up to three neighbours while the result fits, so segments end
  between half the cap and the cap; full ones are only rewritten once a
  third dead. A row is written about twice. (A first version kept every
  segment over a quarter of the byte cap apart: in a soak with small
  limits, 1,978 such segments of 8 to 32 KiB accumulated.)
- `--derive-cache-bytes` is not raised: the 32 MiB data cache and 16 MiB
  block cache are shared with the other families (measured below).

### 6. The one-way door: `derived-segments-3`
Recorded before the first `content-v1` row is deleted, by the same gate as
ADR 0105: in `retire`, only a server naming `content-v1` (or `all`) in
`--derived-segments-retire-families`, or finding the feature recorded.
Binaries of ADR 0105 and before do not know it and refuse the store: they
read no `content-v1` segment (every search would derive its rows again)
and their `gc` stops at the family's manifest row anyway. `build` records
nothing and keeps every row, so older servers answer everything; but an
ADR 0105 `gc` refuses to run while the `content-v1` manifest row exists, as
ADR 0105 (section 5) decided for any family it does not know, so roll a
fleet's `gc` back only with its servers (or delete that one manifest row
by hand once the servers are rolled back: every row is still there). The
same lease covers every family; each manifest is swapped on its own.

### 7. Retention beside the passes
Retention keeps its own lease and touches only rows, so the two need no
coordination:

- Retention deleting a row a pass is sealing: the pass seals the value it
  read; a later walk finds the key dead and a merge leaves it out.
- A search's `put_derived_if_absent` writing a row for a sealed key (its
  row retired): the next pass finds it held, equal by set, and deletes it
  again; the store converges.
- A derivation rewriting a row between a pass's compare and its delete:
  the delete is value-conditional, so the row stays and the next pass
  compares it again.

Retention's backfill asks `derived_present`, which consults the segments,
so a retired head row is not derived again. Rows are retired at least 10
minutes after their segment is published and a miss is judged against a
manifest at most 60 s old (ADR 0104), so retention never takes a retired
row for a missing one.

### 8. Integrity
Listings stay rows, so `integrity::rows::RowScan` gained `RowScan::of`,
which also visits the keys only a live segment holds, in key order, read
through the view (a row still wins). `verify` (`scan_blob_trigrams`,
`scan_tree_trigrams`) and `repair rederive` of the four kinds use it, so a
sealed value is checked like a row and a wrong one rewritten as a row.
`count_derived` and `estimate_derived` already count sealed keys (ADR
0104). Audited and unchanged: `upgrade-index` (per-changeset and retired
kinds only), the copy of retired `blob-trigrams-v1` rows (reads v2 through
the view), the sweep and blob GC (every family's manifest, ADR 0105).

### 9. Reads
The view serves the four kinds like the others. A batched read's miss now
refreshes every family's manifest with one batched read even for one key
(a single-row read's miss still uses single-row reads, ADR 0105): the
search paths read rows with `get_derived_many`, often of one key.

## Cost
- A pass walks the live set when the ref log moved (one batched read per
  level of distinct trees of the window's heads) and holds it while it
  seals or merges: about 50 bytes a live blob or tree.
- `budget_tests`: a search on a new process makes 32 metadata calls, not
  31 (the first miss of a search row reads the three manifests in one
  batched read, at most once a minute); search land stays 11 and create
  13; the blob calls of a sealed create stay 10 (no search rows there).
- A cold search reads each touched content data blob whole (at most 1 MiB:
  milliseconds on a filesystem, 50 to 150 ms on an object store, where
  ranged reads are the follow-up).

## Consequences
- Corpus B, measured as in ADR 0105 (release builds of 784bed6 and this
  change, every family named in `--derived-segments-retire-families`, `gc`,
  `VACUUM FULL` / `VACUUM`):

  | Backend | Before | After | x 6.35 MB (rebuilt) | x 6.58 MB (ADR 0104) |
  |---|---|---|---|---|
  | Postgres | 14.31 MB | **12.04 MB** (2.34 db + 9.69 blobs) | 2.25x -> **1.90x** | 2.17x -> **1.83x** |
  | SQLite | 14.69 MB | **12.48 MB** (2.79 + 9.69) | 2.31x -> **1.97x** | 2.23x -> **1.90x** |

  The ≤2x target is met on both backends against both Git figures, in
  `retire` after `VACUUM FULL`. The search rows (4.23 MB of key and value
  bytes) became 4 segments of 2.62 MB. The design estimated about -1.25 MB
  (±0.3) with 64 KiB blocks; measured with them -1.97 MB on Postgres
  (12.34 MB, 1.94x / 1.88x) and -1.92 MB on SQLite (12.77 MB, 2.01x of
  6.35 MB), so the larger blocks were adopted (-0.30 MB more). Most of the
  saving is Postgres's and SQLite's per-row and TOAST overhead and the
  rows' own space once the table is vacuumed.
- Search latency on B (PERFORMANCE.md, "Derived segments, family
  content-v1"): round 1 p50 8.0-9.6 ms against 10.0-11.2, p99 84-93
  against 91-109; warm p50 6.5-7.3 against 7.5-8.2. No regression on a
  filesystem blob store; object stores need ranged reads (follow-up).
- The storage size guard measures `content-v1` per key and against its
  rows (new lines, `crates/leep-e2e/tests/storage_size.rs`).
- `budget_tests`: search on a new process 31 -> 32 metadata calls (the
  manifests, once a minute); search land 11 and create 13 unchanged.
- Soak (8 minutes, 50 agents and 2 CLI agents, Postgres, `retire` naming
  every family, the small limits): passes, 6.0 lands a second, land p99
  42 ms; `content-v1` 294 live segments, 32,306 rows retired. With the
  first tier rule the same soak failed twice the same way: 1,978
  `content-v1` segments accumulated and a CLI agent made no progress for
  120 s about five minutes in (no command finished, so which one stalled
  was not recorded); with the rule above it did not recur.
- Deviations from the design: blocks of 512 KiB at zstd level 17 instead
  of the other families' 64 KiB at 9 (measured, above); a merge leaves keys
  out only while its walk is current; keyframes are never left out (they
  are not retired by retention either); a patch whose keyframe cannot be
  resolved is neither equal nor counted as a mismatch; the seal trigger
  stays a key count (1,024, or 64 idle for 10 minutes, as the other
  families) and only segment cuts and tiers are by bytes; a batched read's
  miss refreshes the manifests in one batched read even for one key; the
  amendment is this ADR rather than a second part of ADR 0105, since it
  amends ADR 0102 too.
