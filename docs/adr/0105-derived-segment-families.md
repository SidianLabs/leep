# 0105. Derived segment families: generations, last merges, root trees and log filters as segments
Status: Accepted; Stage B is [0106](0106-search-rows-as-derived-segments.md)
Date: 2026-11-15

Amends [0104](0104-derived-segments.md) (one family; its extension
points) and the storage of `generation`, `last_merge-v1`, `root_tree`
(ADR 0001) and `log-filter-v1` (ADR 0047) rows. Second step of the storage
plan's item A1 (pure-function derived data as segments in the blob store).
Stage A of storage PR 6; Stage B, a hash-keyed family `content-v1` for the
code search rows, is planned below and not part of this change.

## Context
After ADR 0104, corpus B (Leep's history at 89fb9ad plus 2,000 one-file
edits, 2,231 changesets) takes 15.85 MB on Postgres in `retire` after
`VACUUM FULL`, 2.41 times Git (6.58 MB; 2.50 times the 6.35 MB of the
reconstruction measured here). The target is at most twice Git, judged in
`retire` after `VACUUM FULL` with `build` staying the default (maintainer
decision, 2026-11-15).

### Stage 0: where the derived bytes go
`crates/leep-server/examples/derived_breakdown.rs` (new) lists every kind's
rows, key and value bytes and an estimate of the Postgres heap tuple, line
pointer and primary key entry (TOAST ignored), then zstd -9 of 64 KiB
blocks of candidate encodings. On corpus B with default flags after `gc`
(before `retire`), Postgres:

| Kind | Rows | Key and value bytes | Est. Postgres bytes | Share |
|---|---|---|---|---|
| `blob-trigrams-v2` | 802 | 2,274,912 | 2.38 MB | 23.3% |
| `history-tree-v4` | 2,231 | 1,786,093 | 2.07 MB | 20.3% |
| `tree-trigrams-v2` | 104 | 1,958,356 | 1.97 MB | 19.3% |
| `skiplist` | 2,231 | 977,832 | 1.22 MB | 12.0% |
| `blob-claim-v1` | 5,968 | 238,720 | 0.90 MB | 8.8% |
| `log-filter-v1` | 2,231 | 332,237 (149 a row) | 0.59 MB | 5.7% |
| `git-to-leep-v1` / `leep-to-git-v1` (importer maps) | 2,231 each | 142,784 / 116,012 | 0.41 / 0.37 MB | |
| `root_tree` | 2,231 | 142,784 | 0.39 MB | 3.8% |
| `generation`, `last_merge-v1` | 2,231 each | 89,240 each | 0.34 MB each | 3.3% each |

`pg_column_size` of the heap tuples alone (after `VACUUM FULL`):
`generation` 171,787, `last_merge-v1` 178,480, `root_tree` 223,100,
`log-filter-v1` 423,709 bytes; the four kinds take about 1.0 MB of heap and
about 0.55 MB of primary key entries.

Candidate encodings of the four kinds in `(generation, id)` order, zstd -9
blocks:

| Column | Rows verbatim | Varint codec |
|---|---|---|
| `generation` | 2,305 | 4,537 |
| `last_merge-v1` | 25 | 22 |
| `root_tree` | 73,643 | (incompressible hashes) |
| `log-filter-v1` | 5,577 | (verbatim) |
| key index (32 bytes a key, ordinals, presence bits) | about 76,842 | |

So about 1.65 MB of rows (the example's estimate with Postgres row
overhead) become a segment of about 0.16 MB, a tenth of them, nearly half
of it the key index and a third the root trees; eight-byte
generations in generation order compress better than varints, so the codec
keeps the eight bytes. The design's estimate was about 2.0 MB becoming
about 0.2 MB, -1.75 MB (±0.25): measured, -1.5 MB, at the edge of that
band and not a contradiction. Code search, the planned Stage B: blob rows
2.38 MB estimated become 1.60 MB of zstd blocks plus 26 KB of keys, tree
rows 1.97 MB become 1.42 MB plus 3 KB, as the design said (about 4.4 MB to
3.1 MB).

### What ADR 0104 made awkward
1. An unknown column reads as damage (`parse_index`), and a pass drops a
   segment it cannot parse: an ADR 0104 binary would drop a `changeset-v1`
   segment that gained a column, losing the only copy of retired rows. So
   new kinds cannot go into `changeset-v1` while ADR 0104 binaries may run.
2. An ADR 0104 `gc` reads only `changeset-v1`'s manifest and would collect
   another family's live segments as orphans.
3. The compactor is written for exactly two kinds (`KINDS`, `other(col)`,
   the lone-row rule, the per-column equality special case).
4. The compactor reads `generation` rows to order a segment; once
   `generation` is sealed and retired, that must come from segments.
5. `kind_column`, `manifest_key()`, the view and `drop_columns` assume one
   family.
6. Listings stay rows: anything listing a moved kind would miss sealed keys.

## Decision

### 1. Families
Families are one table (`derived_segments::codec::FAMILIES`). A family has
a number, a name (its manifest key is `Hash::of_object
("derived-segment-family", name)`), columns, the store feature its deletes
need, and whether it tolerates unknown columns. A column has a number, the
kind it holds and a codec: encode (refusing a value that does not decode
as its kind, so it is never sealed), decode, and the equality a row is
compared by before it is deleted. `kind_column(kind)` gives `(family,
column)`. The index, data and manifest layouts of ADR 0104 do not change;
`changeset-v1` is byte for byte as before, with its golden bytes.

### 2. Family `changeset-v2`
Number 2, keys are changeset ids, ordered by generation (the sort key is
generic, `SealRow::order`; both families use the generation). Columns,
numbered forever:

| Column | Kind | Encoding in the segment | Equality before delete |
|---|---|---|---|
| 1 | `generation` | the eight big-endian bytes | bytes |
| 2 | `last_merge-v1` | the eight big-endian bytes | bytes |
| 3 | `root_tree` | the 32 bytes | bytes |
| 4 | `log-filter-v1` | the row's bytes. Sealed only if it decodes and is its own canonical encoding, a policy: a segment never holds a value the kind would not write. Decoding takes the bytes without parsing them (the data blob is checked by address, and the kind's reader checks the value where it is used), so a value that would not parse is never counted as segment damage | bytes: a merge over the side budget stores one of several right supersets, so a row derived again may differ from a sealed one and stay (counted in `leep_derived_segment_mismatch_total{kind="log-filter-v1"}`, which is then not a sign of damage) |

A key is expected to have all four; one missing a row (the warmer derives
`log-filter-v1`, a query `last_merge-v1`) waits `lone_wait` (10 minutes),
as `history-tree-v4` does in `changeset-v1`, then seals with what it has.
`cs-reach-v1` stays a row: it is GC bookkeeping with a closure rule, not a
pure function.

### 3. Forward compatibility (point 1)
A family added after ADR 0104 tolerates unknown columns: an index naming a
column this binary does not know parses; the column gives no value and its
blocks are not parsed (a later column may frame them otherwise; the index
checks only that they lie inside the data blob, past its header); a segment
holding one is never merged (the merge could not carry it), never dropped
for it, and `repair drop` keeps its bit. `changeset-v1` still reads an
unknown column as damage, as ADR 0104 binaries do, so it never gains
columns: later kinds take `changeset-v2` columns or a new family.

What tolerance does not cover is protected only by a store feature (which
the change that makes it records before writing it): an index or data
blob of another version, a column number of 64 or more (the manifest's
mask has 64 bits), another row framing inside a known column, or a new
family (whose manifest row an older `gc` refuses, section 5).

### 4. Mixed fleets: the store feature `derived-segments-2`
Recorded before the first `changeset-v2` row is deleted (each family
records its own feature before its own first delete, in every process).
ADR 0104 binaries do not know it and refuse the store, so they never derive
the moved kinds from the root or collect `changeset-v2` segments as
orphans. `build` records nothing: rolling back from it is free (an ADR 0104
`gc` may collect `changeset-v2` segments as orphans; every row is still
there and the next pass seals again). A damaged `changeset-v2` manifest is
replaced automatically only on a store without `derived-segments-2`, by a
process that would not retire the family (`build`, or `retire` holding it
back).

**A door is passed only on purpose.** A fleet already running
`--derived-segments retire` would otherwise pass the second door just by
upgrading, perhaps with an ADR 0104 server or cron job still about. So
`retire` deletes a family's rows (and records its feature first) only if
the family is named in `--derived-segments-retire-families` (a comma list
of family names, or `all`; `changeset-v1` is always included, as ADR 0104's
`retire` meant), or its feature is already recorded in the store (another
server, or an earlier run, passed the door). Otherwise the family still
seals and merges, and the server says once per process that it holds the
family back. While it holds a family back it asks the store whether the
feature is recorded at most once a minute (each ask opens a connection),
so another server passing the door is seen up to a minute late. The flag
takes Stage B's `content-v1` the same way.

| Situation | What happens |
|---|---|
| Fresh store, `retire`, no list | As ADR 0104: `changeset-v1` records `derived-segments` and retires; `changeset-v2` seals only |
| Fleet on `retire` (ADR 0104) upgrades to this release, no list | `changeset-v1` goes on; `changeset-v2` seals, records nothing, deletes nothing; ADR 0104 binaries still open the store |
| The operator names `changeset-v2` (or `all`) once every binary runs this release | The server records `derived-segments-2`, then retires `changeset-v2` rows; ADR 0104 binaries refuse the store from then on |
| Another server, not naming it, on a store with `derived-segments-2` | Retires `changeset-v2` too: the door is already passed |
| Every binary rolled back to ADR 0104 while `changeset-v2` only built | Free: its manifest row is ignored, its segments are orphans |

**Blob GC of a binary that cannot read a family.** Such a `gc`, started
before the family's feature was recorded, could have condemned the family's
live segments as orphans. A pass does not delete rows of a segment either of
whose blobs carries a condemnation (`blob-condemned-v1`): they go after the
next collect (if the segment survives it) or stay, the segment dropped and
sealed again. The condemnations are read before a segment's rows are
compared and again before each delete statement. For a family's first
delete that second read comes after the feature is recorded; from then on
such binaries refuse the store, so only a `gc` already under way could
condemn later. That window
is closed by the rule to set the flag only once every binary runs this
release. The warning is logged once per segment per process.

### 5. GC over every family (point 2)
`gc_view` lists every `derived-segments-v1` row and unions the live and
retired blobs of every family. A row that does not decode, one a newer
binary wrote, or one whose key is not a known family's stops GC: it cannot
tell that family's live segments from orphans. A blob live in one family is
never collected as another's retired.

### 6. The compactor by table (points 3 and 4)
One pass runs every family in turn under the one `maintenance/derived-segments`
lease; each family's manifest is swapped by its own compare-and-swap. What
a process carries between passes (ADR 0104's backfill, retirement, `known`
segments, lone and mismatch state, verified and suspect segments) is kept
per family, so one family's segments never invalidate the other's carried
work, and `known` covers every live segment of its family. Seal candidates
use the family's expected-columns mask instead of the two-kind rule; rows
carry a mask per column. A family whose pass fails (a damaged manifest with
its feature recorded) does not stop the other; the pass returns the first
error once all ran, except a lost lease or swap, which stops at once.
Whether a key's changeset is still stored is asked of the changesets
themselves (`changesets_stored_at`, or `get_changesets` where a store cannot
say when), in batches: a seal skips one that is not, and a merge leaves it
out, so ADR 0104's rule that merges shed swept changesets holds in both
families even though a sealed `changeset-v2` segment still answers the
generation of a swept changeset. Generations are then read through the view
only to order the stored ones, so once `generation` is sealed and retired
they come from `changeset-v2` segments (and are derived only for changesets
no row or segment holds).

A pass's bounds hold over every family together: `max_seal_time` counts
from the start of the pass, `max_retire_time` from its first delete, and
`max_retire_rows` over both families, so a pass stays well under the two
minutes `repair` waits for the lease. The family that goes first rotates
each pass. A family's rows are listed again in mode `build` once their
estimated count grew by `seal_rows` times its columns (1,024 changesets,
whatever the family's width). Pause points for crash tests gain
`seal-swapped` and `feature-recorded`, and `changeset-v2:` names for the
second family.

### 7. Per-family view, manifest and drop (point 5)
`Repo::segments()` holds one view per family over shared block and data
caches (one budget). The view refreshes every family's manifest together,
at most every 60 s per process: in the read's own `get_derived_kinds`
call, in one batched read for a batched miss, or, for a single-row read's
miss, one single-row read per family (a single read never becomes a
batched one, whose failure behaviour differs). `manifest_key(family)`,
`read_manifest(repo, family)`, `drop_columns(repo, lease, family, cols)`;
`repair drop` clears each family's columns of the dropped kinds. With a
family's manifest damaged, replacing it with an empty one stops every
kind of the family answering, so `repair drop` refuses unless all of the
family's kinds are named (their rows go too, and derive again). Every
family's manifest is applied whatever another's error (a transient read of
one family's index blob does not freeze the other), and the first error is
returned.

### 8. Listings and integrity (point 6)
The moved kinds are per-changeset kinds: `verify` and `repair rederive`
reach them by walking changesets and read through the view, so sealed
values are checked like rows; `count_derived` and `estimate_derived` count
rows and sealed keys together (the view's sealed-key enumerator), so
`upgrade-index` does not see a retired store as missing rows. No code lists
the moved kinds' rows (audited: the sweep, reachability and unreachable-GC
listings name other kinds). `verify` checks every family's manifest and
segments and notes a segment with columns a newer binary wrote. The
row-listing scan of content kinds (`integrity/rows.rs`) is Stage B's
concern.

### 9. Metrics
`leep_derived_segments_live`, `leep_derived_segment_bytes`,
`leep_derived_segment_unsealed_rows` and `leep_derived_segment_index_bytes`
are labelled by `family`; reads, errors, mismatches and retired rows gain a
`family` label. Sums are unaffected.

## Stage B (planned here, decided in [ADR 0106](0106-search-rows-as-derived-segments.md))
A hash-keyed family `content-v1` for `blob-trigrams-v2`,
`tree-trigrams-v2` and their keyframes, in the same container: ordinals by
a head-tree walk, set equality (`same_blob_set_with`, `same_filter`)
instead of bytes, a 1 MiB data cap and two tiers, and retention seals only
live rows (the maintainer accepted the amendment to ADR 0102: a sealed row
that becomes superseded waits for the segment's rewrite, at about a third
dead). Its listing-based verify must see sealed keys in the same change.

## Cost
- **Backfill and retirement**, measured on Postgres 16 with
  `crates/leep-server/tests/derived_segments_backfill.rs`: a chain of
  stored changesets with their `skiplist` (`changeset-v1`) and
  `generation` (`changeset-v2`) rows, default limits, both families:

  | Changesets | Build passes | First pass | Each after | All | Retire passes | First | Each after | All |
  |---|---|---|---|---|---|---|---|---|
  | 200,000 | 4 | 5.9 s | 0.05 to 0.9 s | 7.7 s | 7 | 8.7 s | 0.3 to 1.7 s | 16.5 s |
  | 500,000 | 8 | 15.4 s | 0.6 to 1.1 s | 22.1 s | 16 | 11.8 s | 0.4 to 2.7 s | 40.0 s |

  ADR 0104 measured 6.2 s for the first pass and 10.1 s in all at 500,000
  changesets of one family. The first pass of each mode lists every row of
  both families (here one kind each; a real store lists six kinds) and
  reads every changeset's generation and whether it is stored (one more
  statement per 1,000 keys); later passes carry their candidates. Each
  pass seals four pages a family and deletes at most 65,536 rows over both
  families, then gives the lease up; every pass stayed far under the two
  minutes `repair` waits.
- **Listing in mode `build`**: each family lists its kinds once its rows
  grew by 1,024 changesets' worth (`seal_rows` times its columns) or 10
  minutes passed: on a Linux-sized store about 2.4 million keys of
  `changeset-v1` and 4.8 million of `changeset-v2` at most every 10
  minutes. In mode `retire` only unsealed rows remain to list.
- **A seal or merge** asks whether each key's changeset is stored (batched,
  one statement per 1,000 keys); a retirement asks for two
  `blob-condemned-v1` rows per holding segment and again per delete
  statement (1,000 rows).

## Consequences
- Measured on corpus B (reconstructed: 2,231 changesets, Git 6.35 MB after
  `git repack -adf`), import, a server with default flags until nothing
  changes for 30 s, stop, `gc --grace 0 --fence 0s --unsafe-no-fence`, then
  a server in `retire` with its waits cut until the rows are gone, `gc`
  again and `VACUUM FULL` (Postgres) or `VACUUM` (SQLite). Release builds of
  f3588f2 ("before": ADR 0104; segment formats as at eb3508d) and this
  change. Database above an empty one plus blob store:

  | Backend | Before (x Git) | After (x Git) | Change |
  |---|---|---|---|
  | Postgres | 15.85 MB (8.94 db + 6.92 blobs; 2.50x) | 14.33 MB (7.26 + 7.07; 2.26x) | **-1.53 MB** |
  | SQLite | 15.87 MB (8.95 + 6.92; 2.50x) | 14.69 MB (7.62 + 7.07; 2.31x) | **-1.17 MB** (1,173,213 bytes) |

  Against ADR 0104's Git figure of 6.58 MB that is 2.41x to 2.18x on
  Postgres. The `changeset-v2` segment is 158,596 bytes (77,007 of index,
  81,589 of data; 71 bytes a changeset); the blob store grew by 0.15 MB.
  The design's estimate for this step was 14.1 MB (2.14x); it assumed
  about 150 bytes of Postgres overhead a row, and the four kinds' rows take
  about 1.6 MB, not 2.0. The ≤2x target needs Stage B (search: about -1.25 MB on the
  design's estimate, its rows confirmed by Stage 0 above).
- The storage size guard measures each family apart: `changeset-v1` 57.0 to
  59.2 bytes a changeset (unchanged), `changeset-v2` 73.5 (six runs), 0.316
  of the 27.9 KB of rows; new budgets 92 and 0.40.
- `budget_tests`: a create on a sealed and retired store makes the same 13
  metadata calls; blob calls go from 8 to 10 (the second family's index and
  data blob, once per process).
- A single-row read's miss now reads two manifests (one statement per
  family) at most once a minute per process.
- Each server keeps about 40 bytes a changeset more of index (B 90 KB).
- `repair drop --kind generation` (and the other three) clears that
  family's column first.
- Deviations from the design: generations and last merges keep their eight
  bytes (measured better than varints under zstd); the view refreshes all
  families together rather than per family; a single-row miss refreshes
  with single-row reads; `generation` is read through the view only for
  ordering, while whether a changeset is stored is asked of the changesets
  (the pass's own segments decide liveness and deletes, as before); one
  segment with an unknown column is not merged at all rather than merged
  around; `retire` passes a new family's door only when the family is
  named or its feature already recorded.
- Changed after review, before release: merges shed swept changesets again
  (an earlier draft took a sealed generation as proof a changeset was
  stored); the retire-families gate; pass bounds shared by both families,
  the first rotating; every family applied whatever another's error; the
  `log-filter-v1` column read without parsing; unknown columns' blocks not
  parsed; the listing trigger scaled by a family's columns; condemned
  segments' rows not retired; `repair drop` with a damaged manifest needs
  every kind of the family.
- Changed after a second review, before release: a `retire` server holding
  a family back replaces that family's damaged manifest like `build` (it
  deletes none of its rows); blob condemnations are read again before each
  delete, so after the family's feature is recorded, and their warning is
  logged once per segment; a held-back family asks for its feature at most
  once a minute; the crash test no longer expects a row deleted when
  killed at either family's `feature-recorded`, since the first family
  rotates; the server's family list cannot silently fall back to empty.
