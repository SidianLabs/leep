# 0094. Directory filters as patches on keyframes
Status: Accepted
Date: 2026-11-05

Amends [0014](0014-code-search.md).

## Context
ADR 0014 stores one `tree-trigrams-v1` row per directory: a bloom filter of
2^19 bits over every trigram below it, dense (65,537 bytes) or as a sorted
list of set positions when that is shorter. Rows are keyed by the tree
node, so a land writes a new row for every directory on each changed path,
and the warmer (ADR 0028) derives the search rows of every head it sees.

Measured on Postgres 16 with Leep's own history imported (221 commits, 738
files, 10.6 MB at the head), one land a second through the CLI, every head
warmed:

| Lands | Database growth per land | `tree-trigrams-v1` per land | rows per land |
|---|---|---|---|
| 200 single-file lands at depths 1 to 5 | 198 KB | 182 KB | 3.0 |
| 200 three-file lands | 459 KB | 412 KB | 7.0 |

Everything that is not search adds about 1.5 KB a land. The filters
upstream of a change are the cost: the root, `crates/`, `docs/` and the
busiest crates are dense (`crates/` has 114,618 of its 2^19 bits set, `docs/`
77,020; the root, their union, more than a quarter), and the sparse rows below
them are 30 to 53 KB. Random bit sets do not compress (Postgres stores them as they
are; zlib saves 20%), and the rows are never deleted. At 1,000 lands a day
that is 88 to 168 GB a year.

A land changes a directory's filter by the bits its edited files add or
drop: tens to a few hundred positions out of 2^19. ADR 0037 stored
path-history nodes the same way and kept 3.5x fewer bytes.

## Decision
A new kind **`tree-trigrams-v2`**, keyed by tree node like v1, whose row
for a directory is one of four tags (bytes in FORMATS.md):

- **`0x01` dense** and **`0x02` sparse**: the whole filter, as in v1;
- **`0x03` saturated**: the filter has more than 2^17 bits set (a quarter).
  One probe of such a filter passes at least 1.6% of the time and a query's
  several probes far more, so it prunes little, and a search treats it as
  admitting everything and descends. A parent has every bit of its children,
  so a saturated child makes its parent saturated, which derivation decides
  without reading the other children;
- **`0x04` patch**: `{keyframe, added positions, removed positions}`, the
  filter of the tree `keyframe` (read from the new kind
  **`tree-trigrams-keyframe-v2`**, key = that tree) with the positions
  added and removed. A keyframe is always whole, never a patch, so a read
  costs at most two rows.

**Keyframes have a kind of their own.** A `tree-trigrams-v2` row is a
function of the tree and of the row its predecessor had when it was
derived, so two derivations racing on the same tree from different
predecessors (the warmer and a search on another server, say) may write
different, equally correct rows, and `put_derived_many` keeps the last.
A patch must never find its keyframe rewritten as a patch. A keyframe row
holds only its tree's whole filter in its one encoding, so every write of
it writes the same bytes, and it is never deleted except by `repair drop`
of the whole kind. A keyframe is written before the patch on it, in the
same ordered write, so a derivation stopped at any point leaves no patch
without its keyframe.

**Writing.** A directory's predecessor is the directory at the same path in
the first parent's tree. When the first parent's tree has no row (the
warmer skips heads when lands outrun it), the nearest of the next 8
first-parent ancestors whose tree has one stands in. The predecessors'
nodes are read with each level's nodes, and their rows with each level's
rows, so a land's derivation makes the same metadata calls as before (11
for a file three directories deep; a budget test holds it). Then, for a
directory whose filter is not saturated:

- predecessor's row a patch on keyframe `K`: write a patch on `K` when
  `patch² <= 512 × whole` in bytes, `whole` being the directory's own whole
  encoding; otherwise start a new keyframe;
- predecessor's row whole (derived without a predecessor): start a new
  keyframe;
- no predecessor, or a saturated one: write the whole filter.

Starting a keyframe writes the directory's whole filter as its
`tree-trigrams-keyframe-v2` row and an empty patch on itself (34 bytes) as
its `tree-trigrams-v2` row. The rule is ADR 0037's: a patch holds every
change since its keyframe, so it grows by `d` bytes per version and a run of
versions costs about `L/2 + d·T/L` bytes each for a final patch of `L` bytes
on a keyframe of `T`, least at `L = sqrt(2·d·T)`. A land adds 100 to 300
new positions of about two bytes each to each directory on its path, so
`d` is about 256 and the constant is `2d = 512`: a patch may grow to about
5.8 KB on a dense 64 KiB filter before a new keyframe.

A first revision, a search on a path, and a bounded query (ADR 0058) derive
without a predecessor and write whole rows, as v1 did. A search that must
derive a head's filters uses the first parent's tree when this process
already holds it, which costs no read.

**Reading.** A search reads a directory's row as before and, for a patch,
its keyframe through the same filter cache (`--derive-cache-bytes`): the keyframes
of the busy directories near the root are few and stay cached across the
heads that patch on them. The patch is applied at the positions the query
probes only, a merge of two short sorted lists. A saturated row admits the
query. Answers are those of a search without the index, as before.

**Checking.** `verify` and `repair` compare the filter a row stands for with
the one recomputed from the tree, not the bytes: a patch is applied to its
keyframe, and a row that cannot be resolved (keyframe missing or not whole,
a patch that does not fit) does not match. Repair rewrites a wrong row
whole. Keyframes are checked first, so the patches on a repaired keyframe
are checked against it.

**Upgrade.** `tree-trigrams-v1` is retired, like `history-node-v2` (ADR
0037): current servers neither read nor write it, and `repair drop --kind
tree-trigrams-v1` (or `upgrade-index --drop-retired --old-servers-stopped`)
deletes its rows once no old server is left. The index lease of the search
group is named by its primary kind, so it becomes `tree-trigrams-v2`: an
old and a new server never share it, and each derives its own rows during a
rolling upgrade. `--index-kinds search` names the group as before. New
servers derive v2 rows for each head the warmer sees; until then a search
of an older head derives them as it would after an import.

## Consequences
The same harness, every head warmed (PERFORMANCE.md, "Directory filters as
patches"):

| Lands | Database growth per land, v1 → v2 | Filter bytes per land (v2: rows and keyframes) |
|---|---|---|
| 40 × `README.md` (depth 1) | 75.2 KB → 7.4 KB | 65,569 → 33 |
| 40 × `docs/ARCHITECTURE.md` (2) | 148.5 KB → 16.6 KB | 131,138 → 1,940 |
| 40 × `docs/adr/0014-code-search.md` (3) | 201.7 KB → 10.0 KB | 184,158 → 2,362 |
| 40 × `crates/leep-cli/src/main.rs` (4) | 248.6 KB → 10.4 KB | 234,243 → 5,980 |
| 40 × `crates/leep-derive/src/search/trigram.rs` (5) | 318.5 KB → 19.5 KB | 292,897 → 6,855 |
| **200 single-file lands, depths 1 to 5** | **198.5 KB → 12.8 KB** (15.5x) | **181,601 → 3,434** (53x) |
| **200 three-file lands** | **459.0 KB → 44.3 KB** (10.4x) | **411,628 → 13,299** (31x) |

  The root and `crates/` are saturated (one byte a version), about a dozen
  keyframes (564 KB) were written over 400 lands, the first version of each
  busy directory after the import among them, and the 1,600 patches average
  1.7 KB. What is left per land is mostly the new blob's `blob-trigrams-v1`
  row (2 to 21 KB), the other kinds (1.5 KB) and Postgres's own overhead.
- Searches on the final head answered the same 20 queries with the same
  matches and the same pruning, apart from a token found nowhere: v1 ruled
  out the whole tree at the root, v2 at its eight children. Warm (cached)
  latency was p50 18.2 → 18.9 ms, p99 43.4 → 41.1 ms; on a just restarted
  server, p50 40.1 → 41.8 ms, p99 94.0 → 77.2 ms, and Postgres sent 2.50 MB
  → 2.53 MB per search, most of it the blob rows of the files the filters
  could not rule out. A cold search reads about the same: the filters of one
  revision are no smaller to read, only to keep.

- Smaller stored filters also mean fewer bytes a cold search moves from the
  store; warm searches read filters from the cache either way.
- A row of `tree-trigrams-v2` no longer has one value per tree: the same
  directory reached from different predecessors may be stored as different
  patches of the same filter. Nothing depends on it, as with ADR 0037.
- A patch read costs a second row when its keyframe is not cached: one more
  batched read per directory opened on a cold process.
- On a synthetic chain of 80 small lands into four busy directories under a
  dense root, v2 stores 24x fewer filter bytes than whole rows would; a test
  holds it at 5x or more.
- Lands that outrun the warmer still share its work as before; the warmed
  head now patches on the nearest indexed ancestor instead of writing whole
  rows.
- Not done: deleting the rows of trees that are no longer heads, and
  sharding the filters of very wide directories. A rule or encoding change
  needs `tree-trigrams-v3`.
