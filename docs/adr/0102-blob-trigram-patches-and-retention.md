# 0102. Blob trigram rows as patches on keyframes, and their retention
Status: Accepted
Date: 2026-11-14

Amends [0014](0014-code-search.md) (the blob rows) and
[0058](0058-one-indexer-per-kind.md) (a bounded search stores the blob rows it
computes). Follows [0094](0094-patched-directory-filters.md), whose rule it
applies to blob rows.

## Context
After ADR 0100 and 0101 a default server's blob store settles near Git's
size, and the database is what grows: 27.5 KB a land on Postgres over 400
CLI lands of corpus C (Leep's own history plus 40 edits each of five files
at depths 1 to 5 and 200 three-file edits), of which `blob-trigrams-v1` is
13.5 KB (49%). The maintainer's target is at most twice Git with indexes
on.

A `blob-trigrams-v1` row is the whole trigram set of one blob: 5.7 KB for
the average source file, 2 to 21 KB here. A land writes one for every file
it changes, though the set of the new version differs from the old one's by
a few dozen codes (15 bytes as a patch, measured on C), and the old row is
never deleted though no head holds the old version and searches rarely look
at it.

Two changes, from the storage plan's items M1 and Q5:

- store a new version's set as a patch on the previous version's, as ADR
  0094 does for directory filters;
- delete the rows of versions no head holds, after a grace period ("keep N
  days, then on demand"), since a row is a pure function of its blob.

## Decision

### 1. `blob-trigrams-v2`: patches on keyframes
A new kind **`blob-trigrams-v2`**, keyed by blob hash like v1, whose row is
one of three tags (bytes in FORMATS.md):

- **`0x00`** "not indexed" and **`0x01`** the whole set: the v1 encodings,
  byte for byte;
- **`0x02` patch**: `{keyframe blob, added codes, removed codes}`, the set
  of the keyframe (read from the new kind **`blob-trigrams-keyframe-v2`**,
  key = that blob) with the codes added and removed. A keyframe is always
  whole, so a probe costs at most two rows.

**The predecessor** of a file is the file at the same path in its
directory's predecessor: the directory ADR 0094 diffs a directory's filter
against (the same path in the first parent's tree, or in the nearest of the
next 8 first-parent ancestors whose tree has a filter). The derivation
already reads that directory's node with the level's nodes; the file
predecessors' rows are read in the same batched read as the files' own, so
a land's derivation makes no more round trips than before, and a keyframe
written by one land is in the filter cache for the next. Then, for an
indexed blob:

- predecessor's row a patch on keyframe `K`: a patch on `K` when it is
  shorter than the whole row and `patch² <= 128 × whole`;
- predecessor's row whole and indexed: that row becomes the predecessor's
  keyframe (the same set in its one encoding, so writing it is idempotent)
  and the blob a patch on it, under the same rule;
- otherwise, or when the rule refuses: a new keyframe (the whole row as its
  keyframe row and the empty patch on itself, 34 bytes) for a whole row of
  128 bytes or more, the whole row for a shorter one;
- no predecessor (a new path, a first revision, a search or a bounded
  query): the whole row.

The rule is ADR 0037's: a patch grows by `d` bytes a version, and the bytes
per version are least for a constant of `2d`. Replayed over the 3,858 file
versions of Leep's own history (whole commits, `d` in the hundreds) the best
constant is 256 (2.84 times smaller than whole rows; 128 gives 2.69); over
corpus C's 849 versions of small edits (`d` about 15) it is 32 (9.25 times;
128 gives 7.94). 128 is within 14% of the best on both, and a small file
(under 128 bytes whole) is never a keyframe, where the keyframe and the empty
patch would cost more than the row.

**Keyframes have a kind of their own,** for ADR 0094's reason: a
`blob-trigrams-v2` row depends on the predecessor's row when it was
derived, so two derivations racing on one blob from different predecessors
(the warmer and a search on another server) may write different, equally
correct rows, and the last write wins. A keyframe row holds only its blob's
whole set in its one encoding, so every write of it writes the same bytes;
it is written before the patch on it in the same ordered write, so a
derivation stopped anywhere leaves no patch without its keyframe; and it is
never deleted except by `repair drop --kind blob-trigrams-v2`. A patch whose
keyframe is not stored (a page-ordered drop racing a derivation) counts as a
row not stored: it is derived again, whole, and overwritten. A keyframe
that is there but not a whole set, or that the patch does not fit, is
corrupt, for `verify` to report.

**Reading.** A search reads a directory's blob rows in one batched read, as
before, and the keyframes of its patches through the search filter cache
(`--derive-cache-bytes`) in one more read when any is not cached; keyframes
the warmer writes are cached as they are written. The patch is applied at
the probed codes only, a merge of two short sorted lists against the
keyframe's codes. Answers are those of a search without the index, as
before.

**Checking.** `verify` and `repair` compare the set a row stands for with
the set recomputed from the blob, not the bytes; a keyframe must equal the
whole row byte for byte, and keyframes are checked first, so the patches on
a repaired keyframe are checked against it. A row that differs is rewritten
whole. A keyframe of a blob that is not indexed cannot be right and is
reported, not rewritten; the patches on it then fail their check and are
rewritten whole.

**Upgrade, online.** `blob-trigrams-v1` is retired but still read: a new
server reads a blob's v1 row wherever it has no v2 row, so the rows an old
server wrote keep answering and nothing is derived again. The fallback read
is skipped while the store holds no v1 row, which each process counts
(whether any exist is exact) at most every 10 minutes, so a store that has
none pays nothing. New servers write v2 rows only; old servers neither read
nor write them and derive v1 rows for the files of a head they find
without one, a few per land. Both versions keep sharing the search group's
index lease (`tree-trigrams-v2`), which is safe because the directory
filters they write are the same: whichever holds it warms heads in its own
blob format, and the other side reads or derives what it lacks. Going back
to an older binary works the same way. `repair drop --kind
blob-trigrams-v1` (or `upgrade-index --drop-retired
--old-servers-stopped`) deletes the old rows once no old server is left.

### 2. Retention of blob rows
A task in `leep-server` (`search_retention`, `--search-retention`, default
`14d`, `off` to disable; `--search-retention-interval`, default `1h`, the
first pass 10 to 15 minutes after start) runs
`leep_derive::search::retention` under a store-wide lease (`maintenance` /
`search-retention`, held and renewed like the GC lease), so one server at a
time runs it, and in the index lane (ADR 0073), so it uses the index
connections and never the land path's. A pass:

1. **Finds the live blobs.** The heads are every ref's head now, and the old
   and new head of every ref log move within the window (newest first, read
   five minutes past the window's start for the servers' clock skew): every head
   a ref held at any time within it, deleted refs included. Their trees are
   walked iteratively into a set of live blobs, one batched read per level,
   each distinct node once; heads whose changeset is gone are skipped.
2. **Deletes the others.** The keys of `blob-trigrams-v2`, then of the
   retired `blob-trigrams-v1`, are listed 500 at a time with the new
   `MetaStore::list_derived_keys` (the primary key alone; the Postgres and
   SQLite backends never read a value), and the keys not live are deleted
   with one `delete_derived_keys` per batch. The pass rests three times as
   long as each batch took.
3. **Rereads the refs around each delete.** Before it, the blobs of any
   head not yet walked join the live set and leave the batch. After it, a
   deleted row whose blob a head that appeared meanwhile holds is derived
   again at once.

Why it is safe beside lands, the warmer and searches, without coordinating
with them:

- A row a head holds is never left deleted by a pass: a head that existed
  before the read that precedes a delete is in the live set; one that
  appeared between that read and the delete is seen by the read after it,
  which derives the row again; one that appeared after that read was
  warmed after the delete took effect, and a derivation computes every
  missing row of the directories it derives.
- What can be left missing is what "keep N days, then on demand" means: a
  head that takes over a whole directory unchanged from a revision older
  than the window (a revert to old content, a branch from an old commit)
  reuses that directory's stored filter, so the warmer derives nothing below
  it; the rows retention removed there are derived by the first search that
  probes them, as for any revision older than the window. ADR 0094's
  invariant, "a stored filter has its whole subtree stored", which the
  bounded checks and `stale-ok` rely on to judge cost, therefore holds for
  every tree a head held within the window, and for older trees only once
  searched. The answers never depend on it.
- So that such searches converge in `lease` mode, a bounded query now
  stores the blob rows it computes for the files it probes (with
  `put_derived_if_absent`, a pure function of the blob, safe to write from
  any server), not only in its own memory: a search over its budget stores
  what it computed, and the next attempt goes further. It still stores no
  directory filter and no keyframe. This amends ADR 0058's "a bounded query
  stores nothing".
- Keyframes are never deleted by retention: a live patch may stand on the
  keyframe of a blob no head holds. They are one per few dozen versions of a
  file (section 1); retiring the unreferenced ones needs each live patch's
  base, which a pass would have to read, and is left for later. Directory
  filters are not retired either (ADR 0094 left that open too).
- Searches of revisions inside the window are unaffected. A search of an
  older revision fetches each blob its query probes and lacks a row for, as
  after an import, once.

**14 days** keeps the revisions of the last two weekly cycles (open reviews,
the bases agents branch from, last week's release) as fast to search as
heads, while bounding the rows kept to two weeks of lands: at 1,000
one-file lands a day, about 14,000 patches of a few hundred bytes.

**Postgres frees space only after vacuum.** A deleted row is a dead tuple
until autovacuum marks its space reusable; later rows fill it, so `derived`
stops growing by what retention deletes rather than shrinking, and `VACUUM
FULL derived` or `pg_repack` returns it to the filesystem. Measured below.

## Consequences
- **Per land** (PERFORMANCE.md, "Blob trigram patches and retention";
  Postgres 16, 400 CLI lands of corpus C, one a second, every head warmed,
  against 31d8dff measured the same day):

  | Per land | Before | After |
  |---|---|---|
  | Database | 27.5 KB | 13.7 KB |
  | ... `derived` table | 23.8 KB | 9.9 KB |
  | ... blob rows and keyframes (key and value bytes) | 13.5 KB | 1.1 KB |
  | ... of which 200 one-file lands | 4.9 KB | 0.40 KB |
  | ... of which 200 three-file lands | 22.1 KB | 1.8 KB |

  Corpora A and B at rest are unchanged, as the storage plan expected: an
  import indexes only its head, all whole rows.
- With retention, a store's blob rows are those of the window's heads plus
  the patches of its lands; on a store older than the window the first pass
  deletes most of the kind. On corpus C, a pass after the window kept the
  head's 802 rows and deleted the other 612 (875 bytes a land) in 76 ms;
  on Postgres that left 612 dead tuples and the table's size unchanged
  until `VACUUM` made the space reusable, and `VACUUM FULL derived` gave
  back 434 KB of 9.87 MB. Patches leave retention little to reclaim per
  land; it bounds what a long-lived store keeps to the window's rows.
- Search latency (20 queries, two runs, a fresh server): unchanged on a hit
  (warm p50 9.3-10.0 ms against 9.4-10.2, round 1 p99 84-90 against
  88-102). A miss fetches each probed blob without a row: with every blob
  row of the head deleted, the first round's p99 rose to 287 ms while it
  stored 583 rows, and later rounds were warm. The ratio bench against
  31d8dff failed none of its 23 checks, one inconclusive (swarm sync p99,
  whose A runs differed 16x).
- A row of `blob-trigrams-v2` no longer has one value per blob: the same
  blob reached from different predecessors may be stored as different
  patches of the same set. Nothing depends on it, as with ADR 0094.
- A probe of a patch reads its keyframe, usually from the cache: one more
  batched read per directory on a cold process.
- Not done: retiring keyframes nothing live patches on, retiring the
  directory filters of trees no head holds, and an encoding or rule change,
  which needs `blob-trigrams-v3`.
