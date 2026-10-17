# 0037. Patched path-history rows
Status: Accepted
Date: 2026-10-17

## Context
After ADR 0027 the path-history index is still Leep's largest storage cost.
On all of git/git (82,500 commits) `history-node-v2` holds 410,685 rows and
796 MB of value bytes, of a 1.0 GB database, against 190 MB of packed blobs
and 328 MB for git's whole repository.

On the first 3,795 commits of git/git (11,725 rows, 13.0 MB) the bytes are:

| Part | Bytes | Share |
|---|---|---|
| Changeset ids in `changed` (32 bytes each) | 7.12 MB | 55% |
| Entry names | 3.17 MB | 24% |
| Chunk addresses in chunk lists | 1.43 MB | 11% |
| First names in chunk lists | 0.62 MB | 5% |
| Tags, counts and lengths | 0.45 MB | 3% |
| Child node addresses | 0.23 MB | 2% |

By row type: 6,501 chunks averaging 1.5 KB, 4,160 chunk lists averaging
496 bytes and 1,064 whole nodes averaging 1.2 KB. The 222,540 changeset ids
stored are only 3,792 distinct ones, each repeated about 59 times. The
repetition is between rows, not within them. Each changeset rewrites the chunk
that holds each changed entry, and the new chunk repeats the roughly 31
entries that did not change, ids and all. So the waste is the many versions of
one chunk, which differ in an entry or two.

Measured and rejected:

- **zstd per row** (level 3 or 19): 1.40x to 1.42x. A row is mostly hashes,
  and the repetition is between rows, which per-row compression cannot see.
  Postgres already compresses rows over about 2 KB with pglz: 1,211 of the
  11,725 rows, which shrinks 13.0 MB to 11.2 MB (1.16x).
- **zstd with a trained dictionary**: a 110 KB dictionary trained on all rows
  gives 5.2x, but most of that is the slice's own changeset ids sitting in the
  dictionary. Trained on the older half and applied to the newer half it gives
  2.0x (1.8x with 16 KB), and the gap widens as history moves past the
  dictionary. Keeping it useful would need a stored dictionary row with
  versioned rollover, shared by servers that may be deriving at the same
  moment, for about 2x.
- **Changeset ids as small integers**: no deterministic, collision-free
  small number exists for a changeset without a global counter. Generation
  numbers are not unique. Two servers deriving concurrent branches against one
  database would need coordination, and the two derivations would no longer be
  byte-identical. An id prefix would need a lookup per entry (a round trip per
  step of a path walk), and a crafted 64-bit collision can be ground out.
- **An id table per row plus front-coded names**: 1.33x (1.24x for the table
  alone): ids repeat across rows, not within them.
- **Smaller chunks or a deeper chunk tree**: halving the chunk halves the
  chunk written per change but doubles every list; with two levels an edit
  still writes about 1 KB on a 500-entry root, an estimated 1.8x.

## Decision
Store each changed whole node, chunk or chunk list as a **patch** on a
**keyframe**: a row holding only what differs from an earlier, whole version
of itself.

**Rows.** `history-node-v3` rows are a postcard `StoredHistoryNode` with two
new variants beside `Flat` (variant 0) and `Split` (1):

- `Patch { base, over }` (2): the `Flat` row `base`, with the entries of
  `over` added or replaced;
- `SplitPatch { base, refs }` (3): the `Split` row `base`, with refs set or
  removed by first name. Each set ref also carries the first name of the chunk
  after it.

A patch's base is always a keyframe of its own shape, never another patch, so
a read costs at most two rows, and readers reject anything else as corrupt.
Rows stay keyed by `Hash::of_object("history-node-v3", bytes)`, so they are
content addressed and checked on read exactly as before. Bytes are in
FORMATS.md.

**Writing.** `put_history_node(node, base)` already knew the node's
predecessor, the first parent's version. For the whole node, for each chunk
(its predecessor is the base chunk whose range holds its first name) and for
the chunk list, it takes the predecessor's keyframe `K` and writes
`Patch(K, entries that differ from K)` when two conditions hold:

- every name of `K` is still present (derivation never removes a name);
- `patch² <= 128 × whole`, in bytes.

Otherwise it writes a new keyframe. A patch holds every change since its
keyframe, so it grows by about `d` bytes per version, and a run ending at a
patch of `L` bytes costs about `L/2 + d·T/L` bytes per version, for a keyframe
of `T` bytes. That is least at `L = sqrt(2·d·T)`, and 128 is `2·d` for
`d ≈ 64`. The rule allows about a quarter of a 2 KB chunk and half of a
500-byte list, which a fixed fraction would keyframe after a change or two.
On the slice it stores 7% less than a fixed quarter, and is within 3% of the
best constant tried. Unchanged chunks keep their address and are shared with
the predecessor, as in ADR 0027.

**Determinism.** A row is a function of the logical content and the stored
predecessor only, and a changeset's predecessor is fixed by its parents. Two
servers deriving the same history, in any order, therefore write
byte-identical rows, and `verify` still recomputes and compares them. One
property of ADR 0027 is given up: a logical node no longer has a single
address, because the same content reached from different predecessors may be
stored as different patches. Nothing depends on it for correctness. Merges
diff the parents' history trees and skip subtrees by address; a subtree
merely rebuilt identically on both sides is now compared entry by entry
instead.

**Reading.** The row cache holds rows as stored. Lookups apply patches lazily:

- A name found in a chunk's `over` is answered without reading the keyframe.
  The patch's own entries are first checked to lie in the chunk's range, and
  when the keyframe is read the whole chunk is checked.
- A name in the range of a chunk the list patch sets is routed without reading
  the keyframe list. That is what the stored successor name is for, and
  `resolve` rejects a successor that does not match the patched list.

Recently changed paths, the common question, therefore cost what they cost
under v2. Full reads join keyframe and patch. The rows of one node are written
in a single `put_derived_many` call, chunks before the list, so a failed batch
leaves no dangling reference.

**Only the labels are versioned.** `history-tree-v2` and `history-node-v2`
become `history-tree-v3` and `history-node-v3`; types, modules and functions
change in place, as in ADR 0027.

**Upgrade.** v2 rows become garbage to new servers, as v1 rows did. The warmer
(ADR 0028) derives the v3 index after the new server starts, or
`leep-server repair rederive --kind history-tree-v3` derives it ahead of
traffic. Once no old server is left,
`leep-server repair drop --kind history-tree-v2` (with `--dry-run` first)
removes `history-tree-v2` and `history-node-v2`. `drop` knows them as retired
kinds and refuses `history-node-v2` on its own; `rederive` and `verify` do not
accept them. Old and new servers can share a database meanwhile, each
answering from its own index.

## Consequences
- On the first 3,795 commits of git/git (local Postgres 16, release builds,
  index derived from scratch):

  | | v2 | v3 |
  |---|---|---|
  | Node rows | 11,725 | 11,733 |
  | Node value bytes | 13.0 MB | 3.70 MB (3.5x smaller) |
  | Postgres size of the node rows (heap, TOAST, key index) | 14.5 MB | 5.8 MB |
  | Derive from scratch (three runs) | 18.5 to 19.4 s | 15.7 to 16.0 s |

  v3 holds 942 keyframe nodes and chunks (1.05 MB), 1,024 keyframe lists
  (0.42 MB), 6,631 patches averaging 255 bytes and 3,136 list patches
  averaging 173 bytes. Derivation is faster because each node's rows go in one
  write.
- API answers are byte-identical: `last-change`, `log path=` and `blame`
  bodies for 50 paths from a v2 and a v3 server on one database agreed.
- Warm queries, path logs and blame are unchanged. A single lookup on a
  freshly started server can read one more row (a keyframe list, when the name
  lies in a chunk the newest list patch does not set): cold `last-change` is
  0 to 0.6 ms slower out of 3.5 to 5.5 ms.
- On the full history, whose rows are larger (1.9 KB on average against
  1.1 KB here), the saving should be at least the same factor, about 796 MB
  down to 230 MB or less. It has not been measured.
- A size regression test asserts that a synthetic wide, deep history stores
  at least 3x fewer bytes than whole rows would (4.5x when written); golden
  tests fix the bytes and the patch rule.
- Changing the patch rule, its budget or the row encoding changes rows and
  needs `history-tree-v4` and `history-node-v4`.
