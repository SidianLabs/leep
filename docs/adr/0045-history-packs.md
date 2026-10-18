# 0045. Path-history packs
Status: Accepted
Date: 2026-10-18

## Context
The path-history index (ADR 0013, 0027, 0037) is complete on the full Linux
history (1,484,762 changesets, Postgres 16), and it is by far the largest
thing Leep stores: `history-node-v3` holds 25,426,370 rows and 24 GB of
values, about 17 rows and 16 KB per changeset, in a 27 GB database. All of
git's repository is 3.8 GB, and Leep's packed blobs are 3.2 GB. On git/git
(82,500 commits) the same index was 1.9 KB per changeset. Queries on it are
fast (last-change 2 to 55 ms, a 50-entry path log 0.2 to 0.4 s) and must stay
so.

`history_profile --breakdown-v3` attributes every stored row to the
changeset whose derivation wrote it and counts its bytes by row type,
directory width, merge or not, and field. Two histories were measured: the
first 21,109 commits of Linux (to v2.6.16, 1,522 merges) and the synthetic
late-Linux history of `history_profile --synth 20000 --old-base 15000`
(19,777 changesets, 1,930 merges, directories of 2,000 to 5,000 files).

| | Linux to v2.6.16 | Synthetic |
|---|---|---|
| Node rows, value bytes | 290,330, 209.8 MB | 540,706, 300.8 MB |
| Per changeset | 13.8 rows, 9.9 KB | 27.3 rows, 15.2 KB |
| Held by merges | 83.5% (115 KB and 121 rows per merge) | 80.4% (125 KB, 187 rows) |
| Other changesets | 5.4 rows, 1.8 KB | 10.1 rows, 3.3 KB |
| Top 1% of changesets | 70% of bytes | 46% |
| Changeset ids in `changed` sets | 72.0% | 61.9% |
| Entry names | 12.5% | 9.5% |
| Child node addresses | 7.6% | 7.3% |
| Chunk addresses and first names in chunk lists | 2.2% | 13.8% |
| Patch base addresses | 2.9% | 4.6% |
| Tags, counts, lengths | 2.8% | 2.9% |
| Whole-node keyframes / chunk keyframes | 47.7% / 28.6% | 19.4% / 30.3% |
| Patches of whole nodes / of chunks | 15.5% / 5.8% | 17.6% / 18.3% |
| Chunk-list keyframes / patches | 1.4% / 1.1% | 11.3% / 3.0% |
| Rows of directories over 500 entries | 3% of rows, 5% of bytes | 30% of rows, 35% of bytes |

The synthetic history's 15.2 KB per changeset matches Linux's 16 KB, so its
shape stands in for late Linux; the slice, from 2005-2006, has narrower
directories and smaller merges.

**Merges are most of the bytes, and their rows repeat a few ids.** A merge
writes a row for every directory, chunk and chunk list its changes reach
(124 directories per merge on the slice): every path brought in from the
second parent changes, with `changed = {merge}`, and so do the paths whose
newest change becomes a merge-back of the branch. Many of those rows are
keyframes, because so many entries change. A merge's rows hold 2,748 changeset
ids on average (2,570 on the synthetic history), but only 47 distinct ones
(63): the same 32-byte ids, row after row. ADR 0037 measured an id table *per
row* at 1.24x and rejected it; per row it is still only 1.4x to 1.9x here,
because the repetition is between the rows of one changeset.

**Rows are many and small.** Each row also costs Postgres about 160 bytes of
tuple header, line pointer, key and index entry beside its value, so 17 rows
per changeset are about 2.7 KB more per changeset on Linux.

## Decision
Store all the nodes one changeset's derivation writes in one **pack**, under
new kinds `history-tree-v4` and `history-node-v4`.

**Packs and parts.** A pack holds the changeset's new nodes, chunks and
chunk lists, in the order derivation stages them (children first). It is cut
into **parts** of about 16 KB before compression; all but 1.4% of packs on the slice (301 merges) are one part. Part 0 is the changeset's `history-tree-v4`
value, and it also names the root node; parts 1 and up are `history-node-v4`
rows keyed by `Hash::of_object("history-node-v4", id || part_le32)`. Parts
are numbered from the last one staged, so the root's is part 0 and a lookup
at a changeset reads the value it already fetches for the root. A changeset
that changes nothing has a part 0 with no nodes, naming its first parent's
root.

**Addresses.** A node is addressed by a `NodeRef`: (the changeset that wrote
its pack, part, index in the part). A pack is a function of its changeset,
so an address always names the same content, as a content hash did.

**Inside a part:**

- *An id table.* Changeset ids are varint indices into the part's table, 0
  standing for the writer itself. A merge's entries mostly say
  `changed = {merge}`, which costs one byte.
- *Node references* are a table index, a part and an index: three bytes for
  a node of the same pack (the part counted from the referring one), and a
  table entry shared with the ids for another pack's node, whose writer is
  usually also the entry's `changed` id.
- *Positional patches.* A patch names each keyframe entry it replaces by its
  position in the keyframe (the step from the previous one, usually one
  byte), and spells out only names the keyframe lacks. A list patch does the
  same for chunks and no longer stores each chunk's successor, which a
  reader derives from the patched list. Reading a patch therefore always
  reads its keyframe; it did whenever the name sought was not in the patch.
- *Front-coded names*: each name is the length it shares with the previous
  one and the rest.
- *Node lengths*, so a reader decodes only the node it wants.
- The part is zstd-compressed (level 3) when that is shorter, behind an
  8-byte checksum of its uncompressed bytes bound to its writer and number.
  Parts after the first also name their writer and number, so a scan of
  `history-node-v4` rows can check each against its key.

Chunks, boundaries, the 64- and 128-entry limits and patches on keyframes
(never on patches) are as in ADR 0027 and 0037. The patch rule is the same
`patch² <= B × whole`, now over the bytes a node costs encoded on its own
with an id table of its own (`history_pack::cost`), with B = 96 instead of
128. The logical tree and the meaning of `changed` are unchanged, so every
answer is the same.

**Determinism.** A node, and so a pack, is a function of the changeset, the
stored predecessor and the stored keyframe, as before. Parts are cut by a
deterministic rule. Two servers derive the same packs; their bytes are equal
as long as they use the same zstd, and `verify` and `repair` compare packs by
their uncompressed bytes so that a different zstd version is not a
mismatch. Nodes are no longer shared between changesets by content: two
changesets that build the same node store it twice, which on these
histories happens only for nodes nothing else shares either.

**Reading.** Each `Repo` caches parts as read (32 MiB), nodes decoded on
demand (96 MiB, counted at their decoded size: the encoding is several times
smaller), patches waiting for their keyframe (16 MiB) and the root of each
changeset whose value it read (4 MiB). The root cache saves a lookup at a
changeset the value read v3 paid every time. A damaged part fails its
checksum; a reference past a part's nodes is a missing node.

**Measured and rejected:**

| Variant | Linux to v2.6.16 | Synthetic |
|---|---|---|
| v3 | 209.8 MB | 300.8 MB |
| v3 rows with an id table per row (estimate from the rows) | 1.86x | 1.39x |
| ... and front-coded names | 1.91x | 1.44x |
| zstd -3 on each v3 row | 1.98x | 1.47x |
| Packs, 4 KB parts, no compression | 55.0 MB (3.8x) | 85.7 MB (3.5x) |
| Packs, 16 KB parts, no compression | 46.9 MB (4.5x) | 69.2 MB (4.3x) |
| Packs, one part each (no repeated tables) | 44.5 MB (4.7x) | |
| Packs, 8 KB parts, zstd -3 | 38.3 MB (5.5x) | |
| **Packs, 16 KB parts, zstd -3 (chosen, B = 96)** | **34.6 MB (6.1x)** | **51.8 MB (5.8x)** |
| Packs, 32 KB parts, zstd -3 | 32.7 MB | |
| Packs, 16 KB parts, zstd -19 | 33.2 MB | 49.4 MB |

- **zstd -19**: 4% smaller than level 3 and many times slower to write.
- **Larger parts**: 5% smaller at 32 KB, but every lookup at a large merge
  then reads and decodes a part that large. A table per pack shared by its
  parts would get the one-part size (4.7x uncompressed) at small parts, at
  the price of a second row per read of any part but the first; not worth it
  while 98.6% of packs are one part.
- **A dictionary for zstd** trained on rows: rejected in ADR 0037 for its
  versioned rollover; the parts already compress 1.35x without one.
- **Patch budget**: with zstd, B = 96 stores least among 48 (7.60 MB), 64
  (7.44), 80 (7.36), 96 (7.40), 128 (7.68) and 192 (8.06) on 5,000
  synthetic changesets, and 34.1, 34.1 and 34.6 MB for 80, 96 and 128 on the
  slice. Costing ids at fewer than their 32 bytes in the rule stored more (4
  bytes: 13.8 MB uncompressed against 11.6 MB).
- **Chunk sizes**: chunks of 64 names on average (and 256 at most), or of 16
  and whole nodes up to 32, changed the total by +0% and +6%; whole nodes up
  to 128 by -1%.
- **Ids shared with the first parent's pack.** 73% of the ids a single-parent
  changeset's pack names (59% on the synthetic history) are also named by
  its first parent's pack, so a table that extended the parent's would save
  most of the rest of those packs' table bytes. But a reader would need the
  base table's part, known only after reading the part, before decoding any
  node: one more dependent round trip on every cold read. Server-local
  integer changeset ids (the draft ADR on changeset numbers) remove most of
  the table instead, without that read.
- **Changing what a merge writes**: with ids in a table, a merge's entries
  cost a few bytes each; a merge pack is 13.1 KB on the slice against 115 KB
  of v3 rows, and 15.3 KB against 125 KB on the synthetic history.

**Only the labels are versioned.** As in ADR 0027 and 0037, types and
functions change in place: `HistoryEntry::Tree.node`, `ChunkRef.node` and
patch bases are `NodeRef`s. Current servers neither read nor write v3 rows.

**Upgrade.** v3 rows become garbage to new servers. On a store with a full
v3 index (the Linux server: 24 GB), with the old server still serving from
v3:

1. Install the new binary beside the old server and run
   `leep-server repair rederive --kind history-tree-v4` (or start a new
   server with warming on and let it derive). Old and new servers can share
   the database: each answers from its own index. The v4 index of all of
   Linux is expected at about 2.5 to 4 GB, so the disk needs that much free
   beside the 24 GB of v3 rows.
2. Switch traffic to new servers and stop the old ones.
3. `leep-server repair drop --kind history-tree-v3 --dry-run`, then without
   `--dry-run`: it removes `history-tree-v3` and `history-node-v3` rows.
   `drop` knows them as retired kinds and refuses `history-node-v3` on its
   own; `rederive` and `verify` do not accept them.
4. `VACUUM` (or `VACUUM FULL` in a maintenance window, or `pg_repack`) to
   give the space back to the operating system; plain `VACUUM` only makes it
   reusable by Postgres.

Where the disk cannot hold both, drop v3 first and let v4 derive: path
queries then wait for derivation until it finishes.

## Consequences
- On the slice (local Postgres 16, release builds): v4 holds 34.6 MB in
  21,109 values and 1,665 node rows, against 210.5 MB in 311,439 rows (6.1x
  fewer bytes, 13.7x fewer rows). After `VACUUM FULL` the history kinds take
  42.6 MB of the `derived` table, heap, TOAST and index together, against
  200.1 MB; the database went from 292 MB to 135 MB.
- On the synthetic history: 51.8 MB and 22,004 rows against 300.8 MB and
  560,483 (5.8x, 25x); 2.6 KB per changeset against 15.2 KB. Extrapolated to
  Linux's 16 KB per changeset that is about 2.8 KB per changeset, or about
  4 GB for the 24 GB index; the slice's ratio would give 2.6 GB.
- Derivation is no slower on Postgres: 85.4 to 112.9 s on the slice against
  95.5 to 120.5 s for v3 in alternating runs (85.4 s against 102.0 s for the
  final build), and 167.7 to 174.0 s on the synthetic history against 164.5
  to 184.0 s, writing 22,774 rows instead of 371,132 on the slice. In
  memory, where writes cost nothing, it takes 34% longer on the synthetic
  history (162 s against 121 s): encoding, and cache misses that decode a
  node from its part. Peak memory is 253 MiB against 240 MiB there, with
  the row cache counting nodes at their decoded size under 96 MiB (v3: 64
  MiB at twice the encoded size, which undercounted by about 1.7x), 32 MiB
  of parts, 16 MiB of patches and 4 MiB of roots.
- Criterion, in memory: path logs and `last-change` 15 to 30% faster except
  a 1,000-entry log (13% slower), derivation 2 to 11% slower.
- Path queries are unchanged within noise: 312 paths (files, directories,
  deleted files) and 30 blames on the slice, each against a fresh server and
  again warm, alternating v3 and v4 servers on one database (PERFORMANCE.md,
  "Index size: packs"): 50-entry logs took 37.8 to 40.2 s per run against
  35.3 to 48.6 s, `last-change` 5.1 to 6.9 s against 5.6 to 6.6 s. All 654
  response bodies (`last-change`, `log path=`, `blame`) were byte-identical
  in all 34 runs.
- Answers do not change: a test checks every entry of the index, for every
  path that ever existed at every third changeset of a merge-heavy history,
  against the definition of `changed`, and the existing tests compare
  indexed logs with scans.
- `verify` checks parts by checksum, and a scan of `history-node-v4` rows
  checks each part against its key; sampling no longer checks the bytes of
  parts it does not reach (v3 compared each row's hash with its key).
- Changing the part encoding, the cut, the patch rule or its budget needs
  `history-tree-v5` and `history-node-v5`.
