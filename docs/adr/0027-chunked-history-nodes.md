# 0027. Chunked path-history nodes
Status: Accepted
Date: 2026-10-16

## Context
The path-history index (ADR 0013) stores one `HistoryNode` row per version of
each directory. A changeset rewrites the whole node of every directory on the
path of each changed file, and nodes keep the entries of deleted paths for
good, so a busy directory's node only grows and every change to anything in it
rewrites all of it.

On the real git/git history (82,500 commits) this made 174,978 node rows
averaging 36 KB (largest 139 KB): 6.1 GB of node bytes, 3.5 GB in Postgres,
against a 328 MB git repository. git/git's root directory holds hundreds of
live and deleted entries, so nearly every commit wrote another ~36 KB root
node that differed from its parent's in one or two entries.

## Decision
Store large nodes in content-defined chunks, so a change rewrites only the
chunk that holds the changed entry and unchanged chunks are shared, by hash,
between versions.

**Rows.** Each `history-node-v2` row is a postcard `StoredHistoryNode`:
`Flat(HistoryNode)`, a whole node, or `Split(Vec<ChunkRef>)` with
`ChunkRef { first: String, node: Hash }`, the first name and address of each
chunk; every chunk is a `Flat` row of the same kind. Rows are keyed by
`Hash::of_object("history-node-v2", bytes)`. A node's address, used in
`HistoryEntry::Tree.node` and as the `history-tree-v2` value, is the hash of
its top row. The logical `HistoryNode` and `HistoryEntry` and the meaning of
`changed` are unchanged; bytes are in FORMATS.md.

**Splitting.** Nodes of at most 64 entries are `Flat`. Larger ones are cut over
their sorted names: a chunk ends after a name whose
`Hash::of_object("history-chunk-boundary", name)` has its first byte's low 5
bits zero (one name in 32 on average), after 128 entries, or at the end.
Boundaries depend only on names (apart from the cap), so the rule is
deterministic, a logical node has exactly one address, and an insert or
delete changes at most two chunks. One level suffices: the chunk list costs
about 40 bytes per 32 entries, so a 10,000-entry directory's list is about
12 KB and a 100,000-entry one about 120 KB, still far below what one flat
node of that size was. The 128-entry cap bounds a chunk when hashing finds no
boundary in a long run of names.

**One seam.** Reads and writes go through `Repo::read_history_node` (returns
the full logical node, joining chunks), `Repo::history_child` (one entry,
reading only the chunk that would hold it; used by every path lookup) and
`Repo::put_history_node(node, base)`. `base` is the stored node the new one
was edited from (the first parent's version); rows the two share are not
written again, so an edit writes one chunk and the chunk list. Skipping is
decided by `base`, never by what is cached: after `repair drop` the cache may
still hold rows that are gone, while `base` was itself stored by the
derivation that produced it (the same argument by which v1 shared untouched
subdirectories). The empty node is never stored; its address is always
present. Readers check that each chunk is `Flat`, begins with its `first` and
ends before the next chunk's `first`, so a damaged list is an error, not a
wrong answer.

**Cache.** Joining a split node costs one row read per chunk, and derivation
reads the root and other hot directories for every changeset. Each `Repo`
keeps decoded rows by hash in a bounded cache (64 MiB, counting a row at twice
its encoded size, in two rotating maps like the ancestry cache). Rows are content addressed and checked against their hash before
they are cached, so entries never go stale. `verify` reads the store directly,
without the cache, so damage on disk is never hidden by it.

**Only the labels are versioned.** The derived kinds are renamed
`history-tree-v1` to `history-tree-v2` and `history-node-v1` to
`history-node-v2`, and that is the only versioned name in this change: types,
modules and functions keep their names and change in place. The rename is
needed because existing databases hold v1 rows in the old encoding, and an old
and a new server may share one database during an upgrade: under the old
names each would read rows the other cannot decode, or, worse, decode them as
something else. The FORMATS.md rule for a changed derived encoding (ADR 0013,
"Consequences") asks for exactly these names.

**Upgrade.** v1 rows become garbage to new servers. The index derives again on
the first path query of a history (on a long history, that query derives all
of it), or ahead of traffic with
`leep-server repair rederive --kind history-tree-v2`. Once no old server is
left, `leep-server repair drop --kind history-tree-v1` (with `--dry-run` to
count first) removes `history-tree-v1` and `history-node-v1` rows; `repair
drop` knows them as retired kinds, refuses `history-node-v1` alone as before,
and `rederive` and `verify` do not accept them.

**Mixed versions.** An old server keeps reading and writing v1 rows, a new one
v2 rows; both answer correctly and identically from their own index, and
neither touches the other's rows. Dropping v1 while an old server runs only
makes that server derive its index again.

## Consequences
- On the first 3,795 commits of git/git (local Postgres 16, release build),
  node rows went from 5,224 rows and 45.2 MB (v1) to 11,725 rows and 13.0 MB
  (v2): 4,160 chunk lists averaging 496 bytes and 7,565 whole nodes and chunks
  averaging 1.4 KB. Deriving the index took the same time as v1 within the
  noise of a shared machine (30.7 to 58.7 s for v1, 34.1 to 38.3 s for v2 over
  three runs each; about 14 s of server CPU for both). The savings grow with
  the width of the directories that change, so the full history, whose root
  node averaged 36 KB, gains more than this early prefix.
- More rows, smaller ones: a change to an entry of a split directory writes
  two rows (chunk and list) instead of one.
- Lookups by path read the chunk list and one chunk per split directory
  instead of the whole node.
- Each server holds up to 64 MiB of decoded history rows.
- Changing the split rule, the boundary domain or the row encoding changes
  addresses and needs `history-tree-v3` and `history-node-v3`.
