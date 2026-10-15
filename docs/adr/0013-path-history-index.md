# 0013. Path-history index
Status: Accepted
Date: 2026-10-15

## Context
`log -- <path>` read every ancestor and compared its entry at the path with its
first parent's: linear in history (about 57 ms for 10k changesets, whatever the
number of matches). The roadmap lists a path-history index, and "when did this
file or directory last change" is the question agents ask most. The answer
must stay exactly what the scan returns, because the scan is the specification:
a changeset is in the log of a path when its tree entry at that path differs
from `parents[0]`'s (a root is compared with the empty tree), and the log is
ordered by generation, then id, newest first.

## Decision
A derived kind `history-tree-v1`, in the style of Mononoke's fastlog and
unodes, maps every changeset to the hash of the root of a **history tree**: a
tree of `HistoryNode`s parallel to the root tree. Nodes are postcard-encoded,
content-addressed under `Hash::of_object("history-node-v1", bytes)` and stored
as `history-node-v1` rows in the meta store (`put_derived`), so a changeset
shares every node it did not touch with its first parent. Encodings are in
FORMATS.md.

An entry records, for one path component, `changed`: the **maximal** changesets,
under ancestry, among those whose entry for the path differs from their first
parent's. For a directory that includes any change beneath it. It is a single
id except when merged branches changed the path concurrently. A directory entry
also carries the hash of its children's node.

**Derivation** for changeset `X`, per path `p` touched by `X`'s changes (and
their ancestor directories):
- if `X`'s tree entry at `p` differs from `parents[0]`'s: `changed = [X]`;
- otherwise `changed` = the maximal elements of the union of the parents'
  `changed` for `p`.

Everything else is inherited from `parents[0]`'s nodes. Only the spines of
changed paths are read or written, like `root_tree`.

**Merges.** A merge `M` changes relative to `parents[0]` only, so "changed at
`M`" means the same as for any changeset. If `M` leaves a path as it was in
`parents[0]`, `M` is not a change to it, but the other parents' ancestors may
be, and the first parent's entry alone would lose them. For example both sides
make the same edit and the merge is empty: the scan lists both edits. So an
unchanged path takes the union of all parents' entries. The extra paths a merge
must visit are found by diffing the first parent's history tree against each
other parent's, skipping equal subtrees by hash, so the cost is bounded by how
far the parents' histories differ. No tree lookups for other parents are
needed, because the comparison is always with `parents[0]`.

**Deleted paths keep their entries** (a path that has been removed stays in the
history tree with the changeset that removed it), because the scan finds the
deleting changeset in the log of a deleted file, and a path that changed
between file and directory keeps both lineages in one `changed` set.

**Log walk.** Start at the tip: look up the path to get `changed`, push those
ids on a heap ordered by `(generation, id)`. Pop the newest, yield it (subject
to the author and `extra` filters, which still apply to each yielded
changeset), and look the path up in each of its parents' history trees to push
the next `changed` sets, skipping ids already seen. Because every unvisited
change is an ancestor of something on the heap, and ancestors have lower
generations, pops come out in exactly the scan's order. The walk is iterative,
reads only changesets that changed the path, and a directory works the same
as a file. `last_change` is the first element of that order and is read from
one lookup.

`log_filtered` with a path uses the index. Without a path, or when only filters
are given, the linear walk remains. The old path-filtered scan stays as
`Repo::log_scan` (hidden from docs): the oracle of the property test and the
benchmark baseline.

New endpoint `GET /v1/last-change/{id}?path=p` and CLI `leep last-change`.
`/v1/log` is unchanged on the wire.

## Consequences
- A path log costs about one lookup per change to the path instead of one
  changeset read per ancestor; `last_change` is independent of history length.
- The index is derived lazily on the first path query for a history, oldest
  first and iteratively, and costs a few node writes per changeset. Concurrent
  servers write identical bytes, so races are harmless.
- The history tree is not pruned: it holds an entry for every path that ever
  existed, so it grows with churn (renames and deletes) rather than with the
  size of the current tree. Entries are small (one 32-byte id each).
- A merge pays a diff of the parents' history trees; merging a branch that
  changed many paths derives in time proportional to those paths.
- Frontier sets are pruned with `is_ancestor`, so deriving a merge of branches
  that both changed a path costs a few skiplist walks per such path.
- Blame still walks first parents; the index could shorten that walk but does
  not follow first-parent semantics, so it was left alone.
- Changing the entry encoding or the meaning of `changed` needs
  `history-tree-v2` and `history-node-v2`; old rows are ignored.
