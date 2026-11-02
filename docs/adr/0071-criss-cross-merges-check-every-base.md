# 0071. Criss-cross merges check every merge base
Status: Accepted
Date: 2026-11-01

## Context
`POST /v1/merge` merged against one merge base: `merge_base`, the common
ancestor with the highest (generation, id). In a criss-cross history two
changesets have several best common ancestors, none behind another. When
branches A and B merge into each other as M1 and M2 and resolve file `f`
differently (M1 keeps A's, M2 keeps B's), descendants of M1 and M2 have
bases A and B. Against A, ours left `f` alone and theirs changed it, so a
three-way merge takes theirs; against B it keeps ours. Either way the
merge is clean, and the other side's deliberate resolution is dropped with
no conflict (issue #160). The server must never resolve a conflict
silently.

The review rules that decide whether a merge took a side's change cleanly
(`MergeParent`, and the "dropped side change" check) read one merge base
too, and can be fooled the same way.

Options:

1. Keep one base, chosen by a heuristic (Mercurial's choice). Deterministic,
   but still silent.
2. A virtual base (git's recursive strategy): merge the bases pairwise into
   a synthetic tree and merge against it. Where the bases conflict, git
   writes conflict markers into the virtual base's file, which a later
   three-way merge may then cancel out or carry into the result. It needs
   synthetic trees and blobs nobody asked to store, a rule for nested
   conflicts, and an iterative fold over the bases.
3. Merge against each base and conflict where the answers differ.

## Decision
Option 3.

- `Repo::merge_bases(a, b)` returns every best common ancestor,
  `merge_base`'s answer first. It costs nothing extra in the common case:
  if one side has no ancestor below `merge_base` that `merge_base` lacks
  (`side_is_clear`, up to 10,000 changesets per side), that base is the
  only one. Otherwise git's paint-down walk finds the candidates
  iteratively with batched reads and drops those behind another.
- The merge is made against the first base exactly as before. Then every
  path where some other base differs from the first is resolved against
  each base with the same per-path rules. Where the answers differ, or any
  is a conflict, the path is a conflict (`base` reports the first base's
  version, `merged_with_markers` is empty) and leaves `clean`. Paths all
  bases agree on resolve identically against each, so they are not looked
  at again.
- A merge that every base resolves the same way stays clean, so the common
  criss-cross where both merges resolved alike merges as before.
- The review checks call `merge_bases`: a merge's path counts as taken
  cleanly from a side only if the first parent left it as every base has
  it, and a side's change counts as dropped if any base shows it.
- Lands are unaffected: a land replays a non-merge stack with its first
  parent as the base (a cherry-pick) and never picks a merge base.
  `history-tree-v4` derivation keeps its own handling of several bases
  (it is not a tree merge) and its format is unchanged.

## Consequences
- No edit vanishes in a criss-cross merge: it survives or the merge
  reports a conflict. A randomized test builds histories whose branches
  merge each other with arbitrary resolutions and checks every merge
  against an oracle that brute-forces all best common ancestors.
- Some criss-cross merges that were clean are now conflicts, exactly those
  whose result depended on which base was picked.
- A later edit to a file whose criss-cross resolution the bases disagree on
  conflicts on every merge of the two lines, since each base has neither
  side's version, until a merge of them lands; that merge is then the
  single best common ancestor and later merges are ordinary again. git's
  recursive strategy would merge such an edit cleanly against its virtual
  base. The cost is bounded by one resolved merge, and it is the price of
  never picking a side silently.
- The API shape is unchanged; `GET /v1/merge-base` still answers one base.
- A criss-cross merge where the shortcut fails pays a paint-down walk and
  one tree diff per extra base, plus reads at the disputed paths.
