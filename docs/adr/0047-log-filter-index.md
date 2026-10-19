# 0047. Log filter index
Status: Accepted; how a merge's side is computed is replaced by ADR 0051
Date: 2026-10-18

## Context
`GET /v1/log/{id}` with `author=` or `extra=` and no path, and the CLI's
`leep log --author/--agent/--agent-model/--agent-session`, walked every
ancestor newest first and tested each changeset. A common value fills the
limit quickly; a rare one reads all of history. On the full Linux history
(1.48M changesets, Postgres) `log author=Junio limit=100` took 151-154 s
cold and warm. Agents make this worse: a team asking "what did model M or
session S do" asks exactly the rare-value question, on histories that grow
by thousands of changesets a day.

The answer must stay what the scan returns: all ancestors (every parent,
not just the first), ordered by generation then id, `limit` counting
matches; `author` a case-insensitive substring; `extra=key:value` exact and
`extra=key` presence, all ANDed.

Options considered:

- **A map per changeset from value to its newest matching ancestors**, a
  persistent hash trie like the path-history index (ADR 0013). Exact, and a
  lookup costs a few reads, but every changeset rewrites the root-to-leaf
  path of its author: about 1-2 KB per changeset with 32-byte child
  addresses (1.5-3 GB on Linux) before chunking and patches (ADR 0027,
  0037), which is the complexity the path index already carries.
- **Global posting lists per value** plus an ancestry test per candidate.
  Not a pure function of a changeset and its parents, so not derived data
  (posting lists grow), and an ancestry test on a merge-heavy history costs
  milliseconds, so 100 candidates is still slow.
- **Fingerprint sets over first-parent stretches** (chosen): per-changeset,
  pure, a few hundred bytes, and enough to skip stretches without a match.

## Decision
A per-changeset kind `log-filter-v1`. With `d` the changeset's
first-parent depth (roots 0), it stores one set of 64-bit fingerprints per
level `j` in `0..=tz(d)` (trailing zero bits; a root has level 0 only).
Level `j` is a superset of the values of every changeset reachable from it
but not from its first-parent ancestor `2^j` steps up (the skiplist's entry
`j`), side branches merged in that stretch included:

- level 0 = the changeset's own values plus, at a merge, those of exactly
  the changesets each other parent brings in;
- level `j` = level `j - 1` united with level `j - 1` of the ancestor
  `2^(j-1)` up, whose depth has exactly `j - 1` trailing zeros, so it stores
  that level.

Like a Fenwick tree, `popcount(d)` levels cover a changeset's whole history
and any first-parent stretch splits into a few levels. Values are the author
(lower-cased), the presence of every `extra` key, and the value of
`agent.model` and `agent.session` (others, like prompts and request ids, are
mostly unique; a filter on them is narrowed by the key's presence). The row
also holds the depth, the generation of the nearest merge on the
first-parent chain, and at a merge, per side parent, its *stop*: the first
changeset on that parent's first-parent chain that the first parent reaches.

**A merge's side** is computed without walking it changeset by changeset.
With `m = merge_base(first, side)`, every chain changeset ranking above `m`
(generation, then id) is outside the first parent's history: it would be a
better common ancestor. The walk down the side's chain takes the widest
level that stays above `m` and spans no merge, and stops at `m`. A merge on
the chain adds its own values and its other parents are walked the same way;
its level 0 is not used, because on a branch that merges mainline back it
holds mostly history the first parent already has, and using it made every
level above say "maybe" (a first version did: 5.2 KB per changeset and no
skipping on the benchmark history). Past `m`'s rank without meeting it (`m`
is off the chain, after a back-merge), whether the first parent reaches a
changeset is asked of one shared walk of the first parent's ancestors while
within 2,048 generations of it, else of a new merge base. One `merge_base`
per merge in the common case, as the path-history index already pays.

**The query** is the scan's walk with two changes. Before expanding a
changeset it reads its row, and if the highest level that cannot hold a
match is `j`, it queues the ancestor `2^j` up instead of the parents (level
0 ruling out: the first parent only). Every remaining match is still below a
queued changeset, and an ancestor ranks below its descendants, so the heap
yields the same matches in the same order. And a merge's side parents are
queued with their stop: the side's walk follows only first parents and
jumps, so it stays on that chain, and drops a changeset ranking at or below
the stop, which the first parent's walk covers. A changeset queued by two
walks with different stops is kept unbounded, and a dropped one is
forgotten, so the walk responsible for it can still queue it. Without stops
the walk into a branch followed the branch to its root. Changesets are read
only where level 0 may match; rows, skiplists and changesets are read 128 at
a time for the changesets nearest the top of the heap. The first 512
changesets are scanned plainly, which answers frequent values faster than
the index; the walk then continues from the same heap.

**Substring authors.** Name rows `log-filter-name-v1`, keyed by
`Hash::of_object("log-filter-name-v1", fingerprint)`, hold the lower-cased
name. A query unites the author fingerprints of the widest levels down the
head's first-parent chain (every author the head reaches), looks up their
names (cached per process) and keeps those containing the substring. A name
row that is missing or does not hash to its fingerprint counts as a match,
so a lost row costs speed, never answers. More than 4096 matching authors
(a broad substring) uses the scan.

**Bounded rows.** A level keeps at most 65,536 fingerprints per class
(author, key, value); past that the class is marked as overflowing (anything
may be present) and dropped, so a row stays under about half a megabyte
however many sessions a stretch holds. A query then skips at the widest
level that did not overflow.

Fingerprints are the first 64 bits of `Hash::of_object("log-filter-fp",
class ‖ parts)` with the class in the top two bits. A collision only makes a
level say "maybe"; two authors sharing a name row's fingerprint (about
`n² / 2^63` for `n` authors, 1e-10 for Linux's 30,000) would hide one name
from substring matching.

The kind is derived by the background warmer for every ref head (ADR 0028)
and otherwise by the first filtered query that gets past the plain scan.
Rows are staged and written in batches, like `history-tree-v3`.

## Consequences
- Filtered logs read changesets near matches and a few rows per skipped
  stretch: on a 200,000-changeset synthetic history in Postgres a rare
  author or session takes 60-70 ms warm and 0.3-0.7 s cold instead of 5-7 s;
  see PERFORMANCE.md "Filtered log". The walk visits about 20 changesets per
  match, so a value of middling frequency (one changeset in a few hundred)
  is slower than the scan when cold.
- 859 bytes per changeset on that history, which merges mainline back into
  long-lived branches often (626 in memory at 60,000); a fraction of the
  path-history index's 16 KB on Linux. Deriving it took 240 s for 200,000
  changesets.
- Not yet measured on Linux or git/git.
- `provenance` is unchanged: it summarizes the newest `limit` changesets, a
  bounded scan, not a filter.
- `log --path` with filters still walks the path index and tests each
  changeset it yields.
- `verify` recomputes the kind; `repair rederive --kind log-filter-v1`
  rewrites rows and names; `repair drop --kind log-filter-v1` removes both.
- Indexing another `extra` value is a new kind name (`log-filter-v2`).
