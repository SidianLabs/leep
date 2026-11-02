# 0077. Find a missing range down first-parent chains in batches
Status: Accepted
Date: 2026-11-02

## Context
`derive_range` finds the changesets a derivation must compute before it
computes any. It walked a frontier, starting from the parents of the
changesets it had found: one batched row read for the frontier, then one
batched changeset read for the ones lacking a row. On a linear chain the
frontier is one changeset wide, so a gap of N changesets cost about 2N
serial round trips before the first derivation started. The warmer derives
every moved head through `derive_range` (`derive_heads_counted`, every
250 ms), and lands rebase, so its gap is exactly that chain. In the
2,000-agent swarm the history index trailed by about 3,900 changesets and
derived about 70 a second.

ADR 0073 fixed the same shape for bounded queries by counting the
un-indexed chain down the skiplists. `derive_range` still walked.

## Decision
- **Chains, not frontiers, when the frontier is narrow.** After the roots'
  own changesets are read, a frontier of at most 8 changesets
  (`CHAIN_SEEDS`) is followed one first-parent chain at a time
  (`derived::chain_walk::follow_chain`). A wider frontier already fills
  its batches and walks as before.
- **Listing a chain.** From the start, the walk follows skiplist entries:
  a changeset reached through entry `j` follows only its entries below
  `j`, so each distance down the chain is reached once. Each level is one
  batched row read of the kind (a row, or a changeset already seen, bounds
  the chain) and one batched skiplist read of those still short of the
  bound (lists in the skiplist cache need no read). The first row found
  puts the bound within a factor of two of the gap, so levels past it list
  nothing beyond it. A gap of N costs about log2(N) levels and 2N/256 + N/256
  batched reads, the changesets included.
- **The same range.** The chain kept is the run of distances 0, 1, 2, ...
  that lack a row, below the bound. Its changesets are then read in
  batches, and each is kept only while it is the first parent of the one
  above, read from the changeset itself. A missing or damaged skiplist
  row only shortens the chain; the frontier walk goes on from where it
  stops. Every changeset recorded had its row checked and is reached
  through parents lacking rows, as before, so the range, its order and the
  rows written are unchanged. Only parents are kept, as before.
- **The skiplist kind** walks the frontier only: its rows are the ones
  missing.

## Consequences
- On local Postgres, the history index catching up a 4,000-changeset
  linear gap from a cold handle made 138 metadata reads instead of 8,083,
  and started deriving after 0.14 s instead of 1.92 s; it derived 5,000
  changesets a second instead of 1,550 (`warm_catch_up`, PERFORMANCE.md).
  The saving grows with round-trip latency, which is what a loaded
  Postgres adds.
- Every derivation gets it, not only the warmer's: a query deriving inline
  in `local` mode finds its range the same way.
- A short gap costs about one read more than before: the chain's start is
  checked, then its skiplist read, before the walk learns the gap is one
  changeset.
- A frontier wider than 8 that stays wide for long (several long branches
  side by side) still walks a step per round trip, with up to 256
  changesets per batch.
- The warmer does not pass the head it last warmed as a stop: the rows
  already bound each chain within a few reads.

## Alternatives considered
- **Descend first, then list.** Finding the boundary with `newest_indexed`
  and then listing the chain takes about twice the levels; listing and
  bounding in one pass reaches the same boundary.
- **Reading changesets ahead along the chain.** A changeset names only its
  parents, so the chain cannot be read in batches without knowing its ids.
- **Warmer-only stop at the last warmed head.** Saves nothing over the
  rows, and does nothing for inline derivations.
