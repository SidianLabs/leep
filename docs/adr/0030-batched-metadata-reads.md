# 0030. Batched metadata reads for history walks
Status: Accepted
Date: 2026-10-16

## Context
`log` without a path, `log` filtered only by `author` or `extra`, and
`provenance` walk every ancestor newest first. The walk read one changeset,
then the generation of each new parent, then the root tree, each awaited
before the next: three metadata round trips per entry on a cold server, two
once generations are in the ancestry cache. On Postgres that is about 0.1 ms
each, so on the first 3,795 commits of git/git `log limit=3795` issued 11,385
statements and took 0.9 to 1.4 s, and on all of git/git `log limit 10000` took
3.6 s against git's 0.15 s. The cost is latency, not work: every row is a
primary-key lookup.

Fetching the frontier in batches helps less than it seems, because the walk
only learns a changeset's parents by reading it. A batch per generation level
still costs one round trip per level, and a long first-parent chain has one
changeset per level.

## Decision
1. **Batched reads on `MetaStore`.** `get_changesets(&[ChangesetId])` and
   `get_derived_many(kind, &[Hash])` answer many keys in one call, one slot per
   key in request order, `None` for an absent key, a repeated key answered at
   every position. Both are provided methods that loop over the single-item
   call, so every backend and test double keeps working unchanged. Postgres
   overrides them with one `= ANY($1)` query, SQLite with `IN (...)` queries of
   at most 500 distinct keys, and the memory store with one lock. The meta
   store contract covers order, absent and repeated keys, empty and large
   requests and kind isolation.
2. **Read-ahead in the ancestor walk.** `leep_derive`'s walk keeps a read-ahead
   (`prefetch::ReadAhead`) of changesets it expects to reach, and when it needs
   one it does not have, reads up to 256 at once: changesets, generations, root
   trees and skiplists, the four batched calls issued concurrently. Its guesses
   are the parents of changesets it has read and the skiplist entries
   (first-parent ancestors 1, 2, 4, ... steps up, with their generations)
   within the batch size. Once a run of consecutive ancestors has been read,
   their skiplists name every ancestor of the next run, so a long chain is read
   a window at a time instead of one level per round trip, and generations come
   from the skiplists rather than another read. An unfiltered `log` reads no
   further than `limit`: a changeset more than `want` generations below the
   walk cannot be among the next `want` it visits.
3. **The walk's answer does not depend on the read-ahead.** The walk keeps its
   heap ordered by generation then id, its `seen` set and its `limit`
   semantics, and asks the read-ahead for data by id. Anything a batch did not
   supply (an absent row, an undecodable one, a failed batch) is read with the
   old single-item call at the same point, so derivation and errors happen
   where they did. With only a metadata filter, the root trees of the matches
   are read in one batch at the end rather than ahead for every changeset
   scanned.
4. **The path walk is unchanged.** `log --path` yields changesets it finds in
   the path-history index at the previous one's parents, so it has nothing to
   read ahead of; batches of one, tried, were slower than plain reads.

## Consequences
- On the 3,795-commit slice of git/git (local Postgres 16, release build), `log
  limit=3795` takes 0.10 s instead of 0.93 s and 84 statements instead of
  11,385; `limit=100` 7 ms instead of 27 ms; an author filter that matches
  nothing scans the slice in 65 ms instead of 0.46 s (`docs/PERFORMANCE.md`).
- On in-memory stores a read costs well under a microsecond, and the
  read-ahead's bookkeeping and extra skiplist reads make the walk two to four
  times slower there (tens of milliseconds for 10,000 changesets). Only tests
  and benchmarks use those stores.
- A limited `log` may read up to one batch (256 changesets) it does not
  return, and a filtered one up to one batch past the last match. Batches are
  bounded so a single query never grows with history.
- Backends that do not override the batched methods get the old cost, and no
  error. A new backend should implement them with one query each.
- `MetaStore` grows by two provided methods; nothing persisted or on `/v1`
  changes.
