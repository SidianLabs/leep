# 0079. Land batches read skiplists a level at a time
Status: Accepted
Date: 2026-11-03

## Context
On a 1,000,000-changeset linear history (PERFORMANCE.md, "One million
changesets of history") a land's replay cost 2.75 ms against 0.74 ms on the
same tree 1,000 deep, land-only capacity fell from 454-484 to 167-191 lands
a second, and with 1,000 agents the land p99 rose from 207 ms to 4.4 s.
The replay had not been profiled.

A changeset's skiplist names its first-parent ancestors 1, 2, 4, ... steps
up. `Skiplist::compute` builds entry `k` from the list of entry `k - 1`, so
it reads up to log2(depth) lists one after another. `store_changeset` did
that after the tree was written, inside the land batch's replay of each
copy. The `SkiplistCache` (2 x 16k lists) holds the lists this process
computed, so the low levels hit; but each new copy's high entries are
ancestors 2^k steps up, a different changeset for every copy, and those
rows are read one round trip at a time.

Measured on a 150,000-changeset history from `leep-history-gen` (Postgres
on the same 4-vCPU box, server pinned to two CPUs, `leep-land-bench
--files 2`, 30 s at 16 and 64 agents): skiplist derivation averaged 1.33 ms
(`leep_derive_duration_seconds{kind="skiplist"}`, 0.06 ms on the store 1,000
deep), and the replay histogram 2.79-2.81 ms a land against 0.45-0.48 ms:
the skiplist was nearly all of the difference. A memory-store test of 32
lands on a 4,097-deep history counted 264 skiplist reads.

## Decision
No stored format changes: every skiplist row is byte for byte what
`compute` wrote before. Only where and when its inputs are read changes.

### 1. A batch reads its copies' inputs a level at a time
A land batch attempt knows the head it replays onto before it replays.
Its copies stack on that head one after another, so copy `i`'s entry `k`
is the changeset `2^k` steps up from it: an earlier copy of the batch, or
a stored changeset on the head's first-parent chain. `compute` reads the
list of copy `i`'s entry `k - 1`; when that entry is an earlier copy `j`,
the answer is `j`'s own entry `k - 1`, already planned, and when it is
stored, it is a row to read. So level `k` needs only the rows of the
stored entries `k - 1` of all copies, all known once level `k - 1` is.

`Repo::prefetch_copy_skiplists(head, copies)` walks those levels with one
`get_derived_many` per level for the rows not already cached, and puts the
decoded rows in the `SkiplistCache` (rows read while creating changesets
may be kept there; verification and repair never call it). Each copy's
`compute` then finds every list in memory. The batch plans one copy per
land plus 8 (stacks of more than one), at most 256; a copy past the plan,
or one that does not stack on the head (a stack squashed after a
conflict), reads what is missing exactly as before. A row the plan cannot
read or decode is skipped and left to `compute`, which reads or derives it
and reports errors where it always did.

### 2. Off the replay's path
The plan runs on a task of its own (in the land lane, ADR 0067), started
as soon as the attempt knows its head, beside the batch's other prefetches
and its replays. It is shared through the staging (`Staging::set_skip_plan`,
handed to every handle the attempt makes), and `store_changeset` waits for
it only just before it builds a copy's list. A batch of fast-forwards
never waits for it. A plan never fails; at worst it read rows nobody used.

### 3. A create computes its list beside its tree
`store_changeset` reads the generations and builds the skiplist
concurrently with the blob checks and the tree write, instead of after
them. Their errors keep their order: a check or tree error comes first,
then a generation error, then a skiplist error. A client create on a deep
history no longer adds its skiplist reads to its tree write's time.

## Consequences
- 150,000 deep, before and after (two runs each, lands a second at 16 and
  64 agents): 143-186 and 204-215 before, 249 and 297-327 after; replay
  2.79-2.81 ms before, 0.53-0.56 ms after; land p99 at 16 agents 93-370 ms
  before, 57-60 ms after. On the store 1,000 deep: 256-267 and 299-330
  before, 240-283 and 295-325 after (noise), replay 0.45-0.48 ms before,
  0.39-0.40 ms after. The deep store now lands as fast as the shallow one.
- The 4,097-deep memory test makes at most 30 skiplist reads for 32 copies
  (two batches' plans and a few ancestry checks) where it made 264.
- A batch reads up to one skiplist row per planned copy per level, about
  800 bytes a row at a depth of a million: tens of kilobytes a level, a
  few thousand cached lists at most. A lone land plans 9 copies; rows of
  copies that never come are read for nothing.
- A create may hold two metadata connections for a moment (a skiplist read
  beside the tree write's condemnation read), and a batch's plan holds one
  of the land lane's connections beside the leader's. ADR 0067's reserve
  (at least 4) already counts prefetch reads.
- Tests: `copies_skiplists_match_the_definition` compares every stored row
  of linear and merge histories and of a batch's copies with lists walked
  one parent at a time; `a_batch_on_a_deep_history_reads_skiplists_a_level_at_a_time`
  bounds the reads. Test bookkeeping of a batch's lands now records each
  land as it is taken, before any answer, so a test holding its answers
  always finds the batch.
