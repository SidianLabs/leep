# 0043. Staged land batches
Status: Accepted
Date: 2026-10-18

## Context
Group commit (ADR 0032) moves the ref once per batch, but on a 4-vCPU VPS
with Postgres and filesystem blobs on a real disk `leep-land-bench` still
landed a flat ~60 times a second at 1, 4, 16 and 64 agents: a batch of 32
took about half a second. Timers around the batch on a local run with the
same shape (45 lands/s at 64 agents) put 16 ms on each land inside a batch:

- 9 ms storing the replayed copy: three tree nodes written and fsynced, the
  condemnation read that follows them, the creation inputs read, and a
  transaction for the changeset and its rows. Each is a round trip waiting
  for a connection in a pool that 64 agents' uploads and creates also use.
- 4 ms in the landed-copy scan of the first land of each batch, reading one
  `rebased-origin-v1` row per changeset the previous batch added.
- 3 ms of reads of the land's changeset, root trees and base tree, plus the
  merge itself (0.5 ms).

The ref read and swap were 12 ms per batch. The cost was the stores' latency
per replay, not computing the replays: trees were already patched along the
changed paths only.

## Decision
- **Stage.** Each batch attempt replays onto a `Repo` whose stores are a
  staging layer over the real ones (`land_stage.rs`). Blob, changeset and
  derived-row writes go to memory; reads see them first, then fall through.
  Changesets and rows of kinds fixed once written (root tree, generation,
  rebase link) that a replay reads are kept for the rest of the attempt.
  Refs cannot be moved through it. The replay code is unchanged and shares
  the process caches, which only hold facts fixed by content.
- **Store once, in order.** Before the swap the batch stores what it staged:
  every blob in one wave of concurrent writes followed by the batched GC
  condemnation read and rescue (`put_rescuing_many`, write before check as
  ADR 0012 requires), then every changeset and row in one atomic write
  through a new `MetaStore::put_changesets_with_derived` (one statement on
  Postgres, one transaction on SQLite, one lock on memory; the default writes
  them in turn). Only then does the ref move. A failure is a failed commit
  (ADR 0032): the batch is redone from a fresh read, and three in a row are
  answered. A batch that only conflicts or finds its lands already landed
  stores nothing.
- **Prefetch.** A batch of several reads its lands' changesets and the root
  trees of them and of their parents in one round of batched reads, and the
  `rebased-origin-v1` rows of the head's ancestors its landed-copy scans will
  walk (as far as the process already knows the ancestry) in one more, both
  alongside each other. A lone land reads lazily as before.

## Consequences
- On a local 4-vCPU run, 64 agents land 215 to 230 times a second on
  Postgres instead of 58 to 70, and 199 instead of 90 to 96 on SQLite; a
  lone land is unchanged (`docs/PERFORMANCE.md`). A replay inside a batch
  costs about 2 ms; what is left per batch is the store (20 to 45 ms) and a
  few round trips, which under load wait for a pool connection behind the
  agents' own uploads and creates.
- Durability and crash safety are those of a lone land: nothing references
  an object before it is stored, the ref moves after everything is stored,
  and an interrupted store leaves only unreferenced objects for GC. The
  fault sweeps and the crash test pass unchanged; the sweep's guard against
  a vacuous plan now allows fewer injected faults, since a land makes fewer
  store calls.
- Every `MetaStore` should override `put_changesets_with_derived`;
  `check_meta_store_contract` checks it. Store wrappers that do not forward
  it fall back to the default, which is correct but not atomic.
- A staged land batch holds its writes in memory until it stores them: a few
  tree nodes and one changeset per land, bounded by the batch size of 64.
