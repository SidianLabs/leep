# 0044. Deferred directory syncs for filesystem blobs
Status: Accepted
Date: 2026-10-18

## Context
A filesystem blob is durable once its file is synced and the rename that
published it is synced, which takes an fsync of its shard directory (and of
the store root when the shard is new). The hardening change before ADR 0043
added that directory fsync to every put, so a blob a changeset names cannot
vanish on power loss. On a 4-vCPU VPS each one cost 1.3 to 2 ms, a lone
agent's lands fell 15 to 25%, and with 64 agents landing on one ref the
agents' own requests became the limit: a blob put took 66 to 110 ms at p50
and a create 130 to 215 ms, against 6.5 and 10.5 ms for one agent. A create
writes three or four tree nodes, each a file fsync plus a directory fsync,
and every upload paid a directory fsync that nothing needed until a
changeset named the blob.

The rule that matters is narrower than "every put is durable": *a blob that
a durable changeset names survives power loss*. Nothing else a client is told
depends on a blob's durability: an upload's 204 is followed by a create
before the blob is used, and `POST /v1/blobs/missing` answers "present"
only to let the client name the blob in a create.

The same change decided whether a shard was new before creating it, so two
puts racing to create a shard could both skip the root sync.

## Decision
- **`BlobStore::flush(named)`.** A new provided method: make durable, before
  returning, every blob the store has written or found already stored since
  the last flush, and the stored copies of `named`. The default does nothing,
  for backends whose puts are durable when they return (object stores) or
  never durable (memory). The cache, the server's store wrapper and the fault
  injector forward it; a land batch's staging store makes it a no-op, since
  the batch flushes the real store when it stores what it staged.
- **The filesystem store defers its directory syncs.** A put still syncs the
  file before the rename, so a blob at its address is always whole, but it
  only records the shard (`dirsync.rs`). A put that finds the blob already
  stored records it too, since that may be another writer's rename that
  nobody synced. The root needs a sync for a shard this process has not
  synced it for; that is tracked per shard after the fact, so the race on
  new shards is gone. `flush` syncs every recorded directory in one wave
  (split over four blocking tasks), deduplicated. Flushes coalesce: each
  record takes a sequence number, one flush at a time runs a wave over
  everything recorded so far, and a flush returns as soon as a wave that
  started after its records has finished, whoever ran it.
- **Flush before acknowledging.** `Repo::put_rescuing_many` (tree nodes,
  merged contents, batched imports, a land batch's store) flushes the blobs
  it wrote alongside its condemnation read, and `apply_changes` names the
  contents its changes reference in that same flush, so a create pays one
  wave for its tree and its contents. `store_changeset` (create, merge,
  rebase, import, root-tree derivation) and the land batch therefore store a
  changeset only after everything it names is durable. Naming the contents
  covers blobs uploaded to another server process on the same directory, or
  to this one before a restart, whose own records are not here. A blob put
  (`PUT /v1/blobs`) is no longer durable when it is answered; its create is.
  `sync_blobs` (backup and restore) flushes the destination before it
  returns. A repack syncs the shard of every loose copy it puts back, which
  may be the only live copy.
- **Batched existence checks.** `BlobStore::contains_many` (provided: 64
  `contains` at a time) answers many hashes in order. The filesystem store
  stats loose files in a few blocking tasks and looks the misses up in the
  packs after one rescan, instead of a blocking task and a rescan of
  `packs/` per hash. `Repo::missing_blobs` now runs in phases, each for every
  hash before the next: condemnation rows in batched reads of 8192 keys, 4 at
  a time; the full fence of the hashes that have a row, 64 at a time;
  existence in one `contains_many`; the claims in one batch. Each hash's
  fence is still read before its existence and both before its claim
  (ADR 0012).
- **Uploads.** `put_blob` writes the claim alongside the condemnation read
  instead of after it. Both follow the blob write, which is what the rescue
  needs; the claim only keeps `condemn` from picking the blob, which its
  fresh modification time does too until the claim lands.

## Consequences
- The durability contract for what is acknowledged is unchanged: a stored
  changeset's blobs survive power loss. What changed is that an acknowledged
  upload alone may not; a client that uploads and never creates loses
  nothing it was promised. The ARCHITECTURE durability table says so.
- On a shared local 4-vCPU machine (load 6 to 8) with Postgres and blobs on
  ext4, `POST /v1/blobs/missing` with 90,000 never-stored hashes takes 0.34
  to 0.44 s instead of 6.8 to 11.6 s; most of what is left is the
  condemnation lookup in Postgres. Upload p50 at 64 agents halves (19 to 22
  ms instead of 40 to 42). Creates and lands at 64 agents are within the
  noise of the machine; a create now waits for a sync wave shared with every
  agent's writes instead of its own syncs, and waves grow with the number of
  shards written since the last one. `docs/PERFORMANCE.md` has the runs.
- A wave syncs each recorded directory, so at high write rates the number of
  directory fsyncs per second approaches the number of blobs written, as
  before; the saving is that uploads stop waiting for them and that a
  create's nodes and contents share one.
- Store wrappers that do not forward `flush` fall back to the no-op, which
  is wrong only in front of a store that defers durability: the server's
  stack forwards it, and `check_blob_store_contract` exercises `flush` and
  `contains_many` on every backend. Tests (`flush_tests.rs`) check that no
  changeset is stored while a blob it names, including one written by
  "another process", is unflushed, for creates, merges and batched lands.
