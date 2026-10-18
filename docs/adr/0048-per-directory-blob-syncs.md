# 0048. Per-directory syncs for blob flushes
Status: Accepted
Date: 2026-10-18

## Context
ADR 0044 deferred the filesystem store's directory fsyncs to
`BlobStore::flush`, which synced every directory recorded since the last
flush in one wave, one wave at a time, and coalesced concurrent flushes. On
`leep-land-bench` with 64 agents and 10-file changesets, create p50 got
about 25% worse than before ADR 0044 while uploads and throughput improved.

Timers on the create path (64 agents, local 4-vCPU machine, Postgres)
showed that a create spent 20 to 44 ms of its 51 to 93 ms server time
waiting in its flush, about one wave: waves held 80 to 122 directories
(every shard the agents' uploads and tree writes had dirtied since the last
wave), took 21 to 40 ms over four blocking tasks and ran 10 to 17 times a
second. A create needs about 13 of those directories: the shards of its three
or four tree nodes and of the ten contents it names.

The first explanation tried was that waves start lazily, only when a
flush asks for one, so a create waits for a whole wave it could have
overlapped with. A build that ran waves continuously in the background
whenever anything was marked brought create p50 from 156 to 129 ms in the
same rounds (four alternating rounds, medians), by keeping waves smaller.
But the waits were already about one wave, not one and a half, so waves
were not starting late. The cost was the size of the wave: a create waited
for every directory dirtied since the last wave, and a create arriving
during a wave still had to wait for the next one, since its tree nodes
were written after the running one started. Eager waves also add
directory fsyncs that nothing waits for.

## Decision
- **`BlobStore::flush_blobs(hashes)`.** A new provided method: make the
  stored copies of `hashes` durable before returning, and nothing more is
  promised. The default calls `flush(hashes)`, which covers at least as much,
  so a backend or wrapper that does not override it stays correct. The
  cache, the server's store wrapper, the fault injector and the land
  staging store (a no-op, as for `flush`) forward it.
- **Callers name what they depend on.** `Repo::put_rescuing_many` (tree
  nodes, merged contents, batched imports, a land batch's store) calls
  `flush_blobs` with the blobs it wrote and the contents it was asked to
  name, alongside its condemnation read; a rescue that writes a blob again
  flushes that blob. `flush(named)` keeps its meaning (everything since the
  last flush, and `named`) for `sync_blobs`, which seals a backup or restore.
- **The filesystem store tracks each directory on its own.** A mark (a put
  that renamed a blob into place or found it stored) takes a sequence
  number and records it on its shard, and on the root while the shard's
  entry there is not known durable. A sync of a directory that started at
  sequence `n` makes every mark of it up to `n` durable. A flush computes,
  for each directory it needs, the mark a sync must cover; it starts syncs
  at once for the directories no running sync covers (in up to four
  blocking tasks), and waits for the running ones that already cover its
  marks. Flushes no longer run one at a time. The blocking tasks record
  their own completion, so a sync started by a request that was cancelled
  still counts for the requests waiting on it; a failed sync is retried by
  the next flush that needs the directory.
- `flush_blobs` marks the shards it is given before computing what to wait
  for, as `flush(named)` did, so a blob renamed into place by another
  process, or before a restart, is covered by a sync that starts after the
  caller found it.

## Consequences
- The durability rule of ADR 0044 is unchanged: a stored changeset's blobs
  survive power loss, and no changeset is stored before the directory
  entries of the blobs it names are synced. An upload alone is still not
  durable. `flush_tests.rs` now models `flush_blobs` as syncing only what it
  names, so a write path that relied on someone else's flush fails there.
- At 64 agents a create's flush takes 3 to 6 ms instead of 20 to 44, shorter
  than the condemnation read it runs alongside, so it is off the create's
  critical path; mean server time per create fell from 74 to 56 ms (median of
  eight 5-second windows). What is left of a create is Postgres round trips
  and, on a loaded machine, waiting for a CPU. `docs/PERFORMANCE.md` has the
  benchmark rounds, which are noisy on the shared machine.
- Directory fsyncs are no longer shared across creates whose sets overlap
  unless their syncs happen to run at the same time: with 64 agents a wave
  of 80 to 120 directories served ten to fifteen creates needing about 13 each,
  so the store now issues somewhat more directory fsyncs per land under
  heavy load (about 13 per create instead of about 8), and the same at low
  load. Uploads issue none, as before.
- Each flush can start up to four blocking tasks instead of sharing a
  wave's four; at 64 agents that did not cost measurable throughput.
