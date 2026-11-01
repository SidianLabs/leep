# 0072. Pipelined land batches
Status: Accepted
Date: 2026-11-01

Amends ADR 0032 (one batch at a time per ref), ADR 0043 (a staging layer
over the stores only), ADR 0060 section 2 and ADR 0068 (how admission
counts batches and turns seconds into them).

## Context
With 1,024 agents on one ref and the server pinned to 2 vCPU (Postgres,
filesystem blobs, the 20,000-file fixture of ADR 0070), lands ran at about
221 a second with p50 4.0 s and p99 4.6 s, which is Little's law for 1,024
closed-loop agents at that rate. Every batch filled (64 lands) and took
about 290 ms, of which a large part was its commit: the pack write and its
fsyncs, the GC rescue read, the one metadata write and the swap. The leader
replayed nothing during that time; it waited on I/O while the next 64
lands sat in the queue.

The same run refused 483 lands, all in a burst in the first 2 s: before any
batch of a ref ended, `--land-queue-seconds` counted a batch as 1 s, so its
5 s admitted five batches (320 lands) against 1,024 arrivals, though
batches took 0.3 s.

## Decision

### 1. The next batch replays while a batch commits
When a batch has replayed and starts storing (its first commit), it offers
the leader what the next batch may build on: the head its commit will
leave, its staging, and the land ids its moves record. The leader takes
the next batch from the queue at once and runs it in the same task,
alongside the commit. Its first attempt is *speculative*:

- It replays onto the offered head without reading the ref. Its staging is
  layered over the committing batch's (`Staging::over`): blob, changeset,
  row and decoded-node reads fall through to that layer before the stores,
  so copies stack on the batch ahead's unstored copies and the landed-copy
  scan and land-id lookups see its moves (its recorded ids are merged into
  the `find_lands` answer).
- It answers nothing, not even an answer that precedes every move (an
  already-landed changeset, a conflict, a refusal): each rests on a head
  that may not land. It stores nothing.
- Then it waits for the commit ahead. If that applied whole and left no
  land of its batch to retry, and this server still holds the ref's lease
  when any of the lands came through it (ADR 0056), the attempt is kept:
  it detaches from the layer below (now stored, which keeps the chain one
  deep), answers what preceded every move, and commits from the offered
  head with its own flush, which suffices because everything below it is
  stored. Otherwise it is dropped whole: its lands go back into the batch,
  rebase links it read are forgotten, it waits for the batch ahead to end,
  and redoes the attempt from the ref. A dropped attempt does not count
  against the 32 attempts (`attempt -= 1`), and its lands may still be
  dropped as cancelled. That cannot loop: each drop waits for the batch
  ahead to *end*, and after it ended there is nothing ahead to speculate
  on, so the redo reads the ref and counts. The batch ahead itself ends:
  its own attempts are bounded by the 32 attempts, `MAX_COMMIT_ERRORS`
  (three failed commits in a row answer its lands) and
  `MAX_BATCH_LIFETIME`. The bound on the batch behind therefore rests on
  those of the batch ahead; a change that let a batch retry its commit
  without limit would let the batch behind wait without limit too.
- A changeset sweep under way (ADR 0059) needs each land's history rescued
  at a stored head, so a speculative attempt that reads the fence up waits
  for the batch ahead to end and starts from the ref.

At most one batch replays behind the one committing (depth 1): the batch
behind offers its own head only when it starts its commit, which is after
the batch ahead committed. A batch that offers nothing (pipelining off, a
batch with no move, a rebase-onto job) is waited out before the next
starts, as before. When the queue is empty during a commit, the leader
waits for the first land to arrive or the commit to end, so a land that
arrives meanwhile is still pipelined.

Why this is safe:
- **Order and exactly-once.** A batch commits only from the head its
  predecessor's commit left, with a compare-and-swap from that head, and
  only after that commit applied whole. A failed, partial or ambiguous
  commit ahead drops it before any write, so nothing is ever committed on
  a head that did not land, and the redo reads the ref like any attempt,
  finding a stack an ambiguous commit did land. Land ids recorded ahead
  are visible behind (merged), so a resend in the batch behind answers
  from its original's record, and `rebased-origin-v1` rows staged ahead
  are read through the layer, so an unkeyed resend finds its copy. Rows
  read from the layer below are dropped from the rows carried to later
  attempts, as staged rows always were.
- **Durability order** (ADR 0044, 0048, 0068) is per batch and unchanged:
  each batch stores its blobs, then its changesets and rows, then moves
  the ref. The batch behind references objects of the batch ahead, which
  are stored before its own flush starts.
- **Caches.** Process caches hold facts fixed by content, as before. The
  decoded-node memo of the layer below answers the batch behind, so a
  node the batch ahead staged never reaches the cache of stored nodes.
  The fast path's write map (ADR 0066) describes exactly one head and is
  synced against the head of every attempt, so a map advanced by dropped
  replays is restarted, never trusted.
- **Policy** (land policy, review gates) runs per land in the batch behind
  exactly as in any attempt, at the head it replays onto.
- **Lease loss.** A batch kept after its predecessor's commit asks the
  coordinator again before committing when it holds forwarded or leased
  lands; a lost lease drops it and the next attempt hands those lands back
  to be routed (ADR 0056).

`--land-pipeline on|off` (`LEEP_LAND_PIPELINE`, `Repo::with_land_pipeline`)
keeps the old one-batch-at-a-time leader for comparison; answers are the
same either way.

### 2. Batches close after 200 ms of replay
A batch's first attempt stops taking lands once it has replayed for
`--land-batch-replay-ms` (200 ms by default, `off` for none,
`Repo::with_land_batch_replay_limit`) and hands the lands it has not
reached back to the front of the queue, in order, for the next batch. A
batch is bounded by size (`--land-max-batch`, 64) and by time: fast
replays fill it, slow ones (large merges, cold caches after a restart, a
store under load) close it early instead of holding its last land behind
64 of them. The trade-off is more, smaller batches under slow replays:
one ref log write, one pack and one metadata transaction each, every
write smaller. Later attempts of a batch (after a lost swap) keep every
land they replayed, as before.

### 3. Admission before a batch time and with two batches in flight
- Before a ref's first batch ended there is no batch time to turn
  `--land-queue-seconds` into batches, and the seconds refuse nothing. A
  herd at a fresh server is no larger than its clients; the first batch
  to end sets the estimate, and the bound applies from then on.
  `--land-queue-batches` is unaffected. `Retry-After` jitter is unchanged.
- The batch time is the interval between batch ends (the rate the queue
  drains at) rather than each batch's life, which pipelining makes longer
  than that interval.
- Batches in flight count as one batch ahead of a new land, whether one
  replays or one commits while the next replays onto it: the one
  committing ends within the batch time. Counting both would make the
  smallest bound (two batches) refuse every land while batches pipeline.

### Metrics
`leep_land_pipeline_total{outcome="kept"|"discarded"|"fenced"}` and
`leep_land_batch_closed_early_total`.

## Consequences
- Benchmark (`leep-land-bench`, server `taskset -c 0,1`, bench `-c 2,3`,
  Postgres, filesystem blobs, `--agents 1024 --overlap-pct 5 --base-lag 0
  --subtrees 256 --fixture-files 20000 --files 2`, 60 s after a 20 s
  warm-up, fresh database each, alternating; the host was shared with
  other builds, so absolute rates are below the 221 lands/s of an idle
  run):

  | `--land-pipeline` | lands/s | land p50 / p99 | create p50 | queue wait | batch | 503s |
  |---|---|---|---|---|---|---|
  | off | 163, 181, 185 | 3.0 to 4.6 s / 4.4 to 9.7 s | 0.5 to 1.1 s | 2.5 to 4.1 s | 59 to 63 | 5,386, 620, 400 |
  | on | 157, 163, 177, 187 | 1.2 to 1.3 s / 1.7 to 1.9 s | 2.0 to 2.4 s | 90 to 110 ms | 31 to 34 | 752, 416, 0, 0 |

  Throughput did not rise, and the latency moved rather than went away:
  create p50 rose from 0.5 to 1.1 s to 2.0 to 2.4 s as land p50 fell from
  3 to 4.6 s to 1.2 s. With pipelining the queue drains faster than
  lands arrive (half-full batches, a land waits about 0.1 s for its batch)
  and each land answers in a third of the time, but the server's two cores
  were already saturated: the time agents no longer spend queued they
  spend in creates and uploads, whose handlers now get less CPU, and the
  closed-loop rate stays where the cores put it. The leader's idle time
  during a commit was not the bottleneck on this host; CPU spent on
  creates, uploads and replays is. Pipelining pays where the commit's
  latency, not CPU, bounds the rate (slower disks, a remote database or
  object store, more cores). `--land-max-batch 128` changed nothing, since
  pipelined batches do not fill 64, and the default stays 64. The time cap
  closed 27 to 30 batches per run without pipelining and 1 to 18 with it.
  Warm-up refusals are gone: the remaining 503s came later, from the
  seconds bound under a busy host's slow batches.
- Smaller batches cost more per land in the stores, measured over the
  same runs (lands include the warm-up):

  | per 1,000 lands | off | on |
  |---|---|---|
  | batches (metadata transactions: two each) | 16 to 17 (33) | 30 to 33 (61 to 65) |
  | packs written (batches of fewer than 16 objects write loose) | 16 to 17 | 25 to 31 |
  | pack bytes per land | 10.9 to 11.1 KiB | 10.8 to 11.1 KiB |
  | pack write time | 0.40 to 0.60 s | 0.60 to 0.71 s |
  | background repack runs / packs merged | 0.5 to 0.6 / 16 to 19 | 1.9 to 2.1 / 28 to 33 |
  | objects the repack rewrote per land | 8.0 to 9.0 | 7.6 to 8.7 |
  | background repack time | 0.23 to 0.47 s | 0.31 to 0.44 s |

  Ref log rows are one per land either way, so a batch's single
  `cas_ref_logged_chain` writes about half as many rows per statement.
  Bytes stored and objects rewritten per land do not change; what doubles
  is fixed per-batch cost: metadata transactions, pack files and their
  fsyncs, about 0.2 ms per land of pack writing. The background repack
  merges about four times as often (it runs once 32 packs exist) but
  rewrites about as much per land, and took a similar 0.3 to 0.5 ms per
  land. GC's mark and sweep are per changeset and object, not per pack,
  and are unaffected; a collect reads more, smaller packs until the next
  repack. Under full load pipelined batches would fill again and these
  costs fall back to those of full batches.
- A batch holds two stagings in memory while one commits and the next
  replays: about twice the bound ADR 0043 gave, a few tree nodes and a
  changeset per land.
- A commit ahead that fails costs the batch behind its replays. With one
  server per ref (ADR 0056) commits fail only on store errors and races
  with raw ref updates or imports; `discarded` should stay near zero.
- Tests: pipelined batches answer, log and move the ref as landing their
  lands one at a time on randomized concurrent histories (fast and slow
  paths, overlaps, land ids, resends, lazy creates on and off); a lost
  swap, a failed flush and an applied commit ahead with a batch staged
  behind; answers held behind a commit; resends staged behind their
  original; a coordinated leader paused past its lease with batches
  staged; the fault sweep counts kept and dropped speculative attempts;
  the SIGKILL crash test runs pipelined batches of two.
