# 0059. Garbage collection of unreachable changesets
Status: Proposed. Phase 2 approved with changes on 2026-10-22 and built behind the gate of section 11; off by default
Date: 2026-10-21

## Context
Nothing in the metadata store is ever deleted:

- **Changesets.** ADR 0008 defines "live" as "referenced by any stored
  changeset", and ARCHITECTURE §8 says changesets are never deleted. Blob GC
  relies on that. Its incremental mark (ADR 0033) records objects as
  referenced for good, because "a row's reason never goes away".
- **Originals of rebased lands.** A land that replays onto a newer head
  stores two changesets: the client's original and the server's rebased copy.
  The `rebased-origin-v1` row links the copy to the original. The copy's
  `leep.rebased_from` extra names the original too. Each changeset also gets
  its own `root_tree`, `generation`, `skiplist` and `last_merge-v1` rows,
  plus its own path-copied tree nodes in the blob store. Once the land
  answers, nothing structural reaches the original again. Only the link does.
- **The ref log.** It is never pruned (ADR 0018). ADR 0057 made it the store
  of land results as well, at about 120 more bytes per keyed land.

The swarm measured how fast that grows. `docs/PERFORMANCE.md`, "Many-agent
workload", reports 5 to 8 KB of `pg_database_size` growth per land on the
smoke test (table column "DB bytes / write bytes / index bytes per land").
Its subsection "On all of Linux (VPS)" reports 2.1 to 2.4 KB of write-path
logical data per land and "total database growth 6.5 to 22 KB per land
including rows built by reads". ADR 0058 plans for 2,500 lands a second
(50,000 agents at 0.05 lands a second each). That is 216 million lands a
day, or about **1.1 to 4.8 TB of metadata a day** at 5 to 22 KB a land.
ADR 0058's sizing table counts path-history cores. It never sizes storage.

Not all of that growth is unreachable. A rough split of one rebased land at
the VPS figures follows. These are estimates to be replaced by experiment E4
below, not measurements:

| Part | Logical bytes per land | Reachable after the land? |
|---|---|---|
| Rebased copy: changeset row and its `root_tree`, `generation`, `skiplist`, `last_merge-v1` and `rebased-origin-v1` rows | ~0.8–1.0 KB | yes, it is history |
| Original: changeset row and its `root_tree`, `generation`, `skiplist`, `last_merge-v1` and `gc-mark-v1` rows | ~0.7–1.0 KB | no, only by the link |
| Ref log entry, with its land id | ~0.27 KB | it is a root |
| Index rows built by reads and the indexer (history nodes, trigrams, blame, log filter) | 1.1 KB to ~15 KB | mostly rows of reachable history |
| The original's path-copied tree nodes (blob store, not counted above) | several KB on a deep tree, less packed | no |

Postgres turns logical bytes into 2.5 to 3.5 times as much on disk (page,
tuple and index overhead in the same tables). On those estimates,
reclaiming unreachable changesets saves about **2 to 3.5 KB of database per
land** plus the original's tree nodes. Index rows of reachable history are
the larger part on wide trees, and this ADR does not address them (see
"Not addressed"). At 2,500 lands a second the ref log alone is about
58 GB a day. That is a separate and smaller problem, left to a follow-up
ADR (section 9).

## Decision
Changesets that no root can reach, after a grace period, are deleted by a
fenced condemn, fence, collect protocol modelled on ADR 0012. So are their
per-changeset derived rows. The ref log is not pruned, and land ids keep
answering at any age: a keyed retry whose original has been reclaimed is
answered from its record (section 5). Every part is off by default and
starts with a dry run. The scope is phases 1 and 2 of section 9. Ref log
retention and blob-GC mark epochs are follow-up ADRs.

### 1. Roots
A changeset is **reachable** when a root names it or names one of its
descendants. Reachability is the closure of the roots under `parents`. Only
reachable changesets are kept. The roots are exactly:

1. **Ref targets.** Every ref's current target, read at the start of each
   mark.
2. **Ref log entries.** The `old` and `new` targets of every entry. The log
   is never pruned, so every changeset that was ever a ref target stays
   reachable, and `leep undo` and `--to SEQ` keep working on everything.
3. **The landed-copy scan window.** For every ref head *h*: every ancestor
   *c* of *h* with `generation(c) >= generation(h) - 1023` that has a
   `rebased-origin-v1` row makes that row's value, the original, a root.
   - `landed_copy` in `land.rs` walks with a LIFO stack, not in generation
     order. It visits at most `LANDED_COPY_SCAN` (1024) changesets.
   - Each visited node other than the head is pushed by a node visited
     before it, so it lies at most 1023 parent hops from the head.
   - Generation falls by at least 1 per hop.
   - So every copy the scan can reach has a generation of at least
     `generation(h) - 1023`, and the band covers it. The band only moves up
     as the head advances.
4. **Attestations.** Every changeset with a `signature-v1` or `review-v1`
   row. These rows are keyed by `hash(changeset, key id or reviewer)`.
   Policies check them on the original, not on the copy (ARCHITECTURE §10),
   and `GET` reports them through the copy's `via` link. Deleting a signed
   or reviewed original would leave an attestation over an id whose content
   can no longer be shown. Deployments that require signatures or reviews
   therefore keep their originals (open question 2).
5. **Mirror maps.** Every changeset with a `leep-to-git-v1` row. The git
   mirror and `leep-git export` must keep resolving their maps.
6. **Grace.** Every changeset stored less than `--changeset-grace` ago
   (default 7 days). This root covers four things:
   - work in progress, which is identified only by id;
   - `leep sync` and `stash` snapshots (`leep.snapshot`);
   - unlanded stacks;
   - agents that keep committing on an original before syncing to its copy.

   Age comes from the new `stored_at` column (section 2), never from
   `Changeset.timestamp`, which the client chooses.

**Land ids are not a root.** `land_enqueue` (`land.rs`) reads the changeset
it is asked to land before anything else. That read is the only reason a
keyed retry would need its original. Section 5 answers such a retry from
its land id record instead. Keyed and unkeyed originals alike are therefore
kept only by the scan window (root 3), grace (root 6), or an attestation or
mirror map (roots 4 and 5).

**Rule for originals of rebased lands.** An original stays reachable while
its `rebased-origin-v1` link is needed by the landed-copy scan window, or
while it is within grace. Its retention is the maximum of the two, never
less than either.

Everything a root reaches is kept, including every ancestor. A descendant
of an unreachable changeset is itself unreachable, since otherwise its
ancestor would be reachable. So the deleted set is always closed under
descendants. A young changeset (root 6) keeps its whole ancestry. The
grace never protects a child while its parent is deleted.

**What is deleted with an unreachable changeset**, in the same transaction
as its row:

- **Per-changeset rows keyed by its id:**
  - `root_tree`, `generation`, `skiplist`, `last_merge-v1` and
    `gc-mark-v1`;
  - `rebased-origin-v1` when the changeset is itself a copy (an unlanded
    `rebase` result);
  - `history-tree-v4` and `log-filter-v1`, if any were written.

  The ADR 0058 lease holder writes those last two outside any request. Its
  rows are keyed by the changeset, so a late write after the delete leaves
  an orphan row, never a dangling reference. The tombstone pass (section 3)
  removes such rows. It must finish before `gc-fence-active-v1` is cleared.
- **Kept:** the `rebased-origin-v1` row of a *reachable* copy that names a
  deleted original. It is 64 bytes, `via` and `rebase_chain` still show
  where the copy came from, and the original then reads as not found.
- **Left for a rebuild:** content-keyed index rows (`history-node-v4`
  parts, `log-filter-name-v1`, `blob-trigrams-v1`, `tree-trigrams-v1`).
  Their keys can be shared with live history, and deleting them belongs to
  the index lease holder (ADR 0058 §6).
- **Leaked:** `blame` rows. They are keyed by `hash(changeset, path)` and
  cannot be found from the id. Their leak is unbounded until the next
  `repair drop --kind blame`. A new gauge,
  `leep_gc_blame_rows_of_deleted_estimate`, tracks the leak: deleted
  changesets times the blame rows per changeset that `estimate_derived`
  reports.

**Tree nodes and blobs** stay where they are in this ADR. Blob GC's
incremental record (ADR 0033) says "referenced" for good, so nodes that only
deleted changesets used stay marked. `gc --full` ignores the record and
marks from stored changesets only, so it does free them, and section 6
says when it may run. Making that incremental (mark epochs) is a follow-up
ADR.

### 2. Persisted changes (all additive; see `change-persistent-format`)
| Change | Where | Purpose |
|---|---|---|
| `changesets.stored_at BIGINT NULL` (migration 0007, both backends) | `put_changeset*` writes the server's unix seconds; `compact-changesets` rewrites `data` only and keeps the column | Grace. `NULL` (rows from before 0007) counts as stored at the migration's `schema_migrations.applied_at` |
| `cs-reach-v1` derived rows, keyed by changeset id | sweep | Incremental reachability (section 4) |
| `cs-condemned-v1`, `cs-rescue-v1`, `cs-doom-v1` derived rows, keyed by changeset id | sweep, rescuers | The fence (section 3), the same values and rules as `blob-*-v1` (ADR 0012) |
| `gc-fence-active-v1`, one row (zero key): the time the current condemnation round began, or 0 | condemn sets it, collect clears it | Lets servers skip fence reads when no changeset is condemned |
| `MetaStore::delete_changesets(ids, kinds)` | new trait method; the default refuses (like `find_lands`) | Deletes the changesets and their rows of `kinds` in one transaction |

**`stored_at` is a column, decided.** `ALTER TABLE changesets ADD COLUMN
stored_at BIGINT` with no default is a catalog-only change on Postgres and
on SQLite. Old servers never name the column, so they leave it `NULL`, which
reads as old. That errs towards reclaiming, so finish upgrading servers
before the first condemn (section 9). The alternative, a derived row written
with `root_tree`, costs about 50 bytes a changeset against 8, and a
separate read on every sweep.

No `Changeset`, `TreeNode`, `Hash` or hash input changes. No `/v1` field
or endpoint changes.

### 3. The sweep: condemn, fence, collect
`leep-server gc --unreachable` takes the `gc` lease (section 6), then:

1. **Condemn.** Record `started_at` and set `gc-fence-active-v1`. Mark
   reachability (section 4). For every stored changeset that is unmarked and
   older than `started_at - grace`, write `cs-condemned-v1 = max(now,
   previous + 1)`, where `now` is read when the row is written, unless it
   is already condemned. *Amended in review (section 11):* the first
   version stamped `started_at`, so a long mark backdated every row and
   collect's fence ran from before writers could see it. At its end,
   condemn writes its end time into `gc-fence-active-v1`, and collect
   refuses to start until a fence after that. Nothing is deleted.
   `--dry-run` writes nothing at all (section 8).
2. **Fence.** Wait `--fence`. The bound is in "Safety argument" below.
3. **Collect.** Record `m`. Mark reachability again from fresh roots. Then
   take each condemnation at most `m - fence` old, **in descending
   generation order** (children before parents):
   - rescued: skip it;
   - now marked: write the rescue and skip it;
   - otherwise, for each batch: write doom *deleting*, re-read condemnation
     and rescue, mark the rescued ones *abandoned*, `delete_changesets` the
     rest with their rows, then write doom *deleted*.
4. **Tombstone pass.** Doom rows of deleted changesets stay as tombstones.
   Collect then waits ADR 0058's `--index-fence` (60 s), so that any staged
   index write in flight has landed. It deletes every per-changeset row
   keyed by a tombstoned id, then drops tombstones older than one more
   collect.
5. When nothing is left condemned and unresolved, and the tombstone pass
   has finished, collect clears `gc-fence-active-v1`.

Deleting children first means that at every instant the stored changesets
stay closed under ancestry. A backup snapshot, or a crash in the middle of
collect, never sees a changeset without its parent.

**Who rescues.** Any writer that is about to make something durable that
names an existing changeset *X* first reads *X*'s fence state, before its
existence check, in the order ADR 0012 uses. That covers:

- a create with *X* as a parent;
- a land, rebase or merge of *X*;
- any ref move to *X*, through `land`, `cas_ref_checked`, raw
  `POST /v1/refs` or `leep undo`;
- a signature or review on *X*;
- a mirror map write.

If *X* is condemned, the writer writes `cs-rescue-v1` for *X* and for every
condemned ancestor reached from it. That walk is iterative. It follows
parents only while they are condemned, so it costs nothing unless something
is condemned. The writer then reads doom:

- *deleting*: poll for up to 30 s, then answer 409;
- *deleted*: the request fails as if *X* were missing (400 "missing
  parent" or 404). A client that still has the body can
  `POST /v1/changesets` it again under the same id.

A writer never deletes and never resolves anything. A rescue only keeps a
changeset.

**Detached writers re-check per attempt.** `land` batches run in a spawned
task (`tokio::spawn(lead(...))`), not inside the request. A batch drops a
cancelled land only at its first attempt, and may make up to
`MAX_LAND_ATTEMPTS` (32) attempts. So a batch can commit a ref move after
the request that asked for it has timed out. ADR 0012's case (b), "the
write happens within the request timeout of the read", does not hold for
it. Therefore:

- **Each attempt re-checks.** Beside its per-attempt head read, each batch
  attempt re-reads `gc-fence-active-v1`, bypassing the cache. When the flag
  is active, the attempt also reads the fence state of every land's subject
  and its unlanded stack (`new_history`) and rescues as above.
- **The re-check comes before the CAS.** Its read happens before the
  attempt's CAS, and the attempt's own duration is bounded by its replays.
- **Batches have a deadline.** A new constant, `MAX_BATCH_LIFETIME` (60 s),
  bounds a batch from its first attempt. A batch past it answers its lands
  with a retryable error. They are then resent with their land ids, so
  this never causes a double land.
  - While a sweep deletes, a batch whose rescue meets a changeset being
    collected waits for its doom to settle (`wait_for_doom`, up to 30 s per
    rescue call) and replays, so it can hold its ref's queue until the
    deadline fires. Lands on that ref wait meanwhile, then get 409 and
    retry. This is acceptable for phase 1, where nothing condemns; phase 2
    revisits it with measured collect times.
- **Other detached writers re-check too.** The same applies to the ADR 0058
  indexer and to any other detached task that writes something naming a
  changeset: it re-checks before each write batch.
  - An indexer row keyed by a deleted changeset is only an orphan, so the
    indexer needs no rescue. Its late writes are what the tombstone pass
    removes.
  - A writer that would create a reference *to* a changeset (a ref, a
    parent edge, a link) must rescue.

**Fast path.** Requests read `gc-fence-active-v1` at most once every 10 s
and cache the answer. While it is 0, no request reads per-changeset fence
rows, so steady-state creates pay nothing. Batch attempts read the row
uncached, which is one point read per attempt.

**Safety argument.** It is ADR 0012's, with "blob B referenced by stored
changeset X" replaced by "changeset P named by durable state S" (a child, a
ref, a log entry, a link, an attestation). Let *W* be the longest time from
a writer's last fence read to its durable write:

- for requests, the request timeout plus the flag TTL (10 s);
- for land batches, `MAX_BATCH_LIFETIME` plus one flush and
  compare-and-swap: no attempt starts its swap after the batch's first
  attempt plus 60 s, but a swap already started can take the store's
  latency to complete;
- for the indexer, one staged write batch.

Assume:

- **`fence > max(request timeout + TTL, MAX_BATCH_LIFETIME, index write
  window) + skew + margin`.** The default is 10 minutes, against a 5 minute
  timeout, a 60 s batch (plus one swap) and a 60 s index fence.
- **`grace > fence`.**
- **No two GC runs at once.** This is now enforced (section 6).

Then:

- (a) S was durable before collect's mark at `m`. Then P is marked, because
  every kind of S is a root or a parent edge that the mark walks.
- (b) The writer of S last read P's fence state before the condemnation at
  `c`. It wrote S before `c + W < c + fence <= m`, which is case (a).
- (c) It read the state after `c`. Then it saw P rescued or wrote the
  rescue, and the rescue/doom handshake orders it against collect's
  doom/re-read, exactly as for blobs.
- (d) It saw P blocked. It wrote nothing that names P.

A grace-young child keeps its parents through the mark (root 6). The
descendants-first order keeps the stored set closed under ancestry
throughout.

**Land and the land coordinator (ADR 0056).**
- The coordinator lease decides only where a land is batched. The sweep
  never moves a ref and never needs that lease.
- A forwarded land runs in the leader's batch, so the per-attempt re-check
  covers it whichever server forwarded it.
- A stale leader's attempt re-checks before its CAS like any other. If it
  stalls between the re-check and the CAS for longer than the fence, it is
  outside the assumption above, as a VM freeze is in ADR 0012. The fence
  margin covers it.
- The copies a batch writes are new and cannot be condemned. The originals
  become scan-window roots (root 3) as soon as the move commits.

**The indexer lease (ADR 0058).** The index holder derives from ref heads,
whose ancestry is reachable. Content-keyed index rows are left alone
(section 1). The sweep therefore takes no index lease, and ADR 0058 §6's
"GC never deletes what the holder reads" still holds for reachable history.
A query that derives inline on an unreachable id (`log <original>`) may fail
with 404 halfway through, or leave per-changeset rows behind. The tombstone
pass removes those rows.

### 4. Reachability mark
**Reachability is incremental, like ADR 0033.** A `cs-reach-v1` row on *X*
means "*X* is the target of a ref or a log entry, or an ancestor of one, and
every parent of *X* has a row". Rows are written post-order (parents first),
in batches of 1,000, so a crash leaves a closed prefix. The ref log is never
pruned, so a row never goes stale: its reason is a log entry that stays.
That claim depends on the log never being pruned; ref log retention must
deal with it first (section 11).

A run does three things:

- **Ref heads and new log entries.** It walks from every ref head and every
  log entry newer than the last run, through `parents`, iteratively with an
  explicit stack. It stops at rowed changesets.
- **Transient roots.** It walks the scan window, attestations, mirror maps
  and grace from scratch on every run, again stopping at rowed changesets.
  Those sets are small: one band per ref, plus a week of originals and work
  in progress.
- **Candidates.** These are stored changesets with no row and not reached
  from a transient root, found by listing changesets as blob GC does today.

`--full` ignores the rows and walks from the roots.

### 5. Land ids answer without the original
**The change.** In `land_enqueue`, `self.get_changeset(cs)` comes before
anything else. When that read returns `NotFound` and the request carries a
`land_id`, the server asks `find_lands(target_ref, [id])`:

- **Recorded for this changeset:** answer `Landed { landed, rebased:
  landed != changeset }` from the record, exactly as a batch would, and move
  nothing.
- **Recorded for another changeset:** answer 400, as ADR 0057 does.
- **Not recorded:** keep the `NotFound`.

The hot path is unchanged, since a present changeset is read as today. The
miss costs one indexed read on the `ref_log_land_id` index.

**Result.** ADR 0057 and #103 ("answer a retried land from its land id at
any age") hold unchanged:

| Retry | Answer |
|---|---|
| keyed, original present | as today (batch lookup, ADR 0057) |
| keyed, original reclaimed, id recorded | from the record, by the miss path above |
| keyed, original reclaimed, id never recorded (the land never applied) | 404: the land did not happen, and the client can re-create and land it |
| unkeyed, copy within the scan window | from the scan, as today; the window keeps the original |
| unkeyed, past the window, original reclaimed | 404: nothing lands twice |
| unkeyed, past the window, original present (grace, attestation) | lands again, as today without this ADR (ADR 0021's documented limit) |

Deleting originals never makes an unkeyed late retry worse than today.
Where it changes the outcome at all, it turns a double land into a 404.

The ref log keeps every entry. That costs about 270 bytes a land, or 58 GB
a day at 2,500 lands a second. Whether and how to prune it is the
follow-up ADR's question. That ADR must keep keyed lands answerable, for
example with partitioning and a compact id table.

### 6. Operation
- **One GC at a time, enforced.** The changeset sweep and blob `condemn`
  and `collect` take the ADR 0056 lease `scope = 'gc'`, name `store`, with
  a 60 s TTL renewed every 10 s, and refuse to start while another holder
  is live. Today ADR 0012 only assumes this.
- **Backups (ADR 0026), metadata half.** The snapshot is one transaction,
  and every collect batch is one transaction that deletes children first. A
  snapshot therefore holds either a whole changeset with its rows and its
  parents, or none of it.
- **Backups, blob half.** A snapshot taken before a deletion names tree
  nodes that a later `gc --full` may delete before the backup has copied
  them. `gc --full` therefore treats the root trees of changesets
  tombstoned within `--backup-fence` as live. Tombstones keep the deleted
  root tree hash for this.
  - **Where the fence comes from.** The default is derived from the sealed
    backups the operator points at with `--backups DIR`: three times the
    longest run of the last 30 days (manifest write time minus
    `snapshot_taken_at`), and at least one hour.
  - **Without `--backups`,** an explicit `--backup-fence` is required.
  - **There is no measured figure for a large store.** PERFORMANCE.md,
    "Backup and restore", measures a 5,000-changeset store only (8 to
    16 s). A backup of the Linux store on the VPS is pending: it is
    measured and recorded there before phase 2.
  - **A slower backup fails loudly.** One that runs longer than the fence
    still finds the blob missing and is sealed failed (exit 1), which is
    loud, not corrupting. `LeepBackupSlow` already alerts on backup
    duration.
  - The backup pool only grows, so reclaiming in the source does not shrink
    it until a new pool is started (OPERATIONS.md, "Retention").
- **Point-in-time recovery of Postgres alone** to a moment more than
  `--backup-fence` before a `gc --full` can name tree nodes that have since
  been deleted. Restore blobs from bucket versioning too, or restore a
  sealed backup.
- **Restore** copies the fence rows with the snapshot. A collect on the
  restored store re-marks before deleting anything, so it is safe.
- **Postgres space.** Deleted rows are reused by later inserts after
  vacuum. The database stops growing by that amount. It does not shrink
  without `VACUUM FULL` or `pg_repack`.

### 7. Not changed
- Refs move only through `cas_ref` and its logged variants. The sweep never
  moves a ref, and rescues never touch `refs`.
- One ref-log entry per land, written in the same transaction as the move.
  No entry is removed or rewritten.
- Land ids are recorded in the same transaction as the move that lands them
  (ADR 0057), and are answered at any age.
- The server never resolves a conflict. The sweep makes no merges, and a
  rescued changeset is kept exactly as it was.
- No history recursion on the call stack. The reach mark, the rescue walk,
  the descendants-first ordering and the tree walks all use explicit
  stacks or queues.
- Ids, hashes, `Changeset`, `TreeNode` and the `/v1` API are unchanged.
- Blob GC's grace, fence, rescue and doom semantics (ADR 0012) and its
  incremental record (ADR 0033) are unchanged.

### 8. Experiment E4 (this branch): measure before building
`leep-server gc --unreachable --dry-run` counts what this ADR would
reclaim on a real store. It is strictly read-only:

- it refuses a Postgres URL that already sets `options=`;
- it connects to Postgres with `default_transaction_read_only=on`, and
  opens SQLite with `mode=ro`;
- it never derives a missing row.

It reports, from one run:

- the changesets reachable and unreachable, with unreachable ones split
  into originals of rebased lands, snapshots and other work in progress,
  and bucketed by age;
- the reclaimable changesets beyond `--grace`, with their encoded bytes and
  their per-changeset rows by kind;
- path-copied tree nodes and contents, with bytes. By default this is an
  upper bound, from nodes that differ from the first parent's tree and the
  rebased copy's tree;
- the store's logical sizes: rows and bytes per derived kind (one
  `GROUP BY kind`), changesets, refs and ref log;
- per land, side by side and all logical:
  - write-path bytes;
  - index bytes;
  - unreachable bytes.

  The report notes that `pg_database_size` is physical, 2.5 to 3.5 times
  larger.

It also shows, for the follow-up ADR, what a `--ref-log-retention` would
add.

`--exact-trees` makes the tree counts exact by walking every reachable
tree, which costs a full GC mark. **Do not use it on the 2-core VPS.**

On a store whose lands are all recent, such as the swarm's, run it with
`--grace 0`, so that the report shows what is eventually reclaimable per
land:

```sh
nice -n 19 leep-server --meta-pool-size 4 \
  --meta "$LEEP_META" --blobs "$LEEP_BLOBS" \
  gc --dry-run --unreachable --grace 0 --ref-log-retention 0
```

Before phase 1 the dry run aged changesets by their `timestamp`, which is
set by the client. Since phase 1 it ages them by `stored_at`.

**Decision rule.** Build phase 2 if the reclaimable metadata exceeds about
20% of the logical growth per land. Otherwise the effort goes first to the
index rows (see "Not addressed").

### 9. Rollout
| Phase | What | Default |
|---|---|---|
| 0 | E4 dry run (this branch) | read-only tool |
| 1 | Migration 0007 (`stored_at`); `cs-reach-v1`; servers rescue (flag read, handshake, per-attempt batch re-check, `MAX_BATCH_LIFETIME`); `land_enqueue`'s land-id miss path. The dry run switches to `stored_at`. Pending, measured on the VPS before phase 2: the Linux store's backup duration | rescue code active, nothing condemns |
| 2 | `MetaStore::delete_changesets` with its contract tests (the delete path gets its own review gate); `gc --unreachable` condemn, collect and tombstone pass; `gc --full` honours `--backup-fence` | off; `--dry-run` first, on staging, then production |

Upgrade every server to phase 1 before the first condemnation. Servers
built earlier do not rescue, exactly as ADR 0012 required for blobs.
`/v1/ready` lists a `changeset-rescue` feature. `gc --unreachable` refuses
to condemn while any live lease holder it can reach does not list it. That
check is best effort, so the runbook step is still the guarantee.

**Follow-up ADRs, out of this ADR's scope:**
- **Ref log retention.** Pruning, partitioning `ref_log` by `seq` on
  Postgres, and keeping keyed lands answerable.
- **Blob-GC mark epochs.** Make freeing tree nodes of deleted changesets
  incremental instead of `gc --full`.

**Metrics:**
- `leep_gc_changesets_condemned_total`;
- `leep_gc_changesets_deleted_total` and
  `leep_gc_changeset_bytes_deleted_total`;
- `leep_gc_changesets_rescued_total{by=create|land|ref|attest|mirror}`;
- `leep_gc_tombstone_rows_deleted_total`;
- `leep_gc_blame_rows_of_deleted_estimate`;
- `leep_gc_unreachable_last_run_timestamp_seconds`;
- `leep_land_answered_from_record_total{original=missing}`;
- `leep_land_batch_deadline_total`.

**Runbook (RUNBOOKS.md):**
- `LeepChangesetRescueBurst`: agents building on reclaimed originals. Raise
  `--changeset-grace`.
- `LeepUnreachableGcStale`: no sweep for 3 days.
- `LeepBlameLeakHigh`: run `repair drop --kind blame`.

OPERATIONS.md gains a "Changeset garbage collection" section, beside "Blob
garbage collection".

### 10. Testing
- **Contract** (`check_meta_store_contract`, every backend: memory, SQLite,
  Postgres, and each through a quiet `leep-testkit` wrapper):
  - `delete_changesets` deletes the rows and the listed kinds atomically;
  - a repeated `delete_changesets` is a no-op;
  - a wrapper that does not forward it fails loudly;
  - `stored_at` round trip, including `NULL` rows from a v6 database.
- **Fault sweeps**, in `leep-derive`'s `fault_tests`, run with
  `LEEP_FAULT_SEEDS=300 cargo test -p leep-derive fault_tests`, with store
  faults on every call:
  - `unreachable_sweep_never_deletes_a_reachable_changeset`: random swarms
    of keyed and unkeyed lands, stacks, `undo`, branch deletes, snapshots
    and signatures, with condemn and collect at random points. Every root
    and its ancestry still reads, and verify finds no dangling parent;
  - `create_on_condemned_parent_rescues_its_ancestry`;
  - `land_of_condemned_original_is_rescued`;
  - `keyed_retry_of_reclaimed_original_is_answered_from_its_record`;
  - `ref_move_to_condemned_changeset_rescues`;
  - `other_writers_naming_a_condemned_changeset_rescue_it`: merge,
    rebase, signature and review, each on a condemned stack (and, in
    `leep-git`, `export_rescues_condemned_changesets_before_mapping_them`);
  - `cancelled_land_whose_batch_commits_later_is_fenced`: the client's
    request is cancelled after the batch's first attempt, condemn runs, and
    the batch commits on a later attempt. The batch's re-check rescues the
    subject, or the collect leaves it alone;
  - `batch_past_its_deadline_answers_retryably`;
  - `sweep_racing_a_forwarded_land` (phase 2: it needs collect): ADR 0056,
    two simulated servers. The
    land is forwarded to the leader while condemn and collect run, and no
    ref ever points at a deleted changeset;
  - `indexer_write_after_delete_is_removed_by_the_tombstone_pass`;
  - `collect_deletes_children_before_parents`: every intermediate state is
    closed under ancestry;
  - `collect_killed_between_doom_and_delete_resumes`;
  - `incremental_reach_rows_agree_with_full_reachability`, an oracle like
    `check_incremental_mark`;
  - `gc_full_keeps_trees_of_changesets_deleted_within_the_backup_fence`.
- **Crash** (`cargo test -p leep-e2e --test crash`). The scenario rotation
  gains a changeset collect SIGKILLed mid-run beside live keyed lands, and
  a collect SIGKILLed between doom and delete. `LEEP_CRASH_ROUNDS` becomes
  a multiple of 5. After every kill:
  - `verify` must be clean, with a new check: every stored changeset's
    parents are stored;
  - every land answered `landed` must be on its ref exactly once;
  - a rerun collect must finish.
- **Soak and swarm.** `leep-soak` runs the sweep beside the swarm, and
  `leep-swarm` reports `pg_database_size` per land with it on and off.
  Double and lost lands must stay at 0.

### 11. Phase 2: review outcome and what was built
Section 9 gave the delete path its own review gate. The design below was
reviewed on 2026-10-22 and approved with changes; this section records the
decisions and the result, and changes sections 1 to 10 where it says so.

**The gate: the 20% rule, as decided.**
- **Who, where.** The maintainer measures it with
  `scripts/unreachable-gc-gate.sh`: a release build, on Postgres, on a host
  with at least 4 cores, never on the VPS. Twice: on the swarm store after
  at least ten times `LANDED_COPY_SCAN` lands per ref (so the scan window
  does not hide most originals), and on a store restored from a sealed
  backup of the Linux store. Results go into PERFORMANCE.md, "Unreachable
  changesets (E4)".
- **What is counted.** The policy built: ref log kept. Tree counts are
  exact (`--exact-trees`, now exact for that policy too). The metadata
  denominator is the write path plus every ref log entry (not only the
  lands' own) plus index rows, per land.
- **Two ratios.** *Metadata:* reclaimable metadata over that denominator.
  *Combined:* plus the exact reclaimable tree nodes and contents, over the
  same plus the blob store's growth (live plus reclaimable trees and
  contents).
- **Rule.** Enable the sweep in production if the metadata ratio reaches
  20%, or if the combined ratio does **and** `gc --full` can actually be
  scheduled: its measured duration (two marks plus the fence) fits a cron
  interval, and the backup fence fits within the tombstone retention. The
  script prints both ratios and that verdict.
- **Evidence so far.** The Linux store: 6 to 15% metadata, trees as an
  upper bound. A local synthetic store (PERFORMANCE.md, E4b): 19% metadata,
  34% combined, exact trees equal to the bound, and 1,024 originals still
  inside the window. Neither is the gate's measurement.

The code is built behind that gate: it is off by default, and nothing runs
it until an operator schedules it.

**What is deleted.**
| Deleted | How |
|---|---|
| Unreachable changesets older than `--changeset-grace` (section 1, ref log kept) | `MetaStore::delete_changesets`, children first, one transaction per batch |
| Their per-changeset rows: `root_tree`, `generation`, `skiplist`, `last_merge-v1`, `gc-mark-v1`, `rebased-origin-v1` (of an unreachable copy), `history-tree-v4`, `log-filter-v1` | in the same transaction |
| Late per-changeset rows written after the delete, and the deleted ids' `cs-condemned-v1` and `cs-rescue-v1` | the tombstone pass (`MetaStore::delete_derived_keys`), after `--index-fence` |
| `cs-tombstone-v1` and `cs-doom-v1` of deleted ids | the tombstone pass, past `--tombstone-retention` (7 days) |
| Tree nodes and contents only deleted changesets used | only `gc --full`, which keeps those of changesets deleted within `--backup-fence` |
| Ref log entries | **nothing.** Retention stays a follow-up ADR, which must keep keyed lands answerable |

Not deleted: signatures, reviews and mirror maps (their changesets are
roots), content-keyed index rows (left to the index holder), `blame` rows
(leaked; each collect reports an estimate).

A new bookkeeping kind, `cs-tombstone-v1`, keyed by the deleted id, holds
the deletion time and the root tree hash. It is written before the delete,
so `gc --full` can keep the root trees a backup in flight still needs
(section 6).

**Ancestry stays closed: three corrections.** The argument of section 3
needs the stored set closed under ancestry at every instant. Review of
phase 1 found ways to break that once collect deletes:

1. **Rescue order.** `rescue_condemned` wrote its rows in reverse
   discovery order. With two condemned siblings that order puts one before
   their shared parent, and `put_derived_many` may store any prefix on
   error. A writer that failed part way could leave a child rescued and
   its parent condemned. Rows are now written ancestors first, in
   topological order over parent edges.
2. **What collect keeps.** A changeset collect does not delete still
   needs its ancestors: one rescued, one condemned again later than its
   parent and so too young, one a killed condemn never reached. Collect
   now works out, over the whole unreachable set **before its first
   delete**, everything it will not delete and keeps all their condemned
   ancestors. A batch that finds a rescue keeps that changeset's
   ancestors for the rest of the run.
3. **Order by parent edges, not generation.** `generation` and `root_tree`
   rows can be missing. Deletes go in reverse topological order computed
   from parent edges. So a run cut short (`--max-delete`, a failure, a
   kill) leaves only ancestors of what it kept.

`verify` gained `stored_parent_missing`: every stored changeset's parents
are stored, whether or not a ref reaches it.

A fourth fix came from the fault sweeps. Blob GC read a `gc-mark-v1` row
whose value was damaged as no row. One damaged read in condemn and another
in collect then deleted a referenced tree node (seed 2897 of
`gc_under_faults_never_loses_referenced_data` at 3,000 seeds, on `main`).
A present row now counts whatever it reads. `cs-reach-v1` had the same
flaw and the same fix (ADR 0033 is amended).

**One GC at a time, now enforced (section 6).** Blob condemn and collect
and both sweep phases hold the ADR 0056 lease scope `gc`, name `store`
(repository 0), with a 60 s TTL renewed every 10 s. Each refuses, naming
the other job, while another holder is live. A run that cannot renew
stops before its next write. A killed run's lease lapses after 60 s, so
the next cron run may be refused once.

**Fence.**
- **Measured from the row write.** A condemnation is stamped when it is
  written, not when condemn started. Found in review: with condemn at
  02:13, a 25-minute mark and collect at 02:43, a request at 02:37 saw no
  row, the row was written at 02:38 stamped 02:13, and collect deleted
  under the request's ref move. Condemn also leaves its end time in the
  flag, and `--collect-only` refuses until a fence after it. Test:
  `condemnation_is_stamped_when_written_not_when_condemn_started`.
- **Invariant and default.** The invariant is `fence > max(request
  timeout + statement timeout + flag TTL, MAX_BATCH_LIFETIME + statement
  timeout, index fence) + skew`. The default is 15 minutes, as for blobs
  since ADR 0012's statement bound; this replaces section 3's 10 minutes.
- **Refusal.** `gc` now **refuses**, for blobs and for the sweep, a fence
  that the request and statement timeouts it is given do not cover.
  `--unsafe-fence` overrides it, as `--unsafe-no-fence` already did for a
  zero fence. ADR 0012's status line is amended.
- **Batches that wait.** `rescue_land` may rescue twice in one attempt and
  wait up to 30 s each time, so 60 s, past `MAX_BATCH_LIFETIME`. That is
  safe: the deadline is checked again just before the swap, so the attempt
  moves nothing and answers 409, and a keyed land is then answered from its
  record when resent.

**The rest of the safety argument is section 3's.**
- **Concurrent and forwarded lands.** Each batch attempt re-checks before
  its compare-and-swap. A batch has a 60 s deadline, and copies are new.
- **In-flight uploads and creates.** Deleting a changeset deletes no blob.
  A create naming a deleted parent fails with "unknown parent", and the
  client can post the body again. Existence checks that decide a write
  read durable meta.
- **Rescue and doom.** As ADR 0012, per id, batched.
- **Followers (ADR 0056).** Before condemning, the sweep asks every live
  land and index lease holder's `/v1/ready` and refuses if one answers
  without `changeset-rescue`. One it cannot reach is only warned about, so
  the upgrade runbook step stays the guarantee. Leep has no replica
  routing; fence reads go to the primary.
- **Backups and standbys.** Each batch is one transaction, children first,
  so a snapshot or a standby is closed under ancestry. A restored older
  backup brings deleted changesets back as unreachable, and the next sweep
  condemns them again.

**Crash safety and idempotency.**
- Condemn writes only absent condemnations.
- Collect keeps no run state. It finishes a delete a killed run announced
  but never settled, and abandons one whose changeset is stored again.
- `delete_changesets` and `delete_derived_keys` skip what is absent.
- The tombstone pass touches only ids that are not stored. A client may
  have stored a deleted changeset again; then only its tombstone goes.
- The flag is cleared only when nothing stored is left condemned.

**Operator controls and defaults.**
- Off by default. Run `gc --unreachable --dry-run` first, then
  `--condemn-only` and `--collect-only` as two cron jobs, at least
  `--fence` apart.
- Defaults: `--changeset-grace 7d`, `--fence 15m`, `--batch 500`,
  `--rate 500` (changesets a second), `--max-delete 100000`,
  `--index-fence 60s`, `--tombstone-retention 7d`.
- `gc --full` requires `--backups DIR` or `--backup-fence`.

**Metrics.**
- **Servers.** `leep_gc_changesets_rescued_total{by}`,
  `leep_gc_changeset_doom_conflicts_total` and the gauge
  `leep_gc_fence_active`.
- **The sweep.** It is a cron job with no scrape endpoint, so
  `--metrics-file` writes a Prometheus textfile of the last run:
  - `leep_gc_unreachable_last_run_timestamp_seconds`;
  - `leep_gc_changesets_condemned`, `leep_gc_changesets_deleted` and
    `leep_gc_changeset_bytes_deleted`;
  - `leep_gc_changesets_abandoned` and
    `leep_gc_changesets_kept_by_descendant`;
  - `leep_gc_changesets_condemned_pending`,
    `leep_gc_changesets_left_by_cap` and `leep_gc_tombstone_rows_deleted`;
  - `leep_gc_changeset_collect_batch_seconds_max` and
    `leep_gc_blame_rows_of_deleted_estimate`.
- These replace the `_total` counters section 9 named for the sweep.

**Alerts and runbooks:**
- `LeepUnreachableGcStale`;
- `LeepChangesetRescueBurst`;
- `LeepChangesetDoomStuck` (writers getting 409 on a delete announced
  and never settled);
- `LeepGcFenceFlagStuck`;
- `LeepChangesetCollectSlow`.

The blame leak has no alert. Each collect reports its estimate, and the
runbook covers `repair drop --kind blame`.

**`cs-reach-v1` depends on the ref log.** "A row never goes stale"
(section 4) holds only because the ref log is never pruned. The follow-up
ADR on ref log retention must invalidate rows whose only reason was a
pruned entry (for example, rewrite them with `--full`) before it prunes
anything.

**Tests.**
- **Contract**, on every backend and through the fault wrapper: atomic
  delete, idempotent repeats, and a non-forwarding wrapper refusing.
- **Fault sweeps** (`LEEP_FAULT_SEEDS=300`): section 10's list, plus
  - `rescue_rows_are_written_ancestors_first`;
  - `collect_keeps_ancestors_of_every_changeset_it_skips`;
  - `collect_stopped_by_max_delete_is_closed_under_ancestry`;
  - `collect_orders_without_generation_rows`;
  - `blob_gc_refuses_during_changeset_collect`;
  - `get_of_deleted_original_is_404_and_copy_via_answers`;
  - `restored_backup_recondemns`.
- **Crash test.** It SIGKILLs a collect at each of a batch announced but
  not deleted, a batch deleted but not settled, and the tombstone pass. A
  hidden `--debug-pause-at` stops it exactly there. After each kill,
  `verify --full` must be clean.

**Changed from the reviewed design:**
- The ordering and kept propagation use parent edges over the whole set
  (review item 2).
- The `--rate` default is 500, not 2,000.
- The metrics are textfile gauges, not counters.
- There is no blame-leak alert.
- A tombstone kind was added.
- The mark-row fix is new.

## Alternatives considered## Alternatives considered
| Alternative | Why not (now) |
|---|---|
| **Keep originals while their land id is retained (a land-id root)** | With the log never pruned, every keyed original, which is almost all of them, would be kept for good. Reclaiming them would then need ref log pruning, weakening ADR 0057. The miss path of section 5 answers the same retries without the original |
| **Never store originals of rebased lands** (land takes the body, or deletes the original at once) | Policies check signatures and reviews on the original, the scan and the client's own references name it, and a stack's lower entries are ordinary stored changesets. Changing that is an API change, and it loses the audit trail. A grace-delayed delete gives most of the saving with none of that |
| **Delete originals immediately after the land commits** | It races retries, the scan and agents still building on the original. Grace and the fence exist for exactly these |
| **Time-partitioned `changesets` table** (drop old partitions) | Originals and their live copies land in the same time slices, so no partition is ever wholly garbage. It suits the append-only `ref_log` (follow-up ADR), not changesets |
| **An archival tier** (move unreachable changesets and their nodes to cold object storage) | It keeps everything, at a cheaper price per byte, and needs a read fallback on every miss. Worth revisiting if audit requires keeping originals. It can reuse this ADR's roots and fence, with "move" in place of "delete" |
| **Reference counts on changesets and tree nodes** | Every create and land would update counts on shared rows: hot-row contention at 2,500 lands a second, and counts that are wrong after a crash. A mark costs one batched pass per run instead |
| **Reachability from refs only (no log, no window)** | Undo and `via` break. That is what ADR 0008 warned about |

## Not addressed
- **Index rows of reachable history:**
  - trigram rows for every changed directory version;
  - history packs;
  - `blame` rows of heads nobody will ask about again.

  On wide trees these are most of the 6.5 to 22 KB per land. That needs a
  retention and eviction design of its own for caches (`blame`) and
  compaction for indexes, owned by the ADR 0058 holder.
- **Ref log growth and blob-GC epochs:** follow-up ADRs (section 9).
- **Multiple repositories (ADR 0054):** roots become per repository, and
  membership tables join the roots. That is designed in when ADR 0054 is
  implemented.

## Open questions
1. **Defaults.** Is a 7-day changeset grace right for agents that keep
   building on their originals, or for snapshots kept by clients?
2. **Attested originals.** Keeping every signed or reviewed original means
   protected refs reclaim nothing. Is the `signature-v1` row plus the
   copy's `rebased-origin-v1` link enough audit trail? A signature covers
   only the id, and the copy carries the content. If so, drop root 4 for
   originals of landed copies.
3. **Snapshots.** Should `leep.snapshot` changesets get a longer grace than
   other work in progress, or a client-renewed hold?
4. **E4 numbers.** Does unreachable data reach the 20% threshold on the
   swarm store? Is the upper bound for tree nodes close enough to the
   exact count, measured on a larger host than the VPS, to judge the
   epochs ADR?
5. **Phase 2 gate (section 11).** Decided in review: the combined ratio
   counts only if `gc --full` can be scheduled; the sweep refuses an unsafe
   fence; the maintainer measures the gate (section 11).
6. **Number.** 0059 was the next free ADR number on `main` and on every
   origin branch on 2026-10-21, including
   `derive/index-lease-and-bounded-queries`. Renumber if another ADR merges
   first.

## Consequences
- Metadata growth per land falls by the unreachable share, which E4
  measures. Blob-store growth falls only when `gc --full` runs, until the
  epochs ADR.
- "Changesets are never deleted" stops being true. The places that say so
  are updated in the implementing changes:
  - ARCHITECTURE §8;
  - ADR 0008, 0033 and 0058 §6;
  - FORMATS.md "Ref log";
  - OPERATIONS.md "Ref log".

  An unreachable changeset older than grace may answer 404, but a keyed
  land retry still gets its original answer.
- Requests gain a cached flag read. Land batch attempts gain one uncached
  point read. Only while a sweep is between condemn and collect do writers
  also read fence rows for the changesets they rely on.
- Land batches gain a 60 s lifetime, which bounds the fence and also caps
  how long a land waits behind replays.
