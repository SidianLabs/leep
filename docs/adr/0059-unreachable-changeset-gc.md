# 0059. Garbage collection of unreachable changesets
Status: Proposed
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
   older than `started_at - grace`, write `cs-condemned-v1 =
   max(started_at, previous + 1)`, unless it is already condemned. Nothing
   is deleted. `--dry-run` writes nothing at all (section 8).
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

## Alternatives considered
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
5. **Number.** 0059 was the next free ADR number on `main` and on every
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
