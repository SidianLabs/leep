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
58 GB a day, so OPERATIONS.md's "a few tens of MB a year" no longer holds at
swarm rates.

## Decision
Changesets that no root can reach, after a grace period, are deleted by a
fenced condemn, fence, collect protocol modelled on ADR 0012. So are their
per-changeset derived rows. Tree nodes and blobs that only those changesets
referenced follow through blob GC under mark epochs. The ref log may be
pruned to a retention period, off by default. Every part is off by default
and starts with a dry run.

### 1. Roots
A changeset is **reachable** when a root names it or names one of its
descendants. Reachability is the closure of the roots under `parents`. Only
reachable changesets are kept. The roots are exactly:

1. **Ref targets.** Every ref's current target, read at the start of each
   mark.
2. **Ref log entries within retention.** The `old` and `new` targets of every
   retained entry (section 5). With retention off, that is every entry ever
   written, so every changeset that was ever a ref target stays reachable.
   `leep undo` and `--to SEQ` keep working on everything they can still
   name.
3. **Land ids.** For every retained entry that records a land id (ADR 0057),
   the entry's `land_changeset`, which is the original the client asked to
   land. The record answers a retry with ids alone (`LandRecord { changeset,
   landed, seq }`), but `land` reads the changeset it is asked to land
   before it reaches the lookup. So the original must exist for as long as
   the id can be answered. Its ancestors that are not on the ref (the rest
   of its stack) follow by closure.
4. **The landed-copy scan window.** For every ref head *h*: every ancestor
   *c* of *h* with `generation(c) >= generation(h) - LANDED_COPY_SCAN`
   (1024) that has a `rebased-origin-v1` row makes that row's value, the
   original, a root. The scan in `land.rs` visits at most 1024 changesets,
   each strictly lower in generation than the one before. Every copy it can
   find therefore lies in this band, and the band only moves up as the head
   advances.
5. **Attestations.** Every changeset with a `signature-v1` or `review-v1`
   row. These rows are keyed by `hash(changeset, key id or reviewer)`.
   Policies check them on the original, not on the copy (ARCHITECTURE §10),
   and `GET` reports them through the copy's `via` link. Deleting a signed
   or reviewed original would leave an attestation over an id whose content
   can no longer be shown. Deployments that require signatures or reviews
   therefore keep their originals (open question 2).
6. **Mirror maps.** Every changeset with a `leep-to-git-v1` row. The git
   mirror and `leep-git export` must keep resolving their maps.
7. **Grace.** Every changeset stored less than `--changeset-grace` ago
   (default 7 days). This root covers four things:
   - work in progress, which is identified only by id;
   - `leep sync` and `stash` snapshots (`leep.snapshot`);
   - unlanded stacks;
   - agents that keep committing on an original before syncing to its copy.

   Age comes from a new `stored_at` column (section 2), never from
   `Changeset.timestamp`, which the client chooses.

**Rule for originals of rebased lands.** An original stays reachable while
its `rebased-origin-v1` link is needed by the landed-copy scan window
(root 4), or by a land id recorded for it (root 3), or while it is within
grace (root 7). Its retention is therefore the maximum of the ref log
retention, the scan window and the grace, never less than any of them.
With ref log retention off, every original of a keyed land is kept for
good. Reclaiming keyed originals needs a retention, and section 5 sets out
that trade-off.

Everything a root reaches is kept, including every ancestor. A descendant
of an unreachable changeset is itself unreachable, since otherwise its
ancestor would be reachable. So the deleted set is always closed under
descendants. A young changeset (root 7) keeps its whole ancestry. The
grace never protects a child while its parent is deleted.

**What is deleted with an unreachable changeset**, in the same transaction
as its row:

- **Per-changeset rows keyed by its id:** `root_tree`, `generation`,
  `skiplist`, `last_merge-v1`, `history-tree-v4`, `log-filter-v1`,
  `gc-mark-v1`, and `rebased-origin-v1` when the changeset is itself a copy
  (an unlanded `rebase` result). Plus this ADR's own fence rows once the
  delete is final (section 3).
- **Kept:** the `rebased-origin-v1` row of a *reachable* copy that names a
  deleted original. It is 64 bytes, `via` and `rebase_chain` still show
  where the copy came from, and the original then reads as not found.
- **Left for a rebuild:**
  - content-keyed index rows (`history-node-v4` parts,
    `log-filter-name-v1`, `blob-trigrams-v1`, `tree-trigrams-v1`). Their
    keys can be shared with live history, and deleting them belongs to the
    index lease holder (ADR 0058 §6). A later kind rebuild under that lease
    drops the ones nothing uses.
  - `blame` rows. They are keyed by `hash(changeset, path)` and cannot be
    found from the id. Leaked rows of deleted changesets go with the next
    `repair drop --kind blame`.

**Tree nodes and blobs.** ARCHITECTURE §8 has blob GC mark from every
stored changeset. Once a changeset row is gone, blob GC stops marking its
root tree. The nodes and contents that only deleted changesets referenced
become unmarked, and condemn and collect reclaim them as they do any other
garbage. The incremental record of ADR 0033 is what stands in the way: its
rows say "referenced" for good. Section 4 bounds their lifetime with mark
epochs.

### 2. Persisted changes (all additive; see `change-persistent-format`)
| Change | Where | Purpose |
|---|---|---|
| `changesets.stored_at BIGINT NULL` (migration 0007, both backends) | `put_changeset*` writes the server's unix seconds; `compact-changesets` rewrites `data` only and keeps the column | Grace. `NULL` (rows from before 0007) counts as stored at the migration's `schema_migrations.applied_at` |
| `cs-reach-v1` derived rows, keyed by changeset id, value = mark epoch | sweep | Incremental reachability (section 4) |
| `cs-condemned-v1`, `cs-rescue-v1`, `cs-doom-v1` derived rows, keyed by changeset id | sweep, rescuers | The fence (section 3), the same values and rules as `blob-*-v1` (ADR 0012) |
| `gc-fence-active-v1`, one row (zero key): the time the current condemnation round began, or 0 | condemn sets it, collect clears it | Lets servers skip fence reads when no changeset is condemned |
| `gc-epoch-v1`, one row (zero key): the current mark epoch | blob GC | Section 4 |
| `gc-mark-v1` value: `0x01` stays epoch 1; later epochs are 8 bytes big-endian | blob GC | Section 4. Old binaries read any other value as "no row" and walk more, which is safe |
| `MetaStore::delete_changesets(ids, kinds)` | new trait method; the default refuses (like `find_lands`) | Deletes the changesets and their rows of `kinds` in one transaction |
| `MetaStore::prune_ref_log(before_time, limit)` | new trait method; the default refuses | Section 5 |
| `MetaStore::ref_log` entries gain `land_changeset: Option<ChangesetId>` (`#[serde(default)]`) | read side only | Root 3 needs it. The column already exists (migration 0005) |
| `GET /v1/ready` gains `land_id_retention_secs: Option<u64>` (`#[serde(default)]`) | API | Section 5 |

No `Changeset`, `TreeNode`, `Hash` or hash input changes. No `/v1` field is
removed or renamed.

### 3. The sweep: condemn, fence, collect
`leep-server gc --unreachable` takes the `gc` lease (section 6), then:

1. **Condemn.** Record `started_at` and set `gc-fence-active-v1`. Mark
   reachability (section 4). For every stored changeset that is unmarked and
   older than `started_at - grace`, write `cs-condemned-v1 =
   max(started_at, previous + 1)`, unless it is already condemned. Nothing
   is deleted. `--dry-run` writes nothing at all (section 8).
2. **Fence.** Wait `--fence`. The default is 10 minutes, as for blobs, and
   it must exceed the servers' `--request-timeout`, plus clock skew, plus
   the flag cache TTL (10 s).
3. **Collect.** Record `m`. Mark reachability again from fresh roots. Then
   take each condemnation at most `m - fence` old, **in descending
   generation order** (children before parents):
   - rescued: skip it;
   - now marked: write the rescue and skip it;
   - otherwise, for each batch: write doom *deleting*, re-read condemnation
     and rescue, mark the rescued ones *abandoned*, `delete_changesets` the
     rest with their rows, then write doom *deleted*.

   Doom rows of deleted changesets stay as tombstones for `--backup-fence`
   (section 6). A later collect uses them to delete per-changeset rows that
   a slow derivation wrote after the delete (keyed by a tombstoned id), and
   then drops them.
4. When nothing is left condemned and unresolved, collect clears
   `gc-fence-active-v1`.

Deleting children first means that at every instant the stored changesets
stay closed under ancestry. A backup snapshot, or a crash in the middle of
collect, never sees a changeset without its parent.

**Who rescues.** Any request that is about to make something durable that
names an existing changeset *X* first reads *X*'s fence state, before its
existence check, in the order ADR 0012 uses. That covers:

- a create with *X* as a parent;
- a land, rebase or merge of *X*;
- any ref move to *X*, through `land`, `cas_ref_checked`, raw
  `POST /v1/refs` or `leep undo`;
- a signature or review on *X*;
- a mirror map write.

If *X* is condemned, the request writes `cs-rescue-v1` for *X* and for
every condemned ancestor reached from it. That walk is iterative. It
follows parents only while they are condemned, so it costs nothing unless
something is condemned. The request then reads doom:

- *deleting*: poll for up to 30 s, then answer 409;
- *deleted*: the request fails as if *X* were missing (400 "missing
  parent" or 404). Unlike a blob, a changeset cannot be rewritten by the
  server. A client that still has the body can `POST /v1/changesets` it
  again under the same id.

A request never deletes and never resolves anything. A rescue only keeps
a changeset.

**Fast path.** Servers read `gc-fence-active-v1` at most once every 10 s
and cache the answer. While it is 0, no request reads per-changeset fence
rows, so steady-state lands and creates pay nothing. Condemn sets the flag
before the first condemnation, and the fence exceeds TTL + timeout + skew.
So a request that acted on a stale "inactive" made its write before
collect's mark began. That is case (a) of ADR 0012's proof.

**Safety argument.** It is ADR 0012's, with "blob B referenced by stored
changeset X" replaced by "changeset P named by durable state S" (a child, a
ref, a log entry, a link, an attestation). The assumptions are the same:
`fence > request timeout + skew + flag TTL`, `grace > request timeout`, and
no two GC runs at once (now enforced, section 6).

- (a) S was durable before collect's mark at `m`. Then P is marked, because
  every kind of S is a root or a parent edge that the mark walks.
- (b) The request that made S read P's fence state before the condemnation
  at `c`. It wrote S before `c + timeout < m`, which is case (a).
- (c) It read the state after `c`. Then it saw P rescued or wrote the
  rescue, and the rescue/doom handshake orders it against collect's
  doom/re-read, exactly as for blobs.
- (d) It saw P blocked. It failed and wrote nothing.

A grace-young child keeps its parents through the mark (root 7). The
descendants-first order keeps the stored set closed under ancestry
throughout.

**Land and the land coordinator (ADR 0056).**
- A land rescues its subject and the subject's unlanded stack
  (`new_history`) before replaying. With the flag clear that is one cached
  read.
- The coordinator lease decides only where a land is batched. The sweep
  never moves a ref and never needs that lease. A stale leader's replay is
  an ordinary request under the request timeout.
- A forwarded land is fenced by the leader's request timeout, and the
  follower's timeout covers its wait.
- The copies a batch writes are new and cannot be condemned. The originals
  become scan-window roots (root 4) as soon as the move commits.

**The indexer lease (ADR 0058).** The index holder derives from ref heads,
whose ancestry is reachable. It never reads an unreachable changeset, and
the sweep never deletes a row the holder depends on. Content-keyed index
rows are left alone (section 1). The sweep therefore takes no index lease,
and ADR 0058 §6's "GC never deletes what the holder reads" still holds for
reachable history. A query that derives inline on an unreachable id (`log
<original>`) may fail with 404 halfway through, or leave per-changeset rows
behind. The tombstone pass removes those rows.

### 4. Reachability mark and blob-GC epochs
**Reachability is incremental, like ADR 0033.** A `cs-reach-v1` row on *X*
means "*X* is the target of a ref or a retained log entry, or an ancestor of
one, and every parent of *X* has a row". Rows are written post-order
(parents first), in batches of 1,000, so a crash leaves a closed prefix. A
run does three things:

- **Ref heads and new log entries.** It walks from every ref head and every
  log entry newer than the last run, through `parents`, iteratively with an
  explicit stack. It stops at rowed changesets.
- **Transient roots.** It walks the land ids, the scan window, attestations,
  mirror maps and grace from scratch on every run, again stopping at rowed
  changesets. Those sets are small: one band per ref, plus a few days of
  originals and work in progress.
- **Candidates.** These are stored changesets with no row and not reached
  from a transient root, found by listing changesets as blob GC does today.

A row can outlive its reason when a ref is rewound or deleted and the
entries naming the old part are later pruned. That stale row keeps more,
never less. `--full` ignores the rows and walks from the roots, in a new
reach epoch.

**Blob GC epochs.** A `gc-mark-v1` row counts only if its value equals the
epoch in `gc-epoch-v1`, so within an epoch ADR 0033's closure argument holds
unchanged. Changeset deletions make rows of earlier epochs untrustworthy as
"still referenced". `leep-server gc --new-epoch` is how tree nodes and
contents freed by deletions become collectable:

- it bumps the epoch;
- its mark walks every stored changeset once, like a first run after
  ADR 0033, rewriting live rows in place;
- every later run is incremental within the epoch.

It refuses to start until the newest changeset deletion is `--backup-fence`
old (section 6). When collect deletes a blob or tree node, it also deletes
the object's `gc-mark-v1` and trigram rows. Fence rows of deleted blobs stay
as ADR 0012 requires. On large stores epochs run weekly or monthly. The
changeset sweep itself runs daily.

### 5. Ref log retention and land ids
**Policy.** `leep-server gc --prune-ref-log --ref-log-retention 30d` is off
by default, and nothing prunes without it. It deletes entries older than
the retention, in batches. It always keeps each ref's newest entry, so
`leep undo` of the last move always works, and `verify` replays each ref
from its oldest retained entry's `old` value. A pruned entry takes its land
id with it. Pruning is a `DELETE` in batches on SQLite and on Postgres
until the table is partitioned. At swarm rates Postgres should partition
`ref_log` by `seq` range, so that pruning drops whole partitions with no
vacuum debt (phase 4).

**Effect on land ids (#103: "answer a retried land from its land id at any
age").** With retention off, nothing changes: an id is answered at any age,
and keyed originals are never reclaimed. With retention *R*:

| Retry age | Answer |
|---|---|
| under *R* | from the recorded id, as today; the original is kept (root 3) |
| over *R*, copy still in the 1024-changeset scan window | from the landed-copy scan, as for unkeyed lands |
| over *R* and out of the window, original not yet deleted (still within grace, or attested) | **the land happens again**: a double land, the failure ADR 0057 exists to prevent |
| over *R*, out of the window, original deleted | 404 for the changeset; nothing lands |

The trade-off is plain: #103's "at any age" becomes "within *R*". Two
rules keep the third row from happening to well-behaved clients:

- **Retention is never shorter than grace.** The gc command refuses
  `--ref-log-retention` below `--changeset-grace`. An original that
  outlives its entry is then attested, a mirror target, or condemned
  shortly after.
- **The server advertises *R*.** `GET /v1/ready` reports
  `land_id_retention_secs`. `leep land` and `leep-mcp` keep a saved land's
  creation time in `pending_land`. They do not resend a saved land older
  than *R* minus one day. They clear it and print the existing message
  ("may have applied; check `leep reflog REF`"). Clients from before this
  change see no field. A server with retention on therefore also stops
  listing the `land-id` feature for resends across runs until a newer
  `features` value (`land-id-retention`) is listed. Old clients then treat
  it as ADR 0057's "server does not record ids" case, which is safe.

As defence in depth, `land` will consult `find_lands` before it requires
the subject changeset to exist, so a keyed retry is answered from its
record even when the original was reclaimed early by a misconfiguration.

The retained log still costs about 270 bytes per land: about 1.75 TB at
2,500 lands a second with *R* = 30 days. Operators at that rate choose *R*
from that figure. OPERATIONS.md's sizing sentence is corrected in the
implementing change.

### 6. Operation
- **One GC at a time, enforced.** The changeset sweep, blob `condemn` and
  `collect`, epoch rollovers and ref-log pruning all take the ADR 0056
  lease `scope = 'gc'`, name `store`, with a 60 s TTL renewed every 10 s,
  and refuse to start while another holder is live. Today ADR 0012 only
  assumes this.
- **Backups (ADR 0026).**
  - The metadata snapshot is one transaction, and every collect batch is
    one transaction that deletes children first. A snapshot therefore holds
    either a whole changeset with its rows and its parents, or none of it.
  - The open risk is the *blob* half. A snapshot taken before a deletion
    names tree nodes that an epoch's collect may delete before the backup
    has copied them. Epochs start only after `--backup-fence` (default 24h,
    longer than the slowest backup) has passed since the newest deletion,
    and collect waits `--fence` after that. A backup that runs longer than
    that finds the blob missing and is sealed failed (exit 1), which is
    loud, not corrupting.
  - The backup pool only grows, so reclaiming in the source does not shrink
    it until a new pool is started (OPERATIONS.md, "Retention").
- **Point-in-time recovery of Postgres alone** to a moment more than
  `--backup-fence` before now can name tree nodes that epochs have since
  deleted. Restore blobs from bucket versioning too, or restore a sealed
  backup.
- **Restore** copies the fence rows with the snapshot. A collect on the
  restored store re-marks before deleting anything, so it is safe.
- **Postgres space.** Deleted rows are reused by later inserts after
  vacuum. The database stops growing by that amount. It does not shrink
  without `VACUUM FULL` or `pg_repack`.

### 7. Not changed
- Refs move only through `cas_ref` and its logged variants. The sweep never
  moves a ref. Rescues and pruning never touch `refs`.
- One ref-log entry per land, written in the same transaction as the move.
  Pruning removes old entries. It never rewrites, merges or reorders them.
- Land ids are recorded in the same transaction as the move that lands them
  (ADR 0057). They are only ever removed together with their entry.
- The server never resolves a conflict. The sweep makes no merges, and a
  rescued changeset is kept exactly as it was.
- No history recursion on the call stack. The reach mark, the rescue walk,
  the descendants-first ordering and the tree walks all use explicit
  stacks or queues.
- Ids, hashes, `Changeset`, `TreeNode` and the `/v1` API are unchanged,
  apart from additive fields.
- Blob GC's grace, fence, rescue and doom semantics (ADR 0012) are
  unchanged. Epochs only bound how long a mark row is trusted.

### 8. Experiment E4 (this branch): measure before building
`leep-server gc --unreachable --dry-run` is read-only, and opens Postgres
with `default_transaction_read_only`. It reports what this ADR would reclaim on a real store, under two policies at
once: ref log unpruned, and ref log kept `--ref-log-retention` (default
30d). For each policy it reports:

- the changesets reachable and unreachable, with unreachable ones split
  into originals of rebased lands, snapshots and other work in progress,
  and bucketed by age;
- the reclaimable changesets beyond `--grace`, with their encoded bytes;
- per-changeset derived rows by kind, with logical bytes;
- the ref-log entries and bytes that retention would prune;
- path-copied tree nodes and contents, with bytes. By default this is an
  upper bound, from nodes that differ from the first parent's tree and the
  rebased copy's tree. `--exact-trees` gives the exact count for the
  retention policy by walking every reachable tree (the cost of a
  `gc --full` mark);
- totals per land, divided by the ref log's `land` entries.

On a store whose lands are all recent, such as the swarm's, run it with
`--grace 0 --ref-log-retention 0`. Every entry but each ref's newest then
counts as pruned, so the retention column shows what is eventually
reclaimable per land once grace and retention have passed:

```sh
nice -n 19 leep-server --meta-pool-size 4 --meta "$LEEP_META" --blobs "$LEEP_BLOBS" \
  gc --dry-run --unreachable --grace 0 --ref-log-retention 0
```

The `stored_at` column does not exist yet, so the dry run ages changesets
by their `timestamp`, which is set by the client. Its land-id roots
approximate `land_changeset` by the `rebased-origin-v1` origin of each
`land` entry's target, which includes unkeyed lands and so keeps more.

**Decision rule.** Build phases 1 to 3 if, at the swarm's ref-log
retention, the reclaimable metadata exceeds about 20% of growth per land.
Otherwise the effort goes first to the index rows (see "Not addressed").

### 9. Rollout
| Phase | What | Default |
|---|---|---|
| 0 | E4 dry run (this branch) | read-only tool |
| 1 | Migration 0007 (`stored_at`); `cs-reach-v1`; `delete_changesets` and `prune_ref_log` with contract tests; servers rescue (flag read, handshake); `/v1/ready` field. The dry run switches to `stored_at` | rescue code active, nothing condemns |
| 2 | `gc --unreachable` condemn and collect | off; `--dry-run` first, on staging, then production |
| 3 | `gc --prune-ref-log --ref-log-retention`; client changes in `leep` and `leep-mcp` | off |
| 4 | Blob-GC epochs (`--new-epoch`); `ref_log` partitioning on Postgres | off |

Upgrade every server to phase 1 before the first condemnation. Servers
built earlier do not rescue, exactly as ADR 0012 required for blobs.
`gc --unreachable` refuses to condemn while `/v1/ready` from any peer it
can see in the `leases` table lacks the `changeset-rescue` feature. That
check is best effort, so the runbook step is still the guarantee.

**Metrics:**
- `leep_gc_changesets_condemned_total`;
- `leep_gc_changesets_deleted_total` and `leep_gc_changeset_bytes_deleted_total`;
- `leep_gc_changesets_rescued_total{by=create|land|ref|attest|mirror}`;
- `leep_gc_ref_log_pruned_total`;
- `leep_gc_unreachable_last_run_timestamp_seconds`;
- `leep_land_retry_past_retention_total`, for a keyed land answered by the
  scan or 404 because its entry was pruned.

**Runbook (RUNBOOKS.md):**
- `LeepChangesetRescueBurst`: agents building on reclaimed originals. Raise
  `--changeset-grace`.
- `LeepUnreachableGcStale`: no sweep for 3 days.
- `LeepLandRetryPastRetention`: clients retrying past *R*. Check their
  versions and raise *R*.

OPERATIONS.md gains a "Changeset garbage collection" section, beside "Blob
garbage collection" and "Ref log".

### 10. Testing
- **Contract** (`check_meta_store_contract`, every backend: memory, SQLite,
  Postgres, and each through a quiet `leep-testkit` wrapper):
  - `delete_changesets` deletes the rows and the listed kinds atomically;
  - a repeated `delete_changesets` is a no-op;
  - a wrapper that does not forward it fails loudly;
  - `stored_at` round trip, including `NULL` rows from a v6 database;
  - `prune_ref_log` keeps each ref's newest entry, takes ids with entries
    (`find_lands` then answers nothing), and never touches `refs`;
  - the new `land_changeset` field on entries.
- **Fault sweeps**, in `leep-derive`'s `fault_tests`, run with
  `LEEP_FAULT_SEEDS=300 cargo test -p leep-derive fault_tests`, with store
  faults on every call:
  - `unreachable_sweep_never_deletes_a_reachable_changeset`: random swarms
    of keyed and unkeyed lands, stacks, `undo`, branch deletes, snapshots
    and signatures, with condemn and collect at random points. Every root
    and its ancestry still reads, and verify finds no dangling parent;
  - `create_on_condemned_parent_rescues_its_ancestry`;
  - `land_of_condemned_original_is_rescued_or_answered_by_id`;
  - `ref_move_to_condemned_changeset_rescues`;
  - `collect_deletes_children_before_parents`: every intermediate state is
    closed under ancestry;
  - `collect_killed_between_doom_and_delete_resumes`;
  - `land_id_answers_survive_the_sweep_within_retention`;
  - `pruned_ref_log_retry_lands_at_most_once`;
  - `incremental_reach_rows_agree_with_full_reachability`, an oracle like
    `check_incremental_mark`;
  - `epoch_rollover_frees_only_exclusive_tree_nodes`.
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
| **Never store originals of rebased lands** (land takes the body, or deletes the original at once) | Policies check signatures and reviews on the original, the scan and the client's own references name it, and a stack's lower entries are ordinary stored changesets. Changing that is an API change, and it loses the audit trail. A grace-delayed delete gives most of the saving with none of that |
| **Delete originals immediately after the land commits** | It races retries, the scan and agents still building on the original. Grace and the fence exist for exactly these |
| **Time-partitioned `changesets` table** (drop old partitions) | Originals and their live copies land in the same time slices, so no partition is ever wholly garbage. It suits the append-only `ref_log` (phase 4), not changesets |
| **An archival tier** (move unreachable changesets and their nodes to cold object storage) | It keeps everything, at a cheaper price per byte, and needs a read fallback on every miss. Worth revisiting if audit requires keeping originals. It can reuse this ADR's roots and fence, with "move" in place of "delete" |
| **Reference counts on changesets and tree nodes** | Every create and land would update counts on shared rows: hot-row contention at 2,500 lands a second, and counts that are wrong after a crash. A mark costs one batched pass per run instead |
| **Prune the ref log but keep land ids in a side table forever** | That saves only the ~150 bytes of entry body and still grows without bound. It splits ADR 0057's "id exists if and only if the move does" across two tables |
| **Reachability from refs only (no log, no land ids, no window)** | Undo, retried lands and `via` all break. That is what ADR 0008 warned about |

## Not addressed
- **Index rows of reachable history:**
  - trigram rows for every changed directory version;
  - history packs;
  - `blame` rows of heads nobody will ask about again.

  On wide trees these are most of the 6.5 to 22 KB per land. That needs a
  retention and eviction design of its own for caches (`blame`) and
  compaction for indexes, owned by the ADR 0058 holder.
- Multiple repositories (ADR 0054): roots become per repository, and
  membership tables join the roots. That is designed in when ADR 0054 is
  implemented.

## Open questions
1. **Defaults.** Changeset grace 7 days, and the ref log unpruned unless an
   operator sets *R*. Should swarm deployments default to *R* = 30 days?
2. **Attested originals.** Keeping every signed or reviewed original means
   protected refs reclaim nothing. Is the `signature-v1` row plus the
   copy's `rebased-origin-v1` link enough audit trail? A signature covers
   only the id, and the copy carries the content. If so, drop root 5 for
   originals of landed copies.
3. **Snapshots.** Should `leep.snapshot` changesets get a longer grace than
   other work in progress, or a client-renewed hold?
4. **Is `stored_at` worth a migration?** The alternative is a derived row
   written with `root_tree` in the same call, which costs about 50 bytes a
   changeset against 8.
5. **E4 numbers.** Does unreachable data reach the 20% threshold on the
   swarm store? Does `--exact-trees` show the upper bound for tree nodes to
   be close enough to skip epochs (phase 4)?
6. **Number.** 0059 was the next free ADR number on `main` and on every
   origin branch on 2026-10-21, including
   `derive/index-lease-and-bounded-queries`. Renumber if another ADR merges
   first.

## Consequences
- Metadata growth per land falls by the unreachable share, which E4
  measures. Blob-store growth falls by the originals' path-copied tree
  nodes, once epochs run.
- "Changesets are never deleted" stops being true. The places that say so
  are updated in the implementing changes:
  - ARCHITECTURE §8;
  - ADR 0008, 0018, 0033 and 0058 §6;
  - FORMATS.md "Ref log";
  - OPERATIONS.md "Ref log".

  An unreachable changeset older than grace may answer 404.
- Requests gain a cached flag read. Only while a sweep is between condemn
  and collect do they also read fence rows for the changesets they rely on.
- With ref log retention on, #103's "answer a retried land at any age"
  becomes "within the advertised retention". Clients that honour
  `land_id_retention_secs` never resend a land past it, so they cannot
  land twice that way.
