# Operating Leep

## Logs

`leep-server` logs with `tracing`. Choose the format with `--log-format
text|json` (env `LEEP_LOG_FORMAT`, default `text`); filter with `RUST_LOG`
(default `info`). Use `json` when a log pipeline ingests the output: one JSON
object per line.

Every HTTP request produces one INFO line, `request completed`, inside a
`request` span. The span carries `method`, `route` (the matched template such
as `/v1/blobs/{hash}`, or `unmatched`), `status` and `latency_ms`:

```json
{"timestamp":"...","level":"INFO","fields":{"message":"request completed","status":404,"latency_ms":0.39},"target":"leep_server::observe","span":{"method":"GET","route":"/v1/blobs/{hash}","status":404,"latency_ms":0.39,"name":"request"}}
```

Headers are never logged, so `Authorization` values cannot appear. The
authentication span (DEBUG) records the token's name, never its value.
Server-side failures additionally log an ERROR line with the error text.

## Metrics

Metrics are off by default. Start the server with `--metrics-listen <ADDR>`
(env `LEEP_METRICS_LISTEN`), for example `127.0.0.1:9187`, and scrape
`GET /metrics` (Prometheus text format). The listener is separate from the
API port, needs no token and serves nothing else, so bind it to a private
interface or network and never publish it through the API's reverse proxy.
The address is logged at startup.

Instrumentation uses the `metrics` facade, which does nothing unless a
recorder is installed; only the server binary installs one. HTTP latency is
measured until the response head is ready, not until the body is streamed.

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `leep_http_requests_total` | counter | `method`, `route`, `status` | Requests handled. `route` is the matched template or `unmatched`; unknown methods are `other`. |
| `leep_http_request_duration_seconds` | histogram | `method`, `route` | Time to produce the response head. |
| `leep_land_total` | counter | `outcome` = `landed`, `conflicts`, `exhausted`, `error` | Land calls by result. `conflicts` is a normal reply that needs client action; `exhausted` means the CAS lost every retry. |
| `leep_land_attempts` | histogram | none | CAS attempts per land that reached the retry loop; above 1 means contention. |
| `leep_land_cas_losses_total` | counter | none | Lost ref compare-and-swaps (races with other server instances). |
| `leep_land_duration_seconds` | histogram | none | Whole land call, including lock wait. |
| `leep_land_lock_wait_seconds` | histogram | none | Time waiting on the per-ref in-process land lock. |
| `leep_derive_total` | counter | `kind` | Derived values computed (cache hits in the meta store are not counted). |
| `leep_derive_duration_seconds` | histogram | `kind` | Time to compute one derived value. |
| `leep_search_total` | counter | `indexed` = `true`, `false` | Searches served; `false` means the query had no required trigram and scanned files directly. |
| `leep_search_duration_seconds` | histogram | none | Whole search call, including any index rows it derived. |
| `leep_blob_cache_hits_total` | counter | none | Blob reads served from the in-memory cache. |
| `leep_blob_cache_misses_total` | counter | none | Blob reads that went to the durable store. |
| `leep_blob_cache_bytes` | gauge | none | Bytes held in the cache. |
| `leep_blob_cache_entries` | gauge | none | Blobs held in the cache. |
| `leep_requests_limited_total` | counter | `reason` = `rate`, `concurrency`, `size`; `token` | Requests refused by a request limit (ADR 0020). `token` is the token's name, or `anonymous` for every client address when there is no tokens file. |
| `leep_requests_in_flight` | gauge | `token` | Requests in progress, until their response body has been sent. Only tokens that have a limit are tracked. |
| `leep_build_info` | gauge | `version` | Always 1; carries the server version. |
| `leep_schema_version` | gauge | none | Newest metadata schema migration applied (ADR 0023), read when the server opened the store. Servers sharing a database report the same value once all are upgraded. |

Seconds histograms use buckets from 1 ms to 10 s; `leep_land_attempts` uses
1 to 32. The cache metrics are recorded by `CachedBlobStore` itself as it
serves requests (the gauges refresh on every read and write), so there is no
polling interval. With `--cache-bytes 0` there is no cache and the
`leep_blob_cache_*` series are absent.

### Suggested alerts

```yaml
groups:
- name: leep
  rules:
  - alert: LeepLandExhausted
    expr: increase(leep_land_total{outcome="exhausted"}[10m]) > 0
    annotations: {summary: "A land lost the ref CAS on every retry; check contention across instances."}
  - alert: LeepLandErrors
    expr: increase(leep_land_total{outcome="error"}[10m]) > 0
  - alert: LeepLandSlowP99
    expr: histogram_quantile(0.99, sum by (le) (rate(leep_land_duration_seconds_bucket[5m]))) > 5
    for: 10m
  - alert: LeepBlobCacheColdHitRatio
    expr: |
      sum(rate(leep_blob_cache_hits_total[15m]))
        / (sum(rate(leep_blob_cache_hits_total[15m])) + sum(rate(leep_blob_cache_misses_total[15m]))) < 0.5
    for: 30m
    annotations: {summary: "Cache hit ratio below 50%; consider a larger --cache-bytes."}
  - alert: LeepServerErrors
    expr: sum(rate(leep_http_requests_total{status=~"5.."}[5m])) / sum(rate(leep_http_requests_total[5m])) > 0.01
    for: 5m
  - alert: LeepTokenThrottled
    expr: sum by (token, reason) (rate(leep_requests_limited_total[10m])) > 0.5
    for: 15m
    annotations: {summary: "A token is being limited steadily: a runaway agent, or a limit set too low."}
```

## Request limits

Limits are per token (ADR 0020; settings in [DEPLOY.md](DEPLOY.md#request-limits)).
A refused request is a `429` (rate or concurrency) or `413` (size), counted in
`leep_requests_limited_total{reason,token}` and `leep_http_requests_total`, and
logged at DEBUG only (`RUST_LOG=leep_server=debug`), so a runaway client cannot
flood the log. The client sees `Retry-After`, and `leep` retries a throttled
read a few times.

Tuning:

- Start generous and tighten. `leep_requests_limited_total` shows who hits a
  limit; set `--rate-limit` at about twice a token's steady rate and
  `--rate-burst` at the largest legitimate burst. A checkout or `leep sync` of
  a large tree makes tens to hundreds of reads in a second, so too small a
  burst throttles ordinary use; agents making many small commits are better
  served by a high burst than a high rate.
- `rate` counts requests, not cost. A search or blame request costs far more
  than a ref read; the concurrency cap, which bounds how many run at once, is
  what protects the server from those. Pick it near the number of cores you are
  willing to give one token. A slow upload holds its slot for the whole
  request, up to `--request-timeout`.
- Give a token that legitimately lands for many agents (a CI or merge bot)
  its own entry with higher limits or `"unlimited"`, rather than raising the
  default for everyone. `land` retries lost races itself; throttling it
  stretches that loop.
- Read tokens rarely need bodies above a few KiB (`POST /v1/blobs/missing` takes
  about 70 bytes per hash), so `--read-max-body-bytes 1048576` is a safe
  default.
- A sustained rate of `reason="rate"` means the limit is too low or an agent is
  in a retry loop: check the token's name against the agent. `reason="concurrency"`
  with a low request rate means slow requests are piling up (a cold search index,
  a slow blob store); look at `leep_http_request_duration_seconds` before raising
  the cap. `reason="size"` is almost always a client bug.
- State is per process and resets on restart. With several servers behind a
  balancer each applies the whole limit, so divide the target by the number of
  servers, and balance by token if you can.
- Anonymous mode keys by peer address; behind a proxy that is one bucket for
  all clients (see DEPLOY.md). The table holds 4096 addresses, idle ones are
  evicted, and extra active clients share one entry, so memory does not grow
  under a flood of spoofed or rotating sources.
- Limits do not apply to `GET /v1/health` or to unauthenticated requests
  (which fail with 401 without costing a token anything); rate-limit those at
  the proxy.

## Ref log

Every ref create, move and delete is recorded in the `ref_log` table of the
metadata store (ADR 0018), by the same transaction that changes the ref. Read
it with `leep reflog [REF]` or `GET /v1/reflog` (a read token is enough). The
table is created by the schema migrations (see [Upgrading](#upgrading)) when a
server first opens an existing database, and starts empty: refs that moved
before the upgrade have no history. Upgrade every server before relying on it; an older server still
running moves refs without writing entries.

The log is never pruned, and it needs no maintenance: a row is about 150 bytes
and is written only when a ref moves, so even a swarm landing every second
adds a few tens of MB a year. Include `ref_log` in metadata backups, since it
is the only record of how refs moved. Entries name changesets, which are never
deleted, so `leep-server gc` is unaffected and a ref restored with `leep undo`
always points at an existing changeset. Postgres serializes the final insert of
each ref move with an advisory lock (key 7164738) so `seq` follows commit order;
it is held for the length of one insert.

## Upgrading

The metadata schema is versioned (ADR 0023). Each database records the
migrations applied to it in the `schema_migrations` table; the server knows
the list that was compiled into it. The newest applied version is the
`leep_schema_version` gauge, and `leep-server migrate --status` prints it.

**Procedure.** For every upgrade, whether or not it changes the schema:

1. **Back up** metadata and blobs (`leep-server backup`, [Backup and
   restore](#backup-and-restore)) and check it (`backup-verify`). There is no
   downgrade: going back means restoring the backup, which needs a binary at
   least as new as the schema in the backup.
2. Look at what the new version would do. With the new binary (for example
   `docker compose run --rm leep-server migrate --status`, or `docker run --rm leep:new migrate --status` with the same `LEEP_META`):
   ```sh
   leep-server migrate --status      # version, history, and what is pending
   leep-server migrate --dry-run     # the SQL of every pending migration
   ```
   Both change nothing. Read the CHANGELOG for anything that needs the old
   servers stopped first.
3. Apply it, deliberately:
   ```sh
   leep-server migrate
   ```
   It runs all pending migrations in one transaction under a lock, so it is
   all or nothing, and may run beside servers of the previous version when the
   release notes say they can coexist. Run it from one place; if two run at
   once the second waits and finds nothing to do.
4. Start the new servers. With `--no-migrate` (`LEEP_NO_MIGRATE=1`) a server
   refuses to start while a migration is pending, which is how to make steps
   2 and 3 mandatory in a deployment.

**Default: servers migrate on start.** Without `--no-migrate`, a starting
server applies pending migrations itself, as it always created its tables.
This is what a single-container deployment wants, and the same transaction and
lock apply, so a fleet that starts together migrates once. Use `--no-migrate`
whenever you want a change window or the server's database account may not
alter the schema. `gc`, `derive-search`, `verify` and `repair` follow the same
flag.

**First start after the versioning release.** A database made by an older
Leep has no `schema_migrations` table. The first server or `migrate` to open it
inspects the tables, records the migrations they already satisfy (version 1 for
every database that has the ref log) without changing any data, and applies any
that are missing (a database from before the ref log gains its table). With
`--no-migrate` this counts as pending: run `migrate` once.

**Rolling upgrades.** Servers share only the two stores, so old and new
servers can run side by side while a release's migrations are compatible with
both. Servers from before ADR 0023 do not read the version, so they will not
notice a newer schema: do not apply a migration after version 1 while one is
still running, and read each release's notes for whether its migration
tolerates old servers. A server never writes to a schema it does not
understand:

- A database **newer** than the server (`database schema v3 is newer than this
  server (max v2); upgrade the server`) is refused at startup by the server,
  `migrate`, `gc`, `derive-search`, `verify` and `repair`, in every mode.
  Finish the rollout rather than restarting an old server onto the migrated database; to go back, restore
  the backup taken in step 1.
- A migration whose checksum differs from the recorded one (someone edited an
  applied migration, or a build is from a different branch) is refused, naming
  the migration.

A failed migration rolls back completely and leaves the database as it was;
fix the cause and run `migrate` again. Fresh SQLite and Postgres databases
need no setup: the first server creates them at the current version.

## Blob garbage collection

Abandoned uploads and failed commits leave unreferenced blobs. `leep-server
gc` reclaims them while servers keep running (ADR 0008, ADR 0012). It uses the
same `--meta` and `--blobs` as the server, talks to the stores directly, and
needs no downtime.

```sh
leep-server gc --dry-run                     # what would be condemned; writes nothing
leep-server gc                               # condemn, wait --fence, collect
leep-server gc condemn                       # phase one only
leep-server gc collect --fence 10m           # phase two only
```

How it works: **condemn** marks every unreferenced blob older than `--grace`
(default 24h); a condemned blob is reported missing to clients, which upload it
again if they need it. After `--fence` (default 10m) **collect** marks again and
deletes the blobs that are still condemned. Collect acts only on condemnations
at least `--fence` old, so the two phases can run as separate cron jobs or
processes. A crashed run is safe: rerun it.

Choosing durations:

| Setting | Must exceed | Default |
|---|---|---|
| `--fence` (gc) | the servers' `--request-timeout` plus clock skew between hosts plus a margin | 10m |
| `--request-timeout` (server, `LEEP_REQUEST_TIMEOUT`) | the longest legitimate request, for example a large blob upload on a slow link | 5m |
| `--grace` (gc) | the time a client takes from upload to commit, and the request timeout | 24h |

If you raise `--request-timeout`, raise `--fence` with it. `--request-timeout
0` removes the bound the fence relies on; GC is then not safe beside servers.
`gc` refuses `--fence 0` unless `--unsafe-no-fence` is given, which is only
valid with every server stopped.

Run one GC at a time. A cron job such as `17 3 * * *  leep-server gc` works; to
avoid a long-lived process use `17 3 * * *  leep-server gc condemn` and `47 3 *
* *  leep-server gc collect`. The report counts blobs condemned, rescued (by an
upload, by a changeset that referenced them during the fence, or by a newer
write) and deleted. After a collect, a clients' dedup check on a deleted blob
answers "missing" and the client uploads it.

Signals: a burst of 400 "missing blob" right after a condemn is normal (clients
retry by re-uploading); 409 on `PUT /v1/blobs/{hash}` means a collect was
killed mid-delete of that blob, so run `leep-server gc collect` again; 408
means a request exceeded `--request-timeout`.

Upgrade note: servers built before ADR 0012 ignore condemnations. Upgrade every
server before the first `condemn`.

## Verify and repair

`leep-server verify` checks a store against itself and `leep-server repair`
fixes the part of it that can be fixed (ADR 0025). Both work offline on the
stores, with the same `--meta` and `--blobs` as the server and no server
needed, and both are safe beside running servers: `verify` writes nothing, and
`repair` writes only derived rows, which are pure functions of content. They
never create a database or a blob directory that does not exist.

Run `verify`:

- after restoring a backup, before pointing servers at it (`restore` already
  runs the cheap closure check; `--full --rehash` is the thorough one);
- after upgrading, and after a crash, a disk or volume problem, or a bad
  migration;
- when an answer looks wrong (a log, a blame, a search, an `is_ancestor` that
  disagrees with another), because a damaged derived row changes answers
  silently;
- on a schedule: the default mode is cheap enough for a daily cron job, and
  `--full --rehash` for a weekly one from a replica or in a quiet window.

```sh
leep-server verify                             # sample: complete reachability and ref log, sampled derived rows
leep-server verify --full                      # recompute every derived row; also changesets no ref reaches
leep-server verify --full --rehash             # also re-hash the bytes of every reachable blob
leep-server verify --ref main --derived generation skiplist   # one ref, some kinds
leep-server verify --json > report.json        # the same report as one JSON object
```

Exit status is `0` for a clean store, `1` when it found problems and `2` when
it could not run (an unknown ref or kind, a database that does not exist, a
store that could not be reached). Progress goes to stderr every few seconds
and the report to stdout. `--rehash` is the flag that re-reads blob bytes;
`--blobs` is the blob store URL.

What each mode costs, for a store of N changesets and F distinct files:

| Mode | Reads | Notes |
|---|---|---|
| default | every changeset twice, every new tree node once, one existence check per distinct blob, the ref log, and about 256 rows of each kind | memory about 100 bytes per changeset plus 50 per tree node and blob |
| `--full` | the above, plus a recomputation of every derived row: tree nodes along every changeset's changes, history nodes, every trigram row against its blob | the cost of deriving everything again; read-only |
| `--rehash` | additionally every reachable blob's bytes, eight at a time | I/O bound: the size of the repository |

See [PERFORMANCE.md](PERFORMANCE.md#verify) for measured times.

### Reading the report

The report lists what it looked at (refs, changesets, tree nodes, blobs, ref
log entries), then per derived kind how many rows it compared (`checked`),
how many changesets had no row (`missing`, not a problem: rows derive on
demand and a crash leaves some missing), how many were wrong (`mismatched`),
how many it could not recompute because an input is damaged (`unverifiable`)
and how many it had to skip because the row's subject is gone (`skipped`: a
trigram row whose blob GC collected). Then each problem, with the kind, row
key, changeset, path or ref that locates it, and `result: clean` or the number
found. `notes` explain what sampling did, and mention blame rows that differ
from a recomputation but agree with the file (blame diffs run under a time
limit, so two answers can both be right).

In sample mode a derived mismatch at a changeset may come from a damaged row
at one of its parents that was not sampled; `--full` finds the row that is
actually damaged and reports only it, because it compares each row with what
its parents' true rows imply.

### Runbook: verify found X

Fix problems in this order, because later checks assume the earlier ones:
history, trees and blobs first, derived rows after.

| Finding | What it means | What to do |
|---|---|---|
| `missing_changeset`, `corrupt_changeset` | A changeset row is gone or does not hash to its id; the report names the child that needs it | Not repairable: changesets are source data. Restore the `changesets` row from a backup (`verify` then checks everything again). Do not delete the referencing ref; `leep undo` and the ref log can show where it pointed before |
| `missing_tree`, `corrupt_tree` | A tree node is absent or its bytes do not hash to its address. The report gives the oldest changeset that reaches it and the directory | `leep-server repair rederive --kind root_tree` rewrites every node from the changes that introduced it, for the changesets in scope, and `verify` confirms. If it reports `rederive_failed`, an input blob is missing; restore it first |
| `missing_blob` | A file blob a changeset names is gone (a bad restore, a deleted file, a GC bug). The report names the changeset and path | Not recomputable. Restore the blob from the blob store's backup; its address is in the report, `blobs/<hex[0..2]>/<hex[2..]>`. Once you have the bytes (a client's `~/.cache/leep/objects` may still hold them) `PUT /v1/blobs/{hash}` stores them again |
| `corrupt_blob`, `blob_size_mismatch` (`--rehash`) | The bytes under a blob's address do not hash to it, or are not the declared length | Delete the file and restore from backup, as for a missing blob. Until then reads of it return the damaged bytes |
| `derived_mismatch` | A derived row differs from its recomputation. The report gives kind, row key and changeset | `leep-server repair rederive --kind <kind>` and run `verify --full` again. Find out why (disk, restore, an old version): derived rows are only ever wrong through damage |
| `derived_unverifiable` | A row could not be recomputed because something it reads is damaged | Fix the damaged tree node, blob or parent, then verify again |
| `corrupt_history_node`, `missing_history_node` | A history index node does not match its address, or a `history-tree-v1` row points at none | `repair rederive --kind history-tree-v1` rewrites the nodes. Or `repair drop --kind history-tree-v1` and let the index rebuild on the next path query (slow on a long history: it derives the whole index at once) |
| `bad_attestation`, `invalid_signature` | A signature or review row does not decode, does not belong to any changeset, or a signature does not verify | These are not derived from history and are never rewritten by `repair`. Restore the row from a backup, or delete it with SQL if it is junk, and have the author sign again |
| `bad_rebased_origin`, `bad_gc_row` | Bookkeeping rows that are malformed or name changesets that do not exist | Restore from a backup. A bad GC row can be deleted with SQL while no `gc` runs; `repair` refuses to touch them |
| `reflog_divergence` | A ref's newest ref log entry does not say where the ref is now | Someone changed the `refs` table or the log directly, or a restore mixed two points in time. Decide which is right: if the ref is right, the log is merely incomplete and nothing breaks; if the log is right, `leep undo`-style recovery is `POST /v1/refs/{name}` to the logged value. `repair` never edits either |
| `reflog_chain_break` | An entry's `new` does not equal the next entry's `old`: an entry was removed or edited | The log is the record of what happened; restore it from a backup if you need it complete. Refs are unaffected |

After a repair, run `verify --full` again. A store that is clean under
`--full --rehash` has every reachable object intact and every derived row
equal to what the history implies.

### Repair

```sh
leep-server repair rederive --kind generation --dry-run   # what would change; exit 1 if anything would
leep-server repair rederive --kind skiplist               # also checks generation, which it reads
leep-server repair rederive --kind root_tree --ref main   # only the history of one ref
leep-server repair rederive --kind all                    # every kind that can be recomputed
leep-server repair drop --kind blame blob-trigrams-v1     # delete rows; they derive again when asked for
leep-server repair drop --kind history-tree-v1 --dry-run  # also removes history-node-v1; counts only
```

`rederive` walks the chosen changesets oldest first, recomputes each selected
kind and every kind it depends on (so a skiplist is never rebuilt from a
damaged generation), and writes rows that are missing or wrong. It reports per
kind how many it examined, how many were already right, how many it wrote, and
how many it could not recompute. Rows no query ever asked for are written too:
for the lazily derived kinds that is eager derivation, which is also how to
warm an index after a restore. It is safe to repeat and to run beside servers;
the servers write the same bytes. `root_tree` and `history-tree-v1` also
rewrite the tree and history nodes they derive, which is what restores a
missing or damaged one. The search kinds (`blob-trigrams-v1`,
`tree-trigrams-v1`) are rewritten for the rows that exist; use
`derive-search REV` to build rows that do not.

`drop` removes every row of a kind in batches and the kind then derives again
on demand. It changes no answer, only latency: the next query that needs a row
derives it, and for `generation`, `skiplist`, `root_tree` and `history-tree-v1` on a
long history the first query derives the whole history. Warm them with
`repair rederive` instead of waiting for a user. Dropping `history-tree-v1` also
drops `history-node-v1`, because only it refers to those rows.

Kinds that cannot be rederived (and so are refused by `drop` with an
explanation and exit status 2): `signature-v1` and `review-v1` (clients wrote
them), `rebased-origin-v1` (written by rebase; `land`'s retry detection depends
on it), `blob-claim-v1`, `blob-condemned-v1`, `blob-rescue-v1`, `blob-doom-v1` (the GC
fence, ADR 0012), `history-node-v1` on its own, and the ref log. The full table
with reasons is in ADR 0025. `blame` can be dropped but not rederived: its rows
are keyed by a hash of (changeset, path) and cannot be listed.

Everything `repair` changes is derived data. If a repair makes things worse
(it cannot, short of a bug), `repair drop` of the same kind returns the store
to "derives on demand".

## Backup and restore

`leep-server backup` takes a consistent copy of both stores while servers and
GC keep running, and `leep-server restore` puts it back (ADR 0026). A backup
is only worth having if it has been restored: the drill below is part of the
procedure, not an extra.

### What to back up

| What | Where it lives | How |
|---|---|---|
| Metadata: changesets, refs, ref log, signatures, reviews, derived data | SQLite file or Postgres database (`--meta`) | `leep-server backup` |
| Blobs: file contents and tree nodes | filesystem directory or S3 bucket (`--blobs`) | `leep-server backup` |
| Configuration: tokens file, signing keys file, review policy file, TLS/proxy config | wherever you keep it | your configuration management; **not in the stores and not in the backup** |

Back up the configuration files with the deployment's other configuration. A
restored store with lost tokens still works, but nobody can authenticate to it
until tokens are issued again (`leep-server gen-token`).

### Taking a backup

```sh
leep-server backup --out /backups/leep                 # uses LEEP_META and LEEP_BLOBS as the source
leep-server backup --out /backups/leep --rehash        # also re-hash every referenced blob (slow, thorough)
leep-server backup --out /backups/leep --blobs-to s3://my-backups?region=us-east-1   # blobs go to a bucket
leep-server backup --out /backups/leep --metrics-file /var/lib/node_exporter/textfile/leep_backup.prom
```

`--out` becomes a directory with one blob pool shared by every snapshot and one
directory per run:

```
/backups/leep/blobs/ab/cdef...                       the pool, only ever added to
/backups/leep/snapshots/20261016T031500Z/meta.db     metadata as of one instant
/backups/leep/snapshots/20261016T031500Z/manifest.json   written last: the snapshot exists once this does
```

What a run does, in order, and why the order matters:

1. **Metadata first**, in one read transaction (`REPEATABLE READ` on Postgres),
   written to `meta.db`. Its instant is the backup's recovery point.
2. **Blobs second**: every blob the pool lacks is copied. Blobs are immutable and
   named by their content, so "already there" is proof it is right and the
   second backup copies only what is new (a store with nothing new costs two
   directory or bucket listings).
3. **Check**: every changeset in the snapshot is walked against the pool, and
   every tree node and blob it names must be there. This is what proves the two
   halves match; the result is stored in the manifest.

Metadata before blobs is the whole consistency argument. A blob that a stored
changeset needs is never deleted (ADR 0012), so everything the snapshot needs
is in the blob store when step 2 looks, no matter what lands or what GC deletes
meanwhile; copying blobs first would miss blobs of changesets landed between
the two. So there is **no need to stop servers or pause GC**, and no `gc
collect` schedule has to avoid the backup window. If a backup's check ever
finds a missing blob (a GC bug, `gc --unsafe-no-fence` run under live servers, a
store that was already damaged) the backup is sealed as failed, prints the
problems, exits 1 and `restore` refuses it.

Exit status: `0` sealed and the check clean, `1` sealed but the check found
problems (do not rely on that snapshot), `2` could not run (nothing sealed).
`--json` prints the manifest and problems as one object. Progress goes to
stderr.

Things to know:

- **Source of truth.** `backup` only reads the source. It refuses a source that
  does not exist (it never creates one), one with pending migrations (run
  `leep-server migrate`, ADR 0023) and one newer than the binary.
- **Where to put it.** On a different disk, host and failure domain from the
  stores. The pool holds all repository content and the snapshots hold review
  and signature history: put it on encrypted storage.
- **Memory.** Each blob is read whole while it is copied, so memory is about
  `--concurrency` (default 8) times your largest blob. Lower it for very large
  blobs.
- **Long snapshots.** The metadata read transaction lasts as long as the
  metadata takes to stream. On Postgres it holds back vacuum for that time; on
  SQLite it delays WAL checkpoints. Metadata is small next to blobs (a
  million changesets is a few GB), so this is minutes, not hours. For metadata
  too large for that, use Postgres base backups and WAL archiving for the
  metadata half (and see "Blobs protected by the platform" for the blobs).
- **Blobs protected by the platform.** If the bucket already has versioning or
  cross-region replication, take `backup --no-blobs`: only the snapshot and the
  manifest are written, and the check runs against the source blobs. Once the
  platform's copy has caught up, run `backup-verify --blobs-from URL` against
  it: that is the proof the two match.
- **Scratch space.** Tools never open `meta.db` in place (the medium may be
  read-only, and opening would change the file the manifest hashed). The check,
  `backup-verify` and `restore` work on a temporary copy of it under
  `$TMPDIR`, so point `TMPDIR` at a disk with room for the metadata.
- **Two runs at once** write separate snapshots and share the pool safely. A
  directory under `snapshots/` without a `manifest.json` is an interrupted
  run: it is ignored and can be deleted.

### Schedule, retention, RPO and RTO

A daily run is a reasonable start; run it from cron on a host that can reach
both stores:

```cron
# 02:10 daily: take a backup; Sundays 05:20: re-read the pool and check it against the newest snapshot
10 2 * * *  leep-server backup --out /backups/leep --metrics-file /var/lib/node_exporter/textfile/leep_backup.prom
20 5 * * 0  leep-server backup-verify --from /backups/leep --rehash
```

- **RPO** (data you can lose): everything landed after the newest sealed
  snapshot's instant (`snapshot_taken_at` in the manifest), so the backup
  interval plus the duration of the run is the worst case. The ref log shows
  what was landed after the instant if the source survives. For an RPO of
  minutes, back up more often (an incremental run on a store with little new
  content is cheap), or use Postgres WAL archiving for the metadata and the
  bucket's versioning for blobs.
- **RTO** (time to restore): the time to copy the pool to the target plus the
  time to load the metadata (one transaction), plus `verify` if you run it. The
  drill measures both on your data; figures from a modest store are in
  [PERFORMANCE.md](PERFORMANCE.md). Blob bytes over your network dominate on
  large stores.
- **Retention.** A snapshot directory is self-contained apart from the pool, so
  deleting old directories under `snapshots/` is safe. The pool is shared and
  only grows: to reclaim space, start a new pool (`--out` a new directory) every
  few months and delete the old directory once its newest snapshot is older than
  your retention. Keep at least the last few snapshots and one that is a few
  weeks old: a corruption noticed late is only recoverable from a snapshot from
  before it.

### Restoring

```sh
leep-server restore --from /backups/leep                     # the newest sealed snapshot
leep-server restore --from /backups/leep --snapshot 20261016T021000Z
leep-server restore --from /backups/leep --blobs-from s3://my-backups?region=us-east-1
```

`restore` writes to `--meta` and `--blobs`: the new, empty stores. Procedure
after losing the stores:

1. Provision empty stores: a new Postgres database (any name; the server
   creates the tables) or an empty path for SQLite, and an empty directory or
   bucket for blobs.
2. Restore the configuration files (tokens, signing keys, policy).
3. Run `leep-server restore --from DIR` against them, with the same
   `LEEP_META`/`LEEP_BLOBS` the servers will use. It checks the manifest, the
   file's BLAKE3 and the schema version, copies the missing blobs, loads the
   metadata in one transaction, and runs the closure check on the result. Exit
   `0` means done and clean.
4. Run `leep-server verify --full --rehash` (optional but recommended; it also
   recomputes derived data).
5. Start the servers. Clients with work that landed after the snapshot find
   their refs behind: they `leep sync` and land again; nothing else changes.
   The ref log continues from the snapshot's last entry.

Refusals, which are the point:

| Message | Meaning | What to do |
|---|---|---|
| `destination metadata store is not empty` | The target database has data | Restore into a fresh database; `--force` deletes what is there, in the same transaction, only after you have stopped every server that uses it |
| `destination blob store is not empty` | Blobs already there | `--resume` continues an interrupted restore (metadata must still be empty); `--force` allows it anyway |
| `newer than this server (vN)` | The backup's schema is newer than this binary | Restore with the newer `leep-server` (ADR 0023); never edit the backup |
| `does not match its manifest` | `meta.db` was damaged or replaced | Use another snapshot |
| `closure check failed` / `was skipped` | The backup was sealed with problems, or without a check | Use another snapshot, or `--accept-failed-check` after reading the problems |
| `manifest format N` | Written by a newer Leep | Use a newer `leep-server` |

An older schema is not a refusal: it is migrated on a scratch copy and loaded
(ADR 0023), so a backup from before an upgrade restores into the new version.
A restore interrupted by a crash leaves the metadata empty (it is loaded last,
in one transaction), so run the same command again with `--resume`.

A backup can be restored into the other metadata backend: a snapshot taken
from Postgres loads into SQLite and the other way round. That is also the
supported way to move between them.

After a restore, filesystem blobs have the restore time as their modification
time, so GC's `--grace` window starts again: nothing is deleted for a day.

### Checking a backup without restoring it

```sh
leep-server backup-verify --from /backups/leep               # newest snapshot: file hash, then every blob it names
leep-server backup-verify --from /backups/leep --rehash      # also re-read and re-hash the blobs
```

Exit `0` clean, `1` problems (with the first ones named), `2` could not run. Run
it after copying a backup elsewhere, after a `--no-blobs` run, and from cron
against the backup directory: bit rot in the pool shows up here, not during the
disaster.

### The restore drill

Do this on a schedule (monthly is a good start, and after every Leep upgrade),
on a scratch host or the same host with throwaway stores. It is the
`crates/leep-e2e/tests/backup.rs` test done by hand against your real backup,
and the only way to know the backup works:

```sh
# 1. Restore the newest backup into scratch stores, never the real ones.
export LEEP_META='sqlite:///scratch/drill/meta.db?mode=rwc'     # or a scratch Postgres database
export LEEP_BLOBS=file:///scratch/drill/blobs
leep-server restore --from /backups/leep --rehash               # exit 0, "result: restored"

# 2. Full verification of the restored stores.
leep-server verify --full --rehash                              # exit 0, "result: clean"

# 3. Serve it and ask questions.
leep-server --listen 127.0.0.1:7071 &
export LEEP_SERVER=http://127.0.0.1:7071
leep refs                                                       # every tip in manifest.json "refs"
leep log main -n 20; leep blame main some/file; leep search 'a known string' --rev main
leep init /scratch/drill/wc && cd /scratch/drill/wc && leep checkout main   # a fresh checkout
```

Pass criteria: `restore` and `verify` exit 0; `leep refs` equals the manifest's
`refs`; the checkout has the files you expect; the time from step 1 to step 2 is
your measured RTO. Record the date and the time. Then delete the scratch
stores. If any step fails the backup you tested is not good: take another,
keep the failed one for diagnosis, and find out why before the next run.

### Alerts

`backup --metrics-file` writes node_exporter text-file metrics. Alert on:

| Alert | Expression | Meaning |
|---|---|---|
| No recent backup | `time() - leep_backup_last_success_timestamp_seconds > 2 * 86400` (use twice your interval) | Backups are not running or not passing |
| Last backup failed its check | `leep_backup_last_run_ok == 0` | A sealed snapshot is not restorable; read the run's output |
| Backup slowing | `leep_backup_duration_seconds` rising week on week | The store is outgrowing the window |
| Drill overdue | your own metric or ticket: no drill in 35 days | An untested backup is not a backup |

Also alert on the cron job's exit status (2 means nothing was sealed), and on
free space where `--out` lives. The metrics (`last_run_timestamp`,
`snapshot_timestamp` (the recovery point), `duration`, `check_problems`,
`changesets`, `blobs_copied`, `blob_bytes_copied`) are listed in ADR 0026.

## Crash and fault tests

Two suites check what happens when stores fail and processes die (ADR 0021).
Neither is needed to run Leep; run them after changing `land`, GC, a store
backend or the ref log, and for longer when hardening a release.

**Fault sweeps** (in-memory stores, a few seconds). Seeded; a failure prints
its seed.

```sh
cargo test -p leep-derive fault_tests                      # 25 seeds
LEEP_FAULT_SEEDS=500 cargo test -p leep-derive fault_tests # seeds 1..=500
LEEP_FAULT_SEED=137 cargo test -p leep-derive fault_tests  # replay one seed
```

**Crash test** (real `leep-server` and `leep` binaries, SIGKILL). It needs
the binaries built first, and runs the Postgres variant when
`LEEP_TEST_POSTGRES_URL` is set:

```sh
cargo build --workspace
cargo test -p leep-e2e --test crash -- --nocapture                    # about a minute
LEEP_CRASH_ROUNDS=40 cargo test -p leep-e2e --test crash -- --nocapture   # long run
LEEP_CRASH_SEED=12345 LEEP_CRASH_ROUNDS=9 cargo test -p leep-e2e --test crash
```

| Variable | Default | Meaning |
|---|---|---|
| `LEEP_CRASH_ROUNDS` | 3 | SIGKILLs of the server per backend. Rounds cycle through three scenarios (offline GC, `gc collect` killed mid-run, a fenced GC beside live traffic), so use a multiple of 3. Roughly 7 to 10 seconds per round, longer as history grows. |
| `LEEP_CRASH_SEED` | fixed | Seeds the kill delays, each agent's workload and which requests hang up. |
| `LEEP_CRASH_AGENTS` | 6 | HTTP agents; two `leep` CLI agents always run too. |

After every kill the test also runs `leep-server verify` against the store, and at the end `verify --full --rehash` (see "Verify and repair"), so a kill that left a derived row wrong, or a ref log that no longer replays, fails the run.

The seed fixes the plan, not the interleaving of threads and processes, so a
failure usually, but not always, reproduces from its seed. A failure message
names the backend, the seed and the round, then the invariant that broke
(`[postgres seed 0x7 round 4, after killed collect] ...`); the server's log is
in the test's temporary directory (`server-*.log`), which is removed when the
test ends, so rerun with the seed and read the output of the failing round.

**Leftovers after a real crash.** A killed filesystem blob store can leave
`.tmp-*` files in its shard directories. Nothing reads or lists them, and
nothing removes them; delete those older than the request timeout if the
space matters:
`find /var/lib/leep/blobs -name '.tmp-*' -mmin +60 -delete`.

## Soak tests

`leep-soak` (ADR 0022) runs a swarm of simulated agents against a real
`leep-server` for as long as you ask, checks invariants all the way through,
and records performance and resource trends. Run it before a release, after
changing `land`, GC, derived data, a store backend or the request limits, and
for a long time when hardening. CI runs it weekly for 30 minutes
(`.github/workflows/soak.yml`, also on demand from the Actions tab), and
`cargo test` runs a ten-second version (`leep-e2e`, `soak_smoke`) so it keeps
working.

```sh
cargo build --release --workspace    # leep-soak starts leep-server and leep from its own directory
export LEEP_SOAK_POSTGRES_URL=postgres://postgres:leep@127.0.0.1:5432/postgres
target/release/leep-soak                                    # smoke: 2 minutes, 50 agents
target/release/leep-soak --duration 1h --out soak-1h        # one hour
target/release/leep-soak --duration 8h --chaos --chaos-restarts 12 --gc-interval 10m --out soak-8h
target/release/leep-soak --duration 30m --rate-limit 5 --rate-burst 10 --max-concurrent 8   # limits pressure
target/release/leep-soak --duration 10m --seed 0x2a --out replay   # the plan of an earlier failure
```

It creates a throwaway Postgres database in the admin URL (`--postgres-url`,
`LEEP_SOAK_POSTGRES_URL`, else `LEEP_TEST_POSTGRES_URL`; `--sqlite` or no URL
means SQLite), puts blobs in a directory, starts `leep-server` with
metrics, tokens (one per agent), trusted signing keys and a review policy on
`release`, and drops the database and the directory when the run passes.
After a failure it keeps both (the report names them) so you can start a
server on them and look; its `work` directory is cleared when the same `--out`
is used again. Use `--server URL --token T` to soak a server you
run yourself; give it `--metrics-url`, `--pid`, `--meta` and `--blobs` to get
resource trends and GC, otherwise those parts are skipped (chaos and the
release flow need a server the harness started).

| Flag | Default | Meaning |
|---|---|---|
| `--duration` | 2m | How long the swarm runs; setup (about ten seconds) and the final checks come on top |
| `--agents`, `--cli-agents` | 50, 2 | Agents using the HTTP API; agents driving real `leep` processes |
| `--think` | 6s | Mean think time of an agent between iterations (heavy-tailed) |
| `--seed` | 1 | Seed of the workload plan; the report shows it |
| `--files` | 3000 | Files in the synthetic repository |
| `--request-timeout` | 8s | The server's `--request-timeout`; the GC fence is derived from it (1.5 times it plus 2 s) |
| `--gc-interval`, `--derive-interval` | duration/3 (60 s to 30 min), duration/8 (30 s to 10 min) | Pause between fenced GCs, and between `derive-search main` warmups; 0 disables |
| `--backup-interval` | off | Pause between `leep-server backup` runs beside the swarm; each must seal with a clean check. With it, the run ends with a quiet backup that is restored into scratch SQLite and file stores and verified (`--full --rehash`), whatever the source backend (ADR 0026). Violations are reported under `backup` and `restore` |
| `--chaos`, `--chaos-restarts` | off, 4 | Restart the server, SIGTERM for two in three and SIGKILL for the others |
| `--rate-limit`, `--rate-burst`, `--max-concurrent` | off | Passed to the server per token. The `leep` agents' tokens are exempt: a checkout is thousands of reads (see Request limits above) |
| `--cache-mib` | 32 | The server's blob cache. Keep it small: a cache still filling looks like a leak |
| `--no-policy` | off | No tokens, signatures or review (then limits apply per address) |
| `--check-interval`, `--sample-interval` | 20s, 10s | Invariant rounds; metric and resource samples |
| `--stall-timeout` | 120s | An agent with no progress for this long is a deadlock |
| `--max-land-p99` | 5s | Client-side p99 of land requests |
| `--max-rss-slope-mib-h`, `--rss-min-window-secs` | 64, 300 | Leak bound and the shortest window it is judged over |
| `--baseline`, `--max-regression` | none, 1.5 | Compare p99 and throughput with an earlier `report.json` |
| `--no-fail-fast` | off | Keep running after the first violation |

**Size `--think` to the machine.** Lands to one ref are serialized in the
server, so a ref takes only as many lands per second as one land costs. On the
4 vCPU machine of `docs/PERFORMANCE.md` that is about 6 per second with p99 near a
second, which 50 agents with the default 6 s think time offer. Agents that offer
more only queue: land latency becomes about the number of agents times the
service time and the p99 bound fails, which says the offered load was wrong,
not that Leep regressed. Watch `land_lock_wait_mean_ms` against
`land_duration_mean_ms` in `server_metrics`; if the wait is most of the
latency, lengthen `--think` (or drop agents; the workflow uses 10 s). Shorten it,
on purpose, to measure saturation. A land also costs more the further behind
main its stack is (ADR 0022), so the workload keeps a branch for only a few
iterations; an agent that sits on work for minutes will see slower lands than
the report.

### Reading the report

`summary.txt` is printed at the end and has the lines to read first:
throughput (lands per second), client land latency (p50, p95, p99, max),
the server's own view (attempts per land, CAS losses), the workload mix, the
error counters, server RSS and its slope, database and blob growth, GC
results, and the invariant counters. `report.json` has everything:

| Section | Contents |
|---|---|
| `violations`, `violation_kinds` | What broke, with the time into the run. The first one stops the run unless `--no-fail-fast` |
| `thresholds`, `baseline`, `warnings` | Each bound with its value; warnings never fail the run (408s, GC that deleted nothing, no conflicts) |
| `throughput`, `latency` | Lands per second; latency of every route, of lands per minute (a creeping p99 shows here), and land end to end including conflict redo |
| `errors`, `status_counts`, `workload` | 5xx, those attributed to restarts, transport errors, 429, 408; every counter the agents keep |
| `invariants` | How many times each check ran, and how long each phase of a round took (`ms_*`) |
| `server_metrics` | The server's Prometheus metrics merged across restarts: land outcomes and attempts, CAS losses, derive latency by kind, cache hit ratio, requests limited |
| `resources`, `samples` | RSS slope fit, CPU, database, table and blob sizes, harness RSS; the raw time series every sample interval |
| `gc`, `derive_search`, `restarts` | Each GC with its counts and whether the reachability check after it passed, each warmup, each restart |

What fails a run, and what to do:

| Kind | Meaning |
|---|---|
| `ack_lost`, `history_duplicate`, `ack_content`, `ref_vanished`, `conflict_resolved_silently` | A land the server acknowledged is missing, doubled or wrong, or a conflicted stack landed anyway. The most serious findings: keep the database, find the key (`soak <run>:<agent>:<n>` in the message) in the ref log and history |
| `blob_lost`, `blob_corrupt`, `deep_read`, `changeset_integrity`, `generation` | Something stored no longer reads back as it was written (GC, a store, or derived data) |
| `reflog`, `path_log`, `last_change`, `blame`, `search_token`, `search`, `read_integrity`, `rename_detection` | A derived answer disagrees with the history it is derived from |
| `server_error`, `transport_error`, `stall`, `deadlock`, `postgres_deadlock`, `unexpected_status`, `cli_server_error` | A 5xx, a dropped connection or a hang outside a restart, a request that gave no answer in 90 s, an agent with no progress, an answer nobody expected |
| `threshold_land_p99`, `threshold_rss_slope`, `baseline_regression` | A bound on latency, memory growth or a baseline was exceeded |
| `policy`, `land_rejected`, `release` | Signing and review did not behave: an unsigned or unreviewed land was accepted, or a good one refused |
| `verify` | `leep-server verify --full --rehash` over the stores after the swarm stopped found a problem: the message names the first ones. Run it by hand on the kept data (see Verify and repair) |

A seed replays the workload plan, not the interleaving of threads and
processes, so a failure usually, but not always, reproduces. Server logs are
in `server.log` (filter `warn`: request lines are off) and each GC's output
in `gc.log`.

What it costs: the database grows by about 70 KiB per acknowledged land with
the default 100-file directories (derived rows dominate: the search index and
the path-history index rewrite a changed directory whole; `PERFORMANCE.md` has
the breakdown), blobs by about 26 KiB, and the harness itself keeps about 100 bytes per changeset and per
acknowledged land (its RSS is in the report), so an eight-hour run at full
speed needs a few hundred MiB for it. GC and each round of checks take longer as
history deepens (a GC marks from every changeset), which is why the
interval is measured from the end of one run to the start of the next.

## Code search index

`GET /v1/search/{rev}` (`leep search`, the `leep_search` MCP tool) uses two
derived kinds, `blob-trigrams-v1` and `tree-trigrams-v1` (ADR 0014). They are
built on demand: the first search of a tree that was never indexed derives all
of it before it can prune anything, which takes about a second per 20 000 files
(35 MiB) on in-memory stores and longer against real stores. After an import,
or before an agent swarm starts, warm a revision:

```sh
leep-server derive-search main               # uses --meta and --blobs; prints rows derived
```

It is idempotent and safe beside running servers (rows are written
idempotently; the server reads them as it goes). A later revision derives only
what changed, so run it again after large lands if you want searches to stay
fast. A search limited by `path` derives only that subtree, and a query with no
required trigram (`.*`, a pattern shorter than three bytes) derives nothing and
scans. Rows are never deleted and take about 30% of the size of the indexed text
for source code (up to about 100% for random text) plus at most 64 KiB per
directory; they live in the `derived` table, so size the metadata store for them.
Searches are bounded by the 1000-match cap and a 256 MiB scan budget per request
and by `--request-timeout`.
