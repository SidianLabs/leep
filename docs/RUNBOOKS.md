# Runbooks

One runbook for every alert in [OPERATIONS.md](OPERATIONS.md#suggested-alerts).
Each alert's `runbook_url` annotation points at its section here; the anchor is
the alert name in lower case (`#leeplandexhausted`). `scripts/check-runbooks.sh`
fails CI when an alert has no section or a section has no alert, so the two
cannot drift apart.

Every runbook has the same parts:

- **Meaning**: what fired, and whether it is a *page* (wake someone) or a
  *ticket* (fix in working hours). The rule's `severity` label says the same.
- **Impact**: what users and agents experience.
- **Diagnose**: commands, queries and the log lines to look for.
- **Likely causes**, most likely first, each with how to confirm it.
- **Mitigate / fix**, in the safe order, with what not to do.
- **Escalate / collect**: what to attach to a bug report.

Read [Before you start](#before-you-start) once; it holds the rules that apply to
every runbook.

## Alert index

| Alert | Severity | Fires when |
|---|---|---|
| [LeepServerDown](#leepserverdown) | page | a server's metrics endpoint has not been scrapable for 2 minutes |
| [LeepNotReady](#leepnotready) | page | `GET /v1/ready` reported a store down for 5 minutes |
| [LeepServerErrors](#leepservererrors) | page | more than 1% of requests answer 5xx for 5 minutes |
| [LeepLandErrors](#leeplanderrors) | page | `POST /v1/land` answered 5xx in the last 10 minutes |
| [LeepLandExhausted](#leeplandexhausted) | ticket | a land lost the ref compare-and-swap on all 32 attempts |
| [LeepLandBatchDeadline](#leeplandbatchdeadline) | ticket | a land batch gave up after retrying for 60 seconds |
| [LeepLandCasLossesHigh](#leeplandcaslosseshigh) | ticket | more than one lost compare-and-swap per land, for 15 minutes |
| [LeepLandAdmissionRefusing](#leeplandadmissionrefusing) | ticket | more than 10% of lands are refused because their ref's queue is full, for 15 minutes |
| [LeepLandForwardFailing](#leeplandforwardfailing) | ticket | servers fail to reach the server coordinating a ref's lands, for 10 minutes |
| [LeepLandLeaseFlapping](#leeplandleaseflapping) | ticket | land leases change hands more than six times a minute, for 10 minutes |
| [LeepIndexLagging](#leepindexlagging) | ticket | an index group trails ref moves by over 2 minutes for 10 minutes |
| [LeepIndexerMissing](#leepindexermissing) | page | an index group has had no lease holder (queries derive inline) for 5 minutes |
| [LeepIndexingAnswersHigh](#leepindexinganswershigh) | ticket | more than 5% of indexed reads answer 503 (index behind) for 10 minutes |
| [LeepLandSlowP99](#leeplandslowp99) | ticket | land p99 latency above 5 seconds for 10 minutes |
| [LeepRequestTimeouts](#leeprequesttimeouts) | ticket | more than 5 requests hit `--request-timeout` (408) in 15 minutes |
| [LeepTokenThrottled](#leeptokenthrottled) | ticket | one token has been limited steadily for 15 minutes |
| [LeepBroadThrottling](#leepbroadthrottling) | ticket | more than 5% of all requests are 429 for 15 minutes |
| [LeepBodyBudgetExhausted](#leepbodybudgetexhausted) | ticket | requests are refused because request bodies in flight fill `--max-inflight-bytes`, for 10 minutes |
| [LeepReadsShedding](#leepreadsshedding) | ticket | a server refuses more than half of its searches because its search class is full, for 15 minutes |
| [LeepBlobCacheColdHitRatio](#leepblobcachecoldhitratio) | ticket | blob cache hit ratio below 50% for 30 minutes |
| [LeepConfigReloadFailed](#leepconfigreloadfailed) | page | a SIGHUP reload was refused |
| [LeepTokensExpiringSoon](#leeptokensexpiringsoon) | ticket | a token expires within 7 days, or has expired and is still in the tokens file, for 1 hour |
| [LeepConfigSkew](#leepconfigskew) | ticket | servers report different config fingerprints for 15 minutes |
| [LeepSchemaVersionSkew](#leepschemaversionskew) | ticket | servers report different schema versions for 1 hour |
| [LeepMixedVersions](#leepmixedversions) | ticket | servers report different `version` for 2 hours |
| [LeepMirrorDown](#leepmirrordown) | page | a git mirror's metrics endpoint has not been scrapable for 5 minutes |
| [LeepMirrorLagging](#leepmirrorlagging) | page | a mirrored ref has differed between Leep and git for 10 minutes while the mirror retries |
| [LeepMirrorStuck](#leepmirrorstuck) | ticket | a mirrored ref is diverged, rewound, refused, remote-ahead or deleted in Leep for 15 minutes |
| [LeepBackupStale](#leepbackupstale) | ticket | no backup has passed its check for 2 days (or ever) |
| [LeepBackupCheckFailed](#leepbackupcheckfailed) | ticket | the newest backup is sealed but did not pass its check |
| [LeepBackupSlow](#leepbackupslow) | ticket | backup duration is growing week on week |
| [LeepBackupVerifyFailed](#leepbackupverifyfailed) | ticket | the weekly `backup-verify` failed or has not passed in 9 days |
| [LeepRestoreDrillOverdue](#leeprestoredrilloverdue) | ticket | no passing restore drill in 35 days |
| [LeepGcStale](#leepgcstale) | ticket | `leep-server gc` has not succeeded in 3 days |
| [LeepUnreachableGcStale](#leepunreachablegcstale) | ticket | the changeset sweep has not succeeded in 3 days (once it has run) |
| [LeepChangesetRescueBurst](#leepchangesetrescueburst) | ticket | writers rescue more than one condemned changeset a second for 15 minutes |
| [LeepChangesetDoomStuck](#leepchangesetdoomstuck) | ticket | writers get 409 on a changeset delete announced and never settled |
| [LeepGcFenceFlagStuck](#leepgcfenceflagstuck) | ticket | the changeset fence flag has been up for a day |
| [LeepChangesetCollectSlow](#leepchangesetcollectslow) | ticket | a changeset collect batch took over 5 s |
| [LeepDiskSpaceLow](#leepdiskspacelow) | ticket | a Leep data or backup filesystem has under 10% free |

Some rows depend on things the servers do not export:

- `LeepBackupStale` to `LeepGcStale`, and `LeepUnreachableGcStale`, read
  wrapper metrics from [Cron wrappers](#cron-wrappers).
- `LeepChangesetCollectSlow` reads the sweep's `--metrics-file`.
- `LeepDiskSpaceLow` reads node_exporter's filesystem metrics.

Servers export only the changeset sweep's writer-side metrics
(`leep_gc_changesets_rescued_total`,
`leep_gc_changeset_doom_conflicts_total`, `leep_gc_fence_active`).

## Before you start

**Rules that hold in every runbook.**

- **Refs move only by compare-and-swap.** Never edit the `refs` or `ref_log`
  tables by hand. To put a ref back, use `leep undo REF` (a client-side CAS
  against the ref log, ADR 0018). On a ref covered by `--require-signatures` or
  review, `undo` of a land is refused with 400 and the CLI tells you to
  `leep revert` the change and land the revert. `leep reflog REF -n 50` shows
  who moved it, when and from what.
- **Never run `leep-server gc --unsafe-no-fence` while any server is running**,
  and never give `gc` a `--fence` shorter than the servers' `--request-timeout`
  plus clock skew (ADR 0012). A fence that is too short is silent data loss: a
  stored changeset pointing at a deleted blob.
- **Never edit an applied migration or the `schema_migrations` table**, and
  never point an older binary at a migrated database. The only way back from a
  migration is restoring the backup taken before it (ADR 0023).
- **Never delete blob files, tree nodes or `derived` rows by hand.** Unreferenced
  blobs go through `gc`; derived rows go through `repair drop` or `repair
  rederive`. A reachable blob that is gone is recoverable only from backup.
- **Never restore over live stores.** `restore` goes into new, empty stores
  (`--force` deletes what is there and is only for stores no server uses).
- **A killed server is not damaged.** SIGKILL, OOM kills and host crashes are
  covered by the failure model (ARCHITECTURE.md, section 9): restarting is
  always safe.
- Take a backup before any step that cannot be undone, and check it with
  `backup-verify`.

**Running `leep-server`.** The offline subcommands (`verify`, `repair`, `gc`,
`migrate`, `backup`, `restore`, `backup-verify`) open the stores directly with
the same `--meta`/`--blobs` (`LEEP_META`/`LEEP_BLOBS`) as the servers. The
runbooks write `leep-server SUBCOMMAND`; run it as:

```sh
leep-server SUBCOMMAND ...                                                   # a host, with the server's environment
docker compose -f deploy/docker-compose.yml run --rm leep-server SUBCOMMAND ...   # the compose stack
docker run --rm -v leep-data:/var/lib/leep leep:dev SUBCOMMAND ...           # the single container
```

Most of them are safe beside running servers (`verify`, `gc`, `repair`,
`backup`, `backup-verify`, `derive-search`, `migrate --status`). `restore` is the
one that is not, and it refuses a non-empty target.

**Logs.** Every request logs one INFO line, `request completed`, with `status`
and `latency_ms` in the `request` span (`method`, `route`). A 5xx also logs an
ERROR, `request failed`, with the error text in the `error` field. With
`--log-format json`:

```sh
docker compose -f deploy/docker-compose.yml logs --since 30m leep-server \
  | jq -r 'select(.fields.message=="request failed") | .fields.error' | sort | uniq -c | sort -rn
```

The request line carries no token name. Token names appear in the DEBUG
authentication span and in `request limited` DEBUG lines (`RUST_LOG=leep_server=debug`),
which needs a restart because `RUST_LOG` is read at startup. The reason for a 429
or 413 is logged at DEBUG only; the INFO `request completed` line still shows its
status.

**Metrics.** Scrape `GET /metrics` on `--metrics-listen` (private port; the
compose stack does not enable it). To look at one server by hand:
`curl -s http://127.0.0.1:9187/metrics | grep '^leep_'`. Queries below use
Prometheus syntax; add `by (instance)` to see one server.

**Liveness and readiness.** `GET /v1/health` is liveness: it answers `"ok"`
whenever the process is serving HTTP and touches no store, so an `"ok"` with
failing requests means a store problem, not a dead server. `GET /v1/ready` is
readiness: it reads one ref from the metadata store and asks the blob store
about one fixed hash, concurrently, each under `--ready-timeout` (default 2 s).
It answers 200 `{"status":"ready","features":["land-id","indexing-retry","changeset-rescue","land-clean-blobs"]}`, or 503 with the failed store named:
`{"status":"not ready","failed":[{"store":"meta","reason":"timed out after
2000ms"}]}` (`reason` is `unavailable` or a timeout, never the driver's
message; that is in the server log as `readiness check failed`). Neither needs
a token or counts against limits. Restart on liveness, take out of rotation on
readiness.

## Collecting a bug report

Attach what applies, with secrets removed. The server never logs tokens or
digests, but `LEEP_META` may hold a database password: redact it.

1. Versions: `leep-server --version` on every instance, the image digest (two
   builds can share a version string, see [LeepMixedVersions](#leepmixedversions)),
   and `leep-server migrate --status`.
2. The alert's expression graphed for the hour around the event, and a raw
   `curl -s .../metrics` from each instance taken while the problem lasted.
3. Server logs for 15 minutes either side, in JSON, at least the WARN and ERROR
   lines and the `request completed` lines for the routes involved.
4. `leep-server verify --json > verify.json` (sample mode is cheap; add
   `--full` if you can wait). Exit status 0 clean, 1 problems, 2 could not run.
5. For anything that moved a ref: `leep reflog REF -n 200` and the changeset
   ids involved.
6. The flags and environment the servers run with (tokens, keys and policy
   files are not needed; their names and mounts are), the metadata backend
   (SQLite or Postgres and its version) and the blob backend (directory or S3).
7. For backup problems: the backup's output (`--json` for the report), the
   `manifest.json` of the newest snapshot, and `ls -l` of `snapshots/`.
8. What changed in the last day: deploys, config edits, new agents, a restore,
   a migration, a store maintenance job.
9. For a failed client command: its `request id` (the last line of `leep`'s
   error, `request_id` in `--json` errors, `(server request id: ...)` in an
   MCP tool error, or the `X-Request-Id` response header), its time, and the
   server log lines whose `request_id` matches:
   `jq -c 'select(.span.request_id == "ID")' server.log`. The id is random and
   says nothing about the request, so it is safe to share.

Never attach the tokens file, signing keys file, backup directory or database
dump: they hold repository content and credentials.

## LeepServerDown

**Meaning.** Prometheus could not scrape a server's `/metrics` for 2 minutes
(`up == 0`). **Page.** It means the process is gone, hung or unreachable, or that
only its metrics listener died while the API still serves. Replace
`job="leep"` in the rule with your scrape job's name.

**Impact.** If the process is down, requests to that instance fail (the CLI
reports a network error and exits 3) and the other instances behind the
balancer carry the load. With one instance it is a full outage. Nothing is
corrupted: servers keep no durable state of their own.

**Diagnose.**

1. Is the API up? `curl -fsS http://HOST:7070/v1/health` should print `"ok"`.
   API up and metrics down points at the metrics listener or the network path
   to it (step 4); API down points at the process (steps 2 and 3). `curl -s
   http://HOST:7070/v1/ready` says whether it can also reach its stores; a 503
   there is [LeepNotReady](#leepnotready), not a dead process.
2. Is the process running, and why did it stop? `docker compose -f
   deploy/docker-compose.yml ps`, `docker inspect -f '{{.State.OOMKilled}}
   {{.State.ExitCode}}' CONTAINER`, `systemctl status leep`. A container that
   restarts repeatedly (`restart: unless-stopped`) is a crash loop: read the last
   lines of its log.
3. Read the log around the last start. A normal stop logs `shutting down`
   (SIGTERM or SIGINT). A startup refusal prints `Error:` and a cause, for
   example:
   - `binding 0.0.0.0:7070` (port in use or no permission) or `binding metrics
     ADDR`;
   - `opening meta store` or `opening blob store` with the store's error (wrong
     URL, database down, bucket missing, credentials);
   - `database schema v3 is newer than this server (max v2); upgrade the server`;
   - `1 schema migration(s) pending (database is at v1, this server needs v2,
     first pending v2); run leep-server migrate, or start without --no-migrate`
     (only with `--no-migrate` / `LEEP_NO_MIGRATE=1`);
   - `migration v2 (name) differs from the one applied to this database ...;
     applied migrations are immutable`;
   - a config file error naming the file (`tokens file /etc/leep/tokens.json:
     bad JSON: ...`).
4. Metrics listener only: `ERROR metrics listener stopped` in the log; check
   that `LEEP_METRICS_LISTEN` is set (the compose stack does not set it), that
   the address is reachable from Prometheus, and `ss -ltnp | grep 9187`.
5. Out of memory: `dmesg -T | grep -i 'killed process'`. The blob cache is
   `--cache-bytes` (default 256 MiB), and request bodies are read whole, up to
   `--max-body-bytes` (default 512 MiB) each, so a few large concurrent uploads
   add to the cache.

**Likely causes.**

1. A deploy or config change that the server refuses to start with (step 3).
   Confirm: the log shows `Error:` right after the change.
2. Out of memory (step 5): `OOMKilled=true` or a `dmesg` line. Confirm: memory
   graph rising to the container limit before the stop.
3. Host, network or orchestrator failure. Confirm: other services on the host are
   down too, or the API health check fails from outside only.
4. The metrics listener is not reachable (firewall, wrong interface) while the
   API is fine. Confirm: step 1 answers and `curl` to the metrics port fails.
5. A hung process that still holds its port. Confirm: the port accepts
   connections but neither `/v1/health` nor `/metrics` answers; the store may be
   stalled (see [LeepServerErrors](#leepservererrors)).

**Mitigate / fix.**

- Restart the instance. It is always safe, whatever killed it.
- Fix a refusal by its cause. For `newer than this server`, deploy the newer
  binary; do not edit the database, and do not remove `--no-migrate` to make a
  server start on a schema it does not understand (it refuses regardless).
  For a pending migration, run `leep-server migrate` (after a backup), see
  [Upgrading](OPERATIONS.md#upgrading).
- For OOM, lower `--cache-bytes`, give the container more memory, or cap
  `--max-body-bytes`; do not disable the request timeout.
- With several instances, drain the dead one from the balancer first.

**Escalate / collect.** Exit code and `OOMKilled`, the log from before the stop to
the failed restart, `dmesg` lines, the `leep-server --version`, and the
[standard bundle](#collecting-a-bug-report).


## LeepNotReady

**Meaning.** `GET /v1/ready` has reported a store as failing or too slow for 5
minutes (`leep_ready_checks_total{result!="ok"}` rose, labelled with the `store`:
`meta` or `blobs`). **Page.** The server is running (liveness, `/v1/health`, is
fine) but cannot reach its metadata store (the readiness check reads one ref) or
its blob store (it asks about one fixed hash), within `--ready-timeout` (default
2 s). The counter only moves when something probes the endpoint, so this alert
needs an orchestrator or load balancer that calls `/v1/ready` (see
[DEPLOY.md](DEPLOY.md#probes)); without one it never fires, and
[LeepServerErrors](#leepservererrors) is the signal.

**Impact.** Requests to that server that need the failing store return 500 (see
[LeepServerErrors](#leepservererrors)), or hang until `--request-timeout`. A
readiness probe takes the server out of rotation, so with every server affected
there is nothing left to serve. Nothing is corrupted: the check only reads.

**Diagnose.**

1. Ask the server: `curl -si http://HOST:7070/v1/ready`. 200 means it recovered; a
   503 body names the store and says `unavailable` (the call failed) or `timed
   out after Nms` (it hung). The driver's own message is in the log as `WARN
   readiness check failed` with `store=` and `error=` (or `readiness check timed
   out`); it can name a host, so read it there, not in the response.
2. Which store, and how many servers: `sum by (instance, store, result)
   (increase(leep_ready_checks_total{result!="ok"}[5m]))`. One server only points
   at that host's network path or its local disk; all servers point at the shared
   store.
3. `meta`: Postgres up and accepting connections (`pg_isready`), connection slots
   (`SELECT count(*) FROM pg_stat_activity` against `max_connections`; each server
   holds a pool of 16), locks and long transactions, disk full. SQLite: the file
   is on local disk with space and not locked by a long job (`database is locked`).
4. `blobs`: filesystem mounted and not read-only (`df -h`, `touch` as the server's
   user), or for S3, the bucket, endpoint, credentials and clock skew.
5. `timeout` rather than `error`: the store is up but slow or saturated. Look at
   `leep_http_request_duration_seconds` and the store's own load. If the store
   is merely slow under load and the 2 s default is too tight, raise
   `--ready-timeout` (`LEEP_READY_TIMEOUT`); do not raise it to hide a stall.

**Likely causes.**

1. The metadata store is down, restarting or out of connections. Confirm: `meta`
   in the 503 body, step 3.
2. The blob volume or bucket is unavailable, unmounted or has bad credentials.
   Confirm: `blobs` in the 503 body, step 4.
3. A network partition between this server and a store. Confirm: only some
   servers fail; the store is fine from elsewhere.
4. A saturated store answering slowly. Confirm: `timeout` results and high
   latencies on every route.

**Mitigate / fix.**

- Fix the store; the server needs no restart. The next probe after the store
  answers returns 200 and the orchestrator puts it back.
- Do not restart the Leep servers for this: they hold no durable state and the
  problem is not in them. Do not point liveness at `/v1/ready`: a database blip
  would then restart every server at once.
- With several servers and only one failing, leave it out of rotation and fix its
  path to the store.

**Escalate / collect.** The 503 body, the `readiness check failed` log lines, the
store's own logs for the same minutes, and the
[standard bundle](#collecting-a-bug-report).

## LeepServerErrors

**Meaning.** More than 1% of HTTP requests answered 5xx for 5 minutes. **Page.**
Leep returns 500 only when a store call or an internal step fails
(`Storage`, `Encoding` and `Other` errors); user mistakes are 400, 404 and 409,
and neither those nor 408 and 429 count here. `GET /v1/health` and
`GET /v1/ready` are in the denominator, so busy probes dilute the ratio, and a
failing `/v1/ready` (503) counts as a 5xx. Indexing answers (503 with
`Leep-Indexing`, ADR 0058) are subtracted: see
[LeepIndexingAnswersHigh](#leepindexinganswershigh). So are searches shed
because their class was full (503 with `Leep-Refused`, ADR 0060): see
[LeepReadsShedding](#leepreadsshedding). So are lands not admitted because
their ref's queue was full (503 with `Leep-Refused: admission`), a
forwarded one twice since the follower relays the leader's 503: see
[LeepLandAdmissionRefusing](#leeplandadmissionrefusing).

**Impact.** Agents see failed commits, lands and reads; the CLI prints the
server's message and exits 3. Writes are idempotent, so retrying is safe, but
retries will keep failing until the cause is fixed.

**Diagnose.**

1. Where: `sum by (route, status) (rate(leep_http_requests_total{status=~"5.."}[5m]))`.
   One route points at a derived-data or content problem; every route points at
   a store.
2. Which error: group the `request failed` lines (see
   [Before you start](#before-you-start)). The text after `error` is the
   `leep_core::Error`: `storage error: ...` for stores, for example
   `storage error: corrupt tree node HASH`, `storage error: corrupt history node HASH`
   or `storage error: corrupt changeset row HEX`. Database and disk errors are
   the driver's or the OS's words: `pool timed out while waiting for an open
   connection` (Postgres pool of 16 connections per server exhausted),
   `database is locked` (SQLite, after its 30 s busy timeout), `database or disk is
   full`, `No space left on device`, connection refused or reset, S3 errors.
3. Are the stores healthy? `curl -s http://HOST:7070/v1/ready` names the store
   that does not answer. Postgres: `pg_isready`, `SELECT count(*) FROM
   pg_stat_activity` against `max_connections`, locks and long transactions.
   SQLite: the file is on local disk (WAL needs it), disk has space. Blobs:
   `df -h` and permissions on the directory, bucket reachability and
   credentials.
4. Are latencies up too? `histogram_quantile(0.99, sum by (le, route)
   (rate(leep_http_request_duration_seconds_bucket[5m])))`.
5. Damage: `leep-server verify` (read-only, safe beside servers).

**Likely causes.**

1. The metadata store is down, overloaded or out of connections. Confirm: errors
   on every route, step 2 shows connection or pool errors, step 3 agrees.
2. The disk holding SQLite, the blob directory or Postgres is full. Confirm:
   `df -h`, see [LeepDiskSpaceLow](#leepdiskspacelow).
3. The blob store is failing (S3 outage, credentials expired, mount gone).
   Confirm: errors mention the object store or the file path; reads and writes of
   blobs fail while metadata-only routes work.
4. A damaged tree node, history node or changeset row after a disk problem or bad
   restore. Confirm: 5xx on specific routes and refs only, `corrupt ...` errors,
   and `verify` reports `corrupt_tree`, `corrupt_history_node` or similar.
5. A bad deploy. Confirm: the rise starts at the deploy and ERROR messages are
   new.

**Mitigate / fix.**

- Fix the store, then do nothing to Leep: servers reconnect on their own and
  clients retry. A 5xx never leaves partial mutable state (ARCHITECTURE.md,
  section 9).
- For damage, follow [Verify and repair](OPERATIONS.md#verify-and-repair): fix
  history, trees and blobs first, derived rows after, with `repair rederive`.
- Roll back a bad deploy of the binary only if the schema did not change
  (`leep-server migrate --status`); otherwise roll forward.
- Do not run `gc`, `repair drop` or `restore --force` as a fix for 5xx.

**Escalate / collect.** The grouped error text, the per-route 5xx graph, store
health output, `verify --json`, and the [standard bundle](#collecting-a-bug-report).

## LeepLandErrors

**Meaning.** `POST /v1/land` answered 5xx at least once in 10 minutes: a store
failed while landing. **Page.** The rule counts HTTP 5xx on the land route, not
`leep_land_total{outcome="error"}`: that outcome counts every failed land except
a lost race, including refused lands (400: unsigned or unreviewed changeset, tag
ref, bad ref name) and unknown changesets (404), which are the caller's
doing and would page on every policy refusal. Lands not admitted because
their ref's queue was full (503 with `Leep-Refused: admission`, ADR 0060)
are subtracted: nothing was done, and clients send them again (see
[LeepLandAdmissionRefusing](#leeplandadmissionrefusing)).

**Impact.** The agent's land fails (CLI exit 3). `land` already retries a failed
commit from a fresh read up to three times in a row before it gives up, and the
commit that failed may or may not have applied.

**Diagnose.**

1. `request failed` ERROR lines around the time, see
   [LeepServerErrors](#leepservererrors) for what the text means.
2. Did the land actually apply? `leep reflog main -n 20` (a `land` entry by the
   token) and `leep log main -n 20`. A repeated land finds its earlier result and
   reports it, so repeating is safe; the one exception is a retry after more than
   1024 changesets landed in between (ADR 0021), where a duplicate stack is
   possible: look at the log first when in doubt.
3. Whether other routes fail too: if so, it is [LeepServerErrors](#leepservererrors).
   If only land fails, suspect a damaged object on the replay path (`corrupt tree
   node`, `corrupt changeset row`) and run `leep-server verify`.
4. The breakdown of outcomes: `sum by (outcome) (increase(leep_land_total[1h]))`
   against `sum by (status) (increase(leep_http_requests_total{route="/v1/land"}[1h]))`.

**Likely causes.**

1. The metadata store is failing (connections, locks, disk). Confirm: step 3, and
   store health as in [LeepServerErrors](#leepservererrors).
2. Disk full on the metadata or blob volume. Confirm: `df -h`, `No space left on
   device` or `database or disk is full` in the error text.
3. A damaged tree node or changeset row on the ref being landed on. Confirm:
   `corrupt ...` in the error and `verify` problems.
4. A blob store error while the server writes tree nodes or merge results.
   Confirm: errors name the object store.

**Mitigate / fix.**

- Fix the store; agents retry and `land` resolves ambiguous commits itself.
- If a land is reported failed but the log shows it landed, nothing to do. If it
  landed wrongly, `leep undo main` (unprotected refs) or `leep revert` plus a land
  (protected refs). Never edit the ref.
- Do not retry in a tight loop while the store is down; the CLI does not retry
  writes, so restart the agent step after the fix.

**Escalate / collect.** Error text, `leep reflog` and `leep log` for the ref,
`verify --json`, land metrics for the hour, and the
[standard bundle](#collecting-a-bug-report).

## LeepLandExhausted

**Meaning.** A land lost the ref compare-and-swap on all 32 attempts
(`leep_land_total{outcome="exhausted"}`). **Ticket**; page if it repeats for
half an hour. The client got HTTP 409 with `conflict: land onto "main" lost the
race 32 times; retry`. The rule also catches the first event of a series that
did not exist before (a counter that first appears at 1 has no earlier sample,
so `increase()` alone would report 0).

**Impact.** The agent's land fails with a 409 (CLI exit 5) and must be repeated;
nothing is half done. Retrying is safe.

**Diagnose.**

1. Lands within one server process are group committed per ref (ADR 0032)
   and never race each other, so CAS losses come from other processes writing
   the same ref: other `leep-server` instances,
   `leep-git import`, or clients using raw `POST /v1/refs/{name}`. Check how many
   instances take land traffic.
2. Who moves the ref: `leep reflog main -n 100`. The actor is the token name and
   the operation is `land`, `undo`, `import` or the client's own label (default
   `cas`). A burst of non-land entries is a raw-CAS loop.
3. Contention: `sum(rate(leep_land_cas_losses_total[10m]))` per instance,
   `histogram_quantile(0.99, sum by (le) (rate(leep_land_attempts_bucket[10m])))`,
   and `sum(rate(leep_land_attempts_sum[10m])) / sum(rate(leep_land_attempts_count[10m]))`
   (mean attempts per land). A loss redoes a whole batch, so one loss can show
   as several lands with 2 attempts; `leep_land_batch_size` shows how big the
   batches are.
4. Slowness that widens the race window: land p99 and
   [store health](#leepservererrors). Each loss waits a random backoff of at most
   200 ms.

**Likely causes.**

1. Many instances landing on one hot ref at once under heavy agent load. Confirm:
   losses on several instances, queue wait small compared with land duration
   (`leep_land_lock_wait_seconds`). Often one instance wins every swap and the
   others starve: their land p99 is tens of seconds and their batches are
   full while the winner's are small (`docs/PERFORMANCE.md`, "Several servers
   on one ref").
2. A raw-CAS writer or importer in a loop. Confirm: step 2.
3. A slow metadata store, so the head moves between a land's read and its swap.
   Confirm: land and request latencies up on all routes.
4. Very long unlanded stacks, whose replay takes long enough for the head to move.
   Confirm: `leep_land_duration_seconds` and `leep_derive_duration_seconds` high.

**Mitigate / fix.**

- Agents repeat the land (`leep sync`, then `leep land`).
- Turn on `--land-coordination` on every instance (`docs/OPERATIONS.md`,
  "Several servers on one ref", ADR 0056): one server per ref batches every
  land on it and the others forward to it. Until every instance runs it (a
  rolling upgrade), send `POST /v1/land` to a single instance (route by path
  at the balancer) instead.
- Find the looping writer by actor name; stop the agent, or revoke its token (edit
  the tokens file, then SIGHUP, see [DEPLOY.md](DEPLOY.md#reloading-config)).
- Ask agents to land smaller stacks and sync more often.
- Do not edit refs, and do not restart servers to "clear" it: there is nothing to
  clear. The 32-attempt bound is compiled in.

**Escalate / collect.** `leep reflog` excerpt (100 entries), the per-instance
attempts and CAS-loss graphs, the number of instances, which clients write the
ref, and the [standard bundle](#collecting-a-bug-report).

## LeepLandBatchDeadline

**Meaning.** A land batch was still retrying 60 s after its first attempt
(`MAX_BATCH_LIFETIME`, ADR 0059) and answered every land in it with HTTP 409
`conflict: land onto "main" did not finish within 60s; retry`
(`leep_land_batch_deadline_total`, one per land). **Ticket.** The bound
exists so that a changeset sweep's fence outlasts any batch that checked
for condemned changesets before it; it is not tunable.

**Impact.** The agents' lands fail with a 409 (CLI exit 5) and must be
repeated. The 409 does not always mean the ref did not move: an attempt
whose swap failed with an ambiguous store error may have applied before
the batch ran out of time. Repeating the land is still safe: a keyed land
(the CLI sends one) is answered from its land id record, and an unkeyed one
is found on the ref by the landed-copy scan while the copy is within its
window, so neither lands twice.

**Diagnose.**

1. Whether attempts or single steps are slow: land p99
   (`leep_land_duration_seconds`), mean attempts per land
   (`leep_land_attempts`), and `leep_land_cas_losses_total`. Many attempts
   point at contention ([LeepLandCasLossesHigh](#leeplandcaslosseshigh)); few
   but slow ones at the store.
2. Store health: request latencies on every route and the metadata store's
   own latency ([LeepServerErrors](#leepservererrors)). A batch that keeps
   failing on store errors also retries until the deadline.
3. Batch sizes (`leep_land_batch_size`) and stack lengths: a batch of 64
   long stacks replays a lot on every lost swap.

**Likely causes.**

1. A slow or overloaded metadata store. Confirm: step 2.
2. Several writers racing one hot ref. Confirm: as in
   [LeepLandExhausted](#leeplandexhausted).
3. Very long stacks landed together. Confirm: step 3.

**Mitigate / fix.** As in [LeepLandExhausted](#leeplandexhausted): agents
repeat the land, turn on `--land-coordination`, find looping writers, fix
the store. Do not restart servers to clear it.

**Escalate / collect.** Land latency, attempts and CAS-loss graphs per
instance, metadata store latency, and the
[standard bundle](#collecting-a-bug-report).

## LeepLandCasLossesHigh

**Meaning.** Over 15 minutes, on average more than one lost ref compare-and-swap
per land (`leep_land_cas_losses_total` against `leep_land_total`), sustained for
15 minutes. **Ticket.** It is the early warning for
[LeepLandExhausted](#leeplandexhausted): one process never loses to itself, so
losses are other writers of the same ref.

**Impact.** Lands take more attempts and longer (each loss adds up to 200 ms of
backoff and a replay); the land p99 rises. Nothing fails until a land loses 32
times in a row.

**Diagnose.** Same as [LeepLandExhausted](#leeplandexhausted), steps 1 to 4.
Compare `sum by (instance) (rate(leep_land_cas_losses_total[15m]))`: losses on
every instance mean all of them are racing each other; losses on one point at
something that writes through it. With a single server instance, losses should
be near zero, so a rate there means a raw-CAS or import writer.

**Likely causes.** As in LeepLandExhausted: several instances on a hot ref, a raw
writer in a loop, a slow metadata store.

**Mitigate / fix.** As in LeepLandExhausted. Rule of thumb for deciding to act: the
ratio is still below one when agents are merely busy; above it for hours means
the topology (several instances landing on one ref) no longer fits the load.
Do not add instances to speed up lands on one ref: they add cross-process
losses.

**Escalate / collect.** As in LeepLandExhausted.

## LeepLandAdmissionRefusing

**Meaning.** For 15 minutes, more than 10% of lands were refused because
their ref's queue was at `--land-queue-batches` (ADR 0060, section 2).
**Ticket.** A refusal is `503` `unavailable` with `Retry-After` and
`Leep-Refused: admission`, made before the changeset is read, counted in
`leep_land_admission_refused_total{route}` on the server that batches the
ref (`forwarded` when another server sent the land and relays the 503). It
is not a failure: nothing was queued or recorded, and
[LeepServerErrors](#leepservererrors) and [LeepLandErrors](#leeplanderrors)
do not count it. The ref is offered more lands than it can replay.

**Impact.** Lands take longer to get in, but none waits in the queue past
the bound, so clients stop timing out and resending into it. `leep land`
waits the `Retry-After` plus jitter and sends the land again under the same
land id, up to five times or 60 s, then exits 3 and keeps the land pending
for the next `leep land`. MCP agents get a retryable tool error with
`retry_after`. Clients older than the header report a 503.

**Diagnose.**

1. Which server and how full: `max by (instance) (leep_land_queue_batches)`
   against the `land_queue_batches` the server logged at startup (`expensive
   read and land admission limits`). Pinned at the bound means every
   batch is full behind the one replaying. With `--land-coordination on`,
   only the lease holder of the hot ref refuses; its followers relay
   (`route="forwarded"`).
2. How fast the ref lands: `sum(rate(leep_land_total{outcome="landed"}[15m]))`,
   `histogram_quantile(0.5, sum by (le) (rate(leep_land_batch_size_bucket[15m])))`
   and the batch time it implies (64 lands over the land rate). Batches of
   64 mean the ref is saturated.
3. Whether replays got slower: `leep_land_duration_seconds` and
   `leep_land_cas_losses_total` (another writer racing the ref, see
   [LeepLandCasLossesHigh](#leeplandcaslosseshigh)), and host CPU: on a
   small host expensive reads take the lands' cores
   ([LeepReadsShedding](#leepreadsshedding)).
4. Who sends: `sum by (status) (rate(leep_http_requests_total{route="/v1/land"}[15m]))`
   and the DEBUG log (`land refused: queue full`, with the ref and the
   batches ahead). A new fleet of agents, or agents resending without
   waiting?

**Likely causes.**

1. More agents landing on one ref than one server can replay. Confirm:
   step 1 pinned, step 2 full batches at a steady land rate.
2. Replays got slower: a slow metadata store, CPU taken by searches, CAS
   races with an uncoordinated server. Confirm: step 3.
3. A bound set lower than the host needs. Confirm: refusals while
   `leep_land_lock_wait_seconds` stays far below the clients' timeout.

**Mitigate / fix.**

- A larger queue is not the fix: it only moves the wait back into the
  queue until clients time out (ADR 0060). Raise `--land-queue-batches`
  only when lands wait well under half the clients' timeout
  (`leep_land_lock_wait_seconds`).
- Make replays faster: give lands their cores back (lower
  `--read-concurrency`, move indexing off the host, ADR 0058), fix a slow
  metadata store, and route the hot ref's lands to one server
  (`--land-coordination on`, or one instance at the balancer) so batches
  do not race.
- Spread the work over more refs: agents that land on review refs and
  merge in batches offer the hot ref fewer lands. More replay per ref
  (pipelined or parallel replay) is ADR 0056's next step.
- Make sure the balancer does not count these 503s against the backend
  (they carry `Leep-Refused`; OPERATIONS.md, "Balancers").
- `--land-queue-batches off` turns admission off. Lands then queue without
  bound and clients time out and resend again; do it only to rule the
  bound out.

**Escalate / collect.** The queue gauge, refusal and land rates per
instance, batch sizes and land durations, host CPU, the startup line, and
the [standard bundle](#collecting-a-bug-report).

## LeepLandForwardFailing

**Meaning.** With `--land-coordination on`, servers sent lands to the server
holding their ref's lease and got no answer, more than once every 10 seconds
for 10 minutes (`leep_land_forwarded_total{outcome="failed"}`). **Ticket.**

**Impact.** Those lands wait: the sender asks the holder again every half
second but never batches the land itself while the lease is live, and
answers 409 after 30 seconds. Agents retry. Lands sent to the holder itself
are unaffected. Nothing is lost or landed twice.

**Diagnose.**

1. Why: `sum by (reason) (rate(leep_land_forwarded_total{outcome="failed"}[10m]))`
   on the senders. `not_allowed` is step 2; `connect` and `timeout` are
   step 3; `reset` with a healthy network points at the holder (step 4).
   Which holder: `SELECT name, holder, address, epoch FROM leases WHERE scope = 'land'`
   on the metadata store, and the WARN lines `peer address ... is not
   allowed` on the senders.
2. An address the senders refuse: it is outside their `--peer-url-prefix` /
   `--peer-cidr`, or is plain http to a public host. Compare the holder's
   `--advertise-url` with the senders' allow-list; a server whose list
   differs from the rest is a config skew.
3. An address that does not answer: from a sender's host, `curl -i
   <address>/v1/health` should answer 200 with a `Leep-Instance` header equal
   to the lease's `holder`. A firewall between servers, or a balancer
   address used as `--advertise-url`, shows here. (Servers check their own
   address at startup, so a typo stops the server rather than this alert.)
4. A holder that is alive but overloaded: its request latency and CPU.

**Mitigate / fix.** Fix the allow-list or the network so servers reach each
other's `--advertise-url` directly. To get lands flowing at once, restart (or
SIGTERM) the holder: it releases its leases on shutdown, and the next land
elsewhere takes them. As a last resort, `--land-coordination off` everywhere
brings back the old behaviour, with its contention.

**Escalate / collect.** The `leases` rows of scope `land`, each server's flags, the
WARN lines and the [standard bundle](#collecting-a-bug-report).

## LeepLandLeaseFlapping

**Meaning.** Land leases were taken more than six times a minute for 10
minutes (`leep_land_lease_acquired_total`). **Ticket.** A lease changes hands
only when its holder failed to renew it for the TTL (2 s), released it after 30 s
idle, or shut down.

**Impact.** Each change costs the lands in flight on the old holder a
reroute, and lands batched on both sides for a moment race the CAS as
without coordination. Correctness is unaffected.

**Diagnose.**

1. `leep_land_lease_lost_total` by `reason` on each server: `taken` means a
   holder failed to renew; `idle` with reacquisitions means refs that see a
   land every half minute or so (harmless churn, raise nothing).
2. Renewals failing: the metadata store's latency and errors (a renewal is
   one `UPDATE` every 500 ms per server, on connections reserved for lease
   calls, so a busy pool does not delay it; a slow or overloaded database
   does), and the servers' logs.
3. Servers restarting: `LeepServerDown`, deploy history.
4. Clock: Postgres judges lease times by its own clock, so server clocks do
   not matter; SQLite judges by the host's clock.

**Mitigate / fix.** Fix the store's latency. If renewals are slow but
succeed, raise `--land-lease-renew` and `--land-lease-ttl` together on every
server (for example 1s and 4s).

**Escalate / collect.** The lease metrics of every server, store latency
graphs and the [standard bundle](#collecting-a-bug-report).

## LeepIndexLagging

**Meaning.** `leep_index_lag_seconds` of one index group has been above 120 s
for 10 minutes: the lease holder's passes start on ref moves more than two
minutes old (ADR 0058). **Ticket.**

**Impact.** Queries on new heads scan more un-indexed history and are slower;
past the budgets, CLI requests wait and retry, MCP tools answer at an older
changeset (`answered_at`), and requests without the header get 503 after
their wait. Lands are never slowed.

**Diagnose.**

1. Who holds it: `leep_index_lease_held{kind}` per server, or `SELECT holder,
   address, epoch, meta FROM leases WHERE scope = 'index'`. `meta` holds the
   holder's rate (changesets a second) and backlog.
2. Is it deriving: `rate(leep_index_derived_total{kind}[5m])` on the holder,
   and its `leep_warm_duration_seconds`. A backfill (an import, a dropped or
   new kind) shows a large `leep_index_backlog_changesets`.
3. Is it starved: the holder's CPU, and the metadata store's latency (a
   changeset costs about two store round trips per group).
4. Lease churn: `leep_index_lease_acquired_total` rising means holders keep
   changing (restarts, failed renewals), each new holder starting cold.

**Mitigate / fix.** Give the holder CPU: move query traffic off it (balancer
weights), or run it on a host of its own with `--indexing lease` and the
others `off`. Split groups over servers with `--index-kinds` (one server
each for `history`, `log-filter`, `search`). For a backfill, run
`leep-server repair rederive --kind history-tree-v4 log-filter-v1 --missing
--jobs N` on a machine of its own; it takes the leases and waits the fence
first.

**Escalate / collect.** Lease rows, the holder's metrics and logs, store
latency, and the [standard bundle](#collecting-a-bug-report).

## LeepIndexerMissing

**Meaning.** For 5 minutes an index group had no lease holder among the
servers in `lease` mode, or queries in `lease`/`off` mode derived inline
because no lease was live. **Page.**

**Impact.** Queries derive the index inside requests again, on every query
server (the pre-ADR 0058 behaviour): slower reads, timeouts under load,
duplicate derivation. Answers stay correct; lands are unaffected.

**Diagnose.**

1. Which servers are candidates: those started with `--indexing lease` (and
   `--index-kinds` including the group, and `--warm-interval` not 0). Are any
   up (`LeepServerDown`)?
2. Their logs for `took the index lease` / `lost the index lease`, and
   `leep_index_lease_lost_total{reason}`: `store` means renewals failed.
3. A tool holding it: `SELECT holder FROM leases WHERE scope = 'index'`
   names `repair-...` or `upgrade-index-...` while one runs; servers wait
   until it releases.

**Mitigate / fix.** Start (or restart) a server in `--indexing lease` mode.
If the store fails lease calls, fix the store. As a stopgap, switch query
servers back to `--indexing local`.

**Escalate / collect.** The candidates' logs and lease metrics, the lease
rows, and the [standard bundle](#collecting-a-bug-report).

## LeepIndexingAnswersHigh

**Meaning.** More than 5% of index-backed reads (log, last-change, blame,
search) answered 503 with `Retry-After` for 10 minutes because their index
was further behind than the scan budgets (ADR 0058). **Ticket.**

**Impact.** The CLI retries for up to a minute and then fails with exit code
3 and error code `indexing`; scripts without the header wait and then fail.
MCP tools answer at an older changeset instead.

**Load balancers.** These 503s are an expected answer, not a sick backend:
configure passive health checks and balancer retries to ignore responses
that carry a `Leep-Indexing` response header (otherwise one lagging index
takes every query server out of rotation, or multiplies the load by
retrying elsewhere).

**Diagnose.** First [LeepIndexLagging](#leepindexlagging) and
[LeepIndexerMissing](#leepindexermissing). `leep_query_indexing_answers_total`
by `route` and `mode` says which queries and clients; `leep_query_unindexed_scanned_total`
shows how much they scan.

**Mitigate / fix.** Fix the indexer's lag. If the lag is normal for the
fleet's load, raise the budgets on the query servers (`--query-scan-budget`,
`--blame-scan-budget`, `--search-scan-budget`, `--search-scan-bytes`):
each scanned changeset costs a few reads.

**Escalate / collect.** The metrics above, lease rows and the
[standard bundle](#collecting-a-bug-report).

## LeepLandSlowP99

**Meaning.** The 99th percentile of the whole land call, including the time
queued for the ref's next batch, has been above 5 seconds for 10 minutes. **Ticket.** The
histogram tops out at 10 s, so very slow lands read as 10 s.

**Impact.** Agents wait; lands to one ref queue in the server and are landed in
batches (ADR 0032), each land replayed after the ones ahead of it, so the
latency of the busiest ref grows with the number of agents landing on it.

**Diagnose.**

1. Queueing or work? Mean queue wait against mean duration:
   `sum(rate(leep_land_lock_wait_seconds_sum[10m])) / sum(rate(leep_land_lock_wait_seconds_count[10m]))`
   and the same for `leep_land_duration_seconds`. If the wait is most of the
   latency, the offered load exceeds what one ref can replay (see the soak notes
   in [OPERATIONS.md](OPERATIONS.md#soak-tests) and "Concurrent lands on one
   ref" in [PERFORMANCE.md](PERFORMANCE.md)). `leep_land_batch_size` near 64
   means every batch is full.
2. Offered load: `sum(rate(leep_land_total[5m]))`.
3. Retries: mean attempts per land (see
   [LeepLandExhausted](#leeplandexhausted)); above 1 means CAS contention.
4. Cold derived data: `sum by (kind) (rate(leep_derive_total[5m]))` high and
   `leep_derive_duration_seconds` by kind. Derived rows are computed on first use;
   after a restore or a `repair drop` the first land or query derives a whole
   history.
5. Store latency: [LeepServerErrors](#leepservererrors), step 3.

**Likely causes.**

1. More landing agents than one ref can replay. Confirm: step 1.
2. Agents landing stacks that are far behind main: a land costs more the further
   behind its stack is (replay). Confirm: durations grow with stack age, not with
   count.
3. A slow metadata or blob store. Confirm: all routes slow.
4. Derived data being rebuilt after a restore, `repair drop` or a very large
   import. Confirm: step 4.
5. CAS contention between instances. Confirm: step 3.

**Mitigate / fix.**

- Warm derived data instead of waiting for users: `leep-server repair rederive --kind
  all` (safe beside servers), and `leep-server derive-search main` for search.
  After an upgrade that changed an index, `leep-server upgrade-index` shows
  which indexes are incomplete and `--yes` builds them (resumable; see
  [Upgrading](OPERATIONS.md#upgrading)).
- Ask agents to `leep sync` more often so stacks stay short; lengthen the agents'
  think time or reduce how many land on one ref.
- Scale or tune the metadata store.
- Do not add server instances to parallelize lands on one ref: lands are only
  batched within a process, so more instances mean more lost swaps, and each
  loss redoes a whole batch.

**Escalate / collect.** Land duration, queue-wait and batch-size histograms, derive rates by
kind, lands per second, and the [standard bundle](#collecting-a-bug-report).

## LeepRequestTimeouts

**Meaning.** More than 5 requests were aborted with 408 by `--request-timeout`
(default 5 minutes) within 15 minutes. **Ticket.** A timed-out request stops and
writes nothing further, but a statement already sent to Postgres can still commit
afterwards, until Postgres cancels it at `--meta-statement-timeout` (default the
request timeout plus 5 s; ADR 0012).

**Impact.** Those requests fail (CLI exit 3). Repeating is safe, but if the cause
is structural (an upload that can never finish in time) it fails again.

**Diagnose.**

1. Which routes: `sum by (route) (increase(leep_http_requests_total{status="408"}[1h]))`.
2. Latency of those routes: `histogram_quantile(0.99, sum by (le, route)
   (rate(leep_http_request_duration_seconds_bucket[10m])))`. The histogram measures
   until the response head is ready, not the body stream.
3. Uploads: `PUT /v1/blobs/{hash}` and `POST /v1/changesets` timing out means big
   blobs on slow links. Searches: `/v1/search/{rev}` timing out on a never-indexed
   tree means the index is deriving (about a second per 20 000 files on in-memory
   stores, longer on real ones).
4. Saturation: `sum(leep_requests_in_flight)` and
   [LeepTokenThrottled](#leeptokenthrottled).
5. The current setting: the server's `--request-timeout` / `LEEP_REQUEST_TIMEOUT`,
   and the `--fence` your GC runs with.

**Likely causes.**

1. Large uploads over a slow link. Confirm: step 3, a `PUT /v1/blobs/{hash}`
   route, and a handful of clients.
2. A slow blob or metadata store. Confirm: all routes slow.
3. A cold search index or history index. Confirm: `/v1/search/{rev}` or `/v1/log/{id}`
   routes, and `leep_search_total{indexed="false"}`/`leep_derive_total` rising.
4. A paused or overloaded host. Confirm: CPU steal, swapping, or a VM freeze (a
   server paused longer than the timeout can resume a request after its deadline).

**Mitigate / fix.**

- For legitimate big uploads, raise `--request-timeout` **and** `--fence` of every
  `gc` job together: the fence must exceed the request timeout plus clock skew
  plus a margin. A timeout that reaches the fence makes GC unsafe. Never set
  `--request-timeout 0` on servers that GC runs beside (the server warns
  `request timeout disabled: gc --fence cannot be guaranteed safe`).
- Warm indexes before an agent swarm starts: `leep-server derive-search main`,
  `leep-server repair rederive --kind all`.
- Fix the slow store, see [LeepServerErrors](#leepservererrors).
- After a long VM freeze, restart the servers.

**Escalate / collect.** Routes and latency graphs, the server flags, the GC
command lines, and the [standard bundle](#collecting-a-bug-report).

## LeepTokenThrottled

**Meaning.** One token (or `anonymous`) is refused by a request limit at more than
0.5 requests per second for 15 minutes, per `reason`: `rate`, `concurrency` or
`size`. **Ticket.** Either an agent is stuck in a retry loop or its limit is too low
for legitimate use.

**Impact.** That token's requests get 429 with `Retry-After` (`rate limit
exceeded; retry after Ns`, `too many concurrent requests; retry after 1s`) or 413
(`request body exceeds the limit of N bytes for this token`). The CLI retries a
throttled GET up to four tries, waiting no more than 10 seconds each; writes
(commit, land) fail at once. Other tokens are unaffected, which is the point of the
limits.

**Diagnose.**

1. Who and why: `topk(5, sum by (token, reason) (rate(leep_requests_limited_total[10m])))`.
2. Is it a loop or real work? `leep_requests_in_flight{token="NAME"}` against its
   concurrency limit, and the request rate of the whole fleet. A flat, high refusal
   rate with no change in real work is a loop. Enable `RUST_LOG=leep_server=debug`
   (restart needed) to see `request limited` lines with the token and reason, and
   the routes the token hits.
3. `reason="concurrency"` with a low request rate means slow requests are holding
   slots: look at route latencies before raising the cap.
   `reason="size"` is almost always a client bug (a read token uploading).
4. Anonymous mode: `token="anonymous"` is every client address when there is no
   tokens file, and behind a proxy it is a single bucket for everybody
   ([DEPLOY.md](DEPLOY.md#request-limits)).
5. Limits are per server process: with N servers, each applies the whole limit.

**Likely causes.**

1. A runaway agent retrying. Confirm: step 2.
2. A limit set too low for a legitimate workload, such as a CI or merge bot sharing
   the default. Confirm: the token's work is real and steady.
3. Slow requests holding concurrency slots (`reason="concurrency"`). Confirm: step 3.
4. Anonymous mode behind a proxy. Confirm: step 4.
5. A recent change to the defaults (`LEEP_RATE_LIMIT`, `LEEP_RATE_BURST`,
   `LEEP_MAX_CONCURRENT_PER_TOKEN`). Confirm: deploy history.

**Mitigate / fix.**

- Runaway agent: stop it, fix its loop, or revoke its token (edit the tokens file,
  SIGHUP; requests already admitted finish, up to `--request-timeout`).
- Legitimate load: give the token its own entry in the tokens file with `rate`,
  `burst` and `concurrency` (per-token fields reload on SIGHUP; the flag defaults
  need a restart). Set the rate near twice the steady rate and the burst to the
  largest legitimate burst; see [Request limits](OPERATIONS.md#request-limits).
- Do not give everyone `"unlimited"`, and do not raise the default to fix one
  token.
- Anonymous behind a proxy: enable tokens, and rate-limit at the proxy.

**Escalate / collect.** The `topk` table, in-flight graph, the token's tokens-file
entry (without its hash), the server's limit flags and the number of servers.

## LeepBroadThrottling

**Meaning.** More than 5% of all HTTP requests answered 429 for 15 minutes.
**Ticket.** It catches what [LeepTokenThrottled](#leeptokenthrottled) cannot: many
tokens each just under 0.5 refusals per second, or the shared `anonymous` bucket.
Only the request limits produce 429.

**Impact.** A large part of the fleet is slowed or failing: reads are retried by
the CLI, writes fail at once.

**Diagnose.**

1. `sum by (route) (rate(leep_http_requests_total{status="429"}[10m]))` and
   `sum by (token, reason) (rate(leep_requests_limited_total[10m]))`: is it one
   route, one reason, one token, or everyone?
2. `token="anonymous"` dominant: no tokens file, or a proxy collapsing all clients
   into one address.
3. Did the fleet grow, or did the limits shrink? Compare with the deploy history
   and the agent count. Each server applies the whole limit.
4. Slow requests holding concurrency slots: route latencies.

**Likely causes.**

1. Defaults sized for a smaller fleet. Confirm: refusals spread over many tokens,
   most with real work.
2. Anonymous mode behind a proxy (one bucket). Confirm: step 2.
3. A new agent swarm or a retry storm after an outage. Confirm: request rate up
   across the board, often right after another alert.
4. Slowness (store, cold index) filling concurrency slots. Confirm: step 4.

**Mitigate / fix.** Fix the slowness first if there is one. Otherwise raise the
defaults with a restart of every server (they are flags), and give heavy
tokens their own entries. Enable tokens in anonymous deployments. Do not remove the
limits to make the alert go away: they are what keeps one agent from starving the
rest.

**Escalate / collect.** The 429 breakdown by route, token and reason, the limit
flags, the agent count, and the [standard bundle](#collecting-a-bug-report).

## LeepBodyBudgetExhausted

**Meaning.** More than 0.05 requests a second (3 a minute) on one server were
refused with `503` because the bodies of the requests already in flight fill
`--max-inflight-bytes` (default 1 GiB), for 10 minutes. **Ticket.** Each request
with a body holds its `Content-Length` of the budget (its route's cap,
`--max-blob-bytes` or `--max-json-body-bytes`, when it declares none) until its
handler finishes. These refusals also count as 5xx in
[LeepServerErrors](#leepservererrors).

**Impact.** Uploads, commits, lands and other requests with a body fail with
`server busy: the request bodies in flight are at this server's limit; retry
after 1s`. `leep` tries a blob upload up to three times within 10 seconds,
then exits 3; running the command again works once the budget frees up. A
token past its share of the budget gets `429` instead (`reason="principal"`),
which does not count toward this alert. Reads without a body are unaffected.

**Diagnose.**

1. How full: `leep_request_body_bytes_in_flight / leep_request_body_budget_bytes`
   per instance. Pinned near 1 is a real shortage; spikes are bursts.
2. Who holds it: `sum by (token) (leep_requests_in_flight)` (tokens with a limit
   only) and `sum by (route) (rate(leep_http_requests_total{route=~"/v1/blobs/.*|/v1/changesets"}[5m]))`.
3. Slow or fast: upload latency, `histogram_quantile(0.99, sum by (le) (rate(leep_http_request_duration_seconds_bucket{route="/v1/blobs/{hash}"}[10m])))`.
   Many slow uploads with a low request rate mean clients trickle their bodies.
4. Bodies with no declared length are charged the whole route cap. A client
   that streams (`Transfer-Encoding: chunked`) holds the blob cap (256 MiB by
   default) per blob upload however small the blob is; a streamed JSON body
   holds 1 MiB.
6. One token at its share: `sum by (reason) (rate(leep_request_body_refused_total[10m]))`
   with `reason="principal"` (429s) means a token holds a quarter of the budget;
   `RUST_LOG=leep_server=debug` names it.
5. The settings: `--max-inflight-bytes`, `--max-blob-bytes`,
   `--max-json-body-bytes` (logged at startup as `request body limits`), and
   the memory the container has.

**Likely causes.**

1. A burst of large uploads: an agent swarm committing generated or binary
   files, or a git mirror importing a large push. Confirm: step 2, many PUTs at
   once, and a budget that drains within minutes.
2. Slow uploaders holding their share, including a client declaring large
   bodies and sending them slowly. Confirm: step 3, few requests per second with
   the gauge high.
3. Clients that stream bodies without a length. Confirm: step 4, the gauge
   rising by the route cap per upload.
4. A budget too small for the fleet. Confirm: steady legitimate traffic and the
   container's memory well above what the server uses.

**Mitigate / fix.**

- A runaway or slow client: give its token a `concurrency` limit (tokens file,
  SIGHUP) so it cannot hold more than that many shares, or revoke it.
- Legitimate load with memory to spare: raise `--max-inflight-bytes` (a flag, so
  a restart of each server). Keep cache sizes plus budget within the container.
- Large files that do not belong in the repository: lower `--max-blob-bytes`, or
  keep them out of the tree.
- Do not set the budget far above the container's memory to quiet the alert: it
  is what stops many uploads at once from getting the process killed.

**Escalate / collect.** The gauge and refusal graphs per instance, upload
latency, the three flags, the container memory limit, and the
[standard bundle](#collecting-a-bug-report).

## LeepReadsShedding

**Meaning.** For 15 minutes, one server refused more than half of its
searches because every permit of its search class was in use
(`--read-concurrency`, ADR 0060). **Ticket.** A refusal is `503`
`unavailable` with `Retry-After` and `Leep-Refused: read-class`, counted in
`leep_read_class_shed_total{class="search"}`. It is not a failure, and
[LeepServerErrors](#leepservererrors) does not count it. Searches are wanted
faster than the host can run them.

**Impact.** Lands and cheap reads are unaffected: that is what the limit
protects. Searches are slower to get. `leep search` waits the `Retry-After`
plus jitter and tries up to four times, then exits 3 with `server busy`; MCP
agents get a retryable tool error with `retry_after` and call again. Some
searches lose every try, because a refused client can keep losing the next
free permit to new arrivals.

**Diagnose.**

1. How full: `leep_read_class_in_flight{class="search"}` against
   `leep_read_class_permits{class="search"}` per instance. Pinned at the
   permits means every permit is busy.
2. How slow: `histogram_quantile(0.5, sum by (le) (rate(leep_http_request_duration_seconds_bucket{route=~"/v1/search/.*",status="200"}[15m])))`.
   Admitted searches that got slower point at CPU or the metadata store.
3. Who: `sum by (route, status) (rate(leep_http_requests_total{route=~"/v1/search/.*"}[15m]))`
   and the DEBUG log (`read refused: class full`). A burst after a deploy of
   agents, or one agent in a loop?
4. The host: CPU use of `leep-server` and of Postgres. If the cores are not
   busy while searches are refused, the class is set too low for the host.
5. The settings, logged at startup as `expensive read limits`:
   `read_concurrency` and `search_pool_permits`.

**Likely causes.**

1. More agents, or agents searching more often, than the host has cores for.
   Confirm: step 1 pinned, step 4 cores busy.
2. Expensive queries: very common words, broad regexes, or searches whose
   index is cold (a new large import), each holding its permit longer.
   Confirm: step 2 p50 up, the same queries in the logs.
3. A class set lower than the host needs. Confirm: step 4 cores idle with
   step 1 pinned.

**Mitigate / fix.**

- Add query capacity: more servers behind the balancer, or the indexing
  split of ADR 0058, which adds cores. Each server has its own permits.
- On a host with idle cores, raise `--read-concurrency search=N` and restart.
  Do not raise it on a host whose lands are slow: the limit is what keeps
  them fast (`LeepLandSlowP99`).
- Make sure the balancer does not count these 503s against the backend (they
  carry `Leep-Refused`; OPERATIONS.md, "Balancers"), or it will pull shedding
  servers out and overload the rest.
- `--read-concurrency search=off` turns the limit off. Do it only to rule the
  limit out: searches then take the lands' cores again.

**Escalate / collect.** The in-flight, permits and shed graphs per instance,
search latency, host CPU, the startup line, and the
[standard bundle](#collecting-a-bug-report).

## LeepBlobCacheColdHitRatio

**Meaning.** Fewer than half of blob reads were served from the in-memory cache,
over 15 minutes, for 30 minutes (summed over all servers). **Ticket.** A miss is a
read from the durable store, which is slower and (on S3) costs requests. Tree nodes
are blobs too, so they count. With `--cache-bytes 0` the series do not exist and the
alert cannot fire.

**Impact.** Higher latency and more load on the blob store. Nothing fails.

**Diagnose.**

1. Occupancy against the limit: `leep_blob_cache_bytes` and `leep_blob_cache_entries`
   per instance against `--cache-bytes` (default 256 MiB). A cache sitting at its
   limit is evicting.
2. Per-instance ratio: `sum by (instance) (rate(leep_blob_cache_hits_total[15m])) /
   (sum by (instance) (rate(leep_blob_cache_hits_total[15m])) + sum by (instance)
   (rate(leep_blob_cache_misses_total[15m])))`. One cold instance is a restart.
3. Workload: `rate(leep_search_total{indexed="false"}[15m])` (scans read many
   files), large checkouts, new agents on other parts of the tree.
4. Blob sizes: a blob larger than the whole cache is never cached, so every read of
   it is a miss.

**Likely causes.**

1. A restart or deploy: caches start empty. Confirm: the server's uptime. It heals
   by itself.
2. The working set is larger than `--cache-bytes`. Confirm: step 1 at the limit and
   the ratio falling as traffic grows.
3. Scan-heavy traffic (unindexed searches, many whole checkouts). Confirm: step 3.
4. Large blobs that never fit. Confirm: step 4.
5. Several instances behind a balancer that spreads one workload thinly over all
   caches. Confirm: every instance equally low.

**Mitigate / fix.** Raise `--cache-bytes` (a flag, so a restart) and give the
container the memory: resident size is the cache plus request buffers. Warm what
searches need with `leep-server derive-search main`. For scans, narrow the queries
with a `path`. Do not set `--cache-bytes 0` to quiet the alert: it removes the cache
and the series.

**Escalate / collect.** Cache gauges and hit and miss rates per instance, search
and derive rates, `--cache-bytes`, the blob store type and its read latency.

## LeepConfigReloadFailed

**Meaning.** A SIGHUP reload was refused in the last 10 minutes: the server is
healthy but running its previous config (`leep_config_reload_total{result="error"}`
increased). **Page**, because the edit may have been a revocation: a token you meant
to remove still works. **The alert clears after 10 minutes even though the config
is still stale**; verify the fix, do not wait for the alert to resolve.

**Impact.** None visible to agents; the server keeps serving the old tokens, signing
keys and review policy. A restart does not help: it applies the same checks and
refuses to start on the bad file.

**Diagnose.**

1. The log line: `ERROR config reload failed; keeping the previous config` with
   `error=` naming the file and the reason, for example `tokens file
   /etc/leep/tokens.json: bad JSON: ...`, `cannot read tokens file PATH: ...`,
   `duplicate token name "ci"`, `token "agent-7": ...`, `request limits: ...`,
   `--require-signatures needs --signing-keys-file`, or `--require-review and
   --review-policy-file need --tokens-file`. A line `config reload task failed` is
   an internal failure of the reload task. `SIGHUP: no tokens, signing keys or
   review policy file to reload` means the server has no file configured.
2. Which instance: the counter is per server; `sum by (instance)
   (increase(leep_config_reload_total{result="error"}[10m]))`.
3. Was the file half written, or not yet synced to that host (a ConfigMap volume
   updates after a delay)? Compare `sha256sum` across hosts.
4. Validate offline with the startup checks, which are the same as a reload's. Use
   throwaway in-memory stores and the same flags as production (`--tokens-file`,
   `--signing-keys-file`, `--review-policy-file`, `--require-signatures`,
   `--require-review`):
   ```sh
   timeout 3 leep-server --listen 127.0.0.1:0 --meta mem:// --blobs mem:// \
     --tokens-file new/tokens.json --signing-keys-file new/signing-keys.json
   ```
   A bad file prints the same error and exits at once; a good one starts listening
   and `timeout` stops it.

**Likely causes.**

1. A syntax or schema error in the edited file (trailing comma, unknown field,
   bad `blake3:` digest, duplicate name). Confirm: step 1.
2. An inconsistent set of files: a review policy without a tokens file,
   `--require-signatures` without keys, a ref name with a stray space. Confirm:
   step 1.
3. A file written non-atomically, or missing on one host. Confirm: step 3.
4. A single-file bind mount that still points at the old inode after a rename.
   Confirm: the host's file is right but `changed=false` follows every reload; mount
   the directory instead, see [DEPLOY.md](DEPLOY.md#reloading-config).

**Mitigate / fix.**

1. If the edit was a revocation and you cannot fix the file quickly, put back the
   last good file with only the revoked entry removed, rather than anything new.
2. Fix the file, write it atomically (save beside it, then `mv` over it), validate
   it (step 4), then `kill -HUP "$(pidof leep-server)"` (or `docker kill
   --signal=HUP leep`, or `docker compose kill -s HUP leep-server`) on every
   instance.
3. Confirm per instance: `config reloaded` with a higher `generation` and, if
   you changed the file, a new `fingerprint` (also `leep_config_info == 1` for
   that fingerprint), and `leep_config_last_reload_timestamp_seconds` is recent. Try the revoked token (`LEEP_TOKEN=... leep refs`) and expect an authentication
   error (401).
4. Do not restart the server to "pick up" a bad file; do not edit flags expecting
   SIGHUP to apply them (listeners, stores, cache size, request timeout, size and
   limit defaults, and the `--require-*` refs need a restart).

**Escalate / collect.** The full `config reload failed` line, the file with
digests and keys removed, and the [standard bundle](#collecting-a-bug-report).

<a id="leepconfiggenerationskew"></a>

## LeepTokensExpiringSoon

**Meaning.** At least one entry of the tokens file has an `expires` time within
the next 7 days (`leep_tokens_expiring_soon`), or in the past while the entry is
still listed (`leep_tokens_expired`), for an hour. **Ticket.** Expiry is how
short-lived agent credentials end by themselves; this is the reminder to rotate
the ones still needed and to remove the rest.

**Impact.** None yet for a token expiring soon. Once it expires, every request
with it gets 401 with code `token_expired` (`authentication required: token
expired at <time>`), which `leep` reports with a hint to get a new token; the
agent stops working. Other tokens are unaffected.

**Diagnose.**

1. Which tokens: the server logs `tokens expire within 7 days` and `tokens in
   the tokens file have expired and are refused` with their names at startup and
   on every SIGHUP. Without access to logs, read the file:
   `jq -r '.tokens[] | select(.expires) | "\(.expires) \(.name)"' tokens.json | sort`.
2. Still in use? `sum by (token) (increase(leep_token_expired_requests_total[1h]))`
   shows expired tokens that agents still present.
3. Several servers: each reads its own file; compare
   [LeepConfigSkew](#leepconfigskew) if only some servers report it.

**Likely causes.**

1. A token created with `gen-token --expires-in` is nearing its end as
   intended. Confirm: step 1, a name you recognise.
2. An agent or job that finished long ago, whose entry nobody removed. Confirm:
   step 2 shows no requests.
3. A clock problem on the server (a time far ahead makes tokens look expired).
   Confirm: `date -u` on the host against a reference.

**Mitigate / fix.**

- Still needed: `leep-server gen-token --name NAME --role ROLE --expires-in 30d`,
  replace the entry's `hash` and `expires` (or add a second entry and remove the
  old one later), SIGHUP every server, then hand the new token to the agent
  ([Reloading config](DEPLOY.md#reloading-config)).
- No longer needed: remove the entry and SIGHUP; the gauge drops on the reload.
- To extend without rotating, change only `expires` and SIGHUP; the token
  itself does not change.
- Do not remove `expires` from agent tokens to silence the alert: a token that
  never ends outlives the agent it was made for.

**Escalate / collect.** The token names and `expires` values (never the
hashes), the server time, and the reload log lines.

## LeepConfigSkew

Replaces `LeepConfigGenerationSkew`, which compared `leep_config_generation`. That
counter is per process (1 at startup, +1 per reload even with no change), so the
old alert fired after any single restart although every server ran the same
config. If your rules still use the old name, replace the rule with
`LeepConfigSkew` from [OPERATIONS.md](OPERATIONS.md#suggested-alerts).

**Meaning.** For 15 minutes the servers have exported more than one
`leep_config_info` fingerprint set to 1, so they are not all running the same
config. **Ticket.** The fingerprint is a hash of the parsed config (tokens and
their limits, trusted signing keys, the review policy, the
`--require-signatures` and review ref sets, and the server-wide limit defaults).
It is the same on servers with the same content, whatever their uptime, file
formatting or entry order, and it changes when any of that content changes. A
restart, or a reload that finds nothing new, does not move it.

**Impact.** One or more servers are enforcing different tokens, keys, limits or
protected refs than the rest: a revoked token may still work on one, and the
same land can be accepted by one server and refused by another.

**Diagnose.**

1. Which servers disagree: `count by (fingerprint) (leep_config_info == 1)` and,
   per server, `leep_config_info == 1` (the `fingerprint` label; add `by
   (instance)`). The fingerprints are also in each server's log: `config loaded`
   at startup and `config reloaded ... fingerprint=... previous_fingerprint=...`
   after each SIGHUP. The odd one out is usually the lone server or the minority.
2. Per instance, did it reload? `leep_config_last_reload_timestamp_seconds`, and
   the log: `SIGHUP: reloading config` then `config reloaded`, or an ERROR (then
   it is also [LeepConfigReloadFailed](#leepconfigreloadfailed)).
3. Do the files differ? `sha256sum` of the tokens, keys and policy files on every
   host or mount (byte-identical files give equal fingerprints; files that only
   differ in whitespace or order do too, so a checksum difference alone is not
   proof). A flag difference counts as well: `--require-signatures`,
   `--require-review` and the limit defaults (`--rate-limit`, `--rate-burst`,
   `--max-concurrent-per-token`, `--read-max-body-bytes`) are part of the
   fingerprint and only change on restart.

**Likely causes.**

1. A reload was not delivered to every instance (a missed host, a container that
   was not signalled), so some still run the previous files. Confirm: no `SIGHUP:
   reloading config` line, and an older `leep_config_last_reload_timestamp_seconds`,
   on the odd server.
2. A reload failed on one instance only. Confirm: ERROR line there.
3. The files differ between hosts (configuration management lag, a bind mount
   that still points at the old inode, see
   [DEPLOY.md](DEPLOY.md#reloading-config)). Confirm: step 3.
4. The flags differ between hosts (a rollout that changed `--require-*` or a limit
   default is only half done). Confirm: step 3, and the flags each server runs
   with.
5. A rollout in progress. Confirm: the skew clears when the last server is
   signalled or restarted; the alert waits 15 minutes.

**Mitigate / fix.**

- If an instance missed the reload, send it SIGHUP, or restart it.
- If the files differ, make them identical (configuration management), validate
  them, and reload everywhere. If a flag differs, finish the rollout by
  restarting the stragglers.
- Confirm by looking for a single series at 1 again:
  `count(count by (fingerprint) (leep_config_info == 1))` is 1.
- The old fingerprint stays exported at 0 until the process restarts; that is
  expected, which is why the rule selects `== 1`.
- Do not "fix" it by restarting servers to reset a counter: restarts do not
  change the fingerprint, only the content does.

**Escalate / collect.** The `leep_config_info` series from every instance, the
`config loaded` and `config reloaded` log lines, the checksums of the config
files on each host, the flags each server runs with, and the
[standard bundle](#collecting-a-bug-report).

## LeepSchemaVersionSkew

**Meaning.** Running servers report different `leep_schema_version` (the newest
migration applied when they opened the store) for an hour. **Ticket.** Normal in the
middle of a rolling upgrade; a problem when the rollout is stuck.

**Impact.** Usually none, if the release notes say the new migrations tolerate old
servers. A server never writes to a schema it does not understand at startup, but
one that is already running is not checked again: an old server keeps running on a
database a newer one migrated. Servers from before ADR 0023 do not read the
version at all.

**Diagnose.**

1. `leep_schema_version` and `leep_build_info` per instance.
2. `leep-server migrate --status` with the new binary (read-only): prints `schema
   version: N (this server: M)`, the `applied  v0001 name (...)` history and what is
   pending.
3. Rollout state: which instances run the old image, and why they were not
   replaced (crash loop, drained, an autoscaler template with the old tag).
4. Refusal messages in an old instance's log on restart: `database schema vN is
   newer than this server (max vM); upgrade the server`.

**Likely causes.**

1. A rolling upgrade in progress. Confirm: instances are being replaced; the skew
   shrinks.
2. A stuck rollout: an instance not replaced or an autoscaler template still
   pointing at the old image. Confirm: step 3.
3. A rollback of some instances onto a migrated database, which the old binary
   refuses at startup (step 4), leaving fewer servers than intended.
4. A server that was restarted on a build whose migration list differs, refused
   with `migration vN (name) differs from the one applied to this database ...;
   applied migrations are immutable`. Confirm: that message.

**Mitigate / fix.**

- Finish the rollout: restart the old instances on the new image. Check the release
  notes for whether old and new may run together while you wait.
- There is no downgrade. Going back means restoring the backup taken before the
  upgrade into new stores, which loses everything since (see
  [Upgrading](OPERATIONS.md#upgrading) and [Restoring](OPERATIONS.md#restoring)).
- Never edit `schema_migrations` or an applied migration, and never stop servers
  "to avoid skew" by pointing an old build at the migrated database.

**Escalate / collect.** The per-instance gauge values, `migrate --status` output,
image digests and the log lines around each instance's start.

## LeepMixedVersions

**Meaning.** Servers report more than one `version` in `leep_build_info` for 2
hours. **Ticket.** Expected during a rollout; a problem when it lasts. `version` is
the server crate's version, which only the maintainer bumps and stays below 0.1.0:
**two different builds can report the same version**, so equal values do not prove
equal builds. Compare image digests too.

**Impact.** Mixed behaviour across a fleet: a fix present on some servers only,
different limits or messages per request depending on the balancer's choice. The
schema may also differ ([LeepSchemaVersionSkew](#leepschemaversionskew)).

**Diagnose.** `leep_build_info` per instance; `leep-server --version` and the image
digest on each host; the rollout tool's status; schema versions per instance.

**Likely causes.** A rollout in progress; a stuck rollout; an instance that cannot
start on the new image (check its log, see [LeepServerDown](#leepserverdown)); an
autoscaler or restart policy that pulls an old tag.

**Mitigate / fix.** Finish the rollout (or roll back the binary only if the schema
did not change, see [LeepSchemaVersionSkew](#leepschemaversionskew)). Pin image
digests instead of tags.

**Escalate / collect.** Per-instance version and digest, schema version, and the
rollout log.

## LeepMirrorDown

**Meaning.** The `leep-git mirror run` process's `--listen` endpoint has not
been scrapable for 5 minutes. **Page.**

**Impact.** Lands stop reaching the git remote, so the team's CI and the
people working in git stop seeing agent changes. Pushes to git stop landing
in Leep. Nothing is lost: Leep keeps landing, and the remote keeps what it
has. A restarted mirror catches up with one push per ref.

**Diagnose.**

1. Is the process running? Check `systemctl status leep-mirror` or the
   container. Its last log lines on stderr give the reason for an exit:
   - `another mirror holds .../lock`: a second instance on the same work
     directory;
   - `reading token file`: the token file is missing;
   - `--allow-rewind main: main and tags are never rewound`: a refused flag.
2. `leep-git mirror status --work-dir DIR` reads the last status the mirror
   wrote, even when it is down. It exits 1 with `status is Ns old; is the
   mirror (pid P) running?` when the file is stale.
3. If the process runs but cannot be scraped, check `--listen`: the address
   and port, a firewall, and whether the scrape job's target matches.
4. A long first export or a huge push can block the loop for minutes (up
   to `--git-timeout`). The process is alive and the log shows the push
   when it ends. Scrapes still answer during it; only the status file goes
   stale.

**Likely causes.**

1. The process exited: a deploy, a crash, or a configuration error at
   startup. Confirm: step 1.
2. The host or container was rescheduled without its work directory, and
   the new one is still exporting history. Confirm: a fresh `started` in
   `GET /status` and a growing `commits_exported`.
3. A scrape configuration problem. Confirm: step 3.

**Mitigate / fix.**

- Restart it with the same work directory. Restarting is always safe; the
  mirror decides from current values and repeats nothing (ADR 0046).
- Never run two mirrors on one work directory. The lock refuses it. Two
  mirrors with different work directories pushing the same remote do not
  corrupt anything, since pushes are fast-forward only, but they double the
  traffic.
- If the work directory is lost, start with an empty one. It rebuilds by
  exporting again, to the same commits.

**Escalate / collect.** The mirror's stderr log, `leep-git mirror status
--json`, its flags (without the token), and `git --version`.

## LeepMirrorLagging

**Meaning.** A mirrored ref has differed between Leep and the remote for
over 10 minutes while the mirror is still trying (state `pending` or
`error`). **Page.**

**Impact.** As [LeepMirrorDown](#leepmirrordown), for this ref. Lands
pile up and reach git in one push once the cause is fixed.

**Diagnose.**

1. `leep-git mirror status --work-dir DIR` shows the ref's `detail`. The
   log (stderr) has the same text, with credentials redacted.
2. `leep_mirror_pushes_total{result=...}` and `leep_mirror_errors_total{stage=...}`
   say which side fails:
   - `stage="git"` or `result="failed"`: the remote or its credentials. In
     the detail, `git push ... failed:` or `git ls-remote ... failed:` is
     followed by git's own message:
     - `Authentication failed` or `Permission denied (publickey)`: an
       expired token or key;
     - `Could not resolve host`: DNS or the network;
     - `timed out after`: a hung transfer.
   - `result="rejected"` repeating: the remote refuses the update. The
     detail quotes the reason: `pre-receive hook declined`, `protected
     branch hook declined`, or `non-fast-forward` that does not clear.
   - `stage="leep"`: the server. In the log, `reading refs from the server
     failed:` is followed by the CLI's error: `cannot reach`, 401 or 403
     (token), 429 (limits).
   - `stage="export"` or `"import"`: reading a changeset or blob, or
     creating one, failed. The detail names the changeset or commit.
3. Try git by hand as the mirror's user:
   `git --git-dir DIR/git ls-remote <remote> refs/heads/main`.
4. Try the server: `curl -H "Authorization: Bearer $LEEP_TOKEN" $SERVER/v1/refs/main`.

**Likely causes.**

1. The remote's credential expired or lost write access. Confirm: step 3.
2. Branch protection or a hook on the remote rejects the mirror's pushes.
   Confirm: `result="rejected"` and the detail.
3. The remote or the network is down or slow. Confirm: step 3, the
   provider's status page, and `timed out` in the detail.
4. The Leep server is down, throttling the mirror's token, or refusing its
   token. Confirm: step 4, [LeepTokenThrottled](#leeptokenthrottled).
5. A changeset cannot be exported (a missing blob). Confirm: a stable
   `export of <id>` detail. Run `leep-server verify` and see
   [OPERATIONS.md](OPERATIONS.md#verify-and-repair).

**Mitigate / fix.**

- Renew the credential, or let the mirror's account bypass branch
  protection on the mirrored branches. It needs push, but never force-push.
- The mirror retries on its own, with backoff up to `--max-backoff`. A new
  land or push retries at once. `POST /poke` forces a remote poll.
- Do not push Leep's commits to the remote by hand, and do not force-push
  the remote to "help". The mirror's next push is already the complete
  backlog.

**Escalate / collect.** The status JSON, the log since the lag began,
`git --version`, and the output of steps 3 and 4.

## LeepMirrorStuck

**Meaning.** For 15 minutes a mirrored ref has been in a state only a
person can clear: `diverged`, `rewound`, `remote-ahead`, `refused` or
`deleted-in-leep`. **Ticket.**

**Impact.** That ref is not mirrored in either direction. The other refs
are unaffected. Nothing is lost: each side keeps its own commits, and the
mirror resolves no conflict and forces nothing (ADR 0046).

**Diagnose.** `leep-git mirror status --work-dir DIR` gives the state and a
detail that says what to do; [OPERATIONS.md](OPERATIONS.md#mirror-states)
has the table.

**Likely causes and fixes.**

1. **`diverged`**: someone pushed to git while agents landed in Leep, so
   both sides moved. `leep_mirror_divergences_total` counts how often. The
   fix happens in git:
   ```sh
   git fetch origin
   git checkout main && git merge --ff-only origin/main
   git merge origin/leep-diverged/main     # resolve any conflict here
   git push origin main
   ```
   The merge contains both sides, so the mirror lands it in Leep as a
   fast-forward and deletes `leep-diverged/main`. If agents land again
   before the push, it diverges again with a newer `leep-diverged/main`:
   repeat. If it keeps happening, people should push their work through
   Leep (`leep land`) or to a land branch (`git push origin
   HEAD:leep-land/main/NAME`, with `--land-branches`) rather than straight
   to busy branches. If the changes rarely touch the same files, restart
   the mirror with `--on-divergence merge`: it merges clean divergences
   itself and reports only conflicts (ADR 0049). With that mode on, the
   detail says why this one was not merged: the conflicting paths, a land
   policy refusal of the merge, or a remote tip Leep moved back from.
2. **`rewound`**: someone ran `leep undo` (or a raw ref CAS) on a mirrored
   ref, so Leep is behind the remote. Either land a revert in Leep, which
   the mirror pushes as a fast-forward, or undo the undo (`leep undo`
   again). For a scratch branch where rewinding is normal, restart the
   mirror with `--allow-rewind NAME`.
3. **`refused`**: a push from git did not pass Leep's land policy. The
   detail quotes the server, for example `changeset X is not signed by a
   trusted key` or `needs review`. Approve the changesets with `leep review`
   (the mirror retries every minute), or revert the push in git. Never
   relax the policy to let a mirror through.
4. **`remote-ahead`**: `--import` is off and someone pushed to the remote.
   Enable `--import`, or remove the commits from the remote by an ordinary
   revert pushed through Leep. For a ref under `--ref-prefix` (such as
   `review/x`), someone pushed to its branch, which the mirror never
   imports or overwrites. Land that work in Leep if it is wanted, then
   delete the branch (`git push origin :refs/heads/review/x`); the mirror
   pushes Leep's ref again, or forgets it if Leep deleted it.
   A prefix branch also stays in `remote-ahead` when its Leep ref was
   deleted and the mirror, after a restart, can no longer prove the ref
   held the branch's tip: the changesets were collected by GC after the
   delete and the mirror's map does not list the commit (a rebuilt work
   directory), or the ref's log entries are gone. It stays until someone
   deletes it by hand: check that it holds nothing wanted first. A branch whose name is no Leep ref
   name (such as `review/-x`) is reported the same way and needs the same
   fix.
5. **`deleted-in-leep`**: the Leep ref was deleted. Delete the remote branch
   if that was intended (`git push origin :refs/heads/NAME`), or recreate
   the Leep ref. Remove the `--ref` if the branch is no longer mirrored.

**What not to do.** Do not force-push the remote's branch to match Leep, and
do not move the Leep ref to match the remote with a raw CAS. Both discard
someone's commits. Do not delete the mirror's work directory to "reset" it:
the state comes from the two refs, not from the directory.

**Escalate / collect.** The status JSON, `git log --oneline --graph -20
origin/main origin/leep-diverged/main`, and `leep log main -n 20`.

## LeepMirrorLandBranchFailing

**Meaning.** For an hour at least one land branch
(`leep-land/<ref>/<name>`, with `--land-branches`) has stayed on the remote
because it conflicts with its Leep ref or was refused. **Ticket.**

**Impact.** The work on that branch has not landed. Nothing else is
affected: the mirrored ref stays in sync, other land branches keep landing,
and nothing is lost (ADR 0049).

**Diagnose.** `leep-git mirror status --work-dir DIR` lists each land
branch with its state and a detail; `--json` has them under
`land_branches`. `leep_mirror_land_branches_failing{ref}` says which ref.

**Likely causes and fixes.**

1. **`conflict`**: the branch and the ref changed the same paths (the
   detail lists them). Tell the person who pushed it. They rebase and push
   again:
   ```sh
   git fetch origin
   git rebase origin/main            # resolve the listed paths
   git push --force-with-lease origin HEAD:leep-land/main/NAME
   ```
   The mirror tries a new value at once.
2. **`refused`**, by the land policy (`not signed`, `needs review`): get
   the review or signature in Leep; the mirror retries every minute. Never
   relax the policy for it.
3. **`refused`**, `is a merge`: land replays only linear stacks. Rebase the
   branch instead of merging main into it, and push again.
4. **`refused`**, `names no mirrored ref`, or a 404 for the ref: the name
   is wrong, or the ref is not mirrored. Push to
   `leep-land/<mirrored ref>/<name>` and delete the old branch.
5. A branch nobody will fix: delete it (`git push origin
   :refs/heads/leep-land/main/NAME`). The mirror forgets it.

**What not to do.** Do not resolve the conflict by landing the branch with
a raw ref CAS, and do not push it straight to the mirrored branch to get
around the conflict: that only moves the conflict to a divergence.

## LeepBackupStale

**Meaning.** `leep_backup_last_success_timestamp_seconds` is older than 2 days
(use twice your backup interval), or does not exist at all. **Ticket**; treat it as
a page when the stores have had trouble lately or the gap passes three intervals.
The metric is written by `leep-server backup --metrics-file` and served by
node_exporter's textfile collector. It moves only when a backup is sealed *and*
passes its check; a run that exits 2 (nothing sealed) writes nothing, so staleness is
how that failure shows. `absent()` also catches a backup that never succeeded, or a
textfile collector that is not reading the file.

**Impact.** None for users. Your recovery point (RPO) is the last good snapshot, and
grows by the hour; a disaster now loses everything since.

**Diagnose.**

1. Is the metrics path working? On the backup host: `ls -l
   /var/lib/node_exporter/textfile/leep_backup.prom` (the file name must end in
   `.prom`, and node_exporter must run with `--collector.textfile.directory`
   pointing there; `node_textfile_scrape_error` and `node_textfile_mtime_seconds`
   say whether it is read).
2. Is the job running, and what did it say? Cron logs, and the job's own output.
   Exit status 0 sealed and clean, 1 sealed but the check found problems
   ([LeepBackupCheckFailed](#leepbackupcheckfailed)), 2 could not run. Run it by hand:
   `leep-server backup --out /backups/leep --metrics-file /var/lib/node_exporter/textfile/leep_backup.prom`.
3. Messages of a run that could not run (`error:` on stderr):
   - `metadata database PATH does not exist (backup never creates one)` or `blob
     directory DIR does not exist (backup never creates one)`: wrong `LEEP_META` or
     `LEEP_BLOBS` in the cron environment;
   - `N schema migration(s) pending ... run leep-server migrate` while
     `opening the source metadata store`: the backup binary is newer than the
     database. Run `leep-server migrate` (or start the new servers) after a backup
     taken with the old binary;
   - `database schema vN is newer than this server`: the backup binary is older than
     the database;
   - `a mem:// store lives in one process, so there is nothing to back up`;
   - `cannot snapshot a metadata store at ...: only sqlite: and postgres:// are
     supported`;
   - errors from `starting the snapshot read`, `copying rows`, `creating the
     snapshot file` (database, permissions, scratch space: `TMPDIR` needs room for
     the metadata), or `writing PATH` (the metrics file);
   - blob copy errors (S3 credentials, permissions on `--out`, a full disk).
4. Free space and permissions where `--out` lives: `df -h /backups`.
5. The newest sealed snapshot: `ls /backups/leep/snapshots | tail -3` and `jq
   '{name, created_at, snapshot_taken_at, check, elapsed_secs}'
   /backups/leep/snapshots/NAME/manifest.json`. A snapshot directory without a
   `manifest.json` is an interrupted run; ignore or delete it.

**Likely causes.**

1. The cron job stopped or is failing before it seals: wrong environment, host
   rebuilt, crontab lost, `flock` held by a hung run. Confirm: steps 2 and 3.
2. The source is not backup-able right now: pending migrations after an upgrade,
   a database newer than the binary. Confirm: step 3 messages.
3. The destination is full or unwritable. Confirm: step 4.
4. Backups run but fail their check (`leep_backup_last_run_ok == 0`): the
   last-success time is carried over, so staleness follows. Confirm:
   [LeepBackupCheckFailed](#leepbackupcheckfailed).
5. The textfile pipeline is broken (collector directory, file ownership, the file
   not named `.prom`). Confirm: step 1, while snapshots under `snapshots/` are new.

**Mitigate / fix.** Fix the cause and run the backup by hand; confirm
`result: sealed and checked` and that the metric moved. Then check the pool with
`leep-server backup-verify --from /backups/leep`. Do not "fix" the alert by
touching the metrics file, and do not delete old snapshots to make room before the
next backup has passed.

**Escalate / collect.** The run's stderr and (with `--json`) its report, the newest
`manifest.json`, `ls -l` of `snapshots/`, free space, and the cron configuration.

## LeepBackupCheckFailed

**Meaning.** `leep_backup_last_run_ok == 0`: the newest backup was sealed but its
closure check found problems, or the run used `--no-check` (the manifest's check
`mode` is `skipped`, the run exits 0, and the metric is still 0). **Ticket**, same
day. That snapshot is not proven restorable and `restore` refuses it: `this
snapshot's closure check failed (N problem(s): ...); a restore from it would not
verify. Restore an earlier snapshot, or pass --accept-failed-check` (or `was
skipped`).

**Impact.** None for users. Your recovery point is the last snapshot that passed,
which may be older than you think; a failed check usually means the source stores
themselves are damaged.

**Diagnose.**

1. What failed: the run's output (`PROBLEMS: N` and the listed lines) or `jq
   .check /backups/leep/snapshots/NAME/manifest.json` (`mode`, `ok`, `problems`,
   `first_problems`), and `jq .blobs.corrupt` for source blobs that do not hash to
   their names (`WARNING: source blob HASH does not hash to its name; not copied`).
2. Is the source damaged? `leep-server verify --full --rehash` against the live
   stores (read-only, safe beside servers). If it finds `missing_blob`,
   `corrupt_blob`, `missing_tree` and so on, the source is the problem: use
   [Verify and repair](OPERATIONS.md#runbook-verify-found-x).
3. Was it transient? Run the backup again. A source or destination object store
   whose listing lagged (S3) can fail one run and pass the next.
4. Was the check skipped on purpose? `--no-check` in the job's command line.
5. `--no-blobs`: the check ran against the source blobs; the platform's own copy is
   still unproven until `backup-verify --blobs-from URL` passes.

**Likely causes.**

1. Damaged source: a blob a changeset needs is missing or corrupt (a bad restore, a
   deleted file, disk trouble). Confirm: step 2.
2. A GC mistake: `gc --unsafe-no-fence` run beside live servers, or a fence shorter
   than the request timeout. Confirm: GC command lines in cron and shell history;
   `verify` shows `missing_blob`.
3. A transient listing or copy problem. Confirm: step 3 passes.
4. `--no-check`. Confirm: step 4.

**Mitigate / fix.**

- Get a good snapshot first: fix the cause (repair or restore the damaged blobs
  from an earlier backup or a client's cache, see the verify runbook), then run the
  backup again and confirm `result: sealed and checked`.
- Keep the failed snapshot for diagnosis, but pause retention deletions until a
  newer good one exists.
- Do not use `--accept-failed-check` unless you are in a disaster and nothing better
  exists; then expect `verify` to report the same problems after the restore.
- Do not remove `--rehash` or the check from the job to make the metric green.

**Escalate / collect.** The failing run's report (`--json`), `manifest.json`,
`verify --full --rehash --json` of the source, GC command lines, and recent
storage events.

## LeepBackupSlow

**Meaning.** The average backup duration over the last 7 days is more than 1.5 times
the week before, and above 10 minutes (`leep_backup_duration_seconds`). **Ticket.**
The store is outgrowing the window.

**Impact.** None yet. A backup that approaches the interval overlaps the next run,
delays the recovery point and holds a metadata read transaction open longer (on
Postgres it holds back vacuum; on SQLite it delays WAL checkpoints).

**Diagnose.**

1. What the last runs did: `jq '{name, elapsed_secs, counts, blobs: {copied:
   .blobs.copied, bytes_copied: .blobs.bytes_copied, listed: .blobs.listed}, check}'`
   over the newest manifests. Duration splits into the metadata stream (grows with
   changesets and derived rows), the blob copy (what is new) and the check (with
   `--rehash`, every referenced blob).
2. Does the job use `--rehash`? That makes every run read the whole repository.
3. Did you rotate the pool recently? A new pool copies every blob once.
4. Backup storage and network: throughput to `--out` or the `--blobs-to` bucket.
5. Overlapping runs: two at once are safe but compete.

**Likely causes.**

1. Growth in content or history, as expected. Confirm: `counts.changesets` and
   copied bytes grow.
2. `--rehash` on the daily job. Confirm: step 2.
3. A new pool after a rotation (one run, then back to normal). Confirm: step 3.
4. Slow or throttled backup storage. Confirm: step 4.
5. A very large metadata store streaming in one transaction. Confirm: the metadata
   part dominates.

**Mitigate / fix.**

- Keep `--rehash` for the weekly `backup-verify --rehash`, as in the schedule in
  [Backup and restore](OPERATIONS.md#schedule-retention-rpo-and-rto).
- Raise `--concurrency` if the network allows; memory is about `--concurrency`
  times your largest blob.
- Stop runs overlapping with `flock`.
- For metadata too large to stream, use Postgres base backups with WAL archiving
  and `backup --no-blobs`; for blobs that the platform already protects, `--no-blobs`
  and `backup-verify --blobs-from URL`.
- Do not skip the check to save time.

**Escalate / collect.** Durations and counts from the last month of manifests, the
job's command line, backup storage throughput.

## LeepBackupVerifyFailed

**Meaning.** The scheduled `leep-server backup-verify` (weekly, through the
[wrapper](#cron-wrappers)) exited non-zero (1: problems found, 2: could not run), or
has not passed in 9 days, or its metric does not exist. **Ticket**, same day. It
is the bit-rot check of the backup medium: damage shows here, not during the
disaster.

**Impact.** None for users. Some or all snapshots may not restore.

**Diagnose.**

1. Run it: `leep-server backup-verify --from /backups/leep --rehash --json`. Exit 1
   lists problems (`PROBLEMS: N`), exit 2 prints an `error:`; for example `FILE does
   not match its manifest (BLAKE3 ..., N bytes; expected ..., N bytes): the file was
   damaged or replaced after the backup`.
2. Which snapshots are affected: repeat with `--snapshot NAME` for the last few. A
   bad `meta.db` damages one snapshot; a missing or corrupt pool blob damages every
   snapshot that needs it.
3. Is the live store fine? `leep-server verify` on the source: if the blob is intact
   there, it can be copied again.
4. Is the medium failing? Kernel and disk logs (`dmesg`, SMART), filesystem errors,
   a full or remounted volume, an S3 lifecycle rule deleting objects.

**Likely causes.**

1. Bit rot or a failing disk under the pool. Confirm: step 4, a few blobs or files
   affected.
2. Someone deleted or modified files under the backup directory (cleanup scripts,
   retention that removed `blobs/`). Confirm: file times, shell history.
3. The backup directory was moved or the mount is missing: `error:` mentions `is not
   a backup directory`. Confirm: `ls`.
4. A `--no-blobs` platform copy that has not caught up. Confirm: `backup-verify
   --blobs-from URL` problems list recent blobs.

**Mitigate / fix.**

- For a damaged pool blob that still exists in the source: delete that file from the
  pool (`blobs/ab/cdef...`) and run `leep-server backup`, which copies every blob the
  pool lacks, then run `backup-verify --rehash` again. If the source also lost it,
  see the `missing_blob` row in [the verify runbook](OPERATIONS.md#runbook-verify-found-x).
- For a damaged `meta.db`: rely on another snapshot, and take a fresh backup.
- If the medium is failing, start a new pool on healthy storage (`--out` a new
  directory) and keep the old one until the new one verifies.
- Do not delete old snapshots until a newer one verifies clean.

**Escalate / collect.** The `backup-verify --json` output, the list of affected
snapshots, medium health logs, and the retention and cleanup jobs on the host.

## LeepRestoreDrillOverdue

**Meaning.** The restore drill has not passed in 35 days (or never), according to
the metric your drill job writes through the [wrapper](#cron-wrappers). **Ticket.** A
backup nobody has restored is a hope: the drill is the only evidence of your
recovery time (RTO) and that the backup restores at all.

**Impact.** None today. Unknown recoverability.

**Diagnose.**

1. Did the drill run and fail, or not run? `leep_restore_drill_last_exit_code` and
   `leep_restore_drill_last_run_timestamp_seconds`.
2. If it failed, rerun the steps by hand, in scratch stores only, from
   [The restore drill](OPERATIONS.md#the-restore-drill): `restore --from
   /backups/leep --rehash`, `verify --full --rehash`, serve it, `leep refs` against the
   manifest's `refs`, a fresh checkout. Each step has an expected result.
3. Refusals from `restore` and what they mean are in the table in
   [Restoring](OPERATIONS.md#restoring): `destination metadata store is not empty`
   (the scratch stores were not fresh), `newer than this server (vN)` (use a newer
   binary), `does not match its manifest` (damaged `meta.db`; try another snapshot),
   `closure check failed` or `was skipped`.

**Likely causes.**

1. Nobody scheduled it, or the owner changed. Confirm: no runs.
2. Scratch space or time ran out (the drill needs room for the whole store and
   `$TMPDIR` for the metadata). Confirm: the failing step.
3. The backup is not restorable. Confirm: a restore or verify step fails.
   [LeepBackupCheckFailed](#leepbackupcheckfailed) and
   [LeepBackupVerifyFailed](#leepbackupverifyfailed) usually fired first.
4. A new Leep version changed a step. Confirm: the release notes; run the drill
   after every upgrade.

**Mitigate / fix.** Run the drill now and record the date and the time from restore
to verify (your measured RTO). If any step fails, treat the backup as not good: take
another, keep the failed one for diagnosis, and find out why before the next
scheduled run. Never run the drill against the real stores or with `--force`.

**Escalate / collect.** The failing step's output, the manifest, the restore and
verify `--json` reports, free space on the scratch host.

## LeepGcStale

**Meaning.** `leep-server gc` has not exited successfully in 3 days (daily cron),
according to the metric written by the [wrapper](#cron-wrappers); Leep exports no GC
metric of its own. **Ticket.** Abandoned uploads and failed commits leave
unreferenced blobs and only GC reclaims them.

**Impact.** None immediately; the blob store grows until
[LeepDiskSpaceLow](#leepdiskspacelow). GC never runs in the request path.

**Diagnose.**

1. `leep_gc_last_exit_code` and the cron log. Run what it would do without writing
   anything: `leep-server gc --dry-run`. The report lines are `changesets scanned`,
   `would condemn: N blobs, B bytes`, `kept (within grace)`, `kept (recently
   claimed)`.
2. Errors from the command:
   - `opening blob store` or `opening meta store` with the store's error (wrong
     environment in cron, credentials, database down);
   - `database schema vN is newer than this server (max vM); upgrade the server`, or
     pending migrations with `--no-migrate`;
   - `invalid input: a zero fence can lose blobs while servers run; pass a positive
     fence (longer than the servers' request timeout) or --unsafe-no-fence`;
   - `--fence Ns does not exceed --request-timeout ...`: since ADR 0059 phase 2
     `gc` refuses a fence the servers' timeouts do not cover. Run it with the
     servers' `--request-timeout` and `--meta-statement-timeout` (or their
     `LEEP_` variables), or raise `--fence`;
   - `another garbage collection is running (...)`: the changeset sweep or
     another `gc` holds the `gc` lease; a killed run's lease lapses in 60 s;
   - `gc --full frees the trees of deleted changesets ...`: `--full` needs
     `--backups DIR` or `--backup-fence`;
   - `bad duration "..."; use e.g. 0, 90s, 30m, 24h, 7d`.
3. Is it simply slow or killed? Condemn and collect walk only changesets stored since
   the last run (`changesets walked` in the report; ADR 0033), but still list every
   changeset and blob, so they take longer as the store grows, and the first run
   after an upgrade, or one with `--full`, walks all of history. A job killed by a
   timeout or an overlapping run never reports success. Killing GC is safe, and a
   killed run is finished by running it again; what it marked before dying is kept.
4. Nothing deleted for a day after a restore: restored blobs have the restore time
   as their modification time, so `--grace` (default 24h) starts again.

**Likely causes.**

1. The job is not scheduled, or its environment differs from the server's. Confirm:
   steps 1 and 2.
2. A refusal because of the schema (after an upgrade) or flags. Confirm: step 2.
3. The job is killed before it finishes (cron timeout, host reboots, an overlapping
   run). Confirm: step 3, the run's duration against its schedule.
4. The stores are unreachable from the job's host. Confirm: step 2.

**Mitigate / fix.**

- Fix the cause and run `leep-server gc` (or `gc condemn` now and `gc collect` later,
  as two jobs). Run one GC at a time (`flock`).
- Keep `--fence` above the servers' `--request-timeout` plus their
  `--meta-statement-timeout` plus clock skew (default 15m against 5m + 5m 5s; `gc`
  warns when it is not), and `--grace` above the time agents take from upload to
  commit.
- Never use `--unsafe-no-fence` beside running servers. Never delete blobs by hand to
  make room: use [LeepDiskSpaceLow](#leepdiskspacelow) for urgent space.

**Escalate / collect.** The cron output of the last runs, `gc --dry-run` output,
the job's flags and the servers' `--request-timeout`, and the store sizes.

## LeepUnreachableGcStale

**Meaning.** `leep-server gc --unreachable` (the changeset sweep, ADR 0059)
has not exited successfully in 3 days, by the [wrapper](#cron-wrappers)'s
`leep_gc_unreachable_last_success_timestamp_seconds`. **Ticket.**

**Impact.** Unreachable changesets accumulate as before the sweep existed.
If a condemn ran and no collect finished since, the fence flag stays up
and every server reads fence rows for what it names
([LeepGcFenceFlagStuck](#leepgcfenceflagstuck)).

**Diagnose.**

1. The cron log and `leep_gc_unreachable_last_exit_code`. Common errors:
   - `another garbage collection is running (job, holder, Ns left)`: blob
     GC or the other sweep phase overlaps; or a killed run's lease, which
     lapses in 60 s.
   - `--fence Ns does not exceed ...`: the fence check. Give `gc` the
     servers' `--request-timeout` and `--meta-statement-timeout` (or their
     `LEEP_` variables), or raise `--fence`.
   - `servers that do not rescue changesets hold leases: ...`: upgrade
     them before condemning.
2. Whether it is slow: the run's printed `collect: left for the next run
   (--max-delete)`, and `leep_gc_changesets_left_by_cap`.

**Mitigate / fix.** Fix the cause and run the job by hand. A killed run
needs nothing else: the next collect finishes what it left.

**Escalate / collect.** The cron log, the run's output, `gc --unreachable
--dry-run --grace 0`.

## LeepChangesetRescueBurst

**Meaning.** Servers rescue more than one condemned changeset a second
(`leep_gc_changesets_rescued_total{by}`): agents keep naming changesets the
sweep found unreachable and older than `--changeset-grace`. **Ticket.**

**Impact.** Correct but costly: each rescue writes rows and may wait up to
30 s for a delete in progress. A rescue that loses to a delete answers
"unknown parent" or 404, and the agent must post its work again.

**Diagnose.** The `by` label says who: `create` (building on old work),
`land` (landing old stacks), `ref` (`leep undo` or branches at old work).

**Mitigate / fix.** Raise `--changeset-grace` above how long agents keep
building on unlanded work, or fix the agents that resume stale stacks.

**Escalate / collect.** The rate by `by`, the sweep's `--changeset-grace`,
and examples of the changesets named.

## LeepChangesetDoomStuck

**Meaning.** Writers were answered 409 "changeset ... is being garbage
collected; retry" after waiting 30 s
(`leep_gc_changeset_doom_conflicts_total`). A changeset collect announced
a delete (`cs-doom-v1` *deleting*) and did not settle it: it was killed
between announcing a batch and finishing it, or it is very slow. **Ticket.**

**Impact.** Requests naming those changesets fail until a collect settles
them. Nothing is lost: a kill leaves the store closed under ancestry, and
the next collect either finishes the delete or abandons it.

**Diagnose.**

1. Is a collect running? `ps` on the GC host. If it is, see
   [LeepChangesetCollectSlow](#leepchangesetcollectslow).
2. Was one killed? The cron log.

**Mitigate / fix.** Run `leep-server gc --unreachable --collect-only` now,
once the killed run's lease has lapsed (60 s). Do not edit `cs-doom-v1`
rows by hand.

**Escalate / collect.** The cron log, the collect's output, and
`leep-server verify --full`.

## LeepGcFenceFlagStuck

**Meaning.** `leep_gc_fence_active` has been 1 for a day: a changeset
condemn raised `gc-fence-active-v1`, and no collect has resolved every
condemnation since. **Ticket.**

**Impact.** Every server reads fence rows for the changesets it names,
which costs a few reads per write, and every land batch attempt reads them
too. Nothing is unsafe.

**Diagnose.** When the collect job last succeeded
([LeepUnreachableGcStale](#leepunreachablegcstale)), and its
`collect: still condemned` and `collect: left for the next run` lines. A
store with more condemned changesets than `--max-delete` takes several
runs.

**Mitigate / fix.** Run the collect job. Raise `--max-delete` or run it
more often if it cannot keep up. The flag is cleared by the collect that
leaves nothing stored condemned.

**Escalate / collect.** The last collect's output and the metrics file.

## LeepChangesetCollectSlow

**Meaning.** A changeset collect batch took more than 5 s from announcing
its deletes to settling them
(`leep_gc_changeset_collect_batch_seconds_max`, from the run's metrics
file). **Ticket.**

**Impact.** Writers naming a changeset in the batch wait for it (up to
30 s, then 409); land batches waiting on it hold their ref's queue.

**Diagnose.** Metadata store latency and load during the run. Batches
delete rows of eight kinds plus the changesets in one transaction.

**Mitigate / fix.** Lower `--batch` (default 500) and `--rate`, or run the
collect in a quieter hour.

**Escalate / collect.** The run's output, the metrics file, and the
database's slow query log for the run.

## LeepDiskSpaceLow

**Meaning.** A filesystem that holds Leep data (blobs, the SQLite file, Postgres
data, the backup directory) has less than 10% free for 15 minutes. **Ticket**; a
page when it is near full. The rule uses node_exporter's `node_filesystem_avail_bytes`
and `node_filesystem_size_bytes` with `mountpoint` placeholders you must set; Leep
exports no disk metrics, and an S3 bucket needs the cloud provider's own alerts.

**Impact.** At zero free space writes fail: servers answer 5xx (`storage error:
...` with `No space left on device` or SQLite's `database or disk is full`),
Postgres cannot write, backups fail (see [LeepBackupStale](#leepbackupstale)).

**Diagnose.**

1. `df -h` and `df -i` (inodes: the filesystem blob store makes one file per blob).
2. What grows: `du -xsh /var/lib/leep/*`, `du -xsh /backups/leep/*`, and for
   Postgres `SELECT pg_size_pretty(pg_database_size('leep'))` plus the size of its WAL
   directory and replication slots (a stuck slot or broken archiving keeps WAL).
3. What GC could reclaim: `leep-server gc --dry-run` (`would condemn: N blobs, B
   bytes`). Only blobs older than `--grace` (24h default) and unreferenced qualify,
   and deletion follows the fence.
4. Leftover temp files in a filesystem blob store: `find /var/lib/leep/blobs
   -path '*/packs' -prune -o -name '.tmp-*' -mmin +60 -print` lists shard temp files
   older than the request timeout, which are safe to delete (replace `-print` with
   `-delete` once counted). Leave `packs/` and any `.repack-*` file alone: the
   next `leep-server repack` cleans those up, and a `.repack-*` file is a whole
   object it puts back.
5. Backups: the pool only grows; old snapshot directories are the thing to expire.
6. Metadata: derived rows dominate (the search index is about 30% of the indexed text
   for source code, up to about 100% for random text); the ref log is never pruned
   but small (tens of MB a year). `leep-server compact-changesets --dry-run` reports
   how much compressing the `changesets` table would save (about half, ADR 0053).
7. Is the store packed? A filesystem store with no `packs/*.pack` keeps one file
   per object, uncompressed; `leep-server repack` usually shrinks a long history
   ten to thirty times (OPERATIONS.md, "Packing blobs").

**Likely causes.**

1. Normal growth of content and history. Confirm: steady growth in `du` over weeks.
2. GC not running or not keeping up. Confirm: [LeepGcStale](#leepgcstale), and a large
   `would condemn`.
3. The backup volume filling with snapshots, or a pool that was never rotated.
   Confirm: step 5.
4. A burst: a large import, an agent swarm uploading big files, or abandoned uploads.
   Confirm: growth starts at an event.
5. Postgres WAL retained by a stuck slot or failing archive. Confirm: step 2.
6. Leftover `.tmp-*` after crashes. Confirm: step 4.
7. A filesystem blob store that is never repacked. Confirm: step 7.

**Mitigate / fix**, in this order:

1. If space is critical, free the safe things first: backup snapshot directories past
   your retention (delete whole directories under `snapshots/`; the pool is shared
   and stays), old logs, and `.tmp-*` older than the request timeout.
2. Run `leep-server gc`. Space returns after the grace window and the fence, not at
   once. On a filesystem blob store, then run `leep-server repack`: it needs room
   for the new pack (a fraction of the loose size) before it frees the loose copies,
   after its fence.
3. Grow the volume: that is the fix for steady growth. On SQLite, freed pages are
   reused inside the file but the file does not shrink by itself.
   If `changesets` was never compacted, `leep-server compact-changesets` (every
   server must be on a release that reads compressed rows; OPERATIONS.md,
   "Upgrading") halves it, then `VACUUM FULL changesets` returns the space (each
   needs free room of about half the table while it runs).
4. Last resort for metadata: `leep-server repair drop --kind blob-trigrams-v1
   tree-trigrams-v1` removes the search index rows; they rebuild on demand and the
   first searches are slow (`derive-search main` warms them). Take a backup first.
- Do not delete blob files, `-wal` files of SQLite or Postgres WAL by hand, and do not
  lower `--grace` to hurry GC while agents upload. Do not use `--unsafe-no-fence`.

**Escalate / collect.** `df` and `du` output over time, the GC dry-run report, the
size of the largest tables or WAL, and the [standard bundle](#collecting-a-bug-report).

## Cron wrappers

Leep exports no metric for `gc`, `backup-verify` or the restore drill, and a cron
job's exit status is invisible to Prometheus. This small wrapper runs any command
and writes node_exporter text-file metrics for it. [LeepBackupVerifyFailed](#leepbackupverifyfailed),
[LeepRestoreDrillOverdue](#leeprestoredrilloverdue) and [LeepGcStale](#leepgcstale) alert on them.
(The `leep-server backup` job writes its own metrics with `--metrics-file`.)

```sh
#!/usr/bin/env bash
# /usr/local/bin/leep-cron NAME COMMAND [ARG...]
# Runs COMMAND, then writes node_exporter text-file metrics for NAME:
#   leep_NAME_last_run_timestamp_seconds, leep_NAME_last_exit_code and
#   leep_NAME_last_success_timestamp_seconds (carried over while runs fail).
set -u
name=$1; shift
dir=${LEEP_TEXTFILE_DIR:-/var/lib/node_exporter/textfile}
out=$dir/leep_$name.prom
prev=$(grep "^leep_${name}_last_success_timestamp_seconds " "$out" 2>/dev/null || true)
"$@"
code=$?
now=$(date +%s)
tmp=$(mktemp "$dir/.leep_$name.XXXXXX")
{
  echo "leep_${name}_last_run_timestamp_seconds $now"
  echo "leep_${name}_last_exit_code $code"
  if [ "$code" -eq 0 ]; then
    echo "leep_${name}_last_success_timestamp_seconds $now"
  elif [ -n "$prev" ]; then
    echo "$prev"
  fi
} >"$tmp"
chmod 0644 "$tmp"
mv "$tmp" "$out"   # atomic, and not a *.prom name until it is complete
exit "$code"
```

```cron
17 3 * * *  flock -n /run/leep-gc.lock  leep-cron gc leep-server gc
20 5 * * 0  leep-cron backup_verify leep-server backup-verify --from /backups/leep --rehash
```

The names matter: the rules read `leep_gc_*`, `leep_backup_verify_*` and
`leep_restore_drill_*`. For the drill, put the steps of
[The restore drill](OPERATIONS.md#the-restore-drill) in a script that stops at the
first failing step (`set -e`) and run it monthly as `leep-cron restore_drill
/usr/local/bin/leep-restore-drill`. A job that exits non-zero keeps the old
last-success time, so the age-based alerts fire. These three rules also fire
when the metric does not exist at all; delete the `absent(...)` line of a rule for a
task you do not run.
