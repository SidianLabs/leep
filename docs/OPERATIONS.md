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
table is created when a server first opens an existing database, with no
manual migration, and starts empty: refs that moved before the upgrade have no
history. Upgrade every server before relying on it; an older server still
running moves refs without writing entries.

The log is never pruned, and it needs no maintenance: a row is about 150 bytes
and is written only when a ref moves, so even a swarm landing every second
adds a few tens of MB a year. Include `ref_log` in metadata backups, since it
is the only record of how refs moved. Entries name changesets, which are never
deleted, so `leep-server gc` is unaffected and a ref restored with `leep undo`
always points at an existing changeset. Postgres serializes the final insert of
each ref move with an advisory lock (key 7164738) so `seq` follows commit order;
it is held for the length of one insert.

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
