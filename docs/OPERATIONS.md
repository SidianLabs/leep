# Operating Leep

## Logs

`leep-server` logs with `tracing`. Choose the format with `--log-format
text|json` (env `LEEP_LOG_FORMAT`, default `text`); filter with `RUST_LOG`
(default `info`). Use `json` when a log pipeline ingests the output: one JSON
object per line.

Every HTTP request produces one INFO line, `request completed`, inside a
`request` span. The span carries `method`, `route` (the matched template such
as `/v1/blobs/{hash}`, or `unmatched`), `request_id`, `status` and
`latency_ms`:

```json
{"timestamp":"...","level":"INFO","fields":{"message":"request completed","status":404,"latency_ms":0.39},"target":"leep_server::observe","span":{"method":"GET","route":"/v1/blobs/{hash}","request_id":"5f0c9e2a41d34b7a9c1e0f6b2d8a7c35","status":404,"latency_ms":0.39,"name":"request"}}
```

`request_id` is the client's `X-Request-Id` when it is 1 to 128 characters
of `A-Z a-z 0-9 . _ : / -`, else 32 random hex digits the server picks. The
charset has no space, `=` or quote, so an id cannot pose as another field,
and text logs print it quoted (`request_id="..."`). The server
answers with it in `X-Request-Id`, and every JSON error body carries it as
`request_id`, so a user's error message leads to the log line. `leep` sends
one id per invocation and prints it as the last line of an error; `leep-mcp`
sends one per tool call. A land forwarded to the server that coordinates its
ref (ADR 0056) keeps the id, so both servers log it under the same one. A
load balancer that sets `X-Request-Id` makes its logs and the server's share
the id.

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
| `leep_land_total` | counter | `outcome` = `landed`, `conflicts`, `exhausted`, `error` | Land calls by result. `conflicts` is a normal reply that needs client action; `exhausted` means the CAS lost every retry (HTTP 409). `error` is every other failure, including refused lands (400: unsigned, unreviewed, bad ref) and unknown changesets (404), so it is not a server-health signal; count 5xx on `route="/v1/land"` for that. |
| `leep_land_attempts` | histogram | none | CAS attempts of the batch a land was committed in (ADR 0032); above 1 means another writer of the ref won a race. |
| `leep_land_cas_losses_total` | counter | none | Lost ref compare-and-swaps, one per batch attempt that lost (races with other writers of the ref: other server instances, imports, raw ref updates; lands in one process are batched and never race each other). |
| `leep_land_duration_seconds` | histogram | none | Whole land call, including time queued. |
| `leep_land_lock_wait_seconds` | histogram | none | Time a land waited in its ref's in-process queue before its batch started (the name dates from the per-ref lock that batching replaced). |
| `leep_land_batch_size` | histogram | none | Lands taken by each batch (group commit, ADR 0032), cancelled ones excluded. 1 means no contention; under load it approaches the number of lands queued behind one batch, at most 64. |
| `leep_land_cancelled_total` | counter | none | Lands whose caller went away (timeout, disconnect) while queued, dropped before their batch replayed them. |
| `leep_land_batch_deadline_total` | counter | none | Lands answered 409 because their batch was still retrying 60 s after its first attempt (`MAX_BATCH_LIFETIME`, ADR 0059) and gave up instead of moving the ref. Clients resend; keyed lands are then answered from their record. Anything but rare means a store so slow, or a ref so contended, that lands do not finish. |
| `leep_land_answered_from_record_total` | counter | `original` = `present`, `missing` | Keyed lands answered from their land id record instead of landing again (ADR 0057). `missing`: the changeset was no longer stored (garbage collected, ADR 0059) and the record answered without reading it. |
| `leep_gc_changesets_rescued_total` | counter | `by` = `create`, `land`, `ref`, `attest`, `mirror` | Condemned changesets a writer rescued because it was about to store something naming them (ADR 0059): a child, a land, a ref move, a signature or review, a git mirror map. 0 until a changeset sweep condemns; a steady rate means agents build on changesets the sweep found unreachable. |
| `leep_land_leases_held` | gauge | none | Refs whose land lease this server holds ([Several servers on one ref](#several-servers-on-one-ref), ADR 0056). |
| `leep_land_lease_acquired_total` | counter | none | Land leases this server took. Under steady traffic about one per hot ref per server restart; more means leases change hands. |
| `leep_land_lease_lost_total` | counter | `reason` = `taken`, `idle` | Leases this server stopped holding: another server took it after this one failed to renew it for the TTL (`taken`), or it released it after 30 s without a land (`idle`). |
| `leep_land_forwarded_total` | counter | `outcome` = `answered`, `not_leader`, `failed`, `lease_changed`; with `failed`, `reason` = `not_allowed`, `connect`, `timeout`, `reset`, `redirect` | Lands this server sent to the lease holder. `failed` (no answer) is retried to the same holder or its successor, never batched here while the lease is live; `not_allowed` means the lease names an address outside the allow-list. |
| `leep_land_lease_read_failed_total` | counter | none | Reads of a lease that failed; the land went on with the last lease read while that was still live. |
| `leep_land_forward_gave_up_total` | counter | none | Lands answered 409 after 30 s without reaching the server coordinating their ref. |
| `leep_land_received_forwarded_total` | counter | none | Lands forwarded here by an allowed peer. |
| `leep_land_rerouted_total` | counter | none | Lands handed back because this server lost the lease before replaying them; each is routed again. |
| `leep_land_routed_total` | counter | `route` = `uncoordinated` | Lands batched here with coordination on but no lease: no live lease was known and none could be taken (the store failing). They race the CAS as without coordination. |
| `leep_derive_total` | counter | `kind` | Derived values computed (cache hits in the meta store are not counted). |
| `leep_derive_duration_seconds` | histogram | `kind` | Time to compute one derived value. |
| `leep_warm_refs_total` | counter | `result` = `ok`, `error` | Ref heads the background warmer derived indexes for (ADR 0028). Errors are retried by the next full listing of refs, every 5 s. |
| `leep_warm_duration_seconds` | histogram | none | Time to warm the heads that moved since the last pass, together (ADR 0058). Seconds to hours after an import or a restart onto a large history; milliseconds once warm. |
| `leep_warm_delay_seconds` | histogram | none | Time from a ref move until the warmer stored that head's indexes, at **1-second resolution**: the ref log records whole seconds, so every sample is an integer and 0 means the same second. Recorded only for moves the ref-log tail found, not for refs a full listing turned up. How far the indexes trail the refs; 0 or 1 when the warmer keeps up, so read it as a count of slow warms rather than a latency below a second. |
| `leep_index_lease_held` | gauge | `kind` | 1 while this server holds the index lease of the group named by its primary kind (`history-tree-v4`, `log-filter-v1`, `tree-trigrams-v1`), `--indexing lease` only (ADR 0058). |
| `leep_index_lease_acquired_total` | counter | `kind` | Index leases this server took (a new epoch). |
| `leep_index_lease_lost_total` | counter | `kind`; `reason` = `taken`, `store` | Index leases lost: another server took it after renewals stopped (`taken`), or renewals failed until the view ran out (`store`). |
| `leep_index_derived_total` | counter | `kind` | Changesets (heads, for search) the lease holder's passes indexed. |
| `leep_index_backlog_changesets` | gauge | `kind`, `lane` = `tip` | Changesets the holder's last pass found missing. |
| `leep_index_lag_seconds` | gauge | `kind` | Age, by the ref log's whole seconds, of the oldest ref move a pass started on and had not indexed; 0 once a pass completes. |
| `leep_query_unindexed_scanned_total` | counter | `route` = `log`, `log_filter`, `last_change`, `blame`, `search` | Changesets (files, for search) a bounded query examined without index rows. |
| `leep_query_indexing_answers_total` | counter | `route`; `mode` = `retry`, `legacy` | Queries answered 503 because their index was over budget behind (`legacy`: requests without `Leep-Indexing`, after their wait and larger scan). Subtracted by `LeepServerErrors`. |
| `leep_query_stale_answers_total` | counter | `route` | `stale-ok` queries answered at an older changeset. |
| `leep_query_inline_derive_total` | counter | `route` | Queries in `lease` or `off` mode that found no live index lease and derived inline. Stays 0 while an indexer runs. |
| `leep_query_index_wait_seconds` | histogram | none | Time requests without the header waited for the indexer. |
| `leep_query_index_waiters` | gauge | none | Requests waiting for the indexer now (at most `--index-waiters`). |
| `leep_query_index_wait_refused_total` | counter | none | Requests that found every waiter's place taken and did not wait. |
| `leep_query_rows_vanished_total` | counter | `kind` | Bounded queries that met index rows deleted under them (a `repair drop` racing them) and answered 503. |
| `leep_create_last_merge_skipped_total` | counter | none | Changesets stored without their `last_merge-v1` row because the parent's was missing; `upgrade-index` backfills them. |
| `leep_search_total` | counter | `indexed` = `true`, `false` | Searches served; `false` means the query had no required trigram and scanned files directly. |
| `leep_search_duration_seconds` | histogram | none | Whole search call, including any index rows it derived. |
| `leep_blob_cache_hits_total` | counter | none | Blob reads served from the in-memory cache. |
| `leep_blob_cache_misses_total` | counter | none | Blob reads that went to the durable store. |
| `leep_blob_cache_bytes` | gauge | none | Bytes held in the cache. |
| `leep_blob_cache_entries` | gauge | none | Blobs held in the cache. |
| `leep_requests_limited_total` | counter | `reason` = `rate`, `concurrency`, `size`; `token` | Requests refused by a request limit (ADR 0020). `token` is the token's name, or `anonymous` for every client address when there is no tokens file. |
| `leep_requests_in_flight` | gauge | `token` | Requests in progress, until their response body has been sent. Only tokens that have a limit are tracked. |
| `leep_request_body_bytes_in_flight` | gauge | none | Body bytes the requests in flight hold of the `--max-inflight-bytes` budget: each request's `Content-Length`, or its route's cap when it declares none, from admission until its handler finishes. |
| `leep_request_body_budget_bytes` | gauge | none | The `--max-inflight-bytes` budget, for ratios. |
| `leep_request_body_refused_total` | counter | `reason` = `budget`, `principal`, `size`; `class` = `blob`, `json` | Requests refused by the server-wide body limits: `budget` is a 503 because the budget was spent, `principal` a 429 because the token already holds its share of it, `size` a 413 over `--max-blob-bytes` (`class="blob"`) or `--max-json-body-bytes` (`class="json"`). All six series exist from startup. |
| `leep_read_class_permits` | gauge | `class` = `search` | Permits of an expensive read class (`--read-concurrency`, ADR 0060): how many such requests this server runs at once. Absent for a class that is `off`. |
| `leep_read_class_in_flight` | gauge | `class` | Requests of the class holding a permit, until their response body has been sent or dropped. |
| `leep_read_class_shed_total` | counter | `class` | Requests of the class refused at once with `503` `unavailable`, `Retry-After` and `Leep-Refused: read-class` because every permit was in use. They also count in `leep_http_requests_total` as 503; `LeepServerErrors` subtracts them. The series exist from startup for each limited class. |
| `leep_tokens_expiring_soon` | gauge | none | Tokens in the tokens file whose `expires` is within 7 days and not yet past. Refreshed every minute and on every load. |
| `leep_tokens_expired` | gauge | none | Tokens still in the tokens file whose `expires` has passed; requests with them get 401. |
| `leep_token_expired_requests_total` | counter | `token` | Requests refused because their token had expired: an agent still using a token that ran out. |
| `leep_config_reload_total` | counter | `result` = `ok`, `error` | Config reloads (SIGHUP) by result; both series exist from startup (ADR 0024). |
| `leep_config_generation` | gauge | none | **Per process:** 1 at startup, plus one per successful reload, even when nothing changed. It resets on restart, so it differs between healthy servers that run identical config; do not alert on it. Use `leep_config_info` to compare servers. |
| `leep_config_info` | gauge | `fingerprint` | 1 for the fingerprint of the config in force, 0 for the ones this process ran before, so `leep_config_info == 1` has exactly one series per server. The fingerprint is 16 hex characters of a BLAKE3 over the parsed config (tokens with their authors and limits, trusted keys, review policy, `--require-signatures` and review refs, and the server-wide limit defaults), so it is the same on every server that runs the same config, across restarts, and does not change when a file is only reformatted or reordered. It holds no secret: tokens are already digests, keys are public, and the hash is truncated. It is also logged at startup and on every reload. |
| `leep_ready_checks_total` | counter | `store` = `meta`, `blobs`; `result` = `ok`, `error`, `timeout` | Checks made by `GET /v1/ready`, one per store per check. A check's answer is reused for 1 s, so calls within that second add nothing; all six series exist from startup. |
| `leep_config_last_reload_timestamp_seconds` | gauge | none | Unix time of the last successful load, including startup. |
| `leep_build_info` | gauge | `version` | Always 1; carries the server version. |
| `leep_schema_version` | gauge | none | Newest metadata schema migration applied (ADR 0023), read when the server opened the store. Servers sharing a database report the same value once all are upgraded. |

Seconds histograms use buckets from 1 ms to 10 s; `leep_land_attempts` uses
1 to 32 and `leep_land_batch_size` 1 to 64. The cache metrics are recorded by `CachedBlobStore` itself as it
serves requests (the gauges refresh on every read and write), so there is no
polling interval. With `--cache-bytes 0` there is no cache and the
`leep_blob_cache_*` series are absent.

### Suggested alerts

Every alert has a runbook in [RUNBOOKS.md](RUNBOOKS.md), linked from the rule's
`runbook_url`. `scripts/check-runbooks.sh` (run in CI) fails if a rule here has
no section there or a section has no rule. The rules assume the metrics above,
the git mirror's own metrics for the `leep-mirror` group (see
[Git mirror](#git-mirror)) and, for the last groups, the backup
`--metrics-file`, the cron wrapper and node_exporter described in the runbooks.

```yaml
# Every rule's runbook_url ends in RUNBOOKS.md#<alert name in lower case>.
# Replace the base URL below with wherever you publish docs/RUNBOOKS.md;
# scripts/check-runbooks.sh only checks the part from "RUNBOOKS.md#" on.
# severity: page = wake someone, ticket = fix in working hours.
#
# Leep's counters appear when they are first incremented, and increase() cannot
# see a counter's first event (there is no earlier sample), so the rules that
# watch rare events add a second branch: "present now, absent 10 minutes ago".
groups:
- name: leep
  rules:
  - alert: LeepServerDown
    # Needs the metrics listener scraped under a job named "leep"; adjust.
    expr: up{job="leep"} == 0
    for: 2m
    labels: {severity: page}
    annotations:
      summary: "A leep-server's /metrics cannot be scraped: the process is down, or only its metrics listener is."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepserverdown
  - alert: LeepServerErrors
    # Indexing answers (503 with Retry-After, ADR 0058) and shed reads (503
    # with Leep-Refused, ADR 0060) are not failures.
    expr: (sum(rate(leep_http_requests_total{status=~"5.."}[5m])) - (sum(rate(leep_query_indexing_answers_total[5m])) or vector(0)) - (sum(rate(leep_read_class_shed_total[5m])) or vector(0))) / sum(rate(leep_http_requests_total[5m])) > 0.01
    for: 5m
    labels: {severity: page}
    annotations:
      summary: "More than 1% of requests answer 5xx: a store is failing, or data is damaged."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepservererrors
  - alert: LeepLandErrors
    # Counts HTTP 5xx on the land route. leep_land_total{outcome="error"} also
    # counts refused lands (400, 404), so it is not used here.
    expr: |
      sum(increase(leep_http_requests_total{route="/v1/land",status=~"5.."}[10m])) > 0
      or
      sum(leep_http_requests_total{route="/v1/land",status=~"5.."} unless leep_http_requests_total{route="/v1/land",status=~"5.."} offset 10m) > 0
    labels: {severity: page}
    annotations:
      summary: "A land failed with a server error (5xx): a store failed while landing. Refused lands do not count."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeplanderrors
  - alert: LeepLandExhausted
    expr: |
      sum(increase(leep_land_total{outcome="exhausted"}[10m])) > 0
      or
      sum(leep_land_total{outcome="exhausted"} unless leep_land_total{outcome="exhausted"} offset 10m) > 0
    labels: {severity: ticket}
    annotations:
      summary: "A land lost the ref compare-and-swap on all 32 attempts; other writers are racing it for the same ref."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeplandexhausted
  - alert: LeepLandBatchDeadline
    expr: |
      sum(increase(leep_land_batch_deadline_total[10m])) > 0
      or
      sum(leep_land_batch_deadline_total unless leep_land_batch_deadline_total offset 10m) > 0
    labels: {severity: ticket}
    annotations:
      summary: "A land batch was still retrying 60 s after it started and answered its lands 409: the metadata store is very slow, or the ref is badly contended."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeplandbatchdeadline
  - alert: LeepLandCasLossesHigh
    expr: sum(rate(leep_land_cas_losses_total[15m])) / sum(rate(leep_land_total[15m])) > 1
    for: 15m
    labels: {severity: ticket}
    annotations:
      summary: "More than one lost ref compare-and-swap per land: several writers contend for one ref."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeplandcaslosseshigh
  - alert: LeepLandForwardFailing
    expr: sum(rate(leep_land_forwarded_total{outcome="failed"}[10m])) > 0.1
    for: 10m
    labels: {severity: ticket}
    annotations:
      summary: "Servers cannot reach the server coordinating a ref's lands (address not allowed, wrong --advertise-url, network); lands wait or end in 409."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeplandforwardfailing
  - alert: LeepLandLeaseFlapping
    expr: sum(rate(leep_land_lease_acquired_total[10m])) * 60 > 6
    for: 10m
    labels: {severity: ticket}
    annotations:
      summary: "Land leases change hands more than six times a minute: servers fail to renew them (slow or overloaded metadata store, restarts)."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeplandleaseflapping
  - alert: LeepIndexLagging
    expr: max by (kind) (leep_index_lag_seconds) > 120
    for: 10m
    labels: {severity: ticket}
    annotations:
      summary: "An index group trails ref moves by more than 2 minutes: its lease holder is slow, overloaded or backfilling (ADR 0058)."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepindexlagging
  - alert: LeepIndexerMissing
    # Only fleets with servers in --indexing lease or off mode export the
    # lease gauge; no holder means queries fell back to inline derivation.
    expr: |
      (max by (kind) (leep_index_lease_held) == 0)
      or
      (sum(rate(leep_query_inline_derive_total[5m])) > 0)
    for: 5m
    labels: {severity: page}
    annotations:
      summary: "An index group has no lease holder: queries derive inline again and the index stops keeping up."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepindexermissing
  - alert: LeepIndexingAnswersHigh
    expr: |
      sum(rate(leep_query_indexing_answers_total[10m]))
        / sum(rate(leep_http_requests_total{route=~"/v1/(log|last-change|blame|search)/.*"}[10m])) > 0.05
    for: 10m
    labels: {severity: ticket}
    annotations:
      summary: "More than 5% of indexed reads answer 503 because their index is too far behind."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepindexinganswershigh
  - alert: LeepLandSlowP99
    expr: histogram_quantile(0.99, sum by (le) (rate(leep_land_duration_seconds_bucket[5m]))) > 5
    for: 10m
    labels: {severity: ticket}
    annotations:
      summary: "Land p99 above 5 s (includes the time queued for the ref's next batch)."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeplandslowp99
  - alert: LeepRequestTimeouts
    expr: sum(increase(leep_http_requests_total{status="408"}[15m])) > 5
    labels: {severity: ticket}
    annotations:
      summary: "Requests are hitting --request-timeout (408). If you raise it, raise the GC --fence with it."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeprequesttimeouts
  - alert: LeepTokenThrottled
    expr: sum by (token, reason) (rate(leep_requests_limited_total[10m])) > 0.5
    for: 15m
    labels: {severity: ticket}
    annotations:
      summary: "A token is being limited steadily: a runaway agent, or a limit set too low."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeptokenthrottled
  - alert: LeepBroadThrottling
    expr: sum(rate(leep_http_requests_total{status="429"}[10m])) / sum(rate(leep_http_requests_total[10m])) > 0.05
    for: 15m
    labels: {severity: ticket}
    annotations:
      summary: "More than 5% of all requests are throttled (429): limits too low for the fleet, or one shared anonymous bucket."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepbroadthrottling
  - alert: LeepBodyBudgetExhausted
    expr: sum by (instance) (rate(leep_request_body_refused_total{reason="budget"}[10m])) > 0.05
    for: 10m
    labels: {severity: ticket}
    annotations:
      summary: "Requests are refused with 503 because the request bodies in flight fill --max-inflight-bytes: a burst of large uploads, slow uploaders holding budget, or a budget too small for the fleet."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepbodybudgetexhausted
  - alert: LeepReadsShedding
    # Only the search class is built; add a class's route here when it is.
    expr: |
      sum by (instance) (rate(leep_read_class_shed_total{class="search"}[15m]))
        / sum by (instance) (rate(leep_http_requests_total{route=~"/v1/search/.*"}[15m])) > 0.5
    for: 15m
    labels: {severity: ticket}
    annotations:
      summary: "A server refuses more than half of its searches (503 with Leep-Refused) because its search class is full: searches are wanted faster than the host can run them."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepreadsshedding
  - alert: LeepBlobCacheColdHitRatio
    expr: |
      sum(rate(leep_blob_cache_hits_total[15m]))
        / (sum(rate(leep_blob_cache_hits_total[15m])) + sum(rate(leep_blob_cache_misses_total[15m]))) < 0.5
    for: 30m
    labels: {severity: ticket}
    annotations:
      summary: "Cache hit ratio below 50%; consider a larger --cache-bytes."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepblobcachecoldhitratio
  - alert: LeepNotReady
    # GET /v1/ready checks run only when something probes the server; with no
    # prober (or a prober that stopped) no series moves and this stays quiet.
    expr: sum by (instance, store) (increase(leep_ready_checks_total{result!="ok"}[5m])) > 0
    for: 5m
    labels: {severity: page}
    annotations:
      summary: "GET /v1/ready is failing for a store: that server cannot reach its metadata or blob store and its orchestrator should be taking it out of rotation."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepnotready
  - alert: LeepConfigReloadFailed
    expr: increase(leep_config_reload_total{result="error"}[10m]) > 0
    labels: {severity: page}
    annotations:
      summary: "A SIGHUP reload was refused; the server runs the previous config. This alert clears after 10 minutes even if the file is still bad: read the ERROR log line, fix the file, signal again, and confirm."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepconfigreloadfailed
  - alert: LeepTokensExpiringSoon
    expr: max(leep_tokens_expiring_soon) > 0 or max(leep_tokens_expired) > 0
    for: 1h
    labels: {severity: ticket}
    annotations:
      summary: "A token in the tokens file expires within 7 days, or has expired and is still listed: rotate it before its agent starts getting 401."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeptokensexpiringsoon
  - alert: LeepConfigSkew
    # One series per server is 1; two different fingerprints means two different configs.
    expr: count(count by (fingerprint) (leep_config_info == 1)) > 1
    for: 15m
    labels: {severity: ticket}
    annotations:
      summary: "Servers run different config (different leep_config_info fingerprints): a reload missed one, or the files differ between hosts. A restart alone does not trigger this."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepconfigskew
  - alert: LeepSchemaVersionSkew
    expr: max(leep_schema_version) != min(leep_schema_version)
    for: 1h
    labels: {severity: ticket}
    annotations:
      summary: "Servers run against the same database but report different schema versions: a rollout is unfinished."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepschemaversionskew
  - alert: LeepMixedVersions
    expr: count(count by (version) (leep_build_info)) > 1
    for: 2h
    labels: {severity: ticket}
    annotations:
      summary: "Servers report different versions for over 2 hours: a rollout is unfinished (equal versions do not prove equal builds)."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepmixedversions
- name: leep-mirror
  # Metrics from `leep-git mirror run --listen`, scraped under a job named
  # "leep-mirror"; adjust. See "Git mirror" above.
  rules:
  - alert: LeepMirrorDown
    expr: up{job="leep-mirror"} == 0
    for: 5m
    labels: {severity: page}
    annotations:
      summary: "A leep-git mirror cannot be scraped: lands are not reaching git, and pushes to git are not landing."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepmirrordown
  - alert: LeepMirrorLagging
    # Only while the mirror is still trying (pending or error); states that
    # need a person are LeepMirrorStuck.
    expr: |
      max by (instance, ref) (leep_mirror_lag_seconds) > 600
      and on (instance, ref)
      max by (instance, ref) (leep_mirror_ref_state{state=~"pending|error"}) == 1
    for: 2m
    labels: {severity: page}
    annotations:
      summary: "A mirrored ref has differed between Leep and git for over 10 minutes while the mirror retries: the remote, its credentials or the server is failing."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepmirrorlagging
  - alert: LeepMirrorStuck
    expr: max by (instance, ref) (leep_mirror_ref_state{state=~"diverged|rewound|remote-ahead|refused|deleted-in-leep"}) == 1
    for: 15m
    labels: {severity: ticket}
    annotations:
      summary: "A mirrored ref needs a person: it diverged, was rewound in Leep, was refused by the land policy, or the remote is ahead."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepmirrorstuck
  - alert: LeepMirrorLandBranchFailing
    # Land branches (--land-branches) that conflict or were refused; the
    # person who pushed them fixes them, so this is a ticket, not a page.
    expr: max by (instance, ref) (leep_mirror_land_branches_failing) > 0
    for: 1h
    labels: {severity: ticket}
    annotations:
      summary: "A land branch on the git remote has conflicted with, or been refused by, its Leep ref for over an hour."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepmirrorlandbranchfailing
- name: leep-backup
  # Metrics written by `leep-server backup --metrics-file` (node_exporter textfile collector).
  rules:
  - alert: LeepBackupStale
    # Use twice your backup interval. absent() covers "never succeeded" and a
    # textfile collector that does not read the file.
    expr: |
      time() - leep_backup_last_success_timestamp_seconds > 2 * 86400
      or
      absent(leep_backup_last_success_timestamp_seconds)
    for: 30m
    labels: {severity: ticket}
    annotations:
      summary: "No backup has passed its check recently (or ever): your recovery point is getting older."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepbackupstale
  - alert: LeepBackupCheckFailed
    expr: leep_backup_last_run_ok == 0
    labels: {severity: ticket}
    annotations:
      summary: "The newest backup was sealed but failed its check (or ran with --no-check); restore refuses it."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepbackupcheckfailed
  - alert: LeepBackupSlow
    # Last 7 days against the 7 days before (needs 14 days of retention), above 10 minutes.
    expr: |
      avg_over_time(leep_backup_duration_seconds[7d]) > 600
      and
      avg_over_time(leep_backup_duration_seconds[7d]) > 1.5 * avg_over_time(leep_backup_duration_seconds[7d] offset 7d)
    labels: {severity: ticket}
    annotations:
      summary: "Backups take over 1.5 times longer than a week ago: the store is outgrowing the window."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepbackupslow
- name: leep-cron
  # Metrics written by the leep-cron wrapper (docs/RUNBOOKS.md#cron-wrappers),
  # not by Leep. Delete the absent() line of a task you do not run.
  rules:
  - alert: LeepBackupVerifyFailed
    expr: |
      leep_backup_verify_last_exit_code != 0
      or
      time() - leep_backup_verify_last_success_timestamp_seconds > 9 * 86400
      or
      absent(leep_backup_verify_last_success_timestamp_seconds)
    for: 30m
    labels: {severity: ticket}
    annotations:
      summary: "The weekly backup-verify failed or has not passed in 9 days: the backup medium may be damaged."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepbackupverifyfailed
  - alert: LeepRestoreDrillOverdue
    expr: |
      time() - leep_restore_drill_last_success_timestamp_seconds > 35 * 86400
      or
      absent(leep_restore_drill_last_success_timestamp_seconds)
    for: 1h
    labels: {severity: ticket}
    annotations:
      summary: "No passing restore drill in 35 days: an untested backup is not a backup."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leeprestoredrilloverdue
  - alert: LeepGcStale
    expr: |
      time() - leep_gc_last_success_timestamp_seconds > 3 * 86400
      or
      absent(leep_gc_last_success_timestamp_seconds)
    for: 1h
    labels: {severity: ticket}
    annotations:
      summary: "leep-server gc has not succeeded in 3 days: unreferenced blobs are piling up."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepgcstale
- name: leep-host
  # node_exporter metrics, not Leep's. Set the mountpoints to the filesystems
  # that hold the blob directory, the SQLite file or Postgres data, and the backups.
  rules:
  - alert: LeepDiskSpaceLow
    expr: |
      node_filesystem_avail_bytes{mountpoint=~"/var/lib/leep.*|/backups.*"}
        / node_filesystem_size_bytes{mountpoint=~"/var/lib/leep.*|/backups.*"} < 0.10
    for: 15m
    labels: {severity: ticket}
    annotations:
      summary: "A Leep data or backup filesystem has under 10% free; when it fills, writes fail."
      runbook_url: https://git.example.com/leep/blob/main/docs/RUNBOOKS.md#leepdiskspacelow
```

## Several servers on one ref

Group commit batches the lands one server process receives (ADR 0032).
Without coordination, servers that share a metadata store race each other's
batches through the ref's compare-and-swap, and under load one wins every
swap while the others' agents wait tens of seconds (`docs/PERFORMANCE.md`,
"Several servers on one ref"). With `--land-coordination on` (ADR 0056) one
server per ref, the holder of the ref's lease in the `leases` table (scope `land`),
batches every land on it; the others forward lands to it over HTTP.

**Status.** Off by default. A single server, or SQLite, does not need it. For
several servers on one Postgres it removes the starvation (no agent stalled
in any bench run), but on one shared host it cost 9 to 25% of one server's
throughput (`docs/PERFORMANCE.md`). It will be recommended as the
multi-server default only after throughput is measured with the servers on
separate hosts at 95% of one server's or better. Until then the alternative
is to route `POST /v1/land` to one instance at the balancer
(`docs/RUNBOOKS.md`, LeepLandExhausted).

```sh
leep-server --meta postgres://... --blobs ... --listen 0.0.0.0:7070 \
  --land-coordination on \
  --advertise-url http://10.0.3.7:7070/ \
  --peer-cidr 10.0.3.0/24
```

| Flag | Variable | Meaning |
|---|---|---|
| `--land-coordination on\|off` | `LEEP_LAND_COORDINATION` | Default `off`. Needs Postgres or SQLite, `--advertise-url` and an allow-list. |
| `--advertise-url URL` | `LEEP_ADVERTISE_URL` | Where other servers reach this one directly, not through the balancer. Checked at startup: the server fetches `URL/v1/health` and refuses to start unless the answer comes from itself. |
| `--peer-url-prefix P` | `LEEP_PEER_URL_PREFIX` | Repeatable (comma-separated in the variable). A land, with its client's token, is forwarded only to a lease address that starts with one of these, compared as raw text: write the scheme, host, port and the trailing slash, `https://leep-1.internal:7070/`, never `https://leep-` (which also matches `https://leep-evil.example`). |
| `--peer-cidr NET` | `LEEP_PEER_CIDR` | ...or whose host is an IP address in one of these networks. Requests from these networks (and from the IP hosts of the prefixes) are recognised as peers. |
| `--peer-allow-plain-http` | `LEEP_PEER_ALLOW_PLAIN_HTTP` | Allow `http://` to a host that is not a loopback or private IP address. Without it such a peer needs `https://`. |
| `--land-lease-ttl` | `LEEP_LAND_LEASE_TTL` | Default `2s`. Another server takes over a lease not renewed for this long; the holder itself stops trusting it one renew interval earlier (1.5 s by default). |
| `--land-lease-renew` | `LEEP_LAND_LEASE_RENEW` | Default `500ms`. At most half the TTL. |

**Peer networks.** `--peer-cidr` must cover the servers' own addresses and
nothing that forwards client traffic: exclude load balancers, ingress
controllers and sidecar proxies. A request that reaches a server from a
peer address with a `Leep-Land-Forward` header skips that token's
concurrency limit, so a client whose requests arrive through a proxy inside
a peer network could set the header itself and get round the limit. If the
servers sit behind a sidecar that connects to them over loopback, list the
sidecar-free addresses the servers use for each other, not `127.0.0.0/8`. An
allow-list of host names alone recognises no peer (names are never
resolved), which the server warns about at startup: forwarded lands are then
charged the concurrency limit twice, and a land reaching a server that no
longer leads is forwarded again.

**What it guarantees.** Correctness never depends on the lease: refs move
only by compare-and-swap, so two servers that both believe they lead are as
safe as two uncoordinated servers. The lease gives liveness: lands of a ref
are batched in one place, so no server's agents starve. A server takes a
ref's lease on the first land it receives for a ref nobody leads, renews it
every 500 ms in one statement for all its leases, releases it after 30 s
without a land, and releases all of them at the start of a graceful
shutdown. Lease calls use connections of their own (two on Postgres), so a
renewal never waits behind the data traffic of a busy pool. A SIGKILLed
leader's lease is taken over by another server once it has not been
renewed for the TTL; the crash test checks every agent lands again within
the TTL plus two seconds (`cargo test -p leep-e2e --test crash`).

**When the leader cannot be reached.** A server whose forward gets no answer
asks the leader again, or its successor once the lease changes, but never
batches the land itself while a live lease names another server: the land
may still be in that server's batch. After 30 s it answers 409, which
clients already retry. When the store cannot be read, a server forwards on
the last lease it read for as long as that lease's own time left lasts. It
batches locally only when no live lease is known and none can be taken (the
store is failing), counted in `leep_land_routed_total{route="uncoordinated"}`.
A server shutting down answers lands it no longer coordinates with 503
(`Retry-After: 1`), and `leep land` sends them again under their land id.

**Security.** A forward carries the client's own token to another server on
the allow-list, and the leader authenticates it and applies its own land
policy, so forwarding grants no identity. Tokens are sent only to addresses
on the allow-list, never through a redirect, and over plain http only to
loopback or private addresses unless `--peer-allow-plain-http`. Lease
addresses come from the metadata store, and anyone who can write it could
point a lease at another address; the allow-list bounds where a token can
go to the servers the operator listed. That is no new exposure: write access
to the metadata store already owns every ref of the repository. The store
holds no secret for coordination. The `Leep-Land-Forward` header is a hint:
from an allowed peer address it exempts the land from the per-token
concurrency cap (the sending server holds the permit) and stops it being
forwarded again; from anywhere else it is ignored. Rate limits are charged
on both servers.

**Rollout.** Servers from before this change do not read leases and keep
landing locally, racing the leader. During a rolling upgrade, keep routing
`POST /v1/land` to one instance at the balancer, then turn coordination on
everywhere and remove the rule. Migration 0006 only adds the empty
`leases` table. To back out, set `--land-coordination off`; leases left
behind expire within the TTL.

A tier of dedicated landing servers (API servers that only forward, landers
that only lead) is future work; today every coordinating server can lead.

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
- Limits do not apply to `GET /v1/health`, `GET /v1/ready` or to unauthenticated requests
  (which fail with 401 without costing a token anything); rate-limit those at
  the proxy.

### Body limits and the in-flight budget

Unlike the per-token limits these are always on (ADR 0020, "Server-wide body
limits"). A blob upload over `--max-blob-bytes` (default 256 MiB) or any other
body over `--max-json-body-bytes` (32 MiB) is a `413`, before anything is read
when the length is declared and as soon as it is exceeded when it is not. Every
request with a body also holds its `Content-Length` of `--max-inflight-bytes`
(1 GiB) until its handler is done; one that declares no length holds the blob
cap for a blob upload and 1 MiB for anything else. One that would go over is a
`503` (code `unavailable`) with `Retry-After: 1`. With a tokens file, one token may hold at most a
quarter of the budget (or one request of the largest cap, if that is more);
past its share it gets `429` (`rate_limited`), while other tokens go on. Bodies are held in memory
while they are hashed and stored, so the budget bounds that memory whatever the
number of clients. Requests without a valid token are refused before they hold
anything.

- Memory: plan for `--cache-bytes` plus `--derive-cache-bytes` plus
  `--max-inflight-bytes` plus a few hundred MB, and set the budget to fit the
  container. With 1 GiB of budget, four maximum-size uploads run at once; real
  uploads are mostly a few KB, so thousands do.
- `leep_request_body_bytes_in_flight` near `leep_request_body_budget_bytes`
  means uploads are queuing for memory. If it sits there with few requests per
  second, clients are uploading slowly (a slow link, or a client trickling a
  large declared body) and holding their share for up to `--request-timeout`.
  No token can hold more than its share; `reason="principal"` refusals name
  the token in the DEBUG log, and per-token `concurrency` bounds it further.
- `leep` sends a blob upload refused with a 503 again, up to three tries and
  10 seconds of waiting; past that it reports a server error (exit status 3),
  and running the command again uploads only what is missing.
- `--max-body-bytes` (512 MiB) stays a ceiling on every route; a cap above it
  has no effect. A cap above the budget stops the server at startup, since
  such a request could never be admitted.

### Expensive read limits

Each class of expensive read has a concurrency limit for the whole server,
whatever the token (ADR 0060, section 1). Per-token limits cannot do this: a
fleet of agents each running one search is within every token's limit while
the searches take every core the lands need. Measured on all of Linux, a
warm search costs about 0.45 s of CPU, and on a two-core host the swarm's 20
to 30 s searches were CPU queueing.

| Flag / env | Default | What it bounds |
|---|---|---|
| `--read-concurrency CLASS=N\|off` / `LEEP_READ_CONCURRENCY` (comma-separated) | `search` = half the cores, at least 1 | Requests of a class running at once. Repeatable; a later setting of a class wins. Only `search` (`GET /v1/search/{rev}`) is built. `land_check`, `blame`, `log_filtered` and `diff_large` accept only `off`, which is what the server does for them; a number for one of them stops startup. |
| `--search-pool-permits N\|off` / `LEEP_SEARCH_POOL_PERMITS` | a quarter of `--meta-pool-size`, at least 1 (4 for a pool of 16) | Directory opens of all searches that read the metadata store at once. Each holds up to two connections, so searches together hold at most half of the pool and lands and ref reads keep the rest. |

- A search that finds every permit in use is refused at once, never queued:
  `503`, code `unavailable`, `Retry-After` and `Leep-Refused: read-class`.
  `Retry-After` is the class's median request time over its last 64
  requests, divided by its permits and rounded up, between 1 and 10 s.
  10 s is the CLI's longest wait: `leep` retries a refused `GET` up to four
  times with up to 50% jitter, and the MCP server hands the refusal back to
  the agent as a retryable error without waiting.
- The permit is held until the response body is sent, or the client goes
  away or `--request-timeout` drops the request. A UI that searches as the
  user types should cancel the request it replaces (the permit is released
  on cancel) or debounce, rather than raise the class: an overlapping
  request is refused for at least 1 s. Cores are the limit: the
  count is `available_parallelism`, which follows the container's CPU quota
  on Linux.
- Never limited: lands, ref reads (`/v1/refs`, `/v1/resolve`, `/v1/reflog`),
  trees, files, blobs, changeset creates, `GET /v1/health` and
  `GET /v1/ready`. Limits are per process, so N servers run up to N times a
  class's permits.
- Tuning. `leep_read_class_in_flight` at `leep_read_class_permits` with a
  rising `leep_read_class_shed_total` means searches wait for cores. On a
  host whose lands are idle, raise the class; on a busy one, add query hosts
  (ADR 0058's split). A pool permit that waits shows as search latency, not
  as refusals; raise `--search-pool-permits` only with `--meta-pool-size`.
- To back out, set `--read-concurrency search=off` (and
  `--search-pool-permits off`) and restart. Nothing persisted depends on
  either. Balancers must not count these 503s against a backend (see
  [Indexing a large history](#indexing-a-large-history), "Balancers").

## Config reload

`kill -HUP` re-reads the tokens, signing keys and review policy files (ADR 0024;
procedure and what is covered in [DEPLOY.md](DEPLOY.md#reloading-config)).
Watch it with:

- Logs: `SIGHUP: reloading config`, then `INFO` lines per change (`tokens
  changed`, `request limit state updated`, `trusted signing keys changed`,
  `protected refs changed`, `review rules changed`) listing names and counts,
  then `config reloaded` with the new `generation` and the `fingerprint` (and
  `previous_fingerprint`) of the config now in force; the fingerprint is also
  logged at startup (`config loaded`). On failure a single `ERROR`
  `config reload failed; keeping the previous config` carries the file and the
  reason. Tokens and digests are never logged.
- Metrics: `leep_config_reload_total{result}`, `leep_config_info{fingerprint}`,
  `leep_config_generation` and `leep_config_last_reload_timestamp_seconds`.
  Alert on any `error`, and on servers whose `leep_config_info` fingerprints
  differ for longer than a rollout takes. The fingerprint depends only on what
  the config contains, so a restart, or a reload that changed nothing, never
  causes a difference. `leep_config_generation` is a per-process count that
  resets on restart; read it to see whether *this* server reloaded, not to
  compare servers. Runbooks:
  [LeepConfigReloadFailed](RUNBOOKS.md#leepconfigreloadfailed),
  [LeepConfigSkew](RUNBOOKS.md#leepconfigskew).
- A reload that logs `changed=false` found the files identical to what was
  running. Check that the file you edited is the file the server was started
  with (`LEEP_TOKENS_FILE` and friends), and, in a container, that you mounted
  the directory.

After a failed reload the server is healthy but its config is stale: a token
you meant to revoke still works. Treat `LeepConfigReloadFailed` as urgent when
the edit was a revocation. A failed reload never leaves a half-applied config;
fix the file and send SIGHUP again, or restart (which applies the same checks
and refuses to start on a bad file). A restart is needed only for flags and
listeners.

Revocation is immediate for new requests. A request admitted before the swap
finishes, at most `--request-timeout`; to cut one off sooner, restart.

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

A land that names itself with a `land_id` (ADR 0057; `leep land` and
`leep-mcp` always do) has the id stored in its entry. That is how a retry of
the land, however late, is answered with the original result instead of
landing twice, so the ids are kept with the log: about 120 more bytes per
keyed land, index included. Never prune or rewrite `ref_log` by hand; a
removed entry takes its id with it, and a retry older than that falls back
to the landed-copy scan, which only sees the last 1024 changesets.
`GET /v1/reflog` shows the id as `land_id`, with `land_changeset` (the
changeset the land asked for; `new` is it or its rebased copy), and
`leep reflog` prints it as `land=ID`, so a client's land can be found in the
log by the id it sent.
`leep_land_recorded_total` counts lands answered from a recorded id: a
steady rate means clients are retrying lands that had already applied
(timeouts, restarts), which is expected in small numbers.

### Protected refs and the log

The log records how a ref moved; it does not stop a move. A write token can
reset or delete any ref no land policy covers, and the log is then the way to
find and undo it. Refs that must never go backwards (`main`, release branches)
belong in `--protect-ref` (`LEEP_PROTECT_REFS`) if no signature or review
policy names them already ([DEPLOY.md](DEPLOY.md#protected-refs)): such a ref
only moves forward and cannot be deleted through the API, and a refused move
writes no entry. The startup line `land policy protects refs` lists every
protected ref, whichever flag or file protects it. Moving a protected ref back
by hand needs the metadata store itself; prefer `leep revert` and a land.

## Git mirror

`leep-git mirror run` keeps a git remote (GitHub, GitLab, any server git
can push to over https, ssh or a path) in step with a Leep server
([ADR 0046](adr/0046-live-git-mirror.md)). Lands reach the remote within
about a second plus the push time. With `--import`, commits pushed to the
remote land in Leep when they fast-forward the Leep ref. When both sides
moved, the mirror reports `diverged` and stops that ref until someone merges
in git; with `--on-divergence merge` it first tries a clean merge on the
server and lands it. With `--land-branches`, people push to
`leep-land/<ref>/<name>` and the mirror lands that work through Leep like an
agent's land ([ADR 0049](adr/0049-mirror-merges-and-land-branches.md)). With
`--ref-prefix review/`, every Leep ref under `review/` is pushed to the
branch of the same name as it appears, and its branch is deleted when the
Leep ref is ([ADR 0061](adr/0061-mirror-ref-prefixes.md)). It never resolves
a conflict itself and never force-pushes a mirrored branch.

The mirror is a separate process. It talks to `leep-server` over the HTTP
API with a token, and to the remote with the system `git` (2.30 or later).
It needs no access to the metadata or blob stores. Run one mirror per remote.

```sh
LEEP_TOKEN=$(cat /etc/leep/mirror.token) \
leep-git mirror run \
  --server https://leep.internal:8080 \
  --remote https://github.com/acme/app.git \
  --work-dir /var/lib/leep-mirror/app \
  --ref main --ref release/1.0 --tags \
  --import --on-divergence merge --land-branches \
  --listen 127.0.0.1:9188
```

- **Token.** Pass it with `--token-file` or `LEEP_TOKEN`, never on the
  command line. A `read` token is enough without `--import`. With
  `--import` the mirror creates changesets and moves refs, so it needs a
  `write` token. Its name is the actor of the mirror's ref log entries
  (operation `git-mirror`), so give it a recognisable name such as
  `git-mirror`.
- **Git credentials** stay with git:
  - for https, a credential helper or a token in the helper's store. A
    token embedded in the URL also works, and the mirror redacts it from
    every log line and the status file, but a helper keeps it out of the
    process list;
  - for ssh, a deploy key with write access, for example through
    `GIT_SSH_COMMAND="ssh -i /etc/leep/deploy_key -o IdentitiesOnly=yes"`.

  The mirror sets `GIT_TERMINAL_PROMPT=0`, so a missing credential fails
  with an error rather than waiting for a password.
- **Work directory.** Holds `git/` (a bare repository of everything
  exported and fetched), `map` (changeset to commit), `status.json` and a
  lock that stops a second mirror on the same directory. Keep it on
  persistent storage and back it up.
  - If you lose it, a new mirror rebuilds it by exporting again; exports
    are deterministic, so the commits come out the same. The exception is a
    commit imported from git that Leep cannot reproduce, such as one with
    submodules: that ref then reports `diverged` instead of pushing
    anything wrong.
- **Branch protection.** The remote must let the mirror's credential push
  to the mirrored branches, and create and delete branches under
  `--diverged-prefix` (default `leep-diverged/`) and, with
  `--land-branches`, delete branches under `--land-prefix` (default
  `leep-land/`). People need to be able to push there. The mirror never
  needs force-push rights: it deletes a land branch with a lease on the
  value it landed.
  - With `--ref-prefix`, it must also create, update with a lease and
    delete branches under each prefix. Nobody else should push there.
  - An exception is a ref listed in `--allow-rewind`, which it updates with
    `--force-with-lease`. `main` and tags cannot be listed there.
  - Required pull requests or status checks on the mirrored branch make
    every push fail. The ref goes to `error` after three rejections. Let the
    mirror's account bypass those rules.

| Flag | Default | Meaning |
|---|---|---|
| `--ref NAME[:GIT_REF]` | – | Mirror a Leep ref, repeatable. `NAME` maps to `refs/heads/NAME`, `tags/T` to `refs/tags/T`. |
| `--tags` | off | Also mirror every Leep tag. Tags are created, never moved. |
| `--ref-prefix PREFIX` | – | Also mirror every Leep ref under `PREFIX` (such as `review/`) to the branch of the same name, as refs appear and go. Repeatable. Push-only; see "Prefix refs" below. Must end in `/` and not overlap `--diverged-prefix`, `--land-prefix`, another prefix or any `--ref`. |
| `--import` | off | Land fast-forward pushes from the remote in Leep, through the land policy. |
| `--interval` | `1s` | How often to check the ref log for moves. |
| `--git-poll` | `15s` | How often to `ls-remote` the remote. `POST /poke` polls at once. |
| `--reconcile` | `60s` | Re-read every Leep ref even if the ref log is quiet. |
| `--git-timeout` | `10m` | Kill a git command that runs longer. |
| `--max-backoff` | `5m` | Longest wait between retries of a failing ref. |
| `--allow-rewind NAME` | – | Push Leep undos of this ref with a lease. Never `main` or a tag. |
| `--diverged-prefix` | `leep-diverged/` | The branch prefix the mirror owns. |
| `--on-divergence` | `report` | When both sides moved: `report` stops the ref for a merge in git; `merge` (needs `--import`) asks the server for a merge and lands it when it is clean and the land policy allows, and reports otherwise. |
| `--merge-author` | `Leep git mirror <git-mirror@leep.invalid>` | Author of the mirror's merges. Part of each merge's id: choose once. |
| `--land-branches` | off | Land remote branches `PREFIX<ref>/<name>` onto the Leep ref `<ref>` through the server's land, then delete them. Needs `--import`. |
| `--land-prefix` | `leep-land/` | Prefix of land branches. Must not overlap `--diverged-prefix` or any mirrored branch. |
| `--provenance-trailers`, `--no-changeset-trailer` | off | Change commit messages, and so commit ids. Choose once, before the first push. |
| `--listen ADDR` | off | Serve `GET /metrics`, `GET /status`, `GET /health` and `POST /poke`. |
| `--once` | off | Run until nothing changes, then exit 0 if healthy and 1 if not (for cron or a check). |

**First start against an existing remote.** If the remote is the repository
Leep was first imported from (`leep-git import`), the mirror's first run
exports Leep's history over HTTP. Imported changesets reproduce their
original commits, so the first push is a fast-forward. If the remote was
filled by `leep-git export`, add `--no-changeset-trailer` to produce the
same commits. Without it, the ref reports `diverged` on the first run.

**Webhooks.** To skip the `--git-poll` delay, point a push webhook from
GitHub or GitLab at `POST http://<mirror>/poke`, through a proxy that only
forwards that path. The poke carries no data and needs no secret: at worst
a forged one makes the mirror poll sooner.

### Mirror states

`leep-git mirror status --work-dir DIR` prints each ref's state, both
values, the lag and the reason. `--json` prints the whole status document,
which `GET /status` also serves. The command exits 0 when every ref is
`in-sync` or `pending`, 1 when a ref needs a person or the status is more
than three intervals old (the daemon is down or stuck in a long push), and 2
when there is no status file.

| State | Meaning | What to do |
|---|---|---|
| `in-sync` | The remote ref is the export of the Leep ref. | Nothing. |
| `pending` | One side moved and the mirror is catching up, or retrying after the remote moved under a push. | Nothing, unless the lag keeps growing ([LeepMirrorLagging](RUNBOOKS.md#leepmirrorlagging)). |
| `diverged` | Both sides have commits the other lacks. Leep's side is on `leep-diverged/<branch>`. With `--on-divergence merge` the detail says why the mirror did not merge: the conflicting paths, a land policy refusal, or a remote tip Leep moved back from. | In git, merge `origin/leep-diverged/<branch>` into the branch and push. The mirror lands the merge and deletes the diverged branch. |
| `rewound` | Leep moved the ref back (an undo). The remote keeps the newer commits. | Land a revert in Leep (the usual fix), or, for a scratch branch, allow it with `--allow-rewind`. |
| `remote-ahead` | The remote has commits Leep lacks and `--import` is off. For a prefix ref: someone pushed to its branch, before or after the Leep ref was deleted. | Enable `--import`, or remove the commits from the remote. For a prefix ref, land the work in Leep, then delete the branch. |
| `refused` | Leep's land policy refused a push from git (unsigned, unreviewed, OWNERS). The detail quotes the server. | Approve the changesets with `leep review`, or revert the push in git. Checked again every minute. |
| `deleted-in-leep` | The Leep ref of a `--ref` is gone. The remote branch is left alone. (A prefix ref's branch is deleted instead.) | Delete the remote branch by hand if that is intended. |
| `error` | The last attempt failed: server, remote, credentials, or the remote rejecting pushes. Retried with backoff. | Read the detail and the log ([LeepMirrorLagging](RUNBOOKS.md#leepmirrorlagging)). |

### Mirror metrics

| Metric | Type | Labels | Meaning |
|---|---|---|---|
| `leep_mirror_up` | gauge | none | 1 while the mirror serves metrics. |
| `leep_mirror_ref_state` | gauge | `ref`, `state` | 1 for the state each ref is in, 0 for the others. |
| `leep_mirror_lag_seconds` | gauge | `ref` | Seconds since the two sides first differed; 0 in sync. |
| `leep_mirror_last_sync_timestamp_seconds` | gauge | `ref` | When the ref was last in sync. |
| `leep_mirror_pushes_total` | counter | `ref`, `result` = `ok`, `rejected`, `failed` | Pushes of mirrored refs. `rejected` is the remote refusing (it moved, or branch protection); `failed` is a transport or credential failure. |
| `leep_mirror_imports_total` | counter | `ref`, `result` = `landed`, `refused`, `lost_race` | Remote pushes the mirror tried to land in Leep. |
| `leep_mirror_divergences_total` | counter | `ref` | Times the ref entered `diverged`. A steady rate means people push to a ref agents land on all the time. With `--on-divergence merge` only divergences the mirror could not merge count. |
| `leep_mirror_merges_total` | counter | `ref`, `result` = `landed`, `conflicts`, `refused`, `lost_race` | The mirror's automatic merges (`--on-divergence merge`). `conflicts` and `refused` fall back to `diverged`. |
| `leep_mirror_land_branches_total` | counter | `ref`, `result` = `landed`, `conflicts`, `refused` | Land branches tried, counted once per pushed commit. |
| `leep_mirror_land_branches_failing` | gauge | `ref` | Land branches left on the remote because they conflict or were refused (`ref=""` for a name that matches no mirrored ref). |
| `leep_mirror_branches_deleted_total` | counter | none | Branches under a `--ref-prefix` deleted because their Leep ref was deleted. |
| `leep_mirror_commits_exported_total` | counter | none | Commits written from changesets. |
| `leep_mirror_commits_imported_total` | counter | none | Changesets created from remote commits. |
| `leep_mirror_errors_total` | counter | `stage` = `leep`, `git`, `export`, `import` | Failed steps. |
| `leep_mirror_cycles_total` | counter | none | Poll cycles. |
| `leep_mirror_last_cycle_timestamp_seconds` | gauge | none | When the last cycle finished. |

Counters restart from 0 when the mirror restarts. The log goes to stderr,
one line per state change, push or landed import, with credentials
redacted.

### Prefix refs

With `--ref-prefix review/`, the mirror follows Leep refs under `review/`
without a `--ref` for each. Agents land on `review/<area>-<desc>` and the
branch of that name appears on the remote, ready for a pull request.

- **Found as they appear.** Each read of Leep's refs adds new ones; the
  status lists them with `"ref_prefix": "review/"`.
- **Push-only.** A prefix ref is never imported, merged, or landed onto by a
  land branch, whatever `--import`, `--on-divergence` and `--land-branches`
  say. A push to its branch is reported as `remote-ahead` (or `diverged` if
  Leep moved too) and left there.
- **Rewrites.** When an agent rebases or squashes its review ref, the mirror
  pushes the new value with `--force-with-lease` on the old one, but only
  if the Leep ref held the remote's tip (checked in the ref log). Otherwise
  it reports, as above.
- **Deletes.** When the Leep ref is deleted, the mirror deletes the branch
  with a lease on that same tip, under the same check, and stops tracking
  it. This is healthy: the deletion is listed under `deleted_branches` in
  the status and counted in `leep_mirror_branches_deleted_total`. A branch
  deleted in Leep while the mirror was down is deleted on its next start.
  If the remote refuses the lease, or the branch holds commits the Leep ref
  never held, nothing is deleted and the ref reports `remote-ahead`.

The mirror never deletes or rewinds a branch outside its prefixes, a tag,
or a `--ref` branch such as `main`.

### Land branches

With `--land-branches`, a person hands work to Leep by pushing it to
`leep-land/<ref>/<name>` instead of to `<ref>`:

```sh
git push origin HEAD:leep-land/main/fix-login
```

The mirror imports the commits and lands them with `POST /v1/land`: a
fast-forward if the branch sits on the current head, otherwise the server
rebases them onto it like an agent's land. Then it deletes the branch and
pushes the new head to `main`. `<ref>` is the longest mirrored branch the
name continues, so `leep-land/release/1.0/x` lands on `release/1.0` when
that is mirrored.

A land branch that cannot land stays on the remote and is listed under
`land_branches` in the status (`leep-git mirror status` prints it as
`land branch ... conflict` or `refused`):

| State | Meaning | What to do |
|---|---|---|
| `pending` | Seen and not landed yet. | Nothing. |
| `conflict` | It conflicts with the ref; the detail lists the paths. Checked again every 30 seconds. | Rebase it onto `origin/<branch>`, resolve, and push it again (with `--force-with-lease`; it is your branch). |
| `refused` | The land policy refused it, it contains a merge commit (land replays only linear stacks), the ref does not exist, or its name matches no mirrored ref. Checked again every minute. | Read the detail: get a review or signature, rebase away the merge, or fix the name. |
| `error` | The last attempt failed (server or remote). Retried with backoff. | As for the ref's `error` state. |

A failing land branch does not make `mirror status` exit 1: it needs the
person who pushed it, not the operator.
[LeepMirrorLandBranchFailing](RUNBOOKS.md#leepmirrorlandbranchfailing)
opens a ticket after an hour.

**Crashes.** Killing the mirror at any moment is safe, SIGKILL included.
Each cycle decides from the current Leep ref and remote ref, not from
remembered progress. A push the remote accepted before the crash is found
there, and a push is never repeated. An import that created changesets but
did not move the ref finishes on restart, without importing anything twice.
A merge the mirror made but did not land is made again with the same id,
because its author, message and time depend only on the two sides. A land
branch landed but not deleted is landed again, which the server answers
with what is already there, and then deleted.
`LEEP_MIRROR_CRASH_AT` is a test hook that aborts the process at a named
step; never set it in production.

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
alter the schema. `gc`, `derive-search`, `verify`, `repair` and `upgrade-index` follow the same
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

**Migration 5 (land ids, ADR 0057) on a large Postgres.** The migration adds
two nullable columns to `ref_log` (instant) and builds a unique index on it,
which holds a `SHARE` lock on `ref_log` for the whole build: no ref moves,
so no lands, until it finishes. On a ref log of millions of entries, build
it by hand first, beside the running v4 servers, without blocking writes:

```sql
ALTER TABLE ref_log ADD COLUMN IF NOT EXISTS land_id BYTEA;
ALTER TABLE ref_log ADD COLUMN IF NOT EXISTS land_changeset BYTEA;
CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS ref_log_land_id
    ON ref_log (name, land_id) WHERE land_id IS NOT NULL;
```

(in the schema the servers use). A `CONCURRENTLY` build that fails leaves an
invalid index under that name, which `IF NOT EXISTS` would then accept:
check `SELECT indisvalid FROM pg_index WHERE indexrelid =
'ref_log_land_id'::regclass` is true, and if not, `DROP INDEX CONCURRENTLY
ref_log_land_id` and build it again. Then run `leep-server migrate`, which
finds the columns and index and only records the version.

**Upgrade every server before clients rely on land ids.** A v4 server
ignores `land_id`, so it cannot answer a late retry with the earlier result;
only the landed-copy scan (the last 1024 changesets) protects it. `leep
land` resends a land saved in `state.json` from an earlier run only to a
server whose `GET /v1/ready` lists `land-id` in `features`; against an
older server it stops and says the land may have applied (check `leep
reflog REF`). Other clients that resend across runs should do the same.

**Migration 7 (`changesets.stored_at`, ADR 0059).** Adds one nullable
column with no default: on Postgres and SQLite a catalog change that
rewrites no row, so once it has its lock it is instant on any size, and it
may run beside servers of the previous version. On Postgres that lock is
ACCESS EXCLUSIVE on `changesets` and waits for every open transaction that
read the table, while every new reader and writer queues behind it. **Do
not run it while a backup or `gc --unreachable --dry-run` is in flight.**
On Postgres, Leep's migrations wait at most 5 s for a table lock (`lock_timeout`) and
then fail without changing anything (`could not lock a table within 5s`);
run `leep-server migrate` again when the long transaction has finished. They keep storing changesets without the column,
which read as stored when the migration ran; finish the rollout before the
first changeset condemnation (see [Changeset garbage
collection](#changeset-garbage-collection)).

**Derived kinds that change encoding.** A release that changes how a derived
kind is stored gives it a new name, so old and new servers never read each
other's rows; nothing to migrate. The new kind derives on first use, which on
a long history means the first query after the upgrade derives it for the
whole history. One command does the whole job, with the new binary against
the shared stores:

```sh
leep-server upgrade-index                         # plan only: exit 0 nothing to do, 1 work to do
leep-server upgrade-index --yes --jobs 8          # derive what is missing (beside the old servers)
# ... replace the old servers with new ones ...
leep-server upgrade-index --yes --drop-retired --old-servers-stopped   # then drop the old index
```

- The plan reads the history of every ref once (about the time `repair
  rederive` takes to plan) and counts rows without reading them. On
  Postgres it estimates a kind of more than 100,000 rows from the planner's
  statistics, which takes milliseconds where `count(*)` takes about 30 s a
  kind on all of Linux. Estimates are marked `~`. It counts exactly where a
  decision rests on the number: whether a kind has any rows (always exact),
  and whether a kind whose ref heads all have rows is short. An estimate
  below the reachable changesets is checked with a count before the plan
  calls the kind partial. One case is caught only by an exact count: an
  interrupted `repair drop` of a *current* kind that left the heads' rows,
  before autovacuum's `ANALYZE` has seen the delete. After such a drop, run
  `ANALYZE derived` or pass `--exact-counts`. SQLite always counts. For each
  kind this version reads it shows rows, how many reachable changesets lack
  one, and `complete`, `partial` or `missing`; then the retired kinds that
  still have rows (such as `history-tree-v3` with `history-node-v3`) and the
  size of their values; then the steps with estimated time (from the rates
  in PERFORMANCE.md: about 150 changesets a second per free core for
  `history-tree-v4`, 25,000 to 140,000 rows a second for a drop), the disk
  the new rows need against the free space it can see (the SQLite file's
  filesystem, or Postgres's data directory when the role may read
  `data_directory` and the database is on this machine; otherwise it says it
  cannot tell), and the command to reclaim space. It changes nothing.
- `--yes` derives the missing rows exactly as `repair rederive --missing
  --jobs N` does: from each ref's head, parents first, `--jobs` at a time,
  with rows, rate and an ETA every ten seconds. Stop it at any point and run
  it again: it continues. If the counts show rows missing behind stored ones
  (a `repair drop` of a current kind that was stopped), it checks every
  reachable changeset and fills those too. It refuses to start when the free
  space is known to be smaller than its estimate (`--skip-space-check`).
- Retired kinds are dropped only with `--drop-retired`, which requires
  `--old-servers-stopped`: servers of the previous version read them, so
  drop only once none is left. The drop is `repair drop`'s walk (ADR 0052),
  after the current kinds are complete. `--drop-first` drops before deriving,
  for a disk that cannot hold both (path queries then wait for derivation
  until the run has caught up), and with `--vacuum` runs `VACUUM (ANALYZE)
  derived` so Postgres reuses the freed space for the new rows.
- It never runs `VACUUM FULL`, which locks the table: it prints the command
  for the backend. `--vacuum` runs the plain, non-blocking `VACUUM (ANALYZE)
  derived` on Postgres after a drop.
- Exit status 0 when done or nothing is left to do, 1 when a plan shows work
  (or a run left a kind incomplete), 2 when it could not run (including
  `--drop-retired` without `--old-servers-stopped`). `--json` prints the
  plan, estimates and report as one object.

The manual steps it runs, for reference: warm the new kind with
`leep-server repair rederive --kind <new kind> --missing` before sending
traffic, and drop the old kind once no server of the previous version is
left (`repair drop` accepts retired kind names; it changes no answer of the
new servers). The path-history index moved from
`history-tree-v1` to `history-tree-v2` this way (ADR 0027), from
`history-tree-v2` to `history-tree-v3` (ADR 0037), and from
`history-tree-v3` to `history-tree-v4` (ADR 0045). The warmer derives the new
index in the background after the new servers start; on a long history,
rederive first if path queries must not wait for it:

```sh
leep-server repair rederive --kind history-tree-v4          # warm the new index (old servers keep serving from v3)
leep-server repair drop --kind history-tree-v3 --dry-run    # count the old rows (and history-node-v3)
leep-server repair drop --kind history-tree-v3              # delete them once no old server runs
leep-server repair drop --kind history-tree-v2              # rows left from before ADR 0037, if any
leep-server repair drop --kind history-tree-v1              # rows left from before ADR 0027, if any
```

**From `history-tree-v3` to `history-tree-v4` on a large store** (ADR 0045).
`upgrade-index` runs these steps; they are kept here to say what each costs.
The v4 index is about a sixth of v3's size: on all of Linux, where v3 holds
24 GB of values in 25 million rows, v4 should take about 2.5 to 4 GB. With
the old servers still serving from v3:

1. Check that the disk has room for the v4 index beside v3 (a sixth of v3's
   size, plus Postgres overhead), then run the new binary's
   `leep-server repair rederive --kind history-tree-v4 --missing --jobs N`
   against the shared database (see [Indexing a large
   history](#indexing-a-large-history)). It is safe beside running servers,
   derives parents first and can be stopped and run again; with `--missing`
   it skips what is already derived (without it, every stored row is
   recomputed and compared again). On Linux expect hours with one job (v4
   derives at about v3's rate or faster on Postgres: 187 to 247 changesets a
   second on the v2.6.16 slice against 175 to 221 for v3), divided by about
   the number of idle cores.
2. Replace the old servers with new ones. Path queries are answered from v4
   at once.
3. `repair drop --kind history-tree-v3 --dry-run` (it counts without reading
   values: about a minute on Linux), then without `--dry-run`, which deletes
   `history-tree-v3` and `history-node-v3` rows in one walk of the `derived`
   table in storage order, a range of pages per short transaction (ADR 0052).
   It prints rows deleted, the rate, how far through the table it is and an
   ETA every ten seconds. Expect about the time to read the table once and
   write about as much WAL and data: 10 to 20 minutes for Linux's 33 GB
   table on a disk that streams 150 to 300 MB/s (it took 5.5 hours before
   ADR 0052). Make sure `pg_wal` can absorb that write rate: WAL is about
   the size of the table, recycled at each checkpoint. It is safe beside
   running servers; if it is stopped, run it again, which reads the table
   again from the start but has only the rest to delete.
4. Postgres keeps the freed pages for reuse. Soon after the drop autovacuum
   vacuums `derived` (another sequential pass over the table) so the space
   can be reused. To give the space back to the operating system, run
   `VACUUM FULL derived` in a maintenance window (it locks the table and
   needs room for a copy of what remains) or use `pg_repack`. Then
   `ANALYZE derived`.

If the disk cannot hold both indexes, drop v3 first (step 3), start the new
servers and let the warmer (or `rederive`) build v4: path queries wait for
derivation, or derive on demand, until it has caught up.

**Compressing changesets (schema v3, ADR 0053).** Migration 0003 adds an
empty `changeset_dicts` table and changes nothing old servers read, so it
may be applied while servers of the previous version still run (rolling
upgrade). The space saving is a separate step, run once every server is on
this version or later, because older servers cannot read compressed rows:

1. Finish the rollout: no server, `leep-git` importer or tool older than
   this release may still use the database.
2. See what it would do (trains a dictionary in memory, writes nothing):
   ```sh
   leep-server compact-changesets --dry-run
   ```
3. Run it. It stores a dictionary trained on 50,000 changesets of this
   store, then rewrites every plain changeset row in batches; servers keep
   serving, and reads and writes are unaffected (each row reads the same
   before and after). It can be stopped and run again; a later run only
   compresses rows that are still plain. On Postgres each rewritten row is a
   new, smaller row version beside the old one until vacuum, so it needs free
   disk of about half the table.
   ```sh
   leep-server compact-changesets
   ```
   About 4 minutes per million changesets of Linux's size on Postgres
   (PERFORMANCE.md, "Changeset compression"), with the `changesets` table
   about halved. Servers pick up the dictionary on their next read of a
   compressed row, or within a minute for writes; rows a server wrote plain
   meanwhile are compressed by the next run, so run it again after a day or
   on a schedule (it skips compressed rows, but reads every row).
4. On Postgres the old row versions are free space in the table; `VACUUM
   FULL changesets` (locks the table for the copy; minutes on all of Linux)
   or `pg_repack` returns it to the filesystem, then `ANALYZE changesets`.
   On SQLite, `VACUUM`.

Never delete a `changeset_dicts` row: the changesets compressed with it
cannot be read without it. Backups carry both tables. `--retrain` trains a
new dictionary (for example after the history has changed character) and
moves every row to it; the old dictionary stays. A store with fewer than
1,000 changesets is left alone.

### Format features

**Format features (schema v4, ADR 0055).** Migration 0004 adds a
`store_features` table. Like 0003, it may be applied beside servers of the
previous version. A row names a format the store's rows use that a server
must understand. `compact-changesets` records `changeset-zstd-dict` before
it stores a dictionary, and the migration records it for a store compacted
before the table existed. A server, or any `leep-server` tool, that opens a
store naming a feature it does not know refuses to start:

```
the metadata store uses format features this server cannot read (some-feature);
a newer leep-server wrote them. Upgrade this server
```

Upgrade that binary. Do not delete the row: the rows it describes are
already written, and an older binary would fail on them anyway.
`leep-server verify` and backups behave the same way, and backups carry the
table.

The check only protects against binaries that have it. **Servers from
before this release do not read the table**, and those from before ADR 0053
cannot read compressed rows. So for this release the rule above still
holds: no server, importer or tool older than ADR 0053 may use the
database when `compact-changesets` runs. From now on, a format change that
old servers cannot read is refused by them at startup, instead of failing
their reads.

## Connection pool and caches

Two settings size a `leep-server` process. Both are global flags, so they
apply to the server and to its tools (`verify`, `repair`, `upgrade-index`,
`gc`, `derive-search`):

| Flag / env | Default | What it bounds |
|---|---|---|
| `--meta-pool-size` / `LEEP_META_POOL_SIZE` | 16 | Connections this process opens to a Postgres metadata store. SQLite ignores it. |
| `--derive-cache-bytes` / `LEEP_DERIVE_CACHE_BYTES` | 423624704 (404 MiB) | The in-memory caches of derived indexes: path-history nodes, parts and roots (148 MiB), tree nodes (32 MiB), search's directory filters (64 MiB), and the filtered-log caches (160 MiB). The ancestry and skiplist caches are bounded by entries, about 100 MB together by default, and scale by the same factor. |

`--cache-bytes` (256 MiB) is separate: it caches blobs.

- **Pool.** A request or derivation job holds at most one connection at a
  time. Raise the pool with `--derive-jobs` or `rederive --jobs` past 16 on
  a machine with the cores for it. Every server and tool has its own pool,
  so keep the sum of all pools under Postgres's `max_connections` (100 by
  default) less what other clients need. A pool that is too small makes
  requests wait. They do not fail unless they wait past sqlx's 30 s acquire
  timeout. A search opens directories ahead of its walk, two reads each, so
  searches together are capped by the server's `--search-pool-permits`
  (default a quarter of the pool): they hold at most half of it, and lands
  and ref reads keep the rest (see
  [Expensive read limits](#expensive-read-limits)).
- **Statement bound.** sqlx sends Postgres no cancel request when a query's
  future is dropped, which is what `--request-timeout` does to a request that
  runs too long: the request answers 408 but its statement runs on in the
  database, holding a pool connection, locks and CPU, until it ends. The
  server therefore sets `statement_timeout` on its data connections to
  `--meta-statement-timeout` (default `--request-timeout` plus 5 s; `0` turns
  it off), so Postgres cancels such a statement within that bound. The margin
  lets the request's own timeout fire first. Lease connections (renewals must
  not be cancelled under load) and migrations are not bounded, and the
  command-line tools (`gc`, `backup`, `repair`, ...) set no bound. The server
  also uses the data pool for background index warming, whose statements are
  batches of milliseconds to seconds. A statement that hits the bound fails
  with `canceling statement due to statement timeout` in the ERROR log; if a
  legitimate one does, raise the flag rather than turning it off.
- **Derive caches.** Every cache gets its default share of the budget
  times `budget / 404 MiB`. More helps derivation of very wide histories
  and filtered logs over long ranges: `history-tree-v4` derivation re-reads
  hot directory nodes, and `log-filter-v1` merge walks reuse fingerprints.
  Search keeps the `tree-trigrams-v1` rows it reads from the store (64 MiB
  by default, about 1,000 dense filters), so repeated searches stop moving
  the upper directories' filters out of the metadata store each time.
  Less suits small containers. A small budget changes no answer, only how
  often rows are read again. RSS also holds the blob cache, request
  buffers and the derivation working set.

## Indexing a large history

After importing a long history (`leep-git import`, a restore without
derived rows, or a `repair drop`), the lazily derived indexes
(`history-tree-v4` for path history and blame, `log-filter-v1` for filtered
logs) are missing for every changeset. The warmer derives them in the
background (ADR 0028), and since ADR 0050 every derivation of a missing range
uses several cores: changesets whose parents are done derive side by side,
`--derive-jobs` at a time (`LEEP_DERIVE_JOBS`, default the cores up to 8).
To build them before sending traffic, or on a machine of its own, run:

```sh
leep-server repair rederive --kind history-tree-v4 log-filter-v1 --missing --jobs 16
```

- `--missing` derives only rows that are absent and never reads stored
  ones again, so the command can be stopped (Ctrl-C, a reboot, `kill -9`)
  and run again: it resumes where it was. Rows reach the database in
  windows of 256 changesets, always after their parents' rows, so a stop
  loses at most the windows in flight. Without `--missing`, `rederive`
  recomputes and compares every stored row too, which is what repairing
  damaged rows needs and costs a full pass however much is already there.
- `--jobs N`: changesets derived at once. The work is CPU-bound in the
  `leep-server` process, so use about the cores the machine can spare;
  with Postgres on another host a little more (1.5 to 2 times the cores)
  hides round trips. Each job may hold a Postgres connection at a time and
  the pool has 16 (`--meta-pool-size`), so more jobs than connections gains
  nothing; see [Connection pool and caches](#connection-pool-and-caches). Memory stays near
  that of one job (a few hundred MB on Linux-sized histories); the
  history-row and tree caches are shared.
- Run `ANALYZE changesets, refs, derived;` first if the import did not (see
  the next section).
- Rows are byte-identical whatever `--jobs` is, and identical to what a
  server's warmer or a query derives, so servers may keep running and
  warming beside it.
- `--dry-run` runs one changeset at a time.

`rederive` prints the rows derived so far every five seconds. On a server,
the rate of `leep_derive_total{kind}` is the warmer's derivation speed.

**The warmer with several servers.** In `--indexing local` (the default)
every server runs a warmer: it reads the ref log every `--warm-interval`
(`LEEP_WARM_INTERVAL`, default 250 ms) and lists every ref every 5 s, and
with N servers on one metadata store each moved head is derived N times.
Use index leases instead (ADR 0058):

| Mode (`--indexing`, `LEEP_INDEXING`) | Warms | Queries |
|---|---|---|
| `local` (default) | every group, on this server | derive what they miss, as before |
| `lease` | the groups whose lease it holds (`--index-kinds`, default all) | bounded while a lease lives |
| `off` | nothing | bounded while a lease lives |

- **Leases** are rows of the `leases` table in scope `index`, one per group
  (`history-tree-v4`, `log-filter-v1`, `tree-trigrams-v1`). Candidates take a
  free lease on their 3 s renewal tick, never a live one, and release on
  shutdown; a killed holder's lease is taken 6 s after its last renewal.
  They need Postgres or SQLite (SQLite: one process, which holds every
  lease). `leep_index_lease_held` shows who holds what; the lease row's
  `address` is the holder's `--advertise-url`.
- **Bounded queries.** In `lease` and `off` modes, while a lease lives, path
  logs, last-change, blame, filtered logs and search never derive. They scan
  what is missing up to `--query-scan-budget` (1,024 changesets),
  `--blame-scan-budget` (128 first-parent steps), `--search-scan-budget`
  (512 files) and `--search-scan-bytes` (16 MiB), with the same answers as
  a fully indexed store. Further behind, clients sending `Leep-Indexing:
  retry` (the CLI) get 503 with `Retry-After`, `stale-ok` (MCP) an older
  answer, and requests without the header wait up to `--index-wait` (10 s,
  at most `--index-waiters` of them at once, 64), then scan `--legacy-scan-factor`
  (8) times the budgets, then get the 503. With no live lease for a group,
  queries derive inline (`leep_query_inline_derive_total`).
- **Rollout.** Run `leep-server upgrade-index --yes` (it backfills
  `last_merge-v1` too), switch the servers that should build indexes to
  `lease`, then the query-only servers to `off`. Servers still in `local`
  keep warming meanwhile, which costs duplicate work only. To back out,
  return to `local`; leases expire in 15 s.
- **Balancers** must not count a 503 that carries a `Leep-Indexing` or a
  `Leep-Refused` header against a backend or retry it elsewhere: a balancer
  that pulled a shedding server out of rotation would push its load onto the
  others, which then shed too. See
  [RUNBOOKS.md](RUNBOOKS.md#leepindexinganswershigh) and
  [RUNBOOKS.md](RUNBOOKS.md#leepreadsshedding).
- **Tools that delete or rebuild rows.** `repair drop`, `repair rederive
  --missing` and `upgrade-index --yes` take the index leases of the groups
  they touch, refuse when a live server holds one (stop it, set it to `off`,
  or pass `--ignore-lease`, which allows duplicate work and is refused for
  `repair drop` and `upgrade-index --drop-first`, where it would allow the
  race the fence prevents), wait
  `--index-fence` (60 s; the 6 s stale time plus more than the longest
  staged write) before touching rows, so a former holder's late write lands
  first. They keep the leases while they run, so no server warms meanwhile,
  and a drop checks them once more right before it deletes.
  Servers in `local` mode take no leases: a drop of a current kind still
  needs them stopped, or not deriving that kind.

## Bulk loads and planner statistics (Postgres)

Postgres plans from statistics that autovacuum refreshes after a share of a
table changes. Migration 2 lowers that share for `derived` (1% plus 10,000
rows) and `changesets` (2% plus 10,000), and `leep-git import` and
`leep-server restore` run `ANALYZE` on the metadata tables when they finish
(ADR 0042). After loading by any other means (`pg_restore`, bulk SQL, copying
rows between databases), run `ANALYZE changesets, refs, derived, ref_log;`
before serving traffic or deriving indexes. A sign of stale statistics is
Postgres busy with few requests in flight while `pg_stat_user_tables` shows
`n_mod_since_analyze` close to `n_live_tup` for `derived`.

## Blob garbage collection

Abandoned uploads and failed commits leave unreferenced blobs. `leep-server
gc` reclaims them while servers keep running (ADR 0008, ADR 0012). It uses the
same `--meta` and `--blobs` as the server, talks to the stores directly, and
needs no downtime.

```sh
leep-server gc --dry-run                     # what would be condemned; writes nothing
leep-server gc                               # condemn, wait --fence, collect
leep-server gc condemn                       # phase one only
leep-server gc collect --fence 15m           # phase two only
leep-server gc --full                        # walk all of history, not only what is new
```

How it works: **condemn** marks every unreferenced blob older than `--grace`
(default 24h); a condemned blob is reported missing to clients, which upload it
again if they need it. After `--fence` (default 15m) **collect** marks again and
deletes the blobs that are still condemned. Collect acts only on condemnations
at least `--fence` old, so the two phases can run as separate cron jobs or
processes. A crashed run is safe: rerun it.

The fence must exceed the servers' `--request-timeout` plus, on Postgres, their
`--meta-statement-timeout` (default the request timeout plus 5 s) plus clock
skew (ADR 0012): with the defaults, 5m + 5m 5s + skew, under 15m. `gc` checks
the `--request-timeout` and `--meta-statement-timeout` it is given (or their
`LEEP_` variables, so run it with the servers' environment) and logs a WARN
when the fence is too short. If you raise `--request-timeout`, raise
`--fence` with it, or set a short `--meta-statement-timeout` such as `30s`.

Marking is incremental (ADR 0033): each run records what it marked in
`gc-mark-v1` rows and the next one walks only changesets stored since, so a
run costs a listing of changesets and blobs plus the new history, not a walk
of all of it. The first run after an upgrade walks everything once. The
report's `changesets walked` and `referenced blobs and trees marked by this
run` show the walk; `referenced blobs marked by earlier runs` the rest.
Moving, deleting or recreating refs and tags never needs a full walk, because
GC keeps whatever any stored changeset references. `--full` ignores the record,
walks everything and rewrites it. It is never needed for safety; it costs a
walk of all of history (about 10 s per phase per 20,000 commits on Postgres,
see `docs/PERFORMANCE.md`), and because it reads every tree node again it also
fails loudly on a tree node lost from the store, which `verify` reports too.
`repair drop` refuses `gc-mark-v1`: dropping part of it could let a collect
delete a referenced blob. On a loose filesystem store the blob
listing then dominates a run (about 40 us per loose object); `repack` makes it
about 7 us per object.

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

Run one GC at a time. A cron job such as `17 3 * * *  leep-server gc` works
(see [Packing blobs](#packing-blobs) for the `repack` line beside it); to
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

### Sizing what unreachable changesets cost

Changesets are never deleted, so originals of rebased lands, abandoned work
and `leep sync` snapshots stay forever. ADR 0059 (proposed) would collect
them; see [Changeset garbage collection](#changeset-garbage-collection). To
measure what it would reclaim on a store, run the read-only count:

```sh
nice -n 19 leep-server --meta-pool-size 4 --meta "$LEEP_META" --blobs "$LEEP_BLOBS" \
  gc --dry-run --unreachable --grace 0 --ref-log-retention 0
```

It writes nothing. Postgres sessions refuse writes
(`default_transaction_read_only`), and SQLite opens with `mode=ro`; a
`--meta` URL that already sets `options=` or `mode=` is refused. It
reports:

- the unreachable changesets by age and kind;
- the changesets and per-changeset rows older than `--grace`, with their
  logical bytes;
- the tree nodes and contents only those changesets introduced;
- the store's logical sizes (rows and bytes per derived kind, changesets,
  ref log, refs);
- per land, side by side: write-path bytes, index bytes and reclaimable
  bytes. All are logical; the physical database size is printed apart and
  runs 2.5 to 3.5 times larger on Postgres.

A second column shows what keeping the ref log only `--ref-log-retention`
would add, for a later decision. Use `--grace 0` on a store whose lands
are all recent. Ages come from when the store first stored each changeset
(`changesets.stored_at`, schema v7), never from the client's timestamp; a
changeset stored before migration 0007 counts as stored when it was
applied. The count needs a v7 schema: run `leep-server migrate` first.

Tree counts are an upper bound. `--exact-trees` makes them exact by walking
every reachable tree, which costs as much as `gc --full`. **Do not use it
on a small host beside live servers, such as the 2-core VPS.** Memory is
about 60 bytes per changeset, plus one hash per live object with
`--exact-trees`.

## Changeset garbage collection

ADR 0059 will delete changesets that nothing reaches (refs, every ref log
entry, the landed-copy scan window of every head, attestations, git mirror
maps, and a grace period) with the condemn, fence and collect protocol
blob GC uses. **Phase 1 is built: nothing condemns or deletes yet.** It is
what every server must run before a sweep may run beside it. Deleting
(`MetaStore::delete_changesets`) and the sweep itself come with phase 2.
Phase 1 is:

- **Store times.** Migration 0007 adds `changesets.stored_at`, written on
  every new changeset; sweeps and the dry run age changesets by it. It is
  a catalog-only `ADD COLUMN` on both backends and tolerates servers of
  the previous version, which leave it NULL (read as the migration's
  time).
- **Rescues.** Before a server stores anything that names an existing
  changeset (a child, a land, a rebase or merge, a ref move including
  `leep undo`, a signature or review), and `leep-git export` before it
  writes a mirror map, it checks whether a sweep condemned that changeset,
  and if so rescues it and its condemned ancestors
  (`leep_gc_changesets_rescued_total`). The check reads one row
  (`gc-fence-active-v1`) at most every 10 s per server while no sweep runs.
- **Land batches** read that row on every attempt, and give up with a
  retryable 409 after 60 s (`leep_land_batch_deadline_total`), which bounds
  how long after its check a batch can still move a ref.
- **Land ids outlive originals.** A keyed land retry whose changeset is no
  longer stored is answered from its land id record
  (`leep_land_answered_from_record_total{original="missing"}`).
- **Metrics and alert:** `leep_gc_changesets_rescued_total`,
  `leep_land_batch_deadline_total` and
  `leep_land_answered_from_record_total` (see [Metrics](#metrics)), and
  [LeepLandBatchDeadline](RUNBOOKS.md#leeplandbatchdeadline).
- `GET /v1/ready` lists `changeset-rescue`.

**Before the first condemnation** (phase 2), upgrade every server, importer
and tool that writes to the store to a release that lists
`changeset-rescue`: an older one neither rescues nor re-checks, exactly as
servers from before ADR 0012 did for blobs.

## Packing blobs

A blob store keeps every object whole in its own file (or bucket object)
until `leep-server repack` packs them (ADR 0029; for `s3://` stores,
ADR 0040). A pack stores each file version
as a zstd delta against the previous version at its path and each tree node
against the same directory one commit earlier, so a long history shrinks by
one to two orders of magnitude (the first 3,795 commits of git/git: 91.2 MB
loose, 4.2 MB packed with its index) and the store holds a handful of files
instead of one per object. New writes stay loose until the next repack.

```sh
leep-server repack                    # loose objects into a new pack; merge small packs (ADR 0039)
leep-server repack --full             # every live object into one pack
leep-server repack --level 19         # smaller and slower (zstd level 1..22, default 9)
leep-server repack --json             # the report as JSON
```

It uses the same `--meta` and `--blobs` as the server and runs beside running
servers. By default it is incremental. It walks back from every ref only as
far as the history whose objects are still loose, writes and publishes a new
pack holding the loose objects, and leaves existing packs alone, except that
it folds in small packs so the count stays low. It then waits `--fence`
(default 10m: longer than the servers' `--request-timeout` plus clock skew;
the statement term of `gc --fence` does not apply, since repack's fence covers
reads of the loose copies, not metadata writes) and deletes the loose copies and any
packs it merged. `--fence 0` is refused unless `--unsafe-no-fence` is given,
which is only valid with every server stopped. Filesystem blob stores
(`file://` or a path) and object stores (`s3://`) can be packed; a `mem://`
store is refused. Object stores are always packed in full for now (ADR 0040).

The merge rule (`--geometric 2 --max-packs 8`, the defaults): packs whose
sizes in objects already grow by a factor of 2 stay, and the smaller ones are
merged into the new pack, as `git repack --geometric` does. Then the smallest
packs are merged until at most `--max-packs` remain. A merge copies the
merged packs' compressed entries as they are, so it costs about a read of
those packs. The report's `packs_merged`, `frames_reused` and `packs_after`
show what it did.

`--full` walks every changeset, recompresses where a base changed, and
writes one pack of every live object. It is the only mode that drops objects
GC deleted from packs it would otherwise keep, and the only one that writes
the first new version of each file as a delta. An incremental run writes it
whole, because its base is in an older pack. A store kept only by
incremental runs is larger: 51% more pack bytes in the ADR 0039 measurement.
A full repack reuses unchanged entries too, but its walk is linear in the
history: 15 s at 12,000 commits of a synthetic history, against about 0.4 s
for an incremental run over 100 new commits.

The report counts loose objects and packs before, the new pack's size, how
many objects were stored as deltas and how many whole, how many were copied
from merged packs without recompressing, objects dropped because GC had
deleted them, and what is left loose (objects uploaded during the repack). It
also gives the time spent walking, writing, fencing and cleaning up. Only one
repack runs at a time per store (a second one exits with "another repack is
running"). Killing it is safe at any point: the store stays readable, and
the next run deletes the leftovers and finishes.

When to run it: after a large import, then incremental runs often and a full
one rarely. The incremental run's cost follows the new history, so daily or
hourly both suit. Run `--full` weekly to monthly, depending on how much
GC deletes and how much the pack bytes grow between full runs. Run both after
`gc`, so deleted objects are dropped:

```cron
# 03:17 daily: collect garbage; 04:37 daily: incremental repack; Sunday 05:07: full repack
17 3 * * *  leep-server gc
37 4 * * *  leep-server repack
7 5 * * 0   leep-server repack --full
```
The daily and weekly jobs may collide; the second one exits with "another
repack is running" and the next day's run picks up.

GC and repack may overlap; a blob GC deletes during a repack stays deleted.
GC deletes a packed blob by appending a line to `packs/tombstones`. A repack
leaves those blobs out of every pack it writes, and drops the lines of the
packs it replaces. Reads of packed blobs decode a delta chain (at most 32
entries), which the store's cache of decoded objects and the server's blob
cache absorb; [PERFORMANCE.md](PERFORMANCE.md) has the measured latencies.
Backups copy packs as files into the backup pool (ADR 0039; see "Backup and
restore"), so restoring a packed store gives a packed store.

Do not delete anything under `packs/` by hand, and do not delete
`.repack-*` files in the shard directories: each is a whole object that the
next repack puts back. `.tmp-*` files under `packs/` belong to a running
repack, or are leftovers the next repack deletes.

### Packing an object store (`s3://`)

The same command and flags. Differences from a filesystem store:

- **Local disk.** The repack downloads the packs it replaces and writes the
  new pack to a temp file before uploading it, so the machine running it
  needs free space in `TMPDIR` of about twice the packed size (plus the new
  pack). A full repack reads every loose object once (one GET each, 32 at a
  time) and every pack once.
- **Conditional writes.** One repack at a time is enforced with a lease
  object, `packs/repack.lease`, created with `If-None-Match` and renewed
  with `If-Match`. AWS S3, MinIO, Cloudflare R2 and GCS support these; a
  bucket that does not makes `repack` exit with a storage error before it
  writes anything. Reads do not need them.
- **A killed repack holds the lease for up to 10 minutes.** A second repack
  in that time exits with "another repack is running ... until unix time
  T". Wait, or, if you are sure no repack is running, delete
  `packs/repack.lease`. Everything else a kill leaves is cleaned by the next
  runs: a `.pack` with no `.idx` is deleted once it is older than the lease
  TTL.
- **Incomplete multipart uploads** from a killed repack are invisible but
  billed. Give the bucket a lifecycle rule that aborts incomplete multipart
  uploads after a day (`AbortIncompleteMultipartUpload`, `DaysAfterInitiation:
  1`).
- **Tombstones** are empty objects under `packs/tombstones/<pack id>/`, one
  per blob GC deleted from a pack; the next full repack removes them with
  their pack. Do not delete them by hand.
- **Readers** fetch each pack's index once and keep it in memory. A server
  notices a pack another process published on its next miss, listing
  `packs/` at most every 5 s; the repack's fence (minutes) keeps every object
  readable until then. A packed read costs one GET for the loose miss plus
  one ranged GET in the common case; `contains` of a packed blob costs three
  HEADs. ADR 0040 has the full request costs.

Backups (`leep-server backup`) read a packed object store like an unpacked
one.

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
| (note) `log-filter-v1 merge row(s) hold exactly what the merge brings in` | Not a problem. Past the side-walk budget (200,000 changesets) this version stores a superset in a merge's `log-filter-v1` row, and rows written before the budget (ADR 0047's code) hold the exact set. Both are valid (ADR 0051). `verify` walks such a merge again without the budget and accepts the row when it is that exact set. A row a build with a different budget wrote is not recognised and is reported as `derived_mismatch` | Nothing. `repair rederive --kind log-filter-v1` would rewrite those rows as this version writes them, which changes no answer |
| `derived_unverifiable` | A row could not be recomputed because something it reads is damaged | Fix the damaged tree node, blob or parent, then verify again |
| `corrupt_history_node`, `missing_history_node` | A part of a history pack fails its checksum or does not decode, a `history-node-v4` row holds another part than its key says, a split node's chunk does not fit its list, a patch does not fit its keyframe, or a `history-tree-v4` row, chunk list or patch points at nothing | `repair rederive --kind history-tree-v4` rewrites the packs. Or `repair drop --kind history-tree-v4` and let the index rebuild on the next path query (slow on a long history: it derives the whole index at once) |
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
leep-server repair rederive --kind history-tree-v4 --missing --jobs 16   # build an index after an import; resumable
leep-server repair drop --kind blame blob-trigrams-v1     # delete rows; they derive again when asked for
leep-server repair drop --kind history-tree-v4 --dry-run  # also removes history-node-v4; counts only
```

`rederive` walks the chosen changesets parents first, recomputes each
selected kind and every kind it depends on (so a skiplist is never rebuilt
from a damaged generation), and writes rows that are missing or wrong.
Changesets whose parents are done are recomputed side by side, `--jobs` at a
time (default the cores, up to 8; ADR 0050); `--missing` only fills absent
rows (see [Indexing a large history](#indexing-a-large-history)). It reports per
kind how many it examined, how many were already right, how many it wrote, and
how many it could not recompute. Rows no query ever asked for are written too:
for the lazily derived kinds that is eager derivation, which is also how to
warm an index after a restore. It is safe to repeat and to run beside servers;
the servers write the same bytes (for `history-tree-v4`, the same packs:
their compressed bytes may differ between zstd versions, and `verify` and
`repair` compare them uncompressed). `root_tree` and `history-tree-v4` also
rewrite the tree nodes and history pack parts they derive, which is what
restores a missing or damaged one. The search kinds (`blob-trigrams-v1`,
`tree-trigrams-v1`) are rewritten for the rows that exist; use
`derive-search REV` to build rows that do not.

`drop` removes every row of a kind and the kind then derives again on demand.
A kind that is a large share of `derived` is deleted by walking the table in
storage order, a range of pages per transaction of under a second; a small one
in key-order batches of 10,000 (ADR 0052). The kinds of one command go in one
walk, with a progress line (rows, rate, position, ETA) every ten seconds, and
`--dry-run` counts rows from the index without reading values. It is safe
beside servers: rows of other kinds are untouched, and a row of the dropped
kind that a server writes while it runs may survive, as one written just after
it would. A stopped `drop` is finished by running it again. It changes no answer, only latency: the next query that needs a row
derives it, and for `generation`, `skiplist`, `root_tree` and `history-tree-v4` on a
long history the first query derives the whole history. Warm them with
`repair rederive` instead of waiting for a user. Dropping `history-tree-v4` also
drops `history-node-v4`, because only it refers to those rows. `drop` also
accepts the retired `history-tree-v1`, `history-tree-v2` and `history-tree-v3`
(with `history-node-v1`, `history-node-v2` and `history-node-v3`), which
current servers no longer use; see [Upgrading](#upgrading).

Kinds that cannot be rederived (and so are refused by `drop` with an
explanation and exit status 2): `signature-v1` and `review-v1` (clients wrote
them), `rebased-origin-v1` (written by rebase; `land`'s retry detection depends
on it), `blob-claim-v1`, `blob-condemned-v1`, `blob-rescue-v1`, `blob-doom-v1`, `gc-mark-v1` (the GC
fence, ADR 0012), `history-node-v4` (or `-v3`, `-v2`, `-v1`) on its own, and the ref log. The full table
with reasons is in ADR 0025. `blame` can be dropped but not rederived: its rows
are keyed by a hash of (changeset, path) and cannot be listed.

**Restart servers after a repair of the search kinds.** A running server
keeps the `tree-trigrams-v1` rows its searches read in memory (see
[Connection pool and caches](#connection-pool-and-caches)). `repair rederive`
overwrites a damaged row in place and `repair drop --kind tree-trigrams-v1`
deletes them, but both run in their own process, so a server that cached a
damaged row before the repair keeps using it until it restarts. Rows are keyed
by content, so a correct row is never stale; only a damaged one outlives the
repair this way. After repairing or dropping `tree-trigrams-v1`, restart each
`leep-server` (a rolling restart is enough).

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
| Metadata: changesets and their compression dictionaries, refs, ref log, signatures, reviews, derived data | SQLite file or Postgres database (`--meta`) | `leep-server backup` |
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
- **Packed sources** (ADR 0039). When the source and the pool both keep
  packs (filesystem or `s3://`), `backup` copies each new pack and its
  index as files, checked against the hash the index records, then copies
  the loose objects one by one as before. The pool stays about the size of
  the packed source, and an unchanged store copies nothing. A pool pack that
  the source merged away is deleted once other pool packs hold every object
  it held (`packs_pruned` in the manifest). `restore` copies the packs back
  first, so a packed store restores packed. Pools from before this change
  (all loose) restore as they always did; later backups add packs beside
  their loose objects.
- **Retention.** A snapshot directory is self-contained apart from the pool, so
  deleting old directories under `snapshots/` is safe. The pool is shared and
  its set of objects only grows: to reclaim space, start a new pool (`--out` a new directory) every
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

`backup --metrics-file` writes node_exporter text-file metrics (the file must be
named `*.prom` in the directory node_exporter's textfile collector reads). The
rules are in [Suggested alerts](#suggested-alerts), each with a runbook:

| Alert | Fires when | Runbook |
|---|---|---|
| LeepBackupStale | `leep_backup_last_success_timestamp_seconds` is older than twice your interval, or absent (never succeeded, or the metric is not collected). A run that exits 2 writes nothing, so this is how it shows | [LeepBackupStale](RUNBOOKS.md#leepbackupstale) |
| LeepBackupCheckFailed | `leep_backup_last_run_ok == 0`: a sealed snapshot is not restorable (or the run used `--no-check`) | [LeepBackupCheckFailed](RUNBOOKS.md#leepbackupcheckfailed) |
| LeepBackupSlow | `leep_backup_duration_seconds` averages over 1.5 times last week's and over 10 minutes | [LeepBackupSlow](RUNBOOKS.md#leepbackupslow) |
| LeepBackupVerifyFailed | the weekly `backup-verify` exited non-zero or has not passed in 9 days (cron wrapper metric) | [LeepBackupVerifyFailed](RUNBOOKS.md#leepbackupverifyfailed) |
| LeepRestoreDrillOverdue | no passing drill in 35 days (cron wrapper metric) | [LeepRestoreDrillOverdue](RUNBOOKS.md#leeprestoredrilloverdue) |

Leep exports no metric for `backup-verify`, the drill or `gc`; the
[cron wrapper](RUNBOOKS.md#cron-wrappers) turns a job's exit status into
metrics for them (and [LeepGcStale](RUNBOOKS.md#leepgcstale)). Alert on free
space where `--out` lives too ([LeepDiskSpaceLow](RUNBOOKS.md#leepdiskspacelow)).
The backup metrics (`last_run_timestamp`, `snapshot_timestamp` (the recovery
point), `duration`, `check_problems`, `changesets`, `blobs_copied`,
`blob_bytes_copied`) are listed in ADR 0026.

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

**Repack** (ADR 0029, ADR 0039). `cargo test -p leep-store pack_tests` aborts
a full and an incremental repack at every step
(`a_repack_killed_at_any_step_leaves_a_readable_store`,
`an_incremental_repack_killed_at_any_step_leaves_a_readable_store`) and
checks every object still reads before and after the next repack;
`cargo test -p leep-e2e --test repack` SIGKILLs the real `leep-server repack`
while it writes, after it publishes and during its cleanup, and checks that
log, blame, search and show answer the same and `verify --full --rehash` is
clean. Run both after changing the pack format, the filesystem store or GC.
`cargo test -p leep-store objpack_tests` does the same for object stores
(ADR 0040) over the in-memory and local-filesystem `object_store` backends:
the kill sweep, GC deletes and a rescue racing the cleanup, and readers
holding indexes of deleted packs; `cargo test -p leep-server --test
repack_object` runs GC against a packed object store. Run them after
changing the object store backend.

**Git mirror** (ADR 0046). `cargo test -p leep-git --test mirror` drives
the mirror against in-memory stores and local bare repositories.
`cargo test -p leep-e2e --test mirror` runs the real binaries:
- follow and import, including the diverge-and-merge flow;
- a seeded SIGKILL sweep over export, import and diverged rounds
  (`LEEP_MIRROR_KILL_ROUNDS`, default 12, and `LEEP_MIRROR_SEED`);
- an abort at each named step (`LEEP_MIRROR_CRASH_AT`): mid-export, after
  the export, after a push whose answer is lost, after an import, after its
  land, and after a diverged push;
- with `--on-divergence merge --land-branches` (ADR 0049): a clean merge, a
  conflicting divergence reported, land branches landing and conflicting, a
  kill sweep that also pushes land branches, and an abort after the
  remote's side is imported, after the merge, after it lands, after its
  push, and after a land branch is imported, landed and deleted;
- agents landing while a person pushes to git, in both modes; the merging
  mode converges with no merge in git.

After each test, every changeset on Leep's main has exactly one commit on
the remote, the remote's reflog shows only fast-forwards, and the trees
match. Run them after changing the mirror, the git bridge's commit format or
ref CAS.

**Leftovers after a real crash.** A killed filesystem blob store can leave
`.tmp-*` files in its shard directories. Nothing reads or lists them, and
nothing removes them; delete those older than the request timeout if the
space matters:
`find /var/lib/leep/blobs -path '*/packs' -prune -o -name '.tmp-*' -mmin +60 -delete`.
A killed repack leaves `.tmp-*` files in `packs/` and `.repack-*` files in the
shard directories; leave them, the next repack deletes the first and puts the
second (whole objects) back.

## Soak tests

`leep-soak` (ADR 0022) runs a swarm of simulated agents against a real
`leep-server` for as long as you ask, checks invariants all the way through,
and records performance and resource trends. Run it before a release, after
changing `land`, GC, derived data, a store backend or the request limits, and
for a long time when hardening. CI runs it weekly for 30 minutes
(`.github/workflows/soak.yml`, also on demand from the Actions tab), and
`cargo test` runs a ten-second version (`leep-e2e`, `soak_smoke`) so it keeps
working. For load rather than endurance, `leep-swarm` drives thousands of
simulated agents with think time, reads and hot files against servers you
start yourself, and checks history for double lands at the end
(PERFORMANCE.md, "Many-agent workload").

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
release flow need a server the harness started). If that server's `main`
already holds a history (an imported git repository, say), add
`--seed-on-existing`: the synthetic files land on top of it under `src/`,
`docs/` and `assets/`, so the swarm measures lands, reads and GC at that
history's depth.

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

**Size `--think` to the machine.** Lands to one ref are replayed one after
another in the server (in batches, ADR 0032), so a ref takes only as many lands
per second as one replay costs. On the
4 vCPU machine of `docs/PERFORMANCE.md` that was about 6 per second with p99 near a
second (measured before batching), which 50 agents with the default 6 s think time offer. Agents that offer
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
