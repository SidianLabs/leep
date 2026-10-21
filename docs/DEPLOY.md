# Deploying Leep

Authentication is **off unless you give the server a tokens file**, and
`leep-server` never terminates TLS. For anything beyond localhost, enable
tokens (below) and put a TLS-terminating reverse proxy in front: bearer
tokens must not cross untrusted networks in cleartext. Without a tokens file
the server refuses to listen on anything but a loopback address unless
`LEEP_INSECURE_NO_AUTH=1` (`--insecure-no-auth`) is set. The image listens on
`0.0.0.0:7070`, so the local examples below set it and publish the port on the
host's loopback only.

The repo `Dockerfile` builds `leep-server`, `leep` and `leep-git` into one
`debian:bookworm-slim` image. The entrypoint is `leep-server`; it runs as the
non-root user `leep` (uid 10001) and keeps state under `/var/lib/leep`.
The image also carries `git` and the ssh client, so the same image can run
the git mirror with `--entrypoint leep-git` and the arguments
`mirror run ...` ([OPERATIONS.md](OPERATIONS.md#git-mirror)).

```sh
docker build -t leep:dev .
```

## Single container (SQLite + files)

```sh
docker run -d --name leep -p 127.0.0.1:7070:7070 \
  -e LEEP_INSECURE_NO_AUTH=1 -v leep-data:/var/lib/leep leep:dev
curl -s localhost:7070/v1/health      # "ok": the process is up
curl -s localhost:7070/v1/ready       # {"status":"ready","features":["land-id","indexing-retry","changeset-rescue","land-clean-blobs"]}: it can reach its stores
```

Drive it with the CLI from the host or from the same image:

```sh
export LEEP_SERVER=http://127.0.0.1:7070
leep init && leep log
docker run --rm --network host -e LEEP_SERVER --entrypoint leep leep:dev log
```

## Compose stack (Postgres + blob volume)

`deploy/docker-compose.yml` runs Postgres 16 for metadata and `leep-server`
with blobs on a named volume, published on `127.0.0.1:7070` only.

```sh
docker compose -f deploy/docker-compose.yml up -d --build
docker compose -f deploy/docker-compose.yml down       # keep data
docker compose -f deploy/docker-compose.yml down -v    # delete data
```

The database password in that file is a dev-only placeholder. Change it, and
move it out of the file (for example into an `.env` file or your secret
manager), before any shared use.

### Blobs in S3 or an S3-compatible store

To keep blobs in object storage instead of the volume, override `LEEP_BLOBS`
and the standard AWS credential variables on `leep-server`, for example in a
`docker-compose.override.yml`:

```yaml
services:
  leep-server:
    environment:
      LEEP_BLOBS: s3://my-bucket?region=eu-west-1
      # S3-compatible stores (Ceph RGW, Garage, SeaweedFS, ...):
      # LEEP_BLOBS: s3://leep?endpoint=http://s3.internal:9000&region=us-east-1&allow_http=true
      AWS_ACCESS_KEY_ID: ...
      AWS_SECRET_ACCESS_KEY: ...
```

The bucket must already exist. The stack does not bundle an object store,
so it depends only on the `postgres` image.

The image has no `curl`, so it declares no `HEALTHCHECK`, and the compose
file has no healthcheck for `leep-server`. Probe it from outside instead
(see [Probes](#probes)).

## Probes

Two endpoints, both public (no token, never rate-limited, whatever the tokens
file says):

| Endpoint | Question | Checks | Answer |
|---|---|---|---|
| `GET /v1/health` | Is the process alive? (liveness) | nothing but that HTTP is served | 200 `"ok"` |
| `GET /v1/ready` | Can it serve right now? (readiness) | reads one ref from the metadata store and asks the blob store about one fixed hash, concurrently, each under `--ready-timeout` (default 2 s) | 200 `{"status":"ready","features":["land-id","indexing-retry","changeset-rescue","land-clean-blobs"]}`, or 503 `{"status":"not ready","failed":[{"store":"meta","reason":"unavailable"}],"features":["land-id","indexing-retry","changeset-rescue","land-clean-blobs"]}` (`features`: optional behaviours clients may check for, ADR 0057) |

`store` is `meta` or `blobs`; `reason` is `unavailable` or `timed out after
2000ms`, never the driver's message (it can carry a host name; the server logs
it as `readiness check failed`). Restart on liveness, and take a server out of
rotation on readiness. Do not use `/v1/ready` as a liveness probe: when a
shared database blips, every server would be restarted at once for no gain.
A check does two small store reads, and its answer is reused for one second,
so however many callers probe the public endpoint a server checks its stores
at most once a second; an outage or a recovery shows up to a second late. The checks are counted in
`leep_ready_checks_total{store,result}`, and the
[LeepNotReady](RUNBOOKS.md#leepnotready) alert fires on failures.

Kubernetes:

```yaml
livenessProbe:
  httpGet: {path: /v1/health, port: 7070}
  periodSeconds: 10
  failureThreshold: 3
readinessProbe:
  httpGet: {path: /v1/ready, port: 7070}
  periodSeconds: 5
  timeoutSeconds: 3        # above --ready-timeout, so the 503 body arrives
  failureThreshold: 2
startupProbe:              # optional: allow a slow first migration
  httpGet: {path: /v1/ready, port: 7070}
  periodSeconds: 5
  failureThreshold: 60
```

Docker or compose: the image has no `curl` or `wget`, so a `healthcheck:` needs
a command that exists in it or one you add (for example `curl -fsS
http://127.0.0.1:7070/v1/ready` after installing curl in a derived image).
Without one, probe from the host, a sidecar or your load balancer.

## Authentication

Generate tokens with the server binary and mount a tokens file holding only
their digests (see ADR 0007):

```sh
docker run --rm --entrypoint leep-server leep:dev gen-token --name ci --role write
# token: <shown once>          entry: {"name":"ci","role":"write","hash":"blake3:..."}
echo '{"tokens":[<entries, comma separated>]}' > tokens.json
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/tokens.json:/etc/leep/tokens.json:ro" \
  -e LEEP_TOKENS_FILE=/etc/leep/tokens.json leep:dev
```

In compose, add the same volume and `LEEP_TOKENS_FILE` to `leep-server`.
An entry may carry `"expires": "2027-01-31T00:00:00Z"` (an RFC 3339 time with
an offset; `gen-token --expires-in 30d` writes one), after which the token gets
401 with code `token_expired` (`token expired at ...`). Give agent tokens an expiry so a forgotten one ends
by itself, and rotate the ones still needed before then: the
`LeepTokensExpiringSoon` alert fires 7 days ahead
([OPERATIONS.md](OPERATIONS.md#metrics)). Changing `expires` takes effect on
SIGHUP like any other edit; a request admitted before the time passes finishes.
An entry may also carry `"author": "Name <email>"` (or `gen-token --author`).
When a client names no author itself (no `--author`, `LEEP_AUTHOR` or login
name, as in most containers), `leep` stamps that on its changesets, falling
back to the token's `name`. It is a default, not enforcement: a client that
sets `LEEP_AUTHOR` still wins.
Clients pass the token with `LEEP_TOKEN`, `leep --token` or
`leep init --token`. `GET /v1/health` and `GET /v1/ready` stay public for probes. The file is
re-read on SIGHUP, so tokens can be rotated and revoked without a restart
([Reloading config](#reloading-config)).

## Request limits

Optional, and off unless set (ADR 0020). One runaway agent should not starve
the others, so the server can limit each token's request rate, requests in
flight and body size. Server-wide defaults apply to every token:

```sh
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/tokens.json:/etc/leep/tokens.json:ro" \
  -e LEEP_TOKENS_FILE=/etc/leep/tokens.json \
  -e LEEP_RATE_LIMIT=20 -e LEEP_RATE_BURST=40 \
  -e LEEP_MAX_CONCURRENT_PER_TOKEN=8 \
  -e LEEP_READ_MAX_BODY_BYTES=1048576 leep:dev
```

and any entry in the tokens file can override them, or opt out with
`"unlimited"`:

```json
{ "tokens": [
  { "name": "agent-7", "role": "write", "hash": "blake3:...", "rate": 5, "burst": 10 },
  { "name": "lander",  "role": "write", "hash": "blake3:...", "rate": "unlimited", "concurrency": 32 },
  { "name": "viewer",  "role": "read",  "hash": "blake3:...", "max_body": 4096 }
] }
```

| Setting | Flag / variable | Token field | Meaning |
|---|---|---|---|
| Rate | `--rate-limit` / `LEEP_RATE_LIMIT` | `rate` | Requests per second, as a decimal (`0.5` is one per two seconds). Reads and writes have separate buckets with these parameters. |
| Burst | `--rate-burst` / `LEEP_RATE_BURST` | `burst` | Requests allowed at once; default is the rate rounded up. Needs a rate. |
| Concurrency | `--max-concurrent-per-token` / `LEEP_MAX_CONCURRENT_PER_TOKEN` | `concurrency` | Requests in flight, reads and writes together. More are refused, not queued. |
| Body size | `--read-max-body-bytes` / `LEEP_READ_MAX_BODY_BYTES` (read tokens only; default 1 MiB) | `max_body` | Largest request body in bytes. `--max-body-bytes` still caps everyone. |

A token that sets `rate` but not `burst` uses `--rate-burst` if given, else its
rate rounded up. Bad values (zero, negative, a burst without a rate, a read body
limit without a tokens file) stop the server at startup with a message naming
the token or flag, and make a reload fail. Per-token fields reload on SIGHUP; the
flags need a restart. A request over the rate or concurrency limit gets `429` with
`Retry-After`; a body over the size limit gets `413`; both have a JSON error
body. `GET /v1/health` and `GET /v1/ready` are never limited. The `leep` CLI retries a throttled
`GET` a few times and then prints the server's message. Tuning and metrics are
in [OPERATIONS.md](OPERATIONS.md#request-limits).

### Body limits

These are always on, tokens file or not:

| Flag / variable | Default | Meaning |
|---|---|---|
| `--max-blob-bytes` / `LEEP_MAX_BLOB_BYTES` | 256 MiB | Largest blob upload (`PUT /v1/blobs/{hash}`): the largest file a commit can contain. |
| `--max-json-body-bytes` / `LEEP_MAX_JSON_BODY_BYTES` | 32 MiB | Largest body of every other request. The largest is a changeset: about 150 bytes per changed path, so some 200,000 paths. |
| `--max-inflight-bytes` / `LEEP_MAX_INFLIGHT_BYTES` | 1 GiB | Body bytes all requests in flight may hold together; a request that would go over gets `503` with `Retry-After: 1`. With a tokens file, one token may hold at most a quarter of it (or one largest request, if that is more); beyond that it gets `429`. |
| `--max-body-bytes` / `LEEP_MAX_BODY_BYTES` | 512 MiB | Ceiling on every body; the caps above apply first. |

A body over its cap gets `413` with a JSON error naming the flag; `leep` prints
it with a hint and does not retry. A blob upload refused with `503` for a spent
budget is sent again by `leep` up to three times in all, waiting the
`Retry-After` (at most 10 seconds altogether). **Upgrading from a version without these
limits:** blob uploads used to be capped only by `--max-body-bytes` (512 MiB),
so a file between 256 MiB and 512 MiB that committed before is now refused, by
`leep commit` and by the git mirror importing a push. Raise `--max-blob-bytes`
(and `--max-inflight-bytes`, which must be at least as large) if your
repositories hold such files. Bodies are held in memory while they are hashed
and stored, so size the budget with the container: memory use is about
`--cache-bytes` plus `--derive-cache-bytes` plus `--max-inflight-bytes` plus a
few hundred MB. `leep-git import` writes the stores directly and is not
limited. Tuning and the alert are in
[OPERATIONS.md](OPERATIONS.md#body-limits-and-the-in-flight-budget).

Without a tokens file the rate and concurrency defaults apply per client IP
address (IPv6 per /64). Behind a reverse proxy every request arrives from the
proxy's address, so those limits become one bucket shared by everybody; use
tokens when you need fairness between agents, and keep the proxy's own limits
as the first line against anonymous floods. Limit state is per server process,
so with N servers behind a balancer a token can use up to N times its rate.

## Protected refs

Protect the refs everyone builds on, such as `main`, even when you require
neither signatures nor review:

```sh
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/tokens.json:/etc/leep/tokens.json:ro" \
  -e LEEP_TOKENS_FILE=/etc/leep/tokens.json \
  -e LEEP_PROTECT_REFS=main,release leep:dev
```

`--protect-ref REF` (repeatable; `LEEP_PROTECT_REFS` is comma-separated) makes
`REF` move only forward and refuses to delete it: `land` works as before and
`POST /v1/refs/{name}` may fast-forward it (the git mirror's imports do), but a
rewind (a raw compare-and-swap, or `leep undo` of a land), a sideways move or a
delete (`leep branch -d`) is a 400. Any write token can still land on it; add `--require-signatures` or
`--require-review` for that. Refs listed there, or in a review policy's
`refs`, are protected the same way already, so `--protect-ref` is for refs
without such a policy. Without it, a write token could reset `main` to an old
changeset or delete it with one raw compare-and-swap.

The names are exact and checked at startup like the other policy flags: a name
that is not a valid ref name stops the server, and one with no ref yet is
logged as a warning. The flag is read once; changing it needs a restart.
`leep-git import` writes the stores directly and is not covered. To take a
change back on a protected ref, `leep revert` it and land the revert.

## Signed changesets

Optional. Authors sign changesets with Ed25519 keys; the server trusts only the
keys you list, and can refuse to land unsigned work on chosen refs (ADR 0010).

```sh
leep keygen --out ~/.config/leep/alice.key --name alice   # prints the keys-file entry
echo '{"keys":[<entries, comma separated>]}' > signing-keys.json
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/tokens.json:/etc/leep/tokens.json:ro" \
  -v "$PWD/signing-keys.json:/etc/leep/signing-keys.json:ro" \
  -e LEEP_TOKENS_FILE=/etc/leep/tokens.json \
  -e LEEP_SIGNING_KEYS_FILE=/etc/leep/signing-keys.json \
  -e LEEP_REQUIRE_SIGNATURES=main leep:dev
```

`--signing-keys-file` enables `POST /v1/changesets/{id}/signatures`, which
needs a write token when auth is on. `--require-signatures REF` (repeatable;
the environment variable is comma-separated) makes `land` onto `REF` fail
with 400 unless every changeset it adds is signed by a listed key, and needs
the keys file. The keys file is re-read on SIGHUP; the `--require-signatures`
refs are flags and need a restart ([Reloading config](#reloading-config)). Authors
commit with `leep commit --sign KEYFILE` or `LEEP_SIGNING_KEY=KEYFILE`; the key
file must be mode 0600. Raw ref updates (`POST /v1/refs/{name}`) are checked
the same way. `leep-git` import writes to the stores directly and is not
covered, so run it only as an operator. The git mirror (`leep-git mirror run
--import`) lands pushes from git through those raw ref updates, so it is
covered: an unsigned push to a protected ref is refused and reported
([OPERATIONS.md](OPERATIONS.md#git-mirror)).

## Review and owners

Optional, and it needs authentication: reviewers are the write-role tokens, so
`OWNERS` files name token names (ADR 0011). Put `OWNERS` files in the tree
(`alice`, one name per line, `#` comments, `noparent`), then protect refs:

```sh
cat > review-policy.json <<'JSON'
{ "refs": ["main"],
  "auto_approve": ["comment_only", "whitespace_only", "docs_only"],
  "docs_globs": ["**/*.md", "docs/**"] }
JSON
docker run -d -p 127.0.0.1:7070:7070 -v leep-data:/var/lib/leep \
  -v "$PWD/tokens.json:/etc/leep/tokens.json:ro" \
  -v "$PWD/review-policy.json:/etc/leep/review-policy.json:ro" \
  -e LEEP_TOKENS_FILE=/etc/leep/tokens.json \
  -e LEEP_REVIEW_POLICY_FILE=/etc/leep/review-policy.json leep:dev
```

`--review-policy-file` sets the auto-approval rules and lists `refs` that
require review; `--require-review REF` (repeatable; the environment variable is
comma-separated) adds more refs with the same rules. Both need the tokens file.
The policy file is re-read on SIGHUP, so its `refs` and rules change without a
restart; `--require-review` refs are flags and need one. `land` onto a protected ref fails with 400 unless
every changeset it adds is approved: each changed path needs an approval from
an owner of that path as listed in the ref's current head (any one approval
from anyone where no `OWNERS` file applies), or an enabled auto rule, and no
owner may have rejected it. Reviewers use `leep review approve|reject REV`;
`leep review status REV` shows what is missing. It composes with
`--require-signatures` (both must pass), and raw ref updates are checked too.
Review endpoints answer 400 on a server without a tokens file. Reviews are
looked up once per write token, so removing a token drops its reviews.

Every write token is a reviewer unless its entry says `"review": false`
(`gen-token --no-review` writes it). **Give agent tokens `"review": false`**,
and keep review for the tokens of people who own code: otherwise one agent can
approve another agent's change, or its own wherever no `OWNERS` file applies,
and the policy checks nothing. Such a token can still commit, land and
move refs like any write token; posting a review is a 403 (`token may not
review`), and reviews it posted before are ignored from the next request on
(the field reloads on SIGHUP).

## Reloading config

Send SIGHUP to the server to re-read the tokens file, the signing keys file and
the review policy file without a restart (ADR 0024):

```sh
kill -HUP "$(pidof leep-server)"        # a host
docker kill --signal=HUP leep           # a container
docker compose kill -s HUP leep-server  # compose
```

In the image the server is PID 1, so any way of delivering SIGHUP to the
container works. The slim image has no shell tooling you can rely on, so under
Kubernetes signal the container from the node (or with a tool your cluster
provides) rather than `kubectl exec ... kill`. Without SIGHUP, restart.

All configured files are read and validated first. If any is invalid, the whole
reload is refused, the server keeps its previous config and logs an `ERROR`
naming the file and the reason; otherwise the new config takes effect for the
next request and the log says what changed (counts and names, never tokens or
digests). A request already running finishes under the config it started with.
SIGHUP is the only trigger: there is no reload endpoint and no file watcher.

What a reload covers:

| Reloaded | Not reloaded (restart) |
|---|---|
| Tokens: added, revoked, rotated, role changes, `expires` | Listen and metrics addresses, stores, cache size |
| Per-token limits (`rate`, `burst`, `concurrency`, `max_body`) | `--request-timeout`, the body limits (`--max-body-bytes`, `--max-blob-bytes`, `--max-json-body-bytes`, `--max-inflight-bytes`), and the server-wide limit defaults |
| Trusted signing keys | `--require-signatures`, `--require-review` and `--protect-ref` refs |
| Review policy: `refs`, auto-approval rules, docs globs | Which files are configured at all (a server started without `--tokens-file` stays open) |

Write each file atomically (save beside it, then `mv` over it) so a reload never
reads half a file; a half-written file fails safely, but nothing changes until
the next SIGHUP. In containers, mount the *directory* holding the files: a
single-file bind mount keeps pointing at the old file after a rename. A
Kubernetes ConfigMap volume updates the files by itself after a short delay;
send SIGHUP after the change shows up.

Rotating a token without downtime: `gen-token`, add the new entry (or replace
the digest of the existing name), SIGHUP, roll the new token out to the agent,
then remove the old entry and SIGHUP again. To revoke at once, remove the entry
and SIGHUP; requests that were already admitted finish, up to
`--request-timeout`. Limiter state is kept for tokens whose limits did not
change, so a rotation does not give anyone a fresh rate-limit bucket. Rotating
signing keys works the same way: add the new key, SIGHUP, switch authors over,
then remove the old one. Removing a key makes changesets signed only by it
unsigned again, including for `land`.

With several servers behind a balancer, signal every instance and compare
`leep_config_info{fingerprint}` across them (the fingerprint hashes the loaded
config, so identical config gives the same value on every server whatever its
uptime; `leep_config_generation` is a per-process counter and is not
comparable between servers). See
[OPERATIONS.md](OPERATIONS.md#config-reload) for metrics and alerts, and
[LeepConfigReloadFailed](RUNBOOKS.md#leepconfigreloadfailed) for what to do when
a reload is refused.

## Environment variables

| Variable | Default in image | Meaning |
|---|---|---|
| `LEEP_LISTEN` | `0.0.0.0:7070` | Listen address. |
| `LEEP_META` | `sqlite:///var/lib/leep/meta.db?mode=rwc` | Metadata store: `sqlite:`, `postgres://`, `mem://`. |
| `LEEP_BLOBS` | `file:///var/lib/leep/blobs` | Blob store: `file://`, `s3://bucket?endpoint=URL&region=R&allow_http=true`, `mem://`. |
| `LEEP_TOKENS_FILE` | none | Tokens file; enables authentication. |
| `LEEP_INSECURE_NO_AUTH` | off | `1` lets the server run without a tokens file on a non-loopback address (the image's default listen address). Only for a port published on loopback or behind a proxy that authenticates. |
| `LEEP_SIGNING_KEYS_FILE` | none | Trusted author keys file; enables signature submission. |
| `LEEP_REQUIRE_SIGNATURES` | none | Comma-separated refs that only accept signed lands; needs the keys file. |
| `LEEP_REVIEW_POLICY_FILE` | none | Review policy JSON (auto-approval rules, refs that require review); needs the tokens file. |
| `LEEP_REQUIRE_REVIEW` | none | Comma-separated refs that only accept reviewed lands; needs the tokens file. |
| `LEEP_PROTECT_REFS` | none | Comma-separated refs that only move forward and cannot be deleted; see [Protected refs](#protected-refs). |
| `LEEP_SERVER` | none | CLI only: server URL. |
| `LEEP_SIGNING_KEY` | none | CLI only: private key file for `leep commit --sign`. |
| `LEEP_TOKEN` | none | CLI only: bearer token. |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY` | none | S3 credentials (standard AWS variables). |
| `LEEP_RATE_LIMIT`, `LEEP_RATE_BURST` | none | Per-token request rate and burst; see [Request limits](#request-limits). |
| `LEEP_MAX_CONCURRENT_PER_TOKEN` | none | Requests in flight per token. |
| `LEEP_READ_MAX_BODY_BYTES` | 1 MiB with a tokens file | Body size cap for read tokens; needs the tokens file. |
| `LEEP_MAX_BLOB_BYTES` | 256 MiB | Largest blob upload; see [Body limits](#body-limits). |
| `LEEP_MAX_JSON_BODY_BYTES` | 32 MiB | Largest body of any other request. |
| `LEEP_MAX_INFLIGHT_BYTES` | 1 GiB | Body bytes all requests in flight may hold together. |
| `LEEP_MAX_BODY_BYTES` | 512 MiB | Ceiling on every request body. |
| `LEEP_NO_MIGRATE` | off | `1` makes the server (and `gc`, `derive-search`) refuse to start while metadata schema migrations are pending instead of applying them; see [Upgrades](#upgrades). |
| `LEEP_META_STATEMENT_TIMEOUT` | `--request-timeout` + 5s | Bound on each statement the server sends to Postgres (`statement_timeout`), so a query whose request timed out does not run on in the database; `0` turns it off. Not applied to lease calls or migrations; SQLite ignores it. |
| `LEEP_REQUEST_TIMEOUT` | `5m` | Abort any request that runs longer (`90s`, `5m`; `0` disables). `leep-server gc --fence` must exceed it; see [OPERATIONS.md](OPERATIONS.md#blob-garbage-collection). |
| `LEEP_WARM_INTERVAL` | `250ms` | How often the background warmer reads the ref log for moved refs and derives their path-history, log-filter and search indexes (ADR 0028, ADR 0058); every ref is also listed every 5 s. Accepts `ms`. `0` turns it off. With several servers on one metadata store, set `0` on all but one (see OPERATIONS.md, "Indexing a large history"). |
| `LEEP_WARM_SEARCH` | `true` | Whether the warmer also derives the code-search index of each ref head. |
| `LEEP_DERIVE_JOBS` | the cores, up to 8 | Changesets the warmer's catch-up, or a query over un-derived history, derives at once (ADR 0050). Lower it to leave cores for requests while a large import is being indexed. |
| `LEEP_META_POOL_SIZE` | 16 | Most connections to a Postgres metadata store per process; keep the sum over every server and tool under `max_connections` (OPERATIONS.md, "Connection pool and caches"). |
| `LEEP_DERIVE_CACHE_BYTES` | 404 MiB | Budget of the in-memory derived-index caches (path history, trees, search's directory filters, filtered log); separate from `--cache-bytes`, the blob cache. |
| `LEEP_READY_TIMEOUT` | `2s` | Bound on each store check behind `GET /v1/ready` (`500ms`, `2s`; not zero). See [Probes](#probes). |
| `LEEP_METRICS_LISTEN` | off | Address for a separate, unauthenticated `GET /metrics` listener; keep it private. See [OPERATIONS.md](OPERATIONS.md). |
| `LEEP_LOG_FORMAT` | `text` | `text` or `json` (one object per line). |
| `RUST_LOG` | `info` | Log filter. |

`leep-server gc` takes `--meta`/`--blobs` (or their variables) plus `--grace`,
`--fence`, `--dry-run` and `--unsafe-no-fence`, all flags only; see
[OPERATIONS.md](OPERATIONS.md#blob-garbage-collection).

`leep-server migrate [--dry-run|--status]` applies or previews metadata schema
migrations (`--meta` or `LEEP_META`); see [Upgrades](#upgrades).

`--cache-bytes` and `--max-body-bytes` are flags only; pass them after the
image name, e.g. `docker run leep:dev --cache-bytes 0`.

## Persistence and backup

State lives in two places, plus your configuration:

- SQLite + files: metadata is `meta.db` and blobs are files, both under the
  `/var/lib/leep` volume.
- Postgres + blobs: metadata is the database, blobs are a volume or an S3
  bucket.
- Configuration the stores do not hold: the tokens file, the signing keys file
  and the review policy. Keep them with your other deployment configuration.

`leep-server backup` copies metadata and blobs consistently **while the server
runs**, and `leep-server restore` puts them back into empty stores (ADR 0026).
It reads the metadata in one transaction first, copies the blobs the backup
lacks second, then checks that every blob the snapshot needs was copied; no
downtime, no pause of GC. It uses the same `--meta`/`--blobs` (or
`LEEP_META`/`LEEP_BLOBS`) as the server.

```sh
# Single container: run it beside the server, mounting the same volume and a backup volume.
docker run --rm -v leep-data:/var/lib/leep -v /backups/leep:/backups leep:dev \
  backup --out /backups

# Compose stack (Postgres + blob volume): the compose file's leep-server service has the right LEEP_META and LEEP_BLOBS.
docker compose -f deploy/docker-compose.yml run --rm -v /backups/leep:/backups leep-server backup --out /backups
```

The image's `LEEP_META` and `LEEP_BLOBS` point at the volume in the single
container; in the compose stack they point at Postgres and the blob volume. The
container runs as the unprivileged user `leep`, so the backup directory must be
writable by it (or pass `--user`). A backup directory holds one shared blob pool and one snapshot per run
(`snapshots/<UTC time>/`), so repeating it is incremental.

Restore into fresh stores, then start the server on them:

```sh
docker run --rm -v leep-data-new:/var/lib/leep -v /backups/leep:/backups leep:dev \
  restore --from /backups
```

Postgres native tooling is still supported for the metadata half when it is
too large to stream: take `pg_dump -Fc` or base backups with WAL archiving, run
`leep-server backup --no-blobs` for the manifest, and prove the pair with
`leep-server backup-verify`. Blobs in S3 can be protected by bucket versioning
or replication instead of copying (`--no-blobs`); `backup-verify --blobs-from
URL` checks the copy.

Take a backup before every upgrade, schedule one daily, check it with
`backup-verify`, and run the restore drill on a schedule: an untested backup is
not a backup. The procedure, retention, RPO and RTO, the drill and the alerts
are in [OPERATIONS.md](OPERATIONS.md#backup-and-restore); what to do when a
backup alert fires is in [RUNBOOKS.md](RUNBOOKS.md#leepbackupstale).

## Upgrades

Metadata schema migrations (ADR 0023) run automatically when the server
connects to the metadata store, in one transaction under a lock, so several
servers starting together migrate once. Rebuild the image, then recreate the
container (`docker compose up -d --build`). Take a backup first
(`leep-server backup`, above): there is no
automatic downgrade, and a server refuses a database that a newer release has
migrated. Servers are stateless, so several can share one Postgres and bucket.

To migrate on your own schedule instead, set `LEEP_NO_MIGRATE=1` on the
servers (they refuse to start while anything is pending), then run, with the
new image and the same `LEEP_META`:

```sh
docker compose -f deploy/docker-compose.yml run --rm leep-server migrate --status   # what is pending
docker compose -f deploy/docker-compose.yml run --rm leep-server migrate --dry-run  # the SQL
docker compose -f deploy/docker-compose.yml run --rm leep-server migrate            # apply it, then start the servers
```

The full procedure, rolling upgrades and the newer-schema refusal are in
[OPERATIONS.md](OPERATIONS.md#upgrading).

### Running commands on the server from GitHub Actions

`.github/workflows/remote.yml` runs one shell command on a server over SSH,
from a manual dispatch on `main` (Actions > Remote > Run workflow, or
`gh workflow run remote.yml -f command='...'`). It needs four repository
secrets: `DEPLOY_HOST`, `DEPLOY_USER`, `DEPLOY_SSH_KEY` (a key made only for
this and added to that user's `~/.ssh/authorized_keys`) and
`DEPLOY_KNOWN_HOSTS` (the server's host key line, so a changed host key fails
the run). Anyone who can dispatch workflows can run any command as that user,
so give the user only the rights deploys need. Revoke by deleting the key's
line from `authorized_keys`.

## Monitoring and runbooks

Start the server with `--metrics-listen` (the compose stack does not set it) and
load the rules from [OPERATIONS.md](OPERATIONS.md#suggested-alerts) into
Prometheus. Every rule has a `runbook_url` pointing at its section of
[RUNBOOKS.md](RUNBOOKS.md): what fired, the impact, how to diagnose, the likely
causes and the safe fix. Set the base URL in the rules to wherever you publish
that file. `GET /v1/health` only says the process is serving HTTP; it does not
check the stores. `GET /v1/ready` does (see [Probes](#probes)). Alert on the
metrics, not on a probe alone.
