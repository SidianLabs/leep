# 0020. Per-token request limits
Status: Accepted
Date: 2026-10-15

## Context
Teams run many agents against one server. A runaway agent (a retry loop, a
search in a tight loop, an upload that never ends) can use all of a server's
CPU, connections or bandwidth and starve everyone else. The only protection
was a global body size cap and the request timeout. The roadmap's hardening
list asks for size, rate and concurrency limits per token.

## Decision
- **Three limits, per principal.** A principal is a token's name. Each has its
  own state: a token bucket for requests per second, a cap on requests in
  flight, and a cap on request body size. All are off unless configured, so a
  deployment that sets nothing behaves as before and no middleware is
  installed.
- **Rate.** `rate` requests per second with room for `burst` at once (default:
  the rate rounded up, at least 1). A principal has two buckets with the same
  parameters, one for reads and one for writes, classified by the same
  function as authentication (`needs_write`; `POST /v1/blobs/missing` is a
  read). A burst of uploads therefore cannot lock an agent out of reading.
  Refusal is `429` with `Retry-After` (whole seconds, rounded up, at least 1)
  and the usual JSON `{"error": ...}` body.
- **Concurrency.** A semaphore per principal, taken with `try_acquire`: a full
  principal is refused at once with `429` and `Retry-After: 1`, never queued.
  Queueing would only move the pile-up from clients into server memory. The
  permit is held by a guard that rides inside the response body, so it is
  released when the body has been sent or dropped; a client that disconnects
  or a request cut off by the timeout drops the future, and with it the
  guard. The count covers reads and writes together.
- **Size.** A per-principal `max_body`. A declared `Content-Length` over it is
  refused before anything is read; a body with no declared length is wrapped
  in a counting limit. Both answer `413` with a JSON body (the extractor's own
  413 is plain text and is rewritten). The global `--max-body-bytes` stays and
  wins when it is smaller. A server-wide default for `read` tokens
  (`--read-max-body-bytes`) exists because the only body a reader sends is a
  list of hashes.
- **Order.** Authentication, then limits, then the handler. Size is checked
  first (a request that can never succeed should not spend budget), then rate,
  then concurrency. Unauthenticated requests spend nobody's budget.
  `GET /v1/health` is never limited, so probes and load balancers keep
  working.
- **Configuration.** Server-wide defaults are flags with environment
  equivalents: `--rate-limit`/`LEEP_RATE_LIMIT`,
  `--rate-burst`/`LEEP_RATE_BURST`,
  `--max-concurrent-per-token`/`LEEP_MAX_CONCURRENT_PER_TOKEN` and
  `--read-max-body-bytes`/`LEEP_READ_MAX_BODY_BYTES`. A tokens-file entry may
  set `rate`, `burst`, `concurrency` and `max_body`, which override the
  defaults for that token; the word `"unlimited"` opts a token out of a
  default (a CI bot that lands for many agents, say). Existing files parse
  unchanged and `deny_unknown_fields` still rejects typos. Values are checked
  at startup: zero, negative, non-finite or fractional-where-whole values, a
  `burst` without a rate, and `--read-max-body-bytes` without a tokens file
  all stop the server with a message naming the token or flag. A token that
  sets only `rate` takes `--rate-burst` if given, else its rate rounded up.
- **Anonymous mode.** With no tokens file, the defaults apply per client IP
  (the peer address; IPv6 by /64, so one subscriber's prefix is one client).
  Per-IP beats one global bucket because one noisy machine cannot lock out
  the rest. Behind a reverse proxy every request has the proxy's address, so
  this degenerates to one shared bucket, which is safe (over-limiting) but not
  fair; run a tokens file when fairness between agents matters. Trusting
  `X-Forwarded-For` is not offered: it is trivially forged unless the proxy
  strips it, and a wrong default is worse than none. The table holds at most
  4096 clients. When it is full, entries that are idle (bucket full, nothing
  in flight) are swept at most once a second, and clients that still do not
  fit share one overflow entry, so memory is bounded and the failure mode is
  over-limiting. `--read-max-body-bytes` does not apply (no roles).
- **Metrics and logs.** `leep_requests_limited_total{reason, token}` with
  `reason` of `rate`, `concurrency` or `size`, and `leep_requests_in_flight
  {token}`. `token` is the token's name (`anonymous` for every client address:
  addresses are unbounded and never become labels). Only tokens that have a
  limit are tracked. Each refusal is a DEBUG log line; there are no warnings,
  so a runaway client cannot flood the log. Refusals also count in
  `leep_http_requests_total` as 429 or 413.
- **State is per server process.** With several servers behind a balancer each
  enforces the full limit itself, so a principal can get up to N times the
  configured rate across N servers. Shared limits would need a shared store
  on every request; not worth it yet.
- **CLI.** The client retries a `GET` that gets a 429, at most 4 tries in all,
  waiting the `Retry-After` it is given, and reports a wait over 10 seconds
  instead of sitting through it. Other methods are never retried.
- No `leep-core` type changed and no endpoint or field was removed: 429 is a
  new status for existing endpoints, and the tokens file only gains optional
  fields (FORMATS.md rule 4).

## Alternatives
- **A separate, lower concurrency class for land, search, log and blame.**
  They are the expensive endpoints, but the per-token cap and the request
  timeout already bound them, and a second class would add a knob to every
  token and a route table to keep in step with the API. It can be added later
  as another optional field.
- **Queueing instead of refusing when concurrency is full.** Hides overload
  from clients and holds their connections; a bounded queue still needs a
  refusal path, so the refusal comes first.
- **Separate read and write rates.** Same parameters per class were enough to
  stop uploads starving reads; two sets of fields can be added if someone
  needs them.
- **Limits in the reverse proxy only.** Proxies see addresses, not token
  names, and agents often share a NAT. They remain a good first line against
  unauthenticated floods, which this does not address.
- **A global limit.** Protects the server but lets one agent consume all of
  it, which is the problem.

## Consequences
- A well-behaved deployment pays nothing; a limited one pays two mutex
  operations per request.
- Limits live in the tokens file and reload on SIGHUP (ADR 0024), keeping the
  state of tokens whose limits did not change. The server-wide defaults are
  flags and still need a restart.
- Rate state is not shared between servers or persisted, and resets on
  restart.
- Floods of unauthenticated requests are not limited here (they fail
  authentication cheaply); use the proxy.
- Retries add up to about 30 seconds of waiting to a throttled `leep` read
  before it fails.

## Security review 2026-10
See [SECURITY-REVIEW.md](../SECURITY-REVIEW.md).
- The request timeout drops a request's future, so it stops work only at an
  await point. Synchronous CPU work runs to the end on its async worker.
  `blame`'s line diff now has a 500 ms deadline (S-05, fixed); the
  server-side text merge does not yet (S-10, deferred; ADR 0019).
- A `log` filtered by `author` or `extra` walks history until it finds
  `limit` matches, so a filter that matches nothing walks all of it. It awaits
  storage between steps, so the timeout stops it, and these limits bound
  repeats (S-13, accepted).
- `POST /v1/changesets` refuses a path over 4096 bytes, or paths naming over
  a million directories or 256 MiB of directory names, because building the
  tree needs memory quadratic in path depth (S-04, fixed).

## Tighter defaults, 2026-10-18
- `read` tokens get a 1 MiB body cap by default when a tokens file is given
  (`--read-max-body-bytes` or a token's `max_body` overrides it). They cannot
  upload, and their largest legitimate body is a `POST /v1/blobs/missing`
  list; the old default was the 512 MiB global cap, which let a read token
  make one request claim about eight million blobs (see ADR 0007, S-11).
- `POST /v1/blobs/missing` takes at most 100,000 hashes (`MAX_MISSING_BLOBS`)
  and answers more with 400, never a truncated answer. Each hash costs a
  metadata read and a claim write, so the list bounds the work of one request
  whatever the token. The cap sits above a Linux-sized tree (about 90k files)
  because older CLIs send a whole tree in one request; the CLI and MCP client
  now send 10,000 at a time (`MISSING_BLOBS_CHUNK`, about 670 KB of JSON), which
  also keeps a read token under its 1 MiB default. That endpoint gets no
  larger read limit of its own: chunking is enough, and a read token that
  needs more can be given `max_body`.

## Server-wide body limits, 2026-10-21
The per-token caps are off by default, and the one global cap was 512 MiB on
every route, so nothing bounded the memory request bodies take together: the
blob and changeset handlers read the whole body into memory, and sixteen
concurrent 512 MiB uploads from any writers would ask for 8 GiB.
- **Per-route caps, always on.** `PUT /v1/blobs/{hash}` takes at most
  `--max-blob-bytes` (64 MiB); every other route, all JSON, at most
  `--max-json-body-bytes` (32 MiB, about 200,000 changed paths in a
  changeset). `--max-body-bytes` stays as a ceiling over both. Over the cap is
  `413` with a JSON body naming the flag: before the body is read when its
  length is declared, while it streams when not.
- **An in-flight byte budget.** A semaphore of `--max-inflight-bytes` (1 GiB),
  counted in KiB. In the limits middleware, so after authentication (a request
  without a valid token holds nothing), each request with a body takes its
  `Content-Length`, or its route's cap when it declares none, and holds it until
  its handler returns, which is when the body's memory is released. A request
  that does not fit is refused at once with `503` and `Retry-After: 1`, not
  queued, for the reason concurrency is refused rather than queued. A cap above
  the budget fails startup.
- **Why 1 GiB, fixed.** It bounds request-body memory on its own scale, next to
  the 256 MiB blob cache and the derive cache, and fits the 2 to 4 GiB
  containers the deployment examples assume, while allowing 16 maximum-size
  uploads or thousands of ordinary ones at once. Deriving it from the host's
  memory would make two servers with the same flags behave differently and
  would need cgroup parsing to be right in containers; operators with more
  memory raise it.
- **Behaviour change.** Files between 64 MiB and 512 MiB, which committed
  before, are now refused (413) until the operator raises `--max-blob-bytes`.
  The CLI reports the 413 with a hint naming the flag and does not retry; the
  git mirror reports the server's message on the ref. `leep-git import` writes
  the stores directly and is not limited.
- **Trade-off.** A slow client holds its declared share for up to
  `--request-timeout`, so a writer that declares large bodies and trickles them
  can hold the budget; per-token `concurrency` bounds how many shares one token
  holds. Metrics `leep_request_body_bytes_in_flight`,
  `leep_request_body_budget_bytes` and
  `leep_request_body_refused_total{reason,class}`, and the alert
  `LeepBodyBudgetExhausted`.

## Probe cost, 2026-10-21
`GET /v1/ready` is public and unlimited, so anyone who can reach the server
could make it read both stores on every call. Its answer is now reused for one
second (`DEFAULT_READY_CACHE`), and callers that arrive while a check runs wait
for that check instead of starting their own, so a server checks its stores at
most once a second whatever the probe rate. Readiness changes show up to a
second late, well inside any orchestrator's probe period.
`leep_ready_checks_total` counts checks, not calls.
