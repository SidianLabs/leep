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
- Limits live in the tokens file, so changing them needs a restart, like
  everything else in that file (config reload is M6).
- Rate state is not shared between servers or persisted, and resets on
  restart.
- Floods of unauthenticated requests are not limited here (they fail
  authentication cheaply); use the proxy.
- Retries add up to about 30 seconds of waiting to a throttled `leep` read
  before it fails.
