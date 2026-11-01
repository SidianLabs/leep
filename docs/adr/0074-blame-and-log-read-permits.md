# 0074. Default permits for blame and filtered log
Status: Accepted
Date: 2026-11-01

## Context
ADR 0060 built one expensive-read class, `search`, and shipped `blame`,
`log_filtered` and `diff_large` off "until the swarm rerun decides their
defaults". The flag accepted only `off` for them.

The rerun after the index-lag fix (ADR 0073): 2,000 mixed agents against one
two-core `leep-server` (Postgres, `--indexing lease`, `--meta-pool-size 32`).
The server was capacity-bound: server CPU about 160% of 200%, the data pool
32 of 32 in use all the time, Postgres CPU low. Request time summed over
about 200 s:

| Route | Server time | Requests | Mean |
|---|---|---|---|
| blame | 28,664 s | 2,835 | 10 s |
| path log | 35,011 s | 9,178 | 3.8 s |
| create | 13,833 s | 6,897 | 2.0 s |
| files | 10,859 s | 18,133 | 0.6 s |

Blame and path log together held about 320 requests in flight on two cores.
They do not fail; they queue for cores and pool connections, and so do the
creates, syncs, file reads and lands behind them, which should take under
500 ms: create p50 3.4 s, land p50 1.4 s. A blame walks up to 64 history
stretches at once, so a handful of blames can take every connection.
Nothing bounded either route but `--request-timeout`.

## Decision
- **The `blame` and `log_filtered` classes are built,** on ADR 0060's
  mechanism unchanged: one semaphore per class for the whole server,
  `try_acquire` in the read-limits middleware, refusal at once with `503`
  `unavailable`, `Retry-After` (the class's recent median hold over its
  permits, 1 to 10 s) and `Leep-Refused: read-class`.
- **What each class covers.** `blame` is `GET`/`HEAD /v1/blame/{id}`.
  `log_filtered` is `GET /v1/log/{id}` with a `path` that is not empty
  after trimming `/`, or any `author` or `extra` parameter, as the handler
  reads them, plus `GET /v1/last-change/{id}`. Last-change walks the same
  path history index as a path log (`Route::LastChange`, ADR 0058), so it
  shares its class rather than escaping both. A log with none of these is
  a first-parent page walk bounded by its `limit` and stays unlimited, as
  do `provenance`, `files`, `tree` and `diff`.
- **Defaults.** `blame` = `max(2, cores)`; `log_filtered` =
  `2 × max(2, cores)`; cores is `available_parallelism`, as for `search`.
  On the two-core host: search 1, blame 2, log_filtered 4.
  - A blame is part CPU (line diffs on blocking threads) and part waiting
    on batched metadata and blob reads. Search got half the cores because
    it is almost all CPU (0.45 s per warm search). One blame per core keeps
    the cores fed while blames wait on reads, without the run queue the
    swarm showed. Two at least, so on a one- or two-core host one long
    blame (a file changed thousands of times) does not shed every other.
  - A filtered log or last-change is mostly index and changeset reads with
    little CPU, and agents send them three to four times as often as
    blames (9,178 to 2,835 above). Twice blame's count admits that mix.
  - Together the three classes admit at most `3.5 × cores` requests at once
    on hosts of two or more cores (7 on two), against about 320 in flight
    in the run above, while cheap reads and lands keep running beside them.
  - These are starting values, like ADR 0060's: the next swarm run should
    confirm that create and land p50 return under 500 ms and that the
    classes' per-permit throughput stays above `LeepReadsShedding`'s floor.
- **`off` stays an explicit value** for each class, and is how to back out
  (`--read-concurrency blame=off,log_filtered=off`). A number is now
  accepted for both; `land_check` and `diff_large` still take only `off`.
- **Shed before any work.** The class is decided from the method, path and
  query string alone (the query is parsed as the handler parses it), and the
  middleware sits inside authentication and the per-token limits, neither
  of which reads the metadata store, and before the handler. A refused
  request takes no connection and runs no query. A query that does not
  parse is classified unfiltered; the handler then refuses it with 400
  before any store read.
- **The permit is held to the end.** As for search, the permit rides in the
  response body, so it is held while the answer is sent and released when
  the body ends or is dropped: on a client disconnect, on cancellation, and
  when `--request-timeout` drops the request (its 408 is answered without
  the permit). A blame or path log that waits for the indexer
  (`--index-wait`) holds its permit while it waits, so a lagging index
  shows as shedding in these classes; that is intended, since the waiting
  request is also holding a task and soon a connection.
- **Immediate shed, no wait queue.** A short bounded queue (say up to N
  waiters for 250 ms) was considered. In the run above the mean blame was
  10 s. Even if the limit brings admitted blames down to 1 s, two permits
  free one every 500 ms on average, so under a swarm's arrival rate most
  waiters would still be shed, only 250 ms later and after holding a task
  and a timer. The clients already answer a
  refusal with a jittered wait of `Retry-After` (`leep`, up to four tries),
  a retryable tool error (`leep-mcp`) or a wait (`leep-swarm`), which
  spreads the retries out better than a server-side FIFO would. It also
  keeps one rule for every class, which ADR 0060 argued for. If a later
  run shows many sheds landing just before a permit frees, a queue can be
  added behind the same flag without a protocol change.
- **Metrics.** Two counters join ADR 0060's three, per class:
  `leep_read_class_admitted_total{class}` (got a permit) and
  `leep_read_class_completed_total{class}` (admitted and answered 2xx,
  counted when the permit is released). The `log_filtered` class cannot be
  told from an unfiltered log by the HTTP route label, so the alert needs
  class counters of its own.
- **Alert.** `LeepReadsShedding` is evaluated per `(instance, class)` on
  these counters: shed share over half for 15 minutes, and fewer than 2
  completions a minute per permit. Its thresholds are unchanged. Servers
  older than this change have no `admitted_total`, so the rule is silent for
  them until upgraded.
- **Clients.** Nothing changes on the wire: the shed answer is the
  documented 503 shape, `/v1` gains no field. `leep blame`, `leep log
  --path`, `leep log --author` and last-change retry like any refused `GET`.
  `leep-swarm` counts shed reads by the server's class name (`search`,
  `blame`, `log_filtered`) and, after a shed read, waits its `Retry-After`
  plus up to half again before its next operation, as its lands already do
  for admission refusals, so shed agents do not spin.

## Consequences
- On small hosts blame and path log become scarce under a swarm: agents see
  `server busy` from `leep` after four tries, or wait. Lands, creates,
  syncs and file reads get their cores and connections back.
- Each server process has its own permits; N servers run up to N times
  each class.
- Within a class the permit does not bound connections: two blames may
  still read many rows at once through their batched reads. If a run shows
  the pool still saturated with blame at its permits, the next step is a
  pool bound like `--search-pool-permits`.
- Operators who relied on unlimited blame (a single-user server with many
  cores and an idle pool) can set `blame=off`; on such hosts the default
  is already one blame per core.

## Testing
- `crates/leep-server/src/read_limits.rs`: defaults resolve to
  `max(2, cores)` and twice that; parsing of `blame`/`log_filtered` numbers
  and `off`; classification of every route and filter, including empty and
  `/`-only paths, `author`, `extra`, last-change and unfiltered logs.
- `crates/leep-server/tests/read_limits.rs`, on memory stores through the
  real router: a full blame or filtered-log class refuses with 503,
  `Retry-After` and `Leep-Refused` without one metadata store call (counted
  by a wrapping store), while an admitted one reads; the permit is released
  on completion, on a 4xx, on a dropped answer, on client cancellation and
  on `--request-timeout`; `off` lifts both; the defaults admit exactly their
  permits; lands, files and unfiltered logs go through while both classes
  are full; admitted, completed and shed counts per class.
- `crates/leep-soak/src/swarm.rs`: a shed answer carries its `Retry-After`
  (1 s without one; a 503 without `Leep-Refused` is not a shed), reads are
  counted under the server's class, and the back-off is the wait plus up to
  half.
