# 0060. Protect lands from expensive reads, and admit lands per ref
Status: Proposed
Date: 2026-10-21

## Context
Lands and reads share one server's cores. On small hosts the expensive
reads win, and lands queue behind them until clients time out and resend.
Three experiments on the two-core VPS and the Linux store measured this.
`docs/PERFORMANCE.md`, under "On all of Linux (VPS)", has the full numbers:

- **"Land leader on its own runtime (E1)".** Without reads, one ref lands
  61 to 77 times a second at 64 agents. In the swarm with the default read
  mix it lands 1.6 to 3.8 times a second, with a land p50 of a minute or more.
  Leaving search out of the mix raises lands/s about ten times (30.4 and
  18.8 at 500 and 2,000 agents). Running the land leader on a tokio runtime
  of its own changed nothing: the limit is cores, not scheduling.
- **"Indexing split on one host (E1b)".** No query derived anything inline,
  locally or split, because Linux's search index was already warm. The cost
  is executing the search, about 20 to 30 s of CPU per search over ~90k
  files. Moving indexing to another process (ADR 0058) cannot give those
  cores back on one host, and did not.
- **"Conflicts per land (E3)".** With realistic stale bases and hot files,
  each change takes 2.4 to 6.4 extra sends (re-lands after conflicts plus
  resends after timeouts) at 500 to 2,000 agents. At 5,000 agents a change
  takes about 3.5 minutes to land, with 5.8 to 7.9 sends per change. In the
  window the box landed about 0 a second. Nothing landed twice or was lost,
  but most of the work was resends.

Two things are missing. Nothing bounds how many expensive reads run at once.
ADR 0020's limits are per token, and a fleet of agents each running one
search is within every token's limit. And nothing bounds a ref's land
queue. A land waits in an unbounded queue past the client's 30 s timeout,
and the client then sends it again into the same queue. ADR 0056 dropped
the per-ref queue bound of its first draft from that change. This ADR
brings it back, measured in batches.

## Decision

### 1. Concurrency limits per class of expensive read
Each class of expensive read gets a semaphore of its own, taken with
`try_acquire` in the limits middleware after the per-principal limits of
ADR 0020. A request that finds its class full is refused at once, never
queued, for the reason ADR 0020 gives: a queue only moves the pile-up into
server memory. It gets `503`, code `unavailable` (FORMATS.md, "Error
codes"), with `Retry-After`. The permit rides in the response body like
ADR 0020's concurrency guard, so it is released when the body ends or the
request is dropped.

| Class | Requests | Default permits |
|---|---|---|
| `search` | `GET /v1/search/{rev}` | max(1, cores / 2) |
| `land_check` | `POST /v1/land/check` (section 3) | max(1, cores / 2) |
| `blame` | `GET /v1/blame/{id}` | off |
| `log_filtered` | `GET /v1/log/{id}` with a path, `author` or `extra` filter | off |
| `diff_large` | `GET /v1/diff` whose two sides are more than 1,024 generations apart | off |

- **Cores** is `std::thread::available_parallelism()`, which the server
  already uses for derivation and blame. On the two-core VPS search gets
  one permit, so at most one core searches and the other serves lands and
  cheap reads. ADR 0020 fixed its byte budget instead of deriving it from
  the host's memory, because memory needs cgroup parsing to be right in
  containers. Searches are CPU-bound, the permit count bounds cores, and
  `available_parallelism` already reads the CPU quota on Linux. The flag
  overrides it.
- **Classifying `diff`.** A diff's cost is known only once it runs. The
  generation distance of its two sides is a cheap proxy that is known
  before any work, because the handler reads both generations anyway. The
  threshold is a flag, and the swarm rerun (Testing) will check the
  proxy.
- **Never limited by this:** `POST /v1/land`, ref reads (`/v1/refs`,
  `/v1/resolve`, `/v1/reflog`), trees, files, blobs, changeset creates,
  `/v1/health` and `/v1/ready`. Their cost is small and bounded, and
  starving them is what this ADR prevents.
- **`Retry-After`** is the class's recent median request time divided by
  its permits, rounded up, between 1 and 30 s. A client that waits that
  long finds a permit about as often as one frees.
- **Configuration.** `--read-concurrency CLASS=N|off`, repeatable
  (`LEEP_READ_CONCURRENCY`, comma-separated), and
  `--diff-large-generations` (1,024). It applies to every token, including
  `"unlimited"` ones. The class protects the server, not the token's
  share, so a token's own limits cannot opt out of it. The values are
  flags, so changing them needs a restart, like ADR 0020's server-wide
  defaults.
- **Metrics:** `leep_read_class_permits{class}`,
  `leep_read_class_in_flight{class}` and `leep_read_class_shed_total{class}`.
  Refusals also count in `leep_http_requests_total` as 503.
- **Alert:** `LeepReadsShedding`, a ticket, when a class sheds more than
  half its requests for 15 minutes. Expensive reads are then wanted faster
  than the host can serve them: add hosts (ADR 0058's split) or raise the
  class. It has a runbook in `docs/RUNBOOKS.md` when this is built.
- **Clients.** `leep` retries a `GET` answered `503` `unavailable` with
  `Retry-After` the way it already retries a 429: at most four tries, and
  it reports a wait over 10 seconds. The MCP server returns the refusal to
  the agent as a retryable tool error that carries `retry_after`, rather
  than sleeping inside the tool call. `leep-swarm` counts shed reads apart
  from timeouts.

### 2. Land admission per ref
The server that batches a ref's lands bounds that ref's queue. This is the
holder of the ref's lease under `--land-coordination on`, or the only
server otherwise. The bound is measured in batches, not lands:

- **Bound.** `--land-queue-batches N` (default `off`). A land is refused
  when the queue already holds `N` batches: the waiting lands, divided by
  the 64-land batch limit and rounded up, plus one for a batch being
  replayed. A land waits about one batch time per batch ahead of it, so a
  bound in batches is a bound in time, whatever the batch sizes. A bound in
  lands would mean 64 times more waiting at full batches than at single
  ones. As a starting point, `N` times a full batch's time should be under
  half the clients' request timeout. The swarm runs in Testing set the
  default.
- **Where.** The check runs in `land_enqueue`, before `get_changeset`. It
  is one look at the in-memory `RefQueue` under the lock that enqueueing
  already takes. A refused land costs no store read, no batch slot, and no
  lease work. Lands already queued are never dropped to make room.
- **Answer.** `503`, code `unavailable`, with `Retry-After`: the number of
  batches ahead times the ref's recent batch time (a moving average),
  rounded up, between 1 and 30 s. On `POST /v1/land`, a 503 `unavailable`
  now also means the land was not admitted and nothing was done. That is
  already true of the body budget's 503, the only other source of the code
  on this route, because it refuses before the handler. FORMATS.md's
  condition text says so. No new code is added.
- **Forwarded lands (ADR 0056).** A forwarded land counts against the
  leader's bound like a land that arrived there directly. The leader
  answers the refusal, and the follower relays it unchanged, `Retry-After`
  included, as an answered forward
  (`leep_land_forwarded_total{outcome=answered}`). The follower never
  batches it locally (H1), and does not route it again or count it as a
  failed forward. A follower has no queue for a ref it does not lead, so
  only the leader can judge the bound.
- **Clients.** `leep land` and the MCP server treat a 503 `unavailable`
  from `/v1/land` as "not admitted". They wait the `Retry-After` plus up to
  50% random jitter, so refused agents do not return together, and send
  the land again under the same land id. The resend is safe twice over.
  Nothing was queued, and if an earlier send of the same id did land, the
  record answers it (ADR 0057). Because admission guarantees that nothing
  happened, `leep land` also resends an unkeyed land after this answer,
  which it never does after a timeout. Resends after admission refusals
  have their own budget (five, or 60 s of waiting) beside the
  `LAND_RESENDS` for timeouts and 5xx.
- **Metrics** (no ref label, as in ADR 0056):
  `leep_land_admission_refused_total{route=direct|forwarded}` and
  `leep_land_queue_batches`, the largest queue in batches over the refs
  this server leads.
- **Alert:** `LeepLandAdmissionRefusing`, a ticket, when more than 10% of
  lands are refused for 15 minutes. The ref is offered more lands than it
  can replay, which needs ADR 0056's next steps (pipelined or parallel
  replay), not a larger queue. It has a runbook when this is built.

### 3. A land dry run: `POST /v1/land/check`
Agents with stale bases mostly ask "does this still land?". Today the only
way to ask is to land, which takes a batch slot on the hot ref. A dry run
answers that question without the queue:

- **Route.** `POST /v1/land/check` takes the `LandRequest` body and
  answers with the `LandResponse` shape. It is a new route, not a
  `dry_run` field on `POST /v1/land`, because older servers drop unknown
  fields (ADR 0057, "Upgrade and mixed versions"). A dry run sent to a
  server from before this ADR would land for real. A new route gets `404`
  there, which is safe. A separate route also keeps ADR 0056's forwarding
  simple. Followers forward `/v1/land` bodies byte for byte without
  parsing them, and a flag in the body would send dry runs to the leader.
  `GET /v1/ready` lists the `land-check` feature, and clients use the
  route only when a server lists it. Adding a route is allowed for `/v1`.
- **What it runs.** It runs the same code as one land's `land_step`: the
  name, tag and `create` checks, the land-id record (`find_lands`), the
  ancestor and fast-forward tests, the landed-copy scan, the land policy,
  and the stack replay onto the head the server reads with `get_ref`. The
  replay goes into a `Staging` layer (ADR 0043) that is never flushed and
  is dropped with the request, so it writes no blob, changeset, row or ref
  log entry. It never enters a `RefQueue` or takes a batch slot. It never
  takes, holds or asks for the land lease, and it is never forwarded. It
  runs on whichever server receives it, which spreads dry runs over every
  server instead of the leader. It rescues nothing from changeset GC (ADR
  0059), because it makes nothing reachable.
- **Answer.** `Conflicts { onto, conflicts, clean }` is exactly what a land
  on that head would answer. `Landed { landed, rebased, attempts: 0 }`
  means it would land. `landed` is the copy the replay computed: it is not
  stored, and the real land usually makes a different copy because the
  head moves. `attempts: 0` marks a dry run. A land policy refusal is the
  same 400 (`needs_review`, `not_signed`, ...) that the land would get. The
  answer holds for the head it read. A land sent afterwards can still
  conflict.
- **Authorization and limits.** It needs a write token, like `land`, and
  charges the write bucket. It is the `land_check` class of section 1, so
  a burst of dry runs is refused with 503 before it can take the cores the
  leader needs.

### 4. Not adopted
- **The land leader on its own runtime.** E1 measured it with and without
  reads, at 500 and 2,000 agents and on the bench, and found no gain. On a
  host short of cores, a second runtime adds threads, not CPU.
- **Commutative lands batched into one changeset.** Folding a batch of
  non-overlapping lands into one changeset would cut replays and ref log
  rows, but it breaks what each land promises. A land id is recorded with
  the move that lands it (ADR 0057). Each land gets its own answer and its
  own ref log entry, with its own actor. Each rebased copy is linked to
  its original by `rebased-origin-v1`, with its signatures and reviews.
  One changeset for many lands would have one move, one id slot and one
  author.

## Consequences
- Under read load, lands and cheap reads keep cores. Expensive reads are
  refused when their class is full, so on small hosts searches are slower
  to get, with retries, and lands are faster.
- Clients see 503 `unavailable` on search, and on lands when an operator
  turns admission on. New `leep` and MCP versions wait and retry. Older
  ones report the 503 as an error, whose message says to retry.
- A land's longest wait in the queue is bounded by the admission bound
  instead of growing until clients time out and resend into the same
  queue.
- `/v1` gains one route and one feature string. No field, endpoint or code
  is removed or renamed. No persisted format, schema or hash changes, and
  there is no migration.
- New flags (`--read-concurrency`, `--diff-large-generations`,
  `--land-queue-batches`), five metrics and two alerts with runbooks.
- Limits are per server process. N servers run up to N times a class's
  permits, which is what is wanted when each server has its own cores.

## Alternatives considered
| Option | Why not, or not alone |
|---|---|
| Read replicas, or separate hosts for queries and indexing (ADR 0058's split) | The right answer on several hosts, where it adds cores. E1b showed it gives nothing on one host. It complements limits and does not replace them: a fleet can still saturate a query host |
| Cheaper search | A follow-up. 20 to 30 s per search over ~90k files with a warm index (E1b) needs a profile of its own (trigram filter selectivity, blob reads per candidate). Limits are still needed whatever a search costs |
| An expensive-read class per token (ADR 0020, "Alternatives") | Bounds one token, not the server. Thousands of agents each running one search stay within their limits |
| Queueing expensive reads instead of refusing | Holds connections and memory, and hides overload from clients. ADR 0020 chose refusing for the same reason |
| Bounding the land queue in lands | A wait depends on batches ahead, not lands. A lands bound is 64 times looser at full batches than at single ones |
| A `dry_run` field on `POST /v1/land` | Older servers drop the field and land for real, and followers would have to parse bodies they forward byte for byte |
| A lower OS or tokio priority for reads | E1: isolating the leader's scheduling did not help when cores are the limit |

## Not changed
- Refs move only through `cas_ref` and its logged variants. Admission and
  the dry run never move a ref, and the dry run never writes.
- One ref log entry per land that moves the ref, with its actor, written
  in the same transaction as the move (ADR 0032).
- Conflicts are answered, never resolved by the server. The dry run
  reports them exactly as a land would.
- Land ids are recorded with the move, in the same transaction, and
  answered at any age (ADR 0057). A refused land records nothing, and the
  id stays free for the resend.
- ADR 0056's routing, lease and H1 rule are unchanged. A refusal is an
  answer, not a failed forward.

## Testing
- **Fault sweeps.** `LEEP_FAULT_SEEDS=300 cargo test -p leep-derive
  fault_tests`, with these new cases under store faults, each ending with
  the sweep's checks (nothing lost, nothing twice, the ref log replays):
  - `admission_refused_lands_nothing`: lands refused at a full queue, then
    resent under their ids, interleaved with batches that lose CAS races
    and fail commits;
  - `forwarded_land_refused_by_full_leader`: the follower relays the 503,
    never batches locally and never counts a failed forward;
  - `leader_killed_with_full_queue`: the leader killed or paused with its
    queue at the bound; every agent lands within TTL + 2 s of the takeover;
  - `land_check_writes_nothing`: dry runs racing lands under a wrapper
    store that counts writes. No dry run writes or moves anything, and the
    ref log equals a run without them;
  - `land_check_matches_land`: on a fixed head, a dry run's answer equals
    what `land_unstaged` answers (the same conflicts and `clean`; `Landed`
    where the land moves).
- **Read classes.** With a gated search on memory stores: a full class
  refuses at once with 503 and `Retry-After`, the permit is released when
  a request is cancelled or times out, and lands, ref reads and file reads
  go through while every search permit is held.
- **Crash test.** `cargo test -p leep-e2e --test crash` with
  `--land-queue-batches` set low enough that agents are refused. Agents
  resend after 503s, the server is SIGKILLed with a full queue, and the
  exactness checks (every commit once, one ref move per land) hold.
- **Swarm.** E1's reads arm again, on the same VPS and store, at 500 and
  2,000 agents, three rounds, shared runtime, with the search class at its
  default (one permit on two cores). It succeeds if lands/s recovers at
  least half of the gap to E1's no-search arm (30.4 and 18.8 lands/s) and
  no land is double or lost. It reports searches shed and admitted, and
  the admitted searches' p50. Then E3 with `--land-queue-batches` at a few
  values, to choose the default and to show whether sends per change fall.
  Each run also reports blame, filtered log and large diff times, to
  decide whether those classes need defaults.

## Rollout
- **Search and `land_check` limits are on by default.** A refused search
  costs one client a retry. An unlimited one costs every land on the host
  (E1: about ten times the lands/s), and that is today's default, not an
  edge case. The limit is a read limit, so correctness cannot depend on it.
  The `unavailable` code already tells clients to back off and retry. On
  large hosts, half the cores rarely binds unless searches are already
  crowding out everything else. `land_check` is a new route with no
  existing clients to surprise.
- **`blame`, `log_filtered` and `diff_large` ship off.** No measurement yet
  shows them starving lands. E1's no-search arm, which still ran them,
  landed 16 to 31 times a second against a 61 to 77 ceiling, so they are
  the next suspects. The swarm rerun decides their defaults.
- **The land queue bound ships off** until a swarm run with it on shows no
  resend storm. That run needs the 503s answered with jittered waits,
  sends per change no higher than without the bound, and no double or
  lost lands. This is the rule ADR 0056 used for `--land-coordination`.
  Clients that honour the 503 ship first. Operators then turn the bound on
  per deployment.
- To back out, set `--read-concurrency search=off` or
  `--land-queue-batches off` and restart. Nothing persisted depends on
  either.
