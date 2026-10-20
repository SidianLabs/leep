# 0056. One land coordinator per ref across servers
Status: Proposed (design; not implemented yet)
Date: 2026-10-20

## Context
Group commit (ADR 0032) and staged batches (ADR 0043) made lands on one ref
cheap inside one server process: a leader task replays a batch of queued
lands and moves the ref once. Across processes nothing changed. Instances
sharing one metadata store still race through `cas_ref`, and a lost swap
redoes the whole batch.

`docs/PERFORMANCE.md`, "Several servers on one ref" (#100), measured what
that costs with 2 and 3 servers on one Postgres. Adding servers added no
throughput and starved agents. In all 18 multi-server runs one instance
answered every land inside the 20 s window. The other instances' agents
waited 21 to 30 s, or got 409 after 32 attempts:

| Servers | Agents | Lands/s | Land p99 (ms) | Land max (ms) | Stalled agents |
|---|---|---|---|---|---|
| 1 | 64 | 201 | 460 | 590 | 0 |
| 1 | 256 | 125 | 2175 | 2190 | 0 |
| 2 | 64 | 170 | 564 | 25702 | 32 of 64 |
| 3 | 64 | 170 | 21480 | 23051 | 43 of 64 |
| 3 | 256 | 146 | 24401 | 24584 | 171 of 256 |

The profile explains why the loser never recovers. The winner moves the ref
every 70 to 80 ms. A losing attempt takes 0.75 to 1 s between reading the
head and the swap: a full batch of 30 to 64 replays at 7 to 15 ms each (they
are further from the head), plus a landed-copy scan (ADR 0021) that walks
everything the head gained since the lands' bases. That scan is 465 ms by
the second or third loss. Each loss makes the next attempt longer. The
jittered backoff (at most 200 ms) cannot help because the winner never
loses and so never pauses.

The interim advice in `docs/RUNBOOKS.md` is to route `POST /v1/land` to a
single instance at the balancer. That works, but it sends every ref's lands
to one process, fails over only as fast as the balancer's health checks, and
brings back the starvation silently if the rule is lost.

This decision also has to fit the planned scale of about 50,000 agents.
That needs bigger batches and lands handled by a dedicated pool of
instances. An architecture review summarised the direction: a store-level
per-ref leader (a lease in the metadata store), other servers forward lands
to it or park them in a queue table the leader drains, and correctness
stays solely on `cas_ref`.

Constraints:

- Refs move only through `MetaStore::cas_ref` and its logged variants. The
  coordinator is an optimisation for liveness. It must not become a second
  source of truth for correctness.
- Every land keeps the answer and ref log entry it gets today: sequential
  semantics within a batch, one entry per land with that land's actor
  (ADR 0018, 0032).
- `land` stays idempotent across lost answers. Another change is adding a
  client land idempotency key (`land_id`) to the land request and ref-log
  path. This design must not depend on how a retry is recognised, and
  must not remove the landed-copy scan.
- SQLite and memory stores serve one process. They keep today's in-process
  path unless coordination is asked for.

## Decision

### Summary
1. **Leadership is a lease row per ref in the metadata store**, taken and
   renewed with single conditional statements, with an epoch that counts
   holders. It is not an advisory lock or an external election service.
2. **Other servers forward the land over HTTP to the leader's own
   `/v1/land`**, carrying the client's original body and token. The leader
   authenticates the client itself, so the actor in the ref log still comes
   from the token. The answer comes back on the same exchange. A durable
   `land_queue` table is not used.
3. **Correctness rests only on `cas_ref` and the ref log.** Epochs are not
   checked in the swap. A stale leader's swap either loses, because the
   ref moved, or wins legitimately, because the ref had not moved. Both
   outcomes are correct. A leader that finds its lease gone stops
   retrying and sends its lands back to be routed again.
4. **When forwarding fails, the server lands locally.** It takes the
   in-process path and races the CAS, as every server does today. A broken
   coordinator therefore costs throughput, never correctness or
   availability.

### The lease
Migration 0005 (Postgres and SQLite) adds:

```sql
CREATE TABLE land_leases (
    repo_id    BIGINT NOT NULL DEFAULT 1,  -- ADR 0054; 1 is the default repository
    ref_name   TEXT   NOT NULL,
    holder     TEXT   NOT NULL,            -- random per process start, 32 hex chars
    address    TEXT   NOT NULL,            -- the holder's --advertise-url
    protocol   INT    NOT NULL,            -- forwarding protocol version, 1
    epoch      BIGINT NOT NULL,            -- +1 on every change of holder
    expires_at BIGINT NOT NULL,            -- ms, by the store's clock
    PRIMARY KEY (repo_id, ref_name)
);
CREATE TABLE cluster_keys (name TEXT PRIMARY KEY, value BYTEA NOT NULL);
```

The migration only creates tables. Like 0003, it tolerates old servers that
are still running (ADR 0023). It is not a format feature (ADR 0055): no row
that an old server reads changes.

`MetaStore` gains four methods, each defaulting to "unsupported". A store
that does not support them gets no coordination: every land takes the
in-process path, as today.

- `land_lease(ref)` returns the row, plus the time left before it expires
  as measured by the store's clock. Callers never compare their own clock
  with the store's.
- `acquire_land_lease(ref, holder, address, ttl, steal_epoch)` is one
  statement: `INSERT ... ON CONFLICT (repo_id, ref_name) DO UPDATE ... WHERE
  expires_at < now OR holder = $holder OR epoch = $steal_epoch RETURNING
  epoch`. It bumps the epoch when the holder changes. It returns the
  epoch, or nothing when another holder's lease is still live.
- `renew_land_leases(holder, [(ref, epoch)], ttl)` is one `UPDATE ... WHERE
  holder = $1 AND (ref_name, epoch) IN (...)` for every lease the process
  holds. It reports which leases it renewed.
- `release_land_lease(ref, holder, epoch)` deletes the row only if it is
  still that holder's lease at that epoch.

Postgres uses `now()`. SQLite uses the process clock: all its processes
share a host. Memory uses tokio's clock, so tests can pause it.
`check_land_lease_contract` in `leep_core::memory` covers these rules, and
every backend calls it.

**Timing.** The default TTL is 5 s (`--land-lease-ttl`), and a background
task renews every lease the process holds once a second, in one statement.
A leader treats its lease as valid until `sent + TTL - 1 s`, where `sent`
is the monotonic time at which it sent the last successful renewal. The
store's expiry is at least `commit + TTL`, which is no earlier, so the
leader's own view always ends first. A lease is released after 30 s
without a land, and when the server shuts down gracefully, after its queue
drains. A restarted process has a new holder id, so it never mistakes its
predecessor's lease for its own.

**Who leads.** A server whose `--advertise-url` is set, and which receives
a land for a ref with no live lease, takes the lease and leads. The lease
is taken on first use rather than only after contention is seen. Leaders
are then deterministic and easy to test. The cost is one write per ref per
idle period, and renewal is one statement per process per second however
many leases the process holds. Whether to take the lease only on contention
is an open question below.

**Fencing.** The epoch is not passed into `cas_ref`. Refs are also moved by
imports, raw `POST /v1/refs`, `leep undo` and old servers, none of which
hold a lease. A fenced swap would only add a way to fail. The epoch is
used for:

- detecting that the lease was lost, when a renewal finds no row for
  `(holder, epoch)`;
- routing: a forwarded land names the epoch it was sent under;
- metrics and logs.

**Leader death and staleness.** Correctness never depends on anyone
noticing these cases. Only how fast lands resume does.

| Event | What happens | Lands resume after |
|---|---|---|
| Leader SIGKILLed before its swap | Staged writes may have stored unreferenced objects, which GC removes; the ref did not move. Followers' forwards fail at once (connection reset). The next forward gets "connection refused" twice, so the follower steals the lease (`steal_epoch` = the dead epoch), leads, and lands the request again | well under 1 s, plus one batch |
| Leader SIGKILLed after its swap committed, before answering | The lands applied. Their callers get errors and retry, which is what clients already do after a lost answer. The new leader finds each land in the head's history: an ancestor, its landed copy (ADR 0021), or by `land_id` once that exists | as above |
| Leader host lost (no reset) | Forwards time out. TCP keepalive detects a dead peer in about 3 s, and the connect timeout is 500 ms. Followers land locally (fallback) until the lease expires, then one of them takes it | at most TTL plus about 1 s (~6 s), with local landing meanwhile |
| Leader paused past its TTL (SIGSTOP, VM pause, a long stall) | Another server takes the expired lease. When the paused leader resumes, its swap loses if the new leader has moved the ref, or wins legitimately if not. Before every attempt it checks its own lease view. Once that has expired, or a renewal finds the epoch moved, it stops retrying | no stall: the new leader is already landing |
| Leader can reach the store but not its followers (partition) | Its lease stays live. Followers' forwards fail and they land locally (fallback, counted), racing it as today. They do not steal: stealing on a timeout would flap during a symmetric partition | no stall, reduced throughput; alerted |

A stepping-down leader answers each land it has not replayed with an
internal `Rerouted` outcome. A land that arrived at that server directly
is routed again by its own handler: it is forwarded to the new leader, or
landed locally. A land that was forwarded to it gets 503 with
`Leep-Land-Not-Leader: 1`, and the follower that sent it routes it again.
Lands already replayed in an attempt whose swap is in flight wait for that
swap, as for a cancelled request (ADR 0032).

### Forwarding
**Path.** A follower reads the target ref from the body. It looks up the
lease, which it caches until the store-reported expiry minus 1 s, and on
any forward failure. It then sends one `POST {address}/v1/land` with:

- the client's body, byte for byte, so fields the follower does not know
  (`land_id`, and later additions) reach the leader unchanged;
- the client's `Authorization` header;
- `Leep-Land-Forward: v=1; epoch=<n>; from=<holder>`;
- `Leep-Peer-Auth: <unix ms>.<mac>`, a keyed BLAKE3 MAC over the header
  above, the timestamp and a hash of the body, keyed by the
  `cluster_keys` row `land-forward-v1`. The first server to need the key
  creates it with `INSERT ... ON CONFLICT DO NOTHING`.

**At the leader.** The request goes through the normal stack: config
snapshot, token authentication and the land policy of the leader's
current config. The actor is the client's token name, as if the client
had called the leader directly. Forwarding adds no identity the leader has
to trust. A valid peer MAC that is no more than 30 s old buys one thing:
the request is exempt from the per-principal limits (ADR 0020), because
the edge server has already charged them. Without the exemption, all of a
shared token's lands would count against one instance's in-flight cap.
A forwarded land is never forwarded again. If the receiver does not hold
the named epoch, it answers 503 `Leep-Land-Not-Leader` and the follower
routes the land again. If it does hold it, the land joins the same
in-process queue as the leader's own lands, in arrival order.

**Answer, timeouts, cancellation.** The leader's status and body are
relayed to the client verbatim. Both servers apply their own
`--request-timeout`. A follower whose client goes away drops the forward.
The leader then sees a disconnect and drops the land if it has not yet
been replayed, exactly as for a direct client (ADR 0032).

**Retries at the follower.** These happen within the client's request
deadline, at most 3 times:

- `Not-Leader`, or a changed lease: route again.
- Connection refused twice from the same holder and epoch: steal the lease
  and land as leader.
- Timeout or reset with no response: land locally (fallback).

A retry after an ambiguous failure is just a repeated land, which is
idempotent.

**Back-pressure.** The per-ref in-process queue gets a bound,
`--land-queue-max` (default 4096). Over the bound, a land is answered 503
with `Retry-After: 1`. This bound is new: today the queue is unbounded.
Relayed 503s keep their header.

**Transport.** HTTP/1.1 with a keep-alive pool per leader address, via
`reqwest`, which becomes a normal dependency of `leep-server` (it is
already a dev-dependency). HTTP/2 multiplexing is a later option if the
pool size matters. `--advertise-url` must reach the leader directly on
the private network, not through the balancer. A client's bearer token
crosses that network just as it already does between the balancer and the
servers.

**Placement in the code.** `leep-derive` owns the coordinator: lease
bookkeeping and the renew task, the routing decision, step-down and
`Rerouted`, the queue bound. It uses only `MetaStore`, plus a
`LandForwarder` trait. `leep-server` implements that trait over HTTP.
`leep-derive` stays free of HTTP and of concrete backends, and its tests
use an in-process forwarder that can drop, delay, and lose the answers of
forwards. A `Repo` without a coordinator, the library default, lands in
process exactly as today.

### Modes and rollout
`--land-coordination auto|on|follow|off`:

| Mode | Behaviour |
|---|---|
| `on` | Leads and forwards. Needs `--advertise-url` |
| `follow` | Forwards to leaders, never leads. With no live lease, lands locally |
| `off` | Exactly today's path, no lease reads |
| `auto` (default) | `on` if `--advertise-url` is set; otherwise `follow` on Postgres and `off` on SQLite and memory |

With these defaults a single SQLite or memory server does exactly what it
does today.

**Mixed versions.** Servers from before this change never read leases.
They land locally and race the leader through the CAS, which is the
existing behaviour between them and one other process. Correctness holds.
Throughput between old servers and the leader is today's, so finish the
rollout (`LeepMixedVersions`) before judging land latency. New followers
never forward to an old server, because only new servers write leases. A
lease row whose `protocol` the follower does not know is treated as no
lease: the follower lands locally.

**Rolling back.** Set `--land-coordination off`. Downgrading the binary
after migration 0005 means restoring a backup, as for any migration
(ADR 0023).

### Batch size and limits
With one coordinator, every land on a ref joins one queue. A batch is
therefore `min(queued, MAX_BATCH)` across the whole fleet: with three
servers and 64 agents each, the leader sees what one server with 192
agents sees today.

`MAX_BATCH` stays 64 but becomes `--land-batch-max` (at most 1024). #100
shows that bigger batches alone do not raise throughput. One server landed
201 lands/s with 64 agents at batch 28, and 125 with 256 agents at batch
64. Replays inside a batch are sequential, at 4 to 5 ms each, and they
were 65 to 80% of the batch. That puts the ceiling of one ref on that
machine near 200 to 250 lands/s, whatever the batch size. A single owner
per ref is what makes the next steps possible:

- **Pipelining.** Replay batch k+1 onto batch k's staged head while k
  stores and swaps. It is safe only because nobody else normally moves the
  ref: if k's swap fails, k+1 is discarded and redone. It hides the store
  and swap, 10 to 25% of a batch.
- **Parallel replay** of lands that touch disjoint paths, combined
  afterwards. This is the main lever for a hot ref.
- **A dedicated lander pool** for about 50k agents. API instances run
  `follow` and lander instances run `on`. Each hot ref's leader then has
  its own CPU, and leases spread refs (and, after ADR 0054, repositories)
  across landers. Having a follow-only server pick a lander for a ref with
  no lease (rendezvous hashing over a heartbeat table of landers) is left
  to that step. It needs no change to the lease table.

These are separate decisions, to be measured on their own.

### Metrics and alerts
None of these metrics carries a ref label, because ref names are
unbounded.

- `leep_land_leases_held` (gauge).
- `leep_land_lease_acquired_total{how=fresh|expired|stolen}`.
- `leep_land_lease_lost_total{reason=renew_failed|epoch_moved|released}`.
- `leep_land_lease_renew_seconds` (histogram).
- `leep_land_forwarded_total{outcome=answered|not_leader|refused|timeout|fallback_local|stolen}`.
- `leep_land_forward_overhead_seconds`: the forward's round trip minus the
  leader's reported land time, carried in a `Leep-Land-Time` response
  header.
- `leep_land_received_forwarded_total`.
- `leep_land_queue_rejected_total`.
- `leep_land_queue_depth` (gauge, the deepest ref).

Alerts, each with a runbook entry:

- `LeepLandForwardFallback`: any `fallback_local` for 10 min. Ticket.
- `LeepLandLeaseFlapping`: more than 6 acquisitions a minute for 10 min.
  Ticket.
- `LeepLandQueueFull`: any rejection for 5 min. Page.

`LeepLandCasLossesHigh` keeps its expression, but its meaning sharpens:
with coordination, a loss means an uncoordinated writer. That is an old
server, an import, a raw ref writer, or a handoff.

Policy skew between servers is already covered by `LeepConfigSkew`. The
leader applies its own policy snapshot, as any server receiving the land
directly would.

## Alternatives considered
The estimates use #100's profile: losing attempts of 754 to 1058 ms, of
which 465 ms is the scan and 7 to 15 ms per replay; winner moves every
70 to 80 ms.

| Option | Expected effect | Why not (alone) |
|---|---|---|
| **Shrink a losing batch** (quarter it per loss) | A losing attempt is still about 5 + 465 (scan) + 8×10 + 30 ≈ 580 ms, far more than 75 ms | Still starves; it cuts throughput for no gain while the scan dominates |
| **Carry the landed-copy scan across attempts** | Scan drops to the rows the head gained since the last attempt (a few ms). A losing attempt of 30 lands is about 290 ms; of 64, about 600 ms | Still several times the winner's period: starves |
| **Both of the above** | Shrinks to 1 or 2 lands at about 40 ms and wins about half the time against a 75 ms period. Each win lands 1 or 2, so a loser with 32 waiting agents clears about 8/s. p99 is seconds, not 20 s | Unfair and worse at 3 servers; it treats the symptom. Keep the scan carry as a cheap hardening step (below) |
| **Sticky routing at the balancer** (all `/v1/land` to one instance) | One server's throughput (201/s at 64), no stalls | All refs on one process. The ref is in the body, so a balancer cannot split by ref. Failover takes the health-check time (often 10 to 30 s) and both instances race while it happens. It needs an L7 rule per deployment, and a lost rule silently brings starvation back. It stays the interim mitigation |
| **Postgres session advisory lock** as leadership (`pg_try_advisory_lock` on a pinned connection) | Released at once when the leader process dies | Postgres only: SQLite, memory and the distributed stores of §11 have none. Discovery still needs a table. One pinned connection is held outside the pool. On a lost host, release waits for TCP keepalive (2 h by default). It breaks under PgBouncer transaction pooling. Ref names hash into 64 bits. Stealing after "connection refused" gives the lease the same fast failover on process death |
| **External election** (etcd, a Kubernetes Lease) | Mature leases | A new runtime dependency and deployment requirement for something the metadata store, which every server already trusts, can do |
| **Static ownership** (consistent hashing of refs over `--peers`) | No store writes | Static membership. A change in membership splits ownership until every config agrees (correct by CAS, but contended), and it needs config on every host |
| **Durable `land_queue` table** drained by the leader | Queued lands survive the leader's death. No server-to-server networking or auth. Answers could be written in the swap's transaction | See below |

**Why not the queue table.** It would cost the store, which is the shared
bottleneck. Each land would add an insert, an answer update and a delete,
plus vacuum churn on a hot table. Today the land path makes about 5
statements per batch, about 0.2 per land at batch 28. The queue would add
2 to 3 statements per land, at least ten times the land path's statements.
It would also add latency: a commit to enqueue, a wake-up for the leader
(LISTEN/NOTIFY on Postgres, polling on SQLite and the distributed stores,
neither of which has NOTIFY) and a wake-up for the waiting follower. That
is about 3 to 6 ms per land on local Postgres and tens of ms with polling,
against about 1 ms for an HTTP hop on a LAN. The durability it buys is
not needed. The client still waits on its own HTTP request, and a land
whose answer was lost is already safe to repeat. That is how a forward
lost with a dead leader is handled. If some deployment cannot let servers
reach each other, a table transport can be added behind the same
`LandForwarder` seam and lease.

**Scan carry, adopted as hardening.** Lands still race in some cases: old
servers during a rollout, imports, a handoff, a partition fallback. So
the `rebased-origin-v1` rows a batch reads stay valid across its attempts.
ADR 0032 kept them per batch attempt. That is not needed: a changeset in a
head's history was stored with its rows in one atomic call (ADR 0031), or,
on a backend without one, before the swap that made it reachable (ADR
0043). Its row, present or absent, cannot change. Only the head's new
ancestors are read on a later attempt. This is independent of the
coordinator and of how retries are recognised. The scan itself stays.

## Test plan
- **Store contract** (`check_land_lease_contract`, memory, SQLite and
  Postgres):
  - acquire on an empty table;
  - refused while another holder's lease is live;
  - the holder re-acquires at the same epoch;
  - expiry lets a new holder in at epoch + 1;
  - a steal succeeds only at the named epoch;
  - renew extends only `(holder, epoch)` and reports the rest;
  - release only by `(holder, epoch)`;
  - the time left comes from the store's clock;
  - concurrent acquires: exactly one wins.

  The migration gets a fixture and the upgrade-path test (ADR 0023).
- **Coordinator unit tests** (`leep-derive`, memory, in-process forwarder):
  - a follower's lands batch with the leader's own in one queue;
  - no CAS loss among coordinated instances with no faults;
  - not-leader, refused, steal and fallback paths;
  - step-down answers `Rerouted` and the land completes elsewhere;
  - the queue bound gives 503;
  - a forwarded land is never forwarded again;
  - a `Repo` without a coordinator is unchanged.
- **Fault sweep** (`fault_tests`, `LEEP_FAULT_SEEDS=300`). The existing
  three-instance land property becomes three coordinated instances over one
  memory store, with forwards that fail, lose their answer after the leader
  applied them, or stall past the client timeout. Renewals fail. The
  leader's tasks are aborted at random suspension points (a kill), or the
  clock pauses past the TTL while the leader is mid-batch (a stale
  leader). On top of the existing checks (every success in history, each
  commit once, the ref log replays to the refs) it checks that every land
  is answered within a bound that depends on the TTL, never left hanging.
- **Crash test** (`leep-e2e`, `tests/crash.rs`, Postgres): a variant with 3
  servers, agents spread across them and `--advertise-url` set. The kill
  schedule picks the current leader, read from `land_leases`, in two of
  three rounds, and a follower in the third. Existing end checks apply, and
  every agent must land again within TTL + 2 s of each kill. One more
  round runs one server in `off` mode (standing in for an old version)
  beside two coordinated ones: correctness only.
- **Bench acceptance** (`leep-land-bench`, rounds as in #100: Postgres,
  `--agents 8,64,256 --duration 20`, three rounds with the server order
  rotated). With 2 and 3 coordinated servers:
  - median lands/s at least 95% of the 1-server median at the same total
    agent count (all processes share one 4-vCPU host, so the extra
    servers' CPU is a cost the 1-server run does not pay);
  - zero stalled agents in every run;
  - land max at most 2× the 1-server max (about 1.2 s at 64 agents, from
    22 to 30 s today);
  - CAS losses at most 1 per 1000 lands, and no 409s;
  - forward overhead p50 at most 2 ms.

  A leader-kill run (SIGKILL the leader every 10 s at 64 agents) records
  the longest gap without any answered land: under 1 s on a kill, under
  TTL + 1 s on a `SIGSTOP`. If the many-agent load generator lands first,
  the same criteria run on it at 1,000 agents and more.

## Consequences
- Lands on a ref are batched by one process however many servers take
  traffic, so adding servers no longer starves agents. Throughput per ref
  stays one leader's, which is the ceiling #100 measured. Raising it is the
  pipelining, parallel replay and lander pool work this enables.
- A forwarded land costs one extra LAN round trip, about 1 ms. The leader
  does all replays for its refs, so its CPU is the per-ref limit. Leases
  spread different refs over servers by first use.
- Servers that coordinate must reach each other's `--advertise-url`. A
  deployment that cannot do this runs `follow` everywhere, or `off`, and
  keeps today's behaviour.
- New schema (migration 0005: `land_leases`, `cluster_keys`), new
  `MetaStore` methods with a contract, new request headers on `/v1/land`
  (`Leep-Land-Forward`, `Leep-Peer-Auth`) and response headers
  (`Leep-Land-Not-Leader`, `Leep-Land-Time`). They are documented in
  `docs/FORMATS.md` when they are implemented. No `/v1` body field changes.
- A valid peer MAC exempts a forwarded land from per-principal limits.
  Anyone who can read `cluster_keys` (anyone with read access to the
  database or a backup) could therefore make a valid-token client's lands
  skip their limits. Identity and policy are still checked from the token.
- Stores that do not implement leases, and SQLite and memory by default,
  are unchanged.
- The `docs/RUNBOOKS.md` advice to pin `/v1/land` to one instance becomes
  unnecessary once every server coordinates. It stays valid, and is
  harmless, during a rollout.

## Open questions for review
1. Lease on first use (chosen) or only after a CAS loss, or a live lease,
   is seen on the ref? On contention saves a write per cold ref. But it
   leaves two leaders racing for up to a cache period at the start of
   every burst, and it is harder to test.
2. Is the limit exemption worth the `cluster_keys` MAC? The alternative is
   to charge forwarded lands at both hops and tell operators that
   per-token in-flight caps now concentrate on a ref's leader.
3. Should a follower ever steal, or only land locally until expiry?
   Stealing on "connection refused" gives sub-second failover after a
   process death. The risk is a port that refuses while the process lives,
   such as a misconfigured `--advertise-url` that points at the wrong port.
   That would flap the lease every second. The flapping alert catches it,
   and startup could check that the advertise URL reaches its own
   `/v1/health` with its holder id.
4. TTL 5 s and renew 1 s are guesses. They bound the stall on host loss
   (about 6 s, lands keep flowing locally) against one renewal statement
   per process per second.
5. When old and new servers share a hot ref, the leader can still starve
   an old server's agents, the same starvation as today. Should a leader
   back off when it sees non-coordinated `land` entries interleaving in the
   ref log, or is "finish the rollout" enough?
