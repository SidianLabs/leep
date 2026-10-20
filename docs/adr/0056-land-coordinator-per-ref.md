# 0056. One land coordinator per ref across servers
Status: Accepted
Date: 2026-10-20

## Context
Group commit (ADR 0032) and staged batches (ADR 0043) made lands on one ref
cheap inside one server process: a leader task replays a batch of queued
lands and moves the ref once. Across processes nothing changed. Instances
sharing one metadata store raced through `cas_ref`, and a lost swap redid
the whole batch.

`docs/PERFORMANCE.md`, "Several servers on one ref" (#100), measured the
cost with 2 and 3 servers on one Postgres. Adding servers added no
throughput, and it starved agents. In all 18 multi-server runs one instance
answered every land inside the 20 s window. The other instances' agents
waited 21 to 30 s, or got 409 after 32 attempts:

| Servers | Agents | Lands/s | Land p99 (ms) | Land max (ms) | Stalled agents |
|---|---|---|---|---|---|
| 1 | 64 | 201 | 460 | 590 | 0 |
| 2 | 64 | 170 | 564 | 25702 | 32 of 64 |
| 3 | 64 | 170 | 21480 | 23051 | 43 of 64 |
| 3 | 256 | 146 | 24401 | 24584 | 171 of 256 |

The loser never recovered. The winner moved the ref every 70 to 80 ms, and
a losing attempt took 0.75 to 1 s from reading the head to the swap. Of
that time, 465 ms was a landed-copy scan (ADR 0021) that read again, on
every attempt, everything the head had gained, and the rest was a full
batch of replays onto a head further away. The interim advice was to route
`POST /v1/land` to a single instance at the balancer. That sends every
ref's lands to one process, fails over only as fast as the balancer's
health checks, and quietly brings the starvation back if the rule is ever
lost.

Constraints:

- Refs move only through `cas_ref` and its logged variants. A coordinator
  may decide where lands are batched. It must never be what correctness
  rests on.
- Every land keeps today's answer and ref log entry.
- `land` stays idempotent: the landed-copy scan stays, and so does the
  client land id of ADR 0057.
- SQLite and memory serve one process and need nothing new.

A first draft of this ADR was reviewed. The review asked for a simpler
variant, which this ADR now records:

- a follower never lands locally under a live lease;
- a live lease is never stolen;
- no shared MAC key;
- an allow-list for peer addresses;
- `on`/`off` modes only.

"Review changes" below lists the differences.

## Decision

### Lease
Migration 0006 (Postgres and SQLite) adds one `leases` table for every kind
of work a server can hold, so the indexer service of ADR 0058 reuses it
without a migration of its own. It is numbered after ADR 0057's migration
0005:

```sql
leases(scope TEXT NOT NULL, repo_id BIGINT NOT NULL DEFAULT 1, name TEXT NOT NULL,
       shard INTEGER NOT NULL DEFAULT 0, holder TEXT NOT NULL, address TEXT NOT NULL,
       protocol INTEGER NOT NULL, meta TEXT NOT NULL DEFAULT '', epoch BIGINT NOT NULL,
       renewed_at BIGINT NOT NULL, expires_at BIGINT NOT NULL,
       PRIMARY KEY (scope, repo_id, name, shard))
```

- `scope` names the kind of work. Land coordination uses `land`, with the
  ref as `name` and shard 0. Each scope picks its own timing per call (land:
  2 s TTL renewed every 500 ms; ADR 0058 plans 15 s and 3 s).
- `meta` is free text a holder publishes for its scope; lands leave it empty.
- `holder` is random per process start.
- `address` is the holder's `--advertise-url`.
- Times are milliseconds by the store's clock: `clock_timestamp()` on
  Postgres, the host clock on SQLite.
- `repo_id` is ready for ADR 0054.
- The migration only creates a table, so it tolerates old servers.

`MetaStore` gains generic `supports_leases`, `lease`, `acquire_lease`,
`renew_leases` and `release_lease`, keyed by `LeaseKey` (scope, repository,
name, shard); `LandCoordinator` wraps them for the `land` scope. Each is one
conditional statement, plus a read of the row it settled. The defaults
refuse, and a store that refuses is left uncoordinated.
`check_lease_contract` covers memory, SQLite and Postgres, for the land
scope, a sharded scope with `meta`, and keys that differ only in scope,
repository, name or shard. All scopes share the store's lease connections.

- **Take.** `INSERT ... ON CONFLICT DO UPDATE ... WHERE` the row is the
  claimant's own, or `expires_at` has passed, or `renewed_at` is older than
  the caller's `stale_after`. The coordinator passes the TTL as
  `stale_after`, so there is one rule: a lease is taken over one TTL after
  its last renewal. A live lease of another
  server is never taken: there is no stealing, even when its address does
  not answer. The epoch grows by one whenever the holder changes. A
  release sets both times to 0 instead of deleting the row, so the epoch
  never falls.
- **Timing.** The TTL is 2 s (`--land-lease-ttl`), and it means what it
  says: another server may take a lease once its last renewal is one TTL
  old. One statement renews every lease a process holds, every 500 ms
  (`--land-lease-renew`, at most half the TTL). Failover after a crash
  therefore comes from the short lease. After a graceful shutdown it comes
  from the release: a server gives up its leases when shutdown starts,
  before it drains its requests.
- **A connection of its own.** Lease calls never wait behind data traffic.
  On Postgres they use a pool of two connections of their own, opened on
  first use; a renewal that queued behind a saturated data pool for longer
  than the takeover window would hand the lease over with nothing wrong.
- **The holder's own view.** The store reports the time left: the lesser
  of `expires_at` and `renewed_at + stale_after`, minus now. A holder trusts
  its lease until the moment it sent its last successful renewal, plus that
  reported time, minus one renew interval. The clock it trusts is the
  store's answer, not its own monotonic clock alone. When the view has run
  out before the periodic renewal comes round, the holder renews once on
  the spot before it gives the lease up.
- **Lifetime.** A server takes a ref's lease lazily, on the first land it
  receives for a ref nobody leads. It releases the lease after 30 s
  without a land.
- **No fencing in the swap.** Imports, raw `POST /v1/refs`, `leep undo` and
  servers with coordination off all move refs without a lease. A stale
  leader's swap either loses, because the ref moved, or wins legitimately,
  because it had not moved. Both are correct.

### Routing a land (`Repo::land_coordinated`)

```
loop, for at most 30 s (then 409, which clients retry):
  this server holds a lease whose view is valid, or renews it now → batch it here
  another server holds a live lease → forward it there:
      answered      → relay the answer
      Not-Leader    → route again
      no answer     → wait one renew interval, or less if the lease changes; route again
      (while a forward is in flight, a change of holder or epoch, or expiry, abandons it)
  no live lease → take it → batch it here
  the store cannot be read → forward on the last lease read, while its own time left lasts
  no live lease known, none can be taken (the store failing) → batch it here, uncoordinated
  this server is shutting down, no other live lease → 503 Not-Leader; the client sends it again
```

- **H1, never batch locally under a live lease.** A follower whose forward
  gets no answer asks the holder again, or its successor once the lease
  changes. The land may still be in the holder's batch, and batching it in
  two places is the race this ADR removes. When the store cannot be read,
  a server forwards on the last lease it read, for as long as that
  lease's reported time left has not run out. It is batched locally only
  when no live lease is known and none can be taken.
- **Shutdown.** A server that has started shutting down never batches a
  land itself again. A land it does not hand to another live holder,
  including one its own batches handed back, is answered 503 with
  `Retry-After: 1`; `leep land` sends it again under its land id.
- **Repeats are safe.** `land` finds its earlier landing (ADR 0021, and the
  land id of ADR 0057).
- **Step-down.** A leader asks `still_leading` before every attempt of a
  batch. A leader whose lease is gone, after a long stall for example, stops
  retrying. It hands back every land it had not settled: a land that arrived
  directly is routed again by its own handler, and a forwarded one is
  answered 503 `Leep-Land-Not-Leader`.
- **Forwarded lands.** A land forwarded here is never forwarded again. It
  is answered Not-Leader whenever this server does not lead the ref,
  including the uncoordinated case.
- **Land ids.** Land ids from ADR 0057 are settled per land inside a
  batch. Coordination neither reads nor changes them. It only has to carry
  the client's body unchanged, which it does.

### Forwarding over HTTP
- **Request.** A follower sends `POST {address}/v1/land` with the client's
  body byte for byte, so fields the follower does not know arrive intact.
  It also sends the client's `Authorization` header and
  `Leep-Land-Forward: v=1; epoch=N; from=HOLDER`. The holder authenticates
  the client and applies its own land policy snapshot, so forwarding grants
  no identity. The ref log actor is the client's token, as before.
- **H5, allow-list.**
  - Lands go only to an address that starts with a `--peer-url-prefix`, or
    whose host is an IP address in a `--peer-cidr`.
  - Redirects are refused, and so are URLs with credentials or a query.
  - Plain `http://` is refused unless the host is loopback or private, or
    `--peer-allow-plain-http` is set. Host names are never resolved for
    this check.
  - A lease naming any other address is not forwarded to. The land waits
    as for an unreachable holder and is answered 409 after 30 s.
  - At startup the server checks `--advertise-url` against its own list,
    and checks that `GET {advertise-url}/v1/health` answers with its own
    `Leep-Instance` header. A wrong URL stops the server instead of
    stranding lands.
  - `--peer-url-prefix` is compared as raw text, so it is written with the
    port and trailing slash (`https://leep-1.internal:7070/`).
  - A server whose allow-list has host names only warns at startup: no
    request can be recognised as a peer's, so forwarded lands are charged
    the concurrency limit twice and may be forwarded again.
- **H3, no shared key.** The forward header is an unauthenticated hint. It
  counts only on a request whose peer address is a `--peer-cidr` address or
  a prefix's IP host. From anywhere else it is ignored and the request is
  an ordinary client land. Peer networks must therefore exclude load
  balancers and sidecar proxies, or a client behind one could set the
  header and skip its concurrency limit.
- **Threat model.** A forward carries a client's token to another server.
  Lease addresses come from the metadata store, so whoever can write the
  store could point a lease elsewhere; the allow-list keeps tokens within
  the servers the operator listed. That adds nothing: write access to the
  store already owns every ref. The store holds no secret for coordination.
- **Limits for forwarded lands.** The follower charges the token's rate and
  concurrency limits as for any request. The leader skips only the
  per-token concurrency permit for a recognised forward, since the follower
  holds it, and charges the rest.
- **Transport.** `reqwest` with rustls: pooled keep-alive connections, a
  1 s connect timeout and no response timeout of its own. The client's
  `--request-timeout` bounds the land on both servers.

### Modes and rollout
`--land-coordination on|off`, default `off`.

- `on` needs Postgres or SQLite, `--advertise-url` and an allow-list. It
  stays off by default, and is recommended as the multi-server default
  only once throughput is measured at 95% of one server's or better with
  the servers on separate hosts (see Testing).
- Servers from before this change, and servers with `off`, never read
  leases. They land locally and race the leader through the CAS: correct,
  but contended. During a rolling upgrade, keep `/v1/land` pinned to one
  instance at the balancer, as `docs/RUNBOOKS.md` already says. Turn
  coordination on everywhere, then drop the rule.
- To back out, set `off`. Leftover leases expire in 2 s. Downgrading the
  binary after migration 0006 means restoring a backup (ADR 0023).
- A tier where API servers only forward and dedicated landers only lead is
  future work. Today every `on` server can lead.

### Metrics and alerts
None of these carries a ref label.

- `leep_land_leases_held` (gauge).
- `leep_land_lease_acquired_total`.
- `leep_land_lease_lost_total{reason=taken|idle}`.
- `leep_land_forwarded_total{outcome=answered|not_leader|failed|lease_changed}`,
  with `reason=not_allowed|connect|timeout|reset|redirect` on `failed`.
- `leep_land_lease_read_failed_total`.
- `leep_land_forward_gave_up_total`.
- `leep_land_received_forwarded_total`.
- `leep_land_rerouted_total`.
- `leep_land_routed_total{route=uncoordinated}`.

New alerts, each with a runbook:

- `LeepLandForwardFailing`: failed forwards above 0.1/s for 10 min. Ticket.
- `LeepLandLeaseFlapping`: more than 6 acquisitions a minute for 10 min.
  Ticket.

With every server coordinating, `LeepLandCasLossesHigh` now means an
uncoordinated writer.

### Scan carry (separate change, merged first)
A batch redone after a lost swap keeps the `rebased-origin-v1` rows that
earlier attempts read from the store. Its landed-copy scans then read only
the head's new ancestors. Only stored rows are carried. A row is stored
with its changeset before any swap can make that changeset reachable, and
it never changes. What an attempt staged itself is dropped when the
attempt ends: its swap lost or its write failed, so it may never have been
stored. ADR 0032's "Shared reads" is amended accordingly. This hardens the
races that remain: old servers, imports, handoffs.

### Batch size
Unchanged: at most 64 lands per batch. With one coordinator, the batch is
whatever is queued across the whole fleet. Replays inside a batch are
sequential, so one ref's ceiling is one leader's CPU, about 200 to 250
lands/s on #100's machine. Pipelined batches, parallel replay of disjoint
lands and a lander tier are the next steps, each to be measured on its own.

## Alternatives considered
| Option | Why not |
|---|---|
| Shrinking a losing batch, carrying the scan | The scan carry is adopted as hardening. Even with both, a losing attempt is 40 to 300 ms against a winner that moves every 75 ms: unfair, and worse with 3 servers |
| Sticky routing at the balancer | One process for every ref; failover only as fast as health checks; silently lost. Kept as the rollout measure |
| Postgres session advisory lock | Postgres only; no address discovery; a pinned connection; release waits for TCP keepalive after a host loss; breaks under PgBouncer transaction pooling |
| External election (etcd, a Kubernetes Lease) | A new dependency for what the store, which every server already trusts, can do |
| Durable `land_queue` table drained by the leader | 2 to 3 more statements per land on the shared bottleneck (against about 0.2 today), 3 to 6 ms of wake-ups. Its durability is not needed, because lands are idempotent and clients retry |
| Stealing a lease whose holder refuses connections (first draft) | A wrong port flaps the lease every second; replaced by no stealing, a short TTL, release on shutdown and the startup self-check |
| Landing locally when a forward times out (first draft) | Batches the same ref in two places while the holder may still be landing it: the race this ADR removes |
| A shared MAC key in the store to authenticate forwards (first draft) | A secret readable by anyone with database read access, guarding only a limit exemption; replaced by the address allow-list |

## Review changes
These are the differences from the first draft, made at review:

- H1: no local landing under a live lease.
- H2: no stealing; leases are taken only when expired or stale; the
  startup self-check.
- TTL 2 s and renew 500 ms, down from 5 s and 1 s.
- H7: the holder's view comes from the store's answer.
- H3: no `cluster_keys` table and no MAC.
- H5: the allow-list.
- Modes `on` and `off` only.
- Migration 0006.
- The per-ref queue bound and `--land-batch-max` of the draft are dropped
  from this change.

The code review of the implementation added:

- Lease calls on connections of their own, after renewals queued behind a
  saturated pool handed leases over under load.
- One takeover rule, the TTL, instead of a separate window of twice the
  renew interval.
- When a lease cannot be read, the last lease read is used while it is
  still live, instead of batching locally (a gap in H1).
- A server shutting down answers lands 503 instead of batching them.
- The startup warning for an allow-list of host names, failure reasons on
  `leep_land_forwarded_total`, and the peer-network and prefix guidance.

The indexer service's design review (ADR 0058) asked for one lease table
for every kind of work instead of `land_leases`, so the indexer needs no
second migration: `leases` keyed by scope, repository, name and shard, a
generic `MetaStore` API with timing per call, one contract for both scopes,
and one set of lease connections shared by all scopes. Still migration
0006.

## Testing
- **Store contract** on memory, SQLite and Postgres: take, keep, refuse a
  live lease, take over a stale or expired one at the next epoch, renew
  only at `(holder, epoch)`, release, racing claimants.
- **Coordinator tests** (`land_coord_tests.rs`). Simulated servers share
  one memory store whose lease clock is tokio's, and forwards go straight
  to the other `Repo`:
  - no races without faults: every land on its batch's first attempt, the
    lease never changes hands;
  - forwards that are reset or lose their answer after the leader landed:
    nothing lands twice, no agent is failed, and no server lands without
    the lease;
  - a leader paused past its lease mid-batch;
  - a leader killed at a random point of a batch: every agent lands within
    TTL + 2 s;
  - a wrong advertise address: the lease never changes hands, and
    followers give up with 409 instead of landing;
  - release on shutdown and when idle, and a shutting-down server
    answering Not-Leader instead of batching;
  - lease reads failing on every server but a healthy leader (a 300-seed
    sweep): lands keep reaching the leader and none is batched elsewhere;
  - a 300-seed sweep with store faults, lost and reset forwards, and a
    leader killed or paused.

  Every case ends with the fault sweep's checks: nothing reported lost,
  nothing twice, and the ref log replays.
- **End-to-end.** The crash test gains three servers on Postgres. Each
  round SIGKILLs the lease holder and requires every agent to land again
  within TTL + 2 s; the longest wait measured was 1.25 s. The usual
  exactness checks follow: every commit once, one ref move per land. A
  second test starts a server with a wrong `--advertise-url`, or one
  outside its allow-list, and expects it to refuse.
- **Bench.** `docs/PERFORMANCE.md`, "Several servers on one ref", has the
  multi-server results: 2 and 3 servers, 8, 64 and 256 agents, three
  rotated rounds on a shared 4-vCPU host.
  - Met: no stalled agents in any run (4 to 171 per run before), land max
    at most 1.2 times one server's (22 to 30 s before), no 409s, no lost
    swaps. Six further runs at 256 agents took the lease once each and
    ended at epoch 1: no handover without a failure.
  - Not met: throughput was 75 to 91% of one server's at the same total
    agents, against the 95% target. Every process shares the one host, and
    a forwarded land costs the follower an extra request and answer on the
    same CPUs as the leader's replays. The target must be re-measured with
    the servers on separate hosts before `on` is recommended as the
    multi-server default.
  - The bench also found that a leader replayed lands created on other
    servers with cold caches: parents and generations were read one at a
    time, and the rebase-link prefetch was skipped. A batch now learns them
    in one batched read first. That raised throughput through a follower
    from 122 to 147 lands/s at 8 agents.

## Consequences
- However many servers take traffic, a ref's lands are batched in one
  place, so adding servers no longer starves agents.
- A forwarded land costs one extra round trip between servers.
- Servers that coordinate must reach each other's `--advertise-url`
  directly, and their allow-lists must cover each other.
- New schema (migration 0006) and new `MetaStore` methods with a contract.
  New headers between servers: `Leep-Land-Forward`, `Leep-Land-Not-Leader`,
  `Leep-Instance` on `/v1/health`. No `/v1` body changed.
- A client token is sent to peer servers, on the allow-list only.
- Rate limits are charged on both the forwarding and the coordinating
  server. A token whose lands all funnel through one leader is
  rate-limited by that server's bucket for the ref's lands. Size per-token
  rate limits for that.
