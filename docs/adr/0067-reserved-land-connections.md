# 0067. Reserved metadata connections for land leaders
Status: Accepted
Date: 2026-10-31

## Context
Every land on a ref waits for one task: the leader of the ref's batch
(ADR 0032), which reads the ref, the fence and land records, prefetches
the batch's rebase inputs, reads condemned rows and moves the ref with
`cas_ref_logged_chain`. Until now the leader drew on the same Postgres
pool as every request handler.

`leep-land-bench` against a release server on Postgres 16 with
`--meta-pool-size 32` showed what that costs. From 64 to 1,024 agents,
`POST /v1/changesets` p50 rose from 134 ms to 3-4 s, sqlx logged acquires
slower than its 2 s threshold, and the mean `leep_land_lock_wait_seconds`
rose from 22 ms to 4-7 s: the leader queued for connections behind the
creates and reads it was making wait. Each create made this worse by
reading its inputs with six queries at once (`read_create_inputs`), so one
request held up to six connections.

Lease calls already have connections of their own (ADR 0056) for the same
reason: a renewal must not wait behind data traffic.

## Decision
- **A land lane.** `leep_core::lane` holds the lane of the running task,
  `Shared` or `Land`. It is task-local without an async runtime: a
  `Scoped` future sets a thread-local while it is polled and restores it
  after, so only code running inside that poll sees it. The land code
  spawns each batch leader in `Land`; the batch's parallel prefetch tasks
  inherit the lane they were spawned from.
- **A reserved pool.** `PostgresMetaStore` keeps a second data pool,
  opened on first use, and answers every data call made in `Land` from it.
  Its size is `--meta-land-pool-size` (`LEEP_META_LAND_POOL_SIZE`),
  default an eighth of `--meta-pool-size` and at least 4: one leader per
  busy ref plus its prefetch reads. `0` turns the reserve off and the lane
  shares the data pool. Its connections get the data pool's statement
  bound, as the leader's statements did before. SQLite and the memory
  store ignore the lane: SQLite has one writer at a time whatever the pool.
- **Creates hold one connection at a time.** `read_create_inputs` reads the
  parents with `get_changesets`, then every derived kind it needs (blob
  condemnations, generations, root tree, skiplist, last merge) with one
  new `MetaStore::get_derived_kinds` call. Postgres answers that with one
  `UNION ALL` statement of the existing per-kind query, so each branch
  keeps its plan; the trait default calls `get_derived_many` per kind.
  A failed read leaves every kind to the caller's single reads, as a
  failed batch did before.
- **One blob read where one suffices.** `BlobStore::get_stored` is `get`
  that answers only from durable storage. Its default is `get` then
  `contains`, what creation did; the filesystem and object stores answer
  a loose read with that read alone, and the blob cache checks a hit with
  the inner store's `contains`.
- **Pool gauges.** `leep_meta_pool_connections{pool, state}` reports open
  connections of the `data` and `land` pools, `in_use` and `idle`.

## Consequences
- Request handlers can fill the shared pool without delaying lands that
  are already queued; the leader waits only for other leaders.
- A server opens up to `--meta-land-pool-size` more Postgres connections
  (4 by default). Operators sizing against `max_connections` count them.
- Ref moves made outside a batch (raw ref updates, imports) stay on the
  shared pool; they are not what every land waits for.
- Any future work that a land batch depends on must run inside the lane,
  or it loses the reservation: a task the leader spawns must be wrapped in
  `lane::inherit`.
- A backend that wants a reservation of its own reads `lane::current()`.

## Alternatives considered
- **A second `MetaStore` instance passed to the coordinator.** Every call
  the leader makes goes through `Repo::meta()`, shared by all code paths;
  threading a second store through the land code would touch every read
  the batch makes and the derive caches that sit in front of them.
- **A semaphore over one pool.** Request handlers acquire connections
  inside sqlx, so a permit taken before each call bounds callers, not
  connections, and adds a wait to every request.
- **Raising `--meta-pool-size`.** Moves the point where the leader starves
  without removing it, and costs Postgres backends for every request.
