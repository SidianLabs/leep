# 0084. A warm floor of Postgres connections in servers
Status: Accepted
Date: 2026-11-03

## Context
A server built every Postgres pool lazily (the data pool opened one
connection to check the database, the land, index and lease pools none), so
each first use of a connection paid TCP, authentication and the
`after_connect` statements (`search_path`, `statement_timeout`). A fresh
server's first `leep log -n 100` took 20-60 ms more than a warm one, of
which about 10-15 ms was opening connections (ADR 0082). sqlx's defaults
then close a connection idle for 10 minutes or older than 30, so a server
that went quiet paid it again.

sqlx 0.9 has `min_connections`, but keeps it only by reopening a
connection right after its reaper closed one: a pool built with
`connect_lazy_with` opens nothing up front while an idle or lifetime
timeout is set, `connect_with` opens the floor as part of the open and
fails it if the floor cannot be reached, and a reopen that fails is not
tried again until another connection is reaped.

## Decision
1. **A floor per pool.** `--meta-pool-min N` (`LEEP_META_POOL_MIN`,
   default 4) keeps `N` connections of the data pool open, `N` of the land
   reserve (all of it at the defaults), and one each of the index and lease
   pools, whose single loops use one connection at a time: 10 at the
   default. Each is capped by its pool's size. `0` is the old lazy
   behaviour.
2. **Servers only.** Only `leep-server`'s serve path passes the flag.
   Commands that open a store briefly (`gc`, `repack`, `migrate`, `backup`,
   `verify`, ...) and `LanePools::default()` keep a floor of 0, so they
   open no connection they do not use.
3. **Opened before serving.** The store opens the floor after its schema
   check and before the server binds its listener, all pools at once,
   within 10 s. Past that, or on an error, the server logs a warning and
   starts anyway; the database being unreachable at all still fails the
   start as before, on the data pool's first connection (retried for
   sqlx's 30 s acquire timeout, then the server exits). So a server that
   answers `/v1/ready` has its floor open, or has said why not.
4. **Kept.** The pools set `min_connections`, so sqlx reopens a floor
   connection as soon as its reaper retires one (idle and lifetime
   timeouts stay at sqlx's defaults: connections above the floor still go
   away when idle). A keeper task looks every 30 s for an idle pool below
   its floor (after a failed reopen or a database restart) and tops it up;
   a pool with connections in use is left alone. The keeper stops with the
   store.
5. **Warmed, not only open.** A Postgres backend loads a table's catalog
   entries on its first statement on it, 1-2 ms a table. sqlx hands out
   idle connections first in, first out, so a few calls on a fresh floor
   each land on a different cold backend: a first create was 3-7 ms
   slower with a floor of 4 than with one lazily opened connection that
   the open's own queries had warmed. So every new connection of a pool
   with a floor plans `SELECT 1 FROM changesets, refs, ref_log, derived
   LIMIT 0` (the lease pool: `leases`) in `after_connect`, which loads
   them without reading a row; its error before the first migration is
   ignored.

## Consequences
- A fresh server's first requests, and the first after a quiet spell, no
  longer open connections (measured in docs/PERFORMANCE.md, "Warm floor of
  Postgres connections").
- Every server holds its floor even when idle: count `N` + min(`N`, land
  reserve) + 2 per server against Postgres's `max_connections`, besides
  the maxima. The maxima are unchanged.
- The index floor is held by a server that does not warm indexes too; one
  idle connection was judged not worth another flag.
- sqlx still retires each floor connection after 10 minutes idle and opens
  its replacement in the background, so the floor costs a reconnect per
  connection every 10 minutes on a quiet server, off the request path.
