# 0082. Short logs in fewer round trips
Status: Accepted
Date: 2026-11-03

## Context
Agents read far more than they write, and each read is a new `leep`
process. `leep log -n 100` took 17-20 ms end to end against a warm local
server where `git log -n 100` took 6 ms, and 35-55 ms against a server
that had just started. Measured on a 100,000-changeset linear history
(`leep-history-gen`, local Postgres 16, release builds), the time went:

- **The server repeated a cold walk on every request.** The ADR 0030
  read-ahead learns a window by doubling it along skiplists: 22 store calls
  in 7 dependent round trips for 100 entries, and the same again on every
  repeat, because it used only the generations the ancestry cache held,
  never the parents and root trees it also holds. 9-12 ms a request, 27-32
  Postgres transactions.
- **The CLI resolved first.** A log or show of a ref (`leep log main`,
  `leep show main~1`), and every log outside a working copy (the target
  ref), asked `/v1/resolve` and then the log: two requests a process.
- **`leep show <id>` asked again** for a changeset it had shown before,
  though the answer is a function of the id.
- Not the cause: authentication (one blake3 compare), HTTP, JSON encoding
  (0.2-0.3 ms), or connection reuse inside a process (one pooled client
  already).
- A server's first request also opens its Postgres connections; with a
  warm pool and a cold cache the walk still takes 7 rounds of 2-3 ms.

There was no way to see this from outside: the server's metrics aggregate,
and a single slow request did not say where its time went.

## Decision
1. **`Server-Timing`.** `GET /v1/log`, `/v1/changesets/{id}` and
   `/v1/resolve` answer with a `Server-Timing` header: `resolve`, `walk` or
   `fetch` and `encode` phases, and `db` (and `resolve-db`) with the time
   waited for metadata reads and, in `desc`, the store calls and round trips.
   `leep_derive::read_stats` counts them in a task-local scope that the
   read-ahead's spawned batches inherit. The header is for diagnosis; its
   names are not a contract.
2. **The read-ahead uses the ancestry cache.** What a batch reads (parents,
   generations, root trees, and the generations skiplists name) is kept in
   the ancestry cache, as single reads already were. When the walk asks for
   a changeset whose parents the cache holds, those parents are guessed at
   once, transitively within the window, so a stretch the server has walked
   or created before is asked for in one batch. A batch reads no root tree
   the cache holds and no skiplist of a changeset whose parents it holds. A
   walk starts its first batch from the start's cached generation, and on
   the last step its caller expects, does not read parents whose generation
   it already knows. The answer does not change (ADR 0030 point 3); a
   property test checks cold and warm walks against the serial walk on
   random histories with merges and missing rows.
3. **Revisions in the path.** `/v1/log/{id}` and `/v1/changesets/{id}` also
   take a revision as `/v1/resolve/{rev}` does (ref, tag, `~N`/`^N`), with
   `/` sent as `%2F`; a 64-hex id is used as before. The answer is the same
   body as by id plus `Leep-Resolved: <id>`; a log's later pages are asked
   for by that id, since a cursor belongs to the query on the id. The CLI
   sends the revision when it does not already hold the id; a 400 (a server
   from before this, which takes only ids) or 404 (a short id, a typo) falls
   back to resolving as before, with the same hints and errors.
4. **Changesets in the client cache.** `leep show` keeps what
   `GET /v1/changesets/{id}` answered in the machine-wide cache, under
   `changesets/` keyed by id, and checks on read that the changeset hashes to
   its id. Only display reads it: commands that need to know whether a server
   has a changeset still ask that server.

A generation-range query for a linear stretch (`last_merge-v1` says which
ancestors are first-parent) would answer a cold window in one statement, but
needs an index on generation the schema does not have; that is a persistent
format change, not made here.

## Consequences
- On the 100,000-changeset history, a repeated `log limit=100` takes 4.5-5.4
  ms on the server instead of 9.7-12.6 ms, in 1 store call instead of 22
  (3 Postgres transactions instead of 27-32), and `limit=10000` 0.13-0.19 s
  instead of 0.27-0.30 s. End to end, `leep log -n 100` takes 10-15 ms
  instead of 17-20 ms (`git log -n 100`: 6 ms) and `leep --json show <id>`
  of a changeset shown before 5 ms instead of 7 ms. Bodies are byte for byte
  those of the old server (`docs/PERFORMANCE.md`).
- A server's first log of a stretch is unchanged: 7 rounds, plus opening
  pool connections. Warming the pool at start is a separate change.
- The ancestry cache fills faster: a log of 10,000 entries puts 10,000
  entries in it. It is bounded (about 2 x 262,144 entries) and keeps only
  values of immutable rows, like before.
- `/v1` grows: two endpoints accept more inputs, one response header and the
  `Server-Timing` header are added. Nothing persisted changes. Old clients
  are unaffected; new clients against old servers pay one failed request per
  revision log, then work as before.
