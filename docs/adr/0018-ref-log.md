# 0018. Ref log and `leep undo`
Status: Accepted
Date: 2026-10-15

## Context
Refs are the only mutable state, yet nothing recorded how they moved. A bad
`land` or a mistaken `branch -D` could not be recovered without guesswork:
the old value was in no table, and the changeset it pointed at was reachable
only if someone remembered its id.

## Decision
- **A persisted log of every ref change.** The `ref_log` table gets one row
  per create, move or delete: a store-wide increasing `seq`, the ref name, the
  old and new target (null for create and delete), unix time by the server's
  clock, the actor (the token's name, null when authentication is off), a
  short `operation` label and an optional message.
- **Written in the same atomic step as the change.** `MetaStore::cas_ref_logged`
  takes a `RefUpdateContext` (actor, operation, message) and appends the entry
  in the same SQL transaction (SQLite, Postgres) or under the same lock
  (memory) as the conditional update. A lost CAS, and a CAS that changes
  nothing (`expected == new`), write nothing. `cas_ref` stays as a provided
  method that logs with operation `cas` and no actor, for callers with no
  better context (tests, benchmarks). `land` records `land` (via
  `Repo::land_as`, which `land` calls without an actor), the git bridge
  records `import`, and `Repo::cas_ref_checked` takes the context from its
  caller, so the server passes the principal.
- **Order.** SQLite serializes writers, so `seq` is commit order. Postgres
  takes a transaction-scoped advisory lock after the ref update and before
  the insert, so `seq` is also commit order there and a reader paging by
  `seq` never skips a row that commits late. The lock is taken last and
  nothing waits on a row lock while holding it, so it cannot deadlock. Moves
  of one ref are ordered by the ref's own row lock regardless, so replaying
  one ref's entries oldest first always ends at its current value.
- **Reading.** `MetaStore::ref_log(name, before_seq, limit)` returns entries
  newest first. `GET /v1/reflog` and `GET /v1/reflog/{name}` take `before` and
  `limit` (default 50, at most 1000) and need only a read token.
- **Labels.** `POST /v1/refs/{name}` accepts optional `operation` and
  `message` (new `#[serde(default)]` fields, so `/v1` only grew). The actor is
  always the token's name. The label is lowercase `a-z 0-9 - _ .`, up to 32
  bytes; `land` and `import` are reserved so a client cannot pose as the
  server; a message is up to 256 bytes with no control characters.
- **Schema.** The table and its `(name, seq)` index are created with
  `CREATE ... IF NOT EXISTS` at startup, like the existing tables, so a
  database made by an earlier version upgrades by opening it (a test opens a
  database built with the old schema). Refs that moved before the upgrade have
  no entries; the first logged move of such a ref records its previous value.
- **`leep reflog [REF] [-n N]`** prints entries newest first.
- **`leep undo [REF] [--to SEQ]`** is a client over existing endpoints. It
  reads the latest entry of REF (or entry SEQ), then compare-and-swaps REF
  from its current value to that entry's old value with operation `undo`. It
  needs no server endpoint of its own: the swap is atomic in the store, and
  its `expected` is the value just read, so a move in between makes the swap
  lose and the CLI reports a 409 instead of overwriting. Without `--to` the
  CLI also checks that the latest entry's new value is the current one, so it
  never reverts a state the log does not describe. Because the swap goes
  through `cas_ref_checked`, every rule still applies: names are validated,
  tags are never moved, a protected ref cannot be deleted, and history the
  land policy has not seen (unsigned or unreviewed) cannot be put back on a
  protected ref. Undoing a delete recreates the ref, undoing a create deletes
  it. The redo is another undo, of the undo entry. The working copy is not
  touched; the CLI suggests `leep sync`.
- **Retention.** The log is unbounded and never pruned in this version. Rows
  are small and written only when a ref moves. Entries name changesets, and
  changesets are never deleted, so blob garbage collection is unaffected: it
  marks from every stored changeset, not from refs or the log.

## Consequences
- Every ref move since the upgrade is recoverable and attributable. A
  `branch -D` or a land that should not have happened is one `leep undo`
  away, subject to the same policies as any other move.
- Every `MetaStore` backend implements `cas_ref_logged` and `ref_log`, and
  `check_meta_store_contract` checks both.
- Servers older than this change do not write entries, so run the upgrade on
  every server before relying on the log; mixed fleets leave gaps.
- A client can label its own moves, but not as `land` or `import`, and cannot
  choose the actor.
- Undo of a protected ref can refuse for a reason the user did not cause (a
  policy added after the move). That is the intended behavior, not a bug.
