# 0057. Land ids make a retried land idempotent at any age
Status: Accepted
Date: 2026-10-20

## Context
A land whose commit applied but whose answer never reached the client (a
timeout, a killed server, an error after `COMMIT`) is sent again by the
client. ADR 0021 made the retry recognise its earlier result by looking for
the server's rebased copy of the changeset (`rebased-origin-v1`) among the
head's ancestors, scanning at most 1024 changesets (`LANDED_COPY_SCAN`), and
recorded that past the window a late retry lands twice.

At the rates Leep is built for that window is short. With a few thousand
agents landing several times a second each, 1024 lands pass in well under a
minute, and a client whose first attempt hung on a request timeout, then
waited out a backoff and a server restart, can easily retry later than that.
The second land replays the stack onto a head that already has it and adds
an empty copy of every commit, or a false conflict once the files changed
since. Raising the scan only moves the edge and makes every replaying land
read more.

## Decision

### The request names the land
`LandRequest` gains the optional `land_id`: 16 to 32 bytes, lowercase hex on
the wire, picked at random by the client once per logical land and sent
unchanged on every retry of it. Absent (every client from before this ADR),
nothing changes. An invalid id is 400.

### The server records it with the move, in the same transaction
`RefUpdateContext` gains `land: Option<LandKey>` (the id and the changeset
the client asked to land). `land` puts it on the move it makes, and the
backends store it in the log entry of that move: two new nullable
`ref_log` columns, `land_id` and `land_changeset` (migration 0005, both
backends), written by the same `INSERT` as the rest of the entry, so the id
exists if and only if the move does. A unique partial index
`ref_log_land_id ON ref_log (name, land_id) WHERE land_id IS NOT NULL`
makes an id unique per ref and is the lookup path. The memory store keeps
the same under the lock it holds for the ref and its log.

Contract (`check_meta_store_contract`, every backend):

- `MetaStore::find_lands(name, ids)` answers, per id, the recorded
  `LandRecord { changeset, landed, seq }` or nothing, at any age. The default
  implementation is an error, like `list_derived`, so a wrapper that forgets
  to forward it fails a keyed land loudly instead of silently disabling the
  check.
- A swap that would happen with a land id the ref already recorded fails
  with `Conflict` and changes nothing. A swap that loses is just lost.
- One id twice in a chain, or a land id on a delete, is `Invalid`.
- Moves without a land id write exactly the columns a v4 schema has.

### `land` answers a recorded id with its result
Each batch attempt reads `find_lands` for the keyed lands in it, after it has
read the head, beside its other prefetches (one query per attempt, none for
a batch without ids). A land whose id is recorded is answered
`Landed { landed, rebased: landed != changeset }` with that record and moves
nothing; one whose id was recorded for a different changeset is 400. A land
whose id appears earlier in the same batch, on a land that moves the ref,
waits for that batch's commit and gets the same answer. Each of these
settles only its own land; the rest of the batch goes on. A lookup that
fails holds back only the lands with ids, to the next attempt, and fails
them only after three failures in a row (the budget of failed commits).

Ordering makes this race-free across servers. The lookup runs after the
head read. If the original's move committed before the lookup, the lookup
sees it. If it commits after, the head this attempt read does not contain
it, so this attempt's swap from that head loses, and the next attempt's
lookup finds it. The exception is a ref that came back to the head this
attempt read (another server landed the id, then the ref was undone): the
swap would succeed, and the unique index stops it. A move that would record
a taken id fails with `Conflict` and applies nothing, so `land` treats it as
a lost race, not a failed commit: it re-reads the head and redoes the
attempt, whose lookup answers that land, without spending the commit-error
budget.

The landed-copy scan stays as it is, for clients that send no id and for
ids recorded by nothing (a land answered "already landed" moves nothing and
records nothing; a retry of it is found the same way).

### Clients
- `leep land` saves the id in the working copy's `state.json`
  (`pending_land`: changeset, ref, id) before sending, and clears it once it
  has an answer or an error that sending again cannot change. Running
  `leep land` again for the same changeset and ref reuses the id, if the
  server advertises `land-id` (see "Upgrade and mixed versions"). Within one
  run, `Client::land_resending` sends a keyed land again, with the same id,
  after a timeout, a lost connection, a 5xx or a lost race (three more times,
  0.5 to 2 s apart). A land without an id is still never resent.
- The MCP server keeps the id per (changeset, ref) until the land is answered,
  so the call that its own error message asks for (the same changeset, or the
  same `request_id`) reuses it. Amended 2026-10-21: a land that carries a
  `request_id` instead sends an id derived from it, the ref, and the
  sender's identity: the first 16 bytes of `blake3("leep-mcp-land-id\0" ||
  author || "\0" || session || "\0" || ref || "\0" || request_id)`, with the
  agent session empty when unset (docs/MCP.md). It is the same in every MCP
  process of one author and session, so a retry after an MCP restart is
  answered from the record too, and a `request_id` need only be unique per
  author (and session). A fresh changeset refused with `land_id_reused` is
  answered with the recorded land. A conflict moves nothing and so records
  nothing: the id stays free, and the call that resolves the conflict lands
  under it.
- The git mirror's land branches and other API callers send no id yet and
  keep the scan.

### Retention
An id lives as long as its log entry, and the ref log is never pruned
(OPERATIONS.md, "Ref log"). The cost is 16 to 32 bytes plus a 32-byte
changeset per keyed land, and an index entry: about 120 bytes a land, or
roughly 1 GB per ten million lands on Postgres. If the log is ever pruned,
pruning an entry drops its id with it, and a retry older than that falls
back to the scan, which is the behaviour before this ADR. An id survives the
ref being deleted and created again, and an undo of the land: a retry of a
land that applied and was undone afterwards is answered with its result,
which is what happened, and does not land it again.

## Upgrade and mixed versions
Migration 0005 adds two nullable columns and an index. Servers of v4 never
read or write the columns, so it may be applied beside them. On Postgres the
`ALTER TABLE ... ADD COLUMN` without a default is a catalog change, but the
index build reads `ref_log` once under a `SHARE` lock, which blocks every
ref move (every land) for the time it takes. So the migration is written
with `IF NOT EXISTS` (`ADD COLUMN IF NOT EXISTS` and `CREATE UNIQUE INDEX IF
NOT EXISTS` on Postgres; the index only on SQLite, which cannot build
concurrently anyway), the one exception to "only 0001 is idempotent": a
large deployment adds the columns and builds the index by hand first with
`CREATE UNIQUE INDEX CONCURRENTLY`, which does not block writes, and the
migration then finds them done (OPERATIONS.md, "Upgrading").

**Upgrade servers before relying on ids.** A v4 server ignores `land_id`
(unknown fields are dropped): it lands without recording the id, and a
repeat is recognised only by the landed-copy scan. That is harmless for a
resend within seconds, but a resend in a later run, after many lands, can
land twice. So clients resend across runs only to a server that says it
records ids: `GET /v1/ready` lists `land-id` in its new `features` field.
`leep land` checks it before resending a land saved in `state.json`; against
a server that does not list it, it clears the saved land and says the land
may have applied and how to check (`leep reflog REF`). Resends within one
run (after a timeout, a 5xx or a lost connection) stay, since the scan
covers them.

Backups (ADR 0026) copy the columns. A backup from v4 restores through the
scratch-copy migration, with no ids.

## Consequences
- A retried keyed land, however late, returns the original result and adds
  nothing. Under the fault sweep half the simulated agents now land with ids.
- A keyed land costs one indexed read per batch attempt and two more columns
  in the entry it writes.
- `RefUpdateContext` has a new public field; code that builds it with a
  struct literal must set `land`. `MetaStore` wrappers must forward
  `find_lands`.
- The ref log is now the store of land results; a future change that prunes
  it must keep ids for as long as clients may retry.
