# 0086. Review requests, an inbox, and who created each change
Status: Accepted
Date: 2026-11-03

## Context
Agents working through Leep on a protected ref (ADR 0011) hit two gaps in a
session of five agents and one reviewer:

- **Nothing asks for a review.** A land refused with `needs review` told an
  agent to run `leep review status`, but there was no way to tell the owner
  that a change was waiting. The agents used a directory outside Leep as a
  queue, and the reviewer polled it. Nothing listed "what is waiting for
  me".
- **Nothing says who made a change.** Every agent committed as the same
  author (`Claude <noreply@anthropic.com>`), because `author` is a
  client-supplied string. The reviewer could tell agents apart only from the
  out-of-band queue, though the server knew each one's token: the ref log
  records it for lands, but nothing records it for the changeset itself.

A request is not an attestation about history. It is opened by one
principal, read by many (each reviewer's inbox), and closed when its change
lands or its requester gives up. Two agents may ask at the same moment, and a
reviewer's inbox is a query by reviewer name. Derived rows are keyed by one
hash and have no listing by a field, and a single row per changeset would be
a read-modify-write race. The creator, on the other hand, is a fact about one
changeset that never changes once written.

## Decision

### Review requests are rows of their own table
Migration 0008 (`review_requests`) adds, on both backends:

```
review_requests(id BIGSERIAL PRIMARY KEY, repo_id BIGINT NOT NULL DEFAULT 1,
                changeset BYTEA NOT NULL, target_ref TEXT NOT NULL,
                requester TEXT NOT NULL, message TEXT NOT NULL DEFAULT '',
                created BIGINT NOT NULL, state TEXT NOT NULL DEFAULT 'open',
                closed BIGINT, landed_as BYTEA)
review_request_reviewers(request_id BIGINT NOT NULL, reviewer TEXT NOT NULL,
                         PRIMARY KEY (request_id, reviewer))
```

with indexes on `(state, target_ref, id)`, `(requester, id)`, `(changeset)` and
`review_request_reviewers (reviewer, request_id)`. Like the ref log (ADR
0018), a request is JSON on the wire and columns at rest, never postcard and
never hashed, so it may grow optional fields. `state` is `open`, `landed` or
`withdrawn`; a request with no reviewer rows is open to any reviewer.
`repo_id` is 1 until repositories exist (ADR 0054).

`MetaStore` gains `supports_review_requests`, `put_review_request` (one
transaction: the request and its reviewer rows; the store assigns increasing
ids), `get_review_request`, `list_review_requests(filter)` (newest first;
reviewer, requester, changeset, ref, states, `before` and `limit`) and
`close_review_request(id, state, landed_as, at)`, one conditional `UPDATE ...
WHERE state = 'open'`, so two closers never both win and nothing reopens.
The defaults refuse, as for leases. `check_review_request_contract` checks
every backend, and `check_meta_store_contract` runs it for stores that
support requests. A reviewer filter matches requests that name the reviewer
and requests open to any reviewer that someone else made: nobody is asked to
review their own change.

### The API (additive)
- `POST /v1/review-requests` `{changeset, target_ref = "main", reviewers =
  [], message = ""}` opens a request. The requester is the authenticated
  token's name, never the body's. Named reviewers must be write tokens that
  may review (`Auth::reviewers`, ADR 0011) and not the requester; anything
  else is 400 naming the problem. With no names the server asks the owners
  of the paths the changeset still needs approved on `target_ref`, pending or
  rejected, as `review-status` judges them now, keeping those who may review
  and are not the requester; when no such path has an owner the request is
  open to any reviewer. An open request of the same requester, changeset,
  ref and reviewers answers instead of a second one (`existing: true`), so a
  retry does not duplicate it. The ref must exist (404 otherwise: nothing
  would ever close the request), unless a land policy covers it, since the
  land that creates a protected ref needs review too. A changeset already on
  the ref's first-parent chain is 400; an unknown one 404. One requester may
  have at most 100 open requests (`MAX_OPEN_REQUESTS`); one more is 400 with
  code `too_many_review_requests`, after the requester's open requests that
  reached the ref by an unreported move are closed. Without a tokens file
  the endpoint is 400, as reviews are.
- `GET /v1/review-requests?reviewer=&requester=&changeset=&ref=&state=&open=&before=&limit=`
  lists them (a read token may), each with the changeset's `created_by` and
  the first line of its message as `title`. `limit` defaults to 50, at most
  500; `next` is the `before` of the next page.
- `GET /v1/review-requests/{id}` and `POST /v1/review-requests/{id}/withdraw`
  (only the requester; 403 for anyone else; a closed request is answered as
  it is).

### Lands close requests, and nothing about it can fail a land
After `land` answers `landed` on the server that ran it (the coordinating
server, for a forwarded land), the server spawns a task that closes, as
`landed`, the open requests of that ref whose changeset is the one landed,
one of the stack the land replayed (found by walking the new head's
first-parent chain while each changeset has a `rebased-origin-v1` row, at
most 256 steps; `landed_as` is the copy), or now on the head's first-parent
chain (`landed_as` is itself). Only the `rebased-origin-v1` row counts as
"a copy of": the server writes it when it makes a copy, while a changeset's
`leep.rebased_from` is whatever its client wrote (the CLI writes it for
cherry-picks and conflict resolutions), so trusting it would let one
agent's land close another agent's request. The task runs after the answer is decided, with a
10-second timeout; an error or a timeout is counted in
`leep_review_request_close_errors_total` and logged, never returned. A land
never waits for it, and the close never changes the ref, so it adds no new
way for a land to fail and nothing to the land's crash or fault behaviour.

What the task misses (it timed out, the server stopped, or the ref moved by
a raw `POST /v1/refs`, an import or `leep undo`), a listing closes: before
answering, every open request it is about to return whose changeset is on its
ref's head's first-parent chain is closed as landed, and dropped from an
`open` listing. That check rides skip pointers, so it is logarithmic in the
distance, and only the requests on the page are checked. A request whose
change was squashed or rebased by its author before landing stays open until
withdrawn: nothing links the new changeset to the old.

### The creator is a derived row written with the changeset
`POST /v1/changesets` from an authenticated client writes a
`created-by-v1` row, key = changeset id, value = postcard `CreatedBy {
principal: String, timestamp: i64 }` (`leep_core::review_request`), through
the new `MetaStore::put_changeset_with_derived_if_new`: the row is written
only by the store write that inserts the changeset, atomically with it (a
SQLite `BEGIN IMMEDIATE` transaction that writes it only when the insert
changed a row; on Postgres one statement whose insert of the row depends on
the changeset insert's `RETURNING`). So posting the bytes of a changeset
that is already stored (an old commit, a server-made one, one stored
without authentication) never makes the poster its creator, and of
concurrent posts exactly the inserting one records itself. The default of
the method writes no such row, so a backend without it records no creators
rather than wrong ones. `check_insert_once_contract` checks backends.
Without authentication nothing is recorded. Server-made changesets (merges,
rebased copies) get no row. `GET /v1/changesets/{id}` answers it as
`ChangesetInfo::created_by` (optional; omitted when absent, so the shape is
unchanged for older data, servers and clients), and review request listings
carry it. `leep show` prints `Created-by: NAME`.

The kind is an attestation for verify and repair: verify checks that rows
decode, `repair drop` refuses it, and the changeset sweep deletes it with its
changeset (`PER_CHANGESET_KINDS`).

### The CLI
`leep review request REV [--to NAME]... [-m MSG] [--ref REF]`,
`leep review inbox [--mine] [--all] [-n N]` (open requests that name the
caller, by `GET /v1/whoami`; `--mine` the caller's own) and
`leep review cancel ID`. Against a server without the endpoints the CLI says
the server is too old instead of printing a 404.

## Consequences
- Agents ask for reviews and reviewers find them through Leep, with the
  requester and creator shown from tokens rather than from the author field.
  For that to tell agents apart, each agent needs its own token; DEPLOY.md
  says so.
- One more migration. Old servers never read the tables, so a rolling
  upgrade is safe in either order; requests opened on new servers are
  closed only by new servers' lands or listings. Backups (ADR 0026) copy both
  tables, keeping ids, and a restore moves the Postgres sequence past them.
- `GET /v1/changesets/{id}` makes one more metadata read, for the row.
  `POST /v1/changesets` with a token makes no extra statement: the row
  rides in the changeset's own write.
- A successful land with open requests on its ref makes one indexed query,
  and with matching requests a few reads and one update each, after its
  answer. A land with none makes one indexed query.
- The table is never pruned in this version: rows are small and written only
  when someone asks for a review.
- The creator is the token, not a person or a model: `agent.model` and the
  author still say what the client claims. A token shared by several agents
  makes them one creator.

## Alternatives considered
- **Requests as derived rows** keyed by (changeset, requester): last writer
  wins, no listing by reviewer without O(principals × changesets) lookups,
  and closing would be a read-modify-write.
- **The creator in `extra`.** It is hashed into the id, the client writes it,
  and it could claim any name.
- **Closing requests inside the land's ref transaction.** Atomic, but it
  would add a write, and a failure mode, to every land for bookkeeping that
  can be repaired at read time.
- **Defaulting reviewers to every reviewer token.** Floods every inbox in a
  large fleet; owners are who the policy asks for anyway.

*Amended by [ADR 0087](0087-review-requests-close-by-content.md):* a
request also closes when a changeset the land added names its changeset in
`leep.rebased_from` and either lands its reviewed change byte for byte (the
content rule of ADR 0085) or was landed by the request's requester; a
listing applies the content rule too. So "a request whose change was
squashed or rebased by its author before landing stays open" now holds only
when the reviewed change itself was altered and someone else landed it.
Entries gain `approved_by` and `rejected_by`.
