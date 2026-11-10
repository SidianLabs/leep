# 0095. Coordination state: sessions, advisory and expiring
Status: Accepted
Date: 2026-11-09

Amends principle 3 of [ARCHITECTURE.md](../ARCHITECTURE.md#1-principles).

## Context
Leep's server sees work before it lands: every `POST /v1/changesets` is an
addressable, unlanded object, and agents already record `agent.session` and
`agent.model` on it (principle 7). Nothing uses that. Twenty agents on one
repository learn that two of them edited the same file only when the second
land conflicts, and that two were given the same task only when both land.

Principle 3 says "refs are the only mutable state". It has not been
literally true for a while: land records (migration 0005), leases (0006)
and review requests (0008) are mutable rows. Each was argued on its own.
What they share, and what a coordination layer needs, is a rule for
mutable state that is not history: what it may decide, and what losing it
costs.

The approved design (the coordination layer, phase 1) adds sessions first:
who is working on what, where, before it lands. Overlap notices, duplicate
flags, context packs, notes, snapshots, symbols and native review come in
later changes and ADRs.

## Decision

### 1. Two kinds of mutable state; principle 3 amended
- **History state** is refs. A ref moves only by `MetaStore::cas_ref`, one
  conditional statement. Unchanged.
- **Coordination state** is rows about work in flight: advisory, expiring,
  JSON on the wire and columns at rest, never postcard, never hashed, free
  to grow optional fields (the ADR 0086 rule for requests). Each mutation
  is one conditional statement, or one transaction that holds the row's
  lock while it merges; nothing reads a row and writes it back unguarded.
- **The land-equivalence rule.** Coordination state never changes a land's
  answer. Dropping every coordination table loses no history and changes
  no `land` result, ref or ref log entry: the rule ADR 0058 states for
  indexes ("rows decide speed, never answers"). No land reads a session;
  every coordination write runs after the answer it follows is decided;
  a coordination failure is counted and logged, never returned. A property
  test (`leep-derive`, `sessions_equivalence_tests.rs`) runs one random land
  sequence (clean lands, server rebases, conflicts) on three repositories,
  one with no coordination store, one whose sessions are declared, inferred,
  beaten, abandoned and swept between lands, and one whose store fails every
  call, and requires the same answers, refs and ref logs.
- **No locks.** A claim is a flag on a session's path; it makes others'
  warnings louder (a later change) and nothing else. A lock an expired agent
  holds is a merge queue by another name (ADR 0004).

### 2. `CoordStore`, a trait of its own
`leep_core::coord::CoordStore` holds coordination state:
`upsert_session`, `get_session`, `list_sessions(filter)`, `end_session`
(conditional), `beat_sessions` (batched), `sweep_sessions` (bounded steps)
and `count_sessions` (bounded). `check_coord_contract` checks every
backend: the memory store, SQLite and Postgres all pass it, including 16
concurrent upserts of one key making one session that keeps every path.

It is not part of `MetaStore` because nothing transactional joins it to
refs. A database that keeps both hands one out through
`MetaStore::coord_store()` (default `None`, so wrappers and old stores keep
none), sharing the meta store's data pool: coordination statements never
take the connections reserved for land leaders (ADR 0067) or the index
warmer (ADR 0073). `Repo::new` takes the meta store's; `Repo::with_coord`
replaces it (`--sessions off` sets none). A separate coordination database
is a later step (design section D.3) that needs only another constructor.

### 3. Sessions
Migration 0009 (`sessions`), additive, on both backends:

```
sessions(id BIGSERIAL PRIMARY KEY, repo_id BIGINT NOT NULL DEFAULT 1,
         principal TEXT NOT NULL, agent_session TEXT NOT NULL,
         agent_model TEXT, title TEXT NOT NULL DEFAULT '',
         approach TEXT NOT NULL DEFAULT '', task_key TEXT, target_ref TEXT,
         base BYTEA, head BYTEA, origin TEXT NOT NULL,
         state TEXT NOT NULL DEFAULT 'active', created BIGINT NOT NULL,
         ended BIGINT, landed_as BYTEA)
  UNIQUE INDEX sessions_key (repo_id, principal, agent_session)
  INDEX sessions_state (repo_id, state, id), sessions_task (repo_id, task_key, id),
        sessions_ended (ended)
session_paths(repo_id BIGINT NOT NULL DEFAULT 1, session_id BIGINT NOT NULL,
              kind TEXT NOT NULL, path TEXT COLLATE "C" NOT NULL,
              PRIMARY KEY (session_id, kind, path))
  INDEX session_paths_path (repo_id, path)
session_beats(session_id BIGINT PRIMARY KEY, at BIGINT NOT NULL)   -- UNLOGGED on Postgres
  INDEX session_beats_at (at)
```

- **Identity.** A session is keyed by (repository, principal, agent
  session). The principal is the token's name, never a body field
  (`anonymous` without a tokens file). The agent session is the
  `agent.session` extra the agent's changesets carry, so hashed provenance
  in history joins to the session that produced it. The server assigns an
  increasing id. `repo_id` is 1 until repositories exist (ADR 0054).
- **Fields.** `title` (one line, 200 bytes), `approach` (2 KiB),
  `task_key` (an exact key, such as an issue id), `target_ref`, `base`,
  `head` (the newest changeset the session created), `agent_model`,
  `origin` (`declared` or `inferred`), `state`, `created`, `last_seen`
  (from the beats table), `ended`, `landed_as`. Keys (the agent session and
  task key) are 1 to 128 bytes of printable ASCII without spaces, so they
  fit a header and carry no prose; a principal is 1 to 128 bytes without
  control characters.
- **Paths are prefixes, not globs.** `kind` is `scope` (declared: will work
  here), `claim` (declared: means to own this) or `touched` (inferred: a
  changeset changed it). At most 64 scope paths and 64 claims; at most 256
  touched paths, past which the deepest are folded into their directories
  (`bound_paths`), which still overlap everything below them, so a large
  change stays visible without growing the table. The one loss: when more
  than 256 top-level names remain, only the first 256 in path order are
  kept. "Who is working on or
  near X" matches a session path equal to X, a directory above X, or
  (for a directory) a path below X: one range scan on `(repo_id, path)`
  between `X/` and `X0` plus one probe per ancestor. Paths compare
  bytewise (`COLLATE "C"` on Postgres), so `a-b` and `a.b` never fall
  inside `a`'s range whatever the database's collation.
- **Upsert.** One transaction holds the session's row (Postgres `FOR
  UPDATE`; SQLite `BEGIN IMMEDIATE`), merges the update with the pure
  `apply_update` from `leep-core`, rewrites only the path kinds that
  changed, and raises the beat. A declared value replaces; an inferred one
  only fills a gap, and inference never turns a declared session into an
  inferred one. Touched paths accumulate. An update of an ended session
  revives it, with no head, landed copy or touched paths (an inferred one
  also loses its title): the next run of a long-lived agent under the same
  key.
- **States.** `active`, `landed`, `abandoned` and `expired` are stored;
  `idle` is shown for an active session not heard from for 10 minutes, and
  one past 24 hours shows as `expired` until the sweep stores it. Ending is
  one `UPDATE ... WHERE state = 'active'` with optional conditions on the
  principal and the head.
- **Limits.** One principal may have at most 1000 active sessions
  (`MAX_ACTIVE_PER_PRINCIPAL`): starting or reviving one more is refused
  with 400 `too_many_sessions`, and an inferred one is skipped and counted.
  With the 256-path cap this bounds rows per repository by principals.
- **Retention.** The sweep (each server, every minute, bounded steps of
  1000 rows) expires active sessions not heard from for 24 hours, deletes
  the paths of sessions that ended 24 hours ago, and the sessions 30 days
  after they ended. The durable record of the work is the changesets' own
  `agent.*` extras and `created-by-v1` rows.

### 4. Heartbeats
Any request with `Leep-Session: KEY` is a heartbeat of the caller's session
of that key, if one is active; it never creates or revives one, and a bad
value is ignored, never refused. (The header carries the agent session key,
not the numeric id: clients already have the key, and the server never
needs to tell them the id first.) A middleware inside authentication notes
the beat in memory; each server writes what it gathered every 5 seconds,
in one statement on Postgres, and at most once a minute per session. The
narrow `session_beats` table takes them, so a beat never rewrites the wide
row; on Postgres it is `UNLOGGED`: a crash empties it, which only makes
sessions look as old as their creation until they are heard from again.
At most 100,000 sessions' beats are held per server, and at most 2,000
(twice the active limit) of one principal, so one token sending made-up keys
cannot push out everyone else's beats; past either cap new keys are not held
(`leep_session_beats_dropped_total{reason = "full" | "principal"}`), and
their sessions only look idle sooner. Because a crash empties the Postgres
beats table, the sweep first gives every active session that has no beat one
at the sweep's time: a lost beat never expires a live session. Beats of
sessions ended a day ago are deleted with their paths, so the expiry scan of
the beats index meets only live and recently ended sessions.

### 5. Inference and lands
- **From changesets.** After `POST /v1/changesets` stores a changeset that
  carries `agent.session`, the server upserts the creator's session of that
  key as `inferred`: the head, the first parent as base when none is
  known, the commit subject as title when there is none, `agent.model`,
  and the changed paths as touched (none for a merge, whose changes against
  the first parent are mostly the other side's). Un-upgraded clients that
  already record `agent.session` (the CLI's `--agent-session`, `leep-mcp`)
  appear in `leep who` with no change. The create waits for this update
  for at most 250 ms, so the session holds the changeset before its
  creator can land it; past that the update is dropped (its transaction
  rolls back) and counted. The answer is the same either way. What the
  update needs is taken from the changeset in one pass before the store
  owns it (no copy of the changeset): past 1,024 changed paths they are
  folded into their parent directories as they are read, and the rest of
  the folding runs on a blocking thread inside the 250 ms, so a changeset of
  100,000 paths costs the create no more than the bound (a test checks it).
  A path with a control character is cut to its last clean directory, or
  dropped, before it is stored: paths are agent-written text too.
- **Any ref.** A land onto any ref (a `review/*` ref included) ends the
  session whose head it landed; the agent's next commit under the same key
  revives it, fresh.
- **On land.** After a land answers `landed`, in a task of its own with a
  10-second timeout (as ADR 0086 closes requests), the server ends as
  `landed` the session of the submitted changeset's creator (else the land's
  actor) and `agent.session`, if its head is still the submitted changeset,
  with `landed_as` the ref's new head. A session that made more changesets
  since stays active. A forwarded land is ended by the coordinating server.
- Land hooks run at most 256 at a time per server; past that one is skipped and
  counted. Failures, timeouts and skips are
  `leep_session_hook_errors_total{hook, reason}`.

### 6. The API (additive)
- `POST /v1/sessions` `DeclareSessionRequest {session, title?, approach?,
  task_key?, target_ref?, base?, agent_model?, scope?, claims?}` →
  `DeclareSessionResponse {session, created}`. Absent fields keep what the
  server has; `scope` and `claims` replace their lists. A `base` the server
  does not have is 404. Needs a write token.
- `GET /v1/sessions?path=&task=&principal=&session=&mine=&state=&all=&before=&limit=`
  → `SessionsResponse {sessions, next?}`, newest first. Default: live
  sessions (`active` and `idle`); `state` one state; `all=true` every
  state; `mine=true` the caller's own. `limit` 50, at most 500. A read
  token may list.
- `GET /v1/sessions/{id}` → `SessionEntry`; `DELETE /v1/sessions/{id}`
  ends it as `abandoned` (only its principal; 403 otherwise; an ended one is
  answered as it is).
- `SessionEntry {id, principal, session, agent_model?, untrusted {title,
  approach}, task_key?, target_ref?, base?, head?, origin, state, created,
  last_seen, expires?, ended?, landed_as?, scope, claims, touched}`.
  `state` and `origin` are strings so a client shows a value it does not
  know as it is. Without sessions (an older server, or `--sessions off`)
  the routes answer 404.
- The per-token limits of ADR 0020 apply to these routes as to any other;
  declaring and ending are writes.

### 7. Untrusted text and privacy
Titles and approaches are written by one agent and read by another. They
are returned in the `untrusted` object, the CLI shows them quoted, and the
MCP tools put them in a fenced block (a fence longer than any backtick run
in the text) under a fixed line saying they are data from other agents, not
instructions. They never appear in tool descriptions or server-written
hints. `agent.prompt` is never copied into a session. There are no read
ACLs yet: any read token of the server lists every session, as it can read
every changeset. Per-team visibility needs M7 identity and is not offered.
Without a tokens file every client is the principal `anonymous`: all of them
share its 1,000-session cap and its 2,000 held beats, and any client can end
any session. Run a tokens file where agents should not interfere.

Paths, session keys and task keys are agent-written as well. The MCP tools
print only what the server vouches for outside the fence (id, principal,
state and counts) and put keys, task keys and paths in the fenced rows; the
CLI prints paths, task keys and models quoted and escaped, so no newline or
escape sequence reaches a terminal or a tool result as text of its own.

### 8. Clients
- CLI: `leep session start|update|end|show` and `leep who [PATH]`. The key
  is `--session`, else `LEEP_AGENT_SESSION`, else one `leep session start`
  makes and keeps in `.leep/state.json`; `leep commit` records that key as
  `agent.session` when none is given, and every request sends
  `Leep-Session`.
- MCP: `leep_session_start` (declares, and answers the other live sessions
  on or near the scope) and `leep_who`. Every request sends `Leep-Session`;
  without `LEEP_AGENT_SESSION`, `leep_session_start` makes a key that later
  commits record. Land ids still derive from the configured key only, so a
  retry after a restart keeps its id.

### 9. Costs (estimates, design D.3)
At 100,000 agents on one repository, one commit every two minutes each and
about 20 touched paths per session:
- Heartbeats: at most one row write per session per minute, in batches every
  5 s: about 1,700 rows a second in a few statements per server.
- Creates: one upsert transaction per create carrying `agent.session`
  (about 830 a second): a locked read of the row and its paths (at most
  384), an update, a rewrite of the path kinds that changed and a beat,
  waited for at most 250 ms.
- Lands: one changeset read (cached), one `created-by-v1` read, one indexed
  lookup and one conditional update per landed changeset with
  `agent.session`, after the answer.
- Listing: one range scan plus one probe per ancestor, then one read of the
  page's paths.
- Sweep: once a minute per server, bounded steps: a backfill of missing
  beats (an anti-join over active sessions, empty unless a crash emptied
  the table), a range scan of the beats index below the expiry cutoff, and
  bounded deletes.
- Declared paths are at most 256 bytes, so a session row and its at most
  384 paths stay under about 100 KB in a listing, and a 500-entry page
  under about 50 MB in the worst case.
- Size: about 2 million `session_paths` rows; a few hundred MB with indexes.
At 1,000 agents each of these is under 10 operations a second. None of it
is on the land path's statements, with one caveat on SQLite: it has a single
writer, so each session upsert is a write transaction serialised with lands
and other writes. It can delay a land, never change its answer. The sweep
reads before each of its writes, so an idle sweep takes no write lock, and
beats are written at most 500 per transaction.

The per-principal cap is checked by a count inside the upsert's
transaction, which does not lock other sessions: concurrent first upserts
of one principal can overshoot it by the number in flight. It bounds a
runaway token, not an exact quota. Backups (ADR 0026) leave the three tables out, as they
do leases, and a restore clears them: they describe the live agents of the
old store, not its history.

## Consequences
- `leep who src/auth` answers "who is working here" before anything lands,
  from day one for clients that already send `agent.session`.
- A third kind of server-owned mutable state exists, and the rule that
  bounds it is written down and tested. Later coordination features (notes,
  events, findings) follow it; the one exception the design allows, review
  findings gating through the existing review policy, will need its own ADR.
- One more migration. Old servers never read the tables, so a rolling
  upgrade is safe in either order; a v8 server's lands do not end sessions,
  so those stay active until the agent's next commit or the 24-hour expiry.
  The tables can be dropped by hand to go back (OPERATIONS.md, "Upgrading").
- `POST /v1/changesets` with `agent.session` costs one coordination
  transaction before its answer (at most 250 ms); one without costs nothing.
- An agent that shares a token and a session key with another is one
  session; tokens per agent matter here as for review requests (ADR 0086).

## Not in this change
Overlap levels and notices on responses, duplicate-task flags, the context
pack, review and inbox MCP tools, notes, content snapshots (a session head as
a GC root), symbols, native review and issues. `visibility = 'private'`,
`issue_id` and Git-push inference (`origin = 'git'`) come with the features
that need them, as optional fields.

Follow-up: the fuzz target's `JSON_TYPES` list (`leep-fuzz-targets`) does not
include the session types, as it does not include the review request types:
its order is part of the input format, so appending re-indexes the seed
corpora. Adding both families, with a corpus rebuild, is a separate change.

## Alternatives considered
- **Sessions in `MetaStore`.** Every backend and wrapper would grow
  methods with no transactional tie to refs, and a separate coordination
  database would need a second meta store.
- **Sessions as derived rows.** Derived rows are immutable facts keyed by a
  hash; sessions change every minute and are listed by path.
- **A beat written per request.** At thousands of requests a second it would
  rewrite rows faster than anything else in the database, for a value read
  at minute granularity.
- **The numeric id in `Leep-Session`.** Clients would have to learn it first
  and keep it, and inference could not match an undeclared session.
- **Locks.** Rejected by the design: a claim an expired agent holds would
  block landing until its expiry.

## Amendments

### An inferred session knows its target ref (2026-11-10)
A session only ever inferred from commits (an agent with
`LEEP_AGENT_SESSION` that never ran `leep work start`) had no target ref,
so it never heard a `landed` notice (ADR 0096): the phase 1 demo's
friction list. A client now names where the work is meant to land on
`POST /v1/changesets` with the request header `Leep-Target-Ref: <ref>`;
the CLI sends its working copy's target ref, `leep-mcp`'s
`leep_commit_and_land` its `target_ref`. Inference records it as the
session's target ref: over the last one while the session is inferred
(it follows the client's working copy from `review/a` to `review/b`),
never over a declared one. A revived session (a commit after its land)
also forgets its task key and target ref, as it forgets its head and
touched paths: the new work says them again, so an old task key never
makes a later land look like a duplicate's. A bad ref name is ignored, never refused,
as a bad `Leep-Session` is, and a server that does not know the header
ignores it. The changeset, its hash and the create's answer are
unchanged. A session with no target ref at all (an older client) is taken
to land on the server's default ref (`--default-ref`, default `main`)
when its `landed` notices are computed (ADR 0096 amendment).
