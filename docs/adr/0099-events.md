# 0099. Events: an ordered log of coordination facts
Status: Accepted
Date: 2026-11-10

Builds on [ADR 0095](0095-coordination-state.md) (coordination state and
its land-equivalence rule), [ADR 0096](0096-overlap-notices-and-duplicate-flags.md)
(notices) and [ADR 0098](0098-notes.md) (notes). Design section C.

## Context
Agents hear about each other's work on the answers they already get
(ADR 0096): a notice rides on a create, a land, a declaration. Two readers
are left out. An orchestrator watching twenty agents has no answer to ride
on; it can only poll `GET /v1/sessions` and diff. And an MCP server between
tool calls cannot know whether anything happened without asking: it asked
`GET /v1/notices` at most every 30 seconds, so a land on an agent's file
could go unsaid for 30 seconds (the known limit in
[DEMO-COORDINATION.md](../DEMO-COORDINATION.md)), and every poll that found
nothing cost a notice computation.

Both need "what happened since I last looked", in one order every server
agrees on, cheap when nothing happened. That is an event log with a
cursor. Events are not how agents hear things: answers still carry the
notices. Events serve orchestrators and give MCP a cheap "anything new?"
check.

## Decision

### 1. Storage
Migration 0011 (`events`), additive, on both backends:

```
events(seq BIGSERIAL PRIMARY KEY,         -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
       repo_id BIGINT NOT NULL DEFAULT 1,
       kind TEXT NOT NULL, session_id BIGINT, principal TEXT NOT NULL,
       subject TEXT NOT NULL,             -- 'session:N' | 'note:N' | changeset id
       paths TEXT NOT NULL DEFAULT '[]',  -- JSON array, at most 16, folded
       payload TEXT NOT NULL DEFAULT '{}',-- JSON object of facts
       created BIGINT NOT NULL)
```

- **One row per fact.** Kinds: `session.started` (created or revived),
  `session.paths` (its folded path set changed), `session.ended` (`state`
  `landed` with `landed_as`, or `abandoned`), `ref.landed` (every land,
  whatever its changesets carry: `landed_as`, the paths the land changed,
  and the ref under `untrusted`), `note.added`,
  `note.endorsed`, `note.superseded`, and `conflict.predicted`, reserved
  for the merge dry run (ADR 0096, part 2); nothing emits it yet.
- **No prose.** An event holds ids, states, enum names, principals (token
  names) and paths. Titles, approaches, task keys, commit messages and note
  text never enter one; a reader fetches them through the routes that
  fence them. A session's task key travels as a 16-hex digest (`task`),
  so a reader can match two sessions on one task without reading the key.
  Paths are agent-chosen names: on the wire they sit under `untrusted`.
  Events add no injection surface.
- **Logged**, unlike `session_beats`. Since Postgres 15 a sequence owned by
  an unlogged table is unlogged too: a crash would reset `seq`, and every
  cursor would point past the end.
- **One value at a time.** The sequence is `CACHE 1` (set explicitly):
  with a cache, a connection could hand out a `seq` below one another
  connection already committed, and the append lock (section 3) would no
  longer make commit order `seq` order. Within one append the rows take
  ascending values in the order given; nothing depends on more than that
  they all commit together.
- **An epoch.** `events_epoch` holds one random value, made by the
  migration and again when a restore clears the events. Cursors carry it
  (section 3).
- **No secondary index.** The sweep walks the primary key (section 4).
  The design's `events_created` index would only serve a sweep by time,
  which can leave holes (below).
- **Not backed up**, like sessions: events point at sessions and notes of
  the live system. A restore clears the table; the sequence goes on (or,
  in a new database, starts again), and section 3 answers every old cursor
  as expired.

### 2. Writes
Hooks emit after the answer they follow is decided: a declaration, a
create's session inference, an abandon, the post-land hook (a land ending
its session is `session.ended{landed}`), a note write. Each pushes into a
per-server buffer of 10,000 events (overflow is dropped and counted); a
task flushes the buffer every 250 ms in one `append_events` call. A failed
append is counted and logged, never returned: no answer waits on it, and
the land-equivalence test runs with an `append_events` that always fails.

Lands become events from the post-land hook, not the ref log. That gives
one cursor space for every kind, at the price of losing the event of a
land whose hook failed; ADR 0096's history walk stays the truth for
`landed` notices. Every land is a `ref.landed`, whether or not a session
ended with it (a changeset without `agent.session`, a person's `leep
land`, a stack whose head was not a session's): its paths are the diff
from the ref's head before the land (its ref log entry; else the landed
changeset's first parent), at most 1,000 read and folded to 16, and the
top-level names that changed for a larger land.

`session.paths` is sent when the folded set (scope, claims and touched
paths, at most 16) differs from the one this server last sent for the
session. A server that has not seen the session since it started (another
server took its requests, or it restarted) sends it once more; the fact is
idempotent.

Session expiry by the sweep is not an event: the sweep ends sessions in
bulk without naming them, and an expired session's paths are already
silent. A reader that needs it reads `GET /v1/sessions`.

### 3. Order and cursors
The cursor is the store's epoch and `seq`, sent as an opaque string
`e<epoch>.<seq>` so its shape can change; `e<seq>` (no epoch) is accepted
and checked by `seq` alone. A cursor of another epoch (another store, a
restore, a new database) is expired, whatever its `seq`.

The hazard: Postgres assigns `seq` from a sequence before commit. A
reader can see 11, move its cursor past it, and miss 10 when 10 commits a
moment later. So each append is one transaction that takes
`pg_advisory_xact_lock(EVENTS_LOCK, repo_id)` before it inserts:
appends commit one at a time, in `seq` order (each waits at most 2 s for
the lock, `lock_timeout`, and then fails and is counted, so a stuck
appender never holds others' data-pool connections), and a reader that has seen
`seq` N has seen every event below it that will ever exist. SQLite has one
writer (`BEGIN IMMEDIATE`) and is ordered already; `AUTOINCREMENT` keeps
`seq` from being handed out twice after the sweep empties the table.
Gaps (a failed append) are allowed.

`events_bounds` answers the oldest kept `seq` and the newest handed out.
On Postgres it takes the same lock shared (also within 2 s, else it
fails), so the sequence's value it reads is committed. The ring (section
6) answers only cursors it holds everything after; a cursor past what it
has read goes to the store, so a cursor from elsewhere is still 410. A cursor `c` has expired when the events just above
it are gone (`c + 1 < oldest`, or nothing is kept and `c < newest`), or
when it is past `newest` (a restored or reset store). An expired cursor is
410 `cursor_expired` with `oldest`, the cursor to read on from after
resyncing from state (`GET /v1/notices`, `/overlaps`, `/sessions`). A gap
just above a cursor can also answer 410: that costs a resync, never a
missed event. No `after` answers the current tail and no events.

### 4. Retention
24 hours (`--events-retention`, at least a minute and at most a day: an
event names a session's paths, which are deleted a day after it ends), not
the design's 7 days: a reader further behind is better resynced from
state. Every server sweeps in its per-minute coordination sweep, in steps
of 1,000 for up to 5 seconds (at most 600 steps), and sets
`leep_events_oldest_age_seconds`, which grows past the retention when the
sweep falls behind: it reads the oldest
1,000 rows by `seq` and deletes the prefix created before the cutoff,
stopping at the first younger one. Deleting by `created` alone could
delete 10 and keep 9 (servers' clocks and buffers differ), leaving a hole
a cursor could fall into without a 410; deleting a prefix keeps the kept
events "everything above one `seq`".

### 5. The route
`GET /v1/events?after=&session=KEY&paths=&kinds=&wait=`, read token.

- `kinds` and `paths` filter (repeated or comma-separated; at most 16
  paths, which match by `paths_overlap`). Filtered-out events still move
  `next`, the last event looked at.
- `session=KEY` resolves the key among the caller's own principal's
  sessions only (another principal's key resolves to nothing) and keeps
  the events of other sessions on the session's paths, or of sessions on
  its task. Events expose nothing that `GET /v1/sessions` does not.
- **Long-poll.** `wait` is clamped to 0 to 30 seconds, and below nine
  tenths of the request timeout; the answer comes as soon as something
  matches. A read that matches nothing scans at most 8 store pages before
  it answers `more`. `EventsResponse {events, next, more}`, at
  most 500 events or 256 KiB; `more` says there is more now.
- **SSE.** The same route with `Accept: text/event-stream`. `id:` is the
  cursor and `Last-Event-ID` resumes; a page that matched nothing still
  moves the reader's id (an `id:` line alone, which is not dispatched). A
  comment goes out every 15 s, `session=KEY` is resolved again as often
  (the session's new paths count), and the stream closes after 10 minutes
  (the client reconnects, which checks its token again).

### 6. Fan-out on read
No LISTEN/NOTIFY, which would tie the design to Postgres and hold a
connection per server. One tailer task per server reads
`WHERE seq > $last ORDER BY seq LIMIT 1000` every 250 ms, and only while
the server has waiters. It fills a ring of the last 10,000 events and
bumps a `tokio::sync::watch`; waiters filter from the ring in process. A
cursor behind the ring, or a read that does not wait (`wait=0`, MCP's
check), reads one bounded page from the store, plus the bounds when the
page does not start right after the cursor.

### 7. Backpressure
- A waiter (a long-poll with `wait` above 0, or a stream; one parse of
  the request decides, the same in the limits middleware and the
  handler, the last `wait` winning) takes its place first, before any
  store read. It is admitted by its own class, at most 10,000 per server and 64 per principal, else 503
  `Leep-Refused: events`. It does not hold its token's ADR 0020
  concurrency permit: a 30-second wait would otherwise starve the token's
  other requests. The token's rate limit still applies.
- A stream hands events to a channel of 64. A reader that lets it fill gets
  what the channel holds, then `event: reset`, and the stream ends: never
  unbounded buffering. It reconnects from its last id.
- Shutdown wakes every waiter (long-polls answer, streams end) and
  appends the events still held.

### 8. Clients
- CLI: `leep watch [PATH...]`, long-polling from the tail, one line per
  event, paths quoted.
- MCP: no new tool. On each tool call `leep-mcp` reads
  `GET /v1/events?session=K&after=C&wait=0` and asks `GET /v1/notices`
  when something matched (or the cursor expired, or on its first call),
  and in any case at least every 30 seconds, also when the events read
  fails: events are lossy, and a server whose events keep failing must
  not silence notices. Against a server without events it asks every 30
  seconds.

### 9. Metrics
`leep_events_appended_total{kind}`, `leep_events_dropped_total{reason}`
(`full`, `error`, `off`), `leep_event_waiters`,
`leep_events_oldest_age_seconds`,
`leep_events_refused_total{reason}` (`server`, `principal`, and `lagged`
for a reset stream), `leep_events_cursor_expired_total`.

## Consequences
- An orchestrator follows every agent's work with one long-poll and one
  cursor, on any server.
- MCP hears of a land on its files on the next tool call after the event
  is appended (about 250 ms after the land's answer), and an idle call
  costs one indexed read instead of a notice computation every 30 s.
- Events are lossy by design: an overflow, a failed append, a failed
  post-land hook or an expired session leaves no event. State
  (`/v1/sessions`, `/v1/notices`) stays the truth; events say when to
  look.
- One more table to sweep, and one advisory lock per append on Postgres
  (appends are one per server every 250 ms at most).

## Rollback
The table is new and nothing but events reads it. Roll back by stopping
the new servers, then `DROP TABLE events; DROP TABLE events_epoch;` and
`DELETE FROM schema_migrations WHERE version = 11;`, then starting the
v10 servers (OPERATIONS.md, "Upgrading", migration 11).
