### Events: what changed in coordination, in one order every server agrees on
- **Added.** `GET /v1/events` (ADR 0099): coordination events after a
  cursor (`session.started`, `session.paths`, `session.ended` with
  `landed_as`, `ref.landed` for every land, `note.added`, `note.endorsed`, `note.superseded`), ids,
  states and paths only, never what an agent wrote (paths under
  `untrusted`). Filters `paths`, `kinds` and `session=KEY` (the caller's
  own session: other sessions' work on its paths or task). Long-poll
  with `wait` up to 30 s (500 events or 256 KiB an answer), or a stream
  with `Accept: text/event-stream` (`id:` the cursor, `Last-Event-ID`
  resumes, a heartbeat every 15 s, closed after 10 minutes). A cursor
  from one server resumes on any other with no loss or duplicate.
- **Added.** `leep watch [PATH...]` prints events as they happen
  (`--kind`, `--after`, `--once`, `--json`).
- **Changed.** `leep-mcp` reads the events bearing on its session on
  every tool call and asks for notices when one matched, and at least
  every 30 s: a land on an agent's file is said on its next tool call.
  Against an older server it keeps the 30 s poll.
- **Added.** Migration 0011 (`events`), additive; events are kept 24
  hours (`--events-retention`, `LEEP_EVENTS_RETENTION`, at most a day) and
  are not in backups. A cursor older than that, or from another store or
  before a restore (cursors carry the store's epoch), is 410
  `cursor_expired` with the cursor to read on from. Waiting readers have
  their own admission class (10,000 per server, 64 per principal, 503
  `Leep-Refused: events`) and hold no token concurrency slot. Metrics
  `leep_events_appended_total`, `leep_events_dropped_total`,
  `leep_event_waiters`, `leep_events_oldest_age_seconds`,
  `leep_events_refused_total` and
  `leep_events_cursor_expired_total`.
