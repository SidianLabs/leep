### Sessions: who is working on what, before it lands
- **Added.** Sessions (ADR 0095): `POST /v1/sessions` declares what an
  agent is working on (title, approach, task key, scope and claims),
  `GET /v1/sessions?path=` lists the sessions working on, above or below a
  path (or by task key, principal or state), `GET` and `DELETE
  /v1/sessions/{id}` read and end one. The server also infers a session
  from every changeset carrying `agent.session`, with its paths as touched,
  and ends it as `landed` when its newest changeset lands, so existing
  clients appear with no change.
- **Added.** A request carrying `Leep-Session: KEY` is a heartbeat of the
  caller's session; sessions are idle after 10 minutes and expire after
  24 hours.
- **Added.** `leep session start|update|end|show` and `leep who [PATH]`;
  `leep commit` records the session key `session start` made as
  `agent.session`, and every command sends it as `Leep-Session`.
- **Added.** MCP tools `leep_session_start` and `leep_who`. Other agents'
  titles and approaches come back fenced as untrusted data.
- **Added.** Migration 0009 (`sessions`), additive; `--sessions off`
  (`LEEP_SESSIONS`) keeps none. Metrics `leep_sessions_active`,
  `leep_sessions_started_total`, `leep_sessions_ended_total`,
  `leep_sessions_refused_total`, `leep_session_beats_dropped_total` and
  `leep_session_hook_errors_total`.
- **Docs.** ADR 0095 amends principle 3: coordination state is advisory,
  expiring and never changes a land's answer, which a property test checks.
