### Coordination notices and packs, tuned by the twenty-agent demo
- **Fixed.** A context pack's in-flight work is ranked by the agent's own
  and the request's paths: it no longer calls a co-changed file "your"
  file or names a level the notice in the same answer contradicts. Work
  met only on a file related to the task carries `via: "related"` (a new
  optional `Notice` field) and is listed after work on the agent's own
  files (ADR 0097 amendment).
- **Fixed.** The first of two sessions with one task key is told it has a
  duplicate: the task-key lookup (one indexed listing) now runs on
  changeset creates, lands and `GET /v1/notices`, not only on declarations
  (ADR 0096 amendment).
- **Fixed.** An agent that only sets `LEEP_AGENT_SESSION` (no `leep work
  start`) now hears `landed` notices: the CLI and `leep_commit_and_land`
  send the new request header `Leep-Target-Ref` on changeset creates, the
  inferred session records it, and a session with no target ref is taken
  to land on `main` (ADR 0095 and 0096 amendments).
- **Changed.** A `landed` notice for a land by a session with your task
  key says so: `by: task_key`, naming that session and the key, and the
  CLI and MCP tools add "your duplicate landed this task; check whether
  your work is still needed" (ADR 0096 amendment).
- **Changed.** Context packs give one owners item per distinct owners
  answer and one history item per changeset, listing the files each covers
  in `untrusted.paths`; MCP explains a `carry` rule once; `leep_task_start`
  and `leep work start` no longer repeat as a notice what the pack lists
  (ADR 0097 amendment).
