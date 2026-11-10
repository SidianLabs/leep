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
