### Context packs for a task
- **Added.** `POST /v1/context` (ADR 0097): what the server already has on a
  task, assembled with no model: files (the request's paths, a search for
  identifiers in the task, files that changed with the paths), owners and
  `carry` rules, the last change per file with its creator and model, live
  work overlapping the files, possible duplicates of the task, and the
  ref's green, red and suspects touching the files. Deterministic, cut to
  a byte budget (default 32 KiB, at most 256 KiB) by per-section quotas,
  each section within 1 s and the call within 2 s; parts that fail or time
  out are listed as omitted. A read token may call it, and it marks no
  notice as said.
- **Added.** MCP tools `leep_task_start` (declare the session and get the
  pack in one call), `leep_context`, `leep_review_request` and
  `leep_review_inbox`; other agents' words stay in the untrusted fence.
- **Added.** `leep context` and `leep work start`, with agent-written text
  quoted and escaped.
- **Added.** Metrics `leep_context_packs_total`, `leep_context_bytes_total`,
  `leep_context_items_cut_total` and `leep_context_sections_omitted_total`.
- **Docs.** ADR 0097.
