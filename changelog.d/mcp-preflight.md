### MCP lands stop for overlaps the agent has not seen
- **Changed.** `leep_commit_and_land` and `leep_resolve_and_land` check
  the notices their commit got before landing (ADR 0096 amendment): a
  possible duplicate, a claim on or another session on one of the
  changed files, or a land on one of them since the base, not shown to
  the agent before, stops the call with `status: stopped` (not an
  error), the changeset it made, its ref and every such notice. The
  stopped changeset lands only with `force: true`; calling again reuses
  it, and without `force` stops again. `near`, directory scopes and
  related files never stop a land.
- **Changed.** A changeset create's answer carries every notice that
  stops a land of its files, past the cap of three, and a notice is said
  again when it comes to stop a file it did not stop before. Notices name
  the file two sessions share rather than a directory holding it, and a
  `landed` notice names the request's file it changed.
- **Fixed.** `leep_task_start` asks once more for a context pack the
  server shed with a `Retry-After` of at most 2 s, instead of reporting
  "Context unavailable" at once.
