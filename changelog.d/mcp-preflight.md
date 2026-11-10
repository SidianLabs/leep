### MCP lands stop for overlaps the agent has not seen
- **Changed.** `leep_commit_and_land` and `leep_resolve_and_land` check
  the notices their commit got before landing (ADR 0096 amendment): a
  possible duplicate, a claim on or another session on one of the
  changed files, or a land on one of them since the base, not shown to
  the agent before, stops the call with `status: stopped`, the changeset
  it made and the notices. `force: true` lands anyway; calling again
  reuses the stopped changeset instead of making another. `near`,
  directory scopes and related files never stop a land.
- **Changed.** Overlap notices name the file two sessions share rather
  than a directory holding it, and a `landed` notice names a file of
  yours the land changed before a directory of yours.
- **Fixed.** `leep_task_start` asks once more for a context pack the
  server shed with a `Retry-After` of at most 2 s, instead of reporting
  "Context unavailable" at once.
