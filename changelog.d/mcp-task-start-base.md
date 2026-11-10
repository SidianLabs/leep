### `leep_task_start` records a base, so lands on the task's files are noticed
- **Fixed.** A session started with `leep_task_start` had no base, so an
  agent working over MCP (no working copy) was never told with a `landed`
  notice that another agent had landed a change to its files, even one
  given the same task. A new session's base is now the head its first
  context pack describes; a resumed session keeps the base it has.
