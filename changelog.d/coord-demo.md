### A twenty-agent coordination demo
- **Added.** `scripts/demo-coordination.sh` runs twenty scripted agents,
  each with its own token and agent session, on one repository through the
  real `leep-server`, `leep` and `leep-mcp`, and prints a narrated
  transcript: who works where, the warnings two agents on one file get
  before landing, duplicate tasks flagged at `leep_task_start`, `landed`
  notices naming the changeset, a newcomer's context pack, and the notices
  every agent heard (docs/DEMO-COORDINATION.md).
- **Tests.** The same scenario is the `leep-e2e` test `coord_demo`, on
  SQLite and Postgres.
