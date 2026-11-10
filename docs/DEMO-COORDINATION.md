# The coordination demo

Twenty scripted agents on one repository, showing what the coordination
layer of ADRs [0095](adr/0095-coordination-state.md) (sessions),
[0096](adr/0096-overlap-notices-and-duplicate-flags.md) (notices) and
[0097](adr/0097-context-pack.md) (context packs) tells agents before they
land. The agents are scripts, not models: each runs the commands an agent
would, through the real `leep-server`, `leep` and `leep-mcp` binaries, with
its own token (its principal) and its own `agent.session`.

```sh
scripts/demo-coordination.sh                    # SQLite; prints the transcript
scripts/demo-coordination.sh --out demo.md      # and writes it to demo.md
LEEP_TEST_POSTGRES_URL=postgres://... scripts/demo-coordination.sh --postgres
```

The script builds the workspace and runs the `coord_demo` example of
`leep-e2e`, which prints a Markdown transcript: every command or tool call,
its output and the notices it carried, then a table of the claims below and
the notices each agent heard, by level. It exits 1 when a claim did not
hold. The same scenario is the test `cargo test -p leep-e2e --test
coord_demo` (on Postgres too when `LEEP_TEST_POSTGRES_URL` is set), which
asserts the claims. Nothing in it bounds time: a wait is a bounded number
of retries of a command, so a loaded machine makes it slower, not flaky.

## The cast

| Agents | Via | Work |
|---|---|---|
| ada, bo, cy | `leep` | `src/auth/`: ada rotates keys in `keys.rs`; bo claims `token.rs`, cy scopes it; both edit it |
| dee, eli | `leep-mcp` | both handed issue `BILL-42` (`src/billing/invoice.rs`), worded differently |
| fay | `leep-mcp` | arrives last, to add refresh tokens to `parse_token` |
| gus ... tess (14) | 12 `leep`, 2 `leep-mcp` | one file each in their own area, concurrently with everything else; rex and sol share `src/api/` and ask for `near` notices |

A maintainer seeds the repository one area at a time (so a file's
co-changes are its own area's), with `OWNERS` files naming owners and one
`carry identical` directory, and a CI token passes the required check
`test` on the seed. After bo lands, CI fails `test` on the head, so the
ref is red with bo's land as a suspect.

## The claims

1. `leep who src/auth/` shows the three agents working there.
2. bo and cy, editing `token.rs`, are each warned about the other
   (`claimed` at cy's `leep work start`, `same_file` on bo's commit)
   before either lands.
3. eli's `leep_task_start` lists dee's session as a `possible_duplicate`
   by task key in its pack; the result's notices say so too.
4. bo lands and cy's next `leep status` names the changeset in a
   `landed` notice; dee lands over MCP and eli's next tool call names it.
5. fay's first call, `leep_task_start`, answers the owners of its file,
   its last change with the agent and model that made it, the live work on
   it (cy) and the red ref with bo's land as the suspect touching it.
6. The fourteen solo agents all land, and hear nothing about each other
   but `near` (rex and sol, who asked for it).

## Reading the transcript critically

The demo is also a check on how the notices read. Things to look at:

- A pack's "work in flight" ranks levels by the agent's own paths, so it
  agrees with the notices: rex's pack and notice both call sol `near`.
  Work met only on a file the task turned up (a co-change) is marked
  "a file related to your task" and listed last (fay's pack names ada so
  on `keys.rs`).
- Both agents of a duplicate pair are told: eli at `leep_task_start`, dee
  on its next answer (the task-key lookup runs on creates, lands and notice
  reads too), as `possible_duplicate` by task key.
- eli's `landed` notice for dee's land is tagged `by task_key`, naming dee
  and telling eli its duplicate landed the task.
- `leep_commit_and_land` creates and lands in one call, so an MCP agent
  hears of overlaps on its commit after it has landed.
- Packs repeat the same owners and last change for every co-changed file.
