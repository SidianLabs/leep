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
| (maintainer) | `leep` | starts documenting token refresh in `docs/auth.md` before fay arrives |
| rhea (a person) | `leep` | an owner of `src/auth/` with a `"kind": "human"` token; endorses ada's note on `token.rs` |
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
   (`claimed` in the pack of cy's `leep work start`, `same_file` on bo's
   commit) before either lands.
3. eli's `leep_task_start` lists dee's session as a `possible_duplicate`
   by task key in its pack (and not again as a notice).
4. bo lands and cy's next `leep status` names the changeset in a
   `landed` notice; dee lands over MCP and eli's next tool call names it.
5. fay's first call, `leep_task_start`, answers the owners of its file,
   its last change with the agent and model that made it, the live work on
   it (cy) and the red ref with bo's land as the suspect touching it.
6. The fourteen solo agents all land, and hear nothing about each other
   but `near` (rex and sol, who asked for it).
7. A pack ranks live work by the agent's own files, as notices do: the
   pack of rex or sol names the other, and as `near` (sol starts first and
   lands only after rex has started, so this never depends on
   scheduling); fay's pack lists the maintainer's live work on
   `docs/auth.md`, which changed with `token.rs`, as on "a file related to
   your task", after cy's work on fay's own file.
8. dee, the first of the `BILL-42` pair, is told `possible_duplicate` of
   eli by task key, and eli's `landed` notice for dee's land says its
   duplicate landed the task.
9. A task start says each thing once: eli's names dee as a duplicate on
   exactly one line, and dee's pack explains `carry identical` exactly
   once.
10. dee's `leep_commit_and_land` into eli's file (the same task key, the
    same file) is stopped by the pre-flight check before it lands: the
    result names the changeset it made and eli's notice, and `main` has not
    moved. dee lands that changeset with `changeset` and `force: true`, and
    no second changeset is made.
11. ada's note on `token.rs` (`leep note add`), endorsed by rhea, a human
    owner (`leep note endorse`), is in fay's `leep_task_start` pack: the
    line outside the fence says `endorsed by owner rhea`, and its title
    and body are only in the fence (ADR 0098).

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
- The pre-flight check (ADR 0096 amendment) stops dee's commit before it
  lands, so dee hears of eli first; it reads the warning and lands with
  `force`. Only notices dee had not been shown stop it; the stopped
  changeset lands only with `force`, so a blind retry is stopped again,
  never landed. The solo agents' commits, which meet nothing louder than
  `near`, land in one call as before.
- Known limit, left to the maintainer: `leep-mcp` asks for notices at most
  every 30 s when a tool's answers carry none, so a `landed` notice can
  take up to 30 s to reach an MCP agent that is only reading.
- Packs list owners and last changes once per distinct answer, naming the
  files each covers, and `leep_task_start` does not repeat as a notice
  what its pack shows.
- A note reaches the next agent in its pack, last: endorsed by a person,
  it ranks first and brings its body; an agent's unendorsed note would
  bring only its title. Its words are fenced like every other agent's.
