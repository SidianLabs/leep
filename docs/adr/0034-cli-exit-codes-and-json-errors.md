# 0034. CLI exit codes and JSON errors
Status: Accepted
Date: 2026-10-17

## Context
Agents drive `leep` from a shell and decide what to do next from its exit
code. The CLI research (`docs/research/CLI-UX.md`, R3) found that the codes
did not say enough: `CliError` mapped every server or network failure to 2,
which is also clap's code for a bad command line, and 1 covered a land
conflict, "nothing to commit", a 404, a dirty working copy, an existing tag and
a missing server URL alike. An agent could not tell "retry later" from "my
command is wrong" from "go resolve the conflict".

The compatibility promise covers the CLI only from 1.0 (`docs/ROADMAP.md`),
so the codes can still change, but every agent built on them makes a later
change costlier. This record fixes them before 0.1.

## Decision
**Exit codes.** Every failure has a kind; the kind alone decides the code.

| Code | Meaning | Kinds (`--json` `code`) |
|---|---|---|
| 0 | Success | |
| 1 | Generic failure: bad input, a refused operation, authentication | `user`, `auth`, `ambiguous` |
| 2 | The command line does not parse (clap, unchanged) | `usage` |
| 3 | The server or the network failed; running the command again may work | `unreachable`, `server`, `timeout`, `rate_limited` |
| 4 | Something named does not exist: a revision, ref, branch, tag, file, stash or log entry | `not_found` |
| 5 | Conflict: content conflicts to resolve, or a concurrent update that won | `conflict` |
| 6 | Nothing to do: the requested change is already the state | `nothing_to_do` |
| 130 | Interrupted by SIGINT | |

- **Where the kind comes from.** HTTP answers map in one place, the client's
  `error_from`: no connection is `unreachable`, a client timeout or 408 is
  `timeout` (the request may have taken effect), 429 after the client's own
  retries is `rate_limited`, 5xx is `server`, 404 `not_found`, 409 `conflict`,
  401 and 403 `auth`, any other 4xx `user`. A `leep_core::Error` converts by
  variant (`NotFound`, `Conflict`, else `user`). Local checks construct the
  kind they mean (`CliError::not_found`, `conflict`, `lost_race`,
  `nothing_to_do`).
- **Conflicts.** Exit 5 covers both kinds of collision. Content conflicts
  (`land`, `sync`, `merge`, `cherry-pick`, `revert`, `stash apply` writing
  markers, `rebase` and `sync` stopping on a conflicting local commit) and
  refusals because conflicts or an unfinished merge or pick are still waiting
  (`commit`, `land` and the rest while markers remain) are not retryable: the
  way on is to resolve. A 409, a compare-and-swap lost to another writer
  (`branch -d`, `tag -d`, `undo`) and a land that kept losing races are
  retryable. `--json` errors say which in `retryable`. Commands that finish
  and report conflicts on stdout (`land`, `sync`, `merge`, `cherry-pick`,
  `revert`, `stash apply`/`pop`) print their usual report, or with `--json`
  their usual result document, and exit 5.
- **Nothing to do (6) is opt-in per command.** Only `commit` (clean working
  copy), `amend` (no changes and no new message) and `stash` / `stash push`
  (no local changes) use it. These record local work, so an agent re-running
  a "save what I did" step meets an already-satisfied request as a normal
  outcome; it is still non-zero so a `set -e` script stops exactly as it did
  with 1. Other "already done" answers stay what they were (`land` of a landed
  changeset succeeds; `undo` with nothing to undo is 1) because there the
  caller usually asked for a change and should look. `commit` while a
  cherry-pick or revert waits stays 1: the user still has a decision to make.
- **Predicates keep 1 for "no".** `search` with no match (text mode, like
  grep) and `review status` when not approved exit 1 without an error. They
  answer a question; 1 is the shell's "false". `search --json` keeps exiting 0
  with an empty `matches`.
- **Creating an existing branch or tag is 1**, not 5: the compare-and-swap
  lost to a ref that exists, a state that retrying will not change.
- **Integrity refusals are 3, not retryable**: a blob that does not hash to
  its address, a path the CLI refuses to write (a control directory, a
  symbolic link on the way). They come from the server's data, but repeating
  the request fetches the same data.
- **130.** The CLI installs no signal handler; SIGINT terminates it and the
  shell reports 128 + 2. Writes to the working copy go through a temp file
  and a rename, and the state is saved last (ADR 0017), so an interrupted
  command leaves either the old or the new file. A handler would add a
  dependency for no gain in safety.
- **Text output is unchanged** except that an error may add a second line,
  `hint: ...`, on stderr (for example where the server URL comes from when the
  server cannot be reached).

**JSON errors.** `--json` covered only results; an error was plain text on
stderr (R4), so an agent had to scrape messages. With `--json`, an error now
prints one line on stdout:

```json
{"error":{"code":"not_found","message":"server: not found: revision \"x\" (404 Not Found)","hint":null,"retryable":false}}
```

- `code` is the kind from the table above, a stable snake_case string; new
  kinds may be added, existing ones are never renamed. `message` is the
  human text, `hint` a suggested next step or null, `retryable` whether
  running the same command again can succeed. An ambiguous short id adds
  `candidates` (ADR 0035). Fields may be added, never removed.
- **stdout, deliberately.** Agents parse stdout, so with `--json` stdout
  always holds exactly one JSON document per run: the result or the error.
  `out=$(leep --json show X)` is always parseable, and the exit code says
  which it is. The human `leep: ...` line (and hint) still goes to stderr as
  without `--json`, so logs and tools that read stderr see the same text;
  clig.dev's "messages to stderr" holds for humans, and the machine document
  goes where machine output goes.
- **Usage errors too.** When clap rejects the command line and `--json`
  appears anywhere in it, the CLI prints the same shape with `code: "usage"`
  and exit 2 (clap could not parse the flag, so the raw arguments are
  checked). `--help` and `--version` print their usual text.
- **Outcomes are not errors.** A land, sync, merge, cherry-pick, revert or
  stash apply that reports conflicts prints its normal `--json` result (for
  example `{"status":"conflicts",...}`) and exits 5; `search` with no match
  prints its empty result and exits 0, as before.

## Consequences
- An agent can branch on the code alone: 3 means wait and retry, 4 means a
  name is wrong, 5 means resolve (or, for a retryable conflict, retry), 6
  means the work is already recorded.
- Scripts that tested for exactly 1 after a land conflict, a missing
  revision or "nothing to commit", or for 2 after a server failure, must
  change. Scripts that only tested for non-zero keep working. The soak agent
  and the end-to-end tests were updated with this change.
- New error sites must pick a kind deliberately; `CliError::user` is the
  default and means exit 1.
- An agent with `--json` never parses free text: success and failure are
  both one JSON document on stdout.
- `docs/CLI.md` lists the codes and the JSON shape for users and agents.
