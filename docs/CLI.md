# The `leep` command line

`leep` is the client for a Leep server. `leep --help` lists the commands and
`leep COMMAND --help` explains one. This page holds what the help cannot: the
exit codes and the contract scripts and agents can rely on.

## Exit codes

Decided in [ADR 0033](adr/0033-cli-exit-codes-and-json-errors.md).

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | Generic failure: bad input, a refused operation (dirty working copy, existing tag), authentication. Also "no" from `search` (no match) and `review status` (not approved). |
| 2 | The command line does not parse. |
| 3 | The server or the network failed: unreachable, 5xx, a timeout, rate limited after retries. Running the command again may work. |
| 4 | Not found: a revision, ref, branch, tag, file, stash or log entry. |
| 5 | Conflict: content conflicts to resolve (a land, sync, merge, cherry-pick, revert or stash apply that wrote markers; a command refused while conflicts wait), or a concurrent update that won (409, a lost compare-and-swap). |
| 6 | Nothing to do: `commit`, `amend` or `stash push` found nothing to record. |
| 130 | Interrupted (SIGINT; the shell reports 128 + 2). |

Errors print `leep: MESSAGE` on stderr, sometimes followed by `hint: ...`.

## For agents

- **Branch on the exit code.** 3 means wait and retry; 4 means a name is
  wrong; 5 means resolve the conflicts (`leep status` lists them) and commit,
  or for a lost race, retry; 6 means the work is already recorded.
- **Leep never prompts** and never reads stdin, so it is safe to run with a
  closed or redirected stdin.
- **Environment:**

  | Variable | Effect |
  |---|---|
  | `LEEP_SERVER` | Server URL; `--server` overrides it, and it overrides `.leep/config.json`. |
  | `LEEP_TOKEN` | Bearer token; `--token` overrides it, and it overrides `.leep/config.json`. |
  | `LEEP_AUTHOR` | Author of new changesets. |
  | `LEEP_AGENT_MODEL`, `LEEP_AGENT_SESSION`, `LEEP_AGENT_PROMPT`, `LEEP_AGENT_TESTS` | Provenance recorded by `commit` and `amend` (`agent.*` metadata). |
  | `LEEP_SIGNING_KEY` | Key file that `commit`, `amend`, `merge`, `cherry-pick` and `revert` sign with. |
  | `LEEP_CACHE_DIR` | Local blob cache. |

- `leep commit` prints the new changeset's full id on stdout, so
  `id=$(leep commit -m ...)` works.
