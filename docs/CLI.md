# The `leep` command line

`leep` is the client for a Leep server. `leep --help` lists the commands and
`leep COMMAND --help` explains one. This page holds what the help cannot: the
exit codes and the contract scripts and agents can rely on.

## Revisions

Wherever a command takes a revision (`REV`, `FROM TO`, `--rev`, `--source`):

- `@` is the working copy's base changeset;
- a ref name (`main`, `release/1.0`) or a tag (`tags/v1`, or `v1` when no
  branch has that name);
- a changeset id, full (64 hex digits) or short: 4 or more leading digits, in
  any case, as long as only one changeset starts with them
  ([ADR 0034](adr/0034-short-changeset-ids.md)). `leep log` and the other
  commands print 12-digit ids, which work as they are.

A ref wins over a short id that spells the same. An ambiguous short id fails
with exit 1 and lists the candidates (`--json`: `code: "ambiguous"` and
`candidates`).

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

## JSON output

`--json` (on any command) prints the result as JSON on stdout, usually the
server's `/v1` response. With `--json`, an error is also JSON on stdout, one
line, in addition to the usual text on stderr:

```json
{"error":{"code":"not_found","message":"...","hint":null,"retryable":false}}
```

| `code` | Exit | Meaning |
|---|---|---|
| `user` | 1 | Bad input or a refused operation. |
| `auth` | 1 | Missing, unknown or read-only token (401, 403). |
| `ambiguous` | 1 | A short changeset id matches several changesets; `candidates` lists them. |
| `usage` | 2 | The command line does not parse. |
| `unreachable` | 3 | No connection to the server. |
| `server` | 3 | A 5xx answer, an answer that does not decode, or data that fails its hash check. |
| `timeout` | 3 | The request timed out; it may or may not have taken effect. |
| `rate_limited` | 3 | 429 after the client's own retries. |
| `not_found` | 4 | Something named does not exist. |
| `conflict` | 5 | Conflicts to resolve, or (with `retryable: true`) a concurrent update that won. |
| `nothing_to_do` | 6 | Nothing to record. |

`code` values are stable; new ones may be added. `retryable` says whether
running the same command again can succeed. Commands that finish with
conflicts (`land`, `sync`, `merge`, `cherry-pick`, `revert`, `stash apply`)
print their normal JSON result and exit 5 rather than an error.

## For agents

- **Use `--json`.** stdout then always holds exactly one JSON document, the
  result or `{"error":{...}}`, so nothing needs scraping.
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
