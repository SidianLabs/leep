# The `leep` command line

`leep` is the client for a Leep server. `leep --help` lists the commands and
`leep COMMAND --help` explains one. This page holds what the help cannot: the
exit codes and the contract scripts and agents can rely on.

## Commands

`leep --help` groups the commands by task, and `leep COMMAND --help` shows a
command's options with examples for the common ones.

| Group | Commands |
|---|---|
| Work | `init`, `checkout`, `status`, `diff`, `commit`, `amend`, `stash`, `restore`, `resolve`, `reset`, `sync`, `land` |
| Branches | `branch`, `switch`, `tag`, `merge`, `rebase`, `cherry-pick`, `revert` |
| History | `log`, `show`, `cat`, `blame`, `last-change`, `search`, `provenance`, `refs`, `reflog` |
| Review | `review` (`approve`, `reject`, `status`, `list`), `reviews` |
| Admin | `undo`, `keygen` |

Global options, accepted by every command and listed only in `leep --help`:

| Option | Effect |
|---|---|
| `--server URL` | Server URL; overrides `LEEP_SERVER`, which overrides `.leep/config.json`. |
| `--token TOKEN` | Bearer token; overrides `LEEP_TOKEN` and `.leep/config.json`. Prefer `LEEP_TOKEN`: a flag is visible in process listings. |
| `--json` | Print results as JSON (usually the raw `/v1` response), and errors as one JSON document (below). |
| `--color WHEN` | `auto` (default), `always` or `never`; see "Colour and paging". |

## Colour and paging

Human output is coloured only when stdout is a terminal, unless the flag or
the environment says otherwise: `--color always` or `never` wins; in `auto`,
a non-empty `NO_COLOR` turns colour off, then `CLICOLOR_FORCE` (not `0`)
forces it on, then `CLICOLOR=0` or `TERM=dumb` turn it off. `--json` output is
never coloured. Diffs, status letters and changeset headers are coloured.

On a terminal, `log`, `diff` and `show` page through `LEEP_PAGER`, else
`PAGER`, else `less` (with `LESS=FRX` unless `LESS` is set, so a short output
just prints). An empty `LEEP_PAGER`, or `cat`, turns paging off. Output to a
pipe or a file, and `--json`, is never paged.

## Revisions

Wherever a command takes a revision (`REV`, `FROM TO`, `--rev`, `--source`):

- `@` is the working copy's base changeset;
- a ref name (`main`, `release/1.0`) or a tag (`tags/v1`, or `v1` when no
  branch has that name);
- a changeset id, full (64 hex digits) or short: 4 or more leading digits, in
  any case, as long as only one changeset starts with them
  ([ADR 0035](adr/0035-short-changeset-ids.md)). `leep log` and the other
  commands print 12-digit ids, which work as they are.

A ref wins over a short id that spells the same. An ambiguous short id fails
with exit 1 and lists the candidates (`--json`: `code: "ambiguous"` and
`candidates`).

## Long listings

`leep log -n`, `leep search -n` and `leep reflog -n` may ask for more than the
server returns per request (10000 log entries, 1000 search matches, 1000 ref
log entries); the CLI fetches the rest page by page with the server's cursors.
With `--json`, a log or search that stopped at `-n` while more remains carries
`next`; `leep log --after NEXT` and `leep search --after NEXT`, with the same
other flags and revision (use the full id the first page started from if a ref
may move), continue from there. An older server without cursors answers one
page.

`leep log --oneline` prints one line per changeset, the short id and the
message's first line. It asks the server for summary entries
(`fields=summary`: no changed paths, the message's first line, `agent.*`
metadata), about a quarter of the bytes of full ones on git/git; with
`--json` it prints them. `leep log` and `leep log --json` are unchanged.

## Landing onto a new ref

`leep land` moves the working copy's target ref, or the ref given with
`--ref`. A ref that does not exist is not created unless you pass `--create`
([ADR 0036](adr/0036-explicit-ref-creation-on-land.md)), so a typo such as
`--ref mian` fails with exit 4 instead of making a branch. The one exception
is the first land onto `main` of an empty repository. When a land creates a
ref, `leep` prints `created ref NAME` on stderr.

## The base against the target ref

`leep status` says where the working copy's base stands against the head of
its target ref. `leep --json status` carries the same in `on_target`,
`ahead` and `behind`:

| Case | Text | `on_target` | `ahead` | `behind` |
|---|---|---|---|---|
| The base is in the target's history | (nothing) | `true` | `null` | absent |
| The target's head is in the base's history | `base is N commits ahead of main` | `false` | N | absent |
| Each side has commits the other lacks | `base is N commits ahead, M commits behind main` | `false` | N | M |
| No common ancestor | `base shares no history with main` | `false` | `null` | absent |
| Server unreachable, or no such ref | (nothing) | `null` | `null` | absent |

The counts are generation differences from the merge base, exact for a
linear history. `leep sync` moves the local commits onto the head in the
last two cases.

`leep sync --no-rebase` moves nothing local: it refuses (exit 1) a working
copy with uncommitted changes, and a base that is not in the target's history,
naming the local commits it would leave behind. `--discard`, which needs
`--no-rebase`, syncs anyway and prints the abandoned base's id on stderr;
`leep checkout ID` returns to it and `leep branch NAME ID` keeps it, until
then no ref points at it.

## Conflict markers

When a land, sync, merge, cherry-pick, revert or stash apply cannot merge a
text file, `leep` writes conflict markers into it, labelled by role:

| Command | `<<<<<<<` side | `>>>>>>>` side |
|---|---|---|
| `land`, `sync` | `TARGET (landed)`, for example `main (landed)` | `yours` |
| `merge REV` | `yours` | `ID (merging)` |
| `cherry-pick REV`, `revert REV` | `yours` | `ID (cherry-pick)`, `ID (revert)` |
| `stash apply`, `stash pop` | `working copy` | `stash ID` |

A diff3 base section (`||||||| original`) may sit between them. Only the
labels are the CLI's: the server's `merged_with_markers` text, which the MCP
server passes on, still says `ours` and `theirs`, and the `/v1` fields keep
those names. Detection only looks for a line starting with `<<<<<<< `, so
files written by older clients with `<<<<<<< ours` still block `commit` and
`land` until resolved.

## Exit codes

Decided in [ADR 0034](adr/0034-cli-exit-codes-and-json-errors.md).

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
| 141 | stdout was closed before the output was written (`leep log \| head -1`); the shell reports 128 + 13 for SIGPIPE. Nothing is printed on stderr. |

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
| `indexing` | 3 | The server's index lags too far behind and it answered 503 after `LEEP_INDEX_WAIT` seconds (60) of retries; `retry_after_ms` says when to try again (ADR 0058). `log`, `last-change`, `blame` and `search` print `indexing: N changesets behind ..., retrying in Ss` while they retry, and `--stale-ok` takes an answer at the newest indexed changeset instead, printing `(answered at X, N behind)` on stderr. |
| `not_found` | 4 | Something named does not exist. |
| `conflict` | 5 | Conflicts to resolve, or (with `retryable: true`) a concurrent update that won. |
| `nothing_to_do` | 6 | Nothing to record. |

When the server named the error (servers since error codes; see
[FORMATS.md](FORMATS.md), "Error codes"), the object also carries
`server_code`, such as `not_signed`, `needs_review` or `ref_missing`. A
`land_id_reused` refusal also carries `land`: the server's record of that
land id (`land_id`, the `changeset` it was recorded for and the changeset
the ref `landed` on), from servers that send it.

Every request of one `leep` invocation carries the same random
`X-Request-Id`. An error from a request carries it as `request_id` (the id
the server answered with, when it echoed one), and the text on stderr ends
with `request id: ID`. Quote it in a bug report: the server logs every
request under its id.

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
  | `NO_COLOR`, `CLICOLOR_FORCE`, `CLICOLOR` | Colour, as above; output to a pipe is plain unless `CLICOLOR_FORCE` is set. |
  | `LEEP_PAGER`, `PAGER` | Pager for `log`, `diff` and `show` on a terminal; never used for a pipe. |

- `leep commit` prints the new changeset's full id on stdout, so
  `id=$(leep commit -m ...)` works.
