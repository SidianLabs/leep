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
| History | `log`, `show`, `bisect`, `cat`, `blame`, `last-change`, `search`, `provenance`, `refs`, `reflog` |
| Review | `review` (`approve`, `reject`, `withdraw`, `status`, `list`, `request`, `inbox`, `cancel`), `reviews`, `checks` (`post`, `status`, `ref`, `culprits`) |
| Admin | `undo`, `keygen`, `config` |

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

Any of these may be followed by git-style suffixes
([ADR 0064](adr/0064-revision-suffixes.md)), applied left to right:

- `REV~N` is the Nth first-parent ancestor (`main~1` is the changeset before
  `main`); `REV~` is `REV~1`;
- `REV^N` is the Nth parent, so `REV^2` is a merge's second parent; `REV^`
  is `REV^1` and `REV^0` is `REV` itself;
- they chain: `main~2^2~1`, `@~1`, `2a954d8ef7c7^2`.

Walking past the first changeset, or asking for a parent that does not exist,
fails with exit 4. A malformed suffix (`main~x`, `main^{commit}`) and git's
reflog syntax (`main@{1}`; use `leep reflog`) fail with exit 1.

## Ignored files

`status`, `commit` and the other commands that scan the working copy skip
untracked files matched by the root `.gitignore` and the root `.leepignore`,
in gitignore syntax. Both are read, `.gitignore` first, so a repository
migrated from git keeps its ignores; a rule in `.leepignore` overrides one in
`.gitignore` (`!build/keep.txt` takes a path back). Only the files at the
working copy root are read: a `.gitignore` or `.leepignore` in a
subdirectory, `.git/info/exclude` and git's global excludes file are not.

Ignore rules only hide untracked files. A tracked file that a rule matches is
still scanned, and shows as `M` or `D` when it changes; to stop tracking it,
delete it and commit. `.leep` and `.git`
directories, at any depth, are never scanned.

## Author of new changesets

`commit`, `merge`, `cherry-pick`, `revert` and `stash` stamp new changesets
with the first author they find:

1. `--author` (where the command has it);
2. `LEEP_AUTHOR`;
3. `author` in the working copy's `.leep/config.json`, set by `leep config
   author`;
4. `author` in the user config file, set by `leep config --global author`:
   `$XDG_CONFIG_HOME/leep/config.json`, else `%APPDATA%\leep\config.json` on
   Windows, else `~/.config/leep/config.json`;
5. git's identity: `git config --get user.name` and `user.email`, run in the
   working copy root, as `Name <email>` (just the name without an email).
   A missing `git` or an unset name is skipped silently;
6. the login name (`USER`, `LOGNAME`, `USERNAME`);
7. the server's name for the token (its `author`, else its `name`; see
   DEPLOY.md), else `unknown`.

Blank values count as unset. `amend` keeps the author unless given
`--author`.

`leep config` reads and writes Leep's own settings; only `author` exists so
far, and any other key is a usage error (exit 2) that lists the supported
ones. It needs no server.

| Command | Effect |
|---|---|
| `leep config author "Name <email>"` | Set it in this working copy's `.leep/config.json`. |
| `leep config --global author "Name <email>"` | Set it in the user config file. |
| `leep config author` | Print the author in effect on stdout, and where it comes from on stderr. Works outside a working copy too. |
| `leep config --global author` | Print the user config file's value, if any. |
| `leep config --unset author` | Remove it from `.leep/config.json` (with `--global`, from the user config file). |

With `--json`, every form prints `{"author": ..., "source": ...}`. `source` is
one of `env`, `repo`, `user`, `git`, `login`, or `server` when nothing local
names an author (then `author` is `null`); after a set or unset it is the
level written (`repo` or `user`). A value with a line break or another
control character is refused (exit 1); so is an empty one: use `--unset`.

## Review requests

On a server with a tokens file, a change that needs review can ask for it
through the server instead of out of band
([ADR 0086](adr/0086-review-requests-and-creators.md)). Names are token
names, as in `OWNERS` files.

| Command | Effect |
|---|---|
| `leep review request REV [-m MSG]` | Ask for a review of `REV` before it lands on the target ref (`--ref` for another). With no `--to`, the server asks the owners of the paths still pending or rejected on that ref, or any reviewer when none is owned. Prints `Requested review of ID on REF from NAMES (#N)` and how to follow it up (`leep review status ID --ref REF --wait`, `leep review cancel N`); asking again for the same thing answers the open request (`Already requested ...`). `--wait[=SECS]` then waits for the verdict as `leep review status --wait` does. |
| `leep review request REV --to NAME [--to NAME]...` | Ask these reviewers instead. Each must be a token that may review, and not you; anything else is refused (exit 1). |
| `leep review inbox` | Open requests that name you, or that any reviewer may take and someone else made, newest first: `#N STATE ID -> REF by REQUESTER (created by CREATOR), for REVIEWERS (TIME)` (`for any reviewer, no owner pending` when no pending path had an owner to ask), then the changeset's title, its files (`files: a.rs, b.rs (+3)`, from servers with ADR 0089) and the note. The header ends with who answered (`, approved by you, rhea`, `, rejected by bo`: verdicts on the changeset itself; carried ones are judged by `review status`). Requests you already approved or rejected are left out, with a count of them at the end (also left out of `--json`); withdrawing your verdict brings one back. `--mine` lists the requests you made, `--all` adds closed ones and the ones you answered, `-n N` caps the list (50, counted before your answered ones are left out). `--wait[=SECS]` asks again, with `review status --wait`'s backoff, until something is listed (exit 0) or SECS pass (exit 1; default 3600, 0 for no limit). |
| `leep review cancel N` | Withdraw request `N`, which you made. |

The ref must exist (or be one the server protects), and you may have at most 100 open requests; one more
is refused (exit 1, `--json` `server_code: "too_many_review_requests"`)
until you cancel some or they land.

A request closes by itself when its changeset lands on its ref, as itself or
as the copy the land replayed it as; `leep review inbox --mine --all` shows
`landed as ID` for a copy. A changeset you squash or rebase yourself is a
new one: request again, and cancel the old request. `--json` prints the
server's answer. Against a server without review requests the commands say
the server needs an upgrade.

`leep show` prints `Created-by: NAME` after `Author:` when the server recorded which token
created the changeset, so a reviewer can tell agents apart even when they
share an author.

## CI checks

On a server with a checks policy, CI posts check results and the server keeps
`green/<ref>`, the newest changeset on the ref that passed every required
check ([ADR 0091](adr/0091-known-good-refs-and-check-attestations.md)).
Posting needs a token whose `checks` lists the check, and never works on a
changeset the token created; reading needs any token.

| Command | Effect |
|---|---|
| `leep checks post REV CHECK pass\|fail [--url URL] [-m SUMMARY]` | Record a result for `REV` (resolved to an id first: post for the changeset you tested). Prints `Recorded CHECK OUTCOME on ID`, and `green/REF moved to ID` for each known-good ref it promoted. A rerun replaces the earlier result. |
| `leep checks status [REV]` | The results on `REV` (default `@`): `OUTCOME CHECK by TOKEN TIME URL`, then `known good on REF` for each green ref that holds it. |
| `leep checks ref [REF]` | Where `REF` (default: the target ref) stands: its head, `green/REF` and how many changesets it is behind, the required checks, and `REF is red since ID (CHECK)` when the head fails. Exit 1 when red. |
| `leep checks culprits [REF] [--bad REV] [--good REV] [-n N] [--before SEQ]` | The suspects for a failure: the lands in the range above `--good` (default `green/REF`) up to the one that brought `--bad` (default the head), newest first, with ref log `seq`, id, who created it (or landed it), title, agent model and the paths changed. `-n` caps the page (100, at most 500); a full page ends with `(more: add --before SEQ)`. |

A suspect is a land in the range, never a verdict: a land whose test fails
only together with a later land shows the later one too. Read the paths,
rerun, then revert. No `green/` ref can be landed on, created, moved or
undone, and `green/REF` cannot be deleted while the policy names `REF` (exit
1, `--json` `server_code: "promotion_only"`). Results are taken only for
changesets some ref holds. `leep checkout
green/main` gives you its tree. Keep basing new work on the ref itself.

## Long listings

`leep log -n`, `leep search -n` and `leep reflog -n` may ask for more than the
server returns per request (10000 log entries, 1000 search matches, 1000 ref
log entries); the CLI fetches the rest page by page with the server's cursors.
With `--json`, a log or search that stopped at `-n` while more remains carries
`next`; `leep log --after NEXT` and `leep search --after NEXT`, with the same
other flags and revision (use the full id the first page started from if a ref
may move), continue from there. An older server without cursors answers one
page.

`leep log` prints each changeset's header and the first paragraph of its
message, marking a cut message with `[...]`, and leaves out `git.*` metadata:
history imported from git keeps each commit's raw headers there (`gpgsig`
signatures, encodings), kilobytes per entry. On a repository imported from
git, `leep log -n 5` printed 328,661 bytes before and prints 1,004 now.
`leep log -v` prints every key and the whole message, as `leep show` does;
`leep --json log` is unchanged.

`leep log --oneline` prints one line per changeset, the short id and the
message's first line. It asks the server for summary entries
(`fields=summary`: no changed paths, the message's first line, `agent.*`
metadata), about a quarter of the bytes of full ones on git/git; with
`--json` it prints them.

`leep log --graph` draws the commit graph left of the entries, with or
without `--oneline`, as `git log --graph` does: `*` marks each changeset,
`|` a line of history, `\` a merge's parents fanning out, `/` lines
joining, `_` a line crossing others to join one further left, and `*-.`
(one more `-.` per extra parent) an octopus merge. The server's log comes
newest first by generation, so each changeset follows all its children
shown; the lanes carry over every page the CLI fetches for one `-n`, and
stop where `-n` does. `--after` starts a new drawing at the page it
continues. With `--path`, `--follow`, `--author`, `--agent`,
`--agent-model`, `--agent-session` or `--extra`, `--graph` is refused
(exit 2): the changesets that connect the shown ones are left out, and the
CLI does not rewrite parents as git does. With `--json` it is ignored and
the raw response is printed.

## Reading one change

`leep diff REV` shows what changeset REV changed: its diff against its first
parent (a root changeset against the empty tree), the same as
`leep diff REV^ REV`. `leep show REV` prints the header with a `Parent:` line
per parent, the signers and the changed paths; `leep show -p REV` adds the
patch, and with `--json` puts it in a `patch` field beside the changeset.

`leep review status REV` prints one line per changed path, saying what
satisfied it (`approved by rhea (any reviewer)`, `approved by alice (owner)`,
`needs no review (auto:docs_only)`) or what it still needs
(`needs one of: alice, bob`), and ends with `diff: leep diff ID`. `--json`
keeps the server's rule names (`owner`, `pending`, ...). A path whose
verdict carried from the changeset's original ends `; carried from X
(identical)`, `(rebased onto the new base)` or `(both sides' insertions
kept)`. Before the `diff:` line, each original the changeset names
(`leep.rebased_from`) whose reviews could carry gets a line:
`replaces X, approved by rhea; carried`, or `replaces X, approved by rhea;
not carried: PATH differs (leep diff X; leep diff ID)`, so a reviewer can
compare the two and approve again
([ADR 0089](adr/0089-review-verdicts-carry-across-client-rebases.md)). Once the ref
holds the changeset (the server's `on_refs` names it,
[ADR 0087](adr/0087-review-requests-close-by-content.md)), the first line
says `landed on main` instead of `not approved` and the exit status is 0:
nothing is left to wait for. `leep review approve` and `reject` then read
the status on the target ref and print `now approved for main` or the
paths still waiting. A verdict on a changeset already on a protected ref
is stored all the same and prints `already on main: verdicts now only
affect copies (cherry-picks, rebases)`; a reject or withdrawal that the
server also wrote on originals whose approval carried here prints
`also rejected on ID` (or `withdrawn`) for each. `leep review list` marks
a review that carries from the changeset's original with
`(carried from ID)`; such reviews count for landing.

`leep review status REV --wait[=SECS]` asks again until the changeset is
approved for the ref or the ref holds it (exit 0), a path is rejected
(exit 1 at once, since someone has to act first) or SECS pass (exit 1,
default 3600; `--wait=0` waits without limit). It asks after 2 seconds,
then twice as late each time, at most a minute apart, and prints the last
status either way; what it is waiting for goes to stderr. A land refused
for review names both `leep review request ID --ref REF` (the refused
changeset, from the server's message; `@` when it names none) and this.

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

Between the `<<<<<<<` line and `=======` is the first side's text, between
`=======` and `>>>>>>>` the second's. The server writes diff3 style, with the
common ancestor in a `||||||| original` section before `=======`; the CLI
labels a non-empty one `||||||| base` and drops an empty one (both sides
added text where there was none), which says nothing. The conflict report
explains the sections in one paragraph. Only the labels are the CLI's: the
server's `merged_with_markers` text, which the MCP server passes on, still
says `ours`, `original` and `theirs`, and the `/v1` fields keep those names.

A conflicted file counts as resolved once no line starts with `<<<<<<< `,
`||||||| ` or `>>>>>>> `, so a half-deleted conflict still blocks `commit`,
`amend` and `land`, as do files written by older clients with
`<<<<<<< ours`. A `=======` line alone does not count: Markdown and
reStructuredText underline headings with it.

## Resolving a conflicted land

When `leep land` exits 5 the working copy is on the ref's head with your
changes applied and the conflicts marked; resolve them, `leep commit`, and
`leep land` again. When the land carried one changeset, `leep commit`
without `-m` reuses its message (the report says
`` `leep commit` (its message by default) ``); a stack of several has no one
message, so the report asks for `-m`. Until a land lands, `.leep/state.json` remembers the
conflicted land (`resolving`; `leep status` shows it and `leep --json status`
carries it), and the commit made on that head records the changesets it
replaces in `leep.rebased_from`: their ids, newest first and joined with
commas when the land carried a stack. A server can use it to carry review
verdicts over to the resolution where the reviewed changes are byte for byte
the same, or exactly what the server's own rebase of them onto the new head
would have made, or keep both sides' lines where both only added lines at
one place ([ADR 0089](adr/0089-review-verdicts-carry-across-client-rebases.md));
the ids are only a hint, and the server checks content. The conflict report
says so: other paths need review again. Any
checkout, `sync`, `switch` or `reset --hard` forgets the land.

A land the server replays onto a moved head gives the copy a new id and
prints `Landed ID on REF (rebased from ORIGINAL; reviews of the original
count)`: the copy's `leep.rebased_from` names the original, and the land
policy judged the original, so its reviews count for the copy.

## Bisect

`leep bisect` finds the changeset that introduced a bug by binary search,
with git's commands and semantics:

```sh
leep bisect start main v1.2     # BAD, then GOOD...; checks out the first candidate
leep bisect good                # or `bad`, or `skip`: marks the checked-out changeset
leep bisect bad 2a954d8ef7c7    # any revision, including `main~3` and `@`
leep bisect run cargo test -p parser
leep bisect log                 # the marks so far
leep bisect reset               # back to the base and target ref of `start`
```

`start` needs a clean working copy and a base. `bad`, `good` and `skip`
take revisions (default `@`, the checked-out changeset), and a changeset
has one mark: marking it again replaces the old one. The candidates are the
ancestors of the bad changeset that are not ancestors of any good one, minus
the skipped ones, so on merge-heavy history a culprit on a merged branch is
found, not the merge. Each step checks out the candidate that best halves
them (git's bisection weight: the most even split between the candidates it
has in its history and the rest) and prints

```
Bisecting: 9 revisions left to test after this (roughly 3 steps)
[<id>] <subject>
```

When one is left, `bisect` prints `<id> is the first bad changeset` and its
`leep show` summary; the working copy stays where it is until `reset`. If
only skipped changesets are left, it prints `There are only 'skip'ped
changesets left to test.`, `The first bad changeset could be any of:` and
their ids, and exits 1. Marks that leave no candidate (a bad changeset in the
history of a good one) are refused and not recorded.

`bisect run CMD [ARGS...]` runs the command in the current directory on each
candidate and marks it by the exit status, like git: 0 good, 125 skip, 1 to
127 bad; 128 or more, a signal, or a command that cannot start stops the
bisect with exit 1 and keeps the marks. With `--json` the command's stdout
goes to stderr, so stdout holds one document.

While a bisect is in progress, `status` says so (`--json`: `bisecting`, with
`bad`, `good`, `skip`, `original_base` and `original_ref`), and `commit`,
`amend`, `land`, `merge`, `rebase`, `cherry-pick`, `revert` and `sync`
refuse with a pointer to `leep bisect reset`. `checkout` and `switch` still
work; `reset` returns to where the bisect started either way. A step refuses
edits to tracked files, which the next checkout would overwrite; untracked
files (build output) may stay.

`--json` prints, for every step, `status` (`waiting`, `testing`, `found` or
`only_skipped`) with `bad`, `good` and `skip`, plus `current`, `remaining`
and `steps` when testing, `first_bad` (the changeset as `leep --json show`
prints it) when found, and `candidates` when only skipped ones are left.
`bisect run` prints one such document at the end, with `runs`: each tested
`id`, its `exit_code` and the `result`. `bisect reset` prints the `base`,
`target_ref`, `written` and `removed`; `bisect log` the marks as `entries`.

Each step reads the logs of the bad and good changesets from the server
(`GET /v1/log/{id}?fields=summary`, newest first by generation, in pages
that start at 128 entries and double). The bad changeset's log is read only
until every parent of a candidate has been seen, and each good changeset's
log only down to the oldest generation reached, so a step costs about as many
entries as there are candidates, plus one `GET /v1/changesets/{id}` per good
changeset and for the chosen one. Nothing but the marks is kept between
steps (`.leep/bisect.json`; see [FORMATS.md](FORMATS.md)). A step refuses
more than 100,000 candidates or 1,000,000 log entries read; mark a newer good
changeset. Up to 2,048 candidates every one is weighed exactly; above that,
the 256 nearest the middle of the generation order.

## Exit codes

Decided in [ADR 0034](adr/0034-cli-exit-codes-and-json-errors.md).

| Code | Meaning |
|---|---|
| 0 | Success. |
| 1 | Generic failure: bad input, a refused operation (dirty working copy, existing tag), authentication. Also "no" from `search` (no match), `review status` (not approved, and not yet on the ref; with `--wait`, also rejected or out of time) and `checks ref` (the ref is red). |
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
  | `LEEP_AUTHOR` | Author of new changesets; `--author` overrides it, and it overrides `leep config author` (see "Author of new changesets"). |
  | `LEEP_AGENT_MODEL`, `LEEP_AGENT_SESSION`, `LEEP_AGENT_PROMPT`, `LEEP_AGENT_TESTS` | Provenance recorded by `commit` and `amend` (`agent.*` metadata). |
  | `LEEP_SIGNING_KEY` | Key file that `commit`, `amend`, `merge`, `cherry-pick` and `revert` sign with. |
  | `LEEP_CACHE_DIR` | Local blob cache. |
  | `LEEP_UPLOAD_JOBS` | Blob uploads in flight at once when `commit`, `amend`, `sync` or `stash` send new files (8). |
  | `NO_COLOR`, `CLICOLOR_FORCE`, `CLICOLOR` | Colour, as above; output to a pipe is plain unless `CLICOLOR_FORCE` is set. |
  | `LEEP_PAGER`, `PAGER` | Pager for `log`, `diff` and `show` on a terminal; never used for a pipe. |

- `leep commit` prints the new changeset's full id on stdout when stdout is
  not a terminal, so `id=$(leep commit -m ...)` works, with
  `committed ID (N files)` on stderr; on a terminal it prints that one line
  only.
