# CLI UX research

Status: research input, not a decision. Written 2026-10-16 against `main` at
505f9e4 (CLI version 0.0.1). Backlog items that change `/v1` or persisted data
need an ADR; every `/v1` change here is additive only.

Method: sources were fetched on 2026-10-16. Where a fetch failed or only a
search snippet was available, the text says so. Section 4 uses commands run
against a real `leep-server` (SQLite, filesystem blobs) built from this commit.

## 1. Executive summary

1. Leep already avoids git's biggest conceptual traps: no staging area, no
   detached HEAD, no ours/theirs flip between merge and rebase, `land` is
   idempotent, `undo`/`reflog` exist, and `--json` is on every command.
2. The CLI is functional but plain: no colour, no TTY detection, no pager, no
   completions, no `--help` examples, no `--plain`/`--no-input`.
3. The most agent-relevant gap is machine contracts. Errors are never JSON, exit
   code 2 means both "usage error" (clap) and "server/network failure", and a
   land conflict, "nothing to commit" and "not found" all exit 1.
4. The most human-relevant gap is identifiers. Every command prints 12-char ids,
   but no command accepts them back (`leep show 2a954d8ef7c7` is a 404).
5. `leep land --ref nosuch` silently creates a new ref. A typo should not create
   a branch on the production server.
6. Conflict markers use `ours`/`theirs`, where `ours` is the already-landed side
   and `theirs` is the user's own change. That is the opposite of most people's
   intuition and is git's most-polled confusion.
7. jj and Sapling show where the ceiling is: a smartlog-style overview, `undo`
   for every operation, conflicts that are data, not a blocked state. Leep has
   the server-side ingredients (reflog, review status, provenance) to do this.
8. Recommended P0 set is small (S/M effort, all additive): accept short ids and
   `--ref` safety, split exit codes, JSON errors, colour/TTY/`NO_COLOR`,
   examples in `--help`, completions.
9. UI: do not build a web UI now. Build a read-only terminal "smartlog"
   (`leep log --graph` / `leep activity`) first; a web view comes after design
   partners ask (it is already M7 on the roadmap).
10. Everything in section 5 is a CLI-side change except the items marked
    `/v1`.

## 2. Git pain points by theme, with sources

Frequency below is how often the theme appears in the sources I could read, not a
survey. I found no quantitative "top complaint" ranking for git; the Stack
Overflow survey page I fetched did not cover version-control satisfaction (see
"Could not verify").

### 2.1 Confusing terminology (very common)
- Julia Evans, "confusing git terminology" (2023-11-01), https://jvns.ca/blog/2023/11/01/confusing-git-terminology/
  lists about 19 terms. Examples: "Checkout does two totally unrelated things",
  "Index/staged/cached: All of these refer to the exact same thing", "Reset/revert/restore
  ... are very similar words and it's hard to differentiate them", remote-tracking
  branch "which is not a kind of branch (I'm sorry)", and the reflog as "one of the most
  confusing parts of git's UI".
- Julia Evans, "How HEAD works in git" (2024-03), https://jvns.ca/blog/2024/03/08/how-head-works-in-git/
  says HEAD "has a few different closely related meanings" and notes git says
  "detached HEAD" but never "attached HEAD".
- Julia Evans' git poll results (2024-03), https://jvns.ca/blog/2024/03/28/git-poll-results/
  report: 14% have "literally no idea" what HEAD means, 21% fast-forward, 29% ref;
  48% did not know the sides of a conflict flip between merge and rebase (another
  21% answered "what?").
- Steve Bennett, "10 things I hate about Git" (2012-02-24), https://stevebennett.me/2012/02/24/10-things-i-hate-about-git/:
  "There is essentially no distinction between implementation detail and user interface."
- Perez De Rosso and Jackson analysed git with a concepts/purposes/misfits
  framework and built Gitless to fix it: "We think the problem lies deeper than
  the user interface, in the concepts underlying Git." (https://sdg.csail.mit.edu/project/gitless;
  the paper list there names "Purposes, Concepts, Misfits, and a Redesign of Git",
  OOPSLA 2016, and "What's Wrong with Git? A Conceptual Design Analysis", Onward! 2013.)
  A summary of the OOPSLA paper's user study says participants found Gitless
  "more satisfying and less frustrating" with no big difference in efficiency
  (https://neverworkintheory.org/2016/09/30/rethinking-git.html).

### 2.2 Destructive operations and fear of losing work (common)
- Evans' poll: 17% said they lost work to a git problem in the last year or two; 4%
  said "no, but git did something else unforgivable".
- HN thread "Three words against git: Detached Head State" (2015-02-18),
  https://news.ycombinator.com/item?id=9066376: "git ate my changes and I had to reflog
  to recover them" and "Once you get a detached head state or corrupted repo, then
  you need a git expert to clean things up."
- Bennett: "Git breaks this promise." (data loss, his item 7.)
- Rev News 139 (2026-09-30) quotes developer Harald Nordgren: "many are very afraid to
  mess with their Git settings" (per the fetch summary; I did not open the spotlight itself).

### 2.3 Merge-conflict UX (common)
- Evans: 14% had seen a production bug from a bad conflict resolution "many
  times", 47% "once or twice" (poll results above).
- Evans, "popular git config options" (2024-02-16), https://jvns.ca/blog/2024/02/16/popular-git-config-options/:
  the default conflict view lacks context (`merge.conflictstyle`), people want
  conflicts "more readable".
- "ours/theirs" flipping: 48% did not know (poll above).

### 2.4 Rebase pain and history rewriting (common)
- Evans' config post: "The goal is to make it easier to modify old commits"; stacked
  branches are hard to rebase together until `rebase.updateRefs`.
- Bennett: "rebase encourages you to lie in order to provide other developers with a
  'clean', 'uncluttered' history."

### 2.5 Inconsistent flags and commands (common)
- Bennett: "The command line syntax is completely arbitrary and inconsistent."
- Evans: `..` vs `...` "do something completely different than they do with `git log`";
  `HEAD^` vs `HEAD~`.
- A single command doing many jobs: "Checkout does two totally unrelated things" (Evans).
  The git-switch manual page I fetched does not state why `switch`/`restore` exist.

### 2.6 Poor error messages and status text (occasional in what I read)
- Evans: "Your branch is up to date with 'origin/main'" can mislead; 13% answered
  "what?" to whether it could be out of date.
- Bennett, on documentation written "from the perspective of a computer scientist,
  not a user".

### 2.7 Staging area (common in design critiques)
- A design-analysis discussion on HN (mirror, not verified at the original site) notes
  that staged and working versions are coupled so that add, edit, reset can overwrite
  the working copy. Evans lists index/staged/cached as three names for one thing.
- Sapling: "In Sapling, there is no staging area." (https://sapling-scm.com/docs/introduction/differences-git)

### 2.8 Submodules, large repos, auth
- Evans (config post): "I've never understood anything about submodules." Only one
  quote found; not a strong signal.
- Large-repo performance and auth: I found no citable user complaints in this pass.
  Not covered here; see "Could not verify". Leep's lazy sparse checkouts and server-side
  search already target the large-repo case.

### 2.9 Agent-specific (new, 2026)
- Several 2026 practitioner write-ups say parallel agents on one checkout cause silent
  overwrites and contention over the git index lock; the usual fix is one worktree per
  agent, which "moves conflicts to merge time" (Augment, https://augmentcode.com/guides/git-worktrees-parallel-ai-agent-execution;
  also mindstudio.ai, noqta.tn, wavect.io). I read only search snippets, not the full
  pages; they are vendor and practitioner blogs, not research.

## 3. Ideas from newer tools and CLI guidance

### 3.1 Jujutsu (jj)
Source: https://docs.jj-vcs.dev/latest/git-comparison/ (the old jj-vcs.github.io URL 301-redirects).
- "The working copy is automatically committed." and "There's no index (staging area)."
  Leep has no staging area already; auto-snapshotting the working copy is a larger change
  that Leep's `stash` and `amend` only partly cover.
- "The operation log replaces reflogs." It records atomic updates to all refs and powers
  `jj undo` (https://docs.jj-vcs.dev/latest/operation-log/). Leep's `reflog` is per ref;
  `undo` moves one ref. Take: `leep undo` for working-copy-affecting commands too (local
  state), and an "operation id" that groups multi-ref changes.
- "Conflicts can be committed." and "No commands fail because of merge conflicts"
  (https://docs.jj-vcs.dev/latest/conflicts/): conflicts are stored logically, not as
  markers, so there is no `--continue`. Leep deliberately never lands conflicts
  (ARCHITECTURE: no silent server resolution), so take the UX, not the model: make
  `land` conflicts a first-class, resumable state with one next command.
- "Descendant commits are automatically rebased." Leep's `sync` already rebases local
  commits ("Moved 1 local commit onto main").
- Revsets: only linked from the page I fetched, so I did not read the language reference.

### 3.2 Sapling
Sources: https://sapling-scm.com/docs/overview/smartlog and
https://sapling-scm.com/docs/introduction/differences-git
- Smartlog "hid[es] all commits that aren't relevant to you", is "the centerpiece of
  Sapling's user experience", marks landed or rewritten commits (`x`, "Landed as"), and
  `sl ssl` adds review and test status from the server.
- `absorb`, `uncommit`, `unamend`, `unhide`, `undo`, `undo -i`, and "each command generally
  does one thing".
- `sl web` is an auto-refreshing interactive view.
Take: a smartlog for Leep is natural because the server knows landing, review status
(`leep review`) and provenance.

### 3.3 GitHub CLI (gh)
Sources: https://cli.github.com/manual/gh_help_formatting and
https://cli.github.com/manual/gh_help_environment (an exit-codes page returned 404).
- `--json fields`, `--jq` built in ("does not need to be installed"), `--template`.
  Running `--json` with no field list prints the valid fields.
- Environment contract: `GH_TOKEN`, `GH_PROMPT_DISABLED`, `NO_COLOR`, `CLICOLOR`,
  `CLICOLOR_FORCE`, `GH_FORCE_TTY`, `GH_SPINNER_DISABLED`, `GH_NO_UPDATE_NOTIFIER`, `GH_DEBUG`.
- Telemetry with an explicit off switch (`GH_TELEMETRY`, `DO_NOT_TRACK`).
Take: a documented environment contract for agents; `--json` field selection and `--jq`.

### 3.4 Stacks: Graphite, git-branchless, git-machete
- git-branchless: `git undo` "can undo any operation to the commit graph"; `-i` browses
  previous states (https://github.com/arxanas/git-branchless/wiki/Command:-git-undo).
  Working-copy changes are undoable only if a snapshot captured them.
- git-machete: a branch-stack layout file, `status` shows what would be rebased/pushed,
  `traverse` walks the stack (https://github.com/VirtusLab/git-machete).
- Graphite: my fetch hit a redirect to graphite.com and I did not follow it, so I have
  nothing citable from it.
Leep relevance: moderate. Agents land small independent changesets, so stacks matter
less than for human PR workflows; `rebase`/`sync` already move local commits.

### 3.5 CLI guidelines (clig.dev, https://clig.dev/)
Applicable points, with exact quotes:
- "Human-readable output is paramount." Machine-readable output goes to stdout, messages to stderr.
- Disable colour when not a TTY, when `NO_COLOR` is set, when `TERM=dumb`, or with `--no-color`.
- Offer `--json`; "use `--plain` to display output in plain, tabular text format".
- "Only use prompts or interactive elements if `stdin` is an interactive terminal." Support `--no-input`.
- "Print something to the user in <100ms." Show progress for slow work; no animations when not a TTY.
- "If the user did something wrong and you can guess what they meant, suggest it."
- Rewrite expected errors so they say what to do next; "Make it recoverable."
- Concise help with one or two examples; lead with examples; link to docs.
- "Map distinct non-zero codes to the most important failure modes."
- Flags > env vars > project config > user config > system config; follow XDG.
- "Do not read secrets from environment variables." (Leep currently reads `LEEP_TOKEN`; the
  guideline is debatable for CI and agents, but `--token` on the command line is the worse
  leak and should be discouraged in help.)
- "Do not phone home usage or crash data without consent." Leep has no telemetry; keep it that way.

### 3.6 Tools for agents
There is no authoritative standard. What I found are practitioner conventions
(tessl.io agent-cli and ai-native-cli skill specs; GitLab CLI issue
https://gitlab.com/gitlab-org/cli/-/issues/8177, which proposes structured errors with a
code, a retryable flag and a suggested fix). Common asks:
- JSON on stdout, diagnostics on stderr; never prompt; distinct documented exit codes
  (the GitLab proposal: 0 ok, 1 general, 2 usage, 3 auth, 4 not found, 130 interrupt);
  machine-readable help/schema; treat agent input as untrusted.
- Leep also ships `leep-mcp` (docs/MCP.md), which covers agents that cannot shell out. The
  CLI still matters for CI and for agents with a shell.

## 4. Leep CLI audit

Setup: `leep-server` on SQLite, three working copies, six concurrent working copies, run
with `LEEP_AUTHOR=alice`. Commands below are exact; output is trimmed only where marked.
Version 0.0.1. Speed: `leep log` 25 ms; six concurrent `leep land` (five rebased, all
`attempts: 1`) finished in 0.15 s total. Speed is not a problem.

### 4.1 What is already good
- No staging area, so no `add`/`reset`/`restore --staged` triangle. `leep status` is one
  list of `A`/`M`/`D`/`C`.
- Hints are often present and specific:
  - `leep tag v1 main` (again): `leep: tag v1 already exists; tags never move, delete it with `leep tag -d v1` first`
  - `leep switch nosuch`: `leep: branch nosuch does not exist; create it with `leep switch -c nosuch``
  - `leep reset --hard` with edits: `leep: working copy has uncommitted changes; --hard would discard them (use --force)`
  - `leep undo`: `Moved main back: acafefba7eff -> 2a954d8ef7c7` / `To redo: leep undo main --to 4` / `The working copy is unchanged; run `leep sync` to follow main.`
- A land conflict explains itself and leaves the working copy resolvable:
  ```
  $ leep land
  Cannot land cleanly: 1 conflict against 2a954d8ef7c7 on main
    C a.txt (markers written)
  Your working copy is now on 2a954d8ef7c7 with your changes applied.
  Resolve the conflicts (`leep resolve PATH` once chosen), then run `leep commit` and `leep land`.
  [exit 1]
  ```
- `leep commit` prints the full id on stdout and `committed 34168e108fb4 (2 files)` on stderr,
  so `id=$(leep commit -m x)` works.
- `leep land` is idempotent and reports `rebased`/`attempts`; `leep land --json` on conflict
  returns the structured conflict list (with `merged_with_markers`) and exit 1.
- clap gives `did you mean` for near typos: `leep stat` -> `tip: some similar subcommands exist: 'tag', 'stash', 'status'`.
- Server-side `search`, `blame`, `log --path`, `log --agent`, `provenance` need no checkout.

### 4.2 Rough edges (each: command, observed output, why it matters)

**R1. Short ids are printed everywhere and accepted nowhere.**
```
$ leep log            # prints full 64-hex ids
$ leep blame a.txt
30f9056dcdd8    1 | hello
$ leep show 2a954d8ef7c7
leep: server: not found: revision "2a954d8ef7c7" (404 Not Found)
$ leep diff acafefba7eff 2a954d8ef7c7
leep: server: not found: revision "acafefba7eff" (404 Not Found)
```
`checkout --help` says revs are "Ref name, 64-hex changeset id, or `@`". Humans copy what the
CLI printed and it fails. Git's single most useful ergonomics is abbreviated hashes. Also
`leep log` shows the 64-hex id header, so the 12-char form is unused where people look.

**R2. `land --ref` creates refs silently.**
```
$ leep land --ref nosuch
Landed 72fb4e3cabf5 on nosuch (1 attempt)
$ leep reflog -n 2
20  2026-10-16 12:33:17  -  land  nosuch  - -> 72fb4e3cabf5  landed ...
```
A typo makes a new branch on the shared server. It is recoverable (`leep branch -d`, `undo`),
but the output reads as success and does not say "created".

**R3. Exit codes collide.** `CliError` maps `Server` to 2 (`crates/leep-cli/src/error.rs`), and
clap's usage errors also exit 2:
```
$ leep cat a.txt                 -> error: the following required arguments were not provided ... [exit 2]
$ leep --server http://127.0.0.1:1 refs -> leep: cannot reach http://127.0.0.1:1: error sending request ... [exit 2]
```
Meanwhile exit 1 covers: land conflict, `nothing to commit`, 404, dirty working copy, tag exists, no
server configured, and `resolve nofile`. An agent cannot tell "retry later" from "my command is wrong"
from "conflict, go resolve".

**R4. `--json` does not cover errors.**
```
$ leep show bogus --json
leep: server: not found: revision "bogus" (404 Not Found)     # stderr, plain text
$ leep commit --json -m x
leep: nothing to commit, working copy clean
```
Only successful results are JSON, and "nothing to commit" is a failure with no distinct code, though
for an agent re-running a step it is a normal outcome.

**R5. Error text leaks transport detail.** `leep: server: not found: revision "bogus" (404 Not Found)`;
`cannot reach ...: error sending request for url (http://127.0.0.1:1/v1/refs)`. The user needs "revision
bogus not found; try `leep refs` or `leep log`" and, for connection failures, how to check the server URL
(from where it came: flag, env, or `.leep/config.json`).

**R6. No colour, no TTY detection, no pager.** `grep` of `crates/leep-cli/src` finds no `NO_COLOR`,
`is_terminal` or `stdin` handling. Diffs, `status` letters and conflicts are monochrome. `leep log` on a long
history prints 20 entries of five lines each without a pager. This is not a bug, but it is the biggest gap
against gh/jj/sl for human readers; the safe default (colour only on a TTY, honour `NO_COLOR`) is cheap.

**R7. `--help` has no examples and repeats global flags.** `leep commit --help` lists `--server`, `--token`,
`--json` alongside the actual options; `leep land --help` is two sentences plus three global flags.
No command has `after_help` examples (checked with a search for `after_help|long_about`). Top-level help
is an unsorted 31-command list with no grouping (`keygen` sits between `diff` and `commit`).
`--help` prints the current `LEEP_SERVER` value (`[env: LEEP_SERVER=http://127.0.0.1:7071]`), which is
helpful; `hide_env_values` is correctly set for the token.

**R8. No shell completions.** `leep completions bash` -> `error: unrecognized subcommand 'completions'`.
With revs, refs and tags as arguments, completion is worth more here than in most tools. (`clap_complete`
would be a new dependency, in the CLI crate only, which is allowed.)

**R9. Conflict markers: `ours` is not "mine".**
```
<<<<<<< ours
line from alice          # already landed on main
||||||| original
=======
line from bob            # the user's own change (bob's working copy)
>>>>>>> theirs
```
When landing, "ours" is the target and "theirs" is the user. Evans' poll shows 48% do not know sides flip
in git; here the labels are inverted from what a person landing their work expects. Labels such as
`main (landed)` and `yours` (or the changeset ids) are safer, and agents resolving conflicts parse them.
Note the format is a persisted/wire artifact only if the server builds the markers; the JSON `ours`/`theirs`
fields are `/v1`, so rename only the markers text, not the fields.

**R10. `search` with no match exits 1 and prints nothing.**
```
$ leep search nomatchzzz
[exit 1]
$ leep search zzz --json     # exit 0, "matches": []
```
Same condition, two exit codes, plain mode silent. grep-like, but it reads as failure in scripts.

**R11. Repeated land reports success without saying it was a no-op.**
```
$ leep land
Landed 30f9056dcdd8 on main (1 attempt)
$ leep land
Landed 30f9056dcdd8 on main (1 attempt)
```
Idempotence is a feature for agents; the second message should say `already landed` and JSON should carry
that (`"status": "landed"` plus `"already": true`, additive).

**R12. Status lacks next steps and landing state.** After a land, `leep status` says
`base 30f9056dcdd8 (target main)` / `nothing to commit, working copy clean`. After a local commit it
says `base is 1 commit ahead of main` but not "run `leep land`". `leep status --json` has
`on_target`/`ahead`; `ahead` is `null` after a land conflict. A conflicted `status` exits 0.
`leep status` for a conflicted tree prints `unresolved conflicts: fix the markers, or `leep resolve PATH`...`,
which is the right kind of hint; extend it to the clean cases.

**R13. Output formats and JSON shapes are raw API responses and inconsistent.**
- `leep log --json` returns `{"entries":[{"id","changeset":{...changes with blob hashes...},"generation","root_tree"}]}`;
  `show --json` returns `{"id","changeset",...}`; `blame --json` uses `changeset` for an id and `text`,
  `origin_line`; `reflog --json` uses `time` (seconds) and `actor: null`, while changesets use `timestamp`.
- `branch --json` and `tag --json` and `refs --json` all return `{"refs":{name:id}}`; `tag` keeps the `tags/`
  prefix that the text mode strips, and `branch --json` does not say which branch is current.
- `search --json` includes 9 internal `stats` counters (`dirs_pruned`, `blob_rows_derived`, ...).
- `--help` says "Print the raw API response as JSON": that is accurate, and also a stability trap: the shape
  is a `/v1` response, so changing the CLI's JSON means changing `/v1`.
- Plain `leep log` prints no refs and no short form, `show` prints `parent` before `changeset`.
Agents mostly want `{id, short, author, message, timestamp, paths}`, not a full changeset.

**R14. Misc.**
- `leep log` has `--path`, `--follow FILE` and the rev positional (`leep log main --path a.txt`), while
  `blame [REV] PATH` takes the rev first and `cat REV PATH` requires it, and `last-change PATH --rev`
  uses a flag. `tag NAME [REV]` is the reverse of `branch NAME [REV]` only in name; they agree. Rev
  argument position is inconsistent across read commands.
- `reset --hard` with a clean tree and no argument moved the base back past a landed changeset
  (`Reset to 2a954d8ef7c7 (was acafefba7eff); discarded 0 changes`). Git's no-argument `reset --hard` stays put.
  It is safe (nothing lost) but surprising to git users and the message does not say it moved to the parent.
- `leep diff main main` prints nothing and exits 0 (fine). `leep log --agent` with no agent commits is silent.
- `leep --token bad refs` succeeded against the anonymous dev server; I could not test auth failures
  (no auth-enabled server was run), so the 401/403 messages are unverified.
- `provenance` shows the table, then `scanned 3 changesets` on stdout, which breaks plain parsing.
- `leep commit -m` is required even for a trivial change; agents fine, humans have no editor fallback
  (`$EDITOR`), which is consistent with an agent-first product but worth a conscious decision.

### 4.3 Where Leep avoids git's pain, where it repeats it

| Theme (section 2) | Leep today |
|---|---|
| Terminology | Avoids index/staged/HEAD/detached. Repeats `ours`/`theirs` (R9), `reset` modes (`--hard`, R14), `sync` vs `rebase` vs `merge`. |
| Fear of losing work | Better: `undo`, `reflog`, stash, `--force` guards. Repeats: no operation log for local state (`restore`, `reset --hard --force`, `checkout --force` are not undoable). |
| Merge conflict UX | Better: land conflicts keep the working copy resolvable and say what to do. Repeats: marker labels, no `--continue`-style single next step. |
| Rebase pain | Better: server rebases on `land`, `sync` moves local commits. |
| Error messages | Mixed: hints good in some paths (tag, switch, reset), raw transport text in others (R5). |
| Flag consistency | Repeats it (R14: rev position; short ids R1). |
| Large repos | Avoids via sparse checkout, server-side search. |
| Auth | Not audited (see "Could not verify"). |

## 5. Prioritized backlog

Legend: users = H (humans), A (agents), B (both). `/v1` = touches the HTTP API (additive
only). ADR = needs an architecture decision record. "CLI only" means `leep-cli` (the
only dependency allowed is `leep-core`).

### P0 (do first: bugs against the agent promise, or cheap and high-value)

| # | Problem | Proposed change | Effort | Users | Notes |
|---|---|---|---|---|---|
| P0-1 | Exit code 2 means two things; exit 1 means everything (R3) | Documented codes: 0 ok, 1 generic/user, 2 usage (clap), 3 server/network (retry), 4 not found, 5 conflict, 6 nothing to do (opt-in via `--exit-code`?), plus 130. Keep current values for existing cases only if the ADR says so. | M | A | **Done** ([ADR 0033](../adr/0033-cli-exit-codes-and-json-errors.md), `docs/CLI.md`): 6 is used by `commit`, `amend` and `stash push` only. |
| P0-2 | `--json` errors are plain text (R4) | With `--json`, print `{"error":{"code","message","hint","retryable"}}` on stdout (or stderr, decide), same exit codes as P0-1. | M | A | **Done** (ADR 0033): stdout, plus the usual text on stderr. Note: the server's error body carries only `error`; the kind comes from the HTTP status. |
| P0-3 | Short ids unusable (R1) | Resolve unique prefixes (min 4 hex) of changeset ids client-side via a new server prefix lookup, or via `GET /v1/...` additive endpoint; ambiguous prefix lists candidates. Print short ids in `log` headers. | M | B | `/v1` additive (prefix resolve). Server-side is needed because the CLI does not hold history. |
| P0-4 | `land --ref` typo creates a ref (R2) | Require `--create` (or a ref that exists) and say "created ref X" when it does; keep `main` first-land working. | S | B | CLI plus `/v1` request field (`create: bool`, default current behaviour for compatibility until 0.1). |
| P0-5 | No colour/TTY/`NO_COLOR` handling, no pager (R6) | Colour on a TTY only (`NO_COLOR`, `CLICOLOR_FORCE`, `--color auto/always/never`); pager for `log`/`diff`/`show` on a TTY (`LEEP_PAGER`, `PAGER`). | M | H | CLI only. |
| P0-6 | `--help` without examples, noisy global flags (R7) | `after_help` examples on the top 12 commands; group commands in top-level help (Work, History, Branches, Review, Admin); `--server/--token/--json` documented once in the top-level help. | S | B | CLI only. |
| P0-7 | Conflict marker labels (R9) | Label sides by role, e.g. `<<<<<<< main (landed)` / `>>>>>>> yours`. | S | B | Markers are text written by the server/client merge; check FORMATS.md and the `resolve` parser before changing. May need ADR if marker text is parsed. |

### P1 (next: agents and humans both notice)

| # | Problem | Proposed change | Effort | Users | Notes |
|---|---|---|---|---|---|
| P1-1 | No shell completions (R8) | `leep completions {bash,zsh,fish}` using `clap_complete`; dynamic completion of branches/tags via `leep refs`. | S | H | CLI only (new dep in `leep-cli`; add to workspace deps). |
| P1-2 | Inconsistent JSON shapes, noisy stats (R13) | Define a documented `--json` contract per command (curated `short`, `author`, `message`, `timestamp`, `paths`); keep raw API behind `--json=raw`. Add a field-selection flag as in gh (`--json id,message`) and `--jq` later. | L | A | A curated shape is CLI-side; raw stays. Needs ADR on stability (1.0 promise). |
| P1-3 | Repeated `land` says "Landed" (R11); `search` exit code (R10); `provenance` footer (R14) | `already landed` text and `"already": true` JSON; `search` exit 0 with message on stderr, or a documented exit 1 only under `--quiet`; footers to stderr. | S | A | `already` is a `/v1` response field, additive (`#[serde(default)]`). |
| P1-4 | `status` has no next step (R12) | Add a `next:` line (`run leep land`, `run leep sync`, resolve) and a `next_step` field in JSON, as `leep_review_status` already does. | S | B | JSON field additive. |
| P1-5 | `NO_INPUT` and prompt behaviour is undocumented | Document that Leep never prompts, add `LEEP_NO_INPUT` as a no-op guarantee, and test with a closed stdin. Add a "for agents" section to the docs listing env vars and exit codes. | S | A | Docs and a test. |
| P1-6 | Raw transport errors (R5) | Map known server errors to messages with hints; show where the server URL came from (flag, env, config). | S | B | CLI only. |
| P1-7 | No overview command | `leep log --graph`/`leep smartlog`: local unlanded commits, the target head, landed marker, review status per changeset, agent/model. | M | B | Likely needs a small `/v1` read (landed vs unlanded). See section 6. |
| P1-8 | Rev argument position inconsistent (R14) | Accept `--rev` on every read command; keep positionals. | S | B | CLI only. |

### P2 (later, or after design partners)

| # | Problem | Proposed change | Effort | Users | Notes |
|---|---|---|---|---|---|
| P2-1 | Local operations are not undoable | Local operation log (jj-style) for `restore`, `reset --hard`, `checkout --force`, `stash drop`; `leep undo --local`. | L | H | ADR; persisted local format. |
| P2-2 | `leep land` conflicts need a resumable state | `leep land --continue`/`--abort` or a single `leep conflicts` view with per-file next commands. | M | B | CLI only; maybe `/v1` additive. |
| P2-3 | Typos beyond clap's similarity | Suggest `Did you mean` for refs and revs (`leep switch mian` -> `main`). | S | H | CLI only. |
| P2-4 | Progress for slow paths | Spinner/bar on `checkout`, large `commit` uploads, first `search` on an unindexed rev, only on a TTY; print something within 100 ms. | M | H | CLI only. |
| P2-5 | Config file and precedence | Documented flags > env > `.leep/config.json` > `~/.config/leep/config.toml` (XDG). | M | B | ADR if config format is persisted. |
| P2-6 | `reset` semantics | Decide whether no-argument `reset --hard` should move to the parent; at least print "moved to parent". | S | H | Compatibility decision. |
| P2-7 | Machine-readable help | `leep help --json` (command tree, flags, exit codes) for agent frameworks. | M | A | CLI only. |
| P2-8 | `absorb`, `split`, `fold`-style history editing | Evaluate from Sapling once agents ask. | L | H | Not recommended yet. |

Items to resist: telemetry (clig.dev: opt-in only; skip), an interactive TUI-by-default, a
staging area, and `$EDITOR` message prompts for agents.

## 6. UI recommendation

Fit: the first market is teams running many agents that already work through the
CLI and `leep-mcp`. Their pain is visibility (what is landing, who made it, why a
changeset is blocked), not editing.

1. **Smartlog in the terminal: build first (P1-7).** It is cheap (CLI plus one read
   endpoint), serves humans and agents (`--json`), needs no new server surface, and Sapling
   shows it can be the centerpiece of the UX. Contents: local unlanded commits, target head,
   recent landings with author/agent/model, and review status.
2. **Read-only web view served by `leep-server`: build later, after design partners.** It is
   already on the roadmap under M7 (the "web view") and PRODUCT.md lists replacing GitHub's
   review UI as a non-goal. If built, keep it to refs, history, blame, search and a live
   "landing activity" feed with provenance; do not add editing, review threads or auth UI.
   Do not build it before the JSON contract (P1-2) exists, so the view and CLI share it.
3. **TUI (lazygit/tig style): do not build.** It serves humans doing interactive staging,
   which Leep removed, and it is a large surface for the second-priority user. If humans ask
   for it, `leep log --graph` with a pager and `sl web`-style local viewer cover most of the need.
4. **Not goals:** a desktop GUI client (PRODUCT.md).

## 7. Could not verify

- Stack Overflow Developer Survey: the 2025 technology page I fetched only reported
  collaboration tools (GitHub 81.1%, GitLab 35.6%), not satisfaction with git. No survey number on
  git pain is cited above.
- "What's Wrong with Git? A Conceptual Design Analysis" itself: the ACM page returned HTTP 403. I rely on
  the MIT SDG project page (which lists it, Onward! 2013) and a review of the OOPSLA 2016 paper.
  I did not read either paper's text.
- Git Rev News: the index page I fetched showed only one tangential UX item (Edition 139, Harald
  Nordgren spotlight). I did not find rev-news or mailing-list threads about CLI UX specifically.
- Hacker News and Reddit: one HN thread (id 9066376) was fetched. Other HN results were mirrors I could not
  confirm. No Reddit threads were fetched.
- Submodule, large-repo performance and auth complaints: thin or not found (section 2.8).
- gh exit codes page: 404. Graphite guide: the fetch redirected and was not followed. git-branchless
  smartlog page: not fetched. jj revsets: not read.
- Agent write-ups (section 2.9, 3.6) are search snippets or blog/skill-spec pages, not primary standards.
- Leep audit: auth-enabled server, Postgres, `merge`, `rebase`, `cherry-pick`, `revert`, `review`,
  signing and `checkout --prefix` were not exercised. No terminal colour test was possible because Leep emits none.
  Findings R9 and P0-7 about marker text were not checked against FORMATS.md.
