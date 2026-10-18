# 0046. Live two-way mirror between Leep and a git remote
Status: Accepted; extended by [0049](0049-mirror-merges-and-land-branches.md)
Date: 2026-10-18

## Context
The first market is teams that run many coding agents on one repository
while their people, CI and code review stay on GitHub or GitLab
([PRODUCT.md](../PRODUCT.md)). Leep's own development is meant to move onto
a Leep server with GitHub as its mirror. Until now the git bridge only had
two offline batch commands: `leep-git import` and `leep-git export`. Both
open the server's stores directly, so the e2e test runs them before the
server starts "so only one process touches the stores", and someone has to
run them by hand.

Roadmap M7 opens with a live mirror. Lands should show up in git within
seconds. Pushes to git should land in Leep. Conflicts are reported and never
resolved silently (CLAUDE.md). Three properties make this harder than a
cron job that runs export:

- **Exactly once across crashes.** The mirror can die between any two
  steps, including after a push the remote accepted but before the mirror
  saw the answer. A rerun must not push a second copy of a commit, import a
  commit twice, or lose one.
- **No loops.** A commit that came from Leep must not come back as a new
  change, and a commit imported from git must not go back out as a
  different commit.
- **Never force.** The remote's `main` is what a team's CI and people
  trust. Nothing the mirror does can rewrite it.

## Decision

### Where it runs: a daemon on the HTTP API, not inside the server
`leep-git mirror run` is a separate process. It talks to `leep-server`
over the existing `/v1` HTTP API with an ordinary token, and to the remote
through the system `git` binary.

- **Not in `leep-server`.** The server stays free of git dependencies and
  of remote credentials. A slow or dead remote cannot hold up a request
  thread or a land. The mirror fails, restarts and scales on its own, one
  process per remote.
- **HTTP, not direct store access.** The offline bridge's direct access is
  safe only while nothing else writes the stores. Over HTTP, the mirror
  works against any deployment, including a hosted one, needs no database
  credentials, and goes through the same validation, land policy and ref
  log as every other client. The API already had all it needs: changesets,
  blobs, `blobs/missing`, refs, the ref log and ref CAS. The mirror needs
  **no server change** and no new endpoint.
- **System `git` for transport.** gix does not push. `git` supports every
  transport (https, ssh, file) and every credential helper people already
  use, so Leep never handles a git password itself. Objects are written
  with gix into the mirror's own bare repository, and `git push` sends
  them.
- **The mirror uses the CLI's HTTP client library** (`leep_cli::client`), as
  `leep-mcp` does. `leep-git` therefore depends on `leep-cli`.
  `leep-cli` still depends only on `leep-core`.

### State: a work directory, and decisions made from current values
The work directory holds four things:

- `git/`, a bare repository holding every exported and fetched object;
- `map`, an append-only list of `<changeset> <commit> <e|i>` lines;
- `status.json`, the last reported state;
- `lock`, an exclusive lock so only one mirror uses the directory.

Each cycle decides what to do from **current values only**: the Leep ref
`L`, its export `E`, and the remote ref `G`. It never resumes from
remembered progress:

| relation | action |
|---|---|
| `G == E` | in sync |
| `G` absent, or an ancestor of `E` | push `E` without force; the remote accepts only a fast-forward |
| `E` an ancestor of `G`, and `G` came from git | import, then move the Leep ref by CAS from `L` (fast-forward only) |
| `E` an ancestor of `G`, and `G` came from Leep | Leep moved back (an undo): report `rewound` and never force |
| neither | `diverged`: report it, push `E` to a mirror-owned branch, move nothing |

This is what makes crash safety and exactly-once work:

- **Exports are deterministic.** A changeset always becomes the same commit:
  its first parent's tree with the changeset's own changes applied, plus
  fixed commit metadata. A crash during an export leaves objects that the
  rerun writes again to the same ids.
- **A push is a compare-and-swap on the remote.** Without force, the remote
  accepts an update only as a fast-forward. Re-pushing a value the remote
  already has is "up to date", a no-op. A lost acknowledgement therefore
  needs no record: the rerun sees `G == E`. The crash test aborts the mirror
  just after a successful push and checks that the restart pushes nothing.
- **Imports are content addressed.** The mirror builds a commit's changeset
  with the same code as `leep-git import` (`import::changeset_for`), so a
  commit gets the same id however often it is imported. Creating a
  changeset again is a no-op. The Leep ref moves only by
  `POST /v1/refs/{name}` CAS from the `L` it read. A rerun after a crash
  between import and CAS imports nothing new and does the CAS. A rerun
  after the CAS sees `G == E` and stops. The ref log shows the move once,
  with operation `git-mirror`.

The ref-log cursor is only a cheap way to notice change. Losing it costs a
re-read.

### Leep → git
- **Trigger.** Every `--interval` (default 1 s) the mirror reads the newest
  ref log `seq` (`GET /v1/reflog?limit=1`, one small query). When it
  changes, it reads `GET /v1/refs`. Every `--reconcile` (60 s) it reads all
  refs regardless, and it reads them at startup. A land therefore reaches
  the remote within about a second plus the push time; the e2e test checks
  "within 10 s".
  - We did not build a long-poll or event stream on the server: polling meets
    the latency goal without a server change. `GET /v1/reflog?after=&wait=`
    can be added (only adding to `/v1`) when there are many mirrors or the
    interval must shrink.
- **Incremental export.** The mirror walks back from `L` until it meets
  changesets the map knows and whose commit is present. It fetches each new
  changeset (`GET /v1/changesets/{id}`) and only the blobs its changes name
  (`GET /v1/blobs/{hash}`), and checks both against their hashes. It then
  rewrites only the directories those changes touch on top of the first
  parent's tree. Parents are written first, in an iterative walk; recursion
  follows directory depth only.
  - A test checks that the result is **byte for byte the same commit** that
    `leep-git export` writes from the full tree (merges, deletes that empty a
    directory, file and directory swaps, exec bits, symlinks), with and
    without provenance trailers.
- **Commit identity.** Changesets imported from git carry `git.commit` and
  the extras that rebuild the original bytes, so they export to their
  original commit. Changesets made in Leep (and, since ADR 0049, any
  changeset whose export is not the commit its `git.commit` names, such as
  a rebased copy) get a `Leep-Changeset: <id>` trailer, added to an existing trailer paragraph if there is one. The
  trailer tells people which Leep change a commit is, and it lets loop
  prevention work without the map (below). `--no-changeset-trailer` and
  `--provenance-trailers` change commit ids, so they must stay fixed for the
  life of a mirror.
- **Ref mapping.** `--ref NAME[:GIT_REF]`, repeatable. `NAME` maps to
  `refs/heads/NAME`, and `tags/T` to `refs/tags/T`. `--tags` mirrors every
  Leep tag. Tags are created and never moved: a remote tag that differs is
  reported as `diverged`. A branch must map to a branch and a tag to a tag.
- **Force policy.** No push ever uses `--force`. Only two kinds of update
  may be non-fast-forward, and both use `--force-with-lease` with the exact
  expected value:
  - the mirror-owned diverged branches (`--diverged-prefix`, default
    `leep-diverged/`), which no configured ref may map into;
  - refs named with `--allow-rewind`, where a Leep undo is pushed back.
    `main` and tags are refused for this at startup.

  A Leep undo of any other ref is reported as `rewound` and the remote keeps
  the newer commits until someone lands a revert. The e2e tests check the
  remote's reflog: every update of `main` was a fast-forward.
- **Deletes.** A Leep ref that is deleted leaves the remote branch alone and
  is reported as `deleted-in-leep`. A tag that the mirror found through
  `--tags` is simply forgotten.
- **Backpressure.** Leep never waits for the mirror. The mirror merges a
  backlog into one push of the newest value. A failing ref is retried with
  exponential backoff up to `--max-backoff` (5 min), a new value on either
  side retries at once, and every git command is killed after
  `--git-timeout`. The remote rejecting a push three times in a row (branch
  protection, a hook) is an `error`, not a hot loop. Lag is reported per
  ref.

### git → Leep (`--import`)
- **Detection.** The mirror runs `git ls-remote` for the mirrored refs and
  their diverged branches every `--git-poll` (15 s), after any rejected
  push, and at once on `POST /poke`. It fetches only values it lacks, into
  `refs/leep-mirror/remote/*`, so they stay reachable. A GitHub or GitLab
  push webhook can point at `/poke`. The poke carries no data and needs no
  secret: a forged poke only makes the mirror poll sooner. Signed webhook
  payloads are not needed for that.
- **Import.** The mirror walks the remote's new commits back to known ones,
  parents first. For each commit it hashes the blobs, uploads the ones
  `POST /v1/blobs/missing` reports, and creates the changeset. The map
  entries (`i`) are synced to disk before the ref moves. They record that
  the changeset came from that exact commit, which matters when an export
  would not reproduce it byte for byte (submodule gitlinks, which Leep does
  not store).
- **Landing.** Only fast-forwards land: `E` must be an ancestor of `G`, so the
  imported head descends from `L`. The move is a ref CAS through
  `cas_ref_checked`, so the land policy applies in full: signatures,
  review, OWNERS, and forward-only on protected refs. A refusal (400)
  leaves the ref alone and reports `refused` with the server's reason. It is
  checked again every minute, since a review or a new key can change the
  answer. A lost CAS (an agent landed meanwhile) is checked again at once,
  and usually turns into a divergence.
  - We did not use `land`. It would rebase the commits onto a moved head and
    so give them new ids. The remote's `main` already holds the originals,
    so the next export would not fast-forward it.
- **Conflict policy: stop and report.** When both sides moved, the mirror
  lands nothing and pushes nothing to that ref. It reports `diverged`,
  pushes Leep's side to `leep-diverged/<branch>`, and keeps checking. A
  person merges that branch in git and pushes. The merge descends from both
  sides, so it imports as a fast-forward, lands, and the mirror deletes the
  diverged branch with a lease. We considered and rejected two other
  answers:
  - *Rebase the git commits*: this rewrites commits the remote already
    published.
  - *Merge automatically when the merge is clean*: this is safe, since it
    resolves no conflict, and it is the planned next step (below), but it
    adds a commit that no person or agent wrote. Signature policies refuse
    it, since the mirror has no signing key. Teams should opt in.

  Stopping is the only choice that is right under every policy. Its cost
  shows in the concurrency test: while agents land continuously, a git
  push usually races a land, so the ref spends time `diverged` until
  someone merges. It stays the default; ADR 0049 adds the opt-in
  `--on-divergence merge` and land branches.
- **Loop prevention.**
  - A commit is Leep's own if the map lists it. Otherwise, it is Leep's own
    if its `Leep-Changeset:` trailer names a changeset the server has *and*
    exporting that changeset gives exactly this commit. A trailer alone
    proves nothing: it survives a cherry-pick.
  - Leep's own commits are never imported.
  - Imported changesets map back to their original commits, so they are
    never exported as new ones.
  - A remote tip that is Leep's own and ahead of `E` means Leep moved back:
    it is `rewound`, never a fast-forward import. For a commit the mirror
    imported, this applies once the ref log shows the ref held it. Before
    that, the import's land is simply retried, for example after a refusal.

### Observability
- **Status.** `status.json` in the work directory is written atomically
  every cycle. It lists each ref's state (`in-sync`, `pending`, `diverged`,
  `rewound`, `remote-ahead`, `refused`, `deleted-in-leep`, `error`), a
  readable `detail`, both values, the lag, and push and import counters.
  - `leep-git mirror status [--json]` prints it. It exits 0 when every ref is
    `in-sync` or `pending` and the file is fresh, 1 when a ref needs a
    person or the daemon looks dead, and 2 when there is no status.
  - `--listen ADDR` serves `GET /status`, `GET /metrics`, `GET /health` and
    `POST /poke`.
- **Metrics.**
  - `leep_mirror_ref_state{ref,state}`, `leep_mirror_lag_seconds{ref}` and
    `leep_mirror_last_sync_timestamp_seconds{ref}`;
  - `leep_mirror_pushes_total{ref,result}`, `leep_mirror_imports_total{ref,result}`
    and `leep_mirror_divergences_total{ref}`;
  - `leep_mirror_commits_exported_total`, `leep_mirror_commits_imported_total`
    and `leep_mirror_errors_total{stage}`;
  - `leep_mirror_up`, `leep_mirror_cycles_total` and
    `leep_mirror_last_cycle_timestamp_seconds`.
- **Alerts and runbooks.** `LeepMirrorDown`, `LeepMirrorLagging` and
  `LeepMirrorStuck` (any state that needs a person), each with a runbook in
  RUNBOOKS.md.
- **Secrets.** The Leep token comes from `--token-file` or `LEEP_TOKEN`,
  never from argv. Git credentials stay with git. Anything of the form
  `scheme://user:pass@` is redacted from logs, `detail` and the status
  file, including git's own error output.
  - The mirror runs git with `GIT_TERMINAL_PROMPT=0`, so a missing credential
    fails instead of hanging.

### Testing
- **Unit and library tests (`leep-git`)** use in-memory stores and local
  bare repositories:
  - the incremental export equals the full export;
  - trailers are placed correctly;
  - fast-forward pushes, tags, and a resumed run that pushes nothing new;
  - imports with the original sha kept, no loop, divergence reported, and
    a merge in git resolving it;
  - an undo reported as `rewound` without a force;
  - `remote-ahead` without `--import`;
  - the map survives a torn tail;
  - redaction.
- **e2e with the real binaries (`crates/leep-e2e/tests/mirror.rs`)**:
  - follow and import, including the diverge-and-merge flow, `status` exit
    codes and metrics (SQLite, plus a Postgres variant);
  - a seeded SIGKILL sweep across export, import and diverged rounds;
  - six named crash points (`LEEP_MIRROR_CRASH_AT`, a test hook that
    aborts the process at one step);
  - agents landing while a person pushes.

  Every e2e test checks four things:
  - every changeset on Leep's main has exactly one commit on the remote's
    main, and every commit there maps to one of them;
  - the two trees match;
  - the remote only ever fast-forwarded;
  - `git fsck --strict` passes on the remote.

## Consequences
- A team can keep git, its CI and its code review while agents land through
  Leep. Leep's own development can move onto a Leep server with GitHub as
  the mirror (the M5 self-hosting item). No server change was needed, and
  `/v1` is unchanged.
- Under continuous agent landing, a person's git push usually diverges and
  waits for a merge in git. That is safe and visible, but slow for teams
  whose people push often. The next steps below address it.
- Changesets imported by the mirror are not recorded in the server's
  `git-to-leep-v1` / `leep-to-git-v1` rows, since the API cannot write
  derived rows. The work directory's `map` is the mirror's record.
  - Losing it is recoverable: exports are recomputed to the same commits.
    The exception is a changeset imported from a commit its export does not
    reproduce, such as one with submodules. It would then export
    differently, the remote would refuse the push, and the mirror would
    stop as `diverged` rather than do damage.
  - Back up the work directory like any other state.
- The first run of a mirror against a long Leep history exports all of it
  over HTTP: one request per changeset and per new blob. For Leep's own
  repository that takes seconds. For a very large history, seeding the map
  offline from the server's mapping rows is future work.
- The status file can look stale while one long push or first export runs,
  since a cycle publishes at its end.
- The mirror's commits for native changesets differ from what plain
  `leep-git export` writes, because of the `Leep-Changeset:` trailer. A
  remote first filled by `leep-git export` needs `--no-changeset-trailer`,
  or it diverges on the first push.

### Remaining for M7
- Clean-merge mode for divergence and land branches: done in
  [ADR 0049](0049-mirror-merges-and-land-branches.md).
- Optional long-poll on the ref log, mirroring of Leep ref deletes for
  allowed refs, and seeding the map offline.
- The git wire protocol on the server (`git clone`, `fetch`, `push` against
  Leep), which retires most uses of the mirror.
