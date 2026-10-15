# 0017. Working-copy workflow: sync with local changes, stash, restore, amend, reset
Status: Accepted
Date: 2026-10-15

## Context
`leep sync` refused a working copy with uncommitted changes, so moving to a new
head meant committing half-finished work first or throwing it away. There was
no way to shelve work, discard it for a few files, rewrite the last commit or
move the base. Leep is server-centric: a commit is a changeset on the server
whose parent is the working copy's base, and `land` replays the unlanded stack
(the changesets not in the target ref's history) onto the head. Most of what a
git user does in a normal day can be built from those pieces and the existing
`/v1/rebase` endpoint, without a new server operation.

## Decision
- **Snapshots are changesets.** To move uncommitted work, the CLI stores it as
  an ordinary changeset: parent = the base, the uncommitted changes as
  `changes`, and `extra["leep.snapshot"]` set to `"sync"` or `"stash"`
  (`leep_core::changeset::EXTRA_SNAPSHOT`). The key is part of the changeset
  hash, which is fine because a snapshot is a new changeset; no persisted
  format changes. No ref points at a snapshot, and `land` never sees one.
- **`sync` rebases through the server.** With uncommitted changes the CLI
  snapshots them and calls `POST /v1/rebase` for the snapshot onto the new
  base. A clean answer is the tree to put on disk; conflicts come back as
  `clean` changes plus per-file results, and the CLI writes conflict markers
  exactly as `land` does (the server never resolves anything). The result is
  materialized as *uncommitted* changes: the base becomes the new head and the
  recorded files are the new base's tree, so `status` shows the rebased edits.
  Marked files are listed in `state.conflicts`, so `commit` and `land` refuse
  until the markers are gone.
- **Local commits move too.** If the base is not in the target ref's history,
  `sync` rebases the first-parent stack onto the head one changeset at a time
  with the same endpoint, oldest first, and uses the new tip as the base.
  A conflict inside the stack stops the sync with nothing changed and points to
  `land`, which already knows how to squash a stack and report conflicts. A
  stack that contains a merge is refused, because `/v1/rebase` rejects merges.
  This also applies to a clean working copy; before, `sync` silently left the
  local commits behind.
- **Never lose local content.** The disk is touched only after every blob and
  server answer needed is in hand. Each file is written to a temp file and
  renamed. Writes are journalled, and if a write or the state save fails the
  files already written are put back and the state in memory is restored. The
  state is saved last. Errors after the snapshot exists print its id;
  `leep stash apply <id>` re-applies it. Untracked files in the way (including
  ignored ones) make the sync fail before anything is written.
- **Conflicts are never resolved silently, on the client either.** A conflict
  with no text merge (binary file, delete versus modify) cannot get markers, so
  the CLI keeps the local version and records the path in `State.unresolved`
  (also for `land`'s local rebase). `status` shows it as `C` with a note,
  `commit`, `amend`, `land` and `stash` refuse while any such path or
  marker file remains and name them, and the message says how to see the other
  side (`leep cat @ PATH`). `leep resolve PATH...` clears the mark, and refuses
  a file that still has conflict markers unless `--force`. `leep restore`
  clears it too, by taking the base version.
- **Local stash list.** `leep stash` snapshots (`"stash"`), records
  `{id, base, message, timestamp}` in `State.stashes` in `.leep/state.json`,
  and restores the working copy to its base. New files are part of the stash
  and are removed. `apply` and `pop` rebase the snapshot onto the current base
  with the same code as `sync`, need a clean working copy, and on conflicts
  write markers and keep the entry. No refs are created.
- **`restore`** replaces the named files with the base content (or with the
  content at `--source REV`) one file at a time, and says what it touched.
  New files are left alone unless `--force`; `--source` will not overwrite a new
  file without `--force` either. Restoring a path clears its conflict mark.
- **`amend`** is refused if the base is in the target ref's history. Otherwise
  it creates a changeset with the same parents, author and `extra` (the `agent.*`
  flags replace keys they name), a new message if `-m` is given, and the base's
  changes combined with the uncommitted ones, then sets the base to it. The old
  changeset stays on the server. A path that both touch is looked up in the
  parent so a file the base added and the user deleted vanishes from the
  result, and a change that returns a file to the parent's content is dropped.
  A signature on the old changeset does not carry over; `--sign` signs the new
  one and the CLI warns when the old one was signed.
- **`reset`** moves only the working copy's base and never a server ref.
  `reset REV` (soft) keeps every file and recomputes what is recorded as
  materialized from REV's tree, so the difference is uncommitted. `--hard`
  materializes REV and discards local changes to tracked files; with
  uncommitted changes it needs `--force`, and new files that REV does not
  have are left. The default REV is the base's first parent, which uncommits
  the last commit and keeps its changes. REV's file listing is fetched before
  anything changes, so an unknown or unreadable REV fails cleanly.
- **`status`** reports the number of stashes and, from the merge base and
  generation numbers, `base is N commits ahead of <target>` (`base is not on
  <target>` when they share no history). The lookup is best effort: offline,
  `status` just omits it.
- `write_file` now writes through a temp file and rename for every caller
  (`checkout`, `land`, `sync`), not just the new commands.

## Alternatives
- **A server-side stash or sync operation.** One new endpoint could do the
  snapshot, rebase and answer in a single round trip. It would also let the
  server keep stashes, so they survive loss of the working copy. It adds API
  surface for something the existing endpoints already express, and it would
  need an identity for the stash list. We can add it later without changing
  this behaviour.
- **Stash as a ref** (`stash/<user>/<n>`). Survives disk loss and is visible
  to `leep refs`, but refs are the only mutable global state (principle 3) and
  would collect stale entries per user. Rejected.
- **A local patch file instead of a server changeset.** No server storage, but
  no three-way rebase (the server does that), and no recovery by id.
- **Squashing local commits at `sync`** instead of rebasing each. Loses the
  commit boundaries the user made.
- **Resolving stack conflicts with markers** in `sync`. A conflict in an
  already-committed local commit has no uncommitted home to live in; `land`
  already models it as one squashed change against the head.

## Consequences
- No change to stored formats or the `/v1` API; no new server operations.
- Every `sync` with local changes, and every `stash`, leaves a changeset on
  the server (and, for `sync` and `stash apply`, a rebased copy that keeps the
  `leep.snapshot` marker and `leep.rebased_from`). Changesets are never
  garbage collected: blob GC marks from all stored changesets, so the blobs they
  reference are kept too. Snapshots therefore cost storage forever, roughly the
  size of the changed files. They are cheap to find (`leep log --extra
  leep.snapshot`) if an operator later wants a retention policy, which would be
  a new decision.
- Stashes live on the server as changesets, but the list is local. Losing the
  working copy loses the list, not the content: the id (from `stash list`,
  or a sync error) still works with `leep stash apply <id>` or `leep checkout`.
- `amend` and `reset` leave the previous changeset unreferenced locally; it
  stays on the server and can be reached by id. Local commits that were
  rebased by `sync` are new changesets, so a signature on the old ones does
  not apply to them.
- With a sparse checkout the snapshot contains only in-scope paths, and
  results that touch paths outside the checkout are refused rather than
  half-applied.

## Security review 2026-10
See [SECURITY-REVIEW.md](../SECURITY-REVIEW.md), S-02 (fixed). Every write
and delete of a repository path now goes through `WorkCopy::abs_for_write`.
It refuses a path with a `.leep` or `.git` component in any spelling the
filesystem folds together (case, HFS+ ignorable code points, Windows trailing
dots and spaces, 8.3 short names), on Windows also `\`, `:` and device names,
and a path whose directory on disk is a symbolic link, checked right before
each write. Before, a crafted tree on a case-insensitive filesystem, or any
malicious server, could make a checkout write outside the working copy (for
example through a symlink `A` and a file `a/...`), plant `.git/hooks`, or
overwrite `.leep/config.json` with `--force`. The server still accepts these
paths; the client is where files are written, so the check lives there.
