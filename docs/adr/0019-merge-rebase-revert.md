# 0019. `merge`, `rebase`, `cherry-pick` and `revert` on the working copy
Status: Accepted
Date: 2026-10-15

## Context
A git user expects to combine branches, move commits and undo a change
without leaving the CLI. The server already has the pieces: `POST /v1/merge`
makes a merge changeset, `POST /v1/rebase` replays one changeset onto another
with a three-way merge, and ADR 0017 built the client side for putting a
conflicted answer on disk. What was missing were commands, a way to finish a
merge that stopped on conflicts, and an inverse of a change.

## Decision
- **Local, unlanded commits.** All four commands change only the working
  copy's base (and the files). None moves a ref; `leep land` does, through
  the same policies as any commit, so signatures and review still apply at
  land. All need a clean working copy (`Ctx::require_clean`, which also
  refuses while paths are conflicted, naming them, and while another merge,
  cherry-pick or revert is unfinished). Nothing is written until every server
  call has answered: results go to disk with `overlay::apply` (everything
  fetched first, journalled temp-and-rename writes, state saved last), not
  `materialize`, which saves partial progress.
- **`merge REV`** calls `POST /v1/merge` with parents `[base, REV]`, so the
  working copy is "ours". `REV` already in the base's history is "already up to
  date". A base that is an ancestor of `REV` fast-forwards unless `--no-ff`.
  A clean answer becomes the base. `-m` defaults to
  `Merge REV into <target ref>`; `--sign` signs a clean result (the merge
  changeset is made by the server, so the author cannot sign it any other way).
- **Merge in progress.** `MergeResponse::Conflicts` gains `clean`, the
  cleanly merged changes relative to `parents[0]`, mirroring `RebaseResponse`
  and `LandResponse` (additive, `#[serde(default)]`; old clients ignore it).
  On conflicts the CLI writes the clean changes plus conflict markers (text)
  or our version marked `unresolved` (anything else), exactly as `sync` does,
  leaves the base at the first parent and records
  `State.merge_in_progress = {other, message, added}` in the same state save.
  The next `leep commit` (which then needs no `-m`) creates a changeset with
  parents `[base, other]`, even when the resolution left no file changed, and
  clears the record. `merge --abort` removes the files the merge created
  (`added`) and hard-resets to the base. `status` shows `merging <id>`
  (`merging` in JSON). While it is unfinished, `merge`, `rebase`,
  `cherry-pick`, `revert`, `sync`, `land`, `amend`, `stash`, a soft `reset`
  and a plain `checkout` or `switch` are refused, pointing at `commit` and
  `merge --abort`. `reset --hard` and `checkout --force` are the other ways
  out and clear the record.
- **A server without `clean`** answers conflicts with an empty map, and using
  that would drop the other side's non-conflicting changes from the merge
  commit without a word. The CLI therefore diffs the other side against the
  merge base and checks that every changed path is reported (clean or
  conflicting) or already identical in the base; otherwise it stops with
  nothing changed and says to upgrade the server.
- **Whose version stays.** Where a conflict has no text merge, `sync` keeps
  the rebased edits (theirs in the rebase) and these commands keep the working
  copy's own file (ours), because the working copy is on the base and the other
  side is the thing being brought in. `overlay::plan_conflicts` takes a `Keep`
  to say which. The note printed with each such path gives `leep cat` and
  `leep restore --source` commands to see or take the other side.
- **`rebase [ONTO]`** moves the first-parent stack of unlanded commits
  (the base and its ancestors not in `ONTO`'s history) onto `ONTO`, oldest
  first, with the single-changeset rebase, sharing `overlay::move_stack` with
  `sync`. Without an argument `ONTO` is the target ref's head, which is `sync`
  without local changes. A base already in `ONTO`'s history is fast-forwarded.
  A conflict stops with nothing changed and names the commit and its paths;
  there is no markers mode, because a conflict inside an already-committed
  stack has no uncommitted home. The message says to settle it by hand:
  `checkout ONTO`, then `cherry-pick` each commit. Stacks containing a merge
  are refused, as in `sync`, because `/v1/rebase` rejects merges.
- **`cherry-pick REV`** is `POST /v1/rebase {changeset: REV, onto: base}`. The
  copy keeps REV's author, message and `extra` and gains the server's
  `leep.rebased_from = REV`; the rebase API cannot set other `extra` keys, so
  there is no separate `leep.cherry_picked_from`. A conflict is written like a
  merge conflict and recorded as `State.pick_in_progress = {from, message,
  revert, added}`; `commit` defaults to that message, adds the same
  `leep.rebased_from` key and clears it; `cherry-pick --abort` as for merge.
  Merges and changesets already in the base's history are refused.
- **`revert REV` without a new server operation.** The CLI computes the
  inverse of REV's first-parent change from `GET /v1/diff` between REV's
  parent and REV (the old content, or a delete for a path REV added), stores it
  as a temporary changeset R with `parents = [REV]` and
  `extra["leep.revert_of"] = REV`, and sends `POST /v1/rebase {changeset: R,
  onto: base}`. `Repo::rebase` merges three ways with R's first parent as the
  merge base, so the base of the merge is REV itself, "theirs" is REV undone
  and "ours" is the current tree. Lines edited since elsewhere in a file
  survive, and only the reverted hunks go back; a change made since to the same
  lines is a conflict, not a silent pick. This reads
  `merge_trees(tree(first parent of R), tree(onto), tree(R))` in
  `leep-derive/src/merge.rs`, and is tested at the derive level and end to end
  with the real server. The message is `Revert "<first line>"` plus
  `This reverts <id>.`. A revert needs REV in the base's history (otherwise the
  three-way merge has nothing sensible to undo), a non-empty change, and a
  non-merge REV: reverting a merge needs a mainline (`-m`), which is not
  supported yet. A conflict is recorded like a cherry-pick's with `revert =
  true`; the manual commit carries `leep.revert_of`.
- **Why not `POST /v1/revert`.** An endpoint would be API surface for what two
  existing calls express, and the CLI does the same for `sync` snapshots and
  stashes (ADR 0017). The costs are the same: a temporary changeset per revert.
  A server endpoint can replace it later without changing the behaviour.
- **`--json`** gives each command a `status` (`merged`, `fast_forward`,
  `up_to_date`, `rebased`, `picked`, `conflicts`, `aborted`) with the new
  `base`, the file counts and, for conflicts, the `conflicts` (markers) and
  `unmerged` (no text merge) paths. Exit code 1 for conflicts, as `sync`.
- **`commit -m` is optional** only while a merge, cherry-pick or revert is
  unfinished; otherwise it is still required, now with a message of our own
  instead of the argument parser's.

## Alternatives
- **A server-side `/v1/revert` or cherry-pick endpoint.** See above.
- **`git revert -m` mainline support.** Needs a choice of parent as the
  merge base; the inverse-changeset approach generalizes by parenting R on
  the chosen mainline, left for later.
- **Markers for a conflict inside a rebased stack.** One squashed change per
  stack is what `land` does; replaying commit by commit with markers needs a
  per-commit "rebase in progress" state. Not needed yet.
- **Always a merge commit, or never fast-forward.** `--no-ff` covers the
  choice; the default matches what a git user expects.

## Consequences
- `/v1` only grows: `MergeResponse::Conflicts.clean` is optional on the wire.
  No persisted object format changed. `.leep/state.json` gains two optional
  fields; an older client ignores them and so would not know a merge is in
  progress.
- Every revert leaves a temporary changeset on the server (blob GC keeps what
  it references, as with snapshots); every clean cherry-pick, revert and
  rebase leaves the originals behind as well. They are cheap to find
  (`leep log --extra leep.revert_of`).
- Signatures do not carry from the originals to merge or revert results made
  here, except through the server's rebased-origin lookup; use `--sign` (or
  `commit --sign` after a conflict) on a ref that requires them.
- Limitations: no mainline revert, no merge in a rebased stack, no
  `--continue` for a rebase, and sparse checkouts refuse results that touch
  paths outside their prefixes rather than half-apply them.

## Security review 2026-10
See [SECURITY-REVIEW.md](../SECURITY-REVIEW.md). Deferred (S-10): the
server's three-way text merge (`diffy`) is quadratic in differing lines (about
25 s for two unrelated 40,000-line versions) and synchronous, so a write
token can tie up a worker in `merge`, `rebase` or `land` for longer than the
request timeout. Bounding it needs a deterministic cost limit; a wall-clock
deadline would make merge results depend on server load. Per-token
concurrency limits (ADR 0020) bound how many workers one token can hold.
