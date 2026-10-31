# 0065. Blame across renames and merges
Status: Accepted
Date: 2026-10-31

## Context
Blame followed first parents only and credited each line to the changeset
whose diff against its first parent inserted it. Compared with `git blame`
on ripgrep imported through `leep-git`, that was wrong in two common ways:

- **Renames.** About 130 lines of `crates/core/main.rs` were credited to
  "repo: move all source code in crates directory", the changeset that moved
  the file, because the walk stopped where the path first appeared. Git
  follows the file across the move.
- **Merges.** A line written on a branch and merged was credited to the
  merge, because the merge's diff against its first parent inserts it. Git
  credits the branch's changeset.

Leep already detects renames for `log --follow` (ADR 0016), and the blame
walk (index skipping, stretches, batched reads; ADR 0058 bounds it) is
fast for first-parent chains. Blames are cached as derived data, so cached
answers computed the old way must not be served for the new semantics.

## Decision
- **Semantics** (as git passes blame to parents). For a version of the file
  that differs from its first parent's:
  1. Lines the diff against the first parent's version keeps take that
     version's credit.
  2. For each other parent in order whose version differs from the first
     parent's, the lines still unexplained that the diff against it keeps
     take its credit.
  3. The rest are credited to the changeset.

  A version whose path does not exist in its first parent is a creation.
  If it is not a merge that took the file from another parent, and the
  changeset deletes some file, its diff against the first parent is
  searched for renames with `log --follow`'s detection and default
  threshold (`RenameOptions::default()`, whole files, no copies); a rename
  source takes the first parent's place in step 1. A version the same as
  its first parent's has that version's blame, as before. Differences from
  git: renames are not searched at other parents, and a merge whose file
  equals a non-first parent's still diffs against the first parent first
  (git passes everything to an identical parent); lines both parents hold
  then keep the first parent's credit.
- **Walks.** The first-parent walk is unchanged and finds the changers of
  one path down one chain. Among the changers it finds, merges (told apart
  by `last_merge-v1`, falling back to the changeset) and creations start
  further walks from the other parents and rename sources. A walk stops at
  the first changer an earlier walk of the same blame found, as it stops at
  a cached blame, so a merged branch costs its own changes plus a step into
  the main line. Walks run breadth first, 16 at a time, iteratively; no
  history is recursed over on the call stack. The versions found form a
  graph, replayed oldest first by generation, in the existing windows of
  64 versions read while the previous window is diffed on several threads;
  a version is held only until the versions made from it are replayed.
- **Bounds.** A blame starts at most 1,024 walks below merges and renames.
  A bounded blame (ADR 0058) walks a merged branch with its own floor, the
  branch's newest indexed first-parent ancestor (rows are parent-closed, so
  everything below is indexed), and does not follow a branch more than 128
  un-indexed steps from one. Past either bound the merge keeps the lines it
  brought in, as before, and the answer is **not cached**, so a cached row
  is always the complete answer; later requests, finding the walks' cached
  rows, get further. The API is unchanged.
- **Non-UTF-8 versions.** One on the blamed file's first-parent line fails
  the blame (`Invalid`), as before. One reached through a merge or rename
  is not followed: the merge keeps those lines.
- **Derived data.** The cache moves from `blame` to `blame-v2` (same key and
  encoding, new meaning). `blame` is listed as a retired kind:
  `repair drop --kind blame` and `upgrade-index --drop-retired` delete its
  rows, and no current server reads it.

## Consequences
- Blame credits match git's in the measured cases: lines of a moved file go
  to the changesets that wrote them, merged lines to the branch changesets.
- **Cost.** A linear history costs what it did, plus one batched
  `last_merge-v1` read per batch of walks. A changed merge costs one more
  diff and a short walk per other parent; a creation that deletes something
  costs a tree diff and rename detection, as `log --follow` does. Rename
  detection is skipped for root creations and for changesets that delete
  nothing.
- **Upgrade.** The first blame of each file after the upgrade is computed
  again (no warming needed). Old servers keep reading and writing `blame`
  beside new ones; drop it once they are gone.
- **Determinism.** A complete blame depends only on history: a walk that
  stops at another walk's changer or at a cached row reaches the same
  version it would by walking on. Incomplete answers are never stored.
- `/v1/blame` is unchanged: `origin_line` is the line in the crediting
  changeset's version, which may be under the file's old name.
