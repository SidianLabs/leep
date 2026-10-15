# 0016. Rename detection
Status: Accepted
Date: 2026-10-15

## Context
A changeset records a rename as a delete of the old path and an add of the new
one. That is the right thing to store: it keeps `Changeset` small and says
only what changed. But a reader of a diff wants "this file moved", and
`log -- <path>` stops where a file got its current name, which hides most of
its history. Git has the same model and detects renames when displaying. Leep
needs the same for `diff`, `status` and `log`, for humans and for agents
reading history through MCP.

## Decision
Renames are detected at read time and never stored. Nothing persisted
changes: no new derived kind, no change to `Changeset`, `TreeNode` or any
encoding.

**The pairing is a pure algorithm in `leep_core::rename`**, so the server and
the CLI's working-copy diff run the same code (`leep-cli` may depend only on
`leep-core`). It pairs the *files* a diff deleted with the files it added, in
stages over what the previous one left:
1. **Exact**: same content hash and same kind. Always runs, linear time (a
   hash map). When several added files match one deleted file (copies), the
   deleted and added files pair in sorted path order and the rest stay adds.
2. **Same bytes, kind changed** (regular to executable): similarity 100,
   `exact: false`, so the mode change is still shown.
3. **Similar**: regular and executable files that are UTF-8 text without NUL
   and at most 1 MiB. A file is split into lines, each keeping its `\n` (a
   last line without one is a line). The score of two files is
   `floor(200 * common / (bytes_a + bytes_b))`, where `common` is the multiset
   intersection of their lines weighted by line length in bytes, so identical
   files score 100 and files sharing no line score 0. Lines are compared by a
   64-bit FNV-1a hash. Pairs scoring at least the threshold (default 50,
   configurable 0 to 100) are taken greedily by score, then by deleted path,
   then by added path. A pair of sizes whose best possible score is already
   below the threshold is skipped without reading content.

Symlinks pair only in stage 1. Binary files and files over 1 MiB pair only in
stages 1 and 2. Output is sorted by new path and does not depend on input
order.

**Limits.** Similarity compares every remaining deleted file with every
remaining added file. If that is more than 1 000 000 pairs (git's rename
limit of 1000, expressed as a pair count: 1000 x 1000), or the candidates
total more than 256 MiB, the stage is skipped and the result says
`renames_truncated`. Exact detection always runs. The pair count is taken
after exact pairing, so moving a large directory unchanged never truncates.
A candidate's content is read once and reduced to a line multiset, so memory
follows the number of distinct lines and not file size. Ties are not broken by
file name similarity, as git does; sorted path order is used.

**API (additive).** `GET /v1/diff` takes `renames=true` (default false) and
`rename_threshold=0..100` (default 50; anything else is 400, even when
`renames` is off). With renames on, `DiffResponse` gains
`renames: [{from, to, similarity, exact}]` and `renames_truncated`. Both are
`#[serde(default)]` and omitted from the JSON when empty or false. `entries`
is untouched: a rename is still a delete entry and an add entry. We chose a
side list over changing entries (a `renamed_from` field on the add, or a
third entry kind) because every existing client keeps working unchanged, a
client that ignores the list sees exactly the old diff, and the list is
self-contained (`from` and `to` are paths in `entries`, whose `old` and `new`
carry the content hashes). The cost is that a client wanting a rename's
content diff looks the two entries up by path. Renames are only paired within
the diff's `prefix`, since the other side is not in view.

**`log --follow`.** `GET /v1/log/{id}` takes `follow=true` with a file `path`
(400 for no path or a directory). `Repo::log_follow` walks the path-history
index (ADR 0013) like the plain path log. When it pops a changeset that
created the file (a modify of the path whose first parent has no file there),
it diffs that changeset against its first parent, runs detection with the
default threshold, and if the file is the target of a rename, continues from
the changeset's parents under the old name. Because the walk is a heap over
many changesets, each pending changeset carries the name it is followed
under, so a rename on one branch does not rename the file on a concurrent one:
after a rename the old name is followed in every parent, and the new name only
in merged-in parents, which may hold the file's earlier history under it. A
changeset reached under two names is yielded once, preferring the name a
rename created. It stays iterative, and costs one extra diff per changeset
that created the file. Entries carry two additive fields on `ChangesetInfo`:
`path_at` (the file's name in that changeset) and `renamed` (the rename that
gave it its name, on the entry where that happened). Both are absent outside
a follow log. Filters and `limit` apply to what is yielded, while the walk
follows renames through filtered-out changesets.

**Clients.** `leep diff` asks for renames by default and prints
`renamed: old -> new (NN%)` followed by a content diff unless the rename is
exact; `--no-renames` and `-M[=N]` / `--find-renames[=N]` control it (the
value needs `=`, so `-M a b` is a flag and two revisions). The working-copy
diff and `leep status` run the same algorithm locally, reading old content
through the blob cache and new content from disk. `status --json` keeps its
`changes` list and adds `renames`. `leep log --follow FILE` takes one file
(it conflicts with `--path`). MCP: `leep_log` takes `follow`, `leep_diff`
takes `renames` and `rename_threshold`, both default off so existing output
is unchanged.

## Alternatives considered
- **Store renames in the changeset** (a `Rename { from }` variant of
  `FileChange`, or a side map). Rejected: it changes the persisted
  `Changeset`, so every hash and every stored changeset, and needs a
  `changeset-v2` with a migration. It would also record what an author said
  and not what the content shows, so a rename done by delete and add (every
  existing client, the git bridge) would still go undetected. Detection at read
  time works on all history already stored and can improve later without a
  format change.
- **Derive and index renames per changeset** (a new derived kind). Rejected for
  now: detection is cheap for typical diffs, only runs for the changesets a
  follow walk touches, and an index would need a similarity definition frozen
  into the format. Revisit if `--follow` on very large changesets is slow.
- **Change `entries` to carry renames in place.** Rejected: it breaks clients
  that treat entries as delete or add or modify.
- **Chunk-based scoring as git does** (`copied bytes / max size`). Rejected for
  Dice-style `2 * common / total` over whole lines: it is symmetric, simple to
  state and test, and lines are what `merge` and `blame` already use.

## Consequences
- `/v1` only grows; old clients and servers interoperate (a new CLI against an
  old server sees no renames and no `path_at`; `follow` and the rename
  parameters are ignored by old servers, so an old server answers a follow log
  with the plain path log).
- Rename detection costs blob reads for similarity candidates, bounded by the
  limits above. Exact renames cost nothing.
- Results depend on the algorithm and its constants. They are not persisted, so
  changing them (threshold default, scoring) is not a format change, but it
  changes what users see; do it deliberately and record it here.
- A rename combined with a rewrite below the threshold, a move of a file whose
  content was split or merged, and a rename inside a diff cut by `prefix` are
  not detected.
- `follow` of a file that is a directory at the starting revision is an error;
  a path that was a directory in the past is just a path.
