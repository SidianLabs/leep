# 0066. Land fast path and conflict policy
Status: Accepted
Date: 2026-10-31

## Context
A land whose head moved since its base is replayed: each changeset of its
stack gets a three-way merge of its parent's tree (base), the head (ours)
and its own tree (theirs), and the result is stored as a rebased copy
(ARCHITECTURE section 6, ADR 0032, ADR 0043). With many agents on one ref
three costs grew with the ref's activity rather than with the land:

- `merge_trees` diffed base against ours in full, so a land based a few
  hundred lands back paid for every file those lands touched.
- Every replayed land paid that merge, although under many agents most
  lands touch files nothing else touched since their base, where the merge
  always takes their side.
- Each copy's tree was built by reading back, verifying and decoding the
  directories the previous copy had just written, and every rewritten node
  was encoded twice.

Mononoke's pushrebase takes the other route for the same problem: a land
conflicts when any file it touches changed since its base, so the server
never merges file contents.

## Decision

### 1. The slow path reads ours only where theirs changed
`merge_trees` diffs only base against theirs, and reads ours at the paths
that diff names, one directory level at a time with batched tree reads
(`Repo::files_at`). The file ours has at such a path is exactly what the
full diff gave: its new side where the path changed, base's file where it
did not. Resolution, the file/directory collision check and the conflict
answers are unchanged. A test-only oracle keeps the full-diff merge and a
randomized test checks they agree.

### 2. The fast path: copy disjoint lands without a merge
The leader of a ref's lands (one per ref per process, the lease holder
under ADR 0056) keeps a write map in the ref's queue (`land_fast.rs`): for
each changeset of the ref's first-parent line it has followed, a sequence
number; for each path, the newest such changeset that wrote it; for each
directory except the root, the newest that wrote below it.

A stack (each changeset of it, in order) takes the fast path when its base
(the parent of its lowest member) is a changeset the map holds and none of
its members' paths was written after the base, none of their ancestor
directories was written as a file, and nothing was written below any of
them. Then each member is copied with `rebased_copy` onto the previous copy
(the head first), with its own changes less the modifies that leave a file
as its parent has it, and no merge.

This is the slow path's copy to the byte. For each path the member changes,
ours and base hold the same entry, so the merge resolves to theirs: the
member's change, the tree diff it starts from having omitted the no-op
modifies. The collision check finds nothing either: a file at an ancestor
in ours is one base had, which the member must delete; a directory at the
path in ours is base's, whose files the member must delete. The copy keeps
the author, timestamp, message and extra and adds `leep.rebased_from`, and
the `rebased-origin-v1` row is written with it, through the same
`rebased_copy` the slow path uses, so ids, root trees, links and the ref
log are identical. Tests check this byte for byte, and on random concurrent
histories against a repository replaying every land in full.

Correctness never rests on the map. It is keyed by the head it describes
and checked against the head each batch attempt reads; any other head
(a restart, a lost lease, another server's move, a lost swap, a
`rebase-onto`) starts it again from that head. Every move a batch makes is
followed down its first-parent chain to the previous head, at most 1,024
changesets, or the map starts again at the new head. It holds the newest
4,096 changesets and at most 2^20 paths and directories, dropping writes
only lands based before its oldest changeset could need, or starting again.
A base the map does not hold (cold, out of its window, off the first-parent
line: a stack on its own landed copy, a base reached through a merge), a
root changeset, a stack with a merge (refused as before) or any overlap
replays the slow way. Turning the fast path off (`--land-fast-path off`)
changes no answer.

The land policy (signatures, review, including the paths a merge drops
from a side, ADR 0011) runs on every land before any replay, as before, on
the submitted changesets; the fast path only changes how the copies are
computed.

The landed-copy scan (ADR 0021) stays for every land, keyed or not. ADR
0057 answers a keyed land from its record only when that land moved the
ref. A keyed land answered "already landed" from the scan records nothing,
so its keyed retry, and a second land of an already-landed changeset under
a new id (`leep land` run again after an answer), is recognised only by
the scan; skipping it would land an empty duplicate copy.

### 3. A batch builds its trees on the nodes it holds
A batch attempt's staging keeps the decoded tree nodes its replays read and
write, by hash. Each copy's tree is built on the previous copy's nodes in
memory, rewriting only the directories on its changed paths, and every
intermediate tree and its `root_tree` row is still written (the
parent-closed invariant, ARCHITECTURE section 5). A node's hash names its
content, so the memo can only answer with the stored node; it is dropped
with the attempt. Nodes are encoded once.

### 4. A per-ref conflict policy for the slow path
`--land-conflicts merge|strict` (default `merge`, the behaviour before this
ADR) and `--strict-land-ref REF` choose what the slow path makes of a file
both the ref and the land changed since the land's base: `merge` merges it
three ways; `strict` answers it as a conflict (no merged text), whatever a
merge would make of it, as pushrebase does. The rest of the answer is as
before, so the client resolves the conflicts on the new head and lands
again. It is server configuration only: nothing persisted or on the wire
changes. Disjoint lands land without a merge under either policy.

### 5. Admission waits out slow batches
ADR 0060 clamped a refused land's `Retry-After` (the time until a queue
slot frees, about one batch time) to 1 to 10 s. With 20 s batches at 1,024
agents clients came back while the batch they waited for still ran, and
were refused 13,000 to 28,000 times a run. The ceiling is now half of
`MAX_BATCH_LIFETIME` (30 s), which covers every batch a healthy server
makes; `leep land` waits it out with its jitter (at most 45 s) inside its
60 s admission budget. A `leep` that reports a wait over 10 s instead of
sleeping does so only for reads (`MAX_RETRY_WAIT`), never for a refused
land.

### 6. Read caching
`Repo::read_tree` and `read_trees` keep the nodes they read from the store,
decoded, in a byte-bounded cache by hash (16 MiB, taken from the former
32 MiB tree cache, so the default cache budget is unchanged); they read
from the store on every call before, which made a land 50 lands behind the
head cost 170 to 200 ms on Postgres with a 20,000-file tree. A staged batch
reads the nodes it did not write in one batched read. The merge's collision
checks read their entries in one batched walk instead of one walk from the
root per path component.

### 7. Metrics
`leep_land_replays_total{path}` and `leep_land_replay_seconds{path}` (fast,
slow), `leep_land_fast_map_total{result}` (hit, overlap, cold, window,
ineligible), `leep_land_fast_map_resets_total`,
`leep_land_batch_replay_seconds` and `leep_land_batch_tree_seconds`.

## Consequences
- A replayed land costs the size of its own change on both paths, and a
  disjoint one no merge at all. What it still pays per land: the landed-copy
  scan and the stack walk, the land policy, `store_changeset`'s checks of
  each content it names, and hashing every rewritten directory up to the
  root.
- The map is memory only and per leader. A new leader starts cold and lands
  based before its first head replay the slow way until the agents' bases
  catch up.
- `strict` refuses lands that would have merged; agents redo them on the
  new head. It is opt-in per server or ref.
- Both paths must stay byte-identical: a change to the slow path's copies
  (say, a new rebase marker) must be made in `rebased_copy`, which both use,
  or the fast path must be changed with it; the differential tests catch a
  divergence.
