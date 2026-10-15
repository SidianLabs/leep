# 0008. Blob garbage collection
Status: Accepted; the residual race is closed by ADR 0012
Date: 2026-10-15

## Context
Clients upload blobs before they create the changeset that references them
(`PUT /v1/blobs/{hash}`, then `POST /v1/changesets`). A client that crashes,
abandons the commit, or fails validation leaves its blobs behind. The server
also writes tree nodes and merge-result blobs before it stores the changeset
they belong to, so a failed `create_changeset`, `merge` or `rebase` leaves
those behind too. Nothing ever removes them.

Changesets are immutable and never deleted in v0.2, and a changeset can be
referenced only by its id (an unlanded stack, a review link). So "reachable
from refs" would be an unsafe definition of live.

## Decision
**Live** means referenced by any stored changeset: every
`FileChange::Modify.content`, plus every tree node and file blob reachable from
the root tree of every changeset. Marking from all stored changesets is
equivalent to marking from refs for landed history, and additionally keeps
unlanded changesets safe. Everything else is garbage.

`leep_derive::gc::run` (backend-agnostic, on `Repo`):

1. Record `started_at`.
2. Mark: page through `MetaStore::list_changesets`. For each changeset, mark
   its `Modify` hashes and walk its root tree (derived on demand) with an
   explicit stack. A tree hash already in the mark set is not walked again, so
   subtrees shared between snapshots are visited once. Tree hashes and blob
   hashes are domain-separated, so one `HashSet<Hash>` serves as both mark set
   and visited set. Memory is about one hash per distinct live blob or tree
   node (roughly 50 bytes each); a repository with 10 million live blobs needs
   about 500 MB for a run. Any error (missing tree node, undecodable
   changeset) aborts before the sweep, so a partial mark set never deletes.
3. Sweep: page through `BlobStore::list_page`. Delete a blob only if it is not
   marked and its `modified` time is at or before `started_at - grace`. A blob
   with unknown `modified` is kept unless `grace` is 0.
4. Report changesets scanned, marked count, blobs scanned, deleted count and
   bytes, kept by grace, kept for unknown age. `dry_run` does everything
   except the delete calls.

Trait additions (no persisted format changes):
- `BlobStore::list_page(after, limit) -> Vec<BlobMeta>` (ascending hash order,
  exclusive cursor; `BlobMeta { hash, size, modified: Option<i64> }`) and
  `BlobStore::delete(hash)` (idempotent).
- `MetaStore::list_changesets(after, limit) -> Vec<ChangesetId>`.
- No default implementations: a backend that cannot list or delete should fail
  to compile rather than silently never reclaim space. All backends implement
  them and `check_blob_store_contract` / `check_meta_store_contract` cover
  them.
- `put` of a blob that already exists refreshes its modification time (the
  filesystem backend touches the file; object stores and the memory store
  rewrite it). Without this, re-uploading an old orphan would leave an old
  mtime and the grace window would not protect it.

Operation: `leep-server gc [--dry-run] [--grace 24h]`, using the same
`--meta` and `--blobs` flags. Durations are `0`, `Ns`, `Nm`, `Nh` or `Nd`.
It never goes through the HTTP API and does not use the blob cache.

### The in-flight commit race
The mark phase only sees stored changesets. A client that has uploaded blobs
but not yet created its changeset is invisible to it. The grace window covers
this: **a client must create its changeset within `grace` of its last upload
of each blob** (default 24h). The CLI uploads and commits in one command, so
this holds by a wide margin; long-lived agent sessions that stage uploads for
longer than the grace window should raise `--grace`. If the window is violated,
`create_changeset` fails with "missing blob" (it checks that every blob
exists), and the client re-uploads and retries. That is loud, not corrupting.

This is also why GC is safe to run while servers are up: every write a server
makes (upload, tree node, merge blob) carries a fresh mtime and is protected
for `grace`, and writes that become referenced are marked.

### The reuse race and blob claims
Content is deduplicated: before committing, a client asks which blobs are
missing (`POST /v1/blobs/missing`) and skips uploading the rest. If one of
those "present" blobs is an old orphan, the client references it without
refreshing its mtime. A changeset stored after the mark phase passed its
position would then point at a blob the sweep deletes, and this window is the
whole GC run, not milliseconds.

So the server records a **claim** for every blob it reports present: derived
data kind `blob-claim-v1`, key = blob hash, value = unix seconds. The sweep
treats a claim like an upload: a blob claimed at or after `started_at - grace`
is kept (`kept_by_claim` in the report), and the claim is checked immediately
before each delete. Claims older than the grace window no longer protect
anything, so abandoned claims don't pin garbage forever.

Superseded in part by [ADR 0012](0012-fenced-gc.md): the sweep now condemns,
waits out a fence and collects, and the race below no longer exists. The text
is kept as the record of the original design.

Residual race: a claim written between the sweep's claim check and its
delete of that same blob (microseconds), for a blob that has been an
unreferenced orphan for longer than the grace window. Operators who cannot
accept even that should run GC with writes quiesced. Closing it fully needs a
delete conditional on metadata, which neither filesystems nor S3 offer
portably; a fenced two-phase GC is on the roadmap.

Clock skew: `modified` comes from the blob store (file mtime, S3
`LastModified`), `started_at` from the machine running GC. Skew smaller than
the grace window is harmless; choose a grace that dwarfs expected skew.

## Consequences
- Space of abandoned uploads and failed derivations is reclaimable, safely
  online.
- Because history is never pruned, GC never removes anything a changeset can
  reach, and `read_file` over all history is unaffected. Tests assert this
  exhaustively on a branching history with merges, deletes and nested
  directories.
- A run scans every changeset and every blob, so it costs O(history +
  blobs) listing and O(live blobs) memory. Run it from cron, not per request.
- Derived data (`root_tree`, `generation`, `skiplist`, `blame`) is not blobs
  and is untouched. GC may write missing `root_tree` rows while marking.
- If a future change prunes history or adds a blob reference outside
  changesets, the mark phase must be extended in the same change.
