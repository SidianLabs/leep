# Leep architecture

Leep is an open-source, server-centric version control system for
repositories where most clients are always-online AI agents and CI jobs. It
follows the design Anthropic published for its internal system Scion
("Redesigning source control at Anthropic", Oct 2026), borrows from Meta's
Mononoke (bonsai changesets, derived data) and adds Zed DeltaDB's idea of
recording agent provenance with every change.

Leep does not try to replace git for ordinary projects. It targets
repositories with very high merge rates, and interoperates with git through an
import/export bridge.

This document is the source of truth for how Leep fits together. Decisions and
their reasons live in [`adr/`](adr/). Wire and storage formats are specified in
[FORMATS.md](FORMATS.md). Planned work is in [ROADMAP.md](ROADMAP.md).

---

## 1. Principles

These are load-bearing. A change that breaks one needs an ADR.

1. **The server is the center.** Commit, merge, rebase, diff, blame and
   history run on the server. Clients fetch only what they read.
2. **Immutable, content-addressed data.** Blobs, trees, changesets and
   derived values never change once written. Any cache at any tier may keep
   them forever, with no invalidation.
3. **Minimal global mutable state.** Refs are the only mutable state. They exist
   for `main` and release branches. Work in progress is identified by
   changeset id. A ref moves only by compare-and-swap on one row.
4. **Store the minimum, derive the rest.** A changeset lists only the paths it
   changed. Full trees, generation numbers, ancestry indexes, path history and
   blame are *derived data*, computed from parents and cached.
5. **Cost scales with the change, not the repository.** Committing rewrites
   only the directories on changed paths. Diff skips identical subtrees by
   hash.
6. **No merge queue.** Landing rebases onto the current head on the server
   and retries the CAS. Testing happens after merge and before deploy.
7. **Agent provenance is part of history.** `Changeset.extra` carries
   `agent.*` keys and is covered by the changeset hash.
8. **Thin clients.** The CLI depends only on `leep-core` and speaks HTTP. All
   repository logic lives behind the API, so it can change without client
   upgrades.

## 2. System overview

```
 agents / CI / humans
        │  leep CLI (lazy, sparse checkouts; local object cache)
        ▼
 ┌──────────────────────────┐    HTTP/JSON  (leep_core::api, v1)
 │ leep-server (stateless)  │◄──────────────────────────────────
 │  axum handlers           │
 │  └─ leep-derive::Repo    │  operations + derived data
 │      ├─ BlobStore ───────┼──► CachedBlobStore (LRU) ─► S3 / MinIO / filesystem
 │      └─ MetaStore ───────┼──► Postgres (prod) / SQLite (single node) / memory
 └──────────────────────────┘
        ▲
        │ in-process
 leep-git (import git history → changesets; export → git)
```

Any number of server instances can run behind a load balancer. They share
nothing except the two stores. Correctness depends only on (a)
content-addressing and (b) the atomicity of `MetaStore::cas_ref`.

## 3. Crates and dependency rules

| Crate | Role | May depend on |
|---|---|---|
| `leep-core` | Types, hashing, encoding, `BlobStore`/`MetaStore` traits (including listing and blob deletion for GC), in-memory stores, conformance tests, API types, pure rename detection | no other leep crate; no I/O, no async runtime |
| `leep-store` | `BlobStore` backends: filesystem, `object_store` (S3/MinIO), `CachedBlobStore`, `open_blob_store(url)` | core |
| `leep-meta` | `MetaStore` backends: SQLite, Postgres, `open_meta_store(url)` | core |
| `leep-derive` | `Repo`: derived data framework and kinds, plus all operations and blob garbage collection (`gc`) | core (traits only, never a concrete backend) |
| `leep-server` | HTTP server binary `leep-server` | core, store, meta, derive |
| `leep-cli` | Client binary `leep` | core only |
| `leep-mcp` | MCP server binary `leep-mcp` for AI agents (stdio JSON-RPC; talks to `leep-server` over HTTP) | core and the `leep-cli` library (its HTTP client); never derive, store, meta or server outside dev-dependencies |
| `leep-git` | Git bridge: library plus the `leep-git` admin binary | core, derive; store and meta only to open stores by URL in the binary |

Rules:
- Dependencies point toward `leep-core`. Nothing depends on `leep-server`, and
  only `leep-mcp` depends on `leep-cli` (for its client library).
- `leep-derive` must stay backend-agnostic. It is tested against
  `leep_core::memory`.
- Every `BlobStore`/`MetaStore` backend must pass
  `leep_core::memory::check_*_contract`.

## 4. Data model

| Object | Identity | Stored in |
|---|---|---|
| Blob (file bytes) | `BLAKE3(bytes)` | `BlobStore::put` |
| `TreeNode` (one directory) | `Hash::of_object("tree-v1", postcard)` | `BlobStore::put_with_hash` |
| `Changeset` | `Hash::of_object("changeset-v1", postcard)` | `MetaStore::put_changeset` |
| Ref | name → `ChangesetId` | `MetaStore`, CAS only |
| Ref log entry | `seq` → (ref, old, new, time, actor, operation, message) | `MetaStore`, appended by `cas_ref_logged` in the CAS's own transaction |
| Derived value | `(kind, key)` → bytes | `MetaStore::put_derived` |

`Changeset { parents, author, timestamp, message, changes, extra }`:
- `changes: BTreeMap<RepoPath, FileChange>` is relative to `parents[0]`. A
  root changeset is relative to the empty tree.
- `FileChange` is `Modify { content, kind, size }` or `Delete`.
- Merges have two or more parents. Changes are still relative to the first.

**Invariants.** `leep-derive` checks these on create and rejects violations
with `Error::Invalid`:
- every parent exists;
- every `Modify` blob exists, and its length equals `size`;
- `Delete` targets a file that exists in `parents[0]`'s tree;
- the resulting tree never has a path that is both a file and a directory.

`RepoPath` is validated on construction and on deserialization, so invalid
paths cannot enter the system through any route.

## 5. Derived data

A derived kind is a pure function
`value(cs) = f(cs, value(parent₁), …, value(parentₙ))`. The framework:

1. finds ancestors without a stored value by walking parents **iteratively**
   (histories can be hundreds of thousands deep);
2. derives them oldest-first;
3. stores each with `put_derived(kind, cs_id, bytes)`.

Because inputs are immutable, a value is computed at most once per kind and
changeset, and concurrent servers that race produce identical bytes.

| Kind | Value | Used by |
|---|---|---|
| `root_tree` | 32-byte tree hash. Applies `changes` to `parents[0]`'s tree, rewriting only the directories on changed paths and pruning empty ones | everything |
| `generation` | `1 + max(parents)`; roots = 1 | log ordering, ancestry |
| `skiplist` | power-of-two first-parent ancestors | `is_ancestor`, `merge_base` |
| `signature-v1` / `rebased-origin-v1` | attestations and server rebase links, keyed per (changeset, key) and per copy; not pure functions of the changeset, so written by `put_signature` and `rebase` rather than derived | signed-changeset listing and the land policy |
| `review-v1` | review verdicts, keyed per (changeset, reviewer name) (`Hash::of_object("review", id‖name)`); attestations written by `put_review`, last write wins, listed with one lookup per principal | review listing and the review land policy |
| `last_merge-v1` | generation of the nearest merge on the first-parent chain (0 if none) | lets `merge_base` answer from skiplists when no merge sits above the first-parent common ancestor |
| `blob-claim-v1` / `blob-condemned-v1` / `blob-rescue-v1` / `blob-doom-v1` | per blob hash, 8-byte big-endian unix seconds (doom adds a state byte); GC bookkeeping, not derived from changesets: claims by uploads and dedup checks, condemnation by `gc condemn`, rescue by uploads of a condemned blob, doom by `gc collect` (ADR 0012) | blob garbage collection and the server's missing-blob checks |
| `history-tree-v1` | 32-byte hash of the root `HistoryNode`, a tree parallel to `root_tree` recording for each path (including deleted ones) the newest changesets that changed it; nodes are `history-node-v1` rows keyed by content hash. Built from the first parent's tree plus the spines of changed paths, and at merges the paths where another parent's history differs (ADR 0013) | path log, `last-change` |
| `blame` | per (changeset, path), keyed `Hash::of_object("blame", id‖path)` | blame |
| `blob-trigrams-v1` | per blob hash: sorted trigram set of a UTF-8 text blob up to 1 MiB (delta varints), or a "not indexed" marker (ADR 0014) | code search |
| `tree-trigrams-v1` | per tree node hash: fixed-size bloom filter (2^19 bits) over every trigram below the directory, the OR of its children's filters; shared by every revision with the same subtree (ADR 0014) | code search directory pruning |

`root_tree` and `generation` are derived eagerly when a changeset is created.
Other kinds are derived on first read (the search kinds on the first search
that needs them; see "Code search"). Add new kinds with the
[`add-derived-data-kind`](../.claude/skills/add-derived-data-kind/SKILL.md)
skill. A new kind never requires a client change.

**Path history.** `log --path P` and `GET /v1/last-change/{id}?path=P` read
the `history-tree-v1` index instead of scanning ancestors (ADR 0013). A lookup
walks `P`'s components through the changeset's history tree and yields the
newest changesets that changed `P` (for a directory, anything under it). The
log pushes those on a heap ordered by generation and id, yields the newest,
looks `P` up in that changeset's parents to find the next ones, and repeats. It
is iterative and returns exactly what the old scan returned, in the same order,
reading only changesets that changed the path. A merge's entry is the merge
itself if it changed the path relative to its first parent, otherwise the union
of its parents' entries, so a side branch's changes are never lost. The index
is derived on the first path query for a history, oldest first. `author` and
`extra` filters apply to the changesets the walk yields, so a selective filter
on a frequently changed path still reads each of them.

**Rename detection** ([ADR 0016](adr/0016-rename-detection.md)). Changesets
store a rename as a delete plus an add, and renames are detected when
displaying, with nothing persisted. `leep_core::rename` pairs the deleted and
added files of a diff: exact (same hash and kind, always, linear), then
same-bytes-with-a-mode-change, then line-multiset similarity for text files up
to 1 MiB (`2 * common / total` by bytes, threshold 50 by default, greedy by
score then path), symlinks exact only. Similarity is skipped, and reported as
`renames_truncated`, past 1000 x 1000 candidate pairs. `GET /v1/diff` returns
the pairs beside the unchanged `entries` when asked (`renames=true`), the CLI's
working-copy `diff` and `status` run the same code, and `GET /v1/log` with
`follow=true` continues a file's path log under its old name by detecting the
rename at the changeset that created the file.

**Provenance queries.** `agent.*` keys in `extra` (see `EXTRA_AGENT_*`) are
queryable without any new persisted data. `GET /v1/log` takes `author` and
repeated `extra=key:value` / `extra=key` filters (ANDed), `GET /v1/provenance`
counts changesets per agent model and session and per human author, and blame
responses carry a deduplicated per-changeset summary (author, time, first
message line, `agent.*` keys). The summaries are attached on read, so the
cached `blame` value keeps its original encoding. All of these walk history
linearly (provenance is capped at 10 000 changesets and reports `truncated`);
an index over `agent.*` keys is future work.

**Code search** ([ADR 0014](adr/0014-code-search.md)). `GET /v1/search/{rev}`
greps a revision on the server: a literal or regex (`regex` crate), optionally
case-insensitive, restricted by path prefix and glob, matched per line. The
planner turns the query into trigrams every matching file must contain
(literal trigrams, case variants, or literals extracted from the regex syntax
tree; none means a plain scan). Execution walks the tree depth first with an
explicit stack, skipping directories whose `tree-trigrams-v1` filter lacks a
required trigram and files whose `blob-trigrams-v1` set does, then fetches the
survivors (16 at a time, order preserved) and runs the matcher. Output is ordered
by path then line and is identical with or without the index, which a property
test checks; the walk is lazy, so a search that has enough matches stops early.
Limits: 100 matches by default, 1000 at most, 256 MiB of blob content scanned,
`truncated` when either is hit. Only UTF-8 text blobs up to 1 MiB are searched;
others count as `files_skipped`. The index rows are derived lazily by the first
search that needs them and are keyed by content, so a new revision derives only
what it changed; `leep-server derive-search <rev>` warms a revision ahead of
time. Clients are `leep search` and the `leep_search` MCP tool.

## 6. Request flows

**Commit** (`leep commit`):
1. The client hashes changed files and calls `POST /v1/blobs/missing`.
2. It uploads only the blobs the server lacks, with `PUT /v1/blobs/{hash}`.
3. It sends `POST /v1/changesets`. The server validates the changeset, derives
   its tree and returns the id.
Nothing mutable has changed yet; the changeset is just an addressable object.

**Land** (`leep land`, the no-merge-queue path):
```
lock(target)                      # per ref, within this server process
loop (≤32 attempts):
  head ← get_ref(target)
  if head is None:                 CAS(None → cs)
  elif cs is an ancestor of head:  already landed
  elif head is an ancestor of cs:  CAS(head → cs)          # fast-forward
  else:
    stack ← first-parent chain from cs down to where it joins head's history
    replay each stack entry onto head, oldest first        # 3-way per entry
    conflict? → return Conflicts{onto: head, conflicts, clean}
                (the whole stack squashed into one 3-way merge vs head;
                 never auto-resolved on the server)
    CAS(head → replayed top)
  CAS won → Landed;  CAS lost → jittered backoff, retry
attempts exhausted → 409 Conflict (client retries)
```
Lands on one ref are serialized within each server process. Without that,
concurrent lands each redo the replay only to lose the CAS to a sibling
request, so under a swarm most of them exhaust their attempts. The CAS still
coordinates separate server processes; jittered backoff keeps them from
colliding in lockstep.
Replaying the whole stack matters: rebasing only `cs` would silently drop the
changes of unlanded changesets below it. On conflict the client applies
`clean`, resolves `conflicts` on top of `onto`, commits once and lands again,
which is then a fast-forward. Stacks containing merges are rejected.

**Branches and tags** ([ADR 0015](adr/0015-branches-and-tags.md)). Refs are
branches, except those under `tags/`, which are tags. `cas_ref_checked` and
`land` in `leep-derive` apply the rules for every caller: valid names
(`leep_core::refname`), tags are created or deleted but never moved, `land`
refuses `tags/`, and a ref a land policy covers cannot be deleted. The CLI has
no special server API for this: `leep branch`, `leep switch` and `leep tag`
are compare-and-swap calls on `/v1/refs/{name}`. `branch -d` deletes only if
`merge_base(head, target) == head`, reading the head once and passing it as
`expected` so a concurrent move fails with 409. `leep switch` needs a clean
working copy, sets `target_ref` in `.leep/config.json` and materializes the
branch head the way `sync` does.

**Working copy** ([ADR 0017](adr/0017-working-copy-workflow.md)).
`.leep/state.json` records the base, the files as materialized (content, kind,
size, mtime) so `status` rehashes only files whose size or mtime changed, the
paths where conflict markers were written, and the local stash list. Uncommitted
work is whatever differs from that record. `sync` with local changes and
`stash` store the work as a *snapshot changeset* (parent = base,
`extra["leep.snapshot"]` = `sync` or `stash`, no ref). `sync` and `stash apply`
call `POST /v1/rebase` for the snapshot onto the new base and write the result
back as uncommitted changes (`overlay.rs`): everything is fetched first, each
file is written through a temp file and renamed, writes are journalled and
undone if anything fails, and the state is saved last. Conflicts become marker
files like in `land`, and `commit` and `land` refuse while any remain.
Conflicts with no text merge (binary, delete versus modify) keep the local
version and are recorded in `state.unresolved`; they block the same way until
`leep resolve PATH` or `leep restore PATH` clears them. If the
base is not in the target's history, `sync` first rebases the unlanded
first-parent stack onto the head, one changeset at a time; a conflict there
stops with nothing changed. `restore`, `reset` and `stash` write files the same
way, and `reset` and `amend` only move the base, never a ref. `amend` is
refused for a base in the target ref's history (`merge_base(base, head) ==
base`). Snapshots are ordinary changesets, so blob GC keeps what they reference.

**Ref log and undo** ([ADR 0018](adr/0018-ref-log.md)). Every successful ref
create, move or delete appends a row to `ref_log` in the same transaction (or
under the same lock, in memory) as the conditional update, so a ref never
moves unrecorded and a lost CAS writes nothing. Callers pass a
`RefUpdateContext` (actor, operation, message) to `MetaStore::cas_ref_logged`:
`land` records `land`, the git bridge `import`, and the server passes the
token's name and the request's operation for `POST /v1/refs/{name}` (`land`
and `import` are reserved so clients cannot forge them). `GET /v1/reflog` and
`/v1/reflog/{name}` page the log newest first. `leep undo` is a client of
existing endpoints: it reads a ref's latest log entry and compare-and-swaps the
ref from its current value to that entry's old value, with operation `undo`,
through `cas_ref_checked`, so name rules, immutable tags, protected deletes and
signature or review policies still apply and a concurrent move is a 409. It
never touches the working copy. Undoing a delete recreates the ref, undoing a
create deletes it, and the redo of an undo is another undo. The log is
unbounded; entries name changesets, which are never deleted, so GC is
unaffected.

**Checkout** (`leep checkout`): the client resolves a rev to an id and lists
files under the requested prefixes (`GET /v1/files`). It then fetches the
missing blobs through its local content-addressed cache and writes them. Only
requested prefixes are materialized; other files are fetched on demand with
`leep cat`.

**Merge**: three-way, per path, with base = `merge_base(ours, theirs)`.
Identical changes on both sides merge trivially. Text files merge by line
(`diffy`). Binary files and delete-vs-modify cases are returned as conflicts.

## 7. Caching tiers

```
client ~/.cache/leep/objects   (per machine, shared by all jobs on it)
   → server CachedBlobStore    (bounded LRU, per instance)
      → durable BlobStore      (S3 / MinIO / filesystem)
```
All three tiers are safe without invalidation, because keys are content
hashes. Planned: a regional cache tier, the same server binary in a
read-through mode against an upstream server. Strict tiering means a cold
regional cache cannot stampede the origin. The server's cache reports hits,
misses and occupancy as Prometheus metrics (see [OPERATIONS.md](OPERATIONS.md)).

## 8. Concurrency and consistency

- Only refs need coordination. `cas_ref` is a single conditional SQL statement
  (an `UPDATE … WHERE target = expected` or an `INSERT … ON CONFLICT DO
  NOTHING`), never a read followed by a write. Its ref log row is inserted in
  the same transaction, only when the statement changed a row.
- Changesets, blobs and derived values are written idempotently. Duplicate
  writes from racing servers are harmless.
- A blob may be orphaned if a client uploads it and never commits, and tree
  nodes or merge blobs may be orphaned if the changeset that would reference
  them fails to store. `leep-server gc` reclaims them in two fenced phases:
  it marks everything reachable from every stored changeset (not only refs, so
  unlanded changesets survive), *condemns* unmarked blobs older than a grace
  window (default 24h) by writing `blob-condemned-v1`, waits a fence (default
  10m, longer than the server's `--request-timeout`), re-marks, and deletes the
  blobs still condemned. A condemned blob is reported missing by
  `POST /v1/blobs/missing` and rejected by `POST /v1/changesets`, so clients
  re-upload it, and an upload rescues it; collect orders its delete against
  uploads with a two-flag handshake in meta. Grace and fence together make GC
  safe beside running servers, with no residual race. See ADR 0008 and
  ADR 0012.

## 9. Failure model

| Failure | Effect | Recovery |
|---|---|---|
| Server instance dies mid-request | No partial mutable state (only refs are mutable, by CAS) | Client retries; every write is idempotent |
| MetaStore unavailable | Writes and uncached reads fail | Fail fast with 500; no local fallback |
| BlobStore slow | Cache tiers absorb repeated reads | — |
| Land CAS contention | Bounded retries, then an error | Client retries `land` |

Observability: libraries emit `tracing` events and `metrics` facade calls
(no-ops without a recorder); only `leep-server` installs a recorder and
serves `/metrics` on a separate listener. Land retries, exhaustion and cache
hit ratio are the signals to watch; see [OPERATIONS.md](OPERATIONS.md).

## 10. Security

Authentication is static bearer tokens ([ADR 0007](adr/0007-token-authentication.md)).
`leep-server --tokens-file <PATH>` (or `LEEP_TOKENS_FILE`) loads a JSON file of
`{name, role, hash}` entries, where `hash` is `blake3:<hex>` of the token; the
file is read at startup only. Without it, auth is off and the server warns.
With it, an axum middleware requires `Authorization: Bearer <token>` on every
endpoint except `GET /v1/health`: 401 (with `WWW-Authenticate: Bearer`) when the
token is missing or unknown, 403 when a `read` token makes a mutating request.
`POST /v1/blobs/missing` counts as read because it only queries. Digests are
compared in constant time, and only the token name is logged. Use
`leep-server gen-token` and `hash-token` to create entries. Terminate TLS in
front of the server; bearer tokens are otherwise visible on the wire.

The CLI reads the token from `--token`, `LEEP_TOKEN`, or `.leep/config.json`.

**Signed changesets** ([ADR 0010](adr/0010-signed-changesets.md)). A changeset's
id covers its whole encoding, so a signature cannot live inside it. Ed25519
signatures over `b"leep-changeset-signature-v1\0" ‖ id` are stored as
attestations beside the changeset (derived kind `signature-v1`, one row per
changeset and key), and `Changeset` is unchanged. `leep-server
--signing-keys-file <PATH>` (or `LEEP_SIGNING_KEYS_FILE`) lists trusted keys as
`{name, public_key: "ed25519:<hex>"}`, read at startup only; signatures by
other keys are refused at `POST /v1/changesets/{id}/signatures` (write token),
and `GET` re-verifies what it lists. `--require-signatures <REF>` (repeatable,
`LEEP_REQUIRE_SIGNATURES`) makes `land` onto that ref fail with 400 unless every
changeset the land adds to the ref, including each unlanded stack entry and
merged side branch, is signed by a trusted key. The policy checks the
originals the client submitted; the rebased copies the server creates carry
no signature of their own and are linked to their originals by a server-written
`rebased-origin-v1` row, which `GET` reports as `via`. The hook is
`leep-derive`'s `LandPolicy`; the key handling lives in `leep-server`. `leep
keygen` and `leep commit --sign` are the client side. Raw ref CAS over HTTP is
gated the same way; the offline git bridge is not.

**Review service** ([ADR 0011](adr/0011-review-service.md)). Approvals are
attestations too (derived kind `review-v1`, one row per changeset and reviewer,
`Changeset` unchanged). The auth middleware hands handlers the token's
`Principal`; `POST /v1/changesets/{id}/reviews` records `approve` or `reject`
under the token's name, never a name from the body, and needs authentication.
Ownership is per directory: `OWNERS` files (one name per line, `#` comments,
`noparent`) in the tree, read from the **target ref's head** at check time so a
change cannot grant itself ownership; a path no file covers has no owners and
any one approval satisfies it. Per changed path, an owner's reject blocks,
then a merge-side match (`merge_parent`), an enabled auto rule from the
`--review-policy-file` (`comment_only`, `whitespace_only`, `docs_only`; pure
functions in `leep-derive`) or an owner's approval satisfies it.
`--require-review <REF>` (repeatable, `LEEP_REQUIRE_REVIEW`) and the policy
file's `refs` make `land` onto a ref fail with 400 `changeset X needs review:
path P ...` unless every changeset it adds is approved, checked on the
originals the client submitted, as for signatures. It is a second
`LandPolicy`: `Repo::with_land_policy` composes policies (all must pass), and
`check_land` now receives the head so owners and new history agree.
`GET .../review-status?ref=` shows the per-path state, `GET .../reviews`
lists verdicts (with `via` for rebased copies), and `leep review ...` and the
`leep_review_status` MCP tool are the clients.

Planned: OIDC or mTLS, per-path signing policy and key rotation, team groups
and self-approval rules for reviews. See the roadmap.

## 11. Scaling path

| Stage | Metadata | Blobs | Servers |
|---|---|---|---|
| Laptop / tests | SQLite or memory | filesystem or memory | 1 |
| Team | Postgres | MinIO / S3 | 2–3 behind a load balancer |
| Agent swarm | Postgres with read replicas for derived reads; partition `derived` by kind | S3 + regional cache tier | many stateless servers |
| Scion scale | Distributed SQL (CockroachDB/FoundationDB) behind the same `MetaStore` trait | S3 | many, multi-region |

Moving between stages means swapping a `MetaStore`/`BlobStore`
implementation. Nothing above the traits changes.
