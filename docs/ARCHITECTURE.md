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
| `leep-core` | Types, hashing, encoding, `BlobStore`/`MetaStore` traits (including listing and blob deletion for GC), in-memory stores, conformance tests, API types | no other leep crate; no I/O, no async runtime |
| `leep-store` | `BlobStore` backends: filesystem, `object_store` (S3/MinIO), `CachedBlobStore`, `open_blob_store(url)` | core |
| `leep-meta` | `MetaStore` backends: SQLite, Postgres, `open_meta_store(url)` | core |
| `leep-derive` | `Repo`: derived data framework and kinds, plus all operations and blob garbage collection (`gc`) | core (traits only, never a concrete backend) |
| `leep-server` | HTTP server binary `leep-server` | core, store, meta, derive |
| `leep-cli` | Client binary `leep` | core only |
| `leep-git` | Git bridge: library plus the `leep-git` admin binary | core, derive; store and meta only to open stores by URL in the binary |

Rules:
- Dependencies point toward `leep-core`. Nothing depends on `leep-server` or
  `leep-cli`.
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
| `last_merge-v1` | generation of the nearest merge on the first-parent chain (0 if none) | lets `merge_base` answer from skiplists when no merge sits above the first-parent common ancestor |
| `blame` | per (changeset, path), keyed `Hash::of_object("blame", id‖path)` | blame |

`root_tree` and `generation` are derived eagerly when a changeset is created.
Other kinds are derived on first read. Add new kinds with the
[`add-derived-data-kind`](../.claude/skills/add-derived-data-kind/SKILL.md)
skill. A new kind never requires a client change.

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
  NOTHING`), never a read followed by a write.
- Changesets, blobs and derived values are written idempotently. Duplicate
  writes from racing servers are harmless.
- A blob may be orphaned if a client uploads it and never commits, and tree
  nodes or merge blobs may be orphaned if the changeset that would reference
  them fails to store. `leep-server gc` reclaims them: it marks everything
  reachable from every stored changeset (not only refs, so unlanded changesets
  survive) and deletes unmarked blobs older than a grace window (default 24h).
  A client must create its changeset within the grace window of uploading, and
  that window is what makes GC safe beside running servers. See ADR 0008.

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

Planned: OIDC or mTLS, per-directory ACLs evaluated against changeset paths,
and signed changesets (author key in `extra`) for agent provenance. See the
roadmap.

## 11. Scaling path

| Stage | Metadata | Blobs | Servers |
|---|---|---|---|
| Laptop / tests | SQLite or memory | filesystem or memory | 1 |
| Team | Postgres | MinIO / S3 | 2–3 behind a load balancer |
| Agent swarm | Postgres with read replicas for derived reads; partition `derived` by kind | S3 + regional cache tier | many stateless servers |
| Scion scale | Distributed SQL (CockroachDB/FoundationDB) behind the same `MetaStore` trait | S3 | many, multi-region |

Moving between stages means swapping a `MetaStore`/`BlobStore`
implementation. Nothing above the traits changes.
