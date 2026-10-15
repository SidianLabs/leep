# Leep design

Leep is an open-source, server-centric version control system for repositories
where most clients are always-online agents and CI jobs. It follows the design
Anthropic described for its internal system Scion ("Redesigning source control
at Anthropic", Oct 2026) and borrows ideas from Meta's Mononoke and Zed's
DeltaDB. It is not trying to replace git for ordinary projects; it interoperates
with git through an import/export bridge.

## Principles

1. **The server is the center.** Commits, merges, rebases, diffs, blame and
   history run on the server. Clients fetch only what they read.
2. **Everything is immutable and content-addressed** except refs. Any cache
   at any tier can keep any object forever without invalidation.
3. **Avoid global mutable state.** Refs exist only for `main` and release
   branches. Work in progress is just changeset ids. A ref moves by
   compare-and-swap on a single row.
4. **Store the minimum, derive the rest.** A changeset lists only the files it
   changed. Full trees, generation numbers, ancestry indexes, blame and path
   history are *derived data*, computed recursively from parents and cached.
   New kinds of derived data can be added without touching clients.
5. **Write cost scales with the change, not the repository.** Deriving a
   changeset's tree rewrites only the directories on changed paths.
6. **No merge queue.** `land` rebases onto the current head on the server and
   retries the compare-and-swap. Testing happens after merge and before deploy.
7. **Agent provenance is first-class.** `Changeset.extra` carries `agent.*`
   keys (model, session, prompt, tests) and is part of the hash.

## Data model (`leep-core`)

| Object | Identity | Stored in |
|---|---|---|
| Blob (file bytes) | `Hash::of_blob(bytes)` = BLAKE3(bytes) | `BlobStore` |
| `TreeNode` (one directory) | `Hash::of_object("tree-v1", postcard)` | `BlobStore` via `put_with_hash` |
| `Changeset` | `Hash::of_object("changeset-v1", postcard)` | `MetaStore` |
| Ref | name → `ChangesetId` | `MetaStore`, CAS only |
| Derived value | `(kind, key hash)` → bytes | `MetaStore` |

A `Changeset` has `parents`, `author`, `timestamp`, `message`,
`changes: BTreeMap<RepoPath, FileChange>` relative to `parents[0]`, and
`extra`. `FileChange` is `Modify { content, kind, size }` or `Delete`.
`RepoPath` is normalized `/`-separated and validated on construction and
deserialization.

## Crates

| Crate | Role |
|---|---|
| `leep-core` | Types, hashing, `BlobStore` / `MetaStore` traits, in-memory stores, store conformance tests, HTTP API types (`api`) |
| `leep-store` | `BlobStore` backends: local filesystem, S3/MinIO (via `object_store`), and `CachedBlobStore` (bounded LRU in front of any store) |
| `leep-meta` | `MetaStore` backends: SQLite (single node, tests) and Postgres (production) |
| `leep-derive` | Derived data framework and kinds (root tree, generation, ancestry skip list, path history, blame) plus repository operations: diff, merge-base, three-way merge, rebase, land |
| `leep-server` | Stateless HTTP/JSON server (axum) exposing `leep_core::api` |
| `leep-cli` | `leep` binary: lazy checkouts, status, commit, land, log, diff, blame, cat; shared local object cache |
| `leep-git` | Import a git repository's first-parent history into Leep; export Leep history to git |

## Derived data (`leep-derive`)

Each kind implements one recursive rule: value(changeset) = f(changeset,
values of its parents). The framework walks parents iteratively (never
recursion on the call stack, histories are deep), derives missing values
oldest-first, and stores each under `(kind, changeset id)` in `MetaStore`.

- `root_tree`: apply `changes` to `parents[0]`'s root tree, rewriting only
  the directories on changed paths. Empty directories are pruned.
- `generation`: `1 + max(parent generations)`.
- `skiplist`: ancestor pointers at power-of-two distances on the first-parent
  chain, for fast ancestry and merge-base queries.
- `path_history` and `blame`: computed on demand per (changeset, path) and
  cached by `Hash::of_object(kind, changeset ‖ path)`.

## Concurrency and landing

`land(cs, ref)`: read head H. If `cs`'s first parent is H, CAS ref H → cs.
Otherwise rebase `cs` onto H (three-way merge of base = cs.parents[0],
ours = H, theirs = cs). Conflicts are returned, never silently resolved.
On success CAS H → rebased; if the CAS loses, reread and retry (bounded).

## Caching tiers

client object cache (per machine, `~/.cache/leep/objects`) → server
`CachedBlobStore` (LRU) → durable store (S3/filesystem). Because objects are
content-addressed, every tier is safe without invalidation. A regional cache
tier is the same server binary pointed at an upstream server (future work).

## Out of scope for v0.1 (planned)

FUSE virtual filesystem (the CLI does lazy sparse materialization instead),
regional cache servers, auth and per-directory ACLs, the review tool with
semantic approval rules, and DeltaDB-style live CRDT edit streams.
