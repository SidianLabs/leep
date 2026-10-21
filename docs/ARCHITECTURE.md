# Leep architecture

Leep is an open-source, server-centric version control system for
repositories where most clients are always-online AI agents and CI jobs. It
follows the design Anthropic published for its internal system Scion
("Redesigning source control at Anthropic", Oct 2026), borrows from Meta's
Mononoke (bonsai changesets, derived data) and adds Zed DeltaDB's idea of
recording agent provenance with every change.

Leep does not try to replace git for ordinary projects. It targets
repositories with very high merge rates, and interoperates with git through a
bridge: offline import and export, and a live two-way mirror that keeps a git
remote such as GitHub in step with a server ([ADR 0046](adr/0046-live-git-mirror.md)).

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

 leep-git mirror run ── HTTP/JSON (v1) ──► leep-server
        └── system git (https / ssh) ──► git remote (GitHub, GitLab, ...)
```

Any number of server instances can run behind a load balancer. They share
nothing except the two stores. Correctness depends only on (a)
content-addressing and (b) the atomicity of `MetaStore::cas_ref`.

## 3. Crates and dependency rules

| Crate | Role | May depend on |
|---|---|---|
| `leep-core` | Types, hashing, encoding, `BlobStore`/`MetaStore` traits (including listing and blob deletion for GC, a changeset id prefix lookup for short ids, listing and dropping derived rows by kind for repair, batched changeset and derived reads for history walks, and batched derived writes and changeset-with-rows writes for changeset creation), in-memory stores, conformance tests, API types, pure rename detection | no other leep crate; no I/O, no async runtime |
| `leep-store` | `BlobStore` backends: filesystem (loose objects plus delta-compressed packs and the repack writer, ADR 0029, ADR 0039), `object_store` (S3/MinIO; loose objects plus the same packs, ADR 0040), `CachedBlobStore`, `open_blob_store(url)`, and the incremental blob and pack copy backup and restore use (`sync`, `packfiles`, ADR 0026, ADR 0039) | core |
| `leep-meta` | `MetaStore` backends: SQLite, Postgres, `open_meta_store(url)`, the versioned schema migrator (`migrate`, ADR 0023), metadata snapshots and their import (`snapshot`, ADR 0026), and `analyze_meta_store`, which bulk loaders call to refresh Postgres planner statistics (ADR 0042) | core |
| `leep-derive` | `Repo`: derived data framework and kinds, plus all operations, blob garbage collection (`gc`), the delta-base walks for `repack` (`pack_order`, full or loose-only) and store verification and repair (`integrity`) | core (traits only, never a concrete backend) |
| `leep-server` | HTTP server binary `leep-server`, and its offline subcommands (`gc`, `migrate`, `verify`, `repair`, `backup`, `restore`, `backup-verify`) | core, store, meta, derive |
| `leep-cli` | Client binary `leep` | core only |
| `leep-mcp` | MCP server binary `leep-mcp` for AI agents (stdio JSON-RPC; talks to `leep-server` over HTTP) | core and the `leep-cli` library (its HTTP client); never derive, store, meta or server outside dev-dependencies |
| `leep-git` | Git bridge: library plus the `leep-git` admin binary (offline `import` and `export`, which open the stores; the `mirror` daemon, which uses only the HTTP API and the system `git`) | core, derive, and the `leep-cli` library (its HTTP client, for the mirror); store and meta only to open stores by URL in the binary |
| `leep-fuzz-targets` | Test-only (`publish = false`): fuzz harness functions and the seed corpus loader for every decoder and parser (`docs/FUZZING.md`) | core, derive, server (to drive the real router and config parsers); nothing depends on it except the `fuzz/` crate |
| `leep-testkit` | Fault-injecting `BlobStore`/`MetaStore` wrappers and a seeded fault plan, for tests (ADR 0021) | core; a dev-dependency only, never a normal dependency of any crate |
| `leep-soak` | Soak test binary (`publish = false`): an agent swarm against a real `leep-server`, invariant checks, GC and restarts beside it, a trend report (ADR 0022). Starts the `leep` and `leep-server` binaries; talks to the server over HTTP only | core only (types); nothing depends on it |

Rules:
- Dependencies point toward `leep-core`. Nothing depends on `leep-server`
  except the test-only `leep-fuzz-targets`, and only `leep-mcp` and
  `leep-git` (for the mirror) depend on `leep-cli`, for its client library.
- `leep-derive` must stay backend-agnostic. It is tested against
  `leep_core::memory`.
- Every `BlobStore`/`MetaStore` backend must pass
  `leep_core::memory::check_*_contract`, also through a quiet
  `leep-testkit` wrapper.

## 4. Data model

| Object | Identity | Stored in |
|---|---|---|
| Blob (file bytes) | `BLAKE3(bytes)` | `BlobStore::put` |
| `TreeNode` (one directory) | `Hash::of_object("tree-v1", postcard)` | `BlobStore::put_with_hash` |
| `Changeset` | `Hash::of_object("changeset-v1", postcard)` | `MetaStore::put_changeset` |
| Ref | name → `ChangesetId` | `MetaStore`, CAS only |
| Ref log entry | `seq` → (ref, old, new, time, actor, operation, message) | `MetaStore`, appended by `cas_ref_logged` (or `cas_ref_logged_chain`, one entry per step) in the CAS's own transaction |
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
- the resulting tree never has a path that is both a file and a directory;
- every path is at most 4096 bytes, and all paths together name at most a
  million directories (256 MiB of directory names), since building the tree
  holds each changed directory and its full path in memory.

A blob may not be the hash preimage of an object (bytes starting `leep:`
with a NUL after it): it would share a tree node's address in the
`BlobStore`, so `Repo::put_blob` refuses it with `Error::Invalid`
([ADR 0041](adr/0041-blob-and-object-address-separation.md)).

`RepoPath` is validated on construction and on deserialization, so invalid
paths cannot enter the system through any route.

**Stored changeset rows** ([ADR 0053](adr/0053-compressed-changeset-rows.md)).
The SQL backends store a changeset either as its canonical bytes or as a
zstd frame of those bytes compressed with a dictionary trained on the
store's own history (`changeset_dicts`, named by the frame's dictionary
id). The encoding lives entirely inside `leep-meta`: `MetaStore` takes and
returns `Changeset` values, readers check every decoded row against its id
as before, and ids, hashes and the API are unchanged. A server writes with
the newest dictionary it knows (it looks again on a read that names an
unknown one and at most a minute before a write), and plain rows until a
dictionary exists. `leep-server compact-changesets` trains the dictionary
and rewrites existing rows in place, beside running servers of this
version; on git/git and Linux it halves the table (about 2.9 to 1.75 GB on
all of Linux). A frame header's claimed size is trusted for at most 1 MiB
of memory up front; larger frames decode as a stream.

**Format features** ([ADR 0055](adr/0055-store-format-features.md)). The
`store_features` table names what a store's rows use that a reader must
understand (`changeset-zstd-dict` so far). A tool records a feature before
it writes the first row that needs it (`compact-changesets` does so before
it stores a dictionary). Every binary that has the table reads it on open
and refuses to start on a name it does not know, so a newer format fails
at startup rather than on the first row it cannot read. Binaries from
before the table do not check it.

## 5. Derived data

A derived kind is a pure function
`value(cs) = f(cs, value(parent₁), …, value(parentₙ))`. The framework:

1. finds ancestors without a stored value by walking parents **iteratively**
   (histories can be hundreds of thousands deep), checking and reading 256 per
   round trip and keeping only their parents, so memory follows the number of
   changesets and not their size;
2. derives them parents first, several at a time (ADR 0050, below),
   reading changesets again 256 at a time;
3. stores each with `put_derived(kind, cs_id, bytes)`; a kind may instead
   stage its rows and write a window's in one `put_derived_many`, each
   changeset's value after the rows it names (`history-tree-v4`,
   `log-filter-v1` and `last_merge-v1` do; values held back are found by
   later derivations in the same run, as a merge asks for its merge base's).

Because inputs are immutable, a value is computed at most once per kind and
changeset, and concurrent servers that race produce identical bytes.

**Parallel derivation** ([ADR 0050](adr/0050-parallel-derivation.md)). A
changeset whose parents all have their rows depends on nothing still being
computed, so `derived::wavefront` runs a missing range as a wavefront: the
range in a parents-first order with its links as positions (about 50 bytes
a changeset), a heap of ready changesets by position, and up to `jobs`
derivations at once as Tokio tasks, lowest position first (one job is the
serial order). Results return to one coordinator in the order changesets
finish, which lists parents before children; staged rows join a buffer in
that order and go to `put_derived_many` a window at a time, one write in
flight while derivation goes on, so any prefix stored is whole changesets
after their parents and a stopped run resumes cleanly. Changesets are read,
and the kind's `prepare` runs, a window ahead of the work, at most
`max(8, 4 x jobs)` windows past the oldest unfinished changeset. Tasks
inherit the derivation's held-lock list and skiplist reads. Rows are the
same bytes for any number of jobs. `Repo::with_derive_jobs` sets the number
(default: the cores, up to 8; `leep-server --derive-jobs`), and `repair
rederive --jobs` runs its recompute-compare-write per changeset the same
way.

| Kind | Value | Used by |
|---|---|---|
| `root_tree` | 32-byte tree hash. Applies `changes` to `parents[0]`'s tree, rewriting only the directories on changed paths and pruning empty ones | everything |
| `generation` | `1 + max(parents)`; roots = 1 | log ordering, ancestry |
| `skiplist` | power-of-two first-parent ancestors | `is_ancestor`, `merge_base` |
| `signature-v1` / `rebased-origin-v1` | attestations and server rebase links, keyed per (changeset, key) and per copy; not pure functions of the changeset, so written by `put_signature` and `rebase` rather than derived | signed-changeset listing and the land policy |
| `review-v1` | review verdicts, keyed per (changeset, reviewer name) (`Hash::of_object("review", id‖name)`); attestations written by `put_review`, last write wins, listed with one lookup per reviewer (write tokens without `"review": false`) | review listing and the review land policy |
| `last_merge-v1` | generation of the nearest merge on the first-parent chain (0 if none) | lets `merge_base` answer from skiplists when no merge sits above the first-parent common ancestor |
| `blob-claim-v1` / `blob-condemned-v1` / `blob-rescue-v1` / `blob-doom-v1` | per blob hash, 8-byte big-endian unix seconds (doom adds a state byte); GC bookkeeping, not derived from changesets: claims by uploads and dedup checks, condemnation by `gc condemn`, rescue by uploads of a condemned blob, doom by `gc collect` (ADR 0012) | blob garbage collection and the server's missing-blob checks |
| `history-tree-v4` | the changeset's history pack (ADR 0045): the nodes of a tree parallel to `root_tree` recording for each path (including deleted ones) the newest changesets that changed it, as written by this changeset's derivation, and the address of its root. A pack is cut into parts of about 16 KB; this value is part 0 and `history-node-v4` rows hold the rest. Nodes are addressed by (writer, part, index) and name changesets through a table per part, so the hundreds of nodes a merge rewrites name its few dozen ids once; nodes of more than 64 entries are stored as content-defined chunks so an edit rewrites one chunk (ADR 0027), and a changed node, chunk or chunk list is stored as a patch on an earlier whole version that names the entries it replaces by position (ADR 0037). Built from the first parent's tree plus the spines of changed paths, and at merges the paths where the second parent's history differs from the merge base's (another parent's from the first's, for octopus or unrelated merges) (ADR 0013) | path log, `last-change` |
| `log-filter-v1` | per changeset at first-parent depth `d`: for each level `j ≤ tz(d)`, the 64-bit fingerprints of the authors, `extra` keys and `agent.model`/`agent.session` values of everything reachable from it but not from its first-parent ancestor `2^j` up (side branches included); lower-cased author names by fingerprint in `log-filter-name-v1` (ADR 0047) | `log` with `author`/`extra` filters and no path |
| `blame` | per (changeset, path), keyed `Hash::of_object("blame", id‖path)` | blame |
| `cs-condemned-v1` / `cs-rescue-v1` / `cs-doom-v1` / `gc-fence-active-v1` | per changeset id (the flag: one row), the encodings of their `blob-*-v1` counterparts; the fence of changeset GC (ADR 0059): rescues by every writer that names a condemned changeset; condemnation, doom and the flag by the sweep (phase 2, not built yet) | changeset garbage collection, and the rescue checks of creates, lands, ref moves, rebases, merges, attestations and mirror maps |
| `cs-reach-v1` | per changeset id, one byte: "a ref or ref log entry reaches it, and every parent has a row"; written parents first, plus a progress row at the zero key (ADR 0059 section 4) | the changeset sweep's incremental reachability mark |
| `gc-mark-v1` | per changeset id, tree node hash or blob hash, one byte: "referenced by a stored changeset, and everything below has a row"; written by GC marks in post-order (ADR 0033) | incremental GC marks, which skip covered changesets and stop at marked tree nodes |
| `blob-trigrams-v1` | per blob hash: sorted trigram set of a UTF-8 text blob up to 1 MiB (delta varints), or a "not indexed" marker (ADR 0014) | code search |
| `tree-trigrams-v1` | per tree node hash: fixed-size bloom filter (2^19 bits) over every trigram below the directory, the OR of its children's filters; shared by every revision with the same subtree (ADR 0014) | code search directory pruning |

`root_tree`, `generation`, `skiplist` and `last_merge-v1` are computed
eagerly when a changeset is created and written with it in one call (ADR
0031, see "Commit" below; `last_merge-v1` from the parents' rows only, never
by deriving, and skipped when they lack one, ADR 0058).
Other kinds are derived on first read (the search kinds on the first search
that needs them; see "Code search"), and every server also warms them in the
background for each ref head that moved (ADR 0028), so queries on an imported
or restarted history do not pay for the whole history. The warmer tails the
ref log every `--warm-interval` (250 ms; `seq` is assigned in commit order, so
the tail misses nothing), lists every ref every 5 s as a backstop, and
derives all moved heads of a pass as one range per kind
(`derived::derive_heads`), so shared history is derived once and different
refs' new changesets derive side by side (ADR 0058, phase 0).
`derive` takes a per-kind lock once a value is missing, so concurrent requests
and the warmer never derive the same range twice.

**Who builds indexes (ADR 0058).** `--indexing local` (the default) is the
above, on every server. With `--indexing lease`, each index group (path
history, log filter, code search) has a lease in the `index` scope of the
`leases` table (repository 0, the group's primary kind as name, shard 0;
`leep_derive::indexing::IndexLeases`), and only its holder warms that group;
the holder checks the lease before each staged write and stops a derivation
once it is lost, so a stale holder wastes work but never writes a wrong row.
`--indexing off` never warms. In `lease` and `off` modes queries never derive
an index while a lease lives (`indexing::bounded`): rows are parent-closed,
so a query's un-indexed part is the changesets above a set of indexed
ancestors. It finds that part (stopping at a budget) and runs on a handle
that knows it: the path walk examines an un-indexed changeset the way
`log_scan` does and queues its parents, then settles its queue so the page's
`next` is the indexed walk's; last-change computes the newest changes bottom
up over the part; blame walks un-indexed first-parent steps one by one;
the filtered log scans the part; search computes missing trigram rows in
memory. A request over the budget gets `Error::Indexing` (503) with
`Leep-Indexing: retry`, an answer at the newest indexed first-parent
ancestor with `stale-ok`, and without the header waits, scans more, then
gets the 503. With no live lease queries derive inline. `repair drop`,
`rederive --missing` and `upgrade-index --yes` take the leases and wait a
fence before touching rows (`ToolLease`). Add new kinds with the
[`add-derived-data-kind`](../.claude/skills/add-derived-data-kind/SKILL.md)
skill. A new kind never requires a client change.

**Log without a path.** `log` with no path and `provenance` walk all ancestors on a heap ordered by
generation and id. The walk reads ahead in batches (ADR 0030): when it needs a
changeset it has not read, it reads up to 256 that it expects to reach next,
with their generations, root trees and skiplists, through
`MetaStore::get_changesets` and `get_derived_many`, issued together. Its
guesses are the parents of what it has read and the skiplist entries within
the batch size, so a long first-parent chain is read a window per round trip
rather than one changeset. A walk that expects to visit more than one batch
reads two at once, on separate tasks. An unfiltered log reads no further than
its limit. A long log's JSON is encoded in chunks of 512 entries on blocking
threads and streamed in order once the walk has finished, so a failed walk
still answers with an error status.
The read-ahead never changes the answer: the heap, the `seen` set and the
order are those of the plain walk, and whatever a batch did not return is read
singly at the point the plain walk would read it.

**Path history.** `log --path P` and `GET /v1/last-change/{id}?path=P` read
the `history-tree-v4` index instead of scanning ancestors (ADR 0013). A lookup
walks `P`'s components through the changeset's history tree, reading only the
chunk that holds each component in a large directory (and the keyframe a
patch is on), and yields the
newest changesets that changed `P` (for a directory, anything under it). The
log pushes those on a heap ordered by generation and id, yields the newest,
looks `P` up in that changeset's parents to find the next ones, and repeats. It
is iterative and returns exactly what the old scan returned, in the same order,
reading only changesets that changed the path. A merge's entry is the merge
itself if it changed the path relative to its first parent, otherwise the union
of its parents' entries, so a side branch's changes are never lost. The index
is derived oldest first, by the warmer or by the first path query, whichever
comes first. `author` and
`extra` filters apply to the changesets the walk yields, so a selective filter
on a frequently changed path still reads each of them. In a plain path log
(not `follow`, which still reads each changeset to look for renames) without
a filter, the walk reads a changeset only when the ancestry cache lacks its
parents or a bounded query must examine it, and the page's changesets and
root trees are read afterwards in one batch each, so a warm path log makes
no round trip per entry.
Directory nodes keep deleted entries, so a busy directory's node grows with
every name it ever held. Nodes of more than 64 entries are therefore stored as
chunks of about 32 entries whose boundaries depend only on the names (ADR
0027): a changeset rewrites the chunk holding each changed entry and a short
chunk list, and shares the rest with its parent. A rewritten chunk would
still repeat its unchanged entries, so a changed node, chunk or list is
stored as a patch (ADR 0037): a row naming a keyframe, an earlier whole
version, and holding only the entries that differ from it. A new keyframe
starts once the patch grows past about `sqrt(128 × keyframe size)` bytes.
Patches point only at keyframes, so a row costs at most two reads, and a
lookup of a recently changed name is answered from the patch alone. Rows
depend only on the content and the first parent's stored version, so every
server derives identical bytes. Each `Repo` keeps a cache of decoded
node rows (96 MiB by default; all of `Repo`'s derive caches scale with
`leep-server --derive-cache-bytes`), so derivation does not re-read the chunks of the root and other
hot directories for every changeset.

**Blame.** Blame follows first parents and credits each line to the
changeset whose diff against its first parent inserted it; the result is
cached under `blame` for the changeset and for the newest changer. The walk
asks the path-history index where the next change is: the newest changers'
largest generation `g` says that no first-parent ancestor above `g` changed
the file, and skiplists find the lowest one at or above it. Every skiplist
read during a walk names first-parent ancestors 1, 2, 4, ... steps further
down, so the walk keeps them by generation and starts each descent from
the nearest known point, and keeps their generations, which spares a read
for changers on the chain; a step reads the index entry and the skiplist of
the changeset it moves to together. The walk reads no files itself: the
files at the examined changesets' parents, which decide whether each one
changed the file and where its history starts, are read 64 steps at a time
(root trees in one batched metadata read, each directory level of the path
in one `get_many`), and cached blames are looked up 32 changers at a time.

Each step needs the one before it, so a file changed thousands of times
costs thousands of round trips in a row. The first 32 steps are walked
from the tip, which is all that most files (changed rarely) and a cached
blame near the tip need. Meanwhile the first-parent chain is cut into
**stretches** of about 1/256 of the tip's generation (at least 32
generations, shorter near the tip), the cuts found a level of skiplists at
a time (each round one batched read of the lists of cuts above a gap still
too wide). If the walk goes on, the stretches below where it stopped are
walked at once, up to 64, each from its top, as separate tasks; their reads
are gathered into batched reads (`batched_reads`: single-row and batched
derived reads that arrive while a batch is out go in the next
`get_derived_many`, blob batches in the next `get_many`), so the stretches
advance a step per round trip together. A stretch does not know the file
at its top, which is where the stretch above ends, so its first step's
cached blame is looked up in case it changed the file; the steps are then
taken in order, newest first, each stretch's first step judged against the
file the one above ended with, down to the first step that ends the walk
(the file's creation, the chain's root or a cached blame), and the walks
below it are dropped. A stretch whose top has no index entry for the path
lies below the file's creation and stops at once. The steps are exactly
those of one walk, so the answer is too, and a file untouched for most of
history costs a few reads per stretch rather than any per changeset.

The versions are read 64 (and at most 32 MiB) at a time, each window while
the one before is diffed on several threads. Each diff is the one
`similar::TextDiff::diff_lines` makes, with each version split into lines
and its lines numbered once per task rather than twice per diff (Myers
compares lines only for equality, so the numbers give the same diff), and
lines carry only (changeset, line) until the last version gives them their
text. The changeset summaries are one `get_changesets`.

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
queryable without any new data on changesets. `GET /v1/log` takes `author`
and repeated `extra=key:value` / `extra=key` filters (ANDed), `GET
/v1/provenance` counts changesets per agent model and session and per human
author, and blame responses carry a deduplicated per-changeset summary
(author, time, first message line, `agent.*` keys). The summaries are
attached on read, so the cached `blame` value keeps its original encoding.
Provenance scans the newest changesets (capped at 10 000, reporting
`truncated`); it is an aggregate, so it has no cursor.

**Page cursors.** `GET /v1/log`, `/v1/search` and `/v1/reflog` answer one
capped page and, when more remains, an opaque `next` cursor that the client
sends back as `after` (`leep_derive::cursor`, layout in FORMATS.md). A log
cursor carries the walk's queue: every log walk (plain, path index, `follow`,
and the `log-filter-v1` walk with its merge-side bounds) pops in descending
`(generation, id)` and queues only ancestors, so seeding a fresh walk with
the queue continues it exactly, and pages cost what one long walk costs. A
queue wider than 64 entries falls back to "restart from the tip below the
last entry", which is correct but rescans. A search cursor pins the searched
changeset and the last (path, line), and the walk skips directories and files
before it. Cursors are bound to their query by a fingerprint and validated as
untrusted input (400 on any failure). `fields=summary` returns the same log entries
as listings need them (no changed paths, the message's first line,
`agent.*` metadata), projected after the walk, so cursors and order are
shared with full pages and either shape continues the other; `leep log
--oneline` and the MCP `leep_log` tool ask for it.

**Filtered log** ([ADR 0047](adr/0047-log-filter-index.md)). A log with
`author` or `extra` filters and no path scans its first 512 changesets
plainly, longer while at least one in 150 matches (frequent values are found
there), then continues the same walk with
the `log-filter-v1` index. Each changeset stores fingerprint sets for
first-parent stretches of `2^j` steps, `j` up to its depth's trailing zero
bits, covering everything those stretches reach, side branches included.
Before expanding a changeset the walk reads its row; if a level rules out a
match it queues the first-parent ancestor at that level's far end instead of
the parents. Every remaining match is still below something queued, so the
heap yields exactly the scan's matches in the scan's order, while reading
only changesets near matches and a row per skipped stretch. An author
substring becomes the set of author fingerprints whose name (in
`log-filter-name-v1`) contains it, found from the head's widest levels; a
filter on an `extra` value outside `agent.model`/`agent.session` uses the
key's presence. A merge's level 0 holds exactly what its other parents bring
in, found by one walk of both parents' histories newest first
([ADR 0051](adr/0051-log-filter-merge-walk.md)), as `git rev-list
first..side` does, so a back-merge that brings in a release with thousands
of merges costs one walk, not a `merge_base` per inner merge. Past 200,000
changesets the walk gives up and stores a superset from the sides' own
levels, bounding every merge. The row
also names per side the first changeset on its first-parent chain that the
first parent reaches, its *stop*, and the walk
into a side ends there, leaving the rest to the first parent's walk; without
it, the walk into a branch that merges mainline back would follow the branch
to its root. Rows, skiplists and changesets are read a batch at a time for
the changesets nearest the top of the heap. The warmer derives the kind for
ref heads; otherwise the first filtered query past the plain scan does.

**Code search** ([ADR 0014](adr/0014-code-search.md)). `GET /v1/search/{rev}`
greps a revision on the server: a literal or regex (`regex` crate), optionally
case-insensitive, restricted by path prefix and glob, matched per line. The
planner turns the query into trigrams every matching file must contain
(literal trigrams, case variants, or literals extracted from the regex syntax
tree; none means a plain scan). Execution walks the tree depth first with an
explicit stack, skipping directories whose `tree-trigrams-v1` filter lacks a
required trigram and files whose `blob-trigrams-v1` set does, then fetches the
survivors (16 at a time, order preserved) and runs the matcher on blocking
threads. A directory's children's rows and surviving nodes are read in batches
when it is opened, and the subdirectories of the directory the walk is in are
opened ahead of it, four at a time. Output is ordered
by path then line and is identical with or without the index, which a property
test checks; the walk is lazy, so a search that has enough matches stops early.
Limits: 100 matches by default, 1000 at most, 256 MiB of blob content scanned,
`truncated` when either is hit. Only UTF-8 text blobs up to 1 MiB are searched;
others count as `files_skipped`. The index rows are derived lazily by the first
search that needs them, or by the warmer for ref heads (ADR 0028), and are
keyed by content, so a new revision derives only what it changed;
`leep-server derive-search <rev>` warms any other revision ahead of time. Clients are `leep search` and the `leep_search` MCP tool.

**Verification and repair** ([ADR 0025](adr/0025-repair-tools.md)).
`leep-server verify` and `leep-server repair` run offline against the stores
(`leep_derive::integrity`) and are the answer to data damaged at rest, which
the failure model below does not cover. `verify` walks history oldest first
from the refs (or every stored changeset with `--full`), checks that every
changeset, tree node and blob a changeset names exists and hashes to its
address (`--rehash` reads blob bytes), replays the ref log against the refs,
and compares each derived row with what the code that derives it computes from
the changeset and its parents' rows. Because parents are checked first, a
damaged row is reported where it is and later rows are judged against the true
value, so one flipped bit is one finding. The recomputation runs on in-memory
shadows of the stores, so `verify` cannot write. `repair rederive` runs the
same recomputation against the real stores and writes the rows that differ
(derivation also rewrites the tree and history nodes it derives, which restores
missing ones), several changesets at a time once their parents are done
(ADR 0050); `repair rederive --missing` only derives absent rows, through
`derive`, so a stopped initial build resumes where it was; `repair drop`
deletes a kind so it derives again on demand, through
`MetaStore::delete_derived_step` ([ADR 0052](adr/0052-fast-derived-kind-drop.md)):
a kind that is a large share of the table is deleted by walking the table in
physical order, a range of pages per short transaction, so each page is read
and changed once; a small kind in key-order batches. Its dry run counts with
`count_derived`, which reads no values. `leep-server upgrade-index`
(`integrity::upgrade`) composes these for an upgrade: it compares each
current kind's count (`estimate_derived`, which on Postgres estimates a
large kind from planner statistics, counting exactly only where a decision
depends on it) and ref heads' rows with the reachable history, derives
what is missing through `derive` from the heads (rows are stored after their
parents', so that finds every gap but one a stopped drop left), and drops
retired kinds only when told no older server reads them.
Only kinds that history can regenerate are rederived or dropped; signatures,
reviews, rebase links, GC fence rows and the ref log are refused. The table of
kinds is `leep_derive::integrity::kinds`, mirrored in the ADR.

**Backup and restore** ([ADR 0026](adr/0026-backup-and-restore.md)).
`leep-server backup` takes a consistent copy of both stores while servers and
GC run. It reads the metadata first, in one read transaction
(`leep_meta::snapshot`: `REPEATABLE READ` on Postgres, a WAL read transaction
on SQLite), and writes it as a SQLite file with the normal schema, whichever
backend it came from. It then copies the blobs the backup pool lacks
(`leep_store::sync`: a merge of two ascending listings; content addressing
makes "present" mean "correct"), and finally walks every changeset of the
snapshot against the pool and seals the result in `manifest.json`. The order
is the consistency argument: a blob a stored changeset needs is never deleted
(ADR 0012), so everything the snapshot needs is in the store when the copy
looks, however GC interleaves; the walk enforces it on every run, so no lock
against GC is needed. `restore` loads blobs, then metadata in one transaction,
into empty stores only, refusing a newer schema and a damaged file, and the
restored stores pass `verify`. Compressed changeset rows and their
dictionaries are copied byte for byte. `backup-verify` checks a backup without
restoring it. The format is in FORMATS.md; the drill and the runbook are in
OPERATIONS.md.

## 6. Request flows

**Commit** (`leep commit`):
1. The client hashes changed files and calls `POST /v1/blobs/missing`.
2. It uploads only the blobs the server lacks, with `PUT /v1/blobs/{hash}`.
3. It sends `POST /v1/changesets`. The server validates the changeset, derives
   its tree and returns the id.
Nothing mutable has changed yet; the changeset is just an addressable object.

Storing a changeset (ADR 0031) costs one round of reads and one write. The
server reads, concurrently and in batches, whether the parents exist, their
generations, the first parent's root tree and skiplist, and the contents'
`blob-condemned-v1` rows. It checks parents and blobs, writes the rewritten
tree nodes (then reads their condemnation rows in one batch), computes the
generation and skiplist with the derived kinds' own code, and stores the
changeset with its `root_tree`, `generation` and `skiplist` rows through
`MetaStore::put_changeset_with_derived`: one statement on Postgres, one
transaction on SQLite, so the changeset never appears without them. A rebase
adds its `rebased-origin-v1` link and the git importer its commit mapping to
that write. Building a skiplist needs the lists of the first-parent ancestors
1, 2, 4, ... steps up, one after another; `Repo` caches the lists it computes
and those it reads while creating changesets (never rows read by `verify` or
`repair`, which substitute recomputed rows as they go). Whatever the batched
round does not supply is read or derived singly, as before.

**Land** (`leep land`, the no-merge-queue path), group committed per ref
within each server process ([ADR 0032](adr/0032-group-commit-for-lands.md)),
with the batch's writes staged in memory ([ADR 0043](adr/0043-staged-land-batches.md)):
```
land(cs):  check name/tag/cs; enqueue on queue(target); start a leader if none; await answer

leader(target):  while queue not empty: land_batch(take ≤64 queued, in arrival order)

land_batch:
loop (≤32 attempts):
  start ← get_ref(target);  head ← start;  staging ← empty
  prefetch the lands' changesets, root trees and the head's rebase links
           (stored rebase links an earlier attempt of the batch read are kept),
    and what target's ref log recorded for the lands' land ids
  for each land, writing into staging and reading it back (dropped if its caller left before it was reached):
    if its land id is recorded (or moved earlier in this batch):
                                     that land's result (400 if the id named another cs)
    elif head is None:               move to cs
    elif cs is an ancestor of head:  already landed
    elif head is an ancestor of cs:  move to cs                   # fast-forward
    elif head's history holds a server-made copy of cs:  already landed
    else:
      stack ← first-parent chain from cs down to where it joins head's history,
              or to an entry whose server-made copy is in it
      replay each stack entry onto head, oldest first        # 3-way per entry
      conflict? → Conflicts{onto: head, conflicts, clean} for this land only
                  (the whole stack squashed into one 3-way merge vs head;
                   never auto-resolved on the server)
      move to replayed top
    a move sets head; answers that precede every move are sent at once
  no moves → done (staging dropped)
  store staging: blobs (one wave, GC rescue check, flush), then changesets
                 and rows (one atomic write); an error counts as a CAS error below
  CAS(start → head) logging one entry per move (cas_ref_logged_chain)
  CAS won → every land gets its answer;  CAS lost → jittered backoff, redo batch
  CAS errored → it may have applied: the next pass re-reads the head and
                finds each land's copy, or tries again (3 errors in a row → the error)
attempts exhausted → 409 Conflict for every land left (client retries)
```
Each land's answer is the one it would get if the batch's lands ran one after
another, and the ref log reads the same way: one entry per land that moved the
ref, with its own actor, chained from the batch's starting head, written in
the same transaction as the single ref update. Batching is what removed the
cost of contention: lands on one ref in one process never race the CAS (they
were serialized by a per-ref lock before), but each paid a ref read, a commit
and a landed-copy scan as long as the number of agents ahead of it; a batch
pays the read and commit once and its scans share the rows they read. Its
replays write into a staging layer (`land_stage.rs`: a `BlobStore` and a
`MetaStore` over the real ones that keep writes in memory and serve them back),
so a replay costs memory reads and CPU instead of a tree fsync wave and a
transaction; the batch then stores everything with one wave of blob writes and
one `put_changesets_with_derived` before the swap, in the order a lone write
keeps, so the ref never names an object that is not durable. The CAS
still coordinates separate server processes; a lost swap redoes the batch, and
jittered backoff keeps processes from colliding in lockstep. That is not
enough under load: with several servers on one ref one instance wins every
swap and the others' lands starve (`docs/PERFORMANCE.md`, "Several servers on
one ref"). With `--land-coordination on`
([ADR 0056](adr/0056-land-coordinator-per-ref.md), `land_coord.rs`) each ref
has one coordinating server, the holder of its lease in the metadata store,
to which the others forward their lands; the CAS stays the only thing
correctness rests on:
```
land_coordinated(cs, ref):  loop until answered (at most 30 s, then 409):
  route(ref):
    this server holds a lease whose view is valid (or renews now) → batch it here (as above);
      a batch that finds the lease gone before an attempt hands its lands back → route again
    another server holds a live lease → forward the client's body and token to its address;
      no answer → ask again after 500 ms, or ask the next holder once the lease changes;
      never batch it here while that lease is live
    no live lease → take it (one conditional upsert) → batch it here
    store unreadable → forward on the last lease read while it is still live;
      none known and none can be taken → batch it here, uncoordinated
    shutting down and no other live lease → 503, the client sends it again
```
A lease lives 2 s (the TTL) from its last renewal, and only then may another
server take it; a holder trusts it until the store-reported time left, less
one renew interval (500 ms), counted from when it sent that renewal. Lease
calls use connections of their own, so a renewal never waits behind data
traffic. A forwarded land is never forwarded again. The leader of a batch is a
spawned task, so a cancelled request cannot stop a batch; a land cancelled
before its batch reached it is dropped, one already replayed lands.
The copy test makes `land` idempotent: a client that never saw the answer to
a land that applied (a timeout, a killed server, an error after commit) can
send it again and gets the copy back instead of a second one. It looks for a
rebased copy that `Repo::rebase` recorded (`rebased-origin-v1`), not for the
forgeable `leep.rebased_from` extra, among the head's ancestors above `cs`'s
generation, and gives up after 1024 changesets (ADR 0021). Past that window
only a land id recognises the retry
([ADR 0057](adr/0057-land-ids.md)): a client that names its land
(`land_id`, random, the same on every retry) has the id stored in the ref
log entry of the move, in the same transaction, and a later land with that
id is answered from the entry at any age. `leep land` and the MCP server
send one (the MCP server derives it from the agent's `request_id`, the ref,
the author and the agent session when there is one, so a retry from a restarted MCP process is answered
too; docs/MCP.md); the scan stays for clients that do not.
A land onto a ref that does not exist creates it, unless the request says
`create: false` ([ADR 0036](adr/0036-explicit-ref-creation-on-land.md)), which
`leep land` does unless given `--create` (or landing the first changeset onto
`main` of an empty repository); then the step that finds no head answers 404
and the ref stays absent. The check uses the head the batch read for its CAS.
Replaying the whole stack matters: rebasing only `cs` would silently drop the
changes of unlanded changesets below it. The stack also stops at an entry
whose rebased copy already landed: an agent that landed `a` (landed as `a'`)
and built `b` on its own `a` lands only `b`, instead of a second, empty copy
of `a`, or a false conflict when the ref changed `a`'s files since. On conflict the client applies
`clean`, resolves `conflicts` on top of `onto`, commits once and lands again,
which is then a fast-forward. Stacks containing merges are rejected.

**Branches and tags** ([ADR 0015](adr/0015-branches-and-tags.md)). Refs are
branches, except those under `tags/`, which are tags. `cas_ref_checked` and
`land` in `leep-derive` apply the rules for every caller: valid names
(`leep_core::refname`), tags are created or deleted but never moved, `land`
refuses `tags/`, and a ref a land policy covers cannot be deleted and only
moves forward (a swap to a value that does not descend from the current one
is a 400). `--protect-ref` covers a ref with a policy that checks nothing
else (`ProtectOnly` in `leep-server`), for protection without signatures or
review. The CLI has no special server API for this: `leep branch`,
`leep switch` and `leep tag` are compare-and-swap calls on `/v1/refs/{name}`. `branch -d` deletes only if
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
files like in `land`, and `commit` and `land` refuse while any remain. The CLI
relabels the server's `<<<<<<< ours` / `>>>>>>> theirs` by role before writing
(`main (landed)` / `yours` for `land` and `sync`, `yours` / `ID (merging)` for
`merge`; `markers.rs`); detection only checks the `<<<<<<< ` prefix, so old and
new labels both count.
Conflicts with no text merge (binary, delete versus modify) keep the local
version and are recorded in `state.unresolved`; they block the same way until
`leep resolve PATH` or `leep restore PATH` clears them. If the
base is not in the target's history, `sync` first rebases the unlanded
first-parent stack onto the head, one changeset at a time; a conflict there
stops with nothing changed. `restore`, `reset` and `stash` write files the same
way. Every write or delete of a repository path goes through
`WorkCopy::abs_for_write`, which refuses a path with a `.leep` or `.git`
component (in any spelling the filesystem folds together) and a path whose
directory on disk is a symbolic link, checked right before each write, so
neither a malicious server nor a crafted tree can make the CLI write outside
the working copy or into its control directories. `reset` and `amend` only
move the base, never a ref. `amend` is refused for a base in the target ref's
history (`merge_base(base, head) == base`). Snapshots are ordinary changesets,
so blob GC keeps what they reference.

**Merge, rebase, cherry-pick and revert** ([ADR 0019](adr/0019-merge-rebase-revert.md)).
These build local, unlanded commits from existing endpoints; `land` moves the
ref. `merge` calls `POST /v1/merge` with parents `[base, REV]`; `cherry-pick`
and `rebase` call `POST /v1/rebase` (a rebase uses the changeset's first parent
as the merge base, so only its own change is replayed). `revert` stores the
inverse of REV's change as a changeset whose parent is REV and rebases that
onto the base, which makes REV the base of the three-way merge. A clean answer
becomes the base through `overlay::apply`. A conflicted one is written like a
`sync` conflict (`overlay::plan_conflicts`, keeping the working copy's file where
there is no text merge), the base stays put, and `state.json` records
`merge_in_progress` or `pick_in_progress`. `commit` then supplies the message
and, for a merge, the second parent; `--abort` removes what the operation
added and resets hard. `MergeResponse::Conflicts` carries `clean` so the client
has the cleanly merged paths; the CLI checks that a server reported them.

**Ref log and undo** ([ADR 0018](adr/0018-ref-log.md)). Every successful ref
create, move or delete appends a row to `ref_log` in the same transaction (or
under the same lock, in memory) as the conditional update, so a ref never
moves unrecorded and a lost CAS writes nothing. Callers pass a
`RefUpdateContext` (actor, operation, message) to `MetaStore::cas_ref_logged`,
or one per step to `cas_ref_logged_chain`, which a land batch uses to move the
ref once while logging each land (ADR 0032):
`land` records `land`, the git bridge `import`, and the server passes the
token's name and the request's operation for `POST /v1/refs/{name}` (`land`
and `import` are reserved so clients cannot forge them). `GET /v1/reflog` and
`/v1/reflog/{name}` page the log newest first. `leep undo` is a client of
existing endpoints: it reads a ref's latest log entry and compare-and-swaps the
ref from its current value to that entry's old value, with operation `undo`,
through `cas_ref_checked`, so name rules, immutable tags, protected deletes,
forward-only protected refs and signature or review policies still apply and a
concurrent move is a 409. Undoing a land on a protected ref is therefore
refused; the CLI points at `leep revert`. It never touches the working copy. Undoing a delete recreates the ref, undoing a
create deletes it, and the redo of an undo is another undo. The log is
unbounded; entries name changesets, which are never deleted, so GC is
unaffected.

**Checkout** (`leep checkout`): the client resolves a rev to an id and lists
files under the requested prefixes (`GET /v1/files`). It then fetches the
missing blobs through its local content-addressed cache and writes them. Only
requested prefixes are materialized; other files are fetched on demand with
`leep cat`.

**CLI contract** ([ADR 0034](adr/0034-cli-exit-codes-and-json-errors.md)).
Every CLI failure is a `CliError` with a kind, and the kind alone decides the
exit code: 1 generic, 2 usage, 3 server or network (retry may work), 4 not
found, 5 conflict, 6 nothing to do (`commit`, `amend`, `stash push` only).
HTTP statuses map to kinds in one place, the client's `error_from`. With
`--json` an error is also printed on stdout as
`{"error":{"code","message","hint","retryable"}}`, `code` being the kind.
`docs/CLI.md` is the user-facing list. Colour and paging (`term.rs`) apply
only to human output on a terminal; JSON and piped output stay plain.

**Short ids** ([ADR 0035](adr/0035-short-changeset-ids.md)). A revision
argument is `@`, a full id, a ref or tag name, or 4 to 64 hex digits of a
changeset id. The CLI asks `/v1/resolve/{rev}` first, so a ref always wins,
then `GET /v1/resolve-prefix/{prefix}`, which `Repo::resolve_prefix` answers
from `MetaStore::changesets_with_prefix`: a bounded range scan of the
changesets' primary key (the prefix padded with `00` and `ff` bytes). Several
matches are a 400 listing up to 10 candidates; `leep log` prints 12-digit ids.

**Merge**: three-way, per path, with base = `merge_base(ours, theirs)`.
Identical changes on both sides merge trivially. Text files merge by line
(`diffy`). Binary files and delete-vs-modify cases are returned as conflicts.

`merge_base(a, b)` is the common ancestor with the highest generation, ties
going to the highest id; no other common ancestor can descend from it. It is
found cheapest first: the deepest changeset on both first-parent chains, by
skiplist jumps (bounded at 64 reads), is a common ancestor and so a lower
bound; if it is `a` or `b` itself, or neither chain has a merge above it
(`last_merge-v1`), it is the answer. Otherwise a walk marks changesets newest
first by (generation, id) with the side or sides that reach them and stops at
the first one both reach, or when either side runs out; parents and
generations come from `Repo`'s ancestry cache or are read ahead in batches as
the log walk does (ADR 0030). `is_ancestor` rides the skiplist and falls back
to the same walk.

**Git mirror** (`leep-git mirror run`, [ADR 0046](adr/0046-live-git-mirror.md)).
A daemon outside the server, one per git remote, on the `/v1` API and the
system `git`; the server has no git code and holds no git credentials. Every
cycle (1 s) it reads the newest ref log `seq`, and `GET /v1/refs` when it
moved; the remote's refs come from `git ls-remote` every 15 s or on
`POST /poke`. Per mirrored ref it compares the export `E` of the Leep ref `L`
with the remote ref `G`:
```
G == E                       in sync
G absent or G ⊂ E            git push E (no force: the remote takes only a fast-forward)
E ⊂ G, tip from git          import the new commits (POST /v1/blobs/missing, PUT blobs,
                             POST /v1/changesets), then POST /v1/refs CAS L → head
                             (land policy applies; fast-forward only)
E ⊂ G, tip from Leep         Leep moved back: report "rewound", never force
otherwise                    "diverged": push E to leep-diverged/<branch>, move nothing
```
The export is incremental and deterministic: a changeset becomes its first
parent's git tree with its own changes applied (only touched directories are
rewritten, only named blobs fetched), and the same commit `leep-git export`
writes, plus a `Leep-Changeset:` trailer for changesets made in Leep. Imports
build changesets with the importer's code, so a commit has one id however
often it arrives. A work directory holds the mirror's bare repository and an
append-only changeset-to-commit map. Because each decision uses only the two
current values and the map, a crash anywhere is recovered by the next cycle:
re-exports give the same commits, a push the remote already took is a no-op,
and the ref CAS lands an import once. Loop prevention: a commit is Leep's own
if the map lists it or its trailer names a changeset whose export is exactly
that commit; those are never imported, and imported changesets export to
their original commits.

Two opt-in additions ([ADR 0049](adr/0049-mirror-merges-and-land-branches.md)):
with `--on-divergence merge`, the "otherwise" row imports `G`, asks
`POST /v1/merge` for `[L, T]` and lands the merge by ref CAS from `L`
(land policy applies), then pushes it as a fast-forward; conflicts or a
refusal fall back to the report. The merge's author, message and timestamp
are functions of `L` and `G`, so a crash anywhere reproduces the same
changeset and commit. With `--land-branches`, remote branches
`leep-land/<ref>/*` are imported and landed with `POST /v1/land`
(`create: false`), which rebases them like an agent's land, then deleted
with an exact lease; conflicts and refusals stay in the status.

## 7. Caching tiers

```
client ~/.cache/leep/objects   (per machine, shared by all jobs on it)
   → server CachedBlobStore    (bounded LRU, per instance)
      → durable BlobStore      (S3 / MinIO / filesystem)
```
All three tiers are safe without invalidation, because keys are content
hashes.

The filesystem store has two layers of its own (ADR 0029):

```
FsBlobStore.get(hash)
   → loose file <root>/<2 hex>/<62 hex>          (every new write lands here)
   → packs/*.idx  (in memory as file bytes, searched in place: hash → pack, offset, base; minus tombstones)
        → decoded-object cache (64 MiB, per store) → delta chain in the .pack
```
`leep-server repack` moves loose objects into a pack: a file version is
stored as a zstd delta against the previous version at its path, a tree node
against the same directory in the first parent, chains capped at 32. By
default it is incremental (ADR 0039). It walks back from the refs only
through changesets whose root tree is still loose, packs the loose objects,
and merges the smallest packs by a geometric rule, so the pack count stays
logarithmic and at most `--max-packs`. Merged entries whose base does not
change are copied frame for frame. `--full` walks all history and rewrites
everything into one pack. The walk and the writer hold each hash once and
name objects by `u32` ids (a sorted table of the run's objects, whose ranks
are the ids, and `leep_core::intern` in the walk), about 140 bytes per
object for a full repack. Compression runs on one thread per CPU; a single
writer thread picks each frame in order, so the pack's bytes do not depend
on the thread count. Backups copy packs and their tombstones as files
(`leep_store::packfiles`, `sync::sync_packs`) before the object-by-object
copy, which then finds the packed objects present. A read
decodes the chain iteratively and checks the result against its hash. GC
deletes a packed object by appending a tombstone; the next full repack leaves
it out. A store reads a pack published by another process on its next miss
(it rescans `packs/` before answering "absent"). An index (ADR 0038) keeps
the hashes in a sorted column behind a 256-entry fanout table. Each hash then
has a record whose field widths are chosen per pack, about 45 bytes per
object in all. The store keeps index files in memory as they are on disk and
searches them without decoding. Indexes in the original 96-byte layout still
read, and a full repack rewrites them. A store checks an index's structure
when it first opens the pack, on several threads for large indexes.

**Cold reads.** A chain's frames were written with the changesets that made
each version, so they lie far apart in the pack; reading them one at a time
from a cold disk waits once per frame. A read plans the chain from the index
first and asks the kernel for every frame at once (`posix_fadvise
WILLNEED`, Linux only, advice only). Walks that know many objects before
reading any (the file listing, tree diffs, blame's file versions) use
`BlobStore::get_many`: the filesystem store splits it over up to 16
blocking tasks that each prefetch all their chains before decoding, the
cache serves its hits and forwards the misses in one call, and other stores
run 32 `get`s at a time. `list_files` and `diff` walk the tree breadth first
and read each level (up to 1,024 nodes) with one `get_many`, so a cold walk
waits on the store about once per level rather than once per directory.

The object store backend (ADR 0040) reads the same packs from the bucket:

```
ObjectBlobStore.get(hash)
   → GET blobs/<2 hex>/<62 hex>                     (every new write lands here)
   → packs/*.idx  (fetched once, kept in memory; minus packs/tombstones/<id>/<hash>)
        → decoded-object cache → one batched ranged GET of the chain's frames in the .pack
```
With no directory to watch, a miss lists `packs/` again at most every 5 s,
which is safe because a repack deletes loose copies only after a fence of
minutes, and a read that finds its cached pack deleted lists at once.
`contains` HEADs the copy's tombstone and the pack's index, so it is exact
across processes as GC needs. Tombstones are one empty object each, so GC
deletes never contend, and a lease object (`packs/repack.lease`, conditional
create and renew) replaces the repack lock.

Planned: a regional cache tier, the same server binary in a
read-through mode against an upstream server. Strict tiering means a cold
regional cache cannot stampede the origin. The server's cache reports hits,
misses and occupancy as Prometheus metrics (see [OPERATIONS.md](OPERATIONS.md)).

## 8. Concurrency and consistency

- Only refs need coordination. `cas_ref` is a single conditional SQL statement
  (an `UPDATE … WHERE target = expected` or an `INSERT … ON CONFLICT DO
  NOTHING`), never a read followed by a write. Its ref log row is inserted in
  the same transaction, only when the statement changed a row.
- Changesets, blobs and derived values are written idempotently. Duplicate
  writes from racing servers are harmless. Batched derived writes insert in
  `(kind, key)` order, so two batches that share rows (claims for the same
  blob from two uploads) queue on each other's row locks instead of
  deadlocking (ADR 0031).
- Durability follows references: a changeset is stored only after every blob
  it names survives power loss. `BlobStore::flush(named)` makes a store's
  recent writes and the named blobs durable, and `flush_blobs(hashes)` only
  the given ones; tree writes flush together with the contents their
  changeset names (`flush_blobs`), and a land batch flushes the blobs it
  stores before its metadata write. The filesystem store defers its
  directory fsyncs to the flush, so an upload is answered before its
  directory is synced (ADR 0044). It tracks each shard directory on its own
  and a flush syncs only the directories it needs, sharing syncs already
  running, so a create waits for its own directories and not for other
  requests' uploads (ADR 0048).
  `POST /v1/blobs/missing` checks fences, then existence
  (`BlobStore::contains_many`), then claims, each phase for all hashes in
  batches.
- A blob may be orphaned if a client uploads it and never commits, and tree
  nodes or merge blobs may be orphaned if the changeset that would reference
  them fails to store. `leep-server gc` reclaims them in two fenced phases:
  it marks everything reachable from every stored changeset (not only refs, so
  unlanded changesets survive), *condemns* unmarked blobs older than a grace
  window (default 24h) by writing `blob-condemned-v1`, waits a fence (default
  15m, longer than the servers' `--request-timeout` plus their statement
  bound), re-marks, and deletes the
  blobs still condemned. A condemned blob is reported missing by
  `POST /v1/blobs/missing` and rejected by `POST /v1/changesets`, so clients
  re-upload it, and an upload rescues it; collect orders its delete against
  uploads with a two-flag handshake in meta. Grace and fence together make GC
  safe beside running servers, with no residual race. See ADR 0008 and
  ADR 0012. The mark is incremental (ADR 0033): each run records what it
  walked in `gc-mark-v1` rows, written so a row always vouches for
  everything below it, and the next run walks only changesets with no row
  and stops at marked tree nodes. Changesets are never deleted, so liveness
  only grows and ref moves never force a full walk; `gc --full` walks
  everything anyway.
  [ADR 0059](adr/0059-unreachable-changeset-gc.md) (proposed; phase 1
  built) will delete changesets no root reaches (refs, every ref log entry,
  landed-copy scan windows, attestations, mirror maps, a grace period) by
  the same condemn, fence and collect protocol. Phase 1 is everything a
  server must do before a sweep may run beside it, and deletes nothing:
  `changesets.stored_at` (migration 0007) ages changesets by the store's
  clock; every writer that is about to make durable something naming an
  existing changeset (a child, a land, a rebase or merge, a ref move, a
  signature or review, a git mirror map) first reads its fence state and
  rescues it and its condemned ancestors (`Repo::rescue_changesets`),
  reading the `gc-fence-active-v1` flag at most every 10 s, so the check
  costs nothing while no sweep runs. Land batches run detached from their
  requests, so each attempt reads the flag uncached before its swap, and a
  batch gives up with a retryable 409 after `MAX_BATCH_LIFETIME` (60 s),
  which bounds how late it can move a ref after its fence read. A keyed
  land retry whose original is not stored is answered from its land id
  record. `changeset_reach::mark` keeps `cs-reach-v1` rows, the incremental
  reachability the sweep will use. `/v1/ready` lists `changeset-rescue`.
  `gc --unreachable --dry-run` measures what the sweep would reclaim.

## 9. Failure model

| Failure | Effect | Recovery |
|---|---|---|
| Server instance dies mid-request (SIGKILL included) | No partial mutable state: refs move by one transaction, everything else is written idempotently, so a request leaves at most unreferenced blobs, tree nodes and derived rows | Client retries; every write is idempotent and a repeated `land` finds its earlier result |
| The server coordinating a ref's lands dies or stalls (`--land-coordination on`) | Its lease is not renewed; lands forwarded to it get no answer and are asked again, never batched elsewhere while the lease is live | Another server takes the lease once its last renewal is a TTL (2 s) old; the crash test checks every agent lands again within the TTL plus 2 s of a SIGKILL; a stalled leader resuming finds its lease gone and hands its lands back. A graceful shutdown releases leases at once (ADR 0056) |
| Store call errors | The request fails with 500 (or 409 when the swap lost); nothing it did is rolled back, and nothing needs to be | Client retries |
| Store call applies, then reports an error (lost commit acknowledgement, timeout after commit) | The write may exist. For a blob, changeset or derived row that is harmless. For `cas_ref` the caller cannot tell | `land` reads the ref again and recognises its own result (by land id at any age, ADR 0057; otherwise within the last 1024 changesets); other ref callers must read the ref before repeating |
| Request cancelled by `--request-timeout` or a client disconnect | Same as a dead server for the work it had done; it performs no further writes, though a Postgres statement already sent runs on until it ends or `--meta-statement-timeout` (the request timeout plus 5 s by default) cancels it | Client retries |
| MetaStore unavailable | Writes and uncached reads fail | Fail fast with 500; no local fallback |
| BlobStore slow | Cache tiers absorb repeated reads | — |
| Land CAS contention | Bounded retries, then an error | Client retries `land` |
| Server started on a database from a newer release, or whose applied migration was edited | The store refuses to open (`database schema vN is newer than this server`), before any write | Upgrade the server; never edit an applied migration (ADR 0023) |
| Server started on a store whose rows use a format feature it does not know (`store_features`) | The store refuses to open, naming the feature, before any read of a row | Upgrade the server (ADR 0055). Servers from before the table cannot check it, so finish a rollout before running `compact-changesets` |
| Migration fails or the migrating process dies mid-run | Every pending migration and its `schema_migrations` row run in one transaction: the database stays at the version it started at | Fix the cause and start again; concurrent starters wait on the lock and find the work done |
| Data damaged at rest (bit rot, a bad restore, a hand-edited table) | Content-addressed objects fail their hash check on read; derived rows without one can answer wrongly and silently (ADR 0021) | `leep-server verify` names every damaged object and row; `repair rederive` rewrites derived rows, and blobs and changesets are restored from backup (OPERATIONS.md, "Verify and repair") |
| Backup taken while servers and GC run | The metadata snapshot is one instant; blobs are copied after it, and a blob the snapshot needs cannot have been deleted (ADR 0012), so the backup restores. A backup whose check finds a missing blob is sealed as failed, exits 1, and `restore` refuses it | Alert on `leep_backup_last_run_ok` and the last-success age; take another backup (ADR 0026) |
| Disk, volume or database lost | Everything since the last backup's snapshot instant | `leep-server restore` into fresh stores, `verify --full --rehash`, start servers (OPERATIONS.md, "Backup and restore") |
| `gc` killed at any point | Rows it wrote are harmless (a condemned blob is re-uploaded on demand, an announced delete is closed by the next collect); no reachable blob was deleted | Run `gc collect` again |
| `repack` killed at any point | Before the index rename nothing is published (temp files and an index-less pack are garbage); after it every object is in the new pack and perhaps also loose, moved aside, or in an old pack. Every object reads throughout | Run `repack` again: it deletes the garbage, puts moved-aside copies back and finishes |
| `repack` of an object store killed at any point | Before the index upload nothing is published (an index-less `.pack` or an incomplete multipart upload is garbage); after it every object is in the new pack and perhaps also loose or in an old pack. Every object reads throughout. The lease stays until it expires (10 min) | Run `repack` again after the lease expires: it deletes the garbage (once older than the lease) and finishes |

**What each backend guarantees.** "Applied" means durable against a process
kill; the SQL backends and the filesystem store also survive power loss
except where noted.

| Backend | Atomic unit | Killed or cancelled mid-call | Error after the call applied |
|---|---|---|---|
| `MemoryMetaStore`, `MemoryBlobStore` | One call, under a lock; a ref and its log entry share the lock | Not durable | Cannot happen |
| SQLite (WAL, synchronous FULL) | One transaction per call; `cas_ref_logged` changes the ref and appends its log entry in one | The open transaction rolls back (the next writer sees nothing of it); a committed one is durable | Only when the error is raised while `COMMIT` is in flight; the caller must treat any `cas_ref` error as unknown |
| Postgres | Same, one transaction per call, plus an advisory lock for log order | The server drops the connection, Postgres rolls the transaction back; committed work stays | Connection lost after `COMMIT` was sent: the commit may have applied |
| Filesystem blobs | Write a temp file, fsync it, rename it into place. The shard directory (and the root, for a shard this process has not synced it for) is synced by a later flush that covers the blob: every changeset write first flushes the nodes it wrote and the contents it references, and a backup flushes everything (ADR 0044, ADR 0048) | A blob is never visible partly written; a crash before the rename leaves a `.tmp-*` file that is never listed, read or counted as the blob and never removed (safe to delete when older than the request timeout). A power loss can drop a blob whose put was answered but that no stored changeset names yet; never one a stored changeset names | A put that errors after the rename leaves a whole blob; repeating it is a no-op |
| Filesystem packs | A pack is published by renaming its fsynced index into place after its fsynced pack file, fsyncing `packs/` after each; a delete of a packed object is one fsynced tombstone line | Nothing partial is ever read: an unpublished pack has no index, and a torn tombstone line is ignored, then cut off under the lock by the next append so it cannot swallow that append's first line. Loose copies and replaced packs are deleted only after the fence, so a reader that listed the old layout still finds every object | A delete that errors after its tombstone was written has deleted; repeating it is a no-op |
| Object store (S3) | One PUT | No partial objects | Same as above |
| Object store packs | A pack is published by uploading its index after the complete pack (a multipart upload is invisible until completed); a delete of a packed object is one tombstone object per pack | Nothing partial is ever read. Loose copies and replaced packs are deleted only after the fence, and readers list `packs/` again on a miss or a missing pack | A delete that errors after its tombstones were written has deleted; repeating it is a no-op |
| `CachedBlobStore` | None (memory) | Gone with the process | A failed put or read caches nothing; a deleted blob can be served from the cache until evicted, which is harmless because only unreferenced blobs are deleted |

Derived data is only ever missing after a failure, never wrong: a row is
written after the values it is computed from exist, and derivation retries
from whatever rows are present. On Postgres, SQLite and memory a new
changeset's `root_tree`, `generation` and `skiplist` rows, a rebased copy's
link and an imported commit's mapping are written in the changeset's own
atomic call, so a crash never leaves a stored changeset without them; a
backend without that call writes the changeset first and the rows after, and
nothing depends on the difference (ADR 0031). What the stores do not protect is a row or
blob that is damaged at rest. Tree nodes are content addressed and history
packs carry checksums, and both are checked on read; most other derived rows are not (ADR 0021
lists them). `leep-server verify` closes that gap offline by recomputation (ADR 0025). The tests that establish all of this are in ADR 0021; the soak test
(ADR 0022) checks the same properties over hours and a deep history, with
GC and restarts running beside a swarm of agents.

Observability: libraries emit `tracing` events and `metrics` facade calls
(no-ops without a recorder); only `leep-server` installs a recorder and
serves `/metrics` on a separate listener. The git mirror reports on its own:
a `status.json` in its work directory, `leep-git mirror status`, and
`/metrics` and `/status` on its `--listen` address. Land retries, exhaustion and cache
hit ratio are the signals to watch; see [OPERATIONS.md](OPERATIONS.md), and
[RUNBOOKS.md](RUNBOOKS.md) for what to do when each alert fires.

## 10. Security

Authentication is static bearer tokens ([ADR 0007](adr/0007-token-authentication.md)).
`leep-server --tokens-file <PATH>` (or `LEEP_TOKENS_FILE`) loads a JSON file of
`{name, role, hash}` entries, where `hash` is `blake3:<hex>` of the token; the
file is read at startup and on SIGHUP (see Config reload below). Without it,
auth is off and the server warns, and it refuses a non-loopback `--listen`
unless `--insecure-no-auth` is given.
With it, an axum middleware requires `Authorization: Bearer <token>` on every
endpoint except `GET /v1/health`: 401 (with `WWW-Authenticate: Bearer`) when the
token is missing, unknown or past the entry's optional `expires` time, 403
when a `read` token makes a mutating request.
`POST /v1/blobs/missing` counts as read because it only queries; it takes at
most 100,000 hashes (the CLI sends 10,000 at a time), and read tokens' bodies are capped at 1 MiB by default. Digests are
compared in constant time, and only the token name is logged. Use
`leep-server gen-token` and `hash-token` to create entries. Terminate TLS in
front of the server; bearer tokens are otherwise visible on the wire.

The CLI reads the token from `--token`, `LEEP_TOKEN`, or `.leep/config.json`.
The CLI retries a rate-limited `GET` (429) a few times, honoring `Retry-After`.

**Signed changesets** ([ADR 0010](adr/0010-signed-changesets.md)). A changeset's
id covers its whole encoding, so a signature cannot live inside it. Ed25519
signatures over `b"leep-changeset-signature-v1\0" ‖ id` are stored as
attestations beside the changeset (derived kind `signature-v1`, one row per
changeset and key), and `Changeset` is unchanged. `leep-server
--signing-keys-file <PATH>` (or `LEEP_SIGNING_KEYS_FILE`) lists trusted keys as
`{name, public_key: "ed25519:<hex>"}`, read at startup and on SIGHUP; signatures by
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
gated the same way; the offline git bridge is not, but the git mirror moves
refs only through that gated CAS.

**Review service** ([ADR 0011](adr/0011-review-service.md)). Approvals are
attestations too (derived kind `review-v1`, one row per changeset and reviewer,
`Changeset` unchanged). The auth middleware hands handlers the token's
`Principal`; `POST /v1/changesets/{id}/reviews` records `approve` or `reject`
under the token's name, never a name from the body, and needs authentication.
Ownership is per directory: `OWNERS` files (one name per line, `#` comments,
`noparent`) in the tree, read from the **target ref's head** at check time so a
change cannot grant itself ownership; a path no file covers has no owners and
any one approval satisfies it. Per changed path, an owner's reject blocks,
then a merge-side match (`merge_parent`: the path equals a side parent's and
the first parent left it as it was at their merge base, so taking the side is
what a clean merge does), an enabled auto rule from the
`--review-policy-file` (`comment_only`, `whitespace_only`, `docs_only`; pure
functions in `leep-derive`; never for an `OWNERS` file) or an owner's
approval satisfies it.
`--require-review <REF>` (repeatable, `LEEP_REQUIRE_REVIEW`) and the policy
file's `refs` make `land` onto a ref fail with 400 `changeset X needs review:
path P ...` unless every changeset it adds is approved, checked on the
originals the client submitted, as for signatures. It is a second
`LandPolicy`: `Repo::with_land_policy` composes policies (all must pass), and
`check_land` now receives the head so owners and new history agree.
`GET .../review-status?ref=` shows the per-path state, `GET .../reviews`
lists verdicts (with `via` for rebased copies), and `leep review ...` and the
`leep_review_status` MCP tool are the clients.

**Request path and limits** ([ADR 0020](adr/0020-request-limits.md)). A
request passes, outermost first, through metrics, tracing, the request timeout,
the global body cap, the config snapshot, authentication (which names the
`Principal`), the body limits and the per-principal limiter, and then the
handler. The body limits are always on: the route's cap (`--max-blob-bytes` for
blob uploads, `--max-json-body-bytes` for the rest; 413) and a share of the
server-wide in-flight byte budget (`BodyBudget`, a semaphore of KiB;
`--max-inflight-bytes`), taken from the declared length, or the route's cap
when there is none, and held until the handler returns (503 when spent). The
limiter is the same axum middleware (`leep_server::limits`) and does nothing
unless the request's config has limits. For a principal with limits it checks the declared body
size, takes a token from the read or write bucket, and `try_acquire`s a
concurrency permit, refusing with 413 or 429 (never queueing). The permit
lives in a guard attached to the response body, so it is released when the body
ends, the client goes away or the timeout drops the request. Token names bound
the state; in anonymous mode state is keyed by client address in a size-capped
table. State is per process.

**Config reload** ([ADR 0024](adr/0024-config-reload.md)). The tokens, their
limits, the trusted signing keys, the review service and the composed land
policy form one immutable `RuntimeConfig` (`leep_server::reload`), held in an
`ArcSwap`. SIGHUP re-reads the tokens, signing keys and review policy files,
validates all of them with the startup checks, and swaps in the new config
only if every file is good; otherwise the old config stays, an error is logged
and `leep_config_reload_total{result="error"}` counts it. A middleware pins each
request to the config current on arrival, so authentication, limits and
handlers agree and a request in flight finishes under the config it started
with. The land policy is not stored in `Repo`: `land` and raw ref updates run on
a copy of the repo (`Repo::with_land_policy_replaced`) carrying the request's
policy, so one land uses one policy throughout. A new limiter keeps the slot of
every token whose limits did not change. Flags, listeners and stores are not
reloaded.

Planned: OIDC or mTLS, per-path signing policy, team groups
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

**Lands across servers**
([ADR 0056](adr/0056-land-coordinator-per-ref.md)). Group commit batches a
ref's lands only within one process, so from the Team stage on, servers must
not race each other on a hot ref: run them with `--land-coordination on`.
Each ref gets one leader: the server holding its `land` row in `leases`, taken
and renewed with single conditional statements (2 s TTL, renewed every
500 ms, taken over only once the TTL has passed without a renewal). Other
servers forward `POST /v1/land` to the leader's `--advertise-url`, checked
against an allow-list, with the client's own body and token, and never batch
a land themselves while a live lease names another server. A stale or dead
leader costs only time: its swap loses or wins on `cas_ref` like any other.
Throughput per ref stays one leader's; one owner per ref is what later allows
pipelined and parallel replays and a dedicated tier of landing servers for
the agent swarm stage.

**Multiple repositories** (M7, designed in
[ADR 0054](adr/0054-multiple-repositories-per-server.md), not implemented
yet). A server will hold many repositories in one metadata store and one blob
store. Content-addressed objects and the derived kinds that are pure
functions of their key stay shared; refs, the ref log and a few
policy-bearing kinds (`review-v1`, `signature-v1`, the mirror maps) gain a
repository id; membership tables, closed under ancestry, decide which
changesets and blobs a repository can reach, so only entry points are
checked. Today's `/v1` paths keep serving the default repository and every
endpoint is also served under `/v1/repos/{repo}/...`. Authorization becomes a
function of principal and repository behind one `Authorizer` seam, and
`derived` is not partitioned by repository.

Storage is the cost that grows fastest with history: every blob and tree
node is stored whole, so imported git/git takes 8.7 GB of objects against a
0.33 GB git pack. ADR 0029 adds packs below `BlobStore`: `leep-server
repack` stores objects as zstd deltas against the previous version of the
same path, with no change to hashes, the API or clients. The filesystem
store has them (the first 3,795 commits of git/git: 91.2 MB loose, a 3.1 MB
pack plus a 0.5 MB index since ADR 0038, against git's 4.4 MB pack), and
so does the object store backend since ADR 0040. Since ADR 0039 the
scheduled repack is incremental (it costs the new history, not all of it)
and backups copy packs as files; incremental repacks of object stores are
next.
