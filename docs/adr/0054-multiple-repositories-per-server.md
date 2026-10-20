# 0054. Multiple repositories per server
Status: Accepted (design; not implemented yet)
Date: 2026-10-20

## Context
A Leep server holds exactly one repository. One `MetaStore` and one
`BlobStore` back one `leep_derive::Repo`. The ref namespace (`refs.name`),
the ref log (`ref_log`), the tokens file, the review and signing policies,
the request limits and every `/v1` path are implicitly that repository's.
M7 asks for many repositories per server, "each with its own refs, tokens
and policies" (ROADMAP.md), and PRODUCT.md makes that a prerequisite for a
hosted service. This ADR decides the shape before any code moves, so the
schema migration, the API additions and the auth seam are designed once.

What the current code already gives us:

- **Objects are content-addressed and repository-free.** A changeset id is
  BLAKE3 over its canonical bytes, which name its parents (ADR 0001, 0002);
  tree nodes and blobs are addressed by content (ADR 0041). The same id in
  two repositories is the same object with the same history.
- **Most derived rows are pure functions of an id.** `generation`,
  `skiplist`, `root_tree`, `last_merge-v1`, `history-tree-v4` and
  `history-node-v4`, `log-filter-v1` and `log-filter-name-v1`, `blame`,
  `blob-trigrams-v1` and `tree-trigrams-v1` are keyed by a changeset id or a
  content hash and computed only from what that key reaches. They are
  correct in every repository that contains the key.
- **A few kinds hold policy or server state, not functions of content.**
  `review-v1` (reviewer = a token name, ADR 0011), `signature-v1` (an
  attestation checked against the server's trusted keys, ADR 0010),
  `rebased-origin-v1`, the mirror maps `git-to-leep-v1`/`leep-to-git-v1`
  (ADR 0046), and the GC state `blob-claim-v1`, `blob-condemned-v1`,
  `blob-rescue-v1`, `blob-doom-v1` and `gc-mark-v1` (ADR 0012, 0033).
- **Liveness is store-wide.** GC marks from every stored changeset, not
  from refs (ADR 0008), and changesets are never deleted.
- **`Repo` is already "one repository".** Everything above the
  `MetaStore` trait asks one store for refs, changesets and rows. A
  repository-scoped view of a shared store can be another `MetaStore`.
- `derived` is the largest table (33 GB on Linux, ADR 0052) and the hot
  path of every read. ADR 0052 declined partitioning it by kind for now.

## Decision

### 1. Identity
A repository has an immutable internal id (`RepoId`, a positive 64-bit
integer assigned by the store) and a unique, mutable **name**: 1 to 64 bytes
of `a-z 0-9 . _ -`, starting with a letter or digit, never `.` or `..`, and
never containing `/` (so it is one URL path segment). Names are what users,
URLs, tokens and policies use; ids are what rows use, so a rename touches one
row. A renamed repository's old name is free at once; nothing redirects.

A new metadata table is the registry:

```
repos(id BIGSERIAL PRIMARY KEY, name TEXT NOT NULL UNIQUE,
      created_at BIGINT NOT NULL, deleted_at BIGINT,          -- soft delete
      limits TEXT NOT NULL DEFAULT '{}',                       -- JSON, section 7
      membership_complete BOOLEAN NOT NULL)                    -- section 6
```

Repository 1 always exists: the **default repository**, the one every store
holds today.

### 2. Tenancy model: one store, logical repositories
All repositories of a server share its `MetaStore` and `BlobStore`. Rows that
are repository state gain a `repo_id`; content-addressed objects and pure
derived rows stay shared. One schema, one migration run, one backup, one GC,
one pool, and forks of a codebase share their history and indexes.

**Membership** decides what a repository can see. Two tables, written in the
same transaction as the data they admit:

```
repo_changesets(repo_id BIGINT, id BYTEA, PRIMARY KEY (repo_id, id))
repo_blobs(repo_id BIGINT, hash BYTEA, PRIMARY KEY (repo_id, hash))
```

- A changeset is admitted to repository R by `put_changeset[s]_with_derived`
  through R, and only if **every parent is already a member of R** (the
  server already refuses unknown parents). Membership is therefore closed
  under ancestry: any walk that starts at a member reaches only members, so
  only *entry points* are checked: ids in URL paths and bodies (`{id}`,
  `from`/`to`, `a`/`b`, land and merge inputs), ids a ref resolves to (refs
  are per repository, so always members), and the ids inside page cursors
  (a log cursor's queue, a search cursor's changeset). History walks,
  derivation and caches stay unchanged.
- A blob is a member of R when it was uploaded through R
  (`PUT /v1/repos/{repo}/blobs/{hash}`), named by the `Modify` of a changeset
  admitted to R, or written by the server for R (merge results, git import).
  `POST .../blobs/missing` answers per repository: a blob stored for another
  repository is "missing" here, the client uploads it, the server checks the
  hash and only adds the membership row (no second copy). Possession is
  proved by the bytes, so the endpoint is not an existence oracle across
  repositories.
- Tree nodes are never members. They live in the `BlobStore` but are served
  only through changeset-rooted endpoints (`tree`, `files`, `file`, `diff`,
  `blame`, `search`), which check the changeset. In repository scope
  `GET .../blobs/{hash}` serves only `repo_blobs` members.

A request naming an object outside R is 404, exactly as if it did not exist.

### 3. Keys, refs and the ref log
- `refs` becomes `refs(repo_id, name, target, PRIMARY KEY (repo_id, name))`.
  Ref names, tags (`tags/`), protection and land policy are per repository.
  Refs still move only through `cas_ref` (now scoped), never read-then-write.
- `ref_log` gains `repo_id`, with indexes `(repo_id, name, seq)` and
  `(repo_id, seq)` replacing `ref_log_name`. `seq` stays one store-wide
  sequence: per repository it still increases (with gaps), which is all ADR
  0018 promises, and a cross-repository audit stream comes for free. Ref log
  page cursors (FORMATS.md, "Page cursors") fingerprint the repository.
- `changesets`, `changeset_dicts` and `schema_migrations` do not change. The
  compression dictionary stays per store: it is trained on all rows and
  decoding never needs to know the repository.

### 4. Blobs
The blob store layout does not change: loose objects and packs stay keyed by
content (ADR 0029, 0040). Isolation is the membership check above, not
separate prefixes or buckets. GC keeps its store-wide definition of live
("referenced by any stored changeset", ADR 0008), so sharing cannot make GC
delete a blob another repository needs. When GC deletes an orphan blob it
also deletes that hash's `repo_blobs` rows, inside the same fence (ADR 0012);
a row left by a crash in between only makes a read answer 404 sooner.

### 5. Derived rows
- **Pure kinds stay shared in `derived`, with no `repo_id`.** A row is
  correct for every repository that can reach its key, and membership
  decides who can reach it. Adding `repo_id` would duplicate the largest
  table per fork and re-derive history that already has rows.
- **Repository-scoped kinds** (`review-v1`, `signature-v1`,
  `rebased-origin-v1`, `git-to-leep-v1`, `leep-to-git-v1`, and any future
  kind that is not a pure function of its key) move to
  `repo_derived(repo_id, kind, key, value, PRIMARY KEY (repo_id, kind, key))`.
  `leep_derive` declares the list (`REPO_SCOPED_KINDS`); the scoped store
  (section 8) routes those kinds to `repo_derived` and the rest to `derived`.
  For the default repository the scoped kinds keep living in `derived`, so
  the upgrade moves no rows. A new kind must say which list it is on; the
  `add-derived-data-kind` skill gains that step.
- **GC state stays store-wide** in `derived`; GC is one process over one
  store.
- **No partitioning by repository.** Shared rows have no repository.
  `repo_derived` is small (reviews, signatures, mirror maps) and indexed by
  `repo_id` first, so deleting a repository is one range delete.
- **No partitioning by kind in this change.** ADR 0052's reasoning holds:
  the kind walk drops a kind at the cost of reading the table once, and list
  partitioning rewrites the 33 GB table once and needs a partition per new
  kind. Multiple repositories add no kinds, so they do not change that
  trade. If a deployment needs it, Postgres list partitioning by `kind` (with
  a default partition) is compatible with this design because the primary
  key already leads with `kind`; it would be its own ADR and an
  operator-run migration.

### 6. Migrating an existing single-repository store
One forward migration (the next free number in both backends, ADR 0023):

1. Create `repos` and insert `(1, 'default', now, NULL, '{}', false)`.
2. `ALTER TABLE refs ADD COLUMN repo_id BIGINT NOT NULL DEFAULT 1`, rebuild
   the primary key as `(repo_id, name)`; the same column on `ref_log`, with
   the two new indexes replacing `ref_log_name`. Existing rows become
   repository 1's with no rewrite of their values (Postgres adds a column
   with a constant default without rewriting the table; the key rebuild
   reads `refs`, which is small; the `ref_log` indexes are rebuilt once).
3. Create `repo_changesets`, `repo_blobs` and `repo_derived`, empty.

An old server refuses the migrated database (ADR 0023), so a fleet upgrades
together, as for any migration. After it, a server behaves exactly as
before: the default repository answers today's paths, and while
`membership_complete` is false the default repository skips the membership
check (it sees every object, as today) and no other repository can be
created.

**Enabling multiple repositories** is a deliberate operator step, never
part of server start: `leep-server repos backfill` writes repository 1's
membership rows from `changesets` and from the `Modify` blobs of every
changeset, in key order from a resumable cursor in batches (the pattern of
ADR 0052's walks), then sets `membership_complete`. It is idempotent and
safe while the server runs: new writes add their own rows, and the check
stays off until the flag flips. Only then does `POST /v1/repos` create a
second repository. A single-repository deployment never pays for
membership rows. Backups (ADR 0026) learn the four new tables; older
snapshots restore through the migration like any older schema.

### 7. Per-repository limits and quotas
- **Request limits** (ADR 0020) stay per principal, across repositories: a
  principal is one actor. `repos.limits` adds optional aggregate `rate`,
  `burst` and `concurrency` per repository, so one busy repository cannot
  starve the others; a request takes a permit from both.
- **Storage quotas** in `repos.limits`: `max_blob_bytes` (sum of member
  blob sizes), `max_changesets`, `max_refs`, `max_blob_size` (one upload).
  Usage is counted when membership rows are inserted, accumulated in process
  and flushed to a `repo_usage(repo_id, blob_bytes, changesets, refs)` row
  every few seconds, so the hot land path never contends on one counter.
  Quotas are therefore soft by up to a flush interval per server. They are
  checked on blob upload, changeset creation, land and ref creation. Over a
  quota is a new `Error::QuotaExceeded` mapped to 507 with the usual
  `{"error": ...}` body; one blob over `max_blob_size` is 413. Usage is
  logical (what the repository references), not physical bytes after
  sharing and packing, so it is stable and explainable to a tenant.
- **Background work** (index warming, ADR 0028; derive jobs, ADR 0050) is
  queued per repository and served round robin.
- `repos.limits` is edited by an admin endpoint or `leep-server repos set`,
  and read on change, so no restart or SIGHUP is needed.

### 8. Code shape
- `leep-core` adds `RepoId`, a `RepoScope` (id plus name), and a
  `ScopedMetaStore` adapter: it implements `MetaStore` for one repository
  over a backend's repository-aware methods, so `leep_derive::Repo` keeps
  meaning one repository and every operation in `leep-derive` is untouched.
  The backend trait grows `*_in(repo, ...)` variants for refs, the ref log,
  membership and `repo_derived`; the in-memory store implements them, and
  `check_meta_contract` gains cross-repository isolation cases that every
  backend must pass.
- `leep-server` keeps a registry `name -> Arc<Repo>` (built lazily, evicted
  when idle), all sharing the store pools and the content-keyed caches
  (trees, history nodes, ancestry), which are safe to share because their
  keys are content. Any cache keyed by a ref name gains the repository.
- `leep-cli`, `leep-mcp` and `leep-git` stay HTTP clients; they gain an
  optional repository name (section 9) and nothing else.

### 9. API paths
- Every repository-scoped endpoint is also served under
  `/v1/repos/{repo}/...` with the same path tail, query, body and response:
  `/v1/repos/{repo}/log/{id}`, `/v1/repos/{repo}/refs/{*name}`,
  `/v1/repos/{repo}/land`, and so on. `{repo}` is one segment, so
  `{*name}` and `{*rev}` keep matching the rest of the path.
- Today's unprefixed paths stay and mean the **default repository**
  (`--default-repo NAME`, default the repository with id 1). No `/v1` path or
  field is removed or renamed (FORMATS.md rule 4), so every existing client
  keeps working against the default repository.
- Server-wide endpoints stay unprefixed and gain nothing repository-shaped:
  `GET /v1/health`, `GET /v1/ready`, `GET /v1/whoami` (which adds an optional
  `repos` field: the names and roles the token holds).
- New: `GET /v1/repos` (the repositories the token can read, paged with the
  same cursor convention), `POST /v1/repos` (create; admin),
  `GET /v1/repos/{repo}` (name, id, created, limits and usage for an admin),
  `PATCH /v1/repos/{repo}` (rename, limits; admin) and
  `DELETE /v1/repos/{repo}` (soft delete; admin).
- A repository the token cannot read is 404, not 403, so names cannot be
  probed. A deleted repository is 404.
- The CLI stores an optional `repo` in `.leep/config.json` (`leep init
  --repo NAME`) and prefixes its paths when set; the MCP server takes the
  same setting. Without it they talk to the default repository, as today.

### 10. Auth scoping hooks
- Authentication stays global and unchanged (who is calling). Authorization
  moves after routing and becomes a function of (principal, repository,
  action): an `Authorizer` with one method, `role(principal, repo) ->
  Option<Role>`, and roles `read`, `write` and `admin` (admin may also manage
  the repository; a server admin may create repositories).
- The static tokens file implements it. An entry gains an optional `repos`
  map, repository name or `*` to role. An entry without `repos` keeps
  today's meaning **for the default repository only**, so an existing file
  grants nothing on new repositories (fail closed). `server_admin: true`
  marks the principals that may create and delete repositories.
- OIDC and mTLS (the other M7 identity item) plug in as further
  `Authorizer`s that map claims or certificate fields to the same roles; no
  handler changes.
- Per-repository policy files: review policy, owners rules beyond `OWNERS`
  files (which already live in each repository's tree), trusted signing
  keys and protected refs move under a per-repository section of the
  existing files, keyed by name, still reloaded on SIGHUP (ADR 0024). A
  repository without a section has no policy, as a server without the files
  does today.
- Metrics gain a `repo` label only on low-cardinality series (requests,
  lands, quota refusals), and only for repositories an operator lists, so a
  server with thousands of repositories does not explode Prometheus.

## Alternatives considered
- **One server process (and store) per repository behind a router.** What
  operators can do today. Strong isolation, but N pools, N backups, N GC
  runs, N migrations and no sharing between forks. It remains the answer for
  hard isolation (a dedicated tenant), and this design does not prevent it.
- **A database or schema per repository.** Easy export and deletion, but
  migrations, pools and backups multiply, Postgres catalogs bloat with
  thousands of schemas, and nothing is shared.
- **`repo_id` on every table, including `changesets` and `derived`.**
  Simplest isolation story, but it rewrites the two largest tables'
  primary keys, duplicates every fork's history and indexes, and forces
  re-deriving rows that are already correct.
- **Salting object ids with the repository.** Breaks content addressing,
  the git round trip and the "never change hashing" rule, for isolation the
  membership check already gives.
- **Membership checked on every object read**, not only at entry points.
  Correct without the ancestry rule, but it doubles the metadata reads of
  every history walk.
- **No blob isolation** (a shared namespace with dedup by hash). Cheapest,
  but any token could read any repository's blob by hash and probe for
  known files through `blobs/missing`; unacceptable for a hosted service.
- **Partitioning `derived` by repository or by kind.** By repository
  contradicts sharing; by kind is unrelated to multiple repositories and
  stays deferred (ADR 0052).
- **Repository in a header or query parameter, or a subdomain.** A header
  is invisible in access logs and easy to forget, and cannot be authorized
  by path; subdomains need DNS and certificates per repository. A path
  prefix works behind any proxy, and a proxy can map hosts to it.
- **Per-repository `seq` for the ref log.** Gapless numbers per repository,
  at the cost of a counter row per repository on every ref move (contended
  by group commit, ADR 0032). The store-wide sequence already satisfies ADR
  0018.

## Consequences
- An upgraded single-repository server behaves exactly as before, with one
  migration that adds a column and empty tables, and pays nothing for
  membership until an operator enables multiple repositories.
- Every new `/v1` surface is additive; old clients keep working against the
  default repository.
- Forks share changesets, blobs and index rows; a repository's quota counts
  what it references, so physical usage can be lower than the sum of
  quotas.
- Deleting a repository removes its refs, ref log, membership,
  `repo_derived` rows and usage, but not changesets, derived rows or blobs,
  because "changesets are never deleted" and GC liveness are store-wide.
  Reclaiming objects no repository references needs a store-wide sweep of
  changesets, with its own ADR.
- The ancestry rule makes membership a write-time invariant; a backend bug
  that admits a changeset without its parents would widen visibility, so
  the contract tests check it, and `leep-server verify` (ADR 0025) gains a
  membership-closure check.
- Implementation order: the core types and contract tests, the migration
  and backfill command, the scoped store and server registry with the
  prefixed router, auth and policy scoping, then quotas, CLI and MCP.
