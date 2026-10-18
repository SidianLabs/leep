# Leep roadmap

Milestones are ordered. Each one should leave `main` releasable. Who Leep is
for, and why the milestones are in this order, is in
[PRODUCT.md](PRODUCT.md).

## How versions move

Milestones are numbered M1, M2, ... and are planning labels, not versions.
The workspace version (`[workspace.package] version` in `Cargo.toml`) stays
below `0.1.0` (currently `0.0.1`) until the maintainer decides otherwise,
and nothing is tagged until then. ADRs 0007 to 0010 predate this naming and
call M1 to M3 "v0.1" to "v0.3".

- A milestone closes only when every item is done **and** its exit criteria
  hold. Ticking the last box is not enough.
- CI runs `scripts/check-version.sh`, which fails if any crate reaches
  `0.1.0`. Raising that ceiling is part of the maintainer's version change.
- Changing the version, tagging, or publishing artifacts is a
  maintainer decision. Agents never do it on their own.
- Most of the road to 1.0 is hardening, operating and using Leep for real,
  not adding features. Expect the later milestones to take longer than the
  early ones.
- `1.0` means a promise: on-disk formats, the `/v1` API and the CLI stay
  compatible, with a documented deprecation policy. That promise is only
  made once the [1.0 criteria](#10-criteria) are met.

## M1: Core loop
- [x] Core data model, storage traits, API types (`leep-core`)
- [x] Blob backends: filesystem, S3/MinIO, LRU cache (`leep-store`)
- [x] Metadata backends: SQLite, Postgres (`leep-meta`)
- [x] Derived data and operations: tree, generation, skiplist, diff, log, blame, merge, rebase, land (`leep-derive`)
- [x] HTTP server (`leep-server`)
- [x] CLI with lazy sparse checkouts (`leep-cli`)
- [x] Git import and export (`leep-git`)
- [x] End-to-end test: import a git repo, run 50 concurrent agent lands, export back to git

## M2: Operable
- [x] Token auth (static bearer tokens, read/write roles; ADR 0007)
- [x] Structured logs (`--log-format json`, one line per request)
- [x] Prometheus metrics (cache hit rates, land retries, derive latency; `--metrics-listen`)
- [x] Docker image and a docker-compose file (server + Postgres; `docs/DEPLOY.md`)
- [x] Blob garbage collection (mark from all stored changesets plus a retention window; ADR 0008)
- [x] Benchmarks: commit and land latency versus repository size (`docs/PERFORMANCE.md`)

## M3: Agent-native
- [x] Fenced two-phase blob GC that closes the last claim/delete race (ADR 0012)
- [x] Provenance: `leep log --agent`, and queries over `agent.*` metadata (`leep provenance`, `leep blame --who`)
- [x] Signed changesets (author keys; Ed25519 attestations, `--require-signatures` land policy; ADR 0010)
- [x] Review service with semantic approval rules (comment-only, whitespace-only and docs-only changes auto-approve) and per-directory owners (`OWNERS` files, `--require-review`, `leep review`; ADR 0011)
- [x] MCP server exposing Leep operations to agents (`leep-mcp`; ADR 0009)

## M4: History and search
- [x] Path-history index as derived data (`history-tree-v1`, `leep last-change`; ADR 0013)
- [x] Server-side code search as derived data (`leep search`; ADR 0014)

Exit: both indexes derive lazily on a 100k-changeset repository without
operator action, and their benchmarks are in `docs/PERFORMANCE.md`.

## M5: Hardening

### Everyday workflow (a git user can do a normal day's work without the API)
- [x] `leep branch`, `leep switch` and immutable tags
- [x] Rename detection in `diff` and `log --follow` (ADR 0016)
- [x] `sync` that rebases uncommitted work, and `leep stash`
- [x] `leep restore`, `leep amend` and `leep reset` for unlanded work
- [x] `leep merge`, `leep rebase`, `leep revert` and `leep cherry-pick` (ADR 0019)
- [x] Ref log (history of every ref move) and `leep undo` (ADR 0018)

### Hardening
- [ ] Self-host Leep's own development on a Leep server, mirrored to GitHub,
  with the git bridge keeping the mirror in sync
- [x] Fuzz every decoder (postcard objects, derived rows, API request bodies,
  OWNERS and policy files) and the comment lexer; run the fuzzers in CI
  (`docs/FUZZING.md`)
- [x] Crash-consistency tests: kill the server and GC at random points and
  check that no ref, changeset or blob reachable from a ref is lost (ADR 0021)
- [x] Fault injection in the storage traits: timeouts, partial writes, errors (ADR 0021)
- [x] Long-running soak test: many agents landing for hours against Postgres
  (`leep-soak`, ADR 0022; scheduled weekly in `.github/workflows/soak.yml`)
- [x] Security review of auth, signing, review policies and GC, with findings
  fixed or written down in the relevant ADR (`docs/SECURITY-REVIEW.md`)
- [x] Request size, rate and concurrency limits per token (ADR 0020)

### Scale on real histories (git/git, Linux; PERFORMANCE.md)
- [x] Import, pack and index the whole Linux history (1.48M commits) on a
  4-vCPU, 12 GB machine, with the results in PERFORMANCE.md
- [x] Lands scale with batching under many agents (ADR 0043)
- [x] Repack of all of Linux in about 2 GB of memory (#80)
- [ ] Path-history index at least 5x smaller on Linux (24 GB today), queries
  no slower
- [ ] Index for `log --author` and provenance filters (154 s on Linux today)
- [ ] Cold full listings, deep diffs and first blames on Linux in seconds

Exit: a full soak run and the fuzzers find nothing new for two weeks.

## M6: Operations
- [x] Backup and restore for metadata and blobs, with a tested restore drill
  (`leep-server backup`, `restore` and `backup-verify`; the drill is
  `crates/leep-e2e/tests/backup.rs`; ADR 0026)
- [x] Upgrade path: versioned schema migrations for SQLite and Postgres, and a
  test that upgrades a populated store from every earlier tagged version
  (ADR 0023; until a release is tagged, from the pre-ref-log and un-versioned
  schemas, with a fixture added for every new migration)
- [x] Repair tools: verify reachability, re-derive or drop derived data by kind
  (`leep-server verify` and `repair`; ADR 0025)
- [x] Runbook for every alert in `docs/OPERATIONS.md` (`docs/RUNBOOKS.md`;
  `scripts/check-runbooks.sh` keeps them in step)
- [x] Config reload without restart (tokens, keys, policies; SIGHUP, ADR 0024)
- [ ] Two or three design partners from the first market (see
  [PRODUCT.md](PRODUCT.md)) running Leep for real work

Exit: an operator who has never seen Leep can deploy, back up, restore and
upgrade it from the docs alone.

## M7: Git compatibility
Leep stays its own system; git is a door in (see [STRATEGY.md](STRATEGY.md)).
- Live two-way mirror with GitHub and GitLab: lands appear in git within
  seconds, and pushes to the git side land in Leep, with conflicts reported,
  never silently resolved
- Self-host Leep's own development through the mirror (the M5 item above)
- git wire protocol on the server: `git clone`, `fetch` and `push` against a
  Leep server, so IDEs and CI work unchanged
- Installable releases: prebuilt binaries, a published container image,
  `cargo install`
- Client polish: clear errors, `leep status`/`diff` UX, conflict resolution
  flow, shell completions

Exit: a team keeps working in git and its CI unchanged while its agents
land through Leep, for a month, with no lost or diverged change.

## M8: Multi-repository and identity
- Multiple repositories per server, each with its own refs, tokens, policies
  and indexes
- Organizations, teams and per-repository roles
- Identity: OIDC and SAML single sign-on, and mTLS for machines, in addition
  to static tokens
- Audit log of every authenticated action
- Events and webhooks (lands, proposals, ref moves) for CI and other tools

Exit: the design partners' top requests for this milestone are shipped,
and Leep's own development has run on a Leep server since M7.

## M9: Collaboration
- Proposals: Leep's equivalent of a pull request, with review threads,
  approvals and required checks, built on the existing review service
- Early conflict warnings: the land queue tells an agent its change will
  conflict before it lands
- A web view of repositories, history, diffs and proposals

Exit: a design partner reviews and lands its human changes in Leep, not in
a mirror.

## M10: Enterprise operations and scale out
- High availability: several servers on one database, with tested failover
- Helm chart and a documented production topology
- Large binary files
- Regional read-through cache tier
- Distributed SQL `MetaStore` backend
- FUSE virtual filesystem (`leep mount`)
- Published performance targets (land latency, read latency, repository
  size) with benchmarks that check them

Exit: the targets hold in a multi-region test deployment, and a server can
be lost without losing an acknowledged land.

## M11: Release candidate
- Freeze on-disk formats and the `/v1` API; document what is stable
- Deprecation and support policy
- Complete user, operator and API reference docs
- External users running it for real work, with their feedback addressed

Exit: no format or API change needed for one full milestone.

## 1.0 criteria
- Formats, `/v1` and the CLI are covered by the compatibility promise above
- Every earlier milestone's exit criteria still hold on the release commit
- Upgrades from the last two minor versions are tested in CI
- No open security findings rated high or above
- A maintainer signs off on the release

## M12: Hosted platform (after 1.0)
- Multi-tenant hosted Leep built on the open core: organizations,
  repositories and the web interface for code, history and review
- Billing, usage limits and support
- Features for agent-heavy teams: per-agent identity and provenance views,
  merge queues for thousands of lands a day, agent APIs

Exit: outside teams run their main repositories on the hosted service.

## Later / research
- DeltaDB-style live edit streams (CRDT worktrees) between commits
- Linking each change to the agent conversation that produced it
