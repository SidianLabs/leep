# 0022. Soak test
Status: Accepted
Date: 2026-10-15

## Context
The M5 hardening list asks for a "long-running soak test: many agents landing
for hours against Postgres", and its exit criterion is a full soak run that
finds nothing new for two weeks. The tests we have are short by design: the
crash test (ADR 0021) kills the server a few times in a minute, the fault
sweeps run on in-memory stores, the e2e tests check one feature at a time.
None of them can show what only time and volume show: a leak that grows a few
MiB an hour, land latency that creeps up as history deepens, an index that
disagrees with the history after a few hundred thousand changesets, a lock
that deadlocks once in a million requests, GC that slowly eats a blob that
is reachable only from an old changeset.

## Decision

### A binary, `leep-soak`, not an ignored test
`crates/leep-soak` is a workspace member (`publish = false`) with one binary.
The alternatives were an `#[ignore]` e2e test driven by environment
variables, or a script around the existing tests. A binary wins for long
runs: it has real flags (`--duration 8h`, `--agents`, `--chaos`, `--seed`), it
writes its report when a violation stops it as well as when it finishes, CI and a
person run the same thing, and nothing in `cargo test` has to stay alive for
hours. It depends on `leep-core` only (types and the API shapes), talks to the
server over HTTP like any client, and drives the real `leep` and
`leep-server` binaries for the CLI agents, GC and index warmups. No product
crate depends on it. It does not use `leep-testkit`: that crate holds
fault-injecting store wrappers, which a soak against real stores has no use
for.

A ten-second run of the same binary is one test of `leep-e2e`
(`soak_smoke`), so the harness cannot rot between scheduled runs. It uses
Postgres when `LEEP_TEST_POSTGRES_URL` is set and SQLite otherwise, rather
than skipping: the harness has no Postgres-only logic except the database it
creates and the size and deadlock counters it reads, and an SQLite run still
covers the agents, the checks, GC, a restart and the report.

### The workload
A seeded plan: each agent's choices come from a generator seeded by
`--seed` and the agent number, so a failure's seed replays the same plan
(thread and process scheduling stay up to the OS). The repository is
synthetic: about 3000 files in 30 directories of source, a tenth docs, a
few dozen binary assets (`--files`), imported as a chain of changesets.

Fifty HTTP agents (`--agents`) and two CLI agents (`--cli-agents`) loop with
a heavy-tailed think time (`--think`, mean 6 s by default; a tenth of the
mean to ten times it). An iteration is one of:

- reads (cat checked against the directory listing's hash, list, search, log,
  blame checked line by line against the file, last-change);
- a change landed on main: one to three changesets stacked, each editing,
  adding, deleting or renaming files (renames only of files the agent added),
  replacing a binary asset, or editing one of eight hot files so that real
  conflicts happen; every changeset carries agent provenance, about one in seven
  is signed, and the stack is landed. A conflict is never forced: the agent
  starts over from the new head;
- a step of a feature branch (create, one to three lands on it, then merge to
  main by landing its head, delete unmerged, or leave it);
- a tag created or deleted, blobs uploaded and never committed (what GC is
  for), and the old ones committed later (a draft resumed);
- the release flow (below).

Every changeset's message starts `soak <key> ` and its notes file (a file
only that agent writes) gets a line with a unique token. CLI agents work in
a real working copy with `leep init`, `sync`, `commit` (sometimes
`--sign`), `land`, `switch -c`, `merge`, `revert`, `cherry-pick` and `rebase`,
and give up a conflicted land with `checkout --force`.

With policy on (the default when the harness owns the server) every agent has
its own token, and the server enforces `release`: only signed changesets,
only reviewed ones. The `release` ref is its own root lineage, created at
setup by a signed and approved changeset. Agents first try to land unsigned
and unreviewed (the refusal is the expected answer), then sign, get approval
from a reviewer token and land.

Operators run beside the swarm: a fenced `leep-server gc` every
`--gc-interval` (grace twice the request timeout, fence 1.5 times it plus two
seconds, always above the timeout as ADR 0012 requires) and `derive-search
main` warmups. With `--chaos` the harness restarts the server, SIGTERM for two
restarts in three and SIGKILL for the rest. `--rate-limit`, `--rate-burst` and
`--max-concurrent` pass server limits through, so with per-agent tokens the
agents meet 429 and wait out `Retry-After`.

### Invariants
A checker reads only through the HTTP API (so it also works against a server
the harness did not start) and runs every `--check-interval` and once more
after the swarm stops. It keeps every changeset it has fully walked in memory
(they are immutable) and walks only what is new, so each round is
incremental over the whole history. Checked:

1. **Acknowledged lands.** Every land a client was told succeeded is in the
   history of the ref it landed on, and each of its changesets' keys is there
   exactly once. The ledger is read before the ref heads, so whatever it lists
   was acknowledged before the heads were read. A branch merged into main
   moves its keys to main's expectation before the branch is deleted, and one
   deleted unmerged is expected nowhere and forbidden in main. Duplicates are
   violations even for keys nobody was told about.
2. **Content.** The first time an acknowledged land is seen, its changesets
   carry the message and exactly the paths the agent gave them; a
   fast-forwarded land has exactly the contents. (A replay merges text, so a
   rebased copy of a file other agents also changed may differ, and a replay
   may drop a change the head already holds; those cases are compared by
   path.)
3. **Silent resolution.** A stack whose land answered conflicts is never in
   main.
4. **History is readable and consistent.** Every changeset reachable from any
   ref reads back with the id it was asked for (it is hashed again), with
   `generation = 1 + max(parent generations)`. Each round re-reads a random
   sample of old changesets and compares them with what was walked, lists the
   files of two old changesets and checks a sample of their blobs, and
   fetches a few blobs and hashes them. After the swarm stops everything is read again and every blob
   any changeset names is confirmed present.
5. **Ref log.** The newest 3000 entries each round, all of them at the end:
   sequence numbers decrease, each entry's `old` is the next entry's `new`
   for the same ref, every ref read before the log is the `new` of some entry,
   the oldest entry of each ref starts from nothing, and at the end the newest
   entry of each ref is the ref (or the ref is absent after a delete) and every
   land whose answer arrived at the first try is a ref log entry of main.
6. **Search.** A token planted by an acknowledged land is found exactly once,
   in the right agent's notes file, when searching the changeset that landed
   it (mostly recent ones, which cost a delta rather than a tree); a token
   never planted is not found; every line of a search result contains the
   query.
7. **Path history.** The log of a sampled path at main's head, restricted to
   the newest 600 generations, equals a scan of those changesets' changes
   (merge commits excluded, whose path history has its own rules), and
   `last-change` is no older than the newest changeset in the scan.
8. **Blame.** Every token line of a sampled notes file is blamed on the
   changeset that wrote it, and blame has one entry per line of the file.
9. **GC.** After every GC, the files of every ref's head and a sample of every
   blob any walked changeset ever referenced are present
   (`POST /v1/blobs/missing` is empty). GC is only ever allowed to delete
   unreferenced blobs.
10. **Tags and conflict state.** Tags still point where they were created and
    deleted ones are gone. Each CLI working copy ends with no merge,
    cherry-pick or conflict in progress.
11. **Rename detection.** A changeset that renames a file it added reports the
    rename (exact) in `diff`.
12. **Policy.** An unsigned or unreviewed land on `release` is refused; a
    signed and reviewed one lands.

Reads also verify what they can as they go: a file's bytes hash to the tree
entry that listed them.

### Failure
A run fails if any invariant breaks; if any 5xx or any dropped connection
occurs outside a server restart (those inside a restart, and the margin of
three seconds after it, are counted and attributed to it, not hidden); if an
answer was unexpected; if a request cannot finish in 90 seconds or an agent
makes no progress for `--stall-timeout` (a hang or deadlock); if Postgres
reports a deadlock; if the client-side land p99 exceeds `--max-land-p99`
(5 s, the suggested alert in OPERATIONS.md); if the server's RSS grows
faster than `--max-rss-slope-mib-h` (a least-squares fit over the second half
of the run and the last server incarnation, so a restart's reset is not a
slope, and judged only when that window is at least five minutes, because
warm-up looks like a leak); or if `--baseline` names an earlier report and p99
or throughput regressed by more than `--max-regression`. The first violation
stops the run (`--no-fail-fast` keeps going), the report is still written and
the database and blobs are kept so the failure can be inspected.
Things that look off but are legitimate are warnings: 408s, GC that deleted
nothing, conflicts never exercised.

### Reports
`report.json` has the configuration and seed, every violation, throughput,
latency per route, land latency per minute, the server's own metrics
(Prometheus scrapes merged across restarts: land outcomes, attempts, CAS
losses, derive latency by kind, cache hit ratio, requests limited), a time
series every `--sample-interval` of server RSS and CPU (from `/proc`), the
database size (`pg_database_size`) and the blob directory size, per-table
sizes, GC and warmup runs, restarts and the counts of each invariant check
(with the time each phase of a round took). `summary.txt` is the same in
twenty lines. `server.log` and `gc.log` come with it.

### CI
`.github/workflows/soak.yml` runs weekly (30 minutes) and on demand
(`workflow_dispatch` with duration, agents, chaos and seed), against a
Postgres service, and uploads the report, the summary and the logs. It does not
run on pull requests and is not part of the `test` job.

## What the first runs found

**Fixed: `blame` read the whole history.** `blame_lines` walked every
first-parent ancestor, with a tree lookup at each step, down to the changeset
that last changed the file; for a file untouched since the import that is the
entire history. At 4400 changesets one blame took 1.4 s (cold or cached, the
cache was consulted only at changed points), 3.4 s on average under the swarm,
and the CPU it used queued every land behind it: land p50 went from 5 ms to
6 s over seven minutes, 48 requests hit the request timeout and an agent's land
got no usable answer for 90 s (`stall`). Blame now asks the path-history index
for the newest changers of the file and rides the first-parent skip list down
to the lowest ancestor at or above their generation, so only changes are
visited; it also reads its cache for the requested changeset first. 12 ms for
the same request. `blame_skipping_agrees_with_a_full_walk` compares it with the
old walk over seeded histories with reverts, deletions, re-creations and
merges (and fails if the jump is made too far), and
`blame_of_an_untouched_file_does_not_walk_the_whole_history` pins the cost by
counting the changesets examined (the reference walk reads over 300, the
skipping walk 1).

**Recorded, not fixed: a land costs time in proportion to how far behind main
its stack is.** Inside the per-ref land lock, `is_ancestor(cs, head)` falls
back to a search over every ancestor above `cs`'s generation, `landed_copy`
reads up to 1024 of them (three reads each), and the replay's `merge_base`
walks the same stretch whenever a merge commit lies in it (the
documented limit in `PERFORMANCE.md`). A fast-forward is 5 ms. A stale land is
not: with the first version of the workload (a branch lived for dozens of
agent iterations, so merging it landed a stack 500 to 700 changesets behind)
the stale lands held the lock for 0.5 to 2 s in `is_ancestor`, 0.6 to 1.1 s in
`landed_copy` and up to 3.6 s in the replay, on Postgres on 4 vCPUs also
running the server, fifty agents and the checks; a few per minute were
enough that p99 reached the 8 s request timeout and, in two runs, a land
stalled 90 s. The reads are cheap on their own (1 to 2 ms under that load); the
count is the problem. Directions, none done: keep changesets and
generations in a bounded in-process cache (both immutable), answer
`is_ancestor` from `last_merge` and the skip list where no merge lies above,
and find a land's earlier copy through an index from origin to copy instead
of a scan. The workload now keeps a branch for a few iterations (an agent that
has one works on it three times in four), which is also what agents do, and
the swarm then runs at 6 lands per second with p99 near a second;
`--think` and `--agents` find the edge again.

**Found by the harness's own runs (fixed in the harness).** A check that read a
branch's ledger entry after the refs it compared with saw a merge that had
landed in between (`ack_lost`); entries now carry the ledger version of their
last change and a round skips the ones newer than its snapshot. A changeset
that "modifies" a file to what it already was lists the path but is, rightly,
not in the path's history (`path_log`); the check now asks `diff` before it
calls the index wrong. A repeated `leep switch -c` after a lost answer finds the
branch already created, the known non-idempotence of ref creation in ADR 0021;
the CLI agent switches to it. Blame follows first parents, as documented, so a
line merged in belongs to the merge commit.

## Consequences
- Lands to one ref are serialized in the server (ADR 0004), so one hot ref
  saturates at the rate one land takes. A swarm whose think time is shorter
  than N times that cost queues: land latency becomes N times the service
  time, and the p99 bound fails. The recipes therefore size `--think` to the
  machine; `docs/PERFORMANCE.md` records the capacity found.
- The checker's own load is part of the run (a round of search, blame and path
  checks costs the server real work). It is bounded per round and reported
  per phase.
- Memory of the harness grows with history (about 100 bytes per changeset
  and per acknowledged land), so an eight-hour run at full speed needs a few
  hundred MiB. Its RSS is in the report.
- A violation is reproducible by seed in plan, not in interleaving; the
  stores are kept to inspect it.
- Blame is checked as documented: it follows first parents, so a line that
  came in through a merge is blamed on the merge commit and the check accepts
  that (the soak's first chaos run flagged it before the check knew).
- The two-week exit criterion of M5 is separate: the harness and the
  schedule exist; the clock is the maintainer's.

## Not covered
- One server process at a time. Compare-and-swap losses between instances
  (`leep_land_cas_losses_total`) are therefore always zero, and the soak says
  nothing about a balancer in front of several servers.
- One hot ref. Lands on `main` and a few hundred short-lived branches, not
  hundreds of long-lived refs; and `Repo`'s per-ref land lock map has an entry
  for every ref name ever landed on, never removed (about a hundred bytes
  each), which only a run with millions of branch names would show.
- Object stores other than the filesystem, and several repositories.
- Power loss (the crash test of ADR 0021 kills processes, not machines).
