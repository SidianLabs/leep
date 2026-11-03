# 0081. Operation budgets and a ratio bench guard performance in CI
Status: Accepted
Date: 2026-11-03

## Context
Performance regressions were caught only by benchmarks run by hand on a
shared 4-vCPU box. CI ran the tests and compiled the benches
(`cargo bench --no-run`), nothing more.

The first version of #253 (ADR 0079) raised land-only throughput by 60%
and passed every test, and in the 1,000-agent mixed swarm it took the
create p99 from 101 ms to 2.7 s and had 13,247 lands not admitted against
none. The causes were changes of resource shape, not of speed: a create
made two metadata calls at once instead of one, and a land batch's
skiplist plan, started before the batch's own reads, asked the
four-connection land pool for five or six at once. Both are counts, and
both are visible in process on any machine. Wall time on a shared machine
is not: on the dev box, with other builds running, one build's land-bench
p99 differed by 2x between two short runs and its swarm create p99 by 4x,
and shared CI runners are noisier still.

## Decision
Two layers, both in CI.

### 1. Operation budgets (deterministic, in `cargo test`)
`leep_testkit::budget::Budget` wraps a repository's `BlobStore` and
`MetaStore` in one value. While started it records, per store method and
per derived kind, the calls, keys and bytes, and the most calls in flight
at once: overall, per `Lane` (each Postgres pool) and per actor (a
task-local label a test gives each concurrent flow). Every method forwards
to the same method of the inner store, so the inner store sees the flow's
calls in their own shape; each counted call yields once, so calls a flow
makes concurrently overlap even on the memory store.

`crates/leep-derive/src/budget_tests.rs` runs each hot flow on a new
`Repo` over filled memory stores (nothing cached, the case where a server
makes the most calls) and checks it against `BUDGETS`, one line per flow
and metric, fixed or `base + per * k`:

| Flow | Lines |
|---|---|
| create (one file, 300-deep history) | meta calls, meta in flight (1), blob calls |
| 8 creates at once, one actor each | meta in flight per create (1) |
| land batch of k (8 and 32) on a 1,024-deep history | meta calls (`37 + k`), blob calls, land lane in flight (4) |
| log of 100 | meta calls, meta in flight |
| cold blame of a file of v versions on a 2,000-deep history | meta calls (`9 + 3v`), meta in flight (1) |
| search of a 256-file tree, index derived | meta calls, blob calls |
| upload of 1 and of 16 blobs | meta calls, meta in flight, blob calls |

The flows run on a current-thread runtime, so every count is the same on
every run and machine, and the lines are exact. The check fails both ways:
over a line, and more than 20% under one, with a message naming the line
and what to do. A gain is written into the table in the change that makes
it, so a later regression cannot spend it unseen. Raising a line is
allowed when the growth is intended, and the commit says why.

The land lane's line is 4, the land pool's default size (ADR 0067), not 2:
a batch leader's four reads after the head (fence flag, recorded lands,
prefetch, origins) overlap by design. The skiplist plan runs after them.

With #253's first version reapplied to this tree, the create line reads 2
in flight (and 2 per create with eight at once), and the land lane reads 5.

### 2. A ratio bench (on a real server, gated on counts)
A `perf` job, on pull requests only and on a runner of its own, builds the
merge base's `leep-server` and this change's (one target directory, so
dependencies build once), and `scripts/perf-ratio.sh` runs them in turn on
the same runner, A B A B, each on a fresh Postgres database, pinned to
CPUs 0-1 with the generators on 2-3. Each run is an 8 s `leep-land-bench`
at 32 agents and a 20 s `leep-swarm` of 200 agents (500 ms think time)
against a server with `--meta-pool-size 8`, which presses the pool as
1,000 agents press 32. The generators are this change's on both sides.
About 2.7 minutes of runs.

`leep-perf-compare` judges the four runs. Counts the swarm and the servers
report fail when both B runs exceed twice the larger A run plus a slack:
lands not admitted, 409s, stalled agents, CAS losses
(`leep_land_cas_losses_total`), discarded pipelined attempts
(`leep_land_pipeline_total{outcome="discarded"}`), reads shed
(`leep_read_class_shed_total`), and the most connections in use per pool
(`leep_meta_pool_connections`, sampled each second). Ratios of means fail
outside wide bands: throughput below 0.7x, p99 above 2x, statements and
bytes per land above 1.2-1.5x. A check whose two A runs differ by as much
as it allows is reported *inconclusive* and does not fail the job.

A new counter, `leep_meta_statements_total{op, pool}`, counts each query
or transaction the SQL metadata stores send. Statements per land does not
depend on the hardware; in two local A/A runs it stayed within 2% on the
land bench.

## Consequences
- A change that makes a hot flow do more storage work, or more of it at
  once, fails `cargo test` with the call that grew. Changes that add work
  on purpose edit `BUDGETS` beside the code.
- Ad hoc counting stores move onto `Budget`: `deep_blame_tests` now counts
  with it, and `a_create_makes_one_metadata_call_at_a_time` became the
  `create` and `create x8` budgets.
- The budgets see only what the memory store shows: the shape and number
  of calls, not their cost on Postgres (plans, locks, row sizes). The ratio
  bench covers those, coarsely.
- The ratio bench does not catch #253 itself: with both halves of its
  first version as B, on a fresh store a few thousand changesets deep, every
  check passed (PERFORMANCE.md, "Ratio bench in CI"). That regression needed
  a 500,000-deep store and 1,000 agents for minutes. The budgets catch it;
  the bench is for collapses and for costs only Postgres shows.
- The ratio bench adds a second job to every pull request: two release
  builds (shared dependencies) and about three minutes of runs, in
  parallel with `test`, so no wall time. It cannot see what needs a deep
  history or many minutes (ADR 0079's one-million-deep store, slow leaks);
  the soak and the manual benchmarks in docs/PERFORMANCE.md still do.
- Before this lands, a merge base has no statement counter; those checks
  report "skipped".
- An inconclusive verdict says the runner was noisy; rerun the job.

## Alternatives considered
- **Gate on wall time.** Short runs of one build vary 2-4x on a shared
  machine; any band wide enough to stay quiet misses #253's land-only side
  entirely and passes changes that halve throughput.
- **Run the bench on a dedicated machine.** Steadier, and worth having for
  baselines, but a second system to keep running, and it still would not
  say which call grew.
- **Budgets with slack instead of exact lines.** The flows are
  deterministic, so slack would only let small regressions pile up.
