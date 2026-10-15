# 0021. Fault injection and crash tests
Status: Accepted
Date: 2026-10-15

## Context
Leep's safety claims rest on a few rules: refs move only by compare-and-swap,
everything else is written idempotently, and GC marks from every changeset
before it deletes (ADR 0004, 0008, 0012, 0018). The existing tests check those
rules with stores that never fail, so they say nothing about the cases that
matter in production: a store call that errors, one that applied and then
reported an error (a lost commit acknowledgement), a request cancelled by the
server's timeout, a read that returns damaged bytes, and a process killed at
an arbitrary instruction. The M5 hardening list asks for storage fault
injection and for crash-consistency tests.

## Decision

### Fault-injecting stores live in `leep-testkit`, a dev-only crate
`FaultyBlobStore` and `FaultyMetaStore` wrap any `Arc<dyn BlobStore>` or
`Arc<dyn MetaStore>` and consult a shared, seeded `FaultPlan`. The plan can
fail a call with a probability per operation, or exactly at the nth call of an
operation, and each fault is one of:

| Fault | Meaning |
|---|---|
| `Fail` | The backend never sees the call; the caller gets `Error::Storage`. |
| `FailAfter` | The backend applies the call and the caller still gets `Error::Storage`: the ambiguous commit for `cas_ref_logged` and `put_changeset`, a put that errored after writing for blobs. |
| `Delay` / `Stall` | Sleep (tokio time, so tests can pause the clock) before the call, or after it has applied. A caller that gives up during a stall holds an applied write it never heard about. |
| `Corrupt` | Flip one seeded bit in the bytes a read returns. |

`jitter` makes every call yield to the scheduler a seeded number of times, so
in-memory stores (which never suspend) still interleave tasks on a
single-threaded runtime, reproducibly. `cancel_after_polls` drops a future
after a given number of suspensions, which is what a request timeout or a
client disconnect does to a handler.

A crate rather than a `testing` feature of `leep-core`: a feature would put
the wrappers one `--features` flag away from every binary, and Cargo unifies
features across a workspace build, so "normal builds do not include them"
would hold only by convention. `leep-testkit` is `publish = false`, is a
`[dev-dependencies]` entry of the crates that test with it, and no binary
depends on it. `leep-core` cannot dev-depend on it without a dependency cycle,
so the `check_*_contract` functions stay in `leep-core::memory`, unchanged: a
quiet wrapper must still pass them, and a test per backend says so. The
contracts describe correct behaviour without faults and are not weakened.

Blob "torn writes" are modelled as `FailAfter`, because that is the only
partial state a blob store can show: the filesystem store publishes by rename
and object stores publish whole objects, so a prefix of a blob is never
visible under its address. For the filesystem store the crash between writing
the temporary file and renaming it is tested directly, by abandoning puts at
every suspension point and by planting what a crash leaves behind.

### Properties checked
In `leep-derive` (`fault_tests`), over in-memory stores, a seeded sweep of 25
seeds by default (`LEEP_FAULT_SEEDS=n`, `LEEP_FAULT_SEED=n` to replay one):

- **Land.** Twelve agents land stacks through three server instances while
  commits fail, answer ambiguously, stall past a client timeout and are
  cancelled. Afterwards every ref reaches only readable trees and blobs, every
  reported success is an ancestor of the head, every file of every stack is
  present, each commit appears in history exactly once, and the ref log
  replays to the refs. Separate tests script the ambiguous commit and the
  cancelled-after-commit request, and retry the land.
- **Derived data.** Derivation under failing reads and writes leaves only
  missing rows, never wrong ones, no key is written twice with different
  values, and retrying completes. Every single-bit flip of every stored row
  (and of every tree node) of a small history is applied, and the answers of
  every query the rows feed are compared with the truth: nothing panics, and
  for the kinds that can be verified a flip is an error or changes nothing.
- **GC.** Condemn and collect run under failing and damaged reads, ambiguous
  writes and kills at random suspension points while clients upload, reuse and
  commit the same blobs. No stored changeset ever loses a tree node or a blob;
  a fault-free run afterwards collects exactly the garbage; and no blob is
  left unusable by a dead run.
- **Ref log.** The memory store keeps a ref and its log entry under one lock;
  `leep-meta` abandons ref moves at random suspension points on SQLite (a
  file database) and Postgres and checks that the log replays to the refs, so
  no move is unlogged and no entry lacks its move.

In `leep-e2e` (`tests/crash.rs`), with the real binaries on SQLite and on
Postgres: see "Crash test" below.

### Crash test
Six HTTP agents and two `leep` CLI agents commit and land continuously
against a real `leep-server` (`--request-timeout 2s`). A seeded schedule
SIGKILLs the server; a quarter of the HTTP attempts also hang up on one of
their requests a few microseconds after sending it. After every kill the test
opens the stores itself and checks: every stored changeset still has its tree
and its blobs, every ref reaches only stored changesets, the ref log replays to
the refs, and every land a client was told succeeded is in `main`'s history.
Per round it does one of three things while the server is down (so `gc
--unsafe-no-fence` is justified, ADR 0012):

1. `gc --dry-run`, then a real condemn-and-collect with no fence;
2. plant 400 orphans, condemn them, SIGKILL `gc collect` partway through,
   check, finish the collect, and upload the same bytes again;
3. leave the store as the kill left it, and, after the restart, run a fenced
   `gc --fence 4s` (above the 2s request timeout) beside live traffic.

At the end the agents stop and the test checks, exactly: every commit any
client was told landed appears once in `main`'s history with its file intact,
there is one ref move per land (a retried land did not move the ref twice), no
request got an answer other than success, a retry-class error (408, 409,
connection loss) or "missing blob", and a fresh `leep` checkout reads
everything back through the server.

Rounds, agents and the seed are `LEEP_CRASH_ROUNDS` (default 3 kills per
backend), `LEEP_CRASH_AGENTS` and `LEEP_CRASH_SEED`. The seed fixes the kill
and GC schedule and each agent's workload and which requests hang up; the OS
schedules the threads, so a seed replays the plan, not the interleaving. The
default run takes about a minute on both backends.

## Bugs this found, and the fixes
1. **A retried land could land its stack twice.** If the commit of `land`'s
   compare-and-swap applied but its answer was lost (an error after commit, a
   request cancelled by the timeout, a killed server), the client's retry
   replayed the stack onto a head that already contained it, adding an empty
   copy of every commit. `land` now re-drives a failed commit from a fresh
   read (at most three failures in a row), and before replaying it looks for a
   rebased copy of the changeset that the server itself recorded
   (`rebased-origin-v1`, never the forgeable `leep.rebased_from` extra) among
   the head's ancestors above the changeset's generation, scanning at most
   1024 changesets. Past that window a late retry can still land twice.
2. **Damaged tree nodes and history nodes were read as valid.** Both are
   content-addressed but were not checked on read. GC marks reachability from
   tree nodes, so a damaged node could leave live blobs unmarked. `read_tree`
   and `read_history_node` now compare the bytes with their address and fail
   with `Error::Storage`.
3. **A collect killed after deleting a blob could wedge the hash.** If the
   delete applied and "deleted" was never recorded, the blob's "deleting" row
   stayed. The blob is no longer listed, so no later collect revisited it, and
   once a client uploaded it again (rescuing it) the stale row still made
   every upload wait 30s and fail with 409, and made `missing` and `create`
   reject it, for as long as the blob was not re-condemned. Collect now closes
   the stale announcement for every blob it lists and does not delete.

## Known gaps, recorded rather than fixed
- Most derived rows carry no checksum. The fault tests verify `root_tree`,
  `history-tree-v1` and `history-node-v1` (a flipped bit is an error), but a
  flipped bit in a `generation`, `skiplist`, `last_merge-v1` or trigram row
  can change an answer silently: a wrong generation makes `is_ancestor` wrong,
  which can make `land` report success without moving the ref. Detecting that
  needs a checksum per row, a persisted-format change for the M6 repair tools
  ("verify, re-derive or drop derived data by kind"); until then rely on the
  database's own page checksums (Postgres `data_checksums`).
- File blobs are not re-hashed when read, and `CachedBlobStore` keeps whatever
  its inner store returned.
- The filesystem store fsyncs the temporary file but not the shard directory,
  so a power failure (not a process kill) can lose the rename of a put that
  was acknowledged. Interrupted puts leave `.tmp-*` files that are never
  served or listed and never removed.
- `POST /v1/refs/{name}` is not idempotent: a client that gets an error after
  the swap applied and repeats it gets 409. Clients must read the ref to
  learn what happened (`leep undo` already does).
- `sqlite::memory:` (tests only) keeps its database in one connection, which
  sqlx discards when a query is cancelled, taking the database with it.

## Consequences
- `leep-testkit` is a new workspace member that only tests depend on. New
  backends (`add-storage-backend`) should run their contract through a quiet
  wrapper and add a fault-tolerance test for what an error after commit means
  for them.
- `land` makes a few more reads on the replay path (the copy search) and may
  return a rebased copy's id for a changeset that was landed earlier.
- `read_tree` hashes every node it reads. Nodes are small and BLAKE3 is
  fast; the benchmarks in `docs/PERFORMANCE.md` should be rerun when that is
  next measured.
- Collect writes one more derived row per stale announcement it closes, and
  none in the ordinary case.
- A change to land, GC or the stores should run `LEEP_FAULT_SEEDS=300 cargo
  test -p leep-derive fault_tests` and the crash test with several seeds
  before it merges.
