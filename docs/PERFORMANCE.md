# Performance

Benchmarks live in `crates/leep-derive/benches/repo_ops.rs` (criterion). They
use `leep_core::memory` stores, so they measure Leep's algorithms and not a
database or object store. Real backends add per-call latency on top; what
matters here is how cost grows with repository size.

## Running

```sh
cargo bench -p leep-derive                      # default set, about 2 minutes
LEEP_BENCH_LARGE=1 cargo bench -p leep-derive   # adds 100k files / 100k changesets, about 4 minutes
cargo bench -p leep-derive -- land              # filter by name
cargo bench --no-run -p leep-derive             # compile only (what CI does)
```

Criterion builds with the release profile. Fixtures are built once per group.
Repositories use about 1000 files per leaf directory, nested three levels
deep (`a*/b*/c*/f<i>`). Large repositories are imported one directory per
changeset, so no single changeset parent is huge (see "Fixture note").

To record a baseline, run the large set on a quiet machine and update the
table below. Use `cargo bench -- --save-baseline <name>` and
`--baseline <name>` to compare a change against an earlier run locally. Delete
`target/criterion` afterwards if disk is tight.

## Results

Machine: 4 vCPU (`nproc` = 4), Intel Xeon @ 2.80GHz, 15 GB RAM, release
profile, in-memory stores. Median of 20 samples (10 for `log`).
Path-history rows (`cargo bench -p leep-derive -- 'log_path|last_change|derive_history'`) use a 10k-changeset linear history in which `d0/f` changes every 10th changeset and `rare/f` every 1000th; the `log` rows without a path (`-- log_10k_history`) use the same history and were measured before and after in one session on a shared 4 vCPU Xeon @ 2.10GHz, so compare them with each other rather than with the other rows. Numbers are
indicative; expect roughly +/-15% run to run.

| Benchmark | 1k | 10k | 100k |
|---|---|---|---|
| `create_changeset` (one file, files in repo) | 498 us | 457 us | 594 us |
| `land` fast-forward (files in repo) | | 3.1 us | 2.5 us |
| `land` replay, head moved by 1 (files in repo) | | 4.0 ms | n/a |
| `land` replay, head moved by 10 | | 3.1 ms | 3.7 ms |
| `land` replay, head moved by 100 | | 3.0 ms | 3.5 ms |
| `diff` one changed file (files in repo) | | 1.05 ms | 1.02 ms |
| `is_ancestor` root of tip (changesets) | 1.5 us | 1.6 us | 2.2 us |
| `merge_base` tip vs. branch from midpoint (changesets) | 5.0 us | 5.5 us | 6.7 us |
| `log` path filter, limit 10, old scan (10k changesets) | | 489 us | |
| `log` path filter, limit 10, path-history index | | 67 us | |
| `log` path filter, limit 1000, old scan (reads all 10k) | | 79 ms | |
| `log` path filter, limit 1000, path-history index (1000 matches) | | 9.3 ms | |
| `log` of a path changed 10 times, old scan | | 95 ms | |
| `log` of a path changed 10 times, path-history index | | 45 us | |
| `log` path filter, no match, old scan | | 75 ms | |
| `log` path filter, no match, path-history index | | 3.1 us | |
| `last_change` of a file, a rarely changed file, a directory | | 4.0 / 4.2 / 3.7 us | |
| derive the history index (per changeset, one file each) | | 36 us | |
| `log` without a path, limit 100 (before / after ADR 0030) | | 95 us / 214 us | |
| `log` without a path, limit 10,000 (before / after ADR 0030) | | 22.7 ms / 59.7 ms | |
| `log` with an author filter matching nothing, all 10k (before / after ADR 0030) | | 8.6 ms / 35.5 ms | |

## Rename detection

`cargo bench -p leep-derive --bench repo_ops -- detect_renames` times the pure
pairing of 1000 deleted and 1000 added files of about 30 lines (ADR 0016), the
default limit's worst case. Contents are in memory, so blob reads are not
included. Same machine and profile as above, median of 10 samples.

| Benchmark | Time |
|---|---|
| exact: every added file is an unchanged deleted file | 0.4 ms |
| similar: every added file is an edit of one deleted file (1 000 000 pairs scored) | 122 ms |

Exact detection is a hash map pass, so it stays linear however many files move.
Similarity is quadratic in the candidates, which is why it stops at 1000 x 1000
pairs; a followed `log` pays it once per changeset that created the file, over
that changeset's own deleted and added files only.

## Code search

`crates/leep-derive/benches/search.rs` compares an indexed search with a full
scan of the same revision (`use_index: false`, which also checks that both
return identical matches before timing). Same machine and profile as above.
`cargo bench -p leep-derive --bench search` takes about 5 minutes;
`LEEP_BENCH_SEARCH_FILES=4000` shrinks the repository.

Repository: 20 000 files (34.8 MiB) in 400 leaf directories of 50 files, each
directory drawing its identifiers from its own 300-word vocabulary, imported as
one changeset, in-memory stores. Median of 10 samples, limit 100 unless the
matches are fewer.

| Query | Matches | Indexed | Full scan | Speedup |
|---|---|---|---|---|
| word in no file | 0 | 7.2 ms | 86 ms | 12x |
| token in 3 files | 3 | 6.8 ms | 83 ms | 12x |
| word of one directory (100 matches, truncated) | 100 | 8.3 ms | 45 ms | 5x |
| same, ignore case | 100 | 9.4 ms | 57 ms | 6x |
| regex `word_[a-z]+\(\d+\)` | 44 | 17 ms | 102 ms | 6x |
| regex `(word\|quux_needle)` | 100 | 10.5 ms | 55 ms | 5x |
| common word in every file, limit 100 | 100 | 0.66 ms | 0.10 ms | 0.15x |
| regex whose trigrams are in every file, no match | 0 | 155 ms | 94 ms | 0.6x |
| regex with no required trigram | 0 | 92 ms | 91 ms | 1x (same path) |
| cold index derivation | | 1.0 s | | |

Index size for this repository: blob rows 36 MiB (1.9 KB per file, about 1.05x
the text, because the synthetic text is random letters), 521 tree rows 7.6 MiB
(all sparse). On source code the rows are about 30% of the text (measured on
this repository's own text files).

What the numbers say:

- A selective query reads only the files that can match: the 3-file token scans
  3 of 20 000 files (38 directories pruned, 147 files pruned by their sets). The
  remaining 7 ms is walking 521 directories (a tree node and a filter probe
  each) and probing the rows of the files in surviving directories. A scan of the
  same tree costs 83 ms of CPU here only because the data is in memory; against
  object storage the scan also pays one fetch per file, so the gap grows with
  fetch latency, while the indexed search pays for fetches only on candidates.
- The two cases where the index loses are the cost of consulting it when it
  cannot help. A common word with a small limit is slower (0.66 ms against
  0.10 ms; both are negligible) because the indexed path reads the root filter
  first. A query whose trigrams occur in every file reads a row for each of the
  20 000 files and then scans them all, 1.7x a plain scan. A later optimization
  could stop consulting rows once nothing has been pruned for a while.
- The walk is lazy: with enough matches it stops reading directories, which is
  why the "common word" case is sub-millisecond for both modes.
- Cold derivation is 1.0 s for this repository (20 000 blob rows and 521 tree
  rows, with 16 blob fetches in flight). It is paid once per distinct blob and
  directory, by the first search that needs it or by `leep-server derive-search`.

## Verify

`leep-server verify` (ADR 0025, `docs/OPERATIONS.md`) over a store of 10 000
changesets built by the ignored test `verify_ten_thousand_changesets`
(`cargo test --release -p leep-e2e --test verify -- --ignored --nocapture`):
two files changed per changeset across 40 x 25 x 60 directories and files,
a merge of two side branches every 200 changesets, SQLite metadata and a
filesystem blob store, with the path-history index, last-merge rows, the search
index for the tip and two blame rows already derived. 4 vCPU Intel Xeon
@ 2.10GHz, 15 GB RAM, a machine shared with other builds, release build.

| Mode | Time | Peak RSS |
|---|---|---|
| `verify` (sample of 256) | 7.0 s | 36 MiB |
| `verify --full` | 51 s | not measured |
| `verify --full --rehash` | 52 s (59 s on a busier run) | 104 MiB |

Reachability is the part that scales with history: every changeset is read
twice and every new tree node once. `--full` adds a recomputation of every
derived row, which costs about what deriving them did (the warm-up above took
44 s). The files here are small, so `--rehash` adds little; it is bound by
reading the blob store.

## Backup and restore

`leep-server backup`, `backup-verify` and `restore` (ADR 0026,
`docs/OPERATIONS.md`) over a store of 5 000 changesets built by the ignored test
`backup_and_restore_timing` (`cargo test --release -p leep-e2e --test backup --
--ignored --nocapture`): a linear history, three small files changed per
changeset, 32 441 derived rows (the search index for the tip is warmed), 50 000
blobs and tree nodes (21.4 MiB) on a filesystem blob store, and a 8.3 MiB
metadata snapshot. The server is not running and nothing else is writing;
backup, restore and the stores share one disk. Same 4 vCPU shared machine as
above, Postgres 16 on the same host, release build. One run each.

| Step | SQLite metadata | Postgres metadata |
|---|---|---|
| `backup`, first (empty pool: all 50 000 blobs copied, then the check) | 15.7 s | 15.2 s |
| `backup` again, nothing new (two listings, the snapshot, the check) | 8.8 s | 8.0 s |
| `backup-verify` | 3.9 s | 3.8 s |
| `backup-verify --rehash` | 3.8 s | 3.8 s |
| `restore` (copy 50 000 blobs, load metadata, check) | 15.3 s | 16.6 s |
| `verify --full --rehash` on the restored stores | 20.3 s | 33.1 s |

Reading the numbers: the blob copy is bound by per-object cost (50 000 files of
about 450 bytes), so expect time to follow blob count on a filesystem, and
bytes and round trips on S3, rather than repository size in MiB. The second
backup shows the fixed cost of a run when nothing changed: listing both blob
stores, streaming the metadata into the snapshot file, and the closure check,
which is the same history walk as `backup-verify` and scales with changesets and
tree nodes. `--rehash` adds little here because the files are tiny. For a
restore drill, plan on `restore` plus `verify --full --rehash`: about 36 s on
SQLite and 50 s on Postgres for this store. The metadata load is one transaction
of batched inserts (500 rows per statement). `verify --full` is slower on
Postgres than on SQLite here because it reads every row over a connection
rather than from a local file; that is `verify`'s cost (see above), not the
restore's.

## Soak run

`leep-soak` (ADR 0022, `docs/OPERATIONS.md`) runs a swarm against a real
`leep-server`, so these numbers include Postgres, the filesystem blob store,
HTTP and 52 competing clients, which the benchmarks above leave out.

Machine: the 4 vCPU Intel Xeon @ 2.80GHz with 15 GB RAM used above, a virtual
machine that was not otherwise idle (load average 1 to 3 before each run).
Postgres 16.15 on the same host with its default settings (`shared_buffers`
128MB, `fsync` on), `leep-server` release build with `--cache-bytes` 32 MiB and
`--request-timeout 8s`, blobs on the root disk, and the harness and every agent
on the same four cores. Workload: `leep-soak --duration 16m` (defaults: 50 HTTP
agents and 2 `leep` CLI agents, mean think time 6 s, a 3025-file repository,
signatures and review enforced on `release`, GC every 5.3 minutes and
`derive-search main` every two). The chaos column is `--duration 6m --chaos
--chaos-restarts 4`. One run each, not a statistical sample; expect the same
shape and noise of tens of percent.

| | 16 minutes | 6 minutes with restarts |
|---|---|---|
| Lands acknowledged (lands per second) | 5691 (5.9) | 2164 (6.0) |
| HTTP requests per second | 132 | 139 |
| `land` request p50 / p95 / p99 / max | 4.9 / 551 / 1270 / 2753 ms | 6.1 / 777 / 1373 / 1752 ms |
| Whole land including conflict redo, p50 / p99 | 52 / 1260 ms | 54 / 1413 ms |
| Server view: attempts per land, CAS losses | 1.00, 0 | 1.00, 0 |
| Server view: land mean, of which waiting for the ref lock | 85 ms, 52 ms | |
| Fast-forward / replayed lands | 3562 / 1808 | 1286 / 758 |
| Conflicts answered (stacks redone) | 5 | 1 |
| 5xx | 0 | 0 |
| Restarts (graceful, SIGKILL); requests attributed to them | | 3, 1; 153 (151 refused or dropped connections) |
| Server RSS first, last, max | 38.8, 120.5, 122.0 MiB | |
| RSS slope over the second half (limit 64 MiB/h) | 14.2 MiB/h over 475 s | not judged (restarts) |
| Server CPU, mean; blob cache hit ratio | 0.61 cores; 98% | 0.60 cores; 96% |
| Database size | 8.5 to 418 MiB | 8.4 to 147 MiB |
| Blob directory | 6.8 to 153 MiB | 6.8 to 62 MiB |
| GC runs (changesets scanned), time each | 2 (3746, 8091), 46 s and 78 s | 2 |
| Blobs condemned and deleted by GC | 272 | 97 |

The land p99 is flat across the sixteen per-minute windows (0.5 to 1.6 s, with
a median of 4 to 11 ms in each), so latency did not creep as history grew to
8000 changesets. The p95 and p99 are mostly lands that replayed: a land that
lost a race replays its stack under the ref's lock (mean 52 ms of waiting), and
a handful a minute are stacks some dozens of changesets behind.

Per route (same run; p50 / p99): `changeset.create` 18 / 74 ms, `blobs.put` 2.3 /
9.6 ms, `file` 0.7 / 3.8 ms, `tree` 0.7 / 3.7 ms, `ref.get` 0.7 / 3.8 ms,
`last-change` 12 / 272 ms, `log` by path 19 / 260 ms, `blame` 27 / 328 ms, `search`
(token or identifier at a recent head) 142 / 590 ms. Deriving the path-history
index takes 15.6 ms per changeset and the skip list 3.9 ms; generation,
last-merge and trigram rows are under a millisecond. `derive-search main` takes
0.03 to 0.5 s each time. A `leep` checkout (`init`) of the repository takes about
5 s, a `sync` 0.3 s, a `commit` 55 ms.

**Capacity of one ref.** A fast-forward land is 5 ms, but lands to one ref are
serialized and a replay costs more, so this machine sustains about 6 lands per
second to one ref at the latencies above; 50 agents with a mean think time of 6 s
offer that. The same swarm with a first version of the workload (branches that
lived for dozens of iterations, so merging one landed a stack hundreds of
changesets behind) queued: the land lock was held for seconds per stale land,
p99 reached the 8 s request timeout and an agent's land stalled for 90 s.
ADR 0022 has the breakdown. The cost of a stale land is a few reads per
changeset of distance, so it grows with how long work stays unlanded, not with
the size of the tree.

**Growth.** The database grew 72 KiB per acknowledged land (418 MiB over 5691
lands), the blob directory 26 KiB. In the 6-minute run's 4389 changesets the
`derived` rows were: `tree-trigrams-v1` 74 MB (7832 rows of 9.9 KB, the search
index of each changed directory, built by searches and warmups), `history-node-v1`
41 MB (15 978 rows of 2.7 KB, the path-history nodes of every changed
directory), blame cache 5.9 MB, `blob-trigrams-v1` 5.7 MB, skip lists 1.8 MB;
everything else is under 150 KB. A hundred-file directory that changes in every
land is rewritten as a whole in both indexes, which is where the bytes come from.
Plan for roughly 70 KiB of database per land at this directory width (about
70 GB per million lands); `docs/OPERATIONS.md` has the same figure.

**Before the `blame` fix.** The first 16-minute run, same configuration,
failed after seven minutes: `blame` walked every ancestor, took 3.4 s on average
at 4400 changesets (1.4 s for a file untouched since the import, cached or not),
and land p50 rose from 5 ms to 6 s with 54 requests hitting the request timeout.
After the fix a blame is 12 ms cold and 3 ms cached on the same history, and
the run above is flat.

## Real history: git/git

The benchmarks above use synthetic, mostly linear histories. `git/git` is real
and merge-heavy (82,500 commits, 21,347 of them merges, longest parent
chain 26,484), so it exercises what they do not.

Import, on the Oracle VPS of `docs/DEPLOY.md` (4 Neoverse-N1 cores, 11 GiB,
Postgres 16 with `shared_buffers` 2GB, blobs on a block volume), with the
one-commit-at-a-time importer of before #50: 82,500 commits in 47 minutes
(29 per second, falling from 45 to 31 as commits grow). Result: 238 MB of
metadata and 8.9 GB of blobs (162,500 file contents stored whole), against
322 MB for the same history as a git pack. Storing contents uncompressed is
the largest cost Leep pays against git.

The first path query on that history derives the path-history index for every
changeset, and was the first thing to break: about 180 ms per changeset, so
about four hours before the first `last-change` answered. A probe on the first
3,795 commits (367 merges; a local Postgres 16) put almost all of it in merges: at a
merge, each path that differs between the parents' history trees went through
`maximal`, whose `is_ancestor` checks fell back to the generation-ordered walk
(fetching and decoding every changeset on the way) and cost about 6 ms each.
Merges touching 230 paths took 5 to 8 s.

Two changes fixed it, with byte-identical index rows (same checksum of every
`history-tree-v1` and `history-node-v1` row):

- At a two-parent merge, a path's set at the merge base settles it: if one
  parent's set equals the base's, nothing behind that parent changed the path
  since the base, so the other parent's set is the answer. Only paths changed
  on both sides still need `maximal`. Test builds cross-check every shortcut
  against `maximal`.
- `Repo` keeps the parents and generation of changesets it has walked in a
  bounded in-memory cache (about 2 x 262,144 entries). Both are immutable, so
  entries never go stale, and walks stop making a round trip per step.

| Deriving the index for the first 3,795 commits of git/git | Time |
|---|---|
| Before | 270 s |
| Merge-base shortcut alone, using the first-parent common ancestor | 254 s |
| Merge-base shortcut with the true merge base | 101 s |
| Plus the ancestry cache | 24 s |

What remains is about 4.6 ms per changeset of node reads and writes, one
metadata round trip each, plus octopus merges, which still take `maximal`.

**Index size.** On the full history the v1 index was 174,978 node rows
averaging 36 KB (largest 139 KB): 6.1 GB of node bytes and 3.5 GB in Postgres,
against 328 MB for the git repository. Directory nodes keep deleted entries,
and the root of git/git holds hundreds, so almost every commit wrote another
36 KB root node. ADR 0027 stores nodes of more than 64 entries as chunks of
about 32 entries, so a change rewrites one chunk and a short chunk list. On the
first 3,795 commits (local Postgres 16, release builds, a shared 4-core machine
under load from other jobs, the index derived from scratch three times each):

| Index for the first 3,795 commits | Node rows | Node bytes | Cold `last-change` | Server CPU |
|---|---|---|---|---|
| `history-node-v1` (whole nodes) | 5,224 | 45.2 MB | 58.7 / 30.7 / 35.0 s | 14.6 / 13.5 / 14.9 s |
| `history-node-v2` (chunked) | 11,725 | 13.0 MB | 34.1 / 34.2 / 38.3 s | 13.4 / 13.7 / 14.6 s |

Derivation costs the same; wall times vary with the machine's load. The 4,160
chunk lists average 496 bytes and the 7,565 whole nodes and chunks 1.4 KB.
Answers are byte-identical: `/v1/log` and `/v1/last-change` bodies from a v1
and a v2 server on one database agreed for 51 paths (files, directories,
deleted and re-created paths, and paths that never existed). The prefix's
directories are narrow compared with later git/git, so the full history
should shrink by more than this factor of 3.5.

### Warming instead of deriving on first query

With ADR 0028 every server derives the path-history and search indexes of
moved ref heads in the background. On the same 3,795-commit slice, with all
history and search rows removed first, a server started with the default
`--warm-interval 30s` had the path-history index for every changeset and the
search index of `main` after 22 s, starting at launch. A `last-change` sent 3 s
after launch waited 19 s for the warmer's lock instead of deriving the range a
second time. Afterwards `last-change` answered in 3 ms, a search in 0.18 s and
a blame of a rarely changed file in 9 ms. On the full git/git history the same
first query took 69 minutes inside one request; with warming that work happens
after an import or restart without anyone waiting on it.

### Log without a path

`log` without a path walks every ancestor. It used to read one changeset, then
each new parent's generation, then the root tree, one round trip each. ADR 0030
reads ahead in batches of up to 256 (changesets, generations, root trees and
skiplists, four queries issued together), guessing the next changesets from
parents and skiplist entries, so a long chain is read a window per round trip.
Same 3,795-commit slice on a scratch copy of the database, local Postgres 16
over a Unix socket, release builds, `--warm-interval 0`, each limit on a freshly
started server (so the first request also fills the ancestry cache), then the
median of five more; statements counted with `log_statement = all`:

| `GET /v1/log/{head}` | Before: first / median | After: first / median | Statements, first request |
|---|---|---|---|
| `limit=100` | 38 ms / 27 ms | 21 ms / 7.3 ms | 307 -> 27 |
| `limit=3795` (whole slice, 4.7 MB of JSON) | 1.42 s / 0.93 s | 0.15 s / 0.10 s | 11,385 -> 84 |
| `author=nobody&limit=10` (scans all) | 0.89 s / 0.46 s | 0.09 s / 0.07 s | |
| `author=Junio&limit=1000` | 0.34 s / 0.33 s | 0.08 s / 0.08 s | |

Before, the first request made exactly three statements per entry (changeset,
parent generation, root tree) and later ones two; after, the whole slice takes
84 batched statements and 100 entries take 27, most of them in the first rounds
while the skiplists double the known window. The full git/git history was not re-measured
here; its `log limit 10000` took 3.6 s before.

Path logs (`log --path`) are unchanged: each changeset the path walk yields is
found in the index at the previous one's parents, so batches would hold one
changeset. Reading those with the batched calls was tried and was slower
(`path=Documentation`, 711 entries: 0.27 s before, 0.35 s with batches of one).

The in-memory benchmark rows above go the other way: there a read costs well
under a microsecond, so the read-ahead's bookkeeping (a map of known ids, a
heap of guesses, decoding a skiplist per changeset) and its extra skiplist
reads make the walk 2 to 4 times slower, 3 to 4 us per changeset. Against about
0.1 ms per Postgres round trip saved per changeset, that is the right trade
for any real backend.

## Packed blobs

`leep-server repack` (ADR 0029) on a copy of the first 3,795 commits of
git/git (a local Postgres 16, filesystem blobs; 4 vCPU Intel Xeon @ 2.10GHz,
15 GB RAM, release build), zstd level 9:

| | Bytes |
|---|---|
| Loose objects (11,735: 6,791 file versions, 4,944 tree nodes) | 91,168,650 |
| Pack | 3,100,053 |
| Index (96 bytes per object) | 1,126,648 |
| Prototype's pack (ADR 0029), no index | 3.2 MB |
| `git pack-objects` of the same commits | 4.4 MB |

10,795 objects were stored as deltas and 940 whole; the mean delta chain is 12
entries (cap 32). The run took 8.4 s: 3.6 s walking history for bases, 4.6 s
compressing and writing (single-threaded, two compressions per object to pick
the smaller), 0.2 s deleting the loose copies. `verify --full --rehash`
reports the packed copy clean in the same 47 s as the loose one (it is
dominated by checking derived rows).

`get` of 1,000 random objects, one at a time, page cache dropped first
(`cargo run --release -p leep-store --example blob_read_latency -- <dir> 1000
<seed>`; two seeds, which agreed within 5%). "First read" is a fresh store;
"again" reads the same objects from the same store:

| | First read, mean | p50 | p99 | Again, mean |
|---|---|---|---|---|
| Loose | 88 us | 76 us | 240-290 us | 44-55 us |
| Packed | 190 us | 100 us | 0.96-1.1 ms | 35 us |
| Packed, decoded-object cache off | 230 us | 178 us | 0.8 ms | 187 us |

A first read of a packed object decodes its chain (13 frames on average) and
checks each against its hash. The store keeps 64 MiB of decoded objects, so
reading neighbouring versions, or the same object again, skips the chain: with
the cache off, repeated reads cost as much as first ones. The p99 of first
reads is the deepest chains plus loading the 1.1 MB index on the first call.
Two implementation choices mattered: one zstd decoder per thread instead of
one per frame (first-read mean 340 us before), and reading the loose file and
the pack in one blocking task (one thread-pool hop per `get`, as for a loose
store).

Queries through a server over each store (`--warm-interval 0`, derived rows
already present, server started fresh with the page cache dropped; first
request, then the median of five more):

| Query | Loose, first / again | Packed, first / again |
|---|---|---|
| `GET /v1/log/<head>?limit=1000` | 390-393 / 223-264 ms | 396-470 / 245-268 ms |
| `GET /v1/blame/<head>?path=Makefile` | 17-21 / 18-19 ms | 26-35 / 14-15 ms |
| `GET /v1/search/main?q=strbuf_addf` | 35-42 / 20-22 ms | 37 / 17-20 ms |

With indexes derived, these read few blobs, so packing changes them by less
than the run-to-run noise, except a first blame (about +10 ms; it reads
file contents, which now come through delta chains).Deriving indexes from
scratch over a packed store was not measured.

## Analysis

Principle 5 (cost scales with the change, not the repository) holds for the
commit path.

- `create_changeset`: flat from 1k to 100k files. The cost is dominated by
  decoding, re-encoding and hashing the one 1000-entry leaf directory on the
  changed path; the other directories are untouched.
- `land`: a fast-forward is a few microseconds (two `is_ancestor` checks and a
  CAS). A replay is one three-way tree merge plus one `create_changeset`, about
  3-4 ms, independent of repository size and of how far the head moved (1, 10
  or 100 changesets) because the merge compares trees, not intermediate
  changesets.
- `diff`: flat at about 1 ms for 10k and 100k files, thanks to skipping equal
  subtrees by hash. The ~1 ms is again the 1000-entry leaf.
- `is_ancestor`: effectively constant (1.5-2 us) on linear histories up to
  100k; the skiplist works.
- `merge_base`: effectively constant (5-7 us) up to 100k. It was linear in
  the distance to the fork point (376 us, 12 ms, 112 ms) until it learned to
  find the deepest common changeset of the two first-parent chains with
  skiplist jumps. That answer is used whenever neither chain has a merge
  above it (tracked by the `last_merge-v1` derived kind); histories with
  merges in between still take the generation-ordered walk, which is linear
  in the distance. That walk reads parents and generations from `Repo`'s
  ancestry cache once they have been seen (see "Real history: git/git").

- Path `log` and `last_change`: the path-history index (ADR 0013) turns the
  scan into lookups. The old walk read every ancestor (about 7.5 us per
  changeset here: 75-95 ms for 10k changesets whatever the number of matches).
  The index costs one lookup per *match*: 4 us for the newest change, 45 us for
  a path changed 10 times in 10k changesets, 9 us per entry when listing 1000
  matches (each entry reads its changeset and its parents' lookups). A path
  with no match is answered in 3 us, 20,000 times faster. Cost no longer depends
  on history length. The first query on a history derives the index, about
  36 us per changeset (0.4 s for 10k, oldest first, iteratively); later
  queries and new changesets pay only the new changesets. `author` and `extra`
  filters still apply to every changeset the path walk yields, and `log`
  without a path still scans.

What does not scale with the change:

- Merges: deriving the index for a merge diffs the first parent's history tree
  against each other parent's, so a merge of a branch that changed many paths
  derives in time proportional to those paths. The benchmarks use linear
  histories and do not measure this.
- `log` without `path` and the `author` or `extra` filters alone scan
  changesets linearly. Since ADR 0030 the scan reads them in batches, so on a
  database it costs a round trip per window of up to 256 changesets rather
  than two or three per changeset (see "Log without a path").

### Fixture note

A first version imported the whole repository as one root changeset. Then
`create_changeset` appeared to grow with size (0.5 ms, 2.5 ms, 17 ms for 1k,
10k, 100k files). The cause is that `create_changeset` loads each parent
changeset to validate it, and the memory store clones all of its changes. This
is a property of giant import changesets, not of the tree algorithm; a
backend that fetches the whole parent row pays the same. A repository
imported in one 100k-file changeset will have that cost on its first
children only.

### Optimization ideas (not implemented)

- `create_changeset` validates parents with `get_changeset`, which transfers
  the full parent, including all its changes. An `exists`-style `MetaStore`
  method (or a generation lookup) would make the check O(1) regardless of
  parent size. This needs a trait addition, so it is an ADR-sized change.
