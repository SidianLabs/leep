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
| derive the history index (per changeset, one file each) | | 32 us (36 us before the batched derivation of "Path history on Linux") | |
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

**Capacity of one ref.** A fast-forward land is 5 ms, but lands to one ref were
serialized and a replay costs more, so this machine sustained about 6 lands per
second to one ref at the latencies above (measured before group commit; see
"Concurrent lands on one ref" for the swarm without think time); 50 agents with a mean think time of 6 s
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

## Concurrent lands on one ref

`leep-land-bench` (in `crates/leep-soak`) runs N agents against a running
server. Each agent loops: read `main`, upload one blob, create a changeset on
that head writing its own file (`agents/a<i>/f`, so nothing conflicts), and
land it onto `main`. Every land except the first of a round is a replay, since
the other agents moved the head meanwhile. The tables report lands per second
over 10 s per N (after a one-second warm-up), the land request's latency, the
upload and create latencies of the same loop, and from the server's metrics
the CAS losses, the mean time a land waits for its batch and the mean batch
size:

```sh
cargo build --release -p leep-server -p leep-soak
target/release/leep-server --meta <url> --blobs file:///tmp/b --listen 127.0.0.1:7391 \
  --metrics-listen 127.0.0.1:7392 --warm-interval 0 --request-timeout 0 &
target/release/leep-land-bench --server http://127.0.0.1:7391 \
  --metrics http://127.0.0.1:7392 --agents 1,4,16,64 --duration 10 --missing 90000
```
`--missing 90000` first times `POST /v1/blobs/missing` with 90,000 hashes
that were never stored, three times.

**Several servers on one ref.** `--server` and `--metrics` take
comma-separated lists; agent `i` talks to server `i % count`, and the
counters are summed over the servers. The instances share one database and
one blob directory. Since this change the bench also keeps the latency of
lands still waiting when the window closes (every agent finishes its land
before stopping), reports lands per server, and counts *stalled agents*,
those with no land answered inside the window; lands per second still counts
only lands answered inside it.

Local 4-vCPU Xeon shared with other agents' builds (load average 9 to 24),
Postgres 16 on the same machine, blobs on ext4, `05b7630`, release build,
`--agents 8,64,256 --duration 20` on a fresh database and blob directory
per run, three rounds with the server-count order rotated. Cells are the
median of the three runs, with the range in brackets; latencies in ms:

| Servers | Agents | Lands/s | Land p50 | Land p99 | Land max | CAS losses | 409 retries | Stalled agents |
|---|---|---|---|---|---|---|---|---|
| 1 | 8 | 147 (102–159) | 29 (23–33) | 101 (72–259) | 463 (444–1184) | 0 | 0 | 0 |
| 1 | 64 | 201 (185–265) | 194 (141–218) | 460 (383–493) | 590 (528–976) | 0 | 0 | 0 |
| 1 | 256 | 125 (99–163) | 1769 (1396–2031) | 2175 (2002–3028) | 2190 (2040–3080) | 0 | 0 | 0 |
| 2 | 8 | 84 (56–86) | 25 (25–28) | 91 (67–323) | 22101 (21602–22965) | 26 (24–38) | 3 | 4 of 8 |
| 2 | 64 | 170 (169–190) | 112 (96–117) | 564 (474–588) | 25702 (22820–30459) | 8 (6–16) | 0 | 32 of 64 |
| 2 | 256 | 147 (132–147) | 602 (595–692) | 23070 (22837–25390) | 23137 (22878–25426) | 9 (8–14) | 0 | 128 of 256 |
| 3 | 8 | 59 (54–96) | 19 (17–22) | 55 (37–319) | 22035 (21888–22552) | 67 (67–83) | 2 (1–2) | 5 (4–6) of 8 |
| 3 | 64 | 170 (166–180) | 71 (66–78) | 21480 (21289–22024) | 23051 (22579–23293) | 34 (34–49) | 0 | 43 (42–43) of 64 |
| 3 | 256 | 146 (138–163) | 383 (338–397) | 24401 (23530–24761) | 24584 (23716–24989) | 24 (23–28) | 0 | 171 of 256 |

Adding servers does not add throughput, and it starves agents: in every one
of the 18 multi-server runs one instance answered every land inside the
window (lands per server `0 / 3410`, `0 / 0 / 3258` and so on), and the
agents of the other instances waited for the whole window. Their lands
finished only when the winning instance's agents stopped, 21 to 30 s after
they were sent, or ran out of the 32 attempts and answered 409 (the bench
then lands again). The lands per second and p50 above are the winner's
agents alone, which is why they look no worse than one server: their
batches are smaller (15 at 64 agents with 2 servers, 10 with 3, against 32
with one) and come round faster.

Why the loser never wins, from per-attempt timers in a profiling build
(`LEEP_LAND_PROFILE`, not merged; medians, ms):

| Batch | Lands | Attempt | Prefetch | Replays | of which landed-copy scan | Blob store | Changesets and rows | CAS and ref log |
|---|---|---|---|---|---|---|---|---|
| 1 server, 64 agents | 28 | 170 | 14 | 108 | 0.8 | 31 | 3.8 | 2.7 |
| 1 server, 256 agents | 64 | 417 | 23 | 340 | 5.4 | 37 | 5.3 | 3.6 |
| 2 servers, 64 agents, winner | 16 | 74 | 7 | 36 | 0.2 | 21 | 2.5 | 2.2 |
| 2 servers, 64 agents, loser (attempt 2 to 19) | 30 | 754 | 5 | 671 | 465 | 49 | 4.1 | 1.1 |
| 2 servers, 256 agents, loser (attempt 2 to 12) | 64 | 1058 | 6 | 948 | 467 | 135 | 9.0 | 1.3 |

The winning instance moves the ref every 70 to 80 ms; a losing attempt
takes 0.75 to 1 s between reading the head and the swap, so it always finds
the head moved. Each loss makes the next attempt slower: the losing lands
fall further behind the head, and the landed-copy scan (ADR 0021, 0032)
walks every changeset the head gained since their base, up to its bound of
1024, which is half the attempt by the second or third loss. All of the
losing instance's agents are waiting, so its batch is always all of them,
and its replays onto a head further away cost more (7 to 15 ms each, against
2 to 5). The backoff (at most 200 ms) does not break the cycle, because the
winner never pauses. With one server, the time of a batch at 64 and more
agents is replays (65 to 80%), then storing the staged tree blobs with their
fsync and condemnation read (10 to 20%), the prefetch (5 to 8%), the
changeset and derived-row write (1 to 3%) and the swap with its ref log
rows (1 to 2%); the landed-copy scan is under 2%.

As far as the bench checks, correctness held: every land was answered
`Landed` (after the 409s above), and none conflicted or errored. Without
`--land-coordination on`, route each hot ref's lands to one instance
(`docs/RUNBOOKS.md`).

**With coordination (ADR 0056).** The same bench, every server of a
multi-server run started with `--land-coordination on --advertise-url
http://127.0.0.1:<port>/ --peer-url-prefix http://127.0.0.1:` (1 server: off,
as above). Same local 4-vCPU Xeon, shared with other agents' builds and tests
(load average 3 to 12 during the rounds), Postgres 16 on the same machine,
blobs on ext4, release build of the code review's revision (lease calls on
their own connections, takeover at the TTL), `--agents 8,64,256 --duration
20`, a fresh database and blob directory per run, three rounds with the
server-count order rotated (1 2 3, 2 3 1, 3 1 2). Cells are the median of the
three runs, range in brackets; latencies in ms. "Agents" is the total,
spread round-robin over the servers:

| Servers | Agents | Lands/s | vs 1 server | Land p50 | Land p99 | Land max | CAS losses | 409 retries | Stalled agents |
|---|---|---|---|---|---|---|---|---|---|
| 1 | 8 | 231 (193–238) | | 19 | 39 | 231 | 0 | 0 | 0 |
| 1 | 64 | 316 (297–316) | | 132 | 207 | 442 | 0 | 0 | 0 |
| 1 | 256 | 179 (179–192) | | 1297 | 1463 | 1483 | 0 | 0 | 0 |
| 2 | 8 | 194 (170–199) | 84% | 24 | 48 | 74 | 0 | 0 | 0 |
| 2 | 64 | 256 (251–261) | 81% | 184 | 255 | 310 | 0 | 0 | 0 |
| 2 | 256 | 163 (147–173) | 91% | 1406 | 1563 | 1607 | 0 | 0 | 0 |
| 3 | 8 | 174 (170–174) | 75% | 27 | 52 | 104 | 0 | 0 | 0 |
| 3 | 64 | 251 (240–254) | 80% | 183 | 252 | 281 | 0 | 0 | 0 |
| 3 | 256 | 157 (154–157) | 88% | 1463 | 1744 | 1798 | 0 | 0 | 0 |

Starvation is gone. Not one agent stalled in any of the 18 multi-server runs,
against 4 to 171 per run before. Land max is at most 1.2 times one server's,
where it was 22 to 30 s. No swap was lost and no land was answered 409.
Lands per server are even (for example `1664 / 1664`, `1032 / 1046 / 1058`),
where before one server answered all of them.

**Lease handovers.** Six more runs of 2 and 3 servers at 256 agents (169.6,
166.4 and 160.0 lands/s with 2 servers; 176.0, 156.8 and 156.8 with 3) each
took the lease once and ended at epoch 1: no handover. An earlier round had
reported 2 to 4 acquisitions per run. Those were not handovers: lands that
arrive together all claim the free lease at the start, each claim succeeded
as the same holder at epoch 1, and each was counted. Only the first claim of
an epoch counts now. Lease calls also use connections of their own, so a
renewal can no longer queue behind 256 agents' uploads for the takeover
window. Six more such runs after the lease table became the generic
`leases` table, on a busier host (load average 8 to 26), again took the
lease once each and lost none.

**Throughput.** It is 75 to 91% of one server's at the same total number of
agents, short of the 95% target ADR 0056 set. All the processes share one
loaded 4-vCPU host, and each forwarded land costs two more HTTP exchanges on
it: the follower takes the client's request, sends it to the leader and
relays the answer, on the same CPUs as the leader's replays, which are what
limits a ref. On one host the extra servers add work and no CPU. Whether
coordinated servers reach 95% must be measured with the servers on separate
hosts before `--land-coordination on` is recommended as the multi-server
default. A direct comparison on one host: two coordinated servers, and 8
agents landing through the leader, then through the follower, then through
the leader again. The first run through the leader started on a new
database:

| Agents land through | Lands/s | Land p50 ms |
|---|---|---|
| the leader | 140 / 237 | 25.6 / 18.1 |
| the follower | 196 | 24.6 |
| the follower, before the fix below | 124 | 43.8 |

One agent sees about 3 ms more through the follower (2.0 to 2.4 ms against
5.3 to 5.7 ms).

**Lands created on another server.** The first multi-server round ran at 54
to 78% of one server. The leader had never seen the changesets of forwarded
lands, which were created through other servers. So it read their parents
and generations one at a time during the replay. Without the generations,
it also skipped the batch's prefetch of rebase links, and the landed-copy
scans read those row by row. A batch now first learns the parents and
generations of the changesets it has not seen, in one round of batched
reads. With 8 agents landing through a follower that raised throughput from
122 lands/s to 147.

**Per-directory flushes (ADR 0048).** After ADR 0044, with 64 agents and
10-file changesets (`--files 10`) create p50 was about 25% worse than before
it. Timers on the create path put 20 to 44 ms of a create's 51 to 93 ms
server time in its flush: one sync wave of every directory dirtied since the
last (80 to 122, 21 to 40 ms), of which the create needed about 13. Waves
started as soon as a flush asked, so starting them eagerly was not the fix:
a build that ran waves continuously in the background improved create p50
(129 ms against 156 for `main`, against 108 for per-directory flushes, in
the same four rounds) only by keeping waves smaller. A flush now syncs only
the directories of the blobs its caller wrote or names and shares syncs
already running; a create's flush takes 3 to 6 ms, under the condemnation
read beside it, and mean server time per create at 64 agents fell from 74 to
56 ms (median of eight 5-second windows, alternating builds).

Same local 4-vCPU Xeon, shared with other agents' builds and benchmarks
(load average 7 to 12 during these rounds, up to 28 in earlier ones),
Postgres 16, blobs on ext4, `--files 10 --duration 10`. Six alternating
rounds of each build; cells are medians, p50 / p99 in ms. Before is `04d8312`
(the parent of ADR 0044's change), `main` is `d958eae`:

| Agents | Build | Lands/s | Land p50 / p99 | Put p50 / p99 | Create p50 / p99 |
|---|---|---|---|---|---|
| 1 | before | 12.3 | 3.3 / 10.7 | 5.6 / 17.6 | 10.0 / 22.4 |
| 1 | main | 17.4 | 3.0 / 7.7 | 3.8 / 11.4 | 10.2 / 24.0 |
| 1 | per-directory | 9.0 ¹ | 4.2 / 12.7 | 8.0 / 19.3 | 16.1 / 34.2 |
| 8 | before | 47.9 | 39.4 / 89.7 | 9.4 / 25.1 | 17.2 / 45.2 |
| 8 | main | 54.1 | 50.6 / 180 | 5.8 / 17.7 | 22.5 / 77.7 |
| 8 | per-directory | 52.9 | 47.4 / 103 | 6.7 / 22.5 | 20.0 / 47.8 |
| 64 | before | 66.8 | 315 / 515 | 45.0 / 113 | 99.4 / 212 |
| 64 | main | 69.5 | 378 / 788 | 30.4 / 128 | 143 / 275 |
| 64 | per-directory | 62.2 | 399 / 774 | 39.5 / 91.2 | 135 / 235 |

¹ Single-agent runs on this machine fall into two modes for every build
(about 17 lands/s and a 10 ms create, or about 9 lands/s and a 16 to 19 ms
create, for a whole run); four of the six per-directory runs and one or two
of each other build's landed in the slow one. Three further alternating
1- and 8-agent rounds with timers put `main` and per-directory flushes level:
create p50 10.5 / 12.2 / 18.5 ms against 11.0 / 7.6 / 19.8, server time per
create 11.8 to 13.0 ms against 10.0 to 14.7, flush 4.6 to 4.9 ms against 2.8
to 3.6.

At 64 agents the create p50 medians of three sets of rounds were 119, 127
and 99 ms before, 181, 156 and 143 ms on `main`, and 137, 108 and 135 ms
with per-directory flushes; single rounds of one build ranged over 70 to
170 ms. The client's create time is about twice the server's: on this
machine the rest is waiting for a CPU, which is also why moving the flush
off the critical path shows up more clearly in the server's timers than in
the bench. Lands per second and land latency moved within the noise; no run
lost a CAS or answered 409.

**Write path (ADR 0044).** After staged batches, on a 4-vCPU VPS (Postgres
16, blobs on a real disk) 64 agents landed about 117 times a second and the
limit was the agents' own requests: put p50 66 to 110 ms, create p50 130 to
215 ms (one agent: 6.5 and 10.5 ms). Timers on the put and create paths and
a `perf` profile of the server at 64 agents (local run) found:

- Every put fsynced its shard directory after the rename, and every create
  wrote three or four tree nodes, each with its own file and directory
  fsync. Nothing needs an upload's directory synced until a changeset names
  the blob.
- `missing_blobs` checked existence one hash at a time: a blocking task per
  hash and, for every miss, a rescan of `packs/` under a lock (25 to 80 µs
  each), 2.2 to 2.9 s for 90,000 hashes on the VPS and 7 to 11 s on the
  loaded local machine. The batched condemnation read before it (one query
  of 90,000 keys) took 0.2 to 0.3 s in Postgres.
- A put made three round trips in turn: the blob write (six hops to the
  blocking pool through `tokio::fs`), the condemnation read, the claim write.
- Under load each Postgres round trip took 5 to 15 ms, but Postgres itself
  was mostly idle (10 of 16 pooled connections idle, the rest waiting on the
  server) and raising the pool from 16 to 48 connections changed nothing
  beyond the noise: the server was short of CPU, most of it (30% of samples)
  in the kernel scheduling threads, not in the pool. A separate pool for the
  land leader would not have helped here and was left out.

Fixed: a put syncs only its file and records the directory, and a flush
before every changeset write syncs the recorded directories in one wave
shared by every request waiting (`BlobStore::flush`); a create's tree nodes
and the contents it names share one wave. The blob write is one blocking
task. The claim is written alongside the condemnation read. `missing_blobs`
runs in phases over all hashes (condemnation rows in parallel chunks, fences
of the few with a row 64 at a time, existence in one batched
`contains_many` that stats in a few tasks and rescans the packs once,
claims), keeping each hash's order.

Same local 4-vCPU Xeon as below, shared with other agents' builds and tests
(load average 6 to 8 throughout), Postgres 16, blobs on ext4. Before is
`main` with staged batches; run alternately, two rounds, both shown
(round 1 / round 2; latency cells read before → after):

| Agents | Lands/s before | Lands/s after | Put p50 ms | Put p99 ms | Create p50 ms | Create p99 ms |
|---|---|---|---|---|---|---|
| 1 | 40 / 77 | 110 / 84 | 9.3 / 3.5 → 1.8 / 3.0 | 20.7 / 8.7 → 5.6 / 6.5 | 8.1 / 5.2 → 4.0 / 5.2 | 20.1 / 11.7 → 9.5 / 10.9 |
| 4 | 115 / 72 | 98 / 135 | 4.9 / 7.2 → 3.8 / 3.1 | 14.8 / 23.7 → 15.6 / 8.3 | 7.2 / 11.4 → 9.4 / 7.8 | 17.9 / 30.5 → 31.5 / 15.1 |
| 16 | 162 / 139 | 175 / 239 | 13.0 / 14.3 → 7.6 / 6.3 | 36.8 / 57.7 → 28.9 / 17.8 | 20.5 / 23.1 → 24.3 / 19.9 | 52.1 / 75.9 → 70.1 / 36.0 |
| 64 | 184 / 160 | 170 / 231 | 39.8 / 42.3 → 22.2 / 18.9 | 121 / 139 → 118 / 45 | 76.7 / 93.1 → 81.4 / 64.8 | 169 / 181 → 202 / 123 |

`POST /v1/blobs/missing`, 90,000 never-stored hashes in one request, three
runs each round: 9.2 to 11.6 s / 6.8 to 7.3 s before, 0.34 to 0.44 s / 0.35
to 0.40 s after. What is left is mostly the condemnation lookup in Postgres
(one index probe per key) and parsing the 6 MB request.

No run lost a CAS or answered 409. Uploads are faster at every agent count,
about half the time at 16 and 64 agents. Creates and lands per second at 4
to 64 agents moved within the machine's noise (the two rounds disagree more
than before and after do): a create now waits for a sync wave that holds
every directory written since the previous one, by all agents, instead of
only its own syncs, so under heavy load it trades its own fsyncs for a share
of a larger wave. On storage where each fsync is slower (the VPS: 1.3 to
2 ms per directory sync) the saving per upload and per create is larger.
Earlier runs during the work, with lower load, gave 300 to 320 lands/s at 64
agents against 184 to 220 before.

**Staged batches (ADR 0043).** Group commit (below) made the ref move once per
batch, but each replay inside the batch still went to the stores: on a 4-vCPU
VPS (Postgres 16, blobs on a real disk) throughput was a flat 56 to 62 lands/s
at 1, 4, 16 and 64 agents, batch mean 32 at 64 agents, land p50 0.9 s and lock
wait 0.3 s. A batch of 32 took half a second. Timers in a local run of the
same shape (Postgres, 64 agents, 45 lands/s) put 16 ms on each land in a batch:
9 ms storing its replayed copy (three tree nodes written and fsynced, the
condemnation read after them, the creation inputs, one transaction for the
changeset and its rows), 4 ms in the first landed-copy scan of each batch
(one `rebased-origin-v1` read per changeset the previous batch added), 3 ms of
reads of the land's changeset and trees and the merge itself (0.5 ms). Each
read or write was a round trip waiting for a connection of a pool the 64
agents' uploads and creates also use. Trees were already patched along the
changed paths only; computing a replay was never the cost.

A batch now replays into a staging layer that keeps writes in memory and
serves them back, and stores everything once before the swap: one wave of
blob writes (then the GC rescue check), one `put_changesets_with_derived`.
It also reads its lands' changesets and root trees and the head's rebase
links in one round of batched reads. A replay in a batch then costs about
2 ms (merge 0.7, building and staging the copy 0.6, reads 0.3 to 0.6), the
landed-copy scans nothing, and a batch of 32 about 100 to 140 ms: the replays,
20 to 45 ms of store (blobs 18 ms, metadata 4 to 16 ms), 20 ms of prefetch and
a few milliseconds each for the ref read and swap, all of them waiting for
pool connections behind the agents' uploads and creates.

Same local 4 vCPU Xeon as below, shared with other agents' builds and
benchmarks (load average 4 to 12), same harness. Before is `main` with group
commit, after is staged batches; run alternately, two rounds, both shown
(round 1 / round 2):

| Store, agents | Lands/s before | Lands/s after | Land p50 ms before / after | p99 ms before / after | Put p50 ms after | Create p50 ms after | Lock wait mean ms before / after | Batch mean before / after |
|---|---|---|---|---|---|---|---|---|
| Postgres, 1 | 96 / 71 | 66 / 86 ¹ | 2.0 / 2.3 | 5 / 6 | 4.4 | 7.0 | 0.0 / 0.0 | 1.0 / 1.0 |
| Postgres, 4 | 109 / 70 | 83 / 115 | 19-36 / 19-27 | 72-199 / 43-52 | 4.8-7.3 | 7.6-10.6 | 4-8 / 5-9 | 2.0 / 1.6 |
| Postgres, 16 | 114 / 69 | 186 / 216 | 112-194 / 42-44 | 262-388 / 86-312 | 9.3-10.1 | 15.9-16.8 | 48-51 / 9-11 | 8.0 / 6.9-7.5 |
| Postgres, 64 | 70 / 58 | 229 / 215 | 824-937 / 173-192 | 1029-1108 / 270-398 | 27-29 | 62-64 | 79-107 / 35-37 | 32 / 31 |
| SQLite, 1 | 75 / 44 | 60 / 49 | 1.8-2.1 / 1.9-2.1 | 5-12 / 8-12 | 4.5-4.6 | 7.1-7.2 | 0.0 / 0.0 | 1.0 / 1.0 |
| SQLite, 4 | 102 / 113 | 147 / 86 | 20-21 / 13-23 | 41-49 / 30-49 | 3.8-6.9 | 5.8-10.8 | 4-5 / 4-7 | 1.7 / 1.3-1.4 |
| SQLite, 16 | 103 / 128 | 197 / 125 | 92-110 / 38-60 | 166-444 / 79-132 | 7.9-14.6 | 13.1-20.7 | 33-43 / 13-21 | 7.5 / 5.1 |
| SQLite, 64 | 96 / 90 | 199 / 199 | 564-598 / 188-190 | 686-721 / 300-340 | 30-33 | 75-82 | 224-231 / 48 | 32 / 25-26 |

¹ The lone land does the same store calls as before (no prefetch, nothing to
store for a fast-forward). The first two rounds put it at 42 and 32 lands/s
after a Postgres checkpoint from the previous run's dropped database; three
back-to-back alternating 6 s runs gave 62 / 66, 86 / 86 and 75 / 75 lands/s,
p50 1.8 to 2.4 ms on both, which the row shows.

No run lost a CAS or answered 409. At 16 and 64 agents throughput is now
2 to 4 times what it was and grows with the batch instead of staying flat;
land p50 at 64 agents fell from about 0.9 s to 0.18 s. At 4 agents the loop is
bound by its own latency (upload, create, land in turn), not by the batch.
What is left per batch is mostly round trips that queue for one of the 16
pool connections behind the agents' own uploads and creates (whose latency
rises from 4 and 7 ms at one agent to 30 and 60-80 ms at 64): a leader that
did not share the pool, or creates that cost fewer round trips, are the next
steps. On the VPS, where each fsync is slower, storing a batch's blobs as one
concurrent wave instead of a wave per land should matter more than here.

**Group commit (ADR 0032).** Same 4 vCPU Xeon @ 2.80GHz as above, shared with another agent's builds (load
average 2 to 6), harness and server on the same cores; a fresh database per
run, Postgres 16 on a Unix socket or SQLite in WAL mode. Before and after were
run alternately, two rounds each; the table shows the second round (the first
agreed within the noise except a cold N = 1 start). Before is the per-ref land
lock, after is group commit (ADR 0032):

| Store, agents | Lands/s before / after | p50 ms before / after | p99 ms before / after | Batch mean |
|---|---|---|---|---|
| Postgres, 1 | 128 / 130 | 1.6 / 1.6 | 3.8 / 4.1 | 1.0 |
| Postgres, 4 | 146 / 185 | 20 / 14 | 35 / 24 | 2.0 |
| Postgres, 16 | 78 / 181 | 197 / 76 | 265 / 105 | 8.0 |
| Postgres, 64 | 41 / 154 | 1565 / 350 | 1680 / 403 | 32 |
| SQLite, 1 | 135 / 128 | 1.3 / 1.4 | 4.1 / 4.4 | 1.0 |
| SQLite, 4 | 130 / 147 | 20 / 16 | 48 / 35 | 1.7 |
| SQLite, 16 | 67 / 121 | 230 / 97 | 323 / 189 | 7.6 |
| SQLite, 64 | 35 / 122 | 1824 / 422 | 2052 / 614 | 32 |

No run, before or after, lost a CAS or answered 409: lands in one process
never raced each other, before or after. Throughput fell with N because each
land's work grew with N. A land whose changeset is behind the head looks for a
copy an earlier attempt may already have landed (the idempotency scan of ADR
0021): it walks the head's ancestors down to the changeset's generation,
reading each one's `rebased-origin-v1` row and parents, two reads per
changeset of distance. With N agents a changeset is about N lands behind, so
under the lock each land cost about 5 ms plus 0.35 ms per agent on Postgres. A
build with the scan switched off held SQLite at 131 lands/s at N = 64 (51 with
it, in the same session), which located the cost.

A batch replays its lands one after another on the head the previous one
left, so the scans of one batch walk the same stretch of history: they share
the `rebased-origin-v1` rows already read (parents come from the process-wide
ancestry cache), the first land of a batch pays the walk and the others only
the few changesets the batch added. One ref read and one commit serve the
batch. What remains per land is the replay itself (merge, write the copy), so
throughput stays near the single-agent rate as N grows, and latency at N = 64
is about two batches of replays instead of 64 lands. A lone land does the same
work as before plus a task spawn and a channel send, within the noise.

**Linger.** Waiting 0.5 or 2 ms before taking a batch, so more lands join it,
did not raise throughput at N = 64 beyond the noise (152 and 158 lands/s against
142 in the same session on SQLite) and cost a lone land its whole wait (p50 3.2
and 5.3 ms against 1.3, 99 and 63 lands/s at N = 1 against 138). Batches fill on
their own: lands arriving while a batch replays form the next one. The server
does not linger.

## Many-agent workload

`leep-swarm` (in `crates/leep-soak`) simulates a team of agents rather than a
land loop: thousands of async agents, each a cheap task with its own seeded
choices, against one or more running servers. Each agent repeats a cycle:
think, sync its working copy (read the ref, list its own files at the head),
make `--reads` reads drawn from `--read-mix` (log, with a hot path half the
time; files; blame; search; diff from its last base), think, upload
`--files` blobs of `--blob-bytes`, create a changeset on a head up to
`--stale` observed heads behind the newest, and land it. Before every
operation it thinks for an exponentially distributed time with mean
`--think-ms` (0 is a burst). A share `--hot-fraction` of the files written
are one of `--hot-files` shared files, which is where conflicts come from.
A land answered 409 is sent again after a short pause; one that times out,
loses its connection or gets a 5xx is resent the way a client would, with
its land id (ADR 0057) unless `--land-id false`.

Every land writes a unique marker (its message, `swarm <run> a<agent> s<seq>`).
After the window the generator walks the target ref's whole history and
reports *double lands* (a marker found more than once) and *lost lands* (a
land answered `landed` whose marker is absent); with land ids on it exits
non-zero if either is above zero, and always if a land is lost.

It reports lands per second (answered inside the window), p50 and p99 of
every operation, conflicts, 409s, resends, the servers' CAS losses, batch
size and lands answered from a recorded id, lands per server, *stalled
agents* (sent a land inside the window and had none answered there) and,
separately, *incomplete cycles* (agents that sent no land inside the window:
still thinking, reading or building one when it closed). With
`--database-url` pointing at the Postgres metadata store it also reports:

- **Warmer lag**, sampled every `--lag-every` seconds: the head's generation
  minus that of the newest head the run has seen (the generator keeps the
  last 4096) that has a `history-tree-v4` row (path history) or a
  `log-filter-v1` row. A sample is one ref read, one indexed query per kind
  and a changeset read per generation not yet known, so it does not load the
  server. Queries derive what they need, so the lag is the index's, not only
  the warmer's.
- **Bytes per land**: the growth of `pg_database_size` over the window, and
  of the logical bytes (keys and values) of what the write path stores
  (changesets, ref log, refs and their `root_tree`, `generation`, `skiplist`
  and `rebased-origin-v1` rows) and of every other derived row (indexes that
  reads and the warmer build), each divided by the lands answered `landed`.
  Tree nodes live in the blob store and are not counted.

`--json FILE` writes the same report as JSON.

```sh
cargo build --release -p leep-server -p leep-soak
target/release/leep-server --listen 127.0.0.1:7471 --metrics-listen 127.0.0.1:7472 \
  --meta postgres://localhost/swarm --blobs file:///var/tmp/swarm/blobs --meta-pool-size 32 &
target/release/leep-swarm --server http://127.0.0.1:7471 --metrics http://127.0.0.1:7472 \
  --database-url postgres://localhost/swarm \
  --agents 2000 --think-ms 2000 --duration 60 --ramp 20 \
  --read-mix log=3,files=3,blame=1,search=1,diff=2 --reads 4 --files 2 \
  --hot-files 8 --hot-fraction 0.02 --stale 4
```

Several `--server` URLs (with their `--metrics`) share the agents round
robin, as in `leep-land-bench`. The target ref is created with a 200-file
tree and the hot files when absent.

**Smoke test, not a baseline.** These numbers show the generator works and
what it reports; they are not a performance baseline. They come from a
shared 4-vCPU Xeon (2.8 GHz, 15 GB) on which other agents were building and
testing: the load average was 12 before the runs and 13 to 21 during them,
and the generator itself (thousands of tasks) shared the same four cores.
Each configuration ran once, so differences under well over 20% mean
nothing. The real baseline will be run on the Linux store on an idle host,
three runs or more per configuration.

Setup: one server, Postgres 16 on the same machine, blobs on ext4, release
build of this branch, a fresh database and blob directory per run, server
defaults (warmer every 30 s with search warming, 256 MiB blob cache) and
`--meta-pool-size 32`. Generator defaults (`--reads 4`, the mix above,
`--files 2` of 512 bytes, 8 hot files at 2%, `--stale 4`, 30 s request
timeout), 2 s think time and land ids on, except where named.

| Run | Lands | Lands/s | Land p50 / p99 ms | Conflicts | Resends | Batch mean | Stalled agents | Incomplete cycles | Double / lost lands | Lag path history p50 / max | Lag log filter p50 / max | DB bytes / write bytes / index bytes per land |
|---|---|---|---|---|---|---|---|---|---|---|---|---|
| 500 agents, 60 s | 1420 | 23.7 | 342 / 1634 | 0 | 0 | 4.6 | 0 of 500 | 0 | 0 / 0 | 217 / 308 | 733 / 1167 | 8,166 / 1,815 / 2,822 |
| 2000 agents, 60 s | 1313 | 21.9 | 10883 / 49927 | 48 | 381 | 64.0 | 284 of 2000 | 574 | 0 / 0 | 890 / 1448 | 1020 / 1578 | 5,143 / 1,639 / 1,121 |
| 2000 agents, no search reads | 853 | 14.2 | 29305 / 89206 | 90 | 2164 | 53.8 | 1128 of 2000 | 0 | 0 / 0 | 434 / 617 | 845 / 1280 | 5,048 / 1,598 / 1,470 |
| 256 agents, no think time, 30 s | 1107 | 36.9 | 249 / 1223 | 18 | 0 | 6.0 | 0 of 256 | 0 | 0 / 0 | 272 / 318 | 555 / 1127 | 7,877 / 1,790 / 2,304 |
| same, land ids off | 832 | 27.7 | 412 / 1385 | 16 | 0 | 7.0 | 0 of 256 | 0 | 0 / 0 | 253 / 281 | 430 / 801 | 7,900 / 1,707 / 2,250 |

Reads, p50 / p99 ms:

| Run | sync | log | files | blame | search | diff | upload | create |
|---|---|---|---|---|---|---|---|---|
| 500 agents | 45 / 310 | 191 / 17492 | 24 / 283 | 657 / 17862 | 9259 / 16428 | 46 / 435 | 28 / 324 | 73 / 351 |
| 2000 agents | 300 / 750 | 909 / 1682 | 153 / 534 | timeouts | timeouts | 310 / 731 | 160 / 365 | 472 / 852 |
| 2000, no search | 97 / 1024 | 312 / 28794 | 30 / 535 | 8406 / 29377 | – | 71 / 1040 | 22 / 169 | 51 / 372 |
| 256 burst | 26 / 138 | 115 / 8543 | 13 / 61 | 7313 / 8745 | 2321 / 6020 | 23 / 105 | 21 / 92 | 49 / 175 |

What it suggests, to be checked on the real baseline:

- **Search reads are the most expensive operation.** The head moves several
  times a second, so most searches land on a head whose trigram index is not
  warm and derive it (p50 9 s at 500 agents here). At 2000 agents 1,063
  searches, 1,165 logs and 799 blames timed out at 30 s.
- **Land throughput falls as agents rise** while batches fill to the 64
  limit: every land of a full batch replays onto a head up to 64 lands
  newer, on a stale base, and the batch competes with the reads for the
  same cores. Without search reads more agents land at once and lands wait
  longest: 2,164 timed out at the client's 30 s and were resent under their
  land id. None landed twice; a resent land whose first request was
  cancelled had been dropped from its batch.
- **The indexes fall behind under load.** The warmer runs every 30 s; with
  thousands of agents the path-history index trails the head by hundreds of
  generations and the log-filter index by about a thousand, so path logs
  and blames derive on the request path (the multi-second p99s).
- **Double lands were zero in every run**, with and without land ids. No
  run lost an answer after its commit, so this does not exercise the window
  ADR 0057 closes; the crash and fault tests do.
- **Bytes per land.** About 1.6 to 1.8 KB of logical data per land comes
  from the write path (changeset rows, ref log entry, root tree, generation,
  skiplist and rebase rows) and 1.1 to 2.8 KB from indexes built by reads
  and the warmer; `pg_database_size` grew 5 to 8 KB per land, the rest
  being Postgres's own index and page overhead.

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
| `history-node-v3` (chunked, patched), later on a quieter machine | 11,733 | 3.70 MB | 15.7 / 16.0 / 16.0 s (v2: 18.5 / 19.4 / 18.5 s) | 9.2 / 9.4 / 9.4 s (v2: 9.6 to 9.8 s) |

Derivation costs the same; wall times vary with the machine's load. The 4,160
chunk lists average 496 bytes and the 7,565 whole nodes and chunks 1.4 KB.
Answers are byte-identical: `/v1/log` and `/v1/last-change` bodies from a v1
and a v2 server on one database agreed for 51 paths (files, directories,
deleted and re-created paths, and paths that never existed). The prefix's
directories are narrow compared with later git/git, so the full history
should shrink by more than this factor of 3.5.

**Patched rows (ADR 0037).** The v2 bytes on that slice were 55% changeset
ids, 24% entry names and 16% chunk lists. The 222,540 ids stored were only
3,792 distinct ones: each new version of a chunk repeated its unchanged
entries. What per-row compression and the alternatives achieve on those rows:

| v2 rows of the 3,795-commit slice (13.0 MB) | Bytes | Smaller by |
|---|---|---|
| Postgres pglz, as stored (only rows over about 2 KB are compressed) | 11.2 MB | 1.16x |
| zstd per row, level 3 / 19 | 9.30 / 9.15 MB | 1.40x / 1.42x |
| Per-row id table and front-coded names | 9.79 MB | 1.33x |
| zstd with a 110 KB dictionary trained on all rows | 2.52 MB | 5.2x |
| The same, trained on the older half, on the newer half | | 2.0x |
| Patches on keyframes (v3) | 3.70 MB | 3.5x |

The dictionary's figure comes from the slice's own changeset ids sitting in
the dictionary; on rows it has not seen it falls to 2x and keeps falling as
history moves on. v3 stores each changed node, chunk or list as a patch on an
earlier whole version. It holds 942 keyframe nodes and chunks (1.05 MB),
1,024 keyframe lists (0.42 MB), 6,631 patches averaging 255 bytes and 3,136
list patches averaging 173 bytes. The threshold for starting a new keyframe
was chosen on this slice:

| Patch kept while | Node bytes |
|---|---|
| at most 1/2 / 1/3 / 1/4 / 1/8 of the whole row | 4.83 / 3.95 / 3.88 / 5.36 MB |
| `patch² <= C × whole`, C = 64 / 96 / 128 / 200 / 300 | 3.71 / 3.60 / 3.62 / 3.98 / 4.54 MB |
| C = 128, lists never patched | 4.80 MB |
| C = 128, list patches naming the next chunk (as shipped) | 3.70 MB |

In Postgres the node rows, copied into their own table with their primary
key, took 14.5 MB for v2 (12 MB heap, 1.4 MB TOAST, 0.9 MB index) and 5.8 MB
for v3 (4.6 MB, 0.1 MB, 0.9 MB). Deriving from scratch got faster, because
the rows of a node now go in one write; the warmer took 15.1 s. Three
derivations, and the warmer's, wrote byte-identical rows (same checksum of
every row), and `verify --full` recomputed all 11,733 and found them equal.

Queries on a scratch copy of the slice, release builds, a v2 and a v3 server
in turn on one database (each holding its own index), `--warm-interval 0`.
"Cold" is the first request to a freshly started server, the median of 5
starts (15 for single lookups); "warm" the median of as many repeats; blame
rows were deleted before each cold blame. Bodies for 50 paths (`last-change`,
`log path=… limit=1000` and `blame` each, 150 responses) were byte-identical.

| Query | v2 cold / warm | v3 cold / warm |
|---|---|---|
| `last-change Makefile` | 5.5 / 1.4 ms | 5.4 / 1.3 ms |
| `last-change COPYING` | 4.8 / 1.3 ms | 5.1 / 1.3 ms |
| `last-change` of a path that never existed | 3.5 / 0.9 ms | 3.8 / 1.0 ms |
| `last-change Documentation/git-pull.txt` | 5.0 / 1.3 ms | 5.6 / 1.6 ms |
| `log path=Makefile limit=50` | 61 / 24 ms | 57 / 20 ms |
| `log path=Documentation limit=1000` | 604 / 306 ms | 602 / 319 ms |
| `blame Makefile` | 520 / 14 ms | 503 / 15 ms |
| `blame COPYING` | 13.8 / 1.9 ms | 13.0 / 1.9 ms |

Path logs and blame, which make many lookups, are unchanged. A single lookup
on a fresh server can read one more row, the keyframe of the newest chunk
list when the name lies in a chunk that list patch does not set: up to
0.6 ms. A name that changed recently is answered from the patches alone.

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
while the skiplists double the known window. On the full git/git history (below) `log limit 10000` went from 3.6 s to
0.64 s.

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

### Full history with chunked nodes, warming and batched log

The whole of git/git again, on the same VPS, after ADRs 0027 (chunked nodes),
0028 (warming) and 0030 (batched log). All path-history and search rows were
deleted and a server started with `--warm-interval 30s`; nobody sent a query.

| Path-history index, all 82,500 commits | v1, derived by the first query | v2, derived by the warmer | v3 (ADR 0037), derived by the warmer |
|---|---|---|---|
| Time to build | 69 min, inside one request | 41 min, in the background | 44 min, in the background (beside a Linux repack) |
| Node rows | 174,978 | 410,685 | 411,290 |
| Node bytes | 6.1 GB | 796 MB | **157 MB** |
| Whole database | 3.7 GB | 1.0 GB | **483 MB** (`derived` 389 MB) |

ADR 0037's patched rows store 5.1x fewer node bytes than v2 on the full
history, more than the 3.5x of the 3,795-commit slice because the full
history's rows are larger. The database figure is after `repair drop --kind
history-tree-v2` and `VACUUM FULL derived`; of what remains, 157 MB is the
path-history index, 44 MB skiplists, and the rest changesets, mappings and
table overhead.
Queries on v3 (fresh server, repeat; a Linux import was running beside them):
`last-change Makefile` 4.4 ms, `last-change COPYING` 7.4 ms, a path that
never existed 2.7 ms, `log path=Makefile` 50 entries 91 ms, `blame Makefile`
0.36 s, merge-base 5-8 ms: the same as v2 within the noise of the shared
machine.

Queries against a freshly started server on that database (`main` at
generation 26,484), first request and the repeat. A Linux kernel import ran
on the same machine during these runs, so times are somewhat inflated; the two
passes agreed within 20%.

| Query | First | Repeat |
|---|---|---|
| `log` 100 | 75–81 ms | 34–44 ms |
| `log` 10,000 (19.5 MB of JSON) | 0.75–0.77 s | 0.64–0.69 s |
| `last-change Makefile` (changed often) | 6–8 ms | 2.3–2.6 ms |
| `last-change COPYING` (changed rarely) | 9–11 ms | 7.0–7.5 ms |
| `log` of a path that never existed | 2–3 ms | 1.2–1.5 ms |
| `log path=Makefile` 50 | 0.17–0.22 s | 90–94 ms |
| `blame Makefile` (derived once, 6.7 s, then stored) | 0.31–0.35 s | 0.34 s |
| `blame COPYING` | 3–6 ms | 3–5 ms |
| `diff` against 1,000 commits back | 18–65 ms | 11–12 ms |
| `merge-base` with `master~20000` | 2.2 s (3 ms after the fix below) | 0.11–0.14 s |
| `merge-base` with `master~1000` | **70 s** (6 ms after the fix below) | 0.10 s |
| `search strbuf_addf` (limit 100) | 0.29–0.30 s | 0.23–0.25 s |
| `log author=Junio` 100 | 69–82 ms | 85–87 ms |

The cold `merge-base` against an ancestor 1,000 commits back was a bug: the
fast path does not apply on a merge-heavy history, and the reference walk then
followed long-merged topic branches one round trip at a time. The repeat was
fast only because the walk's parents and generations were cached by then.
After the fix ("Merge base" below), the same database on a freshly started
server answers `master~1000` in 6 ms, `master~20000` in 3 ms and the root in
5 ms.

### Against other version control systems

The same history and queries with git (as cloned, and with a
commit-graph), jj 0.46 (`jj git clone`), Sapling (`sl clone --git`), Fossil
(`git fast-export --reencode=yes | fossil import --git`) and Mercurial 6.7
(`hg convert`), each command run twice on the same VPS, repeat times shown.
Leep is the server above, measured over HTTP on loopback, so it includes
JSON encoding and transfer; the others are local command lines and include
process start (about 0.25 s for Mercurial, 0.3 s for Sapling). A dash means
not measured.

| Query, repeat | Leep | git | git + commit-graph | jj | Sapling | Fossil | Mercurial |
|---|---|---|---|---|---|---|---|
| `log` 100 | 34–44 ms | 5 ms | 5 ms | 0.31 s | 0.36 s | 7 ms | 0.28 s |
| `log` 10,000 | 0.64 s | 0.15 s | 0.15 s | 26 s | 2.6 s | — | 2.0 s |
| `last-change Makefile` | 2.5 ms | 5 ms | 4 ms | 23 s | 2.5 s | 66 ms | 0.28 s |
| `last-change COPYING` | 7 ms | 0.35 s | 65 ms | 18 s | 0.56 s | 5 ms | 0.40 s |
| Path that never existed | 1.3 ms | 0.89 s | 50 ms | 19 s | — | 6 ms | 0.44 s |
| `log path=Makefile` 50 | 92 ms | 25 ms | 12 ms | 1.3 s | 2.5 s | 66 ms | 0.30 s |
| `blame Makefile` | 0.34 s | 1.5 s | 1.2 s | 27 s | 21 s | 1.0 s | 6.9 s |
| `blame COPYING` | 4 ms | 0.50 s | 94 ms | 18 s | 0.62 s | 49 ms | 0.35 s |
| `diff` 1,000 back | 11 ms | 24 ms | 24 ms | 31 ms | 0.37 s | — | 0.28 s |
| `merge-base` deep | 4 ms | 0.70 s | 0.11 s | 36 ms | 0.35 s | — | 0.26 s |
| `merge-base` 1,000 back | 4–7 ms | 0.17 s | 27 ms | 24 ms | 0.33 s | — | 0.26 s |
| Search / `grep` | 0.24 s | 0.18 s | 0.18 s | — | — | — | 2.3 s |
| `log author=…` 100 | 86 ms | 0.94 s | 0.89 s | 0.90 s | — | — | 0.29 s |

| Storage and import | Leep | git | jj | Sapling | Fossil | Mercurial |
|---|---|---|---|---|---|---|
| On disk | 483 MB database + 225 MB packed blobs (was 1.0 GB + 8.9 GB loose) | 328 MB | 444 MB | 386 MB | 512 MB | 404 MB |
| Import from git | 47 min (before the pipelined importer, #50) | — | 43 s | 43 s | 72 min | 84 min |

What this shows:

- **Per-path history is where Leep leads.** `last-change` and `log` of a rare
  or missing path answer from the path-history index in milliseconds whatever
  the history's depth; git without a commit-graph scans 0.35–0.9 s for them,
  and jj and Sapling take seconds to tens of seconds. Blame of a hot file is
  3–4x faster than git's once stored, and blame of a rare file is two orders
  of magnitude faster. Author filters are 3x faster than Mercurial and 10x
  faster than the others.
- **Full-history listing is where git leads.** `log` 10,000 is 4x slower than
  git: Leep returns 19.5 MB of JSON over HTTP where git prints a line per
  commit from a memory-mapped pack. Short `log`s pay a round trip and JSON
  encoding that a local command does not.
- **Storage was the largest gap.** Loose, uncompressed blobs made Leep 27x
  larger than git. With packed blobs (ADR 0029, "Packed blobs" below) and
  patched path-history rows (ADR 0037), all of git/git takes 708 MB (483 MB
  database, 225 MB blobs on disk) against git's 328 MB, down from about
  10 GB. That 2.2x buys the per-path index, skiplists and mappings git does
  not keep.
- **Not compared:** Scion. There was no build to run on this machine, and
  published figures from other hardware and other histories are not
  comparable with these.

### Writing changesets

ADR 0031 stores a changeset with one round of batched reads and one write
(changeset, root tree, generation and skiplist rows together, plus the rebase
link or the git mapping), checks the condemnation rows of new tree nodes and
uploaded blobs in batches, and caches skiplists so building a new one does not
read one ancestor's list per level. Same 3,795-commit slice, local Postgres 16
over a Unix socket, release builds, filesystem blobs, `--warm-interval 0`.
Statements were counted with `log_statement = all` on a scratch database;
timings were taken with logging off, on fresh imports made by each version,
two rounds alternating old and new (the machine is shared, so compare within a
round).

| | Before | After |
|---|---|---|
| `leep-git import` of the slice (3,795 commits, 6,850 blobs), two runs | 42.6 s / 38.7 s | 14.1 s / 13.8 s |
| Statements per imported commit | 38.3 | 7.8 |
| Same import of the first 914 commits, SQLite | 8.5 s | 4.4 s |

Per commit, the import made 27.4 single derived reads (12.6 skiplist, 5.4
`blob-condemned-v1`, 5.0 generation, one root tree, one mapping lookup), 3.1
changeset reads and 7.8 writes. It now makes three batched reads, the parent
read, one root tree read, the mapping lookup of the planning walk and two
writes (the changeset with its rows and mapping; the commit's blob claims).
The imported data is byte-identical to the old importer's: same checksum of
every changeset, `root_tree`, `generation`, `skiplist` and mapping row, same
claim keys and the same blobs.

HTTP, driven by a script that per iteration creates a changeset `x` on the
head and a sibling `y` (one existing blob at a new path three directories
deep), lands `x` (a fast-forward) and then `y` (a replay of one changeset onto
`x`). Latency is the median over 100 iterations after 20 warm-up ones, on a
freshly started server; statements are per request, over 20 iterations right
after start ("cold") and over 20 after another 100 ("steady"):

| Request | Statements, before | Statements, after (cold / steady) | p50, before (two rounds) | p50, after |
|---|---|---|---|---|
| `POST /v1/changesets` | 34 | 11.7 / 7.4 | 13.4 / 9.4 ms | 5.1 / 4.1 ms |
| `POST /v1/land`, replaying one changeset | 50 | 26.6 / 22.4 | 17.4 / 12.8 ms | 9.0 / 7.5 ms |
| `POST /v1/land`, fast-forward | 9 | 8.1 / 8 | 2.9 / 2.3 ms | 2.5 / 2.2 ms |

A create used to make 27 single derived reads, three changeset reads and four
writes. Now, at steady state, it makes two batched condemnation reads (the
contents, then the new tree nodes), one batched parent read, the first
parent's root tree, the one write, and two or three skiplist reads for the
chain entries deep enough that the server has not seen them yet. A server
that has just started reads more of those (about log2 of the history, as
every create did before) until it has worked through that stretch. The fast
land is the ref CAS (five statements, unchanged by design) and three reads.
The replaying land still reads its merge base, tree merge and landed-copy scan
one row at a time; those are reads outside the write path.

The first import with batched claims failed with `deadlock detected`: commits
uploaded ahead of the writer wrote claim batches that shared blobs in
different orders. Batched statements now insert in `(kind, key)` order (ADR
0031); a test races overlapping batches on every backend.

### Importer memory

The import of all of Linux grew to about 8.5 GB and was restarted by a
guard, while its own maps (blobs by git id, commits imported this run)
stay well under 1 GB. Profiled with heaptrack and `/proc/<pid>/status`
sampled every 2 s on two local imports (SQLite metadata, filesystem blobs,
4 vCPU sandbox):

- **Whole commits buffered.** A commit's new blobs were all read into memory
  before any was uploaded, and 16 commits run ahead. Linux's first commit
  adds 187 MB in 17,118 files; heaptrack's peak heap was 222 MB, almost all
  of it that buffer. Blobs are now read and uploaded in 8 MB slices.
- **Allocator retention.** glibc keeps freed memory in per-thread arenas (up
  to eight per CPU), and the importer frees blob buffers on other threads
  than the ones that read them. After the slices, heap peaked at 59 MB but
  anonymous memory still reached 243 MB and kept growing; with two arenas it
  ended at 78 MB, and the import ran faster. `leep-git` now restarts itself
  with `MALLOC_ARENA_MAX=2` unless it is set.
- **Mapped packs.** gix maps the pack files, and every page read stays
  resident in the process (file-backed, reclaimable, but counted by RSS-based
  guards). Over all of Linux that approaches the whole multi-gigabyte pack.
  The importer now opens a fresh repository handle every 20,000 commits,
  which unmaps what the previous one read.

| | Before | After |
|---|---|---|
| git/git to v1.7.0 (21,205 commits): time, peak RSS, anonymous at the end | 3:16, 165 MB, 97 MB | **2:29, 98 MB, 45 MB** |
| Linux to v2.6.16 (21,109 commits, 76,000 blobs): time, peak RSS | 5:30, 496 MB | **3:56, 212 MB** |
| Linux slice: RSS at 1, 2, 4 min, and at the end (anonymous + file) | 428, 463, 484 MB; 353 + 130 MB | **151, 173, 193 MB; 78 + 73 MB** |

Both importers produced the same head changeset. What still grows with
history is the blob map (about 100 bytes per distinct blob, a few hundred
MB for Linux) and the commit map, both well under 1 GB.

### Merge base

On all of git/git (82,500 commits) on the VPS, `GET /v1/merge-base` of `main`
and `master~1000` took 70 s on a freshly started server (31 s with the
machine less loaded) and 0.10 s once the process had cached the ancestry it
walked; `main` against `master~20000` took 2.2 s. `master~1000` is an
ancestor of `main`, so the answer is `master~1000` itself. git/git merges
everywhere, so the first-parent shortcut never applied and the request fell
to the reference walk, which painted both histories down until no unstale
entry was left. Topic branches merged long ago kept one-sided entries alive
deep into history, every step was one metadata round trip for the parents
and one per new generation, and the candidates were then filtered with
pairwise `is_ancestor` calls, each a one-read-per-step search of its own.

Three changes, with the same answers in every case:

- The best common ancestor is the one with the highest (generation, id), and
  the walk pops changesets in that order with their marks final, so the
  first changeset both sides reach is the answer. The walk stops there (or
  when either side has nothing queued) and the pairwise filter is gone.
  A randomized test checks this against a copy of the old walk on DAGs with
  criss-crosses, octopus merges and unrelated roots, cold and warm, with and
  without batched reads.
- The first-parent common ancestor found by skiplist jumps is a lower bound.
  When it is one of the two inputs, that input is an ancestor of the other
  and is returned without a walk: `main` against `master~1000` is a few
  skiplist reads. Its search stops after 64 reads, since long parallel
  first-parent stretches were read one round trip per step; the walk does
  better on those.
- The walk reads parents and generations through the log walk's batched
  read-ahead (ADR 0030) and keeps them in the ancestry cache. `is_ancestor`
  falls back to the same walk instead of a one-read-per-step search.

On the first 3,795 commits of git/git in a scratch copy of the database
(local Postgres 16 over a Unix socket, release builds, `--warm-interval 0`,
statements counted with `log_statement = all`): a freshly started server per
case, then the same request again. Times are medians of three runs.

| `GET /v1/merge-base` | Before: cold / warm | After: cold / warm | Statements, cold (warm) |
|---|---|---|---|
| head vs `head~1000` | 0.92 s / 8 ms | 6 ms / 3 ms | 7,385 (7) -> 8 (6) |
| head vs root | 1.02 s / 6 ms | 5 ms / 2.5 ms | 7,593 (3) -> 4 (2) |
| head vs a topic merged at `head~841` (an ancestor through a second parent) | 0.95 s / 8 ms | 0.11 s / 11 ms | 7,345 (20) -> 134 (20) |
| parents of the merge at `head~121` | 44 ms / 5 ms | 34 ms / 6 ms | 245 (18) -> 40 (18) |
| parents of the merge at `head~446` | 0.16 s / 20 ms | 27 ms / 14 ms | 1,302 (149) -> 73 (64) |
| parents of a merge whose base is 3,326 commits back | 0.86 s / 25 ms | 0.11 s / 17 ms | 7,017 (186) -> 192 (64) |

Before, a cold request read the whole slice twice over whatever the answer;
on all of git/git that is the 70 s above. After, the two ancestor cases cost
a logarithmic number of reads, and the others a batch per window of the
changesets above the answer. Warm requests still read skiplist rows, which
are not cached in memory. Deriving the path-history index for the slice,
which calls `merge_base` at every merge and `is_ancestor` for paths changed
on both sides, gave byte-identical rows (23.6 s before, 22.3 s after).

The same shapes on a synthetic git-like history in the memory store (16,616
changesets, 6,053 on the first-parent chain, topics forked up to 400 back
and merged, some merging main first), each metadata call delayed by 1 ms
(about 2 ms with tokio's timer); `cargo test -p leep-derive --release --lib
merge_base_cold_cost -- --ignored --nocapture`:

| Pair | Before: cold, reads | After: cold, reads (single + batched) |
|---|---|---|
| head vs `head~1000` | 92 s, 43,013 | 18 ms, 8 + 0 |
| head vs root | 71 s, 33,171 | 19 ms, 9 + 0 |
| head vs a topic merged ~1,000 back | 16.5 s, 7,635 | 129 ms, 10 + 118 |
| head vs an open topic forked at `head~800` | 13.0 s, 5,980 | 125 ms, 19 + 96 |

Batched reads are issued three at a time (changesets, generations,
skiplists), so 118 of them are about 40 round trips. The in-memory
`merge_base` and `is_ancestor` benchmarks are unchanged (same machine, both
builds: 9-14 us and 5-6 us). `merge_base_on_a_git_like_history_reads_little`
keeps the read counts bounded.

## Packed blobs

`leep-server repack` (ADR 0029) on a copy of the first 3,795 commits of
git/git (a local Postgres 16, filesystem blobs; 4 vCPU Intel Xeon @ 2.10GHz,
15 GB RAM, release build), zstd level 9:

| | Bytes |
|---|---|
| Loose objects (11,735: 6,791 file versions, 4,944 tree nodes) | 91,168,650 |
| Pack | 3,100,053 |
| Index (43.1 bytes per object, ADR 0038) | 505,733 |
| Index before ADR 0038 (96 bytes per object) | 1,126,648 |
| Prototype's pack (ADR 0029), no index | 3.2 MB |
| `git pack-objects` of the same commits | 4.4 MB |

10,795 objects were stored as deltas and 940 whole; the mean delta chain is 12
entries (cap 32). The run took 8.4 s: 3.6 s walking history for bases, 4.6 s
compressing and writing (single-threaded then, two compressions per object to
pick the smaller; compression now runs on every CPU, see "Repack memory and
time"), 0.2 s deleting the loose copies. `verify --full --rehash`
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
reads is the deepest chains plus loading the index (1.1 MB then) on the first call.
Two implementation choices mattered: one zstd decoder per thread instead of
one per frame (first-read mean 340 us before), and reading the loose file and
the pack in one blocking task (one thread-pool hop per `get`, as for a loose
store).

#### Index size, lookups and memory (ADR 0038)

The compact index (`LEEPIDX2`) against the original 96-byte records
(`LEEPIDX1`). Slice figures come from repacking the same store with each
binary; the pack bytes are identical. The 5-million-object rows come from a
synthetic index: random hashes, 100 to 1,000-byte frames, 15 in 16 objects
deltas
(`cargo run --release -p leep-store --example pack_index_scale -- build
<file> 5000000`, then `-- load <file>` in a fresh process, 1,000,000 random
lookups of present and of absent hashes; two runs agreed within 10%).

| | Before | After |
|---|---|---|
| Slice index (11,735 objects) | 1,126,648 bytes, 96.0 per object | **505,733 bytes, 43.1 per object** (widths 3+2+2+3+1) |
| 5M synthetic index on disk | 96.0 bytes per object | **46.0** |
| Resident memory of a loaded index | 104 bytes per object (decoded to a `Vec`) | **46.0** (file bytes searched in place); an old index read by the new code: 100 |
| Load and check, 5M objects | 10.9 s (one binary search per base) | **1.5 s**; an old index read by the new code: 6.8 s |
| Lookup, 5M objects, present / absent | 1.8 / 2.2 us | **0.9 / 0.8 us** (fanout table, dense hash column); old index, new code: 1.3 / 1.4 us |
| Lookup, 326,731 objects, present / absent | 0.85 / 1.0 us | **0.30 / 0.27 us** |
| `get` of 1,000 random objects on the slice, first read mean / again mean (three seeds) | 161-221 / 33-44 us | 161-204 / 33-35 us; old index, new code: 204-223 / 34-41 us |

`get` latency does not change: decoding the delta chain dominates it, and
the lookup is under 1 us. The index's bytes per object follow the field
widths of each pack: 32 for the hash plus offset (bytes to hold the pack's
length), stored length, base (bytes to hold the object count), logical
length and modification time (bytes to hold the span from the earliest).
Projected for the measured stores below, the 4-byte offsets and 3-byte
bases, stored and logical lengths, and 2- or 3-byte modification times (a
span under 194 days) give 47 to 49 bytes per object:

| Store | Objects | Index before (measured) | Index after (projected) | Share of pack + index, before / after |
|---|---|---|---|---|
| All of git/git | 326,731 | 31.4 MB | 15.4-16.0 MB | 16% / 8-9% |
| Linux, first 669,466 commits (1.24 GB pack) | 4,653,029 | 446.7 MB | 219-228 MB | 26% / 15% |

For the Linux slice, that is also about 260 MB less resident memory per
server process that has the pack open.

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
file contents, which now come through delta chains). Deriving indexes from
scratch over a packed store was not measured.

### All of git/git

`leep-server repack --json` on the VPS store of the whole history (82,500
changesets; server stopped, Linux import running beside it), default level 9
and fence:

| | |
|---|---|
| Loose objects before | 326,731, 8.69 GB logical, 8.9 GB on disk |
| Pack | 167 MB (167,467,024 bytes) |
| Index | 31 MB (31,366,264 bytes; about 15.5 MB with ADR 0038's layout, projected above) |
| Blob directory after | 190 MB logical, 225 MB on disk, no loose objects left |
| Stored as deltas / whole | 312,029 / 14,702 |
| Time | 27.4 min: 6.2 walking history, 10.4 writing, 10 fence, 0.7 cleanup |

That is 46x smaller than the loose store and smaller than git's 328 MB for the
same history; the index is 16% of pack plus index (8-9% with ADR 0038). Queries on a freshly
started server over the packed store, against the loose runs on the same
database just before the repack (current main, or the previous image for rows
it does not affect; first request / repeat):

| Query | Loose | Packed |
|---|---|---|
| `log` 100 | 122 / 53 ms | 52 / 38 ms |
| `log` 10,000 | 1.27 / 0.63 s | 0.79 / 0.71 s |
| Full file listing | 0.18 / 0.02 s | 0.46 / 0.01 s |
| `log path=Makefile` 50 | 0.17–0.22 / 0.09 s | 0.42 / 0.10 s |
| `blame Makefile` (stored) | 0.48 / 0.31 s | 0.37 / 0.39 s |
| `diff` 1,000 back | 18–65 / 11 ms | 115 / 15 ms |
| `search strbuf_addf` | 0.29 / 0.24 s | 0.45 / 0.24 s |

Repeat requests are unchanged; first requests that read many tree nodes or
file contents (listings, diffs, path logs, search) pay for decoding delta
chains until the decoded-object cache holds them.

### Linux, first 669,466 commits

The Linux kernel import was stopped at commit 669,466 (45% of 1,484,762) by
an importer bug (fixed in #66) and with 73 GB of loose blobs on a disk with
55 GB free. `leep-server repack --json` on that store, release image, same
VPS, default level 9 and fence:

| | |
|---|---|
| Loose objects before | 4,653,029; 66.0 GB logical, 73 GB on disk |
| Pack | 1.24 GB (1,244,502,985 bytes) |
| Index | 447 MB (446,690,872 bytes) |
| Blob directory after | 2.1 GB on disk, no loose objects left |
| Stored as deltas / whole | 4,417,908 / 235,121 |
| Time | 2 h 55 min: 49 min walking history, 1 h 45 min writing, 10 min fence, 10 min cleanup |

That is 35x smaller on disk, with no corrupt objects. Memory peaked at about
5 GB of the machine's 11 GB. The index is 26% of pack plus index, so the 96-byte record (a
32-byte base hash per object) is the next thing to shrink for histories of
this size. ADR 0038's index layout stores about 47-49 bytes per object
instead (projected 219-228 MB for this pack, 15% of pack plus index; see
"Packed blobs"). The import then resumed from the packed store.

### All of Linux

The whole Linux kernel history: 1,484,762 commits on `master` (head
generation 208,600), imported into Postgres 16 with filesystem blobs on a
4-vCPU, 12 GB VPS (149 GB data disk) that also runs other services.

**Import and storage.** The import stopped once at 1,442,634 commits when
the disk guard hit 86%; a full repack (ADR 0038 index, `leep:3e1966e`) then
took the store from 104 GB of loose objects to one 2.74 GB pack plus a
484 MB index (48 bytes per object for 10.08M objects) in 4 h 55 min
(1 h 23 min walking history), and the last 42,128 commits imported on top.

| Store, all of Linux | Size |
|---|---|
| git repository (bare clone) | 3.8 GB |
| Leep blobs: pack + index | 3.2 GB (plus loose objects of the last 42k commits until the next repack) |
| Leep Postgres, before path history | 8.3 GB (`changesets` 2.9 GB, skiplists 0.9 GB) |
| Leep Postgres, before path history, changesets compressed (ADR 0053, projected) | about 7.1 GB (`changesets` about 1.6-1.75 GB; see "Changeset compression") |
| Leep Postgres, with the full path-history index | 27 GB (`history-node-v3`: 25.4M rows, 24 GB of values) |
| Leep Postgres, all indexes (v4, log filter, search), changesets compressed | 11 GB (`changesets` 1.86 GB; "After #93 to #98") |

The path-history index costs about 16 KB per changeset on Linux against
1.9 KB on git/git: Linux's directories hold thousands of names and its merges
bring in hundreds of paths from the second parent. It is now the largest
thing Leep stores and the next storage target.

**Repack after #80.** A second full repack over the finished store (10,358,516
objects, 329,002 of them loose), `leep:a3a75de`, 4 threads:

| Full repack of all of Linux | #69 (first repack) | #80 |
|---|---|---|
| Objects | 10.08M | 10.36M |
| Walk | 1 h 23 min | 23 min |
| Write | about 3 h 20 min | 44 min |
| Total, with the 10 min fence | 4 h 55 min | 1 h 18 min |
| Peak memory (anonymous, page cache excluded) | 6.2 GiB | 2.16 GiB |
| Pack + index | 2.74 GB + 484 MB | 2.82 GB + 508 MB |

9.73M of the 10.36M frames were copied from the old pack without
recompressing. The blob directory went from 11 GB (the old pack plus the
loose objects of the last 42k commits) to 3.7 GB.

**Building the path-history index.** The background warmer derived it for
`master` while the server ran (5 GB memory cap):

| Stage | Commits | Rate |
|---|---|---|
| Before #72 (first attempt) | 19,504 in 50 min | ~390/min, 7.25 GiB and growing; stopped |
| #72, stale planner statistics | | ~500/min, Postgres at 100% CPU |
| #72 after `ANALYZE` (#73, #75 make it permanent) | to 611,689 | ~2,500/min average |
| #78 | to 1,484,762 | 4,400–5,500/min on most of history, 2,000–2,600/min on the last, merge-heavy third |
| One merge (`7214dd4e`, work.thaw) | | about 30 min alone; #81 addresses the cause |

Memory stayed between 0.9 and 2.5 GiB throughout. From the first attempt's
projected 63 hours, the index took about 9.5 hours of wall time across the
code changes (including two restarts).

**Queries**, `leep:e643adf` (#78) over the finished index, packed store,
cold (first request after a restart) and warm:

| Query | Cold | Warm |
|---|---|---|
| `log` limit 100 | 0.28 s | 0.055 s |
| `log` limit 10,000 | 5.5 s | 1.27 s |
| full file listing (~90k files, 15 MB) | 14.4 s | 0.17 s |
| root tree / `Makefile` contents | 20 / 2.6 ms | 1.5 / 1.6 ms |
| `last-change Makefile` | 6.9 ms | 2.0 ms |
| `log path=Makefile` limit 50 | 0.40 s | 0.20 s |
| `last-change COPYING` (rare) | 55 ms | 38 ms |
| `log path=COPYING` limit 50 | 1.24 s | 0.84 s |
| `log` of a path that never existed | 184 ms | 1.8 ms |
| `blame Makefile` (derived once, then stored) | 47 s | 0.13 s |
| `blame COPYING` | 166 ms | 17 ms |
| diff head vs 1,000 back | 3.8 s | 0.17 s |
| diff head vs 20,000 back | 19.7 s | 0.36 s |
| merge-base head vs 1,000 back / 20,000 back / root | 54 / 4.9 / 8.5 ms | 2.9 / 2.7 / 4.0 ms |
| search a common token (limit 100) / a rare token | 1.6 s / 0.61 s | 1.5 s / 0.64 s |
| provenance, limit 10,000 | 0.98 s | 0.97 s |
| `log author=Junio` limit 100 | 154 s | 151 s |

Indexed history queries answered in milliseconds on 1.48M commits; the slow
rows were a cold full listing or deep diff (thousands of tree nodes decoded
one at a time), the first `blame` of a long-lived file, and `log author=`,
which had no index. The run below, after #84 to #91, measures the fixes.

#### After #84 to #91

`leep:dcf16c0` (main after #91) on the same store and VPS, which this run
had with two CPUs available to containers (`nproc` 2) and other services
beside it. Same query script, cold (first request after a restart) and warm.

**Path-history index v4** (ADR 0045, #84), rederived beside v3 with
`repair rederive --kind history-tree-v4` (`leep:b03750c`, before parallel
derivation):

| | v3 | v4 |
|---|---|---|
| Rows | 25.4M nodes + 1.48M trees | 233,792 parts + 1,484,762 trees |
| Values | 24 GB | 2.68 GB + 1.63 GB = 4.3 GB (5.6x smaller) |
| Build | | 10 h 6 min, peak 2.2 GiB, 0 rows that could not be recomputed |

The v3 rows are still in the database (33 GB in all); dropping them with
`repair drop --kind history-tree-v3` and `VACUUM FULL derived` is the last
step of the upgrade (OPERATIONS.md, "Upgrading") and was left out of this
run.

**Log filter index** (ADR 0047, #86): the first attempt (main at #90)
derived 636k changesets in 20 minutes, then stalled for over an hour with
memory growing to 3 GiB. `perf` showed the time in `merge_base` called per
chain of large back-merges (mainline merged into a topic branch, such as
"Merge tag 'v4.9-rc3' into xtensa-for-next", which brings in 30,248 commits
with 2,061 merges). #91 (ADR 0051) walks each merge once. Resumed with
`repair rederive --kind log-filter-v1 --missing --jobs 2`, the remaining
846,554 changesets took 12 min 37 s (1,118 a second), peak 595 MiB. The
index is 635 MB for 1,484,762 rows (430 bytes a changeset) plus 43,523
name rows (1.6 MB).

**Queries**, before (`leep:e643adf`, above) and after:

| Query | Before, cold / warm | After, cold / warm |
|---|---|---|
| `log` limit 100 | 0.28 s / 0.055 s | 0.23 s / 0.047 s |
| `log` limit 10,000 | 5.5 s / 1.27 s | 4.6 s / 1.19 s |
| full file listing (~90k files, 15 MB) | 14.4 s / 0.17 s | **1.35 s** / 0.21 s |
| root tree / `Makefile` contents | 20 / 2.6 ms | 72 / 9 ms cold, 1.8 / 2.0 ms warm |
| `last-change Makefile` | 6.9 / 2.0 ms | 6.3 / 2.0 ms |
| `log path=Makefile` limit 50 | 0.40 s / 0.20 s | 0.41 s / 0.17 s |
| `log path=COPYING` limit 50 | 1.24 s / 0.84 s | 1.15 s / 0.70 s |
| `blame Makefile` | 47 s (first, derives rows) / 0.13 s | 0.36 s (rows stored earlier) / 0.085 s |
| diff head vs 1,000 back | 3.8 s / 0.17 s | **0.70 s** / 0.20 s |
| diff head vs 20,000 back | 19.7 s / 0.36 s | **1.18 s** / 0.40 s |
| merge-base head vs 1,000 back / root | 54 / 8.5 ms | 167 / 3.9 ms cold, 5.0 / 3.7 ms warm |
| search a common token / a rare token | 1.6 s / 0.61 s | 3.3 s / 0.69 s cold, 1.4 s / 0.54 s warm |
| provenance, limit 10,000 | 0.98 s | 1.04 s / 0.96 s |
| `log author=Junio` limit 100 | 154 s / 151 s | **2.6 s / 0.52 s** |

The cold listing is 11x faster and the deep diff 17x, from batched tree
reads (#88); a rare author's log is 59x faster cold and 290x warm, from the
log filter index (#86, #91). The `blame Makefile` row is not a first blame:
its rows were stored by the earlier runs, so it measures reading them.
Search and merge-base cold times move with what the page cache held after
the restart.

**Concurrent lands** on this VPS (fresh database, `leep-land-bench`,
15 s per agent count):

| Changesets | Agents | Lands/s | Land p50 / p99 | Create p50 / p99 | 409s, CAS losses |
|---|---|---|---|---|---|
| 1 file | 1 | 76.2 | 2.1 / 3.4 ms | 6.9 / 9.1 ms | 0 |
| 1 file | 8 | 112.1 | 38 / 64 ms | 19 / 32 ms | 0 |
| 1 file | 64 | 152.7 | 233 / 405 ms | 120 / 192 ms | 0 |
| 10 files | 1 | 21.2 | 2.3 / 3.9 ms | 7.5 / 10.2 ms | 0 |
| 10 files | 8 | 40.7 | 59 / 97 ms | 28 / 48 ms | 0 |
| 10 files | 64 | 51.9 | 268 / 629 ms | 227 / 271 ms | 0 |

At 64 agents with one-file changesets that is 153 lands a second against
117 after #77 on this machine, with batches of 31 lands on average.

#### After #93 to #98

`leep:2d62448` (main after #95) on the same store and VPS, after the
retired v3 rows were dropped (`repair drop --kind history-tree-v3`, 1 h 36 min
before #93's storage-order walk, then `VACUUM FULL derived`: 33 GB to 12 GB).
Before is `leep:dcf16c0` on that same 12 GB store, so the rows below compare
code, not index sizes.

**Upgrade plan.** `leep-server upgrade-index` (#96) reported nothing to do:
every index this version reads is complete for the 1,484,762 changesets
reachable from `master`, and no retired rows remain. It took 7 min 11 s, most
of it `SELECT count(*) FROM derived WHERE kind = $1` (about 32 s per large
kind on this disk); the counts are what a plan prints, and an estimate from
`pg_class` would do for the plan.

**Changeset compression** (ADR 0053, #94), servers stopped:

| | Plain | Compressed |
|---|---|---|
| `compact-changesets --dry-run` | | 3.10 GB to 1.65 GB of values (1.87x) in 3 min 25 s |
| `compact-changesets` | | 1,484,762 rows rewritten in 5 min 25 s, one 64 KiB dictionary |
| `changesets` table after `VACUUM FULL changesets` (2 min 7 s) | 2.87 GB | 1.86 GB |
| database | 12 GB | 11 GB |

The 1.87x is above the 1.6 to 1.75 GB projected from the last 117,680
commits: older commits are shorter and more alike.

**Queries**, one cold and one warm pass each (single samples; the VPS runs
other services):

| Query | Before, cold / warm | After, cold / warm |
|---|---|---|
| `log` limit 100 | 0.31 s / 0.038 s | 0.31 s / 0.034 s |
| `log` limit 10,000 | 6.2 s / 1.15 s | **4.3 s / 0.90 s** |
| full file listing | 1.11 s / 0.19 s | 0.74 s / 0.18 s |
| `log path=COPYING` limit 50 | 1.09 s / 0.69 s | 0.88 s / 0.59 s |
| `blame Makefile` (rows stored earlier) | 0.48 s / 0.083 s | 0.19 s / 0.073 s |
| diff head vs 1,000 back | 0.58 s / 0.21 s | 0.59 s / 0.21 s |
| diff head vs 20,000 back | 0.71 s / 0.39 s | 1.15 s / 0.38 s |
| search a common token (`strbuf_addf`, limit 100) | 1.95 s / 1.49 s | 1.94 s / **0.42 s** |
| search a rare token | 0.57 s / 0.59 s | **0.19 s / 0.14 s** |
| provenance, limit 10,000 | 1.04 s / 1.02 s | 0.80 s / 0.81 s |
| `log author=Junio` limit 100 | 1.83 s / 0.55 s | 2.38 s / 0.55 s |

Search is 3.5x faster warm for a common token and 3 to 4x for a rare one
(#95). The cold common-token search is unchanged: it is spent reading the
filters and files from a cold page cache. The 10,000-entry log is 1.4x
faster cold and 1.3x warm, as on the sandbox; its cold time is the decoding of
12,481 changesets, now from zstd frames. The `blame Makefile` row reads stored rows,
so it does not measure #97's first blame (git/git, "First blame in
stretches"). The cold deep diff and the cold author log were slower in this
pass; a second pass right after `verify` put them at 0.63 s and 1.95 s cold
(0.35 s and 0.53 s warm), against 0.71 s and 1.83 s before, so neither
moved beyond the noise of single cold samples on this machine. That pass
also had a common-token search at 1.29 s cold / 0.40 s warm, a rare token at
0.18 s / 0.14 s, and provenance at 0.72 s.

**Concurrent lands** (fresh database, `leep-land-bench`, 15 s per agent
count), before and after:

| Changesets | Agents | Lands/s before | Lands/s after | Land p50 / p99 after | 409s, CAS losses |
|---|---|---|---|---|---|
| 1 file | 1 | 74.1 | 68.9 | 2.3 / 6.0 ms | 0 |
| 1 file | 8 | 109.8 | 100.7 | 41 / 71 ms | 0 |
| 1 file | 64 | 147.8 | 141.6 | 248 / 429 ms | 0 |
| 10 files | 1 | 21.7 | 20.9 | 2.3 / 4.4 ms | 0 |
| 10 files | 8 | 39.1 | 38.3 | 64 / 103 ms | 0 |
| 10 files | 64 | 47.9 | 47.6 | 260 / 589 ms | 0 |

Ten-file lands are unchanged. One-file lands are 4 to 8% slower, in line with
compressing each new changeset row before it is written; single runs on
this machine vary by about that much.

**Verify** (sample mode, 1 h 42 min against 1 h 46 min before compression)
was clean: 1,484,762 changesets (each one read back from its zstd frame and
its id re-hashed), 7,164,424 tree nodes and 3,194,092 blobs, and about 256
recomputed rows per derived kind with none missing or mismatched. The 29
missing `blame` rows are blames never asked for, which are derived on
demand.

### Incremental repack

ADR 0039, measured with `cargo run --release -p leep-server --example
incremental_repack -- <dir>`, which grows a synthetic history in a fresh
store (SQLite metadata, filesystem blobs, 20 directories, three files edited
per commit, one commit in twenty adds a file). It lands 10,000 commits with
an incremental repack every 500, then 20 rounds of 100 commits each followed
by an incremental repack, then one `--full` (sandbox VM, release build, no
fence):

| | Walk | Write | Total | Packs after |
|---|---|---|---|---|
| Incremental, 500 new commits (history up to 10,000) | 0.42–0.89 s | 0.88–1.56 s | 1.7–2.9 s | 1–3 |
| Incremental, 100 new commits (history 10,100–12,000), 15 rounds that only pack loose objects or merge small packs | 0.10–0.20 s | 0.17–0.32 s | 0.36–0.59 s | 2–5 |
| The same, the round that merged 5 packs (72,631 objects, 71,945 frames copied as they were) | 0.12 s | 1.35 s | 1.61 s | 1 |
| Full, 12,000 commits | 11.24 s | 4.08 s | 15.34 s | 1 |

The incremental walk stays at about 0.1 s per 100 new commits whatever the
history's depth, while the full walk grows with it. The pack count never
exceeded 5. Size: a store maintained only by incremental repacks had
10.44 MB of packs where `--full` gives 6.91 MB (indexes about 3.56 MB in
both), because the first new version of each path is written whole (its base
is in an older pack). Writing a whole copy of that base and a delta instead
was measured on every such chain head (about 256 per round): 3.6% larger
than writing the version whole.

### Repack memory and time

A full repack of all of Linux (10.08M objects) on the VPS peaked at about
6.2 GiB, roughly 620 bytes per object, and took 4 h 55 min (1 h 23 min
walking, the rest writing). Almost all of that memory was per-object
structures keyed by 32-byte hashes, each hash held several times over:

| Held during a full repack (bytes per object) | Before | After |
|---|---|---|
| Walk output (object, base) | 65 (`(Hash, Option<Hash>)`) | 8 (two `u32` ids) plus each hash once, 32 |
| Walk's seen set, roots, changeset maps | 40-80, plus 64 and about 350 per changeset | 1 bit, plus about 10 for the id table and 60 per changeset |
| Sources (loose path and mtime, packed entry) | 170-340 in a `HashMap<Hash, Source>`, plus about 100 of path per loose object | 16 in two arrays by id |
| Live set, placed set, write order, old bases | 40-80 + 40-80 + 65 + 65 | one sorted hash table of 32 (ids are ranks), 8 for the order, 1 bit placed |
| Written map and index records | 50-100 + 72 | 4 (position) + 1 (depth) + 28 (four arrays) |
| Index encoding | 8 + 45 (the whole new index in memory) | 4; the index is streamed to its file twice, once for its id |
| Old pack index, loaded by the store | 45 | 45 (unchanged) |
| Base cache | 256 MiB of data, plus about 150 bytes of bookkeeping per entry without a count limit (280 MB for small objects) | 256 MiB including bookkeeping, at most 131,072 entries |

Measured with `cargo run --release -p leep-store --example repack_scale`
(synthetic 350-byte objects in chains of 32, written in batches of 500,000
and packed incrementally, then `full` in a fresh process; peak is `VmHWM`;
4 vCPU sandbox, release build, the same input copied for each run):

| Full repack of a packed store | Before: peak / write | After: peak / write |
|---|---|---|
| 1,000,000 objects | 1,307 MB / 21.4 s | **315 MB** / 14.0 s |
| 3,000,000 objects | 2,230 MB / 58.4 s | **601 MB** / 48.0 s |
| Slope | 462 bytes per object | **143 bytes per object** (45 of them the old pack's index) |

Projected to Linux's 10.08M objects: about 1.6 GB instead of 6.2 GiB. Every
pack and index above was byte-identical between the two binaries.

Time. Writing was single-threaded, and each object is compressed twice
(whole, and against its base, to keep the smaller), so zstd at level 9 took
nearly all of it. Compression now runs on one thread per CPU
(`--threads`): a reader thread reads objects in order, worker threads make
both candidate frames, and a writer thread picks between them in order with
the same rules (the base written, within the chain cap, the delta smaller),
so the bytes do not depend on the thread count. Whole frames reuse one zstd
context per thread (about 15% of single-threaded write time went to creating
contexts and clearing their tables); a delta's context borrows its base, so
it is still made per frame. The walk reads changesets and root trees in
batches of 1,000 and reads tree nodes through the shared tree cache (the
parent's node is usually the one just read). The fsyncs are one per pack and
index and do not show in profiles.

Full repacks on SQLite stores imported with `leep-git import`, the same
input copied for each run (4 vCPU sandbox with another build running beside
it; walk / write / total, peak RSS):

| | Before | After |
|---|---|---|
| git/git to v1.7.0: 21,205 changesets, 71,122 loose objects (1.21 GB) | 33.0 / 85.6 / 120.7 s, 405 MB | **12.7 / 24.4 / 39.1 s**, 433 MB |
| Linux to v2.6.16: 21,109 changesets, 168,402 loose objects | 52.7 / 163.4 / 220.3 s, 484 MB | **27.4 / 39.4 / 71.4 s**, 501 MB |
| git/git v1.7.0 packed plus v1.8.0 loose (38,796), incremental, merging the pack | 21.6 / 48.1 / 70.7 s, 486 MB | **13.2 / 18.2 / 32.7 s**, 530 MB |
| The same store, `--full` | 47.3 / 47.4 / 95.7 s, 537 MB | **23.6 / 19.2 / 43.7 s**, 565 MB |

The pack and index files were byte-identical (same checksums) in every
row. At these sizes the fixed caches (256 MiB of bases, 64 MiB of decoded
objects, 32 MiB of tree nodes) dominate peak memory, and the new writer
holds up to 64 MiB of objects in flight and a zstd context per thread, so
peak RSS is 5 to 10% higher; the per-object savings show from about a
million objects. With every frame copied from an old pack (the synthetic
runs above), compression is idle and the single reader, which decodes each
object and checks its hash, sets the pace. Decoding on the worker threads
is the next step for merge-heavy repacks.

## Cold queries

The cold full listing (14.4 s), deep diff (19.7 s) and first `blame
Makefile` (47 s) on all of Linux, reproduced on a synthetic history with
`cargo run --release -p leep-server --example cold_queries`, which builds a
Linux-shaped history (`build`), and times one query on freshly opened
stores (`query`, a 256 MiB blob cache in front as in `leep-server`).

**The store.** `build --commits 60000` (stopped by a time limit at 57,345
changesets; `finish` then pointed `main` at the changeset of the highest
generation, 7,802, and derived its path history), then `leep-server repack
--full`: 706,331 objects, 599,701 of them deltas, a 73 MB pack and a 32 MB
index. At the head: 3,211 directories, 86,684 files, and a 1,825-line root
`Makefile` that 1,764 first-parent changesets changed, directly or by merge.
Commits per generation are 7.3, against 7.1 for Linux. Postgres 16 in a
private cluster, filesystem blobs, a 4-vCPU sandbox shared with other jobs.
Cold means: page cache dropped (`echo 3 > /proc/sys/vm/drop_caches`),
Postgres restarted, a new process.

**Where the time went**, `derive/history-index-size` (b03750c):

- *Listing and diff.* `list_files` read one tree node at a time, depth
  first; `diff` the same for both sides. Each node is the head of a delta
  chain of up to 32 frames (each tree node is a delta on its directory's
  previous version, so a head node decodes 16 on average), and the chain's
  frames, written with the changesets that made them, lie far apart in the
  pack: every frame was one synchronous `pread` from a cold disk. The
  listing spent 9.85 of 10.0 s in 3,211 sequential blob reads on a busy
  disk, about 2 s on a quiet one.
- *Blame.* About 18,600 sequential round trips for 1,896 examined
  changesets: per step a history-index lookup, the changers' generations,
  a skiplist descent from the top (3,800 skiplist reads), the changeset (for
  its parent), the parent's root tree and root node (to compare the file), a
  cached-blame lookup per changer (1,765), one file version read at a time,
  and a changeset read per distinct changeset at the end. Replaying the
  versions copied every line's text through every version (1,825 lines times
  1,764 versions), about 1.5 s of CPU.
- *Opening the pack.* The first read after a start checks the whole index:
  350 ms for 706,331 objects, a single-threaded pass of random reads of the
  index that grows with it (10.36M objects on Linux).

**Changes:**

- `BlobStore::get_many` (default: 32 `get`s at a time). The filesystem
  store splits it over up to 16 blocking tasks, each of which first plans all
  its chains from the index and asks the kernel for every frame
  (`posix_fadvise WILLNEED`) before decoding; `CachedBlobStore` serves hits
  and forwards misses in one call. A single `get` of a chain prefetches its
  frames too.
- `list_files` and `diff` walk breadth first and read each level (up to
  1,024 nodes) with one `get_many` (`Repo::read_trees`).
- Blame: the walk keeps every skiplist entry it has read by generation and
  starts each descent from the nearest known point, takes the parent from
  the skiplist instead of reading the changeset, and remembers skiplist
  generations; the files at the parents are read 64 steps at a time (one
  batched root-tree read, one `get_many` per directory level), cached blames
  32 changers at a time, versions 64 at a time with one `get_many` and
  diffed on four threads; lines carry (changeset, line) until the last
  version supplies their text; summaries are one `get_changesets`.
- The pack index check reads only the fields each check needs and runs on up
  to 8 threads for indexes of 65,536 objects or more (350 to 110-140 ms here).
- A decoded chain hashes each link once: links share the kind (tree node or
  file) of the link below, which is tried first.

**Results**, the same store, cold, median of three interleaved rounds (load
average 4-6 from other jobs):

| Query | Before | After |
|---|---|---|
| Full file listing (86,684 files) | 2.22 s | 0.51 s |
| `blame Makefile` (1,764 changers, nothing cached) | 10.48 s | 5.02 s |
| Diff head vs 1,000 first parents back (9,635 entries) | 2.74 s | 0.62 s |
| Diff head vs 3,000 back (18,786 entries) | 3.13 s | 0.73 s |

On a busy disk (load average 12, the first round measured) the listing went
from 10.0 to 1.3 s and the 1,000-back diff from 3.4 to 1.8 s. Blame's store
calls, cold:

| Calls | Before | After |
|---|---|---|
| history-index rows | 2,852 | 2,852 |
| skiplists | 3,800 | 1,926 |
| generations | 2,489 | 753 |
| changesets | 2,203 | 1 batch (307) |
| root trees | 1,896 | 30 batches (1,895) |
| cached-blame lookups | 1,765 | 56 batches (1,764) |
| blob reads | 3,660 | 58 `get_many` (3,659) + 1 |

Blame is now mostly the history-index lookup and skiplist read of each step,
which depend on the step before. On Linux the same causes apply at about 26
times the history (and a 508 MB index), so the 14.4 s, 19.7 s and 47 s rows
should fall by similar factors; they have not been remeasured there.

## Search and long logs

On all of Linux (above, "After #84 to #91") a search for a common token took
3.3 s cold and 1.4 s warm, and `log` limit 10,000 4.6 s cold and 1.19 s
warm. Reproduced on two local stores, Postgres 16 in a private cluster
(`fsync` off), filesystem blobs repacked with `repack --full`, release build,
a 4-vCPU sandbox shared with other builds (load average 7 to 13 throughout;
the server and Postgres ran at `nice -15`):

- *git/git*: all 82,513 commits imported with `leep-git import` (in four
  stages, repacking between them for disk), the search index derived with
  `derive-search main`; 4,737 files at the head.
- *Linux-shaped tree*: `cargo run --release -p leep-server --example
  cold_queries -- build <meta> <blobs> --commits 0` (82,765 files in 3,211
  directories, a few of them with thousands of files), repacked and indexed
  the same way.

Cold is: Postgres restarted, page cache dropped, a new server process, first
request. Warm is the median of 8 requests after it. Each row is the median of
3 (git/git) or 2 (Linux-shaped) such rounds, base and new interleaved.

**Where the time went.**

- *Search.* The walk opened one directory at a time: one `get_derived` for its
  filter, one `read_tree`, then one `get_derived` per file, 16 in flight. A
  search for a token near the end of the tree in path order (Linux's
  `strbuf_addf` lives in `tools/`) paid a round trip for every directory
  before it and one for every 16 files in directories its filters did not
  rule out: on the Linux-shaped tree, 1,879 files and 115 directories for one
  match. Matching ran on the task that polled the pipeline, one file at a
  time.
- *Log.* `log` limit 10,000 on git/git spent 350 ms walking and 65 ms
  encoding 19.5 MB of JSON (instrumented build, warm). The walk was 47
  read-ahead batches of 256 changesets, read one after another, at about
  7 ms each; most of that was decoding the answers on one thread (postcard,
  the id check, and a clone of every changeset in the store's batched read),
  not Postgres. Encoding allocated a `String` for every hash written.

**Changes.**

- Search opens a directory with one `get_derived_many` for its
  subdirectories' filters and one for its files' rows, then one `read_trees`
  for the subdirectories that survive, and opens the subdirectories of the
  directory it is in ahead of the walk, four at a time per search, on their
  own tasks (aborted if the search stops early). Matching runs on blocking
  threads. Results, their order and `truncated` are unchanged; under
  truncation `dirs_pruned` now also counts pruned siblings of the directories
  the walk entered.
- The log's read-ahead issues two batches at once on separate tasks when the
  walk expects to visit more than one batch, while the changesets it has read
  and not yet visited number fewer than the limit (so it reads about as far
  past the limit as before: 12,481 changesets for 10,000 entries on git/git,
  against about 12,000).
- Batched changeset reads hand each decoded changeset to its request instead
  of cloning it (only repeated ids clone).
- A log of more than 512 entries is encoded in chunks of 512 on blocking
  threads and streamed in order, after the walk has finished (so a failed walk
  still gets its error status). The body is byte-for-byte what one-piece
  encoding gives. Hashes are hex-encoded on the stack in every JSON response.

**Results** (ms, base `fc98b7f` / after; answers compared byte for byte):

| Query | Cold | Warm |
|---|---|---|
| git/git: search `strbuf_addf`, limit 100 | 160 / 158 | 97 / 54 |
| git/git: search `xdl_cleanup_merge` (1 file) | 150 / 88 | 79 / 35 |
| git/git: `log` limit 10,000 (19.5 MB) | 845 / 627 | 450 / 325 |
| git/git: `log` limit 100 | 67 / 60 | 16 / 14 |
| Linux-shaped: search `file 8165` (4 files near the end) | 324 / 140 | 229 / 36 |
| Linux-shaped: search `of file 81657 lorem` (1 file) | 334 / 164 | 184 / 40 |
| Linux-shaped: search `rem dolor` (trigrams in every file, no match; scans 256 MiB) | 11,665 / 5,145 | 11,239 / 5,085 |

The git/git searches are small (926 files considered); their cold time is
mostly reading the matched files' delta chains from a cold pack. The
Linux-shaped tree shows what the round trips cost on a large tree: 5 to 6
times faster warm and twice as fast cold. On all of Linux, where the large
directories' filters are saturated and the walk reaches thousands of
directories before `tools/`, the search should improve at least as much; it
has not been remeasured there. The long log is 1.4 times faster: two lanes
overlap the round trips, but decoding is the larger part and two threads is
all the VPS has. Its first byte arrives sooner than the whole body did
(470 to 319 ms warm in another interleaved run). The next step for the log is the store's decoding: every changeset
is decoded, re-encoded and hashed to check its id.

### First blame in stretches

The first blame of a long-lived file was still mostly round trips in a row
after the batching above: each step of the walk reads the index entry at a
changer's parent and a skiplist, and needs the step before it. Blame now
walks the first 32 steps from the tip as before, and meanwhile cuts the
first-parent chain into about 256 stretches; if the walk goes on, up to 64
stretches are walked at once below where it stopped, as separate tasks,
with their reads gathered into batched reads, and their steps are taken in
order (ARCHITECTURE.md, "Blame"). The replay splits each version into lines
and numbers them once instead of twice per diff, and reads each window of
versions while the one before is diffed.

**Measured** on all of git/git (82,513 changesets, imported with `leep-git`
into Postgres 16 in a private cluster, filesystem blobs repacked into one
167 MB pack, path history derived), `cold_queries query ... blame PATH`,
which drops all blame rows first, so every run is a first blame. Base is
main at #92. Warm: a new process with Postgres and the page cache warm;
cold: Postgres restarted and the page cache dropped. Median of three
interleaved runs, a 4-vCPU sandbox under load from other jobs (load
average 6 to 14), with a 16-step head (the 32 kept is below):

| File (first-parent changes, lines) | Warm before | Warm after | Cold before | Cold after |
|---|---|---|---|---|
| `Makefile` (1,501, 4,143) | 3.35 s | 1.18 s | 3.00 s | 1.13 s |
| `diff.c` (741, 7,860) | 1.82 s | 0.81 s | 1.92 s | 0.90 s |
| `revision.c` (504, 4,833) | 1.16 s | 0.57 s | 1.53 s | 0.83 s |
| `t/test-lib.sh` (381, 2,019) | 1.18 s | 0.55 s | 1.85 s | 1.19 s |
| `git-compat-util.h` (361, 1,144) | 0.92 s | 0.46 s | 1.50 s | 0.89 s |
| `ewah/bitmap.c` (17, 310) | 0.16 s | 0.18 s | 0.25 s | 0.32 s |
| `COPYING` (2, 360) | 0.06 s | 0.08 s | 0.09 s | 0.10 s |

With a 32-step head (cold, a separate interleaved round): `ewah/bitmap.c`
0.19 s against 0.19 s before, `Makefile` 1.84 s against 4.28 s and
`revision.c` 1.24 s against 2.31 s at a higher load. Stored blames are read
as before.

**Where the time went**, `Makefile`, cold: before, about 6,050 store calls,
nearly all single-row reads one after another (3,187 index rows, 2,053
skiplists, 677 generations), plus 48 batched blob reads. After, about 2,200
calls, most of them batches (757 index batches for 3,558 rows, 751
skiplist batches for 3,874). Each stretch's work no longer waits on the
others, so the walk is bound by what the process and Postgres can do: on
this machine about 0.85 s of CPU for the walk, half of it decoding root
trees (git/git's root directory holds hundreds of names, and each root
tree is a delta chain in the pack) and half decoding history-pack parts,
and 0.45 s for the replay, down from about 0.8 s. Steps tried along the
way, warm `Makefile`:

| Change | Time |
|---|---|
| Before | 2.9 to 3.6 s |
| 64 stretches walked 12 at a time in one task | 1.9 to 2.5 s |
| ... each stretch a task of its own | 1.5 to 2.3 s (the walk 0.9 to 1.3 s whatever the stretch count: 7,000 single-row reads a second was the limit) |
| ... reads gathered into batches | 1.3 to 1.8 s |
| ... diffs numbering lines once | 1.1 to 1.4 s (replay 0.9 to 0.35 to 0.55 s) |
| ... no read of the file at every cut up front (0.1 to 0.46 s of root trees) | 1.0 to 1.3 s |

More stretches (512, 1,024) were no faster on `Makefile` and slower on
rarely changed files, which pay a few reads per stretch: the head walk
keeps those as they were. A file whose stretches lie below its creation
stops each at its top's index entry.

All of Linux has not been remeasured; its root directory is small, so its
root trees decode faster than git/git's, and `Makefile`'s 1,800 changes
should take about what git/git's 1,501 do. What remains is CPU: decoding a
delta chain per root tree read (a `get_many` splits its hashes over tasks
that do not share decoded bases), history-pack parts, and the replay.


## Filtered log

ADR 0047. `log` with `author=` or `extra=` and no path used to test every
ancestor; it now scans 512 changesets plainly (longer while at least one in
150 matches) and continues with the `log-filter-v1` index, skipping
first-parent stretches that cannot hold a match. Measured with
`cargo run --release -p leep-server --example log_filter_profile --
<meta url> main --synth 200000 --authors 4000 --scan`: a synthetic history of
200,001 changesets in local Postgres 16 (Unix socket) on a shared 4-vCPU
machine with a load average around 8, built to be hard on the index: 24
long-lived branches, mainline merging one of them on 30% of its steps,
branches merging mainline back every ~67 commits, authors Zipf over 4,000
(`Developer 0` writes about 6.5% of changesets, `Developer 50` 0.22%), a third of changesets
carrying `agent.model` (Zipf over 8 models), `agent.session` (about 20
changesets each) and a prompt, and three rare authors only in the oldest 2%.
"First" is a fresh `Repo` (no caches), "repeat" the same query again; "scan"
is the old code (`log_scan`) on a fresh `Repo`. Every answer was checked
equal to the scan's. Limit 100 unless noted:

| Query | Matches | First | Repeat | Scan (before) |
|---|---|---|---|---|
| rare author (oldest 2%) | 2 | 0.72 s | 68 ms | 7.0 s |
| rare author, full name | 1 | 0.33 s | 59 ms | 7.3 s |
| tail author (`Developer 2000`) | 6 | 0.76 s | 0.19 s | 5.8 s |
| `author=nobody`, limit 10 | 0 | 0.14 s | 41 ms | 5.0 s |
| one `agent.session` | 12 | 0.33 s | 69 ms | 6.4 s |
| common author (`Developer 0`) | 100 | 0.12 s | 66 ms | 0.10 s |
| mid author (`Developer 50`) | 100 | 2.4 s | 0.60 s | 1.6 s |
| `--agent` (key present) | 100 | 34 ms | 30 ms | 25 ms |
| common / rare `--agent-model` | 100 | 67 / 165 ms | 63 / 165 ms | 57 / 157 ms |
| `Developer 3` and one model | 100 | 2.6 s | 0.77 s | 1.7 s |

Rare values go from a scan of the whole history to a few hundred
milliseconds cold and tens warm; frequent values stay on the scan, which
reaches the limit first. The walk visits about 20 changesets per match
(climbing and descending the levels, measured with the ignored
`profile_branchy_history` test: `Developer 50` visits 3,300 changesets for 100
matches where the scan reads 37,500), each a round trip when cold, so a
value of middling frequency, here one changeset in 450, is the worst case:
slower than the scan cold, faster once its rows are cached (64 MiB of
decoded rows and 32 MiB of skiplists per process). On this loaded machine a
cold round trip took about 1.2 ms; most of the cold time is those.

**Size and derivation.** 172 MB of rows for 200,001 changesets, 859 bytes
per changeset, plus 4,002 name rows (154 KB). The branches' back-merges are
what makes it this large: a branch's levels cover what mainline brought it,
about 1,500 changesets of other branches every 67 commits. On the same shape
in memory, 60,000 changesets take 626 bytes each. Deriving all 200,001 rows took 240 s against
Postgres (8.3 s for 60,000 in memory), mostly `merge_base` at merges. A first
version that used the side branch's own levels at merges (rather than
walking merges on the side exactly) stored 5.2 KB per changeset and skipped
almost nothing; walking each side with a merge base per nested chain took
48 s for the 60,000 in memory, and one shared walk of the first parent's
ancestors for nearby questions brought that to 8.3 s.

**Back-merges (ADR 0051).** On all of Linux the build stalled for hours at
back-merges such as "Merge tag 'v4.9-rc3' into xtensa-for-next" (30,248
changesets and 2,061 merges brought in): the side walk above made a
`merge_base` per inner merge whose branch was based long before the first
parent. Each merge is now one walk of both parents' histories, giving up
past 200,000 changesets. Measured with `cargo run --release -p leep-server
--example history_profile -- <meta url> mem:// main --releases 8 --next-back
4 --old-percent 30 --old-back 14 --direct 3 --log-filter --time-back-merges
--jobs 2`: 8 release cycles shaped like Linux's (400 subsystem branches a
cycle, 30% based on a release up to 14 cycles back, topic merges and merges
of mainline inside them, two `for-next` branches a cycle merging a release
four cycles newer back), 80,886 changesets, 14 back-merges each walking
about 80,000 changesets; local Postgres 16 over a Unix socket, 4 vCPUs, the
old code timed with a counter of its `merge_base` calls:

| | Before | After |
|---|---|---|
| One back-merge, parents derived first (oldest to newest) | 2.3 s to 35 s | 0.025 s to 0.11 s |
| The same on a fresh `Repo` (empty caches) | | 0.36 s to 1.6 s |
| `merge_base` calls for one merge's side, most | 1,423 | 0 |
| All 80,886 changesets, 2 jobs | 843 s (96/s), stalls of 40 s at 0/s | 59.5 s (1,360/s) |
| The same in memory | 549 s (147/s) | 43.8 s (1,845/s) |
| Single reads: changesets, skiplists | 451,734, 975,296 | 14, 0 |

The rows were byte for byte the old code's (all 83,856, compared in
memory). Merges of branches based many cycles back still walk the first
parent's history down to the base, about 80,000 changesets here, which is
most of what derivation now spends; past the budget such a merge stores a
superset from the branch's own levels instead.

## Path history on Linux

**The problem.** The whole Linux history (1,484,762 commits) imported into a
Postgres 16 store, blobs in one filesystem pack, on the VPS of
`docs/DEPLOY.md`: the first `last-change` started deriving the path-history
index and managed about 390 changesets a minute (19,504 in 50 minutes, the
2005-2006 commits), so about 63 hours for the whole history, at 99% of one
core and 7.25 GiB resident (`--cache-bytes 512 MiB`). All of git/git had
derived at about 1,875 a minute.

**Reproduced** on the first 21,109 commits (`v2.6.16`: 1,522 merges, one of
them with twelve parents), imported with `leep-git import`, local Postgres 16
over TCP, release builds, 4 vCPU Xeon, timed with
`cargo run --release -p leep-server --example history_profile -- <meta> <blobs> main --reset`,
which derives the index for a head and counts store calls. Profiled with
`perf` and with per-changeset timers (not kept). Causes, largest first:

- **Ancestry checks at merges.** A path changed on both sides of a merge took
  `maximal`: an `is_ancestor` per pair of ids, each a generation-ordered walk
  from one id down to the other's generation, repeated for every such path.
  The walks were 60-70% of CPU samples (`AncestryCache::get`,
  `newest_common_ancestor`, hashing ids). Single merges with 3,000-3,600 such
  paths took 7-20 s, almost all of it there; the twelve-parent merge, which
  takes `maximal` for all of its 12,063 paths, took minutes.
- **Paths visited at merges.** The extra paths were found by diffing the
  first parent's history tree against the second's, which includes every path
  mainline changed while the branch was out. Merges visited 600-6,900 paths
  each, nearly all to conclude that nothing changed.
- **Cost per visited path**, about 0.4-0.7 ms: every directory on a changed
  spine was joined from its chunks (each chunk's keyframe cloned, the patch
  applied) up to five times (cursor, rebuild, write, each parent and the
  merge base), and cost a write round trip, a read round trip per chunk not in
  the cache, and two root-tree reads per changeset.
- **Memory.** Finding the missing range kept every missing changeset decoded,
  with all its changes, until the last was derived. Changesets average 1.7 KB
  encoded on this slice and about 5 KB decoded, which is the 7.25 GiB measured
  on the 1.48 million of Linux.

**Fixes** (rows unchanged: the 265,273 rows the old code wrote for the first
16,800 changesets, 1,262 merges among them, are all among the new
derivation's 311,439, byte for byte):

- At a two-parent merge, only paths whose history at the second parent
  differs from the merge base's are visited: where they are equal, the first
  parent's entry is the answer, and an equal directory entry means an equal
  subtree.
- `maximal` at a merge answers from the history at the merge base: an id of
  one parent's set that is in the merge base's set lies behind the other
  parent and is dropped; one that is not can only lie behind the other
  parent's set at a criss-cross merge, which one walk per parent (shared by
  every path of the merge) rules out before any exact check. Octopus merges do
  the same per pair of parents. Tests check every answer against the old
  `maximal`.
- The range is found, read and written 256 changesets per round trip, keeping
  only parents (about 170 bytes per changeset, so about 250 MB for all of
  Linux) and dropping each value after its last child used it. Root trees,
  generations and parents of a window are read in one batch each, and a
  window's node rows and values are written in one `put_derived_many`, before
  each merge so it can read its merge base's value.
- A directory is joined once per changeset; writing it compares chunks with
  the parent's without joining them, a join reads all chunk rows in one batch,
  the other parents' and merge bases' trees are looked up chunk by chunk, the
  merge diff skips chunks the two trees share, and tree nodes are kept decoded
  in a 32 MiB cache (each changeset's old tree is the previous one's new tree).

| Deriving the index for the 21,109 commits | Loose blobs | Packed blobs (one pack) |
|---|---|---|
| Before, stopped after 40 minutes | about 16,800 changesets, 7.0 per second, peak RSS 256 MiB | about 17,000 changesets, 7.1 per second, peak RSS 349 MiB |
| After (two runs) | 95-103 s, **205-221 per second**, peak RSS 238-239 MiB | 94-97 s, **217-224 per second**, peak RSS 344-348 MiB |
| After, the first 16,800 | 81-83 s (about 29x) | about 82 s (about 29x) |

The old code never reached the twelve-parent merge (changeset 19,532). Peak
memory is alike on the slice because its 21,109 changesets decode to about
100 MB; it is the 1.48 million of Linux that no longer have to fit.

Store calls after, loose, the second run (packed, blob reads took 10.8 s for
the same 126,609):

| Call | Calls | Items | Time |
|---|---|---|---|
| blob `get` (tree nodes) | 126,609 | | 11.1 s |
| `get_derived` history nodes (cache misses) | 37,558 | | 8.9 s |
| `get_derived` skiplists (`merge_base`) | 35,234 | | 9.3 s |
| `put_derived_many` | 1,678 | 371,132 | 11.9 s |
| `get_changesets` | 557 | 42,218 | 1.7 s |
| the rest (merge bases' values, last merges, batched reads) | 6,422 | | 2.5 s |

Packing costs the walk little: it reads trees oldest first, and the store's
decoded-object cache resumes each delta chain from the previous version, so a
tree read averaged 83-85 us packed against 77-88 us loose. The cold full listing is
another matter (9.7 s on Linux), not addressed here.

What remained after that: the twelve-parent merge still took about 1 s and
the largest two-parent merges 0.5-0.9 s; skiplist reads in `merge_base`, one
per round trip; and a changeset still joined every chunk of each directory it
rewrote, so the cost grew with directory width.

**Wide directories.** Later Linux has 70,000-90,000 files, directories of
thousands of names (`drivers/*`, `arch/*`) and many more merges, and the full
history derived at only about 40 changesets a second there, CPU-bound in the
server. Reproduced with a synthetic history of that shape: `history_profile
--synth N` (generator in `crates/leep-derive/benches/support/wide_history.rs`)
imports 83,000 files, 12 directories of 2,000-5,000 files and 800 directories
of 52 in 42 changesets, then grows them with 8 subsystem branches that take
topic merges, merge mainline back into themselves and are merged into
mainline, so most merges carry hundreds of paths from their second parent.
6,000 changesets of it (5,860 on the head's history) derived at 80 a second
on in-memory stores and 52-55 on Postgres, against about 200 for the
narrower v2.6.16 slice. Per-phase timers and `perf` (not kept), in-memory
stores, 3,900 changesets:

| Phase | Before | After |
|---|---|---|
| Rebuilding directories (join every chunk, re-cut, compare) | 19.9 s | 3.6 s |
| First parent's entries (joins every directory on a spine) | 14.7 s | 0.6 s |
| Old and new tree lookups (decode wide tree nodes, drop them on eviction) | 13.7 s | 0.5 s |
| Merge diffs (join both sides' unshared chunks) | 5.1 s | 1.5 s |
| All of the derivation | 55 s | 8.4 s |

On Postgres the store added single `last_merge-v1` writes (one per changeset:
4.9 s on the synthetic history, 56 s on the slice), skiplist reads in `merge_base` (12,184, 4.4-5.6 s) and,
on the Linux slice, 37,558 single history-row reads (16-21 s) that missed the
64 MiB row cache.

**Fixes** (rows unchanged: every `history-tree-v3` and `history-node-v3` row of
the old and new derivations compared equal, by key and value hash, on the
synthetic history in memory and on Postgres, 137,961 rows, and on the
v2.6.16 slice, 311,439 rows):

- A directory is edited chunk by chunk (`Repo::stage_history_edit`). Chunks
  are cut after boundary names wherever they fall, so the runs of chunks
  between boundary names that hold no changed name are the base's chunks;
  only the runs that do are read and cut again, each chunk against the same
  predecessor a full rebuild picks. Tests check every edit against a full
  rebuild. The first parent's directories are looked up chunk-locally like
  the other parents'.
- Tree nodes are read as views (`TreeView`): the encoded bytes and an offset
  per entry, built in one pass without allocating, looked up by binary
  search. The directories a changeset reads are loaded a level of the tree at
  a time with the reads of a level in flight together, and the history rows
  of the other parents and the merge base a batch per level.
- Merge diffs compare split nodes a run of chunks at a time; a run of one
  chunk a side whose rows are patches on the same keyframe is compared only
  at the names the patches set. Merge diffs read a level's rows in one batch.
- Staged values (`history-tree-v3`, and now `last_merge-v1`) are found by
  later derivations in the same run, so a window's rows go in one write
  instead of a write before every merge; `last_merge-v1` is staged, and
  derived for a window from the changesets in hand before its merges ask.
- Skiplist rows read by ancestry queries are kept for the duration of one
  path-history derivation (not across derivations, so a repaired row is read
  again).
- Smaller costs: rows are encoded and hashed only when chosen, chunk lists
  are resolved by merging instead of through a map, and hash-keyed caches
  hash with a cheap mix instead of SipHash.

| Deriving the index, release builds, local Postgres 16 over TCP, blobs on tmpfs, two runs each | Before | After |
|---|---|---|
| Synthetic late-Linux shape, in memory (5,860 changesets) | 70-74 s, 80-83 per second | 14-16 s, **371-406 per second** (4.6-4.9x) |
| Synthetic late-Linux shape, Postgres | 106-114 s, 52-55 per second | 28-29 s, **201-211 per second** (3.8-3.9x) |
| v2.6.16 slice, Postgres, `last_merge-v1` derived too | 197-262 s, 81-107 per second | 89-90 s, **235-237 per second** |
| v2.6.16 slice, Postgres, `last_merge-v1` present | 153 s, 138 per second | 89 s, **236 per second** |

Peak RSS was 206-242 MiB before and after on Postgres. The machine was shared
(load 1-6), which moved the old code's runs most. Store calls after, on the
v2.6.16 slice (first run):

| Call | Calls | Items | Time |
|---|---|---|---|
| blob `get` (tree nodes, 16 in flight) | 112,731 | | 12.4 s |
| `put_derived_many` | 520 | 392,241 | 12.4 s |
| `get_derived` skiplists (`merge_base`) | 11,267 | | 4.8 s |
| `get_derived_many` history nodes | 3,202 | 44,049 | 2.2 s |
| `get_changesets` | 814 | 42,730 | 2.8 s |
| `get_derived` history nodes (cache misses) | 2,480 | | 1.2 s |
| the rest | 3,612 | | 2.3 s |

`cargo bench -p leep-derive -- derive_history` has a wide case
(`wide_merges_300_changesets`, directories of 1,000-2,000 files): 438 ms
before, 141 ms after; `1000_changesets` 56 ms before, 46 ms after; path `log`
and `last_change` queries unchanged.

What remains: CPU in the chunk edits (copying the chunks of a run and the
chunk list, about 150 refs for a 5,000-name directory, once per changed
directory), resolving list patches each time a cursor opens a directory
(not cached), skiplist reads in `merge_base` on the first meeting of each
chain (one per round trip), and the writes themselves. The row cache stays
at 64 MiB: on the slice, 256 MiB made misses vanish and derived at 241 a
second, but the whole slice's rows then fit, which says nothing about full
Linux; batching took most of the same gain without the memory. Not measured
here: the packed blob store and the full Linux history.

**Stale planner statistics.** On the full Linux database the first run of
this code derived only about 500 changesets a minute, with Postgres at 100%
CPU and the server idle. The table was last analyzed before the path-history
rows existed, so Postgres estimated a row or two for `history-node-v3`, and
answered each batched `get_derived_many` by walking all 512,000 rows of the
kind and filtering on `key`: 2.3 s per lookup. `ANALYZE derived` brought it
to 3 ms and the derivation to about 10,300 changesets a minute. The query now
takes the kind through a sub-select, which keeps the planner off the per-kind
estimate: on a local reproduction (300,000 rows of a new kind added after
`ANALYZE`) a 200-key lookup takes 1-3 ms with fresh or stale statistics,
against 191 ms before.

**Planner statistics after bulk writes** (ADR 0042). A local reproduction on
Postgres 16: `derived` with 600,000 rows of two kinds, `ANALYZE`, then 300,000
rows of a new kind with autovacuum off; `changesets` analyzed at 1,000 rows,
then grown to 300,000. Every `postgres.rs` statement was run with
`EXPLAIN ANALYZE` under custom and generic plans, stale and after `ANALYZE`.
Two misbehave on stale statistics, both with the kind estimated at one row:
the batched key lookup `kind = $1 AND key = ANY($2)` (200 keys: 530 ms
stale, 3.3 ms after `ANALYZE`), and the batch of `delete_derived_kind`, a
nested loop comparing every row of the kind with the batch (17.0 s for a
100-row batch, linear in the batch, so about half an hour for the 10,000-row
batch it uses; with fresh statistics 84 ms for 100 rows, from a heap scan
that starts at the front of the table each time). Deleting by `ctid` from
one ordered primary-key walk takes 30-50 ms per 10,000-row batch with stale
or fresh statistics and either plan. Lookups on the single-column primary
keys and range scans of `(kind, key)` keep their plans either way (0.03-4 ms).
The only `changesets` sequential scans in an import, index warming, GC,
`verify`, `backup` and `restore` of a 2,700-changeset history were batched
reads (`id = ANY` of 256 or 1,000 ids) and backup pages on that small table,
which the planner rightly scans; a 256-id batch switches to the index from
about 5,000 changesets. Statistics now follow bulk writes: migration 2
re-analyzes `derived` after 1% of it changes and `changesets` after 2%, and
`leep-git import` and `restore` run `ANALYZE` when done.

**Merges with ancestry walks per path.** On the full Linux history (Postgres
16, packed store, the code above) derivation ran at 4,400-5,500 changesets a
minute but stopped for about 30 minutes of CPU at single merges, such as
`7214dd4ea9` ("Merge branch 'work.thaw'", 2018-04: two commits on
`v4.14-rc3`, five months and 44,583 changesets behind the first parent, 35,917
files apart). `perf` showed the ancestry walks: `AncestryCache::get` from
`Reader::generations`, `Arc` clones, SipHash of ids and
`newest_common_ancestor`.

Reproduced with `cargo run --release -p leep-derive --example dag_replay`,
which replays a real commit graph (`git rev-list --parents`, fetched with
`--filter=tree:0`) with synthetic contents (1-2 of 9,216 files per commit;
merges take the other side's changes since the merge base) on in-memory
stores. The whole graph up to `7214dd4ea9` (751,193 commits) did not fit in
15 GB, so the run keeps the 119,255 commits above generation 123,457 (10,000
below the merge base) on a 3,335-changeset chain standing in for older
history. Per-merge counters (not kept) showed:

- **An exact `is_ancestor` per path at criss-cross merges.** When an id of
  one parent's set was not in the merge base's set but the other parent's
  walk reached it, `maximal` checked it against each id of the other set
  with `is_ancestor`, a walk from that id down to the first, per path. The
  slowest merge made 38,230 such calls, 394 million walk steps, and took
  582 s; 22 merges over 1 s took 897 of the run's 1,232 s.
- **A walk as deep as the oldest id asked about.** An id older than the
  merge base and not behind it (a change merged in from an old branch, kept
  newest because the merge changed nothing there, as when the same patch
  went in twice) sent the other parent's walk down to that id's generation:
  for a branch on a five-month-old base, through every changeset behind the
  base above it, which on Linux can be most of history and more than the
  ancestry cache holds. In the replay the `work.thaw` merge itself had no
  such id and took 72 ms.

Merge-base search was not the problem: one walk per merge, 38,651 changesets
for `work.thaw` (about 1 s on a cold cache, 30-60 ms warm).

**Fix** (rows unchanged): an id `x` of parent `i`'s set lies behind another
parent `j` exactly when it is an ancestor of `j`, since `j`'s set covers every
change behind `j`; the per-id `is_ancestor` checks were redundant and are
gone. Before the walk, two exact shortcuts with `m` the merge base of the two
parents: `x` not in `m`'s set is not behind `m`, so it can only be a common
ancestor outside `m`'s ancestors, which ranks below `m` by (generation, id);
an `x` ranking above `m` is answered without a walk. And if one parent's
ancestors outside `m`'s that rank below `m` are all on `m`'s first-parent
chain (`Repo::side_is_clear`, walking at most 10,000 changesets of that side,
the closer one first), there is no such common ancestor at all: a two-commit
branch on `m` settles every question in three steps, however old `m` is. The
walks also hash ids with the cheap key hash and read single fields of the
ancestry cache instead of cloning entries.

| Dag replay above, in memory, one run each | Before | After |
|---|---|---|
| Deriving 122,590 changesets | 1,232 s, 99 per second | 290 s, **422 per second** |
| Slowest merge | 582 s | 1.1 s (that one: 0.22 s) |
| Merges over 1 s | 22, 897 s in all | 2 (1.1 s and 1.0 s) |
| The `work.thaw` merge alone | 72 ms | 65 ms |

All 1,157,442 rows of both kinds were identical. The synthetic late-Linux
history with such a merge at its end (`history_profile --synth 20000
--old-base 15000 --last`) and the v2.6.16 slice were unchanged in speed,
having no such merges (memory 186 and 187 per second; Postgres 127 and 126,
the final merge 64 ms and 42 ms; slice on Postgres 246 per second both),
with identical rows (560,483 and 311,439). Criterion: the new
`derive_history_index/old_base_merge_3000_changesets` 8.8 ms before, 7.6 ms
after; other `derive_history`, `merge_base`, `log_path` and `last_change`
cases unchanged within noise (`wide_merges_300_changesets` 131 ms and
137 ms, `1000_changesets` 39 ms and 34 ms).

### Index size: packs (ADR 0045)

**The problem.** With the index complete on all of Linux, `history-node-v3`
held 25,426,370 rows and 24 GB of values (about 17 rows and 16 KB per
changeset) in a 27 GB database, against 3.8 GB for git's repository.

**What the bytes were.** `history_profile --breakdown-v3` attributes every
row to the changeset whose derivation wrote it. On the v2.6.16 slice (local
Postgres 16) and the synthetic late-Linux history (`--synth 20000 --old-base
15000`, in memory):

| | v2.6.16 slice | Synthetic |
|---|---|---|
| Node rows, value bytes | 290,330, 209.8 MB | 540,706, 300.8 MB |
| Per changeset | 13.8 rows, 9.9 KB | 27.3 rows, 15.2 KB |
| Merges | 1,522: 121 rows, 115 KB each; 83.5% of bytes | 1,930: 187 rows, 125 KB each; 80.4% |
| Other changesets | 5.4 rows, 1.8 KB | 10.1 rows, 3.3 KB |
| Bytes per changeset, p50 / p99 / max | 1.3 KB / 188 KB / 1.96 MB | 2.9 KB / 474 KB / 1.35 MB |
| Ids in `changed` / names / child addresses | 72.0% / 12.5% / 7.6% | 61.9% / 9.5% / 7.3% |
| Chunk-list addresses and names / patch bases / tags and lengths | 2.2% / 2.9% / 2.8% | 13.8% / 4.6% / 2.9% |
| Ids a merge's rows hold, distinct ids among them | 2,748, 47 | 2,570, 63 |
| Directories a changeset writes: merges / others | 124 / 4.5 | 115 / 7.5 |
| Keyframes (whole nodes, chunks, lists) | 77.7% of bytes | 61.0% |
| Rows of directories over 500 entries | 3.3% of rows, 4.8% of bytes | 29.6%, 35.2% |

The synthetic history's 15.2 KB per changeset is Linux's 16 KB; the slice
is narrower and lighter on merges. Per-row estimates from the same rows: an
id table per row 1.86x (slice) and 1.39x (synthetic), with front-coded names
1.91x and 1.44x, zstd -3 per row 1.98x and 1.47x.

**The change.** One pack per changeset of every node its derivation writes,
cut into parts of 16 KB (part 0 is the changeset's value), with ids
interned per part, nodes addressed by (writer, part, index), positional
patches, front-coded names and zstd. Variants measured on the way are in
ADR 0045.

| | v3 | v4 |
|---|---|---|
| Slice: value bytes, rows | 210.5 MB, 311,439 | **34.6 MB, 22,774 (6.1x, 13.7x)** |
| Slice: history in Postgres after `VACUUM FULL` (heap, TOAST, index) | 200.1 MB | **42.6 MB (4.7x)** |
| Slice: database after `VACUUM FULL` | 292 MB | 135 MB |
| Synthetic: value bytes, rows | 301.4 MB, 560,483 | **51.8 MB, 22,004 (5.8x, 25x)** |
| Synthetic: bytes per changeset | 15.2 KB | 2.6 KB |
| Slice: merge / other changeset | 115 KB / 1.8 KB | 13.1 KB / 0.75 KB |
| Synthetic: merge / other changeset | 125 KB / 3.3 KB | 15.3 KB / 1.2 KB |

Extrapolated to Linux (16 KB per changeset, 24 GB): 2.8 KB per changeset
and about 4 GB at the synthetic ratio, 2.6 GB at the slice's.

Derivation (release builds, rows written in one run of `history_profile
--reset`; alternating runs):

| | v3 | v4 |
|---|---|---|
| Slice, Postgres (6 runs each) | 95.5 to 120.5 s | 85.4 to 112.9 s (final build: 85.4 s against 102.0 s for v3 run beside it) |
| Synthetic, Postgres (4 runs / 2 runs of the final build) | 164.5 to 184.0 s | 167.7 to 174.0 s |
| Synthetic, in memory | 121.0 s | 162.3 s |
| Rows written (slice) | 371,132 in 437 calls, 10.8 to 12.8 s | 22,774 in 83 calls, 1.3 s |
| Peak RSS: synthetic on Postgres / slice | 239 to 241 / 209 to 217 MiB | 253 to 254 / 236 MiB |

v4 writes a sixteenth of the rows, and spends the time on encoding (each
node is costed for the patch rule and encoded again into its part) and on
cache misses, which decode a node from its part (up to 16 KB uncompressed)
and name a patch from its keyframe. In memory, where writes cost nothing,
that is 34% more time on the synthetic history; with a 512 MiB row cache
the difference was 5% (25.4 s against 24.1 s on 8,000 changesets). The row
cache now counts nodes at about their decoded size, under 96 MiB, beside
32 MiB of parts, 16 MiB of patches waiting for their keyframe and 4 MiB of
roots; v3 counted rows at twice their encoded size under 64 MiB, which
undercounted decoded rows by about 1.7x. With v4's row cache at 64 MiB the
synthetic history took 197 to 221 s on Postgres, missing three times as
often.

Queries on the slice (312 paths: 12 well-known files, 220 random files, 50
directories, 30 deleted files; 50-entry `log path=`, `last-change`, and 30
blames), each run against a freshly started server and then again warm, v3
and v4 servers alternating on one database, totals per run in seconds:

| | v3 (6 runs) | v4 (final build, 2 runs) |
|---|---|---|
| `last-change`, cold | 5.70 to 6.61 | 5.88 |
| `last-change`, warm | 5.56 to 6.26 | 6.79 to 6.91 |
| `log path=`, cold | 42.2 to 48.6 | 37.8 to 39.3 |
| `log path=`, warm | 35.3 to 41.3 | 38.2 to 40.2 |
| `blame`, cold / warm | 0.97 to 6.58 / 0.88 to 1.18 | 0.99 to 1.02 / 1.08 to 1.12 |
| `last-change` alone, cold / warm (2 runs each) | 5.64 to 5.86 / 5.77 to 6.21 | 5.58 to 5.59 / 5.08 to 5.28 |

The warm pass interleaves each path's `last-change` with the previous
path's 50-entry log, whose walk turns the caches over; asked alone,
`last-change` is as fast as v3's or faster.

All 654 response bodies were identical between the two servers, in every
one of 34 runs. A first v4
build, which decoded whole parts and read each changeset's value again for
every lookup at it, was 50 to 80% slower on `log`; decoding only the node
asked for (parts carry node lengths) and caching each changeset's root
closed that.

Criterion (`cargo bench -p leep-derive -- 'derive_history|log_path|last_change'`,
in-memory stores, so encoding and decoding without the writes they save):

| Case | v3 | v4 |
|---|---|---|
| `log_path_filter_10k_history/index/limit_10` | 23.8 us | 17.3 us |
| `log_path_filter_10k_history/index/limit_1000` | 7.48 ms | 8.47 ms |
| `log_path_filter_10k_history/index/rare_path` | 26.6 us | 18.9 us |
| `last_change_10k_history/d0/f` | 1.66 us | 1.39 us |
| `derive_history_index/1000_changesets` | 47.8 ms | 53.0 ms |
| `derive_history_index/wide_merges_300_changesets` | 146 ms | 157 ms |
| `derive_history_index/old_base_merge_3000_changesets` | 8.57 ms | 8.73 ms |

## Parallel derivation

ADR 0050. Derivation of a missing range runs changesets whose parents are
done side by side, `jobs` at a time, and stores rows in an order that keeps
each after its parents. Measured on the synthetic late-Linux history
(`history_profile --synth 20000 --old-base 15000`: 19,777 changesets on
`main`, 20,045 stored) in a local Postgres 16 (TCP on 127.0.0.1) with
filesystem blobs (loose objects), release builds, on a 4-vCPU, 16 GB VM that
another workload shared throughout (load average 1.2 to 5 before each run,
mostly 2 to 3, so about two cores were free). "Serial" is the build before
the change (`d9150a4`). Every run's rows were dumped (`--dump`, or
`history_profile ... --dump` after a `rederive`) and compared with the serial
build's: all identical.

**Deriving** (`history_profile <meta> <blobs> main --reset --jobs N`, two
alternating rounds; `history-tree-v4`, 19,777 changesets):

| | Wall, round 1 / 2 | Changesets/s | Peak RSS |
|---|---|---|---|
| Serial | 160.3 / 127.9 s | 123 / 155 | 246 / 252 MiB |
| 1 job | 123.0 / 114.3 s | 161 / 173 | 361 / 366 MiB |
| 2 jobs | 70.5 / 67.9 s | 280 / 292 | 312 / 325 MiB |
| 4 jobs | 59.0 / 65.6 s | 335 / 302 | 331 / 329 MiB |
| 8 jobs | 63.1 / 61.3 s | 313 / 323 | 322 / 324 MiB |

The final build, once more on a quieter machine (load average 1.4): one job
127.3 s, four jobs 52.3 s (378 a second, 2.4 times one job), rows again
identical.

The work is CPU-bound in the process: with one job it kept 0.92 cores busy
(99.8 s user and 17.2 s system over 127.5 s), with four 1.98 cores (119.9 s
and 17.9 s over 69.5 s), which was what the other workload left. Past two
jobs the gain stopped where the idle cores ran out, and CPU time per
changeset rose by 18% at four jobs. One job beats the serial build by the
read-ahead (changesets and their prepared inputs a window ahead, writes in
flight while the next window derives).

Peak memory is about 100 MB above the serial build's at any number of jobs,
and most of it is the allocator: with `MALLOC_ARENA_MAX=2` the peaks were
276 MiB (1 job) and 270 MiB (4 jobs), but four jobs then took 100 s instead
of 60 to 65, contending for the allocator. An allocator built for threads
(jemalloc, mimalloc) is the next thing to try for both memory and the 18%.

`log-filter-v1` on the same history (`--log-filter`), which spends its time
on round trips (5 s of user time in 12.8 s with one job):

| | 1 job | 2 jobs | 4 jobs | 8 jobs |
|---|---|---|---|---|
| Wall | 12.8 s | 8.0 s | 6.4 s | 6.5 s |
| Peak RSS | 81 MiB | 82 MiB | 89 MiB | 93 MiB |

**`repair rederive`** (`leep-server repair rederive --kind history-tree-v4`,
every stored changeset, which also recomputes `generation`, `skiplist` and
`root_tree`; one run each):

| | Rows stored (recompute and compare) | From scratch (after `repair drop`) | `--missing`, from scratch |
|---|---|---|---|
| Serial | 380.2 s (53/s), 249 MiB | 411.4 s (49/s), 247 MiB | (no such flag) |
| 1 job | 290.3 s, 349 MiB | | 131.9 s, 359 MiB |
| 2 jobs | | | 81.2 s, 319 MiB |
| 4 jobs | 131.0 s (153/s), 317 MiB | 144.1 s (139/s), 312 MiB | 67.9 / 70.8 s (295/s), 333 MiB |

The serial rederive's 49 to 53 changesets a second is the 45 a second seen
on all of Linux. With four jobs the full pass is 2.9 times faster. A build
of a missing index is faster again with `--missing`, which derives through
`derive` (the warmer's path, which batches its reads and stages its writes)
instead of recomputing, reading back and comparing each row: six times the
serial rederive. For `log-filter-v1` from scratch: serial rederive 121.6 s,
one job 60.5 s, four jobs 23.1 s, four jobs with `--missing` 9.9 s, all
with the serial build's rows.

**Kill and resume.** `rederive --missing --jobs 4` killed with SIGKILL after
30 s had stored 10,007 of the 20,045 rows; run again, it derived the other
10,038 in 60.6 s. A full `rederive --jobs 4` killed after 70 s had stored
8,678; the next run found those right and wrote the other 11,367. Both ended
with every `history-tree-v4` and `history-node-v4` row (22,291) identical to
the serial build's. In memory, `fault_tests::parallel_derivation_under_faults_stores_rows_after_their_parents`
injects store failures into four-job derivations of both kinds and checks
after every failed attempt that each stored row has its parents' rows and
the fault-free value.

**On a large machine.** The gain follows the free cores: on Linux, where
the serial `rederive` ran at about 45 a second (nine hours), `rederive
--missing --jobs N` derives at the warmer's rate times about the idle
cores, with memory near one job's. Not yet measured on Linux.

## Garbage collection

`leep-server gc` phases timed with
`cargo run --release -p leep-server --example gc_profile -- <meta> <blobs> [condemn|collect|both] [--full] [--commits N]`,
which wraps both stores to count calls and runs with a zero fence (scratch
stores only). Local Postgres 16 over a Unix socket, filesystem blob store,
4 vCPU Intel Xeon @ 2.10GHz, release build. "Before" is the build of main
before ADR 0033; "incremental" is a run after a previous run, with 10 new
changesets stored first (`--commits 10`, each changing one file three
levels deep).

First 3,795 commits of git/git (11,735 objects):

| | condemn | collect | store calls (condemn + collect) |
|---|---|---|---|
| Before | 2.5 s | 4.5 s | 12,552 + 24,287 |
| First run after the change (walks everything, writes the record) | 1.46 s | 0.54 s | 4,990 + 34 |
| Incremental, nothing new, loose | 0.69 s | 0.61 s | 34 + 34 |
| Incremental, 10 new commits, loose | 0.95 s | 0.55 s | 104 + 34 |
| Incremental, 10 new commits, packed | 0.17 s | 0.10 s | 34 + 34 |
| `--full`, loose | 1.28 s | 1.36 s | 5,016 + 5,028 |

First 20,967 commits of git/git (70,323 objects; imported with `leep-git
import`):

| | condemn | collect | store calls (condemn + collect) |
|---|---|---|---|
| Before, loose | 18.2 s | 29.7 s | 75,085 + 145,408 |
| Before, packed | 24.5 s | 34.6 s | 75,135 + 145,498 |
| First run after the change, loose | 10.8 s | 3.3 s | 33,306 + 186 |
| Incremental, 10 new commits, loose | 3.4 s | 3.3 s | 274 + 186 |
| Incremental, 10 new commits, packed | **0.95 s** | **0.71 s** | 268 + 186 |
| `--full`, loose | 9.4 s | 9.7 s | 33,315 + 33,386 |
| `--full`, packed | 14.9 s | 16.0 s | 33,345 + 33,416 |

Where the time went before (20,967 commits, loose, condemn): 4.7 s in one
`get_changeset` per changeset, 4.4 s in one root-tree `get_derived` per
changeset, 2.3 s reading 33,057 tree nodes, 2.8 s listing blobs, the rest
decoding and hashing tree nodes. Collect repeated the mark and added one
`get_derived` per blob for its condemnation row (91,290 calls, 16 s). Packed,
tree node reads cost more (delta chains), so the full mark is slower.

After: an incremental run makes one call per 1,000 changesets listed, one
batched row read per 1,000 changesets and per 1,000 blobs, and two reads per
new tree node on a changed path (the node and its entries' rows). What is
left grows with the store, not with history walked: the blob listing (about
40 us per loose object, mostly a `stat`, or 7 us per packed object) and the
batched row reads (about 5 us per object). On the 82,500-commit git/git
(326,731 objects) that projects to a few seconds per phase packed, against
about 4 minutes per GC before. The first run after the change still walks
everything, faster than before because changesets and root trees are read in
batches of 1,000 (ADR 0030), and writes one `gc-mark-v1` row per referenced
object and changeset (91,290 rows, about 1 s).

## Changeset compression

ADR 0053 stores changeset rows as zstd frames compressed with a dictionary
trained on the store's own history. Measured on a local Postgres 16
(`shared_buffers` 512 MB, 4 cores shared with other work) on two
histories:

- **git/git v1.7.0**: 21,205 commits imported with `leep-git import`.
- **Linux, last 117,680 commits** (`--shallow-since=2025-06-01`, blobless
  clone): rows built by the `changeset_compression` example the way
  `leep-git` imports each commit (same fields and `extra` keys; blob hashes
  and sizes are stand-ins, which compress no better or worse than real
  ones), loaded into a Leep schema with `COPY`. These rows are larger than
  Linux's average (2,279 bytes against about 1.8 KB over all of history):
  recent commits have longer messages, more trailers and `mergetag`
  signatures.

**What a row holds**, from the example's breakdown (bytes per row): git/git
763 (parents 37, blob hashes 87, paths 50, message 374, author 35, `extra`
146); Linux 2,279 (parents 35, blob hashes 471, paths 518 for 15.1 changes,
message 856, author 38, `extra` 263). Ids and hashes, 16% and 22%, do not
compress.

**Encodings** (`cargo run --release -p leep-server --example
changeset_compression -- --hex rows.hex` or `--git DIR`), total size against
plain rows:

| Encoding | git/git | Linux |
|---|---|---|
| zstd -3 per row | 1.36x | 1.58x |
| zstd -19 per row | 1.38x | 1.62x |
| zstd -3, 32 KB segments of consecutive rows | 2.01x | 2.04x |
| zstd -19, 32 KB segments | 2.12x | 2.15x |
| zstd -3, 16 / 32 / 64 / 110 KB dictionary from the rows | 1.87 / 1.91 / 1.98 / 2.01x | |
| zstd -9, 32 / 64 / 110 KB dictionary from the rows | | 1.93 / 1.97 / 2.00x |
| zstd -1 / -9 / -19, 64 KB dictionary | 1.81 / 2.08 / 2.13x | 1.78 / 1.97 / 2.00x |
| newer half, 64 KB dictionary from the older half | 1.88x (-3) | 1.93x (-9) |
| 64 KB dictionary trained on the other history | | 1.81x |

A dictionary trained on the store's own rows gets within 2-8% of segments,
row by row, and loses 2-6% on history newer than it; one built into the
binary (trained on git/git) would be 8% worse on Linux. Training 64 KB on
50,000 Linux rows takes about 14 s.

**Codec cost** per git/git row (release build, one thread, 64 KB
dictionary): writing 6 µs at level 3 (1.98x), 22 µs at 6 (2.07x), 39 µs at
9 (2.10x, the level used); reading about 2 µs at any level, beside 3-4 µs to
decode postcard and hash the id, which every read did before.

**Tables**, before and after `leep-server compact-changesets` and `VACUUM
FULL changesets`:

| | git/git, before | after | Linux slice, before | after |
|---|---|---|---|---|
| `data`, as written | 16.19 MB (763 B a row) | 7.68 MB (362 B) | 268.2 MB (2,279 B) | 135.3 MB (1,150 B) |
| `data`, as stored (`pg_column_size`; pglz on rows over 2 KB) | 16.03 MB | 7.76 MB | 228.5 MB | 135.8 MB |
| table with TOAST and index (`pg_total_relation_size`) | 19.9 MB (938 B a row) | **10.9 MB** (512 B) | 261.8 MB (2,225 B) | **159.1 MB** (1,352 B) |
| `compact-changesets` | | 4.3 s | | 31.8 s, 275 MB peak |

The dictionary row is 64 KB. Postgres already compressed the 14% of Linux
rows over 2 KB with pglz (1.17x on all rows); zstd with the dictionary takes
what is stored from 228.5 to 135.8 MB, and rows no longer reach TOAST as
often.

**All of Linux, projected.** The table is 2.9 GB for 1,484,762 rows; the
slice's table shrank to 61% and git/git's to 55%, so about **1.6 to 1.75 GB,
saving 1.15 to 1.3 GB** (8.3 GB of Postgres before path history becomes about
7.1 GB). `compact-changesets` took about 190 µs a row on the slice with
reading and writing, so about 5 minutes, plus `VACUUM FULL`.

**Speed** on the git/git store, compressed against a copy of the same
database with plain rows (`CREATE DATABASE ... TEMPLATE`, rows reloaded), the
same release binary, alternating runs:

| | Plain | Compressed |
|---|---|---|
| Path-history derivation, `history_profile --reset`, 4 jobs (3 runs) | 22.9 / 24.1 / 21.5 s | 25.1 / 19.4 / 21.2 s |
| ... of which `get_changesets` (1,875 calls, 42,410 rows) | 1.41 / 1.66 / 1.17 s | 1.23 / 1.45 / 1.43 s |
| `log` limit 10,000, mean / best of 10 (fresh server every 5) | 0.42 / 0.39 s | 0.47 / 0.39 s |
| `log` limit 100, mean of 6 | 11 ms | 14 ms |
| `log path=Makefile` limit 50, mean / best of 6 | 53 / 31 ms | 44 / 19 ms |
| `log author=Linus` limit 100, mean / best of 6 | 178 / 139 ms | 229 / 156 ms |

Differences are within the run-to-run noise of this shared machine; the
codec adds about 2 µs a changeset read (20 ms per 10,000), and with the
tables in memory the smaller rows save no I/O. On a store larger than
memory, as all of Linux is on its VPS, half the bytes per changeset read
should make cold walks faster.

## Dropping a derived kind

`repair drop --kind history-tree-v3` (which also drops `history-node-v3`)
before and after ADR 0052. On the Linux VPS (Postgres 16, 2 vCPUs, 26.9M rows,
24 GB of values in a 33 GB `derived` table) the old dry run took 38 minutes
and the delete 6.7 to 8.5 s per 10,000-row batch with autovacuum running,
about 5.5 hours.

Reproduction: local Postgres 16, 4 vCPU Intel Xeon @ 2.10GHz, debug build of
`leep-server`, defaults except `shared_buffers`. A synthetic `derived` table
shaped like Linux's: per changeset 17 `history-node-v3` rows of 400 to 1,500
bytes (one in 40 of 6 KB, stored out of line), a `history-tree-v3` row and
four small rows of other kinds, interleaved as derivation writes them, then
three `history-node-v4` rows and a `history-tree-v4` row per changeset
appended; `VACUUM ANALYZE`d. Keys are hashes, as in Leep. "Cold" sets
`debug_io_direct = data` and `shared_buffers = 256MB`, so reads go to disk
as on a table larger than memory; direct IO also disables readahead, which
slows sequential reads (a plain sequential scan read 40 MB/s here), so cold
numbers for the walk are pessimistic. Autovacuum ran beside every delete
(it started part way through each).

| 100,000 changesets: 1.8M rows to drop, 2.6 GB table, cold | before | after |
|---|---|---|
| `--dry-run` | 153.6 s | 2.0 s |
| delete | 6,100 rows/s (killed after 710k rows) | 34,700 rows/s (51.9 s) |
| WAL per row deleted | 6.5 KB | 1.2 KB |

| 240,000 changesets: 4.3M rows to drop, 6.3 GB table | before | after |
|---|---|---|
| `--dry-run`, cold | 414 s | 7.9 s (`count(*)` in psql) |
| delete, cold | 4,900 rows/s (killed after 1.39M rows) | 37,200 rows/s (116 s; walk run as a SQL procedure) |
| delete, warm | 43,000 rows/s (100 s) | 144,000 rows/s (30 s; SQL procedure) |
| WAL | 28.5 GB (warm; 7.7 KB per row cold) | 5.1 GB |

The old delete took each batch in key order, so its 10,000 rows sat on about
10,000 pages scattered over the table: read at random, and each changed page
logged in full again after every checkpoint (6.5 to 7.7 KB of WAL per 1 KB
row; 28.5 GB to delete 4.3M rows from a 4.9 GB heap). The walk reads each
page once in order and logs it about once: the WAL is about the heap's size.
With a stale estimate the walk statement, left to the planner, became an
index scan over the whole kind for every page range; it runs with index paths
off (a test checks the plan).

Extrapolated to Linux (26.9M rows, 6.2 times the larger reproduction): the
old dry run predicts 43 minutes (38 measured on the VPS), the new one about a
minute. The new delete reads 33 GB in order and writes about 33 GB of WAL and
33 GB of pages: 10 to 20 minutes at 150 to 300 MB/s, against 5.5 hours. The
cold reproduction's 5.7 times speedup, with its handicapped sequential reads,
bounds it at about an hour. A kill part way and a rerun finished the drop and
left every other kind's row count as it was.

SQLite (release build, warm, 24,000 changesets: 432,000 rows to drop from a
520 MB file): dry-run count 4.6 s to 61 ms; delete 20,900 to 40,800 rows a
second (rowid walk, 40 steps).

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
  above it (tracked by the `last_merge-v1` derived kind), or when one side is
  a first-parent ancestor of the other. Histories with merges in between
  take the generation-ordered walk, which stops at the first changeset both
  sides reach and reads ahead in batches; it is linear in the changesets
  above the answer, not in history (see "Merge base" under "Real history:
  git/git").

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

- Merges: deriving the index for a merge diffs the second parent's history
  tree against the merge base's, so a merge of a branch that changed many
  paths (or merged mainline into itself, which makes its merge the last change
  of everything mainline brought) derives in time proportional to those paths.
  The benchmarks use linear histories and do not measure this; "Path history
  on Linux" does.
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

- `create_changeset` validates parents with `get_changesets` (one batched
  read since ADR 0031), which transfers the full parents, including all
  their changes. An `exists`-style `MetaStore`
  method (or a generation lookup) would make the check O(1) regardless of
  parent size. This needs a trait addition, so it is an ADR-sized change.
