# Search cost on a Linux-sized tree

> **Review note (2026-10-21).** Fixes 2 and 3 were approved with changes:
> - Fix 2 (early stop): approved as written. The reviewer confirmed it is
>   sound, since `Expr` has no negation and `eval` and `Present::has` are
>   monotone. Stopping early also skips validating a row's tail, which must
>   be documented (`verify` still catches a corrupt row).
>   `leep_query_inline_derive_total` stays as it is: counting it in `local`
>   mode would page `LeepIndexerMissing` on every local deployment (see
>   doubt 2).
> - Fix 3 (filter cache): cache the *encoded* `tree-trigrams-v1` rows, not
>   decoded 64 KiB bitsets, in the existing `SizedCache` under a new share
>   of `DEFAULT_CACHE_BYTES` (about 64 MiB) rather than a separate flag.
>   Cache only rows read from the store, never a bounded query's
>   `SearchScan` rows. Add metrics only if the existing caches have them.
>   Document in OPERATIONS.md that a running server's cache can be stale
>   across `repair` until it restarts.

Status: research input, not a decision. Written 2026-10-21 against `main` at
8e33cc3. Nothing here changes `/v1` or anything persisted unless a section
says so, and those sections need an ADR (`change-persistent-format`).

## 1. Summary

- **A warm search is not 20 to 30 s of work.** Linux 6.6 (81,675 files,
  1.05 GB) with a warm index on Postgres 16 and filesystem blobs: the swarm's
  query (`handler_05_123_`, limit 20) takes **0.22 to 0.34 s** wall time
  for one search. It uses **about 0.45 s of CPU**, 0.32 s in `leep` and
  0.13 s in Postgres, on a 4-vCPU Xeon at 2.8 GHz. The worst realistic query
  measured, with common trigrams and no match, is 2.6 s wall and 4.7 s CPU.
- **The 20 to 30 s is queueing.** Throughput levels off at about 8 searches
  a second on 4 cores, which is 4 cores divided by 0.45 s. Past that,
  latency grows with the backlog: 64 concurrent searches give a p50 of
  6.7 s and a maximum of 13 s. A two-core VPS that also runs Postgres and
  the generator is overloaded by the swarm's search rate, and the 30 s
  client timeout cuts the latency off. E1b's "20 to 30 s per search" is a
  queueing latency, not a cost per search.
- **Searches also starve the connection pool.** A `get_ref` running beside
  the searches (pool of 32) has a p50 of 0.44 ms alone, 6.5 ms beside 16
  searches and **141 ms** beside 64 (p99 0.74 s). Every land needs that pool.
- **Where the 0.45 s goes.** One swarm search reads **45 MB of index rows**
  from Postgres: 36.6 MB of directory filters and 8.4 MB of blob rows. Most
  of the filters belong to subdirectories that are then pruned. Without
  Postgres (memory stores), 0.17 to 0.21 s of CPU is left. Probing those rows
  takes **115 ms (filters) + 65 ms (blob rows)** of it, almost all in
  decoding sparse delta-varint rows end to end.
- **What is *not* the cost:** blob fetches (1 candidate file), regex
  verification (under 1 ms), early stop (the query has fewer than 20
  matches, so it has to walk the whole tree), and building the index.

## 2. Method

Throwaway harness, not committed: an ignored test in `leep-server/tests`,
release build. Corpus: the `linux-6.6` tarball, plus one
`src/m05/f123.rs` file holding 8 `handler_05_123_k` functions, as the swarm
writes them. The whole tree went in one changeset, and `derive_search` ran
first (24 s in memory, 75 s on Postgres). Two store setups were measured:
memory stores, and Postgres 16 (local, `shared_buffers=1GB`, pool 32) with
loose filesystem blobs. Each search ran 3 times and the median is reported.
CPU was read from `getrusage` for `leep` and from `/proc` for every
postgres process. Phases were timed by replaying the walk one step at a
time.

Index of the head: 5,317 directory filters (109 MiB) and 81,617 blob rows
(184 MiB).

## 3. Phase breakdown

Swarm query `handler_05_123_` (13 required trigrams), limit 20, 8 matches:

| | Memory | Postgres + fs |
|---|---|---|
| Wall, one search | 125–160 ms | 220–335 ms |
| CPU per search (leep / pg) | 0.17–0.21 s / – | 0.25–0.39 s / 0.13–0.18 s |
| Directories opened / pruned | 43 / 1,144 | same |
| Files whose row was probed / pruned / scanned | 2,973 / 2,972 / 1 | same |
| `get_derived_many` calls | 77 | 77 |
| Filter rows read | 36.6 MiB (210 dense) | 138–150 ms to read |
| Blob rows read | 8.4 MiB | 73–92 ms to read |
| Probe CPU, filters / blob rows | 115 / 65 ms | same |
| Probe of the same filters if already decoded to 64 KiB bitsets | 0.7–0.9 ms | |
| Probe that stops at the first absent required value | filters 2–3 ms, blob rows 6–7 ms | |
| Blob fetch + regex | < 1 ms | < 1 ms |

The other queries follow the same pattern. `serialize_struct` (in no Linux
file, but its trigrams are common) opens 1,627 directories and probes
48,373 rows. It reads 96 MiB of filters and 128 MiB of blob rows, and
fetches and scans 1,863 files (133 MiB). On memory stores the probes take
355 ms (filters) and 889 ms (blob rows), and the scan takes 296 ms. On
Postgres, reading the rows takes 2.9 s of a 4.4 s phase sum.

Under load (Postgres, distinct swarm queries):

| Concurrent searches | Searches/s | Search p50 / max | `get_ref` p50 / p99 |
|---|---|---|---|
| 1 | 3.3 | 0.28 / 0.52 s | 0.44 / 7.4 ms |
| 4 | 7.2 | 0.55 / 1.0 s | 1.1 / 41 ms |
| 16 | 8.0 | 1.7 / 3.0 s | 6.5 / 98 ms |
| 64 | 8.4 | 6.7 / 13.4 s | 141 / 737 ms |

### Causes considered

| Suspect | Verdict |
|---|---|
| Too many candidates | No for the swarm query: 1 file. Yes for broad queries (1,863 scanned). |
| Fetching and decompressing candidate blobs | No: under 1 ms for the swarm query. |
| No early stop at `limit` | Not the cause. The early stop exists (`limit + 1`), but a query with few matches must walk the whole tree. |
| Store round trips | Partly: 77 batched calls and 45 MB moved per search, 80% of it filters. |
| Regex verification | No. |
| Holding DB connections | Yes, under load. Each search holds up to 4 open-ahead tasks plus 2 joined reads, and lands queue for the pool. |
| CPU on the async runtime | The probe loops (about 180 ms a search) run on tokio workers. E1 says isolating them does not add cores, so the effect is on fairness, not total cost. |
| Index derivation | No: everything was warm. But see doubt 2 in section 5. |

## 4. Ranked fixes

### 1. Admission for searches (ADR 0060, section 1); no format change
This is already designed. It matters most because the overload is in the
offered rate, not in the cost per search: even a search 10 times cheaper
saturates two cores at the swarm's rate. Expected gain: lands get back
most of E1's no-search gap. The pool is protected too, because at most
`cores / 2` searches run. Risk: low (a read refusal). Verify with ADR
0060's swarm rerun.

### 2. Probe that stops early; no format change
`find_sorted` decodes a sparse row until it has passed every needed
position. Because positions are spread over 2^19 bits, that is almost the
whole row. `Expr` is monotone: a trigram only ever helps a file or
directory pass. So once a needed value is passed and found absent, the
probe can evaluate `required` with the unknown values counted as present,
and stop as soon as that is false. The answer is the same as a full
evaluation. A tree row maps an absent bit to its absent trigram.
Expected gain: measured on a conjunction, probe CPU falls from 115 to 3 ms
(filters) and from 65 to 7 ms (blob rows) for the swarm query. Broad
queries fall about 3 times (355 to 144 ms, 889 to 318 ms). That removes
most of `leep`'s 0.2 s. Risk: low and contained in `trigram.rs`. Verify with
a property test that early and full evaluation agree on random rows and
expressions (OR, case folding), the existing indexed-against-brute-force
property test, and the `search` bench.

### 3. Shared cache of decoded directory filters; no format change
`tree-trigrams-v1` rows are keyed by content and never change, so a cache
never needs invalidating. Keep a byte-bounded LRU in `Repo`, keyed by tree
hash, that holds each filter decoded to its 64 KiB bitset. Then `Opener::open`
reads from Postgres only the filters it does not have. The upper levels
(root, `drivers/`, `arch/`) are shared by every query and every revision.
Expected gain: 36.6 of the 45 MB per search no longer leaves Postgres, so
most of Postgres's 0.13–0.18 s of CPU and the filter round trips go away,
and probing a filter costs under 1 ms. With fix 2, a warm swarm search
should cost about 0.1 s of CPU in total. That is an estimate: profile it
again after the change. Risk: memory. All of Linux's filters would take
340 MiB, so the default needs a budget (about 128 MiB covers the 1,190
filters one swarm query touches), a flag, and a hit/miss metric. Verify
with this harness, the cache's hit rate in the swarm, and `get_ref`
latency under load.

### 4. Bound one search's share of the pool; no format change
Add a server-wide semaphore around search's `get_derived_many` and
`read_trees` calls, for example a quarter of `--meta-pool-size`. Without
it, `cores / 2` searches on a large host can still take most of a
32-connection pool. Verify that `get_ref` p99 stays flat at 64 concurrent
searches.

### 5. A budget on index bytes read; no format change
The existing 256 MiB budget counts blob content only. A broad query reads
224 MiB of index rows without counting any of it. Charge index bytes to the
same kind of budget, and stop with the existing `truncated` and `next`
fields. An optional `stats.index_bytes` field would be additive, with
`#[serde(default)]`. Risk: such queries return fewer matches per page.

### 6. Lower priority
- Run `Opener::open`'s probe with `block_in_place` or `spawn_blocking`.
  After fixes 2 and 3 it is milliseconds, so this is for fairness only.
- Match the regex over the whole buffer instead of line by line. Only
  broad queries would gain (296 ms of scanning above).
- A blob-row cache. Rows total 184 MiB and are shared less often, so
  fix 2 is the better lever.

### Needs a format change (ADR and `change-persistent-format`)
- Store per-child summaries in the parent's row, so a pruned child's filter
  is never read.
- One row per directory holding its files' sets, which would halve the
  `get_derived_many` calls.
- Store derived rows with `STORAGE EXTERNAL`, so Postgres skips pglz on
  rows that do not compress. This is a schema migration.
Measure these only if fixes 1 to 3 are not enough.

## 5. Doubts

1. The numbers are from a 4-vCPU Xeon with the database in RAM. The VPS
   has two shared cores, packed (zstd) blobs, and a 13 GB database. It may
   pay more per search, for example in disk reads of rows. The swarm
   reported only client-side latency, cut off at 30 s. Before trusting
   any factor here, record `leep_search_duration_seconds` and process CPU
   per search on the VPS.
2. "Zero inline derivations" does not prove anything for the local arm.
   `leep_query_inline_derive_total` is counted only in `lease` or `off`
   mode with no live lease (`bounding()` returns early for `Local`). The
   split arm is a real measurement. The swarm should record
   `stats.blob_rows_derived` and `stats.tree_rows_derived`.
3. The early-stop numbers come from an ideal conjunction, without the full
   `Expr` evaluation per absent value. The real gain will be a little lower.
