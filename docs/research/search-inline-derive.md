# Search on a moving head: inline derivation

> **Review, 2026-10-22.** Fixes 1 and 2 were approved and are implemented,
> with these changes: rows are written strictly a level's blob rows, then
> its filters, then the parent level, in batches bounded by rows (4,096)
> and bytes (16 MiB); the bounded scan keeps storing nothing and stopping
> early over budget; each index group gets its own warm loop; search
> derivation is single-flight per tree root, a waiter giving back its
> search pool permit; and the metric fix is additive
> (`leep_index_rows_total{kind}`), leaving `leep_index_derived_total`
> counting heads, since it feeds lease meta and `Retry-After`. Measured
> after both: a search 128 lands past the index takes 72 ms and 10 round
> trips in the harness below (2,989 ms and 697 before). See
> [PERFORMANCE.md](../PERFORMANCE.md), "Search rows derived in level
> batches (after E5)". Fix 3 is not done; per section 5 it waits for the
> next E5 run.

Status: research input, not a decision. Written 2026-10-22 against `main` at
718e2a3. Fixes 1 and 2 change neither `/v1` nor anything persisted. Fix 3
changes what `local` mode does with `Leep-Indexing: stale-ok`. That needs an
amendment to ADR 0058, and its API change is additive only.

## 1. Summary

- **The mechanism is confirmed, but not its attribution in E5.** A search
  on a ref name resolves the current head. In `local` mode it derives and
  stores every trigram row missing below that head before it walks. The
  cost of that grows with the number of lands since the last indexed head,
  and it is almost all **sequential metadata round trips**, not CPU. In the
  harness below, with 5 ms per metadata call (the VPS's ref reads were 25 ms
  p50 under load), a search 32 lands past the index takes 0.89 s and makes
  195 round trips. A search 128 lands past it takes 3.0 s and makes 697
  round trips. The same search on an indexed head takes 13 ms, and the CPU
  is 0.04 to 0.12 s in all of these cases. That is about **5.4 serial round
  trips per landed change**, and it fits E5's 4 to 8 s per admitted search.
- **E5's "35 rows from the warmer" is a misread metric.** For search,
  `leep_index_derived_total` counts warmed *heads*, not rows
  (`warm.rs:273-281`, counted at `warm.rs:187`). `leep_derive_total` counts
  rows from every caller, so the warmer's rows are in it too. The 1,552 +
  993 rows are roughly every row the minute's lands created (about 708 lands
  × 2 blobs). E5's numbers cannot say how those rows split between the
  searches and the warmer. Only the cost per search points at inline
  derivation.
- **The `skiplist` time is not on the search path.** `skiplist` is a
  write-path row computed in `create_changeset` (`repo.rs:832-845`).
  `Skiplist::compute` awaits up to about log2(generation) sequential
  skiplist reads on a cache miss (`derived/skiplist.rs:52-79`).
  `leep_derive_duration_seconds{kind="skiplist"}` is wall time including
  those awaits, so 18.7 s per 60 s across about 1,400 creates and rebases is
  about 13 ms of mostly waiting each. It is not a third of a core of CPU.
  Search touches skiplists only in `stale_target`, which `local` mode never
  reaches.

## 2. The request path

| Question | Answer |
|---|---|
| Which revision is searched? | The requested `rev`, resolved when the request arrives (`search/mod.rs:553`). A ref name therefore means its head at that moment. A cursor pins the first page's id (`mod.rs:554-565`). The swarm searches the ref name (`leep-soak/src/swarm.rs:454-457`). |
| Missing rows, `local` | `plan` returns `Inline` at once (`indexing/bounded.rs:327-331`, `416`). The search runs unbounded (`bounded.rs:1069`). Its first step, the root filter (`mod.rs:441`), goes to `search_tree_row` → `tree_row_unstored` (`search/index.rs:212-217`, `263-327`). That derives **and stores** every missing filter and blob row below the root, children first, one directory at a time, before any file is matched. Later misses are derived in `Opener::open` (`mod.rs:335`) and `check_file` (`mod.rs:764`). |
| Missing rows, `lease`/`off`, live lease | `fits` computes the missing rows in memory, storing nothing, within `--search-scan-budget` (512 files) and `--search-scan-bytes` (16 MiB) (`bounded.rs:378-399`). Over budget: `retry` gets a 503 `Indexing`, `stale-ok` falls back, and no header waits, then gets 8× the budget, then a 503 (`bounded.rs:425-470`). |
| Missing rows, `lease`/`off`, no live lease | Inline, as in `local`, counted in `leep_query_inline_derive_total` (`bounded.rs:332-337`). |
| What `stale-ok` does for search | In `local` mode, nothing. In bounded modes it applies only **over budget**: `stale_target` walks up to 64 first-parent ancestors (`STALE_SEARCH_STEPS`, `bounded.rs:296`, `596-617`). For each one it reads a skiplist, resolves the root or path entry and looks up its row, so the worst case is about 64 × 3 round trips. It answers at the first ancestor whose start row is stored, with `answered_at` and `behind`. Within budget, a `stale-ok` search is answered at the head. |
| A "last fully indexed revision"? | Not stored. Two things come close. (1) The warmer's in-memory `warmed[group][ref]` (`warm.rs:56`), which is neither shared nor queryable. (2) Implicitly, a stored root `tree-trigrams-v1` row: rows are written children first, so a stored filter means its whole subtree is stored. `stale_target` relies on that. The `index_frontier` table of ADR 0058 is deferred. |
| Why the warmer falls behind | (1) A pass runs the history, log-filter and search groups with `join_all` and starts the next pass only after all of them finish (`warm.rs:177-182`), so search warming runs at the pace of the slowest group. Path history trailed by hundreds of generations on the VPS. (2) A warm derives only the newest head per ref per pass, which batches lands, but with the same serial walk as a search, about 5.4 round trips per land (harness column D). At 10 to 25 ms per round trip and 12 to 21 lands/s, that is close to one fully serial chain's capacity, before any contention. (3) Searches and the warmer derive the same missing rows at the same time. Search kinds have no derive lock or single-flight (`index.rs` has none, unlike `derive_locks` for the other kinds), so they duplicate work. |
| Skiplist on the search path | Only in `stale_target` (`bounded.rs:607`), in bounded modes over budget. Not in `local`. |

**Whether (e) is already the case.** Partly. Rows are content-keyed, an
existing row stops the descent, and only blobs without a row are fetched
(`index.rs:196-206`). One land therefore derives 2 blob rows and 3 filters
(root, `agents/`, `agents/aN`), which the harness confirms. What is *not*
incremental is the **round-trip pattern**:

- the walk derives one directory at a time (`index.rs:269-295`), so the
  ~N sibling `agents/aK` directories of an N-land gap are derived one after
  another;
- each missing blob row is looked up twice: once in `blob_rows_of`'s batch
  (`index.rs:174`), then again in `blob_row_raw` (`index.rs:75`);
- every row is its own `put_derived` (`index.rs:99`, `317`), never
  `put_derived_many`.

There is also a CPU cost, smaller and secondary. A changed directory's
filter re-reads all its subdirectories' filters and fully decodes the blob
rows of all its files (`index.rs:306-312`), on every new head. On Linux's
root (about 25 dense filters, `MAINTAINERS`) that is a few ms per head, and
the harness's root costs about 5 ms on memory stores. It is not the problem
measured here.

## 3. Measurement

Throwaway harness, not committed: an ignored test in `leep-server/tests`,
release build, on a 4-vCPU container. Corpus: 20,000 source files (20 top
directories × 10 × 100), 6 root files, one of them 15,000 lines, and 500
agent directories of 2 files. That is 21,006 blob rows and 722 filters. Each
"land" is a swarm-shaped change: 2 files of 512 B in one `agents/aK`, or
occasionally `hot/hX`, chained on the previous head. Steps for each gap:

- A: search the new head.
- B: search it again.
- C: search the last indexed head.
- D: `derive_search(head)` alone, on a fresh gap of the same size.
- E: `derive_search` after every land, which is what a warmer that kept up
  would do.

Stores:

- memory;
- memory behind a wrapper that adds 5 ms to every metadata call, a model of
  the loaded VPS database;
- local Postgres 16 (fsync on, idle).

Round trips (rt) are counted by the wrapper.

**Memory + 5 ms per metadata call**

| Gap (lands) | A: search at head, ms (CPU s, rt) | Rows derived (blob / tree) | B: again, ms (rt) | C: at indexed head, ms | D: derive alone, ms | E: warm per land, ms per land |
|---|---|---|---|---|---|---|
| 1 | 77 (0.010, 13) | 2 / 3 | 12.8 (2) | 5.4 | 76 | 101 |
| 8 | 255 (0.016, 55) | 16 / 10 | 12.9 (2) | 12.9 | 255 | 81 |
| 32 | 892 (0.043, 195) | 62 / 34 | 12.9 (2) | 12.9 | 845 | 79 |
| 128 | 2,989 (0.120, 697) | 230 / 117 | 13.0 (2) | 13.2 | 2,881 | 79 |

**Local Postgres 16, idle:** A is 25 / 82 / 289 / 275 ms for gaps 1 / 8 / 32
/ 128, and B and C are 1 to 1.5 ms. **Memory:** A is 5.6 to 10 ms, and B and
C are under 0.1 ms.

What it shows:

- A search's extra cost is exactly the derivation of the gap (A ≈ D), and
  it scales with round-trip latency, not CPU. At 5 ms it is 13 to 54 times
  the warm search.
- At E5's gap (about 120 blob and 76 tree rows per search, so 60 to 70
  lands) and VPS latency (10 to 25 ms), the model gives 350 to 400 serial
  round trips, which is 3.5 to 10 s. E5 measured 4.1 to 8.0 s.
- Warming every land costs 79 ms per land at 5 ms latency. Batched across a
  gap it costs 845 / 32 = 26 ms per land, because the root and `agents/`
  are derived once. The warmer's "newest head per pass" is the right shape.
  What it lacks is round-trip batching and a cadence of its own.
- Searching an indexed revision costs the same as a warm search.

## 4. Ranked fixes

### 1. Derive search rows in batches by level; no format or API change
Replace `tree_row_unstored`'s one-directory-at-a-time walk with a
level-batched one:

1. Walk down from the root and collect the missing directories level by
   level. Each level is one `read_trees` and one `get_derived_many` for the
   level's subdirectory filters.
2. At the deepest level first, read every missing directory's file rows in
   one `get_derived_many`. Compute the missing blob rows concurrently, with
   no second `get_derived`. Store them with one `put_derived_many`.
3. Compute that level's filters and store them with one `put_derived_many`.
   Then go up a level.

Each level's write must complete before its parents' write starts. That
keeps the invariant `stale_target` and `tree_row` rely on: a stored filter
means a stored subtree. Check `write_allowed` per batch, as the other
kinds' staged writes do. Cap a batch at 4,096 rows, as ADR 0058 section 6
already assumes.

- **Expected gain:** round trips go from about 5.4 per land to about 3 per
  level of depth (about 10 to 15 for the swarm at any gap). In the model,
  gap 32 goes from 892 ms to about 60 to 80 ms, and gap 128 from 2,989 ms
  to about 70 to 90 ms. These are estimates from round-trip counts, to be
  measured. E5's admitted search should fall to about its own cost (the
  research's 0.45 s CPU, slower on the VPS), and the warmer gains the same
  factor. This fixes inline derivation and warmer throughput at once, in
  every mode, including the bounded scan, which uses the same walk.
- **Risk:** low to medium. The rows must stay byte-identical. Test against
  `recompute_tree_row`, the existing indexed-against-scan property tests,
  and a fault sweep that checks parent-closure after an interrupted batch.
  Memory grows with the gap, but is bounded by the batch cap.
- **ADR:** none. This is an implementation change under ADR 0014 and ADR
  0058, worth an implementation note.

### 2. Give each index group its own warm loop; single-flight search derivation; no format or API change
- Run the search group's passes independently of history and log-filter
  (`warm.rs:177-182`), so a slow path-history catch-up does not hold back
  search warming. Each group already keeps its own `warmed` map.
- Add a per-process single-flight keyed by tree hash around
  `tree_row_unstored` (as `derive_locks` do for the other kinds), so a
  search arriving while the warmer derives the same head waits for it
  instead of deriving it a second time.
- **Fix the metric:** count rows, not heads, in
  `leep_index_derived_total{kind="tree-trigrams-v1"}`, or add
  `leep_index_heads_total`, so the next swarm run can attribute rows.
- **Expected gain:** the gap a search sees shrinks to about one warm pass
  (250 ms plus a batched derive). That is a handful of lands, and with fix 1
  a few round trips. Duplicate derivation goes away.
- **Risk:** low. With three loops there are three tails of the ref log, a
  cheap indexed read each pass.
- **ADR:** none. ADR 0058 section 2's "one range per group" still holds per
  group. Note it in the implementation notes.

### 3. Honour `stale-ok` for search in `local` mode, at the last warmed head; additive API, ADR 0058 amendment
When a `stale-ok` search's root filter is missing in `local` mode, answer
at the warmer's last warmed head for that ref if it is a first-parent
ancestor. That head is the in-process `warmed` map, exposed as a cheap
read, so it costs no skiplist walk. Report the existing `answered_at` and
`behind`. Clients that send `retry`, and old clients, keep today's exact
inline answer. Exact-head search stays the default for the CLI; the MCP
already sends `stale-ok`.

- **Expected gain:** opted-in searches never derive, at any gap: warm cost
  (13 ms against 892 to 2,989 ms in the model).
- **Risk:** freshness. An agent may not find the file it just landed (ADR
  0058 open question 2). The `behind` field says so, and the gap is about
  one warm pass once fix 2 lands.
- **API:** additive only. The fields exist on `SearchResponse` and the
  header exists. Only the server behaviour in `local` mode changes.
- **ADR:** amend ADR 0058 sections 3 and 7, which today say `local` derives
  inline. Also worth doing: replace `stale_target`'s 64-step walk for
  search with the same last-indexed lookup in `lease` mode (the holder's
  map; for `off` servers, a value in the lease `meta`).

### Not recommended
- **(b) Inline only up to a budget, then scan or 503, in `local`.** A scan
  makes the same serial reads and skips only the writes (about 40% of the
  round trips). A 503 makes `local`, the single-server default, refuse
  searches that it can answer. Fix 1 makes the gap cheap enough that a
  budget has nothing to bound.
- **(d) Cap inline derivation per search and return `truncated` + `next`.**
  Derivation happens before the walk (the root filter needs the whole
  subtree), so a cut-off page would have no matches and no meaningful
  cursor position. `truncated` would also stop meaning "more matches". That
  is an API semantics change for no gain over fix 1.
- **Incremental filters (OR the old row with the changed children's).**
  This is cheap, but a row would then depend on history rather than
  content. It breaks determinism, `verify` and `recompute_tree_row`. It
  would be a persisted-format change (ADR plus `change-persistent-format`),
  and fix 1 removes the need for it.

## 5. Recommendation

Do fix 1 first. It attacks the measured cost (serial round trips) in
searches, the warmer and bounded scans alike, with no API, format or ADR
change. Do fix 2 with it, since it is small, and correct the
`leep_index_derived_total` reading in PERFORMANCE.md's E5 section. Then
rerun E5 arm A, recording `stats.blob_rows_derived` and
`stats.tree_rows_derived` per search (doubt 2 of `search-cost.md`). Do fix 3
only if admitted searches still derive noticeably after that, as an ADR 0058
amendment.

## 6. Doubts
1. The 5 ms wrapper models latency per call, not contention. On the VPS,
   Postgres CPU and the pool also queue writes. Fix 1 issues fewer, larger
   statements, which should help there too, but that is not measured.
2. The corpus's agent directories hold 2 files and the root holds 26
   entries, as the swarm's do. A change spread over many deep, wide
   directories (a refactor in Linux's `drivers/`) pays fix 1's per-level
   cost plus the decode of wide directories' file rows. That CPU part would
   then deserve its own look.
3. The fix-1 numbers are estimates from round-trip counts, not a prototype.
