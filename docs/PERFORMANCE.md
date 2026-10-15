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
Path-history rows (`cargo bench -p leep-derive -- 'log_path|last_change|derive_history'`) use a 10k-changeset linear history in which `d0/f` changes every 10th changeset and `rare/f` every 1000th. Numbers are
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
  in the distance.

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
  changesets linearly.

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
- Blame could start from the index's last change of the file instead of
  walking first parents from the tip, but the index follows all parents and
  blame follows first parents, so it would need its own first-parent entries.
