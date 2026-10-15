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
