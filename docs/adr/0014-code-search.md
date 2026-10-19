# 0014. Server-side code search as derived data
Status: Accepted
Date: 2026-10-15

## Context
Agents need to grep a revision without checking it out. Walking every file of a
large tree per query reads the whole repository from the blob store each time,
which the thin-client design (principle 8) exists to avoid. We want a query to
cost roughly in proportion to the files that can match, and an update to the
index to cost in proportion to the change (principle 5).

## Decision
Two derived kinds, both keyed by content hash and so shared by every revision
that contains the same bytes, plus a planner and executor in `leep-derive`.

### Index
- `blob-trigrams-v1`, key = blob hash: the set of byte trigrams of a text blob,
  or a marker saying the blob is not indexed. Indexed means valid UTF-8 with no
  NUL byte and at most 1 MiB (`MAX_INDEXED_BYTES`). Larger and binary blobs get
  the marker and are skipped by search, counted in `files_skipped`. Symlinks are
  skipped too. Skipping is the only behaviour; there is no option to scan them.
  The set is a sorted list of 24-bit codes stored as delta varints.
- `tree-trigrams-v1`, key = tree node hash: a bloom filter over every trigram of
  every indexed blob below that directory. All filters have the same size (2^19
  bits, 3 hash functions), so a directory's filter is the bitwise OR of its
  children's filters and its own files' trigrams, and is derived from rows
  alone. A filter is stored dense (64 KiB) or, when smaller, as a sorted list of
  set bit positions. At 20 000 distinct trigrams the false positive rate of one
  probe is about 0.1%, at 100 000 about 8%; a query probes several trigrams, so
  the rate of a whole query is far lower. A filter that is too full only loses
  pruning, never correctness.

Both are pure functions of their inputs, written idempotently with
`put_derived`, and never deleted. Encodings are in FORMATS.md.

### Query planning
A query is a literal or a regex, case-sensitive or not, matched line by line
with the `regex` crate (literals are escaped into it, so there is one matcher).
The planner derives a boolean condition on trigrams that every file with a
match must satisfy:
- a case-sensitive literal requires each of its trigrams;
- a case-insensitive literal requires, for each window of three characters, any
  trigram from the case variants of that window, taken from the same simple
  case folding the regex crate uses (so `k` also admits the Kelvin sign);
- a regex is analysed on its `regex-syntax` syntax tree: literals and small
  classes expand to exact string sets that combine by concatenation and union,
  repetitions with a minimum of one and non-expandable parts contribute the
  requirements of their sub-expressions, anything else contributes nothing.
  Alternations become ORs, so `foo|bar` requires `foo` or `bar`.

Every rule may drop a constraint but never adds one, so the condition is
necessary, and an over-weak condition only costs speed. A condition capped at 96
trigrams is still necessary. If no trigram is required (`.*`, `\w+`, a literal
shorter than three bytes) the search scans files without consulting the index and
reports `stats.indexed: false`.

### Execution
`Repo::search(rev, query)` walks the tree depth first with an explicit stack and
yields files lazily in path order. A directory is skipped when its filter lacks a
required trigram; a file is skipped when its row lacks one; the rest are fetched,
at most 16 at a time with order preserved (`buffered`), and matched on blocking
threads. Opening a directory reads the filters of all its subdirectories and the
rows of all its files with one `get_derived_many` each, then the nodes of the
subdirectories that survive with one `read_trees`; when the walk enters a
directory it opens that directory's subdirectories ahead of it on their own
tasks, four at a time per search, and aborts them if it stops early. (Before,
each filter and each row was its own round trip, one directory at a time, which
set the pace of a search over a large tree.) Because the walk is lazy and
ordered, a search that has enough matches stops reading directories as well. Output is ordered by path then line, so it is
deterministic and identical with or without the index (a property test checks
this against a brute-force scan). Limits:
- at most `limit` matches (default 100, capped at 1000); `truncated` is set if
  more exist;
- at most 256 MiB of fetched blob content per request; `truncated` is set when
  it runs out, and the matches returned are those of the files read before it;
- lines are cut at 1024 bytes.
`path` restricts the walk to a directory or file, and `glob` (globset, `*` within
one path level, `**` across) filters files and skips directories its literal
prefix excludes. Stats report files considered, directories and files pruned,
files scanned and skipped, bytes scanned and rows derived. Under truncation they
cover the work done so far.

### Derivation is lazy
Rows are computed on demand by the first search that needs them, not in
`create_changeset`. Eager derivation of blob rows would cost one blob fetch and
sort per changed file on every commit, and refreshing the filters of the changed
directories would read every sibling's row, on the commit path that must stay
proportional to the change. The cost of laziness is that the first search of a
tree that was never indexed derives all of it before it can prune anything
(about 1 s for 20 000 files / 35 MiB on in-memory stores; real stores add the
latency of each blob fetch and row write). A search restricted by `path` derives
only that subtree. A later revision derives one blob row per changed file and one
filter per directory on the changed paths; everything else is shared.
`leep-server derive-search <rev>` computes the rows ahead of time and can be run
after an import or from a cron job; it is idempotent and safe beside running
servers. A search that has no required trigram derives nothing.

### API and clients
`GET /v1/search/{rev}?q=&regex=&i=&path=&glob=&limit=` returns
`{matches, truncated, stats}` (read role). `rev` is a ref name or a full id;
unknown is 404, an empty or over-long query, a bad regex or a bad glob is 400.
`leep search` (grep-style `path:line:text`, exit 1 when nothing matches) and the
read-only MCP tool `leep_search` are the clients.

## Consequences
- Selective queries read a few blobs instead of the tree (see PERFORMANCE.md:
  about 12x faster than a scan for a token in 3 of 20 000 files, on in-memory
  stores where a scan is cheap; against object storage the gap is larger).
- The index is the same order of size as the text: about 30% of the bytes of
  this repository's own source, up to about the size of the text for random
  prose, plus at most 64 KiB per directory. Rows are never garbage collected;
  a `-v2` kind would orphan the v1 rows like any other derived kind.
- A query whose trigrams occur in every file pays for a row lookup per file on
  top of the scan (about 1.7x a scan in memory). A heuristic that stops
  consulting rows when nothing is pruned is possible later.
- Matching is per line and the regex engine is linear time, so a pathological
  pattern cannot take unbounded time; the scan budget bounds I/O.
- Files over 1 MiB and binaries are invisible to search. Raising the cap changes
  what `blob-trigrams-v1` rows mean for blobs between the two limits, so it needs
  a `-v2` kind.
- Rejected: a postings index (trigram to file list) per revision. It answers
  faster but is rewritten for every revision unless it is sharded and merged, and
  that breaks "cost scales with the change". Per-blob sets plus per-directory
  filters give up some speed on dense directories for rows that are shared by
  content.
