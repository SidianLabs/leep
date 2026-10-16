# 0025. Repair tools: verify, rederive and drop
Status: Accepted
Date: 2026-10-16

## Context
Leep's safety argument covers what the stores do when calls fail or processes
die (ADR 0012, 0018, 0021). It says nothing about data that is damaged at
rest: a bad disk, a bad restore, a bug in an older version, an operator with
a SQL prompt. ADR 0021 records the consequence under "Known gaps": most
derived rows carry no checksum, so a flipped bit in a `generation`, `skiplist`,
`last_merge-v1` or trigram row can change an answer silently, and a wrong
generation can make `land` report success without moving a ref. File blobs
are not re-hashed when read. Until now an operator who suspected any of this
had nothing to run. The M6 list asks for "repair tools: verify reachability,
re-derive or drop derived data by kind".

Two facts make a repair tool tractable. Changesets, tree nodes and blobs are
content-addressed, so what they should contain is known from their names.
And every derived row is a pure function of content (ARCHITECTURE.md §5), so
overwriting a row with its recomputation can only make it right.

## Decision

### Commands
Both run offline against the stores, opened from `--meta` and `--blobs` the way
`gc` does, never through the HTTP API, and both are safe beside running
servers. They never create a database or blob directory that does not exist.

```
leep-server verify [--ref R]... [--full] [--rehash] [--derived KIND...] [--sample N]
                   [--max-problems N] [--json]
leep-server repair rederive --kind KIND... [--all | --ref R...] [--dry-run] [--json]
leep-server repair drop --kind KIND... [--dry-run] [--json]
```

Exit status: `0` clean, `1` problems found (for `repair rederive --dry-run`: rows
that need repair; for a real run: rows that could not be recomputed), `2` could
not run (bad arguments, a refusal, an unreachable or missing store). Progress
goes to stderr, the report to stdout, as text or with `--json` as one object.

`verify` writes nothing. `repair` writes only derived rows (and the content-
addressed tree and history nodes that recomputing them rewrites). Neither
touches changesets, file blobs or refs.

**Deviation from the task text: `--rehash`, not `--blobs`.** `--blobs` is already
the global option that names the blob store, and one name cannot be both. The
flag that re-hashes every reachable blob is `--rehash`.

### What `verify` checks
One walk over history, parents before children, with a visited set of ids as
its only per-changeset memory (the stack holds an id and a parent list per
level, about a hundred bytes, never changesets). Tips are the selected refs;
`--full` adds every stored changeset. For each changeset:

| Check | How | Findings |
|---|---|---|
| History | every changeset reads back (its id is the hash of its bytes in the SQL backends) and every parent exists | `corrupt_changeset`, `missing_changeset` (with the child that names it) |
| Trees | from the changeset's root tree, every node not seen before is read and hashed against its address | `missing_tree`, `corrupt_tree` (with the changeset that first reaches it, which is the oldest, and the directory) |
| Blobs | every blob named by a `Modify` or a tree entry, once each, exists; with `--rehash` its bytes hash to its address and its length equals the entry's `size` | `missing_blob`, `corrupt_blob`, `blob_size_mismatch` (with changeset and path) |
| Derived rows | for each kind selected, the stored row equals a recomputation | `derived_mismatch`, `derived_unverifiable` |

After the walk: rows keyed by content (`blob-trigrams-v1`, `tree-trigrams-v1`,
`history-node-v1`) are scanned and recomputed from the blob, the tree node or
by hashing the row; attestations and bookkeeping rows are checked for
structure; blame rows for the files at ref tips are checked against the file;
and the ref log is replayed.

| Kind | Check |
|---|---|
| `generation`, `skiplist`, `last_merge-v1`, `root_tree`, `history-tree-v1` | recomputed by the code that derives them, from the changeset and its parents' rows. A `history-tree-v1` row's nodes must also exist and match their addresses (the root only when sampling, all of them with `--full`) |
| `blob-trigrams-v1` | recomputed from the blob. A row whose blob was collected is skipped: GC never deletes derived rows |
| `tree-trigrams-v1` | recomputed from the tree node and the blob rows below it, never from other directories' filters, so damage in one cannot mislead the check of another |
| `history-node-v1` | the row hashes to its key and decodes |
| `blame` | decodes; has one entry per line of the file at the tip with that line's text; credits only stored changesets. A recomputation that credits lines differently is a note, not a problem: blame diffs run under a time limit (blame.rs), so two correct answers can differ |
| `signature-v1`, `review-v1` | decode; a signature's key id matches its public key. With `--full`, each row is matched to its changeset (by hashing every stored changeset with every key id and reviewer found in a row: the row key hashes them in) and signatures are verified with Ed25519. A row that matches no changeset is `bad_attestation`. Whether a key is trusted is server configuration, not store data, so it is not checked |
| `rebased-origin-v1` | 32 bytes, naming two stored changesets |
| `blob-claim-v1`, `blob-condemned-v1`, `blob-rescue-v1`, `blob-doom-v1` | decode (`gc::decode_row`) |
| ref log | newest first, each entry's `new` equals the next newer entry's `old` (`reflog_chain_break`), and the newest entry of each ref equals the ref (`reflog_divergence`). A ref with no entries is counted, not reported: refs that moved before ADR 0018 have none. A divergence is looked at again, up to three times, before it is reported, so a ref that moved while `verify` ran is not a finding |

**Recomputation is local, in history order, with substitution.** A row is
compared with the function of the changeset and its parents' stored rows.
Because parents are visited first, a damaged row is reported where it is, and
the recomputed value is substituted for it in the in-memory view that later
checks read. A skiplist that recorded a parent's true generation is therefore
not blamed for that parent's damaged generation. By induction on history, a
walk with no finding proves every row is the true value. Kinds a kind depends
on are checked with it (`skiplist` needs `generation`; `history-tree-v1` needs
`generation`, `skiplist` and `root_tree`; `tree-trigrams-v1` needs
`blob-trigrams-v1`), in dependency order. `leep_derive::integrity::kinds` is the table.

**Nothing is written.** Derivation writes as it goes (tree nodes, history
nodes, rows it finds missing). `verify` runs the real derivation code against
shadow stores: reads fall through to the real stores plus the substitutes,
writes land in memory and changesets and refs refuse writes. This costs one
copy of the code path's work and makes "verify cannot change a store" a
property of the types, not of discipline. A test snapshots every row, ref, log
entry and blob around a verify.

**Sampling.** The default checks about `--sample` (256) changesets and, for each
other kind, that many rows, plus every ref tip and root changeset. A changeset
is chosen by the leading bytes of its id, and rows are read from eight evenly
spaced windows of the key space, so a rerun checks the same ones and a hash
key is unbiased. Reachability (history, trees, existence of blobs) and the ref
log are always complete. `--full` recomputes everything and also walks
changesets no ref reaches. In sample mode a mismatch at a sampled changeset
may come from a damaged parent row that was not sampled; `--full` attributes
it.

**What is not a problem.** A missing derived row is counted (`missing` in the
report), because rows derive on demand and a crash leaves some missing (ADR 0021).
`repair rederive` fills them.

**Damage versus failure.** An error that says the data is wrong (`Encoding`,
`NotFound`, or a `Storage` error whose text starts with "corrupt" or says
"missing") is a finding and the run continues, skipping what depends on the
damaged object. Any other error, such as a lost connection, stops the run with
exit status 2, because the result would be incomplete.

**Bounded output.** Only the first `--max-problems` (1000) are kept; the rest
are counted.

### What `repair` does
`rederive` walks the chosen changesets in history order (every stored changeset,
or the history of `--ref`s), recomputes each selected kind, and writes the row
when it is missing or differs. The kinds a selected kind depends on are
rederived with it, so a skiplist is never rebuilt from a damaged generation.
It runs the real derivation, so it also rewrites the content-addressed nodes it
derives: `root_tree` restores tree nodes that are missing or damaged
(every node is on the spine of the changeset that introduced it, which is
visited before any changeset that reads it), and `history-tree-v1` restores
history nodes the same way. Rows keyed by content (`blob-trigrams-v1`,
`tree-trigrams-v1`) are rewritten for the rows that exist; a row whose subject
is gone is left alone. `--dry-run` does the same against the shadow stores and
reports what it would write.

`drop` deletes every row of a kind, so it derives again on demand. All kinds
named are checked before anything is deleted; one refusal refuses the command.
Dropping `history-tree-v1` also drops `history-node-v1`, which only that kind
refers to, in that order, so an interruption never leaves a pointer without
its node.

**Kinds that can be rederived or dropped:**

| Kind | Why history can regenerate it |
|---|---|
| `generation`, `skiplist`, `last_merge-v1` | pure functions of the changeset and its parents' rows |
| `root_tree` | `changes` applied to the first parent's tree |
| `history-tree-v1` (with `history-node-v1`) | a function of the changeset, its parents' history trees and root trees |
| `blob-trigrams-v1`, `tree-trigrams-v1` | functions of a blob, and of a tree node's contents |
| `blame` (drop only) | recomputed on demand; keyed by a hash of (changeset, path) so rows cannot be listed, which rules out rederive |

**Kinds that are refused, and why:**

| Kind | Why it cannot be recomputed |
|---|---|
| `signature-v1`, `review-v1` | attestations clients and reviewers submitted; nothing in history contains them |
| `rebased-origin-v1` | written by `rebase` to link a server-made copy to its original; the copy's `extra` does not say which changeset the server rebased (a client can forge `leep.rebased_from`), and `land`'s retry detection (ADR 0021) and signature `via` depend on it |
| `blob-claim-v1`, `blob-condemned-v1`, `blob-rescue-v1`, `blob-doom-v1` | the GC fence (ADR 0012). Dropping a rescue or doom row, or a claim a client relied on, can let a collect delete a blob a client was just told it may reuse |
| `history-node-v1` alone | `history-tree-v1` rows point at these nodes; drop `history-tree-v1`, which removes both |
| the ref log | not derived: the only record of how refs moved |
| any other name | unknown to this version, possibly written by a newer one |

### Store API
Two `MetaStore` methods, both data operations that use the existing primary key
`(kind, key)`, so no schema, index or migration changes:

- `list_derived(kind, after, limit)`: rows of a kind in ascending key order,
  paged like `list_changesets`.
- `delete_derived_kind(kind)`: removes every row of a kind and returns the
  count, in batches of 10 000 rows so a large kind is many short transactions
  rather than one long lock. An interrupted call leaves the kind partly
  dropped, which is the same as a kind that was never fully derived.

Both have contract tests that every backend runs (memory, SQLite, Postgres, and
through the quiet fault wrapper), and a test that a delete spans several
batches. The defaults return an error, so wrappers that never list rows keep
compiling; every real backend implements both.

## Results
- `verify --full --rehash` over the database of a 60-second chaos soak (two
  server restarts, two fenced GCs, 22 agents, rebases, signatures, reviews),
  after every SIGKILL of the crash test (sample mode, nine rounds on SQLite
  and Postgres) and at its end (`--full --rehash`), and after the git import
  plus 50-agent swarm of the e2e suite: clean each time. No bug in Leep was
  found in those stores; the checks exist so a future one is. They did show
  that a killed server leaves some eager rows (`generation`, `skiplist`)
  missing, which is why a missing row is counted and not reported. The
  soak harness and the crash and import tests now run `verify` at the end.
- Corruption-injection tests (`integrity/tests.rs`, `leep-e2e/tests/verify.rs`)
  cover a lost blob, wrong blob bytes, a flipped tree node byte, a missing
  tree node, a missing or replaced changeset row, and a damaged generation,
  skiplist, last-merge, root-tree, history-pointer, history-node, blob-trigram,
  tree-trigram, blame, signature, review, claim and rebase row, plus a ref
  that moved without a log entry and a log with a hole: each is reported
  exactly once with its kind, key, changeset and path, and `repair rederive`
  then makes `verify --full` clean. Dropping each droppable kind leaves every
  answer of `log`, `blame`, `search`, `merge_base`, `is_ancestor` and
  `last_change` unchanged.
- Timings for 10 000 changesets are in PERFORMANCE.md.

## Known limits
- `verify` reads every changeset twice (once to find parents, once to visit
  it) and holds a set of visited ids, tree nodes and blobs: about 100 bytes per
  changeset and 50 per distinct tree node and blob. A history of 10 million
  changesets needs about a gigabyte.
- Sample mode does not look for orphaned attestation rows, because it cannot
  enumerate changesets cheaply; `--full` does.
- A missing or damaged blob cannot be recomputed. Restore it from a backup of
  the blob store; nothing else can supply the bytes. The same holds for a
  changeset row. `verify` names exactly which objects to restore.
- A ref log that disagrees with the refs is reported and never edited: it is
  the record of what happened, and which side is right is the operator's call
  (OPERATIONS.md, "Verify and repair").
- Opening a store applies whatever schema migration the binary carries (ADR
  0023), as `gc` does, unless `--no-migrate` is set, in which case `verify`
  and `repair` refuse to run while migrations are pending. Run them with the
  version the servers run.

## Future work: checksums for the unverified kinds
Recomputation finds every wrong derived row but costs a changeset walk, and it
cannot tell which of a row and its inputs is damaged without that walk. A
checksum per row would let a server notice damage at read time. It is a
persisted-format change, so it is a proposal here and not part of this change
(`change-persistent-format`):

- New kind versions `generation-v2`, `skiplist-v2`, `last_merge-v2`,
  `blob-trigrams-v2`, `tree-trigrams-v2`, `history-tree-v2`, each row being the
  v1 value followed by a 4-byte truncated BLAKE3 over
  `kind ‖ 0x00 ‖ key ‖ value`. Binding the key makes a row moved to another key
  fail, which a content checksum alone would miss.
- Readers accept the checksummed kind, and fall back to deriving when the
  checksum fails, which repairs the row by overwriting it instead of failing
  the request. v1 rows are ignored (FORMATS.md rule 2) and `repair drop` removes
  them once every server runs the new version.
- The golden bytes tests of `leep-derive` gain checksums, and the fault test
  that flips every bit of every row would then expect an error or no change
  for every kind, not only the self-verifying ones.
- `verify` keeps its recomputation: a checksum proves a row is what was
  written, not that what was written was right.

Other follow-ups: a scheduled `verify` with metrics (the numbers are in the
report), a parallel walk, and `verify --since` to check only changesets newer
than a previous run.

## Consequences
- Operators can check a store after a restore, an upgrade, a crash or a disk
  scare, and get a precise list of what to restore or repair.
- Two methods are added to `MetaStore`; backends outside this repository need to
  implement them to be used with these commands.
- The soak and crash suites gain a final `verify`, so a regression in any
  derived kind's determinism shows as a finding, not as a wrong answer a
  user sees later.
- `verify --full --rehash` reads the whole repository. Run it from a replica or
  in a quiet window; the default is cheap enough for a cron job.
