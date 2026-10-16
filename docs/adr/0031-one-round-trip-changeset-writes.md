# 0031. One round trip of reads and one write per changeset
Status: Accepted
Date: 2026-10-16

## Context
Creating a changeset, through `POST /v1/changesets`, the git importer or the
rebase inside `land`, issued every metadata statement on its own and awaited
each before the next. Counted with `log_statement = all` on a local Postgres 16
holding the first 3,795 commits of git/git (release builds):

| Path | Statements | Writes among them |
|---|---|---|
| `POST /v1/changesets`, one file in a new directory | 34 | 4 |
| `POST /v1/land`, fast-forward | 9 | the CAS transaction (5) |
| `POST /v1/land`, replaying one changeset | 50 | 5, plus the CAS |
| `leep-git import`, per commit (3,795 commits, 42.6 s) | 38.3 | 7.8 |

The writes were the smaller part. Writing the changeset, its root tree,
generation and skiplist rows (and the rebase link or the importer's mapping) in
one transaction would have saved three or four statements of 34. Per imported
commit the reads were: 12.6 skiplist reads (the `generation` and `skiplist`
derivations each read their own rows before and after taking the kind's lock,
then the parents' rows, and building a skiplist reads the list of each
first-parent ancestor 1, 2, 4, ... steps up, one after another), 5.4
`blob-condemned-v1` reads (one per uploaded blob, per content checked, and per
rewritten tree node, each after that node's write), 5.0 generation reads, about
3 changeset reads, and one root tree and one mapping lookup. Each is a primary
key lookup of about 0.1 ms; the cost is the sequence.

## Decision
1. **Batched writes on `MetaStore`.** `put_derived_many(&[DerivedRow])` writes
   many rows with the result of writing them in order (a repeated key keeps its
   last value). `put_changeset_with_derived(&cs, &[DerivedRow])` stores a
   changeset with rows computed for it, writing the rows even when the
   changeset was already stored. Both are provided methods: the defaults call
   `put_changeset`, then `put_derived` per row, the order changeset creation
   always used, so every backend and test double keeps working. Postgres does
   each in one statement (`INSERT ... SELECT FROM UNNEST`, the changeset behind
   a data-modifying `WITH`, which Postgres runs to completion whether or not it
   is read), SQLite in one transaction, the memory store under both of its
   locks. The contract covers order, repeated keys, empty and large batches,
   idempotent re-puts and kind isolation; backend tests refuse one row with a
   trigger and check that nothing of the batch remains.
2. **One lock order for batched rows.** A multi-row upsert locks rows in the
   order it inserts them. The first import with batched claims deadlocked on
   Postgres: two commits in flight whose new blobs overlapped wrote their
   claims in different orders. Both statements insert `ORDER BY kind, key`, so
   every batch locks in one global order and overlapping batches queue. The
   changeset row is locked by no other statement that also locks derived rows.
   A test races 16 overlapping batches in rotated and reversed orders on every
   backend; without the ordering it fails on Postgres.
3. **Creating a changeset reads in one round and writes once.**
   `store_changeset` issues, concurrently, `get_changesets` for the parents,
   `get_derived_many` for the parents' generations (those not in the ancestry
   cache) and for the contents' condemnation rows, and single reads of the
   first parent's root tree and skiplist. It checks the parents and the blobs
   in the old order, builds the tree, computes the generation and the skiplist
   with the code the derived kinds use (`Skiplist::compute` is shared with
   `Skiplist::derive`), and writes the changeset, its `root_tree`,
   `generation` and `skiplist` rows and any extra rows with one
   `put_changeset_with_derived`. As with the log's read-ahead (ADR 0030),
   whatever the round does not supply (a failed batch, an absent or
   undecodable row) is read or derived singly where it was before, so errors
   and derivation happen where they did.
4. **A skiplist cache.** `Repo` keeps skiplists in a bounded two-generation
   cache (16,384 per generation; about 25 MiB at a depth of 100,000), so the
   chain walk for a new changeset is served from memory once a server has seen
   that stretch of history. It holds lists the process computed and rows read
   while creating a changeset. Verification and repair never create
   changesets, and they substitute recomputed rows for damaged ones as they
   walk, so their caches hold only computed lists, never a damaged row read
   before its substitution.
5. **Blob writes check condemnation in batches.** The tree nodes a changeset
   rewrites are written (four at a time), then their condemnation rows read in
   one `get_derived_many`, and the rare condemned one rescued as before
   (`put_rescuing_many`). `POST /v1/blobs/missing` reads every fence in one
   batch and writes its claims in one; `put_blobs` does the same for the
   importer's uploads. The GC handshake (ADR 0012) needs each blob's write
   before its own condemnation check, and each fence before that blob's
   existence check and claim; both orders hold.
6. **Rows that belong to a changeset go in its write.** A rebase writes its
   `rebased-origin-v1` link with the copy, and the importer writes the
   `git-to-leep-v1` and `leep-to-git-v1` rows with each changeset
   (`create_changeset_with_rows`, written only for a valid changeset), instead
   of in background tasks behind it.
7. **Refs are unchanged.** `cas_ref_logged` is still a statement and a log
   insert in one transaction of its own; no ref moves inside a batched write.
   A fast-forward land is the CAS and three reads, as before.

## Crash safety
Before, a crash between statements could leave a changeset without some of its
rows, a rebased copy without its link, or an imported changeset without its
mapping. Each was harmless: rows derive again on demand, an unlinked copy is
on no ref (the CAS comes after it), and a rerun imports the commit again to
the same id. On Postgres, SQLite and memory the changeset and those rows now
appear together or not at all. A backend on the default methods writes in the
old order and can leave the same states as before, and nothing relies on the
atomicity: derivation still fills in missing rows, and `land` still finds its
copy only through the link. The fault-injecting test wrapper keeps the default
methods, so the fault sweeps drive the non-atomic order.

Tree nodes are written before the changeset that references them, as before,
but now up to four at once, so a crash can leave any subset of a changeset's
new nodes rather than a bottom-up prefix. No stored changeset references them
either way, and GC reclaims them after the grace window.

## Consequences
Same database, release builds, statement logging off for timings (details and
the method in `docs/PERFORMANCE.md`, "Writing changesets"):

| | Before | After |
|---|---|---|
| Import of 3,795 commits, Postgres | 42.6 s / 38.7 s | 14.1 s / 13.8 s |
| Statements per imported commit | 38.3 | 7.8 |
| `POST /v1/changesets`, statements (cold server / steady) | 34 | 11.7 / 7.4 |
| `POST /v1/changesets`, p50 | 13.4 / 9.4 ms | 5.1 / 4.1 ms |
| Replaying land, statements (cold / steady) | 50 | 26.6 / 22.4 |
| Replaying land, p50 | 17.4 / 12.8 ms | 9.0 / 7.5 ms |
| Fast-forward land, statements; p50 | 9; 2.9 / 2.3 ms | 8; 2.5 / 2.2 ms |
| Import of 914 commits, SQLite | 8.5 s | 4.4 s |

- Imported data is byte-identical: every changeset, `root_tree`,
  `generation`, `skiplist` and mapping row, the claim keys and the blob set
  match an import by the old code. A test compares the rows creation writes
  with those the derived kinds compute from scratch, with and without working
  batched reads, and the skiplists with a walk of the first-parent chain.
- A freshly started server pays a skiplist read for each chain entry it has
  not seen (about log2 of the history, as every create did before) until it
  has worked through that stretch of history.
- A create issues up to five statements at once, each on its own pooled
  connection and none waiting on another, so a burst queues on the pool
  rather than deadlocking.
- The rest of a replaying land (merge base, tree merge, the landed-copy scan)
  still reads one row at a time; those are reads outside the write path.
- `MetaStore` grows by two provided methods; nothing persisted or on `/v1`
  changes. A backend that does not override them gets the old cost, and no
  error; a new backend should implement them with one atomic round trip each,
  in a fixed row order.
