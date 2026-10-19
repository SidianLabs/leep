# 0052. Changeset rows compressed with a per-store dictionary
Status: Accepted
Date: 2026-10-19

## Context
All of Linux (1,484,762 changesets) in Postgres 16 holds 2.9 GB in the
`changesets` table, about 1.96 KB a row with overhead, against 3.8 GB for
the whole git repository. A row is the changeset's canonical postcard
bytes, which its id hashes (ADR 0001). Postgres compresses a value only
when its row passes about 2 KB (TOAST, with pglz), so most rows are stored
as they are. Each row holds a commit message, author and committer names
and addresses, `git.*` extras, and the paths it changes, all of which
repeat across history, beside parent ids and blob hashes, which do not.

What a row holds, per changeset (`changeset_compression` example,
PERFORMANCE.md "Changeset compression"):

| | git/git v1.7.0 (21,205) | Linux, last 117,680 |
|---|---|---|
| Row, plain | 763 B | 2,279 B |
| Parent ids and blob hashes (incompressible) | 124 B | 506 B |
| Paths (15 changes a row on Linux) | 50 B | 518 B |
| Message | 374 B | 856 B |
| Author and `extra` (committer, time zones, `mergetag` signatures) | 181 B | 301 B |

A draft ADR (branch `docs/integer-ids-draft`) proposes server-local
changeset numbers, a `changeset_index` table that folds the per-changeset
derived rows into one, and zstd *segments* of about 40 consecutive
changesets. Segments compress best (2.0-2.15x measured), but they need the
numbers first, an offline backfill, a packing job with its own state, and
reads that decode a whole segment. The numbering is the larger change: a
new piece of shared state, a stopped fleet to upgrade, and new failure
modes (a wrong number, a restore that forgets the sequence).

Compressing each row on its own needs none of that. Measured on both
histories (git/git rows as stored; Linux rows built from `git log` the way
`leep-git` imports them):

| Encoding | git/git | Linux slice |
|---|---|---|
| Plain (today) | 1.00x | 1.00x (1.17x after pglz on rows over 2 KB) |
| zstd -3 per row, no dictionary | 1.36x | 1.58x |
| zstd -3 over 32 KB segments | 2.01x | 2.04x |
| zstd -9 per row, 64 KB dictionary trained on the store's rows | **2.08x** | **1.97x** |
| ... trained on the older half, applied to the newer half | 1.88x (-3) | 1.93x |
| ... trained on the other history (a dictionary shipped in the binary) | | 1.81x |

A dictionary trained on the store's own history gets almost all of the
segments' gain, row by row, and stays good as history grows: one trained on
the older half lost 2-6% on the newer half. A dictionary built into the
binary would be 8% worse on Linux and worse still on a repository unlike
the ones it was trained on.

## Decision
**Row encoding.** `changesets.data` holds either

- the canonical bytes (`Changeset::encode()`), as every row did before, or
- a zstd frame (it starts `28 b5 2f fd`) whose content is exactly the
  canonical bytes, compressed with the dictionary whose id the frame header
  names, or with none (id 0).

Readers decompress, decode and check that the bytes hash to the row's id, as
they checked plain rows (FORMATS.md "Integrity on read"). The id and every
hash are unchanged; nothing on the wire changes; `Changeset` and its
encoding are untouched. A plain row can begin with the frame magic only if
the changeset has 40 parents and the first one's id begins `b5 2f fd`, so a
reader that cannot use such a row as a frame also tries it as plain bytes;
both readings are checked against the id, so neither can return a wrong
changeset. Writers never store plain bytes that begin with the magic: they
compress them, with no dictionary if there is none.

**Dictionaries.** Migration 0003 adds
`changeset_dicts(id BIGINT PRIMARY KEY, created_at BIGINT NOT NULL, data
BYTEA NOT NULL)`. A row is a zstd dictionary whose header id equals the row
id, so every frame names the row it needs. Rows are only ever added;
nothing deletes one, since its changesets cannot be read without it.

**Writers.** A server compresses a changeset at level 9 with the
highest-numbered dictionary it knows, and stores the frame when it is
smaller than the plain bytes, otherwise the plain bytes. It loads the
dictionaries when it opens the store, again when a read meets a frame
naming one it lacks, and before a write when it last looked more than a
minute ago. Until a dictionary exists it writes plain rows, as before.

**`leep-server compact-changesets`** trains and stores a dictionary (64 KB
by default) on the first 50,000 changesets in id order, which is a uniform
sample because ids are hashes, then rewrites every plain row in batches of
500 with `UPDATE ... WHERE id = ...`. Later runs reuse the newest dictionary
and compress only plain rows (written before it existed, or by a server
that had not yet looked). `--retrain` trains a new dictionary and moves
every row to it; `--dry-run` reports the sizes and writes nothing. Fewer
than 1,000 changesets are left alone. Rewriting a row in place is safe
beside running servers of this version: the row's value is immutable, so a
reader sees old or new bytes that decode to the same changeset, and a
concurrent insert of the same id does nothing (`ON CONFLICT DO NOTHING`).

**Backups** (ADR 0026) copy `changeset_dicts` with the other tables, rows
byte for byte; `SnapshotInfo` reports how many dictionaries it copied. The
manifest format is unchanged. A backup from before migration 3 restores as
any older backup does: migrated on a scratch copy, which adds the empty
table.

## Upgrade and mixed versions
Migration 0003 only creates an empty table, and a server writes plain rows
until a dictionary exists, so it tolerates old servers: a rolling upgrade
proceeds as usual (ADR 0023). Old servers cannot read compressed rows, so
**`compact-changesets` must not run until every server is on this version
or later**; it is a separate, deliberate step, not part of `migrate`. After
it, Postgres keeps the old row versions as free space; `VACUUM FULL
changesets` (or `pg_repack`) returns it to the filesystem, and SQLite's
`VACUUM` does the same. Downgrading after a compaction means restoring a
backup, as for any migration.

## Consequences
- The `changesets` table shrinks to about 55-60% of its size: 19.9 to
  10.9 MB on git/git v1.7.0, 262 to 159 MB on the Linux slice, about
  2.9 to 1.75 GB on all of Linux (PERFORMANCE.md).
- Reads cost about 2 µs more a changeset to decompress, and Postgres
  returns fewer bytes; derivation and `log` measured the same within noise.
  Writes cost about 40 µs (git/git rows) to 100 µs (Linux rows) to
  compress, next to a create's milliseconds.
- A changeset row now depends on a second row. Losing a dictionary loses
  every changeset compressed with it, as losing the changesets would:
  backups carry both, `verify` reads every changeset through the same
  decoder, and nothing deletes a dictionary. A missing one is reported as
  "changeset row X needs dictionary N, which changeset_dicts lacks".
- Rows written by different stores of the same history differ in bytes (each
  store trains its own dictionary). Nothing compares rows across stores;
  backups copy them unchanged.
- The other per-changeset rows (`skiplist`, `generation`, `root_tree`, the git
  mappings) and their row overhead are untouched. Shrinking them needs
  changeset numbers (the draft's phase 1): `skiplist` entries as varint
  numbers would take 0.9 GB to about 0.14 GB, and one `changeset_index` row
  in place of four `derived` rows would save about 0.7 GB more on Linux.
  The dictionary table would then also serve segments, if they are still
  worth their cost once rows are compressed: the measured gap is 2-8%.
