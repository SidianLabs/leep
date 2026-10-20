# 0055. Store format features
Status: Accepted
Date: 2026-10-20

## Context
ADR 0053 lets `leep-server compact-changesets` rewrite changeset rows as
zstd frames. A server from before ADR 0053 reads a compressed row as plain
postcard bytes and fails on it: with an error for the request at best,
after it has already served traffic. Nothing stops an operator running the
compaction while one such server is still up. The ADR says not to, but the
only guard is the operator reading it.

The schema version (ADR 0023) does not help. Migration 0003 deliberately
tolerates old servers, so the version says nothing about whether rows have
been rewritten. A store can sit at v3 for months before anyone compacts
it.

## Decision
**Table.** Migration 0004 adds `store_features(name TEXT PRIMARY KEY,
added_at BIGINT NOT NULL)` to both backends. A row names a format feature:
something the store's rows use that a server must understand to read
them. If the store already has a changeset dictionary (it was compacted at
v3), the migration inserts `changeset-zstd-dict` itself. Otherwise the
table starts empty.

**Writers.** `compact-changesets` inserts `changeset-zstd-dict` (`ON
CONFLICT DO NOTHING`) before it stores a dictionary, and again before it
rewrites rows with an existing one. Servers of this version start
compressing new rows within a minute of a dictionary existing, so the row
must come first. A dry run writes nothing. A future feature follows the
same rule: record it before writing the first row that needs it.

**Readers.** On open, after migrations, every store reads the table. It
refuses to start if a row names a feature this binary does not know,
naming the features ("the metadata store uses format features this server
cannot read (x); ... Upgrade this server"). This binary knows
`changeset-zstd-dict`. The check runs wherever a store is opened, so `serve`,
`gc`, `verify`, `repair`, `upgrade-index`, `backup` and
`compact-changesets` all refuse.

**Backups** (ADR 0026) copy the table with the others. A restore with
`--force` clears it first. A backup from v3 is migrated on a scratch copy,
which adds the row if the backup has a dictionary. The manifest format is
unchanged.

## Upgrade and mixed versions
Migration 0004 creates a table and at most one row, and servers of v3
neither read nor write it, so it can be applied beside them like 0003.

**Binaries already deployed do not check the table.** Servers from before
this ADR, including those from ADR 0053's release that read compressed rows
fine, never look at `store_features`. Servers from before ADR 0053 are the
ones that cannot read compressed rows, and they are also the ones this
check cannot reach. So the operator rule from ADR 0053 still holds for this
upgrade: finish the rollout before running `compact-changesets`. The table
protects the next format change. Once every server checks, a newer
binary's feature is refused at startup instead of failing reads.

A feature is never removed by any tool. Going back to a binary that lacks
one means restoring a backup taken before it was recorded, as for any
migration.

## Consequences
- A newer format change has a mechanical guard: a server that cannot read
  the rows does not start, and the error names the feature.
- Opening a store costs one more small query.
- A store whose dictionary rows were deleted by hand still names the
  feature. That is harmless: the server can read whatever rows remain.
- Rows a v3 server compressed with no dictionary (a plain row that would
  start with the zstd magic, which needs 40 parents and a first id that
  begins `b5 2f fd`) are not recorded as a feature. Servers since ADR 0053
  read them, and older ones never wrote them.
