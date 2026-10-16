# 0023. Versioned schema migrations
Status: Accepted
Date: 2026-10-16

## Context
`leep-meta` created its tables with `CREATE TABLE IF NOT EXISTS` every time a
store connected, under a Postgres advisory lock. That worked while the schema
only grew by whole tables (the `ref_log` table of ADR 0018 arrived this way),
but it cannot express anything else: adding a column, changing a type or
backfilling a row has no "already done" check to hang on `IF NOT EXISTS`, and
nothing records which version of the schema a database is at. Two problems
follow. A server may start on a database written by a newer server and write
rows it does not understand. And no test can say "this database, made by that
release, upgrades cleanly", which the M6 upgrade-path item asks for.

## Decision

### Migrations are an ordered, append-only list per backend
`crates/leep-meta/migrations/{sqlite,postgres}/NNNN_name.sql`, compiled into
the binary with `include_str!` and listed in `SQLITE_MIGRATIONS` and
`POSTGRES_MIGRATIONS`. Versions are 1, 2, 3... with no gaps. The two backends
have the same versions and names (a test checks it) but their own SQL, since
the dialects differ. Migrations only go forward; there are no down scripts. To
undo a change, write a new migration.

Migration 0001 is today's schema: `changesets`, `refs`, `derived`, `ref_log`
and the `ref_log_name` index, written with `IF NOT EXISTS` so that it can also
finish a database made before the ref log existed. It is the only idempotent
migration. Every later one runs exactly once and uses plain `CREATE`/`ALTER`.

### `schema_migrations` records what ran
`version` (primary key), `name`, `applied_at` (unix seconds) and `checksum`
(BLAKE3 of the migration's SQL, with CRLF normalized to LF). The migrator
creates the table itself, inside the same transaction as the first migration.

### One transaction, under a lock
A migration run is a single transaction: Postgres takes
`pg_advisory_xact_lock` (the key the old connect code used, so old and new
servers exclude each other during a rolling upgrade); SQLite uses
`BEGIN IMMEDIATE`, so concurrent migrators queue on the write lock (up to the
busy timeout). The run reads `schema_migrations` after it has the lock, so
servers starting together apply each step once and the rest find it done.
Every pending migration, plus the bookkeeping rows, commit together or not at
all. A migration that fails leaves the database at the version it started at.
Both databases make DDL transactional, which is why this is enough; a
migration that cannot run in a transaction (`CREATE INDEX CONCURRENTLY`) needs
its own ADR and a different mechanism.

### Adoption of databases that predate versioning
A database with no `schema_migrations` rows but with Leep's tables is
un-versioned: it was made by a server from before this ADR. The migrator looks
at the tables and indexes present. Each migration lists the objects whose
presence shows it has already taken effect (`satisfied_by`). Walking the
pending migrations in order, it records every migration whose objects are all
present as applied (with the compiled checksum, without running the SQL), and
runs the first one that is not, and everything after it. Today that means:

| Found | Result |
|---|---|
| Nothing | Run 0001. |
| `changesets`, `refs`, `derived`, `ref_log`, `ref_log_name` | Record 0001; run nothing. |
| `changesets`, `refs`, `derived` only (before ADR 0018) | Run 0001, which creates the rest. |

Adoption applies only when the history is empty. A database with history is
never inspected this way.

### The migrator refuses what it does not understand
- **Newer database.** If a recorded version exceeds the newest this binary
  knows, it fails with `database schema vN is newer than this server (max vM);
  upgrade the server` and writes nothing. This holds in every mode and for
  every command that opens the store.
- **Edited migration.** If a recorded checksum differs from the compiled
  one, it fails and names the migration: applied migrations are immutable.
- **Unknown or missing history.** A recorded version the binary has no
  migration for, or a pending migration older than the newest applied one,
  fails.

### Automatic by default, explicit on request
By default a server migrates when it starts, as it has always created its
tables. That keeps a single-container deploy at "start the new image". Operators
who want a change window, or who run Leep under an account that may not alter
the schema, use:

- `--no-migrate` (`LEEP_NO_MIGRATE=1`): open the store read-only with respect
  to the schema, and refuse to start (or run `gc`, `derive-search`) while
  anything is pending or an un-versioned database still has to be recorded.
- `leep-server migrate`: apply pending migrations and exit.
  `migrate --status` prints the version and history; `migrate --dry-run`
  prints what would run, with its SQL. Both change nothing.

Nothing is ever applied automatically that the operator cannot first inspect
with `--dry-run`, and no migration may drop or rewrite data without its own ADR
that says how an operator takes the step explicitly. Until such a migration
exists, auto-migrate stays the default.

### Diagnostics
`MetaStore::schema_version()` (a provided method returning `None` by
default) reports the newest applied version; the server exports it as the gauge
`leep_schema_version`.

### Every schema change comes with a fixture
A schema change is a new numbered migration, in both backends, plus a test
fixture of the schema it upgrades from, populated with data written by SQL of
that version, and a check that the data reads back through `MetaStore` after the
upgrade (`crates/leep-meta/src/migrate_tests.rs` has the template). With no
release tagged yet, the fixtures are the pre-ref-log schema and the un-versioned
schema of the last commit before this ADR. Once releases are tagged, the
fixtures are one per tagged version, which is how "upgrade from every earlier
tagged version" is tested without keeping old binaries.

The migrator takes its migration list as a parameter, so tests exercise the
multi-step path, a failing step and concurrent runs with lists they build, not
only the compiled one.

## Consequences
- Existing databases need no manual step: the first server to open one records
  version 1.
- A rolling upgrade works in one direction. While old and new servers share a
  database, an old server keeps running on the schema it knows (migration 0001
  is the schema it creates) but must be upgraded before a migration that
  changes what it reads or writes. Such a migration says so in its own ADR and
  needs the old servers stopped first. A server older than this ADR cannot
  detect a newer schema, so the first migration after 0001 must not be applied
  while one still runs.
- A new server started against a database a later release migrated refuses,
  instead of corrupting it. Downgrading means restoring a backup.
- Editing a shipped migration, even to fix a typo, is a startup error on every
  database that applied it. Fix forward with a new migration.
- Each migration costs two SQL files and a fixture.
