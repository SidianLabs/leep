---
name: change-persistent-format
description: Safely change anything Leep persists or sends over the wire — Changeset, TreeNode, hashing, derived data encodings, SQL schema, blob layout, or /v1 API shapes. Use before editing any of these, even for a "small" field addition.
---

# Change a persistent format

Changeset and tree hashes are computed over postcard bytes. A reordered field,
a new field or a changed serde attribute changes every hash and orphans
existing history. Read docs/FORMATS.md first.

## Decide which case you're in

| You want to | Do this |
|---|---|
| Record new metadata on changesets | Use a namespaced `extra` key. No format change. Stop here. |
| Change a derived kind's encoding | Rename the kind (`foo-v1` → `foo-v2`). Old rows become garbage. |
| Add an API field | `Option`/`#[serde(default)]` on the `/v1` type. |
| Remove or rename an API field | Add `/v2` alongside `/v1`. |
| Change `Changeset`/`TreeNode` shape or hashing | Continue below. Needs an ADR. |
| Change the SQL schema | Add a numbered migration for both backends and a fixture (see below). |

## New object version (`changeset-v2`, `tree-v2`)

1. Write an ADR covering why, the migration plan, and mixed-version behavior.
2. Add a new type (e.g. `ChangesetV2`) with its own `KIND` tag. Leave the v1
   type byte-for-byte unchanged.
3. Decoding must accept both versions forever. Ids from v1 objects stay valid.
4. Decide how stores tell versions apart (a tag column or a prefix byte) and
   document it in FORMATS.md.
5. Add golden tests: fixed v1 bytes and their expected hash, checked into
   the tests, so an accidental change fails CI.
6. Update FORMATS.md and ARCHITECTURE.md §4.

## SQL schema change (ADR 0023)

Migrations are forward-only and numbered; a released one is never edited
(its checksum is recorded in every database that ran it).

1. Write an ADR (or extend the one for your feature) saying what changes and
   what an old server does against the new schema. A migration that drops or
   rewrites data needs its own ADR and must not be the first thing a server
   start does silently; say how the operator runs it deliberately.
2. Add `crates/leep-meta/migrations/sqlite/NNNN_name.sql` and
   `.../postgres/NNNN_name.sql` (next number, no gaps, same name), plain
   `CREATE`/`ALTER` (not `IF NOT EXISTS`), and append them to
   `MIGRATIONS` in `sqlite.rs` and `postgres.rs` with `satisfied_by: &[]`.
   The statements run in one transaction.
3. Never edit an earlier migration, including 0001.
4. Fixture: in `crates/leep-meta/src/migrate_tests.rs` add a `Stage` for the
   schema you are upgrading from (the previous latest) and populate it with
   SQL that version used (`Data`/`load`), then check the data reads back in
   `verify`, and that the new column or table works. The template at the top
   of the file lists the steps.
5. Run `LEEP_TEST_POSTGRES_URL=... cargo test -p leep-meta` (both backends)
   and `cargo test -p leep-e2e --test migrate`.
6. Backups (ADR 0026): `crates/leep-meta/src/snapshot.rs` reads and writes the
   current layout of every table. A migration that adds or changes a column or
   table must teach its `Source` and `Sink` (SQLite and Postgres), and
   `snapshot_tests.rs` must populate and compare it. Old backups keep
   restoring: they are migrated on a scratch copy like any old database, so add
   a restore of the previous schema's snapshot to the test. A change to the
   backup directory or `manifest.json` needs a new manifest `format` unless it
   only adds a field.
7. Update docs/FORMATS.md (schema section), the CHANGELOG and, if operators
   must do something, docs/OPERATIONS.md "Upgrading".
