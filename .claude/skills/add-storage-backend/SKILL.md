---
name: add-storage-backend
description: Add a new BlobStore or MetaStore backend to Leep (e.g. GCS, Azure Blob, CockroachDB, FoundationDB, Redis cache). Use when Leep needs to run on a new database or object store.
---

# Add a storage backend

Backends implement `leep_core::BlobStore` (in `leep-store`) or
`leep_core::MetaStore` (in `leep-meta`). See ADR 0006.

## Steps

1. Put it in the right crate, behind a Cargo feature if it pulls in heavy
   dependencies.
2. Implement the trait:
   - Map every backend error to `Error::Storage`.
   - "Not found" is `Ok(None)`, not an error.
   - Writes are idempotent. A blob or changeset that already exists is
     success.
   - Garbage collection (ADR 0008) needs three more methods on every backend:
     - `BlobStore::list_page(after, limit)`: at most `limit` blobs with hash
       strictly greater than `after`, in ascending hash order, with `size` and
       `modified` (unix seconds, or `None` if unknown; GC keeps those). Skip
       temp files and foreign keys. Do not return an unordered listing.
     - `BlobStore::delete(hash)`: idempotent, absent is success.
     - A repeated `put` of an existing blob must refresh `modified`, or the
       grace window will not protect re-uploaded orphans.
     - `MetaStore::list_changesets(after, limit)`: ids strictly greater than
       `after` in ascending byte order. Never skip or repeat across pages.
   - **MetaStore `cas_ref` must be one atomic conditional operation.** That
     means a conditional update/insert/delete, or a transaction at
     serializable isolation. Never read then write.
   - Values returned from `get_changeset` must decode, and their id must
     match the key. Otherwise return `Error::Storage`.
3. Register a URL scheme in `open_blob_store` / `open_meta_store`. A
   MetaStore with a schema must version it like the SQL backends (ADR 0023):
   an ordered, append-only migration list driven by `leep_meta::migrate`
   (`Session` over one transaction, an exclusive lock so concurrent starts
   apply each step once, a `schema_migrations` record with checksums, refusal
   of a newer or edited history), `migrate_meta_store` and `SchemaMode`
   support, and `schema_version()`. Add `migrate_tests.rs`-style tests: a
   fixture per earlier schema, newer-version and checksum refusal, concurrent
   migrators, and a failing migration that rolls back fully.
4. Tests:
   - call `leep_core::memory::check_blob_store_contract` /
     `check_meta_store_contract`;
   - for MetaStore, a race: 32 tasks CAS from the same expected value, and
     exactly one wins;
   - if the backend needs a service, gate the tests on an env var
     (`LEEP_TEST_<NAME>_URL`), skip with a printed note when it's unset, and
     add the service to `.github/workflows/rust.yml`.
5. Fault tolerance (ADR 0021): run the contract through a quiet
   `leep_testkit::FaultyBlobStore` / `FaultyMetaStore` (it must still pass),
   and write down in `docs/ARCHITECTURE.md` §9 what the backend guarantees
   when a call is killed or cancelled mid-flight and what an error after the
   call applied means for it. For a MetaStore, abandon `cas_ref_logged` at
   random suspension points (`leep_testkit::cancel_after_polls`) and check
   the ref log still replays to the refs.
6. Add the schema or key layout to FORMATS.md and a row to the scaling table in
   ARCHITECTURE.md §11 if relevant.
