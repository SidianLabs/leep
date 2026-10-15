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
   - **MetaStore `cas_ref` must be one atomic conditional operation.** That
     means a conditional update/insert/delete, or a transaction at
     serializable isolation. Never read then write.
   - Values returned from `get_changeset` must decode, and their id must
     match the key. Otherwise return `Error::Storage`.
3. Register a URL scheme in `open_blob_store` / `open_meta_store`.
4. Tests:
   - call `leep_core::memory::check_blob_store_contract` /
     `check_meta_store_contract`;
   - for MetaStore, a race: 32 tasks CAS from the same expected value, and
     exactly one wins;
   - if the backend needs a service, gate the tests on an env var
     (`LEEP_TEST_<NAME>_URL`), skip with a printed note when it's unset, and
     add the service to `.github/workflows/rust.yml`.
5. Add the schema or key layout to FORMATS.md and a row to the scaling table in
   ARCHITECTURE.md §11 if relevant.
