---
name: add-api-endpoint
description: Add or extend a Leep HTTP API endpoint end to end — request/response types in leep-core::api, the handler in leep-server, and the client call in leep-cli. Use whenever clients need a new operation or a new field.
---

# Add an API endpoint

The API contract lives in `crates/leep-core/src/api.rs`, shared by the server
and the CLI. `/v1` only grows (FORMATS.md, rule 4).

## Steps

1. **Types in `leep-core/src/api.rs`:**
   - Add the request and response structs. Add the row to the endpoint table
     in the module doc.
   - New fields on existing types must be `Option` or `#[serde(default)]`.
   - Tagged result enums use `#[serde(tag = "status", rename_all = "snake_case")]`.
     This is allowed because API types are JSON-only and never persisted.
   - Add a JSON shape test.
2. **Logic in `leep-derive::Repo`.** The handler must stay a thin wrapper,
   with no repository logic in `leep-server`.
3. **Handler in `leep-server`.** Map errors with the shared conversion
   (`Invalid` → 400, `NotFound` → 404, CAS loss → 409, else 500). Add a
   router test using in-memory stores.
4. **Client in `leep-cli`.** Add a method on the HTTP client and, if needed, a
   subcommand. The CLI must not depend on anything except `leep-core`.
5. **End to end.** Extend the end-to-end test if the endpoint is part of a
   user flow.
6. Run fmt, clippy and tests for the whole workspace.
