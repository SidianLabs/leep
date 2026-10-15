# 0006. Two storage traits with pluggable backends
Status: Accepted
Date: 2026-10-15

## Context
Leep must run on a laptop (no services), for a team (Postgres + S3) and at
large scale (distributed SQL).

## Decision
All persistence goes through `BlobStore` (immutable bytes) and `MetaStore`
(changesets, refs with CAS, derived data). Backends are selected by URL.
Every backend must pass the shared conformance tests in `leep_core::memory`.

## Consequences
Scaling up means swapping a backend. The traits are deliberately small, and
any new capability added to them must be implementable on every backend.
