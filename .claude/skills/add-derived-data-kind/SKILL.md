---
name: add-derived-data-kind
description: Add a new kind of derived data to Leep (an index or precomputed view such as path history, code search, or file stats) in leep-derive. Use when a query needs information computed from history rather than stored in changesets.
---

# Add a derived data kind

Derived data is a pure function of a changeset and its parents' values,
stored in `MetaStore` under `(kind, key)`. See ARCHITECTURE.md §5.

## Steps

1. **Check that it really is derived.** It must be computable from the
   changeset, its parents' values of the same kind, and other derived kinds.
   If it needs data not already in history (for example, review approvals),
   it belongs in `extra` or a separate service, not here.
2. **Choose the key:**
   - per changeset: key = changeset id, derived recursively from parents (like
     `generation` and `root_tree`);
   - per (changeset, path) or another input: key =
     `Hash::of_object("<kind>", <canonical input bytes>)`, derived on demand
     (like `blame`).
3. **Name it with a version:** `name-v1`. If its encoding changes later, rename
   the kind; never reinterpret old bytes.
4. **Implement** the kind trait in `crates/leep-derive/src/derived/`:
   - Walk history **iteratively** and derive oldest-first. No recursion on
     history.
   - The value must be deterministic: no timestamps, no ordering from a
     `HashMap`.
   - `derive` runs several changesets of a range at once (ADR 0050): read
     only the changeset, its parents' values and rows of ancestors (through
     `derive`), never state another changeset's derivation writes, and keep
     shared caches behind locks. A staged kind's `dropped` must undo any
     "already written" bookkeeping for rows an error threw away.
   - Decide whether it is derived eagerly in `create_changeset` (only if almost
     every request needs it) or lazily on first read (the default).
5. **Expose it** through a `Repo` method. If clients need it, add an endpoint
   with the `add-api-endpoint` skill.
6. **Test with `leep_core::memory` stores:**
   - correctness on linear history, merges and deletes;
   - deriving twice gives identical bytes, also with
     `Repo::with_derive_jobs(1)` against several jobs;
   - a deep chain (≥ 10k changesets) derives without stack overflow.
7. Add a row to the table in ARCHITECTURE.md §5 and a line to FORMATS.md.
