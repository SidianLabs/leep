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
| Change the SQL schema | Add a forward-only, idempotent migration in leep-meta. |

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
