# 0041. Blob and object address separation
Status: Accepted (stopgap); the format change is deferred
Date: 2026-10-18

## Context
A blob's address is `Hash::of_blob(bytes)`, bare BLAKE3. A tree node's is
`Hash::of_object("tree-v1", x) = BLAKE3("leep:tree-v1\0" ‖ x)`, and the node
is stored as the bytes `x` in the same `BlobStore`. So the blob whose bytes
are `"leep:tree-v1\0" ‖ x` has the node's address. FORMATS.md and the
comment on `Hash::of_object` claimed the domain prefix made this impossible;
it only separates object kinds from each other.

Anyone with a write token can compute the node for a tree they expect to
exist (an empty directory, a vendored directory, the next state of a file
they are about to edit) and `PUT /v1/blobs/<that hash>` with the preimage
first. The hash check in `put_blob` passes. `put_with_hash` is
touch-if-exists, so when the real node is written later the store keeps the
preimage, and every read of that tree decodes `"leep:tree-v1\0" ‖ x` and
fails as corrupt, for every changeset that shares the directory, until an
operator repairs the store. Changesets live in the `MetaStore`, so only trees
(and anything else a future change writes to the `BlobStore` with
`put_with_hash`) are exposed.

## Decision
**Now (stopgap):** no uploaded or server-made blob may be an object preimage.
`leep_core::hash::is_object_preimage(bytes)` is true when the bytes start
with `leep:` and contain a NUL after it; every `Hash::of_object` preimage has
that shape, whatever its kind. Such bytes are refused with
`Error::Invalid` (400):

- `Repo::put_blob` and `Repo::put_blobs`, so `PUT /v1/blobs/{hash}`, the git
  import and every other server-side blob write refuse them.
- A three-way text merge whose result would be such bytes is a conflict
  instead.
- `leep-git import` checks each new file before writing anything for its
  commit and stops with `commit <oid>: <path> starts with "leep:" ...`.
  Commits before it stay imported, and a later run resumes from there, so
  nothing is half-written or corrupt. `leep-git export` needs no change:
  such a blob cannot be created, and one stored before this change exports
  byte for byte.

A text file that merely begins with `leep:` (a YAML key, say) has no NUL and
is unaffected. The cost is that a binary file beginning with `leep:` and
containing a NUL cannot be stored at all until the format change below.

Trees written by the server itself (`put_rescuing` in `tree_ops`) are not
checked: their bytes are postcard, not a preimage, and their address is the
object hash.

**Later (format change, its own PR and ADR revision):** separate the two
address spaces properly. Two designs were weighed.

1. **Separate key namespaces in the store.** Keep every hash as it is and
   store trees under a different key (`trees/<hex>` beside `blobs/<hex>`, a
   type tag in the pack index, a separate column or table in other
   backends). Existing ids, changesets, tree hashes and signatures stay
   valid; nothing is rehashed. The cost is in every `BlobStore`
   implementation and contract (`put_tree`/`get_tree` or a namespace
   argument), packs (the index gains a kind per entry, a new pack version),
   backups, GC's mark and sweep (two key spaces), sync and repair tools,
   and a migration that moves existing tree nodes to the new keys, during
   which a reader must look in both places.
2. **A domain-separated blob hash.** Define blobs as
   `BLAKE3("leep:blob\0" ‖ bytes)` (or BLAKE3 keyed mode). The store stays
   one key space and needs no change. But every blob address changes, and
   so does every `FileChange::Modify.content`, every tree node that names a
   file, every root tree and every changeset id: all history is rewritten,
   signatures (which cover ids) no longer verify, git mappings and clients'
   caches go stale, and `/v1/blobs/{hash}` changes meaning. It needs a
   `changeset-v2`/`tree-v2` with mixed-version reads forever (see the
   `change-persistent-format` skill).

Option 1 is preferred: it fixes the class of bug at the store, where it
lives, without touching any id a user or signature has seen. Option 2 is
only attractive if a history rewrite happens for another reason.

## Consequences
- The attack above returns 400; existing stores are not changed. A store
  that was already poisoned still reads that tree as corrupt; the repair
  walk (ADR 0025) reports it as `corrupt_tree`, and the node can be
  rewritten after deleting the bad copy.
- `is_object_preimage` must stay in front of every path that writes
  client-supplied or derived file content to the `BlobStore`. Code that adds
  such a path without `Repo::put_blob`/`put_blobs` reopens the hole.
- The stopgap is removed when the namespaces are separated; the refused
  files then become storable.
