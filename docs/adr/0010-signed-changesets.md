# 0010. Signed changesets
Status: Accepted
Date: 2026-10-15

## Context
Agent provenance (`agent.*` in `extra`) says who claims to have made a change,
but anyone with a write token can claim anything. v0.3 needs a way to prove
that a trusted author key vouches for a changeset, and a way for a protected
ref to refuse anything unvouched.

A changeset's id is `BLAKE3` of its encoding including `extra`. A signature
over the id therefore cannot be stored inside the changeset: adding it would
change the id it signs. Changing `Changeset` itself is off the table, since it
would rewrite every hash (FORMATS.md).

## Decision
**Signatures are attestations stored beside the changeset, keyed by its id.**
`Changeset` and every other persisted type is unchanged.

- Algorithm: Ed25519 (`ed25519-dalek` 2). The signed message is
  `b"leep-changeset-signature-v1\0" ‖ changeset_id` (28 + 32 bytes). The domain
  prefix stops a signature from being valid in any other protocol that signs
  32 bytes. The server verifies strictly (`verify_strict`), which rejects
  malleable signatures and small-order keys.
- Key id: first 16 hex characters of `BLAKE3(public key)`. It is a label for
  logs and storage keys, never used to decide trust; verification always uses
  the full key. A text public key is `ed25519:<64 hex>`.
- `leep_core::signature` holds the shared pieces (message construction, key id,
  the stored `Signature` type, text parsing) so server and client cannot
  disagree about the signed bytes. The Ed25519 operations live in `leep-server`
  and `leep-cli`; `leep-core` and `leep-derive` do no cryptography, and the CLI
  still depends only on `leep-core` among Leep crates.
- Storage: derived-data kind `signature-v1`. One row per (changeset, key): key
  = `Hash::of_object("signature", changeset_id ‖ key_id ascii)`, value =
  postcard `Signature { key_id, public_key, sig }`. Derived data is
  last-writer-wins, so appending to one row per changeset would be a
  read-then-write race between two signers. One row per key makes adding a
  signature a single idempotent write.
- Listing: the server knows its trusted keys, so it lists a changeset's
  signatures with one lookup per trusted key and re-verifies each on read. No
  per-changeset index is needed. The cost is O(trusted keys) per query.
- Trusted keys: `--signing-keys-file` / `LEEP_SIGNING_KEYS_FILE`, JSON
  `{"keys":[{"name","public_key"}]}`, read once at startup like the tokens file
  (ADR 0007). Unknown fields, malformed or invalid keys, small-order keys,
  duplicate names or keys, and an empty list fail startup. Changing keys means
  restarting. A signature by an unlisted key is rejected at submission (400),
  so only trusted signatures are stored.
- API (additive, `/v1`): `POST /v1/changesets/{id}/signatures`
  `{public_key, signature}` returns `{key_id, name}` (400 bad signature or
  untrusted key, 404 unknown changeset); `GET` returns
  `{signatures: [{key_id, name, public_key}], verified, via}`. POST is
  mutating, so the existing auth middleware already requires a write token;
  GET is a read.
- Policy: `--require-signatures REF` (repeatable; `LEEP_REQUIRE_SIGNATURES`
  comma-separated; needs the keys file). `land` onto a listed ref fails with
  400 `changeset X is not signed by a trusted key` unless every changeset the
  land would add to the ref has a valid signature from a trusted key. That set
  is every ancestor of the submitted changeset that is not already in the
  ref's history, so it covers each entry of an unlanded stack and the side
  branches of a merge. It is enforced in `leep-derive::land` through an
  optional `LandPolicy` hook on `Repo` (`with_land_policy`), keeping
  `leep-derive` free of key handling; the server supplies the policy.
  `POST /v1/refs/{name}` (raw CAS) goes through the same check
  (`Repo::cas_ref_checked`) against what the ref would gain over `expected`,
  so it cannot be used to skip `land`. Rewinds and deletes add nothing and are
  not checked. `leep-git` import writes to the stores directly and is not
  gated; it is an operator tool. A policy decision is
  cached within one land call, since signatures are never removed.
- Rebased copies. When a land replays a stack onto a moved head, the server
  creates copies with new ids, so no signature covers them. The policy checks
  the original changesets the client submitted, before the replay. The server
  records `rebased-origin-v1` (copy id to original id) whenever it rebases, and
  `GET .../signatures` on a copy with no signature of its own reports the
  original's signatures with `via: <original id>`. This is informational:
  the policy never accepts a changeset because of `via`. The link is a derived
  row only the server writes, not the `leep.rebased_from` extra key, because a
  client can put any value in `extra`; a test confirms a forged
  `leep.rebased_from` neither lands nor shows a `via`.
- CLI: `leep keygen --out PATH` writes `leep-ed25519-secret:<hex seed>` with
  mode 0600 (never overwriting) and prints the keys-file entry. `leep commit
  --sign KEYFILE` (or `LEEP_SIGNING_KEY`) loads the key first, so a bad key
  fails before any upload, then signs the new id and posts it. If posting fails
  the working copy is left unchanged and the commit can be retried. Loading
  refuses a key readable by group or others. `leep show` prints `Signed-by:`
  lines. `leep land` adds a hint to the policy error.

## Consequences
- History and ids are untouched; existing repositories and clients keep
  working. A server without a keys file behaves as before.
- A signature vouches for a changeset id, which commits to the full content
  (parents, changes, extra), so it covers everything the id does.
- Landing onto a moved head still works for signed work, but the commits that
  end up on the ref are server-made copies. Verifying "signed by X" offline
  from the repository alone is not possible; the answer comes from the server.
- Signatures cannot be revoked, and removing a key from the file stops its
  signatures counting immediately, including old ones (they are checked
  against the current set at read and land time). That doubles as crude
  revocation.
- Listing cost grows with the number of trusted keys. Hundreds are fine;
  thousands would want an index.
- Key ids are 64 bits. A collision between two listed keys fails startup.

## Future work
- Per-path policies (for example only owners' keys may sign `infra/`),
  alongside the review service and directory owners.
- Key rotation with validity windows and key-signed-by-key delegation, so
  old signatures survive a rotation without keeping retired keys listed.
- A transparency log of signatures, so a server cannot silently drop or
  invent attestations.
- SIGHUP reload of the keys file (as for tokens).
- Gate `leep-git` import by the same policy.
