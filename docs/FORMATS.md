# Leep formats and compatibility

Anything listed here is persisted or sent over the wire. Changing it
carelessly corrupts history or breaks deployed clients, so every change goes
through the [`change-persistent-format`](../.claude/skills/change-persistent-format/SKILL.md)
skill.

## Hashing

- Algorithm: BLAKE3, 32 bytes. Hex is lowercase, 64 characters.
- Blob: `BLAKE3(raw file bytes)`, with no header.
- Object: `BLAKE3("leep:" ‖ kind ‖ 0x00 ‖ postcard(object))`, where `kind`
  carries a version suffix (`changeset-v1`, `tree-v1`). This domain separation
  means a blob can never collide with an object.

## Binary encoding

- `postcard` (serde), using `BTreeMap` for every map, so encoding is
  deterministic. **Field order and enum variant order are part of the
  format.** Reordering fields or variants changes every hash.
- Enums are externally tagged. postcard cannot decode internally tagged
  enums, so `#[serde(tag = ...)]` is banned on persisted types.
- `Hash` serializes as 32 raw bytes in binary formats and as hex in JSON.

## Persisted types (v1)

| Type | Kind tag | Where |
|---|---|---|
| `Changeset` | `changeset-v1` | `MetaStore.changesets.data` |
| `TreeNode` | `tree-v1` | `BlobStore`, keyed by `TreeNode::hash()` |
| derived `root_tree` | — | `MetaStore.derived`, value = 32 raw bytes |
| derived `generation` | — | `MetaStore.derived`, value = u64 (owned by leep-derive) |
| derived `signature-v1` | — | `MetaStore.derived`, key = `Hash::of_object("signature", changeset_id ‖ key_id)`, value = postcard `Signature { key_id: String, public_key: [u8; 32], sig: [u8; 64] }` (`leep_core::signature`) |
| derived `review-v1` | — | `MetaStore.derived`, key = `Hash::of_object("review", changeset_id ‖ reviewer name utf-8)`, value = postcard `Review { reviewer: String, verdict: Verdict, comment: String, timestamp: i64 }` (`leep_core::review`); `Verdict` is `Approve` (variant 0) or `Reject` (variant 1) |
| derived `rebased-origin-v1` | — | `MetaStore.derived`, key = rebased copy id, value = 32 raw bytes (id it was rebased from); written by `Repo::rebase` |
| derived `blob-claim-v1` | — | `MetaStore.derived`, key = blob hash, value = unix seconds as 8-byte big-endian `i64` |
| derived `blob-condemned-v1` | — | `MetaStore.derived`, key = blob hash, value = condemnation time, unix seconds as 8-byte big-endian `i64`, from the GC host's clock |
| derived `blob-rescue-v1` | — | `MetaStore.derived`, key = blob hash, value = the condemnation time the writer observed, 8-byte big-endian `i64` |
| derived `blob-doom-v1` | — | `MetaStore.derived`, key = blob hash, value = condemnation time (8-byte big-endian `i64`) then one state byte: 0 deleting, 1 abandoned, 2 deleted |
| derived `blob-trigrams-v1` | — | `MetaStore.derived`, key = blob hash, value = see "Code search index" below |
| derived `tree-trigrams-v1` | — | `MetaStore.derived`, key = tree node hash (`TreeNode::hash()`), value = see "Code search index" below |
| derived `history-tree-v1` | — | `MetaStore.derived`, key = changeset id, value = 32 raw bytes (hash of that changeset's root `HistoryNode`) |
| `HistoryNode` | `history-node-v1` | `MetaStore.derived` (kind `history-node-v1`), key = `Hash::of_object("history-node-v1", postcard(node))`, value = the postcard bytes |
| other derived kinds | — | owned and versioned by `leep-derive` |

## Versioning rules

1. **Never change v1 types in place.** That includes field order, variant
   order, added fields and serde attributes. Make a new version (`-v2`) with
   its own kind tag, and keep the v1 decoder forever.
2. Derived data is disposable. To change a derived kind's encoding, rename the
   kind (`blame` → `blame-v2`). Old rows are ignored and can be garbage
   collected later.
3. `extra` is how history gets new metadata without changing the format. Use
   namespaced keys (`agent.*`, `leep.*`, `review.*`).
4. HTTP API: `/v1` endpoints only grow, by adding endpoints or optional
   fields with `#[serde(default)]`. Removing or renaming anything requires
   `/v2`, served alongside v1.
5. SQL schemas migrate forward only. Migrations are idempotent and live in
   `leep-meta`.

## Signatures (no change to `Changeset`)

ADR 0010. A signature cannot be stored in the changeset it signs, so
`Changeset` and the changeset id are unchanged. The signed message is
`b"leep-changeset-signature-v1\0" ‖ changeset_id` (28 + 32 bytes, the id as
raw bytes), Ed25519. The key id is the first 16 hex characters of
`BLAKE3(public key bytes)`; a public key is written `ed25519:<64 hex>` and a
signature as 128 hex digits in the API. Two derived kinds hold the data (see
the table above); the `signature-v1` key hashes the changeset id bytes
followed by the ASCII key id, with object kind `signature`. Changing the
message prefix, the key id derivation or the `Signature` field order requires
a `-v2` kind, because stored rows would stop verifying. The API additions
(`/v1/changesets/{id}/signatures`) only add endpoints and types.

The server's signing keys file is `{"keys":[{"name","public_key"}]}`; unknown
fields are rejected.

## Reviews and owners (no change to `Changeset`)

ADR 0011. A review is an attestation stored beside the changeset it is about,
so `Changeset` and the changeset id are unchanged. The `review-v1` row is
overwritten by the same reviewer, so a verdict can change; the reviewer in the
value is the token name of the authenticated caller. Changing the `Review`
field order, the `Verdict` variant order or the key derivation requires a
`-v2` kind. JSON uses `"approve"` / `"reject"`. The API additions
(`/v1/changesets/{id}/reviews`, `/v1/changesets/{id}/review-status`) only add
endpoints and types.

**`OWNERS` file** (any directory of the tree; a regular or executable file,
symlinks are ignored; at most 1 MiB): one owner name per line, where a name is
a token name from the server's tokens file; `#` starts a comment (so names
cannot contain `#`); blank lines are ignored; a line that is exactly
`noparent` stops inheritance from ancestor directories. Names keep inner
spaces and are trimmed at both ends. The owners of a path are the union of
the files in its directory and every ancestor up to the first `noparent`
(that file's own names included) or the root. Invalid UTF-8 is read lossily.
Owners are always read from the head of the ref being landed on. No file
covering a path (or no names in the covering files) means no owners: any one
approval from any principal satisfies the path.

**Review policy file** (`--review-policy-file`, JSON, unknown fields are
rejected, every field optional):

```json
{ "refs": ["main"],
  "auto_approve": ["comment_only", "whitespace_only", "docs_only"],
  "docs_globs": ["**/*.md", "docs/**"] }
```
`refs` are refs that require review (added to `--require-review`);
`auto_approve` names the rules that approve a path without a human;
`docs_globs` (required by `docs_only`, rejected without it) are `globset`
patterns with `*` limited to one path level. The comment languages are
selected by file extension (case-insensitive).

## Code search index (new derived kinds, no other format change)

ADR 0014. Two kinds, both pure functions of content-addressed input and never
deleted. Golden bytes are asserted in `leep-derive` (`blob_row_golden_bytes`,
`bloom_positions_are_frozen`); any change to an encoding, a constant below or the
definition of "text" is a new kind name (`-v2`).

A *trigram* is three consecutive bytes `b0 b1 b2`, coded `b0<<16 | b1<<8 | b2`
(24 bits). A blob is *indexed* iff it is at most 1 048 576 bytes, contains no
NUL byte and is valid UTF-8. The empty blob is indexed with an empty set. Symlink
entries are never indexed or searched.

**`blob-trigrams-v1`** value, by first byte:
- `0x00 0x01`: not indexed, binary (NUL or invalid UTF-8);
- `0x00 0x02`: not indexed, over the size cap (recorded without reading the blob);
- `0x01` then the sorted, deduplicated trigram codes of the blob as LEB128 varints
  of the deltas, the first relative to zero. Delta 0 after the first value, a
  non-minimal varint, a value of 2^24 or more, and a trailing partial varint are
  invalid.

**`tree-trigrams-v1`** value: a bloom filter of 2^19 bits with 3 hash functions
over the trigram codes of all indexed blobs below the directory (recursively;
filters of subdirectories are OR-ed in). Bit positions of a code `c`: let `z` be
splitmix64 of `c` (`z = c + 0x9E3779B97F4A7C15; z = (z ^ z>>30) * 0xBF58476D1CE4E5B9;
z = (z ^ z>>27) * 0x94D049BB133111EB; z ^= z>>31`, wrapping 64-bit),
`h1 = z mod 2^32`, `h2 = (z >> 32) | 1`, and position `i` (0, 1, 2) is
`(h1 + i*h2 mod 2^32) mod 2^19`. The row is either
- `0x01` then exactly 65 536 bytes, bit `p` at byte `p/8`, bit `p%8`
  (least significant first); or
- `0x02` then the sorted set bit positions as delta varints as above (the first
  relative to zero, each below 2^19), used when it is shorter than the dense form.
The encoder is canonical (sparse iff strictly shorter), but readers accept either
form for any filter. An empty directory is `0x02`.

The rows are lazily derived. A search over `path` derives only the subtree.

The `/v1/search/{rev}` endpoint only adds an endpoint and types (`SearchResponse`,
`SearchMatch`, `SearchRange`, `SearchStats`), all with `#[serde(default)]` fields.
Ranges are byte offsets into the returned (possibly cut) `line`.
## Path-history index (new derived kinds, no change to `Changeset`)

ADR 0013. `leep_derive::derived::HistoryNode { entries: BTreeMap<String,
HistoryEntry> }`, keyed by single path components, postcard-encoded.
`HistoryEntry` is an externally tagged enum, **variant order is format**:
`Leaf { changed: Vec<ChangesetId> }` (variant 0) and
`Tree { changed: Vec<ChangesetId>, node: Hash }` (variant 1). `changed` is a
sorted (by id bytes), deduplicated set of changeset ids, serialized as a
varint length and 32 raw bytes per id; `node` is 32 raw bytes, the hash of the
children's `HistoryNode`. The empty node is the single byte `0` (zero entries)
and is never stored: readers treat its hash as present. Example, a root node
with `a` (Leaf, one id) and `d` (Tree, two ids):

```
02 | 01 61 00 01 <32 bytes id1> | 01 64 01 02 <32 bytes id1> <32 bytes id2> <32 bytes node>
```

The golden bytes are asserted in `leep-derive` (`node_encoding_is_stable`).
Node rows are content-addressed and idempotent. The meaning of `changed` (the
maximal changesets whose entry at the path differs from their first parent's;
merges take the union of parents' sets for paths they leave as in `parents[0]`)
is part of the format: changing it, the field or variant order, or the key
derivation requires `history-tree-v2` and `history-node-v2`. Entries of deleted
paths are kept. The API addition (`GET /v1/last-change/{id}`) adds one endpoint
and one response type; `/v1/log` is unchanged.

## SQL schema (leep-meta)

```
changesets(id BYTEA PRIMARY KEY, data BYTEA NOT NULL)          -- data = Changeset::encode()
refs(name TEXT PRIMARY KEY, target BYTEA NOT NULL)
derived(kind TEXT, key BYTEA, value BYTEA, PRIMARY KEY(kind, key))
```

## Blob storage layout (leep-store)

- Filesystem: `<root>/<hex[0..2]>/<hex[2..]>`
- Object store: `blobs/<hex[0..2]>/<hex[2..]>`

## Garbage collection (no format change)

ADR 0008 adds `BlobStore::list_page` and `delete` and `MetaStore::list_changesets`.
These are trait additions only: no encoding, hash, SQL schema or blob layout
changed. Listings walk the existing layouts: filesystem shard directories and
object-store `blobs/<hex[0..2]>/` prefixes in hash order, and
`SELECT id FROM changesets ORDER BY id` (byte order) for metadata. Blob
modification time is the file mtime or object `LastModified`; a repeated `put`
refreshes it.

## Fenced garbage collection (new derived kinds, no other format change)

ADR 0012 adds the three GC kinds in the table above (`blob-claim-v1` already
existed under ADR 0008 with the same encoding). All use the blob hash as the
key and fixed-width big-endian integers, so rows are readable without postcard.
A blob is condemned iff `condemned > rescue` (a missing rescue row counts as
less than any condemnation); rows are never deleted, and a new condemnation of
the same blob is written strictly later than the previous one. Golden bytes are
asserted in `leep-derive` (`row_encodings_are_stable`); changing an encoding
means a new kind name (`-v2`).

`/v1` wire shapes are unchanged: a condemned blob is reported by
`POST /v1/blobs/missing` as missing, and `POST /v1/changesets` answers 400
"missing blob" for it, exactly as for an absent blob. `PUT /v1/blobs/{hash}`
may answer 408 (request timeout) or 409 (blob being collected, retry). New
server flag `--request-timeout` / `LEEP_REQUEST_TIMEOUT`.
