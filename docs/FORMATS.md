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

## Integrity on read

Content-addressed data is verified when read: `Repo::read_tree` and
`read_history_node` hash the stored bytes and compare them with the key, and
the SQL backends decode a changeset and compare its id with its row key. A
mismatch is `Error::Storage`. File blobs and the other derived kinds are not
verified (ADR 0021 lists which).

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
5. SQL schemas migrate forward only, by numbered migrations in
   `leep-meta/migrations/` (ADR 0023). A schema change is a new migration for
   both backends plus a test fixture of the previous schema; an applied
   migration is never edited (its checksum is checked at startup).

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

## Rename detection (no persisted change)

ADR 0016. Renames are computed when displaying; no stored data, derived kind or
hash changed. The API additions are all optional, and absent fields mean the old
behavior:

- `GET /v1/diff` query: `renames=true|false` (default false) and
  `rename_threshold=0..100` (default 50; anything else is 400, also when
  `renames` is false).
- `DiffResponse`: `renames: [RenameEntry]` and `renames_truncated: bool`, both
  `#[serde(default)]` and omitted from the JSON when empty or false. `entries` is
  unchanged: a rename is still a delete entry and an add entry.
  `RenameEntry { from, to, similarity: 0..100, exact }`, sorted by `to`; a path
  is in at most one rename; `exact` means same content hash and same kind.
- `GET /v1/log/{id}` query: `follow=true|false` (default false). It needs a file
  `path` (400 for none, or a directory).
- `ChangesetInfo`: `path_at` (the followed file's name in that changeset) and
  `renamed` (a `RenameEntry`, on the entry where the file got its name). Both are
  `#[serde(default)]`, omitted when absent, and set only by a follow log.

The similarity score is part of what clients see, not of what is stored: lines
keep their `\n`, compare by FNV-1a 64, and weigh by byte length;
`score = floor(200 * common_bytes / (bytes_a + bytes_b))`. Changing it is a
behavior change recorded in the ADR, not a format change.

## Ref names (no change to stored data)

Every ref write is checked by `leep_core::refname::validate`; reads and
deletes of names that already exist are never refused for their spelling. A
name is valid when it:

- is 1 to 255 bytes of printable ASCII, with no space and none of
  `~ ^ : ? * [ \`;
- has no `..`, no `@{`, no leading, trailing or doubled `/`;
- has no `/`-separated component that starts with `.` or `-` or ends with
  `.lock`;
- is not `@` (the working copy's base) or 64 hex digits (a changeset id), so
  resolving a revision is never ambiguous.

Namespaces: a branch is any valid name outside `tags/`. A tag named `v1` is the
ref `tags/v1`. Tags are write-once: the server accepts creating one
(`expected: null`) or deleting one (`new: null`) and refuses to move it to
another changeset or to `land` onto it, both with 400. The server also refuses
to delete a ref that a land policy (signatures, review) protects. A revision
is looked up as a ref, then a full changeset id, then `tags/<rev>`, so a
branch or id always wins over a tag of the same short name.

## Merge conflicts report clean changes (additive API field, ADR 0019)

`POST /v1/merge` answered a conflict with only the list of conflicts, so a
client could not rebuild the merged tree. `MergeResponse::Conflicts` now also
carries `clean`: for every path that merged cleanly and differs from
`parents[0]`, the `FileChange` relative to `parents[0]`. It is the same field
`RebaseResponse::Conflicts` and `LandResponse::Conflicts` have. It is optional
on the wire (`#[serde(default)]`), so old clients ignore it and a client reading
an older server's answer sees an empty map. No changeset, tree, hash or derived
encoding changed.

The client adds two optional fields to `.leep/state.json`, which is not an
API: `merge_in_progress` (`other`, `message`, `added`) and `pick_in_progress`
(`from`, `message`, `revert`, `added`). Absent means none. The changesets they
lead to use existing fields: a merge has `parents = [base, other]`, a
cherry-pick keeps the original's author, message and `extra` plus
`leep.rebased_from`, and a revert carries `leep.revert_of` (an `extra` key,
the id of the reverted changeset) and the message `Revert "<first line>"`,
a blank line and `This reverts <id>.`.

## SQL schema (leep-meta)

```
changesets(id BYTEA PRIMARY KEY, data BYTEA NOT NULL)          -- data = Changeset::encode()
refs(name TEXT PRIMARY KEY, target BYTEA NOT NULL)
derived(kind TEXT, key BYTEA, value BYTEA, PRIMARY KEY(kind, key))
ref_log(seq BIGSERIAL PRIMARY KEY,                             -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
        name TEXT NOT NULL, old_target BYTEA, new_target BYTEA, -- 32-byte ids; NULL = no ref
        time BIGINT NOT NULL, actor TEXT, operation TEXT NOT NULL, message TEXT)
ref_log_name: INDEX ON ref_log(name, seq)
```

`ref_log` (ADR 0018) is appended in the transaction of every ref create, move
or delete and never updated or deleted. `old_target` is NULL for a create,
`new_target` for a delete; `time` is unix seconds by the writing server;
`actor` is the token name or NULL; `operation` is a short label (`land`,
`cas`, `undo`, `import`, or a client's own, see "Ref log" below). Tables and
index come from migration 0001; databases made before the ref log existed
gain them on upgrade, and refs that moved before then simply have no rows.

### Schema versioning (ADR 0023)

```
schema_migrations(version BIGINT PRIMARY KEY,   -- SQLite: INTEGER
                  name TEXT NOT NULL,
                  applied_at BIGINT NOT NULL,   -- unix seconds
                  checksum TEXT NOT NULL)       -- hex BLAKE3 of the migration SQL, CRLF as LF
```

Migrations live in `crates/leep-meta/migrations/{sqlite,postgres}/NNNN_name.sql`
and are compiled into the binary. Rules:

- Versions are 1, 2, 3... with no gaps, the same in both backends. A database
  at version N has every migration up to N and no other.
- **An applied migration is immutable.** Never edit, reorder, renumber or
  delete a migration that has been merged: its checksum is recorded in every
  database that ran it, and a mismatch stops the server. Fix mistakes with a
  new migration.
- Every schema change is a new numbered migration in both backends, plus a
  fixture of the schema it upgrades from in `migrate_tests.rs`.
- Only 0001 is idempotent (`IF NOT EXISTS`); it must be, to finish databases
  that predate the ref log. Later migrations run exactly once.
- A migration runs in a transaction together with all the others pending in
  that run, so it must not contain statements that cannot (`CREATE INDEX
  CONCURRENTLY`, `VACUUM`).
- A server refuses a database whose newest version is above its own. The
  newest version is exported as `leep_schema_version`.

## Ref log (new table and API fields, no change to stored objects)

ADR 0018 adds the `ref_log` table above, `MetaStore::cas_ref_logged` and
`ref_log`, `GET /v1/reflog` and `GET /v1/reflog/{name}`, and the optional
`operation` and `message` fields of `POST /v1/refs/{name}`. No changeset, tree,
hash or derived encoding changed. Rules:

- `seq` increases with every entry in the store; entries of one ref replay, oldest
  first, to its current value (each `old` is the previous `new`).
- A swap that loses, and a swap with `expected == new`, write no entry.
- `operation` from a client is 1 to 32 bytes of `a-z 0-9 - _ .`, defaults to
  `cas`, and may not be `land` or `import`; `message` is up to 256 bytes with
  no control characters. `actor` is never taken from a request.
- Entries are never removed. They name changesets, which are never deleted, so
  blob garbage collection is unaffected.

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

## Request limits (additive, ADR 0020)

Nothing persisted changed. On the wire, any endpoint may now answer `429` with
a `Retry-After` header (whole seconds) and the usual `{"error": ...}` body when
a token is over its rate or concurrency limit, and `413` with the same JSON
body when a request body exceeds a token's `max_body` (the global limit's 413
is unchanged). Clients should treat 429 as retryable after the delay for reads.
The tokens file gains optional entry fields `rate` (requests per second,
fractions allowed), `burst`, `concurrency` and `max_body` (bytes). Each is a
number, except `rate`, `concurrency` and `max_body` also accept `"unlimited"`
to override a server-wide default; `burst` is a whole number of at least 1 and
needs a rate. A file without them parses as before, and unknown fields are
still refused. New flags and variables: `--rate-limit`/`LEEP_RATE_LIMIT`,
`--rate-burst`/`LEEP_RATE_BURST`,
`--max-concurrent-per-token`/`LEEP_MAX_CONCURRENT_PER_TOKEN`,
`--read-max-body-bytes`/`LEEP_READ_MAX_BODY_BYTES`.
