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
| derived `gc-mark-v1` | — | `MetaStore.derived`, key = changeset id, tree node hash or blob hash, value = the single byte `0x01` (anything else reads as absent). Written by GC marks in post-order, so a row exists only after the rows of everything below it (ADR 0033) |
| derived `blob-trigrams-v1` | — | `MetaStore.derived`, key = blob hash, value = see "Code search index" below |
| derived `tree-trigrams-v1` | — | `MetaStore.derived`, key = tree node hash (`TreeNode::hash()`), value = see "Code search index" below |
| derived `history-tree-v3` | — | `MetaStore.derived`, key = changeset id, value = 32 raw bytes (address of that changeset's root history node) |
| `StoredHistoryNode` | `history-node-v3` | `MetaStore.derived` (kind `history-node-v3`), key = `Hash::of_object("history-node-v3", postcard(row))`, value = the postcard bytes; a whole node, a chunk of one, a split node's chunk list (ADR 0027), or a patch of a node, chunk or list on an earlier whole version (ADR 0037) |
| retired `history-tree-v1`, `history-node-v1` | — | written by servers before ADR 0027; never read by current servers; removed with `repair drop --kind history-tree-v1` |
| retired `history-tree-v2`, `history-node-v2` | — | written by servers from ADR 0027 to ADR 0037; never read by current servers; removed with `repair drop --kind history-tree-v2` |
| other derived kinds | — | owned and versioned by `leep-derive` |

## Integrity on read

Content-addressed data is verified when read: `Repo::read_tree` and
`read_history_row` hash the stored bytes and compare them with the key, and
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

ADR 0013, stored in chunks since ADR 0027 and as patches since ADR 0037. The
logical node is
`leep_derive::derived::HistoryNode { entries: BTreeMap<String, HistoryEntry> }`,
keyed by single path components, postcard-encoded. `HistoryEntry` is an
externally tagged enum, **variant order is format**:
`Leaf { changed: Vec<ChangesetId> }` (variant 0) and
`Tree { changed: Vec<ChangesetId>, node: Hash }` (variant 1). `changed` is a
sorted (by id bytes), deduplicated set of changeset ids, serialized as a
varint length and 32 raw bytes per id; `node` is 32 raw bytes, the address of
the children's node. Example, a node with `a` (Leaf, one id) and `d` (Tree,
two ids):

```
02 | 01 61 00 01 <32 bytes id1> | 01 64 01 02 <32 bytes id1> <32 bytes id2> <32 bytes node>
```

A node is stored as one or more `history-node-v3` rows, each a postcard
`StoredHistoryNode`, an externally tagged enum (**variant order is format**).
`Flat` and `Split` are keyframes; `Patch` and `SplitPatch` are edits of one.

- `Flat(HistoryNode)` (variant 0): the whole node. Used for nodes of at most
  64 entries, and for every chunk. The example above is stored as
  `00 02 01 61 00 01 ...`. The empty node is `00 00`; it is never stored, and
  readers treat its address as present.
- `Split(Vec<ChunkRef>)` (variant 1), `ChunkRef { first: String, node: Hash }`:
  a node of more than 64 entries, cut into runs of entries in name order. Each
  chunk is a `Flat` or `Patch` row; `first` is its first name and `node` its
  address. A chunk ends after an entry whose name `n` has
  `Hash::of_object("history-chunk-boundary", n)` with its first byte's low 5
  bits zero (about one name in 32), or when it reaches 128 entries, or at the
  last entry. Boundaries depend only on names, so an edit rewrites one chunk
  and an insert at most two. Example, a directory of 1,000 names
  `f0000`..`f0999` (the first boundary is after `f0029`), 33 chunks:

```
01 21 | 05 66 30 30 30 30 <32 bytes chunk 0> | 05 66 30 30 33 30 <32 bytes chunk 1> | ...
```

- `Patch { base: Hash, over: HistoryNode }` (variant 2): the `Flat` row
  `base` with the entries of `over` added or replaced. Example, `a` set on the
  keyframe `<base>`:

```
02 <32 bytes base> | 01 | 01 61 00 01 <32 bytes id1>
```

- `SplitPatch { base: Hash, refs: BTreeMap<String, Option<SplitChange>> }`
  (variant 3), `SplitChange { node: Hash, next: Option<String> }`: the
  `Split` row `base` with the chunk whose first name is the key set to `node`
  (`Some`) or removed (`None`). `next` is the first name of the chunk after
  it in the patched list (`None` if last), so a lookup that falls in a set
  chunk's range needs no read of `base`; it must match the patched list.
  Example, chunk `a` set (next `q`) and chunk `m` removed:

```
03 <32 bytes base> | 02 | 01 61 01 <32 bytes chunk> 01 01 71 | 01 6d 00
```

A patch's `base` must be a keyframe of the patch's shape (`Flat` for `Patch`,
`Split` for `SplitPatch`), never another patch, so any row reads in at most
two rows. Rows are written by `put_history_node` from the new logical node and
the stored predecessor, the first parent's version of the same node. The
predecessor of a chunk is the predecessor node's chunk whose range holds the
new chunk's first name. For each whole node, chunk and chunk list that
changed, with `K` the predecessor's keyframe (the predecessor itself if it is
one), the row is:

- `K` itself, if the content equals `K`;
- otherwise a patch on `K` of the entries (or refs) that differ, if every
  name of `K` is still present and the patch's encoded length `p` satisfies
  `p² <= 128 × w`, where `w` is the encoded length of the whole row;
- otherwise the whole row, a new keyframe.

Unchanged chunks keep their address. With no predecessor, or one of the other
shape, every row is a keyframe.

A node's address (the `history-tree-v3` value, and `Tree.node` in its parent)
is the hash of its top row. Rows depend only on the content and the stored
predecessor, so derivation is deterministic, but the same logical node can be
stored under more than one address when reached from different predecessors.
Readers check that each chunk starts with its `first`, ends before the next
chunk's `first`, and that a patch fits its keyframe; a split node of `n`
entries needs about `n / 32` refs, one level deep.

The golden bytes, the boundary choice and the patch rule are asserted in
`leep-derive` (`node_encoding_is_stable`, `chunk_boundaries_are_stable`,
`patches_are_chosen_while_small_and_name_their_keyframe`). Rows are
content-addressed and idempotent. The meaning of `changed` (the maximal
changesets whose entry at the path differs from their first parent's; merges
take the union of parents' sets for paths they leave as in `parents[0]`) is
part of the format: changing it, the field or variant order, the split rule,
the patch rule or the key derivation requires `history-tree-v4` and
`history-node-v4`. Entries of deleted paths are kept. The API addition
(`GET /v1/last-change/{id}`) adds one endpoint and one response type;
`/v1/log` is unchanged.

`history-tree-v1` and `history-node-v1` (ADR 0013 as first written) stored each
node as one row of the bare `HistoryNode` postcard, keyed by
`Hash::of_object("history-node-v1", bytes)`. `history-tree-v2` and
`history-node-v2` (ADR 0027) stored `Flat` and `Split` rows only, keyed by
`Hash::of_object("history-node-v2", bytes)`. Current servers neither read nor
write either.

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

Migration 0002 (`analyze_thresholds`, ADR 0041) changes no table, column or
index. On Postgres it sets storage parameters so autovacuum re-analyzes the
large tables sooner: `derived (autovacuum_analyze_scale_factor = 0.01,
autovacuum_analyze_threshold = 10000)` and `changesets
(autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold =
10000)`. The SQLite migration of the same number does nothing; it keeps the
two backends' versions equal.

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

- Filesystem: loose objects at `<root>/<hex[0..2]>/<hex[2..]>`, and packs in
  `<root>/packs/` (below)
- Object store: loose objects at `blobs/<hex[0..2]>/<hex[2..]>`, and packs
  under `packs/` (below, "Packs in an object store")

### Packs (filesystem store, ADR 0029)

A pack holds many objects in one immutable file. A store with no `packs/`
directory reads exactly as before; a store with packs reads loose objects
first, then packs. Hashes, `TreeNode`, the API and clients do not change.

| Path | Contents |
|---|---|
| `packs/<id>.pack` | the entries |
| `packs/<id>.idx` | the index; its presence publishes the pack |
| `packs/tombstones` | packed objects deleted by GC |
| `packs/tombstones.lock`, `packs/repack.lock` | empty files, locked with `flock` |
| `packs/.tmp-*` | a write in progress, or garbage of a killed repack |
| `<shard>/.repack-<rest of hex>` | a loose copy a repack moved aside; a whole object |

`<id>` is the lowercase hex BLAKE3 of the whole `.idx` file. All integers are
big-endian.

**`.pack`**: the 8 bytes `LEEPPAK1`, then the entries back to back with no
separators or padding. Each entry is one standard zstd frame (with its
content size, without zstd's checksum), either:
- *whole*: the object compressed alone; or
- *delta*: the object compressed with its base object's bytes as a raw-content
  reference prefix (`zstd --patch-from=<base>`). The base is another entry of
  the same pack, at a lower offset.

**`.idx`**: one of two layouts, told apart by the magic. Writers produce
`LEEPIDX2` (ADR 0038); readers accept both, because stores packed before
ADR 0038 hold `LEEPIDX1` indexes. Both list every object of the pack once,
in strictly ascending hash order.

`LEEPIDX2`:

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `LEEPIDX2` |
| 8 | 8 | record count `n` |
| 16 | 8 | length of the `.pack` file in bytes |
| 24 | 32 | BLAKE3 of the whole `.pack` file |
| 56 | 8 | base modification time `t0` (signed unix seconds; the earliest in the pack) |
| 64 | 5 | field widths in bytes: offset `wo`, stored length `ws`, base `wb`, logical length `wl`, modification time `wm` |
| 69 | 3 | zero |
| 72 | 1024 | fanout: 256 u32, entry `b` = number of hashes whose first byte is at most `b` |
| 1096 | 32 × `n` | object hashes, strictly ascending |
| 1096 + 32`n` | `r` × `n` | records, the `i`-th for the `i`-th hash; `r = wo + ws + wb + wl + wm` |
| 1096 + (32 + `r`)`n` | 32 | BLAKE3 of all bytes before it |

Each record is five big-endian unsigned integers of the header's widths, back
to back:

| Field | Width | Value |
|---|---|---|
| offset | `wo` (1 to 8) | offset of the entry's frame in the `.pack` |
| stored length | `ws` (1 to 8) | length of the frame |
| base | `wb` (1 to 4) | 0 for a whole entry; for a delta, 1 + the rank (position in hash order) of its base's record |
| logical length | `wl` (0 to 8) | decoded length of the object |
| modification time | `wm` (0 to 8) | the object's modification time before it was packed, minus `t0` |

A writer picks each width as the fewest bytes that hold that field's largest
value in the pack (`wb` from `n`), and `t0` as the earliest modification
time. A width of 0 means the field is 0 for every record. A reader refuses
widths outside the ranges above, nonzero reserved bytes, a length other than
`1128 + (32 + r)n`, a fanout that does not count the hashes, `n` too large
for `wb`, a base past `n` or naming its own record, and a modification time
that overflows i64.

`LEEPIDX1` (read only):

| Offset | Size | Field |
|---|---|---|
| 0 | 8 | magic `LEEPIDX1` |
| 8 | 8 | record count `n` |
| 16 | 8 | length of the `.pack` file in bytes |
| 24 | 32 | BLAKE3 of the whole `.pack` file |
| 56 | 96 × `n` | records, strictly ascending by hash |
| 56 + 96`n` | 32 | BLAKE3 of all bytes before it |

Each 96-byte record:

| Offset | Size | Field |
|---|---|---|
| 0 | 32 | object hash (its address in the store) |
| 32 | 8 | offset of the entry's frame in the `.pack` |
| 40 | 8 | length of the frame |
| 48 | 8 | logical (decoded) length of the object |
| 56 | 32 | base object hash, or 32 zero bytes for a whole entry |
| 88 | 8 | the object's modification time before it was packed (signed unix seconds) |

A reader of either layout checks the magic, the trailing checksum, the order,
that every frame lies inside the pack after its magic, that every base is
another entry of the same pack at a lower offset (so chains end), and that
the `.pack` has the recorded length and magic. It
decodes a delta chain iteratively (the writer caps chains at 32; readers
refuse more than 64) and checks every decoded object against its address:
`BLAKE3(bytes)` for a file blob, `Hash::of_object("tree-v1", bytes)` for a
tree node. Any failure is `Error::Storage`.

Listings report a packed object with its logical size and the recorded
modification time, so a packed and an unpacked store list identically (the
backup copy compares sizes, ADR 0026) and GC's grace window is not reset by a
repack.

**`tombstones`**: append-only text, one line per deleted packed copy:
`<pack id hex> <object hash hex>\n`. A final line without its newline (a
torn append) is ignored, as is any malformed line. A tombstone hides only the
copy in the pack it names; a loose copy of the same hash is always served.
Appends and rewrites hold an exclusive lock on `tombstones.lock`; a full
repack rewrites the file (temp file, fsync, rename) keeping only lines whose
pack still exists, and removes it when none remain.

**Publishing.** A repack writes `.tmp-*.pack`, fsyncs it, renames it to
`<id>.pack`, then does the same for `<id>.idx` and fsyncs the directory after
each rename. The index rename publishes. A `.pack` with no `.idx` is garbage.
Replaced packs are removed index first.

### Packs in an object store (ADR 0040)

The same `.pack` and `.idx` bytes and ids as the filesystem store, at the
same names relative to the bucket. Locks and the append-only tombstones file
are replaced:

| Key | Contents |
|---|---|
| `packs/<id>.pack` | the entries, uploaded first (a multipart upload, visible only when complete) |
| `packs/<id>.idx` | the index, uploaded after the pack; its presence publishes the pack |
| `packs/tombstones/<pack id>/<object hash hex>` | empty; the object's copy in that pack is deleted |
| `packs/repack.lease` | text: `leep repack lease\nholder <id>\nexpires <unix seconds>\n`; present while a repack runs |

A tombstone hides only the copy in the pack it names, and a loose copy of
the same hash is always served, as on the filesystem. Readers ignore keys
under `packs/` they do not recognise. The lease is created only if absent
and replaced only if its etag is the one its holder last wrote; once its
expiry has passed another repack may take it over. A `.pack` with no `.idx`
is garbage. Replaced packs are removed index first, then their tombstones.

Golden bytes: `leep-store` asserts a fixed `LEEPIDX2` index's bytes and id
(`pack::tests::index_golden_bytes`), decodes fixed `LEEPIDX1` bytes with
their id (`idx1_golden_bytes_decode`), decodes fixed whole and delta frames
(`frame_golden_bytes_decode`), and reads a pack written by hand from this
layout with an index of each kind (`pack_tests::hand_written_pack_reads`).
A new layout needs a new magic (`LEEPPAK2`, `LEEPIDX3`) that old readers
refuse, and readers must keep reading the earlier ones.

## Garbage collection (no format change)

ADR 0008 adds `BlobStore::list_page` and `delete` and `MetaStore::list_changesets`.
These are trait additions only: no encoding, hash, SQL schema or blob layout
changed. Listings walk the existing layouts: filesystem shard directories and
object-store `blobs/<hex[0..2]>/` prefixes in hash order, and
`SELECT id FROM changesets ORDER BY id` (byte order) for metadata. Blob
modification time is the file mtime or object `LastModified`; a repeated `put`
refreshes it.

## Verify and repair (new `MetaStore` methods, no format change)

ADR 0025 adds `MetaStore::list_derived` and `delete_derived_kind`. They are data
operations on the existing `derived` table, not a schema change: `list_derived`
is `SELECT key, value FROM derived WHERE kind = ? AND key > ? ORDER BY key LIMIT ?`
(a range scan of the primary key `(kind, key)`, an empty `after` sorting below
every 32-byte key), and `delete_derived_kind` is
`DELETE FROM derived WHERE kind = ? AND key IN (SELECT key FROM derived WHERE kind = ? LIMIT 10000)`
repeated until it removes nothing (on Postgres, since ADR 0041,
`DELETE FROM derived WHERE ctid = ANY(ARRAY(SELECT ctid FROM derived WHERE kind = $1 ORDER BY key LIMIT 10000))`,
which no statistics can turn into a nested loop). No encoding, hash, index, table or `/v1`
shape changed, and `verify` and `repair` are not part of the HTTP API.
`verify` lists every kind to check it; `repair` rewrites or drops only the kinds
that history can regenerate, and refuses the rest (ADR 0025).

## Batched reads (new `MetaStore` methods, no format change)

ADR 0030 adds `MetaStore::get_changesets` and `get_derived_many`. They read the
existing tables: Postgres runs `SELECT id, data FROM changesets WHERE id = ANY($1)`
and `SELECT key, value FROM derived WHERE kind = $1 AND key = ANY($2)`, SQLite
the same with `IN (...)` of at most 500 distinct keys per statement. Changeset
rows are decoded and checked against their id as in `get_changeset`. No
encoding, hash, index, table or `/v1` shape changed.

## Batched writes (new `MetaStore` methods, no format change)

ADR 0031 adds `MetaStore::put_derived_many` and `put_changeset_with_derived`.
They write the existing tables with the existing encodings. Postgres runs
`INSERT INTO derived (kind, key, value) SELECT * FROM UNNEST($1::text[],
$2::bytea[], $3::bytea[]) ORDER BY 1, 2 ON CONFLICT (kind, key) DO UPDATE SET
value = excluded.value`, and for a changeset with rows the same statement
behind `WITH c AS (INSERT INTO changesets (id, data) VALUES ($1, $2) ON
CONFLICT DO NOTHING)`, so either is one atomic statement. Rows repeating a
`(kind, key)` are reduced to the last one first, and the `ORDER BY` gives every
batch the same row lock order. SQLite runs the single-row statements inside one
transaction. The rows a new changeset is stored with (`root_tree`,
`generation`, `skiplist`, and `rebased-origin-v1` or the git mapping kinds) are
byte-identical to what the derived kinds and the old writes produced. No
encoding, hash, index, table or `/v1` shape changed.

## Chained ref moves (new `MetaStore` method, no format change)

ADR 0032 adds `MetaStore::cas_ref_logged_chain(name, expected, steps)`, which
group commit of lands uses. It writes the existing `refs` and `ref_log` tables
with the existing encodings: one conditional update of `refs` to the last
step's target, then (after the Postgres advisory lock, as for
`cas_ref_logged`) one multi-row `INSERT INTO ref_log` whose rows are listed in
step order, so `seq` increases along the chain and each row's `old_target` is
the previous row's `new_target`. A reader of the log sees the same rows as for
the same moves made one swap at a time. No `/v1` shape changed.

## Short changeset ids (new endpoint and `MetaStore` method, no format change)

ADR 0035 adds `GET /v1/resolve-prefix/{prefix}` with two response types:
`ResolvePrefixResponse` `{"id"}` (200, one match) and
`AmbiguousPrefixResponse` `{"error", "candidates", "truncated"}` (400, several
matches, at most 10 candidates in ascending order; a superset of
`ErrorResponse`). No match is 404; a prefix that is not 4 to 64 hex digits is
400 with `ErrorResponse`. `MetaStore::changesets_with_prefix(prefix, limit)`
reads the existing `changesets` table: `SELECT id FROM changesets WHERE id >= ?
AND id <= ? ORDER BY id LIMIT ?`, the bounds being the prefix padded with
`0x00` and `0xff` bytes (`leep_core::prefix::IdPrefix`). No encoding, hash,
index or table changed, and no existing `/v1` shape changed.

## Explicit ref creation on land (additive API field, ADR 0036)

`LandRequest` gains the optional `create` (`true`, `false` or absent; omitted
when unset). Absent means what it always meant: a missing `target_ref` is
created. `false` turns a land onto a missing ref into a 404 containing
`does not exist and the land may not create it`. Old clients never send the
field and old servers ignore it, so neither side's behaviour changes until
both are new and the client asks. No response shape, stored object or table
changed.

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

## Backup directory (new, ADR 0026; no change to stored objects)
Written and read by `leep-server backup`, `restore` and `backup-verify`.
Nothing persisted by the server or sent over `/v1` changes.

```
<dir>/blobs/<2 hex>/<62 hex>              blob pool: the filesystem layout above (or any blob store via --blobs-to)
<dir>/blobs/packs/...                     packs copied whole from a packed source (ADR 0039): the "Packs" layout above
<dir>/snapshots/<name>/meta.db            metadata snapshot
<dir>/snapshots/<name>/manifest.json      written last, atomically; a snapshot without one does not exist
```
`name` is the UTC time the run started, `YYYYMMDDTHHMMSSZ` (or `--name`).

The pool holds the source's packs as files when both sides keep packs
(ADR 0039): `packs/<id>.pack`, `packs/<id>.idx` and the tombstones of those
packs. In a bucket (`--blobs-to s3://...`) they use the object store layout
of ADR 0040, with tombstones at `packs/tombstones/<id>/<hash>`. Loose objects
stay loose. A pool may hold an object both loose and packed, and pools
written before ADR 0039 hold no packs; both read and restore the same way.

`meta.db` is a SQLite database with the schema of `schema_migrations` (ADR
0023) at the version in the manifest, whatever backend the metadata came from:
tables `changesets`, `refs`, `derived`, `ref_log` hold the rows of one read
transaction, byte for byte, including every `derived` kind (signatures,
reviews and GC rows too). It is a single file (journal mode `DELETE`, no `-wal`).
The highest ref log `seq` ever issued is preserved (`sqlite_sequence`), so a
restore continues the numbering. It is never opened in place by a reader: tools
work on a scratch copy.

`manifest.json` (UTF-8 JSON; unknown fields are ignored and readers refuse a
`format` above the one they know):

| Field | Meaning |
|---|---|
| `format` | `1` |
| `leep_version`, `name`, `created_at` | writer version, directory name, unix seconds when sealed |
| `snapshot_taken_at` | unix seconds of the metadata read transaction: the recovery point |
| `source_backend` | `sqlite` or `postgres` |
| `schema_version` | newest migration applied to `meta.db`; a reader older than this refuses |
| `meta_file`, `meta_blake3`, `meta_bytes` | the file, its BLAKE3 (hex) and size; checked before any read |
| `counts` | `changesets`, `refs`, `derived`, `ref_log` row counts; a restore must load exactly these |
| `ref_log_last_seq` | highest ref log `seq` ever issued |
| `refs` | ref name to changeset id (hex) at the snapshot |
| `blobs` | `included`, `location`, `listed`, `already_present`, `copied`, `bytes_copied`, `replaced`, `vanished`, `only_in_backup`, `corrupt` (hashes the source held with wrong bytes); since ADR 0039 also `packs_listed`, `packs_already_present`, `packs_copied`, `pack_bytes_copied`, `packs_pruned`, `tombstones_copied`, `corrupt_packs` (0 or empty when absent) |
| `check` | `mode` (`closure`, `closure+rehash`, `skipped`), `ok`, `changesets`, `tree_nodes`, `blobs_checked`, `blobs_rehashed`, `problems`, `first_problems` |
| `elapsed_secs` | wall time of the run |

A snapshot restores when `check.ok` is true (or `--accept-failed-check`), its
`meta.db` matches `meta_blake3`, and the blob pool holds every blob its
changesets need. Adding a manifest field is additive; changing the meaning of
an existing one needs a new `format`.

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
