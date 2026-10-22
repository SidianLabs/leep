# Leep formats and compatibility

Anything listed here is persisted or sent over the wire. Changing it
carelessly corrupts history or breaks deployed clients, so every change goes
through the [`change-persistent-format`](../.claude/skills/change-persistent-format/SKILL.md)
skill.

## Hashing

- Algorithm: BLAKE3, 32 bytes. Hex is lowercase, 64 characters.
- Blob: `BLAKE3(raw file bytes)`, with no header.
- Object: `BLAKE3("leep:" ‖ kind ‖ 0x00 ‖ postcard(object))`, where `kind`
  carries a version suffix (`changeset-v1`, `tree-v1`). The prefix separates
  object kinds from each other, but not from blobs: the blob whose bytes are
  that preimage has the object's address. Since tree nodes share the
  `BlobStore` with blobs, blob writes refuse any bytes that start with
  `leep:` and contain a NUL (`leep_core::hash::is_object_preimage`) until the
  two are stored in separate namespaces (ADR 0041).

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
| `Changeset` | `changeset-v1` | `MetaStore.changesets.data`, as the canonical bytes or a zstd frame of them (ADR 0053, "Changeset rows" below) |
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
| derived `cs-condemned-v1` | — | `MetaStore.derived`, key = changeset id, value = condemnation time, unix seconds as 8-byte big-endian `i64`, from the sweep host's clock (ADR 0059) |
| derived `cs-rescue-v1` | — | `MetaStore.derived`, key = changeset id, value = the condemnation time the writer observed, 8-byte big-endian `i64` |
| derived `cs-doom-v1` | — | `MetaStore.derived`, key = changeset id, value = condemnation time (8-byte big-endian `i64`) then one state byte: 0 deleting, 1 abandoned, 2 deleted |
| derived `gc-fence-active-v1` | — | `MetaStore.derived`, one row, key = 32 zero bytes, value = unix seconds the current changeset condemnation round began, 8-byte big-endian `i64`, or 0 when none is under way |
| derived `cs-reach-v1` | — | `MetaStore.derived`, key = changeset id, value = the single byte `0x01` (anything else reads as absent): a ref or ref log entry reaches the changeset, and every parent has a row. Written parents first (ADR 0059). One more row, key = 32 zero bytes, holds the `seq` of the newest ref log entry the rows cover (8-byte big-endian) |
| derived `blob-trigrams-v1` | — | `MetaStore.derived`, key = blob hash, value = see "Code search index" below |
| derived `tree-trigrams-v1` | — | `MetaStore.derived`, key = tree node hash (`TreeNode::hash()`), value = see "Code search index" below |
| derived `history-tree-v4` | — | `MetaStore.derived`, key = changeset id, value = part 0 of the changeset's history pack (ADR 0045): the history nodes its derivation wrote, and its root node's address |
| history pack part | `history-node-v4` | `MetaStore.derived` (kind `history-node-v4`), key = `Hash::of_object("history-node-v4", changeset id ‖ part as u32 LE)`, value = part 1 and up of a pack that did not fit in one |
| derived `log-filter-v1` | — | `MetaStore.derived`, key = changeset id, value = see "Log filter index" below (ADR 0047) |
| derived `log-filter-name-v1` | — | `MetaStore.derived`, key = `Hash::of_object("log-filter-name-v1", fingerprint as 8 bytes big-endian)`, value = the author's name, lower-cased (`str::to_lowercase`), UTF-8 |
| retired `history-tree-v1`, `history-node-v1` | — | written by servers before ADR 0027; never read by current servers; removed with `repair drop --kind history-tree-v1` |
| retired `history-tree-v2`, `history-node-v2` | — | written by servers from ADR 0027 to ADR 0037; never read by current servers; removed with `repair drop --kind history-tree-v2` |
| retired `history-tree-v3`, `history-node-v3` | — | written by servers from ADR 0037 to ADR 0045 (one content-addressed postcard row per node, chunk, list or patch, keyed by `Hash::of_object("history-node-v3", bytes)`); never read by current servers; removed with `repair drop --kind history-tree-v3` |
| other derived kinds | — | owned and versioned by `leep-derive` |

## Integrity on read

Content-addressed data is verified when read: `Repo::read_tree` hashes the
stored bytes and compares them with the key, the SQL backends decode a
changeset (decompressing a compressed row first) and compare its id with its
row key, and a history pack's part is
checked against its checksum, which covers its writer and number. A
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
## Log filter index (new derived kinds, no change to `Changeset`)

ADR 0047. Golden values are asserted in `leep-derive`
(`golden_fingerprints_and_bytes`); any change to the fingerprint, the values
indexed, the caps or the encoding is a new kind name (`-v2`).

A *fingerprint* is a `u64`: take `h = Hash::of_object("log-filter-fp", c ‖
p₁ ‖ 0x00 ‖ p₂ ‖ 0x00 …)` with `c` the class as one byte, read its first 8
bytes big-endian as `v`, and the fingerprint is `(v >> 2) | (c << 62)`.
Classes: 0 author (`p₁` = the author lower-cased with `str::to_lowercase`),
1 `extra` key present (`p₁` = key), 2 `extra` key with value (`p₁` = key,
`p₂` = value), for the keys `agent.model` and `agent.session` only.

A changeset's *depth* is 0 for a root, else its first parent's depth + 1.
It has `tz(depth) + 1` *levels* (`tz` = trailing zero bits; a root has 1).
Level 0 holds the fingerprints of the changeset and, at a merge, of what
each other parent brings (the changesets they reach and the first parent
does not); level `j` is level `j - 1` united with level `j - 1` of the
first-parent ancestor `2^(j-1)` steps up. A class of more than 65,536
fingerprints in one level is dropped from it and flagged. A merge whose
walk (ADR 0051) visits more than 200,000 changesets instead stores, besides
its own values, the union of the levels down each other parent's
first-parent chain to that parent's stop (a superset). A merge's *stop* per
other parent is the first changeset on that parent's first-parent chain
that the first parent reaches; past the walk's budget, any changeset on that
chain the first parent reaches, or none.

**`log-filter-v1`** value: LEB128 varint depth, varint generation of the
nearest merge on the first-parent chain (the changeset included; 0 if none),
varint level count (must be `tz(depth) + 1`), then per level: one byte of
overflow flags (bit `c` set: class `c` overflowed and has no fingerprints
here), a varint count, and the fingerprints ascending, as varint deltas, the
first relative to zero (a zero delta after the first is invalid). Then a
varint count of stops, one per parent after the first, each a byte 0 (none)
or 1 followed by the stop's varint generation and 32-byte id. No trailing
bytes.

## Path-history index (new derived kinds, no change to `Changeset`)

ADR 0013, stored in chunks since ADR 0027, as patches since ADR 0037 and in
one pack per changeset since ADR 0045. The logical node is
`leep_derive::derived::HistoryNode { entries: BTreeMap<String, HistoryEntry> }`,
keyed by single path components. `HistoryEntry` is
`Leaf { changed: Vec<ChangesetId> }` or
`Tree { changed: Vec<ChangesetId>, node: NodeRef }`. `changed` is a sorted
(by id bytes), deduplicated set of changeset ids; `node` is where the
children's node is stored. The meaning of `changed` (the maximal changesets
whose entry at the path differs from their first parent's; merges take the
union of parents' sets for paths they leave as in `parents[0]`) is part of
the format. Entries of deleted paths are kept.

**Stored nodes.** A node is stored as one `StoredHistoryNode`, or as a chunk
list and chunks:

- `Flat(HistoryNode)`: the whole node. Used for nodes of at most 64 entries,
  and for every chunk. The empty node is never stored; its address
  `NodeRef::EMPTY` (the zero changeset id, part 0, index 0) is always
  present.
- `Split(Vec<ChunkRef>)`, `ChunkRef { first, node }`: a node of more than 64
  entries, cut into runs of entries in name order. A chunk ends after an
  entry whose name `n` has `Hash::of_object("history-chunk-boundary", n)`
  with its first byte's low 5 bits zero (about one name in 32), or when it
  reaches 128 entries, or at the last entry.
- `Patch { base, over }`: the `Flat` node `base` with the entries of `over`
  added or replaced.
- `SplitPatch { base, refs }`: the `Split` node `base` with the chunk whose
  first name is each key set (`Some`) or removed (`None`).

A patch's `base` must be a keyframe (`Flat` or `Split`) of its shape, never
another patch. For each whole node, chunk and chunk list that changed, with
`K` the keyframe of its predecessor (the first parent's version of the same
node; for a chunk, the predecessor's chunk whose range holds its first
name), the stored node is:

- `K` itself, if the content equals `K`, and the predecessor itself if the
  content equals it;
- otherwise a patch on `K` of the entries (or refs) that differ, if every
  name of `K` is still present and `p² <= 96 × w`, where `p` and `w` are
  `history_pack::cost` of the patch and of the whole node (the bytes each
  takes encoded as below, in a part of its own with its own id table: 32
  bytes per distinct id other than the writer's, and three per reference to
  a node of the same pack);
- otherwise the whole node, a new keyframe.

Unchanged chunks keep their address.

**Addresses.** A node's address is a `NodeRef { cs, part, idx }`: node `idx`
of part `part` of the pack written by changeset `cs`'s derivation.

**Packs.** A changeset's derivation writes every node it creates into its
pack, children before the nodes that name them, and cuts the sequence into
parts: a node goes into the current part unless the part already holds a
node and would then exceed 16,384 bytes (counting its node bytes and id
table, before compression); then a new part starts. Parts are numbered from
the last one cut, which holds the root and is part 0: the changeset's
`history-tree-v4` value. Parts 1 and up are `history-node-v4` rows. A
changeset whose history equals its first parent's has a part 0 with no
nodes, naming that root.

**A part** is

```
checksum (8 bytes) | encoding (1 byte) | rest
```

`checksum` is the first 8 bytes of
`Hash::of_object("history-node-v4", cs ‖ part as u32 LE ‖ rest)`, with
`rest` uncompressed. `encoding` 0 means `rest` follows as is; 1 means a
varint of its length and then a zstd frame of it (written at level 3, when
shorter). Two copies of a part are the same part when their checksums and
uncompressed `rest` are equal; their compressed bytes may differ between
zstd versions. `rest` is, with varints in LEB128:

```
[parts 1 and up: cs (32 bytes) | part (varint)]
table length (varint) | table ids (32 bytes each)
[part 0: root (node reference)]
node count (varint) | each node's length in bytes (varint) | nodes
```

The table holds the changeset ids the part names other than its writer `cs`.
An **id** is a varint: 0 for `cs`, `i` for the table's `i`-th id (from 1). A
**node reference** is an id, then a part and an index (varints). For the
writer's own pack the part is counted from the part holding the reference
(the referenced part's number minus this one's), and for another pack it is
the part's number.

A **name** is front-coded against the previous one (or the empty string):
the length of the prefix they share (varint), the length of the rest
(varint), the rest. An **entry** after its name is `(ids << 1) | tree`
(varint, `tree` 1 for `Tree`), the ids of `changed`, and for `Tree` the node
reference. Nodes are a tag and a body:

- `0` `Flat`: entry count, then each entry's name and entry.
- `1` `Split`: chunk count, then each chunk's first name and node reference.
- `2` `Patch`: the base's node reference, the count of `over`, then each
  entry of `over`, in name order: a varint `d` and the entry. `d >= 1` names
  the keyframe's entry at position `prev + d` (positions from 0, `prev`
  starting at -1 and set to each position named); `d = 0` is followed by a
  name the keyframe lacks, front-coded against the previous such name.
- `3` `SplitPatch`: the base's node reference, the count of changes, then
  each change, in first-name order: its first name as for `Patch` (a step
  over the keyframe list's chunks, or 0 and a new name), then 0 (removed) or
  1 and the chunk's node reference. The first name of the chunk after each
  set chunk is not stored; readers take it from the patched list.

Example: the pack of changeset `w` with one node holding `a` (a file last
changed by `w` itself) and `ab` (a directory last changed by changeset `c`,
whose node is node 3 of part 2 of `c`'s pack). Its part 0 is stored
uncompressed (`encoding` 0) with this `rest`:

```
01 <32 bytes c> | 00 00 00 | 01 0f | 00 02 | 00 01 61 02 00 | 01 01 62 03 01 01 02 03
```

(one table id; root: this pack, part 0, node 0; one node of 15 bytes; `Flat`,
two entries; `a`, one id, leaf, the writer; prefix `a` plus `b`, one id,
tree, table id 1, node at table id 1, part 2, node 3.)

Readers check each part's checksum, that a part after the first names its
own writer and number, that each node is exactly its length, that each
chunk starts with its `first` and ends before the next chunk's `first`, and
that a patch fits its keyframe; damage is an error, not a wrong answer. A
node index past a part's nodes is a missing node. The golden bytes, the
boundary choice and the patch rule are asserted in `leep-derive`
(`pack_encoding_is_stable`, `chunk_boundaries_are_stable`,
`patches_are_chosen_while_small_and_name_their_keyframe`). Changing any of
them, the cut, the field or variant order, the meaning of `changed` or the
key derivation requires `history-tree-v5` and `history-node-v5`. The API
addition (`GET /v1/last-change/{id}`) adds one endpoint and one response
type; `/v1/log` is unchanged.

`history-tree-v1` and `history-node-v1` (ADR 0013 as first written) stored each
node as one row of the bare postcard `HistoryNode` (32-byte ids and child
addresses), keyed by `Hash::of_object("history-node-v1", bytes)`.
`history-tree-v2` and `history-node-v2` (ADR 0027) stored postcard `Flat` and
`Split` rows, and `history-tree-v3` and `history-node-v3` (ADR 0037) added
`Patch` and `SplitPatch` rows (whose list patches stored each set chunk's
successor), each keyed by `Hash::of_object` of its kind and bytes, with a
`history-tree-v*` value of 32 bytes, the root's row hash. Current servers
neither read nor write any of them.

## Rename detection (no persisted change)

ADR 0016. Renames are computed when displaying; no stored data, derived kind or
hash changed. The API additions are all optional, and absent fields mean the old
behavior:

- `GET /v1/diff` query: `renames=true|false` (default false) and
  `rename_threshold=0..100` (default 50; anything else is 400, also when
  `renames` is false). Paged diffs (`limit`) are under "Page cursors".
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
changesets(id BYTEA PRIMARY KEY, data BYTEA NOT NULL,          -- data = Changeset::encode(), or a zstd frame of it (ADR 0053)
           stored_at BIGINT)                                   -- migration 0007; SQLite: INTEGER; unix seconds of the first store
changeset_dicts(id BIGINT PRIMARY KEY,                         -- SQLite: INTEGER; migration 0003
                created_at BIGINT NOT NULL, data BYTEA NOT NULL) -- a zstd dictionary whose header id = id
store_features(name TEXT PRIMARY KEY,                          -- migration 0004
               added_at BIGINT NOT NULL)                       -- SQLite: INTEGER; unix seconds
refs(name TEXT PRIMARY KEY, target BYTEA NOT NULL)
derived(kind TEXT, key BYTEA, value BYTEA, PRIMARY KEY(kind, key))
ref_log(seq BIGSERIAL PRIMARY KEY,                             -- SQLite: INTEGER PRIMARY KEY AUTOINCREMENT
        name TEXT NOT NULL, old_target BYTEA, new_target BYTEA, -- 32-byte ids; NULL = no ref
        time BIGINT NOT NULL, actor TEXT, operation TEXT NOT NULL, message TEXT,
        land_id BYTEA, land_changeset BYTEA)                   -- migration 0005; NULL unless a keyed land
ref_log_name: INDEX ON ref_log(name, seq)
ref_log_land_id: UNIQUE INDEX ON ref_log(name, land_id) WHERE land_id IS NOT NULL
leases(scope TEXT NOT NULL,                                     -- migration 0006; 'land' for lands
       repo_id BIGINT NOT NULL DEFAULT 1,                       -- SQLite: INTEGER
       name TEXT NOT NULL, shard INTEGER NOT NULL DEFAULT 0,    -- land: the ref, shard 0
       holder TEXT NOT NULL, address TEXT NOT NULL,             -- holder: random per server process; its --advertise-url
       protocol INTEGER NOT NULL, meta TEXT NOT NULL DEFAULT '',-- per scope; land: protocol 1, meta ''
       epoch BIGINT NOT NULL,                                   -- +1 whenever the holder changes
       renewed_at BIGINT NOT NULL, expires_at BIGINT NOT NULL,  -- ms, the store's clock; 0 = released
       PRIMARY KEY (scope, repo_id, name, shard))
```

`ref_log` (ADR 0018) is appended in the transaction of every ref create, move
or delete and never updated or deleted. `old_target` is NULL for a create,
`new_target` for a delete; `time` is unix seconds by the writing server;
`actor` is the token name or NULL; `operation` is a short label (`land`,
`cas`, `undo`, `import`, or a client's own, see "Ref log" below). Tables and
index come from migration 0001; databases made before the ref log existed
gain them on upgrade, and refs that moved before then simply have no rows.

Migration 0002 (`analyze_thresholds`, ADR 0042) changes no table, column or
index. On Postgres it sets storage parameters so autovacuum re-analyzes the
large tables sooner: `derived (autovacuum_analyze_scale_factor = 0.01,
autovacuum_analyze_threshold = 10000)` and `changesets
(autovacuum_analyze_scale_factor = 0.02, autovacuum_analyze_threshold =
10000)`. The SQLite migration of the same number does nothing; it keeps the
two backends' versions equal.

Migration 0003 (`changeset_dicts`, ADR 0053) adds the dictionaries table,
empty. Nothing else changes, and a server keeps writing plain changeset rows
until the table has a row; see "Changeset rows" below.

Migration 0004 (`store_features`, ADR 0055) adds the table of format
features, and inserts `changeset-zstd-dict` if `changeset_dicts` has a row;
see "Format features" below.

Migration 0005 (`land_ids`, ADR 0057) adds the nullable `ref_log` columns
`land_id` (16 to 32 bytes, the client's id) and `land_changeset` (32-byte id
of the changeset the land asked for; `new_target` is where it went) and the
unique partial index `ref_log_land_id`. A move made by a land that carried a
`land_id` writes both in its entry; every other entry leaves them NULL, and
writes exactly the columns of v4. An id is unique per ref. The rows are read
by `MetaStore::find_lands`, never changed, and kept with the entry. v4
servers neither read nor write the columns.

Migration 0006 (`leases`, ADR 0056) adds the lease table, empty. Servers
write it only with `--land-coordination on` (and, later, for the indexer
scope of ADR 0058); older servers never read it, so the migration tolerates
them. See "Leases" below.

Migration 0007 (`changeset_stored_at`, ADR 0059) adds the nullable
`changesets.stored_at`: unix seconds, by the clock of the server that first
stored the changeset. Every `put_changeset*` writes it on insert and a
repeated store keeps the first value; `compact-changesets` rewrites `data`
only. With no default it is a catalog-only change on both backends: no row
is rewritten. Rows stored before the migration, and rows a v6 server stores
after it (v6 never names the column), stay NULL and read through
`MetaStore::changesets_stored_at` as the migration's
`schema_migrations.applied_at`. Changeset garbage collection ages changesets
by it, never by the client's `Changeset::timestamp`. Backups (ADR 0026) copy
the column with that substitution already made, so a restore, whose own
migrations run later, keeps every row's age.

### Changeset rows (ADR 0053)

`changesets.data` is one of:

- **Plain**: the canonical postcard bytes, `Changeset::encode()`. Every row
  written before ADR 0053, and every row written while `changeset_dicts` is
  empty, a changeset whose frame would not be smaller, or one over 256 MiB.
- **Compressed**: one zstd frame (magic `28 b5 2f fd`) whose content is
  exactly the canonical bytes. The frame header records the content size
  (required; at most 256 MiB, which bounds the decoded size; readers never
  reserve more than 1 MiB on the header's word and decode larger frames as
  a stream with a window of at most 8 MiB) and the dictionary id: a `changeset_dicts.id`,
  or 0 (absent) for a frame made without a dictionary. No checksum: the id
  check covers it. Written at level 9; the level is not part of the format.

A reader that finds the magic decompresses with the named dictionary,
decodes and compares the id with the row key; if that fails it decodes the
row as plain bytes, since a plain changeset with 40 parents whose first id
starts `b5 2f fd` begins with the magic too. A row that is neither is
`Error::Storage` ("corrupt changeset row", or "needs dictionary N, which
changeset_dicts lacks"). Writers never store plain bytes that start with the
magic; they compress them, with dictionary 0 if there is none.

`changeset_dicts.data` is a zstd dictionary (magic `37 a4 30 ec`, then the
dictionary id as `u32` little-endian), trained with zstd's
`ZDICT_trainFromBuffer` on canonical changeset bytes; its header id must
equal the row's `id` (ids start at 1). Rows are added by `leep-server
compact-changesets` and never changed or deleted. Rows in different stores
of the same history are different bytes; only the decoded changesets and
their ids compare.

### Format features (ADR 0055)

A `store_features` row names something the store's rows use that a reader
must understand. Every binary that has the table reads it when it opens the
store and refuses to start if a name is unknown to it. A tool records a
feature before it writes the first row that needs it, and nothing removes
one. Names so far:

| Name | Meaning | Recorded by |
|---|---|---|
| `changeset-zstd-dict` | `changesets.data` may be a zstd frame made with a `changeset_dicts` dictionary (ADR 0053) | `compact-changesets`, before it stores or uses a dictionary; migration 0004 for a store that already has one |

Binaries from before migration 0004 do not read the table.

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
  that predate the ref log. Later migrations run exactly once. The one
  exception is 0005, whose Postgres columns and index (and SQLite index) are
  `IF NOT EXISTS` so the index can be built by hand `CONCURRENTLY` first
  (ADR 0057).
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

## Leases (new table, `MetaStore` methods and headers, ADR 0056)

The `leases` table above holds which server holds one kind of work (`scope`)
over one name and shard. Land coordination uses scope `land`: per ref
(`name`, shard 0), which server batches its lands.
A row is written only by single conditional statements: take or keep
(`INSERT ... ON CONFLICT DO UPDATE ... WHERE` the row is the claimant's own,
or `expires_at` has passed, or `renewed_at` is older than the caller's
`stale_after`, which the coordinator sets to the TTL, so a lease is taken over
exactly one TTL after its last renewal), renew (`UPDATE ... WHERE holder AND epoch`) and release (the same,
setting both times to 0 so `epoch` keeps growing). Times are milliseconds by
the database's clock on Postgres and by the process clock on SQLite.
`repo_id` is 1 until repositories exist (ADR 0054). Rows are hints about
where lands are batched and are never needed to read anything: a backup does
not copy them and a restore clears the table. `MetaStore` gains
`supports_leases`, `lease`, `acquire_lease`, `renew_leases` and
`release_lease`, keyed by `LeaseKey` (scope, repo_id, name, shard), with the
timing passed per call; the defaults refuse, which leaves a backend without
them uncoordinated. `check_lease_contract` checks them.

On the wire nothing a client sends or reads changed. Between servers:

- A forwarded `POST /v1/land` carries the client's body byte for byte and its
  `Authorization` header, plus `Leep-Land-Forward: v=1; epoch=<n>;
  from=<holder>`. The header is an unauthenticated hint, honoured only from
  an address in the receiving server's `--peer-url-prefix` hosts or
  `--peer-cidr` networks; from anywhere else the request is an ordinary land.
- A server that does not coordinate the ref answers such a land `503` with
  `Leep-Land-Not-Leader: 1` and an `ErrorResponse` body; the sender routes the
  land again. Clients never see it.
- With coordination on, `GET /v1/health` carries `Leep-Instance: <holder>`,
  which a server uses to check that its `--advertise-url` reaches itself.

## Index leases and bounded queries (additive, ADR 0058)

No table, column or stored encoding changed.

- **Index leases** are `leases` rows with `scope = 'index'`, `repo_id = 0`
  (index rows are shared by every repository), `name` the group's primary
  kind (`history-tree-v4`, `log-filter-v1`, `tree-trigrams-v1`) and `shard =
  0`, taken with a 15 s TTL and taken over 6 s after the last renewal.
  `meta` is a JSON object the holder rewrites on each renewal:
  `{"rate": <changesets a second over the last minute>, "backlog": <changesets
  its last pass derived>, "shards": 1}`; readers ignore fields they do not
  know and treat a missing or unreadable one as unknown. `address` is the
  holder's `--advertise-url` (informational), `protocol` is 1.
- **`last_merge-v1`** rows are now written with the changeset (same value as
  before); a changeset whose first parent has no row and is neither a merge
  nor a root, and whose grandparent's row is not known, is stored without
  one and the row stays derived lazily.
- **Request header** `Leep-Indexing: retry | stale-ok` on `GET /v1/log`,
  `/v1/last-change`, `/v1/blame` and `/v1/search`; unknown values count as
  absent. Servers that honour it list the ready feature `indexing-retry`.
- **503 answer** to a query whose index is too far behind: `Retry-After`
  (seconds, 1 to 30), `Leep-Indexing: kind=<group>; behind=<n>`, and the usual
  `ErrorResponse` with an optional `indexing: {kind, behind, retry_after_ms}`
  (omitted on every other error). `behind` is generations between the
  requested changeset and the newest indexed first-parent ancestor (for
  search, at most 64 are looked at).
- **Optional response fields** `answered_at` (changeset id) and `behind`
  (generations) on `LogResponse`, `LastChangeResponse`, `BlameResponse` and
  `SearchResponse`, present only when a `stale-ok` answer is for an older
  changeset than asked; `SearchStats.files_unindexed` (omitted when 0): files
  fetched to stand in for missing trigram rows. Successful indexed reads carry
  `Leep-Index-Scanned: <n>`, the changesets (files, for search) examined
  without index rows.
- Answers are the same with or without index rows: entries, order,
  `truncated` and whether `next` is given. Cursor text may differ, and search
  `stats` other than `files_considered`, `files_skipped` and `indexed`
  describe the work done (`blob_rows_derived` and `tree_rows_derived` are 0
  for a bounded search). A filtered log page now has `next` only when another
  match follows.

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

`<id>` is the lowercase hex BLAKE3 of the whole `.idx` file. The id is
therefore a function of the compressed bytes: the same objects packed by a
different zstd version, level or build get a different id, and a full repack
after a zstd upgrade rewrites every pack once. Nothing relies on the reverse;
one id names one set of bytes, which is all repack, backup and sync assume.
All integers are big-endian.

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
torn append) is ignored, as is any malformed line; the next append cuts a
torn line off first, under the lock, so the two cannot run together. A tombstone hides only the
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
repeated until it removes nothing (on Postgres, since ADR 0042,
`DELETE FROM derived WHERE ctid = ANY(ARRAY(SELECT ctid FROM derived WHERE kind = $1 ORDER BY key LIMIT 10000))`,
which no statistics can turn into a nested loop). No encoding, hash, index, table or `/v1`
shape changed, and `verify` and `repair` are not part of the HTTP API.
`verify` lists every kind to check it; `repair` rewrites or drops only the kinds
that history can regenerate, and refuses the rest (ADR 0025).

ADR 0052 adds `MetaStore::count_derived` (`SELECT count(*) FROM derived WHERE
kind = ?`) and `delete_derived_step`, which replaces the loops above as the way
backends delete; `delete_derived_kind` is now a provided method over it. A
large drop walks the table in physical order, on Postgres
`DELETE FROM derived WHERE ctid >= $2::tid AND ctid < $3::tid AND kind = ANY($1)`
with index paths switched off for the statement, on SQLite
`DELETE FROM derived WHERE rowid >= ? AND rowid < ? AND +kind IN (...)`; a
small one runs the key-order batch from a cursor (`key > $after`). The cursor
passed between steps is in memory only and never stored. No table, index,
encoding or `/v1` shape changed.

`MetaStore::estimate_derived` returns a `RowCount { rows, exact }` for
`upgrade-index`'s plan. The default is `count_derived`. Postgres counts up
to 100,000 rows (`SELECT count(*) FROM (SELECT 1 FROM derived WHERE kind = $1
LIMIT 100000)`), and past that multiplies `pg_class.reltuples` of `derived`
by the kind's frequency among `pg_stats.most_common_vals` of `kind`. If the
kind is missing from the statistics it counts. `rows` is 0 only when there
are none. Nothing is stored.

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

## Land ids (additive API field and ref log columns, ADR 0057)

`LandRequest` gains the optional `land_id`: a string of 32 to 64 lowercase
hex digits (16 to 32 bytes), omitted when unset. Anything else is 400. A
land carrying an id whose land `target_ref` already recorded answers with
that land's `Landed` result and moves nothing; if the recorded land was of
another changeset it is 400 (`land id ... already landed ...`). Old clients
never send it and old servers ignore it. `ReadyResponse` (`GET /v1/ready`)
gains `features`, a list of strings omitted when empty and absent from older
servers; a server that records land ids lists `land-id`. No other response
shape changed.

Stored as above ("SQL schema", migration 0005). `MetaStore` gains
`find_lands(name, ids)`; `RefUpdateContext` gains `land`, which
`cas_ref_logged` and `cas_ref_logged_chain` store with the entry, refusing a
taken id with `Conflict` and a land id on a delete or twice in one chain with
`Invalid`. No changeset, tree, hash or derived encoding changed.

The CLI's `.leep/state.json` gains the optional `pending_land` (`changeset`,
`target_ref`, `land_id`), present only while a land waits for a final
answer; older CLIs ignore it.

`RefLogEntry` (`GET /v1/reflog`, `GET /v1/reflog/{name}`) later gained the
optional `land_id` (the same lowercase hex as `LandRequest.land_id`) and
`land_changeset` (a changeset id), read from the columns above. Both are
omitted when the move carried no land id, so entries without one serialize
exactly as before, and older servers never send them. `RefLogEntry` is JSON
on the wire and SQL columns at rest; it is not hashed and not encoded with
postcard, and backups already carried the columns (`SnapLogRow`), so no
stored format changed.

## Stored `clean` blobs of a land conflict (`land-clean-blobs`)

`GET /v1/ready` lists `land-clean-blobs` in `features`: every blob a
`LandResponse::Conflicts` names in `clean` is stored before the answer. An
older server could name merged text it had only staged in memory, so a
client that applied `clean` from it could hit `missing blob`. No `/v1`
field, stored object or encoding changed.

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

## Changeset garbage collection, phase 1 (new column, derived kinds and `MetaStore` method, ADR 0059)

Persisted: `changesets.stored_at` (migration 0007 above) and the derived
kinds `cs-condemned-v1`, `cs-rescue-v1`, `cs-doom-v1`, `gc-fence-active-v1`
and `cs-reach-v1` in the table above. The first three have exactly the
encodings and rules of their `blob-*-v1` counterparts (ADR 0012), keyed by
changeset id: a changeset is condemned iff `condemned > rescue`. Golden
bytes are asserted in `leep-derive` (`cs_row_encodings_are_stable`).
`MetaStore` gains `changesets_stored_at(ids)`, whose default is an error
(like `find_lands`), so a wrapper that does not forward it fails loudly.

Wire: `GET /v1/ready` lists `changeset-rescue` in `features` (a server that
rescues condemned changesets before writing anything that names them). A
keyed `POST /v1/land` whose changeset is not stored is answered from its
land id record, when there is one, with the `Landed` it got the first time,
instead of 404. A land batch that runs past 60 s answers its lands 409
(retry). No `/v1` field, `Changeset`, `TreeNode`, hash or other encoding
changed.

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
tables `changesets`, `changeset_dicts` (from schema 3), `store_features`
(from schema 4), `refs`, `derived`,
`ref_log` hold the rows of one read transaction, byte for byte, including
every `derived` kind (signatures, reviews and GC rows too) and compressed
changeset rows with the dictionaries they need. It is a single file (journal mode `DELETE`, no `-wal`).
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

## Page cursors (additive API fields, no persisted change)

`LogResponse`, `SearchResponse`, `RefLogResponse`, `FilesResponse` and
`DiffResponse` gain an optional `next` (`#[serde(default)]`, omitted when
absent), and `GET /v1/log/{id}`, `GET /v1/search/{rev}`,
`GET /v1/reflog[/{name}]`, `GET /v1/files/{id}` and `GET /v1/diff` accept
`after=<cursor>`.
A response carries `next` when the page stopped before the end; the same
request plus `after=<next>` returns the following page, and concatenated pages
equal one uncapped answer. Requests without `after` answer exactly as before
apart from the extra field, so old clients are unaffected; old servers never
send `next`, so new clients stop after one page.

The cursor is opaque text, at most 4096 characters of base64url without
padding (`leep_derive::MAX_CURSOR_CHARS`). Clients must not parse or build
one. Today's layout (version 1) is `version (1 byte) | endpoint kind (1: log,
2: search, 3: ref log, 4: files, 5: diff) | fingerprint (16 bytes: the first bytes of BLAKE3 over
the kind and the postcard encoding of every query parameter that changes the
answer, but not `limit`) | postcard body`:

- log: either the walk's queue when the page ended (`Frontier`: up to 64
  changeset ids, each with the followed file's name for `follow=true` and the
  merge-side bound of the `log-filter-v1` walk, plus whether that walk had
  switched to the index) or, when the queue is wider, `Below { generation, id
  }` of the last entry, which restarts from the tip and skips what ranks at or
  above it. Every log walk pops changesets in descending `(generation, id)` and
  queues only ancestors of what it popped, so a walk seeded with the queue
  yields exactly the rest of the original walk. A page answered at an older
  changeset under `Leep-Indexing: stale-ok` wraps either in `Pinned { at,
  inner }` (ADR 0058), so later pages continue the walk of `at`; servers
  without it refuse such a cursor as malformed.
- search: the changeset searched (pinned, so later pages read the same tree
  even if `rev` moved) and the position after the last match kept (`path`,
  `line`), or after the last file finished (`line = u64::MAX`) when the scan
  budget cut the page short.
- ref log: the `seq` bound (`before`) of the next page.
- files and diff: the last path of the page (a postcard string); the next
  page holds the paths after it in byte order. The fingerprint covers the
  changeset id and the prefix with its `/`s trimmed (files), or `from`, `to`,
  the trimmed prefix and the rename threshold when `renames=true`, else none
  (diff). A path too long to fit a cursor cannot be paged past: that page is
  400 with code `cursor_unrepresentable` (`CURSOR_UNREPRESENTABLE_MARKER`
  ends its `error`), and the client repeats the request without `limit`.

`GET /v1/files/{id}` and `GET /v1/diff` (added later than the others) page
only when the request has `limit`; without it they answer the whole list in
one response, exactly as before, so a client that predates paging is never
cut short. `limit` above 10,000 (`leep_core::api::MAX_PATHS_LIMIT`) is
lowered to it, and 0 or a non-number is 400 `invalid`. `files` and diff
`entries` are sorted by path (byte order), as they always were. With
`renames=true`, renames are detected over the whole diff on every page and
each one is on the page that holds its `to` entry, so concatenated pages
(entries, renames, and `renames_truncated`, which every page repeats) equal
the unpaged answer.
Each such page therefore costs a full walk and rename detection of the
whole diff, so paging a renames diff of N pages costs about N unpaged
requests; the CLI and MCP server ask for renames diffs without `limit`, and
paging them is for UIs that want the first page early.

The server treats a cursor as untrusted: it checks the length before
decoding, then the version, kind and fingerprint, decodes the body strictly
(no trailing bytes), and checks that named changesets exist and that a log
queue is no wider than 64 entries and matches the query's walk (names only
with `follow`, bounds only from the indexed walk). Any failure is 400 with an
`error` that mentions the cursor; the client restarts without `after`. A
change to the layout bumps the version byte, so cursors from an older server
are refused rather than misread. `before` and `after` together on the ref log
are 400.

## Log summary entries (additive API parameter, no persisted change)

`GET /v1/log/{id}` accepts `fields=full` (the default, unchanged byte for
byte) or `fields=summary`; any other value is 400. A summary answers
`LogSummaryResponse`, `LogResponse` with each entry a `LogSummaryEntry`:
`{"id", "parents", "author", "timestamp", "message_first_line", "agent",
"files_changed", "generation", "root_tree"}` plus `path_at` and `renamed`
when a full entry would have them. `message_first_line` and `agent` (only the
`agent.*` keys of `extra`) are as in blame's `ChangesetSummary`;
`files_changed` is the number of paths in `changes`. `next`, `answered_at` and `behind`
are as in `LogResponse`, and a cursor from either shape continues the other
(`fields` is not part of the cursor fingerprint). Servers that predate the
parameter ignore it and send full entries, which clients project.

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

## Read class refusals (additive header, ADR 0060)

Nothing persisted changed, and no field, code or route was added. A server
may refuse `GET /v1/search/{rev}` when the search class is at its
concurrency limit (`--read-concurrency`): `503`, code `unavailable`, the
usual `ErrorResponse` body, `Retry-After` (whole seconds, 1 to 10), and the
new response header `Leep-Refused: <reason>`.

- `Leep-Refused` means a Leep server refused the request before doing any
  work, so it may be sent again unchanged, even a request that is not
  idempotent. Only a Leep server sets it, and only on such a refusal; a
  `503` without it, which a proxy or balancer may also send, promises
  nothing.
- Values: `read-class` (an expensive read whose class is full),
  `admission` (a land refused because its ref's queue is full, see
  [Land admission](#land-admission-additive-header-value-adr-0060)) and
  `body-budget` (the in-flight body budget's `503`, `--max-inflight-bytes`).
  Clients should treat any value as "refused before any work" and must
  not depend on the list being closed.
- A follower that relays a forwarded land's answer (ADR 0056) copies
  `Leep-Refused` along with `Content-Type` and `Retry-After`.
- `leep` retries a `GET` answered this way like a `429`: at most four tries,
  waiting `Retry-After` plus up to 50% jitter, and reporting a wait over
  10 s instead of sleeping. Older clients report the `503` as a server
  error, whose message says to retry.

## Land admission (additive header value, ADR 0060)

Nothing persisted changed, and no field, code or route was added. A server
started with `--land-queue-batches N` (default `off`) refuses
`POST /v1/land` when the land would wait behind more than `N` batches of
its ref's queue, its own included: `503`, code `unavailable`, the usual
`ErrorResponse` body, `Retry-After` (whole seconds, 1 to 10) and
`Leep-Refused: admission`.

- The refusal comes before the changeset is read and before anything is
  queued or recorded, so the land id stays free and the land may be sent
  again, with or without a land id. A keyed land whose id the ref already
  recorded is answered from the record (`200`, or `400`
  `land_id_reused`) instead of being refused.
- Only the server that batches the ref's lands refuses: the land lease
  holder under `--land-coordination on`. A follower relays the leader's
  `503` with its `Retry-After` and `Leep-Refused`.
- `leep land` sends the land again after `Retry-After` plus up to 50%
  jitter, under the same land id, at most five times or 60 s of waiting.
  The MCP server returns a retryable tool error with `retry_after` instead
  of waiting. Older clients report the `503` as a server error; they resend
  a keyed land after it as after any `5xx`, and never resend an unkeyed one.
- The body budget's `503` now carries `Leep-Refused: body-budget` too.

## Error codes (additive API field, no persisted change)

`ErrorResponse` gains `code`, and `AmbiguousPrefixResponse` too: a string
omitted when unset and absent from older servers. Every error body a server
of this version sends carries one, including the answers that used to be
plain text or empty (the global body cap's `413`, the request timeout's
`408`, an unknown route's `404`, a `405`), which are now `ErrorResponse`
JSON with the same status. `error` stays as before. The codes are a stable
contract: a code keeps its meaning forever, new ones may be added, and a
client treats an unknown or absent code by its HTTP status.

| Status | `code` | Condition |
|---|---|---|
| 400 | `needs_review` | a land or ref move onto a ref whose review policy some changeset does not meet |
| 400 | `not_signed` | a land or ref move onto a ref that needs every changeset signed by a trusted key |
| 400 | `not_forward` | a move of a land-policy-protected ref, or of a protected ref (`--protect-ref`), that is not a fast-forward |
| 400 | `tag_immutable` | a move of a tag, or a land onto one |
| 400 | `land_id_reused` | a land id the ref already recorded for another changeset (ADR 0057); `land` carries the record |
| 400 | `invalid_name` | a ref name the validator refuses |
| 400 | `ambiguous_prefix` | a short id matching several changesets (`AmbiguousPrefixResponse`) |
| 400 | `cursor_unrepresentable` | a paged `GET /v1/files` or `/v1/diff` whose page would end at a path too long for a cursor (added with file and diff paging): repeat the request without `limit` |
| 400 | `invalid` | any other bad request (also `405`) |
| 401 | `unauthorized` | no token, or an unknown one |
| 401 | `token_expired` | a known token past its `expires` (ADR 0007): fetch a new one; retrying will not help |
| 403 | `forbidden` | a `read` token on a write, or a `"review": false` token posting a review |
| 404 | `ref_missing` | a land onto a missing ref with `create: false` (ADR 0036) |
| 404 | `not_found` | anything else that does not exist |
| 408 | `timeout` | the request timeout ran out; the request may or may not have applied |
| 409 | `cas_lost` | a concurrent update won repeatedly (a land that kept losing); read again and retry |
| 409 | `forward_failed` | the land could not reach the server coordinating its ref; nothing landed here, retry (ADR 0056) |
| 413 | `too_large` | the body is over the server's or the token's limit (`--max-body-bytes`, `--max-blob-bytes`, `--max-json-body-bytes`, `max_body`) |
| 429 | `rate_limited` | a rate or concurrency limit, or a token over its share of the body budget; see `Retry-After` |
| 500 | `internal` | the server failed |
| 503 | `indexing` | the index is too far behind; see `indexing` and `Retry-After` (ADR 0058) |
| 503 | `not_leader` | the server no longer coordinates lands on that ref; send the land again (ADR 0056) |
| 503 | `unavailable` | overloaded or busy, including request bodies in flight at the server's limit (`--max-inflight-bytes`), an expensive read whose class is full (`--read-concurrency`, ADR 0060) and a land whose ref's queue is full (`--land-queue-batches`, ADR 0060), with `Retry-After` when known: back off and retry. With `Leep-Refused` the request was refused before any work and may be sent again, an unkeyed land included; without it nothing is promised |

A lost `POST /v1/refs/{name}` swap is unchanged: `409` with `CasRefResponse`
(`ok: false`), not an error body. The marker texts (`NEEDS_REVIEW_MARKER`,
`NOT_SIGNED_MARKER`, `NOT_FORWARD_MARKER`, `MISSING_REF_MARKER`) stay in the
messages, so clients that match them keep working; the CLI and the MCP server
go by `code` when it is present and by the markers otherwise. The constants
are `leep_core::api::error_code`.

`ErrorResponse` later gained `land` (`LandRefusal`), set only on a 400
`land_id_reused` and omitted otherwise: `{"land_id": "<hex>", "changeset":
"<id>", "landed": "<id>"}`, the land id, the changeset the ref recorded for
it, and the changeset the ref moved to for that land (omitted when unknown).
Clients read it instead of parsing `error`, whose text is unchanged; older
servers never send it, and older clients ignore it. Nothing persisted
changed.

## Request ids (additive header and API field, no persisted change)

Every request may carry `X-Request-Id`. The server keeps it when it is 1 to
128 characters of `A-Z a-z 0-9 . _ : / -`, else it replaces it with 32 random lowercase hex digits; a request without one gets a
random one too. Every response carries the id it used in `X-Request-Id`, and
every JSON error body (`ErrorResponse`, `AmbiguousPrefixResponse`) gains
`request_id`, omitted by older servers. The server logs each request under it
(OPERATIONS.md, "Logs") and passes it on when it forwards a land to the
server coordinating the ref. Success bodies are unchanged. Clients that send
nothing see only the new header and field.
