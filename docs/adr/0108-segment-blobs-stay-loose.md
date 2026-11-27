# 0108. Segment blobs stay loose
Status: Accepted
Date: 2026-11-27

Amends [0100](0100-automatic-delta-repack.md) (what a delta run packs and
counts as waiting) and [0107](0107-consolidating-delta-repack.md) (its
follow-up on segment blobs). Storage PR 7, stage 2.

## Context
The server writes segment blobs to the blob store: derived-data segments
(ADR 0104, index and data blobs) and GC mark segments (ADR 0103). Each is
an ordinary blob whose bytes start with an object prefix
(`leep:derived-segment-index-v1\0`, `leep:derived-segment-data-v1\0`,
`leep:gc-mark-segment-v1\0`), written loose with `put_with_hash`. They are
compressed already, never a delta base (no walk lists them), and
short-lived: a compaction or a later mark retires them and GC deletes them.

Until now every repack packed them like any loose object. Packed, a GC
delete only tombstones an object; its bytes stay in the pack until a merge
drops them, which for a delta pack meant a consolidation (ADR 0107) or
`repack --full`. On the land-driven corpus about 1 MB of dead segment bytes
sat in packs this way.

## Decision

### 1. Repacks leave them loose
`RepackOptions::leave_loose` names byte prefixes. A repack (any mode that
packs loose objects, on filesystem and object stores alike) does not pack
a loose object whose hash its hints do not list and whose bytes start with
one of them: the object is not in the new pack, its loose file is not
deleted, and the report names it (`left_loose`). Deciding reads only the
first bytes of each unlisted loose object (one short file read, or one
ranged GET on an object store).

`leep-store` does not depend on `leep-derive`, so it does not know the
segment kinds: the server passes `leep_derive::segment_blob_prefixes()` to
the delta runs, the object store's scheduled full repack and
`leep-server repack`. Merges of packs alone take no loose object and are
unchanged. Segment blobs packed before this change stay in their packs
until GC deletes them and a merge drops them (section 3).

### 2. They never wait
The background task reads the first bytes of each loose object once (its
bytes never change; the classification is kept while the object is
listed) and leaves those that start with a prefix out of the objects and
bytes waiting for a delta run, as it does corrupt loose objects. So segment
blobs alone never start a run, on a threshold or on `max_age`. For a run
that starts anyway, the walk's `wanted` includes them, and those it lists
join the batch.

### 3. Dead bytes of delta packs count toward the consolidation gain
As ADR 0107 section 4 already decided: the stored bytes of tombstoned
objects in the delta packs count toward the gain, so they reach the ratio
rule (an eighth of the packs' bytes) or, smaller, the age rule (a 64th and
the age), and history objects GC deleted leave the packs in bounded time.
This stage adds tests that hold it.

### 4. Content addressing is unchanged
Only where an object lives changes, never its bytes or address. A file
that starts with a segment prefix (refused at upload since ADR 0041, but a
store may hold one written before) is packed whenever a walk lists it,
since only unlisted objects are left loose; one no walk lists merely stays
loose, and reads back the same either way.

### 5. Unchanged
No persisted or wire format changes: no new file, index, frame, schema,
derived kind or API field. Loose objects are an existing form of every
object, read, listed, backed up and deleted as before. Old and new servers
share a store: an old server's repack packs the segment blobs, as before,
and a new one leaves the ones written since loose. Crash safety is
untouched: an object left loose is never moved aside or deleted by the
repack, so a crash at any step leaves it where it was.

## Consequences

- GC deletes a segment blob's bytes at once (an unlink, or one DELETE),
  instead of leaving them in a pack until a merge. On the land-driven
  corpus the design estimated this at about 1 MB of the 15.6 MB total.
- Segment blobs add files to the loose listing at each check: tens on a
  small store, more at scale (one per derived seal and GC checkpoint, until
  GC deletes them), and one short read per new loose object.
- A store with many loose segment blobs and nothing else loose never runs
  a delta repack for them; `leep-server verify` and backups see them as the
  loose objects they are.
- A file that starts with a segment prefix and that history names is not
  counted as waiting either, so on an old store that holds one it is packed
  only by a run something else starts (or by `leep-server repack`); one
  past the walk limit stays loose.
- A repack no longer reads a loose segment blob, so it no longer reports a
  damaged one whose first bytes still match as corrupt; `leep-server verify
  --full --rehash` and readers still find it.
- A full repack's `loose_after` counts the segment blobs it left loose
  (`left_loose`), so it is no longer zero on a server that writes them.
- A loose file the task cannot read counts as waiting, is logged, and is
  read again at the next check.
