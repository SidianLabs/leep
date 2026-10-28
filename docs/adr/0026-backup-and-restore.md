# 0026. Backup and restore
Status: Accepted. Amended 2026-10-28 (issue #155): changesets can be
deleted since ADR 0059, so the pool keeps every object any snapshot ever
copied, packed ones included, and nothing the source deletes is hidden in it.
Date: 2026-10-16

## Context
A Leep deployment keeps its state in two places: metadata (changesets, refs,
derived data including signatures, reviews and GC bookkeeping, and the ref
log) in SQLite or Postgres, and blobs (file contents and tree nodes) in a
filesystem or S3-style store. Until now the advice was "copy both":
`sqlite3 .backup` or `pg_dump`, and `rsync` or `aws s3 sync`, with the warning
that metadata which names a missing blob is unusable. That leaves four gaps,
and M6 asks for a backup that is tested to restore:

1. **Nothing makes the two halves agree.** They are copied by different tools
   at different times, while the server keeps landing and GC keeps deleting.
   Whether the result restores depends on an ordering the operator must get
   right and cannot check.
2. **Nothing checks a backup.** A backup that is missing a blob is found out
   during the disaster.
3. **The SQL backups are not portable or inspectable.** A `pg_dump` can only be
   read by Postgres of a matching version; Leep cannot verify it without
   restoring it.
4. **Restore is untested**, and has no refusal rules: nothing stops a restore
   over a live database, or from a newer schema (ADR 0023).

Blobs make the first one tractable: they are content-addressed and immutable,
and ADR 0012 says when one may be deleted. This ADR decides what a consistent
backup is, how to take and check it while servers and GC run, and how to
restore it.

## Decision
`leep-server` gains three subcommands, built on a metadata snapshot in
`leep-meta` and an incremental blob copy in `leep-store`:

```sh
leep-server backup        --out DIR [--blobs-to URL | --no-blobs] [--no-check] [--rehash] ...
leep-server restore       --from DIR [--snapshot NAME] [--blobs-from URL] [--force | --resume] ...
leep-server backup-verify --from DIR [--snapshot NAME] [--blobs-from URL] [--rehash]
```
`backup` reads `--meta`/`--blobs` as the source; `restore` writes them as the
target. Like `gc`, `verify` and `repair`, they open the stores directly, need
no downtime, and never create a source that is missing. Exit status: 0 done
and clean, 1 done but the check found problems, 2 could not run.

### Layout
```
DIR/blobs/<2 hex>/<62 hex>              one pool, shared by every snapshot, only ever added to
DIR/snapshots/<UTC stamp>/meta.db       a Leep metadata database as of one instant
DIR/snapshots/<UTC stamp>/manifest.json written last; its presence is what makes the snapshot exist
```
`--blobs-to` puts the pool in any blob store URL (`s3://...`). The manifest
(FORMATS.md) records the schema version, the BLAKE3 and size of `meta.db`,
row counts, every ref tip, the highest ref log `seq`, the Leep version, the
snapshot instant, blob statistics and the result of the check. A snapshot
directory without a manifest is an interrupted run: readers ignore it and an
operator may delete it.

### Metadata: one read transaction, written as a SQLite file
`leep_meta::snapshot::export_snapshot` opens a single read transaction on the
source (`REPEATABLE READ, READ ONLY` on Postgres; a WAL read transaction on
SQLite, whose first statement fixes the snapshot) and streams the four tables
through it into a new SQLite database with the normal schema, in pages of 500
rows. Every row was committed at one instant, so a ref, its ref log entry and
the changesets it names are together. The file is written as `meta.db.partial`
in WAL mode, converted to a single self-contained file, fsynced and renamed.
The highest ref log `seq` ever issued is carried across (SQLite's
`sqlite_sequence`, Postgres's sequence value), so a restored store never reuses
a number even if the newest entries had been pruned.

Why a SQLite file for both backends, instead of `VACUUM INTO` and `pg_dump`:

- One format and one code path. A snapshot taken from Postgres restores into
  SQLite and the other way round (tested), which also makes the backup the
  supported way to move between backends.
- It is a Leep metadata database. `leep-server verify` and the server open it
  unchanged, so a backup can be checked without restoring it and without
  Postgres tooling or a matching Postgres version.
- Schema versioning is ADR 0023's, not a second mechanism: the file carries
  `schema_migrations`.
- No external binary on the host and no credentials to a second tool.

What it costs: the copy is one stream, as slow as a sequential read of the
metadata and a write of the file, and a long read transaction holds back
Postgres vacuum and SQLite checkpoints for its length. For metadata too large
for that (many GB), Postgres's own base backups and WAL archiving remain the
right tool for the metadata half; `backup --no-blobs` then still produces the
manifest, and `backup-verify` is how to check that the blobs match. This is
the same reason blobs in S3 may stay with bucket versioning or replication:
`--no-blobs` exists for operators whose platform already does this better.

The source is never migrated or written: a source with pending migrations is
refused (run `leep-server migrate`; ADR 0023), as is one newer than the binary.

### Blobs: copy what the pool lacks
`leep_store::sync::sync_blobs` merges two ascending listings (the
`BlobStore::list_page` order every backend already guarantees) and copies the
source's blobs that the destination lacks. A blob present under a hash holds
the bytes of that hash, whenever and by whomever it was written, so presence
is proof of content and an unchanged store costs two listings and no reads.
That is what makes incremental backup cheap and repeated, interrupted and
concurrent runs safe. Details:

- Each copied blob is checked against its name (a file blob hashes to it, or a
  tree node decodes and hashes to it). One that does not is reported and not
  copied, so a damaged source blob never becomes a trusted copy.
- A destination blob of a different size from the source's is deleted and
  copied again (a torn object from some other tool).
- A blob listed by the source and gone when read was deleted by GC meanwhile;
  it is counted (`vanished`) and skipped. By the argument below it is
  unreferenced.
- Blobs only in the pool (deleted from the source since, or needed by an older
  snapshot) stay. The pool never shrinks on its own: to retire old snapshots
  and reclaim space, start a new pool and keep the old one for as long as its
  snapshots must restore (OPERATIONS.md). *Amended 2026-10-28:* this holds
  for packed objects too. A backup never copies the source's pack
  tombstones into the pool (ADR 0039, section 6), and no pool-side prune
  removes an object, so no source deletion reaches a sealed snapshot.

### Consistency
**Invariant (ADR 0012).** A blob or tree node that a stored changeset needs is
in the blob store at every instant after the changeset is stored. ADR 0012's
safety argument shows that no `collect` deletes such a blob: any changeset
stored before the collect's mark protects it by the mark (the mark covers every
stored changeset, reachable or not, and every tree it names), and any stored
after reads the blob as unblocked, which the rescue and doom handshake orders
against the delete. ~~Changesets are never deleted, so the protection is
permanent.~~ *Amended 2026-10-28:* since ADR 0059 the changeset sweep
deletes unreachable changesets, and a later `gc` frees the blobs and tree
nodes only they used. The protection lasts while the changeset is stored,
which covers the snapshot taken while it was (the claim below, and ADR
0059's `--backup-fence` for a delete that races the copy). After that the
source may delete the objects, so an **older** snapshot that names them is
safe only because the pool keeps them: the pool's object set only grows,
for loose and packed objects alike. A backup that hid them in the pool
(as copying the source's pack tombstones did, issue #155) would break that
older snapshot silently, since each run's closure check covers only its
own snapshot.

**Claim.** Take the metadata snapshot at instant T0, then list and copy blobs
afterwards. Every blob the snapshot's changesets need is in the pool when the
copy ends, however GC and uploads interleave with it.

*Proof.* A blob B needed by a changeset in the snapshot was stored before T0
and, by the invariant, is in the source store from T0 on, so it is listed and
readable whenever the copy looks. (Reading before T0 cannot happen: the copy
starts after the snapshot is written.) Therefore it is copied or was already in
the pool. For anything else the copy might lose, nothing in the snapshot needs
it. ∎

Reversing the order breaks this: a changeset landed after the blob listing
needs blobs the listing never saw, and the snapshot would name them. That is
why metadata is first, and it is the only ordering the backup needs. Cases
checked against the GC state:

- **Unreachable changesets.** The snapshot contains every stored changeset and
  GC marks from every stored changeset, so the blobs of an abandoned branch
  survive both. (Tested: an unreachable changeset's tree is in the backup after
  a full GC between the two halves.)
- **Claims.** Claims only keep blobs; they never delete, so rows in the
  snapshot that name blobs absent from the pool do not matter.
- **Condemned and doomed blobs in the snapshot.** A condemned blob is
  unreferenced when condemned, and nothing since can reference it without
  rescuing it first (ADR 0012). If a collect deletes it after T0, the
  snapshot holds the condemnation row and the pool lacks the blob: exactly the
  state of a crashed collect, which the next collect resolves, and the blob is
  reported missing to clients until then. A doom row *deleting* in the snapshot
  with the blob present is a collect that had not finished; same recovery.
- **Uploads in flight.** An uploaded blob no changeset references yet may or may
  not be in the pool. If a changeset that references it lands after T0 it is not
  in the snapshot; if it landed before, the invariant covers it.
- **Pool extras.** Blobs deleted from the source after T0 but copied before are
  merely extra.

**Backup does not hold off GC.** The claim holds with GC running, so a lock
would add no safety. It would add a failure: a backup that died holding it
would stop GC until someone noticed, and it would need a cross-process lease
in the metadata store. The procedure does depend on ADR 0012's own
assumptions (a fence longer than the request timeout, no `--unsafe-no-fence`
under live servers). Instead of trusting them, **the closure check enforces
the claim in code, every run**: after the copy, `backup` walks every changeset
of the snapshot (`verify --full` semantics with no derived-row recomputation)
against the blobs it copied, and checks that each tree node and file blob the
changesets name is present (with `--rehash`, also that the bytes match). The
result is sealed into the manifest. If anything is missing, whatever the
cause (a GC bug, a mis-run `gc --unsafe-no-fence`, an S3 listing that lagged, a
source that was already damaged), `backup` exits 1, the manifest says
`check.ok: false`, `restore` refuses the snapshot unless told
`--accept-failed-check`, and the drill and alerts see it. A passing backup is
therefore proven restorable by this check rather than assumed.
With `--no-blobs` the check runs against the source blobs (the strongest thing
available), and `backup-verify --blobs-from URL` is run again after the blobs
have been copied by whatever tool does it.

**Restore is a recovery from a crash.** The GC rows in a snapshot are state
that ADR 0012 already makes safe to resume from: a restored store is a store
whose servers all died at T0. Condemnations compare GC-host timestamps with
GC-host timestamps, so the age of a backup does not enter; every request that
could have relied on a condemned blob died with the old servers, and every
changeset restored is marked by the next GC. File modification times in a
restored filesystem store are the restore time, so the grace window restarts:
GC will not delete anything for `--grace` after a restore.

### Restore
`leep-server restore` is the same machinery in reverse:

1. Locate the snapshot (the newest sealed one, or `--snapshot NAME`), read the
   manifest, and refuse: a manifest format newer than the binary; a metadata
   schema newer than the binary (ADR 0023, "restore it with a newer
   leep-server"); a `meta.db` whose BLAKE3 or size differs from the manifest; a
   snapshot whose backup-time check failed or was skipped, unless
   `--accept-failed-check`.
2. Refuse a non-empty target before writing anything: metadata that holds any
   changeset, ref, derived row or ref log entry unless `--force`; a non-empty
   blob store unless `--force`, or `--resume` when the metadata is still empty
   (continuing an interrupted restore).
3. Copy the pool's blobs the target lacks (the same incremental copy).
   *Amended 2026-10-28:* the target gets the whole pool, so objects no
   changeset of the snapshot names (deleted from the source after it, or
   needed only by other snapshots) arrive as well, packed or loose, as
   loose pool extras always did. They are unreferenced, and GC frees them
   after its grace. No tombstones are derived per snapshot: nothing in a
   snapshot depends on an object being absent (a doomed blob that is still
   present is a collect that had not finished, above).
4. Load the metadata in **one transaction**. A snapshot from an older schema is
   migrated on a scratch copy and adopted exactly as ADR 0023 describes for a
   database (the backup medium is never opened for writing, and its hash
   stays valid); one from a newer schema was refused in step 1. `--force` deletes
   what was there inside the same transaction. The restored row counts must
   equal the manifest's.
5. Run the closure check on the restored stores (`--no-check` skips it).

Blobs are written before metadata, and metadata atomically, so a restore that
dies halfway leaves a target whose metadata is empty, which is not a repository
yet and cannot be started by mistake; the same command (with `--resume`)
finishes it. `--force` on a Postgres target takes `ACCESS EXCLUSIVE` locks on
the four tables for the length of the load, and the caller must stop servers
first: Leep cannot tell. The restored ref log continues its numbering.

Restore writes the `refs` and `ref_log` rows directly instead of moving each
ref through `MetaStore::cas_ref`. This is not a ref move: the target holds no
refs, the emptiness check and the load run in one transaction that excludes
writers (`BEGIN IMMEDIATE` on SQLite, `ACCESS EXCLUSIVE` on Postgres), and the
refs arrive together with the ref log entries that produced them, so refs and
log agree from the first instant the target has any. Replaying the log
through `cas_ref` would write new entries and lose the original times, actors
and messages.

### The drill
A backup nobody has restored is a hope. `crates/leep-e2e/tests/backup.rs` is
the drill, with the real binaries, for SQLite and (when
`LEEP_TEST_POSTGRES_URL` is set) Postgres. It populates a server with real
history, takes a backup while agents land and `leep-server gc` condemns and
collects in a loop, with a hidden `--pause-after-snapshot` that stretches the
window between the two halves past a full GC cycle so that collects certainly
run inside it, restores into fresh stores, and compares everything a user can
ask with the source at the snapshot's point in time. OPERATIONS.md has the
drill operators run on a schedule: the same steps, with `restore` into scratch
stores and `verify --full --rehash`.

### Metrics
`backup --metrics-file PATH` writes Prometheus text-file metrics (for
node_exporter's textfile collector): last run time, last run ok, last
successful backup time, snapshot instant, duration, check problems and bytes
copied. A failed check carries the previous last-success time forward so an
alert on its age fires.

## Alternatives considered
- **`VACUUM INTO` and `pg_dump` wrapped by a script.** Two formats, two tools
  to keep in step with Leep's schema, no way for Leep to check them, and no
  cross-backend restore. Rejected for the default path; Postgres's native
  tools remain supported for very large metadata (above).
- **Pausing GC for the duration (a lease row, or "do not run `gc collect`").**
  Not needed (the claim above), and it turns a crashed backup into an outage
  of GC. A runbook line "do not run GC during backups" would also have been an
  unenforced procedure, which this ADR set out to avoid.
- **Blobs first, then metadata.** Unsafe, as shown.
- **Copying the filesystem or volume.** Crash-consistent at best across two
  volumes; no check; not portable to S3.
- **A backup endpoint in the HTTP API.** The data is large and the stores are
  already reachable by the operator; it would put a long, privileged stream
  behind request limits and tokens meant for agents.
- **Excluding derived rows that can be recomputed (search trigrams are most of
  a large store).** Smaller backups, but a restored server would answer search
  slowly until `derive-search`, and the exclusion list would need to track
  `repair`'s notion of rederivable. Deferred: the snapshot includes every row.

## What remains
- Memory per copy is blob size times `--concurrency` (the `BlobStore` trait has
  no streaming read). Large blobs need a lower `--concurrency`.
- The metadata copy is one stream and one long transaction.
- Retention is by deleting snapshot directories. Reclaiming pool space needs a
  new pool (or deleting a pool once its snapshots are gone); there is no
  pruning tool. A pool-side prune would have to know which sealed snapshots
  still name each object (a closure walk of every snapshot kept); none
  exists, so the pool keeps everything, including the objects of
  changesets the sweep deleted. A shared pool also means a snapshot is only as good as the
  pool: keep it on durable storage and verify on a schedule.
- No encryption or compression: put the backup on encrypted storage. The
  backup holds all repository content, signatures and review history, and
  **no secrets from the server's configuration**: the tokens file, the signing
  keys file and the review policy are not in the stores and must be backed up
  separately (they are small, and belong in the same place as the rest of the
  deployment's configuration).
- Point in time recovery below the backup interval needs Postgres WAL archiving
  or S3 versioning; Leep's tools give the recovery points they are run at.

## Consequences
- Backups are checked every run and restores are tested by a drill; a backup
  that would not restore fails loudly at backup time.
- No change to any persisted type, the schema, `/v1`, or GC. New: the backup
  directory and manifest (FORMATS.md), `leep-meta::snapshot`,
  `leep-store::sync`, `leep-server::backup`, and three subcommands.
- `leep-meta` exposes the SQLite and Postgres pools to its own snapshot module
  only (crate-private); `leep-cli` and `leep-derive` are unchanged.
- The closure check costs one walk of history per backup; `--no-check` removes
  it at the price of an unchecked snapshot that `restore` refuses by default.
