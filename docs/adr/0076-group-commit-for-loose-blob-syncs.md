# 0076. Group commit for loose blob syncs
Status: Accepted
Date: 2026-11-02

## Context
At 2,000 swarm agents one two-vCPU `leep-server` is CPU-bound on basic
operations. A `perf` profile showed 43% of its CPU time in the kernel, about
12 points of it filesystem and block-cache work, and a run with Postgres
pinned to other cores showed the ceiling is Leep's own, not contention for
the host. Uploads (`PUT /v1/blobs/{hash}`, about 150 a second) each write a
loose file in the filesystem store and fsync it on their own before the
rename that publishes it. The directory fsyncs were already deferred to a
flush (ADR 0044, ADR 0048) and land batches already write one pack
(ADR 0068); the file fsync per upload was the one per-request sync left.

What the fsync buys is the store's first invariant: *a blob is under its
final name only once its bytes are durable*, so a power loss never leaves a
torn file at an address. It does not make the upload durable on its own: the
rename is synced by the flush of the create that names the blob (ADR 0044).

## Decision
- **Group commit in `FsBlobStore` (`groupsync.rs`).** A put writes its temp
  file as before, then hands it to the store's `GroupSync`:
  - If no other put of this store is in flight (writing its file) or
    queued, it syncs its own file and renames it at once, on its own
    blocking thread: exactly the old path, with no wait.
  - Otherwise it joins the open batch, or opens one and spawns its batch
    task. The task waits out the window (or until 256 files have queued),
    then takes the shared-sync lock, and only then takes the queue, so files
    that arrive while the previous batch syncs join this one. It makes the
    batch durable, renames each file into place, marks each shard for the
    next flush, and answers each put.
  - **How a batch is made durable.** On Linux 5.8 and later, one `syncfs` on
    a descriptor of the store root, opened before the first file it covers
    was written. Elsewhere, on older kernels, and for a batch of one, an
    fsync per file, so the window still batches the renames but saves no
    syncs. A queued file's descriptor is closed when `syncfs` is in use, so
    a backlog does not hold one descriptor per put; where it is not, a put
    that finds a full batch queued syncs its own file instead of queueing.
  - **Errors.** Since 5.8 `syncfs` reports a writeback error raised on the
    file system after the descriptor was opened, once per descriptor. Two
    batches share the descriptor, so one batch's sync can take the error
    that was the other's file. Shared syncs run one at a time under a lock,
    and one that fails bumps a failure count under that lock; every file
    records the count when it is created, and a batch fails a file whose
    count changed by the time its own sync returned. A failed sync fails
    every put of its batch, removes their temp files and publishes nothing;
    the client retries the upload.
- **`--blob-fsync-window` / `LEEP_BLOB_FSYNC_WINDOW`**, default `2ms`, at
  most `100ms`; `0` restores the old path for every put. The library
  default (`FsBlobStore::new`) stays 0; the server sets the flag. Object
  stores do not fsync and are unaffected.
- **Why 2 ms.** A put only waits when another is in flight, and then for at
  most the window plus one shared sync. A file or directory fsync on the
  4-vCPU VPS of ADR 0044 cost 1.3 to 2 ms, so a 2 ms window costs a put at
  most about one more fsync of latency, which is what it would have spent on
  its own sync under contention anyway, while at 150 uploads a second (and
  clients uploading `PUT_IN_FLIGHT` blobs at once) a window that long
  usually holds several files. Longer windows add latency to every upload
  with company for a saving that is already more than 99% of the syncs
  under load; 100 ms is the cap because past it an upload's wait would show
  up in create latency.

## Why every invariant still holds
- **Whole blobs at an address (ADR 0029, ADR 0044).** The rename of a file
  happens only after the sync that covers it returned success and, for a
  shared sync, after no shared sync failed since the file was created. A
  crash before the rename leaves a `.tmp-*` file, which is never listed,
  read or counted, as before.
- **A put returns only once its bytes are durable.** A put answers after
  its rename, which follows its sync; a batch completes even when the put
  that waited for it was cancelled, as a cancelled blocking put did before.
- **Directory durability (ADR 0044, ADR 0048).** Unchanged: each rename
  marks its shard after it happens, and `flush`/`flush_blobs` sync the
  marked directories before any changeset naming the blob is stored. An
  upload alone is still not durable; a stored changeset's blobs are.
- **GC fence and rescue (ADR 0012).** `Repo::put_blob` writes the claim and
  the rescue after the store put returns, which is still after the blob is
  at its address. A put that finds the blob stored touches it and returns at
  once, as before, so the grace window counts from the latest upload. The
  window only lengthens the time between writing a file and its rename,
  which a slow fsync could already make as long.
- **Rescue writes (`put_rescuing_many`) and pack thresholds (ADR 0068).**
  `put_many` still decides what is packed; the loose remainder goes through
  `put_with_hash` and so shares syncs, and the flush that follows is
  unchanged. Packs and repack keep their own per-file syncs.
- **Pack-set stamps (ADR 0070).** Loose writes do not touch `packs/` or its
  stamps. A `syncfs` also syncs other dirty files on the file system (a
  pack being written, another process's uploads), which only makes them
  durable sooner.
- **Several processes on one directory.** Each has its own `GroupSync` and
  descriptor; every descriptor sees each writeback error once, so one
  process's sync cannot hide another's error.

## Consequences
- With the default window, concurrent uploads issue one sync per batch
  instead of one per upload. 1,000 concurrent 512-byte puts on the
  development VM's ext4 (`measure_fsync_window`, release build, three
  rounds): 0.002 to 0.008 syncs per put against 1.000; puts per second
  within noise (7,300 to 12,600 against 7,000 to 10,300), because an fsync
  on that VM's disk costs microseconds. At 20,000 puts, 4,700 to 5,300 puts
  a second against 3,300 to 4,500, and 230 to 238 µs of system CPU per put
  against 273 to 286. The swarm host, whose fsyncs reach a real device
  flush, should gain more; the next swarm run should confirm it.
- A `syncfs` writes back every dirty file on the store's file system, not
  only the batch. Where the blob directory shares a file system with
  something that keeps much dirty data (a Postgres data directory between
  checkpoints), a shared sync waits for that too. Put blobs on their own
  file system, or set `--blob-fsync-window 0`.
- A writeback error anywhere on that file system fails the batch whose sync
  reports it, even when the failed file was not a blob: uploads fail and are
  retried rather than risk a torn blob.
- Unit tests check that concurrent puts share syncs, that no put returns
  and no blob appears under its name before its batch's sync, that a failed
  batch sync fails its puts and leaves nothing behind, that a lone put does
  not wait, and that abandoned puts never publish a partial blob with a
  window. The server's e2e and crash tests run with the default window.
