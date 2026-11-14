### The server compresses new file versions by itself
- **Added.** The background repack now also runs the history-ordered
  delta repack (ADR 0100): once 1,024 objects or 32 MiB wait (loose
  objects, or packs written whole by land batches and imports), or any
  have waited 30 minutes, it walks their history and rewrites them as
  deltas against their previous versions, on its low-priority thread.
  Lands never wait for it. New flags `--auto-repack`,
  `--auto-repack-objects`, `--auto-repack-bytes`, `--auto-repack-max-age`,
  `--auto-repack-check-interval`, `--auto-repack-batch-bytes`, `--auto-repack-level` and
  `--auto-repack-object-store-interval` (`LEEP_AUTO_REPACK*`); new metrics
  `leep_repack_auto_*` and `leep_blob_store_{bytes,logical_bytes,size_ratio}`.
  A backlog runs in batches (`--auto-repack-batch-bytes`, 1 GiB) with
  merges and cleanups between them; packs already compressed are listed in
  `packs/considered`. Object stores get a full repack instead, once a
  quarter of the store waits, at most once a day (kept in
  `packs/.last-full`). Run `leep-server repack --full` once after
  upgrading an existing store.
- **Changed.** `leep-server repack` (incremental) compresses packs written
  whole by land batches and imports instead of copying their frames.
- **Changed.** Bytes waiting for a background cleanup are capped at half
  the live store (at least 64 MiB, at most the old 4 GiB; merges under
  64 MiB exempt); the fence is unchanged.
- **Docs.** OPERATIONS.md: a nightly `repack` cron is no longer needed.
