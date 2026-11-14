### The server compresses changeset rows by itself, and storage size is guarded in CI
- **Added.** Automatic changeset compaction (ADR 0101): what
  `leep-server compact-changesets` does, run by the server in the
  background on a thread at the lowest CPU priority with one database
  connection of its own. It trains a 16 KiB dictionary once a store has
  100 changesets, 60 KiB at 1,000 and 64 KiB at 10,000, stores a first one
  only if it saves at least 5% counting its own size (a replacement 12%),
  and compresses the rows not yet on the newest dictionary, one server at
  a time (a Postgres advisory lock). A dictionary over 60 KiB, such as one
  `compact-changesets` trains by default, is never replaced. New flags `--auto-compact`,
  `--auto-compact-interval` (10m), `--auto-compact-min-rows` (100) and
  `--auto-compact-batch` (`LEEP_AUTO_COMPACT*`); new metrics
  `leep_changeset_compact_*` and `leep_changeset_dictionary`. Corpus B's
  changeset rows go from 3.41 MB to 1.06 MB. On Postgres, rows written
  before the first dictionary (an import) leave their old versions as free
  space that later rows reuse; `VACUUM FULL changesets` returns it.
- **Changed.** **Upgrade note:** about 2 to 3 minutes after it first
  starts, a server of this release records the `changeset-zstd-dict`
  format feature and stores a dictionary on its own, so no binary older
  than ADR 0053 may still use the database; start the
  new servers with `--auto-compact off` until such a rollout is done.
- **Changed.** The pack writer's delta chain cap is 63 instead of 32, so a
  file edited hundreds of times stores a whole version every 64 instead of
  every 33 (blob store growth per land 2,198 to 1,515 bytes on corpus C);
  every release since ADR 0029 reads such packs. Readers now
  accept chains of up to 255, for a later, higher cap.
- **Tests.** `cargo test -p leep-e2e --test storage_size` checks a default
  server's settled blob and database bytes per land, its total against
  `git gc`, and `leep_blob_store_size_ratio` against budgets
  (PERFORMANCE.md, "Storage size guard"); `leep-soak
  --max-blob-store-ratio` (0.9) bounds the ratio in soaks, whose server
  now runs with `--auto-repack-max-age 2m`; the crash test
  kills servers during automatic compaction.
