### Code search stores blob rows as patches and retires old ones
- **Changed.** The search index's blob rows are a new kind,
  `blob-trigrams-v2`: a file version the warmer derives after its previous
  version is stored as a patch on the keyframe that version's row is on
  (`blob-trigrams-keyframe-v2`), with a new keyframe when the patch passes
  `patch² <= 128 × whole`, as ADR 0094 does for directory filters. A probe
  reads at most two rows, and keyframes are cached. Whole rows keep the
  `blob-trigrams-v1` bytes, and a blob without a v2 row is read from its v1
  row, so an upgrade needs no downtime and no warming (ADR 0102).
- **Added.** `--search-retention` (default `14d`, `off` to keep every row)
  and `--search-retention-interval` (default `1h`): one server at a time
  deletes the blob rows of blobs no ref head has held within the window (the
  ref log names every head of the window) and a search derives a deleted
  row again on its first probe. It runs in the index lane, rereads the refs
  around each delete and derives again at once a row a ref that moved
  meanwhile holds (retried by the next pass if that fails). Each pass also
  backfills, a bounded number at a time, the missing rows of current heads
  whose directory filters were reused from old content (a branch from an
  old commit, a revert, a rollback). Liveness is a Bloom filter, and a pass
  with nothing new to do is skipped. Metrics `leep_search_retention_*`.
- **Changed.** `repair drop --kind blob-trigrams-v1` and `upgrade-index
  --drop-retired` copy v1 rows without a v2 row into `blob-trigrams-v2`
  first, so files unchanged since the upgrade keep their rows; `verify`
  checks v1 rows while any exist.
- **Changed.** A bounded query (ADR 0058) stores the blob rows of the files
  it probes and computes, so a search of an old revision whose rows were
  retired derives each one once rather than on every retry.
- **Added.** `MetaStore::list_derived_keys` and `MetaStore::derived_present`,
  which list and test keys of a kind without reading values.
- **Changed.** Database growth per land halves on Postgres, 27.5 KB to
  13.7 KB over 400 CLI lands, the blob rows and keyframes going from 13.5 KB
  to 1.1 KB a land. The `storage_size` guard's database budgets are
  tightened and it now also bounds the blob rows per land and counts the
  patches of lands paced to the warmer.
- **Docs.** ADR 0102; ARCHITECTURE, FORMATS, OPERATIONS ("Search row
  retention", "Upgrading") and PERFORMANCE ("Blob trigram patches and
  retention").
