### Search directory filters stored as patches
- **Changed.** Code search stores each directory's filter in a new kind,
  `tree-trigrams-v2`: as a patch on the whole filter of an earlier version
  of the directory (`tree-trigrams-keyframe-v2`), or as one byte when more
  than a quarter of its bits are set, instead of a whole filter of up to
  64 KiB for every directory on every land's changed paths (ADR 0094). On
  Leep's own history a warmed single-file land grows the database by
  12.9 KB instead of 198 KB, and a three-file land by 44 KB instead of
  459 KB. Answers are unchanged. The search index lease and
  `leep_index_rows_total` now name `tree-trigrams-v2`.
- **Changed.** `verify` and `repair` compare directory filters by the
  filter a row stands for, not its bytes, and rewrite a wrong one whole.
- **Removed.** `tree-trigrams-v1` is retired: current servers neither read
  nor write it. Once no older server runs, `leep-server repair drop --kind
  tree-trigrams-v1` (or `upgrade-index --drop-retired
  --old-servers-stopped`) deletes its rows (OPERATIONS.md, "Upgrading").
