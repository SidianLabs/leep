### Git ids derived from Leep objects, and one commit style per store
- **Added.** The server derives the id a git client would use for every
  blob, tree and changeset (`git-blob-v1`, `git-tree-v1`, `git-commit-v1`),
  and a reverse index from git ids (`git-oid-v1`), as pure functions of
  content: imported history derives to its original shas with no recorded
  mapping. First step of the git wire protocol (ADR 0090).
- **Added.** `GET`/`POST /v1/git/style`: the store's git commit style
  (`plain` or `trailer`, optionally `+provenance`), recorded once. The
  mirror records its flags on first start and refuses to start against
  another style; `leep-git export` follows a recorded style.
- **Changed.** The git object codec moved from `leep-git` into a new crate,
  `leep-gitobj`, shared by the importer, the exporter, the mirror and the
  server. The importer's and the mirror's commit bytes are unchanged.
- **Changed.** `leep-git export` of a store that recorded its commit style
  writes commits under it: under `trailer`, changesets made in Leep get a
  `Leep-Changeset:` trailer they did not get before. Without a recorded
  style its output is as before.
- **Added.** `MetaStore::put_derived_if_absent`, a write that keeps the
  first row; `repair drop --kind git-style-v1` undoes a wrong style with
  every git id derived under it.
