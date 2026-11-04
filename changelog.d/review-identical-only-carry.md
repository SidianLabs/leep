### Paths whose review verdicts carry only to identical changes
- **Added.** An `OWNERS` line `carry identical` keeps review verdicts in
  that directory from carrying to a client's rebase or union of a reviewed
  change; only an identical change carries there, in every directory below
  up to a `noparent` file (a subdirectory's `carry merged` cannot undo
  it). A misspelled `carry` line in an `OWNERS` file is refused at land
  and shown by `leep review status`, never read as an owner name. The
  review policy file's new `identical_only_globs` does the same per glob.
  Both are read from the target ref's head and can only make a path
  stricter, never undo a strict ref or each other (ADR 0092). Review
  requests close by the same rule; taking a verdict back is unchanged.
- **Added.** `review-status` answers each path's `carry` (`identical` or
  `merged`), and `leep review status` says `not carried: P is
  identical-only (src/OWNERS)`, naming the glob or the strict ref when one
  decided. Optional; older clients ignore it.
- **Changed.** Upgrade every server before adding `identical_only_globs`
  (an older server refuses the policy file) or `carry` lines to `OWNERS`
  files (an older server reads them as an owner name).
