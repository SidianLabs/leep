### Review verdicts carry across the client's rebase of a reviewed change
- **Added.** An approval (or reject) of a changeset now also carries to a
  client's conflict resolution naming it where a reviewed file's base moved,
  when the file is exactly what the server's own rebase of the reviewed
  change would make there, or keeps both sides' lines where both only added
  lines at one place (two tests appended to one module), in either order.
  Text files only, at most 2 MiB a side, never `OWNERS`, at most 32 merged
  paths per original, and only identical changes on strict land refs
  (ADR 0089). Review requests close by the same rule.
- **Added.** `GET .../review-status` answers `origins` (the originals it
  compared, their verdicts, and why one did not carry) and each path's
  `carried_rule` (`identical`, `rebased`, `union`); review request entries
  gain `paths` and `changed`. All optional; older clients ignore them.
- **Added.** `leep review status` prints `replaces X, approved by rhea;
  not carried: PATH differs (leep diff X; leep diff Y)` or `; carried`, and
  says how a carried path matched. `leep review inbox --wait[=SECS]` waits
  until a request is listed (exit 0) or time runs out (exit 1), and entries
  list the changeset's files.
- **Changed.** `leep land` after a server rebase prints `rebased from X;
  reviews of the original count`; a conflicted land says which reviews will
  carry; a land refused for review names the refused changeset in its
  `leep review request` hint. `leep commit` on a terminal prints one line
  (`committed ID (N files)`); piped output is unchanged.
- **Docs.** `changelog.d/README.md` says how to name a fragment without a
  review ref.
