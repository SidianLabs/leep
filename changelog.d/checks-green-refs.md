### Known-good refs and CI check results
- **Added.** CI posts check results with `POST /v1/changesets/{id}/checks`
  (`leep checks post REV CHECK pass|fail`) through a token whose new
  `"checks"` entry lists the check names it may post; the creator of a
  changeset cannot attest it. Results are stored as `check-v1` attestations
  (ADR 0091).
- **Added.** `--checks-policy-file` names the checks each ref requires. The
  server moves `green/<ref>` forward, and only by promotion, to the newest
  changeset on the ref's first-parent chain that passes all of them. Land,
  raw moves, undo and delete of `green/<ref>` answer 422 `promotion_only`,
  and a ref in the policy becomes forward-only. The policy reloads on SIGHUP,
  and green catches up when the policy changes.
- **Added.** `GET /v1/refs/{name}/checks` (`leep checks ref`) shows green,
  how far behind it is and whether the head is red, and
  `GET /v1/refs/{name}/culprits?bad=&good=` (`leep checks culprits`) lists
  the suspects: the lands in the range, never a verdict. Also
  `leep checks status` and the MCP tools `leep_checks_status` and
  `leep_checks_culprits`.
