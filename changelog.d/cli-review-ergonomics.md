### The review loop reads as guidance, waits tell "nothing yet" apart, and commits check markers and add trailers
- **Changed.** A land refused for review prints guidance: each path still
  waiting, whom it needs and the `OWNERS` files that named them, without
  the HTTP status or request id line. `--json` keeps the server's message,
  `request_id` and `server_code`, and now names the next commands in `hint`.
- **Changed.** A `review status --wait`, `review request --wait` or
  `review inbox --wait` that runs out of time exits 7, "nothing yet",
  instead of 1, so it differs from a rejection (still 1) and from errors
  (ADR 0034).
- **Added.** `--wait SECS` with a space, and durations `90`, `10m`, `1h`,
  on `review status`, `request` and `inbox`; `--wait REV` still waits for
  `REV`.
- **Added.** `leep review show ID|#N [--diff]`: the requests, message,
  author, per-path review status (owners files, carried rule,
  identical-only source) and diff stat of a change under review.
- **Added.** `review status` and `land` say what a rebase did ("rebased
  from X onto Y: your changes are unchanged", with each path's carry
  rule), and a land conflict where both sides only added lines says keeping
  both carries the review.
- **Added.** `leep commit` warns about a file holding a whole conflict
  marker trio (`--allow-markers` silences it) and appends trailers from
  `--trailer KEY=VALUE` and `commit.trailers` in `.leep/config.json`.
- **Added.** `GET .../review-status` paths carry `owners_files` and
  `carry_source` (optional, omitted when empty).
