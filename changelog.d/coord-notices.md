### Overlap notices and duplicate flags
- **Added.** Answers tell an agent about other live work near its session
  (ADR 0096): `near` (same directory), `same_file`, `claimed`,
  `possible_duplicate` (the same task key, or a declared title and approach
  much alike) and `landed` (a change touching its paths landed on its
  target ref since its base, naming the changeset). At most three per
  answer, highest first, each said once, on changeset creates, lands,
  `POST /v1/sessions` and `GET /v1/sessions`, in an optional `notices`
  field; `Leep-Notices` picks the lowest level or `off`.
- **Added.** `GET /v1/notices` and `GET /v1/sessions/{id}/overlaps`.
- **Added.** The CLI prints notices on stderr after each command, escaped;
  `leep status` asks for them; `leep who --overlaps`; `LEEP_NOTICE_LEVEL`.
- **Added.** MCP tool results end with notices, other agents' words
  fenced; new tool `leep_overlaps`.
- **Added.** `--notices off` (`LEEP_NOTICES`) and metrics
  `leep_overlap_notices_total`, `leep_duplicate_sessions_flagged_total`,
  `leep_notices_followed_total` and `leep_notices_omitted_total`.
- **Docs.** ADR 0096: notices are computed after an answer is decided,
  within 150 ms or not at all, and never change it; the land-equivalence
  test now computes them throughout.
