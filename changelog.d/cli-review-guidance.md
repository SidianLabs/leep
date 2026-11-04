### The CLI guides review requests, waits for verdicts, and keeps changelog entries apart
- **Added.** `leep review status REV --wait[=SECS]` asks again, less and
  less often, until the changeset is approved or the ref holds it (exit 0),
  a path is rejected or SECS pass (exit 1; default an hour, 0 for no
  limit). `leep review request --wait` asks and then waits the same way.
- **Changed.** A land refused for review names `leep review request @`
  and `--wait`; `leep review request` says how to follow the request up.
- **Changed.** `leep review inbox` leaves out requests you already approved
  or rejected (and says how many); `--all` shows them, and every entry says
  who approved or rejected it ("approved by you, rhea").
- **Changed.** `leep review list` marks verdicts carried from a rebased
  original `(carried from ID)` and drops the "do not count for landing"
  note when they carry. `leep review status` says `landed on main` (exit 0)
  for a changeset main already holds, and a verdict on one says it now only
  affects copies, and where a reject or withdrawal was also written.
- **Changed.** After a conflicted land of one changeset, `leep commit`
  without `-m` reuses that changeset's message (`state.json` `resolving`
  gains an optional `message`; see docs/FORMATS.md).
- **Changed.** `leep show` prints `Created-by:` after `Author:`, not after
  the message. The shallow-clone refusal of `leep-git import` and `mirror`
  no longer says "import" for the mirror.
- **Docs.** Changelog entries are now fragments in `changelog.d/`, which
  `scripts/changelog-assemble.sh` moves into `CHANGELOG.md` (ADR 0088).
