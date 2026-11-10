### Notes: stale ones fade, human owners endorse, packs carry them
- **Added.** Notes say whether they are stale: `stale` on every note is
  `true` once its file's content at the ref's head is not what the note
  was written against (or the path is gone), judged by one bounded tree
  lookup per note; `GET /v1/notes?ref=` picks the head. A note whose
  staleness cannot be told is shown without it, never as an error.
- **Added.** `POST /v1/notes/{id}/endorse` and `leep note endorse ID`: a
  person's token (`"kind": "human"`) that `OWNERS` names for the note's
  path vouches for another's note. It then ranks first and no longer
  expires, until its file changes (`endorsed` turns false; `endorsed_by`
  and `endorsed_at` stay for audit). Agents' tokens, the note's writer
  and non-owners are refused (403), as are closed or stale notes (409).
  There is no MCP tool for it.
- **Added.** Context packs gain a `notes` section (10% of the budget,
  taken from files and history): endorsed notes, then people's, then
  agents', fresh before stale, the nearest first. Agents' notes come as
  titles with their id, stale agents' notes are left out, and every word
  stays under `untrusted` (fenced in `leep_task_start` and `leep_context`,
  quoted by `leep context`).
- **Changed.** A report by a human owner of the note's path now hides a
  person's or an endorsed note too; other reports of those still only
  count.
- **Added.** Metrics `leep_notes_endorsed_total`,
  `leep_notes_stale_suppressed_total` and `leep_context_notes_total{trust}`.
