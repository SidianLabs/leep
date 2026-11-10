### Notes: what an agent learned, for the next one on the same files
- **Added.** Notes (ADR 0098): `POST /v1/notes` writes a `finding`,
  `gotcha`, `decision` or `howto` on a file, a directory or a changeset
  (a path is resolved at the session's target ref and must exist there);
  `GET /v1/notes?path=` lists the notes on a path and the directories
  above it (or by changeset, principal or state); `GET` and `DELETE
  /v1/notes/{id}` read and retract one; `POST /v1/notes/{id}/report`
  hides an agent's note at once. An edit is a new note that `supersedes`
  the old one.
- **Added.** `leep note add|list|show|retract|report`, and the MCP tools
  `leep_note_add` and `leep_notes` (26 tools). A note's title, body and
  anchor are other agents' words: under `untrusted` on the wire, fenced
  in MCP results, quoted and escaped by the CLI.
- **Added.** The tokens file takes `"kind": "human" | "agent"` (absent is
  `agent`); a human token's notes are trusted as a person's.
  `gen-token --kind`, and `kind` in `GET /v1/whoami`. Upgrade every
  server before a tokens file uses it.
- **Added.** Limits: titles 200 bytes, bodies 4 KiB, plain text only (no
  control characters, line separators or bidirectional controls); 200
  open notes and 30 an hour per principal (400 `too_many_notes`, 429).
  Agent notes expire after 90 days, human ones after a year.
  `leep-server notes revoke --principal P` retracts every open note of a
  token.
- **Added.** Migration 0010 (`notes`), additive. Notes are included in
  backups. Metrics `leep_notes_added_total`, `leep_notes_refused_total`,
  `leep_notes_reported_total` and `leep_notes_swept_total`.
