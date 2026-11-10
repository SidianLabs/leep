-- Notes (ADR 0098): shared memory between agents, anchored to a path or a
-- changeset. Coordination state: advisory and expiring, never read by a
-- land. `kind` is 'finding', 'gotcha', 'decision' or 'howto';
-- `anchor_type` 'path' or 'changeset'; `trust` 'agent' or 'human', fixed
-- when written; `state` 'open', 'superseded', 'retracted' or 'reported'.
-- `reported_by` and `reported_at` name who hid it by a report, and when.
-- `expires` is when the sweep deletes the row: the end of an open note's
-- life (NULL once endorsed), or 30 days after a note closed. Title, body
-- and anchor are agent-written text. v9 servers already running never read
-- the table and keep working beside the migration; a v9 server refuses to
-- start on the migrated database (ADR 0023).
CREATE TABLE notes (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id INTEGER NOT NULL DEFAULT 1,
    kind TEXT NOT NULL,
    anchor_type TEXT NOT NULL,
    anchor TEXT NOT NULL,
    at_changeset BLOB,
    at_blob BLOB,
    line_start INTEGER,
    line_end INTEGER,
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    principal TEXT NOT NULL,
    agent_model TEXT,
    session_id INTEGER,
    trust TEXT NOT NULL DEFAULT 'agent',
    endorsed_by TEXT,
    endorsed_at INTEGER,
    state TEXT NOT NULL DEFAULT 'open',
    superseded_by INTEGER,
    reported_by TEXT,
    reported_at INTEGER,
    created INTEGER NOT NULL,
    expires INTEGER
);
-- Notes on a path or changeset, newest first: exact probes of the anchor
-- and the directories above it.
CREATE INDEX notes_anchor ON notes (repo_id, anchor_type, anchor, id);
-- A principal's notes: its open count and hourly rate, and its listing.
CREATE INDEX notes_principal ON notes (repo_id, principal, created);
CREATE INDEX notes_expires ON notes (expires);
-- Who hid notes by reporting them, and when: a reporter's hourly cap, and
-- an operator's undo of one reporter's reports.
CREATE INDEX notes_reported ON notes (reported_by, reported_at) WHERE reported_by IS NOT NULL;
