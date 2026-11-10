-- Notes (ADR 0098): shared memory between agents, anchored to a path or a
-- changeset. Coordination state: advisory and expiring, never read by a
-- land. `kind` is 'finding', 'gotcha', 'decision' or 'howto';
-- `anchor_type` 'path' or 'changeset'; `trust` 'agent' or 'human', fixed
-- when written; `state` 'open', 'superseded', 'retracted' or 'reported'.
-- `expires` is when the sweep deletes the row: the end of an open note's
-- life (NULL once endorsed), or 30 days after a note closed. Title, body
-- and anchor are agent-written text. v9 servers already running never read
-- the table and keep working beside the migration; a v9 server refuses to
-- start on the migrated database (ADR 0023).
CREATE TABLE notes (
    id BIGSERIAL PRIMARY KEY,
    repo_id BIGINT NOT NULL DEFAULT 1,
    kind TEXT NOT NULL,
    anchor_type TEXT NOT NULL,
    anchor TEXT COLLATE "C" NOT NULL,
    at_changeset BYTEA,
    at_blob BYTEA,
    line_start INT,
    line_end INT,
    title TEXT NOT NULL,
    body TEXT NOT NULL DEFAULT '',
    principal TEXT NOT NULL,
    agent_model TEXT,
    session_id BIGINT,
    trust TEXT NOT NULL DEFAULT 'agent',
    endorsed_by TEXT,
    endorsed_at BIGINT,
    state TEXT NOT NULL DEFAULT 'open',
    superseded_by BIGINT,
    created BIGINT NOT NULL,
    expires BIGINT
);
-- Notes on a path or changeset, newest first: exact probes of the anchor
-- and the directories above it.
CREATE INDEX notes_anchor ON notes (repo_id, anchor_type, anchor, id);
-- A principal's notes: its open count and hourly rate, and its listing.
CREATE INDEX notes_principal ON notes (repo_id, principal, created);
CREATE INDEX notes_expires ON notes (expires);
