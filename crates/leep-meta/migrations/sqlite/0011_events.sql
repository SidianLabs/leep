-- Events (ADR 0099): one row per coordination fact (a session started,
-- changed its paths or ended; a note was added, endorsed or superseded),
-- read in `seq` order by every server so a cursor from one server resumes
-- on another. Ids, states and paths only, never agent-written text.
-- `paths` and `payload` are JSON (an array of at most 16 paths, an object
-- of facts). SQLite has one writer, so appends commit in `seq` order.
-- AUTOINCREMENT, so a `seq` is never handed out twice, even after the
-- sweep empties the table: cursors never go backwards. Kept 24 hours,
-- swept in `seq` order from the primary key, so no other index. v10
-- servers already running never read it and keep working beside the
-- migration; a v10 server refuses to start on the migrated database (ADR
-- 0023).
CREATE TABLE events (
    seq INTEGER PRIMARY KEY AUTOINCREMENT,
    repo_id INTEGER NOT NULL DEFAULT 1,
    kind TEXT NOT NULL,
    session_id INTEGER,
    principal TEXT NOT NULL,
    subject TEXT NOT NULL,
    paths TEXT NOT NULL DEFAULT '[]',
    payload TEXT NOT NULL DEFAULT '{}',
    created INTEGER NOT NULL
);
