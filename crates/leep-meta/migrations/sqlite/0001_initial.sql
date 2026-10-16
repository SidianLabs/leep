-- Baseline: the schema as it was before versioning. Idempotent (IF NOT
-- EXISTS) so it can also finish an un-versioned database made by an older
-- release that lacks `ref_log`. No later migration may be idempotent in
-- this way; they run exactly once. Never edit this file (ADR 0023).
CREATE TABLE IF NOT EXISTS changesets (id BLOB PRIMARY KEY, data BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target BLOB NOT NULL);
CREATE TABLE IF NOT EXISTS derived (kind TEXT NOT NULL, key BLOB NOT NULL,
    value BLOB NOT NULL, PRIMARY KEY (kind, key));
-- AUTOINCREMENT so a seq is never reused, even after rows are pruned.
CREATE TABLE IF NOT EXISTS ref_log (seq INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL, old_target BLOB, new_target BLOB, time INTEGER NOT NULL,
    actor TEXT, operation TEXT NOT NULL, message TEXT);
CREATE INDEX IF NOT EXISTS ref_log_name ON ref_log (name, seq);
