-- Baseline: the schema as it was before versioning. Idempotent (IF NOT
-- EXISTS) so it can also finish an un-versioned database made by an older
-- release that lacks `ref_log`. No later migration may be idempotent in
-- this way; they run exactly once. Never edit this file (ADR 0023).
CREATE TABLE IF NOT EXISTS changesets (id BYTEA PRIMARY KEY, data BYTEA NOT NULL);
CREATE TABLE IF NOT EXISTS refs (name TEXT PRIMARY KEY, target BYTEA NOT NULL);
CREATE TABLE IF NOT EXISTS derived (kind TEXT NOT NULL, key BYTEA NOT NULL,
    value BYTEA NOT NULL, PRIMARY KEY (kind, key));
CREATE TABLE IF NOT EXISTS ref_log (seq BIGSERIAL PRIMARY KEY, name TEXT NOT NULL,
    old_target BYTEA, new_target BYTEA, time BIGINT NOT NULL, actor TEXT,
    operation TEXT NOT NULL, message TEXT);
CREATE INDEX IF NOT EXISTS ref_log_name ON ref_log (name, seq);
