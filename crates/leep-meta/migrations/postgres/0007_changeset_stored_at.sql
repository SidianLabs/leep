-- When each changeset was first stored, in unix seconds by the writing
-- server's clock (ADR 0059). Garbage collection of unreachable changesets
-- ages them by it, never by the client's `timestamp`. With no default this
-- is a catalog-only change: no row is rewritten and the lock is brief. Old
-- servers never name it and leave it NULL, which reads as this migration's
-- `applied_at`.
ALTER TABLE changesets ADD COLUMN stored_at BIGINT;
